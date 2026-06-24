//! ShareSyncManager —— 分享同步顶层 orchestrator
//!
//! ## 职责
//!
//! 1. 维护订阅集合（DashMap<id, ShareSubscription>）
//! 2. 为每条订阅维护一个 `SubscriptionScheduler`（独立的 tokio task）
//! 3. 实现 `ExecutorHooks`（生产环境），把 transfer/download 派发到既有 manager
//! 4. 对外暴露 CRUD + trigger + 列表/详情 API
//!
//! ## 生命周期
//!
//! - `new()`  → 打开 SQLite + 读取 JSON 订阅 → 恢复每条的 scheduler
//! - `add/update/delete`  → 写 JSON + DB → 启停 scheduler
//! - `execute_one(id)`  → 抓取 → diff → 提交 → 持久化 → 广播 WS
//! - `shutdown()`  → 停所有 scheduler → 关闭连接

use crate::auth::Uid;
use crate::netdisk::client::NetdiskClient;
use crate::share_sync::config::{ShareSubscription, SyncTarget};
use crate::share_sync::diff::{diff_snapshots, ShareDiff, ShareModifiedItem};
use crate::share_sync::error::ShareSyncError;
use crate::share_sync::events::{
    NoopShareSyncEventPublisher, ShareSyncEvent, ShareSyncEventPublisher,
};
use crate::share_sync::executor::{
    ApplyOutcome, ExecutorHooks, NetdiskTargetEntry, ShareSyncExecutor,
};
use crate::share_sync::persistence::ShareSyncPersistence;
use crate::share_sync::resolver::ShareSyncAccountResolver;
use crate::share_sync::scheduler::SubscriptionScheduler;
use crate::share_sync::snapshot::{
    CapturedShare, ShareSnapshot, ShareSnapshotItem, SnapshotCollector,
};
use crate::share_sync::types::{ConflictStrategy, RunStatus};
use crate::transfer::{TransferManager, TransferStatus};
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// 顶层 Manager
pub struct ShareSyncManager {
    /// 订阅 ID → 最新配置（in-memory 权威）
    subscriptions: DashMap<String, ShareSubscription>,
    /// 订阅 ID → Scheduler
    schedulers: DashMap<String, SubscriptionScheduler>,
    /// 正在执行中的订阅 ID 集合（并发触发去重；presence = 有 run 在跑）
    running: DashMap<String, ()>,
    /// 持久化层
    persistence: Arc<ShareSyncPersistence>,
    /// 配置文件路径（JSON）
    config_path: PathBuf,
    /// 事件发布器
    publisher: Arc<dyn ShareSyncEventPublisher>,
    /// 账号解析器：按订阅 owner_uid 解析其 NetdiskClient / TransferManager（多账号隔离）
    resolver: Arc<dyn ShareSyncAccountResolver>,
    /// v2 阶段 6:share-sync 全局风控限速器 — 阻挡 ProductionHooks 出去的
    /// submit_transfer/submit_download 调用,避免并行 worker 撞 errno=132 风控。
    /// 参数从 env(BAIDUPCS_RATE_LIMIT_*) 读, 默认 4 RPS / burst=8;
    /// BAIDUPCS_RATE_LIMIT_ENABLED=0 时退化为无限速直通(供 A/B 对照)。
    rate_limiter: Arc<crate::share_sync::rate_limit::QuotaLimiter>,
}

impl std::fmt::Debug for ShareSyncManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareSyncManager")
            .field("subscriptions_count", &self.subscriptions.len())
            .field("config_path", &self.config_path)
            .finish_non_exhaustive()
    }
}

/// Manager 构造参数
pub struct ManagerConfig {
    pub config_path: PathBuf,
    pub db_path: PathBuf,
    /// 账号解析器：按订阅 owner_uid 解析对应账号的 NetdiskClient / TransferManager
    pub resolver: Arc<dyn ShareSyncAccountResolver>,
    pub publisher: Option<Arc<dyn ShareSyncEventPublisher>>,
}

impl ShareSyncManager {
    /// 构造并恢复订阅
    pub async fn new(cfg: ManagerConfig) -> Result<Arc<Self>, ShareSyncError> {
        let persistence = Arc::new(ShareSyncPersistence::new(&cfg.db_path)?);

        // 启动期 stale-run 自愈:进程重启后,上次留下的 status='running' run 都是
        // 孤儿(内存里的 run task 已随进程退出)。**不再粗暴标 Failed**——那对用户是
        // "明明只是重启却显示失败"(用户反馈:"重启后那些要自动恢复跑吧,就跟自动备份
        // 一样,怎么能直接标记失败呢")。改为标 Interrupted(中断),并在订阅恢复后对其
        // 所属(且启用)订阅自动重跑一次:同步是增量的(基线只在成功后推进),被中断的
        // run 没推进基线,重跑会重新 diff 把没跑完的补上。可用 env
        // BAIDUPCS_STALE_FIXUP_ENABLED=0 关闭收编;BAIDUPCS_SHARE_SYNC_RESUME_ON_STARTUP=0
        // 仅收编不自动重跑(交给下个轮询周期)。
        let stale_fixup_enabled = std::env::var("BAIDUPCS_STALE_FIXUP_ENABLED")
            .ok()
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);
        let resume_on_startup = std::env::var("BAIDUPCS_SHARE_SYNC_RESUME_ON_STARTUP")
            .ok()
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);
        // 被中断、待自动重跑的订阅 id(去重)。在订阅恢复后才知道哪些 enabled。
        let mut interrupted_sub_ids: Vec<String> = Vec::new();
        if stale_fixup_enabled {
            match persistence.mark_running_runs_interrupted() {
                Ok(interrupted) if !interrupted.is_empty() => {
                    info!(
                        "share_sync 启动自愈: 收编 {} 条中断 run,稍后自动重跑",
                        interrupted.len()
                    );
                    for rec in &interrupted {
                        if !interrupted_sub_ids.contains(&rec.subscription_id) {
                            interrupted_sub_ids.push(rec.subscription_id.clone());
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => warn!("share_sync 启动自愈失败: {}", e),
            }
        }

        if let Some(parent) = cfg.config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 数据库为唯一可信源（订阅已并入主库）。先尝试从 DB 恢复。
        let mut subs = persistence.list_subscriptions().unwrap_or_else(|e| {
            error!("share-sync: 从 DB 读取订阅失败，按空列表启动: {}", e);
            Vec::new()
        });

        // 一次性兼容旧版本：早期把订阅写在独立的 subscriptions.json。
        // 仅当 DB 尚无订阅且存在旧 JSON 时导入，导入后把 JSON 改名为 .migrated，
        // 之后不再读写 JSON（消除 JSON↔DB 双写漂移与损坏静默丢失问题）。
        if subs.is_empty() && cfg.config_path.exists() {
            subs = Self::import_legacy_json(&cfg.config_path, &persistence);
        }

        let manager = Arc::new(Self {
            subscriptions: DashMap::new(),
            schedulers: DashMap::new(),
            running: DashMap::new(),
            persistence,
            config_path: cfg.config_path,
            publisher: cfg
                .publisher
                .unwrap_or_else(|| Arc::new(NoopShareSyncEventPublisher)),
            resolver: cfg.resolver,
            rate_limiter: crate::share_sync::rate_limit::QuotaLimiter::from_env(),
        });

        // 多账号时代之前的旧订阅 owner_uid 默认为 0（#[serde(default)] u64）。
        // 设计上由"上层在创建/导入时赋值"(见 config.rs:133 注释),但实际跑起来
        // 发现历史数据没补过,导致 trigger 后静默失败:resolver.netdisk_client(0)
        // 拿不到 client,execute_one 抛 ConfigError 被 scheduler on_tick 闭包吞掉,
        // 前端却拿到 `{"triggered": true}` 误以为成功。启动期集中补齐到当前活跃账号。
        let active_uid = manager.resolver.active_uid().await;
        match active_uid {
            Some(active) => {
                let mut migrated = 0usize;
                for sub in subs.iter_mut() {
                    if sub.owner_uid == 0 {
                        info!(
                            "share-sync: 迁移旧订阅 {} owner_uid 0 -> {} (历史/未归属数据)",
                            sub.id, active
                        );
                        sub.owner_uid = active;
                        if let Err(e) = manager.persistence.upsert_subscription(sub) {
                            warn!("share-sync: 迁移订阅 {} 写回 DB 失败: {}", sub.id, e);
                        }
                        migrated += 1;
                    }
                }
                if migrated > 0 {
                    info!(
                        "share-sync: 启动期 owner_uid 迁移完成, 共 {} 条 (active_uid={})",
                        migrated, active
                    );
                }
            }
            None => {
                let legacy = subs.iter().filter(|s| s.owner_uid == 0).count();
                if legacy > 0 {
                    warn!(
                        "share-sync: 启动时发现 {} 条 owner_uid=0 的旧订阅, 但当前无活跃账号, \
                         无法迁移 —— 用户登录后下次启动会自动迁移,或在前端编辑订阅以重设归属",
                        legacy
                    );
                }
            }
        }

        for sub in subs {
            manager.subscriptions.insert(sub.id.clone(), sub.clone());
            if sub.enabled && sub.poll_config.enabled {
                let mgr_clone = Arc::clone(&manager);
                mgr_clone.start_scheduler_for(&sub);
            }
        }

        info!(
            "ShareSyncManager 初始化完成: 恢复 {} 条订阅",
            manager.subscriptions.len()
        );

        // 对被中断的、且仍启用的订阅自动重跑一次(像自动备份重启续跑)。只保留 enabled
        // 的——已禁用的订阅不该被启动悄悄唤醒。后台 spawn,best-effort:账号尚未就绪时
        // execute_one 在创建 run 前就报错(不留失败记录),退避重试若干次;始终不行则交给
        // 调度器在下个轮询周期补上,不阻塞启动。
        if resume_on_startup && !interrupted_sub_ids.is_empty() {
            let resume_ids: Vec<String> = interrupted_sub_ids
                .into_iter()
                .filter(|id| {
                    manager
                        .subscriptions
                        .get(id)
                        .map(|s| s.enabled)
                        .unwrap_or(false)
                })
                .collect();
            if !resume_ids.is_empty() {
                let mgr = Arc::clone(&manager);
                tokio::spawn(async move {
                    mgr.resume_interrupted_runs(resume_ids).await;
                });
            }
        }

        Ok(manager)
    }

    /// 启动期对被中断的订阅自动重跑一次。best-effort:先**轮询等账号登录态就绪**
    /// 再触发,避免账号没恢复时 execute_one 反复在「抓取阶段」失败、刷出一堆失败 run。
    /// 等到就绪(或超时)后只触发一次;触发不成则交给轮询调度兜底。供 `new` 后台调用。
    async fn resume_interrupted_runs(self: Arc<Self>, sub_ids: Vec<String>) {
        // 给账号登录态恢复留点时间(进程刚起,resolver 可能还没就绪)。
        const RESUME_INITIAL_DELAY_SECS: u64 = 5;
        const READY_MAX_ATTEMPTS: u32 = 12;
        const READY_RETRY_DELAY_SECS: u64 = 10;
        tokio::time::sleep(std::time::Duration::from_secs(RESUME_INITIAL_DELAY_SECS)).await;
        for id in sub_ids {
            let owner_uid = match self.get_subscription(&id) {
                Some(s) => s.owner_uid,
                None => continue, // 订阅启动后被删,跳过
            };
            // 等账号(网盘客户端 + 转存管理器)就绪——execute_one 在抓取前需要它们。
            let mut ready = false;
            for _ in 0..READY_MAX_ATTEMPTS {
                let has_netdisk = self.resolver.netdisk_client(owner_uid).await.is_some();
                let has_transfer = self.resolver.transfer_manager(owner_uid).await.is_some();
                if has_netdisk && has_transfer {
                    ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(READY_RETRY_DELAY_SECS)).await;
            }
            if !ready {
                warn!(
                    "share_sync 启动续跑: 订阅 {} 所属账号(uid={})迟迟未就绪,交给轮询调度兜底",
                    id, owner_uid
                );
                continue;
            }
            match self.execute_one(&id).await {
                Ok(_) => info!("share_sync 启动续跑: 订阅 {} 已重新同步", id),
                // 调度器抢先触发了 —— 正常,无需重复。
                Err(ShareSyncError::AlreadyRunning(_)) => {}
                Err(e) => warn!(
                    "share_sync 启动续跑: 订阅 {} 触发失败({}),交给轮询调度兜底",
                    id, e
                ),
            }
        }
    }

    /// 一次性导入旧版 `subscriptions.json` 到主库，成功后把文件改名为 `.migrated`。
    ///
    /// 读/解析失败不静默吞掉：读失败仅告警返回空；解析失败把损坏文件改名备份后告警，
    /// 避免误判为"无旧数据"。导入的订阅 owner_uid 保持 JSON 中的值（旧数据通常为 0，
    /// 由上层在初始化时按当前活跃账号补归属）。
    fn import_legacy_json(
        config_path: &Path,
        persistence: &ShareSyncPersistence,
    ) -> Vec<ShareSubscription> {
        let content = match std::fs::read_to_string(config_path) {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "share-sync: 读取旧订阅配置 {} 失败，跳过导入: {}",
                    config_path.display(),
                    e
                );
                return Vec::new();
            }
        };
        let list: Vec<ShareSubscription> = match serde_json::from_str(&content) {
            Ok(l) => l,
            Err(e) => {
                let backup = config_path
                    .with_extension(format!("corrupt.{}.json", chrono::Utc::now().timestamp()));
                let hint = match std::fs::rename(config_path, &backup) {
                    Ok(()) => format!("已备份损坏文件到 {}", backup.display()),
                    Err(re) => format!("备份损坏文件失败: {}", re),
                };
                error!(
                    "share-sync: 旧订阅配置 {} 解析失败，跳过导入（{}）: {}",
                    config_path.display(),
                    hint,
                    e
                );
                return Vec::new();
            }
        };
        for sub in &list {
            if let Err(e) = persistence.upsert_subscription(sub) {
                error!("share-sync: 导入旧订阅 {} 到主库失败: {}", sub.id, e);
            }
        }
        let migrated = config_path.with_extension("json.migrated");
        if let Err(e) = std::fs::rename(config_path, &migrated) {
            warn!(
                "share-sync: 旧订阅配置已导入主库，但改名 {} 失败（下次启动会因 DB 已有数据而跳过导入）: {}",
                migrated.display(),
                e
            );
        }
        info!("share-sync: 已从旧 JSON 导入 {} 条订阅到主库", list.len());
        list
    }

    // ===================================================
    // 订阅 CRUD
    // ===================================================

    pub fn list_subscriptions(&self) -> Vec<ShareSubscription> {
        self.subscriptions
            .iter()
            .map(|kv| kv.value().clone())
            .collect()
    }

    /// 列出归属指定账号的订阅（多账号隔离：handler 按 active_uid 过滤）
    pub fn list_for_owner(&self, owner_uid: u64) -> Vec<ShareSubscription> {
        self.subscriptions
            .iter()
            .filter(|kv| kv.value().owner_uid == owner_uid)
            .map(|kv| kv.value().clone())
            .collect()
    }

    pub fn get_subscription(&self, id: &str) -> Option<ShareSubscription> {
        self.subscriptions.get(id).map(|kv| kv.value().clone())
    }

    /// 列出某订阅当前的子任务进度（下载段 + 内部转存段），供 REST 轮询兜底接口。
    ///
    /// 与「每个 run 的进度广播器」共用 `collect_share_sync_subtasks`，形状一致。
    /// 账号转存管理器未就绪时返回空列表（视为暂无进行中子任务，不报错）。
    pub async fn subtasks(&self, id: &str) -> Result<Vec<ShareSyncSubtask>, ShareSyncError> {
        let sub = self
            .get_subscription(id)
            .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
        let owner_uid = sub.owner_uid;
        match self.resolver.transfer_manager(owner_uid).await {
            Some(tm) => {
                // 这是「进行中子任务」轮询兜底接口：必须只返回**未到终态**的子任务。
                // 文件夹下载任务带 backup_config_id 归属后会**持久保留**(完成也不删),
                // 若不过滤,切换页面后重新拉取会把已完成的文件夹当成「进行中」显示
                // (前端 REST 路径直接信任后端,不像 WS upsert 那样剔除终态)。
                // 与前端 SUBTASK_TERMINAL 口径一致,在源头过滤掉终态子任务。
                let subs = collect_share_sync_subtasks(&tm, id, owner_uid)
                    .await
                    .into_iter()
                    .filter(|s| !is_terminal_subtask_status(&s.status))
                    .collect();
                Ok(subs)
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn create_subscription(
        self: &Arc<Self>,
        sub: ShareSubscription,
    ) -> Result<ShareSubscription, ShareSyncError> {
        sub.validate().map_err(ShareSyncError::ConfigError)?;
        if self.subscriptions.contains_key(&sub.id) {
            return Err(ShareSyncError::SubscriptionExists(sub.id.clone()));
        }
        self.persistence.upsert_subscription(&sub)?;
        self.subscriptions.insert(sub.id.clone(), sub.clone());
        if sub.enabled && sub.poll_config.enabled {
            self.start_scheduler_for(&sub);
        }
        self.publisher.publish(ShareSyncEvent::SubscriptionCreated {
            subscription_id: sub.id.clone(),
            name: sub.name.clone(),
            owner_uid: sub.owner_uid,
        });
        info!("ShareSyncManager: 创建订阅 id={}", sub.id);
        // 启用的订阅创建后立即执行一次首同步，无需等待首个轮询周期
        if sub.enabled {
            let mgr = Arc::clone(self);
            let sub_id = sub.id.clone();
            tokio::spawn(async move {
                if let Err(e) = mgr.trigger_one(&sub_id).await {
                    warn!("share-sync: 订阅 {} 创建后首同步触发失败: {}", sub_id, e);
                }
            });
        }
        Ok(sub)
    }

    pub fn update_subscription(
        self: &Arc<Self>,
        id: &str,
        mut new_sub: ShareSubscription,
    ) -> Result<ShareSubscription, ShareSyncError> {
        new_sub.validate().map_err(ShareSyncError::ConfigError)?;
        {
            let existing = self
                .subscriptions
                .get(id)
                .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
            new_sub.id = existing.id.clone();
            new_sub.created_at = existing.created_at;
        }
        new_sub.touch();
        self.persistence.upsert_subscription(&new_sub)?;
        self.subscriptions.insert(id.into(), new_sub.clone());
        // 重启 scheduler（间隔可能变了）
        self.stop_scheduler_for(id);
        if new_sub.enabled && new_sub.poll_config.enabled {
            self.start_scheduler_for(&new_sub);
        }
        self.publisher.publish(ShareSyncEvent::SubscriptionUpdated {
            subscription_id: id.into(),
            owner_uid: new_sub.owner_uid,
        });
        Ok(new_sub)
    }

    pub fn set_enabled(self: &Arc<Self>, id: &str, enabled: bool) -> Result<(), ShareSyncError> {
        let mut sub = self
            .subscriptions
            .get_mut(id)
            .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
        sub.enabled = enabled;
        sub.touch();
        let sub_clone = sub.clone();
        drop(sub);
        self.persistence.upsert_subscription(&sub_clone)?;
        if enabled && sub_clone.poll_config.enabled {
            self.start_scheduler_for(&sub_clone);
        } else {
            self.stop_scheduler_for(id);
        }
        self.publisher.publish(ShareSyncEvent::StatusChanged {
            subscription_id: id.into(),
            enabled,
            owner_uid: sub_clone.owner_uid,
        });
        Ok(())
    }

    /// 「链接确定性失效」连续失败阈值：达到即自动暂停轮询。可用
    /// `BAIDUPCS_SHARE_SYNC_LINK_FAIL_THRESHOLD` 覆盖（最小 1），默认 2。
    fn link_fail_threshold() -> u32 {
        std::env::var("BAIDUPCS_SHARE_SYNC_LINK_FAIL_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .map(|v| v.max(1))
            .unwrap_or(2)
    }

    /// 仅当错误属于「链接确定性失效」时才累加失效计数；临时网络/风控错误不计。
    fn maybe_note_link_failure(&self, id: &str, err: &ShareSyncError) {
        if err.is_link_invalid() {
            self.note_link_failure(id, &err.to_string());
        }
    }

    /// 记一次「链接确定性失效」：连续计数 +1；达阈值则置 `link_invalid` 暂停轮询。
    ///
    /// 不在此处停 scheduler（本方法在 `execute_one` 内、即 scheduler tick 内被调用，
    /// 显式 stop 会等自身 task 结束造成死锁）；改由 `execute_one` 在 `link_invalid`
    /// 时提前返回（不发任何百度请求）实现「不再定时唤起」。
    fn note_link_failure(&self, id: &str, reason: &str) {
        let Some(mut sub) = self.subscriptions.get_mut(id) else {
            return;
        };
        if sub.link_invalid {
            return; // 已暂停，无需重复累加
        }
        sub.consecutive_link_failures = sub.consecutive_link_failures.saturating_add(1);
        let threshold = Self::link_fail_threshold();
        let mut paused = false;
        if sub.consecutive_link_failures >= threshold {
            sub.link_invalid = true;
            sub.link_invalid_reason = Some(reason.to_string());
            paused = true;
        }
        let count = sub.consecutive_link_failures;
        let owner_uid = sub.owner_uid;
        let sub_clone = sub.clone();
        drop(sub);
        let _ = self.persistence.upsert_subscription(&sub_clone);
        if paused {
            warn!(
                "share-sync: 订阅 {} 连续 {}/{} 次链接失效，自动暂停轮询: {}",
                id, count, threshold, reason
            );
            self.publisher.publish(ShareSyncEvent::StatusChanged {
                subscription_id: id.into(),
                enabled: sub_clone.enabled,
                owner_uid,
            });
        } else {
            info!(
                "share-sync: 订阅 {} 链接失效计数 {}/{}（达阈值后将暂停）: {}",
                id, count, threshold, reason
            );
        }
    }

    /// 成功抓取一次分享 → 链接可用：归零连续失效计数（若曾被标记失效也清除）。
    fn clear_link_failure(&self, id: &str) {
        let Some(mut sub) = self.subscriptions.get_mut(id) else {
            return;
        };
        if sub.consecutive_link_failures == 0 && !sub.link_invalid {
            return; // 无状态可清，省一次写库
        }
        sub.consecutive_link_failures = 0;
        sub.link_invalid = false;
        sub.link_invalid_reason = None;
        let sub_clone = sub.clone();
        drop(sub);
        let _ = self.persistence.upsert_subscription(&sub_clone);
    }

    /// 用户「我已更新链接，恢复」：清除失效标记 + 计数，恢复轮询并立即触发一次。
    pub async fn resume_link_invalid(self: &Arc<Self>, id: &str) -> Result<(), ShareSyncError> {
        {
            let mut sub = self
                .subscriptions
                .get_mut(id)
                .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
            sub.link_invalid = false;
            sub.link_invalid_reason = None;
            sub.consecutive_link_failures = 0;
            sub.touch();
            let sub_clone = sub.clone();
            drop(sub);
            self.persistence.upsert_subscription(&sub_clone)?;
            // scheduler 从未停过（见 note_link_failure），但防御性确保它在跑
            if sub_clone.enabled && sub_clone.poll_config.enabled {
                self.start_scheduler_for(&sub_clone);
            }
            self.publisher.publish(ShareSyncEvent::StatusChanged {
                subscription_id: id.into(),
                enabled: sub_clone.enabled,
                owner_uid: sub_clone.owner_uid,
            });
        }
        // 立即重试一次（链接已更新）。恢复动作本身（清除失效标记 + 恢复轮询）此时已成功，
        // 立即触发只是锦上添花；若该账号未登录等导致触发失败，不应让整个「恢复」接口报错，
        // 否则会出现「标记已清、轮询已恢复，接口却返回失败」的状态不一致。降级为只记日志。
        if let Err(e) = self.trigger_one(id).await {
            warn!(
                "share-sync: 订阅 {} 恢复后立即触发失败（已恢复轮询，下个周期会自动重试）: {}",
                id, e
            );
        }
        Ok(())
    }

    pub async fn delete_subscription(self: &Arc<Self>, id: &str) -> Result<(), ShareSyncError> {
        let removed = match self.subscriptions.remove(id) {
            Some((_, sub)) => sub,
            None => return Err(ShareSyncError::SubscriptionNotFound(id.into())),
        };
        self.stop_scheduler_for(id);
        // DB 删除（级联清理 snapshots/runs）
        let _ = self.persistence.delete_subscription(id);
        // 清理该订阅名下的内部转存/下载任务（带 share-sync:{id} 归属），
        // 否则删订阅后这些任务会成为孤儿（重启被恢复成永远跑不完的隐藏任务 → 脏数据）。
        // 转存管理器会连带清理它持有的下载子任务。
        let cfg_id = share_sync_backup_config_id(id);
        if let Some(transfer) = self.resolver.transfer_manager(removed.owner_uid).await {
            let (mem, hist) = transfer.delete_tasks_for_backup_config(&cfg_id).await;
            if mem > 0 || hist > 0 {
                info!(
                    "share-sync: 删除订阅 {} 已清理内部转存任务（内存={}, 历史={}）",
                    id, mem, hist
                );
            }
        }
        self.publisher.publish(ShareSyncEvent::SubscriptionDeleted {
            subscription_id: id.into(),
            owner_uid: removed.owner_uid,
        });
        Ok(())
    }

    // ===================================================
    // 触发 / 执行
    // ===================================================

    /// 立即触发一次（HTTP / 手动）。
    ///
    /// 手动触发不再只唤醒 scheduler：scheduler 的错误只会落日志，HTTP 调用方会误以为
    /// 已成功开始。这里同步完成可判定的前置校验，然后直接排一个后台 run。
    pub async fn trigger_one(self: &Arc<Self>, id: &str) -> Result<String, ShareSyncError> {
        let sub = self
            .get_subscription(id)
            .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
        info!(
            "share-sync: trigger subscription id={} name={} owner_uid={} enabled={}",
            sub.id, sub.name, sub.owner_uid, sub.enabled
        );
        // owner_uid=0 表示历史脏数据:resolver 拿不到 client,execute_one 会抛
        // ConfigError,被 scheduler on_tick 闭包吞掉,前端却拿到 success 误判。
        // 启动期迁移应当把 owner_uid 补上;若仍为 0(无活跃账号等情形),直接报错。
        if sub.owner_uid == 0 {
            return Err(ShareSyncError::ConfigError(
                "订阅所属账号未设置（owner_uid=0），请等待启动迁移完成或在前端编辑订阅".into(),
            ));
        }
        if sub.link_invalid {
            return Err(ShareSyncError::ShareLinkError(
                sub.link_invalid_reason
                    .clone()
                    .unwrap_or_else(|| "分享链接已失效，已暂停轮询；请更新链接后恢复".into()),
            ));
        }
        if self.running.contains_key(id) {
            return Err(ShareSyncError::AlreadyRunning(format!(
                "订阅 {} 正在同步中，请等待当前运行结束",
                id
            )));
        }
        if self.resolver.netdisk_client(sub.owner_uid).await.is_none() {
            return Err(ShareSyncError::ConfigError(format!(
                "订阅所属账号(uid={})未登录，请先登录该账号后再同步",
                sub.owner_uid
            )));
        }
        if self
            .resolver
            .transfer_manager(sub.owner_uid)
            .await
            .is_none()
        {
            return Err(ShareSyncError::ConfigError(format!(
                "订阅所属账号(uid={})的转存管理器未就绪",
                sub.owner_uid
            )));
        }

        // 同步落库 + 广播：让 HTTP 调用方在拿到响应瞬间就能拿到一个"已经写进 runs 表、
        // status=running、WS 端能监听到 RunStarted"的真实 run_id。
        // 前端可以直接跳到 run 详情页拿进度 / 子任务列表，不用再做"我刚点的触发是不是真的
        // 排队成功了"的二次轮询判断。
        let run_id = Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now().timestamp();
        if let Err(e) = self.persistence.start_run(&run_id, id, started_at) {
            return Err(e);
        }
        self.publisher.publish(ShareSyncEvent::RunStarted {
            run_id: run_id.clone(),
            subscription_id: id.into(),
            owner_uid: sub.owner_uid,
        });

        let mgr = Arc::clone(self);
        let id_owned = id.to_string();
        let run_id_for_spawn = run_id.clone();
        tokio::spawn(async move {
            if let Err(e) = mgr
                .execute_one_with_run_id(&id_owned, Some(run_id_for_spawn))
                .await
            {
                warn!(
                    "share-sync: 订阅 {} 手动触发的 execute_one 失败: {}",
                    id_owned, e
                );
            }
        });
        info!(
            "share-sync: 已启动订阅 {} 的手动同步 run, run_id={}",
            id, run_id
        );
        Ok(run_id)
    }

    /// 执行一次（由 scheduler 调用或 trigger_one 同步入口）。
    ///
    /// 内部委托给 [`execute_one_with_run_id`] 并自动 mint 新 run_id；scheduler tick
    /// 路径与外部调用继续走这里即可，无需自己管 run_id 生成。
    pub async fn execute_one(&self, id: &str) -> Result<ApplyOutcome, ShareSyncError> {
        self.execute_one_with_run_id(id, None).await
    }

    /// `execute_one` 的内部版本：当 `given_run_id` 为 `Some(rid)` 时，复用预先生成的
    /// run_id（不再重新 mint），跳过 `start_run` / `RunStarted` 广播——用于 `trigger_one`
    /// 这类"已经让前端拿到 run_id"的入口；为 `None` 时由本函数自管整个 run 生命周期。
    pub async fn execute_one_with_run_id(
        &self,
        id: &str,
        given_run_id: Option<String>,
    ) -> Result<ApplyOutcome, ShareSyncError> {
        // 全局并发去重：同一订阅同一时刻只允许一个 run 在执行。
        // scheduler 的 running 标志只防它自己循环内重入；这里覆盖所有入口
        // （手动 trigger / 被禁用订阅的 spawn 路径 / 多个调度器并存），
        // 避免并发 run 重复转存同一批文件并产生快照基线竞争。
        if self.running.insert(id.to_string(), ()).is_some() {
            debug!("share-sync: 订阅 {} 已有 run 在执行，跳过本次触发", id);
            // trigger_one 预建的 run 已写库并广播 RunStarted，但本次执行被去重拦下、
            // 不会再走到收尾，需在此 fail_run，避免留下永远 running 的孤儿 run。
            if let Some(rid) = given_run_id.as_deref() {
                let owner_uid = self.get_subscription(id).map(|s| s.owner_uid).unwrap_or(0);
                self.fail_run(rid, id, owner_uid, "已有同步任务在执行，已取消本次触发");
            }
            return Err(ShareSyncError::AlreadyRunning(id.into()));
        }
        // RAII 守卫：无论从哪条分支返回都移除 in-flight 标记。
        struct RunGuard<'g> {
            running: &'g DashMap<String, ()>,
            id: String,
        }
        impl Drop for RunGuard<'_> {
            fn drop(&mut self) {
                self.running.remove(&self.id);
            }
        }
        let _run_guard = RunGuard {
            running: &self.running,
            id: id.to_string(),
        };

        // v2 阶段 7:打 timing A/B metric 用
        let run_started = std::time::Instant::now();
        let sub = self
            .get_subscription(id)
            .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;

        // 链接已确定性失效（自动暂停）：直接跳过，不发任何百度请求，等用户「恢复」。
        // 这样调度器即便每轮 tick 也只是空转返回，不再徒劳访问已失效的分享、不增风控压力。
        if sub.link_invalid {
            debug!(
                "share-sync: 订阅 {} 链接已失效（已暂停），跳过本次触发；等待用户更新链接后恢复",
                id
            );
            let reason = sub
                .link_invalid_reason
                .clone()
                .unwrap_or_else(|| "分享链接已失效，已暂停轮询；请更新链接后恢复".into());
            // 同上：收尾 trigger_one 预建的 run，避免孤儿 running。
            if let Some(rid) = given_run_id.as_deref() {
                self.fail_run(rid, id, sub.owner_uid, &reason);
            }
            return Err(ShareSyncError::ShareLinkError(reason));
        }

        // 多账号隔离：按订阅 owner_uid 解析**该账号**的网盘客户端与转存管理器，
        // 而非进程当前活跃账号。后台调度对账号 A 的订阅始终用账号 A 的实例，
        // 账号切换无需 relink。任一未就绪 → 明确报错，绝不落到错误账号。
        let owner_uid = sub.owner_uid;
        let netdisk = match self.resolver.netdisk_client(owner_uid).await {
            Some(c) => c,
            None => {
                let msg = format!(
                    "订阅所属账号(uid={})未登录，请先登录该账号后再同步",
                    owner_uid
                );
                // 收尾 trigger_one 预建的 run，避免孤儿 running。
                if let Some(rid) = given_run_id.as_deref() {
                    self.fail_run(rid, id, owner_uid, &msg);
                }
                return Err(ShareSyncError::ConfigError(msg));
            }
        };
        let transfer = match self.resolver.transfer_manager(owner_uid).await {
            Some(t) => t,
            None => {
                let msg = format!("订阅所属账号(uid={})的转存管理器未就绪", owner_uid);
                // 收尾 trigger_one 预建的 run，避免孤儿 running。
                if let Some(rid) = given_run_id.as_deref() {
                    self.fail_run(rid, id, owner_uid, &msg);
                }
                return Err(ShareSyncError::ConfigError(msg));
            }
        };

        let run_id = match given_run_id {
            Some(rid) => rid,
            None => {
                let rid = Uuid::new_v4().to_string();
                let started_at = chrono::Utc::now().timestamp();
                if let Err(e) = self.persistence.start_run(&rid, id, started_at) {
                    return Err(e);
                }
                self.publisher.publish(ShareSyncEvent::RunStarted {
                    run_id: rid.clone(),
                    subscription_id: id.into(),
                    owner_uid,
                });
                rid
            }
        };

        // 1) 抓取
        let (captured, curr_snapshot) = match SnapshotCollector::from_url(
            netdisk.as_ref(),
            &sub.share_url,
            sub.password.clone(),
            sub.include_paths.clone(),
            sub.exclude_patterns.clone(),
            // 列目录抓快照与转存提交共用同一个全局风控限速器
            self.rate_limiter.clone(),
        )
            .await
        {
            Ok(collector) => match collector.collect().await {
                Ok(t) => t,
                Err(e) => {
                    self.fail_run(&run_id, id, owner_uid, &format!("抓取失败: {}", e));
                    self.maybe_note_link_failure(id, &e);
                    return Err(e);
                }
            },
            Err(e) => {
                self.fail_run(&run_id, id, owner_uid, &format!("抓取初始化失败: {}", e));
                self.maybe_note_link_failure(id, &e);
                return Err(e);
            }
        };
        // 成功抓取到分享内容 → 链接可用，归零失效计数（如曾标记失效也清除）。
        self.clear_link_failure(id);

        // 2) 绑定 subscription_id 后，先读"上次成功应用的快照"再计算 diff。
        //    当前快照必须等执行成功后才能推进基线；否则下载/转存失败会把
        //    未落地的新版本标记成已同步，后续轮询 diff 变空而不再重试。
        let mut curr_snapshot = curr_snapshot;
        curr_snapshot.subscription_id = id.into();
        let prev = self.persistence.latest_snapshot(id).ok().flatten();

        // 3) diff
        let mut diff = diff_snapshots(prev.as_ref(), &curr_snapshot);
        if let Err(e) =
            augment_diff_with_local_target_state(&sub, prev.as_ref(), &curr_snapshot, &mut diff)
        {
            self.fail_run(&run_id, id, owner_uid, &format!("本地目标校验失败: {}", e));
            return Err(e);
        }

        self.publisher.publish(ShareSyncEvent::DiffDetected {
            run_id: run_id.clone(),
            subscription_id: id.into(),
            added: diff.added.iter().filter(|i| !i.is_dir).count(),
            modified: diff.modified.iter().filter(|i| !i.new.is_dir).count(),
            removed: diff.removed.iter().filter(|i| !i.is_dir).count(),
            owner_uid,
        });

        // 4) 执行
        // 启动「子任务进度广播器」：run 期间约 1s 推一次 ItemProgress（走 share_sync 频道，
        // 不与自动备份 / 下载管理混淆）。run 结束后 abort。前端 WS 实时刷，REST 轮询兜底。
        let progress_handle = {
            let publisher = Arc::clone(&self.publisher);
            let transfer = Arc::clone(&transfer);
            let run_id = run_id.clone();
            let subscription_id = id.to_string();
            tokio::spawn(async move {
                broadcast_subtask_progress(publisher, transfer, run_id, subscription_id, owner_uid)
                    .await;
            })
        };

        let hooks = ProductionHooks {
            netdisk,
            transfer,
            captured: captured.clone(),
            owner_uid,
            rate_limiter: Arc::clone(&self.rate_limiter),
            subscription_id: sub.id.clone(),
        };
        let executor = ShareSyncExecutor::new(&sub, &self.persistence, &hooks);
        // v2 阶段 3:默认走 tree 入口(顶层节点整体提交,目录 fs_id 直传);
        // 仅当显式 BAIDUPCS_DIR_TRANSFER_ENABLED=0/false 时退回老的单文件路径。
        // 阶段 4-6 的二分/并行/限速都挂在 tree 入口下。
        let dir_transfer_enabled = std::env::var("BAIDUPCS_DIR_TRANSFER_ENABLED")
            .ok()
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);
        let outcome = if dir_transfer_enabled {
            let (execution_diff, added_dir_ancestors) =
                execution_diff_with_directory_ancestors(&diff, &curr_snapshot);
            if added_dir_ancestors > 0 {
                info!(
                    "share_sync_tree_prepare: run_id={} subscription={} added_dir_ancestors={}",
                    run_id, sub.id, added_dir_ancestors
                );
            }
            info!(
                "share_sync_route: run_id={} subscription={} mode=tree",
                run_id, sub.id
            );
            executor
                .apply_with_run_id_tree(run_id.clone(), &captured, &execution_diff)
                .await
        } else {
            executor
                .apply_with_run_id(run_id.clone(), &captured, &diff)
                .await
        };

        // run 结束，停止进度广播器（再补推一帧最终态，确保前端拿到 completed/failed）。
        progress_handle.abort();
        broadcast_subtask_progress_once(
            Arc::clone(&self.publisher),
            self.resolver.transfer_manager(owner_uid).await,
            run_id.clone(),
            id.to_string(),
            owner_uid,
        )
            .await;

        // 仅当 run 完成**且**没有任何子项因资源类原因（配额满 / 本地磁盘满）被跳过时，
        // 才推进快照基线。否则被跳过、尚未真正落地的项会被写入新基线，导致下一次
        // diff 不再包含它们 —— 即使后来腾出空间也不会补传。
        if should_advance_snapshot_baseline(outcome.status) && !outcome.resource_skipped {
            if let Err(e) = self.persistence.save_snapshot(&curr_snapshot) {
                warn!("save_snapshot 失败，下一次同步会重试本次 diff: {}", e);
            }
        } else {
            warn!(
                "share-sync: run 未完全成功或有资源类跳过，不推进快照基线，下一次将重试 diff: run_id={}, status={:?}, failed={}, resource_skipped={}",
                outcome.run_id, outcome.status, outcome.diff_summary.failed, outcome.resource_skipped
            );
        }

        // 5) 广播
        match outcome.status {
            crate::share_sync::types::RunStatus::Completed
            | crate::share_sync::types::RunStatus::CompletedWithErrors => {
                let duration_ms = run_started.elapsed().as_millis() as u64;
                info!(
                    "share_sync_run_finished: run_id={} subscription={} status={:?} duration_ms={} added={} modified={} removed={} failed={} skipped={}",
                    outcome.run_id,
                    id,
                    outcome.status,
                    duration_ms,
                    outcome.diff_summary.added,
                    outcome.diff_summary.modified,
                    outcome.diff_summary.removed,
                    outcome.diff_summary.failed,
                    outcome.diff_summary.skipped,
                );
                self.publisher.publish(ShareSyncEvent::RunCompleted {
                    run_id: outcome.run_id.clone(),
                    subscription_id: id.into(),
                    added: outcome.diff_summary.added,
                    modified: outcome.diff_summary.modified,
                    removed: outcome.diff_summary.removed,
                    failed: outcome.diff_summary.failed,
                    owner_uid,
                    duration_ms: Some(duration_ms),
                    n_bisects: None, // v2 阶段 4 的二分数未在 manager 层累积, 占 None
                    max_bisect_depth: None,
                });
            }
            crate::share_sync::types::RunStatus::Failed => {
                self.publisher.publish(ShareSyncEvent::RunFailed {
                    run_id: outcome.run_id.clone(),
                    subscription_id: id.into(),
                    error: outcome
                        .error
                        .clone()
                        .unwrap_or_else(|| "unknown error".into()),
                    owner_uid,
                    // v1: 目前 outcome.error 仍以原始字符串承载，reason 由 executor
                    // 在 quota/local_disk_full 早停时显式设置。
                    // 此分支对应 manager 自身检查到的失败（如 start_run 失败），
                    // 暂归类为 unknown，前端用 error 字段兜底展示。
                    reason: None,
                });
            }
            _ => {}
        }
        Ok(outcome)
    }

    fn fail_run(&self, run_id: &str, sub_id: &str, owner_uid: u64, err: &str) {
        use crate::share_sync::types::{DiffSummary, RunStatus};
        let now = chrono::Utc::now().timestamp();
        let _ = self.persistence.start_run(run_id, sub_id, now);
        let _ = self.persistence.finish_run(
            run_id,
            now,
            RunStatus::Failed,
            &DiffSummary::default(),
            Some(err),
        );
        self.publisher.publish(ShareSyncEvent::RunFailed {
            run_id: run_id.into(),
            subscription_id: sub_id.into(),
            error: err.into(),
            owner_uid,
            reason: None,
        });
    }

    // ===================================================
    // 调度启停
    // ===================================================

    fn start_scheduler_for(self: &Arc<Self>, sub: &ShareSubscription) {
        let interval = sub.poll_config.effective_interval_secs();
        if interval == 0 {
            return;
        }
        if self.schedulers.contains_key(&sub.id) {
            return;
        }
        let mut sched = SubscriptionScheduler::new(sub.id.clone(), interval);
        let mgr = Arc::clone(self);
        let sub_id = sub.id.clone();
        sched.start(move |id| {
            let mgr2 = Arc::clone(&mgr);
            async move {
                // 无论成功/失败都映射为 ()，由 scheduler 把 Err 记到日志
                mgr2.execute_one(&id).await.map(|_| ())
            }
        });
        info!("scheduler: 启动订阅 {} (interval={}s)", sub_id, interval);
        self.schedulers.insert(sub.id.clone(), sched);
    }

    fn stop_scheduler_for(&self, id: &str) {
        if let Some((_, mut sched)) = self.schedulers.remove(id) {
            // drop 时会 cancel，但显式 stop 等待 task 结束
            let id_owned = id.to_string();
            tokio::spawn(async move {
                sched.stop().await;
                info!("scheduler: 停止订阅 {}", id_owned);
            });
        }
    }

    /// 优雅停机
    pub async fn shutdown(&self) {
        let ids: Vec<String> = self.schedulers.iter().map(|kv| kv.key().clone()).collect();
        for id in ids {
            self.stop_scheduler_for(&id);
        }
        // 等一会儿让 task 退出
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        info!("ShareSyncManager 已关闭");
    }

    pub fn persistence(&self) -> &Arc<ShareSyncPersistence> {
        &self.persistence
    }
}

fn should_advance_snapshot_baseline(status: RunStatus) -> bool {
    matches!(status, RunStatus::Completed)
}

/// Tree 执行专用 diff。
///
/// 普通 diff 只包含新增/修改的文件；如果目录本身没有变化，tree::build 只能为
/// `/dir/file` 造一个 fs_id=0 的 placeholder 目录，最终会退回逐文件提交。这里把
/// 当前快照中真实存在的目录祖先补进 added，让 tree 路径优先用目录 fs_id 整体转存。
/// 目录项不计入 summary，也不会被保存为新的基线；它们只影响本次执行计划。
///
/// **仅对「整目录全新」的目录补祖先**：树顶若是带真实 fs_id 的目录节点，会走整目录
/// 转存+整目录下载(folder download 扫描网盘目标里**全部**文件)。若某个已同步目录里
/// 只改动了个别文件，却把这个目录整体补进来，就会把目录里**未变动的文件也重新转存、
/// 重新下载一遍**(默认 Overwrite 策略下物理重下)。所以这里加判据：只有当该目录在当前
/// 快照下的**全部子文件**都属于本次变动集(added∪modified)时,才补成整目录转存;否则保留
/// placeholder,让 tree 回退到逐文件提交,只下变动的那几个文件。首次同步/全新子目录的
/// 目录项本就在 diff.added 里,不受此判据影响,仍走整目录高效转存。
fn execution_diff_with_directory_ancestors(
    diff: &ShareDiff,
    curr: &ShareSnapshot,
) -> (ShareDiff, usize) {
    let curr_index = curr.index_by_path();
    let mut existing_paths: BTreeSet<String> = diff
        .added
        .iter()
        .map(|item| item.path.clone())
        .chain(diff.modified.iter().map(|item| item.new.path.clone()))
        .chain(diff.removed.iter().map(|item| item.path.clone()))
        .collect();

    // 本次变动涉及的文件路径(added∪modified, 仅文件)。判断目录是否「整目录全新」时,
    // 要求其全部子文件都落在这个集合里。
    let changed_files: BTreeSet<String> = diff
        .added
        .iter()
        .chain(diff.modified.iter().map(|m| &m.new))
        .filter(|item| !item.is_dir)
        .map(|item| item.path.clone())
        .collect();

    let mut out = diff.clone();
    let action_paths: Vec<String> = changed_files.iter().cloned().collect();

    let mut added = 0usize;
    for path in action_paths {
        let mut current = parent_netdisk_dir(&path);
        while current != "/" {
            if existing_paths.insert(current.clone()) {
                if let Some(item) = curr_index.get(&current).filter(|item| item.is_dir) {
                    // 仅当整目录子文件全部变动时才整体转存,避免重下未变动文件。
                    if dir_subtree_fully_changed(&curr_index, &current, &changed_files) {
                        out.added.push((**item).clone());
                        added += 1;
                    }
                }
            }
            let parent = parent_netdisk_dir(&current);
            if parent == current {
                break;
            }
            current = parent;
        }
    }

    out.added.sort_by(|a, b| a.path.cmp(&b.path));
    (out, added)
}

/// 判断目录 `dir` 在当前快照下的**全部子文件**(递归)是否都属于本次变动集 `changed`。
///
/// 用于决定能否把这个目录整体补进 tree 的整目录转存:只有「整棵子树的文件都是本次新增/
/// 修改」时才安全(否则会连带重传/重下未变动的文件)。目录里**没有**任何子文件时返回
/// `false` —— 空目录/纯子目录壳没有整目录转存的价值,留给上层(若其子目录各自满足条件会
/// 被单独补)处理,避免误把一个含未变动文件的祖先判成「全新」。
fn dir_subtree_fully_changed(
    curr_index: &BTreeMap<String, &ShareSnapshotItem>,
    dir: &str,
    changed: &BTreeSet<String>,
) -> bool {
    let prefix = format!("{}/", dir.trim_end_matches('/'));
    let mut saw_file = false;
    for (path, item) in curr_index.range(prefix.clone()..) {
        if !path.starts_with(&prefix) {
            break;
        }
        if item.is_dir {
            continue;
        }
        saw_file = true;
        if !changed.contains(path) {
            return false;
        }
    }
    saw_file
}

// =====================================================
// 生产环境 ExecutorHooks
// =====================================================

/// 分享同步子任务的归属 id：`"share-sync:{订阅id}"`。
///
/// 永不与自动备份的 UUID 配置 id 冲突，故下载段 `is_backup=true` 复用不会挂到自动备份。
pub fn share_sync_backup_config_id(subscription_id: &str) -> String {
    format!("share-sync:{}", subscription_id)
}

/// 分享同步「进行中子任务」的进度快照（REST 轮询接口 + WS 广播共用同一形状）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShareSyncSubtask {
    /// 底层任务 id（下载任务 id 或内部转存任务 id）
    pub task_id: String,
    /// 文件名 / 展示名
    pub name: String,
    /// 子任务种类:`"transfer"`(转存段) | `"download"`(下载段)
    pub kind: String,
    /// 状态字符串(downloading / completed / failed / transferring ...)
    pub status: String,
    /// 已完成字节(下载段);转存段用已完成文件数
    pub downloaded: u64,
    /// 总字节(下载段);转存段用总文件数
    pub total: u64,
    /// 进度百分比 0-100
    pub progress: f64,
    /// 瞬时速度(B/s,仅下载段有意义)
    pub speed: u64,
    /// 预计剩余时间(秒,仅下载段且 speed>0 时有值)，与自动备份 `eta_seconds` 对齐
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_seconds: Option<u64>,
    /// 订阅所属账号 uid
    pub owner_uid: u64,
}

/// 由「已下载/总字节/瞬时速度」推算预计剩余时间(秒)。
///
/// 仅在 `speed > 0` 且 `total > downloaded` 时返回 `Some`，否则 `None`
/// (与自动备份 `SpeedCalculator::calculate_eta` 同义)。
fn compute_eta_seconds(downloaded: u64, total: u64, speed: u64) -> Option<u64> {
    if speed == 0 || total <= downloaded {
        return None;
    }
    Some((total - downloaded) / speed)
}

fn basename_of(path: &str) -> String {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(path)
        .to_string()
}

/// 收集某个订阅当前的子任务进度（下载段 + 内部转存段），按 `backup_config_id` 归属。
///
/// REST 轮询接口与「每个 run 的进度广播器」共用此函数，保证两条链路形状一致。
pub async fn collect_share_sync_subtasks(
    transfer: &TransferManager,
    subscription_id: &str,
    owner_uid: u64,
) -> Vec<ShareSyncSubtask> {
    let cfg = share_sync_backup_config_id(subscription_id);
    let mut out: Vec<ShareSyncSubtask> = Vec::new();

    // 下载管理器句柄：单文件下载段 + 文件夹聚合段(按 group 汇总子任务速度)共用。
    let dm_handle = transfer.download_manager_handle().await;

    // 下载段:复用自动备份同款查询(is_backup && backup_config_id==cfg)
    if let Some(dm) = dm_handle.as_ref() {
        for t in dm.get_tasks_by_backup_config(&cfg).await {
            let name = t
                .local_path
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| basename_of(&t.remote_path));
            let progress = if t.total_size > 0 {
                (t.downloaded_size as f64 / t.total_size as f64) * 100.0
            } else {
                0.0
            };
            out.push(ShareSyncSubtask {
                task_id: t.id.clone(),
                name,
                kind: "download".to_string(),
                status: format!("{:?}", t.status).to_lowercase(),
                downloaded: t.downloaded_size,
                total: t.total_size,
                progress,
                speed: t.speed,
                eta_seconds: compute_eta_seconds(t.downloaded_size, t.total_size, t.speed),
                owner_uid,
            });
        }
    }

    // 转存段:内部转存任务(is_internal && backup_config_id==cfg),用文件数做进度
    for t in transfer.get_all_tasks().await {
        if t.is_internal && t.backup_config_id.as_deref() == Some(cfg.as_str()) {
            let progress = if t.total_count > 0 {
                (t.transferred_count as f64 / t.total_count as f64) * 100.0
            } else {
                0.0
            };
            out.push(ShareSyncSubtask {
                task_id: t.id.clone(),
                name: t
                    .file_name
                    .clone()
                    .unwrap_or_else(|| basename_of(&t.save_path)),
                kind: "transfer".to_string(),
                status: format!("{:?}", t.status).to_lowercase(),
                downloaded: t.transferred_count as u64,
                total: t.total_count as u64,
                progress,
                speed: 0,
                eta_seconds: None,
                owner_uid,
            });
        }
    }

    // 下载段(文件夹):tree 模式整目录转存 + 自动下载会产生文件夹下载任务
    // (backup_config_id==cfg)。其子文件任务带 group_id 不会单独出现在下载管理,
    // 也不走 is_backup 的 get_tasks_by_backup_config,因此在此按文件夹级聚合进度。
    if let Some(fdm) = transfer.folder_download_manager_handle().await {
        for f in fdm.get_folders_by_backup_config(&cfg).await {
            // 文件夹本身不持有速度，按其活跃子文件任务(group_id==folder.id)的
            // 瞬时速度求和聚合，口径与文件夹进度广播器(folder_manager)一致。
            // 必须用 is_active_download_status 而非单匹配 Downloading：
            // folder group 子任务会在 Downloading ↔ Decrypting ↔ Pending 之间切换，
            // 仅 Downloading 会漏掉解密中/等待中的子任务的速度贡献 → 前端恒为 0。
            let speed: u64 = if let Some(dm) = dm_handle.as_ref() {
                dm.get_tasks_by_group(&f.id)
                    .await
                    .iter()
                    .filter(|t| t.status.is_active_download_status())
                    .map(|t| t.speed)
                    .sum()
            } else {
                0
            };
            out.push(ShareSyncSubtask {
                task_id: format!("folder:{}", f.id),
                name: f.name.clone(),
                kind: "download".to_string(),
                status: format!("{:?}", f.status).to_lowercase(),
                downloaded: f.downloaded_size,
                total: f.total_size,
                progress: f.progress(),
                speed,
                eta_seconds: compute_eta_seconds(f.downloaded_size, f.total_size, speed),
                owner_uid,
            });
        }
    }

    out
}

/// 子任务状态是否已到终态（完成/失败/取消）。
///
/// 口径覆盖三个来源的状态枚举(lowercased `{:?}`):
/// - 文件/文件夹下载(`TaskStatus`/`FolderStatus`): `completed` / `failed` / `cancelled`
/// - 内部转存(`TransferStatus`): `completed` / `transferfailed` / `downloadfailed`
///
/// 与前端 `SUBTASK_TERMINAL`(completed/failed/cancelled/success) 对齐,额外纳入
/// 转存段特有的失败态,确保「进行中子任务」轮询接口不会回包任何已结束的子任务。
fn is_terminal_subtask_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "success" | "failed" | "cancelled" | "transferfailed" | "downloadfailed"
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RestartableShareSyncDownload {
    Task(String),
    Folder(String),
}

fn restartable_share_sync_download(
    subtask: &ShareSyncSubtask,
) -> Option<RestartableShareSyncDownload> {
    if subtask.kind != "download" || is_terminal_subtask_status(&subtask.status) {
        return None;
    }

    if let Some(folder_id) = subtask.task_id.strip_prefix("folder:") {
        return matches!(subtask.status.as_str(), "scanning" | "downloading")
            .then(|| RestartableShareSyncDownload::Folder(folder_id.to_string()));
    }

    (subtask.status == "downloading")
        .then(|| RestartableShareSyncDownload::Task(subtask.task_id.clone()))
}

async fn restart_stalled_share_sync_downloads(
    transfer: &TransferManager,
    subtasks: &[ShareSyncSubtask],
    resume_delay: Duration,
) -> usize {
    let mut task_ids = Vec::new();
    let mut folder_ids = Vec::new();

    for subtask in subtasks {
        match restartable_share_sync_download(subtask) {
            Some(RestartableShareSyncDownload::Task(task_id)) => task_ids.push(task_id),
            Some(RestartableShareSyncDownload::Folder(folder_id)) => folder_ids.push(folder_id),
            None => {}
        }
    }

    task_ids.sort();
    task_ids.dedup();
    folder_ids.sort();
    folder_ids.dedup();

    let mut restarted = 0usize;

    if !folder_ids.is_empty() {
        match transfer.folder_download_manager_handle().await {
            Some(folder_manager) => {
                for folder_id in folder_ids {
                    match folder_manager.pause_folder(&folder_id).await {
                        Ok(()) => {
                            if resume_delay > Duration::from_secs(0) {
                                tokio::time::sleep(resume_delay).await;
                            }
                            match folder_manager.resume_folder(&folder_id).await {
                                Ok(()) => {
                                    restarted += 1;
                                    warn!(
                                        "share-sync: stalled folder download restarted: folder_id={}",
                                        folder_id
                                    );
                                }
                                Err(e) => warn!(
                                    "share-sync: resume stalled folder download failed: folder_id={}, error={}",
                                    folder_id, e
                                ),
                            }
                        }
                        Err(e) => warn!(
                            "share-sync: pause stalled folder download failed: folder_id={}, error={}",
                            folder_id, e
                        ),
                    }
                }
            }
            None => warn!(
                "share-sync: stalled folder downloads found but folder download manager is unavailable"
            ),
        }
    }

    if !task_ids.is_empty() {
        match transfer.download_manager_handle().await {
            Some(download_manager) => {
                for task_id in task_ids {
                    match download_manager.pause_task(&task_id, true).await {
                        Ok(()) => {
                            if resume_delay > Duration::from_secs(0) {
                                tokio::time::sleep(resume_delay).await;
                            }
                            match download_manager.resume_task(&task_id).await {
                                Ok(()) => {
                                    restarted += 1;
                                    warn!(
                                        "share-sync: stalled download task restarted: task_id={}",
                                        task_id
                                    );
                                }
                                Err(e) => warn!(
                                    "share-sync: resume stalled download task failed: task_id={}, error={}",
                                    task_id, e
                                ),
                            }
                        }
                        Err(e) => warn!(
                            "share-sync: pause stalled download task failed: task_id={}, error={}",
                            task_id, e
                        ),
                    }
                }
            }
            None => {
                warn!(
                    "share-sync: stalled download tasks found but download manager is unavailable"
                )
            }
        }
    }

    restarted
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubtaskActivitySignature {
    task_id: String,
    kind: String,
    status: String,
    downloaded: u64,
    total: u64,
    progress_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskActivitySignature {
    status: TransferStatus,
    transferred_count: usize,
    total_count: usize,
    updated_at: i64,
    download_task_ids: Vec<String>,
    completed_download_ids: Vec<String>,
    failed_download_ids: Vec<String>,
    subtasks: Vec<SubtaskActivitySignature>,
}

fn task_activity_signature(
    task: &crate::transfer::task::TransferTask,
    subtasks: &[ShareSyncSubtask],
) -> TaskActivitySignature {
    let mut download_task_ids = task.download_task_ids.clone();
    download_task_ids.sort();
    let mut completed_download_ids = task.completed_download_ids.clone();
    completed_download_ids.sort();
    let mut failed_download_ids = task.failed_download_ids.clone();
    failed_download_ids.sort();
    let mut subtask_signatures: Vec<SubtaskActivitySignature> = subtasks
        .iter()
        .map(|s| {
            let progress = if s.progress.is_finite() {
                s.progress.clamp(0.0, 100.0)
            } else {
                0.0
            };
            SubtaskActivitySignature {
                task_id: s.task_id.clone(),
                kind: s.kind.clone(),
                status: s.status.clone(),
                downloaded: s.downloaded,
                total: s.total,
                progress_millis: (progress * 1000.0).round() as u64,
            }
        })
        .collect();
    subtask_signatures.sort_by(|a, b| {
        a.task_id
            .cmp(&b.task_id)
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.status.cmp(&b.status))
    });

    TaskActivitySignature {
        status: task.status.clone(),
        transferred_count: task.transferred_count,
        total_count: task.total_count,
        updated_at: task.updated_at,
        download_task_ids,
        completed_download_ids,
        failed_download_ids,
        subtasks: subtask_signatures,
    }
}

fn env_duration_secs(name: &str) -> Option<Duration> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok().and_then(|v| v.parse::<u32>().ok())
}

fn share_sync_task_idle_timeout(default: Duration) -> Duration {
    env_duration_secs("BAIDUPCS_SHARE_SYNC_TASK_IDLE_TIMEOUT_SECS").unwrap_or(default)
}

fn share_sync_task_hard_timeout() -> Option<Duration> {
    match std::env::var("BAIDUPCS_SHARE_SYNC_TASK_HARD_TIMEOUT_SECS") {
        Ok(v) => match v.parse::<u64>() {
            Ok(0) => None,
            Ok(secs) => Some(Duration::from_secs(secs)),
            Err(_) => Some(Duration::from_secs(7 * 24 * 60 * 60)),
        },
        Err(_) => Some(Duration::from_secs(7 * 24 * 60 * 60)),
    }
}

fn share_sync_stall_retry_after(idle_timeout: Duration) -> Option<Duration> {
    let configured = env_duration_secs("BAIDUPCS_SHARE_SYNC_STALL_RETRY_SECS")
        .unwrap_or_else(|| Duration::from_secs(5 * 60));
    if configured == Duration::from_secs(0) || idle_timeout == Duration::from_secs(0) {
        return None;
    }
    if configured >= idle_timeout {
        let one_second = Duration::from_secs(1);
        return Some(if idle_timeout > one_second {
            idle_timeout - one_second
        } else {
            idle_timeout
        });
    }
    Some(configured)
}

fn share_sync_stall_retry_max() -> u32 {
    env_u32("BAIDUPCS_SHARE_SYNC_STALL_RETRY_MAX").unwrap_or(3)
}

fn share_sync_stall_retry_cooldown() -> Duration {
    env_duration_secs("BAIDUPCS_SHARE_SYNC_STALL_RETRY_COOLDOWN_SECS")
        .unwrap_or_else(|| Duration::from_secs(5))
}

/// 收集当前子任务并逐个推送 `ShareSyncEvent::ItemProgress`（一帧）。
async fn emit_subtask_progress(
    publisher: &Arc<dyn ShareSyncEventPublisher>,
    transfer: &TransferManager,
    run_id: &str,
    subscription_id: &str,
    owner_uid: u64,
) {
    let subs = collect_share_sync_subtasks(transfer, subscription_id, owner_uid).await;
    for s in subs {
        publisher.publish(ShareSyncEvent::ItemProgress {
            run_id: run_id.to_string(),
            subscription_id: subscription_id.to_string(),
            task_id: s.task_id,
            name: s.name,
            kind: s.kind,
            status: s.status,
            downloaded: s.downloaded,
            total: s.total,
            progress: s.progress,
            speed: s.speed,
            eta_seconds: s.eta_seconds,
            owner_uid,
        });
    }
}

/// 「每个 run 的子任务进度广播器」：约 1s 推一帧，直到被 abort。
async fn broadcast_subtask_progress(
    publisher: Arc<dyn ShareSyncEventPublisher>,
    transfer: Arc<TransferManager>,
    run_id: String,
    subscription_id: String,
    owner_uid: u64,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        emit_subtask_progress(&publisher, &transfer, &run_id, &subscription_id, owner_uid).await;
    }
}

/// 补推一帧最终态（run 结束后调用，确保前端拿到 completed/failed 终态）。
async fn broadcast_subtask_progress_once(
    publisher: Arc<dyn ShareSyncEventPublisher>,
    transfer: Option<Arc<TransferManager>>,
    run_id: String,
    subscription_id: String,
    owner_uid: u64,
) {
    if let Some(tm) = transfer {
        emit_subtask_progress(&publisher, &tm, &run_id, &subscription_id, owner_uid).await;
    }
}

struct ProductionHooks {
    /// 该订阅所属账号的网盘客户端（已按 owner_uid 解析）
    netdisk: Arc<NetdiskClient>,
    /// 该订阅所属账号的转存管理器（已按 owner_uid 解析）
    transfer: Arc<TransferManager>,
    captured: CapturedShare,
    /// 订阅所属账号 uid，透传给 transfer 的 owner_uid_override，确保落到正确账号
    owner_uid: u64,
    /// v2 阶段 6:出站请求前 acquire().await 走全局风控限速门
    rate_limiter: Arc<crate::share_sync::rate_limit::QuotaLimiter>,
    /// 当前订阅 id，用于构造下载子任务的 `backup_config_id = "share-sync:{id}"`，
    /// 实现任务隔离（隐藏 + 优先级 + 归属，详见 TransferTask::backup_config_id）。
    subscription_id: String,
}

impl ProductionHooks {
    /// 分享同步子任务的归属 id：`"share-sync:{订阅id}"`。
    /// 永不与自动备份的 UUID 配置 id 冲突，故 `is_backup=true` 复用不会挂到自动备份。
    fn share_sync_backup_config_id(&self) -> String {
        share_sync_backup_config_id(&self.subscription_id)
    }
}

#[async_trait]
impl ExecutorHooks for ProductionHooks {
    async fn submit_transfer(
        &self,
        captured: &CapturedShare,
        target_dir: &str,
        item: &ShareSnapshotItem,
        internal_label: Option<&str>,
    ) -> Result<String, ShareSyncError> {
        // v2 阶段 6:全局风控限速器
        self.rate_limiter.acquire().await;
        let tm = self.transfer_manager();
        use crate::transfer::manager::CreateTransferRequest;
        use crate::transfer::types::SharedFileInfo;

        // v1 修复：用 `item.path`（相对分享根的干净路径，如 `/data/2024/file.zip`）
        // 而非 `netdisk_transfer_selected_path(item)` 拼出的 `/sharelink1-1/<basename>`。
        // 这让 `TransferManager::execute_task` 内部的 `group_files_by_parent_dir`（见
        // `backend/src/transfer/manager.rs:4349`）能按 file.path 父目录分 batch，
        // 每个 batch 的 `group_target_dir = "{target_dir}/<relative_parent>"`，
        // 百度服务端在 target_dir 下自动创建中间目录 → **网盘目标里的子目录结构被还原**。
        let selected_path = item.path.clone();
        let req = CreateTransferRequest {
            share_url: share_url_for_captured(captured),
            password: captured.password.clone(),
            randsk: captured.randsk.clone(),
            prefetched_share: Some(prefetched_share_for_captured(captured)),
            save_path: target_dir.to_string(),
            save_fs_id: 0,
            auto_download: Some(false),
            local_download_path: None,
            is_share_direct_download: false,
            download_conflict_strategy: None,
            selected_fs_ids: Some(vec![item.fs_id]),
            selected_files: Some(vec![SharedFileInfo {
                fs_id: item.fs_id,
                // 不再强制 false：executor 不传目录项过来，但 batch 化时
                // 若上层带 is_dir=true 的"目录根锚点"也能透传。
                is_dir: item.is_dir,
                path: selected_path.clone(),
                size: item.size,
                name: item.name.clone(),
            }]),
            owner_uid_override: Some(Uid::new(self.owner_uid)),
            // 分享同步内部任务：从「转存管理」隐藏 + 归属 share-sync:{订阅id}
            // （下载段据此走自动备份同款 create_backup_task：隐藏 + 优先级 + 归属）。
            is_internal: true,
            backup_config_id: Some(self.share_sync_backup_config_id()),
        };
        let resp = tm
            .create_task(req)
            .await
            .map_err(|e| ShareSyncError::TransferError(e.to_string()))?;
        if resp.need_password {
            return Err(ShareSyncError::ShareLinkError("需要提取码".into()));
        }
        if let Some(err) = resp.error {
            return Err(ShareSyncError::TransferError(err));
        }
        let task_id = resp
            .task_id
            .ok_or_else(|| ShareSyncError::TransferError("TransferManager 未返回任务 ID".into()))?;
        info!(
            "share-sync: transfer submitted label={:?} task_id={} target_dir={} selected_path={}",
            internal_label, task_id, target_dir, selected_path
        );
        Ok(task_id)
    }

    async fn find_netdisk_file(
        &self,
        target_path: &str,
    ) -> Result<Option<NetdiskTargetEntry>, ShareSyncError> {
        let target_path = normalize_netdisk_path(target_path);
        let parent = parent_netdisk_dir(&target_path);
        let name = basename_netdisk_path(&target_path);
        let mut page = 1;
        let page_size = 1000;

        loop {
            let resp = match self.netdisk.get_file_list(&parent, page, page_size).await {
                Ok(resp) => resp,
                Err(e) => {
                    let msg = e.to_string();
                    if is_netdisk_not_found_error(&msg) {
                        return Ok(None);
                    }
                    return Err(ShareSyncError::TransferError(format!(
                        "查询网盘目标失败: path={}, error={}",
                        target_path, msg
                    )));
                }
            };

            if let Some(found) = resp.list.iter().find(|f| {
                normalize_netdisk_path(&f.path) == target_path || f.server_filename == name
            }) {
                return Ok(Some(NetdiskTargetEntry {
                    path: normalize_netdisk_path(&found.path),
                    name: found.server_filename.clone(),
                    fs_id: found.fs_id,
                    is_dir: found.isdir == 1,
                }));
            }

            if resp.list.len() < page_size as usize {
                return Ok(None);
            }
            page += 1;
            if page > 10_000 {
                return Err(ShareSyncError::TransferError(format!(
                    "查询网盘目标分页超过安全上限: parent={}",
                    parent
                )));
            }
        }
    }

    async fn rename_netdisk(
        &self,
        path: &str,
        fs_id: u64,
        new_name: &str,
    ) -> Result<String, ShareSyncError> {
        use crate::netdisk::{FileOperationOutcome, RenameItem};

        let path = normalize_netdisk_path(path);
        let outcome = self
            .netdisk
            .rename_file(RenameItem {
                path: path.clone(),
                newname: new_name.to_string(),
                id: fs_id,
            })
            .await
            .map_err(|e| ShareSyncError::TransferError(format!("网盘重命名失败: {}", e)))?;

        match outcome {
            FileOperationOutcome::Success(_) => {
                let new_path = join_netdisk_path(&parent_netdisk_dir(&path), new_name);
                info!("share-sync: netdisk rename 成功 {} -> {}", path, new_path);
                Ok(new_path)
            }
            FileOperationOutcome::Failed { message, .. } => Err(ShareSyncError::TransferError(
                format!("网盘重命名失败: {}", message),
            )),
        }
    }

    async fn submit_download(
        &self,
        item: &ShareSnapshotItem,
        local_dir: &Path,
        strategy: ConflictStrategy,
        transfer_netdisk_dir: Option<&str>,
    ) -> Result<String, ShareSyncError> {
        // v2 阶段 6:全局风控限速器
        self.rate_limiter.acquire().await;
        // 本地同步模式分流：
        // - 分享直下（transfer_netdisk_dir=None）：转存到临时目录，下载后清理（is_share_direct_download=true）。
        // - 转存并下载（Some(网盘目录)）：转存到该网盘目录并保留，再下载（is_share_direct_download=false）。
        // 两种模式的下载段都因 backup_config_id 走自动备份同款 create_backup_task。
        let (sync_save_path, sync_local_download, sync_is_share_direct) = match transfer_netdisk_dir
        {
            Some(netdisk_dir) => (
                netdisk_dir.to_string(),
                local_dir.to_string_lossy().to_string(),
                false,
            ),
            None => {
                let local_download_root = share_direct_download_root(local_dir, item)?;
                (
                    String::new(),
                    local_download_root.to_string_lossy().to_string(),
                    true,
                )
            }
        };
        let tm = self.transfer_manager();
        use crate::transfer::manager::CreateTransferRequest;
        use crate::transfer::types::SharedFileInfo;

        let raw_path = if item.raw_path.trim().is_empty() {
            item.path.clone()
        } else {
            item.raw_path.clone()
        };
        let req = CreateTransferRequest {
            share_url: share_url_for_captured(&self.captured),
            password: self.captured.password.clone(),
            randsk: self.captured.randsk.clone(),
            prefetched_share: Some(prefetched_share_for_captured(&self.captured)),
            save_path: sync_save_path,
            save_fs_id: 0,
            auto_download: Some(true),
            local_download_path: Some(sync_local_download),
            is_share_direct_download: sync_is_share_direct,
            download_conflict_strategy: Some(download_conflict_strategy_for_share_sync(strategy)),
            selected_fs_ids: Some(vec![item.fs_id]),
            selected_files: Some(vec![SharedFileInfo {
                fs_id: item.fs_id,
                is_dir: false,
                path: raw_path.clone(),
                size: item.size,
                name: item.name.clone(),
            }]),
            owner_uid_override: Some(Uid::new(self.owner_uid)),
            // 分享同步内部任务：从「转存管理」隐藏 + 归属 share-sync:{订阅id}
            // （下载段据此走自动备份同款 create_backup_task：隐藏 + 优先级 + 归属）。
            is_internal: true,
            backup_config_id: Some(self.share_sync_backup_config_id()),
        };

        let resp = tm
            .create_task(req)
            .await
            .map_err(|e| ShareSyncError::DownloadError(e.to_string()))?;
        if resp.need_password {
            return Err(ShareSyncError::ShareLinkError("需要提取码".into()));
        }
        if let Some(err) = resp.error {
            return Err(ShareSyncError::DownloadError(err));
        }
        let task_id = resp
            .task_id
            .ok_or_else(|| ShareSyncError::DownloadError("TransferManager 未返回任务 ID".into()))?;
        info!(
            "share-sync: download submitted task_id={} path={} share_direct={} netdisk_dir={:?}",
            task_id, raw_path, sync_is_share_direct, transfer_netdisk_dir
        );
        Ok(task_id)
    }

    async fn wait_transfer_task(
        &self,
        task_id: &str,
        require_download_completion: bool,
        timeout: Duration,
    ) -> Result<(), ShareSyncError> {
        let tm = self.transfer_manager();
        let idle_timeout = share_sync_task_idle_timeout(timeout);
        let hard_timeout = share_sync_task_hard_timeout();
        let stall_retry_after = share_sync_stall_retry_after(idle_timeout);
        let stall_retry_max = share_sync_stall_retry_max();
        let stall_retry_cooldown = share_sync_stall_retry_cooldown();
        let started_at = tokio::time::Instant::now();
        let mut last_activity_at = started_at;
        let mut last_stall_retry_at: Option<tokio::time::Instant> = None;
        let mut stall_retry_attempts = 0u32;
        let mut last_signature: Option<TaskActivitySignature> = None;
        loop {
            let task = tm.get_task(task_id).await.ok_or_else(|| {
                ShareSyncError::TransferError(format!("转存任务不存在: {}", task_id))
            })?;

            match &task.status {
                TransferStatus::Completed => return Ok(()),
                TransferStatus::Transferred if !require_download_completion => return Ok(()),
                TransferStatus::Transferred => {
                    return Err(ShareSyncError::DownloadError(format!(
                        "转存已完成但自动下载未完成或未创建: task_id={}",
                        task_id
                    )))
                }
                TransferStatus::TransferFailed => {
                    return Err(ShareSyncError::TransferError(
                        task.error.unwrap_or_else(|| "转存失败".into()),
                    ))
                }
                TransferStatus::DownloadFailed => {
                    return Err(ShareSyncError::DownloadError(
                        task.error.unwrap_or_else(|| "下载失败".into()),
                    ))
                }
                _ => {}
            }

            let subtasks = if require_download_completion {
                collect_share_sync_subtasks(&tm, &self.subscription_id, self.owner_uid).await
            } else {
                Vec::new()
            };
            let signature = task_activity_signature(&task, &subtasks);
            if last_signature.as_ref() != Some(&signature) {
                last_activity_at = tokio::time::Instant::now();
                last_signature = Some(signature);
            }

            let now = tokio::time::Instant::now();
            if let Some(hard_timeout) = hard_timeout {
                if now.duration_since(started_at) >= hard_timeout {
                    let msg = format!(
                        "等待任务完成超过硬上限: task_id={}, status={:?}, elapsed_secs={}, hard_timeout_secs={}",
                        task_id,
                        task.status,
                        now.duration_since(started_at).as_secs(),
                        hard_timeout.as_secs()
                    );
                    return if require_download_completion {
                        Err(ShareSyncError::DownloadError(msg))
                    } else {
                        Err(ShareSyncError::TransferError(msg))
                    };
                }
            }

            let idle_for = now.duration_since(last_activity_at);
            if require_download_completion {
                if let Some(retry_after) = stall_retry_after {
                    let retry_due = idle_for >= retry_after
                        && stall_retry_attempts < stall_retry_max
                        && last_stall_retry_at
                        .map(|last| now.duration_since(last) >= retry_after)
                        .unwrap_or(true);
                    if retry_due {
                        last_stall_retry_at = Some(now);
                        let restarted = restart_stalled_share_sync_downloads(
                            &tm,
                            &subtasks,
                            stall_retry_cooldown,
                        )
                            .await;
                        if restarted > 0 {
                            stall_retry_attempts += 1;
                            last_activity_at = tokio::time::Instant::now();
                            warn!(
                                "share-sync: 下载子任务长时间无进度, 已尝试暂停后继续: task_id={}, attempt={}/{}, restarted={}, idle_secs={}, retry_after_secs={}",
                                task_id,
                                stall_retry_attempts,
                                stall_retry_max,
                                restarted,
                                idle_for.as_secs(),
                                retry_after.as_secs()
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }

            if idle_for >= idle_timeout {
                let msg = format!(
                    "等待任务完成超时: task_id={}, status={:?}, idle_secs={}, idle_timeout_secs={}",
                    task_id,
                    task.status,
                    idle_for.as_secs(),
                    idle_timeout.as_secs()
                );
                return if require_download_completion {
                    Err(ShareSyncError::DownloadError(msg))
                } else {
                    Err(ShareSyncError::TransferError(msg))
                };
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn delete_netdisk(
        &self,
        target_path: &str,
        relative_paths: &[String],
    ) -> Result<(), ShareSyncError> {
        let paths: Vec<String> = relative_paths
            .iter()
            .map(|p| normalize_netdisk_path(p))
            .collect();
        let resp = self
            .netdisk
            .delete_files(&paths)
            .await
            .map_err(|e| ShareSyncError::TransferError(format!("网盘删除失败: {}", e)))?;
        if resp.success {
            info!(
                "share-sync: netdisk delete 成功 {}/{} from {}",
                resp.deleted_count,
                paths.len(),
                target_path
            );
            Ok(())
        } else {
            Err(ShareSyncError::TransferError(format!(
                "网盘删除失败: {}; failed_paths={:?}",
                resp.error.unwrap_or_else(|| "未知错误".into()),
                resp.failed_paths
            )))
        }
    }

    fn delete_local(&self, local_dir: &Path, relative_path: &str) -> Result<(), ShareSyncError> {
        let full = local_dir.join(relative_path.trim_start_matches('/'));
        match std::fs::remove_file(&full) {
            Ok(()) => {
                info!("share-sync: local delete 成功 {:?}", full);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ShareSyncError::FileSystemError(e.to_string())),
        }
    }

    // ============================================================
    // v1 新增：整批 submit
    // ============================================================
    //
    // 把 items 整组打包成 `selected_files` + `selected_fs_ids`，
    // 单次 `TransferManager.create_task` 调用，由 transfer 内部
    // `group_files_by_parent_dir`（`transfer/manager.rs:4349`）按父目录
    // 分 batch 转存到 target_dir/<parent>。
    //
    // 与单文件 `submit_transfer` 的关键区别：
    // - **一次 access_share_page + 鉴权**（百度的 /share/list 鉴权每条 fs_id 都要走）
    // - **子目录结构在 target_dir 下还原**（百度服务端在 group_target_dir 不存在时
    //   自动创建，见 `transfer/manager.rs:1001-1043` 的 `ensure_dirs_exist` + errno=2
    //   重试逻辑）
    // - **任务数从 N 降到 1**：大目录（500 文件）从 500 个 transfer 任务变成 1 个
    //
    // 失败语义：整组任一文件失败 → 整组视为失败（v1 简化）。executor 在
    // `apply_with_run_id_grouped` 里检测到 Quota / LocalDiskFull 早停类别时
    // 还会再细粒度地把"未提交"项标 Skipped。

    async fn submit_transfer_batch(
        &self,
        captured: &CapturedShare,
        target_dir: &str,
        items: &[ShareSnapshotItem],
        internal_label: Option<&str>,
    ) -> Result<String, ShareSyncError> {
        if items.is_empty() {
            return Err(ShareSyncError::Internal(
                "submit_transfer_batch 被传入空 items 列表".to_string(),
            ));
        }
        // v2 阶段 6:全局风控限速器
        self.rate_limiter.acquire().await;
        let tm = self.transfer_manager();
        use crate::transfer::manager::CreateTransferRequest;
        use crate::transfer::types::SharedFileInfo;

        let selected_files: Vec<SharedFileInfo> = items
            .iter()
            .map(|item| SharedFileInfo {
                fs_id: item.fs_id,
                is_dir: item.is_dir,
                path: item.path.clone(),
                size: item.size,
                name: item.name.clone(),
            })
            .collect();
        let selected_fs_ids: Vec<u64> = items.iter().map(|i| i.fs_id).collect();

        let req = CreateTransferRequest {
            share_url: share_url_for_captured(captured),
            password: captured.password.clone(),
            randsk: captured.randsk.clone(),
            prefetched_share: Some(prefetched_share_for_captured(captured)),
            save_path: target_dir.to_string(),
            save_fs_id: 0,
            // 网盘目标不下载本地，与单文件版本一致
            auto_download: Some(false),
            local_download_path: None,
            is_share_direct_download: false,
            download_conflict_strategy: None,
            selected_fs_ids: Some(selected_fs_ids),
            selected_files: Some(selected_files),
            owner_uid_override: Some(Uid::new(self.owner_uid)),
            // 分享同步内部任务：从「转存管理」隐藏 + 归属 share-sync:{订阅id}
            // （下载段据此走自动备份同款 create_backup_task：隐藏 + 优先级 + 归属）。
            is_internal: true,
            backup_config_id: Some(self.share_sync_backup_config_id()),
        };
        let resp = tm
            .create_task(req)
            .await
            .map_err(|e| ShareSyncError::TransferError(e.to_string()))?;
        if resp.need_password {
            return Err(ShareSyncError::ShareLinkError("需要提取码".into()));
        }
        if let Some(err) = resp.error {
            return Err(ShareSyncError::TransferError(err));
        }
        let task_id = resp
            .task_id
            .ok_or_else(|| ShareSyncError::TransferError("TransferManager 未返回任务 ID".into()))?;
        info!(
            "share-sync: batch transfer submitted label={:?} task_id={} target_dir={} items={}",
            internal_label,
            task_id,
            target_dir,
            items.len()
        );
        Ok(task_id)
    }

    async fn submit_download_batch(
        &self,
        items: &[ShareSnapshotItem],
        local_dir: &Path,
        strategy: ConflictStrategy,
        transfer_netdisk_dir: Option<&str>,
    ) -> Result<String, ShareSyncError> {
        if items.is_empty() {
            return Err(ShareSyncError::Internal(
                "submit_download_batch 被传入空 items 列表".to_string(),
            ));
        }
        // v2 阶段 6:全局风控限速器
        self.rate_limiter.acquire().await;
        // 本地同步模式分流（batch）：见 submit_download 单文件版说明。
        let (sync_save_path, sync_local_download, sync_is_share_direct) = match transfer_netdisk_dir
        {
            Some(netdisk_dir) => (
                netdisk_dir.to_string(),
                local_dir.to_string_lossy().to_string(),
                false,
            ),
            None => (String::new(), local_dir.to_string_lossy().to_string(), true),
        };
        let tm = self.transfer_manager();
        use crate::transfer::manager::CreateTransferRequest;
        use crate::transfer::types::SharedFileInfo;

        let selected_files: Vec<SharedFileInfo> = items
            .iter()
            .map(|item| {
                // 保留子目录信息：path 用 item.path，让 transfer 内部
                // group_files_by_parent_dir 按 item.path 的父目录分 batch，
                // 最终落 local_dir/<item.path>
                let raw_path = if item.raw_path.trim().is_empty() {
                    item.path.clone()
                } else {
                    item.raw_path.clone()
                };
                SharedFileInfo {
                    fs_id: item.fs_id,
                    is_dir: item.is_dir,
                    path: raw_path,
                    size: item.size,
                    name: item.name.clone(),
                }
            })
            .collect();
        let selected_fs_ids: Vec<u64> = items.iter().map(|i| i.fs_id).collect();

        let req = CreateTransferRequest {
            share_url: share_url_for_captured(&self.captured),
            password: self.captured.password.clone(),
            randsk: self.captured.randsk.clone(),
            prefetched_share: Some(prefetched_share_for_captured(&self.captured)),
            // 走 is_share_direct_download=true 路径，save_path 在 transfer 里
            // 会被 temp_dir 强制覆盖——这是 transfer 的硬编码行为，不在 share-sync
            // 控制范围。最终落点是 `local_download_path`（自动下载阶段被消费）。
            save_path: sync_save_path,
            save_fs_id: 0,
            auto_download: Some(true),
            local_download_path: Some(sync_local_download),
            is_share_direct_download: sync_is_share_direct,
            download_conflict_strategy: Some(download_conflict_strategy_for_share_sync(strategy)),
            selected_fs_ids: Some(selected_fs_ids),
            selected_files: Some(selected_files),
            owner_uid_override: Some(Uid::new(self.owner_uid)),
            // 分享同步内部任务：从「转存管理」隐藏 + 归属 share-sync:{订阅id}
            // （下载段据此走自动备份同款 create_backup_task：隐藏 + 优先级 + 归属）。
            is_internal: true,
            backup_config_id: Some(self.share_sync_backup_config_id()),
        };

        let resp = tm
            .create_task(req)
            .await
            .map_err(|e| ShareSyncError::DownloadError(e.to_string()))?;
        if resp.need_password {
            return Err(ShareSyncError::ShareLinkError("需要提取码".into()));
        }
        if let Some(err) = resp.error {
            return Err(ShareSyncError::DownloadError(err));
        }
        let task_id = resp
            .task_id
            .ok_or_else(|| ShareSyncError::DownloadError("TransferManager 未返回任务 ID".into()))?;
        info!(
            "share-sync: batch share-direct download submitted task_id={} local_dir={:?} items={}",
            task_id,
            local_dir,
            items.len()
        );
        Ok(task_id)
    }
}

impl ProductionHooks {
    fn transfer_manager(&self) -> Arc<TransferManager> {
        self.transfer.clone()
    }
}

/// 用已捕获的分享上下文构造 `SharePageInfo`，让 `create_task` 跳过逐批
/// `access_share_page`（大目录二分拆批降频、规避风控）。
fn prefetched_share_for_captured(
    captured: &CapturedShare,
) -> crate::transfer::types::SharePageInfo {
    crate::transfer::types::SharePageInfo {
        shareid: captured.shareid.clone(),
        uk: captured.uk.clone(),
        share_uk: captured.share_uk.clone(),
        bdstoken: captured.bdstoken.clone(),
    }
}

fn share_url_for_captured(captured: &CapturedShare) -> String {
    match captured.password.as_deref().filter(|p| !p.is_empty()) {
        Some(pwd) => format!("https://pan.baidu.com/s/{}?pwd={}", captured.short_key, pwd),
        None => format!("https://pan.baidu.com/s/{}", captured.short_key),
    }
}

fn download_conflict_strategy_for_share_sync(
    strategy: ConflictStrategy,
) -> crate::uploader::conflict::DownloadConflictStrategy {
    match strategy {
        ConflictStrategy::Overwrite => {
            crate::uploader::conflict::DownloadConflictStrategy::Overwrite
        }
        ConflictStrategy::Versioned => {
            crate::uploader::conflict::DownloadConflictStrategy::Overwrite
        }
        ConflictStrategy::Skip => crate::uploader::conflict::DownloadConflictStrategy::Skip,
    }
}

fn share_direct_download_root(
    local_dir: &Path,
    item: &ShareSnapshotItem,
) -> Result<PathBuf, ShareSyncError> {
    // Keep the path traversal guard in share-sync, but do not pre-append item.parent().
    // TransferManager restores the relative parent under local_download_path after
    // the temporary share-direct transfer completes.
    let _ = safe_relative_download_path(&item.path)?;
    Ok(local_dir.to_path_buf())
}

fn safe_relative_download_path(path: &str) -> Result<String, ShareSyncError> {
    let normalized = path.trim().replace('\\', "/");
    let trimmed = normalized.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(ShareSyncError::ConfigError("本地下载相对路径为空".into()));
    }

    let mut parts = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(ShareSyncError::ConfigError(format!(
                "非法同步路径（包含 ..）: {}",
                path
            )));
        }
        parts.push(part);
    }

    if parts.is_empty() {
        Err(ShareSyncError::ConfigError("本地下载相对路径为空".into()))
    } else {
        Ok(parts.join("/"))
    }
}

fn normalize_netdisk_path(path: &str) -> String {
    let replaced = path.trim().replace('\\', "/");
    let prefixed = if replaced.starts_with('/') {
        replaced
    } else {
        format!("/{}", replaced)
    };
    let mut collapsed = String::with_capacity(prefixed.len());
    let mut prev_slash = false;
    for ch in prefixed.chars() {
        if ch == '/' {
            if !prev_slash {
                collapsed.push(ch);
            }
            prev_slash = true;
        } else {
            collapsed.push(ch);
            prev_slash = false;
        }
    }
    if collapsed.len() > 1 {
        collapsed.trim_end_matches('/').to_string()
    } else {
        collapsed
    }
}

fn parent_netdisk_dir(path: &str) -> String {
    let normalized = normalize_netdisk_path(path);
    if normalized == "/" {
        return "/".to_string();
    }
    match normalized.rsplit_once('/') {
        Some(("", _)) => "/".to_string(),
        Some((parent, _)) if parent.is_empty() => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
        None => "/".to_string(),
    }
}

fn basename_netdisk_path(path: &str) -> String {
    normalize_netdisk_path(path)
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

fn join_netdisk_path(base: &str, name: &str) -> String {
    let base = normalize_netdisk_path(base);
    let name = name.trim_start_matches('/');
    if base == "/" {
        format!("/{}", name)
    } else if name.is_empty() {
        base
    } else {
        format!("{}/{}", base, name)
    }
}

fn is_netdisk_not_found_error(msg: &str) -> bool {
    msg.contains("API error 2")
        || msg.contains("errno=2")
        || msg.contains("errno 2")
        || msg.contains("路径不存在")
        || msg.contains("文件不存在")
}

fn augment_diff_with_local_target_state(
    sub: &ShareSubscription,
    prev: Option<&ShareSnapshot>,
    curr: &ShareSnapshot,
    diff: &mut ShareDiff,
) -> Result<(), ShareSyncError> {
    let local_roots: Vec<&Path> = sub
        .targets
        .iter()
        .filter_map(|target| match target {
            SyncTarget::Local(t) => Some(t.local_path.as_path()),
            SyncTarget::Netdisk(_) => None,
        })
        .collect();
    if local_roots.is_empty() {
        return Ok(());
    }

    let prev_map = prev.map(|snap| snap.index_by_path()).unwrap_or_default();
    let mut action_paths: BTreeSet<String> = diff
        .added
        .iter()
        .map(|item| item.path.clone())
        .chain(diff.modified.iter().map(|item| item.new.path.clone()))
        .chain(diff.removed.iter().map(|item| item.path.clone()))
        .collect();

    let mut repaired = 0usize;
    for item in curr.items.iter().filter(|item| !item.is_dir) {
        if action_paths.contains(&item.path) {
            continue;
        }

        let relative = safe_relative_download_path(&item.path)?;
        let needs_repair = local_roots.iter().any(|root| {
            let local_path = root.join(&relative);
            match std::fs::metadata(&local_path) {
                Ok(meta) => !meta.is_file() || meta.len() != item.size,
                Err(_) => true,
            }
        });

        if !needs_repair {
            continue;
        }

        let old = prev_map
            .get(&item.path)
            .map(|item| (**item).clone())
            .unwrap_or_else(|| item.clone());
        diff.modified.push(ShareModifiedItem {
            old,
            new: item.clone(),
        });
        action_paths.insert(item.path.clone());
        diff.unchanged_count = diff.unchanged_count.saturating_sub(1);
        repaired += 1;
    }

    if repaired > 0 {
        diff.modified.sort_by(|a, b| a.old.path.cmp(&b.old.path));
        info!(
            "share-sync: 本地目标校验发现 {} 个缺失/大小不一致文件，已纳入 modified diff: subscription={}",
            repaired, sub.id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::config::{LocalTarget, NetdiskTarget, SyncTarget};
    use crate::share_sync::events::NoopShareSyncEventPublisher;
    use crate::share_sync::resolver::StaticAccountResolver;
    use tempfile::tempdir;

    fn sub(name: &str) -> ShareSubscription {
        ShareSubscription::new(
            name.into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: std::env::temp_dir(),
                conflict_strategy: None,
                mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
            })],
        )
    }

    #[test]
    fn test_prefetched_share_for_captured_maps_all_fields() {
        let captured = CapturedShare {
            short_key: "1abc".into(),
            shareid: "sid".into(),
            uk: "uk-1".into(),
            share_uk: "share-uk-2".into(),
            bdstoken: "tok".into(),
            password: Some("pwd".into()),
            randsk: Some("rsk".into()),
        };
        let info = prefetched_share_for_captured(&captured);
        assert_eq!(info.shareid, "sid");
        assert_eq!(info.uk, "uk-1");
        // share_uk 必须取 access_share_page 返回的 share_uk（转存接口用），
        // 不能误用 uk —— 二者在部分分享场景下不同。
        assert_eq!(info.share_uk, "share-uk-2");
        assert_eq!(info.bdstoken, "tok");
    }

    #[tokio::test]
    async fn test_new_manager_empty() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        assert_eq!(m.list_subscriptions().len(), 0);
    }

    #[tokio::test]
    async fn test_create_get_delete() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let s = m.create_subscription(sub("a")).unwrap();
        assert_eq!(m.list_subscriptions().len(), 1);
        assert!(m.get_subscription(&s.id).is_some());

        // DB 为唯一可信源：不再写 JSON（已移除 JSON 双写）
        let json_path = dir.path().join("subs.json");
        assert!(!json_path.exists());

        m.delete_subscription(&s.id).await.unwrap();
        assert_eq!(m.list_subscriptions().len(), 0);
    }

    #[tokio::test]
    async fn test_list_for_owner_isolates_accounts() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();

        let mut a = sub("a");
        a.owner_uid = 1;
        let mut b = sub("b");
        b.owner_uid = 2;
        m.create_subscription(a).unwrap();
        m.create_subscription(b).unwrap();

        // 账号 1 只看见自己的订阅，看不见账号 2 的
        let owner1 = m.list_for_owner(1);
        assert_eq!(owner1.len(), 1);
        assert_eq!(owner1[0].name, "a");
        assert!(owner1.iter().all(|s| s.owner_uid == 1));

        let owner2 = m.list_for_owner(2);
        assert_eq!(owner2.len(), 1);
        assert_eq!(owner2[0].name, "b");

        // 未知账号看不到任何订阅
        assert_eq!(m.list_for_owner(999).len(), 0);
    }

    #[tokio::test]
    async fn test_update_subscription_preserves_id_and_created_at() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let s = m.create_subscription(sub("a")).unwrap();
        let original_created = s.created_at;

        let mut updated = s.clone();
        updated.name = "renamed".into();
        let back = m.update_subscription(&s.id, updated).unwrap();
        assert_eq!(back.id, s.id);
        assert_eq!(back.created_at, original_created);
        assert_eq!(back.name, "renamed");
    }

    #[tokio::test]
    async fn test_set_enabled_persists_state() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let s = m.create_subscription(sub("a")).unwrap();
        m.set_enabled(&s.id, false).unwrap();
        assert!(!m.get_subscription(&s.id).unwrap().enabled);
        m.set_enabled(&s.id, true).unwrap();
        assert!(m.get_subscription(&s.id).unwrap().enabled);
    }

    #[test]
    fn test_snapshot_baseline_only_advances_after_clean_success() {
        assert!(should_advance_snapshot_baseline(RunStatus::Completed));
        assert!(!should_advance_snapshot_baseline(
            RunStatus::CompletedWithErrors
        ));
        assert!(!should_advance_snapshot_baseline(RunStatus::Failed));
        assert!(!should_advance_snapshot_baseline(RunStatus::Running));
    }

    #[test]
    fn test_share_sync_download_conflict_strategy_mapping() {
        use crate::uploader::conflict::DownloadConflictStrategy;

        assert_eq!(
            download_conflict_strategy_for_share_sync(ConflictStrategy::Overwrite),
            DownloadConflictStrategy::Overwrite
        );
        assert_eq!(
            download_conflict_strategy_for_share_sync(ConflictStrategy::Versioned),
            DownloadConflictStrategy::Overwrite
        );
        assert_eq!(
            download_conflict_strategy_for_share_sync(ConflictStrategy::Skip),
            DownloadConflictStrategy::Skip
        );
    }

    #[test]
    fn test_is_terminal_subtask_status() {
        // 终态：完成/成功/各类失败/取消 → 不应出现在「进行中子任务」
        for s in [
            "completed",
            "success",
            "failed",
            "cancelled",
            "transferfailed",
            "downloadfailed",
        ] {
            assert!(is_terminal_subtask_status(s), "{s} 应判终态");
        }
        // 非终态：仍在进行 → 保留
        for s in [
            "pending",
            "scanning",
            "downloading",
            "transferring",
            "transferred",
            "waiting_transfer",
            "paused",
        ] {
            assert!(!is_terminal_subtask_status(s), "{s} 不应判终态");
        }
    }

    fn wait_signature_task() -> crate::transfer::task::TransferTask {
        let mut task = crate::transfer::task::TransferTask::new(
            "https://pan.baidu.com/s/1abc".into(),
            None,
            "/target".into(),
            0,
            true,
            Some("/downloads".into()),
        );
        task.status = TransferStatus::Downloading;
        task.download_task_ids = vec!["dl-1".into()];
        task.updated_at = 1;
        task
    }

    fn wait_signature_subtask(downloaded: u64, speed: u64) -> ShareSyncSubtask {
        let total = 100;
        ShareSyncSubtask {
            task_id: "dl-1".into(),
            name: "large.bin".into(),
            kind: "download".into(),
            status: "downloading".into(),
            downloaded,
            total,
            progress: downloaded as f64 / total as f64 * 100.0,
            speed,
            eta_seconds: None,
            owner_uid: 1,
        }
    }

    #[test]
    fn test_restartable_share_sync_download_detects_active_downloads() {
        let task = wait_signature_subtask(10, 1024);
        assert_eq!(
            restartable_share_sync_download(&task),
            Some(RestartableShareSyncDownload::Task("dl-1".into()))
        );

        let mut folder = task.clone();
        folder.task_id = "folder:folder-1".into();
        assert_eq!(
            restartable_share_sync_download(&folder),
            Some(RestartableShareSyncDownload::Folder("folder-1".into()))
        );

        folder.status = "scanning".into();
        assert_eq!(
            restartable_share_sync_download(&folder),
            Some(RestartableShareSyncDownload::Folder("folder-1".into()))
        );
    }

    #[test]
    fn test_restartable_share_sync_download_skips_terminal_and_non_running_tasks() {
        let mut subtask = wait_signature_subtask(10, 1024);

        subtask.kind = "transfer".into();
        assert_eq!(restartable_share_sync_download(&subtask), None);

        subtask.kind = "download".into();
        for status in ["completed", "failed", "cancelled", "paused", "pending"] {
            subtask.status = status.into();
            assert_eq!(
                restartable_share_sync_download(&subtask),
                None,
                "status {status} 不应自动暂停/继续"
            );
        }
    }

    #[test]
    fn test_task_activity_signature_tracks_downloaded_bytes() {
        let task = wait_signature_task();
        let a = task_activity_signature(&task, &[wait_signature_subtask(10, 1024)]);
        let b = task_activity_signature(&task, &[wait_signature_subtask(20, 1024)]);
        assert_ne!(a, b, "下载字节增长应刷新等待活动指纹");
    }

    #[test]
    fn test_task_activity_signature_ignores_speed_only_noise() {
        let task = wait_signature_task();
        let a = task_activity_signature(&task, &[wait_signature_subtask(10, 1024)]);
        let b = task_activity_signature(&task, &[wait_signature_subtask(10, 4096)]);
        assert_eq!(a, b, "只有速度抖动不应重置空闲超时");
    }

    // ===== execution_diff_with_directory_ancestors：整目录转存只在「整目录全新」时启用 =====

    fn ss_file(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }
    fn ss_dir(path: &str, fs_id: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, 0, true)
    }

    #[test]
    fn test_exec_diff_promotes_dir_only_when_subtree_fully_new() {
        // 首次同步：/d 整目录全新（2 个文件 + 目录项都在 added）→ 应保留/补成整目录转存。
        let curr = ShareSnapshot::with_items(
            "sub",
            vec![
                ss_dir("/d", 100),
                ss_file("/d/a", 1, 10),
                ss_file("/d/b", 2, 20),
            ],
        );
        let diff = diff_snapshots(None, &curr); // prev=None → 全部 added（含目录项 /d）
        let (out, _added) = execution_diff_with_directory_ancestors(&diff, &curr);
        // /d 已在 added 里（目录项），整目录转存可用。
        assert!(out.added.iter().any(|i| i.path == "/d" && i.is_dir));
    }

    #[test]
    fn test_exec_diff_does_not_promote_partially_changed_dir() {
        // 增量：/d 已同步，仅新增 /d/c。/d 本身未变（不在 diff），不应被补成整目录转存，
        // 否则会把未变动的 /d/a、/d/b 也整目录重下。
        let prev = ShareSnapshot::with_items(
            "sub",
            vec![
                ss_dir("/d", 100),
                ss_file("/d/a", 1, 10),
                ss_file("/d/b", 2, 20),
            ],
        );
        let curr = ShareSnapshot::with_items(
            "sub",
            vec![
                ss_dir("/d", 100),
                ss_file("/d/a", 1, 10),
                ss_file("/d/b", 2, 20),
                ss_file("/d/c", 3, 30),
            ],
        );
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].path, "/d/c");

        let (out, added) = execution_diff_with_directory_ancestors(&diff, &curr);
        // 不补 /d（含未变动文件），added 集合里不应出现目录 /d。
        assert_eq!(added, 0, "含未变动文件的目录不应被补成整目录转存");
        assert!(!out.added.iter().any(|i| i.path == "/d"));
        // 仍只携带变动的那个文件。
        assert_eq!(out.added.len(), 1);
        assert_eq!(out.added[0].path, "/d/c");
    }

    #[test]
    fn test_dir_subtree_fully_changed_predicate() {
        let curr = ShareSnapshot::with_items(
            "sub",
            vec![
                ss_dir("/d", 100),
                ss_file("/d/a", 1, 10),
                ss_file("/d/sub/x", 4, 40),
            ],
        );
        let idx = curr.index_by_path();

        // 全部子文件都变 → true
        let all: BTreeSet<String> = ["/d/a".to_string(), "/d/sub/x".to_string()]
            .into_iter()
            .collect();
        assert!(dir_subtree_fully_changed(&idx, "/d", &all));

        // 只变一部分（缺 /d/sub/x） → /d 整体 false
        let some: BTreeSet<String> = ["/d/a".to_string()].into_iter().collect();
        assert!(!dir_subtree_fully_changed(&idx, "/d", &some));

        // 嵌套子目录视角：/d/sub 的全部文件(/d/sub/x)都变 → true
        let sub_only: BTreeSet<String> = ["/d/sub/x".to_string()].into_iter().collect();
        assert!(dir_subtree_fully_changed(&idx, "/d/sub", &sub_only));

        // 空集合 / 无子文件 → false（不误判为全新）
        let none: BTreeSet<String> = BTreeSet::new();
        assert!(!dir_subtree_fully_changed(&idx, "/d", &none));
    }

    #[test]
    fn test_share_direct_download_root_avoids_duplicate_parent_dir() {
        let item =
            ShareSnapshotItem::new("/monthly/000009.SZ.csv", "000009.SZ.csv", 9, 1024, false);
        let target_root = PathBuf::from("/home/hyx/codespace/one-family/data");

        let download_root = share_direct_download_root(&target_root, &item).unwrap();
        assert_eq!(download_root, target_root);

        let transfer_restored_path =
            download_root.join(safe_relative_download_path(&item.path).unwrap());
        assert_eq!(
            transfer_restored_path,
            PathBuf::from("/home/hyx/codespace/one-family/data/monthly/000009.SZ.csv")
        );
        assert_ne!(
            transfer_restored_path,
            PathBuf::from("/home/hyx/codespace/one-family/data/monthly/monthly/000009.SZ.csv")
        );
    }

    #[test]
    fn test_local_missing_file_is_promoted_to_modified_diff() {
        let dir = tempdir().unwrap();
        let sub = ShareSubscription::new(
            "local".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: dir.path().to_path_buf(),
                conflict_strategy: None,
                mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
            })],
        );
        let items = vec![
            ShareSnapshotItem::new("/a.csv", "a.csv", 1, 3, false),
            ShareSnapshotItem::new("/b.csv", "b.csv", 2, 4, false),
        ];
        std::fs::write(dir.path().join("a.csv"), b"abc").unwrap();
        let prev = ShareSnapshot::with_items(&sub.id, items.clone());
        let curr = ShareSnapshot::with_items(&sub.id, items);
        let mut diff = diff_snapshots(Some(&prev), &curr);

        augment_diff_with_local_target_state(&sub, Some(&prev), &curr, &mut diff).unwrap();

        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].new.path, "/b.csv");
        assert_eq!(diff.unchanged_count, 1);
    }

    #[test]
    fn test_local_size_mismatch_is_promoted_to_modified_diff() {
        let dir = tempdir().unwrap();
        let sub = ShareSubscription::new(
            "local".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: dir.path().to_path_buf(),
                conflict_strategy: None,
                mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
            })],
        );
        let item = ShareSnapshotItem::new("/nested/a.csv", "a.csv", 1, 4, false);
        std::fs::create_dir_all(dir.path().join("nested")).unwrap();
        std::fs::write(dir.path().join("nested/a.csv"), b"abc").unwrap();
        let prev = ShareSnapshot::with_items(&sub.id, vec![item.clone()]);
        let curr = ShareSnapshot::with_items(&sub.id, vec![item]);
        let mut diff = diff_snapshots(Some(&prev), &curr);

        augment_diff_with_local_target_state(&sub, Some(&prev), &curr, &mut diff).unwrap();

        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].new.path, "/nested/a.csv");
        assert_eq!(diff.unchanged_count, 0);
    }

    #[test]
    fn test_netdisk_only_target_does_not_use_local_filesystem_diff() {
        let sub = ShareSubscription::new(
            "netdisk".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/backup".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        let item =
            ShareSnapshotItem::new("/missing-locally.csv", "missing-locally.csv", 1, 4, false);
        let prev = ShareSnapshot::with_items(&sub.id, vec![item.clone()]);
        let curr = ShareSnapshot::with_items(&sub.id, vec![item]);
        let mut diff = diff_snapshots(Some(&prev), &curr);

        augment_diff_with_local_target_state(&sub, Some(&prev), &curr, &mut diff).unwrap();

        assert!(diff.modified.is_empty());
        assert_eq!(diff.unchanged_count, 1);
    }

    #[tokio::test]
    async fn test_create_invalid_subscription_rejected() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let mut bad = sub("a");
        bad.share_url = "https://example.com".into();
        let r = m.create_subscription(bad);
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_recovery_from_json_on_startup() {
        let dir = tempdir().unwrap();
        // 预写一个 JSON
        let json_path = dir.path().join("subs.json");
        let s = sub("preloaded");
        let all = vec![s.clone()];
        std::fs::write(&json_path, serde_json::to_string(&all).unwrap()).unwrap();

        let m = ShareSyncManager::new(ManagerConfig {
            config_path: json_path,
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let list = m.list_subscriptions();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "preloaded");
    }

    #[tokio::test]
    async fn test_trigger_one_when_not_logged_in_fails() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let s = m.create_subscription(sub("a")).unwrap();
        // netdisk_client 为 None → 应报错
        let r = m.execute_one(&s.id).await;
        assert!(r.is_err());
        let r = m.trigger_one(&s.id).await;
        assert!(matches!(r, Err(ShareSyncError::ConfigError(_))));
    }

    #[tokio::test]
    async fn test_trigger_one_when_already_running_fails_fast() {
        let dir = tempdir().unwrap();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            resolver: Arc::new(StaticAccountResolver::none()),
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
            .await
            .unwrap();
        let mut sub = sub("a");
        sub.owner_uid = 1;
        let s = m.create_subscription(sub).unwrap();
        m.running.insert(s.id.clone(), ());
        let r = m.trigger_one(&s.id).await;
        m.running.remove(&s.id);
        assert!(matches!(r, Err(ShareSyncError::AlreadyRunning(_))));
    }
}
