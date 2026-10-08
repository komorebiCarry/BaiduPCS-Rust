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
use crate::share_sync::persistence::{
    ShareSyncPersistence, RUN_PHASE_DIFFING, RUN_PHASE_EXECUTING, RUN_PHASE_SCANNING,
};
use crate::share_sync::resolver::ShareSyncAccountResolver;
use crate::share_sync::scheduler::SubscriptionScheduler;
use crate::share_sync::snapshot::{
    CapturedShare, ScanCache, ScanProgressSink, ShareSnapshot, ShareSnapshotItem, SnapshotCollector,
};
use crate::share_sync::types::{ConflictStrategy, RunStatus};
use crate::transfer::{TransferManager, TransferStatus};
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
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

/// 每条订阅最多取几条历史 `interrupted` run 作为「续跑重试」候选。
///
/// 残留下载是按订阅（`backup_config_id`）归属的，最新那次尝试才是它们的主人；
/// 取 3 条而不是 1 条，是为了容忍「最新那条在抓取阶段就断了、根本没有 run_items，
/// 真正有活儿的是再往前一条」这种情况。
const STALE_INTERRUPTED_PER_SUB: usize = 3;

/// 启动续跑的待处理 run，按「本次收编」和「历史遗留」分开。
///
/// 两者的处理权限不同，混在一起会出事：
/// - `fresh`：本次启动刚从 `running` 收编的。进程上次退出时它确实在飞，
///   所以既可以原地接管，也可以在接管不了时**清理残留 + 重跑**。
/// - `stale`：上次启动就已经是 `interrupted` 的（说明上一轮的续跑没来得及把它标回
///   `running` 就又被杀了）。这些**只允许原地接管**：真有残留就接管，没残留就跳过。
///   若也允许重跑，那么任何一条历史 interrupted 记录都会变成「每次开机自动同步一次」，
///   用户没触发却凭空多出 run。
#[derive(Default)]
struct ResumeTarget {
    fresh: Vec<String>,
    stale: Vec<String>,
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
        // 被中断的 run：订阅 id -> 该订阅名下的待处理 run。
        // 需要 run_id 而不只是订阅 id —— 重跑之前要按 run 清掉候选快照，
        // 否则那些永远不会被提升的候选会一直留在库里。
        let mut interrupted_runs: BTreeMap<String, ResumeTarget> = BTreeMap::new();
        if stale_fixup_enabled {
            match persistence.mark_running_runs_interrupted() {
                Ok(interrupted) if !interrupted.is_empty() => {
                    info!(
                        "share_sync 启动自愈: 收编 {} 条中断 run,稍后自动重跑",
                        interrupted.len()
                    );
                    for rec in &interrupted {
                        interrupted_runs
                            .entry(rec.subscription_id.clone())
                            .or_default()
                            .fresh
                            .push(rec.run_id.clone());
                    }
                }
                Ok(_) => {}
                Err(e) => warn!("share_sync 启动自愈失败: {}", e),
            }

            // 再捞一遍**上次启动就已经是** `interrupted` 的 run。
            //
            // 上面那个查询只认 `status='running'`，一条 run 被标成 interrupted 后就
            // 再也捡不起来了。而「标记 interrupted」到 `resume_run_in_place` 把它标回
            // `running` 之间隔着续跑任务的初始延迟 + 等账号就绪（最长两分钟）——
            // 进程在这个窗口里再被杀一次（连着重启、改代码重编译时很常见），这条 run
            // 就永久卡死，它名下那批恢复成暂停态的下载再没人驱动，表现为「同步任务
            // 全是暂停的，重启多少次都不动」。
            //
            // 这些候选走**只接管、不重跑**的路径（见 `ResumeTarget::stale`）：真有残留
            // 就原地接管保住已下的分片，没残留就原样跳过，不会变成每次开机重跑老 run。
            match persistence.list_interrupted_runs(STALE_INTERRUPTED_PER_SUB) {
                Ok(stale) => {
                    let mut n = 0usize;
                    for rec in stale {
                        let entry = interrupted_runs
                            .entry(rec.subscription_id.clone())
                            .or_default();
                        // 本次刚收编的优先，别把同一条 run 重复排进来
                        if entry.fresh.contains(&rec.run_id) {
                            continue;
                        }
                        entry.stale.push(rec.run_id);
                        n += 1;
                    }
                    if n > 0 {
                        info!(
                            "share_sync 启动自愈: 另有 {} 条历史中断 run 待判定是否接管",
                            n
                        );
                    }
                }
                Err(e) => warn!("share_sync 启动自愈: 读取历史中断 run 失败: {}", e),
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
        if resume_on_startup && !interrupted_runs.is_empty() {
            let resume_targets: Vec<(String, ResumeTarget)> = interrupted_runs
                .into_iter()
                .filter(|(id, _)| {
                    manager
                        .subscriptions
                        .get(id)
                        .map(|s| s.enabled)
                        .unwrap_or(false)
                })
                .collect();
            if !resume_targets.is_empty() {
                let mgr = Arc::clone(&manager);
                tokio::spawn(async move {
                    mgr.resume_interrupted_runs(resume_targets).await;
                });
            }
        }

        Ok(manager)
    }

    /// 判断某条被中断的 run 能否「接着跑」而不是推倒重来。
    ///
    /// 三个条件缺一不可：
    /// 1. **候选快照还在** —— 没有它就无法在收尾时推进基线，续跑的活儿等于白干
    ///    （下一轮 diff 照样重做），不如直接重跑；
    /// 2. **run_items 里记着 transfer_task_id** —— 这是找回"上一轮在做什么"的唯一线索；
    /// 3. **那些转存任务已被恢复且仍是 `Downloading`** —— 说明源（网盘临时目录）还在、
    ///    下载监控 watcher 也已由 `TransferManager::restore_task` 重建。若它已是终态
    ///    或压根没恢复出来，说明这轮的活已经没法继续，只能重跑。
    ///
    /// 返回可续跑的转存任务 id 列表（去重）。任一条件不满足返回 `None`。
    async fn probe_resumable_run(
        &self,
        sub_id: &str,
        run_id: &str,
        owner_uid: u64,
    ) -> Option<Vec<String>> {
        // 1) 候选快照
        match self.persistence.snapshot_for_run(run_id) {
            Ok(Some(_)) => {}
            Ok(None) => {
                debug!("run {} 无候选快照，无法续跑（将清理后重跑）", run_id);
                return None;
            }
            Err(e) => {
                warn!("读取 run {} 的候选快照失败，按不可续跑处理: {}", run_id, e);
                return None;
            }
        }

        // 2) run_items 里的 transfer_task_id
        let items = match self.persistence.list_run_items(run_id) {
            Ok(v) => v,
            Err(e) => {
                warn!("读取 run {} 的 items 失败，按不可续跑处理: {}", run_id, e);
                return None;
            }
        };
        let mut task_ids: Vec<String> = items
            .iter()
            .filter(|it| !is_terminal_run_item_status(&it.status))
            .filter_map(|it| it.transfer_task_id.clone())
            .collect();
        task_ids.sort();
        task_ids.dedup();
        if task_ids.is_empty() {
            debug!("run {} 没有未完成且带转存任务的 item，无需续跑", run_id);
            return None;
        }

        // 3) 该订阅还有**没下完的下载**（文件夹段 / 单文件段）
        //
        // 注意这里看的是**下载**，不是转存任务。
        //
        // 转存任务一旦启动了自动下载就会被落盘成 completed 并移入历史库
        // （见 `transfer/manager.rs` 的「转存任务已标记完成（自动下载已启动）」），
        // 所以重启后 `restore_task` 根本不会把它恢复成活跃任务 —— 拿它当判据，
        // 分享直下这条路永远判不出"可续跑"。真正还在跑的活儿在文件夹下载 /
        // 下载子任务上，而它们是会被恢复的（恢复成暂停态）。
        //
        // `collect_share_sync_subtasks` 按 backup_config_id 查，同时覆盖两段，
        // 与前端「进行中子任务」用的是同一份数据。
        let tm = self.resolver.transfer_manager(owner_uid).await?;
        let unfinished = collect_share_sync_subtasks(&tm, sub_id, owner_uid)
            .await
            .into_iter()
            .filter(|s| s.kind == "download" && !is_terminal_subtask_status(&s.status))
            .count();
        if unfinished == 0 {
            debug!(
                "run {} 所属订阅没有未完成的下载，无需续跑（将清理后重跑）",
                run_id
            );
            return None;
        }
        debug!(
            "run {} 可续跑：订阅 {} 还有 {} 个未完成的下载子任务",
            run_id, sub_id, unfinished
        );
        Some(task_ids)
    }

    /// 接管一条可续跑的 run：唤醒残留下载 → 等转存任务跑完 → 给 run 收尾。
    ///
    /// 这是「重启续跑」的核心，替代原来的"推倒重来"：不再重新抓取/转存，而是让上一轮
    /// 已经转存好、正在下载的那批活儿接着做完，最后正常收尾并推进基线。
    ///
    /// 为什么需要它：`wait_transfer_task` 跑在 `execute_one` 的调用栈里，进程一重启
    /// 那个 future 就没了。转存任务本身会被 `TransferManager::restore_task` 恢复、
    /// 下载监控 watcher 也会重建，但**没有任何东西在等它、给 run 收尾** ——
    /// 于是 run 永远停在 Interrupted、基线永远不推进、下一轮全部重做。
    ///
    /// 期间占住该订阅的 in-flight 标记，避免调度器起一轮竞争的 run。
    async fn resume_run_in_place(
        self: Arc<Self>,
        sub_id: String,
        run_id: String,
        owner_uid: u64,
        transfer_task_ids: Vec<String>,
    ) {
        const POLL_INTERVAL_SECS: u64 = 2;
        // 唤醒残留下载的节流，与 wait_transfer_task 里的 paused-resume 同口径
        const RESUME_INTERVAL_SECS: u64 = 10;

        if self.running.insert(sub_id.clone(), ()).is_some() {
            debug!("续跑接管: 订阅 {} 已有 run 在执行，放弃接管", sub_id);
            return;
        }
        struct RunGuard<'g> {
            running: &'g DashMap<String, ()>,
            id: String,
        }
        impl Drop for RunGuard<'_> {
            fn drop(&mut self) {
                self.running.remove(&self.id);
            }
        }
        let _guard = RunGuard {
            running: &self.running,
            id: sub_id.clone(),
        };

        let Some(tm) = self.resolver.transfer_manager(owner_uid).await else {
            warn!("续跑接管: 订阅 {} 的转存管理器不可用，放弃接管", sub_id);
            return;
        };

        info!(
            "share_sync 续跑接管: 订阅 {} run={} 接管 {} 个转存任务，不再重跑",
            sub_id,
            run_id,
            transfer_task_ids.len()
        );
        // 🔥 接管即把 run 标回 `running`。
        //
        // `mark_running_runs_interrupted()` 只收编 `status = 'running'` 的 run。
        // 若接管后把状态留在 `interrupted`，进程再被杀一次，下次启动就找不到这条
        // run —— 不收编、不续跑，这批活儿直接变孤儿（实测踩到过：接管跑到一半被杀，
        // 重启后一条续跑日志都没有，前端全是暂停）。
        if let Err(e) = self.persistence.mark_run_running(&run_id) {
            warn!("续跑接管: 标记 run 为运行中失败 run_id={}, error={}", run_id, e);
        }
        let _ = self
            .persistence
            .update_run_phase(&run_id, RUN_PHASE_EXECUTING);

        // 🔥 续跑期间同样要开进度广播器（每秒推一帧 `item_progress`）。
        //
        // 正常 run 在 `execute_one_with_run_id` 里 spawn 了同一个广播器；接管路径漏掉
        // 的话，前端在整个续跑期间收不到任何推送，界面停在接管那一刻不动，
        // 只能靠切页面触发 REST 兜底拉取 —— 表现就是"速度不动，切菜单回来才刷新"。
        let progress_handle = {
            let publisher = Arc::clone(&self.publisher);
            let transfer = Arc::clone(&tm);
            let rid = run_id.clone();
            let sid = sub_id.clone();
            tokio::spawn(async move {
                broadcast_subtask_progress(publisher, transfer, rid, sid, owner_uid).await;
            })
        };
        // 无论从哪条分支退出（跑完 / 硬上限）都要停掉广播器，避免留下永不退出的后台任务。
        struct ProgressGuard(tokio::task::JoinHandle<()>);
        impl Drop for ProgressGuard {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _progress_guard = ProgressGuard(progress_handle);

        // 兜底上限：waiter 期间占着该订阅的 in-flight 标记，调度器无法再起新一轮。
        // 万一转存任务因为某种原因永远不到终态，没有上限就意味着这个订阅**永久停摆**。
        // 复用与正常 run 相同的硬上限（默认 7 天，`BAIDUPCS_SHARE_SYNC_TASK_HARD_TIMEOUT_SECS`）。
        let started_at = tokio::time::Instant::now();
        let hard_timeout = share_sync_task_hard_timeout();

        let mut last_resume_at: Option<tokio::time::Instant> = None;
        loop {
            if let Some(cap) = hard_timeout {
                if started_at.elapsed() >= cap {
                    warn!(
                        "share_sync 续跑接管: 订阅 {} run={} 超过硬上限 {}s 仍未结束，放弃接管并按失败收尾",
                        sub_id,
                        run_id,
                        cap.as_secs()
                    );
                    break;
                }
            }
            // 唤醒残留下载：重启后它们都是暂停态，没人拉起就永远不动。
            // 与 wait_transfer_task 同款闸门：槽位满时不硬唤醒（否则只是把自己挪到队尾）。
            let now = tokio::time::Instant::now();
            let resume_due = last_resume_at
                .map(|last| now.duration_since(last) >= Duration::from_secs(RESUME_INTERVAL_SECS))
                .unwrap_or(true);
            if resume_due {
                last_resume_at = Some(now);
                let has_slot = match tm.download_manager_handle().await {
                    Some(dm) => dm.task_slot_pool().available_slots().await > 0,
                    None => true,
                };
                if has_slot {
                    let paused: Vec<ShareSyncSubtask> =
                        collect_share_sync_subtasks(&tm, &sub_id, owner_uid)
                            .await
                            .into_iter()
                            .filter(|s| s.status == "paused")
                            .collect();
                    if !paused.is_empty() {
                        let n =
                            restart_stalled_share_sync_downloads(&tm, &paused, Duration::ZERO).await;
                        if n > 0 {
                            info!(
                                "share_sync 续跑接管: 订阅 {} 唤醒了 {} 个残留下载",
                                sub_id, n
                            );
                        }
                    }
                }
            }

            // 这批下载都跑完了吗？
            //
            // 等的是**下载**而不是转存任务：转存任务启动自动下载后就被落盘成
            // completed 并移入历史，重启后压根不会被恢复（见 `probe_resumable_run`
            // 的说明）。真正承载进度的是文件夹下载 / 下载子任务。
            let unfinished = collect_share_sync_subtasks(&tm, &sub_id, owner_uid)
                .await
                .into_iter()
                .filter(|s| s.kind == "download" && !is_terminal_subtask_status(&s.status))
                .count();
            if unfinished == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        }

        self.finalize_resumed_run(&sub_id, &run_id, owner_uid, &tm)
            .await;
    }

    /// 给续跑完成的 run 收尾：补齐 item 状态 → 汇总 run 状态 → 落库 → 推进/清理基线。
    async fn finalize_resumed_run(
        &self,
        sub_id: &str,
        run_id: &str,
        owner_uid: u64,
        tm: &TransferManager,
    ) {
        use crate::share_sync::types::{DiffSummary, RunItemStatus, RunStatus};

        // 1) 看这批下载有没有失败的 —— 判据与等待条件同源（都看下载，不看转存任务）。
        //
        // 收尾粒度是「整批」而不是逐个 item：run_item 记的是转存任务 id，而一个转存
        // 任务往往对应一整个文件夹下载，没法反查到单个文件的成败。这与现有语义一致
        // ——`should_advance_snapshot_baseline` 本来就是全或无。
        let download_failed = collect_share_sync_subtasks(tm, sub_id, owner_uid)
            .await
            .into_iter()
            .filter(|s| s.kind == "download")
            .any(|s| matches!(s.status.as_str(), "failed" | "cancelled" | "downloadfailed"));

        let items = self.persistence.list_run_items(run_id).unwrap_or_default();
        let mut failed = 0usize;
        let mut completed = 0usize;
        let mut skipped = 0usize;
        for it in &items {
            if is_terminal_run_item_status(&it.status) {
                match it.status.as_str() {
                    "failed" => failed += 1,
                    "skipped" => skipped += 1,
                    _ => completed += 1,
                }
                continue;
            }
            // 还挂着的 item：按整批下载的结果统一判定
            let (status, err) = if download_failed {
                (
                    RunItemStatus::Failed,
                    Some("重启续跑：本批下载存在失败项".to_string()),
                )
            } else {
                (RunItemStatus::Completed, None)
            };
            match status {
                RunItemStatus::Completed => completed += 1,
                _ => failed += 1,
            }
            let _ = self
                .persistence
                .update_run_item_status(it.id, status, err.as_deref());
        }

        // 2) 汇总 run 状态
        let status = if failed > 0 {
            RunStatus::CompletedWithErrors
        } else {
            RunStatus::Completed
        };
        let summary = DiffSummary {
            total: items.len(),
            failed,
            skipped,
            ..Default::default()
        };
        let now = chrono::Utc::now().timestamp();
        if let Err(e) = self
            .persistence
            .finish_run(run_id, now, status, &summary, None)
        {
            warn!("续跑收尾: finish_run 失败 run_id={}, error={}", run_id, e);
        }

        // 3) 推进或清理候选快照 —— 判据与正常收尾完全一致
        if should_advance_snapshot_baseline(status) {
            match self.persistence.promote_snapshot(run_id) {
                Ok(true) => info!(
                    "share_sync 续跑收尾: 订阅 {} run={} 干净完成，已推进快照基线",
                    sub_id, run_id
                ),
                Ok(false) => warn!("续跑收尾: 找不到候选快照，未推进基线 run_id={}", run_id),
                Err(e) => warn!("续跑收尾: 提升候选快照失败 run_id={}, error={}", run_id, e),
            }
        } else {
            warn!(
                "share_sync 续跑收尾: 订阅 {} run={} 有 {} 项失败，不推进基线，下一轮会重试",
                sub_id, run_id, failed
            );
            if let Err(e) = self.persistence.delete_snapshot_for_run(run_id) {
                warn!("续跑收尾: 清理候选快照失败 run_id={}, error={}", run_id, e);
            }
        }

        // 4) 广播，让前端把这条 run 从「运行中」收掉
        self.publisher.publish(ShareSyncEvent::RunCompleted {
            run_id: run_id.into(),
            subscription_id: sub_id.into(),
            added: completed,
            modified: 0,
            removed: 0,
            failed,
            owner_uid,
            duration_ms: None,
            n_bisects: None,
            max_bisect_depth: None,
        });
    }

    /// 启动期对被中断的订阅自动重跑一次。best-effort:先**轮询等账号登录态就绪**
    /// 再触发,避免账号没恢复时 execute_one 反复在「抓取阶段」失败、刷出一堆失败 run。
    /// 等到就绪(或超时)后只触发一次;触发不成则交给轮询调度兜底。供 `new` 后台调用。
    async fn resume_interrupted_runs(self: Arc<Self>, targets: Vec<(String, ResumeTarget)>) {
        // 给账号登录态恢复留点时间(进程刚起,resolver 可能还没就绪)。
        const RESUME_INITIAL_DELAY_SECS: u64 = 5;
        const READY_MAX_ATTEMPTS: u32 = 12;
        const READY_RETRY_DELAY_SECS: u64 = 10;
        tokio::time::sleep(std::time::Duration::from_secs(RESUME_INITIAL_DELAY_SECS)).await;
        for (id, target) in targets {
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
            // 🔥 重跑之前先清掉上一轮的残留（否则每中断一次就叠一批孤儿）。
            //
            // 需要清的东西：
            //   - 内部转存任务 + 其衍生下载任务（`delete_tasks_for_backup_config` 连带处理）
            //   - tree 模式产生的文件夹下载（同上，按 backup_config_id 归属）
            //   - 被中断那几轮的候选快照（它们永远不会被提升了）
            //
            // 为什么此刻清是安全的：本轮还没建任何新任务，而上一轮的残留在重启后
            // 都是暂停态、没有任何 run 在驱动它们。唯一的例外是调度器抢先触发了新
            // 一轮 —— 那种情况下 `running` 里已有在飞的 run，跳过清理与重跑，交给它。
            if self.running.contains_key(&id) {
                debug!(
                    "share_sync 启动续跑: 订阅 {} 已有 run 在执行（调度器抢先），跳过清理与重跑",
                    id
                );
                continue;
            }

            // 🔥 先看能不能「接着跑」——能续就不推倒重来（判据见 `probe_resumable_run`）。
            // 只接管一条：同一订阅同一时刻只允许一个 run 在飞。
            //
            // fresh 排在 stale 前面：本次刚收编的那条才是进程被杀时真正在飞的活儿，
            // 历史 interrupted 只是它的前身。
            let mut taken_over = false;
            for run_id in target.fresh.iter().chain(target.stale.iter()) {
                if let Some(task_ids) = self.probe_resumable_run(&id, run_id, owner_uid).await {
                    let mgr = Arc::clone(&self);
                    let sub = id.clone();
                    let rid = run_id.clone();
                    tokio::spawn(async move {
                        mgr.resume_run_in_place(sub, rid, owner_uid, task_ids).await;
                    });
                    taken_over = true;
                    break;
                }
            }
            if taken_over {
                // 已被接管：残留正是要接着做的活，绝不能清；也不重跑。
                continue;
            }

            // 只有历史遗留候选、且一条都接管不了 —— 说明这些 interrupted 记录名下
            // 已经没有未完成的下载了（`probe_resumable_run` 第 3 步的判据），纯属历史。
            // 到此为止：不清快照、不重跑。否则每次开机都会凭空多跑一轮，
            // 用户会看到「我没点，它自己同步了」。真要重跑交给轮询调度器。
            if target.fresh.is_empty() {
                debug!(
                    "share_sync 启动续跑: 订阅 {} 的历史中断 run 名下没有未完成下载，跳过（不重跑）",
                    id
                );
                continue;
            }

            for run_id in &target.fresh {
                if let Err(e) = self.persistence.delete_snapshot_for_run(run_id) {
                    warn!("清理中断 run 的候选快照失败: run_id={}, error={}", run_id, e);
                }
            }
            let cfg_id = share_sync_backup_config_id(&id);
            if let Some(transfer) = self.resolver.transfer_manager(owner_uid).await {
                let (mem, hist) = transfer.delete_tasks_for_backup_config(&cfg_id).await;
                if mem > 0 || hist > 0 {
                    info!(
                        "share_sync 启动续跑: 订阅 {} 已清理上一轮残留（转存内存={}, 历史={}，连带下载/文件夹子任务）",
                        id, mem, hist
                    );
                }
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
    /// 与「每个 run 的进度广播器」共用 `collect_share_sync_subtasks_with_children`，
    /// 形状一致——含文件夹展开出来的子文件行（`parent_task_id` 指向文件夹行）。
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
                let subs = collect_share_sync_subtasks_with_children(&tm, id, owner_uid)
                    .await
                    .into_iter()
                    .filter(|s| !is_terminal_subtask_status(&s.status))
                    .collect();
                Ok(drop_orphan_children(subs))
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

        // 0) 清掉上一轮遗留的、没人再驱动的内部子任务。
        //
        // 到这里 `running` 已被本次 run 独占（`resume_run_in_place` 走同一把守卫），
        // 所以此刻还挂在本订阅名下的转存/下载任务必然是**孤儿**：上一轮 run 已经
        // 结束（超时、失败、或进程被杀后没走到启动续跑），没有任何 run 在等它们。
        //
        // 留着它们的代价就是 issue #148：本轮 diff 仍会包含同一批文件（上一轮没成功
        // ⇒ 快照基线没推进），重新提交后新旧两支一起挂在「进行中子任务」里，同一个
        // 文件显示两条（旧的 paused / 新的 pending）。
        //
        // 与启动续跑路径的处理保持一致（见 `resume_interrupted_runs` 里的同名清理）：
        // 只删任务记录，已下载的本地文件保留，本轮会按 diff 重新补齐。
        self.sweep_residual_subtasks(id, owner_uid).await;

        // 1) 抓取
        //
        // 百度分享页/列表接口在高频同步时会偶发返回 HTML 风控页或半截响应，
        // 下层表现为"解析验证响应失败"/"解析子目录文件列表响应失败"。这不是
        // 链接失效，不应直接失败整次 run 或累加 link_invalid；先做短退避重试。
        let snapshot_max_retries: u32 = std::env::var("BAIDUPCS_SHARE_SYNC_SNAPSHOT_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);
        let snapshot_base_delay_ms: u64 =
            std::env::var("BAIDUPCS_SHARE_SYNC_SNAPSHOT_BACKOFF_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000);
        let mut snapshot_attempt: u32 = 0;

        // 跨「整轮重试」复用的目录缓存：只在**本次 run** 内有效。
        // 让重试从「整棵树重爬」退化成「续爬剩余目录」——大分享一轮几百个目录、
        // 上百秒，重爬既慢又因请求量翻倍更容易再次撞限流。
        let scan_cache = Arc::new(ScanCache::new());

        // 抓取阶段的进度广播：`RunStarted` 到 `DiffDetected` 之间是最长的一段静默，
        // 没有它前端只能一直显示「运行中」，用户会以为卡死。
        //
        // 节流约 500ms 一帧：几百个目录逐个回调会刷爆 WS。用 Mutex<Instant> 记上次
        // 发送时间，回调是同步闭包（在抓取的 async 上下文里被调），不能阻塞。
        let scan_attempt = Arc::new(AtomicU32::new(1));
        let scan_sink: ScanProgressSink = {
            let publisher = Arc::clone(&self.publisher);
            let run_id_for_sink = run_id.clone();
            let sub_id_for_sink = id.to_string();
            let attempt_for_sink = Arc::clone(&scan_attempt);
            let last_emit = Arc::new(std::sync::Mutex::new(None::<std::time::Instant>));
            Arc::new(move |p: crate::share_sync::snapshot::ScanProgress| {
                {
                    let mut guard = match last_emit.lock() {
                        Ok(g) => g,
                        Err(_) => return,
                    };
                    let now = std::time::Instant::now();
                    if let Some(prev) = *guard {
                        if now.duration_since(prev) < std::time::Duration::from_millis(500) {
                            return;
                        }
                    }
                    *guard = Some(now);
                }
                publisher.publish(ShareSyncEvent::ScanProgress {
                    run_id: run_id_for_sink.clone(),
                    subscription_id: sub_id_for_sink.clone(),
                    dirs_done: p.dirs_done,
                    dirs_pending: p.dirs_pending,
                    files_seen: p.files_seen,
                    current_dir: p.current_dir,
                    attempt: attempt_for_sink.load(Ordering::Relaxed),
                    cached_hits: p.cached_hits,
                    owner_uid,
                });
            })
        };

        // 阶段落库：WS 断线 / 页面刷新后，REST 轮询仍能显示「扫描目录中」。
        let _ = self
            .persistence
            .update_run_phase(&run_id, RUN_PHASE_SCANNING);

        let (captured, curr_snapshot) = loop {
            scan_attempt.store(snapshot_attempt + 1, Ordering::Relaxed);
            let attempt_result = match SnapshotCollector::from_url(
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
                Ok(collector) => collector
                    .with_progress(Arc::clone(&scan_sink))
                    .with_cache(Arc::clone(&scan_cache))
                    .collect()
                    .await
                    .map_err(|e| ("抓取失败", e)),
                Err(e) => Err(("抓取初始化失败", e)),
            };

            match attempt_result {
                Ok(t) => break t,
                Err((stage, e)) if e.should_retry() && snapshot_attempt < snapshot_max_retries => {
                    let backoff = snapshot_base_delay_ms.saturating_mul(1u64 << snapshot_attempt);
                    warn!(
                        "share-sync: {} 临时失败，{}ms 后重试 subscription={} run_id={} attempt={}/{} cached_dirs={} err={}",
                        stage,
                        backoff,
                        id,
                        run_id,
                        snapshot_attempt + 1,
                        snapshot_max_retries,
                        // 已缓存的目录数 = 下一轮可跳过的请求数，用来观察续爬是否生效
                        scan_cache.len(),
                        e
                    );
                    if backoff > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    }
                    snapshot_attempt += 1;
                }
                Err((stage, e)) => {
                    self.fail_run(&run_id, id, owner_uid, &format!("{}: {}", stage, e));
                    self.maybe_note_link_failure(id, &e);
                    return Err(e);
                }
            }
        };
        // 成功抓取到分享内容 → 链接可用，归零失效计数（如曾标记失效也清除）。
        self.clear_link_failure(id);
        let _ = self.persistence.update_run_phase(&run_id, RUN_PHASE_DIFFING);

        // 2) 绑定 subscription_id 后，先读"上次成功应用的快照"再计算 diff。
        //    当前快照必须等执行成功后才能推进基线；否则下载/转存失败会把
        //    未落地的新版本标记成已同步，后续轮询 diff 变空而不再重试。
        let mut curr_snapshot = curr_snapshot;
        curr_snapshot.subscription_id = id.into();

        // 抓取已完成，先把这份快照作为**候选**落库（`run_id = 本轮 run`）。
        //
        // 它此刻还不是基线 —— `latest_snapshot` 过滤 `run_id IS NULL`，看不到它。
        // 落库的唯一目的是：进程若在执行期间重启，仍能拿回"这一轮抓到了什么"，
        // 从而给被中断的 run 收尾并推进基线（续跑）。否则这份快照只存在于内存，
        // 重启即丢，被中断的 run 永远无法完成，基线永远不推进。
        //
        // 写失败只告警：后续 `promote_snapshot` 会因为找不到候选而返回 false，
        // 退化成"不推进基线"，下一轮 diff 重做本次内容 —— 与改动前的失败语义一致。
        if let Err(e) = self
            .persistence
            .save_candidate_snapshot(&curr_snapshot, &run_id)
        {
            warn!(
                "落候选快照失败（本轮结束后将无法推进基线，下次同步会重试本次 diff）: run_id={}, error={}",
                run_id, e
            );
        }

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
        let _ = self
            .persistence
            .update_run_phase(&run_id, RUN_PHASE_EXECUTING);
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
            // 把抓取阶段落的候选快照提升为基线（单事务：删旧基线 + 候选转正）。
            match self.persistence.promote_snapshot(&outcome.run_id) {
                Ok(true) => {}
                Ok(false) => warn!(
                    "run 成功但找不到候选快照（落库时可能失败过），未推进基线，下一次同步会重试本次 diff: run_id={}",
                    outcome.run_id
                ),
                Err(e) => warn!(
                    "提升候选快照为基线失败，下一次同步会重试本次 diff: run_id={}, error={}",
                    outcome.run_id, e
                ),
            }
        } else {
            warn!(
                "share-sync: run 未完全成功或有资源类跳过，不推进快照基线，下一次将重试 diff: run_id={}, status={:?}, failed={}, resource_skipped={}",
                outcome.run_id, outcome.status, outcome.diff_summary.failed, outcome.resource_skipped
            );
            // 候选已无用（这轮不会再被续跑：run 已到终态），清掉防止累积。
            if let Err(e) = self.persistence.delete_snapshot_for_run(&outcome.run_id) {
                warn!("清理候选快照失败: run_id={}, error={}", outcome.run_id, e);
            }
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

    /// 清掉某订阅名下没人驱动的内部转存/下载任务（孤儿残留）。
    ///
    /// **调用方必须已持有该订阅的 `running` 守卫**——否则会把正在跑的 run 的子任务
    /// 一起删掉。目前只有 `execute_one_with_run_id` 在建任何新任务之前调用。
    ///
    /// 只删任务记录，不删已下载的本地文件：本轮 diff 会把缺的重新补上，而
    /// `local_file_matches` 对已经落地的文件仍会直接标 Completed，不会白下一遍。
    async fn sweep_residual_subtasks(&self, id: &str, owner_uid: u64) {
        let Some(transfer) = self.resolver.transfer_manager(owner_uid).await else {
            return;
        };

        // 先探测有没有「还没到终态」的残留，有才动手。
        //
        // 健康订阅每个轮询周期都会走到这里，而 `delete_tasks_for_backup_config` 是
        // 全量操作：会对历史库发 DELETE、还会对**已完成**的文件夹调 `cancel_folder`
        // （刷一行 warn）。没残留时白跑一遍纯属噪音，探测只读内存、代价可以忽略。
        //
        // 转存段一起看：卡在 `checkingshare` / `transferring` 的孤儿转存同样该收。
        // （前提是 `is_terminal_subtask_status` 认得 `transferred` —— 那是纯网盘腿的
        // 正常终点，漏判的话每轮都会被误认成有残留。）
        let cfg_id = share_sync_backup_config_id(id);
        let mut residual: Vec<String> = collect_share_sync_subtasks(&transfer, id, owner_uid)
            .await
            .into_iter()
            .filter(|s| !is_terminal_subtask_status(&s.status))
            .map(|s| format!("{}:{}({})", s.kind, s.name, s.status))
            .collect();
        // `collect_share_sync_subtasks` 的转存段走 `get_all_tasks`，而它为「转存管理」
        // 列表隐藏了内部任务 —— 转存段在那里永远是空的。展示链路沿用现状，这里单独
        // 直接查内存里的内部转存任务，否则卡在转存阶段的孤儿任务（连同它的临时目录）
        // 永远收不走。
        residual.extend(
            transfer
                .get_internal_tasks_by_backup_config(&cfg_id)
                .await
                .into_iter()
                .filter(|t| !t.status.is_terminal())
                .map(|t| {
                    format!(
                        "transfer:{}({:?})",
                        t.file_name.unwrap_or_else(|| basename_of(&t.save_path)),
                        t.status
                    )
                }),
        );
        if residual.is_empty() {
            return;
        }

        let (mem, hist) = transfer.delete_tasks_for_backup_config(&cfg_id).await;
        warn!(
            "share-sync: 订阅 {} 本轮开始前清理上一轮残留子任务 {} 个（转存内存={}, 历史={}）: {}",
            id,
            residual.len(),
            mem,
            hist,
            residual.join(", ")
        );
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
        // run 已落到终态 Failed，不会再被续跑；若抓取阶段已落过候选快照，
        // 此刻它已无用，清掉防止候选在库里累积。
        if let Err(e) = self.persistence.delete_snapshot_for_run(run_id) {
            warn!("清理候选快照失败: run_id={}, error={}", run_id, e);
        }
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
    /// 所属父任务的 `task_id`（当前只有一种父子关系：文件夹聚合行 `"folder:{id}"`
    /// 与它的子文件下载任务）。顶层行为 `None`。
    ///
    /// 前端据此把子文件嵌套渲染在文件夹那一行下面；**只出现在展示链路**
    /// （[`collect_share_sync_subtasks_with_children`]），控制链路拿不到带父的行。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
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

/// 一组下载任务是否「没有一个真的在跑，却有在等待队列里」。
///
/// 这是 [`ProductionHooks::is_waiting_for_download_slot`] 的判定核心，抽成纯函数
/// 便于单测（那个方法本身要拿 DownloadManager，测不动）。
///
/// 注意不能复用 [`TaskStatus::is_active_download_status`]：它把 `Pending` 也算作
/// 「活跃」（那是速度聚合的口径），拿它判断会永远得出「有任务在跑」。
fn no_progress_but_queued<'a>(
    statuses: impl Iterator<Item = &'a crate::downloader::TaskStatus>,
) -> bool {
    use crate::downloader::TaskStatus;

    let mut any_running = false;
    let mut any_waiting = false;
    for status in statuses {
        match status {
            TaskStatus::Downloading | TaskStatus::Decrypting => any_running = true,
            // 🔥 `Paused` 与 `Pending` 同样算「在等」。
            //
            // 分享同步的下载走 `create_backup_task` → `TaskPriority::Backup`，是**可被
            // 抢占**的最低优先级：普通下载任务一来就把它踢成 `Paused`（见
            // `task_slot_pool.rs` 的 `is_preemptable`）。此前只认 `Pending`，被抢占的
            // 任务既不算在跑也不算在排队 → `is_waiting_for_download_slot` 返回 false
            // → idle 计时不清零 → 30 分钟后 run 被判「等待任务完成超时」而失败，
            // 而它派生的下载子任务还留在系统里，成为下一轮的重复项来源（issue #148）。
            //
            // 与下面 `folder_has_queued_work` 的口径也就此统一（那边一直把 `Paused`
            // 算作待办，理由同样是「恢复出来的子任务以 Paused 起步」）。
            TaskStatus::Pending | TaskStatus::Paused => any_waiting = true,
            _ => {}
        }
    }
    any_waiting && !any_running
}

/// run_item 是否已到终态（不会再变）。
///
/// 对应 [`RunItemStatus`] 里的 `Completed` / `Failed` / `Skipped`；
/// `Pending` / `Transferring` / `Downloading` / `Deleting` 都还在途中。
fn is_terminal_run_item_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "skipped")
}

/// 文件夹是否「还有活没做完，但一个都没在跑」。
///
/// 与 [`no_progress_but_queued`] 同口径：`Pending` / `Paused` 都算待办。
///
/// 原因：重启恢复出来的文件夹子任务是以 `Paused` 创建的
/// （日志「恢复模式补任务完成: 文件夹 X 创建了 N 个暂停任务」），
/// 之后进入等待队列时状态也不变（`add_to_waiting_queue_*` 只入队、不改状态）。
/// 若只认 `Pending`，重启后所有在等槽位的文件夹都会一直显示「下载中」。
///
/// 「用户显式暂停」的歧义在调用方消解：[`folder_subtask_status`] 只在
/// `FolderStatus::Downloading` 时才改写状态，而用户暂停的文件夹自身是
/// `FolderStatus::Paused`，不会走到这里。
fn folder_has_queued_work<'a>(
    statuses: impl Iterator<Item = &'a crate::downloader::TaskStatus>,
) -> bool {
    use crate::downloader::TaskStatus;

    let mut any_running = false;
    let mut any_unfinished = false;
    for status in statuses {
        match status {
            TaskStatus::Downloading | TaskStatus::Decrypting => any_running = true,
            TaskStatus::Pending | TaskStatus::Paused => any_unfinished = true,
            _ => {}
        }
    }
    any_unfinished && !any_running
}

/// 文件夹下载在子任务列表里上报的状态。
///
/// `waiting_for_slot`（子任务全在等待队列、没有一个在跑，见 [`no_progress_but_queued`]）
/// 时把 `Downloading` 改报成 `"pending"`，与单文件段（`TaskStatus::Pending` →
/// 前端「等待中」）口径一致。1 个任务槽时抢不到槽位的文件夹就处于这个状态。
///
/// **只改写 `Downloading` 这一档**，其余状态一律照实上报：
/// - `Scanning`：还在列目录，是真的在干活，不该说成「等待中」
/// - `Failed` / `Cancelled`：终态。改写成 `pending` 会让它从终态变成非终态，
///   REST `subtasks()` 的终态过滤失效，失败的文件夹会永远挂在「等待中」不消失
/// - `Paused`：用户显式暂停，不是在等槽位
/// - `Completed`：同理，终态
fn folder_subtask_status(
    folder_status: crate::downloader::FolderStatus,
    waiting_for_slot: bool,
) -> String {
    if waiting_for_slot && folder_status == crate::downloader::FolderStatus::Downloading {
        return "pending".to_string();
    }
    format!("{:?}", folder_status).to_lowercase()
}

/// 收集某个订阅当前的子任务进度（下载段 + 内部转存段），按 `backup_config_id` 归属。
///
/// **控制链路专用**：文件夹只出一条聚合行，不含它的子文件任务。
///
/// 这个口径必须保持「一个可操作单元一行」——调用方会拿返回的 `task_id` 去做
/// 暂停/恢复/取消/统计未完成数（见 [`restartable_share_sync_download`]、
/// `finalize_resumed_run`、`cancel_residual_downloads`）。文件夹的子文件任务由
/// folder_manager 按批物化并自己调度（`refill_tasks_batch`），拿单个子任务 id 去
/// pause/resume 会和文件夹的槽位逻辑打架，未完成计数也会被重复计一遍。
///
/// 要给用户看逐文件进度时用 [`collect_share_sync_subtasks_with_children`]。
pub async fn collect_share_sync_subtasks(
    transfer: &TransferManager,
    subscription_id: &str,
    owner_uid: u64,
) -> Vec<ShareSyncSubtask> {
    collect_subtasks_impl(transfer, subscription_id, owner_uid, false).await
}

/// 收集子任务进度，**并把文件夹的子文件任务一并展开**（`parent_task_id` 指向
/// 文件夹那一行的 `"folder:{id}"`）。
///
/// **展示链路专用**：REST `/subtasks` 与 WS `item_progress` 广播用它，前端把子文件
/// 嵌套渲染在文件夹行下面。别拿它去驱动控制逻辑，理由见
/// [`collect_share_sync_subtasks`]。
///
/// 注意能展开的只有**当前已物化**的那一批子任务：folder_manager 只维持约等于槽位数
/// 的活跃子任务，其余文件还躺在 `folder.pending_files` 里，压根没有 `DownloadTask`
/// 对象、也就没有进度可言。所以这里给出的是「文件夹整体进度 + 正在跑的那几个文件的
/// 逐文件进度」，不是全量文件列表。
pub async fn collect_share_sync_subtasks_with_children(
    transfer: &TransferManager,
    subscription_id: &str,
    owner_uid: u64,
) -> Vec<ShareSyncSubtask> {
    collect_subtasks_impl(transfer, subscription_id, owner_uid, true).await
}

async fn collect_subtasks_impl(
    transfer: &TransferManager,
    subscription_id: &str,
    owner_uid: u64,
    include_folder_children: bool,
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
                parent_task_id: None,
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
                parent_task_id: None,
                owner_uid,
            });
        }
    }

    // 下载段(文件夹):tree 模式整目录转存 + 自动下载会产生文件夹下载任务
    // (backup_config_id==cfg)。其子文件任务带 group_id 不会单独出现在下载管理,
    // 也不走 is_backup 的 get_tasks_by_backup_config,因此在此按文件夹级聚合进度。
    if let Some(fdm) = transfer.folder_download_manager_handle().await {
        for f in fdm.get_folders_by_backup_config(&cfg).await {
            // 文件夹本身不持有速度，按其子文件任务(group_id==folder.id)的瞬时速度求和。
            //
            // **只统计 `Downloading`**，与 `folder_manager` / `folder_download` handler
            // 两处的口径一致（那边注释也是「速度只统计仍在下载中的子任务」）。
            //
            // 这里原本用 `is_active_download_status()`，它包含 `Pending` —— 而任务被
            // 暂停/入队时并不会把 `speed` 字段清零（只有 `auto_requeue_task` 那条错误
            // 路径清），于是排队中任务保留着上一次的速度值，全被加进总和：
            // 1 个任务实际只跑 78 KB/s，界面却显示 4.39 MB/s。
            // 排队中的任务本来就没有速度贡献，按 0 计才是对的。
            //
            // 顺带用同一次查询判断「整个文件夹卡在排队等槽位」（见 `folder_subtask_status`）：
            // 只有 1 个任务槽时，抢不到槽位的文件夹 fixed_slot_id=None、子任务全部
            // 停在 Pending，但 FolderStatus 仍是 Downloading（它没有 Pending 这一档），
            // 照原样上报会让前端显示「下载中」而实际一个字节没动。
            let (speed, waiting_for_slot, children) = if let Some(dm) = dm_handle.as_ref() {
                let children = dm.get_tasks_by_group(&f.id).await;
                let speed = children
                    .iter()
                    .filter(|t| t.status == crate::downloader::TaskStatus::Downloading)
                    .map(|t| t.speed)
                    .sum();
                let waiting = folder_has_queued_work(children.iter().map(|t| &t.status));
                (speed, waiting, children)
            } else {
                (0, false, Vec::new())
            };
            let folder_task_id = format!("folder:{}", f.id);
            let folder_status = folder_subtask_status(f.status.clone(), waiting_for_slot);
            let folder_is_terminal = is_terminal_subtask_status(&folder_status);
            out.push(ShareSyncSubtask {
                task_id: folder_task_id.clone(),
                name: f.name.clone(),
                kind: "download".to_string(),
                status: folder_status,
                downloaded: f.downloaded_size,
                total: f.total_size,
                progress: f.progress(),
                speed,
                eta_seconds: compute_eta_seconds(f.downloaded_size, f.total_size, speed),
                parent_task_id: None,
                owner_uid,
            });

            // 展示链路：把上面那次查询已经拿到的子文件任务也吐出来，挂在文件夹行下面。
            // 复用同一份 `children`，不额外查一遍。
            //
            // 文件夹本身到终态时一个子行都不出：父行会被 `subtasks()` 的终态过滤掉，
            // 再吐子行就成了没有父的孤儿。文件夹终态意味着这批下载已经结束（完成 /
            // 失败 / 取消），内存里可能残留的子任务对用户没有意义。
            if include_folder_children && !folder_is_terminal {
                out.extend(
                    children
                        .into_iter()
                        .map(|t| folder_child_subtask(t, &folder_task_id, owner_uid)),
                );
            }
        }
    }

    out
}

/// 丢掉父行已经不在列表里的子行。
///
/// REST `subtasks()` 会先按终态过滤：文件夹自己到终态（比如整体 failed）被滤掉，
/// 而它内存里残留的子任务可能还是非终态，于是留下一批没有父行的孤儿。前端按
/// `parent_task_id` 分组时这些行会挂不上任何父节点，直接从界面上消失——用户看到的是
/// 「文件夹没了、子文件也没了」。这里在源头收掉，保证 REST 快照自洽。
///
/// 只对全量快照有意义，WS 那条增量链路做不了这个判断（见前端 `groupedSubtasks` 里
/// 把孤儿提到顶层的兜底）。
fn drop_orphan_children(subs: Vec<ShareSyncSubtask>) -> Vec<ShareSyncSubtask> {
    let parents: std::collections::HashSet<&str> = subs
        .iter()
        .filter(|s| s.parent_task_id.is_none())
        .map(|s| s.task_id.as_str())
        .collect();
    let keep: Vec<bool> = subs
        .iter()
        .map(|s| match s.parent_task_id.as_deref() {
            Some(p) => parents.contains(p),
            None => true,
        })
        .collect();
    subs.into_iter()
        .zip(keep)
        .filter_map(|(s, k)| k.then_some(s))
        .collect()
}

/// 把文件夹的一个子文件下载任务转成挂在 `parent` 下的 [`ShareSyncSubtask`]。
///
/// 展示名优先取 `relative_path`（如 `科幻片/星际穿越.mp4`）：文件夹里重名文件很常见
/// （每季一个 `01.mp4`），只给 basename 的话列表里会出现一串分不清谁是谁的同名行。
fn folder_child_subtask(
    t: crate::downloader::DownloadTask,
    parent_task_id: &str,
    owner_uid: u64,
) -> ShareSyncSubtask {
    let name = t
        .relative_path
        .clone()
        .filter(|p| !p.is_empty())
        .or_else(|| {
            t.local_path
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| basename_of(&t.remote_path));
    let progress = if t.total_size > 0 {
        (t.downloaded_size as f64 / t.total_size as f64) * 100.0
    } else {
        0.0
    };
    ShareSyncSubtask {
        task_id: t.id.clone(),
        name,
        kind: "download".to_string(),
        status: format!("{:?}", t.status).to_lowercase(),
        downloaded: t.downloaded_size,
        total: t.total_size,
        progress,
        speed: t.speed,
        eta_seconds: compute_eta_seconds(t.downloaded_size, t.total_size, t.speed),
        parent_task_id: Some(parent_task_id.to_string()),
        owner_uid,
    }
}

/// 子任务状态是否已到终态（完成/失败/取消）。
///
/// 口径覆盖三个来源的状态枚举(lowercased `{:?}`):
/// - 文件/文件夹下载(`TaskStatus`/`FolderStatus`): `completed` / `failed` / `cancelled`
/// - 内部转存(`TransferStatus`): `completed` / `transferred` / `transferfailed` / `downloadfailed`
///
/// `transferred` 也是终态:它是**纯网盘腿的正常终点**（`submit_transfer_batch` 传
/// `auto_download: false`，枚举上就注释成「转存成功（无自动下载）」），自动下载那条
/// 路径不经过它而是直接进 `Downloading`。此前漏了它，后果是纯网盘目标的订阅每次
/// 同步完成后，都会在「进行中子任务」里永久留下一条转存记录不消失（REST 过滤不掉，
/// WS 也因为不判终态而每秒重推）。`wait_transfer_task` 早就把它当终点处理
/// （`Transferred if !require_download_completion => Ok(())`），这里补齐口径。
///
/// 与前端 `SUBTASK_TERMINAL` 保持一致（两边一起改，否则 REST 不回包、WS 推来的那条
/// 仍会挂在列表里）。
fn is_terminal_subtask_status(status: &str) -> bool {
    matches!(
        status,
        "completed"
            | "success"
            | "failed"
            | "cancelled"
            | "transferred"
            | "transferfailed"
            | "downloadfailed"
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
        // "pending" = 子任务全在等待队列（见 `folder_subtask_status`）。必须留在
        // 可重启集合里：这个状态既可能是「在排队等槽位」，也可能是「有空槽却卡住」。
        // 前者由 wait_transfer_task 的 `is_waiting_for_download_slot` 分支把 idle
        // 清零，stall 重试根本不会触发；只有后者才会走到这里被 pause+resume 踢一下。
        // 若把 pending 排除掉，真卡死的文件夹就再也没人救，只能干等到 idle 超时失败。
        return matches!(
            subtask.status.as_str(),
            "scanning" | "downloading" | "paused" | "pending"
        )
            .then(|| RestartableShareSyncDownload::Folder(folder_id.to_string()));
    }

    matches!(subtask.status.as_str(), "downloading" | "paused")
        .then(|| RestartableShareSyncDownload::Task(subtask.task_id.clone()))
}

async fn restart_stalled_share_sync_downloads(
    transfer: &TransferManager,
    subtasks: &[ShareSyncSubtask],
    resume_delay: Duration,
) -> usize {
    let mut task_ids = Vec::new();
    let mut folder_ids = Vec::new();
    let mut paused_task_ids = Vec::new();
    let mut paused_folder_ids = Vec::new();

    for subtask in subtasks {
        match restartable_share_sync_download(subtask) {
            Some(RestartableShareSyncDownload::Task(task_id)) if subtask.status == "paused" => {
                paused_task_ids.push(task_id)
            }
            Some(RestartableShareSyncDownload::Folder(folder_id)) if subtask.status == "paused" => {
                paused_folder_ids.push(folder_id)
            }
            Some(RestartableShareSyncDownload::Task(task_id)) => task_ids.push(task_id),
            Some(RestartableShareSyncDownload::Folder(folder_id)) => folder_ids.push(folder_id),
            None => {}
        }
    }

    task_ids.sort();
    task_ids.dedup();
    folder_ids.sort();
    folder_ids.dedup();
    paused_task_ids.sort();
    paused_task_ids.dedup();
    paused_folder_ids.sort();
    paused_folder_ids.dedup();

    let mut restarted = 0usize;

    if !folder_ids.is_empty() || !paused_folder_ids.is_empty() {
        match transfer.folder_download_manager_handle().await {
            Some(folder_manager) => {
                for folder_id in paused_folder_ids {
                    match folder_manager.resume_folder(&folder_id).await {
                        Ok(()) => {
                            restarted += 1;
                            warn!(
                                "share-sync: paused folder download resumed: folder_id={}",
                                folder_id
                            );
                        }
                        Err(e) => warn!(
                            "share-sync: resume paused folder download failed: folder_id={}, error={}",
                            folder_id, e
                        ),
                    }
                }
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

    if !task_ids.is_empty() || !paused_task_ids.is_empty() {
        match transfer.download_manager_handle().await {
            Some(download_manager) => {
                for task_id in paused_task_ids {
                    match download_manager.resume_task(&task_id).await {
                        Ok(()) => {
                            restarted += 1;
                            warn!("share-sync: paused download task resumed: task_id={}", task_id);
                        }
                        Err(e) => warn!(
                            "share-sync: resume paused download task failed: task_id={}, error={}",
                            task_id, e
                        ),
                    }
                }
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

/// 空转多久后才去探测「是不是在排队等槽位」。
///
/// `is_waiting_for_download_slot()` 要遍历下载任务表，1 秒一次的轮询里每轮都查
/// 太浪费；正常有进展时 idle 根本涨不到这个值，只有真的停住了才需要区分
/// 「排队等槽」和「卡死」。
const QUEUE_WAIT_PROBE_AFTER: Duration = Duration::from_secs(30);

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
    reported_terminal: &mut std::collections::HashSet<String>,
) {
    let subs =
        collect_share_sync_subtasks_with_children(transfer, subscription_id, owner_uid).await;
    for s in subs {
        // 终态只推一次（见 `broadcast_subtask_progress` 的说明）
        if is_terminal_subtask_status(&s.status) && !reported_terminal.insert(s.task_id.clone()) {
            continue;
        }
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
            parent_task_id: s.parent_task_id,
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
    // 已经推过终态的子任务：终态只推**一次**，之后不再重复。
    //
    // 前端 `upsertSubtask` 收到终态的处理是「从进行中列表移除」，所以终态必须推一次
    // （否则跑完的子任务会一直挂在界面上）；但重复推就变成每秒「收到 → 移除」，
    // 白占带宽，日志也被刷满。REST 的 `subtasks()` 本来就只返回非终态，这里对齐它的
    // 语义：进行中的持续推，终态推一次即止。
    let mut reported_terminal: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        ticker.tick().await;
        emit_subtask_progress(
            &publisher,
            &transfer,
            &run_id,
            &subscription_id,
            owner_uid,
            &mut reported_terminal,
        )
            .await;
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
        // run 结束后的最终帧：用空集合，保证终态一定会被推出去（前端据此移除）
        let mut reported_terminal = std::collections::HashSet::new();
        emit_subtask_progress(
            &publisher,
            &tm,
            &run_id,
            &subscription_id,
            owner_uid,
            &mut reported_terminal,
        )
            .await;
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

    /// 当前账号的下载槽位池是否还有空位。
    ///
    /// 拿不到下载管理器时返回 `true`（不阻断），维持旧行为。
    async fn has_free_download_slot(&self) -> bool {
        let tm = self.transfer_manager();
        match tm.download_manager_handle().await {
            Some(dm) => dm.task_slot_pool().available_slots().await > 0,
            None => true,
        }
    }

    /// 本订阅是否正卡在「等下载槽位」上。
    ///
    /// 三个条件同时成立才算：
    /// 1. 槽位池当前没有空位；
    /// 2. 本订阅**没有任何**下载子任务真的在跑（Downloading / Decrypting）；
    /// 3. 本订阅有下载子任务在等（`Pending` 排队，或被高优先级任务抢占成 `Paused`）。
    ///
    /// 条件 2、3 的判定抽在 [`no_progress_but_queued`] 里（纯函数，便于单测）。
    ///
    /// 条件 2 不能省：槽位可能正被**本订阅自己**占着（pool=1 时文件 1 在下载、
    /// 文件 2..N 排队）。此时若只看「池满 + 有 Pending」就判定在排队，会把
    /// idle 一直清零 —— 万一那个在下载的其实卡死在 0 字节，stall 重试和 idle
    /// 超时就都失效了，run 会一直挂到 7 天硬超时，比改动前更糟。有任务真的在跑
    /// 时就该老老实实走原来的停滞检测。
    ///
    /// 满足时的处理：
    /// - 不触发 stall 重试（重试也抢不到槽，只会把自己挪到队尾、打乱 FIFO）
    /// - 不计入 idle 超时（否则非会员 1 槽位场景下，排在后面的同步会在
    ///   30 分钟后被误判为「等待任务完成超时」而整个 run 失败）
    async fn is_waiting_for_download_slot(&self) -> bool {
        let tm = self.transfer_manager();
        let Some(dm) = tm.download_manager_handle().await else {
            return false;
        };
        // 有空位就不是「等槽位」——此时不动仍属于需要关注的停滞
        if dm.task_slot_pool().available_slots().await > 0 {
            return false;
        }

        let cfg = self.share_sync_backup_config_id();
        // 单文件下载段：is_backup && backup_config_id == cfg
        let mut statuses: Vec<crate::downloader::TaskStatus> = dm
            .get_tasks_by_backup_config(&cfg)
            .await
            .iter()
            .map(|t| t.status.clone())
            .collect();

        // 文件夹下载段：子任务带 group_id、is_backup=false，不会出现在上面按
        // backup_config 的查询里（见 `DownloadTask::new_with_group`），要单独取
        if let Some(fdm) = tm.folder_download_manager_handle().await {
            for f in fdm.get_folders_by_backup_config(&cfg).await {
                statuses.extend(dm.get_tasks_by_group(&f.id).await.iter().map(|t| t.status.clone()));
            }
        }

        no_progress_but_queued(statuses.iter())
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
        let mut last_paused_resume_at: Option<tokio::time::Instant> = None;
        // 上次「是否在排队等槽位」探测的时刻。探测要遍历下载任务表，必须节流：
        // 探测返回 false（真卡住、有空槽）时 idle 不会被清零，若不记时刻就会每秒
        // 全表扫一次、一路扫到 idle 超时（30 分钟 ≈ 1800 次），纯属浪费。
        let mut last_slot_probe_at: Option<tokio::time::Instant> = None;
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
            let now = tokio::time::Instant::now();
            if require_download_completion {
                let paused_subtasks: Vec<ShareSyncSubtask> = subtasks
                    .iter()
                    .filter(|s| s.status == "paused")
                    .cloned()
                    .collect();
                let paused_resume_due = !paused_subtasks.is_empty()
                    && last_paused_resume_at
                    .map(|last| now.duration_since(last) >= Duration::from_secs(10))
                    .unwrap_or(true);
                // 🔥 槽位满时不要强行 resume：resume 拿不到槽位只会让任务立刻回到
                // 等待队列尾部（add_to_waiting_queue_*），状态在 paused/pending 之间
                // 来回抖，还会把自己排到队尾、打乱 FIFO 顺序。等真有空位了再唤醒，
                // 期间由等待队列按顺序拉起即可。
                //
                // 注意这里只是跳过本轮 resume，不能 `continue`——否则下面的
                // 硬超时 / idle 超时永远不会被求值，任务会无限期挂着。
                let slot_available_for_resume =
                    paused_resume_due && self.has_free_download_slot().await;
                if paused_resume_due {
                    // 无论本轮是否真的 resume，都走同一个 10s 节流窗口
                    last_paused_resume_at = Some(now);
                }
                if paused_resume_due && !slot_available_for_resume {
                    debug!(
                        "share-sync: 槽位已满，本轮不自动 resume paused 子任务: task_id={}, paused={}",
                        task_id,
                        paused_subtasks.len()
                    );
                }
                if slot_available_for_resume {
                    let restarted =
                        restart_stalled_share_sync_downloads(&tm, &paused_subtasks, Duration::ZERO)
                            .await;
                    if restarted > 0 {
                        warn!(
                            "share-sync: paused download subtasks resumed immediately: task_id={}, restarted={}",
                            task_id, restarted
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            let signature = task_activity_signature(&task, &subtasks);
            if last_signature.as_ref() != Some(&signature) {
                last_activity_at = tokio::time::Instant::now();
                last_signature = Some(signature);
            }

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

            let mut idle_for = now.duration_since(last_activity_at);

            // 🔥 排队等下载槽位不算「无进展」。
            //
            // 非会员 max_concurrent_tasks=1 时，同时创建的多个同步里只有一个能拿到
            // 槽位，其余的下载子任务停在 Pending 排队 —— 这期间 activity signature
            // 不变，idle 会一路涨到 30 分钟的 idle_timeout，把本来只是「在排队」的
            // 同步判成「等待任务完成超时」失败。而 stall 重试也救不了它：
            // `restartable_share_sync_download` 压根不认 pending，排队中的任务不在
            // 可重启集合里；就算重启了也抢不到槽位，只是把自己挪到队尾而已。
            //
            // 因此确认「确实在排队等槽」时把活跃时间往前推，让 idle 从真正开始跑的
            // 那一刻起算。整体仍受 hard_timeout（默认 7 天）兜底，不会真的无限等。
            //
            // 探测本身按 QUEUE_WAIT_PROBE_AFTER 节流（见 `last_slot_probe_at`）：
            // 它要遍历下载任务表，而 idle 一旦越过阈值就会每轮都满足条件。
            let probe_due = require_download_completion
                && idle_for >= QUEUE_WAIT_PROBE_AFTER
                && last_slot_probe_at
                .map(|last| now.duration_since(last) >= QUEUE_WAIT_PROBE_AFTER)
                .unwrap_or(true);
            if probe_due {
                last_slot_probe_at = Some(now);
                if self.is_waiting_for_download_slot().await {
                    debug!(
                        "share-sync: 下载子任务在排队等槽位，不计入 idle 超时: task_id={}, idle_secs={}",
                        task_id,
                        idle_for.as_secs()
                    );
                    last_activity_at = now;
                    idle_for = Duration::ZERO;
                }
            }

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

    async fn discard_task(&self, task_id: &str) {
        let (downloads, folders) = self.transfer_manager().discard_task(task_id).await;
        if downloads > 0 || folders > 0 {
            info!(
                "share-sync: 已丢弃转存任务 {}（连带下载子任务={}, 文件夹子任务={}）",
                task_id, downloads, folders
            );
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
        kind: captured.kind,
        short_key: captured.short_key.clone(),
    }
}

/// 从已捕获的分享上下文还原分享 URL
///
/// 两套体系的 URL 形态不同，企业版套用个人版的 `/s/{short_key}` 会拼出
/// 一个不存在的链接（`short_key` 是不带 `1` 前缀的 surl）。
fn share_url_for_captured(captured: &CapturedShare) -> String {
    let base = match captured.kind {
        crate::transfer::ShareKind::Apaas => {
            format!("https://pan.baidu.com/apaas/share?surl={}", captured.short_key)
        }
        crate::transfer::ShareKind::Personal => {
            format!("https://pan.baidu.com/s/{}", captured.short_key)
        }
    };
    match captured.password.as_deref().filter(|p| !p.is_empty()) {
        Some(pwd) => {
            let sep = if base.contains('?') { '&' } else { '?' };
            format!("{}{}pwd={}", base, sep, pwd)
        }
        None => base,
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

    fn subtask(status: &str) -> ShareSyncSubtask {
        ShareSyncSubtask {
            task_id: "dl-1".into(),
            name: "a.bin".into(),
            kind: "download".into(),
            status: status.into(),
            downloaded: 0,
            total: 1024,
            progress: 0.0,
            speed: 0,
            eta_seconds: None,
            parent_task_id: None,
            owner_uid: 1,
        }
    }

    /// 构造一条挂在 `parent` 下的子文件行（其余字段用不到，取默认）
    fn child_subtask(task_id: &str, parent: &str, status: &str) -> ShareSyncSubtask {
        ShareSyncSubtask {
            task_id: task_id.into(),
            name: format!("{}.mp4", task_id),
            kind: "download".into(),
            status: status.into(),
            downloaded: 0,
            total: 1024,
            progress: 0.0,
            speed: 0,
            eta_seconds: None,
            parent_task_id: Some(parent.into()),
            owner_uid: 1,
        }
    }

    fn folder_row(task_id: &str, status: &str) -> ShareSyncSubtask {
        ShareSyncSubtask {
            task_id: task_id.into(),
            status: status.into(),
            name: "某文件夹".into(),
            ..subtask(status)
        }
    }

    /// 父行还在时子行原样保留 —— 这是展开逐文件进度的正常路径。
    #[test]
    fn test_drop_orphan_children_keeps_children_with_present_parent() {
        let subs = vec![
            folder_row("folder:f1", "downloading"),
            child_subtask("dl-1", "folder:f1", "downloading"),
            child_subtask("dl-2", "folder:f1", "pending"),
        ];
        let out = drop_orphan_children(subs);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].task_id, "folder:f1");
    }

    /// 父行被上游的终态过滤掉之后，子行必须一起消失。
    ///
    /// 留着的话前端按 `parent_task_id` 分组时它们挂不上任何父节点，
    /// 直接从界面上蒸发——比"整个文件夹一起消失"更让人摸不着头脑。
    #[test]
    fn test_drop_orphan_children_drops_children_without_parent() {
        let subs = vec![
            // 顶层的单文件下载没有父，永远保留
            subtask("downloading"),
            child_subtask("dl-9", "folder:gone", "downloading"),
        ];
        let out = drop_orphan_children(subs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].task_id, "dl-1");
    }

    /// 空列表 / 全是顶层行时行为不变（别把正常路径改坏了）
    #[test]
    fn test_drop_orphan_children_noop_without_children() {
        assert!(drop_orphan_children(Vec::new()).is_empty());
        let flat = vec![subtask("downloading"), folder_row("folder:f1", "scanning")];
        assert_eq!(drop_orphan_children(flat).len(), 2);
    }

    /// 子文件展示名优先用 `relative_path`：文件夹里 `01.mp4` 这种重名极常见，
    /// 只给 basename 的话展开后是一串分不清谁是谁的同名行。
    #[test]
    fn test_folder_child_subtask_prefers_relative_path() {
        use crate::downloader::{DownloadTask, TaskStatus};

        let mut t = DownloadTask::new(
            1,
            "/来自：分享/剧集/第一季/01.mp4".to_string(),
            std::path::PathBuf::from("/data/剧集/第一季/01.mp4"),
            2000,
            crate::auth::Uid(42),
        );
        t.id = "dl-7".to_string();
        t.status = TaskStatus::Downloading;
        t.downloaded_size = 500;
        t.speed = 100;
        t.relative_path = Some("第一季/01.mp4".to_string());

        let c = folder_child_subtask(t.clone(), "folder:f1", 42);
        assert_eq!(c.name, "第一季/01.mp4");
        assert_eq!(c.task_id, "dl-7");
        assert_eq!(c.parent_task_id.as_deref(), Some("folder:f1"));
        assert_eq!(c.kind, "download");
        assert_eq!(c.status, "downloading");
        assert_eq!(c.progress, 25.0);
        // eta = (2000-500)/100
        assert_eq!(c.eta_seconds, Some(15));
        assert_eq!(c.owner_uid, 42);

        // 没有 relative_path 时回退到本地文件名，不能是空串
        let mut no_rel = t;
        no_rel.relative_path = None;
        assert_eq!(folder_child_subtask(no_rel, "folder:f1", 42).name, "01.mp4");
    }

    /// 续跑的判据/等待/收尾统一看**下载子任务**，所以子任务终态判定是这条链的地基。
    ///
    /// 判错的后果：
    /// - 未完成被当成终态 → 续跑提前收尾，可能把没下完的当成功而推进基线（永久漏同步）
    /// - 终态被当成未完成 → 续跑永远等不到头，占着订阅的 in-flight 标记直到硬上限
    #[test]
    fn test_subtask_terminal_status_drives_resume() {
        // 终态：续跑不再等它们
        for s in [
            "completed",
            "success",
            "failed",
            "cancelled",
            "transferred",
            "transferfailed",
            "downloadfailed",
        ] {
            assert!(is_terminal_subtask_status(s), "{} 应是终态", s);
        }
        // 非终态：续跑要继续等
        for s in ["pending", "downloading", "paused", "scanning", "transferring"] {
            assert!(!is_terminal_subtask_status(s), "{} 不是终态，续跑要继续等", s);
        }
    }

    /// run_item 终态判定 —— 续跑判据和收尾都依赖它。
    ///
    /// 判错的后果是双向的：
    /// - 把未完成误判成终态 → 收尾时当成"已完成"，可能推进基线 → **永久漏同步**
    /// - 把终态误判成未完成 → 续跑时白等一个不会再变的 item
    #[test]
    fn test_is_terminal_run_item_status() {
        for s in ["completed", "failed", "skipped"] {
            assert!(is_terminal_run_item_status(s), "{} 应是终态", s);
        }
        for s in ["pending", "transferring", "downloading", "deleting"] {
            assert!(!is_terminal_run_item_status(s), "{} 不是终态", s);
        }
    }

    /// 续跑收尾对基线的处理，必须与正常收尾**同一判据**。
    ///
    /// 这是整个续跑机制唯一可能造成数据问题的地方：只要有一项失败，
    /// 就绝不能提升候选快照，否则那些没落地的文件会被当作"已同步"而永久漏掉。
    #[test]
    fn test_resumed_finalize_uses_same_baseline_rule() {
        use crate::share_sync::types::RunStatus;

        // 全部完成 → 可推进
        assert!(should_advance_snapshot_baseline(RunStatus::Completed));
        // 有失败 → 不可推进（续跑收尾在 failed > 0 时给出的正是这个状态）
        assert!(!should_advance_snapshot_baseline(
            RunStatus::CompletedWithErrors
        ));
    }

    /// 文件夹层的「有活没做完但没人在跑」——必须把 `Paused` 也算进去。
    ///
    /// 回归实测：重启恢复的文件夹子任务是以 `Paused` 创建的，入等待队列后状态不变。
    /// 只认 `Pending` 的话，重启后 N 个等槽位的文件夹在前端会全部显示「下载中」，
    /// 而实际只有一个在动 —— 正是 issue #138 报告者描述的观感。
    #[test]
    fn test_folder_has_queued_work_counts_paused() {
        use crate::downloader::TaskStatus;

        assert!(
            folder_has_queued_work([TaskStatus::Paused, TaskStatus::Paused].iter()),
            "重启恢复出来的暂停子任务也是「待办」"
        );
        assert!(
            folder_has_queued_work([TaskStatus::Pending, TaskStatus::Paused].iter()),
            "Pending 与 Paused 混合同样算待办"
        );
        assert!(
            !folder_has_queued_work([TaskStatus::Paused, TaskStatus::Downloading].iter()),
            "有子任务在跑就不是「等待中」"
        );
        assert!(
            !folder_has_queued_work([TaskStatus::Completed, TaskStatus::Failed].iter()),
            "没有待办 → 不是在等槽位"
        );
        assert!(!folder_has_queued_work(std::iter::empty()));
    }

    /// 「在等槽位」= 没有一个真的在跑、却有在等待队列里。
    ///
    /// 关键是不能用 `is_active_download_status()` 判「在跑」—— 它把 Pending 也算活跃，
    /// 那样 `[Pending, Pending]` 会被判成「有任务在跑」，这个判定就永远不成立。
    #[test]
    fn test_no_progress_but_queued() {
        use crate::downloader::TaskStatus;

        assert!(
            no_progress_but_queued([TaskStatus::Pending, TaskStatus::Pending].iter()),
            "全在等待队列 → 在等槽位"
        );
        assert!(
            !no_progress_but_queued([TaskStatus::Pending, TaskStatus::Decrypting].iter()),
            "解密中也算在动"
        );
        assert!(
            !no_progress_but_queued([TaskStatus::Completed, TaskStatus::Failed].iter()),
            "没有排队中的任务 → 不是在等槽位"
        );
        assert!(
            no_progress_but_queued([TaskStatus::Completed, TaskStatus::Pending].iter()),
            "跑完的不算在跑，剩下的还在排队 → 是在等槽位"
        );
        assert!(
            !no_progress_but_queued(std::iter::empty()),
            "还没建出任务 → 不该误报"
        );
    }

    /// issue #148：被普通任务抢占成 `Paused` 的下载子任务也算「在等槽位」。
    ///
    /// 分享同步的下载是 `TaskPriority::Backup`，普通下载一来就把它踢成 `Paused`。
    /// 此前只认 `Pending`，这批任务既不算在跑也不算在排队 → idle 一路涨到 30 分钟
    /// → run 被判「等待任务完成超时」失败，而它派生的下载子任务还留着，下一轮
    /// 触发就叠出重复项。
    #[test]
    fn test_preempted_paused_counts_as_waiting_for_slot() {
        use crate::downloader::TaskStatus;

        assert!(
            no_progress_but_queued([TaskStatus::Paused, TaskStatus::Paused].iter()),
            "全被抢占暂停 → 仍是在等槽位，不该计入 idle 超时"
        );
        assert!(
            no_progress_but_queued([TaskStatus::Paused, TaskStatus::Pending].iter()),
            "一部分被抢占、一部分排队 → 同样是在等槽位"
        );
        assert!(
            !no_progress_but_queued([TaskStatus::Paused, TaskStatus::Downloading].iter()),
            "还有在跑的 → 走原来的停滞检测，不能把 idle 清零"
        );
    }

    /// 钉死 `is_waiting_for_download_slot` 依赖的关键性质：
    /// **只要有一个在跑就不算「在等槽位」**。
    ///
    /// 场景：pool=1，本订阅的文件 1 正在下载（占着唯一的槽）、文件 2..N 排队。
    /// 池子确实是满的、也确实有 Pending，但这不是「在排队等别人让位」，而是自己
    /// 在跑。若这里判成 true，wait 循环会把 idle 一直清零 —— 万一文件 1 其实卡死
    /// 在 0 字节，stall 重试和 idle 超时就双双失效，run 挂到 7 天硬超时才结束，
    /// 比不做这个改动更糟。
    #[test]
    fn test_waiting_requires_nothing_actually_running() {
        use crate::downloader::TaskStatus;

        assert!(
            !no_progress_but_queued(
                [
                    TaskStatus::Downloading,
                    TaskStatus::Pending,
                    TaskStatus::Pending,
                ]
                    .iter()
            ),
            "槽位被自己占着在下载 → 不是「等槽位」，必须继续走停滞检测"
        );
    }

    /// 文件夹排队等槽位时改报 pending —— 但**只在 Downloading 这一档**改写。
    ///
    /// 三条回归线，都是审查中实际踩到过的坑：
    /// 1. Failed / Cancelled 若被改写成 pending，就从终态变成非终态，
    ///    REST `subtasks()` 的终态过滤失效，失败的文件夹永远挂在「等待中」不消失
    /// 2. Scanning 还在列目录、是真的在干活，说成「等待中」与前端 TRANSFER_ACTIVE
    ///    的口径矛盾
    /// 3. Paused 是用户显式暂停，不是在等槽位
    #[test]
    fn test_folder_subtask_status_only_rewrites_downloading() {
        use crate::downloader::FolderStatus;

        assert_eq!(
            folder_subtask_status(FolderStatus::Downloading, true),
            "pending",
            "下载中 + 子任务全排队 → 改报「等待中」"
        );
        assert_eq!(
            folder_subtask_status(FolderStatus::Downloading, false),
            "downloading",
            "有子任务在跑 → 照实上报"
        );

        for terminal in [FolderStatus::Failed, FolderStatus::Cancelled, FolderStatus::Completed] {
            let s = folder_subtask_status(terminal.clone(), true);
            assert!(
                is_terminal_subtask_status(&s),
                "终态 {:?} 不得被改写成非终态（会绕过 subtasks() 的终态过滤）, 实际={}",
                terminal,
                s
            );
        }
        assert_eq!(
            folder_subtask_status(FolderStatus::Scanning, true),
            "scanning",
            "扫描中是真的在干活，不该说成等待中"
        );
        assert_eq!(
            folder_subtask_status(FolderStatus::Paused, true),
            "paused",
            "用户显式暂停 ≠ 在等槽位"
        );
    }

    /// 改报 pending 后，文件夹仍必须留在 stall 重试的可重启集合里 ——
    /// 否则「有空槽却卡住」的文件夹再也没人救，只能干等到 idle 超时失败。
    #[test]
    fn test_pending_folder_stays_restartable() {
        let mut folder = subtask("pending");
        folder.task_id = "folder:abc".into();
        assert!(
            restartable_share_sync_download(&folder).is_some(),
            "pending 的文件夹必须可被 stall 重试踢一下"
        );
    }

    /// 回归：排队等槽位的下载子任务是 `Pending`，而 stall 重试只认
    /// downloading/paused —— 也就是说「排队」这件事 stall 重试**救不了**。
    ///
    /// 这正是 idle 超时必须对「排队等槽」单独豁免的原因（见 wait_transfer_task
    /// 里的 `is_waiting_for_download_slot` 分支）：否则非会员 1 槽位场景下，
    /// 排在后面的同步会一路空转到 30 分钟 idle_timeout 被判失败。
    #[test]
    fn test_pending_download_subtask_is_not_restartable() {
        assert!(
            restartable_share_sync_download(&subtask("pending")).is_none(),
            "pending（排队等槽位）不是可重启的停滞任务"
        );
        assert!(
            restartable_share_sync_download(&subtask("downloading")).is_some(),
            "downloading 才是 stall 重试的目标"
        );
        assert!(
            restartable_share_sync_download(&subtask("paused")).is_some(),
            "paused 可被重启（但槽位满时由调用方跳过）"
        );
        assert!(
            restartable_share_sync_download(&subtask("completed")).is_none(),
            "终态不该被重启"
        );
    }

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
            kind: crate::transfer::ShareKind::Personal,
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
        //
        // `transferred` 是纯网盘腿的正常终点（auto_download=false，见枚举注释
        // 「转存成功（无自动下载）」）。此前误判成非终态，导致纯网盘订阅每次同步完
        // 都在「进行中子任务」里永久留下一条转存记录。
        for s in [
            "completed",
            "success",
            "failed",
            "cancelled",
            "transferred",
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
            parent_task_id: None,
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

        folder.status = "paused".into();
        assert_eq!(
            restartable_share_sync_download(&folder),
            Some(RestartableShareSyncDownload::Folder("folder-1".into()))
        );

        let mut paused_task = task.clone();
        paused_task.status = "paused".into();
        assert_eq!(
            restartable_share_sync_download(&paused_task),
            Some(RestartableShareSyncDownload::Task("dl-1".into()))
        );
    }

    #[test]
    fn test_restartable_share_sync_download_skips_terminal_and_non_running_tasks() {
        let mut subtask = wait_signature_subtask(10, 1024);

        subtask.kind = "transfer".into();
        assert_eq!(restartable_share_sync_download(&subtask), None);

        subtask.kind = "download".into();
        for status in ["completed", "failed", "cancelled", "pending"] {
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
