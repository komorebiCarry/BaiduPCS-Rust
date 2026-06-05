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

use crate::downloader::DownloadManager;
use crate::netdisk::client::NetdiskClient;
use crate::share_sync::config::ShareSubscription;
use crate::share_sync::diff::diff_snapshots;
use crate::share_sync::error::ShareSyncError;
use crate::share_sync::events::{
    NoopShareSyncEventPublisher, ShareSyncEvent, ShareSyncEventPublisher,
};
use crate::share_sync::executor::{
    ApplyOutcome, ExecutorHooks, NetdiskTargetEntry, ShareSyncExecutor,
};
use crate::share_sync::persistence::ShareSyncPersistence;
use crate::share_sync::scheduler::SubscriptionScheduler;
use crate::share_sync::snapshot::{CapturedShare, ShareSnapshotItem, SnapshotCollector};
use crate::share_sync::types::{ConflictStrategy, RunStatus};
use crate::transfer::{TransferManager, TransferStatus};
use async_trait::async_trait;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

/// 顶层 Manager
pub struct ShareSyncManager {
    /// 订阅 ID → 最新配置（in-memory 权威）
    subscriptions: DashMap<String, ShareSubscription>,
    /// 订阅 ID → Scheduler
    schedulers: DashMap<String, SubscriptionScheduler>,
    /// 持久化层
    persistence: Arc<ShareSyncPersistence>,
    /// 配置文件路径（JSON）
    config_path: PathBuf,
    /// 事件发布器
    publisher: Arc<dyn ShareSyncEventPublisher>,
    /// NetdiskClient（Option 化以支持初始化时尚未登录）
    netdisk_client: Arc<tokio::sync::RwLock<Option<NetdiskClient>>>,
    /// TransferManager（同上）
    transfer_manager: Arc<tokio::sync::RwLock<Option<Arc<TransferManager>>>>,
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
    pub netdisk_client: Arc<tokio::sync::RwLock<Option<NetdiskClient>>>,
    pub transfer_manager: Arc<tokio::sync::RwLock<Option<Arc<TransferManager>>>>,
    pub download_manager: Arc<tokio::sync::RwLock<Option<Arc<DownloadManager>>>>,
    pub publisher: Option<Arc<dyn ShareSyncEventPublisher>>,
}

impl ShareSyncManager {
    /// 构造并恢复订阅
    pub async fn new(cfg: ManagerConfig) -> Result<Arc<Self>, ShareSyncError> {
        let persistence = Arc::new(ShareSyncPersistence::new(&cfg.db_path)?);

        if let Some(parent) = cfg.config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 从 JSON 恢复（缺失则空）
        let subs: Vec<ShareSubscription> = if cfg.config_path.exists() {
            let s = std::fs::read_to_string(&cfg.config_path).unwrap_or_default();
            serde_json::from_str(&s).unwrap_or_default()
        } else {
            Vec::new()
        };

        let manager = Arc::new(Self {
            subscriptions: DashMap::new(),
            schedulers: DashMap::new(),
            persistence,
            config_path: cfg.config_path,
            publisher: cfg
                .publisher
                .unwrap_or_else(|| Arc::new(NoopShareSyncEventPublisher)),
            netdisk_client: cfg.netdisk_client,
            transfer_manager: cfg.transfer_manager,
        });

        for sub in subs {
            // 同步到 DB（便于诊断 / 后台 UI）
            let _ = manager.persistence.upsert_subscription(&sub);
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
        Ok(manager)
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

    pub fn get_subscription(&self, id: &str) -> Option<ShareSubscription> {
        self.subscriptions.get(id).map(|kv| kv.value().clone())
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
        self.persist_to_disk();
        if sub.enabled && sub.poll_config.enabled {
            self.start_scheduler_for(&sub);
        }
        self.publisher.publish(ShareSyncEvent::SubscriptionCreated {
            subscription_id: sub.id.clone(),
            name: sub.name.clone(),
        });
        info!("ShareSyncManager: 创建订阅 id={}", sub.id);
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
        self.persist_to_disk();
        // 重启 scheduler（间隔可能变了）
        self.stop_scheduler_for(id);
        if new_sub.enabled && new_sub.poll_config.enabled {
            self.start_scheduler_for(&new_sub);
        }
        self.publisher.publish(ShareSyncEvent::SubscriptionUpdated {
            subscription_id: id.into(),
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
        self.persist_to_disk();
        if enabled && sub_clone.poll_config.enabled {
            self.start_scheduler_for(&sub_clone);
        } else {
            self.stop_scheduler_for(id);
        }
        self.publisher.publish(ShareSyncEvent::StatusChanged {
            subscription_id: id.into(),
            enabled,
        });
        Ok(())
    }

    pub fn delete_subscription(self: &Arc<Self>, id: &str) -> Result<(), ShareSyncError> {
        if self.subscriptions.remove(id).is_none() {
            return Err(ShareSyncError::SubscriptionNotFound(id.into()));
        }
        self.stop_scheduler_for(id);
        // DB 删除（级联清理 snapshots/runs）
        let _ = self.persistence.delete_subscription(id);
        self.persist_to_disk();
        self.publisher.publish(ShareSyncEvent::SubscriptionDeleted {
            subscription_id: id.into(),
        });
        Ok(())
    }

    // ===================================================
    // 触发 / 执行
    // ===================================================

    /// 立即触发一次（HTTP / 手动）
    pub fn trigger_one(self: &Arc<Self>, id: &str) -> Result<String, ShareSyncError> {
        let sub = self
            .get_subscription(id)
            .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
        if let Some(sched) = self.schedulers.get(id) {
            sched.trigger_now();
            // 实际 run 由 scheduler 的 on_tick 触发
            Ok(sub.id)
        } else {
            // 没有 scheduler（被禁用），后台执行一次
            let mgr = Arc::clone(self);
            let id_owned = id.to_string();
            tokio::spawn(async move {
                let _ = mgr.execute_one(&id_owned).await;
            });
            Ok(sub.id)
        }
    }

    /// 执行一次（由 scheduler 调用或 trigger_one 同步入口）
    pub async fn execute_one(&self, id: &str) -> Result<ApplyOutcome, ShareSyncError> {
        let sub = self
            .get_subscription(id)
            .ok_or_else(|| ShareSyncError::SubscriptionNotFound(id.into()))?;
        let netdisk = {
            let g = self.netdisk_client.read().await;
            g.clone()
        };
        let netdisk = netdisk.ok_or_else(|| {
            ShareSyncError::ConfigError("网盘客户端未登录，请先登录百度账号".into())
        })?;

        let run_id = Uuid::new_v4().to_string();
        self.publisher.publish(ShareSyncEvent::RunStarted {
            run_id: run_id.clone(),
            subscription_id: id.into(),
        });

        // 1) 抓取
        let (captured, curr_snapshot) = match SnapshotCollector::from_url(
            &netdisk,
            &sub.share_url,
            sub.password.clone(),
            sub.include_paths.clone(),
            sub.exclude_patterns.clone(),
        )
        .await
        {
            Ok(collector) => match collector.collect().await {
                Ok(t) => t,
                Err(e) => {
                    self.fail_run(&run_id, id, &format!("抓取失败: {}", e));
                    return Err(e);
                }
            },
            Err(e) => {
                self.fail_run(&run_id, id, &format!("抓取初始化失败: {}", e));
                return Err(e);
            }
        };

        // 2) 绑定 subscription_id 后，先读"上次成功应用的快照"再计算 diff。
        //    当前快照必须等执行成功后才能推进基线；否则下载/转存失败会把
        //    未落地的新版本标记成已同步，后续轮询 diff 变空而不再重试。
        let mut curr_snapshot = curr_snapshot;
        curr_snapshot.subscription_id = id.into();
        let prev = self.persistence.latest_snapshot(id).ok().flatten();

        // 3) diff
        let diff = diff_snapshots(prev.as_ref(), &curr_snapshot);

        self.publisher.publish(ShareSyncEvent::DiffDetected {
            run_id: run_id.clone(),
            subscription_id: id.into(),
            added: diff.added.iter().filter(|i| !i.is_dir).count(),
            modified: diff.modified.iter().filter(|i| !i.new.is_dir).count(),
            removed: diff.removed.iter().filter(|i| !i.is_dir).count(),
        });

        // 4) 执行
        let hooks = ProductionHooks {
            netdisk: Arc::new(netdisk.clone()),
            transfer: self.transfer_manager.clone(),
            captured: captured.clone(),
        };
        let executor = ShareSyncExecutor::new(&sub, &self.persistence, &hooks);
        let outcome = executor
            .apply_with_run_id(run_id.clone(), &captured, &diff)
            .await;

        if should_advance_snapshot_baseline(outcome.status) {
            if let Err(e) = self.persistence.save_snapshot(&curr_snapshot) {
                warn!("save_snapshot 失败，下一次同步会重试本次 diff: {}", e);
            }
        } else {
            warn!(
                "share-sync: run 未完全成功，不推进快照基线，下一次将重试 diff: run_id={}, status={:?}, failed={}",
                outcome.run_id, outcome.status, outcome.diff_summary.failed
            );
        }

        // 5) 广播
        match outcome.status {
            crate::share_sync::types::RunStatus::Completed
            | crate::share_sync::types::RunStatus::CompletedWithErrors => {
                self.publisher.publish(ShareSyncEvent::RunCompleted {
                    run_id: outcome.run_id.clone(),
                    subscription_id: id.into(),
                    added: outcome.diff_summary.added,
                    modified: outcome.diff_summary.modified,
                    removed: outcome.diff_summary.removed,
                    failed: outcome.diff_summary.failed,
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

    fn fail_run(&self, run_id: &str, sub_id: &str, err: &str) {
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
                let _ = mgr2.execute_one(&id).await;
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

    // ===================================================
    // 内部辅助
    // ===================================================

    fn persist_to_disk(&self) {
        let all: Vec<ShareSubscription> = self
            .subscriptions
            .iter()
            .map(|kv| kv.value().clone())
            .collect();
        match serde_json::to_string_pretty(&all) {
            Ok(s) => {
                if let Err(e) = std::fs::write(&self.config_path, s) {
                    error!("写订阅 JSON 失败: {}", e);
                }
            }
            Err(e) => error!("序列化订阅失败: {}", e),
        }
    }
}

fn should_advance_snapshot_baseline(status: RunStatus) -> bool {
    matches!(status, RunStatus::Completed)
}

// =====================================================
// 生产环境 ExecutorHooks
// =====================================================

struct ProductionHooks {
    netdisk: Arc<NetdiskClient>,
    transfer: Arc<tokio::sync::RwLock<Option<Arc<TransferManager>>>>,
    captured: CapturedShare,
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
        let tm = self.transfer_manager().await?;
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
    ) -> Result<String, ShareSyncError> {
        let local_download_root = share_direct_download_root(local_dir, item)?;
        let tm = self.transfer_manager().await?;
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
            save_path: String::new(),
            save_fs_id: 0,
            auto_download: Some(true),
            local_download_path: Some(local_download_root.to_string_lossy().to_string()),
            is_share_direct_download: true,
            download_conflict_strategy: Some(download_conflict_strategy_for_share_sync(strategy)),
            selected_fs_ids: Some(vec![item.fs_id]),
            selected_files: Some(vec![SharedFileInfo {
                fs_id: item.fs_id,
                is_dir: false,
                path: raw_path.clone(),
                size: item.size,
                name: item.name.clone(),
            }]),
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
            "share-sync: share-direct download submitted task_id={} path={} local_root={:?}",
            task_id, raw_path, local_download_root
        );
        Ok(task_id)
    }

    async fn wait_transfer_task(
        &self,
        task_id: &str,
        require_download_completion: bool,
        timeout: Duration,
    ) -> Result<(), ShareSyncError> {
        let tm = self.transfer_manager().await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let task = tm.get_task(task_id).await.ok_or_else(|| {
                ShareSyncError::TransferError(format!("转存任务不存在: {}", task_id))
            })?;
            match task.status {
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

            if tokio::time::Instant::now() >= deadline {
                let msg = format!(
                    "等待任务完成超时: task_id={}, status={:?}",
                    task_id, task.status
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
        let tm = self.transfer_manager().await?;
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
            save_path: target_dir.to_string(),
            save_fs_id: 0,
            // 网盘目标不下载本地，与单文件版本一致
            auto_download: Some(false),
            local_download_path: None,
            is_share_direct_download: false,
            download_conflict_strategy: None,
            selected_fs_ids: Some(selected_fs_ids),
            selected_files: Some(selected_files),
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
    ) -> Result<String, ShareSyncError> {
        if items.is_empty() {
            return Err(ShareSyncError::Internal(
                "submit_download_batch 被传入空 items 列表".to_string(),
            ));
        }
        let tm = self.transfer_manager().await?;
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
                    is_dir: false, // executor 不传目录项
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
            // 走 is_share_direct_download=true 路径，save_path 在 transfer 里
            // 会被 temp_dir 强制覆盖——这是 transfer 的硬编码行为，不在 share-sync
            // 控制范围。最终落点是 `local_download_path`（自动下载阶段被消费）。
            save_path: String::new(),
            save_fs_id: 0,
            auto_download: Some(true),
            local_download_path: Some(local_dir.to_string_lossy().to_string()),
            is_share_direct_download: true,
            download_conflict_strategy: Some(download_conflict_strategy_for_share_sync(strategy)),
            selected_fs_ids: Some(selected_fs_ids),
            selected_files: Some(selected_files),
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
    async fn transfer_manager(&self) -> Result<Arc<TransferManager>, ShareSyncError> {
        self.transfer
            .read()
            .await
            .clone()
            .ok_or_else(|| ShareSyncError::ConfigError("TransferManager 未初始化".into()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::config::{LocalTarget, SyncTarget};
    use crate::share_sync::events::NoopShareSyncEventPublisher;
    use tempfile::tempdir;

    fn sub(name: &str) -> ShareSubscription {
        ShareSubscription::new(
            name.into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: std::env::temp_dir(),
                conflict_strategy: None,
            })],
        )
    }

    fn empty_managers() -> (
        Arc<tokio::sync::RwLock<Option<NetdiskClient>>>,
        Arc<tokio::sync::RwLock<Option<Arc<TransferManager>>>>,
        Arc<tokio::sync::RwLock<Option<Arc<DownloadManager>>>>,
    ) {
        (
            Arc::new(tokio::sync::RwLock::new(None)),
            Arc::new(tokio::sync::RwLock::new(None)),
            Arc::new(tokio::sync::RwLock::new(None)),
        )
    }

    #[tokio::test]
    async fn test_new_manager_empty() {
        let dir = tempdir().unwrap();
        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
        .await
        .unwrap();
        assert_eq!(m.list_subscriptions().len(), 0);
    }

    #[tokio::test]
    async fn test_create_get_delete() {
        let dir = tempdir().unwrap();
        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
        .await
        .unwrap();
        let s = m.create_subscription(sub("a")).unwrap();
        assert_eq!(m.list_subscriptions().len(), 1);
        assert!(m.get_subscription(&s.id).is_some());

        // JSON 已写入
        let json_path = dir.path().join("subs.json");
        assert!(json_path.exists());

        m.delete_subscription(&s.id).unwrap();
        assert_eq!(m.list_subscriptions().len(), 0);
    }

    #[tokio::test]
    async fn test_update_subscription_preserves_id_and_created_at() {
        let dir = tempdir().unwrap();
        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
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
        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
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

    #[tokio::test]
    async fn test_create_invalid_subscription_rejected() {
        let dir = tempdir().unwrap();
        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
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

        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: json_path,
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
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
        let (net, tx, dl) = empty_managers();
        let m = ShareSyncManager::new(ManagerConfig {
            config_path: dir.path().join("subs.json"),
            db_path: dir.path().join("s.db"),
            netdisk_client: net,
            transfer_manager: tx,
            download_manager: dl,
            publisher: Some(Arc::new(NoopShareSyncEventPublisher)),
        })
        .await
        .unwrap();
        let s = m.create_subscription(sub("a")).unwrap();
        // netdisk_client 为 None → 应报错
        let r = m.execute_one(&s.id).await;
        assert!(r.is_err());
    }
}
