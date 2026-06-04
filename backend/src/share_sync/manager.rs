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
use crate::share_sync::events::{NoopShareSyncEventPublisher, ShareSyncEvent, ShareSyncEventPublisher};
use crate::share_sync::executor::{ApplyOutcome, ExecutorHooks, ShareSyncExecutor};
use crate::share_sync::persistence::ShareSyncPersistence;
use crate::share_sync::scheduler::SubscriptionScheduler;
use crate::share_sync::snapshot::{CapturedShare, ShareSnapshot, ShareSnapshotItem, SnapshotCollector};
use crate::transfer::TransferManager;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    /// DownloadManager
    download_manager: Arc<tokio::sync::RwLock<Option<Arc<DownloadManager>>>>,
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
            download_manager: cfg.download_manager,
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

    pub fn create_subscription(self: &Arc<Self>, sub: ShareSubscription) -> Result<ShareSubscription, ShareSyncError> {
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
            ShareSyncError::Internal("网盘客户端未登录，请先登录百度账号".into())
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

        // 2) 绑定 subscription_id 并入库
        let mut curr_snapshot = curr_snapshot;
        curr_snapshot.subscription_id = id.into();
        if let Err(e) = self.persistence.save_snapshot(&curr_snapshot) {
            warn!("save_snapshot 失败: {}", e);
        }

        // 3) diff（用上次快照）
        let prev = self.persistence.latest_snapshot(id).ok().flatten();
        let diff = diff_snapshots(prev.as_ref(), &curr_snapshot);

        self.publisher.publish(ShareSyncEvent::DiffDetected {
            run_id: run_id.clone(),
            subscription_id: id.into(),
            added: diff.added.len(),
            modified: diff.modified.len(),
            removed: diff.removed.len(),
        });

        // 4) 执行
        let hooks = ProductionHooks {
            netdisk: Arc::new(netdisk.clone()),
            transfer: self.transfer_manager.clone(),
            download: self.download_manager.clone(),
            captured: captured.clone(),
        };
        let executor = ShareSyncExecutor::new(&sub, &self.persistence, &hooks);
        let outcome = executor.apply(&captured, &diff).await;

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
                });
            }
            _ => {}
        }
        Ok(outcome)
    }

    fn fail_run(&self, run_id: &str, sub_id: &str, err: &str) {
        use crate::share_sync::types::{DiffSummary, RunStatus};
        let _ = self.persistence.finish_run(
            run_id,
            chrono::Utc::now().timestamp(),
            RunStatus::Failed,
            &DiffSummary::default(),
            Some(err),
        );
        self.publisher.publish(ShareSyncEvent::RunFailed {
            run_id: run_id.into(),
            subscription_id: sub_id.into(),
            error: err.into(),
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

// =====================================================
// 生产环境 ExecutorHooks
// =====================================================

struct ProductionHooks {
    netdisk: Arc<NetdiskClient>,
    transfer: Arc<tokio::sync::RwLock<Option<Arc<TransferManager>>>>,
    download: Arc<tokio::sync::RwLock<Option<Arc<DownloadManager>>>>,
    captured: CapturedShare,
}

impl ExecutorHooks for ProductionHooks {
    fn submit_transfer(
        &self,
        captured: &CapturedShare,
        target_path: &str,
        fs_id: u64,
        internal_label: Option<&str>,
    ) -> Result<String, ShareSyncError> {
        // 异步逻辑：调用 TransferManager
        let transfer_g = self.transfer.clone();
        let captured = captured.clone();
        let target = target_path.to_string();
        let label = internal_label.map(|s| s.to_string());
        let fs_id_str = format!("{}", fs_id);
        let task_id = Uuid::new_v4().to_string();
        // 立即返回 task_id；真正的 transfer 在 spawn 后台执行
        let task_id_clone = task_id.clone();
        tokio::spawn(async move {
            let transfer_opt = { transfer_g.read().await.clone() };
            if let Some(tm) = transfer_opt {
                use crate::transfer::manager::CreateTransferRequest;
                let req = CreateTransferRequest {
                    share_url: format!(
                        "https://pan.baidu.com/s/{}?pwd={}",
                        captured.short_key,
                        captured.password.as_deref().unwrap_or("")
                    ),
                    password: captured.password.clone(),
                    save_path: target,
                    save_fs_id: 0,
                    auto_download: Some(false),
                    local_download_path: None,
                    is_share_direct_download: false,
                    selected_fs_ids: Some(vec![fs_id]),
                    selected_files: None,
                    owner_uid_override: None,
                };
                match tm.create_task(req).await {
                    Ok(resp) => {
                        info!(
                            "share-sync: transfer submitted label={:?} task_id={:?} resp.task_id={:?}",
                            label, task_id_clone, resp.task_id
                        );
                    }
                    Err(e) => {
                        error!("share-sync: transfer 创建失败: {}", e);
                    }
                }
            }
        });
        let _ = fs_id_str;
        Ok(task_id)
    }

    fn submit_download(
        &self,
        fs_id: u64,
        remote_name_hint: &str,
        local_dir: &Path,
    ) -> Result<String, ShareSyncError> {
        let download_g = self.download.clone();
        let dir = local_dir.to_path_buf();
        let name = remote_name_hint.to_string();
        let task_id = Uuid::new_v4().to_string();
        let task_id_clone = task_id.clone();
        tokio::spawn(async move {
            let dl_opt = { download_g.read().await.clone() };
            if let Some(dm) = dl_opt {
                match dm
                    .create_task_with_dir(
                        fs_id,
                        name.clone(),  // remote_path
                        name.clone(),  // filename
                        0,             // total_size（未知，分享侧拿不到准确值）
                        &dir,
                        None,          // conflict_strategy: 由 DownloadConflictStrategy::Overwrite 默认
                    )
                    .await
                {
                    Ok(dl_task_id) => {
                        info!(
                            "share-sync: download submitted task_id={} actual={}",
                            task_id_clone, dl_task_id
                        );
                    }
                    Err(e) => {
                        error!("share-sync: download 创建失败: {}", e);
                    }
                }
            }
        });
        Ok(task_id)
    }

    fn delete_netdisk(
        &self,
        target_path: &str,
        relative_paths: &[String],
    ) -> Result<(), ShareSyncError> {
        let netdisk = self.netdisk.clone();
        let target = target_path.to_string();
        let paths = relative_paths.to_vec();
        tokio::spawn(async move {
            match netdisk.delete_files(&paths).await {
                Ok(resp) => info!(
                    "share-sync: netdisk delete 成功 {}/{} from {}",
                    resp.deleted_count, paths.len(), target
                ),
                Err(e) => error!("share-sync: netdisk delete 失败: {}", e),
            }
        });
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::config::{LocalTarget, NetdiskTarget, SyncTarget};
    use crate::share_sync::events::NoopShareSyncEventPublisher;
    use tempfile::tempdir;

    fn sub(name: &str) -> ShareSubscription {
        ShareSubscription::new(
            name.into(),
            "https://pan.baidu.com/s/1abc".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: PathBuf::from("/tmp/x"),
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
