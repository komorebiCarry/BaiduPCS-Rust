use crate::auth::UserAuth;
use crate::autobackup::events::BackupTransferNotification;
use crate::common::{
    ProxyConfig, RefreshCoordinator, RefreshCoordinatorConfig, SpeedAnomalyConfig, StagnationConfig,
};
use crate::downloader::{
    calculate_task_max_chunks, ChunkScheduler, DownloadEngine, DownloadTask, TaskScheduleInfo,
    TaskStatus, FolderDownloadManager,
};
use crate::task_slot_pool::{TaskSlotPool, TaskPriority};
use crate::persistence::{
    DownloadRecoveryInfo, PersistenceManager, TaskMetadata,
};
use crate::server::events::{DownloadEvent, ProgressThrottler, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 🔥 auto_requeue 退回冷却时长（秒）
///
/// 任务从"无可用分片" / Fatal 退回等待队列后，至少冷却这段时间才会被重新拉起。
/// 防止"失败→立即拉起→立即失败"的死循环。
pub const REQUEUE_COOLDOWN_SECS: i64 = 60;

/// 🔥 启动阶段（prepare / register）失败重试上限
///
/// 三条启动链路统一使用：
/// - `start_task_internal` 主流程（handle_task_failure）
/// - `start_waiting_queue_monitor` 后台监控（handle_task_failure）
/// - 0 延迟触发器（内联失败处理）
///
/// 文件夹子任务或备份任务在启动阶段累计失败 `MAX_START_RETRIES` 次后，不再无限重入等待队列，
/// 改为标记为 `Failed` 以避免死循环 / 队列堵塞。普通单文件任务从第 1 次失败即标记 Failed。
pub const MAX_START_RETRIES: u32 = 3;

/// 🔥 自动退回队列请求（scheduler → manager 消息）
///
/// scheduler 不能直接调 `manager.auto_requeue_task`（manager 在 scheduler
/// 之上持有 Arc，循环依赖）。通过 mpsc channel 解耦。
#[derive(Debug, Clone)]
pub struct AutoRequeueRequest {
    pub task_id: String,
    pub reason: String,
}

/// 下载任务「聚合用」终态查询结果（见 `DownloadManager::lookup_aggregate_outcome`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadAggregateOutcome {
    /// 任务仍在内存任务表中，附带其当前状态。
    InMemory(TaskStatus),
    /// 内存中已不存在，但历史库标记为已完成（快速完成后被归档移除）。
    ArchivedCompleted,
    /// 内存中已不存在，历史库标记为失败。
    ArchivedFailed,
    /// 内存与历史库均查不到（真正取消/丢失）。
    NotFound,
}

/// 下载管理器
#[derive(Debug)]
pub struct DownloadManager {
    /// 所有任务
    tasks: Arc<RwLock<HashMap<String, Arc<Mutex<DownloadTask>>>>>,
    /// 任务取消令牌（task_id -> CancellationToken）
    cancellation_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
    /// 等待队列（task_id 列表，FIFO）
    waiting_queue: Arc<RwLock<VecDeque<String>>>,
    /// 下载引擎
    engine: Arc<DownloadEngine>,
    /// 默认下载目录（使用 RwLock 支持动态更新）
    download_dir: Arc<RwLock<PathBuf>>,
    /// 全局分片调度器
    chunk_scheduler: ChunkScheduler,
    /// 最大同时下载任务数（动态可调整）
    ///
    /// 用 `Arc<AtomicUsize>` 而不是 `usize`：
    /// 旧版本是 `usize`，`update_max_concurrent_tasks` 内部
    /// 用 `let old_max = self.max_concurrent_tasks.load(Ordering::SeqCst)` 读后又因为 `&self` 不能写回，
    /// 注释自承"这个字段只在创建时使用"。多次调用时（5→3→4）旧值始终是 5，
    /// 误判为"调小"，不会走 `try_start_waiting_tasks()`。改成 `Arc<AtomicUsize>` +
    /// `swap` 后，old_max 与上一次写入值严格一致。
    max_concurrent_tasks: Arc<AtomicUsize>,
    /// 🔥 持久化管理器引用（可选）
    persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,
    /// 🔥 WebSocket 管理器
    ws_manager: Arc<RwLock<Option<Arc<WebSocketManager>>>>,
    /// 🔥 文件夹进度通知发送器（由子任务进度变化触发）
    folder_progress_tx: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<String>>>>,
    /// 🔥 备份任务统一通知发送器（进度、状态、完成、失败等）
    backup_notification_tx: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>>>>,
    /// 🔥 任务位池管理器
    task_slot_pool: Arc<TaskSlotPool>,
    /// 🔥 文件夹下载管理器引用（可选，用于回收借调槽位）
    folder_manager: Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
    /// 🔥 加密快照管理器（用于查询加密文件映射，获取原始文件名）
    snapshot_manager: Arc<RwLock<Option<Arc<crate::encryption::snapshot::SnapshotManager>>>>,
    /// 🔥 加密配置存储（用于根据 key_version 选择正确的解密密钥）
    encryption_config_store: Arc<RwLock<Option<Arc<crate::encryption::EncryptionConfigStore>>>>,
    /// 🔥 链接级重试次数（从配置读取，传递给 TaskScheduleInfo）
    max_retries: u32,
    /// 🔥 活跃任务计数（O(1) 查询，漂移校准每 60 秒）
    active_count: Arc<AtomicUsize>,
    /// 🔥 auto_requeue 请求发送端（传给 scheduler，以便 scheduler 失败时回调）
    requeue_tx: mpsc::UnboundedSender<AutoRequeueRequest>,
    /// 🔥 auto_requeue 请求接收端（start_auto_requeue_consumer 消费用）
    requeue_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<AutoRequeueRequest>>>>,
    /// 🔥 多账号：该管理器所属的账号 UID（`Uid(0)` 表示遗留单账号场景）
    owner_uid: crate::auth::Uid,
}

impl DownloadManager {
    /// 创建新的下载管理器
    ///
    /// 测试便捷构造函数 — 内部新建一个临时
    /// `BudgetScheduler` + `Semaphore` 用于单元测试场景。生产路径请用
    /// `new_for_account`。
    ///
    /// 构造时按 `user_auth.uid` 注册账号到 `BudgetScheduler`（双轨预算池），
    /// 否则任何调用 `acquire_chunk_permit` 的真实分片下载会因 unknown account
    /// 永远拿不到 permit 而卡死。这里在构造时按 `user_auth.uid` 调用
    /// `add_account` 把测试账号 seed 进去；调用方需在 tokio 运行时下使用
    /// （`#[tokio::test]`）。
    #[cfg(test)]
    pub async fn new(user_auth: UserAuth, download_dir: PathBuf) -> Result<Self> {
        use crate::auth::Uid;
        use crate::downloader::budget_scheduler::{
            BudgetScheduler, BudgetSchedulerConfig, RequestedSource, VipType,
        };

        let budget_scheduler = BudgetScheduler::new(BudgetSchedulerConfig::default());
        // seed 当前测试账号到双轨预算池
        let uid = Uid::new(user_auth.uid);
        let vip = VipType::from_raw(user_auth.vip_type);
        budget_scheduler
            .add_account(
                uid,
                vip,
                RequestedSource::Auto,
                RequestedSource::Auto,
            )
            .await;

        let decrypt_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(
            ChunkScheduler::calculate_decrypt_concurrency(),
        ));
        Self::new_for_account(
            user_auth,
            download_dir,
            10,
            5,
            3,
            None,
            None,
            budget_scheduler,
            decrypt_semaphore,
        )
    }

    /// 🔥 多账号唯一构造入口
    ///
    /// 旧的两阶段 `DownloadManager::with_config(...) → wire_global_resources(...)`
    /// 已删除：多账号下载**唯一**走 `BudgetScheduler::acquire_chunk_permit(uid, BudgetKind::Download)`
    /// 一条路径，构造时直接注入避免漏调 wire 的破口（与上传侧对称）。
    ///
    /// # 参数
    /// * `user_auth` - 用户认证信息（提供 uid、bduss）
    /// * `download_dir` - 默认下载目录（必须存在或可创建）
    /// * `max_global_threads` - 全局分片线程数
    /// * `max_concurrent_tasks` - 最大同时下载任务数
    /// * `max_retries` - 单分片最大重试次数
    /// * `proxy_config` / `fallback_mgr` - 代理配置（可选）
    /// * `budget_scheduler` - 多账号配额调度器（必填，每分片 acquire 唯一来源）
    /// * `decrypt_semaphore` - 全局解密 CPU 闸门（机器级单例）
    pub fn new_for_account(
        user_auth: UserAuth,
        download_dir: PathBuf,
        max_global_threads: usize,
        max_concurrent_tasks: usize,
        max_retries: u32,
        proxy_config: Option<&ProxyConfig>,
        fallback_mgr: Option<std::sync::Arc<crate::common::ProxyFallbackManager>>,
        budget_scheduler: std::sync::Arc<crate::downloader::budget_scheduler::BudgetScheduler>,
        decrypt_semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    ) -> Result<Self> {
        // 确保下载目录存在（路径验证已在配置保存时完成）
        if !download_dir.exists() {
            std::fs::create_dir_all(&download_dir).context("创建下载目录失败")?;
            info!("✓ 下载目录已创建: {:?}", download_dir);
        }

        // 创建全局分片调度器（构造时直接注入 budget + decrypt + uid）
        //
        // 🔥 调度器 owner_uid 设计：当前架构是 per-uid 独立
        // `DownloadManager` 实例，每个 manager 内部都有自己的 `ChunkScheduler`。
        // scheduler 内部的 owner_uid 仍用 `Uid::default()` 占位禁用 scheduler-level
        // assert（保留：单 manager 内同时调度多任务时 task.owner_uid 仍可能不同；
        // 历史共享 Arc 路径与测试路径仍可能让多账号 task 流入同一调度器）；
        // budget acquire 在 spawn_chunk_download 内按 `task.owner_uid` 路由，
        // 确保跨账号场景 budget 从正确账号桶借调。chunk-level 防御走
        // `ChunkManager::assert_chunk_owner(task_uid, &chunk)`。
        let chunk_scheduler = ChunkScheduler::new(
            max_global_threads,
            max_concurrent_tasks,
            budget_scheduler,
            decrypt_semaphore,
            crate::auth::Uid::default(),
        );

        info!(
            "创建下载管理器（per-uid uid={}）: 下载目录={:?}, 全局线程数={}, 最大同时下载数={} (分片大小自适应)",
            user_auth.uid, download_dir, max_global_threads, max_concurrent_tasks
        );

        let engine = Arc::new(DownloadEngine::new_with_proxy(user_auth.clone(), proxy_config, fallback_mgr));

        // 🔥 auto_requeue channel
        let (requeue_tx, requeue_rx) = mpsc::unbounded_channel();

        let manager = Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            waiting_queue: Arc::new(RwLock::new(VecDeque::new())),
            engine,
            download_dir: Arc::new(RwLock::new(download_dir)),
            chunk_scheduler,
            max_concurrent_tasks: Arc::new(AtomicUsize::new(max_concurrent_tasks)),
            persistence_manager: None,
            ws_manager: Arc::new(RwLock::new(None)),
            folder_progress_tx: Arc::new(RwLock::new(None)),
            backup_notification_tx: Arc::new(RwLock::new(None)),
            task_slot_pool: {
                let pool = Arc::new(TaskSlotPool::new(max_concurrent_tasks));
                // 🔥 启动槽位清理后台任务（托管模式，JoinHandle 会被保存以便 shutdown 时取消）
                {
                    let pool_clone = pool.clone();
                    tokio::spawn(async move {
                        pool_clone.start_cleanup_task_managed().await;
                    });
                }
                pool
            },
            folder_manager: Arc::new(RwLock::new(None)),
            snapshot_manager: Arc::new(RwLock::new(None)),
            encryption_config_store: Arc::new(RwLock::new(None)),
            max_retries,
            active_count: Arc::new(AtomicUsize::new(0)),
            requeue_tx,
            requeue_rx: Arc::new(Mutex::new(Some(requeue_rx))),
            // 🔥 多账号归属（从 user_auth.uid 提取）
            owner_uid: crate::auth::Uid::new(user_auth.uid),
        };

        // 🔥 设置槽位超时释放处理器
        manager.setup_stale_release_handler();

        // 🔥 启动活跃计数漂移校准（每 60 秒）
        // 注意：start_waiting_queue_monitor 和 setup_waiting_queue_trigger 已移至
        // set_persistence_manager() 中调用，确保它们捕获到有效的 persistence_manager
        {
            let tasks_ref = manager.tasks.clone();
            let counter = manager.active_count.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    let tasks = tasks_ref.read().await;
                    let mut real = 0usize;
                    for task_arc in tasks.values() {
                        let t = task_arc.lock().await;
                        if matches!(t.status, TaskStatus::Pending | TaskStatus::Downloading | TaskStatus::Decrypting) {
                            real += 1;
                        }
                    }
                    drop(tasks);
                    let stored = counter.load(Ordering::SeqCst);
                    if stored != real {
                        tracing::warn!("download active_count 漂移校准: {} -> {}", stored, real);
                        counter.store(real, Ordering::SeqCst);
                    }
                }
            });
        }

        Ok(manager)
    }

    /// 🔥 多账号：设置该管理器所属的账号 UID
    pub fn set_owner_uid(&mut self, uid: crate::auth::Uid) {
        self.owner_uid = uid;
    }

    /// 🔥 多账号：获取该管理器所属的账号 UID
    pub fn owner_uid(&self) -> crate::auth::Uid {
        self.owner_uid
    }

    // 🔥 旧的两阶段预算注入入口已删除。
    // BudgetScheduler / decrypt_semaphore / owner_uid 由 `new_for_account` 构造时
    // 一次性注入；分片调度路径不再有 None fallback。结构性消除"漏调 wire 时
    // 分片调度静默 fallback 绕过 budget"的破口（与上传侧对称）。

    /// 🔥 覆盖单个任务的归属 UID
    ///
    /// 用于 handler 接收到 `req.uid` 显式归属时，纠正 `create_task` 默认使用的
    /// `self.owner_uid`（active 账号）。同时同步更新 `.meta` 持久化数据，使重启
    /// 恢复后任务依然归属到正确账号。
    ///
    /// 语义：
    /// - 任务必须存在于 `self.tasks`，否则返回 `Ok(())` 静默忽略（已迁出/被删除）
    /// - 仅修改运行态 `DownloadTask.owner_uid` + 持久化 `.meta`，不调度任何重算
    /// - 必须在 `start_task` 之前调用，否则 BudgetScheduler 已按旧 owner_uid 借调 permit
    pub async fn override_task_owner_uid(
        &self,
        task_id: &str,
        new_owner_uid: crate::auth::Uid,
    ) -> anyhow::Result<()> {
        // 1) 修改运行态 task
        let task_arc_opt = {
            let tasks = self.tasks.read().await;
            tasks.get(task_id).cloned()
        };
        if let Some(task_arc) = task_arc_opt {
            let mut t = task_arc.lock().await;
            if t.owner_uid == new_owner_uid {
                return Ok(());
            }
            t.owner_uid = new_owner_uid;
        } else {
            tracing::warn!(
                "override_task_owner_uid: 任务不存在于运行态 tasks: {}（可能已迁出）",
                task_id
            );
        }

        // 2) 同步持久化 .meta
        if let Some(ref pm) = self.persistence_manager {
            pm.lock()
                .await
                .update_download_owner_uid(task_id, new_owner_uid.raw())
                .map_err(|e| anyhow::anyhow!("更新 .meta owner_uid 失败: {e}"))?;
        }
        Ok(())
    }

    /// 🔥 设置持久化管理器
    ///
    /// 由 AppState 在初始化时调用，注入持久化管理器
    pub fn set_persistence_manager(&mut self, pm: Arc<Mutex<PersistenceManager>>) {
        self.persistence_manager = Some(pm);
        info!("下载管理器已设置持久化管理器");
        // 🔥 在 persistence_manager 设置完成后启动依赖它的后台任务
        // 这样两个 monitor 捕获的 self.persistence_manager 克隆为 Some(pm) 而非 None
        self.start_waiting_queue_monitor();
        self.setup_waiting_queue_trigger();
    }

    /// 热更新代理配置（由 update_config handler 调用）
    /// 直接通过 self.engine 调用 DownloadEngine 的方法，无需中间引用
    pub fn update_proxy_config(&self, new_proxy: Option<&ProxyConfig>) {
        self.engine.update_proxy_and_rebuild_client(new_proxy);
    }

    /// 🔥 设置 WebSocket 管理器
    ///
    /// 由 AppState 在初始化时调用，注入 WebSocket 管理器用于直接推送
    pub async fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        let mut guard = self.ws_manager.write().await;
        *guard = Some(ws_manager);
        info!("下载管理器已设置 WebSocket 管理器");
    }

    /// 🔥 获取 WebSocket 管理器引用
    pub async fn get_ws_manager(&self) -> Option<Arc<WebSocketManager>> {
        let guard = self.ws_manager.read().await;
        guard.clone()
    }

    /// 🔥 设置快照管理器
    ///
    /// 由 AppState 在初始化时调用，注入快照管理器用于查询加密文件映射
    pub async fn set_snapshot_manager(&self, snapshot_manager: Arc<crate::encryption::snapshot::SnapshotManager>) {
        let mut guard = self.snapshot_manager.write().await;
        *guard = Some(snapshot_manager);
        info!("下载管理器已设置快照管理器");
    }

    /// 🔥 获取快照管理器引用
    pub async fn get_snapshot_manager(&self) -> Option<Arc<crate::encryption::snapshot::SnapshotManager>> {
        let guard = self.snapshot_manager.read().await;
        guard.clone()
    }

    /// 🔥 设置加密配置存储
    ///
    /// 由 AppState 在初始化时调用，注入加密配置存储用于根据 key_version 选择正确的解密密钥
    pub async fn set_encryption_config_store(&self, config_store: Arc<crate::encryption::EncryptionConfigStore>) {
        let mut guard = self.encryption_config_store.write().await;
        *guard = Some(config_store);
        info!("下载管理器已设置加密配置存储");
    }

    /// 🔥 获取加密配置存储引用
    pub async fn get_encryption_config_store(&self) -> Option<Arc<crate::encryption::EncryptionConfigStore>> {
        let guard = self.encryption_config_store.read().await;
        guard.clone()
    }

    /// 获取持久化管理器引用
    pub fn persistence_manager(&self) -> Option<&Arc<Mutex<PersistenceManager>>> {
        self.persistence_manager.as_ref()
    }

    /// 🔥 获取任务位池管理器引用
    pub fn task_slot_pool(&self) -> Arc<TaskSlotPool> {
        self.task_slot_pool.clone()
    }

    /// 🔥 发布下载事件
    async fn publish_event(&self, event: DownloadEvent) {
        // 🔥 如果是备份任务，不发送普通的 WebSocket 事件
        // 备份任务的事件由 AutoBackupManager 统一处理
        if event.is_backup() {
            return;
        }

        let ws = self.ws_manager.read().await;
        if let Some(ref ws) = *ws {
            let group_id = event.group_id().map(|s| s.to_string());
            ws.send_if_subscribed(TaskEvent::Download(event), group_id);
        }
    }

    /// 创建下载任务
    pub async fn create_task(
        &self,
        fs_id: u64,
        remote_path: String,
        filename: String,
        total_size: u64,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
    ) -> Result<String> {
        let download_dir = self.download_dir.read().await;
        let local_path = download_dir.join(&filename);
        drop(download_dir);

        self.create_task_internal(fs_id, remote_path, local_path, total_size, conflict_strategy, None)
            .await
    }

    /// 🔥 创建下载任务（显式 owner_uid）
    ///
    /// 让 task / 持久化 / Created event / 后续所有事件都使用 `owner_uid`，
    /// 避免事后 override 与 Created event / async execution 竞态。
    pub async fn create_task_with_owner(
        &self,
        fs_id: u64,
        remote_path: String,
        filename: String,
        total_size: u64,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        let download_dir = self.download_dir.read().await;
        let local_path = download_dir.join(&filename);
        drop(download_dir);

        self.create_task_internal(
            fs_id,
            remote_path,
            local_path,
            total_size,
            conflict_strategy,
            Some(owner_uid),
        )
            .await
    }

    /// 创建下载任务（指定下载目录）
    ///
    /// 用于批量下载时支持自定义下载目录
    pub async fn create_task_with_dir(
        &self,
        fs_id: u64,
        remote_path: String,
        filename: String,
        total_size: u64,
        target_dir: &std::path::Path,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
    ) -> Result<String> {
        let local_path = target_dir.join(&filename);
        self.create_task_internal(fs_id, remote_path, local_path, total_size, conflict_strategy, None)
            .await
    }

    /// 🔥 创建下载任务（指定下载目录 + 显式 owner_uid）
    pub async fn create_task_with_dir_and_owner(
        &self,
        fs_id: u64,
        remote_path: String,
        filename: String,
        total_size: u64,
        target_dir: &std::path::Path,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        let local_path = target_dir.join(&filename);
        self.create_task_internal(
            fs_id,
            remote_path,
            local_path,
            total_size,
            conflict_strategy,
            Some(owner_uid),
        )
            .await
    }

    /// 内部方法：创建下载任务
    ///
    /// `owner_uid_override`：`Some(uid)` 用 uid，
    /// `None` 沿用 `self.owner_uid`（per-uid manager 架构下即该 manager 自己的 uid；
    /// 历史共享 Arc / 测试路径下可能为 startup active）。
    async fn create_task_internal(
        &self,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid_override: Option<crate::auth::Uid>,
    ) -> Result<String> {
        // effective_uid 在 task 创建前就确定：
        // task / 持久化 / Created event 全部用 effective_uid，根除事后 override 竞态。
        let effective_uid = owner_uid_override.unwrap_or(self.owner_uid);
        // 获取默认策略（如果未指定）
        let strategy = conflict_strategy.unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);

        // 解决冲突
        use crate::uploader::conflict_resolver::ConflictResolver;
        let resolution = ConflictResolver::resolve_download_conflict(&local_path, strategy)?;

        // 根据解决方案处理
        let final_local_path = match resolution {
            crate::uploader::conflict::ConflictResolution::Proceed => local_path,
            crate::uploader::conflict::ConflictResolution::Skip => {
                // 发送跳过事件
                let filename = local_path
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                info!("跳过下载（文件已存在）: {:?}", local_path);

                // 用 effective_uid 而非 self.owner_uid
                self.publish_event(DownloadEvent::Skipped {
                    task_id: format!("skipped-{}", uuid::Uuid::new_v4()),
                    filename,
                    reason: "文件已存在".to_string(),

                    owner_uid: Some(effective_uid.raw()),
                })
                    .await;

                return Ok("skipped".to_string());
            }
            crate::uploader::conflict::ConflictResolution::UseNewPath(new_path) => {
                info!("自动重命名下载路径: {:?} -> {}", local_path, new_path);
                PathBuf::from(new_path)
            }
        };

        // 确保目标目录存在
        if let Some(parent) = final_local_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).context("创建下载目录失败")?;
            }
        }

        let filename = final_local_path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // 🔥 查询映射表获取原始文件名（用于加密文件显示）
        let original_filename = self.query_original_filename(&filename).await;

        let mut task = DownloadTask::new(fs_id, remote_path.clone(), final_local_path.clone(), total_size, effective_uid);

        // 🔥 设置原始文件名和加密标记
        if let Some(ref orig_name) = original_filename {
            task.original_filename = Some(orig_name.clone());
            task.is_encrypted = true;
        }

        let task_id = task.id.clone();
        let group_id = task.group_id.clone();

        info!("创建下载任务: id={}, 文件名={}, 原始文件名={:?}", task_id, filename, original_filename);

        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc);

        // 🔥 活跃计数 +1（新建任务为 Pending）
        self.inc_active();

        // 🔥 创建即落盘占位元数据：让任务在 register（准备阶段）之前失败时也有持久化记录，
        // 重启后可恢复为终态「失败」，而不是凭空消失（与上传任务创建即落盘对齐）。
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.persist_download_task_on_create(
                task_id.clone(),
                fs_id,
                remote_path.clone(),
                final_local_path.clone(),
                total_size,
                None,
                None,
                None,
                false,
                None,
                None,
                Some(effective_uid.raw()),
            ) {
                warn!("写入下载任务创建占位元数据失败: {}", e);
            }
        }

        // 🔥 发送任务创建事件
        //
        // 携带 `effective_uid`（请求传入 → 使用之，否则沿用 self.owner_uid）。
        // 这彻底解决了"Created 事件 owner 错位"问题：handler 不再
        // 需要事后 `override_task_owner_uid`，task / 持久化 / Created event 三者
        // 在创建瞬间就一致。
        self.publish_event(DownloadEvent::Created {
            task_id: task_id.clone(),
            fs_id,
            remote_path,
            local_path: final_local_path.to_string_lossy().to_string(),
            total_size,
            group_id,
            is_backup: false,
            original_filename,

            owner_uid: Some(effective_uid.raw()),
        })
            .await;

        Ok(task_id)
    }

    /// 🔥 查询映射表获取原始文件名
    async fn query_original_filename(&self, encrypted_filename: &str) -> Option<String> {
        // 检查是否为加密文件名格式
        if !DownloadTask::detect_encrypted_filename(encrypted_filename) {
            return None;
        }

        // 查询映射表
        let snapshot_manager = self.snapshot_manager.read().await;
        if let Some(ref mgr) = *snapshot_manager {
            match mgr.find_by_encrypted_name(encrypted_filename) {
                Ok(Some(info)) => {
                    debug!("找到加密文件映射: {} -> {}", encrypted_filename, info.original_name);
                    return Some(info.original_name);
                }
                Ok(None) => {
                    debug!("未找到加密文件映射: {}", encrypted_filename);
                }
                Err(e) => {
                    warn!("查询加密文件映射失败: {}", e);
                }
            }
        }
        None
    }

    /// 开始下载任务
    ///
    /// 🔥 集成任务位分配机制：
    /// 1. 先尝试分配固定任务位
    /// 2. 如果没有任务位，加入等待队列
    /// 3. 获得任务位后，启动任务
    pub async fn start_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        // 检查任务状态
        let is_folder_task = {
            let t = task.lock().await;
            if t.status == TaskStatus::Downloading {
                anyhow::bail!("任务已在下载中");
            }
            if t.status == TaskStatus::Completed {
                anyhow::bail!("任务已完成");
            }
            // 检查是否为文件夹子任务（有 group_id 表示属于文件夹）
            t.group_id.is_some()
        };

        info!("请求启动下载任务: {} (文件夹子任务: {})", task_id, is_folder_task);

        // 🔥 关键修复：文件夹子任务必须"已持有某种槽位"才能启动
        // 两种合法持有形态（任一满足即可）：
        //   1. slot_id=Some              → 普通固定槽位 / 借调槽位
        //   2. uses_folder_fixed_slot=true → 文件夹固定槽位（task_slot_pool 不可见，已由 folder_manager 分配）
        if is_folder_task {
            let (has_slot, uses_folder_fixed_slot, start_folder_group_id) = {
                let t = task.lock().await;
                (
                    t.slot_id.is_some(),
                    t.uses_folder_fixed_slot,
                    t.group_id.clone(),
                )
            };
            let has_any_slot = has_slot || uses_folder_fixed_slot;

            if !has_any_slot {
                // 🔥 文件夹子任务没有任何槽位，不能启动，加入等待队列
                // 使用优先级方法：文件夹子任务优先级介于普通任务和备份任务之间
                warn!(
                    "文件夹子任务 {} 没有槽位，无法启动，加入等待队列",
                    task_id
                );
                self.add_to_waiting_queue_with_task_type(task_id, false, true).await;
                return Ok(());
            }

            // 🔥 已持有文件夹槽位的子任务：入口处立即按 group_id touch 一次
            //    覆盖两种合法持有形态：
            //      a. uses_folder_fixed_slot=true → 文件夹固定槽位（pool owner=group_id）
            //      b. slot_id=Some && is_borrowed_slot=true → 文件夹借调槽位（pool owner=group_id）
            //    （slot_id=Some && !is_borrowed 普通全局固定位 owner=task_id 不会出现在这条文件夹子任务路径下）
            //    防止本次 start 流程（prepare_for_scheduling / register_task）耗时过长时，
            //    槽位超时监控把 group 的槽位当成"无进展"回收。
            if let Some(ref folder_id) = start_folder_group_id {
                self.task_slot_pool.touch_slot(folder_id).await;
            }

            info!(
                "文件夹子任务 {} 有槽位，继续启动 (has_slot={}, uses_folder_fixed_slot={})",
                task_id, has_slot, uses_folder_fixed_slot
            );
        }

        // 🔥 尝试分配固定任务位（文件夹子任务由 FolderManager 管理槽位，这里跳过）
        if !is_folder_task {
            // 获取任务是否为备份任务（此分支前置条件 !is_folder_task 已保证 group_id=None）
            let is_backup = {
                let t = task.lock().await;
                t.is_backup
            };
            // 🔥 此分支下的 backup slot / 普通全局 fixed slot 在 pool 中 owner 都是 task_id，
            //    且 group_id=None，所以 touch 直接用 task_id。

            // 🔥 根据任务类型选择不同的槽位分配策略
            if is_backup {
                // 备份任务：只能使用空闲槽位，不能抢占
                let slot_id = self.task_slot_pool.allocate_backup_slot(task_id).await;

                if let Some(slot_id) = slot_id {
                    // 分配成功，记录槽位信息
                    {
                        let mut t = task.lock().await;
                        t.slot_id = Some(slot_id);
                        t.is_borrowed_slot = false;
                    }
                    // 🔥 backup slot owner=task_id，按 task_id touch
                    self.task_slot_pool.touch_slot(task_id).await;
                    info!("备份任务 {} 获得任务位: slot_id={}，已刷新槽位时间戳", task_id, slot_id);
                } else {
                    // 🔥 备份任务无可用槽位，加入等待队列末尾（最低优先级）
                    self.add_to_waiting_queue_by_priority(task_id, true).await;
                    info!(
                        "备份任务 {} 无可用任务位，加入等待队列末尾 (已用槽位: {}/{})",
                        task_id,
                        self.task_slot_pool.used_slots().await,
                        self.max_concurrent_tasks.load(Ordering::SeqCst)
                    );
                    return Ok(());
                }
            } else {
                // 普通任务：使用带优先级的分配方法，可以抢占备份任务
                let result = self.task_slot_pool.allocate_fixed_slot_with_priority(
                    task_id, false, TaskPriority::Normal
                ).await;

                match result {
                    Some((slot_id, preempted_task_id)) => {
                        // 分配成功，记录槽位信息
                        {
                            let mut t = task.lock().await;
                            t.slot_id = Some(slot_id);
                            t.is_borrowed_slot = false;
                        }

                        // 🔥 普通全局 fixed slot owner=task_id，按 task_id touch
                        self.task_slot_pool.touch_slot(task_id).await;

                        // 🔥 如果有被抢占的备份任务，需要暂停它并加入等待队列末尾
                        if let Some(preempted_id) = preempted_task_id {
                            info!("普通任务 {} 抢占了备份任务 {} 的槽位: slot_id={}，已刷新槽位时间戳", task_id, preempted_id, slot_id);
                            // 暂停被抢占的备份任务（skip_try_start_waiting=true，避免循环）
                            if let Err(e) = self.pause_task(&preempted_id, true).await {
                                warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
                            }
                            // 🔥 将被暂停的备份任务加入等待队列末尾（包含状态转换和通知）
                            self.add_preempted_backup_to_queue(&preempted_id).await;
                        } else {
                            info!("普通任务 {} 获得固定任务位: slot_id={}，已刷新槽位时间戳", task_id, slot_id);
                        }
                    }
                    None => {
                        // 🔥 无可用任务位，先尝试回收文件夹的借调槽位
                        let folder_manager = {
                            let fm = self.folder_manager.read().await;
                            fm.clone()
                        };

                        if let Some(fm) = folder_manager {
                            // 检查是否有借调槽位可回收
                            if self.task_slot_pool.find_folder_with_borrowed_slots().await.is_some() {
                                info!("普通任务 {} 无可用槽位，尝试回收文件夹借调槽位", task_id);

                                // 尝试回收一个借调槽位（按当前 manager 的 owner_uid 过滤）
                                if let Some(reclaimed_slot_id) = fm.reclaim_borrowed_slot_for_owner(self.owner_uid()).await {
                                    // 回收成功，分配槽位给新任务
                                    if let Some((slot_id, preempted_task_id)) = self.task_slot_pool.allocate_fixed_slot_with_priority(
                                        task_id, false, TaskPriority::Normal
                                    ).await {
                                        {
                                            let mut t = task.lock().await;
                                            t.slot_id = Some(slot_id);
                                            t.is_borrowed_slot = false;
                                        }
                                        // 🔥 普通全局 fixed slot owner=task_id，按 task_id touch
                                        self.task_slot_pool.touch_slot(task_id).await;
                                        // 🔥 处理被抢占的备份任务
                                        if let Some(preempted_id) = preempted_task_id {
                                            info!("普通任务 {} 通过回收借调槽位获得任务位并抢占了备份任务 {}: slot_id={} (回收的槽位={})，已刷新槽位时间戳", task_id, preempted_id, slot_id, reclaimed_slot_id);
                                            self.pause_preempted_task(&preempted_id).await;
                                            // 🔥 将被暂停的备份任务加入等待队列末尾（包含状态转换和通知）
                                            self.add_preempted_backup_to_queue(&preempted_id).await;
                                        } else {
                                            info!("普通任务 {} 通过回收借调槽位获得任务位: slot_id={} (回收的槽位={})，已刷新槽位时间戳", task_id, slot_id, reclaimed_slot_id);
                                        }
                                    } else {
                                        warn!("回收借调槽位成功但重新分配失败，普通任务 {} 加入等待队列", task_id);
                                        self.add_to_waiting_queue_by_priority(task_id, false).await;
                                        return Ok(());
                                    }
                                } else {
                                    // 回收失败，加入等待队列
                                    info!("回收借调槽位失败，普通任务 {} 加入等待队列", task_id);
                                    self.add_to_waiting_queue_by_priority(task_id, false).await;
                                    info!(
                                        "普通任务 {} 无可用任务位，加入等待队列 (已用槽位: {}/{})",
                                        task_id,
                                        self.task_slot_pool.used_slots().await,
                                        self.max_concurrent_tasks.load(Ordering::SeqCst)
                                    );
                                    return Ok(());
                                }
                            } else {
                                // 没有借调槽位可回收，直接加入等待队列
                                self.add_to_waiting_queue_by_priority(task_id, false).await;
                                info!(
                                    "普通任务 {} 无可用任务位且无借调槽位可回收，加入等待队列 (已用槽位: {}/{})",
                                    task_id,
                                    self.task_slot_pool.used_slots().await,
                                    self.max_concurrent_tasks.load(Ordering::SeqCst)
                                );
                                return Ok(());
                            }
                        } else {
                            // 无文件夹管理器，直接加入等待队列
                            self.add_to_waiting_queue_by_priority(task_id, false).await;
                            info!(
                                "普通任务 {} 无可用任务位，加入等待队列 (已用槽位: {}/{})",
                                task_id,
                                self.task_slot_pool.used_slots().await,
                                self.max_concurrent_tasks.load(Ordering::SeqCst)
                            );
                            return Ok(());
                        }
                    }
                }
            }
        }

        // 立即启动任务
        self.start_task_internal(task_id).await
    }

    /// 🔥 静态版本的槽位释放（供 spawn 出去的失败处理路径使用）
    ///
    /// 实现等价于 [`Self::release_task_slot_by_kind`]，但避免直接持有 `&self`，
    /// 方便在 `tokio::spawn` 后的纯静态闭包中调用。
    async fn release_task_slot_by_kind_static(
        task_id: &str,
        group_id: Option<&str>,
        slot_id: Option<usize>,
        is_borrowed_slot: bool,
        uses_folder_fixed_slot: bool,
        task_slot_pool: &Arc<crate::task_slot_pool::TaskSlotPool>,
        folder_manager: &Arc<RwLock<Option<Arc<crate::downloader::FolderDownloadManager>>>>,
    ) {
        if uses_folder_fixed_slot {
            if let Some(folder_id) = group_id {
                let folder_mgr = folder_manager.read().await.clone();
                if let Some(fm) = folder_mgr {
                    fm.release_fixed_slot_from_subtask(folder_id, task_id).await;
                    debug!(
                        "(static) 释放任务 {} 的文件夹 {} 固定槽位",
                        task_id, folder_id
                    );
                }
            }
        } else if is_borrowed_slot {
            if let Some(folder_id) = group_id {
                let folder_mgr = folder_manager.read().await.clone();
                if let Some(fm) = folder_mgr {
                    let released = fm
                        .release_subtask_borrowed_slot(folder_id, task_id)
                        .await;
                    debug!(
                        "(static) 任务 {} 借调位映射已清除 (slot_id={:?})",
                        task_id, released
                    );
                }
            }
        } else if let Some(sid) = slot_id {
            task_slot_pool.release_fixed_slot(task_id).await;
            debug!("(static) 释放任务 {} 的普通固定槽位 {}", task_id, sid);
        }
    }

    /// 处理任务准备或注册失败的统一逻辑
    ///
    /// - 文件夹子任务 / 备份任务：累加 `start_retry_count`，未超上限则重置为 Pending 放回等待队列；
    ///   达到 `MAX_START_RETRIES` 则转为标记 Failed，统一与 0 延迟路径一致。
    /// - 普通单文件任务：直接标记失败并发送失败事件。
    ///
    /// 🔥 typed rollback：失败前先按持有方式释放任务槽位（文件夹固定/借调/普通），
    /// 防止 prepare/register 失败时槽位状态泄漏导致后续任务无法分配槽位。
    #[allow(clippy::too_many_arguments)]
    async fn handle_task_failure(
        task_id: String,
        task: Arc<Mutex<DownloadTask>>,
        error_msg: String,
        waiting_queue: Arc<RwLock<VecDeque<String>>>,
        cancellation_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
        ws_manager: Option<Arc<WebSocketManager>>,
        persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,
        tasks: Arc<RwLock<HashMap<String, Arc<Mutex<DownloadTask>>>>>,
        task_slot_pool: Arc<crate::task_slot_pool::TaskSlotPool>,
        folder_manager: Arc<RwLock<Option<Arc<crate::downloader::FolderDownloadManager>>>>,
        // 🔥 备份任务终态失败需要走 BackupTransferNotification::Failed，
        //    避免和 publish_event() 的"备份任务跳过普通下载事件"约定冲突。
        backup_notification_tx: Option<tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>>,
    ) {
        // 🔥 一次性读取需要的字段，避免持锁过久
        let (group_id, is_backup, slot_id, is_borrowed_slot, uses_folder_fixed_slot, start_retry_count) = {
            let t = task.lock().await;
            (
                t.group_id.clone(),
                t.is_backup,
                t.slot_id,
                t.is_borrowed_slot,
                t.uses_folder_fixed_slot,
                t.start_retry_count,
            )
        };

        // 🔥 typed rollback：先释放槽位，再处理状态变更
        Self::release_task_slot_by_kind_static(
            &task_id,
            group_id.as_deref(),
            slot_id,
            is_borrowed_slot,
            uses_folder_fixed_slot,
            &task_slot_pool,
            &folder_manager,
        )
            .await;

        let is_folder_subtask = group_id.is_some();

        // 🔥 文件夹子任务或备份任务：有重试机会，走 start_retry_count 循环
        //    与 0 延迟路径保持一致，避免出现"prepare/register 失败无限重入队"
        if (is_folder_subtask || is_backup) && start_retry_count < MAX_START_RETRIES {
            warn!(
                "任务 {} 启动失败（{}），放回等待队列等待重试 (重试 {}/{}, is_backup={}, is_folder_subtask={})",
                task_id,
                error_msg,
                start_retry_count + 1,
                MAX_START_RETRIES,
                is_backup,
                is_folder_subtask
            );

            // 将任务状态重置为 Pending，清空槽位字段，累加 start_retry_count
            {
                let mut t = task.lock().await;
                t.status = TaskStatus::Pending;
                t.error = Some(error_msg);
                t.slot_id = None;
                t.is_borrowed_slot = false;
                t.uses_folder_fixed_slot = false;
                t.start_retry_count += 1;
            }

            // 🔥 使用优先级方法重新放回等待队列
            Self::add_to_queue_by_priority(
                &waiting_queue,
                &tasks,
                &task_id,
                is_backup,
                is_folder_subtask,
            )
                .await;

            // 移除取消令牌，避免泄漏
            cancellation_tokens.write().await.remove(&task_id);
            return;
        }

        // 🔥 进入此分支的三种情形：
        //    1) 普通单文件任务（is_folder_subtask=false 且 is_backup=false） → 直接失败
        //    2) 文件夹子任务 / 备份任务但 start_retry_count >= MAX_START_RETRIES → 达上限失败
        if (is_folder_subtask || is_backup) && start_retry_count >= MAX_START_RETRIES {
            error!(
                "任务 {} 启动重试次数已达上限 ({})，标记为失败",
                task_id, MAX_START_RETRIES
            );
        }

        // 取出 task.owner_uid 用于 Failed 事件。
        // handle_task_failure 是无 self 的 standalone async fn，无法访问 manager.owner_uid。
        let owner_uid_raw_for_failed: Option<u64> = {
            let mut t = task.lock().await;
            t.mark_failed(error_msg.clone());
            t.slot_id = None;
            t.is_borrowed_slot = false;
            t.uses_folder_fixed_slot = false;
            Some(t.owner_uid.raw())
        };

        // 发布任务失败事件：备份任务走 BackupTransferNotification::Failed
        // （publish_event / send_if_subscribed 路径对备份任务统一交给 AutoBackupManager 消费），
        // 普通下载任务保持原有的 WebSocket 路径。
        if is_backup {
            if let Some(ref tx) = backup_notification_tx {
                use crate::autobackup::events::TransferTaskType;
                let notification = BackupTransferNotification::Failed {
                    task_id: task_id.clone(),
                    task_type: TransferTaskType::Download,
                    error_message: error_msg.clone(),
                };
                if let Err(e) = tx.send(notification) {
                    warn!("发送备份任务失败通知失败 (task_id={}): {}", task_id, e);
                }
            } else {
                warn!(
                    "备份任务 {} 终态失败但 backup_notification_tx 未设置，AutoBackupManager 可能感知不到",
                    task_id
                );
            }
        } else if let Some(ref ws) = ws_manager {
            ws.send_if_subscribed(
                TaskEvent::Download(DownloadEvent::Failed {
                    task_id: task_id.clone(),
                    error: error_msg.clone(),
                    group_id: group_id.clone(),
                    is_backup,

                    owner_uid: owner_uid_raw_for_failed,
                }),
                group_id.clone(),
            );
        }

        // 更新持久化错误信息
        if let Some(ref pm) = persistence_manager {
            if let Err(e) = pm.lock().await.update_task_error(&task_id, error_msg) {
                warn!("更新下载任务错误信息失败: {}", e);
            }
        }

        // 移除取消令牌
        cancellation_tokens.write().await.remove(&task_id);
    }

    /// 🔥 按持有方式释放任务槽位（统一入口，供 auto_requeue / 失败回滚等场景调用）
    ///
    /// 三种持有方式按互斥优先级处理（每个任务最多命中其中一种）：
    /// 1. `uses_folder_fixed_slot=true` → 文件夹固定槽位
    ///    - 通过 `folder_manager.release_fixed_slot_from_subtask` 仅清除文件夹端映射，
    ///    - 借出时本身就没占用 task_slot_pool，无需释放回 pool。
    /// 2. `is_borrowed_slot=true`       → 文件夹借调槽位
    ///    - 通过 `folder_manager.release_subtask_borrowed_slot` 清除 borrowed_subtask_map，
    ///    - 借调位归属仍保留在文件夹的 `borrowed_slot_ids` 中，可被同组其他子任务复用。
    /// 3. `is_borrowed_slot=false && slot_id=Some` → 普通固定槽位
    ///    - 通过 `task_slot_pool.release_fixed_slot` 释放回任务槽位池。
    ///
    /// 注意：此函数仅释放 folder_manager / task_slot_pool 端的槽位状态，
    /// 调用方需自行重置 `task` 的 `slot_id` / `is_borrowed_slot` / `uses_folder_fixed_slot` 字段。
    pub(crate) async fn release_task_slot_by_kind(
        &self,
        task_id: &str,
        group_id: Option<&str>,
        slot_id: Option<usize>,
        is_borrowed_slot: bool,
        uses_folder_fixed_slot: bool,
    ) {
        if uses_folder_fixed_slot {
            // a. 文件夹固定槽位
            if let Some(folder_id) = group_id {
                let folder_mgr = self.folder_manager.read().await.clone();
                if let Some(fm) = folder_mgr {
                    fm.release_fixed_slot_from_subtask(folder_id, task_id).await;
                    debug!(
                        "release_task_slot_by_kind: 释放任务 {} 的文件夹 {} 固定槽位",
                        task_id, folder_id
                    );
                } else {
                    warn!(
                        "release_task_slot_by_kind: 任务 {} 标记 uses_folder_fixed_slot 但 folder_manager 缺失",
                        task_id
                    );
                }
            } else {
                warn!(
                    "release_task_slot_by_kind: 任务 {} uses_folder_fixed_slot=true 但 group_id 缺失",
                    task_id
                );
            }
        } else if is_borrowed_slot {
            // b. 文件夹借调槽位
            if let Some(folder_id) = group_id {
                let folder_mgr = self.folder_manager.read().await.clone();
                if let Some(fm) = folder_mgr {
                    let released = fm
                        .release_subtask_borrowed_slot(folder_id, task_id)
                        .await;
                    debug!(
                        "release_task_slot_by_kind: 任务 {} 借调位映射已清除 (slot_id={:?})",
                        task_id, released
                    );
                } else {
                    warn!(
                        "release_task_slot_by_kind: 任务 {} is_borrowed_slot=true 但 folder_manager 缺失",
                        task_id
                    );
                }
            } else {
                warn!(
                    "release_task_slot_by_kind: 任务 {} is_borrowed_slot=true 但 group_id 缺失",
                    task_id
                );
            }
        } else if let Some(sid) = slot_id {
            // c. 普通固定槽位
            self.task_slot_pool.release_fixed_slot(task_id).await;
            debug!(
                "release_task_slot_by_kind: 释放任务 {} 的普通固定槽位 {}",
                task_id, sid
            );
        }
    }

    /// 🔥 自动退回任务到等待队列（无可用分片 / Fatal 触发）
    ///
    /// 由 scheduler 通过 `requeue_tx` channel 异步调用。
    /// 流程：
    /// 1. 校验任务存在且仍处于活跃态
    /// 2. 清空下载进度、解冻分片、释放槽位
    /// 3. 置 status = Pending、记录 next_retry_at
    /// 4. 加回等待队列（保留文件夹优先级）
    /// 5. 推送 StatusChanged 事件（error 字段携带退回原因）
    pub async fn auto_requeue_task(&self, task_id: &str, reason: String) -> Result<()> {
        // 1. 获取任务 & 校验
        let task = match self.tasks.read().await.get(task_id).cloned() {
            Some(t) => t,
            None => {
                debug!("auto_requeue_task: 任务 {} 不存在（可能已被删除），跳过", task_id);
                return Ok(());
            }
        };

        // 🔥 收尾/解密窗口保护：任务已下完（downloaded==total）、已 spawn 收尾协程、
        //   或已进入 Decrypting 时，绝不退回重排——否则会把一个其实已完成的任务
        //   重新下载/重新调度，触发"双 finalize 抢同一临时文件"（错误一）以及新旧
        //   写者并发写同一文件导致"大小对但内容坏"（错误二）。
        {
            let t = task.lock().await;
            if t.status == TaskStatus::Decrypting
                || t.finalize_spawned
                || (t.total_size > 0 && t.downloaded_size >= t.total_size)
            {
                debug!(
                    "auto_requeue_task: 任务 {} 处于收尾/解密窗口（status={:?}, finalize_spawned={}, downloaded={}/{}），跳过退回重排",
                    task_id, t.status, t.finalize_spawned, t.downloaded_size, t.total_size
                );
                return Ok(());
            }
        }

        // 同时取出真实 owner_uid
        let (old_status, group_id, is_backup, slot_id, is_borrowed_slot, uses_folder_fixed_slot, task_owner_uid_raw) = {
            let t = task.lock().await;
            (
                match t.status {
                    TaskStatus::Pending => "pending".to_string(),
                    TaskStatus::Downloading => "downloading".to_string(),
                    TaskStatus::Paused => "paused".to_string(),
                    TaskStatus::Completed => {
                        debug!("auto_requeue_task: 任务 {} 已完成，跳过", task_id);
                        return Ok(());
                    }
                    TaskStatus::Failed => "failed".to_string(),
                    TaskStatus::Decrypting => "decrypting".to_string(),
                },
                t.group_id.clone(),
                t.is_backup,
                t.slot_id,
                t.is_borrowed_slot,
                t.uses_folder_fixed_slot,
                t.owner_uid.raw(),
            )
        };

        info!(
            "自动退回任务 {} 到等待队列（原因: {}，来自状态: {}）",
            task_id, reason, old_status
        );

        // 2. 触发取消令牌，终止当前分片
        if let Some(token) = self.cancellation_tokens.read().await.get(task_id) {
            token.cancel();
        }

        // 3. 释放槽位（按 3 种持有方式分别处理）：
        //    a. uses_folder_fixed_slot=true → 文件夹固定槽位（仅清除文件夹映射，不释放到 pool）
        //    b. is_borrowed_slot=true       → 文件夹借调槽位（清除子任务映射，借调位仍归文件夹）
        //    c. is_borrowed_slot=false 且 slot_id=Some → 普通固定槽位（释放回 task_slot_pool）
        self.release_task_slot_by_kind(
            task_id,
            group_id.as_deref(),
            slot_id,
            is_borrowed_slot,
            uses_folder_fixed_slot,
        )
            .await;

        // 4. 清除进度、重置状态、写入冷却时间戳
        let now_ms = chrono::Utc::now().timestamp_millis();
        {
            let mut t = task.lock().await;
            t.status = TaskStatus::Pending;
            // 清零已下载大小（分片内断点续传信息保留在持久化 chunk_progress，下次调度会读取）
            t.downloaded_size = 0;
            t.speed = 0;
            t.error = Some(reason.clone());
            t.next_retry_at = Some(now_ms + REQUEUE_COOLDOWN_SECS * 1000);
            t.slot_id = None;
            t.is_borrowed_slot = false;
            t.uses_folder_fixed_slot = false;
        }

        // 5. 清理取消令牌（避免后续 start_task_internal 读到旧 token）
        self.cancellation_tokens.write().await.remove(task_id);

        // 6. 持久化：写入错误原因但保持状态为 Pending（与内存语义一致，避免误标 Failed）
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.update_task_error_with_status(
                task_id,
                reason.clone(),
                crate::persistence::types::TaskPersistenceStatus::Pending,
            ) {
                warn!("auto_requeue: 更新持久化 error+pending 失败: {}", e);
            }
        }

        // 7. 放回等待队列（复用 add_to_waiting_queue_for_retry，含优先级）
        let is_folder_subtask = group_id.is_some();
        self.add_to_waiting_queue_for_retry(task_id, is_backup, is_folder_subtask).await;

        // 8. 活跃计数 -1（退回等待队列不算活跃）
        self.dec_active();

        // 9. 发送 StatusChanged 事件（携带退回原因）
        // 🔥 备份任务走 BackupTransferNotification::StatusChanged（publish_event 会跳过备份任务），
        //    否则 AutoBackupManager 看不到下载侧已经把状态改回 Pending，备份状态机会卡在旧状态。
        if is_backup {
            use crate::autobackup::events::{TransferTaskStatus, TransferTaskType};
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                // old_status 字符串 → TransferTaskStatus（与同文件其它处映射保持一致：
                // pending → Pending, downloading → Transferring, 其它 → Paused）
                let old_transfer_status = match old_status.as_str() {
                    "pending" => TransferTaskStatus::Pending,
                    "downloading" => TransferTaskStatus::Transferring,
                    _ => TransferTaskStatus::Paused,
                };
                let notification = BackupTransferNotification::StatusChanged {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                    old_status: old_transfer_status,
                    new_status: TransferTaskStatus::Pending,
                };
                if let Err(e) = tx.send(notification) {
                    warn!(
                        "auto_requeue: 发送备份任务 StatusChanged 通知失败 (task_id={}): {}",
                        task_id, e
                    );
                }
            } else {
                warn!(
                    "auto_requeue: 备份任务 {} 退回等待队列但 backup_notification_tx 未设置",
                    task_id
                );
            }
            // 备份任务的 reason / error 由 AutoBackupManager 单独维护，无需再带上
            let _ = reason;
        } else {
            self.publish_event(DownloadEvent::StatusChanged {
                task_id: task_id.to_string(),
                old_status,
                new_status: "pending".to_string(),
                group_id: group_id.clone(),
                is_backup,
                error: Some(reason),

                owner_uid: Some(task_owner_uid_raw),
            })
                .await;
        }

        Ok(())
    }

    /// 🔥 退回队列专用（不发 StatusChanged，auto_requeue_task 自行推送）
    async fn add_to_waiting_queue_for_retry(
        &self,
        task_id: &str,
        is_backup: bool,
        is_folder_subtask: bool,
    ) {
        Self::add_to_queue_by_priority(
            &self.waiting_queue,
            &self.tasks,
            task_id,
            is_backup,
            is_folder_subtask,
        )
            .await;
    }

    /// 🔥 检查任务是否处于退回冷却中
    ///
    /// 等待队列消费点调用此方法，冷却中的任务放回队尾。
    pub async fn is_task_cooling(&self, task_id: &str) -> bool {
        let tasks = self.tasks.read().await;
        let task = match tasks.get(task_id) {
            Some(t) => t.clone(),
            None => return false,
        };
        drop(tasks);

        let t = task.lock().await;
        match t.next_retry_at {
            Some(until_ms) => chrono::Utc::now().timestamp_millis() < until_ms,
            None => false,
        }
    }

    /// 🔥 获取 requeue channel 发送端（给 scheduler 用）
    pub fn requeue_sender(&self) -> mpsc::UnboundedSender<AutoRequeueRequest> {
        self.requeue_tx.clone()
    }

    /// 🔥 启动 auto_requeue 消费任务
    ///
    /// 由 AppState 在管理器初始化完成后调用（因为 Arc<Self> 外部才能持有）。
    /// 消费 `requeue_rx`，顺序处理每个 AutoRequeueRequest。
    pub async fn start_auto_requeue_consumer(self: Arc<Self>) {
        let mut rx = match self.requeue_rx.lock().await.take() {
            Some(rx) => rx,
            None => {
                warn!("start_auto_requeue_consumer: 接收端已被占用，忽略重复启动");
                return;
            }
        };
        let manager = self.clone();
        tokio::spawn(async move {
            info!("auto_requeue 消费任务已启动");
            while let Some(req) = rx.recv().await {
                let AutoRequeueRequest { task_id, reason } = req;
                if let Err(e) = manager.auto_requeue_task(&task_id, reason).await {
                    error!("auto_requeue_task 执行失败: task_id={}, err={}", task_id, e);
                }
            }
            warn!("auto_requeue 消费任务已退出（channel 关闭）");
        });
    }

    /// 内部方法：真正启动一个任务
    ///
    /// 该方法会检查任务是否有槽位，有槽位才启动探测
    /// 任务探测完成后直接注册到调度器，不再需要预注册机制
    async fn start_task_internal(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        // 🔥 关键修复：检查任务是否有"可用的槽位"
        // 三种合法的"已持有槽位"情形（任一满足即可启动）：
        //   1. slot_id=Some           → 普通固定槽位 / 借调槽位
        //   2. uses_folder_fixed_slot → 文件夹固定槽位（task_slot_pool 不可见，但已由 folder_manager 分配）
        // 仅当三者都不成立时才视为"无槽位"。
        let (has_slot, is_folder_task, uses_folder_fixed_slot) = {
            let t = task.lock().await;
            (
                t.slot_id.is_some(),
                t.group_id.is_some(),
                t.uses_folder_fixed_slot,
            )
        };
        let has_any_slot = has_slot || uses_folder_fixed_slot;

        // 🔥 文件夹子任务必须已持有某种槽位才能启动
        if is_folder_task && !has_any_slot {
            warn!(
                "文件夹子任务 {} 没有槽位，无法启动，加入等待队列",
                task_id
            );
            // 🔥 使用优先级方法：文件夹子任务优先级介于普通任务和备份任务之间
            self.add_to_waiting_queue_with_task_type(task_id, false, true).await;
            return Ok(());
        }

        info!(
            "启动下载任务: {} (has_slot={}, uses_folder_fixed_slot={})",
            task_id, has_slot, uses_folder_fixed_slot
        );

        // 创建取消令牌
        let cancellation_token = CancellationToken::new();
        self.cancellation_tokens
            .write()
            .await
            .insert(task_id.to_string(), cancellation_token.clone());

        // 准备任务（获取下载链接、创建分片管理器等）
        let engine = self.engine.clone();
        let task_clone = task.clone();
        let chunk_scheduler = self.chunk_scheduler.clone();
        let task_id_clone = task_id.to_string();
        let cancellation_tokens = self.cancellation_tokens.clone();
        let persistence_manager = self.persistence_manager.clone();
        let ws_manager_arc = self.ws_manager.clone();
        let folder_progress_tx_arc = self.folder_progress_tx.clone();
        let backup_notification_tx_arc = self.backup_notification_tx.clone();
        let waiting_queue = self.waiting_queue.clone();
        let task_slot_pool_clone = self.task_slot_pool.clone();
        let tasks_clone = self.tasks.clone(); // 🔥 用于 handle_task_failure 的优先级队列插入
        let snapshot_manager_arc = self.snapshot_manager.clone(); // 🔥 用于查询加密文件映射
        let encryption_config_store_arc = self.encryption_config_store.clone(); // 🔥 用于根据 key_version 选择解密密钥
        let max_retries = self.max_retries;
        // 🔥 auto_requeue 发送端（scheduler 回调时用）
        let requeue_tx_clone = self.requeue_tx.clone();
        // 🔥 文件夹管理器引用（scheduler 中分片下载需要）
        let folder_manager_arc_clone = self.folder_manager.clone();
        // 不再 capture self.owner_uid。
        // 共享 manager 设计下 self.owner_uid 不可靠（同一个 Arc 服务多个账号）。
        // 事件归属一律读 `task.owner_uid`（已在下方 prepare 后从 task 读出 task_owner_uid）。

        tokio::spawn(async move {
            // 获取 WebSocket 管理器和文件夹进度发送器
            let ws_manager = ws_manager_arc.read().await.clone();
            let folder_progress_tx = folder_progress_tx_arc.read().await.clone();
            let backup_notification_tx = backup_notification_tx_arc.read().await.clone();
            let snapshot_manager = snapshot_manager_arc.read().await.clone(); // 🔥 获取快照管理器
            let encryption_config_store = encryption_config_store_arc.read().await.clone(); // 🔥 获取加密配置存储
            // 🔥 文件夹管理器（scheduler 调度时需要）
            let folder_manager_clone_for_schedule = folder_manager_arc_clone.read().await.clone();
            // 准备任务
            let prepare_result = engine
                .prepare_for_scheduling(task_clone.clone(), cancellation_token.clone())
                .await;

            // 探测完成后，先检查是否被取消
            if cancellation_token.is_cancelled() {
                info!("任务 {} 在探测完成后发现已被取消", task_id_clone);
                return;
            }

            match prepare_result {
                Ok((
                       client,
                       cookie,
                       referer,
                       url_health,
                       output_path,
                       chunk_size,
                       chunk_manager,
                       speed_calc,
                   )) => {
                    // 获取文件总大小、远程路径和 fs_id（用于探测恢复链接和速度异常检测）
                    // 🔥 同时读取 is_borrowed_slot / uses_folder_fixed_slot，用于下方 prepare 后 touch 的 owner 判定
                    let (
                        total_size,
                        remote_path,
                        fs_id,
                        local_path,
                        group_id,
                        group_root,
                        relative_path,
                        is_backup,
                        backup_config_id,
                        transfer_task_id,
                        is_borrowed_slot,
                        uses_folder_fixed_slot,
                        task_owner_uid,
                    ) = {
                        let t = task_clone.lock().await;
                        (
                            t.total_size,
                            t.remote_path.clone(),
                            t.fs_id,
                            t.local_path.clone(),
                            t.group_id.clone(),
                            t.group_root.clone(),
                            t.relative_path.clone(),
                            t.is_backup,
                            t.backup_config_id.clone(),
                            t.transfer_task_id.clone(),
                            t.is_borrowed_slot,
                            t.uses_folder_fixed_slot,
                            // 让 .meta 持久化跟随 task.owner_uid
                            t.owner_uid,
                        )
                    };

                    // 获取分片数
                    let total_chunks = {
                        let cm = chunk_manager.lock().await;
                        cm.chunk_count()
                    };

                    // 🔥 prepare_for_scheduling 完成后立即刷新槽位，防止探测阶段耗时过长导致超时
                    //   touch owner 必须与"任务实际持有的槽位类型"对应：
                    //   - 文件夹 fixed/borrowed 槽位 → pool owner = group_id
                    //   - 普通全局 fixed / backup 槽位 → pool owner = task_id
                    {
                        let prepare_touch_id = if (uses_folder_fixed_slot || is_borrowed_slot)
                            && group_id.is_some()
                        {
                            group_id.clone().unwrap()
                        } else {
                            task_id_clone.clone()
                        };
                        task_slot_pool_clone.touch_slot(&prepare_touch_id).await;
                    }

                    // 🔥 发送状态变更事件：pending → downloading
                    // 此时 prepare_for_scheduling 已完成，任务状态已变为 Downloading
                    if is_backup {
                        // 备份任务：发送到 backup_notification_tx
                        use crate::autobackup::events::TransferTaskType;
                        if let Some(ref tx) = backup_notification_tx {
                            let notification = BackupTransferNotification::StatusChanged {
                                task_id: task_id_clone.clone(),
                                task_type: TransferTaskType::Download,
                                old_status: crate::autobackup::events::TransferTaskStatus::Pending,
                                new_status: crate::autobackup::events::TransferTaskStatus::Transferring,
                            };
                            let _ = tx.send(notification);
                        }
                    } else if let Some(ref ws) = ws_manager {
                        // 普通任务：发送到 WebSocket
                        ws.send_if_subscribed(
                            TaskEvent::Download(DownloadEvent::StatusChanged {
                                task_id: task_id_clone.clone(),
                                old_status: "pending".to_string(),
                                new_status: "downloading".to_string(),
                                group_id: group_id.clone(),
                                is_backup,
                                error: None,

                                owner_uid: Some(task_owner_uid.raw()),
                            }),
                            group_id.clone(),
                        );
                    }

                    // 🔥 检测是否为加密文件，并获取 key_version
                    let (is_encrypted, encryption_key_version) = {
                        let filename = local_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("");

                        // 通过文件名检测是否为加密文件
                        let is_encrypted = DownloadTask::detect_encrypted_filename(filename);

                        // 如果是加密文件，尝试从 snapshot_manager 获取 key_version
                        let key_version = if is_encrypted {
                            if let Some(ref snapshot_mgr) = snapshot_manager {
                                match snapshot_mgr.find_by_encrypted_name(filename) {
                                    Ok(Some(snapshot_info)) => {
                                        debug!(
                                            "任务 {} 从映射表获取 key_version: {}",
                                            task_id_clone, snapshot_info.key_version
                                        );
                                        Some(snapshot_info.key_version)
                                    }
                                    Ok(None) => {
                                        debug!("任务 {} 在映射表中未找到加密信息", task_id_clone);
                                        None
                                    }
                                    Err(e) => {
                                        warn!("任务 {} 查询映射表失败: {}", task_id_clone, e);
                                        None
                                    }
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        (if is_encrypted { Some(true) } else { None }, key_version)
                    };

                    // 🔥 注册任务到持久化管理器
                    // 显式传 task.owner_uid，避免 PersistenceManager
                    // 启动账号与 task 实际归属不一致（per-uid manager 架构下 PersistenceManager
                    // 是 manager 自己 uid 的 sqlite，但跨 manager 历史路径仍可能出现错配）。
                    if let Some(ref pm) = persistence_manager {
                        if let Err(e) = pm.lock().await.register_download_task(
                            task_id_clone.clone(),
                            fs_id,
                            remote_path.clone(),
                            local_path.clone(),
                            total_size,
                            chunk_size,
                            total_chunks,
                            group_id.clone(),
                            group_root.clone(),
                            relative_path.clone(),
                            is_backup,
                            backup_config_id.clone(),
                            is_encrypted,
                            encryption_key_version,
                            transfer_task_id.clone(),
                            Some(task_owner_uid.raw()),
                        ) {
                            warn!("注册任务到持久化管理器失败: {}", e);
                        } else {
                            info!(
                                "任务 {} 已注册到持久化管理器 ({} 个分片, is_backup={}, transfer_task_id={:?}, owner_uid={})",
                                task_id_clone, total_chunks, is_backup, transfer_task_id, task_owner_uid.raw()
                            );
                        }

                        // 🔥 修复：从持久化管理器获取已完成的分片，并标记到 ChunkManager（实现真正的断点续传）
                        if let Some(completed_chunks) = pm.lock().await.get_completed_chunks(&task_id_clone) {
                            let mut cm = chunk_manager.lock().await;
                            let mut completed_count = 0;
                            for chunk_index in completed_chunks.iter() {
                                cm.mark_completed(chunk_index);
                                completed_count += 1;
                            }
                            if completed_count > 0 {
                                info!(
                                    "任务 {} 恢复了 {} 个已完成分片，将跳过这些分片的下载",
                                    task_id_clone, completed_count
                                );
                            }
                        }
                        // 🔥 恢复分片内部分进度（分片内断点续传）
                        if let Some(partial_progress) = pm.lock().await.get_partial_progress(&task_id_clone) {
                            let mut cm = chunk_manager.lock().await;
                            let mut partial_count = 0;
                            for (chunk_index, bytes_downloaded) in &partial_progress {
                                cm.update_bytes_downloaded(*chunk_index, *bytes_downloaded);
                                partial_count += 1;
                            }
                            if partial_count > 0 {
                                info!(
                                    "任务 {} 恢复了 {} 个分片的部分进度（分片内断点续传）",
                                    task_id_clone, partial_count
                                );
                            }
                        }
                    }

                    // 创建任务调度信息
                    let max_concurrent_chunks = calculate_task_max_chunks(total_size);
                    info!(
                        "任务 {} 文件大小 {} 字节, 最大并发分片数: {}",
                        task_id_clone, total_size, max_concurrent_chunks
                    );

                    // 为速度异常检测保存需要的引用
                    let url_health_for_detection = url_health.clone();
                    let client_for_detection = client.read().unwrap().clone();
                    let cancellation_token_for_detection = cancellation_token.clone();
                    let chunk_scheduler_for_detection = chunk_scheduler.clone();

                    // 🔥 获取任务的槽位信息
                    let (slot_id, is_borrowed_slot) = {
                        let t = task_clone.lock().await;
                        (t.slot_id, t.is_borrowed_slot)
                    };

                    // 🔥 创建任务级共享槽位刷新节流器（所有分片共享，防止分片切换重置计时）
                    let touch_id = group_id.clone().unwrap_or_else(|| task_id_clone.clone());
                    let slot_touch_throttler = Arc::new(crate::task_slot_pool::SlotTouchThrottler::new(
                        task_slot_pool_clone.clone(), touch_id,
                    ));

                    // 🔥 构造 HTTP/2 降级触发器闭包：根据信号增减 engine 内部计数
                    let engine_for_trigger = engine.clone();
                    let http11_trigger_arc: crate::downloader::engine::H2DowngradeTrigger =
                        Arc::new(move |signal| match signal {
                            crate::downloader::engine::H2DowngradeSignal::ZeroFailureFrameError => {
                                if engine_for_trigger.report_h2_zero_failure() {
                                    engine_for_trigger.trigger_http11_downgrade();
                                }
                            }
                            crate::downloader::engine::H2DowngradeSignal::DataReceived => {
                                engine_for_trigger.reset_h2_zero_failure_counter();
                            }
                        });

                    let task_info = TaskScheduleInfo {
                        task_id: task_id_clone.clone(),
                        task: task_clone.clone(),
                        chunk_manager,
                        speed_calc,
                        client,
                        cookie,
                        referer,
                        url_health,
                        output_path,
                        chunk_size,
                        total_size,
                        cancellation_token: cancellation_token.clone(),
                        active_chunk_count: Arc::new(AtomicUsize::new(0)),
                        // 🔥 任务级连续分片失败计数器，达阀触发 auto_requeue
                        consecutive_chunk_failures: Arc::new(AtomicU32::new(0)),
                        max_concurrent_chunks,
                        persistence_manager: persistence_manager.clone(),
                        ws_manager: ws_manager.clone(),
                        progress_throttler: Arc::new(ProgressThrottler::default()),
                        folder_progress_tx: folder_progress_tx.clone(),
                        backup_notification_tx: backup_notification_tx.clone(),
                        // 🔥 任务位借调机制字段
                        slot_id,
                        is_borrowed_slot,
                        task_slot_pool: Some(task_slot_pool_clone.clone()),
                        // 🔥 加密服务（用于下载完成后解密）- 由调度器根据 encryption_config_store 动态创建
                        encryption_service: None,
                        // 🔥 快照管理器（用于查询加密文件映射，获取原始文件名）
                        snapshot_manager: snapshot_manager.clone(),
                        // 🔥 加密配置存储（用于根据 key_version 选择正确的解密密钥）
                        encryption_config_store: encryption_config_store.clone(),
                        // 🔥 Manager 任务列表引用（用于任务完成时立即清理）
                        manager_tasks: Some(tasks_clone.clone()),
                        // 🔥 链接级重试次数（从配置读取）
                        max_retries,
                        // 🔥 代理故障回退管理器
                        fallback_mgr: engine.fallback_mgr.clone(),
                        // 🔥 任务级共享槽位刷新节流器
                        slot_touch_throttler,
                        // 🔥 auto_requeue 发送端
                        requeue_tx: Some(requeue_tx_clone.clone()),
                        // 🔥 HTTP/2 降级触发器
                        http11_trigger: Some(http11_trigger_arc),
                        // 🔥 文件夹管理器引用
                        folder_manager: folder_manager_clone_for_schedule.clone(),
                    };

                    // 注册到调度器
                    match chunk_scheduler.register_task(task_info).await {
                        Ok(()) => {
                            // 注册成功，启动速度异常检测循环和线程停滞检测循环
                            info!("任务 {} 注册成功，启动CDN链接检测", task_id_clone);

                            // 🔥 成功注册即视为"启动成功"，清零 start_retry_count
                            //    确保下次再出现 prepare/register 失败时重新计数，
                            //    避免因历史失败次数累积导致过早撞上 MAX_START_RETRIES。
                            {
                                let mut t = task_clone.lock().await;
                                if t.start_retry_count > 0 {
                                    debug!(
                                        "任务 {} 启动成功，重置 start_retry_count {} -> 0",
                                        task_id_clone, t.start_retry_count
                                    );
                                    t.start_retry_count = 0;
                                }
                            }

                            // 创建刷新协调器（每个任务独立一个，防止并发刷新）
                            let refresh_coordinator = Arc::new(RefreshCoordinator::new(
                                RefreshCoordinatorConfig::default(),
                            ));

                            // 启动速度异常检测循环
                            let _speed_anomaly_handle =
                                DownloadEngine::start_speed_anomaly_detection(
                                    engine.clone(),
                                    remote_path.clone(),
                                    total_size,
                                    url_health_for_detection.clone(),
                                    Arc::new(chunk_scheduler_for_detection.clone()),
                                    client_for_detection.clone(),
                                    refresh_coordinator.clone(),
                                    cancellation_token_for_detection.clone(),
                                    SpeedAnomalyConfig::default(),
                                );

                            // 启动线程停滞检测循环
                            let _stagnation_handle = DownloadEngine::start_stagnation_detection(
                                engine.clone(),
                                remote_path,
                                total_size,
                                url_health_for_detection,
                                client_for_detection,
                                Arc::new(chunk_scheduler_for_detection),
                                refresh_coordinator,
                                cancellation_token_for_detection,
                                StagnationConfig::default(),
                            );

                            info!(
                                "📈 任务 {} CDN链接检测已启动（速度异常+线程停滞）",
                                task_id_clone
                            );
                        }
                        Err(e) => {
                            let error_msg = e.to_string();
                            error!("注册任务到调度器失败: {}", error_msg);

                            // 统一处理任务失败逻辑（typed rollback）
                            Self::handle_task_failure(
                                task_id_clone,
                                task_clone,
                                error_msg,
                                waiting_queue,
                                cancellation_tokens,
                                ws_manager,
                                persistence_manager,
                                tasks_clone,
                                task_slot_pool_clone.clone(),
                                folder_manager_arc_clone.clone(),
                                backup_notification_tx,
                            )
                                .await;

                            // 不在这里调用 try_start_waiting_tasks，避免循环引用
                        }
                    }
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    error!("准备任务失败: {}", error_msg);

                    // 统一处理任务失败逻辑（typed rollback）
                    Self::handle_task_failure(
                        task_id_clone,
                        task_clone,
                        error_msg,
                        waiting_queue,
                        cancellation_tokens,
                        ws_manager,
                        persistence_manager,
                        tasks_clone,
                        task_slot_pool_clone.clone(),
                        folder_manager_arc_clone.clone(),
                        backup_notification_tx,
                    )
                        .await;

                    // 不在这里调用 try_start_waiting_tasks，避免循环引用
                }
            }
        });

        Ok(())
    }

    /// 尝试从等待队列启动任务
    ///
    /// 🔥 改用任务槽可用性检查，并在启动前分配槽位
    /// 🔥 区分备份任务和普通任务，实现优先级调度：
    /// - 普通任务优先启动
    /// - 备份任务只有在没有普通任务等待时才启动
    /// - 备份任务使用 allocate_backup_slot（不抢占）
    /// - 普通任务使用 allocate_fixed_slot_with_priority（可抢占备份任务）
    pub(crate) async fn try_start_waiting_tasks(&self) {
        // 🔥 记录本轮被跳过（冷却中）的任务，防止反复 pop→push 死循环
        let mut skipped_cooling: HashSet<String> = HashSet::new();
        // 🔥 记录本轮因"需要全局槽位但 pool 已满"被放回队头的任务，避免反复 pop/push
        let mut skipped_no_global_slot: HashSet<String> = HashSet::new();

        loop {
            // 🔥 不再在循环开头判断 available_slots == 0 直接 break：
            //    文件夹子任务可以走 folder_manager 侧的固定槽位路径，
            //    即便 task_slot_pool 当前为空，这类任务仍有机会启动。
            //    改为在"取出任务并确定类型后"再按需判定是否需要全局槽位。

            // 从等待队列取出任务
            let task_id = {
                let mut queue = self.waiting_queue.write().await;
                queue.pop_front()
            };

            match task_id {
                Some(id) => {
                    // 🔥 获取任务信息：是否为备份任务、是否需要槽位、是否为文件夹子任务、group_id、next_retry_at
                    // needs_slot 同时排除 uses_folder_fixed_slot=true 的子任务，避免它们重新申请槽位
                    let (is_backup, needs_slot, is_folder_subtask, try_start_group_id, next_retry_at) = {
                        if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                            let t = task.lock().await;
                            (
                                t.is_backup,
                                t.slot_id.is_none() && !t.uses_folder_fixed_slot,
                                t.group_id.is_some(),
                                t.group_id.clone(),
                                t.next_retry_at,
                            )
                        } else {
                            // 任务不存在，跳过
                            warn!("等待队列中的任务 {} 不存在，跳过", id);
                            continue;
                        }
                    };
                    // 🔥 注意：touch_slot 的 owner 选择必须延后到"槽位类型已确定"之后
                    //   - 文件夹 fixed/borrowed 槽位：pool owner = group_id
                    //   - 普通全局 fixed 槽位 / backup 槽位：pool owner = task_id
                    // 不要在此处提前根据 group_id 计算单一 touch_id，否则文件夹子任务
                    // fallback 到普通全局 fixed slot 时会刷新错对象。

                    // 🔥 冷却检查：next_retry_at 未到的任务放回队尾，防止立即重试死循环
                    if let Some(until_ms) = next_retry_at {
                        if chrono::Utc::now().timestamp_millis() < until_ms {
                            // 若本轮所有候选都冷却中，跳出避免死循环
                            if skipped_cooling.contains(&id) {
                                self.waiting_queue.write().await.push_back(id);
                                debug!("try_start_waiting_tasks: 队列中任务全部处于冷却期，暂停启动");
                                break;
                            }
                            skipped_cooling.insert(id.clone());
                            self.waiting_queue.write().await.push_back(id);
                            continue;
                        }
                    }

                    // 🔥 备份任务特殊处理：检查是否有普通任务在等待
                    if is_backup {
                        let has_normal_waiting = self.has_normal_tasks_waiting().await;
                        if has_normal_waiting {
                            // 有普通任务等待，备份任务放回队列末尾，让普通任务先执行
                            self.waiting_queue.write().await.push_back(id);
                            info!("备份任务让位：有普通任务等待，备份任务放回队列末尾");
                            continue;
                        }
                    }

                    // 🔥 预先判断此任务是否"必须"走 task_slot_pool 的全局槽位：
                    //    文件夹子任务（非备份）可以尝试走 folder_manager 侧固定槽位；
                    //    其他任务必须有全局空槽才能分配。
                    //    对于只能走全局槽位的任务，若 available_slots==0，放回队头并跳出：
                    //    避免对文件夹任务造成连锁阻塞。
                    let available_slots = self.task_slot_pool.available_slots().await;
                    let can_try_folder_fixed_slot =
                        needs_slot && is_folder_subtask && !is_backup;
                    if needs_slot && !can_try_folder_fixed_slot && available_slots == 0 {
                        // 必须走全局槽位但 pool 已空 → 放回队尾并跳过
                        // 若本轮此 id 已经因同一原因被放回过，说明队列里所有此类任务都过不去，跳出
                        if skipped_no_global_slot.contains(&id) {
                            self.waiting_queue.write().await.push_back(id);
                            debug!("try_start_waiting_tasks: 全局槽位耗尽且队列中剩余任务均需全局槽，暂停启动");
                            break;
                        }
                        skipped_no_global_slot.insert(id.clone());
                        self.waiting_queue.write().await.push_back(id);
                        continue;
                    }

                    info!("⚡ 启动等待队列任务: {} (全局空槽: {}, is_backup: {}, is_folder_subtask: {})", id, available_slots, is_backup, is_folder_subtask);

                    // 🔥 文件夹子任务优先占用同组的文件夹固定槽位
                    let mut used_folder_fixed_slot = false;
                    if needs_slot && is_folder_subtask && !is_backup {
                        if let Some(ref folder_id) = try_start_group_id {
                            let folder_mgr = self.folder_manager.read().await.clone();
                            if let Some(fm) = folder_mgr {
                                if fm.try_allocate_fixed_slot_for_subtask(folder_id, &id).await {
                                    if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                                        let mut t = task.lock().await;
                                        // 使用文件夹固定槽位 → 不占用 task_slot_pool 也不占借调位
                                        t.slot_id = None;
                                        t.is_borrowed_slot = false;
                                        // 🔥 关键：标记此任务正在使用文件夹固定槽位
                                        // 后续等待队列消费点据此判定无需再申请槽位，避免自循环
                                        t.uses_folder_fixed_slot = true;
                                    }
                                    used_folder_fixed_slot = true;
                                    // 🔥 文件夹 fixed slot 分配即 touch，与普通 fixed/backup 分配后的
                                    //    touch_slot 语义保持一致，防止随后的 prepare_for_scheduling
                                    //    阶段耗时较长导致槽位超时监控误回收该 group 的 slot。
                                    self.task_slot_pool.touch_slot(folder_id).await;
                                    info!(
                                        "⚡ 文件夹子任务 {} 占用文件夹 {} 的固定槽位，已刷新槽位时间戳",
                                        id, folder_id
                                    );
                                }
                            }
                        }
                    }

                    if needs_slot && !used_folder_fixed_slot {
                        // 🔥 根据任务类型选择不同的槽位分配方法
                        if is_backup {
                            // 备份任务：只能使用空闲槽位
                            let slot_id = self.task_slot_pool.allocate_backup_slot(&id).await;
                            if let Some(sid) = slot_id {
                                if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                                    let mut t = task.lock().await;
                                    t.slot_id = Some(sid);
                                    t.is_borrowed_slot = false;
                                    info!("为备份任务 {} 分配槽位: {}", id, sid);
                                }
                                // 🔥 backup slot 在 task_slot_pool 中 owner=task_id，必须按 id touch
                                self.task_slot_pool.touch_slot(&id).await;
                            } else {
                                // 分配失败，放回队列末尾（备份任务优先级最低）
                                warn!("无法为备份任务 {} 分配槽位，放回等待队列末尾", id);
                                self.waiting_queue.write().await.push_back(id);
                                break;
                            }
                        } else {
                            // 🔥 非备份任务：根据是否为文件夹子任务选择优先级
                            let priority = if is_folder_subtask {
                                TaskPriority::SubTask
                            } else {
                                TaskPriority::Normal
                            };
                            let task_type_str = if is_folder_subtask { "文件夹子任务" } else { "普通任务" };

                            let result = self.task_slot_pool.allocate_fixed_slot_with_priority(
                                &id, false, priority
                            ).await;

                            match result {
                                Some((sid, preempted_task_id)) => {
                                    if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                                        let mut t = task.lock().await;
                                        t.slot_id = Some(sid);
                                        t.is_borrowed_slot = false;
                                    }

                                    // 🔥 普通全局 fixed slot 在 task_slot_pool 中 owner=task_id
                                    //    文件夹子任务 fallback 到此路径时不能用 group_id touch
                                    self.task_slot_pool.touch_slot(&id).await;

                                    // 处理被抢占的备份任务
                                    if let Some(preempted_id) = preempted_task_id {
                                        info!("{} {} 抢占了备份任务 {} 的槽位: slot_id={}，已刷新槽位时间戳", task_type_str, id, preempted_id, sid);
                                        // 🔥 直接暂停被抢占的任务（不调用 pause_task 避免递归）
                                        self.pause_preempted_task(&preempted_id).await;
                                        // 🔥 将被暂停的备份任务加入等待队列末尾（包含状态转换和通知）
                                        self.add_preempted_backup_to_queue(&preempted_id).await;
                                    } else {
                                        info!("为{} {} 分配槽位: {}，已刷新槽位时间戳", task_type_str, id, sid);
                                    }
                                }
                                None => {
                                    // 分配失败，使用优先级方法放回队列
                                    warn!("无法为{} {} 分配槽位，放回等待队列", task_type_str, id);
                                    self.add_to_waiting_queue_with_task_type(&id, is_backup, is_folder_subtask).await;
                                    break;
                                }
                            }
                        }
                    }

                    // 🔥 任务被真正拉起：清除冷却字段和错误信息
                    if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                        let mut t = task.lock().await;
                        t.next_retry_at = None;
                        t.error = None;
                    }
                    if let Some(ref pm) = self.persistence_manager {
                        if let Err(e) = pm.lock().await.clear_task_error(&id) {
                            warn!("清除任务错误信息失败: {}", e);
                        }
                    }

                    // 启动任务
                    if let Err(e) = self.start_task_internal(&id).await {
                        error!("启动等待任务失败: {}, 错误: {}", id, e);
                    }
                }
                None => break, // 队列为空
            }
        }
    }

    /// 启动后台监控任务：定期检查并启动等待队列中的任务
    ///
    /// 这确保了当活跃任务自然完成时，等待队列中的任务能被自动启动
    /// 🔥 改用任务槽可用性检查，并在启动前分配槽位
    fn start_waiting_queue_monitor(&self) {
        let waiting_queue = self.waiting_queue.clone();
        let chunk_scheduler = self.chunk_scheduler.clone();
        let tasks = self.tasks.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();
        let engine = self.engine.clone();
        let task_slot_pool = self.task_slot_pool.clone();
        let persistence_manager = self.persistence_manager.clone();
        let ws_manager_arc = self.ws_manager.clone();
        let folder_progress_tx_arc = self.folder_progress_tx.clone();
        let backup_notification_tx_arc = self.backup_notification_tx.clone();
        let snapshot_manager_arc = self.snapshot_manager.clone(); // 🔥 用于查询加密文件映射
        let encryption_config_store_arc = self.encryption_config_store.clone(); // 🔥 用于根据 key_version 选择解密密钥
        let max_retries = self.max_retries;
        // 🔥 auto_requeue 发送端和文件夹管理器引用
        let requeue_tx_for_monitor = self.requeue_tx.clone();
        let folder_manager_arc_for_monitor = self.folder_manager.clone();
        // 不再 capture self.owner_uid。
        // 共享 manager 设计下 self.owner_uid 不可靠（同一个 Arc 服务多个账号）。
        // 事件归属一律读 `task.owner_uid`（已在内层 spawn 后从 task 读出 task_owner_uid）。

        tokio::spawn(async move {
            // 🔥 优化：缩短检查间隔从3秒到1秒，减少等待时间
            // 注意：有了0延迟触发器后，这里主要作为保底机制
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

            loop {
                interval.tick().await;

                // 检查是否有等待任务
                let has_waiting = {
                    let queue = waiting_queue.read().await;
                    !queue.is_empty()
                };

                if !has_waiting {
                    continue;
                }

                // 🔥 不再因 available_slots == 0 直接 continue：
                //    文件夹子任务可以走 folder_manager 固定槽位路径，与 try_start_waiting_tasks 语义一致。

                // 尝试启动等待任务
                //   外层使用 HashSet 记录"本轮因无全局槽位被放回"的任务，避免在单轮内反复 pop/push
                let mut skipped_no_global_slot: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                loop {
                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            // 🔥 冷却检查：next_retry_at 未到的任务放回队尾
                            let cooling = {
                                if let Some(t_arc) = tasks.read().await.get(&id).cloned() {
                                    let t = t_arc.lock().await;
                                    matches!(
                                        t.next_retry_at,
                                        Some(until) if chrono::Utc::now().timestamp_millis() < until
                                    )
                                } else {
                                    false
                                }
                            };
                            if cooling {
                                waiting_queue.write().await.push_back(id);
                                // 冷却任务放回队尾，跳出当前循环等待下一轮 tick
                                break;
                            }

                            // 获取任务
                            let task = tasks.read().await.get(&id).cloned();
                            if let Some(task) = task {
                                // 🔥 获取任务信息：是否需要槽位、是否为备份任务、是否为文件夹子任务、group_id
                                // 文件夹固定槽位子任务（uses_folder_fixed_slot=true）不需要再申请槽位
                                let (needs_slot, is_backup, is_folder_subtask, monitor_group_id) = {
                                    let t = task.lock().await;
                                    (
                                        t.slot_id.is_none() && !t.uses_folder_fixed_slot,
                                        t.is_backup,
                                        t.group_id.is_some(),
                                        t.group_id.clone(),
                                    )
                                };
                                // 🔥 注意：touch_slot 的 owner 选择必须等到"槽位类型已确定"之后
                                //   - 文件夹 fixed/borrowed 槽位：pool owner = group_id（folder_id）
                                //   - 普通全局 fixed 槽位 / backup 槽位：pool owner = task_id（id）
                                // 不要提前据 group_id 一票选 owner，否则文件夹子任务 fallback 到普通全局 fixed slot 时会刷新错对象。

                                // 🔥 预检查：只能走全局槽位的任务若 pool 已满，放回队尾跳过
                                //    文件夹子任务（非备份）可以尝试 folder_manager 固定位路径
                                let available_slots = task_slot_pool.available_slots().await;
                                let can_try_folder_fixed_slot =
                                    needs_slot && is_folder_subtask && !is_backup;
                                if needs_slot && !can_try_folder_fixed_slot && available_slots == 0 {
                                    if skipped_no_global_slot.contains(&id) {
                                        waiting_queue.write().await.push_back(id);
                                        break;
                                    }
                                    skipped_no_global_slot.insert(id.clone());
                                    waiting_queue.write().await.push_back(id);
                                    continue;
                                }

                                info!(
                                    "🔄 后台监控：从等待队列启动任务 {} (全局空槽: {}, is_backup: {}, is_folder_subtask: {})",
                                    id, available_slots, is_backup, is_folder_subtask
                                );

                                if needs_slot {
                                    // 🔥 文件夹子任务优先尝试同文件夹的固定槽位
                                    let mut used_folder_fixed_slot = false;
                                    if can_try_folder_fixed_slot {
                                        if let Some(ref folder_id) = monitor_group_id {
                                            let folder_mgr =
                                                folder_manager_arc_for_monitor.read().await.clone();
                                            if let Some(fm) = folder_mgr {
                                                if fm
                                                    .try_allocate_fixed_slot_for_subtask(
                                                        folder_id, &id,
                                                    )
                                                    .await
                                                {
                                                    let mut t = task.lock().await;
                                                    t.slot_id = None;
                                                    t.is_borrowed_slot = false;
                                                    t.uses_folder_fixed_slot = true;
                                                    drop(t);
                                                    used_folder_fixed_slot = true;
                                                    // 🔥 文件夹 fixed slot 分配即 touch（保持与普通槽位分配后 touch 一致）
                                                    task_slot_pool.touch_slot(folder_id).await;
                                                    info!(
                                                        "后台监控：文件夹子任务 {} 占用文件夹 {} 的固定槽位，已刷新槽位时间戳",
                                                        id, folder_id
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    if !used_folder_fixed_slot {
                                        // 🔥 根据任务类型选择优先级
                                        let priority = if is_backup {
                                            TaskPriority::Backup
                                        } else if is_folder_subtask {
                                            TaskPriority::SubTask
                                        } else {
                                            TaskPriority::Normal
                                        };

                                        // 🔥 备份任务使用 allocate_backup_slot，其他任务使用带优先级的分配
                                        let slot_result = if is_backup {
                                            task_slot_pool.allocate_backup_slot(&id).await.map(|sid| (sid, None))
                                        } else {
                                            task_slot_pool.allocate_fixed_slot_with_priority(&id, false, priority).await
                                        };

                                        if let Some((sid, preempted_task_id)) = slot_result {
                                            // 分配成功，更新任务槽位信息
                                            let mut t = task.lock().await;
                                            t.slot_id = Some(sid);
                                            t.is_borrowed_slot = false;
                                            info!("后台监控：为任务 {} 分配槽位: {} (priority: {:?})", id, sid, priority);
                                            drop(t); // 释放锁

                                            // 🔥 backup_slot / 普通全局 fixed slot 在 pool 中 owner=task_id，按 id touch
                                            //    （文件夹 fixed slot 走另一分支，已用 folder_id touch）
                                            task_slot_pool.touch_slot(&id).await;

                                            // 🔥 处理被抢占的备份任务
                                            if let Some(preempted_id) = preempted_task_id {
                                                info!("后台监控：任务 {} 抢占了备份任务 {} 的槽位", id, preempted_id);
                                                // 暂停被抢占的任务并加入等待队列
                                                Self::pause_and_requeue_preempted_task(
                                                    &tasks, &cancellation_tokens, &waiting_queue, &preempted_id
                                                ).await;
                                            }
                                        } else {
                                            // 分配失败，使用优先级方法放回队列
                                            warn!("后台监控：无法为任务 {} 分配槽位，放回等待队列", id);
                                            Self::add_to_queue_by_priority(&waiting_queue, &tasks, &id, is_backup, is_folder_subtask).await;
                                            break;
                                        }
                                    }
                                }
                                // 创建取消令牌
                                let cancellation_token = CancellationToken::new();
                                cancellation_tokens
                                    .write()
                                    .await
                                    .insert(id.clone(), cancellation_token.clone());

                                // 启动任务（简化版，直接在这里处理）
                                let engine_clone = engine.clone();
                                let task_clone = task.clone();
                                let chunk_scheduler_clone = chunk_scheduler.clone();
                                let id_clone = id.clone();
                                let cancellation_tokens_clone = cancellation_tokens.clone();
                                let persistence_manager_clone = persistence_manager.clone();
                                let ws_manager_arc_clone = ws_manager_arc.clone();
                                let folder_progress_tx_arc_clone = folder_progress_tx_arc.clone();
                                let backup_notification_tx_arc_clone = backup_notification_tx_arc.clone();
                                let waiting_queue_clone = waiting_queue.clone();
                                let task_slot_pool_clone = task_slot_pool.clone();
                                let tasks_clone = tasks.clone(); // 🔥 用于 handle_task_failure 的优先级队列插入
                                let snapshot_manager_arc_clone = snapshot_manager_arc.clone(); // 🔥 用于查询加密文件映射
                                let encryption_config_store_arc_clone = encryption_config_store_arc.clone(); // 🔥 用于根据 key_version 选择解密密钥
                                // 🔥 auto_requeue 发送端和文件夹管理器引用
                                let requeue_tx_cloned_monitor = requeue_tx_for_monitor.clone();
                                let folder_manager_arc_clone = folder_manager_arc_for_monitor.clone();

                                tokio::spawn(async move {
                                    // 获取 WebSocket 管理器和文件夹进度发送器
                                    let ws_manager = ws_manager_arc_clone.read().await.clone();
                                    let folder_progress_tx =
                                        folder_progress_tx_arc_clone.read().await.clone();
                                    let backup_notification_tx =
                                        backup_notification_tx_arc_clone.read().await.clone();
                                    let snapshot_manager = snapshot_manager_arc_clone.read().await.clone(); // 🔥 获取快照管理器
                                    let encryption_config_store = encryption_config_store_arc_clone.read().await.clone(); // 🔥 获取加密配置存储
                                    // 🔥 文件夹管理器（构造 TaskScheduleInfo 时使用）
                                    let folder_manager_for_task = folder_manager_arc_clone.read().await.clone();
                                    let prepare_result = engine_clone
                                        .prepare_for_scheduling(
                                            task_clone.clone(),
                                            cancellation_token.clone(),
                                        )
                                        .await;

                                    // 探测完成后，先检查是否被取消
                                    if cancellation_token.is_cancelled() {
                                        info!("后台监控:任务 {} 在探测完成后发现已被取消", id_clone);
                                        return;
                                    }

                                    match prepare_result {
                                        Ok((
                                               client,
                                               cookie,
                                               referer,
                                               url_health,
                                               output_path,
                                               chunk_size,
                                               chunk_manager,
                                               speed_calc,
                                           )) => {
                                            // 获取文件总大小、远程路径和 fs_id
                                            // 🔥 同时读取 is_borrowed_slot / uses_folder_fixed_slot，用于下方 prepare 后 touch 的 owner 判定
                                            let (
                                                total_size,
                                                remote_path,
                                                fs_id,
                                                local_path,
                                                group_id,
                                                group_root,
                                                relative_path,
                                                is_backup,
                                                backup_config_id,
                                                transfer_task_id,
                                                is_borrowed_slot,
                                                uses_folder_fixed_slot,
                                                task_owner_uid,
                                            ) = {
                                                let t = task_clone.lock().await;
                                                (
                                                    t.total_size,
                                                    t.remote_path.clone(),
                                                    t.fs_id,
                                                    t.local_path.clone(),
                                                    t.group_id.clone(),
                                                    t.group_root.clone(),
                                                    t.relative_path.clone(),
                                                    t.is_backup,
                                                    t.backup_config_id.clone(),
                                                    t.transfer_task_id.clone(),
                                                    t.is_borrowed_slot,
                                                    t.uses_folder_fixed_slot,
                                                    //
                                                    t.owner_uid,
                                                )
                                            };

                                            // 获取分片数
                                            let total_chunks = {
                                                let cm = chunk_manager.lock().await;
                                                cm.chunk_count()
                                            };

                                            // 🔥 prepare_for_scheduling 完成后立即刷新槽位
                                            //   touch owner 必须与"任务实际持有的槽位类型"对应：
                                            //   - 文件夹 fixed/borrowed 槽位 → pool owner = group_id
                                            //   - 普通全局 fixed / backup 槽位 → pool owner = task_id
                                            {
                                                let prepare_touch_id = if (uses_folder_fixed_slot
                                                    || is_borrowed_slot)
                                                    && group_id.is_some()
                                                {
                                                    group_id.clone().unwrap()
                                                } else {
                                                    id_clone.clone()
                                                };
                                                task_slot_pool_clone.touch_slot(&prepare_touch_id).await;
                                            }

                                            // 🔥 发送状态变更事件：pending → downloading
                                            // 此时 prepare_for_scheduling 已完成，任务状态已变为 Downloading
                                            if is_backup {
                                                // 备份任务：发送到 backup_notification_tx
                                                use crate::autobackup::events::TransferTaskType;
                                                if let Some(ref tx) = backup_notification_tx {
                                                    let notification = BackupTransferNotification::StatusChanged {
                                                        task_id: id_clone.clone(),
                                                        task_type: TransferTaskType::Download,
                                                        old_status: crate::autobackup::events::TransferTaskStatus::Pending,
                                                        new_status: crate::autobackup::events::TransferTaskStatus::Transferring,
                                                    };
                                                    let _ = tx.send(notification);
                                                }
                                            } else if let Some(ref ws) = ws_manager {
                                                // 普通任务：发送到 WebSocket
                                                ws.send_if_subscribed(
                                                    TaskEvent::Download(DownloadEvent::StatusChanged {
                                                        task_id: id_clone.clone(),
                                                        old_status: "pending".to_string(),
                                                        new_status: "downloading".to_string(),
                                                        group_id: group_id.clone(),
                                                        is_backup,
                                                        error: None,

                                                        owner_uid: Some(task_owner_uid.raw()),
                                                    }),
                                                    group_id.clone(),
                                                );
                                            }

                                            // 🔥 检测是否为加密文件，并获取 key_version
                                            let (is_encrypted, encryption_key_version) = {
                                                let filename = local_path
                                                    .file_name()
                                                    .and_then(|n| n.to_str())
                                                    .unwrap_or("");

                                                // 通过文件名检测是否为加密文件
                                                let is_encrypted = DownloadTask::detect_encrypted_filename(filename);

                                                // 如果是加密文件，尝试从 snapshot_manager 获取 key_version
                                                let key_version = if is_encrypted {
                                                    if let Some(ref snapshot_mgr) = snapshot_manager {
                                                        match snapshot_mgr.find_by_encrypted_name(filename) {
                                                            Ok(Some(snapshot_info)) => {
                                                                debug!(
                                                                    "后台任务 {} 从映射表获取 key_version: {}",
                                                                    id_clone, snapshot_info.key_version
                                                                );
                                                                Some(snapshot_info.key_version)
                                                            }
                                                            Ok(None) => {
                                                                debug!("后台任务 {} 在映射表中未找到加密信息", id_clone);
                                                                None
                                                            }
                                                            Err(e) => {
                                                                warn!("后台任务 {} 查询映射表失败: {}", id_clone, e);
                                                                None
                                                            }
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                } else {
                                                    None
                                                };

                                                (if is_encrypted { Some(true) } else { None }, key_version)
                                            };

                                            // 🔥 注册任务到持久化管理器
                                            // 显式传 task.owner_uid
                                            if let Some(ref pm) = persistence_manager_clone {
                                                if let Err(e) = pm.lock().await.register_download_task(
                                                    id_clone.clone(),
                                                    fs_id,
                                                    remote_path.clone(),
                                                    local_path.clone(),
                                                    total_size,
                                                    chunk_size,
                                                    total_chunks,
                                                    group_id.clone(),
                                                    group_root.clone(),
                                                    relative_path.clone(),
                                                    is_backup,
                                                    backup_config_id.clone(),
                                                    is_encrypted,
                                                    encryption_key_version,
                                                    transfer_task_id.clone(),
                                                    Some(task_owner_uid.raw()),
                                                ) {
                                                    warn!(
                                                        "后台监控：注册任务到持久化管理器失败: {}",
                                                        e
                                                    );
                                                }

                                                // 🔥 修复：从持久化管理器获取已完成的分片，并标记到 ChunkManager（实现真正的断点续传）
                                                if let Some(completed_chunks) = pm.lock().await.get_completed_chunks(&id_clone) {
                                                    let mut cm = chunk_manager.lock().await;
                                                    let mut completed_count = 0;
                                                    for chunk_index in completed_chunks.iter() {
                                                        cm.mark_completed(chunk_index);
                                                        completed_count += 1;
                                                    }
                                                    if completed_count > 0 {
                                                        info!(
                                                            "后台任务 {} 恢复了 {} 个已完成分片，将跳过这些分片的下载",
                                                            id_clone, completed_count
                                                        );
                                                    }
                                                }
                                                // 🔥 恢复分片内部分进度（分片内断点续传）
                                                if let Some(partial_progress) = pm.lock().await.get_partial_progress(&id_clone) {
                                                    let mut cm = chunk_manager.lock().await;
                                                    let mut partial_count = 0;
                                                    for (chunk_index, bytes_downloaded) in &partial_progress {
                                                        cm.update_bytes_downloaded(*chunk_index, *bytes_downloaded);
                                                        partial_count += 1;
                                                    }
                                                    if partial_count > 0 {
                                                        info!(
                                                            "后台任务 {} 恢复了 {} 个分片的部分进度（分片内断点续传）",
                                                            id_clone, partial_count
                                                        );
                                                    }
                                                }
                                            }

                                            let max_concurrent_chunks =
                                                calculate_task_max_chunks(total_size);
                                            info!(
                                                "后台任务 {} 文件大小 {} 字节, 最大并发分片数: {}",
                                                id_clone, total_size, max_concurrent_chunks
                                            );

                                            // 为速度异常检测保存需要的引用
                                            let url_health_for_detection = url_health.clone();
                                            let client_for_detection = client.read().unwrap().clone();
                                            let cancellation_token_for_detection =
                                                cancellation_token.clone();
                                            let chunk_scheduler_for_detection =
                                                chunk_scheduler_clone.clone();

                                            // 🔥 获取任务的槽位信息
                                            let (slot_id, is_borrowed_slot) = {
                                                let t = task_clone.lock().await;
                                                (t.slot_id, t.is_borrowed_slot)
                                            };

                                            // 创建任务级共享槽位刷新节流器（所有分片共享，防止分片切换重置计时）
                                            let touch_id = group_id.clone().unwrap_or_else(|| id_clone.clone());
                                            let slot_touch_throttler = Arc::new(crate::task_slot_pool::SlotTouchThrottler::new(
                                                task_slot_pool_clone.clone(), touch_id,
                                            ));

                                            // 🔥 构造 HTTP/2 降级触发器闭包：根据信号增减 engine 内部计数
                                            let engine_for_trigger = engine_clone.clone();
                                            let http11_trigger_arc: crate::downloader::engine::H2DowngradeTrigger =
                                                Arc::new(move |signal| match signal {
                                                    crate::downloader::engine::H2DowngradeSignal::ZeroFailureFrameError => {
                                                        if engine_for_trigger.report_h2_zero_failure() {
                                                            engine_for_trigger.trigger_http11_downgrade();
                                                        }
                                                    }
                                                    crate::downloader::engine::H2DowngradeSignal::DataReceived => {
                                                        engine_for_trigger.reset_h2_zero_failure_counter();
                                                    }
                                                });

                                            let task_info = TaskScheduleInfo {
                                                task_id: id_clone.clone(),
                                                task: task_clone.clone(),
                                                chunk_manager,
                                                speed_calc,
                                                client,
                                                cookie,
                                                referer,
                                                url_health,
                                                output_path,
                                                chunk_size,
                                                total_size,
                                                cancellation_token: cancellation_token.clone(),
                                                active_chunk_count: Arc::new(AtomicUsize::new(0)),
                                                // 🔥 任务级连续分片失败计数器，达阀触发 auto_requeue
                                                consecutive_chunk_failures: Arc::new(AtomicU32::new(0)),
                                                max_concurrent_chunks,
                                                persistence_manager: persistence_manager_clone
                                                    .clone(),
                                                ws_manager: ws_manager.clone(),
                                                progress_throttler: Arc::new(
                                                    ProgressThrottler::default(),
                                                ),
                                                folder_progress_tx: folder_progress_tx.clone(),
                                                backup_notification_tx: backup_notification_tx.clone(),
                                                // 🔥 任务位借调机制字段
                                                slot_id,
                                                is_borrowed_slot,
                                                task_slot_pool: Some(task_slot_pool_clone.clone()),
                                                // 🔥 加密服务（用于下载完成后解密）- 由调度器根据 encryption_config_store 动态创建
                                                encryption_service: None,
                                                // 🔥 快照管理器（用于查询加密文件映射，获取原始文件名）
                                                snapshot_manager: snapshot_manager.clone(),
                                                // 🔥 加密配置存储（用于根据 key_version 选择正确的解密密钥）
                                                encryption_config_store: encryption_config_store.clone(),
                                                // 🔥 Manager 任务列表引用（用于任务完成时立即清理）
                                                manager_tasks: Some(tasks_clone.clone()),
                                                // 🔥 链接级重试次数（从配置读取）
                                                max_retries,
                                                // 🔥 代理故障回退管理器
                                                fallback_mgr: engine_clone.fallback_mgr.clone(),
                                                // 🔥 任务级共享槽位刷新节流器
                                                slot_touch_throttler,
                                                // 🔥 auto_requeue 发送端
                                                requeue_tx: Some(requeue_tx_cloned_monitor.clone()),
                                                // 🔥 HTTP/2 降级触发器
                                                http11_trigger: Some(http11_trigger_arc),
                                                // 🔥 文件夹管理器引用
                                                folder_manager: folder_manager_for_task.clone(),
                                            };

                                            // 注册任务到调度器
                                            match chunk_scheduler_clone
                                                .register_task(task_info)
                                                .await
                                            {
                                                Ok(()) => {
                                                    // 注册成功，启动速度异常检测循环和线程停滞检测循环
                                                    info!(
                                                        "后台任务 {} 注册成功，启动CDN链接检测",
                                                        id_clone
                                                    );

                                                    // 🔥 成功注册即视为"启动成功"，清零 start_retry_count
                                                    {
                                                        let mut t = task_clone.lock().await;
                                                        if t.start_retry_count > 0 {
                                                            debug!(
                                                                "后台监控：任务 {} 启动成功，重置 start_retry_count {} -> 0",
                                                                id_clone, t.start_retry_count
                                                            );
                                                            t.start_retry_count = 0;
                                                        }
                                                    }

                                                    // 创建刷新协调器
                                                    let refresh_coordinator =
                                                        Arc::new(RefreshCoordinator::new(
                                                            RefreshCoordinatorConfig::default(),
                                                        ));

                                                    // 启动速度异常检测循环
                                                    let _speed_anomaly_handle = DownloadEngine::start_speed_anomaly_detection(
                                                        engine_clone.clone(),
                                                        remote_path.clone(),
                                                        total_size,
                                                        url_health_for_detection.clone(),
                                                        Arc::new(chunk_scheduler_for_detection.clone()),
                                                        client_for_detection.clone(),
                                                        refresh_coordinator.clone(),
                                                        cancellation_token_for_detection.clone(),
                                                        SpeedAnomalyConfig::default(),
                                                    );

                                                    // 启动线程停滞检测循环
                                                    let _stagnation_handle =
                                                        DownloadEngine::start_stagnation_detection(
                                                            engine_clone.clone(),
                                                            remote_path,
                                                            total_size,
                                                            url_health_for_detection,
                                                            client_for_detection,
                                                            Arc::new(chunk_scheduler_for_detection),
                                                            refresh_coordinator,
                                                            cancellation_token_for_detection,
                                                            StagnationConfig::default(),
                                                        );

                                                    info!("📈 后台任务 {} CDN链接检测已启动（速度异常+线程停滞）", id_clone);
                                                }
                                                Err(e) => {
                                                    let error_msg = e.to_string();
                                                    error!("后台监控：注册任务失败: {}", error_msg);

                                                    // 统一处理任务失败逻辑（typed rollback）
                                                    Self::handle_task_failure(
                                                        id_clone,
                                                        task_clone,
                                                        error_msg,
                                                        waiting_queue_clone,
                                                        cancellation_tokens_clone,
                                                        ws_manager,
                                                        persistence_manager_clone,
                                                        tasks_clone,
                                                        task_slot_pool_clone.clone(),
                                                        folder_manager_arc_clone.clone(),
                                                        backup_notification_tx,
                                                    )
                                                        .await;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let error_msg = e.to_string();
                                            error!("后台监控：准备任务失败: {}", error_msg);

                                            // 统一处理任务失败逻辑（typed rollback）
                                            Self::handle_task_failure(
                                                id_clone,
                                                task_clone,
                                                error_msg,
                                                waiting_queue_clone,
                                                cancellation_tokens_clone,
                                                ws_manager,
                                                persistence_manager_clone,
                                                tasks_clone,
                                                task_slot_pool_clone.clone(),
                                                folder_manager_arc_clone.clone(),
                                                backup_notification_tx,
                                            )
                                                .await;
                                        }
                                    }
                                });
                            } else {
                                // 任务不存在，跳过
                                warn!("后台监控：任务 {} 不存在，跳过", id);
                            }
                        }
                        None => {
                            // 队列为空
                            break;
                        }
                    }
                }
            }
        });
    }

    /// 🔥 设置槽位超时释放处理器
    ///
    /// 当槽位因超时被自动释放时，将对应任务状态设置为失败
    fn setup_stale_release_handler(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // 设置通知通道到槽位池
        let task_slot_pool = self.task_slot_pool.clone();
        tokio::spawn(async move {
            task_slot_pool.set_stale_release_handler(tx).await;
        });

        // 启动监听循环
        let tasks = self.tasks.clone();
        let ws_manager = self.ws_manager.clone();
        let chunk_scheduler = self.chunk_scheduler.clone();
        // 🔥 备份任务终态失败需要走 BackupTransferNotification::Failed，
        //    与 publish_event() 的"备份任务跳过普通下载事件"约定保持一致。
        let backup_notification_tx_arc = self.backup_notification_tx.clone();
        // 🔥 槽位超时终态失败也要写持久化，否则重启后这条失败任务会凭空消失。
        let persistence_manager = self.persistence_manager.clone();
        // 不再 capture self.owner_uid。
        // 共享 manager 设计下 self.owner_uid 不可靠（同一个 Arc 服务多个账号）。
        // 事件归属一律读 `task.owner_uid`（在锁内一并取出）。
        tokio::spawn(async move {
            while let Some(task_id) = rx.recv().await {
                info!("收到槽位超时释放通知，将任务设置为失败: {}", task_id);

                const STALE_ERROR_MSG: &str =
                    "槽位超时释放：任务长时间无进度更新，可能已卡住";

                // 更新任务状态为失败
                let tasks_guard = tasks.read().await;
                if let Some(task) = tasks_guard.get(&task_id) {
                    // 🔥 先在锁内读出后续要用到的字段，再尽快释放锁，避免发送通知时持锁过久
                    let (group_id, total_size, is_backup, task_owner_uid_raw) = {
                        let mut t = task.lock().await;
                        t.status = crate::downloader::TaskStatus::Failed;
                        t.error = Some(STALE_ERROR_MSG.to_string());
                        // 🔥 清除已释放的槽位ID，避免重试时误以为还持有槽位
                        t.slot_id = None;
                        (t.group_id.clone(), t.total_size, t.is_backup, t.owner_uid.raw())
                    };

                    // 发送终态失败通知：备份任务走 BackupTransferNotification::Failed，
                    // 普通下载任务保持原有的 WebSocket 路径。
                    if is_backup {
                        let tx_guard = backup_notification_tx_arc.read().await;
                        if let Some(tx) = tx_guard.as_ref() {
                            use crate::autobackup::events::TransferTaskType;
                            let notification = BackupTransferNotification::Failed {
                                task_id: task_id.clone(),
                                task_type: TransferTaskType::Download,
                                error_message: STALE_ERROR_MSG.to_string(),
                            };
                            if let Err(e) = tx.send(notification) {
                                warn!(
                                    "槽位超时：发送备份任务失败通知失败 (task_id={}): {}",
                                    task_id, e
                                );
                            }
                        } else {
                            warn!(
                                "槽位超时：备份任务 {} 终态失败但 backup_notification_tx 未设置",
                                task_id
                            );
                        }
                    } else {
                        let ws_guard = ws_manager.read().await;
                        if let Some(ref ws) = *ws_guard {
                            use crate::server::events::{DownloadEvent, TaskEvent};
                            ws.send_if_subscribed(
                                TaskEvent::Download(DownloadEvent::Failed {
                                    task_id: task_id.clone(),
                                    error: STALE_ERROR_MSG.to_string(),
                                    group_id: group_id.clone(),
                                    is_backup,

                                    owner_uid: Some(task_owner_uid_raw),
                                }),
                                group_id.clone(),
                            );
                        }
                    }

                    // 🔥 写持久化错误并标记为失败，确保重启后该失败任务仍保留（终态可见、可重试/删除）
                    if let Some(ref pm) = persistence_manager {
                        if let Err(e) = pm
                            .lock()
                            .await
                            .update_task_error(&task_id, STALE_ERROR_MSG.to_string())
                        {
                            warn!("槽位超时：更新下载任务错误信息失败 (task_id={}): {}", task_id, e);
                        }
                    }

                    // 🔥 通知文件夹管理器子任务失败
                    if let Some(gid) = group_id {
                        chunk_scheduler
                            .notify_subtask_failed(gid, task_id.clone(), total_size)
                            .await;
                    }
                }
            }
        });

        info!("下载管理器已设置槽位超时释放处理器");
    }

    /// 🔥 设置任务完成触发器（0延迟启动等待任务）
    ///
    /// 当调度器检测到任务完成时，会通过 channel 发送信号，
    /// 这里的监听循环会立即响应并启动等待队列中的任务
    fn setup_waiting_queue_trigger(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        // 设置触发器到调度器
        let chunk_scheduler = self.chunk_scheduler.clone();
        tokio::spawn(async move {
            chunk_scheduler.set_waiting_queue_trigger(tx).await;
        });

        // 启动监听循环
        let waiting_queue = self.waiting_queue.clone();
        let chunk_scheduler = self.chunk_scheduler.clone();
        let tasks = self.tasks.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();
        let engine = self.engine.clone();
        let task_slot_pool = self.task_slot_pool.clone();
        let persistence_manager = self.persistence_manager.clone();
        let ws_manager_arc = self.ws_manager.clone();
        let folder_progress_tx_arc = self.folder_progress_tx.clone();
        let backup_notification_tx_arc = self.backup_notification_tx.clone();
        let snapshot_manager_arc = self.snapshot_manager.clone(); // 🔥 用于查询加密文件映射
        let encryption_config_store_arc = self.encryption_config_store.clone(); // 🔥 用于根据 key_version 选择解密密钥
        let max_retries = self.max_retries;
        // 🔥 auto_requeue 发送端和文件夹管理器引用
        let requeue_tx_for_trigger = self.requeue_tx.clone();
        let folder_manager_arc_for_trigger = self.folder_manager.clone();
        // 不再 capture self.owner_uid。
        // 共享 manager 设计下 self.owner_uid 不可靠（同一个 Arc 服务多个账号）。
        // 事件归属一律读 `task.owner_uid`（已在内层 spawn 后从 task 读出 task_owner_uid）。

        tokio::spawn(async move {
            while let Some(()) = rx.recv().await {
                // 收到任务完成信号，立即检查并启动等待任务
                // 检查是否有等待任务
                let has_waiting = {
                    let queue = waiting_queue.read().await;
                    !queue.is_empty()
                };

                if !has_waiting {
                    continue;
                }

                // 🔥 不再因 available_slots == 0 直接 continue：
                //    文件夹子任务可以走 folder_manager 固定槽位路径，与 try_start_waiting_tasks 语义一致。

                info!("⚡ 收到任务完成信号，立即启动等待任务");

                // 尝试启动等待任务（与 start_waiting_queue_monitor 逻辑对齐）
                let mut skipped_no_global_slot: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                loop {
                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            // 🔥 冷却检查：next_retry_at 未到的任务放回队尾
                            let cooling = {
                                if let Some(t_arc) = tasks.read().await.get(&id).cloned() {
                                    let t = t_arc.lock().await;
                                    matches!(
                                        t.next_retry_at,
                                        Some(until) if chrono::Utc::now().timestamp_millis() < until
                                    )
                                } else {
                                    false
                                }
                            };
                            if cooling {
                                waiting_queue.write().await.push_back(id);
                                // 冷却任务放回队尾，跳出此轮触发
                                break;
                            }

                            // 获取任务
                            let task = tasks.read().await.get(&id).cloned();
                            if let Some(task) = task {
                                // 🔥 获取任务信息：是否需要槽位、是否为备份任务、是否为文件夹子任务、group_id
                                // 文件夹固定槽位子任务（uses_folder_fixed_slot=true）不需要再申请槽位
                                let (needs_slot, is_backup, is_folder_subtask, zero_delay_group_id) = {
                                    let t = task.lock().await;
                                    (
                                        t.slot_id.is_none() && !t.uses_folder_fixed_slot,
                                        t.is_backup,
                                        t.group_id.is_some(),
                                        t.group_id.clone(),
                                    )
                                };
                                // 🔥 注意：touch_slot 的 owner 选择必须等到"槽位类型已确定"之后
                                //   - 文件夹 fixed/borrowed 槽位：pool owner = group_id（folder_id）
                                //   - 普通全局 fixed 槽位 / backup 槽位：pool owner = task_id（id）
                                // 不要提前据 group_id 一票选 owner，否则文件夹子任务 fallback 到普通全局 fixed slot 时会刷新错对象。

                                // 🔥 预检查：只能走全局槽位的任务若 pool 已满，放回队尾跳过
                                //    文件夹子任务（非备份）可以尝试 folder_manager 固定位路径
                                let available_slots = task_slot_pool.available_slots().await;
                                let can_try_folder_fixed_slot =
                                    needs_slot && is_folder_subtask && !is_backup;
                                if needs_slot && !can_try_folder_fixed_slot && available_slots == 0 {
                                    if skipped_no_global_slot.contains(&id) {
                                        waiting_queue.write().await.push_back(id);
                                        break;
                                    }
                                    skipped_no_global_slot.insert(id.clone());
                                    waiting_queue.write().await.push_back(id);
                                    continue;
                                }

                                info!(
                                    "⚡ 0延迟启动：从等待队列启动任务 {} (全局空槽: {}, is_backup: {}, is_folder_subtask: {})",
                                    id, available_slots, is_backup, is_folder_subtask
                                );

                                if needs_slot {
                                    // 🔥 文件夹子任务优先尝试同文件夹的固定槽位
                                    let mut used_folder_fixed_slot = false;
                                    if can_try_folder_fixed_slot {
                                        if let Some(ref folder_id) = zero_delay_group_id {
                                            let folder_mgr =
                                                folder_manager_arc_for_trigger.read().await.clone();
                                            if let Some(fm) = folder_mgr {
                                                if fm
                                                    .try_allocate_fixed_slot_for_subtask(
                                                        folder_id, &id,
                                                    )
                                                    .await
                                                {
                                                    let mut t = task.lock().await;
                                                    t.slot_id = None;
                                                    t.is_borrowed_slot = false;
                                                    t.uses_folder_fixed_slot = true;
                                                    drop(t);
                                                    used_folder_fixed_slot = true;
                                                    // 🔥 文件夹 fixed slot 分配即 touch（保持与普通槽位分配后 touch 一致）
                                                    task_slot_pool.touch_slot(folder_id).await;
                                                    info!(
                                                        "0延迟启动：文件夹子任务 {} 占用文件夹 {} 的固定槽位，已刷新槽位时间戳",
                                                        id, folder_id
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    if !used_folder_fixed_slot {
                                        // 🔥 根据任务类型选择优先级
                                        let priority = if is_backup {
                                            TaskPriority::Backup
                                        } else if is_folder_subtask {
                                            TaskPriority::SubTask
                                        } else {
                                            TaskPriority::Normal
                                        };

                                        // 🔥 备份任务使用 allocate_backup_slot，其他任务使用带优先级的分配
                                        let slot_result = if is_backup {
                                            task_slot_pool.allocate_backup_slot(&id).await.map(|sid| (sid, None))
                                        } else {
                                            task_slot_pool.allocate_fixed_slot_with_priority(&id, false, priority).await
                                        };

                                        if let Some((sid, preempted_task_id)) = slot_result {
                                            // 分配成功，更新任务槽位信息
                                            let mut t = task.lock().await;
                                            t.slot_id = Some(sid);
                                            t.is_borrowed_slot = false;
                                            info!("0延迟启动：为任务 {} 分配槽位: {} (priority: {:?})", id, sid, priority);
                                            drop(t); // 释放锁

                                            // 🔥 backup_slot / 普通全局 fixed slot 在 pool 中 owner=task_id，按 id touch
                                            //    （文件夹 fixed slot 走另一分支，已用 folder_id touch）
                                            task_slot_pool.touch_slot(&id).await;

                                            // 🔥 处理被抢占的备份任务
                                            if let Some(preempted_id) = preempted_task_id {
                                                info!("0延迟启动：任务 {} 抢占了备份任务 {} 的槽位", id, preempted_id);
                                                // 暂停被抢占的任务并加入等待队列
                                                Self::pause_and_requeue_preempted_task(
                                                    &tasks, &cancellation_tokens, &waiting_queue, &preempted_id
                                                ).await;
                                            }
                                        } else {
                                            // 分配失败，使用优先级方法放回队列
                                            warn!("0延迟启动：无法为任务 {} 分配槽位，放回等待队列", id);
                                            Self::add_to_queue_by_priority(&waiting_queue, &tasks, &id, is_backup, is_folder_subtask).await;
                                            break;
                                        }
                                    }
                                }

                                // 创建取消令牌
                                let cancellation_token = CancellationToken::new();
                                cancellation_tokens
                                    .write()
                                    .await
                                    .insert(id.clone(), cancellation_token.clone());

                                // 启动任务
                                let engine_clone = engine.clone();
                                let task_clone = task.clone();
                                let chunk_scheduler_clone = chunk_scheduler.clone();
                                let id_clone = id.clone();
                                let cancellation_tokens_clone = cancellation_tokens.clone();
                                let persistence_manager_clone = persistence_manager.clone();
                                let ws_manager_arc_clone = ws_manager_arc.clone();
                                let folder_progress_tx_arc_clone = folder_progress_tx_arc.clone();
                                let backup_notification_tx_arc_clone = backup_notification_tx_arc.clone();
                                let task_slot_pool_clone = task_slot_pool.clone();
                                let snapshot_manager_arc_clone = snapshot_manager_arc.clone(); // 🔥 用于查询加密文件映射
                                let encryption_config_store_arc_clone = encryption_config_store_arc.clone(); // 🔥 用于根据 key_version 选择解密密钥
                                let tasks_clone = tasks.clone(); // 🔥 用于任务完成时立即清理
                                let waiting_queue_clone = waiting_queue.clone(); // 🔥 用于备份任务失败重试
                                // 🔥 auto_requeue 发送端和文件夹管理器引用
                                let requeue_tx_cloned_trigger = requeue_tx_for_trigger.clone();
                                let folder_manager_arc_clone_trig = folder_manager_arc_for_trigger.clone();

                                tokio::spawn(async move {
                                    // 获取 WebSocket 管理器和文件夹进度发送器
                                    let ws_manager = ws_manager_arc_clone.read().await.clone();
                                    let folder_progress_tx =
                                        folder_progress_tx_arc_clone.read().await.clone();
                                    let backup_notification_tx =
                                        backup_notification_tx_arc_clone.read().await.clone();
                                    let snapshot_manager = snapshot_manager_arc_clone.read().await.clone(); // 🔥 获取快照管理器
                                    let encryption_config_store = encryption_config_store_arc_clone.read().await.clone(); // 🔥 获取加密配置存储
                                    // 🔥 文件夹管理器（构造 TaskScheduleInfo 时使用）
                                    let folder_manager_for_task = folder_manager_arc_clone_trig.read().await.clone();

                                    let prepare_result = engine_clone
                                        .prepare_for_scheduling(
                                            task_clone.clone(),
                                            cancellation_token.clone(),
                                        )
                                        .await;

                                    if cancellation_token.is_cancelled() {
                                        info!("0延迟启动: 任务 {} 在探测完成后发现已被取消", id_clone);
                                        return;
                                    }

                                    match prepare_result {
                                        Ok((
                                               client,
                                               cookie,
                                               referer,
                                               url_health,
                                               output_path,
                                               chunk_size,
                                               chunk_manager,
                                               speed_calc,
                                           )) => {
                                            // 获取文件总大小、远程路径和 fs_id
                                            // 🔥 同时读取 is_borrowed_slot / uses_folder_fixed_slot，用于下方 prepare 后 touch 的 owner 判定
                                            let (
                                                total_size,
                                                remote_path,
                                                fs_id,
                                                local_path,
                                                group_id,
                                                group_root,
                                                relative_path,
                                                is_backup,
                                                backup_config_id,
                                                transfer_task_id,
                                                is_borrowed_slot,
                                                uses_folder_fixed_slot,
                                                task_owner_uid,
                                            ) = {
                                                let t = task_clone.lock().await;
                                                (
                                                    t.total_size,
                                                    t.remote_path.clone(),
                                                    t.fs_id,
                                                    t.local_path.clone(),
                                                    t.group_id.clone(),
                                                    t.group_root.clone(),
                                                    t.relative_path.clone(),
                                                    t.is_backup,
                                                    t.backup_config_id.clone(),
                                                    t.transfer_task_id.clone(),
                                                    t.is_borrowed_slot,
                                                    t.uses_folder_fixed_slot,
                                                    //
                                                    t.owner_uid,
                                                )
                                            };

                                            // 获取分片数
                                            let total_chunks = {
                                                let cm = chunk_manager.lock().await;
                                                cm.chunk_count()
                                            };

                                            // 🔥 prepare_for_scheduling 完成后立即刷新槽位
                                            //   touch owner 必须与"任务实际持有的槽位类型"对应：
                                            //   - 文件夹 fixed/borrowed 槽位 → pool owner = group_id
                                            //   - 普通全局 fixed / backup 槽位 → pool owner = task_id
                                            {
                                                let prepare_touch_id = if (uses_folder_fixed_slot
                                                    || is_borrowed_slot)
                                                    && group_id.is_some()
                                                {
                                                    group_id.clone().unwrap()
                                                } else {
                                                    id_clone.clone()
                                                };
                                                task_slot_pool_clone.touch_slot(&prepare_touch_id).await;
                                            }

                                            // 🔥 发送状态变更事件：pending → downloading
                                            // 此时 prepare_for_scheduling 已完成，任务状态已变为 Downloading
                                            if is_backup {
                                                // 备份任务：发送到 backup_notification_tx
                                                use crate::autobackup::events::TransferTaskType;
                                                if let Some(ref tx) = backup_notification_tx {
                                                    let notification = BackupTransferNotification::StatusChanged {
                                                        task_id: id_clone.clone(),
                                                        task_type: TransferTaskType::Download,
                                                        old_status: crate::autobackup::events::TransferTaskStatus::Pending,
                                                        new_status: crate::autobackup::events::TransferTaskStatus::Transferring,
                                                    };
                                                    let _ = tx.send(notification);
                                                }
                                            } else if let Some(ref ws) = ws_manager {
                                                // 普通任务：发送到 WebSocket
                                                ws.send_if_subscribed(
                                                    TaskEvent::Download(DownloadEvent::StatusChanged {
                                                        task_id: id_clone.clone(),
                                                        old_status: "pending".to_string(),
                                                        new_status: "downloading".to_string(),
                                                        group_id: group_id.clone(),
                                                        is_backup,
                                                        error: None,

                                                        owner_uid: Some(task_owner_uid.raw()),
                                                    }),
                                                    group_id.clone(),
                                                );
                                            }

                                            // 🔥 检测是否为加密文件，并获取 key_version
                                            let (is_encrypted, encryption_key_version) = {
                                                let filename = local_path
                                                    .file_name()
                                                    .and_then(|n| n.to_str())
                                                    .unwrap_or("");

                                                // 通过文件名检测是否为加密文件
                                                let is_encrypted = DownloadTask::detect_encrypted_filename(filename);

                                                // 如果是加密文件，尝试从 snapshot_manager 获取 key_version
                                                let key_version = if is_encrypted {
                                                    if let Some(ref snapshot_mgr) = snapshot_manager {
                                                        match snapshot_mgr.find_by_encrypted_name(filename) {
                                                            Ok(Some(snapshot_info)) => {
                                                                debug!(
                                                                    "0延迟任务 {} 从映射表获取 key_version: {}",
                                                                    id_clone, snapshot_info.key_version
                                                                );
                                                                Some(snapshot_info.key_version)
                                                            }
                                                            Ok(None) => {
                                                                debug!("0延迟任务 {} 在映射表中未找到加密信息", id_clone);
                                                                None
                                                            }
                                                            Err(e) => {
                                                                warn!("0延迟任务 {} 查询映射表失败: {}", id_clone, e);
                                                                None
                                                            }
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                } else {
                                                    None
                                                };

                                                (if is_encrypted { Some(true) } else { None }, key_version)
                                            };

                                            // 🔥 注册任务到持久化管理器
                                            // 显式传 task.owner_uid.raw()
                                            if let Some(ref pm) = persistence_manager_clone {
                                                if let Err(e) = pm.lock().await.register_download_task(
                                                    id_clone.clone(),
                                                    fs_id,
                                                    remote_path.clone(),
                                                    local_path.clone(),
                                                    total_size,
                                                    chunk_size,
                                                    total_chunks,
                                                    group_id.clone(),
                                                    group_root.clone(),
                                                    relative_path.clone(),
                                                    is_backup,
                                                    backup_config_id.clone(),
                                                    is_encrypted,
                                                    encryption_key_version,
                                                    transfer_task_id.clone(),
                                                    Some(task_owner_uid.raw()),
                                                ) {
                                                    warn!(
                                                        "0延迟启动：注册任务到持久化管理器失败: {}",
                                                        e
                                                    );
                                                }

                                                // 🔥 修复：从持久化管理器获取已完成的分片，并标记到 ChunkManager（实现真正的断点续传）
                                                if let Some(completed_chunks) = pm.lock().await.get_completed_chunks(&id_clone) {
                                                    let mut cm = chunk_manager.lock().await;
                                                    let mut completed_count = 0;
                                                    for chunk_index in completed_chunks.iter() {
                                                        cm.mark_completed(chunk_index);
                                                        completed_count += 1;
                                                    }
                                                    if completed_count > 0 {
                                                        info!(
                                                            "0延迟任务 {} 恢复了 {} 个已完成分片，将跳过这些分片的下载",
                                                            id_clone, completed_count
                                                        );
                                                    }
                                                }
                                                // 🔥 恢复分片内部分进度（分片内断点续传）
                                                if let Some(partial_progress) = pm.lock().await.get_partial_progress(&id_clone) {
                                                    let mut cm = chunk_manager.lock().await;
                                                    let mut partial_count = 0;
                                                    for (chunk_index, bytes_downloaded) in &partial_progress {
                                                        cm.update_bytes_downloaded(*chunk_index, *bytes_downloaded);
                                                        partial_count += 1;
                                                    }
                                                    if partial_count > 0 {
                                                        info!(
                                                            "0延迟任务 {} 恢复了 {} 个分片的部分进度（分片内断点续传）",
                                                            id_clone, partial_count
                                                        );
                                                    }
                                                }
                                            }

                                            let max_concurrent_chunks =
                                                calculate_task_max_chunks(total_size);
                                            info!(
                                                "0延迟任务 {} 文件大小 {} 字节, 最大并发分片数: {}",
                                                id_clone, total_size, max_concurrent_chunks
                                            );

                                            let url_health_for_detection = url_health.clone();
                                            let client_for_detection = client.read().unwrap().clone();
                                            let cancellation_token_for_detection =
                                                cancellation_token.clone();
                                            let chunk_scheduler_for_detection =
                                                chunk_scheduler_clone.clone();

                                            // 🔥 获取任务的槽位信息
                                            let (slot_id, is_borrowed_slot) = {
                                                let t = task_clone.lock().await;
                                                (t.slot_id, t.is_borrowed_slot)
                                            };

                                            // 🔥 创建任务级共享槽位刷新节流器（所有分片共享，防止分片切换重置计时）
                                            let touch_id = group_id.clone().unwrap_or_else(|| id_clone.clone());
                                            let slot_touch_throttler = Arc::new(crate::task_slot_pool::SlotTouchThrottler::new(
                                                task_slot_pool_clone.clone(), touch_id,
                                            ));

                                            // 🔥 构造 HTTP/2 降级触发器闭包：根据信号增减 engine 内部计数
                                            let engine_for_trigger = engine_clone.clone();
                                            let http11_trigger_arc: crate::downloader::engine::H2DowngradeTrigger =
                                                Arc::new(move |signal| match signal {
                                                    crate::downloader::engine::H2DowngradeSignal::ZeroFailureFrameError => {
                                                        if engine_for_trigger.report_h2_zero_failure() {
                                                            engine_for_trigger.trigger_http11_downgrade();
                                                        }
                                                    }
                                                    crate::downloader::engine::H2DowngradeSignal::DataReceived => {
                                                        engine_for_trigger.reset_h2_zero_failure_counter();
                                                    }
                                                });

                                            let task_info = TaskScheduleInfo {
                                                task_id: id_clone.clone(),
                                                task: task_clone.clone(),
                                                chunk_manager,
                                                speed_calc,
                                                client,
                                                cookie,
                                                referer,
                                                url_health,
                                                output_path,
                                                chunk_size,
                                                total_size,
                                                cancellation_token: cancellation_token.clone(),
                                                active_chunk_count: Arc::new(AtomicUsize::new(0)),
                                                // 🔥 任务级连续分片失败计数器，达阀触发 auto_requeue
                                                consecutive_chunk_failures: Arc::new(AtomicU32::new(0)),
                                                max_concurrent_chunks,
                                                persistence_manager: persistence_manager_clone
                                                    .clone(),
                                                ws_manager: ws_manager.clone(),
                                                progress_throttler: Arc::new(
                                                    ProgressThrottler::default(),
                                                ),
                                                folder_progress_tx: folder_progress_tx.clone(),
                                                backup_notification_tx: backup_notification_tx.clone(),
                                                // 🔥 任务位借调机制字段
                                                slot_id,
                                                is_borrowed_slot,
                                                task_slot_pool: Some(task_slot_pool_clone.clone()),
                                                // 🔥 加密服务（用于下载完成后解密）- 由调度器根据 encryption_config_store 动态创建
                                                encryption_service: None,
                                                // 🔥 快照管理器（用于查询加密文件映射，获取原始文件名）
                                                snapshot_manager: snapshot_manager.clone(),
                                                // 🔥 加密配置存储（用于根据 key_version 选择正确的解密密钥）
                                                encryption_config_store: encryption_config_store.clone(),
                                                // 🔥 Manager 任务列表引用（用于任务完成时立即清理）
                                                manager_tasks: Some(tasks_clone.clone()),
                                                // 🔥 链接级重试次数（从配置读取）
                                                max_retries,
                                                // 🔥 代理故障回退管理器
                                                fallback_mgr: engine_clone.fallback_mgr.clone(),
                                                // 🔥 任务级共享槽位刷新节流器
                                                slot_touch_throttler,
                                                // 🔥 auto_requeue 发送端
                                                requeue_tx: Some(requeue_tx_cloned_trigger.clone()),
                                                // 🔥 HTTP/2 降级触发器
                                                http11_trigger: Some(http11_trigger_arc),
                                                // 🔥 文件夹管理器引用
                                                folder_manager: folder_manager_for_task.clone(),
                                            };

                                            match chunk_scheduler_clone
                                                .register_task(task_info)
                                                .await
                                            {
                                                Ok(()) => {
                                                    info!(
                                                        "0延迟任务 {} 注册成功，启动CDN链接检测",
                                                        id_clone
                                                    );

                                                    // 🔥 成功注册即视为"启动成功"，清零 start_retry_count
                                                    {
                                                        let mut t = task_clone.lock().await;
                                                        if t.start_retry_count > 0 {
                                                            debug!(
                                                                "0延迟启动：任务 {} 启动成功，重置 start_retry_count {} -> 0",
                                                                id_clone, t.start_retry_count
                                                            );
                                                            t.start_retry_count = 0;
                                                        }
                                                    }

                                                    let refresh_coordinator =
                                                        Arc::new(RefreshCoordinator::new(
                                                            RefreshCoordinatorConfig::default(),
                                                        ));

                                                    let _speed_anomaly_handle = DownloadEngine::start_speed_anomaly_detection(
                                                        engine_clone.clone(),
                                                        remote_path.clone(),
                                                        total_size,
                                                        url_health_for_detection.clone(),
                                                        Arc::new(chunk_scheduler_for_detection.clone()),
                                                        client_for_detection.clone(),
                                                        refresh_coordinator.clone(),
                                                        cancellation_token_for_detection.clone(),
                                                        SpeedAnomalyConfig::default(),
                                                    );

                                                    let _stagnation_handle =
                                                        DownloadEngine::start_stagnation_detection(
                                                            engine_clone.clone(),
                                                            remote_path,
                                                            total_size,
                                                            url_health_for_detection,
                                                            client_for_detection,
                                                            Arc::new(chunk_scheduler_for_detection),
                                                            refresh_coordinator,
                                                            cancellation_token_for_detection,
                                                            StagnationConfig::default(),
                                                        );

                                                    info!(
                                                        "📈 0延迟任务 {} CDN链接检测已启动",
                                                        id_clone
                                                    );
                                                }
                                                Err(e) => {
                                                    error!("0延迟启动：注册任务失败: {}", e);
                                                    // 🔥 typed rollback：按 3 种持有方式释放槽位
                                                    let (
                                                        slot_id,
                                                        is_borrowed_slot,
                                                        uses_folder_fixed_slot,
                                                        is_backup,
                                                        is_folder_subtask,
                                                        retry_count,
                                                        group_id_for_release,
                                                    ) = {
                                                        let t = task_clone.lock().await;
                                                        (
                                                            t.slot_id,
                                                            t.is_borrowed_slot,
                                                            t.uses_folder_fixed_slot,
                                                            t.is_backup,
                                                            t.group_id.is_some(),
                                                            t.start_retry_count,
                                                            t.group_id.clone(),
                                                        )
                                                    };
                                                    Self::release_task_slot_by_kind_static(
                                                        &id_clone,
                                                        group_id_for_release.as_deref(),
                                                        slot_id,
                                                        is_borrowed_slot,
                                                        uses_folder_fixed_slot,
                                                        &task_slot_pool_clone,
                                                        &folder_manager_arc_clone_trig,
                                                    ).await;

                                                    // 🔥 最大重试次数限制（与公共常量保持一致）
                                                    // 🔥 备份任务或文件夹子任务：检查重试次数后决定是否重试
                                                    if (is_backup || is_folder_subtask) && retry_count < MAX_START_RETRIES {
                                                        warn!(
                                                            "0延迟启动：任务 {} 注册失败（{}），放回等待队列等待重试 (重试 {}/{})",
                                                            id_clone, e, retry_count + 1, MAX_START_RETRIES
                                                        );
                                                        {
                                                            let mut t = task_clone.lock().await;
                                                            t.status = TaskStatus::Pending;
                                                            t.slot_id = None;
                                                            t.is_borrowed_slot = false;
                                                            t.uses_folder_fixed_slot = false;
                                                            t.error = Some(e.to_string());
                                                            t.start_retry_count += 1;
                                                        }
                                                        waiting_queue_clone.write().await.push_back(id_clone.clone());
                                                    } else {
                                                        if retry_count >= MAX_START_RETRIES {
                                                            error!(
                                                                "0延迟启动：任务 {} 重试次数已达上限 ({})，标记为失败",
                                                                id_clone, MAX_START_RETRIES
                                                            );
                                                        }
                                                        let mut t = task_clone.lock().await;
                                                        t.mark_failed(e.to_string());
                                                        t.slot_id = None;
                                                        t.is_borrowed_slot = false;
                                                        t.uses_folder_fixed_slot = false;
                                                        // 🔥 通知文件夹管理器子任务失败
                                                        let group_id = t.group_id.clone();
                                                        let total_size = t.total_size;
                                                        drop(t);
                                                        if let Some(gid) = group_id {
                                                            chunk_scheduler_clone.notify_subtask_failed(gid, id_clone.clone(), total_size).await;
                                                        }
                                                    }
                                                    cancellation_tokens_clone
                                                        .write()
                                                        .await
                                                        .remove(&id_clone);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("0延迟启动：准备任务失败: {}", e);
                                            // 🔥 typed rollback：按 3 种持有方式释放槽位
                                            let (
                                                slot_id,
                                                is_borrowed_slot,
                                                uses_folder_fixed_slot,
                                                is_backup,
                                                is_folder_subtask,
                                                retry_count,
                                                group_id_for_release,
                                            ) = {
                                                let t = task_clone.lock().await;
                                                (
                                                    t.slot_id,
                                                    t.is_borrowed_slot,
                                                    t.uses_folder_fixed_slot,
                                                    t.is_backup,
                                                    t.group_id.is_some(),
                                                    t.start_retry_count,
                                                    t.group_id.clone(),
                                                )
                                            };
                                            Self::release_task_slot_by_kind_static(
                                                &id_clone,
                                                group_id_for_release.as_deref(),
                                                slot_id,
                                                is_borrowed_slot,
                                                uses_folder_fixed_slot,
                                                &task_slot_pool_clone,
                                                &folder_manager_arc_clone_trig,
                                            ).await;

                                            // 🔥 最大重试次数限制（与公共常量保持一致）
                                            // 🔥 备份任务或文件夹子任务：检查重试次数后决定是否重试
                                            if (is_backup || is_folder_subtask) && retry_count < MAX_START_RETRIES {
                                                warn!(
                                                    "0延迟启动：任务 {} 准备失败（{}），放回等待队列等待重试 (重试 {}/{}, is_backup={}, is_folder_subtask={})",
                                                    id_clone, e, retry_count + 1, MAX_START_RETRIES, is_backup, is_folder_subtask
                                                );
                                                {
                                                    let mut t = task_clone.lock().await;
                                                    t.status = TaskStatus::Pending;
                                                    t.slot_id = None;
                                                    t.is_borrowed_slot = false;
                                                    t.uses_folder_fixed_slot = false;
                                                    t.error = Some(e.to_string());
                                                    t.start_retry_count += 1;
                                                }
                                                // 放回等待队列末尾
                                                waiting_queue_clone.write().await.push_back(id_clone.clone());
                                            } else {
                                                // 普通单文件任务或重试次数已达上限：标记失败
                                                if retry_count >= MAX_START_RETRIES {
                                                    error!(
                                                        "0延迟启动：任务 {} 重试次数已达上限 ({})，标记为失败",
                                                        id_clone, MAX_START_RETRIES
                                                    );
                                                }
                                                let mut t = task_clone.lock().await;
                                                t.mark_failed(e.to_string());
                                                t.slot_id = None;
                                                t.is_borrowed_slot = false;
                                                t.uses_folder_fixed_slot = false;
                                                // 🔥 通知文件夹管理器子任务失败
                                                let group_id = t.group_id.clone();
                                                let total_size = t.total_size;
                                                drop(t);
                                                if let Some(gid) = group_id {
                                                    chunk_scheduler_clone.notify_subtask_failed(gid, id_clone.clone(), total_size).await;
                                                }
                                            }
                                            cancellation_tokens_clone
                                                .write()
                                                .await
                                                .remove(&id_clone);
                                        }
                                    }
                                });
                            } else {
                                // 任务不存在，跳过
                                warn!("0延迟启动：任务 {} 不存在，跳过", id);
                            }
                        }
                        None => {
                            // 队列为空
                            break;
                        }
                    }
                }
            }
        });
    }

    /// 暂停下载任务
    /// 暂停下载任务
    ///
    /// # 参数
    /// - `task_id`: 任务ID
    /// - `skip_try_start_waiting`: 是否跳过尝试启动等待队列
    ///   - `false`: 正常暂停，会尝试启动等待队列中的任务（默认行为）
    ///   - `true`: 回收借调槽位场景，不触发等待队列启动（槽位留给新任务）
    pub async fn pause_task(&self, task_id: &str, skip_try_start_waiting: bool) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        let mut t = task.lock().await;
        let group_id = t.group_id.clone();
        let is_backup = t.is_backup;
        // 从 task 取真实 owner_uid（共享 manager 下 self.owner_uid 不可靠）
        let task_owner_uid_raw = t.owner_uid.raw();

        if t.status != TaskStatus::Downloading {
            anyhow::bail!("任务未在下载中");
        }

        // 🔥 保存旧状态用于发布 StatusChanged
        let old_status = format!("{:?}", t.status).to_lowercase();

        // 🔥 获取槽位信息，用于释放槽位（覆盖三种持有方式：
        //    普通固定位 / 文件夹借调位 / 文件夹固定位直持有）
        let slot_id = t.slot_id;
        let is_borrowed = t.is_borrowed_slot;
        let uses_folder_fixed_slot = t.uses_folder_fixed_slot;

        t.mark_paused();

        // 🔥 清除任务的槽位字段（与 release_task_slot_by_kind 的契约一致：
        //    folder_manager / task_slot_pool 端的释放由后面 release_task_slot_by_kind 负责，
        //    task 内的字段必须由调用方自行重置）
        t.slot_id = None;
        t.is_borrowed_slot = false;
        t.uses_folder_fixed_slot = false;

        info!("暂停下载任务: {}", task_id);
        drop(t);

        // 🔥 活跃计数 -1（Downloading → Paused）
        self.dec_active();

        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        // 🔥 统一按持有方式释放槽位（替代旧的"slot_id+is_borrowed 二分支"逻辑），
        //    与 auto_requeue_task / handle_task_failure 等终态/重排路径保持一致，
        //    避免漏掉 uses_folder_fixed_slot=true 或 is_borrowed_slot=true 的文件夹子任务。
        self.release_task_slot_by_kind(
            task_id,
            group_id.as_deref(),
            slot_id,
            is_borrowed,
            uses_folder_fixed_slot,
        )
            .await;

        // 🔥 问题2修复：先持久化状态，再发送事件
        // 确保前端收到消息时，状态已经保存到磁盘（与 pause_folder 保持一致）
        if let Some(ref pm) = self.persistence_manager {
            use crate::persistence::types::TaskPersistenceStatus;
            if let Err(e) = crate::persistence::metadata::update_metadata(
                pm.lock().await.wal_dir(),
                task_id,
                |m| {
                    m.set_status(TaskPersistenceStatus::Paused);
                },
            ) {
                warn!("持久化暂停状态失败: {}", e);
            } else {
                info!("任务 {} 暂停状态已持久化", task_id);
            }
        }

        // 🔥 发送状态变更事件（问题3修复：在持久化之后发送）
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status: old_status.clone(),
            new_status: "paused".to_string(),
            group_id: group_id.clone(),
            is_backup,
            error: None,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 发送暂停事件
        self.publish_event(DownloadEvent::Paused {
            task_id: task_id.to_string(),
            group_id,
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 如果是备份任务，发送状态变更通知和暂停通知到 AutoBackupManager
        if is_backup {
            use crate::autobackup::events::{TransferTaskType, TransferTaskStatus};
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                // 🔥 问题1修复：发送 StatusChanged 通知（Transferring -> Paused）
                // 前端依赖 StatusChanged 更新状态，与 resume_task 保持一致
                let status_notification = BackupTransferNotification::StatusChanged {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                    old_status: TransferTaskStatus::Transferring,
                    new_status: TransferTaskStatus::Paused,
                };
                if let Err(e) = tx.send(status_notification) {
                    warn!("发送备份任务状态变更通知失败: {}", e);
                } else {
                    info!("已发送备份任务状态变更通知: {} (Transferring -> Paused)", task_id);
                }

                // 发送 Paused 通知
                let notification = BackupTransferNotification::Paused {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                };
                let _ = tx.send(notification);
            }
        }

        // 🔥 根据参数决定是否尝试启动等待队列中的任务
        if !skip_try_start_waiting {
            self.try_start_waiting_tasks().await;
        }

        Ok(())
    }

    /// 🔥 按优先级将任务加入等待队列（简化版，仅区分备份/非备份）
    ///
    /// # 参数
    /// - `task_id`: 任务ID
    /// - `is_backup`: 是否为备份任务
    async fn add_to_waiting_queue_by_priority(&self, task_id: &str, is_backup: bool) {
        // 委托给完整版方法，非备份任务默认为普通任务（非文件夹子任务）
        self.add_to_waiting_queue_with_task_type(task_id, is_backup, false).await;
    }

    /// 🔥 将被抢占的备份任务加入等待队列末尾
    ///
    /// 供 FolderManager 等外部模块调用
    ///
    /// 完整流程：
    /// 1. 将任务状态从 Paused 改为 Pending
    /// 2. 持久化状态
    /// 3. 发送状态变更事件（Paused -> Pending）
    /// 4. 发送备份通知
    /// 5. 将任务加入等待队列
    pub async fn add_preempted_backup_to_queue(&self, task_id: &str) {
        // 🔥 问题2/3修复：更新状态从 Paused 到 Pending，并发送通知
        // 同时取出真实 owner_uid
        let (group_id, is_backup, task_owner_uid_raw) = {
            let task = match self.tasks.read().await.get(task_id).cloned() {
                Some(t) => t,
                None => {
                    warn!("加入等待队列失败：任务 {} 不存在", task_id);
                    return;
                }
            };
            let mut t = task.lock().await;
            // 只有 Paused 状态的任务才需要转换为 Pending
            if t.status == TaskStatus::Paused {
                t.status = TaskStatus::Pending;
                info!("被抢占的备份任务 {} 状态已从 Paused 改为 Pending", task_id);
            }
            (t.group_id.clone(), t.is_backup, t.owner_uid.raw())
        };

        // 🔥 持久化状态
        if let Some(ref pm) = self.persistence_manager {
            use crate::persistence::types::TaskPersistenceStatus;
            if let Err(e) = crate::persistence::metadata::update_metadata(
                pm.lock().await.wal_dir(),
                task_id,
                |m| {
                    m.set_status(TaskPersistenceStatus::Pending);
                },
            ) {
                warn!("持久化等待状态失败: {}", e);
            }
        }

        // 🔥 发送状态变更事件（Paused -> Pending）
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status: "paused".to_string(),
            new_status: "pending".to_string(),
            group_id: group_id.clone(),
            is_backup,
            error: None,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 如果是备份任务，发送状态变更通知到 AutoBackupManager
        if is_backup {
            use crate::autobackup::events::{TransferTaskType, TransferTaskStatus};
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = BackupTransferNotification::StatusChanged {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                    old_status: TransferTaskStatus::Paused,
                    new_status: TransferTaskStatus::Pending,
                };
                if let Err(e) = tx.send(notification) {
                    warn!("发送备份任务等待状态通知失败: {}", e);
                } else {
                    info!("已发送备份任务等待状态通知: {} (Paused -> Pending)", task_id);
                }
            }
        }

        // 将任务加入等待队列
        self.add_to_waiting_queue_with_task_type(task_id, true, false).await;
        info!("被抢占的备份任务 {} 已加入等待队列末尾", task_id);
    }

    /// 🔥 静态方法：按优先级将任务加入等待队列
    ///
    /// 用于 handle_task_failure 等静态上下文中
    async fn add_to_queue_by_priority(
        waiting_queue: &Arc<RwLock<VecDeque<String>>>,
        tasks: &Arc<RwLock<HashMap<String, Arc<Mutex<DownloadTask>>>>>,
        task_id: &str,
        is_backup: bool,
        is_folder_subtask: bool,
    ) {
        let mut queue = waiting_queue.write().await;

        if is_backup {
            queue.push_back(task_id.to_string());
            info!("备份任务 {} 加入等待队列末尾 (队列长度: {})", task_id, queue.len());
        } else if is_folder_subtask {
            // 文件夹子任务：插入到备份任务之前
            let insert_pos = {
                let tasks_guard = tasks.read().await;
                let mut backup_pos = None;
                for (i, id) in queue.iter().enumerate() {
                    if let Some(task_arc) = tasks_guard.get(id) {
                        if let Ok(t) = task_arc.try_lock() {
                            if t.is_backup {
                                backup_pos = Some(i);
                                break;
                            }
                        }
                    }
                }
                backup_pos
            };

            if let Some(pos) = insert_pos {
                queue.insert(pos, task_id.to_string());
                info!("文件夹子任务 {} 插入到等待队列位置 {} (队列长度: {})", task_id, pos, queue.len());
            } else {
                queue.push_back(task_id.to_string());
                info!("文件夹子任务 {} 加入等待队列末尾 (队列长度: {})", task_id, queue.len());
            }
        } else {
            // 普通任务：插入到文件夹子任务和备份任务之前
            let insert_pos = {
                let tasks_guard = tasks.read().await;
                let mut pos = None;
                for (i, id) in queue.iter().enumerate() {
                    if let Some(task_arc) = tasks_guard.get(id) {
                        if let Ok(t) = task_arc.try_lock() {
                            if t.is_backup || t.group_id.is_some() {
                                pos = Some(i);
                                break;
                            }
                        }
                    }
                }
                pos
            };

            if let Some(pos) = insert_pos {
                queue.insert(pos, task_id.to_string());
                info!("普通任务 {} 插入到等待队列位置 {} (队列长度: {})", task_id, pos, queue.len());
            } else {
                queue.push_back(task_id.to_string());
                info!("普通任务 {} 加入等待队列末尾 (队列长度: {})", task_id, queue.len());
            }
        }
    }

    /// 🔥 静态方法：暂停被抢占的任务并加入等待队列
    ///
    /// 用于后台监控和0延迟启动等静态上下文中处理被抢占的备份任务
    ///
    /// 完整流程：
    /// 1. 将任务状态从 Downloading 改为 Paused，再改为 Pending
    /// 2. 将任务加入等待队列
    ///
    /// 注意：由于是静态方法，无法发送事件通知，调用方需要自行处理通知
    async fn pause_and_requeue_preempted_task(
        tasks: &Arc<RwLock<HashMap<String, Arc<Mutex<DownloadTask>>>>>,
        cancellation_tokens: &Arc<RwLock<HashMap<String, CancellationToken>>>,
        waiting_queue: &Arc<RwLock<VecDeque<String>>>,
        preempted_id: &str,
    ) {
        // 获取被抢占的任务
        let task = tasks.read().await.get(preempted_id).cloned();
        if let Some(task) = task {
            // 更新任务状态：Downloading -> Paused -> Pending
            {
                let mut t = task.lock().await;
                if t.status == TaskStatus::Downloading {
                    // 🔥 问题2/3修复：直接将状态改为 Pending（跳过 Paused 中间状态）
                    // 因为被抢占的任务会立即加入等待队列，应该是 Pending 状态
                    t.status = TaskStatus::Pending;
                    // 清除槽位信息（槽位已被抢占）
                    t.slot_id = None;
                    t.is_borrowed_slot = false;
                    info!("被抢占的备份任务 {} 状态已改为 Pending", preempted_id);
                }
            }

            // 取消任务的取消令牌
            if let Some(token) = cancellation_tokens.write().await.remove(preempted_id) {
                token.cancel();
            }

            // 将被抢占的任务加入等待队列末尾（备份任务优先级最低）
            Self::add_to_queue_by_priority(waiting_queue, tasks, preempted_id, true, false).await;
        }
    }

    /// 🔥 按优先级将任务加入等待队列（完整版，支持三级优先级）
    ///
    /// 等待队列按优先级排序：
    /// - 普通下载任务（is_backup=false, is_folder_subtask=false）：最高优先级
    /// - 文件夹子任务（is_backup=false, is_folder_subtask=true）：中等优先级
    /// - 自动备份任务（is_backup=true）：最低优先级，插入到队列末尾
    ///
    /// # 参数
    /// - `task_id`: 任务ID
    /// - `is_backup`: 是否为备份任务
    /// - `is_folder_subtask`: 是否为文件夹子任务
    async fn add_to_waiting_queue_with_task_type(&self, task_id: &str, is_backup: bool, is_folder_subtask: bool) {
        let mut queue = self.waiting_queue.write().await;

        if is_backup {
            // 备份任务：直接加入队列末尾
            queue.push_back(task_id.to_string());
            info!("备份任务 {} 加入等待队列末尾 (队列长度: {})", task_id, queue.len());
        } else if is_folder_subtask {
            // 文件夹子任务：插入到备份任务之前，但在普通任务之后
            // 找到第一个备份任务或文件夹子任务的位置
            let insert_pos = {
                let tasks = self.tasks.read().await;
                let mut backup_pos = None;
                for (i, id) in queue.iter().enumerate() {
                    if let Some(task_arc) = tasks.get(id) {
                        if let Ok(t) = task_arc.try_lock() {
                            if t.is_backup {
                                backup_pos = Some(i);
                                break;
                            }
                        }
                    }
                }
                backup_pos
            };

            if let Some(pos) = insert_pos {
                // 插入到第一个备份任务之前
                queue.insert(pos, task_id.to_string());
                info!("文件夹子任务 {} 插入到等待队列位置 {} (在备份任务之前, 队列长度: {})", task_id, pos, queue.len());
            } else {
                // 没有备份任务，加入队列末尾
                queue.push_back(task_id.to_string());
                info!("文件夹子任务 {} 加入等待队列末尾 (无备份任务, 队列长度: {})", task_id, queue.len());
            }
        } else {
            // 普通任务：插入到所有文件夹子任务和备份任务之前
            // 找到第一个文件夹子任务或备份任务的位置
            let insert_pos = {
                let tasks = self.tasks.read().await;
                let mut pos = None;
                for (i, id) in queue.iter().enumerate() {
                    if let Some(task_arc) = tasks.get(id) {
                        if let Ok(t) = task_arc.try_lock() {
                            // 找到第一个文件夹子任务或备份任务
                            if t.is_backup || t.group_id.is_some() {
                                pos = Some(i);
                                break;
                            }
                        }
                    }
                }
                pos
            };

            if let Some(pos) = insert_pos {
                // 插入到第一个文件夹子任务或备份任务之前
                queue.insert(pos, task_id.to_string());
                info!("普通任务 {} 插入到等待队列位置 {} (在文件夹子任务/备份任务之前, 队列长度: {})", task_id, pos, queue.len());
            } else {
                // 没有文件夹子任务和备份任务，加入队列末尾
                queue.push_back(task_id.to_string());
                info!("普通任务 {} 加入等待队列末尾 (无低优先级任务, 队列长度: {})", task_id, queue.len());
            }
        }
    }

    /// 🔥 从等待队列移除并暂停指定的任务列表
    ///
    /// 用于备份任务暂停时，将等待队列中属于该备份任务的子任务也暂停
    ///
    /// # 参数
    /// - `task_ids`: 要暂停的任务ID列表
    ///
    /// # 返回
    /// - 成功暂停的任务数量
    pub async fn pause_waiting_tasks(&self, task_ids: &[String]) -> usize {
        if task_ids.is_empty() {
            return 0;
        }

        let task_id_set: std::collections::HashSet<&String> = task_ids.iter().collect();
        let mut paused_count = 0;

        // 1. 从等待队列移除
        {
            let mut queue = self.waiting_queue.write().await;
            let original_len = queue.len();
            queue.retain(|id| !task_id_set.contains(id));
            let removed = original_len - queue.len();
            if removed > 0 {
                info!(
                    "从下载等待队列移除了 {} 个任务 (队列剩余: {})",
                    removed, queue.len()
                );
            }
        }

        // 2. 将这些任务标记为暂停状态
        let tasks = self.tasks.read().await;
        for task_id in task_ids {
            if let Some(task_arc) = tasks.get(task_id) {
                let mut task = task_arc.lock().await;
                // 只暂停 Pending 状态的任务（等待队列中的任务应该是 Pending 状态）
                if task.status == TaskStatus::Pending {
                    let old_status = format!("{:?}", task.status).to_lowercase();
                    let group_id = task.group_id.clone();
                    let is_backup = task.is_backup;
                    // 取真实 owner_uid
                    let task_owner_uid_raw = task.owner_uid.raw();
                    task.mark_paused();
                    paused_count += 1;

                    debug!(
                        "等待队列中的下载任务 {} 已暂停 (原状态: {})",
                        task_id, old_status
                    );

                    drop(task);

                    // 发送状态变更事件（publish_event 对备份任务自动跳过）
                    self.publish_event(DownloadEvent::StatusChanged {
                        task_id: task_id.to_string(),
                        old_status,
                        new_status: "paused".to_string(),
                        group_id: group_id.clone(),
                        is_backup,
                        error: None,

                        owner_uid: Some(task_owner_uid_raw),
                    })
                        .await;

                    // 发送暂停事件
                    self.publish_event(DownloadEvent::Paused {
                        task_id: task_id.to_string(),
                        group_id,
                        is_backup,

                        owner_uid: Some(task_owner_uid_raw),
                    })
                        .await;

                    // 🔥 备份任务：补送 BackupTransferNotification
                    //    publish_event 对备份任务直接跳过；这里需要单独通知 AutoBackupManager，
                    //    否则等待队列中的备份子任务被批量暂停后，备份状态机会停留在旧状态。
                    //    与 pause_task / pause_preempted_task 行为对齐：
                    //    StatusChanged(Pending → Paused) + Paused。
                    if is_backup {
                        use crate::autobackup::events::{TransferTaskStatus, TransferTaskType};
                        let tx_guard = self.backup_notification_tx.read().await;
                        if let Some(tx) = tx_guard.as_ref() {
                            let status_notification = BackupTransferNotification::StatusChanged {
                                task_id: task_id.to_string(),
                                task_type: TransferTaskType::Download,
                                old_status: TransferTaskStatus::Pending,
                                new_status: TransferTaskStatus::Paused,
                            };
                            if let Err(e) = tx.send(status_notification) {
                                warn!(
                                    "pause_waiting_tasks: 发送备份任务 StatusChanged 通知失败 (task_id={}): {}",
                                    task_id, e
                                );
                            }

                            let paused_notification = BackupTransferNotification::Paused {
                                task_id: task_id.to_string(),
                                task_type: TransferTaskType::Download,
                            };
                            if let Err(e) = tx.send(paused_notification) {
                                warn!(
                                    "pause_waiting_tasks: 发送备份任务 Paused 通知失败 (task_id={}): {}",
                                    task_id, e
                                );
                            }
                        } else {
                            warn!(
                                "pause_waiting_tasks: 备份任务 {} 暂停但 backup_notification_tx 未设置",
                                task_id
                            );
                        }
                    }
                }
            }
        }

        if paused_count > 0 {
            info!("已暂停 {} 个等待队列中的下载任务", paused_count);
        }

        paused_count
    }

    /// 🔥 检查等待队列中是否有非备份任务（普通任务或文件夹子任务）
    ///
    /// 用于判断备份任务是否应该让位
    /// 包括：
    /// - 普通单文件任务（group_id.is_none()）
    /// - 文件夹子任务（group_id.is_some()）
    async fn has_normal_tasks_waiting(&self) -> bool {
        let queue = self.waiting_queue.read().await;
        let tasks = self.tasks.read().await;

        for id in queue.iter() {
            if let Some(task_arc) = tasks.get(id) {
                if let Ok(t) = task_arc.try_lock() {
                    // 只要不是备份任务，就算有普通任务等待
                    if !t.is_backup {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// 🔥 暂停被抢占的任务（简化版，不触发等待队列启动，避免递归）
    ///
    /// 用于 try_start_waiting_tasks 中抢占备份任务时使用
    /// 与 pause_task 的区别：
    /// - 不调用 try_start_waiting_tasks（避免递归）
    ///
    /// 🔥 修复：现在会发送状态变更通知（Transferring -> Paused）
    async fn pause_preempted_task(&self, task_id: &str) {
        // 获取任务
        let task = match self.tasks.read().await.get(task_id).cloned() {
            Some(t) => t,
            None => {
                warn!("暂停被抢占任务失败：任务 {} 不存在", task_id);
                return;
            }
        };

        // 更新任务状态并获取必要信息
        // 取真实 owner_uid
        let (group_id, is_backup, task_owner_uid_raw) = {
            let mut t = task.lock().await;
            if t.status != TaskStatus::Downloading {
                warn!("暂停被抢占任务失败：任务 {} 不在下载中，当前状态: {:?}", task_id, t.status);
                return;
            }
            let group_id = t.group_id.clone();
            let is_backup = t.is_backup;
            let task_owner_uid_raw = t.owner_uid.raw();
            t.mark_paused();
            // 清除槽位信息（槽位已被抢占）
            t.slot_id = None;
            t.is_borrowed_slot = false;
            (group_id, is_backup, task_owner_uid_raw)
        };

        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        // 🔥 持久化暂停状态
        if let Some(ref pm) = self.persistence_manager {
            use crate::persistence::types::TaskPersistenceStatus;
            if let Err(e) = crate::persistence::metadata::update_metadata(
                pm.lock().await.wal_dir(),
                task_id,
                |m| {
                    m.set_status(TaskPersistenceStatus::Paused);
                },
            ) {
                warn!("持久化被抢占任务暂停状态失败: {}", e);
            }
        }

        // 🔥 发送状态变更事件（Downloading/Transferring -> Paused）
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status: "downloading".to_string(),
            new_status: "paused".to_string(),
            group_id: group_id.clone(),
            is_backup,
            error: None,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 发送暂停事件
        self.publish_event(DownloadEvent::Paused {
            task_id: task_id.to_string(),
            group_id,
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 如果是备份任务，发送状态变更通知到 AutoBackupManager
        if is_backup {
            use crate::autobackup::events::{TransferTaskStatus, TransferTaskType};
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                // 发送 StatusChanged 通知（Transferring -> Paused）
                let status_notification = BackupTransferNotification::StatusChanged {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                    old_status: TransferTaskStatus::Transferring,
                    new_status: TransferTaskStatus::Paused,
                };
                if let Err(e) = tx.send(status_notification) {
                    warn!("发送被抢占备份任务状态变更通知失败: {}", e);
                } else {
                    info!(
                        "已发送被抢占备份任务状态变更通知: {} (Transferring -> Paused)",
                        task_id
                    );
                }

                // 发送 Paused 通知
                let notification = BackupTransferNotification::Paused {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                };
                let _ = tx.send(notification);
            }
        }

        info!("被抢占的备份任务 {} 已暂停", task_id);
    }

    /// 恢复下载任务（支持从 Paused 或 Failed 状态恢复）
    pub async fn resume_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;
        let group_id;
        let old_status;
        let is_backup;
        // 取真实 owner_uid
        let task_owner_uid_raw;

        // 检查任务状态并将 Paused/Failed 改回 Pending

        {
            let mut t = task.lock().await;
            match t.status {
                TaskStatus::Paused => {
                    old_status = "paused".to_string();
                }
                TaskStatus::Failed => {
                    // 🔥 允许从失败状态重试：重置错误信息
                    old_status = "failed".to_string();
                    t.error = None;
                }
                _ => {
                    anyhow::bail!("任务当前状态不支持恢复: {:?}", t.status);
                }
            }

            // 将状态改回 Pending，准备重新启动
            t.status = TaskStatus::Pending;
            // 🔥 用户主动重试：复位收尾标记，允许重试后的下载重新收尾
            t.finalize_spawned = false;
            group_id = t.group_id.clone();
            is_backup = t.is_backup;
            task_owner_uid_raw = t.owner_uid.raw();
        }

        info!("用户请求恢复下载任务: {}", task_id);

        // 🔥 活跃计数 +1（Paused/Failed → Pending）
        self.inc_active();

        // 🔥 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "pending".to_string(),
            group_id: group_id.clone(),
            is_backup,
            error: None,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 发送恢复事件
        self.publish_event(DownloadEvent::Resumed {
            task_id: task_id.to_string(),
            group_id,
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 如果是备份任务，发送恢复通知到 AutoBackupManager
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = BackupTransferNotification::Resumed {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                };
                let _ = tx.send(notification);
            }
        }

        // 🔥 关键修复：恢复任务时，如果无可用槽位，尝试回收文件夹借调槽位
        // 这与 start_task 的逻辑保持一致
        // 注意：uses_folder_fixed_slot=true 的子任务虽然 slot_id=None，但已由 folder_manager
        //       侧分配到文件夹固定槽位，此时不需要再向 task_slot_pool 申请槽位。
        let (has_slot, uses_folder_fixed_slot, is_folder_subtask, resume_group_id) = {
            let t = task.lock().await;
            (
                t.slot_id.is_some(),
                t.uses_folder_fixed_slot,
                t.group_id.is_some(),
                t.group_id.clone(),
            )
        };
        let has_any_slot = has_slot || uses_folder_fixed_slot;
        // 🔥 注意：下方 backup_slot / 普通全局 fixed slot 分配后的 touch 必须按 task_id（pool owner=task_id），
        //    不能再按 resume_group_id.unwrap_or(task_id) 一票选 owner，否则文件夹子任务
        //    fallback 到普通全局 fixed slot 时会刷新错对象。

        // 🔥 已持有文件夹槽位的子任务：resume 入口处立即按 group_id touch 一次
        //    覆盖两种合法持有形态：
        //      a. uses_folder_fixed_slot=true → 文件夹固定槽位（pool owner=group_id）
        //      b. slot_id=Some && is_borrowed_slot=true → 文件夹借调槽位（pool owner=group_id）
        //    与 start_task 的对应分支保持一致：防止 resume 过程（重新 prepare + register）
        //    耗时过长导致槽位超时监控误回收 group 的槽位。
        if has_any_slot && is_folder_subtask {
            if let Some(ref folder_id) = resume_group_id {
                self.task_slot_pool.touch_slot(folder_id).await;
            }
        }

        // 如果任务既没有普通/借调槽位，也没有占用文件夹固定槽位，才尝试分配或回收
        if !has_any_slot {
            // 🔥 根据任务类型选择不同的槽位分配策略
            if is_backup {
                // 备份任务：只能使用空闲槽位，不能抢占
                let slot_id = self.task_slot_pool.allocate_backup_slot(task_id).await;

                if let Some(slot_id) = slot_id {
                    {
                        let mut t = task.lock().await;
                        t.slot_id = Some(slot_id);
                        t.is_borrowed_slot = false;
                    }
                    // 🔥 backup slot owner=task_id，按 task_id touch
                    self.task_slot_pool.touch_slot(task_id).await;
                    info!("恢复备份任务 {} 获得任务位: slot_id={}，已刷新槽位时间戳", task_id, slot_id);
                } else {
                    // 备份任务无可用槽位，加入等待队列末尾
                    self.add_to_waiting_queue_with_task_type(task_id, true, false).await;
                    info!("恢复备份任务 {} 无可用槽位，加入等待队列末尾", task_id);
                    return Ok(());
                }
            } else {
                // 🔥 非备份任务：根据是否为文件夹子任务选择优先级
                let priority = if is_folder_subtask {
                    TaskPriority::SubTask
                } else {
                    TaskPriority::Normal
                };
                let task_type_str = if is_folder_subtask { "文件夹子任务" } else { "普通任务" };

                let result = self.task_slot_pool.allocate_fixed_slot_with_priority(
                    task_id, false, priority
                ).await;

                match result {
                    Some((slot_id, preempted_task_id)) => {
                        {
                            let mut t = task.lock().await;
                            t.slot_id = Some(slot_id);
                            t.is_borrowed_slot = false;
                        }

                        // 🔥 普通全局 fixed slot owner=task_id，按 task_id touch
                        self.task_slot_pool.touch_slot(task_id).await;

                        // 处理被抢占的备份任务
                        if let Some(preempted_id) = preempted_task_id {
                            info!("恢复{} {} 抢占了备份任务 {} 的槽位: slot_id={}，已刷新槽位时间戳", task_type_str, task_id, preempted_id, slot_id);
                            self.pause_preempted_task(&preempted_id).await;
                            // 🔥 将被暂停的备份任务加入等待队列末尾（包含状态转换和通知）
                            self.add_preempted_backup_to_queue(&preempted_id).await;
                        } else {
                            info!("恢复{} {} 获得任务位: slot_id={}，已刷新槽位时间戳", task_type_str, task_id, slot_id);
                        }
                    }
                    None => {
                        // 🔥 无可用任务位，先尝试回收文件夹的借调槽位
                        let folder_manager = {
                            let fm = self.folder_manager.read().await;
                            fm.clone()
                        };

                        if let Some(fm) = folder_manager {
                            // 检查是否有借调槽位可回收
                            if self.task_slot_pool.find_folder_with_borrowed_slots().await.is_some() {
                                info!("恢复{} {} 无可用槽位，尝试回收文件夹借调槽位", task_type_str, task_id);

                                // 尝试回收一个借调槽位（按当前 manager 的 owner_uid 过滤）
                                if let Some(reclaimed_slot_id) = fm.reclaim_borrowed_slot_for_owner(self.owner_uid()).await {
                                    // 回收成功，分配槽位给恢复的任务（使用正确的优先级）
                                    if let Some((slot_id, preempted_task_id)) = self.task_slot_pool.allocate_fixed_slot_with_priority(
                                        task_id, false, priority
                                    ).await {
                                        {
                                            let mut t = task.lock().await;
                                            t.slot_id = Some(slot_id);
                                            t.is_borrowed_slot = false;
                                        }
                                        // 🔥 普通全局 fixed slot owner=task_id，按 task_id touch
                                        self.task_slot_pool.touch_slot(task_id).await;
                                        // 🔥 处理被抢占的备份任务
                                        if let Some(preempted_id) = preempted_task_id {
                                            info!("恢复{} {} 通过回收借调槽位获得任务位并抢占了备份任务 {}: slot_id={} (回收的槽位={})，已刷新槽位时间戳", task_type_str, task_id, preempted_id, slot_id, reclaimed_slot_id);
                                            self.pause_preempted_task(&preempted_id).await;
                                            // 🔥 将被暂停的备份任务加入等待队列末尾（包含状态转换和通知）
                                            self.add_preempted_backup_to_queue(&preempted_id).await;
                                        } else {
                                            info!("恢复{} {} 通过回收借调槽位获得任务位: slot_id={} (回收的槽位={})，已刷新槽位时间戳", task_type_str, task_id, slot_id, reclaimed_slot_id);
                                        }
                                    } else {
                                        warn!("回收借调槽位成功但重新分配失败，恢复{} {} 加入等待队列", task_type_str, task_id);
                                        self.add_to_waiting_queue_with_task_type(task_id, false, is_folder_subtask).await;
                                        return Ok(());
                                    }
                                } else {
                                    // 回收失败，加入等待队列
                                    info!("回收借调槽位失败，恢复{} {} 加入等待队列", task_type_str, task_id);
                                    self.add_to_waiting_queue_with_task_type(task_id, false, is_folder_subtask).await;
                                    return Ok(());
                                }
                            } else {
                                // 没有借调槽位可回收，加入等待队列
                                self.add_to_waiting_queue_with_task_type(task_id, false, is_folder_subtask).await;
                                info!(
                                    "恢复{} {} 无可用槽位且无借调槽位可回收，加入等待队列",
                                    task_type_str, task_id
                                );
                                return Ok(());
                            }
                        } else {
                            // 无文件夹管理器，加入等待队列
                            self.add_to_waiting_queue_with_task_type(task_id, false, is_folder_subtask).await;
                            info!("恢复{} {} 无可用槽位，加入等待队列", task_type_str, task_id);
                            return Ok(());
                        }
                    }
                }
            }
        }

        // 有槽位，立即启动
        self.start_task_internal(task_id).await
    }

    /// 将暂停的任务重新加入等待队列
    ///
    /// 用于回收借调槽位场景：被暂停的子任务需要重新排队，而不是一直暂停
    ///
    /// # 功能
    /// - 将任务状态从 Paused 改回 Pending
    /// - 智能插入位置：找到同一 group_id 的第一个等待任务，插入到它前面
    /// - 如果没有同组任务，插入到队列前面（优先恢复）
    /// - 发送状态变更事件
    ///
    /// # 参数
    /// - `task_id`: 任务ID
    pub async fn requeue_paused_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        let group_id;
        let old_status;
        let is_backup;
        // 取真实 owner_uid
        let task_owner_uid_raw;

        // 检查任务状态并将 Paused 改回 Pending
        {
            let mut t = task.lock().await;
            if t.status != TaskStatus::Paused {
                anyhow::bail!("任务未暂停，无法重新入队，当前状态: {:?}", t.status);
            }

            // 保存旧状态
            old_status = format!("{:?}", t.status).to_lowercase();

            // 将状态改回 Pending，准备重新启动
            t.status = TaskStatus::Pending;
            // 🔥 用户主动重试：复位收尾标记，允许重试后的下载重新收尾
            t.finalize_spawned = false;
            group_id = t.group_id.clone();
            is_backup = t.is_backup;
            task_owner_uid_raw = t.owner_uid.raw();

            // 🔥 关键修复：清除槽位信息
            // 当任务被暂停并重新入队时，原来的槽位已经被释放（如借调位回收）
            // 必须清除 slot_id，否则 try_start_waiting_tasks 会认为任务已有槽位
            // 导致多个任务同时启动，超过最大并发数限制
            t.slot_id = None;
            t.is_borrowed_slot = false;
        }

        info!("重新入队暂停任务: {} (group: {:?}, is_backup: {}), 已清除槽位信息", task_id, group_id, is_backup);

        // 🔥 活跃计数 +1（Paused → Pending）
        self.inc_active();

        // 🔥 使用优先级方法加入等待队列
        // 备份任务加入队列末尾，非备份任务根据是否为文件夹子任务决定位置
        let is_folder_subtask = group_id.is_some();
        drop(task); // 释放任务锁，避免死锁
        self.add_to_waiting_queue_with_task_type(task_id, is_backup, is_folder_subtask).await;

        // 🔥 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "pending".to_string(),
            group_id: group_id.clone(),
            is_backup,
            error: None,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        Ok(())
    }

    /// 删除下载任务
    /// 取消任务但不删除（仅触发取消令牌，用于文件夹删除时先停止所有任务）
    pub async fn cancel_task_without_delete(&self, task_id: &str) {
        // 从等待队列移除（如果存在）
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| id != task_id);
        }

        // 🔥 立即更新任务状态为 Paused（表示已停止）
        // 这样 folder_manager 就不会等待30秒超时
        let was_active = {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                let mut t = task.lock().await;
                if t.status == TaskStatus::Downloading || t.status == TaskStatus::Pending {
                    t.mark_paused(); // 立即标记为暂停
                    info!("任务 {} 状态已更新为 Paused（取消中）", task_id);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        if was_active {
            self.dec_active();
        }

        // 从调度器取消任务（已注册的任务）
        self.chunk_scheduler.cancel_task(task_id).await;

        // 触发取消令牌（通知正在下载的任务停止）
        {
            let tokens = self.cancellation_tokens.read().await;
            if let Some(token) = tokens.get(task_id) {
                token.cancel();
            }
        }

        info!("任务 {} 已触发取消令牌", task_id);
    }

    pub async fn delete_task(&self, task_id: &str, delete_file: bool) -> Result<()> {
        // 🔥 在删除前获取 group_id、is_backup 和活跃状态（用于事件通知和计数）
        // 同时取真实 owner_uid（共享 manager 下 self.owner_uid 不可靠）
        let (group_id, is_backup, was_active, task_owner_uid_raw) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                let active = matches!(
                    t.status,
                    TaskStatus::Pending | TaskStatus::Downloading | TaskStatus::Decrypting
                );
                (t.group_id.clone(), t.is_backup, active, t.owner_uid.raw())
            } else {
                // 任务不在内存，尝试从持久化管理器读取
                if let Some(ref pm) = self.persistence_manager {
                    let pm_guard = pm.lock().await;
                    if let Some(metadata) = pm_guard.get_history_task(task_id) {
                        // 历史/元数据中的 owner_uid 也优先使用，缺失时退到 self.owner_uid（兼容旧数据）
                        // 注：metadata.owner_uid 是 Option<u64>（持久化层），无需 .raw()
                        let owner_raw = metadata
                            .owner_uid
                            .unwrap_or_else(|| self.owner_uid.raw());
                        (metadata.group_id.clone(), metadata.is_backup, false, owner_raw)
                    } else {
                        (None, false, false, self.owner_uid.raw())
                    }
                } else {
                    (None, false, false, self.owner_uid.raw())
                }
            }
        };

        // 从等待队列移除（如果存在）
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| id != task_id);
        }

        // 从调度器取消任务（已注册的任务）
        self.chunk_scheduler.cancel_task(task_id).await;

        // 先触发取消令牌（通知正在探测的任务停止），再移除
        // 注意：必须先 cancel 再 remove，否则探测中的任务检测不到取消
        {
            let tokens = self.cancellation_tokens.read().await;
            if let Some(token) = tokens.get(task_id) {
                token.cancel();
            }
        }
        self.cancellation_tokens.write().await.remove(task_id);

        // 等待一小段时间让下载任务有机会清理
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 🔥 释放任务槽位（在移除任务前获取槽位信息，覆盖三种持有方式：
        //    普通固定位 / 文件夹借调位 / 文件夹固定位直持有）
        let (slot_id_to_release, is_borrowed, uses_folder_fixed_slot) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                (t.slot_id, t.is_borrowed_slot, t.uses_folder_fixed_slot)
            } else {
                (None, false, false)
            }
        };

        // 🔥 统一按持有方式释放槽位，与 auto_requeue_task / handle_task_failure / pause_task 一致，
        //    避免漏掉文件夹子任务的 fixed/borrowed 映射。
        self.release_task_slot_by_kind(
            task_id,
            group_id.as_deref(),
            slot_id_to_release,
            is_borrowed,
            uses_folder_fixed_slot,
        )
            .await;

        // 读取任务（内存或历史）
        let removed_task = self.tasks.write().await.remove(task_id);
        let mut local_path = None;
        let mut status_completed = None;

        // 🔥 活跃计数 -1（删除活跃任务）
        if was_active && removed_task.is_some() {
            self.dec_active();
        }

        if let Some(task) = removed_task {
            let t = task.lock().await;
            local_path = Some(t.local_path.clone());
            status_completed = Some(t.status == TaskStatus::Completed);
            info!("删除下载任务（内存中）: {}", task_id);
            drop(t);
        } else {
            // 不在内存，尝试从历史/元数据读取，保证删除幂等
            if let Some(ref pm) = self.persistence_manager {
                // 先克隆需要的引用，避免持锁期间持有 dashmap Ref 生命周期
                let (wal_dir, history_task) = {
                    let pm = pm.lock().await;
                    (pm.wal_dir().clone(), pm.get_history_task(task_id))
                };

                // 先查历史数据库
                if let Some(meta) = history_task {
                    local_path = meta.local_path.clone();
                    status_completed = meta
                        .status
                        .map(|s| s == crate::persistence::types::TaskPersistenceStatus::Completed);
                    info!("删除下载任务（历史数据库）: {}", task_id);
                } else {
                    // 再从元数据文件读取
                    if let Some(meta) =
                        crate::persistence::metadata::load_metadata(&wal_dir, task_id)
                    {
                        local_path = meta.local_path.clone();
                        status_completed = meta.status.map(|s| {
                            s == crate::persistence::types::TaskPersistenceStatus::Completed
                        });
                        info!("删除下载任务（元数据文件）: {}", task_id);
                    } else {
                        warn!("删除下载任务时未找到内存/历史记录: {}", task_id);
                    }
                }
            } else {
                warn!("删除下载任务时持久化管理器未初始化: {}", task_id);
            }
        }

        // 决定是否删除本地文件
        // 1. 对于未完成的任务（包括无法确认状态的情况），自动删除临时文件
        // 2. 对于已完成的任务，根据 delete_file 参数决定
        let should_delete = match status_completed {
            Some(true) => delete_file,
            Some(false) => true,
            None => delete_file,
        };

        if let Some(path) = local_path {
            if should_delete && path.exists() {
                tokio::fs::remove_file(&path)
                    .await
                    .context("删除本地文件失败")?;
                info!("已删除本地文件: {:?}", path);
            }

            // 同时清理可能残留的 .downloading 临时文件
            let temp_path = PathBuf::from(format!(
                "{}{}",
                path.display(),
                crate::downloader::engine::DOWNLOADING_EXTENSION
            ));
            if temp_path.exists() {
                if let Err(e) = tokio::fs::remove_file(&temp_path).await {
                    warn!("清理 .downloading 临时文件失败: {:?} - {}", temp_path, e);
                } else {
                    info!("已清理 .downloading 临时文件: {:?}", temp_path);
                }
            }
        }

        // 🔥 清理持久化文件
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.on_task_deleted(task_id) {
                warn!("清理任务持久化文件失败: {}", e);
            }
        }

        // 🔥 发送删除事件（携带 group_id；publish_event 对备份任务自动跳过）
        self.publish_event(DownloadEvent::Deleted {
            task_id: task_id.to_string(),
            group_id,
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 备份任务：补送 BackupTransferNotification::Deleted
        //    publish_event 对备份任务直接跳过，不补发 AutoBackupManager 看不到下载任务已删除，
        //    备份状态机会残留旧 transfer 记录。
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = BackupTransferNotification::Deleted {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                };
                if let Err(e) = tx.send(notification) {
                    warn!(
                        "delete_task: 发送备份任务 Deleted 通知失败 (task_id={}): {}",
                        task_id, e
                    );
                }
            } else {
                warn!(
                    "delete_task: 备份任务 {} 被删除但 backup_notification_tx 未设置",
                    task_id
                );
            }
        }

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

        Ok(())
    }

    /// 批量删除下载任务（用于自动备份取消等场景）
    ///
    /// 与逐个调用 delete_task 相比，此方法：
    /// - 一次性清理 waiting_queue（O(n) 而非 O(n²)）
    /// - 跳过每个任务的 100ms sleep
    /// - 仅在所有任务删除完成后调用一次 try_start_waiting_tasks
    pub async fn batch_delete_tasks(&self, task_ids: &[String], delete_file: bool) -> (usize, usize) {
        if task_ids.is_empty() {
            return (0, 0);
        }

        let id_set: HashSet<&str> = task_ids.iter().map(|s| s.as_str()).collect();

        // 1. 一次性从 waiting_queue 移除所有目标任务
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| !id_set.contains(id.as_str()));
        }

        // 2. 批量取消所有任务的 cancellation token
        {
            let tokens = self.cancellation_tokens.read().await;
            for task_id in task_ids {
                if let Some(token) = tokens.get(task_id.as_str()) {
                    token.cancel();
                }
            }
        }

        // 等待一次让所有任务有机会清理
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 批量移除 cancellation tokens
        {
            let mut tokens = self.cancellation_tokens.write().await;
            for task_id in task_ids {
                tokens.remove(task_id.as_str());
            }
        }

        // 3. 逐个处理任务删除（释放槽位、移除、清理文件）
        let mut success = 0usize;
        let mut failed = 0usize;

        for task_id in task_ids {
            if let Err(e) = self.delete_task_internal(task_id, delete_file).await {
                tracing::debug!("批量删除下载任务失败: task={}, error={}", task_id, e);
                failed += 1;
            } else {
                success += 1;
            }
        }

        // 4. 所有任务删除完成后，仅调用一次 try_start_waiting_tasks
        self.try_start_waiting_tasks().await;

        (success, failed)
    }

    /// 删除单个下载任务的内部实现（不触发 waiting_queue 清理、sleep 和 try_start_waiting_tasks）
    async fn delete_task_internal(&self, task_id: &str, delete_file: bool) -> Result<()> {
        // 获取任务信息
        // 取真实 owner_uid
        let (group_id, is_backup, was_active, task_owner_uid_raw) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                let active = matches!(
                    t.status,
                    TaskStatus::Pending | TaskStatus::Downloading | TaskStatus::Decrypting
                );
                (t.group_id.clone(), t.is_backup, active, t.owner_uid.raw())
            } else {
                (None, false, false, self.owner_uid.raw())
            }
        };

        // 从调度器取消
        self.chunk_scheduler.cancel_task(task_id).await;

        // 🔥 释放槽位（覆盖三种持有方式：普通固定位 / 文件夹借调位 / 文件夹固定位直持有）
        let (slot_id_to_release, is_borrowed, uses_folder_fixed_slot) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                (t.slot_id, t.is_borrowed_slot, t.uses_folder_fixed_slot)
            } else {
                (None, false, false)
            }
        };

        // 🔥 统一按持有方式释放槽位，与 delete_task / auto_requeue_task / pause_task 一致
        self.release_task_slot_by_kind(
            task_id,
            group_id.as_deref(),
            slot_id_to_release,
            is_borrowed,
            uses_folder_fixed_slot,
        )
            .await;

        // 移除任务
        let removed_task = self.tasks.write().await.remove(task_id);
        let mut local_path = None;
        let mut status_completed = None;

        if was_active && removed_task.is_some() {
            self.dec_active();
        }

        if let Some(task) = removed_task {
            let t = task.lock().await;
            local_path = Some(t.local_path.clone());
            status_completed = Some(t.status == TaskStatus::Completed);
            drop(t);
        }

        // 决定是否删除本地文件
        let should_delete = match status_completed {
            Some(true) => delete_file,
            Some(false) => true,
            None => delete_file,
        };

        if let Some(path) = local_path {
            if should_delete && path.exists() {
                let _ = tokio::fs::remove_file(&path).await;
            }

            // 同时清理可能残留的 .downloading 临时文件
            let temp_path = PathBuf::from(format!(
                "{}{}",
                path.display(),
                crate::downloader::engine::DOWNLOADING_EXTENSION
            ));
            if temp_path.exists() {
                let _ = tokio::fs::remove_file(&temp_path).await;
            }
        }

        // 清理持久化文件
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.on_task_deleted(task_id) {
                warn!("清理任务持久化文件失败: {}", e);
            }
        }

        // 发送删除事件（备份任务会被 publish_event 跳过）
        self.publish_event(DownloadEvent::Deleted {
            task_id: task_id.to_string(),
            group_id,
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 备份任务：补送 BackupTransferNotification::Deleted。与 delete_task 路径一致，
        //    避免批量删除走 batch_delete_tasks 后备份状态机看不到下载任务已删除。
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = BackupTransferNotification::Deleted {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                };
                if let Err(e) = tx.send(notification) {
                    warn!(
                        "delete_task_internal: 发送备份任务 Deleted 通知失败 (task_id={}): {}",
                        task_id, e
                    );
                }
            } else {
                warn!(
                    "delete_task_internal: 备份任务 {} 被删除但 backup_notification_tx 未设置",
                    task_id
                );
            }
        }

        Ok(())
    }

    /// 获取任务
    pub async fn get_task(&self, task_id: &str) -> Option<DownloadTask> {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(task_id) {
            Some(task.lock().await.clone())
        } else {
            None
        }
    }

    /// 🔥 解析下载任务的「聚合用」终态：先查内存，内存查不到再查历史库。
    ///
    /// 背景：小文件下载极快（如几 KB），完成后会立即「标记完成 + 归档到历史库 +
    /// 从内存任务表移除」。转存任务的下载状态监听器每 2s 轮询一次，若在这之间任务
    /// 已被归档移除，仅查内存（`get_task` 返回 None）会把「已完成并归档」误判为
    /// 「已取消」，导致转存状态从 Downloading 回退成 Transferred → share-sync 把本
    /// 已成功的子项标记为失败。这里补查历史库，区分「已归档完成/失败」与「真正丢失」。
    pub async fn lookup_aggregate_outcome(&self, task_id: &str) -> DownloadAggregateOutcome {
        if let Some(task) = self.tasks.read().await.get(task_id) {
            return DownloadAggregateOutcome::InMemory(task.lock().await.status.clone());
        }
        if let Some(ref pm) = self.persistence_manager {
            if let Some(meta) = pm.lock().await.get_history_task(task_id) {
                use crate::persistence::types::TaskPersistenceStatus;
                match meta.status {
                    Some(TaskPersistenceStatus::Completed) => {
                        return DownloadAggregateOutcome::ArchivedCompleted
                    }
                    Some(TaskPersistenceStatus::Failed) => {
                        return DownloadAggregateOutcome::ArchivedFailed
                    }
                    _ => {}
                }
            }
        }
        DownloadAggregateOutcome::NotFound
    }

    /// 检查任务是否存在于内存或持久化存储中
    pub async fn has_task_anywhere(&self, task_id: &str) -> bool {
        let tasks = self.tasks.read().await;
        if tasks.contains_key(task_id) {
            return true;
        }
        if let Some(ref pm) = self.persistence_manager {
            let pm_guard = pm.lock().await;
            if pm_guard.get_history_task(task_id).is_some() {
                return true;
            }
        }
        false
    }

    /// 任务是否存在于**内存**中（不查历史库）。
    ///
    /// 用于跨账号路由的"内存优先"判定：内存命中即可确定归属为本 manager 的
    /// `owner_uid`（per-uid 独立 manager）。历史库为全局共享，无法据此判定归属，
    /// 故路由的历史回退路径改用 `metadata.owner_uid`（见
    /// `AppState::find_download_manager_for_task`）。
    pub async fn has_task_in_memory(&self, task_id: &str) -> bool {
        self.tasks.read().await.contains_key(task_id)
    }

    /// 🔥 更新任务的槽位信息
    ///
    /// 用于恢复/补任务路径为子任务分配槽位后更新任务状态。
    ///
    /// 🔥 三种场景自动识别（避免上层调用方感知细节）：
    ///
    /// 1. `is_borrowed=true` → 借调位
    ///    - 写入 `slot_id=Some, is_borrowed_slot=true, uses_folder_fixed_slot=false`
    ///    - 后续释放走 `release_subtask_borrowed_slot`
    ///
    /// 2. `is_borrowed=false` 且 task 属于某文件夹且 `slot_id == folder.fixed_slot_id` → **文件夹固定位直持有**
    ///    - 写入 `slot_id=None, is_borrowed_slot=false, uses_folder_fixed_slot=true`
    ///    - 同步登记 `folder_manager.fixed_slot_subtask = task_id`（幂等）
    ///    - 后续释放走 `release_fixed_slot_from_subtask`（只清 folder 端映射，不释放 pool）
    ///    - ❗ 关键：**不能**写成 `slot_id=Some(fixed_slot_id)`，否则 scheduler 完成路径
    ///      会错误调用 `task_slot_pool.release_fixed_slot(task_id)`，而 pool 里这条 fixed slot
    ///      的 owner 是 `group_id`，既清不掉 slot pool 状态，也留下 `fixed_slot_subtask` 泄漏。
    ///
    /// 3. 其他情况（普通全局固定位） → `slot_id=Some, is_borrowed_slot=false, uses_folder_fixed_slot=false`
    ///    - 后续释放走 `task_slot_pool.release_fixed_slot(task_id)`
    pub async fn update_task_slot(&self, task_id: &str, slot_id: usize, is_borrowed: bool) {
        // 1. 读取 group_id（用于判定是否为文件夹固定位场景）
        let group_id: Option<String> = {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                task.lock().await.group_id.clone()
            } else {
                warn!("update_task_slot: 任务 {} 不存在，忽略", task_id);
                return;
            }
        };

        // 2. 判定是否命中"文件夹 fixed slot 直持有"路径
        //    仅当 is_borrowed=false 且 group 存在且 slot_id == folder.fixed_slot_id 时成立
        let is_folder_fixed_slot = if !is_borrowed {
            if let Some(ref folder_id) = group_id {
                let folder_mgr = self.folder_manager.read().await.clone();
                match folder_mgr {
                    Some(fm) => fm.folder_fixed_slot_id(folder_id).await == Some(slot_id),
                    None => false,
                }
            } else {
                false
            }
        } else {
            false
        };

        // 3. 根据场景写入任务状态
        {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                let mut t = task.lock().await;
                if is_folder_fixed_slot {
                    // 文件夹固定位直持有：slot_id=None + uses_folder_fixed_slot=true
                    t.slot_id = None;
                    t.is_borrowed_slot = false;
                    t.uses_folder_fixed_slot = true;
                    info!(
                        "更新任务 {} 槽位信息: 文件夹固定位直持有 (group={:?}, fixed_slot={})",
                        task_id, group_id, slot_id
                    );
                } else {
                    // 借调位 or 普通全局固定位
                    t.slot_id = Some(slot_id);
                    t.is_borrowed_slot = is_borrowed;
                    t.uses_folder_fixed_slot = false;
                    info!(
                        "更新任务 {} 槽位信息: slot_id={}, is_borrowed={}",
                        task_id, slot_id, is_borrowed
                    );
                }
            }
        }

        // 4. 文件夹固定位直持有路径：同步登记 fixed_slot_subtask
        //    避免 try_allocate_fixed_slot_for_subtask 再次把同一个 fixed slot 分给别的子任务
        if is_folder_fixed_slot {
            if let Some(folder_id) = group_id {
                let folder_mgr = self.folder_manager.read().await.clone();
                if let Some(fm) = folder_mgr {
                    let ok = fm.set_fixed_slot_subtask(&folder_id, task_id).await;
                    if !ok {
                        warn!(
                            "update_task_slot: folder={} 固定位同步登记失败（task={}, slot={}）",
                            folder_id, task_id, slot_id
                        );
                    }
                }
            }
        }
    }

    /// 🔥 将任务设为 Pending 状态并加入等待队列
    ///
    /// 用于文件夹任务恢复时，没有槽位的子任务应该变成等待状态而不是保持暂停状态
    pub async fn set_task_pending_and_queue(&self, task_id: &str) -> Result<()> {
        // 更新任务状态为 Pending，同时获取 group_id 和 is_backup
        //
        // 🔥 把任务从 Paused 翻回 Pending 时，必须再确认一次槽位字段为空，
        // 否则后续 try_start_waiting_tasks 会用
        // `needs_slot = slot_id.is_none() && !uses_folder_fixed_slot` 判定，
        // 若上游忘了清字段（比如 cancel_tasks_by_group 之外的暂停路径漏处理），
        // 这里就会让子任务带着已经失效的旧槽位标记重新进入调度，跳过真正的重新分配。
        // cancel_tasks_by_group 本身已经在源头清过；这里属于 defensive cleanup，
        // 保证从 set_task_pending_and_queue 出去的任务一定是"Pending + 三字段全空"。
        // 取真实 owner_uid
        let (old_status, group_id, is_backup, task_owner_uid_raw) = {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                let mut t = task.lock().await;
                let old = format!("{:?}", t.status).to_lowercase();
                let gid = t.group_id.clone();
                let backup = t.is_backup;
                let owner_raw = t.owner_uid.raw();
                if t.status == TaskStatus::Paused {
                    t.status = TaskStatus::Pending;
                    info!("任务 {} 状态从 Paused 改为 Pending（等待槽位）", task_id);
                }
                // defensive：清空槽位字段（与 cancel_tasks_by_group 行为对齐）
                t.slot_id = None;
                t.is_borrowed_slot = false;
                t.uses_folder_fixed_slot = false;
                // 🔥 复位收尾标记：文件夹恢复的无槽位分支不经过 resume_task，
                //   这里一并复位，避免重试后的下载因标记残留而跳过收尾 spawn
                t.finalize_spawned = false;
                (old, gid, backup, owner_raw)
            } else {
                anyhow::bail!("任务不存在: {}", task_id);
            }
        };

        // 🔥 使用优先级方法加入等待队列
        let is_folder_subtask = group_id.is_some();
        self.add_to_waiting_queue_with_task_type(task_id, is_backup, is_folder_subtask).await;

        let queue_len = self.waiting_queue.read().await.len();
        info!(
            "任务 {} 已加入等待队列（当前队列长度: {}, is_backup: {}, is_folder_subtask: {}）",
            task_id, queue_len, is_backup, is_folder_subtask
        );

        // 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status: old_status.clone(),
            new_status: "pending".to_string(),
            group_id,
            is_backup,
            error: None,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 如果是备份任务，发送状态变更通知到 AutoBackupManager
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                // 将 old_status 字符串转换为 TransferTaskStatus
                let old_transfer_status = match old_status.as_str() {
                    "paused" => crate::autobackup::events::TransferTaskStatus::Paused,
                    "pending" => crate::autobackup::events::TransferTaskStatus::Pending,
                    "downloading" => crate::autobackup::events::TransferTaskStatus::Transferring,
                    _ => crate::autobackup::events::TransferTaskStatus::Paused,
                };
                let notification = BackupTransferNotification::StatusChanged {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Download,
                    old_status: old_transfer_status,
                    new_status: crate::autobackup::events::TransferTaskStatus::Pending,
                };
                if let Err(e) = tx.send(notification) {
                    warn!("发送备份任务等待状态通知失败: {}", e);
                } else {
                    info!("已发送备份任务等待状态通知: {} (Paused -> Pending)", task_id);
                }
            }
        }

        Ok(())
    }

    /// 设置任务的关联转存任务 ID
    ///
    /// 用于将下载任务与转存任务关联，支持跨任务跳转
    pub async fn set_task_transfer_id(
        &self,
        task_id: &str,
        transfer_task_id: String,
    ) -> Result<()> {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(task_id) {
            let mut t = task.lock().await;
            t.set_transfer_task_id(transfer_task_id);
            Ok(())
        } else {
            anyhow::bail!("任务不存在: {}", task_id)
        }
    }

    /// 设置任务为分享直下任务
    ///
    /// 分享直下任务完成后不会被 clear_completed 清除，由转存管理器负责清理
    pub async fn set_task_share_direct_download(
        &self,
        task_id: &str,
        is_share_direct_download: bool,
    ) -> Result<()> {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(task_id) {
            let mut t = task.lock().await;
            t.is_share_direct_download = is_share_direct_download;
            Ok(())
        } else {
            anyhow::bail!("任务不存在: {}", task_id)
        }
    }

    /// 清除指定的分享直下任务（由转存管理器调用）
    ///
    /// 用于转存管理器在清理临时文件后移除已完成的分享直下下载任务
    pub async fn remove_share_direct_download_task(&self, task_id: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get(task_id) {
            let t = task.lock().await;
            if t.is_share_direct_download && t.status == TaskStatus::Completed {
                drop(t);
                tasks.remove(task_id);
                info!("移除分享直下下载任务: {}", task_id);
                Ok(())
            } else {
                anyhow::bail!("任务不是已完成的分享直下任务: {}", task_id)
            }
        } else {
            // 任务不存在，可能已被移除，视为成功
            Ok(())
        }
    }

    /// 获取活跃任务数（O(1)）
    pub fn active_task_count(&self) -> usize {
        self.active_count.load(Ordering::SeqCst)
    }

    /// 活跃计数 +1
    fn inc_active(&self) {
        self.active_count.fetch_add(1, Ordering::SeqCst);
    }

    /// 活跃计数 -1
    fn dec_active(&self) {
        let prev = self.active_count.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            self.active_count.store(0, Ordering::SeqCst);
        }
    }

    /// 获取所有任务（包括当前任务和历史任务，排除备份任务）
    pub async fn get_all_tasks(&self) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        // 获取当前任务（排除备份任务）
        for task in tasks.values() {
            let t = task.lock().await;
            if !t.is_backup {
                result.push(t.clone());
            }
        }

        // 从历史数据库获取历史任务
        if let Some(ref pm) = self.persistence_manager {
            let pm = pm.lock().await;

            // 从数据库查询已完成的下载任务（排除备份任务）
            if let Some((history_tasks, _total)) = pm.get_history_tasks_by_type_and_status(
                "download",
                "completed",
                true,  // exclude_backup
                0,
                500,   // 限制最多500条
            ) {
                for metadata in history_tasks {
                    // 排除已在当前任务中的（避免重复）
                    if !tasks.contains_key(&metadata.task_id) {
                        if let Some(task) = Self::convert_history_to_task(&metadata) {
                            result.push(task);
                        }
                    }
                }
            }
        }

        // 按创建时间倒序排序
        result.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        result
    }

    /// 获取所有备份任务
    pub async fn get_backup_tasks(&self) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        for task in tasks.values() {
            let t = task.lock().await;
            if t.is_backup {
                result.push(t.clone());
            }
        }

        // 按创建时间倒序排序
        result.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        result
    }

    /// 获取指定备份配置的任务
    pub async fn get_tasks_by_backup_config(&self, backup_config_id: &str) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        for task in tasks.values() {
            let t = task.lock().await;
            if t.is_backup && t.backup_config_id.as_deref() == Some(backup_config_id) {
                result.push(t.clone());
            }
        }

        result
    }

    /// 创建备份下载任务
    ///
    /// 备份任务使用最低优先级，会在普通任务之后执行
    ///
    /// # 参数
    /// * `fs_id` - 文件服务器ID
    /// * `remote_path` - 网盘路径
    /// * `local_path` - 本地保存路径
    /// * `total_size` - 文件大小
    /// * `backup_config_id` - 备份配置ID
    /// * `owner_uid` - 任务归属账号 UID（）
    ///
    /// # 返回
    /// 任务ID
    ///
    /// # 多账号说明
    /// 共享 `DownloadManager` 设计下 `self.owner_uid` 不可靠（同一个 Arc 服务多账号）。
    /// AutoBackup 创建子下载任务时必须把 `BackupConfig.owner_uid` / `BackupTask.owner_uid`
    /// 显式传进来，否则衍生任务会落到启动账号或随机 manager owner，影响：
    /// - 列表/过滤（按 owner_uid 过滤会漏掉这些任务）
    /// - 预算（BudgetScheduler 借调 permit 走错账号）
    /// - 事件归属（owner_uid 字段错误）
    /// - 删除账号扫描（被错误账号"持有"的任务被孤立）
    pub async fn create_backup_task(
        &self,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        backup_config_id: String,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        use crate::uploader::conflict_resolver::ConflictResolver;
        use crate::uploader::conflict::{ConflictResolution, DownloadConflictStrategy};

        // 获取默认策略（如果未指定，使用 Overwrite 默认值）
        let strategy = conflict_strategy.unwrap_or(DownloadConflictStrategy::Overwrite);

        // 解决下载冲突
        let resolution = ConflictResolver::resolve_download_conflict(&local_path, strategy)?;

        // 根据解决方案处理
        let final_local_path = match resolution {
            ConflictResolution::Proceed => local_path,
            ConflictResolution::Skip => {
                // 发送跳过事件
                let filename = local_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                info!("跳过备份下载（文件已存在）: {:?}", local_path);

                self.publish_event(DownloadEvent::Skipped {
                    task_id: format!("backup-skipped-{}", uuid::Uuid::new_v4()),
                    filename,
                    reason: "文件已存在".to_string(),

                    // 用调用方传入的 owner_uid，
                    // 不再用共享 manager 的 self.owner_uid（共享 manager 下不可靠）
                    owner_uid: Some(owner_uid.raw()),
                })
                    .await;

                // 返回特殊的 skipped 标记，而不是错误
                return Ok("skipped".to_string());
            }
            ConflictResolution::UseNewPath(new_path) => PathBuf::from(new_path),
        };

        // 确保目标目录存在
        if let Some(parent) = final_local_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).context("创建下载目录失败")?;
            }
        }

        // 创建备份任务
        // task.owner_uid = 调用方传入的 owner_uid
        let task = DownloadTask::new_backup(
            fs_id,
            remote_path.clone(),
            final_local_path.clone(),
            total_size,
            backup_config_id.clone(),
            owner_uid,
        );
        let task_id = task.id.clone();

        info!(
            "创建备份下载任务: id={}, remote={}, local={:?}, size={}, backup_config={}, strategy={:?}, owner_uid={}",
            task_id, remote_path, final_local_path, total_size, backup_config_id, strategy, owner_uid.raw()
        );

        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc);

        // 🔥 活跃计数 +1（新建备份任务为 Pending）
        self.inc_active();

        // 🔥 创建即落盘占位元数据（与非备份 create_task_internal 同款）：
        // 备份下载任务若在 register 之前失败，重启后也能恢复为终态「失败」。
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.persist_download_task_on_create(
                task_id.clone(),
                fs_id,
                remote_path.clone(),
                final_local_path.clone(),
                total_size,
                None,
                None,
                None,
                true,
                Some(backup_config_id.clone()),
                None,
                Some(owner_uid.raw()),
            ) {
                warn!("写入备份下载任务创建占位元数据失败: {}", e);
            }
        }

        // 🔥 发送任务创建事件（备份任务，is_backup=true）
        // 见非备份 create_task_internal 同款注释。
        self.publish_event(DownloadEvent::Created {
            task_id: task_id.clone(),
            fs_id,
            remote_path,
            local_path: final_local_path.to_string_lossy().to_string(),
            total_size,
            group_id: None,
            is_backup: true,
            original_filename: None, // 备份下载任务不需要原始文件名

            //
            owner_uid: Some(owner_uid.raw()),
        })
            .await;

        Ok(task_id)
    }

    /// 将历史元数据转换为下载任务
    fn convert_history_to_task(metadata: &TaskMetadata) -> Option<DownloadTask> {
        // 验证必要字段
        let fs_id = metadata.fs_id?;
        let remote_path = metadata.remote_path.clone()?;
        let local_path = metadata.local_path.clone()?;
        let file_size = metadata.file_size.unwrap_or(0);

        Some(DownloadTask {
            id: metadata.task_id.clone(),
            // 多账号归属：从 metadata 恢复，缺失时为默认 Uid(0)
            owner_uid: metadata.owner_uid.map(crate::auth::Uid::new).unwrap_or_default(),
            fs_id,
            remote_path,
            local_path,
            total_size: file_size,
            downloaded_size: file_size, // 已完成的任务
            status: TaskStatus::Completed,
            speed: 0,
            created_at: metadata.created_at.timestamp(),
            started_at: Some(metadata.created_at.timestamp()),
            completed_at: metadata.completed_at.map(|t| t.timestamp()),
            error: None,
            // 从 metadata 恢复 group 信息
            group_id: metadata.group_id.clone(),
            group_root: metadata.group_root.clone(),
            relative_path: metadata.relative_path.clone(),
            transfer_task_id: metadata.transfer_task_id.clone(),
            // 任务位借调机制字段（历史任务不需要槽位）
            slot_id: None,
            is_borrowed_slot: false,
            uses_folder_fixed_slot: false,
            // 自动备份字段（从 metadata 恢复）
            is_backup: metadata.is_backup,
            backup_config_id: metadata.backup_config_id.clone(),
            start_retry_count: 0,
            // 解密字段（历史任务默认无解密）
            is_encrypted: false,
            decrypt_progress: 0.0,
            decrypted_path: None,
            original_filename: None,
            // 分享直下字段（历史任务默认为 false）
            is_share_direct_download: false,
            // 🔥 退回等待队列冷却字段（历史任务默认为 None）
            next_retry_at: None,
            // 🔥 R20: 解密协程版本号（历史任务从 0 开始，无遗留协程，安全）
            decrypt_epoch: 0,
            // 🔥 R22: 解密原子提交标志（历史任务已是 Completed，无关运行时收尾）
            decrypt_committed: false,
            // 🔥 收尾/解密协程已 spawn 标记（历史任务无遗留协程，从 false 开始）
            finalize_spawned: false,
        })
    }

    /// 获取进行中的任务数量
    pub async fn active_count(&self) -> usize {
        // 使用调度器的计数（更准确）
        self.chunk_scheduler.active_task_count().await
    }

    /// 清除已完成的任务
    pub async fn clear_completed(&self) -> usize {
        let mut tasks = self.tasks.write().await;
        let mut to_remove = Vec::new();

        // 1. 收集内存中的已完成任务（跳过分享直下任务，由转存管理器清理）
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.status == TaskStatus::Completed && !t.is_share_direct_download {
                to_remove.push(id.clone());
            }
        }

        // 2. 从内存中移除
        let memory_count = to_remove.len();
        for id in &to_remove {
            tasks.remove(id);
        }

        // 释放写锁，避免长时间持锁
        drop(tasks);

        // 3. 从历史数据库中清除已完成任务
        let mut history_count = 0;
        if let Some(ref pm) = self.persistence_manager {
            let pm_guard = pm.lock().await;
            let history_db = pm_guard.history_db().cloned();

            // 释放 pm_guard，避免长时间持锁
            drop(pm_guard);

            // 从历史数据库中删除已完成的下载任务
            if let Some(db) = history_db {
                match db.remove_tasks_by_type_and_status("download", "completed") {
                    Ok(count) => {
                        history_count = count;
                    }
                    Err(e) => {
                        warn!("从历史数据库删除已完成下载任务失败: {}", e);
                    }
                }
                // 🔥 同时清除 folder_history表中已完成的文件夹任务
                match db.remove_completed_folders() {
                    Ok(count) => {
                        history_count += count;
                        info!("从历史数据库删除了 {} 个已完成的文件夹任务", count);
                    }
                    Err(e) => {
                        warn!("从历史数据库删除已完成文件夹任务失败: {}", e);
                    }
                }
            }
        }

        // 4. 清除 FolderDownloadManager 内存中已完成的文件夹
        let folder_memory_count = {
            let fm = self.folder_manager.read().await;
            if let Some(ref folder_manager) = *fm {
                folder_manager.clear_completed_folders().await
            } else {
                0
            }
        };

        let total_count = memory_count + history_count + folder_memory_count;
        info!(
            "清除了 {} 个已完成的任务（文件内存: {}, 文件夹内存: {}, 历史: {}）",
            total_count, memory_count, folder_memory_count, history_count
        );
        total_count
    }

    /// 清除失败的任务
    pub async fn clear_failed(&self) -> usize {
        let mut tasks = self.tasks.write().await;
        let mut to_remove = Vec::new();

        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.status == TaskStatus::Failed {
                to_remove.push((id.clone(), t.local_path.clone()));
            }
        }

        let count = to_remove.len();
        for (id, local_path) in to_remove {
            tasks.remove(&id);

            // 删除失败任务的临时文件
            if local_path.exists() {
                if let Err(e) = std::fs::remove_file(&local_path) {
                    warn!("删除失败任务的临时文件失败: {:?}, 错误: {}", local_path, e);
                } else {
                    info!("已删除失败任务的临时文件: {:?}", local_path);
                }
            }
        }

        info!("清除了 {} 个失败的任务", count);
        count
    }

    // ==================== 批量操作方法 ====================

    /// 批量暂停下载任务
    ///
    /// 🔥 修复：全部暂停时，等待队列中的非备份任务也会被暂停，防止暂停后队列任务继续启动
    /// 自动备份任务不受影响，仍保留在等待队列中
    pub async fn batch_pause(&self, task_ids: &[String]) -> Vec<(String, bool, Option<String>)> {
        let mut results = Vec::with_capacity(task_ids.len());

        // 🔥 第一步：暂停等待队列中的非备份 Pending 任务
        let mut pending_paused: Vec<String> = Vec::new();
        for id in task_ids {
            let task = self.tasks.read().await.get(id).cloned();
            if let Some(task) = task {
                let mut t = task.lock().await;
                if t.status == TaskStatus::Pending && !t.is_backup {
                    let group_id = t.group_id.clone();
                    let is_backup = t.is_backup;
                    // 取真实 owner_uid
                    let task_owner_uid_raw = t.owner_uid.raw();
                    t.mark_paused();
                    drop(t);

                    self.dec_active();

                    // 移除取消令牌（如果有）
                    self.cancellation_tokens.write().await.remove(id);

                    // 持久化暂停状态
                    if let Some(ref pm) = self.persistence_manager {
                        use crate::persistence::types::TaskPersistenceStatus;
                        if let Err(e) = crate::persistence::metadata::update_metadata(
                            pm.lock().await.wal_dir(),
                            id,
                            |m| {
                                m.set_status(TaskPersistenceStatus::Paused);
                            },
                        ) {
                            warn!("持久化暂停状态失败: {}", e);
                        }
                    }

                    self.publish_event(DownloadEvent::StatusChanged {
                        task_id: id.to_string(),
                        old_status: "pending".to_string(),
                        new_status: "paused".to_string(),
                        group_id: group_id.clone(),
                        is_backup,
                        error: None,

                        owner_uid: Some(task_owner_uid_raw),
                    })
                        .await;

                    self.publish_event(DownloadEvent::Paused {
                        task_id: id.to_string(),
                        group_id,
                        is_backup,

                        owner_uid: Some(task_owner_uid_raw),
                    })
                        .await;

                    pending_paused.push(id.clone());
                    results.push((id.clone(), true, None));
                }
            }
        }

        // 🔥 从等待队列中批量移除已暂停的任务（一次写锁，O(n)）
        if !pending_paused.is_empty() {
            let paused_set: std::collections::HashSet<&String> = pending_paused.iter().collect();
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| !paused_set.contains(id));
            info!("批量暂停：从等待队列移除 {} 个非备份 Pending 任务", pending_paused.len());
        }

        // 🔥 第二步：暂停活跃任务（Downloading），跳过已处理的 Pending 任务
        let paused_set: std::collections::HashSet<&String> = pending_paused.iter().collect();
        for id in task_ids {
            if paused_set.contains(id) {
                continue;
            }
            match self.pause_task(id, true).await {
                Ok(_) => results.push((id.clone(), true, None)),
                Err(e) => results.push((id.clone(), false, Some(e.to_string()))),
            }
        }

        self.try_start_waiting_tasks().await;

        // 🔥 通知 FolderDownloadManager 更新文件夹级别状态
        // batch_pause 只更新了子任务状态，文件夹状态须同步为 Paused
        // 否则 task_completed_listener 仍会创建新子任务，前端文件夹状态也不会变更
        {
            let affected_folder_ids: std::collections::HashSet<String> = {
                let tasks_guard = self.tasks.read().await;
                let mut ids = std::collections::HashSet::new();
                for id in task_ids {
                    if let Some(task_arc) = tasks_guard.get(id) {
                        let task = task_arc.lock().await;
                        if let Some(ref gid) = task.group_id {
                            ids.insert(gid.clone());
                        }
                    }
                }
                ids
            };
            if !affected_folder_ids.is_empty() {
                let fm_opt = self.folder_manager.read().await.clone();
                if let Some(fm) = fm_opt {
                    for folder_id in &affected_folder_ids {
                        if let Err(e) = fm.pause_folder(folder_id).await {
                            warn!("批量暂停：文件夹 {} 状态更新失败（可能已暂停）: {}", folder_id, e);
                        }
                    }
                }
            }
        }

        results
    }

    /// 批量恢复下载任务
    pub async fn batch_resume(&self, task_ids: &[String]) -> Vec<(String, bool, Option<String>)> {
        let mut results = Vec::with_capacity(task_ids.len());
        for id in task_ids {
            match self.resume_task(id).await {
                Ok(_) => results.push((id.clone(), true, None)),
                Err(e) => results.push((id.clone(), false, Some(e.to_string()))),
            }
        }

        // 🔥 通知 FolderDownloadManager 更新文件夹级别状态
        // batch_resume 只恢复了子任务，文件夹状态须同步为 Downloading
        // 保证前端文件夹状态正确推送，并允许 FolderManager 调度后续任务
        {
            let affected_folder_ids: std::collections::HashSet<String> = {
                let tasks_guard = self.tasks.read().await;
                let mut ids = std::collections::HashSet::new();
                for id in task_ids {
                    if let Some(task_arc) = tasks_guard.get(id) {
                        let task = task_arc.lock().await;
                        if let Some(ref gid) = task.group_id {
                            ids.insert(gid.clone());
                        }
                    }
                }
                ids
            };
            if !affected_folder_ids.is_empty() {
                let fm_opt = self.folder_manager.read().await.clone();
                if let Some(fm) = fm_opt {
                    for folder_id in &affected_folder_ids {
                        if let Err(e) = fm.resume_folder(folder_id).await {
                            // 文件夹可能未处于 Paused 状态（如之前未通过 pause_folder 暂停），忽略
                            info!("批量恢复：文件夹 {} 状态更新跳过: {}", folder_id, e);
                        }
                    }
                }
            }
        }

        results
    }

    /// 批量删除下载任务
    pub async fn batch_delete(&self, task_ids: &[String], delete_files: bool) -> Vec<(String, bool, Option<String>)> {
        let mut results = Vec::with_capacity(task_ids.len());
        for id in task_ids {
            match self.delete_task(id, delete_files).await {
                Ok(_) => results.push((id.clone(), true, None)),
                Err(e) => results.push((id.clone(), false, Some(e.to_string()))),
            }
        }
        results
    }

    /// 获取可暂停的任务ID列表
    pub async fn get_pausable_task_ids(&self) -> Vec<String> {
        let tasks = self.tasks.read().await;
        let mut ids = Vec::new();
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            // 🔥 只返回非备份任务（下载管理页面不应操作自动备份任务）
            if !t.is_backup && matches!(t.status, TaskStatus::Downloading | TaskStatus::Pending) {
                ids.push(id.clone());
            }
        }
        ids
    }

    /// 获取可恢复的任务ID列表
    pub async fn get_resumable_task_ids(&self) -> Vec<String> {
        let tasks = self.tasks.read().await;
        let mut ids = Vec::new();
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            // 🔥 只返回非备份任务（下载管理页面不应操作自动备份任务）
            if !t.is_backup && matches!(t.status, TaskStatus::Paused | TaskStatus::Failed) {
                ids.push(id.clone());
            }
        }
        ids
    }

    /// 获取所有任务ID列表（用于批量删除）
    pub async fn get_all_task_ids(&self) -> Vec<String> {
        self.tasks.read().await.keys().cloned().collect()
    }

    // ============================================================
    // 多账号：按 owner_uid 过滤的批量操作助手
    //
    // 共享 Manager 设计下，所有持久化账号共用同一个 `DownloadManager` 实例
    // （见 `state.rs::register_account_managers`）。前端在 active 账号下点
    // 「全部暂停 / 全部恢复 / 全部删除 / 清除已完成 / 清除失败」时，必须
    // 按 `task.owner_uid == uid` 过滤，否则会误操作其他账号的任务。
    //
    // 调用约定：handler 解析 `effective_uid = req.uid.or(active_uid)`，
    // 再传入 `_for_uid` 方法。备份任务（is_backup=true）始终被排除。
    // ============================================================

    /// 获取属于指定账号的可暂停任务ID列表
    pub async fn get_pausable_task_ids_for_uid(&self, uid: crate::auth::Uid) -> Vec<String> {
        let tasks = self.tasks.read().await;
        let mut ids = Vec::new();
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.owner_uid == uid
                && !t.is_backup
                && matches!(t.status, TaskStatus::Downloading | TaskStatus::Pending)
            {
                ids.push(id.clone());
            }
        }
        ids
    }

    /// 获取属于指定账号的可恢复任务ID列表
    pub async fn get_resumable_task_ids_for_uid(&self, uid: crate::auth::Uid) -> Vec<String> {
        let tasks = self.tasks.read().await;
        let mut ids = Vec::new();
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.owner_uid == uid
                && !t.is_backup
                && matches!(t.status, TaskStatus::Paused | TaskStatus::Failed)
            {
                ids.push(id.clone());
            }
        }
        ids
    }

    /// 获取属于指定账号的所有任务ID列表（含 active/Paused/Failed/Completed，
    /// 用于"全部删除"批量操作）。备份任务排除。
    pub async fn get_all_task_ids_for_uid(&self, uid: crate::auth::Uid) -> Vec<String> {
        let tasks = self.tasks.read().await;
        let mut ids = Vec::new();
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.owner_uid == uid && !t.is_backup {
                ids.push(id.clone());
            }
        }
        ids
    }

    /// 校验显式提交的 task_ids 是否都属于指定账号
    ///
    /// 共享 manager 设计下，handler 只用 `effective_uid` 路由是不够的——前端
    /// 直接传 `task_ids` 时也必须每条校验 `task.owner_uid == uid`，否则 A 账号
    /// 上下文可以操作 B 账号任务。
    ///
    /// 返回 `(allowed, denied)`：
    /// - `allowed`：所有 `task.owner_uid == uid` 且任务存在的 id（直接传给 batch_*）
    /// - `denied`：`(id, reason)` —— 任务不存在 / 归属不同账号 / is_backup
    ///
    /// 调用方按 `denied` 生成 `BatchOperationItem { success: false, error: Some(reason) }`，
    /// 与 `batch_pause` 的返回结构合并即可。
    pub async fn validate_task_ids_for_uid(
        &self,
        uid: crate::auth::Uid,
        task_ids: &[String],
    ) -> (Vec<String>, Vec<(String, String)>) {
        let tasks = self.tasks.read().await;
        let mut allowed = Vec::new();
        let mut denied = Vec::new();
        for id in task_ids {
            match tasks.get(id) {
                Some(task_arc) => {
                    let t = task_arc.lock().await;
                    if t.owner_uid != uid {
                        denied.push((
                            id.clone(),
                            format!(
                                "任务不属于当前账号（task.owner_uid={}, 请求 uid={}）",
                                t.owner_uid.raw(),
                                uid.raw()
                            ),
                        ));
                    } else if t.is_backup {
                        denied.push((
                            id.clone(),
                            "备份任务不允许通过下载管理批量接口操作".to_string(),
                        ));
                    } else {
                        allowed.push(id.clone());
                    }
                }
                None => {
                    denied.push((id.clone(), "任务不存在".to_string()));
                }
            }
        }
        (allowed, denied)
    }

    /// 清除指定账号下已完成的任务（仅内存 + 内存对应历史）
    ///
    /// 共享 manager 设计下 `clear_completed()` 会清掉所有账号的已完成任务，
    /// 这里按 owner_uid 严格过滤。
    pub async fn clear_completed_for_uid(&self, uid: crate::auth::Uid) -> usize {
        let mut tasks = self.tasks.write().await;
        let mut to_remove = Vec::new();

        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.owner_uid == uid
                && t.status == TaskStatus::Completed
                && !t.is_share_direct_download
            {
                to_remove.push(id.clone());
            }
        }

        let memory_count = to_remove.len();
        for id in &to_remove {
            tasks.remove(id);
        }
        drop(tasks);

        // 历史数据库：仅删该 owner_uid 的已完成下载任务
        let mut history_count = 0;
        if let Some(ref pm) = self.persistence_manager {
            let pm_guard = pm.lock().await;
            let history_db = pm_guard.history_db().cloned();
            drop(pm_guard);

            if let Some(db) = history_db {
                match db.remove_tasks_by_type_status_owner(
                    "download",
                    "completed",
                    Some(uid.raw()),
                ) {
                    Ok(count) => history_count = count,
                    Err(e) => warn!("从历史数据库删除已完成下载任务（按 owner_uid={}）失败: {}", uid.raw(), e),
                }
                match db.remove_completed_folders_for_owner(Some(uid.raw())) {
                    Ok(count) => {
                        history_count += count;
                        info!(
                            "从历史数据库删除了 {} 个已完成的文件夹任务（owner_uid={}）",
                            count,
                            uid.raw()
                        );
                    }
                    Err(e) => warn!("从历史数据库删除已完成文件夹任务（按 owner_uid={}）失败: {}", uid.raw(), e),
                }
            }
        }

        // FolderDownloadManager 内存：按 owner_uid 过滤已完成
        let folder_memory_count = {
            let fm = self.folder_manager.read().await;
            if let Some(ref folder_manager) = *fm {
                folder_manager.clear_completed_folders_for_owner(uid).await
            } else {
                0
            }
        };

        let total_count = memory_count + history_count + folder_memory_count;
        info!(
            "清除了 {} 个已完成的任务（owner_uid={}, 文件内存: {}, 文件夹内存: {}, 历史: {}）",
            total_count,
            uid.raw(),
            memory_count,
            folder_memory_count,
            history_count
        );
        total_count
    }

    /// 清除指定账号下失败的任务
    pub async fn clear_failed_for_uid(&self, uid: crate::auth::Uid) -> usize {
        let mut tasks = self.tasks.write().await;
        let mut to_remove = Vec::new();

        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.owner_uid == uid && t.status == TaskStatus::Failed {
                to_remove.push((id.clone(), t.local_path.clone()));
            }
        }

        let count = to_remove.len();
        for (id, local_path) in to_remove {
            tasks.remove(&id);

            if local_path.exists() {
                if let Err(e) = std::fs::remove_file(&local_path) {
                    warn!("删除失败任务的临时文件失败: {:?}, 错误: {}", local_path, e);
                } else {
                    info!("已删除失败任务的临时文件: {:?}", local_path);
                }
            }
        }

        info!("清除了 {} 个失败的任务（owner_uid={}）", count, uid.raw());
        count
    }

    /// 删除指定账号下所有下载任务
    ///
    /// 用于 `force_delete_account` 链路：共享 `DownloadManager` 设计下，
    /// 删除账号时必须取消并删除该 uid 归属的所有任务（运行中的取消、内存中的
    /// 移除、`.meta` 持久化清理、历史记录清理），否则任务在共享 manager 内
    /// 继续跑，账号已从 accounts.json 删除但下载任务还在消耗带宽 + 占槽位。
    ///
    /// 返回 `(memory_deleted, history_deleted)`。
    pub async fn delete_tasks_for_owner(
        &self,
        uid: crate::auth::Uid,
        delete_files: bool,
    ) -> (usize, usize) {
        // 1) 收集内存中归属该 uid 的任务 ID（含备份任务，账号删除时一起清理）
        let target_ids: Vec<String> = {
            let tasks = self.tasks.read().await;
            let mut ids = Vec::new();
            for (id, task) in tasks.iter() {
                let t = task.lock().await;
                if t.owner_uid == uid {
                    ids.push(id.clone());
                }
            }
            ids
        };

        let memory_count = target_ids.len();
        info!(
            "delete_tasks_for_owner: uid={} 内存中找到 {} 个下载任务",
            uid.raw(),
            memory_count
        );

        // 2) batch_delete_tasks 复用：取消、移除内存、清理 .meta、释放槽位
        if !target_ids.is_empty() {
            let (success, failed) = self.batch_delete_tasks(&target_ids, delete_files).await;
            info!(
                "delete_tasks_for_owner: uid={} batch_delete 完成: 成功={}, 失败={}",
                uid.raw(),
                success,
                failed
            );
        }

        // 3) 历史数据库：按 owner_uid 删除该账号所有 download 历史 + 文件夹历史
        let mut history_count = 0;
        if let Some(ref pm) = self.persistence_manager {
            let pm_guard = pm.lock().await;
            let history_db = pm_guard.history_db().cloned();
            drop(pm_guard);

            if let Some(db) = history_db {
                match db.remove_tasks_by_type_owner("download", Some(uid.raw())) {
                    Ok(count) => history_count += count,
                    Err(e) => warn!(
                        "delete_tasks_for_owner: 删除历史下载任务（owner_uid={}）失败: {}",
                        uid.raw(),
                        e
                    ),
                }
                match db.remove_folders_by_owner(uid.raw()) {
                    Ok(count) => history_count += count,
                    Err(e) => warn!(
                        "delete_tasks_for_owner: 删除历史文件夹任务（owner_uid={}）失败: {}",
                        uid.raw(),
                        e
                    ),
                }
            }
        }

        info!(
            "delete_tasks_for_owner: uid={} 完成（内存={}, 历史={}）",
            uid.raw(),
            memory_count,
            history_count
        );
        (memory_count, history_count)
    }

    /// 删除归属某 `backup_config_id`（如 `share-sync:{订阅id}`）的全部下载任务
    /// （内存 + 历史）。用于删除分享同步订阅时清掉内部下载子任务,避免孤儿脏数据。
    /// 返回 `(内存数, 历史数)`。下载文件本身保留（`delete_files=false`）。
    pub async fn delete_tasks_for_backup_config(&self, cfg_id: &str) -> (usize, usize) {
        // 1) 收集内存中归属该 cfg_id 的任务 ID。
        let target_ids: Vec<String> = {
            let tasks = self.tasks.read().await;
            let mut ids = Vec::new();
            for (id, task) in tasks.iter() {
                let t = task.lock().await;
                if t.backup_config_id.as_deref() == Some(cfg_id) {
                    ids.push(id.clone());
                }
            }
            ids
        };

        let memory_count = target_ids.len();
        if !target_ids.is_empty() {
            let (success, failed) = self.batch_delete_tasks(&target_ids, false).await;
            info!(
                "delete_tasks_for_backup_config: cfg={} batch_delete 完成: 成功={}, 失败={}",
                cfg_id, success, failed
            );
        }

        // 2) 历史数据库：按 backup_config_id 删除历史。
        let mut history_count = 0;
        if let Some(ref pm) = self.persistence_manager {
            let pm_guard = pm.lock().await;
            let history_db = pm_guard.history_db().cloned();
            drop(pm_guard);

            if let Some(db) = history_db {
                match db.remove_tasks_by_backup_config(cfg_id) {
                    Ok(count) => history_count += count,
                    Err(e) => warn!(
                        "delete_tasks_for_backup_config: 删除历史下载任务（cfg={}）失败: {}",
                        cfg_id, e
                    ),
                }
            }
        }

        info!(
            "delete_tasks_for_backup_config: cfg={} 完成（内存={}, 历史={}）",
            cfg_id, memory_count, history_count
        );
        (memory_count, history_count)
    }

    /// 获取下载目录
    pub async fn download_dir(&self) -> PathBuf {
        self.download_dir.read().await.clone()
    }

    /// 动态更新下载目录
    ///
    /// 当配置中的 download_dir 改变时调用此方法
    /// 注意：只影响新创建的下载任务，已存在的任务不受影响
    pub async fn update_download_dir(&self, new_dir: PathBuf) {
        let mut dir = self.download_dir.write().await;
        if *dir != new_dir {
            // 确保新目录存在
            if !new_dir.exists() {
                if let Err(e) = std::fs::create_dir_all(&new_dir) {
                    error!("创建新下载目录失败: {:?}, 错误: {}", new_dir, e);
                    return;
                }
                info!("✓ 新下载目录已创建: {:?}", new_dir);
            }
            info!("更新下载目录: {:?} -> {:?}", *dir, new_dir);
            *dir = new_dir;
        }
    }

    /// 动态更新全局最大线程数
    ///
    /// 该方法可以在运行时调整线程池大小，无需重启下载管理器
    /// 正在进行的下载任务不受影响
    pub fn update_max_threads(&self, new_max: usize) {
        self.chunk_scheduler.update_max_threads(new_max);
    }

    /// 动态更新最大并发任务数
    ///
    /// 该方法可以在运行时调整最大并发任务数：
    /// - **调大**：自动从等待队列启动新任务，同时扩展任务位池容量
    /// - **调小**：不会打断正在下载的任务，但新任务会进入等待队列
    ///   当前运行的任务完成后，会根据新的限制从等待队列启动任务
    ///   任务位池容量同步缩减（超出上限的占用槽位继续运行到完成）
    pub async fn update_max_concurrent_tasks(&self, new_max: usize) {
        // 用 swap 原子读写，正确返回上一次写入值，避免 5→3→4 序列时误判为"调小"。
        let old_max = self.max_concurrent_tasks.swap(new_max, Ordering::SeqCst);

        // 更新调度器的限制
        self.chunk_scheduler.update_max_concurrent_tasks(new_max);

        // 🔥 动态调整任务位池容量
        self.task_slot_pool.resize(new_max).await;

        if new_max > old_max {
            // 调大：立即尝试启动等待队列中的任务
            info!(
                "🔧 最大并发任务数调大: {} -> {}, 启动等待任务",
                old_max, new_max
            );
            self.try_start_waiting_tasks().await;
        } else if new_max < old_max {
            // 调小：不打断现有任务，但新任务会进入等待队列
            let active_count = self.chunk_scheduler.active_task_count().await;
            info!(
                "🔧 最大并发任务数调小: {} -> {} (当前活跃: {})",
                old_max, new_max, active_count
            );

            if active_count > new_max {
                info!(
                    "当前有 {} 个活跃任务超过新限制 {}，这些任务将继续运行直到完成",
                    active_count, new_max
                );
            }
        }
    }

    /// 获取当前线程池状态
    pub fn get_thread_pool_stats(&self) -> (usize, usize) {
        let max_threads = self.chunk_scheduler.max_threads();
        let active_threads = self.chunk_scheduler.active_threads();
        (active_threads, max_threads)
    }

    /// 设置任务完成通知发送器（用于文件夹下载补充任务）
    pub async fn set_task_completed_sender(&self, tx: tokio::sync::mpsc::UnboundedSender<(String, String, u64, bool)>) {
        self.chunk_scheduler.set_task_completed_sender(tx).await;
    }

    /// 🔥 设置备份任务统一通知发送器
    ///
    /// AutoBackupManager 调用此方法设置 channel sender，
    /// 所有备份相关事件（进度、状态、完成、失败等）都通过此 channel 发送
    pub async fn set_backup_notification_sender(&self, tx: tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>) {
        // 设置到调度器（用于进度和完成/失败事件）
        self.chunk_scheduler.set_backup_notification_sender(tx.clone()).await;
        // 设置到管理器自身（用于状态变更事件，如暂停/恢复）
        let mut guard = self.backup_notification_tx.write().await;
        *guard = Some(tx);
        info!("下载管理器已设置备份任务统一通知发送器");
    }

    /// 🔥 设置文件夹进度通知发送器（用于子任务进度变化时通知文件夹管理器）
    pub async fn set_folder_progress_sender(&self, tx: tokio::sync::mpsc::UnboundedSender<String>) {
        let mut guard = self.folder_progress_tx.write().await;
        *guard = Some(tx);
        info!("下载管理器已设置文件夹进度通知发送器");
    }

    /// 根据 group_id 获取任务列表
    pub async fn get_tasks_by_group(&self, group_id: &str) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        for task_arc in tasks.values() {
            let task = task_arc.lock().await;
            if task.group_id.as_deref() == Some(group_id) {
                result.push(task.clone());
            }
        }

        result
    }

    /// 从等待队列中移除指定 group 的所有任务
    ///
    /// 用于文件夹暂停时，防止暂停活跃任务后触发从等待队列启动新任务
    pub async fn remove_waiting_tasks_by_group(&self, group_id: &str) -> usize {
        let mut waiting_queue = self.waiting_queue.write().await;
        let tasks = self.tasks.read().await;

        let original_len = waiting_queue.len();

        // 保留不属于该 group 的任务
        let mut new_queue = VecDeque::new();
        for task_id in waiting_queue.drain(..) {
            let should_keep = if let Some(task_arc) = tasks.get(&task_id) {
                let task = task_arc.lock().await;
                task.group_id.as_deref() != Some(group_id)
            } else {
                true // 任务不存在，保留 ID（后续会自然处理）
            };

            if should_keep {
                new_queue.push_back(task_id);
            }
        }

        let removed_count = original_len - new_queue.len();
        *waiting_queue = new_queue;

        if removed_count > 0 {
            info!(
                "从等待队列移除了 {} 个属于文件夹 {} 的任务",
                removed_count, group_id
            );
        }

        removed_count
    }

    /// 取消指定 group 的所有任务（包括正在探测中的任务）
    ///
    /// 用于文件夹暂停时，取消所有子任务：
    /// - 从等待队列移除
    /// - 触发取消令牌（让正在探测的任务知道应该停止）
    /// - 从调度器取消（已注册的任务）
    /// - 更新任务状态为 Paused
    ///
    /// 注意：此方法不会删除任务，只是暂停它们
    pub async fn cancel_tasks_by_group(&self, group_id: &str) {
        // 1. 从等待队列移除
        self.remove_waiting_tasks_by_group(group_id).await;

        // 2. 收集本 group 下所有子任务的 (task_id, Arc<Mutex<DownloadTask>>)
        //
        // 🔥 不再使用 try_lock 过滤：try_lock 失败会被静默当作"无此任务"，
        //    导致正被启动 / 进度更新 / 准备探测等路径短暂持锁的子任务
        //    被整段清理流程跳过——后续既不会触发取消令牌、也不会从调度器移除、
        //    更不会标 Paused 清槽位字段。结果是文件夹已经"暂停"，但最容易卡在
        //    临界区的那个子任务反而继续跑下去，或暂停期间又重新进入调度，
        //    出现"文件夹已暂停但仍有子任务在跑"的竞态。
        //
        //    改为：先在 self.tasks 读锁内只克隆 (id, Arc) 引用，立刻释放读锁；
        //    再对每个 Arc 用 `lock().await` 等到任务锁可用后判定 group_id。
        //    self.tasks 是 RwLock<HashMap>，task_arc 内部是独立 Mutex，
        //    在 self.tasks 读锁外 await task_arc.lock() 不会引入死锁。
        let candidates: Vec<(String, Arc<Mutex<DownloadTask>>)> = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .map(|(id, task_arc)| (id.clone(), task_arc.clone()))
                .collect()
        };

        let mut matched: Vec<(String, Arc<Mutex<DownloadTask>>)> = Vec::new();
        for (id, task_arc) in candidates {
            let t = task_arc.lock().await;
            if t.group_id.as_deref() == Some(group_id) {
                drop(t);
                matched.push((id, task_arc));
            }
        }

        info!(
            "取消文件夹 {} 的 {} 个任务（包括探测中的）",
            group_id,
            matched.len()
        );

        // 3. 对每个任务：触发取消令牌 → scheduler 取消 → 释放槽位 → 标 Paused 清字段
        for (task_id, task_arc) in &matched {
            // 触发取消令牌（让正在探测的任务知道应该停止）
            {
                let tokens = self.cancellation_tokens.read().await;
                if let Some(token) = tokens.get(task_id) {
                    token.cancel();
                }
            }

            // 从调度器取消（已注册的任务）
            self.chunk_scheduler.cancel_task(task_id).await;

            // 🔥 在同一把锁内：快照三个槽位字段、判定是否活跃、mark_paused 并清字段
            //
            //    必须先快照槽位字段再清空：release_task_slot_by_kind 内部需要根据
            //    (slot_id / is_borrowed_slot / uses_folder_fixed_slot) 三元组路由到正确分支
            //    （普通全局 fixed slot / 文件夹借调位 / 文件夹固定位）。
            //    清字段动作放在锁内做完，避免出锁后还有其它路径看到"字段在但状态已 Paused"
            //    的中间态去误用 stale slot_id。
            //
            //    这里对 Downloading / Pending / Decrypting 三种状态都视为活跃：
            //
            //    - Pending：字段大概率为空，清一次是 no-op
            //    - Downloading：必须清，否则脏标记会泄漏到恢复路径让
            //      `needs_slot = slot_id.is_none() && !uses_folder_fixed_slot`
            //      误判为"已有槽位"跳过真正的重新分配
            //    - Decrypting：调度器在分片全部完成后会先把任务从 `active_tasks` 移除并
            //      cancel 其 cancellation_token，再 `tokio::spawn` 一个独立协程进入解密
            //      （见 `@/.../scheduler.rs:732-772`）。文件夹此刻被暂停时：
            //        * `chunk_scheduler.cancel_task(task_id)` 对该任务是 no-op
            //          （已不在 active_tasks）
            //        * 解密协程独立运行，不响应 manager.cancellation_tokens 表
            //        * 若不把 Decrypting 纳入这里，task 字段不会清，槽位也不会释放
            //          —— 关键问题：当子任务持有 fallback owner=task_id 的全局 fixed slot
            //          时（见 `@/.../manager.rs:1785-1787` 的 SubTask 分支），该槽位要等
            //          解密协程跑完进入 `handle_task_completion` 才会被释放，期间所有
            //          其它任务都拿不到这个槽位，整个文件夹"已暂停"但全局调度仍被它阻塞。
            //
            //    把 Decrypting 加进 active 后，槽位被立刻释放、字段被清、状态翻 Paused。
            //    暂停语义闭环还需要 scheduler 解密收尾路径配合：`handle_task_completion`
            //    入口已加入 Paused 短路检查（见 `@/.../scheduler.rs` 中 handle_task_completion
            //    的入口检查段），任务一旦在解密期间被这里翻成 Paused，解密协程结束时
            //    既不会覆盖终态、也不会发 Completed / Failed 事件、不归档、不通知，
            //    任务保持 Paused 等用户恢复。
            //    协程内部的 `slot_pool.release_fixed_slot(task_id)` 也因为 Paused 短路
            //    return 而不会执行；即便它执行（理论上其它路径也调用了同函数），
            //    在我们这里释放过后是 no-op（owner=task_id 的 slot 已 free），无重复副作用。
            let (slot_id, is_borrowed_slot, uses_folder_fixed_slot, was_active) = {
                let mut task = task_arc.lock().await;
                let was_decrypting = task.status == TaskStatus::Decrypting;

                // 🔥 R22: 解密原子提交协作——已提交的任务跳过暂停
                //
                // try_decrypt_if_encrypted 在 rename(attempt → final) 之后的
                // 锁内提交点已置位 decrypt_committed=true，并在同一锁段内更新了
                // mark_decrypt_completed + task.local_path。此时任务已进入"等待
                // 锁外删加密文件 + 持久化 + handle_task_completion mark_completed"
                // 的不可中断收尾窗口，暂停语义在这段时间应该让位。
                //
                // 否则：
                //   - 翻 Paused 会让 status=Paused 但 task 字段已是"解密完成的样子"
                //     （decrypted_path/total_size/downloaded_size 都已更新），形成
                //     不一致 transient state；
                //   - invalidate_decrypt_epoch 也无意义，因为本协程已经过 7.8 锁内
                //     stale check，不会再回头检查 epoch；
                //   - 释放槽位会与 handle_task_completion 内部的释放冲突。
                //
                // 见 DownloadTask::decrypt_committed 的字段文档与
                // scheduler::try_decrypt_if_encrypted 7.8 段的实现注释。
                let already_committed = task.decrypt_committed;
                let active = !already_committed
                    && matches!(
                        task.status,
                        TaskStatus::Downloading | TaskStatus::Pending | TaskStatus::Decrypting
                    );
                let snapshot = (
                    task.slot_id,
                    task.is_borrowed_slot,
                    task.uses_folder_fixed_slot,
                );
                if active {
                    task.mark_paused();
                    task.slot_id = None;
                    task.is_borrowed_slot = false;
                    task.uses_folder_fixed_slot = false;

                    // 🔥 R20: 仅当从 Decrypting 暂停时才递增 decrypt_epoch
                    //
                    // 这个 epoch 是给"当前正在跑的解密协程"发的失效信号——
                    // 它会让旧协程在所有破坏性操作前的检查点（spawn_blocking 前
                    // 预检 / spawn_blocking 后硬检查 / 进度回调 / handle_task_completion
                    // 入口原子判定 / 7.8 锁内提交点）比对发现 my_epoch != task.decrypt_epoch，
                    // 直接 return 不写终态、不删加密文件、不改 local_path。
                    //
                    // 必须**与 mark_paused 在同一锁段**调用：保证旧协程后续
                    // 任何检查点都不可能再看到"status 不是 Paused 但 epoch 仍然
                    // 一致"的中间态。
                    //
                    // 仅 Decrypting 路径需要：Pending / Downloading 阶段没有
                    // spawn 解密协程，递增 epoch 是 no-op，但保留范围聚焦的
                    // 写法更清晰。
                    if was_decrypting {
                        task.invalidate_decrypt_epoch();
                    }
                } else if already_committed {
                    debug!(
                        "任务 {} 已经过解密原子提交（decrypt_committed=true），\
                         cancel_tasks_by_group 跳过该任务以保证收尾不被打断",
                        task_id
                    );
                }
                (snapshot.0, snapshot.1, snapshot.2, active)
            };

            // 🔥 释放槽位：调统一接口 release_task_slot_by_kind，按 kind 路由
            //
            //    这里必须按 task 维度释放，而不是只依赖后续 release_folder_slots 的
            //    `release_all_slots(folder_id)`：后者用 `slot.task_id == folder_id` 比对，
            //    只能命中 owner=folder_id 的槽位（文件夹借调位 + 文件夹固定位）。
            //    但当文件夹子任务通过 try_start_waiting_tasks 走 fallback 路径
            //    （allocate_fixed_slot_with_priority(task_id, ..., TaskPriority::SubTask)
            //    见 @ /backend/src/downloader/manager.rs:1785-1787）拿到普通全局 fixed slot 时，
            //    槽位的 task_id 字段是 task_id 本身，不是 folder_id，
            //    `release_all_slots(folder_id)` 会漏释放，全局槽位就会一直占着直到
            //    超时回收，影响其它任务正常获取槽位。
            //
            //    release_task_slot_by_kind 的 c 分支
            //    （非 uses_folder_fixed_slot && 非 is_borrowed_slot && slot_id=Some）
            //    走的就是 `task_slot_pool.release_fixed_slot(task_id)`，正好覆盖该场景。
            //    a / b 分支处理 folder 侧 fixed_slot_subtask / borrowed_subtask_map 的
            //    per-task 映射清理，与 release_folder_slots 的"folder 端总映射清理"互补。
            if was_active {
                self.release_task_slot_by_kind(
                    task_id,
                    Some(group_id),
                    slot_id,
                    is_borrowed_slot,
                    uses_folder_fixed_slot,
                )
                    .await;
            }
        }
    }

    /// 添加任务（由 FolderDownloadManager 调用）
    pub async fn add_task(&self, task: DownloadTask) -> Result<String> {
        let task_id = task.id.clone();

        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(task_id.clone(), Arc::new(Mutex::new(task)));
        }

        // 启动任务
        self.start_task(&task_id).await?;

        Ok(task_id)
    }

    /// 添加任务但设为暂停状态（由 FolderDownloadManager 恢复模式调用）
    ///
    /// 与 `add_task` 不同的是：
    /// 1. 任务状态设为 Paused
    /// 2. 不调用 start_task，不进入调度队列
    /// 3. 任务仅写入 tasks HashMap，前端可见但不会自动下载
    ///
    /// 用户点击"继续"时，由 FolderDownloadManager::resume_folder 调用
    /// resume_task + refill_tasks 启动下载
    pub async fn add_task_paused(&self, mut task: DownloadTask) -> Result<String> {
        let task_id = task.id.clone();

        // 设为暂停状态
        task.status = TaskStatus::Paused;

        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(task_id.clone(), Arc::new(Mutex::new(task)));
        }

        // 不调用 start_task，仅添加到任务列表
        Ok(task_id)
    }

    /// 🔥 从恢复信息创建任务
    ///
    /// 用于程序启动时恢复未完成的下载任务
    /// 恢复的任务初始状态为 Paused，需要手动调用 resume_task 启动
    ///
    /// # Arguments
    /// * `recovery_info` - 从持久化文件恢复的任务信息
    ///
    /// # Returns
    /// 恢复的任务 ID
    pub async fn restore_task(&self, recovery_info: DownloadRecoveryInfo) -> Result<String> {
        let task_id = recovery_info.task_id.clone();

        // 检查任务是否已存在
        if self.tasks.read().await.contains_key(&task_id) {
            anyhow::bail!("任务 {} 已存在，无法恢复", task_id);
        }

        // 确保目标目录存在
        if let Some(parent) = recovery_info.local_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).context("创建下载目录失败")?;
            }
        }

        // 多账号 owner_uid 优先级 = recovery_info.owner_uid > self.owner_uid
        //
        // recovery_info.owner_uid 已经过 state.rs::filter_by_branch 的兜底填充：
        //   - Some(uid) 且 uid ∈ accounts → Normal { uid }
        //   - Some(0)/None + active_uid    → LegacyFillActive { active_uid }
        //   - Some(uid) 且 uid ∉ accounts → 已被丢弃（不会进 restore_task）
        //
        // 仅当 recovery_info.owner_uid 缺失时（极端边界）才退回 self.owner_uid 作兜底。
        let resolved_owner_uid = recovery_info
            .owner_uid
            .map(crate::auth::Uid::new)
            .unwrap_or(self.owner_uid);

        // 创建恢复任务（使用 Paused 状态）
        // 🔥 根据是否为备份任务选择不同的构造方式
        let mut task = if recovery_info.is_backup {
            DownloadTask::new_backup(
                recovery_info.fs_id,
                recovery_info.remote_path.clone(),
                recovery_info.local_path.clone(),
                recovery_info.file_size,
                recovery_info.backup_config_id.clone().unwrap_or_default(),
                resolved_owner_uid,
            )
        } else {
            DownloadTask::new(
                recovery_info.fs_id,
                recovery_info.remote_path.clone(),
                recovery_info.local_path.clone(),
                recovery_info.file_size,
                resolved_owner_uid,
            )
        };

        // 恢复任务 ID（保持原有 ID）
        task.id = task_id.clone();

        // 🔥 终态失败任务恢复为 Failed（保留错误信息，等待用户手动重试/删除）；
        // 其余任务恢复为 Paused（等待用户手动恢复续传）。
        if recovery_info.is_failed {
            task.status = TaskStatus::Failed;
            task.error = recovery_info.error.clone();
        } else {
            task.status = TaskStatus::Paused;
        }

        // 🔥 精确计算已下载大小：逐片累计（处理稀疏分布）
        let last_chunk_index = recovery_info.total_chunks.saturating_sub(1);
        let downloaded_size: u64 = recovery_info
            .completed_chunks
            .iter()
            .map(|idx| {
                if idx == last_chunk_index {
                    // 最后一个分片可能不足 chunk_size
                    recovery_info
                        .file_size
                        .saturating_sub(idx as u64 * recovery_info.chunk_size)
                } else {
                    recovery_info.chunk_size
                }
            })
            .sum();
        // 🔥 加上分片内部分进度（冷恢复断点续传）
        let partial_bytes: u64 = recovery_info.partial_progress.values().sum();
        task.downloaded_size = downloaded_size + partial_bytes;
        task.created_at = recovery_info.created_at;

        // 恢复文件夹下载组信息
        task.group_id = recovery_info.group_id.clone();
        task.group_root = recovery_info.group_root.clone();
        task.relative_path = recovery_info.relative_path.clone();

        // 🔥 恢复跨任务跳转字段
        task.transfer_task_id = recovery_info.transfer_task_id.clone();

        let completed_count = recovery_info.completed_chunks.len();
        info!(
            "恢复下载任务: id={}, 文件={:?}, 已完成 {}/{} 分片 ({:.1}%), partial_chunks={}, group_id={:?}{}",
            task_id,
            recovery_info.local_path,
            completed_count,
            recovery_info.total_chunks,
            if recovery_info.total_chunks > 0 {
                (completed_count as f64 / recovery_info.total_chunks as f64) * 100.0
            } else {
                0.0
            },
            recovery_info.partial_progress.len(),
            recovery_info.group_id,
            if recovery_info.is_backup { "（备份任务）" } else { "" }
        );

        // 🔥 判断是否为单文件任务（无 group_id），需要分配固定任务位
        let is_single_file = recovery_info.group_id.is_none();

        // 添加到任务列表
        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc.clone());

        // 🔥 暂停/失败态的任务都不分配槽位，等待用户手动恢复/重试时再分配
        // 这样可以让正在下载的任务借用更多槽位
        if is_single_file {
            let restored_state = if recovery_info.is_failed { "失败状态" } else { "暂停状态" };
            info!("单文件任务 {} 恢复完成 ({}，不占用槽位)", task_id, restored_state);
        } else {
            info!("文件夹子任务 {} 恢复完成，槽位由 FolderManager 管理", task_id);
        }

        // 🔥 恢复持久化状态（重新加载到内存）
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.restore_task_state(
                &task_id,
                crate::persistence::TaskType::Download,
                recovery_info.total_chunks,
            ) {
                warn!("恢复任务持久化状态失败: {}", e);
            }
        }

        Ok(task_id)
    }

    /// 🔥 批量恢复任务
    ///
    /// 从恢复信息列表批量创建任务
    ///
    /// # Arguments
    /// * `recovery_infos` - 恢复信息列表
    ///
    /// # Returns
    /// (成功数, 失败数)
    pub async fn restore_tasks(&self, recovery_infos: Vec<DownloadRecoveryInfo>) -> (usize, usize) {
        let mut success = 0;
        let mut failed = 0;

        for info in recovery_infos {
            match self.restore_task(info).await {
                Ok(_) => success += 1,
                Err(e) => {
                    warn!("恢复任务失败: {}", e);
                    failed += 1;
                }
            }
        }

        info!("批量恢复完成: {} 成功, {} 失败", success, failed);
        (success, failed)
    }

    /// 设置文件夹下载管理器引用（用于回收借调槽位）
    pub async fn set_folder_manager(&self, folder_manager: Arc<FolderDownloadManager>) {
        *self.folder_manager.write().await = Some(folder_manager);
    }
}

impl Drop for DownloadManager {
    fn drop(&mut self) {
        // 停止调度器（只有当 DownloadManager 的所有引用都被释放时才会调用）
        self.chunk_scheduler.stop();
        info!("下载管理器已销毁，调度器已停止");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UserAuth;
    use tempfile::TempDir;

    fn create_mock_user_auth() -> UserAuth {
        UserAuth {
            uid: 123456789,
            username: "test_user".to_string(),
            nickname: Some("测试用户".to_string()),
            avatar_url: Some("https://example.com/avatar.jpg".to_string()),
            vip_type: Some(2),                                // SVIP
            total_space: Some(2 * 1024 * 1024 * 1024 * 1024), // 2TB
            used_space: Some(500 * 1024 * 1024 * 1024),       // 500GB
            bduss: "mock_bduss".to_string(),
            stoken: Some("mock_stoken".to_string()),
            ptoken: Some("mock_ptoken".to_string()),
            baiduid: Some("mock_baiduid".to_string()),
            passid: Some("mock_passid".to_string()),
            cookies: Some("BDUSS=mock_bduss".to_string()),
            panpsc: Some("mock_panpsc".to_string()),
            csrf_token: Some("mock_csrf".to_string()),
            bdstoken: Some("mock_bdstoken".to_string()),
            login_time: 0,
            last_warmup_at: None,
            custom_config: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).await.unwrap();

        assert_eq!(manager.download_dir().await, temp_dir.path());
        assert_eq!(manager.get_all_tasks().await.len(), 0);
    }

    #[tokio::test]
    async fn test_create_task() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).await.unwrap();

        let task_id = manager
            .create_task(
                12345,
                "/test/file.txt".to_string(),
                "file.txt".to_string(),
                1024,
                None,
            )
            .await
            .unwrap();

        assert!(!task_id.is_empty());
        assert_eq!(manager.get_all_tasks().await.len(), 1);

        let task = manager.get_task(&task_id).await.unwrap();
        assert_eq!(task.fs_id, 12345);
        assert_eq!(task.status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn test_delete_task() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).await.unwrap();

        let task_id = manager
            .create_task(
                12345,
                "/test/file.txt".to_string(),
                "file.txt".to_string(),
                1024,
                None,
            )
            .await
            .unwrap();

        assert_eq!(manager.get_all_tasks().await.len(), 1);

        manager.delete_task(&task_id, false).await.unwrap();
        assert_eq!(manager.get_all_tasks().await.len(), 0);
    }

    #[tokio::test]
    async fn test_clear_completed() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).await.unwrap();

        // 创建3个任务
        let task_id1 = manager
            .create_task(1, "/test1".to_string(), "file1.txt".to_string(), 1024, None)
            .await
            .unwrap();
        let task_id2 = manager
            .create_task(2, "/test2".to_string(), "file2.txt".to_string(), 1024, None)
            .await
            .unwrap();
        let _task_id3 = manager
            .create_task(3, "/test3".to_string(), "file3.txt".to_string(), 1024, None)
            .await
            .unwrap();

        // 标记2个为已完成
        {
            let tasks = manager.tasks.read().await;
            tasks.get(&task_id1).unwrap().lock().await.mark_completed();
            tasks.get(&task_id2).unwrap().lock().await.mark_completed();
        }

        assert_eq!(manager.get_all_tasks().await.len(), 3);
        let cleared = manager.clear_completed().await;
        assert_eq!(cleared, 2);
        assert_eq!(manager.get_all_tasks().await.len(), 1);
    }

    /// 回归测试：DownloadManager::restore_task 冷恢复主链路
    ///
    /// 验证从 DownloadRecoveryInfo 恢复任务时：
    /// 1. task.downloaded_size = 已完成分片实际大小之和 + partial_progress 字节
    /// 2. restore_task_state 加载的 WAL 状态不被后续 register_download_task 覆盖
    #[tokio::test]
    async fn test_restore_task_cold_recovery_downloaded_size() {
        use bit_set::BitSet;
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();

        // --- 模拟场景 ---
        // 文件 50MB, 分片 5MB, 10 个分片
        // 已完成分片: #0, #3, #9（最后一个分片实际大小 = 50MB - 9*5MB = 5MB）
        // 分片 #4 部分下载了 2MB
        let file_size: u64 = 50 * 1024 * 1024;
        let chunk_size: u64 = 5 * 1024 * 1024;
        let total_chunks = 10usize;

        let pm_config = crate::config::PersistenceConfig {
            wal_dir: "wal".to_string(),
            db_path: "config/baidu-pcs.db".to_string(),
            wal_flush_interval_ms: 100,
            auto_recover_tasks: true,
            wal_retention_days: 7,
            history_archive_hour: 2,
            history_archive_minute: 0,
            history_retention_days: 30,
        };

        // 第一阶段：模拟上一次运行——写入 WAL 然后“崩溃”
        {
            let pm_prev = crate::persistence::PersistenceManager::new(
                pm_config.clone(),
                temp_dir.path(),
            );
            pm_prev
                .register_download_task(
                    "dl_cold".to_string(),
                    777,
                    "/remote/cold.bin".to_string(),
                    PathBuf::from(temp_dir.path().join("cold.bin")),
                    file_size,
                    chunk_size,
                    total_chunks,
                    None, None, None,
                    false, None, None, None, None,
                    None, // owner_uid_override
                )
                .unwrap();
            pm_prev.on_chunk_completed("dl_cold", 0);
            pm_prev.on_chunk_completed("dl_cold", 3);
            pm_prev.on_chunk_completed("dl_cold", 9);
            pm_prev.on_chunk_partial_progress("dl_cold", 4, 2 * 1024 * 1024);
            pm_prev.flush_all().await;
            // pm_prev 被 drop，模拟进程终止
        }

        // 第二阶段：模拟冷重启——新建 PersistenceManager（内存状态为空）
        let pm_new = crate::persistence::PersistenceManager::new(
            pm_config,
            temp_dir.path(),
        );
        let pm_arc = Arc::new(Mutex::new(pm_new));

        let user_auth = create_mock_user_auth();
        let mut manager =
            DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).await.unwrap();
        manager.persistence_manager = Some(pm_arc.clone());

        // 构建 DownloadRecoveryInfo
        let mut completed = BitSet::with_capacity(total_chunks);
        completed.insert(0);
        completed.insert(3);
        completed.insert(9);

        let mut partial_progress = HashMap::new();
        partial_progress.insert(4, 2 * 1024 * 1024u64);

        let recovery_info = DownloadRecoveryInfo {
            task_id: "dl_cold".to_string(),
            owner_uid: None,
            fs_id: 777,
            remote_path: "/remote/cold.bin".to_string(),
            local_path: temp_dir.path().join("cold.bin"),
            file_size,
            chunk_size,
            total_chunks,
            completed_chunks: completed,
            created_at: 1700000000,
            group_id: None,
            group_root: None,
            relative_path: None,
            transfer_task_id: None,
            is_backup: false,
            backup_config_id: None,
            is_encrypted: false,
            encryption_key_version: None,
            partial_progress,
            is_failed: false,
            error: None,
        };

        // 执行冷恢复
        let task_id = manager.restore_task(recovery_info).await.unwrap();
        assert_eq!(task_id, "dl_cold");

        // 验证 task.downloaded_size
        let task = manager.get_task(&task_id).await.unwrap();
        // 分片 #0 = 5MB, #3 = 5MB, #9（最后分片）= 50MB - 9*5MB = 5MB
        let expected_completed_bytes: u64 = 3 * chunk_size; // 3 * 5MB = 15MB
        let expected_partial_bytes: u64 = 2 * 1024 * 1024; // 2MB
        assert_eq!(
            task.downloaded_size,
            expected_completed_bytes + expected_partial_bytes,
            "downloaded_size 应 = 已完成分片大小之和 + 分片内部分进度"
        );

        // 验证持久化状态已恢复（restore_task 内部调用了 restore_task_state）
        {
            let pm_lock = pm_arc.lock().await;
            assert_eq!(
                pm_lock.get_completed_count("dl_cold"),
                Some(3),
                "持久化层应有 3 个已完成分片"
            );
            let partial = pm_lock.get_partial_progress("dl_cold").unwrap();
            assert_eq!(
                partial.get(&4),
                Some(&(2 * 1024 * 1024u64)),
                "持久化层应保留分片 #4 的 2MB 部分进度"
            );
        }

        // 模拟 start_task_internal 再次调用 register_download_task
        {
            let pm_lock = pm_arc.lock().await;
            pm_lock
                .register_download_task(
                    "dl_cold".to_string(),
                    777,
                    "/remote/cold.bin".to_string(),
                    PathBuf::from(temp_dir.path().join("cold.bin")),
                    file_size,
                    chunk_size,
                    total_chunks,
                    None, None, None,
                    false, None, None, None, None,
                    None, // owner_uid_override
                )
                .unwrap();

            // register 不能覆盖已恢复的状态
            assert_eq!(
                pm_lock.get_completed_count("dl_cold"),
                Some(3),
                "register 后，已完成分片数不能被清零"
            );
            let partial = pm_lock.get_partial_progress("dl_cold").unwrap();
            assert_eq!(
                partial.get(&4),
                Some(&(2 * 1024 * 1024u64)),
                "register 后，部分进度不能被清零"
            );
        }
    }

    /// 回归：小文件下载完成并归档后，`lookup_aggregate_outcome` 不能把它当成「丢失/取消」。
    ///
    /// 复现 share-sync 增量单文件失败：5KB 文件下载极快，完成瞬间被归档到历史库并从
    /// 内存移除；转存状态监听器 2s 后聚合时若仅查内存会判 NotFound→cancelled→转存回退
    /// Transferred→run 误报 failed。补查历史库后应识别为 ArchivedCompleted。
    #[tokio::test]
    async fn test_lookup_aggregate_outcome_archived_completed_not_lost() {
        use crate::persistence::types::{TaskMetadata, TaskPersistenceStatus};

        let temp_dir = TempDir::new().unwrap();
        let pm_config = crate::config::PersistenceConfig {
            wal_dir: "wal".to_string(),
            db_path: "config/baidu-pcs.db".to_string(),
            wal_flush_interval_ms: 100,
            auto_recover_tasks: true,
            wal_retention_days: 7,
            history_archive_hour: 2,
            history_archive_minute: 0,
            history_retention_days: 30,
        };
        let pm = crate::persistence::PersistenceManager::new(pm_config, temp_dir.path());

        // 模拟「下载完成 → 归档到历史库」：写入一条 status=Completed 的下载历史。
        let mut meta = TaskMetadata::new_download(
            "dl_fast".to_string(),
            42,
            "/13/分享同步测试/测试2-1/CONFLICT.md".to_string(),
            temp_dir.path().join("CONFLICT.md"),
            5 * 1024,
            4 * 1024 * 1024,
            1,
            None,
            None,
        );
        meta.set_status(TaskPersistenceStatus::Completed);
        pm.history_db()
            .expect("history_db enabled")
            .add_task_to_history(&meta)
            .unwrap();

        let user_auth = create_mock_user_auth();
        let mut manager =
            DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).await.unwrap();
        manager.persistence_manager = Some(Arc::new(Mutex::new(pm)));

        // 内存中并无该任务（已完成移除），但历史库标记 Completed。
        assert!(manager.get_task("dl_fast").await.is_none());
        assert_eq!(
            manager.lookup_aggregate_outcome("dl_fast").await,
            DownloadAggregateOutcome::ArchivedCompleted,
            "已归档完成的快速任务不能被误判为丢失/取消"
        );

        // 内存中的在途任务仍按内存状态返回。
        let live_id = manager
            .create_task(7, "/remote/live.bin".to_string(), "live.bin".to_string(), 1024, None)
            .await
            .unwrap();
        assert_eq!(
            manager.lookup_aggregate_outcome(&live_id).await,
            DownloadAggregateOutcome::InMemory(TaskStatus::Pending),
        );

        // 任何地方都查不到的任务才算 NotFound。
        assert_eq!(
            manager.lookup_aggregate_outcome("does_not_exist").await,
            DownloadAggregateOutcome::NotFound,
        );
    }
}
