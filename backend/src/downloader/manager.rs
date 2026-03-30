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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
    /// 最大同时下载任务数
    max_concurrent_tasks: usize,
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
}

impl DownloadManager {
    /// 创建新的下载管理器
    pub fn new(user_auth: UserAuth, download_dir: PathBuf) -> Result<Self> {
        Self::with_config(user_auth, download_dir, 10, 5, 3, None, None)
    }

    /// 使用指定配置创建下载管理器（不再需要 chunk_size 参数，引擎会自动计算）
    pub fn with_config(
        user_auth: UserAuth,
        download_dir: PathBuf,
        max_global_threads: usize,
        max_concurrent_tasks: usize,
        max_retries: u32,
        proxy_config: Option<&ProxyConfig>,
        fallback_mgr: Option<std::sync::Arc<crate::common::ProxyFallbackManager>>,
    ) -> Result<Self> {
        // 确保下载目录存在（路径验证已在配置保存时完成）
        if !download_dir.exists() {
            std::fs::create_dir_all(&download_dir).context("创建下载目录失败")?;
            info!("✓ 下载目录已创建: {:?}", download_dir);
        }

        // 创建全局分片调度器（不再使用 Semaphore）
        let chunk_scheduler = ChunkScheduler::new(max_global_threads, max_concurrent_tasks);

        info!(
            "创建下载管理器: 下载目录={:?}, 全局线程数={}, 最大同时下载数={} (分片大小自适应)",
            download_dir, max_global_threads, max_concurrent_tasks
        );

        let engine = Arc::new(DownloadEngine::new_with_proxy(user_auth, proxy_config, fallback_mgr));

        let manager = Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            waiting_queue: Arc::new(RwLock::new(VecDeque::new())),
            engine,
            download_dir: Arc::new(RwLock::new(download_dir)),
            chunk_scheduler,
            max_concurrent_tasks,
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
        };

        // 🔥 设置槽位超时释放处理器
        manager.setup_stale_release_handler();

        // 启动后台任务：定期检查并启动等待队列中的任务
        manager.start_waiting_queue_monitor();

        // 🔥 设置任务完成触发器（0延迟启动等待任务）
        manager.setup_waiting_queue_trigger();

        // 🔥 启动活跃计数漂移校准（每 60 秒）
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

    /// 🔥 设置持久化管理器
    ///
    /// 由 AppState 在初始化时调用，注入持久化管理器
    pub fn set_persistence_manager(&mut self, pm: Arc<Mutex<PersistenceManager>>) {
        self.persistence_manager = Some(pm);
        info!("下载管理器已设置持久化管理器");
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

        self.create_task_internal(fs_id, remote_path, local_path, total_size, conflict_strategy)
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
        self.create_task_internal(fs_id, remote_path, local_path, total_size, conflict_strategy)
            .await
    }

    /// 内部方法：创建下载任务
    async fn create_task_internal(
        &self,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
    ) -> Result<String> {
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

                self.publish_event(DownloadEvent::Skipped {
                    task_id: format!("skipped-{}", uuid::Uuid::new_v4()),
                    filename,
                    reason: "文件已存在".to_string(),
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

        let mut task = DownloadTask::new(fs_id, remote_path.clone(), final_local_path.clone(), total_size);

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

        // 🔥 发送任务创建事件
        self.publish_event(DownloadEvent::Created {
            task_id: task_id.clone(),
            fs_id,
            remote_path,
            local_path: final_local_path.to_string_lossy().to_string(),
            total_size,
            group_id,
            is_backup: false,
            original_filename,
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

        // 🔥 关键修复：文件夹子任务必须检查是否有槽位，没有槽位不能启动
        if is_folder_task {
            // 检查任务是否有槽位
            let has_slot = {
                let t = task.lock().await;
                t.slot_id.is_some()
            };

            if !has_slot {
                // 🔥 文件夹子任务没有槽位，不能启动，加入等待队列
                // 使用优先级方法：文件夹子任务优先级介于普通任务和备份任务之间
                warn!(
                    "文件夹子任务 {} 没有槽位，无法启动，加入等待队列",
                    task_id
                );
                self.add_to_waiting_queue_with_task_type(task_id, false, true).await;
                return Ok(());
            }

            info!("文件夹子任务 {} 有槽位，继续启动", task_id);
        }

        // 🔥 尝试分配固定任务位（文件夹子任务由 FolderManager 管理槽位，这里跳过）
        if !is_folder_task {
            // 获取任务是否为备份任务和 group_id（用于槽位刷新）
            let (is_backup, start_task_group_id) = {
                let t = task.lock().await;
                (t.is_backup, t.group_id.clone())
            };
            let start_task_touch_id = start_task_group_id.unwrap_or_else(|| task_id.to_string());

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
                    // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                    self.task_slot_pool.touch_slot(&start_task_touch_id).await;
                    info!("备份任务 {} 获得任务位: slot_id={}，已刷新槽位时间戳", task_id, slot_id);
                } else {
                    // 🔥 备份任务无可用槽位，加入等待队列末尾（最低优先级）
                    self.add_to_waiting_queue_by_priority(task_id, true).await;
                    info!(
                        "备份任务 {} 无可用任务位，加入等待队列末尾 (已用槽位: {}/{})",
                        task_id,
                        self.task_slot_pool.used_slots().await,
                        self.max_concurrent_tasks
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

                        // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                        self.task_slot_pool.touch_slot(&start_task_touch_id).await;

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

                                // 尝试回收一个借调槽位
                                if let Some(reclaimed_slot_id) = fm.reclaim_borrowed_slot().await {
                                    // 回收成功，分配槽位给新任务
                                    if let Some((slot_id, preempted_task_id)) = self.task_slot_pool.allocate_fixed_slot_with_priority(
                                        task_id, false, TaskPriority::Normal
                                    ).await {
                                        {
                                            let mut t = task.lock().await;
                                            t.slot_id = Some(slot_id);
                                            t.is_borrowed_slot = false;
                                        }
                                        // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                                        self.task_slot_pool.touch_slot(&start_task_touch_id).await;
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
                                        self.max_concurrent_tasks
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
                                    self.max_concurrent_tasks
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
                                self.max_concurrent_tasks
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

    /// 处理任务准备或注册失败的统一逻辑
    ///
    /// - 对于文件夹子任务：重置为 Pending 状态并放回等待队列，等待下次重试
    /// - 对于单文件任务：标记失败并发送失败事件
    async fn handle_task_failure(
        task_id: String,
        task: Arc<Mutex<DownloadTask>>,
        error_msg: String,
        waiting_queue: Arc<RwLock<VecDeque<String>>>,
        cancellation_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
        ws_manager: Option<Arc<WebSocketManager>>,
        persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,
        tasks: Arc<RwLock<HashMap<String, Arc<Mutex<DownloadTask>>>>>,
    ) {
        // 获取 group_id 和 is_backup，判断是否为文件夹子任务
        let (group_id, is_backup) = {
            let t = task.lock().await;
            (t.group_id.clone(), t.is_backup)
        };

        if group_id.is_some() {
            // 🔥 文件夹子任务：不标记失败，重新放回等待队列等待重试
            warn!(
                "文件夹子任务 {} 失败（{}），重新放回等待队列等待下次重试",
                task_id, error_msg
            );

            // 将任务状态重置为 Pending，保留错误信息供诊断
            {
                let mut t = task.lock().await;
                t.status = TaskStatus::Pending;
                t.error = Some(error_msg);
            }

            // 🔥 使用优先级方法重新放回等待队列（文件夹子任务插入到备份任务之前）
            Self::add_to_queue_by_priority(&waiting_queue, &tasks, &task_id, is_backup, true).await;

            // 移除取消令牌，避免泄漏
            cancellation_tokens.write().await.remove(&task_id);
        } else {
            // 🔥 单文件任务：标记失败（保持原有逻辑）
            {
                let mut t = task.lock().await;
                t.mark_failed(error_msg.clone());
            }

            // 发布任务失败事件
            if let Some(ref ws) = ws_manager {
                ws.send_if_subscribed(
                    TaskEvent::Download(DownloadEvent::Failed {
                        task_id: task_id.clone(),
                        error: error_msg.clone(),
                        group_id: None,
                        is_backup,
                    }),
                    None,
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

        // 🔥 关键修复：检查任务是否有槽位
        // 任务必须要有任务槽（slot_id）才能下载
        let (has_slot, is_folder_task) = {
            let t = task.lock().await;
            (t.slot_id.is_some(), t.group_id.is_some())
        };

        // 🔥 文件夹子任务必须有槽位才能启动
        if is_folder_task && !has_slot {
            warn!(
                "文件夹子任务 {} 没有槽位，无法启动，加入等待队列",
                task_id
            );
            // 🔥 使用优先级方法：文件夹子任务优先级介于普通任务和备份任务之间
            self.add_to_waiting_queue_with_task_type(task_id, false, true).await;
            return Ok(());
        }

        info!("启动下载任务: {} (has_slot={})", task_id, has_slot);

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

        tokio::spawn(async move {
            // 获取 WebSocket 管理器和文件夹进度发送器
            let ws_manager = ws_manager_arc.read().await.clone();
            let folder_progress_tx = folder_progress_tx_arc.read().await.clone();
            let backup_notification_tx = backup_notification_tx_arc.read().await.clone();
            let snapshot_manager = snapshot_manager_arc.read().await.clone(); // 🔥 获取快照管理器
            let encryption_config_store = encryption_config_store_arc.read().await.clone(); // 🔥 获取加密配置存储
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
                        )
                    };

                    // 获取分片数
                    let total_chunks = {
                        let cm = chunk_manager.lock().await;
                        cm.chunk_count()
                    };

                    // 🔥 prepare_for_scheduling 完成后立即刷新槽位，防止探测阶段耗时过长导致超时
                    {
                        let prepare_touch_id = group_id.clone().unwrap_or_else(|| task_id_clone.clone());
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
                        ) {
                            warn!("注册任务到持久化管理器失败: {}", e);
                        } else {
                            info!(
                                "任务 {} 已注册到持久化管理器 ({} 个分片, is_backup={}, transfer_task_id={:?})",
                                task_id_clone, total_chunks, is_backup, transfer_task_id
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
                    };

                    // 注册到调度器
                    match chunk_scheduler.register_task(task_info).await {
                        Ok(()) => {
                            // 注册成功，启动速度异常检测循环和线程停滞检测循环
                            info!("任务 {} 注册成功，启动CDN链接检测", task_id_clone);

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

                            // 统一处理任务失败逻辑
                            Self::handle_task_failure(
                                task_id_clone,
                                task_clone,
                                error_msg,
                                waiting_queue,
                                cancellation_tokens,
                                ws_manager,
                                persistence_manager,
                                tasks_clone,
                            )
                                .await;

                            // 不在这里调用 try_start_waiting_tasks，避免循环引用
                        }
                    }
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    error!("准备任务失败: {}", error_msg);

                    // 统一处理任务失败逻辑
                    Self::handle_task_failure(
                        task_id_clone,
                        task_clone,
                        error_msg,
                        waiting_queue,
                        cancellation_tokens,
                        ws_manager,
                        persistence_manager,
                        tasks_clone,
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
        loop {
            // 检查是否有可用任务槽
            let available_slots = self.task_slot_pool.available_slots().await;
            if available_slots == 0 {
                break;
            }

            // 从等待队列取出任务
            let task_id = {
                let mut queue = self.waiting_queue.write().await;
                queue.pop_front()
            };

            match task_id {
                Some(id) => {
                    // 🔥 获取任务信息：是否为备份任务、是否需要槽位、是否为文件夹子任务、group_id
                    let (is_backup, needs_slot, is_folder_subtask, try_start_group_id) = {
                        if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                            let t = task.lock().await;
                            (t.is_backup, t.slot_id.is_none(), t.group_id.is_some(), t.group_id.clone())
                        } else {
                            // 任务不存在，跳过
                            warn!("等待队列中的任务 {} 不存在，跳过", id);
                            continue;
                        }
                    };
                    let try_start_touch_id = try_start_group_id.unwrap_or_else(|| id.clone());

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

                    info!("⚡ 启动等待队列任务: {} (可用槽位: {}, is_backup: {})", id, available_slots, is_backup);

                    if needs_slot {
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
                                // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                                self.task_slot_pool.touch_slot(&try_start_touch_id).await;
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

                                    // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                                    self.task_slot_pool.touch_slot(&try_start_touch_id).await;

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

                // 检查是否有可用任务槽
                let available_slots = task_slot_pool.available_slots().await;
                if available_slots == 0 {
                    continue;
                }

                // 尝试启动等待任务
                loop {
                    // 检查是否有可用任务槽
                    let available_slots = task_slot_pool.available_slots().await;
                    if available_slots == 0 {
                        break;
                    }

                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            info!("🔄 后台监控：从等待队列启动任务 {} (可用槽位: {})", id, available_slots);

                            // 获取任务
                            let task = tasks.read().await.get(&id).cloned();
                            if let Some(task) = task {
                                // 🔥 获取任务信息：是否需要槽位、是否为备份任务、是否为文件夹子任务、group_id
                                let (needs_slot, is_backup, is_folder_subtask, monitor_group_id) = {
                                    let t = task.lock().await;
                                    (t.slot_id.is_none(), t.is_backup, t.group_id.is_some(), t.group_id.clone())
                                };
                                let monitor_touch_id = monitor_group_id.unwrap_or_else(|| id.clone());

                                if needs_slot {
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

                                        // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                                        task_slot_pool.touch_slot(&monitor_touch_id).await;

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

                                tokio::spawn(async move {
                                    // 获取 WebSocket 管理器和文件夹进度发送器
                                    let ws_manager = ws_manager_arc_clone.read().await.clone();
                                    let folder_progress_tx =
                                        folder_progress_tx_arc_clone.read().await.clone();
                                    let backup_notification_tx =
                                        backup_notification_tx_arc_clone.read().await.clone();
                                    let snapshot_manager = snapshot_manager_arc_clone.read().await.clone(); // 🔥 获取快照管理器
                                    let encryption_config_store = encryption_config_store_arc_clone.read().await.clone(); // 🔥 获取加密配置存储
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
                                                )
                                            };

                                            // 获取分片数
                                            let total_chunks = {
                                                let cm = chunk_manager.lock().await;
                                                cm.chunk_count()
                                            };

                                            // 🔥 prepare_for_scheduling 完成后立即刷新槽位，防止探测阶段耗时过长导致超时
                                            {
                                                let prepare_touch_id = group_id.clone().unwrap_or_else(|| id_clone.clone());
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

                                                    // 统一处理任务失败逻辑
                                                    Self::handle_task_failure(
                                                        id_clone,
                                                        task_clone,
                                                        error_msg,
                                                        waiting_queue_clone,
                                                        cancellation_tokens_clone,
                                                        ws_manager,
                                                        persistence_manager_clone,
                                                        tasks_clone,
                                                    )
                                                        .await;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let error_msg = e.to_string();
                                            error!("后台监控：准备任务失败: {}", error_msg);

                                            // 统一处理任务失败逻辑
                                            Self::handle_task_failure(
                                                id_clone,
                                                task_clone,
                                                error_msg,
                                                waiting_queue_clone,
                                                cancellation_tokens_clone,
                                                ws_manager,
                                                persistence_manager_clone,
                                                tasks_clone,
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
        tokio::spawn(async move {
            while let Some(task_id) = rx.recv().await {
                info!("收到槽位超时释放通知，将任务设置为失败: {}", task_id);

                // 更新任务状态为失败
                let tasks_guard = tasks.read().await;
                if let Some(task) = tasks_guard.get(&task_id) {
                    let mut t = task.lock().await;
                    t.status = crate::downloader::TaskStatus::Failed;
                    t.error = Some("槽位超时释放：任务长时间无进度更新，可能已卡住".to_string());
                    // 🔥 清除已释放的槽位ID，避免重试时误以为还持有槽位
                    t.slot_id = None;

                    // 发送 WebSocket 通知
                    let ws_guard = ws_manager.read().await;
                    if let Some(ref ws) = *ws_guard {
                        use crate::server::events::{TaskEvent, DownloadEvent};
                        ws.send_if_subscribed(
                            TaskEvent::Download(DownloadEvent::Failed {
                                task_id: task_id.clone(),
                                error: "槽位超时释放：任务长时间无进度更新，可能已卡住".to_string(),
                                group_id: t.group_id.clone(),
                                is_backup: t.is_backup,
                            }),
                            t.group_id.clone(),
                        );
                    }
                    // 🔥 通知文件夹管理器子任务失败
                    let group_id = t.group_id.clone();
                    let total_size = t.total_size;
                    drop(t);
                    if let Some(gid) = group_id {
                        chunk_scheduler.notify_subtask_failed(gid, task_id.clone(), total_size).await;
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

                // 检查是否有可用任务槽
                let available_slots = task_slot_pool.available_slots().await;
                if available_slots == 0 {
                    continue;
                }

                info!("⚡ 收到任务完成信号，立即启动等待任务 (可用槽位: {})", available_slots);

                // 尝试启动等待任务（与 start_waiting_queue_monitor 逻辑相同）
                loop {
                    // 检查是否有可用任务槽
                    let available_slots = task_slot_pool.available_slots().await;
                    if available_slots == 0 {
                        break;
                    }

                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            info!("⚡ 0延迟启动：从等待队列启动任务 {} (可用槽位: {})", id, available_slots);

                            // 获取任务
                            let task = tasks.read().await.get(&id).cloned();
                            if let Some(task) = task {
                                // 🔥 获取任务信息：是否需要槽位、是否为备份任务、是否为文件夹子任务、group_id
                                let (needs_slot, is_backup, is_folder_subtask, zero_delay_group_id) = {
                                    let t = task.lock().await;
                                    (t.slot_id.is_none(), t.is_backup, t.group_id.is_some(), t.group_id.clone())
                                };
                                let zero_delay_touch_id = zero_delay_group_id.unwrap_or_else(|| id.clone());

                                if needs_slot {
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

                                        // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                                        task_slot_pool.touch_slot(&zero_delay_touch_id).await;

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

                                tokio::spawn(async move {
                                    // 获取 WebSocket 管理器和文件夹进度发送器
                                    let ws_manager = ws_manager_arc_clone.read().await.clone();
                                    let folder_progress_tx =
                                        folder_progress_tx_arc_clone.read().await.clone();
                                    let backup_notification_tx =
                                        backup_notification_tx_arc_clone.read().await.clone();
                                    let snapshot_manager = snapshot_manager_arc_clone.read().await.clone(); // 🔥 获取快照管理器
                                    let encryption_config_store = encryption_config_store_arc_clone.read().await.clone(); // 🔥 获取加密配置存储

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
                                                )
                                            };

                                            // 获取分片数
                                            let total_chunks = {
                                                let cm = chunk_manager.lock().await;
                                                cm.chunk_count()
                                            };

                                            // 🔥 prepare_for_scheduling 完成后立即刷新槽位，防止探测阶段耗时过长导致超时
                                            {
                                                let prepare_touch_id = group_id.clone().unwrap_or_else(|| id_clone.clone());
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
                                                    // 🔥 释放已分配的槽位
                                                    let (slot_id, is_backup, is_folder_subtask, retry_count) = {
                                                        let t = task_clone.lock().await;
                                                        (t.slot_id, t.is_backup, t.group_id.is_some(), t.start_retry_count)
                                                    };
                                                    if let Some(sid) = slot_id {
                                                        task_slot_pool_clone.release_fixed_slot(&id_clone).await;
                                                        info!("0延迟启动：注册失败，释放槽位 {} (任务: {})", sid, id_clone);
                                                    }

                                                    // 🔥 最大重试次数限制
                                                    const MAX_START_RETRIES: u32 = 3;

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
                                            // 🔥 释放已分配的槽位
                                            let (slot_id, is_backup, is_folder_subtask, retry_count) = {
                                                let t = task_clone.lock().await;
                                                (t.slot_id, t.is_backup, t.group_id.is_some(), t.start_retry_count)
                                            };
                                            if let Some(sid) = slot_id {
                                                task_slot_pool_clone.release_fixed_slot(&id_clone).await;
                                                info!("0延迟启动：准备失败，释放槽位 {} (任务: {})", sid, id_clone);
                                            }

                                            // 🔥 最大重试次数限制
                                            const MAX_START_RETRIES: u32 = 3;

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

        if t.status != TaskStatus::Downloading {
            anyhow::bail!("任务未在下载中");
        }

        // 🔥 保存旧状态用于发布 StatusChanged
        let old_status = format!("{:?}", t.status).to_lowercase();

        // 🔥 获取槽位信息，用于释放槽位
        let slot_id = t.slot_id;
        let is_borrowed = t.is_borrowed_slot;

        t.mark_paused();

        // 🔥 清除任务的槽位信息（暂停后需要重新获取槽位）
        t.slot_id = None;
        t.is_borrowed_slot = false;

        info!("暂停下载任务: {}", task_id);
        drop(t);

        // 🔥 活跃计数 -1（Downloading → Paused）
        self.dec_active();

        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        // 🔥 释放槽位（暂停时释放，让其他任务可以使用）
        if let Some(sid) = slot_id {
            if is_borrowed {
                // 借调位：由 FolderManager 管理，这里只记录日志
                // 注意：文件夹子任务的借调位释放应该由 FolderManager 处理
                info!("任务 {} 暂停，使用借调位 {}（由FolderManager管理）", task_id, sid);
            } else {
                // 固定位：直接释放
                self.task_slot_pool.release_fixed_slot(task_id).await;
                info!("任务 {} 暂停，释放固定槽位 {}", task_id, sid);
            }
        }

        // 🔥 问题2修复：先持久化状态，再发送事件
        // 确保前端收到消息时，状态已经保存到磁盘（与 pause_folder 保持一致）
        if let Some(ref pm) = self.persistence_manager {
            use crate::persistence::types::TaskPersistenceStatus;
            if let Err(e) = crate::persistence::metadata::update_metadata(
                &pm.lock().await.wal_dir(),
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
        })
            .await;

        // 🔥 发送暂停事件
        self.publish_event(DownloadEvent::Paused {
            task_id: task_id.to_string(),
            group_id,
            is_backup,
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
        let (group_id, is_backup) = {
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
            (t.group_id.clone(), t.is_backup)
        };

        // 🔥 持久化状态
        if let Some(ref pm) = self.persistence_manager {
            use crate::persistence::types::TaskPersistenceStatus;
            if let Err(e) = crate::persistence::metadata::update_metadata(
                &pm.lock().await.wal_dir(),
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
                    task.mark_paused();
                    paused_count += 1;

                    debug!(
                        "等待队列中的下载任务 {} 已暂停 (原状态: {})",
                        task_id, old_status
                    );

                    drop(task);

                    // 发送状态变更事件
                    self.publish_event(DownloadEvent::StatusChanged {
                        task_id: task_id.to_string(),
                        old_status,
                        new_status: "paused".to_string(),
                        group_id: group_id.clone(),
                        is_backup,
                    })
                        .await;

                    // 发送暂停事件
                    self.publish_event(DownloadEvent::Paused {
                        task_id: task_id.to_string(),
                        group_id,
                        is_backup,
                    })
                        .await;
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
        let (group_id, is_backup) = {
            let mut t = task.lock().await;
            if t.status != TaskStatus::Downloading {
                warn!("暂停被抢占任务失败：任务 {} 不在下载中，当前状态: {:?}", task_id, t.status);
                return;
            }
            let group_id = t.group_id.clone();
            let is_backup = t.is_backup;
            t.mark_paused();
            // 清除槽位信息（槽位已被抢占）
            t.slot_id = None;
            t.is_borrowed_slot = false;
            (group_id, is_backup)
        };

        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        // 🔥 持久化暂停状态
        if let Some(ref pm) = self.persistence_manager {
            use crate::persistence::types::TaskPersistenceStatus;
            if let Err(e) = crate::persistence::metadata::update_metadata(
                &pm.lock().await.wal_dir(),
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
        })
            .await;

        // 🔥 发送暂停事件
        self.publish_event(DownloadEvent::Paused {
            task_id: task_id.to_string(),
            group_id,
            is_backup,
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
            group_id = t.group_id.clone();
            is_backup = t.is_backup;
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
        })
            .await;

        // 🔥 发送恢复事件
        self.publish_event(DownloadEvent::Resumed {
            task_id: task_id.to_string(),
            group_id,
            is_backup,
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

        // 检查任务是否已有槽位（文件夹子任务可能已分配）
        let (has_slot, is_folder_subtask, resume_group_id) = {
            let t = task.lock().await;
            (t.slot_id.is_some(), t.group_id.is_some(), t.group_id.clone())
        };
        let resume_touch_id = resume_group_id.unwrap_or_else(|| task_id.to_string());

        // 如果任务没有槽位（单文件任务），尝试分配或回收
        if !has_slot {
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
                    // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                    self.task_slot_pool.touch_slot(&resume_touch_id).await;
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

                        // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                        self.task_slot_pool.touch_slot(&resume_touch_id).await;

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

                                // 尝试回收一个借调槽位
                                if let Some(reclaimed_slot_id) = fm.reclaim_borrowed_slot().await {
                                    // 回收成功，分配槽位给恢复的任务（使用正确的优先级）
                                    if let Some((slot_id, preempted_task_id)) = self.task_slot_pool.allocate_fixed_slot_with_priority(
                                        task_id, false, priority
                                    ).await {
                                        {
                                            let mut t = task.lock().await;
                                            t.slot_id = Some(slot_id);
                                            t.is_borrowed_slot = false;
                                        }
                                        // 🔥 槽位分配后立即刷新，防止长准备阶段被误判超时
                                        self.task_slot_pool.touch_slot(&resume_touch_id).await;
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
            group_id = t.group_id.clone();
            is_backup = t.is_backup;

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
        let (group_id, is_backup, was_active) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                let active = matches!(
                    t.status,
                    TaskStatus::Pending | TaskStatus::Downloading | TaskStatus::Decrypting
                );
                (t.group_id.clone(), t.is_backup, active)
            } else {
                // 任务不在内存，尝试从持久化管理器读取
                if let Some(ref pm) = self.persistence_manager {
                    let pm_guard = pm.lock().await;
                    if let Some(metadata) = pm_guard.get_history_task(task_id) {
                        (metadata.group_id.clone(), metadata.is_backup, false)
                    } else {
                        (None, false, false)
                    }
                } else {
                    (None, false, false)
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

        // 🔥 释放任务槽位（在移除任务前获取槽位信息）
        let (slot_id_to_release, is_borrowed) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                (t.slot_id, t.is_borrowed_slot)
            } else {
                (None, false)
            }
        };

        // 释放固定槽位（单文件任务）
        if let Some(slot_id) = slot_id_to_release {
            if !is_borrowed {
                // 单文件任务：释放固定位
                self.task_slot_pool.release_fixed_slot(task_id).await;
                info!("任务 {} 删除，释放固定槽位 {}", task_id, slot_id);
            } else {
                // 借调位不在这里释放，由 FolderManager 管理
                info!("任务 {} 删除，使用借调位 {}（由FolderManager管理）", task_id, slot_id);
            }
        }

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
        }

        // 🔥 清理持久化文件
        if let Some(ref pm) = self.persistence_manager {
            if let Err(e) = pm.lock().await.on_task_deleted(task_id) {
                warn!("清理任务持久化文件失败: {}", e);
            }
        }

        // 🔥 发送删除事件（携带 group_id）
        self.publish_event(DownloadEvent::Deleted {
            task_id: task_id.to_string(),
            group_id,
            is_backup,
        })
            .await;

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
        let (group_id, is_backup, was_active) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                let active = matches!(
                    t.status,
                    TaskStatus::Pending | TaskStatus::Downloading | TaskStatus::Decrypting
                );
                (t.group_id.clone(), t.is_backup, active)
            } else {
                (None, false, false)
            }
        };

        // 从调度器取消
        self.chunk_scheduler.cancel_task(task_id).await;

        // 释放槽位
        let (slot_id_to_release, is_borrowed) = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                (t.slot_id, t.is_borrowed_slot)
            } else {
                (None, false)
            }
        };

        if let Some(_slot_id) = slot_id_to_release {
            if !is_borrowed {
                self.task_slot_pool.release_fixed_slot(task_id).await;
            }
        }

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
        })
            .await;

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

    /// 🔥 更新任务的槽位信息
    ///
    /// 用于恢复时为子任务分配借调位后更新任务状态
    pub async fn update_task_slot(&self, task_id: &str, slot_id: usize, is_borrowed: bool) {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(task_id) {
            let mut t = task.lock().await;
            t.slot_id = Some(slot_id);
            t.is_borrowed_slot = is_borrowed;
            info!(
                "更新任务 {} 槽位信息: slot_id={}, is_borrowed={}",
                task_id, slot_id, is_borrowed
            );
        }
    }

    /// 🔥 将任务设为 Pending 状态并加入等待队列
    ///
    /// 用于文件夹任务恢复时，没有槽位的子任务应该变成等待状态而不是保持暂停状态
    pub async fn set_task_pending_and_queue(&self, task_id: &str) -> Result<()> {
        // 更新任务状态为 Pending，同时获取 group_id 和 is_backup
        let (old_status, group_id, is_backup) = {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                let mut t = task.lock().await;
                let old = format!("{:?}", t.status).to_lowercase();
                let gid = t.group_id.clone();
                let backup = t.is_backup;
                if t.status == TaskStatus::Paused {
                    t.status = TaskStatus::Pending;
                    info!("任务 {} 状态从 Paused 改为 Pending（等待槽位）", task_id);
                }
                (old, gid, backup)
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
    ///
    /// # 返回
    /// 任务ID
    pub async fn create_backup_task(
        &self,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        backup_config_id: String,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
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
        let task = DownloadTask::new_backup(
            fs_id,
            remote_path.clone(),
            final_local_path.clone(),
            total_size,
            backup_config_id.clone(),
        );
        let task_id = task.id.clone();

        info!(
            "创建备份下载任务: id={}, remote={}, local={:?}, size={}, backup_config={}, strategy={:?}",
            task_id, remote_path, final_local_path, total_size, backup_config_id, strategy
        );

        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc);

        // 🔥 活跃计数 +1（新建备份任务为 Pending）
        self.inc_active();

        // 🔥 发送任务创建事件（备份任务，is_backup=true）
        self.publish_event(DownloadEvent::Created {
            task_id: task_id.clone(),
            fs_id,
            remote_path,
            local_path: final_local_path.to_string_lossy().to_string(),
            total_size,
            group_id: None,
            is_backup: true,
            original_filename: None, // 备份下载任务不需要原始文件名
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
                    t.mark_paused();
                    drop(t);

                    self.dec_active();

                    // 移除取消令牌（如果有）
                    self.cancellation_tokens.write().await.remove(id);

                    // 持久化暂停状态
                    if let Some(ref pm) = self.persistence_manager {
                        use crate::persistence::types::TaskPersistenceStatus;
                        if let Err(e) = crate::persistence::metadata::update_metadata(
                            &pm.lock().await.wal_dir(),
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
                    })
                        .await;

                    self.publish_event(DownloadEvent::Paused {
                        task_id: id.to_string(),
                        group_id,
                        is_backup,
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
        let old_max = self.max_concurrent_tasks;

        // 更新调度器的限制
        self.chunk_scheduler.update_max_concurrent_tasks(new_max);

        // 🔥 动态调整任务位池容量
        self.task_slot_pool.resize(new_max).await;

        // 更新 manager 自己的记录（因为 max_concurrent_tasks 不是 Arc 包装的）
        // 注意：这里有个限制，因为 self 是 &self，我们不能修改 max_concurrent_tasks
        // 但调度器和任务位池已经更新了，这个字段只在创建时使用，之后都用调度器的值

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

        // 2. 获取该 group 的所有任务 ID
        let task_ids: Vec<String> = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .filter_map(|(id, task_arc)| {
                    // 使用 try_lock 避免死锁
                    if let Ok(task) = task_arc.try_lock() {
                        if task.group_id.as_deref() == Some(group_id) {
                            return Some(id.clone());
                        }
                    }
                    None
                })
                .collect()
        };

        info!(
            "取消文件夹 {} 的 {} 个任务（包括探测中的）",
            group_id,
            task_ids.len()
        );

        // 3. 对每个任务：触发取消令牌 + 从调度器取消 + 更新状态
        for task_id in &task_ids {
            // 触发取消令牌（让正在探测的任务知道应该停止）
            {
                let tokens = self.cancellation_tokens.read().await;
                if let Some(token) = tokens.get(task_id) {
                    token.cancel();
                }
            }

            // 从调度器取消（已注册的任务）
            self.chunk_scheduler.cancel_task(task_id).await;

            // 更新任务状态为 Paused
            {
                let tasks = self.tasks.read().await;
                if let Some(task_arc) = tasks.get(task_id) {
                    let mut task = task_arc.lock().await;
                    if task.status == TaskStatus::Downloading || task.status == TaskStatus::Pending
                    {
                        task.mark_paused();
                    }
                }
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

        // 创建恢复任务（使用 Paused 状态）
        // 🔥 根据是否为备份任务选择不同的构造方式
        let mut task = if recovery_info.is_backup {
            DownloadTask::new_backup(
                recovery_info.fs_id,
                recovery_info.remote_path.clone(),
                recovery_info.local_path.clone(),
                recovery_info.file_size,
                recovery_info.backup_config_id.clone().unwrap_or_default(),
            )
        } else {
            DownloadTask::new(
                recovery_info.fs_id,
                recovery_info.remote_path.clone(),
                recovery_info.local_path.clone(),
                recovery_info.file_size,
            )
        };

        // 恢复任务 ID（保持原有 ID）
        task.id = task_id.clone();

        // 设置为暂停状态（等待用户手动恢复）
        task.status = TaskStatus::Paused;

        // 计算已下载大小
        let completed_count = recovery_info.completed_chunks.len();
        let downloaded_size = if completed_count > 0 {
            // 估算已下载大小：完成的分片数 * 分片大小
            // 注意：最后一个分片可能较小，这里是近似值
            let full_chunks = completed_count.saturating_sub(1);
            let full_size = (full_chunks as u64) * recovery_info.chunk_size;

            // 检查最后一个分片是否完成
            let last_chunk_index = recovery_info.total_chunks.saturating_sub(1);
            let last_chunk_size = if recovery_info.completed_chunks.contains(last_chunk_index) {
                // 最后一个分片的大小
                recovery_info
                    .file_size
                    .saturating_sub(last_chunk_index as u64 * recovery_info.chunk_size)
            } else {
                0
            };

            full_size + last_chunk_size
        } else {
            0
        };
        task.downloaded_size = downloaded_size;
        task.created_at = recovery_info.created_at;

        // 恢复文件夹下载组信息
        task.group_id = recovery_info.group_id.clone();
        task.group_root = recovery_info.group_root.clone();
        task.relative_path = recovery_info.relative_path.clone();

        // 🔥 恢复跨任务跳转字段
        task.transfer_task_id = recovery_info.transfer_task_id.clone();

        info!(
            "恢复下载任务: id={}, 文件={:?}, 已完成 {}/{} 分片 ({:.1}%), group_id={:?}{}",
            task_id,
            recovery_info.local_path,
            completed_count,
            recovery_info.total_chunks,
            if recovery_info.total_chunks > 0 {
                (completed_count as f64 / recovery_info.total_chunks as f64) * 100.0
            } else {
                0.0
            },
            recovery_info.group_id,
            if recovery_info.is_backup { "（备份任务）" } else { "" }
        );

        // 🔥 判断是否为单文件任务（无 group_id），需要分配固定任务位
        let is_single_file = recovery_info.group_id.is_none();

        // 添加到任务列表
        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc.clone());

        // 🔥 暂停状态的任务不分配槽位，等待用户手动恢复时再分配
        // 这样可以让正在下载的任务借用更多槽位
        if is_single_file {
            info!("单文件任务 {} 恢复完成 (暂停状态，不占用槽位)", task_id);
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
        }
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

        assert_eq!(manager.download_dir().await, temp_dir.path());
        assert_eq!(manager.get_all_tasks().await.len(), 0);
    }

    #[tokio::test]
    async fn test_create_task() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

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
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

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
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

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
}
