use crate::auth::UserAuth;
use crate::common::{
    RefreshCoordinator, RefreshCoordinatorConfig, SpeedAnomalyConfig, StagnationConfig,
};
use crate::downloader::{
    calculate_task_max_chunks, ChunkScheduler, DownloadEngine, DownloadTask, TaskScheduleInfo,
    TaskStatus, FolderDownloadManager,
};
use crate::downloader::task_slot_pool::TaskSlotPool;
use crate::persistence::{
    DownloadRecoveryInfo, PersistenceManager, TaskMetadata, TaskPersistenceStatus, TaskType,
};
use crate::server::events::{DownloadEvent, ProgressThrottler, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::{Context, Result};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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
    /// 🔥 任务位池管理器
    task_slot_pool: Arc<TaskSlotPool>,
    /// 🔥 文件夹下载管理器引用（可选，用于回收借调槽位）
    folder_manager: Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
}

impl DownloadManager {
    /// 创建新的下载管理器
    pub fn new(user_auth: UserAuth, download_dir: PathBuf) -> Result<Self> {
        Self::with_config(user_auth, download_dir, 10, 5)
    }

    /// 使用指定配置创建下载管理器（不再需要 chunk_size 参数，引擎会自动计算）
    pub fn with_config(
        user_auth: UserAuth,
        download_dir: PathBuf,
        max_global_threads: usize,
        max_concurrent_tasks: usize,
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

        let engine = Arc::new(DownloadEngine::new(user_auth));

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
            task_slot_pool: Arc::new(TaskSlotPool::new(max_concurrent_tasks)),
            folder_manager: Arc::new(RwLock::new(None)),
        };

        // 启动后台任务：定期检查并启动等待队列中的任务
        manager.start_waiting_queue_monitor();

        // 🔥 设置任务完成触发器（0延迟启动等待任务）
        manager.setup_waiting_queue_trigger();

        Ok(manager)
    }

    /// 🔥 设置持久化管理器
    ///
    /// 由 AppState 在初始化时调用，注入持久化管理器
    pub fn set_persistence_manager(&mut self, pm: Arc<Mutex<PersistenceManager>>) {
        self.persistence_manager = Some(pm);
        info!("下载管理器已设置持久化管理器");
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
    ) -> Result<String> {
        let download_dir = self.download_dir.read().await;
        let local_path = download_dir.join(&filename);
        drop(download_dir);

        self.create_task_internal(fs_id, remote_path, local_path, total_size)
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
    ) -> Result<String> {
        let local_path = target_dir.join(&filename);
        self.create_task_internal(fs_id, remote_path, local_path, total_size)
            .await
    }

    /// 内部方法：创建下载任务
    async fn create_task_internal(
        &self,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
    ) -> Result<String> {
        // 确保目标目录存在
        if let Some(parent) = local_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).context("创建下载目录失败")?;
            }
        }

        // 检查文件是否已存在
        if local_path.exists() {
            warn!("文件已存在: {:?}，将覆盖", local_path);
        }

        let filename = local_path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let task = DownloadTask::new(fs_id, remote_path.clone(), local_path.clone(), total_size);
        let task_id = task.id.clone();
        let group_id = task.group_id.clone();

        info!("创建下载任务: id={}, 文件名={}", task_id, filename);

        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc);

        // 🔥 发送任务创建事件
        self.publish_event(DownloadEvent::Created {
            task_id: task_id.clone(),
            fs_id,
            remote_path,
            local_path: local_path.to_string_lossy().to_string(),
            total_size,
            group_id,
        })
            .await;

        Ok(task_id)
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
                warn!(
                    "文件夹子任务 {} 没有槽位，无法启动，加入等待队列",
                    task_id
                );
                self.waiting_queue
                    .write()
                    .await
                    .push_back(task_id.to_string());
                return Ok(());
            }

            info!("文件夹子任务 {} 有槽位，继续启动", task_id);
        }

        // 🔥 尝试分配固定任务位（文件夹子任务由 FolderManager 管理槽位，这里跳过）
        if !is_folder_task {
            // 单文件任务需要分配固定任务位
            let slot_id = self.task_slot_pool.allocate_fixed_slot(task_id, false).await;

            if let Some(slot_id) = slot_id {
                // 分配成功，记录槽位信息
                {
                    let mut t = task.lock().await;
                    t.slot_id = Some(slot_id);
                    t.is_borrowed_slot = false;
                }
                info!("任务 {} 获得固定任务位: slot_id={}", task_id, slot_id);
            } else {
                // 🔥 无可用任务位，先尝试回收文件夹的借调槽位
                let folder_manager = {
                    let fm = self.folder_manager.read().await;
                    fm.clone()
                };

                if let Some(fm) = folder_manager {
                    // 检查是否有借调槽位可回收
                    if self.task_slot_pool.find_folder_with_borrowed_slots().await.is_some() {
                        info!("任务 {} 无可用槽位，尝试回收文件夹借调槽位", task_id);

                        // 尝试回收一个借调槽位
                        if let Some(reclaimed_slot_id) = fm.reclaim_borrowed_slot().await {
                            // 回收成功，分配槽位给新任务
                            // 注意：回收后的槽位已被释放到槽位池，需要重新分配
                            if let Some(slot_id) = self.task_slot_pool.allocate_fixed_slot(task_id, false).await {
                                {
                                    let mut t = task.lock().await;
                                    t.slot_id = Some(slot_id);
                                    t.is_borrowed_slot = false;
                                }
                                info!("任务 {} 通过回收借调槽位获得任务位: slot_id={} (回收的槽位={})", task_id, slot_id, reclaimed_slot_id);
                                // 继续启动任务（不需要 return，直接进入下面的启动流程）
                            } else {
                                warn!("回收借调槽位成功但重新分配失败，任务 {} 加入等待队列", task_id);
                                self.waiting_queue
                                    .write()
                                    .await
                                    .push_back(task_id.to_string());
                                return Ok(());
                            }
                        } else {
                            // 回收失败，加入等待队列
                            info!("回收借调槽位失败，任务 {} 加入等待队列", task_id);
                            self.waiting_queue
                                .write()
                                .await
                                .push_back(task_id.to_string());

                            info!(
                                "任务 {} 无可用任务位，加入等待队列 (已用槽位: {}/{})",
                                task_id,
                                self.task_slot_pool.used_slots().await,
                                self.max_concurrent_tasks
                            );
                            return Ok(());
                        }
                    } else {
                        // 没有借调槽位可回收，直接加入等待队列
                        self.waiting_queue
                            .write()
                            .await
                            .push_back(task_id.to_string());

                        info!(
                            "任务 {} 无可用任务位且无借调槽位可回收，加入等待队列 (已用槽位: {}/{})",
                            task_id,
                            self.task_slot_pool.used_slots().await,
                            self.max_concurrent_tasks
                        );
                        return Ok(());
                    }
                } else {
                    // 无文件夹管理器，直接加入等待队列
                    self.waiting_queue
                        .write()
                        .await
                        .push_back(task_id.to_string());

                    info!(
                        "任务 {} 无可用任务位，加入等待队列 (已用槽位: {}/{})",
                        task_id,
                        self.task_slot_pool.used_slots().await,
                        self.max_concurrent_tasks
                    );
                    return Ok(());
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
    ) {
        // 获取 group_id，判断是否为文件夹子任务
        let group_id = {
            let t = task.lock().await;
            t.group_id.clone()
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

            // 重新放回等待队列尾部
            waiting_queue.write().await.push_back(task_id.clone());

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
            self.waiting_queue
                .write()
                .await
                .push_back(task_id.to_string());
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
        let waiting_queue = self.waiting_queue.clone();
        let task_slot_pool_clone = self.task_slot_pool.clone();

        tokio::spawn(async move {
            // 获取 WebSocket 管理器和文件夹进度发送器
            let ws_manager = ws_manager_arc.read().await.clone();
            let folder_progress_tx = folder_progress_tx_arc.read().await.clone();
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
                        )
                    };

                    // 获取分片数
                    let total_chunks = {
                        let cm = chunk_manager.lock().await;
                        cm.chunk_count()
                    };

                    // 🔥 发送状态变更事件：pending → downloading
                    // 此时 prepare_for_scheduling 已完成，任务状态已变为 Downloading
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Download(DownloadEvent::StatusChanged {
                                task_id: task_id_clone.clone(),
                                old_status: "pending".to_string(),
                                new_status: "downloading".to_string(),
                                group_id: group_id.clone(),
                            }),
                            group_id.clone(),
                        );
                    }

                    // 🔥 注册任务到持久化管理器
                    if let Some(ref pm) = persistence_manager {
                        if let Err(e) = pm.lock().await.register_download_task(
                            task_id_clone.clone(),
                            fs_id,
                            remote_path.clone(),
                            local_path,
                            total_size,
                            chunk_size,
                            total_chunks,
                            group_id,
                            group_root,
                            relative_path,
                        ) {
                            warn!("注册任务到持久化管理器失败: {}", e);
                        } else {
                            info!(
                                "任务 {} 已注册到持久化管理器 ({} 个分片)",
                                task_id_clone, total_chunks
                            );
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
                    let client_for_detection = client.clone();
                    let cancellation_token_for_detection = cancellation_token.clone();
                    let chunk_scheduler_for_detection = chunk_scheduler.clone();

                    // 🔥 获取任务的槽位信息
                    let (slot_id, is_borrowed_slot) = {
                        let t = task_clone.lock().await;
                        (t.slot_id, t.is_borrowed_slot)
                    };

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
                        // 🔥 任务位借调机制字段
                        slot_id,
                        is_borrowed_slot,
                        task_slot_pool: Some(task_slot_pool_clone.clone()),
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
                    info!("⚡ 启动等待队列任务: {} (可用槽位: {})", id, available_slots);

                    // 检查任务是否需要分配槽位
                    let needs_slot = {
                        if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                            let t = task.lock().await;
                            t.slot_id.is_none()
                        } else {
                            false
                        }
                    };

                    if needs_slot {
                        // 尝试分配任务槽
                        let slot_id = self.task_slot_pool.allocate_fixed_slot(&id, false).await;
                        if let Some(sid) = slot_id {
                            // 分配成功，更新任务槽位信息
                            if let Some(task) = self.tasks.read().await.get(&id).cloned() {
                                let mut t = task.lock().await;
                                t.slot_id = Some(sid);
                                t.is_borrowed_slot = false;
                                info!("为等待任务 {} 分配槽位: {}", id, sid);
                            }
                        } else {
                            // 分配失败，放回队列头部
                            warn!("无法为任务 {} 分配槽位，放回等待队列", id);
                            self.waiting_queue.write().await.push_front(id);
                            break;
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
                                // 🔥 检查任务是否需要分配槽位
                                let needs_slot = {
                                    let t = task.lock().await;
                                    t.slot_id.is_none()
                                };

                                if needs_slot {
                                    // 尝试分配任务槽
                                    let slot_id = task_slot_pool.allocate_fixed_slot(&id, false).await;
                                    if let Some(sid) = slot_id {
                                        // 分配成功，更新任务槽位信息
                                        let mut t = task.lock().await;
                                        t.slot_id = Some(sid);
                                        t.is_borrowed_slot = false;
                                        info!("后台监控：为任务 {} 分配槽位: {}", id, sid);
                                    } else {
                                        // 分配失败，放回队列头部
                                        warn!("后台监控：无法为任务 {} 分配槽位，放回等待队列", id);
                                        waiting_queue.write().await.push_front(id);
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
                                let waiting_queue_clone = waiting_queue.clone();
                                let task_slot_pool_clone = task_slot_pool.clone();

                                tokio::spawn(async move {
                                    // 获取 WebSocket 管理器和文件夹进度发送器
                                    let ws_manager = ws_manager_arc_clone.read().await.clone();
                                    let folder_progress_tx =
                                        folder_progress_tx_arc_clone.read().await.clone();
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
                                                )
                                            };

                                            // 获取分片数
                                            let total_chunks = {
                                                let cm = chunk_manager.lock().await;
                                                cm.chunk_count()
                                            };

                                            // 🔥 发送状态变更事件：pending → downloading
                                            // 此时 prepare_for_scheduling 已完成，任务状态已变为 Downloading
                                            if let Some(ref ws) = ws_manager {
                                                ws.send_if_subscribed(
                                                    TaskEvent::Download(DownloadEvent::StatusChanged {
                                                        task_id: id_clone.clone(),
                                                        old_status: "pending".to_string(),
                                                        new_status: "downloading".to_string(),
                                                        group_id: group_id.clone(),
                                                    }),
                                                    group_id.clone(),
                                                );
                                            }

                                            // 🔥 注册任务到持久化管理器
                                            if let Some(ref pm) = persistence_manager_clone {
                                                if let Err(e) =
                                                    pm.lock().await.register_download_task(
                                                        id_clone.clone(),
                                                        fs_id,
                                                        remote_path.clone(),
                                                        local_path,
                                                        total_size,
                                                        chunk_size,
                                                        total_chunks,
                                                        group_id,
                                                        group_root,
                                                        relative_path,
                                                    )
                                                {
                                                    warn!(
                                                        "后台监控：注册任务到持久化管理器失败: {}",
                                                        e
                                                    );
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
                                            let client_for_detection = client.clone();
                                            let cancellation_token_for_detection =
                                                cancellation_token.clone();
                                            let chunk_scheduler_for_detection =
                                                chunk_scheduler_clone.clone();

                                            // 🔥 获取任务的槽位信息
                                            let (slot_id, is_borrowed_slot) = {
                                                let t = task_clone.lock().await;
                                                (t.slot_id, t.is_borrowed_slot)
                                            };

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
                                                // 🔥 任务位借调机制字段
                                                slot_id,
                                                is_borrowed_slot,
                                                task_slot_pool: Some(task_slot_pool_clone.clone()),
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
                                // 🔥 检查任务是否需要分配槽位
                                let needs_slot = {
                                    let t = task.lock().await;
                                    t.slot_id.is_none()
                                };

                                if needs_slot {
                                    // 尝试分配任务槽
                                    let slot_id = task_slot_pool.allocate_fixed_slot(&id, false).await;
                                    if let Some(sid) = slot_id {
                                        // 分配成功，更新任务槽位信息
                                        let mut t = task.lock().await;
                                        t.slot_id = Some(sid);
                                        t.is_borrowed_slot = false;
                                        info!("0延迟启动：为任务 {} 分配槽位: {}", id, sid);
                                    } else {
                                        // 分配失败，放回队列头部
                                        warn!("0延迟启动：无法为任务 {} 分配槽位，放回等待队列", id);
                                        waiting_queue.write().await.push_front(id);
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
                                let task_slot_pool_clone = task_slot_pool.clone();

                                tokio::spawn(async move {
                                    // 获取 WebSocket 管理器和文件夹进度发送器
                                    let ws_manager = ws_manager_arc_clone.read().await.clone();
                                    let folder_progress_tx =
                                        folder_progress_tx_arc_clone.read().await.clone();

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
                                                )
                                            };

                                            // 获取分片数
                                            let total_chunks = {
                                                let cm = chunk_manager.lock().await;
                                                cm.chunk_count()
                                            };

                                            // 🔥 发送状态变更事件：pending → downloading
                                            // 此时 prepare_for_scheduling 已完成，任务状态已变为 Downloading
                                            if let Some(ref ws) = ws_manager {
                                                ws.send_if_subscribed(
                                                    TaskEvent::Download(DownloadEvent::StatusChanged {
                                                        task_id: id_clone.clone(),
                                                        old_status: "pending".to_string(),
                                                        new_status: "downloading".to_string(),
                                                        group_id: group_id.clone(),
                                                    }),
                                                    group_id.clone(),
                                                );
                                            }

                                            // 🔥 注册任务到持久化管理器
                                            if let Some(ref pm) = persistence_manager_clone {
                                                if let Err(e) =
                                                    pm.lock().await.register_download_task(
                                                        id_clone.clone(),
                                                        fs_id,
                                                        remote_path.clone(),
                                                        local_path,
                                                        total_size,
                                                        chunk_size,
                                                        total_chunks,
                                                        group_id,
                                                        group_root,
                                                        relative_path,
                                                    )
                                                {
                                                    warn!(
                                                        "0延迟启动：注册任务到持久化管理器失败: {}",
                                                        e
                                                    );
                                                }
                                            }

                                            let max_concurrent_chunks =
                                                calculate_task_max_chunks(total_size);
                                            info!(
                                                "0延迟任务 {} 文件大小 {} 字节, 最大并发分片数: {}",
                                                id_clone, total_size, max_concurrent_chunks
                                            );

                                            let url_health_for_detection = url_health.clone();
                                            let client_for_detection = client.clone();
                                            let cancellation_token_for_detection =
                                                cancellation_token.clone();
                                            let chunk_scheduler_for_detection =
                                                chunk_scheduler_clone.clone();

                                            // 🔥 获取任务的槽位信息
                                            let (slot_id, is_borrowed_slot) = {
                                                let t = task_clone.lock().await;
                                                (t.slot_id, t.is_borrowed_slot)
                                            };

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
                                                // 🔥 任务位借调机制字段
                                                slot_id,
                                                is_borrowed_slot,
                                                task_slot_pool: Some(task_slot_pool_clone.clone()),
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
                                                    let mut t = task_clone.lock().await;
                                                    t.mark_failed(e.to_string());
                                                    cancellation_tokens_clone
                                                        .write()
                                                        .await
                                                        .remove(&id_clone);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("0延迟启动：准备任务失败: {}", e);
                                            let mut t = task_clone.lock().await;
                                            t.mark_failed(e.to_string());
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

        // 🔥 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "paused".to_string(),
            group_id: group_id.clone(),
        })
            .await;

        // 🔥 发送暂停事件
        self.publish_event(DownloadEvent::Paused {
            task_id: task_id.to_string(),
            group_id,
        })
            .await;

        // 🔥 根据参数决定是否尝试启动等待队列中的任务
        if !skip_try_start_waiting {
            self.try_start_waiting_tasks().await;
        }

        Ok(())
    }

    /// 恢复下载任务
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

        // 检查任务状态并将 Paused 改回 Pending

        {
            let mut t = task.lock().await;
            if t.status != TaskStatus::Paused {
                anyhow::bail!("任务未暂停，当前状态: {:?}", t.status);
            }

            // 🔥 保存旧状态
            old_status = format!("{:?}", t.status).to_lowercase();

            // 将状态改回 Pending，准备重新启动
            // 注意：这里不能用 mark_downloading，因为还没获得资源
            t.status = TaskStatus::Pending;
            group_id = t.group_id.clone();
        }

        info!("用户请求恢复下载任务: {}", task_id);

        // 🔥 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "pending".to_string(),
            group_id: group_id.clone(),
        })
            .await;

        // 🔥 发送恢复事件
        self.publish_event(DownloadEvent::Resumed {
            task_id: task_id.to_string(),
            group_id,
        })
            .await;

        // 🔥 关键修复：恢复任务时，如果无可用槽位，尝试回收文件夹借调槽位
        // 这与 start_task 的逻辑保持一致

        // 检查任务是否已有槽位（文件夹子任务可能已分配）
        let has_slot = {
            let t = task.lock().await;
            t.slot_id.is_some()
        };

        // 如果任务没有槽位（单文件任务），尝试分配或回收
        if !has_slot {
            let slot_id = self.task_slot_pool.allocate_fixed_slot(task_id, false).await;

            if let Some(slot_id) = slot_id {
                // 分配成功
                {
                    let mut t = task.lock().await;
                    t.slot_id = Some(slot_id);
                    t.is_borrowed_slot = false;
                }
                info!("恢复任务 {} 获得固定任务位: slot_id={}", task_id, slot_id);
            } else {
                // 🔥 无可用任务位，先尝试回收文件夹的借调槽位
                let folder_manager = {
                    let fm = self.folder_manager.read().await;
                    fm.clone()
                };

                if let Some(fm) = folder_manager {
                    // 检查是否有借调槽位可回收
                    if self.task_slot_pool.find_folder_with_borrowed_slots().await.is_some() {
                        info!("恢复任务 {} 无可用槽位，尝试回收文件夹借调槽位", task_id);

                        // 尝试回收一个借调槽位
                        if let Some(reclaimed_slot_id) = fm.reclaim_borrowed_slot().await {
                            // 回收成功，分配槽位给恢复的任务
                            if let Some(slot_id) = self.task_slot_pool.allocate_fixed_slot(task_id, false).await {
                                {
                                    let mut t = task.lock().await;
                                    t.slot_id = Some(slot_id);
                                    t.is_borrowed_slot = false;
                                }
                                info!("恢复任务 {} 通过回收借调槽位获得任务位: slot_id={} (回收的槽位={})", task_id, slot_id, reclaimed_slot_id);
                                // 继续启动任务
                            } else {
                                warn!("回收借调槽位成功但重新分配失败，恢复任务 {} 加入等待队列", task_id);
                                self.waiting_queue
                                    .write()
                                    .await
                                    .push_back(task_id.to_string());
                                return Ok(());
                            }
                        } else {
                            // 回收失败，加入等待队列
                            info!("回收借调槽位失败，恢复任务 {} 加入等待队列", task_id);
                            self.waiting_queue
                                .write()
                                .await
                                .push_back(task_id.to_string());
                            return Ok(());
                        }
                    } else {
                        // 没有借调槽位可回收，加入等待队列
                        self.waiting_queue
                            .write()
                            .await
                            .push_back(task_id.to_string());
                        info!(
                            "恢复任务 {} 无可用槽位且无借调槽位可回收，加入等待队列",
                            task_id
                        );
                        return Ok(());
                    }
                } else {
                    // 无文件夹管理器，加入等待队列
                    self.waiting_queue
                        .write()
                        .await
                        .push_back(task_id.to_string());
                    info!("恢复任务 {} 无可用槽位，加入等待队列", task_id);
                    return Ok(());
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

            // 🔥 关键修复：清除槽位信息
            // 当任务被暂停并重新入队时，原来的槽位已经被释放（如借调位回收）
            // 必须清除 slot_id，否则 try_start_waiting_tasks 会认为任务已有槽位
            // 导致多个任务同时启动，超过最大并发数限制
            t.slot_id = None;
            t.is_borrowed_slot = false;
        }

        info!("重新入队暂停任务: {} (group: {:?}), 已清除槽位信息", task_id, group_id);

        // 🔥 智能插入位置：保持同一文件夹内子任务的顺序
        {
            let mut queue = self.waiting_queue.write().await;

            if let Some(ref gid) = group_id {
                // 🔥 收集同组任务的位置（找到同一文件夹的第一个等待任务）
                let same_group_pos = {
                    let tasks = self.tasks.read().await;
                    let mut positions = Vec::new();
                    for (pos, id) in queue.iter().enumerate() {
                        if let Some(task_arc) = tasks.get(id) {
                            // 使用 try_lock 避免死锁
                            if let Ok(t) = task_arc.try_lock() {
                                if t.group_id.as_deref() == Some(gid) {
                                    positions.push(pos);
                                }
                            }
                        }
                    }
                    positions.first().copied()
                };

                if let Some(pos) = same_group_pos {
                    // 插入到同组第一个任务之前
                    queue.insert(pos, task_id.to_string());
                    info!(
                        "任务 {} 插入到同组任务位置 {} (group: {})",
                        task_id, pos, gid
                    );
                } else {
                    // 没有同组任务，插入到队列前面（优先恢复）
                    queue.push_front(task_id.to_string());
                    info!(
                        "任务 {} 插入到队列前面 (group: {}, 无同组等待任务)",
                        task_id, gid
                    );
                }
            } else {
                // 没有 group_id（非文件夹任务），插入到队列前面
                queue.push_front(task_id.to_string());
                info!("任务 {} 插入到队列前面 (无 group_id)", task_id);
            }
        }

        // 🔥 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "pending".to_string(),
            group_id: group_id.clone(),
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
        {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                let mut t = task.lock().await;
                if t.status == TaskStatus::Downloading || t.status == TaskStatus::Pending {
                    t.mark_paused(); // 立即标记为暂停
                    info!("任务 {} 状态已更新为 Paused（取消中）", task_id);
                }
            }
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
        // 🔥 在删除前获取 group_id（用于事件通知）
        let group_id = {
            let tasks = self.tasks.read().await;
            if let Some(task_arc) = tasks.get(task_id) {
                let t = task_arc.lock().await;
                t.group_id.clone()
            } else {
                // 任务不在内存，尝试从持久化管理器读取
                if let Some(ref pm) = self.persistence_manager {
                    pm.lock().await.history_cache()
                        .get(task_id)
                        .and_then(|entry| entry.value().group_id.clone())
                } else {
                    None
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
                let (wal_dir, history_cache) = {
                    let pm = pm.lock().await;
                    (pm.wal_dir().clone(), pm.history_cache().clone())
                };

                // 先查历史缓存
                // 避免持有 Ref 生命周期，先克隆需要的数据
                if let Some(meta) = history_cache
                    .get(task_id)
                    .map(|entry| entry.value().clone())
                {
                    local_path = meta.local_path.clone();
                    status_completed = meta
                        .status
                        .map(|s| s == crate::persistence::types::TaskPersistenceStatus::Completed);
                    info!("删除下载任务（历史缓存）: {}", task_id);
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
        })
            .await;

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

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
        // 更新任务状态为 Pending，同时获取 group_id
        let (old_status, group_id) = {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get(task_id) {
                let mut t = task.lock().await;
                let old = format!("{:?}", t.status).to_lowercase();
                let gid = t.group_id.clone();
                if t.status == TaskStatus::Paused {
                    t.status = TaskStatus::Pending;
                    info!("任务 {} 状态从 Paused 改为 Pending（等待槽位）", task_id);
                }
                (old, gid)
            } else {
                anyhow::bail!("任务不存在: {}", task_id);
            }
        };

        // 加入等待队列
        self.waiting_queue
            .write()
            .await
            .push_back(task_id.to_string());

        let queue_len = self.waiting_queue.read().await.len();
        info!(
            "任务 {} 已加入等待队列（当前队列长度: {}）",
            task_id, queue_len
        );

        // 发送状态变更事件
        self.publish_event(DownloadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "pending".to_string(),
            group_id,
        })
            .await;

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

    /// 获取所有任务（包括当前任务和历史任务）
    pub async fn get_all_tasks(&self) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        // 获取当前任务
        for task in tasks.values() {
            result.push(task.lock().await.clone());
        }

        // 从历史缓存获取历史任务
        if let Some(ref pm) = self.persistence_manager {
            let pm = pm.lock().await;
            let history_cache = pm.history_cache();

            for entry in history_cache.iter() {
                let metadata = entry.value();

                // 只包含下载任务且状态为已完成
                if metadata.task_type == TaskType::Download
                    && metadata.status == Some(TaskPersistenceStatus::Completed)
                {
                    // 排除已在当前任务中的（避免重复）
                    if !tasks.contains_key(&metadata.task_id) {
                        if let Some(task) = Self::convert_history_to_task(metadata) {
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

        // 1. 收集内存中的已完成任务
        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.status == TaskStatus::Completed {
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

        // 3. 从历史缓存和历史文件中清除已完成任务
        let mut history_count = 0;
        if let Some(ref pm) = self.persistence_manager {
            let pm_guard = pm.lock().await;
            let history_cache = pm_guard.history_cache();
            let wal_dir = pm_guard.wal_dir().clone();

            // 收集历史缓存中的已完成下载任务
            let mut history_to_remove = Vec::new();
            for entry in history_cache.iter() {
                let metadata = entry.value();
                if metadata.task_type == TaskType::Download
                    && metadata.status == Some(TaskPersistenceStatus::Completed)
                {
                    history_to_remove.push(metadata.task_id.clone());
                }
            }

            // 从历史缓存中移除
            for task_id in &history_to_remove {
                history_cache.remove(task_id);
            }

            history_count = history_to_remove.len();

            // 释放 pm_guard，避免长时间持锁
            drop(pm_guard);

            // 从历史文件中删除（批量操作）
            for task_id in &history_to_remove {
                if let Err(e) = crate::persistence::history::remove_from_history_file(&wal_dir, task_id) {
                    warn!("从历史文件删除任务失败: task_id={}, 错误: {}", task_id, e);
                }
            }
        }

        let total_count = memory_count + history_count;
        info!(
            "清除了 {} 个已完成的任务（内存: {}, 历史: {}）",
            total_count, memory_count, history_count
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
    pub async fn set_task_completed_sender(&self, tx: tokio::sync::mpsc::UnboundedSender<String>) {
        self.chunk_scheduler.set_task_completed_sender(tx).await;
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
        let mut task = DownloadTask::new(
            recovery_info.fs_id,
            recovery_info.remote_path.clone(),
            recovery_info.local_path.clone(),
            recovery_info.file_size,
        );

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

        info!(
            "恢复下载任务: id={}, 文件={:?}, 已完成 {}/{} 分片 ({:.1}%), group_id={:?}",
            task_id,
            recovery_info.local_path,
            completed_count,
            recovery_info.total_chunks,
            if recovery_info.total_chunks > 0 {
                (completed_count as f64 / recovery_info.total_chunks as f64) * 100.0
            } else {
                0.0
            },
            recovery_info.group_id
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
            .create_task(1, "/test1".to_string(), "file1.txt".to_string(), 1024)
            .await
            .unwrap();
        let task_id2 = manager
            .create_task(2, "/test2".to_string(), "file2.txt".to_string(), 1024)
            .await
            .unwrap();
        let _task_id3 = manager
            .create_task(3, "/test3".to_string(), "file3.txt".to_string(), 1024)
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
