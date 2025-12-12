// 上传管理器
//
// 负责管理多个上传任务：
// - 任务队列管理
// - 并发控制（支持调度器模式和独立模式）
// - 进度跟踪
// - 暂停/恢复/取消
//
//  支持全局调度器模式
// - 多任务公平调度
// - 全局并发控制
// - 预注册机制

use crate::auth::UserAuth;
use crate::config::{UploadConfig, VipType};
use crate::netdisk::NetdiskClient;
use crate::persistence::{
    PersistenceManager, TaskMetadata, TaskPersistenceStatus, TaskType, UploadRecoveryInfo,
};
use crate::server::events::{ProgressThrottler, TaskEvent, UploadEvent};
use crate::server::websocket::WebSocketManager;
use crate::uploader::{
    calculate_upload_task_max_chunks, FolderScanner, PcsServerHealthManager, ScanOptions,
    UploadChunkManager, UploadChunkScheduler, UploadEngine, UploadTask, UploadTaskScheduleInfo,
    UploadTaskStatus,
};
use anyhow::{Context, Result};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// 上传任务信息（用于调度）
#[derive(Debug, Clone)]
pub struct UploadTaskInfo {
    /// 任务
    pub task: Arc<Mutex<UploadTask>>,
    /// 分片管理器
    pub chunk_manager: Arc<Mutex<UploadChunkManager>>,
    /// 取消令牌
    pub cancel_token: CancellationToken,
    /// 最大并发分片数（根据文件大小计算）
    pub max_concurrent_chunks: usize,
    /// 当前活跃分片数
    pub active_chunk_count: Arc<AtomicUsize>,
    /// 是否暂停
    pub is_paused: Arc<AtomicBool>,
    /// 已上传字节数（用于调度器模式）
    pub uploaded_bytes: Arc<AtomicU64>,
    /// 上次速度计算时间
    pub last_speed_time: Arc<Mutex<std::time::Instant>>,
    /// 上次速度计算字节数
    pub last_speed_bytes: Arc<AtomicU64>,
    /// 🔥 恢复的 upload_id（如果任务是从持久化恢复的）
    pub restored_upload_id: Option<String>,
}

/// 上传管理器
pub struct UploadManager {
    /// 网盘客户端
    client: NetdiskClient,
    /// 用户 VIP 类型
    vip_type: VipType,
    /// 所有任务（task_id -> TaskInfo）- 使用 Arc 包装以支持跨线程共享
    tasks: Arc<DashMap<String, UploadTaskInfo>>,
    /// 等待队列（task_id 列表，FIFO）
    waiting_queue: Arc<RwLock<VecDeque<String>>>,
    /// 全局并发控制信号量（用于独立模式）
    #[allow(dead_code)]
    global_semaphore: Arc<Semaphore>,
    /// 服务器健康管理器
    server_health: Arc<PcsServerHealthManager>,
    /// 全局调度器（）
    scheduler: Option<Arc<UploadChunkScheduler>>,
    /// 是否使用调度器模式
    use_scheduler: bool,
    /// 最大同时上传任务数（动态可调整）
    max_concurrent_tasks: Arc<AtomicUsize>,
    /// 最大重试次数（动态可调整）
    max_retries: Arc<AtomicUsize>,
    /// 🔥 持久化管理器引用（使用单锁结构避免死锁）
    persistence_manager: Arc<Mutex<Option<Arc<Mutex<PersistenceManager>>>>>,
    /// 🔥 WebSocket 管理器
    ws_manager: Arc<RwLock<Option<Arc<WebSocketManager>>>>,
}

impl UploadManager {
    /// 创建新的上传管理器（使用默认配置）
    pub fn new(client: NetdiskClient, user_auth: &UserAuth) -> Self {
        Self::new_with_config(client, user_auth, &UploadConfig::default())
    }

    /// 创建上传管理器（从配置读取参数）
    ///
    /// # 参数
    /// * `client` - 网盘客户端
    /// * `user_auth` - 用户认证信息
    /// * `config` - 上传配置
    pub fn new_with_config(
        client: NetdiskClient,
        user_auth: &UserAuth,
        config: &UploadConfig,
    ) -> Self {
        Self::new_with_full_options(client, user_auth, config, true)
    }

    /// 创建上传管理器（完整选项）
    ///
    /// # 参数
    /// * `client` - 网盘客户端
    /// * `user_auth` - 用户认证信息
    /// * `config` - 上传配置
    /// * `use_scheduler` - 是否使用全局调度器模式
    pub fn new_with_full_options(
        client: NetdiskClient,
        user_auth: &UserAuth,
        config: &UploadConfig,
        use_scheduler: bool,
    ) -> Self {
        let max_global_threads = config.max_global_threads;
        let max_concurrent_tasks = config.max_concurrent_tasks;
        let max_retries = config.max_retries as usize;

        // 从 user_auth 获取 VIP 类型
        let vip_type = VipType::from_u32(user_auth.vip_type.unwrap_or(0));

        // 创建服务器健康管理器
        let servers = vec![
            "d.pcs.baidu.com".to_string(),
            "c.pcs.baidu.com".to_string(),
            "pcs.baidu.com".to_string(),
        ];
        let server_health = Arc::new(PcsServerHealthManager::from_servers(servers));

        // 创建调度器（如果启用）
        let scheduler = if use_scheduler {
            info!(
                "上传管理器使用调度器模式: 全局线程数={}, 最大任务数={}, 最大重试={}",
                max_global_threads, max_concurrent_tasks, max_retries
            );
            Some(Arc::new(UploadChunkScheduler::new_with_config(
                max_global_threads,
                max_concurrent_tasks,
                max_retries as u32,
            )))
        } else {
            info!(
                "上传管理器使用独立模式: 全局线程数={}, 最大任务数={}, 最大重试={}",
                max_global_threads, max_concurrent_tasks, max_retries
            );
            None
        };

        let waiting_queue = Arc::new(RwLock::new(VecDeque::new()));
        let max_concurrent_tasks_atomic = Arc::new(AtomicUsize::new(max_concurrent_tasks));
        let max_retries_atomic = Arc::new(AtomicUsize::new(max_retries));

        let tasks = Arc::new(DashMap::new());

        let manager = Self {
            client,
            vip_type,
            tasks: tasks.clone(),
            waiting_queue: waiting_queue.clone(),
            global_semaphore: Arc::new(Semaphore::new(max_global_threads)),
            server_health,
            scheduler: scheduler.clone(),
            use_scheduler,
            max_concurrent_tasks: max_concurrent_tasks_atomic,
            max_retries: max_retries_atomic,
            persistence_manager: Arc::new(Mutex::new(None)),
            ws_manager: Arc::new(RwLock::new(None)),
        };

        // 启动后台任务：定期检查并启动等待队列中的任务
        if use_scheduler {
            manager.start_waiting_queue_monitor();
        }

        manager
    }

    /// 动态更新最大全局线程数
    pub fn update_max_threads(&self, new_max: usize) {
        if let Some(scheduler) = &self.scheduler {
            scheduler.update_max_threads(new_max);
        }
        info!("🔧 上传管理器: 动态调整全局最大线程数为 {}", new_max);
    }

    /// 动态更新最大并发任务数
    pub fn update_max_concurrent_tasks(&self, new_max: usize) {
        self.max_concurrent_tasks.store(new_max, Ordering::SeqCst);
        if let Some(scheduler) = &self.scheduler {
            scheduler.update_max_concurrent_tasks(new_max);
        }
        info!("🔧 上传管理器: 动态调整最大并发任务数为 {}", new_max);
    }

    /// 动态更新最大重试次数
    pub fn update_max_retries(&self, new_max: u32) {
        self.max_retries.store(new_max as usize, Ordering::SeqCst);
        if let Some(scheduler) = &self.scheduler {
            scheduler.update_max_retries(new_max);
        }
        info!("🔧 上传管理器: 动态调整最大重试次数为 {}", new_max);
    }

    /// 🔥 设置 WebSocket 管理器
    pub async fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        let mut ws = self.ws_manager.write().await;
        *ws = Some(ws_manager);
        info!("上传管理器已设置 WebSocket 管理器");
    }

    /// 🔥 发布上传事件
    async fn publish_event(&self, event: UploadEvent) {
        let ws = self.ws_manager.read().await;
        if let Some(ref ws) = *ws {
            ws.send_if_subscribed(TaskEvent::Upload(event), None);
        }
    }

    /// 获取当前最大并发任务数
    pub fn max_concurrent_tasks(&self) -> usize {
        self.max_concurrent_tasks.load(Ordering::SeqCst)
    }

    /// 获取当前最大重试次数
    pub fn max_retries(&self) -> u32 {
        self.max_retries.load(Ordering::SeqCst) as u32
    }

    /// 获取调度器引用
    pub fn scheduler(&self) -> Option<Arc<UploadChunkScheduler>> {
        self.scheduler.clone()
    }

    /// 🔥 设置持久化管理器
    ///
    /// 由 AppState 在初始化时调用，注入持久化管理器
    pub async fn set_persistence_manager(&self, pm: Arc<Mutex<PersistenceManager>>) {
        let mut lock = self.persistence_manager.lock().await;
        *lock = Some(pm);
        info!("上传管理器已设置持久化管理器");
    }

    /// 获取持久化管理器引用的克隆
    pub async fn persistence_manager(&self) -> Option<Arc<Mutex<PersistenceManager>>> {
        self.persistence_manager.lock().await.clone()
    }

    /// 创建上传任务
    ///
    /// # 参数
    /// * `local_path` - 本地文件路径
    /// * `remote_path` - 网盘目标路径
    ///
    /// # 返回
    /// 任务ID
    pub async fn create_task(&self, local_path: PathBuf, remote_path: String) -> Result<String> {
        // 获取文件大小
        let metadata = tokio::fs::metadata(&local_path)
            .await
            .context(format!("无法获取文件元数据: {:?}", local_path))?;

        if metadata.is_dir() {
            return Err(anyhow::anyhow!(
                "不支持直接上传目录，请使用 create_folder_task"
            ));
        }

        let file_size = metadata.len();

        // 创建任务
        let task = UploadTask::new(local_path.clone(), remote_path.clone(), file_size);
        let task_id = task.id.clone();

        // 创建分片管理器（使用用户的 VIP 等级计算分片大小）
        let chunk_manager = UploadChunkManager::with_vip_type(file_size, self.vip_type);

        // 计算最大并发分片数
        let max_concurrent_chunks = calculate_upload_task_max_chunks(file_size);

        // 获取分片信息（用于持久化）
        let total_chunks = chunk_manager.chunk_count();
        let chunk_size =
            crate::uploader::calculate_recommended_chunk_size(file_size, self.vip_type);

        info!(
            "创建上传任务: id={}, local={:?}, remote={}, size={}, chunks={}, max_concurrent={}",
            task_id, local_path, remote_path, file_size, total_chunks, max_concurrent_chunks
        );

        // 🔥 注册任务到持久化管理器
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.register_upload_task(
                task_id.clone(),
                local_path.clone(),
                remote_path.clone(),
                file_size,
                chunk_size,
                total_chunks,
            ) {
                warn!("注册上传任务到持久化管理器失败: {}", e);
            }
        }

        // 保存任务信息
        let task_info = UploadTaskInfo {
            task: Arc::new(Mutex::new(task)),
            chunk_manager: Arc::new(Mutex::new(chunk_manager)),
            cancel_token: CancellationToken::new(),
            max_concurrent_chunks,
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            is_paused: Arc::new(AtomicBool::new(false)),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
            last_speed_time: Arc::new(Mutex::new(std::time::Instant::now())),
            last_speed_bytes: Arc::new(AtomicU64::new(0)),
            restored_upload_id: None, // 新创建的任务没有恢复的 upload_id
        };

        self.tasks.insert(task_id.clone(), task_info);

        // 🔥 发送任务创建事件
        self.publish_event(UploadEvent::Created {
            task_id: task_id.clone(),
            local_path: local_path.to_string_lossy().to_string(),
            remote_path,
            total_size: file_size,
        })
            .await;

        Ok(task_id)
    }

    /// 批量创建上传任务
    pub async fn create_batch_tasks(&self, files: Vec<(PathBuf, String)>) -> Result<Vec<String>> {
        let mut task_ids = Vec::with_capacity(files.len());

        for (local_path, remote_path) in files {
            match self.create_task(local_path.clone(), remote_path).await {
                Ok(task_id) => {
                    task_ids.push(task_id);
                }
                Err(e) => {
                    warn!("创建任务失败: {:?}, 错误: {}", local_path, e);
                }
            }
        }

        Ok(task_ids)
    }

    /// 创建文件夹上传任务
    ///
    /// # 参数
    /// * `local_folder` - 本地文件夹路径
    /// * `remote_folder` - 网盘目标文件夹路径
    /// * `scan_options` - 扫描选项（可选）
    ///
    /// # 返回
    /// 所有创建的任务ID列表
    ///
    /// # 说明
    /// - 会递归扫描本地文件夹
    /// - 保持目录结构
    /// - 自动创建批量上传任务
    pub async fn create_folder_task<P: AsRef<Path>>(
        &self,
        local_folder: P,
        remote_folder: String,
        scan_options: Option<ScanOptions>,
    ) -> Result<Vec<String>> {
        let local_folder = local_folder.as_ref();

        info!(
            "开始创建文件夹上传任务: local={:?}, remote={}",
            local_folder, remote_folder
        );

        // 使用文件夹扫描器扫描文件
        let scanner = if let Some(options) = scan_options {
            FolderScanner::with_options(options)
        } else {
            FolderScanner::new()
        };

        let scanned_files = scanner.scan(local_folder)?;

        if scanned_files.is_empty() {
            return Err(anyhow::anyhow!("文件夹为空或无可上传文件"));
        }

        info!("扫描到 {} 个文件，开始创建上传任务", scanned_files.len());

        // 准备批量任务
        let mut tasks = Vec::with_capacity(scanned_files.len());

        for file in scanned_files {
            // 构建远程路径：remote_folder + relative_path
            let remote_path = if remote_folder.ends_with('/') {
                format!("{}{}", remote_folder, file.relative_path.to_string_lossy())
            } else {
                format!("{}/{}", remote_folder, file.relative_path.to_string_lossy())
            };

            // 统一路径分隔符为 Unix 风格（百度网盘使用 /）
            let remote_path = remote_path.replace('\\', "/");

            tasks.push((file.local_path, remote_path));
        }

        // 批量创建任务
        let task_ids = self.create_batch_tasks(tasks).await?;

        info!("文件夹上传任务创建完成: 成功 {} 个", task_ids.len());

        Ok(task_ids)
    }

    /// 开始上传任务
    ///
    /// 根据 `use_scheduler` 配置选择执行模式：
    /// - 调度器模式：将任务注册到全局调度器，由调度器统一调度
    /// - 独立模式：直接启动 UploadEngine 执行上传
    pub async fn start_task(&self, task_id: &str) -> Result<()> {
        let task_info = self
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;

        // 检查任务状态
        let (local_path, remote_path, total_size) = {
            let task = task_info.task.lock().await;
            match task.status {
                UploadTaskStatus::Pending | UploadTaskStatus::Paused => {}
                UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid => {
                    return Err(anyhow::anyhow!("任务已在上传中"));
                }
                UploadTaskStatus::Completed | UploadTaskStatus::RapidUploadSuccess => {
                    return Err(anyhow::anyhow!("任务已完成"));
                }
                UploadTaskStatus::Failed => {
                    // 允许重试失败的任务
                }
            }
            (
                task.local_path.clone(),
                task.remote_path.clone(),
                task.total_size,
            )
        };

        // 动态获取上传服务器列表
        match self.client.locate_upload().await {
            Ok(servers) => {
                if !servers.is_empty() {
                    self.server_health.update_servers(servers);
                }
            }
            Err(e) => {
                warn!("获取上传服务器列表失败，使用默认服务器: {}", e);
            }
        }

        // 根据模式选择启动方式
        if self.use_scheduler && self.scheduler.is_some() {
            self.start_task_with_scheduler(task_id, &task_info, local_path, remote_path, total_size)
                .await
        } else {
            self.start_task_standalone(task_id, &task_info).await
        }
    }

    /// 调度器模式启动任务
    async fn start_task_with_scheduler(
        &self,
        task_id: &str,
        task_info: &dashmap::mapref::one::Ref<'_, String, UploadTaskInfo>,
        local_path: PathBuf,
        remote_path: String,
        total_size: u64,
    ) -> Result<()> {
        let scheduler = self.scheduler.as_ref().unwrap();

        // 预注册检查
        if !scheduler.pre_register().await {
            // 加入等待队列而不是返回错误
            self.waiting_queue
                .write()
                .await
                .push_back(task_id.to_string());

            info!(
                "上传任务 {} 加入等待队列（系统等待）(活跃任务数已达上限: {})",
                task_id,
                self.max_concurrent_tasks()
            );
            return Ok(());
        }

        // 克隆需要的数据
        let task = task_info.task.clone();
        let chunk_manager = task_info.chunk_manager.clone();
        let cancel_token = task_info.cancel_token.clone();
        let is_paused = task_info.is_paused.clone();
        let active_chunk_count = task_info.active_chunk_count.clone();
        let max_concurrent_chunks = task_info.max_concurrent_chunks;
        let uploaded_bytes = task_info.uploaded_bytes.clone();
        let last_speed_time = task_info.last_speed_time.clone();
        let last_speed_bytes = task_info.last_speed_bytes.clone();
        let server_health = self.server_health.clone();
        let client = self.client.clone();
        let scheduler = scheduler.clone();
        let task_id_string = task_id.to_string();
        let vip_type = self.vip_type;
        let persistence_manager = self.persistence_manager.lock().await.clone();
        // 🔥 检查是否有恢复的 upload_id
        let restored_upload_id = task_info.restored_upload_id.clone();
        // 🔥 获取 WebSocket 管理器
        let ws_manager = self.ws_manager.read().await.clone();
        // 🔥 克隆 tasks 引用，用于更新 restored_upload_id
        let tasks = self.tasks.clone();

        // 在后台执行 precreate 并注册到调度器
        tokio::spawn(async move {
            info!("开始准备上传任务: {}", task_id_string);

            // 标记为上传中
            {
                let mut t = task.lock().await;
                t.mark_uploading();
            }

            // 1. 计算 block_list（必须重新计算，因为它是按 4MB 固定大小计算的）
            let block_list = match crate::uploader::RapidUploadChecker::calculate_block_list(
                &local_path,
                vip_type,
            )
                .await
            {
                Ok(bl) => bl,
                Err(e) => {
                    let error_msg = format!("计算 block_list 失败: {}", e);
                    error!("{}", error_msg);
                    scheduler.cancel_pre_register();

                    let mut t = task.lock().await;
                    t.mark_failed(error_msg.clone());
                    drop(t);

                    // 🔥 发布任务失败事件
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::Failed {
                                task_id: task_id_string.clone(),
                                error: error_msg.clone(),
                            }),
                            None,
                        );
                    }

                    // 🔥 更新持久化错误信息
                    if let Some(ref pm) = persistence_manager {
                        if let Err(e) = pm.lock().await.update_task_error(&task_id_string, error_msg) {
                            warn!("更新上传任务错误信息失败: {}", e);
                        }
                    }

                    return;
                }
            };

            // 2. 检查是否有恢复的 upload_id
            let upload_id = if let Some(restored_id) = restored_upload_id {
                info!(
                    "使用恢复的 upload_id: {} (如果合并失败，说明已过期，需要重新上传)",
                    restored_id
                );
                restored_id
            } else {
                // 没有恢复的 upload_id，需要调用 precreate
                let precreate_response = match client
                    .precreate(&remote_path, total_size, &block_list)
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        let error_msg = format!("预创建文件失败: {}", e);
                        error!("{}", error_msg);
                        scheduler.cancel_pre_register();

                        let mut t = task.lock().await;
                        t.mark_failed(error_msg.clone());
                        drop(t);

                        // 🔥 发布任务失败事件
                        if let Some(ref ws) = ws_manager {
                            ws.send_if_subscribed(
                                TaskEvent::Upload(UploadEvent::Failed {
                                    task_id: task_id_string.clone(),
                                    error: error_msg.clone(),
                                }),
                                None,
                            );
                        }

                        // 🔥 更新持久化错误信息
                        if let Some(ref pm) = persistence_manager {
                            if let Err(e) = pm.lock().await.update_task_error(&task_id_string, error_msg) {
                                warn!("更新上传任务错误信息失败: {}", e);
                            }
                        }

                        return;
                    }
                };

                // 检查秒传
                if precreate_response.is_rapid_upload() {
                    info!("秒传成功: {}", remote_path);
                    scheduler.cancel_pre_register();
                    let mut t = task.lock().await;
                    t.mark_rapid_upload_success();
                    return;
                }

                let new_upload_id = precreate_response.uploadid.clone();
                if new_upload_id.is_empty() {
                    let error_msg = "预创建失败：未获取到 uploadid".to_string();
                    error!("{}", error_msg);
                    scheduler.cancel_pre_register();

                    let mut t = task.lock().await;
                    t.mark_failed(error_msg.clone());
                    drop(t);

                    // 🔥 发布任务失败事件
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::Failed {
                                task_id: task_id_string.clone(),
                                error: error_msg.clone(),
                            }),
                            None,
                        );
                    }

                    // 🔥 更新持久化错误信息
                    if let Some(ref pm) = persistence_manager {
                        if let Err(e) = pm.lock().await.update_task_error(&task_id_string, error_msg) {
                            warn!("更新上传任务错误信息失败: {}", e);
                        }
                    }

                    return;
                }

                // 🔥 更新持久化元数据中的 upload_id
                if let Some(ref pm_arc) = persistence_manager {
                    if let Err(e) = pm_arc
                        .lock()
                        .await
                        .update_upload_id(&task_id_string, new_upload_id.clone())
                    {
                        warn!("更新上传任务 upload_id 失败: {}", e);
                    }
                }

                // 🔥 更新内存中的 restored_upload_id（关键修复：支持暂停恢复）
                if let Some(mut task_info) = tasks.get_mut(&task_id_string) {
                    task_info.restored_upload_id = Some(new_upload_id.clone());
                    info!("✓ 已保存 upload_id 到任务信息，支持暂停恢复: {}", task_id_string);
                }

                new_upload_id
            };

            // 3. 创建调度信息并注册到调度器
            let schedule_info = UploadTaskScheduleInfo {
                task_id: task_id_string.clone(),
                task: task.clone(),
                chunk_manager,
                server_health,
                client,
                local_path,
                remote_path: remote_path.clone(),
                upload_id: upload_id.clone(),
                total_size,
                block_list,
                cancellation_token: cancel_token,
                is_paused,
                is_merging: Arc::new(AtomicBool::new(false)),
                active_chunk_count,
                max_concurrent_chunks,
                uploaded_bytes,
                last_speed_time,
                last_speed_bytes,
                persistence_manager,
                ws_manager,
                progress_throttler: Arc::new(ProgressThrottler::default()),
            };

            if let Err(e) = scheduler.register_task(schedule_info).await {
                error!("注册任务到调度器失败: {}", e);
                scheduler.cancel_pre_register();
                let mut t = task.lock().await;
                t.mark_failed(format!("注册任务失败: {}", e));
                return;
            }

            info!("上传任务已注册到调度器: {}", task_id_string);

            // 注意：调度器会自动处理分片上传和完成
            // 这里不需要等待，调度器会在所有分片完成后调用 create_file
        });

        Ok(())
    }

    /// 独立模式启动任务
    async fn start_task_standalone(
        &self,
        task_id: &str,
        task_info: &dashmap::mapref::one::Ref<'_, String, UploadTaskInfo>,
    ) -> Result<()> {
        // 克隆需要的数据
        let task = task_info.task.clone();
        let chunk_manager = task_info.chunk_manager.clone();
        let cancel_token = task_info.cancel_token.clone();
        let server_health = self.server_health.clone();
        let client = self.client.clone();

        // 创建上传引擎
        let engine = UploadEngine::new(
            client,
            task.clone(),
            chunk_manager,
            server_health,
            cancel_token,
            self.vip_type,
        );

        // 在后台启动上传
        let task_id_clone = task_id.to_string();
        tokio::spawn(async move {
            info!("开始上传任务: {}", task_id_clone);

            match engine.upload().await {
                Ok(()) => {
                    info!("上传任务完成: {}", task_id_clone);
                }
                Err(e) => {
                    error!("上传任务失败: {}, 错误: {}", task_id_clone, e);
                    let mut task = task.lock().await;
                    task.mark_failed(e.to_string());
                }
            }
        });

        Ok(())
    }

    /// 暂停上传任务
    pub async fn pause_task(&self, task_id: &str) -> Result<()> {
        let task_info = self
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;

        // 设置暂停标志（调度器模式使用）
        task_info.is_paused.store(true, Ordering::SeqCst);

        let mut task = task_info.task.lock().await;

        match task.status {
            UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid => {
                // 🔥 保存旧状态用于发布 StatusChanged
                let old_status = format!("{:?}", task.status).to_lowercase();

                task.mark_paused();
                info!("暂停上传任务: {}", task_id);
                drop(task);

                // 🔥 发送状态变更事件
                self.publish_event(UploadEvent::StatusChanged {
                    task_id: task_id.to_string(),
                    old_status,
                    new_status: "paused".to_string(),
                })
                    .await;

                // 🔥 发送暂停事件
                self.publish_event(UploadEvent::Paused {
                    task_id: task_id.to_string(),
                })
                    .await;
                Ok(())
            }
            _ => Err(anyhow::anyhow!("任务当前状态不支持暂停")),
        }
    }

    /// 恢复上传任务
    pub async fn resume_task(&self, task_id: &str) -> Result<()> {
        let task_info = self
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;

        let old_status;
        {
            let task = task_info.task.lock().await;
            if task.status != UploadTaskStatus::Paused {
                return Err(anyhow::anyhow!("任务不是暂停状态"));
            }
            // 🔥 保存旧状态
            old_status = format!("{:?}", task.status).to_lowercase();
        }

        // 清除暂停标志（调度器模式使用）
        task_info.is_paused.store(false, Ordering::SeqCst);

        // 🔥 发送状态变更事件
        self.publish_event(UploadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status,
            new_status: "pending".to_string(),
        })
            .await;

        // 🔥 发送恢复事件
        self.publish_event(UploadEvent::Resumed {
            task_id: task_id.to_string(),
        })
            .await;

        // 重新开始任务
        self.start_task(task_id).await
    }

    /// 取消上传任务
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        // 从等待队列移除（如果存在）
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| id != task_id);
        }

        let task_info = self
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;

        // 发送取消信号
        task_info.cancel_token.cancel();

        // 如果使用调度器模式，也从调度器取消
        if let Some(scheduler) = &self.scheduler {
            scheduler.cancel_task(task_id).await;
        }

        // 更新任务状态
        let mut task = task_info.task.lock().await;
        task.mark_failed("用户取消".to_string());

        info!("取消上传任务: {}", task_id);

        drop(task);
        drop(task_info);

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

        Ok(())
    }

    /// 删除上传任务
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        // 从等待队列移除（如果存在）
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| id != task_id);
        }

        // 先取消任务
        if let Some(task_info) = self.tasks.get(task_id) {
            task_info.cancel_token.cancel();
        }

        // 如果使用调度器模式，也从调度器移除
        if let Some(scheduler) = &self.scheduler {
            scheduler.cancel_task(task_id).await;
        }

        // 移除任务
        self.tasks.remove(task_id);

        // 🔥 清理持久化文件
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.on_task_deleted(task_id) {
                warn!("清理上传任务持久化文件失败: {}", e);
            }
        }

        info!("删除上传任务: {}", task_id);

        // 🔥 发送删除事件
        self.publish_event(UploadEvent::Deleted {
            task_id: task_id.to_string(),
        })
            .await;

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

        Ok(())
    }

    /// 获取任务状态
    pub async fn get_task(&self, task_id: &str) -> Option<UploadTask> {
        let task_info = self.tasks.get(task_id)?;
        let task = task_info.task.lock().await;
        Some(task.clone())
    }

    /// 获取所有任务（包括当前任务和历史任务）
    pub async fn get_all_tasks(&self) -> Vec<UploadTask> {
        let mut tasks = Vec::new();

        // 获取当前任务
        for entry in self.tasks.iter() {
            let task = entry.task.lock().await;
            tasks.push(task.clone());
        }

        // 从历史缓存获取历史任务
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm = pm_arc.lock().await;
            let history_cache = pm.history_cache();

            for entry in history_cache.iter() {
                let metadata = entry.value();

                // 只包含上传任务且状态为已完成
                if metadata.task_type == TaskType::Upload
                    && metadata.status == Some(TaskPersistenceStatus::Completed)
                {
                    // 排除已在当前任务中的（避免重复）
                    if !self.tasks.contains_key(&metadata.task_id) {
                        if let Some(task) = Self::convert_history_to_task(metadata) {
                            tasks.push(task);
                        }
                    }
                }
            }
        }

        // 按创建时间倒序排序
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        tasks
    }

    /// 将历史元数据转换为上传任务
    fn convert_history_to_task(metadata: &TaskMetadata) -> Option<UploadTask> {
        // 验证必要字段
        let local_path = metadata.source_path.clone()?;
        let remote_path = metadata.target_path.clone()?;
        let file_size = metadata.file_size.unwrap_or(0);

        Some(UploadTask {
            id: metadata.task_id.clone(),
            local_path,
            remote_path,
            total_size: file_size,
            uploaded_size: file_size, // 已完成的任务
            status: UploadTaskStatus::Completed,
            speed: 0,
            created_at: metadata.created_at.timestamp(),
            started_at: Some(metadata.created_at.timestamp()),
            completed_at: metadata.completed_at.map(|t| t.timestamp()),
            error: None,
            is_rapid_upload: false,
            content_md5: None,
            slice_md5: None,
            content_crc32: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            total_chunks: metadata.total_chunks.unwrap_or(0),
            completed_chunks: metadata.total_chunks.unwrap_or(0), // 已完成的任务
        })
    }

    /// 获取活跃任务数
    pub fn active_task_count(&self) -> usize {
        let mut count = 0;
        for entry in self.tasks.iter() {
            // 这里使用 try_lock 避免阻塞
            if let Ok(task) = entry.task.try_lock() {
                if matches!(
                    task.status,
                    UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid
                ) {
                    count += 1;
                }
            }
        }
        count
    }

    /// 清除已完成的任务
    pub async fn clear_completed(&self) -> usize {
        let mut to_remove = Vec::new();

        // 1. 收集内存中的已完成任务
        for entry in self.tasks.iter() {
            let task = entry.task.lock().await;
            if matches!(
                task.status,
                UploadTaskStatus::Completed | UploadTaskStatus::RapidUploadSuccess
            ) {
                to_remove.push(entry.key().clone());
            }
        }

        // 2. 从内存中移除
        let memory_count = to_remove.len();
        for task_id in &to_remove {
            self.tasks.remove(task_id);
        }

        // 3. 从历史缓存和历史文件中清除已完成任务
        let mut history_count = 0;
        if let Some(pm_arc) = self.persistence_manager.lock().await.as_ref().map(|pm| pm.clone()) {
            let pm_guard = pm_arc.lock().await;
            let history_cache = pm_guard.history_cache();
            let wal_dir = pm_guard.wal_dir().clone();

            // 收集历史缓存中的已完成上传任务
            let mut history_to_remove = Vec::new();
            for entry in history_cache.iter() {
                let metadata = entry.value();
                if metadata.task_type == TaskType::Upload
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
            "清除了 {} 个已完成的上传任务（内存: {}, 历史: {}）",
            total_count, memory_count, history_count
        );
        total_count
    }

    /// 清除失败的任务
    pub async fn clear_failed(&self) -> usize {
        let mut removed = 0;
        let mut to_remove = Vec::new();

        for entry in self.tasks.iter() {
            let task = entry.task.lock().await;
            if matches!(task.status, UploadTaskStatus::Failed) {
                to_remove.push(entry.key().clone());
            }
        }

        for task_id in to_remove {
            self.tasks.remove(&task_id);
            removed += 1;
        }

        info!("清除了 {} 个失败的上传任务", removed);
        removed
    }

    /// 开始所有待处理的任务
    pub async fn start_all_pending(&self) -> Result<usize> {
        let mut started = 0;
        let mut pending_ids = Vec::new();

        for entry in self.tasks.iter() {
            let task = entry.task.lock().await;
            if matches!(task.status, UploadTaskStatus::Pending) {
                pending_ids.push(entry.key().clone());
            }
        }

        for task_id in pending_ids {
            if let Err(e) = self.start_task(&task_id).await {
                warn!("启动任务失败: {}, 错误: {}", task_id, e);
            } else {
                started += 1;
            }
        }

        info!("启动了 {} 个待处理的上传任务", started);
        Ok(started)
    }

    /// 尝试从等待队列启动任务
    async fn try_start_waiting_tasks(&self) {
        if !self.use_scheduler {
            return;
        }

        let scheduler = match &self.scheduler {
            Some(s) => s,
            None => return,
        };

        loop {
            // 检查是否有空闲位置
            let active_count = scheduler.active_task_count().await;
            if active_count >= self.max_concurrent_tasks() {
                break;
            }

            // 从等待队列取出任务
            let task_id = {
                let mut queue = self.waiting_queue.write().await;
                queue.pop_front()
            };

            match task_id {
                Some(id) => {
                    info!("从等待队列启动上传任务: {}", id);
                    if let Err(e) = self.start_task(&id).await {
                        error!("启动等待上传任务失败: {}, 错误: {}", id, e);
                    }
                }
                None => break, // 队列为空
            }
        }
    }

    /// 启动后台监控任务：定期检查并启动等待队列中的任务
    ///
    /// 这确保了当活跃任务自然完成时，等待队列中的任务能被自动启动
    fn start_waiting_queue_monitor(&self) {
        let waiting_queue = self.waiting_queue.clone();
        let scheduler = match &self.scheduler {
            Some(s) => s.clone(),
            None => return,
        };
        let tasks = self.tasks.clone();
        let client = self.client.clone();
        let server_health = self.server_health.clone();
        let vip_type = self.vip_type;
        let max_concurrent_tasks = self.max_concurrent_tasks.clone();
        let persistence_manager = self.persistence_manager.clone();
        let ws_manager = self.ws_manager.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));

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

                // 检查是否有空闲位置
                let active_count = scheduler.active_task_count().await;
                if active_count >= max_concurrent_tasks.load(Ordering::SeqCst) {
                    continue;
                }

                // 尝试启动等待任务
                loop {
                    // 先预注册，成功才继续
                    if !scheduler.pre_register().await {
                        break;
                    }

                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            info!("🔄 后台监控：从等待队列启动上传任务 {} (已预注册)", id);

                            // 获取任务信息
                            let task_info_opt = tasks.get(&id);
                            if let Some(task_info) = task_info_opt {
                                // 获取任务基本信息
                                let (local_path, remote_path, total_size) = {
                                    let task = task_info.task.lock().await;
                                    (
                                        task.local_path.clone(),
                                        task.remote_path.clone(),
                                        task.total_size,
                                    )
                                };

                                // 克隆需要的数据
                                let task = task_info.task.clone();
                                let chunk_manager = task_info.chunk_manager.clone();
                                let cancel_token = task_info.cancel_token.clone();
                                let is_paused = task_info.is_paused.clone();
                                let active_chunk_count = task_info.active_chunk_count.clone();
                                let max_concurrent_chunks = task_info.max_concurrent_chunks;
                                let uploaded_bytes = task_info.uploaded_bytes.clone();
                                let last_speed_time = task_info.last_speed_time.clone();
                                let last_speed_bytes = task_info.last_speed_bytes.clone();

                                drop(task_info); // 释放 DashMap 引用

                                let server_health_clone = server_health.clone();
                                let client_clone = client.clone();
                                let scheduler_clone = scheduler.clone();
                                let task_id_clone = id.clone();
                                let pm_clone = persistence_manager.lock().await.clone();
                                let ws_manager_clone = ws_manager.read().await.clone();

                                // 在后台执行 precreate 并注册到调度器
                                tokio::spawn(async move {
                                    info!("后台监控：开始准备上传任务: {}", task_id_clone);

                                    // 标记为上传中
                                    {
                                        let mut t = task.lock().await;
                                        t.mark_uploading();
                                    }

                                    // 1. 计算 block_list
                                    let block_list = match crate::uploader::RapidUploadChecker::calculate_block_list(&local_path, vip_type).await {
                                        Ok(bl) => bl,
                                        Err(e) => {
                                            error!("后台监控：计算 block_list 失败: {}", e);
                                            scheduler_clone.cancel_pre_register();
                                            let mut t = task.lock().await;
                                            t.mark_failed(format!("计算 block_list 失败: {}", e));
                                            return;
                                        }
                                    };

                                    // 2. 预创建文件
                                    let precreate_response = match client_clone
                                        .precreate(&remote_path, total_size, &block_list)
                                        .await
                                    {
                                        Ok(resp) => resp,
                                        Err(e) => {
                                            error!("后台监控：预创建文件失败: {}", e);
                                            scheduler_clone.cancel_pre_register();
                                            let mut t = task.lock().await;
                                            t.mark_failed(format!("预创建文件失败: {}", e));
                                            return;
                                        }
                                    };

                                    // 检查秒传
                                    if precreate_response.is_rapid_upload() {
                                        info!("后台监控：秒传成功: {}", remote_path);
                                        scheduler_clone.cancel_pre_register();
                                        let mut t = task.lock().await;
                                        t.mark_rapid_upload_success();
                                        return;
                                    }

                                    let upload_id = precreate_response.uploadid.clone();
                                    if upload_id.is_empty() {
                                        error!("后台监控：预创建失败：未获取到 uploadid");
                                        scheduler_clone.cancel_pre_register();
                                        let mut t = task.lock().await;
                                        t.mark_failed("预创建失败：未获取到 uploadid".to_string());
                                        return;
                                    }

                                    // 🔥 更新持久化元数据中的 upload_id
                                    if let Some(ref pm_arc) = pm_clone {
                                        if let Err(e) = pm_arc
                                            .lock()
                                            .await
                                            .update_upload_id(&task_id_clone, upload_id.clone())
                                        {
                                            warn!("后台监控：更新上传任务 upload_id 失败: {}", e);
                                        }
                                    }

                                    // 3. 创建调度信息并注册到调度器
                                    let schedule_info = UploadTaskScheduleInfo {
                                        task_id: task_id_clone.clone(),
                                        task: task.clone(),
                                        chunk_manager,
                                        server_health: server_health_clone,
                                        client: client_clone,
                                        local_path: local_path.to_path_buf(),
                                        remote_path: remote_path.to_string(),
                                        upload_id: upload_id.clone(),
                                        total_size,
                                        block_list,
                                        cancellation_token: cancel_token,
                                        is_paused,
                                        is_merging: Arc::new(AtomicBool::new(false)),
                                        active_chunk_count,
                                        max_concurrent_chunks,
                                        uploaded_bytes,
                                        last_speed_time,
                                        last_speed_bytes,
                                        persistence_manager: pm_clone,
                                        ws_manager: ws_manager_clone,
                                        progress_throttler: Arc::new(ProgressThrottler::default()),
                                    };

                                    if let Err(e) =
                                        scheduler_clone.register_task(schedule_info).await
                                    {
                                        error!("后台监控：注册任务到调度器失败: {}", e);
                                        scheduler_clone.cancel_pre_register();
                                        let mut t = task.lock().await;
                                        t.mark_failed(format!("注册任务失败: {}", e));
                                        return;
                                    }

                                    info!("后台监控：上传任务 {} 已注册到调度器", task_id_clone);
                                });
                            } else {
                                // 任务不存在，取消预注册
                                warn!("后台监控：任务 {} 不存在，取消预注册", id);
                                scheduler.cancel_pre_register();
                            }
                        }
                        None => {
                            // 队列为空，取消预注册
                            scheduler.cancel_pre_register();
                            break;
                        }
                    }
                }
            }
        });
    }

    /// 🔥 从恢复信息创建上传任务
    ///
    /// 用于程序启动时恢复未完成的上传任务
    /// 恢复的任务初始状态为 Paused，需要手动调用 start_task 启动
    ///
    /// # Arguments
    /// * `recovery_info` - 从持久化文件恢复的任务信息
    ///
    /// # Returns
    /// 恢复的任务 ID
    ///
    /// # 注意
    /// - upload_id 可能已过期，启动任务时会重新 precreate
    /// - 已完成的分片会在分片管理器中标记为完成
    pub async fn restore_task(&self, recovery_info: UploadRecoveryInfo) -> Result<String> {
        let task_id = recovery_info.task_id.clone();

        // 检查任务是否已存在
        if self.tasks.contains_key(&task_id) {
            anyhow::bail!("任务 {} 已存在，无法恢复", task_id);
        }

        // 验证源文件存在
        if !recovery_info.source_path.exists() {
            anyhow::bail!("源文件不存在: {:?}", recovery_info.source_path);
        }

        // 创建恢复任务（使用 Paused 状态）
        let mut task = UploadTask::new(
            recovery_info.source_path.clone(),
            recovery_info.target_path.clone(),
            recovery_info.file_size,
        );

        // 恢复任务 ID（保持原有 ID）
        task.id = task_id.clone();

        // 设置为暂停状态（等待用户手动恢复）
        task.status = UploadTaskStatus::Paused;

        // 设置已上传字节数
        task.uploaded_size = recovery_info.uploaded_bytes();
        task.created_at = recovery_info.created_at;

        // 设置分片信息
        task.total_chunks = recovery_info.total_chunks;
        task.completed_chunks = recovery_info.completed_count();

        // 创建分片管理器并恢复已完成分片状态
        let mut chunk_manager =
            UploadChunkManager::new(recovery_info.file_size, recovery_info.chunk_size);

        // 标记已完成的分片
        for chunk_index in recovery_info.completed_chunks.iter() {
            let md5 = recovery_info.chunk_md5s.get(chunk_index).cloned().flatten();
            chunk_manager.mark_completed(chunk_index, md5);
        }

        // 计算最大并发分片数
        let max_concurrent_chunks = calculate_upload_task_max_chunks(recovery_info.file_size);

        info!(
            "恢复上传任务: id={}, 文件={:?}, 已完成 {}/{} 分片 ({:.1}%)",
            task_id,
            recovery_info.source_path,
            recovery_info.completed_count(),
            recovery_info.total_chunks,
            if recovery_info.total_chunks > 0 {
                (recovery_info.completed_count() as f64 / recovery_info.total_chunks as f64) * 100.0
            } else {
                0.0
            }
        );

        // 保存任务信息
        let task_info = UploadTaskInfo {
            task: Arc::new(Mutex::new(task)),
            chunk_manager: Arc::new(Mutex::new(chunk_manager)),
            cancel_token: CancellationToken::new(),
            max_concurrent_chunks,
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            is_paused: Arc::new(AtomicBool::new(true)), // 恢复的任务默认暂停
            uploaded_bytes: Arc::new(AtomicU64::new(recovery_info.uploaded_bytes())),
            last_speed_time: Arc::new(Mutex::new(std::time::Instant::now())),
            last_speed_bytes: Arc::new(AtomicU64::new(0)),
            // 🔥 保存恢复的 upload_id（如果存在）
            restored_upload_id: recovery_info.upload_id.clone(),
        };

        self.tasks.insert(task_id.clone(), task_info);

        // 🔥 恢复持久化状态（重新加载到内存）
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.restore_task_state(
                &task_id,
                crate::persistence::TaskType::Upload,
                recovery_info.total_chunks,
            ) {
                warn!("恢复任务持久化状态失败: {}", e);
            }
        }

        Ok(task_id)
    }

    /// 🔥 批量恢复上传任务
    ///
    /// 从恢复信息列表批量创建任务
    ///
    /// # Arguments
    /// * `recovery_infos` - 恢复信息列表
    ///
    /// # Returns
    /// (成功数, 失败数)
    pub async fn restore_tasks(&self, recovery_infos: Vec<UploadRecoveryInfo>) -> (usize, usize) {
        let mut success = 0;
        let mut failed = 0;

        for info in recovery_infos {
            match self.restore_task(info).await {
                Ok(_) => success += 1,
                Err(e) => {
                    warn!("恢复上传任务失败: {}", e);
                    failed += 1;
                }
            }
        }

        info!("上传任务批量恢复完成: {} 成功, {} 失败", success, failed);
        (success, failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UserAuth;
    use crate::AppConfig;
    use std::fs;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn create_test_manager() -> UploadManager {
        let user_auth = UserAuth::new(123456789, "test_user".to_string(), "test_bduss".to_string());
        let client = NetdiskClient::new(user_auth.clone()).unwrap();
        let config = AppConfig::default();
        UploadManager::new_with_config(client, &user_auth, &config.upload)
    }

    #[tokio::test]
    async fn test_create_task() {
        let manager = create_test_manager();

        // 创建临时文件
        let mut temp_file = NamedTempFile::new().unwrap();
        let content = b"Test file content for upload";
        temp_file.write_all(content).unwrap();
        temp_file.flush().unwrap();

        let result = manager
            .create_task(
                temp_file.path().to_path_buf(),
                "/test/upload.txt".to_string(),
            )
            .await;

        assert!(result.is_ok());

        let task_id = result.unwrap();
        let task = manager.get_task(&task_id).await;

        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.status, UploadTaskStatus::Pending);
        assert_eq!(task.total_size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_get_all_tasks() {
        let manager = create_test_manager();

        // 创建多个临时文件和任务
        for i in 0..3 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file
                .write_all(format!("Content {}", i).as_bytes())
                .unwrap();
            temp_file.flush().unwrap();

            manager
                .create_task(
                    temp_file.path().to_path_buf(),
                    format!("/test/file{}.txt", i),
                )
                .await
                .unwrap();
        }

        let tasks = manager.get_all_tasks().await;
        assert_eq!(tasks.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_task() {
        let manager = create_test_manager();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"Test content").unwrap();
        temp_file.flush().unwrap();

        let task_id = manager
            .create_task(
                temp_file.path().to_path_buf(),
                "/test/delete.txt".to_string(),
            )
            .await
            .unwrap();

        // 确认任务存在
        assert!(manager.get_task(&task_id).await.is_some());

        // 删除任务
        manager.delete_task(&task_id).await.unwrap();

        // 确认任务已删除
        assert!(manager.get_task(&task_id).await.is_none());
    }

    #[tokio::test]
    async fn test_create_folder_task() {
        let manager = create_test_manager();

        // 创建测试文件夹结构
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // 创建文件
        fs::write(root.join("file1.txt"), "content1").unwrap();
        fs::write(root.join("file2.txt"), "content2").unwrap();

        // 创建子目录和文件
        fs::create_dir(root.join("subdir")).unwrap();
        fs::write(root.join("subdir/file3.txt"), "content3").unwrap();

        // 创建文件夹上传任务
        let result = manager
            .create_folder_task(root, "/test/folder".to_string(), None)
            .await;

        assert!(result.is_ok());

        let task_ids = result.unwrap();
        assert_eq!(task_ids.len(), 3, "应该创建3个上传任务");

        // 验证所有任务都已创建
        let all_tasks = manager.get_all_tasks().await;
        assert_eq!(all_tasks.len(), 3);

        // 验证任务状态
        for task in all_tasks {
            assert_eq!(task.status, UploadTaskStatus::Pending);
            assert!(task.remote_path.starts_with("/test/folder/"));
        }
    }

    #[tokio::test]
    async fn test_create_folder_task_empty_folder() {
        let manager = create_test_manager();

        // 创建空文件夹
        let temp_dir = TempDir::new().unwrap();

        // 尝试创建文件夹上传任务
        let result = manager
            .create_folder_task(temp_dir.path(), "/test/empty".to_string(), None)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("文件夹为空或无可上传文件"));
    }

    #[tokio::test]
    async fn test_create_batch_tasks() {
        let manager = create_test_manager();

        // 创建多个临时文件
        let mut temp_files = Vec::new();
        for i in 0..3 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file
                .write_all(format!("Content {}", i).as_bytes())
                .unwrap();
            temp_file.flush().unwrap();
            temp_files.push(temp_file);
        }

        // 准备批量任务
        let files: Vec<(PathBuf, String)> = temp_files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.path().to_path_buf(), format!("/test/file{}.txt", i)))
            .collect();

        // 批量创建任务
        let result = manager.create_batch_tasks(files).await;

        assert!(result.is_ok());

        let task_ids = result.unwrap();
        assert_eq!(task_ids.len(), 3);

        // 验证所有任务
        let all_tasks = manager.get_all_tasks().await;
        assert_eq!(all_tasks.len(), 3);
    }
}
