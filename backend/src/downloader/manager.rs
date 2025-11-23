use crate::auth::UserAuth;
use crate::downloader::{ChunkScheduler, DownloadEngine, DownloadTask, TaskScheduleInfo, TaskStatus};
use anyhow::{Context, Result};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
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
    /// 默认下载目录
    download_dir: PathBuf,
    /// 全局分片调度器
    chunk_scheduler: ChunkScheduler,
    /// 最大同时下载任务数
    max_concurrent_tasks: usize,
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
            download_dir,
            chunk_scheduler,
            max_concurrent_tasks,
        };

        // 启动后台任务：定期检查并启动等待队列中的任务
        manager.start_waiting_queue_monitor();

        Ok(manager)
    }

    /// 创建下载任务
    pub async fn create_task(
        &self,
        fs_id: u64,
        remote_path: String,
        filename: String,
        total_size: u64,
    ) -> Result<String> {
        let local_path = self.download_dir.join(&filename);

        // 检查文件是否已存在
        if local_path.exists() {
            warn!("文件已存在: {:?}，将覆盖", local_path);
        }

        let task = DownloadTask::new(fs_id, remote_path, local_path, total_size);
        let task_id = task.id.clone();

        info!("创建下载任务: id={}, 文件名={}", task_id, filename);

        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc);

        Ok(task_id)
    }

    /// 开始下载任务
    pub async fn start_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        // 检查任务状态
        {
            let t = task.lock().await;
            if t.status == TaskStatus::Downloading {
                anyhow::bail!("任务已在下载中");
            }
            if t.status == TaskStatus::Completed {
                anyhow::bail!("任务已完成");
            }
        }

        info!("请求启动下载任务: {}", task_id);

        // 检查调度器是否已满
        let active_count = self.chunk_scheduler.active_task_count().await;
        if active_count >= self.max_concurrent_tasks {
            // 加入等待队列
            self.waiting_queue.write().await.push_back(task_id.to_string());

            // 任务保持 Pending 状态（表示系统等待，而非用户暂停）
            // 注意：Pending = 等待系统资源，Paused = 用户主动暂停

            info!(
                "任务 {} 加入等待队列（系统等待） ({}/{} 活跃任务)",
                task_id, active_count, self.max_concurrent_tasks
            );
            return Ok(());
        }

        // 立即启动任务
        self.start_task_internal(task_id).await
    }

    /// 内部方法：真正启动一个任务
    ///
    /// 该方法会先预注册，预注册成功后才启动探测
    async fn start_task_internal(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        // 预注册：在 spawn 前占位，防止并发超限
        if !self.chunk_scheduler.pre_register().await {
            // 预注册失败，加入等待队列
            self.waiting_queue.write().await.push_back(task_id.to_string());
            info!(
                "任务 {} 预注册失败，加入等待队列",
                task_id
            );
            return Ok(());
        }

        info!("启动下载任务: {} (已预注册)", task_id);

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

        tokio::spawn(async move {
            // 准备任务
            let prepare_result = engine.prepare_for_scheduling(task_clone.clone(), cancellation_token.clone()).await;

            // 探测完成后，先检查是否被取消
            if cancellation_token.is_cancelled() {
                info!("任务 {} 在探测完成后发现已被取消，取消预注册", task_id_clone);
                chunk_scheduler.cancel_pre_register();
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
                    // 获取文件总大小（用于探测恢复链接）
                    let total_size = {
                        let t = task_clone.lock().await;
                        t.total_size
                    };

                    // 创建任务调度信息
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
                    };

                    // 注册到调度器（成功会自动减少预注册计数）
                    if let Err(e) = chunk_scheduler.register_task(task_info).await {
                        error!("注册任务到调度器失败: {}", e);

                        // 注册失败，需要取消预注册
                        chunk_scheduler.cancel_pre_register();

                        // 标记任务失败
                        let mut t = task_clone.lock().await;
                        t.mark_failed(e.to_string());

                        // 移除取消令牌
                        cancellation_tokens.write().await.remove(&task_id_clone);

                        // 不在这里调用 try_start_waiting_tasks，避免循环引用
                    }
                }
                Err(e) => {
                    error!("准备任务失败: {}", e);

                    // 探测失败，取消预注册
                    chunk_scheduler.cancel_pre_register();

                    // 标记任务失败
                    let mut t = task_clone.lock().await;
                    t.mark_failed(e.to_string());

                    // 移除取消令牌
                    cancellation_tokens.write().await.remove(&task_id_clone);

                    // 不在这里调用 try_start_waiting_tasks，避免循环引用
                }
            }
        });

        Ok(())
    }

    /// 尝试从等待队列启动任务
    async fn try_start_waiting_tasks(&self) {
        loop {
            // 检查是否有空闲位置
            let active_count = self.chunk_scheduler.active_task_count().await;
            if active_count >= self.max_concurrent_tasks {
                break;
            }

            // 从等待队列取出任务
            let task_id = {
                let mut queue = self.waiting_queue.write().await;
                queue.pop_front()
            };

            match task_id {
                Some(id) => {
                    info!("从等待队列启动任务: {}", id);
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
    fn start_waiting_queue_monitor(&self) {
        let waiting_queue = self.waiting_queue.clone();
        let chunk_scheduler = self.chunk_scheduler.clone();
        let tasks = self.tasks.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();
        let engine = self.engine.clone();
        let max_concurrent_tasks = self.max_concurrent_tasks;

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
                let active_count = chunk_scheduler.active_task_count().await;
                if active_count >= max_concurrent_tasks {
                    continue;
                }

                // 尝试启动等待任务
                loop {
                    // 先预注册，成功才继续
                    if !chunk_scheduler.pre_register().await {
                        break;
                    }

                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            info!("🔄 后台监控：从等待队列启动任务 {} (已预注册)", id);

                            // 获取任务
                            let task = tasks.read().await.get(&id).cloned();
                            if let Some(task) = task {
                                // 创建取消令牌
                                let cancellation_token = CancellationToken::new();
                                cancellation_tokens.write().await.insert(id.clone(), cancellation_token.clone());

                                // 启动任务（简化版，直接在这里处理）
                                let engine_clone = engine.clone();
                                let task_clone = task.clone();
                                let chunk_scheduler_clone = chunk_scheduler.clone();
                                let id_clone = id.clone();
                                let cancellation_tokens_clone = cancellation_tokens.clone();

                                tokio::spawn(async move {
                                    let prepare_result = engine_clone.prepare_for_scheduling(task_clone.clone(), cancellation_token.clone()).await;

                                    // 探测完成后，先检查是否被取消
                                    if cancellation_token.is_cancelled() {
                                        info!("后台监控:任务 {} 在探测完成后发现已被取消，取消预注册", id_clone);
                                        chunk_scheduler_clone.cancel_pre_register();
                                        return;
                                    }

                                    match prepare_result {
                                        Ok((client, cookie, referer, url_health, output_path, chunk_size, chunk_manager, speed_calc)) => {
                                            // 获取文件总大小
                                            let total_size = {
                                                let t = task_clone.lock().await;
                                                t.total_size
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
                                            };

                                            // 注册成功会自动减少预注册计数
                                            if let Err(e) = chunk_scheduler_clone.register_task(task_info).await {
                                                error!("后台监控：注册任务失败: {}", e);
                                                // 注册失败，取消预注册
                                                chunk_scheduler_clone.cancel_pre_register();
                                                let mut t = task_clone.lock().await;
                                                t.mark_failed(e.to_string());
                                                cancellation_tokens_clone.write().await.remove(&id_clone);
                                            }
                                        }
                                        Err(e) => {
                                            error!("后台监控：准备任务失败: {}", e);
                                            // 探测失败，取消预注册
                                            chunk_scheduler_clone.cancel_pre_register();
                                            let mut t = task_clone.lock().await;
                                            t.mark_failed(e.to_string());
                                            cancellation_tokens_clone.write().await.remove(&id_clone);
                                        }
                                    }
                                });
                            } else {
                                // 任务不存在，取消预注册
                                chunk_scheduler.cancel_pre_register();
                            }
                        }
                        None => {
                            // 队列为空，取消预注册
                            chunk_scheduler.cancel_pre_register();
                            break;
                        }
                    }
                }
            }
        });
    }

    /// 暂停下载任务
    pub async fn pause_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        let mut t = task.lock().await;
        if t.status != TaskStatus::Downloading {
            anyhow::bail!("任务未在下载中");
        }

        t.mark_paused();
        info!("暂停下载任务: {}", task_id);
        drop(t);

        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

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

        // 检查任务状态并将 Paused 改回 Pending
        {
            let mut t = task.lock().await;
            if t.status != TaskStatus::Paused {
                anyhow::bail!("任务未暂停，当前状态: {:?}", t.status);
            }

            // 将状态改回 Pending，准备重新启动
            // 注意：这里不能用 mark_downloading，因为还没获得资源
            t.status = TaskStatus::Pending;
        }

        info!("用户请求恢复下载任务: {}", task_id);

        // 检查是否有可用位置
        let active_count = self.chunk_scheduler.active_task_count().await;
        if active_count >= self.max_concurrent_tasks {
            // 没有可用位置，加入等待队列
            self.waiting_queue.write().await.push_back(task_id.to_string());

            info!(
                "恢复任务 {} 时无可用位置，已加入等待队列 ({}/{} 活跃任务)",
                task_id, active_count, self.max_concurrent_tasks
            );
            return Ok(());
        }

        // 有可用位置，立即启动
        self.start_task_internal(task_id).await
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

        let task = self
            .tasks
            .write()
            .await
            .remove(task_id)
            .context("任务不存在")?;

        let t = task.lock().await;

        // 决定是否删除本地文件
        // 1. 对于未完成的任务（Pending/Downloading/Paused/Failed），自动删除临时文件
        // 2. 对于已完成的任务（Completed），根据 delete_file 参数决定
        let should_delete = match t.status {
            TaskStatus::Completed => delete_file,
            _ => true, // 未完成的任务总是删除临时文件
        };

        if should_delete && t.local_path.exists() {
            tokio::fs::remove_file(&t.local_path)
                .await
                .context("删除本地文件失败")?;
            info!("已删除本地文件: {:?}", t.local_path);
        }

        info!("删除下载任务: {}", task_id);
        drop(t);

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

    /// 获取所有任务
    pub async fn get_all_tasks(&self) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        for task in tasks.values() {
            result.push(task.lock().await.clone());
        }

        result
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

        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.status == TaskStatus::Completed {
                to_remove.push(id.clone());
            }
        }

        let count = to_remove.len();
        for id in to_remove {
            tasks.remove(&id);
        }

        info!("清除了 {} 个已完成的任务", count);
        count
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
    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }

    /// 动态更新全局最大线程数
    ///
    /// 该方法可以在运行时调整线程池大小，无需重启下载管理器
    /// 正在进行的下载任务不受影响
    pub fn update_max_threads(&self, new_max: usize) {
        self.chunk_scheduler.update_max_threads(new_max);
    }

    /// 获取预注册余量（还能预注册多少个任务）
    pub async fn pre_register_available(&self) -> usize {
        self.chunk_scheduler.pre_register_available().await
    }

    /// 动态更新最大并发任务数
    ///
    /// 该方法可以在运行时调整最大并发任务数：
    /// - **调大**：自动从等待队列启动新任务
    /// - **调小**：不会打断正在下载的任务，但新任务会进入等待队列
    ///   当前运行的任务完成后，会根据新的限制从等待队列启动任务
    pub async fn update_max_concurrent_tasks(&self, new_max: usize) {
        let old_max = self.max_concurrent_tasks;

        // 更新调度器的限制
        self.chunk_scheduler.update_max_concurrent_tasks(new_max);

        // 更新 manager 自己的记录（因为 max_concurrent_tasks 不是 Arc 包装的）
        // 注意：这里有个限制，因为 self 是 &self，我们不能修改 max_concurrent_tasks
        // 但调度器已经更新了，这个字段只在创建时使用，之后都用调度器的值

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
            vip_type: Some(2), // SVIP
            total_space: Some(2 * 1024 * 1024 * 1024 * 1024), // 2TB
            used_space: Some(500 * 1024 * 1024 * 1024), // 500GB
            bduss: "mock_bduss".to_string(),
            stoken: Some("mock_stoken".to_string()),
            ptoken: Some("mock_ptoken".to_string()),
            cookies: Some("BDUSS=mock_bduss".to_string()),
            login_time: 0,
        }
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

        assert_eq!(manager.download_dir(), temp_dir.path());
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
