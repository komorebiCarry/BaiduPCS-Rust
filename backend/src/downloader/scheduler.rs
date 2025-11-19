use crate::downloader::{ChunkManager, DownloadEngine, DownloadTask, SpeedCalculator, UrlHealthManager};
use anyhow::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// 分片线程ID分配器
///
/// 为每个正在下载的分片分配一个逻辑的线程ID（1, 2, 3...max_global_threads）
/// 使得日志更清晰易读
#[derive(Debug)]
struct ChunkThreadIdAllocator {
    /// 下一个可用的线程ID（循环使用）
    next_id: AtomicUsize,
    /// 最大线程数
    max_threads: usize,
}

impl ChunkThreadIdAllocator {
    fn new(max_threads: usize) -> Self {
        Self {
            next_id: AtomicUsize::new(1),
            max_threads,
        }
    }

    /// 分配一个线程ID（1 到 max_threads）
    fn allocate(&self) -> usize {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        // 循环使用 1 到 max_threads
        ((id - 1) % self.max_threads) + 1
    }
}

/// 任务调度信息
#[derive(Debug, Clone)]
pub struct TaskScheduleInfo {
    /// 任务 ID
    pub task_id: String,
    /// 任务引用
    pub task: Arc<Mutex<DownloadTask>>,
    /// 分片管理器
    pub chunk_manager: Arc<Mutex<ChunkManager>>,
    /// 速度计算器
    pub speed_calc: Arc<Mutex<SpeedCalculator>>,

    // 下载所需的配置
    /// HTTP 客户端
    pub client: Client,
    /// Cookie
    pub cookie: String,
    /// Referer 头
    pub referer: Option<String>,
    /// URL 健康管理器
    pub url_health: Arc<Mutex<UrlHealthManager>>,
    /// 输出路径
    pub output_path: PathBuf,
    /// 分片大小
    pub chunk_size: u64,
    /// 超时时间（秒）
    pub timeout_secs: u64,

    // 控制
    /// 取消令牌
    pub cancellation_token: CancellationToken,

    // 统计
    /// 当前正在下载的分片数
    pub active_chunk_count: Arc<AtomicUsize>,
}

/// 全局分片调度器
///
/// 负责公平调度所有下载任务的分片，实现：
/// 1. 限制同时下载的任务数量（max_concurrent_tasks）
/// 2. 限制全局并发下载的分片数量（global_semaphore）
/// 3. 使用 Round-Robin 算法公平调度
/// 4. 为每个分片分配逻辑线程ID，便于日志追踪
#[derive(Debug, Clone)]
pub struct ChunkScheduler {
    /// 活跃任务列表（task_id -> TaskScheduleInfo）
    /// 线程安全：使用 RwLock 保护，读多写少场景
    active_tasks: Arc<RwLock<HashMap<String, TaskScheduleInfo>>>,
    /// 全局线程池（Semaphore）
    global_semaphore: Arc<Semaphore>,
    /// 分片线程ID分配器
    thread_id_allocator: Arc<ChunkThreadIdAllocator>,
    /// 最大同时下载任务数
    max_concurrent_tasks: usize,
    /// 调度器是否正在运行
    scheduler_running: Arc<AtomicBool>,
}

impl ChunkScheduler {
    /// 创建新的调度器
    pub fn new(global_semaphore: Arc<Semaphore>, max_concurrent_tasks: usize) -> Self {
        let max_threads = global_semaphore.available_permits();

        info!(
            "创建全局分片调度器: 全局线程数={}, 最大并发任务数={}",
            max_threads, max_concurrent_tasks
        );

        let scheduler = Self {
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            global_semaphore,
            thread_id_allocator: Arc::new(ChunkThreadIdAllocator::new(max_threads)),
            max_concurrent_tasks,
            scheduler_running: Arc::new(AtomicBool::new(false)),
        };

        // 启动全局调度循环
        scheduler.start_scheduling();

        scheduler
    }

    /// 注册任务到调度器
    ///
    /// 如果当前活跃任务数已达上限，返回错误
    pub async fn register_task(&self, task_info: TaskScheduleInfo) -> Result<()> {
        let task_id = task_info.task_id.clone();

        // 检查是否超过最大并发任务数
        {
            let tasks = self.active_tasks.read().await;
            if tasks.len() >= self.max_concurrent_tasks {
                anyhow::bail!(
                    "超过最大并发任务数限制 ({}/{})",
                    tasks.len(),
                    self.max_concurrent_tasks
                );
            }
        }

        // 添加到活跃任务列表
        self.active_tasks.write().await.insert(task_id.clone(), task_info);

        info!("任务 {} 已注册到调度器", task_id);
        Ok(())
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) {
        if let Some(task_info) = self.active_tasks.write().await.remove(task_id) {
            task_info.cancellation_token.cancel();
            info!("任务 {} 已从调度器移除并取消", task_id);
        }
    }

    /// 获取活跃任务数量
    pub async fn active_task_count(&self) -> usize {
        self.active_tasks.read().await.len()
    }

    /// 启动全局调度循环
    ///
    /// 核心调度算法：
    /// 1. 轮询所有活跃任务
    /// 2. 每次从当前任务选择一个待下载的分片
    /// 3. 尝试获取 semaphore permit（非阻塞）
    /// 4. 如果成功，启动分片下载
    ///
    /// 线程安全：
    /// - active_tasks 使用 RwLock 保护
    /// - task_info 被 clone，即使原始任务从 HashMap 中移除也不影响
    /// - 所有字段都是 Arc 包装，引用计数安全
    fn start_scheduling(&self) {
        let active_tasks = self.active_tasks.clone();
        let global_semaphore = self.global_semaphore.clone();
        let thread_id_allocator = self.thread_id_allocator.clone();
        let scheduler_running = self.scheduler_running.clone();

        // 标记调度器正在运行
        scheduler_running.store(true, Ordering::SeqCst);

        info!("🚀 全局分片调度循环已启动");

        tokio::spawn(async move {
            let mut round_robin_counter: usize = 0;

            while scheduler_running.load(Ordering::SeqCst) {
                // 获取所有活跃任务 ID
                let task_ids: Vec<String> = {
                    let tasks = active_tasks.read().await;
                    tasks.keys().cloned().collect()
                };

                if task_ids.is_empty() {
                    // 没有活跃任务，等待
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }

                // 轮询选择下一个任务
                let task_id = &task_ids[round_robin_counter % task_ids.len()];
                round_robin_counter = round_robin_counter.wrapping_add(1);

                // 获取任务信息
                let task_info_opt = {
                    let tasks = active_tasks.read().await;
                    tasks.get(task_id).cloned()
                };

                let task_info = match task_info_opt {
                    Some(info) => info,
                    None => continue,
                };

                // 检查任务是否被取消
                if task_info.cancellation_token.is_cancelled() {
                    info!("任务 {} 已被取消，从调度器移除", task_id);
                    active_tasks.write().await.remove(task_id);
                    continue;
                }

                // 获取下一个待下载的分片索引
                let next_chunk_index = {
                    let manager = task_info.chunk_manager.lock().await;
                    // 找到第一个未完成的分片
                    manager
                        .chunks()
                        .iter()
                        .position(|chunk| !chunk.completed)
                };

                match next_chunk_index {
                    Some(chunk_index) => {
                        // 尝试获取 permit（非阻塞，避免某个任务阻塞调度循环）
                        match global_semaphore.clone().try_acquire_owned() {
                            Ok(permit) => {
                                // 成功获取 permit，启动分片下载
                                task_info.active_chunk_count.fetch_add(1, Ordering::SeqCst);

                                debug!(
                                    "调度器选择: 任务 {} 分片 #{}",
                                    task_id, chunk_index
                                );

                                Self::spawn_chunk_download(
                                    chunk_index,
                                    task_info.clone(),
                                    permit,
                                    active_tasks.clone(),
                                    thread_id_allocator.clone(),
                                );
                            }
                            Err(_) => {
                                // 没有可用的 permit，继续下一个任务
                                // 不需要日志，避免刷屏
                            }
                        }
                    }
                    None => {
                        // 该任务没有待下载的分片
                        // 检查是否所有分片都完成
                        if task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                            // 所有分片完成，从调度器移除
                            info!("任务 {} 所有分片完成，从调度器移除", task_id);
                            active_tasks.write().await.remove(task_id);

                            // 标记任务完成
                            {
                                let mut t = task_info.task.lock().await;
                                t.mark_completed();
                            }
                        }
                    }
                }

                // 短暂延迟，避免 CPU 占用过高
                // 10ms 延迟在大多数情况下不会影响性能
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }

            info!("全局分片调度循环已停止");
        });
    }

    /// 启动单个分片的下载任务
    ///
    /// # 参数
    /// * `chunk_index` - 分片索引
    /// * `task_info` - 任务信息
    /// * `permit` - Semaphore permit（下载完成后释放）
    /// * `active_tasks` - 活跃任务列表（用于在失败时移除任务）
    /// * `thread_id_allocator` - 线程ID分配器
    fn spawn_chunk_download(
        chunk_index: usize,
        task_info: TaskScheduleInfo,
        permit: OwnedSemaphorePermit,
        active_tasks: Arc<RwLock<HashMap<String, TaskScheduleInfo>>>,
        thread_id_allocator: Arc<ChunkThreadIdAllocator>,
    ) {
        tokio::spawn(async move {
            let task_id = task_info.task_id.clone();

            // 分配分片线程ID
            let chunk_thread_id = thread_id_allocator.allocate();

            info!(
                "[分片线程{}] 分片 #{} 获得线程资源，开始下载",
                chunk_thread_id, chunk_index
            );

            // 调用 DownloadEngine 的下载方法
            let result = DownloadEngine::download_chunk_with_retry(
                chunk_index,
                task_info.client.clone(),
                &task_info.cookie,
                task_info.referer.as_deref(),
                task_info.url_health.clone(),
                &task_info.output_path,
                task_info.chunk_manager.clone(),
                task_info.speed_calc.clone(),
                task_info.task.clone(),
                task_info.timeout_secs,
                task_info.cancellation_token.clone(),
                chunk_thread_id, // 传递分片线程ID
            )
            .await;

            // 释放 permit（其他分片可以使用）
            drop(permit);

            // 减少活跃分片计数
            task_info.active_chunk_count.fetch_sub(1, Ordering::SeqCst);

            info!("[分片线程{}] 分片 #{} 释放线程资源", chunk_thread_id, chunk_index);

            // 处理下载结果
            if let Err(e) = result {
                // 检查是否是因为取消而失败
                if task_info.cancellation_token.is_cancelled() {
                    info!("[分片线程{}] 分片 #{} 因任务取消而失败", chunk_thread_id, chunk_index);
                } else {
                    error!("[分片线程{}] 分片 #{} 下载失败: {}", chunk_thread_id, chunk_index, e);

                    // 标记任务失败
                    {
                        let mut t = task_info.task.lock().await;
                        t.mark_failed(e.to_string());
                    }

                    // 从调度器移除任务
                    active_tasks.write().await.remove(&task_id);
                    error!("任务 {} 因分片下载失败已从调度器移除", task_id);
                }
            }
        });
    }

    /// 停止调度器
    pub fn stop(&self) {
        self.scheduler_running.store(false, Ordering::SeqCst);
        info!("调度器停止信号已发送");
    }
}

