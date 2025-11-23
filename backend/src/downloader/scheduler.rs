use crate::downloader::{ChunkManager, DownloadEngine, DownloadTask, SpeedCalculator, UrlHealthManager};
use anyhow::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
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
    /// 文件总大小（用于探测恢复链接）
    pub total_size: u64,

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
/// 2. 限制全局并发下载的分片数量（动态可调整）
/// 3. 使用 Round-Robin 算法公平调度
/// 4. 为每个分片分配逻辑线程ID，便于日志追踪
#[derive(Debug, Clone)]
pub struct ChunkScheduler {
    /// 活跃任务列表（task_id -> TaskScheduleInfo）
    /// 线程安全：使用 RwLock 保护，读多写少场景
    active_tasks: Arc<RwLock<HashMap<String, TaskScheduleInfo>>>,
    /// 最大全局线程数（动态可调整）
    max_global_threads: Arc<AtomicUsize>,
    /// 当前活跃的分片线程数
    active_chunk_count: Arc<AtomicUsize>,
    /// 分片线程ID分配器
    thread_id_allocator: Arc<ChunkThreadIdAllocator>,
    /// 最大同时下载任务数（动态可调整）
    max_concurrent_tasks: Arc<AtomicUsize>,
    /// 调度器是否正在运行
    scheduler_running: Arc<AtomicBool>,
    /// 预注册计数（正在探测但还未正式注册的任务数）
    pre_register_count: Arc<AtomicUsize>,
    /// 任务完成通知发送器（用于通知 FolderDownloadManager 补充任务）
    task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
    /// 上一轮的任务数（用于检测任务数变化）
    last_task_count: Arc<AtomicUsize>,
}

impl ChunkScheduler {
    /// 创建新的调度器
    pub fn new(max_global_threads: usize, max_concurrent_tasks: usize) -> Self {
        info!(
            "创建全局分片调度器: 全局线程数={}, 最大并发任务数={}",
            max_global_threads, max_concurrent_tasks
        );

        let scheduler = Self {
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            max_global_threads: Arc::new(AtomicUsize::new(max_global_threads)),
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            thread_id_allocator: Arc::new(ChunkThreadIdAllocator::new(max_global_threads)),
            max_concurrent_tasks: Arc::new(AtomicUsize::new(max_concurrent_tasks)),
            scheduler_running: Arc::new(AtomicBool::new(false)),
            pre_register_count: Arc::new(AtomicUsize::new(0)),
            task_completed_tx: Arc::new(RwLock::new(None)),
            last_task_count: Arc::new(AtomicUsize::new(0)),
        };

        // 启动全局调度循环
        scheduler.start_scheduling();

        scheduler
    }

    /// 设置任务完成通知发送器
    ///
    /// FolderDownloadManager 调用此方法设置 channel sender，
    /// 当文件夹子任务完成时会发送 group_id 到 channel
    pub async fn set_task_completed_sender(&self, tx: mpsc::UnboundedSender<String>) {
        let mut sender = self.task_completed_tx.write().await;
        *sender = Some(tx);
        info!("任务完成通知 channel 已设置");
    }

    /// 动态更新最大全局线程数
    ///
    /// 该方法可以在运行时调整线程池大小，无需重启下载管理器
    pub fn update_max_threads(&self, new_max: usize) {
        let old_max = self.max_global_threads.swap(new_max, Ordering::SeqCst);
        info!(
            "🔧 动态调整全局最大线程数: {} -> {}",
            old_max, new_max
        );
    }

    /// 动态更新最大并发任务数
    pub fn update_max_concurrent_tasks(&self, new_max: usize) {
        let old_max = self.max_concurrent_tasks.swap(new_max, Ordering::SeqCst);
        info!(
            "🔧 动态调整最大并发任务数: {} -> {}",
            old_max, new_max
        );
    }

    /// 获取当前最大线程数
    pub fn max_threads(&self) -> usize {
        self.max_global_threads.load(Ordering::SeqCst)
    }

    /// 获取当前活跃分片线程数
    pub fn active_threads(&self) -> usize {
        self.active_chunk_count.load(Ordering::SeqCst)
    }

    /// 预注册任务（在 spawn 探测前调用）
    ///
    /// 返回 true 表示预注册成功，可以开始探测
    /// 返回 false 表示已达并发上限，不应启动探测
    /// 预注册上限 = max_concurrent_tasks，避免探测占用下载带宽
    pub async fn pre_register(&self) -> bool {
        let max_tasks = self.max_concurrent_tasks.load(Ordering::SeqCst);
        // 预注册上限 = max_tasks，不允许额外探测任务（避免探测占用下载带宽）
        let pre_register_limit = max_tasks;
        let registered_count = self.active_tasks.read().await.len();

        loop {
            let current_pre = self.pre_register_count.load(Ordering::SeqCst);
            let total = registered_count + current_pre;

            // 检查总数（已注册 + 预注册）是否超过预注册上限
            if total >= pre_register_limit {
                info!(
                    "预注册失败：总数已达上限 (已注册:{} + 预注册:{} = {} >= {})",
                    registered_count, current_pre, total, pre_register_limit
                );
                return false;
            }

            // CAS 操作，确保原子性
            match self.pre_register_count.compare_exchange(
                current_pre,
                current_pre + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    info!(
                        "预注册成功：已注册:{} + 预注册:{} -> {} (上限: {})",
                        registered_count, current_pre, current_pre + 1, pre_register_limit
                    );
                    return true;
                }
                Err(_) => {
                    // CAS 失败，重试
                    continue;
                }
            }
        }
    }

    /// 获取预注册余量（还能预注册多少个任务）
    pub async fn pre_register_available(&self) -> usize {
        let max_tasks = self.max_concurrent_tasks.load(Ordering::SeqCst);
        let pre_register_limit = max_tasks;
        let registered_count = self.active_tasks.read().await.len();
        let current_pre = self.pre_register_count.load(Ordering::SeqCst);
        let total = registered_count + current_pre;
        pre_register_limit.saturating_sub(total)
    }

    /// 取消预注册（探测失败或被取消时调用）
    pub fn cancel_pre_register(&self) {
        let old = self.pre_register_count.fetch_sub(1, Ordering::SeqCst);
        info!("取消预注册：预注册数 {} -> {}", old, old.saturating_sub(1));
    }

    /// 获取预注册计数
    pub fn pre_register_count(&self) -> usize {
        self.pre_register_count.load(Ordering::SeqCst)
    }

    /// 注册任务到调度器
    ///
    /// 注册成功后会自动减少预注册计数
    /// 如果当前活跃任务数已达上限，返回错误（此时调用者需要调用 cancel_pre_register）
    pub async fn register_task(&self, task_info: TaskScheduleInfo) -> Result<()> {
        let task_id = task_info.task_id.clone();
        let max_tasks = self.max_concurrent_tasks.load(Ordering::SeqCst);

        // 检查是否超过最大并发任务数（双重检查，理论上预注册已确保）
        {
            let tasks = self.active_tasks.read().await;
            if tasks.len() >= max_tasks {
                // 注意：调用者需要调用 cancel_pre_register()
                anyhow::bail!(
                    "超过最大并发任务数限制 ({}/{})",
                    tasks.len(),
                    max_tasks
                );
            }
        }

        // 添加到活跃任务列表
        self.active_tasks.write().await.insert(task_id.clone(), task_info);

        // 注册成功，减少预注册计数
        let old_pre = self.pre_register_count.fetch_sub(1, Ordering::SeqCst);
        info!(
            "任务 {} 已注册到调度器 (预注册数: {} -> {})",
            task_id, old_pre, old_pre.saturating_sub(1)
        );
        Ok(())
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) {
        if let Some(task_info) = self.active_tasks.write().await.remove(task_id) {
            task_info.cancellation_token.cancel();
            info!("任务 {} 已从调度器移除并取消", task_id);
        }
    }

    /// 获取活跃任务数量（包括已注册和预注册的任务）
    pub async fn active_task_count(&self) -> usize {
        let registered = self.active_tasks.read().await.len();
        let pre_registered = self.pre_register_count.load(Ordering::SeqCst);
        registered + pre_registered
    }

    /// 启动全局调度循环
    ///
    /// 核心调度算法：
    /// 1. 轮询所有活跃任务
    /// 2. 每次从当前任务选择一个待下载的分片
    /// 3. 检查当前活跃线程数是否小于最大限制（动态）
    /// 4. 如果未达上限，启动分片下载
    ///
    /// 线程安全：
    /// - active_tasks 使用 RwLock 保护
    /// - task_info 被 clone，即使原始任务从 HashMap 中移除也不影响
    /// - 所有字段都是 Arc 包装，引用计数安全
    fn start_scheduling(&self) {
        let active_tasks = self.active_tasks.clone();
        let max_global_threads = self.max_global_threads.clone();
        let active_chunk_count = self.active_chunk_count.clone();
        let thread_id_allocator = self.thread_id_allocator.clone();
        let scheduler_running = self.scheduler_running.clone();
        let task_completed_tx = self.task_completed_tx.clone();
        let last_task_count = self.last_task_count.clone();

        // 标记调度器正在运行
        scheduler_running.store(true, Ordering::SeqCst);

        info!("🚀 全局分片调度循环已启动");

        tokio::spawn(async move {
            let mut round_robin_counter: usize = 0;

            while scheduler_running.load(Ordering::SeqCst) {
                // 获取所有活跃任务 ID（排序确保顺序稳定，保证 round-robin 公平性）
                let task_ids: Vec<String> = {
                    let tasks = active_tasks.read().await;
                    let mut ids: Vec<String> = tasks.keys().cloned().collect();
                    ids.sort();
                    ids
                };

                let current_task_count = task_ids.len();

                // 🔥 检测任务数增加，触发速度窗口重置
                {
                    let last_count = last_task_count.load(Ordering::SeqCst);
                    if current_task_count > last_count && last_count > 0 {
                        info!(
                            "🔄 检测到任务数增加: {} -> {}, 重置所有链接速度窗口（带宽重新分配）",
                            last_count, current_task_count
                        );

                        // 遍历所有任务，重置速度窗口
                        let tasks = active_tasks.read().await;
                        for task_info in tasks.values() {
                            let mut health = task_info.url_health.lock().await;
                            health.reset_speed_windows();
                        }
                    }

                    // 更新任务数记录
                    last_task_count.store(current_task_count, Ordering::SeqCst);
                }

                if task_ids.is_empty() {
                    // 没有活跃任务，等待
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }

                // 🔥 批量调度：尽可能填满所有空闲线程，同时保持公平性
                let mut scheduled_count = 0;
                let max_threads = max_global_threads.load(Ordering::SeqCst);
                let current_active = active_chunk_count.load(Ordering::SeqCst);

                // 检查是否有空闲线程
                if current_active >= max_threads {
                    // 所有线程已满，等待
                    tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                    continue;
                }

                // 计算可用线程数
                let available_slots = max_threads.saturating_sub(current_active);

                // 🎯 关键：轮询所有任务，每个任务最多调度1个分片，保证公平性
                // 持续轮询直到填满所有空闲线程或所有任务都没有待下载分片
                let mut consecutive_empty_rounds = 0;
                let task_count = task_ids.len();

                for _ in 0..available_slots {
                    // 轮询选择下一个任务
                    let task_id = &task_ids[round_robin_counter % task_count];
                    round_robin_counter = round_robin_counter.wrapping_add(1);

                    // 获取任务信息
                    let task_info_opt = {
                        let tasks = active_tasks.read().await;
                        tasks.get(task_id).cloned()
                    };

                    let task_info = match task_info_opt {
                        Some(info) => info,
                        None => {
                            consecutive_empty_rounds += 1;
                            if consecutive_empty_rounds >= task_count {
                                // 所有任务都检查过了，没有可调度的
                                break;
                            }
                            continue;
                        }
                    };

                    // 检查任务是否被取消
                    if task_info.cancellation_token.is_cancelled() {
                        info!("任务 {} 已被取消，从调度器移除", task_id);
                        active_tasks.write().await.remove(task_id);
                        consecutive_empty_rounds += 1;
                        if consecutive_empty_rounds >= task_count {
                            break;
                        }
                        continue;
                    }

                    // 获取下一个待下载的分片索引（跳过正在下载的分片）
                    let next_chunk_index = {
                        let mut manager = task_info.chunk_manager.lock().await;
                        // 找到第一个未完成且未在下载的分片
                        let index = manager
                            .chunks()
                            .iter()
                            .position(|chunk| !chunk.completed && !chunk.downloading);

                        // 如果找到，立即标记为"正在下载"，防止其他线程重复调度
                        if let Some(idx) = index {
                            manager.mark_downloading(idx);
                        }

                        index
                    };

                    match next_chunk_index {
                        Some(chunk_index) => {
                            // 原子增加活跃计数
                            active_chunk_count.fetch_add(1, Ordering::SeqCst);
                            task_info.active_chunk_count.fetch_add(1, Ordering::SeqCst);

                            let new_active = active_chunk_count.load(Ordering::SeqCst);

                            debug!(
                                "调度器选择: 任务 {} 分片 #{} (活跃线程: {}/{}, 本轮已调度: {})",
                                task_id, chunk_index, new_active, max_threads, scheduled_count + 1
                            );

                            Self::spawn_chunk_download(
                                chunk_index,
                                task_info.clone(),
                                active_tasks.clone(),
                                thread_id_allocator.clone(),
                                active_chunk_count.clone(),
                            );

                            scheduled_count += 1;
                            consecutive_empty_rounds = 0; // 重置计数器

                            // 继续下一个任务（保证公平轮询）
                        }
                        None => {
                            // 该任务没有待下载的分片
                            // 检查是否所有分片都完成
                            if task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                                // 所有分片完成，从调度器移除
                                info!("任务 {} 所有分片完成，从调度器移除", task_id);
                                active_tasks.write().await.remove(task_id);

                                // 标记任务完成，并获取 group_id
                                let group_id = {
                                    let mut t = task_info.task.lock().await;
                                    t.mark_completed();
                                    t.group_id.clone()
                                };

                                // 如果是文件夹子任务，通知补充新任务
                                if let Some(gid) = group_id {
                                    let tx_guard = task_completed_tx.read().await;
                                    if let Some(tx) = tx_guard.as_ref() {
                                        if let Err(e) = tx.send(gid.clone()) {
                                            error!("发送任务完成通知失败: {}", e);
                                        } else {
                                            debug!("已发送任务完成通知: group_id={}", gid);
                                        }
                                    }
                                }
                            }

                            consecutive_empty_rounds += 1;
                            if consecutive_empty_rounds >= task_count {
                                // 所有任务都检查过了，没有可调度的分片
                                break;
                            }
                            // 继续下一个任务
                        }
                    }
                }

                if scheduled_count > 0 {
                    debug!("本轮调度完成，共启动 {} 个分片", scheduled_count);
                }

                // 短暂延迟，避免 CPU 占用过高
                // 减少到 2ms 以提高响应速度
                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
            }

            info!("全局分片调度循环已停止");
        });
    }

    /// 启动单个分片的下载任务
    ///
    /// # 参数
    /// * `chunk_index` - 分片索引
    /// * `task_info` - 任务信息
    /// * `active_tasks` - 活跃任务列表（用于在失败时移除任务）
    /// * `thread_id_allocator` - 线程ID分配器
    /// * `global_active_count` - 全局活跃分片计数器
    fn spawn_chunk_download(
        chunk_index: usize,
        task_info: TaskScheduleInfo,
        active_tasks: Arc<RwLock<HashMap<String, TaskScheduleInfo>>>,
        thread_id_allocator: Arc<ChunkThreadIdAllocator>,
        global_active_count: Arc<AtomicUsize>,
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
                task_info.chunk_size,
                task_info.total_size,
                task_info.cancellation_token.clone(),
                chunk_thread_id, // 传递分片线程ID
            )
                .await;

            // 释放全局活跃分片计数
            global_active_count.fetch_sub(1, Ordering::SeqCst);

            // 减少任务内活跃分片计数
            task_info.active_chunk_count.fetch_sub(1, Ordering::SeqCst);

            info!("[分片线程{}] 分片 #{} 释放线程资源", chunk_thread_id, chunk_index);

            // 处理下载结果
            if let Err(e) = result {
                // 检查是否是因为取消而失败
                if task_info.cancellation_token.is_cancelled() {
                    info!("[分片线程{}] 分片 #{} 因任务取消而失败", chunk_thread_id, chunk_index);
                } else {
                    error!("[分片线程{}] 分片 #{} 下载失败: {}", chunk_thread_id, chunk_index, e);

                    // 取消下载标记（允许重新调度）
                    {
                        let mut manager = task_info.chunk_manager.lock().await;
                        manager.unmark_downloading(chunk_index);
                    }

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

