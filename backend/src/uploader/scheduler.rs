// 上传分片调度器
//
//  实现：全局上传调度器
//
// 功能：
// - Round-Robin 公平调度多个上传任务
// - 全局并发控制（限制同时上传的分片数）
// - 任务级并发控制（根据文件大小自动计算）
// - 预注册机制（避免探测占用上传带宽）
// - 槽位池管理线程ID（日志追踪）
// - 检测任务数变化，重置服务器速度窗口

use crate::netdisk::{NetdiskClient, UploadErrorKind};
use crate::uploader::{
    PcsServerHealthManager, UploadChunk, UploadChunkManager, UploadTask,
};
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// =====================================================
// 重试配置
// =====================================================

/// 默认最大重试次数
const DEFAULT_MAX_RETRIES: u32 = 3;

/// 初始退避延迟（毫秒）
const INITIAL_BACKOFF_MS: u64 = 100;

/// 最大退避延迟（毫秒）
const MAX_BACKOFF_MS: u64 = 5000;

/// 限流时的额外等待时间（毫秒）
const RATE_LIMIT_BACKOFF_MS: u64 = 10000;

/// 计算指数退避延迟
fn calculate_backoff_delay(retry_count: u32, error_kind: &UploadErrorKind) -> u64 {
    let base_delay = INITIAL_BACKOFF_MS * 2u64.pow(retry_count);
    let delay = base_delay.min(MAX_BACKOFF_MS);

    if matches!(error_kind, UploadErrorKind::RateLimited) {
        delay.max(RATE_LIMIT_BACKOFF_MS)
    } else {
        delay
    }
}

// =====================================================
// 分片线程槽位池
// =====================================================

/// 分片线程槽位池
///
/// 为每个正在上传的分片分配一个唯一的槽位ID（1, 2, 3...max_slots）
/// 分片完成后归还槽位，确保同一时刻每个槽位只有一个分片在使用
#[derive(Debug)]
struct ChunkSlotPool {
    /// 可用槽位栈（使用 Mutex 保护）
    available_slots: std::sync::Mutex<Vec<usize>>,
    /// 最大槽位数
    max_slots: usize,
}

impl ChunkSlotPool {
    fn new(max_slots: usize) -> Self {
        // 初始化所有槽位为可用（从大到小，pop时得到小的）
        let slots: Vec<usize> = (1..=max_slots).rev().collect();
        Self {
            available_slots: std::sync::Mutex::new(slots),
            max_slots,
        }
    }

    /// 获取一个空闲槽位，如果没有则返回备用ID
    fn acquire(&self) -> usize {
        let mut slots = self.available_slots.lock().unwrap();
        slots.pop().unwrap_or(self.max_slots + 1)
    }

    /// 归还槽位
    fn release(&self, slot_id: usize) {
        if slot_id <= self.max_slots {
            let mut slots = self.available_slots.lock().unwrap();
            if !slots.contains(&slot_id) {
                slots.push(slot_id);
            }
        }
    }
}

// =====================================================
// 上传任务调度信息
// =====================================================

/// 上传任务调度信息
#[derive(Debug, Clone)]
pub struct UploadTaskScheduleInfo {
    /// 任务 ID
    pub task_id: String,
    /// 任务引用
    pub task: Arc<Mutex<UploadTask>>,
    /// 分片管理器
    pub chunk_manager: Arc<Mutex<UploadChunkManager>>,
    /// 服务器健康管理器
    pub server_health: Arc<PcsServerHealthManager>,

    // 上传所需的配置
    /// 网盘客户端
    pub client: NetdiskClient,
    /// 本地文件路径
    pub local_path: PathBuf,
    /// 远程路径
    pub remote_path: String,
    /// 上传 ID（precreate 返回）
    pub upload_id: String,
    /// 文件总大小
    pub total_size: u64,
    /// block_list（4MB 分片 MD5 列表，用于 create_file）
    pub block_list: String,

    // 控制
    /// 取消令牌
    pub cancellation_token: CancellationToken,
    /// 是否暂停
    pub is_paused: Arc<AtomicBool>,
    /// 是否正在合并分片（防止重复调用 create_file）
    pub is_merging: Arc<AtomicBool>,

    // 统计
    /// 当前正在上传的分片数
    pub active_chunk_count: Arc<AtomicUsize>,
    /// 任务级最大并发分片数（根据文件大小自动计算）
    pub max_concurrent_chunks: usize,

    // 进度追踪
    /// 已上传字节数（原子计数器）
    pub uploaded_bytes: Arc<AtomicU64>,
    /// 上次速度计算时间
    pub last_speed_time: Arc<Mutex<std::time::Instant>>,
    /// 上次速度计算时的字节数
    pub last_speed_bytes: Arc<AtomicU64>,
}

// =====================================================
// 全局上传分片调度器
// =====================================================

/// 全局上传分片调度器
///
/// 负责公平调度所有上传任务的分片，实现：
/// 1. 限制同时上传的任务数量（max_concurrent_tasks）
/// 2. 限制全局并发上传的分片数量（动态可调整）
/// 3. 使用 Round-Robin 算法公平调度
/// 4. 为每个分片分配逻辑线程ID，便于日志追踪
#[derive(Debug, Clone)]
pub struct UploadChunkScheduler {
    /// 活跃任务列表（task_id -> TaskScheduleInfo）
    active_tasks: Arc<RwLock<HashMap<String, UploadTaskScheduleInfo>>>,
    /// 最大全局线程数（动态可调整）
    max_global_threads: Arc<AtomicUsize>,
    /// 当前活跃的分片线程数
    active_chunk_count: Arc<AtomicUsize>,
    /// 分片线程槽位池
    slot_pool: Arc<ChunkSlotPool>,
    /// 最大同时上传任务数（动态可调整）
    max_concurrent_tasks: Arc<AtomicUsize>,
    /// 最大重试次数（动态可调整）
    max_retries: Arc<AtomicUsize>,
    /// 调度器是否正在运行
    scheduler_running: Arc<AtomicBool>,
    /// 预注册计数（正在准备但还未正式注册的任务数）
    pre_register_count: Arc<AtomicUsize>,
    /// 任务完成通知发送器（用于通知文件夹上传管理器补充任务）
    task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
    /// 上一轮的任务数（用于检测任务数变化）
    last_task_count: Arc<AtomicUsize>,
}

impl UploadChunkScheduler {
    /// 创建新的调度器（使用默认重试次数）
    pub fn new(max_global_threads: usize, max_concurrent_tasks: usize) -> Self {
        Self::new_with_config(max_global_threads, max_concurrent_tasks, DEFAULT_MAX_RETRIES)
    }

    /// 创建新的调度器（完整配置）
    pub fn new_with_config(max_global_threads: usize, max_concurrent_tasks: usize, max_retries: u32) -> Self {
        info!(
            "创建全局上传分片调度器: 全局线程数={}, 最大并发任务数={}, 最大重试次数={}",
            max_global_threads, max_concurrent_tasks, max_retries
        );

        let scheduler = Self {
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            max_global_threads: Arc::new(AtomicUsize::new(max_global_threads)),
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            slot_pool: Arc::new(ChunkSlotPool::new(max_global_threads)),
            max_concurrent_tasks: Arc::new(AtomicUsize::new(max_concurrent_tasks)),
            max_retries: Arc::new(AtomicUsize::new(max_retries as usize)),
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
    pub async fn set_task_completed_sender(&self, tx: mpsc::UnboundedSender<String>) {
        let mut sender = self.task_completed_tx.write().await;
        *sender = Some(tx);
        info!("上传任务完成通知 channel 已设置");
    }

    /// 动态更新最大全局线程数
    pub fn update_max_threads(&self, new_max: usize) {
        let old_max = self.max_global_threads.swap(new_max, Ordering::SeqCst);
        info!(
            "🔧 动态调整上传全局最大线程数: {} -> {}",
            old_max, new_max
        );
    }

    /// 动态更新最大并发任务数
    pub fn update_max_concurrent_tasks(&self, new_max: usize) {
        let old_max = self.max_concurrent_tasks.swap(new_max, Ordering::SeqCst);
        info!(
            "🔧 动态调整上传最大并发任务数: {} -> {}",
            old_max, new_max
        );
    }

    /// 动态更新最大重试次数
    pub fn update_max_retries(&self, new_max: u32) {
        let old_max = self.max_retries.swap(new_max as usize, Ordering::SeqCst);
        info!(
            "🔧 动态调整上传最大重试次数: {} -> {}",
            old_max, new_max
        );
    }

    /// 获取当前最大线程数
    pub fn max_threads(&self) -> usize {
        self.max_global_threads.load(Ordering::SeqCst)
    }

    /// 获取当前最大重试次数
    pub fn max_retries(&self) -> u32 {
        self.max_retries.load(Ordering::SeqCst) as u32
    }

    /// 获取当前活跃分片线程数
    pub fn active_threads(&self) -> usize {
        self.active_chunk_count.load(Ordering::SeqCst)
    }

    /// 预注册任务（在准备上传前调用）
    ///
    /// 返回 true 表示预注册成功，可以开始准备
    /// 返回 false 表示已达并发上限，不应启动准备
    pub async fn pre_register(&self) -> bool {
        let max_tasks = self.max_concurrent_tasks.load(Ordering::SeqCst);
        let pre_register_limit = max_tasks;
        let registered_count = self.active_tasks.read().await.len();

        loop {
            let current_pre = self.pre_register_count.load(Ordering::SeqCst);
            let total = registered_count + current_pre;

            if total >= pre_register_limit {
                info!(
                    "上传预注册失败：总数已达上限 (已注册:{} + 预注册:{} = {} >= {})",
                    registered_count, current_pre, total, pre_register_limit
                );
                return false;
            }

            match self.pre_register_count.compare_exchange(
                current_pre,
                current_pre + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    info!(
                        "上传预注册成功：已注册:{} + 预注册:{} -> {} (上限: {})",
                        registered_count, current_pre, current_pre + 1, pre_register_limit
                    );
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    /// 获取预注册余量
    pub async fn pre_register_available(&self) -> usize {
        let max_tasks = self.max_concurrent_tasks.load(Ordering::SeqCst);
        let pre_register_limit = max_tasks;
        let registered_count = self.active_tasks.read().await.len();
        let current_pre = self.pre_register_count.load(Ordering::SeqCst);
        let total = registered_count + current_pre;
        pre_register_limit.saturating_sub(total)
    }

    /// 取消预注册
    pub fn cancel_pre_register(&self) {
        let old = self.pre_register_count.fetch_sub(1, Ordering::SeqCst);
        info!("取消上传预注册：预注册数 {} -> {}", old, old.saturating_sub(1));
    }

    /// 获取预注册计数
    pub fn pre_register_count(&self) -> usize {
        self.pre_register_count.load(Ordering::SeqCst)
    }

    /// 注册任务到调度器
    ///
    /// 注册成功后会自动减少预注册计数
    pub async fn register_task(&self, task_info: UploadTaskScheduleInfo) -> Result<()> {
        let task_id = task_info.task_id.clone();
        let max_tasks = self.max_concurrent_tasks.load(Ordering::SeqCst);

        // 检查是否超过最大并发任务数
        {
            let tasks = self.active_tasks.read().await;
            if tasks.len() >= max_tasks {
                anyhow::bail!(
                    "超过上传最大并发任务数限制 ({}/{})",
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
            "上传任务 {} 已注册到调度器 (预注册数: {} -> {})",
            task_id, old_pre, old_pre.saturating_sub(1)
        );
        Ok(())
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) {
        if let Some(task_info) = self.active_tasks.write().await.remove(task_id) {
            task_info.cancellation_token.cancel();
            info!("上传任务 {} 已从调度器移除并取消", task_id);
        }
    }

    /// 获取活跃任务数量（包括已注册和预注册的任务）
    pub async fn active_task_count(&self) -> usize {
        let registered = self.active_tasks.read().await.len();
        let pre_registered = self.pre_register_count.load(Ordering::SeqCst);
        registered + pre_registered
    }

    /// 启动全局调度循环
    fn start_scheduling(&self) {
        let active_tasks = self.active_tasks.clone();
        let max_global_threads = self.max_global_threads.clone();
        let active_chunk_count = self.active_chunk_count.clone();
        let slot_pool = self.slot_pool.clone();
        let scheduler_running = self.scheduler_running.clone();
        let task_completed_tx = self.task_completed_tx.clone();
        let last_task_count = self.last_task_count.clone();
        let max_retries = self.max_retries.clone();

        // 标记调度器正在运行
        scheduler_running.store(true, Ordering::SeqCst);

        info!("🚀 全局上传分片调度循环已启动");

        tokio::spawn(async move {
            let mut round_robin_counter: usize = 0;

            while scheduler_running.load(Ordering::SeqCst) {
                // 获取所有活跃任务 ID
                let task_ids: Vec<String> = {
                    let tasks = active_tasks.read().await;
                    let mut ids: Vec<String> = tasks.keys().cloned().collect();
                    ids.sort();
                    ids
                };

                let current_task_count = task_ids.len();

                // 检测任务数增加，触发速度窗口重置
                {
                    let last_count = last_task_count.load(Ordering::SeqCst);
                    if current_task_count > last_count && last_count > 0 {
                        info!(
                            "🔄 检测到上传任务数增加: {} -> {}, 重置所有服务器速度窗口",
                            last_count, current_task_count
                        );

                        let tasks = active_tasks.read().await;
                        for task_info in tasks.values() {
                            task_info.server_health.reset_speed_windows();
                        }
                    }

                    last_task_count.store(current_task_count, Ordering::SeqCst);
                }

                if task_ids.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                // 批量调度
                let max_threads = max_global_threads.load(Ordering::SeqCst);
                let current_active = active_chunk_count.load(Ordering::SeqCst);

                if current_active >= max_threads {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }

                let available_slots = max_threads.saturating_sub(current_active);

                let mut consecutive_empty_rounds = 0;
                let task_count = task_ids.len();

                for _ in 0..available_slots {
                    let task_id = &task_ids[round_robin_counter % task_count];
                    round_robin_counter = round_robin_counter.wrapping_add(1);

                    let task_info_opt = {
                        let tasks = active_tasks.read().await;
                        tasks.get(task_id).cloned()
                    };

                    let task_info = match task_info_opt {
                        Some(info) => info,
                        None => {
                            consecutive_empty_rounds += 1;
                            if consecutive_empty_rounds >= task_count {
                                break;
                            }
                            continue;
                        }
                    };

                    // 检查任务是否被取消
                    if task_info.cancellation_token.is_cancelled() {
                        info!("上传任务 {} 已被取消，从调度器移除", task_id);
                        active_tasks.write().await.remove(task_id);
                        consecutive_empty_rounds += 1;
                        if consecutive_empty_rounds >= task_count {
                            break;
                        }
                        continue;
                    }

                    // 检查任务是否暂停
                    if task_info.is_paused.load(Ordering::SeqCst) {
                        consecutive_empty_rounds += 1;
                        if consecutive_empty_rounds >= task_count {
                            break;
                        }
                        continue;
                    }

                    // 检查任务级并发限制
                    let task_active = task_info.active_chunk_count.load(Ordering::SeqCst);
                    if task_active >= task_info.max_concurrent_chunks {
                        debug!(
                            "上传任务 {} 已达并发上限 ({}/{}), 跳过",
                            task_id, task_active, task_info.max_concurrent_chunks
                        );
                        consecutive_empty_rounds += 1;
                        if consecutive_empty_rounds >= task_count {
                            break;
                        }
                        continue;
                    }

                    // 获取下一个待上传的分片
                    let next_chunk = {
                        let mut manager = task_info.chunk_manager.lock().await;
                        let chunk = manager
                            .chunks_mut()
                            .iter_mut()
                            .find(|chunk| !chunk.completed && !chunk.uploading);

                        if let Some(c) = chunk {
                            c.uploading = true;
                            Some(c.clone())
                        } else {
                            None
                        }
                    };

                    match next_chunk {
                        Some(chunk) => {
                            // 原子增加活跃计数
                            active_chunk_count.fetch_add(1, Ordering::SeqCst);
                            task_info.active_chunk_count.fetch_add(1, Ordering::SeqCst);

                            debug!(
                                "调度器选择: 上传任务 {} 分片 #{} (活跃线程: {}/{})",
                                task_id,
                                chunk.index,
                                active_chunk_count.load(Ordering::SeqCst),
                                max_threads
                            );

                            Self::spawn_chunk_upload(
                                chunk,
                                task_info.clone(),
                                active_tasks.clone(),
                                slot_pool.clone(),
                                active_chunk_count.clone(),
                                task_completed_tx.clone(),
                                max_retries.clone(),
                            );

                            consecutive_empty_rounds = 0;
                        }
                        None => {
                            // 该任务没有待上传的分片
                            if task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                                // 所有分片完成，尝试调用 create_file 合并分片
                                // 使用 compare_exchange 确保只有一处能执行合并
                                if task_info.is_merging.compare_exchange(
                                    false,
                                    true,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                ).is_ok() {
                                    info!(
                                        "上传任务 {} 所有分片完成，开始合并分片 (调度循环触发)",
                                        task_id
                                    );

                                    let create_result = task_info.client
                                        .create_file(
                                            &task_info.remote_path,
                                            &task_info.block_list,
                                            &task_info.upload_id,
                                            task_info.total_size,
                                            "0"
                                        )
                                        .await;

                                    active_tasks.write().await.remove(task_id);

                                    match create_result {
                                        Ok(response) => {
                                            if response.is_success() {
                                                info!("上传任务 {} 合并分片成功，从调度器移除", task_id);

                                                // 标记任务完成
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
                                                            error!("发送上传任务完成通知失败: {}", e);
                                                        } else {
                                                            debug!("已发送上传任务完成通知: group_id={}", gid);
                                                        }
                                                    }
                                                }
                                            } else {
                                                let err_msg = format!(
                                                    "合并分片失败: errno={}, errmsg={}",
                                                    response.errno, response.errmsg
                                                );
                                                error!("上传任务 {} {}", task_id, err_msg);

                                                let mut t = task_info.task.lock().await;
                                                t.mark_failed(err_msg);
                                            }
                                        }
                                        Err(e) => {
                                            let err_msg = format!("调用 create_file 失败: {}", e);
                                            error!("上传任务 {} {}", task_id, err_msg);

                                            let mut t = task_info.task.lock().await;
                                            t.mark_failed(err_msg);
                                        }
                                    }
                                } else {
                                    debug!(
                                        "上传任务 {} 合并分片已由其他位置触发，跳过",
                                        task_id
                                    );
                                }
                            }

                            consecutive_empty_rounds += 1;
                            if consecutive_empty_rounds >= task_count {
                                break;
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(2)).await;
            }

            info!("全局上传分片调度循环已停止");
        });
    }

    /// 启动单个分片的上传任务
    fn spawn_chunk_upload(
        chunk: UploadChunk,
        task_info: UploadTaskScheduleInfo,
        active_tasks: Arc<RwLock<HashMap<String, UploadTaskScheduleInfo>>>,
        slot_pool: Arc<ChunkSlotPool>,
        global_active_count: Arc<AtomicUsize>,
        task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
        max_retries: Arc<AtomicUsize>,
    ) {
        tokio::spawn(async move {
            let task_id = task_info.task_id.clone();
            let chunk_index = chunk.index;

            // 获取槽位ID
            let slot_id = slot_pool.acquire();

            info!(
                "[上传线程{}] 分片 #{} 获得线程资源，开始上传",
                slot_id, chunk_index
            );

            // 执行分片上传
            let result = Self::upload_chunk_with_retry(
                chunk,
                &task_info,
                slot_id,
                max_retries.load(Ordering::SeqCst) as u32,
            )
            .await;

            // 释放全局活跃计数
            global_active_count.fetch_sub(1, Ordering::SeqCst);
            task_info.active_chunk_count.fetch_sub(1, Ordering::SeqCst);

            // 归还槽位
            slot_pool.release(slot_id);

            info!("[上传线程{}] 分片 #{} 释放线程资源", slot_id, chunk_index);

            // 处理上传结果
            if let Err(e) = result {
                if task_info.cancellation_token.is_cancelled() {
                    info!("[上传线程{}] 分片 #{} 因任务取消而失败", slot_id, chunk_index);
                } else {
                    error!("[上传线程{}] 分片 #{} 上传失败: {}", slot_id, chunk_index, e);

                    // 取消上传标记
                    {
                        let mut manager = task_info.chunk_manager.lock().await;
                        if let Some(c) = manager.chunks_mut().get_mut(chunk_index) {
                            c.uploading = false;
                        }
                    }

                    // 标记任务失败
                    {
                        let mut t = task_info.task.lock().await;
                        t.mark_failed(e.to_string());
                    }

                    // 从调度器移除任务
                    active_tasks.write().await.remove(&task_id);
                    error!("上传任务 {} 因分片上传失败已从调度器移除", task_id);
                }
            } else {
                // 检查是否所有分片都完成
                let all_completed = {
                    let manager = task_info.chunk_manager.lock().await;
                    manager.is_completed()
                };

                if all_completed && task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                    // 使用 compare_exchange 确保只有一处能执行合并
                    if task_info.is_merging.compare_exchange(
                        false,
                        true,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ).is_ok() {
                        info!(
                            "上传任务 {} 所有分片上传完成，开始合并分片 (回调触发)",
                            task_id
                        );

                        // 调用 create_file 合并分片
                        let create_result = task_info.client
                            .create_file(
                                &task_info.remote_path,
                                &task_info.block_list,
                                &task_info.upload_id,
                                task_info.total_size,
                                "0"
                            )
                            .await;

                        // 从调度器移除
                        active_tasks.write().await.remove(&task_id);

                        match create_result {
                            Ok(response) => {
                                if response.is_success() {
                                    info!("上传任务 {} 合并分片成功，文件创建完成", task_id);

                                    // 标记完成并通知
                                    let group_id = {
                                        let mut t = task_info.task.lock().await;
                                        t.mark_completed();
                                        t.group_id.clone()
                                    };

                                    if let Some(gid) = group_id {
                                        let tx_guard = task_completed_tx.read().await;
                                        if let Some(tx) = tx_guard.as_ref() {
                                            let _ = tx.send(gid);
                                        }
                                    }
                                } else {
                                    let err_msg = format!(
                                        "合并分片失败: errno={}, errmsg={}",
                                        response.errno, response.errmsg
                                    );
                                    error!("上传任务 {} {}", task_id, err_msg);

                                    let mut t = task_info.task.lock().await;
                                    t.mark_failed(err_msg);
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("调用 create_file 失败: {}", e);
                                error!("上传任务 {} {}", task_id, err_msg);

                                let mut t = task_info.task.lock().await;
                                t.mark_failed(err_msg);
                            }
                        }
                    } else {
                        debug!(
                            "上传任务 {} 合并分片已由其他位置触发，跳过 (回调)",
                            task_id
                        );
                    }
                }
            }
        });
    }

    /// 带重试的分片上传
    async fn upload_chunk_with_retry(
        chunk: UploadChunk,
        task_info: &UploadTaskScheduleInfo,
        slot_id: usize,
        max_retries: u32,
    ) -> Result<String> {
        let chunk_size = chunk.range.end - chunk.range.start;

        debug!(
            "[上传线程{}] 分片 #{} 开始上传 (范围: {}-{}, 大小: {} bytes)",
            slot_id, chunk.index, chunk.range.start, chunk.range.end - 1, chunk_size
        );

        // 读取分片数据
        let chunk_data = read_chunk_data(&task_info.local_path, &chunk).await?;

        let mut last_error = None;

        for retry in 0..=max_retries {
            // 检查取消
            if task_info.cancellation_token.is_cancelled() {
                return Err(anyhow::anyhow!("上传已取消"));
            }

            // 选择服务器
            let server = task_info
                .server_health
                .get_server_hybrid(chunk.index)
                .unwrap_or_else(|| "d.pcs.baidu.com".to_string());

            // 上传分片
            let start_time = std::time::Instant::now();
            match task_info
                .client
                .upload_chunk(
                    &task_info.remote_path,
                    &task_info.upload_id,
                    chunk.index,
                    chunk_data.clone(),
                    Some(&server),
                )
                .await
            {
                Ok(response) => {
                    // 记录速度
                    let elapsed_ms = start_time.elapsed().as_millis() as u64;
                    if elapsed_ms > 0 {
                        task_info.server_health.record_chunk_speed(&server, chunk_size, elapsed_ms);
                    }

                    // 更新已上传字节数
                    let new_uploaded = task_info
                        .uploaded_bytes
                        .fetch_add(chunk_size, Ordering::SeqCst)
                        + chunk_size;

                    // 标记分片完成
                    let (completed_chunks, total_chunks) = {
                        let mut cm = task_info.chunk_manager.lock().await;
                        cm.mark_completed(chunk.index, Some(response.md5.clone()));
                        (cm.completed_count(), cm.chunk_count())
                    };

                    // 计算速度
                    let speed = {
                        let mut last_time = task_info.last_speed_time.lock().await;
                        let elapsed = last_time.elapsed();
                        let elapsed_secs = elapsed.as_secs_f64();

                        if elapsed_secs >= 0.5 {
                            let last_bytes = task_info
                                .last_speed_bytes
                                .swap(new_uploaded, Ordering::SeqCst);
                            let bytes_diff = new_uploaded.saturating_sub(last_bytes);
                            *last_time = std::time::Instant::now();

                            if elapsed_secs > 0.0 {
                                (bytes_diff as f64 / elapsed_secs) as u64
                            } else {
                                0
                            }
                        } else {
                            0
                        }
                    };

                    // 更新任务状态
                    {
                        let mut t = task_info.task.lock().await;
                        t.uploaded_size = new_uploaded;
                        t.completed_chunks = completed_chunks;
                        t.total_chunks = total_chunks;
                        if speed > 0 {
                            t.speed = speed;
                        }
                    }

                    info!(
                        "[上传线程{}] ✓ 分片 #{} 上传成功 ({}/{} 完成, 速度: {} KB/s)",
                        slot_id, chunk.index, completed_chunks, total_chunks, speed / 1024
                    );

                    return Ok(response.md5);
                }
                Err(e) => {
                    let error_kind = classify_upload_error(&e);

                    if !error_kind.is_retriable() {
                        error!(
                            "[上传线程{}] 分片 #{} 上传失败（不可重试）: {:?}, 错误: {}",
                            slot_id, chunk.index, error_kind, e
                        );
                        return Err(e);
                    }

                    if retry < max_retries {
                        let backoff_ms = calculate_backoff_delay(retry, &error_kind);
                        warn!(
                            "[上传线程{}] 分片 #{} 上传失败，等待 {}ms 后重试 ({}/{}): {}",
                            slot_id, chunk.index, backoff_ms, retry + 1, max_retries, e
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }

                    last_error = Some(e);
                }
            }
        }

        // 达到最大重试次数
        {
            let mut cm = task_info.chunk_manager.lock().await;
            cm.increment_retry(chunk.index);
        }

        error!(
            "[上传线程{}] 分片 #{} 上传失败，已达最大重试次数 ({})",
            slot_id, chunk.index, max_retries
        );

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("上传失败")))
    }

    /// 停止调度器
    pub fn stop(&self) {
        self.scheduler_running.store(false, Ordering::SeqCst);
        info!("上传调度器停止信号已发送");
    }
}

// =====================================================
// 辅助函数
// =====================================================

/// 读取分片数据
async fn read_chunk_data(local_path: &std::path::Path, chunk: &UploadChunk) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let local_path = local_path.to_path_buf();
    let start = chunk.range.start;
    let size = (chunk.range.end - chunk.range.start) as usize;

    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(&local_path)
            .map_err(|e| anyhow::anyhow!("无法打开文件 {:?}: {}", local_path, e))?;
        file.seek(SeekFrom::Start(start))?;

        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    })
    .await?
}

/// 错误分类
fn classify_upload_error(error: &anyhow::Error) -> UploadErrorKind {
    let error_str = error.to_string().to_lowercase();

    if error_str.contains("timeout") || error_str.contains("timed out") {
        UploadErrorKind::Timeout
    } else if error_str.contains("connection")
        || error_str.contains("network")
        || error_str.contains("dns")
    {
        UploadErrorKind::Network
    } else if error_str.contains("429") || error_str.contains("rate limit") {
        UploadErrorKind::RateLimited
    } else if error_str.contains("404") || error_str.contains("not found") {
        UploadErrorKind::FileNotFound
    } else if error_str.contains("403") || error_str.contains("forbidden") {
        UploadErrorKind::Forbidden
    } else if error_str.contains("400") || error_str.contains("bad request") {
        UploadErrorKind::BadRequest
    } else if error_str.contains("500") || error_str.contains("internal server") {
        UploadErrorKind::ServerError
    } else {
        UploadErrorKind::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_pool() {
        let pool = ChunkSlotPool::new(3);

        // 获取槽位
        let s1 = pool.acquire();
        let s2 = pool.acquire();
        let s3 = pool.acquire();

        assert!(s1 >= 1 && s1 <= 3);
        assert!(s2 >= 1 && s2 <= 3);
        assert!(s3 >= 1 && s3 <= 3);
        assert_ne!(s1, s2);
        assert_ne!(s2, s3);
        assert_ne!(s1, s3);

        // 超出范围
        let s4 = pool.acquire();
        assert_eq!(s4, 4);

        // 归还槽位
        pool.release(s1);
        let s5 = pool.acquire();
        assert_eq!(s5, s1);
    }

    #[test]
    fn test_calculate_backoff_delay() {
        // 普通错误
        assert_eq!(calculate_backoff_delay(0, &UploadErrorKind::Network), 100);
        assert_eq!(calculate_backoff_delay(1, &UploadErrorKind::Network), 200);
        assert_eq!(calculate_backoff_delay(2, &UploadErrorKind::Network), 400);
        assert_eq!(calculate_backoff_delay(10, &UploadErrorKind::Network), 5000);

        // 限流错误
        assert_eq!(calculate_backoff_delay(0, &UploadErrorKind::RateLimited), 10000);
    }

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = UploadChunkScheduler::new(10, 3);

        assert_eq!(scheduler.max_threads(), 10);
        assert_eq!(scheduler.active_threads(), 0);
        assert_eq!(scheduler.active_task_count().await, 0);
    }

    #[tokio::test]
    async fn test_pre_register() {
        let scheduler = UploadChunkScheduler::new(10, 2);

        // 预注册成功
        assert!(scheduler.pre_register().await);
        assert_eq!(scheduler.pre_register_count(), 1);

        assert!(scheduler.pre_register().await);
        assert_eq!(scheduler.pre_register_count(), 2);

        // 达到上限，预注册失败
        assert!(!scheduler.pre_register().await);
        assert_eq!(scheduler.pre_register_count(), 2);

        // 取消预注册
        scheduler.cancel_pre_register();
        assert_eq!(scheduler.pre_register_count(), 1);

        // 可以再次预注册
        assert!(scheduler.pre_register().await);
    }
}
