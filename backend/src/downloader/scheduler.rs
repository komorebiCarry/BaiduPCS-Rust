use crate::encryption::service::EncryptionService;
use crate::autobackup::events::{BackupTransferNotification, TransferTaskType};
use crate::downloader::{
    ChunkManager, DownloadEngine, DownloadTask, SpeedCalculator, UrlHealthManager,
};
use crate::persistence::PersistenceManager;
use crate::server::events::{DownloadEvent, ProgressThrottler, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 🔥 根据文件大小计算单任务最大并发分片数
///
/// 小文件少线程，大文件多线程，资源利用提升 +50-80%
///
/// # 参数
/// * `file_size` - 文件大小（字节）
///
/// # 返回
/// 最大并发分片数
pub fn calculate_task_max_chunks(file_size: u64) -> usize {
    match file_size {
        0..=10_000_000 => 1,                 // <10MB: 单线程最好
        10_000_001..=100_000_000 => 3,       // 10MB ~ 100MB: 稍微并发
        100_000_001..=1_000_000_000 => 6,    // 100MB ~ 1GB: 并发6个
        1_000_000_001..=5_000_000_000 => 10, // 1GB ~ 5GB: 10线程
        _ => 15,                             // >5GB: 15线程
    }
}

/// 分片线程槽位池
///
/// 为每个正在下载的分片分配一个唯一的槽位ID（1, 2, 3...max_slots）
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
        slots.pop().unwrap_or(self.max_slots + 1) // 如果没有空闲槽位，返回超出范围的ID
    }

    /// 归还槽位
    fn release(&self, slot_id: usize) {
        if slot_id <= self.max_slots {
            let mut slots = self.available_slots.lock().unwrap();
            // 避免重复归还
            if !slots.contains(&slot_id) {
                slots.push(slot_id);
            }
        }
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

    // 🔥 任务级并发控制
    /// 单任务最大并发分片数（根据文件大小自动计算）
    pub max_concurrent_chunks: usize,

    // 🔥 持久化支持
    /// 持久化管理器引用（可选）
    pub persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,

    // 🔥 WebSocket 管理器支持
    /// WebSocket 管理器引用
    pub ws_manager: Option<Arc<WebSocketManager>>,

    // 🔥 进度事件节流器（200ms 间隔，避免事件风暴）
    /// 任务级进度节流器，多个分片共享
    pub progress_throttler: Arc<ProgressThrottler>,

    // 🔥 文件夹进度通知发送器（由子任务进度变化触发）
    /// 可选，仅文件夹子任务需要
    pub folder_progress_tx: Option<mpsc::UnboundedSender<String>>,

    // 🔥 备份任务统一通知发送器（进度、状态、完成、失败等）
    /// 可选，仅备份任务需要
    pub backup_notification_tx: Option<mpsc::UnboundedSender<BackupTransferNotification>>,

    // 🔥 任务位借调机制相关字段
    /// 占用的槽位ID（可选）
    pub slot_id: Option<usize>,
    /// 是否使用借调位（而非固定位）
    pub is_borrowed_slot: bool,
    /// 任务位池引用（用于释放槽位）
    pub task_slot_pool: Option<Arc<crate::task_slot_pool::TaskSlotPool>>,

    // 🔥 加密服务（用于下载完成后解密加密文件）
    /// 加密服务引用（可选，仅当需要解密时使用）
    pub encryption_service: Option<Arc<EncryptionService>>,

    // 🔥 加密快照管理器（用于查询加密文件映射，获取原始文件名）
    /// 快照管理器引用（可选，用于解密后重命名）
    pub snapshot_manager: Option<Arc<crate::encryption::snapshot::SnapshotManager>>,

    // 🔥 加密配置存储（用于根据 key_version 选择正确的解密密钥）
    /// 加密配置存储引用（可选，用于密钥轮换后解密旧文件）
    pub encryption_config_store: Option<Arc<crate::encryption::EncryptionConfigStore>>,

    // 🔥 Manager 任务列表引用（用于任务完成时立即清理，避免内存泄漏）
    /// DownloadManager.tasks 的引用，任务完成后从中移除
    pub manager_tasks: Option<Arc<RwLock<std::collections::HashMap<String, Arc<Mutex<crate::downloader::DownloadTask>>>>>>,
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
    /// 分片线程槽位池
    slot_pool: Arc<ChunkSlotPool>,
    /// 最大同时下载任务数（动态可调整）
    max_concurrent_tasks: Arc<AtomicUsize>,
    /// 调度器是否正在运行
    scheduler_running: Arc<AtomicBool>,
    /// 任务完成通知发送器（用于通知 FolderDownloadManager 补充任务）
    task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
    /// 🔥 备份任务统一通知发送器（用于通知 AutoBackupManager 所有事件）
    /// 包括：进度更新、状态变更、任务完成、任务失败等
    backup_notification_tx: Arc<RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>>,
    /// 🔥 等待队列触发器（任务完成时通知 DownloadManager 启动等待任务）
    waiting_queue_trigger: Arc<RwLock<Option<mpsc::UnboundedSender<()>>>>,
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
            slot_pool: Arc::new(ChunkSlotPool::new(max_global_threads)),
            max_concurrent_tasks: Arc::new(AtomicUsize::new(max_concurrent_tasks)),
            scheduler_running: Arc::new(AtomicBool::new(false)),
            task_completed_tx: Arc::new(RwLock::new(None)),
            backup_notification_tx: Arc::new(RwLock::new(None)),
            waiting_queue_trigger: Arc::new(RwLock::new(None)),
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

    /// 🔥 设置备份任务统一通知发送器
    ///
    /// AutoBackupManager 调用此方法设置 channel sender，
    /// 所有备份相关事件（进度、状态、完成、失败等）都通过此 channel 发送
    pub async fn set_backup_notification_sender(&self, tx: mpsc::UnboundedSender<BackupTransferNotification>) {
        let mut sender = self.backup_notification_tx.write().await;
        *sender = Some(tx);
        info!("备份下载任务统一通知 channel 已设置");
    }

    /// 🔥 设置等待队列触发器
    ///
    /// DownloadManager 调用此方法设置 channel sender，
    /// 当任务完成时会发送信号通知立即启动等待队列中的任务（0延迟）
    pub async fn set_waiting_queue_trigger(&self, tx: mpsc::UnboundedSender<()>) {
        let mut trigger = self.waiting_queue_trigger.write().await;
        *trigger = Some(tx);
        info!("等待队列触发器已设置（0延迟启动）");
    }

    /// 动态更新最大全局线程数
    ///
    /// 该方法可以在运行时调整线程池大小，无需重启下载管理器
    pub fn update_max_threads(&self, new_max: usize) {
        let old_max = self.max_global_threads.swap(new_max, Ordering::SeqCst);
        info!("🔧 动态调整全局最大线程数: {} -> {}", old_max, new_max);
    }

    /// 动态更新最大并发任务数
    pub fn update_max_concurrent_tasks(&self, new_max: usize) {
        let old_max = self.max_concurrent_tasks.swap(new_max, Ordering::SeqCst);
        info!("🔧 动态调整最大并发任务数: {} -> {}", old_max, new_max);
    }

    /// 获取当前最大线程数
    pub fn max_threads(&self) -> usize {
        self.max_global_threads.load(Ordering::SeqCst)
    }

    /// 获取当前活跃分片线程数
    pub fn active_threads(&self) -> usize {
        self.active_chunk_count.load(Ordering::SeqCst)
    }

    /// 注册任务到调度器
    ///
    /// 将任务添加到活跃任务列表，不再限制并发数（由任务槽控制）
    pub async fn register_task(&self, mut task_info: TaskScheduleInfo) -> Result<()> {
        let task_id = task_info.task_id.clone();

        // 🔥 如果是备份任务，注入调度器的 backup_notification_tx
        {
            let t = task_info.task.lock().await;
            if t.is_backup {
                let notification_tx = self.backup_notification_tx.read().await.clone();
                if notification_tx.is_some() {
                    task_info.backup_notification_tx = notification_tx;
                    info!("备份下载任务 {} 已注入统一通知 sender", task_id);
                }
            }
        }

        // 添加到活跃任务列表（不再检查并发上限，由任务槽控制）
        self.active_tasks
            .write()
            .await
            .insert(task_id.clone(), task_info);

        let active_count = self.active_tasks.read().await.len();
        info!(
            "任务 {} 已注册到调度器 (当前活跃任务数: {})",
            task_id,
            active_count
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

    /// 获取活跃任务数量（已注册的任务数）
    pub async fn active_task_count(&self) -> usize {
        self.active_tasks.read().await.len()
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
        let slot_pool = self.slot_pool.clone();
        let scheduler_running = self.scheduler_running.clone();
        let task_completed_tx = self.task_completed_tx.clone();
        let backup_notification_tx = self.backup_notification_tx.clone();
        let waiting_queue_trigger = self.waiting_queue_trigger.clone();
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
                            let health = task_info.url_health.lock().await;
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

                    // 🔥 检查任务级并发限制
                    let task_active = task_info.active_chunk_count.load(Ordering::SeqCst);
                    if task_active >= task_info.max_concurrent_chunks {
                        debug!(
                            "任务 {} 已达并发上限 ({}/{}), 跳过",
                            task_id, task_active, task_info.max_concurrent_chunks
                        );
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
                                task_id,
                                chunk_index,
                                new_active,
                                max_threads,
                                scheduled_count + 1
                            );

                            Self::spawn_chunk_download(
                                chunk_index,
                                task_info.clone(),
                                active_tasks.clone(),
                                slot_pool.clone(),
                                active_chunk_count.clone(),
                                backup_notification_tx.clone(),
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

                                // 🔥 修复：取消 cancellation_token，停止速度异常检测和线程停滞检测循环
                                task_info.cancellation_token.cancel();
                                debug!("任务 {} 的 cancellation_token 已取消", task_id);

                                // 🔥 检测是否为加密文件，如果是则执行解密
                                let decrypt_result = Self::try_decrypt_if_encrypted(&task_info).await;

                                // 🔥 修复：根据解密结果决定任务状态
                                // 如果解密失败，标记为 Failed 而不是 Completed
                                let (group_id, is_backup, decrypt_error) = {
                                    let mut t = task_info.task.lock().await;

                                    if let Err(ref e) = decrypt_result {
                                        // 解密失败，标记为失败状态
                                        let error_msg = format!("解密失败: {}", e);
                                        t.mark_failed(error_msg.clone());
                                        error!("任务 {} 解密失败: {}", task_id, e);
                                        (t.group_id.clone(), t.is_backup, Some(error_msg))
                                    } else {
                                        // 解密成功或无需解密，标记为完成
                                        t.mark_completed();
                                        (t.group_id.clone(), t.is_backup, None)
                                    }
                                };

                                // 🔥 发布任务事件（根据解密结果发送 Completed 或 Failed）
                                if !is_backup {
                                    if let Some(ref ws_manager) = task_info.ws_manager {
                                        if let Some(ref error_msg) = decrypt_error {
                                            // 解密失败，发送 Failed 事件
                                            ws_manager.send_if_subscribed(
                                                TaskEvent::Download(DownloadEvent::Failed {
                                                    task_id: task_id.to_string(),
                                                    error: error_msg.clone(),
                                                    group_id: group_id.clone(),
                                                    is_backup,
                                                }),
                                                group_id.clone(),
                                            );
                                        } else {
                                            // 解密成功或无需解密，发送 Completed 事件
                                            ws_manager.send_if_subscribed(
                                                TaskEvent::Download(DownloadEvent::Completed {
                                                    task_id: task_id.to_string(),
                                                    completed_at: chrono::Utc::now().timestamp_millis(),
                                                    group_id: group_id.clone(),
                                                    is_backup,
                                                }),
                                                group_id.clone(),
                                            );
                                        }
                                    }
                                }

                                // 🔥 根据解密结果区分成功和失败的清理逻辑（与上传任务行为保持一致）
                                if decrypt_error.is_none() {
                                    // 成功时（解密成功或无需解密）：归档到历史数据库，从 manager_tasks 移除
                                    if let Some(ref pm) = task_info.persistence_manager {
                                        if let Err(e) = pm.lock().await.on_task_completed(task_id) {
                                            error!("归档下载任务到历史数据库失败: {}", e);
                                        } else {
                                            debug!("下载任务 {} 已归档到历史数据库", task_id);
                                        }
                                    }

                                    // 🔥 从 DownloadManager.tasks 中移除任务（成功时立即清理，避免内存泄漏）
                                    if let Some(ref manager_tasks) = task_info.manager_tasks {
                                        manager_tasks.write().await.remove(task_id);
                                        debug!("下载任务 {} 已从 DownloadManager.tasks 中移除", task_id);
                                    }
                                } else {
                                    // 失败时（解密失败）：更新 WAL 元数据，不从 manager_tasks 移除（让前端能看到）
                                    if let Some(ref pm) = task_info.persistence_manager {
                                        if let Err(e) = pm.lock().await.update_task_error(
                                            task_id,
                                            decrypt_error.clone().unwrap_or_default()
                                        ) {
                                            warn!("更新下载任务错误信息失败: {}", e);
                                        } else {
                                            debug!("下载任务 {} 错误信息已更新到 WAL", task_id);
                                        }
                                    }
                                    // 🔥 失败时不从 manager_tasks 移除，让前端能看到失败任务
                                    debug!("下载任务 {} 失败，保留在 manager_tasks 中供前端查看", task_id);
                                }

                                // 🔥 释放任务槽位（单文件任务释放固定位，借调位由 FolderManager 管理）
                                if let Some(slot_id) = task_info.slot_id {
                                    if !task_info.is_borrowed_slot {
                                        // 单文件任务：释放固定位
                                        if let Some(ref slot_pool) = task_info.task_slot_pool {
                                            slot_pool.release_fixed_slot(task_id).await;
                                            info!("任务 {} 完成，释放固定槽位 {}", task_id, slot_id);
                                        }
                                    }
                                    // 借调位由 FolderManager 管理，这里不释放
                                }

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

                                // 🔥 如果是备份任务，通知 AutoBackupManager（根据解密结果发送 Completed 或 Failed）
                                if is_backup {
                                    let tx_guard = backup_notification_tx.read().await;
                                    if let Some(tx) = tx_guard.as_ref() {
                                        let notification = if let Some(ref error_msg) = decrypt_error {
                                            // 解密失败，发送 Failed 通知
                                            BackupTransferNotification::Failed {
                                                task_id: task_id.to_string(),
                                                task_type: TransferTaskType::Download,
                                                error_message: error_msg.clone(),
                                            }
                                        } else {
                                            // 解密成功或无需解密，发送 Completed 通知
                                            BackupTransferNotification::Completed {
                                                task_id: task_id.to_string(),
                                                task_type: TransferTaskType::Download,
                                            }
                                        };
                                        if let Err(e) = tx.send(notification) {
                                            error!("发送备份下载任务通知失败: {}", e);
                                        } else {
                                            debug!("已发送备份下载任务通知: task_id={}, 状态={}",
                                                task_id,
                                                if decrypt_error.is_some() { "失败" } else { "完成" });
                                        }
                                    }
                                }

                                // 🔥 触发等待队列检查（0延迟启动新任务）
                                {
                                    let trigger_guard = waiting_queue_trigger.read().await;
                                    if let Some(trigger) = trigger_guard.as_ref() {
                                        let _ = trigger.send(()); // 忽略发送失败（receiver 可能已关闭）
                                        debug!("已触发等待队列检查");
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
    /// * `slot_pool` - 线程槽位池
    /// * `global_active_count` - 全局活跃分片计数器
    /// * `backup_notification_tx` - 备份任务统一通知发送器
    fn spawn_chunk_download(
        chunk_index: usize,
        task_info: TaskScheduleInfo,
        active_tasks: Arc<RwLock<HashMap<String, TaskScheduleInfo>>>,
        slot_pool: Arc<ChunkSlotPool>,
        global_active_count: Arc<AtomicUsize>,
        backup_notification_tx: Arc<RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>>,
    ) {
        tokio::spawn(async move {
            let task_id = task_info.task_id.clone();

            // 从槽位池获取一个槽位ID
            let slot_id = slot_pool.acquire();

            info!(
                "[分片线程{}] 分片 #{} 获得线程资源，开始下载",
                slot_id, chunk_index
            );

            // 调用 DownloadEngine 的下载方法（传入事件总线和节流器）
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
                slot_id, // 传递槽位ID
                task_info.ws_manager.clone(),
                Some(task_info.progress_throttler.clone()),
                task_id.clone(),
                task_info.folder_progress_tx.clone(), // 🔥 文件夹进度通知发送器
                task_info.backup_notification_tx.clone(), // 🔥 备份任务统一通知发送器
            )
                .await;

            // 释放全局活跃分片计数
            global_active_count.fetch_sub(1, Ordering::SeqCst);

            // 减少任务内活跃分片计数
            task_info.active_chunk_count.fetch_sub(1, Ordering::SeqCst);

            // 归还槽位到池中
            slot_pool.release(slot_id);

            info!("[分片线程{}] 分片 #{} 释放线程资源", slot_id, chunk_index);

            // 处理下载结果
            match result {
                Ok(()) => {
                    // 🔥 分片下载成功，调用持久化回调
                    if let Some(ref pm) = task_info.persistence_manager {
                        pm.lock().await.on_chunk_completed(&task_id, chunk_index);
                        debug!(
                            "[分片线程{}] 分片 #{} 已记录到持久化管理器",
                            slot_id, chunk_index
                        );
                    }

                    // 注意：进度事件已在流式回调中通过节流器发布，此处不再重复发布
                }
                Err(e) => {
                    // 检查是否是因为取消而失败
                    if task_info.cancellation_token.is_cancelled() {
                        info!(
                            "[分片线程{}] 分片 #{} 因任务取消而失败",
                            slot_id, chunk_index
                        );
                    } else {
                        error!(
                            "[分片线程{}] 分片 #{} 下载失败: {}",
                            slot_id, chunk_index, e
                        );

                        // 取消下载标记（允许重新调度）
                        {
                            let mut manager = task_info.chunk_manager.lock().await;
                            manager.unmark_downloading(chunk_index);
                        }

                        // 标记任务失败，并获取 group_id 和 is_backup
                        let (error_msg, group_id, is_backup) = {
                            let mut t = task_info.task.lock().await;
                            let err = e.to_string();
                            t.mark_failed(err.clone());
                            (err, t.group_id.clone(), t.is_backup)
                        };

                        // 🔥 发布任务失败事件
                        if !is_backup {
                            if let Some(ref ws_manager) = task_info.ws_manager {
                                ws_manager.send_if_subscribed(
                                    TaskEvent::Download(DownloadEvent::Failed {
                                        task_id: task_id.clone(),
                                        error: error_msg.clone(),
                                        group_id: group_id.clone(),
                                        is_backup,
                                    }),
                                    group_id,
                                );
                            }
                        }

                        // 🔥 如果是备份任务，通知 AutoBackupManager
                        if is_backup {
                            let tx_guard = backup_notification_tx.read().await;
                            if let Some(tx) = tx_guard.as_ref() {
                                let notification = BackupTransferNotification::Failed {
                                    task_id: task_id.clone(),
                                    task_type: TransferTaskType::Download,
                                    error_message: error_msg.clone(),
                                };
                                let _ = tx.send(notification);
                            }
                        }

                        // 🔥 更新持久化错误信息
                        if let Some(ref pm) = task_info.persistence_manager {
                            if let Err(e) = pm.lock().await.update_task_error(&task_id, error_msg) {
                                warn!("更新下载任务错误信息失败: {}", e);
                            }
                        }

                        // 从调度器移除任务
                        active_tasks.write().await.remove(&task_id);
                        error!("任务 {} 因分片下载失败已从调度器移除", task_id);
                    }
                }
            }
        });
    }

    /// 停止调度器
    pub fn stop(&self) {
        self.scheduler_running.store(false, Ordering::SeqCst);
        info!("调度器停止信号已发送");
    }

    /// 🔥 检测并解密加密文件
    ///
    /// 下载完成后自动检测文件是否为加密文件，如果是则执行解密流程
    ///
    /// # 参数
    /// * `task_info` - 任务调度信息
    ///
    /// # 返回
    /// - Ok(()) - 不是加密文件或解密成功
    /// - Err(e) - 解密失败
    async fn try_decrypt_if_encrypted(task_info: &TaskScheduleInfo) -> Result<()> {
        // 1. 检测是否为加密文件
        let (local_path, task_id, group_id, is_backup) = {
            let task = task_info.task.lock().await;
            (
                task.local_path.clone(),
                task.id.clone(),
                task.group_id.clone(),
                task.is_backup,
            )
        };

        // 检查文件名是否为加密文件格式
        let filename = local_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string(); // 转换为 String 避免借用问题

        let is_encrypted_by_name = crate::downloader::task::DownloadTask::detect_encrypted_filename(&filename);

        // 检查文件头魔数
        let is_encrypted_by_content = if local_path.exists() {
            EncryptionService::is_encrypted_file(&local_path).unwrap_or(false)
        } else {
            false
        };

        let is_encrypted = is_encrypted_by_name || is_encrypted_by_content;

        if !is_encrypted {
            debug!("任务 {} 不是加密文件，跳过解密", task_id);
            return Ok(());
        }

        // 2. 🔥 根据 key_version 选择正确的解密密钥
        // 优先从 snapshot_manager 查询 key_version，然后从 encryption_config_store 获取对应密钥
        let encryption_service = {
            // 尝试从 snapshot_manager 获取 key_version
            let key_version = if let Some(ref snapshot_mgr) = task_info.snapshot_manager {
                match snapshot_mgr.find_by_encrypted_name(&filename) {
                    Ok(Some(snapshot_info)) => {
                        info!(
                            "任务 {} 从映射表获取 key_version: {}",
                            task_id, snapshot_info.key_version
                        );
                        Some(snapshot_info.key_version)
                    }
                    Ok(None) => {
                        debug!("任务 {} 在映射表中未找到加密信息，使用默认密钥", task_id);
                        None
                    }
                    Err(e) => {
                        warn!("任务 {} 查询映射表失败: {}，使用默认密钥", task_id, e);
                        None
                    }
                }
            } else {
                None
            };

            // 如果有 key_version 且有 encryption_config_store，尝试获取对应版本的密钥
            if let (Some(version), Some(ref config_store)) = (key_version, &task_info.encryption_config_store) {
                match config_store.get_key_by_version(version) {
                    Ok(Some(key_info)) => {
                        info!(
                            "任务 {} 使用 key_version={} 的密钥进行解密",
                            task_id, version
                        );
                        match EncryptionService::from_base64_key(&key_info.master_key, key_info.algorithm) {
                            Ok(service) => Arc::new(service),
                            Err(e) => {
                                warn!(
                                    "任务 {} 创建 key_version={} 的加密服务失败: {}，回退到默认密钥",
                                    task_id, version, e
                                );
                                // 回退到默认的 encryption_service
                                match &task_info.encryption_service {
                                    Some(service) => service.clone(),
                                    None => {
                                        warn!("任务 {} 是加密文件但没有配置加密服务，跳过解密", task_id);
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        warn!(
                            "任务 {} 未找到 key_version={} 的密钥，回退到默认密钥",
                            task_id, version
                        );
                        // 回退到默认的 encryption_service
                        match &task_info.encryption_service {
                            Some(service) => service.clone(),
                            None => {
                                warn!("任务 {} 是加密文件但没有配置加密服务，跳过解密", task_id);
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "任务 {} 获取 key_version={} 的密钥失败: {}，回退到默认密钥",
                            task_id, version, e
                        );
                        // 回退到默认的 encryption_service
                        match &task_info.encryption_service {
                            Some(service) => service.clone(),
                            None => {
                                warn!("任务 {} 是加密文件但没有配置加密服务，跳过解密", task_id);
                                return Ok(());
                            }
                        }
                    }
                }
            } else {
                // 没有 key_version 或没有 config_store，使用默认的 encryption_service
                match &task_info.encryption_service {
                    Some(service) => service.clone(),
                    None => {
                        warn!("任务 {} 是加密文件但没有配置加密服务，跳过解密", task_id);
                        return Ok(());
                    }
                }
            }
        };

        info!("🔐 任务 {} 检测到加密文件，开始解密...", task_id);

        // 3. 更新任务状态为解密中
        {
            let mut task = task_info.task.lock().await;
            task.is_encrypted = true;
            task.mark_decrypting();
        }

        // 4. 发送状态变更事件
        if is_backup {
            // 备份任务：发送到 backup_notification_tx
            if let Some(ref tx) = task_info.backup_notification_tx {
                let _ = tx.send(BackupTransferNotification::DecryptStarted {
                    task_id: task_id.clone(),
                    file_name: filename.clone(),
                });
            }
        } else {
            // 普通任务：发送到 WebSocket
            if let Some(ref ws_manager) = task_info.ws_manager {
                ws_manager.send_if_subscribed(
                    TaskEvent::Download(DownloadEvent::StatusChanged {
                        task_id: task_id.clone(),
                        old_status: "downloading".to_string(),
                        new_status: "decrypting".to_string(),
                        group_id: group_id.clone(),
                        is_backup,
                    }),
                    group_id.clone(),
                );
            }
        }

        // 5. 生成解密后的文件路径（优先使用映射表中的原始文件名）
        let decrypted_path = Self::generate_decrypted_path(
            &local_path,
            &filename,
            task_info.snapshot_manager.as_ref(),
        );

        // 6. 获取加密文件信息（原始大小）
        let original_size = match EncryptionService::get_encrypted_file_info(&local_path)? {
            Some((_, size)) => size,
            None => {
                return Err(anyhow::anyhow!("无法读取加密文件信息"));
            }
        };

        // 7. 执行解密（带进度回调）
        let task_id_clone = task_id.clone();
        let group_id_clone = group_id.clone();
        let ws_manager_clone = task_info.ws_manager.clone();
        let backup_notification_tx_clone = task_info.backup_notification_tx.clone();
        let task_clone = task_info.task.clone();
        let local_path_for_decrypt = local_path.clone();
        let decrypted_path_for_decrypt = decrypted_path.clone();
        let filename_clone = filename.clone();

        let _decrypt_result = tokio::task::spawn_blocking(move || {
            encryption_service.decrypt_file_with_progress(
                &local_path_for_decrypt,
                &decrypted_path_for_decrypt,
                move |processed, total| {
                    let progress = (processed as f64 / total as f64) * 100.0;

                    // 更新任务解密进度
                    if let Ok(mut task) = task_clone.try_lock() {
                        task.update_decrypt_progress(progress);
                    }

                    // 发送解密进度事件
                    if is_backup {
                        // 备份任务：发送到 backup_notification_tx
                        if let Some(ref tx) = backup_notification_tx_clone {
                            let _ = tx.send(BackupTransferNotification::DecryptProgress {
                                task_id: task_id_clone.clone(),
                                file_name: filename_clone.clone(),
                                progress,
                                processed_bytes: processed,
                                total_bytes: total,
                            });
                        }
                    } else {
                        // 普通任务：发送到 WebSocket
                        if let Some(ref ws_manager) = ws_manager_clone {
                            ws_manager.send_if_subscribed(
                                TaskEvent::Download(DownloadEvent::DecryptProgress {
                                    task_id: task_id_clone.clone(),
                                    decrypt_progress: progress,
                                    processed_bytes: processed,
                                    total_bytes: total,
                                    group_id: group_id_clone.clone(),
                                    is_backup,
                                }),
                                group_id_clone.clone(),
                            );
                        }
                    }
                },
            )
        })
            .await
            .map_err(|e| anyhow::anyhow!("解密任务执行失败: {}", e))??;

        // 8. 删除加密文件
        if let Err(e) = tokio::fs::remove_file(&local_path).await {
            warn!("删除加密文件失败: {:?}, 路径: {:?}", e, local_path);
        } else {
            debug!("已删除加密文件: {:?}", local_path);
        }

        // 9. 更新任务信息
        {
            let mut task = task_info.task.lock().await;
            task.mark_decrypt_completed(decrypted_path.clone(), original_size);
            task.local_path = decrypted_path.clone(); // 更新为解密后的路径
        }

        // 10. 🔥 更新持久化文件中的本地路径（解密后的路径）
        if let Some(ref pm) = task_info.persistence_manager {
            if let Err(e) = pm.lock().await.update_local_path(&task_id, decrypted_path.clone()) {
                warn!("更新持久化本地路径失败: {}", e);
            } else {
                debug!("已更新持久化本地路径: {:?}", decrypted_path);
            }
        }

        // 11. 🔥 备份任务：发送解密完成通知
        // 普通任务不发送 DecryptCompleted 事件（解密完成后会立即发送 Completed 事件）
        // 但备份任务需要发送，以便自动备份 manager 转发给前端显示解密完成状态
        if is_backup {
            if let Some(ref tx) = task_info.backup_notification_tx {
                let original_name = decrypted_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                let _ = tx.send(BackupTransferNotification::DecryptCompleted {
                    task_id: task_id.clone(),
                    file_name: filename.clone(),
                    original_name,
                    decrypted_path: decrypted_path.to_string_lossy().to_string(),
                });
            }
        }

        info!("✅ 任务 {} 解密完成，原始大小: {} bytes", task_id, original_size);

        Ok(())
    }

    /// 生成解密后的文件路径
    ///
    /// 优先从映射表查询原始文件名，如果没有则使用默认命名规则
    ///
    /// # 参数
    /// * `encrypted_path` - 加密文件路径
    /// * `filename` - 文件名
    /// * `snapshot_manager` - 快照管理器（可选，用于查询原始文件名）
    ///
    /// # 返回
    /// 解密后的文件路径
    fn generate_decrypted_path(
        encrypted_path: &std::path::Path,
        filename: &str,
        snapshot_manager: Option<&Arc<crate::encryption::snapshot::SnapshotManager>>,
    ) -> PathBuf {
        let parent = encrypted_path.parent().unwrap_or(std::path::Path::new("."));

        // 🔥 优先查询映射表获取原始文件名
        if let Some(snapshot_mgr) = snapshot_manager {
            if let Ok(Some(snapshot_info)) = snapshot_mgr.find_by_encrypted_name(filename) {
                info!(
                    "找到加密文件映射: {} -> {}",
                    filename, snapshot_info.original_name
                );
                return parent.join(&snapshot_info.original_name);
            }
        }

        // 如果没有映射信息，使用默认命名规则（向后兼容）
        if crate::downloader::task::DownloadTask::detect_encrypted_filename(filename) {
            // 从加密文件名提取 UUID
            let uuid = EncryptionService::extract_uuid_from_encrypted_name(filename)
                .unwrap_or("unknown");
            parent.join(format!("decrypted_{}", uuid))
        } else {
            // 移除 .bkup 扩展名
            let stem = encrypted_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("decrypted_file");
            parent.join(stem)
        }
    }

    /// 🔥 获取所有活跃任务的当前速度
    ///
    /// CDN链接刷新机制使用，用于速度异常检测
    ///
    /// # 返回
    /// Vec<(task_id, speed_bytes_per_sec)>
    pub async fn get_active_task_speeds(&self) -> Vec<(String, u64)> {
        let tasks = self.active_tasks.read().await;
        let mut speeds = Vec::with_capacity(tasks.len());

        for (task_id, task_info) in tasks.iter() {
            let speed = {
                let calc = task_info.speed_calc.lock().await;
                calc.speed()
            };
            speeds.push((task_id.clone(), speed));
        }

        speeds
    }

    /// 🔥 获取所有活跃任务的速度（仅速度值）
    ///
    /// ⚠️ 修复问题4：过滤掉未开始和已完成的任务，避免停滞误判
    ///
    /// # 返回
    /// 只包含有效任务的速度列表（已开始且未完成的任务）
    pub async fn get_valid_task_speed_values(&self) -> Vec<u64> {
        let tasks = self.active_tasks.read().await;
        let mut speeds = Vec::new();

        for task_info in tasks.values() {
            // 获取任务进度
            let (progress_bytes, total_bytes) = {
                let task = task_info.task.lock().await;
                (task.downloaded_size, task_info.total_size)
            };

            // 过滤：只包含已开始且未完成的任务
            // progress > 0: 已经开始下载
            // progress < total: 尚未完成
            if progress_bytes > 0 && progress_bytes < total_bytes {
                let speed = {
                    let calc = task_info.speed_calc.lock().await;
                    calc.speed()
                };
                speeds.push(speed);
            }
        }

        speeds
    }

    /// 🔥 获取全局总速度（所有活跃任务速度之和）
    ///
    /// ⚠️ 修复问题3：速度异常检测应使用全局总速度，而非单任务速度
    /// 当多任务下载时，新任务加入会分流带宽，单任务速度下降是正常的
    /// 使用全局速度更准确反映整体网络状况
    ///
    /// # 返回
    /// 全局总速度（字节/秒）
    pub async fn get_global_speed(&self) -> u64 {
        self.get_valid_task_speed_values().await.iter().sum()
    }
}
