use crate::autobackup::events::{BackupTransferNotification, TransferTaskType};
use crate::downloader::{
    ChunkDownloadFailure, ChunkFailureAction, ChunkManager, DownloadEngine, DownloadTask,
    SpeedCalculator, UrlHealthManager,
};
use crate::encryption::service::EncryptionService;
use crate::persistence::PersistenceManager;
use crate::server::events::{DownloadEvent, ProgressThrottler, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::{Context, Result};
use reqwest::Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 🔥 任务级连续 chunk 失败阈值：达到后触发任务级 auto_requeue
///
/// 语义：任务内部连续 N 次分片下载失败（任意分片、任意失败分类）即认为已无进展，
/// 将任务整体退回等待队列重新调度。任一分片成功（有字节写入）会重置计数器。
pub const CONSECUTIVE_CHUNK_FAILURE_LIMIT: u32 = 6;

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
///
/// 注意：此结构体不能 `#[derive(Debug)]`，因为 `http11_trigger` 是
/// `Option<Arc<dyn Fn() + Send + Sync>>` 不实现 `Debug`。
/// 需要调试输出时，使用 `task_id` 字段即可。
#[derive(Clone)]
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
    /// HTTP 客户端（共享引用，代理热更新时自动生效）
    pub client: Arc<StdRwLock<Client>>,
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

    /// 🔥 任务级连续失败计数器（auto_requeue 的「快路」）
    ///
    /// 计数规则：
    /// - 任一分片下载失败时 +1（不区分 Transient/ServerThrottled/Fatal）
    /// - 任一分片下载成功（有字节写入）时 **重置为 0**
    /// - 连续失败次数累计达到 `CONSECUTIVE_CHUNK_FAILURE_LIMIT` 时触发
    ///   任务级 `auto_requeue`（即便仍有分片处于冷却中，也判定已无进展）
    ///
    /// 🔥 **双触发设计（必须与「兜底慢路」并存）**：
    /// - **快路（本字段）**：分片被实际派发并失败时累加计数，到阈值就退回。
    /// - **慢路（调度循环的 None 分支，见 `start_scheduling`）**：当 `select_next_chunk`
    ///   返回 `None` 且 `active_chunk_count == 0`、无 cooling 分片、未完成时，直接触发
    ///   `auto_requeue`。
    ///
    /// 之所以两条都需要：
    /// - 极端死锁场景下（所有 chunk 全部 deferred 且 `undefer_all` 后 `select_next_chunk`
    ///   仍然 None，例如所有 URL 都进入限速/不健康），分片**根本派发不出去**，本字段不会
    ///   再增长，**永远到不了阈值**。
    /// - 此时只能依赖调度循环的 None 分支（慢路）兜底，否则任务会在调度器里无限空转。
    /// - 反过来，慢路也无法替代快路：在分片仍能间断派发但反复失败的场景下，`active_chunk_count`
    ///   通常不为 0，慢路条件不成立，需要快路按累计失败次数及时退回。
    ///
    /// 因此最终对外的退回语义是：**「连续 N 次分片失败 OR 调度器跑空（active=0 且无 cooling）」**。
    pub consecutive_chunk_failures: Arc<AtomicU32>,

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
    pub manager_tasks: Option<
        Arc<RwLock<std::collections::HashMap<String, Arc<Mutex<crate::downloader::DownloadTask>>>>>,
    >,

    // 🔥 链接级重试次数（单次调度内换链接重试的上限）
    /// 从配置 DownloadConfig.max_retries 读取
    pub max_retries: u32,

    // 🔥 代理故障回退管理器
    /// 可选，用于记录代理失败/成功并触发自动回退
    pub fallback_mgr: Option<Arc<crate::common::ProxyFallbackManager>>,

    // 🔥 任务级槽位刷新节流器，所有分片共享
    /// 防止分片切换时重置节流计时器，确保槽位心跳持续有效
    pub slot_touch_throttler: Arc<crate::task_slot_pool::SlotTouchThrottler>,

    // 🔥 auto_requeue 发送端（scheduler 在无可用分片 / Fatal 时触发退回队列）
    /// 由 DownloadManager 提供（requeue_sender()）
    pub requeue_tx: Option<mpsc::UnboundedSender<crate::downloader::manager::AutoRequeueRequest>>,

    // 🔥 HTTP/2 全局降级触发器（download_chunk_with_retry 在零字节 frame error 时调用）
    /// 由 DownloadManager 提供（桥接到 engine.report_h2_zero_failure / reset_h2_zero_failure_counter）
    pub http11_trigger: Option<crate::downloader::engine::H2DowngradeTrigger>,

    // 🔥 文件夹管理器引用（用于子任务释放固定槽位）
    /// 可选，仅文件夹子任务需要
    pub folder_manager: Option<Arc<crate::downloader::FolderDownloadManager>>,
}

impl std::fmt::Debug for TaskScheduleInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskScheduleInfo")
            .field("task_id", &self.task_id)
            .field("chunk_size", &self.chunk_size)
            .field("total_size", &self.total_size)
            .field("max_concurrent_chunks", &self.max_concurrent_chunks)
            .field("slot_id", &self.slot_id)
            .field("is_borrowed_slot", &self.is_borrowed_slot)
            .finish_non_exhaustive()
    }
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
    task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<(String, String, u64, bool)>>>>,
    /// 🔥 备份任务统一通知发送器（用于通知 AutoBackupManager 所有事件）
    /// 包括：进度更新、状态变更、任务完成、任务失败等
    backup_notification_tx: Arc<RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>>,
    /// 🔥 等待队列触发器（任务完成时通知 DownloadManager 启动等待任务）
    waiting_queue_trigger: Arc<RwLock<Option<mpsc::UnboundedSender<()>>>>,
    /// 上一轮的任务数（用于检测任务数变化）
    last_task_count: Arc<AtomicUsize>,
    /// 🔥 解密并发控制信号量（限制同时解密的文件数，避免内存和CPU过载）
    decrypt_semaphore: Arc<Semaphore>,
}

impl ChunkScheduler {
    /// 🔥 计算解密并发数（根据可用 CPU 核心数动态计算）
    ///
    /// 解密是 CPU 密集型 + 磁盘 IO 操作：
    /// - 使用可用并行度的一半作为基准
    /// - 最少 2 个（避免大文件阻塞小文件）
    /// - 最多 8 个（避免内存和 CPU 过载）
    ///
    /// 注意：使用 std::thread::available_parallelism() 而不是 num_cpus
    /// 因为它会考虑 Docker/cgroups 的 CPU 限制
    fn calculate_decrypt_concurrency() -> usize {
        let available_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4); // 获取失败时默认 4 核
        let concurrency = (available_cpus / 2).max(2).min(8);
        info!(
            "解密并发数: {} (可用并行度: {})",
            concurrency, available_cpus
        );
        concurrency
    }

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
            // 🔥 解密并发限制：根据 CPU 核心数动态计算
            // 解密是 CPU 密集型 + 磁盘 IO 操作
            // 使用 CPU 核心数的一半（至少 2，最多 8）作为并发数
            decrypt_semaphore: Arc::new(Semaphore::new(Self::calculate_decrypt_concurrency())),
        };

        // 启动全局调度循环
        scheduler.start_scheduling();

        scheduler
    }

    /// 设置任务完成通知发送器
    ///
    /// FolderDownloadManager 调用此方法设置 channel sender，
    /// 当文件夹子任务完成时会发送 group_id 到 channel
    pub async fn set_task_completed_sender(
        &self,
        tx: mpsc::UnboundedSender<(String, String, u64, bool)>,
    ) {
        let mut sender = self.task_completed_tx.write().await;
        *sender = Some(tx);
        info!("任务完成通知 channel 已设置");
    }

    /// 🔥 通知文件夹管理器子任务失败
    ///
    /// 供 DownloadManager 的非调度器失败路径（槽位超时、0延迟启动失败等）调用
    pub async fn notify_subtask_failed(&self, group_id: String, task_id: String, total_size: u64) {
        let tx_guard = self.task_completed_tx.read().await;
        if let Some(tx) = tx_guard.as_ref() {
            if let Err(e) = tx.send((group_id, task_id, total_size, false)) {
                error!("发送子任务失败通知失败: {}", e);
            }
        }
    }

    /// 🔥 设置备份任务统一通知发送器
    ///
    /// AutoBackupManager 调用此方法设置 channel sender，
    /// 所有备份相关事件（进度、状态、完成、失败等）都通过此 channel 发送
    pub async fn set_backup_notification_sender(
        &self,
        tx: mpsc::UnboundedSender<BackupTransferNotification>,
    ) {
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
            task_id, active_count
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
        let decrypt_semaphore = self.decrypt_semaphore.clone();

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

                    // 获取下一个待下载的分片索引（跳过正在下载、已推迟、冷却中的分片）
                    // 🔥 若所有非 deferred 分片都已完成/下载中/冷却中 → 返回 None
                    // 🔥 若 None 时有 deferred 分片 → 解冻所有 deferred，重新挑选
                    let next_chunk_index = {
                        let mut manager = task_info.chunk_manager.lock().await;
                        let now = std::time::Instant::now();

                        // 第一轮：跳过 completed / downloading / deferred / cooling_down
                        let mut index = manager.chunks().iter().position(|chunk| {
                            !chunk.completed
                                && !chunk.downloading
                                && !chunk.deferred
                                && chunk.cooldown_until.map(|t| now >= t).unwrap_or(true)
                        });

                        // 若没有可调度且存在已推迟分片 → 解冻后重挑
                        if index.is_none() && manager.has_deferred() {
                            let deferred_count = manager.deferred_count();
                            info!(
                                "任务 {} 无可用分片但存在 {} 个已推迟分片，解冻所有 deferred 重试",
                                task_id, deferred_count
                            );
                            manager.undefer_all();
                            index = manager.chunks().iter().position(|chunk| {
                                !chunk.completed && !chunk.downloading && !chunk.deferred
                            });
                        }

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
                                task_completed_tx.clone(),
                                waiting_queue_trigger.clone(),
                            );

                            scheduled_count += 1;
                            consecutive_empty_rounds = 0; // 重置计数器

                            // 继续下一个任务（保证公平轮询）
                        }
                        None => {
                            // 该任务没有可调度的分片
                            //
                            // 🔥 这里是 auto_requeue 的「兜底慢路」（与 spawn_chunk_download 失败回调里
                            //    `consecutive_chunk_failures >= LIMIT` 的「快路」并存）。
                            //
                            //    为什么需要兜底慢路：当所有 chunk 都已经 deferred、`undefer_all` 之后
                            //    `select_next_chunk` 仍返回 None（典型场景：URL 池全部不健康、限速、CDN 切走），
                            //    分片**根本派发不出去** → 快路计数器不会再增长 → 永远到不了阈值。
                            //    没有这条慢路，任务会在调度器里无限空转。
                            //    详见 `consecutive_chunk_failures` 字段文档的「双触发设计」章节。

                            // 🔥 先确认是否所有分片真的都完成（完成判定只看 completed 标志）
                            let all_done = {
                                let manager = task_info.chunk_manager.lock().await;
                                manager.chunks().iter().all(|c| c.completed)
                            };

                            // 🔥 若非真完成（有 deferred 但尚未解冻成功 / 下载中未完成） → 不走完成路径
                            // 由于前面已经 undefer_all，若 all_done == false 说明还有分片在 downloading 或所有链接/重试都已耗尽
                            if !all_done {
                                // 兜底慢路触发条件：必须同时满足
                                //   - 活跃分片为 0（没有分片在下载，否则快路会随失败累加触发）
                                //   - 没有"冷却中"的分片（冷却中只是等待退避到期，不算死锁，等下一轮即可）
                                //   - 没有"刚完成 undefer_all 后仍可调度"的分片（前面 select_next_chunk 已验证 None）
                                if task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                                    let has_cooling = {
                                        let m = task_info.chunk_manager.lock().await;
                                        m.has_cooling_down()
                                    };

                                    if has_cooling {
                                        // 仅冷却中等待，不是死锁；继续下一个任务，下一轮调度会自动重试
                                        debug!(
                                            "任务 {} 活跃 0 但存在冷却中的分片，等待退避到期，不触发 auto_requeue",
                                            task_id
                                        );
                                    } else {
                                        // 真正死锁：活跃 0 且无冷却分片，所有链接/重试都已耗尽
                                        // → 触发兜底慢路 auto_requeue（与快路按 consecutive_chunk_failures 阈值
                                        //   触发的是同一个 channel，manager 端 `auto_requeue_task` 已做幂等）
                                        let (chunks_total, chunks_done) = {
                                            let m = task_info.chunk_manager.lock().await;
                                            let total = m.chunks().len();
                                            let done =
                                                m.chunks().iter().filter(|c| c.completed).count();
                                            (total, done)
                                        };
                                        warn!(
                                            "任务 {} 调度器陷入死锁: 活跃 0, 无冷却分片, 已完成 {}/{}, 触发 auto_requeue 退回等待队列（兜底慢路）",
                                            task_id, chunks_done, chunks_total
                                        );

                                        // 🔥 从 active_tasks 移除（防止重复触发）
                                        active_tasks.write().await.remove(task_id);

                                        // 🔥 通过 channel 触发 auto_requeue
                                        if let Some(ref tx) = task_info.requeue_tx {
                                            let _ = tx.send(
                                                crate::downloader::manager::AutoRequeueRequest {
                                                    task_id: task_id.clone(),
                                                    reason: format!(
                                                        "所有分片调度失败（活跃 0，已完成 {}/{}）",
                                                        chunks_done, chunks_total
                                                    ),
                                                },
                                            );
                                        } else {
                                            error!(
                                                "任务 {} 无 requeue_tx，无法退回等待队列",
                                                task_id
                                            );
                                        }
                                    }
                                }

                                consecutive_empty_rounds += 1;
                                if consecutive_empty_rounds >= task_count {
                                    break;
                                }
                                continue;
                            }

                            // 检查是否所有分片都完成且没有活跃下载
                            if task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                                // 所有分片完成，从调度器移除
                                info!("任务 {} 所有分片完成，从调度器移除", task_id);
                                active_tasks.write().await.remove(task_id);

                                // 🔥 修复：取消 cancellation_token，停止速度异常检测和线程停滞检测循环
                                task_info.cancellation_token.cancel();
                                debug!("任务 {} 的 cancellation_token 已取消", task_id);

                                // 🔥 异步并发解密：将解密任务 spawn 到独立线程，不阻塞调度循环
                                let task_id_clone = task_id.to_string();
                                let task_info_clone = task_info.clone();
                                let task_completed_tx_clone = task_completed_tx.clone();
                                let backup_notification_tx_clone = backup_notification_tx.clone();
                                let waiting_queue_trigger_clone = waiting_queue_trigger.clone();
                                let decrypt_semaphore_clone = decrypt_semaphore.clone();

                                tokio::spawn(async move {
                                    // 🔥 获取解密信号量，限制并发解密数量
                                    let _permit = decrypt_semaphore_clone.acquire().await.unwrap();
                                    debug!("任务 {} 获取解密信号量，开始解密流程", task_id_clone);

                                    // 🔥 R20: 快照本次解密协程的 epoch，用于在
                                    //   "暂停 Decrypting → 快速恢复 → 新一轮解密协程启动 → 旧协程跑完"
                                    //   race 中识别并失效旧协程的破坏性副作用。
                                    //   把 my_epoch 一路传到 try_decrypt_if_encrypted、
                                    //   spawn_blocking 进度回调、handle_task_completion，
                                    //   各检查点用它和 task.decrypt_epoch 比对。
                                    //   见 DownloadTask::decrypt_epoch 的字段文档。
                                    let my_epoch = {
                                        let t = task_info_clone.task.lock().await;
                                        t.decrypt_epoch
                                    };

                                    let finalize_result =
                                        Self::finalize_download_output(&task_info_clone).await;
                                    let decrypt_result = match finalize_result {
                                        Ok(()) => {
                                            Self::try_decrypt_if_encrypted(
                                                &task_info_clone,
                                                my_epoch,
                                            )
                                            .await
                                        }
                                        Err(e) => Err(e),
                                    };

                                    // 处理解密结果
                                    Self::handle_task_completion(
                                        &task_id_clone,
                                        &task_info_clone,
                                        decrypt_result,
                                        my_epoch,
                                        &task_completed_tx_clone,
                                        &backup_notification_tx_clone,
                                        &waiting_queue_trigger_clone,
                                    )
                                    .await;

                                    debug!("任务 {} 解密流程完成，释放解密信号量", task_id_clone);
                                    // _permit 在这里自动释放
                                });
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
                    // 有新分片启动，短暂延迟后继续调度
                    tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                } else {
                    // 所有任务都达到并发上限或无待下载分片，延长等待避免空转
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
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
    /// * `waiting_queue_trigger` - 等待队列触发器（失败时也要通知排队任务启动）
    #[allow(clippy::too_many_arguments)]
    fn spawn_chunk_download(
        chunk_index: usize,
        task_info: TaskScheduleInfo,
        // 🔥 以下 4 个参数在重构后仅在"任务完成路径"被调用，单分片失败不再触发整任务失败
        // 保留参数是为了兼容现有调用点，unused 警告通过 _ 前缀抑制
        _active_tasks: Arc<RwLock<HashMap<String, TaskScheduleInfo>>>,
        slot_pool: Arc<ChunkSlotPool>,
        global_active_count: Arc<AtomicUsize>,
        _backup_notification_tx: Arc<
            RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>,
        >,
        _task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<(String, String, u64, bool)>>>>,
        _waiting_queue_trigger: Arc<RwLock<Option<mpsc::UnboundedSender<()>>>>,
    ) {
        tokio::spawn(async move {
            let task_id = task_info.task_id.clone();

            // 从槽位池获取一个槽位ID
            let slot_id = slot_pool.acquire();

            info!(
                "[分片线程{}] 分片 #{} 获得线程资源，开始下载",
                slot_id, chunk_index
            );

            // 每次调度时从共享引用读取最新客户端（代理热更新后自动生效）
            let client = task_info.client.read().unwrap().clone();

            // 调用 DownloadEngine 的下载方法（传入事件总线和节流器）
            let result = DownloadEngine::download_chunk_with_retry(
                chunk_index,
                client,
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
                Some(task_info.slot_touch_throttler.clone()), // 🔥 任务级共享槽位刷新节流器
                task_info.max_retries,                // 🔥 链接级重试次数（从配置读取）
                task_info.fallback_mgr.clone(),       // 🔥 代理故障回退管理器
                task_info.http11_trigger.clone(),     // 🔥 HTTP/2 降级触发器
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
                    // 🔥 分片下载成功 → 重置任务级连续失败计数
                    //   任何一个分片完整下载成功，都认为任务重新"有进展"
                    let prev = task_info
                        .consecutive_chunk_failures
                        .swap(0, Ordering::SeqCst);
                    if prev > 0 {
                        debug!(
                            "[分片线程{}] 分片 #{} 成功，重置任务 {} 连续失败计数 {} -> 0",
                            slot_id, chunk_index, task_id, prev
                        );
                    }

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
                        // 🔥 取消/暂停路径也要持久化分片内部分进度
                        {
                            let manager = task_info.chunk_manager.lock().await;
                            let bytes_downloaded = manager.get_bytes_downloaded(chunk_index);
                            if bytes_downloaded > 0 {
                                if let Some(ref pm) = task_info.persistence_manager {
                                    pm.lock().await.on_chunk_partial_progress(
                                        &task_id,
                                        chunk_index,
                                        bytes_downloaded,
                                    );
                                }
                            }
                        }
                    } else {
                        // 🔥 持久化部分进度
                        {
                            let manager = task_info.chunk_manager.lock().await;
                            let bytes_downloaded = manager.get_bytes_downloaded(chunk_index);
                            if bytes_downloaded > 0 {
                                if let Some(ref pm) = task_info.persistence_manager {
                                    pm.lock().await.on_chunk_partial_progress(
                                        &task_id,
                                        chunk_index,
                                        bytes_downloaded,
                                    );
                                }
                            }
                        }

                        // 🔥 根据 ChunkDownloadFailure 分类决定处理策略
                        //    Transient     → 冷却后重试（指数退避 100ms→5s）
                        //    ServerThrottled → 加长冷却（1s→10s）
                        //    Fatal         → 标记为 deferred，等其他活跃分片完成后再解冻
                        let current_retries = {
                            let manager = task_info.chunk_manager.lock().await;
                            manager.retries_of(chunk_index)
                        };

                        // 外层调度级重试上限 = 内层链接级重试 * 2
                        let max_schedule_retries = task_info.max_retries * 2;

                        // 🔥 R25: Cancelled 短路——理论上不可达
                        //
                        // engine 返回 Cancelled 时，必然是 cancellation_token.is_cancelled() == true
                        // 的状态下返回的（见 download_chunk_with_retry 顶部 + Err(e) 分支）。
                        // 而 scheduler 在进入本 match 之前的 `if task_info.cancellation_token.is_cancelled()`
                        // 短路（见上面 line ~915 的取消分支）会先一步把控制流走掉，
                        // 永不进入本 match。
                        //
                        // 但 Rust 要求 match 穷举所有变体；显式处理 Cancelled 作为防御性兜底，
                        // 即使将来 cancellation 检查重构出现漏洞也不会触发误降级。
                        //
                        // 注意：这里所在的 `Err(e)` 分支是 `tokio::spawn(async move { ... })`
                        // 的一次性闭包，不是循环。`return` 直接退出本次 chunk 的 spawn 任务，
                        // 与正常成功/失败的 fall-through 行为一致；不会跳过其它分片或调度。
                        if matches!(&e, ChunkDownloadFailure::Cancelled) {
                            info!(
                                "[分片线程{}] 分片 #{} 收到 Cancelled 分类（防御性兜底），\
                                 短路跳过所有失败处理逻辑（不冷却 / 不 deferred / 不计入连续失败 / 不上报 H2 降级）",
                                slot_id, chunk_index
                            );
                            // 不上报 H2 降级、不计入 consecutive_chunk_failures、不 mark_deferred
                            // 上层 cancel_token 会让任务整体进入 Paused
                            return;
                        }

                        let action = match &e {
                            ChunkDownloadFailure::Transient(_) => {
                                // 冷却 100ms → 5s 指数退避（沿用 calculate_backoff_delay 语义）
                                let backoff_ms = 100u64 * 2u64.pow(current_retries.min(6));
                                let capped = backoff_ms.min(5000);
                                warn!(
                                    "[分片线程{}] 分片 #{} Transient 失败，冷却 {}ms 后重试: {}",
                                    slot_id, chunk_index, capped, e
                                );
                                ChunkFailureAction::Cooldown(std::time::Duration::from_millis(
                                    capped,
                                ))
                            }
                            ChunkDownloadFailure::ServerThrottled(_) => {
                                // 服务端限流：更长退避 1s → 10s
                                let backoff_ms = 1000u64 * 2u64.pow(current_retries.min(4));
                                let capped = backoff_ms.min(10000);
                                warn!(
                                    "[分片线程{}] 分片 #{} ServerThrottled 失败，冷却 {}ms 后重试: {}",
                                    slot_id, chunk_index, capped, e
                                );
                                ChunkFailureAction::Cooldown(std::time::Duration::from_millis(
                                    capped,
                                ))
                            }
                            ChunkDownloadFailure::Fatal(_) => {
                                warn!(
                                    "[分片线程{}] 分片 #{} Fatal 失败，标记为 deferred 等待其他分片完成后重试: {}",
                                    slot_id, chunk_index, e
                                );
                                ChunkFailureAction::Deferred
                            }
                            ChunkDownloadFailure::Cancelled => {
                                // 不可达：上面 matches! 已 short-circuit return，这里仅为穷举性。
                                unreachable!("Cancelled 已被前置短路处理")
                            }
                        };

                        let chunk_retries = {
                            let mut manager = task_info.chunk_manager.lock().await;
                            manager.fail_chunk(chunk_index, action)
                        };

                        // 仅当 Transient/ServerThrottled 达到 max_schedule_retries 时，
                        // 才 mark_deferred（让它等其他分片完成）
                        let should_defer_now = matches!(
                            e,
                            ChunkDownloadFailure::Transient(_)
                                | ChunkDownloadFailure::ServerThrottled(_)
                        ) && chunk_retries >= max_schedule_retries;

                        if should_defer_now {
                            warn!(
                                "[分片线程{}] 分片 #{} 达到调度重试上限 {}/{}，标记为 deferred",
                                slot_id, chunk_index, chunk_retries, max_schedule_retries
                            );
                            let mut manager = task_info.chunk_manager.lock().await;
                            manager.mark_deferred(chunk_index);
                        }

                        // 🔥 任务级连续失败计数：任一分片失败累加 1（auto_requeue 的「快路」）
                        //    达到阈值 CONSECUTIVE_CHUNK_FAILURE_LIMIT 时，主动触发 auto_requeue
                        //    将任务整体退回等待队列（冷却期后再重试），避免死等 deferred/cooldown。
                        //
                        //    注意：本路径只在「分片真的被派发并失败」时累加。如果整个任务陷入
                        //    「所有 chunk 都 deferred、根本派不出去」的死锁，本计数器不会增长，
                        //    需要由调度循环 `start_scheduling` 中 `select_next_chunk == None`
                        //    分支的「兜底慢路」触发 auto_requeue。两者通过同一个 requeue channel
                        //    退回任务，manager 端 `auto_requeue_task` 已做状态幂等保护。
                        //    详见 `consecutive_chunk_failures` 字段文档的「双触发设计」章节。
                        let task_fail_count = task_info
                            .consecutive_chunk_failures
                            .fetch_add(1, Ordering::SeqCst)
                            + 1;
                        debug!(
                            "[分片线程{}] 分片 #{} 失败，任务 {} 连续失败计数 {}/{}",
                            slot_id,
                            chunk_index,
                            task_id,
                            task_fail_count,
                            CONSECUTIVE_CHUNK_FAILURE_LIMIT
                        );

                        if task_fail_count >= CONSECUTIVE_CHUNK_FAILURE_LIMIT
                            && !task_info.cancellation_token.is_cancelled()
                        {
                            // 原子 compare_exchange 保证只触发一次：
                            //   多个分片线程可能并发命中阈值，仅当前面的值确实 >= limit
                            //   且成功置回 0 的那次才负责发送 auto_requeue 请求
                            let swapped = task_info.consecutive_chunk_failures.compare_exchange(
                                task_fail_count,
                                0,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            );
                            if swapped.is_ok() {
                                warn!(
                                    "任务 {} 连续 {} 次 chunk 失败，触发 auto_requeue（最近分片 #{} 错误: {}）",
                                    task_id, task_fail_count, chunk_index, e
                                );

                                if let Some(ref tx) = task_info.requeue_tx {
                                    let _ =
                                        tx.send(crate::downloader::manager::AutoRequeueRequest {
                                            task_id: task_id.clone(),
                                            reason: format!(
                                                "连续 {} 次 chunk 失败",
                                                task_fail_count
                                            ),
                                        });
                                } else {
                                    error!(
                                        "任务 {} 达连续失败阈值但 requeue_tx 缺失，无法退回等待队列",
                                        task_id
                                    );
                                }
                            }
                        }
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

    /// 🔥 处理任务完成（解密后的后续处理）
    ///
    /// 包括：更新任务状态、发送事件、归档、释放槽位、通知等
    async fn handle_task_completion(
        task_id: &str,
        task_info: &TaskScheduleInfo,
        decrypt_result: Result<()>,
        my_epoch: u64,
        task_completed_tx: &Arc<RwLock<Option<mpsc::UnboundedSender<(String, String, u64, bool)>>>>,
        backup_notification_tx: &Arc<
            RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>,
        >,
        waiting_queue_trigger: &Arc<RwLock<Option<mpsc::UnboundedSender<()>>>>,
    ) {
        // 🔥 原子判定：在同一把锁内完成"Paused / epoch 短路检查"+"写终态"两件事，
        //    避免把检查和写入拆成两段锁时，中间窗口被 `cancel_tasks_by_group`
        //    插入 Paused 写入、第二段锁仍无条件 mark_completed/failed 覆盖的竞态。
        //
        // 背景：`DownloadManager::cancel_tasks_by_group`（pause_folder 路径）已把
        // `TaskStatus::Decrypting` 纳入 active 分支，会在解密期间把任务状态翻成
        // `Paused` 并清空槽位字段、释放 fallback 全局 fixed slot；同时调
        // `invalidate_decrypt_epoch()` 递增 `task.decrypt_epoch` 让本协程过期。
        //
        // 但仅靠"翻状态字段 + 释放槽位"还不够：本函数原先无条件调用
        // `mark_completed()` / `mark_failed()` 覆盖状态、发送 `Completed` / `Failed`
        // WebSocket 事件、归档到历史数据库、从 `manager.tasks` 移除、通知文件夹任务
        // 计数器、通知 AutoBackupManager。一旦解密协程在用户暂停后跑完，这些副作用
        // 都会触发，让"暂停文件夹"的语义彻底失效。
        //
        // 关键：Paused / epoch 判定**必须**和 mark_completed/failed 在同一次
        // `lock().await` 作用域内。否则会出现：
        //   1. 我们先拿锁 → 读到 Decrypting / epoch 一致 → 放锁
        //   2. cancel_tasks_by_group 拿锁 → 写 Paused + invalidate_decrypt_epoch → 放锁
        //   3. 我们再拿锁 → 无条件 mark_completed/failed 把 Paused 覆盖
        // 现在把两件事合并到同一个 block：
        //
        // - 若此次锁内读到 `Paused` 或 `epoch != my_epoch`：直接 return（guard 自动 drop 释放锁）。
        //   全部副作用不执行，任务保持当前状态等用户恢复或新协程接手。
        // - 否则在同一锁段内 mark_completed / mark_failed 并取出后续需要的字段快照。
        //
        // **三种 race 边界分析（合并后都安全）：**
        //
        //   a. **cancel_tasks_by_group 抢先**（pause）：它 lock → 写 Paused +
        //      invalidate_decrypt_epoch → unlock；我们随后 lock → 读到 Paused 或
        //      epoch 不一致 → outcome=Stale → return。
        //
        //   b. **我们抢先**：lock → 读到 Decrypting / epoch 一致 → 同锁内
        //      mark_completed/failed → unlock；cancel_tasks_by_group 随后 lock →
        //      入口判定 status = Completed/Failed 不是 Decrypting，不进它的 active
        //      分支，不覆盖。
        //
        //   c. **暂停后立即恢复（旧协程仍在 spawn_blocking 中）**：
        //      暂停时：cancel_tasks_by_group 写 Paused + invalidate_decrypt_epoch（epoch +1）
        //      恢复时：resume_task 翻 Pending → scheduler 重新 spawn 新协程，捕获新 epoch
        //      旧协程到 handle_task_completion：lock → 读到 status 已是 Pending/
        //      Downloading/Decrypting（被新协程或新调度修改），但 task.decrypt_epoch
        //      已 +1 不等于 my_epoch → outcome=Stale → return。
        //      新协程到 handle_task_completion：epoch 一致，正常写终态。
        //      （这条 race 是 R19 之前的实现盲点：原代码只查 status，旧协程发现
        //      status≠Paused 就继续写终态，污染新协程状态机。R20 加 epoch 才闭环。）
        //
        // 关于副作用幂等性：cancel_tasks_by_group 走 Decrypting 分支已经释放了
        // owner=task_id 的 fallback 全局 fixed slot；解密成功的本地文件由
        // try_decrypt_if_encrypted 7.5 hard-check 在过期/暂停时清理掉
        // （decrypted_path 残留），不会影响新协程的工作。
        enum CompletionOutcome {
            /// 任务被外部暂停，或本协程已被新协程接替（epoch 失效）
            Stale,
            Completed {
                group_id: Option<String>,
                is_backup: bool,
            },
            Failed {
                group_id: Option<String>,
                is_backup: bool,
                error_msg: String,
            },
        }

        let outcome = {
            let mut t = task_info.task.lock().await;

            if t.status == crate::downloader::TaskStatus::Paused || t.decrypt_epoch != my_epoch {
                CompletionOutcome::Stale
            } else if let Err(ref e) = decrypt_result {
                let error_msg = format!("解密失败: {}", e);
                t.mark_failed(error_msg.clone());
                CompletionOutcome::Failed {
                    group_id: t.group_id.clone(),
                    is_backup: t.is_backup,
                    error_msg,
                }
            } else {
                t.mark_completed();
                CompletionOutcome::Completed {
                    group_id: t.group_id.clone(),
                    is_backup: t.is_backup,
                }
            }
        };

        // Stale 分支：跳过一切终态副作用，任务保持当前状态等用户恢复或新协程接手
        let (group_id, is_backup, decrypt_error) = match outcome {
            CompletionOutcome::Stale => {
                info!(
                    "任务 {} 解密协程结束但已被暂停或被新协程接替（my_epoch={}），\
                     跳过 handle_task_completion 终态收尾",
                    task_id, my_epoch
                );
                return;
            }
            CompletionOutcome::Failed {
                group_id,
                is_backup,
                error_msg,
            } => {
                error!("任务 {} 解密失败: {}", task_id, error_msg);
                (group_id, is_backup, Some(error_msg))
            }
            CompletionOutcome::Completed {
                group_id,
                is_backup,
            } => (group_id, is_backup, None),
        };

        // 发布任务事件
        if !is_backup {
            if let Some(ref ws_manager) = task_info.ws_manager {
                if let Some(ref error_msg) = decrypt_error {
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

        // 处理持久化和清理
        if decrypt_error.is_none() {
            if let Some(ref pm) = task_info.persistence_manager {
                if let Err(e) = pm.lock().await.on_task_completed(task_id) {
                    error!("归档下载任务到历史数据库失败: {}", e);
                } else {
                    debug!("下载任务 {} 已归档到历史数据库", task_id);
                }
            }
            // 🔥 分享直下任务不从内存中移除，由转存管理器清理后移除
            let is_share_direct_download = task_info.task.lock().await.is_share_direct_download;
            if !is_share_direct_download {
                if let Some(ref manager_tasks) = task_info.manager_tasks {
                    manager_tasks.write().await.remove(task_id);
                    debug!("下载任务 {} 已从 DownloadManager.tasks 中移除", task_id);
                }
            } else {
                debug!(
                    "分享直下任务 {} 完成，保留在内存中等待转存管理器清理",
                    task_id
                );
            }
        } else {
            if let Some(ref pm) = task_info.persistence_manager {
                if let Err(e) = pm
                    .lock()
                    .await
                    .update_task_error(task_id, decrypt_error.clone().unwrap_or_default())
                {
                    warn!("更新下载任务错误信息失败: {}", e);
                }
            }
        }

        // 释放任务槽位
        if let Some(slot_id) = task_info.slot_id {
            if !task_info.is_borrowed_slot {
                if let Some(ref slot_pool) = task_info.task_slot_pool {
                    slot_pool.release_fixed_slot(task_id).await;
                    info!("任务 {} 完成，释放固定槽位 {}", task_id, slot_id);
                }
            }
        }

        // 通知文件夹任务补充（发送 group_id 和 task_id）
        if let Some(gid) = group_id.clone() {
            let tx_guard = task_completed_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let is_success = decrypt_error.is_none();
                if let Err(e) = tx.send((
                    gid.clone(),
                    task_id.to_string(),
                    task_info.total_size,
                    is_success,
                )) {
                    error!("发送任务完成通知失败: {}", e);
                }
            }
        }

        // 通知备份管理器
        if is_backup {
            let tx_guard = backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = if let Some(ref error_msg) = decrypt_error {
                    BackupTransferNotification::Failed {
                        task_id: task_id.to_string(),
                        task_type: TransferTaskType::Download,
                        error_message: error_msg.clone(),
                    }
                } else {
                    BackupTransferNotification::Completed {
                        task_id: task_id.to_string(),
                        task_type: TransferTaskType::Download,
                        upload_meta: None,
                    }
                };
                let _ = tx.send(notification);
            }
        }

        // 触发等待队列检查
        {
            let trigger_guard = waiting_queue_trigger.read().await;
            if let Some(trigger) = trigger_guard.as_ref() {
                let _ = trigger.send(());
            }
        }
    }

    /// 下载完成后：校验临时文件大小 → 重命名为最终路径
    ///
    /// 兼容旧任务：如果 output_path == local_path（旧任务直接写最终文件），仅做大小校验
    async fn finalize_download_output(task_info: &TaskScheduleInfo) -> Result<()> {
        let final_path = {
            let task = task_info.task.lock().await;
            task.local_path.clone()
        };

        // 旧任务兼容：output_path 等于 final_path 时，只做大小校验
        if task_info.output_path == final_path {
            let metadata = tokio::fs::metadata(&final_path)
                .await
                .context("获取最终文件元数据失败")?;
            if metadata.len() != task_info.total_size {
                anyhow::bail!(
                    "文件大小不匹配: 实际 {} bytes, 期望 {} bytes (路径: {:?})",
                    metadata.len(),
                    task_info.total_size,
                    final_path
                );
            }
            return Ok(());
        }

        // 新任务：校验临时文件大小
        let metadata = tokio::fs::metadata(&task_info.output_path)
            .await
            .context("获取临时下载文件元数据失败")?;
        if metadata.len() != task_info.total_size {
            anyhow::bail!(
                "临时下载文件大小不匹配: 实际 {} bytes, 期望 {} bytes (路径: {:?})",
                metadata.len(),
                task_info.total_size,
                task_info.output_path
            );
        }

        // 如果最终路径已存在（异常残留），先删除
        if final_path.exists() {
            tokio::fs::remove_file(&final_path)
                .await
                .context("清理旧的最终文件失败")?;
            warn!("检测到已存在的最终文件，已先删除再重命名: {:?}", final_path);
        }

        tokio::fs::rename(&task_info.output_path, &final_path)
            .await
            .context("重命名临时下载文件失败")?;
        info!(
            "✅ 临时文件已重命名: {:?} -> {:?}",
            task_info.output_path, final_path
        );

        Ok(())
    }

    /// 🔥 检测并解密加密文件
    ///
    /// 下载完成后自动检测文件是否为加密文件，如果是则执行解密流程
    ///
    /// # 参数
    /// * `task_info` - 任务调度信息
    ///
    /// # 返回
    /// - Ok(()) - 不是加密文件或解密成功；也包括"被外部暂停或被 epoch 失效（旧协程过期）
    ///            而提前退出"的两种短路情形——这两种情形下 handle_task_completion
    ///            会因为同样的 status / epoch 检查走 Stale 分支 return 不写终态。
    /// - Err(e) - 解密失败
    ///
    /// # 参数
    /// - `my_epoch`：本次解密协程启动时由调度器主循环快照的 `task.decrypt_epoch` 值，
    ///               用于在所有 Paused 检查点同时校验 epoch 是否仍有效（=任务未被
    ///               cancel_tasks_by_group 失效），以应对"暂停后立即恢复"的 race。
    async fn try_decrypt_if_encrypted(task_info: &TaskScheduleInfo, my_epoch: u64) -> Result<()> {
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

        let is_encrypted_by_name =
            crate::downloader::task::DownloadTask::detect_encrypted_filename(&filename);

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
            if let (Some(version), Some(ref config_store)) =
                (key_version, &task_info.encryption_config_store)
            {
                match config_store.get_key_by_version(version) {
                    Ok(Some(key_info)) => {
                        info!(
                            "任务 {} 使用 key_version={} 的密钥进行解密",
                            task_id, version
                        );
                        match EncryptionService::from_base64_key(
                            &key_info.master_key,
                            key_info.algorithm,
                        ) {
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
                                        warn!(
                                            "任务 {} 是加密文件但没有配置加密服务，跳过解密",
                                            task_id
                                        );
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
                        error: None,
                    }),
                    group_id.clone(),
                );
            }
        }

        // 5. 生成解密后的文件路径（优先使用映射表中的原始文件名）
        //
        //    decrypted_path 是用户语义上的最终目标路径，由 generate_decrypted_path
        //    根据加密文件名稳定推导。多次解密尝试（特别是"暂停 → 恢复 → 新协程接手"
        //    场景下的新旧两个协程）会复用同一路径，所以 stale 分支不能直接 remove
        //    它（否则旧协程会删掉新协程的输出）。
        let decrypted_path = Self::generate_decrypted_path(
            &local_path,
            &filename,
            task_info.snapshot_manager.as_ref(),
        );

        // 🔥 R21: attempt-scoped 输出路径（spawn_blocking 实际写入位置）
        //
        // 路径格式：`{decrypted_path}.decrypt-{my_epoch}.tmp`
        // 例如最终路径 `foo.txt` + epoch=3 → `foo.txt.decrypt-3.tmp`
        //
        // 为什么需要这层间接：
        // - R20 引入 epoch 后，stale 协程能识别"我是过期协程"，但 R20 的 7.5 检查
        //   仍把 final decrypted_path 当 attempt 私有的临时文件 remove_file，
        //   实际上多个协程会复用同一 final 路径 → 旧协程删掉新协程的输出文件。
        //
        // - 把 spawn_blocking 输出重定向到 attempt-scoped 路径，每次解密尝试
        //   都有自己独立的 .decrypt-{epoch}.tmp 文件；stale 协程只清理
        //   自己的 .decrypt-{old_epoch}.tmp，不会触碰 final 或其它协程的 attempt。
        //
        // - 通过所有 stale check 的协程会把 attempt 文件 rename 到 final
        //   （rename 是单 fs 操作，原子性较好；同目录同盘 rename 在所有
        //    主流 OS 上都是原子的）。
        //
        // 用 push 拼接而非 with_extension：with_extension 会替换原扩展名
        // （foo.txt → foo.decrypt-X.tmp），丢失原 .txt 标识；push 直接附加保持
        // 原文件名 + 后缀（foo.txt → foo.txt.decrypt-X.tmp），更清晰。
        let attempt_decrypted_path = {
            let mut p = decrypted_path.clone().into_os_string();
            p.push(format!(".decrypt-{}.tmp", my_epoch));
            PathBuf::from(p)
        };

        // 6. 获取加密文件信息（原始大小）
        let original_size = match EncryptionService::get_encrypted_file_info(&local_path)? {
            Some((_, size)) => size,
            None => {
                return Err(anyhow::anyhow!("无法读取加密文件信息"));
            }
        };

        // 🔥 7.0 spawn_blocking 之前预检 Paused / epoch：如果用户在 mark_decrypting 之后、
        //         CPU 解密开始之前就暂停了文件夹（或本协程已被新一轮恢复 spawn 的
        //         协程接替），不要浪费 CPU 启动 spawn_blocking。
        //         直接 return Ok(()) → handle_task_completion 入口判定 Paused/epoch 短路。
        //
        //         注意：这只是优化，不能依赖它做正确性保证——race 之下用户可能
        //         在 spawn_blocking 启动之后才暂停，所以 spawn_blocking 后还要再做一次
        //         硬检查（见 7.5）。
        {
            let t = task_info.task.lock().await;
            if t.status == crate::downloader::TaskStatus::Paused {
                info!(
                    "任务 {} 解密预检：已被外部暂停，跳过 spawn_blocking 与后续收尾",
                    task_id
                );
                return Ok(());
            }
            if t.decrypt_epoch != my_epoch {
                info!(
                    "任务 {} 解密预检：本协程 epoch={} 已被失效（当前={}），\
                     旧协程过期，跳过 spawn_blocking 与后续收尾",
                    task_id, my_epoch, t.decrypt_epoch
                );
                return Ok(());
            }
        }

        // 7. 执行解密（带进度回调）
        //    🔥 R21: 输出路径从 decrypted_path（共享 final）改为 attempt_decrypted_path
        //            （本次尝试独有），通过 stale check 后再 rename 到 final。
        let task_id_clone = task_id.clone();
        let group_id_clone = group_id.clone();
        let ws_manager_clone = task_info.ws_manager.clone();
        let backup_notification_tx_clone = task_info.backup_notification_tx.clone();
        let task_clone = task_info.task.clone();
        let local_path_for_decrypt = local_path.clone();
        let attempt_decrypted_path_for_decrypt = attempt_decrypted_path.clone();
        let filename_clone = filename.clone();

        let _decrypt_result = tokio::task::spawn_blocking(move || {
            encryption_service.decrypt_file_with_progress(
                &local_path_for_decrypt,
                &attempt_decrypted_path_for_decrypt,
                move |processed, total| {
                    let progress = (processed as f64 / total as f64) * 100.0;

                    // 🔥 进度回调内 Paused / epoch 短路：spawn_blocking 内部的 CPU 解密
                    //    无法被强制中断（依赖第三方解密库实现），但用户在解密期间暂停
                    //    文件夹（或快速恢复后启动新协程使本协程 epoch 失效）后，
                    //    我们应避免继续推送 DecryptProgress 事件误导前端进度条
                    //    （前端会持续看到 Paused 任务的进度仍在跳动；或新协程的进度
                    //     被旧协程的事件冲突覆盖）。
                    //    同样把 update_decrypt_progress 也跳过，避免 task 字段被无意义更新。
                    //
                    //    用 try_lock 而不是 lock：进度回调高频触发，避免阻塞解密计算。
                    //    try_lock 失败时保守地按"未暂停"处理，下一次回调通常能拿到锁纠正；
                    //    最坏情况是少数几次进度事件多发出去，不会破坏正确性。
                    if let Ok(mut task) = task_clone.try_lock() {
                        if task.status == crate::downloader::TaskStatus::Paused
                            || task.decrypt_epoch != my_epoch
                        {
                            // 已暂停或已过期：不更新进度、不发事件，让 CPU 计算无声地跑完
                            return;
                        }
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

        // 🔥 7.5 spawn_blocking 后的硬检查：删除加密文件 / 改 local_path / 持久化
        //         三大破坏性操作前的最后防线，同时校验 Paused 与 epoch。
        //
        //         CPU 解密已经把数据写到 attempt_decrypted_path（本次尝试独有的
        //         临时文件，不与其它协程或 final 路径冲突）。接下来的步骤会：
        //           - 把 attempt 文件 rename 到 final decrypted_path
        //           - 删除原加密文件 local_path（不可逆）
        //           - mark_decrypt_completed + 改 task.local_path = decrypted_path
        //           - 持久化 update_local_path 把记录指向 decrypted_path
        //         一旦执行，用户暂停后再恢复时，原加密文件已不存在、任务字段已被改写，
        //         无法回到"未解密下载完成"的可恢复状态。
        //
        //         **两种应该跳过这些破坏性操作的情形：**
        //
        //         1. status == Paused：用户暂停文件夹后任务仍保持暂停状态。
        //            常见路径：暂停后还没触发恢复就到达此检查点。
        //
        //         2. task.decrypt_epoch != my_epoch：本协程是过期协程。
        //            常见路径："暂停 → 恢复 → 新协程启动" 的 race——
        //            cancel_tasks_by_group 在 Decrypting → Paused 同锁内调用了
        //            invalidate_decrypt_epoch() 把 epoch +1；用户随后立即恢复时
        //            状态从 Paused 翻回 Pending → 新协程启动并捕获了新的 epoch；
        //            旧协程跑到这里，比对发现 my_epoch != task.decrypt_epoch，
        //            必须丢弃自己的解密结果，否则会污染新协程的状态机：
        //              - 删掉新协程依赖的加密文件
        //              - 改写新协程关心的 local_path
        //              - 把持久化记录改写成 decrypted_path（覆盖新协程的进度）
        //
        //         任一条件命中都执行：
        //           1. **只清理本次尝试独有的 attempt_decrypted_path**——绝不动
        //              共享的 final decrypted_path，否则会删掉新协程的输出文件
        //              （这是 R20 引入但 R21 修复的回归 bug）。
        //           2. 保持 local_path 仍指向加密文件（task 字段不动）
        //           3. return Ok(()) → handle_task_completion 入口看到同样的状态/epoch
        //              走 Stale 短路 return
        //         这样恢复时 status 仍是 Paused（或新协程已接手），local_path 仍是
        //         加密文件，可重新触发解密。新协程的 attempt 用的是不同 epoch，
        //         不会与旧协程的 attempt 冲突。
        {
            let t = task_info.task.lock().await;
            let is_paused = t.status == crate::downloader::TaskStatus::Paused;
            let is_stale = t.decrypt_epoch != my_epoch;
            let current_epoch = t.decrypt_epoch;
            drop(t);

            if is_paused || is_stale {
                if is_stale {
                    info!(
                        "任务 {} 解密 CPU 阶段完成但本协程 epoch={} 已过期（当前={}）；\
                         放弃后续破坏性操作，清理 attempt 文件 {:?}",
                        task_id, my_epoch, current_epoch, attempt_decrypted_path
                    );
                } else {
                    info!(
                        "任务 {} 解密 CPU 阶段完成但已被外部暂停，放弃后续删除/改路径/持久化操作，\
                         清理 attempt 文件 {:?} 并保留加密文件以备恢复",
                        task_id, attempt_decrypted_path
                    );
                }
                // 🔥 只删本次尝试独有的 attempt 文件，绝不删共享的 final decrypted_path
                if let Err(e) = tokio::fs::remove_file(&attempt_decrypted_path).await {
                    // 残留清理失败不致命：attempt 文件名带 epoch 后缀互相隔离，
                    // 不会影响其它协程；只在 debug 级别记录。下次系统重启或手动清理即可。
                    debug!(
                        "清理过期/暂停的 attempt 解密文件失败: {:?}, 路径: {:?}",
                        e, attempt_decrypted_path
                    );
                }
                return Ok(());
            }
        }

        // 🔥 7.7 R21+R24: 通过所有 stale check，把 attempt rename 到 final decrypted_path
        //
        // R21：rename 是单 fs 操作（同目录同盘），原子性较好。
        //
        // 🔥 R24: rename 之前主动尝试删除可能残留的旧 final（恢复路径自我修复）
        //
        //   场景：上一轮"暂停 → R23 stale 清理失败"留下的旧 final 文件。
        //   `tokio::fs::rename`（即 `std::fs::rename`）在 Windows 上对已存在
        //   目标文件的行为是 platform-dependent 的——较旧的 Rust 版本会直接
        //   失败（`ERROR_ALREADY_EXISTS`）。
        //
        //   主动删除 + rename 不是单一原子操作，但本轮的并发隔离已经由 R20
        //   epoch 机制 + R21 attempt-scoped 路径完整覆盖：
        //     - 物理上不可能有其他协程在"主动删 final → rename"的微秒级窗口
        //       内 rename 上来（详见 R23 安全性分析）。
        //     - 主动删除失败时（旧 final 不存在，最常见）忽略错误继续 rename。
        //     - 主动删除成功但旧 final 因 OS 占用持续无法删除时，rename 仍会
        //       失败 → handle_task_completion 走 Failed 分支，用户能看到错误。
        //
        //   这一步对正常路径（首次解密、final 不存在）无副作用：remove_file
        //   返回 NotFound，被 `let _ =` 静默忽略，rename 直接成功。
        let _ = tokio::fs::remove_file(&decrypted_path).await;
        if let Err(e) = tokio::fs::rename(&attempt_decrypted_path, &decrypted_path).await {
            // rename 失败时清理 attempt 残留（避免磁盘垃圾），返回错误让上层按解密失败处理
            let _ = tokio::fs::remove_file(&attempt_decrypted_path).await;
            return Err(anyhow::anyhow!(
                "rename 解密结果到最终路径失败: attempt={:?}, final={:?}, err={}",
                attempt_decrypted_path,
                decrypted_path,
                e
            ));
        }
        debug!(
            "已 rename 解密结果: {:?} → {:?}",
            attempt_decrypted_path, decrypted_path
        );

        // 🔥 7.8 R22: 锁内原子提交点——闭合 R21 注释里诚实标注的剩余窗口
        //
        // R21 之前到这里仍存在的时序窗口：rename 之后到删加密文件 / mark_decrypt_completed
        // / 改 local_path / 持久化之间，如果 cancel_tasks_by_group 翻 Paused +
        // invalidate_decrypt_epoch，剩下的破坏性操作没有同步 stale check 拦截，
        // 会发生"暂停期间 final 已就位但 task 字段未更新 + 加密文件被删"的不一致
        // transient state。
        //
        // R22 的协作机制：
        //
        //   1. 这里在同一锁段内做：
        //      - 最后一次 stale check（status=Paused 或 epoch≠my）
        //        → **R23**: stale 时**必须删 final**（详见下方安全性分析），
        //          return Ok(())
        //      - 通过时：
        //        - 置位 `task.decrypt_committed = true`（"这个任务已经过原子提交，
        //          后续锁外操作不可被暂停打断"的协作信号）
        //        - 调用 `mark_decrypt_completed` + 改 `task.local_path = decrypted_path`
        //          （这些动作原本在锁外的 step 9，现在合并入锁内提交点）
        //
        //   2. `cancel_tasks_by_group` 在锁内判定 active 时多看一项：
        //      `decrypt_committed=true` 时跳过翻 Paused / 跳过释放槽位 /
        //      跳过 invalidate_decrypt_epoch。让本协程走完后续锁外的删加密文件 +
        //      持久化 + handle_task_completion mark_completed。
        //
        // 关键：commit 锁段释放后 cancel 来 lock 任务，看到 decrypt_committed=true
        // 就**完全不动**任务状态，所以本协程后续无中断。即使 cancel 同时也释放了
        // owner=task_id 的 fallback 全局 fixed slot 是 idempotent 的（实际不会发生，
        // 因为 cancel 看到 committed 直接跳过）。
        //
        // 三种 race 边界完整闭合：
        //   a. cancel 在 7.8 锁段之前：committed 还是 false，cancel 正常翻 Paused +
        //      invalidate_decrypt_epoch；7.8 锁内看到 stale，return Ok()，删 final。
        //   b. cancel 在 7.8 锁段之后：committed 已是 true，cancel 跳过任务。
        //      本协程无中断完成 step 8 / 10 / 11。
        //   c. cancel 与 7.8 锁段竞争：锁是顺序化的，要么 a 要么 b。
        //
        // 🔥 R23: stale 分支为何必须删 final（推翻 R22 注释里的"不动 final"建议）
        //
        // 平台问题：`tokio::fs::rename` 调用 `std::fs::rename`，在 Windows 上对
        // 已存在目标文件的行为是 platform-dependent 的——较旧的 Rust 版本会直接
        // 失败（`ERROR_ALREADY_EXISTS`）。如果 stale 分支保留 final，下一次用户
        // 恢复任务时新协程仍生成同一 `decrypted_path`，最后跑到 7.7 rename 时会
        // 在 Windows 上稳定失败，整个解密流程卡死。
        //
        // 安全性分析（删 final 不会破坏其他协程）：
        // - 7.7 rename 已经把 attempt → final，本协程是这个 final 的拥有者。
        // - 其他协程要把它们自己的 attempt rename 到同一 final，必须先经过
        //   完整的 finalize_download_output → step 1-7 → spawn_blocking CPU 解密
        //   （秒级到分钟级）→ 7.5 stale check → 7.7 rename。
        // - 而本协程从 7.7 rename 完成到 7.8 lock 是**微秒级**窗口（rename 是单
        //   系统调用，lock 是 tokio Mutex 异步原语）。
        // - 物理上不可能有其他协程在这微秒级窗口里完成自己的完整流程并 rename
        //   覆盖 final——除非新协程在 cancel 之前就已经存在并跑到 7.7，但
        //   "新协程"的定义就是 cancel 之后才 spawn 的，时序上不可能。
        // - 所以 7.8 stale 分支删 final = 删自己刚 rename 上去的文件，与其他
        //   协程隔离。
        //
        // 这与 R21 的 7.5 stale 分支（"不删共享 final"）是不同语境：7.5 阶段
        // 本协程还没 rename，final 是别人留的，删别人的会破坏并发；7.8 阶段
        // final 是本协程刚放的，删自己的是清理职责。
        let original_size_for_completion = original_size;
        {
            let mut task = task_info.task.lock().await;
            let is_paused = task.status == crate::downloader::TaskStatus::Paused;
            let is_stale = task.decrypt_epoch != my_epoch;
            let current_epoch = task.decrypt_epoch;

            if is_paused || is_stale {
                drop(task);
                if is_stale {
                    info!(
                        "任务 {} rename 后发现本协程 epoch={} 已过期（当前={}）；\
                         删除本协程刚 rename 上去的 final {:?} 让恢复路径清洁",
                        task_id, my_epoch, current_epoch, decrypted_path
                    );
                } else {
                    info!(
                        "任务 {} rename 后发现已被外部暂停；\
                         删除本协程刚 rename 上去的 final {:?} 让恢复时新协程能 rename 成功",
                        task_id, decrypted_path
                    );
                }
                // 🔥 R23: 删本协程刚 rename 上去的 final，避免下次恢复时
                //         新协程的 7.7 rename 在 Windows 上失败。
                //         加密文件未删（step 8 还没执行），用户恢复后新协程会
                //         重新解密 → 新 attempt → rename 到不存在的 final → 成功。
                //
                // 🔥 R24: 删除失败提升为 warn 级别（不再是 debug 静默吞掉）
                //
                // 删除失败可能场景：杀毒软件扫描占用 / Windows Search 索引锁 /
                // 用户在文件管理器里预览 / 其它进程持有句柄。在这些情况下：
                //   - Unix：下次 rename 会原子覆盖（rename(2) 即使目标存在也能替换）
                //   - Windows：下次 rename 可能仍因 final 存在而失败
                //
                // 配套防线（R24）：新协程 7.7 rename 之前会主动尝试删除可能残留
                // 的旧 final 做"恢复路径自我修复"。即使本次 stale 清理失败，
                // 只要恢复时 OS 占用已释放，自我修复仍能让 rename 成功。
                //
                // 这里用 warn 而非 error/Failed：
                //   - 不直接让任务进 Failed（用户体感"暂停操作让任务失败"反直觉）
                //   - 但日志可见，运维能在监控里发现"暂停后清理 final 失败"的模式
                //   - 真正不可恢复时由 R24 自我修复路径或 handle_task_completion 走 Failed
                if let Err(e) = tokio::fs::remove_file(&decrypted_path).await {
                    warn!(
                        "stale 分支删除 final 失败：{:?}（路径: {:?}）。\
                         残留 final 可能导致 Windows 下次恢复时 rename 失败；\
                         R24 在新协程 rename 之前会主动尝试再次删除做自我修复。",
                        e, decrypted_path
                    );
                }
                return Ok(());
            }

            // 通过：锁内置位提交标志 + 同步完成原 step 9 的字段更新
            task.decrypt_committed = true;
            task.mark_decrypt_completed(decrypted_path.clone(), original_size_for_completion);
            task.local_path = decrypted_path.clone();
        }
        debug!(
            "任务 {} 解密原子提交完成（decrypt_committed=true），后续锁外步骤无中断",
            task_id
        );

        // 8. 删除加密文件
        //    锁外执行：cancel_tasks_by_group 看到 decrypt_committed=true 已跳过任务，
        //    所以这一步无中断。即使理论上有第三方路径（非 cancel）干扰，remove_file
        //    失败也只 warn 不返回 Err，task 字段已经在 7.8 锁内提交。
        if let Err(e) = tokio::fs::remove_file(&local_path).await {
            warn!("删除加密文件失败: {:?}, 路径: {:?}", e, local_path);
        } else {
            debug!("已删除加密文件: {:?}", local_path);
        }

        // 10. 🔥 更新持久化文件中的本地路径（解密后的路径）
        if let Some(ref pm) = task_info.persistence_manager {
            if let Err(e) = pm
                .lock()
                .await
                .update_local_path(&task_id, decrypted_path.clone())
            {
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

        info!(
            "✅ 任务 {} 解密完成，原始大小: {} bytes",
            task_id, original_size
        );

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
            let uuid =
                EncryptionService::extract_uuid_from_encrypted_name(filename).unwrap_or("unknown");
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
    /// ⚠️ 修复问题5：使用任务状态判断，而非 progress > 0
    ///    - 原逻辑：progress > 0 才纳入检测，导致一开始就卡住的任务无法触发 CDN 刷新
    ///    - 新逻辑：状态为 Downloading 就纳入检测，即使 progress = 0
    ///
    /// # 返回
    /// 只包含有效任务的速度列表（状态为 Downloading 且未完成的任务）
    pub async fn get_valid_task_speed_values(&self) -> Vec<u64> {
        let tasks = self.active_tasks.read().await;
        let mut speeds = Vec::new();

        for task_info in tasks.values() {
            // 获取任务状态和进度
            let (status, progress_bytes, total_bytes) = {
                let task = task_info.task.lock().await;
                (
                    task.status.clone(),
                    task.downloaded_size,
                    task_info.total_size,
                )
            };

            // 过滤：只包含正在下载且未完成的任务
            // status == Downloading: 任务正在下载中（包括 progress = 0 的情况）
            // progress < total: 尚未完成
            if status == crate::downloader::TaskStatus::Downloading && progress_bytes < total_bytes
            {
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
