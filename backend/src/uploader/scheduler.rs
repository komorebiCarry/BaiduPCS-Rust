// 上传分片调度器
//
//  实现：全局上传调度器
//
// 功能：
// - Round-Robin 公平调度多个上传任务
// - 全局并发控制（限制同时上传的分片数）
// - 任务级并发控制（根据文件大小自动计算）
// - 槽位池管理线程ID（日志追踪）
// - 检测任务数变化，重置服务器速度窗口
// - 🔥 任务槽位机制由 UploadManager 的 TaskSlotPool 管理

use crate::autobackup::events::{BackupTransferNotification, TransferTaskType};
use crate::encryption::SnapshotManager;
use crate::netdisk::{NetdiskClient, UploadErrorKind};
use crate::persistence::PersistenceManager;
use crate::task_slot_pool::TaskSlotPool;
use crate::server::events::{ProgressThrottler, TaskEvent, UploadEvent};
use crate::server::websocket::WebSocketManager;
use crate::uploader::{PcsServerHealthManager, UploadChunk, UploadChunkManager, UploadTask};
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

    // 🔥 持久化支持
    /// 持久化管理器引用（可选）
    pub persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,

    // 🔥 WebSocket 管理器支持
    /// WebSocket 管理器引用
    pub ws_manager: Option<Arc<WebSocketManager>>,

    // 🔥 进度事件节流器（200ms 间隔，避免事件风暴）
    /// 任务级进度节流器，多个分片共享
    pub progress_throttler: Arc<ProgressThrottler>,

    // 🔥 备份任务统一通知发送器（进度、状态、完成、失败等）
    /// 备份任务统一通知发送器（可选，仅备份任务需要）
    pub backup_notification_tx: Option<mpsc::UnboundedSender<BackupTransferNotification>>,

    // 🔥 任务槽池引用（用于任务完成/失败时释放槽位）
    /// 任务槽池引用（可选，由 UploadManager 传入）
    pub task_slot_pool: Option<Arc<TaskSlotPool>>,

    // 🔥 槽位刷新节流器（30秒间隔，防止槽位超时释放）
    /// 槽位刷新节流器（可选，仅当 task_slot_pool 存在时创建）
    pub slot_touch_throttler: Option<Arc<crate::task_slot_pool::SlotTouchThrottler>>,

    // 🔥 加密快照管理器（用于保存加密映射到 encryption_snapshots 表）
    /// 快照管理器引用（可选，仅加密上传任务需要）
    pub snapshot_manager: Option<Arc<SnapshotManager>>,

    // 🔥 Manager 任务列表引用（用于任务完成时立即清理，避免内存泄漏）
    /// UploadManager.tasks 的引用，任务完成后从中移除
    pub manager_tasks: Option<Arc<dashmap::DashMap<String, crate::uploader::UploadTaskInfo>>>,
}

// =====================================================
// 加密映射更新辅助函数
// =====================================================

/// 更新加密映射到 encryption_snapshots 表
///
/// 在任务完成时调用，更新 nonce、algorithm、version 并标记为 completed
/// 此函数被调度循环触发和回调触发两处共用
async fn update_encryption_mapping(
    task_id: &str,
    task_info: &UploadTaskScheduleInfo,
    is_backup: bool,
) {
    // 获取加密信息
    let encryption_info = {
        let t = task_info.task.lock().await;
        if t.encrypt_enabled {
            Some((
                t.remote_path.clone(),
                t.encrypted_name.clone(),
                t.encryption_nonce.clone(),
                t.encryption_algorithm.clone(),
                t.encryption_version,
            ))
        } else {
            None
        }
    };

    // 更新加密映射到 encryption_snapshots 表（所有加密任务，包括备份任务）
    // 注意：snapshot 在 create_task 时已创建（状态为 pending），这里只更新 nonce、algorithm 并标记为 completed
    // 每个文件有独立的 nonce，需要在单个文件上传完成时立即更新
    if let Some((remote_path, encrypted_name, nonce, algorithm, version)) = encryption_info {
        if let (Some(_enc_name), Some(enc_nonce), Some(enc_algo)) = (encrypted_name, nonce, algorithm) {
            if let Some(ref snapshot_manager) = task_info.snapshot_manager {
                // 从 remote_path 提取实际的加密文件名（网盘上的真实文件名）
                // remote_path 格式如: /13/上传/BPR_BKUP_xxx.bkup
                let actual_encrypted_name = std::path::Path::new(&remote_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                // 更新已有的 snapshot（在 create_task 时创建的）
                // 更新 nonce、algorithm、version 并标记为 completed
                match snapshot_manager.update_encryption_metadata(
                    &actual_encrypted_name,
                    &enc_nonce,
                    &enc_algo,
                    version as i32,
                ) {
                    Ok(true) => {
                        info!("上传任务 {} 加密映射已更新: {} (is_backup={})", task_id, actual_encrypted_name, is_backup);
                    }
                    Ok(false) => {
                        warn!("上传任务 {} 未找到对应的加密映射记录: {} (is_backup={})", task_id, actual_encrypted_name, is_backup);
                    }
                    Err(e) => {
                        error!("更新加密映射失败: task_id={}, is_backup={}, error={}", task_id, is_backup, e);
                    }
                }
            } else {
                warn!("上传任务 {} 启用了加密但未设置 SnapshotManager，无法更新加密映射 (is_backup={})", task_id, is_backup);
            }
        }
    }
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
    /// 任务完成通知发送器（用于通知文件夹上传管理器补充任务）
    task_completed_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
    /// 🔥 备份任务统一通知发送器（用于通知 AutoBackupManager 所有事件）
    /// 包括：进度更新、状态变更、任务完成、任务失败等
    backup_notification_tx: Arc<RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>>,
    /// 上一轮的任务数（用于检测任务数变化）
    last_task_count: Arc<AtomicUsize>,
}

impl UploadChunkScheduler {
    /// 创建新的调度器（使用默认重试次数）
    pub fn new(max_global_threads: usize, max_concurrent_tasks: usize) -> Self {
        Self::new_with_config(
            max_global_threads,
            max_concurrent_tasks,
            DEFAULT_MAX_RETRIES,
        )
    }

    /// 创建新的调度器（完整配置）
    pub fn new_with_config(
        max_global_threads: usize,
        max_concurrent_tasks: usize,
        max_retries: u32,
    ) -> Self {
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
            task_completed_tx: Arc::new(RwLock::new(None)),
            backup_notification_tx: Arc::new(RwLock::new(None)),
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

    /// 🔥 设置备份任务统一通知发送器
    ///
    /// AutoBackupManager 调用此方法设置 channel sender，
    /// 所有备份相关事件（进度、状态、完成、失败等）都通过此 channel 发送
    pub async fn set_backup_notification_sender(&self, tx: mpsc::UnboundedSender<BackupTransferNotification>) {
        let mut sender = self.backup_notification_tx.write().await;
        *sender = Some(tx);
        info!("备份上传任务统一通知 channel 已设置");
    }

    /// 动态更新最大全局线程数
    pub fn update_max_threads(&self, new_max: usize) {
        let old_max = self.max_global_threads.swap(new_max, Ordering::SeqCst);
        info!("🔧 动态调整上传全局最大线程数: {} -> {}", old_max, new_max);
    }

    /// 动态更新最大并发任务数
    pub fn update_max_concurrent_tasks(&self, new_max: usize) {
        let old_max = self.max_concurrent_tasks.swap(new_max, Ordering::SeqCst);
        info!("🔧 动态调整上传最大并发任务数: {} -> {}", old_max, new_max);
    }

    /// 动态更新最大重试次数
    pub fn update_max_retries(&self, new_max: u32) {
        let old_max = self.max_retries.swap(new_max as usize, Ordering::SeqCst);
        info!("🔧 动态调整上传最大重试次数: {} -> {}", old_max, new_max);
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

    /// 注册任务到调度器
    pub async fn register_task(&self, mut task_info: UploadTaskScheduleInfo) -> Result<()> {
        let task_id = task_info.task_id.clone();

        // 🔥 如果是备份任务，注入调度器的 backup_notification_tx
        {
            let t = task_info.task.lock().await;
            if t.is_backup {
                let notification_tx = self.backup_notification_tx.read().await.clone();
                if notification_tx.is_some() {
                    task_info.backup_notification_tx = notification_tx;
                    info!("备份上传任务 {} 已注入统一通知 sender", task_id);
                }
            }
        }

        // 添加到活跃任务列表
        self.active_tasks
            .write()
            .await
            .insert(task_id.clone(), task_info);

        info!("上传任务 {} 已注册到调度器", task_id);
        Ok(())
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) {
        if let Some(task_info) = self.active_tasks.write().await.remove(task_id) {
            task_info.cancellation_token.cancel();
            info!("上传任务 {} 已从调度器移除并取消", task_id);
        }
    }

    /// 获取活跃任务数量
    pub async fn active_task_count(&self) -> usize {
        self.active_tasks.read().await.len()
    }

    /// 启动全局调度循环
    fn start_scheduling(&self) {
        let active_tasks = self.active_tasks.clone();
        let max_global_threads = self.max_global_threads.clone();
        let active_chunk_count = self.active_chunk_count.clone();
        let slot_pool = self.slot_pool.clone();
        let scheduler_running = self.scheduler_running.clone();
        let task_completed_tx = self.task_completed_tx.clone();
        let backup_notification_tx = self.backup_notification_tx.clone();
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
                                backup_notification_tx.clone(),
                                max_retries.clone(),
                            );

                            consecutive_empty_rounds = 0;
                        }
                        None => {
                            // 该任务没有待上传的分片
                            if task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                                // 所有分片完成，尝试调用 create_file 合并分片
                                // 使用 compare_exchange 确保只有一处能执行合并
                                if task_info
                                    .is_merging
                                    .compare_exchange(
                                        false,
                                        true,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .is_ok()
                                {
                                    info!(
                                        "上传任务 {} 所有分片完成，开始合并分片 (调度循环触发)",
                                        task_id
                                    );

                                    let create_result = task_info
                                        .client
                                        .create_file(
                                            &task_info.remote_path,
                                            &task_info.block_list,
                                            &task_info.upload_id,
                                            task_info.total_size,
                                            "0",
                                        )
                                        .await;

                                    active_tasks.write().await.remove(task_id);

                                    // 🔥 释放槽位（任务完成或失败都需要释放）
                                    if let Some(ref pool) = task_info.task_slot_pool {
                                        pool.release_fixed_slot(task_id).await;
                                        info!("上传任务 {} 调度器合并完成，释放槽位", task_id);
                                    }

                                    match create_result {
                                        Ok(response) => {
                                            if response.is_success() {
                                                info!(
                                                    "上传任务 {} 合并分片成功，从调度器移除",
                                                    task_id
                                                );

                                                // 🔥 清理持久化文件（任务完成）
                                                if let Some(ref pm) = task_info.persistence_manager
                                                {
                                                    if let Err(e) =
                                                        pm.lock().await.on_task_completed(task_id)
                                                    {
                                                        error!("清理上传任务持久化文件失败: {}", e);
                                                    } else {
                                                        debug!(
                                                            "上传任务 {} 持久化文件已清理",
                                                            task_id
                                                        );
                                                    }
                                                }

                                                // 🔥 从 UploadManager.tasks 中移除任务（立即清理，避免内存泄漏）
                                                if let Some(ref manager_tasks) = task_info.manager_tasks {
                                                    manager_tasks.remove(task_id);
                                                    debug!("上传任务 {} 已从 UploadManager.tasks 中移除", task_id);
                                                }

                                                // 标记任务完成
                                                let (group_id, encrypted_temp_path, is_backup) = {
                                                    let mut t = task_info.task.lock().await;
                                                    t.mark_completed();
                                                    (t.group_id.clone(), t.encrypted_temp_path.clone(), t.is_backup)
                                                };

                                                // 🔥 更新加密映射（调度循环触发）
                                                update_encryption_mapping(task_id, &task_info, is_backup).await;

                                                // 🔥 清理临时加密文件（如果存在）
                                                if let Some(temp_path) = encrypted_temp_path {
                                                    if temp_path.exists() {
                                                        match tokio::fs::remove_file(&temp_path).await {
                                                            Ok(_) => {
                                                                info!("上传任务 {} 临时加密文件已清理: {:?}", task_id, temp_path);
                                                            }
                                                            Err(e) => {
                                                                warn!("上传任务 {} 清理临时加密文件失败: {:?}, 错误: {}", task_id, temp_path, e);
                                                            }
                                                        }
                                                    }
                                                }

                                                // 如果是文件夹子任务，通知补充新任务
                                                if let Some(gid) = group_id {
                                                    let tx_guard = task_completed_tx.read().await;
                                                    if let Some(tx) = tx_guard.as_ref() {
                                                        if let Err(e) = tx.send(gid.clone()) {
                                                            error!(
                                                                "发送上传任务完成通知失败: {}",
                                                                e
                                                            );
                                                        } else {
                                                            debug!("已发送上传任务完成通知: group_id={}", gid);
                                                        }
                                                    }
                                                }

                                                // 🔥 如果是备份任务，通知 AutoBackupManager
                                                if is_backup {
                                                    let tx_guard = backup_notification_tx.read().await;
                                                    if let Some(tx) = tx_guard.as_ref() {
                                                        let notification = BackupTransferNotification::Completed {
                                                            task_id: task_id.to_string(),
                                                            task_type: TransferTaskType::Upload,
                                                        };
                                                        if let Err(e) = tx.send(notification) {
                                                            error!("发送备份上传任务完成通知失败: {}", e);
                                                        } else {
                                                            debug!("已发送备份上传任务完成通知: task_id={}", task_id);
                                                        }
                                                    }
                                                }
                                            } else {
                                                let err_msg = format!(
                                                    "合并分片失败: errno={}, errmsg={}",
                                                    response.errno, response.errmsg
                                                );
                                                error!("上传任务 {} {}", task_id, err_msg);

                                                // 🔥 如果是备份任务，通知失败
                                                let is_backup = {
                                                    let mut t = task_info.task.lock().await;
                                                    t.mark_failed(err_msg.clone());
                                                    t.is_backup
                                                };
                                                if is_backup {
                                                    let tx_guard = backup_notification_tx.read().await;
                                                    if let Some(tx) = tx_guard.as_ref() {
                                                        let notification = BackupTransferNotification::Failed {
                                                            task_id: task_id.to_string(),
                                                            task_type: TransferTaskType::Upload,
                                                            error_message: err_msg.clone(),
                                                        };
                                                        let _ = tx.send(notification);
                                                    }
                                                }

                                                // 🔥 更新持久化错误信息
                                                if let Some(ref pm) = task_info.persistence_manager {
                                                    if let Err(e) = pm.lock().await.update_task_error(task_id, err_msg) {
                                                        warn!("更新上传任务错误信息失败: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let err_msg = format!("调用 create_file 失败: {}", e);
                                            error!("上传任务 {} {}", task_id, err_msg);

                                            // 🔥 如果是备份任务，通知失败
                                            let is_backup = {
                                                let mut t = task_info.task.lock().await;
                                                t.mark_failed(err_msg.clone());
                                                t.is_backup
                                            };
                                            if is_backup {
                                                let tx_guard = backup_notification_tx.read().await;
                                                if let Some(tx) = tx_guard.as_ref() {
                                                    let notification = BackupTransferNotification::Failed {
                                                        task_id: task_id.to_string(),
                                                        task_type: TransferTaskType::Upload,
                                                        error_message: err_msg.clone(),
                                                    };
                                                    let _ = tx.send(notification);
                                                }
                                            }

                                            // 🔥 更新持久化错误信息
                                            if let Some(ref pm) = task_info.persistence_manager {
                                                if let Err(e) = pm.lock().await.update_task_error(task_id, err_msg) {
                                                    warn!("更新上传任务错误信息失败: {}", e);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    debug!("上传任务 {} 合并分片已由其他位置触发，跳过", task_id);
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
        backup_notification_tx: Arc<RwLock<Option<mpsc::UnboundedSender<BackupTransferNotification>>>>,
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
                    info!(
                        "[上传线程{}] 分片 #{} 因任务取消而失败",
                        slot_id, chunk_index
                    );
                } else {
                    error!(
                        "[上传线程{}] 分片 #{} 上传失败: {}",
                        slot_id, chunk_index, e
                    );

                    // 取消上传标记 + 递增分片调度级重试计数
                    let chunk_retries = {
                        let mut manager = task_info.chunk_manager.lock().await;
                        if let Some(c) = manager.chunks_mut().get_mut(chunk_index) {
                            c.uploading = false;
                        }
                        manager.increment_retry(chunk_index)
                    };

                    // 外层调度级重试上限 = 内层重试 * 2
                    let max_schedule_retries = max_retries.load(Ordering::SeqCst) as u32 * 2;

                    if chunk_retries < max_schedule_retries {
                        // 分片还有重试机会，留在任务中等待调度器下一轮重新调度
                        warn!(
                            "[上传线程{}] 分片 #{} 第 {}/{} 次调度失败，等待重新调度: {}",
                            slot_id, chunk_index, chunk_retries, max_schedule_retries, e
                        );
                    } else {
                        // 重试耗尽，杀掉整个任务
                        let error_msg = e.to_string();
                        let is_backup = {
                            let mut t = task_info.task.lock().await;
                            t.mark_failed(error_msg.clone());
                            t.is_backup
                        };

                        if !is_backup {
                            if let Some(ref ws_manager) = task_info.ws_manager {
                                ws_manager.send_if_subscribed(
                                    TaskEvent::Upload(UploadEvent::Failed {
                                        task_id: task_id.clone(),
                                        error: error_msg.clone(),
                                        is_backup,
                                    }),
                                    None,
                                );
                            }
                        }

                        if is_backup {
                            let tx_guard = backup_notification_tx.read().await;
                            if let Some(tx) = tx_guard.as_ref() {
                                let notification = BackupTransferNotification::Failed {
                                    task_id: task_id.clone(),
                                    task_type: TransferTaskType::Upload,
                                    error_message: error_msg.clone(),
                                };
                                let _ = tx.send(notification);
                            }
                        }

                        if let Some(ref pm) = task_info.persistence_manager {
                            if let Err(e) = pm.lock().await.update_task_error(&task_id, error_msg) {
                                warn!("更新上传任务错误信息失败: {}", e);
                            }
                        }

                        active_tasks.write().await.remove(&task_id);

                        if let Some(ref pool) = task_info.task_slot_pool {
                            pool.release_fixed_slot(&task_id).await;
                            info!("上传任务 {} 分片上传失败，释放槽位", task_id);
                        }

                        error!("上传任务 {} 因分片 #{} 重试耗尽已从调度器移除", task_id, chunk_index);
                    }
                }
            } else {
                // 检查是否所有分片都完成
                let all_completed = {
                    let manager = task_info.chunk_manager.lock().await;
                    manager.is_completed()
                };

                if all_completed && task_info.active_chunk_count.load(Ordering::SeqCst) == 0 {
                    // 使用 compare_exchange 确保只有一处能执行合并
                    if task_info
                        .is_merging
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        info!(
                            "上传任务 {} 所有分片上传完成，开始合并分片 (回调触发)",
                            task_id
                        );

                        // 调用 create_file 合并分片
                        let create_result = task_info
                            .client
                            .create_file(
                                &task_info.remote_path,
                                &task_info.block_list,
                                &task_info.upload_id,
                                task_info.total_size,
                                "0",
                            )
                            .await;

                        // 从调度器移除
                        active_tasks.write().await.remove(&task_id);

                        // 🔥 释放槽位（任务完成或失败都需要释放）
                        if let Some(ref pool) = task_info.task_slot_pool {
                            pool.release_fixed_slot(&task_id).await;
                            info!("上传任务 {} 回调合并完成，释放槽位", task_id);
                        }

                        match create_result {
                            Ok(response) => {
                                if response.is_success() {
                                    info!("上传任务 {} 合并分片成功，文件创建完成", task_id);

                                    // 🔥 清理持久化文件（任务完成）
                                    if let Some(ref pm) = task_info.persistence_manager {
                                        if let Err(e) = pm.lock().await.on_task_completed(&task_id)
                                        {
                                            error!("清理上传任务持久化文件失败: {}", e);
                                        } else {
                                            debug!("上传任务 {} 持久化文件已清理", task_id);
                                        }
                                    }

                                    // 🔥 从 UploadManager.tasks 中移除任务（立即清理，避免内存泄漏）
                                    if let Some(ref manager_tasks) = task_info.manager_tasks {
                                        manager_tasks.remove(&task_id);
                                        debug!("上传任务 {} 已从 UploadManager.tasks 中移除", task_id);
                                    }

                                    // 标记完成并获取信息
                                    let (group_id, is_backup, encrypted_temp_path) = {
                                        let mut t = task_info.task.lock().await;
                                        t.mark_completed();
                                        (t.group_id.clone(), t.is_backup, t.encrypted_temp_path.clone())
                                    };

                                    // 🔥 更新加密映射（回调触发）
                                    update_encryption_mapping(&task_id, &task_info, is_backup).await;

                                    // 🔥 清理临时加密文件（如果存在）
                                    if let Some(temp_path) = encrypted_temp_path {
                                        if temp_path.exists() {
                                            match tokio::fs::remove_file(&temp_path).await {
                                                Ok(_) => {
                                                    info!("上传任务 {} 临时加密文件已清理: {:?}", task_id, temp_path);
                                                }
                                                Err(e) => {
                                                    warn!("上传任务 {} 清理临时加密文件失败: {:?}, 错误: {}", task_id, temp_path, e);
                                                }
                                            }
                                        }
                                    }

                                    // 🔥 发布任务完成事件（备份任务不发送，由 AutoBackupManager 统一处理）
                                    if !is_backup {
                                        if let Some(ref ws_manager) = task_info.ws_manager {
                                            ws_manager.send_if_subscribed(
                                                TaskEvent::Upload(UploadEvent::Completed {
                                                    task_id: task_id.clone(),
                                                    completed_at: chrono::Utc::now().timestamp_millis(),
                                                    is_rapid_upload: false,
                                                    is_backup,
                                                }),
                                                None,
                                            );
                                        }
                                    }

                                    if let Some(gid) = group_id {
                                        let tx_guard = task_completed_tx.read().await;
                                        if let Some(tx) = tx_guard.as_ref() {
                                            let _ = tx.send(gid);
                                        }
                                    }

                                    // 🔥 如果是备份任务，通知 AutoBackupManager
                                    if is_backup {
                                        let tx_guard = backup_notification_tx.read().await;
                                        if let Some(tx) = tx_guard.as_ref() {
                                            let notification = BackupTransferNotification::Completed {
                                                task_id: task_id.clone(),
                                                task_type: TransferTaskType::Upload,
                                            };
                                            let _ = tx.send(notification);
                                        }
                                    }
                                } else {
                                    let err_msg = format!(
                                        "合并分片失败: errno={}, errmsg={}",
                                        response.errno, response.errmsg
                                    );
                                    error!("上传任务 {} {}", task_id, err_msg);

                                    let is_backup = {
                                        let mut t = task_info.task.lock().await;
                                        t.mark_failed(err_msg.clone());
                                        t.is_backup
                                    };

                                    // 🔥 发布任务失败事件（备份任务不发送，由 AutoBackupManager 统一处理）
                                    if !is_backup {
                                        if let Some(ref ws_manager) = task_info.ws_manager {
                                            ws_manager.send_if_subscribed(
                                                TaskEvent::Upload(UploadEvent::Failed {
                                                    task_id: task_id.clone(),
                                                    error: err_msg.clone(),
                                                    is_backup,
                                                }),
                                                None,
                                            );
                                        }
                                    }

                                    // 🔥 如果是备份任务，通知 AutoBackupManager
                                    if is_backup {
                                        let tx_guard = backup_notification_tx.read().await;
                                        if let Some(tx) = tx_guard.as_ref() {
                                            let notification = BackupTransferNotification::Failed {
                                                task_id: task_id.clone(),
                                                task_type: TransferTaskType::Upload,
                                                error_message: err_msg.clone(),
                                            };
                                            let _ = tx.send(notification);
                                        }
                                    }

                                    // 🔥 更新持久化错误信息
                                    if let Some(ref pm) = task_info.persistence_manager {
                                        if let Err(e) = pm.lock().await.update_task_error(&task_id, err_msg) {
                                            warn!("更新上传任务错误信息失败: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("调用 create_file 失败: {}", e);
                                error!("上传任务 {} {}", task_id, err_msg);

                                let is_backup = {
                                    let mut t = task_info.task.lock().await;
                                    t.mark_failed(err_msg.clone());
                                    t.is_backup
                                };

                                // 🔥 发布任务失败事件
                                if !is_backup {
                                    if let Some(ref ws_manager) = task_info.ws_manager {
                                        ws_manager.send_if_subscribed(
                                            TaskEvent::Upload(UploadEvent::Failed {
                                                task_id: task_id.clone(),
                                                error: err_msg.clone(),
                                                is_backup,
                                            }),
                                            None,
                                        );
                                    }
                                }

                                // 🔥 如果是备份任务，通知 AutoBackupManager
                                if is_backup {
                                    let tx_guard = backup_notification_tx.read().await;
                                    if let Some(tx) = tx_guard.as_ref() {
                                        let notification = BackupTransferNotification::Failed {
                                            task_id: task_id.clone(),
                                            task_type: TransferTaskType::Upload,
                                            error_message: err_msg.clone(),
                                        };
                                        let _ = tx.send(notification);
                                    }
                                }

                                // 🔥 更新持久化错误信息
                                if let Some(ref pm) = task_info.persistence_manager {
                                    if let Err(e) = pm.lock().await.update_task_error(&task_id, err_msg) {
                                        warn!("更新上传任务错误信息失败: {}", e);
                                    }
                                }
                            }
                        }
                    } else {
                        debug!("上传任务 {} 合并分片已由其他位置触发，跳过 (回调)", task_id);
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
            slot_id,
            chunk.index,
            chunk.range.start,
            chunk.range.end - 1,
            chunk_size
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
                        task_info
                            .server_health
                            .record_chunk_speed(&server, chunk_size, elapsed_ms);
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

                    // 🔥 持久化回调：记录分片完成（带 MD5）
                    if let Some(ref pm) = task_info.persistence_manager {
                        pm.lock().await.on_chunk_completed_with_md5(
                            &task_info.task_id,
                            chunk.index,
                            response.md5.clone(),
                        );
                        debug!(
                            "[上传线程{}] 分片 #{} 已记录到持久化管理器",
                            slot_id, chunk.index
                        );
                    }

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

                        // 🔥 刷新槽位时间戳（带节流，防止槽位超时释放）
                        if let Some(ref throttler) = task_info.slot_touch_throttler {
                            throttler.try_touch().await;
                        }

                        // 🔥 发布带节流的进度事件（每 200ms 最多发布一次）
                        if let Some(ref ws_manager) = task_info.ws_manager {
                            // 使用节流器控制发布频率
                            let should_emit = task_info.progress_throttler.should_emit();

                            if should_emit {
                                let total_size = task_info.total_size;
                                let progress = if total_size > 0 {
                                    ( t.uploaded_size  as f64 / total_size as f64) * 100.0
                                } else {
                                    0.0
                                };

                                let (completed_chunks, total_chunks) = {
                                    let manager = task_info.chunk_manager.lock().await;
                                    (manager.completed_count(), manager.chunk_count())
                                };
                                if !t.is_backup {
                                    ws_manager.send_if_subscribed(
                                        TaskEvent::Upload(UploadEvent::Progress {
                                            task_id: t.id.clone(),
                                            uploaded_size: t.uploaded_size,
                                            total_size,
                                            speed,
                                            progress,
                                            completed_chunks,
                                            total_chunks,
                                            is_backup: t.is_backup,
                                        }),
                                        None,
                                    );
                                }

                                // 🔥 如果是备份任务，发送进度通知到 AutoBackupManager
                                if t.is_backup {
                                    if let Some(ref tx) = task_info.backup_notification_tx {
                                        let notification = BackupTransferNotification::Progress {
                                            task_id: t.id.clone(),
                                            task_type: TransferTaskType::Upload,
                                            transferred_bytes: t.uploaded_size,
                                            total_bytes: total_size,
                                        };
                                        let _ = tx.send(notification);
                                    }
                                }
                            }
                        }

                    }

                    info!(
                        "[上传线程{}] ✓ 分片 #{} 上传成功 ({}/{} 完成, 速度: {} KB/s)",
                        slot_id,
                        chunk.index,
                        completed_chunks,
                        total_chunks,
                        speed / 1024
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
                            slot_id,
                            chunk.index,
                            backoff_ms,
                            retry + 1,
                            max_retries,
                            e
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
        assert_eq!(
            calculate_backoff_delay(0, &UploadErrorKind::RateLimited),
            10000
        );
    }

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = UploadChunkScheduler::new(10, 3);

        assert_eq!(scheduler.max_threads(), 10);
        assert_eq!(scheduler.active_threads(), 0);
        assert_eq!(scheduler.active_task_count().await, 0);
    }
}
