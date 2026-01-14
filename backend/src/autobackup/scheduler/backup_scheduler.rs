//! 备份调度器
//!
//! 实现三阶段执行模型：
//! - 阶段 1：逻辑准备阶段（无副作用，不占槽位）
//! - 阶段 2：资源准备阶段（产生副作用，不可中断，不占槽位）
//! - 阶段 3：上传与提交阶段（占用槽位，可被抢占）

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::autobackup::config::BackupConfig;
use crate::encryption::EncryptionService;
use crate::autobackup::priority::{PrepareResourcePool, Priority, PriorityContext, SlotManager};
use crate::autobackup::record::BackupRecordManager;
use crate::autobackup::task::BackupFileTask;

/// 文件任务执行上下文
#[derive(Debug, Clone)]
pub struct FileTaskContext {
    /// 文件任务 ID
    pub file_task_id: String,
    /// 备份任务 ID
    pub backup_task_id: String,
    /// 配置 ID
    pub config_id: String,
    /// 本地文件路径
    pub local_path: PathBuf,
    /// 远程路径
    pub remote_path: String,
    /// 文件大小
    pub file_size: u64,
    /// 是否启用加密
    pub encrypt_enabled: bool,
    /// 加密后的临时文件路径（如果启用加密）
    pub encrypted_temp_path: Option<PathBuf>,
    /// 加密后的文件名（如果启用加密）
    pub encrypted_filename: Option<String>,
}

/// 调度器事件
#[derive(Debug)]
pub enum SchedulerEvent {
    /// 文件任务准备完成，等待上传
    FileReady(FileTaskContext),
    /// 文件任务上传完成
    FileCompleted {
        file_task_id: String,
        backup_task_id: String,
    },
    /// 文件任务失败
    FileFailed {
        file_task_id: String,
        backup_task_id: String,
        error: String,
        retryable: bool,
    },
    /// 备份任务完成
    TaskCompleted { backup_task_id: String },
    /// 备份任务失败
    TaskFailed {
        backup_task_id: String,
        error: String,
    },
}

/// 备份调度器
///
/// 负责协调备份任务的三阶段执行：
/// 1. 逻辑准备阶段：扫描、过滤、去重
/// 2. 资源准备阶段：写快照、加密
/// 3. 上传提交阶段：上传、写记录
pub struct BackupScheduler {
    /// 准备资源池（控制扫描和加密并发）
    prepare_pool: Arc<PrepareResourcePool>,
    /// 槽位管理器（控制上传并发）
    slot_manager: Arc<SlotManager>,
    /// 记录管理器（预留用于后续记录备份历史）
    #[allow(dead_code)]
    record_manager: Arc<BackupRecordManager>,
    /// 加密服务（可选）
    encryption_service: Arc<RwLock<Option<EncryptionService>>>,
    /// 临时文件目录
    temp_dir: PathBuf,
    /// 等待上传的文件队列（按优先级排序）
    upload_queue: Arc<RwLock<VecDeque<FileTaskContext>>>,
    /// 正在准备中的任务数
    preparing_count: Arc<std::sync::atomic::AtomicUsize>,
    /// 正在上传中的任务数
    uploading_count: Arc<std::sync::atomic::AtomicUsize>,
    /// 事件发送通道
    event_tx: mpsc::UnboundedSender<SchedulerEvent>,
    /// 取消令牌
    cancel_token: CancellationToken,
    /// 最大并发备份任务数（预留用于后续扩展）
    #[allow(dead_code)]
    max_concurrent_backup_tasks: usize,
}

impl BackupScheduler {
    /// 创建新的备份调度器
    pub fn new(
        prepare_pool: Arc<PrepareResourcePool>,
        slot_manager: Arc<SlotManager>,
        record_manager: Arc<BackupRecordManager>,
        encryption_service: Arc<RwLock<Option<EncryptionService>>>,
        temp_dir: PathBuf,
        max_concurrent_backup_tasks: usize,
    ) -> (Self, mpsc::UnboundedReceiver<SchedulerEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let scheduler = Self {
            prepare_pool,
            slot_manager,
            record_manager,
            encryption_service,
            temp_dir,
            upload_queue: Arc::new(RwLock::new(VecDeque::new())),
            preparing_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            uploading_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            event_tx,
            cancel_token: CancellationToken::new(),
            max_concurrent_backup_tasks,
        };

        (scheduler, event_rx)
    }

    /// 提交文件任务到调度器
    ///
    /// 文件任务会经历三阶段执行：
    /// 1. 等待扫描许可 → 执行阶段 1（逻辑准备）
    /// 2. 等待加密许可 → 执行阶段 2（资源准备）
    /// 3. 等待上传槽位 → 执行阶段 3（上传提交）
    pub async fn submit_file_task(
        &self,
        file_task: BackupFileTask,
        backup_task_id: String,
        config: &BackupConfig,
    ) -> Result<(), String> {
        let file_task_id = file_task.id.clone();
        let local_path = file_task.local_path.clone();
        let remote_path = file_task.remote_path.clone();
        let file_size = file_task.file_size;
        let encrypt_enabled = config.encrypt_enabled;
        let config_id = config.id.clone();

        // 创建执行上下文
        let context = FileTaskContext {
            file_task_id: file_task_id.clone(),
            backup_task_id: backup_task_id.clone(),
            config_id,
            local_path,
            remote_path,
            file_size,
            encrypt_enabled,
            encrypted_temp_path: None,
            encrypted_filename: None,
        };

        // 启动异步执行流程
        let prepare_pool = self.prepare_pool.clone();
        let encryption_service = self.encryption_service.clone();
        let temp_dir = self.temp_dir.clone();
        let event_tx = self.event_tx.clone();
        let cancel_token = self.cancel_token.clone();
        let upload_queue = self.upload_queue.clone();
        let preparing_count = self.preparing_count.clone();

        tokio::spawn(async move {
            // 阶段 1：逻辑准备阶段（获取扫描许可）
            tracing::debug!("文件任务 {} 进入阶段 1（逻辑准备）", file_task_id);

            // 获取扫描许可
            let _scan_permit = match prepare_pool.acquire_scan_permit().await {
                Ok(permit) => permit,
                Err(e) => {
                    let _ = event_tx.send(SchedulerEvent::FileFailed {
                        file_task_id: file_task_id.clone(),
                        backup_task_id: backup_task_id.clone(),
                        error: format!("获取扫描许可失败: {}", e),
                        retryable: true,
                    });
                    return;
                }
            };

            // 检查取消
            if cancel_token.is_cancelled() {
                return;
            }

            // 阶段 1 完成，进入阶段 2
            tracing::debug!("文件任务 {} 进入阶段 2（资源准备）", file_task_id);
            preparing_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // 阶段 2：资源准备阶段（不可中断）
            let mut final_context = context.clone();

            if encrypt_enabled {
                // 获取加密许可
                let _encrypt_permit = match prepare_pool.acquire_encrypt_permit().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        preparing_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = event_tx.send(SchedulerEvent::FileFailed {
                            file_task_id: file_task_id.clone(),
                            backup_task_id: backup_task_id.clone(),
                            error: format!("获取加密许可失败: {}", e),
                            retryable: true,
                        });
                        return;
                    }
                };

                // 执行加密（不可中断）
                let encryption_result = Self::encrypt_file(
                    &context.local_path,
                    &temp_dir,
                    &encryption_service,
                )
                .await;

                match encryption_result {
                    Ok((temp_path, encrypted_name)) => {
                        final_context.encrypted_temp_path = Some(temp_path);
                        final_context.encrypted_filename = Some(encrypted_name);
                    }
                    Err(e) => {
                        preparing_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = event_tx.send(SchedulerEvent::FileFailed {
                            file_task_id: file_task_id.clone(),
                            backup_task_id: backup_task_id.clone(),
                            error: format!("加密失败: {}", e),
                            retryable: false,
                        });
                        return;
                    }
                }
            }

            preparing_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

            // 阶段 2 完成，进入阶段 3 等待队列
            tracing::debug!("文件任务 {} 进入阶段 3 等待队列", file_task_id);

            // 加入上传等待队列
            {
                let mut queue = upload_queue.write();
                queue.push_back(final_context.clone());
            }

            // 发送准备完成事件
            let _ = event_tx.send(SchedulerEvent::FileReady(final_context));
        });

        Ok(())
    }

    /// 执行文件加密
    async fn encrypt_file(
        local_path: &PathBuf,
        temp_dir: &PathBuf,
        encryption_service: &Arc<RwLock<Option<EncryptionService>>>,
    ) -> Result<(PathBuf, String), String> {
        let service = encryption_service.read();
        let service = service
            .as_ref()
            .ok_or_else(|| "加密服务未配置".to_string())?;

        // 生成加密文件名
        let encrypted_name = EncryptionService::generate_encrypted_filename();

        // 生成临时文件路径
        let temp_path = temp_dir.join(&encrypted_name);

        // 使用 encrypt_file_chunked 方法直接加密文件
        service
            .encrypt_file_chunked(local_path, &temp_path)
            .map_err(|e| format!("加密失败: {}", e))?;

        Ok((temp_path, encrypted_name))
    }

    /// 尝试启动等待上传的任务
    ///
    /// 检查是否有空闲槽位，如果有则从队列中取出任务开始上传
    pub async fn try_start_uploads(&self) -> Vec<FileTaskContext> {
        let mut started = Vec::new();

        loop {
            // 检查是否可以获取槽位
            let context = self.slot_manager.get_context();
            if !self.can_acquire_upload_slot(&context) {
                break;
            }

            // 从队列取出任务
            let task_context = {
                let mut queue = self.upload_queue.write();
                queue.pop_front()
            };

            match task_context {
                Some(ctx) => {
                    // 尝试获取槽位
                    let result = self
                        .slot_manager
                        .try_acquire(&ctx.file_task_id, Priority::Backup);

                    match result {
                        crate::autobackup::priority::SlotAcquireResult::Acquired => {
                            self.uploading_count
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            started.push(ctx);
                        }
                        crate::autobackup::priority::SlotAcquireResult::Wait => {
                            // 放回队列
                            let mut queue = self.upload_queue.write();
                            queue.push_front(ctx);
                            break;
                        }
                        crate::autobackup::priority::SlotAcquireResult::Preempt(_) => {
                            // 备份任务不能抢占其他任务，放回队列
                            let mut queue = self.upload_queue.write();
                            queue.push_front(ctx);
                            break;
                        }
                    }
                }
                None => break,
            }
        }

        started
    }

    /// 检查是否可以获取上传槽位
    fn can_acquire_upload_slot(&self, context: &PriorityContext) -> bool {
        // 备份任务只有在没有高优先级任务等待时才能获取槽位
        context.waiting_count == 0 && context.active_count < context.max_concurrent
    }

    /// 标记文件上传完成
    pub fn mark_file_completed(&self, file_task_id: &str, backup_task_id: &str) {
        // 释放槽位
        self.slot_manager.release(file_task_id);
        self.uploading_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        // 发送完成事件
        let _ = self.event_tx.send(SchedulerEvent::FileCompleted {
            file_task_id: file_task_id.to_string(),
            backup_task_id: backup_task_id.to_string(),
        });
    }

    /// 标记文件上传失败
    pub fn mark_file_failed(&self, file_task_id: &str, backup_task_id: &str, error: String, retryable: bool) {
        // 释放槽位
        self.slot_manager.release(file_task_id);
        self.uploading_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        // 发送失败事件
        let _ = self.event_tx.send(SchedulerEvent::FileFailed {
            file_task_id: file_task_id.to_string(),
            backup_task_id: backup_task_id.to_string(),
            error,
            retryable,
        });
    }

    /// 处理抢占请求
    ///
    /// 当普通任务需要槽位时，会抢占正在上传的备份任务
    pub async fn handle_preempt(&self) -> Option<String> {
        // 订阅抢占通知
        let mut rx = self.slot_manager.subscribe_preempt();

        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(request) => {
                        tracing::info!(
                            "备份任务被抢占: requester={}, target_priority={:?}",
                            request.requester_task_id,
                            request.target_priority
                        );
                        // 返回被抢占的任务 ID（由调用方处理暂停逻辑）
                        self.slot_manager.preempt(&request.requester_task_id, request.requester_priority)
                    }
                    Err(_) => None,
                }
            }
            _ = self.cancel_token.cancelled() => None,
        }
    }

    /// 取消调度器
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// 获取调度器状态
    pub fn get_status(&self) -> SchedulerStatus {
        let queue_len = self.upload_queue.read().len();
        let preparing = self.preparing_count.load(std::sync::atomic::Ordering::SeqCst);
        let uploading = self.uploading_count.load(std::sync::atomic::Ordering::SeqCst);
        let (scan_used, scan_total) = self.prepare_pool.scan_slots_info();
        let (encrypt_used, encrypt_total) = self.prepare_pool.encrypt_slots_info();

        SchedulerStatus {
            queue_length: queue_len,
            preparing_count: preparing,
            uploading_count: uploading,
            scan_slots: format!("{}/{}", scan_used, scan_total),
            encrypt_slots: format!("{}/{}", encrypt_used, encrypt_total),
        }
    }
}

/// 调度器状态
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchedulerStatus {
    /// 等待上传的队列长度
    pub queue_length: usize,
    /// 正在准备中的任务数
    pub preparing_count: usize,
    /// 正在上传中的任务数
    pub uploading_count: usize,
    /// 扫描槽位使用情况
    pub scan_slots: String,
    /// 加密槽位使用情况
    pub encrypt_slots: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_status() {
        let status = SchedulerStatus {
            queue_length: 5,
            preparing_count: 2,
            uploading_count: 1,
            scan_slots: "1/2".to_string(),
            encrypt_slots: "0/2".to_string(),
        };

        assert_eq!(status.queue_length, 5);
        assert_eq!(status.preparing_count, 2);
        assert_eq!(status.uploading_count, 1);
    }
}
