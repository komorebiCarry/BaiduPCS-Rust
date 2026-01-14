//! 备份 WebSocket 事件模块
//!
//! 定义备份相关的 WebSocket 事件和进度节流器

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::Mutex;

use super::task::{BackupTaskStatus, BackupSubPhase, TriggerType};

// ============================================================================
// 传输任务通知（上传/下载管理器 -> 自动备份管理器）
// ============================================================================

/// 传输任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferTaskStatus {
    /// 等待中
    Pending,
    /// 传输中
    Transferring,
    /// 已暂停
    Paused,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

/// 传输任务类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferTaskType {
    /// 上传任务
    Upload,
    /// 下载任务
    Download,
}

/// 统一的传输任务通知
///
/// 上传/下载管理器通过此枚举向自动备份管理器发送所有类型的通知，
/// 包括任务创建、进度更新、状态变更、完成、失败等。
///
/// 使用单一 channel 传输所有事件类型，简化架构。
#[derive(Debug, Clone)]
pub enum BackupTransferNotification {
    /// 任务创建
    Created {
        task_id: String,
        task_type: TransferTaskType,
        total_bytes: u64,
    },
    /// 进度更新
    Progress {
        task_id: String,
        task_type: TransferTaskType,
        transferred_bytes: u64,
        total_bytes: u64,
    },
    /// 状态变更
    StatusChanged {
        task_id: String,
        task_type: TransferTaskType,
        old_status: TransferTaskStatus,
        new_status: TransferTaskStatus,
    },
    /// 任务完成
    Completed {
        task_id: String,
        task_type: TransferTaskType,
    },
    /// 任务失败
    Failed {
        task_id: String,
        task_type: TransferTaskType,
        error_message: String,
    },
    /// 任务暂停
    Paused {
        task_id: String,
        task_type: TransferTaskType,
    },
    /// 任务恢复
    Resumed {
        task_id: String,
        task_type: TransferTaskType,
    },
    /// 任务删除
    Deleted {
        task_id: String,
        task_type: TransferTaskType,
    },
    /// 解密开始（仅下载任务）
    DecryptStarted {
        task_id: String,
        file_name: String,
    },
    /// 解密进度（仅下载任务）
    DecryptProgress {
        task_id: String,
        file_name: String,
        progress: f64,
        processed_bytes: u64,
        total_bytes: u64,
    },
    /// 解密完成（仅下载任务）
    DecryptCompleted {
        task_id: String,
        file_name: String,
        original_name: String,
        decrypted_path: String,
    },
}

impl BackupTransferNotification {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            Self::Created { task_id, .. } => task_id,
            Self::Progress { task_id, .. } => task_id,
            Self::StatusChanged { task_id, .. } => task_id,
            Self::Completed { task_id, .. } => task_id,
            Self::Failed { task_id, .. } => task_id,
            Self::Paused { task_id, .. } => task_id,
            Self::Resumed { task_id, .. } => task_id,
            Self::Deleted { task_id, .. } => task_id,
            Self::DecryptStarted { task_id, .. } => task_id,
            Self::DecryptProgress { task_id, .. } => task_id,
            Self::DecryptCompleted { task_id, .. } => task_id,
        }
    }

    /// 获取任务类型
    pub fn task_type(&self) -> TransferTaskType {
        match self {
            Self::Created { task_type, .. } => *task_type,
            Self::Progress { task_type, .. } => *task_type,
            Self::StatusChanged { task_type, .. } => *task_type,
            Self::Completed { task_type, .. } => *task_type,
            Self::Failed { task_type, .. } => *task_type,
            Self::Paused { task_type, .. } => *task_type,
            Self::Resumed { task_type, .. } => *task_type,
            Self::Deleted { task_type, .. } => *task_type,
            // 解密事件仅用于下载任务
            Self::DecryptStarted { .. } => TransferTaskType::Download,
            Self::DecryptProgress { .. } => TransferTaskType::Download,
            Self::DecryptCompleted { .. } => TransferTaskType::Download,
        }
    }

    /// 是否为上传任务
    pub fn is_upload(&self) -> bool {
        self.task_type() == TransferTaskType::Upload
    }

    /// 是否为下载任务
    pub fn is_download(&self) -> bool {
        self.task_type() == TransferTaskType::Download
    }

    /// 获取事件类型名称
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::Created { .. } => "created",
            Self::Progress { .. } => "progress",
            Self::StatusChanged { .. } => "status_changed",
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
            Self::Paused { .. } => "paused",
            Self::Resumed { .. } => "resumed",
            Self::Deleted { .. } => "deleted",
            Self::DecryptStarted { .. } => "decrypt_started",
            Self::DecryptProgress { .. } => "decrypt_progress",
            Self::DecryptCompleted { .. } => "decrypt_completed",
        }
    }
}

// ============================================================================
// 备份 WebSocket 事件（自动备份管理器 -> 前端）
// ============================================================================

/// 备份事件类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackupEvent {
    /// 任务创建
    TaskCreated(TaskCreatedEvent),
    /// 任务状态变更
    TaskStatusChanged(TaskStatusChangedEvent),
    /// 任务进度更新
    TaskProgress(TaskProgressEvent),
    /// 扫描进度更新
    ScanProgress(ScanProgressEvent),
    /// 文件进度更新
    FileProgress(FileProgressEvent),
    /// 文件完成
    FileCompleted(FileCompletedEvent),
    /// 文件失败
    FileFailed(FileFailedEvent),
    /// 文件跳过
    FileSkipped(FileSkippedEvent),
    /// 任务完成
    TaskCompleted(TaskCompletedEvent),
    /// 任务失败
    TaskFailed(TaskFailedEvent),
    /// 配置变更
    ConfigChanged(ConfigChangedEvent),
}

/// 任务创建事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCreatedEvent {
    pub task_id: String,
    pub config_id: String,
    pub config_name: String,
    pub trigger_type: TriggerType,
    pub created_at: DateTime<Utc>,
}

/// 任务状态变更事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusChangedEvent {
    pub task_id: String,
    pub config_id: String,
    pub old_status: BackupTaskStatus,
    pub new_status: BackupTaskStatus,
    pub sub_phase: Option<BackupSubPhase>,
    pub timestamp: DateTime<Utc>,
}

/// 任务进度事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgressEvent {
    pub task_id: String,
    pub config_id: String,
    pub completed_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    pub speed_bytes_per_sec: u64,
    pub eta_seconds: Option<u64>,
    pub timestamp: DateTime<Utc>,
}

/// 扫描进度事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanProgressEvent {
    pub task_id: String,
    pub config_id: String,
    pub scanned_dirs: usize,
    pub scanned_files: usize,
    pub current_dir: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// 文件进度事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileProgressEvent {
    pub task_id: String,
    pub file_task_id: String,
    pub file_name: String,
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    pub speed_bytes_per_sec: u64,
    pub timestamp: DateTime<Utc>,
}

/// 文件完成事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCompletedEvent {
    pub task_id: String,
    pub file_task_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub duration_ms: u64,
    pub timestamp: DateTime<Utc>,
}

/// 文件失败事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileFailedEvent {
    pub task_id: String,
    pub file_task_id: String,
    pub file_name: String,
    pub error_message: String,
    pub retry_count: u32,
    pub will_retry: bool,
    pub timestamp: DateTime<Utc>,
}

/// 文件跳过事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSkippedEvent {
    pub task_id: String,
    pub file_task_id: String,
    pub file_name: String,
    pub reason: String,
    pub timestamp: DateTime<Utc>,
}

/// 任务完成事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompletedEvent {
    pub task_id: String,
    pub config_id: String,
    pub config_name: String,
    pub success_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub transferred_bytes: u64,
    pub duration_seconds: f64,
    pub timestamp: DateTime<Utc>,
}

/// 任务失败事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailedEvent {
    pub task_id: String,
    pub config_id: String,
    pub config_name: String,
    pub error_message: String,
    pub completed_count: usize,
    pub total_count: usize,
    pub timestamp: DateTime<Utc>,
}

/// 配置变更事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChangedEvent {
    pub config_id: String,
    pub config_name: String,
    pub change_type: ConfigChangeType,
    pub timestamp: DateTime<Utc>,
}

/// 配置变更类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigChangeType {
    Created,
    Updated,
    Deleted,
    Enabled,
    Disabled,
}

/// 进度节流器
///
/// 用于控制进度事件的发送频率，避免过多的 WebSocket 消息
pub struct ProgressThrottler {
    /// 最小发送间隔（毫秒）
    min_interval_ms: u64,
    /// 最小字节变化量
    min_bytes_change: u64,
    /// 上次发送时间（按任务 ID）
    last_send_times: Mutex<HashMap<String, Instant>>,
    /// 上次发送的字节数（按任务 ID）
    last_sent_bytes: Mutex<HashMap<String, u64>>,
}

impl ProgressThrottler {
    /// 创建新的进度节流器
    pub fn new(min_interval_ms: u64, min_bytes_change: u64) -> Self {
        Self {
            min_interval_ms,
            min_bytes_change,
            last_send_times: Mutex::new(HashMap::new()),
            last_sent_bytes: Mutex::new(HashMap::new()),
        }
    }

    /// 使用默认配置创建（500ms 间隔，256KB 变化量）
    pub fn default_config() -> Self {
        Self::new(500, 256 * 1024)
    }

    /// 检查是否应该发送进度更新
    pub fn should_send(&self, task_id: &str, current_bytes: u64) -> bool {
        let now = Instant::now();

        let mut last_times = self.last_send_times.lock();
        let mut last_bytes = self.last_sent_bytes.lock();

        // 检查时间间隔
        if let Some(last_time) = last_times.get(task_id) {
            if now.duration_since(*last_time) < Duration::from_millis(self.min_interval_ms) {
                // 检查字节变化量
                if let Some(&last) = last_bytes.get(task_id) {
                    if current_bytes.saturating_sub(last) < self.min_bytes_change {
                        return false;
                    }
                }
            }
        }

        // 更新记录
        last_times.insert(task_id.to_string(), now);
        last_bytes.insert(task_id.to_string(), current_bytes);

        true
    }

    /// 强制发送（用于任务完成等重要事件）
    pub fn force_send(&self, task_id: &str, current_bytes: u64) {
        let mut last_times = self.last_send_times.lock();
        let mut last_bytes = self.last_sent_bytes.lock();

        last_times.insert(task_id.to_string(), Instant::now());
        last_bytes.insert(task_id.to_string(), current_bytes);
    }

    /// 清理任务记录
    pub fn cleanup(&self, task_id: &str) {
        self.last_send_times.lock().remove(task_id);
        self.last_sent_bytes.lock().remove(task_id);
    }

    /// 清理所有记录
    pub fn cleanup_all(&self) {
        self.last_send_times.lock().clear();
        self.last_sent_bytes.lock().clear();
    }
}

/// 速度计算器
pub struct SpeedCalculator {
    /// 采样窗口大小
    window_size: usize,
    /// 采样数据（时间戳，字节数）
    samples: Mutex<Vec<(Instant, u64)>>,
}

impl SpeedCalculator {
    /// 创建新的速度计算器
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            samples: Mutex::new(Vec::with_capacity(window_size)),
        }
    }

    /// 使用默认窗口大小（10 个采样点）
    pub fn default_window() -> Self {
        Self::new(10)
    }

    /// 添加采样点
    pub fn add_sample(&self, bytes: u64) {
        let mut samples = self.samples.lock();
        let now = Instant::now();

        samples.push((now, bytes));

        // 保持窗口大小
        if samples.len() > self.window_size {
            samples.remove(0);
        }
    }

    /// 计算当前速度（字节/秒）
    pub fn calculate_speed(&self) -> u64 {
        let samples = self.samples.lock();

        if samples.len() < 2 {
            return 0;
        }

        let first = &samples[0];
        let last = &samples[samples.len() - 1];

        let duration = last.0.duration_since(first.0);
        let bytes_diff = last.1.saturating_sub(first.1);

        if duration.as_secs_f64() > 0.0 {
            (bytes_diff as f64 / duration.as_secs_f64()) as u64
        } else {
            0
        }
    }

    /// 计算预计剩余时间（秒）
    pub fn calculate_eta(&self, remaining_bytes: u64) -> Option<u64> {
        let speed = self.calculate_speed();
        if speed > 0 {
            Some(remaining_bytes / speed)
        } else {
            None
        }
    }

    /// 重置计算器
    pub fn reset(&self) {
        self.samples.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_progress_throttler() {
        let throttler = ProgressThrottler::new(100, 1024);

        // 第一次应该发送
        assert!(throttler.should_send("task1", 0));

        // 立即再次调用，时间和字节都不满足，不应该发送
        assert!(!throttler.should_send("task1", 100));

        // 字节变化足够大，应该发送
        assert!(throttler.should_send("task1", 2048));

        // 等待足够时间后应该发送
        sleep(Duration::from_millis(150));
        assert!(throttler.should_send("task1", 2048));
    }

    #[test]
    fn test_speed_calculator() {
        let calc = SpeedCalculator::new(5);

        calc.add_sample(0);
        sleep(Duration::from_millis(100));
        calc.add_sample(1000);

        let speed = calc.calculate_speed();
        // 速度应该大约是 10000 字节/秒（1000 字节 / 0.1 秒）
        assert!(speed > 5000 && speed < 15000);
    }
}
