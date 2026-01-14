//! 备份任务数据结构

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::collections::{HashSet, HashMap};
use chrono::{DateTime, Utc};

/// 备份任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupTaskStatus {
    /// 等待中（在队列中等待槽位）
    Queued,
    /// 准备中（扫描、加密，不可中断）
    Preparing,
    /// 上传/下载中（可被抢占）
    Transferring,
    /// 已完成
    Completed,
    /// 部分完成
    PartiallyCompleted,
    /// 已取消
    Cancelled,
    /// 失败
    Failed,
    /// 已暂停（被抢占或用户暂停）
    Paused,
}

/// 备份子阶段（用于更细粒度的状态追踪）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupSubPhase {
    /// 去重检查中
    DedupChecking,
    /// 等待槽位
    WaitingSlot,
    /// 加密中
    Encrypting,
    /// 上传中
    Uploading,
    /// 下载中
    Downloading,
    /// 解密中
    Decrypting,
    /// 被抢占（等待恢复）
    Preempted,
}

/// 文件备份状态（更细粒度）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileBackupState {
    /// 待扫描
    PendingScan,
    /// 扫描中
    Scanning,
    /// 待去重检查
    PendingDedup,
    /// 去重检查中
    DedupChecking,
    /// 待加密
    PendingEncrypt,
    /// 加密中
    Encrypting,
    /// 待传输
    PendingTransfer,
    /// 传输中
    Transferring,
    /// 待解密
    PendingDecrypt,
    /// 解密中
    Decrypting,
    /// 已完成
    Completed,
    /// 已跳过
    Skipped,
    /// 失败
    Failed,
}

/// 过滤原因（文件被过滤的原因）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterReason {
    /// 扩展名不在包含列表中
    ExtensionNotIncluded(String),
    /// 扩展名在排除列表中
    ExtensionExcluded(String),
    /// 目录被排除
    DirectoryExcluded(String),
    /// 文件太大
    FileTooLarge { size: u64, max: u64 },
    /// 文件太小
    FileTooSmall { size: u64, min: u64 },
    /// 隐藏文件
    HiddenFile,
    /// 系统文件
    SystemFile,
    /// 临时文件
    TempFile,
}

/// 跳过原因（文件被跳过的原因）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// 已存在（去重）
    AlreadyExists,
    /// 文件未变化
    Unchanged,
    /// 被过滤
    Filtered(FilterReason),
    /// 用户取消
    UserCancelled,
    /// 配置禁用
    ConfigDisabled,
}

/// 备份任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupTask {
    /// 任务唯一标识
    pub id: String,
    /// 关联的配置 ID
    pub config_id: String,
    /// 任务状态
    pub status: BackupTaskStatus,
    /// 子阶段（更细粒度的状态）
    #[serde(default)]
    pub sub_phase: Option<BackupSubPhase>,
    /// 触发类型
    pub trigger_type: TriggerType,
    /// 待处理的文件列表
    pub pending_files: Vec<BackupFileTask>,
    /// 已完成的文件数
    pub completed_count: usize,
    /// 失败的文件数
    pub failed_count: usize,
    /// 跳过的文件数（去重）
    pub skipped_count: usize,
    /// 总文件数
    pub total_count: usize,
    /// 已传输字节数
    pub transferred_bytes: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 扫描进度（用于断点恢复）
    #[serde(default)]
    pub scan_progress: Option<ScanProgress>,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 开始时间
    pub started_at: Option<DateTime<Utc>>,
    /// 完成时间
    pub completed_at: Option<DateTime<Utc>>,
    /// 错误信息
    pub error_message: Option<String>,
    /// 待完成的上传任务ID集合（用于监听器模式）
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub pending_upload_task_ids: HashSet<String>,
    /// 待完成的下载任务ID集合（用于监听器模式）
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub pending_download_task_ids: HashSet<String>,
    /// 传输任务ID到文件任务ID的映射（transfer_task_id -> file_task_id）
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub transfer_task_map: HashMap<String, String>,
}

/// 扫描进度（用于断点恢复）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanProgress {
    /// 已扫描的目录数
    pub scanned_dirs: usize,
    /// 已扫描的文件数
    pub scanned_files: usize,
    /// 当前扫描的目录
    pub current_dir: Option<PathBuf>,
    /// 最后扫描时间
    pub last_scan_at: DateTime<Utc>,
}

/// 触发类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerType {
    /// 文件监听触发
    Watch,
    /// 定时轮询触发
    Poll,
    /// 手动触发
    Manual,
}

/// 备份操作类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupOperationType {
    /// 上传
    Upload,
    /// 下载
    Download,
}

/// 单个文件的备份任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupFileTask {
    /// 文件任务 ID
    pub id: String,
    /// 父任务 ID
    pub parent_task_id: String,
    /// 本地文件路径
    pub local_path: PathBuf,
    /// 云端文件路径
    pub remote_path: String,
    /// 文件大小
    pub file_size: u64,
    /// 文件头MD5（前128KB，用于去重兜底）
    #[serde(default)]
    pub head_md5: Option<String>,
    /// 百度网盘文件ID（下载备份用，用于重启后重建下载任务）
    #[serde(default)]
    pub fs_id: Option<u64>,
    /// 文件状态
    pub status: BackupFileStatus,
    /// 子阶段（更细粒度的状态追踪）
    #[serde(default)]
    pub sub_phase: Option<BackupSubPhase>,
    /// 跳过原因（如果被跳过）
    #[serde(default)]
    pub skip_reason: Option<SkipReason>,
    /// 是否加密
    pub encrypted: bool,
    /// 加密后的文件名（如果加密）
    pub encrypted_name: Option<String>,
    /// 临时加密文件路径
    pub temp_encrypted_path: Option<PathBuf>,
    /// 已传输字节数
    pub transferred_bytes: u64,
    /// 解密进度（0.0 - 100.0，仅下载备份使用）
    #[serde(default)]
    pub decrypt_progress: Option<f64>,
    /// 错误信息
    pub error_message: Option<String>,
    /// 重试次数
    pub retry_count: u32,
    /// 关联的任务ID（上传或下载任务ID，用于服务重启后恢复）
    #[serde(default)]
    pub related_task_id: Option<String>,
    /// 备份操作类型（区分上传/下载）
    #[serde(default)]
    pub backup_operation_type: Option<BackupOperationType>,
    /// 创建时间
    #[serde(default = "chrono::Utc::now")]
    pub created_at: DateTime<Utc>,
    /// 更新时间
    #[serde(default = "chrono::Utc::now")]
    pub updated_at: DateTime<Utc>,
}

/// 文件备份状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupFileStatus {
    /// 待处理
    Pending,
    /// 去重检查中
    Checking,
    /// 已跳过（去重）
    Skipped,
    /// 加密中
    Encrypting,
    /// 等待上传/下载
    WaitingTransfer,
    /// 传输中
    Transferring,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

/// 同步结果摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// 任务 ID
    pub task_id: String,
    /// 配置 ID
    pub config_id: String,
    /// 成功数量
    pub success_count: usize,
    /// 失败数量
    pub failed_count: usize,
    /// 跳过数量（去重）
    pub skipped_count: usize,
    /// 总数量
    pub total_count: usize,
    /// 传输字节数
    pub transferred_bytes: u64,
    /// 耗时（秒）
    pub duration_seconds: f64,
    /// 触发类型
    pub trigger_type: TriggerType,
    /// 完成时间
    pub completed_at: DateTime<Utc>,
}

/// 备份任务运行时状态（用于实时进度更新）
#[derive(Debug)]
pub struct BackupTaskRuntime {
    /// 任务 ID
    pub task_id: String,
    /// 配置 ID
    pub config_id: String,
    /// 已完成文件数（原子计数）
    pub completed_count: std::sync::atomic::AtomicUsize,
    /// 已传输字节数（原子计数）
    pub transferred_bytes: std::sync::atomic::AtomicU64,
    /// 是否被取消
    pub cancelled: std::sync::atomic::AtomicBool,
    /// 是否被暂停
    pub paused: std::sync::atomic::AtomicBool,
}

impl BackupTaskRuntime {
    pub fn new(task_id: String, config_id: String) -> Self {
        Self {
            task_id,
            config_id,
            completed_count: std::sync::atomic::AtomicUsize::new(0),
            transferred_bytes: std::sync::atomic::AtomicU64::new(0),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            paused: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn increment_completed(&self) {
        self.completed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn add_transferred_bytes(&self, bytes: u64) {
        self.transferred_bytes.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn pause(&self) {
        self.paused.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
