//! WebSocket 事件类型定义
//!
//! 定义所有任务相关的事件类型，用于 WebSocket 实时推送

use serde::{Deserialize, Serialize};

/// 事件优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventPriority {
    /// 低优先级：进度更新
    Low = 0,
    /// 中优先级：状态变更
    Medium = 1,
    /// 高优先级：完成、失败、删除等关键事件
    High = 2,
}

/// 下载任务事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum DownloadEvent {
    /// 任务创建
    Created {
        task_id: String,
        fs_id: u64,
        remote_path: String,
        local_path: String,
        total_size: u64,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
        /// 原始文件名（加密文件解密后的文件名）
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_filename: Option<String>,
    },
    /// 进度更新
    Progress {
        task_id: String,
        downloaded_size: u64,
        total_size: u64,
        speed: u64,
        progress: f64,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 状态变更
    StatusChanged {
        task_id: String,
        old_status: String,
        new_status: String,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务完成
    Completed {
        task_id: String,
        completed_at: i64,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务失败
    Failed {
        task_id: String,
        error: String,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务暂停
    Paused {
        task_id: String,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务恢复
    Resumed {
        task_id: String,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务删除
    Deleted {
        task_id: String,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 解密进度
    DecryptProgress {
        task_id: String,
        /// 解密进度 (0.0 - 100.0)
        decrypt_progress: f64,
        /// 已处理字节数
        processed_bytes: u64,
        /// 总字节数
        total_bytes: u64,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 解密完成
    DecryptCompleted {
        task_id: String,
        /// 解密后原始文件大小
        original_size: u64,
        /// 解密后文件路径
        decrypted_path: String,
        group_id: Option<String>,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
}

impl DownloadEvent {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            DownloadEvent::Created { task_id, .. } => task_id,
            DownloadEvent::Progress { task_id, .. } => task_id,
            DownloadEvent::StatusChanged { task_id, .. } => task_id,
            DownloadEvent::Completed { task_id, .. } => task_id,
            DownloadEvent::Failed { task_id, .. } => task_id,
            DownloadEvent::Paused { task_id, .. } => task_id,
            DownloadEvent::Resumed { task_id, .. } => task_id,
            DownloadEvent::Deleted { task_id, .. } => task_id,
            DownloadEvent::DecryptProgress { task_id, .. } => task_id,
            DownloadEvent::DecryptCompleted { task_id, .. } => task_id,
        }
    }

    /// 获取分组id
    pub fn group_id(&self) -> Option<&str> {
        match self {
            DownloadEvent::Created { group_id, .. } => group_id.as_deref(),
            DownloadEvent::Progress { group_id, .. } => group_id.as_deref(),
            DownloadEvent::StatusChanged { group_id, .. } => group_id.as_deref(),
            DownloadEvent::Completed { group_id, .. } => group_id.as_deref(),
            DownloadEvent::Failed { group_id, .. } => group_id.as_deref(),
            DownloadEvent::Paused { group_id, .. } => group_id.as_deref(),
            DownloadEvent::Resumed { group_id, .. } => group_id.as_deref(),
            DownloadEvent::Deleted { group_id, .. } => group_id.as_deref(),
            DownloadEvent::DecryptProgress { group_id, .. } => group_id.as_deref(),
            DownloadEvent::DecryptCompleted { group_id, .. } => group_id.as_deref(),
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            DownloadEvent::Progress { .. } => EventPriority::Low,
            DownloadEvent::DecryptProgress { .. } => EventPriority::Low,
            DownloadEvent::StatusChanged { .. } => EventPriority::Medium,
            DownloadEvent::Created { .. } => EventPriority::Medium,
            DownloadEvent::Completed { .. } => EventPriority::High,
            DownloadEvent::Failed { .. } => EventPriority::High,
            DownloadEvent::Paused { .. } => EventPriority::Medium,
            DownloadEvent::Resumed { .. } => EventPriority::Medium,
            DownloadEvent::Deleted { .. } => EventPriority::High,
            DownloadEvent::DecryptCompleted { .. } => EventPriority::High,
        }
    }

    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            DownloadEvent::Created { .. } => "created",
            DownloadEvent::Progress { .. } => "progress",
            DownloadEvent::StatusChanged { .. } => "status_changed",
            DownloadEvent::Completed { .. } => "completed",
            DownloadEvent::Failed { .. } => "failed",
            DownloadEvent::Paused { .. } => "paused",
            DownloadEvent::Resumed { .. } => "resumed",
            DownloadEvent::Deleted { .. } => "deleted",
            DownloadEvent::DecryptProgress { .. } => "decrypt_progress",
            DownloadEvent::DecryptCompleted { .. } => "decrypt_completed",
        }
    }

    /// 是否为自动备份任务
    pub fn is_backup(&self) -> bool {
        match self {
            DownloadEvent::Created { is_backup, .. } => *is_backup,
            DownloadEvent::Progress { is_backup, .. } => *is_backup,
            DownloadEvent::StatusChanged { is_backup, .. } => *is_backup,
            DownloadEvent::Completed { is_backup, .. } => *is_backup,
            DownloadEvent::Failed { is_backup, .. } => *is_backup,
            DownloadEvent::Paused { is_backup, .. } => *is_backup,
            DownloadEvent::Resumed { is_backup, .. } => *is_backup,
            DownloadEvent::Deleted { is_backup, .. } => *is_backup,
            DownloadEvent::DecryptProgress { is_backup, .. } => *is_backup,
            DownloadEvent::DecryptCompleted { is_backup, .. } => *is_backup,
        }
    }
}

/// 文件夹下载事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum FolderEvent {
    /// 文件夹创建
    Created {
        folder_id: String,
        name: String,
        remote_root: String,
        local_root: String,
    },
    /// 进度更新
    Progress {
        folder_id: String,
        downloaded_size: u64,
        total_size: u64,
        completed_files: u64,
        total_files: u64,
        speed: u64,
        status: String,
    },
    /// 状态变更
    StatusChanged {
        folder_id: String,
        old_status: String,
        new_status: String,
    },
    /// 扫描完成
    ScanCompleted {
        folder_id: String,
        total_files: u64,
        total_size: u64,
    },
    /// 文件夹完成
    Completed {
        folder_id: String,
        completed_at: i64,
    },
    /// 文件夹失败
    Failed { folder_id: String, error: String },
    /// 文件夹暂停
    Paused { folder_id: String },
    /// 文件夹恢复
    Resumed { folder_id: String },
    /// 文件夹删除
    Deleted { folder_id: String },
}

impl FolderEvent {
    /// 获取文件夹 ID
    pub fn folder_id(&self) -> &str {
        match self {
            FolderEvent::Created { folder_id, .. } => folder_id,
            FolderEvent::Progress { folder_id, .. } => folder_id,
            FolderEvent::StatusChanged { folder_id, .. } => folder_id,
            FolderEvent::ScanCompleted { folder_id, .. } => folder_id,
            FolderEvent::Completed { folder_id, .. } => folder_id,
            FolderEvent::Failed { folder_id, .. } => folder_id,
            FolderEvent::Paused { folder_id } => folder_id,
            FolderEvent::Resumed { folder_id } => folder_id,
            FolderEvent::Deleted { folder_id } => folder_id,
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            FolderEvent::Progress { .. } => EventPriority::Low,
            FolderEvent::StatusChanged { .. } => EventPriority::Medium,
            FolderEvent::Created { .. }
            | FolderEvent::ScanCompleted { .. }
            | FolderEvent::Completed { .. }
            | FolderEvent::Failed { .. }
            | FolderEvent::Paused { .. }
            | FolderEvent::Resumed { .. }
            | FolderEvent::Deleted { .. } => EventPriority::High,
        }
    }

    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            FolderEvent::Created { .. } => "created",
            FolderEvent::Progress { .. } => "progress",
            FolderEvent::StatusChanged { .. } => "status_changed",
            FolderEvent::ScanCompleted { .. } => "scan_completed",
            FolderEvent::Completed { .. } => "completed",
            FolderEvent::Failed { .. } => "failed",
            FolderEvent::Paused { .. } => "paused",
            FolderEvent::Resumed { .. } => "resumed",
            FolderEvent::Deleted { .. } => "deleted",
        }
    }
}

/// 上传任务事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum UploadEvent {
    /// 任务创建
    Created {
        task_id: String,
        local_path: String,
        remote_path: String,
        total_size: u64,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 进度更新
    Progress {
        task_id: String,
        uploaded_size: u64,
        total_size: u64,
        speed: u64,
        progress: f64,
        completed_chunks: usize,
        total_chunks: usize,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 状态变更
    StatusChanged {
        task_id: String,
        old_status: String,
        new_status: String,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务完成
    Completed {
        task_id: String,
        completed_at: i64,
        is_rapid_upload: bool,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务失败
    Failed {
        task_id: String,
        error: String,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务暂停
    Paused {
        task_id: String,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务恢复
    Resumed {
        task_id: String,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 任务删除
    Deleted {
        task_id: String,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 加密进度
    EncryptProgress {
        task_id: String,
        /// 加密进度 (0.0 - 100.0)
        encrypt_progress: f64,
        /// 已处理字节数
        processed_bytes: u64,
        /// 总字节数
        total_bytes: u64,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
    /// 加密完成
    EncryptCompleted {
        task_id: String,
        /// 加密后文件大小
        encrypted_size: u64,
        /// 原始文件大小
        original_size: u64,
        /// 是否为自动备份任务
        #[serde(default)]
        is_backup: bool,
    },
}

impl UploadEvent {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            UploadEvent::Created { task_id, .. } => task_id,
            UploadEvent::Progress { task_id, .. } => task_id,
            UploadEvent::StatusChanged { task_id, .. } => task_id,
            UploadEvent::Completed { task_id, .. } => task_id,
            UploadEvent::Failed { task_id, .. } => task_id,
            UploadEvent::Paused { task_id, .. } => task_id,
            UploadEvent::Resumed { task_id, .. } => task_id,
            UploadEvent::Deleted { task_id, .. } => task_id,
            UploadEvent::EncryptProgress { task_id, .. } => task_id,
            UploadEvent::EncryptCompleted { task_id, .. } => task_id,
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            UploadEvent::Progress { .. } => EventPriority::Low,
            UploadEvent::EncryptProgress { .. } => EventPriority::Low,
            UploadEvent::StatusChanged { .. } => EventPriority::Medium,
            UploadEvent::Created { .. }
            | UploadEvent::Completed { .. }
            | UploadEvent::Failed { .. }
            | UploadEvent::Paused { .. }
            | UploadEvent::Resumed { .. }
            | UploadEvent::Deleted { .. }
            | UploadEvent::EncryptCompleted { .. } => EventPriority::High,
        }
    }

    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            UploadEvent::Created { .. } => "created",
            UploadEvent::Progress { .. } => "progress",
            UploadEvent::StatusChanged { .. } => "status_changed",
            UploadEvent::Completed { .. } => "completed",
            UploadEvent::Failed { .. } => "failed",
            UploadEvent::Paused { .. } => "paused",
            UploadEvent::Resumed { .. } => "resumed",
            UploadEvent::Deleted { .. } => "deleted",
            UploadEvent::EncryptProgress { .. } => "encrypt_progress",
            UploadEvent::EncryptCompleted { .. } => "encrypt_completed",
        }
    }

    /// 是否为自动备份任务
    pub fn is_backup(&self) -> bool {
        match self {
            UploadEvent::Created { is_backup, .. } => *is_backup,
            UploadEvent::Progress { is_backup, .. } => *is_backup,
            UploadEvent::StatusChanged { is_backup, .. } => *is_backup,
            UploadEvent::Completed { is_backup, .. } => *is_backup,
            UploadEvent::Failed { is_backup, .. } => *is_backup,
            UploadEvent::Paused { is_backup, .. } => *is_backup,
            UploadEvent::Resumed { is_backup, .. } => *is_backup,
            UploadEvent::Deleted { is_backup, .. } => *is_backup,
            UploadEvent::EncryptProgress { is_backup, .. } => *is_backup,
            UploadEvent::EncryptCompleted { is_backup, .. } => *is_backup,
        }
    }
}

/// 转存任务事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum TransferEvent {
    /// 任务创建
    Created {
        task_id: String,
        share_url: String,
        save_path: String,
        auto_download: bool,
    },
    /// 进度更新
    Progress {
        task_id: String,
        status: String,
        transferred_count: usize,
        total_count: usize,
        progress: f64,
    },
    /// 状态变更
    StatusChanged {
        task_id: String,
        old_status: String,
        new_status: String,
    },
    /// 任务完成
    Completed { task_id: String, completed_at: i64 },
    /// 任务失败
    Failed {
        task_id: String,
        error: String,
        error_type: String,
    },
    /// 任务删除
    Deleted { task_id: String },
}

impl TransferEvent {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            TransferEvent::Created { task_id, .. } => task_id,
            TransferEvent::Progress { task_id, .. } => task_id,
            TransferEvent::StatusChanged { task_id, .. } => task_id,
            TransferEvent::Completed { task_id, .. } => task_id,
            TransferEvent::Failed { task_id, .. } => task_id,
            TransferEvent::Deleted { task_id } => task_id,
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            TransferEvent::Progress { .. } => EventPriority::Low,
            TransferEvent::StatusChanged { .. } => EventPriority::Medium,
            TransferEvent::Created { .. }
            | TransferEvent::Completed { .. }
            | TransferEvent::Failed { .. }
            | TransferEvent::Deleted { .. } => EventPriority::High,
        }
    }

    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            TransferEvent::Created { .. } => "created",
            TransferEvent::Progress { .. } => "progress",
            TransferEvent::StatusChanged { .. } => "status_changed",
            TransferEvent::Completed { .. } => "completed",
            TransferEvent::Failed { .. } => "failed",
            TransferEvent::Deleted { .. } => "deleted",
        }
    }
}

/// 备份任务事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum BackupEvent {
    /// 任务创建
    Created {
        task_id: String,
        config_id: String,
        config_name: String,
        direction: String,
        trigger_type: String,
    },
    /// 扫描进度
    ScanProgress {
        task_id: String,
        scanned_files: usize,
        scanned_dirs: usize,
    },
    /// 扫描完成
    ScanCompleted {
        task_id: String,
        total_files: usize,
        total_bytes: u64,
    },
    /// 文件进度
    FileProgress {
        task_id: String,
        file_task_id: String,
        file_name: String,
        transferred_bytes: u64,
        total_bytes: u64,
        status: String,
    },
    /// 文件状态变更
    FileStatusChanged {
        task_id: String,
        file_task_id: String,
        file_name: String,
        old_status: String,
        new_status: String,
    },
    /// 任务进度
    Progress {
        task_id: String,
        completed_count: usize,
        failed_count: usize,
        skipped_count: usize,
        total_count: usize,
        transferred_bytes: u64,
        total_bytes: u64,
    },
    /// 状态变更
    StatusChanged {
        task_id: String,
        old_status: String,
        new_status: String,
    },
    /// 任务完成
    Completed {
        task_id: String,
        completed_at: i64,
        success_count: usize,
        failed_count: usize,
        skipped_count: usize,
    },
    /// 任务失败
    Failed {
        task_id: String,
        error: String,
    },
    /// 任务暂停
    Paused {
        task_id: String,
    },
    /// 任务恢复
    Resumed {
        task_id: String,
    },
    /// 任务取消
    Cancelled {
        task_id: String,
    },
    /// 文件加密开始
    FileEncrypting {
        task_id: String,
        file_task_id: String,
        file_name: String,
    },
    /// 文件加密完成
    FileEncrypted {
        task_id: String,
        file_task_id: String,
        file_name: String,
        encrypted_name: String,
        encrypted_size: u64,
    },
    /// 文件解密开始
    FileDecrypting {
        task_id: String,
        file_task_id: String,
        file_name: String,
    },
    /// 文件解密完成
    FileDecrypted {
        task_id: String,
        file_task_id: String,
        file_name: String,
        original_name: String,
        original_size: u64,
    },
    /// 文件加密进度
    FileEncryptProgress {
        task_id: String,
        file_task_id: String,
        file_name: String,
        /// 加密进度 (0.0 - 100.0)
        progress: f64,
        /// 已处理字节数
        processed_bytes: u64,
        /// 总字节数
        total_bytes: u64,
    },
    /// 文件解密进度
    FileDecryptProgress {
        task_id: String,
        file_task_id: String,
        file_name: String,
        /// 解密进度 (0.0 - 100.0)
        progress: f64,
        /// 已处理字节数
        processed_bytes: u64,
        /// 总字节数
        total_bytes: u64,
    },
}

impl BackupEvent {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            BackupEvent::Created { task_id, .. } => task_id,
            BackupEvent::ScanProgress { task_id, .. } => task_id,
            BackupEvent::ScanCompleted { task_id, .. } => task_id,
            BackupEvent::FileProgress { task_id, .. } => task_id,
            BackupEvent::FileStatusChanged { task_id, .. } => task_id,
            BackupEvent::Progress { task_id, .. } => task_id,
            BackupEvent::StatusChanged { task_id, .. } => task_id,
            BackupEvent::Completed { task_id, .. } => task_id,
            BackupEvent::Failed { task_id, .. } => task_id,
            BackupEvent::Paused { task_id } => task_id,
            BackupEvent::Resumed { task_id } => task_id,
            BackupEvent::Cancelled { task_id } => task_id,
            BackupEvent::FileEncrypting { task_id, .. } => task_id,
            BackupEvent::FileEncrypted { task_id, .. } => task_id,
            BackupEvent::FileDecrypting { task_id, .. } => task_id,
            BackupEvent::FileDecrypted { task_id, .. } => task_id,
            BackupEvent::FileEncryptProgress { task_id, .. } => task_id,
            BackupEvent::FileDecryptProgress { task_id, .. } => task_id,
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            BackupEvent::Progress { .. } => EventPriority::Low,
            BackupEvent::ScanProgress { .. } => EventPriority::Low,
            BackupEvent::FileProgress { .. } => EventPriority::Low,
            BackupEvent::FileEncryptProgress { .. } => EventPriority::Low,
            BackupEvent::FileDecryptProgress { .. } => EventPriority::Low,
            BackupEvent::FileEncrypting { .. } => EventPriority::Medium,
            BackupEvent::FileDecrypting { .. } => EventPriority::Medium,
            BackupEvent::FileStatusChanged { .. } => EventPriority::Medium,
            BackupEvent::StatusChanged { .. } => EventPriority::Medium,
            BackupEvent::Created { .. }
            | BackupEvent::ScanCompleted { .. }
            | BackupEvent::Completed { .. }
            | BackupEvent::Failed { .. }
            | BackupEvent::Paused { .. }
            | BackupEvent::Resumed { .. }
            | BackupEvent::Cancelled { .. }
            | BackupEvent::FileEncrypted { .. }
            | BackupEvent::FileDecrypted { .. } => EventPriority::High,
        }
    }

    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            BackupEvent::Created { .. } => "created",
            BackupEvent::ScanProgress { .. } => "scan_progress",
            BackupEvent::ScanCompleted { .. } => "scan_completed",
            BackupEvent::FileProgress { .. } => "file_progress",
            BackupEvent::FileStatusChanged { .. } => "file_status_changed",
            BackupEvent::Progress { .. } => "progress",
            BackupEvent::StatusChanged { .. } => "status_changed",
            BackupEvent::Completed { .. } => "completed",
            BackupEvent::Failed { .. } => "failed",
            BackupEvent::Paused { .. } => "paused",
            BackupEvent::Resumed { .. } => "resumed",
            BackupEvent::Cancelled { .. } => "cancelled",
            BackupEvent::FileEncrypting { .. } => "file_encrypting",
            BackupEvent::FileEncrypted { .. } => "file_encrypted",
            BackupEvent::FileDecrypting { .. } => "file_decrypting",
            BackupEvent::FileDecrypted { .. } => "file_decrypted",
            BackupEvent::FileEncryptProgress { .. } => "file_encrypt_progress",
            BackupEvent::FileDecryptProgress { .. } => "file_decrypt_progress",
        }
    }
}

/// 离线下载事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum CloudDlEvent {
    /// 任务状态变化
    StatusChanged {
        task_id: i64,
        old_status: Option<i32>,
        new_status: i32,
        task: serde_json::Value,
    },
    /// 任务完成（可触发自动下载）
    TaskCompleted {
        task_id: i64,
        task: serde_json::Value,
        auto_download_config: Option<serde_json::Value>,
    },
    /// 进度更新
    ProgressUpdate {
        task_id: i64,
        finished_size: i64,
        file_size: i64,
        progress_percent: f32,
    },
    /// 任务列表刷新（初始加载或手动刷新）
    TaskListRefreshed {
        tasks: Vec<serde_json::Value>,
    },
}

impl CloudDlEvent {
    /// 获取任务 ID（如果有）
    pub fn task_id(&self) -> Option<String> {
        match self {
            CloudDlEvent::StatusChanged { task_id, .. } => Some(task_id.to_string()),
            CloudDlEvent::TaskCompleted { task_id, .. } => Some(task_id.to_string()),
            CloudDlEvent::ProgressUpdate { task_id, .. } => Some(task_id.to_string()),
            CloudDlEvent::TaskListRefreshed { .. } => None,
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            CloudDlEvent::ProgressUpdate { .. } => EventPriority::Low,
            CloudDlEvent::StatusChanged { .. } => EventPriority::Medium,
            CloudDlEvent::TaskCompleted { .. } => EventPriority::High,
            CloudDlEvent::TaskListRefreshed { .. } => EventPriority::High,
        }
    }

    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            CloudDlEvent::StatusChanged { .. } => "status_changed",
            CloudDlEvent::TaskCompleted { .. } => "task_completed",
            CloudDlEvent::ProgressUpdate { .. } => "progress_update",
            CloudDlEvent::TaskListRefreshed { .. } => "task_list_refreshed",
        }
    }
}

/// 统一任务事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "category", content = "event")]
pub enum TaskEvent {
    /// 下载事件
    #[serde(rename = "download")]
    Download(DownloadEvent),
    /// 文件夹下载事件
    #[serde(rename = "folder")]
    Folder(FolderEvent),
    /// 上传事件
    #[serde(rename = "upload")]
    Upload(UploadEvent),
    /// 转存事件
    #[serde(rename = "transfer")]
    Transfer(TransferEvent),
    /// 备份事件
    #[serde(rename = "backup")]
    Backup(BackupEvent),
    /// 离线下载事件
    #[serde(rename = "cloud_dl")]
    CloudDl(CloudDlEvent),
}

impl TaskEvent {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            TaskEvent::Download(e) => e.task_id(),
            TaskEvent::Folder(e) => e.folder_id(),
            TaskEvent::Upload(e) => e.task_id(),
            TaskEvent::Transfer(e) => e.task_id(),
            TaskEvent::Backup(e) => e.task_id(),
            TaskEvent::CloudDl(_e) => {
                // CloudDl 使用 i64 task_id，这里返回静态字符串
                // 实际 task_id 通过 task_id_string() 方法获取
                "cloud_dl"
            }
        }
    }

    /// 获取任务 ID 字符串（用于 CloudDl 等使用数字 ID 的事件）
    pub fn task_id_string(&self) -> String {
        match self {
            TaskEvent::CloudDl(e) => e.task_id().unwrap_or_default(),
            _ => self.task_id().to_string(),
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            TaskEvent::Download(e) => e.priority(),
            TaskEvent::Folder(e) => e.priority(),
            TaskEvent::Upload(e) => e.priority(),
            TaskEvent::Transfer(e) => e.priority(),
            TaskEvent::Backup(e) => e.priority(),
            TaskEvent::CloudDl(e) => e.priority(),
        }
    }

    /// 获取事件类别
    pub fn category(&self) -> &'static str {
        match self {
            TaskEvent::Download(_) => "download",
            TaskEvent::Folder(_) => "folder",
            TaskEvent::Upload(_) => "upload",
            TaskEvent::Transfer(_) => "transfer",
            TaskEvent::Backup(_) => "backup",
            TaskEvent::CloudDl(_) => "cloud_dl",
        }
    }

    /// 获取事件类型名称
    pub fn event_type(&self) -> &'static str {
        match self {
            TaskEvent::Download(e) => e.event_type_name(),
            TaskEvent::Folder(e) => e.event_type_name(),
            TaskEvent::Upload(e) => e.event_type_name(),
            TaskEvent::Transfer(e) => e.event_type_name(),
            TaskEvent::Backup(e) => e.event_type_name(),
            TaskEvent::CloudDl(e) => e.event_type_name(),
        }
    }

    /// 是否为活跃任务事件（需要高频推送）
    pub fn is_active(&self) -> bool {
        match self {
            TaskEvent::Download(DownloadEvent::Progress { .. }) => true,
            TaskEvent::Download(DownloadEvent::DecryptProgress { .. }) => true,
            TaskEvent::Download(DownloadEvent::StatusChanged { new_status, .. }) => {
                new_status == "downloading" || new_status == "decrypting"
            }
            TaskEvent::Folder(FolderEvent::Progress { .. }) => true,
            TaskEvent::Folder(FolderEvent::StatusChanged { new_status, .. }) => {
                new_status == "downloading" || new_status == "scanning"
            }
            TaskEvent::Upload(UploadEvent::Progress { .. }) => true,
            TaskEvent::Upload(UploadEvent::EncryptProgress { .. }) => true,
            TaskEvent::Upload(UploadEvent::StatusChanged { new_status, .. }) => {
                new_status == "uploading" || new_status == "encrypting"
            }
            TaskEvent::Transfer(TransferEvent::Progress { .. }) => true,
            TaskEvent::Transfer(TransferEvent::StatusChanged { new_status, .. }) => {
                new_status == "transferring" || new_status == "downloading"
            }
            TaskEvent::Backup(BackupEvent::Progress { .. }) => true,
            TaskEvent::Backup(BackupEvent::ScanProgress { .. }) => true,
            TaskEvent::Backup(BackupEvent::FileProgress { .. }) => true,
            TaskEvent::Backup(BackupEvent::FileEncryptProgress { .. }) => true,
            TaskEvent::Backup(BackupEvent::FileDecryptProgress { .. }) => true,
            TaskEvent::Backup(BackupEvent::StatusChanged { new_status, .. }) => {
                new_status == "transferring" || new_status == "preparing"
            }
            TaskEvent::CloudDl(CloudDlEvent::ProgressUpdate { .. }) => true,
            TaskEvent::CloudDl(CloudDlEvent::StatusChanged { new_status, .. }) => {
                *new_status == 1 // Running status
            }
            _ => false,
        }
    }

    /// 是否为自动备份任务事件
    ///
    /// 用于 WebSocket 消息隔离：备份任务事件不应发送到普通下载/上传页面
    pub fn is_backup(&self) -> bool {
        match self {
            TaskEvent::Download(e) => e.is_backup(),
            TaskEvent::Upload(e) => e.is_backup(),
            TaskEvent::Backup(_) => true,
            // 文件夹下载、转存任务和离线下载不支持备份标记
            TaskEvent::Folder(_) => false,
            TaskEvent::Transfer(_) => false,
            TaskEvent::CloudDl(_) => false,
        }
    }
}

/// 带时间戳的事件包装器
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedEvent {
    /// 事件 ID（全局唯一递增）
    pub event_id: u64,
    /// 时间戳（Unix 毫秒）
    pub timestamp: i64,
    /// 事件内容
    #[serde(flatten)]
    pub event: TaskEvent,
}

impl TimestampedEvent {
    /// 创建新的带时间戳事件
    pub fn new(event_id: u64, event: TaskEvent) -> Self {
        Self {
            event_id,
            timestamp: chrono::Utc::now().timestamp_millis(),
            event,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_event_serialization() {
        let event = DownloadEvent::Progress {
            task_id: "test-123".to_string(),
            downloaded_size: 1000,
            total_size: 2000,
            speed: 500,
            progress: 50.0,
            group_id: None,
            is_backup: false,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("progress"));
        assert!(json.contains("test-123"));
    }

    #[test]
    fn test_task_event_serialization() {
        let event = TaskEvent::Download(DownloadEvent::Created {
            task_id: "test-123".to_string(),
            fs_id: 12345,
            remote_path: "/test.txt".to_string(),
            local_path: "./test.txt".to_string(),
            total_size: 1024,
            group_id: None,
            is_backup: false,
            original_filename: None,
        });

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("download"));
        assert!(json.contains("created"));
    }

    #[test]
    fn test_event_priority() {
        let progress = DownloadEvent::Progress {
            task_id: "1".to_string(),
            downloaded_size: 0,
            total_size: 0,
            speed: 0,
            progress: 0.0,
            group_id: None,
            is_backup: false,
        };
        assert_eq!(progress.priority(), EventPriority::Low);

        let completed = DownloadEvent::Completed {
            task_id: "1".to_string(),
            completed_at: 0,
            group_id: None,
            is_backup: false,
        };
        assert_eq!(completed.priority(), EventPriority::High);
    }

    #[test]
    fn test_upload_encrypt_event_serialization() {
        let event = UploadEvent::EncryptProgress {
            task_id: "test-123".to_string(),
            encrypt_progress: 50.0,
            processed_bytes: 512000,
            total_bytes: 1024000,
            is_backup: false,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("encrypt_progress"));
        assert!(json.contains("test-123"));
        assert!(json.contains("512000"));

        // 测试反序列化
        let parsed: UploadEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id(), "test-123");
        assert_eq!(parsed.event_type_name(), "encrypt_progress");
    }

    #[test]
    fn test_upload_encrypt_completed_event() {
        let event = UploadEvent::EncryptCompleted {
            task_id: "test-456".to_string(),
            encrypted_size: 1100,
            original_size: 1024,
            is_backup: true,
        };

        assert_eq!(event.priority(), EventPriority::High);
        assert_eq!(event.event_type_name(), "encrypt_completed");
        assert!(event.is_backup());

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("encrypt_completed"));
        assert!(json.contains("1100"));
        assert!(json.contains("1024"));
    }

    #[test]
    fn test_download_decrypt_event_serialization() {
        let event = DownloadEvent::DecryptProgress {
            task_id: "test-789".to_string(),
            decrypt_progress: 75.0,
            processed_bytes: 768000,
            total_bytes: 1024000,
            group_id: None,
            is_backup: false,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("decrypt_progress"));
        assert!(json.contains("test-789"));
        assert!(json.contains("768000"));

        // 测试反序列化
        let parsed: DownloadEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id(), "test-789");
        assert_eq!(parsed.event_type_name(), "decrypt_progress");
    }

    #[test]
    fn test_download_decrypt_completed_event() {
        let event = DownloadEvent::DecryptCompleted {
            task_id: "test-abc".to_string(),
            original_size: 1024,
            decrypted_path: "/downloads/original.txt".to_string(),
            group_id: Some("folder-123".to_string()),
            is_backup: false,
        };

        assert_eq!(event.priority(), EventPriority::High);
        assert_eq!(event.event_type_name(), "decrypt_completed");
        assert_eq!(event.group_id(), Some("folder-123"));

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("decrypt_completed"));
        assert!(json.contains("/downloads/original.txt"));
    }

    #[test]
    fn test_encrypt_decrypt_event_priority() {
        // 加密进度事件应为低优先级
        let encrypt_progress = UploadEvent::EncryptProgress {
            task_id: "1".to_string(),
            encrypt_progress: 50.0,
            processed_bytes: 0,
            total_bytes: 0,
            is_backup: false,
        };
        assert_eq!(encrypt_progress.priority(), EventPriority::Low);

        // 加密完成事件应为高优先级
        let encrypt_completed = UploadEvent::EncryptCompleted {
            task_id: "1".to_string(),
            encrypted_size: 0,
            original_size: 0,
            is_backup: false,
        };
        assert_eq!(encrypt_completed.priority(), EventPriority::High);

        // 解密进度事件应为低优先级
        let decrypt_progress = DownloadEvent::DecryptProgress {
            task_id: "1".to_string(),
            decrypt_progress: 50.0,
            processed_bytes: 0,
            total_bytes: 0,
            group_id: None,
            is_backup: false,
        };
        assert_eq!(decrypt_progress.priority(), EventPriority::Low);

        // 解密完成事件应为高优先级
        let decrypt_completed = DownloadEvent::DecryptCompleted {
            task_id: "1".to_string(),
            original_size: 0,
            decrypted_path: "".to_string(),
            group_id: None,
            is_backup: false,
        };
        assert_eq!(decrypt_completed.priority(), EventPriority::High);
    }

    #[test]
    fn test_is_active_with_encrypt_decrypt_events() {
        // 加密进度事件应为活跃事件
        let encrypt_progress = TaskEvent::Upload(UploadEvent::EncryptProgress {
            task_id: "1".to_string(),
            encrypt_progress: 50.0,
            processed_bytes: 0,
            total_bytes: 0,
            is_backup: false,
        });
        assert!(encrypt_progress.is_active());

        // 解密进度事件应为活跃事件
        let decrypt_progress = TaskEvent::Download(DownloadEvent::DecryptProgress {
            task_id: "1".to_string(),
            decrypt_progress: 50.0,
            processed_bytes: 0,
            total_bytes: 0,
            group_id: None,
            is_backup: false,
        });
        assert!(decrypt_progress.is_active());

        // encrypting 状态变更应为活跃事件
        let encrypting_status = TaskEvent::Upload(UploadEvent::StatusChanged {
            task_id: "1".to_string(),
            old_status: "pending".to_string(),
            new_status: "encrypting".to_string(),
            is_backup: false,
        });
        assert!(encrypting_status.is_active());

        // decrypting 状态变更应为活跃事件
        let decrypting_status = TaskEvent::Download(DownloadEvent::StatusChanged {
            task_id: "1".to_string(),
            old_status: "downloading".to_string(),
            new_status: "decrypting".to_string(),
            group_id: None,
            is_backup: false,
        });
        assert!(decrypting_status.is_active());
    }

    #[test]
    fn test_backup_file_encrypt_progress_event() {
        let event = BackupEvent::FileEncryptProgress {
            task_id: "backup-123".to_string(),
            file_task_id: "file-456".to_string(),
            file_name: "test.txt".to_string(),
            progress: 50.0,
            processed_bytes: 512000,
            total_bytes: 1024000,
        };

        // 测试 task_id
        assert_eq!(event.task_id(), "backup-123");

        // 测试优先级（进度事件应为低优先级）
        assert_eq!(event.priority(), EventPriority::Low);

        // 测试事件类型名称
        assert_eq!(event.event_type_name(), "file_encrypt_progress");

        // 测试序列化
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("file_encrypt_progress"));
        assert!(json.contains("backup-123"));
        assert!(json.contains("file-456"));
        assert!(json.contains("test.txt"));
        assert!(json.contains("512000"));

        // 测试反序列化
        let parsed: BackupEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id(), "backup-123");
        assert_eq!(parsed.event_type_name(), "file_encrypt_progress");
    }

    #[test]
    fn test_backup_file_decrypt_progress_event() {
        let event = BackupEvent::FileDecryptProgress {
            task_id: "backup-789".to_string(),
            file_task_id: "file-abc".to_string(),
            file_name: "encrypted.bkup".to_string(),
            progress: 75.0,
            processed_bytes: 768000,
            total_bytes: 1024000,
        };

        // 测试 task_id
        assert_eq!(event.task_id(), "backup-789");

        // 测试优先级（进度事件应为低优先级）
        assert_eq!(event.priority(), EventPriority::Low);

        // 测试事件类型名称
        assert_eq!(event.event_type_name(), "file_decrypt_progress");

        // 测试序列化
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("file_decrypt_progress"));
        assert!(json.contains("backup-789"));
        assert!(json.contains("file-abc"));
        assert!(json.contains("encrypted.bkup"));
        assert!(json.contains("768000"));

        // 测试反序列化
        let parsed: BackupEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.task_id(), "backup-789");
        assert_eq!(parsed.event_type_name(), "file_decrypt_progress");
    }

    #[test]
    fn test_backup_encrypt_decrypt_progress_is_active() {
        // 备份文件加密进度事件应为活跃事件
        let encrypt_progress = TaskEvent::Backup(BackupEvent::FileEncryptProgress {
            task_id: "1".to_string(),
            file_task_id: "f1".to_string(),
            file_name: "test.txt".to_string(),
            progress: 50.0,
            processed_bytes: 0,
            total_bytes: 0,
        });
        assert!(encrypt_progress.is_active());

        // 备份文件解密进度事件应为活跃事件
        let decrypt_progress = TaskEvent::Backup(BackupEvent::FileDecryptProgress {
            task_id: "1".to_string(),
            file_task_id: "f1".to_string(),
            file_name: "test.bkup".to_string(),
            progress: 75.0,
            processed_bytes: 0,
            total_bytes: 0,
        });
        assert!(decrypt_progress.is_active());
    }
}
