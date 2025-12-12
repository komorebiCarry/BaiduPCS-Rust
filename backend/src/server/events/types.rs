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
    },
    /// 进度更新
    Progress {
        task_id: String,
        downloaded_size: u64,
        total_size: u64,
        speed: u64,
        progress: f64,
        group_id: Option<String>,
    },
    /// 状态变更
    StatusChanged {
        task_id: String,
        old_status: String,
        new_status: String,
        group_id: Option<String>,
    },
    /// 任务完成
    Completed {
        task_id: String,
        completed_at: i64,
        group_id: Option<String>,
    },
    /// 任务失败
    Failed {
        task_id: String,
        error: String,
        group_id: Option<String>,
    },
    /// 任务暂停
    Paused {
        task_id: String,
        group_id: Option<String>,
    },
    /// 任务恢复
    Resumed {
        task_id: String,
        group_id: Option<String>,
    },
    /// 任务删除
    Deleted {
        task_id: String,
        group_id: Option<String>,
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
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            DownloadEvent::Progress { .. } => EventPriority::Low,
            DownloadEvent::StatusChanged { .. } => EventPriority::Medium,
            DownloadEvent::Created { .. } => EventPriority::Medium,
            DownloadEvent::Completed { .. } => EventPriority::High,
            DownloadEvent::Failed { .. } => EventPriority::High,
            DownloadEvent::Paused { .. } => EventPriority::Medium,
            DownloadEvent::Resumed { .. } => EventPriority::Medium,
            DownloadEvent::Deleted { .. } => EventPriority::High,
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
        is_rapid_upload: bool,
    },
    /// 任务失败
    Failed { task_id: String, error: String },
    /// 任务暂停
    Paused { task_id: String },
    /// 任务恢复
    Resumed { task_id: String },
    /// 任务删除
    Deleted { task_id: String },
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
            UploadEvent::Paused { task_id } => task_id,
            UploadEvent::Resumed { task_id } => task_id,
            UploadEvent::Deleted { task_id } => task_id,
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            UploadEvent::Progress { .. } => EventPriority::Low,
            UploadEvent::StatusChanged { .. } => EventPriority::Medium,
            UploadEvent::Created { .. }
            | UploadEvent::Completed { .. }
            | UploadEvent::Failed { .. }
            | UploadEvent::Paused { .. }
            | UploadEvent::Resumed { .. }
            | UploadEvent::Deleted { .. } => EventPriority::High,
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
}

impl TaskEvent {
    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        match self {
            TaskEvent::Download(e) => e.task_id(),
            TaskEvent::Folder(e) => e.folder_id(),
            TaskEvent::Upload(e) => e.task_id(),
            TaskEvent::Transfer(e) => e.task_id(),
        }
    }

    /// 获取事件优先级
    pub fn priority(&self) -> EventPriority {
        match self {
            TaskEvent::Download(e) => e.priority(),
            TaskEvent::Folder(e) => e.priority(),
            TaskEvent::Upload(e) => e.priority(),
            TaskEvent::Transfer(e) => e.priority(),
        }
    }

    /// 获取事件类别
    pub fn category(&self) -> &'static str {
        match self {
            TaskEvent::Download(_) => "download",
            TaskEvent::Folder(_) => "folder",
            TaskEvent::Upload(_) => "upload",
            TaskEvent::Transfer(_) => "transfer",
        }
    }

    /// 获取事件类型名称
    pub fn event_type(&self) -> &'static str {
        match self {
            TaskEvent::Download(e) => e.event_type_name(),
            TaskEvent::Folder(e) => e.event_type_name(),
            TaskEvent::Upload(e) => e.event_type_name(),
            TaskEvent::Transfer(e) => e.event_type_name(),
        }
    }

    /// 是否为活跃任务事件（需要高频推送）
    pub fn is_active(&self) -> bool {
        match self {
            TaskEvent::Download(DownloadEvent::Progress { .. }) => true,
            TaskEvent::Download(DownloadEvent::StatusChanged { new_status, .. }) => {
                new_status == "downloading"
            }
            TaskEvent::Folder(FolderEvent::Progress { .. }) => true,
            TaskEvent::Folder(FolderEvent::StatusChanged { new_status, .. }) => {
                new_status == "downloading" || new_status == "scanning"
            }
            TaskEvent::Upload(UploadEvent::Progress { .. }) => true,
            TaskEvent::Upload(UploadEvent::StatusChanged { new_status, .. }) => {
                new_status == "uploading"
            }
            TaskEvent::Transfer(TransferEvent::Progress { .. }) => true,
            TaskEvent::Transfer(TransferEvent::StatusChanged { new_status, .. }) => {
                new_status == "transferring" || new_status == "downloading"
            }
            _ => false,
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
        };
        assert_eq!(progress.priority(), EventPriority::Low);

        let completed = DownloadEvent::Completed {
            task_id: "1".to_string(),
            completed_at: 0,
            group_id: None,
        };
        assert_eq!(completed.priority(), EventPriority::High);
    }
}
