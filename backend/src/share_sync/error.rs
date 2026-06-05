//! 分享同步错误类型

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 分享同步错误
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum ShareSyncError {
    /// 订阅配置错误
    #[error("订阅配置错误: {0}")]
    ConfigError(String),

    /// 分享链接无效 / 已失效 / 需要密码
    #[error("分享链接错误: {0}")]
    ShareLinkError(String),

    /// 网络错误
    #[error("网络错误: {0}")]
    NetworkError(String),

    /// 持久化错误
    #[error("持久化错误: {0}")]
    PersistenceError(String),

    /// 转存失败
    #[error("转存失败: {0}")]
    TransferError(String),

    /// 下载失败
    #[error("下载失败: {0}")]
    DownloadError(String),

    /// 文件系统错误
    #[error("文件系统错误: {0}")]
    FileSystemError(String),

    /// 调度错误
    #[error("调度错误: {0}")]
    SchedulerError(String),

    /// 订阅未找到
    #[error("订阅未找到: {0}")]
    SubscriptionNotFound(String),

    /// 订阅已存在
    #[error("订阅已存在: {0}")]
    SubscriptionExists(String),

    /// 内部错误
    #[error("内部错误: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for ShareSyncError {
    fn from(e: anyhow::Error) -> Self {
        ShareSyncError::Internal(e.to_string())
    }
}

impl From<std::io::Error> for ShareSyncError {
    fn from(e: std::io::Error) -> Self {
        ShareSyncError::FileSystemError(e.to_string())
    }
}

impl From<rusqlite::Error> for ShareSyncError {
    fn from(e: rusqlite::Error) -> Self {
        ShareSyncError::PersistenceError(e.to_string())
    }
}

impl From<serde_json::Error> for ShareSyncError {
    fn from(e: serde_json::Error) -> Self {
        ShareSyncError::PersistenceError(e.to_string())
    }
}

/// 错误分类（用于决定是否重试）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// 临时性错误（网络抖动、超时）—— 可重试
    Transient,
    /// 鉴权 / 风控错误 —— 不重试，需用户介入
    Auth,
    /// 配置错误 —— 不重试
    Config,
    /// 资源不存在 —— 跳过即可
    NotFound,
    /// 网盘空间不足 —— 不重试，需用户清理网盘后手动触发
    Quota,
    /// 本地磁盘空间不足 —— 不重试，需用户清理本地后手动触发
    LocalDiskFull,
    /// 其他
    Other,
}

/// 触发 `ErrorCategory::Quota` 判定的关键字
/// - 百度 `transfer_share_files` 的 task_errno=-32 会包装成 "网盘空间不足"（见
///   `backend/src/transfer/manager.rs:2740` 的 `handle_transfer_error`）
/// - 少数中文 errmsg 透传场景可能用 "空间不足" 短串
/// - task_errno 数字串 "errno=-32" 兼容未走 `handle_transfer_error` 的路径
const QUOTA_KEYWORDS: &[&str] = &[
    "网盘空间不足",
    "空间不足",
    "errno=-32",
    "errno=-7",
    "errno=-12",
];

/// 触发 `ErrorCategory::LocalDiskFull` 判定的关键字
/// - `std::io::Error` 的 `StorageFull`/`Other` 都会被 `From<io::Error>` 归为
///   `FileSystemError(msg)`（见上），所以通过 msg 文本判断
/// - 兼容 Linux 的 ENOSPC、macOS 的 "No space left on device"、Windows 的 "There is
///   not enough space on the disk" 等
const LOCAL_DISK_FULL_KEYWORDS: &[&str] = &[
    "ENOSPC",
    "no space left",
    "There is not enough space",
    "StorageFull",
];

fn matches_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|n| haystack.contains(n))
}

impl ShareSyncError {
    /// 错误分类
    pub fn category(&self) -> ErrorCategory {
        match self {
            ShareSyncError::NetworkError(_) => ErrorCategory::Transient,
            ShareSyncError::ShareLinkError(_) => ErrorCategory::Auth,
            ShareSyncError::TransferError(msg) | ShareSyncError::DownloadError(msg) => {
                // 顺序敏感：先查 Quota/LocalDisk（罕见但有特别语义），再查风控/不存在
                if matches_any(msg, QUOTA_KEYWORDS) {
                    ErrorCategory::Quota
                } else if matches_any(msg, LOCAL_DISK_FULL_KEYWORDS) {
                    ErrorCategory::LocalDiskFull
                } else if msg.contains("132") || msg.contains("风控") {
                    ErrorCategory::Auth
                } else if msg.contains("不存在") || msg.contains("已失效") {
                    ErrorCategory::NotFound
                } else {
                    ErrorCategory::Other
                }
            }
            ShareSyncError::FileSystemError(msg) => {
                if matches_any(msg, LOCAL_DISK_FULL_KEYWORDS) {
                    ErrorCategory::LocalDiskFull
                } else {
                    ErrorCategory::Other
                }
            }
            ShareSyncError::ConfigError(_) => ErrorCategory::Config,
            ShareSyncError::SubscriptionNotFound(_) => ErrorCategory::NotFound,
            _ => ErrorCategory::Other,
        }
    }

    /// 是否应该重试
    pub fn should_retry(&self) -> bool {
        matches!(self.category(), ErrorCategory::Transient)
    }

    /// 面向用户的可读消息
    ///
    /// 与 `autobackup/error.rs::DiskSpaceFull::user_message` 风格对齐，但不复用
    /// 代码（两模块解耦，错误类型也独立）。
    pub fn user_message(&self) -> String {
        match self.category() {
            ErrorCategory::Quota => {
                "网盘空间不足，请清理后手动触发同步；本批次未提交的子项已标记为跳过".to_string()
            }
            ErrorCategory::LocalDiskFull => {
                "本地磁盘空间不足，请清理订阅目标目录后手动触发同步".to_string()
            }
            ErrorCategory::Transient => "网络临时异常，将在下次轮询时自动重试".to_string(),
            ErrorCategory::Auth => "分享链接鉴权失败或触发风控，请检查链接与提取码".to_string(),
            ErrorCategory::Config => "订阅配置存在错误，请编辑后重新触发".to_string(),
            ErrorCategory::NotFound => "分享资源不存在或已失效，请检查链接".to_string(),
            ErrorCategory::Other => self.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category() {
        let e = ShareSyncError::NetworkError("timeout".into());
        assert_eq!(e.category(), ErrorCategory::Transient);
        assert!(e.should_retry());

        let e = ShareSyncError::ShareLinkError("需要密码".into());
        assert_eq!(e.category(), ErrorCategory::Auth);
        assert!(!e.should_retry());

        let e = ShareSyncError::ConfigError("invalid".into());
        assert_eq!(e.category(), ErrorCategory::Config);

        let e = ShareSyncError::SubscriptionNotFound("xyz".into());
        assert_eq!(e.category(), ErrorCategory::NotFound);
    }

    #[test]
    fn test_transfer_error_with_errno() {
        let e = ShareSyncError::TransferError("errno=132 风控".into());
        assert_eq!(e.category(), ErrorCategory::Auth);

        let e = ShareSyncError::DownloadError("文件不存在".into());
        assert_eq!(e.category(), ErrorCategory::NotFound);
    }

    #[test]
    fn test_error_serialize_roundtrip() {
        let e = ShareSyncError::ShareLinkError("test".into());
        let json = serde_json::to_string(&e).unwrap();
        let back: ShareSyncError = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{:?}", e), format!("{:?}", back));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let e: ShareSyncError = io_err.into();
        assert!(matches!(e, ShareSyncError::FileSystemError(_)));
    }

    #[test]
    fn test_error_category_quota_detected() {
        let e = ShareSyncError::TransferError("转存失败：网盘空间不足".into());
        assert_eq!(e.category(), ErrorCategory::Quota);
        assert!(!e.should_retry());

        let e = ShareSyncError::DownloadError("errno=-32".into());
        assert_eq!(e.category(), ErrorCategory::Quota);

        // 兼容老中文 errmsg 透传
        let e = ShareSyncError::TransferError("网盘剩余空间不足".into());
        assert_eq!(e.category(), ErrorCategory::Quota);

        // 其它错误不应被误判为 Quota
        let e = ShareSyncError::TransferError("网盘 API 异常".into());
        assert_eq!(e.category(), ErrorCategory::Other);
    }

    #[test]
    fn test_error_category_local_disk_full_detected() {
        let e = ShareSyncError::FileSystemError("ENOSPC: no space left on device".into());
        assert_eq!(e.category(), ErrorCategory::LocalDiskFull);
        assert!(!e.should_retry());

        let e = ShareSyncError::FileSystemError("There is not enough space on the disk".into());
        assert_eq!(e.category(), ErrorCategory::LocalDiskFull);

        // 其它 FileSystemError 不应被误判
        let e = ShareSyncError::FileSystemError("Permission denied (os error 13)".into());
        assert_eq!(e.category(), ErrorCategory::Other);
    }

    #[test]
    fn test_user_message_for_quota_and_disk() {
        let e = ShareSyncError::TransferError("网盘空间不足".into());
        let msg = e.user_message();
        assert!(msg.contains("网盘空间不足"));
        assert!(msg.contains("跳过"));

        let e = ShareSyncError::FileSystemError("ENOSPC".into());
        let msg = e.user_message();
        assert!(msg.contains("本地磁盘"));
    }

    #[test]
    fn test_category_serde_renames_to_snake_case() {
        // 验证 Quota/LocalDiskFull 序列化为小写 snake_case，与现有命名风格一致
        let json = serde_json::to_string(&ErrorCategory::Quota).unwrap();
        assert_eq!(json, "\"quota\"");
        let json = serde_json::to_string(&ErrorCategory::LocalDiskFull).unwrap();
        assert_eq!(json, "\"local_disk_full\"");
    }
}
