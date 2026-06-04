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
    /// 其他
    Other,
}

impl ShareSyncError {
    /// 错误分类
    pub fn category(&self) -> ErrorCategory {
        match self {
            ShareSyncError::NetworkError(_) => ErrorCategory::Transient,
            ShareSyncError::ShareLinkError(_) => ErrorCategory::Auth,
            ShareSyncError::TransferError(msg) | ShareSyncError::DownloadError(msg) => {
                // errno=132 风控/限速，归为 Auth（用户介入）
                if msg.contains("132") || msg.contains("风控") {
                    ErrorCategory::Auth
                } else if msg.contains("不存在") || msg.contains("已失效") {
                    ErrorCategory::NotFound
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
}
