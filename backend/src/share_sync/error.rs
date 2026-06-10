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
    /// v2 阶段 4 新增:目录粒度转存里"模糊失败" —— 百度对一组 fs_id 一次 transfer
    /// 调用一刀切返回 task_errno != 0,无法区分到底哪个子文件出问题。
    /// 触发条件:errno=-33(文件数超限)或某些不在 quota/auth/notfound 关键字
    /// 列表但有 task_errno 的失败。
    /// 行为:不重试同一组(否则一直撞),而是触发二分(`split_two`)把子树拆开
    /// 再各自试一次,递归到叶子(单文件)粒度。
    DirTransferAmbiguous,
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

/// 触发 `ErrorCategory::Transient` 判定的关键字。
///
/// 这些错误通常来自百度接口的临时服务端超时，可能被包在 `TransferError` /
/// `DownloadError` 里，而不是 `NetworkError`。
const TRANSIENT_KEYWORDS: &[&str] = &[
    "请求超时",
    "超时",
    "请稍后",
    "稍后再试",
    "timeout",
    "timed out",
    "temporarily",
    "temporary",
    "网络异常",
    "connection reset",
];

/// 触发 `ErrorCategory::DirTransferAmbiguous` 的关键字。
///
/// 这些错误意味着"百度对一组 fs_id 的转存整批失败,但无法精确归因到子文件" —
/// 重试同一组只会再撞同一面墙,正确做法是拆小再试(阶段 4 二分)。
/// - `-33`(文件数超限) — 百度对单次转存的 fs_id 总数有上限
/// - `task_errno=-33` — 同上,只是写法不同
const DIR_AMBIGUOUS_KEYWORDS: &[&str] = &["errno=-33", "task_errno=-33", "文件数量超出限制"];

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
                // 顺序敏感:先查资源/鉴权/不存在,再把明确的服务端临时错误归为可重试。
                // DirTransferAmbiguous 放在 quota 之后, transient 之前 — 它不是
                // 可重试也不是单纯的失败,但又必须触发二分,所以独占一档。
                if matches_any(msg, QUOTA_KEYWORDS) {
                    ErrorCategory::Quota
                } else if matches_any(msg, LOCAL_DISK_FULL_KEYWORDS) {
                    ErrorCategory::LocalDiskFull
                } else if msg.contains("132") || msg.contains("风控") {
                    ErrorCategory::Auth
                } else if msg.contains("不存在") || msg.contains("已失效") {
                    ErrorCategory::NotFound
                } else if matches_any(msg, DIR_AMBIGUOUS_KEYWORDS) {
                    ErrorCategory::DirTransferAmbiguous
                } else if matches_any(msg, TRANSIENT_KEYWORDS) {
                    ErrorCategory::Transient
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

    /// 是否触发"目录二分递归"(阶段 4 新增)
    ///
    /// 这三类失败有一个共同点:**重试同一组没用,但拆小可能成功**:
    /// - `Quota` / `LocalDiskFull`: 整组放不下,拆开后小目录可能放得下
    /// - `DirTransferAmbiguous`: 整组被一刀切失败但可能只是某个子文件挂了,
    ///   拆开后好的文件能继续转
    pub fn is_bisect_trigger(&self) -> bool {
        matches!(
            self.category(),
            ErrorCategory::Quota
                | ErrorCategory::LocalDiskFull
                | ErrorCategory::DirTransferAmbiguous
        )
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
            ErrorCategory::DirTransferAmbiguous => {
                "整目录转存被一刀切失败,将自动拆为子目录递归重试".to_string()
            }
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
    fn test_error_category_transient_detected() {
        let e = ShareSyncError::TransferError("请求超时，请稍后再试".into());
        assert_eq!(e.category(), ErrorCategory::Transient);
        assert!(e.should_retry());

        let e = ShareSyncError::DownloadError("operation timed out".into());
        assert_eq!(e.category(), ErrorCategory::Transient);

        let e = ShareSyncError::TransferError("风控，请稍后再试".into());
        assert_eq!(e.category(), ErrorCategory::Auth);
        assert!(!e.should_retry());
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

    #[test]
    fn test_dir_transfer_ambiguous_detected_for_errno_minus_33() {
        let e = ShareSyncError::TransferError("errno=-33 文件数量超出限制".into());
        assert_eq!(e.category(), ErrorCategory::DirTransferAmbiguous);
        assert!(e.is_bisect_trigger());
        assert!(!e.should_retry());

        let e = ShareSyncError::TransferError("task_errno=-33".into());
        assert_eq!(e.category(), ErrorCategory::DirTransferAmbiguous);

        let e = ShareSyncError::TransferError("文件数量超出限制".into());
        assert_eq!(e.category(), ErrorCategory::DirTransferAmbiguous);
    }

    #[test]
    fn test_is_bisect_trigger_covers_quota_and_disk() {
        let e = ShareSyncError::TransferError("网盘空间不足".into());
        assert!(e.is_bisect_trigger());

        let e = ShareSyncError::FileSystemError("ENOSPC".into());
        assert!(e.is_bisect_trigger());

        // 非 bisect 类失败
        let e = ShareSyncError::TransferError("errno=132 风控".into());
        assert!(!e.is_bisect_trigger());

        let e = ShareSyncError::TransferError("网盘 API 异常".into());
        assert!(!e.is_bisect_trigger());
    }
}
