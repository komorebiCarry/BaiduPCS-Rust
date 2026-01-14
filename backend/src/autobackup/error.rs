//! 备份错误处理模块
//!
//! 提供统一的错误分类、重试策略和用户友好的错误消息

use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// 备份错误
#[derive(Debug, Error)]
pub enum BackupError {
    /// 配置错误
    #[error("配置错误: {0}")]
    ConfigError(String),

    /// 文件系统错误
    #[error("文件系统错误: {0}")]
    FileSystemError(String),

    /// 网络错误
    #[error("网络错误: {0}")]
    NetworkError(String),

    /// API 错误
    #[error("API 错误: {code} - {message}")]
    ApiError { code: i32, message: String },

    /// 加密错误
    #[error("加密错误: {0}")]
    EncryptionError(String),

    /// 解密错误
    #[error("解密错误: {0}")]
    DecryptionError(String),

    /// 去重检查错误
    #[error("去重检查错误: {0}")]
    DedupError(String),

    /// 数据库错误
    #[error("数据库错误: {0}")]
    DatabaseError(String),

    /// 任务取消
    #[error("任务已取消")]
    Cancelled,

    /// 任务被抢占
    #[error("任务被抢占")]
    Preempted,

    /// 资源不足
    #[error("资源不足: {0}")]
    ResourceExhausted(String),

    /// 超时
    #[error("操作超时: {0}")]
    Timeout(String),

    /// 权限错误
    #[error("权限错误: {0}")]
    PermissionDenied(String),

    /// 文件不存在
    #[error("文件不存在: {0}")]
    FileNotFound(String),

    /// 目录不存在
    #[error("目录不存在: {0}")]
    DirectoryNotFound(String),

    /// 磁盘空间不足
    #[error("磁盘空间不足")]
    DiskSpaceFull,

    /// 未知错误
    #[error("未知错误: {0}")]
    Unknown(String),
}

impl BackupError {
    /// 获取错误分类
    pub fn category(&self) -> ErrorCategory {
        classify_error(self)
    }

    /// 获取用户友好的错误消息
    pub fn user_message(&self) -> String {
        to_user_message(self)
    }

    /// 是否可重试
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.category(),
            ErrorCategory::Transient | ErrorCategory::RateLimited
        )
    }
}

/// 错误分类
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// 临时错误（可重试）
    Transient,
    /// 永久错误（不可重试）
    Permanent,
    /// 速率限制（需要等待后重试）
    RateLimited,
    /// 资源错误（需要用户干预）
    Resource,
    /// 配置错误（需要修改配置）
    Configuration,
    /// 权限错误（需要授权）
    Permission,
    /// 用户取消
    UserCancelled,
}

/// 重试策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// 最大重试次数
    pub max_retries: u32,
    /// 初始延迟（毫秒）
    pub initial_delay_ms: u64,
    /// 最大延迟（毫秒）
    pub max_delay_ms: u64,
    /// 延迟倍数（指数退避）
    pub backoff_multiplier: f64,
    /// 是否添加抖动
    pub add_jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
            add_jitter: true,
        }
    }
}

impl RetryPolicy {
    /// 创建新的重试策略
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    /// 不重试策略
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// 激进重试策略（用于临时错误）
    pub fn aggressive() -> Self {
        Self {
            max_retries: 5,
            initial_delay_ms: 500,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
            add_jitter: true,
        }
    }

    /// 保守重试策略（用于速率限制）
    pub fn conservative() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 5000,
            max_delay_ms: 120000,
            backoff_multiplier: 3.0,
            add_jitter: true,
        }
    }

    /// 计算第 n 次重试的延迟
    pub fn calculate_delay(&self, retry_count: u32) -> Duration {
        if retry_count == 0 {
            return Duration::from_millis(0);
        }

        let delay = self.initial_delay_ms as f64
            * self.backoff_multiplier.powi((retry_count - 1) as i32);
        let delay = delay.min(self.max_delay_ms as f64) as u64;

        let delay = if self.add_jitter {
            // 添加 ±25% 的抖动
            let jitter_range = delay / 4;
            let jitter = rand::random::<u64>() % (jitter_range * 2);
            delay.saturating_sub(jitter_range).saturating_add(jitter)
        } else {
            delay
        };

        Duration::from_millis(delay)
    }

    /// 是否应该重试
    pub fn should_retry(&self, retry_count: u32, error: &BackupError) -> bool {
        retry_count < self.max_retries && error.is_retryable()
    }
}

/// 根据错误类型获取推荐的重试策略
pub fn get_retry_policy(error: &BackupError) -> RetryPolicy {
    match error.category() {
        ErrorCategory::Transient => RetryPolicy::aggressive(),
        ErrorCategory::RateLimited => RetryPolicy::conservative(),
        _ => RetryPolicy::no_retry(),
    }
}

/// 错误分类函数
pub fn classify_error(error: &BackupError) -> ErrorCategory {
    match error {
        BackupError::NetworkError(_) => ErrorCategory::Transient,
        BackupError::Timeout(_) => ErrorCategory::Transient,
        BackupError::ApiError { code, .. } => {
            match *code {
                // 速率限制
                429 | 31034 => ErrorCategory::RateLimited,
                // 临时错误
                500..=599 => ErrorCategory::Transient,
                // 权限错误
                401 | 403 => ErrorCategory::Permission,
                // 其他视为永久错误
                _ => ErrorCategory::Permanent,
            }
        }
        BackupError::ConfigError(_) => ErrorCategory::Configuration,
        BackupError::PermissionDenied(_) => ErrorCategory::Permission,
        BackupError::FileNotFound(_) | BackupError::DirectoryNotFound(_) => ErrorCategory::Resource,
        BackupError::DiskSpaceFull => ErrorCategory::Resource,
        BackupError::ResourceExhausted(_) => ErrorCategory::Transient,
        BackupError::Cancelled | BackupError::Preempted => ErrorCategory::UserCancelled,
        BackupError::EncryptionError(_) | BackupError::DecryptionError(_) => {
            ErrorCategory::Permanent
        }
        BackupError::DatabaseError(_) => ErrorCategory::Permanent,
        BackupError::FileSystemError(_) => ErrorCategory::Resource,
        BackupError::DedupError(_) => ErrorCategory::Transient,
        BackupError::Unknown(_) => ErrorCategory::Permanent,
    }
}

/// 生成用户友好的错误消息
pub fn to_user_message(error: &BackupError) -> String {
    match error {
        BackupError::ConfigError(msg) => format!("配置有误：{}，请检查备份配置", msg),
        BackupError::FileSystemError(msg) => format!("文件操作失败：{}，请检查文件权限", msg),
        BackupError::NetworkError(_) => "网络连接失败，请检查网络后重试".to_string(),
        BackupError::ApiError { code, message } => {
            match *code {
                429 | 31034 => "请求过于频繁，请稍后再试".to_string(),
                401 => "登录已过期，请重新登录".to_string(),
                403 => "没有访问权限，请检查账号状态".to_string(),
                404 => "文件或目录不存在".to_string(),
                _ => format!("服务器错误 ({}): {}", code, message),
            }
        }
        BackupError::EncryptionError(_) => "文件加密失败，请检查加密配置".to_string(),
        BackupError::DecryptionError(_) => "文件解密失败，请确认密钥正确".to_string(),
        BackupError::DedupError(_) => "去重检查失败，将重新上传".to_string(),
        BackupError::DatabaseError(_) => "数据库操作失败，请重试".to_string(),
        BackupError::Cancelled => "任务已取消".to_string(),
        BackupError::Preempted => "任务被暂停，等待恢复".to_string(),
        BackupError::ResourceExhausted(msg) => format!("资源不足：{}", msg),
        BackupError::Timeout(_) => "操作超时，请重试".to_string(),
        BackupError::PermissionDenied(_) => "没有操作权限，请检查文件权限".to_string(),
        BackupError::FileNotFound(path) => format!("文件不存在：{}", path),
        BackupError::DirectoryNotFound(path) => format!("目录不存在：{}", path),
        BackupError::DiskSpaceFull => "磁盘空间不足，请清理后重试".to_string(),
        BackupError::Unknown(msg) => format!("未知错误：{}", msg),
    }
}

/// 从 anyhow::Error 转换
impl From<anyhow::Error> for BackupError {
    fn from(err: anyhow::Error) -> Self {
        // 尝试向下转换为具体错误类型
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return match io_err.kind() {
                std::io::ErrorKind::NotFound => {
                    BackupError::FileNotFound(io_err.to_string())
                }
                std::io::ErrorKind::PermissionDenied => {
                    BackupError::PermissionDenied(io_err.to_string())
                }
                std::io::ErrorKind::TimedOut => {
                    BackupError::Timeout(io_err.to_string())
                }
                _ => BackupError::FileSystemError(io_err.to_string()),
            };
        }

        BackupError::Unknown(err.to_string())
    }
}

/// 从 std::io::Error 转换
impl From<std::io::Error> for BackupError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => BackupError::FileNotFound(err.to_string()),
            std::io::ErrorKind::PermissionDenied => BackupError::PermissionDenied(err.to_string()),
            std::io::ErrorKind::TimedOut => BackupError::Timeout(err.to_string()),
            _ => BackupError::FileSystemError(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        let network_err = BackupError::NetworkError("connection refused".to_string());
        assert_eq!(network_err.category(), ErrorCategory::Transient);
        assert!(network_err.is_retryable());

        let config_err = BackupError::ConfigError("invalid path".to_string());
        assert_eq!(config_err.category(), ErrorCategory::Configuration);
        assert!(!config_err.is_retryable());

        let rate_limit_err = BackupError::ApiError {
            code: 429,
            message: "too many requests".to_string(),
        };
        assert_eq!(rate_limit_err.category(), ErrorCategory::RateLimited);
        assert!(rate_limit_err.is_retryable());
    }

    #[test]
    fn test_retry_policy_delay() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_multiplier: 2.0,
            add_jitter: false,
        };

        assert_eq!(policy.calculate_delay(0), Duration::from_millis(0));
        assert_eq!(policy.calculate_delay(1), Duration::from_millis(1000));
        assert_eq!(policy.calculate_delay(2), Duration::from_millis(2000));
        assert_eq!(policy.calculate_delay(3), Duration::from_millis(4000));
        assert_eq!(policy.calculate_delay(4), Duration::from_millis(8000));
        assert_eq!(policy.calculate_delay(5), Duration::from_millis(10000)); // capped
    }

    #[test]
    fn test_user_message() {
        let err = BackupError::DiskSpaceFull;
        assert_eq!(err.user_message(), "磁盘空间不足，请清理后重试");

        let err = BackupError::ApiError {
            code: 401,
            message: "unauthorized".to_string(),
        };
        assert_eq!(err.user_message(), "登录已过期，请重新登录");
    }
}
