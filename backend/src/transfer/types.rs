// 转存模块类型定义

use serde::{Deserialize, Serialize};

/// 分享链接解析结果
#[derive(Debug, Clone)]
pub struct ShareLink {
    /// surl 或短链 ID（如 "1abcDEFg"）
    pub short_key: String,
    /// 原始分享链接
    pub raw_url: String,
    /// 从链接中提取的密码（如有）
    pub password: Option<String>,
}

/// 分享页面信息（从页面 JS 提取）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharePageInfo {
    /// 分享 ID
    pub shareid: String,
    /// 分享者 UK
    pub uk: String,
    /// 分享 UK（可能与 uk 不同）
    pub share_uk: String,
    /// CSRF 令牌
    pub bdstoken: String,
}

/// 分享文件信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedFileInfo {
    /// 文件 fs_id
    pub fs_id: u64,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件路径
    pub path: String,
    /// 文件大小（目录为 0）
    pub size: u64,
    /// 文件名
    pub name: String,
}

/// 转存结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferResult {
    /// 是否成功
    pub success: bool,
    /// 转存后的文件路径列表
    pub transferred_paths: Vec<String>,
    /// 错误信息
    pub error: Option<String>,
    /// 转存后的文件 fs_id 列表
    pub transferred_fs_ids: Vec<u64>,
}

/// 分享链接状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareStatus {
    /// 正常可用
    Valid,
    /// 需要密码
    NeedPassword,
    /// 密码错误
    InvalidPassword,
    /// 分享已失效
    Expired,
    /// 分享不存在
    NotFound,
}

/// 转存错误类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// 需要提取码
    NeedPassword,
    /// 提取码错误
    InvalidPassword,
    /// 分享已失效
    ShareExpired,
    /// 分享不存在
    ShareNotFound,
    /// 同名文件已存在
    FileExists(String),
    /// 转存数量超限
    TransferLimitExceeded { current: u64, limit: u64 },
    /// 网络错误
    NetworkError(String),
    /// 解析错误
    ParseError(String),
    /// 其他错误
    Other(String),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::NeedPassword => write!(f, "需要提取码"),
            TransferError::InvalidPassword => write!(f, "提取码错误"),
            TransferError::ShareExpired => write!(f, "分享已失效"),
            TransferError::ShareNotFound => write!(f, "分享不存在"),
            TransferError::FileExists(name) => write!(f, "同名文件已存在: {}", name),
            TransferError::TransferLimitExceeded { current, limit } => {
                write!(f, "转存文件数 {} 超过上限 {}", current, limit)
            }
            TransferError::NetworkError(msg) => write!(f, "网络错误: {}", msg),
            TransferError::ParseError(msg) => write!(f, "解析错误: {}", msg),
            TransferError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for TransferError {}
