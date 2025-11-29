// 文件系统模块数据类型定义

use serde::{Deserialize, Serialize};
use std::path::Path;

// 重新导出配置模块中的 FilesystemConfig
pub use crate::config::FilesystemConfig;

/// 文件系统错误码
/// 错误码范围：50001 - 50099
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsErrorCode {
    /// 路径不在白名单
    PathNotAllowed = 50001,
    /// 目录不存在
    DirectoryNotFound = 50002,
    /// 权限不足
    PermissionDenied = 50003,
    /// 符号链接拒绝
    SymlinkRejected = 50004,
    /// 目录读取失败
    DirectoryReadFailed = 50005,
    /// 路径格式无效
    InvalidPathFormat = 50006,
    /// 路径穿越攻击
    PathTraversalDetected = 50007,
    /// 文件不存在
    FileNotFound = 50008,
    /// 不是目录
    NotADirectory = 50009,
    /// 不是文件
    NotAFile = 50010,
}

impl FsErrorCode {
    pub fn code(&self) -> i32 {
        *self as i32
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::PathNotAllowed => "路径不在允许访问的范围内",
            Self::DirectoryNotFound => "目录不存在",
            Self::PermissionDenied => "没有权限访问该路径",
            Self::SymlinkRejected => "不允许访问符号链接",
            Self::DirectoryReadFailed => "读取目录失败",
            Self::InvalidPathFormat => "路径格式无效",
            Self::PathTraversalDetected => "检测到路径穿越攻击",
            Self::FileNotFound => "文件不存在",
            Self::NotADirectory => "指定路径不是目录",
            Self::NotAFile => "指定路径不是文件",
        }
    }
}

/// 文件系统错误
#[derive(Debug)]
pub struct FsError {
    pub code: FsErrorCode,
    pub message: String,
    pub path: Option<String>,
}

impl FsError {
    pub fn new(code: FsErrorCode) -> Self {
        Self {
            message: code.message().to_string(),
            code,
            path: None,
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref path) = self.path {
            write!(f, "{}: {}", self.message, path)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for FsError {}

/// 条目类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    File,
    Directory,
}

/// 文件条目
#[derive(Debug, Clone, Serialize)]
pub struct FileEntry {
    /// 唯一标识（路径哈希）
    pub id: String,
    /// 文件名
    pub name: String,
    /// 条目类型
    #[serde(rename = "entryType")]
    pub entry_type: EntryType,
    /// 文件大小（文件夹为 None）
    pub size: Option<u64>,
    /// 创建时间 (ISO8601)
    #[serde(rename = "createdAt")]
    pub created_at: String,
    /// 修改时间 (ISO8601)
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    /// 可选图标建议
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// 完整路径
    pub path: String,
}

/// 排序字段
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortField {
    #[default]
    Name,
    CreatedAt,
    UpdatedAt,
    Size,
}

/// 排序顺序
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// 列目录请求参数（支持分页）
#[derive(Debug, Deserialize)]
pub struct ListRequest {
    /// 目录路径
    pub path: String,
    /// 页码，从 0 开始
    #[serde(default)]
    pub page: usize,
    /// 每页数量，默认 100
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// 排序字段
    #[serde(default)]
    pub sort_field: SortField,
    /// 排序顺序
    #[serde(default)]
    pub sort_order: SortOrder,
}

fn default_page_size() -> usize {
    100
}

/// 列目录响应（支持分页）
#[derive(Debug, Serialize)]
pub struct ListResponse {
    /// 文件条目列表
    pub entries: Vec<FileEntry>,
    /// 当前路径
    #[serde(rename = "currentPath")]
    pub current_path: String,
    /// 父目录路径
    #[serde(rename = "parentPath")]
    pub parent_path: Option<String>,
    /// 总条目数
    pub total: usize,
    /// 当前页码
    pub page: usize,
    /// 每页数量
    #[serde(rename = "pageSize")]
    pub page_size: usize,
    /// 是否还有更多
    #[serde(rename = "hasMore")]
    pub has_more: bool,
}

/// 路径跳转请求
#[derive(Debug, Deserialize)]
pub struct GotoRequest {
    /// 目标路径（支持绝对路径）
    pub path: String,
}

/// 路径跳转响应
#[derive(Debug, Serialize)]
pub struct GotoResponse {
    /// 路径是否有效
    pub valid: bool,
    /// 解析后的绝对路径
    #[serde(rename = "resolvedPath")]
    pub resolved_path: String,
    /// 目标类型
    #[serde(rename = "entryType")]
    pub entry_type: Option<EntryType>,
    /// 错误信息
    pub message: Option<String>,
}

/// 路径校验请求
#[derive(Debug, Deserialize)]
pub struct ValidateRequest {
    /// 路径
    pub path: String,
    /// 期望的条目类型
    #[serde(rename = "type")]
    pub entry_type: Option<EntryType>,
}

/// 路径校验响应
#[derive(Debug, Serialize)]
pub struct ValidateResponse {
    /// 是否有效
    pub valid: bool,
    /// 是否存在
    pub exists: bool,
    /// 条目类型
    #[serde(rename = "entryType")]
    pub entry_type: Option<EntryType>,
    /// 错误信息
    pub message: Option<String>,
}

/// 获取文件扩展名对应的图标建议
pub fn get_icon_for_extension(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_str()?.to_lowercase();
    let icon = match ext.as_str() {
        // 图片
        "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "ico" => "image",
        // 视频
        "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm" => "video",
        // 音频
        "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" => "audio",
        // 文档
        "pdf" => "pdf",
        "doc" | "docx" => "word",
        "xls" | "xlsx" => "excel",
        "ppt" | "pptx" => "powerpoint",
        "txt" | "md" | "rtf" => "text",
        // 压缩包
        "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" => "archive",
        // 代码
        "rs" | "js" | "ts" | "py" | "java" | "c" | "cpp" | "h" | "go" | "rb" | "php" => "code",
        "html" | "htm" | "css" | "scss" | "less" => "web",
        "json" | "xml" | "yaml" | "yml" | "toml" => "config",
        // 可执行
        "exe" | "msi" | "bat" | "cmd" | "sh" => "executable",
        _ => return None,
    };
    Some(icon.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fs_error_code() {
        assert_eq!(FsErrorCode::PathNotAllowed.code(), 50001);
        assert_eq!(FsErrorCode::DirectoryNotFound.code(), 50002);
        assert_eq!(FsErrorCode::PathTraversalDetected.code(), 50007);
    }

    #[test]
    fn test_fs_error() {
        let err = FsError::new(FsErrorCode::PathNotAllowed)
            .with_path("/etc/passwd");
        assert_eq!(err.code, FsErrorCode::PathNotAllowed);
        assert!(err.path.is_some());
    }

    #[test]
    fn test_icon_detection() {
        assert_eq!(get_icon_for_extension(Path::new("test.jpg")), Some("image".to_string()));
        assert_eq!(get_icon_for_extension(Path::new("video.mp4")), Some("video".to_string()));
        assert_eq!(get_icon_for_extension(Path::new("code.rs")), Some("code".to_string()));
        assert_eq!(get_icon_for_extension(Path::new("unknown.xyz")), None);
    }
}