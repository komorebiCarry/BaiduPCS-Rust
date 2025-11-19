// 网盘API数据类型

use serde::{Deserialize, Serialize};

/// 文件信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileItem {
    /// 文件服务器ID
    #[serde(rename = "fs_id")]
    pub fs_id: u64,

    /// 文件路径
    pub path: String,

    /// 服务器文件名
    pub server_filename: String,

    /// 文件大小（字节）
    pub size: u64,

    /// 是否是目录 (0=文件, 1=目录)
    pub isdir: i32,

    /// 文件类别
    pub category: i32,

    /// MD5（仅文件有效）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,

    /// 服务器创建时间
    pub server_ctime: i64,

    /// 服务器修改时间
    pub server_mtime: i64,

    /// 本地创建时间
    pub local_ctime: i64,

    /// 本地修改时间
    pub local_mtime: i64,
}

impl FileItem {
    /// 是否是目录
    pub fn is_directory(&self) -> bool {
        self.isdir == 1
    }

    /// 是否是文件
    pub fn is_file(&self) -> bool {
        self.isdir == 0
    }

    /// 获取文件名（不含路径）
    pub fn filename(&self) -> &str {
        &self.server_filename
    }
}

/// 文件列表响应
#[derive(Debug, Deserialize)]
pub struct FileListResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,

    /// 文件列表
    #[serde(default)]
    pub list: Vec<FileItem>,

    /// GUID（全局唯一标识）
    #[serde(default)]
    pub guid: i64,

    /// GUID信息
    #[serde(default, rename = "guid_info")]
    pub guid_info: String,
}

/// 下载链接信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadUrl {
    /// 下载URL
    pub url: String,

    /// 链接优先级（越小越优先）
    #[serde(default)]
    pub rank: i32,

    /// 文件大小
    #[serde(default)]
    pub size: u64,
}

/// Locate下载响应
#[derive(Debug, Deserialize)]
pub struct LocateDownloadResponse {
    /// 错误码
    pub errno: i32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,

    /// 文件信息列表
    #[serde(default)]
    pub list: Vec<LocateFileInfo>,
}

/// Locate文件信息
#[derive(Debug, Deserialize)]
pub struct LocateFileInfo {
    /// 文件服务器ID
    #[serde(rename = "fs_id")]
    pub fs_id: u64,

    /// 文件路径
    pub path: String,

    /// 下载链接列表
    #[serde(default)]
    pub dlink: Vec<DownloadUrl>,
}

impl LocateFileInfo {
    /// 获取最优下载链接
    pub fn best_download_url(&self) -> Option<&DownloadUrl> {
        self.dlink.iter().min_by_key(|url| url.rank)
    }
}
