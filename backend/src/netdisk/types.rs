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

// =====================================================
// Locate上传响应类型定义
// =====================================================

/// 上传服务器信息
#[derive(Debug, Deserialize, Clone)]
pub struct UploadServerInfo {
    /// 服务器地址（如 "https://c.pcs.baidu.com"）
    pub server: String,
}

/// Locate上传响应
///
/// 响应示例:
/// ```json
/// {
///   "error_code": 0,
///   "host": "c.pcs.baidu.com",
///   "servers": [{"server": "https://xafj-ct11.pcs.baidu.com"}, {"server": "https://c7.pcs.baidu.com"}],
///   "bak_servers": [{"server": "https://c.pcs.baidu.com"}],
///   "client_ip": "xxx.xxx.xxx.xxx",
///   "expire": 60
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct LocateUploadResponse {
    /// 错误码（0表示成功）
    #[serde(default)]
    pub error_code: i32,

    /// 主服务器主机名
    #[serde(default)]
    pub host: String,

    /// 主服务器列表（优先使用）
    #[serde(default)]
    pub servers: Vec<UploadServerInfo>,

    /// 备用服务器列表
    #[serde(default)]
    pub bak_servers: Vec<UploadServerInfo>,

    /// QUIC 服务器列表
    #[serde(default)]
    pub quic_servers: Vec<UploadServerInfo>,

    /// 客户端IP
    #[serde(default)]
    pub client_ip: String,

    /// 服务器列表有效期（秒）
    #[serde(default)]
    pub expire: i32,

    /// 错误信息
    #[serde(default)]
    pub error_msg: String,

    /// 请求ID
    #[serde(default)]
    pub request_id: u64,
}

impl LocateUploadResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.error_code == 0 && (!self.servers.is_empty() || !self.host.is_empty())
    }

    /// 获取所有服务器主机名列表（去除协议前缀，优先主服务器）
    ///
    /// 返回顺序：host > servers > bak_servers
    pub fn server_hosts(&self) -> Vec<String> {
        let mut hosts = Vec::new();

        // 1. 添加主服务器 host
        if !self.host.is_empty() {
            hosts.push(self.host.clone());
        }

        // 2. 添加 servers 列表（去重，只保留 https）
        for info in &self.servers {
            if info.server.starts_with("https://") {
                let host = info.server
                    .trim_start_matches("https://")
                    .trim_end_matches('/')
                    .to_string();
                if !hosts.contains(&host) {
                    hosts.push(host);
                }
            }
        }

        // 3. 添加备用服务器（去重，只保留 https）
        for info in &self.bak_servers {
            if info.server.starts_with("https://") {
                let host = info.server
                    .trim_start_matches("https://")
                    .trim_end_matches('/')
                    .to_string();
                if !hosts.contains(&host) {
                    hosts.push(host);
                }
            }
        }

        hosts
    }
}

// =====================================================
// 上传相关类型定义
// =====================================================

/// 预创建文件响应
#[derive(Debug, Deserialize)]
pub struct PrecreateResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 返回类型（1=普通上传，2=秒传成功）
    #[serde(default, rename = "return_type")]
    pub return_type: i32,

    /// 上传ID（用于后续分片上传）
    #[serde(default)]
    pub uploadid: String,

    /// 需要上传的分片序号列表（秒传或断点续传时可能部分分片已上传）
    #[serde(default)]
    pub block_list: Vec<i32>,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,
}

impl PrecreateResponse {
    /// 是否秒传成功
    pub fn is_rapid_upload(&self) -> bool {
        self.return_type == 2
    }

    /// 是否需要继续上传
    pub fn needs_upload(&self) -> bool {
        self.return_type == 1 && !self.uploadid.is_empty()
    }
}

/// 上传分片响应
#[derive(Debug, Deserialize)]
pub struct UploadChunkResponse {
    /// 错误码（0表示成功）
    #[serde(default)]
    pub error_code: i32,

    /// 分片 MD5
    #[serde(default)]
    pub md5: String,

    /// 请求ID
    #[serde(default)]
    pub request_id: u64,

    /// 错误信息
    #[serde(default)]
    pub error_msg: String,
}

impl UploadChunkResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.error_code == 0 && !self.md5.is_empty()
    }
}

/// 创建文件响应
#[derive(Debug, Deserialize)]
pub struct CreateFileResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 文件服务器ID
    #[serde(default, rename = "fs_id")]
    pub fs_id: u64,

    /// 文件 MD5
    #[serde(default)]
    pub md5: String,

    /// 服务器文件名
    #[serde(default)]
    pub server_filename: String,

    /// 文件路径
    #[serde(default)]
    pub path: String,

    /// 文件大小
    #[serde(default)]
    pub size: u64,

    /// 服务器创建时间
    #[serde(default)]
    pub ctime: i64,

    /// 服务器修改时间
    #[serde(default)]
    pub mtime: i64,

    /// 是否目录
    #[serde(default)]
    pub isdir: i32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,
}

impl CreateFileResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0 && self.fs_id > 0
    }
}

/// 秒传响应
#[derive(Debug, Deserialize)]
pub struct RapidUploadResponse {
    /// 错误码
    /// - 0: 秒传成功
    /// - 404: 文件不存在（需要普通上传）
    /// - 2: 参数错误
    /// - 31079: 校验失败（MD5不匹配）
    pub errno: i32,

    /// 文件服务器ID
    #[serde(default, rename = "fs_id")]
    pub fs_id: u64,

    /// 文件 MD5
    #[serde(default)]
    pub md5: String,

    /// 服务器文件名
    #[serde(default)]
    pub server_filename: String,

    /// 文件路径
    #[serde(default)]
    pub path: String,

    /// 文件大小
    #[serde(default)]
    pub size: u64,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,

    /// 返回信息
    #[serde(default)]
    pub info: String,
}

impl RapidUploadResponse {
    /// 是否秒传成功
    pub fn is_success(&self) -> bool {
        self.errno == 0 && self.fs_id > 0
    }

    /// 是否文件不存在（需要普通上传）
    pub fn file_not_exist(&self) -> bool {
        self.errno == 404
    }

    /// 是否校验失败（MD5不匹配）
    pub fn checksum_failed(&self) -> bool {
        self.errno == 31079
    }
}

/// 上传错误类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadErrorKind {
    /// 网络错误（可重试）
    Network,
    /// 超时（可重试）
    Timeout,
    /// 服务器错误（可重试）
    ServerError,
    /// 限流（可重试，需要更长等待时间）
    RateLimited,
    /// 文件不存在（不可重试）
    FileNotFound,
    /// 权限不足（不可重试）
    Forbidden,
    /// 参数错误（不可重试）
    BadRequest,
    /// 文件已存在（不可重试，但可能是秒传成功）
    FileExists,
    /// 空间不足（不可重试）
    QuotaExceeded,
    /// 未知错误
    Unknown,
}

impl UploadErrorKind {
    /// 是否可重试
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            UploadErrorKind::Network
                | UploadErrorKind::Timeout
                | UploadErrorKind::ServerError
                | UploadErrorKind::RateLimited
        )
    }

    /// 从百度 API errno 转换
    pub fn from_errno(errno: i32) -> Self {
        match errno {
            0 => UploadErrorKind::Unknown, // 成功不是错误
            -6 | -7 | -8 | -9 => UploadErrorKind::Network,
            -10 | -21 => UploadErrorKind::Timeout,
            -1 | -3 | -11 | 2 => UploadErrorKind::ServerError,
            31023 | 31024 => UploadErrorKind::RateLimited,
            31066 | 404 => UploadErrorKind::FileNotFound,
            -5 | 31062 | 31063 => UploadErrorKind::Forbidden,
            31061 | 31079 => UploadErrorKind::BadRequest,
            31190 => UploadErrorKind::FileExists,
            31064 | 31083 => UploadErrorKind::QuotaExceeded,
            _ => UploadErrorKind::Unknown,
        }
    }
}
