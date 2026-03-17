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
                let host = info
                    .server
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
                let host = info
                    .server
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


// =====================================================
// 分享相关类型定义
// =====================================================

/// 创建分享响应
///
/// 百度网盘 API: POST /share/pset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareSetResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 分享链接
    #[serde(default)]
    pub link: String,

    /// 提取码
    #[serde(default)]
    pub pwd: String,

    /// 分享ID
    #[serde(default)]
    pub shareid: u64,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,
}

impl ShareSetResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0 && !self.link.is_empty() && self.shareid > 0
    }
}

/// 取消分享响应
///
/// 百度网盘 API: POST /share/cancel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareCancelResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,
}

impl ShareCancelResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0
    }
}

/// 分享记录
///
/// 分享列表中的单条记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareRecord {
    /// 分享ID
    #[serde(rename = "shareId")]
    pub share_id: u64,

    /// 文件ID列表
    #[serde(rename = "fsIds", default)]
    pub fs_ids: Vec<i64>,

    /// 短链接
    #[serde(default)]
    pub shortlink: String,

    /// 状态（0=正常, 其他=异常）
    pub status: i32,

    /// 是否公开（0=私密, 1=公开）
    pub public: i32,

    /// 文件类型
    #[serde(rename = "typicalCategory", default)]
    pub typical_category: i32,

    /// 文件路径
    #[serde(rename = "typicalPath", default)]
    pub typical_path: String,

    /// 过期类型（0=永久, 1=1天, 7=7天, 30=30天）
    #[serde(rename = "expiredType", default)]
    pub expired_type: i32,

    /// 过期时间戳（0表示永久）
    #[serde(rename = "expiredTime", default)]
    pub expired_time: i64,

    /// 浏览次数
    #[serde(rename = "vCnt", default)]
    pub view_count: i32,
}

impl ShareRecord {
    /// 是否正常状态
    pub fn is_active(&self) -> bool {
        self.status == 0
    }

    /// 是否永久有效
    pub fn is_permanent(&self) -> bool {
        self.expired_type == 0 || self.expired_time == 0
    }

    /// 获取文件名（从路径中提取）
    pub fn filename(&self) -> &str {
        self.typical_path
            .rsplit('/')
            .next()
            .unwrap_or(&self.typical_path)
    }
}

/// 分享列表响应
///
/// 百度网盘 API: GET /share/record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareListResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 分享记录列表
    #[serde(default)]
    pub list: Vec<ShareRecord>,

    /// 总数
    #[serde(default)]
    pub total: u32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,
}

impl ShareListResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0
    }
}

/// 分享详情响应（ShareSURLInfo）
///
/// 百度网盘 API: GET /share/surlinfoinrecord
/// 用于获取分享的提取码等详细信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareSURLInfoResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 提取码（注意：值为"0"时表示无密码，需要转换为空字符串）
    #[serde(default)]
    pub pwd: String,

    /// 短链接
    #[serde(default)]
    pub shorturl: String,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,
}

impl ShareSURLInfoResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0
    }

    /// 获取实际的提取码
    /// 当 pwd 为 "0" 时表示无密码，返回空字符串
    pub fn actual_pwd(&self) -> &str {
        if self.pwd == "0" {
            ""
        } else {
            &self.pwd
        }
    }
}


// =====================================================
// 删除文件相关类型定义
// =====================================================

/// 风控验证组件信息
///
/// 百度风控系统返回的验证信息，当 errno=132 时可能附带此字段
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthWidget {
    /// 安全随机数
    #[serde(default)]
    pub saferand: String,
    /// 安全签名
    #[serde(default)]
    pub safesign: String,
    /// 安全模板
    #[serde(default)]
    pub safetpl: String,
}

/// 删除文件响应
///
/// 百度网盘 API: POST https://pcs.baidu.com/rest/2.0/pcs/file?method=delete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFilesResponse {
    /// 是否全部成功
    pub success: bool,
    /// 错误信息（如果有）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 删除失败的路径列表
    #[serde(default)]
    pub failed_paths: Vec<String>,
    /// 成功删除的数量
    pub deleted_count: usize,
    /// API 错误码
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errno: Option<i32>,
    /// 风控验证组件（errno=132 时可能存在）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authwidget: Option<AuthWidget>,
    /// 验证场景（风控相关）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_scene: Option<i32>,
}

impl DeleteFilesResponse {
    /// 创建成功响应
    pub fn success(deleted_count: usize) -> Self {
        Self {
            success: true,
            error: None,
            failed_paths: Vec::new(),
            deleted_count,
            errno: Some(0),
            authwidget: None,
            verify_scene: None,
        }
    }

    /// 创建部分成功响应
    pub fn partial_success(deleted_count: usize, failed_paths: Vec<String>) -> Self {
        Self {
            success: false,
            error: Some(format!("部分文件删除失败: {} 个", failed_paths.len())),
            failed_paths,
            deleted_count,
            errno: None,
            authwidget: None,
            verify_scene: None,
        }
    }

    /// 创建失败响应
    pub fn failure(error: String) -> Self {
        Self {
            success: false,
            error: Some(error),
            failed_paths: Vec::new(),
            deleted_count: 0,
            errno: None,
            authwidget: None,
            verify_scene: None,
        }
    }

    /// 创建带 errno 的失败响应
    pub fn failure_with_errno(error: String, errno: i32, authwidget: Option<AuthWidget>, verify_scene: Option<i32>) -> Self {
        Self {
            success: false,
            error: Some(error),
            failed_paths: Vec::new(),
            deleted_count: 0,
            errno: Some(errno),
            authwidget,
            verify_scene,
        }
    }
}

/// 删除文件 API 原始响应
///
/// 百度 PCS API 返回的原始 JSON 格式
#[derive(Debug, Deserialize)]
pub struct DeleteFilesApiResponse {
    /// 错误码（0表示成功）
    #[serde(default)]
    pub errno: i32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,

    /// 请求ID
    #[serde(default)]
    pub request_id: u64,

    /// 风控验证组件（errno=132 时可能存在）
    #[serde(default)]
    pub authwidget: Option<AuthWidget>,

    /// 验证场景（风控相关）
    #[serde(default)]
    pub verify_scene: Option<i32>,
}

impl DeleteFilesApiResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0
    }
}

/// 文件元信息（包含 block_list）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetaInfo {
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

    /// MD5（仅文件有效）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,

    /// block_list（分片MD5列表，JSON字符串格式）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_list: Option<String>,

    /// 服务器创建时间
    pub server_ctime: i64,

    /// 服务器修改时间
    pub server_mtime: i64,
}

/// 文件元信息响应
#[derive(Debug, Deserialize)]
pub struct FileMetasResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 错误信息
    #[serde(default)]
    pub errmsg: String,

    /// 文件元信息列表
    #[serde(default)]
    pub list: Vec<FileMetaInfo>,
}

impl FileMetasResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0
    }
}
