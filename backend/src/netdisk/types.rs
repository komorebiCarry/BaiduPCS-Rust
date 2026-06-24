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

/// 搜索响应
#[derive(Debug, Deserialize)]
pub struct SearchResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 文件列表
    #[serde(default)]
    pub list: Vec<FileItem>,

    /// 是否还有更多
    #[serde(default)]
    pub has_more: i32,

    /// 内容列表（某些接口用 contentlist）
    #[serde(default)]
    pub contentlist: Vec<FileItem>,
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

    /// 服务器创建时间（秒级时间戳，/api/create 返回）
    #[serde(default)]
    pub ctime: i64,

    /// 服务器修改时间（秒级时间戳，/api/create 返回）
    #[serde(default)]
    pub mtime: i64,

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
            -9..=-6 => UploadErrorKind::Network,
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
/// 百度风控系统返回的验证信息，当 errno=132 时可能附带此字段。
///
/// 字段全部为 `Option<String>` 以兼容百度返回中字段缺失或 `null` 的情况；
/// `extra` 通过 `#[serde(flatten)]` 接住未知字段，保证未来新字段不会导致解析失败。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthWidget {
    /// 安全随机数
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saferand: Option<String>,
    /// 安全签名（敏感，日志中需脱敏）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safesign: Option<String>,
    /// 安全模板
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safetpl: Option<String>,
    /// 其它未识别字段透传（百度协议演化时不破坏解析）
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
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
    pub fn failure_with_errno(
        error: String,
        errno: i32,
        authwidget: Option<AuthWidget>,
        verify_scene: Option<i32>,
    ) -> Self {
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

    /// 下载地址（仅当 filemetas 请求 dlink=1 时返回；有效期约 8 小时）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlink: Option<String>,

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

// =====================================================
// 文件管理操作（filemanager: copy / move / rename）相关类型
// =====================================================

/// 自定义 deserializer：兼容百度返回 `taskid` / `request_id` 等字段为 number 或 string 的两种形态
///
/// 百度网盘部分接口在不同版本下会把同一字段在 number 与 string 间漂移；本函数提供一致的 i64 输出。
fn deserialize_i64_flexible<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::Number(n) => Ok(n.as_i64().unwrap_or(0)),
        serde_json::Value::String(s) => Ok(s.parse::<i64>().unwrap_or(0)),
        serde_json::Value::Null => Ok(0),
        _ => Ok(0),
    }
}

/// 自定义 deserializer：兼容 `request_id` 等 u64 字段的 number/string 两种形态
fn deserialize_u64_flexible<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::Number(n) => Ok(n.as_u64().unwrap_or(0)),
        serde_json::Value::String(s) => Ok(s.parse::<u64>().unwrap_or(0)),
        serde_json::Value::Null => Ok(0),
        _ => Ok(0),
    }
}

/// `filemanager` 接口 `opera=copy/move` 时 `filelist` JSON 数组中的单条 item
///
/// 百度协议形如：`[{"path":"/a/x.mp4","dest":"/b","newname":"x.mp4"}, ...]`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOperationItem {
    /// 源文件路径（绝对路径，必须以 `/` 开头）
    pub path: String,
    /// 目标父目录（绝对路径，不带尾斜杠）
    pub dest: String,
    /// 目标文件名（与源同名 = 直接复制/移动；不同 = 复制/移动并重命名）
    pub newname: String,
}

/// `filemanager` 接口 `opera=rename` 时 `filelist` JSON 数组中的单条 item
///
/// 百度协议形如：`[{"path":"/a/x.mp4","newname":"y.mp4","id":123456789}]`
///
/// 注意：`id` 必须是数字（fs_id），不能是字符串。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameItem {
    /// 源文件路径
    pub path: String,
    /// 新文件名
    pub newname: String,
    /// 文件系统 ID（fs_id）
    pub id: u64,
}

/// `share/taskquery` 完成时返回的单条结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOperationResultItem {
    /// 源路径
    #[serde(default)]
    pub from: String,
    /// 目标路径
    #[serde(default)]
    pub to: String,
}

/// `filemanager` 接口同步返回（POST 后立即得到）的响应体
///
/// 在 `async=2` 模式下，POST 仅返回 `taskid`；详细成功/失败由后续 `share/taskquery` 决定。
#[derive(Debug, Deserialize)]
pub struct FileManagerResponse {
    /// 错误码（0 表示请求被接受，不等于业务最终成功）
    #[serde(default)]
    pub errno: i32,
    /// 异步任务 ID（兼容 number / string）
    #[serde(default, deserialize_with = "deserialize_i64_flexible")]
    pub taskid: i64,
    /// 请求 ID（仅日志用，兼容 number / string）
    #[serde(default, deserialize_with = "deserialize_u64_flexible")]
    pub request_id: u64,
    /// 错误描述（百度有时把错误放在 `errmsg`）
    #[serde(default)]
    pub errmsg: String,
    /// 风控验证组件（errno=132 时可能存在）
    #[serde(default)]
    pub authwidget: Option<AuthWidget>,
    /// 验证场景（风控相关）
    #[serde(default)]
    pub verify_scene: Option<i32>,
}

/// `share/taskquery` 接口返回的响应体
#[derive(Debug, Deserialize)]
pub struct FileManagerTaskQueryResponse {
    /// 百度 taskquery 响应 body 中 `errno` 不一定总是存在（running/pending 下可能缺失）。
    /// 补 `#[serde(default)]` 将缺失视为 0，提高对百度字段波动的容忍度。
    #[serde(default)]
    pub errno: i32,
    /// 任务级错误码（status="failed" 时填充）
    #[serde(default)]
    pub task_errno: i32,
    /// "running" / "success" / "failed" / "pending"
    #[serde(default)]
    pub status: String,
    /// 0..100，仅 running 时有意义；百度此字段在不同版本可能为 number 或 string，故用 Value 接住
    #[serde(default)]
    pub progress: serde_json::Value,
    /// 完成时返回 from/to 列表
    #[serde(default)]
    pub list: Vec<FileOperationResultItem>,
    /// 完成项总数
    #[serde(default)]
    pub total: u32,
    /// 用户可读的失败提示
    #[serde(default)]
    pub show_msg: String,
    /// taskquery 阶段也可能返回错误描述
    #[serde(default)]
    pub errmsg: String,
    /// taskquery 阶段也可能遭遇 132 风控，透传 authwidget
    #[serde(default)]
    pub authwidget: Option<AuthWidget>,
    /// 风控场景
    #[serde(default)]
    pub verify_scene: Option<i32>,
}

/// 文件管理操作的统一对外结果（成功路径）
///
/// 仅在内部代码路径中表示「成功」分支，由 handler 转换为 `FileOperationOutcomeDto::Success`。
/// **不含 `success` 字段** —— 业务成败完全由 `FileOperationOutcome` enum / DTO 的 `kind` 表示。
#[derive(Debug, Clone, Serialize)]
pub struct FileOperationSuccess {
    /// 百度异步任务 ID（仅作日志/排查用）
    pub taskid: i64,
    /// 实际处理的文件总数
    pub total: u32,
    /// 完成项目（from -> to）
    pub list: Vec<FileOperationResultItem>,
}

/// 文件管理操作的失败载荷（内部类型，由 handler 转换为 DTO::Failed）
///
/// `errno` 与 `task_errno` 的语义：
/// - POST `filemanager` 阶段失败：`errno = Some(resp.errno)`、`task_errno = None`
/// - taskquery body 本身 `errno != 0`：`errno = task_errno = Some(parsed.errno)`（同步填值，便于 retry 判断）
/// - taskquery `status="failed"`：`errno = task_errno = Some(parsed.task_errno)`（同步填值）
#[derive(Debug, Clone, Serialize)]
pub struct FileOperationErrorPayload {
    /// 百度异步任务 ID（POST 失败时为 0）
    pub taskid: i64,
    /// POST filemanager 的 errno（taskquery 阶段也会同步写入该字段）
    pub errno: Option<i32>,
    /// taskquery 的 task_errno（POST 阶段为 None）
    pub task_errno: Option<i32>,
    /// 风控验证组件
    pub authwidget: Option<AuthWidget>,
    /// 风控场景
    pub verify_scene: Option<i32>,
    /// 60 次轮询仍未完成（仅 taskquery 阶段使用）
    #[serde(default)]
    pub still_running: bool,
}

/// 文件管理操作的统一内部 enum
#[derive(Debug, Clone, Serialize)]
pub enum FileOperationOutcome {
    /// 业务成功
    Success(FileOperationSuccess),
    /// 业务失败（含风控、同名冲突、超时等所有非系统层错误）
    ///
    /// `message` 是用户可读的提示（来自 `show_msg` / `errmsg` 兜底默认串）。
    Failed {
        message: String,
        payload: FileOperationErrorPayload,
    },
}
