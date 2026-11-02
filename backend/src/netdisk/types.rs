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
///
/// 失败时百度可能把字段返回成 `null`，除 `errno` 外都按缺省值处理，
/// 否则整段解析失败，失败原因就丢了
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareSetResponse {
    /// 错误码（0表示成功）
    pub errno: i32,

    /// 分享链接
    #[serde(default, deserialize_with = "null_as_default")]
    pub link: String,

    /// 提取码
    #[serde(default, deserialize_with = "null_as_default")]
    pub pwd: String,

    /// 分享ID
    #[serde(default, deserialize_with = "null_as_default")]
    pub shareid: u64,

    /// 错误信息
    #[serde(default, deserialize_with = "null_as_default")]
    pub errmsg: String,

    /// 百度给用户看的提示文案（如账号受限时的原因），失败时常只有这个字段有内容
    #[serde(default, deserialize_with = "null_as_default")]
    pub show_msg: String,
}

/// 把 JSON `null` 当作缺省值
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

impl ShareSetResponse {
    /// 是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0 && !self.link.is_empty() && self.shareid > 0
    }

    /// 失败原因（不含「创建分享失败」前缀），末尾附 errno 便于排查
    ///
    /// 优先用百度的 `show_msg`（与网页端提示一致），其次已知错误码，再次 `errmsg`
    pub fn error_reason(&self) -> String {
        let show_msg = self.show_msg.trim();
        let errmsg = self.errmsg.trim();
        let reason = if !show_msg.is_empty() {
            show_msg.to_string()
        } else if let Some(known) = share_set_errno_reason(self.errno) {
            known.to_string()
        } else if !errmsg.is_empty() {
            errmsg.to_string()
        } else {
            "百度未返回具体原因。如果在百度网盘网页端分享同样失败，\
             可能是账号未绑定手机号等限制，请按网页端提示处理"
                .to_string()
        };
        format!("{}（errno={}）", reason, self.errno)
    }
}

/// 百度网盘网页端对创建分享常见错误码的提示
fn share_set_errno_reason(errno: i32) -> Option<&'static str> {
    Some(match errno {
        -6 => "登录已过期或凭证不完整，请重新登录",
        -70 => "文件中包含病毒或疑似病毒，无法分享",
        108 => "文件名包含敏感词，无法分享",
        110 => "今日分享次数已达上限",
        115 => "所选文件中有被百度禁止分享的文件",
        9100 | 9200 => "账号存在违规行为，已被冻结",
        9300 => "账号存在违规行为，分享功能暂被冻结",
        9400 => "账号异常，需在百度网盘网页端或 App 完成验证（如绑定手机号）后才能分享",
        9500 => "账号存在安全风险，已进入保护模式，请修改密码后再试",
        // 开放平台分享接口文档中的错误码，网页接口未必返回，保留以防万一
        13100 => "账号未绑定手机号，请先在百度网盘网页端或 App 绑定手机号后再分享",
        _ => return None,
    })
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

    /// 过期状态（-1=已过期, 0=有期限且未过期, 1=永久有效）
    ///
    /// 注意：这不是有效天数，原始有效天数见 `oriExpiredType`
    #[serde(rename = "expiredType", default)]
    pub expired_type: i32,

    /// 剩余有效秒数（不是时间戳；永久或已过期时为 0）
    #[serde(rename = "expiredTime", default)]
    pub expired_time: i64,

    /// 过期时间点（Unix 秒级时间戳；永久或已过期时为 0）
    #[serde(rename = "lastExpireTime", default)]
    pub last_expire_time: i64,

    /// 浏览次数
    #[serde(rename = "vCnt", default)]
    pub view_count: i32,

    /// 下载次数（`dCnt`，百度分享体系里与 `vCnt` 同组的计数字段，见分享页 / 公开分享列表响应）
    ///
    /// BaiduPCS-Go / BaiduPCS-Py 均未解析，`/share/record` 是否返回无公开文档；
    /// 响应里没有时为 `None`，前端不显示，不会臆造为 0。
    #[serde(rename = "dCnt", default, deserialize_with = "de_opt_count")]
    pub download_count: Option<i64>,

    /// 保存（转存）次数（`tCnt`），说明同 `download_count`
    #[serde(rename = "tCnt", default, deserialize_with = "de_opt_count")]
    pub transfer_count: Option<i64>,
}

/// 宽松解析可选计数：数字或数字字符串 → `Some`，其余（null / 空串 / 非数字 / 负数）→ `None`。
///
/// 这两个字段没有文档保证类型，解析失败不能连带整个分享列表失败。
fn de_opt_count<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let n = match value {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    };
    Ok(n.filter(|v| *v >= 0))
}

impl ShareRecord {
    /// 是否正常状态
    pub fn is_active(&self) -> bool {
        self.status == 0
    }

    /// 是否已过期
    pub fn is_expired(&self) -> bool {
        self.expired_type == -1
    }

    /// 是否永久有效
    pub fn is_permanent(&self) -> bool {
        !self.is_expired() && self.expire_at(0) == 0
    }

    /// 过期时间点（Unix 秒级时间戳），0 表示永久或已过期
    ///
    /// 优先使用 `lastExpireTime`；缺失时用 `now + 剩余秒数` 推算
    pub fn expire_at(&self, now: i64) -> i64 {
        if self.is_expired() {
            0
        } else if self.last_expire_time > 0 {
            self.last_expire_time
        } else if self.expired_time > 0 {
            now + self.expired_time
        } else {
            0
        }
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

/// `filemanager` 接口 copy/move 时目标已存在同名项的处理策略（百度 `ondup` 参数）
///
/// 实测（web 版 `api/filemanager`，async=2）：
/// - `ondup` 必须写在 `filelist` 的**每一项**里；放在表单或 URL 上时 `overwrite` 不生效
/// - 默认（`fail`）按项独立处理：不冲突的照常完成，冲突项原样保留，任务以
///   `task_errno=-30` 失败，`list` 中逐项给出 `error_code=-8`
/// - `newcopy`：新的一份自动改名为 `x(1).txt` / `dir(1)`
/// - `overwrite`：同名文件被替换；同名**文件夹被整个替换而不是合并**
/// - `skip` 不被识别，表现与 `fail` 相同，故不提供（「跳过」= 默认提交后不再处理冲突项）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnDup {
    /// 报错（百度默认行为）
    Fail,
    /// 保留两者：百度自动给新的一份改名
    Newcopy,
    /// 覆盖目标
    Overwrite,
}

impl OnDup {
    pub fn as_str(self) -> &'static str {
        match self {
            OnDup::Fail => "fail",
            OnDup::Newcopy => "newcopy",
            OnDup::Overwrite => "overwrite",
        }
    }
}

/// 发送给百度的 copy/move filelist 单项：`FileOperationItem` + 逐项 `ondup`
#[derive(Debug, Serialize)]
pub struct FileOperationItemWithOnDup<'a> {
    #[serde(flatten)]
    pub item: &'a FileOperationItem,
    pub ondup: OnDup,
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

/// `share/taskquery` 返回 `list` 中的原始单项
///
/// 成功时只有 `from` / `to`；失败时额外带 `error_code`（-8 = 目标已存在同名项）
/// 与 `from_meta` / `to_meta`。
#[derive(Debug, Clone, Deserialize)]
pub struct FileManagerTaskListEntry {
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    #[serde(default)]
    pub error_code: i32,
    #[serde(default)]
    pub from_meta: Option<FileManagerTaskEntryMeta>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileManagerTaskEntryMeta {
    #[serde(default, deserialize_with = "deserialize_u64_flexible")]
    pub isdir: u64,
}

/// 失败任务中的单项失败明细
#[derive(Debug, Clone, Serialize)]
pub struct FileOperationFailedItem {
    /// 源路径
    pub from: String,
    /// 目标路径
    pub to: String,
    /// 百度逐项错误码（-8 = 目标已存在同名项）
    pub error_code: i32,
    /// 源是否为文件夹
    pub isdir: bool,
}

impl From<FileManagerTaskListEntry> for FileOperationResultItem {
    fn from(e: FileManagerTaskListEntry) -> Self {
        Self {
            from: e.from,
            to: e.to,
        }
    }
}

impl From<FileManagerTaskListEntry> for FileOperationFailedItem {
    fn from(e: FileManagerTaskListEntry) -> Self {
        Self {
            isdir: e.from_meta.map(|m| m.isdir == 1).unwrap_or(false),
            from: e.from,
            to: e.to,
            error_code: e.error_code,
        }
    }
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
    /// 完成时为 from/to 列表；失败时为逐项失败明细（带 error_code）
    #[serde(default)]
    pub list: Vec<FileManagerTaskListEntry>,
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
    /// taskquery `status="failed"` 时的逐项失败明细；未列出的项已成功处理
    #[serde(default)]
    pub failed_items: Vec<FileOperationFailedItem>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(json: &str) -> ShareRecord {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn share_record_expiry_uses_baidu_semantics() {
        // 7 天有效期：expiredTime 是剩余秒数，lastExpireTime 才是过期时间点
        let timed = record(
            r#"{"shareId":1,"status":0,"public":0,"expiredType":0,"expiredTime":604797,"lastExpireTime":1792397064,"oriExpiredType":7}"#,
        );
        assert!(!timed.is_expired());
        assert!(!timed.is_permanent());
        assert_eq!(timed.expire_at(1791792267), 1792397064);

        // 缺少 lastExpireTime 时按剩余秒数推算
        let timed_no_last =
            record(r#"{"shareId":2,"status":0,"public":0,"expiredType":0,"expiredTime":604797}"#);
        assert_eq!(timed_no_last.expire_at(1000), 1000 + 604797);

        let permanent = record(
            r#"{"shareId":3,"status":0,"public":0,"expiredType":1,"expiredTime":0,"lastExpireTime":0,"oriExpiredType":0}"#,
        );
        assert!(permanent.is_permanent());
        assert_eq!(permanent.expire_at(1000), 0);

        let expired = record(
            r#"{"shareId":4,"status":0,"public":0,"expiredType":-1,"expiredTime":0,"lastExpireTime":0,"oriExpiredType":7}"#,
        );
        assert!(expired.is_expired());
        assert!(!expired.is_permanent());
        assert_eq!(expired.expire_at(1000), 0);
    }

    #[test]
    fn share_record_optional_counts() {
        // 没有 dCnt / tCnt：保持 None（前端不显示），不影响其它字段
        let plain = record(r#"{"shareId":1,"status":0,"public":0,"vCnt":5}"#);
        assert_eq!(plain.view_count, 5);
        assert_eq!(plain.download_count, None);
        assert_eq!(plain.transfer_count, None);

        // 数字
        let num = record(r#"{"shareId":1,"status":0,"public":0,"vCnt":356,"dCnt":29,"tCnt":184}"#);
        assert_eq!(num.download_count, Some(29));
        assert_eq!(num.transfer_count, Some(184));

        // 数字字符串
        let text = record(r#"{"shareId":1,"status":0,"public":0,"dCnt":"7","tCnt":" 3 "}"#);
        assert_eq!(text.download_count, Some(7));
        assert_eq!(text.transfer_count, Some(3));

        // 异常值不拖垮整条记录
        let odd = record(r#"{"shareId":1,"status":0,"public":0,"dCnt":null,"tCnt":"abc"}"#);
        assert_eq!(odd.download_count, None);
        assert_eq!(odd.transfer_count, None);
        let neg = record(r#"{"shareId":1,"status":0,"public":0,"dCnt":-1,"tCnt":{}}"#);
        assert_eq!(neg.download_count, None);
        assert_eq!(neg.transfer_count, None);
    }

    #[test]
    fn share_set_error_reason_prefers_show_msg() {
        let resp = |errno: i32, errmsg: &str, show_msg: &str| ShareSetResponse {
            errno,
            link: String::new(),
            pwd: String::new(),
            shareid: 0,
            errmsg: errmsg.to_string(),
            show_msg: show_msg.to_string(),
        };
        // show_msg 优先于已知错误码和 errmsg
        assert_eq!(
            resp(115, "fail", "请先绑定手机号").error_reason(),
            "请先绑定手机号（errno=115）"
        );
        assert!(resp(115, "", "").error_reason().contains("禁止分享"));
        assert!(resp(110, "", " ").error_reason().contains("上限"));
        assert_eq!(resp(42, "boom", "").error_reason(), "boom（errno=42）");
        let unknown = resp(42, "", "").error_reason();
        assert!(unknown.contains("手机号") && unknown.ends_with("（errno=42）"));
    }

    #[test]
    fn share_set_response_parses_show_msg() {
        let resp: ShareSetResponse =
            serde_json::from_str(r#"{"errno":42,"show_msg":"原因"}"#).unwrap();
        assert_eq!(resp.show_msg, "原因");
        assert!(resp.errmsg.is_empty());

        // 失败时字段为 null 也要能解析出原因
        let resp: ShareSetResponse = serde_json::from_str(
            r#"{"errno":42,"link":null,"pwd":null,"shareid":null,"errmsg":null,"show_msg":"原因"}"#,
        )
        .unwrap();
        assert_eq!(resp.error_reason(), "原因（errno=42）");
        assert_eq!(resp.shareid, 0);

        let resp: ShareSetResponse =
            serde_json::from_str(r#"{"errno":13100,"show_msg":null}"#).unwrap();
        assert!(resp.error_reason().contains("绑定手机号"));
    }
}
