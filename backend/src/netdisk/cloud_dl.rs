//! 离线下载（Cloud Download）数据类型定义
//!
//! 本模块定义了百度网盘离线下载功能相关的数据结构，包括：
//! - 任务状态枚举
//! - 任务信息结构体
//! - 请求/响应类型
//! - 自动下载配置

use serde::{Deserialize, Serialize};

// =====================================================
// 任务状态枚举
// =====================================================

/// 离线下载任务状态
///
/// 状态码对应百度网盘 API 返回的 status 字段
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum CloudDlTaskStatus {
    /// 下载成功
    Success = 0,
    /// 下载进行中
    Running = 1,
    /// 系统错误
    SystemError = 2,
    /// 资源不存在
    ResourceNotFound = 3,
    /// 下载超时
    Timeout = 4,
    /// 资源存在但下载失败
    DownloadFailed = 5,
    /// 存储空间不足
    InsufficientSpace = 6,
    /// 任务取消
    Cancelled = 7,
}

impl CloudDlTaskStatus {
    /// 从 i32 状态码转换为枚举
    ///
    /// 未知状态码默认返回 SystemError
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Success,
            1 => Self::Running,
            2 => Self::SystemError,
            3 => Self::ResourceNotFound,
            4 => Self::Timeout,
            5 => Self::DownloadFailed,
            6 => Self::InsufficientSpace,
            7 => Self::Cancelled,
            _ => Self::SystemError,
        }
    }

    /// 获取状态的中文描述文本
    pub fn to_text(&self) -> &'static str {
        match self {
            Self::Success => "下载成功",
            Self::Running => "下载进行中",
            Self::SystemError => "系统错误",
            Self::ResourceNotFound => "资源不存在",
            Self::Timeout => "下载超时",
            Self::DownloadFailed => "下载失败",
            Self::InsufficientSpace => "存储空间不足",
            Self::Cancelled => "已取消",
        }
    }

    /// 判断任务是否已完成（成功或失败）
    pub fn is_finished(&self) -> bool {
        !matches!(self, Self::Running)
    }

    /// 判断任务是否成功
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    /// 判断任务是否失败
    pub fn is_failed(&self) -> bool {
        matches!(
            self,
            Self::SystemError
                | Self::ResourceNotFound
                | Self::Timeout
                | Self::DownloadFailed
                | Self::InsufficientSpace
                | Self::Cancelled
        )
    }
}

impl Default for CloudDlTaskStatus {
    fn default() -> Self {
        Self::Running
    }
}

// =====================================================
// 文件信息结构体
// =====================================================

/// 离线下载文件信息
///
/// 表示离线下载任务中的单个文件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudDlFileInfo {
    /// 文件名
    pub file_name: String,
    /// 文件大小（字节）
    pub file_size: i64,
}

// =====================================================
// 任务信息结构体
// =====================================================

/// 离线下载任务信息
///
/// 包含任务的完整状态和元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudDlTaskInfo {
    /// 任务唯一标识
    pub task_id: i64,
    /// 任务状态码 (0-7)
    pub status: i32,
    /// 状态文本描述
    pub status_text: String,
    /// 文件总大小（字节）
    pub file_size: i64,
    /// 已下载大小（字节）
    pub finished_size: i64,
    /// 创建时间戳（秒）
    pub create_time: i64,
    /// 开始时间戳（秒）
    pub start_time: i64,
    /// 完成时间戳（秒，未完成时为 0）
    pub finish_time: i64,
    /// 网盘保存路径
    pub save_path: String,
    /// 下载源链接
    pub source_url: String,
    /// 任务名称（通常是文件名）
    pub task_name: String,
    /// 离线下载类型（0=普通，其他值表示特殊类型）
    pub od_type: i32,
    /// 文件列表
    pub file_list: Vec<CloudDlFileInfo>,
    /// 结果码
    pub result: i32,
}

impl CloudDlTaskInfo {
    /// 获取任务状态枚举
    pub fn get_status(&self) -> CloudDlTaskStatus {
        CloudDlTaskStatus::from_i32(self.status)
    }

    /// 计算下载进度百分比
    ///
    /// 返回 0.0 - 100.0 之间的值
    pub fn progress_percent(&self) -> f32 {
        if self.file_size <= 0 {
            return 0.0;
        }
        ((self.finished_size as f64 / self.file_size as f64) * 100.0) as f32
    }

    /// 判断任务是否已完成
    pub fn is_finished(&self) -> bool {
        self.get_status().is_finished()
    }

    /// 判断任务是否成功
    pub fn is_success(&self) -> bool {
        self.get_status().is_success()
    }
}

// =====================================================
// 请求类型
// =====================================================

/// 默认保存路径
fn default_save_path() -> String {
    "/".to_string()
}

/// 添加离线下载任务请求
#[derive(Debug, Clone, Deserialize)]
pub struct AddTaskRequest {
    /// 下载源链接（支持 HTTP/HTTPS/磁力链接/ed2k）
    pub source_url: String,
    /// 网盘保存路径（默认为根目录 "/"）
    #[serde(default = "default_save_path")]
    pub save_path: String,
    /// 是否启用自动下载到本地
    #[serde(default)]
    pub auto_download: bool,
    /// 本地下载目录（自动下载时使用）
    pub local_download_path: Option<String>,
    /// 完成时是否询问下载目录
    #[serde(default)]
    pub ask_download_path: bool,
}

/// 查询任务请求
#[derive(Debug, Clone, Deserialize)]
pub struct QueryTaskRequest {
    /// 任务 ID 列表
    pub task_ids: Vec<i64>,
}

/// 任务列表请求参数
#[derive(Debug, Clone, Deserialize)]
pub struct ListTaskRequest {
    /// 起始位置（默认 0）
    #[serde(default)]
    pub start: u32,
    /// 返回数量限制（默认 1000）
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// 状态过滤（255 表示所有状态）
    #[serde(default = "default_status_filter")]
    pub status: u32,
}

fn default_limit() -> u32 {
    1000
}

fn default_status_filter() -> u32 {
    255
}

// =====================================================
// 响应类型
// =====================================================

/// 添加任务响应
#[derive(Debug, Clone, Serialize)]
pub struct AddTaskResponse {
    /// 新创建的任务 ID
    pub task_id: i64,
}

/// 任务列表响应
#[derive(Debug, Clone, Serialize)]
pub struct TaskListResponse {
    /// 任务列表
    pub tasks: Vec<CloudDlTaskInfo>,
}

/// 清空任务响应
#[derive(Debug, Clone, Serialize)]
pub struct ClearTasksResponse {
    /// 清空的任务数量
    pub total: i32,
}

/// 通用操作响应
#[derive(Debug, Clone, Serialize)]
pub struct OperationResponse {
    /// 操作是否成功
    pub success: bool,
    /// 可选的消息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl OperationResponse {
    /// 创建成功响应
    pub fn success() -> Self {
        Self {
            success: true,
            message: None,
        }
    }

    /// 创建带消息的成功响应
    pub fn success_with_message(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: Some(message.into()),
        }
    }

    /// 创建失败响应
    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
        }
    }
}

// =====================================================
// 自动下载配置
// =====================================================

/// 自动下载配置
///
/// 用于配置离线下载完成后自动下载到本地的行为
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoDownloadConfig {
    /// 关联的任务 ID
    pub task_id: i64,
    /// 是否启用自动下载
    pub enabled: bool,
    /// 本地下载目录（为空时使用默认目录）
    pub local_path: Option<String>,
    /// 完成时是否每次询问下载目录
    pub ask_each_time: bool,
}

impl AutoDownloadConfig {
    /// 创建新的自动下载配置
    pub fn new(task_id: i64) -> Self {
        Self {
            task_id,
            enabled: false,
            local_path: None,
            ask_each_time: false,
        }
    }

    /// 创建启用的自动下载配置
    pub fn enabled(task_id: i64, local_path: Option<String>, ask_each_time: bool) -> Self {
        Self {
            task_id,
            enabled: true,
            local_path,
            ask_each_time,
        }
    }
}

// =====================================================
// 百度 API 原始响应类型（用于解析）
// =====================================================

/// 百度 API 添加任务原始响应
#[derive(Debug, Deserialize)]
pub struct BaiduAddTaskResponse {
    /// errno 字段在成功时可能不存在，默认为 0
    #[serde(default)]
    pub errno: i32,
    #[serde(default)]
    pub task_id: i64,
    #[serde(default)]
    pub errmsg: String,
    /// 秒传标识（1 表示秒传成功）
    #[serde(default)]
    pub rapid_download: i32,
    /// 请求 ID
    #[serde(default)]
    pub request_id: i64,
    /// 错误码（百度 API 有时用 error_code 而不是 errno）
    #[serde(default)]
    pub error_code: i32,
    /// 错误消息（百度 API 有时用 error_msg 而不是 errmsg）
    #[serde(default)]
    pub error_msg: String,
    /// 用户可见的错误提示
    #[serde(default)]
    pub show_msg: String,
}

impl BaiduAddTaskResponse {
    /// 获取实际的错误码（优先使用 error_code，其次 errno）
    pub fn get_error_code(&self) -> i32 {
        if self.error_code != 0 {
            self.error_code
        } else {
            self.errno
        }
    }

    /// 获取实际的错误消息
    pub fn get_error_msg(&self) -> String {
        if !self.show_msg.is_empty() {
            self.show_msg.clone()
        } else if !self.error_msg.is_empty() {
            self.error_msg.clone()
        } else {
            self.errmsg.clone()
        }
    }

    /// 检查是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0 && self.error_code == 0
    }
}

/// 百度 API 任务列表原始响应
#[derive(Debug, Deserialize)]
pub struct BaiduListTaskResponse {
    /// errno 字段在成功时可能不存在，默认为 0
    #[serde(default)]
    pub errno: i32,
    /// task_info 可能是数组或空对象，使用自定义反序列化处理
    #[serde(default, deserialize_with = "deserialize_task_info_list")]
    pub task_info: Vec<BaiduTaskInfo>,
    #[serde(default)]
    pub errmsg: String,
    /// 任务总数
    #[serde(default)]
    pub total: i32,
    /// 请求 ID
    #[serde(default)]
    pub request_id: i64,
}

/// 自定义反序列化器：处理 task_info 可能是数组或空对象的情况
fn deserialize_task_info_list<'de, D>(deserializer: D) -> Result<Vec<BaiduTaskInfo>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct TaskInfoVisitor;

    impl<'de> Visitor<'de> for TaskInfoVisitor {
        type Value = Vec<BaiduTaskInfo>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an array of task info or an empty object")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut tasks = Vec::new();
            while let Some(task) = seq.next_element()? {
                tasks.push(task);
            }
            Ok(tasks)
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: de::MapAccess<'de>,
        {
            // 空对象 {} 或者是 map 格式的任务列表
            // 尝试消费所有键值对，如果是空的就返回空数组
            let mut tasks = Vec::new();
            while let Some((key, value)) = map.next_entry::<String, BaiduTaskInfo>()? {
                // 如果是 map 格式，key 是 task_id
                let mut task = value;
                if task.task_id.is_empty() {
                    task.task_id = key;
                }
                tasks.push(task);
            }
            Ok(tasks)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            // null 值
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_any(TaskInfoVisitor)
}

/// 百度 API 任务信息（原始格式，字段为字符串）
#[derive(Debug, Deserialize)]
pub struct BaiduTaskInfo {
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub file_size: String,
    #[serde(default)]
    pub finished_size: String,
    #[serde(default)]
    pub create_time: String,
    #[serde(default)]
    pub start_time: String,
    #[serde(default)]
    pub finish_time: String,
    #[serde(default)]
    pub save_path: String,
    #[serde(default)]
    pub source_url: String,
    #[serde(default)]
    pub task_name: String,
    #[serde(default)]
    pub od_type: String,
    #[serde(default)]
    pub file_list: Vec<BaiduFileInfo>,
    #[serde(default)]
    pub result: i32,
    // 额外字段（API 可能返回）
    #[serde(default)]
    pub rate_limit: String,
    #[serde(default)]
    pub timeout: String,
    #[serde(default)]
    pub callback: String,
    #[serde(default)]
    pub orgin_web_url: String,
}

/// 百度 API 文件信息（原始格式）
#[derive(Debug, Deserialize)]
pub struct BaiduFileInfo {
    #[serde(default)]
    pub file_name: String,
    #[serde(default)]
    pub file_size: String,
}

impl BaiduTaskInfo {
    /// 转换为内部任务信息格式
    pub fn into_task_info(self) -> CloudDlTaskInfo {
        let status: i32 = self.status.parse().unwrap_or(2);
        let status_enum = CloudDlTaskStatus::from_i32(status);

        CloudDlTaskInfo {
            task_id: self.task_id.parse().unwrap_or(0),
            status,
            status_text: status_enum.to_text().to_string(),
            file_size: self.file_size.parse().unwrap_or(0),
            finished_size: self.finished_size.parse().unwrap_or(0),
            create_time: self.create_time.parse().unwrap_or(0),
            start_time: self.start_time.parse().unwrap_or(0),
            finish_time: self.finish_time.parse().unwrap_or(0),
            save_path: self.save_path,
            source_url: self.source_url,
            task_name: self.task_name,
            od_type: self.od_type.parse().unwrap_or(0),
            file_list: self
                .file_list
                .into_iter()
                .map(|f| CloudDlFileInfo {
                    file_name: f.file_name,
                    file_size: f.file_size.parse().unwrap_or(0),
                })
                .collect(),
            result: self.result,
        }
    }
}

/// 百度 API 查询任务原始响应
#[derive(Debug, Deserialize)]
pub struct BaiduQueryTaskResponse {
    /// 错误码（支持 errno 和 error_code 两种字段名）
    #[serde(default, alias = "error_code")]
    pub errno: i32,
    #[serde(default)]
    pub task_info: serde_json::Value,
    #[serde(default)]
    pub errmsg: String,
}

/// 百度 API 清空任务原始响应
#[derive(Debug, Deserialize)]
pub struct BaiduClearTaskResponse {
    pub errno: i32,
    #[serde(default)]
    pub total: i32,
    #[serde(default)]
    pub errmsg: String,
}

/// 百度 API 通用操作响应
#[derive(Debug, Deserialize)]
pub struct BaiduOperationResponse {
    /// errno 字段在成功时可能不存在，默认为 0
    #[serde(default)]
    pub errno: i32,
    #[serde(default)]
    pub errmsg: String,
    /// 请求 ID
    #[serde(default)]
    pub request_id: i64,
    /// 错误码（百度 API 有时用 error_code 而不是 errno）
    #[serde(default)]
    pub error_code: i32,
    /// 错误消息（百度 API 有时用 error_msg 而不是 errmsg）
    #[serde(default)]
    pub error_msg: String,
    /// 用户可见的错误提示
    #[serde(default)]
    pub show_msg: String,
}

impl BaiduOperationResponse {
    /// 获取实际的错误码（优先使用 error_code，其次 errno）
    pub fn get_error_code(&self) -> i32 {
        if self.error_code != 0 {
            self.error_code
        } else {
            self.errno
        }
    }

    /// 获取实际的错误消息（优先使用 show_msg，其次 error_msg，最后 errmsg）
    pub fn get_error_msg(&self) -> String {
        if !self.show_msg.is_empty() {
            self.show_msg.clone()
        } else if !self.error_msg.is_empty() {
            self.error_msg.clone()
        } else {
            self.errmsg.clone()
        }
    }

    /// 检查是否成功
    pub fn is_success(&self) -> bool {
        self.errno == 0 && self.error_code == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_from_i32() {
        assert_eq!(CloudDlTaskStatus::from_i32(0), CloudDlTaskStatus::Success);
        assert_eq!(CloudDlTaskStatus::from_i32(1), CloudDlTaskStatus::Running);
        assert_eq!(CloudDlTaskStatus::from_i32(2), CloudDlTaskStatus::SystemError);
        assert_eq!(
            CloudDlTaskStatus::from_i32(3),
            CloudDlTaskStatus::ResourceNotFound
        );
        assert_eq!(CloudDlTaskStatus::from_i32(4), CloudDlTaskStatus::Timeout);
        assert_eq!(
            CloudDlTaskStatus::from_i32(5),
            CloudDlTaskStatus::DownloadFailed
        );
        assert_eq!(
            CloudDlTaskStatus::from_i32(6),
            CloudDlTaskStatus::InsufficientSpace
        );
        assert_eq!(CloudDlTaskStatus::from_i32(7), CloudDlTaskStatus::Cancelled);
        // 未知状态码应返回 SystemError
        assert_eq!(CloudDlTaskStatus::from_i32(8), CloudDlTaskStatus::SystemError);
        assert_eq!(
            CloudDlTaskStatus::from_i32(-1),
            CloudDlTaskStatus::SystemError
        );
    }

    #[test]
    fn test_status_to_text() {
        assert_eq!(CloudDlTaskStatus::Success.to_text(), "下载成功");
        assert_eq!(CloudDlTaskStatus::Running.to_text(), "下载进行中");
        assert_eq!(CloudDlTaskStatus::SystemError.to_text(), "系统错误");
        assert_eq!(CloudDlTaskStatus::ResourceNotFound.to_text(), "资源不存在");
        assert_eq!(CloudDlTaskStatus::Timeout.to_text(), "下载超时");
        assert_eq!(CloudDlTaskStatus::DownloadFailed.to_text(), "下载失败");
        assert_eq!(CloudDlTaskStatus::InsufficientSpace.to_text(), "存储空间不足");
        assert_eq!(CloudDlTaskStatus::Cancelled.to_text(), "已取消");
    }

    #[test]
    fn test_status_is_finished() {
        assert!(CloudDlTaskStatus::Success.is_finished());
        assert!(!CloudDlTaskStatus::Running.is_finished());
        assert!(CloudDlTaskStatus::SystemError.is_finished());
        assert!(CloudDlTaskStatus::Cancelled.is_finished());
    }

    #[test]
    fn test_task_info_progress() {
        let task = CloudDlTaskInfo {
            task_id: 1,
            status: 1,
            status_text: "下载进行中".to_string(),
            file_size: 1000,
            finished_size: 500,
            create_time: 0,
            start_time: 0,
            finish_time: 0,
            save_path: "/".to_string(),
            source_url: "http://example.com/file.zip".to_string(),
            task_name: "file.zip".to_string(),
            od_type: 0,
            file_list: vec![],
            result: 0,
        };

        assert!((task.progress_percent() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_task_info_progress_zero_size() {
        let task = CloudDlTaskInfo {
            task_id: 1,
            status: 1,
            status_text: "下载进行中".to_string(),
            file_size: 0,
            finished_size: 0,
            create_time: 0,
            start_time: 0,
            finish_time: 0,
            save_path: "/".to_string(),
            source_url: "http://example.com/file.zip".to_string(),
            task_name: "file.zip".to_string(),
            od_type: 0,
            file_list: vec![],
            result: 0,
        };

        assert_eq!(task.progress_percent(), 0.0);
    }

    #[test]
    fn test_add_task_request_default() {
        let json = r#"{"source_url": "http://example.com/file.zip"}"#;
        let req: AddTaskRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.source_url, "http://example.com/file.zip");
        assert_eq!(req.save_path, "/");
        assert!(!req.auto_download);
        assert!(req.local_download_path.is_none());
        assert!(!req.ask_download_path);
    }

    #[test]
    fn test_auto_download_config() {
        let config = AutoDownloadConfig::enabled(123, Some("/downloads".to_string()), false);
        assert_eq!(config.task_id, 123);
        assert!(config.enabled);
        assert_eq!(config.local_path, Some("/downloads".to_string()));
        assert!(!config.ask_each_time);
    }

    #[test]
    fn test_list_task_response_with_array() {
        // 正常的数组格式
        let json = r#"{"errno": 0, "task_info": [{"task_id": "123", "status": "1", "task_name": "test.zip"}]}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0);
        assert_eq!(resp.task_info.len(), 1);
        assert_eq!(resp.task_info[0].task_id, "123");
    }

    #[test]
    fn test_list_task_response_with_empty_array() {
        // 空数组
        let json = r#"{"errno": 0, "task_info": []}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0);
        assert!(resp.task_info.is_empty());
    }

    #[test]
    fn test_list_task_response_with_empty_object() {
        // 空对象（百度 API 有时返回这种格式）
        let json = r#"{"errno": 0, "task_info": {}}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0);
        assert!(resp.task_info.is_empty());
    }

    #[test]
    fn test_list_task_response_with_map_format() {
        // map 格式（key 是 task_id）
        let json = r#"{"errno": 0, "task_info": {"123": {"task_id": "123", "status": "0", "task_name": "test.zip"}}}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0);
        assert_eq!(resp.task_info.len(), 1);
        assert_eq!(resp.task_info[0].task_id, "123");
    }

    #[test]
    fn test_list_task_response_without_task_info() {
        // 没有 task_info 字段
        let json = r#"{"errno": 0}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0);
        assert!(resp.task_info.is_empty());
    }

    #[test]
    fn test_list_task_response_with_null_task_info() {
        // task_info 为 null
        let json = r#"{"errno": 0, "task_info": null}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0);
        assert!(resp.task_info.is_empty());
    }

    #[test]
    fn test_list_task_response_without_errno() {
        // 成功响应可能没有 errno 字段
        let json = r#"{"task_info":[{"task_id":"123","status":"0","task_name":"test.zip"}],"total":1,"request_id":12345}"#;
        let resp: BaiduListTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.errno, 0); // 默认值
        assert_eq!(resp.task_info.len(), 1);
        assert_eq!(resp.total, 1);
        assert_eq!(resp.request_id, 12345);
    }
}
