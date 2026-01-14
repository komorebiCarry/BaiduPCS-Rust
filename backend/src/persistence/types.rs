//! 持久化模块核心类型定义
//!
//! 定义任务持久化所需的所有数据结构

use bit_set::BitSet;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::warn;

/// 任务持久化状态
///
/// 统一的任务状态枚举，用于持久化和历史归档
/// 使用 snake_case 序列化以便 JSON 可读
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPersistenceStatus {
    /// 等待中（任务已创建，等待开始）
    Pending,
    /// 下载中
    Downloading,
    /// 上传中
    Uploading,
    /// 转存中
    Transferring,
    /// 已暂停
    Paused,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

impl TaskPersistenceStatus {
    /// 是否为终态（完成或失败）
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }

    /// 是否为活跃状态（正在执行）
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Downloading | Self::Uploading | Self::Transferring
        )
    }

    /// 从任务类型推断初始状态
    pub fn initial_for(_task_type: TaskType) -> Self {
        Self::Pending
    }

    /// 从任务类型推断活跃状态
    pub fn active_for(task_type: TaskType) -> Self {
        match task_type {
            TaskType::Download => Self::Downloading,
            TaskType::Upload => Self::Uploading,
            TaskType::Transfer => Self::Transferring,
        }
    }
}

impl std::fmt::Display for TaskPersistenceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Downloading => write!(f, "downloading"),
            Self::Uploading => write!(f, "uploading"),
            Self::Transferring => write!(f, "transferring"),
            Self::Paused => write!(f, "paused"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// 任务类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    /// 下载任务
    Download,
    /// 上传任务
    Upload,
    /// 转存任务
    Transfer,
}

impl TaskType {
    /// 获取任务类型的显示名称
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskType::Download => "download",
            TaskType::Upload => "upload",
            TaskType::Transfer => "transfer",
        }
    }
}

/// 任务元数据
///
/// 保存任务的基本信息，用于恢复时重建任务
/// 以 JSON 格式存储在 .meta 文件中
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetadata {
    /// 任务 ID
    pub task_id: String,

    /// 任务类型
    pub task_type: TaskType,

    /// 创建时间
    pub created_at: DateTime<Utc>,

    /// 最后更新时间
    pub updated_at: DateTime<Utc>,

    // === 下载任务字段 ===
    /// 百度网盘文件 fs_id（下载任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_id: Option<u64>,

    /// 关联的转存任务 ID（如果此下载任务由转存任务自动创建）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_task_id: Option<String>,

    /// 远程文件路径（下载任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_path: Option<String>,

    /// 本地保存路径（下载任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_path: Option<PathBuf>,

    /// 文件大小（字节）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,

    /// 分片大小（字节）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u64>,

    /// 总分片数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_chunks: Option<usize>,

    // === 上传任务字段 ===
    /// 本地文件路径（上传任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,

    /// 远程目标路径（上传任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,

    /// 上传 ID（百度网盘 precreate 返回）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_id: Option<String>,

    /// 上传 ID 创建时间（用于判断是否过期）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_id_created_at: Option<DateTime<Utc>>,

    // === 转存任务字段 ===
    /// 分享链接
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_link: Option<String>,

    /// 提取码
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_pwd: Option<String>,

    /// 转存目标路径
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_target_path: Option<String>,

    /// 转存状态（checking_share, transferring, transferred, downloading, completed）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_status: Option<String>,

    /// 关联的下载任务 ID 列表（转存后下载）
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub download_task_ids: Vec<String>,

    /// 转存成功后的分享信息（JSON 序列化）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_info_json: Option<String>,

    /// 是否开启自动下载
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_download: Option<bool>,

    /// 转存文件名称（用于展示，从分享文件列表中提取主要文件名）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_file_name: Option<String>,

    // === 文件夹下载组信息 ===
    /// 文件夹下载组ID（单文件下载时为 None）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,

    /// 文件夹根路径，如 "/电影"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_root: Option<String>,

    /// 相对于根文件夹的路径，如 "科幻片/星际穿越.mp4"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,

    // === 历史归档字段 ===
    /// 任务状态（使用枚举类型，提供类型安全）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskPersistenceStatus>,

    /// 完成时间（仅已完成任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,

    /// 错误信息（任务失败时）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_msg: Option<String>,

    // === 自动备份字段 ===
    /// 是否为备份任务
    #[serde(default)]
    pub is_backup: bool,

    /// 关联的备份配置 ID（备份任务时使用）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_config_id: Option<String>,

    // === 加密相关字段 ===
    /// 是否启用加密（上传任务）
    #[serde(default, skip_serializing_if = "is_false")]
    pub encrypt_enabled: bool,

    /// 是否为加密文件（下载任务）
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_encrypted: bool,

    /// 加密时使用的密钥版本（上传/下载任务）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_key_version: Option<u32>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl TaskMetadata {
    /// 创建下载任务元数据
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `fs_id` - 百度网盘文件 fs_id
    /// * `remote_path` - 远程文件路径
    /// * `local_path` - 本地保存路径
    /// * `file_size` - 文件大小（字节）
    /// * `chunk_size` - 分片大小（字节）
    /// * `total_chunks` - 总分片数
    /// * `is_encrypted` - 是否为加密文件（可选）
    /// * `encryption_key_version` - 加密密钥版本（可选）
    pub fn new_download(
        task_id: String,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        file_size: u64,
        chunk_size: u64,
        total_chunks: usize,
        is_encrypted: Option<bool>,
        encryption_key_version: Option<u32>,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            task_type: TaskType::Download,
            created_at: now,
            updated_at: now,
            fs_id: Some(fs_id),
            transfer_task_id: None,
            remote_path: Some(remote_path),
            local_path: Some(local_path),
            file_size: Some(file_size),
            chunk_size: Some(chunk_size),
            total_chunks: Some(total_chunks),
            source_path: None,
            target_path: None,
            upload_id: None,
            upload_id_created_at: None,
            share_link: None,
            share_pwd: None,
            transfer_target_path: None,
            transfer_status: None,
            download_task_ids: vec![],
            share_info_json: None,
            auto_download: None,
            transfer_file_name: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            status: Some(TaskPersistenceStatus::Pending),
            completed_at: None,
            error_msg: None,
            is_backup: false,
            backup_config_id: None,
            // 加密字段
            encrypt_enabled: false,
            is_encrypted: is_encrypted.unwrap_or(false),
            encryption_key_version,
        }
    }

    /// 创建下载备份任务元数据
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `fs_id` - 百度网盘文件 fs_id
    /// * `remote_path` - 远程文件路径
    /// * `local_path` - 本地保存路径
    /// * `file_size` - 文件大小（字节）
    /// * `chunk_size` - 分片大小（字节）
    /// * `total_chunks` - 总分片数
    /// * `backup_config_id` - 备份配置 ID
    /// * `is_encrypted` - 是否为加密文件（可选）
    /// * `encryption_key_version` - 加密密钥版本（可选）
    pub fn new_download_backup(
        task_id: String,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        file_size: u64,
        chunk_size: u64,
        total_chunks: usize,
        backup_config_id: String,
        is_encrypted: Option<bool>,
        encryption_key_version: Option<u32>,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            task_type: TaskType::Download,
            created_at: now,
            updated_at: now,
            fs_id: Some(fs_id),
            transfer_task_id: None,
            remote_path: Some(remote_path),
            local_path: Some(local_path),
            file_size: Some(file_size),
            chunk_size: Some(chunk_size),
            total_chunks: Some(total_chunks),
            source_path: None,
            target_path: None,
            upload_id: None,
            upload_id_created_at: None,
            share_link: None,
            share_pwd: None,
            transfer_target_path: None,
            transfer_status: None,
            download_task_ids: vec![],
            share_info_json: None,
            auto_download: None,
            transfer_file_name: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            status: Some(TaskPersistenceStatus::Pending),
            completed_at: None,
            error_msg: None,
            is_backup: true,
            backup_config_id: Some(backup_config_id),
            // 加密字段
            encrypt_enabled: false,
            is_encrypted: is_encrypted.unwrap_or(false),
            encryption_key_version,
        }
    }

    /// 创建上传任务元数据
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `source_path` - 本地源文件路径
    /// * `target_path` - 远程目标路径
    /// * `file_size` - 文件大小（字节）
    /// * `chunk_size` - 分片大小（字节）
    /// * `total_chunks` - 总分片数
    /// * `encrypt_enabled` - 是否启用加密（可选）
    /// * `encryption_key_version` - 加密密钥版本（可选）
    pub fn new_upload(
        task_id: String,
        source_path: PathBuf,
        target_path: String,
        file_size: u64,
        chunk_size: u64,
        total_chunks: usize,
        encrypt_enabled: Option<bool>,
        encryption_key_version: Option<u32>,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            task_type: TaskType::Upload,
            created_at: now,
            updated_at: now,
            fs_id: None,
            transfer_task_id: None,
            remote_path: None,
            local_path: None,
            file_size: Some(file_size),
            chunk_size: Some(chunk_size),
            total_chunks: Some(total_chunks),
            source_path: Some(source_path),
            target_path: Some(target_path),
            upload_id: None,
            upload_id_created_at: None,
            share_link: None,
            share_pwd: None,
            transfer_target_path: None,
            transfer_status: None,
            download_task_ids: vec![],
            share_info_json: None,
            auto_download: None,
            transfer_file_name: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            status: Some(TaskPersistenceStatus::Pending),
            completed_at: None,
            error_msg: None,
            is_backup: false,
            backup_config_id: None,
            // 加密字段
            encrypt_enabled: encrypt_enabled.unwrap_or(false),
            is_encrypted: false,
            encryption_key_version,
        }
    }

    /// 创建上传备份任务元数据
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `source_path` - 本地源文件路径
    /// * `target_path` - 远程目标路径
    /// * `file_size` - 文件大小（字节）
    /// * `chunk_size` - 分片大小（字节）
    /// * `total_chunks` - 总分片数
    /// * `backup_config_id` - 备份配置 ID
    /// * `encrypt_enabled` - 是否启用加密（可选）
    /// * `encryption_key_version` - 加密密钥版本（可选）
    pub fn new_upload_backup(
        task_id: String,
        source_path: PathBuf,
        target_path: String,
        file_size: u64,
        chunk_size: u64,
        total_chunks: usize,
        backup_config_id: String,
        encrypt_enabled: Option<bool>,
        encryption_key_version: Option<u32>,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            task_type: TaskType::Upload,
            created_at: now,
            updated_at: now,
            fs_id: None,
            transfer_task_id: None,
            remote_path: None,
            local_path: None,
            file_size: Some(file_size),
            chunk_size: Some(chunk_size),
            total_chunks: Some(total_chunks),
            source_path: Some(source_path),
            target_path: Some(target_path),
            upload_id: None,
            upload_id_created_at: None,
            share_link: None,
            share_pwd: None,
            transfer_target_path: None,
            transfer_status: None,
            download_task_ids: vec![],
            share_info_json: None,
            auto_download: None,
            transfer_file_name: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            status: Some(TaskPersistenceStatus::Pending),
            completed_at: None,
            error_msg: None,
            is_backup: true,
            backup_config_id: Some(backup_config_id),
            // 加密字段
            encrypt_enabled: encrypt_enabled.unwrap_or(false),
            is_encrypted: false,
            encryption_key_version,
        }
    }

    /// 创建转存任务元数据
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `share_link` - 分享链接
    /// * `share_pwd` - 提取码
    /// * `target_path` - 转存目标路径
    /// * `auto_download` - 是否开启自动下载
    /// * `file_name` - 文件名称（用于展示）
    pub fn new_transfer(
        task_id: String,
        share_link: String,
        share_pwd: Option<String>,
        target_path: String,
        auto_download: bool,
        file_name: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            task_type: TaskType::Transfer,
            created_at: now,
            updated_at: now,
            fs_id: None,
            transfer_task_id: None,
            remote_path: None,
            local_path: None,
            file_size: None,
            chunk_size: None,
            total_chunks: None,
            source_path: None,
            target_path: None,
            upload_id: None,
            upload_id_created_at: None,
            share_link: Some(share_link),
            share_pwd: share_pwd,
            transfer_target_path: Some(target_path),
            transfer_status: Some("checking_share".to_string()),
            download_task_ids: vec![],
            share_info_json: None,
            auto_download: Some(auto_download),
            transfer_file_name: file_name,
            group_id: None,
            group_root: None,
            relative_path: None,
            status: Some(TaskPersistenceStatus::Pending),
            completed_at: None,
            error_msg: None,
            is_backup: false,
            backup_config_id: None,
            // 加密字段
            encrypt_enabled: false,
            is_encrypted: false,
            encryption_key_version: None,
        }
    }

    /// 更新最后修改时间
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }

    /// 设置上传 ID
    pub fn set_upload_id(&mut self, upload_id: String) {
        self.upload_id = Some(upload_id);
        self.upload_id_created_at = Some(Utc::now());
        self.touch();
    }

    /// 设置转存状态
    pub fn set_transfer_status(&mut self, status: &str) {
        self.transfer_status = Some(status.to_string());
        self.touch();
    }

    /// 设置关联的下载任务 ID
    pub fn set_download_task_ids(&mut self, ids: Vec<String>) {
        self.download_task_ids = ids;
        self.touch();
    }

    /// 设置关联的转存任务 ID（下载任务使用）
    pub fn set_transfer_task_id(&mut self, transfer_task_id: String) {
        self.transfer_task_id = Some(transfer_task_id);
        self.touch();
    }

    /// 设置自动下载标记
    pub fn set_auto_download(&mut self, auto_download: bool) {
        self.auto_download = Some(auto_download);
        self.touch();
    }

    /// 设置转存文件名称
    pub fn set_transfer_file_name(&mut self, file_name: String) {
        self.transfer_file_name = Some(file_name);
        self.touch();
    }

    /// 设置错误信息
    pub fn set_error_msg(&mut self, error_msg: String) {
        self.error_msg = Some(error_msg);
        self.touch();
    }

    /// 设置文件夹下载组信息
    pub fn set_group_info(
        &mut self,
        group_id: Option<String>,
        group_root: Option<String>,
        relative_path: Option<String>,
    ) {
        self.group_id = group_id;
        self.group_root = group_root;
        self.relative_path = relative_path;
        self.touch();
    }

    // === 历史归档方法 ===

    /// 标记任务完成
    pub fn mark_completed(&mut self) {
        self.status = Some(TaskPersistenceStatus::Completed);
        self.completed_at = Some(Utc::now());
        self.touch();
    }

    /// 标记任务失败
    pub fn mark_failed(&mut self) {
        self.status = Some(TaskPersistenceStatus::Failed);
        self.touch();
    }

    /// 更新任务状态
    pub fn set_status(&mut self, status: TaskPersistenceStatus) {
        self.status = Some(status);
        self.touch();
    }

    /// 更新加密信息
    ///
    /// # Arguments
    /// * `encrypt_enabled` - 是否启用加密
    /// * `key_version` - 加密密钥版本
    pub fn set_encryption_info(&mut self, encrypt_enabled: bool, key_version: Option<u32>) {
        self.encrypt_enabled = encrypt_enabled;
        self.encryption_key_version = key_version;
        self.touch();
    }

    /// 设置本地路径（解密完成后更新为解密后的路径）
    ///
    /// # Arguments
    /// * `local_path` - 新的本地路径
    pub fn set_local_path(&mut self, local_path: PathBuf) {
        self.local_path = Some(local_path);
        self.touch();
    }

    /// 检查是否已完成
    pub fn is_completed(&self) -> bool {
        self.status == Some(TaskPersistenceStatus::Completed)
    }

    /// 检查是否为终态
    pub fn is_terminal(&self) -> bool {
        self.status.map(|s| s.is_terminal()).unwrap_or(false)
    }
}

/// WAL 记录
///
/// 每条记录占一行，格式为：`{chunk_index}[,{md5}]`
/// - 下载任务：只记录 chunk_index
/// - 上传任务：记录 chunk_index 和 md5（用于 create 请求）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// 分片索引（0-based）
    pub chunk_index: usize,

    /// 分片 MD5（仅上传任务需要）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,

    /// 记录时间戳（Unix 毫秒）
    pub timestamp_ms: i64,
}

impl WalRecord {
    /// 创建下载任务的 WAL 记录
    pub fn new_download(chunk_index: usize) -> Self {
        Self {
            chunk_index,
            md5: None,
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    /// 创建上传任务的 WAL 记录
    pub fn new_upload(chunk_index: usize, md5: String) -> Self {
        Self {
            chunk_index,
            md5: Some(md5),
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    /// 序列化为 WAL 行格式
    ///
    /// 格式：`{chunk_index},{md5},{timestamp_ms}` 或 `{chunk_index},,{timestamp_ms}`
    pub fn to_wal_line(&self) -> String {
        format!(
            "{},{},{}",
            self.chunk_index,
            self.md5.as_deref().unwrap_or(""),
            self.timestamp_ms
        )
    }

    /// 从 WAL 行格式解析（容错）
    ///
    /// 支持格式：
    /// - `{chunk_index},{md5},{timestamp_ms}` - 完整格式
    /// - `{chunk_index},{md5}` - 旧格式（无时间戳）
    /// - `{chunk_index}` - 最简格式
    pub fn from_wal_line(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.trim().split(',').collect();
        if parts.is_empty() {
            return None;
        }

        let chunk_index = parts[0].parse::<usize>().ok()?;

        let md5 = if parts.len() > 1 && !parts[1].is_empty() {
            Some(parts[1].to_string())
        } else {
            None
        };

        let timestamp_ms = if parts.len() > 2 {
            parts[2]
                .parse::<i64>()
                .unwrap_or_else(|_| Utc::now().timestamp_millis())
        } else {
            Utc::now().timestamp_millis()
        };

        Some(Self {
            chunk_index,
            md5,
            timestamp_ms,
        })
    }
}

/// 任务持久化信息（内存状态）
///
/// 每个任务在内存中维护一份持久化状态
pub struct TaskPersistenceInfo {
    /// 任务 ID
    pub task_id: String,

    /// 任务类型
    pub task_type: TaskType,

    /// 已完成的分片集合
    pub completed_chunks: BitSet,

    /// 分片 MD5 列表（仅上传任务）
    pub chunk_md5s: Option<Vec<Option<String>>>,

    /// WAL 缓存：待刷写的分片记录
    /// 使用 parking_lot::Mutex 保护（快速同步操作）
    pub wal_cache: Mutex<Vec<WalRecord>>,

    /// 元数据是否已修改（需要刷写）
    pub metadata_dirty: Mutex<bool>,
}

impl TaskPersistenceInfo {
    /// 创建下载任务的持久化信息
    pub fn new_download(task_id: String, total_chunks: usize) -> Self {
        Self {
            task_id,
            task_type: TaskType::Download,
            completed_chunks: BitSet::with_capacity(total_chunks),
            chunk_md5s: None,
            wal_cache: Mutex::new(Vec::new()),
            metadata_dirty: Mutex::new(false),
        }
    }

    /// 创建上传任务的持久化信息
    pub fn new_upload(task_id: String, total_chunks: usize) -> Self {
        Self {
            task_id,
            task_type: TaskType::Upload,
            completed_chunks: BitSet::with_capacity(total_chunks),
            chunk_md5s: Some(vec![None; total_chunks]),
            wal_cache: Mutex::new(Vec::new()),
            metadata_dirty: Mutex::new(false),
        }
    }

    /// 创建转存任务的持久化信息（无分片）
    pub fn new_transfer(task_id: String) -> Self {
        Self {
            task_id,
            task_type: TaskType::Transfer,
            completed_chunks: BitSet::new(),
            chunk_md5s: None,
            wal_cache: Mutex::new(Vec::new()),
            metadata_dirty: Mutex::new(false),
        }
    }

    /// 标记分片完成（下载任务）
    ///
    /// 只有当分片是新完成时才添加到 WAL 缓存，避免重复记录
    pub fn mark_chunk_completed(&mut self, chunk_index: usize) {
        // 检查分片是否已经完成
        // insert() 返回 true 表示新插入，false 表示已存在
        let is_new = self.completed_chunks.insert(chunk_index);

        if is_new {
            // 只有新完成的分片才添加到 WAL 缓存
            let record = WalRecord::new_download(chunk_index);
            self.wal_cache.lock().push(record);
        } else {
            // 分片已经完成过，可能是重复调用，记录警告
            warn!("分片 #{} 已标记为完成，跳过重复记录到 WAL", chunk_index);
        }
    }

    /// 标记分片完成（上传任务，带 MD5）
    ///
    /// 只有当分片是新完成时才添加到 WAL 缓存，避免重复记录
    pub fn mark_chunk_completed_with_md5(&mut self, chunk_index: usize, md5: String) {
        // 检查分片是否已经完成
        // insert() 返回 true 表示新插入，false 表示已存在
        let is_new = self.completed_chunks.insert(chunk_index);

        if is_new {
            // 保存 MD5
            if let Some(ref mut md5s) = self.chunk_md5s {
                if chunk_index < md5s.len() {
                    md5s[chunk_index] = Some(md5.clone());
                }
            }

            // 只有新完成的分片才添加到 WAL 缓存
            let record = WalRecord::new_upload(chunk_index, md5);
            self.wal_cache.lock().push(record);
        } else {
            // 分片已经完成过，可能是重复调用，记录警告
            warn!(
                "分片 #{} 已标记为完成，跳过重复记录到 WAL (MD5: {})",
                chunk_index, md5
            );
        }
    }

    /// 获取已完成的分片数
    pub fn completed_count(&self) -> usize {
        self.completed_chunks.len()
    }

    /// 检查分片是否已完成
    pub fn is_chunk_completed(&self, chunk_index: usize) -> bool {
        self.completed_chunks.contains(chunk_index)
    }

    /// 获取未完成的分片索引列表
    pub fn get_pending_chunks(&self, total_chunks: usize) -> Vec<usize> {
        (0..total_chunks)
            .filter(|&i| !self.completed_chunks.contains(i))
            .collect()
    }

    /// 获取待刷写的 WAL 记录并清空缓存
    pub fn take_wal_cache(&self) -> Vec<WalRecord> {
        let mut cache = self.wal_cache.lock();
        std::mem::take(&mut *cache)
    }

    /// 标记元数据已修改
    pub fn mark_metadata_dirty(&self) {
        *self.metadata_dirty.lock() = true;
    }

    /// 检查并清除元数据脏标记
    pub fn take_metadata_dirty(&self) -> bool {
        let mut dirty = self.metadata_dirty.lock();
        let was_dirty = *dirty;
        *dirty = false;
        was_dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_type_serialization() {
        assert_eq!(TaskType::Download.as_str(), "download");
        assert_eq!(TaskType::Upload.as_str(), "upload");
        assert_eq!(TaskType::Transfer.as_str(), "transfer");
    }

    #[test]
    fn test_wal_record_line_format() {
        // 下载任务记录
        let record = WalRecord::new_download(5);
        let line = record.to_wal_line();
        assert!(line.starts_with("5,,"));

        // 上传任务记录
        let record = WalRecord::new_upload(3, "abc123".to_string());
        let line = record.to_wal_line();
        assert!(line.starts_with("3,abc123,"));
    }

    #[test]
    fn test_wal_record_parsing() {
        // 完整格式
        let record = WalRecord::from_wal_line("5,abc123,1700000000000").unwrap();
        assert_eq!(record.chunk_index, 5);
        assert_eq!(record.md5, Some("abc123".to_string()));
        assert_eq!(record.timestamp_ms, 1700000000000);

        // 无 MD5
        let record = WalRecord::from_wal_line("3,,1700000000000").unwrap();
        assert_eq!(record.chunk_index, 3);
        assert_eq!(record.md5, None);

        // 最简格式
        let record = WalRecord::from_wal_line("7").unwrap();
        assert_eq!(record.chunk_index, 7);
        assert_eq!(record.md5, None);
    }

    #[test]
    fn test_task_persistence_info() {
        let mut info = TaskPersistenceInfo::new_download("task1".to_string(), 10);

        // 标记分片完成
        info.mark_chunk_completed(0);
        info.mark_chunk_completed(5);
        info.mark_chunk_completed(9);

        assert_eq!(info.completed_count(), 3);
        assert!(info.is_chunk_completed(0));
        assert!(info.is_chunk_completed(5));
        assert!(info.is_chunk_completed(9));
        assert!(!info.is_chunk_completed(1));

        // 获取未完成分片
        let pending = info.get_pending_chunks(10);
        assert_eq!(pending, vec![1, 2, 3, 4, 6, 7, 8]);

        // WAL 缓存
        let cache = info.take_wal_cache();
        assert_eq!(cache.len(), 3);
        assert_eq!(cache[0].chunk_index, 0);
        assert_eq!(cache[1].chunk_index, 5);
        assert_eq!(cache[2].chunk_index, 9);

        // 缓存已清空
        let cache = info.take_wal_cache();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_upload_task_with_md5() {
        let mut info = TaskPersistenceInfo::new_upload("task2".to_string(), 5);

        info.mark_chunk_completed_with_md5(0, "md5_0".to_string());
        info.mark_chunk_completed_with_md5(2, "md5_2".to_string());

        assert_eq!(info.completed_count(), 2);

        // 检查 MD5 存储
        let md5s = info.chunk_md5s.as_ref().unwrap();
        assert_eq!(md5s[0], Some("md5_0".to_string()));
        assert_eq!(md5s[1], None);
        assert_eq!(md5s[2], Some("md5_2".to_string()));

        // WAL 缓存带 MD5
        let cache = info.take_wal_cache();
        assert_eq!(cache.len(), 2);
        assert_eq!(cache[0].md5, Some("md5_0".to_string()));
        assert_eq!(cache[1].md5, Some("md5_2".to_string()));
    }

    #[test]
    fn test_task_metadata_download() {
        let metadata = TaskMetadata::new_download(
            "task1".to_string(),
            12345,
            "/test/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024 * 1024,
            256 * 1024,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );

        assert_eq!(metadata.task_type, TaskType::Download);
        assert_eq!(metadata.fs_id, Some(12345));
        assert_eq!(metadata.total_chunks, Some(4));
    }

    #[test]
    fn test_task_metadata_upload() {
        let mut metadata = TaskMetadata::new_upload(
            "task2".to_string(),
            PathBuf::from("/local/upload.txt"),
            "/remote/upload.txt".to_string(),
            2 * 1024 * 1024,
            512 * 1024,
            4,
            None,  // encrypt_enabled
            None,  // encryption_key_version
        );

        assert_eq!(metadata.task_type, TaskType::Upload);
        assert!(metadata.upload_id.is_none());

        // 设置上传 ID
        metadata.set_upload_id("upload_id_123".to_string());
        assert_eq!(metadata.upload_id, Some("upload_id_123".to_string()));
        assert!(metadata.upload_id_created_at.is_some());
    }

    #[test]
    fn test_task_metadata_transfer() {
        let mut metadata = TaskMetadata::new_transfer(
            "task3".to_string(),
            "https://pan.baidu.com/s/xxx".to_string(),
            Some("1234".to_string()),
            "/save/path".to_string(),
            true, // auto_download
            Some("test_file.zip".to_string()), // file_name
        );

        assert_eq!(metadata.task_type, TaskType::Transfer);
        assert_eq!(metadata.transfer_status, Some("checking_share".to_string()));
        assert_eq!(metadata.auto_download, Some(true));
        assert_eq!(metadata.transfer_file_name, Some("test_file.zip".to_string()));

        // 设置转存状态
        metadata.set_transfer_status("downloading");
        assert_eq!(metadata.transfer_status, Some("downloading".to_string()));

        // 设置关联下载任务
        metadata.set_download_task_ids(vec!["dl1".to_string(), "dl2".to_string()]);
        assert_eq!(metadata.download_task_ids.len(), 2);

        // 测试新增的 setter 方法
        metadata.set_auto_download(false);
        assert_eq!(metadata.auto_download, Some(false));

        metadata.set_transfer_file_name("new_file.zip".to_string());
        assert_eq!(metadata.transfer_file_name, Some("new_file.zip".to_string()));

        metadata.set_transfer_task_id("transfer_001".to_string());
        assert_eq!(metadata.transfer_task_id, Some("transfer_001".to_string()));
    }
}
