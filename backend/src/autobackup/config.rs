//! 备份配置数据结构

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 备份配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// 配置唯一标识
    pub id: String,
    /// 配置名称（用户可识别）
    pub name: String,
    /// 本地源路径
    pub local_path: PathBuf,
    /// 云端目标路径
    pub remote_path: String,
    /// 备份方向
    pub direction: BackupDirection,
    /// 监听配置
    pub watch_config: WatchConfig,
    /// 轮询配置
    pub poll_config: PollConfig,
    /// 过滤配置
    pub filter_config: FilterConfig,
    /// 是否启用加密
    #[serde(default)]
    pub encrypt_enabled: bool,
    /// 是否启用
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 创建时间
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// 更新时间
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

fn default_true() -> bool {
    true
}

/// 备份方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupDirection {
    /// 上传备份：本地 → 云端
    Upload,
    /// 下载备份：云端 → 本地
    Download,
}

/// 监听配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchConfig {
    /// 是否启用监听
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 防抖时间（毫秒）
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
}

fn default_debounce_ms() -> u64 {
    3000
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            debounce_ms: 3000,
        }
    }
}

/// 轮询配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollConfig {
    /// 是否启用轮询
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 轮询模式
    #[serde(default)]
    pub mode: PollMode,
    /// 轮询间隔（分钟）
    #[serde(default = "default_poll_interval")]
    pub interval_minutes: u32,
    /// 指定时间（小时，0-23）
    pub schedule_hour: Option<u32>,
    /// 指定时间（分钟，0-59）
    pub schedule_minute: Option<u32>,
}

fn default_poll_interval() -> u32 {
    30
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: PollMode::Interval,
            interval_minutes: 30,
            schedule_hour: None,
            schedule_minute: None,
        }
    }
}

/// 轮询模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PollMode {
    /// 固定间隔
    #[default]
    Interval,
    /// 指定时间
    Scheduled,
    /// 禁用
    Disabled,
}

/// 过滤配置
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterConfig {
    /// 包含的文件扩展名（为空表示全部）
    #[serde(default)]
    pub include_extensions: Vec<String>,
    /// 排除的文件扩展名
    #[serde(default)]
    pub exclude_extensions: Vec<String>,
    /// 排除的目录名
    #[serde(default)]
    pub exclude_directories: Vec<String>,
    /// 最大文件大小（字节，0 表示不限制）
    #[serde(default)]
    pub max_file_size: u64,
    /// 最小文件大小（字节）
    #[serde(default)]
    pub min_file_size: u64,
}

/// 加密配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// 是否启用加密
    pub enabled: bool,
    /// 主密钥（Base64 编码）
    pub master_key: Option<String>,
    /// 加密算法
    #[serde(default)]
    pub algorithm: EncryptionAlgorithm,
    /// 密钥创建时间
    pub key_created_at: Option<chrono::DateTime<chrono::Utc>>,
    /// 密钥版本（用于密钥轮换）
    #[serde(default = "default_key_version")]
    pub key_version: u32,
    /// 最后使用时间（时间戳毫秒）
    #[serde(default)]
    pub last_used_at: Option<i64>,
}

fn default_key_version() -> u32 {
    1
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            master_key: None,
            algorithm: EncryptionAlgorithm::default(),
            key_created_at: None,
            key_version: 1,
            last_used_at: None,
        }
    }
}

impl EncryptionConfig {
    /// 更新最后使用时间
    pub fn touch(&mut self) {
        self.last_used_at = Some(chrono::Utc::now().timestamp_millis());
    }

    /// 检查密钥是否有效
    pub fn is_key_valid(&self) -> bool {
        self.enabled && self.master_key.is_some()
    }

    /// 获取密钥年龄（天数）
    pub fn key_age_days(&self) -> Option<i64> {
        self.key_created_at.map(|created| {
            (chrono::Utc::now() - created).num_days()
        })
    }
}

/// 加密算法
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM（默认，推荐）
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305（备选）
    ChaCha20Poly1305,
}

impl std::fmt::Display for EncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncryptionAlgorithm::Aes256Gcm => write!(f, "aes-256-gcm"),
            EncryptionAlgorithm::ChaCha20Poly1305 => write!(f, "chacha20-poly1305"),
        }
    }
}

/// 准备阶段资源池配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparePoolConfig {
    /// 最大并发扫描任务数
    #[serde(default = "default_concurrent")]
    pub max_concurrent_scans: usize,
    /// 最大并发加密任务数
    #[serde(default = "default_concurrent")]
    pub max_concurrent_encrypts: usize,
    /// 单个加密任务的缓冲区大小（MB）
    #[serde(default = "default_buffer_size")]
    pub encrypt_buffer_size_mb: usize,
}

fn default_concurrent() -> usize {
    2
}

fn default_buffer_size() -> usize {
    16
}

impl Default for PreparePoolConfig {
    fn default() -> Self {
        Self {
            max_concurrent_scans: 2,
            max_concurrent_encrypts: 2,
            encrypt_buffer_size_mb: 16,
        }
    }
}

/// 创建备份配置请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBackupConfigRequest {
    /// 配置名称
    pub name: String,
    /// 本地源路径
    pub local_path: String,
    /// 云端目标路径
    pub remote_path: String,
    /// 备份方向
    pub direction: BackupDirection,
    /// 监听配置
    #[serde(default)]
    pub watch_config: WatchConfig,
    /// 轮询配置
    #[serde(default)]
    pub poll_config: PollConfig,
    /// 过滤配置
    #[serde(default)]
    pub filter_config: FilterConfig,
    /// 是否启用加密
    #[serde(default)]
    pub encrypt_enabled: bool,
}

/// 更新备份配置请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateBackupConfigRequest {
    /// 配置名称
    pub name: Option<String>,
    /// 本地源路径
    pub local_path: Option<String>,
    /// 云端目标路径
    pub remote_path: Option<String>,
    /// 监听配置
    pub watch_config: Option<WatchConfig>,
    /// 轮询配置
    pub poll_config: Option<PollConfig>,
    /// 过滤配置
    pub filter_config: Option<FilterConfig>,
    /// 是否启用（注意：加密选项创建后不可更改）
    pub enabled: Option<bool>,
}
