// 配置管理模块

pub mod env_detector;
pub mod mount_detector;
pub mod path_validator;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;

pub use env_detector::{EnvDetector, EnvInfo, OsType};
pub use mount_detector::{MountDetector, MountPoint};
pub use path_validator::{PathValidationResult, PathValidator};

/// 应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// 服务器配置
    pub server: ServerConfig,
    /// 下载配置
    pub download: DownloadConfig,
    /// 上传配置
    #[serde(default)]
    pub upload: UploadConfig,
    /// 转存配置
    #[serde(default)]
    pub transfer: TransferConfig,
    /// 文件系统配置
    #[serde(default)]
    pub filesystem: FilesystemConfig,
    /// 持久化配置
    #[serde(default)]
    pub persistence: PersistenceConfig,
    /// 🔥 日志配置
    #[serde(default)]
    pub log: LogConfig,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// 是否启用日志文件持久化
    #[serde(default = "default_log_enabled")]
    pub enabled: bool,
    /// 日志文件保存目录
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
    /// 日志保留天数（默认 7 天）
    #[serde(default = "default_log_retention_days")]
    pub retention_days: u32,
    /// 日志级别（默认 info）
    #[serde(default = "default_log_level")]
    pub level: String,
    /// 单个日志文件最大大小（字节，默认 50MB）
    #[serde(default = "default_log_max_file_size")]
    pub max_file_size: u64,
}

fn default_log_enabled() -> bool {
    true
}

fn default_log_dir() -> PathBuf {
    PathBuf::from("logs")
}

fn default_log_retention_days() -> u32 {
    7
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_max_file_size() -> u64 {
    50 * 1024 * 1024 // 50MB
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            enabled: default_log_enabled(),
            log_dir: default_log_dir(),
            retention_days: default_log_retention_days(),
            level: default_log_level(),
            max_file_size: default_log_max_file_size(),
        }
    }
}

/// 服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// 监听地址
    pub host: String,
    /// 监听端口
    pub port: u16,
    /// CORS允许的源
    pub cors_origins: Vec<String>,
}

/// 下载配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadConfig {
    /// 默认下载目录
    pub download_dir: PathBuf,
    /// 用户设置的默认目录（用于"设置为默认"功能）
    #[serde(default)]
    pub default_directory: Option<PathBuf>,
    /// 最近使用的下载目录
    #[serde(default)]
    pub recent_directory: Option<PathBuf>,
    /// 每次下载时是否询问保存位置
    #[serde(default = "default_ask_each_time")]
    pub ask_each_time: bool,
    /// 全局最大线程数（所有下载任务共享）
    pub max_global_threads: usize,
    /// 分片大小 (MB)
    pub chunk_size_mb: u64,
    /// 最大同时下载文件数
    pub max_concurrent_tasks: usize,
    /// 最大重试次数
    pub max_retries: u32,
    /// CDN刷新配置
    #[serde(default)]
    pub cdn_refresh: CdnRefreshConfig,
}

/// CDN链接刷新配置
///
/// 用于配置三层检测机制的参数：
/// 1. 定时刷新：每隔固定时间强制刷新CDN链接
/// 2. 速度异常检测：检测全局速度异常下降时触发刷新
/// 3. 线程停滞检测：检测大面积线程停滞时触发刷新
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdnRefreshConfig {
    /// 是否启用CDN刷新功能
    #[serde(default = "default_cdn_refresh_enabled")]
    pub enabled: bool,

    /// 定时刷新间隔（分钟），默认10分钟
    #[serde(default = "default_refresh_interval_minutes")]
    pub refresh_interval_minutes: u64,

    /// 最小刷新间隔（秒），防止频繁刷新，默认30秒
    #[serde(default = "default_min_refresh_interval_secs")]
    pub min_refresh_interval_secs: u64,

    /// 速度下降阈值（百分比），下降超过此比例触发刷新，默认50%
    #[serde(default = "default_speed_drop_threshold_percent")]
    pub speed_drop_threshold_percent: u64,

    /// 速度下降持续时长（秒），持续超过此时间触发刷新，默认10秒
    #[serde(default = "default_speed_drop_duration_secs")]
    pub speed_drop_duration_secs: u64,

    /// 基线建立时间（秒），任务开始后多久建立速度基线，默认30秒
    #[serde(default = "default_baseline_establish_secs")]
    pub baseline_establish_secs: u64,

    /// 线程停滞阈值（KB/s），速度低于此值视为停滞，默认10 KB/s
    #[serde(default = "default_stagnation_threshold_kbps")]
    pub stagnation_threshold_kbps: u64,

    /// 线程停滞比例（百分比），超过此比例触发刷新，默认80%
    #[serde(default = "default_stagnation_ratio_percent")]
    pub stagnation_ratio_percent: u64,

    /// 最小检测线程数，少于此数不进行停滞检测，默认3
    #[serde(default = "default_min_threads_for_detection")]
    pub min_threads_for_detection: usize,

    /// 启动延迟（秒），任务开始后多久开始停滞检测，默认10秒
    #[serde(default = "default_startup_delay_secs")]
    pub startup_delay_secs: u64,
}

// CDN刷新配置默认值函数
fn default_cdn_refresh_enabled() -> bool {
    true
}
fn default_refresh_interval_minutes() -> u64 {
    10
}
fn default_min_refresh_interval_secs() -> u64 {
    30
}
fn default_speed_drop_threshold_percent() -> u64 {
    50
}
fn default_speed_drop_duration_secs() -> u64 {
    10
}
fn default_baseline_establish_secs() -> u64 {
    30
}
fn default_stagnation_threshold_kbps() -> u64 {
    10
}
fn default_stagnation_ratio_percent() -> u64 {
    80
}
fn default_min_threads_for_detection() -> usize {
    3
}
fn default_startup_delay_secs() -> u64 {
    10
}

impl Default for CdnRefreshConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval_minutes: 10,
            min_refresh_interval_secs: 30,
            speed_drop_threshold_percent: 50,
            speed_drop_duration_secs: 10,
            baseline_establish_secs: 30,
            stagnation_threshold_kbps: 10,
            stagnation_ratio_percent: 80,
            min_threads_for_detection: 3,
            startup_delay_secs: 10,
        }
    }
}

impl CdnRefreshConfig {
    /// 转换为速度异常检测器配置
    pub fn to_speed_anomaly_config(&self) -> crate::common::SpeedAnomalyConfig {
        crate::common::SpeedAnomalyConfig {
            baseline_establish_secs: self.baseline_establish_secs,
            speed_drop_threshold: self.speed_drop_threshold_percent as f64 / 100.0,
            duration_threshold_secs: self.speed_drop_duration_secs,
            check_interval_secs: 5,         // 固定5秒检查一次
            min_baseline_speed: 100 * 1024, // 最小基线速度 100KB/s
        }
    }

    /// 转换为线程停滞检测器配置
    pub fn to_stagnation_config(&self) -> crate::common::StagnationConfig {
        crate::common::StagnationConfig {
            near_zero_threshold_kbps: self.stagnation_threshold_kbps,
            stagnation_ratio: self.stagnation_ratio_percent as f64 / 100.0,
            min_threads: self.min_threads_for_detection,
            startup_delay_secs: self.startup_delay_secs,
        }
    }

    /// 转换为刷新协调器配置
    pub fn to_refresh_coordinator_config(&self) -> crate::common::RefreshCoordinatorConfig {
        crate::common::RefreshCoordinatorConfig {
            min_refresh_interval_secs: self.min_refresh_interval_secs,
        }
    }
}

/// 默认每次询问保存位置
fn default_ask_each_time() -> bool {
    true
}

/// 上传配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    /// 全局最大线程数（所有上传任务共享）
    pub max_global_threads: usize,
    /// 分片大小 (MB)，范围 4-32MB
    pub chunk_size_mb: u64,
    /// 最大同时上传文件数
    pub max_concurrent_tasks: usize,
    /// 最大重试次数
    pub max_retries: u32,
    /// 上传文件夹时是否跳过隐藏文件（以.开头的文件/文件夹）
    pub skip_hidden_files: bool,
    /// 最近使用的上传源目录
    #[serde(default)]
    pub recent_directory: Option<PathBuf>,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            max_global_threads: 10,
            chunk_size_mb: 4, // 百度网盘上传分片最小 4MB
            max_concurrent_tasks: 5,
            max_retries: 3,
            skip_hidden_files: false, // 默认不跳过隐藏文件
            recent_directory: None,   // 默认无最近目录
        }
    }
}

/// 转存配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferConfig {
    /// 转存后默认行为：transfer_only / transfer_and_download
    #[serde(default = "default_transfer_behavior")]
    pub default_behavior: String,

    /// 最近使用的网盘目录 fs_id（转存目标位置）
    #[serde(default)]
    pub recent_save_fs_id: Option<u64>,

    /// 最近使用的网盘目录路径（与 fs_id 对应）
    #[serde(default)]
    pub recent_save_path: Option<String>,
}

/// 默认转存行为：仅转存
fn default_transfer_behavior() -> String {
    "transfer_only".to_string()
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            default_behavior: default_transfer_behavior(),
            recent_save_fs_id: None,
            recent_save_path: None,
        }
    }
}

/// 文件系统配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    /// 允许访问的路径白名单（空表示允许所有）
    #[serde(default)]
    pub allowed_paths: Vec<String>,
    /// 是否显示隐藏文件
    #[serde(default)]
    pub show_hidden: bool,
    /// 是否跟随符号链接
    #[serde(default)]
    pub follow_symlinks: bool,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            allowed_paths: vec![],
            show_hidden: false,
            follow_symlinks: false,
        }
    }
}

/// 持久化配置
///
/// 用于配置任务持久化和恢复功能：
/// - WAL (Write-Ahead Log) 日志，记录分片完成进度
/// - 元数据持久化，记录任务基本信息
/// - 断点恢复功能
/// - 历史归档功能
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// WAL 文件存储目录（相对于配置文件目录或绝对路径）
    #[serde(default = "default_wal_dir")]
    pub wal_dir: String,

    /// WAL 批量刷写间隔（毫秒），默认 200ms
    #[serde(default = "default_wal_flush_interval_ms")]
    pub wal_flush_interval_ms: u64,

    /// 启动时是否自动恢复任务
    #[serde(default = "default_auto_recover_tasks")]
    pub auto_recover_tasks: bool,

    /// WAL 文件保留天数（超过此天数的未完成任务 WAL 将被清理）
    #[serde(default = "default_wal_retention_days")]
    pub wal_retention_days: u64,

    /// 历史归档时间（小时，0-23，默认 2）
    #[serde(default = "default_history_archive_hour")]
    pub history_archive_hour: u8,

    /// 历史归档时间（分钟，0-59，默认 0）
    #[serde(default = "default_history_archive_minute")]
    pub history_archive_minute: u8,

    /// 历史任务保留天数（超过此天数的历史任务将被清理，默认 30 天）
    #[serde(default = "default_history_retention_days")]
    pub history_retention_days: u64,
}

// PersistenceConfig 默认值函数
fn default_wal_dir() -> String {
    "wal".to_string()
}

fn default_wal_flush_interval_ms() -> u64 {
    200
}

fn default_auto_recover_tasks() -> bool {
    true
}

fn default_wal_retention_days() -> u64 {
    7
}

fn default_history_archive_hour() -> u8 {
    2
}

fn default_history_archive_minute() -> u8 {
    0
}

fn default_history_retention_days() -> u64 {
    30
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            wal_dir: default_wal_dir(),
            wal_flush_interval_ms: default_wal_flush_interval_ms(),
            auto_recover_tasks: default_auto_recover_tasks(),
            wal_retention_days: default_wal_retention_days(),
            history_archive_hour: default_history_archive_hour(),
            history_archive_minute: default_history_archive_minute(),
            history_retention_days: default_history_retention_days(),
        }
    }
}

/// VIP 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VipType {
    /// 普通用户
    Normal = 0,
    /// 普通会员
    Vip = 1,
    /// 超级会员
    Svip = 2,
}

impl VipType {
    /// 从数字创建
    pub fn from_u32(value: u32) -> Self {
        match value {
            2 => VipType::Svip,
            1 => VipType::Vip,
            _ => VipType::Normal,
        }
    }

    /// 获取该 VIP 等级允许的最大分片大小 (MB)
    pub fn max_chunk_size_mb(&self) -> u64 {
        match self {
            VipType::Normal => 4, // 普通用户最高 4MB
            VipType::Vip => 4,    // 普通会员最高 4MB
            VipType::Svip => 5,   // SVIP 最高 5MB
        }
    }
}

/// VIP 等级对应的推荐配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VipRecommendedConfig {
    /// 推荐线程数
    pub threads: usize,
    /// 推荐分片大小 (MB)
    pub chunk_size: u64,
    /// 最大同时下载文件数
    pub max_tasks: usize,
    /// 单文件大小上限 (GB)
    pub file_size_limit_gb: u64,
}

impl DownloadConfig {
    /// 验证下载路径是否为绝对路径
    ///
    /// # 返回值
    /// - Ok(()): 路径是绝对路径
    /// - Err: 路径不是绝对路径或格式无效
    pub fn validate_download_dir(&self) -> Result<()> {
        if !self.download_dir.is_absolute() {
            anyhow::bail!(
                "下载目录必须是绝对路径，当前值: {:?}\n\
                 Windows 示例: D:\\Downloads 或 C:\\Users\\YourName\\Downloads\n\
                 Linux/Docker 示例: /app/downloads 或 /home/user/downloads",
                self.download_dir
            );
        }

        tracing::debug!("✓ 路径格式验证通过（绝对路径）: {:?}", self.download_dir);
        Ok(())
    }

    /// 增强路径验证（检查存在性、可写性、挂载点等）
    ///
    /// # 返回值
    /// - Ok(PathValidationResult): 详细的验证结果
    /// - Err: 验证过程中发生错误
    pub fn validate_download_dir_enhanced(&self) -> Result<PathValidationResult> {
        // 首先验证是否为绝对路径
        self.validate_download_dir()?;

        // 获取环境信息，检测是否在 Docker 中
        let env_info = EnvDetector::get_env_info();

        // 使用 PathValidator 进行增强验证（带 Docker 检查）
        let result =
            PathValidator::validate_with_docker_check(&self.download_dir, env_info.is_docker);

        Ok(result)
    }

    /// 确保下载目录存在（不存在则自动创建）
    ///
    /// # 返回值
    /// - Ok(()): 目录存在或创建成功
    /// - Err: 创建失败
    pub fn ensure_download_dir_exists(&self) -> Result<()> {
        // 先验证是否为绝对路径
        self.validate_download_dir()?;

        // 确保目录存在
        PathValidator::ensure_directory_exists(&self.download_dir)?;

        tracing::info!("下载目录已准备就绪: {:?}", self.download_dir);
        Ok(())
    }

    /// 根据文件大小和 VIP 等级计算最优分片大小(字节)
    ///
    /// 自适应策略:
    /// - < 5MB: 256KB
    /// - 5-10MB: 512KB
    /// - 10-50MB: 1MB
    /// - 50-100MB: 2MB
    /// - 100-500MB: 4MB
    /// - >= 500MB: 5MB
    ///
    /// ⚠️ 重要:百度网盘限制单个 Range 请求最大 5MB,超过会返回 403 Forbidden
    ///
    /// 同时根据 VIP 等级限制最大分片大小:
    /// - 普通用户:最高 4MB
    /// - 普通会员:最高 5MB
    /// - SVIP:最高 5MB
    pub fn calculate_adaptive_chunk_size(file_size_bytes: u64, vip_type: VipType) -> u64 {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;

        // 根据文件大小计算基础分片大小
        let base_chunk_size = if file_size_bytes < 5 * MB {
            256 * KB // < 5MB → 256KB
        } else if file_size_bytes < 10 * MB {
            512 * KB // 5-10MB → 512KB
        } else if file_size_bytes < 50 * MB {
            1 * MB // 10-50MB → 1MB
        } else if file_size_bytes < 100 * MB {
            2 * MB // 50-100MB → 2MB
        } else if file_size_bytes < 500 * MB {
            4 * MB // 100-500MB → 4MB
        } else {
            5 * MB // >= 500MB → 5MB（百度限制）
        };

        // 根据 VIP 等级限制最大分片大小
        let max_allowed = vip_type.max_chunk_size_mb() * MB;

        // 返回较小的值（确保不超过 VIP 限制和百度的 5MB 硬限制）
        base_chunk_size.min(max_allowed).min(5 * MB)
    }

    /// 根据 VIP 类型获取推荐配置
    pub fn recommended_for_vip(vip_type: VipType) -> VipRecommendedConfig {
        match vip_type {
            VipType::Normal => VipRecommendedConfig {
                threads: 1,    // ⚠️ 普通用户只能1个线程！
                chunk_size: 4, // 4MB 分片
                max_tasks: 1,  // 只能下载1个文件
                file_size_limit_gb: 4,
            },
            VipType::Vip => VipRecommendedConfig {
                threads: 5,    // 普通会员5个线程
                chunk_size: 4, // 4MB 分片
                max_tasks: 3,  // 可以同时下载3个文件
                file_size_limit_gb: 10,
            },
            VipType::Svip => VipRecommendedConfig {
                threads: 10,   // SVIP 10个线程（可调至20）
                chunk_size: 5, // 5MB 分片
                max_tasks: 5,  // 可以同时下载5个文件
                file_size_limit_gb: 20,
            },
        }
    }

    /// 应用推荐配置
    pub fn apply_recommended(&mut self, vip_type: VipType) {
        let recommended = Self::recommended_for_vip(vip_type);
        self.max_global_threads = recommended.threads;
        self.chunk_size_mb = recommended.chunk_size;
        self.max_concurrent_tasks = recommended.max_tasks;
    }

    /// 验证配置是否安全
    pub fn validate_for_vip(&self, vip_type: VipType) -> Result<(), String> {
        let recommended = Self::recommended_for_vip(vip_type);

        // 警告：普通用户超过推荐线程数
        if vip_type == VipType::Normal && self.max_global_threads > recommended.threads {
            return Err(format!(
                "⚠️ 警告：普通用户线程数超过推荐值！当前: {}, 推荐: {}\n\
                 调大线程数会导致账号被限速（可能持续数小时至数天）！\n\
                 强烈建议恢复为推荐值！",
                self.max_global_threads, recommended.threads
            ));
        }

        // 警告：会员用户超过安全范围
        if vip_type == VipType::Vip && self.max_global_threads > 10 {
            return Err(format!(
                "⚠️ 注意：会员用户线程数较高！当前: {}, 推荐: {}\n\
                 过高的线程数可能导致限速，建议不超过 10",
                self.max_global_threads, recommended.threads
            ));
        }

        // 警告：SVIP 超过安全范围
        if vip_type == VipType::Svip && self.max_global_threads > 20 {
            return Err(format!(
                "⚠️ 注意：SVIP 线程数过高！当前: {}, 建议不超过 20\n\
                 过高的线程数可能导致不稳定",
                self.max_global_threads
            ));
        }

        Ok(())
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        // 默认使用 SVIP 配置（用户可以根据自己的 VIP 等级调整）
        let svip_config = DownloadConfig::recommended_for_vip(VipType::Svip);

        // 获取环境信息
        let env_info = EnvDetector::get_env_info();

        // 获取默认下载目录（绝对路径）
        // Docker 环境使用 /app/downloads，本地环境使用当前工作目录 + downloads
        let download_dir = if env_info.is_docker {
            // Docker 环境
            PathBuf::from("/app/downloads")
        } else {
            // 本地环境：使用当前工作目录 + downloads
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("downloads")
        };

        // Docker 环境使用 0.0.0.0 以便从宿主机访问，本地环境使用 127.0.0.1
        let host = if env_info.is_docker {
            "0.0.0.0".to_string()
        } else {
            "127.0.0.1".to_string()
        };

        tracing::info!(
            "检测到环境: {} (Docker: {}), 使用默认下载目录: {:?}, 服务器监听地址: {}",
            env_info.os_type.as_str(),
            env_info.is_docker,
            download_dir,
            host
        );

        Self {
            server: ServerConfig {
                host,
                port: 18888,
                cors_origins: vec!["*".to_string()],
            },
            download: DownloadConfig {
                download_dir,
                default_directory: None,
                recent_directory: None,
                ask_each_time: true,
                max_global_threads: svip_config.threads,
                chunk_size_mb: svip_config.chunk_size,
                max_concurrent_tasks: svip_config.max_tasks,
                max_retries: 3,
                cdn_refresh: CdnRefreshConfig::default(),
            },
            upload: UploadConfig::default(),
            transfer: TransferConfig::default(),
            filesystem: FilesystemConfig::default(),
            persistence: PersistenceConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl AppConfig {
    /// 获取当前运行环境信息
    ///
    /// # 返回值
    /// - EnvInfo: 包含 Docker 环境和操作系统类型信息
    pub fn get_env_info() -> EnvInfo {
        EnvDetector::get_env_info()
    }

    /// 从文件加载配置
    pub async fn load_from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .await
            .context("Failed to read config file")?;

        let config: AppConfig = toml::from_str(&content).context("Failed to parse config file")?;

        // 验证下载路径是否为绝对路径
        config
            .download
            .validate_download_dir()
            .context("配置文件中的下载路径验证失败")?;

        Ok(config)
    }

    /// 保存配置到文件
    ///
    /// 执行以下步骤：
    /// 1. 验证下载路径格式（绝对路径）
    /// 2. 增强验证（存在性、可写性、可用空间）
    /// 3. 如果路径不存在，报错（要求用户先创建目录）
    /// 4. 序列化并保存配置文件
    ///
    /// # 返回值
    /// - Ok(PathValidationResult): 保存成功，返回路径验证结果
    /// - Err: 保存失败
    pub async fn save_to_file(&self, path: &str) -> Result<PathValidationResult> {
        // 1. 验证下载路径格式（绝对路径）
        self.download
            .validate_download_dir()
            .context("保存配置失败：下载路径必须是绝对路径")?;

        // 2. 增强验证（存在性、可写性、可用空间）
        let validation_result = self
            .download
            .validate_download_dir_enhanced()
            .context("保存配置失败：路径验证失败")?;

        // 3. 路径必须存在且有效
        if !validation_result.exists {
            anyhow::bail!(
                "保存配置失败：下载目录不存在。路径: {:?}。请先手动创建该目录，或选择一个已存在的目录",
                self.download.download_dir
            );
        }

        if !validation_result.valid {
            // 路径存在但验证失败（不可写或其他问题）
            let details_msg = validation_result.details.as_deref().unwrap_or("无详细信息");
            anyhow::bail!(
                "保存配置失败：{}。详情: {}",
                validation_result.message,
                details_msg
            );
        }

        // 4. 序列化并保存配置文件
        let content = toml::to_string_pretty(self).context("Failed to serialize config")?;

        // 确保父目录存在
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create config directory")?;
        }

        fs::write(path, content)
            .await
            .context("Failed to write config file")?;

        tracing::info!("✓ 配置已保存: {}", path);

        Ok(validation_result)
    }

    /// 加载或创建默认配置
    pub async fn load_or_default(path: &str) -> Self {
        match Self::load_from_file(path).await {
            Ok(config) => {
                tracing::info!("配置文件加载成功: {}", path);
                config
            }
            Err(e) => {
                tracing::warn!("配置文件加载失败，使用默认配置: {}", e);
                let default_config = Self::default();

                // 首次启动：自动创建默认下载目录
                if !default_config.download.download_dir.exists() {
                    if let Err(e) = std::fs::create_dir_all(&default_config.download.download_dir) {
                        tracing::error!(
                            "无法创建默认下载目录 {:?}: {}",
                            default_config.download.download_dir,
                            e
                        );
                    } else {
                        tracing::info!(
                            "✓ 已创建默认下载目录: {:?}",
                            default_config.download.download_dir
                        );
                    }
                }

                // 尝试保存默认配置
                if let Err(e) = default_config.save_to_file(path).await {
                    tracing::error!("保存默认配置失败: {}", e);
                }

                default_config
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.server.port, 18888); // 默认端口 18888
        assert_eq!(config.download.max_global_threads, 10); // SVIP 默认
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let config = AppConfig::default();
        config.save_to_file(path).await.unwrap();

        let loaded = AppConfig::load_from_file(path).await.unwrap();
        assert_eq!(loaded.server.port, config.server.port);
        assert_eq!(
            loaded.download.max_global_threads,
            config.download.max_global_threads
        );
    }

    #[test]
    fn test_vip_recommended_config() {
        // 测试普通用户推荐配置
        let normal = DownloadConfig::recommended_for_vip(VipType::Normal);
        assert_eq!(normal.threads, 1);
        assert_eq!(normal.chunk_size, 4);
        assert_eq!(normal.max_tasks, 1);

        // 测试会员推荐配置
        let vip = DownloadConfig::recommended_for_vip(VipType::Vip);
        assert_eq!(vip.threads, 5);
        assert_eq!(vip.chunk_size, 4);
        assert_eq!(vip.max_tasks, 3);

        // 测试 SVIP 推荐配置
        let svip = DownloadConfig::recommended_for_vip(VipType::Svip);
        assert_eq!(svip.threads, 10);
        assert_eq!(svip.chunk_size, 5);
        assert_eq!(svip.max_tasks, 5);
    }

    #[test]
    fn test_config_validation() {
        let mut config = DownloadConfig {
            download_dir: std::env::current_dir().unwrap().join("downloads"),
            default_directory: None,
            recent_directory: None,
            ask_each_time: true,
            max_global_threads: 5,
            chunk_size_mb: 10,
            max_concurrent_tasks: 2,
            max_retries: 3,
            cdn_refresh: CdnRefreshConfig::default(),
        };

        // 普通用户：5个线程应该触发警告
        let result = config.validate_for_vip(VipType::Normal);
        assert!(result.is_err());

        // SVIP：5个线程应该通过
        let result = config.validate_for_vip(VipType::Svip);
        assert!(result.is_ok());

        // SVIP：30个线程应该触发警告
        config.max_global_threads = 30;
        let result = config.validate_for_vip(VipType::Svip);
        assert!(result.is_err());
    }

    #[test]
    fn test_path_validation() {
        // 测试绝对路径验证（使用当前目录，确保跨平台兼容）
        let absolute_path = std::env::current_dir().unwrap().join("downloads");
        let absolute_config = DownloadConfig {
            download_dir: absolute_path,
            default_directory: None,
            recent_directory: None,
            ask_each_time: true,
            max_global_threads: 5,
            chunk_size_mb: 10,
            max_concurrent_tasks: 2,
            max_retries: 3,
            cdn_refresh: CdnRefreshConfig::default(),
        };
        assert!(absolute_config.validate_download_dir().is_ok());

        // 测试相对路径验证（应该失败）
        let relative_config = DownloadConfig {
            download_dir: PathBuf::from("downloads"),
            default_directory: None,
            recent_directory: None,
            ask_each_time: true,
            max_global_threads: 5,
            chunk_size_mb: 10,
            max_concurrent_tasks: 2,
            max_retries: 3,
            cdn_refresh: CdnRefreshConfig::default(),
        };
        assert!(relative_config.validate_download_dir().is_err());

        // 测试平台特定的绝对路径
        #[cfg(target_os = "windows")]
        {
            let windows_config = DownloadConfig {
                download_dir: PathBuf::from("D:\\Downloads"),
                default_directory: None,
                recent_directory: None,
                ask_each_time: true,
                max_global_threads: 5,
                chunk_size_mb: 10,
                max_concurrent_tasks: 2,
                max_retries: 3,
                cdn_refresh: CdnRefreshConfig::default(),
            };
            assert!(windows_config.validate_download_dir().is_ok());
        }

        #[cfg(not(target_os = "windows"))]
        {
            let unix_config = DownloadConfig {
                download_dir: PathBuf::from("/app/downloads"),
                default_directory: None,
                recent_directory: None,
                ask_each_time: true,
                max_global_threads: 5,
                chunk_size_mb: 10,
                max_concurrent_tasks: 2,
                max_retries: 3,
                cdn_refresh: CdnRefreshConfig::default(),
            };
            assert!(unix_config.validate_download_dir().is_ok());
        }
    }

    #[test]
    fn test_cdn_refresh_config_default() {
        let config = CdnRefreshConfig::default();

        // 验证默认值
        assert!(config.enabled);
        assert_eq!(config.refresh_interval_minutes, 10);
        assert_eq!(config.min_refresh_interval_secs, 30);
        assert_eq!(config.speed_drop_threshold_percent, 50);
        assert_eq!(config.speed_drop_duration_secs, 10);
        assert_eq!(config.baseline_establish_secs, 30);
        assert_eq!(config.stagnation_threshold_kbps, 10);
        assert_eq!(config.stagnation_ratio_percent, 80);
        assert_eq!(config.min_threads_for_detection, 3);
        assert_eq!(config.startup_delay_secs, 10);
    }

    #[test]
    fn test_cdn_refresh_config_to_speed_anomaly() {
        let config = CdnRefreshConfig {
            speed_drop_threshold_percent: 60,
            speed_drop_duration_secs: 15,
            baseline_establish_secs: 45,
            ..Default::default()
        };

        let speed_config = config.to_speed_anomaly_config();

        assert_eq!(speed_config.speed_drop_threshold, 0.6); // 60% -> 0.6
        assert_eq!(speed_config.duration_threshold_secs, 15);
        assert_eq!(speed_config.baseline_establish_secs, 45);
        assert_eq!(speed_config.check_interval_secs, 5); // 固定值
        assert_eq!(speed_config.min_baseline_speed, 100 * 1024); // 100 KB/s
    }

    #[test]
    fn test_cdn_refresh_config_to_stagnation() {
        let config = CdnRefreshConfig {
            stagnation_threshold_kbps: 20,
            stagnation_ratio_percent: 70,
            min_threads_for_detection: 5,
            startup_delay_secs: 20,
            ..Default::default()
        };

        let stag_config = config.to_stagnation_config();

        assert_eq!(stag_config.near_zero_threshold_kbps, 20);
        assert_eq!(stag_config.stagnation_ratio, 0.7); // 70% -> 0.7
        assert_eq!(stag_config.min_threads, 5);
        assert_eq!(stag_config.startup_delay_secs, 20);
    }

    #[test]
    fn test_cdn_refresh_config_to_coordinator() {
        let config = CdnRefreshConfig {
            min_refresh_interval_secs: 60,
            ..Default::default()
        };

        let coord_config = config.to_refresh_coordinator_config();

        assert_eq!(coord_config.min_refresh_interval_secs, 60);
    }

    #[test]
    fn test_cdn_refresh_config_serialization() {
        // 测试序列化和反序列化
        let config = CdnRefreshConfig {
            enabled: false,
            refresh_interval_minutes: 5,
            min_refresh_interval_secs: 20,
            speed_drop_threshold_percent: 40,
            speed_drop_duration_secs: 8,
            baseline_establish_secs: 20,
            stagnation_threshold_kbps: 5,
            stagnation_ratio_percent: 90,
            min_threads_for_detection: 2,
            startup_delay_secs: 5,
        };

        // 序列化为 TOML
        let toml_str = toml::to_string(&config).unwrap();

        // 反序列化
        let deserialized: CdnRefreshConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(deserialized.enabled, false);
        assert_eq!(deserialized.refresh_interval_minutes, 5);
        assert_eq!(deserialized.min_refresh_interval_secs, 20);
        assert_eq!(deserialized.speed_drop_threshold_percent, 40);
        assert_eq!(deserialized.speed_drop_duration_secs, 8);
        assert_eq!(deserialized.baseline_establish_secs, 20);
        assert_eq!(deserialized.stagnation_threshold_kbps, 5);
        assert_eq!(deserialized.stagnation_ratio_percent, 90);
        assert_eq!(deserialized.min_threads_for_detection, 2);
        assert_eq!(deserialized.startup_delay_secs, 5);
    }

    #[test]
    fn test_download_config_with_cdn_refresh() {
        // 测试 DownloadConfig 包含 cdn_refresh 的序列化/反序列化
        let download_config = DownloadConfig {
            download_dir: std::env::current_dir().unwrap().join("downloads"),
            default_directory: None,
            recent_directory: None,
            ask_each_time: true,
            max_global_threads: 10,
            chunk_size_mb: 5,
            max_concurrent_tasks: 5,
            max_retries: 3,
            cdn_refresh: CdnRefreshConfig {
                enabled: false,
                refresh_interval_minutes: 15,
                ..Default::default()
            },
        };

        // 验证 cdn_refresh 配置被正确包含
        assert_eq!(download_config.cdn_refresh.enabled, false);
        assert_eq!(download_config.cdn_refresh.refresh_interval_minutes, 15);
    }
}
