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
    /// 文件系统配置
    #[serde(default)]
    pub filesystem: FilesystemConfig,
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
    /// 全局最大线程数（所有下载任务共享）
    pub max_global_threads: usize,
    /// 分片大小 (MB)
    pub chunk_size_mb: u64,
    /// 最大同时下载文件数
    pub max_concurrent_tasks: usize,
    /// 最大重试次数
    pub max_retries: u32,
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
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            max_global_threads: 10,
            chunk_size_mb: 4,           // 百度网盘上传分片最小 4MB
            max_concurrent_tasks: 5,
            max_retries: 3,
            skip_hidden_files: false,   // 默认不跳过隐藏文件
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
            VipType::Vip => 4,   // 普通会员最高 4MB
            VipType::Svip => 5,  // SVIP 最高 5MB
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
        let result = PathValidator::validate_with_docker_check(
            &self.download_dir,
            env_info.is_docker,
        );

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
                threads: 1,           // ⚠️ 普通用户只能1个线程！
                chunk_size: 4,        // 4MB 分片
                max_tasks: 1,         // 只能下载1个文件
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
                max_global_threads: svip_config.threads,
                chunk_size_mb: svip_config.chunk_size,
                max_concurrent_tasks: svip_config.max_tasks,
                max_retries: 3,
            },
            upload: UploadConfig::default(),
            filesystem: FilesystemConfig::default(),
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
        config.download.validate_download_dir()
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
        self.download.validate_download_dir()
            .context("保存配置失败：下载路径必须是绝对路径")?;

        // 2. 增强验证（存在性、可写性、可用空间）
        let validation_result = self.download.validate_download_dir_enhanced()
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
            max_global_threads: 5,
            chunk_size_mb: 10,
            max_concurrent_tasks: 2,
            max_retries: 3,
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
            max_global_threads: 5,
            chunk_size_mb: 10,
            max_concurrent_tasks: 2,
            max_retries: 3,
        };
        assert!(absolute_config.validate_download_dir().is_ok());

        // 测试相对路径验证（应该失败）
        let relative_config = DownloadConfig {
            download_dir: PathBuf::from("downloads"),
            max_global_threads: 5,
            chunk_size_mb: 10,
            max_concurrent_tasks: 2,
            max_retries: 3,
        };
        assert!(relative_config.validate_download_dir().is_err());

        // 测试平台特定的绝对路径
        #[cfg(target_os = "windows")]
        {
            let windows_config = DownloadConfig {
                download_dir: PathBuf::from("D:\\Downloads"),
                max_global_threads: 5,
                chunk_size_mb: 10,
                max_concurrent_tasks: 2,
                max_retries: 3,
            };
            assert!(windows_config.validate_download_dir().is_ok());
        }

        #[cfg(not(target_os = "windows"))]
        {
            let unix_config = DownloadConfig {
                download_dir: PathBuf::from("/app/downloads"),
                max_global_threads: 5,
                chunk_size_mb: 10,
                max_concurrent_tasks: 2,
                max_retries: 3,
            };
            assert!(unix_config.validate_download_dir().is_ok());
        }
    }
}
