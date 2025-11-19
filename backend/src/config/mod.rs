// 配置管理模块

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;

/// 应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// 服务器配置
    pub server: ServerConfig,
    /// 下载配置
    pub download: DownloadConfig,
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
    /// 根据文件大小和 VIP 等级计算最优分片大小（字节）
    ///
    /// 自适应策略：
    /// - < 5MB: 256KB
    /// - 5-10MB: 512KB
    /// - 10-50MB: 1MB
    /// - 50-100MB: 2MB
    /// - 100-500MB: 4MB
    /// - >= 500MB: 5MB
    ///
    /// ⚠️ 重要：百度网盘限制单个 Range 请求最大 5MB，超过会返回 403 Forbidden
    ///
    /// 同时根据 VIP 等级限制最大分片大小：
    /// - 普通用户：最高 4MB
    /// - 普通会员：最高 5MB
    /// - SVIP：最高 5MB
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
        
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
                cors_origins: vec!["*".to_string()],
            },
            download: DownloadConfig {
                download_dir: PathBuf::from("downloads"),
                max_global_threads: svip_config.threads,
                chunk_size_mb: svip_config.chunk_size,
                max_concurrent_tasks: svip_config.max_tasks,
                max_retries: 3,
            },
        }
    }
}

impl AppConfig {
    /// 从文件加载配置
    pub async fn load_from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .await
            .context("Failed to read config file")?;

        let config: AppConfig = toml::from_str(&content).context("Failed to parse config file")?;

        Ok(config)
    }

    /// 保存配置到文件
    pub async fn save_to_file(&self, path: &str) -> Result<()> {
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

        Ok(())
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
        assert_eq!(config.server.port, 8080);
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
        assert_eq!(vip.chunk_size, 16);
        assert_eq!(vip.max_tasks, 3);

        // 测试 SVIP 推荐配置
        let svip = DownloadConfig::recommended_for_vip(VipType::Svip);
        assert_eq!(svip.threads, 10);
        assert_eq!(svip.chunk_size, 32);
        assert_eq!(svip.max_tasks, 5);
    }

    #[test]
    fn test_config_validation() {
        let mut config = DownloadConfig {
            download_dir: PathBuf::from("downloads"),
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
}
