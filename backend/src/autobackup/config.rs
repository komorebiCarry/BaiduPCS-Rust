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
    /// 上传冲突策略（仅用于 Upload 备份）
    #[serde(default)]
    pub upload_conflict_strategy: Option<crate::uploader::conflict::UploadConflictStrategy>,
    /// 下载冲突策略（仅用于 Download 备份）
    #[serde(default)]
    pub download_conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
    /// 是否启用
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 创建时间
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// 更新时间
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl BackupConfig {
    /// 检测并应用迁移逻辑
    ///
    /// 对于旧备份配置（没有策略字段），根据方向应用默认值：
    /// - Upload 备份：使用 SmartDedup 策略
    /// - Download 备份：使用 Overwrite 策略
    pub fn apply_migration(&mut self) {
        let mut migrated = false;

        // 检测并迁移上传策略
        if self.direction == BackupDirection::Upload && self.upload_conflict_strategy.is_none() {
            self.upload_conflict_strategy = Some(crate::uploader::conflict::UploadConflictStrategy::SmartDedup);
            migrated = true;
            tracing::info!(
                "备份配置迁移: 备份任务 '{}' (ID: {}) 缺少上传冲突策略，应用默认值 SmartDedup",
                self.name,
                self.id
            );
        }

        // 检测并迁移下载策略
        if self.direction == BackupDirection::Download && self.download_conflict_strategy.is_none() {
            self.download_conflict_strategy = Some(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);
            migrated = true;
            tracing::info!(
                "备份配置迁移: 备份任务 '{}' (ID: {}) 缺少下载冲突策略，应用默认值 Overwrite",
                self.name,
                self.id
            );
        }

        if migrated {
            self.updated_at = chrono::Utc::now();
            tracing::info!(
                "备份配置迁移完成: 备份任务 '{}' (ID: {}) 已更新。\
                 您可以在备份配置页面修改冲突策略。",
                self.name,
                self.id
            );
        }
    }

    /// 获取有效的上传冲突策略（考虑默认值）
    pub fn effective_upload_strategy(&self) -> crate::uploader::conflict::UploadConflictStrategy {
        self.upload_conflict_strategy
            .unwrap_or(crate::uploader::conflict::UploadConflictStrategy::SmartDedup)
    }

    /// 获取有效的下载冲突策略（考虑默认值）
    pub fn effective_download_strategy(&self) -> crate::uploader::conflict::DownloadConflictStrategy {
        self.download_conflict_strategy
            .unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite)
    }
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
    /// 上传冲突策略
    #[serde(default)]
    pub upload_conflict_strategy: Option<crate::uploader::conflict::UploadConflictStrategy>,
    /// 下载冲突策略
    #[serde(default)]
    pub download_conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
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
    /// 上传冲突策略
    pub upload_conflict_strategy: Option<crate::uploader::conflict::UploadConflictStrategy>,
    /// 下载冲突策略
    pub download_conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
    /// 是否启用（注意：加密选项创建后不可更改）
    pub enabled: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use crate::uploader::conflict::{UploadConflictStrategy, DownloadConflictStrategy};

    // 生成器：上传冲突策略
    fn prop_upload_strategy() -> impl Strategy<Value = Option<UploadConflictStrategy>> {
        prop_oneof![
            Just(None),
            Just(Some(UploadConflictStrategy::SmartDedup)),
            Just(Some(UploadConflictStrategy::AutoRename)),
            Just(Some(UploadConflictStrategy::Overwrite)),
        ]
    }

    // 生成器：下载冲突策略
    fn prop_download_strategy() -> impl Strategy<Value = Option<DownloadConflictStrategy>> {
        prop_oneof![
            Just(None),
            Just(Some(DownloadConflictStrategy::Overwrite)),
            Just(Some(DownloadConflictStrategy::Skip)),
            Just(Some(DownloadConflictStrategy::AutoRename)),
        ]
    }

    // 生成器：备份方向
    fn prop_backup_direction() -> impl Strategy<Value = BackupDirection> {
        prop_oneof![
            Just(BackupDirection::Upload),
            Just(BackupDirection::Download),
        ]
    }

    // Feature: file-conflict-strategy, Property 5: 备份任务策略持久化
    // **Validates: Requirements 3.3, 6.1**
    proptest! {
        #[test]
        fn test_backup_config_strategy_serialization_roundtrip(
            upload_strategy in prop_upload_strategy(),
            download_strategy in prop_download_strategy(),
            direction in prop_backup_direction()
        ) {
            // 创建备份配置
            let config = BackupConfig {
                id: "test-backup".to_string(),
                name: "Test Backup".to_string(),
                local_path: PathBuf::from("/test/local"),
                remote_path: "/test/remote".to_string(),
                direction,
                watch_config: WatchConfig::default(),
                poll_config: PollConfig::default(),
                filter_config: FilterConfig::default(),
                encrypt_enabled: false,
                upload_conflict_strategy: upload_strategy,
                download_conflict_strategy: download_strategy,
                enabled: true,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };

            // 序列化为 JSON
            let serialized = serde_json::to_string(&config).unwrap();

            // 反序列化
            let deserialized: BackupConfig = serde_json::from_str(&serialized).unwrap();

            // 验证策略字段正确保存和加载
            prop_assert_eq!(config.upload_conflict_strategy, deserialized.upload_conflict_strategy);
            prop_assert_eq!(config.download_conflict_strategy, deserialized.download_conflict_strategy);
            prop_assert_eq!(config.direction, deserialized.direction);
        }

        #[test]
        fn test_create_backup_config_request_with_strategies(
            upload_strategy in prop_upload_strategy(),
            download_strategy in prop_download_strategy()
        ) {
            // 创建请求
            let request = CreateBackupConfigRequest {
                name: "Test Backup".to_string(),
                local_path: "/test/local".to_string(),
                remote_path: "/test/remote".to_string(),
                direction: BackupDirection::Upload,
                watch_config: WatchConfig::default(),
                poll_config: PollConfig::default(),
                filter_config: FilterConfig::default(),
                encrypt_enabled: false,
                upload_conflict_strategy: upload_strategy,
                download_conflict_strategy: download_strategy,
            };

            // 序列化为 JSON
            let serialized = serde_json::to_string(&request).unwrap();

            // 反序列化
            let deserialized: CreateBackupConfigRequest = serde_json::from_str(&serialized).unwrap();

            // 验证策略字段正确保存和加载
            prop_assert_eq!(request.upload_conflict_strategy, deserialized.upload_conflict_strategy);
            prop_assert_eq!(request.download_conflict_strategy, deserialized.download_conflict_strategy);
        }

        #[test]
        fn test_update_backup_config_request_with_strategies(
            upload_strategy in prop_upload_strategy(),
            download_strategy in prop_download_strategy()
        ) {
            // 创建更新请求
            let request = UpdateBackupConfigRequest {
                name: Some("Updated Backup".to_string()),
                local_path: None,
                remote_path: None,
                watch_config: None,
                poll_config: None,
                filter_config: None,
                upload_conflict_strategy: upload_strategy,
                download_conflict_strategy: download_strategy,
                enabled: Some(true),
            };

            // 序列化为 JSON
            let serialized = serde_json::to_string(&request).unwrap();

            // 反序列化
            let deserialized: UpdateBackupConfigRequest = serde_json::from_str(&serialized).unwrap();

            // 验证策略字段正确保存和加载
            prop_assert_eq!(request.upload_conflict_strategy, deserialized.upload_conflict_strategy);
            prop_assert_eq!(request.download_conflict_strategy, deserialized.download_conflict_strategy);
        }
    }

    #[test]
    fn test_backup_config_backward_compatibility() {
        // 模拟旧备份配置（没有策略字段）
        let old_config_json = r#"{
            "id": "test-backup",
            "name": "Test Backup",
            "local_path": "/test/local",
            "remote_path": "/test/remote",
            "direction": "upload",
            "watch_config": {
                "enabled": true,
                "debounce_ms": 3000
            },
            "poll_config": {
                "enabled": true,
                "mode": "interval",
                "interval_minutes": 30,
                "schedule_hour": null,
                "schedule_minute": null
            },
            "filter_config": {
                "include_extensions": [],
                "exclude_extensions": [],
                "exclude_directories": [],
                "max_file_size": 0,
                "min_file_size": 0
            },
            "encrypt_enabled": false,
            "enabled": true,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;

        // 反序列化旧配置
        let config: BackupConfig = serde_json::from_str(old_config_json).unwrap();

        // 验证策略字段为 None（向后兼容）
        assert_eq!(config.upload_conflict_strategy, None);
        assert_eq!(config.download_conflict_strategy, None);
    }

    #[test]
    fn test_backup_config_with_upload_strategy() {
        let config = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Upload,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: Some(UploadConflictStrategy::SmartDedup),
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // 序列化
        let json = serde_json::to_string(&config).unwrap();

        // 反序列化
        let deserialized: BackupConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.upload_conflict_strategy,
            Some(UploadConflictStrategy::SmartDedup)
        );
        assert_eq!(deserialized.download_conflict_strategy, None);
    }

    #[test]
    fn test_backup_config_with_download_strategy() {
        let config = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Download,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: None,
            download_conflict_strategy: Some(DownloadConflictStrategy::Overwrite),
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // 序列化
        let json = serde_json::to_string(&config).unwrap();

        // 反序列化
        let deserialized: BackupConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.upload_conflict_strategy, None);
        assert_eq!(
            deserialized.download_conflict_strategy,
            Some(DownloadConflictStrategy::Overwrite)
        );
    }

    // ========== 迁移逻辑测试 ==========

    #[test]
    fn test_backup_config_migration_upload() {
        // 创建旧的上传备份配置（没有策略字段）
        let mut config = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Upload Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Upload,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: None,
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // 应用迁移
        config.apply_migration();

        // 验证上传策略被设置为 SmartDedup
        assert_eq!(
            config.upload_conflict_strategy,
            Some(UploadConflictStrategy::SmartDedup)
        );
        assert_eq!(config.download_conflict_strategy, None);
    }

    #[test]
    fn test_backup_config_migration_download() {
        // 创建旧的下载备份配置（没有策略字段）
        let mut config = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Download Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Download,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: None,
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // 应用迁移
        config.apply_migration();

        // 验证下载策略被设置为 Overwrite
        assert_eq!(config.upload_conflict_strategy, None);
        assert_eq!(
            config.download_conflict_strategy,
            Some(DownloadConflictStrategy::Overwrite)
        );
    }

    #[test]
    fn test_backup_config_migration_no_change_if_already_set() {
        // 创建已有策略的配置
        let mut config = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Upload,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: Some(UploadConflictStrategy::AutoRename),
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let original_strategy = config.upload_conflict_strategy;

        // 应用迁移
        config.apply_migration();

        // 验证策略没有被改变
        assert_eq!(config.upload_conflict_strategy, original_strategy);
    }

    #[test]
    fn test_backup_config_effective_upload_strategy() {
        // 测试有策略的情况
        let config_with_strategy = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Upload,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: Some(UploadConflictStrategy::AutoRename),
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        assert_eq!(
            config_with_strategy.effective_upload_strategy(),
            UploadConflictStrategy::AutoRename
        );

        // 测试没有策略的情况（应返回默认值）
        let config_without_strategy = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Upload,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: None,
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        assert_eq!(
            config_without_strategy.effective_upload_strategy(),
            UploadConflictStrategy::SmartDedup
        );
    }

    #[test]
    fn test_backup_config_effective_download_strategy() {
        // 测试有策略的情况
        let config_with_strategy = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Download,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: None,
            download_conflict_strategy: Some(DownloadConflictStrategy::Skip),
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        assert_eq!(
            config_with_strategy.effective_download_strategy(),
            DownloadConflictStrategy::Skip
        );

        // 测试没有策略的情况（应返回默认值）
        let config_without_strategy = BackupConfig {
            id: "test-backup".to_string(),
            name: "Test Backup".to_string(),
            local_path: PathBuf::from("/test/local"),
            remote_path: "/test/remote".to_string(),
            direction: BackupDirection::Download,
            watch_config: WatchConfig::default(),
            poll_config: PollConfig::default(),
            filter_config: FilterConfig::default(),
            encrypt_enabled: false,
            upload_conflict_strategy: None,
            download_conflict_strategy: None,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        assert_eq!(
            config_without_strategy.effective_download_strategy(),
            DownloadConflictStrategy::Overwrite
        );
    }
}
