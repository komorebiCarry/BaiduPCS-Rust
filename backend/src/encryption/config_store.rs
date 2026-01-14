//! 加密配置存储
//!
//! 实现加密配置分离存储（参考文档 1.5.5 节）：
//! - app.toml：仅存储 encryption_enabled（是否启用加密功能）
//! - encryption.json：存储密钥相关信息（master_key、algorithm、key_version 等）
//!
//! 支持历史密钥存储，用于密钥轮换后解密旧文件

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::autobackup::config::EncryptionAlgorithm;

/// 单个密钥信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKeyInfo {
    /// 主密钥（Base64 编码的 32 字节）
    pub master_key: String,
    /// 加密算法
    #[serde(default)]
    pub algorithm: EncryptionAlgorithm,
    /// 密钥版本（用于密钥轮换）
    #[serde(default = "default_key_version")]
    pub key_version: u32,
    /// 密钥创建时间（Unix 时间戳，毫秒）
    pub created_at: i64,
    /// 密钥最后使用时间（Unix 时间戳，毫秒）
    pub last_used_at: Option<i64>,
    /// 密钥废弃时间（Unix 时间戳，毫秒，仅历史密钥有此字段）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_at: Option<i64>,
}

/// 加密密钥配置（存储在 encryption.json）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKeyConfig {
    /// 当前使用的密钥
    #[serde(rename = "current_key")]
    pub current: EncryptionKeyInfo,
    
    /// 历史密钥（已废弃但保留用于解密旧文件）
    #[serde(rename = "key_history", default)]
    pub history: Vec<EncryptionKeyInfo>,
}

fn default_key_version() -> u32 {
    1
}

/// 加密配置存储管理器
#[derive(Debug)]
pub struct EncryptionConfigStore {
    /// encryption.json 文件路径
    config_path: PathBuf,
}

impl EncryptionConfigStore {
    /// 创建配置存储管理器
    pub fn new(config_dir: &Path) -> Self {
        Self {
            config_path: config_dir.join("encryption.json"),
        }
    }

    /// 检查密钥配置是否存在
    pub fn has_key(&self) -> bool {
        self.config_path.exists()
    }

    /// 加载密钥配置
    pub fn load(&self) -> Result<Option<EncryptionKeyConfig>> {
        if !self.config_path.exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(&self.config_path)
            .map_err(|e| anyhow!("读取加密配置失败: {}", e))?;

        let config: EncryptionKeyConfig = serde_json::from_str(&content)
            .map_err(|e| anyhow!("解析加密配置失败: {}", e))?;

        Ok(Some(config))
    }

    /// 保存密钥配置
    pub fn save(&self, config: &EncryptionKeyConfig) -> Result<()> {
        // 确保父目录存在
        if let Some(parent) = self.config_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow!("创建配置目录失败: {}", e))?;
        }

        let content = serde_json::to_string_pretty(config)
            .map_err(|e| anyhow!("序列化加密配置失败: {}", e))?;

        std::fs::write(&self.config_path, content)
            .map_err(|e| anyhow!("写入加密配置失败: {}", e))?;

        tracing::info!("Saved encryption config to {:?}", self.config_path);
        Ok(())
    }

    /// 删除密钥配置
    pub fn delete(&self) -> Result<()> {
        if self.config_path.exists() {
            std::fs::remove_file(&self.config_path)
                .map_err(|e| anyhow!("删除加密配置失败: {}", e))?;
            tracing::info!("Deleted encryption config at {:?}", self.config_path);
        }
        Ok(())
    }

    /// 更新最后使用时间
    pub fn update_last_used(&self) -> Result<()> {
        if let Some(mut config) = self.load()? {
            config.current.last_used_at = Some(chrono::Utc::now().timestamp_millis());
            self.save(&config)?;
        }
        Ok(())
    }

    /// 创建新的密钥配置
    pub fn create_new_key(
        &self,
        master_key: String,
        algorithm: EncryptionAlgorithm,
    ) -> Result<EncryptionKeyConfig> {
        let config = EncryptionKeyConfig {
            current: EncryptionKeyInfo {
                master_key,
                algorithm,
                key_version: 1,
                created_at: chrono::Utc::now().timestamp_millis(),
                last_used_at: None,
                deprecated_at: None,
            },
            history: Vec::new(),
        };

        self.save(&config)?;
        Ok(config)
    }

    /// 获取当前密钥
    pub fn get_current_key(&self) -> Result<Option<EncryptionKeyInfo>> {
        let config = self.load()?;
        Ok(config.map(|c| c.current))
    }

    /// 根据版本获取密钥（先查当前，再查历史）
    pub fn get_key_by_version(&self, version: u32) -> Result<Option<EncryptionKeyInfo>> {
        let config = match self.load()? {
            Some(c) => c,
            None => return Ok(None),
        };

        // 先查当前密钥
        if config.current.key_version == version {
            return Ok(Some(config.current));
        }

        // 再查历史密钥
        Ok(config.history.iter()
            .find(|k| k.key_version == version)
            .cloned())
    }

    /// 轮换密钥（将当前密钥移到历史，设置新密钥为当前）
    pub fn rotate_key(
        &self,
        new_master_key: String,
        new_algorithm: EncryptionAlgorithm,
    ) -> Result<EncryptionKeyConfig> {
        let mut config = self.load()?
            .ok_or_else(|| anyhow!("当前没有密钥配置"))?;

        // 将当前密钥移到历史
        let mut old_key = config.current.clone();
        old_key.deprecated_at = Some(chrono::Utc::now().timestamp_millis());
        config.history.push(old_key);

        // 设置新密钥为当前
        config.current = EncryptionKeyInfo {
            master_key: new_master_key,
            algorithm: new_algorithm,
            key_version: config.current.key_version + 1,
            created_at: chrono::Utc::now().timestamp_millis(),
            last_used_at: None,
            deprecated_at: None,
        };

        self.save(&config)?;
        tracing::info!(
            "密钥已轮换，新版本: {}, 历史密钥数: {}",
            config.current.key_version,
            config.history.len()
        );
        Ok(config)
    }

    /// 安全创建新密钥（保留历史）
    /// 
    /// 如果已有有效配置（当前密钥非空），将当前密钥移到历史，版本号递增
    /// 如果没有配置或当前密钥已废弃（为空），创建新的密钥配置但保留历史
    /// 
    /// 此方法用于密钥加载失败或需要重新配置加密时，确保不会丢失历史密钥
    pub fn create_new_key_safe(
        &self,
        master_key: String,
        algorithm: EncryptionAlgorithm,
    ) -> Result<EncryptionKeyConfig> {
        // 尝试加载现有配置
        match self.load()? {
            Some(existing) => {
                // 检查当前密钥是否有效（非空）
                if existing.current.master_key.is_empty() || existing.current.key_version == 0 {
                    // 当前密钥已废弃（为空），创建新密钥但保留历史
                    tracing::info!(
                        "当前密钥已废弃，创建新密钥并保留 {} 个历史密钥",
                        existing.history.len()
                    );
                    
                    // 计算新版本号：取历史密钥中最大版本号 + 1
                    let max_history_version = existing.history.iter()
                        .map(|k| k.key_version)
                        .max()
                        .unwrap_or(0);
                    let new_version = max_history_version + 1;
                    
                    let config = EncryptionKeyConfig {
                        current: EncryptionKeyInfo {
                            master_key,
                            algorithm,
                            key_version: new_version,
                            created_at: chrono::Utc::now().timestamp_millis(),
                            last_used_at: None,
                            deprecated_at: None,
                        },
                        history: existing.history,  // 保留历史密钥
                    };
                    
                    self.save(&config)?;
                    Ok(config)
                } else {
                    // 已有有效配置，使用 rotate_key 保留历史
                    tracing::info!("检测到现有有效密钥配置，使用轮换方式保留历史密钥");
                    self.rotate_key(master_key, algorithm)
                }
            }
            None => {
                // 没有现有配置，创建新的
                tracing::info!("没有现有密钥配置，创建新密钥");
                self.create_new_key(master_key, algorithm)
            }
        }
    }

    /// 废弃当前密钥（保留历史）
    /// 
    /// 将当前密钥移到历史，但不设置新的当前密钥。
    /// 用于用户删除加密密钥时，保留历史密钥以便解密旧文件。
    /// 
    /// 返回 Ok(true) 表示成功废弃当前密钥
    /// 返回 Ok(false) 表示没有当前密钥可废弃
    /// 
    /// # Requirements
    /// - 17.1: 删除密钥时保留历史密钥
    /// - 17.2: 只移除当前密钥，不删除历史
    pub fn deprecate_current_key(&self) -> Result<bool> {
        let config = match self.load()? {
            Some(c) => c,
            None => {
                tracing::info!("没有密钥配置，无需废弃");
                return Ok(false);
            }
        };

        // 检查当前密钥是否有效（非空）
        if config.current.master_key.is_empty() {
            tracing::info!("当前密钥已为空，无需废弃");
            return Ok(false);
        }

        // 将当前密钥移到历史
        let mut old_key = config.current.clone();
        old_key.deprecated_at = Some(chrono::Utc::now().timestamp_millis());
        
        let mut new_history = config.history;
        new_history.push(old_key);

        // 保存只有历史密钥的配置（当前密钥设为空）
        let new_config = EncryptionKeyConfig {
            current: EncryptionKeyInfo {
                master_key: String::new(),  // 空密钥表示无当前密钥
                algorithm: EncryptionAlgorithm::default(),
                key_version: 0,  // 版本 0 表示无效
                created_at: 0,
                last_used_at: None,
                deprecated_at: None,
            },
            history: new_history,
        };

        self.save(&new_config)?;
        tracing::info!(
            "当前密钥已废弃并移至历史，历史密钥数: {}",
            new_config.history.len()
        );
        Ok(true)
    }

    /// 强制删除所有密钥（包括历史）
    /// 
    /// 警告：这将导致无法解密任何已加密的文件
    /// 
    /// # Requirements
    /// - 17.3: 提供完全删除所有密钥的选项
    pub fn force_delete(&self) -> Result<()> {
        self.delete()
    }

    /// 检查是否有历史密钥
    pub fn has_history_keys(&self) -> Result<bool> {
        match self.load()? {
            Some(config) => Ok(!config.history.is_empty()),
            None => Ok(false),
        }
    }

    /// 获取配置文件路径
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        assert!(!store.has_key());

        let config = EncryptionKeyConfig {
            current: EncryptionKeyInfo {
                master_key: "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleTE=".to_string(),
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                key_version: 1,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: Vec::new(),
        };

        store.save(&config).unwrap();
        assert!(store.has_key());

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.current.master_key, config.current.master_key);
        assert_eq!(loaded.current.key_version, 1);
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        let config = EncryptionKeyConfig {
            current: EncryptionKeyInfo {
                master_key: "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleTE=".to_string(),
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                key_version: 1,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: Vec::new(),
        };

        store.save(&config).unwrap();
        assert!(store.has_key());

        store.delete().unwrap();
        assert!(!store.has_key());
    }

    #[test]
    fn test_get_current_key() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 没有配置时返回 None
        assert!(store.get_current_key().unwrap().is_none());

        // 创建配置
        store.create_new_key(
            "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleTE=".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        let key = store.get_current_key().unwrap().unwrap();
        assert_eq!(key.key_version, 1);
    }

    #[test]
    fn test_get_key_by_version() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 创建初始配置
        store.create_new_key(
            "key1".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 轮换密钥
        store.rotate_key(
            "key2".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 查找版本1（历史密钥）
        let key1 = store.get_key_by_version(1).unwrap().unwrap();
        assert_eq!(key1.master_key, "key1");
        assert!(key1.deprecated_at.is_some());

        // 查找版本2（当前密钥）
        let key2 = store.get_key_by_version(2).unwrap().unwrap();
        assert_eq!(key2.master_key, "key2");
        assert!(key2.deprecated_at.is_none());

        // 查找不存在的版本
        assert!(store.get_key_by_version(99).unwrap().is_none());
    }

    #[test]
    fn test_rotate_key() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 创建初始配置
        store.create_new_key(
            "original_key".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 轮换密钥
        let new_config = store.rotate_key(
            "new_key".to_string(),
            EncryptionAlgorithm::ChaCha20Poly1305,
        ).unwrap();

        // 验证新密钥
        assert_eq!(new_config.current.master_key, "new_key");
        assert_eq!(new_config.current.key_version, 2);
        assert_eq!(new_config.current.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);

        // 验证历史密钥
        assert_eq!(new_config.history.len(), 1);
        assert_eq!(new_config.history[0].master_key, "original_key");
        assert_eq!(new_config.history[0].key_version, 1);
        assert!(new_config.history[0].deprecated_at.is_some());
    }

    #[test]
    fn test_create_new_key_safe_without_existing() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 没有现有配置时，应该创建新密钥
        let config = store.create_new_key_safe(
            "new_key".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        assert_eq!(config.current.master_key, "new_key");
        assert_eq!(config.current.key_version, 1);
        assert!(config.history.is_empty());
    }

    #[test]
    fn test_create_new_key_safe_with_existing() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 先创建一个密钥
        store.create_new_key(
            "original_key".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 使用 create_new_key_safe 应该保留历史
        let config = store.create_new_key_safe(
            "new_key".to_string(),
            EncryptionAlgorithm::ChaCha20Poly1305,
        ).unwrap();

        // 验证新密钥
        assert_eq!(config.current.master_key, "new_key");
        assert_eq!(config.current.key_version, 2);
        assert_eq!(config.current.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);

        // 验证历史密钥被保留
        assert_eq!(config.history.len(), 1);
        assert_eq!(config.history[0].master_key, "original_key");
        assert_eq!(config.history[0].key_version, 1);
        assert!(config.history[0].deprecated_at.is_some());
    }

    #[test]
    fn test_deprecate_current_key_no_config() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 没有配置时，返回 false
        let result = store.deprecate_current_key().unwrap();
        assert!(!result);
        assert!(!store.has_key());
    }

    #[test]
    fn test_deprecate_current_key_preserves_history() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 创建初始密钥
        store.create_new_key(
            "original_key".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 废弃当前密钥
        let result = store.deprecate_current_key().unwrap();
        assert!(result);

        // 验证配置文件仍然存在
        assert!(store.has_key());

        // 验证当前密钥已清空
        let config = store.load().unwrap().unwrap();
        assert!(config.current.master_key.is_empty());
        assert_eq!(config.current.key_version, 0);

        // 验证历史密钥被保留
        assert_eq!(config.history.len(), 1);
        assert_eq!(config.history[0].master_key, "original_key");
        assert_eq!(config.history[0].key_version, 1);
        assert!(config.history[0].deprecated_at.is_some());
    }

    #[test]
    fn test_deprecate_current_key_with_existing_history() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 创建初始密钥
        store.create_new_key(
            "key1".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 轮换密钥（创建历史）
        store.rotate_key(
            "key2".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 废弃当前密钥
        let result = store.deprecate_current_key().unwrap();
        assert!(result);

        // 验证历史密钥都被保留
        let config = store.load().unwrap().unwrap();
        assert_eq!(config.history.len(), 2);
        
        // 第一个历史密钥
        assert_eq!(config.history[0].master_key, "key1");
        assert_eq!(config.history[0].key_version, 1);
        
        // 第二个历史密钥（刚废弃的）
        assert_eq!(config.history[1].master_key, "key2");
        assert_eq!(config.history[1].key_version, 2);
        assert!(config.history[1].deprecated_at.is_some());
    }

    #[test]
    fn test_deprecate_current_key_can_still_get_by_version() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 创建初始密钥
        store.create_new_key(
            "original_key".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 废弃当前密钥
        store.deprecate_current_key().unwrap();

        // 仍然可以通过版本号获取历史密钥
        let key = store.get_key_by_version(1).unwrap().unwrap();
        assert_eq!(key.master_key, "original_key");
        assert!(key.deprecated_at.is_some());
    }

    #[test]
    fn test_force_delete_removes_all() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 创建密钥并轮换
        store.create_new_key(
            "key1".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();
        store.rotate_key(
            "key2".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();

        // 强制删除
        store.force_delete().unwrap();

        // 验证配置文件已删除
        assert!(!store.has_key());
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn test_has_history_keys() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());

        // 没有配置时
        assert!(!store.has_history_keys().unwrap());

        // 创建密钥（无历史）
        store.create_new_key(
            "key1".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();
        assert!(!store.has_history_keys().unwrap());

        // 轮换密钥（有历史）
        store.rotate_key(
            "key2".to_string(),
            EncryptionAlgorithm::Aes256Gcm,
        ).unwrap();
        assert!(store.has_history_keys().unwrap());
    }
}
