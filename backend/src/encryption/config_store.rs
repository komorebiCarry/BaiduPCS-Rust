//! age 口令配置存储。
//!
//! `encryption.json` 只保存用户明确配置的 age 口令及其时间信息。
//! 不保存算法选择、密钥历史、轮换数据，也不读取旧的密钥格式。

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// 当前唯一的 age 口令配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionKeyConfig {
    /// 用户提供的 age 口令。
    pub passphrase: String,
    /// 口令配置时间（Unix 时间戳，毫秒）。
    pub created_at: i64,
    /// 口令最后使用时间（Unix 时间戳，毫秒）。
    pub last_used_at: Option<i64>,
}

impl EncryptionKeyConfig {
    fn validate(&self) -> Result<()> {
        if self.passphrase.trim().is_empty() {
            return Err(anyhow!("age 口令不能为空"));
        }
        Ok(())
    }
}

/// 加密配置存储管理器。
#[derive(Debug)]
pub struct EncryptionConfigStore {
    config_path: PathBuf,
}

impl EncryptionConfigStore {
    pub fn new(config_dir: &Path) -> Self {
        Self {
            config_path: config_dir.join("encryption.json"),
        }
    }

    /// 只有可解析且非空的用户口令配置才算已配置。
    pub fn has_key(&self) -> bool {
        matches!(self.load(), Ok(Some(_)))
    }

    /// 加载严格的 age 口令配置。
    pub fn load(&self) -> Result<Option<EncryptionKeyConfig>> {
        if !self.config_path.exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(&self.config_path)
            .map_err(|e| anyhow!("读取 age 口令配置失败: {}", e))?;
        let config: EncryptionKeyConfig = serde_json::from_str(&content).map_err(|e| {
            anyhow!(
                "解析 age 口令配置失败：仅支持新的 passphrase 配置，不兼容旧密钥/算法格式: {}",
                e
            )
        })?;
        config.validate()?;
        Ok(Some(config))
    }

    pub fn save(&self, config: &EncryptionKeyConfig) -> Result<()> {
        config.validate()?;
        if let Some(parent) = self.config_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow!("创建加密配置目录失败: {}", e))?;
        }

        let content = serde_json::to_string_pretty(config)
            .map_err(|e| anyhow!("序列化 age 口令配置失败: {}", e))?;
        std::fs::write(&self.config_path, content)
            .map_err(|e| anyhow!("写入 age 口令配置失败: {}", e))?;
        Ok(())
    }

    /// 保存用户新提供的口令。新口令会直接替换当前口令，不保留历史口令。
    pub fn set_passphrase(&self, passphrase: String) -> Result<EncryptionKeyConfig> {
        let config = EncryptionKeyConfig {
            passphrase,
            created_at: chrono::Utc::now().timestamp_millis(),
            last_used_at: None,
        };
        self.save(&config)?;
        Ok(config)
    }

    pub fn update_last_used(&self) -> Result<()> {
        if let Some(mut config) = self.load()? {
            config.last_used_at = Some(chrono::Utc::now().timestamp_millis());
            self.save(&config)?;
        }
        Ok(())
    }

    pub fn delete(&self) -> Result<()> {
        if self.config_path.exists() {
            std::fs::remove_file(&self.config_path)
                .map_err(|e| anyhow!("删除 age 口令配置失败: {}", e))?;
        }
        Ok(())
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn saves_and_loads_only_user_passphrase() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());
        let saved = store.set_passphrase("user supplied passphrase".to_string()).unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.passphrase, saved.passphrase);
        assert!(!store.config_path().to_string_lossy().is_empty());
        assert!(store.has_key());
    }

    #[test]
    fn rejects_old_key_and_algorithm_shape() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());
        std::fs::write(
            store.config_path(),
            r#"{"current_key":{"master_key":"old","algorithm":"age","key_version":1},"key_history":[]}"#,
        )
        .unwrap();

        let error = store.load().unwrap_err().to_string();
        assert!(error.contains("不兼容旧密钥/算法格式"));
        assert!(!store.has_key());
    }

    #[test]
    fn replacing_passphrase_does_not_keep_history() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());
        store.set_passphrase("first user passphrase".to_string()).unwrap();
        store.set_passphrase("second user passphrase".to_string()).unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.passphrase, "second user passphrase");
        let json = std::fs::read_to_string(store.config_path()).unwrap();
        assert!(!json.contains("history"));
        assert!(!json.contains("algorithm"));
    }

    #[test]
    fn rejects_empty_passphrase() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());
        assert!(store.set_passphrase("   ".to_string()).is_err());
    }

    #[test]
    fn delete_removes_config() {
        let dir = tempdir().unwrap();
        let store = EncryptionConfigStore::new(dir.path());
        store.set_passphrase("user supplied passphrase".to_string()).unwrap();
        store.delete().unwrap();
        assert!(!store.config_path().exists());
        assert!(!store.has_key());
    }
}
