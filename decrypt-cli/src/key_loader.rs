//! 严格加载单一用户 age 口令。

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::types::{DecryptError, EncryptionConfig, EncryptionKeyInfo};

/// age 口令加载器。
#[derive(Debug, Clone)]
pub struct KeyLoader {
    config: EncryptionConfig,
    key: EncryptionKeyInfo,
}

impl KeyLoader {
    /// 从新的 encryption.json 加载用户口令。
    ///
    /// 旧的 `current_key/key_history/master_key/algorithm` 格式会被拒绝，
    /// 不会尝试转换、遍历或回退到其他口令。
    pub fn load(path: &Path) -> Result<Self, DecryptError> {
        let file = File::open(path).map_err(|e| {
            DecryptError::IoError(std::io::Error::new(
                e.kind(),
                format!("无法打开 age 口令文件 '{}': {}", path.display(), e),
            ))
        })?;

        let config: EncryptionConfig = serde_json::from_reader(BufReader::new(file)).map_err(|e| {
            DecryptError::InvalidFormat(format!(
                "age 口令文件 '{}' 格式无效（只接受 passphrase，不兼容旧密钥/算法格式）: {}",
                path.display(), e
            ))
        })?;

        let key = EncryptionKeyInfo {
            passphrase: config.passphrase.clone(),
        };
        if !key.is_valid() {
            return Err(DecryptError::InvalidFormat(
                "age 口令文件中的 passphrase 不能为空".to_string(),
            ));
        }

        Ok(Self { config, key })
    }

    #[allow(dead_code)]
    pub fn from_config(config: EncryptionConfig) -> Self {
        let key = EncryptionKeyInfo {
            passphrase: config.passphrase.clone(),
        };
        Self { config, key }
    }

    /// 返回唯一的用户口令。
    pub fn current_key(&self) -> Option<&EncryptionKeyInfo> {
        self.key.is_valid().then_some(&self.key)
    }

    pub fn key_count(&self) -> usize {
        usize::from(self.current_key().is_some())
    }

    pub fn has_keys(&self) -> bool {
        self.current_key().is_some()
    }

    #[allow(dead_code)]
    pub fn passphrase(&self) -> &str {
        &self.config.passphrase
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &EncryptionConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn loads_one_user_passphrase() {
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"{{"passphrase":"user supplied passphrase","created_at":1,"last_used_at":null}}"#
        )
        .unwrap();

        let loader = KeyLoader::load(file.path()).unwrap();
        assert_eq!(loader.key_count(), 1);
        assert_eq!(loader.current_key().unwrap().passphrase, "user supplied passphrase");
    }

    #[test]
    fn rejects_old_key_history_shape() {
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"{{"current_key":{{"master_key":"old","algorithm":"age","key_version":1}},"key_history":[]}}"#
        )
        .unwrap();

        let error = KeyLoader::load(file.path()).unwrap_err().to_string();
        assert!(error.contains("不兼容旧密钥/算法格式"));
    }
}
