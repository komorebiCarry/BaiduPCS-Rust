//! 认证凭证存储模块
//!
//! 负责 `config/auth.json` 的读写，安全存储密码哈希、TOTP 密钥和恢复码。

use crate::web_auth::{AuthCredentials, WebAuthError};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::sync::RwLock;

/// 默认认证凭证文件路径
pub const DEFAULT_AUTH_STORE_PATH: &str = "config/auth.json";

/// 认证凭证存储
///
/// 负责管理 `config/auth.json` 文件的读写操作。
/// 所有敏感数据（密码哈希、TOTP 密钥、恢复码哈希）都以安全方式存储。
pub struct AuthStore {
    /// 存储文件路径
    path: PathBuf,
    /// 内存中的凭证缓存
    credentials: RwLock<AuthCredentials>,
}

impl AuthStore {
    /// 创建新的 AuthStore 实例
    ///
    /// # Arguments
    /// * `path` - 凭证文件路径
    ///
    /// # Returns
    /// 新的 AuthStore 实例（凭证为空）
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            credentials: RwLock::new(AuthCredentials::default()),
        }
    }

    /// 使用默认路径创建 AuthStore
    pub fn with_default_path() -> Self {
        Self::new(DEFAULT_AUTH_STORE_PATH)
    }

    /// 创建仅内存存储的 AuthStore（用于测试）
    ///
    /// 使用一个不存在的临时路径，所有操作都在内存中进行。
    #[cfg(test)]
    pub fn new_in_memory() -> Self {
        Self {
            path: PathBuf::from("/dev/null/auth.json"),
            credentials: RwLock::new(AuthCredentials::default()),
        }
    }

    /// 从文件加载凭证
    ///
    /// 如果文件不存在，返回空凭证。
    /// 如果文件存在但格式错误，返回错误。
    pub async fn load(&self) -> Result<(), WebAuthError> {
        if !self.path.exists() {
            tracing::debug!("认证凭证文件不存在，使用空凭证: {:?}", self.path);
            return Ok(());
        }

        let content = fs::read_to_string(&self.path)
            .await
            .map_err(|e| WebAuthError::StorageError(format!("读取凭证文件失败: {}", e)))?;

        let credentials: AuthCredentials = serde_json::from_str(&content)
            .map_err(|e| WebAuthError::StorageError(format!("解析凭证文件失败: {}", e)))?;

        let mut guard = self.credentials.write().await;
        *guard = credentials;

        tracing::info!("认证凭证已加载: {:?}", self.path);
        Ok(())
    }

    /// 保存凭证到文件
    ///
    /// 自动创建父目录（如果不存在）。
    pub async fn save(&self) -> Result<(), WebAuthError> {
        // 确保父目录存在
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| WebAuthError::StorageError(format!("创建目录失败: {}", e)))?;
        }

        let credentials = self.credentials.read().await;
        let content = serde_json::to_string_pretty(&*credentials)
            .map_err(|e| WebAuthError::StorageError(format!("序列化凭证失败: {}", e)))?;

        fs::write(&self.path, content)
            .await
            .map_err(|e| WebAuthError::StorageError(format!("写入凭证文件失败: {}", e)))?;

        tracing::info!("认证凭证已保存: {:?}", self.path);
        Ok(())
    }

    /// 获取凭证的只读引用
    pub async fn get_credentials(&self) -> AuthCredentials {
        self.credentials.read().await.clone()
    }

    /// 更新凭证
    ///
    /// 更新内存中的凭证并自动保存到文件。
    pub async fn update_credentials(
        &self,
        credentials: AuthCredentials,
    ) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            *guard = credentials;
        }
        self.save().await
    }

    /// 设置密码哈希
    pub async fn set_password_hash(&self, hash: String) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            guard.set_password_hash(hash);
        }
        self.save().await
    }

    /// 清除密码
    pub async fn clear_password(&self) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            guard.clear_password();
        }
        self.save().await
    }

    /// 设置 TOTP 密钥
    pub async fn set_totp_secret(&self, secret: String) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            guard.set_totp_secret(secret);
        }
        self.save().await
    }

    /// 清除 TOTP 配置
    pub async fn clear_totp(&self) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            guard.clear_totp();
        }
        self.save().await
    }

    /// 设置恢复码
    pub async fn set_recovery_codes(
        &self,
        codes: Vec<crate::web_auth::RecoveryCode>,
    ) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            guard.set_recovery_codes(codes);
        }
        self.save().await
    }

    /// 标记恢复码为已使用
    ///
    /// # Arguments
    /// * `index` - 恢复码在列表中的索引
    ///
    /// # Returns
    /// 如果索引有效且恢复码未使用，返回 Ok(())
    pub async fn mark_recovery_code_used(&self, index: usize) -> Result<(), WebAuthError> {
        {
            let mut guard = self.credentials.write().await;
            if index >= guard.recovery_codes.len() {
                return Err(WebAuthError::InvalidRecoveryCode);
            }
            if guard.recovery_codes[index].used {
                return Err(WebAuthError::InvalidRecoveryCode);
            }
            guard.recovery_codes[index].mark_used();
        }
        self.save().await
    }

    /// 检查是否设置了密码
    pub async fn has_password(&self) -> bool {
        self.credentials.read().await.has_password()
    }

    /// 检查是否启用了 TOTP
    pub async fn has_totp(&self) -> bool {
        self.credentials.read().await.has_totp()
    }

    /// 获取可用恢复码数量
    pub async fn available_recovery_codes_count(&self) -> usize {
        self.credentials
            .read()
            .await
            .available_recovery_codes_count()
    }

    /// 获取存储文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 删除凭证文件
    ///
    /// 同时清空内存中的凭证。
    pub async fn delete(&self) -> Result<(), WebAuthError> {
        // 清空内存中的凭证
        {
            let mut guard = self.credentials.write().await;
            *guard = AuthCredentials::default();
        }

        // 删除文件（如果存在）
        if self.path.exists() {
            fs::remove_file(&self.path)
                .await
                .map_err(|e| WebAuthError::StorageError(format!("删除凭证文件失败: {}", e)))?;
            tracing::info!("认证凭证文件已删除: {:?}", self.path);
        }

        Ok(())
    }
}

/// 创建默认的 AuthStore 实例
pub fn create_auth_store() -> AuthStore {
    AuthStore::with_default_path()
}

/// 创建指定路径的 AuthStore 实例
pub fn create_auth_store_with_path<P: AsRef<Path>>(path: P) -> AuthStore {
    AuthStore::new(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (AuthStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("auth.json");
        let store = AuthStore::new(&path);
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_new_store_has_empty_credentials() {
        let (store, _temp) = create_test_store().await;
        let creds = store.get_credentials().await;
        assert!(!creds.has_password());
        assert!(!creds.has_totp());
        assert_eq!(creds.available_recovery_codes_count(), 0);
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let (store, _temp) = create_test_store().await;

        // 设置一些凭证
        store
            .set_password_hash("test_hash".to_string())
            .await
            .unwrap();
        store
            .set_totp_secret("test_secret".to_string())
            .await
            .unwrap();

        // 创建新的 store 实例并加载
        let store2 = AuthStore::new(store.path());
        store2.load().await.unwrap();

        let creds = store2.get_credentials().await;
        assert_eq!(creds.password_hash, Some("test_hash".to_string()));
        assert_eq!(creds.totp_secret, Some("test_secret".to_string()));
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let (store, _temp) = create_test_store().await;
        // 加载不存在的文件应该成功（返回空凭证）
        store.load().await.unwrap();
        assert!(!store.has_password().await);
    }

    #[tokio::test]
    async fn test_set_and_clear_password() {
        let (store, _temp) = create_test_store().await;

        store
            .set_password_hash("hash123".to_string())
            .await
            .unwrap();
        assert!(store.has_password().await);

        store.clear_password().await.unwrap();
        assert!(!store.has_password().await);
    }

    #[tokio::test]
    async fn test_set_and_clear_totp() {
        let (store, _temp) = create_test_store().await;

        store
            .set_totp_secret("secret123".to_string())
            .await
            .unwrap();
        assert!(store.has_totp().await);

        store.clear_totp().await.unwrap();
        assert!(!store.has_totp().await);
    }

    #[tokio::test]
    async fn test_recovery_codes() {
        use crate::web_auth::RecoveryCode;

        let (store, _temp) = create_test_store().await;

        let codes = vec![
            RecoveryCode::new("hash1".to_string()),
            RecoveryCode::new("hash2".to_string()),
        ];
        store.set_recovery_codes(codes).await.unwrap();
        assert_eq!(store.available_recovery_codes_count().await, 2);

        store.mark_recovery_code_used(0).await.unwrap();
        assert_eq!(store.available_recovery_codes_count().await, 1);

        // 再次使用同一个码应该失败
        let result = store.mark_recovery_code_used(0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_store() {
        let (store, _temp) = create_test_store().await;

        store.set_password_hash("hash".to_string()).await.unwrap();
        assert!(store.path().exists());

        store.delete().await.unwrap();
        assert!(!store.path().exists());
        assert!(!store.has_password().await);
    }

    #[tokio::test]
    async fn test_update_credentials() {
        let (store, _temp) = create_test_store().await;

        let mut creds = AuthCredentials::default();
        creds.set_password_hash("new_hash".to_string());
        creds.set_totp_secret("new_secret".to_string());

        store.update_credentials(creds).await.unwrap();

        let loaded = store.get_credentials().await;
        assert_eq!(loaded.password_hash, Some("new_hash".to_string()));
        assert_eq!(loaded.totp_secret, Some("new_secret".to_string()));
    }
}
