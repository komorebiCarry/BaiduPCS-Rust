// 会话管理和持久化

use crate::auth::{AccountSummary, UserAuth};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tokio::fs;
use tracing::{info, warn};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SessionStore {
    #[serde(default)]
    active_uid: Option<u64>,
    #[serde(default)]
    accounts: Vec<UserAuth>,
}

impl SessionStore {
    fn normalize(&mut self) {
        self.accounts.sort_by_key(|u| u.login_time);
        self.accounts.dedup_by_key(|u| u.uid);

        if let Some(active_uid) = self.active_uid {
            if self.accounts.iter().all(|u| u.uid != active_uid) {
                self.active_uid = self.accounts.last().map(|u| u.uid);
            }
        } else {
            self.active_uid = self.accounts.last().map(|u| u.uid);
        }
    }

    fn active_user(&self) -> Option<UserAuth> {
        let active_uid = self.active_uid?;
        self.accounts.iter().find(|u| u.uid == active_uid).cloned()
    }

    fn summaries(&self) -> Vec<AccountSummary> {
        self.accounts
            .iter()
            .map(|u| AccountSummary::from_user(u, self.active_uid))
            .collect()
    }

    fn upsert_active(&mut self, user_auth: UserAuth) {
        if let Some(existing) = self.accounts.iter_mut().find(|u| u.uid == user_auth.uid) {
            *existing = user_auth.clone();
        } else {
            self.accounts.push(user_auth.clone());
        }
        self.active_uid = Some(user_auth.uid);
        self.normalize();
    }
}

/// 会话管理器
pub struct SessionManager {
    /// 会话文件路径
    session_file: String,
    /// 当前会话（内存缓存）
    current_session: Option<UserAuth>,
    /// 多账号会话存储
    store: SessionStore,
    /// 是否已经从磁盘加载过
    loaded: bool,
}

impl SessionManager {
    /// 创建新的会话管理器
    ///
    /// # Arguments
    /// * `session_file` - 会话文件路径，默认为 "./config/session.json"
    pub fn new(session_file: Option<String>) -> Self {
        let session_file = session_file.unwrap_or_else(|| "./config/session.json".to_string());

        Self {
            session_file,
            current_session: None,
            store: SessionStore::default(),
            loaded: false,
        }
    }

    async fn ensure_loaded(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }

        self.load_session().await?;
        Ok(())
    }

    async fn persist_store(&self) -> Result<()> {
        info!("💾 保存会话到文件: {}", self.session_file);

        if let Some(parent) = Path::new(&self.session_file).parent() {
            info!("📁 创建目录: {:?}", parent);
            fs::create_dir_all(parent)
                .await
                .context("Failed to create config directory")?;
        }

        if self.store.accounts.is_empty() {
            if Path::new(&self.session_file).exists() {
                fs::remove_file(&self.session_file)
                    .await
                    .context("Failed to remove empty session file")?;
            }
            return Ok(());
        }

        let json =
            serde_json::to_string_pretty(&self.store).context("Failed to serialize sessions")?;
        fs::write(&self.session_file, &json)
            .await
            .context("Failed to write session file")?;

        info!(
            "✅ 会话保存完成: active_uid={:?}, accounts={}",
            self.store.active_uid,
            self.store.accounts.len()
        );
        Ok(())
    }

    fn parse_session_content(content: &str) -> Result<SessionStore> {
        let value: Value = serde_json::from_str(content).context("Failed to parse session json")?;

        let mut store = if value.get("accounts").is_some() {
            serde_json::from_value::<SessionStore>(value)
                .context("Failed to deserialize multi-account session")?
        } else {
            let user_auth: UserAuth =
                serde_json::from_value(value).context("Failed to deserialize legacy session")?;
            SessionStore {
                active_uid: Some(user_auth.uid),
                accounts: vec![user_auth],
            }
        };

        store.normalize();
        Ok(store)
    }

    /// 保存会话到文件
    ///
    /// 将用户认证信息加入账号列表，并设为当前激活账号。
    pub async fn save_session(&mut self, user_auth: &UserAuth) -> Result<()> {
        self.ensure_loaded().await?;

        self.store.upsert_active(user_auth.clone());
        self.current_session = self.store.active_user();
        self.persist_store().await?;

        Ok(())
    }

    /// 从文件加载会话
    ///
    /// 同时兼容旧版本的单账号 `UserAuth` JSON 和新版本多账号 JSON。
    pub async fn load_session(&mut self) -> Result<Option<UserAuth>> {
        info!("🔍 从文件加载会话: {}", self.session_file);

        if !Path::new(&self.session_file).exists() {
            warn!("❌ 会话文件不存在: {}", self.session_file);
            self.store = SessionStore::default();
            self.current_session = None;
            self.loaded = true;
            return Ok(None);
        }

        let content = fs::read_to_string(&self.session_file)
            .await
            .context("Failed to read session file")?;

        self.store = Self::parse_session_content(&content)?;
        self.current_session = self.store.active_user();
        self.loaded = true;

        if let Some(user) = &self.current_session {
            info!(
                "会话加载成功: active_uid={}, accounts={}",
                user.uid,
                self.store.accounts.len()
            );
        } else {
            warn!("会话文件中没有可用账号");
        }

        Ok(self.current_session.clone())
    }

    /// 清除所有会话
    ///
    /// 删除会话文件和内存缓存。
    pub async fn clear_session(&mut self) -> Result<()> {
        info!("清除全部会话");

        self.store = SessionStore::default();
        self.current_session = None;
        self.loaded = true;

        if Path::new(&self.session_file).exists() {
            fs::remove_file(&self.session_file)
                .await
                .context("Failed to remove session file")?;
        }

        info!("全部会话清除成功");
        Ok(())
    }

    /// 清除当前激活账号，并返回新的激活账号（如果仍有其他账号）
    pub async fn clear_active_session(&mut self) -> Result<Option<UserAuth>> {
        self.ensure_loaded().await?;

        let active_uid = match self.store.active_uid {
            Some(uid) => uid,
            None => return Ok(None),
        };

        self.remove_session(active_uid).await
    }

    /// 获取当前会话
    ///
    /// 返回内存中的当前激活会话，如果没有则尝试从文件加载。
    pub async fn get_session(&mut self) -> Result<Option<UserAuth>> {
        self.ensure_loaded().await?;
        Ok(self.current_session.clone())
    }

    /// 列出所有已保存账号
    pub async fn list_accounts(&mut self) -> Result<Vec<AccountSummary>> {
        self.ensure_loaded().await?;
        Ok(self.store.summaries())
    }

    /// 获取当前激活账号 UID
    pub async fn active_uid(&mut self) -> Result<Option<u64>> {
        self.ensure_loaded().await?;
        Ok(self.store.active_uid)
    }

    /// 切换当前激活账号
    pub async fn switch_session(&mut self, uid: u64) -> Result<Option<UserAuth>> {
        self.ensure_loaded().await?;

        let user = self.store.accounts.iter().find(|u| u.uid == uid).cloned();

        if let Some(user) = user {
            self.store.active_uid = Some(uid);
            self.current_session = Some(user.clone());
            self.persist_store().await?;
            Ok(Some(user))
        } else {
            Ok(None)
        }
    }

    /// 移除指定账号，并返回新的激活账号（如果仍有其他账号）
    pub async fn remove_session(&mut self, uid: u64) -> Result<Option<UserAuth>> {
        self.ensure_loaded().await?;

        let original_len = self.store.accounts.len();
        self.store.accounts.retain(|u| u.uid != uid);
        if self.store.accounts.len() == original_len {
            return Err(anyhow!("账号不存在: {}", uid));
        }

        if self.store.active_uid == Some(uid) {
            self.store.active_uid = self.store.accounts.last().map(|u| u.uid);
        }

        self.store.normalize();
        self.current_session = self.store.active_user();
        self.persist_store().await?;

        Ok(self.current_session.clone())
    }

    /// 检查是否已登录
    pub async fn is_logged_in(&mut self) -> bool {
        self.get_session().await.ok().flatten().is_some()
    }

    /// 获取BDUSS
    pub async fn get_bduss(&mut self) -> Option<String> {
        self.get_session().await.ok().flatten().map(|s| s.bduss)
    }

    /// 获取用户ID
    pub async fn get_uid(&mut self) -> Option<u64> {
        self.get_session().await.ok().flatten().map(|s| s.uid)
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn session_path() -> String {
        NamedTempFile::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .to_string()
    }

    fn user(uid: u64, username: &str) -> UserAuth {
        UserAuth::new(uid, username.to_string(), format!("bduss_{}", uid))
    }

    #[tokio::test]
    async fn test_session_save_and_load() {
        let path = session_path();
        let mut manager = SessionManager::new(Some(path.clone()));

        let user = user(123456, "test_user");
        manager.save_session(&user).await.unwrap();

        let loaded = manager.load_session().await.unwrap();
        assert!(loaded.is_some());

        let loaded_user = loaded.unwrap();
        assert_eq!(loaded_user.uid, 123456);
        assert_eq!(loaded_user.username, "test_user");

        let _ = manager.clear_session().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_multi_account_save_switch_and_remove() {
        let path = session_path();
        let mut manager = SessionManager::new(Some(path.clone()));

        manager.save_session(&user(1, "first")).await.unwrap();
        manager.save_session(&user(2, "second")).await.unwrap();

        let accounts = manager.list_accounts().await.unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(manager.active_uid().await.unwrap(), Some(2));
        assert!(accounts.iter().any(|a| a.uid == 2 && a.is_active));

        let switched = manager.switch_session(1).await.unwrap().unwrap();
        assert_eq!(switched.username, "first");
        assert_eq!(manager.active_uid().await.unwrap(), Some(1));

        let next = manager.remove_session(1).await.unwrap().unwrap();
        assert_eq!(next.uid, 2);
        assert_eq!(manager.active_uid().await.unwrap(), Some(2));

        let reloaded = SessionManager::new(Some(path.clone()))
            .load_session()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.uid, 2);

        let _ = manager.clear_session().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_legacy_single_account_session_is_loaded() {
        let path = session_path();
        let legacy = user(9, "legacy");
        std::fs::write(&path, serde_json::to_string(&legacy).unwrap()).unwrap();

        let mut manager = SessionManager::new(Some(path.clone()));
        let loaded = manager.load_session().await.unwrap().unwrap();
        assert_eq!(loaded.uid, 9);

        let accounts = manager.list_accounts().await.unwrap();
        assert_eq!(accounts.len(), 1);
        assert!(accounts[0].is_active);

        let _ = manager.clear_session().await;
        let _ = std::fs::remove_file(path);
    }
}
