// 会话管理和持久化

use crate::auth::UserAuth;
use anyhow::{Context, Result};
use std::path::Path;
use tokio::fs;
use tracing::{info, warn};

/// 会话管理器
pub struct SessionManager {
    /// 会话文件路径
    session_file: String,
    /// 当前会话（内存缓存）
    current_session: Option<UserAuth>,
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
        }
    }

    /// 保存会话到文件
    ///
    /// 将用户认证信息序列化为JSON并保存到文件
    pub async fn save_session(&mut self, user_auth: &UserAuth) -> Result<()> {
        info!("💾 保存会话到文件: {}", self.session_file);

        // 确保目录存在
        if let Some(parent) = Path::new(&self.session_file).parent() {
            info!("📁 创建目录: {:?}", parent);
            fs::create_dir_all(parent)
                .await
                .context("Failed to create config directory")?;
        }

        // 序列化为JSON
        let json =
            serde_json::to_string_pretty(user_auth).context("Failed to serialize session")?;
        info!("📝 序列化JSON成功，大小: {} bytes", json.len());

        // 写入文件
        fs::write(&self.session_file, &json)
            .await
            .context("Failed to write session file")?;
        info!("✅ 文件写入成功: {}", self.session_file);

        // 更新内存缓存
        self.current_session = Some(user_auth.clone());

        info!("✅ 会话保存完成");
        Ok(())
    }

    /// 从文件加载会话
    ///
    /// 读取会话文件并反序列化为UserAuth
    pub async fn load_session(&mut self) -> Result<Option<UserAuth>> {
        info!("🔍 从文件加载会话: {}", self.session_file);

        // 检查文件是否存在
        if !Path::new(&self.session_file).exists() {
            warn!("❌ 会话文件不存在: {}", self.session_file);
            return Ok(None);
        }

        // 读取文件内容
        let content = fs::read_to_string(&self.session_file)
            .await
            .context("Failed to read session file")?;

        // 反序列化
        let user_auth: UserAuth =
            serde_json::from_str(&content).context("Failed to deserialize session")?;

        // BDUSS 本地不做过期判断，由 verify_bduss 调百度 API 决定
        info!("会话加载成功: UID={}", user_auth.uid);

        // 更新内存缓存
        self.current_session = Some(user_auth.clone());

        Ok(Some(user_auth))
    }

    /// 清除会话
    ///
    /// 删除会话文件和内存缓存
    pub async fn clear_session(&mut self) -> Result<()> {
        info!("清除会话");

        // 删除文件
        if Path::new(&self.session_file).exists() {
            fs::remove_file(&self.session_file)
                .await
                .context("Failed to remove session file")?;
        }

        // 清除内存缓存
        self.current_session = None;

        info!("会话清除成功");
        Ok(())
    }

    /// 获取当前会话
    ///
    /// 返回内存中的会话，如果没有则尝试从文件加载
    pub async fn get_session(&mut self) -> Result<Option<UserAuth>> {
        // 如果内存中有会话，直接返回（BDUSS 是否失效由 verify_bduss 判断）
        if let Some(ref session) = self.current_session {
            return Ok(Some(session.clone()));
        }

        // 尝试从文件加载
        self.load_session().await
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

    #[tokio::test]
    async fn test_session_save_and_load() {
        let mut manager = SessionManager::new(Some("./test_session.json".to_string()));

        // 创建测试用户
        let user = UserAuth::new(123456, "test_user".to_string(), "test_bduss".to_string());

        // 保存会话
        manager.save_session(&user).await.unwrap();

        // 加载会话
        let loaded = manager.load_session().await.unwrap();
        assert!(loaded.is_some());

        let loaded_user = loaded.unwrap();
        assert_eq!(loaded_user.uid, 123456);
        assert_eq!(loaded_user.username, "test_user");

        // 清理测试文件
        let _ = manager.clear_session().await;
    }
}
