//! Web 认证状态管理模块
//!
//! 管理认证系统的运行时状态，包括：
//! - 认证配置
//! - 凭证存储
//! - 令牌服务
//! - 速率限制器
//! - 清理任务生命周期管理

use crate::web_auth::rate_limiter::RateLimiter;
use crate::web_auth::store::AuthStore;
use crate::web_auth::token::TokenService;
use crate::web_auth::types::{AuthCredentials, AuthMode, WebAuthConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Web 认证状态
///
/// 包含认证系统所需的所有运行时状态和服务。
pub struct WebAuthState {
    /// 认证配置
    pub config: Arc<RwLock<WebAuthConfig>>,
    /// 认证凭证
    pub credentials: Arc<RwLock<AuthCredentials>>,
    /// 令牌服务
    pub token_service: Arc<TokenService>,
    /// 速率限制器
    pub rate_limiter: Arc<RateLimiter>,
    /// 凭证存储
    pub auth_store: Arc<AuthStore>,
}

impl WebAuthState {
    /// 创建新的认证状态
    ///
    /// # Arguments
    /// * `config` - 初始认证配置
    /// * `credentials` - 初始认证凭证
    /// * `jwt_secret` - JWT 签名密钥（可选，为空则自动生成）
    /// * `auth_store` - 凭证存储
    pub fn new(
        config: WebAuthConfig,
        credentials: AuthCredentials,
        jwt_secret: Option<String>,
        auth_store: Arc<AuthStore>,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            credentials: Arc::new(RwLock::new(credentials)),
            token_service: Arc::new(TokenService::new(jwt_secret)),
            rate_limiter: Arc::new(RateLimiter::new()),
            auth_store,
        }
    }

    /// 启动清理任务
    ///
    /// 仅当认证模式不为 `None` 时启动 RateLimiter 和 TokenService 的定时清理任务。
    pub async fn start_cleanup_tasks(&self) {
        let config = self.config.read().await;
        if config.mode != AuthMode::None {
            info!(
                "Starting cleanup tasks for web auth (mode: {:?})",
                config.mode
            );
            self.rate_limiter.start_cleanup_task().await;
            self.token_service.start_cleanup_task().await;
        } else {
            debug!("Auth mode is None, skipping cleanup task startup");
        }
    }

    /// 停止清理任务
    pub async fn stop_cleanup_tasks(&self) {
        info!("Stopping cleanup tasks for web auth");
        self.rate_limiter.stop_cleanup_task().await;
        self.token_service.stop_cleanup_task().await;
    }

    /// 处理认证配置变更
    ///
    /// 当认证模式发生变化时：
    /// - 从 `None` 切换到其他模式：启动清理任务
    /// - 从其他模式切换到 `None`：停止清理任务并清空内存数据
    /// - 任何配置变更：使所有会话失效
    ///
    /// # Arguments
    /// * `old_mode` - 变更前的认证模式
    /// * `new_mode` - 变更后的认证模式
    pub async fn on_config_changed(&self, old_mode: AuthMode, new_mode: AuthMode) {
        info!("Auth config changed: {:?} -> {:?}", old_mode, new_mode);

        // 使所有会话失效（无论模式如何变化）
        self.token_service.revoke_all_tokens();
        debug!("All tokens revoked due to config change");

        match (old_mode, new_mode) {
            // 从 None 切换到其他模式：启动清理任务
            (AuthMode::None, new) if new != AuthMode::None => {
                info!("Auth enabled, starting cleanup tasks");
                self.rate_limiter.start_cleanup_task().await;
                self.token_service.start_cleanup_task().await;
            }
            // 从其他模式切换到 None：停止清理任务并清空数据
            (old, AuthMode::None) if old != AuthMode::None => {
                info!("Auth disabled, stopping cleanup tasks and clearing data");
                self.rate_limiter.stop_cleanup_task().await;
                self.token_service.stop_cleanup_task().await;
                self.rate_limiter.clear_all();
                // token_service 已经通过 revoke_all_tokens 清空了
            }
            // 其他情况：模式变化但都不是 None，只需使会话失效（已在上面完成）
            _ => {
                debug!("Auth mode changed between non-None modes, sessions invalidated");
            }
        }
    }

    /// 更新认证配置
    ///
    /// 更新配置并触发相应的清理任务管理。
    ///
    /// # Arguments
    /// * `new_config` - 新的认证配置
    pub async fn update_config(&self, new_config: WebAuthConfig) {
        let old_mode = {
            let config = self.config.read().await;
            config.mode
        };

        let new_mode = new_config.mode;

        // 更新配置
        {
            let mut config = self.config.write().await;
            *config = new_config;
        }

        // 处理模式变更
        if old_mode != new_mode {
            self.on_config_changed(old_mode, new_mode).await;
        }
    }

    /// 获取当前认证模式
    pub async fn get_auth_mode(&self) -> AuthMode {
        self.config.read().await.mode
    }

    /// 检查认证是否启用
    pub async fn is_auth_enabled(&self) -> bool {
        self.config.read().await.mode.is_enabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_auth::store::AuthStore;

    fn create_test_state() -> WebAuthState {
        let auth_store = Arc::new(AuthStore::new_in_memory());
        WebAuthState::new(
            WebAuthConfig::default(),
            AuthCredentials::default(),
            Some("test_secret".to_string()),
            auth_store,
        )
    }

    #[tokio::test]
    async fn test_new_state() {
        let state = create_test_state();
        assert_eq!(state.get_auth_mode().await, AuthMode::None);
        assert!(!state.is_auth_enabled().await);
    }

    #[tokio::test]
    async fn test_update_config() {
        let state = create_test_state();

        // Generate a token first
        let pair = state.token_service.generate_token_pair().unwrap();
        assert!(state
            .token_service
            .is_refresh_token_valid(&pair.refresh_token));

        // Update config to enable password auth
        let new_config = WebAuthConfig {
            enabled: true,
            mode: AuthMode::Password,
        };
        state.update_config(new_config).await;

        // Token should be invalidated
        assert!(!state
            .token_service
            .is_refresh_token_valid(&pair.refresh_token));
        assert_eq!(state.get_auth_mode().await, AuthMode::Password);
    }

    #[tokio::test]
    async fn test_on_config_changed_none_to_password() {
        let state = create_test_state();

        // Simulate config change from None to Password
        state
            .on_config_changed(AuthMode::None, AuthMode::Password)
            .await;

        // Cleanup tasks should be started (we can't easily verify this without more infrastructure)
        // But we can verify the state is consistent
        assert_eq!(state.token_service.active_token_count(), 0);
    }

    #[tokio::test]
    async fn test_on_config_changed_password_to_none() {
        let state = create_test_state();

        // Add some data
        state.rate_limiter.record_failure("192.168.1.1");
        let _pair = state.token_service.generate_token_pair().unwrap();

        assert_eq!(state.rate_limiter.active_records_count(), 1);
        assert_eq!(state.token_service.active_token_count(), 1);

        // Simulate config change from Password to None
        state
            .on_config_changed(AuthMode::Password, AuthMode::None)
            .await;

        // Data should be cleared
        assert_eq!(state.rate_limiter.active_records_count(), 0);
        assert_eq!(state.token_service.active_token_count(), 0);
    }

    #[tokio::test]
    async fn test_on_config_changed_password_to_totp() {
        let state = create_test_state();

        // Add a token
        let pair = state.token_service.generate_token_pair().unwrap();
        assert!(state
            .token_service
            .is_refresh_token_valid(&pair.refresh_token));

        // Simulate config change from Password to Totp
        state
            .on_config_changed(AuthMode::Password, AuthMode::Totp)
            .await;

        // Token should be invalidated
        assert!(!state
            .token_service
            .is_refresh_token_valid(&pair.refresh_token));
    }
}
