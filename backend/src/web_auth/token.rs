//! Token 服务模块
//!
//! 实现 JWT Access Token 和 Refresh Token 的生成、验证和管理。
//! - Access Token: JWT 格式，有效期 15 分钟
//! - Refresh Token: 安全随机字符串，有效期 7 天
//! - 定时清理过期令牌，防止内存耗尽

use crate::web_auth::error::WebAuthError;
use crate::web_auth::types::{StoredRefreshToken, TokenClaims, TokenPair};
use dashmap::DashMap;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Access Token 有效期（秒）：15 分钟
pub const ACCESS_TOKEN_EXPIRY: i64 = 15 * 60;

/// Refresh Token 有效期（秒）：7 天
pub const REFRESH_TOKEN_EXPIRY: i64 = 7 * 24 * 60 * 60;

/// Refresh Token 前缀
const REFRESH_TOKEN_PREFIX: &str = "rt_";

/// Refresh Token 随机部分长度
const REFRESH_TOKEN_RANDOM_LENGTH: usize = 64;

/// 最大令牌数量（防止内存耗尽）
pub const MAX_TOKENS: usize = 1000;

/// 清理任务间隔（秒）：10 分钟
pub const TOKEN_CLEANUP_INTERVAL_SECS: u64 = 10 * 60;

/// Token 服务
pub struct TokenService {
    /// 活跃的 Refresh Token (token_hash -> StoredRefreshToken)
    refresh_tokens: DashMap<String, StoredRefreshToken>,
    /// JWT 签名密钥
    #[allow(dead_code)]
    jwt_secret: String,
    /// 编码密钥
    encoding_key: EncodingKey,
    /// 解码密钥
    decoding_key: DecodingKey,
    /// 清理任务取消令牌
    cleanup_cancel_token: RwLock<Option<CancellationToken>>,
}

impl TokenService {
    /// 创建新的 Token 服务
    ///
    /// # Arguments
    /// * `jwt_secret` - JWT 签名密钥，如果为空则自动生成
    pub fn new(jwt_secret: Option<String>) -> Self {
        let secret = jwt_secret.unwrap_or_else(|| Self::generate_random_secret());
        let encoding_key = EncodingKey::from_secret(secret.as_bytes());
        let decoding_key = DecodingKey::from_secret(secret.as_bytes());

        Self {
            refresh_tokens: DashMap::new(),
            jwt_secret: secret,
            encoding_key,
            decoding_key,
            cleanup_cancel_token: RwLock::new(None),
        }
    }

    /// 生成随机密钥
    fn generate_random_secret() -> String {
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
        hex::encode(bytes)
    }

    /// 启动定时清理任务
    ///
    /// 每 10 分钟清理一次过期令牌。
    /// 仅当认证模式不为 `None` 时应调用此方法。
    pub async fn start_cleanup_task(self: &Arc<Self>) {
        let mut guard = self.cleanup_cancel_token.write().await;

        // 如果已有清理任务在运行，先停止它
        if let Some(token) = guard.take() {
            token.cancel();
        }

        let cancel_token = CancellationToken::new();
        *guard = Some(cancel_token.clone());
        drop(guard);

        let service = Arc::clone(self);

        tokio::spawn(async move {
            debug!("TokenService cleanup task started");
            let interval = Duration::from_secs(TOKEN_CLEANUP_INTERVAL_SECS);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("TokenService cleanup task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(interval) => {
                        let before_count = service.active_token_count();
                        service.cleanup_expired();
                        let after_count = service.active_token_count();

                        if before_count != after_count {
                            debug!(
                                "TokenService cleanup: removed {} expired tokens ({} -> {})",
                                before_count - after_count,
                                before_count,
                                after_count
                            );
                        }
                    }
                }
            }
        });
    }

    /// 停止定时清理任务
    pub async fn stop_cleanup_task(&self) {
        let mut guard = self.cleanup_cancel_token.write().await;
        if let Some(token) = guard.take() {
            token.cancel();
            debug!("TokenService cleanup task stop requested");
        }
    }

    /// 检查是否已达到最大令牌数
    pub fn is_at_capacity(&self) -> bool {
        self.refresh_tokens.len() >= MAX_TOKENS
    }

    /// 生成令牌对
    ///
    /// 生成 Access Token (JWT) 和 Refresh Token，并存储 Refresh Token 信息。
    /// 如果已达到最大令牌数，将返回错误。
    pub fn generate_token_pair(&self) -> Result<TokenPair, WebAuthError> {
        // 检查容量
        if self.is_at_capacity() {
            warn!(
                "TokenService at capacity ({}), rejecting new token generation",
                MAX_TOKENS
            );
            return Err(WebAuthError::InternalError(
                "Token storage at capacity".to_string(),
            ));
        }

        let now = chrono::Utc::now().timestamp();
        let access_expires_at = now + ACCESS_TOKEN_EXPIRY;
        let refresh_expires_at = now + REFRESH_TOKEN_EXPIRY;

        // 生成 Access Token (JWT)
        let jti = uuid::Uuid::new_v4().to_string();
        let claims = TokenClaims {
            sub: "web_auth".to_string(),
            iat: now,
            exp: access_expires_at,
            jti: jti.clone(),
        };

        let access_token = encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| WebAuthError::InternalError(format!("JWT encode error: {}", e)))?;

        // 生成 Refresh Token
        let refresh_token = self.generate_refresh_token();
        let token_hash = Self::hash_token(&refresh_token);

        // 存储 Refresh Token 信息
        let stored = StoredRefreshToken {
            token_hash: token_hash.clone(),
            expires_at: refresh_expires_at,
            created_at: now,
        };
        self.refresh_tokens.insert(token_hash, stored);

        Ok(TokenPair {
            access_token,
            refresh_token,
            access_expires_at,
            refresh_expires_at,
        })
    }

    /// 验证 Access Token
    ///
    /// 验证 JWT 签名和过期时间
    pub fn verify_access_token(&self, token: &str) -> Result<TokenClaims, WebAuthError> {
        let mut validation = Validation::default();
        validation.set_required_spec_claims(&["sub", "iat", "exp", "jti"]);

        let token_data =
            decode::<TokenClaims>(token, &self.decoding_key, &validation).map_err(|e| {
                match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => WebAuthError::InvalidToken,
                    _ => WebAuthError::InvalidToken,
                }
            })?;

        Ok(token_data.claims)
    }

    /// 刷新令牌
    ///
    /// 使用 Refresh Token 获取新的令牌对，同时轮换 Refresh Token
    pub fn refresh_tokens(&self, refresh_token: &str) -> Result<TokenPair, WebAuthError> {
        let token_hash = Self::hash_token(refresh_token);
        let now = chrono::Utc::now().timestamp();

        // 验证并移除旧的 Refresh Token
        let stored = self
            .refresh_tokens
            .remove(&token_hash)
            .map(|(_, v)| v)
            .ok_or(WebAuthError::InvalidRefreshToken)?;

        // 检查是否过期
        if stored.expires_at < now {
            return Err(WebAuthError::InvalidRefreshToken);
        }

        // 生成新的令牌对
        self.generate_token_pair()
    }

    /// 撤销令牌
    ///
    /// 使 Refresh Token 失效
    pub fn revoke_token(&self, refresh_token: &str) -> Result<(), WebAuthError> {
        let token_hash = Self::hash_token(refresh_token);
        self.refresh_tokens.remove(&token_hash);
        Ok(())
    }

    /// 撤销所有令牌
    ///
    /// 清除所有活跃的 Refresh Token
    pub fn revoke_all_tokens(&self) {
        self.refresh_tokens.clear();
    }

    /// 获取活跃 Refresh Token 数量
    pub fn active_token_count(&self) -> usize {
        self.refresh_tokens.len()
    }

    /// 清理过期的 Refresh Token
    pub fn cleanup_expired(&self) {
        let now = chrono::Utc::now().timestamp();
        self.refresh_tokens.retain(|_, v| v.expires_at > now);
    }

    /// 检查 Refresh Token 是否有效
    pub fn is_refresh_token_valid(&self, refresh_token: &str) -> bool {
        let token_hash = Self::hash_token(refresh_token);
        let now = chrono::Utc::now().timestamp();

        self.refresh_tokens
            .get(&token_hash)
            .map(|stored| stored.expires_at > now)
            .unwrap_or(false)
    }

    /// 生成 Refresh Token
    fn generate_refresh_token(&self) -> String {
        let mut rng = rand::thread_rng();
        let random_bytes: Vec<u8> = (0..REFRESH_TOKEN_RANDOM_LENGTH)
            .map(|_| rng.gen())
            .collect();
        format!("{}{}", REFRESH_TOKEN_PREFIX, hex::encode(random_bytes))
    }

    /// 哈希 Token
    fn hash_token(token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// 获取 JWT 密钥（用于测试）
    #[cfg(test)]
    pub fn get_jwt_secret(&self) -> &str {
        &self.jwt_secret
    }
}

impl Default for TokenService {
    fn default() -> Self {
        Self::new(None)
    }
}

/// 创建共享的 TokenService
pub fn create_token_service(jwt_secret: Option<String>) -> Arc<TokenService> {
    Arc::new(TokenService::new(jwt_secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_token_pair() {
        let service = TokenService::new(Some("test_secret".to_string()));
        let pair = service.generate_token_pair().unwrap();

        assert!(!pair.access_token.is_empty());
        assert!(pair.refresh_token.starts_with(REFRESH_TOKEN_PREFIX));
        assert!(pair.access_expires_at > chrono::Utc::now().timestamp());
        assert!(pair.refresh_expires_at > pair.access_expires_at);
    }

    #[test]
    fn test_verify_access_token() {
        let service = TokenService::new(Some("test_secret".to_string()));
        let pair = service.generate_token_pair().unwrap();

        let claims = service.verify_access_token(&pair.access_token).unwrap();
        assert_eq!(claims.sub, "web_auth");
        assert!(!claims.jti.is_empty());
    }

    #[test]
    fn test_verify_invalid_token() {
        let service = TokenService::new(Some("test_secret".to_string()));
        let result = service.verify_access_token("invalid_token");
        assert!(matches!(result, Err(WebAuthError::InvalidToken)));
    }

    #[test]
    fn test_refresh_tokens() {
        let service = TokenService::new(Some("test_secret".to_string()));
        let pair1 = service.generate_token_pair().unwrap();

        // 刷新令牌
        let pair2 = service.refresh_tokens(&pair1.refresh_token).unwrap();

        // 新令牌应该有效
        assert!(service.verify_access_token(&pair2.access_token).is_ok());

        // 旧的 Refresh Token 应该失效
        let result = service.refresh_tokens(&pair1.refresh_token);
        assert!(matches!(result, Err(WebAuthError::InvalidRefreshToken)));
    }

    #[test]
    fn test_revoke_token() {
        let service = TokenService::new(Some("test_secret".to_string()));
        let pair = service.generate_token_pair().unwrap();

        assert!(service.is_refresh_token_valid(&pair.refresh_token));

        service.revoke_token(&pair.refresh_token).unwrap();

        assert!(!service.is_refresh_token_valid(&pair.refresh_token));
    }

    #[test]
    fn test_revoke_all_tokens() {
        let service = TokenService::new(Some("test_secret".to_string()));

        // 生成多个令牌
        let pair1 = service.generate_token_pair().unwrap();
        let pair2 = service.generate_token_pair().unwrap();

        assert_eq!(service.active_token_count(), 2);

        service.revoke_all_tokens();

        assert_eq!(service.active_token_count(), 0);
        assert!(!service.is_refresh_token_valid(&pair1.refresh_token));
        assert!(!service.is_refresh_token_valid(&pair2.refresh_token));
    }

    #[test]
    fn test_token_hash_consistency() {
        let token = "rt_test_token_12345";
        let hash1 = TokenService::hash_token(token);
        let hash2 = TokenService::hash_token(token);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_tokens_different_hashes() {
        let hash1 = TokenService::hash_token("token1");
        let hash2 = TokenService::hash_token("token2");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_is_at_capacity() {
        let service = TokenService::new(Some("test_secret".to_string()));
        assert!(!service.is_at_capacity());

        // We can't easily test MAX_TOKENS without creating that many entries,
        // but we can verify the method works
        assert_eq!(service.active_token_count(), 0);
    }
}
