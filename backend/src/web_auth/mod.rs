//! Web 访问认证模块
//!
//! 提供 Web 界面的访问保护机制，支持密码认证和 TOTP 双因素认证。

pub mod error;
pub mod handlers;
pub mod middleware;
pub mod password;
pub mod rate_limiter;
pub mod recovery;
pub mod state;
pub mod store;
pub mod token;
pub mod totp;
pub mod types;

// 导出核心类型
pub use error::{ErrorDetails, ErrorResponse, WebAuthError};
pub use handlers::{
    get_config, login, logout, refresh, regenerate_recovery_codes, set_password, status,
    totp_disable, totp_setup, totp_verify, update_config,
};
pub use middleware::{
    web_auth_middleware, AuthErrorResponse, AuthenticatedUser, OptionalAuthenticatedUser,
};
pub use password::{PasswordManager, MIN_PASSWORD_LENGTH};
pub use rate_limiter::{
    create_rate_limiter, RateLimiter, ATTEMPT_WINDOW, CLEANUP_INTERVAL_SECS, LOCKOUT_DURATION,
    MAX_FAILED_ATTEMPTS, MAX_RECORDS,
};
pub use recovery::{RecoveryCodeManager, RECOVERY_CODE_COUNT};
pub use state::WebAuthState;
pub use store::{
    create_auth_store, create_auth_store_with_path, AuthStore, DEFAULT_AUTH_STORE_PATH,
};
pub use token::{
    create_token_service, TokenService, ACCESS_TOKEN_EXPIRY, MAX_TOKENS, REFRESH_TOKEN_EXPIRY,
    TOKEN_CLEANUP_INTERVAL_SECS,
};
pub use totp::{TOTPManager, DEFAULT_ISSUER, TOTP_DIGITS, TOTP_SKEW, TOTP_STEP};
pub use types::{
    AuthCredentials, AuthMode, LoginAttempt, RecoveryCode, StoredRefreshToken, TokenClaims,
    TokenPair, WebAuthConfig,
};
