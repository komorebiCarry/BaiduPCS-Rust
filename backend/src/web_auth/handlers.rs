//! Web 认证 API 处理器模块
//!
//! 实现 Web 访问认证相关的 API 端点：
//! - 登录 API (POST /api/v1/web-auth/login)
//! - 令牌刷新 API (POST /api/v1/web-auth/refresh)
//! - 登出 API (POST /api/v1/web-auth/logout)
//! - 认证状态 API (GET /api/v1/web-auth/status)
//! - 配置管理 API
//! - 密码管理 API
//! - TOTP 管理 API
//! - 恢复码管理 API

use crate::web_auth::{
    AuthCredentials, AuthMode, PasswordManager, RecoveryCodeManager, TOTPManager, WebAuthError,
    WebAuthState,
};
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};

// ============================================================================
// Request/Response Types
// ============================================================================

/// 登录请求
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// 密码（密码认证时必填）
    pub password: Option<String>,
    /// TOTP 验证码（2FA 认证时必填）
    pub totp_code: Option<String>,
    /// 恢复码（2FA 恢复时使用）
    pub recovery_code: Option<String>,
    /// 待验证令牌（密码验证后的临时令牌，用于 2FA 第二步）
    pub pending_token: Option<String>,
}

/// 登录响应
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    /// 状态：success, need_totp, error
    pub status: String,
    /// Access Token（成功时返回）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    /// Refresh Token（成功时返回）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Access Token 过期时间（成功时返回）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_expires_at: Option<i64>,
    /// Refresh Token 过期时间（成功时返回）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_expires_at: Option<i64>,
    /// 待验证令牌（需要 2FA 时返回）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_token: Option<String>,
    /// 错误信息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 剩余锁定时间（秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lockout_remaining: Option<u64>,
}

impl LoginResponse {
    fn success(
        access_token: String,
        refresh_token: String,
        access_expires_at: i64,
        refresh_expires_at: i64,
    ) -> Self {
        Self {
            status: "success".to_string(),
            access_token: Some(access_token),
            refresh_token: Some(refresh_token),
            access_expires_at: Some(access_expires_at),
            refresh_expires_at: Some(refresh_expires_at),
            pending_token: None,
            error: None,
            lockout_remaining: None,
        }
    }

    fn need_totp(pending_token: String) -> Self {
        Self {
            status: "need_totp".to_string(),
            access_token: None,
            refresh_token: None,
            access_expires_at: None,
            refresh_expires_at: None,
            pending_token: Some(pending_token),
            error: None,
            lockout_remaining: None,
        }
    }

    fn error(message: &str) -> Self {
        Self {
            status: "error".to_string(),
            access_token: None,
            refresh_token: None,
            access_expires_at: None,
            refresh_expires_at: None,
            pending_token: None,
            error: Some(message.to_string()),
            lockout_remaining: None,
        }
    }

    fn rate_limited(remaining: u64) -> Self {
        Self {
            status: "error".to_string(),
            access_token: None,
            refresh_token: None,
            access_expires_at: None,
            refresh_expires_at: None,
            pending_token: None,
            error: Some(format!("请求过于频繁，请在 {} 秒后重试", remaining)),
            lockout_remaining: Some(remaining),
        }
    }
}

/// 令牌刷新请求
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

/// 令牌刷新响应
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_at: i64,
    pub refresh_expires_at: i64,
}

/// 认证状态响应
#[derive(Debug, Serialize)]
pub struct AuthStatusResponse {
    /// 是否启用认证
    pub enabled: bool,
    /// 认证模式
    pub mode: AuthMode,
    /// 是否已认证（当前请求）
    pub authenticated: bool,
}

/// 认证配置响应
#[derive(Debug, Serialize)]
pub struct AuthConfigResponse {
    /// 是否启用认证
    pub enabled: bool,
    /// 认证模式
    pub mode: AuthMode,
    /// 是否已设置密码
    pub password_set: bool,
    /// 是否启用 TOTP
    pub totp_enabled: bool,
    /// 可用恢复码数量
    pub recovery_codes_count: usize,
}

/// 更新认证配置请求
#[derive(Debug, Deserialize)]
pub struct UpdateConfigRequest {
    /// 是否启用认证
    pub enabled: Option<bool>,
    /// 认证模式
    pub mode: Option<AuthMode>,
}

/// 设置密码请求
#[derive(Debug, Deserialize)]
pub struct SetPasswordRequest {
    /// 新密码
    pub password: String,
    /// 当前密码（修改密码时需要）
    pub current_password: Option<String>,
}

/// TOTP 设置响应
#[derive(Debug, Serialize)]
pub struct TotpSetupResponse {
    /// TOTP 密钥（Base32 编码）
    pub secret: String,
    /// QR 码（Base64 PNG）
    pub qr_code: String,
    /// 发行者名称
    pub issuer: String,
    /// 账户名称
    pub account: String,
}

/// TOTP 验证请求
#[derive(Debug, Deserialize)]
pub struct TotpVerifyRequest {
    /// TOTP 验证码
    pub code: String,
    /// TOTP 密钥（设置时需要）
    #[serde(default)]
    pub secret: Option<String>,
}

/// TOTP 禁用请求
#[derive(Debug, Deserialize)]
pub struct TotpDisableRequest {
    /// TOTP 验证码
    pub code: Option<String>,
    /// 恢复码
    pub recovery_code: Option<String>,
}

/// 重新生成恢复码请求
#[derive(Debug, Deserialize)]
pub struct RegenerateCodesRequest {
    /// TOTP 验证码
    pub totp_code: String,
}

/// 重新生成恢复码响应
#[derive(Debug, Serialize)]
pub struct RegenerateCodesResponse {
    /// 新的恢复码列表
    pub codes: Vec<String>,
}

/// 通用成功响应
#[derive(Debug, Serialize)]
pub struct SuccessResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl SuccessResponse {
    fn ok() -> Self {
        Self {
            success: true,
            message: None,
        }
    }

    fn with_message(message: &str) -> Self {
        Self {
            success: true,
            message: Some(message.to_string()),
        }
    }
}

/// 错误响应
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub code: u16,
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails>,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lockout_remaining: Option<u64>,
}

impl ErrorResponse {
    fn from_error(err: &WebAuthError) -> Self {
        let details = match err {
            WebAuthError::RateLimited(remaining) => Some(ErrorDetails {
                lockout_remaining: Some(*remaining),
            }),
            _ => None,
        };

        Self {
            code: err.status_code(),
            error: err.error_code().to_string(),
            message: err.to_string(),
            details,
        }
    }

    fn bad_request(message: &str) -> Self {
        Self {
            code: 400,
            error: "bad_request".to_string(),
            message: message.to_string(),
            details: None,
        }
    }

    fn unauthorized(message: &str) -> Self {
        Self {
            code: 401,
            error: "unauthorized".to_string(),
            message: message.to_string(),
            details: None,
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// 从请求中提取客户端 IP
fn extract_client_ip(headers: &axum::http::HeaderMap) -> String {
    // 尝试从 X-Forwarded-For 获取
    if let Some(forwarded) = headers.get("x-forwarded-for") {
        if let Ok(value) = forwarded.to_str() {
            if let Some(ip) = value.split(',').next() {
                return ip.trim().to_string();
            }
        }
    }

    // 尝试从 X-Real-IP 获取
    if let Some(real_ip) = headers.get("x-real-ip") {
        if let Ok(value) = real_ip.to_str() {
            return value.trim().to_string();
        }
    }

    // 默认返回 unknown
    "unknown".to_string()
}

/// 生成待验证令牌（用于两步验证）
fn generate_pending_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
    format!("pending_{}", hex::encode(bytes))
}

// ============================================================================
// Login API (Task 7.1)
// ============================================================================

/// 登录 API
///
/// POST /api/v1/web-auth/login
///
/// 支持以下登录方式：
/// 1. 仅密码认证：提供 password
/// 2. 仅 TOTP 认证：提供 totp_code
/// 3. 密码 + TOTP 认证：
///    - 第一步：提供 password，返回 pending_token
///    - 第二步：提供 pending_token + totp_code
/// 4. 恢复码登录：提供 recovery_code（可选 pending_token）
pub async fn login(
    State(state): State<Arc<WebAuthState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let client_ip = extract_client_ip(&headers);
    debug!("Login attempt from IP: {}", client_ip);

    // 检查是否被速率限制
    if let Some(remaining) = state.rate_limiter.is_locked(&client_ip) {
        warn!(
            "Login blocked due to rate limiting: IP={}, remaining={}s",
            client_ip, remaining
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(LoginResponse::rate_limited(remaining as u64)),
        );
    }

    // 获取当前认证模式
    let auth_mode = state.get_auth_mode().await;
    if auth_mode == AuthMode::None {
        return (
            StatusCode::BAD_REQUEST,
            Json(LoginResponse::error("认证未启用")),
        );
    }

    // 获取凭证
    let credentials = state.credentials.read().await.clone();

    // 处理恢复码登录
    if let Some(recovery_code) = &req.recovery_code {
        return handle_recovery_code_login(
            &state,
            &client_ip,
            &credentials,
            recovery_code,
            &req.pending_token,
        )
        .await;
    }

    // 根据认证模式处理登录
    match auth_mode {
        AuthMode::Password => {
            handle_password_only_login(&state, &client_ip, &credentials, &req).await
        }
        AuthMode::Totp => handle_totp_only_login(&state, &client_ip, &credentials, &req).await,
        AuthMode::PasswordTotp => {
            handle_password_totp_login(&state, &client_ip, &credentials, &req).await
        }
        AuthMode::None => {
            // 不应该到达这里
            (
                StatusCode::BAD_REQUEST,
                Json(LoginResponse::error("认证未启用")),
            )
        }
    }
}

/// 处理仅密码认证
async fn handle_password_only_login(
    state: &Arc<WebAuthState>,
    client_ip: &str,
    credentials: &AuthCredentials,
    req: &LoginRequest,
) -> (StatusCode, Json<LoginResponse>) {
    let password = match &req.password {
        Some(p) => p,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(LoginResponse::error("请提供密码")),
            );
        }
    };

    // 验证密码
    let password_hash = match &credentials.password_hash {
        Some(h) => h,
        None => {
            state.rate_limiter.record_failure(client_ip);
            return (
                StatusCode::UNAUTHORIZED,
                Json(LoginResponse::error("密码验证失败")),
            );
        }
    };

    match PasswordManager::verify_password(password, password_hash) {
        Ok(true) => {
            // 密码正确，生成令牌
            state.rate_limiter.reset(client_ip);
            match state.token_service.generate_token_pair() {
                Ok(pair) => {
                    info!("Login successful: IP={}", client_ip);
                    (
                        StatusCode::OK,
                        Json(LoginResponse::success(
                            pair.access_token,
                            pair.refresh_token,
                            pair.access_expires_at,
                            pair.refresh_expires_at,
                        )),
                    )
                }
                Err(e) => {
                    warn!("Token generation failed: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(LoginResponse::error("令牌生成失败")),
                    )
                }
            }
        }
        Ok(false) | Err(_) => {
            state.rate_limiter.record_failure(client_ip);
            warn!("Password verification failed: IP={}", client_ip);
            (
                StatusCode::UNAUTHORIZED,
                Json(LoginResponse::error("密码验证失败")),
            )
        }
    }
}

/// 处理仅 TOTP 认证
async fn handle_totp_only_login(
    state: &Arc<WebAuthState>,
    client_ip: &str,
    credentials: &AuthCredentials,
    req: &LoginRequest,
) -> (StatusCode, Json<LoginResponse>) {
    let totp_code = match &req.totp_code {
        Some(c) => c,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(LoginResponse::error("请提供 TOTP 验证码")),
            );
        }
    };

    // 验证 TOTP
    let totp_secret = match &credentials.totp_secret {
        Some(s) => s,
        None => {
            state.rate_limiter.record_failure(client_ip);
            return (
                StatusCode::UNAUTHORIZED,
                Json(LoginResponse::error("TOTP 验证失败")),
            );
        }
    };

    match TOTPManager::verify_code(totp_secret, totp_code) {
        Ok(true) => {
            // TOTP 正确，生成令牌
            state.rate_limiter.reset(client_ip);
            match state.token_service.generate_token_pair() {
                Ok(pair) => {
                    info!("Login successful (TOTP): IP={}", client_ip);
                    (
                        StatusCode::OK,
                        Json(LoginResponse::success(
                            pair.access_token,
                            pair.refresh_token,
                            pair.access_expires_at,
                            pair.refresh_expires_at,
                        )),
                    )
                }
                Err(e) => {
                    warn!("Token generation failed: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(LoginResponse::error("令牌生成失败")),
                    )
                }
            }
        }
        Ok(false) | Err(_) => {
            state.rate_limiter.record_failure(client_ip);
            warn!("TOTP verification failed: IP={}", client_ip);
            (
                StatusCode::UNAUTHORIZED,
                Json(LoginResponse::error("TOTP 验证失败")),
            )
        }
    }
}

/// 处理密码 + TOTP 两步认证
async fn handle_password_totp_login(
    state: &Arc<WebAuthState>,
    client_ip: &str,
    credentials: &AuthCredentials,
    req: &LoginRequest,
) -> (StatusCode, Json<LoginResponse>) {
    // 如果有 pending_token，说明是第二步（TOTP 验证）
    if let Some(_pending_token) = &req.pending_token {
        // 验证 TOTP
        let totp_code = match &req.totp_code {
            Some(c) => c,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(LoginResponse::error("请提供 TOTP 验证码")),
                );
            }
        };

        let totp_secret = match &credentials.totp_secret {
            Some(s) => s,
            None => {
                state.rate_limiter.record_failure(client_ip);
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(LoginResponse::error("TOTP 验证失败")),
                );
            }
        };

        match TOTPManager::verify_code(totp_secret, totp_code) {
            Ok(true) => {
                // TOTP 正确，生成令牌
                state.rate_limiter.reset(client_ip);
                match state.token_service.generate_token_pair() {
                    Ok(pair) => {
                        info!("Login successful (Password+TOTP): IP={}", client_ip);
                        (
                            StatusCode::OK,
                            Json(LoginResponse::success(
                                pair.access_token,
                                pair.refresh_token,
                                pair.access_expires_at,
                                pair.refresh_expires_at,
                            )),
                        )
                    }
                    Err(e) => {
                        warn!("Token generation failed: {}", e);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(LoginResponse::error("令牌生成失败")),
                        )
                    }
                }
            }
            Ok(false) | Err(_) => {
                state.rate_limiter.record_failure(client_ip);
                warn!("TOTP verification failed (step 2): IP={}", client_ip);
                (
                    StatusCode::UNAUTHORIZED,
                    Json(LoginResponse::error("TOTP 验证失败")),
                )
            }
        }
    } else {
        // 第一步：验证密码
        let password = match &req.password {
            Some(p) => p,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(LoginResponse::error("请提供密码")),
                );
            }
        };

        let password_hash = match &credentials.password_hash {
            Some(h) => h,
            None => {
                state.rate_limiter.record_failure(client_ip);
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(LoginResponse::error("密码验证失败")),
                );
            }
        };

        match PasswordManager::verify_password(password, password_hash) {
            Ok(true) => {
                // 密码正确，返回 pending_token，要求 TOTP 验证
                let pending_token = generate_pending_token();
                debug!("Password verified, requiring TOTP: IP={}", client_ip);
                (
                    StatusCode::OK,
                    Json(LoginResponse::need_totp(pending_token)),
                )
            }
            Ok(false) | Err(_) => {
                state.rate_limiter.record_failure(client_ip);
                warn!("Password verification failed (step 1): IP={}", client_ip);
                (
                    StatusCode::UNAUTHORIZED,
                    Json(LoginResponse::error("密码验证失败")),
                )
            }
        }
    }
}

/// 处理恢复码登录
async fn handle_recovery_code_login(
    state: &Arc<WebAuthState>,
    client_ip: &str,
    credentials: &AuthCredentials,
    recovery_code: &str,
    pending_token: &Option<String>,
) -> (StatusCode, Json<LoginResponse>) {
    // 获取认证模式
    let auth_mode = state.get_auth_mode().await;

    // 如果是 PasswordTotp 模式，需要先验证密码（通过 pending_token）
    if auth_mode == AuthMode::PasswordTotp && pending_token.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(LoginResponse::error("请先验证密码")),
        );
    }

    // 验证恢复码
    match RecoveryCodeManager::verify_code(recovery_code, &credentials.recovery_codes) {
        Some(index) => {
            // 恢复码有效，标记为已使用
            if let Err(e) = state.auth_store.mark_recovery_code_used(index).await {
                warn!("Failed to mark recovery code as used: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(LoginResponse::error("恢复码使用失败")),
                );
            }

            // 生成令牌
            state.rate_limiter.reset(client_ip);
            match state.token_service.generate_token_pair() {
                Ok(pair) => {
                    info!("Login successful (recovery code): IP={}", client_ip);
                    (
                        StatusCode::OK,
                        Json(LoginResponse::success(
                            pair.access_token,
                            pair.refresh_token,
                            pair.access_expires_at,
                            pair.refresh_expires_at,
                        )),
                    )
                }
                Err(e) => {
                    warn!("Token generation failed: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(LoginResponse::error("令牌生成失败")),
                    )
                }
            }
        }
        None => {
            state.rate_limiter.record_failure(client_ip);
            warn!("Recovery code verification failed: IP={}", client_ip);
            (
                StatusCode::UNAUTHORIZED,
                Json(LoginResponse::error("恢复码无效或已使用")),
            )
        }
    }
}

// ============================================================================
// Token Refresh and Logout API (Task 7.2)
// ============================================================================

/// 令牌刷新 API
///
/// POST /api/v1/web-auth/refresh
///
/// 使用 Refresh Token 获取新的令牌对
pub async fn refresh(
    State(state): State<Arc<WebAuthState>>,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    match state.token_service.refresh_tokens(&req.refresh_token) {
        Ok(pair) => {
            debug!("Token refreshed successfully");
            (
                StatusCode::OK,
                Json(RefreshResponse {
                    access_token: pair.access_token,
                    refresh_token: pair.refresh_token,
                    access_expires_at: pair.access_expires_at,
                    refresh_expires_at: pair.refresh_expires_at,
                }),
            )
                .into_response()
        }
        Err(e) => {
            debug!("Token refresh failed: {}", e);
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::from_error(&e)),
            )
                .into_response()
        }
    }
}

/// 登出 API
///
/// POST /api/v1/web-auth/logout
///
/// 使当前会话的令牌失效
pub async fn logout(
    State(state): State<Arc<WebAuthState>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    // 尝试从请求中提取 Refresh Token
    // 可以从 Cookie 或请求体中获取
    let refresh_token = extract_refresh_token(&headers);

    if let Some(token) = refresh_token {
        if let Err(e) = state.token_service.revoke_token(&token) {
            warn!("Failed to revoke token: {}", e);
        }
    }

    info!("User logged out");
    (StatusCode::OK, Json(SuccessResponse::ok()))
}

/// 从请求头中提取 Refresh Token
fn extract_refresh_token(headers: &axum::http::HeaderMap) -> Option<String> {
    // 尝试从 Cookie 提取
    if let Some(cookie_header) = headers.get(header::COOKIE) {
        if let Ok(cookie_str) = cookie_header.to_str() {
            for cookie in cookie_str.split(';') {
                let cookie = cookie.trim();
                if let Some(value) = cookie.strip_prefix("web_auth_refresh_token=") {
                    let token = value.trim();
                    if !token.is_empty() {
                        return Some(token.to_string());
                    }
                }
            }
        }
    }

    None
}

/// 认证状态查询 API
///
/// GET /api/v1/web-auth/status
///
/// 返回当前认证配置和状态
pub async fn status(
    State(state): State<Arc<WebAuthState>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let config = state.config.read().await;
    let auth_mode = config.mode;
    let enabled = config.enabled;
    drop(config);

    // 检查当前请求是否已认证
    let authenticated = if let Some(auth_header) = headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                state
                    .token_service
                    .verify_access_token(token.trim())
                    .is_ok()
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    (
        StatusCode::OK,
        Json(AuthStatusResponse {
            enabled,
            mode: auth_mode,
            authenticated,
        }),
    )
}

// ============================================================================
// Config API (Task 7.3)
// ============================================================================

/// 获取认证配置 API
///
/// GET /api/v1/web-auth/config
pub async fn get_config(State(state): State<Arc<WebAuthState>>) -> impl IntoResponse {
    let config = state.config.read().await;
    let credentials = state.credentials.read().await;

    (
        StatusCode::OK,
        Json(AuthConfigResponse {
            enabled: config.enabled,
            mode: config.mode,
            password_set: credentials.has_password(),
            totp_enabled: credentials.has_totp(),
            recovery_codes_count: credentials.available_recovery_codes_count(),
        }),
    )
}

/// 更新认证配置 API
///
/// PUT /api/v1/web-auth/config
///
/// 更新认证配置，配置变更时使所有会话失效
pub async fn update_config(
    State(state): State<Arc<WebAuthState>>,
    Json(req): Json<UpdateConfigRequest>,
) -> impl IntoResponse {
    let mut config = state.config.read().await.clone();
    let old_mode = config.mode;

    // 更新配置
    if let Some(enabled) = req.enabled {
        config.enabled = enabled;
        if !enabled {
            config.mode = AuthMode::None;
        }
    }

    if let Some(mode) = req.mode {
        config.mode = mode;
        config.enabled = mode != AuthMode::None;
    }

    // 验证配置有效性
    let credentials = state.credentials.read().await;

    // 如果启用密码认证但未设置密码
    if config.mode.requires_password() && !credentials.has_password() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("请先设置密码")),
        )
            .into_response();
    }

    // 如果启用 TOTP 认证但未设置 TOTP
    if config.mode.requires_totp() && !credentials.has_totp() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("请先启用双因素认证")),
        )
            .into_response();
    }

    drop(credentials);

    // 更新内存中的配置
    state.update_config(config.clone()).await;

    // 持久化配置到 app.toml
    if let Err(e) = persist_web_auth_config(&config).await {
        warn!("Failed to persist web auth config: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::bad_request(&format!("保存配置失败: {}", e))),
        )
            .into_response();
    }

    // 如果模式发生变化，记录日志
    if old_mode != config.mode {
        info!(
            "Auth config changed and persisted: {:?} -> {:?}",
            old_mode, config.mode
        );
    }

    (StatusCode::OK, Json(SuccessResponse::ok())).into_response()
}

/// 持久化 Web 认证配置到 app.toml
async fn persist_web_auth_config(config: &crate::web_auth::WebAuthConfig) -> Result<(), String> {
    use std::path::Path;
    use tokio::fs;

    let config_path = "config/app.toml";

    // 读取现有配置
    let content = fs::read_to_string(config_path)
        .await
        .map_err(|e| format!("读取配置文件失败: {}", e))?;

    // 解析为 toml::Value 以便修改
    let mut toml_value: toml::Value =
        toml::from_str(&content).map_err(|e| format!("解析配置文件失败: {}", e))?;

    // 更新 web_auth 部分
    if let Some(table) = toml_value.as_table_mut() {
        let web_auth_value = toml::Value::try_from(config)
            .map_err(|e| format!("序列化 web_auth 配置失败: {}", e))?;
        table.insert("web_auth".to_string(), web_auth_value);
    }

    // 写回文件
    let new_content =
        toml::to_string_pretty(&toml_value).map_err(|e| format!("序列化配置失败: {}", e))?;

    // 确保目录存在
    if let Some(parent) = Path::new(config_path).parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("创建目录失败: {}", e))?;
    }

    fs::write(config_path, new_content)
        .await
        .map_err(|e| format!("写入配置文件失败: {}", e))?;

    debug!("Web auth config persisted to {}", config_path);
    Ok(())
}

// ============================================================================
// Password API (Task 7.4)
// ============================================================================

/// 设置/修改密码 API
///
/// POST /api/v1/web-auth/password/set
pub async fn set_password(
    State(state): State<Arc<WebAuthState>>,
    Json(req): Json<SetPasswordRequest>,
) -> impl IntoResponse {
    // 验证密码强度
    if let Err(e) = PasswordManager::validate_strength(&req.password) {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::from_error(&e))).into_response();
    }

    let credentials = state.credentials.read().await;

    // 如果已设置密码，需要验证当前密码
    if credentials.has_password() {
        let current_password = match &req.current_password {
            Some(p) => p,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::bad_request("请提供当前密码")),
                )
                    .into_response();
            }
        };

        let password_hash = credentials.password_hash.as_ref().unwrap();
        match PasswordManager::verify_password(current_password, password_hash) {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse::unauthorized("当前密码验证失败")),
                )
                    .into_response();
            }
        }
    }

    drop(credentials);

    // 哈希新密码
    let password_hash = match PasswordManager::hash_password(&req.password) {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::from_error(&e)),
            )
                .into_response();
        }
    };

    // 保存密码哈希
    if let Err(e) = state.auth_store.set_password_hash(password_hash).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::from_error(&e)),
        )
            .into_response();
    }

    // 更新内存中的凭证
    {
        let mut credentials = state.credentials.write().await;
        let stored = state.auth_store.get_credentials().await;
        *credentials = stored;
    }

    info!("Password set/updated successfully");
    (
        StatusCode::OK,
        Json(SuccessResponse::with_message("密码设置成功")),
    )
        .into_response()
}

// ============================================================================
// TOTP API (Task 7.5)
// ============================================================================

/// TOTP 设置 API
///
/// POST /api/v1/web-auth/totp/setup
///
/// 生成新的 TOTP 密钥和 QR 码
pub async fn totp_setup(State(_state): State<Arc<WebAuthState>>) -> impl IntoResponse {
    let secret = TOTPManager::generate_secret();
    let issuer = "BaiduPCS-Rust";
    let account = "admin";

    let qr_code = match TOTPManager::generate_qr_code(&secret, issuer, account) {
        Ok(qr) => qr,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::from_error(&e)),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(TotpSetupResponse {
            secret,
            qr_code,
            issuer: issuer.to_string(),
            account: account.to_string(),
        }),
    )
        .into_response()
}

/// TOTP 验证并启用 API
///
/// POST /api/v1/web-auth/totp/verify
///
/// 验证 TOTP 码并启用双因素认证
pub async fn totp_verify(
    State(state): State<Arc<WebAuthState>>,
    Json(req): Json<TotpVerifyRequest>,
) -> impl IntoResponse {
    let secret = match &req.secret {
        Some(s) => s.clone(),
        None => {
            // 如果没有提供 secret，使用已存储的
            let credentials = state.credentials.read().await;
            match &credentials.totp_secret {
                Some(s) => s.clone(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse::bad_request("请提供 TOTP 密钥")),
                    )
                        .into_response();
                }
            }
        }
    };

    // 验证 TOTP 码
    match TOTPManager::verify_code(&secret, &req.code) {
        Ok(true) => {
            // 保存 TOTP 密钥
            if let Err(e) = state.auth_store.set_totp_secret(secret).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::from_error(&e)),
                )
                    .into_response();
            }

            // 生成恢复码
            let codes = RecoveryCodeManager::generate_codes();
            let formatted_codes = RecoveryCodeManager::format_for_display(&codes);
            let stored_codes = RecoveryCodeManager::codes_to_storage(&codes);

            // 保存恢复码
            if let Err(e) = state.auth_store.set_recovery_codes(stored_codes).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::from_error(&e)),
                )
                    .into_response();
            }

            // 更新内存中的凭证
            {
                let mut credentials = state.credentials.write().await;
                let stored = state.auth_store.get_credentials().await;
                *credentials = stored;
            }

            info!("TOTP enabled successfully");
            (
                StatusCode::OK,
                Json(RegenerateCodesResponse {
                    codes: formatted_codes,
                }),
            )
                .into_response()
        }
        Ok(false) | Err(_) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::unauthorized("TOTP 验证失败")),
        )
            .into_response(),
    }
}

/// TOTP 禁用 API
///
/// POST /api/v1/web-auth/totp/disable
///
/// 禁用双因素认证（需要 TOTP 码或恢复码验证）
pub async fn totp_disable(
    State(state): State<Arc<WebAuthState>>,
    Json(req): Json<TotpDisableRequest>,
) -> impl IntoResponse {
    let credentials = state.credentials.read().await;

    // 验证 TOTP 码或恢复码
    let verified = if let Some(code) = &req.code {
        // 使用 TOTP 码验证
        if let Some(secret) = &credentials.totp_secret {
            TOTPManager::verify_code(secret, code).unwrap_or(false)
        } else {
            false
        }
    } else if let Some(recovery_code) = &req.recovery_code {
        // 使用恢复码验证
        RecoveryCodeManager::verify_code(recovery_code, &credentials.recovery_codes).is_some()
    } else {
        false
    };

    drop(credentials);

    if !verified {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::unauthorized("验证失败")),
        )
            .into_response();
    }

    // 清除 TOTP 配置
    if let Err(e) = state.auth_store.clear_totp().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::from_error(&e)),
        )
            .into_response();
    }

    // 更新内存中的凭证
    {
        let mut credentials = state.credentials.write().await;
        let stored = state.auth_store.get_credentials().await;
        *credentials = stored;
    }

    info!("TOTP disabled successfully");
    (
        StatusCode::OK,
        Json(SuccessResponse::with_message("双因素认证已禁用")),
    )
        .into_response()
}

// ============================================================================
// Recovery Codes API (Task 7.6)
// ============================================================================

/// 重新生成恢复码 API
///
/// POST /api/v1/web-auth/recovery-codes/regenerate
///
/// 重新生成恢复码（需要 TOTP 验证）
pub async fn regenerate_recovery_codes(
    State(state): State<Arc<WebAuthState>>,
    Json(req): Json<RegenerateCodesRequest>,
) -> impl IntoResponse {
    let credentials = state.credentials.read().await;

    // 验证 TOTP 码
    let totp_secret = match &credentials.totp_secret {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request("未启用双因素认证")),
            )
                .into_response();
        }
    };

    drop(credentials);

    match TOTPManager::verify_code(&totp_secret, &req.totp_code) {
        Ok(true) => {
            // 生成新的恢复码
            let codes = RecoveryCodeManager::generate_codes();
            let formatted_codes = RecoveryCodeManager::format_for_display(&codes);
            let stored_codes = RecoveryCodeManager::codes_to_storage(&codes);

            // 保存恢复码（会覆盖旧的）
            if let Err(e) = state.auth_store.set_recovery_codes(stored_codes).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::from_error(&e)),
                )
                    .into_response();
            }

            // 更新内存中的凭证
            {
                let mut credentials = state.credentials.write().await;
                let stored = state.auth_store.get_credentials().await;
                *credentials = stored;
            }

            info!("Recovery codes regenerated successfully");
            (
                StatusCode::OK,
                Json(RegenerateCodesResponse {
                    codes: formatted_codes,
                }),
            )
                .into_response()
        }
        Ok(false) | Err(_) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::unauthorized("TOTP 验证失败")),
        )
            .into_response(),
    }
}
