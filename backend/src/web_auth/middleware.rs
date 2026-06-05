//! Web 认证中间件模块
//!
//! 实现 Axum 中间件，用于保护需要认证的 API 端点。
//!
//! ## 功能
//! - 从 Header 或 Cookie 提取 Access Token
//! - 验证令牌有效性
//! - 认证绕过逻辑（auth 端点、静态资源、健康检查）
//! - 将认证状态注入请求上下文
//!
//! ## 重要说明
//! 此中间件仅作用于 Web 访问认证，不影响：
//! - 百度二维码登录轮询
//! - 下载/上传进度轮询
//! - WebSocket 实时推送
//! - 所有其他现有功能

use crate::web_auth::state::WebAuthState;
use crate::web_auth::types::{AuthMode, TokenClaims};
use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::debug;

/// Authorization Header 前缀
const BEARER_PREFIX: &str = "Bearer ";

/// Cookie 名称
const ACCESS_TOKEN_COOKIE: &str = "web_auth_access_token";

/// Web 认证专用 HTTP 状态码
/// 使用 419 (Page Expired / Session Expired) 来区分 Web 认证失败和百度账号认证失败
/// 百度账号认证失败使用标准 401，Web 认证失败使用 419
const WEB_AUTH_EXPIRED_STATUS: u16 = 419;

/// 认证错误响应
#[derive(Debug, Serialize)]
pub struct AuthErrorResponse {
    pub code: u16,
    pub error: String,
    pub message: String,
}

impl AuthErrorResponse {
    pub fn web_auth_expired(message: &str) -> Self {
        Self {
            code: WEB_AUTH_EXPIRED_STATUS,
            error: "web_auth_expired".to_string(),
            message: message.to_string(),
        }
    }
}

/// 需要绕过认证的路径前缀
/// 注意：由于中间件应用在嵌套路由上，路径是相对于 /api/v1 的
const AUTH_BYPASS_PREFIXES: &[&str] = &[
    "/web-auth/", // Web 认证相关端点（相对路径）
    "/auth/",     // 百度认证相关端点（二维码登录等）
    "/ws",        // WebSocket 端点
    // 完整路径（用于非嵌套路由）
    "/api/v1/web-auth/",
    "/api/v1/auth/",
    "/api/v1/ws",
    "/health",
];

/// 需要绕过认证的精确路径
const AUTH_BYPASS_EXACT: &[&str] = &[
    // 相对路径（用于嵌套路由）
    "/web-auth/status",
    "/web-auth/login",
    "/web-auth/refresh",
    // 完整路径（用于非嵌套路由）
    "/api/v1/web-auth/status",
    "/api/v1/web-auth/login",
    "/api/v1/web-auth/refresh",
];

/// 检查路径是否需要绕过认证
fn should_bypass_auth(path: &str) -> bool {
    // 检查精确匹配
    if AUTH_BYPASS_EXACT.contains(&path) {
        return true;
    }

    // 检查前缀匹配
    for prefix in AUTH_BYPASS_PREFIXES {
        if path.starts_with(prefix) {
            return true;
        }
    }

    // 静态资源（非 API 路径，且不是相对 API 路径）
    // 相对路径以 / 开头但不以 /api/ 开头
    if !path.starts_with("/api/") {
        // 检查是否是嵌套路由的相对路径（以 / 开头的 API 端点）
        // 这些路径应该需要认证
        let api_relative_paths = [
            "/files",
            "/downloads",
            "/uploads",
            "/transfers",
            "/fs/",
            "/config",
            "/autobackup/",
            "/encryption/",
            "/system/",
        ];

        for api_path in api_relative_paths {
            if path.starts_with(api_path) {
                return false;
            }
        }

        // 其他非 API 路径（静态资源）
        return true;
    }

    false
}

/// 从请求中提取 Access Token
///
/// 按优先级尝试：
/// 1. Authorization Header (Bearer token)
/// 2. Cookie (web_auth_access_token)
fn extract_access_token(request: &Request<Body>) -> Option<String> {
    // 1. 尝试从 Authorization Header 提取
    if let Some(auth_header) = request.headers().get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with(BEARER_PREFIX) {
                let token = auth_str[BEARER_PREFIX.len()..].trim();
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }

    // 2. 尝试从 Cookie 提取
    if let Some(cookie_header) = request.headers().get(header::COOKIE) {
        if let Ok(cookie_str) = cookie_header.to_str() {
            for cookie in cookie_str.split(';') {
                let cookie = cookie.trim();
                if let Some(value) = cookie.strip_prefix(&format!("{}=", ACCESS_TOKEN_COOKIE)) {
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

/// Web 认证中间件
///
/// 验证请求的认证状态，根据配置的认证模式决定是否允许访问。
///
/// ## 行为
/// - 当认证模式为 `None` 时，所有请求直接通过
/// - 当认证启用时，验证 Access Token
/// - 认证端点、静态资源等路径绕过认证检查
pub async fn web_auth_middleware(
    State(state): State<Arc<WebAuthState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let method = request.method().clone();

    // 检查是否需要绕过认证
    if should_bypass_auth(path) {
        debug!("Auth bypass for path: {} {}", method, path);
        return next.run(request).await;
    }

    // 获取当前认证模式
    let auth_mode = state.get_auth_mode().await;

    // 如果认证未启用，直接通过
    if auth_mode == AuthMode::None {
        debug!("Auth disabled, allowing request: {} {}", method, path);
        return next.run(request).await;
    }

    // 提取 Access Token
    let token = match extract_access_token(&request) {
        Some(t) => t,
        None => {
            debug!("No access token found for: {} {}", method, path);
            return (
                StatusCode::from_u16(WEB_AUTH_EXPIRED_STATUS).unwrap_or(StatusCode::UNAUTHORIZED),
                Json(AuthErrorResponse::web_auth_expired("未提供认证令牌")),
            )
                .into_response();
        }
    };

    // 验证 Access Token
    match state.token_service.verify_access_token(&token) {
        Ok(claims) => {
            debug!(
                "Token verified for: {} {}, jti: {}",
                method, path, claims.jti
            );
            // 将认证信息注入请求扩展
            let mut request = request;
            request
                .extensions_mut()
                .insert(AuthenticatedUser { claims });
            next.run(request).await
        }
        Err(e) => {
            debug!("Token verification failed for: {} {}: {}", method, path, e);
            (
                StatusCode::from_u16(WEB_AUTH_EXPIRED_STATUS).unwrap_or(StatusCode::UNAUTHORIZED),
                Json(AuthErrorResponse::web_auth_expired("令牌无效或已过期")),
            )
                .into_response()
        }
    }
}

/// 已认证用户信息
///
/// 存储在请求扩展中，供下游处理器使用。
/// 可以作为 Axum 提取器直接在处理器中使用。
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// JWT Claims
    pub claims: TokenClaims,
}

impl AuthenticatedUser {
    /// 获取 JWT ID
    pub fn jti(&self) -> &str {
        &self.claims.jti
    }

    /// 获取令牌过期时间
    pub fn expires_at(&self) -> i64 {
        self.claims.exp
    }

    /// 获取令牌签发时间
    pub fn issued_at(&self) -> i64 {
        self.claims.iat
    }

    /// 获取主题（固定为 "web_auth"）
    pub fn subject(&self) -> &str {
        &self.claims.sub
    }
}

/// 可选的已认证用户
///
/// 用于需要检查认证状态但不强制要求认证的处理器。
#[derive(Debug, Clone)]
pub struct OptionalAuthenticatedUser(pub Option<AuthenticatedUser>);

impl OptionalAuthenticatedUser {
    /// 检查是否已认证
    pub fn is_authenticated(&self) -> bool {
        self.0.is_some()
    }

    /// 获取已认证用户（如果存在）
    pub fn user(&self) -> Option<&AuthenticatedUser> {
        self.0.as_ref()
    }
}

// 实现 FromRequestParts 以便作为提取器使用
use axum::extract::FromRequestParts;
use axum::http::request::Parts;

#[axum::async_trait]
impl<S> FromRequestParts<S> for AuthenticatedUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<AuthErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthenticatedUser>()
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::from_u16(WEB_AUTH_EXPIRED_STATUS)
                        .unwrap_or(StatusCode::UNAUTHORIZED),
                    Json(AuthErrorResponse::web_auth_expired("未认证")),
                )
            })
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for OptionalAuthenticatedUser
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(OptionalAuthenticatedUser(
            parts.extensions.get::<AuthenticatedUser>().cloned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_bypass_auth_exact_paths() {
        // 相对路径（嵌套路由）
        assert!(should_bypass_auth("/web-auth/status"));
        assert!(should_bypass_auth("/web-auth/login"));
        assert!(should_bypass_auth("/web-auth/refresh"));
        // 完整路径
        assert!(should_bypass_auth("/api/v1/web-auth/status"));
        assert!(should_bypass_auth("/api/v1/web-auth/login"));
        assert!(should_bypass_auth("/api/v1/web-auth/refresh"));
    }

    #[test]
    fn test_should_bypass_auth_prefixes() {
        // 相对路径（嵌套路由）
        assert!(should_bypass_auth("/web-auth/config"));
        assert!(should_bypass_auth("/auth/qrcode/generate"));
        assert!(should_bypass_auth("/auth/qrcode/status"));
        assert!(should_bypass_auth("/ws"));
        // 完整路径
        assert!(should_bypass_auth("/api/v1/web-auth/config"));
        assert!(should_bypass_auth("/api/v1/auth/qrcode/generate"));
        assert!(should_bypass_auth("/health"));
        assert!(should_bypass_auth("/api/v1/ws"));
    }

    #[test]
    fn test_should_bypass_auth_static_resources() {
        assert!(should_bypass_auth("/"));
        assert!(should_bypass_auth("/index.html"));
        assert!(should_bypass_auth("/assets/main.js"));
        assert!(should_bypass_auth("/favicon.ico"));
    }

    #[test]
    fn test_should_not_bypass_auth_protected_paths() {
        // 相对路径（嵌套路由）
        assert!(!should_bypass_auth("/files"));
        assert!(!should_bypass_auth("/downloads"));
        assert!(!should_bypass_auth("/uploads"));
        assert!(!should_bypass_auth("/config"));
        assert!(!should_bypass_auth("/autobackup/configs"));
        // 完整路径
        assert!(!should_bypass_auth("/api/v1/files"));
        assert!(!should_bypass_auth("/api/v1/downloads"));
        assert!(!should_bypass_auth("/api/v1/uploads"));
        assert!(!should_bypass_auth("/api/v1/config"));
        assert!(!should_bypass_auth("/api/v1/autobackup/configs"));
    }

    #[test]
    fn test_extract_access_token_from_header() {
        use axum::http::Request;

        let request = Request::builder()
            .uri("/api/v1/files")
            .header(header::AUTHORIZATION, "Bearer test_token_123")
            .body(Body::empty())
            .unwrap();

        let token = extract_access_token(&request);
        assert_eq!(token, Some("test_token_123".to_string()));
    }

    #[test]
    fn test_extract_access_token_from_cookie() {
        use axum::http::Request;

        let request = Request::builder()
            .uri("/api/v1/files")
            .header(
                header::COOKIE,
                "web_auth_access_token=cookie_token_456; other=value",
            )
            .body(Body::empty())
            .unwrap();

        let token = extract_access_token(&request);
        assert_eq!(token, Some("cookie_token_456".to_string()));
    }

    #[test]
    fn test_extract_access_token_header_priority() {
        use axum::http::Request;

        // Header should take priority over Cookie
        let request = Request::builder()
            .uri("/api/v1/files")
            .header(header::AUTHORIZATION, "Bearer header_token")
            .header(header::COOKIE, "web_auth_access_token=cookie_token")
            .body(Body::empty())
            .unwrap();

        let token = extract_access_token(&request);
        assert_eq!(token, Some("header_token".to_string()));
    }

    #[test]
    fn test_extract_access_token_none() {
        use axum::http::Request;

        let request = Request::builder()
            .uri("/api/v1/files")
            .body(Body::empty())
            .unwrap();

        let token = extract_access_token(&request);
        assert!(token.is_none());
    }

    #[test]
    fn test_extract_access_token_invalid_bearer() {
        use axum::http::Request;

        // Missing "Bearer " prefix
        let request = Request::builder()
            .uri("/api/v1/files")
            .header(header::AUTHORIZATION, "token_without_bearer")
            .body(Body::empty())
            .unwrap();

        let token = extract_access_token(&request);
        assert!(token.is_none());
    }

    #[test]
    fn test_extract_access_token_empty_bearer() {
        use axum::http::Request;

        let request = Request::builder()
            .uri("/api/v1/files")
            .header(header::AUTHORIZATION, "Bearer ")
            .body(Body::empty())
            .unwrap();

        let token = extract_access_token(&request);
        assert!(token.is_none());
    }
}
