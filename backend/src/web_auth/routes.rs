//! Web 认证 API 路由（挂在 `/api/v1/web-auth` 下）
//!
//! 分两组：
//! - 公开：状态查询、登录、刷新、登出。未登录时必须能访问，否则无法登录。
//! - 受保护：修改认证配置、设置密码、TOTP、恢复码。认证开启后必须携带有效令牌，
//!   否则任何人都能直接 `PUT /config` 把认证关掉，或者换掉密码 / TOTP。

use std::sync::Arc;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};

use super::handlers::{
    get_config, login, logout, refresh, regenerate_recovery_codes, set_password, status,
    totp_disable, totp_setup, totp_verify, update_config,
};
use super::middleware::web_auth_middleware;
use super::state::WebAuthState;

/// 构建 Web 认证 API 路由
pub fn web_auth_routes(state: Arc<WebAuthState>) -> Router {
    let public = Router::new()
        .route("/login", post(login))
        .route("/refresh", post(refresh))
        .route("/logout", post(logout))
        .route("/status", get(status));

    let protected = Router::new()
        .route("/config", get(get_config).put(update_config))
        .route("/password/set", post(set_password))
        .route("/totp/setup", post(totp_setup))
        .route("/totp/verify", post(totp_verify))
        .route("/totp/disable", post(totp_disable))
        .route("/recovery-codes/regenerate", post(regenerate_recovery_codes))
        .route_layer(middleware::from_fn_with_state(state.clone(), web_auth_middleware));

    public.merge(protected).with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_auth::{create_auth_store_with_path, AuthCredentials, AuthMode, WebAuthConfig};
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
    };
    use tower::Service;

    const WEB_AUTH_EXPIRED: u16 = 419;

    fn state(mode: AuthMode) -> Arc<WebAuthState> {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(create_auth_store_with_path(dir.path().join("auth.json")));
        let config = WebAuthConfig { enabled: mode != AuthMode::None, mode };
        Arc::new(WebAuthState::new(config, AuthCredentials::default(), None, store))
    }

    /// 与 main.rs 相同的挂载结构：业务接口整体挂中间件，web-auth 走独立路由
    fn app(state: Arc<WebAuthState>) -> Router {
        let api = Router::new()
            .route("/accounts/list", get(|| async { "ok" }))
            .route("/shares", get(|| async { "ok" }))
            .route("/ws", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(state.clone(), web_auth_middleware));
        Router::new()
            .nest("/api/v1", api)
            .nest("/api/v1/web-auth", web_auth_routes(state))
    }

    async fn call(mut app: Router, method: &str, uri: &str, headers: &[(header::HeaderName, &str)]) -> u16 {
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(k, *v);
        }
        let req = req
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap();
        // Router 始终就绪，无需先 poll_ready
        app.call(req).await.unwrap().status().as_u16()
    }

    #[tokio::test]
    async fn test_business_routes_require_token_when_enabled() {
        let app = app(state(AuthMode::Password));
        // 以前不在白名单里的路径会被当作静态资源放行
        for uri in ["/api/v1/accounts/list", "/api/v1/shares", "/api/v1/ws"] {
            assert_eq!(call(app.clone(), "GET", uri, &[]).await, WEB_AUTH_EXPIRED, "{uri}");
        }
    }

    #[tokio::test]
    async fn test_valid_token_passes() {
        let st = state(AuthMode::Password);
        let token = st.token_service.generate_token_pair().unwrap().access_token;
        let app = app(st);
        let bearer = format!("Bearer {token}");

        assert_eq!(
            call(app.clone(), "GET", "/api/v1/accounts/list", &[(header::AUTHORIZATION, &bearer)]).await,
            StatusCode::OK.as_u16()
        );
        assert_eq!(
            call(app.clone(), "GET", "/api/v1/web-auth/config", &[(header::AUTHORIZATION, &bearer)]).await,
            StatusCode::OK.as_u16()
        );
        // WebSocket 握手用查询参数携带令牌
        let ws_uri = format!("/api/v1/ws?access_token={token}");
        assert_eq!(
            call(app, "GET", &ws_uri, &[(header::UPGRADE, "websocket")]).await,
            StatusCode::OK.as_u16()
        );
    }

    #[tokio::test]
    async fn test_web_auth_management_routes_require_token() {
        let app = app(state(AuthMode::Password));
        let cases = [
            ("GET", "/api/v1/web-auth/config"),
            ("PUT", "/api/v1/web-auth/config"),
            ("POST", "/api/v1/web-auth/password/set"),
            ("POST", "/api/v1/web-auth/totp/setup"),
            ("POST", "/api/v1/web-auth/totp/verify"),
            ("POST", "/api/v1/web-auth/totp/disable"),
            ("POST", "/api/v1/web-auth/recovery-codes/regenerate"),
        ];
        for (method, uri) in cases {
            assert_eq!(call(app.clone(), method, uri, &[]).await, WEB_AUTH_EXPIRED, "{method} {uri}");
        }
    }

    #[tokio::test]
    async fn test_web_auth_public_routes_stay_open() {
        let app = app(state(AuthMode::Password));
        assert_eq!(call(app.clone(), "GET", "/api/v1/web-auth/status", &[]).await, StatusCode::OK.as_u16());
        // 登录接口可匿名访问（空请求体会被业务校验拒绝，但不能是 419）
        assert_ne!(call(app, "POST", "/api/v1/web-auth/login", &[]).await, WEB_AUTH_EXPIRED);
    }

    #[tokio::test]
    async fn test_everything_open_when_auth_disabled() {
        let app = app(state(AuthMode::None));
        assert_eq!(call(app.clone(), "GET", "/api/v1/accounts/list", &[]).await, StatusCode::OK.as_u16());
        // 未开启认证时要能在设置页先设密码、再开启认证
        assert_eq!(call(app, "GET", "/api/v1/web-auth/config", &[]).await, StatusCode::OK.as_u16());
    }
}
