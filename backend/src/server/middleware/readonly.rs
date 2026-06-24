// 全局只读模式中间件（冲突收口）
//
// 行为：
// - `AppState.readonly_mode == true` 且请求方法为 POST/PUT/DELETE/PATCH → 立即返回 503 `readonly_mode`
// - GET / HEAD / OPTIONS 一律放行
//
// 触发场景：迁移失败、关键持久化异常等 — 由 `persistence/migration.rs` / 启动流程置 `true`，
// 提示前端进入「只读历史模式」、阻止用户继续写入造成数据扩散损坏。

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::atomic::Ordering;

use crate::server::AppState;

/// 拦截写操作并在只读模式下返回 503。
///
/// 用法（`main.rs`）：
/// ```ignore
/// let app = Router::new()
///     .merge(api_routes)
///     .layer(middleware::from_fn_with_state(state.clone(), readonly_middleware));
/// ```
pub async fn readonly_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method();
    let is_write = matches!(
        *method,
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    );

    if is_write && state.readonly_mode.load(Ordering::Relaxed) {
        tracing::warn!("readonly_mode 拦截写请求: {} {}", method, req.uri().path());
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "code": 503,
                "message": "readonly_mode",
            })),
        )
            .into_response();
    }

    next.run(req).await
}
