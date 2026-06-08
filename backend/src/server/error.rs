// 统一错误处理

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::fmt;

/// API 错误类型
#[derive(Debug)]
pub enum ApiError {
    /// 内部服务器错误
    Internal(anyhow::Error),
    /// 未授权
    Unauthorized(String),
    /// Token 过期或无效（需要重新登录）
    TokenExpired(String),
    /// 未找到
    NotFound(String),
    /// 请求参数错误
    BadRequest(String),
    /// 冲突
    Conflict(String),
    // ───────── 多账号错误码 ─────────
    /// 账号不在（账号已删除 / 客户端不可用） — 400
    ///
    /// 触发场景：(1) `?uid=` 或 body `uid` 指向已删除账号；(2) `active_uid = None` 且未显式提供 uid。
    AccountNotAvailable,
    /// 旧历史只读 — 400
    ///
    /// 仅用于 CloudDl 单资源控制接口（`cancel` / `delete :task_id`）显式操作 `owner_uid = NULL` 历史行的场景。
    /// 其他模块的 legacy 行启动时已回填 active_uid，运行态不应再返回此错误。
    LegacyHistoryReadonly,
    /// 全局只读模式 — 503
    ///
    /// 触发：迁移失败 / 关键持久化异常 →`AppState.readonly_mode = true`，写接口（POST/PUT/DELETE）全部 503。
    ReadonlyMode,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Internal(e) => write!(f, "Internal error: {}", e),
            ApiError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ApiError::TokenExpired(msg) => write!(f, "Token expired: {}", msg),
            ApiError::NotFound(msg) => write!(f, "Not found: {}", msg),
            ApiError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            ApiError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            ApiError::AccountNotAvailable => write!(f, "account_not_available"),
            ApiError::LegacyHistoryReadonly => write!(f, "legacy_history_readonly"),
            ApiError::ReadonlyMode => write!(f, "readonly_mode"),
        }
    }
}

impl std::error::Error for ApiError {}

/// 错误响应体
#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<String>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match self {
            ApiError::Internal(e) => {
                tracing::error!("Internal error: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    500,
                    "Internal server error".to_string(),
                    Some(e.to_string()),
                )
            }
            ApiError::Unauthorized(msg) => {
                tracing::warn!("Unauthorized: {}", msg);
                (StatusCode::UNAUTHORIZED, 401, msg, None)
            }
            ApiError::TokenExpired(msg) => {
                tracing::warn!("Token expired: {}", msg);
                (StatusCode::UNAUTHORIZED, 401, msg, None)
            }
            ApiError::NotFound(msg) => {
                tracing::debug!("Not found: {}", msg);
                (StatusCode::NOT_FOUND, 404, msg, None)
            }
            ApiError::BadRequest(msg) => {
                tracing::debug!("Bad request: {}", msg);
                (StatusCode::BAD_REQUEST, 400, msg, None)
            }
            ApiError::Conflict(msg) => {
                tracing::debug!("Conflict: {}", msg);
                (StatusCode::CONFLICT, 409, msg, None)
            }
            ApiError::AccountNotAvailable => {
                tracing::debug!("account_not_available");
                (
                    StatusCode::BAD_REQUEST,
                    400,
                    "account_not_available".to_string(),
                    None,
                )
            }
            ApiError::LegacyHistoryReadonly => {
                tracing::debug!("legacy_history_readonly");
                (
                    StatusCode::BAD_REQUEST,
                    400,
                    "legacy_history_readonly".to_string(),
                    None,
                )
            }
            ApiError::ReadonlyMode => {
                tracing::warn!("readonly_mode: 写接口被全局只读模式拦截");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    503,
                    "readonly_mode".to_string(),
                    None,
                )
            }
        };

        let body = Json(ErrorResponse {
            code,
            message,
            details,
        });

        (status, body).into_response()
    }
}

/// 从 anyhow::Error 转换
impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::Internal(err)
    }
}

/// Result 类型别名
pub type ApiResult<T> = Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ApiError::BadRequest("Invalid parameter".to_string());
        assert_eq!(err.to_string(), "Bad request: Invalid parameter");

        let err = ApiError::NotFound("File not found".to_string());
        assert_eq!(err.to_string(), "Not found: File not found");

        let err = ApiError::Unauthorized("Not logged in".to_string());
        assert_eq!(err.to_string(), "Unauthorized: Not logged in");
    }

    #[test]
    fn test_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("Something went wrong");
        let api_err: ApiError = anyhow_err.into();
        assert!(matches!(api_err, ApiError::Internal(_)));
    }
}
