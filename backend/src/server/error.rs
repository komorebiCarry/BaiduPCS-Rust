// 统一错误处理

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::fmt;
use std::io::ErrorKind;

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
                let msg = e.to_string();
                if is_config_input_error(&e) {
                    tracing::warn!("Config/validation error (mapped as 400): {}", msg);
                    (StatusCode::BAD_REQUEST, 400, msg, None)
                } else {
                    tracing::error!("Internal error: {:?}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        500,
                        "Internal server error".to_string(),
                        Some(msg),
                    )
                }
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
        };

        let body = Json(ErrorResponse {
            code,
            message,
            details,
        });

        (status, body).into_response()
    }
}

fn is_config_input_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    let lower = message.to_lowercase();
    let is_config_related = message.contains("配置")
        || lower.contains("config")
        || message.contains("下载目录")
        || lower.contains("download_dir");

    if !is_config_related {
        return false;
    }

    if message.contains("保存配置失败")
        || message.contains("下载目录不存在")
        || message.contains("路径不存在")
        || message.contains("路径不可写")
        || message.contains("没有写入权限")
        || message.contains("保存下载目录")
        || message.contains("Failed to create config directory")
        || message.contains("Failed to write config file")
        || lower.contains("permission denied")
        || lower.contains("read-only")
        || lower.contains("read only")
        || lower.contains("readonly")
        || lower.contains("invalid input")
        || lower.contains("failed to create config directory")
        || lower.contains("failed to write config file")
        || lower.contains("no such file")
        || lower.contains("not found")
    {
        return true;
    }

    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                matches!(
                    io_err.kind(),
                    ErrorKind::PermissionDenied | ErrorKind::NotFound | ErrorKind::InvalidInput
                )
            })
    })
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
