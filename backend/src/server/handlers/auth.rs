// 认证API处理器

use crate::auth::{QRCode, QRCodeStatus};
use crate::server::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

/// 统一API响应格式
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    /// 状态码 (0: 成功, 其他: 错误码)
    pub code: i32,
    /// 消息
    pub message: String,
    /// 数据
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            code: 0,
            message: "Success".to_string(),
            data: Some(data),
        }
    }

    pub fn error(code: i32, message: String) -> Self {
        Self {
            code,
            message,
            data: None,
        }
    }
}

/// 生成登录二维码
///
/// POST /api/v1/auth/qrcode/generate
pub async fn generate_qrcode(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<QRCode>>, StatusCode> {
    info!("API: 生成登录二维码");

    match state.qrcode_auth.generate_qrcode().await {
        Ok(qrcode) => {
            info!("二维码生成成功: sign={}", qrcode.sign);
            Ok(Json(ApiResponse::success(qrcode)))
        }
        Err(e) => {
            error!("二维码生成失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("Failed to generate QR code: {}", e),
            )))
        }
    }
}

/// 查询参数：sign
#[derive(Debug, Deserialize)]
pub struct QRCodeStatusQuery {
    pub sign: String,
}

/// 查询扫码状态
///
/// GET /api/v1/auth/qrcode/status?sign=xxx
pub async fn qrcode_status(
    State(state): State<AppState>,
    Query(params): Query<QRCodeStatusQuery>,
) -> Result<Json<ApiResponse<QRCodeStatus>>, StatusCode> {
    info!("API: 查询扫码状态: sign={}", params.sign);

    match state.qrcode_auth.poll_status(&params.sign).await {
        Ok(status) => {
            // 如果登录成功，保存会话
            if let QRCodeStatus::Success { ref user, .. } = status {
                info!(
                    "检测到登录成功，准备保存会话: UID={}, 用户名={}",
                    user.uid, user.username
                );
                let mut session = state.session_manager.lock().await;
                match session.save_session(user).await {
                    Ok(_) => {
                        info!(
                            "✅ 会话保存成功: UID={}, BDUSS长度={}",
                            user.uid,
                            user.bduss.len()
                        );
                    }
                    Err(e) => {
                        error!("❌ 保存会话失败: {}", e);
                    }
                }
            }

            Ok(Json(ApiResponse::success(status)))
        }
        Err(e) => {
            error!("查询扫码状态失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("Failed to poll status: {}", e),
            )))
        }
    }
}

/// 获取当前用户信息
///
/// GET /api/v1/auth/user
pub async fn get_current_user(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("🔍 API: 获取当前用户信息");

    let mut session = state.session_manager.lock().await;

    match session.get_session().await {
        Ok(Some(user)) => {
            info!("✅ 找到会话: UID={}, 用户名={}", user.uid, user.username);

            // 验证 BDUSS 是否仍然有效
            match state.qrcode_auth.verify_bduss(&user.bduss).await {
                Ok(true) => {
                    // BDUSS 有效
                    info!("BDUSS 验证通过");
                    Ok(Json(ApiResponse::success(user)))
                }
                Ok(false) => {
                    // BDUSS 已失效，清除会话
                    warn!("BDUSS 已失效，清除会话");
                    let _ = session.clear_session().await;
                    Ok(Json(ApiResponse::error(
                        401,
                        "Session expired, please login again".to_string(),
                    )))
                }
                Err(e) => {
                    // 验证失败（可能是网络问题），暂时允许通过
                    warn!("BDUSS 验证失败: {}，暂时允许通过", e);
                    Ok(Json(ApiResponse::success(user)))
                }
            }
        }
        Ok(None) => {
            warn!("❌ 未找到会话，用户未登录");
            Ok(Json(ApiResponse::error(401, "Not logged in".to_string())))
        }
        Err(e) => {
            error!("获取会话失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("Failed to get session: {}", e),
            )))
        }
    }
}

/// 登出
///
/// POST /api/v1/auth/logout
pub async fn logout(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 用户登出");

    let mut session = state.session_manager.lock().await;

    match session.clear_session().await {
        Ok(_) => {
            info!("登出成功");
            Ok(Json(ApiResponse::<()>::success(())))
        }
        Err(e) => {
            error!("登出失败: {}", e);
            Ok(Json(ApiResponse::<()>::error(
                500,
                format!("Failed to logout: {}", e),
            )))
        }
    }
}
