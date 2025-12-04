// 转存 API 处理器

use crate::server::AppState;
use crate::transfer::{TransferStatus, TransferTask};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

/// API 响应结构
#[derive(Debug, Serialize)]
pub struct TransferApiResponse<T> {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T: Serialize> TransferApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            code: 0,
            message: "success".to_string(),
            data: Some(data),
        }
    }

    pub fn error(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

/// 业务响应码
pub mod error_codes {
    /// 需要提取码
    pub const NEED_PASSWORD: i32 = 1001;
    /// 提取码错误
    pub const INVALID_PASSWORD: i32 = 1002;
    /// 分享已失效
    pub const SHARE_EXPIRED: i32 = 1003;
    /// 分享不存在
    pub const SHARE_NOT_FOUND: i32 = 1004;
    /// 转存管理器未初始化
    pub const MANAGER_NOT_READY: i32 = 1005;
    /// 任务不存在
    pub const TASK_NOT_FOUND: i32 = 1006;
}

// ============================================
// 请求/响应结构
// ============================================

/// 创建转存任务请求
#[derive(Debug, Deserialize)]
pub struct CreateTransferRequest {
    /// 分享链接
    pub share_url: String,
    /// 提取码（可选）
    pub password: Option<String>,
    /// 网盘保存路径
    pub save_path: String,
    /// 网盘保存目录 fs_id
    pub save_fs_id: u64,
    /// 是否自动下载（不传使用全局配置）
    pub auto_download: Option<bool>,
    /// 本地下载路径（auto_download=true 时可选）
    pub local_download_path: Option<String>,
}

/// 创建转存任务响应
#[derive(Debug, Serialize)]
pub struct CreateTransferResponse {
    /// 任务 ID（创建成功时返回）
    pub task_id: Option<String>,
    /// 任务状态
    pub status: Option<TransferStatus>,
    /// 是否需要提取码
    pub need_password: bool,
}

/// 转存任务列表响应
#[derive(Debug, Serialize)]
pub struct TransferListResponse {
    pub tasks: Vec<TransferTask>,
    pub total: usize,
}

// ============================================
// API 处理器
// ============================================

/// POST /api/v1/transfers
/// 创建转存任务
pub async fn create_transfer(
    State(app_state): State<AppState>,
    Json(req): Json<CreateTransferRequest>,
) -> Json<TransferApiResponse<CreateTransferResponse>> {

    // 获取转存管理器
    let transfer_manager = {
        let guard = app_state.transfer_manager.read().await;
        match guard.clone() {
            Some(tm) => tm,
            None => {
                error!("转存管理器未初始化");
                return Json(TransferApiResponse::error(
                    error_codes::MANAGER_NOT_READY,
                    "转存管理器未初始化，请先登录",
                ));
            }
        }
    };

    // 创建转存请求
    let create_request = crate::transfer::manager::CreateTransferRequest {
        share_url: req.share_url,
        password: req.password,
        save_path: req.save_path,
        save_fs_id: req.save_fs_id,
        auto_download: req.auto_download,
        local_download_path: req.local_download_path,
    };

    // 创建任务
    match transfer_manager.create_task(create_request).await {
        Ok(response) => {
            if response.need_password {
                return Json(TransferApiResponse::error(
                    error_codes::NEED_PASSWORD,
                    response.error.unwrap_or_else(|| "需要提取码".to_string()),
                ));
            }

            if let Some(ref err) = response.error {
                // 根据错误内容返回不同的错误码
                let code = if err.contains("需要密码") || err.contains("需要提取码") {
                    error_codes::NEED_PASSWORD
                } else if err.contains("提取码错误") {
                    error_codes::INVALID_PASSWORD
                } else if err.contains("已失效") {
                    error_codes::SHARE_EXPIRED
                } else if err.contains("不存在") {
                    error_codes::SHARE_NOT_FOUND
                } else {
                    -1
                };

                return Json(TransferApiResponse::error(code, err.clone()));
            }

            info!("转存任务创建成功: task_id={:?}", response.task_id);
            Json(TransferApiResponse::success(CreateTransferResponse {
                task_id: response.task_id,
                status: response.status,
                need_password: false,
            }))
        }
        Err(e) => {
            let err_msg = e.to_string();

            error!("创建转存任务失败: {:?}", err_msg);

            // 根据错误内容返回不同的错误码
            let code = if err_msg.contains("提取码错误") {
                error_codes::INVALID_PASSWORD
            } else if err_msg.contains("已失效") {
                error_codes::SHARE_EXPIRED
            } else if err_msg.contains("不存在") {
                error_codes::SHARE_NOT_FOUND
            } else {
                -1
            };

            Json(TransferApiResponse::error(code, err_msg))
        }
    }
}

/// GET /api/v1/transfers
/// 获取所有转存任务
pub async fn get_all_transfers(
    State(app_state): State<AppState>,
) -> Result<Json<TransferApiResponse<TransferListResponse>>, StatusCode> {
    let transfer_manager = {
        let guard = app_state.transfer_manager.read().await;
        guard.clone().ok_or(StatusCode::SERVICE_UNAVAILABLE)?
    };

    let tasks = transfer_manager.get_all_tasks();
    let total = tasks.len();

    Ok(Json(TransferApiResponse::success(TransferListResponse {
        tasks,
        total,
    })))
}

/// GET /api/v1/transfers/:id
/// 获取单个转存任务
pub async fn get_transfer(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Json<TransferApiResponse<TransferTask>> {
    let transfer_manager = {
        let guard = app_state.transfer_manager.read().await;
        match guard.clone() {
            Some(tm) => tm,
            None => {
                return Json(TransferApiResponse::error(
                    error_codes::MANAGER_NOT_READY,
                    "转存管理器未初始化",
                ));
            }
        }
    };

    match transfer_manager.get_task(&task_id).await {
        Some(task) => Json(TransferApiResponse::success(task)),
        None => Json(TransferApiResponse::error(
            error_codes::TASK_NOT_FOUND,
            "任务不存在",
        )),
    }
}

/// DELETE /api/v1/transfers/:id
/// 删除转存任务
pub async fn delete_transfer(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Json<TransferApiResponse<String>> {
    info!("删除转存任务: {}", task_id);

    let transfer_manager = {
        let guard = app_state.transfer_manager.read().await;
        match guard.clone() {
            Some(tm) => tm,
            None => {
                return Json(TransferApiResponse::error(
                    error_codes::MANAGER_NOT_READY,
                    "转存管理器未初始化",
                ));
            }
        }
    };

    match transfer_manager.remove_task(&task_id) {
        Ok(()) => {
            info!("转存任务删除成功: {}", task_id);
            Json(TransferApiResponse::success("ok".to_string()))
        }
        Err(e) => {
            error!("删除转存任务失败: {:?}", e.to_string());
            Json(TransferApiResponse::error(-1, e.to_string()))
        }
    }
}

/// POST /api/v1/transfers/:id/cancel
/// 取消转存任务
pub async fn cancel_transfer(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Json<TransferApiResponse<String>> {
    info!("取消转存任务: {}", task_id);

    let transfer_manager = {
        let guard = app_state.transfer_manager.read().await;
        match guard.clone() {
            Some(tm) => tm,
            None => {
                return Json(TransferApiResponse::error(
                    error_codes::MANAGER_NOT_READY,
                    "转存管理器未初始化",
                ));
            }
        }
    };

    match transfer_manager.cancel_task(&task_id) {
        Ok(()) => {
            info!("转存任务取消成功: {}", task_id);
            Json(TransferApiResponse::success("ok".to_string()))
        }
        Err(e) => {
            error!("取消转存任务失败: {:?}", e);
            Json(TransferApiResponse::error(-1, e.to_string()))
        }
    }
}
