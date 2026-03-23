use crate::filesystem::{FilesystemConfig, PathGuard};
use crate::server::error::{ApiError, ApiResult};
use crate::server::AppState;
use crate::uploader::{ScanOptions, ScanTaskStatus, UploadConflictStrategy, UploadTask};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{error, info};

use super::ApiResponse;

/// 创建单文件上传任务请求
#[derive(Debug, Deserialize)]
pub struct CreateUploadRequest {
    /// 本地文件路径
    pub local_path: String,
    /// 网盘目标路径
    pub remote_path: String,
    /// 是否启用加密（可选，默认 false）
    #[serde(default)]
    pub encrypt: bool,
    /// 冲突策略（可选，未指定则使用默认值）
    #[serde(default)]
    pub conflict_strategy: Option<UploadConflictStrategy>,
}

/// 创建文件夹上传任务请求
#[derive(Debug, Deserialize)]
pub struct CreateFolderUploadRequest {
    /// 本地文件夹路径
    pub local_folder: String,
    /// 网盘目标文件夹路径
    pub remote_folder: String,
    /// 扫描选项（可选）
    #[serde(default)]
    pub scan_options: Option<FolderScanOptions>,
    /// 是否启用加密（可选，默认 false）
    #[serde(default)]
    pub encrypt: bool,
    /// 冲突策略（可选，未指定则使用默认值）
    #[serde(default)]
    pub conflict_strategy: Option<UploadConflictStrategy>,
}

/// 文件夹扫描选项（序列化友好版本）
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FolderScanOptions {
    /// 是否跟随符号链接
    #[serde(default)]
    pub follow_symlinks: bool,
    /// 最大文件大小（字节）
    pub max_file_size: Option<u64>,
    /// 最大文件数量
    pub max_files: Option<usize>,
    /// 跳过隐藏文件
    #[serde(default = "default_skip_hidden")]
    pub skip_hidden: bool,
}

fn default_skip_hidden() -> bool {
    true
}

fn create_path_guard(config: &FilesystemConfig) -> PathGuard {
    PathGuard::new(config.clone())
}

fn validate_upload_file_path(guard: &PathGuard, path: &str) -> Result<PathBuf, ApiError> {
    let normalized = guard
        .normalize(path)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    if !normalized.is_file() {
        return Err(ApiError::BadRequest(format!(
            "上传源文件不存在或不是文件: {}",
            path
        )));
    }

    Ok(normalized)
}

fn validate_upload_directory_path(guard: &PathGuard, path: &str) -> Result<PathBuf, ApiError> {
    let normalized = guard
        .normalize(path)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    if !normalized.is_dir() {
        return Err(ApiError::BadRequest(format!(
            "上传源目录不存在或不是目录: {}",
            path
        )));
    }

    Ok(normalized)
}

impl From<FolderScanOptions> for ScanOptions {
    fn from(options: FolderScanOptions) -> Self {
        Self {
            follow_symlinks: options.follow_symlinks,
            max_file_size: options.max_file_size,
            max_files: options.max_files,
            skip_hidden: options.skip_hidden,
        }
    }
}

/// 批量创建上传任务请求
#[derive(Debug, Deserialize)]
pub struct CreateBatchUploadRequest {
    /// 文件列表 [(本地路径, 远程路径)]
    pub files: Vec<(String, String)>,
    /// 是否启用加密（可选，默认 false）
    #[serde(default)]
    pub encrypt: bool,
    /// 冲突策略（可选，未指定则使用默认值）
    #[serde(default)]
    pub conflict_strategy: Option<UploadConflictStrategy>,
}

/// 扫描启动响应
#[derive(Debug, Serialize)]
pub struct ScanStartResponse {
    pub scan_task_id: String,
}

/// 扫描状态响应
#[derive(Debug, Serialize)]
pub struct ScanStatusResponse {
    pub scan_task_id: String,
    pub status: String,
    pub scanned_files: usize,
    pub created_tasks: usize,
    pub skipped_duplicates: usize,
    pub total_size: u64,
}

/// POST /api/v1/uploads
/// 创建单文件上传任务
pub async fn create_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateUploadRequest>,
) -> ApiResult<Json<ApiResponse<String>>> {
    // 获取上传管理器
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("上传管理器未初始化")))?;

    let config = app_state.config.read().await;
    let guard = create_path_guard(&config.filesystem);

    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = req
        .conflict_strategy
        .or(Some(config.conflict_strategy.default_upload_strategy));

    let local_path = validate_upload_file_path(&guard, &req.local_path)?;
    drop(config);

    // 🔥 传递 encrypt 参数，普通文件上传 is_folder_upload = false
    match upload_manager
        .create_task(
            local_path,
            req.remote_path,
            req.encrypt,
            false,
            conflict_strategy,
        )
        .await
    {
        Ok(task_id) => {
            info!("创建上传任务成功: {} (encrypt={})", task_id, req.encrypt);

            // 自动开始上传
            if let Err(e) = upload_manager.start_task(&task_id).await {
                error!("启动上传任务失败: {:?}", e);
            }

            Ok(Json(ApiResponse::success(task_id)))
        }
        Err(e) => {
            error!("创建上传任务失败: {:?}", e);
            Err(ApiError::Internal(anyhow::anyhow!(e.to_string())))
        }
    }
}

/// POST /api/v1/uploads/folder
/// 创建文件夹上传任务（异步扫描模式）
pub async fn create_folder_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateFolderUploadRequest>,
) -> ApiResult<Json<ApiResponse<ScanStartResponse>>> {
    // 获取扫描管理器
    let scan_manager = app_state
        .scan_manager
        .read()
        .await
        .clone()
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("扫描管理器未初始化")))?;

    // 获取配置
    let config = app_state.config.read().await;
    let guard = create_path_guard(&config.filesystem);
    let skip_hidden_files = config.upload.skip_hidden_files;
    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = req
        .conflict_strategy
        .or(Some(config.conflict_strategy.default_upload_strategy));

    let local_folder = validate_upload_directory_path(&guard, &req.local_folder)?;
    drop(config);

    let scan_options = if let Some(opts) = req.scan_options {
        Some(opts.into())
    } else {
        Some(ScanOptions {
            skip_hidden: skip_hidden_files,
            ..Default::default()
        })
    };

    match scan_manager
        .start_scan(
            local_folder,
            req.remote_folder,
            scan_options,
            req.encrypt,
            conflict_strategy,
        )
        .await
    {
        Ok(scan_task_id) => {
            info!(
                "文件夹扫描任务已启动: {} (encrypt={})",
                scan_task_id, req.encrypt
            );
            Ok(Json(ApiResponse::success(ScanStartResponse {
                scan_task_id,
            })))
        }
        Err(e) => {
            error!("启动文件夹扫描失败: {:?}", e);
            Err(ApiError::Internal(anyhow::anyhow!(e.to_string())))
        }
    }
}

/// POST /api/v1/uploads/batch
/// 批量创建上传任务
pub async fn create_batch_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateBatchUploadRequest>,
) -> ApiResult<Json<ApiResponse<Vec<String>>>> {
    // 获取上传管理器
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("上传管理器未初始化")))?;

    let config = app_state.config.read().await;
    let guard = create_path_guard(&config.filesystem);

    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = req
        .conflict_strategy
        .or(Some(config.conflict_strategy.default_upload_strategy));

    // 转换为 PathBuf，并补充白名单校验
    let files: Vec<(PathBuf, String)> = req
        .files
        .into_iter()
        .map(|(local, remote)| validate_upload_file_path(&guard, &local).map(|path| (path, remote)))
        .collect::<Result<Vec<_>, _>>()?;
    drop(config);

    // 🔥 传递 encrypt 参数
    match upload_manager
        .create_batch_tasks(files, req.encrypt, conflict_strategy)
        .await
    {
        Ok(task_ids) => {
            info!(
                "批量创建上传任务成功: {} 个 (encrypt={})",
                task_ids.len(),
                req.encrypt
            );

            // 自动开始所有任务
            for task_id in &task_ids {
                if let Err(e) = upload_manager.start_task(task_id).await {
                    error!("启动上传任务失败: {}, 错误: {:?}", task_id, e);
                }
            }

            Ok(Json(ApiResponse::success(task_ids)))
        }
        Err(e) => {
            error!("批量创建上传任务失败: {:?}", e);
            Err(ApiError::Internal(anyhow::anyhow!(e.to_string())))
        }
    }
}

/// GET /api/v1/uploads
/// 获取所有上传任务
pub async fn get_all_uploads(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<UploadTask>>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let tasks = upload_manager.get_all_tasks().await;
    Ok(Json(ApiResponse::success(tasks)))
}

/// GET /api/v1/uploads/:id
/// 获取指定上传任务
pub async fn get_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<UploadTask>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match upload_manager.get_task(&task_id).await {
        Some(task) => Ok(Json(ApiResponse::success(task))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/v1/uploads/:id/pause
/// 暂停上传任务
pub async fn pause_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // skip_try_start_waiting = false，正常暂停行为（暂停后尝试启动等待队列中的任务）
    match upload_manager.pause_task(&task_id, false).await {
        Ok(()) => {
            info!("暂停上传任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("已暂停".to_string())))
        }
        Err(e) => {
            error!("暂停上传任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// POST /api/v1/uploads/:id/resume
/// 恢复上传任务
pub async fn resume_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match upload_manager.resume_task(&task_id).await {
        Ok(()) => {
            info!("恢复上传任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("已恢复".to_string())))
        }
        Err(e) => {
            error!("恢复上传任务失败: {:?}", e);
            Ok(Json(ApiResponse::error(
                -1,
                format!("恢复上传任务失败: {}", e),
            )))
        }
    }
}

/// DELETE /api/v1/uploads/:id
/// 删除上传任务
pub async fn delete_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match upload_manager.delete_task(&task_id).await {
        Ok(()) => {
            info!("删除上传任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("已删除".to_string())))
        }
        Err(e) => {
            error!("删除上传任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// POST /api/v1/uploads/clear-completed
/// 清除已完成的上传任务
pub async fn clear_completed_uploads(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = upload_manager.clear_completed().await;
    info!("清除了 {} 个已完成的上传任务", count);
    Ok(Json(ApiResponse::success(count)))
}

/// POST /api/v1/uploads/clear-failed
/// 清除失败的上传任务
pub async fn clear_failed_uploads(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = upload_manager.clear_failed().await;
    info!("清除了 {} 个失败的上传任务", count);
    Ok(Json(ApiResponse::success(count)))
}

/// GET /api/v1/uploads/scan/:id
/// 查询扫描任务状态
pub async fn get_scan_status(
    State(app_state): State<AppState>,
    Path(scan_task_id): Path<String>,
) -> Result<Json<ApiResponse<ScanStatusResponse>>, StatusCode> {
    let scan_manager = app_state
        .scan_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match scan_manager.get_scan_status(&scan_task_id) {
        Some(info) => {
            let status_str = match info.status {
                ScanTaskStatus::Scanning => "scanning",
                ScanTaskStatus::Completed => "completed",
                ScanTaskStatus::Failed => "failed",
                ScanTaskStatus::Cancelled => "cancelled",
            };
            Ok(Json(ApiResponse::success(ScanStatusResponse {
                scan_task_id: info.scan_task_id,
                status: status_str.to_string(),
                scanned_files: info.scanned_files,
                created_tasks: info.created_tasks,
                skipped_duplicates: info.skipped_duplicates,
                total_size: info.total_size,
            })))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/v1/uploads/scan/:id/cancel
/// 取消扫描任务
pub async fn cancel_scan(
    State(app_state): State<AppState>,
    Path(scan_task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let scan_manager = app_state
        .scan_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    if scan_manager.cancel_scan(&scan_task_id) {
        info!("取消扫描任务: {}", scan_task_id);
        Ok(Json(ApiResponse::success("已取消".to_string())))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// ==================== 批量操作 ====================

use super::common::{BatchOperationItem, BatchOperationRequest, BatchOperationResponse};

/// POST /api/v1/uploads/batch/pause
pub async fn batch_pause_uploads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let mgr = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let ids = if req.all == Some(true) {
        mgr.get_pausable_task_ids().await
    } else {
        req.task_ids.unwrap_or_default()
    };

    let raw = mgr.batch_pause(&ids).await;
    let results: Vec<BatchOperationItem> = raw
        .into_iter()
        .map(|(id, ok, err)| BatchOperationItem {
            task_id: id,
            success: ok,
            error: err,
        })
        .collect();
    Ok(Json(ApiResponse::success(
        BatchOperationResponse::from_results(results),
    )))
}

/// POST /api/v1/uploads/batch/resume
pub async fn batch_resume_uploads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let mgr = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let ids = if req.all == Some(true) {
        mgr.get_resumable_task_ids().await
    } else {
        req.task_ids.unwrap_or_default()
    };

    let raw = mgr.batch_resume(&ids).await;
    let results: Vec<BatchOperationItem> = raw
        .into_iter()
        .map(|(id, ok, err)| BatchOperationItem {
            task_id: id,
            success: ok,
            error: err,
        })
        .collect();
    Ok(Json(ApiResponse::success(
        BatchOperationResponse::from_results(results),
    )))
}

/// POST /api/v1/uploads/batch/delete
pub async fn batch_delete_uploads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let mgr = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let ids = if req.all == Some(true) {
        mgr.get_all_task_ids()
    } else {
        req.task_ids.unwrap_or_default()
    };

    let raw = mgr.batch_delete(&ids).await;
    let results: Vec<BatchOperationItem> = raw
        .into_iter()
        .map(|(id, ok, err)| BatchOperationItem {
            task_id: id,
            success: ok,
            error: err,
        })
        .collect();
    Ok(Json(ApiResponse::success(
        BatchOperationResponse::from_results(results),
    )))
}
