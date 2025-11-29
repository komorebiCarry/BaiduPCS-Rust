use crate::server::AppState;
use crate::uploader::{ScanOptions, UploadTask};
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
}

/// POST /api/v1/uploads
/// 创建单文件上传任务
pub async fn create_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateUploadRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    // 获取上传管理器
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let local_path = PathBuf::from(&req.local_path);

    match upload_manager
        .create_task(local_path, req.remote_path)
        .await
    {
        Ok(task_id) => {
            info!("创建上传任务成功: {}", task_id);

            // 自动开始上传
            if let Err(e) = upload_manager.start_task(&task_id).await {
                error!("启动上传任务失败: {:?}", e);
            }

            Ok(Json(ApiResponse::success(task_id)))
        }
        Err(e) => {
            error!("创建上传任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// POST /api/v1/uploads/folder
/// 创建文件夹上传任务
pub async fn create_folder_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateFolderUploadRequest>,
) -> Result<Json<ApiResponse<Vec<String>>>, StatusCode> {
    // 获取上传管理器
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // 获取配置
    let config = app_state.config.read().await;
    let skip_hidden_files = config.upload.skip_hidden_files;
    drop(config); // 释放读锁

    let local_folder = PathBuf::from(&req.local_folder);

    // 如果用户没有提供扫描选项，使用配置中的设置
    let scan_options = if let Some(opts) = req.scan_options {
        Some(opts.into())
    } else {
        Some(ScanOptions {
            skip_hidden: skip_hidden_files,
            ..Default::default()
        })
    };

    match upload_manager
        .create_folder_task(local_folder, req.remote_folder, scan_options)
        .await
    {
        Ok(task_ids) => {
            info!("创建文件夹上传任务成功: {} 个文件", task_ids.len());

            // 自动开始所有任务
            for task_id in &task_ids {
                if let Err(e) = upload_manager.start_task(task_id).await {
                    error!("启动上传任务失败: {}, 错误: {:?}", task_id, e);
                }
            }

            Ok(Json(ApiResponse::success(task_ids)))
        }
        Err(e) => {
            error!("创建文件夹上传任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// POST /api/v1/uploads/batch
/// 批量创建上传任务
pub async fn create_batch_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateBatchUploadRequest>,
) -> Result<Json<ApiResponse<Vec<String>>>, StatusCode> {
    // 获取上传管理器
    let upload_manager = app_state
        .upload_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // 转换为 PathBuf
    let files: Vec<(PathBuf, String)> = req
        .files
        .into_iter()
        .map(|(local, remote)| (PathBuf::from(local), remote))
        .collect();

    match upload_manager.create_batch_tasks(files).await {
        Ok(task_ids) => {
            info!("批量创建上传任务成功: {} 个", task_ids.len());

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
            Err(StatusCode::INTERNAL_SERVER_ERROR)
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

    match upload_manager.pause_task(&task_id).await {
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
            Err(StatusCode::INTERNAL_SERVER_ERROR)
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
