use crate::downloader::DownloadTask;
use crate::server::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use super::ApiResponse;

/// 创建下载任务请求
#[derive(Debug, Deserialize)]
pub struct CreateDownloadRequest {
    pub fs_id: u64,
    pub remote_path: String,
    pub filename: String,
    pub total_size: u64,
}

// ============================================
// 批量下载相关结构
// ============================================

/// 批量下载项
#[derive(Debug, Deserialize)]
pub struct BatchDownloadItem {
    /// 文件系统ID
    pub fs_id: u64,
    /// 远程路径
    pub path: String,
    /// 文件/文件夹名称
    pub name: String,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件大小（文件夹为 None 或 0）
    pub size: Option<u64>,
    /// 原始名称（加密文件/文件夹的还原名称）
    pub original_name: Option<String>,
}

/// 批量下载请求
#[derive(Debug, Deserialize)]
pub struct CreateBatchDownloadRequest {
    /// 下载项列表
    pub items: Vec<BatchDownloadItem>,
    /// 本地下载目录
    pub target_dir: String,
}

/// 批量下载响应
#[derive(Debug, Serialize)]
pub struct BatchDownloadResponse {
    /// 成功创建的单文件任务ID列表
    pub task_ids: Vec<String>,
    /// 成功创建的文件夹任务ID列表
    pub folder_task_ids: Vec<String>,
    /// 失败的项
    pub failed: Vec<BatchDownloadError>,
}

/// 批量下载错误项
#[derive(Debug, Serialize)]
pub struct BatchDownloadError {
    /// 文件/文件夹路径
    pub path: String,
    /// 失败原因
    pub reason: String,
}

/// POST /api/v1/downloads
/// 创建下载任务
pub async fn create_download(
    State(app_state): State<AppState>,
    Json(req): Json<CreateDownloadRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    // 获取下载管理器
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match download_manager
        .create_task(req.fs_id, req.remote_path, req.filename, req.total_size)
        .await
    {
        Ok(task_id) => {
            info!("创建下载任务成功: {}", task_id);

            // 自动开始下载
            if let Err(e) = download_manager.start_task(&task_id).await {
                error!("启动下载任务失败: {:?}", e);
            }

            Ok(Json(ApiResponse::success(task_id)))
        }
        Err(e) => {
            error!("创建下载任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// GET /api/v1/downloads
/// 获取所有下载任务
pub async fn get_all_downloads(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<DownloadTask>>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let tasks = download_manager.get_all_tasks().await;
    Ok(Json(ApiResponse::success(tasks)))
}

/// GET /api/v1/downloads/active
/// 🔥 获取活跃的下载任务（用于降级轮询）
pub async fn get_active_downloads(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<DownloadTask>>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let tasks: Vec<DownloadTask> = download_manager
        .get_all_tasks()
        .await
        .into_iter()
        .filter(|t| {
            matches!(
                t.status,
                crate::downloader::TaskStatus::Downloading
                    | crate::downloader::TaskStatus::Pending
            )
        })
        .collect();

    Ok(Json(ApiResponse::success(tasks)))
}

/// GET /api/v1/downloads/:id
/// 获取指定下载任务
pub async fn get_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<DownloadTask>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match download_manager.get_task(&task_id).await {
        Some(task) => Ok(Json(ApiResponse::success(task))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/v1/downloads/:id/pause
/// 暂停下载任务
pub async fn pause_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // 正常暂停场景，skip_try_start_waiting=false，允许启动等待队列任务
    match download_manager.pause_task(&task_id, false).await {
        Ok(_) => {
            info!("暂停下载任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("Task paused".to_string())))
        }
        Err(e) => {
            error!("暂停下载任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// POST /api/v1/downloads/:id/resume
/// 恢复下载任务
pub async fn resume_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match download_manager.resume_task(&task_id).await {
        Ok(_) => {
            info!("恢复下载任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("Task resumed".to_string())))
        }
        Err(e) => {
            error!("恢复下载任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// DELETE /api/v1/downloads/:id
/// 删除下载任务
#[derive(Debug, Deserialize)]
pub struct DeleteDownloadQuery {
    #[serde(default)]
    pub delete_file: bool,
}

pub async fn delete_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<DeleteDownloadQuery>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    match download_manager
        .delete_task(&task_id, query.delete_file)
        .await
    {
        Ok(_) => {
            info!("删除下载任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("Task deleted".to_string())))
        }
        Err(e) => {
            error!("删除下载任务失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// DELETE /api/v1/downloads/clear/completed
/// 清除已完成的任务
pub async fn clear_completed(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = download_manager.clear_completed().await;
    Ok(Json(ApiResponse::success(count)))
}

/// DELETE /api/v1/downloads/clear/failed
/// 清除失败的任务
pub async fn clear_failed(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = download_manager.clear_failed().await;
    Ok(Json(ApiResponse::success(count)))
}

// ============================================
// 批量下载 API
// ============================================

/// POST /api/v1/downloads/batch
/// 批量下载文件/文件夹
///
/// 根据 `is_dir` 自动选择使用：
/// - 单文件下载（DownloadManager.create_task_with_dir）
/// - 文件夹下载（FolderDownloadManager.create_folder_download_with_dir）
pub async fn create_batch_download(
    State(app_state): State<AppState>,
    Json(req): Json<CreateBatchDownloadRequest>,
) -> Result<Json<ApiResponse<BatchDownloadResponse>>, StatusCode> {
    info!(
        "批量下载请求: {} 个项目, 目标目录: {}",
        req.items.len(),
        req.target_dir
    );

    // 验证目标目录
    let target_dir = std::path::PathBuf::from(&req.target_dir);
    if !target_dir.exists() {
        // 尝试创建目录
        if let Err(e) = std::fs::create_dir_all(&target_dir) {
            error!("创建目标目录失败: {:?}, 错误: {}", target_dir, e);
            return Err(StatusCode::BAD_REQUEST);
        }
        info!("已创建目标目录: {:?}", target_dir);
    }

    // 获取下载管理器
    let download_manager = app_state
        .download_manager
        .read()
        .await
        .clone()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let folder_download_manager = &app_state.folder_download_manager;

    let mut task_ids = Vec::new();
    let mut folder_task_ids = Vec::new();
    let mut failed = Vec::new();

    // 处理每个下载项
    for item in req.items {
        if item.is_dir {
            // 文件夹下载
            match folder_download_manager
                .create_folder_download_with_dir(item.path.clone(), &target_dir, item.original_name.clone())
                .await
            {
                Ok(folder_id) => {
                    info!("创建文件夹下载任务成功: {}, ID: {}", item.path, folder_id);
                    folder_task_ids.push(folder_id);
                }
                Err(e) => {
                    warn!("创建文件夹下载任务失败: {}, 错误: {:?}", item.path, e);
                    failed.push(BatchDownloadError {
                        path: item.path.clone(),
                        reason: e.to_string(),
                    });
                }
            }
        } else {
            // 单文件下载
            let file_size = item.size.unwrap_or(0);

            match download_manager
                .create_task_with_dir(
                    item.fs_id,
                    item.path.clone(),
                    item.name.clone(),
                    file_size,
                    &target_dir,
                )
                .await
            {
                Ok(task_id) => {
                    info!("创建下载任务成功: {}, ID: {}", item.path, task_id);

                    // 自动开始下载
                    if let Err(e) = download_manager.start_task(&task_id).await {
                        warn!("启动下载任务失败: {:?}", e);
                    }

                    task_ids.push(task_id);
                }
                Err(e) => {
                    warn!("创建下载任务失败: {}, 错误: {:?}", item.path, e);
                    failed.push(BatchDownloadError {
                        path: item.path.clone(),
                        reason: e.to_string(),
                    });
                }
            }
        }
    }

    info!(
        "批量下载完成: {} 个文件任务, {} 个文件夹任务, {} 个失败",
        task_ids.len(),
        folder_task_ids.len(),
        failed.len()
    );

    Ok(Json(ApiResponse::success(BatchDownloadResponse {
        task_ids,
        folder_task_ids,
        failed,
    })))
}
