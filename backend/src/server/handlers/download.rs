use crate::downloader::DownloadTask;
use crate::server::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::Deserialize;
use tracing::{error, info};

use super::ApiResponse;

/// 创建下载任务请求
#[derive(Debug, Deserialize)]
pub struct CreateDownloadRequest {
    pub fs_id: u64,
    pub remote_path: String,
    pub filename: String,
    pub total_size: u64,
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

    match download_manager.pause_task(&task_id).await {
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
