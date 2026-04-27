use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::filesystem::{
    FilesystemConfig, FilesystemService, ListRequest, ListResponse, SortField, SortOrder,
};
use crate::server::handlers::ApiResponse;
use crate::server::AppState;

#[derive(Debug, Deserialize)]
pub struct LocalFileListQuery {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub page: usize,
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    #[serde(default)]
    pub sort_field: SortField,
    #[serde(default)]
    pub sort_order: SortOrder,
}

fn default_page_size() -> usize {
    100
}

pub async fn list_local_files(
    State(state): State<AppState>,
    Query(query): Query<LocalFileListQuery>,
) -> Result<Json<ApiResponse<ListResponse>>, StatusCode> {
    let download_dir = state
        .config
        .read()
        .await
        .download
        .download_dir
        .canonicalize()
        .unwrap_or_else(|_| state.config.blocking_read().download.download_dir.clone());

    let download_dir_str = download_dir.to_string_lossy().to_string();

    let service = FilesystemService::new(FilesystemConfig {
        allowed_paths: vec![download_dir_str.clone()],
        default_path: Some(download_dir_str.clone()),
        ..Default::default()
    });

    let list_path = if query.path.is_empty() {
        download_dir_str
    } else {
        query.path.clone()
    };

    let req = ListRequest {
        path: list_path,
        page: query.page,
        page_size: query.page_size,
        sort_field: query.sort_field,
        sort_order: query.sort_order,
    };

    match service.list_directory(&req) {
        Ok(response) => Ok(Json(ApiResponse::success(response))),
        Err(e) => {
            error!("列出本地文件失败: {}", e);
            Ok(Json(ApiResponse::error(500, format!("列出本地文件失败: {}", e))))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeleteLocalFilesRequest {
    pub paths: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DeleteLocalFilesData {
    pub deleted_count: usize,
    pub failed_paths: Vec<String>,
}

pub async fn delete_local_files(
    State(state): State<AppState>,
    Json(request): Json<DeleteLocalFilesRequest>,
) -> Result<Json<ApiResponse<DeleteLocalFilesData>>, StatusCode> {
    info!("API: 删除本地文件 paths={:?}", request.paths);

    if request.paths.is_empty() {
        return Ok(Json(ApiResponse::error(400, "路径列表不能为空".to_string())));
    }

    let download_dir = state
        .config
        .read()
        .await
        .download
        .download_dir
        .canonicalize()
        .unwrap_or_else(|_| state.config.blocking_read().download.download_dir.clone());

    let mut deleted_count = 0usize;
    let mut failed_paths = Vec::new();

    for path_str in &request.paths {
        let path = std::path::Path::new(path_str);
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => {
                failed_paths.push(path_str.clone());
                continue;
            }
        };

        if !canonical.starts_with(&download_dir) {
            error!("路径不在下载目录内: {}", path_str);
            failed_paths.push(path_str.clone());
            continue;
        }

        let result = if canonical.is_dir() {
            std::fs::remove_dir_all(&canonical)
        } else {
            std::fs::remove_file(&canonical)
        };

        match result {
            Ok(()) => deleted_count += 1,
            Err(e) => {
                error!("删除失败 {}: {}", path_str, e);
                failed_paths.push(path_str.clone());
            }
        }
    }

    info!("本地文件删除完成: 成功={}, 失败={}", deleted_count, failed_paths.len());
    Ok(Json(ApiResponse::success(DeleteLocalFilesData {
        deleted_count,
        failed_paths,
    })))
}
