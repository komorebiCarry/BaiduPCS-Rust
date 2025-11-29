// 文件系统 API 处理器

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;

use crate::filesystem::{
    FilesystemConfig, FilesystemService, FileEntry, FsError, FsErrorCode,
    GotoRequest, GotoResponse, ListRequest, ListResponse, ValidateRequest, ValidateResponse,
};
use crate::server::state::AppState;

// 使用 auth 模块的 ApiResponse
use super::auth::ApiResponse;

/// 错误响应
#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

impl IntoResponse for FsError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.code {
            FsErrorCode::PathNotAllowed => StatusCode::FORBIDDEN,
            FsErrorCode::DirectoryNotFound => StatusCode::NOT_FOUND,
            FsErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
            FsErrorCode::SymlinkRejected => StatusCode::FORBIDDEN,
            FsErrorCode::DirectoryReadFailed => StatusCode::INTERNAL_SERVER_ERROR,
            FsErrorCode::InvalidPathFormat => StatusCode::BAD_REQUEST,
            FsErrorCode::PathTraversalDetected => StatusCode::BAD_REQUEST,
            FsErrorCode::FileNotFound => StatusCode::NOT_FOUND,
            FsErrorCode::NotADirectory => StatusCode::BAD_REQUEST,
            FsErrorCode::NotAFile => StatusCode::BAD_REQUEST,
        };

        let body = Json(ErrorResponse {
            code: self.code.code(),
            message: self.message,
            path: self.path,
        });

        (status, body).into_response()
    }
}

/// 创建文件系统服务
fn create_fs_service() -> FilesystemService {
    // TODO: 从配置读取 FilesystemConfig
    FilesystemService::new(FilesystemConfig::default())
}

/// GET /api/v1/fs/list?path=/&page=0&page_size=100&sort_field=name&sort_order=asc
/// 列出目录内容（支持分页）
pub async fn list_directory(
    State(_app_state): State<AppState>,
    Query(req): Query<ListRequest>,
) -> Result<Json<ApiResponse<ListResponse>>, FsError> {
    let service = create_fs_service();
    let response = service.list_directory(&req)?;
    Ok(Json(ApiResponse::success(response)))
}

/// GET /api/v1/fs/goto?path=/home/user/documents
/// 路径跳转（直达路径）
pub async fn goto_path(
    State(_app_state): State<AppState>,
    Query(req): Query<GotoRequest>,
) -> Json<ApiResponse<GotoResponse>> {
    let service = create_fs_service();
    let response = service.goto_path(&req);
    Json(ApiResponse::success(response))
}

/// GET /api/v1/fs/validate?path=/xxx&type=file
/// 校验路径有效性
pub async fn validate_path(
    State(_app_state): State<AppState>,
    Query(req): Query<ValidateRequest>,
) -> Json<ApiResponse<ValidateResponse>> {
    let service = create_fs_service();
    let response = service.validate_path(&req);
    Json(ApiResponse::success(response))
}

/// GET /api/v1/fs/roots
/// 获取根目录列表
pub async fn get_roots(
    State(_app_state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<FileEntry>>>, FsError> {
    let service = create_fs_service();
    let roots = service.get_roots()?;
    Ok(Json(ApiResponse::success(roots)))
}