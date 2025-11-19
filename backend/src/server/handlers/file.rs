// 文件API处理器

use crate::netdisk::{FileItem, NetdiskClient};
use crate::server::handlers::ApiResponse;
use crate::server::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

/// 文件列表查询参数
#[derive(Debug, Deserialize)]
pub struct FileListQuery {
    /// 目录路径
    #[serde(default = "default_dir")]
    pub dir: String,
    /// 页码
    #[serde(default = "default_page")]
    pub page: u32,
    /// 每页数量
    #[serde(default = "default_page_size")]
    pub page_size: u32,
}

fn default_dir() -> String {
    "/".to_string()
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    100
}

/// 文件列表响应
#[derive(Debug, Serialize)]
pub struct FileListData {
    /// 文件列表
    pub list: Vec<FileItem>,
    /// 当前目录
    pub dir: String,
    /// 页码
    pub page: u32,
    /// 总数量
    pub total: usize,
}

/// 获取文件列表
///
/// GET /api/v1/files?dir=/&page=1&page_size=100
pub async fn get_file_list(
    State(state): State<AppState>,
    Query(params): Query<FileListQuery>,
) -> Result<Json<ApiResponse<FileListData>>, StatusCode> {
    info!("API: 获取文件列表 dir={}, page={}", params.dir, params.page);

    // 获取当前会话
    let mut session = state.session_manager.lock().await;
    let user_auth = match session.get_session().await {
        Ok(Some(auth)) => auth,
        Ok(None) => {
            return Ok(Json(ApiResponse::error(401, "未登录".to_string())));
        }
        Err(e) => {
            error!("获取会话失败: {}", e);
            return Ok(Json(ApiResponse::error(
                500,
                format!("获取会话失败: {}", e),
            )));
        }
    };
    drop(session);

    // 创建网盘客户端
    let client = match NetdiskClient::new(user_auth) {
        Ok(c) => c,
        Err(e) => {
            error!("创建网盘客户端失败: {}", e);
            return Ok(Json(ApiResponse::error(
                500,
                format!("创建客户端失败: {}", e),
            )));
        }
    };

    // 获取文件列表
    match client
        .get_file_list(&params.dir, params.page, params.page_size)
        .await
    {
        Ok(file_list) => {
            let total = file_list.list.len();
            let data = FileListData {
                list: file_list.list,
                dir: params.dir.clone(),
                page: params.page,
                total,
            };
            info!("成功获取 {} 个文件/文件夹", total);
            Ok(Json(ApiResponse::success(data)))
        }
        Err(e) => {
            error!("获取文件列表失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("获取文件列表失败: {}", e),
            )))
        }
    }
}

/// 下载链接查询参数
#[derive(Debug, Deserialize)]
pub struct DownloadUrlQuery {
    /// 文件服务器ID
    pub fs_id: u64,
    /// 文件路径（必需，用于 Locate 下载）
    pub path: String,
}

/// 下载链接响应
#[derive(Debug, Serialize)]
pub struct DownloadUrlData {
    /// 文件服务器ID
    pub fs_id: u64,
    /// 下载URL
    pub url: String,
}

/// 获取下载链接
///
/// GET /api/v1/files/download?fs_id=123456&path=/apps/test/file.zip
pub async fn get_download_url(
    State(state): State<AppState>,
    Query(params): Query<DownloadUrlQuery>,
) -> Result<Json<ApiResponse<DownloadUrlData>>, StatusCode> {
    info!(
        "API: 获取下载链接 fs_id={}, path={}",
        params.fs_id, params.path
    );

    // 获取当前会话
    let mut session = state.session_manager.lock().await;
    let user_auth = match session.get_session().await {
        Ok(Some(auth)) => auth,
        Ok(None) => {
            return Ok(Json(ApiResponse::error(401, "未登录".to_string())));
        }
        Err(e) => {
            error!("获取会话失败: {}", e);
            return Ok(Json(ApiResponse::error(
                500,
                format!("获取会话失败: {}", e),
            )));
        }
    };
    drop(session);

    // 创建网盘客户端
    let client = match NetdiskClient::new(user_auth) {
        Ok(c) => c,
        Err(e) => {
            error!("创建网盘客户端失败: {}", e);
            return Ok(Json(ApiResponse::error(
                500,
                format!("创建客户端失败: {}", e),
            )));
        }
    };

    // 获取下载链接（使用文件路径）
    // 默认使用第一个链接（索引0）
    match client.get_download_url(&params.path, 0).await {
        Ok(url) => {
            let data = DownloadUrlData {
                fs_id: params.fs_id,
                url,
            };
            info!("成功获取下载链接");
            Ok(Json(ApiResponse::success(data)))
        }
        Err(e) => {
            error!("获取下载链接失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("获取下载链接失败: {}", e),
            )))
        }
    }
}
