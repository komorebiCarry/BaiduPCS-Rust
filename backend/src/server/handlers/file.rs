// 文件API处理器

use crate::encryption::EncryptionService;
use crate::netdisk::FileItem;
use crate::server::handlers::ApiResponse;
use crate::server::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{error, info, warn};

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
    50
}

/// 带加密信息的文件项
#[derive(Debug, Serialize)]
pub struct FileItemWithEncryption {
    /// 原始文件信息
    #[serde(flatten)]
    pub file: FileItem,
    /// 是否为加密文件
    pub is_encrypted: bool,
    /// 是否为加密文件夹
    pub is_encrypted_folder: bool,
    /// 原始文件名（加密文件显示用）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_name: Option<String>,
    /// 原始文件大小（加密文件可能有）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_size: Option<u64>,
}

/// 文件列表响应
#[derive(Debug, Serialize)]
pub struct FileListData {
    /// 文件列表（带加密信息）
    pub list: Vec<FileItemWithEncryption>,
    /// 当前目录
    pub dir: String,
    /// 页码
    pub page: u32,
    /// 当前页数量
    pub total: usize,
    /// 是否还有更多数据
    pub has_more: bool,
}

/// 获取文件列表
///
/// GET /api/v1/files?dir=/&page=1&page_size=100
pub async fn get_file_list(
    State(state): State<AppState>,
    Query(params): Query<FileListQuery>,
) -> Result<Json<ApiResponse<FileListData>>, StatusCode> {
    info!("API: 获取文件列表 dir={}, page={}", params.dir, params.page);

    // 使用单例网盘客户端
    let client_lock = state.netdisk_client.read().await;
    let client = match client_lock.as_ref() {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
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
            let has_more = total >= params.page_size as usize;

            // 筛选出加密文件名（UUID.dat 格式）
            let encrypted_names: Vec<String> = file_list
                .list
                .iter()
                .filter(|f| is_encrypted_filename(&f.server_filename))
                .map(|f| f.server_filename.clone())
                .collect();

            // 筛选出加密文件夹名（纯 UUID 格式）
            let encrypted_folder_names: Vec<String> = file_list
                .list
                .iter()
                .filter(|f| f.isdir == 1 && is_encrypted_folder_name(&f.server_filename))
                .map(|f| f.server_filename.clone())
                .collect();

            // 批量查询加密文件映射
            let encryption_map = query_encryption_mappings(&state, &encrypted_names);

            // 批量查询加密文件夹映射
            let folder_map = query_folder_mappings(&state, &params.dir, &encrypted_folder_names);

            // 构建带加密信息的文件列表
            let list_with_encryption: Vec<FileItemWithEncryption> = file_list
                .list
                .into_iter()
                .map(|file| {
                    // 检查是否为加密文件夹
                    let (is_encrypted_folder, folder_original_name) =
                        if file.isdir == 1 && is_encrypted_folder_name(&file.server_filename) {
                            match folder_map.get(&file.server_filename) {
                                Some(name) => (true, Some(name.clone())),
                                None => (true, None), // 是加密格式但找不到映射
                            }
                        } else {
                            (false, None)
                        };

                    // 检查是否为加密文件
                    let (is_encrypted, original_name, original_size) =
                        if is_encrypted_filename(&file.server_filename) {
                            match encryption_map.get(&file.server_filename) {
                                Some((name, size)) => (true, Some(name.clone()), Some(*size)),
                                None => (false, None, None),
                            }
                        } else {
                            (false, None, None)
                        };

                    // 如果是加密文件夹，使用文件夹的原始名
                    let final_original_name = if is_encrypted_folder {
                        folder_original_name
                    } else {
                        original_name
                    };

                    FileItemWithEncryption {
                        file,
                        is_encrypted,
                        is_encrypted_folder,
                        original_name: final_original_name,
                        original_size,
                    }
                })
                .collect();

            let data = FileListData {
                list: list_with_encryption,
                dir: params.dir.clone(),
                page: params.page,
                total,
                has_more,
            };
            info!("成功获取 {} 个文件/文件夹, has_more={}", total, has_more);
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

/// 判断文件名是否为加密文件格式
fn is_encrypted_filename(filename: &str) -> bool {
    EncryptionService::is_encrypted_filename(filename)
}

/// 判断文件夹名是否为加密文件夹格式
fn is_encrypted_folder_name(folder_name: &str) -> bool {
    EncryptionService::is_encrypted_folder_name(folder_name)
}

/// 批量查询加密文件映射
/// 直接使用 AppState 中的 snapshot_manager，无需依赖自动备份管理器
fn query_encryption_mappings(
    state: &AppState,
    encrypted_names: &[String],
) -> HashMap<String, (String, u64)> {
    if encrypted_names.is_empty() {
        return HashMap::new();
    }

    info!("查询加密映射，文件名列表: {:?}", encrypted_names);

    // 直接使用 AppState 中的 snapshot_manager
    match state.snapshot_manager.find_by_encrypted_names(encrypted_names) {
        Ok(snapshots) => {
            info!("查询到 {} 条加密映射记录", snapshots.len());
            snapshots
                .into_iter()
                .map(|s| (s.encrypted_name, (s.original_name, s.file_size)))
                .collect()
        }
        Err(e) => {
            warn!("查询加密映射失败: {}", e);
            HashMap::new()
        }
    }
}

/// 批量查询加密文件夹映射
fn query_folder_mappings(
    state: &AppState,
    parent_path: &str,
    encrypted_folder_names: &[String],
) -> HashMap<String, String> {
    if encrypted_folder_names.is_empty() {
        return HashMap::new();
    }

    info!("查询文件夹映射，父路径: {}, 文件夹列表: {:?}", parent_path, encrypted_folder_names);

    let mut result = HashMap::new();

    // 遍历所有加密文件夹名，查找映射
    for encrypted_name in encrypted_folder_names {
        // 查询所有配置的映射（返回 EncryptionSnapshot）
        if let Ok(snapshots) = state.backup_record_manager.get_all_folder_mappings_by_encrypted_name(encrypted_name) {
            for snapshot in snapshots {
                // original_path 存储的是父路径
                if snapshot.original_path == parent_path {
                    result.insert(encrypted_name.clone(), snapshot.original_name);
                    break;
                }
            }
        }
    }

    info!("查询到 {} 条文件夹映射记录", result.len());
    result
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

    // 使用单例网盘客户端
    let client_lock = state.netdisk_client.read().await;
    let client = match client_lock.as_ref() {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
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

/// 创建文件夹请求体
#[derive(Debug, Deserialize)]
pub struct CreateFolderRequest {
    /// 文件夹路径（必须以 / 开头）
    pub path: String,
}

/// 创建文件夹响应数据
#[derive(Debug, Serialize)]
pub struct CreateFolderData {
    /// 文件服务器ID
    pub fs_id: u64,
    /// 文件夹路径
    pub path: String,
    /// 是否是目录
    pub isdir: i32,
}

/// 创建文件夹
///
/// POST /api/v1/files/folder
/// Body: { "path": "/apps/test/新建文件夹" }
pub async fn create_folder(
    State(state): State<AppState>,
    Json(request): Json<CreateFolderRequest>,
) -> Result<Json<ApiResponse<CreateFolderData>>, StatusCode> {
    info!("API: 创建文件夹 path={}", request.path);

    // 验证路径格式
    if !request.path.starts_with('/') {
        return Ok(Json(ApiResponse::error(
            400,
            "路径必须以 / 开头".to_string(),
        )));
    }

    // 使用单例网盘客户端
    let client_lock = state.netdisk_client.read().await;
    let client = match client_lock.as_ref() {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 创建文件夹
    match client.create_folder(&request.path).await {
        Ok(response) => {
            let data = CreateFolderData {
                fs_id: response.fs_id,
                path: response.path,
                isdir: response.isdir,
            };
            info!("成功创建文件夹: fs_id={}", data.fs_id);
            Ok(Json(ApiResponse::success(data)))
        }
        Err(e) => {
            error!("创建文件夹失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("创建文件夹失败: {}", e),
            )))
        }
    }
}
