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

    // 多账号路由：按 active_uid 取客户端（运行时单一真源）
    let client = match state.active_client().await {
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
    match state
        .snapshot_manager
        .find_by_encrypted_names(encrypted_names)
    {
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

    info!(
        "查询文件夹映射，父路径: {}, 文件夹列表: {:?}",
        parent_path, encrypted_folder_names
    );

    let mut result = HashMap::new();

    // 遍历所有加密文件夹名，查找映射
    for encrypted_name in encrypted_folder_names {
        // 查询所有配置的映射（返回 EncryptionSnapshot）
        if let Ok(snapshots) = state
            .backup_record_manager
            .get_all_folder_mappings_by_encrypted_name(encrypted_name)
        {
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

/// 搜索查询参数
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    /// 搜索关键词
    pub key: String,
    /// 页码
    #[serde(default = "default_page")]
    pub page: u32,
    /// 每页数量
    #[serde(default = "default_search_num")]
    pub num: u32,
    /// 是否递归搜索
    #[serde(default = "default_recursion")]
    pub recursion: i32,
}

fn default_search_num() -> u32 {
    100
}

fn default_recursion() -> i32 {
    1
}

/// 搜索响应数据
#[derive(Debug, Serialize)]
pub struct SearchData {
    /// 文件列表（带加密信息）
    pub list: Vec<FileItemWithEncryption>,
    /// 是否还有更多
    pub has_more: bool,
}

/// 搜索文件
///
/// GET /api/v1/files/search?key=xxx&page=1&num=100&recursion=1
pub async fn search_files(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<ApiResponse<SearchData>>, StatusCode> {
    info!("API: 搜索文件 key={}, page={}", params.key, params.page);

    if params.key.trim().is_empty() {
        return Ok(Json(ApiResponse::error(
            400,
            "搜索关键词不能为空".to_string(),
        )));
    }
    if params.key.len() > 255 {
        return Ok(Json(ApiResponse::error(400, "搜索关键词过长".to_string())));
    }

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    match client
        .search_files(&params.key, params.page, params.num, params.recursion)
        .await
    {
        Ok(search_result) => {
            let has_more = search_result.has_more == 1;

            // 合并 list 和 contentlist
            let mut file_list = search_result.list;
            file_list.extend(search_result.contentlist);

            // 处理加密信息
            let encrypted_names: Vec<String> = file_list
                .iter()
                .filter(|f| is_encrypted_filename(&f.server_filename))
                .map(|f| f.server_filename.clone())
                .collect();

            let encryption_map = query_encryption_mappings(&state, &encrypted_names);

            let list_with_encryption: Vec<FileItemWithEncryption> = file_list
                .into_iter()
                .map(|file| {
                    let (is_encrypted, original_name, original_size) =
                        if is_encrypted_filename(&file.server_filename) {
                            match encryption_map.get(&file.server_filename) {
                                Some((name, size)) => (true, Some(name.clone()), Some(*size)),
                                None => (false, None, None),
                            }
                        } else {
                            (false, None, None)
                        };

                    let is_encrypted_folder =
                        file.isdir == 1 && is_encrypted_folder_name(&file.server_filename);

                    FileItemWithEncryption {
                        file,
                        is_encrypted,
                        is_encrypted_folder,
                        original_name,
                        original_size,
                    }
                })
                .collect();

            let data = SearchData {
                list: list_with_encryption,
                has_more,
            };
            Ok(Json(ApiResponse::success(data)))
        }
        Err(e) => {
            error!("搜索文件失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("搜索文件失败: {}", e),
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

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
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

/// 删除文件请求体
#[derive(Debug, Deserialize)]
pub struct DeleteFilesRequest {
    pub paths: Vec<String>,
}

/// 删除文件响应数据
#[derive(Debug, Serialize)]
pub struct DeleteFilesData {
    pub deleted_count: usize,
    pub failed_paths: Vec<String>,
}

/// 删除文件
///
/// POST /api/v1/files/delete
/// Body: { "paths": ["/path/to/file1", "/path/to/file2"] }
pub async fn delete_files(
    State(state): State<AppState>,
    Json(request): Json<DeleteFilesRequest>,
) -> Result<Json<ApiResponse<DeleteFilesData>>, StatusCode> {
    info!("API: 删除文件 paths={:?}", request.paths);

    if request.paths.is_empty() {
        return Ok(Json(ApiResponse::error(
            400,
            "路径列表不能为空".to_string(),
        )));
    }
    if let Some(p) = request.paths.iter().find(|p| !p.starts_with('/')) {
        return Ok(Json(ApiResponse::error(
            400,
            format!("路径必须以 / 开头: {}", p),
        )));
    }

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    match client.delete_files_chunked(&request.paths).await {
        Ok(response) => {
            if response.success {
                // 百度异步删除，等待1秒让服务端完成处理，避免前端刷新时文件仍在
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                info!("成功删除 {} 个文件", response.deleted_count);
                Ok(Json(ApiResponse::success(DeleteFilesData {
                    deleted_count: response.deleted_count,
                    failed_paths: response.failed_paths,
                })))
            } else {
                let msg = response.error.unwrap_or_else(|| "删除失败".to_string());
                // errno=132 透传为 code,让前端弹出明确的安全验证引导
                let code = if response.errno == Some(132) {
                    132
                } else {
                    500
                };
                Ok(Json(ApiResponse::error(code, msg)))
            }
        }
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("errno=-6") {
                warn!("删除文件遇到 errno=-6，触发预热重试...");
                match state.trigger_warmup().await {
                    Ok(true) => {
                        if let Some(c) = state.active_client().await {
                            match c.delete_files_chunked(&request.paths).await {
                                Ok(response) if response.success => {
                                    // 百度异步删除，等待1秒让服务端完成处理
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                    return Ok(Json(ApiResponse::success(DeleteFilesData {
                                        deleted_count: response.deleted_count,
                                        failed_paths: response.failed_paths,
                                    })));
                                }
                                Ok(response) => {
                                    let msg =
                                        response.error.unwrap_or_else(|| "删除失败".to_string());
                                    let code = if response.errno == Some(132) {
                                        132
                                    } else {
                                        500
                                    };
                                    return Ok(Json(ApiResponse::error(code, msg)));
                                }
                                Err(retry_err) => {
                                    error!("预热重试后仍失败: {}", retry_err);
                                    return Ok(Json(ApiResponse::error(
                                        500,
                                        format!("删除文件失败（已重试）: {}", retry_err),
                                    )));
                                }
                            }
                        }
                    }
                    Ok(false) => warn!("预热跳过（用户未登录）"),
                    Err(warmup_err) => error!("预热失败: {}", warmup_err),
                }
            }
            error!("删除文件失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("删除文件失败: {}", e),
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

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
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
            let error_msg = e.to_string();

            // 检查是否是 errno=-6，需要预热重试
            if error_msg.contains("errno=-6") {
                warn!("创建文件夹遇到 errno=-6，触发预热重试...");

                // 触发预热
                match state.trigger_warmup().await {
                    Ok(true) => {
                        info!("预热成功，重试创建文件夹...");
                        // 重新获取活跃账号客户端（预热后 cookies 已刷新）
                        if let Some(c) = state.active_client().await {
                            match c.create_folder(&request.path).await {
                                Ok(response) => {
                                    let data = CreateFolderData {
                                        fs_id: response.fs_id,
                                        path: response.path,
                                        isdir: response.isdir,
                                    };
                                    info!("预热重试成功，创建文件夹: fs_id={}", data.fs_id);
                                    return Ok(Json(ApiResponse::success(data)));
                                }
                                Err(retry_err) => {
                                    error!("预热重试后仍失败: {}", retry_err);
                                    return Ok(Json(ApiResponse::error(
                                        500,
                                        format!("创建文件夹失败（已重试）: {}", retry_err),
                                    )));
                                }
                            }
                        }
                    }
                    Ok(false) => {
                        warn!("预热跳过（用户未登录）");
                    }
                    Err(warmup_err) => {
                        error!("预热失败: {}", warmup_err);
                    }
                }
            }

            error!("创建文件夹失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("创建文件夹失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 文件管理操作（filemanager: copy / move / rename）
// =====================================================

use crate::netdisk::{
    AuthWidget, FileOperationErrorPayload, FileOperationItem, FileOperationOutcome,
    FileOperationResultItem, RenameItem,
};

/// `copy` / `move` 接口的请求体
///
/// `items` 中每条 item：
/// - `path` 必须以 `/` 开头且非根（拒绝整盘移动）
/// - `dest` 必须以 `/` 开头（无尾斜杠由 helper 归一化）
/// - `newname` 经过 `validate_filename` 校验（含 Windows 保留名 / 控制字符 / UTF-16 长度 / 非法字符）
#[derive(Debug, Deserialize)]
pub struct CopyFilesRequest {
    pub items: Vec<FileOperationItem>,
}

#[derive(Debug, Deserialize)]
pub struct MoveFilesRequest {
    pub items: Vec<FileOperationItem>,
}

/// `rename` 接口的请求体（单条）
#[derive(Debug, Deserialize)]
pub struct RenameFileRequest {
    pub item: RenameItem,
}

/// 文件管理操作统一对外 DTO
///
/// 业务结果由 `kind` 字段表达，HTTP status code 始终为 200（仅在系统层错误如客户端未初始化、
/// 序列化失败时使用 5xx）。
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileOperationOutcomeDto {
    Success {
        taskid: i64,
        total: u32,
        list: Vec<FileOperationResultItem>,
    },
    Failed {
        message: String,
        taskid: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        errno: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        task_errno: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        authwidget: Option<AuthWidget>,
        #[serde(skip_serializing_if = "Option::is_none")]
        verify_scene: Option<i32>,
        still_running: bool,
    },
}

impl From<FileOperationOutcome> for FileOperationOutcomeDto {
    fn from(o: FileOperationOutcome) -> Self {
        match o {
            FileOperationOutcome::Success(s) => FileOperationOutcomeDto::Success {
                taskid: s.taskid,
                total: s.total,
                list: s.list,
            },
            FileOperationOutcome::Failed { message, payload } => FileOperationOutcomeDto::Failed {
                message,
                taskid: payload.taskid,
                errno: payload.errno,
                task_errno: payload.task_errno,
                authwidget: payload.authwidget,
                verify_scene: payload.verify_scene,
                still_running: payload.still_running,
            },
        }
    }
}

/// 路径归一化：去除尾斜杠（除根路径），并做最简单的非空检查
///
/// 注意：本函数不做 `.` / `..` 合并，调用方应保证 `path` 来自客户端真实文件路径，
/// 而非用户拼接的相对路径。
fn normalize_path(p: &str) -> Result<String, String> {
    if p.is_empty() {
        return Err("路径不能为空".to_string());
    }
    if !p.starts_with('/') {
        return Err(format!("路径必须以 / 开头: {}", p));
    }
    if p == "/" {
        return Ok("/".to_string());
    }
    // 去除尾斜杠
    let trimmed = p.trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok("/".to_string());
    }
    Ok(trimmed.to_string())
}

/// 是否为根路径
#[inline]
fn is_root(p: &str) -> bool {
    p == "/" || p.is_empty()
}

/// 是否为加密文件命名（UUID.dat）或加密文件夹命名（UUID）
fn is_encrypted_leaf(name: &str) -> bool {
    EncryptionService::is_encrypted_filename(name)
        || EncryptionService::is_encrypted_folder_name(name)
}

/// 加密保护：禁止 copy/move 的源路径为加密文件或加密文件夹
///
/// 仅判断 leaf 名称（路径最后一段），避免把加密目录 / 文件作为整体 copy/move
/// 后破坏「UUID.dat 一一对应原文件名」的元数据约束。
fn ensure_not_encrypted_path(path: &str) -> Result<(), String> {
    let leaf = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or("");
    if is_encrypted_leaf(leaf) {
        return Err(format!("加密文件/文件夹禁止移动或复制: {}", path));
    }
    Ok(())
}

/// 加密保护：禁止 rename 的源是加密命名 或 目标改成加密命名
fn ensure_rename_not_encrypted(src_leaf: &str, new_name: &str) -> Result<(), String> {
    if is_encrypted_leaf(src_leaf) {
        return Err(format!("加密文件/文件夹禁止重命名: {}", src_leaf));
    }
    if is_encrypted_leaf(new_name) {
        return Err(format!("新名称不能使用加密命名格式: {}", new_name));
    }
    Ok(())
}

/// 校验 move 操作不会把文件夹移动到自己内部（避免百度返回不可恢复的错误）
fn ensure_not_move_into_self(path: &str, dest: &str) -> Result<(), String> {
    let path_norm = path.trim_end_matches('/');
    let dest_norm = dest.trim_end_matches('/');
    if dest_norm == path_norm {
        return Err(format!("目标位置不能与源位置相同: {}", path));
    }
    // 拒绝 dest 在 path 之下（含 path 自身作为前缀且后接 `/`）
    let prefix = format!("{}/", path_norm);
    if dest_norm.starts_with(&prefix) {
        return Err(format!(
            "不能把文件夹移动到自己的子目录: {} -> {}",
            path, dest
        ));
    }
    Ok(())
}

/// 取已归一化路径的父目录（保留 leading `/`，根路径返回 "/"）
fn parent_dir_of(path: &str) -> &str {
    let p = path.trim_end_matches('/');
    match p.rfind('/') {
        Some(0) => "/",
        Some(idx) => &p[..idx],
        None => "/",
    }
}

/// 取已归一化路径的 basename（最后一段）
fn basename_of(path: &str) -> &str {
    let p = path.trim_end_matches('/');
    p.rsplit('/').find(|s| !s.is_empty()).unwrap_or("")
}

/// 校验 move 操作的目标父目录与源父目录不相同
///
/// 文档（§ 校验清单）要求 `move` handler 明确拒绝 `dest == 源父目录` 的请求，
/// 无论 `newname` 是否变化：
/// - 同父目录 + 同名 → 完全 no-op，无意义下发
/// - 同父目录 + 改名 → 语义上是 rename，应使用 `rename_file` 接口（基于 `fs_id` 契约）
///   走 move 通道会让 fs_id 检验失效，与协议契约冲突。
fn ensure_move_not_same_parent(path: &str, dest: &str) -> Result<(), String> {
    let parent = parent_dir_of(path);
    let dest_norm = if dest.is_empty() { "/" } else { dest };
    if parent == dest_norm {
        return Err(format!(
            "移动操作无效：目标目录与源父目录相同（同目录改名请使用重命名接口）: {}",
            path
        ));
    }
    Ok(())
}

/// 校验 copy 操作不会产生同目录同名的无效复制
///
/// 百度 filemanager 在 copy 目标目录等于源父目录且 newname 等于源 basename 时
/// 会返回 `errno=-8`（文件已存在）。前端目录选择器无法修改 newname，因此这种
/// 情况是纯 UI 错点，必须提前拦截；但 copy 保留「同父目录 + 改名」语义（副本创建），
/// 所以这里**只**拒绝「同父目录 + 同名」。
fn ensure_copy_not_same_parent_same_name(
    path: &str,
    dest: &str,
    newname: &str,
) -> Result<(), String> {
    let parent = parent_dir_of(path);
    let dest_norm = if dest.is_empty() { "/" } else { dest };
    if parent == dest_norm && basename_of(path) == newname {
        return Err(format!(
            "复制操作无效：目标目录与源父目录相同且未改名（同目录同名复制会与源文件冲突）: {}",
            path
        ));
    }
    Ok(())
}

/// 归一化并校验单条 copy/move item（通用部分：路径、文件名、加密保护）
///
/// **不**包含 move-only 校验（`ensure_not_move_into_self` /
/// `ensure_move_not_same_parent`）与 copy-only 校验
/// （`ensure_copy_not_same_parent_same_name`）。对应 handler 在调用本函数之后
/// 需自行追加 op-specific 校验。
fn normalize_copy_move_item(item: &FileOperationItem) -> Result<FileOperationItem, String> {
    let path = normalize_path(&item.path)?;
    if is_root(&path) {
        return Err("源路径不能为根目录".to_string());
    }
    let dest = normalize_path(&item.dest)?;
    crate::netdisk::client::validate_filename(&item.newname)?;
    ensure_not_encrypted_path(&path)?;
    Ok(FileOperationItem {
        path,
        dest,
        newname: item.newname.clone(),
    })
}

/// 是否需要触发预热并重试一次
///
/// 仅 errno / task_errno 在 -6 / 111 这类「会话过期类」错误时返回 true。
/// 132 风控不重试（需要用户主观介入）。
fn should_retry_after_warmup(payload: &FileOperationErrorPayload) -> bool {
    let candidates = [payload.errno, payload.task_errno];
    candidates.iter().any(|e| matches!(e, Some(-6) | Some(111)))
}

/// 批量复制文件
///
/// POST /api/v1/files/copy
/// Body: { "items": [{"path":"/a/x.mp4","dest":"/b","newname":"x.mp4"}, ...] }
pub async fn copy_files(
    State(state): State<AppState>,
    Json(request): Json<CopyFilesRequest>,
) -> Result<Json<ApiResponse<FileOperationOutcomeDto>>, StatusCode> {
    info!("API: 复制文件 items.len={}", request.items.len());
    if request.items.is_empty() {
        return Ok(Json(ApiResponse::error(400, "items 不能为空".to_string())));
    }

    // 归一化 + 校验
    let mut normalized: Vec<FileOperationItem> = Vec::with_capacity(request.items.len());
    for raw in &request.items {
        match normalize_copy_move_item(raw) {
            Ok(item) => normalized.push(item),
            Err(msg) => return Ok(Json(ApiResponse::error(400, msg))),
        }
    }

    // copy 专属校验：拒绝「同父目录 + 同名」（百度 filemanager 会返回 errno=-8 文件已存在）
    // 允许「同父目录 + 改名」以保留副本创建语义。
    for item in &normalized {
        if let Err(msg) =
            ensure_copy_not_same_parent_same_name(&item.path, &item.dest, &item.newname)
        {
            return Ok(Json(ApiResponse::error(400, msg)));
        }
    }

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    let outcome = match client.copy_files(&normalized).await {
        Ok(o) => o,
        Err(e) => {
            error!("复制文件请求失败: {}", e);
            return Ok(Json(ApiResponse::error(
                500,
                format!("复制文件失败: {}", e),
            )));
        }
    };

    // 失败时按需 warmup 重试一次
    let final_outcome = if let FileOperationOutcome::Failed { ref payload, .. } = outcome {
        if should_retry_after_warmup(payload) {
            warn!(
                "复制文件遇到会话过期类错误（errno={:?}, task_errno={:?}），触发 warmup 重试",
                payload.errno, payload.task_errno
            );
            match state.trigger_warmup().await {
                Ok(true) => match state.active_client().await {
                    Some(c2) => match c2.copy_files(&normalized).await {
                        Ok(o2) => o2,
                        Err(retry_err) => {
                            error!("warmup 重试后仍失败: {}", retry_err);
                            outcome
                        }
                    },
                    None => outcome,
                },
                Ok(false) => {
                    warn!("warmup 跳过（用户未登录）");
                    outcome
                }
                Err(warmup_err) => {
                    error!("warmup 失败: {}", warmup_err);
                    outcome
                }
            }
        } else {
            outcome
        }
    } else {
        outcome
    };

    Ok(Json(ApiResponse::success(final_outcome.into())))
}

/// 批量移动文件
///
/// POST /api/v1/files/move
/// Body: { "items": [{"path":"/a/x.mp4","dest":"/b","newname":"x.mp4"}, ...] }
pub async fn move_files(
    State(state): State<AppState>,
    Json(request): Json<MoveFilesRequest>,
) -> Result<Json<ApiResponse<FileOperationOutcomeDto>>, StatusCode> {
    info!("API: 移动文件 items.len={}", request.items.len());
    if request.items.is_empty() {
        return Ok(Json(ApiResponse::error(400, "items 不能为空".to_string())));
    }

    let mut normalized: Vec<FileOperationItem> = Vec::with_capacity(request.items.len());
    for raw in &request.items {
        match normalize_copy_move_item(raw) {
            Ok(item) => normalized.push(item),
            Err(msg) => return Ok(Json(ApiResponse::error(400, msg))),
        }
    }

    // move 专属校验：
    //   1) 不允许把目录移到自身或自身子目录（百度 API 会返回不可恢复错误）
    //   2) 不允许目标父目录 == 源父目录（同目录改名走 rename 接口，move 通道不应承担）
    for item in &normalized {
        if let Err(msg) = ensure_not_move_into_self(&item.path, &item.dest) {
            return Ok(Json(ApiResponse::error(400, msg)));
        }
        if let Err(msg) = ensure_move_not_same_parent(&item.path, &item.dest) {
            return Ok(Json(ApiResponse::error(400, msg)));
        }
    }

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    let outcome = match client.move_files(&normalized).await {
        Ok(o) => o,
        Err(e) => {
            error!("移动文件请求失败: {}", e);
            return Ok(Json(ApiResponse::error(
                500,
                format!("移动文件失败: {}", e),
            )));
        }
    };

    let final_outcome = if let FileOperationOutcome::Failed { ref payload, .. } = outcome {
        if should_retry_after_warmup(payload) {
            warn!(
                "移动文件遇到会话过期类错误（errno={:?}, task_errno={:?}），触发 warmup 重试",
                payload.errno, payload.task_errno
            );
            match state.trigger_warmup().await {
                Ok(true) => match state.active_client().await {
                    Some(c2) => match c2.move_files(&normalized).await {
                        Ok(o2) => o2,
                        Err(retry_err) => {
                            error!("warmup 重试后仍失败: {}", retry_err);
                            outcome
                        }
                    },
                    None => outcome,
                },
                Ok(false) => {
                    warn!("warmup 跳过（用户未登录）");
                    outcome
                }
                Err(warmup_err) => {
                    error!("warmup 失败: {}", warmup_err);
                    outcome
                }
            }
        } else {
            outcome
        }
    } else {
        outcome
    };

    Ok(Json(ApiResponse::success(final_outcome.into())))
}

/// 重命名单个文件
///
/// POST /api/v1/files/rename
/// Body: { "item": {"path":"/a/x.mp4","newname":"y.mp4","id":1234} }
pub async fn rename_file(
    State(state): State<AppState>,
    Json(request): Json<RenameFileRequest>,
) -> Result<Json<ApiResponse<FileOperationOutcomeDto>>, StatusCode> {
    info!(
        "API: 重命名 path={} newname={} id={}",
        request.item.path, request.item.newname, request.item.id
    );

    // 归一化 + 双层加密保护 + 文件名校验
    let path = match normalize_path(&request.item.path) {
        Ok(p) => p,
        Err(msg) => return Ok(Json(ApiResponse::error(400, msg))),
    };
    if is_root(&path) {
        return Ok(Json(ApiResponse::error(
            400,
            "源路径不能为根目录".to_string(),
        )));
    }
    // 拒绝 fs_id == 0：百度 filemanager rename 接口对 id=0 的行为不可预期，提前拦截
    if request.item.id == 0 {
        return Ok(Json(ApiResponse::error(400, "fs_id 不能为 0".to_string())));
    }
    if let Err(msg) = crate::netdisk::client::validate_filename(&request.item.newname) {
        return Ok(Json(ApiResponse::error(400, msg)));
    }
    let src_leaf = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or("");
    if let Err(msg) = ensure_rename_not_encrypted(src_leaf, &request.item.newname) {
        return Ok(Json(ApiResponse::error(400, msg)));
    }

    let item = RenameItem {
        path,
        newname: request.item.newname.clone(),
        id: request.item.id,
    };

    // 多账号路由：按 active_uid 取客户端
    let client = match state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 首次调用：传 clone 副本，保留 owned `item` 用于潜在 warmup 重试
    let outcome = match client.rename_file(item.clone()).await {
        Ok(o) => o,
        Err(e) => {
            error!("重命名请求失败: {}", e);
            return Ok(Json(ApiResponse::error(500, format!("重命名失败: {}", e))));
        }
    };

    let final_outcome = if let FileOperationOutcome::Failed { ref payload, .. } = outcome {
        if should_retry_after_warmup(payload) {
            warn!(
                "重命名遇到会话过期类错误（errno={:?}, task_errno={:?}），触发 warmup 重试",
                payload.errno, payload.task_errno
            );
            match state.trigger_warmup().await {
                Ok(true) => {
                    match state.active_client().await {
                        // 重试用 owned item（首次传的是克隆，item 仍在作用域内）
                        Some(c2) => match c2.rename_file(item).await {
                            Ok(o2) => o2,
                            Err(retry_err) => {
                                error!("warmup 重试后仍失败: {}", retry_err);
                                outcome
                            }
                        },
                        None => outcome,
                    }
                }
                Ok(false) => {
                    warn!("warmup 跳过（用户未登录）");
                    outcome
                }
                Err(warmup_err) => {
                    error!("warmup 失败: {}", warmup_err);
                    outcome
                }
            }
        } else {
            outcome
        }
    } else {
        outcome
    };

    Ok(Json(ApiResponse::success(final_outcome.into())))
}

#[cfg(test)]
mod filemanager_validation_tests {
    use super::*;

    #[test]
    fn test_parent_dir_of_root_child() {
        assert_eq!(parent_dir_of("/a"), "/");
        assert_eq!(parent_dir_of("/x.txt"), "/");
    }

    #[test]
    fn test_parent_dir_of_nested() {
        assert_eq!(parent_dir_of("/a/b/c.mp4"), "/a/b");
        assert_eq!(parent_dir_of("/a/b"), "/a");
    }

    #[test]
    fn test_parent_dir_of_with_trailing_slash() {
        // 调用方应已 normalize 去尾斜杠，但 parent_dir_of 自身也对尾斜杠鲁棒
        assert_eq!(parent_dir_of("/a/b/"), "/a");
    }

    #[test]
    fn test_basename_of_simple() {
        assert_eq!(basename_of("/a/b/c.mp4"), "c.mp4");
        assert_eq!(basename_of("/x.txt"), "x.txt");
    }

    #[test]
    fn test_basename_of_with_trailing_slash() {
        assert_eq!(basename_of("/a/b/"), "b");
    }

    #[test]
    fn test_ensure_move_not_same_parent_same_name_rejected() {
        // /a/x.txt → 移动到 /a：no-op，必须拒绝
        let err = ensure_move_not_same_parent("/a/x.txt", "/a").unwrap_err();
        assert!(
            err.contains("目标目录与源父目录相同"),
            "应拒绝同父目录: {}",
            err
        );
    }

    #[test]
    fn test_ensure_move_not_same_parent_diff_name_also_rejected() {
        // /a/x.txt → 移动到 /a 但 newname=y.txt：语义上是 rename，必须拒绝
        // （文档要求：同目录改名走 rename 接口，move 通道不应承担）
        let err = ensure_move_not_same_parent("/a/x.txt", "/a").unwrap_err();
        assert!(
            err.contains("重命名接口"),
            "同父目录即使改名也应拒绝并提示走 rename: {}",
            err
        );
    }

    #[test]
    fn test_ensure_move_not_same_parent_root_child_rejected() {
        // /x.txt → 移动到 /：根目录同父
        let err = ensure_move_not_same_parent("/x.txt", "/").unwrap_err();
        assert!(
            err.contains("目标目录与源父目录相同"),
            "应拒绝根目录同父: {}",
            err
        );
    }

    #[test]
    fn test_ensure_move_not_same_parent_diff_dest_allowed() {
        // 不同父目录：允许
        assert!(ensure_move_not_same_parent("/a/x.txt", "/b").is_ok());
        // 注意：移到自身子目录由 ensure_not_move_into_self 处理；这里只校验"不同父目录"
        assert!(ensure_move_not_same_parent("/a/x.txt", "/a/sub").is_ok());
    }

    #[test]
    fn test_ensure_move_not_same_parent_dest_empty_treated_as_root() {
        // 容错：空 dest 视为根
        let err = ensure_move_not_same_parent("/x.txt", "").unwrap_err();
        assert!(
            err.contains("目标目录与源父目录相同"),
            "空 dest 应视为根: {}",
            err
        );
    }

    #[test]
    fn test_normalize_copy_move_item_no_longer_blocks_move_into_self() {
        // 拆分后：normalize_copy_move_item 不再含 move-only 校验。
        // 复制文件夹到自身子目录，对 copy 是合法的（百度 web 端允许递归复制）。
        let item = FileOperationItem {
            path: "/a/folder".into(),
            dest: "/a/folder/sub".into(),
            newname: "folder".into(),
        };
        let result = normalize_copy_move_item(&item);
        assert!(result.is_ok(), "copy 路径不应该被 move 校验拦截");
    }

    #[test]
    fn test_ensure_not_move_into_self_still_works_for_move_handler() {
        // ensure_not_move_into_self 单独可用，move handler 显式调用
        assert!(ensure_not_move_into_self("/a/folder", "/a/folder").is_err());
        assert!(ensure_not_move_into_self("/a/folder", "/a/folder/sub").is_err());
        assert!(ensure_not_move_into_self("/a/folder", "/b").is_ok());
    }

    #[test]
    fn test_ensure_copy_not_same_parent_same_name_rejected() {
        // /a/x.txt → 复制到 /a 且 newname=x.txt：同名 copy，百度 errno=-8
        let err = ensure_copy_not_same_parent_same_name("/a/x.txt", "/a", "x.txt").unwrap_err();
        assert!(
            err.contains("目标目录与源父目录相同且未改名"),
            "应拒绝同父 + 同名 copy: {}",
            err
        );
    }

    #[test]
    fn test_ensure_copy_not_same_parent_same_name_root_rejected() {
        let err = ensure_copy_not_same_parent_same_name("/x.txt", "/", "x.txt").unwrap_err();
        assert!(
            err.contains("目标目录与源父目录相同且未改名"),
            "根目录同父 + 同名 copy 应拒绝: {}",
            err
        );
    }

    #[test]
    fn test_ensure_copy_not_same_parent_diff_name_allowed() {
        // 同父目录 + 改名：copy 保留副本创建语义，允许
        assert!(
            ensure_copy_not_same_parent_same_name("/a/x.txt", "/a", "x_copy.txt").is_ok(),
            "同父 + 改名 copy 应允许（副本创建）"
        );
    }

    #[test]
    fn test_ensure_copy_not_same_parent_diff_dest_allowed() {
        // 不同父目录：永远允许
        assert!(ensure_copy_not_same_parent_same_name("/a/x.txt", "/b", "x.txt").is_ok());
        // 复制到自身子目录也允许（与 move 不同）
        assert!(
            ensure_copy_not_same_parent_same_name("/a/folder", "/a/folder/sub", "folder").is_ok(),
            "copy 允许复制到源文件夹自身子目录"
        );
    }

    #[test]
    fn test_ensure_copy_not_same_parent_dest_empty_treated_as_root() {
        let err = ensure_copy_not_same_parent_same_name("/x.txt", "", "x.txt").unwrap_err();
        assert!(
            err.contains("目标目录与源父目录相同且未改名"),
            "空 dest 应视为根: {}",
            err
        );
    }
}
