use crate::filesystem::{FilesystemConfig, PathGuard};
use crate::server::error::{ApiError, ApiResult};
use crate::server::extractors::{resolve_uid_from_query, UidQuery};
use crate::server::AppState;
use crate::uploader::{ScanOptions, ScanTaskStatus, UploadConflictStrategy, UploadTask};
use axum::{
    extract::{Path, Query, State},
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
    /// 显式指定 owner_uid
    ///
    /// **字段名兼容**：
    /// 同时接受 `uid` 和 `owner_uid` 两个字段名。前端 `upload.ts` 传 `owner_uid`。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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
    /// 显式指定 owner_uid
    ///
    /// **字段名兼容**：
    /// 同时接受 `uid` 和 `owner_uid` 两个字段名。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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
            allowed_paths: vec![],
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
    /// 显式指定 owner_uid
    ///
    /// **字段名兼容**：
    /// 同时接受 `uid` 和 `owner_uid` 两个字段名。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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
    /// 扫描归属账号
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_uid: Option<u64>,
}

/// POST /api/v1/uploads
/// 创建单文件上传任务
///
/// 多账号路由：
/// - `req.uid`（或 `owner_uid` 别名）= Some(uid) → 路由到对应账号 manager，
///   纠正 task.owner_uid 为 uid；无此账号返回 400。
/// - `req.uid = None` → 回退到当前活跃账号。
pub async fn create_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateUploadRequest>,
) -> ApiResult<Json<ApiResponse<String>>> {
    // 🔥 effective_uid = req.uid OR active
    let explicit_uid = req.uid.map(crate::auth::Uid::new);
    let effective_uid = match explicit_uid {
        Some(uid) => uid,
        None => (*app_state.active_uid.read().await)
            .ok_or_else(|| ApiError::Unauthorized("无活跃账号".to_string()))?,
    };
    let upload_manager = app_state
        .upload_manager_for(effective_uid)
        .ok_or_else(|| ApiError::BadRequest(format!("账号 uid={} 不存在", effective_uid.raw())))?;

    let config = app_state.config.read().await;
    let guard = create_path_guard(&config.filesystem);

    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = req
        .conflict_strategy
        .or(Some(config.conflict_strategy.default_upload_strategy));

    let local_path = validate_upload_file_path(&guard, &req.local_path)?;
    drop(config);

    // 🔥 传递 encrypt 参数，普通文件上传 is_folder_upload = false
    // 用 create_task_with_owner 在创建瞬间
    // 写入 effective_uid，task / .meta / Created event 三处一致，不再依赖事后 override。
    match upload_manager
        .create_task_with_owner(
            local_path,
            req.remote_path,
            req.encrypt,
            false,
            conflict_strategy,
            effective_uid,
        )
        .await
    {
        Ok(task_id) => {
            info!(
                "创建上传任务成功: {} (encrypt={}, effective_uid={})",
                task_id,
                req.encrypt,
                effective_uid.raw()
            );

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
///
/// **多账号语义**：`ScanManager` 实例本身是共享单例，
/// 但每次扫描的归属（事件 owner_uid、上传子任务 owner_uid、checkpoint owner_uid）
/// 都按 `effective_uid = req.uid.or(active_uid)` 解析并显式传给
/// `start_scan_with_owner`。这样：
/// - 切换到 B 账号后发起扫描，所有衍生资源都正确落到 B 账号
/// - 显式 `req.uid` 不必再等于 active（之前因 ScanManager 单例 owner 限制只能 active）
/// - `req.uid = None` 时回退到 active；无 active 返回 401
/// - `req.uid = Some(uid)` 但该账号不存在 → 由 `effective_uid` 解析时拒绝
pub async fn create_folder_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateFolderUploadRequest>,
) -> ApiResult<Json<ApiResponse<ScanStartResponse>>> {
    // 解析 effective_uid（不再要求 == active）
    let explicit_uid = req.uid.map(crate::auth::Uid::new);
    let effective_uid = match explicit_uid {
        Some(uid) => {
            // 校验显式 uid 是已知账号
            let mgr = app_state.account_manager.lock().await;
            if mgr.get_user(uid).is_none() {
                return Err(ApiError::BadRequest(format!(
                    "账号 uid={} 不存在或已被删除",
                    uid.raw()
                )));
            }
            drop(mgr);
            uid
        }
        None => match *app_state.active_uid.read().await {
            Some(uid) => uid,
            None => return Err(ApiError::BadRequest(
                "无活跃账号，无法启动文件夹扫描".to_string(),
            )),
        },
    };

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

    // 当 enforce_allowlist_on_followed_symlinks 开启时，收集规范化后的白名单路径
    let symlink_allowed_paths = if config.filesystem.enforce_allowlist_on_followed_symlinks
        && !config.filesystem.allowed_paths.is_empty()
    {
        config
            .filesystem
            .allowed_paths
            .iter()
            .filter_map(|p| std::path::PathBuf::from(p).canonicalize().ok())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    drop(config);

    let scan_options = if let Some(opts) = req.scan_options {
        let mut opts: ScanOptions = opts.into();
        opts.allowed_paths = symlink_allowed_paths;
        Some(opts)
    } else {
        Some(ScanOptions {
            skip_hidden: skip_hidden_files,
            allowed_paths: symlink_allowed_paths,
            ..Default::default()
        })
    };

    // 用 _with_owner 入口显式传入 effective_uid，
    // 使扫描事件 / 上传子任务 / checkpoint 全部绑定到目标账号
    match scan_manager
        .start_scan_with_owner(
            local_folder,
            req.remote_folder,
            scan_options,
            req.encrypt,
            conflict_strategy,
            effective_uid,
        )
        .await
    {
        Ok(scan_task_id) => {
            info!(
                "文件夹扫描任务已启动: {} (encrypt={}, owner_uid={})",
                scan_task_id,
                req.encrypt,
                effective_uid.raw()
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
///
/// 多账号路由：与 create_upload 同语义。
pub async fn create_batch_upload(
    State(app_state): State<AppState>,
    Json(req): Json<CreateBatchUploadRequest>,
) -> ApiResult<Json<ApiResponse<Vec<String>>>> {
    // 🔥 effective_uid = req.uid OR active
    let explicit_uid = req.uid.map(crate::auth::Uid::new);
    let effective_uid = match explicit_uid {
        Some(uid) => uid,
        None => (*app_state.active_uid.read().await)
            .ok_or_else(|| ApiError::Unauthorized("无活跃账号".to_string()))?,
    };
    let upload_manager = app_state
        .upload_manager_for(effective_uid)
        .ok_or_else(|| ApiError::BadRequest(format!("账号 uid={} 不存在", effective_uid.raw())))?;

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
    // 用 create_batch_tasks_with_owner 在创建瞬间
    // 写入 effective_uid，避免事后纠错路径下 Created event 短暂带启动账号。
    match upload_manager
        .create_batch_tasks_with_owner(files, req.encrypt, conflict_strategy, effective_uid)
        .await
    {
        Ok(task_ids) => {
            info!(
                "批量创建上传任务成功: {} 个 (encrypt={}, effective_uid={})",
                task_ids.len(),
                req.encrypt,
                effective_uid.raw()
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

/// GET /api/v1/uploads?uid=
/// 获取所有上传任务
///
/// - `?uid=` 缺省 → 跨账号聚合
/// - `?uid=X` → 仅该账号
pub async fn get_all_uploads(
    State(app_state): State<AppState>,
    Query(q): Query<UidQuery>,
) -> Result<Json<ApiResponse<Vec<UploadTask>>>, StatusCode> {
    let filter_uid = resolve_uid_from_query(&q);
    let tasks = match filter_uid {
        // 🔥 共享 manager 必须按 owner_uid 过滤
        Some(uid) => match app_state.upload_manager_for(uid) {
            Some(um) => um
                .get_all_tasks()
                .await
                .into_iter()
                .filter(|t| t.owner_uid == uid)
                .collect(),
            None => Vec::new(),
        },
        None => {
            // 全局共享历史库会被每个账号的 get_all_tasks 各捞一遍，跨账号聚合时
            // 按 id 去重，避免同一历史任务因账号数 N 而重复出现 N 次。
            let mut all = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for (_uid, um) in app_state.list_upload_managers() {
                for t in um.get_all_tasks().await {
                    if seen.insert(t.id.clone()) {
                        all.push(t);
                    }
                }
            }
            all
        }
    };
    Ok(Json(ApiResponse::success(tasks)))
}

/// GET /api/v1/uploads/:id
/// 获取指定上传任务（多账号路由）
pub async fn get_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<UploadTask>>, StatusCode> {
    let (_owner_uid, upload_manager) = app_state
        .find_upload_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    match upload_manager.get_task(&task_id).await {
        Some(task) => Ok(Json(ApiResponse::success(task))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/v1/uploads/:id/pause
/// 暂停上传任务（多账号路由）
pub async fn pause_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let (_owner_uid, upload_manager) = app_state
        .find_upload_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // 历史/终态任务（仅存于全局历史库、不在内存）不支持暂停：路由命中但内存无此
    // 任务 → 返回 409，而非把 `pause_task` 的"任务不存在"误报成 500。
    if !upload_manager.has_task_in_memory(&task_id) {
        return Err(StatusCode::CONFLICT);
    }

    // skip_try_start_waiting = false，正常暂停行为（暂停后尝试启动等待队列中的任务）
    match upload_manager.pause_task(&task_id, false).await {
        Ok(()) => {
            info!("暂停上传任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("已暂停".to_string())))
        }
        // `pause_task` 仅在两种情况返回 Err：①任务不存在(race，已被上面的内存守卫挡)
        // ②任务状态不支持暂停(终态/已完成等仍在内存、未 clear)。两者都是状态冲突而非
        // 内部错误，统一返回 409，避免「终态但仍在内存」任务被误报成 500。
        Err(e) => {
            error!("暂停上传任务失败(状态冲突): {:?}", e);
            Err(StatusCode::CONFLICT)
        }
    }
}

/// POST /api/v1/uploads/:id/resume
/// 恢复上传任务（多账号路由）
pub async fn resume_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let (_owner_uid, upload_manager) = app_state
        .find_upload_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // 历史/终态任务（仅存于全局历史库、不在内存）不支持恢复 → 返回 409。
    if !upload_manager.has_task_in_memory(&task_id) {
        return Err(StatusCode::CONFLICT);
    }

    match upload_manager.resume_task(&task_id).await {
        Ok(()) => {
            info!("恢复上传任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("已恢复".to_string())))
        }
        // `resume_task` 仅在两种情况返回 Err：①任务不存在(race，已被上面的内存守卫挡)
        // ②任务非 Paused/Failed(终态/已完成等仍在内存、未 clear)。两者都是状态冲突而非
        // 内部错误，统一返回 409，避免「终态但仍在内存」任务被误报成 200+error。
        Err(e) => {
            error!("恢复上传任务失败(状态冲突): {:?}", e);
            Err(StatusCode::CONFLICT)
        }
    }
}

/// DELETE /api/v1/uploads/:id
/// 删除上传任务（多账号路由）
pub async fn delete_upload(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let (_owner_uid, upload_manager) = app_state
        .find_upload_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

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
    axum::extract::Query(query): axum::extract::Query<UploadClearScopeQuery>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    // 按 effective_uid 过滤
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, query.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let upload_manager = app_state
        .upload_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = upload_manager.clear_completed_for_uid(uid).await;
    info!("清除了 {} 个已完成的上传任务（owner_uid={}）", count, uid.raw());
    Ok(Json(ApiResponse::success(count)))
}

/// POST /api/v1/uploads/clear-failed
/// 清除失败的上传任务
pub async fn clear_failed_uploads(
    State(app_state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<UploadClearScopeQuery>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, query.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let upload_manager = app_state
        .upload_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = upload_manager.clear_failed_for_uid(uid).await;
    info!("清除了 {} 个失败的上传任务（owner_uid={}）", count, uid.raw());
    Ok(Json(ApiResponse::success(count)))
}

/// upload clear 路由的 owner_uid 过滤参数
#[derive(Debug, serde::Deserialize)]
pub struct UploadClearScopeQuery {
    /// 显式指定 owner_uid；缺省时使用当前 active_uid
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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
                owner_uid: info.owner_uid.map(|u| u.raw()),
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
    // 所有路径都强制 owner_uid 过滤
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, req.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let mgr = app_state
        .upload_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (allowed_ids, denied_pairs) = if req.all == Some(true) {
        (mgr.get_pausable_task_ids_for_uid(uid).await, Vec::new())
    } else {
        let raw_ids = req.task_ids.unwrap_or_default();
        mgr.validate_task_ids_for_uid(uid, &raw_ids).await
    };

    let raw = mgr.batch_pause(&allowed_ids).await;
    let mut results: Vec<BatchOperationItem> = raw
        .into_iter()
        .map(|(id, ok, err)| BatchOperationItem {
            task_id: id,
            success: ok,
            error: err,
        })
        .collect();
    results.extend(denied_pairs.into_iter().map(|(id, reason)| BatchOperationItem {
        task_id: id,
        success: false,
        error: Some(reason),
    }));
    Ok(Json(ApiResponse::success(
        BatchOperationResponse::from_results(results),
    )))
}

/// POST /api/v1/uploads/batch/resume
pub async fn batch_resume_uploads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, req.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let mgr = app_state
        .upload_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (allowed_ids, denied_pairs) = if req.all == Some(true) {
        (mgr.get_resumable_task_ids_for_uid(uid).await, Vec::new())
    } else {
        let raw_ids = req.task_ids.unwrap_or_default();
        mgr.validate_task_ids_for_uid(uid, &raw_ids).await
    };

    let raw = mgr.batch_resume(&allowed_ids).await;
    let mut results: Vec<BatchOperationItem> = raw
        .into_iter()
        .map(|(id, ok, err)| BatchOperationItem {
            task_id: id,
            success: ok,
            error: err,
        })
        .collect();
    results.extend(denied_pairs.into_iter().map(|(id, reason)| BatchOperationItem {
        task_id: id,
        success: false,
        error: Some(reason),
    }));
    Ok(Json(ApiResponse::success(
        BatchOperationResponse::from_results(results),
    )))
}

/// POST /api/v1/uploads/batch/delete
pub async fn batch_delete_uploads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, req.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let mgr = app_state
        .upload_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (allowed_ids, denied_pairs) = if req.all == Some(true) {
        (mgr.get_all_task_ids_for_uid(uid).await, Vec::new())
    } else {
        let raw_ids = req.task_ids.unwrap_or_default();
        mgr.validate_task_ids_for_uid(uid, &raw_ids).await
    };

    let raw = mgr.batch_delete(&allowed_ids).await;
    let mut results: Vec<BatchOperationItem> = raw
        .into_iter()
        .map(|(id, ok, err)| BatchOperationItem {
            task_id: id,
            success: ok,
            error: err,
        })
        .collect();
    results.extend(denied_pairs.into_iter().map(|(id, reason)| BatchOperationItem {
        task_id: id,
        success: false,
        error: Some(reason),
    }));
    Ok(Json(ApiResponse::success(
        BatchOperationResponse::from_results(results),
    )))
}
