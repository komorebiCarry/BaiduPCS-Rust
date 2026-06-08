use crate::downloader::{DownloadConflictStrategy, DownloadTask};
use crate::server::extractors::{resolve_uid_from_query, UidQuery};
use crate::server::AppState;
use axum::{
    extract::{Path, Query, State},
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
    /// 冲突策略（可选，未指定则使用默认值）
    #[serde(default)]
    pub conflict_strategy: Option<DownloadConflictStrategy>,
    /// 显式指定 owner_uid（多账号场景）
    ///
    /// 未指定时回退到 `active_uid`；两者都缺失返回 400 `account_not_available`。
    ///
    /// **字段名兼容**：同时接受 `uid` 和 `owner_uid` 两个字段名，前端类型与后端 DTO
    /// 不一致时 serde alias 会自动反序列化。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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
    /// 冲突策略（可选，未指定则使用默认值）
    #[serde(default)]
    pub conflict_strategy: Option<DownloadConflictStrategy>,
    /// 显式指定 owner_uid
    ///
    /// **字段名兼容**：同时接受 `uid` 和 `owner_uid` 两个字段名。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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
///
/// 多账号路由：
/// - 解析 `effective_uid = req.uid.or(active_uid)`，**始终** 用 effective_uid
///   override task.owner_uid。当前架构是 per-uid 独立 manager，每个账号有自己的
///   `DownloadManager` 实例与 `manager.owner_uid`；显式 override 是为了让历史 fallback /
///   测试路径下若 manager 被多账号共用时仍能正确归属任务。
/// - `req.uid = Some(uid)` 但 uid 不存在 → 400 BadRequest
/// - 全无 active 且 req.uid 为 None → 500（兜底）
pub async fn create_download(
    State(app_state): State<AppState>,
    Json(req): Json<CreateDownloadRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    // 解析显式 owner_uid（缺省回退 active）
    let explicit_uid = req.uid.map(crate::auth::Uid::new);
    let effective_uid = match explicit_uid {
        Some(uid) => uid,
        None => match *app_state.active_uid.read().await {
            Some(uid) => uid,
            None => return Err(StatusCode::UNAUTHORIZED),
        },
    };

    // 路由到对应 manager（缺省走 effective_uid，等价 download_manager_for_active）
    let download_manager = app_state
        .download_manager_for(effective_uid)
        .ok_or(StatusCode::BAD_REQUEST)?;

    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = match req.conflict_strategy {
        Some(s) => Some(s),
        None => {
            let config = app_state.config.read().await;
            Some(config.conflict_strategy.default_download_strategy)
        }
    };

    // 🔥 用 _with_owner 入口，task / 持久化 / Created event 在创建瞬间就用
    // effective_uid，根除事后 override 与 Created event / async execution 竞态。
    match download_manager
        .create_task_with_owner(
            req.fs_id,
            req.remote_path,
            req.filename,
            req.total_size,
            conflict_strategy,
            effective_uid,
        )
        .await
    {
        Ok(task_id) => {
            // 🔥 P2-2 修复：检查是否为跳过的任务
            if task_id == "skipped" {
                info!("文件已存在，已跳过下载");
                return Ok(Json(ApiResponse::success_with_message(
                    task_id,
                    "文件已存在，已跳过"
                )));
            }

            info!("创建下载任务成功: {} (effective_uid={})", task_id, effective_uid.raw());

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

/// GET /api/v1/downloads?uid=
/// 获取所有下载任务
///
/// - `?uid=` 缺省 → 跨账号聚合（迭代 `list_download_managers`）
/// - `?uid=X` → 仅该账号
pub async fn get_all_downloads(
    State(app_state): State<AppState>,
    Query(q): Query<UidQuery>,
) -> Result<Json<ApiResponse<Vec<DownloadTask>>>, StatusCode> {
    let filter_uid = resolve_uid_from_query(&q);
    let tasks = match filter_uid {
        // 🔥 共享 manager 设计下 get_all_tasks 返回所有账号任务，
        // 必须按 owner_uid 严格过滤才符合 `?uid=X` 隔离契约。
        Some(uid) => match app_state.download_manager_for(uid) {
            Some(dm) => dm
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
            for (_uid, dm) in app_state.list_download_managers() {
                for t in dm.get_all_tasks().await {
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

/// GET /api/v1/downloads/active?uid=
/// 🔥 获取活跃的下载任务（用于降级轮询）
pub async fn get_active_downloads(
    State(app_state): State<AppState>,
    Query(q): Query<UidQuery>,
) -> Result<Json<ApiResponse<Vec<DownloadTask>>>, StatusCode> {
    let filter_uid = resolve_uid_from_query(&q);
    let raw_tasks: Vec<DownloadTask> = match filter_uid {
        // 🔥 共享 manager 必须按 owner_uid 过滤
        Some(uid) => match app_state.download_manager_for(uid) {
            Some(dm) => dm
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
            for (_uid, dm) in app_state.list_download_managers() {
                for t in dm.get_all_tasks().await {
                    if seen.insert(t.id.clone()) {
                        all.push(t);
                    }
                }
            }
            all
        }
    };
    let tasks: Vec<DownloadTask> = raw_tasks
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
///
/// 多账号路由：通过 task_id 反查归属 manager，
/// 不再假设任务必定归属当前活跃账号。在 per-uid 独立 manager 架构下，每个 uid 池内
/// 独立查找，命中即返回。
pub async fn get_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<DownloadTask>>, StatusCode> {
    let (_owner_uid, download_manager) = app_state
        .find_download_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    match download_manager.get_task(&task_id).await {
        Some(task) => Ok(Json(ApiResponse::success(task))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// POST /api/v1/downloads/:id/pause
/// 暂停下载任务（多账号路由）
pub async fn pause_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let (_owner_uid, download_manager) = app_state
        .find_download_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // 历史/终态任务（仅存于全局历史库、不在内存）不支持暂停：路由命中但内存无此
    // 任务，说明它是该账号的历史(已完成/已清理)任务 → 返回 409，而非把
    // `pause_task` 的"任务不存在"误报成 500。可暂停的任务（Pending/Downloading/
    // Paused/Failed）始终在内存中。
    if !download_manager.has_task_in_memory(&task_id).await {
        return Err(StatusCode::CONFLICT);
    }

    // 正常暂停场景，skip_try_start_waiting=false，允许启动等待队列任务
    match download_manager.pause_task(&task_id, false).await {
        Ok(_) => {
            info!("暂停下载任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("Task paused".to_string())))
        }
        // `pause_task` 仅在两种情况返回 Err：①任务不存在(race，已被上面的内存守卫挡)
        // ②任务非 Downloading(终态/已完成等仍在内存、未 clear)。两者都是状态冲突而非
        // 内部错误，统一返回 409，避免「终态但仍在内存」任务被误报成 500。
        Err(e) => {
            error!("暂停下载任务失败(状态冲突): {:?}", e);
            Err(StatusCode::CONFLICT)
        }
    }
}

/// POST /api/v1/downloads/:id/resume
/// 恢复下载任务（多账号路由）
pub async fn resume_download(
    State(app_state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let (_owner_uid, download_manager) = app_state
        .find_download_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // 历史/终态任务（仅存于全局历史库、不在内存）不支持恢复：路由命中但内存无此
    // 任务 → 返回 409。可恢复的任务（Paused/Failed）始终在内存中。
    if !download_manager.has_task_in_memory(&task_id).await {
        return Err(StatusCode::CONFLICT);
    }

    match download_manager.resume_task(&task_id).await {
        Ok(_) => {
            info!("恢复下载任务成功: {}", task_id);
            Ok(Json(ApiResponse::success("Task resumed".to_string())))
        }
        // `resume_task` 仅在两种情况返回 Err：①任务不存在(race，已被上面的内存守卫挡)
        // ②任务非 Paused/Failed(终态/已完成等仍在内存、未 clear)。两者都是状态冲突而非
        // 内部错误，统一返回 409，避免「终态但仍在内存」任务被误报成 200+error。
        Err(e) => {
            error!("恢复下载任务失败(状态冲突): {:?}", e);
            Err(StatusCode::CONFLICT)
        }
    }
}

/// DELETE /api/v1/downloads/:id
/// 删除下载任务（多账号路由）
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
    let (_owner_uid, download_manager) = app_state
        .find_download_manager_for_task(&task_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

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
    axum::extract::Query(query): axum::extract::Query<ClearScopeQuery>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    // 按 effective_uid 过滤，避免跨账号清理
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, query.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let download_manager = app_state
        .download_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = download_manager.clear_completed_for_uid(uid).await;
    Ok(Json(ApiResponse::success(count)))
}

/// DELETE /api/v1/downloads/clear/failed
/// 清除失败的任务
pub async fn clear_failed(
    State(app_state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ClearScopeQuery>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, query.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let download_manager = app_state
        .download_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let count = download_manager.clear_failed_for_uid(uid).await;
    Ok(Json(ApiResponse::success(count)))
}

/// clear 路由按 owner_uid 过滤的查询参数
#[derive(Debug, Deserialize)]
pub struct ClearScopeQuery {
    /// 显式指定 owner_uid；缺省时使用当前 active_uid
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
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

    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = match req.conflict_strategy {
        Some(s) => Some(s),
        None => {
            let config = app_state.config.read().await;
            Some(config.conflict_strategy.default_download_strategy)
        }
    };

    // 🔥 多账号路由：
    // - effective_uid = req.uid.or(active)，**始终** 用 effective_uid 纠正归属
    // - req.uid = Some(uid) 但 uid 不存在 → 400
    // - req.uid = None 且无 active → 401
    let explicit_uid = req.uid.map(crate::auth::Uid::new);
    let effective_uid: crate::auth::Uid = match explicit_uid {
        Some(uid) => uid,
        None => match *app_state.active_uid.read().await {
            Some(uid) => uid,
            None => return Err(StatusCode::UNAUTHORIZED),
        },
    };
    let download_manager = app_state
        .download_manager_for(effective_uid)
        .ok_or(StatusCode::BAD_REQUEST)?;

    let folder_download_manager = &app_state.folder_download_manager;
    // folder 任务归属用 effective_uid（已含 active 兜底）
    let owner_uid = effective_uid;

    let mut task_ids = Vec::new();
    let mut folder_task_ids = Vec::new();
    let mut failed = Vec::new();

    // 处理每个下载项
    for item in req.items {
        if item.is_dir {
            // 文件夹下载
            match folder_download_manager
                .create_folder_download_with_dir(item.path.clone(), &target_dir, item.original_name.clone(), conflict_strategy, owner_uid)
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

            // 🔥 用 _with_dir_and_owner 入口
            match download_manager
                .create_task_with_dir_and_owner(
                    item.fs_id,
                    item.path.clone(),
                    item.name.clone(),
                    file_size,
                    &target_dir,
                    conflict_strategy,
                    effective_uid,
                )
                .await
            {
                Ok(task_id) => {
                    // 检查是否为跳过标记
                    if task_id == "skipped" {
                        info!("跳过下载（文件已存在）: {}", item.path);
                        // 不添加到 task_ids，也不算失败
                        continue;
                    }

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

// ==================== 批量操作 ====================

use super::common::{BatchOperationRequest, BatchOperationItem, BatchOperationResponse};

/// POST /api/v1/downloads/batch/pause
pub async fn batch_pause_downloads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    // 所有路径都强制 owner_uid 过滤
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, req.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let mgr = app_state.download_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // 显式 task_ids 必须每条校验 owner_uid，不能直接下发到底层 manager
    let (allowed_ids, denied_pairs) = if req.all == Some(true) {
        (mgr.get_pausable_task_ids_for_uid(uid).await, Vec::new())
    } else {
        let raw_ids = req.task_ids.unwrap_or_default();
        mgr.validate_task_ids_for_uid(uid, &raw_ids).await
    };

    let raw = mgr.batch_pause(&allowed_ids).await;
    let mut results: Vec<BatchOperationItem> = raw.into_iter()
        .map(|(id, ok, err)| BatchOperationItem { task_id: id, success: ok, error: err })
        .collect();
    // 合并 denied 项（owner 不匹配 / 任务不存在 / 备份任务）
    results.extend(denied_pairs.into_iter().map(|(id, reason)| BatchOperationItem {
        task_id: id,
        success: false,
        error: Some(reason),
    }));
    Ok(Json(ApiResponse::success(BatchOperationResponse::from_results(results))))
}

/// POST /api/v1/downloads/batch/resume
pub async fn batch_resume_downloads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, req.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let mgr = app_state.download_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (allowed_ids, denied_pairs) = if req.all == Some(true) {
        (mgr.get_resumable_task_ids_for_uid(uid).await, Vec::new())
    } else {
        let raw_ids = req.task_ids.unwrap_or_default();
        mgr.validate_task_ids_for_uid(uid, &raw_ids).await
    };

    let raw = mgr.batch_resume(&allowed_ids).await;
    let mut results: Vec<BatchOperationItem> = raw.into_iter()
        .map(|(id, ok, err)| BatchOperationItem { task_id: id, success: ok, error: err })
        .collect();
    results.extend(denied_pairs.into_iter().map(|(id, reason)| BatchOperationItem {
        task_id: id,
        success: false,
        error: Some(reason),
    }));
    Ok(Json(ApiResponse::success(BatchOperationResponse::from_results(results))))
}

/// POST /api/v1/downloads/batch/delete
pub async fn batch_delete_downloads(
    State(app_state): State<AppState>,
    Json(req): Json<BatchOperationRequest>,
) -> Result<Json<ApiResponse<BatchOperationResponse>>, StatusCode> {
    let uid = match crate::server::helpers::resolve_batch_owner_uid(&app_state, req.uid).await {
        Some(u) => u,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let mgr = app_state.download_manager_for(uid)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let delete_files = req.delete_files.unwrap_or(false);
    let (allowed_ids, denied_pairs) = if req.all == Some(true) {
        (mgr.get_all_task_ids_for_uid(uid).await, Vec::new())
    } else {
        let raw_ids = req.task_ids.unwrap_or_default();
        mgr.validate_task_ids_for_uid(uid, &raw_ids).await
    };

    let raw = mgr.batch_delete(&allowed_ids, delete_files).await;
    let mut results: Vec<BatchOperationItem> = raw.into_iter()
        .map(|(id, ok, err)| BatchOperationItem { task_id: id, success: ok, error: err })
        .collect();
    results.extend(denied_pairs.into_iter().map(|(id, reason)| BatchOperationItem {
        task_id: id,
        success: false,
        error: Some(reason),
    }));
    Ok(Json(ApiResponse::success(BatchOperationResponse::from_results(results))))
}
