//! 离线下载（Cloud Download）API Handler
//!
//! 本模块提供离线下载功能的 HTTP API 接口，包括：
//! - 添加离线下载任务
//! - 查询任务列表
//! - 查询单个任务详情
//! - 取消任务
//! - 删除任务
//! - 清空任务记录
//! - 手动刷新任务列表

use crate::netdisk::cloud_dl::{
    AddTaskRequest, AddTaskResponse, AutoDownloadConfig, ClearTasksResponse, CloudDlTaskInfo,
    OperationResponse, TaskListResponse,
};
use crate::server::handlers::ApiResponse;
use crate::server::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use tracing::{error, info};

// =====================================================
// 添加任务
// =====================================================

/// 添加离线下载任务
///
/// POST /api/v1/cloud-dl/tasks
///
/// # 请求体
/// ```json
/// {
///     "source_url": "http://example.com/file.zip",
///     "save_path": "/downloads",
///     "auto_download": true,
///     "local_download_path": "/local/downloads",
///     "ask_download_path": false
/// }
/// ```
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": {
///         "task_id": 123456789
///     }
/// }
/// ```
pub async fn add_task(
    State(state): State<AppState>,
    Json(req): Json<AddTaskRequest>,
) -> Result<Json<ApiResponse<AddTaskResponse>>, StatusCode> {
    info!(
        "API: 添加离线下载任务 source_url={}, save_path={}, auto_download={}, local_download_path={:?}, ask_download_path={}, uid={:?}",
        req.source_url, req.save_path, req.auto_download, req.local_download_path, req.ask_download_path, req.uid
    );

    // 🔥 离线下载只允许在当前 active 账号下创建。
    // 离线下载是百度云端任务（非本地任务），跨账号创建会导致：
    //   - 创建后无法在创建账号下查询/取消（list/cancel/delete 走 active 路由）
    //   - 安全风险：账号 A 用户可向账号 B 注入云端任务
    // 因此强制：effective_uid = active_uid；前端显式传 owner_uid 时必须等于 active，否则 400。
    let active_uid = match *state.active_uid.read().await {
        Some(uid) => uid,
        None => {
            error!("add_task: 未登录");
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    if let Some(uid_raw) = req.uid {
        if uid_raw != active_uid.raw() {
            error!(
                "add_task: 拒绝跨账号创建离线任务 (req.uid={}, active_uid={})",
                uid_raw,
                active_uid.raw()
            );
            return Ok(Json(ApiResponse::error(
                400,
                format!(
                    "离线下载只能在当前账号下创建 (当前账号 uid={}, 请求 uid={})",
                    active_uid.raw(),
                    uid_raw
                ),
            )));
        }
    }

    let effective_uid = active_uid;

    // 多账号路由：按 effective_uid 取客户端（懒加载兜底）
    if let Err(e) = state.ensure_client_for_uid(effective_uid).await {
        error!("add_task: 懒加载 client 失败: uid={}, err={:?}", effective_uid.raw(), e);
        return Ok(Json(ApiResponse::error(
            500,
            format!("无法构造目标账号客户端: {}", e),
        )));
    }
    let client = match state.client_pool.read().await.get_client(effective_uid) {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用 API 添加任务
    match client.cloud_dl_add_task(&req.source_url, &req.save_path).await {
        Ok(task_id) => {
            info!("添加离线下载任务成功: task_id={}, owner_uid={}", task_id, effective_uid.raw());

            // 如果启用了自动下载，注册自动下载配置到监听服务
            if req.auto_download {
                let auto_config = AutoDownloadConfig::enabled(
                    task_id,
                    req.local_download_path.clone(),
                    req.ask_download_path,
                );

                // 获取离线下载监听服务并注册配置（按 effective_uid 路由，覆盖 active 同款语义）。
                //
                // 当 monitor 不存在时（新账号刚切换、还没订阅
                // CloudDL 等场景），先 lazy init 兜底再注册；如果初始化后仍然拿不到
                // monitor → 返回 500，避免静默丢配置。
                let monitor = match state.cloud_dl_monitor_for(effective_uid) {
                    Some(m) => Some(m),
                    None => {
                        info!(
                            "add_task: cloud_dl_monitor uid={} 不存在，先 lazy init 兜底再注册自动下载配置",
                            effective_uid.raw()
                        );
                        state.init_cloud_dl_monitor_for(effective_uid).await;
                        state.cloud_dl_monitor_for(effective_uid)
                    }
                };

                match monitor {
                    Some(monitor) => {
                        monitor.register_auto_download(task_id, auto_config).await;
                        info!(
                            "已注册自动下载配置: task_id={}, local_path={:?}, ask_each_time={}, uid={}",
                            task_id, req.local_download_path, req.ask_download_path, effective_uid.raw()
                        );
                    }
                    None => {
                        error!(
                            "add_task: 无法初始化离线下载监控 uid={}，自动下载配置丢失 (task_id={})",
                            effective_uid.raw(),
                            task_id
                        );
                        return Ok(Json(ApiResponse::error(
                            500,
                            format!(
                                "离线下载任务创建成功 (task_id={})，但自动下载监控初始化失败，请稍后重试自动下载注册",
                                task_id
                            ),
                        )));
                    }
                }
            }

            Ok(Json(ApiResponse::success(AddTaskResponse { task_id })))
        }
        Err(e) => {
            error!("添加离线下载任务失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("添加离线下载任务失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 查询任务列表
// =====================================================

/// 获取离线下载任务列表
///
/// GET /api/v1/cloud-dl/tasks
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": {
///         "tasks": [...]
///     }
/// }
/// ```
pub async fn list_tasks(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<TaskListResponse>>, StatusCode> {
    info!("API: 获取离线下载任务列表");

    // 多账号路由：按 active_uid 取客户端
    let active_uid = match *state.active_uid.read().await {
        Some(uid) => uid,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };
    let client = match state.client_pool.read().await.get_client(active_uid) {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用 API 获取任务列表
    match client.cloud_dl_list_task().await {
        Ok(mut tasks) => {
            // 🔥 在响应前 stamp
            // 当前 active_uid。**仅作为内部事件路由 / 跨账号防御 / 兼容字段**——
            // CloudDl 已收紧为 active-only，前端
            // CloudDlView 不再渲染 AccountBadge / 账号列。后续实现者**不**应基于
            // 此字段恢复账号 UI。百度 API 原始响应不带此字段，stamp 为 active 即可。
            for task in &mut tasks {
                task.owner_uid = Some(active_uid.raw());
            }
            info!("获取离线下载任务列表成功: {} 个任务 (uid={})", tasks.len(), active_uid.raw());
            Ok(Json(ApiResponse::success(TaskListResponse { tasks })))
        }
        Err(e) => {
            error!("获取离线下载任务列表失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("获取离线下载任务列表失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 查询单个任务
// =====================================================

/// 查询单个离线下载任务详情
///
/// GET /api/v1/cloud-dl/tasks/:task_id
///
/// # 路径参数
/// - `task_id`: 任务 ID
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": { ... }
/// }
/// ```
pub async fn query_task(
    State(state): State<AppState>,
    Path(task_id): Path<i64>,
) -> Result<Json<ApiResponse<CloudDlTaskInfo>>, StatusCode> {
    info!("API: 查询离线下载任务详情 task_id={}", task_id);

    // 多账号路由：按 active_uid 取客户端
    let active_uid = match *state.active_uid.read().await {
        Some(uid) => uid,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };
    let client = match state.client_pool.read().await.get_client(active_uid) {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 调用 API 查询任务详情
    match client.cloud_dl_query_task(&[task_id]).await {
        Ok(tasks) => {
            if let Some(mut task) = tasks.into_iter().next() {
                // 🔥 stamp active_uid（同 list_tasks）
                task.owner_uid = Some(active_uid.raw());
                info!("查询离线下载任务详情成功: task_id={} (uid={})", task_id, active_uid.raw());
                Ok(Json(ApiResponse::success(task)))
            } else {
                Ok(Json(ApiResponse::error(404, "任务不存在".to_string())))
            }
        }
        Err(e) => {
            error!("查询离线下载任务详情失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("查询离线下载任务详情失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 取消任务
// =====================================================

/// 取消离线下载任务
///
/// POST /api/v1/cloud-dl/tasks/:task_id/cancel
///
/// # 路径参数
/// - `task_id`: 任务 ID
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": {
///         "success": true
///     }
/// }
/// ```
pub async fn cancel_task(
    State(state): State<AppState>,
    Path(task_id): Path<i64>,
) -> Result<Json<ApiResponse<OperationResponse>>, StatusCode> {
    info!("API: 取消离线下载任务 task_id={}", task_id);

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

    // 调用 API 取消任务
    match client.cloud_dl_cancel_task(task_id).await {
        Ok(()) => {
            info!("取消离线下载任务成功: task_id={}", task_id);
            Ok(Json(ApiResponse::success(OperationResponse::success())))
        }
        Err(e) => {
            error!("取消离线下载任务失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("取消离线下载任务失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 删除任务
// =====================================================

/// 删除离线下载任务
///
/// DELETE /api/v1/cloud-dl/tasks/:task_id
///
/// # 路径参数
/// - `task_id`: 任务 ID
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": {
///         "success": true
///     }
/// }
/// ```
pub async fn delete_task(
    State(state): State<AppState>,
    Path(task_id): Path<i64>,
) -> Result<Json<ApiResponse<OperationResponse>>, StatusCode> {
    info!("API: 删除离线下载任务 task_id={}", task_id);

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

    // 调用 API 删除任务
    match client.cloud_dl_delete_task(task_id).await {
        Ok(()) => {
            info!("删除离线下载任务成功: task_id={}", task_id);
            Ok(Json(ApiResponse::success(OperationResponse::success())))
        }
        Err(e) => {
            error!("删除离线下载任务失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("删除离线下载任务失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 清空任务记录
// =====================================================

/// 清空离线下载任务记录
///
/// DELETE /api/v1/cloud-dl/tasks/clear
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": {
///         "total": 5
///     }
/// }
/// ```
pub async fn clear_tasks(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<ClearTasksResponse>>, StatusCode> {
    info!("API: 清空离线下载任务记录");

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

    // 调用 API 清空任务
    match client.cloud_dl_clear_task().await {
        Ok(total) => {
            info!("清空离线下载任务记录成功: total={}", total);
            Ok(Json(ApiResponse::success(ClearTasksResponse { total })))
        }
        Err(e) => {
            error!("清空离线下载任务记录失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("清空离线下载任务记录失败: {}", e),
            )))
        }
    }
}

// =====================================================
// 手动刷新
// =====================================================

/// 手动刷新离线下载任务列表
///
/// POST /api/v1/cloud-dl/tasks/refresh
///
/// 触发后台监听服务立即刷新任务列表，并通过 WebSocket 推送更新。
///
/// # 响应
/// ```json
/// {
///     "code": 0,
///     "message": "Success",
///     "data": {
///         "tasks": [...]
///     }
/// }
/// ```
pub async fn refresh_tasks(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<TaskListResponse>>, StatusCode> {
    info!("API: 手动刷新离线下载任务列表");

    // 多账号路由：按 active_uid 取客户端
    let active_uid = match *state.active_uid.read().await {
        Some(uid) => uid,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };
    let client = match state.client_pool.read().await.get_client(active_uid) {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };

    // 直接调用 API 获取最新任务列表
    // 注意：后续可以集成 CloudDlMonitor 的 trigger_refresh 方法
    match client.cloud_dl_list_task().await {
        Ok(mut tasks) => {
            // 🔥 stamp active_uid（同 list_tasks）
            for task in &mut tasks {
                task.owner_uid = Some(active_uid.raw());
            }
            info!(
                "手动刷新离线下载任务列表成功: {} 个任务 (uid={})",
                tasks.len(),
                active_uid.raw()
            );
            Ok(Json(ApiResponse::success(TaskListResponse { tasks })))
        }
        Err(e) => {
            error!("手动刷新离线下载任务列表失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("手动刷新离线下载任务列表失败: {}", e),
            )))
        }
    }
}
