//! 分享同步 API 处理器（/api/v1/share-sync/*）

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

use crate::server::error::ApiError;
use crate::server::AppState;
use crate::share_sync::{
    normalize_pagination, CreateShareSubscriptionRequest, RunItemRecord, RunRecord,
    ShareSubscription, ShareSyncError, ShareSyncManager, UpdateShareSubscriptionRequest,
};

pub type ApiResult<T> = Result<T, ApiError>;

/// 统一响应体（与 transfer/autobackup 一致）
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data,
            error: None,
        }
    }
}

/// 获取 ShareSyncManager
async fn get_manager(state: &AppState) -> Result<Arc<ShareSyncManager>, ApiError> {
    let g = state.share_sync_manager.read().await;
    match &*g {
        Some(m) => Ok(Arc::clone(m)),
        None => Err(ApiError::Internal(anyhow::anyhow!(
            "ShareSyncManager 未初始化"
        ))),
    }
}

fn err_internal(e: &str) -> ApiError {
    ApiError::Internal(anyhow::anyhow!("{}", e))
}

fn err_bad(e: &str) -> ApiError {
    ApiError::BadRequest(e.to_string())
}

fn err_not_found(e: &str) -> ApiError {
    ApiError::NotFound(e.to_string())
}

fn map_share_err(e: ShareSyncError) -> ApiError {
    match e {
        ShareSyncError::SubscriptionNotFound(m) => err_not_found(&m),
        ShareSyncError::SubscriptionExists(m) => ApiError::Conflict(m),
        ShareSyncError::ConfigError(m) => err_bad(&m),
        ShareSyncError::ShareLinkError(m) => err_bad(&m),
        other => err_internal(&other.to_string()),
    }
}

// =====================================================
// Routes
// =====================================================

/// GET /api/v1/share-sync/subscriptions
pub async fn list_subscriptions(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<Vec<ShareSubscription>>>> {
    let m = get_manager(&state).await?;
    let list = m.list_subscriptions();
    Ok(Json(ApiResponse::success(list)))
}

/// GET /api/v1/share-sync/subscriptions/:id
pub async fn get_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<ShareSubscription>>> {
    let m = get_manager(&state).await?;
    match m.get_subscription(&id) {
        Some(s) => Ok(Json(ApiResponse::success(s))),
        None => Err(err_not_found("订阅不存在")),
    }
}

/// POST /api/v1/share-sync/subscriptions
pub async fn create_subscription(
    State(state): State<AppState>,
    Json(req): Json<CreateShareSubscriptionRequest>,
) -> ApiResult<Json<ApiResponse<ShareSubscription>>> {
    let m = get_manager(&state).await?;
    let sub = req.into_subscription();
    match m.create_subscription(sub) {
        Ok(s) => Ok(Json(ApiResponse::success(s))),
        Err(e) => Err(map_share_err(e)),
    }
}

/// PUT /api/v1/share-sync/subscriptions/:id
pub async fn update_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateShareSubscriptionRequest>,
) -> ApiResult<Json<ApiResponse<ShareSubscription>>> {
    let m = get_manager(&state).await?;
    let existing = m.get_subscription(&id).ok_or_else(|| err_not_found("订阅不存在"))?;
    let mut new_sub = existing.clone();
    if let Some(v) = req.name {
        new_sub.name = v;
    }
    if let Some(v) = req.share_url {
        new_sub.share_url = v;
    }
    if let Some(v) = req.password {
        new_sub.password = v;
    }
    if let Some(v) = req.include_paths {
        new_sub.include_paths = v;
    }
    if let Some(v) = req.exclude_patterns {
        new_sub.exclude_patterns = v;
    }
    if let Some(v) = req.targets {
        new_sub.targets = v;
    }
    if let Some(v) = req.conflict_strategy {
        new_sub.conflict_strategy = v;
    }
    if let Some(v) = req.delete_missing {
        new_sub.delete_missing = v;
    }
    if let Some(v) = req.poll_config {
        new_sub.poll_config = v;
    }
    if let Some(v) = req.enabled {
        new_sub.enabled = v;
    }
    match m.update_subscription(&id, new_sub) {
        Ok(s) => Ok(Json(ApiResponse::success(s))),
        Err(e) => Err(map_share_err(e)),
    }
}

/// DELETE /api/v1/share-sync/subscriptions/:id
pub async fn delete_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    m.delete_subscription(&id).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(serde_json::json!({"deleted": id}))))
}

/// POST /api/v1/share-sync/subscriptions/:id/enable
pub async fn enable_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    m.set_enabled(&id, true).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(serde_json::json!({"enabled": true}))))
}

/// POST /api/v1/share-sync/subscriptions/:id/disable
pub async fn disable_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    m.set_enabled(&id, false).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(serde_json::json!({"enabled": false}))))
}

/// POST /api/v1/share-sync/subscriptions/:id/trigger
pub async fn trigger_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    m.trigger_one(&id).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(serde_json::json!({"subscription_id": id, "triggered": true}))))
}

#[derive(Debug, Deserialize)]
pub struct RunsQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

/// GET /api/v1/share-sync/subscriptions/:id/runs
pub async fn list_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<RunsQuery>,
) -> ApiResult<Json<ApiResponse<Vec<RunRecord>>>> {
    let m = get_manager(&state).await?;
    let (page, ps) = normalize_pagination(q.page, q.page_size);
    match m.persistence().list_runs(&id, page, ps) {
        Ok(list) => Ok(Json(ApiResponse::success(list))),
        Err(e) => {
            error!("list_runs 失败: {}", e);
            Err(err_internal(&e.to_string()))
        }
    }
}

/// GET /api/v1/share-sync/runs/:id
pub async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<RunDetailDto>>> {
    let m = get_manager(&state).await?;
    let rec = m
        .persistence()
        .get_run(&id)
        .map_err(|e| err_internal(&e.to_string()))?
        .ok_or_else(|| err_not_found("运行记录不存在"))?;
    let items = m
        .persistence()
        .list_run_items(&id)
        .map_err(|e| err_internal(&e.to_string()))?;
    Ok(Json(ApiResponse::success(RunDetailDto {
        run: rec,
        items,
    })))
}

#[derive(Debug, Serialize)]
pub struct RunDetailDto {
    #[serde(flatten)]
    pub run: RunRecord,
    pub items: Vec<RunItemRecord>,
}

/// GET /api/v1/share-sync/subscriptions/:id/snapshots/latest
pub async fn latest_snapshot(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    match m
        .persistence()
        .latest_snapshot(&id)
        .map_err(|e| err_internal(&e.to_string()))?
    {
        Some(snap) => Ok(Json(ApiResponse::success(serde_json::to_value(&snap).unwrap()))),
        None => Err(err_not_found("该订阅暂无快照")),
    }
}

#[allow(dead_code)]
fn _suppress_unused_status() -> StatusCode {
    StatusCode::OK
}
