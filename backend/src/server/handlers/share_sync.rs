//! 分享同步 API 处理器（/api/v1/share-sync/*）

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

use crate::server::error::ApiError;
use crate::server::AppState;
use crate::share_sync::{
    infer_share_root, normalize_pagination, normalize_share_path, CreateShareSubscriptionRequest, RunItemRecord, RunRecord,
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
        None => Err(ApiError::BadRequest(
            "ShareSyncManager 未初始化，请先登录并重试".to_string(),
        )),
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
        ShareSyncError::FileSystemError(m) => err_bad(&m),
        ShareSyncError::NetworkError(m) => err_bad(&format!("网络错误: {}", m)),
        ShareSyncError::PersistenceError(m) => {
            if m.contains("读取配置") || m.contains("保存配置") || m.contains("路径") || m.contains("目录") || m.contains("权限") {
                err_bad(&m)
            } else {
                err_internal(&m)
            }
        }
        ShareSyncError::TransferError(m) => err_bad(&m),
        ShareSyncError::DownloadError(m) => err_bad(&m),
        ShareSyncError::SchedulerError(m) => err_bad(&m),
        ShareSyncError::Internal(m) => err_internal(&m),
    }
}

fn ensure_subscription_exists(
    manager: &Arc<ShareSyncManager>,
    id: &str,
) -> ApiResult<()> {
    if manager.get_subscription(id).is_none() {
        return Err(err_not_found("订阅不存在"));
    }
    Ok(())
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
    ensure_subscription_exists(&m, &id)?;
    let (page, ps) = normalize_pagination(q.page, q.page_size);
    let list = m
        .persistence()
        .list_runs(&id, page, ps)
        .map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(list)))
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
        .map_err(map_share_err)?
        .ok_or_else(|| err_not_found("运行记录不存在"))?;
    let items = m
        .persistence()
        .list_run_items(&id)
        .map_err(map_share_err)?;
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
    ensure_subscription_exists(&m, &id)?;
    match m
        .persistence()
        .latest_snapshot(&id)
        .map_err(map_share_err)?
    {
        Some(snap) => Ok(Json(ApiResponse::success(
            serde_json::to_value(&snap)
                .map_err(|e| err_internal(&format!("快照序列化失败: {}", e)))?,
        ))),
        None => Err(err_not_found("该订阅暂无快照")),
    }
}

// =====================================================
// 预览分享目录树（用于前端选择 include_paths）
// =====================================================

#[derive(Debug, Deserialize)]
pub struct PreviewTreeRequest {
    pub share_url: String,
    #[serde(default)]
    pub password: Option<String>,
    /// 展开深度（1 = 仅根，2 = 根 + 1 层子目录，默认 2）
    #[serde(default = "default_tree_depth")]
    pub depth: u32,
}

fn default_tree_depth() -> u32 {
    2
}

#[derive(Debug, Serialize)]
pub struct TreeNode {
    pub path: String,
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub fs_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<TreeNode>>,
}

#[derive(Debug, Serialize)]
pub struct PreviewTreeResponse {
    pub root: Vec<TreeNode>,
    pub short_key: String,
    pub shareid: String,
}

/// POST /api/v1/share-sync/preview-tree
///
/// 输入：分享链接 + 提取码 + 深度
/// 输出：根节点直接 children 的目录树（每项含 `path` 用于回填 include_paths）
pub async fn preview_tree(
    State(state): State<AppState>,
    Json(req): Json<PreviewTreeRequest>,
) -> ApiResult<Json<ApiResponse<PreviewTreeResponse>>> {
    if req.share_url.trim().is_empty() {
        return Err(err_bad("share_url 不能为空"));
    }
    let depth = req.depth.clamp(1, 4);

    // 取 NetdiskClient（必须已登录）
    let client = {
        let g = state.netdisk_client.read().await;
        match &*g {
            Some(c) => c.clone(),
            None => return Err(err_bad("网盘客户端未初始化，请先登录百度账号")),
        }
    };

    // 解析链接
    let share_link = client
        .parse_share_link(&req.share_url)
        .map_err(|e| err_bad(&format!("解析分享链接失败: {}", e)))?;
    let effective_pwd = req.password.or(share_link.password.clone());

    // 访问分享页取 bdstoken
    let page = client
        .access_share_page(&share_link.short_key, &effective_pwd, true)
        .await
        .map_err(|e| err_bad(&format!("访问分享页失败: {}", e)))?;
    if page.shareid.is_empty() {
        return Err(err_bad("分享页面返回的 shareid 为空"));
    }

    // 🔥 关键：如有密码，先 verify_share_password 拿到 randsk 写入 Cookie，
    // 否则 list_share_files 会返回 errno=-9 "提取码验证失败"。
    if let Some(ref pwd) = effective_pwd {
        if !pwd.is_empty() {
            let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
            if let Err(e) = client
                .verify_share_password(
                    &page.shareid,
                    &page.share_uk,
                    &page.bdstoken,
                    pwd,
                    &referer,
                )
                .await
            {
                return Err(err_bad(&format!("验证提取码失败: {}", e)));
            }
        }
    }

    // 列根
    let root = client
        .list_share_files(&share_link.short_key, &page.bdstoken, 1, 100)
        .await
        .map_err(|e| err_bad(&format!("列根失败: {}", e)))?;
    let root_shareid = if !root.shareid.is_empty() {
        root.shareid
    } else {
        page.shareid.clone()
    };
    let root_uk = if !root.uk.is_empty() {
        root.uk
    } else {
        page.uk.clone()
    };

    // 展开为 TreeNode（异步递归 → 用 Box::Future 显式签名）
    let share_root = infer_share_root(&root.files);
    let mut out: Vec<TreeNode> = Vec::with_capacity(root.files.len());
    for f in root.files {
        let node = build_tree_node(
            &client,
            &share_link.short_key,
            &root_shareid,
            &root_uk,
            &page.bdstoken,
            &share_root,
            f,
            depth - 1,
        )
        .await;
        out.push(node);
    }

    Ok(Json(ApiResponse::success(PreviewTreeResponse {
        root: out,
        short_key: share_link.short_key,
        shareid: root_shareid,
    })))
}

/// 递归构造 TreeNode。返回 `Pin<Box<dyn Future>>` 以支持 async 递归。
fn build_tree_node<'a>(
    client: &'a crate::netdisk::client::NetdiskClient,
    short_key: &'a str,
    shareid: &'a str,
    uk: &'a str,
    bdstoken: &'a str,
    share_root: &'a str,
    info: crate::transfer::SharedFileInfo,
    remaining_depth: u32,
) -> Pin<Box<dyn Future<Output = TreeNode> + Send + 'a>> {
    Box::pin(async move {
        let mut node = TreeNode {
            path: normalize_share_path(&info.path, &info.name, share_root),
            name: info.name.clone(),
            is_dir: info.is_dir,
            size: info.size,
            fs_id: info.fs_id,
            children: None,
        };
        if info.is_dir && remaining_depth > 0 {
            match client
                .list_share_files_in_dir(short_key, shareid, uk, bdstoken, &info.path, 1, 100)
                .await
            {
                Ok(files) => {
                    let mut kids: Vec<TreeNode> = Vec::with_capacity(files.len());
                    for f in files {
                        kids.push(
                            build_tree_node(
                                client,
                                short_key,
                                shareid,
                                uk,
                                bdstoken,
                                share_root,
                                f,
                                remaining_depth - 1,
                            )
                            .await,
                        );
                    }
                    node.children = Some(kids);
                }
                Err(e) => {
                    // 子目录展开失败时仍返回当前节点，前端可见"加载失败"
                    tracing::warn!("展开子目录 {} 失败: {}", info.path, e);
                }
            }
        }
        node
    })
}

#[allow(dead_code)]
fn _suppress_unused_status() -> StatusCode {
    StatusCode::OK
}
