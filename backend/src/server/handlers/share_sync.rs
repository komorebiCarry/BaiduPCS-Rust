//! 分享同步 API 处理器（/api/v1/share-sync/*）

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::server::error::ApiError;
use crate::server::extractors::{ensure_client_available, resolve_owner_uid_for_create};
use crate::server::AppState;
use crate::share_sync::{
    infer_share_root, normalize_pagination, normalize_share_path, CreateShareSubscriptionRequest,
    RunItemRecord, RunRecord, ShareSubscription, ShareSyncError, ShareSyncManager,
    UpdateShareSubscriptionRequest,
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
        ShareSyncError::AlreadyRunning(m) => ApiError::Conflict(m),
        ShareSyncError::ConfigError(m) => err_bad(&m),
        ShareSyncError::ShareLinkError(m) => err_bad(&m),
        ShareSyncError::FileSystemError(m) => err_bad(&m),
        ShareSyncError::NetworkError(m) => err_bad(&format!("网络错误: {}", m)),
        ShareSyncError::PersistenceError(m) => {
            if m.contains("读取配置")
                || m.contains("保存配置")
                || m.contains("路径")
                || m.contains("目录")
                || m.contains("权限")
            {
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

/// 按 id 取订阅；不存在 → 404。
///
/// 多账号策略对齐其它页面（autobackup/transfer/download）：后端不做按账号的访问拦截，
/// 列表默认返回全部账号、各资源带 `owner_uid`，由前端 `AccountFilter` + `AccountBadge`
/// 展示与过滤。执行同步时仍由 manager 按订阅 `owner_uid` 解析对应账号客户端，
/// 保证转存/下载始终落到正确账号（与触发者当前活跃账号无关）。
fn require_subscription(
    manager: &Arc<ShareSyncManager>,
    id: &str,
) -> ApiResult<ShareSubscription> {
    manager
        .get_subscription(id)
        .ok_or_else(|| err_not_found("订阅不存在"))
}

// =====================================================
// Routes
// =====================================================

/// 列表查询参数：可选 `uid` 按账号过滤（缺省返回全部账号）。
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
}

/// GET /api/v1/share-sync/subscriptions
///
/// 默认返回全部账号的订阅（与 transfer/autobackup 一致）；传 `?uid=` 时按账号过滤。
/// 前端用 `AccountFilter` 控制可见性，每条带 `owner_uid` 由 `AccountBadge` 展示。
pub async fn list_subscriptions(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ApiResponse<Vec<ShareSubscription>>>> {
    let m = get_manager(&state).await?;
    let list = match q.uid {
        Some(uid) => m.list_for_owner(uid),
        None => m.list_subscriptions(),
    };
    Ok(Json(ApiResponse::success(list)))
}

/// GET /api/v1/share-sync/subscriptions/:id
pub async fn get_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<ShareSubscription>>> {
    let m = get_manager(&state).await?;
    let s = require_subscription(&m, &id)?;
    Ok(Json(ApiResponse::success(s)))
}

/// POST /api/v1/share-sync/subscriptions
///
/// 归属账号：优先用请求里的 `uid`/`owner_uid`，缺省回退到当前活跃账号（与 download/autobackup 一致）。
/// 创建前校验该账号确实已登录（`client_pool` 中存在），避免存下无法同步的孤儿订阅
/// （传错 uid 时直接 400 `account_not_available`，而不是等到 resolver 执行阶段才失败）。
pub async fn create_subscription(
    State(state): State<AppState>,
    Json(req): Json<CreateShareSubscriptionRequest>,
) -> ApiResult<Json<ApiResponse<ShareSubscription>>> {
    let owner_uid = resolve_owner_uid_for_create(&state, req.uid).await?;
    ensure_client_available(&state, owner_uid).await?;
    let m = get_manager(&state).await?;
    let mut sub = req.into_subscription();
    sub.owner_uid = owner_uid.raw();
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
    let existing = require_subscription(&m, &id)?;
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
    require_subscription(&m, &id)?;
    m.delete_subscription(&id).await.map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(
        serde_json::json!({"deleted": id}),
    )))
}

/// POST /api/v1/share-sync/subscriptions/:id/enable
pub async fn enable_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    require_subscription(&m, &id)?;
    m.set_enabled(&id, true).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(
        serde_json::json!({"enabled": true}),
    )))
}

/// POST /api/v1/share-sync/subscriptions/:id/disable
pub async fn disable_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    require_subscription(&m, &id)?;
    m.set_enabled(&id, false).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(
        serde_json::json!({"enabled": false}),
    )))
}

/// POST /api/v1/share-sync/subscriptions/:id/resume
///
/// 「我已更新链接，恢复」：清除链接失效标记 + 连续失效计数，恢复轮询并立即重试一次。
pub async fn resume_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    require_subscription(&m, &id)?;
    m.resume_link_invalid(&id).await.map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(
        serde_json::json!({"subscription_id": id, "resumed": true}),
    )))
}

/// POST /api/v1/share-sync/subscriptions/:id/trigger
pub async fn trigger_subscription(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    tracing::info!("share-sync: POST /subscriptions/{}/trigger", id);
    let m = get_manager(&state).await?;
    require_subscription(&m, &id)?;
    let run_id = m.trigger_one(&id).await.map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(
        serde_json::json!({"subscription_id": id, "triggered": true, "run_id": run_id}),
    )))
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
    require_subscription(&m, &id)?;
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
    let items = m.persistence().list_run_items(&id).map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(RunDetailDto { run: rec, items })))
}

#[derive(Debug, Serialize)]
pub struct RunDetailDto {
    #[serde(flatten)]
    pub run: RunRecord,
    pub items: Vec<RunItemRecord>,
}

/// GET /api/v1/share-sync/subscriptions/:id/subtasks
///
/// 列出该订阅当前「进行中子任务」的进度（下载段 + 内部转存段），供前端轮询兜底
/// （WS `item_progress` 事件断线时使用）。与 WS 广播共用同一数据形状。
pub async fn list_subtasks(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<Vec<crate::share_sync::ShareSyncSubtask>>>> {
    let m = get_manager(&state).await?;
    require_subscription(&m, &id)?;
    let subs = m.subtasks(&id).await.map_err(map_share_err)?;
    Ok(Json(ApiResponse::success(subs)))
}

/// GET /api/v1/share-sync/subscriptions/:id/snapshots/latest
pub async fn latest_snapshot(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<serde_json::Value>>> {
    let m = get_manager(&state).await?;
    require_subscription(&m, &id)?;
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
    /// 按订阅所属账号路由网盘 client（编辑订阅时传订阅自己的 owner_uid，
    /// 不传则回退当前 active 账号——转存对话框创建场景 owner=active）。
    /// 对齐 AutoBackup：操作按资源 owner_uid 路由，不要求用户先切号。
    #[serde(default)]
    pub owner_uid: Option<u64>,
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

    // 按订阅所属账号路由网盘 client（对齐 AutoBackup）：
    // owner_uid 显式传入 = 编辑既有订阅（按订阅自己的账号路由）；未传 = 新建场景，
    // 用当前 active 账号预览（“新建就是用我当前账号”）。据此区分报错措辞，新建时不提
    // “订阅所属账号”这种令人困惑的说法。
    let is_explicit_owner = req.owner_uid.is_some();
    let owner_uid = match req.owner_uid {
        Some(raw) => crate::auth::Uid::new(raw),
        None => match *state.active_uid.read().await {
            Some(active) => active,
            None => return Err(err_bad("当前无已登录百度账号，请先登录后再预览目录树")),
        },
    };
    // 启动期只急切注入「活跃账号」client，非活跃账号是懒加载的；活跃账号预热也可能
    // 因网络/代理短暂失败而未入池。故取 client 前先 `ensure_client_for_uid` 兜底懒加载
    // （用持久化 cookie 构造，不联网），对齐 cloud_dl/accounts handler 的做法，避免
    // 「账号已登录却报未登录」的误报。仅当账号确实不在 AccountManager（需登录）或
    // 凭证失效导致构造失败时才报错。
    let unavailable_msg = |uid: u64, detail: String| {
        if is_explicit_owner {
            err_bad(&format!(
                "订阅所属账号 uid={uid} 不可用（未登录或登录已失效），请登录该账号后再预览目录树：{detail}"
            ))
        } else {
            err_bad(&format!(
                "当前账号 uid={uid} 不可用（未登录或登录已失效），请重新登录后再预览目录树：{detail}"
            ))
        }
    };
    if let Err(e) = state.ensure_client_for_uid(owner_uid).await {
        return Err(unavailable_msg(owner_uid.raw(), e.to_string()));
    }
    let client = state
        .client_pool
        .read()
        .await
        .get_client(owner_uid)
        .ok_or_else(|| unavailable_msg(owner_uid.raw(), "client 未入池".to_string()))?;

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

    // Keep this share's randsk and pass it explicitly while expanding the tree.
    let mut randsk = None;
    if let Some(ref pwd) = effective_pwd {
        if !pwd.is_empty() {
            let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
            match client
                .verify_share_password(&page.shareid, &page.share_uk, &page.bdstoken, pwd, &referer)
                .await
            {
                Ok(sekey) => randsk = Some(sekey),
                Err(e) => return Err(err_bad(&format!("验证提取码失败: {}", e))),
            }
        }
    }

    // 列根
    let root = client
        .list_share_files_with_randsk(
            &share_link.short_key,
            &page.bdstoken,
            1,
            100,
            randsk.as_deref(),
        )
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
            randsk.as_deref(),
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
    randsk: Option<&'a str>,
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
                .list_share_files_in_dir_with_randsk(
                    short_key, shareid, uk, bdstoken, &info.path, 1, 100, randsk,
                )
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
                                randsk,
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
