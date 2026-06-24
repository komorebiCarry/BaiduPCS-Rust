// 多账号管理 API
//
// 路由（在 main.rs 注册）：
//   GET    /api/v1/accounts/list      → 列出所有账号（脱敏摘要 + active_uid）
//   POST   /api/v1/accounts/switch    → 切换活跃账号（body: { uid: u64 }）
//   DELETE /api/v1/accounts/:uid      → 删除账号；?force=true 时强制关闭运行中任务

use crate::auth::{AccountSummary, Uid};
use crate::server::{
    broadcast_account_list_changed,
    error::{ApiError, ApiResult},
    set_active_uid, AppState,
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::info;

// ============================================================================
// DTO
// ============================================================================

/// `GET /accounts/list` 响应
#[derive(Debug, Serialize)]
pub struct ListAccountsResponse {
    /// 所有账号脱敏摘要
    pub accounts: Vec<AccountSummary>,
    /// 当前活跃账号 UID（`null` = 无活跃账号）
    pub active_uid: Option<u64>,
}

/// `POST /accounts/switch` 请求体
#[derive(Debug, Deserialize)]
pub struct SwitchAccountRequest {
    /// 要切换到的目标 UID（HTTP 线缆 `u64`）
    pub uid: u64,
}

/// `POST /accounts/switch` 响应
#[derive(Debug, Serialize)]
pub struct SwitchAccountResponse {
    /// 切换后的活跃 UID
    pub active_uid: u64,
}

/// `DELETE /accounts/:uid` 查询参数
#[derive(Debug, Default, Deserialize)]
pub struct DeleteAccountQuery {
    /// 是否强制删除（关闭运行中任务 + 持久化清理）
    #[serde(default)]
    pub force: bool,
}

/// `DELETE /accounts/:uid` 响应
#[derive(Debug, Serialize)]
pub struct DeleteAccountResponse {
    /// 被删除的 UID
    pub deleted_uid: u64,
    /// 删除后剩余的活跃 UID（`null` 表示删除的是最后一个账号）
    pub new_active_uid: Option<u64>,
}

// ============================================================================
// Handlers
// ============================================================================

/// `GET /api/v1/accounts/list`
///
/// 返回所有账号脱敏摘要 + 当前活跃 UID。
pub async fn list_accounts(State(state): State<AppState>) -> ApiResult<Json<ListAccountsResponse>> {
    let mgr = state.account_manager.lock().await;
    Ok(Json(ListAccountsResponse {
        accounts: mgr.list_accounts(),
        active_uid: mgr.active_uid().map(|u| u.raw()),
    }))
}

/// `POST /api/v1/accounts/switch`
///
/// 切换活跃账号；目标 UID 必须存在于 `AccountManager.users`，否则 400。
pub async fn switch_account(
    State(state): State<AppState>,
    Json(body): Json<SwitchAccountRequest>,
) -> ApiResult<Json<SwitchAccountResponse>> {
    let target_uid = Uid::new(body.uid);

    // 校验目标账号存在
    {
        let mgr = state.account_manager.lock().await;
        if mgr.get_user(target_uid).is_none() {
            return Err(ApiError::AccountNotAvailable);
        }
    }

    // 🔥 切换前确保 NetdiskClient 在 client_pool 中已存在；
    // preheat 启动期可能失败，此处兜底懒加载，避免切换后 active_client() 返回 None 导致 401 误报。
    state.ensure_client_for_uid(target_uid).await.map_err(|e| {
        tracing::error!(
            "切换前懒加载 client 失败: uid={}, err={:?}",
            target_uid.raw(),
            e
        );
        ApiError::Internal(anyhow::anyhow!(
            "切换账号失败: 无法构造目标账号客户端 ({})",
            e
        ))
    })?;

    // 🔥 切换前补建/补齐 manager + 运行时依赖
    //
    // 之前缺哪个就调 `build_and_register_managers_for_account` 全量重建，
    // 会通过 `register_account_managers` 无条件 insert 覆盖**已存在**的
    // manager（如 download manager 已有运行任务但 upload manager 缺失，全量重建
    // 会替换 download manager → 旧 Arc 上的任务还在跑但从池中消失，暂停/列表/
    // 槽位观测全部失联）。修复策略：
    //   - 三类 manager **全部缺失** → 走 `build_and_register_managers_for_account`
    //     （新构造一致组）
    //   - **部分缺失**（任意一类已存在但其它缺失）→ 直接报错，不允许覆盖在用
    //     manager；这种状态意味着启动期出现了不一致，需要管理员介入
    //   - 三类 manager **全部存在** → 跳过构造，仅走 wire 注入运行时依赖
    //
    // `wire_manager_runtime_deps_for_uid` 失败必须返回
    // Internal Error，而不是 warn 后继续。否则切换继续会让目标账号处于 active 但
    // manager 运行时依赖不完整的状态，后续文件夹下载、扫描上传、加密相关链路
    // 都会异常。
    {
        let dl_present = state.download_manager_for(target_uid).is_some();
        let up_present = state.upload_manager_for(target_uid).is_some();
        let tr_present = state.transfer_manager_for(target_uid).is_some();
        let all_present = dl_present && up_present && tr_present;
        let all_missing = !dl_present && !up_present && !tr_present;

        if !all_present && !all_missing {
            // 部分缺失 — 拒绝覆盖在用 manager
            tracing::error!(
                "切换前检测到 uid={} per-uid manager 池不一致: download={} upload={} transfer={}（拒绝覆盖在用 manager）",
                target_uid.raw(),
                dl_present,
                up_present,
                tr_present
            );
            return Err(ApiError::Internal(anyhow::anyhow!(
                "切换账号失败: uid={} manager 池处于部分缺失状态（download={} upload={} transfer={}）；不允许覆盖在用 manager，请重启服务或修复后重试",
                target_uid.raw(),
                dl_present,
                up_present,
                tr_present
            )));
        }

        if all_missing {
            // 全部缺失 → 启动期构造可能失败，这里按相同流程兜底构造一份
            let user_auth: Option<crate::auth::UserAuth> = {
                let am = state.account_manager.lock().await;
                am.get_user(target_uid).cloned()
            };
            if let Some(user) = user_auth {
                let pm_arc = std::sync::Arc::clone(&state.persistence_manager);
                if let Err(e) = state
                    .build_and_register_managers_for_account(user, pm_arc)
                    .await
                {
                    tracing::error!(
                        "切换前 build_and_register_managers_for_account uid={} 失败: {:?}",
                        target_uid.raw(),
                        e
                    );
                    return Err(ApiError::Internal(anyhow::anyhow!(
                        "切换账号失败: 无法构造目标账号 manager ({})",
                        e
                    )));
                }
            }
        }

        // 注入/刷新运行时依赖（幂等）；失败硬错误
        if let Err(e) = state.wire_manager_runtime_deps_for_uid(target_uid).await {
            tracing::error!(
                "切换前 wire_manager_runtime_deps_for_uid uid={} 失败: {:?}",
                target_uid.raw(),
                e
            );
            return Err(ApiError::Internal(anyhow::anyhow!(
                "切换账号失败: 目标账号 manager 运行时依赖注入失败 ({})",
                e
            )));
        }
    }

    // 走唯一入口写入 active_uid（自动持久化 + 广播 Switched）
    set_active_uid(&state, Some(target_uid))
        .await
        .map_err(ApiError::Internal)?;

    info!("switch_account: 已切换到 uid={}", target_uid.raw());

    Ok(Json(SwitchAccountResponse {
        active_uid: target_uid.raw(),
    }))
}

/// `DELETE /api/v1/accounts/:uid?force=true|false`
///
/// 删除账号：
/// - `force=true`（强制模式）：调用 `AppState::force_delete_account` 编排关闭所有
///   per-uid Manager → 移除 client_pool → 删除 AccountManager 条目 → 同步 active_uid。
/// - `force=false`（默认）：先扫描该账号下是否存在运行中（`Downloading` / `Uploading`
///   / `Pending` 等非终态）任务；若存在 → 返回 409 Conflict，提示调用方先停止任务或
///   显式选择 `force=true`。这是 force 契约的严格语义：避免静默丢弃用户进行中的
///   下载/上传任务。
pub async fn delete_account(
    State(state): State<AppState>,
    Path(uid): Path<u64>,
    Query(q): Query<DeleteAccountQuery>,
) -> ApiResult<Json<DeleteAccountResponse>> {
    let target_uid = Uid::new(uid);

    // 校验目标账号存在
    {
        let mgr = state.account_manager.lock().await;
        if mgr.get_user(target_uid).is_none() {
            return Err(ApiError::AccountNotAvailable);
        }
    }

    // 🔥 force=false：扫描运行中任务，存在则 409 拒绝
    if !q.force {
        if let Some(reason) = scan_running_tasks_for_uid(&state, target_uid).await {
            return Err(ApiError::Conflict(format!(
                "账号 uid={} 存在运行中任务（{}），删除前请先停止或重试时附 ?force=true",
                target_uid.raw(),
                reason
            )));
        }
    }

    // 执行 force-delete 编排
    //
    // `force_delete_account` 内部已通过 `set_active_uid` helper 写运行时
    // `active_uid` + 持久化 + 广播 `Switched`。
    // 这里不再手工补广播，避免重复事件。
    state
        .force_delete_account(target_uid)
        .await
        .map_err(ApiError::Internal)?;

    // 读取最新 active_uid（force_delete_account 已通过 helper 完成切换）
    let new_active = {
        let mgr = state.account_manager.lock().await;
        mgr.active_uid().map(|u| u.raw())
    };

    // 广播 ListChanged（账号列表变更，独立于 Switched）
    broadcast_account_list_changed(&state).await;

    info!(
        "delete_account: uid={} 已删除，new_active_uid={:?}",
        target_uid.raw(),
        new_active
    );

    Ok(Json(DeleteAccountResponse {
        deleted_uid: target_uid.raw(),
        new_active_uid: new_active,
    }))
}

/// 🔥 扫描指定 uid 是否有运行中（非终态）任务
///
/// 返回 `Some(reason)` 表示有运行任务，`reason` 是给用户看的简短描述；
/// 返回 `None` 表示该账号无运行任务，`force=false` 删除路径可安全继续。
///
/// 扫描覆盖：所有 `download_managers` / `upload_managers` / `transfer_managers` 池中
/// owner_uid == target 且 status ∈ { Pending, Downloading, Uploading, Decrypting, ... }
/// 的任务。完成 / 失败 / 暂停态被视为「非运行」。
///
/// 还包括 `folder_download_manager` 扫描。
/// 强删流程会清理 folder download，所以 `force=false` 也必须把进行中的文件夹
/// 任务计入"运行中"，避免账号下只有 `Scanning`/`Downloading` 状态的文件夹任务时
/// 静默走强删。
async fn scan_running_tasks_for_uid(state: &AppState, target_uid: Uid) -> Option<String> {
    use crate::downloader::folder::FolderStatus;
    use crate::downloader::TaskStatus as DlStatus;
    use crate::uploader::UploadTaskStatus as UpStatus;

    // 下载任务：扫描所有 manager（per-uid 独立实例 + 历史共享 Arc 兼容路径），按 owner_uid 过滤
    let mut dl_running = 0usize;
    for (_uid, dm) in state.list_download_managers() {
        for t in dm.get_all_tasks().await {
            if t.owner_uid == target_uid
                && matches!(
                    t.status,
                    DlStatus::Pending | DlStatus::Downloading | DlStatus::Decrypting
                )
            {
                dl_running += 1;
            }
        }
    }

    // 上传任务
    let mut up_running = 0usize;
    for (_uid, um) in state.list_upload_managers() {
        for t in um.get_all_tasks().await {
            if t.owner_uid == target_uid
                && !matches!(
                    t.status,
                    UpStatus::Completed | UpStatus::Failed | UpStatus::Paused
                )
            {
                up_running += 1;
            }
        }
    }

    // 🔥 转存任务
    let mut tr_running = 0usize;
    for (_uid, tm) in state.list_transfer_managers() {
        for t in tm.get_all_tasks().await {
            if t.owner_uid == target_uid && !t.status.is_terminal() {
                tr_running += 1;
            }
        }
    }

    // 🔥 文件夹下载
    // FolderDownloadManager 是共享单例，按 owner_uid 过滤运行态。
    let mut fd_running = 0usize;
    for f in state.folder_download_manager.get_all_folders().await {
        if f.owner_uid == target_uid
            && matches!(f.status, FolderStatus::Scanning | FolderStatus::Downloading)
        {
            fd_running += 1;
        }
    }

    // 🔥 自动备份
    //
    // AutoBackupManager 是进程级单例；`autobackup_manager_for(uid)` 内部回退
    // 池中任意实例（详见 state.rs::autobackup_manager_for 注释）。
    // 该实例的 `count_running_tasks_for_owner(raw)` 会按 `BackupTask.owner_uid`
    // 过滤，不存在跨实例漏数问题。
    let ab_running = state
        .autobackup_manager_for(target_uid)
        .map(|m| m.count_running_tasks_for_owner(target_uid.raw()))
        .unwrap_or(0);

    if dl_running == 0 && up_running == 0 && tr_running == 0 && fd_running == 0 && ab_running == 0 {
        return None;
    }

    let mut parts = Vec::with_capacity(5);
    if dl_running > 0 {
        parts.push(format!("{} 个下载任务", dl_running));
    }
    if up_running > 0 {
        parts.push(format!("{} 个上传任务", up_running));
    }
    if tr_running > 0 {
        parts.push(format!("{} 个转存任务", tr_running));
    }
    if fd_running > 0 {
        parts.push(format!("{} 个文件夹下载", fd_running));
    }
    if ab_running > 0 {
        parts.push(format!("{} 个自动备份任务", ab_running));
    }
    Some(parts.join(" + "))
}
