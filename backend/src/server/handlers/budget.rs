//! 多账号资源配额 HTTP API
//!
//! 路由：
//! - `GET  /api/v1/budget`                            → 全量配额快照
//! - `PUT  /api/v1/config/multi_account_budget`       → 更新机器总上限 + 权重
//! - `PUT  /api/v1/config/vip_recommended`            → 更新 VIP 推荐表
//! - `PUT  /api/v1/accounts/:uid/custom_config`       → 更新单账号 custom_config
//!
//! 所有写接口在落盘 + 调度器热更新成功后，会主动推送一次
//! `WsServerMessage::Budget(BudgetEvent::BudgetRecomputed { .. })` 给所有连接，
//! 前端无须轮询。

use crate::auth::types::{AccountConfig, Uid};
use crate::config::{MultiAccountBudgetConfig, MultiAccountVipRecommendedConfig};
use crate::downloader::budget_scheduler::{
    BudgetScheduler, BudgetSnapshot, RequestedSource, VipRecommendedTable, VipType as BsVipType,
    WeightTable,
};
use crate::server::error::{ApiError, ApiResult};
use crate::server::events::{BudgetEvent, UsageSnapshotEntry, WsAccountBudget};
use crate::server::websocket::WsServerMessage;
use crate::server::AppState;
use axum::{
    extract::{Path, State},
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

use super::ApiResponse;

// ============================================================================
// 序列化辅助类型（snapshot → JSON）
// ============================================================================

/// `BudgetSnapshot` 的 wire 镜像（`Serialize`）
#[derive(Debug, Clone, Serialize)]
pub struct BudgetSnapshotDto {
    pub machine_budget_download: usize,
    pub machine_budget_upload: usize,
    pub per_account: Vec<WsAccountBudget>,
}

impl From<BudgetSnapshot> for BudgetSnapshotDto {
    fn from(s: BudgetSnapshot) -> Self {
        Self {
            machine_budget_download: s.machine_budget_download,
            machine_budget_upload: s.machine_budget_upload,
            per_account: s
                .per_account
                .into_iter()
                .map(|e| WsAccountBudget {
                    uid: e.uid.raw(),
                    vip_cap_download: e.vip_cap_download,
                    base_download: e.base_download,
                    used_download: e.used_download,
                    vip_cap_upload: e.vip_cap_upload,
                    base_upload: e.base_upload,
                    used_upload: e.used_upload,
                })
                .collect(),
        }
    }
}

// ============================================================================
// 公共工具：推送 BudgetRecomputed
// ============================================================================

/// 从调度器拉一次快照并广播 `BudgetRecomputed`。
///
/// 任何修改了 base/vip_cap/M 的写接口都应该在调度器联动后调用本函数。
///
/// 持 `recompute_lock` 读取（避免读到 mutator 中间态）。
pub async fn broadcast_budget_recomputed(app_state: &AppState) {
    let snapshot = app_state.budget_scheduler.snapshot().await;
    let dto: BudgetSnapshotDto = snapshot.into();
    let event = BudgetEvent::BudgetRecomputed {
        machine_budget_download: dto.machine_budget_download,
        machine_budget_upload: dto.machine_budget_upload,
        per_account: dto.per_account,
    };
    app_state
        .ws_manager
        .broadcast(WsServerMessage::budget(event));
}

/// 仅推送 `used` 增量（高频，1s tick 用）。
///
/// 当前实现在 handler 写接口后不会触发，预留给后台 ticker。
#[allow(dead_code)]
pub async fn broadcast_usage_snapshot(app_state: &AppState) {
    let snapshot = app_state.budget_scheduler.snapshot().await;
    let per_account = snapshot
        .per_account
        .iter()
        .map(|e| UsageSnapshotEntry {
            uid: e.uid.raw(),
            used_download: e.used_download,
            used_upload: e.used_upload,
        })
        .collect();
    let event = BudgetEvent::UsageSnapshot { per_account };
    app_state
        .ws_manager
        .broadcast(WsServerMessage::budget(event));
}

// ============================================================================
// GET /api/v1/budget
// ============================================================================

/// 获取当前全量配额快照
pub async fn get_budget(
    State(app_state): State<AppState>,
) -> ApiResult<Json<ApiResponse<BudgetSnapshotDto>>> {
    let snapshot = app_state.budget_scheduler.snapshot().await;
    Ok(Json(ApiResponse::success(snapshot.into())))
}

// ============================================================================
// PUT /api/v1/config/multi_account_budget
// ============================================================================

/// 更新机器总上限 + VIP 权重
///
/// 写入 `config/app.toml.[multi_account_budget]`，再调用
/// `BudgetScheduler::update_machine_budget` / `update_weights`，最后广播 `BudgetRecomputed`。
pub async fn update_multi_account_budget(
    State(app_state): State<AppState>,
    Json(body): Json<MultiAccountBudgetConfig>,
) -> ApiResult<Json<ApiResponse<BudgetSnapshotDto>>> {
    // 基本校验
    if body.weight_normal == 0 || body.weight_vip == 0 || body.weight_svip == 0 {
        return Err(ApiError::BadRequest("权重必须 > 0".into()));
    }
    let m_dl = body.download_machine_budget.resolve();
    let m_up = body.upload_machine_budget.resolve();
    if m_dl == 0 || m_up == 0 {
        return Err(ApiError::BadRequest("机器总上限必须 > 0".into()));
    }

    // 1) 落盘 + 内存 config 更新
    {
        let mut config = app_state.config.write().await;
        config.multi_account_budget = body.clone();
        config
            .save_to_file("config/app.toml")
            .await
            .map_err(ApiError::Internal)?;
    }

    // 2) BudgetScheduler 热更新（合并到一次原子事务）
    //
    // 之前 update_machine_budget + update_weights 是两次独立 recompute 事务，
    // 中间窗口外部可观察到"新 M + 旧 weights"。`update_budget_config` 把两步
    // 在同一 `recompute_lock` 内完成，外部读快照不再有不一致瞬间。
    let bs: &Arc<BudgetScheduler> = &app_state.budget_scheduler;
    let weights = WeightTable {
        normal: body.weight_normal,
        vip: body.weight_vip,
        svip: body.weight_svip,
    };
    bs.update_budget_config(Some(m_dl), Some(m_up), Some(weights.clone()), Some(weights))
        .await;
    info!(
        "更新 multi_account_budget: M_dl={}, M_up={}, weights={}/{}/{}",
        m_dl, m_up, body.weight_normal, body.weight_vip, body.weight_svip
    );

    // 3) 推送 BudgetRecomputed
    broadcast_budget_recomputed(&app_state).await;

    // 4) 返回新快照
    let snapshot = app_state.budget_scheduler.snapshot().await;
    Ok(Json(ApiResponse::success(snapshot.into())))
}

// ============================================================================
// PUT /api/v1/config/vip_recommended
// ============================================================================

/// 更新 VIP 推荐配置表（普通/VIP/SVIP 三档）
pub async fn update_vip_recommended(
    State(app_state): State<AppState>,
    Json(body): Json<MultiAccountVipRecommendedConfig>,
) -> ApiResult<Json<ApiResponse<BudgetSnapshotDto>>> {
    // 基本校验
    for (name, entry) in [
        ("normal", &body.normal),
        ("vip", &body.vip),
        ("svip", &body.svip),
    ] {
        if entry.threads == 0 || entry.chunk_size_mb == 0 || entry.max_concurrent_tasks == 0 {
            return Err(ApiError::BadRequest(format!("VIP 推荐表 {name} 项含 0 值")));
        }
    }

    // 1) 落盘 + 内存 config 更新
    {
        let mut config = app_state.config.write().await;
        config.multi_account_vip_recommended = body.clone();
        config
            .save_to_file("config/app.toml")
            .await
            .map_err(ApiError::Internal)?;
    }

    // 2) BudgetScheduler 热更新（下载 + 上传共用同一档表）
    let table = VipRecommendedTable {
        normal_threads: body.normal.threads,
        vip_threads: body.vip.threads,
        svip_threads: body.svip.threads,
    };
    app_state
        .budget_scheduler
        .update_recommended(Some(table.clone()), Some(table))
        .await;
    info!(
        "更新 vip_recommended: threads={}/{}/{}",
        body.normal.threads, body.vip.threads, body.svip.threads
    );

    // 2.5) 遍历所有 auto_apply_recommended=true
    // 的账号，按其 vip 等级算出新的 effective threads，调对应 per-uid manager
    // 的 update_max_threads 同步本地线程闸门。
    //
    // 之前只调 BudgetScheduler.update_recommended → 预算 base/vip_cap 重算了，
    // 但下载/上传 scheduler 自己的本地 max_global_threads 闸门还是旧值 → 预算
    // 面板上看到新 vip_cap，运行中的分片仍卡在旧本地闸门，必须重启或其它配置
    // 更新才生效。
    {
        let auto_users: Vec<(
            crate::auth::Uid,
            crate::downloader::budget_scheduler::VipType,
        )> = {
            let am = app_state.account_manager.lock().await;
            am.list_users()
                .iter()
                .filter(|u| u.custom_config.auto_apply_recommended)
                .map(|u| {
                    (
                        crate::auth::Uid::new(u.uid),
                        crate::downloader::budget_scheduler::VipType::from_raw(u.vip_type),
                    )
                })
                .collect()
        };
        for (uid, vip) in auto_users {
            let (new_threads, new_max_concurrent) = match vip {
                crate::downloader::budget_scheduler::VipType::Normal => {
                    (body.normal.threads, body.normal.max_concurrent_tasks)
                }
                crate::downloader::budget_scheduler::VipType::Vip => {
                    (body.vip.threads, body.vip.max_concurrent_tasks)
                }
                crate::downloader::budget_scheduler::VipType::Svip => {
                    (body.svip.threads, body.svip.max_concurrent_tasks)
                }
            };
            if let Some(dm) = app_state.download_manager_for(uid) {
                dm.update_max_threads(new_threads);
                dm.update_max_concurrent_tasks(new_max_concurrent).await;
                info!(
                    "✓ vip_recommended 热更新: DownloadManager uid={} max_global_threads={} max_concurrent_tasks={}",
                    uid.raw(),
                    new_threads,
                    new_max_concurrent
                );
            }
            if let Some(um) = app_state.upload_manager_for(uid) {
                um.update_max_threads(new_threads);
                um.update_max_concurrent_tasks(new_max_concurrent).await;
                info!(
                    "✓ vip_recommended 热更新: UploadManager uid={} max_global_threads={} max_concurrent_tasks={}",
                    uid.raw(),
                    new_threads,
                    new_max_concurrent
                );
            }
        }
    }

    // 3) 推送 + 返回
    broadcast_budget_recomputed(&app_state).await;
    let snapshot = app_state.budget_scheduler.snapshot().await;
    Ok(Json(ApiResponse::success(snapshot.into())))
}

// ============================================================================
// PUT /api/v1/accounts/:uid/custom_config
// ============================================================================

/// Partial DTO for `PUT /api/v1/accounts/:uid/custom_config`
///
/// 与 `AccountConfig` 区别：所有字段均为 `Option<...>`，前端可以仅传需要修改的
/// 字段，未传字段保留旧值（merge 语义而非整体替换）。
///
/// 后端 merge 流程（见 `update_account_custom_config`）：
/// 1. 读取旧 `AccountConfig`
/// 2. 用本结构里的非 None 字段逐项覆写（含 download/upload 子结构的子字段）
/// 3. 持久化合并后的完整 `AccountConfig`
/// 4. 仅当真实有「线程预算相关字段」变化时触发 `BudgetScheduler::update_account_request`
///    + `BudgetRecomputed`；纯 `max_concurrent_tasks` / `max_retries` /
///    `chunk_size_mb` / `skip_hidden_files` 变更走"仅本地 manager 热更新"路径，
///    不进配额重算
#[derive(Debug, Default, Deserialize)]
pub struct AccountConfigPartial {
    pub auto_apply_recommended: Option<bool>,
    pub download: Option<AccountDownloadConfigPartial>,
    pub upload: Option<AccountUploadConfigPartial>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AccountDownloadConfigPartial {
    pub max_global_threads: Option<usize>,
    pub chunk_size_mb: Option<u64>,
    pub max_concurrent_tasks: Option<usize>,
    pub max_retries: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AccountUploadConfigPartial {
    pub max_global_threads: Option<usize>,
    pub chunk_size_mb: Option<u64>,
    pub max_concurrent_tasks: Option<usize>,
    pub max_retries: Option<u32>,
    pub skip_hidden_files: Option<bool>,
}

/// 更新单账号 `custom_config`
///
/// 流程：
/// 1. 读取旧 `AccountConfig`，与请求体（部分字段）merge
/// 2. `AccountManager::update_user_custom_config` 持久化到 `accounts.json`
/// 3. **per-uid manager 热更新**：
///    - `DownloadManager::update_max_concurrent_tasks` / `update_max_threads`
///    - `UploadManager::update_max_concurrent_tasks` / `update_max_threads` /
///      `update_max_retries`
///    （否则用户改了"最大任务数/重试次数"持久化但运行 manager 仍按旧值调度）
/// 4. 仅当线程预算相关字段（`auto_apply_recommended` / `*.max_global_threads`）
///    变化时调用 `BudgetScheduler::update_account_request` + 广播 `BudgetRecomputed`；
///    纯本地配置变化不进配额重算
pub async fn update_account_custom_config(
    State(app_state): State<AppState>,
    Path(uid_raw): Path<u64>,
    Json(body): Json<AccountConfigPartial>,
) -> ApiResult<Json<ApiResponse<BudgetSnapshotDto>>> {
    let uid = Uid::new(uid_raw);

    // 1) 读取旧配置（找不到账号 → 404）
    let old_cfg: AccountConfig = {
        let mgr = app_state.account_manager.lock().await;
        mgr.get_user(uid)
            .ok_or_else(|| ApiError::NotFound(format!("账号不存在: uid={uid_raw}")))?
            .custom_config
            .clone()
    };

    // 2) merge：用 body 中非 None 的字段覆写 old_cfg
    let mut merged = old_cfg.clone();
    if let Some(v) = body.auto_apply_recommended {
        merged.auto_apply_recommended = v;
    }
    if let Some(dl) = &body.download {
        if let Some(v) = dl.max_global_threads {
            merged.download.max_global_threads = v;
        }
        if let Some(v) = dl.chunk_size_mb {
            merged.download.chunk_size_mb = v;
        }
        if let Some(v) = dl.max_concurrent_tasks {
            merged.download.max_concurrent_tasks = v;
        }
        if let Some(v) = dl.max_retries {
            merged.download.max_retries = v;
        }
    }
    if let Some(up) = &body.upload {
        if let Some(v) = up.max_global_threads {
            merged.upload.max_global_threads = v;
        }
        if let Some(v) = up.chunk_size_mb {
            merged.upload.chunk_size_mb = v;
        }
        if let Some(v) = up.max_concurrent_tasks {
            merged.upload.max_concurrent_tasks = v;
        }
        if let Some(v) = up.max_retries {
            merged.upload.max_retries = v;
        }
        if let Some(v) = up.skip_hidden_files {
            merged.upload.skip_hidden_files = v;
        }
    }

    // 2.5) 服务端硬校验 — 拒绝 0 值
    //
    // 没有这个校验时，0 值会从 partial DTO 直接 merge → 持久化 → 热更新到本地
    // manager。`TaskSlotPool::resize(0)` / `update_max_threads(0)` 都不兜底，
    // `Semaphore` 容量被设为 0 后所有分片 acquire 永远阻塞 → 该账号下载/上传
    // 完全停摆。前端 `:min="1"` 不能作为安全边界。
    fn validate_positive_usize(v: usize, field: &str) -> Result<(), ApiError> {
        if v == 0 {
            return Err(ApiError::BadRequest(format!(
                "配置字段 {} 不能为 0（会导致调度阀门关闭）",
                field
            )));
        }
        Ok(())
    }
    fn validate_positive_u64(v: u64, field: &str) -> Result<(), ApiError> {
        if v == 0 {
            return Err(ApiError::BadRequest(format!(
                "配置字段 {} 不能为 0（会导致调度阀门关闭）",
                field
            )));
        }
        Ok(())
    }
    validate_positive_usize(
        merged.download.max_global_threads,
        "download.max_global_threads",
    )?;
    validate_positive_u64(merged.download.chunk_size_mb, "download.chunk_size_mb")?;
    validate_positive_usize(
        merged.download.max_concurrent_tasks,
        "download.max_concurrent_tasks",
    )?;
    validate_positive_usize(
        merged.upload.max_global_threads,
        "upload.max_global_threads",
    )?;
    validate_positive_u64(merged.upload.chunk_size_mb, "upload.chunk_size_mb")?;
    validate_positive_usize(
        merged.upload.max_concurrent_tasks,
        "upload.max_concurrent_tasks",
    )?;
    // max_retries 允许为 0（"不重试"是合法语义；不影响调度阀门）

    // 3) 判定哪些字段实际发生变化（用于条件触发预算重算与本地 manager 热更新）
    let auto_changed = old_cfg.auto_apply_recommended != merged.auto_apply_recommended;
    let dl_threads_changed =
        old_cfg.download.max_global_threads != merged.download.max_global_threads;
    let up_threads_changed = old_cfg.upload.max_global_threads != merged.upload.max_global_threads;
    let dl_concurrent_changed =
        old_cfg.download.max_concurrent_tasks != merged.download.max_concurrent_tasks;
    let up_concurrent_changed =
        old_cfg.upload.max_concurrent_tasks != merged.upload.max_concurrent_tasks;
    let up_retries_changed = old_cfg.upload.max_retries != merged.upload.max_retries;

    // 4) 持久化
    let updated = {
        let mut mgr = app_state.account_manager.lock().await;
        mgr.update_user_custom_config(uid, merged.clone())
            .await
            .map_err(ApiError::Internal)?
    };
    if !updated {
        // 与 step 1 的检查竞态时（极小概率）也按 NotFound 返回
        return Err(ApiError::NotFound(format!("账号不存在: uid={uid_raw}")));
    }

    // 5) per-uid manager 热更新
    //    线程槽位 `max_global_threads` 由 `BudgetScheduler::recompute_budget` 统筹计算
    //    全局水位线，但下载/上传 scheduler 自己还有本地 `max_global_threads` 闸门
    //    （`backend/src/downloader/scheduler.rs:542-546` / `backend/src/uploader/scheduler.rs:513-517`），
    //    必须同步调 `update_max_threads` 让本地闸门也跟随，否则预算给了 permit
    //    但本地 sem 还卡在旧值 → 调度不出去。
    //
    //    `auto_apply_recommended` 切换时（auto_changed=true），
    //    effective threads 会从"自定义值"变成"VIP 推荐值"或反之，但 `max_global_threads`
    //    字段没变 → `dl_threads_changed/up_threads_changed` 为 false → 之前不会调
    //    `update_max_threads` → 本地闸门卡在旧 effective 值。修复：当
    //    `auto_changed || dl_threads_changed` 时都触发下载侧 update_max_threads；
    //    上传侧同理 `auto_changed || up_threads_changed`。
    //
    //    任务槽位 `max_concurrent_tasks` 与 `max_retries` 是本地 manager UX 字段，
    //    不进 BudgetScheduler。
    let dl_effective_threads_changed = auto_changed || dl_threads_changed;
    let up_effective_threads_changed = auto_changed || up_threads_changed;
    // max_concurrent_tasks 在 auto 模式下跟随 VIP 推荐表
    // → auto 切换或自定义并发数变化都需要触发本地热更新
    let dl_effective_concurrent_changed = auto_changed || dl_concurrent_changed;
    let up_effective_concurrent_changed = auto_changed || up_concurrent_changed;
    if dl_effective_concurrent_changed || dl_effective_threads_changed {
        if let Some(dm) = app_state
            .download_managers
            .get(&uid)
            .map(|e| e.value().clone())
        {
            // 取该账号 effective 配置（threads + concurrent，auto 模式 → VIP 推荐表）
            let user_for_uid: Option<crate::auth::UserAuth> = {
                let am = app_state.account_manager.lock().await;
                am.get_user(uid).cloned()
            };
            let cfg_guard = app_state.config.read().await;
            let (eff_dl_threads, eff_dl_concurrent, _, _, _, _) =
                if let Some(ref user) = user_for_uid {
                    crate::server::state::resolve_effective_account_config(user, &cfg_guard)
                } else {
                    (
                        merged.download.max_global_threads,
                        merged.download.max_concurrent_tasks,
                        merged.download.max_retries,
                        merged.upload.max_global_threads,
                        merged.upload.max_concurrent_tasks,
                        merged.upload.max_retries,
                    )
                };
            drop(cfg_guard);
            if dl_effective_threads_changed {
                dm.update_max_threads(eff_dl_threads);
                info!(
                    "✓ DownloadManager uid={} 本地线程闸门热更新: max_global_threads={} (auto={})",
                    uid.raw(),
                    eff_dl_threads,
                    merged.auto_apply_recommended
                );
            }
            if dl_effective_concurrent_changed {
                dm.update_max_concurrent_tasks(eff_dl_concurrent).await;
                info!(
                    "✓ DownloadManager uid={} 本地配置热更新: max_concurrent_tasks={} (auto={})",
                    uid.raw(),
                    eff_dl_concurrent,
                    merged.auto_apply_recommended
                );
            }
        }
    }
    if up_effective_concurrent_changed || up_retries_changed || up_effective_threads_changed {
        if let Some(um) = app_state
            .upload_managers
            .get(&uid)
            .map(|e| e.value().clone())
        {
            let user_for_uid: Option<crate::auth::UserAuth> = {
                let am = app_state.account_manager.lock().await;
                am.get_user(uid).cloned()
            };
            let cfg_guard = app_state.config.read().await;
            let (_, _, _, eff_up_threads, eff_up_concurrent, _) =
                if let Some(ref user) = user_for_uid {
                    crate::server::state::resolve_effective_account_config(user, &cfg_guard)
                } else {
                    (
                        merged.download.max_global_threads,
                        merged.download.max_concurrent_tasks,
                        merged.download.max_retries,
                        merged.upload.max_global_threads,
                        merged.upload.max_concurrent_tasks,
                        merged.upload.max_retries,
                    )
                };
            drop(cfg_guard);
            if up_effective_threads_changed {
                um.update_max_threads(eff_up_threads);
                info!(
                    "✓ UploadManager uid={} 本地线程闸门热更新: max_global_threads={} (auto={})",
                    uid.raw(),
                    eff_up_threads,
                    merged.auto_apply_recommended
                );
            }
            if up_effective_concurrent_changed {
                um.update_max_concurrent_tasks(eff_up_concurrent).await;
            }
            if up_retries_changed {
                um.update_max_retries(merged.upload.max_retries);
            }
            info!(
                "✓ UploadManager uid={} 本地配置热更新: max_concurrent_tasks={}, max_retries={}",
                uid.raw(),
                eff_up_concurrent,
                merged.upload.max_retries
            );
        }
    }

    // 6) BudgetScheduler 联动（仅当线程预算相关字段变化时）
    let needs_budget_recompute = auto_changed || dl_threads_changed || up_threads_changed;
    if needs_budget_recompute {
        let dl_req = if merged.auto_apply_recommended {
            RequestedSource::Auto
        } else {
            RequestedSource::User(merged.download.max_global_threads)
        };
        let up_req = if merged.auto_apply_recommended {
            RequestedSource::Auto
        } else {
            RequestedSource::User(merged.upload.max_global_threads)
        };
        let ok = app_state
            .budget_scheduler
            .update_account_request(uid, Some(dl_req), Some(up_req))
            .await;
        if !ok {
            warn!(
                "BudgetScheduler 未注册 uid={}（可能尚未登录到调度器），将仅做持久化",
                uid_raw
            );
        }
        broadcast_budget_recomputed(&app_state).await;
    }

    info!(
        "更新账号 custom_config: uid={}, auto={}, dl_threads={}, up_threads={}, dl_concurrent={}, up_concurrent={}, up_retries={}, budget_recompute={}",
        uid_raw,
        merged.auto_apply_recommended,
        merged.download.max_global_threads,
        merged.upload.max_global_threads,
        merged.download.max_concurrent_tasks,
        merged.upload.max_concurrent_tasks,
        merged.upload.max_retries,
        needs_budget_recompute,
    );

    let snapshot = app_state.budget_scheduler.snapshot().await;
    Ok(Json(ApiResponse::success(snapshot.into())))
}

// suppress unused import warning when VipType is referenced from docs only
#[allow(dead_code)]
fn _typecheck(_v: BsVipType) {}
