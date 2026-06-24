// 多账号 active_uid 唯一写入 helper
//
// **不变式**：`*active_uid.write().await = ...` 在 backend/src 全局只允许出现在本文件内。
//
// 写入路径（持久化先行）：
//   1. 落盘 `accounts.json`（持久化镜像，通过 AccountManager.set_active_persisted）
//      — 失败则保持运行态不变，返回错误（避免半切换状态）
//   2. 写 `AppState.active_uid`（运行时真源）
//   3. 同步 legacy `current_user` / `netdisk_client`（向后兼容期）
//   4. CloudDl WS 订阅按账号重绑
//   5. 发 `AccountEvent::Switched`

use crate::auth::Uid;
use crate::server::events::{AccountEvent, TaskEvent};
use crate::server::AppState;
use anyhow::Result;
use tracing::info;

/// 切换活跃账号 — **唯一允许写入 `AppState.active_uid` 的入口**。
///
/// `new_uid = None` 表示删除最后一个账号、进入未登录状态。
///
/// **持久化先行**：落盘成功后再翻转运行态，避免
/// "前端见到 500 但 active_uid 已变" 的半切换状态。
///
/// **legacy 字段同步**：切换成功后同步 `current_user` / `netdisk_client`
/// legacy 字段以保证还在使用这些字段的旧路径（trigger_warmup / 代理热更等）
/// 不会被卡在上一活跃账号；新代码应直接走 `active_uid → AccountManager →
/// client_pool` 路由（参见 `active_user_auth` / `active_client`）。
pub async fn set_active_uid(state: &AppState, new_uid: Option<Uid>) -> Result<()> {
    // Step 1（持久化先行）：把目标 active_uid 写入 accounts.json。
    // 失败 → 直接返回错误，不动运行态，不发广播；前后端状态保持一致。
    {
        let mut mgr = state.account_manager.lock().await;
        mgr.set_active_persisted(new_uid).await?;
    }

    // Step 2: 翻转运行态真源 active_uid（持久化已成功，此步无 I/O 不会失败）
    *state.active_uid.write().await = new_uid;

    // Step 3: 同步 legacy 字段（current_user / netdisk_client）让旧路径读到新账号。
    //
    // 这两个字段在单账号架构里是真源，多账号架构里仍被部分代码路径使用
    // （trigger_warmup / 代理热更 fallback / Folder/Block-list 客户端）。
    // 切换后这些字段如果不更新，会继续操作上一活跃账号 — 串号。
    //
    // 失败（uid 不在 client_pool / AccountManager）只 warn，不回滚 active_uid：
    // 主要切换语义已完成，旧字段保持不变退化到"读不到 → 跳过该路径"，比硬错误
    // 更安全。
    match new_uid {
        Some(uid) => {
            let user_opt: Option<crate::auth::UserAuth> = {
                let am = state.account_manager.lock().await;
                am.get_user(uid).cloned()
            };
            let client_opt = state.client_pool.read().await.get_client(uid);

            *state.current_user.write().await = user_opt;
            *state.netdisk_client.write().await = client_opt.map(|arc| (*arc).clone());
        }
        None => {
            // 进入未登录状态：legacy 字段同步清空（与 logout handler 等价）
            *state.current_user.write().await = None;
            *state.netdisk_client.write().await = None;
        }
    }

    // Step 4: 🔥 CloudDl WS 订阅按账号重绑
    //
    // 必须放在 active_uid 写入之后、广播 Switched 之前：
    // 这样切换完成时旧 monitor 已不再有订阅者继续轮询，新 monitor 从切换瞬间
    // 起开始计入订阅者数；前端收到 Switched 后立刻能看到新账号的实时事件流。
    state.rebind_cloud_dl_ws_subscribers(new_uid).await;

    // Step 5: 发射 AccountEvent::Switched
    let event = TaskEvent::Account(AccountEvent::Switched {
        new_active_uid: new_uid.map(|u| u.raw()),
    });
    state.ws_manager.send_if_subscribed(event, None);

    info!(
        "set_active_uid: 活跃账号已切换 → {:?}（持久化先行 + legacy 同步 + cloud_dl 重绑 + 广播 Switched）",
        new_uid
    );

    Ok(())
}

/// 广播账号列表变更事件。
///
/// 用法：在 `accounts.rs` 增删账号 / `auth.rs` 登录登出后调用。
pub async fn broadcast_account_list_changed(state: &AppState) {
    let (accounts, active) = {
        let mgr = state.account_manager.lock().await;
        (mgr.list_accounts(), mgr.active_uid())
    };
    let event = TaskEvent::Account(AccountEvent::ListChanged {
        accounts,
        active_uid: active.map(|u| u.raw()),
    });
    state.ws_manager.send_if_subscribed(event, None);
    info!("broadcast_account_list_changed: 已广播 ListChanged");
}

/// 解析批量操作 `req.uid` 为 effective_uid。
///
/// 共享 manager 设计下，"all=true" 批量操作 / clear 操作必须按账号过滤，
/// 否则会跨账号误操作。本 helper 统一处理：
///
/// - `req.uid = Some(uid)` → 直接采用，不校验是否存在（与 cloud_dl 不同：
///   批量操作只是过滤条件，不存在的 uid 会过滤出空集合，不构成安全风险）
/// - `req.uid = None`      → 回退到 `state.active_uid`，无活跃账号返回 `None`
///
/// 调用方：handler 拿到 `Some(uid)` 后传给 `*_for_uid` 助手；拿到 `None` 时
/// 应返回 401（未登录）。
pub async fn resolve_batch_owner_uid(state: &AppState, req_uid: Option<u64>) -> Option<Uid> {
    if let Some(raw) = req_uid {
        return Some(Uid::new(raw));
    }
    *state.active_uid.read().await
}
