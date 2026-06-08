// HTTP 请求提取器 / Uid 解析 helper
//
// 职责：
// 1. **统一 `?uid=` query 参数解析**：HTTP 线缆层 `u64` → 运行态 `Uid` 的边界转换；
// 2. **`body.uid` 与 `active_uid` 回退解析器**：「显式优先 → 回退 active → 否则 400」三段语义；
// 3. **资源型路由的 `?uid=` 拒绝守卫**：单资源路由必须按资源自身 `owner_uid` 路由，不接受 `?uid=`。
//
// 设计要点：
// - 禁止 handler 中直接用 `u64` 调内部 API；所有 `client_pool.get_client` / `budget_scheduler.acquire_chunk_permit`
//   入口必须为 `Uid` 类型；
// - `body.uid` 仅用于「资源创建」路由（POST /downloads / POST /uploads ...），`?uid=` 仅用于「集合筛选」路由
//   （GET /downloads ...），二者**禁止互相复用**（防双重寻址歧义）。

use crate::auth::Uid;
use crate::server::error::ApiError;
use crate::server::AppState;
use serde::Deserialize;

/// `?uid=` query 参数（HTTP 线缆层 `u64`，可缺省）。
///
/// 用法（集合筛选路由）：
/// ```ignore
/// pub async fn list_downloads(
///     State(state): State<AppState>,
///     Query(q): Query<UidQuery>,
/// ) -> ApiResult<Json<...>> {
///     let filter_uid: Option<Uid> = resolve_uid_from_query(&q);
///     // filter_uid = None → 跨账号聚合；Some(Uid) → 仅该账号
/// }
/// ```
#[derive(Debug, Default, Clone, Deserialize)]
pub struct UidQuery {
    /// 可选的 `?uid=` 筛选参数（HTTP 线缆 `u64`）。
    pub uid: Option<u64>,
}

/// 把 `?uid=` query 参数从 HTTP 线缆 `u64` 转为运行态 `Uid`。
///
/// 返回 `None` 表示请求未指定 `?uid=`（集合路由的「跨账号聚合」语义）。
#[inline]
pub fn resolve_uid_from_query(q: &UidQuery) -> Option<Uid> {
    q.uid.map(Uid::new)
}

/// 资源型路由（单资源 `:id`）拒绝 `?uid=` 守卫。
///
/// 单资源路由按资源自身 `owner_uid` 路由；
/// 用户传 `?uid=` 时必须返回 400，避免「资源 owner ≠ 显式 uid」的歧义。
pub fn reject_uid_query_on_resource(q: &UidQuery) -> Result<(), ApiError> {
    if q.uid.is_some() {
        Err(ApiError::BadRequest(
            "单资源路由不接受 ?uid= 参数（按资源自身 owner_uid 路由）".to_string(),
        ))
    } else {
        Ok(())
    }
}

/// 创建资源类 API 的 owner_uid 解析锁定逻辑。
///
/// 解析顺序：`body.uid`（HTTP 线缆 `u64`） → `active_uid`（运行态 `Uid`） → `account_not_available`（400）。
///
/// **绝对禁止**写 `body.uid.unwrap_or_else(|| state.active_uid().raw())` —— 当 `active_uid = None`
/// （网盘已删除最后一个账号）时 `state.active_uid()` 返 None，`.raw()` 不存在。
///
/// 用法（创建路由）：
/// ```ignore
/// pub async fn create_download(
///     State(state): State<AppState>,
///     Json(body): Json<CreateDownloadRequest>,
/// ) -> ApiResult<...> {
///     let owner_uid = resolve_owner_uid_for_create(&state, body.uid).await?;
///     let client = state.client_pool.read().await
///         .get_client(owner_uid)
///         .ok_or(ApiError::AccountNotAvailable)?;
///     ...
/// }
/// ```
pub async fn resolve_owner_uid_for_create(
    state: &AppState,
    body_uid: Option<u64>,
) -> Result<Uid, ApiError> {
    if let Some(raw) = body_uid {
        // 显式指定 → HTTP 线缆 u64 → 运行态 Uid
        return Ok(Uid::new(raw));
    }
    // 回退 active_uid（运行态真源）
    state
        .active_uid
        .read()
        .await
        .ok_or(ApiError::AccountNotAvailable)
}

/// 校验给定 `Uid` 在 `client_pool` 中存在；缺失返回 `account_not_available`（400）。
///
/// 用法：在 `resolve_owner_uid_for_create` 之后立即调用，二态语义 helper。
pub async fn ensure_client_available(state: &AppState, uid: Uid) -> Result<(), ApiError> {
    let pool = state.client_pool.read().await;
    if pool.get_client(uid).is_some() {
        Ok(())
    } else {
        Err(ApiError::AccountNotAvailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_uid_from_query_some_and_none() {
        let q1 = UidQuery { uid: Some(123) };
        assert_eq!(resolve_uid_from_query(&q1), Some(Uid::new(123)));

        let q2 = UidQuery { uid: None };
        assert_eq!(resolve_uid_from_query(&q2), None);
    }

    #[test]
    fn reject_uid_query_on_resource_works() {
        assert!(reject_uid_query_on_resource(&UidQuery { uid: None }).is_ok());
        assert!(reject_uid_query_on_resource(&UidQuery { uid: Some(1) }).is_err());
    }
}
