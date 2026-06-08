use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct BatchOperationRequest {
    pub task_ids: Option<Vec<String>>,
    #[serde(default)]
    pub all: Option<bool>,
    #[serde(default)]
    pub delete_files: Option<bool>,
    /// 批量操作的归属 UID（强制过滤）。
    ///
    /// - `Some(uid)` → 操作仅作用于该账号的任务
    /// - `None`     → 回退到 `state.active_uid()`，无活跃账号时拒绝
    ///
    /// 共享 manager 下所有路径都强制校验：
    /// - `all=true` → 仅取该账号的可操作任务（`*_for_uid` 助手）
    /// - 显式 `task_ids` → handler 调用 `validate_task_ids_for_uid` 逐条校验，
    ///   `task.owner_uid != uid` 的 id 直接以 `success: false` 返回，不再下发到
    ///   底层 manager（防止 A 账号上下文操作 B 账号任务）
    ///
    /// 前端 alias `owner_uid` 兼容（与 cloud_dl/uploads 一致）。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct BatchOperationItem {
    pub task_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BatchOperationResponse {
    pub total: usize,
    pub success_count: usize,
    pub failed_count: usize,
    pub results: Vec<BatchOperationItem>,
}

impl BatchOperationResponse {
    pub fn from_results(results: Vec<BatchOperationItem>) -> Self {
        let total = results.len();
        let success_count = results.iter().filter(|r| r.success).count();
        Self {
            total,
            success_count,
            failed_count: total - success_count,
            results,
        }
    }
}
