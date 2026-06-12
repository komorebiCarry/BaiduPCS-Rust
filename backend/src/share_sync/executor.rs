//! 分享同步执行器
//!
//! 接收一个 `ShareDiff` 与一条订阅，把每条变更按策略应用到「网盘/本地」目标。
//!
//! ## 设计
//!
//! - 通过 `ExecutorHooks` trait 抽象 TransferManager / DownloadManager 调用，
//!   便于单测时 mock
//! - 一次执行 = 一个 `ShareSyncRun`，每个文件级动作 = 一条 `run_item`
//! - 错误不中断整次 run；失败项记到 `run_item.error`，汇总到 `DiffSummary.failed`
//!
//! ## 冲突策略语义
//!
//! | 策略 | 已存在同名文件时 | 文件不存在时 |
//! |------|-----------------|---------------|
//! | `Overwrite` | 直接覆盖（删除旧 / 转存新） | 直接转存 |
//! | `Versioned` | 旧文件 rename 为 `name(YYYYMMDD-HHMMSS).ext` 后再转存 | 直接转存 |
//! | `Skip` | 不处理，记为 skipped | 直接转存 |
//!
//! ## 删除策略
//!
//! 分享中删除的文件：
//! - `delete_missing=true` → 从目标中删除
//! - `delete_missing=false` → 保留目标副本，记为 skipped

use crate::share_sync::config::{LocalTarget, NetdiskTarget, ShareSubscription, SyncTarget};
use crate::share_sync::diff::{ShareDiff, ShareModifiedItem};
use crate::share_sync::error::{ErrorCategory, ShareSyncError};
use crate::share_sync::persistence::ShareSyncPersistence;
use crate::share_sync::snapshot::{CapturedShare, ShareSnapshotItem};
use crate::share_sync::types::{
    ConflictStrategy, DiffSummary, RunItemStatus, RunStatus, SyncAction, TargetKind,
};
use async_trait::async_trait;
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

const TASK_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const TASK_OPERATION_MAX_ATTEMPTS: usize = 3;
const TASK_RETRY_BASE_DELAY: Duration = Duration::from_secs(3);

/// 把"环境资源不足"类错误（Quota / LocalDiskFull）映射到 run_item reason
///
/// 资源类错误**不是失败**——它们是用户层面需要先清理空间再重跑的场景，
/// 所以：
/// - run_item 状态 = `Skipped`（不是 `Failed`）
/// - summary.skipped += 1（不是 failed）
/// - run 状态可保持 `Completed`（仅当没有其它错误时）
///
/// 返回 `None` 表示该错误不是资源类，按普通失败处理。
fn quota_skip_reason(err: &ShareSyncError) -> Option<&'static str> {
    match err.category() {
        ErrorCategory::Quota => Some("quota_full"),
        ErrorCategory::LocalDiskFull => Some("local_disk_full"),
        _ => None,
    }
}

fn seed_diff_summary(summary: &mut DiffSummary, diff: &ShareDiff) {
    let added = diff.added.iter().filter(|item| !item.is_dir).count();
    let modified = diff.modified.iter().filter(|item| !item.new.is_dir).count();
    let removed = diff.removed.iter().filter(|item| !item.is_dir).count();
    summary.total = added + modified + removed + diff.unchanged_count;
    summary.unchanged = diff.unchanged_count;
}

fn reconcile_summary_from_run_items(
    persistence: &ShareSyncPersistence,
    run_id: &str,
    summary: &mut DiffSummary,
) {
    if let Ok(items) = persistence.list_run_items(run_id) {
        summary.failed = items.iter().filter(|i| i.status == "failed").count();
        summary.skipped = items.iter().filter(|i| i.status == "skipped").count();
        summary.overwritten = items
            .iter()
            .filter(|i| i.status == "completed" && i.versioned_old_path.is_some())
            .count();
    }
}

/// v1：整批 submit 触发的最小组内文件数
///
/// 当 `apply_with_run_id_grouped` 把 added/modified 项按"目录根"分组后，
/// 组内文件数 `>= MIN_BATCH_SIZE` 才走整批 submit（`submit_transfer_batch` /
/// `submit_download_batch`），否则退化为逐文件 submit（`apply_with_run_id`）。
///
/// 取 2 的理由：
/// - 1 个文件走整批没有意义（与单文件 submit 等价，且多一次 transfer 内部 grouping 开销）
/// - 2+ 个文件时整批收益明显（N→1 任务数 + 一次鉴权 + transfer 内部按父目录分 batch）
/// - 不取更高阈值（如 5、10）是因为 share-sync 订阅常配"小目录"（3-5 个子文件），
///   用户仍希望享受整批
const MIN_BATCH_SIZE: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetdiskTargetEntry {
    pub path: String,
    pub name: String,
    pub fs_id: u64,
    pub is_dir: bool,
}

/// 抽象 TransferManager / DownloadManager 调用，便于单测 mock
#[async_trait]
pub trait ExecutorHooks: Send + Sync {
    /// 提交一个转存任务（单文件），返回 task_id
    async fn submit_transfer(
        &self,
        captured: &CapturedShare,
        target_dir: &str,
        item: &ShareSnapshotItem,
        internal_label: Option<&str>,
    ) -> Result<String, ShareSyncError>;

    /// 查询网盘目标路径是否已存在。
    async fn find_netdisk_file(
        &self,
        target_path: &str,
    ) -> Result<Option<NetdiskTargetEntry>, ShareSyncError>;

    /// 重命名网盘目标文件，返回重命名后的完整路径。
    async fn rename_netdisk(
        &self,
        path: &str,
        fs_id: u64,
        new_name: &str,
    ) -> Result<String, ShareSyncError>;

    /// 提交一个下载任务（单文件），返回 task_id
    async fn submit_download(
        &self,
        item: &ShareSnapshotItem,
        local_dir: &Path,
        strategy: ConflictStrategy,
    ) -> Result<String, ShareSyncError>;

    /// 等待转存任务进入业务所需的终态
    async fn wait_transfer_task(
        &self,
        task_id: &str,
        require_download_completion: bool,
        timeout: Duration,
    ) -> Result<(), ShareSyncError>;

    /// 删除网盘上的文件（按路径）
    async fn delete_netdisk(
        &self,
        target_path: &str,
        relative_paths: &[String],
    ) -> Result<(), ShareSyncError>;

    /// 删除本地文件
    fn delete_local(&self, local_dir: &Path, relative_path: &str) -> Result<(), ShareSyncError>;

    // ============================================================
    // v1 新增：整批 submit（"按目录根分组"路径使用）
    // ============================================================
    //
    // 设计要点：
    // - 默认实现返回 `InternalError`，让不打算支持批量的实现（如早期 mock）
    //   不被破坏，调用方需在 `apply_with_run_id_grouped` 路径里显式 override
    // - `submit_transfer_batch` / `submit_download_batch` 的语义与单文件版本一致，
    //   区别仅在于一次 `TransferManager.create_task` 调用里打包多个 `selected_files`
    //   与多个 `fs_id`，由 transfer 内部 `group_files_by_parent_dir` 按父目录分
    //   batch 转存，从而在大目录场景下把 N 次 transfer 任务合并为 1 次
    // - batch 失败时**整组视为失败**（v1 简化策略；组内任一文件失败 → 整组记
    //   `RunItemStatus::Failed` + 早停 quota/local_disk 早停时记 `Skipped`）

    /// 整组转存到网盘目标（一次 TransferManager.create_task 传多个 selected_files）
    ///
    /// 默认实现返回错误。如 share-sync 在订阅包含 ≥ `MIN_BATCH_SIZE` 个文件时
    /// 想走整批路径，生产实现必须 override 此方法。
    async fn submit_transfer_batch(
        &self,
        _captured: &CapturedShare,
        _target_dir: &str,
        _items: &[ShareSnapshotItem],
        _internal_label: Option<&str>,
    ) -> Result<String, ShareSyncError> {
        Err(ShareSyncError::Internal(
            "submit_transfer_batch 未实现".to_string(),
        ))
    }

    /// 整组下载到本地（一次 TransferManager.create_task 传多个 selected_files）
    ///
    /// 与 `submit_download` 单文件版本走同一条 transfer 流程（is_share_direct_download=true），
    /// 区别仅在 selected_files 是整组。
    async fn submit_download_batch(
        &self,
        _items: &[ShareSnapshotItem],
        _local_dir: &Path,
        _strategy: ConflictStrategy,
    ) -> Result<String, ShareSyncError> {
        Err(ShareSyncError::Internal(
            "submit_download_batch 未实现".to_string(),
        ))
    }
}

/// 一次同步运行的执行结果
pub struct ApplyOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub diff_summary: DiffSummary,
    pub error: Option<String>,
}

/// 同步执行器
pub struct ShareSyncExecutor<'a> {
    subscription: &'a ShareSubscription,
    persistence: &'a ShareSyncPersistence,
    hooks: &'a dyn ExecutorHooks,
}

impl<'a> ShareSyncExecutor<'a> {
    pub fn new(
        subscription: &'a ShareSubscription,
        persistence: &'a ShareSyncPersistence,
        hooks: &'a dyn ExecutorHooks,
    ) -> Self {
        Self {
            subscription,
            persistence,
            hooks,
        }
    }

    /// 应用一次 diff 到所有目标
    pub async fn apply(&self, captured: &CapturedShare, diff: &ShareDiff) -> ApplyOutcome {
        let run_id = Uuid::new_v4().to_string();
        self.apply_with_run_id(run_id, captured, diff).await
    }

    /// 应用一次 diff 到所有目标（使用调用方提供的 run_id，便于事件与持久化一致）
    pub async fn apply_with_run_id(
        &self,
        run_id: String,
        captured: &CapturedShare,
        diff: &ShareDiff,
    ) -> ApplyOutcome {
        let started_at = Utc::now().timestamp();
        if let Err(e) = self
            .persistence
            .start_run(&run_id, &self.subscription.id, started_at)
        {
            return ApplyOutcome {
                run_id,
                status: RunStatus::Failed,
                diff_summary: DiffSummary::default(),
                error: Some(format!("启动 run 失败: {}", e)),
            };
        }

        let mut summary = DiffSummary::default();
        seed_diff_summary(&mut summary, diff);
        let error: Option<String> = None;
        let mut any_failure = false;
        // 资源类错误（quota / local_disk_full）单独计数——
        // 它们**不算"业务失败"**，run 状态仍可保持 Completed（仅当没有其它失败时）。
        // 触发原因记录在最后一个被跳过的 item 的 error 字段里，供前端展示。
        let mut any_quota_skip = false;
        let mut last_quota_skip_msg: Option<String> = None;

        // 处理 added
        for item in &diff.added {
            if item.is_dir {
                continue;
            }
            for target in &self.subscription.targets {
                let _ = self
                    .process_added_or_modified(
                        captured,
                        run_id.as_str(),
                        item,
                        SyncAction::Added,
                        target,
                        &mut summary,
                    )
                    .await
                    .map_err(|e| {
                        // 资源类（quota / local_disk_full）→ 单独计数，
                        // **不**算作"业务失败"，避免把整次 run 标 CompletedWithErrors
                        if matches!(
                            e.category(),
                            ErrorCategory::Quota | ErrorCategory::LocalDiskFull
                        ) {
                            any_quota_skip = true;
                            last_quota_skip_msg = Some(e.user_message());
                        } else {
                            any_failure = true;
                        }
                        warn!("added 处理失败: path={}, err={}", item.path, e);
                    });
            }
            summary.added += 1;
        }

        // 处理 modified
        for ShareModifiedItem { old: _, new } in &diff.modified {
            if new.is_dir {
                continue;
            }
            for target in &self.subscription.targets {
                let _ = self
                    .process_added_or_modified(
                        captured,
                        run_id.as_str(),
                        new,
                        SyncAction::Modified,
                        target,
                        &mut summary,
                    )
                    .await
                    .map_err(|e| {
                        if matches!(
                            e.category(),
                            ErrorCategory::Quota | ErrorCategory::LocalDiskFull
                        ) {
                            any_quota_skip = true;
                            last_quota_skip_msg = Some(e.user_message());
                        } else {
                            any_failure = true;
                        }
                        warn!("modified 处理失败: path={}, err={}", new.path, e);
                    });
            }
            summary.modified += 1;
        }

        // 处理 removed
        for item in &diff.removed {
            if item.is_dir {
                continue;
            }
            if !self.subscription.delete_missing {
                for target in &self.subscription.targets {
                    let target_kind = match target {
                        SyncTarget::Netdisk(_) => TargetKind::Netdisk,
                        SyncTarget::Local(_) => TargetKind::Local,
                    };
                    let _ = self.persistence.add_run_item(
                        &run_id,
                        &item.path,
                        SyncAction::Removed,
                        target_kind,
                        None,
                        None,
                        RunItemStatus::Skipped,
                        None,
                        None,
                    );
                    summary.skipped += 1;
                }
                summary.removed += 1; // 也计入 removed（虽然 skipped）
                continue;
            }
            for target in &self.subscription.targets {
                let _ = self
                    .process_removed(run_id.as_str(), item, target, &mut summary)
                    .await
                    .map_err(|e| {
                        if matches!(
                            e.category(),
                            ErrorCategory::Quota | ErrorCategory::LocalDiskFull
                        ) {
                            any_quota_skip = true;
                            last_quota_skip_msg = Some(e.user_message());
                        } else {
                            any_failure = true;
                        }
                        warn!("removed 处理失败: path={}, err={}", item.path, e);
                    });
            }
            summary.removed += 1;
        }

        // 决定 run 终态：
        // - 有真实业务失败（any_failure）→ CompletedWithErrors
        // - 仅 quota / local_disk 跳过（资源类）→ Completed（run 业务上成功；
        //   summary.skipped 体现被跳过的子项数；error 字段给前端展示原因）
        // - 全成功 → Completed
        // v1.2: 先做"超时误判恢复" ——
        // 把"wait 超时但文件已落盘"的 failed 项升级为 completed,再重算 summary
        recover_local_run_items(
            &self.persistence,
            self.subscription,
            &run_id,
            &mut summary,
        );
        reconcile_summary_from_run_items(&self.persistence, &run_id, &mut summary);
        // 用 summary.failed 而非 any_failure 判定 ——
        // recovery 后 summary.failed 可能归零,此时应降级回 Completed
        let status = if summary.failed > 0 {
            RunStatus::CompletedWithErrors
        } else {
            RunStatus::Completed
        };
        // run.error：仅当**只有 quota 跳过 + 没有任何真实失败**时，把消息写到
        // outcome.error（前端会把它当作"信息性提示"展示，不会把整次 run 标红）。
        let run_error = if any_quota_skip && !any_failure {
            last_quota_skip_msg
        } else {
            error
        };
        let finished_at = Utc::now().timestamp();
        if let Err(e) = self.persistence.finish_run(
            &run_id,
            finished_at,
            status,
            &summary,
            run_error.as_deref(),
        ) {
            warn!("finish_run 失败: {}", e);
        }

        ApplyOutcome {
            run_id,
            status,
            diff_summary: summary,
            error: run_error,
        }
    }

    // ============================================================
    // v1 新增：整批 submit 路径
    // ============================================================
    //
    // 适用场景：订阅指向**文件夹**时（include_paths 选中目录，或整个分享根是目录），
    // 把 added/modified 项按"目录根"分组，每组整批 submit 一次 TransferManager，
    // 由 transfer 内部 `group_files_by_parent_dir`（见
    // `backend/src/transfer/manager.rs:4349`）按父目录分 batch 转存。
    //
    // 关键设计：
    // - **不动 removed 流程**（仍走 `process_removed` 单条粒度，与 v1 范围一致）
    // - **不动 is_dir 项**：executor 维持 `if item.is_dir { continue; }` skip 目录，
    //   避免和子文件 fs_id 重复转存
    // - **批触发条件**：`grouped.len() >= MIN_BATCH_SIZE` 才走整批；否则退化为
    //   单文件 submit（走 `apply_with_run_id` 路径）
    // - **Quota/LocalDiskFull 早停**：batch 内首条失败若归为这两类，立即 break
    //   后续未提交项，标 `Skipped` + `reason = "skip_due_to_quota_full"` /
    //   `"skip_due_to_local_disk_full"`，run 标 `Failed`（不推进基线但显式区分）

    /// 整批 submit 入口
    ///
    /// 调用方（`ShareSyncManager::execute_one`）可选择走此路径；
    /// 当前实现**在 manager.rs 仍调 `apply_with_run_id` 单文件路径**,
    /// 此方法作为"v1 已就绪、未来调度可切换"的备选入口存在。
    #[allow(unused_assignments, unused_variables)] // any_failure 在 v1.2 之后被 summary.failed 取代终态判定
    pub async fn apply_with_run_id_grouped(
        &self,
        run_id: String,
        captured: &CapturedShare,
        diff: &ShareDiff,
    ) -> ApplyOutcome {
        let started_at = Utc::now().timestamp();
        if let Err(e) = self
            .persistence
            .start_run(&run_id, &self.subscription.id, started_at)
        {
            return ApplyOutcome {
                run_id,
                status: RunStatus::Failed,
                diff_summary: DiffSummary::default(),
                error: Some(format!("启动 run 失败: {}", e)),
            };
        }

        let mut summary = DiffSummary::default();
        seed_diff_summary(&mut summary, diff);
        let mut any_failure = false;
        let mut run_failure_reason: Option<&'static str> = None;

        // 1) 把 added + modified.new 合并为"待处理候选"
        let mut candidates: Vec<(SyncAction, ShareSnapshotItem)> =
            Vec::with_capacity(diff.added.len() + diff.modified.len());
        for item in &diff.added {
            if item.is_dir {
                continue;
            }
            candidates.push((SyncAction::Added, item.clone()));
        }
        for ShareModifiedItem { old: _, new } in &diff.modified {
            if new.is_dir {
                continue;
            }
            candidates.push((SyncAction::Modified, new.clone()));
        }

        // 2) 按"目录根"分组（纯函数 group_by_dir_root）
        let groups = group_by_dir_root(&candidates, &self.subscription.include_paths);

        // 3) 处理每组
        for (root_path, group) in &groups {
            // 批触发：组内文件数 >= MIN_BATCH_SIZE 才走整批 submit
            if group.len() < MIN_BATCH_SIZE {
                // 退化：对组内每条仍走 `process_added_or_modified` 复用现有逻辑
                for (action, item) in group {
                    for target in &self.subscription.targets {
                        if let Err(e) = self
                            .process_added_or_modified(
                                captured,
                                run_id.as_str(),
                                item,
                                *action,
                                target,
                                &mut summary,
                            )
                            .await
                        {
                            any_failure = true;
                            run_failure_reason =
                                update_run_failure_reason(run_failure_reason, e.category());
                            warn!("added/modified 处理失败: path={}, err={}", item.path, e);
                        }
                    }
                    if matches!(action, SyncAction::Added) {
                        summary.added += 1;
                    } else {
                        summary.modified += 1;
                    }
                }
                continue;
            }

            // 整批 submit：对每个 target 各 submit 一次（multi-target 时仍是 N 次 submit，
            // 但组内 N 个文件合成 1 次 TransferManager.create_task）
            for target in &self.subscription.targets {
                let items: Vec<ShareSnapshotItem> =
                    group.iter().map(|(_, item)| item.clone()).collect();

                // 取 action（组内 action 应当一致；如果混了 added/modified，按 Added 处理）
                let action = group
                    .iter()
                    .map(|(a, _)| *a)
                    .find(|a| matches!(a, SyncAction::Added))
                    .unwrap_or(SyncAction::Modified);

                let strategy =
                    target.effective_conflict_strategy(self.subscription.conflict_strategy);
                let target_kind = match target {
                    SyncTarget::Netdisk(_) => TargetKind::Netdisk,
                    SyncTarget::Local(_) => TargetKind::Local,
                };
                // 把 label String 绑到栈变量，避免 .as_str() 返回的 &str 在 await 期间悬空
                let internal_label =
                    format!("share-sync/{}/batch/{}", self.subscription.id, run_id);

                // 提交 batch
                let submit_result: Result<String, ShareSyncError> = match target {
                    SyncTarget::Netdisk(t) => {
                        self.hooks
                            .submit_transfer_batch(
                                captured,
                                &t.remote_path,
                                &items,
                                Some(&internal_label),
                            )
                            .await
                    }
                    SyncTarget::Local(t) => {
                        self.hooks
                            .submit_download_batch(&items, &t.local_path, strategy)
                            .await
                    }
                };

                match submit_result {
                    Ok(task_id) => {
                        // 整组成功：每条 item 记一条 run_item（共享 task_id）
                        for item in &items {
                            let _ = self.persistence.add_run_item(
                                &run_id,
                                &item.path,
                                action,
                                target_kind,
                                Some(task_id.as_str()),
                                None,
                                RunItemStatus::Transferring,
                                None,
                                None,
                            );
                        }
                        // 等待整组完成
                        let wait_result = self
                            .hooks
                            .wait_transfer_task(
                                &task_id,
                                matches!(target, SyncTarget::Local(_)),
                                TASK_WAIT_TIMEOUT,
                            )
                            .await;
                        if let Err(e) = wait_result {
                            any_failure = true;
                            let category = e.category();
                            run_failure_reason =
                                update_run_failure_reason(run_failure_reason, category);
                            warn!(
                                "batch submit 失败: root={}, items={}, err={}",
                                root_path,
                                items.len(),
                                e
                            );

                            // v1 新增：Quota / LocalDiskFull → 退化为**逐文件 submit**。
                            // 原因：整批 submit 把 N 个文件合并到一次 baidu 鉴权 + 转存里，
                            // 当网盘空间不足时整批会全失败；而逐文件 submit 时小文件
                            // 仍能转存成功（直到累计占满剩余空间），只把真正放不下的
                            // 文件标 Skipped。回退后**不再早停后续 group**（剩余空间
                            // 可能还放得下后面更小的目录），整次 run 仍可推进。
                            if matches!(
                                category,
                                ErrorCategory::Quota | ErrorCategory::LocalDiskFull
                            ) {
                                let skip_reason = match category {
                                    ErrorCategory::Quota => "quota_full",
                                    ErrorCategory::LocalDiskFull => "local_disk_full",
                                    _ => unreachable!(),
                                };
                                info!(
                                    "batch 失败因 {} → 退化为逐文件 submit, root={}, items={}",
                                    skip_reason,
                                    root_path,
                                    items.len()
                                );
                                // 之前的"batch 成功插的 run_item"留着不删（add_run_item
                                // 多次插入会有重复行；持久化按 (run_id, path) UNIQUE
                                // 的话只会保留首次）。回退时再走 process_added_or_modified
                                // 会在 add_run_item 阶段报 UNIQUE conflict — 故这里
                                // 直接复用 v1 单文件路径的事件流：不再插入新 run_item，
                                // 让 process_added_or_modified 重新插入。
                                for (item_action, item) in group {
                                    if let Err(e2) = self
                                        .process_added_or_modified(
                                            captured,
                                            run_id.as_str(),
                                            item,
                                            *item_action,
                                            target,
                                            &mut summary,
                                        )
                                        .await
                                    {
                                        // 单文件回退又失败的——`process_added_or_modified`
                                        // 内部已经按 quota 标 Skipped / 其它标 Failed，
                                        // 这里只更新 any_failure 与 run_failure_reason。
                                        any_failure = true;
                                        run_failure_reason = update_run_failure_reason(
                                            run_failure_reason,
                                            e2.category(),
                                        );
                                    }
                                }
                                // 不再 break 外层 group 循环——继续尝试其它 group（用
                                // 单文件路径，因为 batch 路径已经踩到 quota 边界）
                                continue;
                            }

                            // 其它失败：保持原行为——整组标 Failed
                            for item in &items {
                                summary.failed += 1;
                                let _ = self.persistence.add_run_item(
                                    &run_id,
                                    &item.path,
                                    action,
                                    target_kind,
                                    Some(task_id.as_str()),
                                    None,
                                    RunItemStatus::Failed,
                                    None,
                                    None,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        any_failure = true;
                        let category = e.category();
                        run_failure_reason =
                            update_run_failure_reason(run_failure_reason, category);
                        warn!(
                            "batch submit 提交阶段失败: root={}, items={}, err={}",
                            root_path,
                            items.len(),
                            e
                        );

                        // v1 新增：Quota / LocalDiskFull → 同样退化为逐文件 submit
                        if matches!(
                            category,
                            ErrorCategory::Quota | ErrorCategory::LocalDiskFull
                        ) {
                            info!(
                                "batch submit 阶段 quota 失败 → 退化为逐文件 submit, root={}, items={}",
                                root_path,
                                items.len()
                            );
                            for (item_action, item) in group {
                                if let Err(e2) = self
                                    .process_added_or_modified(
                                        captured,
                                        run_id.as_str(),
                                        item,
                                        *item_action,
                                        target,
                                        &mut summary,
                                    )
                                    .await
                                {
                                    any_failure = true;
                                    run_failure_reason = update_run_failure_reason(
                                        run_failure_reason,
                                        e2.category(),
                                    );
                                }
                            }
                            continue;
                        }

                        // 提交阶段其它失败：整组标 Failed
                        for item in &items {
                            summary.failed += 1;
                            let _ = self.persistence.add_run_item(
                                &run_id,
                                &item.path,
                                action,
                                target_kind,
                                None,
                                None,
                                RunItemStatus::Failed,
                                None,
                                None,
                            );
                        }
                    }
                }
            }
        }

        // 4) removed 流程（与 apply_with_run_id 保持一致）
        for item in &diff.removed {
            if item.is_dir {
                continue;
            }
            if !self.subscription.delete_missing {
                for target in &self.subscription.targets {
                    let target_kind = match target {
                        SyncTarget::Netdisk(_) => TargetKind::Netdisk,
                        SyncTarget::Local(_) => TargetKind::Local,
                    };
                    let _ = self.persistence.add_run_item(
                        &run_id,
                        &item.path,
                        SyncAction::Removed,
                        target_kind,
                        None,
                        None,
                        RunItemStatus::Skipped,
                        None,
                        None,
                    );
                    summary.skipped += 1;
                }
                summary.removed += 1;
                continue;
            }
            for target in &self.subscription.targets {
                if let Err(e) = self
                    .process_removed(run_id.as_str(), item, target, &mut summary)
                    .await
                {
                    any_failure = true;
                    run_failure_reason =
                        update_run_failure_reason(run_failure_reason, e.category());
                    warn!("removed 处理失败: path={}, err={}", item.path, e);
                }
            }
            summary.removed += 1;
        }

        // 5) 决定 run 终态（v1.1：quota 不再算 Failed）
        // - 有非资源类失败（any_failure）→ CompletedWithErrors
        // - 仅 quota / local_disk_full 跳过 → Completed（业务上成功；被跳过的子项
        //   在 summary.skipped 体现，error 字段给前端展示原因）
        // - 全成功 → Completed
        // v1.2: 同 apply_with_run_id,先做"超时误判恢复"再 reconcile
        recover_local_run_items(
            &self.persistence,
            self.subscription,
            &run_id,
            &mut summary,
        );
        reconcile_summary_from_run_items(&self.persistence, &run_id, &mut summary);
        let quota_only = matches!(
            run_failure_reason,
            Some("quota_full") | Some("local_disk_full")
        );
        let status = if summary.failed > 0 && !quota_only {
            RunStatus::CompletedWithErrors
        } else {
            RunStatus::Completed
        };

        let error = if quota_only {
            run_failure_reason.map(|r| match r {
                "quota_full" => "网盘空间不足，本批次未提交的子项已标记为跳过".to_string(),
                "local_disk_full" => "本地磁盘空间不足，请清理订阅目标目录".to_string(),
                _ => "未知失败原因".to_string(),
            })
        } else {
            None
        };

        let finished_at = Utc::now().timestamp();
        if let Err(e) =
            self.persistence
                .finish_run(&run_id, finished_at, status, &summary, error.as_deref())
        {
            warn!("finish_run 失败: {}", e);
        }

        ApplyOutcome {
            run_id,
            status,
            diff_summary: summary,
            error,
        }
    }

    // ============================================================
    // v2 阶段 3:按 tree 顶层节点整体提交
    // ============================================================
    //
    // 与 `apply_with_run_id_grouped` 的关键差异:
    // - grouped 仍是"文件粒度"分组(按文件的父目录),目录 fs_id 被硬过滤掉
    // - 这里走"按 tree 顶层节点提交"——顶层是目录就把目录 fs_id 整体发一次 transfer,
    //   百度服务端递归把整目录搬走;顶层是散文件就单独发
    //
    // 阶段 3 的此入口**不带 quota 二分**(失败统一标 Failed),仅做"目录整体转存"
    // 的能力切换;quota 二分递归留给阶段 4 在 `process_subtree` 内部加。
    //
    // 不动 removed 流程(沿用 `apply_with_run_id_grouped` 的 removed 段)。
    #[allow(unused_assignments, unused_variables)] // any_failure 在 v1.2 之后被 summary.failed 取代终态判定
    pub async fn apply_with_run_id_tree(
        &self,
        run_id: String,
        captured: &CapturedShare,
        diff: &ShareDiff,
    ) -> ApplyOutcome {
        use crate::share_sync::tree;

        let started_at = Utc::now().timestamp();
        if let Err(e) = self
            .persistence
            .start_run(&run_id, &self.subscription.id, started_at)
        {
            return ApplyOutcome {
                run_id,
                status: RunStatus::Failed,
                diff_summary: DiffSummary::default(),
                error: Some(format!("启动 run 失败: {}", e)),
            };
        }

        let mut summary = DiffSummary::default();
        seed_diff_summary(&mut summary, diff);
        let mut any_failure = false;
        let mut run_failure_reason: Option<&'static str> = None;

        // 1) 合并 added + modified 项(**保留 is_dir**——让 tree 重建出目录骨架,
        //    然后我们才能用目录 fs_id 整体提交)
        let mut items: Vec<ShareSnapshotItem> =
            Vec::with_capacity(diff.added.len() + diff.modified.len());
        items.extend(diff.added.iter().cloned());
        items.extend(diff.modified.iter().map(|m| m.new.clone()));

        // 2) 重建树(虚拟根的 children 就是顶层节点)
        let t = tree::build(&items);

        // 3) 对每个顶层节点 × 每个 target 调 process_subtree
        let top_nodes: Vec<usize> = t.get(t.root).children.clone();
        info!(
            "share_sync_tree_apply: run_id={} top_nodes={} subscription={}",
            run_id,
            top_nodes.len(),
            self.subscription.id
        );

        // v2 阶段 5:并行调度。决策矩阵:
        // - BAIDUPCS_BISECT_PARALLEL=0 → 串行(等同阶段 4 行为)
        // - 否则:并发上限 = subscription.max_concurrent_transfers
        //                 ?? env BAIDUPCS_BISECT_CONCURRENCY ?? 默认 4
        //
        // 实现选型用 futures::stream::buffer_unordered 而不是 tokio::spawn:
        // - 不需要把 hooks 包成 Arc<dyn ExecutorHooks>(改面太大)
        // - 同一 task 内并发 N 个 future, IO-bound 场景与 spawn 性能相当
        // - 借用 &self / &captured / &t 不需要 'static
        //
        // 每个并发任务持有独立 DiffSummary, 跑完后串行 fold 到主 summary,
        // 避免共享可变状态。`process_subtree` 内部走 ShareSyncPersistence,
        // 后者用 Mutex<Connection> 串行化 SQLite 写入, 多 future 并发安全。
        let bisect_parallel_enabled = std::env::var("BAIDUPCS_BISECT_PARALLEL")
            .ok()
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);
        let concurrency: usize = self
            .subscription
            .max_concurrent_transfers
            .map(|n| n as usize)
            .or_else(|| {
                std::env::var("BAIDUPCS_BISECT_CONCURRENCY")
                    .ok()
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(4)
            .max(1);
        let n_targets = self.subscription.targets.len();
        // 拼工作单元: (top_node_idx, target_idx)
        let work: Vec<(usize, usize)> = top_nodes
            .iter()
            .flat_map(|&n| (0..n_targets).map(move |ti| (n, ti)))
            .collect();

        let results: Vec<(DiffSummary, Result<(), ErrorCategory>)> =
            if bisect_parallel_enabled && concurrency > 1 && work.len() > 1 {
                use futures::stream::{self, StreamExt};
                info!(
                    "share_sync_parallel: run_id={} work_units={} concurrency={}",
                    run_id,
                    work.len(),
                    concurrency
                );
                let t_ref = &t;
                let captured_ref = captured;
                let run_id_ref = run_id.as_str();
                stream::iter(work.into_iter())
                    .map(|(node_idx, target_idx)| {
                        let target = &self.subscription.targets[target_idx];
                        async move {
                            let mut local = DiffSummary::default();
                            let res = self
                                .process_subtree(
                                    captured_ref,
                                    run_id_ref,
                                    t_ref,
                                    node_idx,
                                    target,
                                    &mut local,
                                )
                                .await;
                            (local, res)
                        }
                    })
                    .buffer_unordered(concurrency)
                    .collect()
                    .await
            } else {
                let mut out = Vec::with_capacity(work.len());
                for (node_idx, target_idx) in work {
                    let target = &self.subscription.targets[target_idx];
                    let mut local = DiffSummary::default();
                    let res = self
                        .process_subtree(
                            captured,
                            run_id.as_str(),
                            &t,
                            node_idx,
                            target,
                            &mut local,
                        )
                        .await;
                    out.push((local, res));
                }
                out
            };

        // 聚合: process_subtree 只会改 failed/skipped/overwritten 这几个增量字段,
        // 其它字段(added/modified/removed/unchanged/total)由下方第 4 步从 diff 直接算。
        for (local, res) in results {
            summary.failed += local.failed;
            summary.skipped += local.skipped;
            summary.overwritten += local.overwritten;
            if let Err(category) = res {
                any_failure = true;
                run_failure_reason = update_run_failure_reason(run_failure_reason, category);
            }
        }

        // 4) added/modified 计数(沿用旧逻辑:目录条目不计入文件数,但 process_subtree
        //    内部已经把叶子文件级 run_item 都记上了)
        summary.added = diff.added.iter().filter(|i| !i.is_dir).count();
        summary.modified = diff.modified.iter().filter(|i| !i.new.is_dir).count();

        // 5) removed 流程(完全复用 grouped 路径行为)
        for item in &diff.removed {
            if item.is_dir {
                continue;
            }
            if !self.subscription.delete_missing {
                for target in &self.subscription.targets {
                    let target_kind = match target {
                        SyncTarget::Netdisk(_) => TargetKind::Netdisk,
                        SyncTarget::Local(_) => TargetKind::Local,
                    };
                    let _ = self.persistence.add_run_item(
                        &run_id,
                        &item.path,
                        SyncAction::Removed,
                        target_kind,
                        None,
                        None,
                        RunItemStatus::Skipped,
                        None,
                        None,
                    );
                    summary.skipped += 1;
                }
                summary.removed += 1;
                continue;
            }
            for target in &self.subscription.targets {
                if let Err(e) = self
                    .process_removed(run_id.as_str(), item, target, &mut summary)
                    .await
                {
                    any_failure = true;
                    run_failure_reason =
                        update_run_failure_reason(run_failure_reason, e.category());
                    warn!("removed 处理失败: path={}, err={}", item.path, e);
                }
            }
            summary.removed += 1;
        }

        // 6) run 终态(沿用 grouped 路径语义:仅 quota 不算 Failed)
        // v1.2: 同上,先做"超时误判恢复"再 reconcile
        recover_local_run_items(
            &self.persistence,
            self.subscription,
            &run_id,
            &mut summary,
        );
        reconcile_summary_from_run_items(&self.persistence, &run_id, &mut summary);
        let quota_only = matches!(
            run_failure_reason,
            Some("quota_full") | Some("local_disk_full")
        );
        let status = if summary.failed > 0 && !quota_only {
            RunStatus::CompletedWithErrors
        } else {
            RunStatus::Completed
        };
        let error = if quota_only {
            run_failure_reason.map(|r| match r {
                "quota_full" => "网盘空间不足,部分子项已跳过".to_string(),
                "local_disk_full" => "本地磁盘空间不足".to_string(),
                _ => "未知失败原因".to_string(),
            })
        } else {
            None
        };

        let finished_at = Utc::now().timestamp();
        if let Err(e) =
            self.persistence
                .finish_run(&run_id, finished_at, status, &summary, error.as_deref())
        {
            warn!("finish_run 失败: {}", e);
        }

        ApplyOutcome {
            run_id,
            status,
            diff_summary: summary,
            error,
        }
    }

    /// 处理一棵子树(以 `node_idx` 为根)
    ///
    /// 阶段 4: 失败时若错误是 `is_bisect_trigger()`(Quota / LocalDiskFull /
    /// DirTransferAmbiguous),触发二分递归 — split_two 把 children 切两半,
    /// 各自调用 transfer_node_set 重试;深度上限 32,触顶直接 Failed。
    ///
    /// 入口方法,默认 depth=0;真正的递归在 `transfer_node_set` 里。
    async fn process_subtree(
        &self,
        captured: &CapturedShare,
        run_id: &str,
        tree: &crate::share_sync::tree::Tree,
        node_idx: usize,
        target: &SyncTarget,
        summary: &mut DiffSummary,
    ) -> Result<(), ErrorCategory> {
        self.transfer_node_set(captured, run_id, tree, vec![node_idx], target, summary, 0)
            .await
    }

    /// 提交一组节点(可能是 1 个目录、N 个散文件、混合)的 transfer
    ///
    /// 这是阶段 4 二分递归的核心。
    /// - 把 indices 拼成 items 一次性 submit_transfer_batch / submit_download_batch
    /// - 失败 + is_bisect_trigger + depth < `BISECT_MAX_DEPTH`:
    ///   - len(indices) > 1: 用 `split_indices_two` 对半切, 递归两次
    ///   - len == 1 且节点有 children: 用 `split_two` 切节点的子节点, 递归两次
    ///   - len == 1 且节点是叶子: 标 Skipped/Failed(无法再拆)
    /// - 失败但**非** bisect 触发: 整组按 Failed/Skipped(quota_full/local_disk_full) 标记
    ///
    /// 由 BAIDUPCS_BISECT_ENABLED env flag 控制(默认开)。flag 关时退化为"失败
    /// 直接标 Failed",等价于阶段 3 行为。
    #[async_recursion::async_recursion]
    async fn transfer_node_set(
        &self,
        captured: &CapturedShare,
        run_id: &str,
        tree: &'async_recursion crate::share_sync::tree::Tree,
        indices: Vec<usize>,
        target: &SyncTarget,
        summary: &mut DiffSummary,
        depth: u32,
    ) -> Result<(), ErrorCategory> {
        use crate::share_sync::tree as tree_mod;

        if indices.is_empty() {
            return Ok(());
        }
        let items_to_submit = tree_mod::nodes_to_items(tree, &indices);
        if items_to_submit.is_empty() {
            // 全是 placeholder — 降级按叶子提交
            let mut worst: Option<ErrorCategory> = None;
            for idx in &indices {
                if let Err(c) = self
                    .submit_subtree_as_leaves(captured, run_id, tree, *idx, target, summary)
                    .await
                {
                    worst = Some(worst.map_or(c, |w| max_category(w, c)));
                }
            }
            return worst.map_or(Ok(()), Err);
        }
        let target_kind = match target {
            SyncTarget::Netdisk(_) => TargetKind::Netdisk,
            SyncTarget::Local(_) => TargetKind::Local,
        };
        let strategy = target.effective_conflict_strategy(self.subscription.conflict_strategy);
        let internal_label =
            format!("share-sync/{}/tree/d{}/{}", self.subscription.id, depth, run_id);

        let first_path = tree.get(indices[0]).path.clone();
        info!(
            "share_sync_submit_batch: run_id={} depth={} n_nodes={} first_path={} target={:?}",
            run_id,
            depth,
            indices.len(),
            first_path,
            target_kind
        );

        let submit_result: Result<String, ShareSyncError> = match target {
            SyncTarget::Netdisk(t) => {
                self.hooks
                    .submit_transfer_batch(
                        captured,
                        &t.remote_path,
                        &items_to_submit,
                        Some(&internal_label),
                    )
                    .await
            }
            SyncTarget::Local(t) => {
                self.hooks
                    .submit_download_batch(&items_to_submit, &t.local_path, strategy)
                    .await
            }
        };

        let final_err: ShareSyncError = match submit_result {
            Ok(task_id) => {
                let require_download_completion = matches!(target, SyncTarget::Local(_));
                let wait_res = self
                    .hooks
                    .wait_transfer_task(&task_id, require_download_completion, TASK_WAIT_TIMEOUT)
                    .await;
                match wait_res {
                    Ok(()) => {
                        // 成功:把所有 indices 子树叶子标 Completed
                        let mut all_leaves: Vec<usize> = Vec::new();
                        for &idx in &indices {
                            all_leaves.extend(tree.descendants_leaves(idx));
                        }
                        for leaf_idx in all_leaves {
                            let leaf = tree.get(leaf_idx);
                            let _ = self.persistence.add_run_item(
                                run_id,
                                &leaf.path,
                                SyncAction::Added,
                                target_kind,
                                Some(task_id.as_str()),
                                None,
                                RunItemStatus::Completed,
                                None,
                                None,
                            );
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(
                            "share_sync_wait_failed: run_id={} depth={} first_path={} task_id={} err={}",
                            run_id, depth, first_path, task_id, e
                        );
                        e
                    }
                }
            }
            Err(e) => {
                warn!(
                    "share_sync_submit_failed: run_id={} depth={} first_path={} err={}",
                    run_id, depth, first_path, e
                );
                e
            }
        };

        // 失败处理:判断是否触发二分
        let category = final_err.category();
        let bisect_enabled = std::env::var("BAIDUPCS_BISECT_ENABLED")
            .ok()
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);
        const BISECT_MAX_DEPTH: u32 = 32;

        if bisect_enabled && final_err.is_bisect_trigger() && depth < BISECT_MAX_DEPTH {
            // 决定怎么二分:
            // 1) indices 多个 → 把 indices 自己对半切
            // 2) indices 单个但有 children → 切节点的 children
            // 3) indices 单个且是叶子 → 无法再分,标 Skipped
            let bisect_groups: Vec<Vec<usize>> = if indices.len() > 1 {
                tree_mod::split_indices_two(tree, &indices)
            } else {
                tree_mod::split_two(tree, indices[0])
            };

            if !bisect_groups.is_empty() && bisect_groups.iter().any(|g| !g.is_empty()) {
                info!(
                    "share_sync_bisect_split: run_id={} depth={} from={} into={:?} reason={:?}",
                    run_id,
                    depth,
                    indices.len(),
                    bisect_groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
                    category
                );
                let mut worst: Option<ErrorCategory> = None;
                for group in bisect_groups {
                    if group.is_empty() {
                        continue;
                    }
                    if let Err(c) = self
                        .transfer_node_set(
                            captured,
                            run_id,
                            tree,
                            group,
                            target,
                            summary,
                            depth + 1,
                        )
                        .await
                    {
                        worst = Some(worst.map_or(c, |w| max_category(w, c)));
                    }
                }
                return worst.map_or(Ok(()), Err);
            }
            // 单叶子 + bisect_trigger → 拆不动了, 落到下方的"标记终态"分支(Skipped)
        }

        // 到这里:不二分,直接给所有叶子打终态
        let all_leaves: Vec<usize> = indices
            .iter()
            .flat_map(|&i| tree.descendants_leaves(i))
            .collect();
        // 把 final_err 的可读消息缓存一次,所有失败叶子共用 ——
        // 之前 marker 分支只 `add_run_item(... error 列填 None ...)`,
        // 导致前端详情里看不到任何错误原因,debug 不到 "wait 超时" 等关键信息。
        let err_msg = final_err.to_string();
        for leaf_idx in all_leaves {
            let leaf = tree.get(leaf_idx);
            let (status, reason) = match category {
                ErrorCategory::Quota => {
                    summary.skipped += 1;
                    (RunItemStatus::Skipped, Some("quota_full"))
                }
                ErrorCategory::LocalDiskFull => {
                    summary.skipped += 1;
                    (RunItemStatus::Skipped, Some("local_disk_full"))
                }
                ErrorCategory::DirTransferAmbiguous => {
                    // 已二分到叶子仍失败 → 当作 Failed(实际是该单文件出问题, 比如
                    // 被分享者删除); 暂不区分独立 reason, 走 Failed 让用户看到
                    summary.failed += 1;
                    (RunItemStatus::Failed, None)
                }
                _ => {
                    summary.failed += 1;
                    (RunItemStatus::Failed, None)
                }
            };
            match self.persistence.add_run_item(
                run_id,
                &leaf.path,
                SyncAction::Added,
                target_kind,
                None,
                None,
                status,
                None,
                reason,
            ) {
                Ok(row_id) => {
                    // 失败项把 error 列也写上,前端详情能展示,
                    // recover 阶段也能根据 message 判断是否"超时"等情况
                    if status == RunItemStatus::Failed {
                        if let Err(e) = self.persistence.update_run_item_status(
                            row_id,
                            RunItemStatus::Failed,
                            Some(&err_msg),
                        ) {
                            warn!(
                                "executor: 写入失败项 error 列失败: id={} err={}",
                                row_id, e
                            );
                        }
                    }
                }
                Err(e) => warn!("记录失败 run_item 失败: {}", e),
            }
        }
        Err(category)
    }

    /// 当 process_subtree 遇到 placeholder / 空 items 时,降级为按叶子(单文件)
    /// 提交。阶段 4 的二分递归到底也会复用这条路径。
    async fn submit_subtree_as_leaves(
        &self,
        captured: &CapturedShare,
        run_id: &str,
        tree: &crate::share_sync::tree::Tree,
        node_idx: usize,
        target: &SyncTarget,
        summary: &mut DiffSummary,
    ) -> Result<(), ErrorCategory> {
        let leaves = tree.descendants_leaves(node_idx);
        if leaves.is_empty() {
            return Ok(());
        }
        let mut worst: Option<ErrorCategory> = None;
        for leaf_idx in leaves {
            let leaf = tree.get(leaf_idx);
            let item = ShareSnapshotItem {
                path: leaf.path.clone(),
                raw_path: leaf.path.clone(),
                fs_id: leaf.fs_id,
                size: leaf.size,
                name: leaf.name.clone(),
                is_dir: leaf.is_dir,
            };
            if let Err(e) = self
                .process_added_or_modified(captured, run_id, &item, SyncAction::Added, target, summary)
                .await
            {
                worst = Some(worst.map_or(e.category(), |w| max_category(w, e.category())));
            }
        }
        match worst {
            None => Ok(()),
            Some(c) => Err(c),
        }
    }


    /// 处理 added/modified 文件
    async fn process_added_or_modified(
        &self,
        captured: &CapturedShare,
        run_id: &str,
        item: &ShareSnapshotItem,
        action: SyncAction,
        target: &SyncTarget,
        summary: &mut DiffSummary,
    ) -> Result<(), ShareSyncError> {
        let strategy = target.effective_conflict_strategy(self.subscription.conflict_strategy);
        let target_kind = match target {
            SyncTarget::Netdisk(_) => TargetKind::Netdisk,
            SyncTarget::Local(_) => TargetKind::Local,
        };
        let mut overwrote_existing = false;

        // 1) 按目标处理冲突策略。
        let mut versioned_old: Option<String> = None;
        match target {
            SyncTarget::Netdisk(t) => {
                let target_file_path = netdisk_target_file_path(t, item);
                let existing = match self.hooks.find_netdisk_file(&target_file_path).await {
                    Ok(existing) => existing,
                    Err(e) => {
                        self.record_failed_run_item(
                            summary,
                            run_id,
                            item,
                            action,
                            target_kind,
                            None,
                            &e,
                        );
                        return Err(e);
                    }
                };

                if existing.as_ref().is_some_and(|entry| entry.is_dir)
                    && strategy != ConflictStrategy::Skip
                {
                    let e = ShareSyncError::TransferError(format!(
                        "目标路径已存在同名目录，无法用文件覆盖: {}",
                        target_file_path
                    ));
                    self.record_failed_run_item(
                        summary,
                        run_id,
                        item,
                        action,
                        target_kind,
                        None,
                        &e,
                    );
                    return Err(e);
                }

                match (strategy, existing) {
                    (ConflictStrategy::Skip, Some(_)) => {
                        summary.skipped += 1;
                        self.persistence.add_run_item(
                            run_id,
                            &item.path,
                            action,
                            target_kind,
                            None,
                            None,
                            RunItemStatus::Skipped,
                            None,
                            None,
                        )?;
                        return Ok(());
                    }
                    (ConflictStrategy::Versioned, Some(existing)) => {
                        let new_name = timestamped_name(&existing.name);
                        match self
                            .hooks
                            .rename_netdisk(&existing.path, existing.fs_id, &new_name)
                            .await
                        {
                            Ok(new_path) => versioned_old = Some(new_path),
                            Err(e) => {
                                self.record_failed_run_item(
                                    summary,
                                    run_id,
                                    item,
                                    action,
                                    target_kind,
                                    None,
                                    &e,
                                );
                                return Err(e);
                            }
                        }
                    }
                    (ConflictStrategy::Overwrite, Some(existing)) => {
                        let delete_paths = vec![existing.path.clone()];
                        if let Err(e) = self
                            .hooks
                            .delete_netdisk(&t.remote_path, &delete_paths)
                            .await
                        {
                            self.record_failed_run_item(
                                summary,
                                run_id,
                                item,
                                action,
                                target_kind,
                                None,
                                &e,
                            );
                            return Err(e);
                        }
                        overwrote_existing = true;
                    }
                    _ => {}
                }
            }
            SyncTarget::Local(t) if strategy == ConflictStrategy::Overwrite => {
                if local_file_exists(&t.local_path, &item.path) {
                    overwrote_existing = true;
                }
            }
            SyncTarget::Local(_) if strategy == ConflictStrategy::Skip => {
                if let SyncTarget::Local(t) = target {
                    if local_file_exists(&t.local_path, &item.path) {
                        summary.skipped += 1;
                        self.persistence.add_run_item(
                            run_id,
                            &item.path,
                            action,
                            target_kind,
                            None,
                            None,
                            RunItemStatus::Skipped,
                            None,
                            None,
                        )?;
                        return Ok(());
                    }
                }
            }
            SyncTarget::Local(_) if strategy == ConflictStrategy::Versioned => {
                match self.versioned_local_old(target, item) {
                    Ok(old_path) if !old_path.is_empty() => versioned_old = Some(old_path),
                    Ok(_) => {}
                    Err(e) => {
                        self.record_failed_run_item(
                            summary,
                            run_id,
                            item,
                            action,
                            target_kind,
                            None,
                            &e,
                        );
                        return Err(e);
                    }
                }
            }
            _ => {}
        }

        let mut run_item_id: Option<i64> = None;
        let mut last_retry_error: Option<ShareSyncError> = None;

        for attempt in 1..=TASK_OPERATION_MAX_ATTEMPTS {
            // 2) 提交 transfer / download
            let result: Result<(String, bool, RunItemStatus), ShareSyncError> = match target {
                SyncTarget::Netdisk(t) => {
                    let target_dir = netdisk_target_parent_dir(t, item);
                    self.hooks
                        .submit_transfer(
                            captured,
                            &target_dir,
                            item,
                            Some(&format!("share-sync/{}/{}", self.subscription.id, run_id)),
                        )
                        .await
                        .map(|task_id| (task_id, false, RunItemStatus::Transferring))
                }
                SyncTarget::Local(t) => self
                    .hooks
                    .submit_download(item, &t.local_path, strategy)
                    .await
                    .map(|task_id| (task_id, true, RunItemStatus::Downloading)),
            };

            match result {
                Ok((task_id, require_download_completion, initial_status)) => {
                    let current_run_item_id = if let Some(existing_id) = run_item_id {
                        self.persistence.update_run_item_task_state(
                            existing_id,
                            Some(task_id.as_str()),
                            None,
                            initial_status,
                            None,
                        )?;
                        existing_id
                    } else {
                        let new_id = self.persistence.add_run_item(
                            run_id,
                            &item.path,
                            action,
                            target_kind,
                            Some(task_id.as_str()),
                            None,
                            initial_status,
                            versioned_old.as_deref(),
                            None,
                        )?;
                        run_item_id = Some(new_id);
                        new_id
                    };
                    info!(
                        "executor: 已调度 {}/{} -> target={:?}, task_id={} attempt={}/{}",
                        action,
                        item.path,
                        target_kind,
                        task_id,
                        attempt,
                        TASK_OPERATION_MAX_ATTEMPTS
                    );
                    match self
                        .hooks
                        .wait_transfer_task(
                            &task_id,
                            require_download_completion,
                            TASK_WAIT_TIMEOUT,
                        )
                        .await
                    {
                        Ok(()) => {
                            self.persistence.update_run_item_status(
                                current_run_item_id,
                                RunItemStatus::Completed,
                                None,
                            )?;
                            if overwrote_existing {
                                summary.overwritten += 1;
                            }
                            return Ok(());
                        }
                        Err(e) if e.should_retry() && attempt < TASK_OPERATION_MAX_ATTEMPTS => {
                            let retry_msg = format!("第 {} 次失败，将重试: {}", attempt, e);
                            self.persistence.update_run_item_status(
                                current_run_item_id,
                                initial_status,
                                Some(&retry_msg),
                            )?;
                            let delay = TASK_RETRY_BASE_DELAY * attempt as u32;
                            warn!(
                                "executor: 临时失败，{}ms 后重试 item path={} attempt={}/{} err={}",
                                delay.as_millis(),
                                item.path,
                                attempt + 1,
                                TASK_OPERATION_MAX_ATTEMPTS,
                                e
                            );
                            last_retry_error = Some(e);
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                        Err(e) => {
                            // 资源类错误（quota / local_disk_full）→ 标 Skipped
                            if let Some(reason) = quota_skip_reason(&e) {
                                summary.skipped += 1;
                                self.persistence.update_run_item_status(
                                    current_run_item_id,
                                    RunItemStatus::Skipped,
                                    Some(&e.to_string()),
                                )?;
                                // 用 reason 列显式标记"为什么跳过"，便于前端
                                // 区分"策略跳过"（reason=NULL）与"quota 跳过"
                                let _ = self
                                    .persistence
                                    .set_run_item_reason(current_run_item_id, Some(reason));
                                info!(
                                    "executor: 因 {} 跳过 item path={}, task_id={}",
                                    reason, item.path, task_id
                                );
                                // 不算作 Err（不触发上一级 any_failure 累加），
                                // 但仍返回 Err 让 quota 全局跟踪能感知到
                                return Err(e);
                            } else {
                                summary.failed += 1;
                                self.persistence.update_run_item_status(
                                    current_run_item_id,
                                    RunItemStatus::Failed,
                                    Some(&e.to_string()),
                                )?;
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) if e.should_retry() && attempt < TASK_OPERATION_MAX_ATTEMPTS => {
                    let delay = TASK_RETRY_BASE_DELAY * attempt as u32;
                    warn!(
                        "executor: submit 临时失败，{}ms 后重试 item path={} attempt={}/{} err={}",
                        delay.as_millis(),
                        item.path,
                        attempt + 1,
                        TASK_OPERATION_MAX_ATTEMPTS,
                        e
                    );
                    last_retry_error = Some(e);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(e) => {
                    // 资源类错误（quota / local_disk_full）→ 标 Skipped
                    if let Some(reason) = quota_skip_reason(&e) {
                        summary.skipped += 1;
                        if let Some(existing_id) = run_item_id {
                            self.persistence.update_run_item_status(
                                existing_id,
                                RunItemStatus::Skipped,
                                Some(&e.to_string()),
                            )?;
                            let _ = self
                                .persistence
                                .set_run_item_reason(existing_id, Some(reason));
                        } else {
                            self.record_skipped_run_item(
                                run_id,
                                item,
                                action,
                                target_kind,
                                versioned_old.as_deref(),
                                reason,
                                &e,
                            );
                        }
                        info!(
                            "executor: 因 {} 跳过 item path={} (submit 阶段)",
                            reason, item.path
                        );
                        return Err(e);
                    } else {
                        if let Some(existing_id) = run_item_id {
                            summary.failed += 1;
                            self.persistence.update_run_item_status(
                                existing_id,
                                RunItemStatus::Failed,
                                Some(&e.to_string()),
                            )?;
                        } else {
                            self.record_failed_run_item(
                                summary,
                                run_id,
                                item,
                                action,
                                target_kind,
                                versioned_old.as_deref(),
                                &e,
                            );
                        }
                        return Err(e);
                    }
                }
            }
        }

        let e = last_retry_error
            .unwrap_or_else(|| ShareSyncError::Internal("重试流程未返回结果".to_string()));
        self.record_failed_run_item(
            summary,
            run_id,
            item,
            action,
            target_kind,
            versioned_old.as_deref(),
            &e,
        );
        Err(e)
    }

    /// 处理 removed
    async fn process_removed(
        &self,
        run_id: &str,
        item: &ShareSnapshotItem,
        target: &SyncTarget,
        summary: &mut DiffSummary,
    ) -> Result<(), ShareSyncError> {
        let target_kind = match target {
            SyncTarget::Netdisk(_) => TargetKind::Netdisk,
            SyncTarget::Local(_) => TargetKind::Local,
        };
        let res = match target {
            SyncTarget::Netdisk(t) => {
                let p = format!(
                    "{}/{}",
                    t.remote_path.trim_end_matches('/'),
                    item.path.trim_start_matches('/')
                );
                self.hooks
                    .delete_netdisk(&t.remote_path, std::slice::from_ref(&p))
                    .await
            }
            SyncTarget::Local(t) => self
                .hooks
                .delete_local(&t.local_path, item.path.trim_start_matches('/')),
        };
        match res {
            Ok(()) => {
                self.persistence.add_run_item(
                    run_id,
                    &item.path,
                    SyncAction::Removed,
                    target_kind,
                    None,
                    None,
                    RunItemStatus::Completed,
                    None,
                    None,
                )?;
                Ok(())
            }
            Err(e) => {
                summary.failed += 1;
                self.persistence.add_run_item(
                    run_id,
                    &item.path,
                    SyncAction::Removed,
                    target_kind,
                    None,
                    None,
                    RunItemStatus::Failed,
                    None,
                    None,
                )?;
                Err(e)
            }
        }
    }

    /// 本地目标：版本化重命名旧文件
    fn versioned_local_old(
        &self,
        target: &SyncTarget,
        item: &ShareSnapshotItem,
    ) -> Result<String, ShareSyncError> {
        let local = match target {
            SyncTarget::Local(t) => t,
            SyncTarget::Netdisk(_) => return Err(ShareSyncError::ConfigError("非本地目标".into())),
        };
        let old = local.local_path.join(item.path.trim_start_matches('/'));
        if !old.exists() {
            return Ok(String::new());
        }
        let new_name = timestamped_name(&item.name);
        let new_path = if let Some(parent) = old.parent() {
            parent.join(&new_name)
        } else {
            PathBuf::from(&new_name)
        };
        match std::fs::rename(&old, &new_path) {
            Ok(()) => {
                info!("executor: versioned 本地重命名 {:?} -> {:?}", old, new_path);
                Ok(new_path.to_string_lossy().to_string())
            }
            Err(e) => Err(ShareSyncError::FileSystemError(format!(
                "本地重命名失败: {}",
                e
            ))),
        }
    }

    fn record_failed_run_item(
        &self,
        summary: &mut DiffSummary,
        run_id: &str,
        item: &ShareSnapshotItem,
        action: SyncAction,
        target_kind: TargetKind,
        versioned_old_path: Option<&str>,
        error: &ShareSyncError,
    ) {
        summary.failed += 1;
        match self.persistence.add_run_item(
            run_id,
            &item.path,
            action,
            target_kind,
            None,
            None,
            RunItemStatus::Failed,
            versioned_old_path,
            None,
        ) {
            Ok(run_item_id) => {
                if let Err(e) = self.persistence.update_run_item_status(
                    run_item_id,
                    RunItemStatus::Failed,
                    Some(&error.to_string()),
                ) {
                    warn!("记录 share-sync 失败项错误失败: {}", e);
                }
            }
            Err(e) => warn!("记录 share-sync 失败项失败: {}", e),
        }
    }

    /// 记录"因资源不足被跳过"的 run_item（v1 新增）
    ///
    /// 资源类错误（quota / local_disk_full）不计入 `failed`，但需要在
    /// run_item 行里写明 status=`Skipped` + reason（`quota_full` /
    /// `local_disk_full`），方便前端区分"策略跳过（reason=NULL）"与"quota 跳过"。
    fn record_skipped_run_item(
        &self,
        run_id: &str,
        item: &ShareSnapshotItem,
        action: SyncAction,
        target_kind: TargetKind,
        versioned_old_path: Option<&str>,
        reason: &'static str,
        error: &ShareSyncError,
    ) {
        match self.persistence.add_run_item(
            run_id,
            &item.path,
            action,
            target_kind,
            None,
            None,
            RunItemStatus::Skipped,
            versioned_old_path,
            Some(reason),
        ) {
            Ok(run_item_id) => {
                // 同时把错误信息写到 error 列，方便点击 run_item 时看完整堆栈
                if let Err(e) = self.persistence.update_run_item_status(
                    run_item_id,
                    RunItemStatus::Skipped,
                    Some(&error.to_string()),
                ) {
                    warn!("记录 share-sync 跳过项错误失败: {}", e);
                }
            }
            Err(e) => warn!("记录 share-sync 跳过项失败: {}", e),
        }
    }
}

/// 本地文件是否存在（按 path 拼接到 local_dir）
fn local_file_exists(local_dir: &Path, relative: &str) -> bool {
    local_dir.join(relative.trim_start_matches('/')).exists()
}

fn netdisk_target_file_path(target: &NetdiskTarget, item: &ShareSnapshotItem) -> String {
    join_netdisk_path(&target.remote_path, item.path.trim_start_matches('/'))
}

fn netdisk_target_parent_dir(target: &NetdiskTarget, item: &ShareSnapshotItem) -> String {
    parent_netdisk_dir(&netdisk_target_file_path(target, item))
}

pub(crate) fn join_netdisk_path(base: &str, relative: &str) -> String {
    let base = base.trim_end_matches('/');
    let relative = relative.trim_start_matches('/');
    if base.is_empty() {
        format!("/{}", relative)
    } else if relative.is_empty() {
        base.to_string()
    } else {
        format!("{}/{}", base, relative)
    }
}

pub(crate) fn parent_netdisk_dir(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }
    match path.rsplit_once('/') {
        Some(("", _)) => "/".to_string(),
        Some((parent, _)) if parent.is_empty() => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
        None => "/".to_string(),
    }
}

/// 生成 `name(YYYYMMDD-HHMMSS).ext` 风格的新版本名
pub fn timestamped_name(original: &str) -> String {
    let now = Utc::now();
    let stamp = now.format("%Y%m%d-%H%M%S").to_string();
    if let Some((stem, ext)) = original.rsplit_once('.') {
        if ext.is_empty() || stem.is_empty() {
            format!("{}({})", original, stamp)
        } else {
            format!("{}({}).{}", stem, stamp, ext)
        }
    } else {
        format!("{}({})", original, stamp)
    }
}

// ============================================================
// v1 新增：纯函数工具
// ============================================================

/// 把 added/modified 候选按"目录根"分组
///
/// **算法**：
/// 1. 把 `include_paths` 规整为 `BTreeSet`（保持有序，去重）
/// 2. 对每条 candidate `(action, item)`：
///    - 找 `item.path` 的"最具体祖先 include 路径"（`/data/sub` → `/data`）
///    - 若 `include_paths` 为空 → 根为 `""`（所有 item 一组）
///    - 若 `item.path` 不在任何 include 子树下 → 根为 `""`（兜底）
/// 3. 按根分组；每组内保持 candidates 原顺序
///
/// **为什么用"最近祖先 include"做根**：
/// - include 选目录的语义是"整棵子树都同步"，所以目录内的文件天然属于这棵子树
/// - include_paths 选中的多个目录各自成一组（多目录订阅）
/// - 选 `""`（未分组）作根的场景：include 为空 / item.path 不在 include 子树下
///
/// **返回**：`Vec<(root_path, Vec<(SyncAction, ShareSnapshotItem)>)>`，
/// 按 `root_path` 字典序排序，组内按 `item.path` 字典序排序。
pub(crate) fn group_by_dir_root(
    candidates: &[(SyncAction, ShareSnapshotItem)],
    include_paths: &[String],
) -> Vec<(String, Vec<(SyncAction, ShareSnapshotItem)>)> {
    use std::collections::BTreeMap;

    // 规整 include_paths：BTreeSet 自动去重 + 排序
    let includes: std::collections::BTreeSet<String> = include_paths
        .iter()
        .map(|p| normalize_include_path(p))
        .filter(|p| !p.is_empty())
        .collect();

    let mut groups: BTreeMap<String, Vec<(SyncAction, ShareSnapshotItem)>> = BTreeMap::new();
    for (action, item) in candidates {
        let root = find_included_ancestor(&item.path, &includes).unwrap_or_else(|| String::new());
        groups
            .entry(root)
            .or_default()
            .push((*action, item.clone()));
    }

    // 组内按 item.path 排序，便于调试和测试
    let mut out: Vec<(String, Vec<(SyncAction, ShareSnapshotItem)>)> = groups.into_iter().collect();
    for (_, group) in &mut out {
        group.sort_by(|a, b| a.1.path.cmp(&b.1.path));
    }
    out
}

/// 规整 include 路径：去首尾 `/`，空字符串返回空
fn normalize_include_path(p: &str) -> String {
    let trimmed = p.trim().trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("/{}", trimmed)
    }
}

/// 找 item.path 的"最具体祖先 include 路径"
///
/// 例：includes={/data, /data/sub}，item.path="/data/sub/a.txt" → 返回 "/data/sub"
/// 例：includes={/data}，item.path="/other/a.txt" → 返回 None（兜底 → ""）
fn find_included_ancestor(
    item_path: &str,
    includes: &std::collections::BTreeSet<String>,
) -> Option<String> {
    if includes.is_empty() {
        return None;
    }
    // 候选：从 item_path 自身开始，逐级向上找祖先
    let mut current = item_path.trim().to_string();
    loop {
        if includes.contains(&current) {
            return Some(current);
        }
        // 找 current 的父目录
        match current.rfind('/') {
            Some(0) => return None, // 已到根仍没匹配
            Some(idx) => {
                current = current[..idx].to_string();
            }
            None => return None,
        }
    }
}

/// 累积 run 失败原因：取最严重的类别
///
/// 严重度（高→低）：Quota / LocalDiskFull / Auth / Other
/// 一旦遇到 Quota/LocalDiskFull 就锁定不再被覆盖；
/// 其它类别按"首个"记录。
fn update_run_failure_reason(
    current: Option<&'static str>,
    category: ErrorCategory,
) -> Option<&'static str> {
    use crate::share_sync::error::ErrorCategory;
    let new_reason: Option<&'static str> = match category {
        ErrorCategory::Quota => Some("quota_full"),
        ErrorCategory::LocalDiskFull => Some("local_disk_full"),
        ErrorCategory::Auth => Some("auth"),
        _ => Some("other"),
    };
    match (current, new_reason) {
        (Some("quota_full"), _) => Some("quota_full"),
        (Some("local_disk_full"), _) => Some("local_disk_full"),
        (Some(c), _) => Some(c), // 保留首个非严重类别
        (None, n) => n,
    }
}

/// 取"更严重"的 ErrorCategory:Auth > NotFound > Transient > Quota > LocalDiskFull >
/// 其它。`submit_subtree_as_leaves` 累积多个 leaf 的失败时需要选出最值得上报
/// 的那个,以便 run_failure_reason 在 quota_only 时仍能区分 quota / non-quota。
fn max_category(a: ErrorCategory, b: ErrorCategory) -> ErrorCategory {
    fn weight(c: ErrorCategory) -> u8 {
        use crate::share_sync::error::ErrorCategory as E;
        match c {
            E::Auth => 6,
            E::Config => 5,
            E::NotFound => 4,
            E::Transient => 3,
            E::Quota => 2,
            E::DirTransferAmbiguous => 2,
            E::LocalDiskFull => 1,
            E::Other => 0,
        }
    }
    if weight(a) >= weight(b) {
        a
    } else {
        b
    }
}

/// 恢复"等待超时但下载已完成"的 run_item (v1.2 新增)
///
/// ## 触发场景
///
/// `transfer_node_set` 里的 `wait_transfer_task` 触发 `TASK_WAIT_TIMEOUT` (30 分钟)
/// 时会返回 `Err(DownloadError("等待任务完成超时"))`,category 判为 `Other`,落到
/// marker 分支把整组叶子标 `Failed`。**但 DownloadManager 实际在后台继续把
/// 文件落盘** —— 用户看到磁盘上文件齐全,前端却显示"部分成功" + N 个失败项。
///
/// 这个函数就是事后兜底:对 `target='local'` 的 `Failed` run_item,反查订阅里
/// 的 local target 目录,只要文件确实在,就把它升级为 `Completed`,并把
/// `summary.failed` 减 1。这样:
/// - 详情页错误列表里不再有"假失败"项
/// - `run` 终态从 `CompletedWithErrors` 降回 `Completed` (失败归零时)
/// - 快照基线正常推进,下次轮询不会重下这些文件
///
/// ## 约束
///
/// - **只对 `target='local'` 生效** — netdisk 目标无法用本地文件存在判断
/// - **不动 `Skipped` / `Transferring` / `Downloading` / `Completed`** ——
///   只挑 `Failed` 升级
/// - **文件路径 = `local_root/<item.path>`**,与 executor 的
///   `local_file_exists` 拼法一致
/// - **必须在 `reconcile_summary_from_run_items` 之前调用** ——
///   后续 summary 重算时,DB 里 `status='completed'` 已生效
fn recover_local_run_items(
    persistence: &ShareSyncPersistence,
    subscription: &ShareSubscription,
    run_id: &str,
    summary: &mut DiffSummary,
) {
    let local_roots: Vec<PathBuf> = subscription
        .targets
        .iter()
        .filter_map(|t| match t {
            SyncTarget::Local(t) => Some(t.local_path.clone()),
            _ => None,
        })
        .collect();
    if local_roots.is_empty() {
        return;
    }

    let items = match persistence.list_run_items(run_id) {
        Ok(v) => v,
        Err(e) => {
            warn!("share_sync_recover: 读 run_items 失败: {}", e);
            return;
        }
    };

    let mut upgraded = 0usize;
    for item in items {
        // 只挑"failed + 本地目标",其它状态(Skipped/Transferring/Downloading/
        // Completed)都跳过;其中 Skipped(quota_full/disk_full)是资源类,不属
        // "超时误判",也不该升级
        if item.status != RunItemStatus::Failed.as_str()
            || item.target != TargetKind::Local.as_str()
        {
            continue;
        }
        let rel = item.path.trim_start_matches('/');
        // 多个 local target 时,任一目录下存在即认为已落盘
        let exists = local_roots.iter().any(|root| root.join(rel).is_file());
        if !exists {
            continue;
        }
        // 升级为 completed(同时清掉 error 列,前端展示更干净)
        if let Err(e) = persistence.update_run_item_status(
            item.id,
            RunItemStatus::Completed,
            None,
        ) {
            warn!(
                "share_sync_recover: 升级 run_item 失败: id={} path={} err={}",
                item.id, item.path, e
            );
            continue;
        }
        upgraded += 1;
        if summary.failed > 0 {
            summary.failed -= 1;
        }
    }

    if upgraded > 0 {
        info!(
            "share_sync_recover: run_id={} upgraded={} (文件已落盘但 wait 超时误判为失败)",
            run_id, upgraded
        );
    }
}

#[allow(dead_code)]
fn _suppress_unused() {
    let _ = (
        NetdiskTarget {
            remote_path: String::new(),
            save_fs_id: 0,
            conflict_strategy: None,
        },
        LocalTarget {
            local_path: PathBuf::new(),
            conflict_strategy: None,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::config::{NetdiskTarget, SyncTarget};
    use crate::share_sync::diff::diff_snapshots;
    use crate::share_sync::snapshot::ShareSnapshot;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::tempdir;

    fn sub() -> ShareSubscription {
        ShareSubscription::new(
            "test".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: PathBuf::from("/tmp/x"),
                conflict_strategy: None,
            })],
        )
    }

    fn captured() -> CapturedShare {
        CapturedShare {
            short_key: "1abc".into(),
            shareid: "123".into(),
            uk: "456".into(),
            bdstoken: "tok".into(),
            password: None,
            randsk: None,
        }
    }

    fn item(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }

    fn existing_netdisk_entry(path: &str, name: &str, fs_id: u64) -> NetdiskTargetEntry {
        NetdiskTargetEntry {
            path: path.to_string(),
            name: name.to_string(),
            fs_id,
            is_dir: false,
        }
    }

    // ----- Mock hooks -----
    #[derive(Default)]
    struct MockHooks {
        transfers: Mutex<Vec<(String, u64, String)>>,
        downloads: Mutex<Vec<(u64, String, PathBuf, ConflictStrategy)>>,
        local_deletes: Mutex<Vec<(PathBuf, String)>>,
        netdisk_files: Mutex<HashMap<String, NetdiskTargetEntry>>,
        netdisk_deletes: Mutex<Vec<String>>,
        netdisk_renames: Mutex<Vec<(String, u64, String)>>,
        // v1 新增：整批 submit 调用记录
        batch_transfers: Mutex<Vec<(String, Vec<u64>, Vec<String>)>>,
        batch_downloads: Mutex<Vec<(Vec<u64>, Vec<String>, PathBuf)>>,
        // v1 新增：注入 batch submit 错误（None = 不注入）
        batch_transfer_error: Mutex<Option<ShareSyncError>>,
        batch_download_error: Mutex<Option<ShareSyncError>>,
        // v1.1 新增：注入单文件 submit_transfer 错误（按 fs_id 维度）
        transfer_submit_errors: Mutex<HashMap<u64, ShareSyncError>>,
        // v1.1 新增：注入单文件 submit_download 错误（按 fs_id 维度）
        download_submit_errors: Mutex<HashMap<u64, ShareSyncError>>,
        // v1.1 新增：注入 wait_transfer_task 错误（按 task_id 维度）
        wait_errors: Mutex<HashMap<String, ShareSyncError>>,
    }
    #[async_trait]
    impl ExecutorHooks for MockHooks {
        async fn submit_transfer(
            &self,
            _c: &CapturedShare,
            target: &str,
            item: &ShareSnapshotItem,
            _label: Option<&str>,
        ) -> Result<String, ShareSyncError> {
            // 注入 submit_transfer 错误（按 fs_id 维度）
            if let Some(err) = self
                .transfer_submit_errors
                .lock()
                .unwrap()
                .remove(&item.fs_id)
            {
                return Err(err);
            }
            let mut g = self.transfers.lock().unwrap();
            let id = format!("tx-{}", g.len() + 1);
            g.push((target.to_string(), item.fs_id, item.path.clone()));
            Ok(id)
        }
        async fn find_netdisk_file(
            &self,
            target_path: &str,
        ) -> Result<Option<NetdiskTargetEntry>, ShareSyncError> {
            Ok(self.netdisk_files.lock().unwrap().get(target_path).cloned())
        }
        async fn rename_netdisk(
            &self,
            path: &str,
            fs_id: u64,
            new_name: &str,
        ) -> Result<String, ShareSyncError> {
            self.netdisk_renames.lock().unwrap().push((
                path.to_string(),
                fs_id,
                new_name.to_string(),
            ));
            Ok(join_netdisk_path(&parent_netdisk_dir(path), new_name))
        }
        async fn submit_download(
            &self,
            item: &ShareSnapshotItem,
            dir: &Path,
            strategy: ConflictStrategy,
        ) -> Result<String, ShareSyncError> {
            // 注入 submit_download 错误（按 fs_id 维度）
            if let Some(err) = self
                .download_submit_errors
                .lock()
                .unwrap()
                .remove(&item.fs_id)
            {
                return Err(err);
            }
            let mut g = self.downloads.lock().unwrap();
            let id = format!("dl-{}", g.len() + 1);
            g.push((item.fs_id, item.path.clone(), dir.to_path_buf(), strategy));
            Ok(id)
        }
        async fn wait_transfer_task(
            &self,
            task_id: &str,
            _require_download_completion: bool,
            _timeout: Duration,
        ) -> Result<(), ShareSyncError> {
            // 注入 wait_transfer_task 错误（按 task_id 维度）
            if let Some(err) = self.wait_errors.lock().unwrap().remove(task_id) {
                return Err(err);
            }
            Ok(())
        }
        async fn delete_netdisk(&self, _t: &str, paths: &[String]) -> Result<(), ShareSyncError> {
            self.netdisk_deletes
                .lock()
                .unwrap()
                .extend(paths.iter().cloned());
            Ok(())
        }
        fn delete_local(&self, dir: &Path, rel: &str) -> Result<(), ShareSyncError> {
            let mut g = self.local_deletes.lock().unwrap();
            g.push((dir.to_path_buf(), rel.to_string()));
            Ok(())
        }
        // v1 新增：整批 submit 实现
        async fn submit_transfer_batch(
            &self,
            _c: &CapturedShare,
            target: &str,
            items: &[ShareSnapshotItem],
            _label: Option<&str>,
        ) -> Result<String, ShareSyncError> {
            if let Some(err) = self.batch_transfer_error.lock().unwrap().take() {
                return Err(err);
            }
            let fs_ids: Vec<u64> = items.iter().map(|i| i.fs_id).collect();
            let paths: Vec<String> = items.iter().map(|i| i.path.clone()).collect();
            let mut g = self.batch_transfers.lock().unwrap();
            let id = format!("btx-{}", g.len() + 1);
            g.push((target.to_string(), fs_ids, paths));
            Ok(id)
        }
        async fn submit_download_batch(
            &self,
            items: &[ShareSnapshotItem],
            dir: &Path,
            _strategy: ConflictStrategy,
        ) -> Result<String, ShareSyncError> {
            if let Some(err) = self.batch_download_error.lock().unwrap().take() {
                return Err(err);
            }
            let fs_ids: Vec<u64> = items.iter().map(|i| i.fs_id).collect();
            let paths: Vec<String> = items.iter().map(|i| i.path.clone()).collect();
            let mut g = self.batch_downloads.lock().unwrap();
            let id = format!("bdl-{}", g.len() + 1);
            g.push((fs_ids, paths, dir.to_path_buf()));
            Ok(id)
        }
    }

    #[test]
    fn test_timestamped_name() {
        let n = timestamped_name("file.txt");
        assert!(n.starts_with("file("));
        assert!(n.ends_with(".txt"));
        assert!(n.contains("-"));
    }

    #[test]
    fn test_timestamped_name_no_ext() {
        let n = timestamped_name("README");
        assert!(n.starts_with("README("));
    }

    #[test]
    fn test_overwrite_local_dispatches_download() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(hooks.downloads.lock().unwrap().len(), 1);
        assert_eq!(
            hooks.downloads.lock().unwrap()[0].3,
            ConflictStrategy::Overwrite
        );
        assert_eq!(hooks.transfers.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_overwrite_netdisk_dispatches_transfer() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/我的资源/同步".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(hooks.transfers.lock().unwrap().len(), 1);
        let g = hooks.transfers.lock().unwrap();
        assert_eq!(g[0].0, "/我的资源/同步");
    }

    #[test]
    fn test_overwrite_netdisk_deletes_existing_before_transfer() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/我的资源/同步".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        hooks.netdisk_files.lock().unwrap().insert(
            "/我的资源/同步/a.txt".into(),
            existing_netdisk_entry("/我的资源/同步/a.txt", "a.txt", 91),
        );
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        let deletes = hooks.netdisk_deletes.lock().unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0], "/我的资源/同步/a.txt");
        assert_eq!(hooks.transfers.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_versioned_netdisk_renames_existing_before_transfer() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.conflict_strategy = ConflictStrategy::Versioned;
            s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/我的资源/同步".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        hooks.netdisk_files.lock().unwrap().insert(
            "/我的资源/同步/a.txt".into(),
            existing_netdisk_entry("/我的资源/同步/a.txt", "a.txt", 91),
        );
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        let renames = hooks.netdisk_renames.lock().unwrap();
        assert_eq!(renames.len(), 1);
        assert_eq!(renames[0].0, "/我的资源/同步/a.txt");
        assert_eq!(renames[0].1, 91);
        assert!(renames[0].2.starts_with("a("));
        assert!(renames[0].2.ends_with(".txt"));
        assert_eq!(hooks.transfers.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_skip_strategy_when_netdisk_exists() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.conflict_strategy = ConflictStrategy::Skip;
            s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/我的资源/同步".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        hooks.netdisk_files.lock().unwrap().insert(
            "/我的资源/同步/a.txt".into(),
            existing_netdisk_entry("/我的资源/同步/a.txt", "a.txt", 91),
        );
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(hooks.transfers.lock().unwrap().len(), 0);
        assert_eq!(hooks.netdisk_deletes.lock().unwrap().len(), 0);
        assert_eq!(hooks.netdisk_renames.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_two_targets_both_dispatch() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![
                SyncTarget::Netdisk(NetdiskTarget {
                    remote_path: "/x".into(),
                    save_fs_id: 0,
                    conflict_strategy: None,
                }),
                SyncTarget::Local(LocalTarget {
                    local_path: dir.path().to_path_buf(),
                    conflict_strategy: None,
                }),
            ];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a", 1, 1)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(hooks.transfers.lock().unwrap().len(), 1);
        assert_eq!(hooks.downloads.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_versioned_local_renames_existing() {
        let dir = tempdir().unwrap();
        // 用嵌套 tempdir 隔开：local_dir 与 db 互不干扰
        let local_dir = dir.path().join("local");
        std::fs::create_dir_all(&local_dir).unwrap();
        let db_dir = dir.path().join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let p = &local_dir;
        // 预先创建旧文件
        std::fs::write(p.join("a.txt"), b"old").unwrap();
        let s = {
            let mut s = sub();
            s.conflict_strategy = ConflictStrategy::Versioned;
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        // 旧文件应被重命名为带时间戳；应当只有这一个文件
        let entries: Vec<_> = std::fs::read_dir(p)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(entries.len(), 1, "entries = {:?}", entries);
        assert!(entries[0].starts_with("a("));
        assert!(entries[0].ends_with(".txt"));
        // 仍然 dispatch 了 download
        assert_eq!(hooks.downloads.lock().unwrap().len(), 1);
        assert_eq!(
            hooks.downloads.lock().unwrap()[0].3,
            ConflictStrategy::Versioned
        );
    }

    #[test]
    fn test_skip_strategy_when_local_exists() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        std::fs::write(p.join("a.txt"), b"old").unwrap();
        let s = {
            let mut s = sub();
            s.conflict_strategy = ConflictStrategy::Skip;
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        // skip 触发，download 不应被调用
        assert_eq!(hooks.downloads.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_delete_missing_false_keeps_removed() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: dir.path().to_path_buf(),
                conflict_strategy: None,
            })];
            s.delete_missing = false;
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/old.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.diff_summary.removed, 1);
        // delete_missing=false: 不应调用 delete
        assert_eq!(hooks.local_deletes.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_delete_missing_true_deletes() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        std::fs::write(p.join("old.txt"), b"x").unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s.delete_missing = true;
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![item("/old.txt", 1, 100)]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.diff_summary.removed, 1);
        // delete_missing=true: 调用 delete
        assert_eq!(hooks.local_deletes.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_first_run_persists_snapshot() {
        // 验证 run 状态正常写入
        let dir = tempdir().unwrap();
        let s = sub();
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a", 1, 1)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        let rec = pm.get_run(&outcome.run_id).unwrap().unwrap();
        assert_eq!(rec.added_count, 1);
        assert_eq!(rec.status, "completed");
    }

    #[test]
    fn test_local_file_exists_helper() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();
        assert!(local_file_exists(dir.path(), "/a.txt"));
        assert!(!local_file_exists(dir.path(), "/b.txt"));
    }

    // ============================================================
    // v1.1 新增：quota / local_disk_full 跳过测试
    // ============================================================
    //
    // 覆盖：
    // 1) 单文件 submit_transfer 阶段 quota → 标 Skipped + reason
    // 2) 单文件 submit_download 阶段 quota → 标 Skipped + reason
    // 3) wait_transfer_task 阶段 quota → 标 Skipped + reason
    // 4) 仅 quota 失败时 run.status = Completed（不是 CompletedWithErrors）
    // 5) quota + 真实失败混存时 run.status = CompletedWithErrors
    // 6) summary.skipped 字段正确累加
    // 7) apply_with_run_id_grouped：batch quota → 退化为逐文件 submit

    fn quota_err() -> ShareSyncError {
        ShareSyncError::TransferError("网盘空间不足".into())
    }

    fn disk_full_err() -> ShareSyncError {
        ShareSyncError::FileSystemError("ENOSPC: no space left on device".into())
    }

    #[tokio::test]
    async fn test_wait_transient_error_retries_download_task() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/retry.csv", 300, 999)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        hooks.wait_errors.lock().unwrap().insert(
            "dl-1".to_string(),
            ShareSyncError::DownloadError("请求超时，请稍后再试".into()),
        );

        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = ex.apply(&captured(), &diff).await;

        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.failed, 0);
        assert_eq!(hooks.downloads.lock().unwrap().len(), 2);

        let items = pm.list_run_items(&outcome.run_id).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, "completed");
        assert_eq!(items[0].transfer_task_id.as_deref(), Some("dl-2"));
        assert!(items[0].error.is_none());
    }

    #[test]
    fn test_submit_transfer_quota_marks_run_item_skipped() {
        let dir = tempdir().unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/我的资源/同步".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/big.zip", 100, 999_999_999)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        // 注入 quota 错误到 fs_id=100
        hooks
            .transfer_submit_errors
            .lock()
            .unwrap()
            .insert(100, quota_err());
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        // 仅 quota 跳过 → Completed
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.failed, 0);
        assert_eq!(outcome.diff_summary.skipped, 1);
        assert!(outcome.diff_summary.added == 1);
        // run_item 行：status=Skipped, reason=quota_full
        let items = pm.list_run_items(&outcome.run_id).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, "skipped");
        assert_eq!(items[0].reason.as_deref(), Some("quota_full"));
    }

    #[test]
    fn test_submit_download_quota_marks_run_item_skipped() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/big.zip", 200, 999)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        // 注入 quota 错误到 download submit
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(200, quota_err());
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.skipped, 1);
        assert_eq!(outcome.diff_summary.failed, 0);
        let items = pm.list_run_items(&outcome.run_id).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, "skipped");
        assert_eq!(items[0].reason.as_deref(), Some("quota_full"));
    }

    #[test]
    fn test_wait_transfer_task_quota_marks_run_item_skipped() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a.txt", 300, 50)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        // submit_download 会返回 task_id="dl-1"，我们在 wait 阶段注入 quota
        hooks
            .wait_errors
            .lock()
            .unwrap()
            .insert("dl-1".into(), quota_err());
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.skipped, 1);
        assert_eq!(outcome.diff_summary.failed, 0);
        let items = pm.list_run_items(&outcome.run_id).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, "skipped");
        assert_eq!(items[0].reason.as_deref(), Some("quota_full"));
    }

    #[test]
    fn test_local_disk_full_uses_local_disk_full_reason() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(&s.id, vec![item("/a", 400, 1)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(400, disk_full_err());
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.skipped, 1);
        let items = pm.list_run_items(&outcome.run_id).unwrap();
        assert_eq!(items[0].status, "skipped");
        assert_eq!(items[0].reason.as_deref(), Some("local_disk_full"));
    }

    #[test]
    fn test_quota_plus_real_failure_keeps_completed_with_errors() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let s = {
            let mut s = sub();
            s.targets = vec![SyncTarget::Local(LocalTarget {
                local_path: p.to_path_buf(),
                conflict_strategy: None,
            })];
            s
        };
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        // item 500: quota 跳过；item 600: 触发真实失败（用 -7 errmsg 不是 quota）
        let curr =
            ShareSnapshot::with_items(&s.id, vec![item("/quota", 500, 1), item("/fail", 600, 1)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(500, quota_err());
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(600, ShareSyncError::TransferError("API error 31066".into()));
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply(&captured(), &diff));
        // 存在非 quota 失败 → CompletedWithErrors
        assert_eq!(outcome.status, RunStatus::CompletedWithErrors);
        assert_eq!(outcome.diff_summary.skipped, 1);
        assert_eq!(outcome.diff_summary.failed, 1);
        let items = pm.list_run_items(&outcome.run_id).unwrap();
        let quota_item = items.iter().find(|i| i.path == "/quota").unwrap();
        let fail_item = items.iter().find(|i| i.path == "/fail").unwrap();
        assert_eq!(quota_item.status, "skipped");
        assert_eq!(quota_item.reason.as_deref(), Some("quota_full"));
        assert_eq!(fail_item.status, "failed");
    }

    #[test]
    fn test_quota_only_run_status_is_completed() {
        // 覆盖整批路径（apply_with_run_id_grouped）：batch quota → 退化为逐文件 submit
        // 整组里 2 个文件都 quota 跳过 → run 仍 Completed
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let mut s = sub();
        s.targets = vec![SyncTarget::Local(LocalTarget {
            local_path: p.to_path_buf(),
            conflict_strategy: None,
        })];
        s.include_paths = vec!["/monthly".into()]; // 让 group_by_dir_root 把 /monthly/* 归一组
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(
            &s.id,
            vec![
                item("/monthly/01.csv", 701, 1),
                item("/monthly/02.csv", 702, 1),
            ],
        );
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        // batch submit 阶段注入 quota 错误
        hooks
            .batch_download_error
            .lock()
            .unwrap()
            .replace(quota_err());
        // 单文件回退路径（submit_download）也注入 quota 错误
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(701, quota_err());
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(702, quota_err());
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply_with_run_id_grouped(
            "r1".into(),
            &captured(),
            &diff,
        ));
        // 仅 quota 跳过 → Completed（不是 Failed / CompletedWithErrors）
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.skipped, 2);
        assert_eq!(outcome.diff_summary.failed, 0);
        // run_item 行里至少有 2 条 Skipped + reason=quota_full
        let items = pm.list_run_items(&outcome.run_id).unwrap();
        let skipped_count = items
            .iter()
            .filter(|i| i.status == "skipped" && i.reason.as_deref() == Some("quota_full"))
            .count();
        assert!(
            skipped_count >= 2,
            "skipped/quota_full count = {}",
            skipped_count
        );
    }

    #[test]
    fn test_batch_quota_falls_back_to_per_file_with_partial_success() {
        // 验证 quota 退化路径：batch quota 失败后，回退到逐文件 submit，
        // 其中一个文件**能成功**（小文件，剩余空间够），另一个**quota 跳过**。
        // 关键观察：summary.skipped=1, summary.failed=0, run.status=Completed,
        //          downloads.len()=1（只有成功那个被记录到 mock 的 downloads 列表里）
        let dir = tempdir().unwrap();
        let p = dir.path();
        let db_dir = p.join("db");
        std::fs::create_dir_all(&db_dir).unwrap();
        let mut s = sub();
        s.targets = vec![SyncTarget::Local(LocalTarget {
            local_path: p.to_path_buf(),
            conflict_strategy: None,
        })];
        s.include_paths = vec!["/monthly".into()];
        let pm = ShareSyncPersistence::new(&db_dir.join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(
            &s.id,
            vec![
                item("/monthly/small.csv", 801, 1),
                item("/monthly/big.csv", 802, 999_999_999),
            ],
        );
        let diff = diff_snapshots(Some(&prev), &curr);
        let hooks = MockHooks::default();
        // batch submit 阶段注入 quota 错误
        hooks
            .batch_download_error
            .lock()
            .unwrap()
            .replace(quota_err());
        // 单文件回退路径：big.csv quota 失败，small.csv 不注入错误（默认成功）
        hooks
            .download_submit_errors
            .lock()
            .unwrap()
            .insert(802, quota_err());
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);
        let outcome = futures::executor::block_on(ex.apply_with_run_id_grouped(
            "r2".into(),
            &captured(),
            &diff,
        ));
        // 仍然 Completed（quota 类不算业务失败）
        assert_eq!(outcome.status, RunStatus::Completed);
        assert_eq!(outcome.diff_summary.skipped, 1);
        assert_eq!(outcome.diff_summary.failed, 0);
        // 关键信号：downloads.len() == 1 说明 per-file 退化的 small.csv 真的被
        // submit_download 触发了（这才是"quota 退化到逐文件"想验证的事）
        assert_eq!(
            hooks.downloads.lock().unwrap().len(),
            1,
            "per-file fallback should be triggered for at least one item"
        );
        // 那条成功的 download 应该是 small.csv
        let downloads = hooks.downloads.lock().unwrap();
        assert_eq!(
            downloads[0].0, 801,
            "successful fallback should be small.csv"
        );
    }

    // ============================================================
    // v1.2: recover_local_run_items 单元测试
    // 模拟 "wait 超时 → marker 标 failed → 实际文件已落盘" 的场景,
    // 验证 recovery 能把 failed 项升级为 completed 并减 summary.failed
    // ============================================================

    fn sub_with_local(local_root: PathBuf) -> ShareSubscription {
        let mut s = sub();
        s.targets = vec![SyncTarget::Local(LocalTarget {
            local_path: local_root,
            conflict_strategy: None,
        })];
        s
    }

    /// 模拟 apply_with_run_id 留下的"timeout 失败"项,文件其实已落盘
    #[test]
    fn test_recover_local_run_items_upgrades_existing_local_file() {
        let dir = tempdir().unwrap();
        let local_root = dir.path().to_path_buf();
        // 在 local_root 下创建一个真实文件
        let target_file = local_root.join("weekly/000001.SZ.csv");
        std::fs::create_dir_all(target_file.parent().unwrap()).unwrap();
        std::fs::write(&target_file, b"data").unwrap();

        let s = sub_with_local(local_root);
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        pm.start_run("test-run", &s.id, 1234).unwrap();
        // 模拟 marker 分支写入的 failed 项(error=None)
        let row_id = pm
            .add_run_item(
                "test-run",
                "/weekly/000001.SZ.csv",
                SyncAction::Added,
                TargetKind::Local,
                None,
                None,
                RunItemStatus::Failed,
                None,
                None,
            )
            .unwrap();

        let mut summary = DiffSummary::default();
        summary.failed = 1;
        recover_local_run_items(&pm, &s, "test-run", &mut summary);

        // DB 状态应升级为 completed
        let items = pm.list_run_items("test-run").unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, row_id);
        assert_eq!(items[0].status, "completed", "落盘文件应被升级");
        // summary 应减 1
        assert_eq!(summary.failed, 0, "summary.failed 应同步减 1");
    }

    /// 文件不在 → 不应升级,保持 failed 状态
    #[test]
    fn test_recover_local_run_items_skips_missing_local_file() {
        let dir = tempdir().unwrap();
        let local_root = dir.path().to_path_buf();
        // 不创建任何文件

        let s = sub_with_local(local_root);
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        pm.start_run("test-run", &s.id, 1234).unwrap();
        pm.add_run_item(
            "test-run",
            "/missing/file.csv",
            SyncAction::Added,
            TargetKind::Local,
            None,
            None,
            RunItemStatus::Failed,
            None,
            None,
        )
        .unwrap();

        let mut summary = DiffSummary::default();
        summary.failed = 1;
        recover_local_run_items(&pm, &s, "test-run", &mut summary);

        let items = pm.list_run_items("test-run").unwrap();
        assert_eq!(items[0].status, "failed", "文件不存在应保持 failed");
        assert_eq!(summary.failed, 1, "summary.failed 应不变");
    }

    /// netdisk 目标失败 → 不应被 recovery 升级(只能本地查)
    #[test]
    fn test_recover_local_run_items_ignores_netdisk_targets() {
        let dir = tempdir().unwrap();
        let mut s = sub();
        s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
            remote_path: "/我的资源/同步".into(),
            save_fs_id: 0,
            conflict_strategy: None,
        })];
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        pm.start_run("test-run", &s.id, 1234).unwrap();
        pm.add_run_item(
            "test-run",
            "/some/file.csv",
            SyncAction::Added,
            TargetKind::Netdisk,
            None,
            None,
            RunItemStatus::Failed,
            None,
            None,
        )
        .unwrap();

        let mut summary = DiffSummary::default();
        summary.failed = 1;
        recover_local_run_items(&pm, &s, "test-run", &mut summary);

        let items = pm.list_run_items("test-run").unwrap();
        assert_eq!(
            items[0].status, "failed",
            "netdisk 目标不应被 recovery 升级"
        );
        assert_eq!(summary.failed, 1);
    }

    /// Skipped 状态(quota/disk) → 不应被升级(它是资源类,不是超时误判)
    #[test]
    fn test_recover_local_run_items_ignores_skipped_status() {
        let dir = tempdir().unwrap();
        let local_root = dir.path().to_path_buf();
        // 即使文件存在,Skipped 项也不应被升级
        let target_file = local_root.join("data.csv");
        std::fs::write(&target_file, b"data").unwrap();

        let s = sub_with_local(local_root);
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        pm.start_run("test-run", &s.id, 1234).unwrap();
        pm.add_run_item(
            "test-run",
            "/data.csv",
            SyncAction::Added,
            TargetKind::Local,
            None,
            None,
            RunItemStatus::Skipped,
            None,
            Some("quota_full"),
        )
        .unwrap();

        let mut summary = DiffSummary::default();
        summary.skipped = 1;
        recover_local_run_items(&pm, &s, "test-run", &mut summary);

        let items = pm.list_run_items("test-run").unwrap();
        assert_eq!(
            items[0].status, "skipped",
            "Skipped 状态(quota/disk)不应被 recovery 升级"
        );
    }

    /// 多个 local target 时,任一目录存在即升级
    #[test]
    fn test_recover_local_run_items_multi_local_targets() {
        let dir = tempdir().unwrap();
        let root_a = dir.path().join("a");
        let root_b = dir.path().join("b");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        // 文件只在 root_b 下
        std::fs::write(root_b.join("x.csv"), b"data").unwrap();

        let mut s = sub();
        s.targets = vec![
            SyncTarget::Local(LocalTarget {
                local_path: root_a,
                conflict_strategy: None,
            }),
            SyncTarget::Local(LocalTarget {
                local_path: root_b,
                conflict_strategy: None,
            }),
        ];
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        pm.start_run("test-run", &s.id, 1234).unwrap();
        pm.add_run_item(
            "test-run",
            "/x.csv",
            SyncAction::Added,
            TargetKind::Local,
            None,
            None,
            RunItemStatus::Failed,
            None,
            None,
        )
        .unwrap();

        let mut summary = DiffSummary::default();
        summary.failed = 1;
        recover_local_run_items(&pm, &s, "test-run", &mut summary);

        let items = pm.list_run_items("test-run").unwrap();
        assert_eq!(items[0].status, "completed");
        assert_eq!(summary.failed, 0);
    }

    /// 没有 local target 的订阅 → recovery 是 no-op
    #[test]
    fn test_recover_local_run_items_no_local_targets_is_noop() {
        let dir = tempdir().unwrap();
        let s = sub();
        // 强制改成 netdisk-only
        let mut s = s;
        s.targets = vec![SyncTarget::Netdisk(NetdiskTarget {
            remote_path: "/backup".into(),
            save_fs_id: 0,
            conflict_strategy: None,
        })];

        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();
        pm.start_run("test-run", &s.id, 1234).unwrap();
        pm.add_run_item(
            "test-run",
            "/x.csv",
            SyncAction::Added,
            TargetKind::Netdisk,
            None,
            None,
            RunItemStatus::Failed,
            None,
            None,
        )
        .unwrap();

        let mut summary = DiffSummary::default();
        summary.failed = 1;
        recover_local_run_items(&pm, &s, "test-run", &mut summary);

        // 没有任何改动
        let items = pm.list_run_items("test-run").unwrap();
        assert_eq!(items[0].status, "failed");
        assert_eq!(summary.failed, 1);
    }

    /// 端到端: simulate "marker 失败后 file 已落盘" → 调 apply 全流程
    /// 验证 run 终态能从 CompletedWithErrors 降级为 Completed
    #[tokio::test]
    async fn test_end_to_end_recovery_downgrades_run_status_to_completed() {
        let dir = tempdir().unwrap();
        let local_root = dir.path().to_path_buf();
        let s = sub_with_local(local_root.clone());
        let pm = ShareSyncPersistence::new(&dir.path().join("s.db")).unwrap();
        pm.upsert_subscription(&s).unwrap();

        // 准备 prev=空, curr=2 个文件
        let prev = ShareSnapshot::with_items(&s.id, vec![]);
        pm.save_snapshot(&prev).unwrap();
        let curr = ShareSnapshot::with_items(
            &s.id,
            vec![item("/a.csv", 1, 100), item("/b.csv", 2, 200)],
        );
        let diff = diff_snapshots(Some(&prev), &curr);

        // 用 mock hooks 让 submit 失败(wait 阶段模拟超时错误)
        let hooks = MockHooks {
            wait_errors: Mutex::new(HashMap::new()),
            ..Default::default()
        };
        // 把"a.csv"和"b.csv"两个 task_id 都注入"超时"错误
        {
            let mut waits = hooks.wait_errors.lock().unwrap();
            // 单文件路径会先 submit → tx-1, tx-2,再 wait 各自
            // 实际 task_id 由 submit 顺序决定:tx-1, tx-2
            waits.insert(
                "tx-1".to_string(),
                ShareSyncError::DownloadError("等待任务完成超时".to_string()),
            );
            waits.insert(
                "tx-2".to_string(),
                ShareSyncError::DownloadError("等待任务完成超时".to_string()),
            );
        }
        let ex = ShareSyncExecutor::new(&s, &pm, &hooks);

        // 模拟"wait 超时后 downloader 实际把文件落盘":
        // 在调 apply 之前先创建好文件,这样 recovery 阶段能查到
        std::fs::write(local_root.join("a.csv"), b"data-a").unwrap();
        std::fs::write(local_root.join("b.csv"), b"data-b").unwrap();

        // 调单文件路径(不走 tree)— 用 apply_with_run_id 而不是 apply
        // 因为我们的 mock 单文件路径会触发 process_added_or_modified 的重试,
        // 重试用尽后才标 failed,这样就能完整测到 marker 失败 → recovery 升级
        let run_id = "test-recover-run".to_string();
        let _outcome = ex.apply_with_run_id(run_id.clone(), &captured(), &diff).await;

        // DB 里所有项都应是 completed(recovery 升级)
        let items = pm.list_run_items(&run_id).unwrap();
        assert_eq!(items.len(), 2, "应记录 2 个 run_item");
        for it in &items {
            assert_eq!(
                it.status, "completed",
                "wait 超时后文件已落盘 → recovery 升级为 completed: path={}",
                it.path
            );
        }

        // run 终态应是 Completed(因为 summary.failed == 0)
        // (apply_with_run_id 路径返回的 outcome.status,以及 DB 里存的 share_sync_runs.status)
        let run = pm
            .get_run(&run_id)
            .expect("run 应能查到")
            .expect("run 应存在");
        assert_eq!(
            run.status, "completed",
            "所有失败项都被 recovery 升级 → run 应是 completed,实际是: {}",
            run.status
        );
        assert_eq!(run.failed_count, 0, "failed_count 应为 0");
    }
}
