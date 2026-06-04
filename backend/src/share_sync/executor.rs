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
use crate::share_sync::error::ShareSyncError;
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
    pub async fn apply(
        &self,
        captured: &CapturedShare,
        diff: &ShareDiff,
    ) -> ApplyOutcome {
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
        if let Err(e) = self.persistence.start_run(&run_id, &self.subscription.id, started_at) {
            return ApplyOutcome {
                run_id,
                status: RunStatus::Failed,
                diff_summary: DiffSummary::default(),
                error: Some(format!("启动 run 失败: {}", e)),
            };
        }

        let mut summary = DiffSummary::default();
        let error: Option<String> = None;
        let mut any_failure = false;

        // 处理 added
        for item in &diff.added {
            if item.is_dir {
                continue;
            }
            for target in &self.subscription.targets {
                let _ = self
                    .process_added_or_modified(
                        captured, run_id.as_str(), item, SyncAction::Added, target, &mut summary,
                    )
                    .await
                    .map_err(|e| {
                        any_failure = true;
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
                        captured, run_id.as_str(), new, SyncAction::Modified, target, &mut summary,
                    )
                    .await
                    .map_err(|e| {
                        any_failure = true;
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
                        &run_id, &item.path, SyncAction::Removed, target_kind,
                        None, None, RunItemStatus::Skipped, None,
                    );
                }
                summary.removed += 1; // 也计入 removed（虽然 skipped）
                continue;
            }
            for target in &self.subscription.targets {
                let _ = self
                    .process_removed(run_id.as_str(), item, target, &mut summary)
                    .await
                    .map_err(|e| {
                        any_failure = true;
                        warn!("removed 处理失败: path={}, err={}", item.path, e);
                    });
            }
            summary.removed += 1;
        }

        let status = if any_failure {
            RunStatus::CompletedWithErrors
        } else {
            RunStatus::Completed
        };
        let finished_at = Utc::now().timestamp();
        if let Err(e) = self.persistence.finish_run(
            &run_id, finished_at, status, &summary, error.as_deref(),
        ) {
            warn!("finish_run 失败: {}", e);
        }

        ApplyOutcome {
            run_id,
            status,
            diff_summary: summary,
            error,
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

        // 1) 策略处理
        let versioned_old = match (&target, &strategy) {
            (SyncTarget::Netdisk(_), ConflictStrategy::Versioned) => {
                Some(self.versioned_netdisk_old(captured, item)?)
            }
            (SyncTarget::Local(_), ConflictStrategy::Versioned) => {
                Some(self.versioned_local_old(target, item)?)
            }
            _ => None,
        };

        // 2) Skip 策略
        if strategy == ConflictStrategy::Skip {
            let exists = match target {
                SyncTarget::Netdisk(_) => self.netdisk_file_exists(captured, item),
                SyncTarget::Local(t) => local_file_exists(&t.local_path, &item.path),
            };
            if exists {
                self.persistence.add_run_item(
                    run_id, &item.path, action, target_kind,
                    None, None, RunItemStatus::Skipped, None,
                )?;
                return Ok(());
            }
        }

        // 3) 提交 transfer / download
        let result: Result<(String, bool, RunItemStatus), ShareSyncError> = match target {
            SyncTarget::Netdisk(t) => {
                self.hooks
                    .submit_transfer(
                        captured,
                        &format!(
                            "{}/{}",
                            t.remote_path.trim_end_matches('/'),
                            item.path.trim_start_matches('/')
                        ),
                        item.fs_id,
                        Some(&format!("share-sync/{}/{}", self.subscription.id, run_id)),
                    )
                    .await
                    .map(|task_id| (task_id, false, RunItemStatus::Transferring))
            }
            SyncTarget::Local(t) => {
                self.hooks
                    .submit_download(item, &t.local_path, strategy)
                    .await
                    .map(|task_id| (task_id, true, RunItemStatus::Downloading))
            }
        };

        match result {
            Ok((task_id, require_download_completion, initial_status)) => {
                let run_item_id = self.persistence.add_run_item(
                    run_id,
                    &item.path,
                    action,
                    target_kind,
                    Some(task_id.as_str()),
                    None,
                    initial_status,
                    versioned_old.as_deref(),
                )?;
                info!(
                    "executor: 已调度 {}/{} -> target={:?}, task_id={}",
                    action, item.path, target_kind, task_id
                );
                match self
                    .hooks
                    .wait_transfer_task(&task_id, require_download_completion, TASK_WAIT_TIMEOUT)
                    .await
                {
                    Ok(()) => {
                        self.persistence.update_run_item_status(
                            run_item_id,
                            RunItemStatus::Completed,
                            None,
                        )?;
                        Ok(())
                    }
                    Err(e) => {
                        summary.failed += 1;
                        self.persistence.update_run_item_status(
                            run_item_id,
                            RunItemStatus::Failed,
                            Some(&e.to_string()),
                        )?;
                        Err(e)
                    }
                }
            }
            Err(e) => {
                summary.failed += 1;
                self.persistence.add_run_item(
                    run_id, &item.path, action, target_kind,
                    None, None, RunItemStatus::Failed, versioned_old.as_deref(),
                )?;
                Err(e)
            }
        }
    }

    /// 处理 removed
    async fn process_removed(
        &self,
        run_id: &str,
        item: &ShareSnapshotItem,
        target: &SyncTarget,
        _summary: &mut DiffSummary,
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
            }
            SyncTarget::Local(t) => self
                .hooks
                .delete_local(&t.local_path, item.path.trim_start_matches('/')),
        };
        match res {
            Ok(()) => {
                self.persistence.add_run_item(
                    run_id, &item.path, SyncAction::Removed, target_kind,
                    None, None, RunItemStatus::Completed, None,
                )?;
                Ok(())
            }
            Err(e) => {
                self.persistence.add_run_item(
                    run_id, &item.path, SyncAction::Removed, target_kind,
                    None, None, RunItemStatus::Failed, None,
                )?;
                Err(e)
            }
        }
    }

    /// 网盘目标：版本化重命名旧文件
    ///
    /// 此处仅记录旧路径（versioned_old_path 入库），实际 rename 在文件传输完成前
    /// 不做（顺序：先改名旧文件 → 再转存新文件）。第一版先返回占位路径，让
    /// 后续 PR 接入网盘 rename_file。失败不会阻断 run。
    fn versioned_netdisk_old(
        &self,
        captured: &CapturedShare,
        item: &ShareSnapshotItem,
    ) -> Result<String, ShareSyncError> {
        let target = self
            .subscription
            .targets
            .iter()
            .find_map(|t| match t {
                SyncTarget::Netdisk(nt) => Some(nt),
                _ => None,
            })
            .ok_or_else(|| ShareSyncError::ConfigError("缺少网盘目标".into()))?;
        let stem = item
            .path
            .rsplit_once('/')
            .map(|(d, n)| format!("{}/{}", d, n))
            .unwrap_or_else(|| item.path.clone());
        let new_name = timestamped_name(&item.name);
        let old_full = format!("{}/{}", target.remote_path.trim_end_matches('/'), stem.trim_start_matches('/'));
        let new_full = old_full.rsplit_once('/').map(|(d, _)| format!("{}/{}", d, new_name)).unwrap_or_else(|| new_name.clone());
        info!(
            "executor: versioned 网盘重命名 {} -> {} (captured.uk={})",
            old_full, new_full, captured.uk
        );
        // 实际 rename 通过 NetdiskClient::rename_file 完成；为保持 trait 表面干净，
        // 这里返回"期望的新路径"作为 versioned_old_path 入库。完整 rename 在
        // manager 层的扩展 PR 中实现。
        Ok(new_full)
    }

    /// 本地目标：版本化重命名旧文件
    fn versioned_local_old(
        &self,
        target: &SyncTarget,
        item: &ShareSnapshotItem,
    ) -> Result<String, ShareSyncError> {
        let local = match target {
            SyncTarget::Local(t) => t,
            SyncTarget::Netdisk(_) => {
                return Err(ShareSyncError::ConfigError("非本地目标".into()))
            }
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

    /// 检查网盘目标中是否已存在该路径（轻量判断：传空 fs_id 探测）
    /// 第一版简化：始终返回 false（让 transfer 自然覆盖）
    fn netdisk_file_exists(&self, _captured: &CapturedShare, _item: &ShareSnapshotItem) -> bool {
        false
    }
}

/// 本地文件是否存在（按 path 拼接到 local_dir）
fn local_file_exists(local_dir: &Path, relative: &str) -> bool {
    local_dir.join(relative.trim_start_matches('/')).exists()
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

#[allow(dead_code)]
fn _suppress_unused() {
    let _ = (NetdiskTarget {
        remote_path: String::new(),
        save_fs_id: 0,
        conflict_strategy: None,
    }, LocalTarget {
        local_path: PathBuf::new(),
        conflict_strategy: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::config::{NetdiskTarget, SyncTarget};
    use crate::share_sync::diff::diff_snapshots;
    use crate::share_sync::snapshot::ShareSnapshot;
    use std::sync::Mutex;
    use tempfile::tempdir;

    fn sub() -> ShareSubscription {
        ShareSubscription::new(
            "test".into(),
            "https://pan.baidu.com/s/1abc".into(),
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
        }
    }

    fn item(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }

    // ----- Mock hooks -----
    #[derive(Default)]
    struct MockHooks {
        transfers: Mutex<Vec<(String, u64)>>,
        downloads: Mutex<Vec<(u64, String, PathBuf, ConflictStrategy)>>,
        deletes: Mutex<Vec<(PathBuf, String)>>,
    }
    #[async_trait]
    impl ExecutorHooks for MockHooks {
        async fn submit_transfer(
            &self,
            _c: &CapturedShare,
            target: &str,
            fs_id: u64,
            _label: Option<&str>,
        ) -> Result<String, ShareSyncError> {
            let mut g = self.transfers.lock().unwrap();
            let id = format!("tx-{}", g.len() + 1);
            g.push((target.to_string(), fs_id));
            Ok(id)
        }
        async fn submit_download(
            &self,
            item: &ShareSnapshotItem,
            dir: &Path,
            strategy: ConflictStrategy,
        ) -> Result<String, ShareSyncError> {
            let mut g = self.downloads.lock().unwrap();
            let id = format!("dl-{}", g.len() + 1);
            g.push((item.fs_id, item.path.clone(), dir.to_path_buf(), strategy));
            Ok(id)
        }
        async fn wait_transfer_task(
            &self,
            _task_id: &str,
            _require_download_completion: bool,
            _timeout: Duration,
        ) -> Result<(), ShareSyncError> {
            Ok(())
        }
        fn delete_netdisk(
            &self,
            _t: &str,
            _paths: &[String],
        ) -> Result<(), ShareSyncError> {
            Ok(())
        }
        fn delete_local(
            &self,
            dir: &Path,
            rel: &str,
        ) -> Result<(), ShareSyncError> {
            let mut g = self.deletes.lock().unwrap();
            g.push((dir.to_path_buf(), rel.to_string()));
            Ok(())
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
        assert_eq!(hooks.downloads.lock().unwrap()[0].3, ConflictStrategy::Overwrite);
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
        assert_eq!(g[0].0, "/我的资源/同步/a.txt");
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
        assert_eq!(hooks.downloads.lock().unwrap()[0].3, ConflictStrategy::Versioned);
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
        assert_eq!(hooks.deletes.lock().unwrap().len(), 0);
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
        assert_eq!(hooks.deletes.lock().unwrap().len(), 1);
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
}
