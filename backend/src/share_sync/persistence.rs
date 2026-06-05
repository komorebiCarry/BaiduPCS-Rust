//! 分享同步持久化层
//!
//! 单一职责：管理 `share_sync.db`（SQLite），承载订阅元数据、快照、运行历史。
//! 与 autobackup 模式一致：`Mutex<Connection>` 串行化访问；WAL 模式；外键级联。

use crate::share_sync::config::ShareSubscription;
use crate::share_sync::error::ShareSyncError;
use crate::share_sync::snapshot::ShareSnapshot;
use crate::share_sync::types::{DiffSummary, RunItemStatus, RunStatus, SyncAction, TargetKind};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

/// 默认分页
pub const DEFAULT_PAGE_SIZE: usize = 50;
pub const MAX_PAGE_SIZE: usize = 500;

pub fn normalize_pagination(page: Option<usize>, page_size: Option<usize>) -> (usize, usize) {
    let p = page.unwrap_or(1).max(1);
    let ps = page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    (p, ps)
}

/// 持久化管理器
pub struct ShareSyncPersistence {
    conn: std::sync::Mutex<Connection>,
    db_path: PathBuf,
}

impl std::fmt::Debug for ShareSyncPersistence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareSyncPersistence")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
    }
}

impl ShareSyncPersistence {
    /// 打开数据库（不存在则创建）
    pub fn new(db_path: &Path) -> Result<Self, ShareSyncError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path).map_err(|e| {
            error!("打开 share_sync.db 失败: {}", e);
            ShareSyncError::PersistenceError(e.to_string())
        })?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;
             PRAGMA foreign_keys=ON;",
        )?;
        let mgr = Self {
            conn: std::sync::Mutex::new(conn),
            db_path: db_path.to_path_buf(),
        };
        mgr.init_tables()?;
        info!("ShareSyncPersistence 初始化完成: db={:?}", db_path);
        Ok(mgr)
    }

    /// 初始化表结构
    fn init_tables(&self) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS share_subscriptions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                share_url TEXT NOT NULL,
                password TEXT,
                config_json TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_share_subs_enabled
              ON share_subscriptions(enabled);

            CREATE TABLE IF NOT EXISTS share_snapshots (
                id TEXT PRIMARY KEY,
                subscription_id TEXT NOT NULL,
                captured_at INTEGER NOT NULL,
                item_count INTEGER NOT NULL,
                FOREIGN KEY (subscription_id) REFERENCES share_subscriptions(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_share_snapshots_sub_time
              ON share_snapshots(subscription_id, captured_at DESC);

            CREATE TABLE IF NOT EXISTS share_snapshot_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT NOT NULL,
                path TEXT NOT NULL,
                fs_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                is_dir INTEGER NOT NULL,
                name TEXT NOT NULL,
                UNIQUE(snapshot_id, path),
                FOREIGN KEY (snapshot_id) REFERENCES share_snapshots(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_share_snap_items_snap
              ON share_snapshot_items(snapshot_id);

            CREATE TABLE IF NOT EXISTS share_sync_runs (
                id TEXT PRIMARY KEY,
                subscription_id TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                finished_at INTEGER,
                status TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                added_count INTEGER NOT NULL DEFAULT 0,
                modified_count INTEGER NOT NULL DEFAULT 0,
                removed_count INTEGER NOT NULL DEFAULT 0,
                unchanged_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                overwritten_count INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                FOREIGN KEY (subscription_id) REFERENCES share_subscriptions(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_share_runs_sub_time
              ON share_sync_runs(subscription_id, started_at DESC);

            CREATE TABLE IF NOT EXISTS share_sync_run_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                path TEXT NOT NULL,
                action TEXT NOT NULL,
                target TEXT NOT NULL,
                transfer_task_id TEXT,
                download_task_id TEXT,
                status TEXT NOT NULL,
                versioned_old_path TEXT,
                error TEXT,
                reason TEXT,
                FOREIGN KEY (run_id) REFERENCES share_sync_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_share_run_items_run
              ON share_sync_run_items(run_id);
            "#,
        )?;
        // 兼容老库：老版本的 share_sync_run_items 表没有 reason 列。
        // SQLite 的 `ADD COLUMN` 在列已存在时会报 "duplicate column" 错误，
        // 这里用 `try_exec` 风格的 "忽略特定错误" 模式做幂等迁移。
        // 失败时只记 warn，不影响表结构初始化。
        if let Err(e) = conn.execute(
            "ALTER TABLE share_sync_run_items ADD COLUMN reason TEXT",
            [],
        ) {
            let msg = e.to_string();
            if !msg.contains("duplicate column name") {
                warn!(
                    "迁移 share_sync_run_items.reason 列失败（已存在可忽略）: {}",
                    e
                );
            }
        }
        for ddl in [
            "ALTER TABLE share_sync_runs ADD COLUMN total_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE share_sync_runs ADD COLUMN unchanged_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE share_sync_runs ADD COLUMN skipped_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE share_sync_runs ADD COLUMN overwritten_count INTEGER NOT NULL DEFAULT 0",
        ] {
            if let Err(e) = conn.execute(ddl, []) {
                let msg = e.to_string();
                if !msg.contains("duplicate column name") {
                    warn!("迁移 share_sync_runs 统计列失败（已存在可忽略）: {}", e);
                }
            }
        }
        Ok(())
    }

    // ===================================================
    // Subscription 元数据
    // ===================================================

    /// 写入订阅（id 已存在则覆盖）
    pub fn upsert_subscription(&self, sub: &ShareSubscription) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let config_json = serde_json::to_string(sub)?;
        let password = sub.password.clone().unwrap_or_default();
        let enabled = if sub.enabled { 1 } else { 0 };
        let created = sub.created_at.timestamp();
        let updated = sub.updated_at.timestamp();
        conn.execute(
            r#"INSERT INTO share_subscriptions
               (id, name, share_url, password, config_json, enabled, created_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name,
                 share_url=excluded.share_url,
                 password=excluded.password,
                 config_json=excluded.config_json,
                 enabled=excluded.enabled,
                 updated_at=excluded.updated_at"#,
            params![
                sub.id,
                sub.name,
                sub.share_url,
                password,
                config_json,
                enabled,
                created,
                updated
            ],
        )?;
        Ok(())
    }

    /// 读取订阅
    pub fn get_subscription(&self, id: &str) -> Result<Option<ShareSubscription>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let json: Option<String> = conn
            .query_row(
                "SELECT config_json FROM share_subscriptions WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        match json {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    /// 列出所有订阅（按 created_at DESC）
    pub fn list_subscriptions(&self) -> Result<Vec<ShareSubscription>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT config_json FROM share_subscriptions ORDER BY created_at DESC")?;
        let rows = stmt.query_map([], |row| {
            let s: String = row.get(0)?;
            Ok(s)
        })?;
        let mut out = Vec::new();
        for r in rows {
            let s: String = r?;
            match serde_json::from_str::<ShareSubscription>(&s) {
                Ok(sub) => out.push(sub),
                Err(e) => warn!("反序列化订阅失败: {}", e),
            }
        }
        Ok(out)
    }

    /// 删除订阅（级联删除 snapshots/runs/run_items）
    pub fn delete_subscription(&self, id: &str) -> Result<bool, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute("DELETE FROM share_subscriptions WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }

    // ===================================================
    // Snapshots
    // ===================================================

    /// 保存一次完整快照
    pub fn save_snapshot(&self, snap: &ShareSnapshot) -> Result<(), ShareSyncError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO share_snapshots
             (id, subscription_id, captured_at, item_count)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                snap.id,
                snap.subscription_id,
                snap.captured_at.timestamp(),
                snap.items.len() as i64
            ],
        )?;
        // 简化：每次保存前先清空该 snapshot 的 items（防止 ON CONFLICT 触发 unique 冲突）
        tx.execute(
            "DELETE FROM share_snapshot_items WHERE snapshot_id = ?1",
            params![snap.id],
        )?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO share_snapshot_items
                 (snapshot_id, path, fs_id, size, is_dir, name)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for item in &snap.items {
                stmt.execute(params![
                    snap.id,
                    item.path,
                    item.fs_id as i64,
                    item.size as i64,
                    item.is_dir as i32,
                    item.name
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// 获取某订阅最近的快照
    pub fn latest_snapshot(
        &self,
        subscription_id: &str,
    ) -> Result<Option<ShareSnapshot>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT id, captured_at FROM share_snapshots
                 WHERE subscription_id = ?1
                 ORDER BY captured_at DESC LIMIT 1",
                params![subscription_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((id, captured_at)) = row else {
            return Ok(None);
        };
        let mut stmt = conn.prepare(
            "SELECT path, fs_id, size, is_dir, name
             FROM share_snapshot_items WHERE snapshot_id = ?1",
        )?;
        let items = stmt
            .query_map(params![id], |row| {
                let path: String = row.get(0)?;
                let fs_id: i64 = row.get(1)?;
                let size: i64 = row.get(2)?;
                let is_dir: i32 = row.get(3)?;
                let name: String = row.get(4)?;
                Ok(ShareSnapshotItem {
                    raw_path: path.clone(),
                    path,
                    fs_id: fs_id as u64,
                    size: size as u64,
                    is_dir: is_dir != 0,
                    name,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(ShareSnapshot {
            id,
            subscription_id: subscription_id.to_string(),
            captured_at: chrono::DateTime::from_timestamp(captured_at, 0)
                .unwrap_or_else(chrono::Utc::now),
            items,
        }))
    }

    // ===================================================
    // Runs
    // ===================================================

    /// 启动一次运行
    pub fn start_run(
        &self,
        run_id: &str,
        subscription_id: &str,
        started_at: i64,
    ) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO share_sync_runs
             (id, subscription_id, started_at, status)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                run_id,
                subscription_id,
                started_at,
                RunStatus::Running.to_string()
            ],
        )?;
        Ok(())
    }

    /// 完成一次运行
    pub fn finish_run(
        &self,
        run_id: &str,
        finished_at: i64,
        status: RunStatus,
        diff: &DiffSummary,
        error: Option<&str>,
    ) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE share_sync_runs
             SET finished_at = ?1, status = ?2,
                 total_count = ?3,
                 added_count = ?4, modified_count = ?5,
                 removed_count = ?6, unchanged_count = ?7,
                 failed_count = ?8, skipped_count = ?9,
                 overwritten_count = ?10,
                 error = ?11
             WHERE id = ?12",
            params![
                finished_at,
                status.to_string(),
                diff.total as i64,
                diff.added as i64,
                diff.modified as i64,
                diff.removed as i64,
                diff.unchanged as i64,
                diff.failed as i64,
                diff.skipped as i64,
                diff.overwritten as i64,
                error,
                run_id
            ],
        )?;
        Ok(())
    }

    /// 单个 run_item 入库
    ///
    /// `reason` 是 v1 新增字段，用于说明"为什么这条 item 没被真正执行"——
    /// 当前主要给 quota / local_disk_full 早停场景用，记录"skip_due_to_quota_full"
    /// 之类的语义化原因。普通成功 / 正常失败的 item 传 `None`。
    #[allow(clippy::too_many_arguments)]
    pub fn add_run_item(
        &self,
        run_id: &str,
        path: &str,
        action: SyncAction,
        target: TargetKind,
        transfer_task_id: Option<&str>,
        download_task_id: Option<&str>,
        status: RunItemStatus,
        versioned_old_path: Option<&str>,
        reason: Option<&str>,
    ) -> Result<i64, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO share_sync_run_items
             (run_id, path, action, target, transfer_task_id, download_task_id, status, versioned_old_path, reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                run_id, path, action.to_string(), target.to_string(),
                transfer_task_id, download_task_id,
                status.to_string(), versioned_old_path, reason
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// 更新 run_item 状态
    pub fn update_run_item_status(
        &self,
        run_item_id: i64,
        status: RunItemStatus,
        error: Option<&str>,
    ) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE share_sync_run_items SET status = ?1, error = ?2 WHERE id = ?3",
            params![status.to_string(), error, run_item_id],
        )?;
        Ok(())
    }

    /// 单独更新 run_item 的 reason 字段（v1 新增）
    ///
    /// 用于"submit/wait 阶段因 quota 跳过"——此时 `add_run_item` 已经把 status
    /// 写为 `Transferring`/`Downloading`，事后 `update_run_item_status` 改成
    /// `Skipped` 时如果只更新 status 会丢失"为什么跳"的语义信息，所以这里
    /// 单独写 reason 列。
    ///
    /// reason 取值约定：
    /// - `quota_full`        : 网盘空间不足
    /// - `local_disk_full`  : 本地磁盘满
    /// - `skip_due_to_quota_full` / `skip_due_to_local_disk_full` : 早停场景下
    ///   整组未提交的子项（见 `apply_with_run_id_grouped`）
    pub fn set_run_item_reason(
        &self,
        run_item_id: i64,
        reason: Option<&str>,
    ) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE share_sync_run_items SET reason = ?1 WHERE id = ?2",
            params![reason, run_item_id],
        )?;
        Ok(())
    }

    /// 列出某订阅的运行历史（分页）
    pub fn list_runs(
        &self,
        subscription_id: &str,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<RunRecord>, ShareSyncError> {
        let (p, ps) = normalize_pagination(Some(page), Some(page_size));
        let offset = (p - 1) * ps;
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, started_at, finished_at, status,
                    total_count, added_count, modified_count, removed_count,
                    unchanged_count, failed_count, skipped_count, overwritten_count, error
             FROM share_sync_runs
             WHERE subscription_id = ?1
             ORDER BY started_at DESC
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(params![subscription_id, ps as i64, offset as i64], |row| {
            Ok(RunRecord {
                id: row.get(0)?,
                started_at: row.get::<_, i64>(1)?,
                finished_at: row.get::<_, Option<i64>>(2)?,
                status: row.get(3)?,
                total_count: row.get::<_, i64>(4)? as usize,
                added_count: row.get::<_, i64>(5)? as usize,
                modified_count: row.get::<_, i64>(6)? as usize,
                removed_count: row.get::<_, i64>(7)? as usize,
                unchanged_count: row.get::<_, i64>(8)? as usize,
                failed_count: row.get::<_, i64>(9)? as usize,
                skipped_count: row.get::<_, i64>(10)? as usize,
                overwritten_count: row.get::<_, i64>(11)? as usize,
                error: row.get(12)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// 取单个 run 详情
    pub fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let rec = conn
            .query_row(
                "SELECT id, started_at, finished_at, status,
                        total_count, added_count, modified_count, removed_count,
                        unchanged_count, failed_count, skipped_count, overwritten_count, error
                 FROM share_sync_runs WHERE id = ?1",
                params![run_id],
                |row| {
                    Ok(RunRecord {
                        id: row.get(0)?,
                        started_at: row.get::<_, i64>(1)?,
                        finished_at: row.get::<_, Option<i64>>(2)?,
                        status: row.get(3)?,
                        total_count: row.get::<_, i64>(4)? as usize,
                        added_count: row.get::<_, i64>(5)? as usize,
                        modified_count: row.get::<_, i64>(6)? as usize,
                        removed_count: row.get::<_, i64>(7)? as usize,
                        unchanged_count: row.get::<_, i64>(8)? as usize,
                        failed_count: row.get::<_, i64>(9)? as usize,
                        skipped_count: row.get::<_, i64>(10)? as usize,
                        overwritten_count: row.get::<_, i64>(11)? as usize,
                        error: row.get(12)?,
                    })
                },
            )
            .optional()?;
        Ok(rec)
    }

    /// 列出 run 的所有 run_items
    pub fn list_run_items(&self, run_id: &str) -> Result<Vec<RunItemRecord>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, path, action, target, transfer_task_id, download_task_id,
                    status, versioned_old_path, error, reason
             FROM share_sync_run_items
             WHERE run_id = ?1
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![run_id], |row| {
            Ok(RunItemRecord {
                id: row.get::<_, i64>(0)?,
                path: row.get(1)?,
                action: row.get(2)?,
                target: row.get(3)?,
                transfer_task_id: row.get(4)?,
                download_task_id: row.get(5)?,
                status: row.get(6)?,
                versioned_old_path: row.get(7)?,
                error: row.get(8)?,
                reason: row.get(9)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

/// 一次运行的摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub status: String,
    pub total_count: usize,
    pub added_count: usize,
    pub modified_count: usize,
    pub removed_count: usize,
    pub unchanged_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub overwritten_count: usize,
    pub error: Option<String>,
}

/// 单条 run_item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunItemRecord {
    pub id: i64,
    pub path: String,
    pub action: String,
    pub target: String,
    pub transfer_task_id: Option<String>,
    pub download_task_id: Option<String>,
    pub status: String,
    pub versioned_old_path: Option<String>,
    pub error: Option<String>,
    /// v1 新增：说明 item 为什么没被执行（如 `skip_due_to_quota_full`），普通成功/失败为 `None`
    #[serde(default)]
    pub reason: Option<String>,
}

// 把 ShareSnapshotItem 在 persistence 内部用别名重新引用，避免循环 import
use crate::share_sync::snapshot::ShareSnapshotItem;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::config::{NetdiskTarget, SyncTarget};
    use tempfile::tempdir;

    fn fresh() -> (tempfile::TempDir, ShareSyncPersistence) {
        let dir = tempdir().unwrap();
        let p = dir.path().join("share_sync.db");
        let mgr = ShareSyncPersistence::new(&p).unwrap();
        (dir, mgr)
    }

    fn sub(name: &str) -> ShareSubscription {
        let mut s = ShareSubscription::new(
            name.into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        s.password = Some("1234".into());
        s
    }

    #[test]
    fn test_upsert_get_delete_subscription() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();

        let got = mgr.get_subscription(&s.id).unwrap().unwrap();
        assert_eq!(got.name, "a");
        assert_eq!(got.password.as_deref(), Some("1234"));

        mgr.delete_subscription(&s.id).unwrap();
        assert!(mgr.get_subscription(&s.id).unwrap().is_none());
    }

    #[test]
    fn test_upsert_overwrites_existing() {
        let (_dir, mgr) = fresh();
        let mut s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        s.name = "a-renamed".into();
        s.touch();
        mgr.upsert_subscription(&s).unwrap();
        let got = mgr.get_subscription(&s.id).unwrap().unwrap();
        assert_eq!(got.name, "a-renamed");
    }

    #[test]
    fn test_list_subscriptions() {
        let (_dir, mgr) = fresh();
        let mut a = sub("a");
        a.touch();
        let mut b = sub("b");
        b.touch();
        mgr.upsert_subscription(&a).unwrap();
        mgr.upsert_subscription(&b).unwrap();
        let list = mgr.list_subscriptions().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_save_and_get_snapshot() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();

        let snap = ShareSnapshot::with_items(
            &s.id,
            vec![
                ShareSnapshotItem::new("/a.txt", "a.txt", 1, 100, false),
                ShareSnapshotItem::new("/b", "b", 2, 0, true),
            ],
        );
        mgr.save_snapshot(&snap).unwrap();

        let latest = mgr.latest_snapshot(&s.id).unwrap().unwrap();
        assert_eq!(latest.items.len(), 2);
        assert_eq!(latest.items[0].path, "/a.txt");
    }

    #[test]
    fn test_cascade_delete_subscription_removes_snapshots_runs() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        let snap =
            ShareSnapshot::with_items(&s.id, vec![ShareSnapshotItem::new("/x", "x", 1, 1, false)]);
        mgr.save_snapshot(&snap).unwrap();
        mgr.start_run("run-1", &s.id, 1).unwrap();
        mgr.add_run_item(
            "run-1",
            "/x",
            SyncAction::Added,
            TargetKind::Netdisk,
            None,
            None,
            RunItemStatus::Pending,
            None,
            None,
        )
        .unwrap();

        mgr.delete_subscription(&s.id).unwrap();
        assert!(mgr.latest_snapshot(&s.id).unwrap().is_none());
        assert!(mgr.list_runs(&s.id, 1, 10).unwrap().is_empty());
    }

    #[test]
    fn test_run_lifecycle() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        mgr.start_run("run-1", &s.id, 1000).unwrap();
        mgr.add_run_item(
            "run-1",
            "/a",
            SyncAction::Added,
            TargetKind::Netdisk,
            Some("tx-1"),
            None,
            RunItemStatus::Transferring,
            None,
            None,
        )
        .unwrap();
        mgr.finish_run(
            "run-1",
            1100,
            RunStatus::CompletedWithErrors,
            &DiffSummary {
                total: 1,
                added: 1,
                modified: 0,
                removed: 0,
                unchanged: 0,
                failed: 0,
                overwritten: 0,
                skipped: 0,
            },
            None,
        )
        .unwrap();

        let rec = mgr.get_run("run-1").unwrap().unwrap();
        assert_eq!(rec.status, "completed_with_errors");
        assert_eq!(rec.added_count, 1);

        let items = mgr.list_run_items("run-1").unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].transfer_task_id.as_deref(), Some("tx-1"));
    }

    #[test]
    fn test_normalize_pagination() {
        assert_eq!(normalize_pagination(None, None), (1, DEFAULT_PAGE_SIZE));
        assert_eq!(normalize_pagination(Some(0), Some(0)), (1, 1));
        assert_eq!(
            normalize_pagination(Some(2), Some(1000)),
            (2, MAX_PAGE_SIZE)
        );
    }

    #[test]
    fn test_diff_serde_for_persistence() {
        let prev = ShareSnapshot::with_items(
            "sub",
            vec![ShareSnapshotItem::new("/a", "a", 1, 100, false)],
        );
        let curr = ShareSnapshot::with_items(
            "sub",
            vec![
                ShareSnapshotItem::new("/a", "a", 1, 100, false),
                ShareSnapshotItem::new("/b", "b", 2, 50, false),
            ],
        );
        let diff = crate::share_sync::diff::diff_snapshots(Some(&prev), &curr);
        let summary = diff.summary();
        assert_eq!(summary.added, 1);
        assert_eq!(summary.modified, 0);
    }
}
