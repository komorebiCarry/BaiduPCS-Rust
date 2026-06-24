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

/// 给 `share_sync_run_items(run_id, path, target)` 建 UNIQUE 索引。
///
/// 唯一键含 `target`（target_kind：netdisk / local）：一个文件在"网盘 + 本地"
/// 共存的订阅里会产生两条动作（转存到网盘 + 下载到本地），若只按 (run_id, path)
/// 唯一，两条会互相覆盖，运行详情里的 target/status/task_id 不可信。加上 target
/// 后两条独立存在、各自可查。
///
/// 老库可能积累过重复行 — quota 退化、并发批量重试都会把同一三元组插多次；早期
/// 版本还建过 `(run_id, path)` 的唯一索引。`CREATE UNIQUE INDEX` 在已有重复时会
/// 直接报错把启动卡死,所以这里先按 `(run_id, path, target)` 去重(保留最大 id,
/// 即最近一次写入,状态最新), 删掉旧的二元唯一索引, 再建三元索引。
fn ensure_run_items_unique_index(conn: &Connection) -> Result<(), ShareSyncError> {
    // 新三元索引已存在则直接返回(幂等);避免对大表反复跑 DELETE 扫描
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_share_run_items_run_path_target'",
            [],
            |row| row.get::<_, i64>(0).map(|_| true),
        )
        .optional()?
        .unwrap_or(false);
    if exists {
        return Ok(());
    }
    // 删除"同一 (run_id, path, target) 的重复行,只保留 id 最大的那条"
    let deleted = conn.execute(
        "DELETE FROM share_sync_run_items
         WHERE id NOT IN (
             SELECT MAX(id) FROM share_sync_run_items GROUP BY run_id, path, target
         )",
        [],
    )?;
    if deleted > 0 {
        warn!(
            "share_sync schema 迁移: 去除 {} 条 (run_id, path, target) 重复 run_item",
            deleted
        );
    }
    // 旧的二元唯一索引（早期版本建的）会阻止"同 path 不同 target"两条共存,必须删掉
    conn.execute("DROP INDEX IF EXISTS idx_share_run_items_run_path", [])?;
    conn.execute(
        "CREATE UNIQUE INDEX idx_share_run_items_run_path_target
         ON share_sync_run_items(run_id, path, target)",
        [],
    )?;
    Ok(())
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
                owner_uid INTEGER NOT NULL DEFAULT 0,
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
            -- v2: status + started_at 联合索引,给 mark_stale_runs_failed 扫表用
            CREATE INDEX IF NOT EXISTS idx_share_runs_status_started
              ON share_sync_runs(status, started_at);
            "#,
        )?;
        // v2: (run_id, path) UNIQUE 索引 — 老库可能已有重复行（quota 退化路径
        // 同一 (run_id, path) 会被插两次,见 executor.rs 的 grouped 分支），
        // 直接 CREATE UNIQUE INDEX 会报错让服务起不来。**先去重再建索引**:
        // 同一 (run_id, path) 保留最大 id（最近一次写入,状态最新）。
        ensure_run_items_unique_index(&conn)?;
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
            // 多账号隔离：老库（含早期把 share_subscriptions 建在独立库的版本）补 owner_uid 列
            "ALTER TABLE share_subscriptions ADD COLUMN owner_uid INTEGER NOT NULL DEFAULT 0",
        ] {
            if let Err(e) = conn.execute(ddl, []) {
                let msg = e.to_string();
                if !msg.contains("duplicate column name") {
                    warn!("迁移 share_sync_runs 统计列失败（已存在可忽略）: {}", e);
                }
            }
        }
        // owner_uid 索引必须在上面的 ALTER 补列**之后**建：老库的 share_subscriptions
        // 没有 owner_uid 列，若放进前面的建表 batch 里会因 "no such column" 整体失败。
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_share_subs_owner
             ON share_subscriptions(owner_uid)",
            [],
        )?;
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
               (id, owner_uid, name, share_url, password, config_json, enabled, created_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
               ON CONFLICT(id) DO UPDATE SET
                 owner_uid=excluded.owner_uid,
                 name=excluded.name,
                 share_url=excluded.share_url,
                 password=excluded.password,
                 config_json=excluded.config_json,
                 enabled=excluded.enabled,
                 updated_at=excluded.updated_at"#,
            params![
                sub.id,
                sub.owner_uid,
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

    /// 列出归属指定账号(owner_uid)的订阅（多账号隔离，按 created_at DESC）
    pub fn list_subscriptions_for_owner(
        &self,
        owner_uid: u64,
    ) -> Result<Vec<ShareSubscription>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT config_json FROM share_subscriptions \
             WHERE owner_uid = ?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![owner_uid], |row| {
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
            "INSERT OR IGNORE INTO share_sync_runs
             (id, subscription_id, started_at, status)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                run_id,
                subscription_id,
                started_at,
                RunStatus::Running.as_str()
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
                status.as_str(),
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

    /// 启动期自愈:把 `status='running'` 且 `started_at < now - cutoff_minutes*60`
    /// 的 run 修复为 `Failed`,返回被收编的 run_id 列表(供 manager 广播事件)。
    ///
    /// 旧版本没有 stale-run 检测,服务崩溃 / 强制重启 / 进程被 kill 都会让内存
    /// 里的 manager task 死掉但 db 行 status 留在 `running`,前端永远看到"运行中"。
    /// 这个函数在 ShareSyncManager::start 启动时被调一次。
    ///
    /// 阈值 `cutoff_minutes` 选定逻辑:任何 run 跑得比"P95 × 3"还久就大概率
    /// 是 stale 的;d17ae3f1 那次卡 1h47min 是已知漏网,默认 120 分钟兼顾安全
    /// 与长 run 容忍。可通过 env `BAIDUPCS_STALE_FIXUP_ENABLED=0` 在 manager
    /// 层关闭该调用(本函数本身不读 env)。
    pub fn mark_stale_runs_failed(
        &self,
        cutoff_minutes: i64,
    ) -> Result<Vec<StaleRunRecord>, ShareSyncError> {
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - cutoff_minutes * 60;
        let conn = self.conn.lock().unwrap();
        // 先 SELECT 拿到将被收编的 run 列表(连带 subscription_id, started_at 给事件用)
        // LEFT JOIN 订阅表带出 owner_uid（订阅可能已被删 → COALESCE 回退 0）
        let mut stmt = conn.prepare(
            "SELECT r.id, r.subscription_id, r.started_at, COALESCE(s.owner_uid, 0)
             FROM share_sync_runs r
             LEFT JOIN share_subscriptions s ON s.id = r.subscription_id
             WHERE r.status = ?1 AND r.started_at < ?2",
        )?;
        let rows = stmt.query_map(params![RunStatus::Running.as_str(), cutoff], |row| {
            Ok(StaleRunRecord {
                run_id: row.get(0)?,
                subscription_id: row.get(1)?,
                started_at: row.get(2)?,
                owner_uid: row.get::<_, i64>(3)? as u64,
            })
        })?;
        let mut stale = Vec::new();
        for r in rows {
            stale.push(r?);
        }
        drop(stmt);
        if stale.is_empty() {
            return Ok(stale);
        }
        // 一条 SQL 批量 UPDATE,error 字段填 stale_run_killed_on_startup
        let updated = conn.execute(
            "UPDATE share_sync_runs
             SET status = ?1,
                 finished_at = ?2,
                 error = ?3
             WHERE status = ?4 AND started_at < ?5",
            params![
                RunStatus::Failed.as_str(),
                now,
                "stale_run_killed_on_startup",
                RunStatus::Running.as_str(),
                cutoff,
            ],
        )?;
        info!(
            "share_sync 启动自愈: 收编 {} 条 stale running run (cutoff={}min)",
            updated, cutoff_minutes
        );
        Ok(stale)
    }

    /// 启动期把**所有** `status='running'` 的 run 标记为 `Interrupted`,返回被收编
    /// 的 run 列表(供 manager 在启动时自动重跑)。
    ///
    /// 与 `mark_stale_runs_failed` 的区别:
    /// - 不标 `Failed` 而是 `Interrupted`(中断,非失败)——进程重启打断的 run 不该
    ///   显示成失败(用户反馈:"怎么能直接标记失败呢")。
    /// - 不设 cutoff:`ShareSyncManager::new` 只在进程启动时调一次,那一刻任何
    ///   `running` 行都必然是上次进程残留的孤儿(内存里的 run task 已随进程退出),
    ///   所以全部收编,manager 随后对其所属(且启用)的订阅自动重跑一次。
    /// - 同步是增量的(基线快照只在成功后推进),被中断的 run 没推进基线,重跑会
    ///   重新 diff 把没跑完的项补上。
    pub fn mark_running_runs_interrupted(&self) -> Result<Vec<StaleRunRecord>, ShareSyncError> {
        let now = chrono::Utc::now().timestamp();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT r.id, r.subscription_id, r.started_at, COALESCE(s.owner_uid, 0)
             FROM share_sync_runs r
             LEFT JOIN share_subscriptions s ON s.id = r.subscription_id
             WHERE r.status = ?1",
        )?;
        let rows = stmt.query_map(params![RunStatus::Running.as_str()], |row| {
            Ok(StaleRunRecord {
                run_id: row.get(0)?,
                subscription_id: row.get(1)?,
                started_at: row.get(2)?,
                owner_uid: row.get::<_, i64>(3)? as u64,
            })
        })?;
        let mut interrupted = Vec::new();
        for r in rows {
            interrupted.push(r?);
        }
        drop(stmt);
        if interrupted.is_empty() {
            return Ok(interrupted);
        }
        let updated = conn.execute(
            "UPDATE share_sync_runs
             SET status = ?1,
                 finished_at = ?2,
                 error = ?3
             WHERE status = ?4",
            params![
                RunStatus::Interrupted.as_str(),
                now,
                "interrupted_on_restart",
                RunStatus::Running.as_str(),
            ],
        )?;
        info!(
            "share_sync 启动自愈: 收编 {} 条中断 run,将自动重跑",
            updated
        );
        Ok(interrupted)
    }

    ///
    /// `reason` 是 v1 新增字段，用于说明"为什么这条 item 没被真正执行"——
    /// 当前主要给 quota / local_disk_full 早停场景用，记录"skip_due_to_quota_full"
    /// 之类的语义化原因。普通成功 / 正常失败的 item 传 `None`。
    ///
    /// v2: 内部走 `INSERT ... ON CONFLICT(run_id, path, target) DO UPDATE ... RETURNING id`,
    /// 同一 (run_id, path, target) 重复调用会**覆盖**前一次的所有字段并返回原 row id。
    /// 这是 quota 退化 / 二分递归 / 并发批量重试场景下"状态以最后一次写入为准"
    /// 的关键 — 老实现是裸 INSERT 会塞重复行,没有 UNIQUE 约束, 列表 API 拉出来会
    /// 有多份。唯一键含 `target` 是为了让"网盘 + 本地"共存订阅里同一文件的两条动作
    /// （转存 / 下载）各自独立、互不覆盖。
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
        let row_id: i64 = conn.query_row(
            "INSERT INTO share_sync_run_items
             (run_id, path, action, target, transfer_task_id, download_task_id, status, versioned_old_path, reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(run_id, path, target) DO UPDATE SET
                 action = excluded.action,
                 target = excluded.target,
                 transfer_task_id = COALESCE(excluded.transfer_task_id, share_sync_run_items.transfer_task_id),
                 download_task_id = COALESCE(excluded.download_task_id, share_sync_run_items.download_task_id),
                 status = excluded.status,
                 versioned_old_path = COALESCE(excluded.versioned_old_path, share_sync_run_items.versioned_old_path),
                 reason = COALESCE(excluded.reason, share_sync_run_items.reason)
             RETURNING id",
            params![
                run_id, path, action.as_str(), target.as_str(),
                transfer_task_id, download_task_id,
                status.as_str(), versioned_old_path, reason
            ],
            |row| row.get(0),
        )?;
        Ok(row_id)
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
            params![status.as_str(), error, run_item_id],
        )?;
        Ok(())
    }

    /// 更新 run_item 绑定的任务与状态。
    ///
    /// 分享同步对临时失败做重新提交时，新的 transfer task_id 需要覆盖旧值，
    /// 否则历史记录会指向第一次已经失败的任务。
    pub fn update_run_item_task_state(
        &self,
        run_item_id: i64,
        transfer_task_id: Option<&str>,
        download_task_id: Option<&str>,
        status: RunItemStatus,
        error: Option<&str>,
    ) -> Result<(), ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE share_sync_run_items
             SET transfer_task_id = ?1, download_task_id = ?2, status = ?3, error = ?4
            WHERE id = ?5",
            params![
                transfer_task_id,
                download_task_id,
                status.as_str(),
                error,
                run_item_id
            ],
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

    /// 取某 run 所属的 subscription_id（多账号隔离：用于校验访问者归属）
    pub fn subscription_id_for_run(&self, run_id: &str) -> Result<Option<String>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let sid: Option<String> = conn
            .query_row(
                "SELECT subscription_id FROM share_sync_runs WHERE id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(sid)
    }

    fn map_run_item_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunItemRecord> {
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
    }

    pub fn count_run_items(&self, run_id: &str) -> Result<usize, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM share_sync_run_items WHERE run_id = ?1",
            params![run_id],
            |row| row.get(0),
        )?;
        Ok(total.max(0) as usize)
    }

    /// 分页列出 run_items。用于运行详情页，避免大目录同步一次性返回几千条明细。
    pub fn list_run_items_page(
        &self,
        run_id: &str,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<RunItemRecord>, ShareSyncError> {
        let conn = self.conn.lock().unwrap();
        let offset = (page.saturating_sub(1) * page_size) as i64;
        let mut stmt = conn.prepare(
            "SELECT id, path, action, target, transfer_task_id, download_task_id,
                    status, versioned_old_path, error, reason
             FROM share_sync_run_items
             WHERE run_id = ?1
             ORDER BY id ASC
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(
            params![run_id, page_size as i64, offset],
            Self::map_run_item_row,
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// 列出 run 的所有 run_items。保留给内部测试和少量小数据调用；用户界面应走分页接口。
    pub fn list_run_items(&self, run_id: &str) -> Result<Vec<RunItemRecord>, ShareSyncError> {
        let total = self.count_run_items(run_id)?;
        self.list_run_items_page(run_id, 1, total.max(1))
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

/// 启动自愈被收编的 stale running run 的描述,manager 用来广播 RunFailed 事件
#[derive(Debug, Clone)]
pub struct StaleRunRecord {
    pub run_id: String,
    pub subscription_id: String,
    pub started_at: i64,
    /// 该 run 所属订阅的归属账号（多账号隔离：自愈事件按账号过滤用）
    pub owner_uid: u64,
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
    fn test_owner_uid_roundtrip() {
        let (_dir, mgr) = fresh();
        let mut s = sub("a");
        s.owner_uid = 42;
        mgr.upsert_subscription(&s).unwrap();
        let got = mgr.get_subscription(&s.id).unwrap().unwrap();
        assert_eq!(got.owner_uid, 42);
    }

    #[test]
    fn test_list_subscriptions_for_owner_isolates_accounts() {
        let (_dir, mgr) = fresh();
        // 账号 1 两条，账号 2 一条
        let mut a1 = sub("a1");
        a1.owner_uid = 1;
        a1.touch();
        let mut a2 = sub("a2");
        a2.owner_uid = 1;
        a2.touch();
        let mut b1 = sub("b1");
        b1.owner_uid = 2;
        b1.touch();
        mgr.upsert_subscription(&a1).unwrap();
        mgr.upsert_subscription(&a2).unwrap();
        mgr.upsert_subscription(&b1).unwrap();

        let owner1 = mgr.list_subscriptions_for_owner(1).unwrap();
        assert_eq!(owner1.len(), 2);
        assert!(owner1.iter().all(|s| s.owner_uid == 1));

        let owner2 = mgr.list_subscriptions_for_owner(2).unwrap();
        assert_eq!(owner2.len(), 1);
        assert_eq!(owner2[0].owner_uid, 2);

        // 不存在的账号看不到任何订阅
        assert_eq!(mgr.list_subscriptions_for_owner(999).unwrap().len(), 0);
    }

    #[test]
    fn test_subscription_id_for_run_maps_to_owning_subscription() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        let run_id = "run-1";
        mgr.start_run(run_id, &s.id, 1000).unwrap();

        let mapped = mgr.subscription_id_for_run(run_id).unwrap();
        assert_eq!(mapped.as_deref(), Some(s.id.as_str()));

        // 不存在的 run → None（handler 据此返回 404）
        assert!(mgr
            .subscription_id_for_run("no-such-run")
            .unwrap()
            .is_none());
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
    fn test_run_items_pagination_uses_stable_id_order() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        mgr.start_run("run-page", &s.id, 1000).unwrap();
        for i in 0..125 {
            mgr.add_run_item(
                "run-page",
                &format!("/file-{i:03}.bin"),
                SyncAction::Added,
                TargetKind::Local,
                Some(&format!("tx-{i}")),
                None,
                RunItemStatus::Downloading,
                None,
                None,
            )
            .unwrap();
        }

        assert_eq!(mgr.count_run_items("run-page").unwrap(), 125);
        let first = mgr.list_run_items_page("run-page", 1, 100).unwrap();
        assert_eq!(first.len(), 100);
        assert_eq!(first[0].path, "/file-000.bin");
        assert_eq!(first[99].path, "/file-099.bin");

        let second = mgr.list_run_items_page("run-page", 2, 100).unwrap();
        assert_eq!(second.len(), 25);
        assert_eq!(second[0].path, "/file-100.bin");
        assert_eq!(second[24].path, "/file-124.bin");
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

    /// v2: add_run_item 重复 (run_id, path) 应当走 ON CONFLICT DO UPDATE,
    /// 不抛错、保留原 row id、字段被新值覆盖
    #[test]
    fn test_add_run_item_unique_on_run_path_overwrites() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        mgr.start_run("run-x", &s.id, 1000).unwrap();
        let id1 = mgr
            .add_run_item(
                "run-x",
                "/foo.csv",
                SyncAction::Added,
                TargetKind::Netdisk,
                Some("tx-1"),
                None,
                RunItemStatus::Transferring,
                None,
                None,
            )
            .unwrap();
        // 同一 (run_id, path) 二次写入,期望返回相同 id 且覆盖状态
        let id2 = mgr
            .add_run_item(
                "run-x",
                "/foo.csv",
                SyncAction::Added,
                TargetKind::Netdisk,
                Some("tx-2"),
                Some("dl-2"),
                RunItemStatus::Completed,
                None,
                Some("quota_full"),
            )
            .unwrap();
        assert_eq!(id1, id2, "ON CONFLICT 应当复用同一 row id");
        let items = mgr.list_run_items("run-x").unwrap();
        assert_eq!(items.len(), 1, "(run_id, path) UNIQUE 后只能有一条");
        assert_eq!(items[0].transfer_task_id.as_deref(), Some("tx-2"));
        assert_eq!(items[0].download_task_id.as_deref(), Some("dl-2"));
        assert_eq!(items[0].status, "completed");
        assert_eq!(items[0].reason.as_deref(), Some("quota_full"));
    }

    /// v2: add_run_item 的 COALESCE 行为:第二次传 None 不应覆盖第一次已写入的字段
    #[test]
    fn test_add_run_item_coalesce_keeps_old_fields_on_none() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        mgr.start_run("run-c", &s.id, 1000).unwrap();
        mgr.add_run_item(
            "run-c",
            "/foo",
            SyncAction::Added,
            TargetKind::Netdisk,
            Some("tx-1"),
            Some("dl-1"),
            RunItemStatus::Transferring,
            Some("/old.bak"),
            Some("first_reason"),
        )
        .unwrap();
        // 第二次传 None 给 transfer_task_id / download_task_id / versioned_old_path / reason
        mgr.add_run_item(
            "run-c",
            "/foo",
            SyncAction::Modified,
            TargetKind::Netdisk,
            None,
            None,
            RunItemStatus::Failed,
            None,
            None,
        )
        .unwrap();
        let items = mgr.list_run_items("run-c").unwrap();
        assert_eq!(items.len(), 1);
        // status / action 是 NOT NULL 用 excluded 覆盖
        assert_eq!(items[0].status, "failed");
        assert_eq!(items[0].action, "modified");
        // transfer/download/versioned/reason 通过 COALESCE 保留原值
        assert_eq!(items[0].transfer_task_id.as_deref(), Some("tx-1"));
        assert_eq!(items[0].download_task_id.as_deref(), Some("dl-1"));
        assert_eq!(items[0].versioned_old_path.as_deref(), Some("/old.bak"));
        assert_eq!(items[0].reason.as_deref(), Some("first_reason"));
    }

    /// v2: 同一 (run_id, path) 但不同 target（网盘 + 本地）应当各存一条,互不覆盖。
    /// 唯一键是 (run_id, path, target),这是"网盘 + 本地"共存订阅运行详情可信的前提。
    #[test]
    fn test_add_run_item_netdisk_and_local_coexist() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        mgr.start_run("run-d", &s.id, 1000).unwrap();
        let id_net = mgr
            .add_run_item(
                "run-d",
                "/movie.mkv",
                SyncAction::Added,
                TargetKind::Netdisk,
                Some("tx-net"),
                None,
                RunItemStatus::Completed,
                None,
                None,
            )
            .unwrap();
        let id_local = mgr
            .add_run_item(
                "run-d",
                "/movie.mkv",
                SyncAction::Added,
                TargetKind::Local,
                None,
                Some("dl-local"),
                RunItemStatus::Transferring,
                None,
                None,
            )
            .unwrap();
        assert_ne!(id_net, id_local, "不同 target 应当是两条独立 row");
        let items = mgr.list_run_items("run-d").unwrap();
        assert_eq!(items.len(), 2, "网盘 + 本地各一条,不互相覆盖");
        let net = items.iter().find(|i| i.target == "netdisk").unwrap();
        let local = items.iter().find(|i| i.target == "local").unwrap();
        assert_eq!(net.transfer_task_id.as_deref(), Some("tx-net"));
        assert_eq!(net.status, "completed");
        assert_eq!(local.download_task_id.as_deref(), Some("dl-local"));
        assert_eq!(local.status, "transferring");
    }

    /// v2: mark_stale_runs_failed — 把 status='running' 且 started_at 超阈值的 run
    /// 修复为 Failed, 返回收编列表;运行中且 started_at 在阈值内的 run 不动
    #[test]
    fn test_mark_stale_runs_failed_picks_only_old_running() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        let now = chrono::Utc::now().timestamp();
        // 老 running: 3 小时前(超阈值 120 分钟)
        mgr.start_run("run-old", &s.id, now - 3 * 3600).unwrap();
        // 新 running: 30 分钟前(未超阈值)
        mgr.start_run("run-fresh", &s.id, now - 30 * 60).unwrap();
        // 已完成: 即便很老也不应被收编
        mgr.start_run("run-done", &s.id, now - 5 * 3600).unwrap();
        mgr.finish_run(
            "run-done",
            now - 5 * 3600 + 10,
            RunStatus::Completed,
            &DiffSummary::default(),
            None,
        )
        .unwrap();

        let stale = mgr.mark_stale_runs_failed(120).unwrap();
        let ids: Vec<&str> = stale.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, ["run-old"], "只应收编超阈值的 running run");

        // 校验 db 行确实变成 failed
        let rec = mgr.get_run("run-old").unwrap().unwrap();
        assert_eq!(rec.status, "failed");
        assert_eq!(rec.error.as_deref(), Some("stale_run_killed_on_startup"));
        let fresh = mgr.get_run("run-fresh").unwrap().unwrap();
        assert_eq!(fresh.status, "running");
    }

    /// mark_running_runs_interrupted — 把**所有** running run(不论新旧)标记为
    /// interrupted(非 failed),已完成的 run 不动。供启动续跑用。
    #[test]
    fn test_mark_running_runs_interrupted() {
        let (_dir, mgr) = fresh();
        let s = sub("a");
        mgr.upsert_subscription(&s).unwrap();
        let now = chrono::Utc::now().timestamp();
        // 两条 running:一新一老,都应被收编(进程重启时都是孤儿)
        mgr.start_run("run-old", &s.id, now - 3 * 3600).unwrap();
        mgr.start_run("run-new", &s.id, now - 30).unwrap();
        // 已完成的不动
        mgr.start_run("run-done", &s.id, now - 100).unwrap();
        mgr.finish_run(
            "run-done",
            now - 50,
            RunStatus::Completed,
            &DiffSummary::default(),
            None,
        )
        .unwrap();

        let mut got = mgr.mark_running_runs_interrupted().unwrap();
        let mut ids: Vec<String> = got.drain(..).map(|r| r.run_id).collect();
        ids.sort();
        assert_eq!(ids, ["run-new", "run-old"], "所有 running 都应被收编");
        assert_eq!(got.len(), 0);

        for rid in ["run-old", "run-new"] {
            let rec = mgr.get_run(rid).unwrap().unwrap();
            assert_eq!(rec.status, "interrupted");
            assert_eq!(rec.error.as_deref(), Some("interrupted_on_restart"));
        }
        // 已完成不受影响
        assert_eq!(
            mgr.get_run("run-done").unwrap().unwrap().status,
            "completed"
        );
    }

    /// v2: 老库已有 (run_id, path) 重复行时, init_tables 走 ensure_run_items_unique_index
    /// 应当去重并建索引,而不是把启动卡死
    #[test]
    fn test_init_tables_handles_duplicate_run_items_in_legacy_db() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("legacy.db");
        // 不走 ShareSyncPersistence::new(它会建 UNIQUE), 手动建老结构 + 塞重复
        {
            let conn = Connection::open(&p).unwrap();
            conn.execute_batch(
                "CREATE TABLE share_subscriptions (id TEXT PRIMARY KEY, name TEXT, share_url TEXT, password TEXT, config_json TEXT, enabled INTEGER, created_at INTEGER, updated_at INTEGER);
                 CREATE TABLE share_sync_runs (id TEXT PRIMARY KEY, subscription_id TEXT, started_at INTEGER, finished_at INTEGER, status TEXT, total_count INTEGER DEFAULT 0, added_count INTEGER DEFAULT 0, modified_count INTEGER DEFAULT 0, removed_count INTEGER DEFAULT 0, unchanged_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, skipped_count INTEGER DEFAULT 0, overwritten_count INTEGER DEFAULT 0, error TEXT);
                 CREATE TABLE share_sync_run_items (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, path TEXT, action TEXT, target TEXT, transfer_task_id TEXT, download_task_id TEXT, status TEXT, versioned_old_path TEXT, error TEXT, reason TEXT);"
            ).unwrap();
            conn.execute("INSERT INTO share_sync_runs (id, subscription_id, started_at, status) VALUES ('r1', 'sub1', 1, 'running')", []).unwrap();
            // 同一 (run_id, path) 塞 3 条不同状态
            for status in ["pending", "transferring", "failed"] {
                conn.execute(
                    "INSERT INTO share_sync_run_items (run_id, path, action, target, status) VALUES ('r1', '/dup', 'added', 'netdisk', ?1)",
                    params![status],
                ).unwrap();
            }
        }
        // 走完整 init -> 应当去重并建索引
        let mgr = ShareSyncPersistence::new(&p).unwrap();
        let items = mgr.list_run_items("r1").unwrap();
        assert_eq!(items.len(), 1, "重复行应当被去重");
        assert_eq!(
            items[0].status, "failed",
            "保留 id 最大的那条(最近一次写入)"
        );
        // 再插重复应当走 ON CONFLICT 不抛错
        mgr.add_run_item(
            "r1",
            "/dup",
            SyncAction::Added,
            TargetKind::Netdisk,
            None,
            None,
            RunItemStatus::Completed,
            None,
            None,
        )
        .unwrap();
        let items = mgr.list_run_items("r1").unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, "completed");
    }
}
