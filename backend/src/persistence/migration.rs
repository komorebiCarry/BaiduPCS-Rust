//! 多账号迁移矩阵执行器
//!
//! 集中执行迁移步骤：
//!   1. `session.json` → `accounts.json`（已由 `auth::AccountManager::migrate_from_session` 实现）
//!   2. SQLite `backup_configs` 表 `ALTER TABLE ADD COLUMN owner_uid INTEGER`
//!   3. `.meta` 文件回填 `owner_uid`
//!
//! 幂等性守卫（「迁移幂等性约束」硬规则表）：
//!   - 每个步骤通过「探测目标态」决定是否跳过（已存在列、已迁移条目、active_uid 已设置）；
//!   - 任何步骤失败 → 事务回滚 + 标记 `AppState.readonly_mode = true` + 写错误日志；
//!   - **不**自动重试，避免损坏扩散。
//!
//! 注意：本模块只编排「正向迁移」；回滚由外部 backup 还原。

use crate::auth::Uid;
use anyhow::{Context, Result};
use std::path::Path;
use tracing::{info, warn};

/// 迁移结果摘要
#[derive(Debug, Default, Clone)]
pub struct MigrationSummary {
    /// 是否成功完成（任一步骤失败 → false）
    pub success: bool,
    /// 是否执行了 `session.json` → `accounts.json` 迁移
    pub migrated_session: bool,
    /// 是否执行了 SQLite 列 ALTER
    pub altered_schema: bool,
    /// 是否回填了 .meta 文件
    pub backfilled_meta: bool,
    /// 错误信息（若有）
    pub error: Option<String>,
}

/// 判断 SQLite 错误是否为**临时性**的锁 / 占用错误（可短退避重试）。
///
/// 命中条件（不区分大小写，遍历整个错误链）：`database is locked` /
/// `database is busy` / `SQLITE_BUSY` / `SQLITE_LOCKED`。其它错误（文件损坏、
/// 无权限、磁盘满等）一律视为**非临时**，不重试，由调用方进入只读保护。
pub fn is_transient_sqlite_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let s = cause.to_string().to_lowercase();
        s.contains("database is locked")
            || s.contains("database is busy")
            || s.contains("sqlite_busy")
            || s.contains("sqlite_locked")
    })
}

/// 执行 SQLite 迁移：`backup_configs` 添加 `owner_uid` 列。
///
/// 幂等性守卫（迁移矩阵 + 「迁移幂等性约束」表）：
/// 1. **表不存在** → 直接返回 `Ok(false)`，不抛错（`backup_configs` 在
///    JSON-only 部署中可能根本未建表）。
/// 2. **`owner_uid` 列已存在** → 返回 `Ok(false)`，跳过 ALTER。
/// 3. **ALTER 失败但因列已存在（duplicate column name）** → 视为幂等成功。
/// 4. 其它 ALTER 失败 → 返回 `Err` 由调用方进入 `readonly_mode`。
pub fn migrate_backup_configs_schema(db_path: &Path) -> Result<bool> {
    use rusqlite::Connection;

    let conn = Connection::open(db_path).context("打开 SQLite 数据库失败")?;

    // 守卫 1：表是否存在
    let table_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='backup_configs'",
            [],
            |row| row.get(0),
        )
        .context("查询 backup_configs 是否存在失败")?;
    if table_exists == 0 {
        info!("migrate_backup_configs_schema: backup_configs 表不存在，跳过 ALTER");
        return Ok(false);
    }

    // 守卫 2：列是否已存在
    let mut stmt = conn
        .prepare("PRAGMA table_info(backup_configs)")
        .context("查询 backup_configs schema 失败")?;
    let cols: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();

    if cols.iter().any(|c| c == "owner_uid") {
        info!("migrate_backup_configs_schema: owner_uid 列已存在，跳过");
        return Ok(false);
    }

    // ALTER TABLE — 注意 SQLite 不支持事务内 DDL 部分回滚，所以单独执行
    match conn.execute(
        "ALTER TABLE backup_configs ADD COLUMN owner_uid INTEGER",
        [],
    ) {
        Ok(_) => {
            info!("migrate_backup_configs_schema: 已添加 owner_uid 列");
            Ok(true)
        }
        // 守卫 3：列名冲突视为幂等成功（多进程并发执行时可能命中）
        Err(e) if e.to_string().to_lowercase().contains("duplicate column name") => {
            info!("migrate_backup_configs_schema: owner_uid 已被并发迁移添加，幂等跳过");
            Ok(false)
        }
        Err(e) => Err(e).context("ALTER TABLE backup_configs ADD owner_uid 失败"),
    }
}

/// 回填存量 `.meta` 文件的 `owner_uid`。
///
/// 本函数作为兜底入口，扫描 `wal_dir` 下所有 `.meta` 文件，若 `owner_uid`
/// 缺失（`None`）**或者为 `Some(0)`**（旧二改/历史 bug 写入的占位值），则填充
/// 为 `default_uid` 并写回；返回回填条目数。
///
/// **`Some(0)` 也回填**：`Uid` newtype 阻止 u64-Uid 隐式转换（参见
/// `Uid::new` / `Uid::raw` 文档），但旧登录路径漏掉 `set_owner_uid()` 导致
/// 新任务持久化时写入 `owner_uid=0`。
/// 这些任务在前端会显示成 `UID:0`。回填策略：把所有 `0` 当作"未知"，统一填
/// 当前活跃账号 UID（与 LegacyFillActive 分支语义一致）。
///
/// 幂等：仅当原 `owner_uid` 为 `None` 或 `Some(0)` 时才写入；非零的合法 UID
/// 不会被改动。
pub async fn backfill_meta_owner_uid(wal_dir: &Path, default_uid: Uid) -> Result<usize> {
    use crate::persistence::metadata::{load_metadata, save_metadata, scan_metadata_task_ids};

    let task_ids = scan_metadata_task_ids(wal_dir)
        .map_err(|e| anyhow::anyhow!("scan_metadata_task_ids 失败: {e:?}"))?;

    let mut backfilled = 0usize;
    for task_id in task_ids {
        let mut meta = match load_metadata(wal_dir, &task_id) {
            Some(m) => m,
            None => {
                warn!("backfill_meta_owner_uid: 跳过损坏或缺失的 meta task_id={task_id}");
                continue;
            }
        };
        let needs_backfill = match meta.owner_uid {
            None => true,
            Some(0) => true, // 旧登录路径漏 set_owner_uid 留下的占位值
            Some(_) => false,
        };
        if needs_backfill {
            meta.owner_uid = Some(default_uid.raw());
            match save_metadata(wal_dir, &meta) {
                Ok(_) => backfilled += 1,
                Err(e) => warn!("backfill_meta_owner_uid: 保存失败 task_id={task_id}: {e}"),
            }
        }
    }

    if backfilled > 0 {
        info!(
            "backfill_meta_owner_uid: 共回填 {} 条 .meta 的 owner_uid={}（含 None / Some(0)）",
            backfilled,
            default_uid.raw()
        );
    }
    Ok(backfilled)
}

/// 回填 SQLite `task_history` / `folder_history` 表的 `owner_uid` 列。
///
/// 历史表 `owner_uid` 列由 `HistoryDbManager::init_tables` 的 `ALTER TABLE ... ADD COLUMN owner_uid` 增列，
/// 但存量 NULL / 0 行需要回填为 `default_uid`，否则历史完成的下载/上传任务在前端会显示 `UID:0`。
///
/// **重要约束**：
///   - 仅当**单账号系统**（账号数 = 1）时执行回填，否则**直接跳过**。
///   - 原因：多账号场景下，无法仅凭 `default_uid` 判定一个 NULL 行原本属于哪个账号；
///     盲目用 active_uid 一刀切回填会把其他账号的历史任务**错误归并**到 active 账号上。
///   - 多账号老用户应通过 UI（"任务归属修正"）或人工 SQL 单独修正历史记录。
///
/// 幂等：仅 UPDATE `owner_uid IS NULL OR owner_uid = 0` 的行。
///
/// # Arguments
/// * `db_path` - SQLite 数据库路径
/// * `default_uid` - 用于回填的默认 UID（仅在 `account_count == 1` 时使用）
/// * `account_count` - 系统当前账号数；> 1 时本函数直接返回 `(0, 0)`
pub fn backfill_history_owner_uid(
    db_path: &Path,
    default_uid: Uid,
    account_count: usize,
) -> Result<(usize, usize)> {
    use rusqlite::Connection;

    if !db_path.exists() {
        return Ok((0, 0));
    }

    // 🔥 多账号保护：account_count > 1 时跳过回填
    if account_count > 1 {
        info!(
            "backfill_history_owner_uid: 多账号系统（{}个账号），跳过回填 \
             以避免错误归并历史任务（详见 §6.7.9 注释）",
            account_count
        );
        return Ok((0, 0));
    }

    let conn = Connection::open(db_path).context("打开 SQLite 数据库失败")?;

    let mut task_updated = 0usize;
    let mut folder_updated = 0usize;

    // task_history 表 — 表不存在时 query 报错，先存在性探测
    let task_exists: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='task_history'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if task_exists > 0 {
        match conn.execute(
            "UPDATE task_history SET owner_uid = ?1 WHERE owner_uid IS NULL OR owner_uid = 0",
            rusqlite::params![default_uid.raw() as i64],
        ) {
            Ok(n) => task_updated = n,
            Err(e) => warn!("backfill_history_owner_uid: task_history 回填失败: {e}"),
        }
    }

    // folder_history 表
    let folder_exists: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='folder_history'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if folder_exists > 0 {
        match conn.execute(
            "UPDATE folder_history SET owner_uid = ?1 WHERE owner_uid IS NULL OR owner_uid = 0",
            rusqlite::params![default_uid.raw() as i64],
        ) {
            Ok(n) => folder_updated = n,
            Err(e) => warn!("backfill_history_owner_uid: folder_history 回填失败: {e}"),
        }
    }

    if task_updated > 0 || folder_updated > 0 {
        info!(
            "backfill_history_owner_uid: 回填 task_history={} folder_history={} → uid={}",
            task_updated,
            folder_updated,
            default_uid.raw()
        );
    }
    Ok((task_updated, folder_updated))
}

/// 完整迁移矩阵入口。
///
/// 调用顺序：
///   1. session.json → accounts.json（由 AccountManager 在 AppState::new 时执行）
///   2. SQLite ALTER（本函数）
///   3. .meta 回填（仅当有 active_uid 时）
///   4. SQLite history 回填（仅当有 active_uid 时）
///
/// 任意步骤失败 → 返回 `Err`；调用方应：
///   - 设置 `state.readonly_mode = true`
///   - 写错误日志
///   - **不**重试
pub async fn run_migration_matrix(
    db_path: &Path,
    wal_dir: &Path,
    default_uid: Option<Uid>,
    account_count: usize,
) -> Result<MigrationSummary> {
    let mut summary = MigrationSummary::default();

    // Step 1: session → accounts （已由 AccountManager::migrate_from_session 在启动时处理）
    summary.migrated_session = true;

    // Step 2: SQLite schema
    if db_path.exists() {
        match migrate_backup_configs_schema(db_path) {
            Ok(altered) => summary.altered_schema = altered,
            Err(e) => {
                summary.success = false;
                summary.error = Some(format!("schema migration 失败: {e}"));
                return Err(e);
            }
        }
    } else {
        info!(
            "run_migration_matrix: db_path={:?} 不存在，跳过 schema 迁移",
            db_path
        );
    }

    // Step 3: .meta 回填（仅当有 active_uid 时）
    if let Some(uid) = default_uid {
        if wal_dir.exists() {
            match backfill_meta_owner_uid(wal_dir, uid).await {
                Ok(n) => summary.backfilled_meta = n > 0,
                Err(e) => {
                    summary.success = false;
                    summary.error = Some(format!(".meta 回填失败: {e}"));
                    return Err(e);
                }
            }
        }

        // Step 4: SQLite history 回填
        // 不致命：失败仅日志告警，不进入 readonly。表不存在 / 多账号则跳过。
        if db_path.exists() {
            match backfill_history_owner_uid(db_path, uid, account_count) {
                Ok((t, f)) => {
                    if t > 0 || f > 0 {
                        summary.backfilled_meta = true;
                    }
                }
                Err(e) => warn!("backfill_history_owner_uid 失败（非致命）: {e}"),
            }
        }
    }

    summary.success = true;
    info!(
        "run_migration_matrix: 完成 migrated_session={} altered_schema={} backfilled_meta={}",
        summary.migrated_session, summary.altered_schema, summary.backfilled_meta
    );
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_classifier_matches_lock_and_busy_only() {
        let locked = anyhow::anyhow!("error: database is locked");
        let busy = anyhow::anyhow!("SQLITE_BUSY: database is busy");
        assert!(is_transient_sqlite_error(&locked));
        assert!(is_transient_sqlite_error(&busy));

        // 错误链中任一层命中即算临时
        let chained = anyhow::anyhow!("io error").context("database is locked while ALTER");
        assert!(is_transient_sqlite_error(&chained));

        // 非临时错误不应误判
        let corrupt = anyhow::anyhow!("database disk image is malformed");
        let perm = anyhow::anyhow!("permission denied (os error 13)");
        assert!(!is_transient_sqlite_error(&corrupt));
        assert!(!is_transient_sqlite_error(&perm));
    }
}
