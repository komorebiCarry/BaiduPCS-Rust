//! SyncStateManager：同步状态持久化管理
//!
//! 使用 SQLite 持久化双向同步的文件状态（SyncState 表），
//! 提供同步计划生成和状态更新接口。

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

use super::types::*;

/// 同步状态管理器
///
/// 每个 SyncStateManager 实例持有独立的 SQLite 连接，
/// 与现有 BackupRecordManager 使用相同的 DB 模式。
pub struct SyncStateManager {
    db: Mutex<Connection>,
}

impl SyncStateManager {
    /// 创建 SyncStateManager，初始化数据库表
    pub fn new(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("无法打开同步状态数据库: {:?}", db_path))?;

        // 启用 WAL 模式
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;

        // 创建 sync_state 表
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,

                local_mtime INTEGER,
                local_size INTEGER,
                local_exists INTEGER DEFAULT 1,

                remote_mtime INTEGER,
                remote_size INTEGER,
                remote_fs_id INTEGER,
                remote_exists INTEGER DEFAULT 1,

                last_sync_at INTEGER NOT NULL,
                last_sync_direction TEXT,
                c5_detected INTEGER DEFAULT 0,
                tombstoned_at INTEGER,

                UNIQUE(config_id, relative_path)
            );
            CREATE INDEX IF NOT EXISTS idx_sync_state_config ON sync_state(config_id);"
        )?;

        Ok(Self {
            db: Mutex::new(conn),
        })
    }

    /// 获取指定配置的所有 SyncState 行
    pub fn get_all_states(&self, config_id: &str) -> Result<Vec<SyncStateRow>> {
        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT id, config_id, relative_path,
                    local_mtime, local_size, local_exists,
                    remote_mtime, remote_size, remote_fs_id, remote_exists,
                    last_sync_at, last_sync_direction, c5_detected, tombstoned_at
             FROM sync_state WHERE config_id = ?"
        )?;

        let rows = stmt.query_map(params![config_id], |row| {
            Ok(SyncStateRow {
                id: row.get(0)?,
                config_id: row.get(1)?,
                relative_path: row.get(2)?,
                local_mtime: row.get(3)?,
                local_size: row.get(4)?,
                local_exists: row.get::<_, i32>(5)? != 0,
                remote_mtime: row.get(6)?,
                remote_size: row.get(7)?,
                remote_fs_id: row.get(8)?,
                remote_exists: row.get::<_, i32>(9)? != 0,
                last_sync_at: row.get(10)?,
                last_sync_direction: row.get(11)?,
                c5_detected: row.get::<_, i32>(12)? != 0,
                tombstoned_at: row.get(13)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// 获取指定文件的 SyncState
    pub fn get_state(&self, config_id: &str, relative_path: &str) -> Result<Option<SyncStateRow>> {
        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;
        let result = conn.query_row(
            "SELECT id, config_id, relative_path,
                    local_mtime, local_size, local_exists,
                    remote_mtime, remote_size, remote_fs_id, remote_exists,
                    last_sync_at, last_sync_direction, c5_detected, tombstoned_at
             FROM sync_state WHERE config_id = ? AND relative_path = ?",
            params![config_id, relative_path],
            |row| {
                Ok(SyncStateRow {
                    id: row.get(0)?,
                    config_id: row.get(1)?,
                    relative_path: row.get(2)?,
                    local_mtime: row.get(3)?,
                    local_size: row.get(4)?,
                    local_exists: row.get::<_, i32>(5)? != 0,
                    remote_mtime: row.get(6)?,
                    remote_size: row.get(7)?,
                    remote_fs_id: row.get(8)?,
                    remote_exists: row.get::<_, i32>(9)? != 0,
                    last_sync_at: row.get(10)?,
                    last_sync_direction: row.get(11)?,
                    c5_detected: row.get::<_, i32>(12)? != 0,
                    tombstoned_at: row.get(13)?,
                })
            },
        ).optional()?;
        Ok(result)
    }

    /// 单文件同步完成后，更新 SyncState（双侧写入 + 元字段清理）
    ///
    /// 无论上传还是下载，都同时写入 local_* 和 remote_* 的已观测值。
    /// 成功传输时同时清除 c5_detected、tombstoned_at，恢复 exists 标记。
    pub fn update_after_sync(
        &self,
        config_id: &str,
        relative_path: &str,
        observed: &ObservedFileState,
    ) -> Result<()> {
        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;
        let now = chrono::Utc::now().timestamp();

        // COALESCE: 如果传入 None，保留已有值（上传后远端信息未知时避免覆盖）
        conn.execute(
            "INSERT INTO sync_state (
                config_id, relative_path,
                local_mtime, local_size, local_exists,
                remote_mtime, remote_size, remote_fs_id, remote_exists,
                last_sync_at, last_sync_direction, c5_detected, tombstoned_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0, NULL)
            ON CONFLICT(config_id, relative_path) DO UPDATE SET
                local_mtime = COALESCE(?3, local_mtime),
                local_size = COALESCE(?4, local_size),
                local_exists = ?5,
                remote_mtime = COALESCE(?6, remote_mtime),
                remote_size = COALESCE(?7, remote_size),
                remote_fs_id = COALESCE(?8, remote_fs_id),
                remote_exists = ?9,
                last_sync_at = ?10, last_sync_direction = ?11,
                c5_detected = 0, tombstoned_at = NULL",
            params![
                config_id,
                relative_path,
                observed.local_mtime,
                observed.local_size.map(|s| s as i64),
                observed.local_exists as i32,
                observed.remote_mtime,
                observed.remote_size.map(|s| s as i64),
                observed.remote_fs_id.map(|id| id as i64),
                observed.remote_exists as i32,
                now,
                observed.direction.as_str(),
            ],
        )?;

        Ok(())
    }

    /// 批量写入 state_updates（Stage 3 Step 0 使用，事务包裹）
    ///
    /// 每 500 条为一个事务，避免单事务过大。
    /// BothDeleted → 删除该行而非更新。
    pub fn batch_write_state_updates(
        &self,
        config_id: &str,
        updates: &[SyncStateUpdate],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;
        let now = chrono::Utc::now().timestamp();
        let batch_size = 500;

        for chunk in updates.chunks(batch_size) {
            let tx = conn.unchecked_transaction()?;

            for update in chunk {
                if update.reason == StateUpdateReason::BothDeleted {
                    // C3: 两端都删除，清除 SyncState 行
                    tx.execute(
                        "DELETE FROM sync_state WHERE config_id = ? AND relative_path = ?",
                        params![config_id, &update.relative_path],
                    )?;
                    continue;
                }

                // 计算元字段值
                let c5_val: Option<i32> = update.meta.as_ref()
                    .and_then(|m| m.c5_detected)
                    .map(|v| v as i32);
                let tombstoned_val: Option<Option<i64>> = update.meta.as_ref()
                    .and_then(|m| m.tombstoned_at);

                // UPSERT 操作
                let obs = &update.observed;

                // 构建 SQL 时需要处理元字段的条件更新
                if let Some(tombstoned) = tombstoned_val {
                    if let Some(c5) = c5_val {
                        tx.execute(
                            "INSERT INTO sync_state (
                                config_id, relative_path,
                                local_mtime, local_size, local_exists,
                                remote_mtime, remote_size, remote_fs_id, remote_exists,
                                last_sync_at, last_sync_direction, c5_detected, tombstoned_at
                            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                            ON CONFLICT(config_id, relative_path) DO UPDATE SET
                                local_mtime = ?3, local_size = ?4, local_exists = ?5,
                                remote_mtime = ?6, remote_size = ?7, remote_fs_id = ?8, remote_exists = ?9,
                                last_sync_at = ?10, last_sync_direction = ?11,
                                c5_detected = ?12, tombstoned_at = ?13",
                            params![
                                config_id, &update.relative_path,
                                obs.local_mtime, obs.local_size.map(|s| s as i64), obs.local_exists as i32,
                                obs.remote_mtime, obs.remote_size.map(|s| s as i64),
                                obs.remote_fs_id.map(|id| id as i64), obs.remote_exists as i32,
                                now, obs.direction.as_str(), c5, tombstoned,
                            ],
                        )?;
                    } else {
                        tx.execute(
                            "INSERT INTO sync_state (
                                config_id, relative_path,
                                local_mtime, local_size, local_exists,
                                remote_mtime, remote_size, remote_fs_id, remote_exists,
                                last_sync_at, last_sync_direction, tombstoned_at
                            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                            ON CONFLICT(config_id, relative_path) DO UPDATE SET
                                local_mtime = ?3, local_size = ?4, local_exists = ?5,
                                remote_mtime = ?6, remote_size = ?7, remote_fs_id = ?8, remote_exists = ?9,
                                last_sync_at = ?10, last_sync_direction = ?11, tombstoned_at = ?12",
                            params![
                                config_id, &update.relative_path,
                                obs.local_mtime, obs.local_size.map(|s| s as i64), obs.local_exists as i32,
                                obs.remote_mtime, obs.remote_size.map(|s| s as i64),
                                obs.remote_fs_id.map(|id| id as i64), obs.remote_exists as i32,
                                now, obs.direction.as_str(), tombstoned,
                            ],
                        )?;
                    }
                } else if let Some(c5) = c5_val {
                    tx.execute(
                        "INSERT INTO sync_state (
                            config_id, relative_path,
                            local_mtime, local_size, local_exists,
                            remote_mtime, remote_size, remote_fs_id, remote_exists,
                            last_sync_at, last_sync_direction, c5_detected
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                        ON CONFLICT(config_id, relative_path) DO UPDATE SET
                            local_mtime = ?3, local_size = ?4, local_exists = ?5,
                            remote_mtime = ?6, remote_size = ?7, remote_fs_id = ?8, remote_exists = ?9,
                            last_sync_at = ?10, last_sync_direction = ?11, c5_detected = ?12",
                        params![
                            config_id, &update.relative_path,
                            obs.local_mtime, obs.local_size.map(|s| s as i64), obs.local_exists as i32,
                            obs.remote_mtime, obs.remote_size.map(|s| s as i64),
                            obs.remote_fs_id.map(|id| id as i64), obs.remote_exists as i32,
                            now, obs.direction.as_str(), c5,
                        ],
                    )?;
                } else {
                    // 无元字段更新
                    tx.execute(
                        "INSERT INTO sync_state (
                            config_id, relative_path,
                            local_mtime, local_size, local_exists,
                            remote_mtime, remote_size, remote_fs_id, remote_exists,
                            last_sync_at, last_sync_direction
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                        ON CONFLICT(config_id, relative_path) DO UPDATE SET
                            local_mtime = ?3, local_size = ?4, local_exists = ?5,
                            remote_mtime = ?6, remote_size = ?7, remote_fs_id = ?8, remote_exists = ?9,
                            last_sync_at = ?10, last_sync_direction = ?11",
                        params![
                            config_id, &update.relative_path,
                            obs.local_mtime, obs.local_size.map(|s| s as i64), obs.local_exists as i32,
                            obs.remote_mtime, obs.remote_size.map(|s| s as i64),
                            obs.remote_fs_id.map(|id| id as i64), obs.remote_exists as i32,
                            now, obs.direction.as_str(),
                        ],
                    )?;
                }
            }

            tx.commit()?;
        }

        Ok(())
    }

    /// 清除配置的所有同步状态（用于策略切换后重新建基线）
    pub fn reset_by_config(&self, config_id: &str) -> Result<usize> {
        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;
        let deleted = conn.execute(
            "DELETE FROM sync_state WHERE config_id = ?",
            params![config_id],
        )?;
        tracing::info!("重置同步状态: config={}, deleted={}", config_id, deleted);
        Ok(deleted)
    }

    /// 删除配置的所有同步状态（配置删除时级联清理）
    pub fn delete_by_config(&self, config_id: &str) -> Result<usize> {
        self.reset_by_config(config_id)
    }

    /// 查询配置的 tombstone 列表
    pub fn list_tombstones(&self, config_id: &str) -> Result<Vec<TombstoneInfo>> {
        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT relative_path, local_exists, remote_exists,
                    tombstoned_at, c5_detected,
                    local_mtime, local_size, remote_mtime, remote_size
             FROM sync_state
             WHERE config_id = ? AND (local_exists = 0 OR remote_exists = 0)
             ORDER BY tombstoned_at DESC"
        )?;

        let rows = stmt.query_map(params![config_id], |row| {
            let local_exists: bool = row.get::<_, i32>(1)? != 0;
            let _remote_exists: bool = row.get::<_, i32>(2)? != 0;
            let tombstoned_at: Option<i64> = row.get(3)?;
            let c5_detected: bool = row.get::<_, i32>(4)? != 0;

            let (missing_side, surviving_mtime, surviving_size) = if !local_exists {
                ("local".to_string(), row.get::<_, Option<i64>>(7)?, row.get::<_, Option<i64>>(8)?)
            } else {
                ("remote".to_string(), row.get::<_, Option<i64>>(5)?, row.get::<_, Option<i64>>(6)?)
            };

            Ok(TombstoneInfo {
                relative_path: row.get(0)?,
                missing_side,
                tombstoned_at: tombstoned_at.unwrap_or(0),
                c5_detected,
                surviving_mtime,
                surviving_size,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// 获取配置的同步状态统计
    pub fn get_stats(&self, config_id: &str) -> Result<SyncStats> {
        let conn = self.db.lock().map_err(|e| anyhow::anyhow!("DB lock failed: {}", e))?;

        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sync_state WHERE config_id = ?",
            params![config_id],
            |row| row.get(0),
        )?;

        let tombstones: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sync_state WHERE config_id = ? AND (local_exists = 0 OR remote_exists = 0)",
            params![config_id],
            |row| row.get(0),
        )?;

        let c5_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sync_state WHERE config_id = ? AND c5_detected = 1",
            params![config_id],
            |row| row.get(0),
        )?;

        Ok(SyncStats {
            total_tracked: total as usize,
            tombstones: tombstones as usize,
            c5_frozen: c5_count as usize,
        })
    }
}

/// 同步状态统计
#[derive(Debug, Clone, Serialize)]
pub struct SyncStats {
    pub total_tracked: usize,
    pub tombstones: usize,
    pub c5_frozen: usize,
}

use serde::Serialize;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_test_manager() -> (SyncStateManager, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let manager = SyncStateManager::new(tmp.path()).unwrap();
        (manager, tmp)
    }

    #[test]
    fn test_create_and_read() {
        let (mgr, _tmp) = create_test_manager();

        let observed = ObservedFileState {
            local_mtime: Some(1000),
            local_size: Some(512),
            local_exists: true,
            remote_mtime: Some(1001),
            remote_size: Some(512),
            remote_fs_id: Some(12345),
            remote_exists: true,
            direction: SyncDirection::Upload,
        };

        mgr.update_after_sync("cfg1", "test/file.txt", &observed).unwrap();

        let state = mgr.get_state("cfg1", "test/file.txt").unwrap();
        assert!(state.is_some());
        let state = state.unwrap();
        assert_eq!(state.local_mtime, Some(1000));
        assert_eq!(state.local_size, Some(512));
        assert!(state.local_exists);
        assert_eq!(state.remote_mtime, Some(1001));
        assert_eq!(state.remote_fs_id, Some(12345));
        assert!(!state.c5_detected);
        assert!(state.tombstoned_at.is_none());
    }

    #[test]
    fn test_update_clears_meta() {
        let (mgr, _tmp) = create_test_manager();

        // 先写入带 tombstone 的状态
        let tombstone_update = SyncStateUpdate {
            relative_path: "deleted.txt".to_string(),
            observed: ObservedFileState {
                local_mtime: None,
                local_size: None,
                local_exists: false,
                remote_mtime: Some(2000),
                remote_size: Some(1024),
                remote_fs_id: Some(99),
                remote_exists: true,
                direction: SyncDirection::StateOnly,
            },
            reason: StateUpdateReason::Tombstone,
            meta: Some(SyncStateMeta {
                c5_detected: Some(false),
                tombstoned_at: Some(Some(1234567890)),
            }),
        };
        mgr.batch_write_state_updates("cfg1", &[tombstone_update]).unwrap();

        let state = mgr.get_state("cfg1", "deleted.txt").unwrap().unwrap();
        assert!(!state.local_exists);
        assert_eq!(state.tombstoned_at, Some(1234567890));

        // 然后用 update_after_sync 模拟 C4 恢复
        let recovered = ObservedFileState {
            local_mtime: Some(3000),
            local_size: Some(2048),
            local_exists: true,
            remote_mtime: Some(3001),
            remote_size: Some(2048),
            remote_fs_id: Some(100),
            remote_exists: true,
            direction: SyncDirection::Upload,
        };
        mgr.update_after_sync("cfg1", "deleted.txt", &recovered).unwrap();

        let state = mgr.get_state("cfg1", "deleted.txt").unwrap().unwrap();
        assert!(state.local_exists);
        assert!(state.remote_exists);
        assert!(!state.c5_detected);
        assert!(state.tombstoned_at.is_none());
    }

    #[test]
    fn test_batch_delete_both_deleted() {
        let (mgr, _tmp) = create_test_manager();

        // 先创建一个状态行
        let observed = ObservedFileState {
            local_mtime: Some(1000),
            local_size: Some(100),
            local_exists: true,
            remote_mtime: Some(1000),
            remote_size: Some(100),
            remote_fs_id: Some(1),
            remote_exists: true,
            direction: SyncDirection::Upload,
        };
        mgr.update_after_sync("cfg1", "to_delete.txt", &observed).unwrap();
        assert!(mgr.get_state("cfg1", "to_delete.txt").unwrap().is_some());

        // BothDeleted 应该删除该行
        let update = SyncStateUpdate {
            relative_path: "to_delete.txt".to_string(),
            observed: ObservedFileState {
                local_mtime: None,
                local_size: None,
                local_exists: false,
                remote_mtime: None,
                remote_size: None,
                remote_fs_id: None,
                remote_exists: false,
                direction: SyncDirection::StateOnly,
            },
            reason: StateUpdateReason::BothDeleted,
            meta: None,
        };
        mgr.batch_write_state_updates("cfg1", &[update]).unwrap();

        assert!(mgr.get_state("cfg1", "to_delete.txt").unwrap().is_none());
    }

    #[test]
    fn test_reset_by_config() {
        let (mgr, _tmp) = create_test_manager();

        for i in 0..5 {
            let obs = ObservedFileState {
                local_mtime: Some(i * 100),
                local_size: Some(i as u64 * 50),
                local_exists: true,
                remote_mtime: Some(i * 100),
                remote_size: Some(i as u64 * 50),
                remote_fs_id: Some(i as u64),
                remote_exists: true,
                direction: SyncDirection::Upload,
            };
            mgr.update_after_sync("cfg1", &format!("file_{}.txt", i), &obs).unwrap();
        }

        let states = mgr.get_all_states("cfg1").unwrap();
        assert_eq!(states.len(), 5);

        let deleted = mgr.reset_by_config("cfg1").unwrap();
        assert_eq!(deleted, 5);

        let states = mgr.get_all_states("cfg1").unwrap();
        assert!(states.is_empty());
    }

    #[test]
    fn test_list_tombstones() {
        let (mgr, _tmp) = create_test_manager();

        // 正常文件
        let normal = ObservedFileState {
            local_mtime: Some(1000),
            local_size: Some(100),
            local_exists: true,
            remote_mtime: Some(1000),
            remote_size: Some(100),
            remote_fs_id: Some(1),
            remote_exists: true,
            direction: SyncDirection::Upload,
        };
        mgr.update_after_sync("cfg1", "normal.txt", &normal).unwrap();

        // Tombstone (local deleted)
        let tombstone = SyncStateUpdate {
            relative_path: "local_del.txt".to_string(),
            observed: ObservedFileState {
                local_mtime: None,
                local_size: None,
                local_exists: false,
                remote_mtime: Some(2000),
                remote_size: Some(200),
                remote_fs_id: Some(2),
                remote_exists: true,
                direction: SyncDirection::StateOnly,
            },
            reason: StateUpdateReason::Tombstone,
            meta: Some(SyncStateMeta {
                c5_detected: Some(false),
                tombstoned_at: Some(Some(999)),
            }),
        };
        mgr.batch_write_state_updates("cfg1", &[tombstone]).unwrap();

        let tombstones = mgr.list_tombstones("cfg1").unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].relative_path, "local_del.txt");
        assert_eq!(tombstones[0].missing_side, "local");
    }
}
