//! 扫描缓存管理器
//!
//! 基于 SQLite 的文件扫描缓存，用于增量扫描比对。
//! - 按 config_id 隔离缓存
//! - 写入攒批（batch_upsert + 自动 flush）
//! - find_changed_files 分批比对 mtime/size

use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use tracing::{debug, info};

/// 缓存文件条目
#[derive(Debug, Clone)]
pub struct CachedFileEntry {
    pub config_id: String,
    pub file_path: String,
    pub relative_path: String,
    pub mtime: i64,
    pub size: i64,
    pub head_md5: Option<String>,
    pub last_scan_at: i64,
}

/// 扫描到的文件元信息（用于比对）
#[derive(Debug, Clone)]
pub struct ScannedFileMeta {
    pub file_path: String,
    pub mtime: i64,
    pub size: i64,
}

/// 扫描缓存管理器
pub struct ScanCacheManager {
    db: Mutex<Connection>,
    pending_writes: Mutex<Vec<CachedFileEntry>>,
    batch_threshold: usize,
    last_flush: Mutex<Instant>,
}

impl ScanCacheManager {
    /// 创建新的扫描缓存管理器
    pub fn new(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;

        let manager = Self {
            db: Mutex::new(conn),
            pending_writes: Mutex::new(Vec::new()),
            batch_threshold: 100,
            last_flush: Mutex::new(Instant::now()),
        };

        manager.init_table()?;
        Ok(manager)
    }

    fn init_table(&self) -> Result<()> {
        let conn = self.db.lock().map_err(|e| anyhow!("锁失败: {}", e))?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS scan_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                mtime INTEGER NOT NULL,
                size INTEGER NOT NULL,
                head_md5 TEXT,
                last_scan_at INTEGER NOT NULL,
                UNIQUE(config_id, file_path)
            );
            CREATE INDEX IF NOT EXISTS idx_scan_cache_config ON scan_cache(config_id);
            "#,
        )?;
        info!("scan_cache 表初始化完成");
        Ok(())
    }

    /// 分批比对，返回变化/新增的文件
    pub fn find_changed_files(
        &self,
        config_id: &str,
        files: &[ScannedFileMeta],
    ) -> Result<Vec<ScannedFileMeta>> {
        // 先 flush 确保缓冲区数据已写入
        self.flush()?;

        if files.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.db.lock().map_err(|e| anyhow!("锁失败: {}", e))?;
        let mut changed = Vec::new();

        // 分批查询，每批 500 个
        for chunk in files.chunks(500) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                "SELECT file_path, mtime, size FROM scan_cache WHERE config_id = ?1 AND file_path IN ({})",
                placeholders.join(",")
            );

            let mut stmt = conn.prepare(&sql)?;

            // 构建参数
            let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            param_values.push(Box::new(config_id.to_string()));
            for f in chunk {
                param_values.push(Box::new(f.file_path.clone()));
            }
            let params_ref: Vec<&dyn rusqlite::types::ToSql> =
                param_values.iter().map(|p| p.as_ref()).collect();

            let mut cached: std::collections::HashMap<String, (i64, i64)> =
                std::collections::HashMap::new();

            let rows = stmt.query_map(params_ref.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;

            for row in rows {
                if let Ok((path, mtime, size)) = row {
                    cached.insert(path, (mtime, size));
                }
            }

            // 比对：不在缓存中或 mtime/size 变化的文件
            for f in chunk {
                match cached.get(&f.file_path) {
                    Some(&(cached_mtime, cached_size)) => {
                        if f.mtime != cached_mtime || f.size != cached_size {
                            changed.push(f.clone());
                        }
                    }
                    None => {
                        changed.push(f.clone());
                    }
                }
            }
        }

        debug!(
            "find_changed_files: config={}, 输入={}, 变化={}",
            config_id,
            files.len(),
            changed.len()
        );
        Ok(changed)
    }

    /// 攒批写入缓存
    pub fn batch_upsert(&self, entries: Vec<CachedFileEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let should_flush = {
            let mut pending = self
                .pending_writes
                .lock()
                .map_err(|e| anyhow!("锁失败: {}", e))?;
            pending.extend(entries);
            let over_threshold = pending.len() >= self.batch_threshold;

            let last = self
                .last_flush
                .lock()
                .map_err(|e| anyhow!("锁失败: {}", e))?;
            let time_exceeded = last.elapsed().as_secs() >= 3;

            over_threshold || time_exceeded
        };

        if should_flush {
            self.flush()?;
        }

        Ok(())
    }

    /// 将缓冲区数据写入数据库
    pub fn flush(&self) -> Result<()> {
        let entries: Vec<CachedFileEntry> = {
            let mut pending = self
                .pending_writes
                .lock()
                .map_err(|e| anyhow!("锁失败: {}", e))?;
            if pending.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *pending)
        };

        let mut conn = self.db.lock().map_err(|e| anyhow!("锁失败: {}", e))?;
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                r#"INSERT OR REPLACE INTO scan_cache
                   (config_id, file_path, relative_path, mtime, size, head_md5, last_scan_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
            )?;

            for e in &entries {
                stmt.execute(params![
                    e.config_id,
                    e.file_path,
                    e.relative_path,
                    e.mtime,
                    e.size,
                    e.head_md5,
                    e.last_scan_at,
                ])?;
            }
        }

        tx.commit()?;

        *self
            .last_flush
            .lock()
            .map_err(|e| anyhow!("锁失败: {}", e))? = Instant::now();

        debug!("scan_cache flush: {} 条记录", entries.len());
        Ok(())
    }

    /// 删除指定配置的所有缓存
    pub fn delete_by_config(&self, config_id: &str) -> Result<usize> {
        self.flush()?;
        let conn = self.db.lock().map_err(|e| anyhow!("锁失败: {}", e))?;
        let deleted = conn.execute(
            "DELETE FROM scan_cache WHERE config_id = ?1",
            params![config_id],
        )?;
        if deleted > 0 {
            info!("删除 config {} 的 {} 条缓存", config_id, deleted);
        }
        Ok(deleted)
    }

    /// 删除单个文件的缓存
    pub fn delete_by_path(&self, config_id: &str, file_path: &str) -> Result<bool> {
        self.flush()?;
        let conn = self.db.lock().map_err(|e| anyhow!("锁失败: {}", e))?;
        let deleted = conn.execute(
            "DELETE FROM scan_cache WHERE config_id = ?1 AND file_path = ?2",
            params![config_id, file_path],
        )?;
        Ok(deleted > 0)
    }

    /// 单条更新（FileWatcher 事件触发）
    pub fn upsert_single(&self, entry: CachedFileEntry) -> Result<()> {
        let conn = self.db.lock().map_err(|e| anyhow!("锁失败: {}", e))?;
        conn.execute(
            r#"INSERT OR REPLACE INTO scan_cache
               (config_id, file_path, relative_path, mtime, size, head_md5, last_scan_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
            params![
                entry.config_id,
                entry.file_path,
                entry.relative_path,
                entry.mtime,
                entry.size,
                entry.head_md5,
                entry.last_scan_at,
            ],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, ScanCacheManager) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test_scan_cache.db");
        let mgr = ScanCacheManager::new(&db_path).unwrap();
        (dir, mgr)
    }

    fn make_entry(config_id: &str, path: &str, mtime: i64, size: i64) -> CachedFileEntry {
        CachedFileEntry {
            config_id: config_id.to_string(),
            file_path: path.to_string(),
            relative_path: path.to_string(),
            mtime,
            size,
            head_md5: None,
            last_scan_at: 1000,
        }
    }

    #[test]
    fn test_empty_cache_all_changed() {
        let (_dir, mgr) = setup();
        let files = vec![
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ];
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert_eq!(changed.len(), 1);
    }

    #[test]
    fn test_cached_no_change() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![make_entry("cfg1", "/a.txt", 100, 50)]).unwrap();
        mgr.flush().unwrap();

        let files = vec![
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ];
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_mtime_changed() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![make_entry("cfg1", "/a.txt", 100, 50)]).unwrap();
        mgr.flush().unwrap();

        let files = vec![
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 200, size: 50 },
        ];
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert_eq!(changed.len(), 1);
    }

    #[test]
    fn test_size_changed() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![make_entry("cfg1", "/a.txt", 100, 50)]).unwrap();
        mgr.flush().unwrap();

        let files = vec![
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 99 },
        ];
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert_eq!(changed.len(), 1);
    }

    #[test]
    fn test_delete_by_config_isolation() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![
            make_entry("cfg1", "/a.txt", 100, 50),
            make_entry("cfg2", "/b.txt", 200, 60),
        ]).unwrap();
        mgr.flush().unwrap();

        mgr.delete_by_config("cfg1").unwrap();

        // cfg1 deleted → file shows as changed
        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ]).unwrap();
        assert_eq!(changed.len(), 1);

        // cfg2 unaffected
        let changed = mgr.find_changed_files("cfg2", &[
            ScannedFileMeta { file_path: "/b.txt".into(), mtime: 200, size: 60 },
        ]).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_upsert_single() {
        let (_dir, mgr) = setup();
        mgr.upsert_single(make_entry("cfg1", "/a.txt", 100, 50)).unwrap();

        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ]).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_batch_auto_flush() {
        let (_dir, mgr) = setup();
        // Insert > batch_threshold entries
        let entries: Vec<CachedFileEntry> = (0..150)
            .map(|i| make_entry("cfg1", &format!("/file_{}.txt", i), i as i64, i as i64))
            .collect();
        mgr.batch_upsert(entries).unwrap();

        // Should have auto-flushed, so find_changed_files sees them
        let files: Vec<ScannedFileMeta> = (0..150)
            .map(|i| ScannedFileMeta {
                file_path: format!("/file_{}.txt", i),
                mtime: i as i64,
                size: i as i64,
            })
            .collect();
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_delete_by_path() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![
            make_entry("cfg1", "/a.txt", 100, 50),
            make_entry("cfg1", "/b.txt", 200, 60),
        ]).unwrap();
        mgr.flush().unwrap();

        mgr.delete_by_path("cfg1", "/a.txt").unwrap();

        // /a.txt deleted → changed
        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ]).unwrap();
        assert_eq!(changed.len(), 1);

        // /b.txt still cached
        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/b.txt".into(), mtime: 200, size: 60 },
        ]).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_delete_by_path_flushes_pending() {
        let (_dir, mgr) = setup();
        // batch_upsert adds to pending buffer (threshold=500, won't auto-flush)
        mgr.batch_upsert(vec![
            make_entry("cfg1", "/pending.txt", 100, 50),
        ]).unwrap();
        // Don't call flush() — delete_by_path should flush internally
        let deleted = mgr.delete_by_path("cfg1", "/pending.txt").unwrap();
        assert!(deleted, "should delete entry that was still in pending buffer");

        // Verify it's gone
        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/pending.txt".into(), mtime: 100, size: 50 },
        ]).unwrap();
        assert_eq!(changed.len(), 1, "deleted entry should appear as changed");
    }

    #[test]
    fn test_find_changed_files_empty_input() {
        let (_dir, mgr) = setup();
        let changed = mgr.find_changed_files("cfg1", &[]).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn test_batch_upsert_empty_vec() {
        let (_dir, mgr) = setup();
        // 空输入不应报错
        mgr.batch_upsert(vec![]).unwrap();
    }

    #[test]
    fn test_upsert_single_overwrites() {
        let (_dir, mgr) = setup();
        mgr.upsert_single(make_entry("cfg1", "/a.txt", 100, 50)).unwrap();

        // 用新 mtime 覆盖
        mgr.upsert_single(make_entry("cfg1", "/a.txt", 200, 50)).unwrap();

        // 用旧 mtime 查询应报告变化
        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ]).unwrap();
        assert_eq!(changed.len(), 1, "old mtime should differ from updated cache");

        // 用新 mtime 查询应无变化
        let changed = mgr.find_changed_files("cfg1", &[
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 200, size: 50 },
        ]).unwrap();
        assert!(changed.is_empty(), "new mtime should match updated cache");
    }

    #[test]
    fn test_delete_by_config_returns_count() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![
            make_entry("cfg1", "/a.txt", 100, 50),
            make_entry("cfg1", "/b.txt", 200, 60),
            make_entry("cfg2", "/c.txt", 300, 70),
        ]).unwrap();
        mgr.flush().unwrap();

        let deleted = mgr.delete_by_config("cfg1").unwrap();
        assert_eq!(deleted, 2);

        let deleted = mgr.delete_by_config("cfg1").unwrap();
        assert_eq!(deleted, 0, "second delete should find nothing");
    }

    #[test]
    fn test_find_changed_files_large_batch_chunking() {
        let (_dir, mgr) = setup();
        // 插入 600 条（超过 500 的分批阈值）
        let entries: Vec<CachedFileEntry> = (0..600)
            .map(|i| make_entry("cfg1", &format!("/file_{}.txt", i), i as i64, i as i64))
            .collect();
        mgr.batch_upsert(entries).unwrap();
        mgr.flush().unwrap();

        // 查询全部 600 条，应全部命中缓存
        let files: Vec<ScannedFileMeta> = (0..600)
            .map(|i| ScannedFileMeta {
                file_path: format!("/file_{}.txt", i),
                mtime: i as i64,
                size: i as i64,
            })
            .collect();
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert!(changed.is_empty(), "all 600 files should be cached");

        // 修改其中 3 条的 mtime
        let mut mixed = files.clone();
        mixed[0].mtime = 9999;
        mixed[300].mtime = 9999;
        mixed[599].mtime = 9999;
        let changed = mgr.find_changed_files("cfg1", &mixed).unwrap();
        assert_eq!(changed.len(), 3, "exactly 3 files should be changed");
    }

    #[test]
    fn test_find_changed_files_config_isolation() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![make_entry("cfg1", "/a.txt", 100, 50)]).unwrap();
        mgr.flush().unwrap();

        // 用 cfg2 查询同路径 → 缓存隔离，应报告变化
        let changed = mgr.find_changed_files("cfg2", &[
            ScannedFileMeta { file_path: "/a.txt".into(), mtime: 100, size: 50 },
        ]).unwrap();
        assert_eq!(changed.len(), 1, "different config_id should not hit cache");
    }

    #[test]
    fn test_delete_by_path_nonexistent() {
        let (_dir, mgr) = setup();
        let deleted = mgr.delete_by_path("cfg1", "/no_such_file.txt").unwrap();
        assert!(!deleted, "deleting non-existent path should return false");
    }

    #[test]
    fn test_find_changed_mixed_new_cached_changed() {
        let (_dir, mgr) = setup();
        mgr.batch_upsert(vec![
            make_entry("cfg1", "/cached.txt", 100, 50),
            make_entry("cfg1", "/changed.txt", 100, 50),
        ]).unwrap();
        mgr.flush().unwrap();

        let files = vec![
            ScannedFileMeta { file_path: "/cached.txt".into(), mtime: 100, size: 50 },  // 未变
            ScannedFileMeta { file_path: "/changed.txt".into(), mtime: 200, size: 50 }, // mtime 变了
            ScannedFileMeta { file_path: "/new.txt".into(), mtime: 300, size: 70 },     // 新文件
        ];
        let changed = mgr.find_changed_files("cfg1", &files).unwrap();
        assert_eq!(changed.len(), 2, "changed + new = 2, cached should be skipped");

        let paths: Vec<&str> = changed.iter().map(|f| f.file_path.as_str()).collect();
        assert!(paths.contains(&"/changed.txt"));
        assert!(paths.contains(&"/new.txt"));
    }
}
