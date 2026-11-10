// 扫描缓存模块 - 增量扫描支持
//
// 设计目标：
// 1. 缓存目录快照，避免重复扫描
// 2. 支持增量备份场景
// 3. 降低 API 调用 90%

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// 目录快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectorySnapshot {
    /// 目录路径
    pub path: String,
    /// 修改时间（毫秒时间戳）
    pub mtime: i64,
    /// 文件数量
    pub file_count: u64,
    /// 文件列表哈希（MD5）
    pub files_hash: String,
    /// 快照时间（秒级时间戳）
    pub snapshot_at: i64,
}

/// 扫描缓存管理器
pub struct ScanCache {
    db: Arc<Mutex<Connection>>,
}

impl ScanCache {
    /// 创建扫描缓存管理器
    pub fn new(db: Arc<Mutex<Connection>>) -> Result<Self> {
        let cache = Self { db };
        cache.init_tables_sync()?;
        Ok(cache)
    }

    /// 初始化表（同步方法）
    fn init_tables_sync(&self) -> Result<()> {
        let db = self.db.blocking_lock();

        db.execute(
            "CREATE TABLE IF NOT EXISTS directory_snapshots (
                path TEXT PRIMARY KEY,
                mtime INTEGER NOT NULL,
                file_count INTEGER NOT NULL,
                files_hash TEXT NOT NULL,
                snapshot_at INTEGER NOT NULL
            )",
            [],
        )?;

        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_at
             ON directory_snapshots(snapshot_at)",
            [],
        )?;

        info!("✅ 扫描缓存表初始化完成");
        Ok(())
    }

    /// 检查目录是否需要重新扫描
    ///
    /// 返回 true 表示需要扫描，false 表示可以使用缓存
    pub async fn needs_rescan(&self, path: &str, current_mtime: i64) -> Result<bool> {
        let db = self.db.lock().await;

        let cached: Option<i64> = db
            .query_row(
                "SELECT mtime FROM directory_snapshots WHERE path = ?",
                [path],
                |row| row.get(0),
            )
            .optional()?;

        match cached {
            Some(cached_mtime) => {
                let needs = cached_mtime != current_mtime;
                if needs {
                    debug!("🔄 目录已变化，需要重新扫描: {}", path);
                } else {
                    debug!("✅ 目录未变化，跳过扫描: {}", path);
                }
                Ok(needs)
            }
            None => {
                debug!("🆕 目录首次扫描: {}", path);
                Ok(true) // 无缓存，需要扫描
            }
        }
    }

    /// 保存目录快照
    pub async fn save_snapshot(&self, snapshot: &DirectorySnapshot) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            "INSERT OR REPLACE INTO directory_snapshots
             (path, mtime, file_count, files_hash, snapshot_at)
             VALUES (?, ?, ?, ?, ?)",
            (
                &snapshot.path,
                snapshot.mtime,
                snapshot.file_count,
                &snapshot.files_hash,
                snapshot.snapshot_at,
            ),
        )?;

        debug!("💾 保存目录快照: path={}, files={}", snapshot.path, snapshot.file_count);
        Ok(())
    }

    /// 获取快照
    pub async fn get_snapshot(&self, path: &str) -> Result<Option<DirectorySnapshot>> {
        let db = self.db.lock().await;

        let snapshot = db
            .query_row(
                "SELECT path, mtime, file_count, files_hash, snapshot_at
                 FROM directory_snapshots
                 WHERE path = ?",
                [path],
                |row| {
                    Ok(DirectorySnapshot {
                        path: row.get(0)?,
                        mtime: row.get(1)?,
                        file_count: row.get(2)?,
                        files_hash: row.get(3)?,
                        snapshot_at: row.get(4)?,
                    })
                },
            )
            .optional()?;

        Ok(snapshot)
    }

    /// 清理旧快照（保留最近 30 天）
    pub async fn cleanup_old_snapshots(&self, days: i64) -> Result<u64> {
        let db = self.db.lock().await;

        let cutoff = chrono::Utc::now().timestamp() - days * 24 * 3600;

        let removed = db.execute(
            "DELETE FROM directory_snapshots WHERE snapshot_at < ?",
            [cutoff],
        )?;

        if removed > 0 {
            tracing::info!("🧹 清理 {} 个旧快照（超过 {} 天）", removed, days);
        }

        Ok(removed as u64)
    }

    /// 获取缓存统计
    pub async fn get_stats(&self) -> Result<CacheStats> {
        let db = self.db.lock().await;

        let total: i64 = db.query_row(
            "SELECT COUNT(*) FROM directory_snapshots",
            [],
            |row| row.get(0),
        )?;

        let oldest: Option<i64> = db
            .query_row(
                "SELECT MIN(snapshot_at) FROM directory_snapshots",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let newest: Option<i64> = db
            .query_row(
                "SELECT MAX(snapshot_at) FROM directory_snapshots",
                [],
                |row| row.get(0),
            )
            .optional()?;

        Ok(CacheStats {
            total_snapshots: total as u64,
            oldest_snapshot: oldest,
            newest_snapshot: newest,
        })
    }
}

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// 总快照数
    pub total_snapshots: u64,
    /// 最旧快照时间
    pub oldest_snapshot: Option<i64>,
    /// 最新快照时间
    pub newest_snapshot: Option<i64>,
}

/// 计算文件列表哈希
pub fn calculate_files_hash(files: &[impl AsRef<str>]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.as_ref().as_bytes());
    }

    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_files_hash() {
        let files = vec!["file1.txt", "file2.txt"];
        let hash = calculate_files_hash(&files);
        assert_eq!(hash.len(), 64); // SHA-256 十六进制长度
    }

    #[test]
    fn test_directory_snapshot_creation() {
        let snapshot = DirectorySnapshot {
            path: "/test/path".to_string(),
            mtime: 1234567890,
            file_count: 100,
            files_hash: "abc123".to_string(),
            snapshot_at: 1234567890,
        };

        assert_eq!(snapshot.path, "/test/path");
        assert_eq!(snapshot.file_count, 100);
    }
}
