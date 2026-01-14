//! 备份记录管理器（去重服务）
//! 
//! 使用 SQLite 存储备份记录，支持：
//! - 上传记录（用于上传去重）
//! - 下载记录（用于下载去重）
//! - 快照记录（加密文件映射）

use anyhow::{anyhow, Result};
use chrono::Utc;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

/// 数据库连接池类型
type DbPool = Pool<SqliteConnectionManager>;
type DbConnection = PooledConnection<SqliteConnectionManager>;

/// 备份记录管理器
pub struct BackupRecordManager {
    pool: Arc<DbPool>,
}

impl std::fmt::Debug for BackupRecordManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupRecordManager")
            .field("pool", &"<DbPool>")
            .finish()
    }
}

impl BackupRecordManager {
    /// 创建新的记录管理器
    pub fn new(db_path: &Path) -> Result<Self> {
        // 确保父目录存在
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let manager = SqliteConnectionManager::file(db_path);
        let pool = Pool::builder()
            .max_size(10)
            .build(manager)?;

        let record_manager = Self {
            pool: Arc::new(pool),
        };

        // 初始化数据库
        record_manager.init_database()?;

        Ok(record_manager)
    }

    /// 获取数据库连接
    fn get_conn(&self) -> Result<DbConnection> {
        self.pool.get().map_err(|e| anyhow!("Failed to get db connection: {}", e))
    }

    /// 初始化数据库表
    fn init_database(&self) -> Result<()> {
        let conn = self.get_conn()?;

        // 启用 WAL 模式提升并发性能
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;

        // 上传记录表
        conn.execute(
            "-- ============================================
            -- 表: upload_records (上传去重记录表)
            -- 描述: 记录已上传文件信息，用于增量备份时的去重判断
            -- ============================================
            CREATE TABLE IF NOT EXISTS upload_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,   -- 自增主键
                config_id TEXT NOT NULL,                -- 备份配置ID
                relative_path TEXT NOT NULL,            -- 相对路径 (相对于备份源目录)
                file_name TEXT NOT NULL,                -- 文件名
                file_size INTEGER NOT NULL,             -- 文件大小 (字节)
                head_md5 TEXT NOT NULL,                 -- 文件头MD5 (前128KB，用于快速去重)
                full_md5 TEXT,                          -- 完整文件MD5 (可选，大文件延迟计算)
                remote_path TEXT NOT NULL,              -- 远程存储路径 (百度网盘路径)
                encrypted INTEGER NOT NULL DEFAULT 0,   -- 是否加密: 0=否, 1=是
                encrypted_name TEXT,                    -- 加密后的文件名
                created_at TEXT NOT NULL,               -- 创建时间 (RFC3339格式)
                updated_at TEXT NOT NULL,               -- 更新时间 (RFC3339格式)
                UNIQUE(config_id, relative_path, file_name)  -- 唯一约束: 同一配置下路径+文件名唯一
            )",
            [],
        )?;

        // 创建上传记录索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_records_lookup 
             ON upload_records(config_id, relative_path, file_name)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_records_dedup 
             ON upload_records(config_id, file_size, head_md5)",
            [],
        )?;

        // 下载记录表
        conn.execute(
            "-- ============================================
            -- 表: download_records (下载去重记录表)
            -- 描述: 记录已下载文件信息，用于增量同步时的去重判断
            -- ============================================
            CREATE TABLE IF NOT EXISTS download_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,   -- 自增主键
                config_id TEXT NOT NULL,                -- 备份配置ID
                remote_path TEXT NOT NULL,              -- 远程文件路径 (百度网盘路径)
                file_name TEXT NOT NULL,                -- 文件名
                file_size INTEGER NOT NULL,             -- 文件大小 (字节)
                fs_id TEXT NOT NULL,                    -- 百度网盘文件ID (用于精确匹配)
                local_path TEXT NOT NULL,               -- 本地存储路径
                encrypted INTEGER NOT NULL DEFAULT 0,   -- 是否加密: 0=否, 1=是
                created_at TEXT NOT NULL,               -- 创建时间 (RFC3339格式)
                updated_at TEXT NOT NULL,               -- 更新时间 (RFC3339格式)
                UNIQUE(config_id, remote_path, file_name)  -- 唯一约束: 同一配置下远程路径+文件名唯一
            )",
            [],
        )?;

        // 创建下载记录索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_download_records_lookup 
             ON download_records(config_id, remote_path, file_name)",
            [],
        )?;

        // 加密快照表
        conn.execute(
            "-- ============================================
            -- 表: encryption_snapshots (加密文件映射快照表)
            -- 描述: 存储加密文件/文件夹与原始名称的映射关系，用于解密恢复
            -- ============================================
            CREATE TABLE IF NOT EXISTS encryption_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,   -- 自增主键
                config_id TEXT NOT NULL,                -- 备份配置ID
                original_path TEXT NOT NULL,            -- 原始文件相对路径（文件夹时为父路径）
                original_name TEXT NOT NULL,            -- 原始文件名/文件夹名
                encrypted_name TEXT NOT NULL,           -- 加密后的文件名/文件夹名 (随机生成)
                file_size INTEGER NOT NULL DEFAULT 0,   -- 原始文件大小 (字节，文件夹为0)
                nonce TEXT NOT NULL DEFAULT '',         -- 加密随机数 (Base64编码，文件夹为空)
                algorithm TEXT NOT NULL DEFAULT '',     -- 加密算法 (文件夹为空)
                version INTEGER NOT NULL DEFAULT 1,     -- 加密格式版本号
                key_version INTEGER NOT NULL DEFAULT 1, -- 密钥版本号（关联加密时使用的密钥）
                remote_path TEXT NOT NULL,              -- 远程存储路径 (百度网盘路径)
                is_directory INTEGER NOT NULL DEFAULT 0,-- 是否为文件夹: 0=文件, 1=文件夹
                status TEXT NOT NULL DEFAULT 'pending', -- 状态: pending/uploading/completed/failed
                created_at TEXT NOT NULL,               -- 创建时间 (RFC3339格式)
                updated_at TEXT NOT NULL,               -- 更新时间 (RFC3339格式)
                UNIQUE(config_id, original_path, original_name)  -- 唯一约束: 同一配置下原始路径+文件名唯一
            )",
            [],
        )?;

        // 创建快照索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_lookup 
             ON encryption_snapshots(config_id, original_path, original_name)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_encrypted 
             ON encryption_snapshots(encrypted_name)",
            [],
        )?;

        // 创建 key_version 索引（用于按密钥版本查询）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_key_version
             ON encryption_snapshots(key_version)",
            [],
        )?;

        // 创建 is_directory 索引（用于按类型查询文件夹映射）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_directory
             ON encryption_snapshots(is_directory, encrypted_name)",
            [],
        )?;

        Ok(())
    }

    // ==================== 上传记录操作 ====================

    /// 检查上传记录（初步去重）
    /// 返回 (是否存在, 可能的 MD5)
    pub fn check_upload_record_preliminary(
        &self,
        config_id: &str,
        relative_path: &str,
        file_name: &str,
        file_size: u64,
        head_md5: &str,
    ) -> Result<(bool, Option<String>)> {
        let conn = self.get_conn()?;

        let result: Option<(i32, Option<String>)> = conn
            .query_row(
                "SELECT 1, full_md5 FROM upload_records 
                 WHERE config_id = ?1 AND relative_path = ?2 AND file_name = ?3 
                 AND file_size = ?4 AND head_md5 = ?5",
                params![config_id, relative_path, file_name, file_size as i64, head_md5],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        match result {
            Some((_, md5)) => Ok((true, md5)),
            None => Ok((false, None)),
        }
    }

    /// 添加上传记录
    pub fn add_upload_record(&self, record: &UploadRecord) -> Result<i64> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO upload_records 
             (config_id, relative_path, file_name, file_size, head_md5, full_md5, 
              remote_path, encrypted, encrypted_name, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
            params![
                record.config_id,
                record.relative_path,
                record.file_name,
                record.file_size as i64,
                record.head_md5,
                record.full_md5,
                record.remote_path,
                record.encrypted as i32,
                record.encrypted_name,
                now,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// 批量添加上传记录
    pub fn add_upload_records_batch(&self, records: &[UploadRecord]) -> Result<usize> {
        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;
        let now = Utc::now().to_rfc3339();

        let mut count = 0;
        for record in records {
            tx.execute(
                "INSERT OR REPLACE INTO upload_records 
                 (config_id, relative_path, file_name, file_size, head_md5, full_md5, 
                  remote_path, encrypted, encrypted_name, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
                params![
                    record.config_id,
                    record.relative_path,
                    record.file_name,
                    record.file_size as i64,
                    record.head_md5,
                    record.full_md5,
                    record.remote_path,
                    record.encrypted as i32,
                    record.encrypted_name,
                    now,
                ],
            )?;
            count += 1;
        }

        tx.commit()?;
        Ok(count)
    }

    /// 删除上传记录
    pub fn delete_upload_record(&self, config_id: &str, relative_path: &str, file_name: &str) -> Result<bool> {
        let conn = self.get_conn()?;
        let rows = conn.execute(
            "DELETE FROM upload_records 
             WHERE config_id = ?1 AND relative_path = ?2 AND file_name = ?3",
            params![config_id, relative_path, file_name],
        )?;
        Ok(rows > 0)
    }

    /// 删除配置的所有上传记录
    pub fn delete_upload_records_by_config(&self, config_id: &str) -> Result<usize> {
        let conn = self.get_conn()?;
        let rows = conn.execute(
            "DELETE FROM upload_records WHERE config_id = ?1",
            params![config_id],
        )?;
        Ok(rows)
    }

    // ==================== 下载记录操作 ====================

    /// 检查下载记录
    pub fn check_download_record(
        &self,
        config_id: &str,
        remote_path: &str,
        file_name: &str,
        file_size: u64,
        fs_id: &str,
    ) -> Result<bool> {
        let conn = self.get_conn()?;

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM download_records 
                 WHERE config_id = ?1 AND remote_path = ?2 AND file_name = ?3 
                 AND file_size = ?4 AND fs_id = ?5",
                params![config_id, remote_path, file_name, file_size as i64, fs_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        Ok(exists)
    }

    /// 添加下载记录
    pub fn add_download_record(&self, record: &DownloadRecord) -> Result<i64> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO download_records 
             (config_id, remote_path, file_name, file_size, fs_id, local_path, 
              encrypted, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                record.config_id,
                record.remote_path,
                record.file_name,
                record.file_size as i64,
                record.fs_id,
                record.local_path,
                record.encrypted as i32,
                now,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// 批量添加下载记录
    pub fn add_download_records_batch(&self, records: &[DownloadRecord]) -> Result<usize> {
        let mut conn = self.get_conn()?;
        let tx = conn.transaction()?;
        let now = Utc::now().to_rfc3339();

        let mut count = 0;
        for record in records {
            tx.execute(
                "INSERT OR REPLACE INTO download_records 
                 (config_id, remote_path, file_name, file_size, fs_id, local_path, 
                  encrypted, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
                params![
                    record.config_id,
                    record.remote_path,
                    record.file_name,
                    record.file_size as i64,
                    record.fs_id,
                    record.local_path,
                    record.encrypted as i32,
                    now,
                ],
            )?;
            count += 1;
        }

        tx.commit()?;
        Ok(count)
    }

    // ==================== 快照记录操作 ====================

    /// 添加加密快照
    pub fn add_snapshot(&self, snapshot: &EncryptionSnapshot) -> Result<i64> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO encryption_snapshots 
             (config_id, original_path, original_name, encrypted_name, file_size, 
              nonce, algorithm, version, key_version, remote_path, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)",
            params![
                snapshot.config_id,
                snapshot.original_path,
                snapshot.original_name,
                snapshot.encrypted_name,
                snapshot.file_size as i64,
                snapshot.nonce,
                snapshot.algorithm,
                snapshot.version,
                snapshot.key_version as i64,
                snapshot.remote_path,
                snapshot.status,
                now,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// 根据加密文件名查找快照
    pub fn find_snapshot_by_encrypted_name(&self, encrypted_name: &str) -> Result<Option<EncryptionSnapshot>> {
        let conn = self.get_conn()?;

        let result = conn
            .query_row(
                "SELECT config_id, original_path, original_name, encrypted_name, 
                        file_size, nonce, algorithm, version, key_version, remote_path, is_directory, status
                 FROM encryption_snapshots
                 WHERE encrypted_name = ?1",
                params![encrypted_name],
                |row| {
                    Ok(EncryptionSnapshot {
                        config_id: row.get(0)?,
                        original_path: row.get(1)?,
                        original_name: row.get(2)?,
                        encrypted_name: row.get(3)?,
                        file_size: row.get::<_, i64>(4)? as u64,
                        nonce: row.get(5)?,
                        algorithm: row.get(6)?,
                        version: row.get(7)?,
                        key_version: row.get::<_, i64>(8)? as u32,
                        remote_path: row.get(9)?,
                        is_directory: row.get::<_, i32>(10)? == 1,
                        status: row.get(11)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    /// 根据原始路径查找快照
    pub fn find_snapshot_by_original(
        &self,
        original_path: &str,
        original_name: &str,
    ) -> Result<Option<EncryptionSnapshot>> {
        let conn = self.get_conn()?;

        let result = conn
            .query_row(
                "SELECT config_id, original_path, original_name, encrypted_name,
                        file_size, nonce, algorithm, version, key_version, remote_path, is_directory, status
                 FROM encryption_snapshots
                 WHERE original_path = ?1 AND original_name = ?2",
                params![original_path, original_name],
                |row| {
                    Ok(EncryptionSnapshot {
                        config_id: row.get(0)?,
                        original_path: row.get(1)?,
                        original_name: row.get(2)?,
                        encrypted_name: row.get(3)?,
                        file_size: row.get::<_, i64>(4)? as u64,
                        nonce: row.get(5)?,
                        algorithm: row.get(6)?,
                        version: row.get(7)?,
                        key_version: row.get::<_, i64>(8)? as u32,
                        remote_path: row.get(9)?,
                        is_directory: row.get::<_, i32>(10)? == 1,
                        status: row.get(11)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    /// 更新快照状态
    pub fn update_snapshot_status(&self, encrypted_name: &str, status: &str) -> Result<bool> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        let rows = conn.execute(
            "UPDATE encryption_snapshots SET status = ?1, updated_at = ?2 WHERE encrypted_name = ?3",
            params![status, now, encrypted_name],
        )?;

        Ok(rows > 0)
    }

    /// 更新快照的加密元数据（nonce、algorithm）并标记为已完成
    /// 用于上传完成时更新之前创建的 pending 状态的快照
    pub fn update_snapshot_encryption_metadata(
        &self,
        encrypted_name: &str,
        nonce: &str,
        algorithm: &str,
        version: i32,
    ) -> Result<bool> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        let rows = conn.execute(
            "UPDATE encryption_snapshots 
             SET nonce = ?1, algorithm = ?2, version = ?3, status = 'completed', updated_at = ?4 
             WHERE encrypted_name = ?5",
            params![nonce, algorithm, version, now, encrypted_name],
        )?;

        Ok(rows > 0)
    }

    /// 批量根据加密文件名查找快照
    /// 用于文件列表显示时批量查询原始文件名
    pub fn find_snapshots_by_encrypted_names(&self, encrypted_names: &[String]) -> Result<Vec<EncryptionSnapshot>> {
        if encrypted_names.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.get_conn()?;

        // 构建 IN 子句的占位符
        let placeholders: Vec<String> = encrypted_names.iter()
            .map(|_| "?".to_string())
            .collect();
        let placeholders_str = placeholders.join(", ");

        let sql = format!(
            "SELECT config_id, original_path, original_name, encrypted_name,
                    file_size, nonce, algorithm, version, key_version, remote_path, is_directory, status
             FROM encryption_snapshots
             WHERE encrypted_name IN ({})",
            placeholders_str
        );

        let mut stmt = conn.prepare(&sql)?;

        // 将参数转换为 rusqlite 可接受的格式
        let params: Vec<&dyn rusqlite::ToSql> = encrypted_names
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok(EncryptionSnapshot {
                config_id: row.get(0)?,
                original_path: row.get(1)?,
                original_name: row.get(2)?,
                encrypted_name: row.get(3)?,
                file_size: row.get::<_, i64>(4)? as u64,
                nonce: row.get(5)?,
                algorithm: row.get(6)?,
                version: row.get(7)?,
                key_version: row.get::<_, i64>(8)? as u32,
                remote_path: row.get(9)?,
                is_directory: row.get::<_, i32>(10)? == 1,
                status: row.get(11)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    // ==================== 清理操作 ====================

    /// 清理过期记录
    pub fn cleanup_old_records(&self, days: u32) -> Result<(usize, usize, usize)> {
        let conn = self.get_conn()?;
        let cutoff = Utc::now() - chrono::Duration::days(days as i64);
        let cutoff_str = cutoff.to_rfc3339();

        let upload_deleted = conn.execute(
            "DELETE FROM upload_records WHERE updated_at < ?1",
            params![cutoff_str],
        )?;

        let download_deleted = conn.execute(
            "DELETE FROM download_records WHERE updated_at < ?1",
            params![cutoff_str],
        )?;

        let snapshot_deleted = conn.execute(
            "DELETE FROM encryption_snapshots WHERE updated_at < ?1 AND status = 'completed'",
            params![cutoff_str],
        )?;

        Ok((upload_deleted, download_deleted, snapshot_deleted))
    }

    /// 删除指定配置的所有加密快照记录
    ///
    /// 当删除备份配置时调用，清理该配置关联的所有加密映射数据
    pub fn delete_snapshots_by_config(&self, config_id: &str) -> Result<usize> {
        let conn = self.get_conn()?;

        let deleted = conn.execute(
            "DELETE FROM encryption_snapshots WHERE config_id = ?1",
            params![config_id],
        )?;

        tracing::info!("已删除配置 {} 的 {} 条加密快照记录", config_id, deleted);
        Ok(deleted)
    }

    /// 删除指定配置下未完成的加密快照记录
    ///
    /// 当取消备份任务时调用，清理该配置下未完成（非 completed 状态）的加密映射记录
    /// 已完成的记录会保留，用于下次去重和解密
    pub fn delete_incomplete_snapshots_by_config(&self, config_id: &str) -> Result<usize> {
        let conn = self.get_conn()?;

        let deleted = conn.execute(
            "DELETE FROM encryption_snapshots WHERE config_id = ?1 AND status != 'completed'",
            params![config_id],
        )?;

        if deleted > 0 {
            tracing::info!("已删除配置 {} 的 {} 条未完成加密快照记录", config_id, deleted);
        }
        Ok(deleted)
    }

    /// 批量删除指定加密文件名的快照记录
    ///
    /// 当取消备份任务时调用，清理该任务中已创建但未完成的加密映射记录
    pub fn delete_snapshots_by_encrypted_names(&self, encrypted_names: &[String]) -> Result<usize> {
        if encrypted_names.is_empty() {
            return Ok(0);
        }

        let conn = self.get_conn()?;

        // 构建 IN 子句的占位符
        let placeholders: Vec<String> = encrypted_names.iter()
            .map(|_| "?".to_string())
            .collect();
        let placeholders_str = placeholders.join(", ");

        let sql = format!(
            "DELETE FROM encryption_snapshots WHERE encrypted_name IN ({})",
            placeholders_str
        );

        let params: Vec<&dyn rusqlite::ToSql> = encrypted_names
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let deleted = conn.execute(&sql, params.as_slice())?;

        if deleted > 0 {
            tracing::info!("已删除 {} 条加密快照记录", deleted);
        }
        Ok(deleted)
    }

    /// 获取数据库统计信息
    pub fn get_stats(&self) -> Result<RecordStats> {
        let conn = self.get_conn()?;

        let upload_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM upload_records",
            [],
            |row| row.get(0),
        )?;

        let download_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM download_records",
            [],
            |row| row.get(0),
        )?;

        let snapshot_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM encryption_snapshots",
            [],
            |row| row.get(0),
        )?;

        Ok(RecordStats {
            upload_count: upload_count as usize,
            download_count: download_count as usize,
            snapshot_count: snapshot_count as usize,
        })
    }

    // ==================== 文件夹映射操作（基于 encryption_snapshots 表）====================

    /// 添加文件夹映射（存储到 encryption_snapshots 表，is_directory=1）
    ///
    /// # 参数
    /// - `parent_path`: 父路径
    /// - `original_name`: 原始文件夹名
    /// - `encrypted_name`: 加密后的文件夹名
    /// - `key_version`: 当前加密密钥版本号
    pub fn add_folder_mapping(
        &self,
        parent_path: &str,
        original_name: &str,
        encrypted_name: &str,
        key_version: u32,
    ) -> Result<i64> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();
        let remote_path = format!("{}/{}", parent_path.trim_end_matches('/'), encrypted_name);

        conn.execute(
            "INSERT OR REPLACE INTO encryption_snapshots
             (config_id, original_path, original_name, encrypted_name, file_size, nonce, algorithm,
              version, key_version, remote_path, is_directory, status, created_at, updated_at)
             VALUES ('', ?1, ?2, ?3, 0, '', '', 1, ?4, ?5, 1, 'completed', ?6, ?6)",
            params![parent_path, original_name, encrypted_name, key_version as i64, remote_path, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// 根据原始文件夹名查找加密名
    pub fn find_encrypted_folder_name(
        &self,
        parent_path: &str,
        original_folder_name: &str,
    ) -> Result<Option<String>> {
        let conn = self.get_conn()?;

        let result: Option<String> = conn
            .query_row(
                "SELECT encrypted_name FROM encryption_snapshots
                 WHERE original_path = ?1 AND original_name = ?2 AND is_directory = 1",
                params![parent_path, original_folder_name],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result)
    }

    /// 根据加密文件夹名查找原始名
    pub fn find_original_folder_name(
        &self,
        encrypted_folder_name: &str,
    ) -> Result<Option<String>> {
        let conn = self.get_conn()?;

        let result: Option<String> = conn
            .query_row(
                "SELECT original_name FROM encryption_snapshots
                 WHERE encrypted_name = ?1 AND is_directory = 1",
                params![encrypted_folder_name],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result)
    }

    /// 根据加密文件夹名查找所有映射（跨配置，用于文件列表显示）
    pub fn get_all_folder_mappings_by_encrypted_name(&self, encrypted_name: &str) -> Result<Vec<EncryptionSnapshot>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT config_id, original_path, original_name, encrypted_name, file_size, nonce,
                    algorithm, version, key_version, remote_path, is_directory, status
             FROM encryption_snapshots WHERE encrypted_name = ?1 AND is_directory = 1"
        )?;

        let rows = stmt.query_map(params![encrypted_name], |row| {
            Ok(EncryptionSnapshot {
                config_id: row.get(0)?,
                original_path: row.get(1)?,
                original_name: row.get(2)?,
                encrypted_name: row.get(3)?,
                file_size: row.get(4)?,
                nonce: row.get(5)?,
                algorithm: row.get(6)?,
                version: row.get(7)?,
                key_version: row.get(8)?,
                remote_path: row.get(9)?,
                is_directory: row.get::<_, i32>(10)? == 1,
                status: row.get(11)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }
}

// ==================== 数据结构 ====================

/// 上传记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRecord {
    pub config_id: String,
    pub relative_path: String,
    pub file_name: String,
    pub file_size: u64,
    pub head_md5: String,
    pub full_md5: Option<String>,
    pub remote_path: String,
    pub encrypted: bool,
    pub encrypted_name: Option<String>,
}

/// 下载记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub config_id: String,
    pub remote_path: String,
    pub file_name: String,
    pub file_size: u64,
    pub fs_id: String,
    pub local_path: String,
    pub encrypted: bool,
}

/// 加密快照（统一存储文件和文件夹的加密映射）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionSnapshot {
    pub config_id: String,
    pub original_path: String,      // 原始路径（文件夹时为父路径）
    pub original_name: String,      // 原始文件名/文件夹名
    pub encrypted_name: String,     // 加密后的名称
    pub file_size: u64,             // 文件大小（文件夹为0）
    pub nonce: String,              // 加密随机数（文件夹为空）
    pub algorithm: String,          // 加密算法（文件夹为空）
    pub version: i32,
    pub key_version: u32,
    pub remote_path: String,        // 远程完整路径
    pub is_directory: bool,         // 是否为文件夹
    pub status: String,
}

/// 记录统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordStats {
    pub upload_count: usize,
    pub download_count: usize,
    pub snapshot_count: usize,
}

// ==================== 工具函数 ====================

/// 计算文件头 MD5（前 128KB）
pub fn calculate_head_md5(path: &Path) -> Result<String> {
    const HEAD_SIZE: usize = 128 * 1024; // 128KB

    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(HEAD_SIZE, file);
    let mut buffer = vec![0u8; HEAD_SIZE];

    let bytes_read = reader.read(&mut buffer)?;
    buffer.truncate(bytes_read);

    let digest = md5::compute(&buffer);
    Ok(format!("{:x}", digest))
}

/// 计算完整文件 MD5
pub fn calculate_full_md5(path: &Path) -> Result<String> {
    const BUFFER_SIZE: usize = 1024 * 1024; // 1MB

    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut context = md5::Context::new();
    let mut buffer = vec![0u8; BUFFER_SIZE];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        context.consume(&buffer[..bytes_read]);
    }

    let digest = context.compute();
    Ok(format!("{:x}", digest))
}

/// 去重检查结果
#[derive(Debug, Clone)]
pub struct DedupCheckResult {
    /// 是否应该跳过
    pub should_skip: bool,
    /// 跳过原因
    pub skip_reason: Option<String>,
    /// 现有记录的 MD5（如果有）
    pub existing_md5: Option<String>,
}

impl DedupCheckResult {
    pub fn skip(reason: &str) -> Self {
        Self {
            should_skip: true,
            skip_reason: Some(reason.to_string()),
            existing_md5: None,
        }
    }

    pub fn proceed() -> Self {
        Self {
            should_skip: false,
            skip_reason: None,
            existing_md5: None,
        }
    }
}

// 为 rusqlite 添加 optional 扩展
trait OptionalExtension<T> {
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExtension<T> for std::result::Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(anyhow!("Database error: {}", e)),
        }
    }
}
