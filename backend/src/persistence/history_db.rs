//! 历史记录 SQLite 数据库模块
//!
//! 将历史归档从 JSONL 文件迁移到 SQLite 数据库
//! - task_history: 文件任务历史
//! - folder_history: 文件夹历史

use std::path::Path;
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use chrono::{Duration, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use tracing::{debug, info, warn};

use super::folder::FolderPersisted;
use super::types::{TaskMetadata, TaskPersistenceStatus, TaskType};
use crate::downloader::folder::FolderStatus;

/// 历史数据库管理器
pub struct HistoryDbManager {
    /// SQLite 连接
    conn: Mutex<Connection>,
}

impl HistoryDbManager {
    /// 创建新的历史数据库管理器
    pub fn new(db_path: &Path) -> Result<Self> {
        // 确保父目录存在
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(db_path)?;
        let manager = Self {
            conn: Mutex::new(conn),
        };

        // 初始化表结构
        manager.init_tables()?;

        Ok(manager)
    }

    /// 初始化数据库表
    fn init_tables(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 创建 task_history 表
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS task_history (
                task_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                -- 下载任务字段
                fs_id INTEGER,
                remote_path TEXT,
                local_path TEXT,
                -- 上传任务字段
                source_path TEXT,
                target_path TEXT,
                upload_id TEXT,
                -- 转存任务字段
                share_link TEXT,
                share_pwd TEXT,
                transfer_target_path TEXT,
                transfer_status TEXT,
                transfer_file_name TEXT,
                auto_download INTEGER,
                -- 共用字段
                file_size INTEGER,
                chunk_size INTEGER,
                total_chunks INTEGER,
                error_msg TEXT,
                -- 文件夹组信息
                group_id TEXT,
                group_root TEXT,
                relative_path TEXT,
                -- 备份字段
                is_backup INTEGER DEFAULT 0,
                backup_config_id TEXT,
                -- 关联字段
                transfer_task_id TEXT,
                download_task_ids TEXT,
                -- 兜底 JSON（用于存储不常用字段）
                metadata_json TEXT
            )
            "#,
            [],
        )?;

        // 创建 task_history 索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_task_history_type_completed ON task_history(task_type, completed_at)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_task_history_backup ON task_history(is_backup, backup_config_id, completed_at)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_task_history_status ON task_history(status, completed_at)",
            [],
        )?;

        // 创建 folder_history 表
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS folder_history (
                folder_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                remote_root TEXT NOT NULL,
                local_root TEXT NOT NULL,
                status TEXT NOT NULL,
                total_files INTEGER DEFAULT 0,
                total_size INTEGER DEFAULT 0,
                created_count INTEGER DEFAULT 0,
                completed_count INTEGER DEFAULT 0,
                downloaded_size INTEGER DEFAULT 0,
                scan_completed INTEGER DEFAULT 0,
                scan_progress TEXT,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                error TEXT,
                transfer_task_id TEXT,
                pending_files_json TEXT
            )
            "#,
            [],
        )?;

        // 创建 folder_history 索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_folder_history_status ON folder_history(status, completed_at)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_folder_history_completed ON folder_history(completed_at)",
            [],
        )?;

        // 创建 cloud_dl_auto_download 表（离线下载自动下载配置）
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS cloud_dl_auto_download (
                task_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                local_path TEXT,
                ask_each_time INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                triggered INTEGER NOT NULL DEFAULT 0,
                triggered_at INTEGER
            )
            "#,
            [],
        )?;

        info!("历史数据库表初始化完成");
        Ok(())
    }

    // ========================================================================
    // task_history 操作
    // ========================================================================

    /// 添加任务到历史
    pub fn add_task_to_history(&self, metadata: &TaskMetadata) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let task_type = metadata.task_type.as_str();
        let status = metadata
            .status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "completed".to_string());
        let download_task_ids = if metadata.download_task_ids.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&metadata.download_task_ids).unwrap_or_default())
        };

        conn.execute(
            r#"
            INSERT OR REPLACE INTO task_history (
                task_id, task_type, status, created_at, updated_at, completed_at,
                fs_id, remote_path, local_path,
                source_path, target_path, upload_id,
                share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                file_size, chunk_size, total_chunks, error_msg,
                group_id, group_root, relative_path,
                is_backup, backup_config_id,
                transfer_task_id, download_task_ids
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                ?7, ?8, ?9,
                ?10, ?11, ?12,
                ?13, ?14, ?15, ?16, ?17, ?18,
                ?19, ?20, ?21, ?22,
                ?23, ?24, ?25,
                ?26, ?27,
                ?28, ?29
            )
            "#,
            params![
                metadata.task_id,
                task_type,
                status,
                metadata.created_at.timestamp(),
                metadata.updated_at.timestamp(),
                metadata.completed_at.map(|t| t.timestamp()),
                metadata.fs_id.map(|id| id as i64),
                metadata.remote_path,
                metadata.local_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                metadata.source_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                metadata.target_path,
                metadata.upload_id,
                metadata.share_link,
                metadata.share_pwd,
                metadata.transfer_target_path,
                metadata.transfer_status,
                metadata.transfer_file_name,
                metadata.auto_download.map(|b| if b { 1 } else { 0 }),
                metadata.file_size.map(|s| s as i64),
                metadata.chunk_size.map(|s| s as i64),
                metadata.total_chunks.map(|c| c as i64),
                metadata.error_msg,
                metadata.group_id,
                metadata.group_root,
                metadata.relative_path,
                if metadata.is_backup { 1 } else { 0 },
                metadata.backup_config_id,
                metadata.transfer_task_id,
                download_task_ids,
            ],
        )?;

        debug!("已添加任务到历史数据库: {}", metadata.task_id);
        Ok(())
    }

    /// 批量添加任务到历史（使用事务）
    pub fn add_tasks_to_history_batch(&self, tasks: &[TaskMetadata]) -> Result<usize> {
        if tasks.is_empty() {
            return Ok(0);
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let tx = conn.transaction()?;
        let mut count = 0;

        {
            let mut stmt = tx.prepare(
                r#"
                INSERT OR REPLACE INTO task_history (
                    task_id, task_type, status, created_at, updated_at, completed_at,
                    fs_id, remote_path, local_path,
                    source_path, target_path, upload_id,
                    share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                    file_size, chunk_size, total_chunks, error_msg,
                    group_id, group_root, relative_path,
                    is_backup, backup_config_id,
                    transfer_task_id, download_task_ids
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9,
                    ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16, ?17, ?18,
                    ?19, ?20, ?21, ?22,
                    ?23, ?24, ?25,
                    ?26, ?27,
                    ?28, ?29
                )
                "#,
            )?;

            for metadata in tasks {
                let task_type = metadata.task_type.as_str();
                let status = metadata
                    .status
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "completed".to_string());
                let download_task_ids = if metadata.download_task_ids.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(&metadata.download_task_ids).unwrap_or_default())
                };

                stmt.execute(params![
                    metadata.task_id,
                    task_type,
                    status,
                    metadata.created_at.timestamp(),
                    metadata.updated_at.timestamp(),
                    metadata.completed_at.map(|t| t.timestamp()),
                    metadata.fs_id.map(|id| id as i64),
                    metadata.remote_path,
                    metadata.local_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                    metadata.source_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                    metadata.target_path,
                    metadata.upload_id,
                    metadata.share_link,
                    metadata.share_pwd,
                    metadata.transfer_target_path,
                    metadata.transfer_status,
                    metadata.transfer_file_name,
                    metadata.auto_download.map(|b| if b { 1 } else { 0 }),
                    metadata.file_size.map(|s| s as i64),
                    metadata.chunk_size.map(|s| s as i64),
                    metadata.total_chunks.map(|c| c as i64),
                    metadata.error_msg,
                    metadata.group_id,
                    metadata.group_root,
                    metadata.relative_path,
                    if metadata.is_backup { 1 } else { 0 },
                    metadata.backup_config_id,
                    metadata.transfer_task_id,
                    download_task_ids,
                ])?;
                count += 1;
            }
        }

        tx.commit()?;
        info!("批量添加 {} 个任务到历史数据库", count);
        Ok(count)
    }

    /// 加载所有任务历史到 DashMap
    pub fn load_all_task_history(&self) -> Result<Vec<TaskMetadata>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT
                task_id, task_type, status, created_at, updated_at, completed_at,
                fs_id, remote_path, local_path,
                source_path, target_path, upload_id,
                share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                file_size, chunk_size, total_chunks, error_msg,
                group_id, group_root, relative_path,
                is_backup, backup_config_id,
                transfer_task_id, download_task_ids
            FROM task_history
            ORDER BY completed_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(TaskHistoryRow {
                task_id: row.get(0)?,
                task_type: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
                completed_at: row.get(5)?,
                fs_id: row.get(6)?,
                remote_path: row.get(7)?,
                local_path: row.get(8)?,
                source_path: row.get(9)?,
                target_path: row.get(10)?,
                upload_id: row.get(11)?,
                share_link: row.get(12)?,
                share_pwd: row.get(13)?,
                transfer_target_path: row.get(14)?,
                transfer_status: row.get(15)?,
                transfer_file_name: row.get(16)?,
                auto_download: row.get(17)?,
                file_size: row.get(18)?,
                chunk_size: row.get(19)?,
                total_chunks: row.get(20)?,
                error_msg: row.get(21)?,
                group_id: row.get(22)?,
                group_root: row.get(23)?,
                relative_path: row.get(24)?,
                is_backup: row.get(25)?,
                backup_config_id: row.get(26)?,
                transfer_task_id: row.get(27)?,
                download_task_ids: row.get(28)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_task_metadata(r) {
                    Ok(metadata) => tasks.push(metadata),
                    Err(e) => warn!("转换任务历史失败: {}", e),
                },
                Err(e) => warn!("读取任务历史行失败: {}", e),
            }
        }

        info!("从数据库加载了 {} 条任务历史", tasks.len());
        Ok(tasks)
    }

    /// 检查任务是否存在于历史中
    pub fn task_exists_in_history(&self, task_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM task_history WHERE task_id = ?1",
                params![task_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        Ok(exists)
    }

    /// 获取单个任务历史
    pub fn get_task_history(&self, task_id: &str) -> Result<Option<TaskMetadata>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let row: Option<TaskHistoryRow> = conn
            .query_row(
                r#"
                SELECT
                    task_id, task_type, status, created_at, updated_at, completed_at,
                    fs_id, remote_path, local_path,
                    source_path, target_path, upload_id,
                    share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                    file_size, chunk_size, total_chunks, error_msg,
                    group_id, group_root, relative_path,
                    is_backup, backup_config_id,
                    transfer_task_id, download_task_ids
                FROM task_history
                WHERE task_id = ?1
                "#,
                params![task_id],
                |row| {
                    Ok(TaskHistoryRow {
                        task_id: row.get(0)?,
                        task_type: row.get(1)?,
                        status: row.get(2)?,
                        created_at: row.get(3)?,
                        updated_at: row.get(4)?,
                        completed_at: row.get(5)?,
                        fs_id: row.get(6)?,
                        remote_path: row.get(7)?,
                        local_path: row.get(8)?,
                        source_path: row.get(9)?,
                        target_path: row.get(10)?,
                        upload_id: row.get(11)?,
                        share_link: row.get(12)?,
                        share_pwd: row.get(13)?,
                        transfer_target_path: row.get(14)?,
                        transfer_status: row.get(15)?,
                        transfer_file_name: row.get(16)?,
                        auto_download: row.get(17)?,
                        file_size: row.get(18)?,
                        chunk_size: row.get(19)?,
                        total_chunks: row.get(20)?,
                        error_msg: row.get(21)?,
                        group_id: row.get(22)?,
                        group_root: row.get(23)?,
                        relative_path: row.get(24)?,
                        is_backup: row.get(25)?,
                        backup_config_id: row.get(26)?,
                        transfer_task_id: row.get(27)?,
                        download_task_ids: row.get(28)?,
                    })
                },
            )
            .optional()?;

        match row {
            Some(r) => Ok(Some(self.row_to_task_metadata(r)?)),
            None => Ok(None),
        }
    }

    /// 分页获取任务历史
    ///
    /// # Arguments
    /// * `offset` - 偏移量
    /// * `limit` - 每页数量
    ///
    /// # Returns
    /// * `(Vec<TaskMetadata>, usize)` - (任务列表, 总数)
    pub fn get_task_history_paginated(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<TaskMetadata>, usize)> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 获取总数
        let total: usize = conn.query_row(
            "SELECT COUNT(*) FROM task_history",
            [],
            |row| row.get(0),
        )?;

        // 获取分页数据
        let mut stmt = conn.prepare(
            r#"
            SELECT
                task_id, task_type, status, created_at, updated_at, completed_at,
                fs_id, remote_path, local_path,
                source_path, target_path, upload_id,
                share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                file_size, chunk_size, total_chunks, error_msg,
                group_id, group_root, relative_path,
                is_backup, backup_config_id,
                transfer_task_id, download_task_ids
            FROM task_history
            ORDER BY completed_at DESC
            LIMIT ?1 OFFSET ?2
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(TaskHistoryRow {
                task_id: row.get(0)?,
                task_type: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
                completed_at: row.get(5)?,
                fs_id: row.get(6)?,
                remote_path: row.get(7)?,
                local_path: row.get(8)?,
                source_path: row.get(9)?,
                target_path: row.get(10)?,
                upload_id: row.get(11)?,
                share_link: row.get(12)?,
                share_pwd: row.get(13)?,
                transfer_target_path: row.get(14)?,
                transfer_status: row.get(15)?,
                transfer_file_name: row.get(16)?,
                auto_download: row.get(17)?,
                file_size: row.get(18)?,
                chunk_size: row.get(19)?,
                total_chunks: row.get(20)?,
                error_msg: row.get(21)?,
                group_id: row.get(22)?,
                group_root: row.get(23)?,
                relative_path: row.get(24)?,
                is_backup: row.get(25)?,
                backup_config_id: row.get(26)?,
                transfer_task_id: row.get(27)?,
                download_task_ids: row.get(28)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_task_metadata(r) {
                    Ok(metadata) => tasks.push(metadata),
                    Err(e) => warn!("转换任务历史失败: {}", e),
                },
                Err(e) => warn!("读取任务历史行失败: {}", e),
            }
        }

        Ok((tasks, total))
    }

    /// 按任务类型和状态分页获取任务历史
    ///
    /// # Arguments
    /// * `task_type` - 任务类型 (download, upload, transfer)
    /// * `status` - 任务状态 (completed, failed, etc.)
    /// * `offset` - 偏移量
    /// * `limit` - 每页数量
    ///
    /// # Returns
    /// * `(Vec<TaskMetadata>, usize)` - (任务列表, 总数)
    pub fn get_task_history_by_type_and_status(
        &self,
        task_type: &str,
        status: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<TaskMetadata>, usize)> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 获取总数
        let total: usize = conn.query_row(
            "SELECT COUNT(*) FROM task_history WHERE task_type = ?1 AND status = ?2",
            params![task_type, status],
            |row| row.get(0),
        )?;

        // 获取分页数据
        let mut stmt = conn.prepare(
            r#"
            SELECT
                task_id, task_type, status, created_at, updated_at, completed_at,
                fs_id, remote_path, local_path,
                source_path, target_path, upload_id,
                share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                file_size, chunk_size, total_chunks, error_msg,
                group_id, group_root, relative_path,
                is_backup, backup_config_id,
                transfer_task_id, download_task_ids
            FROM task_history
            WHERE task_type = ?1 AND status = ?2
            ORDER BY completed_at DESC
            LIMIT ?3 OFFSET ?4
            "#,
        )?;

        let rows = stmt.query_map(params![task_type, status, limit as i64, offset as i64], |row| {
            Ok(TaskHistoryRow {
                task_id: row.get(0)?,
                task_type: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
                completed_at: row.get(5)?,
                fs_id: row.get(6)?,
                remote_path: row.get(7)?,
                local_path: row.get(8)?,
                source_path: row.get(9)?,
                target_path: row.get(10)?,
                upload_id: row.get(11)?,
                share_link: row.get(12)?,
                share_pwd: row.get(13)?,
                transfer_target_path: row.get(14)?,
                transfer_status: row.get(15)?,
                transfer_file_name: row.get(16)?,
                auto_download: row.get(17)?,
                file_size: row.get(18)?,
                chunk_size: row.get(19)?,
                total_chunks: row.get(20)?,
                error_msg: row.get(21)?,
                group_id: row.get(22)?,
                group_root: row.get(23)?,
                relative_path: row.get(24)?,
                is_backup: row.get(25)?,
                backup_config_id: row.get(26)?,
                transfer_task_id: row.get(27)?,
                download_task_ids: row.get(28)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_task_metadata(r) {
                    Ok(metadata) => tasks.push(metadata),
                    Err(e) => warn!("转换任务历史失败: {}", e),
                },
                Err(e) => warn!("读取任务历史行失败: {}", e),
            }
        }

        Ok((tasks, total))
    }

    /// 按任务类型和状态获取任务历史（排除备份任务）
    ///
    /// # Arguments
    /// * `task_type` - 任务类型 (download, upload, transfer)
    /// * `status` - 任务状态 (completed, failed, etc.)
    /// * `exclude_backup` - 是否排除备份任务
    /// * `offset` - 偏移量
    /// * `limit` - 每页数量
    ///
    /// # Returns
    /// * `(Vec<TaskMetadata>, usize)` - (任务列表, 总数)
    pub fn get_task_history_by_type_status_exclude_backup(
        &self,
        task_type: &str,
        status: &str,
        exclude_backup: bool,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<TaskMetadata>, usize)> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let backup_filter = if exclude_backup { "AND is_backup = 0" } else { "" };

        // 获取总数
        let count_sql = format!(
            "SELECT COUNT(*) FROM task_history WHERE task_type = ?1 AND status = ?2 {}",
            backup_filter
        );
        let total: usize = conn.query_row(&count_sql, params![task_type, status], |row| row.get(0))?;

        // 获取分页数据
        let query_sql = format!(
            r#"
            SELECT
                task_id, task_type, status, created_at, updated_at, completed_at,
                fs_id, remote_path, local_path,
                source_path, target_path, upload_id,
                share_link, share_pwd, transfer_target_path, transfer_status, transfer_file_name, auto_download,
                file_size, chunk_size, total_chunks, error_msg,
                group_id, group_root, relative_path,
                is_backup, backup_config_id,
                transfer_task_id, download_task_ids
            FROM task_history
            WHERE task_type = ?1 AND status = ?2 {}
            ORDER BY completed_at DESC
            LIMIT ?3 OFFSET ?4
            "#,
            backup_filter
        );

        let mut stmt = conn.prepare(&query_sql)?;
        let rows = stmt.query_map(params![task_type, status, limit as i64, offset as i64], |row| {
            Ok(TaskHistoryRow {
                task_id: row.get(0)?,
                task_type: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
                completed_at: row.get(5)?,
                fs_id: row.get(6)?,
                remote_path: row.get(7)?,
                local_path: row.get(8)?,
                source_path: row.get(9)?,
                target_path: row.get(10)?,
                upload_id: row.get(11)?,
                share_link: row.get(12)?,
                share_pwd: row.get(13)?,
                transfer_target_path: row.get(14)?,
                transfer_status: row.get(15)?,
                transfer_file_name: row.get(16)?,
                auto_download: row.get(17)?,
                file_size: row.get(18)?,
                chunk_size: row.get(19)?,
                total_chunks: row.get(20)?,
                error_msg: row.get(21)?,
                group_id: row.get(22)?,
                group_root: row.get(23)?,
                relative_path: row.get(24)?,
                is_backup: row.get(25)?,
                backup_config_id: row.get(26)?,
                transfer_task_id: row.get(27)?,
                download_task_ids: row.get(28)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_task_metadata(r) {
                    Ok(metadata) => tasks.push(metadata),
                    Err(e) => warn!("转换任务历史失败: {}", e),
                },
                Err(e) => warn!("读取任务历史行失败: {}", e),
            }
        }

        Ok((tasks, total))
    }

    /// 批量删除任务历史（按任务类型和状态）
    pub fn remove_tasks_by_type_and_status(&self, task_type: &str, status: &str) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM task_history WHERE task_type = ?1 AND status = ?2",
            params![task_type, status],
        )?;

        if deleted > 0 {
            info!(
                "已从历史数据库中删除 {} 个 {} 类型的 {} 状态任务",
                deleted, task_type, status
            );
        }
        Ok(deleted)
    }

    /// 从历史中删除任务
    pub fn remove_task_from_history(&self, task_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM task_history WHERE task_id = ?1",
            params![task_id],
        )?;

        if deleted > 0 {
            info!("已从历史数据库中删除任务: {}", task_id);
        }
        Ok(deleted > 0)
    }

    /// 按 group_id 删除任务
    pub fn remove_tasks_by_group(&self, group_id: &str) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM task_history WHERE group_id = ?1",
            params![group_id],
        )?;

        if deleted > 0 {
            info!(
                "已从历史数据库中删除文件夹 {} 的 {} 个子任务",
                group_id, deleted
            );
        }
        Ok(deleted)
    }

    /// 清理过期的任务历史
    pub fn cleanup_expired_task_history(&self, retention_days: u64) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let cutoff_timestamp = (Utc::now() - Duration::days(retention_days as i64)).timestamp();

        let deleted = conn.execute(
            "DELETE FROM task_history WHERE completed_at IS NOT NULL AND completed_at < ?1",
            params![cutoff_timestamp],
        )?;

        if deleted > 0 {
            info!(
                "已清理 {} 条过期任务历史（超过 {} 天）",
                deleted, retention_days
            );
        }
        Ok(deleted)
    }

    // ========================================================================
    // folder_history 操作
    // ========================================================================

    /// 添加文件夹到历史
    pub fn add_folder_to_history(&self, folder: &FolderPersisted) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let status = format!("{:?}", folder.status).to_lowercase();
        let pending_files_json = if folder.pending_files.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&folder.pending_files).unwrap_or_default())
        };

        conn.execute(
            r#"
            INSERT OR REPLACE INTO folder_history (
                folder_id, name, remote_root, local_root, status,
                total_files, total_size, created_count, completed_count, downloaded_size,
                scan_completed, scan_progress,
                created_at, started_at, completed_at, error,
                transfer_task_id, pending_files_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8, ?9, ?10,
                ?11, ?12,
                ?13, ?14, ?15, ?16,
                ?17, ?18
            )
            "#,
            params![
                folder.id,
                folder.name,
                folder.remote_root,
                folder.local_root.to_string_lossy().to_string(),
                status,
                folder.total_files as i64,
                folder.total_size as i64,
                folder.created_count as i64,
                folder.completed_count as i64,
                folder.downloaded_size as i64,
                if folder.scan_completed { 1 } else { 0 },
                folder.scan_progress,
                folder.created_at,
                folder.started_at,
                folder.completed_at,
                folder.error,
                folder.transfer_task_id,
                pending_files_json,
            ],
        )?;

        debug!("已添加文件夹到历史数据库: {}", folder.id);
        Ok(())
    }

    /// 批量添加文件夹到历史
    pub fn add_folders_to_history_batch(&self, folders: &[FolderPersisted]) -> Result<usize> {
        if folders.is_empty() {
            return Ok(0);
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let tx = conn.transaction()?;
        let mut count = 0;

        {
            let mut stmt = tx.prepare(
                r#"
                INSERT OR REPLACE INTO folder_history (
                    folder_id, name, remote_root, local_root, status,
                    total_files, total_size, created_count, completed_count, downloaded_size,
                    scan_completed, scan_progress,
                    created_at, started_at, completed_at, error,
                    transfer_task_id, pending_files_json
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5,
                    ?6, ?7, ?8, ?9, ?10,
                    ?11, ?12,
                    ?13, ?14, ?15, ?16,
                    ?17, ?18
                )
                "#,
            )?;

            for folder in folders {
                let status = format!("{:?}", folder.status).to_lowercase();
                let pending_files_json = if folder.pending_files.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(&folder.pending_files).unwrap_or_default())
                };

                stmt.execute(params![
                    folder.id,
                    folder.name,
                    folder.remote_root,
                    folder.local_root.to_string_lossy().to_string(),
                    status,
                    folder.total_files as i64,
                    folder.total_size as i64,
                    folder.created_count as i64,
                    folder.completed_count as i64,
                    folder.downloaded_size as i64,
                    if folder.scan_completed { 1 } else { 0 },
                    folder.scan_progress,
                    folder.created_at,
                    folder.started_at,
                    folder.completed_at,
                    folder.error,
                    folder.transfer_task_id,
                    pending_files_json,
                ])?;
                count += 1;
            }
        }

        tx.commit()?;
        info!("批量添加 {} 个文件夹到历史数据库", count);
        Ok(count)
    }

    /// 加载所有文件夹历史
    pub fn load_all_folder_history(&self) -> Result<Vec<FolderPersisted>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT
                folder_id, name, remote_root, local_root, status,
                total_files, total_size, created_count, completed_count, downloaded_size,
                scan_completed, scan_progress,
                created_at, started_at, completed_at, error,
                transfer_task_id, pending_files_json
            FROM folder_history
            ORDER BY completed_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(FolderHistoryRow {
                folder_id: row.get(0)?,
                name: row.get(1)?,
                remote_root: row.get(2)?,
                local_root: row.get(3)?,
                status: row.get(4)?,
                total_files: row.get(5)?,
                total_size: row.get(6)?,
                created_count: row.get(7)?,
                completed_count: row.get(8)?,
                downloaded_size: row.get(9)?,
                scan_completed: row.get(10)?,
                scan_progress: row.get(11)?,
                created_at: row.get(12)?,
                started_at: row.get(13)?,
                completed_at: row.get(14)?,
                error: row.get(15)?,
                transfer_task_id: row.get(16)?,
                pending_files_json: row.get(17)?,
            })
        })?;

        let mut folders = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_folder_persisted(r) {
                    Ok(folder) => folders.push(folder),
                    Err(e) => warn!("转换文件夹历史失败: {}", e),
                },
                Err(e) => warn!("读取文件夹历史行失败: {}", e),
            }
        }

        info!("从数据库加载了 {} 条文件夹历史", folders.len());
        Ok(folders)
    }

    /// 检查文件夹是否存在于历史中
    pub fn folder_exists_in_history(&self, folder_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM folder_history WHERE folder_id = ?1",
                params![folder_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        Ok(exists)
    }

    /// 从历史中删除文件夹
    pub fn remove_folder_from_history(&self, folder_id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM folder_history WHERE folder_id = ?1",
            params![folder_id],
        )?;

        if deleted > 0 {
            info!("已从历史数据库中删除文件夹: {}", folder_id);
        }
        Ok(deleted > 0)
    }

    /// 删除所有已完成的文件夹历史
    pub fn remove_completed_folders(&self) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM folder_history WHERE status = 'completed'",
            [],
        )?;

        if deleted > 0 {
            info!("已从历史数据库中删除 {} 个已完成的文件夹", deleted);
        }
        Ok(deleted)
    }

    /// 清理过期的文件夹历史
    pub fn cleanup_expired_folder_history(&self, retention_days: u64) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let cutoff_timestamp = (Utc::now() - Duration::days(retention_days as i64)).timestamp();

        let deleted = conn.execute(
            "DELETE FROM folder_history WHERE completed_at IS NOT NULL AND completed_at < ?1",
            params![cutoff_timestamp],
        )?;

        if deleted > 0 {
            info!(
                "已清理 {} 条过期文件夹历史（超过 {} 天）",
                deleted, retention_days
            );
        }
        Ok(deleted)
    }

    // ========================================================================
    // 备份任务兜底同步方法
    // ========================================================================

    /// 查询已完成的备份任务（用于服务重启时的兜底同步）
    ///
    /// 返回所有 is_backup = 1 且 status = 'completed' 的任务
    /// 包含 task_id 和 file_size（用于更新 backup_file_tasks 的 transferred_bytes）
    pub fn load_completed_backup_tasks(&self) -> Result<Vec<(String, u64)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT task_id, COALESCE(file_size, 0) as file_size
            FROM task_history
            WHERE is_backup = 1 AND status = 'completed'
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;

        let mut results = Vec::new();
        for row in rows {
            match row {
                Ok(r) => results.push(r),
                Err(e) => warn!("读取已完成备份任务失败: {}", e),
            }
        }

        Ok(results)
    }

    // ========================================================================
    // 辅助方法
    // ========================================================================

    /// 将数据库行转换为 TaskMetadata
    fn row_to_task_metadata(&self, row: TaskHistoryRow) -> Result<TaskMetadata> {
        let task_type = match row.task_type.as_str() {
            "download" => TaskType::Download,
            "upload" => TaskType::Upload,
            "transfer" => TaskType::Transfer,
            _ => return Err(anyhow!("未知的任务类型: {}", row.task_type)),
        };

        let status = match row.status.as_str() {
            "pending" => Some(TaskPersistenceStatus::Pending),
            "downloading" => Some(TaskPersistenceStatus::Downloading),
            "uploading" => Some(TaskPersistenceStatus::Uploading),
            "transferring" => Some(TaskPersistenceStatus::Transferring),
            "paused" => Some(TaskPersistenceStatus::Paused),
            "completed" => Some(TaskPersistenceStatus::Completed),
            "failed" => Some(TaskPersistenceStatus::Failed),
            _ => None,
        };

        let download_task_ids: Vec<String> = row
            .download_task_ids
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        Ok(TaskMetadata {
            task_id: row.task_id,
            task_type,
            created_at: Utc.timestamp_opt(row.created_at, 0).single().unwrap_or_else(Utc::now),
            updated_at: Utc.timestamp_opt(row.updated_at, 0).single().unwrap_or_else(Utc::now),
            fs_id: row.fs_id.map(|id| id as u64),
            transfer_task_id: row.transfer_task_id,
            remote_path: row.remote_path,
            local_path: row.local_path.map(std::path::PathBuf::from),
            file_size: row.file_size.map(|s| s as u64),
            chunk_size: row.chunk_size.map(|s| s as u64),
            total_chunks: row.total_chunks.map(|c| c as usize),
            source_path: row.source_path.map(std::path::PathBuf::from),
            target_path: row.target_path,
            upload_id: row.upload_id,
            upload_id_created_at: None,
            share_link: row.share_link,
            share_pwd: row.share_pwd,
            transfer_target_path: row.transfer_target_path,
            transfer_status: row.transfer_status,
            download_task_ids,
            share_info_json: None,
            auto_download: row.auto_download.map(|v| v != 0),
            transfer_file_name: row.transfer_file_name,
            group_id: row.group_id,
            group_root: row.group_root,
            relative_path: row.relative_path,
            status,
            completed_at: row.completed_at.and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
            error_msg: row.error_msg,
            is_backup: row.is_backup.map(|v| v != 0).unwrap_or(false),
            backup_config_id: row.backup_config_id,
            // 加密字段
            encrypt_enabled: false,
            is_encrypted: false,
            encryption_key_version: None,
        })
    }

    /// 将数据库行转换为 FolderPersisted
    fn row_to_folder_persisted(&self, row: FolderHistoryRow) -> Result<FolderPersisted> {
        let status = match row.status.as_str() {
            "scanning" => FolderStatus::Scanning,
            "downloading" => FolderStatus::Downloading,
            "paused" => FolderStatus::Paused,
            "completed" => FolderStatus::Completed,
            "failed" => FolderStatus::Failed,
            "cancelled" => FolderStatus::Cancelled,
            _ => FolderStatus::Scanning, // 默认为 Scanning
        };

        let pending_files = row
            .pending_files_json
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        Ok(FolderPersisted {
            id: row.folder_id,
            name: row.name,
            remote_root: row.remote_root,
            local_root: std::path::PathBuf::from(row.local_root),
            status,
            total_files: row.total_files as u64,
            total_size: row.total_size as u64,
            created_count: row.created_count as u64,
            completed_count: row.completed_count as u64,
            downloaded_size: row.downloaded_size as u64,
            scan_completed: row.scan_completed != 0,
            scan_progress: row.scan_progress,
            pending_files,
            created_at: row.created_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            error: row.error,
            transfer_task_id: row.transfer_task_id,
        })
    }
}

// ========================================================================
// 辅助结构体
// ========================================================================

/// 任务历史行
struct TaskHistoryRow {
    task_id: String,
    task_type: String,
    status: String,
    created_at: i64,
    updated_at: i64,
    completed_at: Option<i64>,
    fs_id: Option<i64>,
    remote_path: Option<String>,
    local_path: Option<String>,
    source_path: Option<String>,
    target_path: Option<String>,
    upload_id: Option<String>,
    share_link: Option<String>,
    share_pwd: Option<String>,
    transfer_target_path: Option<String>,
    transfer_status: Option<String>,
    transfer_file_name: Option<String>,
    auto_download: Option<i64>,
    file_size: Option<i64>,
    chunk_size: Option<i64>,
    total_chunks: Option<i64>,
    error_msg: Option<String>,
    group_id: Option<String>,
    group_root: Option<String>,
    relative_path: Option<String>,
    is_backup: Option<i64>,
    backup_config_id: Option<String>,
    transfer_task_id: Option<String>,
    download_task_ids: Option<String>,
}

/// 文件夹历史行
struct FolderHistoryRow {
    folder_id: String,
    name: String,
    remote_root: String,
    local_root: String,
    status: String,
    total_files: i64,
    total_size: i64,
    created_count: i64,
    completed_count: i64,
    downloaded_size: i64,
    scan_completed: i64,
    scan_progress: Option<String>,
    created_at: i64,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    error: Option<String>,
    transfer_task_id: Option<String>,
    pending_files_json: Option<String>,
}

// ========================================================================
// 离线下载自动下载配置
// ========================================================================

/// 离线下载自动下载配置（用于持久化）
#[derive(Debug, Clone)]
pub struct CloudDlAutoDownloadConfig {
    /// 离线下载任务 ID
    pub task_id: i64,
    /// 是否启用自动下载
    pub enabled: bool,
    /// 本地下载目录
    pub local_path: Option<String>,
    /// 完成时是否每次询问下载目录
    pub ask_each_time: bool,
    /// 创建时间戳
    pub created_at: i64,
    /// 是否已触发自动下载（防止重复触发）
    pub triggered: bool,
    /// 触发时间戳（触发自动下载的时间）
    pub triggered_at: Option<i64>,
}


// ========================================================================
// cloud_dl_auto_download 操作
// ========================================================================

impl HistoryDbManager {
    /// 保存离线下载自动下载配置
    pub fn save_cloud_dl_auto_download(&self, config: &CloudDlAutoDownloadConfig) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        conn.execute(
            r#"
            INSERT OR REPLACE INTO cloud_dl_auto_download (
                task_id, enabled, local_path, ask_each_time, created_at, triggered, triggered_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                config.task_id,
                if config.enabled { 1 } else { 0 },
                config.local_path,
                if config.ask_each_time { 1 } else { 0 },
                config.created_at,
                if config.triggered { 1 } else { 0 },
                config.triggered_at,
            ],
        )?;

        debug!("已保存离线下载自动下载配置: task_id={}", config.task_id);
        Ok(())
    }

    /// 标记自动下载已触发（防止重复触发）
    pub fn mark_cloud_dl_auto_download_triggered(&self, task_id: i64) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let now = chrono::Utc::now().timestamp();
        let updated = conn.execute(
            "UPDATE cloud_dl_auto_download SET triggered = 1, triggered_at = ?1 WHERE task_id = ?2 AND triggered = 0",
            params![now, task_id],
        )?;

        if updated > 0 {
            info!("已标记离线下载自动下载为已触发: task_id={}", task_id);
        }
        Ok(updated > 0)
    }

    /// 删除离线下载自动下载配置
    pub fn remove_cloud_dl_auto_download(&self, task_id: i64) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM cloud_dl_auto_download WHERE task_id = ?1",
            params![task_id],
        )?;

        if deleted > 0 {
            debug!("已删除离线下载自动下载配置: task_id={}", task_id);
        }
        Ok(deleted > 0)
    }

    /// 加载所有未触发的离线下载自动下载配置
    ///
    /// 只返回 triggered = 0 的配置，用于服务重启后恢复监听
    pub fn load_pending_cloud_dl_auto_download(&self) -> Result<Vec<CloudDlAutoDownloadConfig>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT task_id, enabled, local_path, ask_each_time, created_at, triggered, triggered_at
            FROM cloud_dl_auto_download
            WHERE triggered = 0
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(CloudDlAutoDownloadConfig {
                task_id: row.get(0)?,
                enabled: row.get::<_, i64>(1)? != 0,
                local_path: row.get(2)?,
                ask_each_time: row.get::<_, i64>(3)? != 0,
                created_at: row.get(4)?,
                triggered: row.get::<_, i64>(5)? != 0,
                triggered_at: row.get(6)?,
            })
        })?;

        let mut configs = Vec::new();
        for row in rows {
            match row {
                Ok(config) => configs.push(config),
                Err(e) => warn!("读取离线下载自动下载配置失败: {}", e),
            }
        }

        info!("从数据库加载了 {} 条待触发的离线下载自动下载配置", configs.len());
        Ok(configs)
    }

    /// 加载所有离线下载自动下载配置（包括已触发的）
    pub fn load_all_cloud_dl_auto_download(&self) -> Result<Vec<CloudDlAutoDownloadConfig>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT task_id, enabled, local_path, ask_each_time, created_at, triggered, triggered_at
            FROM cloud_dl_auto_download
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(CloudDlAutoDownloadConfig {
                task_id: row.get(0)?,
                enabled: row.get::<_, i64>(1)? != 0,
                local_path: row.get(2)?,
                ask_each_time: row.get::<_, i64>(3)? != 0,
                created_at: row.get(4)?,
                triggered: row.get::<_, i64>(5)? != 0,
                triggered_at: row.get(6)?,
            })
        })?;

        let mut configs = Vec::new();
        for row in rows {
            match row {
                Ok(config) => configs.push(config),
                Err(e) => warn!("读取离线下载自动下载配置失败: {}", e),
            }
        }

        info!("从数据库加载了 {} 条离线下载自动下载配置", configs.len());
        Ok(configs)
    }

    /// 获取单个离线下载自动下载配置
    pub fn get_cloud_dl_auto_download(&self, task_id: i64) -> Result<Option<CloudDlAutoDownloadConfig>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let config = conn
            .query_row(
                r#"
                SELECT task_id, enabled, local_path, ask_each_time, created_at, triggered, triggered_at
                FROM cloud_dl_auto_download
                WHERE task_id = ?1
                "#,
                params![task_id],
                |row| {
                    Ok(CloudDlAutoDownloadConfig {
                        task_id: row.get(0)?,
                        enabled: row.get::<_, i64>(1)? != 0,
                        local_path: row.get(2)?,
                        ask_each_time: row.get::<_, i64>(3)? != 0,
                        created_at: row.get(4)?,
                        triggered: row.get::<_, i64>(5)? != 0,
                        triggered_at: row.get(6)?,
                    })
                },
            )
            .optional()?;

        Ok(config)
    }

    /// 清理已触发的离线下载自动下载配置（可选：保留最近 N 天的记录）
    pub fn cleanup_triggered_cloud_dl_auto_download(&self, retention_days: Option<u64>) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = if let Some(days) = retention_days {
            let cutoff = (chrono::Utc::now() - chrono::Duration::days(days as i64)).timestamp();
            conn.execute(
                "DELETE FROM cloud_dl_auto_download WHERE triggered = 1 AND triggered_at < ?1",
                params![cutoff],
            )?
        } else {
            conn.execute(
                "DELETE FROM cloud_dl_auto_download WHERE triggered = 1",
                [],
            )?
        };

        if deleted > 0 {
            info!("已清理 {} 条已触发的离线下载自动下载配置", deleted);
        }
        Ok(deleted)
    }

    /// 清理所有离线下载自动下载配置
    pub fn clear_all_cloud_dl_auto_download(&self) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute("DELETE FROM cloud_dl_auto_download", [])?;

        if deleted > 0 {
            info!("已清理 {} 条离线下载自动下载配置", deleted);
        }
        Ok(deleted)
    }
}
