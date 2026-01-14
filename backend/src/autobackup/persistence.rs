//! 备份任务持久化模块
//!
//! 将备份任务状态持久化到 SQLite 数据库，支持断点恢复

use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

use super::task::{
    BackupFileStatus, BackupFileTask, BackupOperationType, BackupTask, BackupTaskStatus,
    BackupSubPhase, SkipReason, TriggerType,
};

// ==================== 分页常量 ====================

/// 默认分页大小
pub const DEFAULT_PAGE_SIZE: usize = 100;

/// 最大分页大小
pub const MAX_PAGE_SIZE: usize = 500;

/// 规范化分页参数
/// 
/// - 如果 page_size 为 0，返回默认值 100
/// - 如果 page_size 超过 500，截断为 500 并记录警告
/// - 否则返回原值
pub fn normalize_pagination(page_size: usize) -> usize {
    if page_size == 0 {
        DEFAULT_PAGE_SIZE
    } else if page_size > MAX_PAGE_SIZE {
        tracing::warn!(
            "请求的 page_size {} 超过最大限制，已截断为 {}",
            page_size,
            MAX_PAGE_SIZE
        );
        MAX_PAGE_SIZE
    } else {
        page_size
    }
}

/// 备份任务持久化管理器
pub struct BackupPersistenceManager {
    /// SQLite 连接
    conn: Mutex<Connection>,
}

impl BackupPersistenceManager {
    /// 创建新的持久化管理器
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
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 创建主任务表
        conn.execute(
            r#"
            -- ============================================
            -- 表: backup_tasks (备份主任务表)
            -- 描述: 存储备份任务的整体状态和进度信息
            -- ============================================
            CREATE TABLE IF NOT EXISTS backup_tasks (
                id TEXT PRIMARY KEY,                    -- 任务唯一标识 (UUID)
                config_id TEXT NOT NULL,                -- 关联的备份配置ID
                status TEXT NOT NULL,                   -- 任务状态: queued/preparing/transferring/completed/failed/cancelled/paused
                sub_phase TEXT,                         -- 子阶段: dedupchecking/waitingslot/encrypting/uploading/downloading/decrypting/preempted
                trigger_type TEXT NOT NULL,             -- 触发类型: watch(文件监听)/poll(定时轮询)/manual(手动触发)
                completed_count INTEGER DEFAULT 0,      -- 已完成文件数
                failed_count INTEGER DEFAULT 0,         -- 失败文件数
                skipped_count INTEGER DEFAULT 0,        -- 跳过文件数(去重跳过)
                total_count INTEGER DEFAULT 0,          -- 总文件数
                transferred_bytes INTEGER DEFAULT 0,    -- 已传输字节数
                total_bytes INTEGER DEFAULT 0,          -- 总字节数
                error_message TEXT,                     -- 错误信息(失败时记录)
                created_at INTEGER NOT NULL,            -- 创建时间 (Unix timestamp 秒)
                started_at INTEGER,                     -- 开始执行时间
                completed_at INTEGER                    -- 完成时间
            )
            "#,
            [],
        )?;

        // 创建索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backup_tasks_config ON backup_tasks(config_id)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backup_tasks_status ON backup_tasks(status)",
            [],
        )?;

        // 创建子任务表
        conn.execute(
            r#"
            -- ============================================
            -- 表: backup_file_tasks (备份文件子任务表)
            -- 描述: 记录每次备份任务下的文件级详情，用于历史查询和状态追踪
            -- 用途:
            --   1. 任务详情展示（文件列表、状态、错误信息）
            --   2. 历史任务查询（内存清理后仍可查）
            --   3. 去重兜底（冗余 head_md5，防 upload_records 异常）
            --   4. 故障排查（保留完整路径、加密信息、sub_phase）
            --   5. 下载备份断点续传（fs_id 用于重建下载任务）
            -- ============================================
            CREATE TABLE IF NOT EXISTS backup_file_tasks (
                id TEXT PRIMARY KEY,                    -- 文件任务唯一标识 (UUID)
                backup_task_id TEXT NOT NULL,           -- 所属主任务ID (外键关联 backup_tasks.id)
                config_id TEXT NOT NULL DEFAULT '',     -- 备份配置ID（冗余，方便按配置查询）
                relative_path TEXT NOT NULL DEFAULT '', -- 相对路径（相对于备份源目录）
                file_name TEXT NOT NULL DEFAULT '',     -- 文件名
                local_path TEXT NOT NULL,               -- 本地文件绝对路径
                remote_path TEXT NOT NULL,              -- 远程目标路径 (百度网盘路径)
                file_size INTEGER NOT NULL,             -- 文件大小 (字节)
                head_md5 TEXT NOT NULL DEFAULT '',      -- 文件头MD5（前128KB，去重兜底）
                fs_id INTEGER,                          -- 百度网盘文件ID（下载备份用，用于重启后重建下载任务）
                status TEXT NOT NULL,                   -- 文件状态: pending/checking/skipped/encrypting/waitingtransfer/transferring/completed/failed
                sub_phase TEXT,                         -- 子阶段: dedup_checking/waiting_slot/encrypting/uploading/downloading/decrypting/preempted
                skip_reason TEXT,                       -- 跳过原因 (JSON格式，去重时记录)
                encrypted INTEGER DEFAULT 0,            -- 是否加密: 0=否, 1=是
                encrypted_name TEXT,                    -- 加密后的文件名 (加密时使用)
                temp_encrypted_path TEXT,               -- 临时加密文件路径
                transferred_bytes INTEGER DEFAULT 0,    -- 已传输字节数 (用于断点续传)
                error_message TEXT,                     -- 错误信息
                retry_count INTEGER DEFAULT 0,          -- 重试次数
                related_task_id TEXT,                   -- 关联的任务ID（上传或下载任务ID，用于服务重启后恢复）
                backup_operation_type TEXT,             -- 备份操作类型: upload/download
                created_at INTEGER NOT NULL,            -- 创建时间 (Unix timestamp 秒)
                updated_at INTEGER NOT NULL,            -- 最后更新时间
                FOREIGN KEY (backup_task_id) REFERENCES backup_tasks(id)
            )
            "#,
            [],
        )?;

        // 创建子任务索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backup_file_task_id ON backup_file_tasks(backup_task_id)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backup_file_status ON backup_file_tasks(backup_task_id, status)",
            [],
        )?;
        // 新增索引：按配置和时间查询
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_tasks_config_time ON backup_file_tasks(config_id, created_at)",
            [],
        )?;
        // 新增索引：按状态查询
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_tasks_status ON backup_file_tasks(status)",
            [],
        )?;
        
        // 新增索引：按关联任务ID查询（用于监听器查找备份任务）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_tasks_related_task ON backup_file_tasks(related_task_id)",
            [],
        )?;

        // 新增索引：按 fs_id 查询（用于下载备份重启恢复时重建任务）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_tasks_fs_id ON backup_file_tasks(fs_id)",
            [],
        )?;

        tracing::info!("备份任务数据库表初始化完成");
        Ok(())
    }

    // ==================== 主任务操作 ====================

    /// 保存备份任务
    pub fn save_task(&self, task: &BackupTask) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let status = format!("{:?}", task.status).to_lowercase();
        let sub_phase = task.sub_phase.map(|p| format!("{:?}", p).to_lowercase());
        let trigger_type = format!("{:?}", task.trigger_type).to_lowercase();

        conn.execute(
            r#"
            INSERT OR REPLACE INTO backup_tasks (
                id, config_id, status, sub_phase, trigger_type,
                completed_count, failed_count, skipped_count, total_count,
                transferred_bytes, total_bytes, error_message,
                created_at, started_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            "#,
            params![
                task.id,
                task.config_id,
                status,
                sub_phase,
                trigger_type,
                task.completed_count as i64,
                task.failed_count as i64,
                task.skipped_count as i64,
                task.total_count as i64,
                task.transferred_bytes as i64,
                task.total_bytes as i64,
                task.error_message,
                task.created_at.timestamp(),
                task.started_at.map(|t| t.timestamp()),
                task.completed_at.map(|t| t.timestamp()),
            ],
        )?;

        Ok(())
    }

    /// 加载备份任务
    pub fn load_task(&self, task_id: &str) -> Result<Option<BackupTask>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let result = conn
            .query_row(
                r#"
                SELECT id, config_id, status, sub_phase, trigger_type,
                       completed_count, failed_count, skipped_count, total_count,
                       transferred_bytes, total_bytes, error_message,
                       created_at, started_at, completed_at
                FROM backup_tasks WHERE id = ?1
                "#,
                params![task_id],
                |row| {
                    Ok(BackupTaskRow {
                        id: row.get(0)?,
                        config_id: row.get(1)?,
                        status: row.get(2)?,
                        sub_phase: row.get(3)?,
                        trigger_type: row.get(4)?,
                        completed_count: row.get(5)?,
                        failed_count: row.get(6)?,
                        skipped_count: row.get(7)?,
                        total_count: row.get(8)?,
                        transferred_bytes: row.get(9)?,
                        total_bytes: row.get(10)?,
                        error_message: row.get(11)?,
                        created_at: row.get(12)?,
                        started_at: row.get(13)?,
                        completed_at: row.get(14)?,
                    })
                },
            )
            .optional()?;

        match result {
            Some(row) => Ok(Some(self.row_to_task(row)?)),
            None => Ok(None),
        }
    }

    /// 更新任务状态
    pub fn update_task_status(
        &self,
        task_id: &str,
        status: BackupTaskStatus,
        sub_phase: Option<BackupSubPhase>,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let status_str = format!("{:?}", status).to_lowercase();
        let sub_phase_str = sub_phase.map(|p| format!("{:?}", p).to_lowercase());

        conn.execute(
            "UPDATE backup_tasks SET status = ?1, sub_phase = ?2 WHERE id = ?3",
            params![status_str, sub_phase_str, task_id],
        )?;

        Ok(())
    }

    /// 更新任务进度
    pub fn update_task_progress(
        &self,
        task_id: &str,
        completed_count: usize,
        failed_count: usize,
        skipped_count: usize,
        transferred_bytes: u64,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        conn.execute(
            r#"
            UPDATE backup_tasks
            SET completed_count = ?1, failed_count = ?2, skipped_count = ?3, transferred_bytes = ?4
            WHERE id = ?5
            "#,
            params![
                completed_count as i64,
                failed_count as i64,
                skipped_count as i64,
                transferred_bytes as i64,
                task_id
            ],
        )?;

        Ok(())
    }

    /// 加载未完成的任务
    pub fn load_incomplete_tasks(&self) -> Result<Vec<BackupTask>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT id, config_id, status, sub_phase, trigger_type,
                   completed_count, failed_count, skipped_count, total_count,
                   transferred_bytes, total_bytes, error_message,
                   created_at, started_at, completed_at
            FROM backup_tasks
            WHERE status NOT IN ('completed', 'cancelled', 'failed')
            ORDER BY created_at ASC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(BackupTaskRow {
                id: row.get(0)?,
                config_id: row.get(1)?,
                status: row.get(2)?,
                sub_phase: row.get(3)?,
                trigger_type: row.get(4)?,
                completed_count: row.get(5)?,
                failed_count: row.get(6)?,
                skipped_count: row.get(7)?,
                total_count: row.get(8)?,
                transferred_bytes: row.get(9)?,
                total_bytes: row.get(10)?,
                error_message: row.get(11)?,
                created_at: row.get(12)?,
                started_at: row.get(13)?,
                completed_at: row.get(14)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_task(r) {
                    Ok(task) => tasks.push(task),
                    Err(e) => tracing::warn!("转换任务失败: {}", e),
                },
                Err(e) => tracing::warn!("读取任务行失败: {}", e),
            }
        }

        Ok(tasks)
    }

    /// 删除任务
    pub fn delete_task(&self, task_id: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 先删除子任务
        conn.execute(
            "DELETE FROM backup_file_tasks WHERE backup_task_id = ?1",
            params![task_id],
        )?;

        // 再删除主任务
        conn.execute("DELETE FROM backup_tasks WHERE id = ?1", params![task_id])?;

        Ok(())
    }

    // ==================== 子任务操作 ====================

    /// 保存文件任务
    pub fn save_file_task(&self, file_task: &BackupFileTask, config_id: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let status = format!("{:?}", file_task.status).to_lowercase();
        let skip_reason = file_task
            .skip_reason
            .as_ref()
            .map(|r| serde_json::to_string(r).unwrap_or_default());

        // 提取文件名和相对路径
        let file_name = file_task.local_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();
        let relative_path = file_task.local_path.parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        conn.execute(
            r#"
            INSERT OR REPLACE INTO backup_file_tasks (
                id, backup_task_id, config_id, relative_path, file_name,
                local_path, remote_path, file_size, head_md5, fs_id,
                status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                transferred_bytes, error_message, retry_count,
                related_task_id, backup_operation_type,
                created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
            "#,
            params![
                file_task.id,
                file_task.parent_task_id,
                config_id,
                relative_path,
                file_name,
                file_task.local_path.to_string_lossy().to_string(),
                file_task.remote_path,
                file_task.file_size as i64,
                file_task.head_md5.as_deref().unwrap_or(""),
                file_task.fs_id.map(|id| id as i64),
                status,
                file_task.sub_phase.map(|p| format!("{:?}", p).to_lowercase()),
                skip_reason,
                file_task.encrypted,
                file_task.encrypted_name,
                file_task.temp_encrypted_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                file_task.transferred_bytes as i64,
                file_task.error_message,
                file_task.retry_count as i64,
                file_task.related_task_id,
                file_task.backup_operation_type.map(|t| format!("{:?}", t).to_lowercase()),
                file_task.created_at.timestamp(),
                file_task.updated_at.timestamp(),
            ],
        )?;

        Ok(())
    }

    /// 批量保存文件任务（使用事务，满足内存优化要求）
    pub fn save_file_tasks_batch(&self, file_tasks: &[BackupFileTask], config_id: &str) -> Result<()> {
        let mut conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 使用事务批量插入，提高性能
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                r#"
                INSERT OR REPLACE INTO backup_file_tasks (
                    id, backup_task_id, config_id, relative_path, file_name,
                    local_path, remote_path, file_size, head_md5, fs_id,
                    status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                    transferred_bytes, error_message, retry_count,
                    related_task_id, backup_operation_type,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
                "#,
            )?;

            for file_task in file_tasks {
                let status = format!("{:?}", file_task.status).to_lowercase();
                let skip_reason = file_task
                    .skip_reason
                    .as_ref()
                    .map(|r| serde_json::to_string(r).unwrap_or_default());

                // 提取文件名和相对路径
                let file_name = file_task.local_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                let relative_path = file_task.local_path.parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();

                stmt.execute(params![
                    file_task.id,
                    file_task.parent_task_id,
                    config_id,
                    relative_path,
                    file_name,
                    file_task.local_path.to_string_lossy().to_string(),
                    file_task.remote_path,
                    file_task.file_size as i64,
                    file_task.head_md5.as_deref().unwrap_or(""),
                    file_task.fs_id.map(|id| id as i64),
                    status,
                    file_task.sub_phase.map(|p| format!("{:?}", p).to_lowercase()),
                    skip_reason,
                    file_task.encrypted,
                    file_task.encrypted_name,
                    file_task.temp_encrypted_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                    file_task.transferred_bytes as i64,
                    file_task.error_message,
                    file_task.retry_count as i64,
                    file_task.related_task_id,
                    file_task.backup_operation_type.map(|t| format!("{:?}", t).to_lowercase()),
                    file_task.created_at.timestamp(),
                    file_task.updated_at.timestamp(),
                ])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// 加载文件任务（分页）
    /// 
    /// 分页参数会被规范化：
    /// - page_size 为 0 时使用默认值 100
    /// - page_size 超过 500 时截断为 500
    pub fn load_file_tasks(
        &self,
        task_id: &str,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<BackupFileTask>, usize)> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 规范化分页参数
        let normalized_page_size = normalize_pagination(page_size);

        // 获取总数
        let total: usize = conn.query_row(
            "SELECT COUNT(*) FROM backup_file_tasks WHERE backup_task_id = ?1",
            params![task_id],
            |row| row.get(0),
        )?;

        // 分页查询
        let offset = (page.saturating_sub(1)) * normalized_page_size;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, backup_task_id, config_id, relative_path, file_name,
                   local_path, remote_path, file_size, head_md5, fs_id,
                   status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                   transferred_bytes, error_message, retry_count,
                   related_task_id, backup_operation_type,
                   created_at, updated_at
            FROM backup_file_tasks
            WHERE backup_task_id = ?1
            ORDER BY created_at ASC
            LIMIT ?2 OFFSET ?3
            "#,
        )?;

        let rows = stmt.query_map(params![task_id, normalized_page_size as i64, offset as i64], |row| {
            Ok(BackupFileTaskRow {
                id: row.get(0)?,
                backup_task_id: row.get(1)?,
                config_id: row.get(2)?,
                relative_path: row.get(3)?,
                file_name: row.get(4)?,
                local_path: row.get(5)?,
                remote_path: row.get(6)?,
                file_size: row.get(7)?,
                head_md5: row.get(8)?,
                fs_id: row.get(9)?,
                status: row.get(10)?,
                sub_phase: row.get(11)?,
                skip_reason: row.get(12)?,
                encrypted: row.get(13)?,
                encrypted_name: row.get(14)?,
                temp_encrypted_path: row.get(15)?,
                transferred_bytes: row.get(16)?,
                error_message: row.get(17)?,
                retry_count: row.get(18)?,
                related_task_id: row.get(19)?,
                backup_operation_type: row.get(20)?,
                created_at: row.get(21)?,
                updated_at: row.get(22)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_file_task(r) {
                    Ok(task) => tasks.push(task),
                    Err(e) => tracing::warn!("转换文件任务失败: {}", e),
                },
                Err(e) => tracing::warn!("读取文件任务行失败: {}", e),
            }
        }

        Ok((tasks, total))
    }

    /// 更新文件任务状态（含子阶段）
    pub fn update_file_task_status(
        &self,
        file_task_id: &str,
        status: BackupFileStatus,
        sub_phase: Option<BackupSubPhase>,
        error_message: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let status_str = format!("{:?}", status).to_lowercase();
        let sub_phase_str = sub_phase.map(|p| format!("{:?}", p).to_lowercase());
        let now = chrono::Utc::now().timestamp();

        conn.execute(
            "UPDATE backup_file_tasks SET status = ?1, sub_phase = ?2, error_message = ?3, updated_at = ?4 WHERE id = ?5",
            params![status_str, sub_phase_str, error_message, now, file_task_id],
        )?;

        Ok(())
    }

    /// 更新文件任务进度
    pub fn update_file_task_progress(
        &self,
        file_task_id: &str,
        transferred_bytes: u64,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let now = chrono::Utc::now().timestamp();

        conn.execute(
            "UPDATE backup_file_tasks SET transferred_bytes = ?1, updated_at = ?2 WHERE id = ?3",
            params![transferred_bytes as i64, now, file_task_id],
        )?;

        Ok(())
    }

    // ==================== 批量处理和懒加载 ====================

    /// 获取下一批待处理的文件任务（用于内存优化）
    /// 只加载指定数量的待处理文件，避免一次性加载全部
    pub fn get_next_pending_files(&self, task_id: &str, limit: usize) -> Result<Vec<BackupFileTask>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT id, backup_task_id, config_id, relative_path, file_name,
                   local_path, remote_path, file_size, head_md5, fs_id,
                   status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                   transferred_bytes, error_message, retry_count,
                   related_task_id, backup_operation_type,
                   created_at, updated_at
            FROM backup_file_tasks
            WHERE backup_task_id = ?1 AND status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?2
            "#,
        )?;

        let rows = stmt.query_map(params![task_id, limit as i64], |row| {
            Ok(BackupFileTaskRow {
                id: row.get(0)?,
                backup_task_id: row.get(1)?,
                config_id: row.get(2)?,
                relative_path: row.get(3)?,
                file_name: row.get(4)?,
                local_path: row.get(5)?,
                remote_path: row.get(6)?,
                file_size: row.get(7)?,
                head_md5: row.get(8)?,
                fs_id: row.get(9)?,
                status: row.get(10)?,
                sub_phase: row.get(11)?,
                skip_reason: row.get(12)?,
                encrypted: row.get(13)?,
                encrypted_name: row.get(14)?,
                temp_encrypted_path: row.get(15)?,
                transferred_bytes: row.get(16)?,
                error_message: row.get(17)?,
                retry_count: row.get(18)?,
                related_task_id: row.get(19)?,
                backup_operation_type: row.get(20)?,
                created_at: row.get(21)?,
                updated_at: row.get(22)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_file_task(r) {
                    Ok(task) => tasks.push(task),
                    Err(e) => tracing::warn!("转换文件任务失败: {}", e),
                },
                Err(e) => tracing::warn!("读取文件任务行失败: {}", e),
            }
        }

        Ok(tasks)
    }

    /// 加载用于恢复的文件任务（非终态）
    ///
    /// 用于服务重启后恢复 pending_files，仅加载需要继续处理的文件任务：
    /// - 过滤掉终态：Completed / Failed / Cancelled / Skipped
    /// - 按 updated_at 排序，保证恢复顺序稳定
    ///
    /// 终态定义：
    /// - Completed: 已完成
    /// - Failed: 已失败（不自动重试）
    /// - Skipped: 已跳过（去重等原因）
    /// - 注：Cancelled 状态不在 BackupFileStatus 中，但如有需要可扩展
    pub fn load_file_tasks_for_restore(&self, backup_task_id: &str) -> Result<Vec<BackupFileTask>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 非终态条件：排除 completed, failed, skipped, cancelled
        let mut stmt = conn.prepare(
            r#"
            SELECT id, backup_task_id, config_id, relative_path, file_name,
                   local_path, remote_path, file_size, head_md5, fs_id,
                   status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                   transferred_bytes, error_message, retry_count,
                   related_task_id, backup_operation_type,
                   created_at, updated_at
            FROM backup_file_tasks
            WHERE backup_task_id = ?1
              AND status NOT IN ('completed', 'failed', 'skipped', 'cancelled')
            ORDER BY updated_at ASC, created_at ASC
            "#,
        )?;

        let rows = stmt.query_map(params![backup_task_id], |row| {
            Ok(BackupFileTaskRow {
                id: row.get(0)?,
                backup_task_id: row.get(1)?,
                config_id: row.get(2)?,
                relative_path: row.get(3)?,
                file_name: row.get(4)?,
                local_path: row.get(5)?,
                remote_path: row.get(6)?,
                file_size: row.get(7)?,
                head_md5: row.get(8)?,
                fs_id: row.get(9)?,
                status: row.get(10)?,
                sub_phase: row.get(11)?,
                skip_reason: row.get(12)?,
                encrypted: row.get(13)?,
                encrypted_name: row.get(14)?,
                temp_encrypted_path: row.get(15)?,
                transferred_bytes: row.get(16)?,
                error_message: row.get(17)?,
                retry_count: row.get(18)?,
                related_task_id: row.get(19)?,
                backup_operation_type: row.get(20)?,
                created_at: row.get(21)?,
                updated_at: row.get(22)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_file_task(r) {
                    Ok(task) => tasks.push(task),
                    Err(e) => tracing::warn!("恢复时转换文件任务失败: {}", e),
                },
                Err(e) => tracing::warn!("恢复时读取文件任务行失败: {}", e),
            }
        }

        Ok(tasks)
    }

    /// 获取待处理文件数量（不加载文件内容）
    pub fn count_pending_files(&self, task_id: &str) -> Result<usize> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM backup_file_tasks WHERE backup_task_id = ?1 AND status = 'pending'",
            params![task_id],
            |row| row.get(0),
        )?;

        Ok(count as usize)
    }

    /// 获取各状态文件数量统计
    pub fn get_file_stats(&self, task_id: &str) -> Result<FileTaskStats> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT status, COUNT(*) as cnt, COALESCE(SUM(file_size), 0) as total_size
            FROM backup_file_tasks
            WHERE backup_task_id = ?1
            GROUP BY status
            "#,
        )?;

        let rows = stmt.query_map(params![task_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
        })?;

        let mut stats = FileTaskStats::default();
        for row in rows {
            if let Ok((status, count, size)) = row {
                match status.as_str() {
                    "pending" => {
                        stats.pending_count = count as usize;
                        stats.pending_bytes = size as u64;
                    }
                    "checking" => stats.checking_count = count as usize,
                    "skipped" => stats.skipped_count = count as usize,
                    "encrypting" => stats.encrypting_count = count as usize,
                    "waitingtransfer" => stats.waiting_transfer_count = count as usize,
                    "transferring" => stats.transferring_count = count as usize,
                    "completed" => {
                        stats.completed_count = count as usize;
                        stats.completed_bytes = size as u64;
                    }
                    "failed" => stats.failed_count = count as usize,
                    _ => {}
                }
            }
        }

        Ok(stats)
    }

    /// 批量更新文件任务状态（高效批量操作）
    pub fn batch_update_file_status(
        &self,
        file_task_ids: &[&str],
        status: BackupFileStatus,
    ) -> Result<()> {
        if file_task_ids.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;
        let status_str = format!("{:?}", status).to_lowercase();
        let now = chrono::Utc::now().timestamp();

        // 使用事务批量更新
        let placeholders: Vec<String> = file_task_ids.iter().enumerate().map(|(i, _)| format!("?{}", i + 3)).collect();
        let query = format!(
            "UPDATE backup_file_tasks SET status = ?1, updated_at = ?2 WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        params_vec.push(Box::new(status_str));
        params_vec.push(Box::new(now));
        for id in file_task_ids {
            params_vec.push(Box::new(id.to_string()));
        }

        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
        conn.execute(&query, params_refs.as_slice())?;

        Ok(())
    }

    /// 删除已完成/已跳过的文件任务（释放数据库空间）
    pub fn cleanup_completed_file_tasks(&self, task_id: &str) -> Result<usize> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let deleted = conn.execute(
            "DELETE FROM backup_file_tasks WHERE backup_task_id = ?1 AND status IN ('completed', 'skipped')",
            params![task_id],
        )?;

        Ok(deleted)
    }

    // ==================== 按配置查询（内存优化新增）====================

    /// 按配置查询任务列表（分页）
    /// 用于 DB + 内存合并查询
    /// 
    /// 分页参数会被规范化：
    /// - limit 为 0 时使用默认值 100
    /// - limit 超过 500 时截断为 500
    pub fn get_tasks_by_config(&self, config_id: &str, limit: usize, offset: usize) -> Result<Vec<BackupTask>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 规范化分页参数
        let normalized_limit = normalize_pagination(limit);

        let mut stmt = conn.prepare(
            r#"
            SELECT id, config_id, status, sub_phase, trigger_type,
                   completed_count, failed_count, skipped_count, total_count,
                   transferred_bytes, total_bytes, error_message,
                   created_at, started_at, completed_at
            FROM backup_tasks
            WHERE config_id = ?1
            ORDER BY created_at DESC
            LIMIT ?2 OFFSET ?3
            "#,
        )?;

        let rows = stmt.query_map(params![config_id, normalized_limit as i64, offset as i64], |row| {
            Ok(BackupTaskRow {
                id: row.get(0)?,
                config_id: row.get(1)?,
                status: row.get(2)?,
                sub_phase: row.get(3)?,
                trigger_type: row.get(4)?,
                completed_count: row.get(5)?,
                failed_count: row.get(6)?,
                skipped_count: row.get(7)?,
                total_count: row.get(8)?,
                transferred_bytes: row.get(9)?,
                total_bytes: row.get(10)?,
                error_message: row.get(11)?,
                created_at: row.get(12)?,
                started_at: row.get(13)?,
                completed_at: row.get(14)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_task(r) {
                    Ok(task) => tasks.push(task),
                    Err(e) => tracing::warn!("转换任务失败: {}", e),
                },
                Err(e) => tracing::warn!("读取任务行失败: {}", e),
            }
        }

        Ok(tasks)
    }

    /// 按配置查询最近文件任务
    /// 用于历史文件查询
    /// 
    /// 分页参数会被规范化：
    /// - limit 为 0 时使用默认值 100
    /// - limit 超过 500 时截断为 500
    pub fn load_file_tasks_by_config(&self, config_id: &str, limit: usize) -> Result<Vec<BackupFileTask>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 规范化分页参数
        let normalized_limit = normalize_pagination(limit);

        let mut stmt = conn.prepare(
            r#"
            SELECT id, backup_task_id, config_id, relative_path, file_name,
                   local_path, remote_path, file_size, head_md5, fs_id,
                   status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                   transferred_bytes, error_message, retry_count,
                   related_task_id, backup_operation_type,
                   created_at, updated_at
            FROM backup_file_tasks
            WHERE config_id = ?1
            ORDER BY created_at DESC
            LIMIT ?2
            "#,
        )?;

        let rows = stmt.query_map(params![config_id, normalized_limit as i64], |row| {
            Ok(BackupFileTaskRow {
                id: row.get(0)?,
                backup_task_id: row.get(1)?,
                config_id: row.get(2)?,
                relative_path: row.get(3)?,
                file_name: row.get(4)?,
                local_path: row.get(5)?,
                remote_path: row.get(6)?,
                file_size: row.get(7)?,
                head_md5: row.get(8)?,
                fs_id: row.get(9)?,
                status: row.get(10)?,
                sub_phase: row.get(11)?,
                skip_reason: row.get(12)?,
                encrypted: row.get(13)?,
                encrypted_name: row.get(14)?,
                temp_encrypted_path: row.get(15)?,
                transferred_bytes: row.get(16)?,
                error_message: row.get(17)?,
                retry_count: row.get(18)?,
                related_task_id: row.get(19)?,
                backup_operation_type: row.get(20)?,
                created_at: row.get(21)?,
                updated_at: row.get(22)?,
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            match row {
                Ok(r) => match self.row_to_file_task(r) {
                    Ok(task) => tasks.push(task),
                    Err(e) => tracing::warn!("转换文件任务失败: {}", e),
                },
                Err(e) => tracing::warn!("读取文件任务行失败: {}", e),
            }
        }

        Ok(tasks)
    }

    /// 按配置统计任务数量
    pub fn count_tasks_by_config(&self, config_id: &str) -> Result<usize> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM backup_tasks WHERE config_id = ?1",
            params![config_id],
            |row| row.get(0),
        )?;

        Ok(count as usize)
    }

    // ==================== 服务重启兜底同步方法 ====================

    /// 根据关联的上传/下载任务ID查找备份文件任务
    ///
    /// 用于服务重启时，根据已归档的上传/下载任务ID找到对应的备份文件任务
    pub fn find_file_task_by_related_task_id(&self, related_task_id: &str) -> Result<Option<(String, String)>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let result = conn
            .query_row(
                "SELECT id, backup_task_id FROM backup_file_tasks WHERE related_task_id = ?1",
                params![related_task_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;

        Ok(result)
    }

    /// 根据关联任务ID更新备份文件任务状态为已完成
    ///
    /// 用于服务重启时的兜底同步：当上传/下载任务已归档到历史表时，
    /// 同步更新对应的备份文件任务状态
    pub fn complete_file_task_by_related_task_id(
        &self,
        related_task_id: &str,
        transferred_bytes: u64,
    ) -> Result<Option<String>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let now = chrono::Utc::now().timestamp();

        // 先查找对应的文件任务和主任务ID
        let task_info: Option<(String, String)> = conn
            .query_row(
                "SELECT id, backup_task_id FROM backup_file_tasks WHERE related_task_id = ?1",
                params![related_task_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;

        if let Some((file_task_id, backup_task_id)) = task_info {
            // 更新文件任务状态为已完成
            conn.execute(
                r#"
                UPDATE backup_file_tasks
                SET status = 'completed',
                    transferred_bytes = ?1,
                    updated_at = ?2
                WHERE id = ?3
                "#,
                params![transferred_bytes as i64, now, file_task_id],
            )?;

            tracing::info!(
                "兜底同步: 已更新备份文件任务状态为完成, file_task_id={}, related_task_id={}",
                file_task_id,
                related_task_id
            );

            Ok(Some(backup_task_id))
        } else {
            Ok(None)
        }
    }

    /// 批量根据关联任务ID更新备份文件任务状态
    ///
    /// 用于服务重启时批量同步已完成的上传/下载任务到备份文件任务
    pub fn complete_file_tasks_by_related_task_ids(
        &self,
        task_completions: &[(String, u64)], // (related_task_id, transferred_bytes)
    ) -> Result<Vec<String>> {
        if task_completions.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;
        let tx = conn.transaction()?;

        let now = chrono::Utc::now().timestamp();
        let mut affected_backup_task_ids = Vec::new();

        for (related_task_id, transferred_bytes) in task_completions {
            // 查找对应的文件任务
            let task_info: Option<(String, String)> = tx
                .query_row(
                    "SELECT id, backup_task_id FROM backup_file_tasks WHERE related_task_id = ?1",
                    params![related_task_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;

            if let Some((file_task_id, backup_task_id)) = task_info {
                // 更新文件任务状态
                tx.execute(
                    r#"
                    UPDATE backup_file_tasks
                    SET status = 'completed',
                        transferred_bytes = ?1,
                        updated_at = ?2
                    WHERE id = ?3
                    "#,
                    params![*transferred_bytes as i64, now, file_task_id],
                )?;

                if !affected_backup_task_ids.contains(&backup_task_id) {
                    affected_backup_task_ids.push(backup_task_id);
                }

                tracing::debug!(
                    "兜底同步: 更新备份文件任务, file_task_id={}, related_task_id={}",
                    file_task_id,
                    related_task_id
                );
            }
        }

        tx.commit()?;

        if !affected_backup_task_ids.is_empty() {
            tracing::info!(
                "兜底同步: 批量更新了 {} 个备份文件任务，涉及 {} 个主任务",
                task_completions.len(),
                affected_backup_task_ids.len()
            );
        }

        Ok(affected_backup_task_ids)
    }

    /// 重新计算并更新主任务的进度统计
    ///
    /// 根据文件任务的实际状态重新计算主任务的 completed_count, failed_count 等
    /// 同时计算 total_bytes（排除 skipped）和 transferred_bytes（已完成文件用 file_size，其他用 transferred_bytes）
    pub fn recalculate_task_progress(&self, backup_task_id: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 统计各状态的文件数量和字节数
        // total_bytes: 排除 skipped 状态的文件大小总和
        // transferred_bytes: 已完成文件用 file_size，其他用 transferred_bytes
        let stats: (i64, i64, i64, i64, i64, i64) = conn.query_row(
            r#"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped,
                COALESCE(SUM(CASE WHEN status != 'skipped' THEN file_size ELSE 0 END), 0) as total_bytes,
                COALESCE(SUM(
                    CASE 
                        WHEN status = 'completed' THEN file_size
                        WHEN status != 'skipped' THEN transferred_bytes
                        ELSE 0
                    END
                ), 0) as transferred_bytes
            FROM backup_file_tasks
            WHERE backup_task_id = ?1
            "#,
            params![backup_task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )?;

        let (total, completed, failed, skipped, total_bytes, transferred_bytes) = stats;

        // 更新主任务进度（同时更新 total_bytes 字段）
        conn.execute(
            r#"
            UPDATE backup_tasks
            SET completed_count = ?1,
                failed_count = ?2,
                skipped_count = ?3,
                total_count = ?4,
                total_bytes = ?5,
                transferred_bytes = ?6
            WHERE id = ?7
            "#,
            params![completed, failed, skipped, total, total_bytes, transferred_bytes, backup_task_id],
        )?;

        // 检查是否所有文件都已处理完成（终态）
        let pending_count: i64 = conn.query_row(
            r#"
            SELECT COUNT(*) FROM backup_file_tasks
            WHERE backup_task_id = ?1
              AND status NOT IN ('completed', 'failed', 'skipped', 'cancelled')
            "#,
            params![backup_task_id],
            |row| row.get(0),
        )?;

        // 如果没有待处理的文件，更新主任务状态
        if pending_count == 0 && total > 0 {
            let new_status = if failed > 0 && completed > 0 {
                "partiallycompleted"
            } else if failed > 0 {
                "failed"
            } else {
                "completed"
            };

            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "UPDATE backup_tasks SET status = ?1, completed_at = ?2 WHERE id = ?3",
                params![new_status, now, backup_task_id],
            )?;

            tracing::info!(
                "兜底同步: 主任务状态更新为 {}, backup_task_id={}, completed={}, failed={}, skipped={}",
                new_status,
                backup_task_id,
                completed,
                failed,
                skipped
            );
        }

        Ok(())
    }

    // ==================== 扫描与传输阶段优化方法 ====================

    /// 获取任务的所有文件本地路径（用于增量对比）
    ///
    /// 返回当前任务中所有文件的本地路径集合（包括已完成、失败、跳过的）
    /// 用于增量合并时判断文件是否已在当前任务中
    pub fn get_task_file_local_paths(&self, task_id: &str) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            "SELECT local_path FROM backup_file_tasks WHERE backup_task_id = ?1"
        )?;

        let rows = stmt.query_map(params![task_id], |row| {
            row.get::<_, String>(0)
        })?;

        let mut paths = std::collections::HashSet::new();
        for row in rows {
            if let Ok(path) = row {
                paths.insert(path);
            }
        }

        Ok(paths)
    }

    /// 计算任务的总字节数（排除 skipped 状态）
    ///
    /// 用于增量合并新文件后重新计算 total_bytes
    pub fn calculate_total_bytes_by_task(&self, task_id: &str) -> Result<u64> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let total_bytes: i64 = conn.query_row(
            r#"
            SELECT COALESCE(SUM(file_size), 0)
            FROM backup_file_tasks
            WHERE backup_task_id = ?1 AND status != 'skipped'
            "#,
            params![task_id],
            |row| row.get(0),
        )?;

        Ok(total_bytes as u64)
    }

    /// 计算任务的已传输字节数
    ///
    /// 已完成文件用 file_size，其他用 transferred_bytes
    /// 用于从数据库重新计算 transferred_bytes，确保包含所有文件
    pub fn calculate_transferred_bytes_by_task(&self, task_id: &str) -> Result<u64> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let transferred_bytes: i64 = conn.query_row(
            r#"
            SELECT COALESCE(SUM(
                CASE 
                    WHEN status = 'completed' THEN file_size
                    WHEN status != 'skipped' THEN transferred_bytes
                    ELSE 0
                END
            ), 0)
            FROM backup_file_tasks
            WHERE backup_task_id = ?1
            "#,
            params![task_id],
            |row| row.get(0),
        )?;

        Ok(transferred_bytes as u64)
    }

    /// 统计任务的文件数量
    ///
    /// exclude_skipped: 是否排除 skipped 状态的文件
    pub fn count_files_by_task(&self, task_id: &str, exclude_skipped: bool) -> Result<usize> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let count: i64 = if exclude_skipped {
            conn.query_row(
                "SELECT COUNT(*) FROM backup_file_tasks WHERE backup_task_id = ?1 AND status != 'skipped'",
                params![task_id],
                |row| row.get(0),
            )?
        } else {
            conn.query_row(
                "SELECT COUNT(*) FROM backup_file_tasks WHERE backup_task_id = ?1",
                params![task_id],
                |row| row.get(0),
            )?
        };

        Ok(count as usize)
    }

    /// 更新任务的 total_bytes 和 transferred_bytes
    ///
    /// 用于增量合并新文件后更新任务统计
    pub fn update_task_bytes(&self, task_id: &str, total_bytes: u64, transferred_bytes: u64) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        conn.execute(
            "UPDATE backup_tasks SET total_bytes = ?1, transferred_bytes = ?2 WHERE id = ?3",
            params![total_bytes as i64, transferred_bytes as i64, task_id],
        )?;

        Ok(())
    }

    /// 更新任务的 total_count
    ///
    /// 用于增量合并新文件后更新任务统计
    pub fn update_task_total_count(&self, task_id: &str, total_count: usize) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        conn.execute(
            "UPDATE backup_tasks SET total_count = ?1 WHERE id = ?2",
            params![total_count as i64, task_id],
        )?;

        Ok(())
    }

    /// 获取任务的所有文件远程路径（用于下载备份增量对比）
    ///
    /// 返回当前任务中所有文件的远程路径集合，用于判断新文件是否已在任务中
    pub fn get_task_remote_paths(&self, task_id: &str) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            "SELECT remote_path FROM backup_file_tasks WHERE backup_task_id = ?1"
        )?;

        let paths = stmt.query_map(params![task_id], |row| {
            row.get::<_, String>(0)
        })?;

        let mut result = std::collections::HashSet::new();
        for path in paths {
            if let Ok(p) = path {
                result.insert(p);
            }
        }

        Ok(result)
    }

    // ==================== 辅助方法 ====================

    /// 将数据库行转换为 BackupTask
    fn row_to_task(&self, row: BackupTaskRow) -> Result<BackupTask> {
        let status = parse_task_status(&row.status)?;
        let sub_phase = row.sub_phase.as_ref().map(|s| parse_sub_phase(s)).transpose()?;
        let trigger_type = parse_trigger_type(&row.trigger_type)?;

        Ok(BackupTask {
            id: row.id,
            config_id: row.config_id,
            status,
            sub_phase,
            trigger_type,
            pending_files: Vec::new(), // 子任务单独加载
            completed_count: row.completed_count as usize,
            failed_count: row.failed_count as usize,
            skipped_count: row.skipped_count as usize,
            total_count: row.total_count as usize,
            transferred_bytes: row.transferred_bytes as u64,
            total_bytes: row.total_bytes as u64,
            scan_progress: None,
            created_at: chrono::DateTime::from_timestamp(row.created_at, 0)
                .unwrap_or_else(chrono::Utc::now),
            started_at: row.started_at.and_then(|t| chrono::DateTime::from_timestamp(t, 0)),
            completed_at: row.completed_at.and_then(|t| chrono::DateTime::from_timestamp(t, 0)),
            error_message: row.error_message,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        })
    }

    /// 将数据库行转换为 BackupFileTask
    fn row_to_file_task(&self, row: BackupFileTaskRow) -> Result<BackupFileTask> {
        let status = parse_file_status(&row.status)?;
        let sub_phase = row.sub_phase.as_ref().map(|s| parse_sub_phase(s)).transpose()?;
        let skip_reason: Option<SkipReason> = row
            .skip_reason
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok());
        let backup_operation_type = row
            .backup_operation_type
            .as_ref()
            .map(|s| parse_backup_operation_type(s))
            .transpose()?;

        Ok(BackupFileTask {
            id: row.id,
            parent_task_id: row.backup_task_id,
            local_path: std::path::PathBuf::from(row.local_path),
            remote_path: row.remote_path,
            file_size: row.file_size as u64,
            head_md5: if row.head_md5.is_empty() { None } else { Some(row.head_md5) },
            fs_id: row.fs_id.map(|id| id as u64),
            status,
            sub_phase,
            skip_reason,
            encrypted: row.encrypted,
            encrypted_name: row.encrypted_name,
            temp_encrypted_path: row.temp_encrypted_path.map(std::path::PathBuf::from),
            transferred_bytes: row.transferred_bytes as u64,
            decrypt_progress: None,
            error_message: row.error_message,
            retry_count: row.retry_count as u32,
            related_task_id: row.related_task_id,
            backup_operation_type,
            created_at: chrono::DateTime::from_timestamp(row.created_at, 0)
                .unwrap_or_else(chrono::Utc::now),
            updated_at: chrono::DateTime::from_timestamp(row.updated_at, 0)
                .unwrap_or_else(chrono::Utc::now),
        })
    }
}

// ==================== 辅助结构体 ====================

/// 文件任务统计信息（用于内存优化，避免加载全部文件）
#[derive(Debug, Clone, Default)]
pub struct FileTaskStats {
    /// 待处理数量
    pub pending_count: usize,
    /// 待处理字节数
    pub pending_bytes: u64,
    /// 检查中数量
    pub checking_count: usize,
    /// 已跳过数量
    pub skipped_count: usize,
    /// 加密中数量
    pub encrypting_count: usize,
    /// 等待传输数量
    pub waiting_transfer_count: usize,
    /// 传输中数量
    pub transferring_count: usize,
    /// 已完成数量
    pub completed_count: usize,
    /// 已完成字节数
    pub completed_bytes: u64,
    /// 失败数量
    pub failed_count: usize,
}

impl FileTaskStats {
    /// 获取总数量
    pub fn total(&self) -> usize {
        self.pending_count
            + self.checking_count
            + self.skipped_count
            + self.encrypting_count
            + self.waiting_transfer_count
            + self.transferring_count
            + self.completed_count
            + self.failed_count
    }

    /// 是否全部完成
    pub fn is_all_done(&self) -> bool {
        self.pending_count == 0
            && self.checking_count == 0
            && self.encrypting_count == 0
            && self.waiting_transfer_count == 0
            && self.transferring_count == 0
    }
}

/// 数据库行结构（主任务）
struct BackupTaskRow {
    id: String,
    config_id: String,
    status: String,
    sub_phase: Option<String>,
    trigger_type: String,
    completed_count: i64,
    failed_count: i64,
    skipped_count: i64,
    total_count: i64,
    transferred_bytes: i64,
    total_bytes: i64,
    error_message: Option<String>,
    created_at: i64,
    started_at: Option<i64>,
    completed_at: Option<i64>,
}

/// 数据库行结构（文件任务）
/// 注意：部分字段（config_id, relative_path, file_name）用于数据库存储，
/// 但在转换为 BackupFileTask 时不直接使用（信息已包含在 local_path 中）
#[allow(dead_code)]
struct BackupFileTaskRow {
    id: String,
    backup_task_id: String,
    config_id: String,
    relative_path: String,
    file_name: String,
    local_path: String,
    remote_path: String,
    file_size: i64,
    head_md5: String,
    fs_id: Option<i64>,
    status: String,
    sub_phase: Option<String>,
    skip_reason: Option<String>,
    encrypted: bool,
    encrypted_name: Option<String>,
    temp_encrypted_path: Option<String>,
    transferred_bytes: i64,
    error_message: Option<String>,
    retry_count: i64,
    related_task_id: Option<String>,
    backup_operation_type: Option<String>,
    created_at: i64,
    updated_at: i64,
}

// ==================== 解析函数 ====================

fn parse_task_status(s: &str) -> Result<BackupTaskStatus> {
    match s.to_lowercase().as_str() {
        "queued" => Ok(BackupTaskStatus::Queued),
        "preparing" => Ok(BackupTaskStatus::Preparing),
        "transferring" => Ok(BackupTaskStatus::Transferring),
        "completed" => Ok(BackupTaskStatus::Completed),
        "partiallycompleted" => Ok(BackupTaskStatus::PartiallyCompleted),
        "cancelled" => Ok(BackupTaskStatus::Cancelled),
        "failed" => Ok(BackupTaskStatus::Failed),
        "paused" => Ok(BackupTaskStatus::Paused),
        _ => Err(anyhow!("未知的任务状态: {}", s)),
    }
}

fn parse_sub_phase(s: &str) -> Result<BackupSubPhase> {
    match s.to_lowercase().as_str() {
        "dedupchecking" => Ok(BackupSubPhase::DedupChecking),
        "waitingslot" => Ok(BackupSubPhase::WaitingSlot),
        "encrypting" => Ok(BackupSubPhase::Encrypting),
        "uploading" => Ok(BackupSubPhase::Uploading),
        "downloading" => Ok(BackupSubPhase::Downloading),
        "decrypting" => Ok(BackupSubPhase::Decrypting),
        "preempted" => Ok(BackupSubPhase::Preempted),
        _ => Err(anyhow!("未知的子阶段: {}", s)),
    }
}

fn parse_trigger_type(s: &str) -> Result<TriggerType> {
    match s.to_lowercase().as_str() {
        "watch" => Ok(TriggerType::Watch),
        "poll" => Ok(TriggerType::Poll),
        "manual" => Ok(TriggerType::Manual),
        _ => Err(anyhow!("未知的触发类型: {}", s)),
    }
}

fn parse_file_status(s: &str) -> Result<BackupFileStatus> {
    match s.to_lowercase().as_str() {
        "pending" => Ok(BackupFileStatus::Pending),
        "checking" => Ok(BackupFileStatus::Checking),
        "skipped" => Ok(BackupFileStatus::Skipped),
        "encrypting" => Ok(BackupFileStatus::Encrypting),
        "waitingtransfer" => Ok(BackupFileStatus::WaitingTransfer),
        "transferring" => Ok(BackupFileStatus::Transferring),
        "completed" => Ok(BackupFileStatus::Completed),
        "failed" => Ok(BackupFileStatus::Failed),
        _ => Err(anyhow!("未知的文件状态: {}", s)),
    }
}

fn parse_backup_operation_type(s: &str) -> Result<BackupOperationType> {
    match s.to_lowercase().as_str() {
        "upload" => Ok(BackupOperationType::Upload),
        "download" => Ok(BackupOperationType::Download),
        _ => Err(anyhow!("未知的备份操作类型: {}", s)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_manager() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let manager = BackupPersistenceManager::new(&db_path).unwrap();
        assert!(db_path.exists());
        drop(manager);
    }

    #[test]
    fn test_save_and_load_task() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let manager = BackupPersistenceManager::new(&db_path).unwrap();

        let task = BackupTask {
            id: "test-task-1".to_string(),
            config_id: "config-1".to_string(),
            status: BackupTaskStatus::Queued,
            sub_phase: None,
            trigger_type: TriggerType::Manual,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 10,
            transferred_bytes: 0,
            total_bytes: 1000,
            scan_progress: None,
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };

        manager.save_task(&task).unwrap();

        let loaded = manager.load_task("test-task-1").unwrap().unwrap();
        assert_eq!(loaded.id, task.id);
        assert_eq!(loaded.config_id, task.config_id);
        assert_eq!(loaded.total_count, task.total_count);
    }
}
