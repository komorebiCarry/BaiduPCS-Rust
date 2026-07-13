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
        // 启用 WAL 模式，允许读写并发
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;"
        )?;

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
        // 复合索引：加速 WHERE config_id = ? ORDER BY created_at DESC 查询
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backup_tasks_config_time ON backup_tasks(config_id, created_at DESC)",
            [],
        )?;

        // 迁移旧数据库时先收敛历史竞态产生的重复活跃任务，再建立最终的
        // 部分唯一索引。保留最早创建的任务，其余任务标记为失败而不是删除，
        // 这样历史仍可审计，同时从此以后数据库层面不可能再出现同一配置的
        // Queued/Preparing/Transferring/Paused 双任务。
        let duplicate_count = conn.execute(
            r#"
            UPDATE backup_tasks
            SET status = 'failed',
                error_message = COALESCE(error_message, '启动时清理重复活跃备份任务'),
                completed_at = COALESCE(completed_at, ?1)
            WHERE id IN (
                SELECT duplicate.id
                FROM backup_tasks AS duplicate
                WHERE duplicate.status IN ('queued', 'preparing', 'transferring', 'paused')
                  AND EXISTS (
                      SELECT 1
                      FROM backup_tasks AS winner
                      WHERE winner.config_id = duplicate.config_id
                        AND winner.status IN ('queued', 'preparing', 'transferring', 'paused')
                        AND (
                            winner.created_at < duplicate.created_at
                            OR (
                                winner.created_at = duplicate.created_at
                                AND winner.id < duplicate.id
                            )
                        )
                  )
            )
            "#,
            params![chrono::Utc::now().timestamp()],
        )?;
        if duplicate_count > 0 {
            tracing::warn!(
                "备份任务数据库启动迁移：已将 {} 个重复活跃任务标记为失败",
                duplicate_count
            );
        }

        conn.execute(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_tasks_one_active_per_config
            ON backup_tasks(config_id)
            WHERE status IN ('queued', 'preparing', 'transferring', 'paused')
            "#,
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
                sync_remote_mtime INTEGER,              -- Sync 计划阶段保存的远端 mtime（秒级时间戳）
                sync_remote_size INTEGER,               -- Sync 计划阶段保存的远端 size（字节）
                sync_remote_fs_id INTEGER,              -- Sync 计划阶段保存的远端 fs_id
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

        // 新模型下 `folder:<config_id>` 是配置级稳定父记录，文件清单不能因为
        // 手动/监听/轮询触发而重复插入。同一文件的远端位置在一个文件夹内唯一；
        // 旧版本按每次主任务保存的历史行不参与这个约束，避免破坏已有历史数据。
        conn.execute(
            r#"
            DELETE FROM backup_file_tasks
            WHERE backup_task_id LIKE 'folder:%'
              AND rowid NOT IN (
                  SELECT MAX(rowid)
                  FROM backup_file_tasks
                  WHERE backup_task_id LIKE 'folder:%'
                  GROUP BY config_id, remote_path
              )
            "#,
            [],
        )?;
        conn.execute(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_folder_file_manifest_unique
            ON backup_file_tasks(config_id, remote_path)
            WHERE backup_task_id LIKE 'folder:%'
            "#,
            [],
        )?;

        // 迁移：为已有数据库添加 sync_remote_* 列（ALTER TABLE ADD COLUMN 如果列已存在会报错，忽略即可）
        for col in &[
            "sync_remote_mtime INTEGER",
            "sync_remote_size INTEGER",
            "sync_remote_fs_id INTEGER",
        ] {
            let sql = format!("ALTER TABLE backup_file_tasks ADD COLUMN {}", col);
            if let Err(e) = conn.execute(&sql, []) {
                // "duplicate column name" 说明列已存在，可以安全忽略
                let msg = e.to_string();
                if !msg.contains("duplicate column") {
                    tracing::warn!("迁移 backup_file_tasks 添加列失败: {}", msg);
                }
            }
        }

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
            INSERT INTO backup_tasks (
                id, config_id, status, sub_phase, trigger_type,
                completed_count, failed_count, skipped_count, total_count,
                transferred_bytes, total_bytes, error_message,
                created_at, started_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(id) DO UPDATE SET
                config_id = excluded.config_id,
                status = excluded.status,
                sub_phase = excluded.sub_phase,
                trigger_type = excluded.trigger_type,
                completed_count = excluded.completed_count,
                failed_count = excluded.failed_count,
                skipped_count = excluded.skipped_count,
                total_count = excluded.total_count,
                transferred_bytes = excluded.transferred_bytes,
                total_bytes = excluded.total_bytes,
                error_message = excluded.error_message,
                created_at = excluded.created_at,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at
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

    /// 查找指定配置当前唯一的活跃主任务。
    ///
    /// 该查询与 `idx_backup_tasks_one_active_per_config` 配套使用：查询用于
    /// 友好地返回已有任务 ID，唯一索引负责处理跨管理器/跨请求的最终竞态。
    pub fn find_active_task_id(&self, config_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        conn.query_row(
            r#"
            SELECT id
            FROM backup_tasks
            WHERE config_id = ?1
              AND status IN ('queued', 'preparing', 'transferring', 'paused')
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            "#,
            params![config_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
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
            WHERE status NOT IN ('completed', 'partiallycompleted', 'partially_completed', 'cancelled', 'failed')
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

    /// 丢弃旧版按触发创建的主任务及其文件行。
    ///
    /// 稳定文件夹模型使用 `folder:<config_id>` 作为唯一主记录，因此这里只
    /// 删除同一配置下所有非稳定记录，不尝试迁移或恢复旧状态机的数据。
    pub fn discard_legacy_tasks_for_config(&self, config_id: &str) -> Result<usize> {
        let mut conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;
        let tx = conn.transaction()?;

        let task_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM backup_tasks WHERE config_id = ?1 AND id NOT LIKE 'folder:%'",
            params![config_id],
            |row| row.get(0),
        )?;

        tx.execute(
            r#"
            DELETE FROM backup_file_tasks
            WHERE backup_task_id IN (
                SELECT id
                FROM backup_tasks
                WHERE config_id = ?1
                  AND id NOT LIKE 'folder:%'
            )
               OR (config_id = ?1 AND backup_task_id NOT LIKE 'folder:%')
            "#,
            params![config_id],
        )?;
        tx.execute(
            "DELETE FROM backup_tasks WHERE config_id = ?1 AND id NOT LIKE 'folder:%'",
            params![config_id],
        )?;

        tx.commit()?;
        Ok(task_count as usize)
    }

    /// 清除稳定上传文件夹在进程重启前留下的执行上下文。
    ///
    /// 稳定文件清单是事实来源，重启后必须重新确认本地/远端状态，不能把旧
    /// upload_task_id 当作当前传输窗口继续恢复。终态行仍保留用于历史展示；
    /// 只有非终态且带有旧子任务引用，或处于执行中的行，才被重置为 pending。
    pub fn reset_folder_upload_execution_state(&self, folder_task_id: &str) -> Result<usize> {
        let mut conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp();

        let reset_count = tx.execute(
            r#"
            UPDATE backup_file_tasks
            SET status = 'pending',
                sub_phase = NULL,
                skip_reason = NULL,
                temp_encrypted_path = NULL,
                transferred_bytes = 0,
                error_message = NULL,
                related_task_id = NULL,
                updated_at = ?1
            WHERE backup_task_id = ?2
              AND status NOT IN ('completed', 'failed', 'skipped', 'cancelled')
              AND (
                    related_task_id IS NOT NULL
                    OR status IN ('checking', 'encrypting', 'waitingtransfer', 'transferring')
              )
            "#,
            params![now, folder_task_id],
        )?;

        tx.commit()?;
        Ok(reset_count)
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
                sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
                created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)
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
                file_task.sync_remote_mtime,
                file_task.sync_remote_size.map(|s| s as i64),
                file_task.sync_remote_fs_id.map(|id| id as i64),
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
                    sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)
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
                    file_task.sync_remote_mtime,
                    file_task.sync_remote_size.map(|s| s as i64),
                    file_task.sync_remote_fs_id.map(|id| id as i64),
                    file_task.created_at.timestamp(),
                    file_task.updated_at.timestamp(),
                ])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// 原子维护一个配置对应的稳定文件清单。
    ///
    /// 与旧的“每次主任务插入一批文件”路径不同，这里按
    /// `(config_id, remote_path)` 更新同一行。正在上传的行不会被扫描覆盖，
    /// 等上传完成后下一次扫描会重新比较本地文件和远端状态，避免旧上传的
    /// 完成通知把新版本文件错误地标记为已完成。
    pub fn upsert_folder_file_tasks(
        &self,
        file_tasks: &[BackupFileTask],
        config_id: &str,
        folder_task_id: &str,
    ) -> Result<()> {
        if file_tasks.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;
        let tx = conn.transaction()?;

        for file_task in file_tasks {
            let status = format!("{:?}", file_task.status).to_lowercase();
            let sub_phase = file_task.sub_phase.map(|p| format!("{:?}", p).to_lowercase());
            let skip_reason = file_task
                .skip_reason
                .as_ref()
                .map(|r| serde_json::to_string(r).unwrap_or_default());
            let file_name = file_task
                .local_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();
            let now = chrono::Utc::now().timestamp();

            let existing: Option<(String, String, Option<String>)> = tx
                .query_row(
                    r#"
                    SELECT id, status, related_task_id
                    FROM backup_file_tasks
                    WHERE config_id = ?1
                      AND remote_path = ?2
                      AND backup_task_id LIKE 'folder:%'
                    LIMIT 1
                    "#,
                    params![config_id, file_task.remote_path],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;

            if let Some((existing_id, existing_status, existing_related_task_id)) = existing {
                let active = matches!(
                    existing_status.as_str(),
                    "checking" | "encrypting" | "waitingtransfer" | "transferring"
                );

                if active {
                    // 不改写正在执行的文件快照；否则完成通知可能结算到新版本。
                    tx.execute(
                        "UPDATE backup_file_tasks SET updated_at = ?1 WHERE id = ?2",
                        params![now, existing_id],
                    )?;
                    continue;
                }

                tx.execute(
                    r#"
                    UPDATE backup_file_tasks SET
                        backup_task_id = ?1,
                        relative_path = ?2,
                        file_name = ?3,
                        local_path = ?4,
                        file_size = ?5,
                        head_md5 = ?6,
                        fs_id = ?7,
                        status = ?8,
                        sub_phase = ?9,
                        skip_reason = ?10,
                        encrypted = ?11,
                        encrypted_name = ?12,
                        temp_encrypted_path = ?13,
                        transferred_bytes = ?14,
                        error_message = ?15,
                        retry_count = ?16,
                        related_task_id = ?17,
                        backup_operation_type = ?18,
                        sync_remote_mtime = ?19,
                        sync_remote_size = ?20,
                        sync_remote_fs_id = ?21,
                        updated_at = ?22
                    WHERE id = ?23
                    "#,
                    params![
                        folder_task_id,
                        "",
                        file_name,
                        file_task.local_path.to_string_lossy().to_string(),
                        file_task.file_size as i64,
                        file_task.head_md5.as_deref().unwrap_or(""),
                        file_task.fs_id.map(|id| id as i64),
                        status,
                        sub_phase,
                        skip_reason,
                        file_task.encrypted,
                        file_task.encrypted_name,
                        file_task.temp_encrypted_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                        file_task.transferred_bytes as i64,
                        file_task.error_message,
                        file_task.retry_count as i64,
                        file_task.related_task_id.clone().or(existing_related_task_id),
                        file_task.backup_operation_type.map(|t| format!("{:?}", t).to_lowercase()),
                        file_task.sync_remote_mtime,
                        file_task.sync_remote_size.map(|s| s as i64),
                        file_task.sync_remote_fs_id.map(|id| id as i64),
                        now,
                        existing_id,
                    ],
                )?;
            } else {
                tx.execute(
                    r#"
                    INSERT INTO backup_file_tasks (
                        id, backup_task_id, config_id, relative_path, file_name,
                        local_path, remote_path, file_size, head_md5, fs_id,
                        status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                        transferred_bytes, error_message, retry_count,
                        related_task_id, backup_operation_type,
                        sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
                        created_at, updated_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                              ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
                              ?21, ?22, ?23, ?24, ?25, ?26)
                    "#,
                    params![
                        file_task.id,
                        folder_task_id,
                        config_id,
                        "",
                        file_name,
                        file_task.local_path.to_string_lossy().to_string(),
                        file_task.remote_path,
                        file_task.file_size as i64,
                        file_task.head_md5.as_deref().unwrap_or(""),
                        file_task.fs_id.map(|id| id as i64),
                        status,
                        sub_phase,
                        skip_reason,
                        file_task.encrypted,
                        file_task.encrypted_name,
                        file_task.temp_encrypted_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                        file_task.transferred_bytes as i64,
                        file_task.error_message,
                        file_task.retry_count as i64,
                        file_task.related_task_id,
                        file_task.backup_operation_type.map(|t| format!("{:?}", t).to_lowercase()),
                        file_task.sync_remote_mtime,
                        file_task.sync_remote_size.map(|s| s as i64),
                        file_task.sync_remote_fs_id.map(|id| id as i64),
                        file_task.created_at.timestamp(),
                        now,
                    ],
                )?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// 加载配置级稳定文件清单。
    pub fn load_folder_file_tasks(&self, folder_task_id: &str) -> Result<Vec<BackupFileTask>> {
        let (tasks, _) = self.load_file_tasks(folder_task_id, 1, 10_000)?;
        Ok(tasks)
    }

    /// 将本轮扫描中已经不存在的本地文件标为 skipped，但不删除远端文件。
    pub fn mark_folder_files_not_seen(
        &self,
        folder_task_id: &str,
        seen_remote_paths: &std::collections::HashSet<String>,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;
        let now = chrono::Utc::now().timestamp();

        if seen_remote_paths.is_empty() {
            conn.execute(
                r#"
                UPDATE backup_file_tasks
                SET status = 'skipped',
                    error_message = '本地文件已不存在',
                    updated_at = ?1
                WHERE backup_task_id = ?2
                  AND status NOT IN ('checking', 'encrypting', 'waitingtransfer', 'transferring')
                "#,
                params![now, folder_task_id],
            )?;
            return Ok(());
        }

        let placeholders: Vec<String> = (0..seen_remote_paths.len())
            .map(|index| format!("?{}", index + 3))
            .collect();
        let sql = format!(
            r#"
            UPDATE backup_file_tasks
            SET status = 'skipped',
                error_message = '本地文件已不存在',
                updated_at = ?1
            WHERE backup_task_id = ?2
              AND status NOT IN ('checking', 'encrypting', 'waitingtransfer', 'transferring')
              AND remote_path NOT IN ({})
            "#,
            placeholders.join(", ")
        );

        let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(seen_remote_paths.len() + 2);
        values.push(Box::new(now));
        values.push(Box::new(folder_task_id.to_string()));
        for path in seen_remote_paths {
            values.push(Box::new(path.clone()));
        }
        let refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|value| value.as_ref()).collect();
        conn.execute(&sql, refs.as_slice())?;
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
        self.load_file_tasks_filtered(task_id, page, page_size, false)
    }

    /// 加载当前仍在传输窗口中的文件任务。
    ///
    /// 稳定文件夹任务的完整文件表必须保留 completed/skipped/failed 行供
    /// 历史查询；“当前传输文件”预览则只应读取非终态行，避免完成文件占住
    /// 预览窗口。
    pub fn load_active_file_tasks(
        &self,
        task_id: &str,
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<BackupFileTask>, usize)> {
        self.load_file_tasks_filtered(task_id, page, page_size, true)
    }

    fn load_file_tasks_filtered(
        &self,
        task_id: &str,
        page: usize,
        page_size: usize,
        active_only: bool,
    ) -> Result<(Vec<BackupFileTask>, usize)> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        // 规范化分页参数
        let normalized_page_size = normalize_pagination(page_size);
        let status_filter = if active_only {
            " AND status IN ('pending', 'checking', 'encrypting', 'waitingtransfer', 'transferring')"
        } else {
            ""
        };

        // 获取总数
        let count_sql = format!(
            "SELECT COUNT(*) FROM backup_file_tasks WHERE backup_task_id = ?1{}",
            status_filter
        );
        let total: usize = conn.query_row(
            &count_sql,
            params![task_id],
            |row| row.get(0),
        )?;

        // 分页查询
        let offset = (page.saturating_sub(1)) * normalized_page_size;
        let query = format!(
            r#"
            SELECT id, backup_task_id, config_id, relative_path, file_name,
                   local_path, remote_path, file_size, head_md5, fs_id,
                   status, sub_phase, skip_reason, encrypted, encrypted_name, temp_encrypted_path,
                   transferred_bytes, error_message, retry_count,
                   related_task_id, backup_operation_type,
                   sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
                   created_at, updated_at
            FROM backup_file_tasks
            WHERE backup_task_id = ?1{}
            ORDER BY {}created_at ASC
            LIMIT ?2 OFFSET ?3
            "#,
            status_filter,
            if active_only {
                "CASE status WHEN 'transferring' THEN 0 WHEN 'encrypting' THEN 1 "
                    .to_string()
                    + "WHEN 'checking' THEN 2 WHEN 'waitingtransfer' THEN 3 "
                    + "WHEN 'pending' THEN 4 ELSE 5 END, updated_at DESC, "
            } else {
                String::new()
            }
        );
        let mut stmt = conn.prepare(&query)?;

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
                sync_remote_mtime: row.get(21)?,
                sync_remote_size: row.get(22)?,
                sync_remote_fs_id: row.get(23)?,
                created_at: row.get(24)?,
                updated_at: row.get(25)?,
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
                   sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
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
                sync_remote_mtime: row.get(21)?,
                sync_remote_size: row.get(22)?,
                sync_remote_fs_id: row.get(23)?,
                created_at: row.get(24)?,
                updated_at: row.get(25)?,
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
                   sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
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
                sync_remote_mtime: row.get(21)?,
                sync_remote_size: row.get(22)?,
                sync_remote_fs_id: row.get(23)?,
                created_at: row.get(24)?,
                updated_at: row.get(25)?,
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
        for (status, count, size) in rows.flatten() {
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
                   sync_remote_mtime, sync_remote_size, sync_remote_fs_id,
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
                sync_remote_mtime: row.get(21)?,
                sync_remote_size: row.get(22)?,
                sync_remote_fs_id: row.get(23)?,
                created_at: row.get(24)?,
                updated_at: row.get(25)?,
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

    pub fn get_task_file_local_paths(&self, task_id: &str) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn.lock().map_err(|e| anyhow!("获取数据库锁失败: {}", e))?;

        let mut stmt = conn.prepare(
            "SELECT local_path FROM backup_file_tasks WHERE backup_task_id = ?1"
        )?;

        let rows = stmt.query_map(params![task_id], |row| {
            row.get::<_, String>(0)
        })?;

        let mut paths = std::collections::HashSet::new();
        for path in rows.flatten() {
            paths.insert(path);
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
        for p in paths.flatten() {
            result.insert(p);
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
            // DB schema 不存 owner_uid，由 manager 在装载完后批量从 config 补齐
            owner_uid: None,
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
            sync_remote_mtime: row.sync_remote_mtime,
            sync_remote_size: row.sync_remote_size.map(|s| s as u64),
            sync_remote_fs_id: row.sync_remote_fs_id.map(|id| id as u64),
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
    sync_remote_mtime: Option<i64>,
    sync_remote_size: Option<i64>,
    sync_remote_fs_id: Option<i64>,
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
        "syncscanning" => Ok(BackupSubPhase::SyncScanning),
        "syncplanning" => Ok(BackupSubPhase::SyncPlanning),
        "syncuploading" => Ok(BackupSubPhase::SyncUploading),
        "syncdownloading" => Ok(BackupSubPhase::SyncDownloading),
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
            owner_uid: None,
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

    #[test]
    fn test_only_one_active_task_per_config() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("unique_active.db");
        let manager = BackupPersistenceManager::new(&db_path).unwrap();
        let now = chrono::Utc::now();

        let make_task = |id: &str, status: BackupTaskStatus| BackupTask {
            owner_uid: None,
            id: id.to_string(),
            config_id: "same-config".to_string(),
            status,
            sub_phase: None,
            trigger_type: TriggerType::Manual,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 0,
            transferred_bytes: 0,
            total_bytes: 0,
            scan_progress: None,
            created_at: now,
            started_at: None,
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };

        let first = make_task("first", BackupTaskStatus::Queued);
        manager.save_task(&first).unwrap();
        assert_eq!(manager.find_active_task_id("same-config").unwrap(), Some("first".to_string()));

        let second = make_task("second", BackupTaskStatus::Preparing);
        assert!(manager.save_task(&second).is_err());

        let mut finished = first.clone();
        finished.status = BackupTaskStatus::Completed;
        finished.completed_at = Some(now);
        manager.save_task(&finished).unwrap();

        manager.save_task(&second).unwrap();
        assert_eq!(manager.find_active_task_id("same-config").unwrap(), Some("second".to_string()));
    }

    #[test]
    fn test_concurrent_active_task_inserts_have_one_winner() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("concurrent_unique_active.db");
        let manager = Arc::new(BackupPersistenceManager::new(&db_path).unwrap());
        let barrier = Arc::new(Barrier::new(8));

        let handles: Vec<_> = (0..8)
            .map(|index| {
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let now = chrono::Utc::now();
                    let task = BackupTask {
                        owner_uid: None,
                        id: format!("concurrent-{index}"),
                        config_id: "same-config".to_string(),
                        status: BackupTaskStatus::Preparing,
                        sub_phase: None,
                        trigger_type: TriggerType::Manual,
                        pending_files: Vec::new(),
                        completed_count: 0,
                        failed_count: 0,
                        skipped_count: 0,
                        total_count: 0,
                        transferred_bytes: 0,
                        total_bytes: 0,
                        scan_progress: None,
                        created_at: now,
                        started_at: Some(now),
                        completed_at: None,
                        error_message: None,
                        pending_upload_task_ids: std::collections::HashSet::new(),
                        pending_download_task_ids: std::collections::HashSet::new(),
                        transfer_task_map: std::collections::HashMap::new(),
                    };

                    barrier.wait();
                    manager.save_task(&task).is_ok()
                })
            })
            .collect();

        let winners = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|won| *won)
            .count();

        assert_eq!(winners, 1);
        assert!(manager.find_active_task_id("same-config").unwrap().is_some());
    }

    /// 构造一个用于测试的 BackupFileTask
    fn make_test_file_task(id: &str, parent_task_id: &str) -> BackupFileTask {
        BackupFileTask {
            id: id.to_string(),
            parent_task_id: parent_task_id.to_string(),
            local_path: std::path::PathBuf::from("/tmp/test/file.txt"),
            remote_path: "/remote/file.txt".to_string(),
            file_size: 12345,
            head_md5: Some("abc123".to_string()),
            fs_id: Some(99999),
            status: BackupFileStatus::Pending,
            sub_phase: None,
            skip_reason: None,
            encrypted: false,
            encrypted_name: None,
            temp_encrypted_path: None,
            transferred_bytes: 0,
            decrypt_progress: None,
            error_message: None,
            retry_count: 0,
            related_task_id: None,
            backup_operation_type: Some(BackupOperationType::Upload),
            sync_remote_mtime: Some(1700000000),
            sync_remote_size: Some(12345),
            sync_remote_fs_id: Some(88888),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_discard_legacy_tasks_keeps_folder_manifest() {
        let dir = tempdir().unwrap();
        let manager = BackupPersistenceManager::new(&dir.path().join("discard-legacy.db")).unwrap();
        let now = chrono::Utc::now();
        let make_task = |id: &str| BackupTask {
            owner_uid: None,
            id: id.to_string(),
            config_id: "manifest-config".to_string(),
            status: BackupTaskStatus::Completed,
            sub_phase: None,
            trigger_type: TriggerType::Poll,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 0,
            transferred_bytes: 0,
            total_bytes: 0,
            scan_progress: None,
            created_at: now,
            started_at: None,
            completed_at: Some(now),
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };

        manager.save_task(&make_task("legacy-task")).unwrap();
        manager.save_task(&make_task("folder:manifest-config")).unwrap();
        manager
            .save_file_task(&make_test_file_task("legacy-file", "legacy-task"), "manifest-config")
            .unwrap();
        manager
            .save_file_task(
                &make_test_file_task("manifest-file", "folder:manifest-config"),
                "manifest-config",
            )
            .unwrap();

        assert_eq!(
            manager
                .discard_legacy_tasks_for_config("manifest-config")
                .unwrap(),
            1
        );
        assert!(manager.load_task("legacy-task").unwrap().is_none());
        assert!(manager.load_task("folder:manifest-config").unwrap().is_some());
        assert!(manager.load_file_tasks("legacy-task", 100, 0).unwrap().0.is_empty());
        assert_eq!(manager.load_folder_file_tasks("folder:manifest-config").unwrap().len(), 1);
    }

    #[test]
    fn test_folder_manifest_upsert_is_idempotent_and_does_not_clobber_active_file() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("folder_manifest.db");
        let manager = BackupPersistenceManager::new(&db_path).unwrap();
        let now = chrono::Utc::now();
        let folder_task_id = "folder:cfg-folder";

        let parent = BackupTask {
            owner_uid: None,
            id: folder_task_id.to_string(),
            config_id: "cfg-folder".to_string(),
            status: BackupTaskStatus::Completed,
            sub_phase: None,
            trigger_type: TriggerType::Poll,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 0,
            transferred_bytes: 0,
            total_bytes: 0,
            scan_progress: None,
            created_at: now,
            started_at: None,
            completed_at: Some(now),
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };
        manager.save_task(&parent).unwrap();

        let first = make_test_file_task("manifest-first", folder_task_id);
        manager
            .upsert_folder_file_tasks(&[first.clone()], "cfg-folder", folder_task_id)
            .unwrap();

        // 同一远端位置的下一次扫描应更新同一行，而不是再插入一条文件任务。
        let mut refreshed = first.clone();
        refreshed.id = "manifest-second-generated-id".to_string();
        refreshed.file_size = 54321;
        refreshed.status = BackupFileStatus::Completed;
        manager
            .upsert_folder_file_tasks(&[refreshed], "cfg-folder", folder_task_id)
            .unwrap();

        let loaded = manager.load_folder_file_tasks(folder_task_id).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, first.id, "清单更新必须保留稳定文件行 ID");
        assert_eq!(loaded[0].file_size, 54321);
        assert_eq!(loaded[0].status, BackupFileStatus::Completed);

        // 扫描与上传完成通知交错时，新的扫描不能覆盖正在传输的快照。
        let mut active = loaded[0].clone();
        active.status = BackupFileStatus::Transferring;
        active.related_task_id = Some("upload-child".to_string());
        manager
            .upsert_folder_file_tasks(&[active.clone()], "cfg-folder", folder_task_id)
            .unwrap();

        let mut stale_scan = active.clone();
        stale_scan.file_size = 99999;
        stale_scan.status = BackupFileStatus::Pending;
        stale_scan.related_task_id = None;
        manager
            .upsert_folder_file_tasks(&[stale_scan], "cfg-folder", folder_task_id)
            .unwrap();

        let loaded_active = manager.load_folder_file_tasks(folder_task_id).unwrap();
        assert_eq!(loaded_active.len(), 1);
        assert_eq!(loaded_active[0].status, BackupFileStatus::Transferring);
        assert_eq!(loaded_active[0].file_size, 54321);
        assert_eq!(loaded_active[0].related_task_id.as_deref(), Some("upload-child"));
    }

    #[test]
    fn test_load_active_file_tasks_excludes_terminal_rows() {
        let dir = tempdir().unwrap();
        let manager = BackupPersistenceManager::new(&dir.path().join("active-preview.db")).unwrap();
        let now = chrono::Utc::now();
        let task_id = "folder:active-preview";
        let config_id = "active-preview";

        let parent = BackupTask {
            owner_uid: None,
            id: task_id.to_string(),
            config_id: config_id.to_string(),
            status: BackupTaskStatus::Transferring,
            sub_phase: None,
            trigger_type: TriggerType::Poll,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 5,
            transferred_bytes: 0,
            total_bytes: 500,
            scan_progress: None,
            created_at: now,
            started_at: Some(now),
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };
        manager.save_task(&parent).unwrap();

        let statuses = [
            ("active-pending", BackupFileStatus::Pending),
            ("active-checking", BackupFileStatus::Checking),
            ("active-encrypting", BackupFileStatus::Encrypting),
            ("active-waiting", BackupFileStatus::WaitingTransfer),
            ("active-transferring", BackupFileStatus::Transferring),
            ("done", BackupFileStatus::Completed),
            ("skipped", BackupFileStatus::Skipped),
            ("failed", BackupFileStatus::Failed),
        ];
        for (id, status) in statuses {
            let mut file_task = make_test_file_task(id, task_id);
            file_task.remote_path = format!("/remote/{id}");
            file_task.status = status;
            file_task.created_at = now;
            file_task.updated_at = now;
            manager.save_file_task(&file_task, config_id).unwrap();
        }

        let (active, total) = manager.load_active_file_tasks(task_id, 1, 20).unwrap();
        assert_eq!(total, 5);
        assert_eq!(active.len(), 5);
        assert!(active.iter().all(|file_task| {
            matches!(
                file_task.status,
                BackupFileStatus::Pending
                    | BackupFileStatus::Checking
                    | BackupFileStatus::Encrypting
                    | BackupFileStatus::WaitingTransfer
                    | BackupFileStatus::Transferring
            )
        }));
        assert!(!active.iter().any(|file_task| {
            matches!(
                file_task.status,
                BackupFileStatus::Completed | BackupFileStatus::Skipped | BackupFileStatus::Failed
            )
        }));
    }

    #[test]
    fn test_reset_folder_upload_execution_state_keeps_terminal_rows() {
        let dir = tempdir().unwrap();
        let manager = BackupPersistenceManager::new(&dir.path().join("reset-folder.db")).unwrap();
        let now = chrono::Utc::now();
        let task_id = "folder:reset-folder";
        let config_id = "reset-folder";

        let parent = BackupTask {
            owner_uid: None,
            id: task_id.to_string(),
            config_id: config_id.to_string(),
            status: BackupTaskStatus::Transferring,
            sub_phase: None,
            trigger_type: TriggerType::Poll,
            pending_files: Vec::new(),
            completed_count: 1,
            failed_count: 0,
            skipped_count: 0,
            total_count: 2,
            transferred_bytes: 12345,
            total_bytes: 24690,
            scan_progress: None,
            created_at: now,
            started_at: Some(now),
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };
        manager.save_task(&parent).unwrap();

        let mut active = make_test_file_task("reset-active", task_id);
        active.remote_path = "/remote/reset-active".to_string();
        active.status = BackupFileStatus::Transferring;
        active.related_task_id = Some("stale-upload-task".to_string());
        active.transferred_bytes = 99;
        active.error_message = Some("stale".to_string());
        manager.save_file_task(&active, config_id).unwrap();

        let mut completed = make_test_file_task("reset-completed", task_id);
        completed.remote_path = "/remote/reset-completed".to_string();
        completed.status = BackupFileStatus::Completed;
        completed.related_task_id = Some("completed-upload-task".to_string());
        manager.save_file_task(&completed, config_id).unwrap();

        assert_eq!(manager.reset_folder_upload_execution_state(task_id).unwrap(), 1);

        let rows = manager.load_folder_file_tasks(task_id).unwrap();
        let active = rows.iter().find(|row| row.id == "reset-active").unwrap();
        assert_eq!(active.status, BackupFileStatus::Pending);
        assert_eq!(active.related_task_id, None);
        assert_eq!(active.transferred_bytes, 0);
        assert_eq!(active.error_message, None);

        let completed = rows.iter().find(|row| row.id == "reset-completed").unwrap();
        assert_eq!(completed.status, BackupFileStatus::Completed);
        assert_eq!(
            completed.related_task_id.as_deref(),
            Some("completed-upload-task")
        );
    }

    /// Round-trip: save_file_task → load_file_tasks 应保留 sync_remote_* 字段
    #[test]
    fn test_file_task_sync_remote_roundtrip_single() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_rt.db");
        let manager = BackupPersistenceManager::new(&db_path).unwrap();

        // 先创建父任务满足外键约束
        let parent = BackupTask {
            owner_uid: None,
            id: "task-1".to_string(),
            config_id: "cfg-1".to_string(),
            status: BackupTaskStatus::Transferring,
            sub_phase: None,
            trigger_type: TriggerType::Manual,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 1,
            transferred_bytes: 0,
            total_bytes: 12345,
            scan_progress: None,
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };
        manager.save_task(&parent).unwrap();

        let ft = make_test_file_task("ft-1", "task-1");
        manager.save_file_task(&ft, "cfg-1").unwrap();

        let (loaded, total) = manager.load_file_tasks("task-1", 1, 50).unwrap();
        assert_eq!(total, 1);
        let l = &loaded[0];
        assert_eq!(l.sync_remote_mtime, Some(1700000000));
        assert_eq!(l.sync_remote_size, Some(12345));
        assert_eq!(l.sync_remote_fs_id, Some(88888));
    }

    /// Round-trip: save_file_tasks_batch → load_file_tasks_for_restore 应保留 sync_remote_* 字段
    #[test]
    fn test_file_task_sync_remote_roundtrip_batch_restore() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_batch.db");
        let manager = BackupPersistenceManager::new(&db_path).unwrap();

        // 需要先创建主任务记录（外键约束）
        let task = BackupTask {
            owner_uid: None,
            id: "task-2".to_string(),
            config_id: "cfg-2".to_string(),
            status: BackupTaskStatus::Transferring,
            sub_phase: None,
            trigger_type: TriggerType::Manual,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 2,
            transferred_bytes: 0,
            total_bytes: 24690,
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

        let mut ft1 = make_test_file_task("ft-batch-1", "task-2");
        ft1.sync_remote_mtime = Some(1600000000);
        ft1.sync_remote_size = Some(111);
        ft1.sync_remote_fs_id = Some(222);

        let mut ft2 = make_test_file_task("ft-batch-2", "task-2");
        ft2.sync_remote_mtime = None;
        ft2.sync_remote_size = None;
        ft2.sync_remote_fs_id = None;

        manager.save_file_tasks_batch(&[ft1, ft2], "cfg-2").unwrap();

        // load_file_tasks_for_restore 加载非终态任务
        let restored = manager.load_file_tasks_for_restore("task-2").unwrap();
        assert_eq!(restored.len(), 2);

        let r1 = restored.iter().find(|t| t.id == "ft-batch-1").unwrap();
        assert_eq!(r1.sync_remote_mtime, Some(1600000000));
        assert_eq!(r1.sync_remote_size, Some(111));
        assert_eq!(r1.sync_remote_fs_id, Some(222));

        let r2 = restored.iter().find(|t| t.id == "ft-batch-2").unwrap();
        assert_eq!(r2.sync_remote_mtime, None);
        assert_eq!(r2.sync_remote_size, None);
        assert_eq!(r2.sync_remote_fs_id, None);
    }

    /// Migration: 在没有 sync_remote_* 列的旧数据库上打开应自动添加列
    #[test]
    fn test_migration_adds_sync_remote_columns() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_migrate.db");

        // 1) 创建一个"旧"数据库：只有原始列，没有 sync_remote_*
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE backup_tasks (
                    id TEXT PRIMARY KEY,
                    config_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sub_phase TEXT,
                    trigger_type TEXT NOT NULL,
                    completed_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    skipped_count INTEGER DEFAULT 0,
                    total_count INTEGER DEFAULT 0,
                    transferred_bytes INTEGER DEFAULT 0,
                    total_bytes INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at INTEGER NOT NULL,
                    started_at INTEGER,
                    completed_at INTEGER
                );
                CREATE TABLE backup_file_tasks (
                    id TEXT PRIMARY KEY,
                    backup_task_id TEXT NOT NULL,
                    config_id TEXT NOT NULL DEFAULT '',
                    relative_path TEXT NOT NULL DEFAULT '',
                    file_name TEXT NOT NULL DEFAULT '',
                    local_path TEXT NOT NULL,
                    remote_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    head_md5 TEXT NOT NULL DEFAULT '',
                    fs_id INTEGER,
                    status TEXT NOT NULL,
                    sub_phase TEXT,
                    skip_reason TEXT,
                    encrypted INTEGER DEFAULT 0,
                    encrypted_name TEXT,
                    temp_encrypted_path TEXT,
                    transferred_bytes INTEGER DEFAULT 0,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    related_task_id TEXT,
                    backup_operation_type TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY (backup_task_id) REFERENCES backup_tasks(id)
                );
                "#,
            ).unwrap();
            // 插入一条旧格式的记录
            conn.execute(
                "INSERT INTO backup_tasks (id, config_id, status, trigger_type, created_at) VALUES ('t1','c1','transferring','manual',0)",
                [],
            ).unwrap();
            conn.execute(
                "INSERT INTO backup_file_tasks (id, backup_task_id, config_id, local_path, remote_path, file_size, status, encrypted, transferred_bytes, retry_count, created_at, updated_at) VALUES ('f1','t1','c1','/a','/b',100,'pending',0,0,0,0,0)",
                [],
            ).unwrap();
        }

        // 2) 用 BackupPersistenceManager 打开旧库 → 触发迁移
        let manager = BackupPersistenceManager::new(&db_path).unwrap();

        // 3) 旧记录应能加载，sync_remote_* 为 None
        let restored = manager.load_file_tasks_for_restore("t1").unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].sync_remote_mtime, None);
        assert_eq!(restored[0].sync_remote_size, None);
        assert_eq!(restored[0].sync_remote_fs_id, None);

        // 4) 新记录可以正常写入和读取 sync_remote_*
        let ft = make_test_file_task("f2", "t1");
        manager.save_file_task(&ft, "c1").unwrap();

        let restored2 = manager.load_file_tasks_for_restore("t1").unwrap();
        let new_task = restored2.iter().find(|t| t.id == "f2").unwrap();
        assert_eq!(new_task.sync_remote_mtime, Some(1700000000));
        assert_eq!(new_task.sync_remote_size, Some(12345));
        assert_eq!(new_task.sync_remote_fs_id, Some(88888));
    }
}
