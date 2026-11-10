// 流式任务队列模块
//
// 用于管理海量小文件下载任务，避免将所有任务加载到内存
//
// 核心特性：
// - 基于 SQLite 的任务队列
// - 内存中仅保留活跃任务（默认 500 个）
// - 延迟加载任务详情
// - 批量操作减少数据库 I/O

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::auth::Uid;
use crate::downloader::DownloadTask;

/// 任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    /// 等待中
    Pending,
    /// 活跃（正在下载）
    Active,
    /// 已完成
    Completed,
    /// 失败
    Failed,
    /// 已暂停
    Paused,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Paused => "paused",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "active" => Self::Active,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "paused" => Self::Paused,
            _ => Self::Pending,
        }
    }
}

/// 轻量级任务信息（用于队列管理）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightweightTaskInfo {
    /// 任务 ID
    pub task_id: String,
    /// 所属文件夹 ID
    pub folder_id: String,
    /// 百度网盘 fs_id
    pub fs_id: u64,
    /// 远程路径
    pub remote_path: String,
    /// 本地路径
    pub local_path: String,
    /// 文件大小
    pub file_size: u64,
    /// 任务状态
    pub status: TaskStatus,
    /// 优先级（数字越大越优先）
    pub priority: i32,
    /// 重试次数
    pub retry_count: u32,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 开始时间
    pub started_at: Option<DateTime<Utc>>,
    /// 完成时间
    pub completed_at: Option<DateTime<Utc>>,
    /// 错误信息
    pub error_message: Option<String>,
    /// 所属账号 UID
    pub owner_uid: Uid,
}

/// 任务队列统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    /// 待处理任务数
    pub pending_count: usize,
    /// 活跃任务数
    pub active_count: usize,
    /// 已完成任务数
    pub completed_count: usize,
    /// 失败任务数
    pub failed_count: usize,
    /// 已暂停任务数
    pub paused_count: usize,
    /// 总任务数
    pub total_count: usize,
    /// 内存中任务数
    pub in_memory_count: usize,
}

/// 流式任务队列配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingQueueConfig {
    /// 内存中最大活跃任务数
    #[serde(default = "default_max_active_tasks")]
    pub max_active_tasks: usize,
    /// 批量加载任务数量
    #[serde(default = "default_batch_fetch_size")]
    pub batch_fetch_size: usize,
    /// 批量提交任务数量
    #[serde(default = "default_batch_commit_size")]
    pub batch_commit_size: usize,
}

fn default_max_active_tasks() -> usize {
    500
}

fn default_batch_fetch_size() -> usize {
    100
}

fn default_batch_commit_size() -> usize {
    100
}

impl Default for StreamingQueueConfig {
    fn default() -> Self {
        Self {
            max_active_tasks: default_max_active_tasks(),
            batch_fetch_size: default_batch_fetch_size(),
            batch_commit_size: default_batch_commit_size(),
        }
    }
}

/// 流式任务队列
///
/// 核心设计：
/// - 所有任务存储在 SQLite 数据库中
/// - 内存中仅保留活跃任务（最多 max_active_tasks 个）
/// - 从数据库批量加载待处理任务
/// - 任务完成后从内存移除，更新数据库状态
#[derive(Debug)]
pub struct StreamingTaskQueue {
    /// 数据库连接
    db: Arc<Mutex<Connection>>,
    /// 内存中的活跃任务（task_id -> DownloadTask）
    active_tasks: Arc<DashMap<String, Arc<Mutex<DownloadTask>>>>,
    /// 配置
    config: StreamingQueueConfig,
    /// 所属文件夹 ID
    folder_id: String,
    /// 所属账号 UID
    owner_uid: Uid,
}

impl StreamingTaskQueue {
    /// 创建流式任务队列
    pub fn new(
        db: Arc<Mutex<Connection>>,
        folder_id: String,
        owner_uid: Uid,
        config: StreamingQueueConfig,
    ) -> Result<Self> {
        Ok(Self {
            db,
            active_tasks: Arc::new(DashMap::new()),
            config,
            folder_id,
            owner_uid,
        })
    }

    /// 初始化数据库表
    pub async fn init_db_schema(&self) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            "CREATE TABLE IF NOT EXISTS streaming_task_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL UNIQUE,
                folder_id TEXT NOT NULL,
                fs_id INTEGER NOT NULL,
                remote_path TEXT NOT NULL,
                local_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                priority INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                owner_uid INTEGER NOT NULL
            )",
            [],
        )?;

        db.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_folder_status ON streaming_task_queue (folder_id, status);
             CREATE INDEX IF NOT EXISTS idx_status_priority ON streaming_task_queue (status, priority DESC, id ASC);",
        )?;

        info!(
            "流式任务队列表已初始化: folder_id={}, owner_uid={}",
            self.folder_id, self.owner_uid
        );

        Ok(())
    }

    /// 批量添加任务到队列
    pub async fn add_tasks_batch(&self, tasks: Vec<LightweightTaskInfo>) -> Result<usize> {
        if tasks.is_empty() {
            return Ok(0);
        }

        let db = self.db.lock().await;
        let tx = db.unchecked_transaction()?;

        let mut added = 0;
        for task in tasks {
            let result = tx.execute(
                "INSERT OR IGNORE INTO streaming_task_queue
                (task_id, folder_id, fs_id, remote_path, local_path, file_size,
                 status, priority, created_at, retry_count, owner_uid)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    task.task_id,
                    task.folder_id,
                    task.fs_id as i64,
                    task.remote_path,
                    task.local_path,
                    task.file_size as i64,
                    task.status.as_str(),
                    task.priority,
                    task.created_at.timestamp(),
                    task.retry_count,
                    task.owner_uid.raw() as i64,
                ],
            );

            if result.is_ok() {
                added += 1;
            }
        }

        tx.commit()?;

        debug!(
            "批量添加任务到队列: folder_id={}, 添加数量={}",
            self.folder_id, added
        );

        Ok(added)
    }

    /// 从数据库获取下一批待处理任务
    ///
    /// 按优先级排序，返回 pending 状态的任务
    pub async fn fetch_next_batch(&self, limit: usize) -> Result<Vec<LightweightTaskInfo>> {
        let db = self.db.lock().await;

        let mut stmt = db.prepare(
            "SELECT task_id, folder_id, fs_id, remote_path, local_path, file_size,
                    status, priority, created_at, started_at, completed_at,
                    retry_count, error_message, owner_uid
             FROM streaming_task_queue
             WHERE folder_id = ?1 AND status = 'pending'
             ORDER BY priority DESC, id ASC
             LIMIT ?2",
        )?;

        let tasks = stmt
            .query_map(params![&self.folder_id, limit as i64], |row| {
                Ok(LightweightTaskInfo {
                    task_id: row.get(0)?,
                    folder_id: row.get(1)?,
                    fs_id: row.get::<_, i64>(2)? as u64,
                    remote_path: row.get(3)?,
                    local_path: row.get(4)?,
                    file_size: row.get::<_, i64>(5)? as u64,
                    status: TaskStatus::from_str(&row.get::<_, String>(6)?),
                    priority: row.get(7)?,
                    created_at: DateTime::from_timestamp(row.get(8)?, 0)
                        .unwrap_or_else(Utc::now),
                    started_at: row
                        .get::<_, Option<i64>>(9)?
                        .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                    completed_at: row
                        .get::<_, Option<i64>>(10)?
                        .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                    retry_count: row.get::<_, i64>(11)? as u32,
                    error_message: row.get(12)?,
                    owner_uid: Uid::new(row.get::<_, i64>(13)? as u64),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        debug!(
            "从队列获取任务批次: folder_id={}, 数量={}",
            self.folder_id,
            tasks.len()
        );

        Ok(tasks)
    }

    /// 标记任务为活跃状态
    pub async fn mark_task_active(&self, task_id: &str) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            "UPDATE streaming_task_queue
             SET status = 'active', started_at = ?1
             WHERE task_id = ?2",
            params![Utc::now().timestamp(), task_id],
        )?;

        Ok(())
    }

    /// 标记任务为完成状态
    pub async fn mark_task_completed(&self, task_id: &str) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            "UPDATE streaming_task_queue
             SET status = 'completed', completed_at = ?1
             WHERE task_id = ?2",
            params![Utc::now().timestamp(), task_id],
        )?;

        // 从内存中移除
        self.active_tasks.remove(task_id);

        debug!("任务已完成并从内存移除: task_id={}", task_id);

        Ok(())
    }

    /// 标记任务为失败状态
    pub async fn mark_task_failed(&self, task_id: &str, error: &str) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            "UPDATE streaming_task_queue
             SET status = 'failed', error_message = ?1, completed_at = ?2
             WHERE task_id = ?3",
            params![error, Utc::now().timestamp(), task_id],
        )?;

        // 从内存中移除
        self.active_tasks.remove(task_id);

        warn!("任务失败并从内存移除: task_id={}, error={}", task_id, error);

        Ok(())
    }

    /// 增加任务重试次数
    pub async fn increment_retry_count(&self, task_id: &str) -> Result<u32> {
        let db = self.db.lock().await;

        db.execute(
            "UPDATE streaming_task_queue
             SET retry_count = retry_count + 1, status = 'pending'
             WHERE task_id = ?1",
            params![task_id],
        )?;

        let retry_count: u32 = db
            .query_row(
                "SELECT retry_count FROM streaming_task_queue WHERE task_id = ?1",
                params![task_id],
                |row| row.get::<_, i64>(0).map(|v| v as u32),
            )
            .unwrap_or(0);

        Ok(retry_count)
    }

    /// 获取队列统计信息
    pub async fn get_stats(&self) -> Result<QueueStats> {
        let db = self.db.lock().await;

        let mut stmt = db.prepare(
            "SELECT status, COUNT(*)
             FROM streaming_task_queue
             WHERE folder_id = ?1
             GROUP BY status",
        )?;

        let counts: Vec<(String, usize)> = stmt
            .query_map(params![&self.folder_id], |row| {
                Ok((row.get(0)?, row.get::<_, i64>(1)? as usize))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut pending = 0;
        let mut active = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut paused = 0;

        for (status, count) in counts {
            match status.as_str() {
                "pending" => pending = count,
                "active" => active = count,
                "completed" => completed = count,
                "failed" => failed = count,
                "paused" => paused = count,
                _ => {}
            }
        }

        let total = pending + active + completed + failed + paused;
        let in_memory = self.active_tasks.len();

        Ok(QueueStats {
            pending_count: pending,
            active_count: active,
            completed_count: completed,
            failed_count: failed,
            paused_count: paused,
            total_count: total,
            in_memory_count: in_memory,
        })
    }

    /// 暂停文件夹所有任务
    pub async fn pause_all_tasks(&self) -> Result<usize> {
        let db = self.db.lock().await;

        let affected = db.execute(
            "UPDATE streaming_task_queue
             SET status = 'paused'
             WHERE folder_id = ?1 AND status IN ('pending', 'active')",
            params![&self.folder_id],
        )?;

        info!("暂停文件夹所有任务: folder_id={}, 数量={}", self.folder_id, affected);

        Ok(affected)
    }

    /// 恢复文件夹所有任务
    pub async fn resume_all_tasks(&self) -> Result<usize> {
        let db = self.db.lock().await;

        let affected = db.execute(
            "UPDATE streaming_task_queue
             SET status = 'pending'
             WHERE folder_id = ?1 AND status = 'paused'",
            params![&self.folder_id],
        )?;

        info!("恢复文件夹所有任务: folder_id={}, 数量={}", self.folder_id, affected);

        Ok(affected)
    }

    /// 清理已完成的任务（从数据库删除）
    pub async fn cleanup_completed_tasks(&self) -> Result<usize> {
        let db = self.db.lock().await;

        let affected = db.execute(
            "DELETE FROM streaming_task_queue
             WHERE folder_id = ?1 AND status = 'completed'",
            params![&self.folder_id],
        )?;

        info!(
            "清理已完成任务: folder_id={}, 删除数量={}",
            self.folder_id, affected
        );

        Ok(affected)
    }

    /// 添加任务到内存活跃列表
    pub fn add_active_task(&self, task_id: String, task: Arc<Mutex<DownloadTask>>) {
        self.active_tasks.insert(task_id, task);
    }

    /// 从内存活跃列表获取任务
    pub fn get_active_task(&self, task_id: &str) -> Option<Arc<Mutex<DownloadTask>>> {
        self.active_tasks.get(task_id).map(|t| t.clone())
    }

    /// 检查内存是否已满
    pub fn is_memory_full(&self) -> bool {
        self.active_tasks.len() >= self.config.max_active_tasks
    }

    /// 获取当前活跃任务数
    pub fn active_count(&self) -> usize {
        self.active_tasks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_streaming_queue_basic() {
        let db = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let queue = StreamingTaskQueue::new(
            db,
            "test_folder".to_string(),
            Uid::new(1),
            StreamingQueueConfig::default(),
        )
        .unwrap();

        queue.init_db_schema().await.unwrap();

        // 添加任务
        let tasks = vec![
            LightweightTaskInfo {
                task_id: "task1".to_string(),
                folder_id: "test_folder".to_string(),
                fs_id: 1,
                remote_path: "/test1.txt".to_string(),
                local_path: "/local/test1.txt".to_string(),
                file_size: 1024,
                status: TaskStatus::Pending,
                priority: 0,
                retry_count: 0,
                created_at: Utc::now(),
                started_at: None,
                completed_at: None,
                error_message: None,
                owner_uid: Uid::new(1),
            },
            LightweightTaskInfo {
                task_id: "task2".to_string(),
                folder_id: "test_folder".to_string(),
                fs_id: 2,
                remote_path: "/test2.txt".to_string(),
                local_path: "/local/test2.txt".to_string(),
                file_size: 2048,
                status: TaskStatus::Pending,
                priority: 10,
                retry_count: 0,
                created_at: Utc::now(),
                started_at: None,
                completed_at: None,
                error_message: None,
                owner_uid: Uid::new(1),
            },
        ];

        let added = queue.add_tasks_batch(tasks).await.unwrap();
        assert_eq!(added, 2);

        // 获取任务
        let fetched = queue.fetch_next_batch(10).await.unwrap();
        assert_eq!(fetched.len(), 2);
        // 高优先级排在前面
        assert_eq!(fetched[0].task_id, "task2");

        // 标记完成
        queue.mark_task_completed("task1").await.unwrap();

        // 获取统计
        let stats = queue.get_stats().await.unwrap();
        assert_eq!(stats.completed_count, 1);
        assert_eq!(stats.pending_count, 1);
    }
}
