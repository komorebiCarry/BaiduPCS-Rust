// 流式文件列表 - 内存优化
//
// 设计目标：
// 1. 避免在内存中保存全部文件列表
// 2. 使用数据库游标按需加载
// 3. 支持千万级文件场景

use crate::downloader::folder::PendingFile;
use anyhow::Result;
use rusqlite::Connection;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// 流式文件列表配置
#[derive(Debug, Clone)]
pub struct StreamingFileListConfig {
    /// 每批加载的文件数量
    pub batch_size: usize,
    /// 预取批次数
    pub prefetch_batches: usize,
}

impl Default for StreamingFileListConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,       // 每次加载 100 个文件
            prefetch_batches: 2,   // 预取 2 批
        }
    }
}

/// 流式文件列表
pub struct StreamingFileList {
    config: StreamingFileListConfig,
    db: Arc<Mutex<Connection>>,
    folder_id: String,
}

impl StreamingFileList {
    /// 创建流式文件列表
    pub async fn new(
        folder_id: String,
        db: Arc<Mutex<Connection>>,
        config: StreamingFileListConfig,
    ) -> Result<Self> {
        let list = Self {
            config,
            db,
            folder_id,
        };

        list.init_table().await?;
        Ok(list)
    }

    /// 初始化表
    async fn init_table(&self) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS pending_files_{} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fs_id INTEGER NOT NULL,
                    filename TEXT NOT NULL,
                    remote_path TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER
                )",
                self.folder_id
            ),
            [],
        )?;

        db.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS idx_status_{}
                 ON pending_files_{}(status)",
                self.folder_id, self.folder_id
            ),
            [],
        )?;

        info!("✅ 流式文件列表表初始化完成: {}", self.folder_id);
        Ok(())
    }

    /// 批量插入文件
    pub async fn insert_batch(&self, files: &[PendingFile]) -> Result<()> {
        let db = self.db.lock().await;

        let tx = db.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare(&format!(
                "INSERT INTO pending_files_{}
                 (fs_id, filename, remote_path, relative_path, size, created_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                self.folder_id
            ))?;

            let now = chrono::Utc::now().timestamp();

            for file in files {
                stmt.execute((
                    file.fs_id,
                    &file.filename,
                    &file.remote_path,
                    &file.relative_path,
                    file.size,
                    now,
                ))?;
            }
        }

        tx.commit()?;

        debug!("💾 插入 {} 个文件到流式列表", files.len());
        Ok(())
    }

    /// 获取待处理文件的游标
    pub async fn cursor(&self) -> Result<FileListCursor> {
        FileListCursor::new(
            self.folder_id.clone(),
            Arc::clone(&self.db),
            self.config.clone(),
        )
    }

    /// 获取文件总数
    pub async fn total_count(&self) -> Result<u64> {
        let db = self.db.lock().await;

        let count: i64 = db.query_row(
            &format!(
                "SELECT COUNT(*) FROM pending_files_{} WHERE status = 'pending'",
                self.folder_id
            ),
            [],
            |row| row.get(0),
        )?;

        Ok(count as u64)
    }

    /// 标记文件已完成
    pub async fn mark_completed(&self, fs_id: u64) -> Result<()> {
        let db = self.db.lock().await;

        let now = chrono::Utc::now().timestamp();

        db.execute(
            &format!(
                "UPDATE pending_files_{}
                 SET status = 'completed', updated_at = ?
                 WHERE fs_id = ?",
                self.folder_id
            ),
            (now, fs_id),
        )?;

        Ok(())
    }

    /// 清理表
    pub async fn cleanup(&self) -> Result<()> {
        let db = self.db.lock().await;

        db.execute(
            &format!("DROP TABLE IF EXISTS pending_files_{}", self.folder_id),
            [],
        )?;

        info!("🗑️ 清理流式文件列表表: {}", self.folder_id);
        Ok(())
    }
}

/// 文件列表游标
pub struct FileListCursor {
    folder_id: String,
    db: Arc<Mutex<Connection>>,
    config: StreamingFileListConfig,
    offset: usize,
}

impl FileListCursor {
    fn new(
        folder_id: String,
        db: Arc<Mutex<Connection>>,
        config: StreamingFileListConfig,
    ) -> Result<Self> {
        Ok(Self {
            folder_id,
            db,
            config,
            offset: 0,
        })
    }

    /// 获取下一批文件
    pub async fn next_batch(&mut self) -> Result<Vec<PendingFile>> {
        let db = self.db.lock().await;

        let mut stmt = db.prepare(&format!(
            "SELECT fs_id, filename, remote_path, relative_path, size
             FROM pending_files_{}
             WHERE status = 'pending'
             ORDER BY id
             LIMIT ? OFFSET ?",
            self.folder_id
        ))?;

        let files = stmt
            .query_map((self.config.batch_size, self.offset), |row| {
                Ok(PendingFile {
                    fs_id: row.get(0)?,
                    filename: row.get(1)?,
                    remote_path: row.get(2)?,
                    relative_path: row.get(3)?,
                    size: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        self.offset += files.len();

        debug!(
            "📂 加载文件批次: offset={}, count={}",
            self.offset - files.len(),
            files.len()
        );

        Ok(files)
    }

    /// 重置游标
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    /// 获取当前偏移量
    pub fn offset(&self) -> usize {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = StreamingFileListConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.prefetch_batches, 2);
    }

    #[tokio::test]
    async fn test_cursor_creation() {
        let db = Arc::new(Mutex::new(
            Connection::open_in_memory().unwrap()
        ));

        let list = StreamingFileList::new(
            "test_folder".to_string(),
            db,
            StreamingFileListConfig::default(),
        )
        .await
        .unwrap();

        let mut cursor = list.cursor().await.unwrap();
        assert_eq!(cursor.offset(), 0);

        cursor.reset();
        assert_eq!(cursor.offset(), 0);
    }
}
