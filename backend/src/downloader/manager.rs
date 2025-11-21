use crate::auth::UserAuth;
use crate::downloader::{ChunkScheduler, DownloadEngine, DownloadTask, TaskScheduleInfo, TaskStatus};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// 下载管理器
#[derive(Debug)]
pub struct DownloadManager {
    /// 所有任务
    tasks: Arc<RwLock<HashMap<String, Arc<Mutex<DownloadTask>>>>>,
    /// 任务取消令牌（task_id -> CancellationToken）
    cancellation_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
    /// 下载引擎
    engine: Arc<DownloadEngine>,
    /// 默认下载目录
    download_dir: PathBuf,
    /// 全局分片调度器
    chunk_scheduler: ChunkScheduler,
    /// 最大同时下载任务数
    max_concurrent_tasks: usize,
}

impl DownloadManager {
    /// 创建新的下载管理器
    pub fn new(user_auth: UserAuth, download_dir: PathBuf) -> Result<Self> {
        Self::with_config(user_auth, download_dir, 10, 5)
    }

    /// 使用指定配置创建下载管理器（不再需要 chunk_size 参数，引擎会自动计算）
    pub fn with_config(
        user_auth: UserAuth,
        download_dir: PathBuf,
        max_global_threads: usize,
        max_concurrent_tasks: usize,
    ) -> Result<Self> {
        // 确保下载目录存在（路径验证已在配置保存时完成）
        if !download_dir.exists() {
            std::fs::create_dir_all(&download_dir).context("创建下载目录失败")?;
            info!("✓ 下载目录已创建: {:?}", download_dir);
        }

        // 创建全局线程池
        let global_semaphore = Arc::new(Semaphore::new(max_global_threads));

        // 创建全局分片调度器
        let chunk_scheduler = ChunkScheduler::new(global_semaphore.clone(), max_concurrent_tasks);

        info!(
            "创建下载管理器: 下载目录={:?}, 全局线程数={}, 最大同时下载数={} (分片大小自适应)",
            download_dir, max_global_threads, max_concurrent_tasks
        );

        let engine = Arc::new(DownloadEngine::new(user_auth));

        Ok(Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            engine,
            download_dir,
            chunk_scheduler,
            max_concurrent_tasks,
        })
    }

    /// 创建下载任务
    pub async fn create_task(
        &self,
        fs_id: u64,
        remote_path: String,
        filename: String,
        total_size: u64,
    ) -> Result<String> {
        let local_path = self.download_dir.join(&filename);

        // 检查文件是否已存在
        if local_path.exists() {
            warn!("文件已存在: {:?}，将覆盖", local_path);
        }

        let task = DownloadTask::new(fs_id, remote_path, local_path, total_size);
        let task_id = task.id.clone();

        info!("创建下载任务: id={}, 文件名={}", task_id, filename);

        let task_arc = Arc::new(Mutex::new(task));
        self.tasks.write().await.insert(task_id.clone(), task_arc);

        Ok(task_id)
    }

    /// 开始下载任务
    pub async fn start_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        // 检查任务状态
        {
            let t = task.lock().await;
            if t.status == TaskStatus::Downloading {
                anyhow::bail!("任务已在下载中");
            }
            if t.status == TaskStatus::Completed {
                anyhow::bail!("任务已完成");
            }
        }

        info!("启动下载任务: {}", task_id);

        // 检查调度器是否已满（真正的限制）
        let active_count = self.chunk_scheduler.active_task_count().await;
        if active_count >= self.max_concurrent_tasks {
            anyhow::bail!(
                "超过最大并发任务数限制 ({}/{})",
                active_count,
                self.max_concurrent_tasks
            );
        }

        // 创建取消令牌
        let cancellation_token = CancellationToken::new();
        self.cancellation_tokens
            .write()
            .await
            .insert(task_id.to_string(), cancellation_token.clone());

        // 准备任务（获取下载链接、创建分片管理器等）
        let engine = self.engine.clone();
        let task_clone = task.clone();
        let chunk_scheduler = self.chunk_scheduler.clone();
        let task_id_clone = task_id.to_string();
        let cancellation_tokens = self.cancellation_tokens.clone();

        tokio::spawn(async move {
            // 准备任务
            let prepare_result = engine.prepare_for_scheduling(task_clone.clone()).await;

            match prepare_result {
                Ok((
                    client,
                    cookie,
                    referer,
                    url_health,
                    output_path,
                    chunk_size,
                    timeout_secs,
                    chunk_manager,
                    speed_calc,
                )) => {
                    // 创建任务调度信息
                    let task_info = TaskScheduleInfo {
                        task_id: task_id_clone.clone(),
                        task: task_clone.clone(),
                        chunk_manager,
                        speed_calc,
                        client,
                        cookie,
                        referer,
                        url_health,
                        output_path,
                        chunk_size,
                        timeout_secs,
                        cancellation_token: cancellation_token.clone(),
                        active_chunk_count: Arc::new(AtomicUsize::new(0)),
                    };

                    // 注册到调度器
                    if let Err(e) = chunk_scheduler.register_task(task_info).await {
                        error!("注册任务到调度器失败: {}", e);

                        // 标记任务失败
                        let mut t = task_clone.lock().await;
                        t.mark_failed(e.to_string());

                        // 移除取消令牌
                        cancellation_tokens.write().await.remove(&task_id_clone);
                    }
                }
                Err(e) => {
                    error!("准备任务失败: {}", e);

                    // 标记任务失败
                    let mut t = task_clone.lock().await;
                    t.mark_failed(e.to_string());

                    // 移除取消令牌
                    cancellation_tokens.write().await.remove(&task_id_clone);
                }
            }
        });

        Ok(())
    }

    /// 暂停下载任务
    pub async fn pause_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        let mut t = task.lock().await;
        if t.status != TaskStatus::Downloading {
            anyhow::bail!("任务未在下载中");
        }

        t.mark_paused();
        info!("暂停下载任务: {}", task_id);

        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        Ok(())
    }

    /// 恢复下载任务
    pub async fn resume_task(&self, task_id: &str) -> Result<()> {
        let task = self
            .tasks
            .read()
            .await
            .get(task_id)
            .cloned()
            .context("任务不存在")?;

        let t = task.lock().await;
        if t.status != TaskStatus::Paused {
            anyhow::bail!("任务未暂停");
        }
        drop(t);

        info!("恢复下载任务: {}", task_id);
        self.start_task(task_id).await
    }

    /// 删除下载任务
    pub async fn delete_task(&self, task_id: &str, delete_file: bool) -> Result<()> {
        // 从调度器取消任务
        self.chunk_scheduler.cancel_task(task_id).await;

        // 移除取消令牌
        self.cancellation_tokens.write().await.remove(task_id);

        // 等待一小段时间让下载任务有机会清理
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let task = self
            .tasks
            .write()
            .await
            .remove(task_id)
            .context("任务不存在")?;

        let t = task.lock().await;

        // 决定是否删除本地文件
        // 1. 对于未完成的任务（Pending/Downloading/Paused/Failed），自动删除临时文件
        // 2. 对于已完成的任务（Completed），根据 delete_file 参数决定
        let should_delete = match t.status {
            TaskStatus::Completed => delete_file,
            _ => true, // 未完成的任务总是删除临时文件
        };

        if should_delete && t.local_path.exists() {
            tokio::fs::remove_file(&t.local_path)
                .await
                .context("删除本地文件失败")?;
            info!("已删除本地文件: {:?}", t.local_path);
        }

        info!("删除下载任务: {}", task_id);
        Ok(())
    }

    /// 获取任务
    pub async fn get_task(&self, task_id: &str) -> Option<DownloadTask> {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(task_id) {
            Some(task.lock().await.clone())
        } else {
            None
        }
    }

    /// 获取所有任务
    pub async fn get_all_tasks(&self) -> Vec<DownloadTask> {
        let tasks = self.tasks.read().await;
        let mut result = Vec::new();

        for task in tasks.values() {
            result.push(task.lock().await.clone());
        }

        result
    }

    /// 获取进行中的任务数量
    pub async fn active_count(&self) -> usize {
        // 使用调度器的计数（更准确）
        self.chunk_scheduler.active_task_count().await
    }

    /// 清除已完成的任务
    pub async fn clear_completed(&self) -> usize {
        let mut tasks = self.tasks.write().await;
        let mut to_remove = Vec::new();

        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.status == TaskStatus::Completed {
                to_remove.push(id.clone());
            }
        }

        let count = to_remove.len();
        for id in to_remove {
            tasks.remove(&id);
        }

        info!("清除了 {} 个已完成的任务", count);
        count
    }

    /// 清除失败的任务
    pub async fn clear_failed(&self) -> usize {
        let mut tasks = self.tasks.write().await;
        let mut to_remove = Vec::new();

        for (id, task) in tasks.iter() {
            let t = task.lock().await;
            if t.status == TaskStatus::Failed {
                to_remove.push((id.clone(), t.local_path.clone()));
            }
        }

        let count = to_remove.len();
        for (id, local_path) in to_remove {
            tasks.remove(&id);

            // 删除失败任务的临时文件
            if local_path.exists() {
                if let Err(e) = std::fs::remove_file(&local_path) {
                    warn!("删除失败任务的临时文件失败: {:?}, 错误: {}", local_path, e);
                } else {
                    info!("已删除失败任务的临时文件: {:?}", local_path);
                }
            }
        }

        info!("清除了 {} 个失败的任务", count);
        count
    }

    /// 获取下载目录
    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }
}

impl Drop for DownloadManager {
    fn drop(&mut self) {
        // 停止调度器（只有当 DownloadManager 的所有引用都被释放时才会调用）
        self.chunk_scheduler.stop();
        info!("下载管理器已销毁，调度器已停止");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UserAuth;
    use tempfile::TempDir;

    fn create_mock_user_auth() -> UserAuth {
        UserAuth {
            uid: 123456789,
            username: "test_user".to_string(),
            nickname: Some("测试用户".to_string()),
            avatar_url: Some("https://example.com/avatar.jpg".to_string()),
            vip_type: Some(2), // SVIP
            total_space: Some(2 * 1024 * 1024 * 1024 * 1024), // 2TB
            used_space: Some(500 * 1024 * 1024 * 1024), // 500GB
            bduss: "mock_bduss".to_string(),
            stoken: Some("mock_stoken".to_string()),
            ptoken: Some("mock_ptoken".to_string()),
            cookies: Some("BDUSS=mock_bduss".to_string()),
            login_time: 0,
        }
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

        assert_eq!(manager.download_dir(), temp_dir.path());
        assert_eq!(manager.get_all_tasks().await.len(), 0);
    }

    #[tokio::test]
    async fn test_create_task() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

        let task_id = manager
            .create_task(
                12345,
                "/test/file.txt".to_string(),
                "file.txt".to_string(),
                1024,
            )
            .await
            .unwrap();

        assert!(!task_id.is_empty());
        assert_eq!(manager.get_all_tasks().await.len(), 1);

        let task = manager.get_task(&task_id).await.unwrap();
        assert_eq!(task.fs_id, 12345);
        assert_eq!(task.status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn test_delete_task() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

        let task_id = manager
            .create_task(
                12345,
                "/test/file.txt".to_string(),
                "file.txt".to_string(),
                1024,
            )
            .await
            .unwrap();

        assert_eq!(manager.get_all_tasks().await.len(), 1);

        manager.delete_task(&task_id, false).await.unwrap();
        assert_eq!(manager.get_all_tasks().await.len(), 0);
    }

    #[tokio::test]
    async fn test_clear_completed() {
        let temp_dir = TempDir::new().unwrap();
        let user_auth = create_mock_user_auth();
        let manager = DownloadManager::new(user_auth, temp_dir.path().to_path_buf()).unwrap();

        // 创建3个任务
        let task_id1 = manager
            .create_task(1, "/test1".to_string(), "file1.txt".to_string(), 1024)
            .await
            .unwrap();
        let task_id2 = manager
            .create_task(2, "/test2".to_string(), "file2.txt".to_string(), 1024)
            .await
            .unwrap();
        let _task_id3 = manager
            .create_task(3, "/test3".to_string(), "file3.txt".to_string(), 1024)
            .await
            .unwrap();

        // 标记2个为已完成
        {
            let tasks = manager.tasks.read().await;
            tasks.get(&task_id1).unwrap().lock().await.mark_completed();
            tasks.get(&task_id2).unwrap().lock().await.mark_completed();
        }

        assert_eq!(manager.get_all_tasks().await.len(), 3);
        let cleared = manager.clear_completed().await;
        assert_eq!(cleared, 2);
        assert_eq!(manager.get_all_tasks().await.len(), 1);
    }
}
