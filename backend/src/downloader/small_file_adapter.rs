// 小文件优化集成适配器
//
// 用于将小文件优化模块集成到 FolderDownloadManager，而不直接修改现有代码
//
// 核心职责：
// - 拦截文件夹下载流程
// - 判断是否使用小文件优化
// - 协调流式队列、批量链接获取、小文件下载器
// - 向后兼容传统下载流程

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::auth::Uid;
use crate::config::{AppConfig, SmallFileOptimizationConfig};
use crate::downloader::{
    BatchLinkFetcher, DownloadManager, LightweightTaskInfo, SmallFileDownloader,
    SmallFileTask, StreamingTaskQueue, is_small_file,
};
use crate::downloader::streaming_queue::TaskStatus;
use crate::netdisk::NetdiskClient;
use crate::share_sync::rate_limit::QuotaLimiter;

/// 小文件优化适配器
///
/// 作为 FolderDownloadManager 的辅助组件，负责小文件优化逻辑
#[derive(Debug)]
pub struct SmallFileOptimizationAdapter {
    /// 是否启用优化
    enabled: bool,

    /// 流式任务队列（每个文件夹一个）
    streaming_queues: Arc<tokio::sync::RwLock<HashMap<String, Arc<StreamingTaskQueue>>>>,

    /// 批量链接获取器（共享）
    batch_link_fetcher: Option<Arc<BatchLinkFetcher>>,

    /// 小文件下载器（共享）
    small_file_downloader: Option<Arc<SmallFileDownloader>>,

    /// 配置
    config: SmallFileOptimizationConfig,

    /// 数据库连接（用于流式队列）
    db: Arc<Mutex<rusqlite::Connection>>,
}

/// 文件信息（简化版）
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub fs_id: u64,
    pub remote_path: String,
    pub local_path: String,
    pub file_size: u64,
}

impl SmallFileOptimizationAdapter {
    /// 创建适配器
    pub fn new(
        app_config: &AppConfig,
        db: Arc<Mutex<rusqlite::Connection>>,
    ) -> Result<Self> {
        let config = app_config.download.small_file_optimization.clone();
        let enabled = config.enabled;

        info!(
            "小文件优化适配器初始化: enabled={}, max_active_tasks={}",
            enabled, config.streaming_queue.max_active_tasks
        );

        Ok(Self {
            enabled,
            streaming_queues: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            batch_link_fetcher: None,
            small_file_downloader: None,
            config,
            db,
        })
    }

    /// 初始化批量链接获取器
    pub fn init_batch_link_fetcher(
        &mut self,
        netdisk_client: Arc<NetdiskClient>,
        rate_limiter: Arc<QuotaLimiter>,
    ) {
        if !self.enabled {
            return;
        }

        let fetcher_config = crate::downloader::batch_link_fetcher::BatchLinkFetcherConfig {
            batch_size: self.config.batch_link_fetcher.batch_size,
            batch_interval_ms: self.config.batch_link_fetcher.batch_interval_ms,
            max_concurrent_batches: self.config.batch_link_fetcher.max_concurrent_batches,
            link_cache_ttl_secs: self.config.batch_link_fetcher.link_cache_ttl_secs,
            enable_adaptive_delay: self.config.batch_link_fetcher.enable_adaptive_delay,
        };

        self.batch_link_fetcher = Some(Arc::new(BatchLinkFetcher::new(
            netdisk_client,
            fetcher_config,
            rate_limiter,
        )));

        info!("批量链接获取器已初始化");
    }

    /// 初始化小文件下载器
    pub fn init_small_file_downloader(&mut self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let downloader_config = crate::downloader::small_file_downloader::SmallFileDownloaderConfig {
            batch_size: self.config.small_file_downloader.batch_size,
            max_concurrent_streams: self.config.small_file_downloader.max_concurrent_streams,
            download_timeout_secs: self.config.small_file_downloader.download_timeout_secs,
        };

        self.small_file_downloader = Some(Arc::new(SmallFileDownloader::new(downloader_config)?));

        info!("小文件下载器已初始化");
        Ok(())
    }

    /// 为文件夹创建流式任务队列
    pub async fn create_streaming_queue(
        &self,
        folder_id: String,
        owner_uid: Uid,
    ) -> Result<Arc<StreamingTaskQueue>> {
        if !self.enabled {
            return Err(anyhow::anyhow!("小文件优化未启用"));
        }

        let queue_config = crate::downloader::streaming_queue::StreamingQueueConfig {
            max_active_tasks: self.config.streaming_queue.max_active_tasks,
            batch_fetch_size: self.config.streaming_queue.batch_fetch_size,
            batch_commit_size: self.config.streaming_queue.batch_commit_size,
        };

        let queue = Arc::new(StreamingTaskQueue::new(
            Arc::clone(&self.db),
            folder_id.clone(),
            owner_uid,
            queue_config,
        )?);

        queue.init_db_schema().await?;

        // 保存到缓存
        self.streaming_queues
            .write()
            .await
            .insert(folder_id.clone(), Arc::clone(&queue));

        info!("为文件夹 {} 创建了流式任务队列", folder_id);

        Ok(queue)
    }

    /// 获取文件夹的流式任务队列
    pub async fn get_streaming_queue(
        &self,
        folder_id: &str,
    ) -> Option<Arc<StreamingTaskQueue>> {
        self.streaming_queues.read().await.get(folder_id).cloned()
    }

    /// 判断是否应该使用小文件优化
    ///
    /// 条件：
    /// 1. 优化已启用
    /// 2. 文件数量 > 1000
    /// 3. 小文件（< 1MB）占比 > 50%
    pub fn should_use_optimization(&self, files: &[FileInfo]) -> bool {
        if !self.enabled {
            return false;
        }

        let total = files.len();
        if total < 1000 {
            debug!("文件数量 {} < 1000，不使用小文件优化", total);
            return false;
        }

        let small_file_count = files.iter().filter(|f| is_small_file(f.file_size)).count();
        let ratio = small_file_count as f64 / total as f64;

        if ratio > 0.5 {
            info!(
                "文件数量 {}，小文件占比 {:.1}%，使用小文件优化",
                total,
                ratio * 100.0
            );
            true
        } else {
            debug!(
                "文件数量 {}，小文件占比 {:.1}% < 50%，不使用小文件优化",
                total,
                ratio * 100.0
            );
            false
        }
    }

    /// 批量添加任务到流式队列
    pub async fn add_tasks_to_queue(
        &self,
        folder_id: &str,
        files: Vec<FileInfo>,
        owner_uid: Uid,
    ) -> Result<usize> {
        let queue = self
            .get_streaming_queue(folder_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("流式队列未初始化"))?;

        let tasks: Vec<LightweightTaskInfo> = files
            .into_iter()
            .map(|f| LightweightTaskInfo {
                task_id: uuid::Uuid::new_v4().to_string(),
                folder_id: folder_id.to_string(),
                fs_id: f.fs_id,
                remote_path: f.remote_path,
                local_path: f.local_path,
                file_size: f.file_size,
                status: TaskStatus::Pending,
                priority: 0,
                retry_count: 0,
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                error_message: None,
                owner_uid,
            })
            .collect();

        let added = queue.add_tasks_batch(tasks).await?;

        info!("添加 {} 个任务到流式队列: folder_id={}", added, folder_id);

        Ok(added)
    }

    /// 从队列获取下一批任务并下载
    pub async fn process_next_batch(
        &self,
        folder_id: &str,
        _download_manager: &Arc<DownloadManager>,
        cookie: &str,
        cancellation_token: CancellationToken,
    ) -> Result<usize> {
        let queue = self
            .get_streaming_queue(folder_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("流式队列未初始化"))?;

        // 检查内存是否已满
        if queue.is_memory_full() {
            debug!("内存已满，等待任务完成: folder_id={}", folder_id);
            return Ok(0);
        }

        // 获取下一批任务
        let batch_size = self.config.streaming_queue.batch_fetch_size;
        let tasks = queue.fetch_next_batch(batch_size).await?;

        if tasks.is_empty() {
            debug!("没有待处理任务: folder_id={}", folder_id);
            return Ok(0);
        }

        info!(
            "获取到 {} 个待处理任务: folder_id={}",
            tasks.len(),
            folder_id
        );

        // 批量获取下载链接
        let fs_ids: Vec<u64> = tasks.iter().map(|t| t.fs_id).collect();
        let links = if let Some(ref fetcher) = self.batch_link_fetcher {
            fetcher.fetch_batch_links(fs_ids).await?
        } else {
            warn!("批量链接获取器未初始化，跳过");
            return Ok(0);
        };

        // 按文件大小分类
        let (small_tasks, large_tasks): (Vec<_>, Vec<_>) = tasks
            .into_iter()
            .partition(|t| is_small_file(t.file_size));

        let mut processed = 0;

        // 小文件批量下载
        if !small_tasks.is_empty() && self.small_file_downloader.is_some() {
            processed += self
                .download_small_files_batch(
                    folder_id,
                    small_tasks,
                    &links,
                    cookie,
                    &queue,
                    cancellation_token.clone(),
                )
                .await?;
        }

        // 大文件使用传统下载器
        for task in large_tasks {
            if let Some(_urls) = links.get(&task.fs_id) {
                // 标记为活跃
                queue.mark_task_active(&task.task_id).await?;

                // TODO: 使用 download_manager 创建下载任务
                // 这里需要集成到现有的 DownloadManager

                processed += 1;
            }
        }

        Ok(processed)
    }

    /// 批量下载小文件
    async fn download_small_files_batch(
        &self,
        folder_id: &str,
        tasks: Vec<LightweightTaskInfo>,
        links: &HashMap<u64, Vec<String>>,
        cookie: &str,
        queue: &Arc<StreamingTaskQueue>,
        cancellation_token: CancellationToken,
    ) -> Result<usize> {
        let downloader = self
            .small_file_downloader
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("小文件下载器未初始化"))?;

        // 准备下载任务
        let download_tasks: Vec<SmallFileTask> = tasks
            .iter()
            .filter_map(|t| {
                links.get(&t.fs_id).and_then(|urls| {
                    urls.first().map(|url| SmallFileTask {
                        task_id: t.task_id.clone(),
                        url: url.clone(),
                        local_path: t.local_path.clone(),
                        file_size: t.file_size,
                        cookie: cookie.to_string(),
                    })
                })
            })
            .collect();

        if download_tasks.is_empty() {
            return Ok(0);
        }

        info!(
            "开始批量下载 {} 个小文件: folder_id={}",
            download_tasks.len(),
            folder_id
        );

        // 标记所有任务为活跃
        for task in &tasks {
            queue.mark_task_active(&task.task_id).await?;
        }

        // 批量下载
        let results = downloader
            .download_batch(download_tasks.clone(), cancellation_token)
            .await;

        // 更新任务状态
        let mut success_count = 0;
        for (i, result) in results.into_iter().enumerate() {
            let task_id = &download_tasks[i].task_id;
            match result {
                Ok(_) => {
                    queue.mark_task_completed(task_id).await?;
                    success_count += 1;
                }
                Err(e) => {
                    queue
                        .mark_task_failed(task_id, &format!("{:#}", e))
                        .await?;
                }
            }
        }

        info!(
            "批量下载完成: 成功 {}/{}, folder_id={}",
            success_count,
            download_tasks.len(),
            folder_id
        );

        Ok(success_count)
    }

    /// 获取文件夹队列统计
    pub async fn get_queue_stats(
        &self,
        folder_id: &str,
    ) -> Result<crate::downloader::QueueStats> {
        let queue = self
            .get_streaming_queue(folder_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("流式队列未初始化"))?;

        queue.get_stats().await
    }

    /// 暂停文件夹所有任务
    pub async fn pause_folder(&self, folder_id: &str) -> Result<usize> {
        let queue = self
            .get_streaming_queue(folder_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("流式队列未初始化"))?;

        queue.pause_all_tasks().await
    }

    /// 恢复文件夹所有任务
    pub async fn resume_folder(&self, folder_id: &str) -> Result<usize> {
        let queue = self
            .get_streaming_queue(folder_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("流式队列未初始化"))?;

        queue.resume_all_tasks().await
    }

    /// 清理文件夹队列
    pub async fn cleanup_folder(&self, folder_id: &str) -> Result<()> {
        self.streaming_queues.write().await.remove(folder_id);
        info!("清理文件夹队列: folder_id={}", folder_id);
        Ok(())
    }

    /// 是否已启用
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_use_optimization() {
        let db = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
        let config = AppConfig::default();
        let adapter = SmallFileOptimizationAdapter::new(&config, db).unwrap();

        // 小于 1000 个文件
        let files: Vec<FileInfo> = (0..500)
            .map(|i| FileInfo {
                fs_id: i,
                remote_path: format!("/file{}", i),
                local_path: format!("/local/file{}", i),
                file_size: 500 * 1024, // 500KB
            })
            .collect();
        assert!(!adapter.should_use_optimization(&files));

        // 大于 1000 个文件，小文件占比 > 50%
        let files: Vec<FileInfo> = (0..2000)
            .map(|i| FileInfo {
                fs_id: i,
                remote_path: format!("/file{}", i),
                local_path: format!("/local/file{}", i),
                file_size: 500 * 1024, // 500KB
            })
            .collect();
        assert!(adapter.should_use_optimization(&files));

        // 大于 1000 个文件，但大文件占比 > 50%
        let files: Vec<FileInfo> = (0..2000)
            .map(|i| FileInfo {
                fs_id: i,
                remote_path: format!("/file{}", i),
                local_path: format!("/local/file{}", i),
                file_size: 10 * 1024 * 1024, // 10MB
            })
            .collect();
        assert!(!adapter.should_use_optimization(&files));
    }
}
