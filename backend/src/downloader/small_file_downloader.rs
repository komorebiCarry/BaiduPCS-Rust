// 小文件批量下载器
//
// 用于优化大量小文件的下载性能
//
// 核心特性：
// - HTTP/2 多路复用（单连接并发下载多个文件）
// - 自动识别小文件（< 1MB）
// - 批量下载（50 个文件/批）
// - 流式写入（减少内存占用）

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// 小文件阈值（1MB）
pub const SMALL_FILE_THRESHOLD: u64 = 1 * 1024 * 1024;

/// 小文件下载任务
#[derive(Debug, Clone)]
pub struct SmallFileTask {
    /// 任务 ID
    pub task_id: String,
    /// 下载 URL
    pub url: String,
    /// 本地路径
    pub local_path: String,
    /// 文件大小
    pub file_size: u64,
    /// Cookie
    pub cookie: String,
}

/// 小文件下载配置
#[derive(Debug, Clone)]
pub struct SmallFileDownloaderConfig {
    /// 批量下载数量（默认 50）
    pub batch_size: usize,
    /// HTTP/2 最大并发流（默认 100）
    pub max_concurrent_streams: usize,
    /// 下载超时（秒，默认 60）
    pub download_timeout_secs: u64,
}

impl Default for SmallFileDownloaderConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_concurrent_streams: 100,
            download_timeout_secs: 60,
        }
    }
}

/// 小文件下载统计
#[derive(Debug, Clone, Default)]
pub struct SmallFileDownloadStats {
    /// 总文件数
    pub total_files: usize,
    /// 成功数
    pub success_count: usize,
    /// 失败数
    pub failed_count: usize,
    /// 总下载字节数
    pub total_bytes: u64,
    /// 总耗时（毫秒）
    pub total_time_ms: u64,
    /// 平均速度（KB/s）
    pub avg_speed_kbps: f64,
    /// 吞吐量（文件/秒）
    pub throughput_fps: f64,
}

/// 小文件批量下载器
#[derive(Debug)]
pub struct SmallFileDownloader {
    /// HTTP 客户端（启用 HTTP/2）
    http_client: Arc<Client>,
    /// 配置
    config: SmallFileDownloaderConfig,
    /// 统计信息
    stats: Arc<parking_lot::RwLock<SmallFileDownloadStats>>,
    /// 成功计数器
    success_count: Arc<AtomicUsize>,
    /// 失败计数器
    failed_count: Arc<AtomicUsize>,
    /// 下载字节计数器
    downloaded_bytes: Arc<AtomicU64>,
}

impl SmallFileDownloader {
    /// 创建小文件下载器
    pub fn new(config: SmallFileDownloaderConfig) -> Result<Self> {
        // 创建启用 HTTP/2 的客户端
        let http_client = Client::builder()
            .http2_prior_knowledge() // 强制使用 HTTP/2
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(config.download_timeout_secs))
            .build()?;

        Ok(Self {
            http_client: Arc::new(http_client),
            config,
            stats: Arc::new(parking_lot::RwLock::new(SmallFileDownloadStats::default())),
            success_count: Arc::new(AtomicUsize::new(0)),
            failed_count: Arc::new(AtomicUsize::new(0)),
            downloaded_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// 批量下载小文件
    ///
    /// 使用 HTTP/2 多路复用，在单个连接上并发下载多个文件
    pub async fn download_batch(
        &self,
        tasks: Vec<SmallFileTask>,
        cancellation_token: CancellationToken,
    ) -> Vec<Result<()>> {
        if tasks.is_empty() {
            return Vec::new();
        }

        let total = tasks.len();
        let start_time = Instant::now();

        info!(
            "开始批量下载小文件: 数量={}, HTTP/2 并发流={}",
            total, self.config.max_concurrent_streams
        );

        // 使用 futures stream 并发下载
        let results = stream::iter(tasks)
            .map(|task| {
                let client = Arc::clone(&self.http_client);
                let token = cancellation_token.clone();
                let success_count = Arc::clone(&self.success_count);
                let failed_count = Arc::clone(&self.failed_count);
                let downloaded_bytes = Arc::clone(&self.downloaded_bytes);

                async move {
                    // 检查取消
                    if token.is_cancelled() {
                        return Err(anyhow::anyhow!("下载已取消"));
                    }

                    match self
                        .download_single_file(&client, &task, &token)
                        .await
                    {
                        Ok(bytes) => {
                            success_count.fetch_add(1, Ordering::Relaxed);
                            downloaded_bytes.fetch_add(bytes, Ordering::Relaxed);
                            debug!("小文件下载成功: {} ({} bytes)", task.task_id, bytes);
                            Ok(())
                        }
                        Err(e) => {
                            failed_count.fetch_add(1, Ordering::Relaxed);
                            warn!("小文件下载失败: {}, error: {}", task.task_id, e);
                            Err(e)
                        }
                    }
                }
            })
            .buffer_unordered(self.config.max_concurrent_streams) // HTTP/2 并发流
            .collect::<Vec<_>>()
            .await;

        let elapsed = start_time.elapsed();
        let success = self.success_count.load(Ordering::Relaxed);
        let failed = self.failed_count.load(Ordering::Relaxed);
        let bytes = self.downloaded_bytes.load(Ordering::Relaxed);

        // 更新统计
        {
            let mut stats = self.stats.write();
            stats.total_files = total;
            stats.success_count = success;
            stats.failed_count = failed;
            stats.total_bytes = bytes;
            stats.total_time_ms = elapsed.as_millis() as u64;

            if elapsed.as_secs() > 0 {
                stats.avg_speed_kbps = (bytes as f64 / 1024.0) / elapsed.as_secs_f64();
                stats.throughput_fps = success as f64 / elapsed.as_secs_f64();
            }
        }

        info!(
            "批量下载完成: 成功={}, 失败={}, 耗时={:.2}s, 速度={:.2} KB/s, 吞吐={:.2} 文件/秒",
            success,
            failed,
            elapsed.as_secs_f64(),
            (bytes as f64 / 1024.0) / elapsed.as_secs_f64(),
            success as f64 / elapsed.as_secs_f64()
        );

        results
    }

    /// 下载单个文件
    async fn download_single_file(
        &self,
        client: &Client,
        task: &SmallFileTask,
        cancellation_token: &CancellationToken,
    ) -> Result<u64> {
        // 检查取消
        if cancellation_token.is_cancelled() {
            return Err(anyhow::anyhow!("下载已取消"));
        }

        // 创建目录
        if let Some(parent) = Path::new(&task.local_path).parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("创建目录失败")?;
        }

        // 发起请求
        let response = client
            .get(&task.url)
            .header("Cookie", &task.cookie)
            .send()
            .await
            .context("发送请求失败")?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("HTTP 错误: {}", response.status()));
        }

        // 流式写入文件
        let mut file = File::create(&task.local_path)
            .await
            .context("创建文件失败")?;

        let mut stream = response.bytes_stream();
        let mut total_bytes = 0u64;

        while let Some(chunk_result) = stream.next().await {
            // 检查取消
            if cancellation_token.is_cancelled() {
                // 删除部分下载的文件
                let _ = tokio::fs::remove_file(&task.local_path).await;
                return Err(anyhow::anyhow!("下载已取消"));
            }

            let chunk = chunk_result.context("读取数据流失败")?;
            file.write_all(&chunk)
                .await
                .context("写入文件失败")?;

            total_bytes += chunk.len() as u64;
        }

        file.flush().await.context("刷新文件失败")?;

        Ok(total_bytes)
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> SmallFileDownloadStats {
        self.stats.read().clone()
    }

    /// 重置统计信息
    pub fn reset_stats(&self) {
        *self.stats.write() = SmallFileDownloadStats::default();
        self.success_count.store(0, Ordering::Relaxed);
        self.failed_count.store(0, Ordering::Relaxed);
        self.downloaded_bytes.store(0, Ordering::Relaxed);
    }
}

/// 判断是否为小文件
pub fn is_small_file(file_size: u64) -> bool {
    file_size > 0 && file_size <= SMALL_FILE_THRESHOLD
}

/// 将任务列表按文件大小分类
pub fn partition_tasks_by_size<T>(
    tasks: Vec<T>,
    get_size: impl Fn(&T) -> u64,
) -> (Vec<T>, Vec<T>) {
    tasks
        .into_iter()
        .partition(|t| is_small_file(get_size(t)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_small_file() {
        assert!(is_small_file(100));
        assert!(is_small_file(1024 * 1024)); // 1MB
        assert!(!is_small_file(2 * 1024 * 1024)); // 2MB
        assert!(!is_small_file(0));
    }

    #[test]
    fn test_partition_tasks() {
        struct Task {
            size: u64,
        }

        let tasks = vec![
            Task { size: 100 },
            Task { size: 2 * 1024 * 1024 },
            Task { size: 500 * 1024 },
            Task { size: 10 * 1024 * 1024 },
        ];

        let (small, large) = partition_tasks_by_size(tasks, |t| t.size);

        assert_eq!(small.len(), 2); // 100 bytes, 500KB
        assert_eq!(large.len(), 2); // 2MB, 10MB
    }

    #[test]
    fn test_config_default() {
        let config = SmallFileDownloaderConfig::default();
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.max_concurrent_streams, 100);
    }
}
