// 流式扫描模块 - 边扫描边下载
//
// 设计目标：
// 1. 扫描和下载并行，无需等待全部扫描完成
// 2. 降低用户等待时间
// 3. 支持千万级文件场景

use crate::downloader::folder::PendingFile;
use crate::netdisk::NetdiskClient;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// 扫描批次：每批扫描到的文件
#[derive(Debug, Clone)]
pub struct ScanBatch {
    /// 文件列表
    pub files: Vec<PendingFile>,
    /// 扫描进度：已扫描的目录数
    pub scanned_dirs: u64,
    /// 发现的总文件数（累计）
    pub total_files: u64,
    /// 是否为最后一批
    pub is_final: bool,
}

/// 流式扫描配置
#[derive(Debug, Clone)]
pub struct StreamingScanConfig {
    /// 每批文件数量（达到此数量立即发送）
    pub batch_size: usize,
    /// 缓冲区大小（channel buffer）
    pub channel_buffer: usize,
    /// 扫描并发度
    pub scan_concurrency: usize,
}

impl Default for StreamingScanConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,       // 每 100 个文件发送一批
            channel_buffer: 10,    // 最多缓冲 10 批
            scan_concurrency: 8,   // 8 个并发扫描
        }
    }
}

/// 流式扫描器
pub struct StreamingScanner {
    config: StreamingScanConfig,
    sender: mpsc::Sender<ScanBatch>,
    cancel_token: CancellationToken,
}

impl StreamingScanner {
    /// 创建流式扫描器
    ///
    /// 返回扫描器实例和接收端
    pub fn new(
        config: StreamingScanConfig,
        cancel_token: CancellationToken,
    ) -> (Self, mpsc::Receiver<ScanBatch>) {
        let (tx, rx) = mpsc::channel(config.channel_buffer);

        let scanner = Self {
            config,
            sender: tx,
            cancel_token,
        };

        (scanner, rx)
    }

    /// 开始流式扫描
    ///
    /// 边扫描边通过 channel 发送文件批次
    pub async fn scan(
        self,
        remote_root: String,
        netdisk_client: Arc<NetdiskClient>,
        local_base_dir: String,
    ) -> Result<()> {
        info!(
            "🚀 启动流式扫描: remote_root={}, batch_size={}",
            remote_root, self.config.batch_size
        );

        let mut batch_buffer = Vec::with_capacity(self.config.batch_size);
        let mut scanned_dirs = 0u64;
        let mut total_files = 0u64;

        // BFS 扫描目录树
        let dirs = self
            .bfs_scan_dirs(vec![remote_root.clone()], &netdisk_client)
            .await?;

        info!("📁 发现 {} 个目录，开始扫描文件", dirs.len());

        // 遍历每个目录，发现文件立即发送
        for dir_path in dirs {
            if self.cancel_token.is_cancelled() {
                info!("⚠️ 扫描被取消");
                break;
            }

            scanned_dirs += 1;

            // 列出目录内容
            let files = self
                .list_dir_files(&dir_path, &netdisk_client, &remote_root, &local_base_dir)
                .await?;

            debug!(
                "📂 扫描目录 {} 发现 {} 个文件",
                dir_path,
                files.len()
            );

            for file in files {
                batch_buffer.push(file);
                total_files += 1;

                // 达到批次大小，立即发送
                if batch_buffer.len() >= self.config.batch_size {
                    self.send_batch(&mut batch_buffer, scanned_dirs, total_files, false)
                        .await?;
                }
            }
        }

        // 发送剩余文件
        if !batch_buffer.is_empty() || total_files == 0 {
            self.send_batch(&mut batch_buffer, scanned_dirs, total_files, true)
                .await?;
        }

        info!(
            "✅ 流式扫描完成: 扫描 {} 个目录, 发现 {} 个文件",
            scanned_dirs, total_files
        );

        Ok(())
    }

    /// 发送一批文件
    async fn send_batch(
        &self,
        buffer: &mut Vec<PendingFile>,
        scanned_dirs: u64,
        total_files: u64,
        is_final: bool,
    ) -> Result<()> {
        if buffer.is_empty() && !is_final {
            return Ok(());
        }

        let batch = ScanBatch {
            files: std::mem::take(buffer),
            scanned_dirs,
            total_files,
            is_final,
        };

        debug!(
            "📦 发送扫描批次: {} 个文件, 累计 {}, is_final={}",
            batch.files.len(),
            total_files,
            is_final
        );

        self.sender
            .send(batch)
            .await
            .map_err(|_| anyhow!("接收端已关闭"))?;

        Ok(())
    }

    /// BFS 扫描目录树
    ///
    /// 返回按层级排序的所有目录路径
    async fn bfs_scan_dirs(
        &self,
        roots: Vec<String>,
        netdisk_client: &NetdiskClient,
    ) -> Result<Vec<String>> {
        use futures::stream::{self, StreamExt};

        let mut all_dirs = Vec::new();
        let mut current_level = roots;

        while !current_level.is_empty() && !self.cancel_token.is_cancelled() {
            // 并发扫描当前层级的所有目录
            let next_level: Vec<Vec<String>> = stream::iter(current_level.iter())
                .map(|dir_path| async move {
                    match self.list_subdirs(dir_path, netdisk_client).await {
                        Ok(subdirs) => subdirs,
                        Err(e) => {
                            error!("扫描目录 {} 失败: {}", dir_path, e);
                            Vec::new()
                        }
                    }
                })
                .buffer_unordered(self.config.scan_concurrency)
                .collect()
                .await;

            // 收集结果
            all_dirs.extend(current_level);
            current_level = next_level.into_iter().flatten().collect();
        }

        Ok(all_dirs)
    }

    async fn list_all_items(&self, dir_path: &str, client: &NetdiskClient) -> Result<Vec<crate::netdisk::types::FileItem>> {
        let mut items = Vec::new();
        let mut page = 1;
        loop {
            let response = client.get_file_list(dir_path, page, 100).await?;
            let done = response.list.len() < 100;
            items.extend(response.list);
            if done { break; }
            page += 1;
        }
        Ok(items)
    }

    /// 列出目录下的子目录
    async fn list_subdirs(
        &self,
        dir_path: &str,
        netdisk_client: &NetdiskClient,
    ) -> Result<Vec<String>> {
        let items = self.list_all_items(dir_path, netdisk_client).await?;

        let subdirs: Vec<String> = items
            .into_iter()
            .filter(|item| item.isdir == 1)
            .map(|item| item.path)
            .collect();

        Ok(subdirs)
    }

    /// 列出目录下的文件
    async fn list_dir_files(
        &self,
        dir_path: &str,
        netdisk_client: &NetdiskClient,
        remote_root: &str,
        local_base_dir: &str,
    ) -> Result<Vec<PendingFile>> {
        let items = self.list_all_items(dir_path, netdisk_client).await?;

        let files: Vec<PendingFile> = items
            .into_iter()
            .filter(|item| item.isdir == 0)
            .map(|item| {
                // 计算相对路径
                let relative_path = item
                    .path
                    .strip_prefix(remote_root)
                    .unwrap_or(&item.path)
                    .trim_start_matches('/')
                    .to_string();

                PendingFile {
                    fs_id: item.fs_id,
                    filename: item.server_filename.clone(),
                    remote_path: item.path,
                    relative_path,
                    size: item.size,
                }
            })
            .collect();

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_scan_config_default() {
        let config = StreamingScanConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.channel_buffer, 10);
        assert_eq!(config.scan_concurrency, 8);
    }

    #[tokio::test]
    async fn test_scan_batch_creation() {
        let batch = ScanBatch {
            files: vec![],
            scanned_dirs: 10,
            total_files: 100,
            is_final: false,
        };

        assert_eq!(batch.scanned_dirs, 10);
        assert_eq!(batch.total_files, 100);
        assert!(!batch.is_final);
    }
}
