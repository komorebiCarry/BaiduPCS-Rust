use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::Client;
use std::{ops::Range, path::Path};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use tracing::{debug, info};

/// 默认分片大小: 10MB
pub const DEFAULT_CHUNK_SIZE: u64 = 10 * 1024 * 1024;

/// 分片信息
#[derive(Debug, Clone)]
pub struct Chunk {
    /// 分片索引
    pub index: usize,
    /// 字节范围
    pub range: Range<u64>,
    /// 是否已完成
    pub completed: bool,
    /// 重试次数
    pub retries: u32,
}

impl Chunk {
    pub fn new(index: usize, range: Range<u64>) -> Self {
        Self {
            index,
            range,
            completed: false,
            retries: 0,
        }
    }

    /// 分片大小
    pub fn size(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// 下载分片（流式读取，实时更新进度）
    ///
    /// # 参数
    /// * `referer` - Referer 头（如果存在），用于 Range 请求避免 403 Forbidden
    /// * `progress_callback` - 进度回调函数，参数为新下载的字节数
    pub async fn download<F>(
        &mut self,
        client: &Client,
        cookie: &str,
        referer: Option<&str>,
        url: &str,
        output_path: &Path,
        timeout_secs: u64,
        chunk_thread_id: usize,
        progress_callback: F,
    ) -> Result<u64>
    where
        F: Fn(u64) + Send + Sync,
    {
        let _thread_id = std::thread::current().id();
        let _thread_name = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_string();

        debug!(
            "[分片线程{}] 下载分片 #{}: bytes={}-{}, timeout={}s, referer={:?}",
            chunk_thread_id,
            self.index,
            self.range.start,
            self.range.end - 1,
            timeout_secs,
            referer
        );

        // 记录 URL 和 Cookie 的前 50 个字符用于调试
        info!(
            "[分片线程{}] 分片 #{} 开始请求",
            chunk_thread_id,
            self.index
        );

        // 1. 构建 Range 请求（使用动态超时、Cookie 和 Referer）
        let mut request = client.get(url).header("Cookie", cookie).header(
            "Range",
            format!("bytes={}-{}", self.range.start, self.range.end - 1),
        );

        if let Some(referer_val) = referer {
            info!(
                "[分片线程{}] 分片 #{} 添加 Referer 请求头",
                chunk_thread_id,
                self.index
            );
            request = request.header("Referer", referer_val);
        }

        let resp = request
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .send()
            .await
            .context("发送HTTP请求失败")?;

        // 检查响应状态
        if !resp.status().is_success() && resp.status().as_u16() != 206 {
            anyhow::bail!("HTTP错误: {}", resp.status());
        }

        // 2. 打开文件并定位到起始位置
        let mut file = File::options()
            .write(true)
            .open(output_path)
            .await
            .context("打开输出文件失败")?;

        file.seek(std::io::SeekFrom::Start(self.range.start))
            .await
            .context("文件定位失败")?;

        // 3. 流式读取并写入文件，实时更新进度
        let mut stream = resp.bytes_stream();
        let mut total_bytes_downloaded = 0u64;

        while let Some(chunk_result) = stream.next().await {
            let chunk_data = chunk_result.context("读取数据流失败")?;
            let chunk_len = chunk_data.len() as u64;

            // 写入文件
            file.write_all(&chunk_data)
                .await
                .context("写入文件失败")?;

            total_bytes_downloaded += chunk_len;

            // 🔥 实时更新进度（每读取一块数据就更新）
            progress_callback(chunk_len);

            debug!(
                "[分片线程{}] 分片 #{} 进度: {}/{} bytes ({:.1}%)",
                chunk_thread_id,
                self.index,
                total_bytes_downloaded,
                self.size(),
                (total_bytes_downloaded as f64 / self.size() as f64) * 100.0
            );
        }

        // 4. 刷新文件缓冲
        file.flush().await.context("刷新文件缓冲失败")?;

        self.completed = true;
        debug!(
            "[分片线程{}] 分片 #{} 下载完成，大小: {} bytes",
            chunk_thread_id,
            self.index,
            total_bytes_downloaded
        );

        Ok(total_bytes_downloaded)
    }
}

/// 分片管理器
#[derive(Debug)]
pub struct ChunkManager {
    /// 所有分片
    chunks: Vec<Chunk>,
    /// 文件总大小
    total_size: u64,
    /// 分片大小
    #[allow(dead_code)]
    chunk_size: u64,
}

impl ChunkManager {
    /// 创建新的分片管理器
    pub fn new(total_size: u64, chunk_size: u64) -> Self {
        let chunks = Self::calculate_chunks(total_size, chunk_size);
        info!(
            "创建分片管理器: 文件大小={} bytes, 分片数量={}",
            total_size,
            chunks.len()
        );
        Self {
            chunks,
            total_size,
            chunk_size,
        }
    }

    /// 使用默认分片大小创建
    pub fn with_default_chunk_size(total_size: u64) -> Self {
        Self::new(total_size, DEFAULT_CHUNK_SIZE)
    }

    /// 计算分片
    fn calculate_chunks(total_size: u64, chunk_size: u64) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut index = 0;

        while offset < total_size {
            let end = std::cmp::min(offset + chunk_size, total_size);
            chunks.push(Chunk::new(index, offset..end));
            offset = end;
            index += 1;
        }

        chunks
    }

    /// 获取下一个待下载的分片
    pub fn next_pending(&mut self) -> Option<&mut Chunk> {
        self.chunks.iter_mut().find(|c| !c.completed)
    }

    /// 获取所有分片
    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    /// 获取可变分片引用
    pub fn chunks_mut(&mut self) -> &mut [Chunk] {
        &mut self.chunks
    }

    /// 获取分片数量
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// 获取已完成的分片数量
    pub fn completed_count(&self) -> usize {
        self.chunks.iter().filter(|c| c.completed).count()
    }

    /// 获取已下载的字节数
    pub fn downloaded_bytes(&self) -> u64 {
        self.chunks
            .iter()
            .filter(|c| c.completed)
            .map(|c| c.size())
            .sum()
    }

    /// 计算下载进度
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.downloaded_bytes() as f64 / self.total_size as f64) * 100.0
    }

    /// 是否全部完成
    pub fn is_completed(&self) -> bool {
        self.chunks.iter().all(|c| c.completed)
    }

    /// 标记分片为已完成
    pub fn mark_completed(&mut self, index: usize) {
        if let Some(chunk) = self.chunks.get_mut(index) {
            chunk.completed = true;
        }
    }

    /// 重置所有分片状态
    pub fn reset(&mut self) {
        for chunk in &mut self.chunks {
            chunk.completed = false;
            chunk.retries = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_creation() {
        let chunk = Chunk::new(0, 0..1024);
        assert_eq!(chunk.index, 0);
        assert_eq!(chunk.range.start, 0);
        assert_eq!(chunk.range.end, 1024);
        assert_eq!(chunk.size(), 1024);
        assert!(!chunk.completed);
    }

    #[test]
    fn test_chunk_manager_creation() {
        let manager = ChunkManager::new(100 * 1024 * 1024, 10 * 1024 * 1024);
        assert_eq!(manager.chunk_count(), 10);
        assert_eq!(manager.completed_count(), 0);
        assert_eq!(manager.progress(), 0.0);
    }

    #[test]
    fn test_chunk_calculation() {
        // 测试完整分片
        let manager = ChunkManager::new(100, 10);
        assert_eq!(manager.chunk_count(), 10);
        assert_eq!(manager.chunks[0].range, 0..10);
        assert_eq!(manager.chunks[9].range, 90..100);

        // 测试不完整分片
        let manager = ChunkManager::new(105, 10);
        assert_eq!(manager.chunk_count(), 11);
        assert_eq!(manager.chunks[10].range, 100..105);
        assert_eq!(manager.chunks[10].size(), 5);
    }

    #[test]
    fn test_progress_calculation() {
        let mut manager = ChunkManager::new(1000, 100);
        assert_eq!(manager.progress(), 0.0);

        // 完成前5个分片
        for i in 0..5 {
            manager.mark_completed(i);
        }
        assert_eq!(manager.completed_count(), 5);
        assert_eq!(manager.downloaded_bytes(), 500);
        assert_eq!(manager.progress(), 50.0);

        // 完成所有分片
        for i in 5..10 {
            manager.mark_completed(i);
        }
        assert_eq!(manager.progress(), 100.0);
        assert!(manager.is_completed());
    }

    #[test]
    fn test_next_pending() {
        let mut manager = ChunkManager::new(300, 100);

        let chunk1 = manager.next_pending();
        assert!(chunk1.is_some());
        assert_eq!(chunk1.unwrap().index, 0);

        manager.mark_completed(0);

        let chunk2 = manager.next_pending();
        assert!(chunk2.is_some());
        assert_eq!(chunk2.unwrap().index, 1);
    }

    #[test]
    fn test_reset() {
        let mut manager = ChunkManager::new(300, 100);

        // 完成所有分片
        for i in 0..3 {
            manager.mark_completed(i);
        }
        assert!(manager.is_completed());

        // 重置
        manager.reset();
        assert_eq!(manager.completed_count(), 0);
        assert!(!manager.is_completed());
    }
}
