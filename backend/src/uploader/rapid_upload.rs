// 秒传检查器
//
// 百度网盘秒传原理：
// 1. 计算文件的完整 MD5 (content_md5)
// 2. 计算文件前 256KB 的 MD5 (slice_md5)
// 3. 将这些哈希值发送给服务器
// 4. 如果服务器已有相同文件，则直接完成上传（秒传成功）

use crate::config::VipType;
use crate::uploader::chunk::calculate_recommended_chunk_size;
use anyhow::{Context, Result};
use md5::Context as Md5Context;
use std::io::Read;
use std::path::Path;
use tracing::{debug, info, warn};

/// 秒传所需的哈希信息
#[derive(Debug, Clone)]
pub struct RapidUploadHash {
    /// 文件完整 MD5
    pub content_md5: String,
    /// 文件前 256KB MD5（用于秒传校验）
    pub slice_md5: String,
    /// 文件大小
    pub file_size: u64,
}

/// 秒传检查器
pub struct RapidUploadChecker;

/// 秒传检查结果
#[derive(Debug, Clone)]
pub struct RapidCheckResult {
    /// 文件路径
    pub path: std::path::PathBuf,
    /// 是否可以秒传
    pub can_rapid_upload: bool,
    /// 哈希信息（如果计算成功）
    pub hash: Option<RapidUploadHash>,
    /// 错误信息（如果计算失败）
    pub error: Option<String>,
}

impl RapidUploadChecker {
    /// 前 256KB 的大小常量
    const SLICE_SIZE: usize = 256 * 1024;

    /// 计算文件的秒传哈希值
    ///
    /// # 参数
    /// * `path` - 本地文件路径
    ///
    /// # 返回
    /// 秒传所需的哈希信息
    pub async fn calculate_hash(path: &Path) -> Result<RapidUploadHash> {
        let path = path.to_path_buf();

        // 在阻塞线程池中执行文件 I/O
        tokio::task::spawn_blocking(move || Self::calculate_hash_sync(&path))
            .await
            .context("计算哈希任务执行失败")?
    }

    /// 同步计算文件哈希（内部方法）
    fn calculate_hash_sync(path: &Path) -> Result<RapidUploadHash> {
        use std::fs::File;

        let file = File::open(path).context(format!("无法打开文件: {:?}", path))?;
        let metadata = file.metadata().context("无法获取文件元数据")?;
        let file_size = metadata.len();

        // 使用 BufReader 提高读取效率
        let mut reader = std::io::BufReader::with_capacity(1024 * 1024, file);

        // 1. 计算完整 MD5 和前 256KB MD5
        let mut full_hasher = Md5Context::new();
        let mut slice_hasher = Md5Context::new();
        let mut slice_bytes_read: usize = 0;
        let mut buffer = [0u8; 65536]; // 64KB 缓冲区

        loop {
            let bytes_read = reader.read(&mut buffer).context("读取文件失败")?;
            if bytes_read == 0 {
                break;
            }

            // 更新完整 MD5
            full_hasher.consume(&buffer[..bytes_read]);

            // 更新前 256KB MD5
            if slice_bytes_read < Self::SLICE_SIZE {
                let remaining = Self::SLICE_SIZE - slice_bytes_read;
                let slice_bytes = bytes_read.min(remaining);
                slice_hasher.consume(&buffer[..slice_bytes]);
                slice_bytes_read += slice_bytes;
            }
        }

        let content_md5 = format!("{:x}", full_hasher.compute());
        let slice_md5 = format!("{:x}", slice_hasher.compute());

        debug!(
            "文件哈希计算完成: path={:?}, size={}, content_md5={}, slice_md5={}",
            path, file_size, content_md5, slice_md5
        );

        Ok(RapidUploadHash {
            content_md5,
            slice_md5,
            file_size,
        })
    }

    /// 批量顺序计算文件哈希
    ///
    /// ⚠️ 设计决策：完全顺序处理，无并发
    ///
    /// 为什么不并发？
    /// 1. **磁盘 I/O 特性**：
    ///    - HDD 顺序读: 100-200 MB/s，随机读: 5-10 MB/s（寻道开销致命）
    ///    - SSD 顺序读: 500+ MB/s，并发随机读会让 IO 调度器压力巨大
    /// 2. **秒传场景**：每个文件都要完整读取计算 MD5，并发会让磁盘在多个文件间跳跃
    /// 3. **实测效果**：顺序读取反而比 50 并发快 5-10 倍（HDD 尤其明显）
    ///
    /// # 参数
    /// * `paths` - 文件路径列表
    ///
    /// # 返回
    /// 每个文件的检查结果
    pub async fn batch_calculate_hash(
        paths: Vec<std::path::PathBuf>,
    ) -> Vec<RapidCheckResult> {
        info!(
            "开始批量计算秒传哈希: {} 个文件 (顺序处理，无并发)",
            paths.len()
        );

        let mut results = Vec::with_capacity(paths.len());

        // 完全顺序处理，避免磁盘随机读
        for (index, path) in paths.into_iter().enumerate() {
            debug!(
                "计算文件哈希 {}/{}: {:?}",
                index + 1,
                results.capacity(),
                path
            );

            match Self::calculate_hash(&path).await {
                Ok(hash) => {
                    results.push(RapidCheckResult {
                        path,
                        can_rapid_upload: true,
                        hash: Some(hash),
                        error: None,
                    });
                }
                Err(e) => {
                    warn!("计算文件哈希失败: {:?}, 错误: {}", path, e);
                    results.push(RapidCheckResult {
                        path,
                        can_rapid_upload: false,
                        hash: None,
                        error: Some(e.to_string()),
                    });
                }
            }
        }

        info!(
            "批量哈希计算完成: {}/{} 成功",
            results.iter().filter(|r| r.can_rapid_upload).count(),
            results.len()
        );

        results
    }

    /// 计算文件的 block_list (precreate 接口需要的分片 MD5 数组)
    ///
    /// 分片大小根据 VIP 等级动态计算，和实际上传分片大小保持一致
    ///
    /// # 参数
    /// * `path` - 本地文件路径
    /// * `vip_type` - VIP 类型，用于计算分片大小
    ///
    /// # 返回
    /// JSON 字符串格式的 MD5 数组,例如: ["md5_1", "md5_2", ...]
    pub async fn calculate_block_list(path: &Path, vip_type: VipType) -> Result<String> {
        let path = path.to_path_buf();

        // 在阻塞线程池中执行文件 I/O
        tokio::task::spawn_blocking(move || Self::calculate_block_list_sync(&path, vip_type))
            .await
            .context("计算 block_list 任务执行失败")?
    }

    /// 同步计算 block_list（内部方法）
    fn calculate_block_list_sync(path: &Path, vip_type: VipType) -> Result<String> {
        use std::fs::File;

        let file = File::open(path).context(format!("无法打开文件: {:?}", path))?;
        let metadata = file.metadata().context("无法获取文件元数据")?;
        let file_size = metadata.len();

        // 根据 VIP 等级计算分片大小（和上传分片大小保持一致）
        let chunk_size = calculate_recommended_chunk_size(file_size, vip_type);

        debug!(
            "开始计算 block_list: path={:?}, size={} bytes, chunk_size={} bytes, vip_type={:?}",
            path, file_size, chunk_size, vip_type
        );

        // 文件小于等于分片大小: 直接计算整个文件的 MD5
        if file_size <= chunk_size {
            let mut reader = std::io::BufReader::with_capacity(1024 * 1024, file);
            let mut hasher = Md5Context::new();
            let mut buffer = [0u8; 65536]; // 64KB 缓冲区

            loop {
                let bytes_read = reader.read(&mut buffer).context("读取文件失败")?;
                if bytes_read == 0 {
                    break;
                }
                hasher.consume(&buffer[..bytes_read]);
            }

            let md5 = format!("{:x}", hasher.compute());
            let block_list = vec![md5];
            let json = serde_json::to_string(&block_list)?;

            debug!(
                "block_list 计算完成(单分片): {:?}, block_list={}",
                path, json
            );

            return Ok(json);
        }

        // 文件大于分片大小: 按分片大小切分计算每个分片的 MD5
        let block_count = (file_size + chunk_size - 1) / chunk_size;
        let mut block_md5s = Vec::with_capacity(block_count as usize);

        let mut reader = std::io::BufReader::with_capacity(1024 * 1024, file);
        let mut buffer = [0u8; 65536]; // 64KB 缓冲区

        for block_index in 0..block_count {
            let mut hasher = Md5Context::new();
            let mut bytes_in_block: u64 = 0;
            let block_size = chunk_size.min(file_size - block_index * chunk_size);

            // 读取当前分片
            while bytes_in_block < block_size {
                let remaining = (block_size - bytes_in_block) as usize;
                let to_read = remaining.min(buffer.len());
                let bytes_read = reader
                    .read(&mut buffer[..to_read])
                    .context(format!("读取第 {} 个分片失败", block_index))?;

                if bytes_read == 0 {
                    break; // 文件结束
                }

                hasher.consume(&buffer[..bytes_read]);
                bytes_in_block += bytes_read as u64;
            }

            let md5 = format!("{:x}", hasher.compute());
            block_md5s.push(md5);

            debug!(
                "block_list 分片 {}/{} 完成: {} bytes",
                block_index + 1,
                block_count,
                bytes_in_block
            );
        }

        let json = serde_json::to_string(&block_md5s)?;

        debug!(
            "block_list 计算完成: {:?}, {} 个分片, chunk_size={} bytes, block_list={}",
            path,
            block_md5s.len(),
            chunk_size,
            json
        );

        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_calculate_hash_small_file() {
        // 创建临时文件（小于 256KB）
        let mut temp_file = NamedTempFile::new().unwrap();
        let content = b"Hello, World! This is a test file.";
        temp_file.write_all(content).unwrap();
        temp_file.flush().unwrap();

        let result = RapidUploadChecker::calculate_hash(temp_file.path()).await;
        assert!(result.is_ok());

        let hash = result.unwrap();
        assert_eq!(hash.file_size, content.len() as u64);
        // 对于小于 256KB 的文件，content_md5 和 slice_md5 应该相同
        assert_eq!(hash.content_md5, hash.slice_md5);
    }

    #[tokio::test]
    async fn test_calculate_hash_large_file() {
        // 创建大于 256KB 的临时文件
        let mut temp_file = NamedTempFile::new().unwrap();
        let content = vec![0u8; 512 * 1024]; // 512KB
        temp_file.write_all(&content).unwrap();
        temp_file.flush().unwrap();

        let result = RapidUploadChecker::calculate_hash(temp_file.path()).await;
        assert!(result.is_ok());

        let hash = result.unwrap();
        assert_eq!(hash.file_size, 512 * 1024);
        // 对于大于 256KB 的文件，content_md5 和 slice_md5 应该不同
        assert_ne!(hash.content_md5, hash.slice_md5);
    }

    #[tokio::test]
    async fn test_calculate_hash_nonexistent_file() {
        let result = RapidUploadChecker::calculate_hash(Path::new("/nonexistent/file.txt")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_calculate_hash() {
        // 创建多个临时文件
        let mut temp_files = Vec::new();
        let mut paths = Vec::new();

        for i in 0..5 {
            let mut temp_file = NamedTempFile::new().unwrap();
            let content = format!("File content {}", i);
            temp_file.write_all(content.as_bytes()).unwrap();
            temp_file.flush().unwrap();
            paths.push(temp_file.path().to_path_buf());
            temp_files.push(temp_file);
        }

        let results = RapidUploadChecker::batch_calculate_hash(paths).await;
        assert_eq!(results.len(), 5);

        // 所有文件都应该成功计算哈希
        for result in &results {
            assert!(result.can_rapid_upload);
            assert!(result.hash.is_some());
            assert!(result.error.is_none());
        }
    }

    #[tokio::test]
    async fn test_md5_consistency() {
        // 验证相同内容的 MD5 一致性
        let content = b"Test content for MD5 consistency check";

        let mut temp_file1 = NamedTempFile::new().unwrap();
        temp_file1.write_all(content).unwrap();
        temp_file1.flush().unwrap();

        let mut temp_file2 = NamedTempFile::new().unwrap();
        temp_file2.write_all(content).unwrap();
        temp_file2.flush().unwrap();

        let hash1 = RapidUploadChecker::calculate_hash(temp_file1.path())
            .await
            .unwrap();
        let hash2 = RapidUploadChecker::calculate_hash(temp_file2.path())
            .await
            .unwrap();

        assert_eq!(hash1.content_md5, hash2.content_md5);
        assert_eq!(hash1.slice_md5, hash2.slice_md5);
    }
}
