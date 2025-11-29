// 上传分片管理
//
// 复用下载模块的 ChunkManager 设计模式
//
// 百度网盘上传分片规则：
// - 文件 <= 4MB：无需分片，直接上传
// - 普通用户：单个分片固定 4MB，单文件上限 4GB
// - 普通会员：单个分片上限 16MB，单文件上限 10GB
// - 超级会员：单个分片上限 32MB，单文件上限 20GB

use crate::config::VipType;
use anyhow::{Context, Result};
use std::ops::Range;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, info};

/// 默认上传分片大小: 4MB（百度网盘要求最小 4MB）
pub const DEFAULT_UPLOAD_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// 最小上传分片大小: 4MB
pub const MIN_UPLOAD_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// 最大上传分片大小: 32MB（超级会员限制）
pub const MAX_UPLOAD_CHUNK_SIZE: u64 = 32 * 1024 * 1024;

/// 普通用户分片大小上限: 4MB（固定）
pub const NORMAL_USER_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// 普通会员分片大小上限: 16MB
pub const VIP_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// 超级会员分片大小上限: 32MB
pub const SVIP_CHUNK_SIZE: u64 = 32 * 1024 * 1024;

/// 普通用户单文件大小上限: 4GB
pub const NORMAL_USER_FILE_SIZE_LIMIT: u64 = 4 * 1024 * 1024 * 1024;

/// 普通会员单文件大小上限: 10GB
pub const VIP_FILE_SIZE_LIMIT: u64 = 10 * 1024 * 1024 * 1024;

/// 超级会员单文件大小上限: 20GB
pub const SVIP_FILE_SIZE_LIMIT: u64 = 20 * 1024 * 1024 * 1024;

/// 根据 VIP 类型获取分片大小上限
pub fn get_chunk_size_limit(vip_type: VipType) -> u64 {
    match vip_type {
        VipType::Normal => NORMAL_USER_CHUNK_SIZE,
        VipType::Vip => VIP_CHUNK_SIZE,
        VipType::Svip => SVIP_CHUNK_SIZE,
    }
}

/// 根据 VIP 类型获取文件大小上限
pub fn get_file_size_limit(vip_type: VipType) -> u64 {
    match vip_type {
        VipType::Normal => NORMAL_USER_FILE_SIZE_LIMIT,
        VipType::Vip => VIP_FILE_SIZE_LIMIT,
        VipType::Svip => SVIP_FILE_SIZE_LIMIT,
    }
}

/// 计算推荐的分片大小
///
/// # 参数
/// * `file_size` - 文件大小
/// * `vip_type` - VIP 类型
///
/// # 返回
/// 推荐的分片大小
pub fn calculate_recommended_chunk_size(file_size: u64, vip_type: VipType) -> u64 {
    // 文件 <= 4MB，无需分片（返回文件大小作为单一分片）
    if file_size <= MIN_UPLOAD_CHUNK_SIZE {
        return file_size;
    }

    let max_chunk_size = get_chunk_size_limit(vip_type);

    // 普通用户固定 4MB 分片
    if vip_type == VipType::Normal {
        return NORMAL_USER_CHUNK_SIZE;
    }

    // 会员用户：根据文件大小选择合适的分片大小
    // 目标：分片数量在 100-200 之间，避免过多或过少
    let target_chunks_min = 150u64;
    let target_chunks_max = 300u64;

    // 计算理想分片大小
    let ideal_chunk_size = file_size / target_chunks_min;

    // 限制在允许范围内
    let chunk_size = ideal_chunk_size
        .max(MIN_UPLOAD_CHUNK_SIZE)
        .min(max_chunk_size);

    // 如果分片数量太多，使用最大分片大小
    let estimated_chunks = file_size.div_ceil(chunk_size);
    if estimated_chunks > target_chunks_max {
        max_chunk_size
    } else {
        chunk_size
    }
}

/// 上传分片信息
#[derive(Debug, Clone)]
pub struct UploadChunk {
    /// 分片索引
    pub index: usize,
    /// 字节范围
    pub range: Range<u64>,
    /// 是否已完成
    pub completed: bool,
    /// 是否正在上传（防止重复调度）
    pub uploading: bool,
    /// 重试次数
    pub retries: u32,
    /// 分片 MD5（上传后由服务器返回）
    pub md5: Option<String>,
}

impl UploadChunk {
    pub fn new(index: usize, range: Range<u64>) -> Self {
        Self {
            index,
            range,
            completed: false,
            uploading: false,
            retries: 0,
            md5: None,
        }
    }

    /// 分片大小
    pub fn size(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// 读取分片数据
    ///
    /// # 参数
    /// * `file_path` - 本地文件路径
    ///
    /// # 返回
    /// 分片数据字节数组
    pub async fn read_data(&self, file_path: &Path) -> Result<Vec<u8>> {
        let mut file = File::open(file_path)
            .await
            .context("打开上传文件失败")?;

        // 定位到分片起始位置
        file.seek(std::io::SeekFrom::Start(self.range.start))
            .await
            .context("文件定位失败")?;

        // 读取分片数据
        let chunk_size = self.size() as usize;
        let mut buffer = vec![0u8; chunk_size];
        let bytes_read = file.read_exact(&mut buffer)
            .await
            .context("读取分片数据失败")?;

        debug!(
            "读取分片 #{}: bytes={}-{}, 大小={} bytes",
            self.index,
            self.range.start,
            self.range.end - 1,
            bytes_read
        );

        Ok(buffer)
    }
}

/// 上传分片管理器
#[derive(Debug)]
pub struct UploadChunkManager {
    /// 所有分片
    chunks: Vec<UploadChunk>,
    /// 文件总大小
    total_size: u64,
    /// 分片大小
    #[allow(dead_code)]
    chunk_size: u64,
}

impl UploadChunkManager {
    /// 创建新的上传分片管理器
    ///
    /// # 参数
    /// * `total_size` - 文件总大小
    /// * `chunk_size` - 分片大小（会自动限制在 4MB-32MB 范围内）
    pub fn new(total_size: u64, chunk_size: u64) -> Self {
        // 特殊处理：文件 <= 4MB 时，无需分片
        let (chunk_size, chunks) = if total_size <= MIN_UPLOAD_CHUNK_SIZE {
            // 小文件：单一分片
            let chunks = vec![UploadChunk::new(0, 0..total_size)];
            (total_size, chunks)
        } else {
            // 确保分片大小在有效范围内
            let chunk_size = chunk_size.clamp(MIN_UPLOAD_CHUNK_SIZE, MAX_UPLOAD_CHUNK_SIZE);
            let chunks = Self::calculate_chunks(total_size, chunk_size);
            (chunk_size, chunks)
        };

        info!(
            "创建上传分片管理器: 文件大小={} bytes, 分片大小={} bytes, 分片数量={}",
            total_size,
            chunk_size,
            chunks.len()
        );
        Self {
            chunks,
            total_size,
            chunk_size,
        }
    }

    /// 根据 VIP 类型创建分片管理器
    ///
    /// 自动计算推荐的分片大小
    ///
    /// # 参数
    /// * `total_size` - 文件总大小
    /// * `vip_type` - VIP 类型
    pub fn with_vip_type(total_size: u64, vip_type: VipType) -> Self {
        let chunk_size = calculate_recommended_chunk_size(total_size, vip_type);
        Self::new(total_size, chunk_size)
    }

    /// 使用默认分片大小创建（普通用户 4MB）
    pub fn with_default_chunk_size(total_size: u64) -> Self {
        Self::new(total_size, DEFAULT_UPLOAD_CHUNK_SIZE)
    }

    /// 计算分片
    fn calculate_chunks(total_size: u64, chunk_size: u64) -> Vec<UploadChunk> {
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut index = 0;

        while offset < total_size {
            let end = std::cmp::min(offset + chunk_size, total_size);
            chunks.push(UploadChunk::new(index, offset..end));
            offset = end;
            index += 1;
        }

        chunks
    }

    /// 获取下一个待上传的分片
    pub fn next_pending(&mut self) -> Option<&mut UploadChunk> {
        self.chunks.iter_mut().find(|c| !c.completed && !c.uploading)
    }

    /// 获取所有分片
    pub fn chunks(&self) -> &[UploadChunk] {
        &self.chunks
    }

    /// 获取可变分片引用
    pub fn chunks_mut(&mut self) -> &mut [UploadChunk] {
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

    /// 获取已上传的字节数
    pub fn uploaded_bytes(&self) -> u64 {
        self.chunks
            .iter()
            .filter(|c| c.completed)
            .map(|c| c.size())
            .sum()
    }

    /// 计算上传进度
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.uploaded_bytes() as f64 / self.total_size as f64) * 100.0
    }

    /// 是否全部完成
    pub fn is_completed(&self) -> bool {
        self.chunks.iter().all(|c| c.completed)
    }

    /// 标记分片为已完成
    pub fn mark_completed(&mut self, index: usize, md5: Option<String>) {
        if let Some(chunk) = self.chunks.get_mut(index) {
            chunk.completed = true;
            chunk.uploading = false;
            chunk.md5 = md5;
        }
    }

    /// 标记分片正在上传（防止重复调度）
    pub fn mark_uploading(&mut self, index: usize) {
        if let Some(chunk) = self.chunks.get_mut(index) {
            chunk.uploading = true;
        }
    }

    /// 取消分片上传标记（上传失败时调用）
    pub fn unmark_uploading(&mut self, index: usize) {
        if let Some(chunk) = self.chunks.get_mut(index) {
            chunk.uploading = false;
        }
    }

    /// 增加分片重试次数
    pub fn increment_retry(&mut self, index: usize) -> u32 {
        if let Some(chunk) = self.chunks.get_mut(index) {
            chunk.retries += 1;
            chunk.retries
        } else {
            0
        }
    }

    /// 重置所有分片状态
    pub fn reset(&mut self) {
        for chunk in &mut self.chunks {
            chunk.completed = false;
            chunk.uploading = false;
            chunk.retries = 0;
            chunk.md5 = None;
        }
    }

    /// 获取所有分片的 MD5 列表（用于 createfile 接口）
    pub fn get_block_list(&self) -> Vec<String> {
        self.chunks
            .iter()
            .filter_map(|c| c.md5.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_creation() {
        let chunk = UploadChunk::new(0, 0..1024);
        assert_eq!(chunk.index, 0);
        assert_eq!(chunk.range.start, 0);
        assert_eq!(chunk.range.end, 1024);
        assert_eq!(chunk.size(), 1024);
        assert!(!chunk.completed);
        assert!(!chunk.uploading);
    }

    #[test]
    fn test_chunk_manager_creation() {
        // 100MB 文件，默认 4MB 分片
        let manager = UploadChunkManager::new(100 * 1024 * 1024, DEFAULT_UPLOAD_CHUNK_SIZE);
        assert_eq!(manager.chunk_count(), 25);
        assert_eq!(manager.completed_count(), 0);
        assert_eq!(manager.progress(), 0.0);
    }

    #[test]
    fn test_chunk_calculation() {
        // 测试完整分片
        let manager = UploadChunkManager::new(16 * 1024 * 1024, 4 * 1024 * 1024);
        assert_eq!(manager.chunk_count(), 4);
        assert_eq!(manager.chunks[0].range, 0..(4 * 1024 * 1024));
        assert_eq!(manager.chunks[3].range, (12 * 1024 * 1024)..(16 * 1024 * 1024));

        // 测试不完整分片
        let manager = UploadChunkManager::new(17 * 1024 * 1024, 4 * 1024 * 1024);
        assert_eq!(manager.chunk_count(), 5);
        assert_eq!(manager.chunks[4].range, (16 * 1024 * 1024)..(17 * 1024 * 1024));
        assert_eq!(manager.chunks[4].size(), 1024 * 1024);
    }

    #[test]
    fn test_progress_calculation() {
        let mut manager = UploadChunkManager::new(16 * 1024 * 1024, 4 * 1024 * 1024);
        assert_eq!(manager.progress(), 0.0);

        // 完成前2个分片
        manager.mark_completed(0, Some("md5_0".to_string()));
        manager.mark_completed(1, Some("md5_1".to_string()));
        assert_eq!(manager.completed_count(), 2);
        assert_eq!(manager.uploaded_bytes(), 8 * 1024 * 1024);
        assert_eq!(manager.progress(), 50.0);

        // 完成所有分片
        manager.mark_completed(2, Some("md5_2".to_string()));
        manager.mark_completed(3, Some("md5_3".to_string()));
        assert_eq!(manager.progress(), 100.0);
        assert!(manager.is_completed());
    }

    #[test]
    fn test_next_pending() {
        let mut manager = UploadChunkManager::new(16 * 1024 * 1024, 4 * 1024 * 1024);

        let chunk1 = manager.next_pending();
        assert!(chunk1.is_some());
        assert_eq!(chunk1.unwrap().index, 0);

        manager.mark_completed(0, None);

        let chunk2 = manager.next_pending();
        assert!(chunk2.is_some());
        assert_eq!(chunk2.unwrap().index, 1);
    }

    #[test]
    fn test_uploading_mark() {
        let mut manager = UploadChunkManager::new(16 * 1024 * 1024, 4 * 1024 * 1024);

        // 标记第一个分片正在上传
        manager.mark_uploading(0);

        // next_pending 应该跳过正在上传的分片
        let chunk = manager.next_pending();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().index, 1);

        // 取消上传标记后应该可以再次选择
        manager.unmark_uploading(0);
        let chunk = manager.next_pending();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().index, 0);
    }

    #[test]
    fn test_block_list() {
        let mut manager = UploadChunkManager::new(16 * 1024 * 1024, 4 * 1024 * 1024);

        manager.mark_completed(0, Some("md5_0".to_string()));
        manager.mark_completed(1, Some("md5_1".to_string()));
        manager.mark_completed(2, Some("md5_2".to_string()));
        manager.mark_completed(3, Some("md5_3".to_string()));

        let block_list = manager.get_block_list();
        assert_eq!(block_list.len(), 4);
        assert_eq!(block_list[0], "md5_0");
        assert_eq!(block_list[3], "md5_3");
    }

    #[test]
    fn test_reset() {
        let mut manager = UploadChunkManager::new(16 * 1024 * 1024, 4 * 1024 * 1024);

        // 完成所有分片
        for i in 0..4 {
            manager.mark_completed(i, Some(format!("md5_{}", i)));
        }
        assert!(manager.is_completed());

        // 重置
        manager.reset();
        assert_eq!(manager.completed_count(), 0);
        assert!(!manager.is_completed());
        assert!(manager.get_block_list().is_empty());
    }

    #[test]
    fn test_chunk_size_clamping() {
        // 测试分片大小限制（太小）
        let manager = UploadChunkManager::new(100 * 1024 * 1024, 1 * 1024 * 1024);
        // 应该被限制到最小 4MB
        assert_eq!(manager.chunk_count(), 25);

        // 测试分片大小限制（太大）
        let manager = UploadChunkManager::new(100 * 1024 * 1024, 64 * 1024 * 1024);
        // 应该被限制到最大 32MB
        assert_eq!(manager.chunk_count(), 4);
    }

    #[test]
    fn test_small_file_no_chunking() {
        // 小于 4MB 的文件不需要分片
        let file_size = 2 * 1024 * 1024; // 2MB
        let manager = UploadChunkManager::new(file_size, DEFAULT_UPLOAD_CHUNK_SIZE);

        // 应该只有一个分片
        assert_eq!(manager.chunk_count(), 1);
        assert_eq!(manager.chunks[0].range, 0..file_size);
    }

    #[test]
    fn test_vip_type_chunk_sizes() {
        let file_size = 100 * 1024 * 1024; // 100MB

        // 普通用户：固定 4MB 分片
        let manager = UploadChunkManager::with_vip_type(file_size, VipType::Normal);
        assert_eq!(manager.chunk_count(), 25); // 100MB / 4MB = 25

        // 普通会员：最大 16MB 分片
        let manager = UploadChunkManager::with_vip_type(file_size, VipType::Vip);
        // 100MB / 10 = 10MB，但上限 16MB，所以用 10MB，结果 10 个分片
        assert!(manager.chunk_count() <= 25);

        // 超级会员：最大 32MB 分片
        let manager = UploadChunkManager::with_vip_type(file_size, VipType::Svip);
        assert!(manager.chunk_count() <= 25);
    }

    #[test]
    fn test_vip_chunk_size_limits() {
        // 普通用户
        assert_eq!(get_chunk_size_limit(VipType::Normal), 4 * 1024 * 1024);
        // 普通会员
        assert_eq!(get_chunk_size_limit(VipType::Vip), 16 * 1024 * 1024);
        // 超级会员
        assert_eq!(get_chunk_size_limit(VipType::Svip), 32 * 1024 * 1024);
    }

    #[test]
    fn test_vip_file_size_limits() {
        // 普通用户 4GB
        assert_eq!(get_file_size_limit(VipType::Normal), 4 * 1024 * 1024 * 1024);
        // 普通会员 10GB
        assert_eq!(get_file_size_limit(VipType::Vip), 10 * 1024 * 1024 * 1024);
        // 超级会员 20GB
        assert_eq!(get_file_size_limit(VipType::Svip), 20 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_recommended_chunk_size() {
        // 小文件
        let small_file = 2 * 1024 * 1024;
        assert_eq!(
            calculate_recommended_chunk_size(small_file, VipType::Normal),
            small_file
        );

        // 普通用户：固定 4MB
        let medium_file = 50 * 1024 * 1024;
        assert_eq!(
            calculate_recommended_chunk_size(medium_file, VipType::Normal),
            4 * 1024 * 1024
        );

        // 超级会员大文件
        let large_file = 5 * 1024 * 1024 * 1024; // 5GB
        let chunk_size = calculate_recommended_chunk_size(large_file, VipType::Svip);
        // 应该使用最大分片大小
        assert_eq!(chunk_size, 32 * 1024 * 1024);
    }
}
