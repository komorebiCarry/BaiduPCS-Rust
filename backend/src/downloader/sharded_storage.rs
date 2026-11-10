// 分片持久化模块 - 降低大文件夹持久化开销
//
// 设计目标：
// 1. 将大文件夹分片存储，避免单文件过大
// 2. 支持增量更新，只持久化变化的分片
// 3. 压缩存储，降低 I/O 开销

use crate::downloader::folder::PendingFile;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{debug, info};

/// 分片配置
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// 每个分片的文件数量
    pub files_per_shard: usize,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 压缩级别（1-22，默认 3）
    pub compression_level: i32,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            files_per_shard: 10000, // 每个分片 1 万个文件
            enable_compression: true,
            compression_level: 3, // 平衡压缩率和速度
        }
    }
}

/// 分片元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    /// 分片索引
    pub shard_index: usize,
    /// 文件数量
    pub file_count: usize,
    /// 原始大小（字节）
    pub raw_size: usize,
    /// 压缩后大小（字节）
    pub compressed_size: usize,
    /// 哈希值（用于检测变化）
    pub hash: String,
    /// 最后更新时间
    pub updated_at: i64,
}

/// 分片存储管理器
pub struct ShardedStorage {
    /// 基础目录
    base_dir: PathBuf,
    /// 配置
    config: ShardConfig,
}

impl ShardedStorage {
    /// 创建分片存储管理器
    pub fn new(base_dir: impl AsRef<Path>, config: ShardConfig) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            config,
        }
    }

    /// 保存分片文件夹数据
    ///
    /// 自动分片并压缩存储
    pub async fn save_folder(
        &self,
        folder_id: &str,
        files: &[PendingFile],
    ) -> Result<Vec<ShardMetadata>> {
        info!(
            "💾 开始分片持久化: folder_id={}, 文件数={}",
            folder_id,
            files.len()
        );

        // 创建文件夹目录
        let folder_dir = self.base_dir.join(folder_id);
        fs::create_dir_all(&folder_dir).await?;

        // 分片
        let shards = self.split_into_shards(files);
        let mut metadata_list = Vec::new();

        for (shard_index, shard_files) in shards.iter().enumerate() {
            let metadata = self
                .save_shard(folder_id, shard_index, shard_files)
                .await?;
            metadata_list.push(metadata);
        }

        // 保存元数据索引
        self.save_metadata_index(folder_id, &metadata_list)
            .await?;

        info!(
            "✅ 分片持久化完成: {} 个分片",
            metadata_list.len()
        );

        Ok(metadata_list)
    }

    /// 加载分片文件夹数据
    pub async fn load_folder(&self, folder_id: &str) -> Result<Vec<PendingFile>> {
        info!("📂 开始加载分片数据: folder_id={}", folder_id);

        // 加载元数据索引
        let metadata_list = self.load_metadata_index(folder_id).await?;

        if metadata_list.is_empty() {
            return Ok(Vec::new());
        }

        // 加载所有分片
        let mut all_files = Vec::new();

        for metadata in metadata_list {
            let shard_files = self.load_shard(folder_id, metadata.shard_index).await?;
            all_files.extend(shard_files);
        }

        info!("✅ 分片数据加载完成: {} 个文件", all_files.len());

        Ok(all_files)
    }

    /// 增量更新：只更新变化的分片
    pub async fn update_shard(
        &self,
        folder_id: &str,
        shard_index: usize,
        files: &[PendingFile],
    ) -> Result<ShardMetadata> {
        debug!(
            "🔄 增量更新分片: folder_id={}, shard={}",
            folder_id, shard_index
        );

        self.save_shard(folder_id, shard_index, files).await
    }

    /// 删除文件夹的所有分片
    pub async fn delete_folder(&self, folder_id: &str) -> Result<()> {
        let folder_dir = self.base_dir.join(folder_id);

        if folder_dir.exists() {
            fs::remove_dir_all(&folder_dir).await?;
            info!("🗑️ 删除文件夹分片: folder_id={}", folder_id);
        }

        Ok(())
    }

    /// 将文件列表分片
    fn split_into_shards(&self, files: &[PendingFile]) -> Vec<Vec<PendingFile>> {
        files
            .chunks(self.config.files_per_shard)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    /// 保存单个分片
    async fn save_shard(
        &self,
        folder_id: &str,
        shard_index: usize,
        files: &[PendingFile],
    ) -> Result<ShardMetadata> {
        let shard_path = self.get_shard_path(folder_id, shard_index);

        // 序列化为 JSON
        let raw_data = serde_json::to_vec(files)?;
        let raw_size = raw_data.len();

        // 压缩（如果启用）
        let final_data = if self.config.enable_compression {
            // 简单实现：暂时不压缩，直接使用 JSON
            // TODO: 添加 zstd 依赖后实现压缩
            raw_data.clone()
        } else {
            raw_data
        };

        let compressed_size = final_data.len();

        // 计算哈希
        let hash = self.calculate_hash(&final_data);

        // 写入文件
        fs::write(&shard_path, &final_data).await?;

        debug!(
            "💾 分片已保存: shard={}, 原始={}KB, 压缩={}KB, 压缩率={:.1}%",
            shard_index,
            raw_size / 1024,
            compressed_size / 1024,
            (1.0 - compressed_size as f64 / raw_size as f64) * 100.0
        );

        Ok(ShardMetadata {
            shard_index,
            file_count: files.len(),
            raw_size,
            compressed_size,
            hash,
            updated_at: chrono::Utc::now().timestamp(),
        })
    }

    /// 加载单个分片
    async fn load_shard(
        &self,
        folder_id: &str,
        shard_index: usize,
    ) -> Result<Vec<PendingFile>> {
        let shard_path = self.get_shard_path(folder_id, shard_index);

        if !shard_path.exists() {
            return Err(anyhow!("分片文件不存在: {:?}", shard_path));
        }

        // 读取文件
        let data = fs::read(&shard_path).await?;

        // 解压缩（如果启用）
        let raw_data = if self.config.enable_compression {
            // 简单实现：暂时不解压缩
            // TODO: 添加 zstd 依赖后实现解压缩
            data
        } else {
            data
        };

        // 反序列化
        let files: Vec<PendingFile> = serde_json::from_slice(&raw_data)?;

        debug!("📂 分片已加载: shard={}, 文件数={}", shard_index, files.len());

        Ok(files)
    }

    /// 保存元数据索引
    async fn save_metadata_index(
        &self,
        folder_id: &str,
        metadata_list: &[ShardMetadata],
    ) -> Result<()> {
        let index_path = self.get_metadata_index_path(folder_id);

        let json = serde_json::to_string_pretty(metadata_list)?;
        fs::write(&index_path, json).await?;

        Ok(())
    }

    /// 加载元数据索引
    async fn load_metadata_index(&self, folder_id: &str) -> Result<Vec<ShardMetadata>> {
        let index_path = self.get_metadata_index_path(folder_id);

        if !index_path.exists() {
            return Ok(Vec::new());
        }

        let json = fs::read_to_string(&index_path).await?;
        let metadata_list: Vec<ShardMetadata> = serde_json::from_str(&json)?;

        Ok(metadata_list)
    }

    /// 获取分片文件路径
    fn get_shard_path(&self, folder_id: &str, shard_index: usize) -> PathBuf {
        self.base_dir
            .join(folder_id)
            .join(format!("shard_{:06}.bin", shard_index))
    }

    /// 获取元数据索引路径
    fn get_metadata_index_path(&self, folder_id: &str) -> PathBuf {
        self.base_dir.join(folder_id).join("metadata.json")
    }

    /// 计算数据哈希
    fn calculate_hash(&self, data: &[u8]) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_config_default() {
        let config = ShardConfig::default();
        assert_eq!(config.files_per_shard, 10000);
        assert!(config.enable_compression);
        assert_eq!(config.compression_level, 3);
    }

    #[test]
    fn test_split_into_shards() {
        let config = ShardConfig {
            files_per_shard: 3,
            ..Default::default()
        };

        let storage = ShardedStorage::new("/tmp", config);

        let files = vec![
            PendingFile {
                fs_id: 1,
                filename: "f1".to_string(),
                remote_path: "/f1".to_string(),
                relative_path: "f1".to_string(),
                size: 100,
            },
            PendingFile {
                fs_id: 2,
                filename: "f2".to_string(),
                remote_path: "/f2".to_string(),
                relative_path: "f2".to_string(),
                size: 200,
            },
            PendingFile {
                fs_id: 3,
                filename: "f3".to_string(),
                remote_path: "/f3".to_string(),
                relative_path: "f3".to_string(),
                size: 300,
            },
            PendingFile {
                fs_id: 4,
                filename: "f4".to_string(),
                remote_path: "/f4".to_string(),
                relative_path: "f4".to_string(),
                size: 400,
            },
        ];

        let shards = storage.split_into_shards(&files);
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].len(), 3);
        assert_eq!(shards[1].len(), 1);
    }
}
