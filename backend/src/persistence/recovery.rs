//! 任务恢复模块
//!
//! 实现启动时的任务恢复功能，包括：
//! - 扫描可恢复的任务
//! - 恢复下载/上传/转存任务
//! - 清理过期的 WAL 文件
//!
//! ## 恢复流程
//!
//! 1. 扫描 WAL 目录中的元数据文件
//! 2. 解析元数据和 WAL 记录
//! 3. 验证本地文件状态
//! 4. 创建恢复任务信息
//! 5. 由各管理器负责实际恢复

use std::path::{Path, PathBuf};

use bit_set::BitSet;
use chrono::{Duration, Utc};
use tracing::{debug, error, info, warn};

use super::metadata::{delete_task_files, scan_all_metadata};
use super::types::{TaskMetadata, TaskType};
use super::wal::read_records;

/// 恢复的任务信息
///
/// 包含从持久化文件恢复的任务状态，供管理器使用
#[derive(Debug, Clone)]
pub struct RecoveredTask {
    /// 任务元数据
    pub metadata: TaskMetadata,

    /// 已完成的分片集合（从 WAL 恢复）
    pub completed_chunks: BitSet,

    /// 分片 MD5 列表（仅上传任务，从 WAL 恢复）
    pub chunk_md5s: Option<Vec<Option<String>>>,
}

impl RecoveredTask {
    /// 获取已完成的分片数
    pub fn completed_count(&self) -> usize {
        self.completed_chunks.len()
    }

    /// 获取总分片数
    pub fn total_chunks(&self) -> usize {
        self.metadata.total_chunks.unwrap_or(0)
    }

    /// 获取未完成的分片索引列表
    pub fn pending_chunks(&self) -> Vec<usize> {
        let total = self.total_chunks();
        (0..total)
            .filter(|&i| !self.completed_chunks.contains(i))
            .collect()
    }

    /// 检查是否已完成所有分片
    pub fn is_all_completed(&self) -> bool {
        let total = self.total_chunks();
        total > 0 && self.completed_count() >= total
    }

    /// 获取任务类型
    pub fn task_type(&self) -> TaskType {
        self.metadata.task_type
    }

    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        &self.metadata.task_id
    }
}

/// 恢复扫描结果
#[derive(Debug, Default)]
pub struct RecoveryScanResult {
    /// 可恢复的下载任务
    pub download_tasks: Vec<RecoveredTask>,

    /// 可恢复的上传任务
    pub upload_tasks: Vec<RecoveredTask>,

    /// 可恢复的转存任务
    pub transfer_tasks: Vec<RecoveredTask>,

    /// 已完成的任务（需要清理）
    pub completed_tasks: Vec<String>,

    /// 无效的任务（文件损坏等，需要清理）
    pub invalid_tasks: Vec<String>,
}

impl RecoveryScanResult {
    /// 获取总可恢复任务数
    pub fn total_recoverable(&self) -> usize {
        self.download_tasks.len() + self.upload_tasks.len() + self.transfer_tasks.len()
    }

    /// 是否有可恢复的任务
    pub fn has_recoverable(&self) -> bool {
        self.total_recoverable() > 0
    }
}

/// 扫描所有可恢复的任务
///
/// 遍历 WAL 目录，读取元数据和 WAL 记录，返回可恢复的任务列表
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
///
/// # Returns
/// 恢复扫描结果，包含各类型可恢复任务和需要清理的任务
pub fn scan_recoverable_tasks(wal_dir: &Path) -> std::io::Result<RecoveryScanResult> {
    info!("开始扫描可恢复的任务: {:?}", wal_dir);

    let mut result = RecoveryScanResult::default();

    // 扫描所有元数据文件
    let metadata_list = scan_all_metadata(wal_dir)?;

    if metadata_list.is_empty() {
        info!("未找到可恢复的任务");
        return Ok(result);
    }

    info!("找到 {} 个元数据文件，开始解析", metadata_list.len());

    for metadata in metadata_list {
        let task_id = &metadata.task_id;

        // 读取 WAL 记录
        let records = match read_records(wal_dir, task_id) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // WAL 文件不存在，可能是新创建的任务或已完成
                Vec::new()
            }
            Err(e) => {
                warn!("读取 WAL 文件失败 (task_id={}): {}", task_id, e);
                result.invalid_tasks.push(task_id.clone());
                continue;
            }
        };

        // 构建已完成分片集合
        let total_chunks = metadata.total_chunks.unwrap_or(0);
        let mut completed_chunks = BitSet::with_capacity(total_chunks);
        let mut chunk_md5s: Option<Vec<Option<String>>> = if metadata.task_type == TaskType::Upload
        {
            Some(vec![None; total_chunks])
        } else {
            None
        };

        for record in &records {
            completed_chunks.insert(record.chunk_index);

            // 保存上传任务的 MD5
            if let Some(ref mut md5s) = chunk_md5s {
                if record.chunk_index < md5s.len() {
                    md5s[record.chunk_index] = record.md5.clone();
                }
            }
        }

        let recovered = RecoveredTask {
            metadata: metadata.clone(),
            completed_chunks,
            chunk_md5s,
        };

        // 检查是否已完成所有分片
        if recovered.is_all_completed() {
            debug!("任务 {} 已完成所有分片，标记为需要清理", task_id);
            result.completed_tasks.push(task_id.clone());
            continue;
        }

        // 验证任务有效性
        match metadata.task_type {
            TaskType::Download => {
                if let Err(e) = validate_download_task(&recovered) {
                    warn!("下载任务 {} 验证失败: {}", task_id, e);
                    result.invalid_tasks.push(task_id.clone());
                    continue;
                }
                result.download_tasks.push(recovered);
            }
            TaskType::Upload => {
                if let Err(e) = validate_upload_task(&recovered) {
                    warn!("上传任务 {} 验证失败: {}", task_id, e);
                    result.invalid_tasks.push(task_id.clone());
                    continue;
                }
                result.upload_tasks.push(recovered);
            }
            TaskType::Transfer => {
                // 转存任务不需要验证本地文件
                result.transfer_tasks.push(recovered);
            }
        }
    }

    // 按创建时间倒序排序（最新的在前面），确保恢复顺序与显示顺序一致
    result
        .download_tasks
        .sort_by(|a, b| b.metadata.created_at.cmp(&a.metadata.created_at));
    result
        .upload_tasks
        .sort_by(|a, b| b.metadata.created_at.cmp(&a.metadata.created_at));
    result
        .transfer_tasks
        .sort_by(|a, b| b.metadata.created_at.cmp(&a.metadata.created_at));

    info!(
        "扫描完成: {} 个下载任务, {} 个上传任务, {} 个转存任务, {} 个已完成, {} 个无效",
        result.download_tasks.len(),
        result.upload_tasks.len(),
        result.transfer_tasks.len(),
        result.completed_tasks.len(),
        result.invalid_tasks.len()
    );

    Ok(result)
}

/// 验证下载任务
///
/// 检查：
/// - 必要的元数据字段存在
/// - 本地文件目录可访问（不要求文件存在，因为可能还没开始下载）
fn validate_download_task(recovered: &RecoveredTask) -> Result<(), String> {
    let metadata = &recovered.metadata;

    // 检查必要字段
    if metadata.fs_id.is_none() {
        return Err("缺少 fs_id".to_string());
    }

    if metadata.local_path.is_none() {
        return Err("缺少 local_path".to_string());
    }

    if metadata.total_chunks.is_none() || metadata.total_chunks == Some(0) {
        return Err("缺少或无效的 total_chunks".to_string());
    }

    // 检查本地路径的父目录是否可访问
    let local_path = metadata.local_path.as_ref().unwrap();
    if let Some(parent) = local_path.parent() {
        // 父目录可以不存在（后续会创建），但路径必须有效
        if parent.as_os_str().is_empty() {
            return Err("无效的本地路径".to_string());
        }
    }

    Ok(())
}

/// 验证上传任务
///
/// 检查：
/// - 必要的元数据字段存在
/// - 本地源文件存在
fn validate_upload_task(recovered: &RecoveredTask) -> Result<(), String> {
    let metadata = &recovered.metadata;

    // 检查必要字段
    if metadata.source_path.is_none() {
        return Err("缺少 source_path".to_string());
    }

    if metadata.total_chunks.is_none() || metadata.total_chunks == Some(0) {
        return Err("缺少或无效的 total_chunks".to_string());
    }

    // 检查源文件是否存在
    let source_path = metadata.source_path.as_ref().unwrap();
    if !source_path.exists() {
        return Err(format!("源文件不存在: {:?}", source_path));
    }

    Ok(())
}

/// 清理已完成任务的持久化文件
///
/// 🔥 修复：在删除文件前，先将已完成任务归档到历史记录（优先使用数据库）
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_ids` - 需要清理的任务 ID 列表
///
/// # Returns
/// 成功清理的任务数
pub fn cleanup_completed_tasks(wal_dir: &Path, task_ids: &[String]) -> usize {
    cleanup_completed_tasks_with_db(wal_dir, task_ids, None)
}

/// 清理已完成任务的持久化文件（带数据库支持）
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_ids` - 需要清理的任务 ID 列表
/// * `history_db` - 历史数据库管理器（可选）
///
/// # Returns
/// 成功清理的任务数
pub fn cleanup_completed_tasks_with_db(
    wal_dir: &Path,
    task_ids: &[String],
    history_db: Option<&super::history_db::HistoryDbManager>,
) -> usize {
    use super::metadata::load_metadata;

    let mut cleaned = 0;
    let mut archived = 0;

    for task_id in task_ids {
        // 在删除前先归档到历史记录
        if let Some(mut metadata) = load_metadata(wal_dir, task_id) {
            // 确保标记为已完成
            metadata.mark_completed();

            // 优先归档到数据库
            if let Some(db) = history_db {
                match db.add_task_to_history(&metadata) {
                    Ok(()) => {
                        archived += 1;
                        debug!("已归档已完成任务到数据库: {}", task_id);
                    }
                    Err(e) => {
                        warn!("归档任务 {} 到数据库失败: {}", task_id, e);
                    }
                }
            } else {
                // 回退到文件归档
                use super::history::add_to_history;
                match add_to_history(wal_dir, &metadata) {
                    Ok(()) => {
                        archived += 1;
                        debug!("已归档已完成任务到历史文件: {}", task_id);
                    }
                    Err(e) => {
                        warn!("归档任务 {} 到历史文件失败: {}", task_id, e);
                    }
                }
            }
        }

        // 删除持久化文件
        match delete_task_files(wal_dir, task_id) {
            Ok(count) if count > 0 => {
                debug!("已清理已完成任务 {} 的 {} 个文件", task_id, count);
                cleaned += 1;
            }
            Ok(_) => {
                debug!("任务 {} 无需清理（文件不存在）", task_id);
            }
            Err(e) => {
                error!("清理任务 {} 失败: {}", task_id, e);
            }
        }
    }

    if archived > 0 {
        info!("已归档 {} 个已完成任务到历史记录", archived);
    }
    if cleaned > 0 {
        info!("已清理 {} 个已完成任务的持久化文件", cleaned);
    }

    cleaned
}

/// 清理无效任务的持久化文件
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_ids` - 需要清理的任务 ID 列表
///
/// # Returns
/// 成功清理的任务数
pub fn cleanup_invalid_tasks(wal_dir: &Path, task_ids: &[String]) -> usize {
    let mut cleaned = 0;

    for task_id in task_ids {
        match delete_task_files(wal_dir, task_id) {
            Ok(count) if count > 0 => {
                warn!("已清理无效任务 {} 的 {} 个文件", task_id, count);
                cleaned += 1;
            }
            Ok(_) => {}
            Err(e) => {
                error!("清理无效任务 {} 失败: {}", task_id, e);
            }
        }
    }

    if cleaned > 0 {
        warn!("已清理 {} 个无效任务的持久化文件", cleaned);
    }

    cleaned
}

/// 清理过期任务
///
/// 删除超过保留天数的未完成任务
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `retention_days` - 保留天数
///
/// # Returns
/// 成功清理的任务数
pub fn cleanup_expired_tasks(wal_dir: &Path, retention_days: u64) -> std::io::Result<usize> {
    info!("开始清理过期任务（保留天数: {}）", retention_days);

    let now = Utc::now();
    let retention_duration = Duration::days(retention_days as i64);
    let mut cleaned = 0;

    // 扫描所有元数据
    let metadata_list = scan_all_metadata(wal_dir)?;

    for metadata in metadata_list {
        // 检查是否过期
        let age = now.signed_duration_since(metadata.updated_at);

        if age > retention_duration {
            info!(
                "任务 {} 已过期（{}天前更新），清理中",
                metadata.task_id,
                age.num_days()
            );

            match delete_task_files(wal_dir, &metadata.task_id) {
                Ok(count) if count > 0 => {
                    cleaned += 1;
                }
                Ok(_) => {}
                Err(e) => {
                    error!("清理过期任务 {} 失败: {}", metadata.task_id, e);
                }
            }
        }
    }

    if cleaned > 0 {
        info!("已清理 {} 个过期任务", cleaned);
    } else {
        debug!("无过期任务需要清理");
    }

    Ok(cleaned)
}

/// 恢复转存任务信息
///
/// 用于 TransferManager 从恢复信息创建任务
#[derive(Debug, Clone)]
pub struct TransferRecoveryInfo {
    /// 任务 ID
    pub task_id: String,
    /// 分享链接
    pub share_link: String,
    /// 提取码（可选）
    pub share_pwd: Option<String>,
    /// 转存目标路径
    pub target_path: String,
    /// 转存状态（checking_share, transferring, transferred, downloading, cleaning, completed）
    pub status: Option<String>,
    /// 关联的下载任务 ID 列表
    pub download_task_ids: Vec<String>,
    /// 创建时间
    pub created_at: i64,
    /// 临时目录路径（分享直下专用，用于清理）
    pub temp_dir: Option<String>,
    /// 是否为分享直下任务
    pub is_share_direct_download: bool,
    /// 文件列表 JSON
    pub file_list_json: Option<String>,
}

impl TransferRecoveryInfo {
    /// 从 RecoveredTask 创建
    pub fn from_recovered(recovered: &RecoveredTask) -> Option<Self> {
        let metadata = &recovered.metadata;

        Some(Self {
            task_id: metadata.task_id.clone(),
            share_link: metadata.share_link.clone()?,
            share_pwd: metadata.share_pwd.clone(),
            target_path: metadata.transfer_target_path.clone()?,
            status: metadata.transfer_status.clone(),
            download_task_ids: metadata.download_task_ids.clone(),
            created_at: metadata.created_at.timestamp(),
            temp_dir: metadata.temp_dir.clone(),
            is_share_direct_download: metadata.is_share_direct_download.unwrap_or(false),
            file_list_json: metadata.file_list_json.clone(),
        })
    }
}

/// 恢复下载任务信息
///
/// 用于 DownloadManager 从恢复信息创建任务
#[derive(Debug, Clone)]
pub struct DownloadRecoveryInfo {
    /// 任务 ID
    pub task_id: String,
    /// 百度网盘文件 fs_id
    pub fs_id: u64,
    /// 远程文件路径
    pub remote_path: String,
    /// 本地保存路径
    pub local_path: PathBuf,
    /// 文件大小
    pub file_size: u64,
    /// 分片大小
    pub chunk_size: u64,
    /// 总分片数
    pub total_chunks: usize,
    /// 已完成的分片集合
    pub completed_chunks: BitSet,
    /// 创建时间
    pub created_at: i64,
    // === 文件夹下载组信息 ===
    /// 文件夹下载组ID（单文件下载时为 None）
    pub group_id: Option<String>,
    /// 文件夹根路径
    pub group_root: Option<String>,
    /// 相对于根文件夹的路径
    pub relative_path: Option<String>,
    // === 跨任务跳转字段 ===
    /// 关联的转存任务 ID（如果此下载任务由转存任务自动创建）
    pub transfer_task_id: Option<String>,
    // === 自动备份字段 ===
    /// 是否为备份任务
    pub is_backup: bool,
    /// 关联的备份配置 ID
    pub backup_config_id: Option<String>,
    // === 加密字段 ===
    /// 是否为加密文件
    pub is_encrypted: bool,
    /// 加密密钥版本
    pub encryption_key_version: Option<u32>,
}

impl DownloadRecoveryInfo {
    /// 从 RecoveredTask 创建
    pub fn from_recovered(recovered: &RecoveredTask) -> Option<Self> {
        let metadata = &recovered.metadata;

        Some(Self {
            task_id: metadata.task_id.clone(),
            fs_id: metadata.fs_id?,
            remote_path: metadata.remote_path.clone()?,
            local_path: metadata.local_path.clone()?,
            file_size: metadata.file_size?,
            chunk_size: metadata.chunk_size?,
            total_chunks: metadata.total_chunks?,
            completed_chunks: recovered.completed_chunks.clone(),
            created_at: metadata.created_at.timestamp(),
            // 恢复文件夹下载组信息
            group_id: metadata.group_id.clone(),
            group_root: metadata.group_root.clone(),
            relative_path: metadata.relative_path.clone(),
            // 恢复跨任务跳转字段
            transfer_task_id: metadata.transfer_task_id.clone(),
            // 恢复备份标识
            is_backup: metadata.is_backup,
            backup_config_id: metadata.backup_config_id.clone(),
            // 恢复加密字段
            is_encrypted: metadata.is_encrypted,
            encryption_key_version: metadata.encryption_key_version,
        })
    }

    /// 获取未完成的分片索引列表
    pub fn pending_chunks(&self) -> Vec<usize> {
        (0..self.total_chunks)
            .filter(|&i| !self.completed_chunks.contains(i))
            .collect()
    }
}

/// 恢复上传任务信息
///
/// 用于 UploadManager 从恢复信息创建任务
#[derive(Debug, Clone)]
pub struct UploadRecoveryInfo {
    /// 任务 ID
    pub task_id: String,
    /// 本地源文件路径
    pub source_path: PathBuf,
    /// 远程目标路径
    pub target_path: String,
    /// 文件大小
    pub file_size: u64,
    /// 分片大小
    pub chunk_size: u64,
    /// 总分片数
    pub total_chunks: usize,
    /// 已完成的分片集合
    pub completed_chunks: BitSet,
    /// 分片 MD5 列表（已上传分片的 MD5）
    pub chunk_md5s: Vec<Option<String>>,
    /// 上传 ID（百度网盘 precreate 返回，可能已过期）
    pub upload_id: Option<String>,
    /// 创建时间
    pub created_at: i64,
    // === 自动备份字段 ===
    /// 是否为备份任务
    pub is_backup: bool,
    /// 关联的备份配置 ID
    pub backup_config_id: Option<String>,
    // === 加密字段 ===
    /// 是否启用加密
    pub encrypt_enabled: bool,
    /// 加密密钥版本
    pub encryption_key_version: Option<u32>,
    /// 加密前的原始远程路径（用于重启后重建去重索引）
    pub original_remote_path: Option<String>,
}

impl UploadRecoveryInfo {
    /// 从 RecoveredTask 创建
    pub fn from_recovered(recovered: &RecoveredTask) -> Option<Self> {
        let metadata = &recovered.metadata;

        Some(Self {
            task_id: metadata.task_id.clone(),
            source_path: metadata.source_path.clone()?,
            target_path: metadata.target_path.clone()?,
            file_size: metadata.file_size?,
            chunk_size: metadata.chunk_size?,
            total_chunks: metadata.total_chunks?,
            completed_chunks: recovered.completed_chunks.clone(),
            chunk_md5s: recovered
                .chunk_md5s
                .clone()
                .unwrap_or_else(|| vec![None; metadata.total_chunks.unwrap_or(0)]),
            upload_id: metadata.upload_id.clone(),
            created_at: metadata.created_at.timestamp(),
            // 恢复备份标识
            is_backup: metadata.is_backup,
            backup_config_id: metadata.backup_config_id.clone(),
            // 恢复加密字段
            encrypt_enabled: metadata.encrypt_enabled,
            encryption_key_version: metadata.encryption_key_version,
            original_remote_path: metadata.original_remote_path.clone(),
        })
    }

    /// 获取未完成的分片索引列表
    pub fn pending_chunks(&self) -> Vec<usize> {
        (0..self.total_chunks)
            .filter(|&i| !self.completed_chunks.contains(i))
            .collect()
    }

    /// 获取已完成的分片数
    pub fn completed_count(&self) -> usize {
        self.completed_chunks.len()
    }

    /// 计算已上传的字节数
    pub fn uploaded_bytes(&self) -> u64 {
        let completed_count = self.completed_count();
        if completed_count == 0 {
            return 0;
        }

        // 完整分片的字节数
        let full_chunks = completed_count.saturating_sub(1);
        let full_size = (full_chunks as u64) * self.chunk_size;

        // 检查最后一个分片是否完成
        let last_chunk_index = self.total_chunks.saturating_sub(1);
        let last_chunk_size = if self.completed_chunks.contains(last_chunk_index) {
            // 最后一个分片的大小可能小于 chunk_size
            self.file_size
                .saturating_sub(last_chunk_index as u64 * self.chunk_size)
        } else {
            0
        };

        full_size + last_chunk_size
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::metadata::save_metadata;
    use crate::persistence::types::WalRecord;
    use crate::persistence::wal::append_records;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn setup_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temp dir")
    }

    #[test]
    fn test_scan_empty_directory() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        let result = scan_recoverable_tasks(wal_dir).unwrap();
        assert_eq!(result.total_recoverable(), 0);
    }

    #[test]
    fn test_scan_download_task() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建下载任务元数据
        let metadata = TaskMetadata::new_download(
            "dl_001".to_string(),
            12345,
            "/remote/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024 * 1024,
            256 * 1024,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 创建 WAL 记录（完成2个分片）
        let records = vec![WalRecord::new_download(0), WalRecord::new_download(2)];
        append_records(wal_dir, "dl_001", &records).unwrap();

        // 扫描
        let result = scan_recoverable_tasks(wal_dir).unwrap();

        assert_eq!(result.download_tasks.len(), 1);
        assert_eq!(result.upload_tasks.len(), 0);
        assert_eq!(result.transfer_tasks.len(), 0);

        let recovered = &result.download_tasks[0];
        assert_eq!(recovered.task_id(), "dl_001");
        assert_eq!(recovered.completed_count(), 2);
        assert_eq!(recovered.total_chunks(), 4);
        assert_eq!(recovered.pending_chunks(), vec![1, 3]);
    }

    #[test]
    fn test_scan_upload_task_with_md5() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建源文件（上传任务需要源文件存在）
        let source_path = temp_dir.path().join("source.txt");
        std::fs::write(&source_path, "test content").unwrap();

        // 创建上传任务元数据
        let metadata = TaskMetadata::new_upload(
            "up_001".to_string(),
            source_path.clone(),
            "/remote/upload.txt".to_string(),
            1024,
            256,
            4,
            None,  // encrypt_enabled
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 创建 WAL 记录（带 MD5）
        let records = vec![
            WalRecord::new_upload(0, "md5_0".to_string()),
            WalRecord::new_upload(1, "md5_1".to_string()),
        ];
        append_records(wal_dir, "up_001", &records).unwrap();

        // 扫描
        let result = scan_recoverable_tasks(wal_dir).unwrap();

        assert_eq!(result.upload_tasks.len(), 1);

        let recovered = &result.upload_tasks[0];
        assert_eq!(recovered.completed_count(), 2);

        // 验证 MD5
        let md5s = recovered.chunk_md5s.as_ref().unwrap();
        assert_eq!(md5s[0], Some("md5_0".to_string()));
        assert_eq!(md5s[1], Some("md5_1".to_string()));
        assert_eq!(md5s[2], None);
        assert_eq!(md5s[3], None);
    }

    #[test]
    fn test_scan_transfer_task() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建转存任务元数据
        let metadata = TaskMetadata::new_transfer(
            "tr_001".to_string(),
            "https://pan.baidu.com/s/xxx".to_string(),
            Some("1234".to_string()),
            "/save/path".to_string(),
            true,
            Some("test.zip".to_string()),
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 扫描
        let result = scan_recoverable_tasks(wal_dir).unwrap();

        assert_eq!(result.transfer_tasks.len(), 1);

        let recovered = &result.transfer_tasks[0];
        assert_eq!(recovered.task_id(), "tr_001");
        assert_eq!(recovered.task_type(), TaskType::Transfer);
    }

    #[test]
    fn test_scan_completed_task() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建下载任务元数据
        let metadata = TaskMetadata::new_download(
            "dl_complete".to_string(),
            12345,
            "/remote/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024,
            256,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 创建 WAL 记录（完成所有分片）
        let records = vec![
            WalRecord::new_download(0),
            WalRecord::new_download(1),
            WalRecord::new_download(2),
            WalRecord::new_download(3),
        ];
        append_records(wal_dir, "dl_complete", &records).unwrap();

        // 扫描
        let result = scan_recoverable_tasks(wal_dir).unwrap();

        // 已完成的任务不应在可恢复列表中
        assert_eq!(result.download_tasks.len(), 0);
        assert_eq!(result.completed_tasks.len(), 1);
        assert_eq!(result.completed_tasks[0], "dl_complete");
    }

    #[test]
    fn test_cleanup_completed_tasks() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建任务
        let metadata = TaskMetadata::new_download(
            "dl_clean".to_string(),
            12345,
            "/remote/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024,
            256,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 验证文件存在
        assert!(crate::persistence::metadata::metadata_exists(
            wal_dir, "dl_clean"
        ));

        // 清理
        let cleaned = cleanup_completed_tasks(wal_dir, &["dl_clean".to_string()]);
        assert_eq!(cleaned, 1);

        // 验证文件已删除
        assert!(!crate::persistence::metadata::metadata_exists(
            wal_dir, "dl_clean"
        ));
    }

    #[test]
    fn test_invalid_upload_task_missing_source() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建上传任务元数据（源文件不存在）
        let metadata = TaskMetadata::new_upload(
            "up_invalid".to_string(),
            PathBuf::from("/nonexistent/source.txt"),
            "/remote/upload.txt".to_string(),
            1024,
            256,
            4,
            None,  // encrypt_enabled
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 扫描
        let result = scan_recoverable_tasks(wal_dir).unwrap();

        // 源文件不存在，应标记为无效
        assert_eq!(result.upload_tasks.len(), 0);
        assert_eq!(result.invalid_tasks.len(), 1);
    }

    #[test]
    fn test_download_recovery_info() {
        let metadata = TaskMetadata::new_download(
            "dl_info".to_string(),
            12345,
            "/remote/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024 * 1024,
            256 * 1024,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );

        let mut completed_chunks = BitSet::with_capacity(4);
        completed_chunks.insert(0);
        completed_chunks.insert(2);

        let recovered = RecoveredTask {
            metadata,
            completed_chunks,
            chunk_md5s: None,
        };

        let info = DownloadRecoveryInfo::from_recovered(&recovered).unwrap();

        assert_eq!(info.task_id, "dl_info");
        assert_eq!(info.fs_id, 12345);
        assert_eq!(info.total_chunks, 4);
        assert_eq!(info.pending_chunks(), vec![1, 3]);
    }
}
