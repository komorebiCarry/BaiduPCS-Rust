//! 文件夹下载持久化模块
//!
//! 该模块负责文件夹下载状态的持久化和恢复

use std::collections::HashSet;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::downloader::folder::{FolderDownload, FolderStatus, PendingFile};

/// 文件夹持久化状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderPersisted {
    /// 文件夹ID
    pub id: String,
    /// 文件夹名称
    pub name: String,
    /// 网盘根路径
    pub remote_root: String,
    /// 本地根路径
    pub local_root: PathBuf,
    /// 状态
    pub status: FolderStatus,
    /// 总文件数
    pub total_files: u64,
    /// 总大小
    pub total_size: u64,
    /// 已创建任务数
    pub created_count: u64,
    /// 已完成任务数
    pub completed_count: u64,
    /// 已下载大小
    pub downloaded_size: u64,
    /// 扫描是否完成
    pub scan_completed: bool,
    /// 扫描进度（当前扫描到的目录）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_progress: Option<String>,
    /// 待下载的文件队列
    pub pending_files: Vec<PendingFile>,
    /// 创建时间
    pub created_at: i64,
    /// 开始时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    /// 完成时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
    /// 错误信息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 🔥 关联的转存任务 ID（如果此文件夹下载任务由转存任务自动创建）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_task_id: Option<String>,
}

impl FolderPersisted {
    /// 从 FolderDownload 转换
    pub fn from_folder(folder: &FolderDownload) -> Self {
        Self {
            id: folder.id.clone(),
            name: folder.name.clone(),
            remote_root: folder.remote_root.clone(),
            local_root: folder.local_root.clone(),
            status: folder.status.clone(),
            total_files: folder.total_files,
            total_size: folder.total_size,
            created_count: folder.created_count,
            completed_count: folder.completed_count,
            downloaded_size: folder.downloaded_size,
            scan_completed: folder.scan_completed,
            scan_progress: folder.scan_progress.clone(),
            pending_files: folder.pending_files.clone(),
            created_at: folder.created_at,
            started_at: folder.started_at,
            completed_at: folder.completed_at,
            error: folder.error.clone(),
            transfer_task_id: folder.transfer_task_id.clone(),
        }
    }

    /// 转换为 FolderDownload
    pub fn to_folder(&self) -> FolderDownload {
        FolderDownload {
            id: self.id.clone(),
            name: self.name.clone(),
            remote_root: self.remote_root.clone(),
            local_root: self.local_root.clone(),
            status: self.status.clone(),
            total_files: self.total_files,
            total_size: self.total_size,
            created_count: self.created_count,
            completed_count: self.completed_count,
            downloaded_size: self.downloaded_size,
            pending_files: self.pending_files.clone(),
            scan_completed: self.scan_completed,
            scan_progress: self.scan_progress.clone(),
            created_at: self.created_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
            error: self.error.clone(),
            transfer_task_id: self.transfer_task_id.clone(),
            // 任务位借调机制字段（不持久化，运行时重建）
            fixed_slot_id: None,
            borrowed_slot_ids: Vec::new(),
            borrowed_subtask_map: std::collections::HashMap::new(),
            encrypted_folder_mappings: std::collections::HashMap::new(),
            counted_task_ids: std::collections::HashSet::new(),
            conflict_strategy: None,
        }
    }
}

/// 保存文件夹状态到文件
/// 路径: <wal_dir>/folders/folder_<id>.json
pub fn save_folder(wal_dir: &Path, folder: &FolderPersisted) -> std::io::Result<()> {
    let folders_dir = wal_dir.join("folders");
    std::fs::create_dir_all(&folders_dir)?;

    let file_path = folders_dir.join(format!("folder_{}.json", folder.id));
    let content = serde_json::to_string_pretty(folder)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&file_path, content)?;

    debug!("保存文件夹状态: {} -> {:?}", folder.id, file_path);
    Ok(())
}

/// 加载所有文件夹状态
pub fn load_all_folders(wal_dir: &Path) -> std::io::Result<Vec<FolderPersisted>> {
    let folders_dir = wal_dir.join("folders");
    if !folders_dir.exists() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    for entry in std::fs::read_dir(&folders_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "json").unwrap_or(false) {
            match std::fs::read_to_string(&path) {
                Ok(content) => match serde_json::from_str::<FolderPersisted>(&content) {
                    Ok(folder) => {
                        debug!("加载文件夹状态: {} ({})", folder.name, folder.id);
                        result.push(folder);
                    }
                    Err(e) => {
                        warn!("解析文件夹持久化文件失败 {:?}: {}", path, e);
                    }
                },
                Err(e) => {
                    warn!("读取文件夹持久化文件失败 {:?}: {}", path, e);
                }
            }
        }
    }
    Ok(result)
}

/// 删除文件夹持久化文件
pub fn delete_folder(wal_dir: &Path, folder_id: &str) -> std::io::Result<()> {
    let file_path = wal_dir
        .join("folders")
        .join(format!("folder_{}.json", folder_id));
    if file_path.exists() {
        std::fs::remove_file(&file_path)?;
        debug!("删除文件夹持久化文件: {:?}", file_path);
    }
    Ok(())
}

/// 加载单个文件夹状态
pub fn load_folder(wal_dir: &Path, folder_id: &str) -> std::io::Result<Option<FolderPersisted>> {
    let file_path = wal_dir
        .join("folders")
        .join(format!("folder_{}.json", folder_id));

    if !file_path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&file_path)?;
    let folder = serde_json::from_str::<FolderPersisted>(&content)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    Ok(Some(folder))
}

// ============================================================================
// 文件夹历史归档功能
// ============================================================================

/// 文件夹历史文件名
const FOLDER_HISTORY_FILE_NAME: &str = "folder_history.jsonl";

/// 获取文件夹历史文件路径
pub fn get_folder_history_path(wal_dir: &Path) -> PathBuf {
    wal_dir.join(FOLDER_HISTORY_FILE_NAME)
}

/// 添加单个文件夹到历史文件
///
/// 用于文件夹完成时立即归档
pub fn add_folder_to_history(wal_dir: &Path, folder: &FolderPersisted) -> std::io::Result<()> {
    // 检查文件夹是否已存在
    let existing_ids = load_folder_history_ids(wal_dir)?;
    if existing_ids.contains(&folder.id) {
        debug!("文件夹已存在于历史中，跳过: {}", folder.id);
        return Ok(());
    }

    append_folders_to_history_file(wal_dir, &[folder.clone()])
}

/// 追加文件夹到历史文件
fn append_folders_to_history_file(wal_dir: &Path, folders: &[FolderPersisted]) -> std::io::Result<()> {
    let history_path = get_folder_history_path(wal_dir);

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&history_path)?;

    let mut writer = BufWriter::new(file);

    for folder in folders {
        let json = serde_json::to_string(folder)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        writeln!(writer, "{}", json)?;
    }

    writer.flush()?;

    debug!(
        "已追加 {} 个文件夹到历史文件: {:?}",
        folders.len(),
        history_path
    );

    Ok(())
}

/// 加载文件夹历史文件到 Vec
pub fn load_folder_history(wal_dir: &Path) -> std::io::Result<Vec<FolderPersisted>> {
    let history_path = get_folder_history_path(wal_dir);
    let mut result = Vec::new();

    if !history_path.exists() {
        return Ok(result);
    }

    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    for (line_num, line) in reader.lines().enumerate() {
        match line {
            Ok(line) if !line.trim().is_empty() => {
                match serde_json::from_str::<FolderPersisted>(&line) {
                    Ok(folder) => {
                        result.push(folder);
                    }
                    Err(e) => {
                        warn!("解析文件夹历史记录失败 (行 {}): {}", line_num + 1, e);
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!("读取文件夹历史文件行失败 (行 {}): {}", line_num + 1, e);
            }
        }
    }

    Ok(result)
}

/// 加载文件夹历史 ID 集合（用于去重）
pub fn load_folder_history_ids(wal_dir: &Path) -> std::io::Result<HashSet<String>> {
    let history_path = get_folder_history_path(wal_dir);
    let mut ids = HashSet::new();

    if !history_path.exists() {
        return Ok(ids);
    }

    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                warn!("读取文件 {} 时遇到 IO 错误，跳过该行: {}", history_path.display(), e);
                continue;
            }
        };
        if let Ok(folder) = serde_json::from_str::<FolderPersisted>(&line) {
            ids.insert(folder.id);
        }
    }

    Ok(ids)
}

/// 从文件夹历史文件中删除指定任务
pub fn remove_folder_from_history(wal_dir: &Path, folder_id: &str) -> std::io::Result<bool> {
    let history_path = get_folder_history_path(wal_dir);

    if !history_path.exists() {
        return Ok(false);
    }

    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    let mut records: Vec<String> = Vec::new();
    let mut found = false;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                warn!("读取文件 {} 时遇到 IO 错误，跳过该行: {}", history_path.display(), e);
                continue;
            }
        };
        if !line.trim().is_empty() {
            if let Ok(folder) = serde_json::from_str::<FolderPersisted>(&line) {
                if folder.id == folder_id {
                    found = true;
                    continue;
                }
            }
            records.push(line);
        }
    }

    if !found {
        return Ok(false);
    }

    // 使用临时文件 + 原子替换
    let temp_path = wal_dir.join(format!("{}.tmp", FOLDER_HISTORY_FILE_NAME));

    {
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);
        for record in &records {
            writeln!(writer, "{}", record)?;
        }
        writer.flush()?;
    }

    std::fs::rename(&temp_path, &history_path)?;
    info!("已从文件夹历史文件中删除: {}", folder_id);

    Ok(true)
}

/// 归档已完成的文件夹到历史文件
///
/// 扫描 wal_dir/folders 中所有 folder_*.json 文件，
/// 找出状态为 completed 的文件夹，追加到 folder_history.jsonl 文件中，
/// 然后删除对应的 .json 文件
pub fn archive_completed_folders(wal_dir: &Path) -> std::io::Result<usize> {
    // 1. 加载已有历史文件夹 ID（用于去重）
    let existing_ids = load_folder_history_ids(wal_dir)?;

    // 2. 加载所有持久化的文件夹
    let folders = load_all_folders(wal_dir)?;

    let mut archived_count = 0;
    let mut to_archive: Vec<FolderPersisted> = Vec::new();
    let mut to_cleanup: Vec<String> = Vec::new();

    for folder in folders {
        // 跳过已存在于历史中的文件夹
        if existing_ids.contains(&folder.id) {
            debug!("文件夹已存在于历史中，跳过: {}", folder.id);
            // 仍然需要清理 .json 文件
            to_cleanup.push(folder.id);
            continue;
        }

        // 只归档状态为 completed 的文件夹
        if folder.status == FolderStatus::Completed {
            to_archive.push(folder.clone());
            to_cleanup.push(folder.id);
        }
    }

    // 3. 追加到历史文件
    if !to_archive.is_empty() {
        append_folders_to_history_file(wal_dir, &to_archive)?;
        archived_count = to_archive.len();
        info!("已归档 {} 个已完成文件夹到历史文件", archived_count);
    }

    // 4. 清理已归档文件夹的 .json 文件
    for folder_id in to_cleanup {
        let folder_path = wal_dir
            .join("folders")
            .join(format!("folder_{}.json", folder_id));
        if folder_path.exists() {
            if let Err(e) = std::fs::remove_file(&folder_path) {
                warn!("删除文件夹持久化文件失败: {:?}, 错误: {}", folder_path, e);
            }
        }
    }

    Ok(archived_count)
}

/// 清理过期的文件夹历史
pub fn cleanup_expired_folder_history(wal_dir: &Path, retention_days: u64) -> std::io::Result<usize> {
    let history_path = get_folder_history_path(wal_dir);

    if !history_path.exists() {
        return Ok(0);
    }

    let cutoff_timestamp = (Utc::now() - Duration::days(retention_days as i64)).timestamp();

    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    let mut kept_records: Vec<String> = Vec::new();
    let mut expired_count = 0;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                warn!("读取文件 {} 时遇到 IO 错误，跳过该行: {}", history_path.display(), e);
                continue;
            }
        };
        if !line.trim().is_empty() {
            if let Ok(folder) = serde_json::from_str::<FolderPersisted>(&line) {
                let is_expired = folder
                    .completed_at
                    .map(|t| t < cutoff_timestamp)
                    .unwrap_or(false);

                if is_expired {
                    expired_count += 1;
                    continue;
                }
            }
            kept_records.push(line);
        }
    }

    if expired_count == 0 {
        return Ok(0);
    }

    let temp_path = wal_dir.join(format!("{}.tmp", FOLDER_HISTORY_FILE_NAME));

    {
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);
        for record in &kept_records {
            writeln!(writer, "{}", record)?;
        }
        writer.flush()?;
    }

    std::fs::rename(&temp_path, &history_path)?;
    info!(
        "已清理 {} 个过期文件夹历史（超过 {} 天）",
        expired_count, retention_days
    );

    Ok(expired_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_folder_persisted_conversion() {
        let folder = FolderDownload::new("/test/folder".to_string(), PathBuf::from("/local/folder"));

        let persisted = FolderPersisted::from_folder(&folder);
        assert_eq!(persisted.id, folder.id);
        assert_eq!(persisted.name, folder.name);
        assert_eq!(persisted.remote_root, folder.remote_root);

        let restored = persisted.to_folder();
        assert_eq!(restored.id, folder.id);
        assert_eq!(restored.name, folder.name);
        assert_eq!(restored.status, folder.status);
    }

    #[test]
    fn test_save_and_load_folder() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path();

        let folder = FolderDownload::new("/电影".to_string(), PathBuf::from("/local/电影"));
        let persisted = FolderPersisted::from_folder(&folder);

        // 保存
        save_folder(wal_dir, &persisted).unwrap();

        // 加载所有
        let folders = load_all_folders(wal_dir).unwrap();
        assert_eq!(folders.len(), 1);
        assert_eq!(folders[0].id, persisted.id);

        // 加载单个
        let loaded = load_folder(wal_dir, &persisted.id).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, persisted.name);

        // 删除
        delete_folder(wal_dir, &persisted.id).unwrap();
        let folders = load_all_folders(wal_dir).unwrap();
        assert_eq!(folders.len(), 0);
    }
}
