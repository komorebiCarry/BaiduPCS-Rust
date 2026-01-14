//! 历史归档模块
//!
//! 负责已完成任务的历史归档功能：
//! - 将已完成的任务元数据归档到 history.jsonl 文件
//! - 从历史文件加载任务到内存缓存
//! - 支持从历史中删除任务
//! - 支持定期清理过期历史任务
//!
//! ## 文件格式
//!
//! history.jsonl 使用 JSON Lines 格式，每行一个完整的 TaskMetadata JSON：
//! ```text
//! {"task_id":"dl_001","task_type":"download",...}
//! {"task_id":"dl_002","task_type":"download",...}
//! ```

use std::collections::HashSet;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{Duration, Utc};
use dashmap::DashMap;
use tracing::{debug, info, warn};

use super::metadata::{load_metadata, scan_metadata_task_ids};
use super::types::{TaskMetadata, TaskPersistenceStatus};
use super::wal::delete_wal_file;

/// 历史文件名
const HISTORY_FILE_NAME: &str = "history.jsonl";

/// 获取历史文件路径
pub fn get_history_path(wal_dir: &Path) -> PathBuf {
    wal_dir.join(HISTORY_FILE_NAME)
}

/// 归档已完成的任务到历史文件
///
/// 扫描 wal_dir 中所有 .meta 文件，找出状态为 completed 的任务，
/// 追加到 history.jsonl 文件中，然后删除对应的 .meta 和 .wal 文件
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
///
/// # Returns
/// - `Ok(usize)` - 成功归档的任务数量
/// - `Err` - 归档过程中发生错误
pub fn archive_completed_tasks(wal_dir: &Path) -> std::io::Result<usize> {
    // 1. 加载已有历史任务 ID（用于去重）
    let existing_ids = load_history_task_ids(wal_dir)?;

    // 2. 扫描所有 .meta 文件
    let task_ids = scan_metadata_task_ids(wal_dir)?;

    let mut archived_count = 0;
    let mut to_archive: Vec<TaskMetadata> = Vec::new();
    let mut to_cleanup: Vec<String> = Vec::new();

    for task_id in task_ids {
        // 跳过已存在于历史中的任务
        if existing_ids.contains(&task_id) {
            debug!("任务已存在于历史中，跳过: {}", task_id);
            // 仍然需要清理 .meta 和 .wal 文件
            to_cleanup.push(task_id);
            continue;
        }

        // 加载元数据
        if let Some(metadata) = load_metadata(wal_dir, &task_id) {
            // 只归档状态为 completed 的任务
            if metadata.status == Some(TaskPersistenceStatus::Completed) {
                to_archive.push(metadata);
                to_cleanup.push(task_id);
            }
        } else {
            warn!("加载元数据失败，跳过任务: {}", task_id);
        }
    }

    // 3. 追加到历史文件
    if !to_archive.is_empty() {
        append_to_history_file(wal_dir, &to_archive)?;
        archived_count = to_archive.len();
        info!("已归档 {} 个已完成任务到历史文件", archived_count);
    }

    // 4. 清理已归档任务的 .meta 和 .wal 文件
    for task_id in to_cleanup {
        // 删除 .wal 文件
        if let Err(e) = delete_wal_file(wal_dir, &task_id) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("删除 WAL 文件失败: {}, 错误: {}", task_id, e);
            }
        }

        // 删除 .meta 文件
        let meta_path = wal_dir.join(format!("{}.meta", task_id));
        if meta_path.exists() {
            if let Err(e) = std::fs::remove_file(&meta_path) {
                warn!("删除元数据文件失败: {:?}, 错误: {}", meta_path, e);
            }
        }
    }

    Ok(archived_count)
}

/// 追加任务元数据到历史文件
fn append_to_history_file(wal_dir: &Path, tasks: &[TaskMetadata]) -> std::io::Result<()> {
    let history_path = get_history_path(wal_dir);

    // 以追加模式打开文件
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&history_path)?;

    let mut writer = BufWriter::new(file);

    for task in tasks {
        let json = serde_json::to_string(task)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        writeln!(writer, "{}", json)?;
    }

    writer.flush()?;

    debug!(
        "已追加 {} 条记录到历史文件: {:?}",
        tasks.len(),
        history_path
    );

    Ok(())
}

/// 加载历史文件到缓存
///
/// 读取 history.jsonl 文件，解析每行 JSON 并存入 DashMap
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
///
/// # Returns
/// - `Ok(DashMap<String, TaskMetadata>)` - 历史任务缓存
/// - `Err` - 加载过程中发生错误
pub fn load_history_cache(wal_dir: &Path) -> std::io::Result<DashMap<String, TaskMetadata>> {
    let history_path = get_history_path(wal_dir);
    let cache = DashMap::new();

    if !history_path.exists() {
        debug!("历史文件不存在，返回空缓存: {:?}", history_path);
        return Ok(cache);
    }

    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    let mut loaded_count = 0;
    let mut error_count = 0;

    for (line_num, line) in reader.lines().enumerate() {
        match line {
            Ok(line) if !line.trim().is_empty() => {
                match serde_json::from_str::<TaskMetadata>(&line) {
                    Ok(metadata) => {
                        cache.insert(metadata.task_id.clone(), metadata);
                        loaded_count += 1;
                    }
                    Err(e) => {
                        error_count += 1;
                        warn!("解析历史记录失败 (行 {}): {}", line_num + 1, e);
                    }
                }
            }
            Ok(_) => {} // 空行跳过
            Err(e) => {
                error_count += 1;
                warn!("读取历史文件行失败 (行 {}): {}", line_num + 1, e);
            }
        }
    }

    if error_count > 0 {
        warn!("加载历史文件时有 {} 条记录解析失败", error_count);
    }

    info!("已加载 {} 条历史任务记录", loaded_count);

    Ok(cache)
}

/// 加载历史任务 ID 集合（用于去重）
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
///
/// # Returns
/// - `Ok(HashSet<String>)` - 历史任务 ID 集合
/// - `Err` - 加载过程中发生错误
pub fn load_history_task_ids(wal_dir: &Path) -> std::io::Result<HashSet<String>> {
    let history_path = get_history_path(wal_dir);
    let mut ids = HashSet::new();

    if !history_path.exists() {
        return Ok(ids);
    }

    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        if let Ok(line) = line {
            if let Ok(metadata) = serde_json::from_str::<TaskMetadata>(&line) {
                ids.insert(metadata.task_id);
            }
        }
    }

    Ok(ids)
}

/// 从历史文件中删除指定任务
///
/// 读取整个历史文件，过滤掉指定任务，然后原子替换
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
/// * `task_id` - 要删除的任务 ID
///
/// # Returns
/// - `Ok(bool)` - true 表示找到并删除，false 表示未找到
/// - `Err` - 删除过程中发生错误
pub fn remove_from_history_file(wal_dir: &Path, task_id: &str) -> std::io::Result<bool> {
    let history_path = get_history_path(wal_dir);

    if !history_path.exists() {
        return Ok(false);
    }

    // 读取所有记录
    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    let mut records: Vec<String> = Vec::new();
    let mut found = false;

    for line in reader.lines() {
        match line {
            Ok(line) if !line.trim().is_empty() => {
                if let Ok(metadata) = serde_json::from_str::<TaskMetadata>(&line) {
                    if metadata.task_id == task_id {
                        found = true;
                        continue; // 跳过要删除的任务
                    }
                }
                records.push(line);
            }
            _ => {}
        }
    }

    if !found {
        return Ok(false);
    }

    // 使用临时文件 + 原子替换
    let temp_path = wal_dir.join(format!("{}.tmp", HISTORY_FILE_NAME));

    {
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        for record in &records {
            writeln!(writer, "{}", record)?;
        }

        writer.flush()?;
    }

    // 原子替换
    std::fs::rename(&temp_path, &history_path)?;

    info!("已从历史文件中删除任务: {}", task_id);

    Ok(true)
}

/// 从历史文件中删除指定文件夹的所有子任务
///
/// 根据 group_id 删除所有属于该文件夹的子任务
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
/// * `group_id` - 文件夹 ID
///
/// # Returns
/// - `Ok(usize)` - 删除的任务数量
/// - `Err` - 删除过程中发生错误
pub fn remove_tasks_by_group_from_history(wal_dir: &Path, group_id: &str) -> std::io::Result<usize> {
    let history_path = get_history_path(wal_dir);

    if !history_path.exists() {
        return Ok(0);
    }

    // 读取所有记录
    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    let mut records: Vec<String> = Vec::new();
    let mut removed_count = 0;

    for line in reader.lines() {
        match line {
            Ok(line) if !line.trim().is_empty() => {
                if let Ok(metadata) = serde_json::from_str::<TaskMetadata>(&line) {
                    // 检查是否属于该文件夹
                    if metadata.group_id.as_deref() == Some(group_id) {
                        removed_count += 1;
                        continue; // 跳过要删除的任务
                    }
                }
                records.push(line);
            }
            _ => {}
        }
    }

    if removed_count == 0 {
        return Ok(0);
    }

    // 使用临时文件 + 原子替换
    let temp_path = wal_dir.join(format!("{}.tmp", HISTORY_FILE_NAME));

    {
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        for record in &records {
            writeln!(writer, "{}", record)?;
        }

        writer.flush()?;
    }

    // 原子替换
    std::fs::rename(&temp_path, &history_path)?;

    info!(
        "已从历史文件中删除文件夹 {} 的 {} 个子任务",
        group_id, removed_count
    );

    Ok(removed_count)
}

/// 清理过期的历史任务
///
/// 删除 completed_at 超过 retention_days 天的任务
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
/// * `retention_days` - 保留天数
///
/// # Returns
/// - `Ok(usize)` - 清理的任务数量
/// - `Err` - 清理过程中发生错误
pub fn cleanup_expired_history(wal_dir: &Path, retention_days: u64) -> std::io::Result<usize> {
    let history_path = get_history_path(wal_dir);

    if !history_path.exists() {
        return Ok(0);
    }

    let cutoff_time = Utc::now() - Duration::days(retention_days as i64);

    // 读取所有记录
    let file = std::fs::File::open(&history_path)?;
    let reader = BufReader::new(file);

    let mut kept_records: Vec<String> = Vec::new();
    let mut expired_count = 0;

    for line in reader.lines() {
        match line {
            Ok(line) if !line.trim().is_empty() => {
                if let Ok(metadata) = serde_json::from_str::<TaskMetadata>(&line) {
                    // 检查是否过期
                    let is_expired = metadata
                        .completed_at
                        .map(|t| t < cutoff_time)
                        .unwrap_or(false);

                    if is_expired {
                        expired_count += 1;
                        continue;
                    }
                }
                kept_records.push(line);
            }
            _ => {}
        }
    }

    if expired_count == 0 {
        return Ok(0);
    }

    // 使用临时文件 + 原子替换
    let temp_path = wal_dir.join(format!("{}.tmp", HISTORY_FILE_NAME));

    {
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        for record in &kept_records {
            writeln!(writer, "{}", record)?;
        }

        writer.flush()?;
    }

    // 原子替换
    std::fs::rename(&temp_path, &history_path)?;

    info!(
        "已清理 {} 条过期历史任务（超过 {} 天）",
        expired_count, retention_days
    );

    Ok(expired_count)
}

/// 添加单个任务到历史文件
///
/// 用于任务完成时立即归档
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
/// * `metadata` - 要归档的任务元数据
pub fn add_to_history(wal_dir: &Path, metadata: &TaskMetadata) -> std::io::Result<()> {
    // 检查任务是否已存在
    let existing_ids = load_history_task_ids(wal_dir)?;
    if existing_ids.contains(&metadata.task_id) {
        debug!("任务已存在于历史中，跳过: {}", metadata.task_id);
        return Ok(());
    }

    append_to_history_file(wal_dir, &[metadata.clone()])
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_test_metadata(task_id: &str, completed: bool) -> TaskMetadata {
        let mut metadata = TaskMetadata::new_download(
            task_id.to_string(),
            12345,
            "/test/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024 * 1024,
            256 * 1024,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );

        if completed {
            metadata.mark_completed();
        }

        metadata
    }

    #[test]
    fn test_get_history_path() {
        let wal_dir = PathBuf::from("/tmp/wal");
        let path = get_history_path(&wal_dir);
        assert_eq!(path, PathBuf::from("/tmp/wal/history.jsonl"));
    }

    #[test]
    fn test_load_history_cache_empty() {
        let temp_dir = TempDir::new().unwrap();
        let cache = load_history_cache(temp_dir.path()).unwrap();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_append_and_load_history() {
        let temp_dir = TempDir::new().unwrap();

        // 创建测试数据
        let metadata1 = create_test_metadata("dl_001", true);
        let metadata2 = create_test_metadata("dl_002", true);

        // 追加到历史文件
        append_to_history_file(temp_dir.path(), &[metadata1.clone(), metadata2.clone()]).unwrap();

        // 加载缓存
        let cache = load_history_cache(temp_dir.path()).unwrap();

        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key("dl_001"));
        assert!(cache.contains_key("dl_002"));
    }

    #[test]
    fn test_load_history_task_ids() {
        let temp_dir = TempDir::new().unwrap();

        let metadata = create_test_metadata("dl_001", true);
        append_to_history_file(temp_dir.path(), &[metadata]).unwrap();

        let ids = load_history_task_ids(temp_dir.path()).unwrap();
        assert!(ids.contains("dl_001"));
    }

    #[test]
    fn test_remove_from_history_file() {
        let temp_dir = TempDir::new().unwrap();

        // 添加多个任务
        let metadata1 = create_test_metadata("dl_001", true);
        let metadata2 = create_test_metadata("dl_002", true);
        append_to_history_file(temp_dir.path(), &[metadata1, metadata2]).unwrap();

        // 删除一个任务
        let removed = remove_from_history_file(temp_dir.path(), "dl_001").unwrap();
        assert!(removed);

        // 验证只剩一个
        let cache = load_history_cache(temp_dir.path()).unwrap();
        assert_eq!(cache.len(), 1);
        assert!(!cache.contains_key("dl_001"));
        assert!(cache.contains_key("dl_002"));
    }

    #[test]
    fn test_remove_nonexistent_task() {
        let temp_dir = TempDir::new().unwrap();

        let metadata = create_test_metadata("dl_001", true);
        append_to_history_file(temp_dir.path(), &[metadata]).unwrap();

        let removed = remove_from_history_file(temp_dir.path(), "nonexistent").unwrap();
        assert!(!removed);
    }

    #[test]
    fn test_add_to_history_deduplication() {
        let temp_dir = TempDir::new().unwrap();

        let metadata = create_test_metadata("dl_001", true);

        // 添加第一次
        add_to_history(temp_dir.path(), &metadata).unwrap();

        // 添加第二次（应该被跳过）
        add_to_history(temp_dir.path(), &metadata).unwrap();

        // 验证只有一条记录
        let cache = load_history_cache(temp_dir.path()).unwrap();
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cleanup_expired_history() {
        let temp_dir = TempDir::new().unwrap();

        // 创建过期任务
        let mut old_metadata = create_test_metadata("dl_old", true);
        old_metadata.completed_at = Some(Utc::now() - Duration::days(60));

        // 创建未过期任务
        let new_metadata = create_test_metadata("dl_new", true);

        append_to_history_file(temp_dir.path(), &[old_metadata, new_metadata]).unwrap();

        // 清理超过 30 天的任务
        let cleaned = cleanup_expired_history(temp_dir.path(), 30).unwrap();
        assert_eq!(cleaned, 1);

        // 验证只剩新任务
        let cache = load_history_cache(temp_dir.path()).unwrap();
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key("dl_new"));
    }
}
