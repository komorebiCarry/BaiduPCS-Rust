//! 元数据文件操作
//!
//! 实现任务元数据文件的读写功能，用于保存任务基本信息
//!
//! ## 文件格式
//!
//! 元数据文件为 JSON 格式，扩展名为 `.meta`：
//! ```json
//! {
//!   "task_id": "xxx",
//!   "task_type": "download",
//!   "created_at": "2025-12-05T00:00:00Z",
//!   ...
//! }
//! ```

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use tracing::{debug, error, warn};

use super::types::TaskMetadata;

/// 元数据文件扩展名
const META_EXTENSION: &str = "meta";

// ============================================================================
// 辅助函数
// ============================================================================

/// 获取元数据文件路径
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// 元数据文件的完整路径：`{wal_dir}/{task_id}.meta`
pub fn get_metadata_path(wal_dir: &Path, task_id: &str) -> PathBuf {
    wal_dir.join(format!("{}.{}", task_id, META_EXTENSION))
}

/// 检查元数据文件是否存在
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_id` - 任务 ID
pub fn metadata_exists(wal_dir: &Path, task_id: &str) -> bool {
    get_metadata_path(wal_dir, task_id).exists()
}

/// 确保目录存在
fn ensure_dir(dir: &Path) -> io::Result<()> {
    if !dir.exists() {
        fs::create_dir_all(dir)?;
        debug!("已创建目录: {:?}", dir);
    }
    Ok(())
}

// ============================================================================
// 元数据写入函数
// ============================================================================

/// 保存元数据到文件
///
/// 将任务元数据序列化为 JSON 并保存到 `.meta` 文件
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `metadata` - 任务元数据
///
/// # Returns
/// - `Ok(())` - 保存成功
/// - `Err` - 保存失败
pub fn save_metadata(wal_dir: &Path, metadata: &TaskMetadata) -> io::Result<()> {
    ensure_dir(wal_dir)?;

    let path = get_metadata_path(wal_dir, &metadata.task_id);

    // 先写入临时文件，再原子重命名（防止写入中断导致文件损坏）
    let temp_path = path.with_extension("meta.tmp");

    let file = File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);

    serde_json::to_writer_pretty(&mut writer, metadata).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to serialize metadata: {}", e),
        )
    })?;

    writer.flush()?;
    drop(writer);

    // 原子重命名
    fs::rename(&temp_path, &path)?;

    debug!("已保存元数据: {:?} (task_id={})", path, metadata.task_id);

    Ok(())
}

/// 更新元数据
///
/// 加载现有元数据，应用更新函数，然后保存
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_id` - 任务 ID
/// * `updater` - 更新函数
///
/// # Returns
/// - `Ok(true)` - 更新成功
/// - `Ok(false)` - 元数据不存在
/// - `Err` - 更新失败
pub fn update_metadata<F>(wal_dir: &Path, task_id: &str, updater: F) -> io::Result<bool>
where
    F: FnOnce(&mut TaskMetadata),
{
    // 加载现有元数据
    let mut metadata = match load_metadata(wal_dir, task_id) {
        Some(m) => m,
        None => {
            debug!("元数据不存在，无法更新: task_id={}", task_id);
            return Ok(false);
        }
    };

    // 应用更新
    updater(&mut metadata);

    // 更新时间戳
    metadata.touch();

    // 保存
    save_metadata(wal_dir, &metadata)?;

    debug!("已更新元数据: task_id={}", task_id);

    Ok(true)
}

// ============================================================================
// 元数据读取函数
// ============================================================================

/// 加载元数据
///
/// 从 `.meta` 文件读取并反序列化任务元数据
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// - `Some(TaskMetadata)` - 加载成功
/// - `None` - 文件不存在或解析失败
pub fn load_metadata(wal_dir: &Path, task_id: &str) -> Option<TaskMetadata> {
    let path = get_metadata_path(wal_dir, task_id);

    if !path.exists() {
        return None;
    }

    match load_metadata_from_path(&path) {
        Ok(metadata) => Some(metadata),
        Err(e) => {
            warn!("加载元数据失败 {:?}: {}", path, e);
            None
        }
    }
}

/// 从指定路径加载元数据
fn load_metadata_from_path(path: &Path) -> io::Result<TaskMetadata> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let metadata: TaskMetadata = serde_json::from_reader(reader).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse metadata: {}", e),
        )
    })?;

    debug!("已加载元数据: {:?} (task_id={})", path, metadata.task_id);

    Ok(metadata)
}

/// 扫描所有元数据文件
///
/// 遍历 WAL 目录，读取所有 `.meta` 文件
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
///
/// # Returns
/// 成功加载的所有元数据列表（跳过无法解析的文件）
pub fn scan_all_metadata(wal_dir: &Path) -> io::Result<Vec<TaskMetadata>> {
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }

    let mut metadata_list = Vec::new();
    let mut skipped = 0;

    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == META_EXTENSION {
                    match load_metadata_from_path(&path) {
                        Ok(metadata) => metadata_list.push(metadata),
                        Err(e) => {
                            warn!("跳过无效元数据文件 {:?}: {}", path, e);
                            skipped += 1;
                        }
                    }
                }
            }
        }
    }

    if skipped > 0 {
        warn!("扫描元数据完成，跳过 {} 个无效文件", skipped);
    }

    debug!("扫描到 {} 个元数据文件", metadata_list.len());

    Ok(metadata_list)
}

/// 扫描元数据目录中的所有任务 ID
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
///
/// # Returns
/// 所有元数据文件对应的任务 ID 列表
pub fn scan_metadata_task_ids(wal_dir: &Path) -> io::Result<Vec<String>> {
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }

    let mut task_ids = Vec::new();

    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == META_EXTENSION {
                    if let Some(stem) = path.file_stem() {
                        if let Some(task_id) = stem.to_str() {
                            task_ids.push(task_id.to_string());
                        }
                    }
                }
            }
        }
    }

    debug!("扫描到 {} 个元数据任务 ID", task_ids.len());

    Ok(task_ids)
}

// ============================================================================
// 元数据删除函数
// ============================================================================

/// 删除元数据文件
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// - `Ok(true)` - 文件已删除
/// - `Ok(false)` - 文件不存在
/// - `Err` - 删除失败
pub fn delete_metadata(wal_dir: &Path, task_id: &str) -> io::Result<bool> {
    let path = get_metadata_path(wal_dir, task_id);

    if path.exists() {
        fs::remove_file(&path)?;
        debug!("已删除元数据文件: {:?}", path);
        Ok(true)
    } else {
        Ok(false)
    }
}

/// 删除任务的所有持久化文件（元数据 + WAL）
///
/// # Arguments
/// * `wal_dir` - WAL/元数据目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// 删除的文件数量
pub fn delete_task_files(wal_dir: &Path, task_id: &str) -> io::Result<usize> {
    let mut deleted = 0;

    // 删除元数据文件
    if delete_metadata(wal_dir, task_id)? {
        deleted += 1;
    }

    // 删除 WAL 文件
    if super::wal::delete_wal_file(wal_dir, task_id)? {
        deleted += 1;
    }

    // 删除临时文件（如果存在）
    let temp_path = get_metadata_path(wal_dir, task_id).with_extension("meta.tmp");
    if temp_path.exists() {
        if let Err(e) = fs::remove_file(&temp_path) {
            error!("删除临时文件失败 {:?}: {}", temp_path, e);
        } else {
            deleted += 1;
        }
    }

    debug!("已删除任务 {} 的 {} 个文件", task_id, deleted);

    Ok(deleted)
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::types::TaskType;
    use crate::persistence::wal;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn setup_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temp dir")
    }

    #[test]
    fn test_get_metadata_path() {
        let wal_dir = Path::new("/tmp/wal");
        let path = get_metadata_path(wal_dir, "task_123");
        assert_eq!(path, PathBuf::from("/tmp/wal/task_123.meta"));
    }

    #[test]
    fn test_metadata_exists() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 文件不存在
        assert!(!metadata_exists(wal_dir, "task_123"));

        // 创建文件
        fs::create_dir_all(wal_dir).unwrap();
        let path = get_metadata_path(wal_dir, "task_123");
        File::create(&path).unwrap();

        // 文件存在
        assert!(metadata_exists(wal_dir, "task_123"));
    }

    #[test]
    fn test_save_and_load_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建下载任务元数据
        let metadata = TaskMetadata::new_download(
            "task_001".to_string(),
            12345,
            "/remote/file.txt".to_string(),
            PathBuf::from("/local/file.txt"),
            1024 * 1024,
            256 * 1024,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );

        // 保存
        save_metadata(wal_dir, &metadata).unwrap();

        // 验证文件存在
        assert!(metadata_exists(wal_dir, "task_001"));

        // 加载
        let loaded = load_metadata(wal_dir, "task_001").unwrap();
        assert_eq!(loaded.task_id, "task_001");
        assert_eq!(loaded.task_type, TaskType::Download);
        assert_eq!(loaded.fs_id, Some(12345));
        assert_eq!(loaded.total_chunks, Some(4));
    }

    #[test]
    fn test_save_upload_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建上传任务元数据
        let mut metadata = TaskMetadata::new_upload(
            "task_002".to_string(),
            PathBuf::from("/local/upload.txt"),
            "/remote/upload.txt".to_string(),
            2 * 1024 * 1024,
            512 * 1024,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );

        // 设置 upload_id
        metadata.set_upload_id("upload_id_abc".to_string());

        // 保存
        save_metadata(wal_dir, &metadata).unwrap();

        // 加载并验证
        let loaded = load_metadata(wal_dir, "task_002").unwrap();
        assert_eq!(loaded.task_type, TaskType::Upload);
        assert_eq!(loaded.upload_id, Some("upload_id_abc".to_string()));
        assert!(loaded.upload_id_created_at.is_some());
    }

    #[test]
    fn test_save_transfer_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建转存任务元数据
        let mut metadata = TaskMetadata::new_transfer(
            "task_003".to_string(),
            "https://pan.baidu.com/s/xxx".to_string(),
            Some("1234".to_string()),
            "/save/path".to_string(),
            true,
            Some("test.zip".to_string()),
        );

        // 设置状态和关联下载任务
        metadata.set_transfer_status("downloading");
        metadata.set_download_task_ids(vec!["dl_001".to_string(), "dl_002".to_string()]);

        // 保存
        save_metadata(wal_dir, &metadata).unwrap();

        // 加载并验证
        let loaded = load_metadata(wal_dir, "task_003").unwrap();
        assert_eq!(loaded.task_type, TaskType::Transfer);
        assert_eq!(loaded.transfer_status, Some("downloading".to_string()));
        assert_eq!(loaded.download_task_ids.len(), 2);
    }

    #[test]
    fn test_update_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建并保存元数据
        let metadata = TaskMetadata::new_transfer(
            "task_004".to_string(),
            "https://pan.baidu.com/s/yyy".to_string(),
            None,
            "/target".to_string(),
            false,
            None,
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 更新元数据
        let updated = update_metadata(wal_dir, "task_004", |m| {
            m.set_transfer_status("transferred");
            m.share_info_json = Some(r#"{"files": []}"#.to_string());
        })
            .unwrap();
        assert!(updated);

        // 验证更新
        let loaded = load_metadata(wal_dir, "task_004").unwrap();
        assert_eq!(loaded.transfer_status, Some("transferred".to_string()));
        assert!(loaded.share_info_json.is_some());
    }

    #[test]
    fn test_update_nonexistent_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 更新不存在的元数据
        let updated = update_metadata(wal_dir, "nonexistent", |m| {
            m.set_transfer_status("test");
        })
            .unwrap();
        assert!(!updated);
    }

    #[test]
    fn test_delete_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 删除不存在的文件
        let deleted = delete_metadata(wal_dir, "task_005").unwrap();
        assert!(!deleted);

        // 创建并删除
        let metadata = TaskMetadata::new_download(
            "task_005".to_string(),
            111,
            "/path".to_string(),
            PathBuf::from("/local"),
            1024,
            256,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();
        assert!(metadata_exists(wal_dir, "task_005"));

        let deleted = delete_metadata(wal_dir, "task_005").unwrap();
        assert!(deleted);
        assert!(!metadata_exists(wal_dir, "task_005"));
    }

    #[test]
    fn test_scan_all_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建多个元数据文件
        for i in 1..=3 {
            let metadata = TaskMetadata::new_download(
                format!("task_{:03}", i),
                i as u64,
                format!("/path/{}", i),
                PathBuf::from(format!("/local/{}", i)),
                1024,
                256,
                4,
                None,  // is_encrypted
                None,  // encryption_key_version
            );
            save_metadata(wal_dir, &metadata).unwrap();
        }

        // 创建一个无效的 meta 文件
        fs::write(wal_dir.join("invalid.meta"), "not valid json").unwrap();

        // 扫描
        let metadata_list = scan_all_metadata(wal_dir).unwrap();
        assert_eq!(metadata_list.len(), 3);

        // 验证任务 ID
        let mut task_ids: Vec<_> = metadata_list.iter().map(|m| m.task_id.clone()).collect();
        task_ids.sort();
        assert_eq!(task_ids, vec!["task_001", "task_002", "task_003"]);
    }

    #[test]
    fn test_scan_metadata_task_ids() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建多个元数据文件
        fs::create_dir_all(wal_dir).unwrap();
        File::create(wal_dir.join("task_a.meta")).unwrap();
        File::create(wal_dir.join("task_b.meta")).unwrap();
        File::create(wal_dir.join("task_c.meta")).unwrap();
        File::create(wal_dir.join("task_d.wal")).unwrap(); // 非 meta 文件

        // 扫描
        let mut task_ids = scan_metadata_task_ids(wal_dir).unwrap();
        task_ids.sort();

        assert_eq!(task_ids.len(), 3);
        assert_eq!(task_ids, vec!["task_a", "task_b", "task_c"]);
    }

    #[test]
    fn test_delete_task_files() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建元数据和 WAL 文件
        let metadata = TaskMetadata::new_download(
            "task_006".to_string(),
            123,
            "/path".to_string(),
            PathBuf::from("/local"),
            1024,
            256,
            4,
            None,  // is_encrypted
            None,  // encryption_key_version
        );
        save_metadata(wal_dir, &metadata).unwrap();

        // 创建 WAL 文件
        let wal_path = wal::get_wal_path(wal_dir, "task_006");
        File::create(&wal_path).unwrap();

        // 验证文件存在
        assert!(metadata_exists(wal_dir, "task_006"));
        assert!(wal::wal_exists(wal_dir, "task_006"));

        // 删除所有文件
        let deleted = delete_task_files(wal_dir, "task_006").unwrap();
        assert_eq!(deleted, 2);

        // 验证文件已删除
        assert!(!metadata_exists(wal_dir, "task_006"));
        assert!(!wal::wal_exists(wal_dir, "task_006"));
    }

    #[test]
    fn test_load_nonexistent_metadata() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        let result = load_metadata(wal_dir, "nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_scan_empty_directory() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path().join("empty");

        // 目录不存在
        let metadata_list = scan_all_metadata(&wal_dir).unwrap();
        assert!(metadata_list.is_empty());

        let task_ids = scan_metadata_task_ids(&wal_dir).unwrap();
        assert!(task_ids.is_empty());
    }
}
