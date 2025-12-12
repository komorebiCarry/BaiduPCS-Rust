//! WAL (Write-Ahead Log) 文件操作
//!
//! 实现 WAL 文件的读写功能，用于记录分片完成进度
//!
//! ## 文件格式
//!
//! WAL 文件为纯文本格式，每行一条记录：
//! ```text
//! {chunk_index},{md5},{timestamp_ms}
//! ```
//!
//! - `chunk_index`: 分片索引（0-based）
//! - `md5`: 分片 MD5（上传任务需要，下载任务为空）
//! - `timestamp_ms`: 记录时间戳（Unix 毫秒）

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use tracing::{debug, error, warn};

use super::types::WalRecord;

/// WAL 文件扩展名
const WAL_EXTENSION: &str = "wal";

// ============================================================================
// 辅助函数
// ============================================================================

/// 获取 WAL 文件路径
///
/// # Arguments
/// * `wal_dir` - WAL 目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// WAL 文件的完整路径：`{wal_dir}/{task_id}.wal`
pub fn get_wal_path(wal_dir: &Path, task_id: &str) -> PathBuf {
    wal_dir.join(format!("{}.{}", task_id, WAL_EXTENSION))
}

/// 检查 WAL 文件是否存在
///
/// # Arguments
/// * `wal_dir` - WAL 目录
/// * `task_id` - 任务 ID
pub fn wal_exists(wal_dir: &Path, task_id: &str) -> bool {
    get_wal_path(wal_dir, task_id).exists()
}

/// 删除 WAL 文件
///
/// # Arguments
/// * `wal_dir` - WAL 目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// - `Ok(true)` - 文件已删除
/// - `Ok(false)` - 文件不存在
/// - `Err` - 删除失败
pub fn delete_wal_file(wal_dir: &Path, task_id: &str) -> io::Result<bool> {
    let path = get_wal_path(wal_dir, task_id);
    if path.exists() {
        fs::remove_file(&path)?;
        debug!("已删除 WAL 文件: {:?}", path);
        Ok(true)
    } else {
        Ok(false)
    }
}

/// 确保 WAL 目录存在
///
/// # Arguments
/// * `wal_dir` - WAL 目录路径
pub fn ensure_wal_dir(wal_dir: &Path) -> io::Result<()> {
    if !wal_dir.exists() {
        fs::create_dir_all(wal_dir)?;
        debug!("已创建 WAL 目录: {:?}", wal_dir);
    }
    Ok(())
}

// ============================================================================
// WalWriter - WAL 写入器
// ============================================================================

/// WAL 写入器
///
/// 用于向 WAL 文件追加记录
///
/// # Example
/// ```ignore
/// let mut writer = WalWriter::new(&wal_dir, "task_123")?;
/// writer.append(&[WalRecord::new_download(0)])?;
/// writer.flush()?;
/// writer.close()?;
/// ```
pub struct WalWriter {
    /// 任务 ID
    task_id: String,
    /// WAL 文件路径
    path: PathBuf,
    /// 带缓冲的文件写入器
    writer: Option<BufWriter<File>>,
}

impl WalWriter {
    /// 创建 WAL 写入器
    ///
    /// 如果 WAL 文件不存在，会自动创建
    /// 如果文件已存在，会以追加模式打开
    ///
    /// # Arguments
    /// * `wal_dir` - WAL 目录
    /// * `task_id` - 任务 ID
    pub fn new(wal_dir: &Path, task_id: &str) -> io::Result<Self> {
        // 确保目录存在
        ensure_wal_dir(wal_dir)?;

        let path = get_wal_path(wal_dir, task_id);

        // 以追加模式打开文件
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let writer = BufWriter::new(file);

        debug!("已创建 WAL 写入器: {:?}", path);

        Ok(Self {
            task_id: task_id.to_string(),
            path,
            writer: Some(writer),
        })
    }

    /// 追加记录到 WAL 文件
    ///
    /// 记录会先写入缓冲区，需要调用 `flush()` 才会真正写入磁盘
    ///
    /// # Arguments
    /// * `records` - 要追加的记录列表
    pub fn append(&mut self, records: &[WalRecord]) -> io::Result<()> {
        if let Some(ref mut writer) = self.writer {
            for record in records {
                let line = record.to_wal_line();
                writeln!(writer, "{}", line)?;
            }
            debug!(
                "已追加 {} 条记录到 WAL (task_id={})",
                records.len(),
                self.task_id
            );
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "WAL writer already closed",
            ))
        }
    }

    /// 刷写缓冲区到磁盘
    ///
    /// 确保所有已追加的记录都写入磁盘
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            debug!("已刷写 WAL 到磁盘 (task_id={})", self.task_id);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "WAL writer already closed",
            ))
        }
    }

    /// 关闭 WAL 写入器
    ///
    /// 关闭前会自动刷写缓冲区
    pub fn close(&mut self) -> io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            debug!("已关闭 WAL 写入器 (task_id={})", self.task_id);
        }
        Ok(())
    }

    /// 获取 WAL 文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 获取任务 ID
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if self.writer.is_some() {
            if let Err(e) = self.close() {
                error!("关闭 WAL 写入器失败 (task_id={}): {}", self.task_id, e);
            }
        }
    }
}

// ============================================================================
// WalReader - WAL 读取器
// ============================================================================

/// WAL 读取器
///
/// 用于读取 WAL 文件中的所有记录
///
/// # Example
/// ```ignore
/// let reader = WalReader::new(&wal_path)?;
/// let records = reader.read_all()?;
/// for record in records {
///     println!("chunk {} completed", record.chunk_index);
/// }
/// ```
#[derive(Debug)]
pub struct WalReader {
    /// WAL 文件路径
    path: PathBuf,
}

impl WalReader {
    /// 创建 WAL 读取器
    ///
    /// # Arguments
    /// * `wal_path` - WAL 文件路径
    ///
    /// # Returns
    /// - `Ok(WalReader)` - 成功创建读取器
    /// - `Err` - 文件不存在或无法打开
    pub fn new(wal_path: &Path) -> io::Result<Self> {
        if !wal_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("WAL file not found: {:?}", wal_path),
            ));
        }

        Ok(Self {
            path: wal_path.to_path_buf(),
        })
    }

    /// 从 WAL 目录和任务 ID 创建读取器
    ///
    /// # Arguments
    /// * `wal_dir` - WAL 目录
    /// * `task_id` - 任务 ID
    pub fn from_task(wal_dir: &Path, task_id: &str) -> io::Result<Self> {
        let path = get_wal_path(wal_dir, task_id);
        Self::new(&path)
    }

    /// 读取所有记录
    ///
    /// 容错处理：跳过无法解析的行，并记录警告日志
    ///
    /// # Returns
    /// 成功解析的所有记录列表
    pub fn read_all(&self) -> io::Result<Vec<WalRecord>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);

        let mut records = Vec::new();
        let mut line_number = 0;
        let mut skipped = 0;

        for line_result in reader.lines() {
            line_number += 1;

            match line_result {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    match Self::parse_line(line) {
                        Some(record) => records.push(record),
                        None => {
                            warn!(
                                "WAL 解析失败 (行 {}): {:?} in {:?}",
                                line_number, line, self.path
                            );
                            skipped += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "WAL 读取失败 (行 {}): {} in {:?}",
                        line_number, e, self.path
                    );
                    skipped += 1;
                }
            }
        }

        if skipped > 0 {
            warn!(
                "WAL 读取完成，跳过 {} 行无效记录 (共 {} 行) in {:?}",
                skipped, line_number, self.path
            );
        }

        debug!("已读取 {} 条 WAL 记录 from {:?}", records.len(), self.path);

        Ok(records)
    }

    /// 解析单行 WAL 记录（容错）
    ///
    /// # Arguments
    /// * `line` - WAL 行内容
    ///
    /// # Returns
    /// - `Some(WalRecord)` - 解析成功
    /// - `None` - 解析失败
    pub fn parse_line(line: &str) -> Option<WalRecord> {
        WalRecord::from_wal_line(line)
    }

    /// 获取 WAL 文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ============================================================================
// 批量操作函数
// ============================================================================

/// 批量追加记录到 WAL 文件
///
/// 这是一个便捷函数，用于一次性写入多条记录
///
/// # Arguments
/// * `wal_dir` - WAL 目录
/// * `task_id` - 任务 ID
/// * `records` - 要追加的记录列表
pub fn append_records(wal_dir: &Path, task_id: &str, records: &[WalRecord]) -> io::Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let mut writer = WalWriter::new(wal_dir, task_id)?;
    writer.append(records)?;
    writer.flush()?;
    writer.close()?;

    Ok(())
}

/// 读取 WAL 文件中的所有记录
///
/// 这是一个便捷函数
///
/// # Arguments
/// * `wal_dir` - WAL 目录
/// * `task_id` - 任务 ID
///
/// # Returns
/// - `Ok(Vec<WalRecord>)` - 成功读取的记录
/// - `Err` - 文件不存在或读取失败
pub fn read_records(wal_dir: &Path, task_id: &str) -> io::Result<Vec<WalRecord>> {
    let reader = WalReader::from_task(wal_dir, task_id)?;
    reader.read_all()
}

/// 扫描 WAL 目录中的所有任务 ID
///
/// # Arguments
/// * `wal_dir` - WAL 目录
///
/// # Returns
/// 所有 WAL 文件对应的任务 ID 列表
pub fn scan_wal_task_ids(wal_dir: &Path) -> io::Result<Vec<String>> {
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }

    let mut task_ids = Vec::new();

    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == WAL_EXTENSION {
                    if let Some(stem) = path.file_stem() {
                        if let Some(task_id) = stem.to_str() {
                            task_ids.push(task_id.to_string());
                        }
                    }
                }
            }
        }
    }

    debug!("扫描到 {} 个 WAL 文件", task_ids.len());

    Ok(task_ids)
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temp dir")
    }

    #[test]
    fn test_get_wal_path() {
        let wal_dir = Path::new("/tmp/wal");
        let path = get_wal_path(wal_dir, "task_123");
        assert_eq!(path, PathBuf::from("/tmp/wal/task_123.wal"));
    }

    #[test]
    fn test_wal_exists() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 文件不存在
        assert!(!wal_exists(wal_dir, "task_123"));

        // 创建文件
        let path = get_wal_path(wal_dir, "task_123");
        fs::create_dir_all(wal_dir).unwrap();
        File::create(&path).unwrap();

        // 文件存在
        assert!(wal_exists(wal_dir, "task_123"));
    }

    #[test]
    fn test_delete_wal_file() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 删除不存在的文件
        let result = delete_wal_file(wal_dir, "task_123");
        assert!(result.is_ok());
        assert!(!result.unwrap());

        // 创建并删除文件
        let path = get_wal_path(wal_dir, "task_123");
        fs::create_dir_all(wal_dir).unwrap();
        File::create(&path).unwrap();
        assert!(path.exists());

        let result = delete_wal_file(wal_dir, "task_123");
        assert!(result.is_ok());
        assert!(result.unwrap());
        assert!(!path.exists());
    }

    #[test]
    fn test_wal_writer_basic() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建写入器
        let mut writer = WalWriter::new(wal_dir, "task_001").unwrap();

        // 追加记录
        let records = vec![
            WalRecord::new_download(0),
            WalRecord::new_download(1),
            WalRecord::new_download(2),
        ];
        writer.append(&records).unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();

        // 验证文件存在
        assert!(wal_exists(wal_dir, "task_001"));

        // 读取并验证
        let reader = WalReader::from_task(wal_dir, "task_001").unwrap();
        let read_records = reader.read_all().unwrap();
        assert_eq!(read_records.len(), 3);
        assert_eq!(read_records[0].chunk_index, 0);
        assert_eq!(read_records[1].chunk_index, 1);
        assert_eq!(read_records[2].chunk_index, 2);
    }

    #[test]
    fn test_wal_writer_append_mode() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 第一次写入
        {
            let mut writer = WalWriter::new(wal_dir, "task_002").unwrap();
            writer.append(&[WalRecord::new_download(0)]).unwrap();
            writer.flush().unwrap();
        }

        // 第二次追加
        {
            let mut writer = WalWriter::new(wal_dir, "task_002").unwrap();
            writer.append(&[WalRecord::new_download(1)]).unwrap();
            writer.flush().unwrap();
        }

        // 验证两条记录都存在
        let records = read_records(wal_dir, "task_002").unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].chunk_index, 0);
        assert_eq!(records[1].chunk_index, 1);
    }

    #[test]
    fn test_wal_reader_with_md5() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 写入带 MD5 的记录
        let mut writer = WalWriter::new(wal_dir, "task_003").unwrap();
        let records = vec![
            WalRecord::new_upload(0, "md5_chunk_0".to_string()),
            WalRecord::new_upload(1, "md5_chunk_1".to_string()),
        ];
        writer.append(&records).unwrap();
        writer.flush().unwrap();

        // 读取并验证
        let read_records = read_records(wal_dir, "task_003").unwrap();
        assert_eq!(read_records.len(), 2);
        assert_eq!(read_records[0].md5, Some("md5_chunk_0".to_string()));
        assert_eq!(read_records[1].md5, Some("md5_chunk_1".to_string()));
    }

    #[test]
    fn test_wal_reader_fault_tolerance() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 手动写入包含无效行的 WAL 文件
        fs::create_dir_all(wal_dir).unwrap();
        let path = get_wal_path(wal_dir, "task_004");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "0,,1700000000000").unwrap(); // 有效
        writeln!(file, "invalid_line").unwrap(); // 无效（非数字）
        writeln!(file, "1,md5_1,1700000000001").unwrap(); // 有效
        writeln!(file, "").unwrap(); // 空行（跳过）
        writeln!(file, "2,,1700000000002").unwrap(); // 有效

        // 读取并验证容错
        let records = read_records(wal_dir, "task_004").unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].chunk_index, 0);
        assert_eq!(records[1].chunk_index, 1);
        assert_eq!(records[2].chunk_index, 2);
    }

    #[test]
    fn test_scan_wal_task_ids() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 创建多个 WAL 文件
        fs::create_dir_all(wal_dir).unwrap();
        File::create(wal_dir.join("task_a.wal")).unwrap();
        File::create(wal_dir.join("task_b.wal")).unwrap();
        File::create(wal_dir.join("task_c.wal")).unwrap();
        File::create(wal_dir.join("task_d.meta")).unwrap(); // 非 WAL 文件

        // 扫描
        let mut task_ids = scan_wal_task_ids(wal_dir).unwrap();
        task_ids.sort();

        assert_eq!(task_ids.len(), 3);
        assert_eq!(task_ids, vec!["task_a", "task_b", "task_c"]);
    }

    #[test]
    fn test_append_records_convenience() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 使用便捷函数追加
        let records = vec![WalRecord::new_download(5), WalRecord::new_download(10)];
        append_records(wal_dir, "task_005", &records).unwrap();

        // 验证
        let read_records = read_records(wal_dir, "task_005").unwrap();
        assert_eq!(read_records.len(), 2);
        assert_eq!(read_records[0].chunk_index, 5);
        assert_eq!(read_records[1].chunk_index, 10);
    }

    #[test]
    fn test_empty_records() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        // 追加空记录列表
        append_records(wal_dir, "task_006", &[]).unwrap();

        // 文件不应该被创建
        assert!(!wal_exists(wal_dir, "task_006"));
    }

    #[test]
    fn test_wal_reader_not_found() {
        let temp_dir = setup_temp_dir();
        let wal_dir = temp_dir.path();

        let result = WalReader::from_task(wal_dir, "nonexistent");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
}
