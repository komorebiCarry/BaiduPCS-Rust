//! 文件夹扫描和批量上传模块
//!
//! 负责:
//! - 递归扫描本地文件夹
//! - 保留目录结构
//! - 批量创建上传任务
//! - 分批扫描支持（内存优化）

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use tracing::{debug, info, warn};

/// 分批扫描配置常量
pub const SCAN_BATCH_SIZE: usize = 1000;

/// 文件扫描结果
#[derive(Debug, Clone)]
pub struct ScannedFile {
    /// 本地文件绝对路径
    pub local_path: PathBuf,
    /// 相对于扫描根目录的路径（用于构建远程路径）
    pub relative_path: PathBuf,
    /// 文件大小（字节）
    pub size: u64,
}

/// 文件夹扫描配置
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// 是否跟随符号链接
    pub follow_symlinks: bool,
    /// 最大文件大小（字节），超过此大小的文件将被跳过
    pub max_file_size: Option<u64>,
    /// 最大文件数量，超过此数量将停止扫描
    pub max_files: Option<usize>,
    /// 跳过隐藏文件（以.开头的文件和文件夹）
    pub skip_hidden: bool,
}

/// 文件夹扫描器
pub struct FolderScanner {
    options: ScanOptions,
}

impl FolderScanner {
    /// 创建默认配置的扫描器
    pub fn new() -> Self {
        Self {
            options: ScanOptions::default(),
        }
    }

    /// 创建自定义配置的扫描器
    pub fn with_options(options: ScanOptions) -> Self {
        Self { options }
    }

    /// 递归扫描文件夹
    ///
    /// # 参数
    /// - `root_path`: 要扫描的文件夹路径
    ///
    /// # 返回
    /// - 扫描到的所有文件列表，按相对路径排序
    pub fn scan<P: AsRef<Path>>(&self, root_path: P) -> Result<Vec<ScannedFile>> {
        let root_path = root_path.as_ref();

        if !root_path.exists() {
            anyhow::bail!("扫描路径不存在: {}", root_path.display());
        }

        if !root_path.is_dir() {
            anyhow::bail!("扫描路径不是文件夹: {}", root_path.display());
        }

        info!("开始扫描文件夹: {}", root_path.display());

        let mut files = Vec::new();
        self.scan_recursive(root_path, root_path, &mut files)?;

        // 按相对路径排序（保证目录结构的顺序）
        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

        info!(
            "文件夹扫描完成: {} 个文件，总大小 {}",
            files.len(),
            format_bytes(files.iter().map(|f| f.size).sum())
        );

        Ok(files)
    }

    /// 递归扫描实现
    fn scan_recursive(
        &self,
        root_path: &Path,
        current_path: &Path,
        files: &mut Vec<ScannedFile>,
    ) -> Result<()> {
        // 检查文件数量限制
        if let Some(max_files) = self.options.max_files {
            if files.len() >= max_files {
                warn!("已达到最大文件数量限制 ({}), 停止扫描", max_files);
                return Ok(());
            }
        }

        // 读取目录条目
        let entries = std::fs::read_dir(current_path)
            .with_context(|| format!("读取目录失败: {}", current_path.display()))?;

        for entry in entries {
            let entry =
                entry.with_context(|| format!("读取目录条目失败: {}", current_path.display()))?;

            let path = entry.path();
            let file_name = entry.file_name();

            // 跳过隐藏文件
            if self.options.skip_hidden {
                if let Some(name) = file_name.to_str() {
                    if name.starts_with('.') {
                        debug!("跳过隐藏文件: {}", path.display());
                        continue;
                    }
                }
            }

            // 检查符号链接
            let metadata = if self.options.follow_symlinks {
                std::fs::metadata(&path)
            } else {
                std::fs::symlink_metadata(&path)
            }
                .with_context(|| format!("读取文件元数据失败: {}", path.display()))?;

            if metadata.is_dir() {
                // 递归扫描子目录
                self.scan_recursive(root_path, &path, files)?;

                // 递归后再次检查限制
                if let Some(max_files) = self.options.max_files {
                    if files.len() >= max_files {
                        return Ok(());
                    }
                }
            } else if metadata.is_file() {
                let size = metadata.len();

                // 检查文件大小限制
                if let Some(max_size) = self.options.max_file_size {
                    if size > max_size {
                        warn!("跳过超大文件: {} ({})", path.display(), format_bytes(size));
                        continue;
                    }
                }

                // 计算相对路径
                let relative_path = path
                    .strip_prefix(root_path)
                    .with_context(|| {
                        format!(
                            "计算相对路径失败: {} (root: {})",
                            path.display(),
                            root_path.display()
                        )
                    })?
                    .to_path_buf();

                debug!(
                    "扫描到文件: {} ({})",
                    relative_path.display(),
                    format_bytes(size)
                );

                files.push(ScannedFile {
                    local_path: path,
                    relative_path,
                    size,
                });

                // 添加文件后检查限制
                if let Some(max_files) = self.options.max_files {
                    if files.len() >= max_files {
                        return Ok(());
                    }
                }
            } else {
                debug!("跳过非常规文件: {}", path.display());
            }
        }

        Ok(())
    }
}

impl Default for FolderScanner {
    fn default() -> Self {
        Self::new()
    }
}

/// 分批扫描迭代器
///
/// 每次迭代返回最多 SCAN_BATCH_SIZE 个文件，避免一次性加载所有文件到内存
#[derive(Debug)]
pub struct BatchedScanIterator {
    /// 待扫描的目录队列
    pending_dirs: VecDeque<PathBuf>,
    /// 待处理的文件队列（已扫描但未返回的文件）
    pending_files: VecDeque<ScannedFile>,
    /// 扫描根目录
    root_path: PathBuf,
    /// 扫描选项
    options: ScanOptions,
    /// 批次大小
    batch_size: usize,
    /// 是否已完成扫描
    finished: bool,
    /// 已扫描的文件总数
    total_scanned: usize,
}

impl BatchedScanIterator {
    /// 创建分批扫描迭代器
    pub fn new<P: AsRef<Path>>(root_path: P, options: ScanOptions) -> Result<Self> {
        Self::with_batch_size(root_path, options, SCAN_BATCH_SIZE)
    }

    /// 创建指定批次大小的分批扫描迭代器
    pub fn with_batch_size<P: AsRef<Path>>(
        root_path: P,
        options: ScanOptions,
        batch_size: usize,
    ) -> Result<Self> {
        let root_path = root_path.as_ref().to_path_buf();

        if !root_path.exists() {
            anyhow::bail!("扫描路径不存在: {}", root_path.display());
        }

        if !root_path.is_dir() {
            anyhow::bail!("扫描路径不是文件夹: {}", root_path.display());
        }

        let mut pending_dirs = VecDeque::new();
        pending_dirs.push_back(root_path.clone());

        info!("开始分批扫描文件夹: {}, batch_size={}", root_path.display(), batch_size);

        Ok(Self {
            pending_dirs,
            pending_files: VecDeque::new(),
            root_path,
            options,
            batch_size,
            finished: false,
            total_scanned: 0,
        })
    }

    /// 获取下一批文件
    ///
    /// 返回 Some(Vec<ScannedFile>) 表示有文件，None 表示扫描完成
    pub fn next_batch(&mut self) -> Result<Option<Vec<ScannedFile>>> {
        if self.finished {
            return Ok(None);
        }

        let mut batch = Vec::with_capacity(self.batch_size);

        // 首先从待处理文件队列中取文件
        while batch.len() < self.batch_size && !self.pending_files.is_empty() {
            if let Some(file) = self.pending_files.pop_front() {
                batch.push(file);
            }
        }

        // 如果批次未满，继续扫描目录
        while batch.len() < self.batch_size && !self.pending_dirs.is_empty() {
            // 检查文件数量限制
            if let Some(max_files) = self.options.max_files {
                if self.total_scanned >= max_files {
                    warn!("已达到最大文件数量限制 ({}), 停止扫描", max_files);
                    self.pending_dirs.clear();
                    break;
                }
            }

            let dir_path = self.pending_dirs.pop_front().unwrap();
            self.scan_directory(&dir_path, &mut batch)?;
        }

        // 检查是否扫描完成
        if batch.is_empty() && self.pending_dirs.is_empty() && self.pending_files.is_empty() {
            self.finished = true;
            info!(
                "分批扫描完成: 总共扫描 {} 个文件",
                self.total_scanned
            );
            return Ok(None);
        }

        // 按相对路径排序当前批次
        batch.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

        debug!(
            "返回批次: {} 个文件, 累计扫描: {}",
            batch.len(),
            self.total_scanned
        );

        Ok(Some(batch))
    }

    /// 扫描单个目录，将文件添加到批次或待处理队列
    fn scan_directory(&mut self, dir_path: &Path, batch: &mut Vec<ScannedFile>) -> Result<()> {
        debug!("开始扫描目录: {}", dir_path.display());

        // 读取目录条目
        let entries: Vec<std::fs::DirEntry> = match std::fs::read_dir(dir_path) {
            Ok(entries) => entries.filter_map(|e| e.ok()).collect(),
            Err(e) => {
                warn!("读取目录失败: {}, error={}", dir_path.display(), e);
                return Ok(()); // 跳过无法读取的目录，继续扫描
            }
        };

        debug!("目录 {} 有 {} 个条目", dir_path.display(), entries.len());

        for entry in entries {
            // 检查文件数量限制
            if let Some(max_files) = self.options.max_files {
                if self.total_scanned >= max_files {
                    warn!("已达到最大文件数量限制 ({}), 停止扫描", max_files);
                    self.pending_dirs.clear();
                    return Ok(());
                }
            }

            let path = entry.path();
            let file_name = entry.file_name();

            // 跳过隐藏文件
            if self.options.skip_hidden {
                if let Some(name) = file_name.to_str() {
                    if name.starts_with('.') {
                        debug!("跳过隐藏文件: {}", path.display());
                        continue;
                    }
                }
            }

            // 检查符号链接
            let metadata = if self.options.follow_symlinks {
                std::fs::metadata(&path)
            } else {
                std::fs::symlink_metadata(&path)
            };

            let metadata = match metadata {
                Ok(m) => m,
                Err(e) => {
                    warn!("读取文件元数据失败: {}, error={}", path.display(), e);
                    continue;
                }
            };

            if metadata.is_dir() {
                // 将子目录加入待扫描队列（总是添加，不受批次限制）
                self.pending_dirs.push_back(path);
            } else if metadata.is_file() {
                let size = metadata.len();

                // 检查文件大小限制
                if let Some(max_size) = self.options.max_file_size {
                    if size > max_size {
                        warn!("跳过超大文件: {} ({})", path.display(), format_bytes(size));
                        continue;
                    }
                }

                // 计算相对路径
                let relative_path = match path.strip_prefix(&self.root_path) {
                    Ok(p) => p.to_path_buf(),
                    Err(e) => {
                        warn!(
                            "计算相对路径失败: {} (root: {}), error={}",
                            path.display(),
                            self.root_path.display(),
                            e
                        );
                        continue;
                    }
                };

                debug!(
                    "扫描到文件: {} ({})",
                    relative_path.display(),
                    format_bytes(size)
                );

                let scanned_file = ScannedFile {
                    local_path: path,
                    relative_path,
                    size,
                };

                self.total_scanned += 1;

                // 如果批次未满，直接添加到批次；否则添加到待处理队列
                if batch.len() < self.batch_size {
                    batch.push(scanned_file);
                } else {
                    self.pending_files.push_back(scanned_file);
                }
            } else {
                debug!("跳过非常规文件: {}", path.display());
            }
        }

        Ok(())
    }

    /// 检查是否还有更多批次
    pub fn has_more(&self) -> bool {
        !self.finished
    }

    /// 获取已扫描的文件总数
    pub fn total_scanned(&self) -> usize {
        self.total_scanned
    }
}

/// 辅助函数：格式化字节大小
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// 创建测试目录结构
    fn create_test_folder() -> TempDir {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // 创建文件夹结构:
        // root/
        // ├── file1.txt
        // ├── file2.txt
        // ├── subdir1/
        // │   ├── file3.txt
        // │   └── file4.txt
        // └── subdir2/
        //     └── subdir3/
        //         └── file5.txt

        fs::write(root.join("file1.txt"), "content1").unwrap();
        fs::write(root.join("file2.txt"), "content2").unwrap();

        fs::create_dir(root.join("subdir1")).unwrap();
        fs::write(root.join("subdir1/file3.txt"), "content3").unwrap();
        fs::write(root.join("subdir1/file4.txt"), "content4").unwrap();

        fs::create_dir(root.join("subdir2")).unwrap();
        fs::create_dir(root.join("subdir2/subdir3")).unwrap();
        fs::write(root.join("subdir2/subdir3/file5.txt"), "content5").unwrap();

        temp_dir
    }

    #[test]
    fn test_scan_folder() {
        let temp_dir = create_test_folder();
        let scanner = FolderScanner::new();

        let files = scanner.scan(temp_dir.path()).unwrap();

        assert_eq!(files.len(), 5, "应该扫描到5个文件");

        // 验证文件顺序（按相对路径排序）
        let relative_paths: Vec<_> = files
            .iter()
            .map(|f| f.relative_path.to_str().unwrap())
            .collect();

        assert!(relative_paths.contains(&"file1.txt"));
        assert!(relative_paths.contains(&"file2.txt"));
        assert!(
            relative_paths.contains(&"subdir1/file3.txt")
                || relative_paths.contains(&"subdir1\\file3.txt")
        );
    }

    #[test]
    fn test_scan_nonexistent_folder() {
        let scanner = FolderScanner::new();
        let result = scanner.scan("/nonexistent/path");

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("扫描路径不存在"));
    }

    #[test]
    fn test_scan_file_not_folder() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        fs::write(&file_path, "content").unwrap();

        let scanner = FolderScanner::new();
        let result = scanner.scan(&file_path);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("扫描路径不是文件夹"));
    }

    #[test]
    fn test_scan_with_max_files_limit() {
        let temp_dir = create_test_folder();
        let options = ScanOptions {
            max_files: Some(3), // 限制最多3个文件
            ..Default::default()
        };
        let scanner = FolderScanner::with_options(options);

        let files = scanner.scan(temp_dir.path()).unwrap();

        println!("扫描到的文件数: {}", files.len());
        for file in &files {
            println!("  - {:?}", file.relative_path);
        }

        assert!(
            files.len() <= 3,
            "应该最多扫描3个文件，实际扫描到 {} 个",
            files.len()
        );
    }

    #[test]
    fn test_scan_skip_hidden_files() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // 创建普通文件和隐藏文件
        fs::write(root.join("normal.txt"), "normal").unwrap();
        fs::write(root.join(".hidden.txt"), "hidden").unwrap();

        let options = ScanOptions {
            skip_hidden: true,
            ..Default::default()
        };
        let scanner = FolderScanner::with_options(options);

        let files = scanner.scan(root).unwrap();

        assert_eq!(files.len(), 1, "应该只扫描到1个文件（跳过隐藏文件）");
        assert_eq!(files[0].relative_path.to_str().unwrap(), "normal.txt");
    }

    #[test]
    fn test_scan_with_max_file_size() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // 创建小文件和大文件
        fs::write(root.join("small.txt"), "small").unwrap();
        fs::write(root.join("large.txt"), "x".repeat(1000)).unwrap();

        let options = ScanOptions {
            max_file_size: Some(100), // 限制最大100字节
            ..Default::default()
        };
        let scanner = FolderScanner::with_options(options);

        let files = scanner.scan(root).unwrap();

        assert_eq!(files.len(), 1, "应该只扫描到1个文件（跳过大文件）");
        assert_eq!(files[0].relative_path.to_str().unwrap(), "small.txt");
    }

    #[test]
    fn test_relative_path_calculation() {
        let temp_dir = create_test_folder();
        let scanner = FolderScanner::new();

        let files = scanner.scan(temp_dir.path()).unwrap();

        // 验证相对路径计算正确
        for file in &files {
            let reconstructed = temp_dir.path().join(&file.relative_path);
            assert_eq!(file.local_path, reconstructed);
        }
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1536 * 1024 * 1024), "1.50 GB");
    }

    // ==================== 分批扫描迭代器测试 ====================

    #[test]
    fn test_batched_scan_basic() {
        let temp_dir = create_test_folder();
        let options = ScanOptions::default();
        let mut iterator = BatchedScanIterator::new(temp_dir.path(), options).unwrap();

        let mut all_files = Vec::new();
        while let Some(batch) = iterator.next_batch().unwrap() {
            all_files.extend(batch);
        }

        assert_eq!(all_files.len(), 5, "应该扫描到5个文件");
    }

    #[test]
    fn test_batched_scan_small_batch_size() {
        let temp_dir = create_test_folder();
        let options = ScanOptions {
            skip_hidden: false,
            ..Default::default()
        };
        // 使用小批次大小测试分批逻辑
        let mut iterator = BatchedScanIterator::with_batch_size(temp_dir.path(), options, 2).unwrap();

        let mut batch_count = 0;
        let mut all_files = Vec::new();

        while let Some(batch) = iterator.next_batch().unwrap() {
            // 每批最多2个文件
            assert!(batch.len() <= 2, "每批最多2个文件，实际: {}", batch.len());
            batch_count += 1;
            all_files.extend(batch);
        }

        assert_eq!(all_files.len(), 5, "总共应该扫描到5个文件");
        assert!(batch_count >= 3, "应该至少有3个批次，实际: {}", batch_count);
    }

    #[test]
    fn test_batched_scan_batch_size_limit() {
        // 创建大量文件测试批次大小限制
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // 创建 50 个文件
        for i in 0..50 {
            fs::write(root.join(format!("file{:03}.txt", i)), format!("content{}", i)).unwrap();
        }

        let options = ScanOptions::default();
        let mut iterator = BatchedScanIterator::with_batch_size(root, options, 10).unwrap();

        let mut batch_count = 0;
        let mut total_files = 0;

        while let Some(batch) = iterator.next_batch().unwrap() {
            // 每批最多10个文件
            assert!(batch.len() <= 10, "每批最多10个文件，实际: {}", batch.len());
            batch_count += 1;
            total_files += batch.len();
        }

        assert_eq!(total_files, 50, "总共应该扫描到50个文件");
        assert_eq!(batch_count, 5, "应该有5个批次");
    }

    #[test]
    fn test_batched_scan_nonexistent_folder() {
        let options = ScanOptions::default();
        let result = BatchedScanIterator::new("/nonexistent/path", options);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("扫描路径不存在"));
    }

    #[test]
    fn test_batched_scan_has_more() {
        let temp_dir = create_test_folder();
        let options = ScanOptions::default();
        let mut iterator = BatchedScanIterator::with_batch_size(temp_dir.path(), options, 2).unwrap();

        assert!(iterator.has_more(), "初始状态应该有更多批次");

        // 消费所有批次
        while iterator.next_batch().unwrap().is_some() {}

        assert!(!iterator.has_more(), "扫描完成后应该没有更多批次");
    }

    #[test]
    fn test_batched_scan_total_scanned() {
        let temp_dir = create_test_folder();
        let options = ScanOptions::default();
        let mut iterator = BatchedScanIterator::with_batch_size(temp_dir.path(), options, 2).unwrap();

        assert_eq!(iterator.total_scanned(), 0, "初始扫描数应为0");

        // 消费所有批次
        while iterator.next_batch().unwrap().is_some() {}

        assert_eq!(iterator.total_scanned(), 5, "最终应该扫描到5个文件");
    }

    #[test]
    fn test_batched_scan_with_max_files() {
        let temp_dir = create_test_folder();
        let options = ScanOptions {
            max_files: Some(3),
            ..Default::default()
        };
        let mut iterator = BatchedScanIterator::new(temp_dir.path(), options).unwrap();

        let mut all_files = Vec::new();
        while let Some(batch) = iterator.next_batch().unwrap() {
            all_files.extend(batch);
        }

        assert!(all_files.len() <= 3, "应该最多扫描3个文件，实际: {}", all_files.len());
    }
}
