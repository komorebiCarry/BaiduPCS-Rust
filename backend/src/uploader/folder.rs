//! 文件夹扫描和批量上传模块
//!
//! 负责:
//! - 递归扫描本地文件夹
//! - 保留目录结构
//! - 批量创建上传任务

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

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
                warn!(
                    "已达到最大文件数量限制 ({}), 停止扫描",
                    max_files
                );
                return Ok(());
            }
        }

        // 读取目录条目
        let entries = std::fs::read_dir(current_path)
            .with_context(|| format!("读取目录失败: {}", current_path.display()))?;

        for entry in entries {
            let entry = entry.with_context(|| {
                format!("读取目录条目失败: {}", current_path.display())
            })?;

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
                        warn!(
                            "跳过超大文件: {} ({})",
                            path.display(),
                            format_bytes(size)
                        );
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
        assert!(relative_paths.contains(&"subdir1/file3.txt") ||
                relative_paths.contains(&"subdir1\\file3.txt"));
    }

    #[test]
    fn test_scan_nonexistent_folder() {
        let scanner = FolderScanner::new();
        let result = scanner.scan("/nonexistent/path");

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("扫描路径不存在"));
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

        assert!(files.len() <= 3, "应该最多扫描3个文件，实际扫描到 {} 个", files.len());
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
}
