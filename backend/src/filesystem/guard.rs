// 路径安全守卫
//
// 提供路径安全检查功能，防止路径穿越攻击

use std::path::{Path, PathBuf};

use super::types::{FilesystemConfig, FsError, FsErrorCode};

/// 路径安全守卫
#[derive(Debug, Clone)]
pub struct PathGuard {
    config: FilesystemConfig,
}

impl PathGuard {
    /// 创建新的路径守卫
    pub fn new(config: FilesystemConfig) -> Self {
        Self { config }
    }

    /// 白名单是否已启用
    pub fn has_allowed_paths(&self) -> bool {
        !self.config.allowed_paths.is_empty()
    }

    /// 解析并排序根目录列表。
    ///
    /// 当配置了 default_path 时，会优先放到返回列表第一位。
    pub fn resolve_allowed_roots(&self) -> Result<Vec<PathBuf>, FsError> {
        let mut roots = Vec::new();

        for allowed in &self.config.allowed_paths {
            let canonical = self.normalize_existing_directory(allowed)?;
            Self::push_unique_path(&mut roots, canonical);
        }

        if let Some(default_path) = self.resolve_default_directory()? {
            roots.retain(|path| path != &default_path);
            roots.insert(0, default_path);
        }

        Ok(roots)
    }

    /// 解析默认目录，并校验其处于白名单范围内。
    pub fn resolve_default_directory(&self) -> Result<Option<PathBuf>, FsError> {
        let Some(default_path) = self.config.default_path.as_deref() else {
            return Ok(None);
        };

        let canonical = self.normalize_existing_directory(default_path)?;
        if self.has_allowed_paths() && !self.is_allowed(&canonical) {
            return Err(FsError::new(FsErrorCode::PathNotAllowed).with_path(default_path));
        }

        Ok(Some(canonical))
    }

    /// 检查路径是否在白名单内
    ///
    /// 如果白名单为空，表示允许所有路径
    pub fn is_allowed(&self, path: &Path) -> bool {
        // 白名单为空表示允许所有
        if self.config.allowed_paths.is_empty() {
            return true;
        }

        // 规范化待检查路径
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => return false,
        };

        // 检查是否在任一白名单路径下
        for allowed in &self.config.allowed_paths {
            let allowed_path = PathBuf::from(allowed);
            if let Ok(allowed_canonical) = allowed_path.canonicalize() {
                if canonical.starts_with(&allowed_canonical) {
                    return true;
                }
            }
        }

        false
    }

    /// 规范化路径（防止 ../ 穿越）
    ///
    /// 返回规范化后的绝对路径
    pub fn normalize(&self, path: &str) -> Result<PathBuf, FsError> {
        // 检查是否包含可疑的穿越序列
        if self.contains_traversal(path) {
            return Err(FsError::new(FsErrorCode::PathTraversalDetected).with_path(path));
        }

        let path_buf = PathBuf::from(path);

        // 对于 Windows，处理驱动器根目录
        #[cfg(target_os = "windows")]
        {
            // 如果是驱动器根目录（如 "C:" 或 "C:\"），直接返回
            if path.len() >= 2 && path.chars().nth(1) == Some(':') {
                let drive_path = if path.len() == 2 {
                    format!("{}\\", path)
                } else {
                    path.to_string()
                };
                let normalized = PathBuf::from(&drive_path);
                // 检查驱动器是否存在
                if normalized.exists() {
                    return Ok(normalized);
                }
            }
        }

        // 尝试规范化路径
        match path_buf.canonicalize() {
            Ok(canonical) => {
                // 检查白名单
                if !self.is_allowed(&canonical) {
                    return Err(FsError::new(FsErrorCode::PathNotAllowed).with_path(path));
                }
                Ok(canonical)
            }
            Err(_) => {
                // 路径不存在或无法访问
                Err(FsError::new(FsErrorCode::DirectoryNotFound).with_path(path))
            }
        }
    }

    /// 检查是否为隐藏文件
    pub fn is_hidden(&self, path: &Path) -> bool {
        if self.config.show_hidden {
            return false;
        }

        // Unix: 以 . 开头的文件
        if let Some(name) = path.file_name() {
            if let Some(name_str) = name.to_str() {
                if name_str.starts_with('.') {
                    return true;
                }
            }
        }

        // Windows: 检查隐藏属性
        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::MetadataExt;
            if let Ok(metadata) = path.metadata() {
                const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
                if metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN != 0 {
                    return true;
                }
            }
        }

        false
    }

    /// 检查是否为符号链接
    pub fn is_symlink(&self, path: &Path) -> bool {
        path.symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }

    /// 检查是否应该跳过符号链接
    pub fn should_skip_symlink(&self, path: &Path) -> bool {
        if self.config.follow_symlinks {
            return false;
        }
        self.is_symlink(path)
    }

    /// 将目录路径规范化为已存在的绝对目录
    fn normalize_existing_directory(&self, path: &str) -> Result<PathBuf, FsError> {
        let canonical = PathBuf::from(path)
            .canonicalize()
            .map_err(|_| FsError::new(FsErrorCode::DirectoryNotFound).with_path(path))?;

        if !canonical.is_dir() {
            return Err(FsError::new(FsErrorCode::NotADirectory).with_path(path));
        }

        Ok(canonical)
    }

    fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
        if !paths.iter().any(|existing| existing == &path) {
            paths.push(path);
        }
    }

    /// 检查路径是否包含穿越序列
    fn contains_traversal(&self, path: &str) -> bool {
        // 检查常见的穿越模式
        let patterns = [
            "..",
            "%2e%2e",     // URL 编码
            "%252e%252e", // 双重 URL 编码
        ];

        let path_lower = path.to_lowercase();
        for pattern in &patterns {
            if path_lower.contains(pattern) {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_guard_default() {
        let guard = PathGuard::new(FilesystemConfig::default());

        // 默认白名单为空，应该允许所有存在的路径
        let current_dir = std::env::current_dir().unwrap();
        assert!(guard.is_allowed(&current_dir));
    }

    #[test]
    fn test_traversal_detection() {
        let guard = PathGuard::new(FilesystemConfig::default());

        assert!(guard.contains_traversal("../etc/passwd"));
        assert!(guard.contains_traversal("/home/user/../root"));
        assert!(guard.contains_traversal("%2e%2e/etc"));
        assert!(!guard.contains_traversal("/home/user/files"));
    }

    #[test]
    fn test_hidden_files() {
        let config = FilesystemConfig {
            show_hidden: false,
            ..Default::default()
        };
        let guard = PathGuard::new(config);

        assert!(guard.is_hidden(Path::new("/home/user/.bashrc")));
        assert!(guard.is_hidden(Path::new(".gitignore")));
        assert!(!guard.is_hidden(Path::new("normal_file.txt")));
    }

    #[test]
    fn test_hidden_files_shown() {
        let config = FilesystemConfig {
            show_hidden: true,
            ..Default::default()
        };
        let guard = PathGuard::new(config);

        // 当 show_hidden = true 时，不应该隐藏任何文件
        assert!(!guard.is_hidden(Path::new("/home/user/.bashrc")));
        assert!(!guard.is_hidden(Path::new(".gitignore")));
    }

    #[test]
    fn test_resolve_allowed_roots_prioritizes_default_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let primary = temp_dir.path().join("primary");
        let secondary = temp_dir.path().join("secondary");
        std::fs::create_dir_all(&primary).unwrap();
        std::fs::create_dir_all(&secondary).unwrap();

        let guard = PathGuard::new(FilesystemConfig {
            allowed_paths: vec![
                primary.to_string_lossy().to_string(),
                secondary.to_string_lossy().to_string(),
            ],
            default_path: Some(secondary.to_string_lossy().to_string()),
            ..Default::default()
        });

        let roots = guard.resolve_allowed_roots().unwrap();
        assert_eq!(roots[0], secondary.canonicalize().unwrap());
        assert_eq!(roots.len(), 2);
    }
}
