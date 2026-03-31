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
    /// 不存在的白名单路径会被跳过（打 warning），不会导致整个列表失败。
    /// 当配置了 default_path 时，会优先放到返回列表第一位。
    pub fn resolve_allowed_roots(&self) -> Result<Vec<PathBuf>, FsError> {
        let mut roots = Vec::new();

        for allowed in &self.config.allowed_paths {
            match self.normalize_existing_directory(allowed) {
                Ok(canonical) => Self::push_unique_path(&mut roots, canonical),
                Err(_) => {
                    tracing::warn!(
                        "白名单路径不存在或无法访问，已跳过: {:?}",
                        allowed
                    );
                }
            }
        }

        if let Some(default_path) = self.resolve_default_directory()? {
            roots.retain(|path| path != &default_path);
            roots.insert(0, default_path);
        }

        Ok(roots)
    }

    /// 解析默认目录，并校验其处于白名单范围内。
    ///
    /// 如果 default_path 不存在，返回 None（打 warning），不会报错。
    pub fn resolve_default_directory(&self) -> Result<Option<PathBuf>, FsError> {
        let Some(default_path) = self.config.default_path.as_deref() else {
            return Ok(None);
        };

        let canonical = match self.normalize_existing_directory(default_path) {
            Ok(c) => c,
            Err(_) => {
                tracing::warn!(
                    "默认目录不存在或无法访问，已忽略: {:?}",
                    default_path
                );
                return Ok(None);
            }
        };

        if self.has_allowed_paths() && !self.is_allowed(&canonical) {
            return Err(FsError::new(FsErrorCode::PathNotAllowed).with_path(default_path));
        }

        Ok(Some(canonical))
    }

    /// 检查路径是否恰好是某个白名单根目录（而非其子目录）。
    ///
    /// 用于判断"向上导航"时是否应该回到根列表。
    pub fn is_allowed_root(&self, path: &Path) -> bool {
        if self.config.allowed_paths.is_empty() {
            return false;
        }
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => return false,
        };
        for allowed in &self.config.allowed_paths {
            if let Ok(allowed_canonical) = PathBuf::from(allowed).canonicalize() {
                if canonical == allowed_canonical {
                    return true;
                }
            }
        }
        false
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

        // 对于 Windows，仅处理裸驱动器根目录（"C:" 或 "C:\" / "C:/"）。
        // drive-relative 路径（如 "C:Windows"）以及完整绝对路径（"C:\Users\foo"）
        // 必须走下方的 canonicalize() 流程，以保证返回规范化绝对路径。
        #[cfg(target_os = "windows")]
        {
            let bytes = path.as_bytes();
            let is_bare_drive_root = bytes.len() >= 2
                && bytes[0].is_ascii_alphabetic()
                && bytes[1] == b':'
                && (bytes.len() == 2
                || (bytes.len() == 3 && (bytes[2] == b'\\' || bytes[2] == b'/')));

            if is_bare_drive_root {
                let drive_path = if bytes.len() == 2 {
                    format!("{}\\", path)
                } else {
                    // 统一为反斜杠形式
                    format!("{}\\", &path[..2])
                };
                let normalized = PathBuf::from(&drive_path);
                if normalized.exists() {
                    if !self.is_allowed(&normalized) {
                        return Err(FsError::new(FsErrorCode::PathNotAllowed).with_path(path));
                    }
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

    #[test]
    fn test_normalize_blocks_path_outside_allowlist() {
        let temp_dir = tempfile::tempdir().unwrap();
        let allowed = temp_dir.path().join("allowed");
        let forbidden = temp_dir.path().join("forbidden");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&forbidden).unwrap();

        let guard = PathGuard::new(FilesystemConfig {
            allowed_paths: vec![allowed.to_string_lossy().to_string()],
            ..Default::default()
        });

        // 白名单内路径应该成功
        assert!(guard.normalize(&allowed.to_string_lossy()).is_ok());

        // 白名单外路径应该失败
        let result = guard.normalize(&forbidden.to_string_lossy());
        assert!(result.is_err());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_windows_drive_letter_blocked_by_allowlist() {
        // 在 Windows 上，当白名单只允许特定子目录时，
        // 直接访问驱动器根目录（如 "C:"）应该被阻止
        let temp_dir = tempfile::tempdir().unwrap();
        let allowed = temp_dir.path().join("allowed");
        std::fs::create_dir_all(&allowed).unwrap();

        let guard = PathGuard::new(FilesystemConfig {
            allowed_paths: vec![allowed.to_string_lossy().to_string()],
            ..Default::default()
        });

        // C:\ 不在白名单内，应该被拒绝
        let result = guard.normalize("C:\\");
        assert!(result.is_err(), "驱动器根目录应该被白名单拦截");
    }

    #[test]
    fn test_is_allowed_root() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_a = temp_dir.path().join("root_a");
        let root_b = temp_dir.path().join("root_b");
        let sub_dir = root_a.join("subdir");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        std::fs::create_dir_all(&sub_dir).unwrap();

        let guard = PathGuard::new(FilesystemConfig {
            allowed_paths: vec![
                root_a.to_string_lossy().to_string(),
                root_b.to_string_lossy().to_string(),
            ],
            ..Default::default()
        });

        // 白名单根目录应该被识别
        assert!(guard.is_allowed_root(&root_a));
        assert!(guard.is_allowed_root(&root_b));

        // 子目录不是根目录
        assert!(!guard.is_allowed_root(&sub_dir));

        // 空白名单时任何目录都不是 allowed root
        let empty_guard = PathGuard::new(FilesystemConfig::default());
        assert!(!empty_guard.is_allowed_root(&root_a));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_normalize_drive_relative_path_goes_through_canonicalize() {
        // drive-relative 路径（如 "C:Windows"）不应走裸驱动器根分支，
        // 而应落入 canonicalize() 流程，返回规范化绝对路径。
        let guard = PathGuard::new(FilesystemConfig::default());

        // C:Windows 是一个 drive-relative 路径，canonicalize 会将其解析为
        // 绝对路径（取决于进程在 C: 上的 CWD）。
        // 关键断言：返回结果必须是以 "X:\" 开头的绝对路径，而非原样返回。
        let result = guard.normalize("C:Windows");
        match result {
            Ok(p) => {
                let s = p.to_string_lossy().to_string();
                // canonicalize 在 Windows 上可能返回 \\?\C:\... 或 C:\... 形式
                let is_absolute = s.starts_with("\\\\?\\")
                    || (s.len() >= 3 && s.as_bytes()[1] == b':' && s.as_bytes()[2] == b'\\');
                assert!(
                    is_absolute,
                    "drive-relative 路径应被 canonicalize 为完全限定绝对路径，实际: {}",
                    s
                );
                // 不能是原样返回 "C:Windows"
                assert_ne!(
                    s, "C:Windows",
                    "drive-relative 路径不应被原样返回"
                );
            }
            Err(_) => {
                // 路径不存在也是可接受的（canonicalize 会报错），
                // 重要的是它没有走裸驱动器根分支原样返回。
            }
        }
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_normalize_bare_drive_root_variants() {
        // 测试裸驱动器根的各种形式都能正确处理
        let guard = PathGuard::new(FilesystemConfig::default());

        // "C:" → "C:\"
        let result = guard.normalize("C:");
        if let Ok(p) = result {
            assert_eq!(
                p.to_string_lossy().as_ref(),
                "C:\\",
                "C: 应被规范化为 C:\\"
            );
        }

        // "C:\" → "C:\"
        let result = guard.normalize("C:\\");
        if let Ok(p) = result {
            assert_eq!(
                p.to_string_lossy().as_ref(),
                "C:\\",
                "C:\\ 应保持不变"
            );
        }

        // "C:/" → "C:\"
        let result = guard.normalize("C:/");
        if let Ok(p) = result {
            assert_eq!(
                p.to_string_lossy().as_ref(),
                "C:\\",
                "C:/ 应被规范化为 C:\\"
            );
        }
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_normalize_full_absolute_path_goes_through_canonicalize() {
        // 完整绝对路径（如 "C:\Users"）不应走裸驱动器根分支
        let guard = PathGuard::new(FilesystemConfig::default());

        let result = guard.normalize("C:\\Users");
        match result {
            Ok(p) => {
                // canonicalize 的结果应是以 \\?\ 前缀或 X:\ 开头的规范路径
                let s = p.to_string_lossy().to_string();
                assert!(
                    s.contains("Users"),
                    "C:\\Users 应被正确解析，实际: {}",
                    s
                );
            }
            Err(_) => {
                // 白名单拦截也是正确行为
            }
        }
    }
}
