//! 临时文件管理
//!
//! 提供临时加密文件的 RAII 自动清理机制

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// 临时文件守卫
/// 
/// 使用 RAII 模式自动清理临时文件。
/// 当守卫被丢弃时，会自动删除对应的临时文件。
pub struct TempFileGuard {
    /// 临时文件路径
    path: PathBuf,
    /// 是否已被持久化（如果是，则不删除）
    persisted: AtomicBool,
    /// 是否启用自动清理
    auto_cleanup: bool,
}

impl TempFileGuard {
    /// 创建新的临时文件守卫
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            persisted: AtomicBool::new(false),
            auto_cleanup: true,
        }
    }

    /// 创建不自动清理的守卫（仅用于跟踪）
    pub fn new_no_cleanup(path: PathBuf) -> Self {
        Self {
            path,
            persisted: AtomicBool::new(false),
            auto_cleanup: false,
        }
    }

    /// 获取文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 获取文件路径的字符串表示
    pub fn path_str(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    /// 标记文件已持久化（不会被自动删除）
    pub fn persist(&self) {
        self.persisted.store(true, Ordering::SeqCst);
    }

    /// 检查文件是否已持久化
    pub fn is_persisted(&self) -> bool {
        self.persisted.load(Ordering::SeqCst)
    }

    /// 检查文件是否存在
    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// 获取文件大小
    pub fn size(&self) -> std::io::Result<u64> {
        std::fs::metadata(&self.path).map(|m| m.len())
    }

    /// 手动删除文件
    pub fn remove(&self) -> std::io::Result<()> {
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// 重命名/移动文件（同时标记为持久化）
    pub fn rename_to(&self, new_path: &Path) -> std::io::Result<()> {
        std::fs::rename(&self.path, new_path)?;
        self.persist();
        Ok(())
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.auto_cleanup && !self.is_persisted() {
            if let Err(e) = self.remove() {
                tracing::warn!(
                    "清理临时文件失败: {} - {}",
                    self.path.display(),
                    e
                );
            } else if self.path.exists() {
                tracing::debug!("已清理临时文件: {}", self.path.display());
            }
        }
    }
}

impl std::fmt::Debug for TempFileGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempFileGuard")
            .field("path", &self.path)
            .field("persisted", &self.is_persisted())
            .field("auto_cleanup", &self.auto_cleanup)
            .finish()
    }
}

/// 临时文件管理器
/// 
/// 管理临时文件目录，提供创建和清理功能
pub struct TempFileManager {
    /// 临时文件目录
    temp_dir: PathBuf,
    /// 文件前缀
    prefix: String,
}

impl TempFileManager {
    /// 创建新的临时文件管理器
    pub fn new(temp_dir: PathBuf, prefix: &str) -> std::io::Result<Self> {
        // 确保临时目录存在
        std::fs::create_dir_all(&temp_dir)?;
        
        Ok(Self {
            temp_dir,
            prefix: prefix.to_string(),
        })
    }

    /// 获取临时目录路径
    pub fn temp_dir(&self) -> &Path {
        &self.temp_dir
    }

    /// 创建新的临时文件（带守卫）
    pub fn create_temp_file(&self, extension: &str) -> TempFileGuard {
        let filename = format!(
            "{}{}{}",
            self.prefix,
            uuid::Uuid::new_v4(),
            extension
        );
        let path = self.temp_dir.join(filename);
        TempFileGuard::new(path)
    }

    /// 创建新的加密临时文件（带守卫）
    pub fn create_encrypted_temp_file(&self) -> TempFileGuard {
        self.create_temp_file(".bkup.tmp")
    }

    /// 清理所有残留的临时文件
    /// 
    /// 扫描临时目录，删除所有匹配前缀的文件
    pub fn cleanup_all(&self) -> std::io::Result<usize> {
        let mut cleaned = 0;
        
        if let Ok(entries) = std::fs::read_dir(&self.temp_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with(&self.prefix) {
                            if let Err(e) = std::fs::remove_file(&path) {
                                tracing::warn!("删除残留临时文件失败: {} - {}", path.display(), e);
                            } else {
                                tracing::info!("已清理残留临时文件: {}", path.display());
                                cleaned += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(cleaned)
    }

    /// 清理超过指定时间的临时文件
    pub fn cleanup_old(&self, max_age_secs: u64) -> std::io::Result<usize> {
        let mut cleaned = 0;
        let now = std::time::SystemTime::now();
        let max_age = std::time::Duration::from_secs(max_age_secs);

        if let Ok(entries) = std::fs::read_dir(&self.temp_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with(&self.prefix) {
                            // 检查文件年龄
                            if let Ok(metadata) = std::fs::metadata(&path) {
                                if let Ok(modified) = metadata.modified() {
                                    if let Ok(age) = now.duration_since(modified) {
                                        if age > max_age {
                                            if let Err(e) = std::fs::remove_file(&path) {
                                                tracing::warn!("删除过期临时文件失败: {} - {}", path.display(), e);
                                            } else {
                                                tracing::info!("已清理过期临时文件: {}", path.display());
                                                cleaned += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(cleaned)
    }

    /// 获取临时目录占用空间
    pub fn get_temp_dir_size(&self) -> std::io::Result<u64> {
        let mut total = 0u64;
        
        if let Ok(entries) = std::fs::read_dir(&self.temp_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        total += metadata.len();
                    }
                }
            }
        }

        Ok(total)
    }

    /// 获取临时文件数量
    pub fn get_temp_file_count(&self) -> std::io::Result<usize> {
        let mut count = 0;
        
        if let Ok(entries) = std::fs::read_dir(&self.temp_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with(&self.prefix) {
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_temp_file_guard_auto_cleanup() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_temp.tmp");
        
        // 创建文件
        std::fs::write(&file_path, "test content").unwrap();
        assert!(file_path.exists());
        
        // 创建守卫
        {
            let _guard = TempFileGuard::new(file_path.clone());
            assert!(file_path.exists());
        }
        
        // 守卫丢弃后文件应被删除
        assert!(!file_path.exists());
    }

    #[test]
    fn test_temp_file_guard_persist() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_persist.tmp");
        
        std::fs::write(&file_path, "test content").unwrap();
        
        {
            let guard = TempFileGuard::new(file_path.clone());
            guard.persist();
        }
        
        // 持久化后文件不应被删除
        assert!(file_path.exists());
    }

    #[test]
    fn test_temp_file_manager() {
        let dir = tempdir().unwrap();
        let manager = TempFileManager::new(dir.path().to_path_buf(), "test_").unwrap();
        
        // 创建临时文件
        let guard = manager.create_temp_file(".tmp");
        std::fs::write(guard.path(), "test").unwrap();
        assert!(guard.exists());
        
        let path = guard.path().to_path_buf();
        drop(guard);
        
        // 文件应被清理
        assert!(!path.exists());
    }

    #[test]
    fn test_cleanup_all() {
        let dir = tempdir().unwrap();
        let manager = TempFileManager::new(dir.path().to_path_buf(), "backup_").unwrap();
        
        // 创建一些临时文件
        for i in 0..3 {
            let path = dir.path().join(format!("backup_test{}.tmp", i));
            std::fs::write(&path, "test").unwrap();
        }
        
        // 创建一个不匹配前缀的文件
        let other = dir.path().join("other_file.txt");
        std::fs::write(&other, "other").unwrap();
        
        let cleaned = manager.cleanup_all().unwrap();
        assert_eq!(cleaned, 3);
        assert!(other.exists()); // 不匹配前缀的文件不应被删除
    }
}
