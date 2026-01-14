//! 文件系统监听服务
//!
//! 使用 notify crate 实现跨平台文件监听

use anyhow::{anyhow, Result};
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

/// 文件变更事件
#[derive(Debug, Clone)]
pub struct FileChangeEvent {
    /// 配置 ID
    pub config_id: String,
    /// 变更的文件路径列表
    pub paths: Vec<PathBuf>,
    /// 事件类型
    pub event_type: FileChangeType,
}

/// 文件变更类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileChangeType {
    /// 创建
    Created,
    /// 修改
    Modified,
    /// 删除
    Removed,
    /// 重命名
    Renamed,
}

/// 文件监听器
pub struct FileWatcher {
    /// 内部 watcher（Option 用于 Drop）
    watcher: Option<RecommendedWatcher>,
    /// 监听的路径映射（路径 -> 配置 ID）
    watched_paths: Arc<RwLock<std::collections::HashMap<PathBuf, String>>>,
    /// 事件发送通道（预留用于外部事件订阅）
    #[allow(dead_code)]
    event_tx: mpsc::UnboundedSender<FileChangeEvent>,
    /// 是否运行中
    running: Arc<std::sync::atomic::AtomicBool>,
    /// 监听失败计数器（用于自动切换轮询）
    failure_count: Arc<std::sync::atomic::AtomicU32>,
    /// 最大失败次数（超过后切换到轮询模式）
    max_failures: u32,
}

impl FileWatcher {
    /// 创建新的文件监听器
    pub fn new(event_tx: mpsc::UnboundedSender<FileChangeEvent>) -> Result<Self> {
        Self::with_max_failures(event_tx, 3)
    }

    /// 创建新的文件监听器（自定义最大失败次数）
    pub fn with_max_failures(event_tx: mpsc::UnboundedSender<FileChangeEvent>, max_failures: u32) -> Result<Self> {
        let watched_paths = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let watched_paths_clone = watched_paths.clone();
        let event_tx_clone = event_tx.clone();
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let failure_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let failure_count_clone = failure_count.clone();

        // 创建 watcher
        let watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            match res {
                Ok(event) => {
                    // 成功事件，重置失败计数器
                    failure_count_clone.store(0, std::sync::atomic::Ordering::SeqCst);
                    if let Some(change_event) = Self::process_event(&event, &watched_paths_clone) {
                        if let Err(e) = event_tx_clone.send(change_event) {
                            tracing::warn!("Failed to send file change event: {}", e);
                        }
                    }
                }
                Err(e) => {
                    // 增加失败计数
                    let count = failure_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tracing::error!("File watcher error (count: {}): {}", count + 1, e);
                }
            }
        })?;

        Ok(Self {
            watcher: Some(watcher),
            watched_paths,
            event_tx,
            running,
            failure_count,
            max_failures,
        })
    }

    /// 处理 notify 事件
    fn process_event(
        event: &Event,
        watched_paths: &Arc<RwLock<std::collections::HashMap<PathBuf, String>>>,
    ) -> Option<FileChangeEvent> {
        // 过滤事件类型
        let event_type = match &event.kind {
            EventKind::Create(_) => FileChangeType::Created,
            EventKind::Modify(_) => FileChangeType::Modified,
            EventKind::Remove(_) => FileChangeType::Removed,
            _ => return None,
        };

        // 过滤有效路径
        let paths: Vec<PathBuf> = event
            .paths
            .iter()
            .filter(|p| Self::should_watch_file(p))
            .cloned()
            .collect();

        if paths.is_empty() {
            return None;
        }

        // 查找对应的配置 ID
        let watched = watched_paths.read();
        for (watch_path, config_id) in watched.iter() {
            for path in &paths {
                if path.starts_with(watch_path) {
                    return Some(FileChangeEvent {
                        config_id: config_id.clone(),
                        paths: paths.clone(),
                        event_type,
                    });
                }
            }
        }

        None
    }

    /// 判断是否应该监听此文件
    fn should_watch_file(path: &Path) -> bool {
        // 获取文件名
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => return false,
        };

        // 过滤隐藏文件
        if file_name.starts_with('.') {
            return false;
        }

        // 过滤临时文件
        if file_name.ends_with('~')
            || file_name.ends_with(".tmp")
            || file_name.ends_with(".temp")
            || file_name.ends_with(".swp")
            || file_name.ends_with(".bak")
        {
            return false;
        }

        // 过滤系统文件
        let lower_name = file_name.to_lowercase();
        if lower_name == "thumbs.db"
            || lower_name == "desktop.ini"
            || lower_name == ".ds_store"
            || lower_name == "ehthumbs.db"
        {
            return false;
        }

        true
    }

    /// 添加监听路径
    pub fn watch(&mut self, path: &Path, config_id: &str) -> Result<()> {
        if let Some(ref mut watcher) = self.watcher {
            watcher.watch(path, RecursiveMode::Recursive)?;
            self.watched_paths
                .write()
                .insert(path.to_path_buf(), config_id.to_string());
            tracing::info!("Started watching path: {:?} for config: {}", path, config_id);
            Ok(())
        } else {
            Err(anyhow!("Watcher not initialized"))
        }
    }

    /// 移除监听路径
    pub fn unwatch(&mut self, path: &Path) -> Result<()> {
        if let Some(ref mut watcher) = self.watcher {
            watcher.unwatch(path)?;
            self.watched_paths.write().remove(path);
            tracing::info!("Stopped watching path: {:?}", path);
            Ok(())
        } else {
            Err(anyhow!("Watcher not initialized"))
        }
    }

    /// 移除配置的所有监听路径
    pub fn unwatch_config(&mut self, config_id: &str) -> Result<()> {
        let paths_to_remove: Vec<PathBuf> = {
            let watched = self.watched_paths.read();
            watched
                .iter()
                .filter(|(_, id)| *id == config_id)
                .map(|(p, _)| p.clone())
                .collect()
        };

        for path in paths_to_remove {
            self.unwatch(&path)?;
        }

        Ok(())
    }

    /// 获取监听的路径数量
    pub fn watched_count(&self) -> usize {
        self.watched_paths.read().len()
    }

    /// 检查路径是否在监听中
    pub fn is_watching(&self, path: &Path) -> bool {
        self.watched_paths.read().contains_key(path)
    }

    /// 获取所有监听的路径
    pub fn get_watched_paths(&self) -> Vec<(PathBuf, String)> {
        self.watched_paths
            .read()
            .iter()
            .map(|(p, id)| (p.clone(), id.clone()))
            .collect()
    }

    /// 停止监听
    pub fn stop(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
        self.watcher.take();
        self.watched_paths.write().clear();
        tracing::info!("File watcher stopped");
    }

    /// 是否运行中
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst) && self.watcher.is_some()
    }

    /// 检查是否应该切换到轮询模式
    /// 当监听失败次数超过阈值时返回 true
    pub fn should_fallback_to_poll(&self) -> bool {
        self.failure_count.load(std::sync::atomic::Ordering::SeqCst) >= self.max_failures
    }

    /// 获取失败次数
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 重置失败计数器
    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    /// 获取监听状态信息
    pub fn get_status(&self) -> WatcherStatus {
        WatcherStatus {
            running: self.is_running(),
            watched_count: self.watched_count(),
            failure_count: self.failure_count(),
            should_fallback: self.should_fallback_to_poll(),
        }
    }
}

/// 监听器状态信息
#[derive(Debug, Clone)]
pub struct WatcherStatus {
    /// 是否运行中
    pub running: bool,
    /// 监听的路径数量
    pub watched_count: usize,
    /// 失败次数
    pub failure_count: u32,
    /// 是否应该切换到轮询
    pub should_fallback: bool,
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 过滤服务
#[derive(Clone)]
pub struct FilterService {
    /// 包含的扩展名
    pub include_extensions: HashSet<String>,
    /// 排除的扩展名
    pub exclude_extensions: HashSet<String>,
    /// 排除的目录名
    pub exclude_directories: HashSet<String>,
    /// 最大文件大小
    pub max_file_size: u64,
    /// 最小文件大小
    pub min_file_size: u64,
}

impl FilterService {
    pub fn new(
        include_extensions: Vec<String>,
        exclude_extensions: Vec<String>,
        exclude_directories: Vec<String>,
        max_file_size: u64,
        min_file_size: u64,
    ) -> Self {
        Self {
            include_extensions: include_extensions.into_iter().map(|s| s.to_lowercase()).collect(),
            exclude_extensions: exclude_extensions.into_iter().map(|s| s.to_lowercase()).collect(),
            exclude_directories: exclude_directories.into_iter().collect(),
            max_file_size,
            min_file_size,
        }
    }

    /// 从配置创建
    pub fn from_config(config: &super::super::FilterConfig) -> Self {
        Self::new(
            config.include_extensions.clone(),
            config.exclude_extensions.clone(),
            config.exclude_directories.clone(),
            config.max_file_size,
            config.min_file_size,
        )
    }

    /// 检查文件是否应该被处理
    pub fn should_process(&self, path: &Path) -> Result<bool> {
        // 检查是否为文件
        if !path.is_file() {
            return Ok(false);
        }

        // 检查扩展名
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            let ext_lower = ext.to_lowercase();

            // 如果有包含列表，检查是否在列表中
            if !self.include_extensions.is_empty() && !self.include_extensions.contains(&ext_lower) {
                return Ok(false);
            }

            // 检查排除列表
            if self.exclude_extensions.contains(&ext_lower) {
                return Ok(false);
            }
        } else if !self.include_extensions.is_empty() {
            // 没有扩展名但有包含列表，跳过
            return Ok(false);
        }

        // 检查目录
        for ancestor in path.ancestors() {
            if let Some(dir_name) = ancestor.file_name().and_then(|n| n.to_str()) {
                if self.exclude_directories.contains(dir_name) {
                    return Ok(false);
                }
            }
        }

        // 检查文件大小
        let metadata = std::fs::metadata(path)?;
        let file_size = metadata.len();

        if self.max_file_size > 0 && file_size > self.max_file_size {
            return Ok(false);
        }

        if file_size < self.min_file_size {
            return Ok(false);
        }

        Ok(true)
    }

    /// 批量过滤文件
    pub fn filter_files(&self, paths: &[PathBuf]) -> Vec<PathBuf> {
        paths
            .iter()
            .filter(|p| self.should_process(p).unwrap_or(false))
            .cloned()
            .collect()
    }
}

impl Default for FilterService {
    fn default() -> Self {
        Self {
            include_extensions: HashSet::new(),
            exclude_extensions: HashSet::new(),
            exclude_directories: HashSet::new(),
            max_file_size: 0,
            min_file_size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_filter_service_default() {
        let filter = FilterService::default();
        assert!(filter.include_extensions.is_empty());
        assert!(filter.exclude_extensions.is_empty());
        assert!(filter.exclude_directories.is_empty());
        assert_eq!(filter.max_file_size, 0);
        assert_eq!(filter.min_file_size, 0);
    }

    #[test]
    fn test_filter_service_new() {
        let filter = FilterService::new(
            vec!["txt".to_string(), "md".to_string()],
            vec!["exe".to_string()],
            vec!["node_modules".to_string()],
            1024 * 1024, // 1MB
            100,
        );
        assert!(filter.include_extensions.contains("txt"));
        assert!(filter.include_extensions.contains("md"));
        assert!(filter.exclude_extensions.contains("exe"));
        assert!(filter.exclude_directories.contains("node_modules"));
        assert_eq!(filter.max_file_size, 1024 * 1024);
        assert_eq!(filter.min_file_size, 100);
    }

    #[test]
    fn test_filter_service_extension_case_insensitive() {
        let filter = FilterService::new(
            vec!["TXT".to_string(), "MD".to_string()],
            vec!["EXE".to_string()],
            vec![],
            0,
            0,
        );
        // Extensions should be stored in lowercase
        assert!(filter.include_extensions.contains("txt"));
        assert!(filter.include_extensions.contains("md"));
        assert!(filter.exclude_extensions.contains("exe"));
    }

    #[test]
    fn test_filter_service_should_process_include_extension() {
        let dir = tempdir().unwrap();
        let txt_file = dir.path().join("test.txt");
        let md_file = dir.path().join("test.md");
        let exe_file = dir.path().join("test.exe");

        File::create(&txt_file).unwrap().write_all(b"hello").unwrap();
        File::create(&md_file).unwrap().write_all(b"hello").unwrap();
        File::create(&exe_file).unwrap().write_all(b"hello").unwrap();

        let filter = FilterService::new(
            vec!["txt".to_string()],
            vec![],
            vec![],
            0,
            0,
        );

        assert!(filter.should_process(&txt_file).unwrap());
        assert!(!filter.should_process(&md_file).unwrap());
        assert!(!filter.should_process(&exe_file).unwrap());
    }

    #[test]
    fn test_filter_service_should_process_exclude_extension() {
        let dir = tempdir().unwrap();
        let txt_file = dir.path().join("test.txt");
        let exe_file = dir.path().join("test.exe");

        File::create(&txt_file).unwrap().write_all(b"hello").unwrap();
        File::create(&exe_file).unwrap().write_all(b"hello").unwrap();

        let filter = FilterService::new(
            vec![],
            vec!["exe".to_string()],
            vec![],
            0,
            0,
        );

        assert!(filter.should_process(&txt_file).unwrap());
        assert!(!filter.should_process(&exe_file).unwrap());
    }

    #[test]
    fn test_filter_service_should_process_file_size() {
        let dir = tempdir().unwrap();
        let small_file = dir.path().join("small.txt");
        let large_file = dir.path().join("large.txt");

        File::create(&small_file).unwrap().write_all(b"hi").unwrap(); // 2 bytes
        File::create(&large_file).unwrap().write_all(&vec![0u8; 1000]).unwrap(); // 1000 bytes

        // Test max_file_size
        let filter = FilterService::new(vec![], vec![], vec![], 500, 0);
        assert!(filter.should_process(&small_file).unwrap());
        assert!(!filter.should_process(&large_file).unwrap());

        // Test min_file_size
        let filter = FilterService::new(vec![], vec![], vec![], 0, 100);
        assert!(!filter.should_process(&small_file).unwrap());
        assert!(filter.should_process(&large_file).unwrap());
    }

    #[test]
    fn test_filter_service_should_process_exclude_directory() {
        let dir = tempdir().unwrap();
        let node_modules = dir.path().join("node_modules");
        std::fs::create_dir(&node_modules).unwrap();
        let file_in_node_modules = node_modules.join("test.txt");
        let normal_file = dir.path().join("test.txt");

        File::create(&file_in_node_modules).unwrap().write_all(b"hello").unwrap();
        File::create(&normal_file).unwrap().write_all(b"hello").unwrap();

        let filter = FilterService::new(
            vec![],
            vec![],
            vec!["node_modules".to_string()],
            0,
            0,
        );

        assert!(filter.should_process(&normal_file).unwrap());
        assert!(!filter.should_process(&file_in_node_modules).unwrap());
    }

    #[test]
    fn test_filter_service_filter_files() {
        let dir = tempdir().unwrap();
        let txt_file = dir.path().join("test.txt");
        let md_file = dir.path().join("test.md");
        let exe_file = dir.path().join("test.exe");

        File::create(&txt_file).unwrap().write_all(b"hello").unwrap();
        File::create(&md_file).unwrap().write_all(b"hello").unwrap();
        File::create(&exe_file).unwrap().write_all(b"hello").unwrap();

        let filter = FilterService::new(
            vec!["txt".to_string(), "md".to_string()],
            vec![],
            vec![],
            0,
            0,
        );

        let paths = vec![txt_file.clone(), md_file.clone(), exe_file.clone()];
        let filtered = filter.filter_files(&paths);

        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&txt_file));
        assert!(filtered.contains(&md_file));
        assert!(!filtered.contains(&exe_file));
    }

    #[test]
    fn test_should_watch_file_hidden() {
        assert!(!FileWatcher::should_watch_file(Path::new(".hidden")));
        assert!(!FileWatcher::should_watch_file(Path::new(".gitignore")));
        assert!(FileWatcher::should_watch_file(Path::new("normal.txt")));
    }

    #[test]
    fn test_should_watch_file_temp() {
        assert!(!FileWatcher::should_watch_file(Path::new("file~")));
        assert!(!FileWatcher::should_watch_file(Path::new("file.tmp")));
        assert!(!FileWatcher::should_watch_file(Path::new("file.temp")));
        assert!(!FileWatcher::should_watch_file(Path::new("file.swp")));
        assert!(!FileWatcher::should_watch_file(Path::new("file.bak")));
        assert!(FileWatcher::should_watch_file(Path::new("file.txt")));
    }

    #[test]
    fn test_should_watch_file_system() {
        assert!(!FileWatcher::should_watch_file(Path::new("Thumbs.db")));
        assert!(!FileWatcher::should_watch_file(Path::new("desktop.ini")));
        assert!(!FileWatcher::should_watch_file(Path::new(".DS_Store")));
        assert!(FileWatcher::should_watch_file(Path::new("document.pdf")));
    }

    #[test]
    fn test_watcher_status() {
        let status = WatcherStatus {
            running: true,
            watched_count: 5,
            failure_count: 0,
            should_fallback: false,
        };
        assert!(status.running);
        assert_eq!(status.watched_count, 5);
        assert_eq!(status.failure_count, 0);
        assert!(!status.should_fallback);
    }

    #[test]
    fn test_file_watcher_creation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let watcher = FileWatcher::new(tx);
        assert!(watcher.is_ok());

        let watcher = watcher.unwrap();
        assert!(watcher.is_running());
        assert_eq!(watcher.watched_count(), 0);
        assert_eq!(watcher.failure_count(), 0);
        assert!(!watcher.should_fallback_to_poll());
    }

    #[test]
    fn test_file_watcher_with_max_failures() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let watcher = FileWatcher::with_max_failures(tx, 5);
        assert!(watcher.is_ok());

        let watcher = watcher.unwrap();
        assert!(!watcher.should_fallback_to_poll());
    }

    #[test]
    fn test_file_watcher_watch_and_unwatch() {
        let dir = tempdir().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut watcher = FileWatcher::new(tx).unwrap();

        // Watch a directory
        let result = watcher.watch(dir.path(), "config-1");
        assert!(result.is_ok());
        assert_eq!(watcher.watched_count(), 1);
        assert!(watcher.is_watching(dir.path()));

        // Get watched paths
        let paths = watcher.get_watched_paths();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].1, "config-1");

        // Unwatch
        let result = watcher.unwatch(dir.path());
        assert!(result.is_ok());
        assert_eq!(watcher.watched_count(), 0);
        assert!(!watcher.is_watching(dir.path()));
    }

    #[test]
    fn test_file_watcher_unwatch_config() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut watcher = FileWatcher::new(tx).unwrap();

        // Watch multiple directories with same config
        watcher.watch(dir1.path(), "config-1").unwrap();
        watcher.watch(dir2.path(), "config-1").unwrap();
        assert_eq!(watcher.watched_count(), 2);

        // Unwatch by config
        let result = watcher.unwatch_config("config-1");
        assert!(result.is_ok());
        assert_eq!(watcher.watched_count(), 0);
    }

    #[test]
    fn test_file_watcher_stop() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut watcher = FileWatcher::new(tx).unwrap();

        assert!(watcher.is_running());
        watcher.stop();
        assert!(!watcher.is_running());
    }

    #[test]
    fn test_file_watcher_get_status() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let watcher = FileWatcher::new(tx).unwrap();

        let status = watcher.get_status();
        assert!(status.running);
        assert_eq!(status.watched_count, 0);
        assert_eq!(status.failure_count, 0);
        assert!(!status.should_fallback);
    }

    #[test]
    fn test_file_watcher_failure_count_reset() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let watcher = FileWatcher::new(tx).unwrap();

        // Manually test reset (failure count is internal)
        watcher.reset_failure_count();
        assert_eq!(watcher.failure_count(), 0);
    }

    #[test]
    fn test_file_change_event() {
        let event = FileChangeEvent {
            config_id: "test-config".to_string(),
            paths: vec![PathBuf::from("/test/path.txt")],
            event_type: FileChangeType::Created,
        };

        assert_eq!(event.config_id, "test-config");
        assert_eq!(event.paths.len(), 1);
        assert_eq!(event.event_type, FileChangeType::Created);
    }

    #[test]
    fn test_file_change_type_equality() {
        assert_eq!(FileChangeType::Created, FileChangeType::Created);
        assert_eq!(FileChangeType::Modified, FileChangeType::Modified);
        assert_eq!(FileChangeType::Removed, FileChangeType::Removed);
        assert_eq!(FileChangeType::Renamed, FileChangeType::Renamed);
        assert_ne!(FileChangeType::Created, FileChangeType::Modified);
    }
}
