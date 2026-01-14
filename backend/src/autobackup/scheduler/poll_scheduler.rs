//! 轮询调度器
//!
//! 负责定时触发备份任务

use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::change_aggregator::{ChangeEvent, GlobalPollType};
use crate::autobackup::config::BackupDirection;

/// 全局轮询 ID 常量
pub const GLOBAL_POLL_UPLOAD_INTERVAL: &str = "global_upload_interval";
pub const GLOBAL_POLL_UPLOAD_SCHEDULED: &str = "global_upload_scheduled";
pub const GLOBAL_POLL_DOWNLOAD_INTERVAL: &str = "global_download_interval";
pub const GLOBAL_POLL_DOWNLOAD_SCHEDULED: &str = "global_download_scheduled";

/// 解析全局轮询 ID，返回 (方向, 轮询类型)
fn parse_global_poll_id(config_id: &str) -> Option<(BackupDirection, GlobalPollType)> {
    match config_id {
        GLOBAL_POLL_UPLOAD_INTERVAL => Some((BackupDirection::Upload, GlobalPollType::Interval)),
        GLOBAL_POLL_UPLOAD_SCHEDULED => Some((BackupDirection::Upload, GlobalPollType::Scheduled)),
        GLOBAL_POLL_DOWNLOAD_INTERVAL => Some((BackupDirection::Download, GlobalPollType::Interval)),
        GLOBAL_POLL_DOWNLOAD_SCHEDULED => Some((BackupDirection::Download, GlobalPollType::Scheduled)),
        _ => None,
    }
}

/// 检查是否为全局轮询 ID
pub fn is_global_poll_id(config_id: &str) -> bool {
    parse_global_poll_id(config_id).is_some()
}

/// 轮询配置
#[derive(Debug, Clone)]
pub struct PollScheduleConfig {
    /// 配置 ID
    pub config_id: String,
    /// 是否启用
    pub enabled: bool,
    /// 轮询间隔
    pub interval: Duration,
    /// 指定时间（可选）
    pub scheduled_time: Option<ScheduledTime>,
}

/// 指定时间
#[derive(Debug, Clone, Copy)]
pub struct ScheduledTime {
    pub hour: u32,
    pub minute: u32,
}

/// 轮询调度器
pub struct PollScheduler {
    /// 事件发送通道
    event_tx: mpsc::UnboundedSender<ChangeEvent>,
    /// 取消令牌
    cancel_token: CancellationToken,
    /// 调度任务句柄
    tasks: HashMap<String, tokio::task::JoinHandle<()>>,
}

impl PollScheduler {
    /// 创建新的轮询调度器
    pub fn new(event_tx: mpsc::UnboundedSender<ChangeEvent>) -> Self {
        Self {
            event_tx,
            cancel_token: CancellationToken::new(),
            tasks: HashMap::new(),
        }
    }

    /// 添加轮询配置
    pub fn add_schedule(&mut self, config: PollScheduleConfig) {
        if !config.enabled {
            return;
        }

        // 如果已存在，先移除
        self.remove_schedule(&config.config_id);

        let config_id = config.config_id.clone();
        let event_tx = self.event_tx.clone();
        let cancel_token = self.cancel_token.child_token();

        let handle = if let Some(scheduled_time) = config.scheduled_time {
            // 定时模式
            tokio::spawn(async move {
                Self::run_scheduled_poll(config_id, scheduled_time, event_tx, cancel_token).await;
            })
        } else {
            // 间隔模式
            let interval = config.interval;
            tokio::spawn(async move {
                Self::run_interval_poll(config_id, interval, event_tx, cancel_token).await;
            })
        };

        self.tasks.insert(config.config_id.clone(), handle);
        tracing::info!("Added poll schedule for config: {}", config.config_id);
    }

    /// 移除轮询配置
    pub fn remove_schedule(&mut self, config_id: &str) {
        if let Some(handle) = self.tasks.remove(config_id) {
            handle.abort();
            tracing::info!("Removed poll schedule for config: {}", config_id);
        }
    }

    /// 运行间隔轮询
    async fn run_interval_poll(
        config_id: String,
        interval: Duration,
        event_tx: mpsc::UnboundedSender<ChangeEvent>,
        cancel_token: CancellationToken,
    ) {
        let mut interval_timer = tokio::time::interval(interval);
        // 跳过第一次立即触发
        interval_timer.tick().await;

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    // 判断是否为全局轮询
                    let event = if let Some((direction, poll_type)) = parse_global_poll_id(&config_id) {
                        tracing::debug!("Global interval poll triggered: {:?} {:?}", direction, poll_type);
                        ChangeEvent::GlobalPollEvent { direction, poll_type }
                    } else {
                        tracing::debug!("Poll triggered for config: {}", config_id);
                        ChangeEvent::PollEvent { config_id: config_id.clone() }
                    };

                    if let Err(e) = event_tx.send(event) {
                        tracing::warn!("Failed to send poll event: {}", e);
                        break;
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::debug!("Poll scheduler cancelled for config: {}", config_id);
                    break;
                }
            }
        }
    }

    /// 运行定时轮询
    async fn run_scheduled_poll(
        config_id: String,
        scheduled_time: ScheduledTime,
        event_tx: mpsc::UnboundedSender<ChangeEvent>,
        cancel_token: CancellationToken,
    ) {
        loop {
            // 计算下次触发时间
            let delay = Self::calculate_delay_to_scheduled_time(scheduled_time);

            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    // 判断是否为全局轮询
                    let event = if let Some((direction, poll_type)) = parse_global_poll_id(&config_id) {
                        tracing::debug!("Global scheduled poll triggered: {:?} {:?}", direction, poll_type);
                        ChangeEvent::GlobalPollEvent { direction, poll_type }
                    } else {
                        tracing::debug!("Scheduled poll triggered for config: {}", config_id);
                        ChangeEvent::PollEvent { config_id: config_id.clone() }
                    };

                    if let Err(e) = event_tx.send(event) {
                        tracing::warn!("Failed to send scheduled poll event: {}", e);
                        break;
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::debug!("Scheduled poll cancelled for config: {}", config_id);
                    break;
                }
            }
        }
    }

    /// 计算到指定时间的延迟
    fn calculate_delay_to_scheduled_time(scheduled_time: ScheduledTime) -> Duration {
        let now = chrono::Local::now();
        let today = now.date_naive();

        let target = today
            .and_hms_opt(scheduled_time.hour, scheduled_time.minute, 0)
            .unwrap();

        let target_datetime = if target <= now.naive_local() {
            // 今天的时间已过，设置为明天
            target + chrono::Duration::days(1)
        } else {
            target
        };

        let duration = target_datetime - now.naive_local();
        Duration::from_secs(duration.num_seconds().max(1) as u64)
    }

    /// 手动触发轮询
    pub fn trigger_poll(&self, config_id: &str) {
        let event = ChangeEvent::PollEvent {
            config_id: config_id.to_string(),
        };
        if let Err(e) = self.event_tx.send(event) {
            tracing::warn!("Failed to send manual poll event: {}", e);
        }
    }

    /// 获取调度数量
    pub fn schedule_count(&self) -> usize {
        self.tasks.len()
    }

    /// 获取所有调度的配置 ID
    pub fn scheduled_configs(&self) -> Vec<String> {
        self.tasks.keys().cloned().collect()
    }

    /// 停止所有调度
    pub fn stop_all(&mut self) {
        self.cancel_token.cancel();
        for (config_id, handle) in self.tasks.drain() {
            handle.abort();
            tracing::debug!("Stopped poll schedule for config: {}", config_id);
        }
        tracing::info!("All poll schedules stopped");
    }
}

impl Drop for PollScheduler {
    fn drop(&mut self) {
        self.stop_all();
    }
}

/// 目录扫描器
pub struct DirectoryScanner {
    /// 扫描的根路径
    root_path: std::path::PathBuf,
    /// 过滤服务
    filter: Option<super::super::watcher::FilterService>,
}

impl DirectoryScanner {
    /// 创建新的目录扫描器
    pub fn new(root_path: std::path::PathBuf) -> Self {
        Self {
            root_path,
            filter: None,
        }
    }

    /// 设置过滤服务
    pub fn with_filter(mut self, filter: super::super::watcher::FilterService) -> Self {
        self.filter = Some(filter);
        self
    }

    /// 扫描目录，返回文件迭代器
    pub fn scan(&self) -> impl Iterator<Item = std::path::PathBuf> + '_ {
        walkdir::WalkDir::new(&self.root_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| entry.into_path())
            .filter(move |path| {
                if let Some(ref filter) = self.filter {
                    filter.should_process(path).unwrap_or(false)
                } else {
                    true
                }
            })
    }

    /// 扫描目录并收集为 Vec
    pub fn scan_collect(&self) -> Vec<std::path::PathBuf> {
        self.scan().collect()
    }

    /// 异步扫描目录
    pub async fn scan_async(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let root = self.root_path.clone();
        let filter = self.filter.clone();

        tokio::task::spawn_blocking(move || {
            let scanner = DirectoryScanner {
                root_path: root,
                filter,
            };
            scanner.scan_collect()
        })
        .await
        .map_err(|e| anyhow::anyhow!("Scan task failed: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_parse_global_poll_id() {
        // 测试上传间隔轮询
        let result = parse_global_poll_id(GLOBAL_POLL_UPLOAD_INTERVAL);
        assert!(result.is_some());
        let (direction, poll_type) = result.unwrap();
        assert_eq!(direction, BackupDirection::Upload);
        assert_eq!(poll_type, GlobalPollType::Interval);

        // 测试上传指定时间轮询
        let result = parse_global_poll_id(GLOBAL_POLL_UPLOAD_SCHEDULED);
        assert!(result.is_some());
        let (direction, poll_type) = result.unwrap();
        assert_eq!(direction, BackupDirection::Upload);
        assert_eq!(poll_type, GlobalPollType::Scheduled);

        // 测试下载间隔轮询
        let result = parse_global_poll_id(GLOBAL_POLL_DOWNLOAD_INTERVAL);
        assert!(result.is_some());
        let (direction, poll_type) = result.unwrap();
        assert_eq!(direction, BackupDirection::Download);
        assert_eq!(poll_type, GlobalPollType::Interval);

        // 测试下载指定时间轮询
        let result = parse_global_poll_id(GLOBAL_POLL_DOWNLOAD_SCHEDULED);
        assert!(result.is_some());
        let (direction, poll_type) = result.unwrap();
        assert_eq!(direction, BackupDirection::Download);
        assert_eq!(poll_type, GlobalPollType::Scheduled);

        // 测试普通配置 ID
        let result = parse_global_poll_id("some-config-id");
        assert!(result.is_none());
    }

    #[test]
    fn test_is_global_poll_id() {
        assert!(is_global_poll_id(GLOBAL_POLL_UPLOAD_INTERVAL));
        assert!(is_global_poll_id(GLOBAL_POLL_UPLOAD_SCHEDULED));
        assert!(is_global_poll_id(GLOBAL_POLL_DOWNLOAD_INTERVAL));
        assert!(is_global_poll_id(GLOBAL_POLL_DOWNLOAD_SCHEDULED));
        assert!(!is_global_poll_id("some-config-id"));
        assert!(!is_global_poll_id(""));
    }

    #[test]
    fn test_poll_schedule_config() {
        let config = PollScheduleConfig {
            config_id: "test-config".to_string(),
            enabled: true,
            interval: Duration::from_secs(60),
            scheduled_time: None,
        };

        assert_eq!(config.config_id, "test-config");
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(60));
        assert!(config.scheduled_time.is_none());
    }

    #[test]
    fn test_poll_schedule_config_with_scheduled_time() {
        let config = PollScheduleConfig {
            config_id: "test-config".to_string(),
            enabled: true,
            interval: Duration::from_secs(0),
            scheduled_time: Some(ScheduledTime { hour: 3, minute: 30 }),
        };

        assert!(config.scheduled_time.is_some());
        let time = config.scheduled_time.unwrap();
        assert_eq!(time.hour, 3);
        assert_eq!(time.minute, 30);
    }

    #[test]
    fn test_scheduled_time() {
        let time = ScheduledTime { hour: 14, minute: 30 };
        assert_eq!(time.hour, 14);
        assert_eq!(time.minute, 30);
    }

    #[test]
    fn test_poll_scheduler_creation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scheduler = PollScheduler::new(tx);

        assert_eq!(scheduler.schedule_count(), 0);
        assert!(scheduler.scheduled_configs().is_empty());
    }

    #[test]
    fn test_poll_scheduler_add_disabled_schedule() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut scheduler = PollScheduler::new(tx);

        let config = PollScheduleConfig {
            config_id: "test-config".to_string(),
            enabled: false, // Disabled
            interval: Duration::from_secs(60),
            scheduled_time: None,
        };

        scheduler.add_schedule(config);
        // Disabled schedules should not be added
        assert_eq!(scheduler.schedule_count(), 0);
    }

    #[tokio::test]
    async fn test_poll_scheduler_add_and_remove_schedule() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut scheduler = PollScheduler::new(tx);

        let config = PollScheduleConfig {
            config_id: "test-config".to_string(),
            enabled: true,
            interval: Duration::from_secs(60),
            scheduled_time: None,
        };

        scheduler.add_schedule(config);
        assert_eq!(scheduler.schedule_count(), 1);
        assert!(scheduler.scheduled_configs().contains(&"test-config".to_string()));

        scheduler.remove_schedule("test-config");
        assert_eq!(scheduler.schedule_count(), 0);
    }

    #[tokio::test]
    async fn test_poll_scheduler_replace_schedule() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut scheduler = PollScheduler::new(tx);

        // Add first schedule
        let config1 = PollScheduleConfig {
            config_id: "test-config".to_string(),
            enabled: true,
            interval: Duration::from_secs(60),
            scheduled_time: None,
        };
        scheduler.add_schedule(config1);
        assert_eq!(scheduler.schedule_count(), 1);

        // Add same config again (should replace)
        let config2 = PollScheduleConfig {
            config_id: "test-config".to_string(),
            enabled: true,
            interval: Duration::from_secs(120),
            scheduled_time: None,
        };
        scheduler.add_schedule(config2);
        assert_eq!(scheduler.schedule_count(), 1);
    }

    #[tokio::test]
    async fn test_poll_scheduler_multiple_schedules() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut scheduler = PollScheduler::new(tx);

        for i in 0..3 {
            let config = PollScheduleConfig {
                config_id: format!("config-{}", i),
                enabled: true,
                interval: Duration::from_secs(60),
                scheduled_time: None,
            };
            scheduler.add_schedule(config);
        }

        assert_eq!(scheduler.schedule_count(), 3);
        let configs = scheduler.scheduled_configs();
        assert!(configs.contains(&"config-0".to_string()));
        assert!(configs.contains(&"config-1".to_string()));
        assert!(configs.contains(&"config-2".to_string()));
    }

    #[tokio::test]
    async fn test_poll_scheduler_stop_all() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut scheduler = PollScheduler::new(tx);

        for i in 0..3 {
            let config = PollScheduleConfig {
                config_id: format!("config-{}", i),
                enabled: true,
                interval: Duration::from_secs(60),
                scheduled_time: None,
            };
            scheduler.add_schedule(config);
        }

        assert_eq!(scheduler.schedule_count(), 3);
        scheduler.stop_all();
        assert_eq!(scheduler.schedule_count(), 0);
    }

    #[tokio::test]
    async fn test_poll_scheduler_trigger_poll() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let scheduler = PollScheduler::new(tx);

        scheduler.trigger_poll("manual-config");

        // Should receive the poll event
        let event = rx.recv().await;
        assert!(event.is_some());

        if let Some(ChangeEvent::PollEvent { config_id }) = event {
            assert_eq!(config_id, "manual-config");
        } else {
            panic!("Expected PollEvent");
        }
    }

    #[test]
    fn test_calculate_delay_to_scheduled_time() {
        // Test that delay calculation returns a positive duration
        let scheduled_time = ScheduledTime { hour: 3, minute: 0 };
        let delay = PollScheduler::calculate_delay_to_scheduled_time(scheduled_time);

        // Delay should be positive and less than 24 hours
        assert!(delay.as_secs() >= 1);
        assert!(delay.as_secs() <= 24 * 60 * 60);
    }

    #[test]
    fn test_directory_scanner_creation() {
        let dir = tempdir().unwrap();
        let scanner = DirectoryScanner::new(dir.path().to_path_buf());

        assert!(scanner.filter.is_none());
    }

    #[test]
    fn test_directory_scanner_scan_empty_dir() {
        let dir = tempdir().unwrap();
        let scanner = DirectoryScanner::new(dir.path().to_path_buf());

        let files = scanner.scan_collect();
        assert!(files.is_empty());
    }

    #[test]
    fn test_directory_scanner_scan_with_files() {
        let dir = tempdir().unwrap();

        // Create some test files
        let file1 = dir.path().join("test1.txt");
        let file2 = dir.path().join("test2.txt");
        File::create(&file1).unwrap().write_all(b"hello").unwrap();
        File::create(&file2).unwrap().write_all(b"world").unwrap();

        let scanner = DirectoryScanner::new(dir.path().to_path_buf());
        let files = scanner.scan_collect();

        assert_eq!(files.len(), 2);
        assert!(files.contains(&file1));
        assert!(files.contains(&file2));
    }

    #[test]
    fn test_directory_scanner_scan_nested_dirs() {
        let dir = tempdir().unwrap();

        // Create nested directory structure
        let subdir = dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();

        let file1 = dir.path().join("root.txt");
        let file2 = subdir.join("nested.txt");
        File::create(&file1).unwrap().write_all(b"root").unwrap();
        File::create(&file2).unwrap().write_all(b"nested").unwrap();

        let scanner = DirectoryScanner::new(dir.path().to_path_buf());
        let files = scanner.scan_collect();

        assert_eq!(files.len(), 2);
        assert!(files.contains(&file1));
        assert!(files.contains(&file2));
    }

    #[test]
    fn test_directory_scanner_with_filter() {
        use crate::autobackup::watcher::FilterService;

        let dir = tempdir().unwrap();

        // Create test files with different extensions
        let txt_file = dir.path().join("test.txt");
        let md_file = dir.path().join("test.md");
        File::create(&txt_file).unwrap().write_all(b"hello").unwrap();
        File::create(&md_file).unwrap().write_all(b"world").unwrap();

        let filter = FilterService::new(
            vec!["txt".to_string()],
            vec![],
            vec![],
            0,
            0,
        );

        let scanner = DirectoryScanner::new(dir.path().to_path_buf()).with_filter(filter);
        let files = scanner.scan_collect();

        assert_eq!(files.len(), 1);
        assert!(files.contains(&txt_file));
        assert!(!files.contains(&md_file));
    }

    #[tokio::test]
    async fn test_directory_scanner_scan_async() {
        let dir = tempdir().unwrap();

        // Create test files
        let file1 = dir.path().join("async1.txt");
        let file2 = dir.path().join("async2.txt");
        File::create(&file1).unwrap().write_all(b"hello").unwrap();
        File::create(&file2).unwrap().write_all(b"world").unwrap();

        let scanner = DirectoryScanner::new(dir.path().to_path_buf());
        let result = scanner.scan_async().await;

        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 2);
    }
}

