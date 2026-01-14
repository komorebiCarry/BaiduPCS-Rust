//! 日志系统配置
//!
//! 支持控制台输出和文件持久化，按文件大小和启动时间滚动，自动清理过期日志

use crate::config::LogConfig;
use chrono::{Local, NaiveDate};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt::{self, time::ChronoLocal},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// 日志文件管理器（内部状态）
///
/// 负责管理日志文件的创建、滚动和写入
struct LogFileManagerInner {
    /// 当前日志文件的日期，用于检测跨天
    current_date: NaiveDate,
    /// 当前日志文件的时间戳（格式：YYYY-MM-DD-HHMMSS）
    current_timestamp: String,
    /// 日志目录路径
    log_dir: PathBuf,
    /// 当前文件句柄
    current_file: Option<File>,
    /// 当前文件序号（0 表示基础文件，1、2、3... 表示滚动文件）
    current_index: u32,
    /// 单个文件最大大小（字节）
    max_file_size: u64,
    /// 当前文件已写入的字节数
    current_size: u64,
}

impl LogFileManagerInner {
    /// 创建新的日志文件管理器
    fn new(log_dir: PathBuf, max_file_size: u64) -> io::Result<Self> {
        let now = Local::now();
        let current_date = now.date_naive();
        let current_timestamp = now.format("%Y-%m-%d-%H%M%S").to_string();

        let mut manager = Self {
            current_date,
            current_timestamp,
            log_dir,
            current_file: None,
            current_index: 0,
            max_file_size,
            current_size: 0,
        };

        // 创建初始日志文件
        manager.create_new_file()?;

        Ok(manager)
    }

    /// 生成日志文件路径
    fn generate_file_path(&self, index: u32) -> PathBuf {
        let filename = if index == 0 {
            format!("baidu-pcs-rust.{}.log", self.current_timestamp)
        } else {
            format!("baidu-pcs-rust.{}_{}.log", self.current_timestamp, index)
        };
        self.log_dir.join(filename)
    }

    /// 创建新的日志文件
    fn create_new_file(&mut self) -> io::Result<()> {
        let file_path = self.generate_file_path(self.current_index);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        self.current_file = Some(file);
        self.current_size = 0;

        Ok(())
    }

    /// 检查是否需要按日期滚动（跨天）
    fn should_rotate_by_date(&self) -> bool {
        Local::now().date_naive() != self.current_date
    }

    /// 检查是否需要按大小滚动
    fn should_rotate_by_size(&self, incoming_size: usize) -> bool {
        self.current_size + incoming_size as u64 > self.max_file_size
    }

    /// 按日期滚动到新文件（跨天时调用）
    fn rotate_by_date(&mut self) -> io::Result<()> {
        // 关闭当前文件
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
        }

        // 更新日期和时间戳
        let now = Local::now();
        self.current_date = now.date_naive();
        self.current_timestamp = now.format("%Y-%m-%d-%H%M%S").to_string();

        // 重置文件序号
        self.current_index = 0;

        // 创建新文件
        self.create_new_file()?;

        Ok(())
    }

    /// 按大小滚动到新文件
    fn rotate_by_size(&mut self) -> io::Result<()> {
        // 关闭当前文件
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
        }

        // 增加文件序号
        self.current_index += 1;

        // 创建新文件
        self.create_new_file()?;

        Ok(())
    }

    /// 写入数据
    fn write_data(&mut self, buf: &[u8]) -> io::Result<usize> {
        // 优先检查日期滚动（跨天）
        if self.should_rotate_by_date() {
            self.rotate_by_date()?;
        }
        // 再检查大小滚动
        else if self.should_rotate_by_size(buf.len()) {
            self.rotate_by_size()?;
        }

        // 写入数据
        if let Some(file) = &mut self.current_file {
            let written = file.write(buf)?;
            self.current_size += written as u64;
            Ok(written)
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "日志文件未打开"))
        }
    }

    /// 刷新文件缓冲区
    fn flush_file(&mut self) -> io::Result<()> {
        if let Some(file) = &mut self.current_file {
            file.flush()?;
        }
        Ok(())
    }
}

/// 日志文件管理器（线程安全包装）
///
/// 实现了 Write trait，可以作为日志输出目标
pub struct LogFileManager {
    inner: Arc<Mutex<LogFileManagerInner>>,
}

impl LogFileManager {
    /// 创建新的日志文件管理器
    pub fn new(log_dir: PathBuf, max_file_size: u64) -> io::Result<Self> {
        let inner = LogFileManagerInner::new(log_dir, max_file_size)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

impl Write for LogFileManager {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        inner.write_data(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush_file()
    }
}

impl Clone for LogFileManager {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// 日志系统守卫
/// 必须保持存活，否则日志写入线程会终止
pub struct LogGuard {
    _file_guard: Option<WorkerGuard>,
}

/// 初始化日志系统
///
/// # Arguments
/// * `config` - 日志配置
///
/// # Returns
/// * `LogGuard` - 日志守卫，需要保持存活直到程序结束
pub fn init_logging(config: &LogConfig) -> LogGuard {
    // 创建环境过滤器
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level));

    // 控制台输出层
    let console_layer = fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f".to_string()))
        .with_ansi(true);

    if config.enabled {
        // 确保日志目录存在
        if let Err(e) = fs::create_dir_all(&config.log_dir) {
            eprintln!("创建日志目录失败: {:?}, 错误: {}", config.log_dir, e);
            // 回退到只使用控制台输出
            tracing_subscriber::registry()
                .with(env_filter)
                .with(console_layer)
                .init();

            return LogGuard { _file_guard: None };
        }

        // 创建自定义日志文件管理器
        // 文件名格式: baidu-pcs-rust.YYYY-MM-DD-HHMMSS.log
        let file_manager = match LogFileManager::new(
            config.log_dir.clone(),
            config.max_file_size,
        ) {
            Ok(manager) => manager,
            Err(e) => {
                eprintln!("创建日志文件管理器失败: {}, 回退到仅控制台输出", e);
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(console_layer)
                    .init();
                return LogGuard { _file_guard: None };
            }
        };

        // 创建非阻塞写入器
        let (non_blocking, file_guard) = tracing_appender::non_blocking(file_manager);

        // 文件输出层（不带 ANSI 颜色）
        let file_layer = fmt::layer()
            .with_target(true)
            .with_level(true)
            .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f".to_string()))
            .with_ansi(false)
            .with_writer(non_blocking);

        // 初始化订阅器
        tracing_subscriber::registry()
            .with(env_filter)
            .with(console_layer)
            .with(file_layer)
            .init();

        info!(
            "日志系统初始化完成: 目录={:?}, 保留天数={}, 级别={}, 单文件最大={:.1}MB",
            config.log_dir, config.retention_days, config.level, config.max_file_size as f64 / 1024.0 / 1024.0
        );

        // 启动过期日志清理
        cleanup_old_logs(&config.log_dir, config.retention_days);

        LogGuard {
            _file_guard: Some(file_guard),
        }
    } else {
        // 只使用控制台输出
        tracing_subscriber::registry()
            .with(env_filter)
            .with(console_layer)
            .init();

        info!("日志系统初始化完成（仅控制台输出）");

        LogGuard { _file_guard: None }
    }
}

/// 清理过期日志文件
///
/// 支持两种文件格式：
/// - 旧格式：baidu-pcs-rust.YYYY-MM-DD.log
/// - 新格式：baidu-pcs-rust.YYYY-MM-DD-HHMMSS.log 和 baidu-pcs-rust.YYYY-MM-DD-HHMMSS_N.log
fn cleanup_old_logs(log_dir: &Path, retention_days: u32) {
    let now = Local::now().date_naive();
    let retention_duration = chrono::Duration::days(retention_days as i64);

    let entries = match fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!("读取日志目录失败: {:?}, 错误: {}", log_dir, e);
            return;
        }
    };

    let mut deleted_count = 0;

    for entry in entries.flatten() {
        let path = entry.path();

        // 只处理日志文件
        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => continue,
        };

        // 检查是否为日志文件
        if !filename.starts_with("baidu-pcs-rust.") || !filename.ends_with(".log") {
            continue;
        }

        // 提取日期部分并判断是否过期
        let should_delete = if let Some(date_str) = extract_date_from_filename(filename) {
            // 解析日期字符串 (YYYY-MM-DD)
            if let Ok(file_date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let age = now.signed_duration_since(file_date);
                age > retention_duration
            } else {
                // 日期解析失败，使用文件修改时间作为后备方案
                check_by_modified_time(&entry, retention_days)
            }
        } else {
            // 无法提取日期，使用文件修改时间作为后备方案
            check_by_modified_time(&entry, retention_days)
        };

        if should_delete {
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!("删除过期日志文件失败: {:?}, 错误: {}", path, e);
            } else {
                deleted_count += 1;
                tracing::debug!("已删除过期日志文件: {:?}", path);
            }
        }
    }

    if deleted_count > 0 {
        info!("已清理 {} 个过期日志文件", deleted_count);
    }
}

/// 从文件名中提取日期部分
///
/// 支持的格式：
/// - baidu-pcs-rust.YYYY-MM-DD.log -> YYYY-MM-DD
/// - baidu-pcs-rust.YYYY-MM-DD-HHMMSS.log -> YYYY-MM-DD
/// - baidu-pcs-rust.YYYY-MM-DD-HHMMSS_N.log -> YYYY-MM-DD
fn extract_date_from_filename(filename: &str) -> Option<String> {
    // 移除前缀和后缀
    let name = filename.strip_prefix("baidu-pcs-rust.")?;
    let name = name.strip_suffix(".log")?;

    // 提取日期部分 (YYYY-MM-DD)
    // 格式可能是：
    // - YYYY-MM-DD
    // - YYYY-MM-DD-HHMMSS
    // - YYYY-MM-DD-HHMMSS_N
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() >= 3 {
        // 前三部分是年-月-日
        Some(format!("{}-{}-{}", parts[0], parts[1], parts[2]))
    } else {
        None
    }
}

/// 根据文件修改时间检查是否过期（后备方案）
fn check_by_modified_time(entry: &fs::DirEntry, retention_days: u32) -> bool {
    let now = chrono::Utc::now();
    let retention_duration = chrono::Duration::days(retention_days as i64);

    if let Ok(metadata) = entry.metadata() {
        if let Ok(modified) = metadata.modified() {
            let modified_datetime: chrono::DateTime<chrono::Utc> = modified.into();
            let age = now.signed_duration_since(modified_datetime);
            return age > retention_duration;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_default_log_config() {
        let config = LogConfig::default();
        assert!(config.enabled);
        assert_eq!(config.log_dir, PathBuf::from("logs"));
        assert_eq!(config.retention_days, 7);
        assert_eq!(config.level, "info");
    }
}
