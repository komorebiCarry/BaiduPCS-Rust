//! 内存监控模块
//!
//! 提供进程内存使用监控、异常检测和峰值记录功能

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use sysinfo::{Pid, System};

/// 内存采样数据
#[derive(Debug, Clone)]
pub struct MemorySample {
    /// 采样时间戳
    pub timestamp: Instant,
    /// 内存使用量（字节）
    pub usage_bytes: u64,
}

/// 内存异常信息
#[derive(Debug, Clone)]
pub struct MemoryAnomaly {
    /// 检测时间
    pub detected_at: Instant,
    /// 增长量（字节）
    pub growth_bytes: u64,
    /// 增长时间段
    pub duration: Duration,
    /// 当前内存使用量
    pub current_usage: u64,
}

/// 内存监控配置
#[derive(Debug, Clone)]
pub struct MemoryMonitorConfig {
    /// 采样间隔（默认 30 秒）
    pub sample_interval: Duration,
    /// 异常增长阈值（默认 500MB）
    pub growth_threshold: u64,
    /// 异常检测时间窗口（默认 1 分钟）
    pub anomaly_window: Duration,
    /// 最大采样数量（默认保留 120 个，即 1 小时的数据）
    pub max_samples: usize,
    /// 绝对内存阈值（可选，超过时记录 error 日志）
    pub absolute_threshold: Option<u64>,
}

impl Default for MemoryMonitorConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_secs(30),
            growth_threshold: 500 * 1024 * 1024, // 500MB
            anomaly_window: Duration::from_secs(60),
            max_samples: 120,
            absolute_threshold: Some(2 * 1024 * 1024 * 1024), // 默认 2GB
        }
    }
}

/// 内存监控器
///
/// 定期采样进程内存使用，检测异常增长，记录峰值
pub struct MemoryMonitor {
    /// 采样间隔
    sample_interval: Duration,
    /// 异常增长阈值（字节）
    growth_threshold: u64,
    /// 异常检测时间窗口
    anomaly_window: Duration,
    /// 最大采样数量
    max_samples: usize,
    /// 绝对内存阈值（字节）
    absolute_threshold: Option<u64>,
    /// 历史采样数据
    samples: RwLock<VecDeque<MemorySample>>,
    /// 峰值记录（字节）
    peak_usage: AtomicU64,
    /// 系统信息实例
    system: RwLock<System>,
    /// 当前进程 ID
    pid: Pid,
    /// 是否正在运行
    running: RwLock<bool>,
}

impl MemoryMonitor {
    /// 创建新的内存监控器
    pub fn new(config: MemoryMonitorConfig) -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        system.refresh_process(pid);

        Self {
            sample_interval: config.sample_interval,
            growth_threshold: config.growth_threshold,
            anomaly_window: config.anomaly_window,
            max_samples: config.max_samples,
            absolute_threshold: config.absolute_threshold,
            samples: RwLock::new(VecDeque::with_capacity(config.max_samples)),
            peak_usage: AtomicU64::new(0),
            system: RwLock::new(system),
            pid,
            running: RwLock::new(false),
        }
    }

    /// 使用默认配置创建监控器
    pub fn with_defaults() -> Self {
        Self::new(MemoryMonitorConfig::default())
    }

    /// 获取采样间隔
    pub fn sample_interval(&self) -> Duration {
        self.sample_interval
    }

    /// 获取异常增长阈值
    pub fn growth_threshold(&self) -> u64 {
        self.growth_threshold
    }

    /// 获取绝对内存阈值
    pub fn absolute_threshold(&self) -> Option<u64> {
        self.absolute_threshold
    }

    /// 检测是否超过绝对阈值
    ///
    /// 返回 Some(当前内存使用量) 如果超过阈值，否则返回 None
    pub fn check_absolute_threshold(&self) -> Option<u64> {
        let current = self.current_usage();
        if let Some(threshold) = self.absolute_threshold {
            if current > threshold {
                return Some(current);
            }
        }
        None
    }

    /// 获取当前内存使用量（字节）
    pub fn current_usage(&self) -> u64 {
        let mut system = self.system.write();
        system.refresh_process(self.pid);

        system
            .process(self.pid)
            .map(|p| p.memory())
            .unwrap_or(0)
    }

    /// 获取峰值内存使用量（字节）
    pub fn peak_usage(&self) -> u64 {
        self.peak_usage.load(Ordering::Relaxed)
    }

    /// 获取采样历史
    pub fn get_samples(&self) -> Vec<MemorySample> {
        self.samples.read().iter().cloned().collect()
    }

    /// 获取采样数量
    pub fn sample_count(&self) -> usize {
        self.samples.read().len()
    }

    /// 执行一次内存采样
    pub fn sample(&self) -> MemorySample {
        let usage = self.current_usage();
        let sample = MemorySample {
            timestamp: Instant::now(),
            usage_bytes: usage,
        };

        // 更新峰值
        self.update_peak(usage);

        // 添加到采样历史
        let mut samples = self.samples.write();
        samples.push_back(sample.clone());

        // 限制采样数量
        while samples.len() > self.max_samples {
            samples.pop_front();
        }

        sample
    }

    /// 更新峰值记录
    fn update_peak(&self, current: u64) {
        let mut peak = self.peak_usage.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_usage.compare_exchange_weak(
                peak,
                current,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    tracing::info!(
                        "内存峰值更新: {} MB -> {} MB",
                        peak / (1024 * 1024),
                        current / (1024 * 1024)
                    );
                    break;
                }
                Err(p) => peak = p,
            }
        }
    }

    /// 检测异常增长
    ///
    /// 检查在 anomaly_window 时间窗口内内存增长是否超过 growth_threshold
    pub fn check_anomaly(&self) -> Option<MemoryAnomaly> {
        let samples = self.samples.read();
        if samples.len() < 2 {
            return None;
        }

        let now = Instant::now();
        let window_start = now - self.anomaly_window;

        // 找到时间窗口内的最早采样
        let earliest_in_window = samples
            .iter()
            .find(|s| s.timestamp >= window_start)?;

        // 获取最新采样
        let latest = samples.back()?;

        // 计算增长量
        let growth = latest.usage_bytes.saturating_sub(earliest_in_window.usage_bytes);
        let duration = latest.timestamp.duration_since(earliest_in_window.timestamp);

        if growth > self.growth_threshold {
            Some(MemoryAnomaly {
                detected_at: now,
                growth_bytes: growth,
                duration,
                current_usage: latest.usage_bytes,
            })
        } else {
            None
        }
    }

    /// 启动监控（后台任务）
    pub fn start(self: Arc<Self>) {
        {
            let mut running = self.running.write();
            if *running {
                tracing::warn!("内存监控器已在运行");
                return;
            }
            *running = true;
        }

        let monitor = self.clone();
        tokio::spawn(async move {
            tracing::info!(
                "内存监控器启动，采样间隔: {:?}，异常阈值: {} MB，绝对阈值: {}",
                monitor.sample_interval,
                monitor.growth_threshold / (1024 * 1024),
                monitor.absolute_threshold
                    .map(|t| format!("{} MB", t / (1024 * 1024)))
                    .unwrap_or_else(|| "未设置".to_string())
            );

            loop {
                // 检查是否应该停止
                if !*monitor.running.read() {
                    tracing::info!("内存监控器停止");
                    break;
                }

                // 执行采样
                let sample = monitor.sample();
                tracing::debug!(
                    "内存采样: {} MB，峰值: {} MB",
                    sample.usage_bytes / (1024 * 1024),
                    monitor.peak_usage() / (1024 * 1024)
                );

                // 检测异常增长
                if let Some(anomaly) = monitor.check_anomaly() {
                    tracing::warn!(
                        "检测到内存异常增长: 在 {:?} 内增长了 {} MB，当前使用: {} MB",
                        anomaly.duration,
                        anomaly.growth_bytes / (1024 * 1024),
                        anomaly.current_usage / (1024 * 1024)
                    );
                }

                // 检测绝对阈值
                if let Some(current_usage) = monitor.check_absolute_threshold() {
                    tracing::error!(
                        "内存使用超过绝对阈值: 当前 {} MB，阈值 {} MB",
                        current_usage / (1024 * 1024),
                        monitor.absolute_threshold.unwrap() / (1024 * 1024)
                    );
                }

                // 等待下一次采样
                tokio::time::sleep(monitor.sample_interval).await;
            }
        });
    }

    /// 停止监控
    pub fn stop(&self) {
        let mut running = self.running.write();
        *running = false;
        tracing::info!("内存监控器停止请求已发送");
    }

    /// 检查监控器是否正在运行
    pub fn is_running(&self) -> bool {
        *self.running.read()
    }

    /// 重置监控器状态
    pub fn reset(&self) {
        let mut samples = self.samples.write();
        samples.clear();
        self.peak_usage.store(0, Ordering::Relaxed);
        tracing::info!("内存监控器状态已重置");
    }

    /// 格式化内存大小为人类可读格式
    pub fn format_bytes(bytes: u64) -> String {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_monitor_creation() {
        let monitor = MemoryMonitor::with_defaults();
        assert_eq!(monitor.sample_interval(), Duration::from_secs(30));
        assert_eq!(monitor.growth_threshold(), 500 * 1024 * 1024);
        assert_eq!(monitor.peak_usage(), 0);
        assert_eq!(monitor.sample_count(), 0);
    }

    #[test]
    fn test_memory_monitor_custom_config() {
        let config = MemoryMonitorConfig {
            sample_interval: Duration::from_secs(10),
            growth_threshold: 100 * 1024 * 1024,
            anomaly_window: Duration::from_secs(30),
            max_samples: 50,
            absolute_threshold: Some(1024 * 1024 * 1024), // 1GB
        };
        let monitor = MemoryMonitor::new(config);
        assert_eq!(monitor.sample_interval(), Duration::from_secs(10));
        assert_eq!(monitor.growth_threshold(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_memory_sampling() {
        let monitor = MemoryMonitor::with_defaults();
        
        // 执行采样
        let sample = monitor.sample();
        
        // 验证采样结果
        assert!(sample.usage_bytes > 0);
        assert_eq!(monitor.sample_count(), 1);
        assert!(monitor.peak_usage() > 0);
    }

    #[test]
    fn test_peak_tracking() {
        let monitor = MemoryMonitor::with_defaults();
        
        // 执行多次采样
        for _ in 0..5 {
            monitor.sample();
        }
        
        // 峰值应该被记录
        assert!(monitor.peak_usage() > 0);
    }

    #[test]
    fn test_sample_limit() {
        let config = MemoryMonitorConfig {
            max_samples: 3,
            ..Default::default()
        };
        let monitor = MemoryMonitor::new(config);
        
        // 执行超过限制的采样
        for _ in 0..5 {
            monitor.sample();
        }
        
        // 采样数量应该被限制
        assert_eq!(monitor.sample_count(), 3);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(MemoryMonitor::format_bytes(500), "500 B");
        assert_eq!(MemoryMonitor::format_bytes(1024), "1.00 KB");
        assert_eq!(MemoryMonitor::format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(MemoryMonitor::format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_absolute_threshold_config() {
        // 测试默认配置包含绝对阈值
        let config = MemoryMonitorConfig::default();
        assert_eq!(config.absolute_threshold, Some(2 * 1024 * 1024 * 1024)); // 2GB

        // 测试自定义绝对阈值
        let custom_config = MemoryMonitorConfig {
            absolute_threshold: Some(1024 * 1024 * 1024), // 1GB
            ..Default::default()
        };
        let monitor = MemoryMonitor::new(custom_config);
        assert_eq!(monitor.absolute_threshold(), Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_absolute_threshold_none() {
        // 测试禁用绝对阈值
        let config = MemoryMonitorConfig {
            absolute_threshold: None,
            ..Default::default()
        };
        let monitor = MemoryMonitor::new(config);
        assert_eq!(monitor.absolute_threshold(), None);
        // 当阈值为 None 时，check_absolute_threshold 应返回 None
        assert!(monitor.check_absolute_threshold().is_none());
    }

    #[test]
    fn test_check_absolute_threshold() {
        // 设置一个非常低的阈值来触发告警
        let config = MemoryMonitorConfig {
            absolute_threshold: Some(1), // 1 字节，肯定会超过
            ..Default::default()
        };
        let monitor = MemoryMonitor::new(config);
        
        // 当前内存使用肯定超过 1 字节
        let result = monitor.check_absolute_threshold();
        assert!(result.is_some());
        assert!(result.unwrap() > 1);
    }
}
