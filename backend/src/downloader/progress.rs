use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// 速度计算器（使用滑动窗口）
#[derive(Debug)]
pub struct SpeedCalculator {
    /// 数据点（时间，字节数）
    samples: VecDeque<(Instant, u64)>,
    /// 窗口大小（秒）
    window_size: Duration,
    /// 累计下载字节数
    total_bytes: u64,
}

impl SpeedCalculator {
    /// 创建新的速度计算器
    pub fn new(window_seconds: u64) -> Self {
        Self {
            samples: VecDeque::new(),
            window_size: Duration::from_secs(window_seconds),
            total_bytes: 0,
        }
    }

    /// 使用默认窗口大小（5秒）
    pub fn with_default_window() -> Self {
        Self::new(5)
    }

    /// 添加数据点
    pub fn add_sample(&mut self, bytes: u64) {
        let now = Instant::now();
        self.total_bytes += bytes;
        self.samples.push_back((now, bytes));
        self.cleanup_old_samples(now);
    }

    /// 清理超出窗口的旧数据
    fn cleanup_old_samples(&mut self, now: Instant) {
        while let Some((timestamp, _)) = self.samples.front() {
            if now.duration_since(*timestamp) > self.window_size {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// 计算当前速度（字节/秒）
    pub fn speed(&self) -> u64 {
        if self.samples.len() < 2 {
            return 0;
        }

        let now = Instant::now();
        let total_bytes: u64 = self.samples.iter().map(|(_, bytes)| bytes).sum();

        if let Some((first_time, _)) = self.samples.front() {
            let duration = now.duration_since(*first_time).as_secs_f64();
            if duration > 0.0 {
                return (total_bytes as f64 / duration) as u64;
            }
        }

        0
    }

    /// 获取累计下载字节数
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// 格式化速度（返回人类可读的字符串）
    pub fn format_speed(&self) -> String {
        let speed = self.speed();
        format_bytes_per_second(speed)
    }

    /// 重置计算器
    pub fn reset(&mut self) {
        self.samples.clear();
        self.total_bytes = 0;
    }
}

/// 格式化字节/秒
pub fn format_bytes_per_second(bytes_per_sec: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes_per_sec >= GB {
        format!("{:.2} GB/s", bytes_per_sec as f64 / GB as f64)
    } else if bytes_per_sec >= MB {
        format!("{:.2} MB/s", bytes_per_sec as f64 / MB as f64)
    } else if bytes_per_sec >= KB {
        format!("{:.2} KB/s", bytes_per_sec as f64 / KB as f64)
    } else {
        format!("{} B/s", bytes_per_sec)
    }
}

/// 格式化剩余时间
pub fn format_eta(seconds: u64) -> String {
    if seconds == 0 {
        return "即将完成".to_string();
    }

    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if hours > 0 {
        format!("{}小时{}分钟", hours, minutes)
    } else if minutes > 0 {
        format!("{}分钟{}秒", minutes, secs)
    } else {
        format!("{}秒", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_speed_calculator_creation() {
        let calc = SpeedCalculator::new(5);
        assert_eq!(calc.total_bytes(), 0);
        assert_eq!(calc.speed(), 0);
    }

    #[test]
    fn test_add_sample() {
        let mut calc = SpeedCalculator::new(5);

        calc.add_sample(1024);
        assert_eq!(calc.total_bytes(), 1024);

        calc.add_sample(2048);
        assert_eq!(calc.total_bytes(), 3072);
    }

    #[test]
    fn test_speed_calculation() {
        let mut calc = SpeedCalculator::new(5);

        // 第一个样本
        calc.add_sample(1024 * 1024); // 1MB

        // 等待100ms
        thread::sleep(Duration::from_millis(100));

        // 第二个样本
        calc.add_sample(1024 * 1024); // 1MB

        let speed = calc.speed();
        // 2MB in ~0.1s = ~20MB/s（允许一定误差）
        assert!(speed > 10 * 1024 * 1024); // 至少10MB/s
    }

    #[test]
    fn test_reset() {
        let mut calc = SpeedCalculator::new(5);

        calc.add_sample(1024);
        calc.add_sample(2048);
        assert_eq!(calc.total_bytes(), 3072);

        calc.reset();
        assert_eq!(calc.total_bytes(), 0);
        assert_eq!(calc.speed(), 0);
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_bytes_per_second(500), "500 B/s");
        assert_eq!(format_bytes_per_second(1024), "1.00 KB/s");
        assert_eq!(format_bytes_per_second(1024 * 1024), "1.00 MB/s");
        assert_eq!(format_bytes_per_second(1024 * 1024 * 1024), "1.00 GB/s");
    }

    #[test]
    fn test_format_eta() {
        assert_eq!(format_eta(0), "即将完成");
        assert_eq!(format_eta(30), "30秒");
        assert_eq!(format_eta(90), "1分钟30秒");
        assert_eq!(format_eta(3661), "1小时1分钟");
    }
}
