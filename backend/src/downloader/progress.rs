use std::collections::VecDeque;
use std::time::{Duration, Instant};

const DEFAULT_WINDOW_SECONDS: u64 = 10;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
const MIN_MEASUREMENT_DURATION: Duration = Duration::from_secs(1);

/// 速度计算器（使用累计字节快照滑动窗口）
#[derive(Debug)]
pub struct SpeedCalculator {
    /// 数据点（单调时间，累计字节数）
    samples: VecDeque<(Instant, u64)>,
    /// 统计窗口大小
    window_size: Duration,
    /// 累计下载字节数
    total_bytes: u64,
}

impl SpeedCalculator {
    /// 创建新的速度计算器
    pub fn new(window_seconds: u64) -> Self {
        Self {
            samples: VecDeque::new(),
            window_size: Duration::from_secs(window_seconds.max(1)),
            total_bytes: 0,
        }
    }

    /// 使用默认窗口大小（10秒）
    pub fn with_default_window() -> Self {
        Self::new(DEFAULT_WINDOW_SECONDS)
    }

    /// 累加收到的字节，并按最多每 500ms 一个快照进行采样。
    pub fn add_sample(&mut self, bytes: u64) {
        self.add_sample_at(bytes, Instant::now());
    }

    /// 记录一个定时快照并返回当前速度。
    ///
    /// 即使没有新字节，也要定期调用此方法，让窗口中的空闲时间参与平均速度计算。
    pub fn refresh(&mut self) -> u64 {
        self.refresh_at(Instant::now())
    }

    fn add_sample_at(&mut self, bytes: u64, now: Instant) {
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.record_snapshot(now);
    }

    fn refresh_at(&mut self, now: Instant) -> u64 {
        self.record_snapshot(now);
        self.speed()
    }

    fn record_snapshot(&mut self, now: Instant) {
        let should_record = self
            .samples
            .back()
            .map(|(timestamp, _)| now.duration_since(*timestamp) >= SAMPLE_INTERVAL)
            .unwrap_or(true);

        if should_record {
            self.samples.push_back((now, self.total_bytes));
        }
        self.cleanup_old_samples(now);
    }

    /// 清理超出窗口的旧快照，同时保留一个窗口边界前的基准点。
    fn cleanup_old_samples(&mut self, now: Instant) {
        while self.samples.len() > 1
            && now.duration_since(self.samples[1].0) > self.window_size
        {
            self.samples.pop_front();
        }
    }

    /// 计算当前速度（字节/秒）
    pub fn speed(&self) -> u64 {
        let (Some((first_time, first_bytes)), Some((last_time, last_bytes))) =
            (self.samples.front(), self.samples.back())
        else {
            return 0;
        };

        let duration = last_time.duration_since(*first_time);
        if duration < MIN_MEASUREMENT_DURATION {
            return 0;
        }

        let bytes = last_bytes.saturating_sub(*first_bytes);
        if bytes == 0 {
            return 0;
        }

        (bytes as f64 / duration.as_secs_f64()) as u64
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
        let start = Instant::now();

        calc.refresh_at(start);
        calc.add_sample_at(1024 * 1024, start + Duration::from_millis(500));
        calc.add_sample_at(1024 * 1024, start + Duration::from_secs(1));

        assert_eq!(calc.speed(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_speed_requires_warmup() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        calc.refresh_at(start);
        calc.add_sample_at(1024 * 1024, start + Duration::from_millis(500));

        assert_eq!(calc.speed(), 0);
    }

    #[test]
    fn test_idle_time_reduces_speed_to_zero() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        calc.refresh_at(start);
        for second in 1..=10 {
            calc.add_sample_at(1024 * 1024, start + Duration::from_secs(second));
        }
        assert_eq!(calc.speed(), 1024 * 1024);

        for second in 11..=21 {
            calc.refresh_at(start + Duration::from_secs(second));
        }
        assert_eq!(calc.speed(), 0);
    }

    #[test]
    fn test_snapshot_count_is_bounded() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        for step in 0..=120 {
            calc.add_sample_at(1024, start + Duration::from_millis(step * 500));
        }

        assert!(calc.samples.len() <= 22);
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
