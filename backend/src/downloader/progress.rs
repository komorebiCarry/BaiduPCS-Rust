use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// 默认统计窗口长度。
const DEFAULT_WINDOW_SECONDS: u64 = 10;

/// 两个快照之间的最小间隔。
///
/// 下载回调的触发频率由 `chunk.rs` 的「256KB 或 500ms」双阈值决定，高速下载时
/// 一秒可能回调几十次。按固定间隔落点可以让窗口内的快照数量有上界，
/// 也让速度不随回调疏密漂移。
const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

/// 速度测量所需的最小时间跨度。
///
/// 跨度不足一秒时宁可报 0，也不拿几十毫秒的样本外推 —— 那正是分母塌缩的来源。
const MIN_MEASUREMENT_DURATION: Duration = Duration::from_secs(1);

/// 速度计算器（累计字节快照 + 滑动窗口）
#[derive(Debug)]
pub struct SpeedCalculator {
    /// 数据点（单调时刻，该时刻的**累计**字节数）
    ///
    /// 存累计值而非增量：速度直接由「两个快照的字节差 ÷ 两个快照的时间差」得出，
    /// 分子分母取自同一对时间点，天然自洽。
    samples: VecDeque<(Instant, u64)>,
    /// 统计窗口长度
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

    /// 使用默认窗口大小（10 秒）
    pub fn with_default_window() -> Self {
        Self::new(DEFAULT_WINDOW_SECONDS)
    }

    /// 累加收到的字节，并按 [`SAMPLE_INTERVAL`] 落快照
    pub fn add_sample(&mut self, bytes: u64) {
        self.add_sample_at(bytes, Instant::now());
    }

    /// 记录一个定时快照并返回当前速度。
    ///
    /// 即使没有新字节也要定期调用，让窗口里的空闲时间参与平均 —— 否则下载停滞时
    /// 最老的快照永远滚不出窗口，速度会一直停在最后那个值不动
    /// （实测见过卡在 5.33 MB/s 好几秒）。调用方见 `ChunkScheduler` 的刷新循环。
    pub fn refresh(&mut self) -> u64 {
        self.refresh_at(Instant::now())
    }

    fn add_sample_at(&mut self, bytes: u64, now: Instant) {
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.record_snapshot(now);
    }

    /// 直接用外部维护的**累计**字节数推进快照，并返回当前速度。
    ///
    /// 与 [`add_sample`](Self::add_sample) 的分工：`add_sample` 接收增量，供单个任务
    /// 自己累加；本方法接收绝对累计值，供**跨任务**的场景把分散的字节流接成一条连续曲线。
    ///
    /// 为什么需要它：`MIN_MEASUREMENT_DURATION` 要求至少 1 秒的测量跨度，而海量小文件
    /// 场景下每个子任务只有几十毫秒就结束 —— 每个子任务各自的 `SpeedCalculator` 永远
    /// 达不到预热门槛，`speed()` 恒为 0，界面上表现为"下载在跑但速度一直是 0 B/s"。
    /// 文件夹的累计已下载字节（`FolderDownload::downloaded_size`）是单调连续的量，
    /// 对它做差分才反映真实吞吐。
    ///
    /// 累计值回退时（进度口径修正）直接接受新值：下一轮 `speed_at` 因
    /// `saturating_sub` 得到 0，随后自然恢复，不会算出负速度或爆表速度。
    pub fn observe_total(&mut self, total_bytes: u64) -> u64 {
        self.observe_total_at(total_bytes, Instant::now())
    }

    fn observe_total_at(&mut self, total_bytes: u64, now: Instant) -> u64 {
        self.total_bytes = total_bytes;
        self.record_snapshot(now);
        self.speed_at(now)
    }

    fn refresh_at(&mut self, now: Instant) -> u64 {
        self.record_snapshot(now);
        self.speed_at(now)
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

    /// 清理超出窗口的旧快照，但**保留一个窗口边界之外的基准点**。
    ///
    /// 判据用 `samples[1]` 而不是 `samples[0]`：只要第二个快照还在窗口内，
    /// 第一个就得留着当基准 —— 速度是「差值 ÷ 时间差」，没有左端点就没法算。
    fn cleanup_old_samples(&mut self, now: Instant) {
        while self.samples.len() > 1 && now.duration_since(self.samples[1].0) > self.window_size {
            self.samples.pop_front();
        }
    }

    /// 计算当前速度（字节/秒）
    ///
    /// 最初的实现是 `窗口内字节数 / (now - 最早样本时间)`，分子分母口径不一致，
    /// 样本稀疏时分母会塌缩：下载按分片落盘，`progress_callback` 并非匀速调用 ——
    /// 实测出现过「连续 5 秒 downloaded 不动，然后一次跳 256KB」，那一刻窗口里
    /// 只剩一两个间隔几十毫秒的样本，算出 `256KB / 0.05s ≈ 5.2 MB/s`，而真实
    /// 速率只有约 65 KB/s，界面上表现为速度在 0 / 285KB/s / 5.2MB/s 之间乱跳。
    ///
    /// 现在取「最早快照到现在」的累计字节增量除以同一段时间，不会被单个分片的
    /// 落盘瞬间放大。
    ///
    /// 右端点取 `Instant::now()` 而非最后一个快照，是有意为之：这样 `speed()`
    /// 是一个自洽的纯函数，不依赖外部刷新循环存活。CDN 停滞检测
    /// （`ChunkScheduler::get_valid_task_speed_values`）是直接调用它的，
    /// 若右端点取最后一个快照，刷新循环一旦停摆，停滞检测读到的就是一个
    /// 永远不变的旧值 —— 那正是要修的毛病本身。
    ///
    /// 两层保险：有刷新循环时窗口会被推进、速度干净地衰减到 0；没有刷新循环时
    /// 分母仍随 `now` 增长，速度持续下降而不会僵住。
    pub fn speed(&self) -> u64 {
        self.speed_at(Instant::now())
    }

    fn speed_at(&self, now: Instant) -> u64 {
        let Some((first_time, first_bytes)) = self.samples.front() else {
            return 0;
        };

        let duration = now.duration_since(*first_time);
        if duration < MIN_MEASUREMENT_DURATION {
            return 0;
        }

        let bytes = self.total_bytes.saturating_sub(*first_bytes);
        if bytes == 0 {
            // 窗口内一个字节都没有 = 确实停了
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
    ///
    /// 复用同一个计算器跑下一轮（暂停后恢复、重试）时必须调用：残留的旧快照会把
    /// 分母拉到「上一轮开始的时刻」，恢复后的速度会被稀释到接近 0。
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

        assert_eq!(calc.speed_at(start + Duration::from_secs(1)), 2 * 1024 * 1024);
    }

    /// 跨度不足一个测量周期时报 0，不外推。
    #[test]
    fn test_speed_requires_warmup() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        calc.refresh_at(start);
        calc.add_sample_at(1024 * 1024, start + Duration::from_millis(500));

        assert_eq!(calc.speed_at(start + Duration::from_millis(500)), 0);
    }

    /// 海量小文件的回归测试：逐任务测速恒为 0，跨任务累计值差分才有速度。
    ///
    /// 每个小文件都是独立任务、独立 `SpeedCalculator`，各自只活 20ms —— 达不到
    /// `MIN_MEASUREMENT_DURATION`，逐个算出来的速度全是 0，求和仍是 0。这正是
    /// 「下载进度在涨、界面速度显示 0 B/s」的成因。
    #[test]
    fn test_observe_total_measures_speed_across_short_tasks() {
        let start = Instant::now();
        let file_size = 8 * 1024;

        // 逐任务：每个任务只活 20ms，测不出速度
        for i in 0..10 {
            let mut per_task = SpeedCalculator::new(10);
            let t0 = start + Duration::from_millis(i * 20);
            per_task.observe_total_at(0, t0);
            per_task.observe_total_at(file_size, t0 + Duration::from_millis(20));
            assert_eq!(
                per_task.speed_at(t0 + Duration::from_millis(20)),
                0,
                "单任务跨度不足 1 秒，必须报 0 而不是外推"
            );
        }

        // 跨任务：文件夹累计字节连续推进，1 秒后能得到真实吞吐
        let mut folder = SpeedCalculator::new(10);
        folder.observe_total_at(0, start);
        for i in 1..=10 {
            folder.observe_total_at(file_size * i, start + Duration::from_millis(i * 100));
        }
        assert_eq!(
            folder.speed_at(start + Duration::from_secs(1)),
            file_size * 10
        );
    }

    /// 累计值回退（进度口径修正）时不得算出负速度或爆表速度。
    #[test]
    fn test_observe_total_tolerates_regression() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        calc.observe_total_at(2 * 1024 * 1024, start);
        calc.observe_total_at(1024 * 1024, start + Duration::from_secs(1));

        assert_eq!(calc.total_bytes(), 1024 * 1024);
        assert_eq!(calc.speed_at(start + Duration::from_secs(1)), 0);
    }

    /// 停滞后由定时刷新把空闲时间灌进窗口，速度衰减到 0。
    #[test]
    fn test_idle_time_reduces_speed_to_zero() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        calc.refresh_at(start);
        for second in 1..=10 {
            calc.add_sample_at(1024 * 1024, start + Duration::from_secs(second));
        }
        assert_eq!(calc.speed_at(start + Duration::from_secs(10)), 1024 * 1024);

        for second in 11..=21 {
            calc.refresh_at(start + Duration::from_secs(second));
        }
        assert_eq!(calc.speed_at(start + Duration::from_secs(21)), 0);
    }

    /// 即使没有刷新循环，速度也必须随时间下降而不是僵住。
    ///
    /// 这是 `speed()` 右端点取 `now` 而非最后一个快照的意义：CDN 停滞检测直接调
    /// `speed()`，不经过刷新循环，右端点若取快照就会永远读到同一个旧值。
    #[test]
    fn test_speed_decays_without_refresh() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        calc.refresh_at(start);
        for second in 1..=5 {
            calc.add_sample_at(1024 * 1024, start + Duration::from_secs(second));
        }
        let at_5s = calc.speed_at(start + Duration::from_secs(5));
        assert_eq!(at_5s, 1024 * 1024);

        // 此后不再有任何 add_sample / refresh —— 模拟刷新循环停摆
        let at_20s = calc.speed_at(start + Duration::from_secs(20));
        assert!(
            at_20s < at_5s / 3,
            "无刷新时速度应随时间下降，5s 时 {} B/s，20s 时仍有 {} B/s",
            at_5s,
            at_20s
        );
    }

    /// 窗口内的快照数量有上界，不会无限增长。
    #[test]
    fn test_snapshot_count_is_bounded() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        for step in 0..=120 {
            calc.add_sample_at(1024, start + Duration::from_millis(step * 500));
        }

        assert!(calc.samples.len() <= 22);
    }

    /// 回归（实测踩到的坑）：短时间内落盘一大块**不能**被算成天文速度。
    ///
    /// 下载按分片落盘，`progress_callback` 并非匀速调用 —— 出现过「连续 5 秒
    /// downloaded 不动，然后一次跳 256KB」。最初的实现用「最早样本到现在」当分母，
    /// 那一刻算出 `256KB / 0.05s ≈ 5.2 MB/s`，而真实速率只有约 65 KB/s。
    #[test]
    fn test_burst_write_is_not_reported_as_huge_speed() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        // 前 5 秒一个字节都没来，只有定时刷新在打快照
        for step in 0..=10 {
            calc.refresh_at(start + Duration::from_millis(step * 500));
        }
        // 第 5 秒一次性落盘 256KB，随后的定时刷新把它记进快照
        calc.add_sample_at(256 * 1024, start + Duration::from_secs(5));
        let speed = calc.refresh_at(start + Duration::from_millis(5500));

        assert!(speed > 0, "有字节进来就该有速度");
        assert!(
            speed < 100 * 1024,
            "5.5 秒里只传了 256KB（约 47KB/s）不应被算成 {} B/s（分母塌缩）",
            speed
        );
    }

    /// 回归：同样的传输量，样本疏密不同也应算出同一个速度。
    ///
    /// 分子分母都取自同一段时间，所以「每 500ms 来一点」和「攒 5 秒来一大块」
    /// 必须一致 —— 分母塌缩的根因就是这两者会算出天差地别的值。
    #[test]
    fn test_speed_is_insensitive_to_sample_density() {
        let start = Instant::now();
        let at_10s = start + Duration::from_secs(10);

        // 密集：每 500ms 来 100KB，10 秒共 2MB
        let mut dense = SpeedCalculator::new(10);
        dense.refresh_at(start);
        for step in 1..=20 {
            dense.add_sample_at(100_000, start + Duration::from_millis(step * 500));
        }

        // 稀疏：只在第 5 秒和第 10 秒各来 1MB，10 秒同样共 2MB
        let mut sparse = SpeedCalculator::new(10);
        sparse.refresh_at(start);
        sparse.add_sample_at(1_000_000, start + Duration::from_secs(5));
        sparse.add_sample_at(1_000_000, start + Duration::from_secs(10));

        assert_eq!(dense.speed_at(at_10s), 200_000);
        assert_eq!(sparse.speed_at(at_10s), dense.speed_at(at_10s));
    }

    /// 回归：复用计算器时必须 reset，否则上一轮的快照会把分母拉长、速度稀释成 0。
    #[test]
    fn test_reset_clears_stale_baseline() {
        let mut calc = SpeedCalculator::new(10);
        let start = Instant::now();

        // 第一轮：跑了一会儿
        calc.refresh_at(start);
        for second in 1..=5 {
            calc.add_sample_at(1024 * 1024, start + Duration::from_secs(second));
        }

        // 暂停 5 分钟后恢复，复用同一个计算器
        let resumed = start + Duration::from_secs(305);
        calc.reset();
        calc.refresh_at(resumed);
        calc.add_sample_at(1024 * 1024, resumed + Duration::from_secs(1));

        // 分母应为恢复后的 1 秒，而不是「从 5 分钟前算起」
        assert_eq!(calc.speed_at(resumed + Duration::from_secs(1)), 1024 * 1024);
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
