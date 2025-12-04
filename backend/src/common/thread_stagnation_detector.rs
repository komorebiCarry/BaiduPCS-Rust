//! 线程停滞检测器
//!
//! 检测大面积线程停滞（速度接近零）
//!
//! 核心机制：
//! 1. 监控所有活跃任务的速度
//! 2. 统计速度接近零的任务数量
//! 3. 当停滞比例超过阈值时，触发链接刷新

use std::time::Instant;
use tracing::{debug, info};

/// 线程停滞检测配置
#[derive(Clone, Debug)]
pub struct StagnationConfig {
    /// 停滞速度阈值（KB/s）- 低于此值视为停滞
    pub near_zero_threshold_kbps: u64,
    /// 停滞比例阈值 - 超过此比例触发刷新
    pub stagnation_ratio: f64,
    /// 最小线程数 - 少于此数不检测
    pub min_threads: usize,
    /// 启动延迟（秒）- 任务开始后多久开始检测
    pub startup_delay_secs: u64,
}

impl Default for StagnationConfig {
    fn default() -> Self {
        Self {
            near_zero_threshold_kbps: 10, // 10 KB/s
            stagnation_ratio: 0.8,        // 80%
            min_threads: 3,
            startup_delay_secs: 10,
        }
    }
}

/// 线程停滞检测器
#[derive(Debug)]
pub struct ThreadStagnationDetector {
    /// 任务开始时间
    task_start: Instant,
    /// 配置
    config: StagnationConfig,
}

impl ThreadStagnationDetector {
    /// 创建新的线程停滞检测器
    pub fn new(config: StagnationConfig) -> Self {
        Self {
            task_start: Instant::now(),
            config,
        }
    }

    /// 检查线程停滞
    ///
    /// # 参数
    /// * `thread_speeds` - 各线程/任务的速度（字节/秒）
    ///                     从 ChunkScheduler.get_valid_task_speed_values() 获取
    ///
    /// # 返回
    /// - `true`: 检测到大面积停滞，需要刷新链接
    /// - `false`: 正常
    pub fn check(&self, thread_speeds: &[u64]) -> bool {
        // 1. 检查启动延迟
        if self.task_start.elapsed().as_secs() < self.config.startup_delay_secs {
            return false;
        }

        // 2. 检查最小线程数
        if thread_speeds.len() < self.config.min_threads {
            return false;
        }

        // 3. 统计停滞线程
        let threshold_bytes = self.config.near_zero_threshold_kbps * 1024;
        let stagnant_count = thread_speeds
            .iter()
            .filter(|&&speed| speed <= threshold_bytes)
            .count();

        // 4. 计算停滞比例
        let ratio = stagnant_count as f64 / thread_speeds.len() as f64;

        debug!(
            "线程停滞检测: {}/{} 个线程速度 <= {} KB/s (比例: {:.1}%)",
            stagnant_count,
            thread_speeds.len(),
            self.config.near_zero_threshold_kbps,
            ratio * 100.0
        );

        // 5. 判断是否超过阈值
        if ratio >= self.config.stagnation_ratio {
            info!(
                "⚠️ 线程大面积停滞: {}/{} 个线程速度 <= {} KB/s (比例: {:.1}%)",
                stagnant_count,
                thread_speeds.len(),
                self.config.near_zero_threshold_kbps,
                ratio * 100.0
            );
            return true;
        }

        false
    }

    /// 重置检测器
    pub fn reset(&mut self) {
        self.task_start = Instant::now();
    }

    /// 获取距离启动的时间（秒）
    pub fn elapsed_secs(&self) -> u64 {
        self.task_start.elapsed().as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_speeds() {
        let config = StagnationConfig {
            startup_delay_secs: 0, // 无启动延迟
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 正常速度，不应触发
        let speeds = vec![100_000, 200_000, 300_000, 400_000, 500_000];
        assert!(!detector.check(&speeds));
    }

    #[test]
    fn test_stagnation_detected() {
        let config = StagnationConfig {
            startup_delay_secs: 0,
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 80%停滞（4/5），应触发
        let stagnant_speeds = vec![1_000, 2_000, 3_000, 4_000, 500_000];
        assert!(detector.check(&stagnant_speeds));
    }

    #[test]
    fn test_partial_stagnation() {
        let config = StagnationConfig {
            startup_delay_secs: 0,
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 60%停滞（3/5），不应触发
        let partial_speeds = vec![1_000, 2_000, 3_000, 100_000, 200_000];
        assert!(!detector.check(&partial_speeds));
    }

    #[test]
    fn test_min_threads() {
        let config = StagnationConfig {
            startup_delay_secs: 0,
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 线程数不足，不应触发
        let few_threads = vec![1_000, 2_000];
        assert!(!detector.check(&few_threads));
    }

    #[test]
    fn test_startup_delay() {
        let config = StagnationConfig {
            startup_delay_secs: 60, // 60秒延迟
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 刚启动，即使全部停滞也不应触发
        let stagnant_speeds = vec![1_000, 1_000, 1_000, 1_000, 1_000];
        assert!(!detector.check(&stagnant_speeds));
    }

    #[test]
    fn test_all_stagnant() {
        let config = StagnationConfig {
            startup_delay_secs: 0,
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 100%停滞，应触发
        let all_stagnant = vec![1_000, 2_000, 3_000, 4_000, 5_000];
        assert!(detector.check(&all_stagnant));
    }

    #[test]
    fn test_empty_speeds() {
        let config = StagnationConfig {
            startup_delay_secs: 0,
            near_zero_threshold_kbps: 10,
            stagnation_ratio: 0.8,
            min_threads: 3,
        };
        let detector = ThreadStagnationDetector::new(config);

        // 空速度列表，不应触发
        let empty: Vec<u64> = vec![];
        assert!(!detector.check(&empty));
    }
}
