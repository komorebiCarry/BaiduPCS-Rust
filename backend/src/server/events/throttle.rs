//! 进度事件节流器
//!
//! 用于控制进度事件的发布频率，避免事件风暴
//! 支持时间间隔节流（建议 200-250）

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// 默认节流间隔（毫秒）
pub const DEFAULT_THROTTLE_INTERVAL_MS: u64 = 200;

/// 进度事件节流器
///
/// 线程安全的时间节流器
/// 典型用法：每次更新进度时调用 `should_emit()`，返回 true 时才发布事件
#[derive(Debug)]
pub struct ProgressThrottler {
    /// 上次发布事件的时间
    last_emit: Mutex<Option<Instant>>,
    /// 节流间隔
    interval: Duration,
}

impl ProgressThrottler {
    /// 创建新的节流器
    ///
    /// # 参数
    /// * `interval` - 最小发布间隔
    pub fn new(interval: Duration) -> Self {
        Self {
            last_emit: Mutex::new(None),
            interval,
        }
    }

    /// 使用默认间隔（200ms）创建节流器
    pub fn default_interval() -> Self {
        Self::new(Duration::from_millis(DEFAULT_THROTTLE_INTERVAL_MS))
    }

    /// 使用指定毫秒间隔创建节流器
    pub fn with_millis(interval_ms: u64) -> Self {
        Self::new(Duration::from_millis(interval_ms))
    }

    /// 检查是否应该发布事件
    ///
    /// 如果距离上次发布已超过节流间隔，返回 true 并更新时间戳
    /// 否则返回 false
    pub fn should_emit(&self) -> bool {
        let now = Instant::now();
        let mut last = self.last_emit.lock();

        match *last {
            Some(last_time) if now.duration_since(last_time) < self.interval => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }

    /// 强制发布（用于最后一次更新或完成时）
    ///
    /// 不检查时间间隔，直接更新时间戳并返回 true
    pub fn force_emit(&self) -> bool {
        *self.last_emit.lock() = Some(Instant::now());
        true
    }

    /// 重置节流器状态
    pub fn reset(&self) {
        *self.last_emit.lock() = None;
    }
}

impl Default for ProgressThrottler {
    fn default() -> Self {
        Self::default_interval()
    }
}

impl Clone for ProgressThrottler {
    fn clone(&self) -> Self {
        Self {
            last_emit: Mutex::new(*self.last_emit.lock()),
            interval: self.interval,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_throttler_basic() {
        let throttler = ProgressThrottler::with_millis(100);

        // 第一次应该发布
        assert!(throttler.should_emit());

        // 立即再次调用，不应该发布
        assert!(!throttler.should_emit());
    }

    #[test]
    fn test_throttler_after_interval() {
        let throttler = ProgressThrottler::with_millis(50);

        assert!(throttler.should_emit());

        // 等待超过间隔
        thread::sleep(Duration::from_millis(60));

        // 应该可以发布
        assert!(throttler.should_emit());
    }

    #[test]
    fn test_force_emit() {
        let throttler = ProgressThrottler::with_millis(1000);

        assert!(throttler.should_emit());
        assert!(!throttler.should_emit());

        // 强制发布
        assert!(throttler.force_emit());
    }

    #[test]
    fn test_reset() {
        let throttler = ProgressThrottler::with_millis(1000);

        throttler.should_emit();
        assert!(!throttler.should_emit());

        // 重置后应该可以发布
        throttler.reset();
        assert!(throttler.should_emit());
    }
}
