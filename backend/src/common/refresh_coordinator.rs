//! 链接刷新协调器
//!
//! 防止多个检测机制同时触发刷新
//! ⚠️ 修复问题5：使用 compare_exchange 避免竞态条件

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// 刷新协调器配置
#[derive(Debug, Clone)]
pub struct RefreshCoordinatorConfig {
    /// 最小刷新间隔（秒）
    pub min_refresh_interval_secs: u64,
}

impl Default for RefreshCoordinatorConfig {
    fn default() -> Self {
        Self {
            min_refresh_interval_secs: 30, // 至少间隔30秒
        }
    }
}

/// 链接刷新协调器
///
/// 使用原子操作确保线程安全，无竞态条件
///
/// 核心功能：
/// 1. 防止多个检测机制同时触发刷新
/// 2. 限制刷新频率，避免过于频繁的刷新
/// 3. 提供 RAII 风格的刷新守卫，自动释放锁
#[derive(Debug)]
pub struct RefreshCoordinator {
    /// 是否正在刷新
    is_refreshing: AtomicBool,
    /// 上次刷新时间戳（毫秒）
    last_refresh_ms: AtomicU64,
    /// 配置
    config: RefreshCoordinatorConfig,
}

impl RefreshCoordinator {
    /// 创建新的刷新协调器
    pub fn new(config: RefreshCoordinatorConfig) -> Self {
        Self {
            is_refreshing: AtomicBool::new(false),
            last_refresh_ms: AtomicU64::new(0),
            config,
        }
    }

    /// 获取当前时间戳（毫秒）
    fn current_time_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// 尝试获取刷新锁
    ///
    /// ⚠️ 修复问题5：使用 compare_exchange 原子操作，避免竞态条件
    ///
    /// # 返回
    /// - `Some(RefreshGuard)`: 成功获取，可以执行刷新
    /// - `None`: 正在刷新或间隔太短
    pub fn try_acquire(&self) -> Option<RefreshGuard> {
        // 1. 原子性地尝试将 is_refreshing 从 false 改为 true
        //    使用 compare_exchange 而非 swap，确保只有一个线程能成功
        if self
            .is_refreshing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // 已经有其他线程在刷新
            return None;
        }

        // 2. 成功获取锁，检查时间间隔
        let now_ms = Self::current_time_ms();
        let interval_ms = self.config.min_refresh_interval_secs * 1000;

        // 原子性地检查并更新时间戳
        loop {
            let last_ms = self.last_refresh_ms.load(Ordering::SeqCst);

            if now_ms.saturating_sub(last_ms) < interval_ms {
                // 间隔太短，释放锁并返回 None
                self.is_refreshing.store(false, Ordering::SeqCst);
                return None;
            }

            // 尝试原子更新时间戳
            if self
                .last_refresh_ms
                .compare_exchange(last_ms, now_ms, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // 成功更新时间戳，返回 guard
                break;
            }
            // 如果 compare_exchange 失败，说明有其他线程更新了时间戳
            // 继续循环重新检查
        }

        Some(RefreshGuard { coordinator: self })
    }

    /// 强制获取刷新锁（忽略时间间隔限制）
    ///
    /// 用于定时刷新场景，定时器本身已保证间隔
    pub fn force_acquire(&self) -> Option<RefreshGuard> {
        if self
            .is_refreshing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }

        // 更新时间戳
        self.last_refresh_ms
            .store(Self::current_time_ms(), Ordering::SeqCst);

        Some(RefreshGuard { coordinator: self })
    }

    /// 检查是否正在刷新
    pub fn is_refreshing(&self) -> bool {
        self.is_refreshing.load(Ordering::SeqCst)
    }

    /// 获取距离上次刷新的时间（秒）
    pub fn seconds_since_last_refresh(&self) -> u64 {
        let now_ms = Self::current_time_ms();
        let last_ms = self.last_refresh_ms.load(Ordering::SeqCst);
        now_ms.saturating_sub(last_ms) / 1000
    }
}

/// 刷新守卫（RAII）
///
/// 当 guard 被 drop 时自动释放刷新锁
pub struct RefreshGuard<'a> {
    coordinator: &'a RefreshCoordinator,
}

impl<'a> Drop for RefreshGuard<'a> {
    fn drop(&mut self) {
        self.coordinator
            .is_refreshing
            .store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_single_acquire() {
        let coordinator = RefreshCoordinator::new(RefreshCoordinatorConfig {
            min_refresh_interval_secs: 0, // 无间隔限制
        });

        // 第一次应成功
        let guard = coordinator.try_acquire();
        assert!(guard.is_some());

        // 正在刷新时应失败
        let guard2 = coordinator.try_acquire();
        assert!(guard2.is_none());

        // drop guard 后应能再次获取
        drop(guard);
        let guard3 = coordinator.try_acquire();
        assert!(guard3.is_some());
    }

    #[test]
    fn test_interval_limit() {
        let coordinator = RefreshCoordinator::new(RefreshCoordinatorConfig {
            min_refresh_interval_secs: 1,
        });

        // 第一次应成功
        {
            let guard = coordinator.try_acquire();
            assert!(guard.is_some());
            // guard 在这里 drop
        }

        // 间隔太短，应失败
        let guard2 = coordinator.try_acquire();
        assert!(guard2.is_none());

        // 等待后应成功
        thread::sleep(std::time::Duration::from_secs(2));
        let guard3 = coordinator.try_acquire();
        assert!(guard3.is_some());
    }

    #[test]
    fn test_concurrent_acquire() {
        let coordinator = Arc::new(RefreshCoordinator::new(RefreshCoordinatorConfig {
            min_refresh_interval_secs: 0, // 无间隔限制
        }));

        let mut handles = vec![];
        let success_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // 启动 10 个线程同时尝试获取锁
        for _ in 0..10 {
            let coord = coordinator.clone();
            let count = success_count.clone();
            handles.push(thread::spawn(move || {
                if coord.try_acquire().is_some() {
                    count.fetch_add(1, Ordering::SeqCst);
                    // 模拟刷新操作
                    thread::sleep(std::time::Duration::from_millis(10));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 由于 10ms 的 sleep，大部分线程会被阻塞
        // 但至少有一个线程能成功
        assert!(success_count.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn test_force_acquire() {
        let coordinator = RefreshCoordinator::new(RefreshCoordinatorConfig {
            min_refresh_interval_secs: 60, // 60秒间隔
        });

        // 第一次正常获取
        {
            let guard = coordinator.try_acquire();
            assert!(guard.is_some());
        }

        // 正常 try_acquire 应该因为间隔限制失败
        let guard2 = coordinator.try_acquire();
        assert!(guard2.is_none());

        // force_acquire 应该成功
        let guard3 = coordinator.force_acquire();
        assert!(guard3.is_some());
    }
}
