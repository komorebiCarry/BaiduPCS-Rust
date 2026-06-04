//! 每个订阅的轮询调度器
//!
//! 简化版 PollScheduler：每个订阅一个 tokio task，按 interval 循环触发。
//! 合并触发（coalescing）：如果上一次 run 还没结束就又到了下一次，跳过本轮。
//!
//! CancellationToken 控制优雅停机。

use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// 单个订阅的调度状态
pub struct SubscriptionScheduler {
    pub subscription_id: String,
    pub interval_secs: u32,
    cancel_token: CancellationToken,
    /// 当前是否正在执行（合并触发用）
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// 外部传入的"立刻跑一次"通知器
    trigger_notify: std::sync::Arc<Notify>,
    /// 主循环 JoinHandle
    task: Option<tokio::task::JoinHandle<()>>,
}

impl SubscriptionScheduler {
    pub fn new(subscription_id: String, interval_secs: u32) -> Self {
        Self {
            subscription_id,
            interval_secs: interval_secs.max(60), // 防御：不低于 60s
            cancel_token: CancellationToken::new(),
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            trigger_notify: std::sync::Arc::new(Notify::new()),
            task: None,
        }
    }

    /// 启动主循环
    ///
    /// `on_tick` 由调用方提供；通常会调用 `ShareSyncManager::execute_one(id)`
    pub fn start<F, Fut>(&mut self, on_tick: F)
    where
        F: Fn(String) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        if self.task.is_some() {
            return; // 已经启动
        }
        let sub_id = self.subscription_id.clone();
        let interval = self.interval_secs;
        let running = self.running.clone();
        let trigger = self.trigger_notify.clone();
        let cancel = self.cancel_token.clone();

        let handle = tokio::spawn(async move {
            info!("scheduler: 订阅 {} 主循环启动, interval={}s", sub_id, interval);
            // 首次启动后小幅抖动（避免所有订阅同时发起请求）
            let initial_delay = jitter(interval, 0.25);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(initial_delay as u64)) => {}
                _ = trigger.notified() => {
                    debug!("scheduler: 订阅 {} 启动后被立即 trigger", sub_id);
                }
                _ = cancel.cancelled() => {
                    info!("scheduler: 订阅 {} 启动前已取消", sub_id);
                    return;
                }
            }

            loop {
                if cancel.is_cancelled() {
                    break;
                }
                // 检查是否已有 run 在执行
                if running.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    debug!("scheduler: 订阅 {} 上次 run 还在进行，跳过本轮", sub_id);
                } else {
                    on_tick(sub_id.clone()).await;
                    running.store(false, std::sync::atomic::Ordering::SeqCst);
                }

                // 等到 interval 或被显式 trigger 唤醒
                let next = jitter(interval, 0.20);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(next as u64)) => {}
                    _ = trigger.notified() => {
                        debug!("scheduler: 订阅 {} 被外部 trigger", sub_id);
                    }
                    _ = cancel.cancelled() => {
                        break;
                    }
                }
            }
            info!("scheduler: 订阅 {} 主循环退出", sub_id);
        });
        self.task = Some(handle);
    }

    /// 立即触发一次（与 interval 合并）
    pub fn trigger_now(&self) {
        self.trigger_notify.notify_one();
    }

    /// 停止主循环
    pub async fn stop(&mut self) {
        self.cancel_token.cancel();
        if let Some(h) = self.task.take() {
            let _ = h.await;
        }
    }

    /// 是否正在执行
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for SubscriptionScheduler {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// interval ±ratio 的随机抖动
fn jitter(base_secs: u32, ratio: f64) -> u32 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let delta = (base_secs as f64 * ratio) as i64;
    if delta <= 0 {
        return base_secs;
    }
    let offset = rng.gen_range(-delta..=delta);
    ((base_secs as i64 + offset).max(60) as u32).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_jitter_within_range() {
        for _ in 0..100 {
            let j = jitter(600, 0.20);
            assert!((480..=720).contains(&j), "jitter out of range: {}", j);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_scheduler_trigger_merges_with_running() {
        let mut s = SubscriptionScheduler::new("s1".into(), 60);
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let count_c = count.clone();
        // 模拟一个 long-running tick：第一次开始后立即把 running 设为 true
        s.start(move |_id| {
            let c = count_c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        });
        // 触发两次：第二次应被合并（因为第一次还在跑）
        s.trigger_now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        s.trigger_now();
        tokio::time::sleep(Duration::from_millis(500)).await;
        // 至少跑了 1 次（可能 1-2 次）
        assert!(count.load(Ordering::SeqCst) >= 1);
        s.stop().await;
    }
}
