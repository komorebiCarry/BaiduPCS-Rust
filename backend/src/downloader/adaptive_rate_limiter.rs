// 智能限流器 - API 风控优化
//
// 设计目标：
// 1. 自动检测并规避百度网盘 API 风控
// 2. 动态调整 QPS，最大化吞吐量
// 3. 支持降级策略

use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, warn};

/// 智能限流器配置
#[derive(Debug, Clone)]
pub struct AdaptiveRateLimiterConfig {
    /// 初始 QPS
    pub initial_qps: f64,
    /// 最小 QPS
    pub min_qps: f64,
    /// 最大 QPS
    pub max_qps: f64,
    /// 成功时 QPS 增长率
    pub increase_rate: f64,
    /// 失败时 QPS 衰减率
    pub decrease_rate: f64,
    /// 触发风控后的退避时间（秒）
    pub backoff_seconds: u64,
    /// 连续错误阈值
    pub error_threshold: u32,
}

impl Default for AdaptiveRateLimiterConfig {
    fn default() -> Self {
        Self {
            initial_qps: 10.0,       // 初始 10 QPS
            min_qps: 1.0,            // 最低 1 QPS
            max_qps: 50.0,           // 最高 50 QPS
            increase_rate: 1.05,     // 成功时提速 5%
            decrease_rate: 0.5,      // 失败时降速 50%
            backoff_seconds: 60,     // 风控后等待 60 秒
            error_threshold: 3,      // 3 次连续错误触发降速
        }
    }
}

/// 智能限流器
pub struct AdaptiveRateLimiter {
    config: AdaptiveRateLimiterConfig,
    /// 当前 QPS
    current_qps: Arc<tokio::sync::RwLock<f64>>,
    /// 连续错误计数
    error_count: Arc<tokio::sync::RwLock<u32>>,
    /// 退避截止时间
    backoff_until: Arc<tokio::sync::RwLock<Option<Instant>>>,
    /// 信号量（令牌桶）
    semaphore: Arc<Semaphore>,
    /// 最后请求时间
    last_request: Arc<tokio::sync::RwLock<Option<Instant>>>,
}

impl AdaptiveRateLimiter {
    /// 创建智能限流器
    pub fn new(config: AdaptiveRateLimiterConfig) -> Self {
        let permits = (config.initial_qps * 2.0).ceil() as usize;

        Self {
            current_qps: Arc::new(tokio::sync::RwLock::new(config.initial_qps)),
            error_count: Arc::new(tokio::sync::RwLock::new(0)),
            backoff_until: Arc::new(tokio::sync::RwLock::new(None)),
            semaphore: Arc::new(Semaphore::new(permits)),
            last_request: Arc::new(tokio::sync::RwLock::new(None)),
            config,
        }
    }

    /// 获取许可（阻塞直到可以发送请求）
    pub async fn acquire(&self) -> Result<RateLimitPermit> {
        // 检查是否在退避期
        {
            let backoff = self.backoff_until.read().await;
            if let Some(until) = *backoff {
                if Instant::now() < until {
                    let remaining = until.duration_since(Instant::now());
                    warn!(
                        "⏸️ 正在退避中，剩余 {:.1} 秒",
                        remaining.as_secs_f64()
                    );
                    sleep(remaining).await;
                }
            }
        }

        // 获取令牌
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await?;

        // 根据 QPS 延迟
        let delay = self.calculate_delay().await;
        if delay > Duration::ZERO {
            sleep(delay).await;
        }

        // 更新最后请求时间
        {
            let mut last = self.last_request.write().await;
            *last = Some(Instant::now());
        }

        Ok(RateLimitPermit {
            limiter: self.clone(),
            _permit: permit,
        })
    }

    /// 报告请求成功
    pub async fn on_success(&self) {
        // 重置错误计数
        {
            let mut count = self.error_count.write().await;
            *count = 0;
        }

        // 缓慢提速
        {
            let mut qps = self.current_qps.write().await;
            let new_qps = (*qps * self.config.increase_rate).min(self.config.max_qps);

            if new_qps != *qps {
                debug!("⬆️ QPS 提升: {:.2} -> {:.2}", *qps, new_qps);
                *qps = new_qps;
            }
        }
    }

    /// 报告请求失败
    pub async fn on_error(&self, errno: Option<i32>) {
        // 检查是否为风控错误
        let is_rate_limit = match errno {
            Some(132) | Some(9019) => true, // 百度网盘风控错误码
            _ => false,
        };

        if is_rate_limit {
            warn!("🚨 检测到风控错误: errno={:?}", errno);
            self.on_rate_limit().await;
        } else {
            // 普通错误，增加错误计数
            let mut count = self.error_count.write().await;
            *count += 1;

            if *count >= self.config.error_threshold {
                warn!("⚠️ 连续错误达到阈值: {}", *count);
                self.slow_down().await;
            }
        }
    }

    /// 触发风控时的处理
    async fn on_rate_limit(&self) {
        // 大幅降速
        {
            let mut qps = self.current_qps.write().await;
            let new_qps = (*qps * 0.2).max(self.config.min_qps); // 降至 20%
            warn!("🔻 风控触发，QPS 降低: {:.2} -> {:.2}", *qps, new_qps);
            *qps = new_qps;
        }

        // 设置退避时间
        {
            let mut backoff = self.backoff_until.write().await;
            let until = Instant::now() + Duration::from_secs(self.config.backoff_seconds);
            *backoff = Some(until);
            warn!("⏸️ 进入退避期: {} 秒", self.config.backoff_seconds);
        }

        // 重置错误计数
        {
            let mut count = self.error_count.write().await;
            *count = 0;
        }
    }

    /// 降速
    async fn slow_down(&self) {
        let mut qps = self.current_qps.write().await;
        let new_qps = (*qps * self.config.decrease_rate).max(self.config.min_qps);
        warn!("🔻 降低 QPS: {:.2} -> {:.2}", *qps, new_qps);
        *qps = new_qps;
    }

    /// 计算延迟时间
    async fn calculate_delay(&self) -> Duration {
        let qps = *self.current_qps.read().await;
        let interval = Duration::from_secs_f64(1.0 / qps);

        let last = *self.last_request.read().await;

        match last {
            Some(last_time) => {
                let elapsed = Instant::now().duration_since(last_time);
                if elapsed < interval {
                    interval - elapsed
                } else {
                    Duration::ZERO
                }
            }
            None => Duration::ZERO,
        }
    }

    /// 获取当前 QPS
    pub async fn current_qps(&self) -> f64 {
        *self.current_qps.read().await
    }

    /// 获取统计信息
    pub async fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            current_qps: *self.current_qps.read().await,
            error_count: *self.error_count.read().await,
            in_backoff: self.backoff_until.read().await.is_some(),
        }
    }
}

impl Clone for AdaptiveRateLimiter {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            current_qps: Arc::clone(&self.current_qps),
            error_count: Arc::clone(&self.error_count),
            backoff_until: Arc::clone(&self.backoff_until),
            semaphore: Arc::clone(&self.semaphore),
            last_request: Arc::clone(&self.last_request),
        }
    }
}

/// 限流许可
pub struct RateLimitPermit {
    limiter: AdaptiveRateLimiter,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl RateLimitPermit {
    /// 报告成功
    pub async fn success(self) {
        self.limiter.on_success().await;
    }

    /// 报告失败
    pub async fn error(self, errno: Option<i32>) {
        self.limiter.on_error(errno).await;
    }
}

/// 限流器统计信息
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub current_qps: f64,
    pub error_count: u32,
    pub in_backoff: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = AdaptiveRateLimiterConfig::default();
        assert_eq!(config.initial_qps, 10.0);
        assert_eq!(config.min_qps, 1.0);
        assert_eq!(config.max_qps, 50.0);
    }

    #[tokio::test]
    async fn test_rate_limiter_creation() {
        let config = AdaptiveRateLimiterConfig::default();
        let limiter = AdaptiveRateLimiter::new(config);

        let qps = limiter.current_qps().await;
        assert_eq!(qps, 10.0);
    }

    #[tokio::test]
    async fn test_success_increases_qps() {
        let config = AdaptiveRateLimiterConfig::default();
        let limiter = AdaptiveRateLimiter::new(config);

        let initial_qps = limiter.current_qps().await;
        limiter.on_success().await;
        let new_qps = limiter.current_qps().await;

        assert!(new_qps > initial_qps);
    }

    #[tokio::test]
    async fn test_error_decreases_qps() {
        let mut config = AdaptiveRateLimiterConfig::default();
        config.error_threshold = 1;

        let limiter = AdaptiveRateLimiter::new(config);

        let initial_qps = limiter.current_qps().await;
        limiter.on_error(Some(500)).await;
        let new_qps = limiter.current_qps().await;

        assert!(new_qps < initial_qps);
    }
}
