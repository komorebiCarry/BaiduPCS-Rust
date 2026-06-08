//! 速率限制器模块
//!
//! 实现基于 IP 的登录尝试速率限制，防止暴力破解攻击。
//! - 阈值：5 次失败后锁定 15 分钟
//! - 成功登录后重置计数器
//! - 定时清理过期记录，防止内存耗尽攻击
//!
//! **重要说明**：RateLimiter 仅作用于 Web 认证登录端点 (`POST /api/v1/web-auth/login`)，
//! 不会影响任何现有功能，包括：
//! - 百度二维码登录轮询 (`/api/v1/auth/qrcode/status`)
//! - 下载/上传进度轮询
//! - WebSocket 实时推送
//! - 所有其他 API 端点

use crate::web_auth::types::LoginAttempt;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// 最大失败尝试次数
pub const MAX_FAILED_ATTEMPTS: u32 = 5;

/// 锁定时间（秒）：15 分钟
pub const LOCKOUT_DURATION: i64 = 15 * 60;

/// 尝试窗口时间（秒）：15 分钟
pub const ATTEMPT_WINDOW: i64 = 15 * 60;

/// 最大记录数量（防止内存耗尽攻击）
pub const MAX_RECORDS: usize = 10000;

/// 清理任务间隔（秒）：5 分钟
pub const CLEANUP_INTERVAL_SECS: u64 = 5 * 60;

/// 速率限制器
///
/// 使用滑动窗口算法跟踪登录尝试，防止暴力破解攻击。
/// 支持定时清理过期记录和内存限制。
pub struct RateLimiter {
    /// IP -> 登录尝试记录
    attempts: DashMap<String, LoginAttempt>,
    /// 清理任务取消令牌
    cleanup_cancel_token: RwLock<Option<CancellationToken>>,
}

impl RateLimiter {
    /// 创建新的速率限制器
    pub fn new() -> Self {
        Self {
            attempts: DashMap::new(),
            cleanup_cancel_token: RwLock::new(None),
        }
    }

    /// 启动定时清理任务
    ///
    /// 每 5 分钟清理一次过期记录。
    /// 仅当认证模式不为 `None` 时应调用此方法。
    pub async fn start_cleanup_task(self: &Arc<Self>) {
        let mut guard = self.cleanup_cancel_token.write().await;

        // 如果已有清理任务在运行，先停止它
        if let Some(token) = guard.take() {
            token.cancel();
        }

        let cancel_token = CancellationToken::new();
        *guard = Some(cancel_token.clone());
        drop(guard);

        let limiter = Arc::clone(self);

        tokio::spawn(async move {
            debug!("RateLimiter cleanup task started");
            let interval = Duration::from_secs(CLEANUP_INTERVAL_SECS);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("RateLimiter cleanup task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(interval) => {
                        let before_count = limiter.active_records_count();
                        limiter.cleanup_expired();
                        let after_count = limiter.active_records_count();

                        if before_count != after_count {
                            debug!(
                                "RateLimiter cleanup: removed {} expired records ({} -> {})",
                                before_count - after_count,
                                before_count,
                                after_count
                            );
                        }
                    }
                }
            }
        });
    }

    /// 停止定时清理任务
    pub async fn stop_cleanup_task(&self) {
        let mut guard = self.cleanup_cancel_token.write().await;
        if let Some(token) = guard.take() {
            token.cancel();
            debug!("RateLimiter cleanup task stop requested");
        }
    }

    /// 清空所有记录
    ///
    /// 当认证模式切换到 `None` 时调用
    pub fn clear_all(&self) {
        self.attempts.clear();
        debug!("RateLimiter: all records cleared");
    }

    /// 检查是否已达到最大记录数
    pub fn is_at_capacity(&self) -> bool {
        self.attempts.len() >= MAX_RECORDS
    }

    /// 检查 IP 是否被锁定
    ///
    /// 返回剩余锁定时间（秒），如果未锁定则返回 None
    pub fn is_locked(&self, ip: &str) -> Option<i64> {
        let now = chrono::Utc::now().timestamp();

        if let Some(attempt) = self.attempts.get(ip) {
            if let Some(locked_until) = attempt.locked_until {
                if locked_until > now {
                    return Some(locked_until - now);
                }
            }
        }

        None
    }

    /// 记录失败尝试
    ///
    /// 如果失败次数达到阈值，将锁定该 IP。
    /// 如果已达到最大记录数且是新 IP，将拒绝记录并返回 false。
    ///
    /// # Returns
    /// - `true` 如果记录成功
    /// - `false` 如果因达到容量限制而拒绝（仅对新 IP）
    pub fn record_failure(&self, ip: &str) -> bool {
        let now = chrono::Utc::now().timestamp();

        // 检查是否是已存在的 IP
        if self.attempts.contains_key(ip) {
            self.attempts.entry(ip.to_string()).and_modify(|attempt| {
                // 检查是否在窗口期内
                if now - attempt.first_attempt > ATTEMPT_WINDOW {
                    // 窗口期已过，重置计数
                    attempt.count = 1;
                    attempt.first_attempt = now;
                    attempt.locked_until = None;
                } else {
                    // 在窗口期内，增加计数
                    attempt.count += 1;

                    // 检查是否达到阈值
                    if attempt.count >= MAX_FAILED_ATTEMPTS {
                        attempt.locked_until = Some(now + LOCKOUT_DURATION);
                    }
                }
            });
            return true;
        }

        // 新 IP，检查容量
        if self.is_at_capacity() {
            warn!(
                "RateLimiter at capacity ({}), rejecting new IP: {}",
                MAX_RECORDS, ip
            );
            return false;
        }

        // 插入新记录
        self.attempts.insert(
            ip.to_string(),
            LoginAttempt {
                count: 1,
                first_attempt: now,
                locked_until: None,
            },
        );
        true
    }

    /// 重置计数器（登录成功时调用）
    pub fn reset(&self, ip: &str) {
        self.attempts.remove(ip);
    }

    /// 清理过期记录
    ///
    /// 移除窗口期已过且未锁定的记录，以及锁定期已过的记录
    pub fn cleanup_expired(&self) {
        let now = chrono::Utc::now().timestamp();

        self.attempts.retain(|_, attempt| {
            // 如果有锁定时间且已过期，移除
            if let Some(locked_until) = attempt.locked_until {
                if locked_until <= now {
                    return false;
                }
            }

            // 如果窗口期已过且未锁定，移除
            if attempt.locked_until.is_none() && now - attempt.first_attempt > ATTEMPT_WINDOW {
                return false;
            }

            true
        });
    }

    /// 获取 IP 的当前失败次数
    pub fn get_failure_count(&self, ip: &str) -> u32 {
        self.attempts
            .get(ip)
            .map(|attempt| attempt.count)
            .unwrap_or(0)
    }

    /// 获取剩余尝试次数
    pub fn get_remaining_attempts(&self, ip: &str) -> u32 {
        let count = self.get_failure_count(ip);
        MAX_FAILED_ATTEMPTS.saturating_sub(count)
    }

    /// 获取活跃记录数量（用于监控）
    pub fn active_records_count(&self) -> usize {
        self.attempts.len()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// 创建共享的 RateLimiter
pub fn create_rate_limiter() -> Arc<RateLimiter> {
    Arc::new(RateLimiter::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_ip_not_locked() {
        let limiter = RateLimiter::new();
        assert!(limiter.is_locked("192.168.1.1").is_none());
    }

    #[test]
    fn test_single_failure_not_locked() {
        let limiter = RateLimiter::new();
        assert!(limiter.record_failure("192.168.1.1"));
        assert!(limiter.is_locked("192.168.1.1").is_none());
        assert_eq!(limiter.get_failure_count("192.168.1.1"), 1);
    }

    #[test]
    fn test_multiple_failures_below_threshold() {
        let limiter = RateLimiter::new();
        for _ in 0..4 {
            assert!(limiter.record_failure("192.168.1.1"));
        }
        assert!(limiter.is_locked("192.168.1.1").is_none());
        assert_eq!(limiter.get_failure_count("192.168.1.1"), 4);
        assert_eq!(limiter.get_remaining_attempts("192.168.1.1"), 1);
    }

    #[test]
    fn test_threshold_reached_locks_ip() {
        let limiter = RateLimiter::new();
        for _ in 0..5 {
            assert!(limiter.record_failure("192.168.1.1"));
        }
        assert!(limiter.is_locked("192.168.1.1").is_some());
        assert_eq!(limiter.get_failure_count("192.168.1.1"), 5);
        assert_eq!(limiter.get_remaining_attempts("192.168.1.1"), 0);
    }

    #[test]
    fn test_reset_clears_record() {
        let limiter = RateLimiter::new();
        for _ in 0..3 {
            limiter.record_failure("192.168.1.1");
        }
        assert_eq!(limiter.get_failure_count("192.168.1.1"), 3);

        limiter.reset("192.168.1.1");
        assert_eq!(limiter.get_failure_count("192.168.1.1"), 0);
        assert!(limiter.is_locked("192.168.1.1").is_none());
    }

    #[test]
    fn test_different_ips_independent() {
        let limiter = RateLimiter::new();

        for _ in 0..5 {
            limiter.record_failure("192.168.1.1");
        }
        limiter.record_failure("192.168.1.2");

        assert!(limiter.is_locked("192.168.1.1").is_some());
        assert!(limiter.is_locked("192.168.1.2").is_none());
        assert_eq!(limiter.get_failure_count("192.168.1.2"), 1);
    }

    #[test]
    fn test_lockout_duration() {
        let limiter = RateLimiter::new();
        for _ in 0..5 {
            limiter.record_failure("192.168.1.1");
        }

        let remaining = limiter.is_locked("192.168.1.1").unwrap();
        // Should be approximately 15 minutes (900 seconds)
        assert!(remaining > 0);
        assert!(remaining <= LOCKOUT_DURATION);
    }

    #[test]
    fn test_active_records_count() {
        let limiter = RateLimiter::new();
        assert_eq!(limiter.active_records_count(), 0);

        limiter.record_failure("192.168.1.1");
        assert_eq!(limiter.active_records_count(), 1);

        limiter.record_failure("192.168.1.2");
        assert_eq!(limiter.active_records_count(), 2);

        limiter.reset("192.168.1.1");
        assert_eq!(limiter.active_records_count(), 1);
    }

    #[test]
    fn test_clear_all() {
        let limiter = RateLimiter::new();
        limiter.record_failure("192.168.1.1");
        limiter.record_failure("192.168.1.2");
        assert_eq!(limiter.active_records_count(), 2);

        limiter.clear_all();
        assert_eq!(limiter.active_records_count(), 0);
    }

    #[test]
    fn test_is_at_capacity() {
        let limiter = RateLimiter::new();
        assert!(!limiter.is_at_capacity());

        // We can't easily test MAX_RECORDS without creating that many entries,
        // but we can verify the method works
        assert_eq!(limiter.active_records_count(), 0);
    }
}
