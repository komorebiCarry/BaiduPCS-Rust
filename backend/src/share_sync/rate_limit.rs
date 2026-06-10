//! share-sync 全局风控限速器
//!
//! ## 角色
//!
//! 阶段 5 并行 worker 把 share_sync 的 transfer 调用从串行 ~5s/文件 提到 4 路并行。
//! 但 4 路并发对账号级风控(百度 errno=132)是激进的——风控以**账号**为维度,
//! 单订阅再低也救不了多订阅同时跑。所以引入**全局** leaky bucket:
//! - 任何 transfer/download 调用前 acquire().await 通过限速门
//! - 4 RPS, burst=8: 平均每秒 4 个请求, 突发能爆 8 个再排队
//! - cancel-safe: tokio task 被 abort 不会留下脏状态(governor 默认行为)
//!
//! ## 不限速器场景
//!
//! 该限速**仅作用于** share_sync 经 `ProductionHooks::submit_transfer_batch` /
//! `submit_download_batch` 入站的调用。直接用 TransferManager(autobackup / 手动
//! 转存) 不走这里 — 因为那些场景天然就是用户驱动的人控速率, 加全局限速反而
//! 拖累交互。
//!
//! ## 调参依据
//!
//! 4 RPS / burst=8 是阶段 7 A/B 前的保守起点:
//! - 单文件 share-direct 路径里, 一次转存 ≈ 1 次 /share/transfer + 1 次 /share/taskquery
//!   + 1 次自动下载触发, 即 ~3 个百度请求
//! - 4 RPS × 3 请求 ≈ 12 RPS 实际打到百度域, 这与百度对 web 端 cookie 客户端的
//!   宽松上限相比仍有安全距离(经验值 ~30 RPS 才触发 132)
//! - burst 8 让"短时刚启动 4 worker 同时 submit"不至于立即被截
//!
//! 阶段 7 metric(errno=132 频次)若仍偏高, 调到 2 RPS / burst=4。

use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use std::num::NonZeroU32;
use std::sync::Arc;

/// 默认每秒令牌补充数 — 阶段 7 A/B 前的保守起点
pub const DEFAULT_REPLENISH_PER_SEC: u32 = 4;
/// 默认 burst 上限(瞬时可消耗的令牌数), 平滑"4 个 worker 同时起步"的冲击
pub const DEFAULT_BURST: u32 = 8;

/// share-sync 全局限速器
///
/// 用 `Arc<QuotaLimiter>` 包到 ShareSyncManager / ProductionHooks 里, 所有 share-sync
/// 起的 transfer/download 调用都先 `acquire().await`。
pub struct QuotaLimiter {
    inner: RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>,
    /// 是否启用 — false 时 acquire 立即返回, 退化为无限速
    enabled: bool,
}

impl QuotaLimiter {
    /// 用默认参数构造(默认开启)
    pub fn new_default() -> Arc<Self> {
        Self::new(
            DEFAULT_REPLENISH_PER_SEC,
            DEFAULT_BURST,
            /* enabled */ true,
        )
    }

    /// 自定义参数构造
    ///
    /// - `replenish_per_sec`: 每秒补充的令牌数(平均请求速率上限)
    /// - `burst`: 桶容量(瞬时可消耗的最大令牌数)
    /// - `enabled`: false 时 acquire 不阻塞, 用于关闭限速做 A/B 对照
    pub fn new(replenish_per_sec: u32, burst: u32, enabled: bool) -> Arc<Self> {
        let replenish = NonZeroU32::new(replenish_per_sec.max(1)).unwrap();
        let burst_nz = NonZeroU32::new(burst.max(replenish_per_sec).max(1)).unwrap();
        let quota = Quota::per_second(replenish).allow_burst(burst_nz);
        Arc::new(Self {
            inner: RateLimiter::direct(quota),
            enabled,
        })
    }

    /// 从 env 读取参数构造:
    /// - `BAIDUPCS_RATE_LIMIT_ENABLED`(默认 "1"): "0"/"false" 关闭限速
    /// - `BAIDUPCS_RATE_LIMIT_RPS`(默认 4): 每秒令牌补充数
    /// - `BAIDUPCS_RATE_LIMIT_BURST`(默认 8): burst 上限
    pub fn from_env() -> Arc<Self> {
        let enabled = std::env::var("BAIDUPCS_RATE_LIMIT_ENABLED")
            .ok()
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);
        let rps: u32 = std::env::var("BAIDUPCS_RATE_LIMIT_RPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_REPLENISH_PER_SEC);
        let burst: u32 = std::env::var("BAIDUPCS_RATE_LIMIT_BURST")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BURST);
        Self::new(rps, burst, enabled)
    }

    /// 等待 1 个令牌; 限速关闭时立即返回。
    ///
    /// **cancel-safe**: governor 的 `until_ready` 内部用单调时钟做 sleep_until,
    /// task 被 abort 不会留脏状态。
    pub async fn acquire(&self) {
        if !self.enabled {
            return;
        }
        self.inner.until_ready().await;
    }
}

impl std::fmt::Debug for QuotaLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaLimiter")
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// 限速关闭时 acquire 不阻塞 — 用于 A/B 对照场景
    #[tokio::test]
    async fn test_disabled_returns_immediately() {
        let lim = QuotaLimiter::new(1, 1, false);
        let t0 = Instant::now();
        for _ in 0..10 {
            lim.acquire().await;
        }
        // 10 个 acquire 不应该被限速(关闭),应远小于 1 秒
        assert!(t0.elapsed().as_millis() < 100, "disabled 应当不阻塞");
    }

    /// 开启限速时, burst 内的 acquire 立即通过, 超出 burst 的要等到下个令牌
    #[tokio::test]
    async fn test_burst_then_throttle() {
        // 4 RPS, burst=4 — 头 4 个立即过, 第 5 个要等 ~250ms
        let lim = QuotaLimiter::new(4, 4, true);
        // burst 内 4 个
        let t0 = Instant::now();
        for _ in 0..4 {
            lim.acquire().await;
        }
        let burst_elapsed = t0.elapsed();
        assert!(
            burst_elapsed.as_millis() < 50,
            "burst 内 4 个应立即过, 实测 {}ms",
            burst_elapsed.as_millis()
        );
        // 第 5 个要等令牌补充 — 250ms 是 4 RPS 的周期
        let t1 = Instant::now();
        lim.acquire().await;
        let throttle_elapsed = t1.elapsed();
        assert!(
            throttle_elapsed.as_millis() >= 200,
            "第 5 个应等 ~250ms 拿到下个令牌, 实测 {}ms",
            throttle_elapsed.as_millis()
        );
    }

    /// from_env 在不设环境变量时走默认值
    #[test]
    fn test_from_env_defaults() {
        // 主测线程会污染 env, 这里只验证构造不 panic;具体值在 default 测试覆盖
        std::env::remove_var("BAIDUPCS_RATE_LIMIT_ENABLED");
        std::env::remove_var("BAIDUPCS_RATE_LIMIT_RPS");
        std::env::remove_var("BAIDUPCS_RATE_LIMIT_BURST");
        let lim = QuotaLimiter::from_env();
        assert!(lim.enabled);
    }

    /// new_default 走默认参数
    #[test]
    fn test_new_default_enabled() {
        let lim = QuotaLimiter::new_default();
        assert!(lim.enabled);
    }
}
