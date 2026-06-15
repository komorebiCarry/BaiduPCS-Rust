//! 带优先级的信号量
//!
//! `tokio::sync::Semaphore` 内部 FIFO，无法表达"P0 插队"语义。
//! 本模块提供一个简化版优先级信号量，支持：
//!
//! - `P0` 高优先级：进入等待队列**头部**（队首插队，但不抢占已持有 permit）
//! - `P1` 普通优先级：进入等待队列**尾部**，FIFO
//! - `add_permits(n)`：增加额度（上调 vip_cap 时使用）
//! - `forget_permits(n)`：祖父化销毁额度（下调 vip_cap 时使用）
//!
//! ## 不变式
//!
//! - 已持有 permit **不会被抢占**；只影响下一个释放的 permit 谁拿到
//! - `forget_permits(n)` 不主动撤销已持有 permit；返还时静默销毁直到 `forgotten` 归零
//! - 同优先级内严格 FIFO
//!
//! ## 取消安全性
//!
//! `acquire().await` 是 cancellation-safe 的：waiter future 在 `rx.await` 任何
//! 时刻被 drop（async 取消、runtime shutdown）都不会泄露 permit。
//!
//! 实现机制：waiter 通过 oneshot 接收一个 `WakeToken`；token 的 Drop 会把 permit
//! 归还回 inner（抵消 forgotten 或唤醒下一个等待者或加回 available）。当 acquire
//! 成功构造 `PriorityPermit` 时调用 `token.consume()` 解除 token 归还职责
//! （由 PriorityPermit 接管）。

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// 等待请求的优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// 高优先级（队头插队，未超 `base_i` 的请求用此优先级）
    P0,
    /// 普通优先级（队尾 FIFO，借调请求用此优先级）
    P1,
}

/// 唤醒 token：waiter 通过 oneshot 接收。
///
/// **取消安全性**：如果 oneshot 已发送但 waiter future 在收到前被 drop，
/// 或者收到后未能构造 PriorityPermit 就被 drop，token 的 Drop 会把 permit
/// 归还（抵消 forgotten / 唤醒下一个等待者 / 加回 available）。waiter 成功
/// 构造 permit 时调 `token.consume()` 解除归还职责。
struct WakeToken {
    inner: Arc<Mutex<Inner>>,
    armed: bool,
}

impl WakeToken {
    fn new(inner: Arc<Mutex<Inner>>) -> Self {
        Self { inner, armed: true }
    }

    /// 解除 Drop 时的归还动作（PriorityPermit 接管 permit；或者 send 失败时
    /// 调用方需要保留 permit）
    fn consume(mut self) {
        self.armed = false;
    }
}

impl Drop for WakeToken {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // permit 归还：与 PriorityPermit::drop 完全一致逻辑
        let inner = Arc::clone(&self.inner);
        if let Ok(mut g) = inner.try_lock() {
            release_one(&mut g, &inner);
            return;
        }
        // 持锁中：spawn 一个轻量任务异步归还。
        // 防御：`tokio::spawn` 在 runtime 外会 panic。若不在 runtime 中则不 spawn
        // （正常路径不会到这里——锁被占用意味着有其它任务在跑，即处于 runtime 内；
        // 在 runtime 外 drop 时 `try_lock` 必然成功，走上面的同步归还分支）。
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let mut g = inner.lock().await;
                release_one(&mut g, &inner);
            });
        } else {
            tracing::warn!(
                "WakeToken 在 tokio runtime 外 drop 且 permit 锁被占用，permit 归还被跳过"
            );
        }
    }
}

/// 等待者
struct Waiter {
    /// 当 permit 可用时通过 oneshot 发送 WakeToken 转交所有权
    sender: oneshot::Sender<WakeToken>,
}

/// 内部状态
struct Inner {
    /// 当前可用 permit 数
    available: usize,
    /// 待销毁的 permit 数（祖父化下调时累积，归还时优先抵消）
    forgotten: usize,
    /// P0 队列（队头插队语义）
    p0_waiters: VecDeque<Waiter>,
    /// P1 队列（普通 FIFO）
    p1_waiters: VecDeque<Waiter>,
}

impl Inner {
    /// 尝试唤醒一个等待者，转交一个 permit。
    ///
    /// **语义**：调用方"持有"一个待转交的 permit。
    /// - 返回 `true`：permit 已通过 WakeToken 成功转交给某个 waiter。
    /// - 返回 `false`：所有等待者都已取消（或队列为空），permit 仍归调用方处置。
    ///
    /// **取消安全**：如果 send 失败（waiter rx 已 drop），WakeToken 会通过
    /// `consume()` 解除 Drop 归还动作，避免 permit 被错误地双重释放。
    /// 如果 send 成功但 waiter 收到 token 后被 drop（async 取消），WakeToken::Drop
    /// 自动归还 permit，由 `release_one` 路径再次尝试唤醒下一个等待者。
    fn try_wake_one_with_inner(&mut self, inner_arc: &Arc<Mutex<Inner>>) -> bool {
        // 优先唤醒 P0 队首
        while let Some(w) = self.p0_waiters.pop_front() {
            let token = WakeToken::new(Arc::clone(inner_arc));
            match w.sender.send(token) {
                Ok(()) => return true,
                Err(returned_token) => {
                    // send 失败：waiter rx 已 drop，permit 还在调用方手里
                    // 解除 Drop 归还（避免与调用方语义冲突 / 双重释放）
                    returned_token.consume();
                    // 继续找下一个等待者
                }
            }
        }
        while let Some(w) = self.p1_waiters.pop_front() {
            let token = WakeToken::new(Arc::clone(inner_arc));
            match w.sender.send(token) {
                Ok(()) => return true,
                Err(returned_token) => {
                    returned_token.consume();
                }
            }
        }
        false
    }
}

/// 带优先级的信号量
pub struct PrioritySemaphore {
    inner: Arc<Mutex<Inner>>,
}

impl PrioritySemaphore {
    /// 创建一个容量为 `permits` 的优先级信号量
    pub fn new(permits: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                available: permits,
                forgotten: 0,
                p0_waiters: VecDeque::new(),
                p1_waiters: VecDeque::new(),
            })),
        }
    }

    /// 当前可用 permit 数（用于测试与 snapshot）
    pub async fn available_permits(&self) -> usize {
        self.inner.lock().await.available
    }

    /// 当前等待者数（用于测试）
    #[cfg(test)]
    pub async fn waiters_count(&self) -> (usize, usize) {
        let g = self.inner.lock().await;
        (g.p0_waiters.len(), g.p1_waiters.len())
    }

    /// 申请一个 permit，按优先级排队
    ///
    /// 返回 `PriorityPermit` RAII guard，drop 时自动归还。
    ///
    /// **取消安全**：如果 future 在等待期间被 drop，
    /// 或唤醒后还未构造 PriorityPermit 就被 drop，permit 会通过 WakeToken::Drop
    /// 自动归还，不会泄露。
    pub async fn acquire(&self, priority: Priority) -> PriorityPermit {
        // Fast path: 立即可得（且 P0 队列为空时 P1 也允许走 fast path）
        let rx = {
            let mut g = self.inner.lock().await;
            // P0 总是可以走 fast path（如果 available > 0）
            // P1 只有在 P0 队列为空时才能走 fast path（避免 P0 饥饿）
            let can_fast = g.available > 0
                && (priority == Priority::P0 || g.p0_waiters.is_empty());
            if can_fast {
                g.available -= 1;
                return PriorityPermit {
                    inner: Arc::clone(&self.inner),
                    consumed: false,
                };
            }
            // 否则排队
            let (tx, rx) = oneshot::channel::<WakeToken>();
            let waiter = Waiter { sender: tx };
            match priority {
                Priority::P0 => g.p0_waiters.push_back(waiter),
                Priority::P1 => g.p1_waiters.push_back(waiter),
            }
            rx
        };

        // 等待唤醒
        // 取消安全：如果 future 在此 await 处被 drop，rx drop 会让发送者
        // send 失败 → WakeToken 在 send 内部被 drop → permit 归还（见
        // try_wake_one_with_inner 内部）。
        let token = match rx.await {
            Ok(t) => t,
            Err(_) => {
                // 信号量被关闭（理论不会发生）。返回一个 dummy permit；
                // 调用方应保证 inner 生命周期足够长。
                // 这里走不到正常代码路径，构造一个不持有 permit 的 dummy。
                return PriorityPermit {
                    inner: Arc::clone(&self.inner),
                    consumed: true, // drop 时不归还
                };
            }
        };
        // 收到 token：解除 token 的归还职责，由 PriorityPermit 接管
        token.consume();
        PriorityPermit {
            inner: Arc::clone(&self.inner),
            consumed: false,
        }
    }

    /// 增加 permit 额度（上调 vip_cap 时使用）
    ///
    /// 上调时先抵消 `forgotten` 债务，剩余才唤醒
    /// 等待者 / 加入 available。否则 forgotten 还挂着，旧 permit 释放仍会被
    /// 抵消，但 add_permits 中间窗口已经把 sem 拉高 → 突破新 cap。
    ///
    /// 对称于 `forget_permits` 的 "available 抵消优先 + 剩余记入 forgotten"。
    /// 优先唤醒等待者，剩余加入 `available`。
    pub async fn add_permits(&self, n: usize) {
        let mut g = self.inner.lock().await;
        let mut remaining = n;

        // 第 1 步：先抵消 forgotten 债务（取消祖父化销毁意图）
        if g.forgotten > 0 && remaining > 0 {
            let take = remaining.min(g.forgotten);
            g.forgotten -= take;
            remaining -= take;
        }

        // 第 2 步：剩余优先唤醒等待者
        while remaining > 0 {
            if g.try_wake_one_with_inner(&self.inner) {
                // 唤醒一个等待者：相当于把这个 permit 直接转交，available 不变
                remaining -= 1;
            } else {
                g.available += remaining;
                remaining = 0;
            }
        }
    }

    /// 祖父化销毁 permit 额度（下调 vip_cap 时使用）
    ///
    /// 优先从 `available` 扣除，剩余记入 `forgotten`，待 permit 归还时静默销毁
    pub async fn forget_permits(&self, n: usize) {
        let mut g = self.inner.lock().await;
        let take = n.min(g.available);
        g.available -= take;
        g.forgotten += n - take;
    }
}

/// RAII guard，drop 时归还 permit
pub struct PriorityPermit {
    inner: Arc<Mutex<Inner>>,
    /// 调用 `forget()` 后置 true，drop 时不归还
    consumed: bool,
}

impl PriorityPermit {
    /// 永久消耗此 permit（不归还、也不计入 `forgotten`）
    ///
    /// 仅用于特殊场景（如对应 tokio `SemaphorePermit::forget`）
    #[allow(dead_code)]
    pub fn forget(mut self) {
        self.consumed = true;
    }
}

impl Drop for PriorityPermit {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        let inner = Arc::clone(&self.inner);
        // drop 时不能 .await。用 try_lock；失败时 fallback 到 spawn 异步归还。
        if let Ok(mut g) = inner.try_lock() {
            release_one(&mut g, &inner);
            return;
        }
        // 持锁中：spawn 一个轻量任务异步归还。
        // 防御：`tokio::spawn` 在 runtime 外会 panic。若不在 runtime 中则不 spawn
        // （正常路径不会到这里——锁被占用意味着有其它任务在跑，即处于 runtime 内；
        // 在 runtime 外 drop 时 `try_lock` 必然成功，走上面的同步归还分支）。
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let mut g = inner.lock().await;
                release_one(&mut g, &inner);
            });
        } else {
            tracing::warn!(
                "PriorityPermit 在 tokio runtime 外 drop 且 permit 锁被占用，permit 归还被跳过"
            );
        }
    }
}

fn release_one(g: &mut Inner, inner_arc: &Arc<Mutex<Inner>>) {
    // 优先抵消 forgotten
    if g.forgotten > 0 {
        g.forgotten -= 1;
        return;
    }
    // 唤醒等待者；唤醒成功则 permit 直接转交
    if g.try_wake_one_with_inner(inner_arc) {
        return;
    }
    // 否则归还到 available
    g.available += 1;
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_acquire_release() {
        let sem = PrioritySemaphore::new(2);
        assert_eq!(sem.available_permits().await, 2);

        let p1 = sem.acquire(Priority::P1).await;
        assert_eq!(sem.available_permits().await, 1);
        let p2 = sem.acquire(Priority::P1).await;
        assert_eq!(sem.available_permits().await, 0);

        drop(p1);
        // drop 后异步归还，给少量时间
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sem.available_permits().await, 1);

        drop(p2);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sem.available_permits().await, 2);
    }

    /// 验收：先 P1 再 P0，P0 必须先获得 permit
    #[tokio::test]
    async fn test_p0_overtakes_p1() {
        let sem = Arc::new(PrioritySemaphore::new(1));

        // 先占满
        let _hold = sem.acquire(Priority::P1).await;

        // P1 等待者先到，hold permit 直到收到 release 信号
        let sem_p1 = Arc::clone(&sem);
        let (p1_acquired_tx, p1_acquired_rx) = oneshot::channel();
        let (p1_release_tx, p1_release_rx) = oneshot::channel::<()>();
        let p1_handle = tokio::spawn(async move {
            let _p = sem_p1.acquire(Priority::P1).await;
            let _ = p1_acquired_tx.send(());
            let _ = p1_release_rx.await;
        });

        // 让 P1 排进去
        tokio::time::sleep(Duration::from_millis(20)).await;

        // P0 等待者后到，同样 hold permit
        let sem_p0 = Arc::clone(&sem);
        let (p0_acquired_tx, p0_acquired_rx) = oneshot::channel();
        let (p0_release_tx, p0_release_rx) = oneshot::channel::<()>();
        let p0_handle = tokio::spawn(async move {
            let _p = sem_p0.acquire(Priority::P0).await;
            let _ = p0_acquired_tx.send(());
            let _ = p0_release_rx.await;
        });

        // 让 P0 也排进去
        tokio::time::sleep(Duration::from_millis(20)).await;

        // 此时队列应为：P0=[p0_waiter], P1=[p1_waiter]
        let (p0c, p1c) = sem.waiters_count().await;
        assert_eq!(p0c, 1, "P0 队列应有 1 个等待者");
        assert_eq!(p1c, 1, "P1 队列应有 1 个等待者");

        // 释放一个 permit
        drop(_hold);

        // P0 必须先收到（在 100ms 内）
        let p0_result =
            tokio::time::timeout(Duration::from_millis(100), p0_acquired_rx).await;
        assert!(
            p0_result.is_ok() && p0_result.unwrap().is_ok(),
            "P0 应先获得 permit"
        );

        // P1 此时仍在等待（P0 在 hold permit）
        let mut p1_acquired_rx = Box::pin(p1_acquired_rx);
        let p1_result =
            tokio::time::timeout(Duration::from_millis(50), p1_acquired_rx.as_mut()).await;
        assert!(p1_result.is_err(), "P0 hold 中，P1 应仍在等待");

        // 释放 P0 → P1 应立即拿到
        let _ = p0_release_tx.send(());
        let p1_result =
            tokio::time::timeout(Duration::from_millis(200), p1_acquired_rx.as_mut()).await;
        assert!(
            p1_result.is_ok() && p1_result.unwrap().is_ok(),
            "P0 释放后 P1 应被唤醒"
        );

        // 清理
        let _ = p1_release_tx.send(());
        let _ = p0_handle.await;
        let _ = p1_handle.await;
    }

    #[tokio::test]
    async fn test_add_permits_wakes_waiter() {
        let sem = Arc::new(PrioritySemaphore::new(0));
        let sem_clone = Arc::clone(&sem);
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let _p = sem_clone.acquire(Priority::P1).await;
            let _ = tx.send(());
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        // 应仍在等待
        assert_eq!(sem.waiters_count().await, (0, 1));

        sem.add_permits(1).await;

        let r = tokio::time::timeout(Duration::from_millis(100), rx).await;
        assert!(r.is_ok() && r.unwrap().is_ok(), "add_permits 后等待者应被唤醒");
    }

    /// 验收基础：forget_permits 下调容量后，新 acquire 阻塞，已持有不中断
    #[tokio::test]
    async fn test_forget_permits_grandfather() {
        let sem = Arc::new(PrioritySemaphore::new(3));

        // 占用 2 个
        let h1 = sem.acquire(Priority::P1).await;
        let h2 = sem.acquire(Priority::P1).await;
        assert_eq!(sem.available_permits().await, 1);

        // 销毁 2 个额度：available=1 抵消，剩 1 入 forgotten
        sem.forget_permits(2).await;
        assert_eq!(sem.available_permits().await, 0);

        // 已持有的 h1/h2 不受影响（不会 panic、不会 cancel）

        // 新 acquire 必须阻塞（因为 available=0）
        let sem_clone = Arc::clone(&sem);
        let acquire_handle = tokio::spawn(async move {
            sem_clone.acquire(Priority::P1).await
        });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(sem.waiters_count().await, (0, 1), "新 acquire 应被阻塞");

        // 归还 h1：被 forgotten 静默吸收（forgotten 1 → 0）
        drop(h1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(sem.available_permits().await, 0);
        assert_eq!(sem.waiters_count().await, (0, 1), "h1 归还被 forgotten 吸收，等待者仍阻塞");

        // 再归还 h2：forgotten 已为 0，唤醒等待者
        drop(h2);
        let r = tokio::time::timeout(Duration::from_millis(100), acquire_handle).await;
        assert!(r.is_ok(), "h2 归还后等待者应被唤醒");
    }

    #[tokio::test]
    async fn test_forget_more_than_available() {
        let sem = PrioritySemaphore::new(2);
        sem.forget_permits(5).await;
        assert_eq!(sem.available_permits().await, 0);
        // 5 - 2 = 3 个进了 forgotten，但当前没有 permit 在外，
        // 所以 forgotten 会一直挂着直到未来某次归还（这里没有归还）
    }

    /// 全局级 PrioritySemaphore 回归测试：
    /// 下调产生 forgotten 后未释放再上调，add_permits 必须先抵消 forgotten。
    ///
    /// 场景：global cap=10，acquire 8 → forget 5（available 全消耗，forgotten=3）→
    /// 旧 permit 未释放就 add_permits(5)。
    ///
    /// 修复前：add_permits(5) 直接进 available 或唤醒等待者；旧 8 释放仍被
    /// forgotten=3 抵消（销毁 3 个），但中间窗口 sem 已被拉高 → 突破 cap。
    ///
    /// 修复后：add_permits 先消耗 forgotten=3 → forgotten 归零；剩 (5-3)=2 真正
    /// 加 available。
    #[tokio::test]
    async fn test_add_permits_consumes_forgotten() {
        let sem = Arc::new(PrioritySemaphore::new(10));

        // acquire 8 个
        let mut held: Vec<PriorityPermit> = Vec::new();
        for _ in 0..8 {
            let p = sem.acquire(Priority::P1).await;
            held.push(p);
        }
        assert_eq!(sem.available_permits().await, 2);

        // forget 5 → available 2 被消耗光，forgotten=3
        sem.forget_permits(5).await;
        assert_eq!(sem.available_permits().await, 0);
        {
            let g = sem.inner.lock().await;
            assert_eq!(g.forgotten, 3, "forgotten=3");
        }

        // 旧 permit 未释放就 add_permits(5)
        sem.add_permits(5).await;

        // 验收：forgotten 必须先被 5 抵消 3 个 → forgotten=0；剩 (5-3)=2 真正加到 available
        {
            let g = sem.inner.lock().await;
            assert_eq!(g.forgotten, 0, "add_permits 优先抵消 forgotten");
            assert_eq!(g.available, 2, "available=5-3=2（不是 5）");
        }

        // 释放全部 8 个旧 permit
        held.clear();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // forgotten=0 + 8 个 release → available=2+8=10（与初始 cap 一致）
        {
            let g = sem.inner.lock().await;
            assert_eq!(g.forgotten, 0);
            assert_eq!(g.available, 10, "全部归还后 available=10（与原 cap 一致）");
        }
    }

    #[tokio::test]
    async fn test_p0_no_starvation_on_p1_waiters() {
        // P0 fast path：即使有 P1 在排队，P0 也可以直接走 fast path
        // 但本实现要求 fast path 仅在 available > 0 时；P1 排队意味着 available=0
        // 所以这个测试主要验证：当 available 重新可用时，P0 优先于 P1
        let sem = Arc::new(PrioritySemaphore::new(1));
        let _hold = sem.acquire(Priority::P1).await;

        // 5 个 P1 排队
        let mut p1_handles = vec![];
        for _ in 0..5 {
            let s = Arc::clone(&sem);
            p1_handles.push(tokio::spawn(async move {
                let _p = s.acquire(Priority::P1).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(sem.waiters_count().await.1, 5);

        // 1 个 P0 排队
        let sem_p0 = Arc::clone(&sem);
        let (p0_tx, p0_rx) = oneshot::channel();
        let p0_handle = tokio::spawn(async move {
            let _p = sem_p0.acquire(Priority::P0).await;
            let _ = p0_tx.send(());
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(sem.waiters_count().await, (1, 5));

        // 释放 hold → P0 应先拿到（队头插队）
        drop(_hold);
        let r = tokio::time::timeout(Duration::from_millis(100), p0_rx).await;
        assert!(r.is_ok() && r.unwrap().is_ok(), "P0 应在 P1 之前获得 permit");

        // 清理
        for h in p1_handles {
            h.abort();
        }
        let _ = p0_handle.await;
    }

    /// 取消安全性回归测试：
    ///
    /// 全局满时排队一个 waiter，释放 permit 唤醒 waiter；waiter 在收到
    /// WakeToken 之前/之后 drop future（通过 abort），permit 必须最终归还，
    /// 不能泄露。旧实现用 `oneshot::Sender<()>`：send 成功即认为转交，
    /// waiter abort 后 permit 不会回到 available。
    #[tokio::test]
    async fn test_acquire_cancellation_safety() {
        let sem = Arc::new(PrioritySemaphore::new(1));

        // 占满
        let hold = sem.acquire(Priority::P1).await;
        assert_eq!(sem.available_permits().await, 0);

        // 排一个 P1 waiter
        let sem_clone = Arc::clone(&sem);
        let waiter_handle = tokio::spawn(async move {
            // 这个 future 会被 abort
            let _p = sem_clone.acquire(Priority::P1).await;
            // 持有 permit 一段时间（如果走到这里，permit 应该被自动归还）
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        // 等 waiter 进入队列
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(sem.waiters_count().await, (0, 1));

        // 立即 abort waiter（在 hold 释放之前）
        waiter_handle.abort();
        let _ = waiter_handle.await; // 等待 abort 完成

        // 释放 hold → 应该唤醒队列里的 waiter，但 waiter 已 abort
        // → WakeToken 由 send Ok 后被 dropped，permit 应回到 available
        drop(hold);

        // 给异步归还时间
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 关键断言：available 必须回到 1（permit 没泄露）
        assert_eq!(
            sem.available_permits().await,
            1,
            "waiter abort 后 permit 必须最终归还"
        );

        // 验证可正常 acquire
        let new_permit = tokio::time::timeout(
            Duration::from_millis(100),
            sem.acquire(Priority::P1),
        )
            .await;
        assert!(new_permit.is_ok(), "acquire 在 cancel 之后应能正常成功");
    }

    /// 边界场景：waiter 在 `rx.await` 之前 drop（rx 被 drop）→ send 失败路径
    #[tokio::test]
    async fn test_acquire_cancellation_before_send() {
        let sem = Arc::new(PrioritySemaphore::new(1));
        let hold = sem.acquire(Priority::P1).await;

        // 排一个 waiter
        let sem_clone = Arc::clone(&sem);
        let waiter_handle = tokio::spawn(async move {
            let _p = sem_clone.acquire(Priority::P1).await;
            std::future::pending::<()>().await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(sem.waiters_count().await, (0, 1));

        // abort waiter
        waiter_handle.abort();
        let _ = waiter_handle.await;

        // 此时 waiter 队列里仍有那个已死的 sender；释放 hold 时 try_wake_one
        // 会发现 send 失败（rx 已 drop），尝试唤醒下一个；队列空了 → 归还 available。
        drop(hold);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            sem.available_permits().await,
            1,
            "send 失败路径下 permit 必须归还到 available"
        );
    }
}
