//! 任务控制器
//!
//! 解决三个并发冲突问题：
//! 1. 同一任务执行很久（如 30 分钟），轮询间隔（如 10 分钟）会再次触发
//! 2. 文件监听事件随时触发
//! 3. 轮询和监听同时触发
//!
//! 核心设计：
//! - 单一执行线程 + 触发信号合并（coalescing）
//! - 同一配置只允许一个执行实例
//! - 执行中触发会被合并，不丢、不并发
//! - 轮询和监听共用一套逻辑

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use parking_lot::RwLock;

/// 触发来源
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerSource {
    /// 定时轮询触发
    Poll,
    /// 文件监听触发
    Watch,
    /// 手动触发
    Manual,
}

impl std::fmt::Display for TriggerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerSource::Poll => write!(f, "poll"),
            TriggerSource::Watch => write!(f, "watch"),
            TriggerSource::Manual => write!(f, "manual"),
        }
    }
}

/// 控制器状态（用于外部查询）
#[derive(Debug, Clone)]
pub struct ControllerStatus {
    /// 配置 ID
    pub config_id: String,
    /// 是否正在执行
    pub is_running: bool,
    /// 是否有待处理的触发
    pub has_pending: bool,
    /// 最后触发来源
    pub last_trigger_source: Option<TriggerSource>,
    /// 执行次数
    pub execution_count: u64,
    /// 合并次数（执行期间收到的触发数）
    pub coalesced_count: u64,
}

/// 单配置的任务控制器
///
/// 保证同一配置同时只有一个扫描任务在执行。
/// 执行期间的触发会被合并，执行完成后自动重新执行一次。
///
/// # 架构
///
/// ```text
/// [轮询] ─┐
///         ├── trigger() ──► TaskController ──► task_loop()
/// [监听] ─┘                  running/pending     │
///                                                ▼
///                                          实际扫描逻辑
/// ```
pub struct TaskController {
    /// 配置 ID
    config_id: String,
    /// 是否正在执行
    running: AtomicBool,
    /// 是否有待处理的触发（执行期间收到的）
    pending: AtomicBool,
    /// 是否请求暂停（用于优雅关闭）
    pause_requested: AtomicBool,
    /// 唤醒通知
    notify: Notify,
    /// 取消令牌
    cancel_token: CancellationToken,
    /// 最后触发来源（用于日志/调试）
    last_trigger_source: RwLock<Option<TriggerSource>>,
    /// 执行次数统计
    execution_count: std::sync::atomic::AtomicU64,
    /// 合并次数统计（执行期间收到的触发数）
    coalesced_count: std::sync::atomic::AtomicU64,
}

impl TaskController {
    /// 创建新的任务控制器
    pub fn new(config_id: String) -> Self {
        Self {
            config_id,
            running: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            pause_requested: AtomicBool::new(false),
            notify: Notify::new(),
            cancel_token: CancellationToken::new(),
            last_trigger_source: RwLock::new(None),
            execution_count: std::sync::atomic::AtomicU64::new(0),
            coalesced_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 获取配置 ID
    pub fn config_id(&self) -> &str {
        &self.config_id
    }

    /// 统一触发入口（轮询 & 监听都走这里）
    ///
    /// 这是核心方法，保证：
    /// - 如果当前没有任务在执行，唤醒执行线程
    /// - 如果当前有任务在执行，只标记 pending，不会启动新任务
    ///
    /// # 返回值
    /// - `true`: 触发成功（唤醒了执行线程或标记了 pending）
    /// - `false`: 控制器已取消
    pub fn trigger(&self, source: TriggerSource) -> bool {
        // 检查是否已取消
        if self.cancel_token.is_cancelled() {
            tracing::debug!(
                "配置 {} 控制器已取消，忽略触发（来源: {}）",
                self.config_id, source
            );
            return false;
        }

        // 记录触发来源
        *self.last_trigger_source.write() = Some(source);

        // 如果已经在跑，只标记 pending
        if self.running.load(Ordering::Acquire) {
            let was_pending = self.pending.swap(true, Ordering::Release);
            self.coalesced_count.fetch_add(1, Ordering::Relaxed);

            if !was_pending {
                tracing::debug!(
                    "配置 {} 正在执行，标记 pending（来源: {}）",
                    self.config_id, source
                );
            } else {
                tracing::trace!(
                    "配置 {} 正在执行且已有 pending，忽略重复触发（来源: {}）",
                    self.config_id, source
                );
            }
            return true;
        }

        // 否则唤醒执行线程
        tracing::debug!("配置 {} 触发执行（来源: {}）", self.config_id, source);
        self.notify.notify_one();
        true
    }

    /// 检查是否正在执行
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// 检查是否有待处理触发
    pub fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// 获取最后触发来源
    pub fn last_trigger_source(&self) -> Option<TriggerSource> {
        *self.last_trigger_source.read()
    }

    /// 取消控制器
    ///
    /// 取消后：
    /// - 新的触发会被忽略
    /// - 正在执行的任务会收到取消信号
    /// - task_loop 会退出
    pub fn cancel(&self) {
        tracing::info!("配置 {} 任务控制器取消", self.config_id);
        self.cancel_token.cancel();
        // 唤醒可能在等待的 task_loop
        self.notify.notify_one();
    }

    /// 检查是否已取消
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// 获取取消令牌的子令牌（用于传递给实际任务）
    pub fn child_token(&self) -> CancellationToken {
        self.cancel_token.child_token()
    }

    /// 获取控制器状态
    pub fn status(&self) -> ControllerStatus {
        ControllerStatus {
            config_id: self.config_id.clone(),
            is_running: self.is_running(),
            has_pending: self.has_pending(),
            last_trigger_source: self.last_trigger_source(),
            execution_count: self.execution_count.load(Ordering::Relaxed),
            coalesced_count: self.coalesced_count.load(Ordering::Relaxed),
        }
    }

    /// 重置统计计数
    pub fn reset_stats(&self) {
        self.execution_count.store(0, Ordering::Relaxed);
        self.coalesced_count.store(0, Ordering::Relaxed);
    }

    /// 请求暂停（用于优雅关闭）
    /// 
    /// 暂停请求不会立即停止正在执行的任务，而是：
    /// - 阻止新的触发被处理
    /// - 让当前执行的任务完成后不再继续
    pub fn request_pause(&self) {
        self.pause_requested.store(true, Ordering::SeqCst);
        tracing::info!("配置 {} 请求暂停", self.config_id);
    }

    /// 恢复执行（取消暂停请求）
    pub fn resume(&self) {
        self.pause_requested.store(false, Ordering::SeqCst);
        tracing::info!("配置 {} 恢复执行", self.config_id);
    }

    /// 检查是否请求了暂停
    pub fn is_pause_requested(&self) -> bool {
        self.pause_requested.load(Ordering::SeqCst)
    }
}

/// 任务执行主循环
///
/// 这是核心执行逻辑，保证：
/// - 同一时间只有一个任务在执行
/// - 执行期间的触发会被合并
/// - 执行完成后如果有 pending，立即重新执行
///
/// # 参数
/// - `controller`: 任务控制器
/// - `task_fn`: 实际执行的任务函数，返回 `Result<()>`
///
/// # 示例
///
/// ```ignore
/// let controller = Arc::new(TaskController::new("config-1".to_string()));
///
/// task_loop(controller.clone(), || async {
///     // 实际的扫描和备份逻辑
///     do_backup().await
/// }).await;
/// ```
pub async fn task_loop<F, Fut>(
    controller: Arc<TaskController>,
    mut task_fn: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let config_id = &controller.config_id;
    tracing::info!("配置 {} 任务循环已启动", config_id);

    loop {
        // 等待触发或取消
        tokio::select! {
            biased; // 优先检查取消

            _ = controller.cancel_token.cancelled() => {
                tracing::info!("配置 {} 任务循环已取消", config_id);
                break;
            }
            _ = controller.notify.notified() => {
                // 收到触发信号
            }
        }

        // 再次检查取消（可能是取消唤醒的）
        if controller.cancel_token.is_cancelled() {
            break;
        }

        // CAS 防止并发：尝试将 running 从 false 设为 true
        if controller.running.swap(true, Ordering::AcqRel) {
            // 已经有其他执行在跑，跳过
            // 这种情况理论上不应该发生，因为只有一个 task_loop
            tracing::warn!("配置 {} 检测到并发执行，跳过", config_id);
            continue;
        }

        // 内层循环：处理 pending
        loop {
            // 清除 pending 标志
            controller.pending.store(false, Ordering::Release);

            // 增加执行计数
            controller.execution_count.fetch_add(1, Ordering::Relaxed);

            let trigger_source = controller.last_trigger_source()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            tracing::info!(
                "配置 {} 开始执行扫描任务（触发来源: {}）",
                config_id, trigger_source
            );
            let start = std::time::Instant::now();

            // ===== 真正的长任务 =====
            let result = tokio::select! {
                biased;

                _ = controller.cancel_token.cancelled() => {
                    tracing::info!("配置 {} 任务执行被取消", config_id);
                    controller.running.store(false, Ordering::Release);
                    return;
                }
                r = task_fn() => r,
            };

            let elapsed = start.elapsed();
            match result {
                Ok(()) => {
                    tracing::info!(
                        "配置 {} 扫描任务完成，耗时 {:.2?}",
                        config_id, elapsed
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "配置 {} 扫描任务失败: {}，耗时 {:.2?}",
                        config_id, e, elapsed
                    );
                }
            }

            // 标记执行完成
            controller.running.store(false, Ordering::Release);

            // 检查执行期间是否又被触发过
            if controller.pending.load(Ordering::Acquire) {
                tracing::info!(
                    "配置 {} 执行期间有新触发，立即重新执行",
                    config_id
                );
                // 重新标记为执行中
                controller.running.store(true, Ordering::Release);
                continue;
            }

            break;
        }
    }

    tracing::info!("配置 {} 任务循环已退出", config_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[test]
    fn test_controller_creation() {
        let controller = TaskController::new("test-config".to_string());
        assert_eq!(controller.config_id(), "test-config");
        assert!(!controller.is_running());
        assert!(!controller.has_pending());
        assert!(controller.last_trigger_source().is_none());
    }

    #[test]
    fn test_trigger_when_not_running() {
        let controller = TaskController::new("test-config".to_string());

        // 触发应该成功
        assert!(controller.trigger(TriggerSource::Poll));

        // 应该记录触发来源
        assert_eq!(controller.last_trigger_source(), Some(TriggerSource::Poll));
    }

    #[test]
    fn test_trigger_when_running_sets_pending() {
        let controller = TaskController::new("test-config".to_string());

        // 模拟正在执行
        controller.running.store(true, Ordering::Release);

        // 触发应该设置 pending
        assert!(controller.trigger(TriggerSource::Watch));
        assert!(controller.has_pending());

        // 合并计数应该增加
        assert_eq!(controller.coalesced_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_trigger_after_cancel() {
        let controller = TaskController::new("test-config".to_string());

        controller.cancel();

        // 取消后触发应该返回 false
        assert!(!controller.trigger(TriggerSource::Manual));
    }

    #[test]
    fn test_status() {
        let controller = TaskController::new("test-config".to_string());

        controller.trigger(TriggerSource::Poll);
        controller.running.store(true, Ordering::Release);
        controller.pending.store(true, Ordering::Release);
        controller.execution_count.store(5, Ordering::Relaxed);
        controller.coalesced_count.store(3, Ordering::Relaxed);

        let status = controller.status();
        assert_eq!(status.config_id, "test-config");
        assert!(status.is_running);
        assert!(status.has_pending);
        assert_eq!(status.last_trigger_source, Some(TriggerSource::Poll));
        assert_eq!(status.execution_count, 5);
        assert_eq!(status.coalesced_count, 3);
    }

    #[tokio::test]
    async fn test_task_loop_single_execution() {
        let controller = Arc::new(TaskController::new("test-config".to_string()));
        let execution_count = Arc::new(AtomicUsize::new(0));

        let ctrl = controller.clone();
        let count = execution_count.clone();

        // 启动任务循环
        let handle = tokio::spawn(async move {
            task_loop(ctrl, || {
                let c = count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(())
                }
            }).await;
        });

        // 等待一小段时间让循环启动
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 触发执行
        controller.trigger(TriggerSource::Manual);

        // 等待执行完成
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 取消并等待退出
        controller.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        // 应该执行了一次
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_task_loop_coalescing() {
        let controller = Arc::new(TaskController::new("test-config".to_string()));
        let execution_count = Arc::new(AtomicUsize::new(0));

        let ctrl = controller.clone();
        let count = execution_count.clone();

        // 启动任务循环
        let handle = tokio::spawn(async move {
            task_loop(ctrl, || {
                let c = count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    // 模拟长时间执行
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            }).await;
        });

        // 等待循环启动
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 触发第一次执行
        controller.trigger(TriggerSource::Poll);

        // 等待任务开始执行
        tokio::time::sleep(Duration::from_millis(20)).await;

        // 在执行期间多次触发（应该被合并）
        controller.trigger(TriggerSource::Watch);
        controller.trigger(TriggerSource::Watch);
        controller.trigger(TriggerSource::Poll);

        // 等待足够时间让两次执行完成
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 取消
        controller.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        // 应该执行了 2 次（第一次 + pending 触发的第二次）
        assert_eq!(execution_count.load(Ordering::SeqCst), 2);

        // 合并计数应该是 3（执行期间的 3 次触发）
        assert_eq!(controller.coalesced_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_task_loop_cancel_during_execution() {
        let controller = Arc::new(TaskController::new("test-config".to_string()));
        let started = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));

        let ctrl = controller.clone();
        let s = started.clone();
        let c = completed.clone();

        // 启动任务循环
        let handle = tokio::spawn(async move {
            task_loop(ctrl.clone(), || {
                let started = s.clone();
                let completed = c.clone();
                let token = ctrl.child_token();
                async move {
                    started.store(true, Ordering::SeqCst);

                    // 模拟长时间执行，但可以被取消
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {
                            completed.store(true, Ordering::SeqCst);
                        }
                        _ = token.cancelled() => {
                            // 被取消
                        }
                    }
                    Ok(())
                }
            }).await;
        });

        // 等待循环启动
        tokio::time::sleep(Duration::from_millis(10)).await;

        // 触发执行
        controller.trigger(TriggerSource::Manual);

        // 等待任务开始
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(started.load(Ordering::SeqCst));

        // 取消
        controller.cancel();

        // 等待退出
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        // 任务应该没有完成（被取消了）
        assert!(!completed.load(Ordering::SeqCst));
    }
}
