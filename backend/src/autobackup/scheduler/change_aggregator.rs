//! 变更聚合器
//!
//! 负责聚合文件变更事件，实现防抖和去重

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::autobackup::config::BackupDirection;

/// 默认事件通道容量
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 10000;

/// 背压策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// 阻塞等待（默认）
    Block,
    /// 丢弃最旧的事件
    DropOldest,
    /// 丢弃新事件
    DropNewest,
}

/// 事件发送器（带背压处理）
#[derive(Clone)]
pub struct EventSender {
    /// 内部发送器
    inner: mpsc::Sender<ChangeEvent>,
    /// 背压策略
    strategy: BackpressureStrategy,
    /// 丢弃计数
    dropped_count: std::sync::Arc<AtomicU64>,
}

impl EventSender {
    /// 创建新的事件发送器
    pub fn new(inner: mpsc::Sender<ChangeEvent>, strategy: BackpressureStrategy) -> Self {
        Self {
            inner,
            strategy,
            dropped_count: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    /// 发送事件（根据策略处理背压）
    pub async fn send(&self, event: ChangeEvent) -> Result<(), mpsc::error::SendError<ChangeEvent>> {
        match self.strategy {
            BackpressureStrategy::Block => {
                self.inner.send(event).await
            }
            BackpressureStrategy::DropNewest => {
                match self.inner.try_send(event) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.dropped_count.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("事件通道已满，丢弃新事件（策略: DropNewest）");
                        Ok(()) // 丢弃但不返回错误
                    }
                    Err(mpsc::error::TrySendError::Closed(e)) => {
                        Err(mpsc::error::SendError(e))
                    }
                }
            }
            BackpressureStrategy::DropOldest => {
                // DropOldest 需要在接收端实现，这里退化为 Block
                self.inner.send(event).await
            }
        }
    }

    /// 尝试发送事件（非阻塞）
    pub fn try_send(&self, event: ChangeEvent) -> Result<(), mpsc::error::TrySendError<ChangeEvent>> {
        match self.inner.try_send(event) {
            Ok(()) => Ok(()),
            Err(e) => {
                if matches!(e, mpsc::error::TrySendError::Full(_)) {
                    self.dropped_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// 获取丢弃的事件数量
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// 重置丢弃计数
    pub fn reset_dropped_count(&self) {
        self.dropped_count.store(0, Ordering::Relaxed);
    }

    /// 检查通道是否已关闭
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

/// 创建有界事件通道
pub fn bounded_event_channel(capacity: usize) -> (EventSender, mpsc::Receiver<ChangeEvent>) {
    let (tx, rx) = mpsc::channel(capacity);
    (EventSender::new(tx, BackpressureStrategy::DropNewest), rx)
}

/// 创建带指定策略的有界事件通道
pub fn bounded_event_channel_with_strategy(
    capacity: usize,
    strategy: BackpressureStrategy,
) -> (EventSender, mpsc::Receiver<ChangeEvent>) {
    let (tx, rx) = mpsc::channel(capacity);
    (EventSender::new(tx, strategy), rx)
}

/// 全局轮询类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalPollType {
    /// 间隔轮询
    Interval,
    /// 指定时间轮询
    Scheduled,
}

/// 变更事件
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// 监听事件
    WatchEvent {
        config_id: String,
        paths: Vec<PathBuf>,
    },
    /// 轮询事件（单个配置）
    PollEvent {
        config_id: String,
    },
    /// 全局轮询事件（触发所有匹配方向的配置）
    GlobalPollEvent {
        direction: BackupDirection,
        poll_type: GlobalPollType,
    },
}

/// 待处理事件
struct PendingEvent {
    config_id: String,
    paths: HashSet<PathBuf>,
    /// 是否为轮询事件（预留用于区分事件来源）
    #[allow(dead_code)]
    is_poll: bool,
    first_seen: Instant,
}

/// 变更聚合器
pub struct ChangeAggregator {
    /// 事件接收通道
    event_rx: mpsc::UnboundedReceiver<ChangeEvent>,
    /// 聚合后的事件发送通道
    aggregated_tx: mpsc::UnboundedSender<ChangeEvent>,
    /// 时间窗口（默认 3 秒）
    window_duration: Duration,
    /// 待聚合的事件（按 config_id 分组）
    pending_events: Mutex<HashMap<String, PendingEvent>>,
}

impl ChangeAggregator {
    /// 创建新的变更聚合器
    pub fn new(
        event_rx: mpsc::UnboundedReceiver<ChangeEvent>,
        aggregated_tx: mpsc::UnboundedSender<ChangeEvent>,
        window_duration: Duration,
    ) -> Self {
        Self {
            event_rx,
            aggregated_tx,
            window_duration,
            pending_events: Mutex::new(HashMap::new()),
        }
    }

    /// 使用默认窗口时间创建
    pub fn with_default_window(
        event_rx: mpsc::UnboundedReceiver<ChangeEvent>,
        aggregated_tx: mpsc::UnboundedSender<ChangeEvent>,
    ) -> Self {
        Self::new(event_rx, aggregated_tx, Duration::from_secs(3))
    }

    /// 运行聚合器
    pub async fn run(&mut self) {
        let mut flush_interval = tokio::time::interval(self.window_duration);

        loop {
            tokio::select! {
                // 接收新事件
                event = self.event_rx.recv() => {
                    match event {
                        Some(event) => self.handle_event(event),
                        None => {
                            // 通道关闭，刷新所有待处理事件后退出
                            self.flush_all_pending_events();
                            break;
                        }
                    }
                }
                // 定期刷新
                _ = flush_interval.tick() => {
                    self.flush_pending_events();
                }
            }
        }

        tracing::info!("ChangeAggregator stopped");
    }

    /// 处理事件
    fn handle_event(&self, event: ChangeEvent) {
        match event {
            ChangeEvent::WatchEvent { config_id, paths } => {
                let mut pending_events = self.pending_events.lock();
                let pending = pending_events
                    .entry(config_id.clone())
                    .or_insert_with(|| PendingEvent {
                        config_id: config_id.clone(),
                        paths: HashSet::new(),
                        is_poll: false,
                        first_seen: Instant::now(),
                    });

                // 合并路径（去重）
                for path in paths {
                    pending.paths.insert(path);
                }
            }
            ChangeEvent::PollEvent { config_id } => {
                // 轮询事件优先级更高，直接触发扫描
                // 先刷新该配置的待处理事件
                let pending = {
                    let mut pending_events = self.pending_events.lock();
                    pending_events.remove(&config_id)
                };

                if let Some(p) = pending {
                    self.send_aggregated_event(p);
                }

                // 发送轮询事件
                let poll_event = ChangeEvent::PollEvent { config_id };
                if let Err(e) = self.aggregated_tx.send(poll_event) {
                    tracing::warn!("Failed to send poll event: {}", e);
                }
            }
            ChangeEvent::GlobalPollEvent { direction, poll_type } => {
                // 全局轮询事件直接转发，不聚合
                let global_event = ChangeEvent::GlobalPollEvent { direction, poll_type };
                if let Err(e) = self.aggregated_tx.send(global_event) {
                    tracing::warn!("Failed to send global poll event: {}", e);
                }
            }
        }
    }

    /// 刷新超时的待处理事件
    fn flush_pending_events(&self) {
        let now = Instant::now();
        let to_flush: Vec<PendingEvent> = {
            let mut pending_events = self.pending_events.lock();
            let mut to_flush = Vec::new();

            // 收集需要刷新的事件（超过时间窗口）
            let keys_to_remove: Vec<String> = pending_events
                .iter()
                .filter(|(_, p)| now.duration_since(p.first_seen) >= self.window_duration)
                .map(|(k, _)| k.clone())
                .collect();

            // 移除并收集
            for key in keys_to_remove {
                if let Some(p) = pending_events.remove(&key) {
                    to_flush.push(p);
                }
            }
            to_flush
        };

        // 刷新事件（在锁外执行）
        for pending in to_flush {
            self.send_aggregated_event(pending);
        }
    }

    /// 刷新所有待处理事件
    fn flush_all_pending_events(&self) {
        let to_flush: Vec<PendingEvent> = {
            let mut pending_events = self.pending_events.lock();
            pending_events.drain().map(|(_, v)| v).collect()
        };

        for pending in to_flush {
            self.send_aggregated_event(pending);
        }
    }

    /// 发送聚合后的事件
    fn send_aggregated_event(&self, pending: PendingEvent) {
        if pending.paths.is_empty() {
            return;
        }

        let event = ChangeEvent::WatchEvent {
            config_id: pending.config_id,
            paths: pending.paths.into_iter().collect(),
        };

        if let Err(e) = self.aggregated_tx.send(event) {
            tracing::warn!("Failed to send aggregated event: {}", e);
        }
    }

    /// 获取待处理事件数量
    pub fn pending_count(&self) -> usize {
        self.pending_events.lock().len()
    }

    /// 获取待处理的配置 ID 列表
    pub fn pending_configs(&self) -> Vec<String> {
        self.pending_events.lock().keys().cloned().collect()
    }
}

/// 触发策略 trait
pub trait TriggerPolicy: Send + Sync {
    /// 判断是否应该立即触发
    fn should_trigger_immediately(&self, event: &ChangeEvent) -> bool;

    /// 获取触发延迟
    fn get_trigger_delay(&self, event: &ChangeEvent) -> Duration;

    /// Poll 事件是否应该打断当前 Watch 窗口
    fn poll_interrupts_watch(&self) -> bool;

    /// 策略名称
    fn name(&self) -> &'static str;
}

/// 默认策略
pub struct DefaultTriggerPolicy {
    watch_window: Duration,
}

impl DefaultTriggerPolicy {
    pub fn new(watch_window: Duration) -> Self {
        Self { watch_window }
    }
}

impl Default for DefaultTriggerPolicy {
    fn default() -> Self {
        Self {
            watch_window: Duration::from_secs(3),
        }
    }
}

impl TriggerPolicy for DefaultTriggerPolicy {
    fn should_trigger_immediately(&self, event: &ChangeEvent) -> bool {
        matches!(event, ChangeEvent::PollEvent { .. })
    }

    fn get_trigger_delay(&self, _event: &ChangeEvent) -> Duration {
        self.watch_window
    }

    fn poll_interrupts_watch(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "Default"
    }
}

/// 夜间模式策略
pub struct NightModePolicy {
    trigger_hour: u32,
    trigger_minute: u32,
}

impl NightModePolicy {
    pub fn new(trigger_hour: u32, trigger_minute: u32) -> Self {
        Self {
            trigger_hour,
            trigger_minute,
        }
    }
}

impl TriggerPolicy for NightModePolicy {
    fn should_trigger_immediately(&self, _event: &ChangeEvent) -> bool {
        false
    }

    fn get_trigger_delay(&self, _event: &ChangeEvent) -> Duration {
        // 计算到下一个触发时间的延迟
        let now = chrono::Local::now();
        let target = now
            .date_naive()
            .and_hms_opt(self.trigger_hour, self.trigger_minute, 0)
            .unwrap();

        let target_datetime = if target <= now.naive_local() {
            target + chrono::Duration::days(1)
        } else {
            target
        };

        let duration = target_datetime - now.naive_local();
        Duration::from_secs(duration.num_seconds().max(0) as u64)
    }

    fn poll_interrupts_watch(&self) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "NightMode"
    }
}

/// 低电量模式策略
pub struct LowPowerPolicy {
    min_interval: Duration,
}

impl LowPowerPolicy {
    pub fn new(min_interval: Duration) -> Self {
        Self { min_interval }
    }
}

impl TriggerPolicy for LowPowerPolicy {
    fn should_trigger_immediately(&self, _event: &ChangeEvent) -> bool {
        false
    }

    fn get_trigger_delay(&self, _event: &ChangeEvent) -> Duration {
        self.min_interval
    }

    fn poll_interrupts_watch(&self) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "LowPower"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_change_event_watch() {
        let event = ChangeEvent::WatchEvent {
            config_id: "test-config".to_string(),
            paths: vec![PathBuf::from("/test/path.txt")],
        };

        if let ChangeEvent::WatchEvent { config_id, paths } = event {
            assert_eq!(config_id, "test-config");
            assert_eq!(paths.len(), 1);
        } else {
            panic!("Expected WatchEvent");
        }
    }

    #[test]
    fn test_change_event_poll() {
        let event = ChangeEvent::PollEvent {
            config_id: "test-config".to_string(),
        };

        if let ChangeEvent::PollEvent { config_id } = event {
            assert_eq!(config_id, "test-config");
        } else {
            panic!("Expected PollEvent");
        }
    }

    #[test]
    fn test_global_poll_type() {
        // 测试 GlobalPollType 枚举
        let interval = GlobalPollType::Interval;
        let scheduled = GlobalPollType::Scheduled;

        assert_eq!(interval, GlobalPollType::Interval);
        assert_eq!(scheduled, GlobalPollType::Scheduled);
        assert_ne!(interval, scheduled);

        // 测试 Clone 和 Copy
        let interval_copy = interval;
        assert_eq!(interval, interval_copy);
    }

    #[test]
    fn test_change_event_global_poll() {
        use crate::autobackup::config::BackupDirection;

        // 测试上传间隔轮询
        let upload_interval = ChangeEvent::GlobalPollEvent {
            direction: BackupDirection::Upload,
            poll_type: GlobalPollType::Interval,
        };

        if let ChangeEvent::GlobalPollEvent { direction, poll_type } = upload_interval {
            assert_eq!(direction, BackupDirection::Upload);
            assert_eq!(poll_type, GlobalPollType::Interval);
        } else {
            panic!("Expected GlobalPollEvent");
        }

        // 测试下载指定时间轮询
        let download_scheduled = ChangeEvent::GlobalPollEvent {
            direction: BackupDirection::Download,
            poll_type: GlobalPollType::Scheduled,
        };

        if let ChangeEvent::GlobalPollEvent { direction, poll_type } = download_scheduled {
            assert_eq!(direction, BackupDirection::Download);
            assert_eq!(poll_type, GlobalPollType::Scheduled);
        } else {
            panic!("Expected GlobalPollEvent");
        }
    }

    #[test]
    fn test_default_trigger_policy() {
        let policy = DefaultTriggerPolicy::default();
        assert_eq!(policy.name(), "Default");
        assert!(policy.poll_interrupts_watch());

        // Poll events should trigger immediately
        let poll_event = ChangeEvent::PollEvent {
            config_id: "test".to_string(),
        };
        assert!(policy.should_trigger_immediately(&poll_event));

        // Watch events should not trigger immediately
        let watch_event = ChangeEvent::WatchEvent {
            config_id: "test".to_string(),
            paths: vec![],
        };
        assert!(!policy.should_trigger_immediately(&watch_event));
    }

    #[test]
    fn test_default_trigger_policy_custom_window() {
        let policy = DefaultTriggerPolicy::new(Duration::from_secs(5));
        let event = ChangeEvent::WatchEvent {
            config_id: "test".to_string(),
            paths: vec![],
        };
        assert_eq!(policy.get_trigger_delay(&event), Duration::from_secs(5));
    }

    #[test]
    fn test_night_mode_policy() {
        let policy = NightModePolicy::new(3, 0);
        assert_eq!(policy.name(), "NightMode");
        assert!(!policy.poll_interrupts_watch());

        let event = ChangeEvent::PollEvent {
            config_id: "test".to_string(),
        };
        assert!(!policy.should_trigger_immediately(&event));

        // Delay should be positive
        let delay = policy.get_trigger_delay(&event);
        assert!(delay.as_secs() > 0);
    }

    #[test]
    fn test_low_power_policy() {
        let policy = LowPowerPolicy::new(Duration::from_secs(300));
        assert_eq!(policy.name(), "LowPower");
        assert!(!policy.poll_interrupts_watch());

        let event = ChangeEvent::WatchEvent {
            config_id: "test".to_string(),
            paths: vec![],
        };
        assert!(!policy.should_trigger_immediately(&event));
        assert_eq!(policy.get_trigger_delay(&event), Duration::from_secs(300));
    }

    #[tokio::test]
    async fn test_change_aggregator_creation() {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (aggregated_tx, _aggregated_rx) = mpsc::unbounded_channel();

        let aggregator = ChangeAggregator::new(
            event_rx,
            aggregated_tx,
            Duration::from_secs(1),
        );

        assert_eq!(aggregator.pending_count(), 0);
        assert!(aggregator.pending_configs().is_empty());

        // Clean up
        drop(event_tx);
    }

    #[tokio::test]
    async fn test_change_aggregator_with_default_window() {
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (aggregated_tx, _aggregated_rx) = mpsc::unbounded_channel();

        let aggregator = ChangeAggregator::with_default_window(event_rx, aggregated_tx);
        assert_eq!(aggregator.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_change_aggregator_poll_event_flush_barrier() {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (aggregated_tx, mut aggregated_rx) = mpsc::unbounded_channel();

        let mut aggregator = ChangeAggregator::new(
            event_rx,
            aggregated_tx,
            Duration::from_secs(10), // Long window to ensure events are pending
        );

        // Spawn aggregator
        let handle = tokio::spawn(async move {
            aggregator.run().await;
        });

        // Send a watch event first
        event_tx.send(ChangeEvent::WatchEvent {
            config_id: "config-1".to_string(),
            paths: vec![PathBuf::from("/test/file1.txt")],
        }).unwrap();

        // Give time for event to be processed
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send a poll event - this should flush pending watch events
        event_tx.send(ChangeEvent::PollEvent {
            config_id: "config-1".to_string(),
        }).unwrap();

        // Give time for flush
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should receive the flushed watch event first
        let first_event = tokio::time::timeout(
            Duration::from_millis(100),
            aggregated_rx.recv()
        ).await;

        assert!(first_event.is_ok());
        if let Ok(Some(ChangeEvent::WatchEvent { config_id, paths })) = first_event {
            assert_eq!(config_id, "config-1");
            assert!(!paths.is_empty());
        }

        // Then receive the poll event
        let second_event = tokio::time::timeout(
            Duration::from_millis(100),
            aggregated_rx.recv()
        ).await;

        assert!(second_event.is_ok());
        if let Ok(Some(ChangeEvent::PollEvent { config_id })) = second_event {
            assert_eq!(config_id, "config-1");
        }

        // Clean up
        drop(event_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_change_aggregator_dedup_paths() {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (aggregated_tx, mut aggregated_rx) = mpsc::unbounded_channel();

        let mut aggregator = ChangeAggregator::new(
            event_rx,
            aggregated_tx,
            Duration::from_millis(100), // Short window for testing
        );

        // Spawn aggregator
        let handle = tokio::spawn(async move {
            aggregator.run().await;
        });

        // Send multiple events with same path
        let path = PathBuf::from("/test/file.txt");
        for _ in 0..3 {
            event_tx.send(ChangeEvent::WatchEvent {
                config_id: "config-1".to_string(),
                paths: vec![path.clone()],
            }).unwrap();
        }

        // Wait for aggregation window
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Should receive only one aggregated event with deduplicated paths
        let event = tokio::time::timeout(
            Duration::from_millis(100),
            aggregated_rx.recv()
        ).await;

        assert!(event.is_ok());
        if let Ok(Some(ChangeEvent::WatchEvent { paths, .. })) = event {
            // Paths should be deduplicated
            assert_eq!(paths.len(), 1);
        }

        // Clean up
        drop(event_tx);
        let _ = handle.await;
    }
}
