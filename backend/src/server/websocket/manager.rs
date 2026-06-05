//! WebSocket 连接管理器
//!
//! 管理所有 WebSocket 连接，实现订阅管理机制和消息节流
//!
//! ## 设计要点
//! - 订阅管理：支持通配符匹配（如 `download:*`）
//! - 反向索引优化：高并发场景性能提升
//! - 节流机制：按 event_type:task_id 分桶，避免事件覆盖

use crate::server::events::{EventPriority, TaskEvent, TimestampedEvent};
use crate::server::websocket::message::WsServerMessage;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// 最小推送间隔（毫秒）
const MIN_PUSH_INTERVAL_MS: u64 = 200;
/// 批量发送间隔（毫秒）
const BATCH_INTERVAL_MS: u64 = 100;
/// 默认批量发送最大事件数
const DEFAULT_MAX_BATCH_SIZE: usize = 10;
/// last_sent 过期时间（秒）
const LAST_SENT_EXPIRE_SECS: u64 = 60;
/// 每个连接的最大待发送事件数（防止内存无限增长）
/// Requirements: 13.3
pub const MAX_PENDING_EVENTS_PER_CONNECTION: usize = 100;

/// WebSocket 连接信息
#[derive(Debug)]
pub struct WsConnection {
    /// 连接 ID
    pub id: String,
    /// 消息发送通道
    pub sender: mpsc::UnboundedSender<WsServerMessage>,
    /// 连接时间
    #[allow(dead_code)]
    pub connected_at: Instant,
    /// 最后活动时间
    pub last_active: Instant,
}

/// 待发送事件（包含分组信息）
#[derive(Debug, Clone)]
pub struct PendingEvent {
    /// 事件内容
    pub event: TimestampedEvent,
    /// 分组 ID（用于文件夹下载等场景）
    pub group_id: Option<String>,
}

/// WebSocket 管理器
///
/// 实现直接发送机制
#[derive(Debug)]
pub struct WebSocketManager {
    /// 所有连接
    connections: DashMap<String, WsConnection>,

    /// 订阅管理：connection_id -> 订阅模式集合
    /// 使用 Arc<str> 减少内存分配
    subscriptions: DashMap<String, HashSet<Arc<str>>>,

    /// 反向索引：订阅模式 -> 连接 ID 集合
    /// 用于快速查找订阅了某个模式的所有连接
    subscription_index: DashMap<Arc<str>, HashSet<String>>,

    /// 待发送事件：connection_id -> throttle_key -> PendingEvent
    /// throttle_key = event_type:task_id，避免同一任务的不同事件类型互相覆盖
    pending_events: DashMap<String, HashMap<String, PendingEvent>>,

    /// 上次发送时间：connection_id -> throttle_key -> Instant
    last_sent: DashMap<String, HashMap<String, Instant>>,

    /// 全局事件 ID 计数器
    event_id_counter: Arc<AtomicU64>,

    /// 是否正在运行
    running: AtomicBool,
}

impl WebSocketManager {
    /// 创建新的 WebSocket 管理器
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
            subscriptions: DashMap::new(),
            subscription_index: DashMap::new(),
            pending_events: DashMap::new(),
            last_sent: DashMap::new(),
            event_id_counter: Arc::new(AtomicU64::new(1)),
            running: AtomicBool::new(false),
        }
    }

    // ==================== 订阅管理 ====================

    /// 规范化订阅模式，生成所有通配符版本
    ///
    /// 例如 `download:file:progress` 会生成：
    /// - `download:file:progress`（精确匹配）
    /// - `download:file:*`（匹配所有 download:file 事件）
    /// - `download:*`（匹配所有 download 事件）
    /// - `*`（匹配所有事件）
    fn normalize_subscription(pattern: &str) -> Vec<Arc<str>> {
        let mut patterns = Vec::new();
        patterns.push(Arc::from(pattern));

        // 生成通配符版本
        let parts: Vec<&str> = pattern.split(':').collect();
        for i in (1..parts.len()).rev() {
            let wildcard = format!("{}:*", parts[..i].join(":"));
            patterns.push(Arc::from(wildcard.as_str()));
        }

        patterns
    }

    /// 添加订阅
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `patterns`: 订阅模式列表
    pub fn subscribe(&self, connection_id: &str, patterns: Vec<String>) {
        let mut conn_subs = self
            .subscriptions
            .entry(connection_id.to_string())
            .or_default();

        for pattern in patterns {
            // 规范化订阅模式，生成所有通配符版本，实现 O(1) 匹配
            let normalized_patterns = Self::normalize_subscription(&pattern);

            for pattern_arc in normalized_patterns {
                // 添加到连接的订阅集合
                conn_subs.insert(Arc::clone(&pattern_arc));

                // 更新反向索引
                self.subscription_index
                    .entry(pattern_arc)
                    .or_default()
                    .insert(connection_id.to_string());
            }
        }

        info!("连接 {} 订阅更新: {:?}", connection_id, conn_subs.value());
    }

    /// 移除订阅
    pub fn unsubscribe(&self, connection_id: &str, patterns: Vec<String>) {
        if let Some(mut conn_subs) = self.subscriptions.get_mut(connection_id) {
            for pattern in patterns {
                let pattern_arc: Arc<str> = Arc::from(pattern.as_str());

                // 从连接的订阅集合移除
                conn_subs.remove(&pattern_arc);

                // 更新反向索引
                if let Some(mut index_entry) = self.subscription_index.get_mut(&pattern_arc) {
                    index_entry.remove(connection_id);
                    if index_entry.is_empty() {
                        drop(index_entry);
                        self.subscription_index.remove(&pattern_arc);
                    }
                }
            }
            info!(
                "连接 {} 取消订阅，剩余: {:?}",
                connection_id,
                conn_subs.value()
            );
        }
    }

    /// 取消连接的所有订阅
    fn unsubscribe_all(&self, connection_id: &str) {
        if let Some((_, subs)) = self.subscriptions.remove(connection_id) {
            for pattern in subs {
                if let Some(mut index_entry) = self.subscription_index.get_mut(&pattern) {
                    index_entry.remove(connection_id);
                    if index_entry.is_empty() {
                        drop(index_entry);
                        self.subscription_index.remove(&pattern);
                    }
                }
            }
            debug!("连接 {} 的所有订阅已清理", connection_id);
        }
    }

    /// 检查连接是否应该接收事件
    ///
    /// 使用规范化订阅实现 O(1) 匹配
    ///
    /// ## 备份任务隔离
    /// - 备份任务事件（is_backup=true）只发送给订阅了 `backup` 的连接
    /// - 普通订阅（如 `download`、`upload`、`*`）不会收到备份任务事件
    fn should_send_event(
        &self,
        connection_id: &str,
        event: &TaskEvent,
        group_id: Option<&str>,
    ) -> bool {
        // 获取连接的订阅集合
        let conn_subs = match self.subscriptions.get(connection_id) {
            Some(subs) => subs,
            None => return false,
        };

        let category = event.category();
        let event_type = event.event_type();
        let task_id = event.task_id();
        let is_backup = event.is_backup();

        // --- 备份任务隔离逻辑 ---
        // 备份任务事件只发送给明确订阅了 backup 的连接
        if is_backup {
            // 检查是否订阅了 backup 相关模式
            let backup_pattern = Arc::from("backup");
            let backup_wildcard = Arc::from("backup:*");

            if conn_subs.contains(&backup_pattern) || conn_subs.contains(&backup_wildcard) {
                return true;
            }

            // 备份任务不发送给普通订阅（即使订阅了 * 或 download/upload）
            return false;
        }

        // --- 子任务事件优先处理 ---
        if let Some(gid) = group_id {
            let group_pattern = format!("{}:{}", category, gid);
            let folder_pattern = format!("folder:{}", gid); // 兼容旧格式
            if conn_subs.contains(&Arc::from(group_pattern.as_str()))
                || conn_subs.contains(&Arc::from(folder_pattern.as_str()))
            {
                return true;
            } else {
                return false; // 子任务没有订阅，不发送给普通订阅
            }
        }

        // --- 普通事件匹配 ---
        let exact = format!("{}:{}:{}", category, event_type, task_id);
        if conn_subs.contains(&Arc::from(exact.as_str())) {
            return true;
        }

        let event_type_pattern = format!("{}:{}:*", category, event_type);
        if conn_subs.contains(&Arc::from(event_type_pattern.as_str())) {
            return true;
        }

        let category_pattern = format!("{}:*", category);
        if conn_subs.contains(&Arc::from(category_pattern.as_str())) {
            return true;
        }

        if conn_subs.contains(&Arc::from(category)) {
            return true;
        }

        if conn_subs.contains(&Arc::from("*")) {
            return true;
        }

        false
    }

    /// 获取节流 key
    ///
    /// 返回 `event_type:task_id`，避免同一任务的不同事件类型互相覆盖
    fn get_throttle_key(event: &TaskEvent) -> String {
        format!("{}:{}", event.event_type(), event.task_id())
    }

    /// 获取动态批量处理数量
    ///
    /// 根据连接数调整 max_batch_size
    fn get_dynamic_batch_size(&self) -> usize {
        let conn_count = self.connections.len();
        if conn_count <= 5 {
            DEFAULT_MAX_BATCH_SIZE
        } else if conn_count <= 20 {
            DEFAULT_MAX_BATCH_SIZE * 2
        } else {
            DEFAULT_MAX_BATCH_SIZE * 4
        }
    }

    /// 注册新连接
    ///
    /// 返回用于接收服务端消息的接收器
    pub fn register(&self, connection_id: String) -> mpsc::UnboundedReceiver<WsServerMessage> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let now = Instant::now();

        let connection = WsConnection {
            id: connection_id.clone(),
            sender,
            connected_at: now,
            last_active: now,
        };

        self.connections.insert(connection_id.clone(), connection);
        info!("WebSocket 连接已注册: {}", connection_id);

        receiver
    }

    /// 移除连接
    ///
    /// 同时清理订阅、pending_events、last_sent、反向索引
    pub fn unregister(&self, connection_id: &str) {
        if self.connections.remove(connection_id).is_some() {
            // 清理订阅和反向索引
            self.unsubscribe_all(connection_id);

            // 清理 pending_events
            self.pending_events.remove(connection_id);

            // 清理 last_sent
            self.last_sent.remove(connection_id);

            info!("WebSocket 连接已移除并清理: {}", connection_id);
        }
    }

    /// 更新连接活动时间
    pub fn touch(&self, connection_id: &str) {
        if let Some(mut conn) = self.connections.get_mut(connection_id) {
            conn.last_active = Instant::now();
        }
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// 向指定连接发送消息
    ///
    /// 检查连接存在性并发送消息
    pub fn send_to(&self, connection_id: &str, message: WsServerMessage) -> bool {
        // 先检查连接是否存在
        let conn = match self.connections.get(connection_id) {
            Some(c) => c,
            None => {
                debug!("连接不存在: {}", connection_id);
                return false;
            }
        };

        // 发送消息
        match conn.sender.send(message) {
            Ok(_) => true,
            Err(e) => {
                warn!("发送消息失败（可能连接已关闭）: {} - {}", connection_id, e);
                false
            }
        }
    }

    /// 广播消息给所有连接（仅用于非订阅场景，如 Pong）
    pub fn broadcast(&self, message: WsServerMessage) {
        let mut failed_connections = Vec::new();

        for conn in self.connections.iter() {
            if conn.sender.send(message.clone()).is_err() {
                failed_connections.push(conn.id.clone());
            }
        }

        // 移除发送失败的连接
        for id in failed_connections {
            self.unregister(&id);
        }
    }

    // ==================== 事件发送 ====================

    /// 带订阅检查和节流的发送方法
    ///
    /// 这是业务模块调用的主要方法
    ///
    /// # 参数
    /// - `event`: 任务事件
    /// - `group_id`: 可选的分组 ID（用于文件夹下载等场景）
    pub fn send_if_subscribed(&self, event: TaskEvent, group_id: Option<String>) {
        if self.connection_count() == 0 {
            return;
        }

        let event_id = self.event_id_counter.fetch_add(1, Ordering::SeqCst);
        let timestamped = TimestampedEvent::new(event_id, event.clone());
        let throttle_key = Self::get_throttle_key(&event);
        let priority = event.priority();
        let now = Instant::now();

        // 遍历所有连接，检查订阅并发送
        for conn in self.connections.iter() {
            let connection_id = &conn.id;

            // 检查是否应该发送给该连接
            if !self.should_send_event(connection_id, &event, group_id.as_deref()) {
                continue;
            }

            // 高优先级事件直接发送
            if priority == EventPriority::High {
                let should_send = {
                    let last_sent_map = self.last_sent.get(connection_id);
                    match last_sent_map {
                        Some(map) => match map.get(&throttle_key) {
                            Some(last) => {
                                now.duration_since(*last)
                                    >= Duration::from_millis(MIN_PUSH_INTERVAL_MS / 2)
                            }
                            None => true,
                        },
                        None => true,
                    }
                };

                if should_send {
                    if self.send_to(connection_id, WsServerMessage::event(timestamped.clone())) {
                        // 🔥 记录成功发送的事件
                        info!(
                            "📡 WS事件已发送 | 连接={} | 类别={} | 事件={} | 任务={} | 分组={:?} | 事件ID={} | 优先级={:?} | 节流键={}",
                            connection_id,
                            timestamped.event.category(),
                            timestamped.event.event_type(),
                            timestamped.event.task_id(),
                            group_id,
                            timestamped.event_id,
                            priority,
                            throttle_key
                        );

                        self.last_sent
                            .entry(connection_id.to_string())
                            .or_default()
                            .insert(throttle_key.clone(), now);

                        // 清除该连接该 throttle_key 的待发送事件
                        if let Some(mut pending) = self.pending_events.get_mut(connection_id) {
                            pending.remove(&throttle_key);
                        }
                    }
                    continue;
                }
            }

            // 低/中优先级事件暂存，等待批量发送
            // 检查并限制 pending_events 大小（Requirements: 13.3）
            let mut pending_map = self
                .pending_events
                .entry(connection_id.to_string())
                .or_default();

            // 如果超过限制，丢弃最旧的事件
            if pending_map.len() >= MAX_PENDING_EVENTS_PER_CONNECTION {
                // 找到最旧的事件（按 event_id 排序）
                if let Some(oldest_key) = pending_map
                    .iter()
                    .min_by_key(|(_, pe)| pe.event.event_id)
                    .map(|(k, _)| k.clone())
                {
                    pending_map.remove(&oldest_key);
                    warn!(
                        "连接 {} 的待发送事件队列已满（{}），丢弃最旧事件: {}",
                        connection_id, MAX_PENDING_EVENTS_PER_CONNECTION, oldest_key
                    );
                }
            }

            pending_map.insert(
                throttle_key.clone(),
                PendingEvent {
                    event: timestamped.clone(),
                    group_id: group_id.clone(),
                },
            );
        }
    }

    /// 启动批量发送器
    ///
    /// 使用 Weak 引用避免循环引用导致的内存泄漏
    pub fn start_batch_sender(self: Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("批量发送器已在运行");
            return;
        }

        let weak_self = Arc::downgrade(&self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(BATCH_INTERVAL_MS));

            loop {
                interval.tick().await;

                // 使用 Weak 引用，如果 WebSocketManager 已被销毁则退出
                match weak_self.upgrade() {
                    Some(manager) => {
                        if !manager.running.load(Ordering::SeqCst) {
                            info!("批量发送器收到停止信号");
                            break;
                        }
                        manager.flush_pending_events();
                    }
                    None => {
                        info!("WebSocketManager 已销毁，批量发送器退出");
                        break;
                    }
                }
            }
        });

        info!("WebSocket 批量发送器已启动");
    }

    /// 停止批量发送器
    pub fn stop_batch_sender(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("WebSocket 批量发送器已停止");
    }

    /// 刷新待发送事件
    ///
    /// 按连接分组处理，只遍历有 pending 的连接
    fn flush_pending_events(&self) {
        if self.connection_count() == 0 || self.pending_events.is_empty() {
            return;
        }

        let now = Instant::now();
        let max_batch_size = self.get_dynamic_batch_size();

        // 遍历所有有 pending 事件的连接
        let connection_ids: Vec<String> = self
            .pending_events
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for connection_id in connection_ids {
            // 检查连接是否还存在
            if !self.connections.contains_key(&connection_id) {
                self.pending_events.remove(&connection_id);
                self.last_sent.remove(&connection_id);
                continue;
            }

            let mut events_to_send = Vec::new();
            let mut keys_to_remove = Vec::new();

            // 收集该连接需要发送的事件
            if let Some(mut pending_map) = self.pending_events.get_mut(&connection_id) {
                let mut last_sent_map = self.last_sent.entry(connection_id.clone()).or_default();

                for (throttle_key, pending_event) in pending_map.iter() {
                    // 重新检查订阅状态（用户可能在事件进入 pending 后取消订阅）
                    if !self.should_send_event(
                        &connection_id,
                        &pending_event.event.event,
                        pending_event.group_id.as_deref(),
                    ) {
                        keys_to_remove.push(throttle_key.clone());
                        continue;
                    }

                    // 检查频率限制
                    let should_send = match last_sent_map.get(throttle_key) {
                        Some(last) => {
                            now.duration_since(*last) >= Duration::from_millis(MIN_PUSH_INTERVAL_MS)
                        }
                        None => true,
                    };

                    if should_send {
                        events_to_send.push(pending_event.event.clone());
                        keys_to_remove.push(throttle_key.clone());
                        last_sent_map.insert(throttle_key.clone(), now);

                        if events_to_send.len() >= max_batch_size {
                            break;
                        }
                    }
                }

                // 移除已发送的事件
                for key in &keys_to_remove {
                    pending_map.remove(key);
                }

                // 清理过期的 last_sent 记录
                let expire_threshold = Duration::from_secs(LAST_SENT_EXPIRE_SECS);
                last_sent_map.retain(|_, last| now.duration_since(*last) < expire_threshold);
            }

            // 发送事件
            if !events_to_send.is_empty() {
                if events_to_send.len() == 1 {
                    let event = events_to_send.remove(0);
                    info!(
                        "📡 WS批量事件已发送(单条) | 连接={} | 类别={} | 事件={} | 任务={} | 事件ID={}",
                        connection_id,
                        event.event.category(),
                        event.event.event_type(),
                        event.event.task_id(),
                        event.event_id
                    );
                    self.send_to(&connection_id, WsServerMessage::event(event));
                } else {
                    info!(
                        "📡 WS批量事件已发送({}) | 连接={} | 事件ID范围=[{}-{}]",
                        events_to_send.len(),
                        connection_id,
                        events_to_send.first().map(|e| e.event_id).unwrap_or(0),
                        events_to_send.last().map(|e| e.event_id).unwrap_or(0)
                    );
                    self.send_to(&connection_id, WsServerMessage::event_batch(events_to_send));
                }
            }
        }
    }

    /// 清理超时连接
    pub fn cleanup_stale_connections(&self, timeout: Duration) {
        let now = Instant::now();
        let mut stale_connections = Vec::new();

        for conn in self.connections.iter() {
            if now.duration_since(conn.last_active) > timeout {
                stale_connections.push(conn.id.clone());
            }
        }

        for id in stale_connections {
            warn!("清理超时连接: {}", id);
            self.unregister(&id);
        }
    }

    /// 清理过期的 last_sent 记录
    ///
    /// 移除超过 LAST_SENT_EXPIRE_SECS 未更新的记录
    /// Requirements: 13.2
    pub fn cleanup_expired_last_sent(&self) {
        let now = Instant::now();
        let expire_threshold = Duration::from_secs(LAST_SENT_EXPIRE_SECS);
        let mut cleaned_count = 0usize;

        for mut entry in self.last_sent.iter_mut() {
            let before_len = entry.len();
            entry.retain(|_, last| now.duration_since(*last) < expire_threshold);
            cleaned_count += before_len - entry.len();
        }

        // 移除空的 last_sent 条目
        self.last_sent.retain(|_, map| !map.is_empty());

        if cleaned_count > 0 {
            debug!("清理了 {} 条过期的 last_sent 记录", cleaned_count);
        }
    }

    /// 连接断开时的完整清理
    ///
    /// 清理 pending_events、last_sent 和订阅信息
    /// Requirements: 13.1
    pub fn on_connection_closed(&self, connection_id: &str) {
        // 清理 pending_events
        if self.pending_events.remove(connection_id).is_some() {
            debug!("连接 {} 的 pending_events 已清理", connection_id);
        }

        // 清理 last_sent
        if self.last_sent.remove(connection_id).is_some() {
            debug!("连接 {} 的 last_sent 已清理", connection_id);
        }

        // 清理订阅和反向索引
        self.unsubscribe_all(connection_id);

        // 从连接列表移除
        if self.connections.remove(connection_id).is_some() {
            info!("连接 {} 已关闭并完成清理", connection_id);
        }
    }

    /// 获取指定连接的 pending_events 数量
    ///
    /// 用于测试和监控
    pub fn get_pending_events_count(&self, connection_id: &str) -> usize {
        self.pending_events
            .get(connection_id)
            .map(|map| map.len())
            .unwrap_or(0)
    }

    /// 获取连接的订阅列表
    pub fn get_subscriptions(&self, connection_id: &str) -> Vec<String> {
        self.subscriptions
            .get(connection_id)
            .map(|subs| subs.iter().map(|s| s.to_string()).collect())
            .unwrap_or_default()
    }
}

impl Default for WebSocketManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::events::DownloadEvent;

    #[tokio::test]
    async fn test_register_unregister() {
        let manager = WebSocketManager::new();

        let _receiver = manager.register("conn-1".to_string());
        assert_eq!(manager.connection_count(), 1);

        manager.unregister("conn-1");
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_send_to_connection() {
        let manager = WebSocketManager::new();

        let mut receiver = manager.register("conn-1".to_string());

        manager.send_to("conn-1", WsServerMessage::pong(None));

        let msg = receiver.recv().await.unwrap();
        match msg {
            WsServerMessage::Pong { .. } => {}
            _ => panic!("Expected Pong message"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_and_send() {
        let manager = WebSocketManager::new();
        let mut receiver = manager.register("conn-1".to_string());

        // 订阅 download 类别
        manager.subscribe("conn-1", vec!["download".to_string()]);

        // 发送高优先级事件（Completed 是 High 优先级，会直接发送）
        let event = TaskEvent::Download(DownloadEvent::Completed {
            task_id: "test-1".to_string(),
            completed_at: 0,
            group_id: None,
            is_backup: false,
        });

        manager.send_if_subscribed(event, None);

        // 验证收到事件（高优先级事件直接发送），添加超时避免测试挂起
        let result = tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await;
        match result {
            Ok(Some(WsServerMessage::Event { .. })) => {}
            Ok(Some(msg)) => panic!("Expected Event message, got {:?}", msg),
            Ok(None) => panic!("Channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for event - event was not sent"),
        }
    }

    #[tokio::test]
    async fn test_no_subscription_no_event() {
        let manager = WebSocketManager::new();
        let mut receiver = manager.register("conn-1".to_string());

        // 不订阅，直接发送事件
        let event = TaskEvent::Download(DownloadEvent::Progress {
            task_id: "test-1".to_string(),
            downloaded_size: 100,
            total_size: 1024,
            speed: 100,
            progress: 10.0,
            group_id: None,
            is_backup: false,
        });

        manager.send_if_subscribed(event, None);

        // 使用 try_recv 验证没有消息
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(receiver.try_recv().is_err());
    }
}
