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

    /// 🔥 用户原始订阅（不含派生 wildcard）
    ///
    /// `connection_id -> 用户实际调用 subscribe(...) 传入的原始 pattern 集合`。
    /// 每次 subscribe / unsubscribe 都先维护本字段，再根据本字段**整体重建**
    /// `subscriptions` + `subscription_index` 中该连接的派生 wildcard 视图，
    /// 确保对称性：`unsubscribe(p)` 释放的不只是 `p` 本身，还包含 `p` 贡献且
    /// 没有其他原始订阅共享的派生 wildcard（多个原始订阅共享同一 wildcard 时
    /// 仍保留，避免误删）。
    user_subscriptions: DashMap<String, HashSet<Arc<str>>>,

    /// 订阅管理：connection_id -> 派生订阅模式集合（用户原始 + wildcard 派生项）
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
            user_subscriptions: DashMap::new(),
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
    /// - `patterns`: 订阅模式列表（用户原始订阅，不含 wildcard 派生）
    ///
    /// **订阅对称性**：每次 subscribe / unsubscribe 都先维护
    /// `user_subscriptions`（用户原始订阅集合），再根据它整体重建该连接在
    /// `subscriptions` + `subscription_index` 的派生 wildcard 视图，保证
    /// subscribe/unsubscribe 对称（不会留下无主 wildcard 幽灵订阅）。
    pub fn subscribe(&self, connection_id: &str, patterns: Vec<String>) {
        // Step 1: 维护用户原始订阅
        {
            let mut user_subs = self
                .user_subscriptions
                .entry(connection_id.to_string())
                .or_default();
            for pattern in &patterns {
                user_subs.insert(Arc::from(pattern.as_str()));
            }
        }

        // Step 2: 根据原始订阅整体重建派生 wildcard 视图
        self.rebuild_derived_subscriptions(connection_id);

        let current_subs = self
            .subscriptions
            .get(connection_id)
            .map(|s| s.value().clone())
            .unwrap_or_default();
        info!("连接 {} 订阅更新: {:?}", connection_id, current_subs);
    }

    /// 移除订阅
    ///
    /// **取消订阅对称语义**：先从 `user_subscriptions` 删除原始订阅，再
    /// 整体重建派生 wildcard 视图。这样：
    /// - 用户取消 `download:file` 时，由 `download:file` 派生但被其他原始订阅
    ///   共享的 `download:*`（如 `download:folder`）会保留
    /// - 没有其他原始订阅共享的派生 wildcard（如本连接只订阅了 `download:file`，
    ///   则 `download:*` 和 `*` 也跟随释放）才会从反向索引中清除
    pub fn unsubscribe(&self, connection_id: &str, patterns: Vec<String>) {
        // Step 1: 从用户原始订阅中删除
        {
            if let Some(mut user_subs) = self.user_subscriptions.get_mut(connection_id) {
                for pattern in &patterns {
                    let arc: Arc<str> = Arc::from(pattern.as_str());
                    user_subs.remove(&arc);
                }
            }
        }

        // Step 2: 根据剩余原始订阅整体重建派生 wildcard 视图
        self.rebuild_derived_subscriptions(connection_id);

        let current_subs = self
            .subscriptions
            .get(connection_id)
            .map(|s| s.value().clone())
            .unwrap_or_default();
        info!(
            "连接 {} 取消订阅，剩余: {:?}",
            connection_id, current_subs
        );
    }

    /// 🔥 根据 `user_subscriptions[connection_id]` 整体
    /// 重建该连接在 `subscriptions` + `subscription_index` 中的派生 wildcard 视图。
    ///
    /// 算法：
    /// 1. 计算"应有的派生集合" = ∪ normalize_subscription(p) for p in user_subs
    /// 2. 取"当前派生集合" = subscriptions[connection_id]
    /// 3. 差集 to_add = 应有 \ 当前；to_remove = 当前 \ 应有
    /// 4. 对 to_add：插入 subscriptions[c] + subscription_index
    /// 5. 对 to_remove：从 subscriptions[c] 删除 + 反向索引同步清理（empty 则 drop）
    ///
    /// 时间复杂度：O(P × D + ΔP)，P = 原始订阅数，D = 平均规范化派生数，
    /// ΔP = 实际增减派生数。对单连接订阅数 << 100 的常规场景影响可忽略。
    fn rebuild_derived_subscriptions(&self, connection_id: &str) {
        // 1) 应有派生集合
        let desired: HashSet<Arc<str>> = self
            .user_subscriptions
            .get(connection_id)
            .map(|user_subs| {
                user_subs
                    .iter()
                    .flat_map(|p| Self::normalize_subscription(p))
                    .collect()
            })
            .unwrap_or_default();

        // 2) 当前派生集合
        let current: HashSet<Arc<str>> = self
            .subscriptions
            .get(connection_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();

        // 3) 差集计算
        let to_add: Vec<Arc<str>> = desired.difference(&current).cloned().collect();
        let to_remove: Vec<Arc<str>> = current.difference(&desired).cloned().collect();

        // 4) 应用增量到 subscriptions[c]
        let mut conn_subs = self
            .subscriptions
            .entry(connection_id.to_string())
            .or_default();
        for pat in &to_add {
            conn_subs.insert(Arc::clone(pat));
        }
        for pat in &to_remove {
            conn_subs.remove(pat);
        }
        let conn_subs_empty = conn_subs.is_empty();
        drop(conn_subs);

        // 如果该连接已无任何派生订阅，从 subscriptions 表中移除条目
        if conn_subs_empty {
            self.subscriptions.remove(connection_id);
        }

        // 5) 应用增量到 subscription_index
        for pat in to_add {
            self.subscription_index
                .entry(pat)
                .or_default()
                .insert(connection_id.to_string());
        }
        for pat in to_remove {
            if let Some(mut idx_entry) = self.subscription_index.get_mut(&pat) {
                idx_entry.remove(connection_id);
                if idx_entry.is_empty() {
                    drop(idx_entry);
                    self.subscription_index.remove(&pat);
                }
            }
        }

        // 用户原始订阅清空时，同步释放 user_subscriptions 条目
        let user_empty = self
            .user_subscriptions
            .get(connection_id)
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if user_empty {
            self.user_subscriptions.remove(connection_id);
        }
    }

    /// 取消连接的所有订阅
    fn unsubscribe_all(&self, connection_id: &str) {
        // 清空用户原始订阅 + 整体重建（差集会把所有派生项标记为 to_remove）
        self.user_subscriptions.remove(connection_id);
        self.rebuild_derived_subscriptions(connection_id);
        debug!("连接 {} 的所有订阅已清理", connection_id);
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
                            Some(last) => now.duration_since(*last) >= Duration::from_millis(MIN_PUSH_INTERVAL_MS / 2),
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
            let mut pending_map = self.pending_events
                .entry(connection_id.to_string())
                .or_default();

            // 如果超过限制，丢弃最旧的事件
            if pending_map.len() >= MAX_PENDING_EVENTS_PER_CONNECTION {
                // 找到最旧的事件（按 event_id 排序）
                if let Some(oldest_key) = pending_map.iter()
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

            pending_map.insert(throttle_key.clone(), PendingEvent {
                event: timestamped.clone(),
                group_id: group_id.clone(),
            });
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
        let connection_ids: Vec<String> = self.pending_events.iter()
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
                    if !self.should_send_event(&connection_id, &pending_event.event.event, pending_event.group_id.as_deref()) {
                        keys_to_remove.push(throttle_key.clone());
                        continue;
                    }

                    // 检查频率限制
                    let should_send = match last_sent_map.get(throttle_key) {
                        Some(last) => now.duration_since(*last) >= Duration::from_millis(MIN_PUSH_INTERVAL_MS),
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

    /// 🔥 枚举订阅了某个 topic 的所有连接 ID。
    ///
    /// 用于 CloudDl active-only 重订阅场景：
    /// - `set_active_uid(None → Some(uid))`（删完最后账号后重新登录 / 切回有账号状态）
    ///   时，需要找出所有「topic 订阅是 `cloud_dl` 但 `cloud_dl_ws_subscribers`
    ///   未记录」的连接，把它们补绑到新 active uid 的 monitor 上。
    /// - 不依赖 `cloud_dl_ws_subscribers` 内部状态，直接读 `subscription_index`
    ///   反向索引取所有订阅了 `cloud_dl` 的连接。
    pub fn connections_subscribed_to(&self, topic: &str) -> Vec<String> {
        let key: Arc<str> = Arc::from(topic);
        self.subscription_index
            .get(&key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 🔥 按谓词枚举订阅匹配条件的所有连接 ID。
    ///
    /// 比 `connections_subscribed_to(exact_topic)` 更灵活：可以同时覆盖 exact 和
    /// wildcard 多种订阅形式。CloudDl 场景下连接可能订阅 `cloud_dl` / `cloud_dl:*`
    /// / `cloud_dl:specific_event` 任意一种（`subscribe()` 内部还会规范化生成所有
    /// wildcard 版本），handler 用 `s == "cloud_dl" || s.starts_with("cloud_dl:")`
    /// 二判定来识别 CloudDl 订阅；rebind 补扫必须用同一谓词避免规则漂移。
    ///
    /// 实现：遍历 `subscription_index` 中所有 pattern Arc，对每个 pattern 调
    /// `predicate(pattern)`，命中则把该 pattern 关联的所有 connection_id 加入
    /// 结果集合（去重）。
    pub fn connections_matching_subscription<F>(&self, predicate: F) -> Vec<String>
    where
        F: Fn(&str) -> bool,
    {
        let mut result: HashSet<String> = HashSet::new();
        for entry in self.subscription_index.iter() {
            let pattern: &Arc<str> = entry.key();
            if predicate(pattern.as_ref()) {
                for conn_id in entry.value().iter() {
                    result.insert(conn_id.clone());
                }
            }
        }
        result.into_iter().collect()
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

            owner_uid: None,
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

            owner_uid: None,
        });

        manager.send_if_subscribed(event, None);

        // 使用 try_recv 验证没有消息
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(receiver.try_recv().is_err());
    }

    /// CloudDl rebind 补扫规则的最小回归用例（exact）。
    ///
    /// 场景：连接订阅 exact `cloud_dl` topic。`connections_matching_subscription`
    /// 用 handler 同款判定 `s == "cloud_dl" || s.starts_with("cloud_dl:")` 必须命中。
    #[tokio::test]
    async fn test_connections_matching_cloud_dl_exact() {
        let manager = WebSocketManager::new();
        let _r1 = manager.register("conn-1".to_string());
        let _r2 = manager.register("conn-2".to_string());

        manager.subscribe("conn-1", vec!["cloud_dl".to_string()]);
        manager.subscribe("conn-2", vec!["download".to_string()]);

        let conns = manager
            .connections_matching_subscription(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0], "conn-1");
    }

    /// CloudDl rebind 补扫规则的最小回归用例（wildcard）。
    ///
    /// 场景：连接订阅 `cloud_dl:progress_update`，`subscribe()` 内部规范化生成
    /// `cloud_dl:*` + `cloud_dl:progress_update`。补扫谓词必须能识别这两种 wildcard
    /// 形式（旧版 exact 查询会漏掉，导致 rebind 找不到此连接 → 新 monitor 无订阅者
    /// → 实时事件断流）。
    #[tokio::test]
    async fn test_connections_matching_cloud_dl_wildcard() {
        let manager = WebSocketManager::new();
        let _r = manager.register("conn-w".to_string());

        // 客户端订阅分层 topic（subscribe 会规范化产生 wildcard 版本）
        manager.subscribe("conn-w", vec!["cloud_dl:progress_update".to_string()]);

        let conns = manager
            .connections_matching_subscription(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));
        assert!(
            conns.contains(&"conn-w".to_string()),
            "wildcard `cloud_dl:*` 订阅必须被 starts_with 谓词识别，避免 rebind 漏绑"
        );
    }

    /// 补扫去重 — 同一连接订阅多种 cloud_dl 形式只返回一次。
    #[tokio::test]
    async fn test_connections_matching_cloud_dl_dedup() {
        let manager = WebSocketManager::new();
        let _r = manager.register("conn-d".to_string());

        // 同一连接同时订阅 exact + 分层 topic（产生 cloud_dl + cloud_dl:* 多个 pattern）
        manager.subscribe(
            "conn-d",
            vec![
                "cloud_dl".to_string(),
                "cloud_dl:status_changed".to_string(),
            ],
        );

        let conns = manager
            .connections_matching_subscription(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));
        // 多个 pattern 命中同一 conn → 集合去重 → 仅一条
        let occurrences = conns.iter().filter(|c| *c == "conn-d").count();
        assert_eq!(
            occurrences, 1,
            "同一连接订阅多种 cloud_dl 形式时补扫结果必须去重，否则 rebind 会重复 add_subscriber 导致计数失衡"
        );
    }

    /// subscribe / unsubscribe 对称性回归。
    ///
    /// 场景：连接订阅 `download:file` → subscribe 派生出 `download:*`；
    /// 之后 unsubscribe `download:file` → 必须**同时**释放派生的 `download:*`，
    /// 否则该连接仍会命中 `should_send_event` 中的 `download:*` 路径，收到本不该
    /// 收到的事件（"幽灵订阅"）。
    #[tokio::test]
    async fn test_unsubscribe_releases_derived_wildcards() {
        let manager = WebSocketManager::new();
        let _r = manager.register("conn-x".to_string());

        manager.subscribe("conn-x", vec!["download:file".to_string()]);
        // 订阅后 derived 视图含 download:file + download:*
        let derived_after_sub = manager.get_subscriptions("conn-x");
        assert!(derived_after_sub.iter().any(|s| s == "download:file"));
        assert!(derived_after_sub.iter().any(|s| s == "download:*"));

        manager.unsubscribe("conn-x", vec!["download:file".to_string()]);
        // 退订后 derived 视图必须**全部清空**（无其他原始订阅共享 download:*）
        let derived_after_unsub = manager.get_subscriptions("conn-x");
        assert!(
            derived_after_unsub.is_empty(),
            "退订 download:file 必须同时释放派生的 download:*；当前剩余: {:?}",
            derived_after_unsub
        );

        // 反向索引也必须不再含 download:* / download:file
        let conns_dl_star = manager.connections_subscribed_to("download:*");
        let conns_dl_file = manager.connections_subscribed_to("download:file");
        assert!(
            !conns_dl_star.contains(&"conn-x".to_string()),
            "subscription_index 必须同步清理 download:*"
        );
        assert!(
            !conns_dl_file.contains(&"conn-x".to_string()),
            "subscription_index 必须同步清理 download:file"
        );
    }

    /// 多原始订阅共享 wildcard 时不误删。
    ///
    /// 场景：连接同时订阅 `download:file` 和 `download:folder`，两者派生同一
    /// `download:*`。退订 `download:file` 时 `download:*` 必须**保留**（仍由
    /// `download:folder` 贡献），否则 `download:folder` 的事件无法命中通配符路径。
    #[tokio::test]
    async fn test_unsubscribe_preserves_shared_wildcard() {
        let manager = WebSocketManager::new();
        let _r = manager.register("conn-s".to_string());

        manager.subscribe(
            "conn-s",
            vec!["download:file".to_string(), "download:folder".to_string()],
        );

        // 两个原始订阅共享 download:*；derived 视图含 4 条
        let before = manager.get_subscriptions("conn-s");
        assert!(before.iter().any(|s| s == "download:file"));
        assert!(before.iter().any(|s| s == "download:folder"));
        assert!(before.iter().any(|s| s == "download:*"));

        manager.unsubscribe("conn-s", vec!["download:file".to_string()]);

        let after = manager.get_subscriptions("conn-s");
        assert!(
            !after.iter().any(|s| s == "download:file"),
            "download:file 已退订必须移除"
        );
        assert!(
            after.iter().any(|s| s == "download:folder"),
            "download:folder 未退订必须保留"
        );
        assert!(
            after.iter().any(|s| s == "download:*"),
            "download:* 仍由 download:folder 贡献，必须保留（不能误删共享 wildcard）"
        );
    }

    /// CloudDl 多子 topic 退订一个不应导致 monitor 计数错释。
    ///
    /// 场景：连接订阅 `cloud_dl:progress_update` + `cloud_dl:status_changed`，
    /// 退订 `cloud_dl:progress_update`，必须仍能通过 handler 的判定式
    /// (`s == "cloud_dl" || s.starts_with("cloud_dl:")`) 检测到剩余 cloud_dl 派生项；
    /// 否则 handler 会错误地 `remove_subscriber()` 让 monitor 计数清零、停止轮询。
    ///
    /// 本测试是 `handler.rs::Unsubscribe` 分支「退订后再查 `get_subscriptions` 确认
    /// 是否还有 cloud_dl 派生项」的核心契约保证。
    #[tokio::test]
    async fn test_partial_cloud_dl_unsubscribe_keeps_derived() {
        let manager = WebSocketManager::new();
        let _r = manager.register("conn-cd".to_string());

        // 订阅两个 cloud_dl 子 topic
        manager.subscribe(
            "conn-cd",
            vec![
                "cloud_dl:progress_update".to_string(),
                "cloud_dl:status_changed".to_string(),
            ],
        );

        // derived 视图必须含两个原始 + 共享 wildcard cloud_dl:*
        let before = manager.get_subscriptions("conn-cd");
        assert!(before.iter().any(|s| s == "cloud_dl:progress_update"));
        assert!(before.iter().any(|s| s == "cloud_dl:status_changed"));
        assert!(before.iter().any(|s| s == "cloud_dl:*"));

        // 只退一个子 topic
        manager.unsubscribe("conn-cd", vec!["cloud_dl:progress_update".to_string()]);

        let after = manager.get_subscriptions("conn-cd");
        // handler 判定式必须仍能命中（`cloud_dl:status_changed` + `cloud_dl:*`）
        let still_has_cloud_dl = after
            .iter()
            .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));
        assert!(
            still_has_cloud_dl,
            "退订单个 cloud_dl 子 topic 后必须仍能检测到剩余派生项；当前: {:?}",
            after
        );
        assert!(
            after.iter().any(|s| s == "cloud_dl:status_changed"),
            "未退订的子 topic 必须保留"
        );
        assert!(
            after.iter().any(|s| s == "cloud_dl:*"),
            "仍由 cloud_dl:status_changed 贡献的 wildcard 必须保留"
        );

        // 再退订最后一个子 topic 后才完全无 cloud_dl 派生
        manager.unsubscribe("conn-cd", vec!["cloud_dl:status_changed".to_string()]);
        let final_subs = manager.get_subscriptions("conn-cd");
        let still = final_subs
            .iter()
            .any(|s| s == "cloud_dl" || s.starts_with("cloud_dl:"));
        assert!(
            !still,
            "全部 cloud_dl 子 topic 退订后必须不再有任何 cloud_dl 派生项；当前: {:?}",
            final_subs
        );
    }
}
