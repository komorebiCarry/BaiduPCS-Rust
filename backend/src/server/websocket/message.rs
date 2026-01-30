//! WebSocket 消息类型定义

use crate::server::events::TimestampedEvent;
use serde::{Deserialize, Serialize};

/// 客户端发送给服务端的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMessage {
    /// 心跳 Ping
    Ping {
        /// 客户端时间戳（毫秒）
        timestamp: i64,
    },
    /// 请求状态快照
    RequestSnapshot,
    /// 订阅事件
    ///
    /// 支持的订阅模式：
    /// - `download` - 所有下载事件
    /// - `download:*` - 所有下载事件（通配符）
    /// - `folder` - 所有文件夹下载事件
    /// - `upload` - 所有上传事件
    /// - `transfer` - 所有转存事件
    /// - `cloud_dl` - 所有离线下载事件
    /// - `cloud_dl:*` - 所有离线下载事件（通配符）
    /// - `*` - 所有事件
    Subscribe {
        /// 要订阅的模式列表
        subscriptions: Vec<String>,
    },
    /// 取消订阅事件
    Unsubscribe {
        /// 要取消订阅的模式列表
        subscriptions: Vec<String>,
    },
}

/// 服务端发送给客户端的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsServerMessage {
    /// 心跳 Pong
    Pong {
        /// 服务端时间戳（毫秒）
        timestamp: i64,
        /// 回显客户端时间戳（用于计算延迟）
        client_timestamp: Option<i64>,
    },
    /// 单个事件
    Event {
        /// 事件内容
        #[serde(flatten)]
        event: TimestampedEvent,
    },
    /// 批量事件
    EventBatch {
        /// 事件列表
        events: Vec<TimestampedEvent>,
    },
    /// 状态快照
    Snapshot {
        /// 下载任务列表
        downloads: Vec<serde_json::Value>,
        /// 上传任务列表
        uploads: Vec<serde_json::Value>,
        /// 转存任务列表
        transfers: Vec<serde_json::Value>,
        /// 文件夹下载列表
        folders: Vec<serde_json::Value>,
    },
    /// 连接成功
    Connected {
        /// 连接 ID
        connection_id: String,
        /// 服务端时间戳
        timestamp: i64,
    },
    /// 错误消息
    Error {
        /// 错误码
        code: String,
        /// 错误信息
        message: String,
    },
    /// 订阅成功
    SubscribeSuccess {
        /// 当前订阅列表
        subscriptions: Vec<String>,
    },
    /// 取消订阅成功
    UnsubscribeSuccess {
        /// 剩余订阅列表
        subscriptions: Vec<String>,
    },
}

impl WsServerMessage {
    /// 创建 Pong 消息
    pub fn pong(client_timestamp: Option<i64>) -> Self {
        Self::Pong {
            timestamp: chrono::Utc::now().timestamp_millis(),
            client_timestamp,
        }
    }

    /// 创建 Connected 消息
    pub fn connected(connection_id: String) -> Self {
        Self::Connected {
            connection_id,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// 创建错误消息
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            code: code.into(),
            message: message.into(),
        }
    }

    /// 创建单个事件消息
    pub fn event(event: TimestampedEvent) -> Self {
        Self::Event { event }
    }

    /// 创建批量事件消息
    pub fn event_batch(events: Vec<TimestampedEvent>) -> Self {
        Self::EventBatch { events }
    }

    /// 创建订阅成功消息
    pub fn subscribe_success(subscriptions: Vec<String>) -> Self {
        Self::SubscribeSuccess { subscriptions }
    }

    /// 创建取消订阅成功消息
    pub fn unsubscribe_success(subscriptions: Vec<String>) -> Self {
        Self::UnsubscribeSuccess { subscriptions }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_message_parsing() {
        let json = r#"{"type":"ping","timestamp":1234567890}"#;
        let msg: WsClientMessage = serde_json::from_str(json).unwrap();
        match msg {
            WsClientMessage::Ping { timestamp } => assert_eq!(timestamp, 1234567890),
            _ => panic!("Expected Ping message"),
        }
    }

    #[test]
    fn test_server_message_serialization() {
        let msg = WsServerMessage::pong(Some(1234567890));
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("pong"));
        assert!(json.contains("1234567890"));
    }
}
