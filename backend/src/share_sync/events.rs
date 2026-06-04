//! 分享同步 WebSocket 事件
//!
//! 通过现有 `TaskEvent::ShareSync` 通道下发；前端 `Subscribe { subscriptions: ["share_sync"] }` 接收。

use serde::{Deserialize, Serialize};

/// 分享同步事件
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ShareSyncEvent {
    /// 订阅创建
    SubscriptionCreated {
        subscription_id: String,
        name: String,
    },
    /// 订阅更新
    SubscriptionUpdated {
        subscription_id: String,
    },
    /// 订阅删除
    SubscriptionDeleted {
        subscription_id: String,
    },
    /// 订阅启用/停用
    StatusChanged {
        subscription_id: String,
        enabled: bool,
    },
    /// 一次同步开始
    RunStarted {
        run_id: String,
        subscription_id: String,
    },
    /// 检测到差异
    DiffDetected {
        run_id: String,
        subscription_id: String,
        added: usize,
        modified: usize,
        removed: usize,
    },
    /// 调度的单条 run_item
    ItemScheduled {
        run_id: String,
        subscription_id: String,
        path: String,
        target: String,
        action: String,
    },
    /// run_item 状态变化
    ItemStatusChanged {
        run_id: String,
        subscription_id: String,
        path: String,
        status: String,
        error: Option<String>,
    },
    /// 一次同步完成
    RunCompleted {
        run_id: String,
        subscription_id: String,
        added: usize,
        modified: usize,
        removed: usize,
        failed: usize,
    },
    /// 一次同步失败
    RunFailed {
        run_id: String,
        subscription_id: String,
        error: String,
    },
}

impl ShareSyncEvent {
    /// 关联的 subscription_id（用于 ws 订阅过滤）
    pub fn subscription_id(&self) -> &str {
        match self {
            Self::SubscriptionCreated { subscription_id, .. }
            | Self::SubscriptionUpdated { subscription_id }
            | Self::SubscriptionDeleted { subscription_id }
            | Self::StatusChanged { subscription_id, .. }
            | Self::RunStarted { subscription_id, .. }
            | Self::DiffDetected { subscription_id, .. }
            | Self::ItemScheduled { subscription_id, .. }
            | Self::ItemStatusChanged { subscription_id, .. }
            | Self::RunCompleted { subscription_id, .. }
            | Self::RunFailed { subscription_id, .. } => subscription_id,
        }
    }

    /// 事件类型字符串
    pub fn event_type_name(&self) -> &'static str {
        match self {
            Self::SubscriptionCreated { .. } => "subscription_created",
            Self::SubscriptionUpdated { .. } => "subscription_updated",
            Self::SubscriptionDeleted { .. } => "subscription_deleted",
            Self::StatusChanged { .. } => "status_changed",
            Self::RunStarted { .. } => "run_started",
            Self::DiffDetected { .. } => "diff_detected",
            Self::ItemScheduled { .. } => "item_scheduled",
            Self::ItemStatusChanged { .. } => "item_status_changed",
            Self::RunCompleted { .. } => "run_completed",
            Self::RunFailed { .. } => "run_failed",
        }
    }
}

/// 简单的事件发布器（trait + 默认 noop 实现）
pub trait ShareSyncEventPublisher: Send + Sync {
    fn publish(&self, event: ShareSyncEvent);
}

/// Noop 发布器（用于测试 / 初始化失败时）
pub struct NoopShareSyncEventPublisher;
impl ShareSyncEventPublisher for NoopShareSyncEventPublisher {
    fn publish(&self, _event: ShareSyncEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_subscription_id_extraction() {
        let e = ShareSyncEvent::RunStarted {
            run_id: "r1".into(),
            subscription_id: "s1".into(),
        };
        assert_eq!(e.subscription_id(), "s1");
    }

    #[test]
    fn test_event_type_name() {
        let e = ShareSyncEvent::DiffDetected {
            run_id: "r".into(),
            subscription_id: "s".into(),
            added: 1,
            modified: 2,
            removed: 3,
        };
        assert_eq!(e.event_type_name(), "diff_detected");
    }

    #[test]
    fn test_event_serialize() {
        let e = ShareSyncEvent::RunCompleted {
            run_id: "r".into(),
            subscription_id: "s".into(),
            added: 1,
            modified: 2,
            removed: 3,
            failed: 0,
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"run_completed\""));
    }
}
