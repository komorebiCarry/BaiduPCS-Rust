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
    SubscriptionUpdated { subscription_id: String },
    /// 订阅删除
    SubscriptionDeleted { subscription_id: String },
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
        /// v2 阶段 7:run 总耗时(ms),便于 A/B 对照
        /// 老消费者反序列化时 Option 缺失字段自动 None,不影响兼容
        #[serde(default)]
        duration_ms: Option<u64>,
        /// v2 阶段 7:本次 run 触发的二分递归总次数
        #[serde(default)]
        n_bisects: Option<u32>,
        /// v2 阶段 7:二分递归达到的最深 depth
        #[serde(default)]
        max_bisect_depth: Option<u32>,
    },
    /// 一次同步失败
    RunFailed {
        run_id: String,
        subscription_id: String,
        error: String,
        /// v1 新增：失败原因分类，便于前端红色高亮
        /// - `"quota_full"`        : 网盘空间不足
        /// - `"local_disk_full"`  : 本地磁盘满
        /// - `"unknown"`          : 其它（向后兼容，老数据 / 兜底）
        /// 老事件无此字段时反序列化为 `None`（用 `serde(default)`），前端按 `error` 字符串展示。
        #[serde(default)]
        reason: Option<String>,
    },
}

impl ShareSyncEvent {
    /// 关联的 subscription_id（用于 ws 订阅过滤）
    pub fn subscription_id(&self) -> &str {
        match self {
            Self::SubscriptionCreated {
                subscription_id, ..
            }
            | Self::SubscriptionUpdated { subscription_id }
            | Self::SubscriptionDeleted { subscription_id }
            | Self::StatusChanged {
                subscription_id, ..
            }
            | Self::RunStarted {
                subscription_id, ..
            }
            | Self::DiffDetected {
                subscription_id, ..
            }
            | Self::ItemScheduled {
                subscription_id, ..
            }
            | Self::ItemStatusChanged {
                subscription_id, ..
            }
            | Self::RunCompleted {
                subscription_id, ..
            }
            | Self::RunFailed {
                subscription_id, ..
            } => subscription_id,
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
            duration_ms: Some(1234),
            n_bisects: Some(0),
            max_bisect_depth: Some(0),
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"run_completed\""));
    }

    #[test]
    fn test_run_failed_with_reason() {
        // v1 新增：带 reason 的 RunFailed 序列化
        let e = ShareSyncEvent::RunFailed {
            run_id: "r".into(),
            subscription_id: "s".into(),
            error: "网盘空间不足".into(),
            reason: Some("quota_full".into()),
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"reason\":\"quota_full\""));
    }

    #[test]
    fn test_run_failed_backward_compat_no_reason_field() {
        // 老事件流（无 reason 字段）反序列化时 `reason == None`，
        // 靠 `#[serde(default)]` 保证不报错
        let legacy_json = r#"{
            "type": "run_failed",
            "run_id": "r",
            "subscription_id": "s",
            "error": "some old error"
        }"#;
        let e: ShareSyncEvent = serde_json::from_str(legacy_json).unwrap();
        match e {
            ShareSyncEvent::RunFailed { reason, error, .. } => {
                assert!(reason.is_none());
                assert_eq!(error, "some old error");
            }
            _ => panic!("expected RunFailed"),
        }
    }
}
