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
        /// 订阅所属账号；前端据此渲染账号徽章 / 按账号过滤（与其他模块事件一致）。
        /// 用 `serde(default)` 容忍老事件流缺该字段（事件是瞬时的，不持久化）。
        #[serde(default)]
        owner_uid: u64,
    },
    /// 订阅更新
    SubscriptionUpdated {
        subscription_id: String,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 订阅删除
    SubscriptionDeleted {
        subscription_id: String,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 订阅启用/停用
    StatusChanged {
        subscription_id: String,
        enabled: bool,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 一次同步开始
    RunStarted {
        run_id: String,
        subscription_id: String,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 检测到差异
    DiffDetected {
        run_id: String,
        subscription_id: String,
        added: usize,
        modified: usize,
        removed: usize,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 调度的单条 run_item
    ItemScheduled {
        run_id: String,
        subscription_id: String,
        path: String,
        target: String,
        action: String,
        #[serde(default)]
        owner_uid: u64,
    },
    /// run_item 状态变化
    ItemStatusChanged {
        run_id: String,
        subscription_id: String,
        path: String,
        status: String,
        error: Option<String>,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 一次同步完成
    RunCompleted {
        run_id: String,
        subscription_id: String,
        added: usize,
        modified: usize,
        removed: usize,
        failed: usize,
        #[serde(default)]
        owner_uid: u64,
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
    /// 子任务实时进度（转存段 / 下载段）
    ///
    /// 由「每个 run 的进度广播器」周期性推送（约 1s），与自动备份的 `FileProgress` 对齐。
    /// 仅承载某个订阅自己的子任务（按 `backup_config_id="share-sync:{id}"` 归属），
    /// 绝不与自动备份或「下载管理」混淆。
    ItemProgress {
        run_id: String,
        subscription_id: String,
        /// 底层任务 id（下载任务 id 或内部转存任务 id），前端据此做 upsert 去重
        task_id: String,
        /// 文件名 / 展示名
        name: String,
        /// 子任务种类:`"transfer"`(转存段) | `"download"`(下载段)
        kind: String,
        /// 子任务状态字符串(downloading / completed / failed / transferring ...)
        status: String,
        /// 已完成字节(下载段);转存段用已完成文件数
        downloaded: u64,
        /// 总字节(下载段);转存段用总文件数
        total: u64,
        /// 进度百分比 0-100
        progress: f64,
        /// 瞬时速度(B/s,仅下载段有意义)
        speed: u64,
        /// 预计剩余时间(秒,仅下载段且 speed>0 时有值)，与自动备份对齐
        #[serde(default)]
        eta_seconds: Option<u64>,
        #[serde(default)]
        owner_uid: u64,
    },
    /// 一次同步失败
    RunFailed {
        run_id: String,
        subscription_id: String,
        error: String,
        #[serde(default)]
        owner_uid: u64,
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
            | Self::SubscriptionUpdated {
                subscription_id, ..
            }
            | Self::SubscriptionDeleted {
                subscription_id, ..
            }
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
            | Self::ItemProgress {
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

    /// 关联的 owner_uid（订阅所属账号；前端据此渲染徽章 / 过滤）
    pub fn owner_uid(&self) -> u64 {
        match self {
            Self::SubscriptionCreated { owner_uid, .. }
            | Self::SubscriptionUpdated { owner_uid, .. }
            | Self::SubscriptionDeleted { owner_uid, .. }
            | Self::StatusChanged { owner_uid, .. }
            | Self::RunStarted { owner_uid, .. }
            | Self::DiffDetected { owner_uid, .. }
            | Self::ItemScheduled { owner_uid, .. }
            | Self::ItemStatusChanged { owner_uid, .. }
            | Self::ItemProgress { owner_uid, .. }
            | Self::RunCompleted { owner_uid, .. }
            | Self::RunFailed { owner_uid, .. } => *owner_uid,
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
            Self::ItemProgress { .. } => "item_progress",
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
            owner_uid: 7,
        };
        assert_eq!(e.subscription_id(), "s1");
        assert_eq!(e.owner_uid(), 7);
    }

    #[test]
    fn test_event_type_name() {
        let e = ShareSyncEvent::DiffDetected {
            run_id: "r".into(),
            subscription_id: "s".into(),
            added: 1,
            modified: 2,
            removed: 3,
            owner_uid: 0,
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
            owner_uid: 5,
            duration_ms: Some(1234),
            n_bisects: Some(0),
            max_bisect_depth: Some(0),
        };
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"run_completed\""));
        assert!(json.contains("\"owner_uid\":5"));
    }

    #[test]
    fn test_item_progress_serialize() {
        // PR-D：子任务实时进度事件，走 share_sync 频道（type=item_progress）
        let e = ShareSyncEvent::ItemProgress {
            run_id: "r".into(),
            subscription_id: "s1".into(),
            task_id: "dl-1".into(),
            name: "a.txt".into(),
            kind: "download".into(),
            status: "downloading".into(),
            downloaded: 50,
            total: 100,
            progress: 50.0,
            speed: 1024,
            eta_seconds: Some(50),
            owner_uid: 9,
        };
        assert_eq!(e.subscription_id(), "s1");
        assert_eq!(e.owner_uid(), 9);
        assert_eq!(e.event_type_name(), "item_progress");
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"type\":\"item_progress\""));
        assert!(json.contains("\"task_id\":\"dl-1\""));
        assert!(json.contains("\"kind\":\"download\""));
        // 反序列化回环
        let back: ShareSyncEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event_type_name(), "item_progress");
    }

    #[test]
    fn test_run_failed_with_reason() {
        // v1 新增：带 reason 的 RunFailed 序列化
        let e = ShareSyncEvent::RunFailed {
            run_id: "r".into(),
            subscription_id: "s".into(),
            error: "网盘空间不足".into(),
            owner_uid: 0,
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
