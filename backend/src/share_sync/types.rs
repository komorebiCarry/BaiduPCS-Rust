//! 分享同步公共枚举与运行结果类型

use serde::{Deserialize, Serialize};

/// 冲突处理策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictStrategy {
    /// 覆盖式：用新文件覆盖目标中的同名文件
    Overwrite,
    /// 新版本式：保留旧文件（重命名为带时间戳后缀），写入新文件
    Versioned,
    /// 跳过：目标已存在则不处理，仅同步新增/修改后的真正新文件
    Skip,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::Overwrite
    }
}

impl std::fmt::Display for ConflictStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictStrategy::Overwrite => write!(f, "overwrite"),
            ConflictStrategy::Versioned => write!(f, "versioned"),
            ConflictStrategy::Skip => write!(f, "skip"),
        }
    }
}

/// 目标种类
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    /// 转存到网盘
    Netdisk,
    /// 下载到本地
    Local,
}

impl std::fmt::Display for TargetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetKind::Netdisk => write!(f, "netdisk"),
            TargetKind::Local => write!(f, "local"),
        }
    }
}

/// 轮询模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollMode {
    /// 固定间隔
    #[serde(alias = "interval")]
    Interval,
    /// 指定时间（每天固定时刻）
    #[serde(alias = "scheduled")]
    Scheduled,
    /// 禁用
    #[serde(alias = "disabled")]
    Disabled,
}

impl Default for PollMode {
    fn default() -> Self {
        PollMode::Interval
    }
}

/// 单次同步动作
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncAction {
    Added,
    Modified,
    Removed,
    Skipped,
}

impl std::fmt::Display for SyncAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncAction::Added => write!(f, "added"),
            SyncAction::Modified => write!(f, "modified"),
            SyncAction::Removed => write!(f, "removed"),
            SyncAction::Skipped => write!(f, "skipped"),
        }
    }
}

/// 单个 run item 的处理状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunItemStatus {
    /// 等待调度
    Pending,
    /// 转存中（网盘目标）
    Transferring,
    /// 下载中（本地目标）
    Downloading,
    /// 删除中
    Deleting,
    /// 已完成
    Completed,
    /// 失败
    Failed,
    /// 跳过（策略 Skip / delete_missing=false）
    Skipped,
}

impl std::fmt::Display for RunItemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RunItemStatus::Pending => "pending",
            RunItemStatus::Transferring => "transferring",
            RunItemStatus::Downloading => "downloading",
            RunItemStatus::Deleting => "deleting",
            RunItemStatus::Completed => "completed",
            RunItemStatus::Failed => "failed",
            RunItemStatus::Skipped => "skipped",
        };
        write!(f, "{}", s)
    }
}

/// 运行状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// 执行中
    Running,
    /// 全部完成
    Completed,
    /// 部分失败
    CompletedWithErrors,
    /// 整体失败（启动阶段就出错）
    Failed,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::CompletedWithErrors => "completed_with_errors",
            RunStatus::Failed => "failed",
        };
        write!(f, "{}", s)
    }
}

/// 差异摘要（持久化到 `share_sync_runs`）
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiffSummary {
    /// 本次对比涉及的文件总数（新增 + 修改 + 删除 + 未变化，不含目录）
    #[serde(default)]
    pub total: usize,
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
    /// 和上次成功同步快照一致、无需执行动作的文件数（不含目录）
    #[serde(default)]
    pub unchanged: usize,
    pub failed: usize,
    /// 覆盖已有目标文件的动作数
    #[serde(default)]
    pub overwritten: usize,
    /// 因 quota / local_disk_full 等"环境资源不足"被跳过的子项数（v1 新增）
    ///
    /// 这些项**不是失败**——失败意味着可重试 / 需修复配置；
    /// 跳过意味着"用户清理网盘/本地磁盘后手动重跑即可"，对应 `run_item.reason`
    /// 字段为 `quota_full` / `local_disk_full`。
    ///
    /// 前端展示建议：红色 = failed（需关注），黄色 = skipped（信息性）。
    #[serde(default)]
    pub skipped: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict_strategy_default() {
        assert_eq!(ConflictStrategy::default(), ConflictStrategy::Overwrite);
    }

    #[test]
    fn test_conflict_strategy_serialize() {
        for s in [
            ConflictStrategy::Overwrite,
            ConflictStrategy::Versioned,
            ConflictStrategy::Skip,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let back: ConflictStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn test_target_kind_serialize() {
        for k in [TargetKind::Netdisk, TargetKind::Local] {
            let json = serde_json::to_string(&k).unwrap();
            let back: TargetKind = serde_json::from_str(&json).unwrap();
            assert_eq!(k, back);
        }
    }

    #[test]
    fn test_run_status_serialize() {
        for s in [
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::CompletedWithErrors,
            RunStatus::Failed,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let back: RunStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn test_poll_mode_default() {
        assert_eq!(PollMode::default(), PollMode::Interval);
    }

    #[test]
    fn test_diff_summary_default() {
        let s = DiffSummary::default();
        assert_eq!(s.added, 0);
        assert_eq!(s.modified, 0);
        assert_eq!(s.removed, 0);
        assert_eq!(s.failed, 0);
        assert_eq!(s.skipped, 0);
    }

    #[test]
    fn test_diff_summary_serde_skipped_default() {
        // 老数据无 skipped 字段时反序列化默认为 0（向后兼容）
        let json = r#"{"added":1,"modified":0,"removed":0,"failed":0}"#;
        let s: DiffSummary = serde_json::from_str(json).unwrap();
        assert_eq!(s.skipped, 0);
    }

    #[test]
    fn test_run_item_status_display() {
        assert_eq!(RunItemStatus::Pending.to_string(), "pending");
        assert_eq!(RunItemStatus::Transferring.to_string(), "transferring");
        assert_eq!(RunItemStatus::Downloading.to_string(), "downloading");
        assert_eq!(RunItemStatus::Completed.to_string(), "completed");
        assert_eq!(RunItemStatus::Failed.to_string(), "failed");
        assert_eq!(RunItemStatus::Skipped.to_string(), "skipped");
    }
}
