//! 分享同步子系统
//!
//! ## 角色
//!
//! 订阅第三方分享链接 → 周期性抓取文件列表 → 与上次快照做 diff
//! → 按用户选择的「覆盖式 / 新版本式 / 跳过」策略把差异应用到
//! 网盘目录和/或本地目录。

pub mod config;
pub mod diff;
pub mod error;
pub mod events;
pub mod executor;
pub mod manager;
pub mod persistence;
pub mod rate_limit;
pub mod scheduler;
pub mod snapshot;
pub mod tree;
pub mod types;

pub use config::{
    CreateShareSubscriptionRequest, LocalTarget, NetdiskTarget, PollConfig, ShareSubscription,
    SyncTarget, UpdateShareSubscriptionRequest, MIN_POLL_INTERVAL_SECS,
};
pub use diff::{diff_snapshots, DiffSummaryView, ShareDiff, ShareModifiedItem};
pub use error::{ErrorCategory, ShareSyncError};
pub use events::{ShareSyncEvent, ShareSyncEventPublisher};
pub use executor::{timestamped_name, ApplyOutcome, ExecutorHooks, ShareSyncExecutor};
pub use manager::{ManagerConfig, ShareSyncManager};
pub use persistence::{
    normalize_pagination, RunItemRecord, RunRecord, ShareSyncPersistence, DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
};
pub use snapshot::{
    infer_share_root, normalize_share_path, CapturedShare, ShareSnapshot, ShareSnapshotItem,
    SnapshotCollector,
};
pub use types::{
    ConflictStrategy, DiffSummary, PollMode, RunItemStatus, RunStatus, SyncAction, TargetKind,
};
