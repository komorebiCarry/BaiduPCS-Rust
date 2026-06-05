//! 同步模块数据类型
//!
//! 定义 Sync 三阶段所需的数据结构：快照类型、同步计划、动作、状态更新等。

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 同步动作方向（对应 sync_state.last_sync_direction）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    Upload,
    Download,
    Skip,
    /// 无传输的纯状态写入（tombstone、adopt、mtime 回填等）
    StateOnly,
}

impl SyncDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncDirection::Upload => "upload",
            SyncDirection::Download => "download",
            SyncDirection::Skip => "skip",
            SyncDirection::StateOnly => "state_only",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "upload" => Some(SyncDirection::Upload),
            "download" => Some(SyncDirection::Download),
            "skip" => Some(SyncDirection::Skip),
            "state_only" => Some(SyncDirection::StateOnly),
            _ => None,
        }
    }
}

/// 本地扫描结果（Stage 1a 输出）
///
/// 扩展了 mtime 字段（区别于 uploader::folder::ScannedFile）。
/// Sync 模式需要 mtime 做增量比对和 stale plan check。
#[derive(Debug, Clone)]
pub struct LocalScannedFile {
    /// 相对路径（基于 local_path）
    pub relative_path: String,
    /// 本地绝对路径
    pub local_path: PathBuf,
    /// 文件大小
    pub size: u64,
    /// 修改时间（秒级时间戳）
    pub mtime: i64,
}

/// 远端扫描结果（Stage 1b 输出）
///
/// 从 `NetdiskClient.get_file_list` 的 `FileItem` 映射而来。
#[derive(Debug, Clone)]
pub struct RemoteScannedFile {
    /// 相对路径（基于 remote_path）
    pub relative_path: String,
    /// 百度网盘完整路径
    pub remote_path: String,
    /// 文件大小
    pub size: u64,
    /// server_mtime（秒级）
    pub mtime: i64,
    /// 百度网盘文件 ID
    pub fs_id: u64,
    /// 远端 md5（部分场景可用，用于 Case A3 对齐）
    pub md5: Option<String>,
}

/// 同步完成后的双侧观测状态
///
/// 同时用于传输成功后的双侧写入（Case B）和纯状态更新（Case C tombstone / Case A adopt）。
/// 字段为 Option：None 表示该侧文件不存在，或该字段当时无法获取。
#[derive(Debug, Clone)]
pub struct ObservedFileState {
    pub local_mtime: Option<i64>,
    pub local_size: Option<u64>,
    pub local_exists: bool,
    pub remote_mtime: Option<i64>,
    pub remote_size: Option<u64>,
    pub remote_fs_id: Option<u64>,
    pub remote_exists: bool,
    pub direction: SyncDirection,
}

/// 同步计划（Stage 2 的输出）
#[derive(Debug)]
pub struct SyncPlan {
    /// 需要上传的文件列表
    pub uploads: Vec<SyncUploadAction>,
    /// 需要下载的文件列表
    pub downloads: Vec<SyncDownloadAction>,
    /// 无传输的纯状态写入（tombstone、adopt、skip 等）
    pub state_updates: Vec<SyncStateUpdate>,
    /// 冲突记录（仅 strategy=Skip 时有内容）
    pub conflicts: Vec<SyncConflictRecord>,
    /// 完全无变化的文件数
    pub skipped: usize,
}

impl SyncPlan {
    pub fn empty() -> Self {
        Self {
            uploads: Vec::new(),
            downloads: Vec::new(),
            state_updates: Vec::new(),
            conflicts: Vec::new(),
            skipped: 0,
        }
    }

    pub fn total_actions(&self) -> usize {
        self.uploads.len() + self.downloads.len() + self.state_updates.len()
    }
}

/// 上传动作
#[derive(Debug, Clone)]
pub struct SyncUploadAction {
    pub relative_path: String,
    pub local_path: PathBuf,
    pub local_mtime: i64,
    pub local_size: u64,
}

/// 下载动作
#[derive(Debug, Clone)]
pub struct SyncDownloadAction {
    pub relative_path: String,
    pub remote_path: String,
    pub remote_mtime: i64,
    pub remote_size: u64,
    pub fs_id: u64,
}

/// 纯状态更新（不产生传输任务）
#[derive(Debug, Clone)]
pub struct SyncStateUpdate {
    pub relative_path: String,
    pub observed: ObservedFileState,
    pub reason: StateUpdateReason,
    /// 元字段增量更新（仅写非 None 的字段，None 表示不修改该列）
    pub meta: Option<SyncStateMeta>,
}

/// 同步状态行的管理标记（非文件观测值）
#[derive(Debug, Clone)]
pub struct SyncStateMeta {
    /// C5 半冻结标记。Some(true)=置位，Some(false)=清除，None=不修改
    pub c5_detected: Option<bool>,
    /// tombstone 建立时间。Some(ts)=写入，None=不修改
    /// C1/C2 时写入当前时间戳；C4 传输成功后清为 None（由 update_after_sync 处理）
    pub tombstoned_at: Option<Option<i64>>,
}

/// 状态更新原因
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateUpdateReason {
    /// Case A3 + AdoptBothSides：首次建基线
    AdoptBaseline,
    /// Case A3 + md5 一致：首次建基线
    Md5Match,
    /// Case C1/C2：单边删除 tombstone
    Tombstone,
    /// Case C3：两端都删除，清除 SyncState
    BothDeleted,
    /// strategy=Skip 的冲突
    ConflictSkipped,
    /// Case B NULL mtime 回填
    MtimeBackfill,
}

/// 冲突记录（strategy=Skip 时记录日志用）
#[derive(Debug, Clone)]
pub struct SyncConflictRecord {
    pub relative_path: String,
    pub local_mtime: i64,
    pub local_size: u64,
    pub remote_mtime: i64,
    pub remote_size: u64,
}

/// SyncState 行（从 SQLite 读取）
#[derive(Debug, Clone)]
pub struct SyncStateRow {
    pub id: i64,
    pub config_id: String,
    pub relative_path: String,
    pub local_mtime: Option<i64>,
    pub local_size: Option<i64>,
    pub local_exists: bool,
    pub remote_mtime: Option<i64>,
    pub remote_size: Option<i64>,
    pub remote_fs_id: Option<i64>,
    pub remote_exists: bool,
    pub last_sync_at: i64,
    pub last_sync_direction: Option<String>,
    pub c5_detected: bool,
    pub tombstoned_at: Option<i64>,
}

/// Tombstone 信息（API 查询用）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneInfo {
    pub relative_path: String,
    pub missing_side: String,
    pub tombstoned_at: i64,
    pub c5_detected: bool,
    /// 存活侧最后已知的 mtime
    pub surviving_mtime: Option<i64>,
    /// 存活侧最后已知的 size
    pub surviving_size: Option<i64>,
}
