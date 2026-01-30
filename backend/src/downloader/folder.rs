//! 文件夹下载数据结构

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use uuid::Uuid;

/// 文件夹下载状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FolderStatus {
    /// 正在扫描文件夹
    Scanning,
    /// 扫描完成，正在下载
    Downloading,
    /// 已暂停
    Paused,
    /// 全部完成
    Completed,
    /// 失败
    Failed,
    /// 已取消
    Cancelled,
}

/// 待下载的文件信息（扫描结果）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingFile {
    pub fs_id: u64,
    pub filename: String,
    pub remote_path: String,
    pub relative_path: String,
    pub size: u64,
}

/// 文件夹下载任务组
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderDownload {
    /// 文件夹ID
    pub id: String,
    /// 文件夹名称
    pub name: String,
    /// 网盘根路径
    pub remote_root: String,
    /// 本地根路径
    pub local_root: PathBuf,
    /// 状态
    pub status: FolderStatus,
    /// 总文件数
    pub total_files: u64,
    /// 总大小
    pub total_size: u64,
    /// 已创建任务数
    pub created_count: u64,
    /// 已完成任务数
    pub completed_count: u64,
    /// 已下载大小
    pub downloaded_size: u64,
    /// 待下载的文件队列（扫描发现但还未创建下载任务）
    /// 跳过序列化，避免 API 返回大量数据
    #[serde(default, skip_serializing)]
    pub pending_files: Vec<PendingFile>,
    /// 扫描是否完成
    #[serde(default)]
    pub scan_completed: bool,
    /// 扫描进度（当前扫描到的目录）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_progress: Option<String>,
    /// 创建时间
    pub created_at: i64,
    /// 开始时间
    pub started_at: Option<i64>,
    /// 完成时间
    pub completed_at: Option<i64>,
    /// 错误信息
    pub error: Option<String>,
    /// 🔥 关联的转存任务 ID（如果此文件夹下载任务由转存任务自动创建）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_task_id: Option<String>,

    // === 🔥 新增：任务位借调机制相关字段 ===
    /// 固定任务位ID（文件夹主任务位）
    #[serde(skip)]
    pub fixed_slot_id: Option<usize>,

    /// 借调任务位ID列表（用于子任务并行）
    #[serde(skip)]
    pub borrowed_slot_ids: Vec<usize>,

    /// 正在使用借调位的子任务ID映射（task_id -> slot_id）
    #[serde(skip)]
    pub borrowed_subtask_map: HashMap<String, usize>,

    /// 🔥 加密文件夹映射（加密相对路径 -> 解密后相对路径）
    /// 用于在扫描完成后重命名文件夹并更新路径
    #[serde(default, skip)]
    pub encrypted_folder_mappings: HashMap<String, String>,

    /// 🔥 已计数的任务ID集合（用于避免重复计数 completed_count）
    /// 解决问题：使用固定位的子任务完成时也需要递增 completed_count
    #[serde(default, skip)]
    pub counted_task_ids: HashSet<String>,
}

impl FolderDownload {
    /// 创建新的文件夹下载
    ///
    /// 🔥 修复：从 local_root 提取文件夹名称（解密后的原始名称）
    /// 而不是从 remote_root（可能是加密的 BPR_DIR_xxx 格式）
    pub fn new(remote_root: String, local_root: PathBuf) -> Self {
        // 优先从 local_root 提取名称（已解密的原始名称）
        let name = local_root
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self {
            id: Uuid::new_v4().to_string(),
            name,
            remote_root,
            local_root,
            status: FolderStatus::Scanning,
            total_files: 0,
            total_size: 0,
            created_count: 0,
            completed_count: 0,
            downloaded_size: 0,
            pending_files: Vec::new(),
            scan_completed: false,
            scan_progress: None,
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            error: None,
            transfer_task_id: None,
            // 任务位借调机制字段初始化
            fixed_slot_id: None,
            borrowed_slot_ids: Vec::new(),
            borrowed_subtask_map: HashMap::new(),
            encrypted_folder_mappings: HashMap::new(),
            counted_task_ids: HashSet::new(),
        }
    }

    /// 计算进度百分比
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.downloaded_size as f64 / self.total_size as f64) * 100.0
    }

    /// 标记为下载中
    pub fn mark_downloading(&mut self) {
        self.status = FolderStatus::Downloading;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 标记为已完成
    pub fn mark_completed(&mut self) {
        self.status = FolderStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
    }

    /// 标记为失败
    pub fn mark_failed(&mut self, error: String) {
        self.status = FolderStatus::Failed;
        self.error = Some(error);
    }

    /// 标记为暂停
    pub fn mark_paused(&mut self) {
        self.status = FolderStatus::Paused;
    }

    /// 标记为取消
    pub fn mark_cancelled(&mut self) {
        self.status = FolderStatus::Cancelled;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_folder_download_creation() {
        let folder = FolderDownload::new(
            "/test/folder".to_string(),
            PathBuf::from("./downloads/folder"),
        );

        assert_eq!(folder.name, "folder");
        assert_eq!(folder.status, FolderStatus::Scanning);
        assert_eq!(folder.total_files, 0);
        assert_eq!(folder.progress(), 0.0);
    }

    #[test]
    fn test_progress_calculation() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));

        folder.total_size = 1000;
        folder.downloaded_size = 250;
        assert_eq!(folder.progress(), 25.0);

        folder.downloaded_size = 500;
        assert_eq!(folder.progress(), 50.0);
    }

    #[test]
    fn test_status_transitions() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));

        folder.mark_downloading();
        assert_eq!(folder.status, FolderStatus::Downloading);
        assert!(folder.started_at.is_some());

        folder.mark_paused();
        assert_eq!(folder.status, FolderStatus::Paused);

        folder.mark_failed("Network error".to_string());
        assert_eq!(folder.status, FolderStatus::Failed);
        assert_eq!(folder.error, Some("Network error".to_string()));

        folder.mark_completed();
        assert_eq!(folder.status, FolderStatus::Completed);
        assert!(folder.completed_at.is_some());
    }
}
