use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// 下载任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    /// 等待中
    Pending,
    /// 下载中
    Downloading,
    /// 已暂停
    Paused,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

/// 下载任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    /// 任务ID
    pub id: String,
    /// 文件服务器ID
    pub fs_id: u64,
    /// 网盘路径
    pub remote_path: String,
    /// 本地保存路径
    pub local_path: PathBuf,
    /// 文件大小
    pub total_size: u64,
    /// 已下载大小
    pub downloaded_size: u64,
    /// 任务状态
    pub status: TaskStatus,
    /// 下载速度 (bytes/s)
    pub speed: u64,
    /// 创建时间 (Unix timestamp)
    pub created_at: i64,
    /// 开始时间 (Unix timestamp)
    pub started_at: Option<i64>,
    /// 完成时间 (Unix timestamp)
    pub completed_at: Option<i64>,
    /// 错误信息
    pub error: Option<String>,

    // === 文件夹下载相关字段 ===
    /// 文件夹下载组ID，单文件下载时为 None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// 文件夹根路径，如 "/电影"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_root: Option<String>,
    /// 相对于根文件夹的路径，如 "科幻片/星际穿越.mp4"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,

    // === 🔥 新增：跨任务跳转相关字段 ===
    /// 关联的转存任务 ID（如果此下载任务由转存任务自动创建）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_task_id: Option<String>,

    // === 🔥 新增：任务位借调机制相关字段 ===
    /// 占用的槽位ID
    #[serde(skip)]
    pub slot_id: Option<usize>,

    /// 是否使用借调位（而非固定位）
    #[serde(skip)]
    pub is_borrowed_slot: bool,
}

impl DownloadTask {
    pub fn new(fs_id: u64, remote_path: String, local_path: PathBuf, total_size: u64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            fs_id,
            remote_path,
            local_path,
            total_size,
            downloaded_size: 0,
            status: TaskStatus::Pending,
            speed: 0,
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            error: None,
            // 文件夹下载字段默认为 None
            group_id: None,
            group_root: None,
            relative_path: None,
            // 转存任务关联字段默认为 None
            transfer_task_id: None,
            // 任务位借调机制字段初始化
            slot_id: None,
            is_borrowed_slot: false,
        }
    }

    /// 设置关联的转存任务 ID
    pub fn set_transfer_task_id(&mut self, transfer_task_id: String) {
        self.transfer_task_id = Some(transfer_task_id);
    }

    /// 创建带文件夹组信息的任务
    pub fn new_with_group(
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        group_id: String,
        group_root: String,
        relative_path: String,
    ) -> Self {
        let mut task = Self::new(fs_id, remote_path, local_path, total_size);
        task.group_id = Some(group_id);
        task.group_root = Some(group_root);
        task.relative_path = Some(relative_path);
        task
    }

    /// 计算进度百分比
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.downloaded_size as f64 / self.total_size as f64) * 100.0
    }

    /// 估算剩余时间 (秒)
    pub fn eta(&self) -> Option<u64> {
        if self.speed == 0 || self.downloaded_size >= self.total_size {
            return None;
        }
        let remaining = self.total_size - self.downloaded_size;
        Some(remaining / self.speed)
    }

    /// 标记为下载中
    pub fn mark_downloading(&mut self) {
        self.status = TaskStatus::Downloading;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 标记为已完成
    pub fn mark_completed(&mut self) {
        self.status = TaskStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
        self.downloaded_size = self.total_size;
    }

    /// 标记为失败
    pub fn mark_failed(&mut self, error: String) {
        self.status = TaskStatus::Failed;
        self.error = Some(error);
    }

    /// 标记为暂停
    pub fn mark_paused(&mut self) {
        self.status = TaskStatus::Paused;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let task = DownloadTask::new(
            12345,
            "/test/file.txt".to_string(),
            PathBuf::from("./downloads/file.txt"),
            1024 * 1024, // 1MB
        );

        assert_eq!(task.fs_id, 12345);
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.downloaded_size, 0);
        assert_eq!(task.progress(), 0.0);
    }

    #[test]
    fn test_progress_calculation() {
        let mut task = DownloadTask::new(1, "/test".to_string(), PathBuf::from("./test"), 1000);

        task.downloaded_size = 250;
        assert_eq!(task.progress(), 25.0);

        task.downloaded_size = 500;
        assert_eq!(task.progress(), 50.0);

        task.downloaded_size = 1000;
        assert_eq!(task.progress(), 100.0);
    }

    #[test]
    fn test_eta_calculation() {
        let mut task = DownloadTask::new(1, "/test".to_string(), PathBuf::from("./test"), 1000);

        task.downloaded_size = 200;
        task.speed = 100; // 100 bytes/s
        assert_eq!(task.eta(), Some(8)); // (1000 - 200) / 100 = 8s

        task.speed = 0;
        assert_eq!(task.eta(), None); // 速度为0，无法估算
    }

    #[test]
    fn test_status_transitions() {
        let mut task = DownloadTask::new(1, "/test".to_string(), PathBuf::from("./test"), 1000);

        task.mark_downloading();
        assert_eq!(task.status, TaskStatus::Downloading);
        assert!(task.started_at.is_some());

        task.mark_paused();
        assert_eq!(task.status, TaskStatus::Paused);

        task.mark_failed("Network error".to_string());
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.error, Some("Network error".to_string()));

        task.mark_completed();
        assert_eq!(task.status, TaskStatus::Completed);
        assert_eq!(task.downloaded_size, task.total_size);
        assert!(task.completed_at.is_some());
    }
}
