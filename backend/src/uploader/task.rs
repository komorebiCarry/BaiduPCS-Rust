// 上传任务定义
//
// 复用 DownloadTask 的设计模式

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// 上传任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UploadTaskStatus {
    /// 等待中
    Pending,
    /// 秒传检查中
    CheckingRapid,
    /// 上传中
    Uploading,
    /// 已暂停
    Paused,
    /// 已完成
    Completed,
    /// 秒传成功
    RapidUploadSuccess,
    /// 失败
    Failed,
}

/// 上传任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadTask {
    /// 任务ID
    pub id: String,
    /// 本地文件路径
    pub local_path: PathBuf,
    /// 网盘目标路径
    pub remote_path: String,
    /// 文件大小
    pub total_size: u64,
    /// 已上传大小
    pub uploaded_size: u64,
    /// 任务状态
    pub status: UploadTaskStatus,
    /// 上传速度 (bytes/s)
    pub speed: u64,
    /// 创建时间 (Unix timestamp)
    pub created_at: i64,
    /// 开始时间 (Unix timestamp)
    pub started_at: Option<i64>,
    /// 完成时间 (Unix timestamp)
    pub completed_at: Option<i64>,
    /// 错误信息
    pub error: Option<String>,

    // === 秒传相关字段 ===
    /// 是否为秒传上传
    #[serde(default)]
    pub is_rapid_upload: bool,
    /// 文件 MD5（用于秒传检查）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_md5: Option<String>,
    /// 文件前 256KB MD5（用于秒传检查）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slice_md5: Option<String>,
    /// 文件 CRC32（用于秒传检查）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_crc32: Option<String>,

    // === 文件夹上传相关字段 ===
    /// 文件夹上传组ID，单文件上传时为 None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// 文件夹根路径（本地），如 "D:/uploads/photos"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_root: Option<String>,
    /// 相对于根文件夹的路径，如 "2024/01/photo.jpg"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,

    // === 分片信息字段 ===
    /// 总分片数
    #[serde(default)]
    pub total_chunks: usize,
    /// 已完成分片数
    #[serde(default)]
    pub completed_chunks: usize,
}

impl UploadTask {
    /// 创建新的上传任务
    pub fn new(local_path: PathBuf, remote_path: String, total_size: u64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            local_path,
            remote_path,
            total_size,
            uploaded_size: 0,
            status: UploadTaskStatus::Pending,
            speed: 0,
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            error: None,
            is_rapid_upload: false,
            content_md5: None,
            slice_md5: None,
            content_crc32: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            total_chunks: 0,
            completed_chunks: 0,
        }
    }

    /// 创建带文件夹组信息的任务
    pub fn new_with_group(
        local_path: PathBuf,
        remote_path: String,
        total_size: u64,
        group_id: String,
        group_root: String,
        relative_path: String,
    ) -> Self {
        let mut task = Self::new(local_path, remote_path, total_size);
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
        (self.uploaded_size as f64 / self.total_size as f64) * 100.0
    }

    /// 估算剩余时间 (秒)
    pub fn eta(&self) -> Option<u64> {
        if self.speed == 0 || self.uploaded_size >= self.total_size {
            return None;
        }
        let remaining = self.total_size - self.uploaded_size;
        Some(remaining / self.speed)
    }

    /// 标记为秒传检查中
    pub fn mark_checking_rapid(&mut self) {
        self.status = UploadTaskStatus::CheckingRapid;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 标记为上传中
    pub fn mark_uploading(&mut self) {
        self.status = UploadTaskStatus::Uploading;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 标记为已完成
    pub fn mark_completed(&mut self) {
        self.status = UploadTaskStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
        self.uploaded_size = self.total_size;
    }

    /// 标记为秒传成功
    pub fn mark_rapid_upload_success(&mut self) {
        self.status = UploadTaskStatus::RapidUploadSuccess;
        self.completed_at = Some(chrono::Utc::now().timestamp());
        self.uploaded_size = self.total_size;
        self.is_rapid_upload = true;
    }

    /// 标记为失败
    pub fn mark_failed(&mut self, error: String) {
        self.status = UploadTaskStatus::Failed;
        self.error = Some(error);
    }

    /// 标记为暂停
    pub fn mark_paused(&mut self) {
        self.status = UploadTaskStatus::Paused;
    }

    /// 设置秒传哈希值
    pub fn set_rapid_hash(&mut self, content_md5: String, slice_md5: String, content_crc32: Option<String>) {
        self.content_md5 = Some(content_md5);
        self.slice_md5 = Some(slice_md5);
        self.content_crc32 = content_crc32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let task = UploadTask::new(
            PathBuf::from("./test/file.txt"),
            "/test/file.txt".to_string(),
            1024 * 1024, // 1MB
        );

        assert_eq!(task.status, UploadTaskStatus::Pending);
        assert_eq!(task.uploaded_size, 0);
        assert_eq!(task.progress(), 0.0);
        assert!(!task.is_rapid_upload);
    }

    #[test]
    fn test_progress_calculation() {
        let mut task = UploadTask::new(
            PathBuf::from("./test"),
            "/test".to_string(),
            1000,
        );

        task.uploaded_size = 250;
        assert_eq!(task.progress(), 25.0);

        task.uploaded_size = 500;
        assert_eq!(task.progress(), 50.0);

        task.uploaded_size = 1000;
        assert_eq!(task.progress(), 100.0);
    }

    #[test]
    fn test_eta_calculation() {
        let mut task = UploadTask::new(
            PathBuf::from("./test"),
            "/test".to_string(),
            1000,
        );

        task.uploaded_size = 200;
        task.speed = 100; // 100 bytes/s
        assert_eq!(task.eta(), Some(8)); // (1000 - 200) / 100 = 8s

        task.speed = 0;
        assert_eq!(task.eta(), None); // 速度为0，无法估算
    }

    #[test]
    fn test_status_transitions() {
        let mut task = UploadTask::new(
            PathBuf::from("./test"),
            "/test".to_string(),
            1000,
        );

        task.mark_checking_rapid();
        assert_eq!(task.status, UploadTaskStatus::CheckingRapid);
        assert!(task.started_at.is_some());

        task.mark_uploading();
        assert_eq!(task.status, UploadTaskStatus::Uploading);

        task.mark_paused();
        assert_eq!(task.status, UploadTaskStatus::Paused);

        task.mark_failed("Network error".to_string());
        assert_eq!(task.status, UploadTaskStatus::Failed);
        assert_eq!(task.error, Some("Network error".to_string()));

        task.mark_completed();
        assert_eq!(task.status, UploadTaskStatus::Completed);
        assert_eq!(task.uploaded_size, task.total_size);
        assert!(task.completed_at.is_some());
    }

    #[test]
    fn test_rapid_upload_success() {
        let mut task = UploadTask::new(
            PathBuf::from("./test"),
            "/test".to_string(),
            1000,
        );

        task.set_rapid_hash(
            "abc123".to_string(),
            "def456".to_string(),
            Some("12345678".to_string()),
        );

        task.mark_rapid_upload_success();
        assert_eq!(task.status, UploadTaskStatus::RapidUploadSuccess);
        assert!(task.is_rapid_upload);
        assert_eq!(task.uploaded_size, task.total_size);
        assert!(task.completed_at.is_some());
    }
}
