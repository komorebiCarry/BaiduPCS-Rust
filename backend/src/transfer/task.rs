// 转存任务定义

use serde::{Deserialize, Serialize};
use super::types::{SharePageInfo, SharedFileInfo};
use uuid::Uuid;

/// 转存任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransferStatus {
    /// 排队中
    Queued,
    /// 检查分享信息
    CheckingShare,
    /// 转存中
    Transferring,
    /// 转存失败
    TransferFailed,
    /// 转存成功（无自动下载）
    Transferred,
    /// 下载中
    Downloading,
    /// 下载失败
    DownloadFailed,
    /// 全部完成
    Completed,
}

impl TransferStatus {
    /// 获取状态的中文描述
    pub fn description(&self) -> &'static str {
        match self {
            TransferStatus::Queued => "排队中",
            TransferStatus::CheckingShare => "检查分享信息",
            TransferStatus::Transferring => "转存中",
            TransferStatus::TransferFailed => "转存失败",
            TransferStatus::Transferred => "已转存",
            TransferStatus::Downloading => "下载中",
            TransferStatus::DownloadFailed => "下载失败",
            TransferStatus::Completed => "已完成",
        }
    }

    /// 是否为终止状态
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransferStatus::TransferFailed
                | TransferStatus::Transferred
                | TransferStatus::DownloadFailed
                | TransferStatus::Completed
        )
    }
}

/// 转存任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferTask {
    /// 任务 ID
    pub id: String,
    /// 分享链接
    pub share_url: String,
    /// 提取码
    pub password: Option<String>,
    /// 网盘保存路径
    pub save_path: String,
    /// 网盘保存目录 fs_id
    pub save_fs_id: u64,
    /// 是否自动下载
    pub auto_download: bool,
    /// 本地下载路径（auto_download=true 时使用）
    pub local_download_path: Option<String>,
    /// 任务状态
    pub status: TransferStatus,
    /// 错误信息
    pub error: Option<String>,
    /// 关联的下载任务 ID 列表
    pub download_task_ids: Vec<String>,
    /// 分享页面信息
    pub share_info: Option<SharePageInfo>,
    /// 分享文件列表
    pub file_list: Vec<SharedFileInfo>,
    /// 已转存文件数
    pub transferred_count: usize,
    /// 总文件数
    pub total_count: usize,
    /// 创建时间 (Unix timestamp)
    pub created_at: i64,
    /// 更新时间 (Unix timestamp)
    pub updated_at: i64,

    // === 下载状态追踪 ===
    /// 下载失败的任务 ID 列表（用于重试）
    #[serde(default)]
    pub failed_download_ids: Vec<String>,
    /// 下载成功的任务 ID 列表
    #[serde(default)]
    pub completed_download_ids: Vec<String>,
    /// 进入 Downloading 状态的时间戳
    #[serde(default)]
    pub download_started_at: Option<i64>,
}

impl TransferTask {
    /// 创建新的转存任务
    pub fn new(
        share_url: String,
        password: Option<String>,
        save_path: String,
        save_fs_id: u64,
        auto_download: bool,
        local_download_path: Option<String>,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: Uuid::new_v4().to_string(),
            share_url,
            password,
            save_path,
            save_fs_id,
            auto_download,
            local_download_path,
            status: TransferStatus::Queued,
            error: None,
            download_task_ids: Vec::new(),
            share_info: None,
            file_list: Vec::new(),
            transferred_count: 0,
            total_count: 0,
            created_at: now,
            updated_at: now,
            failed_download_ids: Vec::new(),
            completed_download_ids: Vec::new(),
            download_started_at: None,
        }
    }

    /// 更新时间戳
    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now().timestamp();
    }

    /// 标记为检查分享信息
    pub fn mark_checking(&mut self) {
        self.status = TransferStatus::CheckingShare;
        self.touch();
    }

    /// 标记为转存中
    pub fn mark_transferring(&mut self) {
        self.status = TransferStatus::Transferring;
        self.touch();
    }

    /// 标记转存失败
    pub fn mark_transfer_failed(&mut self, error: String) {
        self.status = TransferStatus::TransferFailed;
        self.error = Some(error);
        self.touch();
    }

    /// 标记转存成功（无自动下载）
    pub fn mark_transferred(&mut self) {
        self.status = TransferStatus::Transferred;
        self.touch();
    }

    /// 标记为下载中
    pub fn mark_downloading(&mut self, download_task_ids: Vec<String>) {
        self.status = TransferStatus::Downloading;
        self.download_task_ids = download_task_ids;
        self.download_started_at = Some(chrono::Utc::now().timestamp());
        self.touch();
    }

    /// 标记下载失败
    pub fn mark_download_failed(&mut self) {
        self.status = TransferStatus::DownloadFailed;
        self.touch();
    }

    /// 标记全部完成
    pub fn mark_completed(&mut self) {
        self.status = TransferStatus::Completed;
        self.touch();
    }

    /// 设置分享信息
    pub fn set_share_info(&mut self, info: SharePageInfo) {
        self.share_info = Some(info);
        self.touch();
    }

    /// 设置文件列表
    pub fn set_file_list(&mut self, files: Vec<SharedFileInfo>) {
        self.total_count = files.len();
        self.file_list = files;
        self.touch();
    }

    /// 增加已转存计数
    pub fn increment_transferred(&mut self) {
        self.transferred_count += 1;
        self.touch();
    }

    /// 计算转存进度百分比
    pub fn transfer_progress(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            (self.transferred_count as f64 / self.total_count as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let task = TransferTask::new(
            "https://pan.baidu.com/s/1abc".to_string(),
            Some("1234".to_string()),
            "/我的资源".to_string(),
            12345,
            false,
            None,
        );

        assert_eq!(task.status, TransferStatus::Queued);
        assert_eq!(task.transferred_count, 0);
        assert_eq!(task.total_count, 0);
        assert!(task.download_task_ids.is_empty());
    }

    #[test]
    fn test_status_transitions() {
        let mut task = TransferTask::new(
            "https://pan.baidu.com/s/1abc".to_string(),
            None,
            "/".to_string(),
            0,
            true,
            Some("/downloads".to_string()),
        );

        task.mark_checking();
        assert_eq!(task.status, TransferStatus::CheckingShare);

        task.mark_transferring();
        assert_eq!(task.status, TransferStatus::Transferring);

        task.mark_transferred();
        assert_eq!(task.status, TransferStatus::Transferred);

        task.mark_downloading(vec!["dl_1".to_string(), "dl_2".to_string()]);
        assert_eq!(task.status, TransferStatus::Downloading);
        assert_eq!(task.download_task_ids.len(), 2);
        assert!(task.download_started_at.is_some());

        task.mark_completed();
        assert_eq!(task.status, TransferStatus::Completed);
    }

    #[test]
    fn test_progress_calculation() {
        let mut task = TransferTask::new(
            "https://pan.baidu.com/s/1abc".to_string(),
            None,
            "/".to_string(),
            0,
            false,
            None,
        );

        // 初始进度为 0
        assert_eq!(task.transfer_progress(), 0.0);

        // 设置总数
        task.total_count = 10;
        task.transferred_count = 5;
        assert_eq!(task.transfer_progress(), 50.0);

        task.transferred_count = 10;
        assert_eq!(task.transfer_progress(), 100.0);
    }

    #[test]
    fn test_status_is_terminal() {
        assert!(!TransferStatus::Queued.is_terminal());
        assert!(!TransferStatus::CheckingShare.is_terminal());
        assert!(!TransferStatus::Transferring.is_terminal());
        assert!(TransferStatus::TransferFailed.is_terminal());
        assert!(TransferStatus::Transferred.is_terminal());
        assert!(!TransferStatus::Downloading.is_terminal());
        assert!(TransferStatus::DownloadFailed.is_terminal());
        assert!(TransferStatus::Completed.is_terminal());
    }
}
