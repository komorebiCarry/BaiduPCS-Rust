// 转存任务定义

use super::types::{SharePageInfo, SharedFileInfo};
use serde::{Deserialize, Serialize};
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
    /// 清理临时文件中（分享直下专用）
    Cleaning,
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
            TransferStatus::Cleaning => "清理临时文件中",
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
    /// 自动下载时使用的本地文件冲突策略；None 时使用全局下载默认策略
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
    /// 任务状态
    pub status: TransferStatus,
    /// 错误信息
    pub error: Option<String>,
    /// 关联的下载任务 ID 列表
    pub download_task_ids: Vec<String>,
    /// 分享页面信息
    pub share_info: Option<SharePageInfo>,
    /// 当前分享提取码验证返回的 randsk。
    ///
    /// 百度只用一个 Cookie 名保存 randsk，并发处理多个带密码分享时会互相覆盖；
    /// 任务执行阶段需要保存自己的值并显式带到请求里。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub randsk: Option<String>,
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

    // === 🔥 新增：跨任务跳转相关字段 ===
    /// 转存文件名称（用于展示，从分享文件列表中提取）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,

    // === 分享直下相关字段 ===
    /// 是否为分享直下任务
    #[serde(default)]
    pub is_share_direct_download: bool,
    /// 临时目录路径（网盘路径，分享直下专用，用于清理）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temp_dir: Option<String>,
    /// 用户选择的文件 fs_id 列表（可选，用于选择性转存）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_fs_ids: Option<Vec<u64>>,
    /// 用户选择的文件完整信息列表（可选，用于获取选中文件的元信息）
    /// 解决子目录选择场景下后端无法从根目录文件列表中匹配到子文件信息的问题
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_files: Option<Vec<SharedFileInfo>>,

    /// 分享根的绝对路径（来自 share/list?root=1 响应的 title 字段）
    ///
    /// 用于在转存/自动下载阶段稳定推导 share_root（剥掉分享者私有上层目录），
    /// 避免从文件路径反推时的歧义。详见 `docs/share-root-fix.md`。
    /// 老任务或异常响应可能为 None，此时由调用方退化到启发式推导。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_root_path: Option<String>,
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
            download_conflict_strategy: None,
            status: TransferStatus::Queued,
            error: None,
            download_task_ids: Vec::new(),
            share_info: None,
            randsk: None,
            file_list: Vec::new(),
            transferred_count: 0,
            total_count: 0,
            created_at: now,
            updated_at: now,
            failed_download_ids: Vec::new(),
            completed_download_ids: Vec::new(),
            download_started_at: None,
            file_name: None,
            is_share_direct_download: false,
            temp_dir: None,
            selected_fs_ids: None,
            selected_files: None,
            share_root_path: None,
        }
    }

    /// 设置文件名称（用于展示）
    pub fn set_file_name(&mut self, name: String) {
        self.file_name = Some(name);
        self.touch();
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

    /// 标记为清理临时文件中（分享直下专用）
    pub fn mark_cleaning(&mut self) {
        self.status = TransferStatus::Cleaning;
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
        assert!(!TransferStatus::Cleaning.is_terminal());
        assert!(TransferStatus::Completed.is_terminal());
    }
}
