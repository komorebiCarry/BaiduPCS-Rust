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
    /// 加密中（新增）
    Encrypting,
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

    // === 🔥 新增：自动备份相关字段 ===
    /// 是否为自动备份任务
    #[serde(default)]
    pub is_backup: bool,

    /// 关联的备份配置ID（is_backup=true 时使用）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_config_id: Option<String>,

    /// 关联的备份文件任务ID（is_backup=true 时使用，用于发送 BackupEvent）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_file_task_id: Option<String>,

    /// 关联的备份主任务ID（is_backup=true 时使用，用于发送 BackupEvent）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_task_id: Option<String>,

    // === 🔥 任务槽位相关字段 ===
    /// 占用的槽位ID（用于任务槽机制）
    #[serde(skip)]
    pub slot_id: Option<usize>,

    /// 是否使用借调位（上传暂不使用，保留字段与下载一致）
    #[serde(skip)]
    pub is_borrowed_slot: bool,

    // === 🔥 加密相关字段 ===
    /// 是否启用加密
    #[serde(default)]
    pub encrypt_enabled: bool,

    /// 加密进度 (0.0 - 100.0)
    #[serde(default)]
    pub encrypt_progress: f64,

    /// 加密后的临时文件路径（加密完成后上传此文件）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted_temp_path: Option<PathBuf>,

    /// 原始文件大小（加密前）
    #[serde(default)]
    pub original_size: u64,

    // === 🔥 加密映射元数据（用于保存到 encryption_snapshots 表）===
    /// 加密后的文件名（如 BPR_BKUP_uuid.bkup）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted_name: Option<String>,

    /// 加密随机数（Base64 编码，用于解密）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_nonce: Option<String>,

    /// 加密算法（aes-256-gcm 或 chacha20-poly1305）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_algorithm: Option<String>,

    /// 加密格式版本号
    #[serde(default)]
    pub encryption_version: u8,

    /// 加密密钥版本号（用于密钥轮换后解密）
    #[serde(default = "default_key_version")]
    pub encryption_key_version: u32,

    // === 🔥 冲突策略字段 ===
    /// 冲突处理策略（用于转换为百度 API 的 rtype 参数）
    #[serde(default)]
    pub conflict_strategy: crate::uploader::UploadConflictStrategy,
}

fn default_key_version() -> u32 {
    1
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
            // 自动备份字段初始化
            is_backup: false,
            backup_config_id: None,
            backup_file_task_id: None,
            backup_task_id: None,
            // 任务槽位字段初始化
            slot_id: None,
            is_borrowed_slot: false,
            // 加密字段初始化
            encrypt_enabled: false,
            encrypt_progress: 0.0,
            encrypted_temp_path: None,
            original_size: total_size,
            // 加密映射元数据初始化
            encrypted_name: None,
            encryption_nonce: None,
            encryption_algorithm: None,
            encryption_version: 0,
            encryption_key_version: 1,
            // 冲突策略初始化
            conflict_strategy: crate::uploader::UploadConflictStrategy::default(),
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

    /// 创建自动备份上传任务
    pub fn new_backup(
        local_path: PathBuf,
        remote_path: String,
        total_size: u64,
        backup_config_id: String,
        encrypt_enabled: bool,
        backup_task_id: Option<String>,
        backup_file_task_id: Option<String>,
    ) -> Self {
        let mut task = Self::new(local_path, remote_path, total_size);
        task.is_backup = true;
        task.backup_config_id = Some(backup_config_id);
        task.encrypt_enabled = encrypt_enabled;
        task.backup_task_id = backup_task_id;
        task.backup_file_task_id = backup_file_task_id;
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

    /// 标记为加密中
    pub fn mark_encrypting(&mut self) {
        self.status = UploadTaskStatus::Encrypting;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 更新加密进度
    pub fn update_encrypt_progress(&mut self, progress: f64) {
        self.encrypt_progress = progress.clamp(0.0, 100.0);
    }

    /// 标记加密完成，设置加密后的临时文件路径和加密元数据
    ///
    /// 注意：此方法会同时将状态更新为 Uploading，确保状态一致性
    /// 这样在发送 EncryptCompleted 事件时，前端查询状态就能得到正确的 Uploading 状态
    ///
    /// # 参数
    /// * `encrypted_path` - 加密后的临时文件路径
    /// * `encrypted_size` - 加密后的文件大小
    /// * `encrypted_name` - 加密后的文件名（如 BPR_BKUP_uuid.bkup）
    /// * `nonce` - 加密随机数（Base64 编码）
    /// * `algorithm` - 加密算法名称
    /// * `version` - 加密格式版本号
    pub fn mark_encrypt_completed(
        &mut self,
        encrypted_path: PathBuf,
        encrypted_size: u64,
        encrypted_name: String,
        nonce: String,
        algorithm: String,
        version: u8,
    ) {
        self.encrypted_temp_path = Some(encrypted_path);
        self.total_size = encrypted_size; // 更新为加密后的大小
        self.encrypt_progress = 100.0;
        // 🔥 保存加密映射元数据（用于上传完成后写入 encryption_snapshots 表）
        self.encrypted_name = Some(encrypted_name);
        self.encryption_nonce = Some(nonce);
        self.encryption_algorithm = Some(algorithm);
        self.encryption_version = version;
        // 🔥 加密完成后立即将状态更新为 Uploading，避免前端查询时状态不一致
        // 这解决了 EncryptCompleted 事件发送后、mark_uploading() 调用前的时间窗口问题
        self.status = UploadTaskStatus::Uploading;
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
    pub fn set_rapid_hash(
        &mut self,
        content_md5: String,
        slice_md5: String,
        content_crc32: Option<String>,
    ) {
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
        let mut task = UploadTask::new(PathBuf::from("./test"), "/test".to_string(), 1000);

        task.uploaded_size = 250;
        assert_eq!(task.progress(), 25.0);

        task.uploaded_size = 500;
        assert_eq!(task.progress(), 50.0);

        task.uploaded_size = 1000;
        assert_eq!(task.progress(), 100.0);
    }

    #[test]
    fn test_eta_calculation() {
        let mut task = UploadTask::new(PathBuf::from("./test"), "/test".to_string(), 1000);

        task.uploaded_size = 200;
        task.speed = 100; // 100 bytes/s
        assert_eq!(task.eta(), Some(8)); // (1000 - 200) / 100 = 8s

        task.speed = 0;
        assert_eq!(task.eta(), None); // 速度为0，无法估算
    }

    #[test]
    fn test_status_transitions() {
        let mut task = UploadTask::new(PathBuf::from("./test"), "/test".to_string(), 1000);

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
        let mut task = UploadTask::new(PathBuf::from("./test"), "/test".to_string(), 1000);

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

    #[test]
    fn test_task_slot_fields() {
        let task = UploadTask::new(
            PathBuf::from("./test/file.txt"),
            "/test/file.txt".to_string(),
            1024,
        );

        assert!(task.slot_id.is_none());
        assert!(!task.is_borrowed_slot);
    }

    #[test]
    fn test_backup_task_slot_fields() {
        let task = UploadTask::new_backup(
            PathBuf::from("./test/file.txt"),
            "/test/file.txt".to_string(),
            1024,
            "config-123".to_string(),
            false,
            Some("backup-task-123".to_string()),
            Some("file-task-456".to_string()),
        );

        assert!(task.slot_id.is_none());
        assert!(!task.is_borrowed_slot);
        assert!(task.is_backup);
        assert_eq!(task.backup_task_id, Some("backup-task-123".to_string()));
        assert_eq!(task.backup_file_task_id, Some("file-task-456".to_string()));
    }

    #[test]
    fn test_encrypting_status() {
        let mut task = UploadTask::new(
            PathBuf::from("./test/file.txt"),
            "/test/file.txt".to_string(),
            1024,
        );

        // 测试加密状态转换
        task.encrypt_enabled = true;
        task.mark_encrypting();
        assert_eq!(task.status, UploadTaskStatus::Encrypting);
        assert!(task.started_at.is_some());

        // 测试加密进度更新
        task.update_encrypt_progress(50.0);
        assert_eq!(task.encrypt_progress, 50.0);

        // 测试进度边界
        task.update_encrypt_progress(150.0);
        assert_eq!(task.encrypt_progress, 100.0);

        task.update_encrypt_progress(-10.0);
        assert_eq!(task.encrypt_progress, 0.0);
    }

    #[test]
    fn test_encrypt_completed() {
        let mut task = UploadTask::new(
            PathBuf::from("./test/file.txt"),
            "/test/file.txt".to_string(),
            1024,
        );

        task.encrypt_enabled = true;
        task.mark_encrypting();
        task.mark_encrypt_completed(
            PathBuf::from("./test/file.bkup"),
            1100,
            "BPR_BKUP_test-uuid.bkup".to_string(),
            "base64_nonce_value".to_string(),
            "aes-256-gcm".to_string(),
            1,
        );

        assert_eq!(task.encrypt_progress, 100.0);
        assert_eq!(task.total_size, 1100);
        assert!(task.encrypted_temp_path.is_some());
        // 🔥 验证加密完成后状态自动转换为 Uploading
        assert_eq!(task.status, UploadTaskStatus::Uploading);
        // 🔥 验证加密元数据已保存
        assert_eq!(task.encrypted_name, Some("BPR_BKUP_test-uuid.bkup".to_string()));
        assert_eq!(task.encryption_nonce, Some("base64_nonce_value".to_string()));
        assert_eq!(task.encryption_algorithm, Some("aes-256-gcm".to_string()));
        assert_eq!(task.encryption_version, 1);
    }

    /// 测试旧版本 JSON 数据反序列化兼容性
    /// 确保缺少新增加密字段的旧数据能正确反序列化
    #[test]
    fn test_backward_compatibility_deserialization() {
        // 模拟旧版本的 JSON 数据（不包含加密相关字段）
        let old_json = r#"{
            "id": "old-task-123",
            "local_path": "./test/file.txt",
            "remote_path": "/test/file.txt",
            "total_size": 1024,
            "uploaded_size": 512,
            "status": "uploading",
            "speed": 100,
            "created_at": 1703203200,
            "started_at": 1703203201,
            "completed_at": null,
            "error": null,
            "is_rapid_upload": false,
            "content_md5": null,
            "slice_md5": null,
            "content_crc32": null,
            "group_id": null,
            "group_root": null,
            "relative_path": null,
            "total_chunks": 4,
            "completed_chunks": 2,
            "is_backup": false,
            "backup_config_id": null
        }"#;

        // 反序列化应该成功，新字段使用默认值
        let task: UploadTask = serde_json::from_str(old_json).expect("反序列化旧版本数据失败");

        // 验证基本字段
        assert_eq!(task.id, "old-task-123");
        assert_eq!(task.total_size, 1024);
        assert_eq!(task.status, UploadTaskStatus::Uploading);

        // 验证新增加密字段使用默认值
        assert!(!task.encrypt_enabled); // 默认 false
        assert_eq!(task.encrypt_progress, 0.0); // 默认 0.0
        assert!(task.encrypted_temp_path.is_none()); // 默认 None
        assert_eq!(task.original_size, 0); // 默认 0
    }

    /// 测试新版本 JSON 数据序列化/反序列化
    #[test]
    fn test_new_version_serialization() {
        let mut task = UploadTask::new(
            PathBuf::from("./test/file.txt"),
            "/test/file.txt".to_string(),
            1024,
        );
        task.encrypt_enabled = true;
        task.encrypt_progress = 50.0;
        task.encrypted_temp_path = Some(PathBuf::from("./temp/encrypted.bkup"));
        task.original_size = 1024;

        // 序列化
        let json = serde_json::to_string(&task).expect("序列化失败");

        // 反序列化
        let restored: UploadTask = serde_json::from_str(&json).expect("反序列化失败");

        // 验证加密字段正确恢复
        assert!(restored.encrypt_enabled);
        assert_eq!(restored.encrypt_progress, 50.0);
        assert_eq!(
            restored.encrypted_temp_path,
            Some(PathBuf::from("./temp/encrypted.bkup"))
        );
        assert_eq!(restored.original_size, 1024);
    }
}
