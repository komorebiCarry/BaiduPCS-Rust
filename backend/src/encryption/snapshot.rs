//! 加密快照管理器
//!
//! 管理加密文件和原始文件的映射关系

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::autobackup::record::{BackupRecordManager, EncryptionSnapshot};

/// 快照状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    /// 待处理
    Pending,
    /// 加密中
    Encrypting,
    /// 上传中
    Uploading,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

impl SnapshotStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotStatus::Pending => "pending",
            SnapshotStatus::Encrypting => "encrypting",
            SnapshotStatus::Uploading => "uploading",
            SnapshotStatus::Completed => "completed",
            SnapshotStatus::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "encrypting" => SnapshotStatus::Encrypting,
            "uploading" => SnapshotStatus::Uploading,
            "completed" => SnapshotStatus::Completed,
            "failed" => SnapshotStatus::Failed,
            _ => SnapshotStatus::Pending,
        }
    }
}

/// 快照管理器
pub struct SnapshotManager {
    record_manager: Arc<BackupRecordManager>,
}

impl std::fmt::Debug for SnapshotManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotManager")
            .field("record_manager", &"<BackupRecordManager>")
            .finish()
    }
}

impl SnapshotManager {
    /// 创建新的快照管理器
    pub fn new(record_manager: Arc<BackupRecordManager>) -> Self {
        Self { record_manager }
    }

    /// 创建快照
    pub fn create_snapshot(
        &self,
        config_id: &str,
        original_path: &str,
        original_name: &str,
        encrypted_name: &str,
        file_size: u64,
        nonce: &str,
        algorithm: &str,
        version: i32,
        key_version: u32,
        remote_path: &str,
    ) -> Result<i64> {
        let snapshot = EncryptionSnapshot {
            config_id: config_id.to_string(),
            original_path: original_path.to_string(),
            original_name: original_name.to_string(),
            encrypted_name: encrypted_name.to_string(),
            file_size,
            nonce: nonce.to_string(),
            algorithm: algorithm.to_string(),
            version,
            key_version,
            remote_path: remote_path.to_string(),
            is_directory: false,  // 文件快照，不是文件夹
            status: SnapshotStatus::Pending.as_str().to_string(),
        };

        self.record_manager.add_snapshot(&snapshot)
    }

    /// 更新快照状态
    pub fn update_status(&self, encrypted_name: &str, status: SnapshotStatus) -> Result<bool> {
        self.record_manager.update_snapshot_status(encrypted_name, status.as_str())
    }

    /// 根据加密文件名查找快照
    pub fn find_by_encrypted_name(&self, encrypted_name: &str) -> Result<Option<SnapshotInfo>> {
        let snapshot = self.record_manager.find_snapshot_by_encrypted_name(encrypted_name)?;
        Ok(snapshot.map(|s| SnapshotInfo::from(s)))
    }

    /// 根据原始文件路径查找快照
    pub fn find_by_original(
        &self,
        original_path: &str,
        original_name: &str,
    ) -> Result<Option<SnapshotInfo>> {
        let snapshot = self.record_manager.find_snapshot_by_original(original_path, original_name)?;
        Ok(snapshot.map(|s| SnapshotInfo::from(s)))
    }

    /// 标记为加密中
    pub fn mark_encrypting(&self, encrypted_name: &str) -> Result<bool> {
        self.update_status(encrypted_name, SnapshotStatus::Encrypting)
    }

    /// 标记为上传中
    pub fn mark_uploading(&self, encrypted_name: &str) -> Result<bool> {
        self.update_status(encrypted_name, SnapshotStatus::Uploading)
    }

    /// 标记为已完成
    pub fn mark_completed(&self, encrypted_name: &str) -> Result<bool> {
        self.update_status(encrypted_name, SnapshotStatus::Completed)
    }

    /// 标记为失败
    pub fn mark_failed(&self, encrypted_name: &str) -> Result<bool> {
        self.update_status(encrypted_name, SnapshotStatus::Failed)
    }

    /// 更新快照的加密元数据（nonce、algorithm）并标记为已完成
    /// 用于上传完成时更新之前创建的 pending 状态的快照
    pub fn update_encryption_metadata(
        &self,
        encrypted_name: &str,
        nonce: &str,
        algorithm: &str,
        version: i32,
    ) -> Result<bool> {
        self.record_manager.update_snapshot_encryption_metadata(encrypted_name, nonce, algorithm, version)
    }

    /// 批量根据加密文件名查找快照
    /// 用于文件列表显示时批量查询原始文件名
    pub fn find_by_encrypted_names(&self, encrypted_names: &[String]) -> Result<Vec<SnapshotInfo>> {
        let snapshots = self.record_manager.find_snapshots_by_encrypted_names(encrypted_names)?;

        Ok(snapshots.into_iter().map(SnapshotInfo::from).collect())
    }
}

/// 快照信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub config_id: String,
    pub original_path: String,
    pub original_name: String,
    pub encrypted_name: String,
    pub file_size: u64,
    pub nonce: String,
    pub algorithm: String,
    pub version: i32,
    pub key_version: u32,
    pub remote_path: String,
    pub status: SnapshotStatus,
}

impl From<EncryptionSnapshot> for SnapshotInfo {
    fn from(s: EncryptionSnapshot) -> Self {
        Self {
            config_id: s.config_id,
            original_path: s.original_path,
            original_name: s.original_name,
            encrypted_name: s.encrypted_name,
            file_size: s.file_size,
            nonce: s.nonce,
            algorithm: s.algorithm,
            version: s.version,
            key_version: s.key_version,
            remote_path: s.remote_path,
            status: SnapshotStatus::from_str(&s.status),
        }
    }
}

/// 获取文件显示信息
pub fn get_file_display_info(
    snapshot_manager: &SnapshotManager,
    encrypted_name: &str,
) -> Result<FileDisplayInfo> {
    if let Some(snapshot) = snapshot_manager.find_by_encrypted_name(encrypted_name)? {
        Ok(FileDisplayInfo {
            display_name: snapshot.original_name.clone(),
            display_path: snapshot.original_path.clone(),
            is_encrypted: true,
            encrypted_name: Some(encrypted_name.to_string()),
            original_size: Some(snapshot.file_size),
        })
    } else {
        // 不是加密文件或找不到快照
        Ok(FileDisplayInfo {
            display_name: encrypted_name.to_string(),
            display_path: String::new(),
            is_encrypted: false,
            encrypted_name: None,
            original_size: None,
        })
    }
}

/// 文件显示信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDisplayInfo {
    /// 显示名称（原始文件名或加密文件名）
    pub display_name: String,
    /// 显示路径
    pub display_path: String,
    /// 是否为加密文件
    pub is_encrypted: bool,
    /// 加密文件名（如果是加密文件）
    pub encrypted_name: Option<String>,
    /// 原始文件大小（如果是加密文件）
    pub original_size: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_record_manager() -> Arc<BackupRecordManager> {
        let dir = tempdir().unwrap();
        let db_path = dir.keep().join("test_backup.db");
        Arc::new(BackupRecordManager::new(&db_path).unwrap())
    }

    #[test]
    fn test_snapshot_manager_create() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        // 创建快照
        let id = snapshot_manager.create_snapshot(
            "config_1",
            "/path/to/file",
            "test.txt",
            "BPR_BKUP_uuid-1234.bkup",
            1024,
            "base64_nonce",
            "aes256gcm",
            1,
            1,  // key_version
            "/remote/path",
        ).unwrap();

        assert!(id > 0);
    }

    #[test]
    fn test_snapshot_manager_find_by_encrypted_name() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        let encrypted_name = "BPR_BKUP_test-uuid.bkup";

        // 创建快照
        snapshot_manager.create_snapshot(
            "config_1",
            "/path/to/file",
            "original.txt",
            encrypted_name,
            2048,
            "nonce_base64",
            "aes256gcm",
            1,
            1,  // key_version
            "/remote/backup",
        ).unwrap();

        // 查找快照
        let found = snapshot_manager.find_by_encrypted_name(encrypted_name).unwrap();
        assert!(found.is_some());

        let snapshot = found.unwrap();
        assert_eq!(snapshot.config_id, "config_1");
        assert_eq!(snapshot.original_name, "original.txt");
        assert_eq!(snapshot.encrypted_name, encrypted_name);
        assert_eq!(snapshot.file_size, 2048);
        assert_eq!(snapshot.key_version, 1);
        assert_eq!(snapshot.status, SnapshotStatus::Pending);
    }

    #[test]
    fn test_snapshot_manager_find_by_original() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        // 创建快照
        snapshot_manager.create_snapshot(
            "config_2",
            "/documents",
            "report.pdf",
            "BPR_BKUP_report-uuid.bkup",
            4096,
            "nonce_123",
            "chacha20poly1305",
            2,
            1,  // key_version
            "/backup/documents",
        ).unwrap();

        // 通过原始路径查找
        let found = snapshot_manager.find_by_original("/documents", "report.pdf").unwrap();
        assert!(found.is_some());

        let snapshot = found.unwrap();
        assert_eq!(snapshot.encrypted_name, "BPR_BKUP_report-uuid.bkup");
        assert_eq!(snapshot.algorithm, "chacha20poly1305");
        assert_eq!(snapshot.version, 2);
    }

    #[test]
    fn test_snapshot_manager_update_status() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        let encrypted_name = "BPR_BKUP_status-test.bkup";

        // 创建快照
        snapshot_manager.create_snapshot(
            "config_3",
            "/path",
            "file.txt",
            encrypted_name,
            512,
            "nonce",
            "aes256gcm",
            1,
            1,  // key_version
            "/remote",
        ).unwrap();

        // 验证初始状态
        let snapshot = snapshot_manager.find_by_encrypted_name(encrypted_name).unwrap().unwrap();
        assert_eq!(snapshot.status, SnapshotStatus::Pending);

        // 更新为加密中
        snapshot_manager.mark_encrypting(encrypted_name).unwrap();
        let snapshot = snapshot_manager.find_by_encrypted_name(encrypted_name).unwrap().unwrap();
        assert_eq!(snapshot.status, SnapshotStatus::Encrypting);

        // 更新为上传中
        snapshot_manager.mark_uploading(encrypted_name).unwrap();
        let snapshot = snapshot_manager.find_by_encrypted_name(encrypted_name).unwrap().unwrap();
        assert_eq!(snapshot.status, SnapshotStatus::Uploading);

        // 更新为已完成
        snapshot_manager.mark_completed(encrypted_name).unwrap();
        let snapshot = snapshot_manager.find_by_encrypted_name(encrypted_name).unwrap().unwrap();
        assert_eq!(snapshot.status, SnapshotStatus::Completed);
    }

    #[test]
    fn test_snapshot_manager_mark_failed() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        let encrypted_name = "BPR_BKUP_fail-test.bkup";

        snapshot_manager.create_snapshot(
            "config_4",
            "/path",
            "file.txt",
            encrypted_name,
            256,
            "nonce",
            "aes256gcm",
            1,
            1,  // key_version
            "/remote",
        ).unwrap();

        // 标记为失败
        snapshot_manager.mark_failed(encrypted_name).unwrap();
        let snapshot = snapshot_manager.find_by_encrypted_name(encrypted_name).unwrap().unwrap();
        assert_eq!(snapshot.status, SnapshotStatus::Failed);
    }

    #[test]
    fn test_snapshot_manager_not_found() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        // 查找不存在的快照
        let result = snapshot_manager.find_by_encrypted_name("nonexistent.bkup").unwrap();
        assert!(result.is_none());

        let result = snapshot_manager.find_by_original("config", "/path").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_snapshot_status_conversion() {
        assert_eq!(SnapshotStatus::Pending.as_str(), "pending");
        assert_eq!(SnapshotStatus::Encrypting.as_str(), "encrypting");
        assert_eq!(SnapshotStatus::Uploading.as_str(), "uploading");
        assert_eq!(SnapshotStatus::Completed.as_str(), "completed");
        assert_eq!(SnapshotStatus::Failed.as_str(), "failed");

        assert_eq!(SnapshotStatus::from_str("pending"), SnapshotStatus::Pending);
        assert_eq!(SnapshotStatus::from_str("encrypting"), SnapshotStatus::Encrypting);
        assert_eq!(SnapshotStatus::from_str("uploading"), SnapshotStatus::Uploading);
        assert_eq!(SnapshotStatus::from_str("completed"), SnapshotStatus::Completed);
        assert_eq!(SnapshotStatus::from_str("failed"), SnapshotStatus::Failed);
        assert_eq!(SnapshotStatus::from_str("unknown"), SnapshotStatus::Pending); // 默认值
    }

    #[test]
    fn test_get_file_display_info_encrypted() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        let encrypted_name = "BPR_BKUP_display-test.bkup";

        snapshot_manager.create_snapshot(
            "config_5",
            "/documents/work",
            "important.docx",
            encrypted_name,
            8192,
            "nonce",
            "aes256gcm",
            1,
            1,  // key_version
            "/backup/work",
        ).unwrap();

        let display_info = get_file_display_info(&snapshot_manager, encrypted_name).unwrap();

        assert!(display_info.is_encrypted);
        assert_eq!(display_info.display_name, "important.docx");
        assert_eq!(display_info.display_path, "/documents/work");
        assert_eq!(display_info.encrypted_name, Some(encrypted_name.to_string()));
        assert_eq!(display_info.original_size, Some(8192));
    }

    #[test]
    fn test_get_file_display_info_not_encrypted() {
        let record_manager = create_test_record_manager();
        let snapshot_manager = SnapshotManager::new(record_manager);

        let display_info = get_file_display_info(&snapshot_manager, "normal_file.txt").unwrap();

        assert!(!display_info.is_encrypted);
        assert_eq!(display_info.display_name, "normal_file.txt");
        assert_eq!(display_info.display_path, "");
        assert!(display_info.encrypted_name.is_none());
        assert!(display_info.original_size.is_none());
    }

    #[test]
    fn test_snapshot_info_from_encryption_snapshot() {
        let encryption_snapshot = EncryptionSnapshot {
            config_id: "config".to_string(),
            original_path: "/path".to_string(),
            original_name: "file.txt".to_string(),
            encrypted_name: "encrypted.bkup".to_string(),
            file_size: 1024,
            nonce: "nonce".to_string(),
            algorithm: "aes256gcm".to_string(),
            version: 1,
            key_version: 1,
            remote_path: "/remote".to_string(),
            is_directory:false,
            status: "completed".to_string(),
        };

        let snapshot_info = SnapshotInfo::from(encryption_snapshot);

        assert_eq!(snapshot_info.config_id, "config");
        assert_eq!(snapshot_info.original_name, "file.txt");
        assert_eq!(snapshot_info.key_version, 1);
        assert_eq!(snapshot_info.status, SnapshotStatus::Completed);
    }
}
