//! 备份记录模块（去重服务）

pub mod record_manager;

pub use record_manager::{
    BackupRecordManager, UploadRecord, DownloadRecord, EncryptionSnapshot,
    RecordStats, calculate_head_md5,
};
