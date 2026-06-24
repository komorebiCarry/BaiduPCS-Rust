//! 备份记录模块（去重服务）

pub mod record_manager;

pub use record_manager::{
    calculate_head_md5, BackupRecordManager, DownloadRecord, EncryptionSnapshot, RecordStats,
    UploadRecord,
};
