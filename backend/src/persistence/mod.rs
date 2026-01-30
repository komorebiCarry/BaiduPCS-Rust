//! 任务持久化模块
//!
//! 该模块负责任务状态的持久化和恢复，包括：
//! - WAL (Write-Ahead Log) 日志：记录分片完成进度
//! - 元数据持久化：记录任务基本信息
//! - 断点恢复：程序重启后自动恢复未完成任务
//!
//! ## 设计原则
//!
//! 1. **WAL 优先**: 分片完成状态先写入内存缓存，定期批量刷写到 WAL 文件
//! 2. **容错性**: WAL 文件格式支持部分损坏恢复
//! 3. **性能**: 使用 parking_lot::Mutex 保护 WAL 缓存，减少锁竞争
//!
//! ## 文件结构
//!
//! ```text
//! wal/
//! ├── {task_id}.meta    # 任务元数据（JSON 格式）
//! └── {task_id}.wal     # WAL 日志（行格式，每行一个分片）
//! ```
//!
//! ## 使用示例
//!
//! ```ignore
//! use crate::persistence::PersistenceManager;
//! use crate::config::PersistenceConfig;
//!
//! // 创建持久化管理器
//! let config = PersistenceConfig::default();
//! let mut manager = PersistenceManager::new(config, base_dir);
//!
//! // 启动后台刷写任务
//! manager.start();
//!
//! // 注册下载任务
//! manager.register_download_task(
//!     "task_001".to_string(),
//!     12345,
//!     "/remote/file.txt".to_string(),
//!     PathBuf::from("/local/file.txt"),
//!     1024 * 1024,
//!     256 * 1024,
//!     4,
//! )?;
//!
//! // 标记分片完成
//! manager.on_chunk_completed("task_001", 0);
//!
//! // 任务完成时清理
//! manager.on_task_completed("task_001")?;
//!
//! // 关闭时优雅退出
//! manager.shutdown().await;
//! ```

pub mod folder;
pub mod history;
pub mod history_db;
pub mod manager;
pub mod metadata;
pub mod recovery;
pub mod types;
pub mod wal;

// 导出类型
pub use types::{TaskMetadata, TaskPersistenceInfo, TaskPersistenceStatus, TaskType, WalRecord};

// 导出 WAL 操作
pub use wal::{
    append_records, delete_wal_file, ensure_wal_dir, get_wal_path, read_records, scan_wal_task_ids,
    wal_exists, WalReader, WalWriter,
};

// 导出元数据操作
pub use metadata::{
    delete_metadata, delete_task_files, get_metadata_path, load_metadata, metadata_exists,
    save_metadata, scan_all_metadata, scan_metadata_task_ids, update_metadata,
};

// 导出持久化管理器
pub use manager::PersistenceManager;

// 导出恢复模块
pub use recovery::{
    cleanup_completed_tasks, cleanup_completed_tasks_with_db, cleanup_expired_tasks,
    cleanup_invalid_tasks, scan_recoverable_tasks, DownloadRecoveryInfo, RecoveredTask,
    RecoveryScanResult, TransferRecoveryInfo, UploadRecoveryInfo,
};

// 导出历史归档模块
pub use history::{
    add_to_history, archive_completed_tasks, cleanup_expired_history, get_history_path,
    load_history_cache, load_history_task_ids, remove_from_history_file,
    remove_tasks_by_group_from_history,
};

// 导出文件夹持久化模块
pub use folder::{
    add_folder_to_history, archive_completed_folders, cleanup_expired_folder_history,
    delete_folder, get_folder_history_path, load_all_folders, load_folder, load_folder_history,
    load_folder_history_ids, remove_folder_from_history, save_folder, FolderPersisted,
};

// 导出历史数据库模块
pub use history_db::{CloudDlAutoDownloadConfig, HistoryDbManager};
