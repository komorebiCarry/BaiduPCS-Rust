//! 自动备份模块
//!
//! 提供本地文件夹自动备份到百度网盘的功能，支持：
//! - 文件系统监听（实时检测文件变更）
//! - 定时轮询（兜底机制）
//! - 客户端侧加密（AES-256-GCM）
//! - 去重服务（避免重复上传）
//! - 优先级控制（备份任务优先级最低）
//! - SQLite 持久化（断点恢复）

pub mod common;
pub mod config;
pub mod error;
pub mod events;
pub mod manager;
pub mod persistence;
pub mod priority;
pub mod record;
pub mod scan_cache;
pub mod scheduler;
pub mod sync;
pub mod task;
pub mod validation;
pub mod watcher;

pub use crate::encryption::{BufferPool, EncryptionService};
pub use common::{TempFileGuard, TempFileManager};
pub use config::*;
pub use error::{BackupError, ErrorCategory, RetryPolicy};
pub use events::*;
pub use manager::AutoBackupManager;
pub use persistence::{normalize_pagination, FileTaskStats, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
pub use task::*;
