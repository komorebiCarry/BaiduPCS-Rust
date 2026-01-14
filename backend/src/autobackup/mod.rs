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
pub mod task;
pub mod scheduler;
pub mod watcher;
pub mod priority;
pub mod record;
pub mod manager;
pub mod error;
pub mod events;
pub mod persistence;
pub mod validation;

pub use common::{TempFileGuard, TempFileManager};
pub use config::*;
pub use task::*;
pub use manager::AutoBackupManager;
pub use crate::encryption::{BufferPool, EncryptionService};
pub use error::{BackupError, ErrorCategory, RetryPolicy};
pub use events::*;
pub use persistence::{FileTaskStats, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, normalize_pagination};
