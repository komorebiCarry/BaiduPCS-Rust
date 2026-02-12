// 转存模块
//
// 实现分享链接转存 + 可选自动下载功能

pub mod manager;
pub mod task;
pub mod types;

pub use manager::TransferManager;
pub use manager::build_fs_ids;
pub use task::{TransferStatus, TransferTask};
pub use types::{ShareFileListResult, ShareLink, SharePageInfo, SharedFileInfo, TransferError, TransferResult};
