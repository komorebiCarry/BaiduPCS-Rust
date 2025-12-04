// 转存模块
//
// 实现分享链接转存 + 可选自动下载功能

pub mod task;
pub mod types;
pub mod manager;

pub use task::{TransferStatus, TransferTask};
pub use types::{ShareLink, SharePageInfo, SharedFileInfo, TransferError, TransferResult};
pub use manager::TransferManager;
