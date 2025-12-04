// Baidu Netdisk Rust Library
// 百度网盘 Rust 客户端核心库

// 认证模块
pub mod auth;

// 配置管理模块
pub mod config;

// Web服务器模块
pub mod server;

// 签名算法模块
pub mod sign;

// 网盘API模块
pub mod netdisk;

// 下载引擎模块
pub mod downloader;

// 上传引擎模块
pub mod uploader;

// 本地文件系统浏览模块
pub mod filesystem;

// 转存模块
pub mod transfer;

// 🔥 公共模块（CDN刷新检测机制等）
pub mod common;

// 导出常用类型
pub use auth::{LoginRequest, LoginResponse, QRCode, QRCodeStatus, UserAuth};
pub use config::AppConfig;
pub use downloader::{DownloadManager, DownloadTask, TaskStatus};
pub use netdisk::{FileItem, NetdiskClient};
pub use server::AppState;
pub use sign::{generate_devuid, LocateSign};
pub use uploader::{
    PcsServerHealthManager, RapidUploadChecker, RapidUploadHash, UploadEngine, UploadManager,
    UploadTask, UploadTaskStatus,
};

// 导出转存相关类型
pub use transfer::{
    TransferManager, TransferStatus, TransferTask, ShareLink, SharePageInfo,
    SharedFileInfo, TransferError, TransferResult,
};

// 🔥 导出CDN刷新相关类型
pub use common::{
    RefreshCoordinator, RefreshCoordinatorConfig,
    SpeedAnomalyDetector, SpeedAnomalyConfig,
    ThreadStagnationDetector, StagnationConfig,
};
