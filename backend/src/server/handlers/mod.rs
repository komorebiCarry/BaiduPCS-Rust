// API处理器模块

pub mod auth;
pub mod autobackup;
pub mod cloud_dl;
pub mod common;
pub mod config;
pub mod download;
pub mod encryption_export;
pub mod file;
pub mod filesystem;
pub mod local_files;
pub mod folder_download;
pub mod share;
pub mod share_sync;
pub mod transfer;
pub mod upload;

pub use auth::*;
pub use config::*;
pub use download::*;
pub use encryption_export::{export_bundle, export_keys, export_mapping};
pub use file::*;
// 只导出需要的函数，避免 ApiResponse 冲突
pub use filesystem::{get_roots, goto_path, list_directory, validate_path};
pub use folder_download::*;
pub use share::*;
// 显式列出 share_sync 里的 handler 函数，避免导入 ApiResponse（避免与 auth/autobackup 冲突）
pub use share_sync::{
    create_subscription, delete_subscription, disable_subscription, enable_subscription,
    get_run, get_subscription, latest_snapshot, list_runs, list_subscriptions,
    trigger_subscription, update_subscription,
};
pub use transfer::*;
pub use upload::*;
