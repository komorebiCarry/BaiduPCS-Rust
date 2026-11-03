// 网盘API模块

pub mod client;
pub mod client_pool;
pub mod cloud_dl;
pub mod cloud_dl_monitor;
pub mod share;
pub mod share_expire;
pub mod types;

pub use client::NetdiskClient;
pub use client_pool::ClientPool;
pub use cloud_dl::{
    AddTaskRequest, AddTaskResponse, AutoDownloadConfig, ClearTasksResponse, CloudDlFileInfo,
    CloudDlTaskInfo, CloudDlTaskStatus, ListTaskRequest, OperationResponse, QueryTaskRequest,
    TaskListResponse,
};
pub use cloud_dl_monitor::{CloudDlEvent, CloudDlMonitor, PollingConfig, TaskProgressTracker};
pub use share_expire::ShareMexpireResponse;
pub use types::*;

// TODO: 后续实现
// pub mod file;
