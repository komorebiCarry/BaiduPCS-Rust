// 网盘API模块

pub mod client;
pub mod cloud_dl;
pub mod cloud_dl_monitor;
pub mod types;

pub use client::NetdiskClient;
pub use cloud_dl::{
    AddTaskRequest, AddTaskResponse, AutoDownloadConfig, ClearTasksResponse, CloudDlFileInfo,
    CloudDlTaskInfo, CloudDlTaskStatus, ListTaskRequest, OperationResponse, QueryTaskRequest,
    TaskListResponse,
};
pub use cloud_dl_monitor::{CloudDlEvent, CloudDlMonitor, PollingConfig, TaskProgressTracker};
pub use types::*;

// TODO: 后续实现
// pub mod file;
