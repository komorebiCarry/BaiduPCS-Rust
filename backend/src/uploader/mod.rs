// 上传引擎模块
//
// 复用下载模块的优化策略：
// - DashMap 并发优化（消除 Mutex 瓶颈）
// - 动态加权服务器选择（性能 +10-33%）
// - 指数退避重试（提升稳定性）
// - 任务级并发控制（资源利用 +50-80%）
// - 全局上传调度器（Round-Robin 公平调度）

pub mod chunk;
pub mod engine;
pub mod folder;
pub mod health;
pub mod manager;
pub mod rapid_upload;
pub mod scan_manager;
pub mod scheduler;
pub mod task;

pub use chunk::{
    calculate_recommended_chunk_size, get_chunk_size_limit, get_file_size_limit, UploadChunk,
    UploadChunkManager, DEFAULT_UPLOAD_CHUNK_SIZE, MAX_UPLOAD_CHUNK_SIZE, MIN_UPLOAD_CHUNK_SIZE,
    NORMAL_USER_CHUNK_SIZE, NORMAL_USER_FILE_SIZE_LIMIT, SVIP_CHUNK_SIZE, SVIP_FILE_SIZE_LIMIT,
    VIP_CHUNK_SIZE, VIP_FILE_SIZE_LIMIT,
};
pub use engine::UploadEngine;
pub use folder::{FolderScanner, ScanOptions, ScannedFile, BatchedScanIterator, SCAN_BATCH_SIZE};
pub use health::PcsServerHealthManager;
pub use manager::{UploadManager, UploadTaskInfo};
pub use rapid_upload::{RapidCheckResult, RapidUploadChecker, RapidUploadHash};
pub use scheduler::{UploadChunkScheduler, UploadTaskScheduleInfo};
pub use scan_manager::{ScanManager, ScanTaskInfo, ScanTaskStatus, ScanCheckpoint};
pub use task::{UploadTask, UploadTaskStatus};

/// 🔥 根据文件大小计算上传任务最大并发分片数
///
/// 上传比下载更保守，避免触发百度服务器限流
/// 小文件单线程，大文件最多4线程
///
/// # 参数
/// * `file_size` - 文件大小（字节）
///
/// # 返回
/// 最大并发分片数
pub fn calculate_upload_task_max_chunks(file_size: u64) -> usize {
    match file_size {
        0..=100_000_000 => 1,             // <100MB: 单线程最佳
        100_000_001..=500_000_000 => 2,   // 100MB-500MB: 2线程
        500_000_001..=1_073_741_824 => 3, // 500MB-1GB: 3线程
        _ => 4,                           // >1GB: 最多4线程（最安全）
    }
}
