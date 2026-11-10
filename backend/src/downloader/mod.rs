pub mod budget_scheduler;
pub mod chunk;
pub mod engine;
pub mod folder;
pub mod folder_manager;
pub mod manager;
pub mod priority_semaphore;
pub mod progress;
pub mod scheduler;
pub mod task;

// 🔥 小文件优化模块
pub mod streaming_queue;
pub mod batch_link_fetcher;
pub mod small_file_downloader;
pub mod small_file_adapter;

// 🔥 千万文件优化模块
pub mod streaming_scan;
pub mod sharded_storage;
pub mod scan_cache;
pub mod streaming_file_list;
pub mod adaptive_rate_limiter;

pub use chunk::{Chunk, ChunkFailureAction, ChunkManager};
pub use engine::{ChunkDownloadFailure, DownloadEngine, UrlHealthManager};
pub use folder::{FolderDownload, FolderStatus, PendingFile};
pub use folder_manager::FolderDownloadManager;
pub use manager::DownloadManager;
pub use progress::SpeedCalculator;
pub use scheduler::{
    calculate_task_max_chunks, ChunkScheduler, ConcurrencyChange, TaskScheduleInfo,
};
pub use task::{DownloadTask, TaskStatus};

// 🔥 导出小文件优化相关
pub use streaming_queue::{StreamingTaskQueue, StreamingQueueConfig, LightweightTaskInfo, QueueStats};
pub use batch_link_fetcher::{BatchLinkFetcher, BatchLinkFetcherConfig, BatchLinkStats};
pub use small_file_downloader::{SmallFileDownloader, SmallFileDownloaderConfig, SmallFileTask, SmallFileDownloadStats, is_small_file, SMALL_FILE_THRESHOLD};
pub use small_file_adapter::{SmallFileOptimizationAdapter, FileInfo};

// 🔥 导出千万文件优化相关
pub use streaming_scan::{StreamingScanner, StreamingScanConfig, ScanBatch};
pub use sharded_storage::{ShardedStorage, ShardConfig, ShardMetadata};
pub use scan_cache::{ScanCache, DirectorySnapshot, calculate_files_hash};
pub use streaming_file_list::{StreamingFileList, StreamingFileListConfig, FileListCursor};
pub use adaptive_rate_limiter::{AdaptiveRateLimiter, AdaptiveRateLimiterConfig, RateLimitPermit, RateLimiterStats};

// Re-export conflict strategy from uploader module for convenience
pub use crate::uploader::conflict::DownloadConflictStrategy;
