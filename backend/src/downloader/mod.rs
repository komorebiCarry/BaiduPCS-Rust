pub mod chunk;
pub mod engine;
pub mod folder;
pub mod folder_manager;
pub mod manager;
pub mod progress;
pub mod scheduler;
pub mod task;

pub use chunk::{Chunk, ChunkManager};
pub use engine::{DownloadEngine, UrlHealthManager};
pub use folder::{FolderDownload, FolderStatus, PendingFile};
pub use folder_manager::FolderDownloadManager;
pub use manager::DownloadManager;
pub use progress::SpeedCalculator;
pub use scheduler::{ChunkScheduler, TaskScheduleInfo, calculate_task_max_chunks};
pub use task::{DownloadTask, TaskStatus};
