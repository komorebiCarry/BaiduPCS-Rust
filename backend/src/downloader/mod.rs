pub mod chunk;
pub mod engine;
pub mod manager;
pub mod progress;
pub mod scheduler;
pub mod task;

pub use chunk::{Chunk, ChunkManager};
pub use engine::{DownloadEngine, UrlHealthManager};
pub use manager::DownloadManager;
pub use progress::SpeedCalculator;
pub use scheduler::{ChunkScheduler, TaskScheduleInfo};
pub use task::{DownloadTask, TaskStatus};
