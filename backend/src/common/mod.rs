//! 公共模块
//!
//! 提供跨模块使用的通用组件

mod refresh_coordinator;
mod speed_anomaly_detector;
mod thread_stagnation_detector;

pub use refresh_coordinator::{RefreshCoordinator, RefreshCoordinatorConfig, RefreshGuard};
pub use speed_anomaly_detector::{SpeedAnomalyConfig, SpeedAnomalyDetector};
pub use thread_stagnation_detector::{StagnationConfig, ThreadStagnationDetector};
