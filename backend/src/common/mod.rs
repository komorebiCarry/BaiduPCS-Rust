//! 公共模块
//!
//! 提供跨模块使用的通用组件

mod memory_monitor;
pub mod proxy;
pub mod proxy_fallback;
mod refresh_coordinator;
mod speed_anomaly_detector;
mod thread_stagnation_detector;

pub use memory_monitor::{MemoryAnomaly, MemoryMonitor, MemoryMonitorConfig, MemorySample};
pub use proxy::{ProxyConfig, ProxyType};
pub use proxy_fallback::{
    is_proxy_or_connection_error, perform_proxy_hot_update, probe_proxy,
    start_proxy_probe_task, ProxyFallbackManager, ProxyHotUpdater, ProxyRuntimeStatus,
};
pub use refresh_coordinator::{RefreshCoordinator, RefreshCoordinatorConfig, RefreshGuard};
pub use speed_anomaly_detector::{SpeedAnomalyConfig, SpeedAnomalyDetector};
pub use thread_stagnation_detector::{StagnationConfig, ThreadStagnationDetector};
