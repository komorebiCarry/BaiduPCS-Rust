//! 事件模块
//!
//! 定义 WebSocket 事件类型和相关工具
//! - `types.rs`: 定义所有任务事件类型（Download/Upload/Transfer/Folder）
//! - `throttle.rs`: 事件节流相关工具，用于控制进度事件的发布频率

mod throttle;
mod types;

pub use throttle::*;
pub use types::*;

