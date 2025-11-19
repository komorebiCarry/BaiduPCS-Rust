// Web服务器模块

pub mod error;
pub mod handlers;
pub mod state;

pub use error::{ApiError, ApiResult};
pub use state::AppState;
