// Web服务器模块

pub mod error;
pub mod events;
pub mod handlers;
pub mod state;
pub mod websocket;

pub use error::{ApiError, ApiResult};
pub use state::AppState;
pub use websocket::WebSocketManager;
