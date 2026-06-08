// Web服务器模块

pub mod error;
pub mod events;
pub mod extractors;
pub mod handlers;
pub mod helpers;
pub mod middleware;
pub mod state;
pub mod websocket;

pub use error::{ApiError, ApiResult};
pub use helpers::{broadcast_account_list_changed, set_active_uid};
pub use state::AppState;
pub use websocket::WebSocketManager;
