//! WebSocket 模块
//!
//! 提供 WebSocket 实时推送功能

mod handler;
pub mod manager;
mod message;

pub use handler::handle_websocket;
pub use manager::{WebSocketManager, PendingEvent, MAX_PENDING_EVENTS_PER_CONNECTION};
pub use message::{WsClientMessage, WsServerMessage};
