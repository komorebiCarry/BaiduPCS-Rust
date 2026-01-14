//! 加密模块

pub mod buffer_pool;
pub mod config_store;
pub mod service;
pub mod snapshot;

pub use buffer_pool::{BufferPool, PooledBuffer, BufferPoolStats};
pub use config_store::{EncryptionConfigStore, EncryptionKeyConfig};
pub use service::{EncryptionService, StreamingEncryptionService, ENCRYPTED_FILE_EXTENSION};
pub use snapshot::SnapshotManager;
