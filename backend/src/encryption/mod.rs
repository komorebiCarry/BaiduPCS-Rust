//! 加密模块

pub mod buffer_pool;
pub mod config_store;
pub mod export;
pub mod service;
pub mod snapshot;

pub use buffer_pool::{BufferPool, BufferPoolStats, PooledBuffer};
pub use config_store::{EncryptionConfigStore, EncryptionKeyConfig, EncryptionKeyInfo};
pub use export::{DecryptBundleExporter, MappingExport, MappingGenerator, MappingRecord};
pub use service::{EncryptionService, StreamingEncryptionService, ENCRYPTED_FILE_EXTENSION};
pub use snapshot::SnapshotManager;
