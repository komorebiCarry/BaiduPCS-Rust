//! 双向同步模块
//!
//! 实现 `BackupDirection::Sync` 的三阶段同步模型：
//! - Stage 1: Snapshot（扫描双端）
//! - Stage 2: Plan（生成同步计划）
//! - Stage 3: Execute（执行传输 + 更新状态）

pub mod types;
pub mod intent;
pub mod state_manager;
pub mod plan;
