//! 账号解析器（多账号隔离核心）
//!
//! share_sync 的订阅按 `owner_uid` 归属具体账号。执行同步时需要拿到**该账号**的
//! `NetdiskClient`（读分享）与 `TransferManager`（转存 / 直链下载），而不是进程当前
//! 活跃账号的实例。
//!
//! 主架构里 `NetdiskClient` / `TransferManager` 都是 per-uid 注册在 `AppState` 的池中，
//! 但 `AppState` 在 server 层、share_sync 在更底层，直接依赖会造成循环。这里定义一个
//! 解析 trait，由 `AppState` 实现（读 `client_pool` / `transfer_managers`），manager 只
//! 依赖该 trait。这样：
//! - 后台调度对账号 A 的订阅始终解析账号 A 的客户端，与当前活跃账号无关；
//! - 账号切换无需 relink，因为每次执行都按 `owner_uid` 实时解析。

use crate::netdisk::client::NetdiskClient;
use crate::transfer::TransferManager;
use async_trait::async_trait;
use std::sync::Arc;

/// 按订阅所属账号（owner_uid）解析其网盘客户端 / TransferManager。
///
/// 返回 `None` 表示该账号当前未登录 / 未注册（调用方应据此把本次 run 标记为失败并
/// 给出明确原因，而不是落到错误账号）。
#[async_trait]
pub trait ShareSyncAccountResolver: Send + Sync {
    /// 解析某账号的 `NetdiskClient`
    async fn netdisk_client(&self, owner_uid: u64) -> Option<Arc<NetdiskClient>>;
    /// 解析某账号的 `TransferManager`
    async fn transfer_manager(&self, owner_uid: u64) -> Option<Arc<TransferManager>>;
    /// 进程当前活跃账号 uid（多账号场景下）。无活跃账号返回 None。
    ///
    /// 主要给 share_sync 启动期"owner_uid=0 历史数据"迁移使用（见 manager.rs::new）。
    /// 默认实现返回 None —— 静态/单元测试用 StaticAccountResolver 时无需强制实现。
    async fn active_uid(&self) -> Option<u64> {
        None
    }
}

/// 固定解析器：忽略 `owner_uid`，始终返回构造时给定的实例。
///
/// 用于单元测试，以及不需要多账号路由的简单场景。
pub struct StaticAccountResolver {
    netdisk: Option<Arc<NetdiskClient>>,
    transfer: Option<Arc<TransferManager>>,
}

impl StaticAccountResolver {
    pub fn new(
        netdisk: Option<Arc<NetdiskClient>>,
        transfer: Option<Arc<TransferManager>>,
    ) -> Self {
        Self { netdisk, transfer }
    }

    /// 两者皆 `None`（模拟未登录）
    pub fn none() -> Self {
        Self {
            netdisk: None,
            transfer: None,
        }
    }
}

#[async_trait]
impl ShareSyncAccountResolver for StaticAccountResolver {
    async fn netdisk_client(&self, _owner_uid: u64) -> Option<Arc<NetdiskClient>> {
        self.netdisk.clone()
    }
    async fn transfer_manager(&self, _owner_uid: u64) -> Option<Arc<TransferManager>> {
        self.transfer.clone()
    }
}
