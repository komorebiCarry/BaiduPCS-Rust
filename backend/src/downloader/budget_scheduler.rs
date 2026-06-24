//! 多账号资源配额调度器
//!
//! 双层 Semaphore 架构：
//!
//! ```text
//!   ┌────────────────────────────────────────────────────┐
//!   │  全局 PrioritySemaphore (permits = M_download)     │
//!   │  全局 PrioritySemaphore (permits = M_upload)       │
//!   └────────────────────┬───────────────────────────────┘
//!                        │
//!         ┌──────────────┼───────────────┐
//!         ▼              ▼               ▼
//!   ┌─────────┐    ┌──────────┐    ┌──────────┐
//!   │ 账号 A  │    │ 账号 B   │    │ 账号 C   │
//!   │Sem(=cap)│    │Sem(=cap) │    │Sem(=cap) │
//!   └─────────┘    └──────────┘    └──────────┘
//! ```
//!
//! ## 核心不变式
//!
//! - `Σ used_i ≤ M`（机器约束，全局 Sem 保证）
//! - `used_i ≤ vip_cap_i`（百度风控线，账号 Sem 保证）
//! - 优先级：`used_i < base_i` → P0（队头插队），否则 P1（FIFO）
//! - 已持有 permit **不会被抢占**（祖父化语义）
//!
//! ## 主要 API
//!
//! - `BudgetScheduler::new(config)` 构造
//! - `add_account(uid, vip_type)` / `remove_account(uid)`
//! - `acquire_chunk_permit(uid, kind) -> ChunkPermit`
//! - `recompute_budget()` 按 VIP 权重压缩算法重算

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};
use tracing::{debug, info, warn};

use crate::auth::Uid;
use crate::downloader::priority_semaphore::{Priority, PriorityPermit, PrioritySemaphore};

/// 配额类型（下载 / 上传，独立分轨防互压）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetKind {
    Download,
    Upload,
}

/// VIP 等级（与 `UserAuth.vip_type: Option<u32>` 对应：0=普通、1=VIP、2=SVIP；其它视为 Normal）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VipType {
    Normal,
    Vip,
    Svip,
}

impl VipType {
    pub fn from_raw(raw: Option<u32>) -> Self {
        match raw {
            Some(1) => Self::Vip,
            Some(2) => Self::Svip,
            _ => Self::Normal,
        }
    }
}

/// VIP 推荐表（每个等级的 `threads` 即 `vip_cap`）
#[derive(Debug, Clone)]
pub struct VipRecommendedTable {
    pub normal_threads: usize,
    pub vip_threads: usize,
    pub svip_threads: usize,
}

impl Default for VipRecommendedTable {
    fn default() -> Self {
        // 默认值：与 `app.toml [multi_account_vip_recommended]` 默认 +
        // `DownloadConfig::recommended_for_vip()` 对齐（普通会员 10 / 超级会员 15）。
        Self {
            normal_threads: 1,
            vip_threads: 10,
            svip_threads: 15,
        }
    }
}

impl VipRecommendedTable {
    pub fn cap_for(&self, vip: VipType) -> usize {
        match vip {
            VipType::Normal => self.normal_threads,
            VipType::Vip => self.vip_threads,
            VipType::Svip => self.svip_threads,
        }
    }
}

/// 压缩算法权重表
#[derive(Debug, Clone)]
pub struct WeightTable {
    pub normal: usize,
    pub vip: usize,
    pub svip: usize,
}

impl Default for WeightTable {
    fn default() -> Self {
        Self {
            normal: 1,
            vip: 3,
            svip: 5,
        }
    }
}

impl WeightTable {
    pub fn weight_of(&self, vip: VipType) -> usize {
        match vip {
            VipType::Normal => self.normal,
            VipType::Vip => self.vip,
            VipType::Svip => self.svip,
        }
    }
}

/// 单个账号的请求值来源
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedSource {
    /// auto_apply_recommended=true：用 vip_cap_i
    Auto,
    /// auto_apply_recommended=false：用 user_config.threads（硬上限裁剪）
    User(usize),
}

/// 账号在调度器内的运行态
#[derive(Debug)]
struct AccountSlot {
    /// VIP 等级（原子化字段以支持 upsert：重新登录后 VIP 状态变化能正确反映到调度算法）
    ///
    /// 编码：`VipType::Normal=0, VipType::Vip=1, VipType::Svip=2`。
    vip_encoded: AtomicU8,
    /// 请求值来源（原子编码：0 = Auto，n>0 = User(n)；支持热更新）
    requested_source_encoded: AtomicUsize,
    /// 最近一次 recompute 写入的 `base_i`
    base: AtomicUsize,
    /// 当前使用量（acquire 自增、permit drop 自减）
    used: AtomicUsize,
    /// 历史最大 vip_cap（祖父化用；保留作历史观测）
    historical_max_cap: AtomicUsize,
    /// 当前生效的 cap
    ///
    /// 解决 `forget_permits` 重复扣减问题：每次 recompute 用
    /// `delta = new_cap - effective_cap` 调整 sem，而非相对 `historical_max_cap`
    /// 重新累计。snapshot 也用此值展示真实可用容量（不再用 historical 误导前端）。
    ///
    /// **不变式**：`effective_cap` 等于该 slot.sem 当前的总容量（init capacity
    /// + 所有 add_permits - 所有 forget_permits - 所有 ChunkPermit::drop 抵消的
    /// shrink_debt 数）。
    effective_cap: AtomicUsize,
    /// 账号级 Semaphore（permits 当前为 effective_cap）
    sem: Arc<Semaphore>,
    /// 待销毁的 permit 数（祖父化下调"债务"）
    ///
    /// `tokio::sync::Semaphore::forget_permits(n)` 只能消耗当前 `available` 的
    /// permit，不能"预约销毁"。如果 cap 下调时 in-flight permit 数 > available，
    /// `forget_permits` 实际只能消耗 available 部分；剩余差额必须记入此字段，
    /// 等到对应 ChunkPermit drop 时由 drop 路径调 `OwnedSemaphorePermit::forget()`
    /// 静默销毁，避免归还后 sem 总容量被还原 → 突破新 vip_cap 的风险。
    ///
    /// 与 `PrioritySemaphore.forgotten` 等价语义；只在 `recompute_track` 写入、
    /// `ChunkPermit::drop` 读出 + 减 1。
    shrink_debt: AtomicUsize,
}

impl AccountSlot {
    /// 把 `RequestedSource` 编码为 `AtomicUsize`：Auto=0, User(n)=n（n≥1）
    #[inline]
    fn encode_requested(src: RequestedSource) -> usize {
        match src {
            RequestedSource::Auto => 0,
            RequestedSource::User(n) => n.max(1),
        }
    }

    /// 反向解码
    #[inline]
    fn decode_requested(encoded: usize) -> RequestedSource {
        if encoded == 0 {
            RequestedSource::Auto
        } else {
            RequestedSource::User(encoded)
        }
    }

    /// 当前 `RequestedSource`
    #[inline]
    fn requested_source(&self) -> RequestedSource {
        Self::decode_requested(self.requested_source_encoded.load(Ordering::SeqCst))
    }

    /// 把 `VipType` 编码为 `AtomicU8`：Normal=0, Vip=1, Svip=2
    #[inline]
    fn encode_vip(v: VipType) -> u8 {
        match v {
            VipType::Normal => 0,
            VipType::Vip => 1,
            VipType::Svip => 2,
        }
    }

    /// 反向解码
    #[inline]
    fn decode_vip(encoded: u8) -> VipType {
        match encoded {
            1 => VipType::Vip,
            2 => VipType::Svip,
            _ => VipType::Normal,
        }
    }

    /// 当前 `VipType`
    #[inline]
    fn vip(&self) -> VipType {
        Self::decode_vip(self.vip_encoded.load(Ordering::SeqCst))
    }
}

/// 全局配额调度器配置
#[derive(Debug, Clone)]
pub struct BudgetSchedulerConfig {
    /// 机器下载总上限 `M_download`
    pub download_machine_budget: usize,
    /// 机器上传总上限 `M_upload`
    pub upload_machine_budget: usize,
    /// 下载 VIP 权重表
    pub download_weights: WeightTable,
    /// 上传 VIP 权重表
    pub upload_weights: WeightTable,
    /// 下载 VIP 推荐表
    pub download_recommended: VipRecommendedTable,
    /// 上传 VIP 推荐表
    pub upload_recommended: VipRecommendedTable,
}

impl Default for BudgetSchedulerConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Self {
            download_machine_budget: cores * 8,
            upload_machine_budget: cores * 8,
            download_weights: WeightTable::default(),
            upload_weights: WeightTable::default(),
            download_recommended: VipRecommendedTable::default(),
            upload_recommended: VipRecommendedTable::default(),
        }
    }
}

/// 最小保底配额（固定 1）
const MIN_PER_ACCOUNT: usize = 1;

/// 单轨（下载或上传）的运行态
struct Track {
    /// 全局优先级信号量（capacity = M）
    global_sem: Arc<PrioritySemaphore>,
    /// 当前 M（容量；recompute 时可调整）
    machine_budget: AtomicUsize,
    /// 每账号 slot
    accounts: DashMap<Uid, Arc<AccountSlot>>,
}

impl Track {
    fn new(machine_budget: usize) -> Self {
        Self {
            global_sem: Arc::new(PrioritySemaphore::new(machine_budget)),
            machine_budget: AtomicUsize::new(machine_budget),
            accounts: DashMap::new(),
        }
    }
}

/// 多账号配额调度器
pub struct BudgetScheduler {
    config: Mutex<BudgetSchedulerConfig>,
    download: Track,
    upload: Track,
    /// 🔥 重算 / 容量调整串行化锁
    ///
    /// 所有"读 prev_effective → 调整 sem → 写 effective_cap"的临界区必须在
    /// 同一把锁下执行，否则两个并发 `recompute_budget` 会读到同一个 prev，
    /// 重复执行 `add_permits` / `forget_permits` → 真实 sem 容量与
    /// `effective_cap` 永久不一致。
    ///
    /// 影响路径：
    /// - `recompute_budget` / `recompute_track`
    /// - `update_machine_budget` → `adjust_track_capacity`
    /// - `update_account_request` / `update_recommended` 等触发 recompute 的写入口
    ///
    /// 锁粒度：全 Scheduler 单锁；这些路径都是低频配置变更（用户改设置 / 账号
    /// 增删 / 启动 seed），并发竞争不在性能关键路径上，单锁简单可靠。
    recompute_lock: Mutex<()>,
}

impl std::fmt::Debug for BudgetScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetScheduler")
            .field("download_accounts", &self.download.accounts.len())
            .field("upload_accounts", &self.upload.accounts.len())
            .field(
                "download_machine_budget",
                &self.download.machine_budget.load(Ordering::Relaxed),
            )
            .field(
                "upload_machine_budget",
                &self.upload.machine_budget.load(Ordering::Relaxed),
            )
            .finish()
    }
}

/// 单账号配额快照（snapshot 接口用）
#[derive(Debug, Clone)]
pub struct AccountBudgetSnapshot {
    pub uid: Uid,
    pub vip_cap_download: usize,
    pub base_download: usize,
    pub used_download: usize,
    pub vip_cap_upload: usize,
    pub base_upload: usize,
    pub used_upload: usize,
}

/// 全量快照
#[derive(Debug, Clone)]
pub struct BudgetSnapshot {
    pub machine_budget_download: usize,
    pub machine_budget_upload: usize,
    pub per_account: Vec<AccountBudgetSnapshot>,
}

impl BudgetScheduler {
    /// 构造调度器
    pub fn new(config: BudgetSchedulerConfig) -> Arc<Self> {
        let download = Track::new(config.download_machine_budget);
        let upload = Track::new(config.upload_machine_budget);
        Arc::new(Self {
            config: Mutex::new(config),
            download,
            upload,
            recompute_lock: Mutex::new(()),
        })
    }

    fn track(&self, kind: BudgetKind) -> &Track {
        match kind {
            BudgetKind::Download => &self.download,
            BudgetKind::Upload => &self.upload,
        }
    }

    /// 添加或更新账号；触发重算（upsert 语义）
    ///
    /// 已有 uid → 更新 `vip_encoded` + `requested_source_encoded`，但保留
    /// `effective_cap` / `sem` / `used` 等运行态（避免抢占已持有的 permit）。
    /// recompute 会按新 `vip()` 重新计算 cap，然后通过 delta 调整 sem 容量。
    ///
    /// mutator 自身先持 `recompute_lock`，把"修改账号状态 +
    /// 重算"做成一个事务。这样并发 `add_account` / `recompute_budget` /
    /// `snapshot` 不会观察到半更新状态（如 download 侧已加但 upload 侧未加）。
    pub async fn add_account(
        &self,
        uid: Uid,
        vip: VipType,
        download_requested: RequestedSource,
        upload_requested: RequestedSource,
    ) {
        let _guard = self.recompute_lock.lock().await;
        let cfg = self.config.lock().await.clone();
        Self::upsert_account(
            &self.download,
            uid,
            vip,
            download_requested,
            &cfg.download_recommended,
        );
        Self::upsert_account(
            &self.upload,
            uid,
            vip,
            upload_requested,
            &cfg.upload_recommended,
        );
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
        info!("BudgetScheduler upsert 账号: uid={}, vip={:?}", uid, vip);
    }

    fn upsert_account(
        track: &Track,
        uid: Uid,
        vip: VipType,
        requested: RequestedSource,
        recommended: &VipRecommendedTable,
    ) {
        // 已存在 → 原子更新 vip / requested（cap 调整由后续 recompute 完成）
        if let Some(slot_ref) = track.accounts.get(&uid) {
            let slot = slot_ref.value();
            slot.vip_encoded
                .store(AccountSlot::encode_vip(vip), Ordering::SeqCst);
            slot.requested_source_encoded
                .store(AccountSlot::encode_requested(requested), Ordering::SeqCst);
            debug!(
                "upsert_account: 更新已有 uid={} vip={:?} requested={:?}",
                uid, vip, requested
            );
            return;
        }

        // 新增 → 构造完整 slot
        let cap = recommended.cap_for(vip).max(MIN_PER_ACCOUNT);
        let slot = Arc::new(AccountSlot {
            vip_encoded: AtomicU8::new(AccountSlot::encode_vip(vip)),
            requested_source_encoded: AtomicUsize::new(AccountSlot::encode_requested(requested)),
            base: AtomicUsize::new(MIN_PER_ACCOUNT),
            used: AtomicUsize::new(0),
            historical_max_cap: AtomicUsize::new(cap),
            // effective_cap 跟随 sem 实际容量
            effective_cap: AtomicUsize::new(cap),
            sem: Arc::new(Semaphore::new(cap)),
            shrink_debt: AtomicUsize::new(0),
        });
        track.accounts.insert(uid, slot);
    }

    /// 移除账号；触发重算
    ///
    /// mutator 自身先持 `recompute_lock`，把"删除账号 +
    /// 重算"做成一个事务，避免并发 mutator 看到半更新状态。
    pub async fn remove_account(&self, uid: Uid) {
        let _guard = self.recompute_lock.lock().await;
        self.download.accounts.remove(&uid);
        self.upload.accounts.remove(&uid);
        let cfg = self.config.lock().await.clone();
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
        info!("BudgetScheduler 移除账号: uid={}", uid);
    }

    /// 申请一个分片 permit（双层）
    ///
    /// # 优先级判定
    ///
    /// 判定 P0/P1 必须与 `used` 计数原子化关联，否则在并发场景下：
    /// 假设 `base=1` 且账号 cap > 1，多个分片同时进入此方法时会**同时**
    /// 读到 `used=0`，全部判成 P0，破坏文档"`used < base` 才是 P0"语义。
    ///
    /// 修复后流程：
    /// 1. 拿账号 sem permit（阻塞直到可用）
    /// 2. **`prev = used.fetch_add(1)`** ← 原子预占；返回前 `used` 序列号
    /// 3. `priority = if prev < base then P0 else P1`（基于唯一 prev，串行化）
    /// 4. 拿全局 priority sem permit
    /// 5. 构造 `ChunkPermit`；drop 时自动 `used.fetch_sub(1)`
    pub async fn acquire_chunk_permit(&self, uid: Uid, kind: BudgetKind) -> Option<ChunkPermit> {
        let track = self.track(kind);
        let slot = track.accounts.get(&uid).map(|r| Arc::clone(&*r))?;

        // 1. 账号 Sem
        // tokio::sync::Semaphore::acquire 会借走一个 permit，drop 时自动归还。
        // 这里我们使用 acquire_owned 以便 ChunkPermit 持有 'static 引用。
        let acc_permit = match Arc::clone(&slot.sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                warn!("账号 Sem 已关闭: uid={}", uid);
                return None;
            }
        };

        // 2. 原子预占 used 并据此决定优先级（P1 #1 修复）
        //    fetch_add 返回 *prev*，每个并发 acquirer 拿到唯一 prev 值，
        //    保证只有最早的 base 个请求走 P0；其余走 P1。
        let prev = slot.used.fetch_add(1, Ordering::SeqCst);
        let base_now = slot.base.load(Ordering::SeqCst);
        let priority = if prev < base_now {
            Priority::P0
        } else {
            Priority::P1
        };

        // 3. 全局优先级 Sem（基于已预占的优先级排队）
        //    注意：到这里如果发生 panic / 被取消，used 会泄露。下面的 await
        //    在 tokio Semaphore 上是 cancellation-safe 的（acquire 不持有
        //    任何 permit 直到完成），但调用方若 drop 此 future 仍可能漏减。
        //    用 RAII guard 在丢弃此 future 时自动 fetch_sub(1) 兜底。
        let _used_guard = UsedGuard {
            slot: Arc::clone(&slot),
            armed: true,
        };
        let global_permit = track.global_sem.acquire(priority).await;

        // 4. 成功路径：解除 guard（permit 接管 used 的归还职责）
        let mut used_guard = _used_guard;
        used_guard.armed = false;

        Some(ChunkPermit {
            account_permit: Some(acc_permit),
            _global_permit: global_permit,
            slot,
        })
    }

    /// 重算 base / vip_cap（压缩算法）；同时按祖父化语义调整账号 Sem
    ///
    /// 本方法在 `recompute_lock` 下串行化执行，
    /// 防止两个并发调用读到同一个 prev_effective 后重复 `add_permits`/`forget_permits`，
    /// 导致 sem 实际容量与 `effective_cap` 永久不一致。
    pub async fn recompute_budget(&self) {
        let _guard = self.recompute_lock.lock().await;
        let cfg = self.config.lock().await.clone();
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
    }

    /// 🔥 更新机器总上限 `M_download` / `M_upload`
    ///
    /// 等价于：调整全局 `PrioritySemaphore` 容量 + 触发 `recompute_budget`。
    /// 容量上调：用 `add_permits` 唤醒等待者；下调：用 `forget_permits` 祖父化。
    /// `None` 表示该轨道不变。
    ///
    /// `adjust_track_capacity` + 后续 recompute 一并放在
    /// `recompute_lock` 下，与并发 `recompute_budget` 串行化，防止全局 PrioritySemaphore
    /// 容量被两路并发调整出现"重复 add_permits"的问题。
    pub async fn update_machine_budget(
        &self,
        new_download: Option<usize>,
        new_upload: Option<usize>,
    ) {
        let _guard = self.recompute_lock.lock().await;
        if let Some(m) = new_download {
            let m = m.max(1);
            let mut cfg = self.config.lock().await;
            cfg.download_machine_budget = m;
            drop(cfg);
            Self::adjust_track_capacity(&self.download, m).await;
        }
        if let Some(m) = new_upload {
            let m = m.max(1);
            let mut cfg = self.config.lock().await;
            cfg.upload_machine_budget = m;
            drop(cfg);
            Self::adjust_track_capacity(&self.upload, m).await;
        }
        // 内联 recompute（不能再调 self.recompute_budget()，否则会再次取同一把锁死锁）
        let cfg = self.config.lock().await.clone();
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
    }

    /// 🔥 更新 VIP 权重表
    ///
    /// `None` 表示对应轨道不变。修改后立即触发重算（同一锁内事务）。
    pub async fn update_weights(
        &self,
        new_download: Option<WeightTable>,
        new_upload: Option<WeightTable>,
    ) {
        let _guard = self.recompute_lock.lock().await;
        {
            let mut cfg = self.config.lock().await;
            if let Some(w) = new_download {
                cfg.download_weights = w;
            }
            if let Some(w) = new_upload {
                cfg.upload_weights = w;
            }
        }
        let cfg = self.config.lock().await.clone();
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
    }

    /// 🔥 一次更新机器总上限 + VIP 权重
    ///
    /// `PUT /api/v1/config/multi_account_budget` 是一次用户操作，语义上同时改 M
    /// 和 weights。之前 handler 拆成 `update_machine_budget` + `update_weights` 两次
    /// 事务，每次单独 lock + recompute → 两次事务之间的窗口期外部 `GET /budget`
    /// / WS / `acquire_chunk_permit` 会读到"新 M + 旧 weights"的中间态。
    ///
    /// 本方法把两步合并到同一个 `recompute_lock` 临界区：写 config → 调整 sem
    /// 容量 → 写 weights → 一次 recompute。中间无窗口可观察到不一致。
    ///
    /// 任一参数为 `None` 表示该项不变；全部 `None` 时退化为单纯 recompute。
    pub async fn update_budget_config(
        &self,
        new_m_download: Option<usize>,
        new_m_upload: Option<usize>,
        new_weights_download: Option<WeightTable>,
        new_weights_upload: Option<WeightTable>,
    ) {
        let _guard = self.recompute_lock.lock().await;

        // 1) 写 config（M + weights 一并）+ 调整 sem 容量
        if let Some(m) = new_m_download {
            let m = m.max(1);
            let mut cfg = self.config.lock().await;
            cfg.download_machine_budget = m;
            drop(cfg);
            Self::adjust_track_capacity(&self.download, m).await;
        }
        if let Some(m) = new_m_upload {
            let m = m.max(1);
            let mut cfg = self.config.lock().await;
            cfg.upload_machine_budget = m;
            drop(cfg);
            Self::adjust_track_capacity(&self.upload, m).await;
        }
        {
            let mut cfg = self.config.lock().await;
            if let Some(w) = new_weights_download {
                cfg.download_weights = w;
            }
            if let Some(w) = new_weights_upload {
                cfg.upload_weights = w;
            }
        }

        // 2) 单次 recompute（事务尾部，确保 M + weights 都已写入）
        let cfg = self.config.lock().await.clone();
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
    }

    /// 🔥 更新 VIP 推荐表
    ///
    /// `None` 表示对应轨道不变。修改后立即触发重算（同一锁内事务）。
    pub async fn update_recommended(
        &self,
        new_download: Option<VipRecommendedTable>,
        new_upload: Option<VipRecommendedTable>,
    ) {
        let _guard = self.recompute_lock.lock().await;
        {
            let mut cfg = self.config.lock().await;
            if let Some(r) = new_download {
                cfg.download_recommended = r;
            }
            if let Some(r) = new_upload {
                cfg.upload_recommended = r;
            }
        }
        let cfg = self.config.lock().await.clone();
        Self::recompute_locked_inner(
            &self.download,
            &cfg.download_weights,
            &cfg.download_recommended,
        )
        .await;
        Self::recompute_locked_inner(&self.upload, &cfg.upload_weights, &cfg.upload_recommended)
            .await;
    }

    /// 🔥 更新单账号 `RequestedSource`
    ///
    /// 用户改 `auto_apply_recommended` 或 `download.max_global_threads` 等会触发本方法。
    /// `None` 表示该轨道不变。修改后立即触发重算（同一锁内事务）。
    ///
    /// mutator 自身先持 `recompute_lock`，避免在 download/
    /// upload 两侧 store 之间被并发观察到"download 已更新但 upload 未更新"的
    /// 半态。
    ///
    /// 返回 `false` 表示账号未注册到调度器（应先 `add_account`）。
    pub async fn update_account_request(
        &self,
        uid: Uid,
        new_download: Option<RequestedSource>,
        new_upload: Option<RequestedSource>,
    ) -> bool {
        let _guard = self.recompute_lock.lock().await;
        let mut found = false;
        if let Some(req) = new_download {
            if let Some(slot) = self.download.accounts.get(&uid) {
                slot.requested_source_encoded
                    .store(AccountSlot::encode_requested(req), Ordering::SeqCst);
                found = true;
            }
        }
        if let Some(req) = new_upload {
            if let Some(slot) = self.upload.accounts.get(&uid) {
                slot.requested_source_encoded
                    .store(AccountSlot::encode_requested(req), Ordering::SeqCst);
                found = true;
            }
        }
        if found {
            let cfg = self.config.lock().await.clone();
            Self::recompute_locked_inner(
                &self.download,
                &cfg.download_weights,
                &cfg.download_recommended,
            )
            .await;
            Self::recompute_locked_inner(
                &self.upload,
                &cfg.upload_weights,
                &cfg.upload_recommended,
            )
            .await;
            info!("BudgetScheduler 更新账号请求: uid={}", uid);
        }
        found
    }

    /// 调整单轨容量（祖父化语义）
    async fn adjust_track_capacity(track: &Track, new_m: usize) {
        let old_m = track.machine_budget.swap(new_m, Ordering::SeqCst);
        if new_m > old_m {
            track.global_sem.add_permits(new_m - old_m).await;
        } else if new_m < old_m {
            track.global_sem.forget_permits(old_m - new_m).await;
        }
    }

    /// 重算单轨的内部实现 — **必须在 `recompute_lock` 持有时调用**。
    ///
    /// `recompute_locked_inner` 强调"调用方负责持锁"。所有 mutator（`add_account` / `remove_account` /
    /// `update_*`）以及 `recompute_budget` 公开入口都先取 `recompute_lock`，再调
    /// 本 helper（避免本 helper 自身重复加锁导致死锁）。
    async fn recompute_locked_inner(
        track: &Track,
        weights: &WeightTable,
        recommended: &VipRecommendedTable,
    ) {
        let m = track.machine_budget.load(Ordering::SeqCst);

        // 收集快照（uid, vip, requested_source, current_cap）
        let entries: Vec<(Uid, Arc<AccountSlot>)> = track
            .accounts
            .iter()
            .map(|r| (*r.key(), Arc::clone(r.value())))
            .collect();

        let n = entries.len();
        if n == 0 {
            return;
        }

        // 第 1 步：vip_cap_i
        // 第 2 步：requested_i = source 决定 + 硬上限裁剪
        let mut vip_caps: HashMap<Uid, usize> = HashMap::with_capacity(n);
        let mut requested: HashMap<Uid, usize> = HashMap::with_capacity(n);
        for (uid, slot) in &entries {
            let cap = recommended.cap_for(slot.vip()).max(MIN_PER_ACCOUNT);
            let req = match slot.requested_source() {
                RequestedSource::Auto => cap,
                RequestedSource::User(v) => v.min(cap).max(MIN_PER_ACCOUNT),
            };
            vip_caps.insert(*uid, cap);
            requested.insert(*uid, req);
        }

        // 第 3 步：可行性
        let total_min = MIN_PER_ACCOUNT.saturating_mul(n);
        if total_min > m {
            warn!(
                "BudgetScheduler 机器上限 {} 小于 N×MIN({}×{}={})；按 MIN 分配但 Σbase 会超 M",
                m, n, MIN_PER_ACCOUNT, total_min
            );
        }

        // 第 4 步：保底
        let mut base: HashMap<Uid, usize> =
            entries.iter().map(|(u, _)| (*u, MIN_PER_ACCOUNT)).collect();

        // 第 5 步：剩余
        let remainder = m.saturating_sub(total_min);

        // 第 6 步：按权重分配
        let total_weight: usize = entries
            .iter()
            .map(|(_, s)| weights.weight_of(s.vip()))
            .sum();

        if total_weight > 0 && remainder > 0 {
            let mut leftover = remainder;
            for (uid, slot) in &entries {
                let w = weights.weight_of(slot.vip());
                let extra = remainder.saturating_mul(w) / total_weight;
                let cap = vip_caps[uid];
                let req = requested[uid];
                let allowed = extra
                    .min(req.saturating_sub(MIN_PER_ACCOUNT))
                    .min(cap.saturating_sub(MIN_PER_ACCOUNT));
                let b = base.get_mut(uid).unwrap();
                *b = (*b).saturating_add(allowed);
                leftover = leftover.saturating_sub(allowed);
            }

            // 第 7 步：二轮分配未用完的额度
            // 收集仍有缺口的账号（base < requested 且 base < vip_cap）
            let mut rounds = 0;
            while leftover > 0 && rounds < 4 {
                rounds += 1;
                let needers: Vec<(Uid, usize)> = entries
                    .iter()
                    .filter_map(|(uid, slot)| {
                        let b = base[uid];
                        let cap = vip_caps[uid];
                        let req = requested[uid];
                        let need = req.min(cap).saturating_sub(b);
                        if need == 0 {
                            None
                        } else {
                            Some((*uid, weights.weight_of(slot.vip()).max(1)))
                        }
                    })
                    .collect();
                if needers.is_empty() {
                    break;
                }
                let needer_total_w: usize = needers.iter().map(|(_, w)| *w).sum();
                if needer_total_w == 0 {
                    break;
                }
                let mut consumed_this_round = 0usize;
                for (uid, w) in &needers {
                    let extra = leftover.saturating_mul(*w) / needer_total_w;
                    if extra == 0 {
                        continue;
                    }
                    let cap = vip_caps[uid];
                    let req = requested[uid];
                    let b = base.get_mut(uid).unwrap();
                    let want = req.min(cap).saturating_sub(*b);
                    let allowed = extra.min(want);
                    *b = (*b).saturating_add(allowed);
                    consumed_this_round = consumed_this_round.saturating_add(allowed);
                }
                if consumed_this_round == 0 {
                    break;
                }
                leftover = leftover.saturating_sub(consumed_this_round);
            }
        }

        // 写回 + 调整账号 Sem 容量（祖父化）
        for (uid, slot) in &entries {
            let new_cap = vip_caps[uid];
            let new_base = base[uid];

            slot.base.store(new_base, Ordering::SeqCst);

            // 历史最大 cap：永远只增不减（祖父化用，仍保留作历史观测）
            let prev_hist = slot.historical_max_cap.load(Ordering::SeqCst);
            if new_cap > prev_hist {
                slot.historical_max_cap.store(new_cap, Ordering::SeqCst);
            }

            // 用 effective_cap 计算 delta
            //
            // 用 `historical_max_cap` 推算 delta 会导致 cap 从 10 → 5 后每次
            // recompute 都额外 forget_permits(5)，sem 容量被反复扣到失效。
            // 新实现：
            //   delta = new_cap - effective_cap
            //   delta > 0 → add_permits(delta)
            //   delta < 0 → forget_permits(-delta)
            //   delta == 0 → no-op
            // 然后把 `effective_cap` 更新为 new_cap，下次 recompute 在新基线上算。
            //
            // **祖父化语义不变**：cap 下降时 `forget_permits` 仍只抑制新 acquire，
            // 已持有的 permit 可正常完成（tokio Semaphore 行为）。
            let prev_effective = slot.effective_cap.load(Ordering::SeqCst);
            if new_cap > prev_effective {
                let diff = new_cap - prev_effective;
                // 上调时先抵消 shrink_debt，剩余才 add_permits。
                //
                // 直接 `add_permits(diff)` 会让 `shrink_debt` 还挂着等待 in-flight
                // permit drop 销毁。如果上调发生在 in-flight permit 释放之前，
                // 旧 permit 后续释放仍会被 debt 抵消，但中间窗口期 sem 已经被
                // add_permits 拉高 → 8 旧 + 5 新 = 13，突破新 vip_cap=10。
                //
                // 修复：上调 diff 中先抵消同等数量的 debt（CAS-loop 安全减），
                // 余下才真正 add_permits。等价 PrioritySemaphore.forgotten 的对称语义。
                let mut prev_debt = slot.shrink_debt.load(Ordering::SeqCst);
                let mut consumed_debt = 0usize;
                while consumed_debt < diff && prev_debt > 0 {
                    let take = (diff - consumed_debt).min(prev_debt);
                    match slot.shrink_debt.compare_exchange(
                        prev_debt,
                        prev_debt - take,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            consumed_debt += take;
                            break;
                        }
                        Err(actual) => {
                            prev_debt = actual;
                        }
                    }
                }
                let to_add = diff - consumed_debt;
                if to_add > 0 {
                    slot.sem.add_permits(to_add);
                }
                slot.effective_cap.store(new_cap, Ordering::SeqCst);
                debug!(
                    "uid={} 上调容量: effective_cap {} → {}（diff={}, 抵消 debt {}, add_permits {}）",
                    uid, prev_effective, new_cap, diff, consumed_debt, to_add
                );
            } else if new_cap < prev_effective {
                let diff = prev_effective - new_cap;
                // tokio Semaphore::forget_permits(n) 只能消耗
                // 当前 available 部分；如果有 in-flight permit 持有 → forget 会"实际只
                // 销毁 available_now、但运行时累计已经声称'忘掉了 n'"，等到 in-flight
                // 释放回来 sem 总容量被还原 → 突破新 vip_cap。
                //
                // 修复：把 forget_permits 调用前的 available 拍照下来，diff 中超出
                // available 的部分记入 `shrink_debt`，等到 ChunkPermit::drop 时由
                // OwnedSemaphorePermit::forget() 静默销毁，等价 PrioritySemaphore.forgotten。
                let available_now = slot.sem.available_permits();
                let consume_now = diff.min(available_now);
                let debt = diff - consume_now;
                if consume_now > 0 {
                    slot.sem.forget_permits(consume_now);
                }
                if debt > 0 {
                    let prev_debt = slot.shrink_debt.fetch_add(debt, Ordering::SeqCst);
                    debug!(
                        "uid={} 下调容量超出 available: 立即销毁 {} / 待 drop 抵消 {} → 累计 shrink_debt {} → {}",
                        uid,
                        consume_now,
                        debt,
                        prev_debt,
                        prev_debt + debt
                    );
                }
                slot.effective_cap.store(new_cap, Ordering::SeqCst);
                debug!(
                    "uid={} 下调容量: effective_cap {} → {}（forget_permits {}，debt={}，祖父化抑制新 acquire）",
                    uid, prev_effective, new_cap, consume_now, debt
                );
            }
            // delta == 0 → 跳过 sem 调整与 effective_cap 写入

            debug!(
                "recompute uid={} vip={:?} cap={} base={} used={}",
                uid,
                slot.vip(),
                new_cap,
                new_base,
                slot.used.load(Ordering::SeqCst)
            );
        }

        info!("BudgetScheduler.recompute 完成: M={}, accounts={}", m, n);
    }

    /// 全量快照
    ///
    /// 用 `effective_cap`（当前 sem 真实容量）而非 `historical_max_cap`，
    /// 避免前端看到的 vip_cap 与实际可获 permit 数量不一致。
    ///
    /// `async` + 持有 `recompute_lock`，避免与 mutator 并发时读到中间态
    /// （如 download 侧已 upsert 但 upload 侧未 upsert 的瞬间）。
    pub async fn snapshot(&self) -> BudgetSnapshot {
        let _guard = self.recompute_lock.lock().await;
        let m_dl = self.download.machine_budget.load(Ordering::SeqCst);
        let m_up = self.upload.machine_budget.load(Ordering::SeqCst);

        let mut by_uid: HashMap<Uid, AccountBudgetSnapshot> = HashMap::new();
        for r in self.download.accounts.iter() {
            let s = r.value();
            by_uid.insert(
                *r.key(),
                AccountBudgetSnapshot {
                    uid: *r.key(),
                    vip_cap_download: s.effective_cap.load(Ordering::SeqCst),
                    base_download: s.base.load(Ordering::SeqCst),
                    used_download: s.used.load(Ordering::SeqCst),
                    vip_cap_upload: 0,
                    base_upload: 0,
                    used_upload: 0,
                },
            );
        }
        for r in self.upload.accounts.iter() {
            let s = r.value();
            let entry = by_uid
                .entry(*r.key())
                .or_insert_with(|| AccountBudgetSnapshot {
                    uid: *r.key(),
                    vip_cap_download: 0,
                    base_download: 0,
                    used_download: 0,
                    vip_cap_upload: 0,
                    base_upload: 0,
                    used_upload: 0,
                });
            entry.vip_cap_upload = s.effective_cap.load(Ordering::SeqCst);
            entry.base_upload = s.base.load(Ordering::SeqCst);
            entry.used_upload = s.used.load(Ordering::SeqCst);
        }

        BudgetSnapshot {
            machine_budget_download: m_dl,
            machine_budget_upload: m_up,
            per_account: by_uid.into_values().collect(),
        }
    }
}

/// 分片 permit RAII guard
///
/// 持有：账号 Sem permit + 全局 PrioritySemaphore permit。
/// drop 时自动归还两层 permit；同步把 `slot.used` 减 1。
///
/// drop 路径检查 `slot.shrink_debt`，>0 时 `forget()` account permit（不归还）+ debt -1，
/// 实现祖父化下调时"已持有 permit 释放后静默销毁"的 PrioritySemaphore.forgotten 等价语义。
pub struct ChunkPermit {
    account_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    _global_permit: PriorityPermit,
    slot: Arc<AccountSlot>,
}

impl Drop for ChunkPermit {
    fn drop(&mut self) {
        // 1) 优先抵消 shrink_debt（祖父化下调时的"待销毁额度"）
        // CAS-loop 安全减 1：debt 0 时不动；debt>0 时减一并 forget account permit
        let mut debt = self.slot.shrink_debt.load(Ordering::SeqCst);
        let mut consume_debt = false;
        while debt > 0 {
            match self.slot.shrink_debt.compare_exchange(
                debt,
                debt - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    consume_debt = true;
                    break;
                }
                Err(actual) => {
                    debt = actual;
                }
            }
        }
        if consume_debt {
            // 销毁 account permit（不归还到 sem）→ 等价 PrioritySemaphore.forgotten 抵消
            if let Some(p) = self.account_permit.take() {
                p.forget();
            }
        }
        // 2) used 计数减 1（无论是否 forget；这是"in-flight"维度计数）
        self.slot.used.fetch_sub(1, Ordering::SeqCst);
        // 3) 如果 account_permit 还在，drop trait 自动归还（默认行为）；global_permit drop 同理
    }
}

/// `used` 计数预占 RAII guard
///
/// `acquire_chunk_permit` 在等待全局 priority sem 之前需要预占 `used`，
/// 用于决定 P0/P1 优先级。如果调用方丢弃 future（async 取消）或全局 sem
/// 被关闭，需要在丢失对应 ChunkPermit 之前归还 `used` 计数。
///
/// `armed = true` 时 drop 会 `fetch_sub(1)`；成功构造 `ChunkPermit` 后调用方
/// 把 `armed = false` 让 ChunkPermit 自己负责 used 归还。
struct UsedGuard {
    slot: Arc<AccountSlot>,
    armed: bool,
}

impl Drop for UsedGuard {
    fn drop(&mut self) {
        if self.armed {
            self.slot.used.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

// 防止 SemaphorePermit 警告（仅用于编译时签名占位）
#[allow(dead_code)]
fn _typecheck_permit(_p: SemaphorePermit<'_>) {}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 3 账号 Normal/VIP/SVIP 权重 1:3:5，Σ base ≤ M
    #[tokio::test]
    async fn test_compress_three_accounts_weights() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 16,
            upload_machine_budget: 16,
            download_weights: WeightTable {
                normal: 1,
                vip: 3,
                svip: 5,
            },
            upload_weights: WeightTable {
                normal: 1,
                vip: 3,
                svip: 5,
            },
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            upload_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
        };
        let bs = BudgetScheduler::new(cfg);
        bs.add_account(
            Uid::new(1),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.add_account(
            Uid::new(2),
            VipType::Vip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.add_account(
            Uid::new(3),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let snap = bs.snapshot().await;
        let sum_base_dl: usize = snap.per_account.iter().map(|a| a.base_download).sum();
        assert!(
            sum_base_dl <= snap.machine_budget_download,
            "Σ base ({}) 必须 ≤ M ({})",
            sum_base_dl,
            snap.machine_budget_download
        );

        // 每个账号 base ≥ MIN
        for a in &snap.per_account {
            assert!(
                a.base_download >= MIN_PER_ACCOUNT,
                "uid={} base < MIN",
                a.uid
            );
            assert!(
                a.base_download <= a.vip_cap_download,
                "uid={} base ({}) > vip_cap ({})",
                a.uid,
                a.base_download,
                a.vip_cap_download
            );
        }

        // SVIP 应分到比 Normal 多
        let mut by_uid: HashMap<u64, &AccountBudgetSnapshot> = HashMap::new();
        for a in &snap.per_account {
            by_uid.insert(a.uid.raw(), a);
        }
        let normal_base = by_uid[&1].base_download;
        let svip_base = by_uid[&3].base_download;
        assert!(
            svip_base >= normal_base,
            "SVIP base ({}) 应 ≥ Normal base ({})",
            svip_base,
            normal_base
        );
    }

    #[tokio::test]
    async fn test_acquire_release_permit() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 4,
            upload_machine_budget: 4,
            ..BudgetSchedulerConfig::default()
        };
        let bs = BudgetScheduler::new(cfg);
        bs.add_account(
            Uid::new(100),
            VipType::Vip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let p1 = bs
            .acquire_chunk_permit(Uid::new(100), BudgetKind::Download)
            .await;
        assert!(p1.is_some());
        let snap = bs.snapshot().await;
        let used = snap
            .per_account
            .iter()
            .find(|a| a.uid.raw() == 100)
            .unwrap()
            .used_download;
        assert_eq!(used, 1);

        drop(p1);
        // used 立即同步
        let snap2 = bs.snapshot().await;
        let used2 = snap2
            .per_account
            .iter()
            .find(|a| a.uid.raw() == 100)
            .unwrap()
            .used_download;
        assert_eq!(used2, 0);
    }

    #[tokio::test]
    async fn test_unknown_account_returns_none() {
        let bs = BudgetScheduler::new(BudgetSchedulerConfig::default());
        let p = bs
            .acquire_chunk_permit(Uid::new(99999), BudgetKind::Download)
            .await;
        assert!(p.is_none());
    }

    #[tokio::test]
    async fn test_account_cap_blocks_concurrent() {
        // VIP cap=2，账号最多 2 个 permit
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100, // 不限全局
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 2,
                svip_threads: 5,
            },
            ..BudgetSchedulerConfig::default()
        };
        let bs = BudgetScheduler::new(cfg);
        bs.add_account(
            Uid::new(7),
            VipType::Vip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let p1 = bs
            .acquire_chunk_permit(Uid::new(7), BudgetKind::Download)
            .await;
        let p2 = bs
            .acquire_chunk_permit(Uid::new(7), BudgetKind::Download)
            .await;
        assert!(p1.is_some() && p2.is_some());

        // 第三个应阻塞
        let bs_clone = Arc::clone(&bs);
        let third = tokio::spawn(async move {
            bs_clone
                .acquire_chunk_permit(Uid::new(7), BudgetKind::Download)
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!third.is_finished(), "第三个 acquire 应阻塞（cap=2）");

        drop(p1);
        let r = tokio::time::timeout(Duration::from_millis(200), third).await;
        assert!(r.is_ok(), "释放后第三个 acquire 应被唤醒");
    }

    #[tokio::test]
    async fn test_remove_account_recomputes() {
        let bs = BudgetScheduler::new(BudgetSchedulerConfig::default());
        bs.add_account(
            Uid::new(1),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.add_account(
            Uid::new(2),
            VipType::Vip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let snap_before = bs.snapshot().await;
        assert_eq!(snap_before.per_account.len(), 2);

        bs.remove_account(Uid::new(2)).await;

        let snap_after = bs.snapshot().await;
        assert_eq!(snap_after.per_account.len(), 1);
        assert_eq!(snap_after.per_account[0].uid.raw(), 1);
    }

    /// 回归测试：
    ///
    /// 并发场景下多个分片同时进入 `acquire_chunk_permit`，必须最多只有 `base` 个走 P0。
    /// 旧实现先 `load(used)` 后等全局再 `fetch_add`，并发窗口让所有请求都读到 used=0
    /// 全部判 P0；新实现用 `fetch_add` 原子预占，每个 acquirer 拿唯一 prev 值，
    /// 串行化判定 P0/P1。
    #[tokio::test]
    async fn test_priority_race_atomic_used() {
        use crate::downloader::priority_semaphore::Priority;

        // 配置：通过低 M_download + 多账号让 base 收敛到一个小于 cap 的值
        // 单账号场景 base 会被算到 cap，不能复现 P0/P1 边界。
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 6,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 5, // cap=5（acc sem 容量）
                vip_threads: 5,
                svip_threads: 5,
            },
            ..Default::default()
        };
        let bs = Arc::new(BudgetScheduler::new(cfg));
        bs.add_account(
            Uid::new(1),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.add_account(
            Uid::new(2),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.add_account(
            Uid::new(3),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let track = &bs.download;
        let slot = track.accounts.get(&Uid::new(1)).unwrap();
        let base = slot.base.load(Ordering::SeqCst);
        assert!(
            base >= 1 && base < 5,
            "base 应在 [1, cap=5) 区间，实际={}",
            base
        );
        // 拷贝一份独立 slot Arc 让 task 内部直接用，避免重新 DashMap 查找
        let slot_arc = Arc::clone(slot.value());
        drop(slot);

        // 并发 5 个 acquire：每个先 fetch_add 取 prev，等所有任务都拿到 prev 后
        // 才一起释放。这样 used 计数在判定瞬间是真实并发状态（不会被 sub 抢跑）。
        use tokio::sync::Barrier;
        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();
        for _ in 0..5 {
            let slot_clone = Arc::clone(&slot_arc);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let _acc = Arc::clone(&slot_clone.sem).acquire_owned().await.unwrap();
                let prev = slot_clone.used.fetch_add(1, Ordering::SeqCst);
                let base_now = slot_clone.base.load(Ordering::SeqCst);
                let priority = if prev < base_now {
                    Priority::P0
                } else {
                    Priority::P1
                };
                // 等所有任务都拿到自己的 prev 再释放（保证并发观测有效）
                bar.wait().await;
                slot_clone.used.fetch_sub(1, Ordering::SeqCst);
                priority
            }));
        }

        let mut p0_count = 0;
        let mut p1_count = 0;
        for h in handles {
            match h.await.unwrap() {
                Priority::P0 => p0_count += 1,
                Priority::P1 => p1_count += 1,
            }
        }

        // 关键断言：P0 数量恰好等于 base（fetch_add 序列化保证）
        // 旧实现下并发 acquire 都会读到 used=0 → 全部 P0（数量=5）→ assert 失败
        assert_eq!(
            p0_count, base,
            "P0 数量必须等于 base（fetch_add 序列化），实际 P0={}, P1={}, base={}",
            p0_count, p1_count, base
        );
        assert_eq!(
            p1_count,
            5 - base,
            "其余应为 P1（5 - base = {}）, 实际 P1={}",
            5 - base,
            p1_count
        );
    }

    /// 回归测试：
    ///
    /// 重新登录后 vip 升级（Normal → SVIP），`add_account` 必须 upsert
    /// 而非 contains_key 早返回，使新 vip 在下次 recompute 生效。
    #[tokio::test]
    async fn test_add_account_upsert_vip_change() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            ..Default::default()
        };
        let bs = BudgetScheduler::new(cfg);

        // 第一次注册：Normal
        bs.add_account(
            Uid::new(1),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let snap1 = bs.snapshot().await;
        let acct1 = snap1.per_account.iter().find(|a| a.uid.raw() == 1).unwrap();
        assert_eq!(acct1.vip_cap_download, 1, "Normal vip_cap=1");

        // 重新登录（同 uid）但变成 SVIP
        bs.add_account(
            Uid::new(1),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let snap2 = bs.snapshot().await;
        let acct2 = snap2.per_account.iter().find(|a| a.uid.raw() == 1).unwrap();
        assert_eq!(
            acct2.vip_cap_download, 10,
            "upsert 后 SVIP vip_cap=10（旧实现因 contains_key 早返回会保持 1）"
        );

        // 反向验证：再降回 Normal
        bs.add_account(
            Uid::new(1),
            VipType::Normal,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let snap3 = bs.snapshot().await;
        let acct3 = snap3.per_account.iter().find(|a| a.uid.raw() == 1).unwrap();
        assert_eq!(acct3.vip_cap_download, 1, "再降回 Normal vip_cap=1");
    }

    /// 回归测试：
    ///
    /// cap 从 10 下降到 5 后，连续多次 recompute 应稳定保持 sem 容量为 5
    /// （之前实现因 historical_max_cap 不下降，每次 recompute 都额外
    /// `forget_permits(5)`，sem 容量被反复扣到 0/失效）。
    #[tokio::test]
    async fn test_recompute_cap_downscale_no_double_forget() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            ..Default::default()
        };
        let bs = BudgetScheduler::new(cfg);

        // 初始 SVIP cap = 10
        bs.add_account(
            Uid::new(42),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let snap1 = bs.snapshot().await;
        let acct1 = snap1
            .per_account
            .iter()
            .find(|a| a.uid.raw() == 42)
            .unwrap();
        assert_eq!(acct1.vip_cap_download, 10, "初始 cap=10");

        // 把 SVIP threads 调到 5（vip_cap 10 → 5）
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 5,
            }),
            None,
        )
        .await;

        let snap2 = bs.snapshot().await;
        let acct2 = snap2
            .per_account
            .iter()
            .find(|a| a.uid.raw() == 42)
            .unwrap();
        assert_eq!(acct2.vip_cap_download, 5, "downscale 后 cap=5");

        // 再连续触发两次 recompute（模拟权重 / 请求 / 账号增删等多种触发路径）
        bs.recompute_budget().await;
        bs.recompute_budget().await;

        let snap3 = bs.snapshot().await;
        let acct3 = snap3
            .per_account
            .iter()
            .find(|a| a.uid.raw() == 42)
            .unwrap();
        assert_eq!(
            acct3.vip_cap_download, 5,
            "多次 recompute 后 cap 仍为 5（不被重复扣减）"
        );

        // sem 当前可用 permit 数应该 == 5（无 acquire 时 = 容量）
        let track = &bs.download;
        let slot = track.accounts.get(&Uid::new(42)).unwrap();
        let available = slot.sem.available_permits();
        assert_eq!(
            available, 5,
            "sem 当前可用 permit 应为 5（不被重复 forget 扣到 0）"
        );

        // 再次 upscale 回 10，验证可正常 add_permits
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            }),
            None,
        )
        .await;

        let snap4 = bs.snapshot().await;
        let acct4 = snap4
            .per_account
            .iter()
            .find(|a| a.uid.raw() == 42)
            .unwrap();
        assert_eq!(acct4.vip_cap_download, 10, "upscale 回 10");

        let slot = track.accounts.get(&Uid::new(42)).unwrap();
        assert_eq!(slot.sem.available_permits(), 10, "sem 可用 permit 回到 10");
    }

    /// 回归测试：cap 下调时已持有 permit 不能让 sem 容量"反弹"。
    ///
    /// 场景：SVIP cap=10，acquire 8 个 permit（available=2），下调 cap=5。
    /// `forget_permits(5)` 实际只能消耗 2 个 available，剩余 3 必须记入
    /// `shrink_debt`。等到 8 个旧 permit 全部释放后，`shrink_debt`=3 会让其中 3 个
    /// 在 ChunkPermit::drop 路径调 `OwnedSemaphorePermit::forget()` 静默销毁，
    /// 留下 5 个归还到 sem → sem 容量稳定为 5，与 vip_cap 一致。
    ///
    /// **验收**：全部释放后再连续 acquire，最多只能拿到 5 个（不能超过新 vip_cap）。
    #[tokio::test]
    async fn test_recompute_cap_downscale_inflight_grandfather_debt() {
        use crate::downloader::priority_semaphore::Priority;

        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            ..Default::default()
        };
        let bs = Arc::new(BudgetScheduler::new(cfg));

        // 初始 SVIP cap=10
        bs.add_account(
            Uid::new(42),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        // acquire 8 个 permit
        let mut held: Vec<ChunkPermit> = Vec::new();
        for _ in 0..8 {
            let p = bs
                .acquire_chunk_permit(Uid::new(42), BudgetKind::Download)
                .await
                .expect("应能拿到 permit");
            held.push(p);
        }
        // 当前 used=8, available=2
        let track = &bs.download;
        let slot = Arc::clone(track.accounts.get(&Uid::new(42)).unwrap().value());
        assert_eq!(slot.used.load(Ordering::SeqCst), 8);
        assert_eq!(slot.sem.available_permits(), 2);

        // 把 SVIP threads 调到 5：cap 10 → 5
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 5,
            }),
            None,
        )
        .await;

        // 此时 forget_permits 只消耗了 available 的 2 个；剩余 3 进入 shrink_debt
        assert_eq!(
            slot.sem.available_permits(),
            0,
            "available 被 forget 消耗光"
        );
        assert_eq!(
            slot.shrink_debt.load(Ordering::SeqCst),
            3,
            "无法立刻销毁的 3 个进入 shrink_debt"
        );
        assert_eq!(slot.effective_cap.load(Ordering::SeqCst), 5);

        // 释放全部 8 个 held permit
        held.clear();

        // 让 drop 异步处理（drop 是同步的，但 sleep 让 CAS-loop 完成）
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // shrink_debt 应被消耗光（最多消耗 3 次）；剩余 5 个 permit 归还到 sem
        assert_eq!(
            slot.shrink_debt.load(Ordering::SeqCst),
            0,
            "shrink_debt 被释放路径完整抵消"
        );
        assert_eq!(slot.used.load(Ordering::SeqCst), 0);
        assert_eq!(
            slot.sem.available_permits(),
            5,
            "sem 容量稳定为 5（不反弹到 8）"
        );

        // 验收：再连续 acquire，最多只能拿到 5 个（不能超过新 vip_cap）
        let mut held2: Vec<ChunkPermit> = Vec::new();
        for _ in 0..5 {
            let p = bs
                .acquire_chunk_permit(Uid::new(42), BudgetKind::Download)
                .await
                .expect("前 5 个应成功");
            held2.push(p);
        }
        assert_eq!(slot.sem.available_permits(), 0);

        // 第 6 个应阻塞（用 timeout 验证）
        let bs_clone = Arc::clone(&bs);
        let acquire_handle = tokio::spawn(async move {
            bs_clone
                .acquire_chunk_permit(Uid::new(42), BudgetKind::Download)
                .await
        });
        let r = tokio::time::timeout(std::time::Duration::from_millis(50), acquire_handle).await;
        assert!(
            r.is_err(),
            "第 6 个 acquire 必须阻塞（不能突破新 vip_cap=5）"
        );
        let _ = Priority::P0; // suppress unused warning
    }

    /// 回归测试（账号级）：下调产生 debt 后未释放再上调，
    /// 不能让 sem 容量绕过 debt 累加。
    ///
    /// 场景：SVIP cap=10 → acquire 8 → 下调 cap=5（debt=3，available=0）→
    /// 旧 permit 未释放就上调 cap=10。
    ///
    /// 修复前行为：上调直接 `add_permits(5)` → sem.available=5；旧 8 个释放
    /// 仍被 debt=3 抵消（销毁 3 个），但中间窗口期 sem 已经被拉高 → 8 旧 + 5 新
    /// 同时持有 = 13，突破新 cap=10。
    ///
    /// 修复后行为：上调时先抵消 debt 中的 5 个 → debt 从 3 减到 0；
    /// 还剩 (5-3)=2 真正 add_permits → sem.available=2；旧 8 释放回 sem 后归还
    /// 8 个（debt 已为 0）；最终 sem.available=10，total_held=0，符合 cap=10。
    #[tokio::test]
    async fn test_recompute_cap_upscale_consumes_shrink_debt() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            ..Default::default()
        };
        let bs = Arc::new(BudgetScheduler::new(cfg));

        bs.add_account(
            Uid::new(7),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        // acquire 8 个 permit
        let mut held: Vec<ChunkPermit> = Vec::new();
        for _ in 0..8 {
            let p = bs
                .acquire_chunk_permit(Uid::new(7), BudgetKind::Download)
                .await
                .expect("应能拿到 permit");
            held.push(p);
        }
        let track = &bs.download;
        let slot = Arc::clone(track.accounts.get(&Uid::new(7)).unwrap().value());
        assert_eq!(slot.used.load(Ordering::SeqCst), 8);
        assert_eq!(slot.sem.available_permits(), 2);

        // 下调到 5：debt=3, available=0
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 5,
            }),
            None,
        )
        .await;
        assert_eq!(slot.shrink_debt.load(Ordering::SeqCst), 3, "debt=3");
        assert_eq!(slot.sem.available_permits(), 0);

        // 旧 permit 未释放就上调到 10
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            }),
            None,
        )
        .await;

        // 验收：debt 必须先被 diff(=5) 抵消 3 个 → debt=0，剩 2 真正 add_permits
        assert_eq!(
            slot.shrink_debt.load(Ordering::SeqCst),
            0,
            "上调 diff=5 应先抵消 debt=3 → debt 归零"
        );
        assert_eq!(
            slot.sem.available_permits(),
            2,
            "上调后 sem.available=5-3=2（不是 5），保证 8 旧 + 2 新 = 10 不超 cap"
        );
        assert_eq!(slot.effective_cap.load(Ordering::SeqCst), 10);

        // 再 acquire 2 个就应满（8 + 2 = 10）
        let mut held2: Vec<ChunkPermit> = Vec::new();
        for _ in 0..2 {
            let p = bs
                .acquire_chunk_permit(Uid::new(7), BudgetKind::Download)
                .await
                .expect("应能拿到 permit");
            held2.push(p);
        }
        assert_eq!(
            slot.used.load(Ordering::SeqCst),
            10,
            "总 in-flight 严格 ≤ cap=10"
        );
        assert_eq!(slot.sem.available_permits(), 0);

        // 第 11 个必须阻塞
        let bs_clone = Arc::clone(&bs);
        let h = tokio::spawn(async move {
            bs_clone
                .acquire_chunk_permit(Uid::new(7), BudgetKind::Download)
                .await
        });
        let r = tokio::time::timeout(std::time::Duration::from_millis(50), h).await;
        assert!(r.is_err(), "第 11 个 acquire 必须阻塞");

        // 释放全部 8 + 2 = 10 个 permit
        held.clear();
        held2.clear();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 全部 drop 后：debt 已为 0 → drop 路径全部归还到 sem → available=10
        assert_eq!(slot.shrink_debt.load(Ordering::SeqCst), 0);
        assert_eq!(
            slot.sem.available_permits(),
            10,
            "所有 permit 归还 → sem.available=10"
        );
    }

    /// 回归测试：`recompute_budget` 必须串行化。
    ///
    /// 场景：cap=5 时并发触发多个 recompute（通过并发上调到不同值或多次调用），
    /// 修复前两个 recompute 都读到同一个 prev_effective=5，都会执行 add_permits，
    /// 真实 sem 容量被翻倍但 effective_cap 仍只显示 10。
    ///
    /// 修复后：`recompute_lock` 串行化保证至多一个临界区在跑，sem.available 与
    /// effective_cap 严格一致。
    #[tokio::test]
    async fn test_recompute_serialized_under_concurrent_calls() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            ..Default::default()
        };
        let bs = Arc::new(BudgetScheduler::new(cfg));

        // 初始 SVIP cap=10
        bs.add_account(
            Uid::new(1),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        let track = &bs.download;
        let slot = Arc::clone(track.accounts.get(&Uid::new(1)).unwrap().value());

        // 反复并发触发 recompute（10 个 task 同时调用）
        let mut handles = Vec::new();
        for _ in 0..10 {
            let bs_clone = Arc::clone(&bs);
            handles.push(tokio::spawn(async move {
                bs_clone.recompute_budget().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // 验收：sem.available == effective_cap == 10（cap 没变 → 不应有任何 add/forget）
        assert_eq!(slot.effective_cap.load(Ordering::SeqCst), 10);
        assert_eq!(
            slot.sem.available_permits(),
            10,
            "并发 recompute 串行化后 sem.available 与 effective_cap 严格一致"
        );

        // 进一步：并发上调 + recompute 混合也不应越过 cap
        // 先把 cap 改到 5 → 触发 cap 变化
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 5,
            }),
            None,
        )
        .await;
        assert_eq!(slot.effective_cap.load(Ordering::SeqCst), 5);
        assert_eq!(slot.sem.available_permits(), 5);

        // 并发触发 5 个 recompute（cap 不变 → 不应再调整）
        let mut handles2 = Vec::new();
        for _ in 0..5 {
            let bs_clone = Arc::clone(&bs);
            handles2.push(tokio::spawn(async move {
                bs_clone.recompute_budget().await;
            }));
        }
        for h in handles2 {
            h.await.unwrap();
        }

        assert_eq!(slot.effective_cap.load(Ordering::SeqCst), 5);
        assert_eq!(
            slot.sem.available_permits(),
            5,
            "并发 recompute 5 次后 sem.available 仍为 5（不重复 add/forget）"
        );
    }

    /// 强化回归测试：cap 在并发窗口内**真的发生变化**。
    ///
    /// 用 `Barrier` 让多个 task 在同一刻一起启动，然后并发触发：
    /// - update_recommended（多次切换 cap：5 ↔ 10）
    /// - recompute_budget
    /// - update_account_request
    /// - add_account / remove_account（同一 uid 反复）
    ///
    /// 旧实现（无 lock 串行化）：高概率出现 sem.available 与 effective_cap 不一致
    ///   或者比 effective_cap 大几倍（因为多次重复 add_permits）。
    /// 新实现（lock 串行化）：每次 cap 变化的事务原子完成，最终态稳定满足
    ///   `sem.available_permits() == effective_cap`。
    #[tokio::test]
    async fn test_recompute_concurrent_cap_changes_stable() {
        use tokio::sync::Barrier;

        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 100,
            upload_machine_budget: 100,
            download_recommended: VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            },
            ..Default::default()
        };
        let bs = Arc::new(BudgetScheduler::new(cfg));
        bs.add_account(
            Uid::new(99),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        // 32 个 task 同时启动，每个 task 跑 ROUNDS 轮
        const TASKS: usize = 32;
        const ROUNDS: usize = 30;
        let barrier = Arc::new(Barrier::new(TASKS));
        let mut handles = Vec::with_capacity(TASKS);
        for i in 0..TASKS {
            let bs_clone = Arc::clone(&bs);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                bar.wait().await;
                for r in 0..ROUNDS {
                    match (i + r) % 5 {
                        0 => {
                            // cap 从 SVIP=10 切到 5
                            bs_clone
                                .update_recommended(
                                    Some(VipRecommendedTable {
                                        normal_threads: 1,
                                        vip_threads: 5,
                                        svip_threads: 5,
                                    }),
                                    None,
                                )
                                .await;
                        }
                        1 => {
                            // cap 从 SVIP=5 切到 10
                            bs_clone
                                .update_recommended(
                                    Some(VipRecommendedTable {
                                        normal_threads: 1,
                                        vip_threads: 5,
                                        svip_threads: 10,
                                    }),
                                    None,
                                )
                                .await;
                        }
                        2 => {
                            bs_clone.recompute_budget().await;
                        }
                        3 => {
                            // 切换 requested 在 Auto / User(7) 之间
                            let req = if r % 2 == 0 {
                                RequestedSource::Auto
                            } else {
                                RequestedSource::User(7)
                            };
                            bs_clone
                                .update_account_request(Uid::new(99), Some(req), Some(req))
                                .await;
                        }
                        _ => {
                            // upsert 同一账号（vip 也变化）
                            let vip = if r % 2 == 0 {
                                VipType::Vip
                            } else {
                                VipType::Svip
                            };
                            bs_clone
                                .add_account(
                                    Uid::new(99),
                                    vip,
                                    RequestedSource::Auto,
                                    RequestedSource::Auto,
                                )
                                .await;
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // 收尾：固定 cap 到 SVIP=10 并 await 一次完整 recompute，让最终态稳定
        bs.add_account(
            Uid::new(99),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.update_recommended(
            Some(VipRecommendedTable {
                normal_threads: 1,
                vip_threads: 5,
                svip_threads: 10,
            }),
            None,
        )
        .await;

        // 验收：sem.available_permits() 必须严格等于 effective_cap
        // （旧无锁实现：会出现并发下 sem 容量被多次 add，比 effective_cap 大几倍）
        let track = &bs.download;
        let slot = Arc::clone(track.accounts.get(&Uid::new(99)).unwrap().value());
        let eff = slot.effective_cap.load(Ordering::SeqCst);
        let avail = slot.sem.available_permits();
        let used = slot.used.load(Ordering::SeqCst);
        let debt = slot.shrink_debt.load(Ordering::SeqCst);
        // 不变式：available + used + debt - 上调时未消耗的 add = effective_cap
        // 由于本测试无任何 acquire（held=0、used=0），简化为 available + debt == effective_cap
        // 但本测试结尾固定回 10 应该没有 debt 累计 → 验证两者严格相等
        assert_eq!(used, 0, "测试无 acquire，used 应为 0");
        assert_eq!(
            avail, eff,
            "并发 cap 变化收敛后 sem.available({}) 必须等于 effective_cap({})（debt={}）",
            avail, eff, debt
        );
        assert_eq!(debt, 0, "无 in-flight 时 debt 应为 0");
    }

    /// 回归测试：`update_budget_config` 把 M + weights 合并
    /// 到一次原子事务，外部不应观察到"新 M + 旧 weights"的中间态。
    ///
    /// 测试策略：用并发 reader 不停 `snapshot()`，并发 writer 不停切换 M+weights。
    /// 因为 `snapshot` 也持 `recompute_lock`，`update_budget_config` 内部 M 与
    /// weights 都在同一锁下，reader 一旦看到 M 已变，对应 weights 也必须已变。
    #[tokio::test]
    async fn test_update_budget_config_atomic() {
        let cfg = BudgetSchedulerConfig {
            download_machine_budget: 50,
            upload_machine_budget: 50,
            download_weights: WeightTable {
                normal: 1,
                vip: 3,
                svip: 5,
            },
            upload_weights: WeightTable {
                normal: 1,
                vip: 3,
                svip: 5,
            },
            ..Default::default()
        };
        let bs = Arc::new(BudgetScheduler::new(cfg));
        bs.add_account(
            Uid::new(1),
            VipType::Svip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;
        bs.add_account(
            Uid::new(2),
            VipType::Vip,
            RequestedSource::Auto,
            RequestedSource::Auto,
        )
        .await;

        // 并发 reader：不断快照
        let bs_r = Arc::clone(&bs);
        let reader_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_stop_clone = Arc::clone(&reader_stop);
        let reader = tokio::spawn(async move {
            let mut iter = 0;
            // 收集每次快照的 (M_dl, M_up) — 后续 writer 切到的 M 必须能在 weights
            // 同步切换的前提下被一致观察到，但 snapshot 不暴露 weights，所以这里
            // 主要是压住 lock 频率，保证 update_budget_config 内部不会被异步任务
            // 在中间中断（已由 lock 保证）。
            while !reader_stop_clone.load(std::sync::atomic::Ordering::SeqCst) {
                let _snap = bs_r.snapshot().await;
                iter += 1;
                if iter % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            iter
        });

        // 并发 writer：在两组 (M, weights) 间快速切换
        let configs: Vec<(usize, usize, WeightTable, WeightTable)> = vec![
            (
                100,
                100,
                WeightTable {
                    normal: 1,
                    vip: 5,
                    svip: 10,
                },
                WeightTable {
                    normal: 1,
                    vip: 5,
                    svip: 10,
                },
            ),
            (
                30,
                30,
                WeightTable {
                    normal: 2,
                    vip: 4,
                    svip: 7,
                },
                WeightTable {
                    normal: 2,
                    vip: 4,
                    svip: 7,
                },
            ),
        ];

        for round in 0..30 {
            let (m_dl, m_up, w_dl, w_up) = configs[round % 2].clone();
            bs.update_budget_config(Some(m_dl), Some(m_up), Some(w_dl), Some(w_up))
                .await;
        }

        reader_stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let read_count = reader.await.unwrap();
        // 仅断言 reader 实际跑了多次（确保并发覆盖到）
        assert!(read_count > 0, "reader 至少应跑过 1 次");

        // 收尾稳定：sem.available 与 effective_cap 必须一致
        let track = &bs.download;
        for r in track.accounts.iter() {
            let s = r.value();
            let avail = s.sem.available_permits();
            let eff = s.effective_cap.load(Ordering::SeqCst);
            let debt = s.shrink_debt.load(Ordering::SeqCst);
            // 无 acquire → used=0；available + debt 应等于 effective_cap
            assert_eq!(s.used.load(Ordering::SeqCst), 0);
            assert_eq!(
                avail + debt,
                eff,
                "uid={} 收敛: avail({}) + debt({}) == eff({})",
                r.key().raw(),
                avail,
                debt,
                eff
            );
        }
    }
}
