// 应用状态

use anyhow::Context;
use crate::auth::{AccountManager, QRCodeAuth, SessionManager, Uid, UserAuth};
use crate::common::ProxyType;
use crate::common::{ProxyConfig, ProxyFallbackManager, ProxyHotUpdater};
use crate::encryption::SnapshotManager;
use crate::autobackup::record::BackupRecordManager;
use crate::autobackup::AutoBackupManager;
use crate::common::{MemoryMonitor, MemoryMonitorConfig};
use crate::config::AppConfig;
use crate::downloader::budget_scheduler::{
    BudgetScheduler, BudgetSchedulerConfig, RequestedSource, VipRecommendedTable, VipType,
    WeightTable,
};
use crate::downloader::{ChunkScheduler, DownloadManager, FolderDownloadManager};
use tokio::sync::Semaphore;
use crate::netdisk::{ClientPool, CloudDlMonitor, NetdiskClient};
use crate::share_sync::resolver::ShareSyncAccountResolver;
use crate::share_sync::ShareSyncManager;
use crate::persistence::{
    cleanup_completed_tasks, cleanup_invalid_tasks, create_pre_migration_backup,
    is_transient_sqlite_error, needs_pre_migration_backup, run_migration_matrix,
    scan_recoverable_tasks, DownloadRecoveryInfo, PersistenceManager, TaskType,
    TransferRecoveryInfo, UploadRecoveryInfo,
};
use crate::server::websocket::WebSocketManager;
use crate::transfer::TransferManager;
use crate::uploader::{ScanManager, UploadManager};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

/// 进入全局只读保护模式的原因（仅后端诊断用，以 `info` 级日志打印）。
///
/// 只在自动修复 / 临时重试都失败、确定要进 `readonly_mode=true` 时填充；
/// 不向用户暴露其中的技术细节（前端只展示固定的友好提示文案）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadonlyReason {
    /// 失败发生的步骤（如「数据迁移」）。
    pub failed_step: String,
    /// 最近一次错误信息（含错误链）。
    pub last_error: String,
    /// 给运维 / 排查者的建议。
    pub suggestion: String,
}

/// share_sync 的账号解析器：按订阅 owner_uid 实时解析对应账号的
/// `NetdiskClient` / `TransferManager`（读 per-uid 池）。
///
/// 只持有所需的 `Arc` 句柄（而非整个 `AppState`），避免与 `AppState` 形成
/// 循环依赖，也让 share_sync 模块无需反向依赖 server 层类型。
struct AppStateShareSyncResolver {
    client_pool: Arc<RwLock<ClientPool>>,
    transfer_managers: Arc<DashMap<Uid, Arc<TransferManager>>>,
}

#[async_trait::async_trait]
impl ShareSyncAccountResolver for AppStateShareSyncResolver {
    async fn netdisk_client(&self, owner_uid: u64) -> Option<Arc<NetdiskClient>> {
        self.client_pool.read().await.get_client(Uid::new(owner_uid))
    }

    async fn transfer_manager(&self, owner_uid: u64) -> Option<Arc<TransferManager>> {
        self.transfer_managers
            .get(&Uid::new(owner_uid))
            .map(|e| Arc::clone(e.value()))
    }
}

/// 应用全局状态
#[derive(Clone)]
pub struct AppState {
    /// 二维码认证客户端（支持热替换）
    pub qrcode_auth: Arc<RwLock<QRCodeAuth>>,
    // ───────── 多账号核心字段 ─────────
    /// 多账号管理器（写主导场景 → Mutex）
    pub account_manager: Arc<Mutex<AccountManager>>,
    /// 多账号网盘客户端池（读主导场景 → RwLock）
    pub client_pool: Arc<RwLock<ClientPool>>,
    /// 当前活跃账号 UID（运行时真源）
    pub active_uid: Arc<RwLock<Option<Uid>>>,
    // ───────── 旧版单用户字段（过渡保留）─────────
    /// 会话管理器
    pub session_manager: Arc<Mutex<SessionManager>>,
    /// 当前登录用户
    pub current_user: Arc<RwLock<Option<UserAuth>>>,
    /// 网盘客户端
    pub netdisk_client: Arc<RwLock<Option<NetdiskClient>>>,
    /// 文件夹下载管理器
    pub folder_download_manager: Arc<FolderDownloadManager>,
    /// 应用配置
    pub config: Arc<RwLock<AppConfig>>,
    /// 🔥 持久化管理器
    pub persistence_manager: Arc<Mutex<PersistenceManager>>,
    /// 🔥 WebSocket 管理器
    pub ws_manager: Arc<WebSocketManager>,
    /// 🔥 快照管理器（加密文件映射，独立于自动备份管理器）
    pub snapshot_manager: Arc<SnapshotManager>,
    /// 🔥 备份记录管理器（供 autobackup 复用）
    pub backup_record_manager: Arc<BackupRecordManager>,
    /// 加密配置存储
    ///
    /// 由 `init_autobackup_manager` 在 active 账号 `AutoBackupManager` 创建后填入；
    /// 然后 `wire_manager_runtime_deps_for_uid` 把它注入到所有 per-uid `DownloadManager`
    /// 实现跨账号共享同一份加密密钥（机器级单例语义，避免多账号下不同 uid 看不到
    /// 同一份 encryption.json）。值为 `None` 表示 autobackup 尚未初始化或环境不支持
    /// 加密下载。
    pub encryption_config_store:
        Arc<RwLock<Option<Arc<crate::encryption::EncryptionConfigStore>>>>,
    /// 🔥 内存监控器
    pub memory_monitor: Arc<MemoryMonitor>,
    /// per-uid 离线下载监听服务池
    ///
    /// 每个账号一个 `CloudDlMonitor` 独立轮询、独立订阅者计数器、独立
    /// `NetdiskClient`。访问推荐使用 `cloud_dl_monitor_for_active()` /
    /// `cloud_dl_monitor_for(uid)`；跨账号场景（代理热更、shutdown）使用
    /// `iter_cloud_dl_monitors()` 遍历。
    pub cloud_dl_monitors: Arc<DashMap<Uid, Arc<CloudDlMonitor>>>,

    /// CloudDl WS 订阅 → uid 映射
    ///
    /// `connection_id -> Uid`。订阅 `cloud_dl` 主题时记录订阅瞬间的 active_uid，
    /// 切换账号 (`set_active_uid`) 时按此映射对所有订阅了 cloud_dl 的连接执行
    /// 「旧 uid monitor `remove_subscriber()` + 新 uid monitor `add_subscriber()`」
    /// 重绑，避免 monitor A 仍认为有订阅者继续轮询、monitor B 没订阅者不轮询的
    /// 串号/丢事件问题。订阅取消 / 连接断开时移除该映射。
    pub cloud_dl_ws_subscribers: Arc<DashMap<String, Uid>>,

    /// CloudDl monitor 初始化锁
    ///
    /// per-uid `Mutex<()>`，包住 `init_cloud_dl_monitor_for` 的"contains_key 检查
    /// + spawn + insert"序列。原版本是 `contains_key` → 多个 `await` → spawn →
    /// insert，并发同一 uid 调用会双 spawn 后台任务，先 spawn 的任务永远不会停止。
    /// 加这把锁后，并发只能一个进入临界区，另一个进锁后看到 `contains_key=true`
    /// 直接返回。
    pub cloud_dl_init_locks: Arc<tokio::sync::Mutex<HashMap<Uid, Arc<tokio::sync::Mutex<()>>>>>,
    /// 🔥 代理故障回退管理器
    pub fallback_mgr: Arc<ProxyFallbackManager>,
    /// 🔥 扫描管理器（用户登录后创建）
    pub scan_manager: Arc<RwLock<Option<Arc<ScanManager>>>>,
    /// 多账号资源配额调度器
    ///
    /// 持有全局 PrioritySemaphore + 每账号 Semaphore。
    /// 启动时按 `AppConfig.multi_account_budget` + 已加载账号 VIP 等级初始化。
    pub budget_scheduler: Arc<BudgetScheduler>,
    /// 全机器解密 CPU 闸门
    ///
    /// 容量 = `ChunkScheduler::calculate_decrypt_concurrency()`（CPU/2 clamp [2,8]）。
    /// 全机器唯一一份；由所有 per-uid `DownloadManager` / `ChunkScheduler` 共享 `acquire`。
    /// 防止 N 账号 × CPU/2 把机器解密并发上限放大 N 倍。
    pub decrypt_semaphore: Arc<Semaphore>,

    // ───────── 多账号 per-uid Manager 池 ─────────
    //
    // 每账号一份独立的 Manager 实例，按 `Uid` 索引；账号登录时插入，账号删除时
    // `shutdown()` 后移除。
    //
    // **对外访问语义**（与 legacy `download_manager` 字段并存）：
    // - `XxxManager_for_active()`：取活跃账号的 Manager（与 legacy 字段语义等价，平滑迁移）
    // - `XxxManager_for(uid)`：按 `Uid` 取目标账号的 Manager（多账号 handler 必走）
    /// per-uid 下载管理器池
    pub download_managers: Arc<DashMap<Uid, Arc<DownloadManager>>>,
    /// per-uid 上传管理器池
    pub upload_managers: Arc<DashMap<Uid, Arc<UploadManager>>>,
    /// per-uid 转存管理器池
    pub transfer_managers: Arc<DashMap<Uid, Arc<TransferManager>>>,
    /// per-uid 自动备份管理器池
    pub autobackup_managers: Arc<DashMap<Uid, Arc<AutoBackupManager>>>,

    // ───────── 全局只读模式 ─────────
    /// 全局只读模式开关。
    ///
    /// 当迁移失败 / 关键持久化异常时由迁移流程置 `true`，
    /// `ReadonlyMiddleware` 拦截所有 POST/PUT/DELETE 写接口并返回 503 `readonly_mode`。
    /// 全局接口（`/encryption/*` 等）也受此拦截。
    pub readonly_mode: Arc<AtomicBool>,

    /// 进入只读保护的原因（仅在 `readonly_mode=true` 时为 `Some`）。
    ///
    /// 后端诊断用：记录 `failed_step` / `last_error` / `suggestion`，
    /// 并在置位时以 `info` 级日志打印。前端不消费此字段（只展示固定友好文案）。
    pub readonly_reason: Arc<RwLock<Option<ReadonlyReason>>>,

    /// 🔥 分享同步管理器
    pub share_sync_manager: Arc<RwLock<Option<Arc<crate::share_sync::ShareSyncManager>>>>,
}

impl AppState {
    /// 创建新的应用状态
    pub async fn new() -> anyhow::Result<Self> {
        // 加载配置
        let config = AppConfig::load_or_default("config/app.toml").await;

        // ───── 迁移前完整备份（P0）─────
        // 必须在**任何会写 DB / 改名 / 回写的初始化之前**完成。否则：
        //   - `PersistenceManager::new()` → `HistoryDbManager::init_tables()` 会先跑
        //     `ALTER TABLE task_history/folder_history ADD COLUMN owner_uid`；
        //   - `BackupRecordManager::new()` 会先建/改 `backup_configs` 表；
        //   - 之后 `AccountManager::migrate_from_session()` 还会把 session.json 改名。
        // 这些都会先改动数据，使「迁移前备份」名不副实。这里**只用 AppConfig** 解析出
        // 真实 db_path / wal_dir（不实例化任何 manager），先备份再初始化。
        // 备份失败仅告警、不阻塞启动；真实破坏性写入若随后失败会进入只读保护，
        // 用户仍可用升级前自备份的 config/ + wal/ 还原。
        {
            let base_dir = std::path::Path::new(".");
            let resolve = |p: &str| -> std::path::PathBuf {
                let pp = std::path::Path::new(p);
                if pp.is_absolute() {
                    pp.to_path_buf()
                } else {
                    base_dir.join(pp)
                }
            };
            let config_dir = std::path::PathBuf::from("config");
            let real_db_path = resolve(&config.persistence.db_path);
            let real_wal_dir = resolve(&config.persistence.wal_dir);
            let session_path = config_dir.join("session.json");
            let accounts_path = config_dir.join("accounts.json");
            if needs_pre_migration_backup(&config_dir, &real_db_path, &session_path, &accounts_path)
            {
                match create_pre_migration_backup(&config_dir, &real_db_path, &real_wal_dir).await {
                    Ok(b) => info!(
                        "迁移前完整备份已创建于 {}（{} 项），如迁移异常可整目录还原",
                        b.dir.display(),
                        b.copied.len()
                    ),
                    Err(e) => warn!("迁移前备份创建失败（不阻塞启动）: {e:#}"),
                }
            } else {
                info!("跳过迁移前备份（已存在 pre_migration 备份或无既有数据）");
            }
        }

        // 创建文件夹下载管理器
        let folder_download_manager = Arc::new(FolderDownloadManager::new(
            config.download.download_dir.clone(),
        ));

        // 🔥 创建持久化管理器
        let base_dir = std::path::Path::new(".");
        let mut persistence_manager = PersistenceManager::new(config.persistence.clone(), base_dir);
        persistence_manager.start();
        info!("持久化管理器已启动");

        // 🔥 创建 WebSocket 管理器
        let ws_manager = Arc::new(WebSocketManager::new());
        info!("WebSocket 管理器已创建");

        // 🔥 创建备份记录管理器和快照管理器（独立于自动备份管理器）
        let db_path = std::path::PathBuf::from(&config.persistence.db_path);
        let backup_record_manager = Arc::new(BackupRecordManager::new(&db_path)?);
        let snapshot_manager = Arc::new(SnapshotManager::new(Arc::clone(&backup_record_manager)));
        info!("快照管理器已创建");

        // 🔥 创建内存监控器
        let memory_monitor = Arc::new(MemoryMonitor::new(MemoryMonitorConfig::default()));
        info!("内存监控器已创建");

        // 读取代理配置
        let proxy_config = if config.network.proxy.proxy_type != ProxyType::None {
            Some(&config.network.proxy)
        } else {
            None
        };

        // 🔥 创建代理故障回退管理器
        let fallback_mgr = Arc::new(ProxyFallbackManager::new());

        // ───── 多账号核心初始化 ─────
        // 注意：迁移前完整备份已在本函数最前面（任何 manager 初始化之前）完成。
        let session_path = std::path::PathBuf::from("config/session.json");
        let accounts_path = std::path::PathBuf::from("config/accounts.json");

        // 1) 从旧 session.json 迁移（若存在）→ 加载 accounts.json
        let account_manager = AccountManager::migrate_from_session(&session_path, &accounts_path)
            .await
            .map_err(|e| {
                error!("AccountManager 加载/迁移失败: {e:?}");
                e
            })?;
        let initial_active_uid = account_manager.active_uid();
        info!(
            "多账号初始化：账号数={}，active_uid={:?}",
            account_manager.list_users().len(),
            initial_active_uid
        );
        let account_manager = Arc::new(Mutex::new(account_manager));

        // 启动期执行完整迁移矩阵：
        //   - SQLite ALTER（backup_configs / task_history / folder_history 加 owner_uid）
        //   - .meta 回填（owner_uid=Some(active_uid)）
        //   - SQLite history 回填（task_history / folder_history 把 NULL/0 行更新为 active_uid）
        //
        // 之前只在 `migrate_from_session` 内调用 → accounts.json 已存在时被跳过 →
        // task_history.owner_uid 永远 NULL → 前端历史任务全部显示 UID:0。
        // 这里改为无条件执行（幂等），不论 session.json 是否存在。
        //
        // 注意：history 回填仅在 **单账号** 系统执行。
        // 多账号下盲目用 active_uid 回填会把另一个账号的历史任务错误归并到当前活跃账号。
        // 新版本代码 INSERT 路径已带正确 owner_uid → 全新数据永远不会有 NULL/0 行 →
        // 此回填仅对老版本升级到新版本的「单账号」用户生效一次（NULL 行更新完后下次启动是 no-op）。
        let migration_db_path = std::path::PathBuf::from(&config.persistence.db_path);
        let migration_wal_dir = persistence_manager.wal_dir().clone();
        let account_count = account_manager.lock().await.list_users().len();
        // 迁移失败必须冻结写入路径：
        // 之前只 `error!` 不 store flag，readonly 中间件永远不会拦截，导致迁移异常下
        // 实例仍可写入 → 加剧数据不一致风险。这里改为预先创建 readonly_mode Arc，
        // 失败时立即 store(true)，并把同一个 Arc 透传到下面的 `Self { readonly_mode, .. }`。
        let readonly_mode = Arc::new(AtomicBool::new(false));
        let mut readonly_reason_init: Option<ReadonlyReason> = None;

        // P0-1：迁移命中**临时**锁/占用错误（database is locked/busy）时短退避自动重试
        //   （200ms → 1s，最多 3 次尝试）。缺列/重复列/表不存在/单账号 owner_uid 回填等
        //   已由迁移矩阵内的幂等守卫吸收，不在此重试。非临时错误（DB 损坏 / 无权限 /
        //   磁盘满）不重试，直接进入下面的只读保护兜底。
        const MAX_MIGRATION_ATTEMPTS: u32 = 3;
        const MIGRATION_BACKOFF_MS: [u64; 2] = [200, 1000];
        let mut attempt: u32 = 1;
        let migration_result = loop {
            match run_migration_matrix(
                &migration_db_path,
                &migration_wal_dir,
                initial_active_uid,
                account_count,
            )
                .await
            {
                Ok(summary) => break Ok(summary),
                Err(e) if attempt < MAX_MIGRATION_ATTEMPTS && is_transient_sqlite_error(&e) => {
                    let backoff = MIGRATION_BACKOFF_MS
                        [(attempt as usize - 1).min(MIGRATION_BACKOFF_MS.len() - 1)];
                    warn!(
                        "启动期迁移命中临时锁/占用（第 {} 次尝试），{} ms 后重试: {e:#}",
                        attempt, backoff
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    attempt += 1;
                    continue;
                }
                Err(e) => break Err(e),
            }
        };
        match migration_result {
            Ok(summary) => {
                info!(
                    "启动期迁移矩阵完成: success={} altered_schema={} backfilled_meta={}",
                    summary.success, summary.altered_schema, summary.backfilled_meta
                );
            }
            Err(e) => {
                // P1：自动修复 + 临时重试均失败 → 进入全局只读保护（最后兜底，不删此机制）。
                readonly_mode.store(true, std::sync::atomic::Ordering::SeqCst);
                let reason = ReadonlyReason {
                    failed_step: "启动期数据迁移".to_string(),
                    last_error: format!("{e:#}"),
                    suggestion: "请先重启后端再次尝试自动修复；若重启后仍未恢复，\
                                 请还原升级前备份的 config/ 与 wal/ 目录后重启。"
                        .to_string(),
                };
                error!("启动期迁移矩阵失败，已进入全局只读保护模式: {e:?}");
                // P0-2：进入 readonly 前记录明确 reason（仅后端诊断用，info 级打印）。
                info!(
                    "readonly 原因: step={} | error={} | suggestion={}",
                    reason.failed_step, reason.last_error, reason.suggestion
                );
                readonly_reason_init = Some(reason);
            }
        }

        // 2) ClientPool 初始化（不在此处预热网络，预热由 main.rs 启动流程承担）
        let client_pool = Arc::new(RwLock::new(ClientPool::new()));

        // 3) active_uid 运行时真源
        let active_uid = Arc::new(RwLock::new(initial_active_uid));

        // 4) 🔥 BudgetScheduler 初始化
        //    按 AppConfig.multi_account_budget + multi_account_vip_recommended 构造，
        //    然后把所有已加载账号 add_account 进去。
        let budget_scheduler = build_budget_scheduler(&config).await;
        seed_budget_scheduler(&budget_scheduler, &account_manager).await;
        info!(
            "BudgetScheduler 已启动: M_download={}, M_upload={}",
            config.multi_account_budget.download_machine_budget.resolve(),
            config.multi_account_budget.upload_machine_budget.resolve()
        );

        // 5) 🔥 机器级解密 CPU 闸门
        //    全机器唯一一份，所有 per-uid ChunkScheduler 共享。
        let decrypt_semaphore = Arc::new(Semaphore::new(
            ChunkScheduler::calculate_decrypt_concurrency(),
        ));

        Ok(Self {
            qrcode_auth: Arc::new(RwLock::new(QRCodeAuth::new_with_proxy(proxy_config)?)),
            account_manager,
            client_pool,
            active_uid,
            session_manager: Arc::new(Mutex::new(SessionManager::default())),
            current_user: Arc::new(RwLock::new(None)),
            netdisk_client: Arc::new(RwLock::new(None)),
            folder_download_manager,
            // 🔥 多账号 per-uid Manager 池
            download_managers: Arc::new(DashMap::new()),
            upload_managers: Arc::new(DashMap::new()),
            transfer_managers: Arc::new(DashMap::new()),
            autobackup_managers: Arc::new(DashMap::new()),
            // 🔥 全局只读模式：
            // 由上方迁移路径预先创建并按需 store(true)，此处直接 move 进 self。
            readonly_mode,
            readonly_reason: Arc::new(RwLock::new(readonly_reason_init)),
            config: Arc::new(RwLock::new(config)),
            persistence_manager: Arc::new(Mutex::new(persistence_manager)),
            ws_manager,
            snapshot_manager,
            backup_record_manager,
            encryption_config_store: Arc::new(RwLock::new(None)),
            memory_monitor,
            cloud_dl_monitors: Arc::new(DashMap::new()),
            cloud_dl_ws_subscribers: Arc::new(DashMap::new()),
            cloud_dl_init_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            fallback_mgr,
            scan_manager: Arc::new(RwLock::new(None)),
            budget_scheduler,
            decrypt_semaphore,
            share_sync_manager: Arc::new(RwLock::new(None)),
        })
    }

    /// 初始化时加载会话
    pub async fn load_initial_session(&self) -> anyhow::Result<()> {
        // 🔥 获取持久化管理器的 Arc 引用（直接使用已启动的实例）
        let pm_arc = Arc::clone(&self.persistence_manager);

        // 从 AccountManager 读活跃账号，替代旧 session.json 单用户读取路径
        // session.json 已在 AppState::new 内被 migrate_from_session 改名为 .migrated
        let active_uid_opt = *self.active_uid.read().await;
        let active_user_opt = if let Some(uid) = active_uid_opt {
            let mgr = self.account_manager.lock().await;
            mgr.get_user(uid).cloned()
        } else {
            None
        };

        // session_manager 仍保留为旧字段（过渡期），但不再读 session.json
        let _session_manager_guard = self.session_manager.lock().await;
        if let Some(mut user_auth) = active_user_opt {
            *self.current_user.write().await = Some(user_auth.clone());

            // 初始化网盘客户端
            let config_guard = self.config.read().await;
            let proxy_config_for_client = if config_guard.network.proxy.proxy_type != ProxyType::None
                && !self.fallback_mgr.is_fallen_back()
            {
                Some(config_guard.network.proxy.clone())
            } else {
                None
            };
            drop(config_guard);

            let fallback_for_client = if proxy_config_for_client.is_some() {
                Some(Arc::clone(&self.fallback_mgr))
            } else {
                None
            };
            let client = NetdiskClient::new_with_proxy(
                user_auth.clone(),
                proxy_config_for_client.as_ref(),
                fallback_for_client.clone(),
            )?;

            // 设置代理回退管理器的用户代理配置
            // 🔥 始终从原始配置读取，而非 proxy_config_for_client（回退时后者为 None）
            {
                let cfg = self.config.read().await;
                if cfg.network.proxy.proxy_type != ProxyType::None {
                    self.fallback_mgr
                        .set_user_proxy_config(Some(cfg.network.proxy.clone()))
                        .await;
                }
            }

            // 预热过期时间（2小时 = 7200秒）
            const WARMUP_EXPIRE_SECS: i64 = 86400;

            // 检查是否需要预热：
            // 1. 预热数据不存在
            // 2. 或者预热数据已过期（超过24小时）
            //
            let need_warmup = if user_auth.panpsc.is_none()
                || user_auth.csrf_token.is_none()
                || user_auth.bdstoken.is_none()
            {
                info!("服务启动检测到会话未预热,开始预热...");
                true
            } else if let Some(last_warmup) = user_auth.last_warmup_at {
                let now = chrono::Utc::now().timestamp();
                let elapsed = now - last_warmup;
                if elapsed > WARMUP_EXPIRE_SECS {
                    info!(
                        "防止预热数据过期({}秒前),清除旧数据并重新预热...",
                        elapsed
                    );
                    // 清除过期的预热数据
                    user_auth.panpsc = None;
                    user_auth.csrf_token = None;
                    user_auth.bdstoken = None;
                    true
                } else {
                    info!(
                        "检测到已有预热 Cookie({}秒前预热),跳过预热",
                        elapsed
                    );
                    false
                }
            } else {
                // 有预热数据但没有时间戳（旧版本数据），执行预热
                info!("预热数据缺少时间戳,重新预热...");
                user_auth.panpsc = None;
                user_auth.csrf_token = None;
                user_auth.bdstoken = None;
                true
            };

            if need_warmup {
                match client.warmup_and_get_cookies().await {
                    Ok((panpsc, csrf_token, bdstoken, stoken)) => {
                        info!("预热成功,更新 session.json");
                        if panpsc.is_some() {
                            user_auth.panpsc = panpsc;
                        }
                        if csrf_token.is_some() {
                            user_auth.csrf_token = csrf_token;
                        }
                        user_auth.bdstoken = bdstoken;
                        user_auth.last_warmup_at = Some(chrono::Utc::now().timestamp());
                        // 预热时下发的 STOKEN 优先于之前保存的
                        if stoken.is_some() {
                            user_auth.stoken = stoken;
                        }

                        // 更新内存中的用户信息
                        *self.current_user.write().await = Some(user_auth.clone());

                        // 保存预热结果回 AccountManager
                        let mut mgr = self.account_manager.lock().await;
                        if let Err(e) = mgr.add_user(user_auth.clone()).await {
                            error!("保存预热 Cookie 到 accounts.json 失败: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("预热失败(可能需要重新登录): {}", e);
                    }
                }
            }

            let client_arc = Arc::new(client.clone());
            *self.netdisk_client.write().await = Some(client.clone());

            // 注入到 ClientPool（活跃账号）
            if let Some(uid) = active_uid_opt {
                let mut pool = self.client_pool.write().await;
                pool.add_client(uid, Arc::clone(&client_arc));
                info!(
                    "ClientPool: 活跃账号 uid={} 已注入（当前池大小 {}）",
                    uid,
                    pool.len()
                );
            }

            // 初始化下载管理器
            //
            // 通过 `resolve_effective_account_config` 结合 `auto_apply_recommended`
            // 与 VIP 推荐表算出该账号的 effective 值（dl_threads/dl_concurrent/dl_retries
            // /up_threads/up_concurrent/up_retries），传给 manager 工厂函数。
            let config = self.config.read().await;
            let download_dir = config.download.download_dir.clone();
            let (
                eff_dl_threads,
                eff_dl_concurrent,
                eff_dl_retries,
                eff_up_threads,
                eff_up_concurrent,
                eff_up_retries,
            ) = resolve_effective_account_config(&user_auth, &config);
            let max_global_threads = eff_dl_threads;
            let max_concurrent_tasks = eff_dl_concurrent;
            let max_retries = eff_dl_retries;
            drop(config);

            // BudgetScheduler + decrypt_semaphore 由 `new_for_account` 构造时直接注入。
            let manager = DownloadManager::new_for_account(
                user_auth.clone(),
                download_dir,
                max_global_threads,
                max_concurrent_tasks,
                max_retries,
                proxy_config_for_client.as_ref(),
                fallback_for_client,
                Arc::clone(&self.budget_scheduler),
                Arc::clone(&self.decrypt_semaphore),
            )?;
            let mut manager = manager;

            // 🔥 设置持久化管理器
            manager.set_persistence_manager(Arc::clone(&pm_arc));

            // 🔥 多账号：将活跃账号 UID 写入持久化管理器，
            // 使后续所有 register_* 写入的元数据自动带上 owner_uid。
            pm_arc.lock().await.set_owner_uid(user_auth.uid);
            info!("PersistenceManager owner_uid 已设置: {}", user_auth.uid);

            // owner_uid 已在 `new_for_account` 构造时由 `user_auth.uid` 直接注入，
            // 无需再调 set_owner_uid。

            // 🔥 设置 WebSocket 管理器
            manager.set_ws_manager(Arc::clone(&self.ws_manager)).await;

            // SAFETY 断言（chunk-level）+ budget acquire 在 spawn_chunk_download 默认启用。

            let manager_arc = Arc::new(manager);

            // 🔥 启动 auto_requeue 消费任务（scheduler 失败时退回等待队列）
            Arc::clone(&manager_arc).start_auto_requeue_consumer().await;

            // 设置文件夹下载管理器的依赖
            self.folder_download_manager
                .set_download_manager(Arc::clone(&manager_arc))
                .await;
            self.folder_download_manager
                .set_netdisk_client(client_arc)
                .await;

            // 🔥 设置文件夹下载管理器的 WAL 目录（用于文件夹持久化）
            let wal_dir = pm_arc.lock().await.wal_dir().clone();
            self.folder_download_manager.set_wal_dir(wal_dir.clone()).await;

            // 注入 ClientPool，支持按 folder.owner_uid 路由
            self.folder_download_manager
                .set_client_pool(Arc::clone(&self.client_pool))
                .await;

            // 注入 per-uid DownloadManager 池：
            // 让 FolderDownloadManager 按 `folder.owner_uid` 路由到对应账号的
            // 独立 manager（而非永远走 active 账号 manager 单例）。
            self.folder_download_manager
                .set_download_manager_pool(Arc::clone(&self.download_managers))
                .await;

            // 注入 per-uid UploadManager 池到 ScanManager：
            // 让扫描批量创建任务按 owner_uid 路由到对应账号 manager（避免 owner=B
            // 的扫描子任务被加进 A 的 manager）。
            if let Some(scan_mgr) = self.scan_manager.read().await.as_ref().cloned() {
                scan_mgr
                    .set_upload_manager_pool(Arc::clone(&self.upload_managers))
                    .await;
                info!("ScanManager per-uid upload 池已注入");
            }

            // 🔥 设置文件夹下载管理器的持久化管理器（用于加载历史文件夹）
            self.folder_download_manager
                .set_persistence_manager(Arc::clone(&pm_arc))
                .await;

            // 🔥 设置文件夹下载管理器的 WebSocket 管理器
            self.folder_download_manager
                .set_ws_manager(Arc::clone(&self.ws_manager))
                .await;

            // 🔥 设置下载管理器对文件夹管理器的引用（用于回收借调槽位）
            manager_arc
                .set_folder_manager(Arc::clone(&self.folder_download_manager))
                .await;

            // 初始化上传管理器
            //
            // 用 effective 配置覆盖 `upload_config` 的三个 manager 字段
            // （max_global_threads / max_concurrent_tasks / max_retries），
            // 这样 manager 一开始就按账号自定义配置初始化。
            let config = self.config.read().await;
            let mut upload_config = config.upload.clone();
            upload_config.max_global_threads = eff_up_threads;
            upload_config.max_concurrent_tasks = eff_up_concurrent;
            upload_config.max_retries = eff_up_retries;
            let transfer_config = config.transfer.clone();
            drop(config);

            // 🔥 配置目录（用于读取 encryption.json）
            // BudgetScheduler 由 `new_for_account` 构造时直接注入。
            let config_dir = std::path::Path::new("config");
            let upload_manager = UploadManager::new_for_account(
                client.clone(),
                &user_auth,
                &upload_config,
                Arc::clone(&self.budget_scheduler),
                config_dir,
            );
            let upload_manager_arc = Arc::new(upload_manager);

            // 🔥 设置持久化管理器
            upload_manager_arc
                .set_persistence_manager(Arc::clone(&pm_arc))
                .await;

            // 🔥 设置上传管理器的 WebSocket 管理器
            upload_manager_arc
                .set_ws_manager(Arc::clone(&self.ws_manager))
                .await;

            // SAFETY 断言 + budget acquire 在 spawn_chunk_upload 默认启用。

            // 🔥 设置备份记录管理器（用于文件夹名加密映射）
            upload_manager_arc
                .set_backup_record_manager(Arc::clone(&self.backup_record_manager))
                .await;

            // 🔥 初始化扫描管理器
            let config = self.config.read().await;
            let max_pending = config.scan.max_pending_tasks;
            drop(config);
            let scan_mgr = ScanManager::new_with_owner(
                Arc::clone(&upload_manager_arc),
                Arc::clone(&self.ws_manager),
                Arc::clone(&self.memory_monitor),
                wal_dir.clone(),
                max_pending,
                Some(crate::auth::Uid::new(user_auth.uid)),
            );
            *self.scan_manager.write().await = Some(Arc::new(scan_mgr));
            info!("扫描管理器初始化完成");

            // 初始化转存管理器
            let mut transfer_manager =
                TransferManager::new(Arc::new(std::sync::RwLock::new(client)), transfer_config, Arc::clone(&self.config));
            // 注入 owner_uid，使后续所有 TransferTask::new(...)
            // 链调 .with_owner_uid(self.owner_uid) 能写入正确归属（之前默认 0）
            transfer_manager.set_owner_uid(crate::auth::Uid::new(user_auth.uid));
            let transfer_manager_arc = Arc::new(transfer_manager);

            // 设置下载管理器（用于自动下载功能）
            transfer_manager_arc
                .set_download_manager(Arc::clone(&manager_arc))
                .await;

            // 设置文件夹下载管理器（用于自动下载文件夹）
            transfer_manager_arc
                .set_folder_download_manager(Arc::clone(&self.folder_download_manager))
                .await;

            // 🔥 设置持久化管理器
            transfer_manager_arc
                .set_persistence_manager(Arc::clone(&pm_arc))
                .await;

            // 🔥 设置转存管理器的 WebSocket 管理器
            transfer_manager_arc
                .set_ws_manager(Arc::clone(&self.ws_manager))
                .await;
            info!("转存管理器初始化完成");

            // 多账号 per-uid Manager 池注入：
            //
            // active uid 复用上面已构造的 3 个 manager（含 snapshot/encryption_config_store/
            // folder_manager 等已 wired 的依赖），其它 persisted uid 按各自
            // `UserAuth + custom_config` 单独构造一份独立 manager，写入 DashMap。
            //
            // 隔离效果：每账号 task_slot_pool / waiting_queue / chunk_scheduler /
            // max_concurrent_tasks / max_retries 都是独立实例。BudgetScheduler 仍是
            // 全机器单例（按 uid 分桶 + 全局闸门），与 manager 隔离正交。
            //
            // 注：非 active 账号的 manager 不会自动 wire snapshot_manager /
            // encryption_config_store / folder_manager。这些目前只对 active 账号有
            // 实际用途（备份记录、加密 / 共享文件夹下载映射 — 跟随 active_uid 切换路径
            // 走专门的 setter），切到该账号成为 active 时由 init 路径补齐。后续若需
            // 跨账号都享用这些依赖，可改在本循环统一调 setter。
            let active_uid_for_register = active_uid_opt;
            let persisted_users: Vec<UserAuth> = {
                let mgr = self.account_manager.lock().await;
                mgr.list_users().to_vec()
            };
            let mut registered_count = 0usize;
            // active uid：复用上面已 wired 的 manager
            if let Some(uid) = active_uid_for_register {
                self.register_account_managers(
                    uid,
                    Arc::clone(&manager_arc),
                    Arc::clone(&upload_manager_arc),
                    Arc::clone(&transfer_manager_arc),
                );
                registered_count += 1;
            }
            // 其它 persisted uid：每账号单独构造 3 个 manager
            for other_user in persisted_users
                .iter()
                .filter(|u| Some(Uid::new(u.uid)) != active_uid_for_register)
            {
                let other_uid = Uid::new(other_user.uid);
                if let Err(e) = self
                    .build_and_register_managers_for_account(
                        other_user.clone(),
                        Arc::clone(&pm_arc),
                    )
                    .await
                {
                    error!(
                        "为账号 uid={} 构造 per-uid manager 失败（跳过，本账号需切换为 active 后再次构造）: {}",
                        other_uid.raw(),
                        e
                    );
                    // 不阻塞其它账号；构造失败时 download_managers/.../ 该 uid 留空
                    // → 下次切到该账号或登录会重新尝试构造
                } else {
                    registered_count += 1;
                }
            }
            info!(
                "AppState: 已为 {} 个持久化账号注册 per-uid manager（独立实例，含 active）",
                registered_count
            );

            // 🔥 初始化离线下载监听服务
            self.init_cloud_dl_monitor().await;

            // 🔥 恢复任务
            self.recover_tasks(
                &manager_arc,
                &upload_manager_arc,
                &transfer_manager_arc,
                &pm_arc,
            )
                .await;

            // 🔥 启动时清理孤立临时目录（如果配置启用）
            transfer_manager_arc.cleanup_orphaned_on_startup_if_enabled().await;
        }

        // 🔥 启动 WebSocket 批量发送器
        Arc::clone(&self.ws_manager).start_batch_sender();
        info!("WebSocket 批量发送器已启动");

        // 🔥 启动内存监控器
        Arc::clone(&self.memory_monitor).start();
        info!("内存监控器已启动");

        // 🔥 初始化自动备份管理器
        self.init_autobackup_manager().await;

        // 所有 per-uid manager 在 autobackup（提供
        // encryption_config_store）初始化完成后统一注入运行时依赖。
        //
        // 此前只有 active 账号被显式 wire（见上面 active 分支注入 set_folder_manager
        // / set_snapshot_manager / set_encryption_config_store），其它持久化账号
        // 走 `build_and_register_managers_for_account` 缺这三件套 → 文件夹槽位
        // 借调/回收依赖 download_manager.folder_manager == None 时降级、加密下载
        // 缺 snapshot/key store。这里收口为对所有已注册账号统一调用 helper，
        // 实现"启动 / 登录 / 切换"三条路径同源。
        let registered_uids: Vec<Uid> = self
            .download_managers
            .iter()
            .map(|e| *e.key())
            .collect();
        for uid in registered_uids {
            if let Err(e) = self.wire_manager_runtime_deps_for_uid(uid).await {
                warn!(
                    "load_initial_session: uid={} wire_manager_runtime_deps 失败（继续）: {}",
                    uid.raw(),
                    e
                );
            }
        }

        // 🔥 初始化分享同步管理器
        self.init_share_sync_manager().await;

        Ok(())
    }

    /// 🔥 恢复持久化的任务
    async fn recover_tasks(
        &self,
        download_manager: &Arc<DownloadManager>,
        upload_manager: &Arc<UploadManager>,
        transfer_manager: &Arc<TransferManager>,
        pm: &Arc<Mutex<PersistenceManager>>,
    ) {
        let config = self.config.read().await;
        if !config.persistence.auto_recover_tasks {
            info!("任务自动恢复已禁用");
            return;
        }
        drop(config);

        info!("开始扫描可恢复的任务...");

        let wal_dir = pm.lock().await.wal_dir().clone();

        // 扫描可恢复的任务
        match scan_recoverable_tasks(&wal_dir) {
            Ok(mut scan_result) => {
                info!(
                    "扫描完成: {} 个下载任务, {} 个上传任务, {} 个转存任务, {} 个已完成, {} 个无效",
                    scan_result.download_tasks.len(),
                    scan_result.upload_tasks.len(),
                    scan_result.transfer_tasks.len(),
                    scan_result.completed_tasks.len(),
                    scan_result.invalid_tasks.len()
                );

                // ════════════════════════════════════════════════════════════
                // 多账号恢复分支分类
                //
                // 在分发给各 Manager 之前，对每个任务按 (owner_uid, active_uid, known_uids)
                // 进行分支分类：
                // - A `Normal` / C `LegacyFillActive`：保留（C 分支把 active_uid 回填到
                //   metadata.owner_uid，并**立即** `save_metadata` 写回 `.meta` 落盘，
                //   保证升级路径"第一次启动"就完整兼容掉旧数据，而不是依赖每次启动重复兜底）
                // - B `AccountDeleted` / D `UnrecoverableNoActive`：丢弃，logged warn
                // ════════════════════════════════════════════════════════════
                let known_uids: std::collections::HashSet<u64> = self
                    .account_manager
                    .lock()
                    .await
                    .list_users()
                    .iter()
                    .map(|u| u.uid)
                    .collect();
                let active_uid_raw: Option<u64> = self
                    .active_uid
                    .read()
                    .await
                    .as_ref()
                    .map(|u| u.raw());

                // 闭包：对一个 task 列表按分支过滤；wal_dir 借用用于 LegacyFillActive 写回 .meta
                let wal_dir_for_filter = wal_dir.clone();
                let filter_by_branch = |tasks: &mut Vec<crate::persistence::RecoveredTask>,
                                        kind: &str| {
                    let mut dropped_b = 0usize;
                    let mut dropped_d = 0usize;
                    let mut filled_c = 0usize;
                    let mut persist_failed = 0usize;
                    tasks.retain_mut(|t| {
                        let branch = crate::persistence::classify_recovery_branch(
                            t.metadata.owner_uid,
                            active_uid_raw,
                            &known_uids,
                        );
                        match branch {
                            crate::persistence::RecoveryBranch::Normal { .. } => true,
                            crate::persistence::RecoveryBranch::LegacyFillActive {
                                active_uid,
                            } => {
                                // C：把 active_uid 回填到内存元数据
                                t.metadata.owner_uid = Some(active_uid);
                                filled_c += 1;
                                // 🔥 立即写回 .meta 文件（升级路径"第一次启动完整兼容"）
                                if let Err(e) = crate::persistence::save_metadata(
                                    &wal_dir_for_filter,
                                    &t.metadata,
                                ) {
                                    warn!(
                                        "{} 任务 {} 回填 owner_uid 后 .meta 落盘失败: {}（内存仍正确，下次启动会重试）",
                                        kind, t.metadata.task_id, e
                                    );
                                    persist_failed += 1;
                                }
                                true
                            }
                            crate::persistence::RecoveryBranch::AccountDeleted {
                                missing_uid,
                            } => {
                                warn!(
                                    "{} 任务 {} 跳过恢复：account_deleted (owner_uid={})",
                                    kind, t.metadata.task_id, missing_uid
                                );
                                dropped_b += 1;
                                false
                            }
                            crate::persistence::RecoveryBranch::UnrecoverableNoActive => {
                                warn!(
                                    "{} 任务 {} 跳过恢复：unrecoverable_no_active_account",
                                    kind, t.metadata.task_id
                                );
                                dropped_d += 1;
                                false
                            }
                        }
                    });
                    if filled_c + dropped_b + dropped_d > 0 {
                        info!(
                            "{} 任务分支分类: legacy_fill_active={} (persist_failed={}), account_deleted={}, unrecoverable_no_active={}",
                            kind, filled_c, persist_failed, dropped_b, dropped_d
                        );
                    }
                };
                filter_by_branch(&mut scan_result.download_tasks, "下载");
                filter_by_branch(&mut scan_result.upload_tasks, "上传");
                filter_by_branch(&mut scan_result.transfer_tasks, "转存");
                filter_by_branch(&mut scan_result.failed_download_tasks, "下载(失败)");

                // 🔥 终态失败的下载任务并入下载恢复流程：from_recovered 会带上 is_failed/error，
                // restore_task 据此恢复为「失败」态（不分配槽位、不续传），重启后仍可见、可重试/删除。
                if !scan_result.failed_download_tasks.is_empty() {
                    info!(
                        "并入 {} 个终态失败下载任务到恢复流程",
                        scan_result.failed_download_tasks.len()
                    );
                    let mut failed = std::mem::take(&mut scan_result.failed_download_tasks);
                    scan_result.download_tasks.append(&mut failed);
                }

                // 清理已完成和无效的任务
                if !scan_result.completed_tasks.is_empty() {
                    cleanup_completed_tasks(&wal_dir, &scan_result.completed_tasks);
                }
                if !scan_result.invalid_tasks.is_empty() {
                    cleanup_invalid_tasks(&wal_dir, &scan_result.invalid_tasks);
                }

                // 🔥 先恢复文件夹任务（必须在恢复子任务之前）
                let (restored_folders, skipped_folders) = self.folder_download_manager.restore_folders().await;
                info!("文件夹任务恢复完成: 恢复 {} 个, 跳过 {} 个", restored_folders, skipped_folders);

                // 🔥 加载历史归档的已完成文件夹到内存（用于前端显示历史记录）
                let history_folders = self.folder_download_manager.load_history_folders_to_memory().await;
                if history_folders > 0 {
                    info!("历史文件夹加载完成: {} 个", history_folders);
                }

                // 恢复下载任务（子任务会关联到已恢复的文件夹）
                //
                // 按 `recovery_info.owner_uid` 分组到对应账号 manager。
                // 任务字段 owner_uid 必须与物理 manager 一致，否则 owner=B 任务被加进
                // A 的 manager → B 列表查不到、A 资源占用错位。
                // 现按 owner_uid 路由到 `download_manager_for(uid)`，缺失账号 manager 时
                // 计入 skipped。
                if !scan_result.download_tasks.is_empty() {
                    let recovery_infos: Vec<DownloadRecoveryInfo> = scan_result
                        .download_tasks
                        .iter()
                        .filter_map(DownloadRecoveryInfo::from_recovered)
                        .collect();

                    let mut by_uid: std::collections::HashMap<Uid, Vec<DownloadRecoveryInfo>> =
                        std::collections::HashMap::new();
                    let mut without_uid: Vec<DownloadRecoveryInfo> = Vec::new();
                    for info in recovery_infos {
                        match info.owner_uid {
                            Some(raw) if raw != 0 => by_uid
                                .entry(Uid::new(raw))
                                .or_default()
                                .push(info),
                            _ => without_uid.push(info),
                        }
                    }

                    let mut total_success = 0usize;
                    let mut total_failed = 0usize;
                    let mut skipped = 0usize;
                    for (uid, infos) in by_uid {
                        if let Some(dm) = self.download_manager_for(uid) {
                            let (s, f) = dm.restore_tasks(infos).await;
                            total_success += s;
                            total_failed += f;
                            info!(
                                "下载任务恢复: uid={} {} 成功, {} 失败",
                                uid.raw(),
                                s,
                                f
                            );
                        } else {
                            skipped += infos.len();
                            warn!(
                                "下载任务恢复跳过: uid={} 无对应 DownloadManager（{} 个任务）",
                                uid.raw(),
                                infos.len()
                            );
                        }
                    }
                    // owner_uid 缺失（已在 filter_by_branch 兜底为 active；这里基本不会进）
                    if !without_uid.is_empty() {
                        let (s, f) = download_manager.restore_tasks(without_uid).await;
                        total_success += s;
                        total_failed += f;
                    }
                    info!(
                        "下载任务恢复完成: {} 成功, {} 失败, {} 跳过（无 manager）",
                        total_success, total_failed, skipped
                    );

                    // 🔥 同步恢复的子任务进度到文件夹
                    self.folder_download_manager.sync_restored_tasks_progress().await;
                }

                // 🔥 恢复模式补任务：从 pending_files 创建暂停状态的任务
                // 让前端能看到"等待/暂停"任务，但不会自动开始下载
                // 用户点击"继续"时才进入调度队列
                if restored_folders > 0 {
                    let prefilled = self.folder_download_manager.prefill_paused_tasks(10).await;
                    info!("恢复模式补任务完成: 创建 {} 个暂停任务", prefilled);
                }

                // 恢复上传任务（按 owner_uid 分组到对应账号 manager）
                if !scan_result.upload_tasks.is_empty() {
                    let recovery_infos: Vec<UploadRecoveryInfo> = scan_result
                        .upload_tasks
                        .iter()
                        .filter_map(UploadRecoveryInfo::from_recovered)
                        .collect();

                    let mut by_uid: std::collections::HashMap<Uid, Vec<UploadRecoveryInfo>> =
                        std::collections::HashMap::new();
                    let mut without_uid: Vec<UploadRecoveryInfo> = Vec::new();
                    for info in recovery_infos {
                        match info.owner_uid {
                            Some(raw) if raw != 0 => by_uid
                                .entry(Uid::new(raw))
                                .or_default()
                                .push(info),
                            _ => without_uid.push(info),
                        }
                    }

                    let mut total_success = 0usize;
                    let mut total_failed = 0usize;
                    let mut skipped = 0usize;
                    for (uid, infos) in by_uid {
                        if let Some(um) = self.upload_manager_for(uid) {
                            let (s, f) = um.restore_tasks(infos).await;
                            total_success += s;
                            total_failed += f;
                            info!("上传任务恢复: uid={} {} 成功, {} 失败", uid.raw(), s, f);
                        } else {
                            skipped += infos.len();
                            warn!(
                                "上传任务恢复跳过: uid={} 无对应 UploadManager（{} 个任务）",
                                uid.raw(),
                                infos.len()
                            );
                        }
                    }
                    if !without_uid.is_empty() {
                        let (s, f) = upload_manager.restore_tasks(without_uid).await;
                        total_success += s;
                        total_failed += f;
                    }
                    info!(
                        "上传任务恢复完成: {} 成功, {} 失败, {} 跳过（无 manager）",
                        total_success, total_failed, skipped
                    );
                }

                // 恢复转存任务（按 owner_uid 分组到对应账号 manager）
                if !scan_result.transfer_tasks.is_empty() {
                    let recovery_infos: Vec<TransferRecoveryInfo> = scan_result
                        .transfer_tasks
                        .iter()
                        .filter_map(TransferRecoveryInfo::from_recovered)
                        .collect();

                    let mut by_uid: std::collections::HashMap<Uid, Vec<TransferRecoveryInfo>> =
                        std::collections::HashMap::new();
                    let mut without_uid: Vec<TransferRecoveryInfo> = Vec::new();
                    for info in recovery_infos {
                        match info.owner_uid {
                            Some(raw) if raw != 0 => by_uid
                                .entry(Uid::new(raw))
                                .or_default()
                                .push(info),
                            _ => without_uid.push(info),
                        }
                    }

                    let mut total_success = 0usize;
                    let mut total_failed = 0usize;
                    let mut skipped = 0usize;
                    for (uid, infos) in by_uid {
                        if let Some(tm) = self.transfer_manager_for(uid) {
                            let (s, f) = tm.restore_tasks(infos).await;
                            total_success += s;
                            total_failed += f;
                            info!("转存任务恢复: uid={} {} 成功, {} 失败", uid.raw(), s, f);
                        } else {
                            skipped += infos.len();
                            warn!(
                                "转存任务恢复跳过: uid={} 无对应 TransferManager（{} 个任务）",
                                uid.raw(),
                                infos.len()
                            );
                        }
                    }
                    if !without_uid.is_empty() {
                        let (s, f) = transfer_manager.restore_tasks(without_uid).await;
                        total_success += s;
                        total_failed += f;
                    }
                    info!(
                        "转存任务恢复完成: {} 成功, {} 失败, {} 跳过（无 manager）",
                        total_success, total_failed, skipped
                    );
                }
            }
            Err(e) => {
                error!("扫描可恢复任务失败: {}", e);
            }
        }
    }

    /// 🔥 初始化自动备份管理器
    ///
    /// 进程级单例语义：
    /// - 首次调用：构造 `AutoBackupManager`，注入所有依赖（含 per-uid 池），启动
    ///   event consumer / transfer listeners，注册到 `autobackup_managers` 池。
    /// - 后续调用（再次登录、新增账号等）：检测到 `autobackup_managers` 已有实例 →
    ///   不重新构造、不重启 event loop / transfer listeners（避免重复 poll / 事件双发）；
    ///   只确保新 active uid 的注册位 + 触发一次 backfill。
    /// - 同时显式注入 `upload_manager_pool` / `download_manager_pool` —— 之前
    ///   遗漏导致 `BackupConfig.owner_uid` 路由会 fallback 到 legacy 单 manager，
    ///   非 active 账号的备份子任务被错派到 active 账号。
    pub async fn init_autobackup_manager(&self) {
        use std::path::PathBuf;

        // 单例复用：池中已有实例 → 只补当前 active 注册位
        let existing: Option<Arc<AutoBackupManager>> = {
            // DashMap iter()，取池中第一个实例（按 autobackup_manager_for 同款语义）
            self.autobackup_managers
                .iter()
                .next()
                .map(|e| Arc::clone(e.value()))
        };
        if let Some(manager_arc) = existing {
            info!(
                "init_autobackup_manager: 已存在进程级单例，跳过重复初始化（仅补 active 注册位）"
            );
            // 重新注入 per-uid 池（幂等：池已经在；DashMap Arc 同份）— 防止之前漏注入
            manager_arc.set_upload_manager_pool(Arc::clone(&self.upload_managers));
            manager_arc.set_download_manager_pool(Arc::clone(&self.download_managers));
            manager_arc.set_client_pool(Arc::clone(&self.client_pool));

            // 刷新 transfer notification sender 到所有 per-uid manager
            // — 新登录账号在登录链路里被加入 download_managers / upload_managers 池，但
            // 它们的 manager 还没绑定 backup_notification_sender；不刷新会让该账号的备份
            // 子任务完成事件丢失，父备份任务卡在 Transferring。
            manager_arc.refresh_transfer_listener_bindings().await;

            // 把单例注册到当前 active uid（如果还没注册），让 autobackup_manager_for(active)
            // 直接命中而不靠 fallback
            if let Some(active_uid) = *self.active_uid.read().await {
                if !self.autobackup_managers.contains_key(&active_uid) {
                    self.register_account_autobackup(active_uid, Arc::clone(&manager_arc));
                }
                // 单账号场景仍可补 backfill（多账号场景内部会跳过）
                let account_count = self.account_manager.lock().await.list_users().len();
                if let Err(e) = manager_arc
                    .backfill_configs_owner_uid(active_uid.raw(), account_count)
                    .await
                {
                    error!("回填备份配置 owner_uid 失败: {}", e);
                }
            }
            return;
        }

        // 从配置读取路径（db_path 使用全局 persistence 配置）
        let config = self.config.read().await;
        let config_path = PathBuf::from(&config.autobackup.config_path);
        let db_path = PathBuf::from(&config.persistence.db_path);
        let temp_dir = PathBuf::from(&config.autobackup.temp_dir);
        // 保存触发配置用于初始化全局轮询
        let upload_trigger = config.autobackup.upload_trigger.clone();
        let download_trigger = config.autobackup.download_trigger.clone();
        drop(config);

        match AutoBackupManager::new(
            config_path,
            db_path,
            temp_dir,
            Arc::clone(&self.backup_record_manager),
            Arc::clone(&self.snapshot_manager),
        ).await {
            Ok(manager) => {
                // 设置 WebSocket 管理器
                manager.set_ws_manager(Arc::clone(&self.ws_manager));

                // 设置上传管理器（用于执行备份上传）
                if let Some(upload_mgr) = self.upload_manager_for_active().await {
                    manager.set_upload_manager(upload_mgr);
                }

                // 设置下载管理器（用于执行备份下载）
                if let Some(download_mgr) = self.download_manager_for_active().await {
                    manager.set_download_manager(download_mgr);
                }

                // 注入 per-uid 池让备份子任务能按
                // BackupConfig.owner_uid 路由到目标账号 manager（之前漏调
                // set_*_manager_pool，pool 未注入时会 fallback 到 legacy 单 manager
                // 误派到 active 账号 — 多账号串账号风险）。
                manager.set_upload_manager_pool(Arc::clone(&self.upload_managers));
                manager.set_download_manager_pool(Arc::clone(&self.download_managers));

                // 注入 ClientPool，让远端扫描/下载/同步快照
                // 路径按 owner_uid 严格路由 client，禁止 legacy session.json fallback
                manager.set_client_pool(Arc::clone(&self.client_pool));

                // 设置代理配置（使备份任务的 NetdiskClient 走代理）
                {
                    let config_guard = self.config.read().await;
                    let proxy = if config_guard.network.proxy.proxy_type != crate::common::ProxyType::None
                        && !self.fallback_mgr.is_fallen_back()
                    {
                        Some(config_guard.network.proxy.clone())
                    } else {
                        None
                    };
                    manager.set_proxy_config(proxy, Arc::clone(&self.fallback_mgr));
                }

                // 🔥 注入 snapshot_manager 到 DownloadManager 和 UploadManager
                // 使用 AppState 中已创建的 snapshot_manager（而非从 manager 获取）
                let encryption_config_store = manager.get_encryption_config_store();

                // 把 encryption_config_store 提到 AppState 层
                // 让 `wire_manager_runtime_deps_for_uid` 后续给所有 per-uid manager 共用
                *self.encryption_config_store.write().await =
                    Some(Arc::clone(&encryption_config_store));

                // 注入到下载管理器（用于解密时查询原始文件名和 key_version）
                if let Some(download_mgr) = self.download_manager_for_active().await {
                    download_mgr.set_snapshot_manager(Arc::clone(&self.snapshot_manager)).await;
                    download_mgr.set_encryption_config_store(Arc::clone(&encryption_config_store)).await;
                    info!("已将 snapshot_manager 和 encryption_config_store 注入到下载管理器");
                }

                // 注入到上传管理器（用于上传完成后保存加密映射）
                if let Some(upload_mgr) = self.upload_manager_for_active().await {
                    upload_mgr.set_snapshot_manager(Arc::clone(&self.snapshot_manager)).await;
                    info!("已将 snapshot_manager 注入到上传管理器");
                }

                let manager_arc = Arc::new(manager);

                // 🔥 初始化全局轮询（使用配置文件中的触发配置）
                manager_arc.update_trigger_config(upload_trigger, download_trigger).await;

                // 启动事件消费循环（监听文件变更和定时轮询事件）
                manager_arc.start_event_consumer().await;

                // 🔥 启动传输完成监听器（监听上传/下载任务完成，更新备份任务状态）
                manager_arc.start_transfer_listeners().await;

                // 🔥 多账号 per-uid 池注入
                if let Some(uid) = *self.active_uid.read().await {
                    self.register_account_autobackup(uid, Arc::clone(&manager_arc));

                    // 回填老 BackupConfig 缺失的 owner_uid
                    // 单账号场景：填入 active_uid 并落盘
                    // 多账号场景：跳过（避免误归属）
                    let account_count = self.account_manager.lock().await.list_users().len();
                    match manager_arc
                        .backfill_configs_owner_uid(uid.raw(), account_count)
                        .await
                    {
                        Ok(0) => {}
                        Ok(n) => info!("启动期回填备份配置 owner_uid: {} 条", n),
                        Err(e) => error!("回填备份配置 owner_uid 失败: {}", e),
                    }
                }

                info!("自动备份管理器初始化完成");
            }
            Err(e) => {
                error!("自动备份管理器初始化失败: {}", e);
            }
        }
    }

    /// 🔥 初始化指定账号的离线下载监听服务
    ///
    /// 每个账号独立一个监听实例，插入 `cloud_dl_monitors` 池。严格按
    /// `client_pool.get_client(uid)` 取，缺失时直接 `warn` 跳过 — 调用方
    /// 应先 `ensure_client_for_uid(uid)` 兜底懒加载。
    /// 重复初始化同一 uid 会幂等跳过。
    pub async fn init_cloud_dl_monitor_for(&self, uid: Uid) {
        // 先取 per-uid init lock 双 check，避免并发初始化。
        let init_lock = {
            let mut locks = self.cloud_dl_init_locks.lock().await;
            locks
                .entry(uid)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _init_guard = init_lock.lock().await;

        if self.cloud_dl_monitors.contains_key(&uid) {
            info!(
                "CloudDlMonitor uid={} 已存在，跳过重复初始化",
                uid.raw()
            );
            return;
        }

        // 严格按 per-uid client_pool 取：
        // 不回退 legacy `netdisk_client`，避免把 active 客户端绑定到非 active uid。
        let client_opt: Option<NetdiskClient> = {
            let pool = self.client_pool.read().await;
            pool.get_client(uid).map(|arc| (*arc).clone())
        };
        let client = match client_opt {
            Some(c) => c,
            None => {
                warn!(
                    "CloudDlMonitor uid={} 初始化失败：client_pool 内无该 uid 客户端（调用方应先 ensure_client_for_uid 兜底懒加载）",
                    uid.raw()
                );
                return;
            }
        };

        // 创建监听服务
        // 传入 owner_uid 让 monitor 推送 WS 事件时 stamp owner_uid
        let monitor = CloudDlMonitor::new(uid, Arc::new(client));

        // 设置 WebSocket 管理器
        monitor.set_ws_manager(Arc::clone(&self.ws_manager)).await;

        // 设置数据库路径（用于持久化自动下载配置）
        let config = self.config.read().await;
        let db_path = std::path::PathBuf::from(&config.persistence.db_path);
        drop(config);
        monitor.set_db_path(db_path).await;

        // 下载管理器严格按 uid 取，不回退到 active
        // （否则 uid=B 的 monitor 内部把任务推到 uid=A 的 download manager 还是会串号）。
        if let Some(dm) = self.download_managers.get(&uid).map(|r| Arc::clone(r.value())) {
            monitor.set_download_manager(dm).await;
        } else {
            warn!(
                "CloudDlMonitor uid={} 初始化时 download_manager 缺失（自动下载功能将不可用，下次该 uid 切到 active 时 init 链路会补齐）",
                uid.raw()
            );
        }

        // 🔥 设置文件夹下载管理器（FolderDownloadManager 全局共享）
        monitor.set_folder_download_manager(Arc::clone(&self.folder_download_manager)).await;

        // 从数据库加载未触发的自动下载配置
        let loaded = monitor.load_auto_download_configs_from_db().await;
        if loaded > 0 {
            info!(
                "CloudDlMonitor uid={} 已恢复 {} 个自动下载配置",
                uid.raw(),
                loaded
            );
        }

        let monitor_arc = Arc::new(monitor);

        // 启动后台监听任务
        let monitor_clone = Arc::clone(&monitor_arc);
        tokio::spawn(async move {
            monitor_clone.start().await;
        });

        self.cloud_dl_monitors.insert(uid, monitor_arc);
        info!("CloudDlMonitor uid={} 初始化完成", uid.raw());
    }

    /// 🔥 初始化分享同步管理器
    pub async fn init_share_sync_manager(&self) {
        use crate::share_sync::events::{ShareSyncEvent, ShareSyncEventPublisher};
        use crate::share_sync::manager::ManagerConfig;
        use crate::server::events::TaskEvent;
        use std::sync::Arc;

        let config = self.config.read().await;

        // 订阅表已并入主库 `baidu-pcs.db`：随主库一起备份/恢复，并天然支持
        // 按 owner_uid 多账号隔离。`config_path` 仅用于一次性导入早期版本可能
        // 残留的独立 `share_sync/subscriptions.json`（导入后改名 .migrated）。
        let db_path = std::path::PathBuf::from(&config.persistence.db_path);
        let parent = db_path.parent().unwrap_or(std::path::Path::new("."));
        let config_path = parent.join("share_sync").join("subscriptions.json");
        drop(config);

        // 把 WS Manager 包装为 ShareSyncEventPublisher。
        // WS 投递按订阅模式（category/type/task_id）匹配，`group_id` 用于子任务
        // 分组、并非账号维度；should_send_event 对所有模块都不按 owner_uid 过滤，
        // 故此处仍传 None。多账号访问隔离在 handler 层（按 active_uid 过滤
        // 列表/CRUD/触发）与执行层（resolver 按 owner_uid 解析客户端）强制保证。
        struct WsPublisher {
            ws: Arc<crate::server::websocket::manager::WebSocketManager>,
        }
        impl ShareSyncEventPublisher for WsPublisher {
            fn publish(&self, event: ShareSyncEvent) {
                self.ws
                    .send_if_subscribed(TaskEvent::ShareSync(event), None);
            }
        }

        let publisher: Arc<dyn ShareSyncEventPublisher> = Arc::new(WsPublisher {
            ws: Arc::clone(&self.ws_manager),
        });

        // 多账号隔离核心：解析器按每条订阅的 owner_uid 实时解析**该账号**的
        // NetdiskClient / TransferManager（读 per-uid 池）。后台调度对账号 A 的
        // 订阅始终用账号 A 的实例；账号切换无需 relink。
        let resolver: Arc<dyn ShareSyncAccountResolver> = Arc::new(AppStateShareSyncResolver {
            client_pool: Arc::clone(&self.client_pool),
            transfer_managers: Arc::clone(&self.transfer_managers),
        });

        let cfg = ManagerConfig {
            config_path,
            db_path,
            resolver,
            publisher: Some(publisher),
        };

        match ShareSyncManager::new(cfg).await {
            Ok(manager) => {
                info!("分享同步管理器初始化完成");
                *self.share_sync_manager.write().await = Some(manager);
            }
            Err(e) => {
                error!("分享同步管理器初始化失败: {}", e);
            }
        }
    }

    /// 🔥 初始化活跃账号的离线下载监听服务（兼容 legacy 调用点）
    ///
    /// 为保持 `auth.rs` / `load_initial_session` 中现有调用点不变，本方法
    /// 读取当前 `active_uid` 并转发到 `init_cloud_dl_monitor_for(uid)`。某
    /// 些场景下 active_uid 为 None（未登录） → 跳过并计入 warn。
    pub async fn init_cloud_dl_monitor(&self) {
        let active = *self.active_uid.read().await;
        match active {
            Some(uid) => self.init_cloud_dl_monitor_for(uid).await,
            None => warn!("init_cloud_dl_monitor: active_uid 为 None，跳过初始化"),
        }
    }

    /// 获取活跃账号的 `CloudDlMonitor`（返回 `None` 表示未初始化）
    pub async fn cloud_dl_monitor_for_active(&self) -> Option<Arc<CloudDlMonitor>> {
        let uid = (*self.active_uid.read().await)?;
        self.cloud_dl_monitors.get(&uid).map(|r| Arc::clone(r.value()))
    }

    /// 按 UID 查找 `CloudDlMonitor`
    pub fn cloud_dl_monitor_for(&self, uid: Uid) -> Option<Arc<CloudDlMonitor>> {
        self.cloud_dl_monitors.get(&uid).map(|r| Arc::clone(r.value()))
    }

    /// 遍历所有已存在的 `CloudDlMonitor`（代理热更 / 接受 shutdown 使用）
    pub fn iter_cloud_dl_monitors(&self) -> Vec<(Uid, Arc<CloudDlMonitor>)> {
        self.cloud_dl_monitors
            .iter()
            .map(|r| (*r.key(), Arc::clone(r.value())))
            .collect()
    }

    /// 🔥 CloudDl WS 订阅按账号切换重绑。
    ///
    /// 由 `helpers::set_active_uid` 在切换 `active_uid` 完成后调用：
    /// 1. 收集 `cloud_dl_ws_subscribers` 中所有已记录的 `(connection_id, old_uid)`。
    /// 2. 补扫 `WebSocketManager` 中 topic 订阅匹配 `cloud_dl` / `cloud_dl:*` 的连接。
    /// 3. 若 `new_uid = Some`，先 lazy init 目标 monitor 后整体迁移；任何步骤失败 → 保留旧映射不动。
    /// 4. 若 `new_uid = None`（删除最后账号）→ 清空所有 cloud_dl_ws_subscribers 映射。
    ///
    /// 结构性消除"切账号后旧 monitor 仍计为有订阅者继续轮询、新 monitor 没订阅者不轮询"
    /// 的串号/丢事件问题。
    pub async fn rebind_cloud_dl_ws_subscribers(&self, new_uid: Option<Uid>) {
        // 收集已记录映射
        let mut entries: Vec<(String, Option<Uid>)> = self
            .cloud_dl_ws_subscribers
            .iter()
            .map(|r| (r.key().clone(), Some(*r.value())))
            .collect();

        // 补扫 topic 订阅了 cloud_dl 但映射缺失的连接
        if new_uid.is_some() {
            let topic_subs = self
                .ws_manager
                .connections_matching_subscription(|s| {
                    s == "cloud_dl" || s.starts_with("cloud_dl:")
                });
            for conn_id in topic_subs {
                if !self.cloud_dl_ws_subscribers.contains_key(&conn_id) {
                    entries.push((conn_id, None));
                }
            }
        }

        if entries.is_empty() {
            return;
        }

        // 若有目标 uid，先 lazy init monitor 确保新 monitor 就绪
        let new_monitor = match new_uid {
            Some(uid) => {
                // 在 lazy init monitor 之前，先 ensure
                // 该 uid 的 client 已在池中。否则 init_cloud_dl_monitor_for 会因
                // `client_pool.get_client(uid)` 缺失而 warn 跳过，导致 rebind 后
                // 仍然没有目标 monitor，cloud_dl 订阅卡在旧账号 monitor 上不被
                // 转移（事件串号）。失败 → 跳过本次 rebind，下次切换重试。
                if let Err(e) = self.ensure_client_for_uid(uid).await {
                    warn!(
                        "rebind_cloud_dl_ws_subscribers: ensure_client_for_uid uid={} 失败: {}（保留旧映射，下次切换重试）",
                        uid.raw(),
                        e
                    );
                    return;
                }
                if !self.cloud_dl_monitors.contains_key(&uid) {
                    self.init_cloud_dl_monitor_for(uid).await;
                }
                self.cloud_dl_monitor_for(uid)
            }
            None => None,
        };

        // 事务性 swap
        for (conn_id, old_uid_opt) in entries {
            if let Some(old_uid) = old_uid_opt {
                if let Some(old_monitor) = self.cloud_dl_monitor_for(old_uid) {
                    old_monitor.remove_subscriber();
                }
            }
            match (&new_monitor, new_uid) {
                (Some(new_m), Some(uid)) => {
                    new_m.add_subscriber();
                    self.cloud_dl_ws_subscribers.insert(conn_id, uid);
                }
                (None, None) => {
                    self.cloud_dl_ws_subscribers.remove(&conn_id);
                }
                _ => {
                    // new_monitor 没拿到（lazy init 失败）：保留旧映射，等下次切换重试
                    warn!("rebind_cloud_dl_ws_subscribers: lazy init monitor 失败，保留旧映射");
                }
            }
        }

        info!(
            "rebind_cloud_dl_ws_subscribers: cloud_dl 订阅已重绑到 uid={:?}",
            new_uid.map(|u| u.raw())
        );
    }

    /// 🔥 手动触发预热
    ///
    /// 当 API 返回特定错误码（如 errno=-6）时，可调用此方法重新预热会话。
    /// 预热成功后会自动更新 `accounts.json` 中对应账号的 cookie 字段。
    ///
    /// 统一按 `active_uid → AccountManager.get_user → client_pool.get_client` 取，
    /// 写回走 `AccountManager.add_user`（add_user 是 upsert，等价于"更新已存在用户"）。
    /// 这样切账号后这些字段不会指向上一活跃账号。
    ///
    /// # 返回值
    /// - `Ok(true)` - 预热成功
    /// - `Ok(false)` - 无需预热（未登录或客户端未初始化）
    /// - `Err(e)` - 预热失败
    pub async fn trigger_warmup(&self) -> anyhow::Result<bool> {
        // 1) 取活跃 uid
        let uid = match *self.active_uid.read().await {
            Some(uid) => uid,
            None => {
                warn!("trigger_warmup: 未登录，无法执行预热");
                return Ok(false);
            }
        };

        // 2) 取活跃账号客户端（按 active_uid 路由，唯一真源）
        let client = match self.client_pool.read().await.get_client(uid) {
            Some(c) => (*c).clone(),
            None => {
                warn!(
                    "trigger_warmup: client_pool uid={} 缺失客户端",
                    uid.raw()
                );
                return Ok(false);
            }
        };

        // 3) 取活跃账号 UserAuth 副本（从 AccountManager，单一持久化真源）
        let mut user_auth = {
            let am = self.account_manager.lock().await;
            match am.get_user(uid) {
                Some(u) => u.clone(),
                None => {
                    warn!(
                        "trigger_warmup: AccountManager 中未找到 uid={}",
                        uid.raw()
                    );
                    return Ok(false);
                }
            }
        };

        info!("手动触发预热 uid={}...", uid.raw());

        // 清除旧的预热数据
        user_auth.panpsc = None;
        user_auth.csrf_token = None;
        user_auth.bdstoken = None;

        // 执行预热
        match client.warmup_and_get_cookies().await {
            Ok((panpsc, csrf_token, bdstoken, stoken)) => {
                info!("手动预热成功 uid={}，更新 accounts.json", uid.raw());
                if panpsc.is_some() {
                    user_auth.panpsc = panpsc;
                }
                if csrf_token.is_some() {
                    user_auth.csrf_token = csrf_token;
                }
                user_auth.bdstoken = bdstoken;
                user_auth.last_warmup_at = Some(chrono::Utc::now().timestamp());

                // 预热时下发的 STOKEN 优先于之前保存的
                if stoken.is_some() {
                    user_auth.stoken = stoken;
                }

                // 保存到 accounts.json（add_user 在已存在时更新）
                {
                    let mut am = self.account_manager.lock().await;
                    if let Err(e) = am.add_user(user_auth.clone()).await {
                        error!(
                            "trigger_warmup uid={}: 保存预热 cookie 到 accounts.json 失败: {}",
                            uid.raw(),
                            e
                        );
                    }
                }

                // 同步更新 legacy 字段（向后兼容期保留）：仅当本次预热的就是当前活跃账号
                if *self.active_uid.read().await == Some(uid) {
                    *self.current_user.write().await = Some(user_auth);
                }

                Ok(true)
            }
            Err(e) => {
                error!("手动预热失败 uid={}: {}", uid.raw(), e);
                Err(anyhow::anyhow!("预热失败: {}", e))
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // 🔥 多账号 per-uid Manager 池访问 API
    // ════════════════════════════════════════════════════════════════════════

    /// 取**当前活跃账号**的 `NetdiskClient`（多账号路由唯一入口）。
    ///
    /// **替代** legacy `state.netdisk_client.read().await` 直接读取模式：
    /// - 旧模式两个真源（`active_uid` 与 `netdisk_client` 字段）切换时不原子，
    ///   存在微秒级竞态窗口；
    /// - 新模式每次原子读 `active_uid → client_pool[uid]`，无竞态；
    /// - 单一真源（`active_uid` 是运行时真源）。
    ///
    /// 返回 `None` 表示未登录或客户端未初始化（handler 应返回 401）。
    pub async fn active_client(&self) -> Option<Arc<NetdiskClient>> {
        let uid = (*self.active_uid.read().await)?;
        self.client_pool.read().await.get_client(uid)
    }

    /// 取**当前活跃账号**的 `UserAuth`（取代 `current_user` 直读）。
    ///
    /// 单一真源：`active_uid → AccountManager.get_user`。预热后回写也走 `AccountManager`，
    /// 避免账号切换后 `current_user` 字段滞留旧账号导致代理热更/预热串账号。
    ///
    /// 返回 `None` 表示未登录或 `AccountManager` 中无该账号。
    pub async fn active_user_auth(&self) -> Option<crate::auth::UserAuth> {
        let uid = (*self.active_uid.read().await)?;
        let am = self.account_manager.lock().await;
        am.get_user(uid).cloned()
    }

    /// 取**指定账号** uid 的 `DownloadManager`。
    ///
    /// **多账号语义（per-uid 独立实例）**：
    /// - 启动 / 登录链路（`build_and_register_managers_for_account`）为每个账号构造
    ///   **独立的** `DownloadManager` 并分别注册到 `download_managers` 池
    /// - 同一进程内不同 uid 的 manager 实例**互不共享**（独立 task_slot_pool /
    ///   chunk_scheduler / persistence override 等）
    /// - 对未知 uid（账号已删除 / 错误传参）严格返回 `None`，handler 应返回 400 / 404
    /// - **不回退到池中任意 manager**——之前的 fallback 会让 `?uid=X` 查询拿到错账号
    ///   的任务列表，破坏隔离。
    ///
    /// 注：FolderDownloadManager / ScanManager / AutoBackupManager 仍是进程级共享/单例
    /// 组件（按 owner_uid 路由内部状态）；本方法只描述 per-uid manager 池语义。
    pub fn download_manager_for(&self, uid: Uid) -> Option<Arc<DownloadManager>> {
        self.download_managers.get(&uid).map(|e| Arc::clone(e.value()))
    }

    /// 取**当前活跃账号**的 `DownloadManager`（无活跃账号时返回 `None`）。
    pub async fn download_manager_for_active(&self) -> Option<Arc<DownloadManager>> {
        let uid = (*self.active_uid.read().await)?;
        self.download_manager_for(uid)
    }

    /// 取指定账号 uid 的 `UploadManager`（语义同 `download_manager_for`）
    pub fn upload_manager_for(&self, uid: Uid) -> Option<Arc<UploadManager>> {
        self.upload_managers.get(&uid).map(|e| Arc::clone(e.value()))
    }

    /// 取活跃账号的 `UploadManager`
    pub async fn upload_manager_for_active(&self) -> Option<Arc<UploadManager>> {
        let uid = (*self.active_uid.read().await)?;
        self.upload_manager_for(uid)
    }

    /// 取指定账号 uid 的 `TransferManager`（语义同 `download_manager_for`）
    pub fn transfer_manager_for(&self, uid: Uid) -> Option<Arc<TransferManager>> {
        self.transfer_managers.get(&uid).map(|e| Arc::clone(e.value()))
    }

    /// 取活跃账号的 `TransferManager`
    pub async fn transfer_manager_for_active(&self) -> Option<Arc<TransferManager>> {
        let uid = (*self.active_uid.read().await)?;
        self.transfer_manager_for(uid)
    }

    /// 从**全局共享**历史库读取任务的 `owner_uid`（用于跨账号精确路由）。
    ///
    /// `persistence_manager` 是进程级单例（所有账号共享同一张 `task_history` 表），
    /// `get_history_task(task_id)` 仅按 `task_id` 查、返回行内的 `owner_uid` 列。
    /// 因此「任务是否在历史库」对每个账号都为真，无法判定归属；唯一可靠的归属
    /// 信号是历史行自身的 `owner_uid`。
    ///
    /// ⚠️ **必须传入 `expected` 任务类型**：历史库是 download/upload/transfer
    /// 共用的一张表，仅按 `task_id` 查不区分类型。若不校验类型，`/downloads/:id`
    /// 传一个历史 transfer/upload 的 id 也会路由到该 owner 的 `DownloadManager`，
    /// 随后 `delete_task` 会按 `task_id` 删掉**非下载**的历史行（`on_task_deleted`
    /// → `remove_task_from_history` 不区分类型）。故历史命中必须
    /// `metadata.task_type == expected` 才据其 `owner_uid` 路由。
    ///
    /// （内存扫描分支天然类型安全：每个 per-uid manager 的内存只存自己那一类任务。）
    ///
    /// 返回 `None` 表示历史中无此任务、类型不匹配，或 `owner_uid` 缺失 / 为 0
    /// （legacy 占位，无法据此精确归属——调用方应回退到内存扫描结果或 active manager）。
    async fn history_owner_uid(&self, task_id: &str, expected: TaskType) -> Option<Uid> {
        let pm = self.persistence_manager.lock().await;
        pm.get_history_task(task_id)
            .filter(|m| m.task_type == expected)
            .and_then(|m| m.owner_uid)
            .filter(|raw| *raw != 0)
            .map(Uid::new)
    }

    /// 🔥 按 task_id 反查所属 `DownloadManager`
    ///
    /// 单任务操作（pause/resume/delete/get）通过 task_id 路由到正确账号的 manager，
    /// 不再依赖 `_for_active()` 假设任务归属当前活跃账号。
    ///
    /// 路径（**内存优先 → 历史 `owner_uid` 精确路由**）：
    /// 1. 活跃账号 manager 的**内存**命中即返回（per-uid 实例下内存命中即确定归属）；
    /// 2. 去重后线性扫描其它 manager 的**内存**；
    /// 3. 仅当任务不在任何内存中时，按**历史库 `owner_uid`** 精确路由到对应账号。
    ///    （历史库全局共享，不能用 `has_task_anywhere` 判定归属——否则历史任务会被
    ///    错误路由到 active manager，删除事件 `owner_uid` 也随之用错。）
    ///
    /// 返回 `Some((owner_uid, manager))` 表示任务归属 owner_uid 的 manager；
    /// `None` 表示任务不存在于任何已注册 manager 的内存/历史（handler 应返回 404
    /// 或回退 active）。
    pub async fn find_download_manager_for_task(
        &self,
        task_id: &str,
    ) -> Option<(Uid, Arc<DownloadManager>)> {
        // 1. 内存优先：活跃账号
        if let Some(uid) = *self.active_uid.read().await {
            if let Some(dm) = self.download_manager_for(uid) {
                if dm.has_task_in_memory(task_id).await {
                    return Some((uid, dm));
                }
            }
        }
        // 2. 内存扫描其它账号（去重）
        for (uid, dm) in self.list_download_managers() {
            if dm.has_task_in_memory(task_id).await {
                return Some((uid, dm));
            }
        }
        // 3. 历史回退：按历史 owner_uid 精确路由（校验 task_type，避免跨类型误删）
        if let Some(owner) = self.history_owner_uid(task_id, TaskType::Download).await {
            if let Some(dm) = self.download_manager_for(owner) {
                return Some((owner, dm));
            }
        }
        None
    }

    /// 🔥 按 task_id 反查所属 `UploadManager`
    ///
    /// 语义同 `find_download_manager_for_task`（内存优先 → 历史 `owner_uid` 精确路由）。
    pub async fn find_upload_manager_for_task(
        &self,
        task_id: &str,
    ) -> Option<(Uid, Arc<UploadManager>)> {
        // 1. 内存优先：活跃账号
        if let Some(uid) = *self.active_uid.read().await {
            if let Some(um) = self.upload_manager_for(uid) {
                if um.has_task_in_memory(task_id) {
                    return Some((uid, um));
                }
            }
        }
        // 2. 内存扫描其它账号（去重）
        for (uid, um) in self.list_upload_managers() {
            if um.has_task_in_memory(task_id) {
                return Some((uid, um));
            }
        }
        // 3. 历史回退：按历史 owner_uid 精确路由（校验 task_type，避免跨类型误删）
        if let Some(owner) = self.history_owner_uid(task_id, TaskType::Upload).await {
            if let Some(um) = self.upload_manager_for(owner) {
                return Some((owner, um));
            }
        }
        None
    }

    /// 🔥 按 task_id 反查所属 `TransferManager`
    ///
    /// 转存任务可能分散在多个账号的 `TransferManager` 内，handler 收到
    /// task_id 时无从直接判断归属 uid。路径与 download/upload 对齐：
    /// 内存优先（活跃账号 → 扫描其它账号）→ 历史库 `owner_uid` 精确路由。
    ///
    /// 之前用 `tm.get_task()`（仅内存），对仅存在于历史的转存任务会路由失败，
    /// handler 退回 active manager → 跨账号删除/取消归属错乱、删除事件 `owner_uid`
    /// 用错。现按历史 `owner_uid` 精确路由修正。
    ///
    /// 返回 `Some((uid, manager))` 即任务归属 uid 的 manager。
    pub async fn find_transfer_manager_for_task(
        &self,
        task_id: &str,
    ) -> Option<(Uid, Arc<TransferManager>)> {
        // 1. 内存优先：活跃账号
        if let Some(uid) = *self.active_uid.read().await {
            if let Some(tm) = self.transfer_manager_for(uid) {
                if tm.has_task_in_memory(task_id) {
                    return Some((uid, tm));
                }
            }
        }

        // 2. 内存扫描全部账号的 manager
        let entries: Vec<(Uid, Arc<TransferManager>)> = self
            .transfer_managers
            .iter()
            .map(|r| (*r.key(), Arc::clone(r.value())))
            .collect();
        for (uid, tm) in entries {
            if tm.has_task_in_memory(task_id) {
                return Some((uid, tm));
            }
        }

        // 3. 历史回退：按历史 owner_uid 精确路由（校验 task_type，避免跨类型误删）
        if let Some(owner) = self.history_owner_uid(task_id, TaskType::Transfer).await {
            if let Some(tm) = self.transfer_manager_for(owner) {
                return Some((owner, tm));
            }
        }
        None
    }

    /// 取指定账号 uid 的 `AutoBackupManager`
    ///
    /// **多账号语义**：`AutoBackupManager` 当前实现是 **进程级单例**
    /// （共享 `config_path` / `db_path` / `temp_dir`，数据按 `owner_uid` 区分），
    /// 因此当指定 `uid` 在池中尚未注册（例如登录时未来得及注册到该 uid）时，
    /// 回退到池中任意已注册的实例。这避免了「切换到刚登录账号 → autobackup
    /// 路由 500」的暂态错误。
    pub fn autobackup_manager_for(&self, uid: Uid) -> Option<Arc<AutoBackupManager>> {
        if let Some(e) = self.autobackup_managers.get(&uid) {
            return Some(Arc::clone(e.value()));
        }
        // 单例回退：池中任何一个 manager 都包含全量数据（按 owner_uid 过滤）
        self.autobackup_managers
            .iter()
            .next()
            .map(|e| Arc::clone(e.value()))
    }

    /// 取活跃账号的 `AutoBackupManager`（含单例回退，详见 `autobackup_manager_for`）
    pub async fn autobackup_manager_for_active(&self) -> Option<Arc<AutoBackupManager>> {
        if let Some(uid) = *self.active_uid.read().await {
            return self.autobackup_manager_for(uid);
        }
        // active_uid 为 None 时也尝试回退（虽极少触发）
        self.autobackup_managers
            .iter()
            .next()
            .map(|e| Arc::clone(e.value()))
    }

    /// 列出当前所有 `DownloadManager`（按 Arc 去重，每个独立实例只出现一次）。
    ///
    /// **跨账号聚合用途**：handler `GET /downloads/all` 等多账号
    /// 列表接口可基于本方法迭代各账号 manager 收集任务。
    ///
    /// **去重保障**：当前架构是 **per-uid 独立 manager**
    /// — 启动 / 登录链路为每个账号构造独立的 `DownloadManager` 实例。但历史
    /// fallback 路径（`build_and_register_managers_for_account` 之前的 legacy
    /// 共享 Arc 注册）可能在测试或部分回归路径上让多个 uid 指向同一个 Arc。
    /// 为了阻止 `get_all_tasks` 集合 N 倍化的退化，这里仍按 `Arc::ptr_eq` 等价
    /// 的指针去重（每个底层 Manager 实例只返回一次，保留池中第一个出现的 uid 作 key）。
    /// 在严格 per-uid 路径下，去重等价于直接遍历池（每个 Arc 只出现一次）。
    pub fn list_download_managers(&self) -> Vec<(Uid, Arc<DownloadManager>)> {
        let entries: Vec<(Uid, Arc<DownloadManager>)> = self
            .download_managers
            .iter()
            .map(|e| (*e.key(), Arc::clone(e.value())))
            .collect();
        dedup_arc_entries(entries)
    }

    /// 列出当前所有 `UploadManager`（按 `Arc::ptr_eq` 去重，详见 `list_download_managers`）
    pub fn list_upload_managers(&self) -> Vec<(Uid, Arc<UploadManager>)> {
        let entries: Vec<(Uid, Arc<UploadManager>)> = self
            .upload_managers
            .iter()
            .map(|e| (*e.key(), Arc::clone(e.value())))
            .collect();
        dedup_arc_entries(entries)
    }

    /// 列出当前所有 `TransferManager`（按 `Arc::ptr_eq` 去重，详见 `list_download_managers`）
    pub fn list_transfer_managers(&self) -> Vec<(Uid, Arc<TransferManager>)> {
        let entries: Vec<(Uid, Arc<TransferManager>)> = self
            .transfer_managers
            .iter()
            .map(|e| (*e.key(), Arc::clone(e.value())))
            .collect();
        dedup_arc_entries(entries)
    }

    /// 列出当前所有 `AutoBackupManager`
    pub fn list_autobackup_managers(&self) -> Vec<(Uid, Arc<AutoBackupManager>)> {
        self.autobackup_managers
            .iter()
            .map(|e| (*e.key(), Arc::clone(e.value())))
            .collect()
    }

    /// 注册账号的 4 个 Manager 到 per-uid 池（登录链路）
    ///
    /// 调用方应在账号成功登录 + 客户端注入 `client_pool` 之后调用。本方法只做插入，
    /// 不负责 Manager 的构造（由 `auth.rs` / `state.rs` 调用工厂方法构造后传入）。
    pub fn register_account_managers(
        &self,
        uid: Uid,
        download: Arc<DownloadManager>,
        upload: Arc<UploadManager>,
        transfer: Arc<TransferManager>,
    ) {
        self.download_managers.insert(uid, download);
        self.upload_managers.insert(uid, upload);
        self.transfer_managers.insert(uid, transfer);
        info!("AppState: 注册 per-uid manager uid={}", uid.raw());
    }

    /// 注册账号自动备份管理器到池（独立于 register_account_managers，因为
    /// AutoBackupManager 的生命周期与备份特性绑定且可能延迟初始化）
    pub fn register_account_autobackup(&self, uid: Uid, autobackup: Arc<AutoBackupManager>) {
        self.autobackup_managers.insert(uid, autobackup);
        info!("AppState: 注册 per-uid autobackup uid={}", uid.raw());
    }

    /// 为指定账号构造一份独立的 download/upload/transfer manager 三元组并注册到池。
    ///
    /// 对应 `load_initial_session` 中"非 active 持久化账号"
    /// 路径，用账号自己的 `UserAuth.custom_config` 计算 effective 配置，构造独立的
    /// `task_slot_pool` / `waiting_queue` / `chunk_scheduler`，避免跨账号共享。
    ///
    /// 行为：
    /// - 构造时按 `resolve_effective_account_config` 计算 effective 配置
    /// - 注入 BudgetScheduler + decrypt_semaphore（机器级单例）+ persistence_manager
    /// - **不**自动 wire snapshot_manager / encryption_config_store / folder_manager —
    ///   这些通过统一入口 `wire_manager_runtime_deps_for_uid` 在 autobackup 初始化
    ///   后注入（避免在 encryption_config_store 还没准备好时无法拿到 store）。
    /// - **启动** `start_auto_requeue_consumer`：
    ///   每个 manager 的 scheduler 在失败/被抢占/需要重排时会调 `requeue_tx.send`，
    ///   `requeue_rx` 必须有人消费，否则任务卡住。`take()` 语义保证幂等。
    /// - 客户端按 `client_pool.get_client(uid)` 取；若池中没有，按需调
    ///   `ensure_client_for_uid` 懒加载
    pub async fn build_and_register_managers_for_account(
        &self,
        user_auth: UserAuth,
        pm_arc: Arc<Mutex<PersistenceManager>>,
    ) -> anyhow::Result<()> {
        let uid = Uid::new(user_auth.uid);

        // 1) 取代理配置（与 active 账号同款）
        let proxy_config = {
            let cfg = self.config.read().await;
            if cfg.network.proxy.proxy_type != ProxyType::None
                && !self.fallback_mgr.is_fallen_back()
            {
                Some(cfg.network.proxy.clone())
            } else {
                None
            }
        };
        let fallback = if proxy_config.is_some() {
            Some(Arc::clone(&self.fallback_mgr))
        } else {
            None
        };

        // 2) 确保该账号 client 已在池中（懒加载）
        self.ensure_client_for_uid(uid).await?;
        let client_arc = {
            let pool = self.client_pool.read().await;
            pool.get_client(uid)
                .ok_or_else(|| anyhow::anyhow!("client_pool uid={} 缺失", uid.raw()))?
        };

        // 3) 取该账号 effective 配置 + 全局 download_dir + transfer_config
        let (download_dir, eff_dl_threads, eff_dl_concurrent, eff_dl_retries,
            upload_config, transfer_config) = {
            let cfg = self.config.read().await;
            let (dl_t, dl_c, dl_r, up_t, up_c, up_r) =
                resolve_effective_account_config(&user_auth, &cfg);
            let mut up_cfg = cfg.upload.clone();
            up_cfg.max_global_threads = up_t;
            up_cfg.max_concurrent_tasks = up_c;
            up_cfg.max_retries = up_r;
            (
                cfg.download.download_dir.clone(),
                dl_t,
                dl_c,
                dl_r,
                up_cfg,
                cfg.transfer.clone(),
            )
        };

        // 4) 构造 DownloadManager
        let mut download_manager = DownloadManager::new_for_account(
            user_auth.clone(),
            download_dir,
            eff_dl_threads,
            eff_dl_concurrent,
            eff_dl_retries,
            proxy_config.as_ref(),
            fallback.clone(),
            Arc::clone(&self.budget_scheduler),
            Arc::clone(&self.decrypt_semaphore),
        )?;
        download_manager.set_persistence_manager(Arc::clone(&pm_arc));
        download_manager
            .set_ws_manager(Arc::clone(&self.ws_manager))
            .await;
        let download_manager_arc = Arc::new(download_manager);

        // 每个 per-uid manager 都启动 auto_requeue 消费循环
        // 否则 scheduler 调 `requeue_tx.send` 时 `requeue_rx` 无人消费，任务卡住。
        Arc::clone(&download_manager_arc)
            .start_auto_requeue_consumer()
            .await;

        // 5) 构造 UploadManager
        let config_dir = std::path::Path::new("config");
        let upload_manager = UploadManager::new_for_account(
            (*client_arc).clone(),
            &user_auth,
            &upload_config,
            Arc::clone(&self.budget_scheduler),
            config_dir,
        );
        let upload_manager_arc = Arc::new(upload_manager);
        upload_manager_arc
            .set_persistence_manager(Arc::clone(&pm_arc))
            .await;
        upload_manager_arc
            .set_ws_manager(Arc::clone(&self.ws_manager))
            .await;
        upload_manager_arc
            .set_backup_record_manager(Arc::clone(&self.backup_record_manager))
            .await;

        // 4.5) 把 active manager 的 task_completed_sender /
        // folder_progress_sender 共享给本 per-uid manager。否则子任务完成时
        // 不会触发 listener，文件夹补任务卡死。
        self.folder_download_manager
            .share_senders_with(&download_manager_arc)
            .await;

        // 6) 构造 TransferManager
        let mut transfer_manager = TransferManager::new(
            Arc::new(std::sync::RwLock::new((*client_arc).clone())),
            transfer_config,
            Arc::clone(&self.config),
        );
        transfer_manager.set_owner_uid(uid);
        let transfer_manager_arc = Arc::new(transfer_manager);
        // download_manager 引用：用于自动下载，传该账号自己的 download_manager
        transfer_manager_arc
            .set_download_manager(Arc::clone(&download_manager_arc))
            .await;
        // folder_download_manager 是机器单例（共享）
        transfer_manager_arc
            .set_folder_download_manager(Arc::clone(&self.folder_download_manager))
            .await;
        transfer_manager_arc
            .set_persistence_manager(Arc::clone(&pm_arc))
            .await;
        transfer_manager_arc
            .set_ws_manager(Arc::clone(&self.ws_manager))
            .await;

        // 7) 注册到池
        self.register_account_managers(
            uid,
            download_manager_arc,
            upload_manager_arc,
            transfer_manager_arc,
        );
        info!(
            "build_and_register_managers_for_account: uid={} 已构造独立 manager 三元组 \
             (dl_threads={}, dl_concurrent={}, dl_retries={}, up_threads={}, up_concurrent={}, up_retries={})",
            uid.raw(),
            eff_dl_threads,
            eff_dl_concurrent,
            eff_dl_retries,
            upload_config.max_global_threads,
            upload_config.max_concurrent_tasks,
            upload_config.max_retries,
        );
        Ok(())
    }

    /// 🔥 统一注入 per-uid manager 的运行时依赖
    ///
    /// 把所有"manager 构造之后才能拿到、跨账号需要复用"的依赖统一注入：
    /// - **DownloadManager**：`set_folder_manager` / `set_snapshot_manager` /
    ///   `set_encryption_config_store`
    /// - **UploadManager**：`set_snapshot_manager` / `set_backup_record_manager`
    /// - **FolderDownloadManager**：刷新 per-uid `download_manager_pool`
    /// - **ScanManager**：刷新 per-uid `upload_manager_pool`
    /// - **DownloadManager folder/task channel sender**：`share_senders_with`
    ///
    /// **该入口必须被以下 3 条路径调用**：
    ///   1. `load_initial_session` 启动期对每个持久化账号
    ///   2. `auth.rs::cookie_login` 新登录账号 manager 注册之后
    ///   3. `accounts.rs::switch_account` 切换 active 之前 — 兜底补齐启动期
    ///      可能的半注入状态
    ///
    /// 幂等：所有 `set_*` 调用都是覆盖式写入，重复调用无副作用。
    pub async fn wire_manager_runtime_deps_for_uid(&self, uid: Uid) -> anyhow::Result<()> {
        // 1) 取该 uid 的三类 manager（要求已构造好；调用方应保证）
        let dm = self
            .download_manager_for(uid)
            .ok_or_else(|| anyhow::anyhow!("wire_manager_runtime_deps_for_uid: uid={} 缺 DownloadManager", uid.raw()))?;
        let um = self
            .upload_manager_for(uid)
            .ok_or_else(|| anyhow::anyhow!("wire_manager_runtime_deps_for_uid: uid={} 缺 UploadManager", uid.raw()))?;

        // 2) 给 DownloadManager 注入 folder_manager / snapshot / encryption_config_store
        dm.set_folder_manager(Arc::clone(&self.folder_download_manager))
            .await;
        dm.set_snapshot_manager(Arc::clone(&self.snapshot_manager))
            .await;
        if let Some(ecs) = self.encryption_config_store.read().await.as_ref().cloned() {
            dm.set_encryption_config_store(ecs).await;
        } else {
            tracing::debug!(
                "wire_manager_runtime_deps_for_uid: uid={} 跳过 encryption_config_store \
                 注入（autobackup 尚未初始化；切到 active 后会由 init_autobackup_manager 链路补齐）",
                uid.raw()
            );
        }

        // 3) 给 UploadManager 注入 snapshot / backup_record_manager
        um.set_snapshot_manager(Arc::clone(&self.snapshot_manager))
            .await;
        um.set_backup_record_manager(Arc::clone(&self.backup_record_manager))
            .await;

        // 4) FolderDownloadManager 共享 sender + 刷新 per-uid 池
        self.folder_download_manager.share_senders_with(&dm).await;
        self.folder_download_manager
            .set_download_manager_pool(Arc::clone(&self.download_managers))
            .await;

        // 5) ScanManager 刷新 per-uid upload 池
        if let Some(scan_mgr) = self.scan_manager.read().await.as_ref().cloned() {
            scan_mgr
                .set_upload_manager_pool(Arc::clone(&self.upload_managers))
                .await;
        }

        // 6) 刷新 AutoBackup 备份子任务通知 sender 到所有 per-uid manager
        //
        // 触发场景：startup 期间某账号 manager 构造失败 → 后续 switch_account 兜底
        // 调 `build_and_register_managers_for_account` 重建 → 旧实现 wire 这里只
        // 注入了 folder/snapshot/encryption/scan 等；新构造的 download/upload manager
        // 没有 backup_notification_sender，该账号自动备份子任务完成事件会丢失，父
        // 备份任务卡在 Transferring。这里把刷新放进统一 wire 入口，让启动 / 登录 /
        // 切换三条路径都覆盖到。
        //
        // 单例幂等：autobackup_manager_for(uid) 找不到 uid 注册位时回退池中任意实例
        // （AutoBackupManager 是进程级单例）；refresh_transfer_listener_bindings
        // 内部对 backup_notification_tx 未初始化的场景会 warn 跳过（autobackup 尚未
        // start_transfer_listeners — 此时只是早，下一轮 init_autobackup_manager 会补齐）。
        if let Some(autobackup) = self.autobackup_manager_for(uid) {
            autobackup.refresh_transfer_listener_bindings().await;
        }

        info!(
            "wire_manager_runtime_deps_for_uid: uid={} 运行时依赖已注入",
            uid.raw()
        );
        Ok(())
    }

    /// 🔥 force-delete 账号编排器
    ///
    /// 完整的删除账号流程，调用方（HTTP handler）只需调一次。本方法保证：
    ///
    /// 1. **AutoBackupManager.delete_configs_for_owner(uid)** —— 删除该账号下所有备份配置/任务（保留单例供其它账号继续使用）
    /// 2. **TransferManager.delete_tasks_for_owner(uid)** —— 取消并清理该账号所有运行/历史转存任务
    ///    （未取消任务会继续跑直到 token 被取消）
    /// 3. **UploadManager.delete_tasks_for_owner(uid)** —— 同上，含备份任务（owner_uid 过滤）
    /// 4. **DownloadManager.delete_tasks_for_owner(uid)** —— 同上
    /// 4.1. **FolderDownloadManager.delete_folders_for_owner(uid)** —— 取消文件夹扫描+清子任务+槽位
    /// 4.5. **CloudDlMonitor.stop()** + 从池移除
    /// 5. **unregister_account_managers(uid)** —— 4 个 DashMap 池移除条目
    /// 6. **client_pool.remove(uid)** —— 网盘客户端池移除
    /// 7. **account_manager.delete_user(uid)** —— 账号元数据移除 + 持久化
    /// 7.5. **budget_scheduler.remove_account(uid)** + 推 BudgetRecomputed
    /// 8. **set_active_uid helper** —— 切换 active + 重绑 CloudDl WS 订阅 + 广播 Switched
    ///
    /// 失败语义：
    /// - 7 失败（account_manager.delete_user）则原子化失败，已 unregister 的内存
    ///   池**不会**回滚（账号确实不应继续运行）；调用方可记录告警，但不应重试。
    pub async fn force_delete_account(&self, uid: Uid) -> anyhow::Result<()> {
        info!("force_delete_account: 开始删除账号 uid={}", uid.raw());

        // 1) AutoBackupManager 删除该 owner_uid 的备份配置 + 子任务
        //
        // 不直接 `autobackup.shutdown()` 关闭整个共享 manager — 那样会停掉聚合器、
        // 所有 watcher、所有 poll scheduler、暂停**所有账号**的备份任务，影响其它账号。
        // `AutoBackupManager` 是进程级单例（`autobackup_manager_for(uid)` 在池中没条目时
        // 回退任意一个实例 — `state.rs::autobackup_manager_for` 文档明确说明）。共享
        // manager 设计下 force-delete 必须只清掉被删账号归属的 config + 任务，manager
        // 本身继续运行服务其它账号 — `delete_configs_for_owner` 就是为此设计的。
        if let Some(autobackup) = self.autobackup_manager_for(uid) {
            let deleted = autobackup.delete_configs_for_owner(uid.raw()).await;
            info!(
                "force_delete_account uid={} autobackup.delete_configs_for_owner: 已删除 {} 个备份配置（不再 shutdown 全局 manager）",
                uid.raw(),
                deleted
            );
        }

        // 2-4) Transfer / Upload / Download 按 owner_uid 取消并清理任务
        //
        // 仅 unregister_account_managers + GC 不够，任务依旧持有 manager Arc + owner client，
        // 会继续跑下去直到 token 被取消 / chunk 完成。必须显式调 `delete_tasks_for_owner` 让：
        //   - cancellation_token cancel
        //   - 槽位释放（task_slot_pool / borrowed slots / chunk active count）
        //   - 持久化 .meta 清理
        //   - history.jsonl / sqlite history_db 清理
        //   - 持久化恢复路径不再产生 `account_deleted` 垃圾态条目
        //
        // 注：FolderDownloadManager.delete_folders_for_owner 也并行清，与
        // download_manager.delete_tasks_for_owner（清单文件子任务）配合幂等。
        if let Some(dm) = self.download_managers.get(&uid).map(|e| e.value().clone()) {
            let (mem, hist) = dm.delete_tasks_for_owner(uid, false).await;
            info!(
                "force_delete_account uid={} download.delete_tasks_for_owner: 内存={}, 历史={}",
                uid.raw(), mem, hist
            );
        }
        // 文件夹下载（共享 FolderDownloadManager，按 owner 清扫描+槽位+持久化）
        let folder_processed = self
            .folder_download_manager
            .delete_folders_for_owner(uid, false)
            .await;
        info!(
            "force_delete_account uid={} folder_download.delete_folders_for_owner: 处理={}",
            uid.raw(), folder_processed
        );
        if let Some(um) = self.upload_managers.get(&uid).map(|e| e.value().clone()) {
            let (mem, hist) = um.delete_tasks_for_owner(uid).await;
            info!(
                "force_delete_account uid={} upload.delete_tasks_for_owner: 内存={}, 历史={}",
                uid.raw(), mem, hist
            );
        }
        if let Some(tm) = self.transfer_managers.get(&uid).map(|e| e.value().clone()) {
            let (mem, hist) = tm.delete_tasks_for_owner(uid).await;
            info!(
                "force_delete_account uid={} transfer.delete_tasks_for_owner: 内存={}, 历史={}",
                uid.raw(), mem, hist
            );
        }

        // 4.5) CloudDlMonitor：停止该账号的轮询并从池中移除
        if let Some((_, monitor)) = self.cloud_dl_monitors.remove(&uid) {
            monitor.stop();
            info!(
                "force_delete_account uid={} CloudDlMonitor 已停止并移除",
                uid.raw()
            );
        }

        // 清理 cloud_dl_init_locks 中该 uid 的 entry，
        // 避免删除账号后陈旧锁条目残留。
        {
            let mut locks = self.cloud_dl_init_locks.lock().await;
            if locks.remove(&uid).is_some() {
                info!(
                    "force_delete_account uid={} cloud_dl_init_locks entry 已清理",
                    uid.raw()
                );
            }
        }

        // 清理 cloud_dl_ws_subscribers 中绑定到该 uid 的订阅，
        // 避免删除账号后旧订阅映射继续引用已停止的 monitor。
        let removed_subs: Vec<String> = self
            .cloud_dl_ws_subscribers
            .iter()
            .filter(|r| *r.value() == uid)
            .map(|r| r.key().clone())
            .collect();
        for conn_id in &removed_subs {
            self.cloud_dl_ws_subscribers.remove(conn_id);
        }
        if !removed_subs.is_empty() {
            info!(
                "force_delete_account uid={} cloud_dl_ws_subscribers 已清理 {} 条映射",
                uid.raw(),
                removed_subs.len()
            );
        }

        // 5) 4 个池移除（autobackup_managers 不在此处理 — 见 step 5.5）
        self.unregister_account_managers(uid);

        // 5.5) 处理 AutoBackup 单例的注册位
        //
        // `autobackup_managers` 是进程级单例的"路由位"映射 — 同一个 `Arc<AutoBackupManager>`
        // 可能只注册在启动时 active uid 下一个条目。如果被删的就是那个 uid，且不做处理，
        // 池会被 step 7 之后变空（虽然 unregister 已经不删 autobackup_managers，但这里
        // 仍可能存在被删 uid 唯一持有该单例的场景），剩余账号调 `autobackup_manager_for`
        // 会找不到任何实例。
        //
        // 修复：捕获被删 uid 下的 autobackup Arc（如有），在剩余账号中挑一个（优先
        // `account_manager.active_uid()` 更新后的值；否则取第一个剩余 uid）做 rebind
        // 注册；如已没有任何剩余账号，则 drop 单例（最后一个账号删除流程）。
        let removed_autobackup_arc =
            self.autobackup_managers.remove(&uid).map(|(_, arc)| arc);
        if let Some(autobackup_arc) = removed_autobackup_arc {
            // 找一个剩余账号承接单例
            let remaining_uid: Option<Uid> = {
                let am = self.account_manager.lock().await;
                // 注：此时 step 7（delete_user）尚未执行，但 unregister 已发生；
                // account_manager 的 list 仍然包含被删 uid，需要排除。
                am.list_users()
                    .iter()
                    .map(|u| Uid::new(u.uid))
                    .find(|u| *u != uid)
            };
            match remaining_uid {
                Some(reg_uid) => {
                    self.rebind_autobackup_singleton_if_needed(reg_uid, autobackup_arc);
                }
                None => {
                    info!(
                        "force_delete_account uid={}：删除最后一个账号，AutoBackup 单例不再 rebind",
                        uid.raw()
                    );
                    // autobackup_arc 在此 drop（除非外部还持有引用 — 池中已无其它注册位）
                }
            }
        }

        // 6) client_pool 移除
        {
            let mut pool = self.client_pool.write().await;
            pool.remove_client(uid);
        }

        // 7) account_manager 删除
        self.account_manager.lock().await.delete_user(uid).await?;

        // 7.5) 从 BudgetScheduler 移除
        //
        // 不移除会导致：
        //   - 账号已删除，但 `BudgetScheduler.download/upload.accounts` 仍持有
        //     该 uid 的 `AccountSlot`；
        //   - `recompute_budget` 仍把该 uid 计入 base/vip_cap 压缩算法分母，
        //     稀释剩余账号的 base_i；
        //   - `snapshot()` / `BudgetEvent::BudgetRecomputed` 仍包含该 uid 条目。
        // `remove_account` 内部已调 `recompute_budget`，这里只需补 WS 推送让
        // 前端 BudgetPanel 立即收敛。
        self.budget_scheduler.remove_account(uid).await;
        self.broadcast_budget_recomputed().await;
        info!(
            "force_delete_account uid={} 已从 BudgetScheduler 移除并推送 BudgetRecomputed",
            uid.raw()
        );

        // 8) 通过 helper 同步 active_uid
        //
        // **不变式**（见 `server/helpers.rs` 文件头）：`*active_uid.write().await = ...`
        // 全局只允许出现在 `helpers.rs::set_active_uid` 内。此前本方法直接写
        // `self.active_uid`，绕过了：
        //   - `rebind_cloud_dl_ws_subscribers(new_active)` 重绑（CloudDl WS 订阅
        //     从被删账号迁移到 new_active）
        //   - 广播 `AccountEvent::Switched`（前端无法感知 active 切换）
        // `account_manager.delete_user` 内部已 fallback 持久化 `active_uid`，
        // 这里只读最新值，再走唯一入口 `set_active_uid`：
        //   1. 写 `state.active_uid`
        //   2. 再次 `set_active_persisted`（幂等：值与 delete_user 落盘后一致）
        //   3. CloudDl WS 订阅 rebind
        //   4. 广播 `AccountEvent::Switched`
        let new_active = self.account_manager.lock().await.active_uid();
        crate::server::helpers::set_active_uid(self, new_active).await?;

        info!(
            "force_delete_account uid={} 完成，新 active_uid={:?}",
            uid.raw(),
            new_active.map(|u| u.raw())
        );

        Ok(())
    }

    /// 注销账号 per-uid manager（用于强制删除账号）
    ///
    /// 从 download/upload/transfer 三个 per-uid 池中移除指定 uid。调用方应在
    /// `account_manager.delete_user(uid)` **之前**调用本方法，并先用 `xxx_manager_for(uid)`
    /// 拿到 Arc 调用各自的 `shutdown()` / `delete_tasks_for_owner` 后再 unregister。
    ///
    /// 本方法**不**移除 `autobackup_managers` — 该字段持有的
    /// 是进程级单例 `AutoBackupManager`（`autobackup_manager_for(uid)` 找不到时回退池中
    /// 任意一个实例），如果在 unregister 时一并移除，会出现"被删 uid 正好是单例注册所在
    /// uid 时池被清空 → 剩余账号 `/autobackup` 接口全部 manager 未初始化"的退化。
    /// `force_delete_account` 流程会单独负责把单例 re-register 到剩余账号下。
    pub fn unregister_account_managers(&self, uid: Uid) {
        self.download_managers.remove(&uid);
        self.upload_managers.remove(&uid);
        self.transfer_managers.remove(&uid);
        // 不再移除 autobackup_managers（进程级单例由 force_delete 单独处理）
        info!(
            "AppState: 注销 per-uid manager uid={}（autobackup 单例保留）",
            uid.raw()
        );
    }

    /// 把 `AutoBackupManager` 单例 rebind 到指定 uid
    ///
    /// 仅在该 uid 在 `autobackup_managers` 池中尚未持有该单例时执行 insert。
    /// 用于 `force_delete_account` 删除原注册 uid 后，把同一个 `Arc<AutoBackupManager>`
    /// 重新注册到剩余账号 uid 下，避免"删 active 账号 → 池中只剩 active 那个条目被清掉
    /// → manager_for(剩余账号) 回退也找不到"的退化（虽然池里其它 uid 的条目还在，
    /// 但仅当 active uid 是单例注册位时该条目的 owner 被删，导致后续清理意外拽走）。
    ///
    /// 通常是幂等 no-op；如池为空但参数传入了一个有效 uid + 显式 Arc，则注册。
    fn rebind_autobackup_singleton_if_needed(
        &self,
        target_uid: Uid,
        autobackup: Arc<AutoBackupManager>,
    ) {
        // 池中已存在 target_uid 条目 → 已有单例，跳过；否则注册
        if !self.autobackup_managers.contains_key(&target_uid) {
            self.autobackup_managers
                .insert(target_uid, Arc::clone(&autobackup));
            info!(
                "AppState: 将 AutoBackup 单例 rebind 到 uid={}（force_delete 后保留路由）",
                target_uid.raw()
            );
        }
    }

    /// 广播一次 `BudgetEvent::BudgetRecomputed`
    ///
    /// 用法：账号增删 / 配置更新 / `BudgetScheduler::update_*` 之后调用。
    /// 内部从 `budget_scheduler.snapshot()` 拉一次全量快照并通过 `ws_manager`
    /// 推送给所有连接（含未订阅 `budget` 的连接，因为 `Budget` 不走订阅过滤）。
    pub async fn broadcast_budget_recomputed(&self) {
        use crate::server::events::{BudgetEvent, WsAccountBudget};
        use crate::server::websocket::WsServerMessage;
        let snap = self.budget_scheduler.snapshot().await;
        let per_account = snap
            .per_account
            .into_iter()
            .map(|e| WsAccountBudget {
                uid: e.uid.raw(),
                vip_cap_download: e.vip_cap_download,
                base_download: e.base_download,
                used_download: e.used_download,
                vip_cap_upload: e.vip_cap_upload,
                base_upload: e.base_upload,
                used_upload: e.used_upload,
            })
            .collect();
        let event = BudgetEvent::BudgetRecomputed {
            machine_budget_download: snap.machine_budget_download,
            machine_budget_upload: snap.machine_budget_upload,
            per_account,
        };
        self.ws_manager.broadcast(WsServerMessage::budget(event));
    }

    /// 多账号 ClientPool 预热
    ///
    /// **职责**：为所有"非活跃"账号构造 `NetdiskClient` 并注入 `ClientPool`。
    /// 活跃账号的客户端已在 `load_initial_session` 中注入。
    ///
    /// **并发策略**：
    /// - Semaphore 限并发 = 3
    /// - 非活跃账号 2 秒错峰（避开同时大量 baidu API 请求）
    /// - 失败不致命：单账号失败仅记录 warn 日志，不阻断启动
    ///
    /// **注意**：本方法只构造客户端、注入池，**不**做 cookie warmup HTTP 调用
    /// （由 handler 首次访问时按需触发 / 或未来引入显式 preheat task）。
    pub async fn preheat_inactive_clients(&self) -> anyhow::Result<()> {
        let active_uid_opt = *self.active_uid.read().await;

        // 收集非活跃账号
        let inactive_users: Vec<UserAuth> = {
            let mgr = self.account_manager.lock().await;
            mgr.list_users()
                .iter()
                .filter(|u| {
                    active_uid_opt
                        .map(|active| active.raw() != u.uid)
                        .unwrap_or(true)
                })
                .cloned()
                .collect()
        };

        if inactive_users.is_empty() {
            info!("ClientPool 预热：无非活跃账号，跳过");
            return Ok(());
        }

        info!(
            "ClientPool 预热：开始注入 {} 个非活跃账号（Semaphore=3, 2s 错峰）",
            inactive_users.len()
        );

        // 读取代理配置
        let proxy_config = {
            let config_guard = self.config.read().await;
            if config_guard.network.proxy.proxy_type != ProxyType::None
                && !self.fallback_mgr.is_fallen_back()
            {
                Some(config_guard.network.proxy.clone())
            } else {
                None
            }
        };
        let fallback = if proxy_config.is_some() {
            Some(Arc::clone(&self.fallback_mgr))
        } else {
            None
        };

        // Semaphore 限并发 = 3
        let sem = Arc::new(tokio::sync::Semaphore::new(3));
        let client_pool = Arc::clone(&self.client_pool);
        let mut handles = Vec::with_capacity(inactive_users.len());

        for (idx, user) in inactive_users.into_iter().enumerate() {
            let sem = Arc::clone(&sem);
            let client_pool = Arc::clone(&client_pool);
            let proxy_config = proxy_config.clone();
            let fallback = fallback.clone();

            let handle = tokio::spawn(async move {
                // 2 秒错峰
                tokio::time::sleep(std::time::Duration::from_millis(idx as u64 * 2000)).await;
                let _permit = sem.acquire().await.ok();
                let uid_raw = user.uid;
                let uid = Uid::new(uid_raw);
                let username = user.username.clone();
                match NetdiskClient::new_with_proxy(user, proxy_config.as_ref(), fallback) {
                    Ok(client) => {
                        let mut pool = client_pool.write().await;
                        pool.add_client(uid, Arc::new(client));
                        info!(
                            "ClientPool 预热成功：uid={} ({})（池大小 {}）",
                            uid_raw,
                            username,
                            pool.len()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "ClientPool 预热失败：uid={} ({}) — {}（不致命，跳过）",
                            uid_raw, username, e
                        );
                    }
                }
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            let _ = handle.await;
        }

        let final_size = self.client_pool.read().await.len();
        info!("ClientPool 预热完成：最终池大小 {}", final_size);
        Ok(())
    }

    /// 🔥 多账号：按需懒加载指定 UID 的 NetdiskClient
    ///
    /// `preheat_inactive_clients` 启动期可能因网络/代理短暂故障导致非活跃账号 client
    /// 注入失败（仅 warn 不阻塞启动）。`switch_account` 之前必须保证目标账号 client
    /// 存在，否则 `active_client()` 返回 `None` → handler 401「未登录」误报。
    ///
    /// 语义：
    /// - 客户端已存在 → 立即返回 `Ok(())`，不重复构造
    /// - 客户端缺失 → 用 AccountManager 中持久化的 `UserAuth` 构造一个新的注入池
    /// - UID 不在 AccountManager → 返回 `Err`（账号不存在）
    /// - 构造失败（代理/cookie 解析等错误）→ 透传 `Err`
    pub async fn ensure_client_for_uid(&self, uid: Uid) -> anyhow::Result<()> {
        // 1) 已存在 → 直接返回
        {
            let pool = self.client_pool.read().await;
            if pool.get_client(uid).is_some() {
                return Ok(());
            }
        }

        // 2) 从 AccountManager 取 UserAuth
        let user = {
            let mgr = self.account_manager.lock().await;
            mgr.list_users()
                .iter()
                .find(|u| u.uid == uid.raw())
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("账号 uid={} 不存在于 AccountManager", uid.raw()))?
        };

        // 3) 构造代理配置（与 preheat_inactive_clients 同款）
        let proxy_config = {
            let config_guard = self.config.read().await;
            if config_guard.network.proxy.proxy_type != ProxyType::None
                && !self.fallback_mgr.is_fallen_back()
            {
                Some(config_guard.network.proxy.clone())
            } else {
                None
            }
        };
        let fallback = if proxy_config.is_some() {
            Some(Arc::clone(&self.fallback_mgr))
        } else {
            None
        };

        // 4) 构造 client + 注入池
        let username = user.username.clone();
        let client = NetdiskClient::new_with_proxy(user, proxy_config.as_ref(), fallback)
            .with_context(|| format!("懒加载 NetdiskClient 失败：uid={} ({})", uid.raw(), username))?;
        {
            let mut pool = self.client_pool.write().await;
            pool.add_client(uid, Arc::new(client));
            info!(
                "ClientPool 懒加载成功：uid={} ({})（池大小 {}）",
                uid.raw(),
                username,
                pool.len()
            );
        }
        Ok(())
    }

    /// 🔥 优雅关闭
    ///
    /// 关闭持久化管理器，确保所有 WAL 数据刷写到磁盘
    pub async fn shutdown(&self) {
        info!("正在关闭应用状态...");

        // 停止所有账号的离线下载监听服务
        for (uid, monitor) in self.iter_cloud_dl_monitors() {
            monitor.stop();
            info!("CloudDlMonitor uid={} 已停止", uid.raw());
        }
        self.cloud_dl_monitors.clear();

        // shutdown 同时清理 cloud_dl_init_locks 和 ws_subscribers
        {
            let mut locks = self.cloud_dl_init_locks.lock().await;
            let count = locks.len();
            locks.clear();
            if count > 0 {
                info!("shutdown: cloud_dl_init_locks 已清理 {} 条 entry", count);
            }
        }
        let ws_subs_count = self.cloud_dl_ws_subscribers.len();
        self.cloud_dl_ws_subscribers.clear();
        if ws_subs_count > 0 {
            info!(
                "shutdown: cloud_dl_ws_subscribers 已清理 {} 条订阅映射",
                ws_subs_count
            );
        }

        // 停止内存监控器
        self.memory_monitor.stop();
        info!("内存监控器已停止");

        // 停止分享同步管理器（取消所有 scheduler）
        if let Some(mgr) = self.share_sync_manager.read().await.as_ref() {
            mgr.shutdown().await;
            info!("分享同步管理器已停止");
        }

        // 关闭持久化管理器
        let mut pm = self.persistence_manager.lock().await;
        pm.shutdown().await;

        info!("应用状态已安全关闭");
    }
}

// 注意：Default trait 不能用于 async，移除或使用 lazy_static

#[async_trait::async_trait]
impl ProxyHotUpdater for AppState {
    async fn update_qrcode_auth(&self, proxy: Option<&ProxyConfig>) -> anyhow::Result<()> {
        let new_auth = QRCodeAuth::new_with_proxy(proxy)?;
        *self.qrcode_auth.write().await = new_auth;
        Ok(())
    }

    async fn update_netdisk_client(&self, proxy: Option<&ProxyConfig>) -> anyhow::Result<()> {
        // 按 active_uid → AccountManager 取，不用 current_user
        let user_auth = self.active_user_auth().await;
        if let Some(user) = user_auth {
            let new_client = NetdiskClient::new_with_proxy(
                user,
                proxy,
                Some(Arc::clone(&self.fallback_mgr)),
            )?;
            *self.netdisk_client.write().await = Some(new_client);
        }
        Ok(())
    }

    async fn update_download_engine(&self, proxy: Option<&ProxyConfig>) {
        if let Some(dm) = self.download_manager_for_active().await {
            dm.update_proxy_config(proxy);
        }
    }

    async fn update_upload_engine(&self, proxy: Option<&ProxyConfig>) {
        if let Some(um) = self.upload_manager_for_active().await {
            // 按 active_uid → AccountManager 取，不用 current_user
            let user_auth = self.active_user_auth().await;
            if let Some(user) = user_auth {
                match NetdiskClient::new_with_proxy(
                    user,
                    proxy,
                    Some(Arc::clone(&self.fallback_mgr)),
                ) {
                    Ok(new_client) => {
                        um.update_netdisk_client(new_client);
                    }
                    Err(e) => {
                        tracing::warn!("UploadManager NetdiskClient 热更新失败: {}", e);
                    }
                }
            }
        }
    }

    async fn update_transfer_engine(&self, proxy: Option<&ProxyConfig>) {
        if let Some(tm) = self.transfer_manager_for_active().await {
            // 按 active_uid → AccountManager 取，不用 current_user
            let user_auth = self.active_user_auth().await;
            if let Some(user) = user_auth {
                match NetdiskClient::new_with_proxy(
                    user,
                    proxy,
                    Some(Arc::clone(&self.fallback_mgr)),
                ) {
                    Ok(new_client) => {
                        tm.update_netdisk_client(new_client);
                    }
                    Err(e) => {
                        tracing::warn!("TransferManager NetdiskClient 热更新失败: {}", e);
                    }
                }
            }
        }
    }

    async fn update_cloud_dl_monitor(&self, proxy: Option<&ProxyConfig>) {
        // 多账号代理热更 — 遍历池内所有 monitor，按 uid 重建客户端
        let monitors = self.iter_cloud_dl_monitors();
        if monitors.is_empty() {
            return;
        }

        for (uid, monitor) in monitors {
            // 严格按 uid 取该账号 UserAuth；
            // 不 fallback 到 current_user（活跃账号），否则 uid=B 的 monitor 会被注入
            // uid=A 的 cookie，导致后续轮询/事件串账号。
            let user_auth: Option<crate::auth::UserAuth> = {
                let am = self.account_manager.lock().await;
                am.get_user(uid).cloned()
            };

            let Some(user) = user_auth else {
                tracing::warn!(
                    "update_cloud_dl_monitor uid={} 跳过：AccountManager 中无该账号（已删除？）",
                    uid.raw()
                );
                continue;
            };

            match NetdiskClient::new_with_proxy(
                user,
                proxy,
                Some(Arc::clone(&self.fallback_mgr)),
            ) {
                Ok(new_client) => {
                    monitor.update_client(new_client);
                    tracing::info!(
                        "CloudDlMonitor uid={} NetdiskClient 已热更新",
                        uid.raw()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "CloudDlMonitor uid={} NetdiskClient 热更新失败: {}",
                        uid.raw(),
                        e
                    );
                }
            }
        }
    }

    async fn update_folder_download_manager(&self, proxy: Option<&ProxyConfig>) {
        // 按 active_uid → AccountManager 取，不用 current_user
        let user_auth = self.active_user_auth().await;
        if let Some(user) = user_auth {
            match NetdiskClient::new_with_proxy(
                user,
                proxy,
                Some(Arc::clone(&self.fallback_mgr)),
            ) {
                Ok(new_client) => {
                    self.folder_download_manager
                        .set_netdisk_client(Arc::new(new_client))
                        .await;
                    info!("✓ FolderDownloadManager NetdiskClient 已热更新");
                }
                Err(e) => {
                    tracing::warn!("FolderDownloadManager NetdiskClient 热更新失败: {}", e);
                }
            }
        }
    }

    async fn update_autobackup_manager(&self, proxy: Option<&ProxyConfig>) {
        if let Some(mgr) = self.autobackup_manager_for_active().await {
            mgr.update_proxy_config(proxy);
        }
    }
}

// ============================================================================
// BudgetScheduler 启动 helpers
// ============================================================================

/// 🔥 按 `Arc::ptr_eq` 等价的内部指针对 `(Uid, Arc<T>)` 列表去重
///
/// 共享 manager 设计下，DashMap 多个 entry 可能指向同一个底层 `Arc<T>`。
/// 直接 iterate 会让聚合查询的 `get_all_tasks` 等被调多次返回相同集合，
/// 造成结果集任务被复制 N 倍。本函数按 raw pointer 等价去重，**保留每个
/// 独立 manager 实例第一次出现的 (uid, manager)**，剔除后续重复指向同一
/// 实例的 entry。
///
/// 复杂度：O(n²)，但 N（账号数）通常 ≤ 5，可接受。
fn dedup_arc_entries<T>(entries: Vec<(Uid, Arc<T>)>) -> Vec<(Uid, Arc<T>)> {
    let mut seen_ptrs: Vec<*const T> = Vec::with_capacity(entries.len());
    let mut out = Vec::with_capacity(entries.len());
    for (uid, arc) in entries {
        let ptr = Arc::as_ptr(&arc);
        if !seen_ptrs.contains(&ptr) {
            seen_ptrs.push(ptr);
            out.push((uid, arc));
        }
    }
    out
}

/// 把 `AppConfig.multi_account_budget` + `multi_account_vip_recommended`
/// 翻译成 `BudgetSchedulerConfig` 并构造调度器（无账号注入）
async fn build_budget_scheduler(config: &AppConfig) -> Arc<BudgetScheduler> {
    let mb = &config.multi_account_budget;
    let vip = &config.multi_account_vip_recommended;
    let bs_cfg = BudgetSchedulerConfig {
        download_machine_budget: mb.download_machine_budget.resolve(),
        upload_machine_budget: mb.upload_machine_budget.resolve(),
        download_weights: WeightTable {
            normal: mb.weight_normal,
            vip: mb.weight_vip,
            svip: mb.weight_svip,
        },
        upload_weights: WeightTable {
            normal: mb.weight_normal,
            vip: mb.weight_vip,
            svip: mb.weight_svip,
        },
        download_recommended: VipRecommendedTable {
            normal_threads: vip.normal.threads,
            vip_threads: vip.vip.threads,
            svip_threads: vip.svip.threads,
        },
        upload_recommended: VipRecommendedTable {
            normal_threads: vip.normal.threads,
            vip_threads: vip.vip.threads,
            svip_threads: vip.svip.threads,
        },
    };
    BudgetScheduler::new(bs_cfg)
}

/// 计算账号的 effective download / upload 初始配置。
///
/// 用于 `DownloadManager::new_for_account` / `UploadManager::new_for_account`
/// 构造时，让 manager 一开始就采用账号自定义配置而非全局默认值。
///
/// 优先级（与 `update_account_custom_config` handler 一致）：
/// 1. **`auto_apply_recommended = true`**（默认）：
///    - `max_global_threads`：取 VIP 推荐表对应等级的 `threads`
///    - **`max_concurrent_tasks`：取 VIP 推荐表对应等级的 `max_concurrent_tasks`**
///    - 其它字段（`max_retries` / `chunk_size_mb` / `skip_hidden_files`）：
///      仍取 `custom_config.{download,upload}` 字段（这些目前不在 VIP 推荐表里）
/// 2. **`auto_apply_recommended = false`**：
///    - 全部字段从 `custom_config.{download,upload}` 直接取
///
/// auto 模式优先级说明：之前 `auto_apply_recommended=true` 时只消费 VIP 推荐
/// 表的 `threads`，但 UI 层 `BudgetPanel.vue` 暴露 3×4 推荐字段（含 `chunk_size_mb`
/// 和 `max_concurrent_tasks`），用户编辑保存后只有 `threads` 真正影响运行时，
/// 造成"保存了但不生效"的配置假象。修复：auto 模式下 `max_concurrent_tasks`
/// 也跟随 VIP 推荐表。`chunk_size_mb` 受
/// 下载侧"自适应分片大小"影响、上传侧依赖账号自身配置覆盖，留待后续按需接入。
///
/// 返回 `(max_global_threads_dl, max_concurrent_tasks_dl, max_retries_dl,
///        max_global_threads_up, max_concurrent_tasks_up, max_retries_up)`。
pub(crate) fn resolve_effective_account_config(
    user_auth: &UserAuth,
    config: &AppConfig,
) -> (usize, usize, u32, usize, usize, u32) {
    let cc = &user_auth.custom_config;
    let vip = crate::downloader::budget_scheduler::VipType::from_raw(user_auth.vip_type);
    let (recommended_threads, recommended_max_concurrent) = match vip {
        crate::downloader::budget_scheduler::VipType::Normal => (
            config.multi_account_vip_recommended.normal.threads,
            config.multi_account_vip_recommended.normal.max_concurrent_tasks,
        ),
        crate::downloader::budget_scheduler::VipType::Vip => (
            config.multi_account_vip_recommended.vip.threads,
            config.multi_account_vip_recommended.vip.max_concurrent_tasks,
        ),
        crate::downloader::budget_scheduler::VipType::Svip => (
            config.multi_account_vip_recommended.svip.threads,
            config.multi_account_vip_recommended.svip.max_concurrent_tasks,
        ),
    };

    let dl_threads = if cc.auto_apply_recommended {
        recommended_threads
    } else {
        cc.download.max_global_threads
    };
    let up_threads = if cc.auto_apply_recommended {
        recommended_threads
    } else {
        cc.upload.max_global_threads
    };
    let dl_concurrent = if cc.auto_apply_recommended {
        recommended_max_concurrent
    } else {
        cc.download.max_concurrent_tasks
    };
    let up_concurrent = if cc.auto_apply_recommended {
        recommended_max_concurrent
    } else {
        cc.upload.max_concurrent_tasks
    };

    (
        dl_threads,
        dl_concurrent,
        cc.download.max_retries,
        up_threads,
        up_concurrent,
        cc.upload.max_retries,
    )
}

/// 把所有 `AccountManager` 内已加载账号注入 BudgetScheduler。
///
/// 来源：`UserAuth.vip_type` + `UserAuth.custom_config.auto_apply_recommended`。
async fn seed_budget_scheduler(
    bs: &Arc<BudgetScheduler>,
    account_manager: &Arc<Mutex<AccountManager>>,
) {
    let users = {
        let guard = account_manager.lock().await;
        guard
            .list_users()
            .iter()
            .map(|u| {
                (
                    Uid::new(u.uid),
                    VipType::from_raw(u.vip_type),
                    u.custom_config.auto_apply_recommended,
                    u.custom_config.download.max_global_threads,
                    u.custom_config.upload.max_global_threads,
                )
            })
            .collect::<Vec<_>>()
    };
    for (uid, vip, auto, dl_threads, up_threads) in users {
        let dl_req = if auto {
            RequestedSource::Auto
        } else {
            RequestedSource::User(dl_threads)
        };
        let up_req = if auto {
            RequestedSource::Auto
        } else {
            RequestedSource::User(up_threads)
        };
        bs.add_account(uid, vip, dl_req, up_req).await;
    }
}
