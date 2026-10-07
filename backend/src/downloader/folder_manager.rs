//! 文件夹下载管理器

use crate::autobackup::record::BackupRecordManager;
use crate::auth::types::Uid;
use crate::downloader::{DownloadManager, DownloadTask, TaskStatus};
use crate::netdisk::{ClientPool, NetdiskClient};
use crate::server::events::{DownloadEvent, FolderEvent, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::{anyhow, Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::folder::{FolderDownload, FolderStatus, PendingFile};
use crate::persistence::{
    delete_folder as delete_folder_persistence, load_all_folders,
    remove_folder_from_history, remove_tasks_by_group_from_history, save_folder, FolderPersisted,
    PersistenceManager,
};

/// 文件夹下载任务的槽位优先级。
///
/// 归属 `backup_config_id` 的文件夹下载（自动备份、以及分享同步的
/// `share-sync:{订阅id}` 内部下载）是**后台任务**，必须与单文件下载段
/// （`DownloadManager::create_backup_task` → `TaskPriority::Backup`）同档：
/// - 只使用空闲槽位，不抢占任何在跑的任务；
/// - 自身可被用户手动发起的下载（`Normal`）抢占，即"后台让位给前台"。
///
/// 之前这里无条件用 `Normal`，两个后果：
/// 1. 后台同步占住槽位后**用户临时想下载别的文件会被挡住**（Normal 抢不动 Normal）；
/// 2. 分享同步的文件夹会把**同一个订阅**里正在跑的单文件下载（Backup）踢下槽位。
///
/// 只有用户手动发起（`backup_config_id == None`）的文件夹下载才是 `Normal`。
fn folder_slot_priority(
    backup_config_id: Option<&str>,
) -> crate::task_slot_pool::TaskPriority {
    if backup_config_id.is_some() {
        crate::task_slot_pool::TaskPriority::Backup
    } else {
        crate::task_slot_pool::TaskPriority::Normal
    }
}

/// 文件夹下载管理器
#[derive(Debug)]
pub struct FolderDownloadManager {
    /// 所有文件夹下载
    folders: Arc<RwLock<HashMap<String, FolderDownload>>>,
    /// 文件夹取消令牌（用于控制扫描任务）
    cancellation_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
    /// 下载管理器（延迟初始化）
    download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
    /// 网盘客户端（延迟初始化）
    netdisk_client: Arc<RwLock<Option<Arc<NetdiskClient>>>>,
    /// 下载目录（使用 RwLock 支持动态更新）
    download_dir: Arc<RwLock<PathBuf>>,
    /// WAL 目录（用于文件夹持久化）
    wal_dir: Arc<RwLock<Option<PathBuf>>>,
    /// 🔥 WebSocket 管理器
    ws_manager: Arc<RwLock<Option<Arc<WebSocketManager>>>>,
    /// 🔥 文件夹进度通知发送器（由子任务触发，发送 group_id）
    folder_progress_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
    /// 🔥 任务完成通知发送器
    ///
    /// 每个 per-uid `DownloadManager` 都通过 `set_task_completed_sender(tx)` 共享
    /// 同一个 sender，子任务完成时统一推到 listener，由 listener 按 `folder.owner_uid`
    /// 路由到对应账号 manager 做槽位释放 / 补任务。保留为字段以便登录路径
    /// （新增账号 manager 时）能取到现有 sender 注入。
    task_completed_tx:
        Arc<RwLock<Option<mpsc::UnboundedSender<(String, String, u64, u64, bool)>>>>,
    /// 持久化管理器（用于访问历史数据库）
    persistence_manager: Arc<RwLock<Option<Arc<tokio::sync::Mutex<PersistenceManager>>>>>,
    /// 🔥 备份记录管理器（用于文件夹名还原）
    backup_record_manager: Arc<RwLock<Option<Arc<BackupRecordManager>>>>,
    /// 🔥 多账号网盘客户端池
    ///
    /// 当注入后，扫描/列表 API 优先按 `FolderDownload.owner_uid` 从池中取该账号
    /// 的 `NetdiskClient`；未注入时回退到全局 `netdisk_client` 字段（兼容旧路径）。
    client_pool: Arc<RwLock<Option<Arc<RwLock<ClientPool>>>>>,
    /// 🔥 多账号 per-uid 下载管理器池
    ///
    /// 当注入后，子任务创建/补任务/槽位分配按 `FolderDownload.owner_uid` 路由到
    /// 对应账号的 `DownloadManager`；未注入时回退到 `download_manager` 单例字段
    /// （legacy 路径）。这避免"folder.owner_uid=B 但任务被加入 A 的 manager"
    /// 错位。
    download_manager_pool:
        Arc<RwLock<Option<Arc<dashmap::DashMap<crate::auth::Uid, Arc<DownloadManager>>>>>>,
    /// 🔥 文件夹快照上次落盘时间（folder_id → Instant），用于 `persist_folder_throttled` 节流
    ///
    /// 补任务会持续从 `pending_files` 取走文件，若不落盘，重启后队列会退回旧状态并重复
    /// 下载（issue #141）；但每完成一个子任务就全量重写快照，大文件夹（万级 pending）
    /// 会产生大量写放大，故按时间窗口节流。
    last_persist_at: Arc<RwLock<HashMap<String, std::time::Instant>>>,
    /// 🔥 全局配置（延迟注入）
    ///
    /// 仅用于在文件夹自身未携带冲突策略时回退到用户设置的全局默认值，
    /// 见 `resolve_conflict_strategy`。存引用而非快照，保证设置页改动即时生效。
    app_config: Arc<RwLock<Option<Arc<RwLock<crate::config::AppConfig>>>>>,
}

/// 🔥 文件夹快照节流落盘的最小间隔
///
/// 崩溃最多丢失该窗口内的补任务记录；这部分由恢复时的 `reconcile_pending_files`
/// 对账兜底，不会造成重复下载。
const FOLDER_PERSIST_THROTTLE: std::time::Duration = std::time::Duration::from_secs(3);

/// 🔥 子任务在文件夹层面的自动重试次数上限
///
/// 这已是第三层重试：分片级瞬时错误退避（100ms→5s）、链接级切换、调度级重试都耗尽后
/// 才轮到这里。此前会直接判死并计入 `failed_count`，而 `resume_folder` 只接受
/// Paused/Failed 状态，用户在下载途中对单个失败文件无法做任何操作，只能删掉整个
/// 文件夹任务重来（issue #141 用户反馈）。
///
/// 现在改为自动把失败子任务重新排到等待队列尾部重试，耗尽次数才计入 `failed_count`。
const MAX_SUBTASK_AUTO_RETRIES: u32 = 3;

/// 🔥 一批补任务的结果，用于 `refill_tasks` 判断是否需要继续补下一批
#[derive(Debug, Default, Clone, Copy)]
struct RefillBatch {
    /// 本批实际创建的子任务数（仅用于日志/调试，续批决策不看它）
    #[allow(dead_code)]
    created: u64,
    /// 本批因冲突策略命中"跳过"而未建任务的文件数
    skipped: u64,
    /// 本批因文件夹被暂停/取消/删除而中止（此时不应继续补批）
    aborted: bool,
}

/// 🔥 广播"因冲突策略跳过某文件"事件
///
/// 文件夹下载跳过一个文件时不会创建子任务，前端此前收不到任何信号 —— 用户看到的就是
/// "详情里长时间没有任务在跑"，分不清是队列卡死还是在正常跳过（issue #141 用户反馈）。
///
/// 做成自由函数的理由同 `persist_folder_state`：任务完成监听器拿不到 `&self`。
async fn publish_skipped_event(
    ws_manager: &Arc<RwLock<Option<Arc<WebSocketManager>>>>,
    folder_id: &str,
    relative_path: &str,
    owner_uid: crate::auth::Uid,
) {
    let ws = ws_manager.read().await;
    if let Some(ref ws) = *ws {
        ws.send_if_subscribed(
            TaskEvent::Download(DownloadEvent::Skipped {
                // 跳过不产生真实任务，用带前缀的合成 ID，与单文件跳过路径保持一致
                task_id: format!("skipped-{}", uuid::Uuid::new_v4()),
                filename: relative_path.to_string(),
                reason: "文件已存在".to_string(),
                owner_uid: Some(owner_uid.raw()),
                group_id: Some(folder_id.to_string()),
            }),
            None,
        );
    }
}

/// 🔥 解析文件夹生效的下载冲突策略（自由函数版）
///
/// 优先级：文件夹自身策略（创建时确定并已持久化） > 全局默认策略 > `Overwrite`。
///
/// 与 `persist_folder_state` 同理做成自由函数：任务完成监听器拿不到 `&self`，而它是
/// 下载过程中建子任务的主路径，必须和 `refill_tasks` 用同一套策略，否则用户选的
/// "跳过"只对首批任务生效，后续文件照样覆盖。
async fn resolve_folder_conflict_strategy(
    folders: &Arc<RwLock<HashMap<String, FolderDownload>>>,
    app_config: &Arc<RwLock<Option<Arc<RwLock<crate::config::AppConfig>>>>>,
    folder_id: &str,
) -> crate::uploader::conflict::DownloadConflictStrategy {
    if let Some(s) = {
        let guard = folders.read().await;
        guard.get(folder_id).and_then(|f| f.conflict_strategy)
    } {
        return s;
    }

    let cfg_opt = app_config.read().await.clone();
    if let Some(cfg) = cfg_opt {
        return cfg.read().await.conflict_strategy.default_download_strategy;
    }

    crate::uploader::conflict::DownloadConflictStrategy::Overwrite
}

/// 🔥 子任务成功时抵消它对应的那笔失败计数（issue #156）
///
/// 失败侧原本只按 `task_id` 记账，而同一个文件被重新建成子任务时 task_id 会变：
/// 「任务 A 耗尽重试被判死 → 同一文件的任务 B 下载成功」这种组合下，按 task_id 根本
/// 抵消不掉 A 的那笔失败，文件夹会带着一笔幽灵失败进终态 —— 报"N 个文件下载失败"，
/// 而文件其实都好端端在盘上。
///
/// 这里按 task_id 和 fs_id 各查一次，但**最多只抵消一笔**：任务 A 自己重试成功时两边
/// 会命中同一笔失败，`||` 保证不会把 failed_count 多减一次。
fn clear_failed_for_success(
    folder: &mut crate::downloader::folder::FolderDownload,
    task_id: &str,
    fs_id: u64,
) -> bool {
    let by_task = folder.failed_task_ids.remove(task_id);
    let by_fs = fs_id != 0 && folder.failed_fs_ids.remove(&fs_id);
    by_task || by_fs
}

/// 🔥 持久化文件夹快照（自由函数版）
///
/// 做成自由函数而非只有 `&self` 方法，是因为**任务完成监听器**是 `tokio::spawn` 出去的
/// 独立任务，只持有若干 Arc 字段的克隆、拿不到 `&self`；而它恰恰是下载过程中消费
/// `pending_files` 的主路径（每完成一个子任务就补建一个），必须能落盘，否则 issue #141
/// 在这条路径上原样复现。
///
/// `throttle = true` 时，距上次落盘不足 `FOLDER_PERSIST_THROTTLE` 直接跳过。
async fn persist_folder_state(
    folders: &Arc<RwLock<HashMap<String, FolderDownload>>>,
    wal_dir: &Arc<RwLock<Option<PathBuf>>>,
    last_persist_at: &Arc<RwLock<HashMap<String, std::time::Instant>>>,
    folder_id: &str,
    throttle: bool,
) {
    if throttle {
        let last = last_persist_at.read().await;
        if let Some(at) = last.get(folder_id) {
            if at.elapsed() < FOLDER_PERSIST_THROTTLE {
                return;
            }
        }
    }

    let wal_dir = match wal_dir.read().await.clone() {
        Some(dir) => dir,
        None => return, // WAL 目录未设置，跳过持久化
    };

    let folder = {
        let guard = folders.read().await;
        guard.get(folder_id).cloned()
    };

    if let Some(folder) = folder {
        let persisted = FolderPersisted::from_folder(&folder);
        if let Err(e) = save_folder(&wal_dir, &persisted) {
            error!("持久化文件夹 {} 失败: {}", folder_id, e);
        }
    }

    // 记录落盘时间，避免紧接着的节流落盘做无谓的重复写
    last_persist_at
        .write()
        .await
        .insert(folder_id.to_string(), std::time::Instant::now());
}

impl FolderDownloadManager {
    /// 创建新的文件夹下载管理器
    pub fn new(download_dir: PathBuf) -> Self {
        Self {
            folders: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            download_manager: Arc::new(RwLock::new(None)),
            netdisk_client: Arc::new(RwLock::new(None)),
            download_dir: Arc::new(RwLock::new(download_dir)),
            wal_dir: Arc::new(RwLock::new(None)),
            ws_manager: Arc::new(RwLock::new(None)),
            folder_progress_tx: Arc::new(RwLock::new(None)),
            task_completed_tx: Arc::new(RwLock::new(None)),
            persistence_manager: Arc::new(RwLock::new(None)),
            backup_record_manager: Arc::new(RwLock::new(None)),
            client_pool: Arc::new(RwLock::new(None)),
            download_manager_pool: Arc::new(RwLock::new(None)),
            last_persist_at: Arc::new(RwLock::new(HashMap::new())),
            app_config: Arc::new(RwLock::new(None)),
        }
    }

    /// 🔥 注入多账号网盘客户端池
    ///
    /// 注入后 `client_for(uid)` 会优先按 uid 查池；未注入时维持原有全局
    /// `netdisk_client` 路径。`AppState` 在创建池且活跃账号客户端已注入后
    /// 调用一次即可。
    pub async fn set_client_pool(&self, pool: Arc<RwLock<ClientPool>>) {
        let mut guard = self.client_pool.write().await;
        *guard = Some(pool);
        info!("FolderDownloadManager 已注入 ClientPool");
    }

    /// 🔥 注入多账号 per-uid `DownloadManager` 池
    ///
    /// 注入后 `download_manager_for(uid)` 会优先按 uid 查池；未注入时维持原有
    /// 全局 `download_manager` 路径。`AppState::load_initial_session` 在所有
    /// 持久化账号 manager 注册到池后调用一次即可。
    pub async fn set_download_manager_pool(
        &self,
        pool: Arc<dashmap::DashMap<Uid, Arc<DownloadManager>>>,
    ) {
        let mut guard = self.download_manager_pool.write().await;
        *guard = Some(pool);
        info!("FolderDownloadManager 已注入 DownloadManager 池");
    }

    /// 🔥 按 uid 解析 `DownloadManager`
    ///
    /// pool 已注入但 uid miss 时**不再** fallback 到
    /// legacy 单例 — 这种情况意味着 owner_uid 已被删除 / manager 构造失败 / pool
    /// 漏注入，悄悄退到 active 单例会让任务被错误归并。现严格返回 `None`。
    ///
    /// 优先级：
    /// 1. **`download_manager_pool` 已注入**（多账号路径）：
    ///    - 命中 uid → 返回该账号独立 manager
    ///    - **miss uid → 返回 `None`**（让调用方失败，不再 fallback）
    /// 2. **`download_manager_pool` 未注入**（legacy / 单账号兼容路径）：
    ///    - 返回全局 `download_manager` 字段
    ///
    /// 调用方应把 `None` 视作硬错误（任务不能被处理），而非"用 active 兜底"。
    pub async fn download_manager_for(&self, uid: Uid) -> Option<Arc<DownloadManager>> {
        // 路径 1：pool 已注入 → 严格按 uid 命中或返回 None
        if let Some(pool_arc) = self.download_manager_pool.read().await.as_ref() {
            return pool_arc
                .get(&uid)
                .map(|entry| Arc::clone(entry.value()));
        }
        // 路径 2：pool 未注入 → legacy 全局 download_manager（单账号兼容）
        self.download_manager.read().await.clone()
    }

    /// 🔥 按 uid 解析 `NetdiskClient`
    ///
    /// 优先级：
    /// 1. `client_pool.get_client(uid)`（多账号路径）
    /// 2. 全局 `netdisk_client` 字段（legacy 单用户路径）
    ///
    /// 返回 `None` 表示两条路径都不可用 → 调用方应放弃扫描并标记任务失败。
    pub async fn client_for(&self, uid: Uid) -> Option<Arc<NetdiskClient>> {
        // 路径 1：池
        if let Some(pool_arc) = self.client_pool.read().await.as_ref() {
            let pool = pool_arc.read().await;
            if let Some(client) = pool.get_client(uid) {
                return Some(client);
            }
        }
        // 路径 2：legacy 全局客户端
        self.netdisk_client.read().await.clone()
    }

    /// 设置持久化管理器
    pub async fn set_persistence_manager(&self, pm: Arc<tokio::sync::Mutex<PersistenceManager>>) {
        let mut pm_guard = self.persistence_manager.write().await;
        *pm_guard = Some(pm);
        info!("文件夹下载管理器已设置持久化管理器");
    }

    /// 🔥 设置 WebSocket 管理器
    pub async fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        let mut ws = self.ws_manager.write().await;
        *ws = Some(ws_manager);
        info!("文件夹下载管理器已设置 WebSocket 管理器");
    }

    /// 🔥 设置备份记录管理器（用于文件夹名还原）
    pub async fn set_backup_record_manager(&self, record_manager: Arc<BackupRecordManager>) {
        let mut rm = self.backup_record_manager.write().await;
        *rm = Some(record_manager);
        info!("文件夹下载管理器已设置备份记录管理器");
    }

    /// 🔥 还原加密文件夹名为原始名
    async fn restore_folder_name(&self, encrypted_name: &str, parent_path: &str) -> Option<String> {
        use crate::encryption::service::EncryptionService;

        if !EncryptionService::is_encrypted_folder_name(encrypted_name) {
            return None;
        }

        let rm = self.backup_record_manager.read().await;
        if let Some(ref record_manager) = *rm {
            // 🔥 直接通过加密文件夹名查询（加密名是 UUID 格式，全局唯一，无需 config_id）
            if let Ok(snapshots) = record_manager.get_all_folder_mappings_by_encrypted_name(encrypted_name) {
                // 优先匹配 parent_path
                for snapshot in &snapshots {
                    if snapshot.original_path == parent_path {
                        info!("还原文件夹名（精确匹配）: {} -> {}", encrypted_name, snapshot.original_name);
                        return Some(snapshot.original_name.clone());
                    }
                }
                // 如果没有精确匹配，返回第一个结果（加密名是 UUID，理论上只有一条记录）
                if let Some(snapshot) = snapshots.first() {
                    info!("还原文件夹名（首条记录）: {} -> {}", encrypted_name, snapshot.original_name);
                    return Some(snapshot.original_name.clone());
                }
            }
        } else {
            warn!("backup_record_manager 未设置，无法还原加密文件夹名: {}", encrypted_name);
        }
        None
    }

    /// 🔥 还原相对路径中的所有加密文件夹名
    ///
    /// 将路径中的 BPR_DIR_xxx 格式的加密文件夹名还原为原始名
    /// 例如：`BPR_DIR_xxx/BPR_DIR_yyy/file.txt` -> `documents/photos/file.txt`
    async fn restore_encrypted_path(&self, relative_path: &str, root_path: &str) -> String {
        use crate::encryption::service::EncryptionService;

        let parts: Vec<&str> = relative_path.split('/').collect();
        if parts.is_empty() {
            return relative_path.to_string();
        }

        let mut restored_parts = Vec::new();
        let mut current_parent = root_path.trim_end_matches('/').to_string();

        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }

            // 最后一个部分是文件名，不需要还原
            if i == parts.len() - 1 {
                restored_parts.push(part.to_string());
                break;
            }

            // 检查是否是加密文件夹名
            if EncryptionService::is_encrypted_folder_name(part) {
                if let Some(original) = self.restore_folder_name(part, &current_parent).await {
                    restored_parts.push(original);
                } else {
                    // 找不到映射，保留原名
                    restored_parts.push(part.to_string());
                }
            } else {
                restored_parts.push(part.to_string());
            }

            // 更新 parent_path（使用加密名，因为数据库中存储的是加密路径）
            current_parent = format!("{}/{}", current_parent, part);
        }

        restored_parts.join("/")
    }

    /// 🔥 发布文件夹事件
    async fn publish_event(&self, event: FolderEvent) {
        let ws = self.ws_manager.read().await;
        if let Some(ref ws) = *ws {
            ws.send_if_subscribed(TaskEvent::Folder(event), None);
        }
    }

    /// 🔥 获取文件夹进度通知发送器
    ///
    /// 用于在子任务进度变化时通知文件夹管理器发送聚合进度
    pub async fn get_folder_progress_sender(&self) -> Option<mpsc::UnboundedSender<String>> {
        let tx = self.folder_progress_tx.read().await;
        tx.clone()
    }

    /// 🔥 设置文件夹关联的转存任务 ID
    pub async fn set_folder_transfer_id(&self, folder_id: &str, transfer_task_id: String) {
        let mut folders = self.folders.write().await;
        if let Some(folder) = folders.get_mut(folder_id) {
            folder.transfer_task_id = Some(transfer_task_id.clone());
            info!("设置文件夹 {} 关联转存任务 ID: {}", folder_id, transfer_task_id);
            // 持久化更新
            drop(folders);
            self.persist_folder(folder_id).await;
        } else {
            warn!("文件夹 {} 不存在，无法设置 transfer_task_id", folder_id);
        }
    }

    /// 设置 WAL 目录（用于文件夹持久化）
    pub async fn set_wal_dir(&self, wal_dir: PathBuf) {
        let mut dir = self.wal_dir.write().await;
        *dir = Some(wal_dir);
    }

    /// 🔥 注入全局配置（用于冲突策略回退，见 `resolve_conflict_strategy`）
    pub async fn set_app_config(&self, config: Arc<RwLock<crate::config::AppConfig>>) {
        let mut guard = self.app_config.write().await;
        *guard = Some(config);
    }

    /// 持久化文件夹状态
    async fn persist_folder(&self, folder_id: &str) {
        persist_folder_state(
            &self.folders,
            &self.wal_dir,
            &self.last_persist_at,
            folder_id,
            false,
        )
            .await;
    }

    /// 🔥 节流持久化文件夹状态
    ///
    /// 与 `persist_folder` 的区别：距上次落盘不足 `FOLDER_PERSIST_THROTTLE` 时直接跳过。
    /// 用于补任务这类高频路径——`pending_files` 每完成一个子任务就会被取走一个，
    /// 不落盘会导致重启后队列退回旧状态并重复下载（issue #141），但每次都全量重写
    /// 快照在万级文件夹上写放大过大。
    async fn persist_folder_throttled(&self, folder_id: &str) {
        persist_folder_state(
            &self.folders,
            &self.wal_dir,
            &self.last_persist_at,
            folder_id,
            true,
        )
            .await;
    }

    /// 🔥 清理文件夹的节流落盘记录（文件夹被删除/取消时调用，避免 map 无限增长）
    async fn forget_persist_state(&self, folder_id: &str) {
        self.last_persist_at.write().await.remove(folder_id);
    }

    /// 🔥 按存活文件夹裁剪节流落盘记录
    ///
    /// 批量清理（如 `clear_completed_folders`）走 `retain` 直接从 folders 移除，
    /// 不经过单个删除入口；用存活集合兜底裁剪，避免遗漏新增的移除点造成缓慢泄漏。
    async fn prune_persist_state(&self) {
        let alive: HashSet<String> = {
            let folders = self.folders.read().await;
            folders.keys().cloned().collect()
        };
        self.last_persist_at
            .write()
            .await
            .retain(|id, _| alive.contains(id));
    }

    /// 🔥 解析文件夹生效的下载冲突策略
    ///
    /// 优先级：文件夹自身策略（创建时由前端/配置确定，并已持久化） > 全局默认策略 > `Overwrite`。
    ///
    /// 中间那层是给**升级前创建的旧文件夹**兜底的：它们的持久化数据里没有
    /// `conflict_strategy` 字段，恢复后为 `None`。此时读全局配置比硬编码 `Overwrite`
    /// 更贴近用户意图——用户在设置页选了"跳过"，就不该因为任务是升级前建的而被覆盖。
    async fn resolve_conflict_strategy(
        &self,
        folder_id: &str,
    ) -> crate::uploader::conflict::DownloadConflictStrategy {
        resolve_folder_conflict_strategy(&self.folders, &self.app_config, folder_id).await
    }

    /// 删除文件夹持久化数据
    async fn delete_folder_persistence(&self, folder_id: &str) {
        let wal_dir = {
            let dir = self.wal_dir.read().await;
            dir.clone()
        };

        if let Some(wal_dir) = wal_dir {
            if let Err(e) = delete_folder_persistence(&wal_dir, folder_id) {
                error!("删除文件夹持久化数据 {} 失败: {}", folder_id, e);
            }
        }
    }

    /// 从持久化存储恢复文件夹任务
    ///
    /// 返回 (恢复成功数, 跳过数)
    pub async fn restore_folders(&self) -> (usize, usize) {
        let wal_dir = {
            let dir = self.wal_dir.read().await;
            dir.clone()
        };

        let wal_dir = match wal_dir {
            Some(dir) => dir,
            None => {
                warn!("WAL 目录未设置，跳过文件夹恢复");
                return (0, 0);
            }
        };

        // 加载所有持久化的文件夹
        let persisted_folders = match load_all_folders(&wal_dir) {
            Ok(folders) => folders,
            Err(e) => {
                error!("加载文件夹持久化数据失败: {}", e);
                return (0, 0);
            }
        };

        if persisted_folders.is_empty() {
            info!("没有需要恢复的文件夹任务");
            return (0, 0);
        }

        info!("发现 {} 个持久化的文件夹任务", persisted_folders.len());

        // 🔥 folder owner_uid 兜底用的 active_uid（与 download/upload 保持一致）
        // 若全局无 active_uid（理论不可能，因 restore_folders 只会在登录后调用），保持原值不变。
        let active_uid_fallback: Option<crate::auth::Uid> = self
            .download_manager
            .read()
            .await
            .as_ref()
            .map(|dm| dm.owner_uid())
            .filter(|u| u.raw() != 0);

        let mut restored = 0;
        let mut skipped = 0;

        for persisted in persisted_folders {
            // 跳过已完成或已取消的文件夹
            if persisted.status == FolderStatus::Completed
                || persisted.status == FolderStatus::Cancelled
            {
                info!(
                    "跳过已完成/取消的文件夹: {} ({})",
                    persisted.name, persisted.id
                );
                skipped += 1;
                // 删除已完成/取消的持久化文件
                if let Err(e) = delete_folder_persistence(&wal_dir, &persisted.id) {
                    warn!("删除已完成文件夹持久化数据失败: {}", e);
                }
                continue;
            }

            // 转换为 FolderDownload
            let mut folder = persisted.to_folder();

            // 🔥 folder.owner_uid==Uid(0) 视作 None，
            // 用 active_uid 兜底填充（与 classify_recovery_branch 的 LegacyFillActive 等价）
            if folder.owner_uid.raw() == 0 {
                if let Some(uid) = active_uid_fallback {
                    debug!(
                        "folder T2-10 兜底: folder_id={} owner_uid 0 → {}",
                        folder.id,
                        uid.raw()
                    );
                    folder.owner_uid = uid;
                }
            }

            // 将状态设置为 Paused，等待用户手动恢复
            folder.status = FolderStatus::Paused;

            let folder_id = folder.id.clone();

            info!(
                "恢复文件夹任务: {} ({}) - {} 个文件, {} 已完成, {} 待处理 (暂停状态，不占用槽位)",
                folder.name,
                folder_id,
                folder.total_files,
                folder.completed_count,
                folder.pending_files.len()
            );

            // 🔥 暂停状态的文件夹不分配槽位，等待用户手动恢复时再分配
            // 这样可以让正在下载的任务借用更多槽位
            folder.fixed_slot_id = None;
            folder.borrowed_slot_ids = Vec::new();

            // 添加到内存
            {
                let mut folders = self.folders.write().await;
                folders.insert(folder_id.clone(), folder);
            }

            // 🔥 持久化更新后的槽位信息
            self.persist_folder(&folder_id).await;

            restored += 1;
        }

        info!(
            "文件夹恢复完成: 恢复 {} 个, 跳过 {} 个",
            restored, skipped
        );

        (restored, skipped)
    }

    /// 同步恢复的子任务进度到文件夹
    ///
    /// 在恢复子任务后调用，将子任务的进度同步到对应的文件夹
    /// 同时维护 borrowed_subtask_map，确保借调位回收时能正确找到对应的子任务
    /// 🔥 修复：为已恢复但没有槽位的子任务分配借调位
    /// 🔥 改为按 folder.owner_uid 路由 download_manager
    pub async fn sync_restored_tasks_progress(&self) {
        // 获取所有文件夹 (id, owner_uid)
        let folders_meta: Vec<(String, crate::auth::Uid)> = {
            let folders = self.folders.read().await;
            folders.iter().map(|(k, f)| (k.clone(), f.owner_uid)).collect()
        };

        for (folder_id, folder_owner_uid) in folders_meta {
            let download_manager = match self.download_manager_for(folder_owner_uid).await {
                Some(dm) => dm,
                None => {
                    warn!(
                        "sync_restored_tasks_progress: 无法解析 folder.owner_uid={} 的下载管理器，跳过",
                        folder_owner_uid.raw()
                    );
                    continue;
                }
            };
            // 获取该文件夹的所有子任务
            let tasks = download_manager.get_tasks_by_group(&folder_id).await;

            if tasks.is_empty() {
                continue;
            }

            let downloaded_size: u64 = tasks.iter().map(|t| t.downloaded_size).sum();

            // 🔥 收集需要分配槽位的子任务（没有槽位且非完成状态）
            let tasks_needing_slots: Vec<String> = tasks
                .iter()
                .filter(|t| t.slot_id.is_none() && t.status != TaskStatus::Completed)
                .map(|t| t.id.clone())
                .collect();

            // 更新文件夹进度，并维护 borrowed_subtask_map
            {
                let mut folders = self.folders.write().await;
                if let Some(folder) = folders.get_mut(&folder_id) {
                    // 🔥 注意：不再从 tasks 计算 completed_count，因为已完成的任务会从内存移除
                    // completed_count 由 start_task_completed_listener 维护

                    // 🔥 初始化 completed_downloaded_size：
                    // folder.downloaded_size 来自持久化，已包含已完成任务的字节数
                    // downloaded_size（此处变量）= 仅活跃任务之和
                    // 差值即为已完成任务的累计字节数
                    folder.completed_downloaded_size = folder.downloaded_size.saturating_sub(downloaded_size);

                    // 🔥 维护 borrowed_subtask_map：记录使用借调位的子任务
                    // 这样在回收借调位时才能正确找到并暂停对应的子任务
                    for task in &tasks {
                        if task.is_borrowed_slot {
                            if let Some(slot_id) = task.slot_id {
                                // 只记录非完成状态的任务
                                if task.status != TaskStatus::Completed {
                                    folder.borrowed_subtask_map.insert(task.id.clone(), slot_id);
                                    info!(
                                        "恢复时记录借调位映射: task_id={}, slot_id={}",
                                        task.id, slot_id
                                    );
                                }
                            }
                        }
                    }

                    // 🔥 为没有槽位的子任务分配空闲的借调位或固定位
                    for task_id in &tasks_needing_slots {
                        // 先查找空闲的借调位（在 borrowed_slot_ids 中但不在 borrowed_subtask_map 中）
                        let mut found_slot = None;
                        for &slot_id in &folder.borrowed_slot_ids {
                            if !folder.borrowed_subtask_map.values().any(|&s| s == slot_id) {
                                found_slot = Some(slot_id);
                                break;
                            }
                        }

                        if let Some(slot_id) = found_slot {
                            folder.borrowed_subtask_map.insert(task_id.clone(), slot_id);
                            info!(
                                "恢复时为无槽位子任务分配借调位: task_id={}, slot_id={}",
                                task_id, slot_id
                            );
                        } else if let Some(fixed_slot_id) = folder.fixed_slot_id {
                            // 如果没有空闲借调位，使用固定位
                            // 注意：固定位不记录在 borrowed_subtask_map 中，由子任务的 slot_id 字段直接持有
                            info!(
                                "恢复时为无槽位子任务分配固定位: task_id={}, slot_id={}",
                                task_id, fixed_slot_id
                            );
                            // 注意：这里只打印日志，实际分配在后续步骤中由 download_manager 处理
                            // 因为我们在这里无法直接修改任务的 slot_id
                        }
                    }

                    info!(
                        "同步文件夹 {} 进度: {} 个子任务, {} 已完成, 已下载 {} bytes, 借调位映射 {} 个",
                        folder.name,
                        tasks.len(),
                        folder.completed_count,
                        folder.downloaded_size,
                        folder.borrowed_subtask_map.len()
                    );
                }
            }

            // 🔥 更新子任务的槽位信息到 DownloadManager
            let mut fixed_slot_used = false;
            for task_id in &tasks_needing_slots {
                let (slot_info, fixed_slot_id) = {
                    let folders = self.folders.read().await;
                    if let Some(folder) = folders.get(&folder_id) {
                        (
                            folder.borrowed_subtask_map.get(task_id).copied(),
                            folder.fixed_slot_id
                        )
                    } else {
                        (None, None)
                    }
                };

                if let Some(slot_id) = slot_info {
                    // 使用借调位
                    download_manager
                        .update_task_slot(task_id, slot_id, true)
                        .await;
                } else if let Some(fixed_slot_id) = fixed_slot_id {
                    // 如果没有借调位，且固定位还未被使用，则使用固定位
                    if !fixed_slot_used {
                        download_manager
                            .update_task_slot(task_id, fixed_slot_id, false)
                            .await;
                        fixed_slot_used = true;
                    }
                }
            }
        }
    }

    /// 恢复模式补充暂停任务
    ///
    /// 在恢复流程结束后调用，从 pending_files 创建 DownloadTask，
    /// 状态设为 Paused，仅写入 download_manager.tasks，不入等待队列，不触发调度器。
    ///
    /// 这样做的目的是让前端能看到"等待/暂停"任务，但不会自动开始下载。
    /// 用户点击"继续"时，由 resume_folder 调用 resume_task + refill_tasks 启动下载。
    ///
    /// # Arguments
    /// * `target_count` - 目标任务数（计入已恢复的子任务）
    ///
    /// # Returns
    /// 创建的暂停任务数
    /// 🔥 每个文件夹按 `folder.owner_uid` 路由 download_manager
    pub async fn prefill_paused_tasks(&self, target_count: usize) -> usize {
        // 获取所有需要补任务的文件夹 ID + owner_uid
        let folder_ids: Vec<(String, crate::auth::Uid)> = {
            let folders = self.folders.read().await;
            folders
                .iter()
                .filter(|(_, f)| {
                    // 只处理：已暂停、扫描完成、还有 pending_files 的文件夹
                    f.status == FolderStatus::Paused
                        && f.scan_completed
                        && !f.pending_files.is_empty()
                })
                .map(|(id, f)| (id.clone(), f.owner_uid))
                .collect()
        };

        if folder_ids.is_empty() {
            return 0;
        }

        let mut total_created = 0usize;

        for (folder_id, folder_owner_uid) in folder_ids {
            // 🔥 建任务前必须先对账。
            //
            //    本方法在**启动恢复阶段**运行（`AppState` 恢复流程紧接 sync 之后调用），
            //    是恢复后第一个消费 pending_files 的入口——比 resume_folder 更早。若不对账，
            //    快照里残留的"已经下载完成/已有子任务"的条目会在这里被建成任务：既可能重复
            //    下载，也会让这些文件的字节同时出现在 completed_downloaded_size 和活跃任务
            //    的 active_sum 里，被 compute_downloaded_size 的 max() 永久锁成虚高进度。
            self.reconcile_pending_files(&folder_id).await;

            let download_manager = match self.download_manager_for(folder_owner_uid).await {
                Some(dm) => dm,
                None => {
                    warn!(
                        "prefill_paused_tasks: 无法解析 folder.owner_uid={} 的下载管理器，跳过",
                        folder_owner_uid.raw()
                    );
                    continue;
                }
            };
            // 获取该文件夹已有的子任务数
            let existing_tasks = download_manager.get_tasks_by_group(&folder_id).await;
            let existing_count = existing_tasks.len();

            // 计算需要补充的数量
            if existing_count >= target_count {
                continue;
            }
            let needed = target_count - existing_count;

            // 从 pending_files 取出需要的文件
            let (files_to_create, local_root, group_root, folder_created_at, folder_owner_uid) = {
                let mut folders = self.folders.write().await;
                let folder = match folders.get_mut(&folder_id) {
                    Some(f) => f,
                    None => continue,
                };

                // 再次检查状态
                if folder.status != FolderStatus::Paused || !folder.scan_completed {
                    continue;
                }

                // 🔥 底账重建后 pending_files 可能包含「已经恢复出子任务」的文件，
                //    按 fs_id 去重，只补真正缺任务的那些
                let existing_fs_ids: std::collections::HashSet<u64> =
                    existing_tasks.iter().map(|t| t.fs_id).collect();
                folder
                    .pending_files
                    .retain(|f| !existing_fs_ids.contains(&f.fs_id));

                // 🔥 issue #156 续：已下完的文件也必须剔除，否则 #156 之前写脏的快照
                //    恢复后会把它们原样重下一遍（说明见 prune_completed_from_pending）。
                let pruned = folder.prune_completed_from_pending();
                if pruned > 0 {
                    warn!(
                        "文件夹 {} 恢复时剪掉 {} 个已下完却仍留在 pending 队列的文件（脏快照自愈）",
                        folder_id, pruned
                    );
                }

                // 🔥 只挑选文件，**不从 pending_files 摘除**
                //
                // 子任务在真正启动之前是不落盘的（`add_task` / `add_task_paused` 都只往
                // 内存任务表里插，只有 `start_task_internal` 才注册持久化）。如果建任务时
                // 就把文件从 pending_files 摘掉，那「已建任务、还没启动」的文件在重启后
                // 两头都不存在 —— 实测一个 8 文件的文件夹重启后只剩 2 个任务、0 待处理。
                //
                // 所以摘除时机推迟到任务真正落盘那一刻（见
                // `FolderDownloadManager::drop_pending_file_after_persist`）。在此之前
                // 文件同时存在于 pending_files 和内存任务里，靠下面的 fs_id 去重
                // 保证不会重复建任务。
                let existing_fs_ids: std::collections::HashSet<u64> =
                    existing_tasks.iter().map(|t| t.fs_id).collect();
                let files: Vec<_> = folder
                    .pending_files
                    .iter()
                    .filter(|f| !existing_fs_ids.contains(&f.fs_id))
                    .take(needed)
                    .cloned()
                    .collect();
                if files.is_empty() {
                    continue;
                }
                (
                    files,
                    folder.local_root.clone(),
                    folder.remote_root.clone(),
                    folder.created_at,
                    folder.owner_uid,
                )
            };

            if files_to_create.is_empty() {
                continue;
            }

            info!(
                "恢复模式补任务: 文件夹 {} 需要补充 {} 个暂停任务 (已有 {} 个)",
                folder_id,
                files_to_create.len(),
                existing_count
            );

            // 🔥 冲突策略：与 refill_tasks / 任务完成监听器保持一致，
            //    否则恢复模式预建的任务会无视用户选的"跳过"直接覆盖本地已有文件（issue #141）
            let conflict_strategy = self.resolve_conflict_strategy(&folder_id).await;

            // 创建暂停状态的任务
            let mut created_count = 0u64;
            let mut skipped_count = 0u64;
            let mut skipped_bytes = 0u64;
            let mut skipped_list: Vec<crate::downloader::folder::SkippedFile> = Vec::new();
            for pending_file in files_to_create {
                let local_path = local_root.join(&pending_file.relative_path);

                // 🔥 应用冲突策略
                let final_local_path = {
                    use crate::uploader::conflict_resolver::ConflictResolver;
                    match ConflictResolver::resolve_download_conflict(&local_path, conflict_strategy)
                    {
                        Ok(crate::uploader::conflict::ConflictResolution::Proceed) => local_path,
                        Ok(crate::uploader::conflict::ConflictResolution::Skip) => {
                            info!("跳过下载（文件已存在）: {:?}", local_path);
                            // 🔥 跳过的文件必须从 pending_files 摘掉（issue #156）
                            //    与另外两条补任务路径同因同治：本路径同样只挑选不摘除，
                            //    Skip 又不建任务、等不到落盘回调，文件会赖在队列里被反复
                            //    选中重复跳过，skipped_count 越算越大。
                            {
                                let mut folders_guard = self.folders.write().await;
                                if let Some(folder) = folders_guard.get_mut(&folder_id) {
                                    folder
                                        .pending_files
                                        .retain(|p| p.fs_id != pending_file.fs_id);
                                }
                            }
                            skipped_count += 1;
                            skipped_bytes += pending_file.size;
                            skipped_list.push(crate::downloader::folder::SkippedFile {
                                relative_path: pending_file.relative_path.clone(),
                                size: pending_file.size,
                                skipped_at: chrono::Utc::now().timestamp(),
                            });
                            publish_skipped_event(
                                &self.ws_manager,
                                &folder_id,
                                &pending_file.relative_path,
                                folder_owner_uid,
                            )
                                .await;
                            continue;
                        }
                        Ok(crate::uploader::conflict::ConflictResolution::UseNewPath(new_path)) => {
                            info!("自动重命名下载路径: {:?} -> {}", local_path, new_path);
                            PathBuf::from(new_path)
                        }
                        Err(e) => {
                            warn!("冲突解决失败: {}, 使用原路径", e);
                            local_path
                        }
                    }
                };

                // 确保目录存在
                if let Some(parent) = final_local_path.parent() {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        error!("创建目录失败: {:?}, 错误: {}", parent, e);
                        continue;
                    }
                }

                let mut task = DownloadTask::new_with_group(
                    pending_file.fs_id,
                    pending_file.remote_path.clone(),
                    final_local_path,
                    pending_file.size,
                    folder_id.clone(),
                    group_root.clone(),
                    pending_file.relative_path,
                    folder_owner_uid,
                );

                // 恢复模式下，保持任务创建时间不晚于原文件夹创建时间，
                // 避免前端按 created_at 排序时，新补的暂停任务排在旧任务前。
                task.created_at = folder_created_at;

                // 使用 add_task_paused 添加暂停任务（不入调度队列）
                if let Err(e) = download_manager.add_task_paused(task).await {
                    warn!("恢复模式创建暂停任务失败: {}", e);
                } else {
                    created_count += 1;
                }
            }

            // 更新已创建 / 跳过计数
            if created_count > 0 || skipped_count > 0 {
                {
                    let mut folders = self.folders.write().await;
                    if let Some(folder) = folders.get_mut(&folder_id) {
                        folder.created_count += created_count;
                        folder.skipped_count += skipped_count;
                        folder.skipped_size += skipped_bytes;
                        folder.skipped_entries.append(&mut skipped_list);
                    }
                }
                total_created += created_count as usize;
                info!(
                    "恢复模式补任务完成: 文件夹 {} 创建了 {} 个暂停任务, 跳过 {} 个已存在文件",
                    folder_id, created_count, skipped_count
                );

                // 🔥 本路径同样从 pending_files 取走了文件，必须落盘。
                //    它在启动恢复阶段运行，若不落盘，紧接着的一次强杀会让队列退回
                //    上次的旧状态并重复下载（issue #141）。
                self.persist_folder(&folder_id).await;
            }
        }

        info!(
            "恢复模式补任务全部完成: 共创建 {} 个暂停任务",
            total_created
        );
        total_created
    }

    /// 设置下载管理器
    pub async fn set_download_manager(&self, manager: Arc<DownloadManager>) {
        // 🔥 把"创建 channel + 启动监听器"做成幂等
        // 一次创建后，channel 句柄保留在 task_completed_tx / folder_progress_tx；
        // 后续 set_download_manager 调用（账号切换/新登录）只把新 manager 的 sender
        // 指到同一个 channel，避免：
        //   (a) 重新创建 channel 后旧 manager 的 sender 持有 dropped tx → 旧账号
        //       任务完成事件被静默丢弃；
        //   (b) 多次启动 spawn listener → 重复处理同一事件
        let already_initialized = {
            let task_tx_guard = self.task_completed_tx.read().await;
            let folder_tx_guard = self.folder_progress_tx.read().await;
            task_tx_guard.is_some() && folder_tx_guard.is_some()
        };

        if already_initialized {
            // 复用已有 channel，把新 manager 的 sender 指到同一 channel
            self.share_senders_with(&manager).await;
            // 更新 download_manager 单例引用（active manager 切换的 legacy 字段维护）
            {
                let mut dm = self.download_manager.write().await;
                *dm = Some(manager);
            }
            info!(
                "FolderDownloadManager: 复用现有 channel + 监听器，已把新 manager 的 sender 接入"
            );
            return;
        }

        // 首次进入：创建 channel + 启动监听器
        // 创建任务完成通知 channel（发送 group_id 和 task_id）
        let (tx, rx) = mpsc::unbounded_channel::<(String, String, u64, u64, bool)>();

        // 设置 sender 到 download_manager
        manager.set_task_completed_sender(tx.clone()).await;

        // 保存 sender 句柄
        {
            let mut tx_guard = self.task_completed_tx.write().await;
            *tx_guard = Some(tx);
        }

        // 🔥 创建文件夹进度通知通道（由子任务进度变化触发）
        let (folder_progress_tx, folder_progress_rx) = mpsc::unbounded_channel::<String>();

        // 🔥 设置文件夹进度发送器到下载管理器（供子任务使用）
        manager.set_folder_progress_sender(folder_progress_tx.clone()).await;

        // 保存 download_manager
        {
            let mut dm = self.download_manager.write().await;
            *dm = Some(manager);
        }

        // 启动监听任务
        self.start_task_completed_listener(rx);

        // 保存 sender（供外部获取使用）
        {
            let mut tx_guard = self.folder_progress_tx.write().await;
            *tx_guard = Some(folder_progress_tx);
        }

        // 启动文件夹进度监听器
        self.start_folder_progress_listener(folder_progress_rx);

        info!(
            "文件夹下载管理器已设置下载管理器，任务完成监听和进度监听器已启动（首次初始化）"
        );
    }

    /// 🔥 把已存在的 task_completed / folder_progress sender 注入到新 manager
    ///
    /// 用于：登录新增账号 / 启动期非 active 持久化账号 等场景下，per-uid manager
    /// 必须共享同一对 sender → 子任务完成才会触发监听器分发；不调用 listener
    /// 拿不到事件，文件夹补任务永远卡死。
    ///
    /// 调用方应在 active manager 已通过 `set_download_manager()` 创建 channel
    /// 之后调用本方法。如果 senders 还没初始化（极端情况：还没设置过 active
    /// manager），方法是 no-op + warn。
    pub async fn share_senders_with(&self, manager: &Arc<DownloadManager>) {
        let task_tx = self.task_completed_tx.read().await.clone();
        let folder_tx = self.folder_progress_tx.read().await.clone();
        match (task_tx, folder_tx) {
            (Some(t), Some(f)) => {
                manager.set_task_completed_sender(t).await;
                manager.set_folder_progress_sender(f).await;
                info!(
                    "FolderDownloadManager: 已共享 task_completed / folder_progress sender 给 per-uid manager"
                );
            }
            _ => {
                warn!(
                    "share_senders_with: senders 尚未初始化（active manager 还没通过 set_download_manager 设置过），跳过"
                );
            }
        }
    }

    /// 🔥 待办文件补任务兜底循环
    ///
    /// 补任务原本**只由「本文件夹的子任务完成」事件驱动**
    /// （`update_folder_progress` 是死代码，没有任何调用方）。
    ///
    /// 子任务拿不到槽位时会把文件退回 `pending_files`（见 `refill_tasks_batch`），
    /// 这是为了让待办可持久化 —— 排队中的任务只活在内存里，后端一重启就整个丢失。
    /// 但退回之后如果文件夹恰好一个子任务都没在跑（槽位刚好被别的任务回收走），
    /// 就再没有任何事件能驱动它，待办会一直补不上。
    ///
    /// 本循环是兜底：每 3 秒检查一次，只在**池子里确实有空闲槽位**时才去扫还有
    /// 待办的文件夹，避免无谓的落盘churn。
    pub fn start_pending_refill_loop(self: &Arc<Self>) {
        let manager = Arc::clone(self);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(3));
            // 跳过第一次立即触发
            ticker.tick().await;

            loop {
                ticker.tick().await;

                // 有待办文件、且处于下载中的文件夹
                let candidates: Vec<(String, crate::auth::Uid)> = {
                    let folders_guard = manager.folders.read().await;
                    folders_guard
                        .iter()
                        .filter(|(_, f)| {
                            f.status == FolderStatus::Downloading && !f.pending_files.is_empty()
                        })
                        .map(|(id, f)| (id.clone(), f.owner_uid))
                        .collect()
                };

                for (folder_id, owner_uid) in candidates {
                    // 没有空闲槽位就别白跑一趟：refill 会把文件 drain 出来又原样退回，
                    // 每次都触发一次文件夹快照落盘
                    let has_free_slot = match manager.download_manager_for(owner_uid).await {
                        Some(dm) => {
                            let pool = dm.task_slot_pool();
                            pool.available_slots().await > 0
                                || pool.available_borrow_slots().await > 0
                        }
                        None => false,
                    };
                    if !has_free_slot {
                        continue;
                    }

                    if let Err(e) = manager.refill_tasks(&folder_id, 10).await {
                        warn!("待办补任务失败: folder={}, err={}", folder_id, e);
                    }
                }
            }
        });
    }

    /// 🔥 启动文件夹进度监听器
    ///
    /// 监听子任务进度变化通知，收到 group_id 后聚合子任务进度并发布 FolderEvent::Progress 事件
    /// 由子任务的节流器控制频率，无需额外节流
    /// 🔥 监听器内部按 `folder.owner_uid` 通过
    /// `self.download_manager_pool` 解析 manager，不再读单例
    /// `self.download_manager`（避免账号切换后用错 manager 查 group / 释放槽 / 补任务）
    fn start_folder_progress_listener(&self, mut rx: mpsc::UnboundedReceiver<String>) {
        let folders = self.folders.clone();
        let download_manager_pool = self.download_manager_pool.clone();
        let download_manager_legacy = self.download_manager.clone();
        let ws_manager = self.ws_manager.clone();
        let wal_dir = self.wal_dir.clone();
        let last_persist_at = self.last_persist_at.clone();

        tokio::spawn(async move {
            while let Some(folder_id) = rx.recv().await {
                // 🔥 先读 folder.owner_uid，再按 uid 解析 manager
                let folder_meta = {
                    let folders_guard = folders.read().await;
                    folders_guard.get(&folder_id).map(|f| {
                        (
                            f.total_files,
                            f.total_size,
                            f.status.clone(),
                            f.completed_count,
                            f.skipped_count,
                            f.skipped_size,
                            f.owner_uid.raw(),
                            f.owner_uid,
                        )
                    })
                };
                let (
                    total_files,
                    total_size,
                    status,
                    completed_files,
                    skipped_files,
                    skipped_size,
                    owner_uid_raw,
                    owner_uid,
                ) = match folder_meta {
                    Some(info) => info,
                    None => continue,
                };

                // 路由 manager（pool 注入则严格按 owner_uid；未注入退回 legacy 单例）
                let dm = {
                    let pool_guard = download_manager_pool.read().await;
                    if let Some(pool_arc) = pool_guard.as_ref() {
                        pool_arc.get(&owner_uid).map(|e| Arc::clone(e.value()))
                    } else {
                        // pool 未注入 → legacy 路径：读单例
                        drop(pool_guard);
                        download_manager_legacy.read().await.clone()
                    }
                };
                let dm = match dm {
                    Some(dm) => dm,
                    None => continue,
                };

                // 获取 WebSocket 管理器
                let ws = {
                    let guard = ws_manager.read().await;
                    guard.clone()
                };

                let ws = match ws {
                    Some(ws) => ws,
                    None => continue,
                };

                // 获取该文件夹的所有活跃子任务
                let tasks = dm.get_tasks_by_group(&folder_id).await;

                // 🔥 速度只统计仍在下载中的子任务
                let speed: u64 = tasks
                    .iter()
                    .filter(|t| t.status == TaskStatus::Downloading)
                    .map(|t| t.speed)
                    .sum();

                // 🔥 使用 compute_downloaded_size：completed_downloaded_size + active_sum
                // max() 保证即使完成通知和进度通知乱序也不会丢字节
                let downloaded_size = {
                    let mut folders_guard = folders.write().await;
                    if let Some(folder) = folders_guard.get_mut(&folder_id) {
                        // 🔥 排除已计入 completed_downloaded_size 的子任务再求 active_sum，
                        // 否则完成瞬间残留任务会被双重累加并被 max() 永久锁死（已下载翻倍 200%）。
                        let active_downloaded = folder.active_downloaded_excluding_counted(
                            tasks.iter().map(|t| (t.id.as_str(), t.downloaded_size)),
                        );
                        folder.compute_downloaded_size(active_downloaded)
                    } else {
                        continue;
                    }
                };

                // 发布文件夹进度事件
                ws.send_if_subscribed(
                    TaskEvent::Folder(FolderEvent::Progress {
                        folder_id: folder_id.clone(),
                        downloaded_size,
                        total_size,
                        completed_files,
                        skipped_files,
                        skipped_size,
                        total_files,
                        speed,
                        status: format!("{:?}", status).to_lowercase(),

                        owner_uid: Some(owner_uid_raw),
                    }),
                    None,
                );

                // 🔥 downloaded_size 只在本监听器里增长，必须在这里落盘。
                //
                //    补任务路径的落盘只在「从 pending_files 取走文件」时触发；一旦所有子任务
                //    都已创建（pending 为空），就再也不会 drain、也就再也不落盘，导致
                //    downloaded_size 在磁盘上永久停在最后一次建任务时的值 —— 强杀重启后
                //    进度回退到那个旧值（实测连续三次重启都回到同一个字节数）。
                //
                //    走节流版本：本监听器由子任务进度节流器驱动，频率很高。
                persist_folder_state(&folders, &wal_dir, &last_persist_at, &folder_id, true).await;
            }
        });
    }

    /// 启动任务完成监听器
    ///
    /// 当收到子任务完成通知时，立即从 pending_files 补充新任务
    /// 根据文件夹可用槽位数量（借调位+固定位）动态补充，充分利用槽位资源
    /// 🔥 监听器内部按 `folder.owner_uid` 通过
    /// `self.download_manager_pool` 解析 manager，不再读单例
    /// `self.download_manager`（避免账号切换后用错 manager 释放槽 / 补任务 /
    /// 把 owner=A 的新子任务塞进 B manager）
    fn start_task_completed_listener(&self, mut rx: mpsc::UnboundedReceiver<(String, String, u64, u64, bool)>) {
        let folders = self.folders.clone();
        let download_manager_pool = self.download_manager_pool.clone();
        let download_manager_legacy = self.download_manager.clone();
        let wal_dir = self.wal_dir.clone();
        let ws_manager = self.ws_manager.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();
        let last_persist_at = self.last_persist_at.clone();
        let app_config = self.app_config.clone();

        tokio::spawn(async move {
            while let Some((group_id, task_id, fs_id, file_size, is_success)) = rx.recv().await {
                // 🔥 先读 folder.owner_uid，再按 uid 解析 manager
                let folder_owner_uid_for_routing = {
                    let folders_guard = folders.read().await;
                    folders_guard.get(&group_id).map(|f| f.owner_uid)
                };
                let dm = match folder_owner_uid_for_routing {
                    Some(uid) => {
                        let pool_guard = download_manager_pool.read().await;
                        if let Some(pool_arc) = pool_guard.as_ref() {
                            pool_arc.get(&uid).map(|e| Arc::clone(e.value()))
                        } else {
                            drop(pool_guard);
                            download_manager_legacy.read().await.clone()
                        }
                    }
                    None => {
                        // 文件夹已不在内存（被删/恢复失败）→ 跳过这条通知
                        continue;
                    }
                };

                let dm = match dm {
                    Some(dm) => dm,
                    None => continue,
                };

                // 🔥 清理已完成子任务的槽位占用并实际释放相应资源
                // 🔥 关键修复：直接使用收到的 task_id，不再依赖 get_tasks_by_group
                //    因为任务完成后会立即从内存中移除，get_tasks_by_group 无法获取到已完成的任务
                //
                // 子任务持有文件夹槽位有两种互斥形态：
                //   A. 借调位：在 borrowed_subtask_map 中，完成后需归还到 task_slot_pool
                //   B. 文件夹固定位：fixed_slot_subtask == Some(task_id)，完成后只需清除映射
                //      （文件夹 fixed slot 本身仍归文件夹，task_slot_pool 中 owner=group_id 保持不变）
                {
                    let slot_pool = dm.task_slot_pool();

                    // 🔥 直接处理收到的 task_id
                    // 🔥 本次失败若还有重试额度，记下是第几次，锁释放后再重新入队
                    let mut retry_attempt: Option<u32> = None;
                    let (slot_id_to_release, released_fixed_slot) = {
                        let mut folders_guard = folders.write().await;

                        if let Some(folder) = folders_guard.get_mut(&group_id) {
                            // 🔥 检查这个**文件**是否已经被计数过（issue #156）
                            //
                            //    去重键必须是 fs_id 而不是 task_id：task_id 是每次建任务新生成的
                            //    UUID，同一个文件被重新建成子任务时就换了一个，按它去重等于没去重
                            //    （实测 1041 个文件计成 5337、18.4GB 计成 94.5GB）。
                            //
                            //    fs_id == 0 是异常数据（百度侧不会给 0）。真出现时若仍按 fs_id
                            //    去重，会把所有这类文件折叠成一个，completed_count 永远涨不上去、
                            //    文件夹到不了终态 —— 比重复计数更糟。这种情况退回按 task_id 去重，
                            //    宁可多计也不少计。
                            let already_counted = if fs_id != 0 {
                                folder.counted_fs_ids.contains(&fs_id)
                            } else {
                                folder.counted_task_ids.contains(&task_id)
                            };

                            // A. 处理借调位映射
                            let slot_id = if let Some(slot_id) = folder.borrowed_subtask_map.remove(&task_id) {
                                info!(
                                    "子任务 {} 完成，清理借调位映射: slot_id={}, folder={}",
                                    task_id, slot_id, group_id
                                );
                                // 🔥 从文件夹的借调位记录中移除
                                folder.borrowed_slot_ids.retain(|&id| id != slot_id);
                                Some(slot_id)
                            } else {
                                None
                            };

                            // B. 🔥 处理文件夹固定位占用
                            //    子任务若占用了文件夹 fixed slot，完成/失败时必须清除
                            //    fixed_slot_subtask，否则后续等待中的子任务会误以为该 fixed slot
                            //    仍被占用而不敢复用（导致整个文件夹剩余子任务堵住）。
                            let released_fixed_slot =
                                if folder.fixed_slot_subtask.as_deref() == Some(&task_id) {
                                    folder.fixed_slot_subtask = None;
                                    info!(
                                        "子任务 {} 完成，释放文件夹 {} 的固定槽位映射",
                                        task_id, group_id
                                    );
                                    true
                                } else {
                                    false
                                };

                            if is_success && already_counted {
                                // 🔥 同一个文件被第二次下完 —— 说明**确实有路径在重复建任务**。
                                //    计数在上面已经被 fs_id 挡住了（这正是 issue #156 的止血点），
                                //    但重复下载本身仍然白烧了一遍流量，值得留一条可 grep 的记录：
                                //    真出现了就照着 fs_id 去追是哪条路径把它重新排进队列的。
                                warn!(
                                    "文件夹 {} 的文件 fs_id={} 被重复下载完成 (task_id={}, size={})，\
                                     已跳过重复计数；这意味着有路径重复创建了该文件的子任务",
                                    group_id, fs_id, task_id, file_size
                                );
                                // 仍要登记 task_id：否则这个已完成任务会被 active_sum 再算一遍
                                folder.counted_task_ids.insert(task_id.clone());
                                folder.subtask_retry_counts.remove(&task_id);
                                // 🔥 自愈：能走到这个分支，说明该文件还赖在 pending_files 里
                                //    （否则补任务路径不会再挑中它）。就地剪掉把循环斩断——
                                //    也让 #156 之前写脏的旧快照在重启后恢复正常。
                                folder.mark_fs_id_completed(fs_id);
                                // 🔥 失败对账不能漏：同一个文件的上一个任务可能已经耗尽重试
                                //    额度被计进 failed_count，之后这个任务把它下成了。若不在这里
                                //    抵消，文件明明都在盘上，文件夹却会以"N 个文件下载失败"收场。
                                if clear_failed_for_success(folder, &task_id, fs_id) {
                                    folder.failed_count = folder.failed_count.saturating_sub(1);
                                }
                            } else if is_success {
                                // 🔥 成功且未计数：递增 completed_count
                                folder.counted_task_ids.insert(task_id.clone());
                                // 🔥 issue #156 续：登记完成 + 把文件剪出 pending_files，
                                //    两件事绑在 mark_fs_id_completed 里一起做（说明见该方法）。
                                //
                                //    起因：drop_pending_file_after_persist 全项目只有
                                //    start_task_internal 一个调用点，而 start_waiting_queue_monitor /
                                //    setup_waiting_queue_trigger 各自内联复制了一份启动逻辑、都没带上它。
                                //    经这两条路径起来的子任务下完后，文件永远留在 pending_files，
                                //    补任务循环下一轮又挑中它 → 全新 task_id、全新 WAL、整文件重下，
                                //    直到某次凑巧走了常规路径才断开。
                                //
                                //    收在这里的理由：每个下载成功的子任务都必经本分支（scheduler 的
                                //    task_completed 通道驱动），与它从哪条路径启动无关 —— 不必枚举
                                //    启动路径，将来新增第 N 条也不会复发。
                                folder.mark_fs_id_completed(fs_id);
                                folder.subtask_retry_counts.remove(&task_id);
                                folder.completed_count += 1;
                                folder.completed_downloaded_size += file_size;
                                // 如果之前失败过（retry→success），从 failed 中移除
                                //
                                // 🔥 必须按 fs_id 抵消：判死的那个任务可能是**同一文件的
                                //    另一个 task_id**（重复建任务时），只 remove 自己的 task_id
                                //    抵消不掉，文件夹会带着一笔幽灵失败进终态。
                                if clear_failed_for_success(folder, &task_id, fs_id) {
                                    folder.failed_count = folder.failed_count.saturating_sub(1);
                                    info!(
                                        "文件夹 {} 子任务重试成功 {}/{} (task_id={}, file_size={})",
                                        group_id, folder.completed_count, folder.total_files, task_id, file_size
                                    );
                                } else {
                                    info!(
                                        "文件夹 {} 已完成 {}/{} 个文件 (task_id={}, file_size={})",
                                        group_id, folder.completed_count, folder.total_files, task_id, file_size
                                    );
                                }
                            } else if !is_success && !already_counted {
                                // 🔥 失败：先看还有没有自动重试额度，耗尽才判死
                                let attempts = folder
                                    .subtask_retry_counts
                                    .entry(task_id.clone())
                                    .or_insert(0);
                                if *attempts < MAX_SUBTASK_AUTO_RETRIES {
                                    *attempts += 1;
                                    retry_attempt = Some(*attempts);
                                    info!(
                                        "文件夹 {} 子任务失败，将重新入队重试 ({}/{}, task_id={})",
                                        group_id, *attempts, MAX_SUBTASK_AUTO_RETRIES, task_id
                                    );
                                    // 刻意不计入 failed_count：否则 completed+skipped+failed
                                    // 会提前凑满 total_files，重试还没跑文件夹就被判成终态
                                } else if folder.failed_task_ids.insert(task_id.clone()) {
                                    folder.failed_count += 1;
                                    // 🔥 同时按文件身份记一份，供成功侧抵消（见 failed_fs_ids）
                                    if fs_id != 0 {
                                        folder.failed_fs_ids.insert(fs_id);
                                    }
                                    warn!(
                                        "文件夹 {} 子任务重试 {} 次仍失败，计为失败 (failed_count={}, task_id={})",
                                        group_id, MAX_SUBTASK_AUTO_RETRIES, folder.failed_count, task_id
                                    );
                                }
                            }

                            (slot_id, released_fixed_slot)
                        } else {
                            (None, false)
                        }
                    }; // 锁在此处自动释放

                    // 🔥 释放锁后，释放借调槽位（文件夹 fixed slot 不入 task_slot_pool 释放流程）
                    if let Some(slot_id) = slot_id_to_release {
                        slot_pool.release_borrowed_slot(&group_id, slot_id).await;
                        info!("子任务完成，已释放借调槽位 {} 到任务位池", slot_id);
                    }

                    // 🔥 释放完固定位/借调位后，立刻尝试拉起等待中的同文件夹子任务
                    //    released_fixed_slot=true 尤其重要：意味着此刻 fixed slot 可被下一个等待子任务复用
                    if released_fixed_slot {
                        debug!(
                            "文件夹 {} 的固定槽位已可复用，立即尝试调度等待子任务",
                            group_id
                        );
                    }

                    // 🔥 尝试启动等待队列中的任务
                    dm.try_start_waiting_tasks().await;

                    // 🔥 失败子任务：立即重新入队
                    //
                    //    刻意放在「释放槽位 + 拉起等待任务」之后：此刻空出的槽位已被排在
                    //    前面的子任务抢走，本任务 resume 后只能进等待队列 —— 即"回到队尾"。
                    //
                    //    用 resume_task 复用原任务（Failed → Pending）而非重建：已下载的
                    //    分片全部保留，大文件在 90% 处失败不会前功尽弃。
                    //
                    //    不做延迟重试：定时器只存在于内存，服务在退避窗口内重启会把重试
                    //    整个吞掉（任务永远停在 Failed 无人再管）。立即入队则任务以 Pending
                    //    落进 WAL，重启后能正常恢复。
                    if let Some(attempt) = retry_attempt {
                        // 失败通知有多条来源（槽位超时、启动失败、调度耗尽…），而暂停/取消
                        // 文件夹会连带终止在途子任务。不加这道闸门，一旦某条路径以
                        // is_success=false 上报，就会把刚被用户停掉的任务立刻拉起来。
                        let still_running = {
                            let guard = folders.read().await;
                            matches!(
                                guard.get(&group_id).map(|f| &f.status),
                                Some(FolderStatus::Downloading) | Some(FolderStatus::Scanning)
                            )
                        };

                        if !still_running {
                            info!(
                                "文件夹 {} 已不在下载中，跳过子任务 {} 的自动重试",
                                group_id, task_id
                            );
                        } else if let Err(e) = dm.resume_task(&task_id).await {
                            // 任务可能已被用户删除，或状态已不允许恢复
                            warn!(
                                "文件夹 {} 子任务重新入队失败 (task_id={}): {}",
                                group_id, task_id, e
                            );
                        } else {
                            info!(
                                "文件夹 {} 子任务已重新入队 ({}/{}, task_id={})",
                                group_id, attempt, MAX_SUBTASK_AUTO_RETRIES, task_id
                            );
                        }
                    }
                }

                // 🔥 计算文件夹可用的槽位数量（借调位 + 固定位）
                let available = {
                    let folders_guard = folders.read().await;
                    if let Some(folder) = folders_guard.get(&group_id) {
                        // 计算有多少借调位是空闲的（未分配给子任务）
                        let free_borrowed_slots = folder.borrowed_slot_ids.iter()
                            .filter(|&&slot_id| !folder.borrowed_subtask_map.values().any(|&s| s == slot_id))
                            .count();

                        // 固定位也可以用于一个子任务，所以总数 = 空闲借调位 + 1（如果有固定位）
                        // 固定槽位占 1，借调位按池子容量（-1）统计，避免总并发超出 max_slots
                        if folder.fixed_slot_id.is_some() {
                            free_borrowed_slots + 1
                        } else {
                            free_borrowed_slots
                        }
                    } else {
                        0
                    }
                };

                // 获取子任务列表统计活跃任务数
                // 🔥 注意：不再从 tasks 计算 completed_count，因为已完成的任务会从内存移除
                // 使用文件夹自己维护的 completed_count（在子任务完成时递增）
                let tasks = dm.get_tasks_by_group(&group_id).await;
                let active_count = tasks
                    .iter()
                    .filter(|t| {
                        t.status == TaskStatus::Downloading || t.status == TaskStatus::Pending
                    })
                    .count();

                // 🔥 终态检查必须在 available==0 之前，否则只有借调位的文件夹
                // 在最后一个子任务结束后 available 变成 0，会卡在 downloading
                {
                    let mut folders_guard = folders.write().await;
                    let folder = match folders_guard.get_mut(&group_id) {
                        Some(f) => f,
                        None => continue,
                    };

                    // 🔥 终态事件 owner_uid
                    let owner_uid_raw_for_folder: u64 = folder.owner_uid.raw();

                    // 检查状态：已终止的文件夹不需要继续处理
                    if folder.status == FolderStatus::Paused
                        || folder.status == FolderStatus::Cancelled
                        || folder.status == FolderStatus::Failed
                        || folder.status == FolderStatus::Completed
                    {
                        continue;
                    }

                    // 🔥 使用文件夹自己维护的 completed_count 检查是否全部完成
                    let completed_count = folder.completed_count;

                    // 检查是否全部完成
                    // 🔥 用 >= 而不是 ==（issue #156）：completed_count 一旦因为重复计数
                    //    冲过 total_files，严格相等就**永远不可能成立**，文件夹会卡在
                    //    downloading 无限补任务；失败终态分支又要求 failed_count > 0 也进不去。
                    //    去重本身在完成监听器里按 fs_id 兜住，这里再加一道不等式防御。
                    if folder.pending_files.is_empty()
                        && folder.scan_completed
                        && active_count == 0
                        && completed_count + folder.skipped_count >= folder.total_files
                    {
                        let old_status = format!("{:?}", folder.status).to_lowercase();
                        folder.mark_completed();
                        info!("文件夹 {} 全部下载完成！", folder.name);

                        // 更新持久化文件（保持 Completed 状态，等待定时归档任务处理）
                        let wal = wal_dir.read().await;
                        if let Some(ref wal_path) = *wal {
                            let persisted = FolderPersisted::from_folder(folder);
                            if let Err(e) = save_folder(wal_path, &persisted) {
                                error!("更新文件夹持久化状态失败: {}", e);
                            }
                        }

                        // 🔥 释放文件夹的所有槽位（完成后不再需要）
                        drop(folders_guard);
                        let slot_pool = dm.task_slot_pool();
                        slot_pool.release_all_slots(&group_id).await;
                        info!("文件夹 {} 完成，已释放所有槽位", group_id);

                        // 🔥 清理取消令牌，避免内存泄漏
                        cancellation_tokens.write().await.remove(&group_id);

                        // 🔥 释放槽位后，尝试启动等待队列中的任务
                        dm.try_start_waiting_tasks().await;

                        // 重新获取锁以清理文件夹槽位记录
                        let mut folders_guard_mut = folders.write().await;
                        if let Some(folder_mut) = folders_guard_mut.get_mut(&group_id) {
                            folder_mut.fixed_slot_id = None;
                            folder_mut.borrowed_slot_ids.clear();
                            folder_mut.borrowed_subtask_map.clear();
                        }
                        drop(folders_guard_mut);

                        // 🔥 发布状态变更事件
                        let ws = ws_manager.read().await;
                        if let Some(ref ws) = *ws {
                            ws.send_if_subscribed(
                                TaskEvent::Folder(FolderEvent::StatusChanged {
                                    folder_id: group_id.clone(),
                                    old_status,
                                    new_status: "completed".to_string(),

                                    owner_uid: Some(owner_uid_raw_for_folder),
                                }),
                                None,
                            );

                            // 🔥 发布文件夹完成事件
                            ws.send_if_subscribed(
                                TaskEvent::Folder(FolderEvent::Completed {
                                    folder_id: group_id.clone(),
                                    completed_at: chrono::Utc::now().timestamp_millis(),

                                    owner_uid: Some(owner_uid_raw_for_folder),
                                }),
                                None,
                            );
                        }
                        continue;
                    }

                    // 🔥 检查是否所有子任务都已终结（成功 + 失败 >= 总数）且有失败
                    if folder.pending_files.is_empty()
                        && folder.scan_completed
                        && active_count == 0
                        && folder.failed_count > 0
                        && (folder.completed_count + folder.skipped_count + folder.failed_count)
                        >= folder.total_files
                    {
                        let old_status = format!("{:?}", folder.status).to_lowercase();
                        let error_msg = format!("{} 个文件下载失败", folder.failed_count);
                        folder.mark_failed(error_msg.clone());
                        info!(
                            "文件夹 {} 下载完成但有 {} 个失败 (completed={}, failed={})",
                            folder.name, folder.failed_count, folder.completed_count, folder.failed_count
                        );

                        // 持久化
                        let wal = wal_dir.read().await;
                        if let Some(ref wal_path) = *wal {
                            let persisted = FolderPersisted::from_folder(folder);
                            if let Err(e) = save_folder(wal_path, &persisted) {
                                error!("更新文件夹持久化状态失败: {}", e);
                            }
                        }

                        // 释放槽位
                        drop(folders_guard);
                        let slot_pool = dm.task_slot_pool();
                        slot_pool.release_all_slots(&group_id).await;
                        info!("文件夹 {} 失败，已释放所有槽位", group_id);

                        cancellation_tokens.write().await.remove(&group_id);
                        dm.try_start_waiting_tasks().await;

                        // 清理槽位记录
                        let mut folders_guard_mut = folders.write().await;
                        if let Some(folder_mut) = folders_guard_mut.get_mut(&group_id) {
                            folder_mut.fixed_slot_id = None;
                            folder_mut.borrowed_slot_ids.clear();
                            folder_mut.borrowed_subtask_map.clear();
                        }
                        drop(folders_guard_mut);

                        // 发布事件
                        let ws = ws_manager.read().await;
                        if let Some(ref ws) = *ws {
                            ws.send_if_subscribed(
                                TaskEvent::Folder(FolderEvent::StatusChanged {
                                    folder_id: group_id.clone(),
                                    old_status,
                                    new_status: "failed".to_string(),

                                    owner_uid: Some(owner_uid_raw_for_folder),
                                }),
                                None,
                            );
                            ws.send_if_subscribed(
                                TaskEvent::Folder(FolderEvent::Failed {
                                    folder_id: group_id.clone(),
                                    error: error_msg,

                                    owner_uid: Some(owner_uid_raw_for_folder),
                                }),
                                None,
                            );
                        }
                        continue;
                    }
                }

                // 🔥 available==0 只阻止派发新子任务，不阻止终态检查
                if available == 0 {
                    continue;
                }

                // 🔥 关键修复：收集所有子任务已占用的槽位，用于防止重复分配
                let mut used_slot_ids: std::collections::HashSet<usize> = tasks
                    .iter()
                    .filter_map(|t| t.slot_id)
                    .collect();

                // 根据余量补充任务
                let files_to_create = {
                    let mut folders_guard = folders.write().await;
                    let folder = match folders_guard.get_mut(&group_id) {
                        Some(f) => f,
                        None => continue,
                    };

                    // 再次检查状态（可能在终态检查和此处之间被改变）
                    if folder.status != FolderStatus::Downloading {
                        continue;
                    }

                    // 检查是否还有待处理文件
                    if folder.pending_files.is_empty() {
                        continue;
                    }

                    // 🔥 只挑选文件，**不从 pending_files 摘除**
                    //
                    // 子任务在真正启动之前是不落盘的（`add_task` / `add_task_paused` 都只往
                    // 内存任务表里插，只有 `start_task_internal` 才注册持久化）。如果建任务时
                    // 就把文件从 pending_files 摘掉，那「已建任务、还没启动」的文件在重启后
                    // 两头都不存在 —— 实测一个 8 文件的文件夹重启后只剩 2 个任务、0 待处理。
                    //
                    // 所以摘除时机推迟到任务真正落盘那一刻（见
                    // `FolderDownloadManager::drop_pending_file_after_persist`）。在此之前
                    // 文件同时存在于 pending_files 和内存任务里，靠下面的 fs_id 去重
                    // 保证不会重复建任务。
                    let existing_fs_ids: std::collections::HashSet<u64> =
                        tasks.iter().map(|t| t.fs_id).collect();
                    // 🔥 issue #156 续：已下完的文件直接**挡掉**，不再只是打日志。
                    //
                    //    existing_fs_ids 只认"内存里还活着的任务"，而子任务一完成就被
                    //    scheduler 移出内存任务表，它兜不住已完成的文件；真正管这件事的是
                    //    「已下完的文件不在 pending 里」这个不变量，已在成功完成处收口。
                    //    这里再按持久化的 counted_fs_ids 拦一道：万一将来又有哪条路径把
                    //    已完成的文件塞回 pending，最坏结果是本轮少补一个任务（下一轮自愈），
                    //    而不是整文件重下一遍。fs_id==0 是异常数据，计数侧按 task_id 回退，
                    //    这里也必须放行，否则这类文件夹永远收不了工。
                    let files: Vec<_> = folder
                        .pending_files
                        .iter()
                        .filter(|f| !existing_fs_ids.contains(&f.fs_id))
                        .filter(|f| {
                            let already_done =
                                f.fs_id != 0 && folder.counted_fs_ids.contains(&f.fs_id);
                            if already_done {
                                warn!(
                                    "文件夹 {} 把已完成过的文件重新排进了补任务队列 (fs_id={}, path={})，已拦截",
                                    group_id, f.fs_id, f.relative_path
                                );
                            }
                            !already_done
                        })
                        .take(available)
                        .cloned()
                        .collect();
                    (files, folder.local_root.clone(), folder.remote_root.clone(), folder.owner_uid)
                };

                let (files, local_root, group_root, folder_owner_uid) = files_to_create;
                let total_files = files.len();
                let mut created_count = 0u64;
                // 🔥 命中"跳过"策略而未建任务的文件数（计入完成数，理由见 refill_tasks 同名变量）
                let mut skipped_count = 0u64;
                let mut skipped_bytes = 0u64;
                let mut skipped_list: Vec<crate::downloader::folder::SkippedFile> = Vec::new();
                // 🔥 中止时尚未处理的文件，循环后归还队列（文件已 drain 出来，丢了就永久丢失）
                let mut aborted_remainder = Vec::new();

                // 🔥 冲突策略：本批次内是常量，提前解析一次。
                //    这条路径以前**完全不做冲突判定**，导致用户选的"跳过"只对
                //    扫描完成时 refill 的首批任务生效，之后每个补建的任务都直接覆盖
                //    本地已存在的同名文件（issue #141）。
                let conflict_strategy =
                    resolve_folder_conflict_strategy(&folders, &app_config, &group_id).await;

                // 创建任务
                for file_to_create in files {
                    // ✅ 创建任务前再次检查状态，防止竞态条件
                    // 场景：取出文件后、创建任务前，pause_folder 可能已更新状态
                    {
                        let folders_guard = folders.read().await;
                        let should_abort = match folders_guard.get(&group_id) {
                            Some(folder) => {
                                let aborted = folder.status == FolderStatus::Paused
                                    || folder.status == FolderStatus::Cancelled
                                    || folder.status == FolderStatus::Failed;
                                if aborted {
                                    info!(
                                        "文件夹 {} 状态已变为 {:?}，放弃创建剩余 {} 个任务",
                                        group_id,
                                        folder.status,
                                        total_files - created_count as usize
                                    );
                                }
                                aborted
                            }
                            // 文件夹已被删除
                            None => true,
                        };

                        if should_abort {
                            drop(folders_guard);
                            // 文件本来就没从 pending_files 摘走，中止时无需归还
                            let _ = &file_to_create;
                            break;
                        }
                    }

                    let local_path = local_root.join(&file_to_create.relative_path);

                    // 🔥 应用冲突策略（与 refill_tasks 保持一致）
                    let final_local_path = {
                        use crate::uploader::conflict_resolver::ConflictResolver;
                        match ConflictResolver::resolve_download_conflict(
                            &local_path,
                            conflict_strategy,
                        ) {
                            Ok(crate::uploader::conflict::ConflictResolution::Proceed) => local_path,
                            Ok(crate::uploader::conflict::ConflictResolution::Skip) => {
                                info!("跳过下载（文件已存在）: {:?}", local_path);
                                // 🔥 跳过的文件必须从 pending_files 摘掉（issue #156）
                                //
                                // 补任务改成「只挑选不摘除、落盘时才摘」之后，Skip 分支既不建任务
                                // 也就永远等不到落盘回调，文件会一直赖在队列里：
                                //   - pending_files 永不为空 → 终态判定过不了，文件夹卡在 downloading
                                //   - 每轮补任务重复选中它 → skipped_count 无限增长
                                //   - refill_tasks 的 `loop { .. if skipped == 0 { break } }` 直接变成死循环
                                {
                                    let mut folders_guard = folders.write().await;
                                    if let Some(folder) = folders_guard.get_mut(&group_id) {
                                        folder
                                            .pending_files
                                            .retain(|p| p.fs_id != file_to_create.fs_id);
                                    }
                                }
                                skipped_count += 1;
                                skipped_bytes += file_to_create.size;
                                skipped_list.push(crate::downloader::folder::SkippedFile {
                                    relative_path: file_to_create.relative_path.clone(),
                                    size: file_to_create.size,
                                    skipped_at: chrono::Utc::now().timestamp(),
                                });
                                publish_skipped_event(
                                    &ws_manager,
                                    &group_id,
                                    &file_to_create.relative_path,
                                    folder_owner_uid,
                                )
                                    .await;
                                continue;
                            }
                            Ok(crate::uploader::conflict::ConflictResolution::UseNewPath(
                                   new_path,
                               )) => {
                                info!("自动重命名下载路径: {:?} -> {}", local_path, new_path);
                                PathBuf::from(new_path)
                            }
                            Err(e) => {
                                warn!("冲突解决失败: {}, 使用原路径", e);
                                local_path
                            }
                        }
                    };

                    // 确保目录存在
                    if let Some(parent) = final_local_path.parent() {
                        if let Err(e) = tokio::fs::create_dir_all(parent).await {
                            error!("创建目录失败: {:?}, 错误: {}", parent, e);
                            continue;
                        }
                    }

                    // 拿不到槽位时要把文件原样退回 pending_files，这里先留一份

                    let mut task = DownloadTask::new_with_group(
                        file_to_create.fs_id,
                        file_to_create.remote_path.clone(),
                        final_local_path,
                        file_to_create.size,
                        group_id.clone(),
                        group_root.clone(),
                        file_to_create.relative_path,
                        folder_owner_uid,
                    );

                    // 🔥 槽位分配顺序：**文件夹自己的固定位优先，借调位兜底**
                    //
                    // 历史实现是反过来的（先借调位、固定位兜底），后果是文件夹把自己的
                    // 固定位空着、却占着从别人那借来的槽位：实测一个只含 1 个文件的文件夹
                    // 会持有 5 个槽位（固定位 0 空闲 + 借调位 1 在用 + 借调位 2/3/4 空闲），
                    // 同账号第二个文件夹一个槽位都拿不到，全部子任务饿死在等待队列。
                    //
                    // 先用自己的位、借来的位只在自己的不够时才动，能还回去的自然就多了。
                    //
                    // 固定位占用是"直持有语义"：必须写成 `uses_folder_fixed_slot=true,
                    // slot_id=None`，并同步登记 `folder.fixed_slot_subtask = Some(task.id)`。
                    // 否则：
                    // (a) scheduler 完成时会误走 `release_fixed_slot(task_id)`，但 pool 里 owner=group_id，清不掉
                    // (b) `fixed_slot_subtask` 仍为 None，后续 `try_allocate_fixed_slot_for_subtask` 会把同一 fixed slot 再次分配给别的等待子任务
                    let fixed_slot_claim: Option<usize> = {
                        let folders_guard = folders.read().await;
                        match folders_guard.get(&group_id) {
                            Some(folder) => {
                                match folder.fixed_slot_id {
                                    Some(fixed_slot_id)
                                    if !used_slot_ids.contains(&fixed_slot_id)
                                        && folder.fixed_slot_subtask.is_none() =>
                                        {
                                            Some(fixed_slot_id)
                                        }
                                    _ => None,
                                }
                            }
                            None => {
                                // 文件夹不存在，跳过当前文件
                                continue;
                            }
                        }
                    };

                    if let Some(fixed_slot_id) = fixed_slot_claim {
                        // 1. 写入任务侧的"文件夹固定位直持有"语义
                        task.slot_id = None;
                        task.is_borrowed_slot = false;
                        task.uses_folder_fixed_slot = true;
                        // 2. 记入本轮 used_slot_ids，防止同一轮内再次分配
                        used_slot_ids.insert(fixed_slot_id);
                        // 3. 同步登记到 folder.fixed_slot_subtask，防止跨轮/并发重复分配
                        {
                            let mut folders_mut = folders.write().await;
                            if let Some(folder_mut) = folders_mut.get_mut(&group_id) {
                                folder_mut.fixed_slot_subtask = Some(task.id.clone());
                            }
                        }
                        info!(
                            "子任务 {} 使用文件夹 {} 的固定位 (直持有语义，slot_id={})",
                            task.id, group_id, fixed_slot_id
                        );
                    } else {
                        // 固定位已被别的子任务占着，退而使用借调位
                        let borrowed_slot_assigned = {
                            let folders_guard = folders.read().await;
                            if let Some(folder) = folders_guard.get(&group_id) {
                                // 检查是否有空闲的借调位（未被映射到子任务，且不在已占用槽位中）
                                let mut assigned = false;
                                for &slot_id in &folder.borrowed_slot_ids {
                                    // 🔥 关键修复：同时检查 borrowed_subtask_map 和 used_slot_ids
                                    let in_map = folder.borrowed_subtask_map.values().any(|&s| s == slot_id);
                                    let in_use = used_slot_ids.contains(&slot_id);
                                    if !in_map && !in_use {
                                        // 找到一个空闲的借调位，分配给此任务
                                        task.slot_id = Some(slot_id);
                                        task.is_borrowed_slot = true;
                                        drop(folders_guard);

                                        // 登记借调位映射
                                        {
                                            let mut folders_mut = folders.write().await;
                                            if let Some(folder_mut) = folders_mut.get_mut(&group_id) {
                                                folder_mut.borrowed_subtask_map.insert(task.id.clone(), slot_id);
                                            }
                                        }
                                        // 🔥 关键修复：将分配的槽位加入已使用集合
                                        used_slot_ids.insert(slot_id);
                                        info!("子任务 {} 分配借调位: slot_id={}", task.id, slot_id);
                                        assigned = true;
                                        break;
                                    }
                                }
                                assigned
                            } else {
                                false
                            }
                        };

                        if !borrowed_slot_assigned {
                            // 拿不到槽位也照常建任务（进等待队列），保持
                            // 「最多 target_count 个子任务」的原有阈值 —— 列表里看得见。
                            //
                            // 这类任务在真正启动前不落盘，但对应文件还留在 pending_files
                            // 里（摘除推迟到落盘那一刻），重启后会被重新建出来，不会丢。
                            info!(
                                "子任务 {} 无空闲槽位，创建任务但不分配槽位（将进入等待队列）",
                                task.id
                            );
                            // task.slot_id 保持 None
                        }
                    }

                    // 启动任务
                    if let Err(e) = dm.add_task(task).await {
                        warn!("补充任务失败: {}", e);
                    } else {
                        created_count += 1;
                    }
                }

                // 更新已创建/跳过计数，并归还中止时未处理的文件
                let returned_count = {
                    let mut folders_guard = folders.write().await;
                    match folders_guard.get_mut(&group_id) {
                        Some(folder) => {
                            folder.created_count += created_count;
                            folder.skipped_count += skipped_count;
                            folder.skipped_size += skipped_bytes;
                            folder.skipped_entries.append(&mut skipped_list);

                            let returned = aborted_remainder.len();
                            if returned > 0 {
                                // 放回队首，保持扫描时按相对路径排好的下载顺序
                                aborted_remainder.append(&mut folder.pending_files);
                                folder.pending_files = aborted_remainder;
                            }
                            returned
                        }
                        // 文件夹已被删除，归还无意义
                        None => 0,
                    }
                };

                if created_count > 0 || skipped_count > 0 || returned_count > 0 {
                    info!(
                        "已补充{}个任务到文件夹 {} (跳过 {}, 归还 {}, 可用槽位: {})",
                        created_count, group_id, skipped_count, returned_count, available
                    );

                    // 🔥 本路径已经从 pending_files 取走了文件，必须落盘，否则重启后队列
                    //    退回旧状态并重复下载（issue #141）。这是下载过程中消费 pending
                    //    的主路径——每完成一个子任务就补建一个，此前完全没有落盘。
                    //
                    //    归还路径无条件落盘：中止通常由 pause_folder 触发，而它在这里归还
                    //    文件**之前**就已落过盘，那份快照里没有这批在途文件，被节流挡掉
                    //    就等于丢失。
                    persist_folder_state(
                        &folders,
                        &wal_dir,
                        &last_persist_at,
                        &group_id,
                        returned_count == 0,
                    )
                        .await;
                }
            }
        });
    }

    /// 设置网盘客户端
    pub async fn set_netdisk_client(&self, client: Arc<NetdiskClient>) {
        let mut nc = self.netdisk_client.write().await;
        *nc = Some(client);
    }

    /// 更新下载目录
    ///
    /// 当配置中的 download_dir 改变时调用此方法
    /// 注意：只影响新创建的文件夹下载任务，已存在的任务不受影响
    pub async fn update_download_dir(&self, new_dir: PathBuf) {
        let mut dir = self.download_dir.write().await;
        if *dir != new_dir {
            info!("更新文件夹下载目录: {:?} -> {:?}", *dir, new_dir);
            *dir = new_dir;
        }
    }

    /// 创建文件夹下载任务
    ///
    /// 多账号：owner_uid 参数由调用方（一般是 handler 从 active_uid
    /// 读取）传入，然后在 internal 里写入 folder.owner_uid。
    pub async fn create_folder_download(
        &self,
        remote_path: String,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        self.create_folder_download_with_name(remote_path, None, None, owner_uid).await
    }

    /// 创建文件夹下载任务（指定下载目录 + 备份配置归属）
    ///
    /// `backup_config_id = Some("share-sync:{订阅id}")` 时，该文件夹下载为分享同步
    /// 内部任务：从「下载管理」隐藏，并作为分享同步子任务被收集。
    pub async fn create_folder_download_with_dir_backup(
        &self,
        remote_path: String,
        target_dir: &std::path::Path,
        original_name: Option<String>,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
        backup_config_id: Option<String>,
    ) -> Result<String> {
        self.create_folder_download_with_dir_inner(
            remote_path,
            target_dir,
            original_name,
            conflict_strategy,
            owner_uid,
            backup_config_id,
        )
            .await
    }

    /// 创建文件夹下载任务（支持指定原始文件夹名）
    ///
    /// 如果传入 original_name，则使用该名称作为本地文件夹名（用于加密文件夹还原）
    /// 如果没有传入，会自动尝试从映射表还原加密的文件夹名
    pub async fn create_folder_download_with_name(
        &self,
        remote_path: String,
        original_name: Option<String>,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        // 获取远程路径中的文件夹名
        let encrypted_folder_name = remote_path
            .trim_end_matches('/')
            .split('/')
            .next_back()
            .unwrap_or("download")
            .to_string();

        // 获取父路径（用于查询映射）
        let parent_path = remote_path
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();

        // 计算本地路径（优先使用传入的原始名称，其次尝试还原，最后使用远程名称）
        let folder_name = if let Some(name) = original_name {
            name
        } else {
            // 🔥 尝试从映射表还原加密的文件夹名
            match self.restore_folder_name(&encrypted_folder_name, &parent_path).await {
                Some(restored) => {
                    info!("还原加密文件夹名: {} -> {}", encrypted_folder_name, restored);
                    restored
                }
                None => encrypted_folder_name
            }
        };

        let download_dir = self.download_dir.read().await;
        let local_root = download_dir.join(&folder_name);
        drop(download_dir);

        self.create_folder_download_internal(
            remote_path,
            local_root,
            conflict_strategy,
            owner_uid,
            None,
        )
            .await
    }

    /// 创建文件夹下载任务（指定下载目录）
    ///
    /// 用于批量下载时支持自定义下载目录
    ///
    /// # 参数
    /// * `remote_path` - 远程路径
    /// * `target_dir` - 目标下载目录
    /// * `original_name` - 原始文件夹名（如果是加密文件夹，传入还原后的名称）
    pub async fn create_folder_download_with_dir(
        &self,
        remote_path: String,
        target_dir: &std::path::Path,
        original_name: Option<String>,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        self.create_folder_download_with_dir_inner(
            remote_path,
            target_dir,
            original_name,
            conflict_strategy,
            owner_uid,
            None,
        )
            .await
    }

    async fn create_folder_download_with_dir_inner(
        &self,
        remote_path: String,
        target_dir: &std::path::Path,
        original_name: Option<String>,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
        backup_config_id: Option<String>,
    ) -> Result<String> {
        // 获取远程路径中的文件夹名
        let encrypted_folder_name = remote_path
            .trim_end_matches('/')
            .split('/')
            .next_back()
            .unwrap_or("download")
            .to_string();

        // 获取父路径（用于查询映射）
        let parent_path = remote_path
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();

        // 计算本地路径（优先使用传入的原始名称，其次尝试还原，最后使用远程名称）
        let folder_name = if let Some(name) = original_name {
            name
        } else {
            // 🔥 尝试从映射表还原加密的文件夹名
            match self.restore_folder_name(&encrypted_folder_name, &parent_path).await {
                Some(restored) => {
                    info!("还原加密文件夹名: {} -> {}", encrypted_folder_name, restored);
                    restored
                }
                None => encrypted_folder_name
            }
        };

        let local_root = target_dir.join(&folder_name);

        self.create_folder_download_internal(
            remote_path,
            local_root,
            conflict_strategy,
            owner_uid,
            backup_config_id,
        )
            .await
    }

    /// 内部方法：创建文件夹下载任务
    ///
    /// 🔥 集成任务位借调机制：
    /// 1. 为文件夹分配一个固定任务位
    /// 2. 尝试借调空闲槽位给子任务并行
    async fn create_folder_download_internal(
        &self,
        remote_path: String,
        local_root: PathBuf,
        conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
        owner_uid: crate::auth::Uid,
        backup_config_id: Option<String>,
    ) -> Result<String> {
        let mut folder = FolderDownload::new(remote_path.clone(), local_root);
        let folder_id = folder.id.clone();

        // 🔥 多账号：设置 folder 归属 UID（与 ClientPool/per-uid downloader 路由对齐）
        folder.owner_uid = owner_uid;

        // 🔥 分享同步内部隐藏文件夹下载：归属 backup_config_id
        folder.backup_config_id = backup_config_id;

        // 🔥 设置冲突策略
        folder.conflict_strategy = conflict_strategy;

        // 🔥 按 folder.owner_uid 解析 download_manager（per-uid）
        let dm_for_owner: Option<Arc<DownloadManager>> = self.download_manager_for(owner_uid).await;

        // 🔥 尝试为文件夹分配固定任务位（优先级见 `folder_slot_priority`）
        let slot_priority = folder_slot_priority(folder.backup_config_id.as_deref());
        let (mut fixed_slot_id, mut preempted_task_id) = {
            if let Some(ref dm) = dm_for_owner {
                let slot_pool = dm.task_slot_pool();
                // 优先级见 `folder_slot_priority`：用户手动发起 → Normal（可抢占备份
                // 任务）；自动备份 / 分享同步内部 → Backup（只用空闲槽、不抢占）
                if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                    &folder_id, true, slot_priority
                ).await {
                    (Some(slot_id), preempted)
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        };

        // 🔥 处理被抢占的备份任务
        if let Some(preempted_id) = preempted_task_id.take() {
            info!("文件夹 {} 抢占了槽位持有者 {}", folder_id, preempted_id);
            if let Some(ref dm) = dm_for_owner {
                // 被抢占者可能是单文件任务，也可能是另一个文件夹 —— 统一分发
                dm.handle_preempted_slot_owner(&preempted_id).await;
            }
        }

        // 🔥 如果没有空闲槽位，尝试从同一账号的其他文件夹回收借调位
        // 这确保了多个文件夹任务之间的公平性：每个文件夹至少能获得一个固定位
        // reclaim 必须按 owner_uid 过滤，避免跨账号误回收
        //
        // 🔥 后台文件夹（Backup 优先级）不参与回收：回收会削减其它文件夹已经拿到的
        // 并行度，等同于「抢别人的资源」，与 Backup「只用空闲槽」的语义冲突。
        // 拿不到就等空槽，由等待队列按 FIFO 唤醒。
        if fixed_slot_id.is_none() && slot_priority == crate::task_slot_pool::TaskPriority::Normal {
            info!("文件夹 {} 无空闲槽位，尝试回收同账号其他文件夹的借调位", folder_id);
            if let Some(reclaimed_slot_id) = self
                .reclaim_borrowed_slot_for_owner(owner_uid)
                .await
            {
                // 回收成功，重新分配固定位
                if let Some(ref dm) = dm_for_owner {
                    let slot_pool = dm.task_slot_pool();
                    if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                        &folder_id, true, slot_priority
                    ).await {
                        fixed_slot_id = Some(slot_id);
                        info!(
                            "文件夹 {} 通过回收借调位获得固定任务位: slot_id={} (回收的槽位={})",
                            folder_id, slot_id, reclaimed_slot_id
                        );
                        // 处理可能被抢占的备份任务
                        if let Some(preempted_id) = preempted {
                            info!("文件夹 {} 抢占了槽位持有者 {}", folder_id, preempted_id);
                            dm.handle_preempted_slot_owner(&preempted_id).await;
                        }
                    }
                }
            }
        }

        if let Some(slot_id) = fixed_slot_id {
            folder.fixed_slot_id = Some(slot_id);
            info!("文件夹 {} 获得固定任务位: slot_id={}", folder_id, slot_id);
        } else {
            warn!("文件夹 {} 无法获得固定任务位，将在有空位时重试", folder_id);
        }

        // 🔥 按池子容量计算可借调槽位（固定槽位保留 1 个）
        // - Normal（用户手动）：空闲槽不够时可抢占备份任务的槽位
        // - Backup（自动备份 / 分享同步）：只借空闲槽，绝不抢占 —— 否则会绕过固定位
        //   的优先级限制，从借调这条路把别人的槽抢走
        let is_normal_priority = slot_priority == crate::task_slot_pool::TaskPriority::Normal;
        let (borrowed_slot_ids, preempted_backup_tasks) = {
            if let Some(ref dm) = dm_for_owner {
                let slot_pool = dm.task_slot_pool();
                let available = if is_normal_priority {
                    slot_pool.available_borrow_slots().await
                } else {
                    slot_pool.available_slots().await
                };
                let fixed_reserved = if fixed_slot_id.is_some() { 1 } else { 0 };
                let max_borrowable = slot_pool.max_slots().saturating_sub(fixed_reserved);
                let to_borrow = available.min(max_borrowable);
                if to_borrow == 0 {
                    (Vec::new(), Vec::new())
                } else if is_normal_priority {
                    slot_pool.allocate_borrowed_slots(&folder_id, to_borrow).await
                } else {
                    (
                        slot_pool
                            .allocate_borrowed_slots_no_preempt(&folder_id, to_borrow)
                            .await,
                        Vec::new(),
                    )
                }
            } else {
                (Vec::new(), Vec::new())
            }
        };

        // 🔥 处理被抢占的备份任务（暂停并加入等待队列）
        if !preempted_backup_tasks.is_empty() {
            info!(
                "文件夹 {} 借调槽位时抢占了 {} 个备份任务: {:?}",
                folder_id,
                preempted_backup_tasks.len(),
                preempted_backup_tasks
            );
            if let Some(ref dm) = dm_for_owner {
                for preempted_id in &preempted_backup_tasks {
                    // 暂停被抢占的备份任务
                    dm.handle_preempted_slot_owner(preempted_id).await;
                }
            }
        }

        if !borrowed_slot_ids.is_empty() {
            folder.borrowed_slot_ids = borrowed_slot_ids.clone();
            info!(
                "文件夹 {} 借调 {} 个任务位: {:?}",
                folder_id,
                borrowed_slot_ids.len(),
                borrowed_slot_ids
            );
        }

        // 保存到列表
        {
            let mut folders = self.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        // 持久化文件夹状态
        self.persist_folder(&folder_id).await;

        info!("创建文件夹下载任务: {}, ID: {}", remote_path, folder_id);

        // 🔥 发布文件夹创建事件
        {
            let folders = self.folders.read().await;
            if let Some(folder) = folders.get(&folder_id) {
                // 🔥 文件夹 Created event 携带 owner_uid
                //
                // 之前固定 None 与文档"任务事件都带 owner_uid"不一致，多账号过滤 /
                // AccountBadge 在 WS 实时路径上不可靠（前端只能等 GET /downloads/all
                // 全量拉刷新才能看到归属）。文件夹本身已有 owner_uid 字段（来自
                // create_folder_download_with_dir 接收的 owner_uid 参数），直接透传。
                // 🔥 内部隐藏文件夹（分享同步）不广播 folder 事件，避免泄漏进
                // 「下载管理」（DownloadsView 依 Created 事件动态插入文件夹行）。
                // 其进度改由分享同步 item_progress（collect_share_sync_subtasks）呈现。
                if folder.backup_config_id.is_none() {
                    self.publish_event(FolderEvent::Created {
                        folder_id: folder_id.clone(),
                        name: folder.name.clone(),
                        remote_root: folder.remote_root.clone(),
                        local_root: folder.local_root.to_string_lossy().to_string(),

                        owner_uid: Some(folder.owner_uid.raw()),
                    })
                        .await;
                }
            }
        }

        // 异步开始扫描并创建任务
        let self_clone = Self {
            folders: self.folders.clone(),
            cancellation_tokens: self.cancellation_tokens.clone(),
            download_manager: self.download_manager.clone(),
            netdisk_client: self.netdisk_client.clone(),
            download_dir: self.download_dir.clone(),
            wal_dir: self.wal_dir.clone(),
            ws_manager: self.ws_manager.clone(),
            folder_progress_tx: self.folder_progress_tx.clone(),
            task_completed_tx: self.task_completed_tx.clone(),
            persistence_manager: self.persistence_manager.clone(),
            backup_record_manager: self.backup_record_manager.clone(),
            client_pool: self.client_pool.clone(),
            download_manager_pool: self.download_manager_pool.clone(),
            last_persist_at: self.last_persist_at.clone(),
            app_config: self.app_config.clone(),
        };
        let folder_id_clone = folder_id.clone();

        tokio::spawn(async move {
            if let Err(e) = self_clone
                .scan_folder_and_create_tasks(&folder_id_clone)
                .await
            {
                error!("扫描文件夹失败: {:?}", e);
                let error_msg = e.to_string();
                // 🔥 mark_failed 同时取出 owner_uid
                let owner_uid_raw_opt: Option<u64> = {
                    let mut folders = self_clone.folders.write().await;
                    if let Some(folder) = folders.get_mut(&folder_id_clone) {
                        folder.mark_failed(error_msg.clone());
                        Some(folder.owner_uid.raw())
                    } else {
                        None
                    }
                };
                // 清理取消令牌
                self_clone
                    .cancellation_tokens
                    .write()
                    .await
                    .remove(&folder_id_clone);

                // 🔥 发布文件夹失败事件
                self_clone
                    .publish_event(FolderEvent::Failed {
                        folder_id: folder_id_clone,
                        error: error_msg,

                        owner_uid: owner_uid_raw_opt,
                    })
                    .await;
            }
        });

        Ok(folder_id)
    }

    /// 递归扫描文件夹并创建任务（边扫描边创建）
    async fn scan_folder_and_create_tasks(&self, folder_id: &str) -> Result<()> {
        let (remote_root, local_root, owner_uid) = {
            let folders = self.folders.read().await;
            let folder = folders
                .get(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;
            (
                folder.remote_root.clone(),
                folder.local_root.clone(),
                folder.owner_uid,
            )
        };

        // 🔥 按 folder.owner_uid 路由网盘客户端
        //
        // 不能直接读 `self.netdisk_client`：那是 legacy 全局单例，只在启动时注入一次，
        // 携带的是**启动时**的活跃账号凭证。切换账号后它不会更新（切号只重新注入了
        // DownloadManager 池和事件 sender），扫描就会拿旧账号的凭证去列新账号的目录，
        // 稳定返回 `API error -9`（文件不存在），并且重试多少次都救不回来。
        //
        // `client_for` 优先从 per-uid 池取，池未注入时才回退到 legacy 单例。
        let client = self
            .client_for(owner_uid)
            .await
            .ok_or_else(|| anyhow!("网盘客户端未初始化（owner_uid={}）", owner_uid.raw()))?;

        // 创建取消令牌
        let cancel_token = CancellationToken::new();
        {
            let mut tokens = self.cancellation_tokens.write().await;
            tokens.insert(folder_id.to_string(), cancel_token.clone());
        }

        // 递归扫描并收集文件信息到 pending_files
        self.scan_recursive(
            folder_id,
            &client,
            &cancel_token,
            &remote_root,
            &remote_root,
            &local_root,
        )
            .await?;

        // 扫描完成，更新状态并对 pending_files 排序
        let should_publish_status_changed = {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.scan_completed = true;

                // 🔥 关键修复：对 pending_files 按相对路径排序，确保子任务顺序一致
                folder.pending_files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

                let should_change = folder.status == FolderStatus::Scanning;
                if should_change {
                    folder.mark_downloading();
                }
                info!(
                    "文件夹扫描完成: {} 个文件, 总大小: {} bytes, pending队列: {} (已按路径排序)",
                    folder.total_files,
                    folder.total_size,
                    folder.pending_files.len()
                );
                should_change
            } else {
                false
            }
        };

        // 清理取消令牌
        {
            let mut tokens = self.cancellation_tokens.write().await;
            tokens.remove(folder_id);
        }

        // 🔥 重命名加密文件夹并更新路径（在创建任务前）
        if let Err(e) = self.rename_encrypted_folders_and_update_paths(folder_id).await {
            warn!("重命名加密文件夹失败: {}", e);
        }

        // 🔥 补任务前对账：本方法也会被"重启时扫描未完成的文件夹"复用（恢复后重新扫描），
        //    此时队列里可能混有上次运行已经下载完成的文件，直接建任务会重复下载。
        //    全新文件夹没有任何子任务/历史记录，对账会立即返回，无额外开销。
        self.reconcile_pending_files(folder_id).await;

        // 扫描完成后，立即创建前10个任务
        if let Err(e) = self.refill_tasks(folder_id, 10).await {
            error!("创建初始任务失败: {}", e);
        }

        // 🔥 关键修复：先持久化，再发送消息
        // 确保前端收到消息时，状态已经保存到磁盘
        self.persist_folder(folder_id).await;

        // 🔥 取出 owner_uid 用于事件
        let owner_uid_raw_opt: Option<u64> = {
            let folders = self.folders.read().await;
            folders.get(folder_id).map(|f| f.owner_uid.raw())
        };

        // 🔥 发送状态变更事件（在持久化之后）
        if should_publish_status_changed {
            self.publish_event(FolderEvent::StatusChanged {
                folder_id: folder_id.to_string(),
                old_status: "scanning".to_string(),
                new_status: "downloading".to_string(),

                owner_uid: owner_uid_raw_opt,
            })
                .await;
        }

        // 🔥 发布扫描完成事件（在锁外发布）
        let scan_event = {
            let folders = self.folders.read().await;
            folders.get(folder_id).map(|folder| FolderEvent::ScanCompleted {
                folder_id: folder_id.to_string(),
                total_files: folder.total_files,
                total_size: folder.total_size,

                owner_uid: Some(folder.owner_uid.raw()),
            })
        };
        if let Some(event) = scan_event {
            self.publish_event(event).await;
        }

        Ok(())
    }

    /// 递归扫描目录（只收集文件信息到 pending_files，不创建任务）
    #[async_recursion::async_recursion]
    async fn scan_recursive(
        &self,
        folder_id: &str,
        client: &NetdiskClient,
        cancel_token: &CancellationToken,
        root_path: &str,
        current_path: &str,
        local_root: &PathBuf,
    ) -> Result<()> {
        // 检查是否已取消
        if cancel_token.is_cancelled() {
            info!("扫描任务被取消");
            return Ok(());
        }

        let mut page = 1;
        let page_size = 100;

        loop {
            // 每页之前检查取消
            if cancel_token.is_cancelled() {
                info!("扫描任务被取消");
                return Ok(());
            }

            // 更新扫描进度
            {
                let mut folders = self.folders.write().await;
                if let Some(folder) = folders.get_mut(folder_id) {
                    folder.scan_progress = Some(current_path.to_string());
                }
            }

            // 获取文件列表
            //
            // 🔥 刚转存到网盘的目录可能因百度侧最终一致性尚未就绪而短暂返回
            // `API error -9`（文件不存在）等瞬时错误（分享同步 tree 模式整目录转存后
            // 立即自动下载会撞上）。直接 `?` 会让整个文件夹下载判失败、子文件全标
            // 「下载失败」。这里对扫描列表做有界重试（最多 4 次，递增 backoff），
            // 真失败仍会在重试耗尽后向上抛出。
            let file_list = {
                const SCAN_LIST_MAX_ATTEMPTS: u32 = 4;
                let mut attempt = 0u32;
                loop {
                    match client.get_file_list(current_path, page, page_size).await {
                        Ok(list) => break list,
                        Err(e) => {
                            attempt += 1;
                            if attempt >= SCAN_LIST_MAX_ATTEMPTS || cancel_token.is_cancelled() {
                                return Err(e);
                            }
                            let backoff = tokio::time::Duration::from_millis(800 * attempt as u64);
                            warn!(
                                "扫描文件夹列表失败(第 {}/{} 次), {:?} 后重试: dir={}, page={}, err={}",
                                attempt, SCAN_LIST_MAX_ATTEMPTS, backoff, current_path, page, e
                            );
                            tokio::time::sleep(backoff).await;
                        }
                    }
                }
            };

            let mut batch_files = Vec::new();
            let mut batch_size = 0u64;

            for item in &file_list.list {
                // 检查取消
                if cancel_token.is_cancelled() {
                    return Ok(());
                }

                if item.isdir == 1 {
                    // 🔥 检查是否是加密文件夹，收集映射关系
                    let folder_name = item.path
                        .rsplit('/')
                        .next()
                        .unwrap_or("");

                    if crate::encryption::service::EncryptionService::is_encrypted_folder_name(folder_name) {
                        // 计算加密文件夹的相对路径
                        let encrypted_relative = item.path
                            .strip_prefix(root_path)
                            .unwrap_or(&item.path)
                            .trim_start_matches('/')
                            .to_string();

                        // 获取解密后的相对路径
                        let decrypted_relative = self
                            .restore_encrypted_path(&encrypted_relative, root_path)
                            .await;

                        // 如果路径不同，说明有加密文件夹需要重命名
                        if encrypted_relative != decrypted_relative {
                            let mut folders = self.folders.write().await;
                            if let Some(folder) = folders.get_mut(folder_id) {
                                folder.encrypted_folder_mappings.insert(
                                    encrypted_relative.clone(),
                                    decrypted_relative.clone()
                                );
                                info!(
                                    "收集加密文件夹映射: {} -> {}",
                                    encrypted_relative, decrypted_relative
                                );
                            }
                        }
                    }

                    // 递归处理子目录
                    self.scan_recursive(
                        folder_id,
                        client,
                        cancel_token,
                        root_path,
                        &item.path,
                        local_root,
                    )
                        .await?;
                } else {
                    // 计算相对路径
                    let relative_path = item
                        .path
                        .strip_prefix(root_path)
                        .unwrap_or(&item.path)
                        .trim_start_matches('/')
                        .to_string();

                    // 🔥 还原加密文件夹名
                    let relative_path = self
                        .restore_encrypted_path(&relative_path, root_path)
                        .await;

                    // 收集文件信息
                    let pending_file = PendingFile {
                        fs_id: item.fs_id,
                        filename: item.server_filename.clone(),
                        remote_path: item.path.clone(),
                        relative_path,
                        size: item.size,
                    };

                    batch_files.push(pending_file);
                    batch_size += item.size;
                }
            }

            // 批量添加到 pending_files
            if !batch_files.is_empty() {
                let batch_count = batch_files.len();

                {
                    let mut folders = self.folders.write().await;
                    if let Some(folder) = folders.get_mut(folder_id) {
                        folder.pending_files.extend(batch_files);
                        folder.total_files += batch_count as u64;
                        folder.total_size += batch_size;
                    }
                }

                info!(
                    "扫描进度: 发现 {} 个文件，总大小 {} bytes (路径: {})",
                    batch_count, batch_size, current_path
                );
            }

            // 检查是否还有下一页
            if file_list.list.len() < page_size as usize {
                break;
            }
            page += 1;
        }

        Ok(())
    }

    /// 获取所有文件夹下载
    pub async fn get_all_folders(&self) -> Vec<FolderDownload> {
        let folders = self.folders.read().await;
        folders.values().cloned().collect()
    }

    /// 🔥 获取归属指定 `backup_config_id`（如 `share-sync:{订阅id}`）的内存文件夹下载
    ///
    /// 供分享同步收集 tree 模式整目录下载产生的文件夹子任务进度。
    pub async fn get_folders_by_backup_config(&self, backup_config_id: &str) -> Vec<FolderDownload> {
        let folders = self.folders.read().await;
        folders
            .values()
            .filter(|f| f.backup_config_id.as_deref() == Some(backup_config_id))
            .cloned()
            .collect()
    }

    /// 获取所有文件夹下载（内存 + 历史数据库）
    ///
    /// 类似于 DownloadManager::get_all_tasks()，合并内存中的文件夹和历史数据库中的已完成文件夹
    pub async fn get_all_folders_with_history(&self) -> Vec<FolderDownload> {
        // 1. 获取内存中的文件夹
        let folders = self.folders.read().await;
        let mut result: Vec<FolderDownload> = folders.values().cloned().collect();
        let folder_ids: std::collections::HashSet<String> =
            folders.keys().cloned().collect();
        drop(folders);

        // 2. 从历史数据库加载已完成的文件夹
        let history_folders = self.load_folder_history().await;

        // 3. 合并，排除已在内存中的（避免重复）
        for hist_folder in history_folders {
            if !folder_ids.contains(&hist_folder.id) {
                result.push(hist_folder);
            }
        }

        result
    }

    /// 获取指定文件夹下载
    pub async fn get_folder(&self, folder_id: &str) -> Option<FolderDownload> {
        let folders = self.folders.read().await;
        folders.get(folder_id).cloned()
    }

    /// 清除内存中已完成的文件夹
    ///
    /// 返回清除的数量
    pub async fn clear_completed_folders(&self) -> usize {
        let mut folders = self.folders.write().await;
        let before_count = folders.len();

        folders.retain(|_, folder| folder.status != FolderStatus::Completed);

        let removed = before_count - folders.len();
        drop(folders);
        if removed > 0 {
            info!("从内存中清除了 {} 个已完成的文件夹", removed);
            self.prune_persist_state().await;
        }
        removed
    }

    /// 清除内存中属于指定账号的已完成文件夹
    ///
    /// 共享 `FolderDownloadManager` 设计下，按 `owner_uid` 严格过滤，避免
    /// 跨账号清理。
    pub async fn clear_completed_folders_for_owner(&self, uid: crate::auth::Uid) -> usize {
        let mut folders = self.folders.write().await;
        let before_count = folders.len();

        folders.retain(|_, folder| {
            !(folder.owner_uid == uid && folder.status == FolderStatus::Completed)
        });

        let removed = before_count - folders.len();
        drop(folders);
        if removed > 0 {
            self.prune_persist_state().await;
            info!(
                "从内存中清除了 {} 个已完成的文件夹（owner_uid={}）",
                removed,
                uid.raw()
            );
        }
        removed
    }

    /// 删除指定账号下所有文件夹下载
    ///
    /// 用于 `force_delete_account` 链路。`clear_completed_folders_for_owner`
    /// 只清已完成文件夹，而真正会取消扫描令牌、清 pending_files、释放
    /// fixed/borrowed 槽位的是 `cancel_folder` / `delete_folder`。账号被删时
    /// 如果文件夹下载还在扫描或 pending，它会在子任务被删后继续补新子任务，
    /// 且聚合接口仍能看到 orphan folder。
    ///
    /// 行为（每个文件夹）：
    /// - 终态文件夹（Completed/Failed/Cancelled）：直接 `delete_folder` 移除内存
    ///   + 删除持久化 + 删除历史
    /// - 进行中文件夹：`cancel_folder` 取消扫描令牌、清 pending、释放槽位、删
    ///   子任务（共享 manager 内的子任务已被 `delete_tasks_for_owner` 删过，
    ///   但本路径再次调用是幂等的）；`delete_files=false` 默认，账号删除不附
    ///   带本地文件删除
    ///
    /// 返回成功处理的文件夹数。
    pub async fn delete_folders_for_owner(
        &self,
        uid: crate::auth::Uid,
        delete_files: bool,
    ) -> usize {
        // 1) 收集归属该 uid 的所有文件夹 ID 与状态（先放锁再做异步操作，避免持锁过久）
        let target_folders: Vec<(String, FolderStatus)> = {
            let folders = self.folders.read().await;
            folders
                .iter()
                .filter(|(_, f)| f.owner_uid == uid)
                .map(|(id, f)| (id.clone(), f.status.clone()))
                .collect()
        };

        if target_folders.is_empty() {
            info!(
                "delete_folders_for_owner: uid={} 内存中无归属文件夹",
                uid.raw()
            );
            return 0;
        }

        let total = target_folders.len();
        info!(
            "delete_folders_for_owner: uid={} 内存中找到 {} 个文件夹下载",
            uid.raw(),
            total
        );

        // 2) 按状态分别处理
        let mut processed = 0;
        for (folder_id, status) in target_folders {
            let result = if matches!(
                status,
                FolderStatus::Completed | FolderStatus::Failed | FolderStatus::Cancelled
            ) {
                // 终态：仅删除记录
                self.delete_folder(&folder_id).await
            } else {
                // 进行中：取消扫描令牌 + 清 pending + 释放槽位 + 删除内存项
                self.cancel_folder(&folder_id, delete_files).await
            };

            match result {
                Ok(()) => {
                    processed += 1;
                    info!(
                        "delete_folders_for_owner: 已处理 folder={}（status={:?}, delete_files={}）",
                        folder_id, status, delete_files
                    );
                }
                Err(e) => {
                    warn!(
                        "delete_folders_for_owner: 处理 folder={} 失败（status={:?}）: {}",
                        folder_id, status, e
                    );
                }
            }
        }

        info!(
            "delete_folders_for_owner: uid={} 完成，处理 {}/{} 个文件夹",
            uid.raw(),
            processed,
            total
        );
        processed
    }

    /// 从历史记录加载已完成的文件夹（优先从数据库加载）
    ///
    /// 返回已完成文件夹的列表（用于前端显示历史记录）
    pub async fn load_folder_history(&self) -> Vec<FolderDownload> {
        // 优先从数据库加载
        let pm_opt = self.persistence_manager.read().await.clone();
        if let Some(pm) = pm_opt {
            let pm_guard = pm.lock().await;
            if let Some(db) = pm_guard.history_db() {
                match db.load_all_folder_history() {
                    Ok(folders) => {
                        return folders.into_iter().map(|f| f.to_folder()).collect();
                    }
                    Err(e) => {
                        error!("从数据库加载文件夹历史失败: {}", e);
                    }
                }
            }
        }

        // 回退到文件加载（兼容旧数据）
        let wal_dir = {
            let dir = self.wal_dir.read().await;
            dir.clone()
        };

        let wal_dir = match wal_dir {
            Some(dir) => dir,
            None => return Vec::new(),
        };

        match crate::persistence::folder::load_folder_history(&wal_dir) {
            Ok(folders) => folders.into_iter().map(|f| f.to_folder()).collect(),
            Err(e) => {
                error!("加载文件夹历史失败: {}", e);
                Vec::new()
            }
        }
    }

    /// 从历史记录加载已完成的文件夹到内存（优先从数据库加载）
    ///
    /// 在恢复时调用，将历史归档的已完成文件夹加载到内存中
    /// 这样前端获取所有下载时可以看到历史完成的文件夹
    pub async fn load_history_folders_to_memory(&self) -> usize {
        // 优先从数据库加载
        let pm_opt = self.persistence_manager.read().await.clone();
        let history_folders: Vec<FolderPersisted> = if let Some(pm) = pm_opt {
            let pm_guard = pm.lock().await;
            if let Some(db) = pm_guard.history_db() {
                match db.load_all_folder_history() {
                    Ok(folders) => folders,
                    Err(e) => {
                        error!("从数据库加载文件夹历史失败: {}", e);
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            }
        } else {
            // 回退到文件加载（兼容旧数据）
            let wal_dir = {
                let dir = self.wal_dir.read().await;
                dir.clone()
            };

            match wal_dir {
                Some(dir) => {
                    match crate::persistence::folder::load_folder_history(&dir) {
                        Ok(folders) => folders,
                        Err(e) => {
                            error!("加载文件夹历史失败: {}", e);
                            Vec::new()
                        }
                    }
                }
                None => {
                    warn!("WAL 目录未设置，跳过加载历史文件夹");
                    Vec::new()
                }
            }
        };

        if history_folders.is_empty() {
            return 0;
        }

        let mut loaded = 0;
        {
            let mut folders = self.folders.write().await;
            for persisted in history_folders {
                // 只添加不存在于内存中的文件夹（避免重复）
                if !folders.contains_key(&persisted.id) {
                    let folder = persisted.to_folder();
                    folders.insert(folder.id.clone(), folder);
                    loaded += 1;
                }
            }
        }

        if loaded > 0 {
            info!("从历史记录加载了 {} 个已完成文件夹到内存", loaded);
        }

        loaded
    }

    /// 从历史记录中删除文件夹（优先从数据库删除）
    pub async fn delete_folder_from_history(&self, folder_id: &str) -> Result<bool> {
        // 优先从数据库删除
        let pm_opt = self.persistence_manager.read().await.clone();
        if let Some(pm) = pm_opt {
            let pm_guard = pm.lock().await;
            if let Some(db) = pm_guard.history_db() {
                match db.remove_folder_from_history(folder_id) {
                    Ok(removed) => return Ok(removed),
                    Err(e) => {
                        error!("从数据库删除文件夹历史失败: {}", e);
                    }
                }
            }
        }

        // 回退到文件删除（兼容旧数据）
        let wal_dir = {
            let dir = self.wal_dir.read().await;
            dir.clone()
        };

        let wal_dir = match wal_dir {
            Some(dir) => dir,
            None => return Ok(false),
        };

        match remove_folder_from_history(&wal_dir, folder_id) {
            Ok(removed) => Ok(removed),
            Err(e) => Err(anyhow!("从历史删除文件夹失败: {}", e)),
        }
    }

    /// 暂停文件夹下载
    pub async fn pause_folder(&self, folder_id: &str) -> Result<()> {
        info!("暂停文件夹下载: {}", folder_id);

        // 🔥 关键：先更新文件夹状态为 Paused，阻止 task_completed_listener 创建新任务
        // 这必须在暂停任务之前执行，避免竞态条件
        // 🔥 同时取出 folder.owner_uid 用于路由 download_manager
        let (old_status, folder_owner_uid_for_routing) = {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                let old_status = format!("{:?}", folder.status).to_lowercase();
                let owner_uid = folder.owner_uid;
                folder.mark_paused();
                info!("文件夹 {} 状态已标记为暂停", folder.name);
                (old_status, Some(owner_uid))
            } else {
                (String::new(), None)
            }
        };

        // 触发取消令牌，停止扫描
        {
            let tokens = self.cancellation_tokens.read().await;
            if let Some(token) = tokens.get(folder_id) {
                token.cancel();
            }
        }

        // 🔥 按 folder.owner_uid 路由 download_manager
        let download_manager = match folder_owner_uid_for_routing {
            Some(uid) => self
                .download_manager_for(uid)
                .await
                .ok_or_else(|| anyhow!("下载管理器未初始化（owner_uid={}）", uid.raw()))?,
            None => return Err(anyhow!("文件夹不存在")),
        };

        // 🔥 关键改进：使用 cancel_tasks_by_group 取消所有子任务
        // 这会：
        // 1. 从等待队列移除该文件夹的任务
        // 2. 触发所有子任务的取消令牌（包括正在探测中的任务！）
        // 3. 从调度器取消已注册的任务
        // 4. 更新任务状态为 Paused
        //
        // 之前的问题：只调用 pause_task，但 pause_task 只能处理 Downloading 状态的任务
        // 正在探测中的任务（Pending 状态）不会被暂停，探测完成后仍会注册到调度器
        download_manager.cancel_tasks_by_group(folder_id).await;

        // 🔥 释放文件夹的所有槽位（固定位 + 借调位）
        // 暂停时释放槽位，让其他任务可以使用。
        //
        // 必须走 release_folder_slots，而不是仅 task_slot_pool.release_all_slots(folder_id)：
        // 后者只清 task_slot_pool 端的状态；前者除此之外还会清理 folder 自身的
        // fixed_slot_id / borrowed_slot_ids / borrowed_subtask_map / fixed_slot_subtask 四个映射，
        // 否则恢复时 borrowed_subtask_map / fixed_slot_subtask 仍会让本来已经释放的槽位被
        // 误判为"已占用"，导致同组的子任务拿不到本该可用的文件夹槽位。
        // 注意：cancel_tasks_by_group 已经把任务侧的 slot_id / is_borrowed_slot /
        // uses_folder_fixed_slot 字段清空，这里只需收尾 folder 侧映射。
        let _ = download_manager; // 仅用于持有 dm 引用直到此处，便于阅读上下文
        self.release_folder_slots(folder_id).await;
        info!("文件夹 {} 暂停，已释放所有槽位（含 folder 侧映射）", folder_id);

        // 🔥 关键修复：先持久化，再发送消息
        // 确保前端收到消息时，状态已经保存到磁盘
        self.persist_folder(folder_id).await;

        // 🔥 取出 owner_uid
        let owner_uid_raw_opt: Option<u64> = {
            let folders = self.folders.read().await;
            folders.get(folder_id).map(|f| f.owner_uid.raw())
        };

        // 🔥 发送状态变更事件（在持久化之后）
        if !old_status.is_empty() {
            self.publish_event(FolderEvent::StatusChanged {
                folder_id: folder_id.to_string(),
                old_status,
                new_status: "paused".to_string(),

                owner_uid: owner_uid_raw_opt,
            })
                .await;
        }

        // 🔥 发布暂停事件
        self.publish_event(FolderEvent::Paused {
            folder_id: folder_id.to_string(),

            owner_uid: owner_uid_raw_opt,
        })
            .await;

        info!("文件夹 {} 暂停完成", folder_id);
        Ok(())
    }

    /// 恢复文件夹下载
    pub async fn resume_folder(&self, folder_id: &str) -> Result<()> {
        info!("恢复文件夹下载: {}", folder_id);

        // 🔥 同时取出 folder.owner_uid 用于路由 download_manager，
        // 以及 backup_config_id —— 决定重新申请槽位时用哪档优先级（见 `folder_slot_priority`）
        let (
            folder_info,
            old_status,
            new_status,
            folder_owner_uid_for_routing,
            folder_backup_config_id,
        ) = {
            let mut folders = self.folders.write().await;
            let folder = folders
                .get_mut(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;

            if folder.status != FolderStatus::Paused && folder.status != FolderStatus::Failed {
                return Err(anyhow!("文件夹状态不正确，当前状态: {:?}", folder.status));
            }

            let old_status = format!("{:?}", folder.status).to_lowercase();

            // 🔥 如果从 Failed 恢复，重置失败计数（失败的子任务将被重新调度）
            if folder.status == FolderStatus::Failed {
                folder.failed_count = 0;
                folder.failed_task_ids.clear();
                // 用户手动点继续 = 明确要求重试，给一份新的自动重试额度
                folder.subtask_retry_counts.clear();
                folder.error = None;
            }

            // 更新状态
            if folder.scan_completed {
                folder.mark_downloading();
            } else {
                folder.status = FolderStatus::Scanning;
            }

            let new_status = format!("{:?}", folder.status).to_lowercase();
            let owner_uid = folder.owner_uid;
            let backup_config_id = folder.backup_config_id.clone();

            (
                (
                    folder.scan_completed,
                    folder.remote_root.clone(),
                    folder.local_root.clone(),
                ),
                old_status,
                new_status,
                owner_uid,
                backup_config_id,
            )
        };

        // 🔥 按 folder.owner_uid 路由 download_manager
        let download_manager = self
            .download_manager_for(folder_owner_uid_for_routing)
            .await
            .ok_or_else(|| {
                anyhow!(
                    "下载管理器未初始化（owner_uid={}）",
                    folder_owner_uid_for_routing.raw()
                )
            })?;

        // 🔥 关键修复：恢复文件夹时，先为文件夹分配槽位（固定位 + 借调位）
        // 这样子任务才能使用借调位，而不是占用固定位
        // 暂停时释放了所有槽位，恢复时需要重新分配
        let slot_pool = download_manager.task_slot_pool();

        // 1. 先分配固定位
        // 与创建路径同款：后台文件夹（自动备份 / 分享同步）走 Backup 优先级，
        // 恢复时同样只用空闲槽、不抢占。
        let slot_priority = folder_slot_priority(folder_backup_config_id.as_deref());
        let (mut fixed_slot_id, mut preempted_task_id) =
            if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                folder_id, true, slot_priority
            ).await {
                (Some(slot_id), preempted)
            } else {
                (None, None)
            };

        // 🔥 处理被抢占的备份任务
        if let Some(preempted_id) = preempted_task_id.take() {
            info!("恢复文件夹 {} 抢占了备份任务 {} 的槽位", folder_id, preempted_id);
            // 暂停被抢占的备份任务并加入等待队列
            download_manager.handle_preempted_slot_owner(&preempted_id).await;
        }

        // 🔥 如果没有空闲槽位，尝试从同一账号的其他文件夹回收借调位
        // 这确保了多个文件夹任务之间的公平性：每个文件夹至少能获得一个固定位
        // reclaim 必须按 owner_uid 过滤，避免跨账号误回收
        // 🔥 后台文件夹（Backup 优先级）不参与回收，理由同创建路径
        if fixed_slot_id.is_none() && slot_priority == crate::task_slot_pool::TaskPriority::Normal {
            info!("恢复文件夹 {} 无空闲槽位，尝试回收同账号其他文件夹的借调位", folder_id);
            if let Some(reclaimed_slot_id) = self
                .reclaim_borrowed_slot_for_owner(folder_owner_uid_for_routing)
                .await
            {
                // 回收成功，重新分配固定位（使用优先级分配）
                if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                    folder_id, true, slot_priority
                ).await {
                    fixed_slot_id = Some(slot_id);
                    info!(
                        "恢复文件夹 {} 通过回收借调位获得固定任务位: slot_id={} (回收的槽位={})",
                        folder_id, slot_id, reclaimed_slot_id
                    );
                    // 处理可能被抢占的备份任务
                    if let Some(preempted_id) = preempted {
                        info!("恢复文件夹 {} 抢占了槽位持有者 {}", folder_id, preempted_id);
                        download_manager.handle_preempted_slot_owner(&preempted_id).await;
                    }
                }
            }
        }

        if let Some(slot_id) = fixed_slot_id {
            let mut folders_guard = self.folders.write().await;
            if let Some(folder) = folders_guard.get_mut(folder_id) {
                folder.fixed_slot_id = Some(slot_id);
                info!("恢复文件夹 {} 获得固定任务位: slot_id={}", folder_id, slot_id);
            }
        } else {
            warn!("恢复文件夹 {} 无法获得固定任务位，将在有空位时重试", folder_id);
        }

        // 2. 按池子容量计算可借调槽位（固定槽位保留 1 个）
        // - Normal（用户手动）：空闲槽不够时可抢占备份任务的槽位
        // - Backup（自动备份 / 分享同步）：只借空闲槽，绝不抢占（理由同创建路径）
        let is_normal_priority = slot_priority == crate::task_slot_pool::TaskPriority::Normal;
        let available = if is_normal_priority {
            slot_pool.available_borrow_slots().await
        } else {
            slot_pool.available_slots().await
        };
        let fixed_reserved = if fixed_slot_id.is_some() { 1 } else { 0 };
        let max_borrowable = slot_pool.max_slots().saturating_sub(fixed_reserved);
        let to_borrow = available.min(max_borrowable);
        let (borrowed_slot_ids, preempted_backup_tasks) = if to_borrow == 0 {
            (Vec::new(), Vec::new())
        } else if is_normal_priority {
            slot_pool.allocate_borrowed_slots(folder_id, to_borrow).await
        } else {
            (
                slot_pool
                    .allocate_borrowed_slots_no_preempt(folder_id, to_borrow)
                    .await,
                Vec::new(),
            )
        };

        // 🔥 处理被抢占的备份任务（暂停并加入等待队列）
        if !preempted_backup_tasks.is_empty() {
            info!(
                "恢复文件夹 {} 借调槽位时抢占了 {} 个备份任务: {:?}",
                folder_id,
                preempted_backup_tasks.len(),
                preempted_backup_tasks
            );
            for preempted_id in &preempted_backup_tasks {
                // 暂停被抢占的备份任务
                download_manager.handle_preempted_slot_owner(preempted_id).await;
            }
        }

        if !borrowed_slot_ids.is_empty() {
            let mut folders_guard = self.folders.write().await;
            if let Some(folder) = folders_guard.get_mut(folder_id) {
                folder.borrowed_slot_ids = borrowed_slot_ids.clone();
                info!(
                    "恢复文件夹 {} 借调 {} 个任务位: {:?}",
                    folder_id,
                    borrowed_slot_ids.len(),
                    borrowed_slot_ids
                );
            }
        }

        // 🔥 获取需要恢复的子任务（暂停 + 失败），为它们分配借调位后再启动
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        let paused_tasks: Vec<_> = tasks.iter().filter(|t| t.status == TaskStatus::Paused || t.status == TaskStatus::Failed).collect();

        // 计算可用的槽位数（固定位 + 借调位）
        let total_slots = {
            let folders_guard = self.folders.read().await;
            if let Some(folder) = folders_guard.get(folder_id) {
                let fixed = if folder.fixed_slot_id.is_some() { 1 } else { 0 };
                fixed + folder.borrowed_slot_ids.len()
            } else {
                0
            }
        };

        info!(
            "恢复文件夹 {} 有 {} 个暂停任务，可用槽位: {} (固定位: {}, 借调位: {})",
            folder_id,
            paused_tasks.len(),
            total_slots,
            if fixed_slot_id.is_some() { 1 } else { 0 },
            borrowed_slot_ids.len()
        );

        // 为子任务分配槽位并启动
        let mut started_count = 0;
        let mut pending_count = 0;
        // 🔥 关键修复：使用 used_slot_ids 跟踪已分配的槽位，防止重复分配
        let mut used_slot_ids: std::collections::HashSet<usize> = std::collections::HashSet::new();

        for task in &paused_tasks {
            // 🔥 槽位分配顺序：**文件夹自己的固定位优先，借调位兜底**
            //
            // 与另外两条补任务路径保持一致。历史实现在这里是反过来的（先借调位、
            // 固定位兜底），后果是恢复出来的文件夹把自己的固定位空着、却占着借来的位：
            // 实测（2026-08-21 14:24 日志）一个只含 1 个文件的文件夹恢复后，
            // 子任务落在借调位 2 上、固定位 1 从头到尾没人用，等于 1 个文件占了 2 个槽，
            // 另一个 8 文件的文件夹因此少拿一个位。
            //
            // 固定位是这个文件夹的保底资源，先用满自己的再去借；借来的位才好还回去。
            let assigned_slot = {
                let mut folders_guard = self.folders.write().await;
                if let Some(folder) = folders_guard.get_mut(folder_id) {
                    let mut found_slot = None;

                    // 1. 先用文件夹自己的固定位（同一时刻只能有一个子任务直持有）
                    if let Some(fixed_slot) = folder.fixed_slot_id {
                        if !used_slot_ids.contains(&fixed_slot)
                            && folder.fixed_slot_subtask.is_none()
                        {
                            folder.fixed_slot_subtask = Some(task.id.clone());
                            found_slot = Some((fixed_slot, false)); // 固定位不是借调位
                        }
                    }

                    // 2. 固定位已被占用 → 退而使用借调位
                    if found_slot.is_none() {
                        for &slot_id in &folder.borrowed_slot_ids {
                            // 🔥 关键修复：同时检查 borrowed_subtask_map 和 used_slot_ids
                            let in_map =
                                folder.borrowed_subtask_map.values().any(|&s| s == slot_id);
                            let in_use = used_slot_ids.contains(&slot_id);
                            if !in_map && !in_use {
                                found_slot = Some((slot_id, true)); // (slot_id, is_borrowed)
                                folder.borrowed_subtask_map.insert(task.id.clone(), slot_id);
                                break;
                            }
                        }
                    }

                    found_slot
                } else {
                    None
                }
            };

            if let Some((slot_id, is_borrowed)) = assigned_slot {
                // 🔥 关键修复：将分配的槽位加入已使用集合，防止后续任务重复分配
                used_slot_ids.insert(slot_id);

                // 更新子任务的槽位信息
                download_manager.update_task_slot(&task.id, slot_id, is_borrowed).await;
                info!(
                    "恢复子任务 {} 分配槽位: slot_id={}, is_borrowed={}",
                    task.id, slot_id, is_borrowed
                );

                // 启动子任务
                if let Err(e) = download_manager.resume_task(&task.id).await {
                    warn!("恢复子任务 {} 失败: {}", task.id, e);
                } else {
                    started_count += 1;
                }
            } else {
                // 🔥 关键修复：没有可用槽位，将任务设为 Pending 状态并加入等待队列
                // 而不是保持 Paused 状态，因为文件夹任务已经是 Downloading 状态
                if let Err(e) = download_manager.set_task_pending_and_queue(&task.id).await {
                    warn!("设置子任务 {} 为等待状态失败: {}", task.id, e);
                } else {
                    pending_count += 1;
                    info!("子任务 {} 无可用槽位，已设为等待状态", task.id);
                }
            }
        }

        info!(
            "恢复文件夹 {} 完成: 启动 {} 个子任务，{} 个进入等待队列",
            folder_id,
            started_count,
            pending_count
        );

        // 🔥 关键修复：先持久化，再发送消息
        // 确保前端收到消息时，状态已经保存到磁盘
        self.persist_folder(folder_id).await;

        // 🔥 取出 owner_uid
        let owner_uid_raw_opt: Option<u64> = {
            let folders = self.folders.read().await;
            folders.get(folder_id).map(|f| f.owner_uid.raw())
        };

        // 🔥 发送状态变更事件（在持久化之后）
        self.publish_event(FolderEvent::StatusChanged {
            folder_id: folder_id.to_string(),
            old_status,
            new_status,

            owner_uid: owner_uid_raw_opt,
        })
            .await;

        // 🔥 发布恢复事件
        self.publish_event(FolderEvent::Resumed {
            folder_id: folder_id.to_string(),

            owner_uid: owner_uid_raw_opt,
        })
            .await;

        // 如果扫描未完成，重新启动扫描
        if !folder_info.0 {
            let self_clone = Self {
                folders: self.folders.clone(),
                cancellation_tokens: self.cancellation_tokens.clone(),
                download_manager: self.download_manager.clone(),
                netdisk_client: self.netdisk_client.clone(),
                download_dir: self.download_dir.clone(),
                wal_dir: self.wal_dir.clone(),
                ws_manager: self.ws_manager.clone(),
                folder_progress_tx: self.folder_progress_tx.clone(),
                task_completed_tx: self.task_completed_tx.clone(),
                persistence_manager: self.persistence_manager.clone(),
                backup_record_manager: self.backup_record_manager.clone(),
                client_pool: self.client_pool.clone(),
                download_manager_pool: self.download_manager_pool.clone(),
                last_persist_at: self.last_persist_at.clone(),
                app_config: self.app_config.clone(),
            };
            let folder_id = folder_id.to_string();

            tokio::spawn(async move {
                if let Err(e) = self_clone.scan_folder_and_create_tasks(&folder_id).await {
                    error!("恢复扫描失败: {:?}", e);
                }
            });
        } else {
            // 🔥 补任务之前先对账 pending_files：重启恢复后的队列可能仍包含已经转成
            //    子任务、甚至已下载完成的文件，直接补任务会重复下载（issue #141）。
            //    继续/恢复是本文件夹重新开始产出子任务的唯一入口，放在这里可覆盖
            //    "重启 → 恢复为 Paused → 用户点继续" 这条链路。
            self.reconcile_pending_files(folder_id).await;

            // 如果扫描已完成，补充任务到10个
            if let Err(e) = self.refill_tasks(folder_id, 10).await {
                warn!("恢复时补充任务失败: {}", e);
            }
        }

        Ok(())
    }

    /// 取消文件夹下载
    pub async fn cancel_folder(&self, folder_id: &str, delete_files: bool) -> Result<()> {
        info!("取消文件夹下载: {}, 删除文件: {}", folder_id, delete_files);

        // 触发取消令牌，停止扫描
        {
            let mut tokens = self.cancellation_tokens.write().await;
            if let Some(token) = tokens.remove(folder_id) {
                token.cancel();
            }
        }

        // 🔥 关键：先更新文件夹状态并清空 pending_files，阻止 task_completed_listener 补充新任务
        // 这必须在删除任务之前执行，避免竞态条件
        // 🔥 同时取出 folder.owner_uid 用于路由 download_manager
        let (local_root, folder_owner_uid_for_routing) = {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.mark_cancelled();
                folder.pending_files.clear(); // 清空待处理队列
                info!(
                    "文件夹 {} 已标记为取消，已清空 pending_files ({} 个待处理文件)",
                    folder.name,
                    folder.pending_files.len()
                );
                (Some(folder.local_root.clone()), Some(folder.owner_uid))
            } else {
                (None, None)
            }
        };

        // 🔥 按 folder.owner_uid 路由 download_manager
        let download_manager = match folder_owner_uid_for_routing {
            Some(uid) => self
                .download_manager_for(uid)
                .await
                .ok_or_else(|| anyhow!("下载管理器未初始化（owner_uid={}）", uid.raw()))?,
            None => return Err(anyhow!("文件夹不存在")),
        };

        // 🔥 新策略：直接删除所有任务记录，让分片自然结束
        // 1. 获取所有子任务
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        let task_count = tasks.len();
        info!("正在删除文件夹 {} 的 {} 个子任务...", folder_id, task_count);

        // 2. 立即删除所有任务（触发取消令牌 + 从 HashMap 移除）
        // delete_task 会：
        //   - 触发 cancellation_token（通知分片停止）
        //   - 从调度器移除
        //   - 从 tasks HashMap 移除
        //   - 删除临时文件（如果 delete_files=true）
        for task in tasks {
            let _ = download_manager.delete_task(&task.id, delete_files).await;
        }
        info!("所有子任务已删除，等待分片物理释放...");

        // 3. 等待分片物理释放（文件句柄关闭、flush 完成）
        // 因为分片下载是异步的 tokio::spawn，删除任务后它们仍在运行
        // 需要等待它们检测到 cancellation_token 并退出
        //
        // 关键等待时间：
        // - 分片检测取消：即时（每次写入都检查）
        // - 文件 flush：最多几秒（取决于磁盘速度和缓冲区大小）
        // - 文件句柄释放：flush 完成后立即释放
        //
        // 保守估计：等待 3 秒足够（HDD 最慢情况）
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        info!("分片物理释放完成");

        // 4. 如果需要删除文件，删除整个文件夹目录
        if delete_files {
            if let Some(root_path) = local_root {
                info!("准备删除文件夹目录: {:?}", root_path);
                if root_path.exists() {
                    match tokio::fs::remove_dir_all(&root_path).await {
                        Ok(_) => info!("已删除文件夹目录: {:?}", root_path),
                        Err(e) => error!("删除文件夹目录失败: {:?}, 错误: {}", root_path, e),
                    }
                } else {
                    warn!("文件夹目录不存在: {:?}", root_path);
                }
            } else {
                warn!("local_root 为空，无法删除文件夹目录");
            }
        }

        // 🔥 释放文件夹的所有槽位
        self.release_folder_slots(folder_id).await;

        // 持久化取消状态
        self.persist_folder(folder_id).await;

        // 🔥 从 folders HashMap 中移除已取消的文件夹
        // 避免已取消的文件夹仍然出现在 get_all_folders 列表中
        // 🔥 remove 时取出 owner_uid 用于事件
        let owner_uid_raw_opt: Option<u64> = {
            let mut folders = self.folders.write().await;
            let removed = folders.remove(folder_id);
            info!("已从 folders HashMap 中移除已取消的文件夹: {}", folder_id);
            removed.map(|f| f.owner_uid.raw())
        };
        self.forget_persist_state(folder_id).await;

        // 🔥 发布删除事件（取消视为删除）
        self.publish_event(FolderEvent::Deleted {
            folder_id: folder_id.to_string(),

            owner_uid: owner_uid_raw_opt,
        })
            .await;

        Ok(())
    }

    /// 🔥 取消并清理某个转存任务派生出的全部文件夹下载（tree 模式整目录转存+自动下载）。
    ///
    /// 与 [`DownloadManager::delete_tasks_for_transfer`] 成对使用：分享同步放弃一次
    /// 转存提交时，要把它派生的下载段一起收走，否则会与重新提交那一支叠成重复项
    /// （issue #148）。不删本地已下载文件。返回清理的文件夹数。
    pub async fn delete_folders_for_transfer(&self, transfer_task_id: &str) -> usize {
        let target_ids: Vec<String> = {
            let folders = self.folders.read().await;
            folders
                .values()
                .filter(|f| f.transfer_task_id.as_deref() == Some(transfer_task_id))
                .map(|f| f.id.clone())
                .collect()
        };

        let count = target_ids.len();
        for id in &target_ids {
            if let Err(e) = self.cancel_folder(id, false).await {
                warn!(
                    "delete_folders_for_transfer: 取消文件夹 {} 失败: {}",
                    id, e
                );
            }
            let _ = self.delete_folder_from_history(id).await;
        }
        if count > 0 {
            info!(
                "delete_folders_for_transfer: transfer={} 清理 {} 个文件夹下载",
                transfer_task_id, count
            );
        }
        count
    }

    /// 🔥 删除归属指定 `backup_config_id`（如 `share-sync:{订阅id}`）的全部文件夹下载
    ///
    /// 用于删除分享同步订阅时，连带清掉 tree 模式整目录下载产生的内部隐藏文件夹任务，
    /// 避免订阅删除后残留无法在任何界面看到的孤儿文件夹。返回处理的文件夹数。
    pub async fn delete_folders_for_backup_config(&self, backup_config_id: &str) -> usize {
        let target_ids: Vec<String> = {
            let folders = self.folders.read().await;
            folders
                .values()
                .filter(|f| f.backup_config_id.as_deref() == Some(backup_config_id))
                .map(|f| f.id.clone())
                .collect()
        };

        let count = target_ids.len();
        for id in &target_ids {
            // 取消正在运行的文件夹下载（停子任务 + 出内存），不删本地已下载文件。
            if let Err(e) = self.cancel_folder(id, false).await {
                warn!(
                    "delete_folders_for_backup_config: 取消文件夹 {} 失败: {}",
                    id, e
                );
            }
            // 一并清掉历史记录，避免订阅删除后历史残留。
            let _ = self.delete_folder_from_history(id).await;
        }
        if count > 0 {
            info!(
                "delete_folders_for_backup_config: cfg={} 清理 {} 个内部文件夹下载",
                backup_config_id, count
            );
        }
        count
    }

    /// 删除文件夹下载记录
    pub async fn delete_folder(&self, folder_id: &str) -> Result<()> {
        let mut folders = self.folders.write().await;
        // 🔥 remove 时取出 owner_uid 用于事件
        let owner_uid_raw_opt: Option<u64> = folders.remove(folder_id).map(|f| f.owner_uid.raw());
        drop(folders);
        self.forget_persist_state(folder_id).await;

        // 删除持久化文件
        self.delete_folder_persistence(folder_id).await;

        // 同时从历史记录中删除（如果存在）
        let _ = self.delete_folder_from_history(folder_id).await;

        // 🔥 发布删除事件
        self.publish_event(FolderEvent::Deleted {
            folder_id: folder_id.to_string(),

            owner_uid: owner_uid_raw_opt,
        })
            .await;

        // 删除子任务的历史记录（优先从数据库删除）
        let pm_opt = self.persistence_manager.read().await.clone();
        if let Some(pm) = pm_opt {
            let pm_guard = pm.lock().await;
            if let Some(db) = pm_guard.history_db() {
                match db.remove_tasks_by_group(folder_id) {
                    Ok(count) if count > 0 => {
                        info!("已从数据库删除文件夹 {} 的 {} 个子任务历史记录", folder_id, count);
                    }
                    Err(e) => {
                        error!("从数据库删除子任务历史记录失败: {}", e);
                    }
                    _ => {}
                }
            }
        } else {
            // 回退到文件删除（兼容旧数据）
            let wal_dir = {
                let dir = self.wal_dir.read().await;
                dir.clone()
            };
            if let Some(wal_dir) = wal_dir {
                match remove_tasks_by_group_from_history(&wal_dir, folder_id) {
                    Ok(count) if count > 0 => {
                        info!("已删除文件夹 {} 的 {} 个子任务历史记录", folder_id, count);
                    }
                    Err(e) => {
                        error!("删除子任务历史记录失败: {}", e);
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// 🔥 对账 `pending_files`：剔除已经有子任务承载的文件
    ///
    /// **背景（issue #141）**：`pending_files` 是文件夹自己的快照，而子任务走的是
    /// 独立的 WAL/历史库两套持久化。快照落盘是有节流和时机的，崩溃或强杀（`docker
    /// restart` 发 SIGTERM，PID 1 无 handler → SIGKILL）时，恢复出来的队列可能仍包含
    /// 那些**已经转成子任务、甚至已经下载完成**的文件。这些文件一旦被重新排进队列，
    /// 补任务只剩冲突策略一道闸门，策略若是 `Overwrite` 就会把本地已下好的文件重下一遍。
    ///
    /// 因此在文件夹恢复运行前做一次对账，剔除两类文件：
    /// - 已有活跃/已恢复子任务的（`DownloadManager` 内存表）——否则会出现两个任务写同一路径
    /// - 已完成并归档到历史库的（完成后会从内存表移除，只能从历史库查）
    ///
    /// 同时把 `completed_count` 校正到历史库的真实完成数，否则剔除文件后
    /// `completed_count + failed_count` 永远够不到 `total_files`，文件夹到不了终态。
    ///
    /// 只在恢复/继续这类低频路径调用（O(pending) 一次），不进补任务热路径。
    async fn reconcile_pending_files(&self, folder_id: &str) {
        let (owner_uid, pending_before) = {
            let folders = self.folders.read().await;
            match folders.get(folder_id) {
                Some(f) => (f.owner_uid, f.pending_files.len()),
                None => return,
            }
        };

        if pending_before == 0 {
            return;
        }

        // 1) 已有子任务承载的相对路径（活跃 + 已恢复）
        let mut covered: HashSet<String> = HashSet::new();
        if let Some(dm) = self.download_manager_for(owner_uid).await {
            for task in dm.get_tasks_by_group(folder_id).await {
                if let Some(rp) = task.relative_path {
                    covered.insert(rp);
                }
            }
        }

        // 2) 已完成并归档到历史库的相对路径
        let mut history_completed = 0usize;
        let pm_opt = self.persistence_manager.read().await.clone();
        if let Some(pm) = pm_opt {
            let pm_guard = pm.lock().await;
            if let Some(db) = pm_guard.history_db() {
                match db.get_completed_relative_paths_by_group(folder_id) {
                    Ok(paths) => {
                        history_completed = paths.len();
                        covered.extend(paths);
                    }
                    Err(e) => {
                        // 查不到就退化为"只按活跃任务对账"，宁可少剔除也不误删待下载文件
                        warn!("查询文件夹 {} 已完成子任务历史失败: {}", folder_id, e);
                    }
                }
            }
        }

        if covered.is_empty() {
            return;
        }

        // 3) 剔除 + 校正完成数
        let (removed, pending_after, completed_count) = {
            let mut folders = self.folders.write().await;
            let folder = match folders.get_mut(folder_id) {
                Some(f) => f,
                None => return,
            };

            folder
                .pending_files
                .retain(|pf| !covered.contains(&pf.relative_path));

            let pending_after = folder.pending_files.len();
            let removed = pending_before.saturating_sub(pending_after);

            // 历史库的完成数是权威值（已完成子任务归档后会从内存表移除）；
            // 取 max 防止历史被清理过导致计数倒退。
            if (history_completed as u64) > folder.completed_count {
                folder.completed_count = history_completed as u64;
            }

            (removed, pending_after, folder.completed_count)
        };

        if removed > 0 {
            info!(
                "文件夹 {} pending 对账: 剔除 {} 个已有任务/已完成的文件 ({} → {}), completed_count={}",
                folder_id, removed, pending_before, pending_after, completed_count
            );
            self.persist_folder(folder_id).await;
        }
    }

    /// 补充任务：保持文件夹有指定数量的活跃任务
    ///
    /// 这是核心方法：检查活跃任务数，如果不足就从 pending_files 补充
    /// 🔥 修复：在分配借调位前，收集所有子任务已占用的槽位，避免重复分配
    async fn refill_tasks(&self, folder_id: &str, target_count: usize) -> Result<()> {
        // 🔥 循环补批：命中"跳过"的文件不会占用槽位，若只补一批就返回，整批全被跳过时
        //    既没有子任务、也就没有完成事件来驱动下一次补任务，文件夹会卡在
        //    downloading 且详情里一个任务都看不到（issue #141 用户反馈的"长时间看不到
        //    任务在进行"）。这里持续补到槽位填满或 pending 取空为止。
        //
        //    终止性：每轮 skipped > 0 意味着至少从 pending 取走了一个文件，pending 单调
        //    减少；取空后 batch 会走 to_create == 0 的早退，skipped 归零跳出。
        loop {
            let batch = self.refill_tasks_batch(folder_id, target_count).await?;
            if batch.aborted || batch.skipped == 0 {
                break;
            }
        }
        Ok(())
    }

    /// 补充任务：单批次实现，返回本批的创建/跳过/中止情况供 `refill_tasks` 决定是否续批
    async fn refill_tasks_batch(
        &self,
        folder_id: &str,
        target_count: usize,
    ) -> Result<RefillBatch> {
        // 🔥 先取 folder.owner_uid，按 uid 路由 download_manager
        let folder_owner_uid_for_routing = {
            let folders = self.folders.read().await;
            folders
                .get(folder_id)
                .map(|f| f.owner_uid)
                .ok_or_else(|| anyhow!("文件夹不存在"))?
        };
        let download_manager = self
            .download_manager_for(folder_owner_uid_for_routing)
            .await
            .ok_or_else(|| {
                anyhow!(
                    "下载管理器未初始化（folder.owner_uid={}）",
                    folder_owner_uid_for_routing.raw()
                )
            })?;

        // 检查当前活跃任务数
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        let active_count = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Downloading || t.status == TaskStatus::Pending)
            .count();

        // 🔥 收集所有子任务已占用的槽位（包括恢复的任务可能不在 borrowed_subtask_map 中）
        // 🔥 关键修复：使用 mut，在循环中分配槽位后需要更新此集合
        let mut used_slot_ids: std::collections::HashSet<usize> = tasks
            .iter()
            .filter_map(|t| t.slot_id)
            .collect();

        // 🔥 先把跑在借调位上的子任务挪回空闲的固定位（腾出借调位），
        //    再归还这一轮用不上的借调位，最后才判断要不要补任务
        self.compact_folder_slots(folder_id).await;

        // 🔥 归还这一轮用不上的借调位
        //
        // 必须放在下面几处早退**之前**：本方法在「活跃数已够」「pending 为空」
        // 「文件夹已暂停/取消」时都会提前 return，而恰恰是这些情况下借调位最闲。
        // 实测（2026-08-21 11:17 日志）：一个恢复出来的单文件文件夹 pending 为空，
        // 每次周期性补任务都在 `to_create == 0` 处返回，末尾的归还永远执行不到，
        // 于是它一直攥着 3 个空借调位，另一个文件夹的 4 个子任务全卡在等待队列。
        self.release_surplus_borrowed_slots(folder_id).await;

        // 如果已经足够，不需要补充
        if active_count >= target_count {
            return Ok(RefillBatch::default());
        }

        // 计算需要补充的数量
        let needed = target_count - active_count;

        // 从 pending_files 取出需要的文件
        let (files_to_create, local_root, group_root, folder_owner_uid) = {
            let mut folders = self.folders.write().await;
            let folder = folders
                .get_mut(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;

            // 检查状态，如果暂停或取消，不补充任务
            if folder.status == FolderStatus::Paused
                || folder.status == FolderStatus::Cancelled
                || folder.status == FolderStatus::Failed
            {
                return Ok(RefillBatch::default());
            }

            // 🔥 只挑选文件，**不从 pending_files 摘除**
            //
            // 子任务在真正启动之前是不落盘的（`add_task` / `add_task_paused` 都只往
            // 内存任务表里插，只有 `start_task_internal` 才注册持久化）。如果建任务时
            // 就把文件从 pending_files 摘掉，那「已建任务、还没启动」的文件在重启后
            // 两头都不存在 —— 实测一个 8 文件的文件夹重启后只剩 2 个任务、0 待处理。
            //
            // 所以摘除时机推迟到任务真正落盘那一刻（见
            // `FolderDownloadManager::drop_pending_file_after_persist`）。在此之前
            // 文件同时存在于 pending_files 和内存任务里，靠下面的 fs_id 去重
            // 保证不会重复建任务。
            let existing_fs_ids: std::collections::HashSet<u64> =
                tasks.iter().map(|t| t.fs_id).collect();
            // 🔥 已下完的文件直接挡掉（issue #156 续），理由见另一条补任务路径的同名过滤
            let files: Vec<_> = folder
                .pending_files
                .iter()
                .filter(|f| !existing_fs_ids.contains(&f.fs_id))
                .filter(|f| {
                    let already_done =
                        f.fs_id != 0 && folder.counted_fs_ids.contains(&f.fs_id);
                    if already_done {
                        warn!(
                            "文件夹 {} 把已完成过的文件重新排进了补任务队列 (fs_id={}, path={})，已拦截",
                            folder_id, f.fs_id, f.relative_path
                        );
                    }
                    !already_done
                })
                .take(needed)
                .cloned()
                .collect();
            if files.is_empty() {
                return Ok(RefillBatch::default());
            }
            (files, folder.local_root.clone(), folder.remote_root.clone(), folder.owner_uid)
        };

        if files_to_create.is_empty() {
            return Ok(RefillBatch::default());
        }

        // 🔥 冲突策略在本批次内是常量，提前解析一次：
        //    既避免每个文件重复取 folders 读锁，也避免在持有 folders 锁时再去读配置锁。
        let conflict_strategy = self.resolve_conflict_strategy(folder_id).await;

        info!(
            "补充任务: 文件夹 {} 需要 {} 个任务 (当前活跃: {}/{})",
            folder_id,
            files_to_create.len(),
            active_count,
            target_count
        );

        // 批量创建任务
        let mut created_count = 0u64;
        // 🔥 因冲突策略命中"跳过"而未建任务的文件数（计入完成数，见下方 Skip 分支）
        let mut skipped_count = 0u64;
        let mut skipped_bytes = 0u64;
        let mut skipped_list: Vec<crate::downloader::folder::SkippedFile> = Vec::new();
        // 🔥 中途中止（暂停/取消）时尚未处理的文件，循环结束后归还给 pending_files
        //
        //    文件是在循环**之前**就从 pending_files 一次性 drain 出来的，中止时若直接丢弃，
        //    这些文件既不在队列里也没有对应任务，永久丢失 → 文件夹凑不满 total_files，
        //    永远到不了终态。以前快照不落盘，重启还能从旧快照"救"回来；现在快照是准的，
        //    丢失会被持久化下来，必须显式归还。
        let mut aborted_remainder = Vec::new();
        for pending_file in files_to_create {
            // ✅ 创建任务前再次检查状态，防止竞态条件
            // 场景：取出文件后、创建任务前，pause_folder 可能已更新状态
            {
                let folders_guard = self.folders.read().await;
                let should_abort = match folders_guard.get(folder_id) {
                    Some(folder) => {
                        let aborted = folder.status == FolderStatus::Paused
                            || folder.status == FolderStatus::Cancelled
                            || folder.status == FolderStatus::Failed;
                        if aborted {
                            info!(
                                "文件夹 {} 状态已变为 {:?}，放弃创建剩余任务",
                                folder_id, folder.status
                            );
                        }
                        aborted
                    }
                    // 文件夹已被删除
                    None => true,
                };

                if should_abort {
                    drop(folders_guard);
                    // 文件本来就没从 pending_files 摘走，中止时无需归还
                    let _ = &pending_file;
                    break;
                }
            }

            let local_path = local_root.join(&pending_file.relative_path);

            // 🔥 应用冲突策略
            let final_local_path = {
                use crate::uploader::conflict_resolver::ConflictResolver;
                match ConflictResolver::resolve_download_conflict(&local_path, conflict_strategy) {
                    Ok(crate::uploader::conflict::ConflictResolution::Proceed) => local_path,
                    Ok(crate::uploader::conflict::ConflictResolution::Skip) => {
                        info!("跳过下载（文件已存在）: {:?}", local_path);
                        // 🔥 跳过的文件必须从 pending_files 摘掉（issue #156）
                        //
                        // 本方法的外层 `refill_tasks` 是 `loop { batch; if skipped == 0 { break } }`，
                        // 其终止性注释写的是「每轮 skipped > 0 意味着至少从 pending 取走了一个文件」。
                        // 但补任务后来改成了「只挑选不摘除、落盘时才摘」，而 Skip 分支不建任务、
                        // 也就永远等不到落盘回调 —— 终止性假设被打破，整个 loop 变成无限热循环
                        // （每轮重选同一批文件、skipped_count 无限涨、还每轮落一次盘）。
                        {
                            let mut folders_guard = self.folders.write().await;
                            if let Some(folder) = folders_guard.get_mut(folder_id) {
                                folder
                                    .pending_files
                                    .retain(|p| p.fs_id != pending_file.fs_id);
                            }
                        }
                        // 🔥 跳过的文件同样是"处理完毕"，必须计入 skipped_count。
                        //    total_files 在扫描时已包含它们，若不计数，
                        //    completed+skipped+failed 永远够不到 total_files，
                        //    文件夹会卡在 downloading 永远不终态。
                        skipped_count += 1;
                        skipped_bytes += pending_file.size;
                        skipped_list.push(crate::downloader::folder::SkippedFile {
                            relative_path: pending_file.relative_path.clone(),
                            size: pending_file.size,
                            skipped_at: chrono::Utc::now().timestamp(),
                        });
                        publish_skipped_event(
                            &self.ws_manager,
                            folder_id,
                            &pending_file.relative_path,
                            folder_owner_uid,
                        )
                            .await;
                        continue; // 跳过此文件，继续下一个
                    }
                    Ok(crate::uploader::conflict::ConflictResolution::UseNewPath(new_path)) => {
                        info!("自动重命名下载路径: {:?} -> {}", local_path, new_path);
                        PathBuf::from(new_path)
                    }
                    Err(e) => {
                        warn!("冲突解决失败: {}, 使用原路径", e);
                        local_path
                    }
                }
            };

            // 确保目录存在
            if let Some(parent) = final_local_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context(format!("创建目录失败: {:?}", parent))?;
            }

            // 拿不到槽位时要把文件原样退回 pending_files，这里先留一份

            let mut task = DownloadTask::new_with_group(
                pending_file.fs_id,
                pending_file.remote_path.clone(),
                final_local_path,
                pending_file.size,
                folder_id.to_string(),
                group_root.clone(),
                pending_file.relative_path,
                folder_owner_uid,
            );

            // 🔥 槽位分配顺序：文件夹自己的固定位优先，借调位兜底
            //    与另一条补任务路径保持一致，原因见那里的长注释。
            let fixed_slot_claim: Option<usize> = {
                let folders_guard = self.folders.read().await;
                match folders_guard.get(folder_id) {
                    Some(folder) => match folder.fixed_slot_id {
                        Some(fixed_slot_id)
                        if !used_slot_ids.contains(&fixed_slot_id)
                            && folder.fixed_slot_subtask.is_none() =>
                            {
                                Some(fixed_slot_id)
                            }
                        _ => None,
                    },
                    None => None,
                }
            };

            if let Some(fixed_slot_id) = fixed_slot_claim {
                // 1. 写入任务侧的"文件夹固定位直持有"语义
                task.slot_id = None;
                task.is_borrowed_slot = false;
                task.uses_folder_fixed_slot = true;
                // 2. 记入本轮 used_slot_ids
                used_slot_ids.insert(fixed_slot_id);
                // 3. 同步登记到 folder.fixed_slot_subtask
                {
                    let mut folders_mut = self.folders.write().await;
                    if let Some(folder_mut) = folders_mut.get_mut(folder_id) {
                        folder_mut.fixed_slot_subtask = Some(task.id.clone());
                    }
                }
                info!(
                    "子任务 {} 使用文件夹 {} 的固定位 (直持有语义，slot_id={})",
                    task.id, folder_id, fixed_slot_id
                );
            } else {
                // 固定位已被别的子任务占着，退而使用借调位
                // 修复：同时检查 borrowed_subtask_map 和已恢复任务的 slot_id，避免重复分配
                let borrowed_slot_assigned = {
                    let folders_guard = self.folders.read().await;
                    if let Some(folder) = folders_guard.get(folder_id) {
                        // 检查是否有空闲的借调位（未被映射到子任务，且不在已占用槽位中）
                        let mut found_slot = None;
                        for &slot_id in &folder.borrowed_slot_ids {
                            // 🔥 关键修复：既要检查 borrowed_subtask_map，也要检查 used_slot_ids
                            let in_map = folder.borrowed_subtask_map.values().any(|&s| s == slot_id);
                            let in_use = used_slot_ids.contains(&slot_id);
                            if !in_map && !in_use {
                                // 找到一个真正空闲的借调位
                                found_slot = Some(slot_id);
                                break;
                            }
                        }

                        if let Some(slot_id) = found_slot {
                            // 分配给此任务
                            task.slot_id = Some(slot_id);
                            task.is_borrowed_slot = true;
                            drop(folders_guard);

                            // 登记借调位映射
                            self.register_subtask_borrowed_slot(folder_id, &task.id, slot_id).await;

                            // 🔥 关键修复：将分配的槽位加入已使用集合，防止后续任务重复分配
                            used_slot_ids.insert(slot_id);

                            info!("子任务 {} 分配借调位: slot_id={}", task.id, slot_id);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if !borrowed_slot_assigned {
                    // 拿不到槽位也照常建任务（进等待队列），保持原有阈值；
                    // 对应文件还留在 pending_files 里，重启会被重新建出来，不会丢。
                    info!(
                        "子任务 {} 无空闲槽位，创建任务但不分配槽位（将进入等待队列）",
                        task.id
                    );
                    // task.slot_id 保持 None，任务会在 start_task 中进入等待队列
                }
            }

            // 创建并启动任务
            if let Err(e) = download_manager.add_task(task).await {
                warn!("创建下载任务失败: {}", e);
            } else {
                created_count += 1;
            }
        }

        // 更新已创建计数 / 跳过计数，并归还中止时未处理的文件
        let returned_count = {
            let mut folders = self.folders.write().await;
            match folders.get_mut(folder_id) {
                Some(folder) => {
                    folder.created_count += created_count;
                    folder.skipped_count += skipped_count;
                    folder.skipped_size += skipped_bytes;
                    folder.skipped_entries.append(&mut skipped_list);

                    let returned = aborted_remainder.len();
                    if returned > 0 {
                        info!(
                            "文件夹 {} 归还 {} 个未建任务的文件到 pending 队列（暂停/取消中止，或暂时没有空闲槽位）",
                            folder_id, returned
                        );
                        // 放回队首：pending_files 在扫描完成时按相对路径排过序，
                        // 归还到队尾会打乱下载顺序
                        aborted_remainder.append(&mut folder.pending_files);
                        folder.pending_files = aborted_remainder;
                    }
                    returned
                }
                // 文件夹已被删除，归还无意义
                None => 0,
            }
        };

        info!(
            "补充任务完成: 文件夹 {} 成功创建 {} 个任务, 跳过 {} 个已存在文件",
            folder_id, created_count, skipped_count
        );

        // 🔥 归还这一轮下来用不上的借调位
        //
        // 借调发生在扫描之前（那时还不知道有几个文件），一律把空位借光。等补任务跑完，
        // 实际需要多少已经清楚了，多借的必须还回去 —— 否则一个只含 1 个文件的文件夹
        // 会一直占着 4 个借调位，同账号的其他文件夹全部饿死在等待队列。
        self.release_surplus_borrowed_slots(folder_id).await;

        // 🔥 批次内全部命中跳过时不会有子任务完成事件来驱动终态检查，
        //    这里主动触发一次，否则文件夹会停在 downloading。
        if created_count == 0 && skipped_count > 0 {
            self.finalize_folder_if_done(folder_id).await;
        }

        // 🔥 补任务已经从 pending_files 取走了文件，必须落盘。
        //    否则重启后恢复出的队列会退回到上次落盘时的旧状态，把已下好的文件重新排进
        //    任务队列重复下载（issue #141）。
        if returned_count > 0 {
            // 归还路径必须无条件落盘：中止通常由 pause_folder 触发，而它在本方法归还
            // 文件**之前**就已经落过盘了，那份快照里恰好没有这批在途文件。若这里被节流
            // 挡掉，归还就只存在于内存，重启后这些文件仍然丢失。
            self.persist_folder(folder_id).await;
        } else {
            // 常规路径走节流版本，避免大文件夹写放大；节流窗口内崩溃丢失的那部分
            // 由恢复时的 reconcile_pending_files 对账兜底。
            self.persist_folder_throttled(folder_id).await;
        }

        Ok(RefillBatch {
            created: created_count,
            skipped: skipped_count,
            aborted: returned_count > 0,
        })
    }

    /// 更新文件夹的下载进度（定期调用）
    ///
    /// 这个方法会：
    /// 1. 更新已完成数和已下载大小
    /// 2. 检查是否全部完成
    /// 3. 补充任务，保持10个活跃任务
    /// 🔥 按 folder.owner_uid 路由 download_manager
    pub async fn update_folder_progress(&self, folder_id: &str) -> Result<()> {
        let folder_owner_uid_for_routing = {
            let folders = self.folders.read().await;
            folders
                .get(folder_id)
                .map(|f| f.owner_uid)
                .ok_or_else(|| anyhow!("文件夹不存在"))?
        };
        let download_manager = self
            .download_manager_for(folder_owner_uid_for_routing)
            .await
            .ok_or_else(|| {
                anyhow!(
                    "下载管理器未初始化（owner_uid={}）",
                    folder_owner_uid_for_routing.raw()
                )
            })?;

        let tasks = download_manager.get_tasks_by_group(folder_id).await;

        {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                // 🔥 不再从 tasks 重新计算 completed_count，因为已完成的任务会从内存移除
                // completed_count 由 start_task_completed_listener 递增维护

                // 🔥 使用 compute_downloaded_size：completed_downloaded_size + active_sum
                // max() 保证单调性
                // 🔥 排除已计入 completed_downloaded_size 的子任务，避免完成瞬间残留任务被
                // active_sum 与 completed_downloaded_size 双重累加导致已下载翻倍（见进度监听器注释）。
                let active_downloaded = folder.active_downloaded_excluding_counted(
                    tasks.iter().map(|t| (t.id.as_str(), t.downloaded_size)),
                );
                folder.compute_downloaded_size(active_downloaded);
            }
        }

        self.finalize_folder_if_done(folder_id).await;

        // 补充任务：保持10个活跃任务（完成1个，进1个）
        if let Err(e) = self.refill_tasks(folder_id, 10).await {
            warn!("补充任务失败: {}", e);
        }

        Ok(())
    }

    /// 🔥 检查文件夹是否已全部处理完毕，是则置终态、落盘并发布事件
    ///
    /// 从 `update_folder_progress` 抽出，原本供两处复用：
    /// - `update_folder_progress`：已是死代码，全 crate 没有调用方（见
    ///   `start_pending_refill_loop` 的注释），因此本方法**实际只剩下面一条活路径**
    /// - `refill_tasks`：整批文件都命中冲突策略"跳过"时，不会有任何子任务完成事件来
    ///   驱动终态检查，需要主动调用，否则文件夹卡在 downloading（issue #141 连带问题）
    ///
    /// 注意：本方法**不能**调用 `refill_tasks`，否则与 `refill_tasks` 形成异步递归。
    async fn finalize_folder_if_done(&self, folder_id: &str) {
        // 🔥 先确认没有子任务还在跑（issue #156）
        //
        //    任务完成监听器里的兄弟判定有 `active_count == 0` 这道闸门，本方法一直没有。
        //    而「只挑选不摘除、落盘时才摘」之后，pending_files 会在子任务**还在下载时**
        //    就空掉；只要计数虚高一点（例如跳过分支重复累加 skipped_count），下面的
        //    不等式就会提前成立 → 文件夹被判终态 → 调用方随后 release_all_slots
        //    把正在跑的子任务连锅端掉。终态判定放宽成 `>=` 之后，这种越界不再自愈，
        //    这道闸门就更不能少。
        let owner_uid = {
            let folders = self.folders.read().await;
            match folders.get(folder_id) {
                Some(f) => f.owner_uid,
                None => return,
            }
        };
        if let Some(dm) = self.download_manager_for(owner_uid).await {
            let has_active = dm.get_tasks_by_group(folder_id).await.iter().any(|t| {
                t.status == TaskStatus::Downloading || t.status == TaskStatus::Pending
            });
            if has_active {
                return;
            }
        }

        let (should_persist, old_status) = {
            let mut folders = self.folders.write().await;
            let mut should_persist = false;
            let mut old_status = String::new();
            if let Some(folder) = folders.get_mut(folder_id) {
                // 检查是否全部完成（成功 + 失败 >= 总数）
                if folder.scan_completed
                    && folder.pending_files.is_empty()
                    && (folder.completed_count + folder.skipped_count + folder.failed_count)
                    >= folder.total_files
                    && folder.status != FolderStatus::Completed
                    && folder.status != FolderStatus::Failed
                    && folder.status != FolderStatus::Cancelled
                {
                    old_status = format!("{:?}", folder.status).to_lowercase();
                    if folder.failed_count > 0 {
                        folder.mark_failed(format!("{} 个文件下载失败", folder.failed_count));
                        info!(
                            "文件夹 {} 下载完成但有 {} 个失败 (completed={}, failed={})",
                            folder.name, folder.failed_count, folder.completed_count, folder.failed_count
                        );
                    } else {
                        folder.mark_completed();
                        info!("文件夹 {} 全部下载完成！", folder.name);
                    }
                    should_persist = true;
                }
            }
            (should_persist, old_status)
        };

        if !should_persist {
            return;
        }

        // 终态时更新持久化文件
        self.persist_folder(folder_id).await;

        // 🔥 清理取消令牌，避免内存泄漏
        self.cancellation_tokens.write().await.remove(folder_id);

        // 🔥 读取实际的新状态
        // 🔥 同时取出 owner_uid 用于事件
        let (new_status, owner_uid_raw_opt) = {
            let folders = self.folders.read().await;
            match folders.get(folder_id) {
                Some(f) => (
                    format!("{:?}", f.status).to_lowercase(),
                    Some(f.owner_uid.raw()),
                ),
                None => (String::new(), None),
            }
        };

        // 🔥 发布状态变更事件
        if !old_status.is_empty() {
            self.publish_event(FolderEvent::StatusChanged {
                folder_id: folder_id.to_string(),
                old_status,
                new_status: new_status.clone(),

                owner_uid: owner_uid_raw_opt,
            })
                .await;
        }

        // 🔥 根据实际状态发布对应事件
        if new_status == "completed" {
            self.publish_event(FolderEvent::Completed {
                folder_id: folder_id.to_string(),
                completed_at: chrono::Utc::now().timestamp_millis(),

                owner_uid: owner_uid_raw_opt,
            })
                .await;
        } else if new_status == "failed" {
            let error_msg = {
                let folders = self.folders.read().await;
                folders.get(folder_id)
                    .and_then(|f| f.error.clone())
                    .unwrap_or_default()
            };
            self.publish_event(FolderEvent::Failed {
                folder_id: folder_id.to_string(),
                error: error_msg,

                owner_uid: owner_uid_raw_opt,
            })
                .await;
        }
    }

    /// 🔥 触发借调位回收（按 owner_uid 严格过滤候选）
    ///
    /// 之前的版本不带 `owner_uid` 参数，遍历**所有**
    /// folder 找借调位 — 在多账号 per-uid `task_slot_pool` 设计下，调用方实际想
    /// 释放的是自己 `owner_uid` 那个池里的借调位；选中其它账号的 folder 来暂停/释放
    /// 不会让调用方的池得到空闲槽位（slot_pool 不共享），同时还误暂停了别账号的
    /// 子任务。本方法现在：候选 folder 必须 `f.owner_uid == owner_uid`，没有匹配
    /// folder 直接返回 `None`（让调用方走等待队列）。
    ///
    /// 当新任务需要槽位但没有空闲时调用此方法，从 **同一账号** 的文件夹回收一个借调位
    /// 流程：
    /// 1. 查找该账号下有借调位的文件夹
    /// 2. 选择一个使用借调位的子任务
    /// 3. 暂停该子任务并等待分片完成
    /// 4. 释放借调位
    /// 5. 返回释放的槽位ID
    pub async fn reclaim_borrowed_slot_for_owner(
        &self,
        owner_uid: crate::auth::Uid,
    ) -> Option<usize> {
        // 🔥 candidate 必须 owner_uid 匹配
        let candidate_folders: Vec<(String, crate::auth::Uid)> = {
            let folders_guard = self.folders.read().await;
            folders_guard
                .iter()
                .filter(|(_, f)| !f.borrowed_slot_ids.is_empty() && f.owner_uid == owner_uid)
                .map(|(id, f)| (id.clone(), f.owner_uid))
                .collect()
        };

        if candidate_folders.is_empty() {
            return None;
        }

        // 🔥 第一阶段：优先释放**完全空闲**的借调位（借来了但压根没有子任务在用）
        //
        // 借调发生在扫描之前，此时还不知道文件夹有几个文件，于是一律把空位借光。
        // 一个只含 1 个文件的文件夹会借走 4 个位却只用 1 个，剩下 3 个白占着。
        //
        // 历史实现只有在 `borrowed_subtask_map` **完全为空**时才会走"直接释放空闲借调位"
        // 分支；只要还有一个子任务占着借调位，它就转去暂停那个子任务，而明明空着的
        // 其余借调位永远回收不掉 —— 同账号第二个文件夹因此一个槽位都拿不到。
        for (folder_id, folder_owner_uid) in &candidate_folders {
            if let Some(slot_id) = self
                .release_idle_borrowed_slot(folder_id, *folder_owner_uid)
                .await
            {
                return Some(slot_id);
            }
        }

        // 🔥 第二阶段：借调位全都在用，只能暂停一个子任务把位置腾出来
        //
        // 逐个候选尝试：某个文件夹暂停失败（典型是子任务还在
        // `prepare_for_scheduling` 里探测链接、状态还不是 Downloading，
        // pause_task 会报"任务未在下载中"）时继续试下一个，
        // 而不是像历史实现那样直接 `return None` 整体放弃。
        for (folder_id, folder_owner_uid) in &candidate_folders {
            if let Some(slot_id) = self
                .reclaim_by_pausing_subtask(folder_id, *folder_owner_uid, owner_uid)
                .await
            {
                return Some(slot_id);
            }
        }

        None
    }

    /// 🔥 兜底：把跑在借调位上的子任务挪回文件夹自己空闲的固定位，腾出借调位
    ///
    /// **正常不该走到这里** —— 三条分配路径（新建补任务 ×2、恢复）都已经是
    /// 「固定位优先、借调位兜底」，子任务不会在固定位空着时去占借调位。
    /// 本方法只收拾分配之后才形成的错配：
    ///
    /// - 占着固定位的那个子任务先完成了，剩下的子任务还在借调位上跑 → 固定位空转
    /// - 历史遗留 / 未来新增路径漏了固定位优先（这正是 2026-08-21 14:24 日志里
    ///   恢复链路的表现：1 个文件的文件夹占着固定位 + 借调位两个槽，固定位没人用）
    ///
    /// 这类错配 [`release_surplus_borrowed_slots`](Self::release_surplus_borrowed_slots)
    /// 救不了 —— 那个借调位**正在被使用**，按设计不能直接收走。
    ///
    /// 这里做的是纯记账搬迁，不打断正在下载的分片：固定位和借调位在 `task_slot_pool`
    /// 里的 owner 都是 `folder_id`，槽位心跳不受影响；任务侧由
    /// [`DownloadManager::update_task_slot`] 切换成「文件夹固定位直持有」语义。
    async fn compact_folder_slots(&self, folder_id: &str) {
        let (owner_uid, fixed_slot_id) = {
            let folders_guard = self.folders.read().await;
            let Some(folder) = folders_guard.get(folder_id) else {
                return;
            };
            // 固定位已经有人用，或者压根没有固定位/借调位 → 无事可做
            if folder.fixed_slot_subtask.is_some() || folder.borrowed_slot_ids.is_empty() {
                return;
            }
            match folder.fixed_slot_id {
                Some(fixed) => (folder.owner_uid, fixed),
                None => return,
            }
        };

        let Some(dm) = self.download_manager_for(owner_uid).await else {
            return;
        };

        // 找一个正在用借调位的存活子任务
        let candidate = dm
            .get_tasks_by_group(folder_id)
            .await
            .into_iter()
            .find(|t| {
                t.is_borrowed_slot
                    && t.slot_id.is_some()
                    && !matches!(t.status, TaskStatus::Completed | TaskStatus::Failed)
            });
        let Some(task) = candidate else {
            return;
        };
        let Some(borrowed_slot) = task.slot_id else {
            return;
        };

        // 1. 先占住固定位，防止并发的补任务把它分给别人
        {
            let mut folders_guard = self.folders.write().await;
            let Some(folder) = folders_guard.get_mut(folder_id) else {
                return;
            };
            // 释放读锁到拿写锁之间可能已被占用，二次确认
            if folder.fixed_slot_subtask.is_some() {
                return;
            }
            folder.fixed_slot_subtask = Some(task.id.clone());
        }

        // 2. 任务侧切换到「文件夹固定位直持有」（slot_id=None, uses_folder_fixed_slot=true）
        dm.update_task_slot(&task.id, fixed_slot_id, false).await;

        // 3. 清掉借调位记账并归还给池子
        {
            let mut folders_guard = self.folders.write().await;
            if let Some(folder) = folders_guard.get_mut(folder_id) {
                folder.borrowed_subtask_map.remove(&task.id);
                folder.borrowed_slot_ids.retain(|&s| s != borrowed_slot);
            }
        }
        dm.task_slot_pool()
            .release_borrowed_slot(folder_id, borrowed_slot)
            .await;

        info!(
            "文件夹 {} 子任务 {} 从借调位 {} 挪回空闲的固定位 {}，借调位已归还",
            folder_id, task.id, borrowed_slot, fixed_slot_id
        );
    }

    /// 🔥 子任务已注册进持久化 → 把它对应的文件从 `pending_files` 摘掉
    ///
    /// 摘除时机之所以卡在"落盘"这一刻，是因为在此之前任务只活在内存里：
    /// `add_task` / `add_task_paused` 都不落盘，只有 `start_task_internal` 走到
    /// 注册持久化才算数。建任务时就摘掉的话，「已建任务、还没启动」的文件在重启后
    /// 既不在 `pending_files`、也没有任务记录 —— 实测一个 8 文件的文件夹重启后
    /// 只剩 2 个任务、0 待处理，另外 6 个文件永久丢失。
    ///
    /// 反过来，摘除推迟意味着这段时间里文件同时存在于 `pending_files` 和内存任务中，
    /// 补任务侧靠 `fs_id` 去重保证不会重复建。
    pub async fn drop_pending_file_after_persist(&self, folder_id: &str, fs_id: u64) {
        let removed = {
            let mut folders_guard = self.folders.write().await;
            let Some(folder) = folders_guard.get_mut(folder_id) else {
                return;
            };
            let before = folder.pending_files.len();
            folder.pending_files.retain(|f| f.fs_id != fs_id);
            before != folder.pending_files.len()
        };

        if !removed {
            return;
        }

        debug!(
            "文件夹 {} 子任务已落盘，从 pending_files 摘除 fs_id={}",
            folder_id, fs_id
        );
        self.persist_folder(folder_id).await;
    }

    /// 🔥 归还文件夹用不上的借调位
    ///
    /// 借调位是在扫描之前一次性借光的，扫描后才知道文件夹到底有几个文件。
    /// 这里按「还剩多少活要干」把多余的借调位还回池子：
    ///
    /// ```text
    /// 还需要的槽位数 = 存活子任务数 + pending_files 剩余数
    /// 可以还掉的     = 空闲借调位（没有子任务在用），且持有量超出所需的部分
    /// ```
    ///
    /// 只还空闲的借调位，正在跑的子任务不受影响；固定位不在归还范围内。
    async fn release_surplus_borrowed_slots(&self, folder_id: &str) {
        let (folder_owner_uid, borrowed_slot_ids, pending_left) = {
            let folders_guard = self.folders.read().await;
            match folders_guard.get(folder_id) {
                Some(f) if !f.borrowed_slot_ids.is_empty() => (
                    f.owner_uid,
                    f.borrowed_slot_ids.clone(),
                    f.pending_files.len(),
                ),
                _ => return,
            }
        };

        let Some(dm) = self.download_manager_for(folder_owner_uid).await else {
            return;
        };

        let live_tasks = dm.get_tasks_by_group(folder_id).await;
        let live_count = live_tasks
            .iter()
            .filter(|t| !matches!(t.status, TaskStatus::Completed | TaskStatus::Failed))
            .count();

        // 固定位算 1 个，剩下的才需要借调位来补
        let needed_borrowed = (live_count + pending_left).saturating_sub(1);
        if borrowed_slot_ids.len() <= needed_borrowed {
            return;
        }

        let mut surplus = borrowed_slot_ids.len() - needed_borrowed;
        let mut released = Vec::new();

        while surplus > 0 {
            match self
                .release_idle_borrowed_slot(folder_id, folder_owner_uid)
                .await
            {
                Some(slot_id) => {
                    released.push(slot_id);
                    surplus -= 1;
                }
                // 没有空闲借调位了（剩下的都在用），停手
                None => break,
            }
        }

        if !released.is_empty() {
            info!(
                "文件夹 {} 归还 {} 个用不上的借调位: {:?}（存活子任务 {}, 待下载 {}）",
                folder_id,
                released.len(),
                released,
                live_count,
                pending_left
            );
            // 不在这里主动推等待队列：try_start_waiting_tasks -> reclaim -> 本方法
            // 会构成 async 递归。归还的槽位由每秒一次的等待队列监控接手即可。
        }
    }

    /// 🔥 找出一个「借来了但没有任何子任务在用」的借调位
    ///
    /// 判定空闲需要同时满足两个条件，缺一不可：
    /// - 不在 `borrowed_subtask_map` 的映射里
    /// - 没有该文件夹的存活子任务把它记在 `task.slot_id` 上
    ///
    /// 第二条是必需的：恢复链路上存在「子任务已占用借调位但 map 未登记」的情况，
    /// 只看 map 会把正在下载的槽位当成空闲。
    async fn find_idle_borrowed_slot(
        &self,
        folder_id: &str,
        folder_owner_uid: crate::auth::Uid,
    ) -> Option<usize> {
        let dm = self.download_manager_for(folder_owner_uid).await?;

        // 该文件夹存活子任务实际占用的借调槽位
        let slots_in_use: std::collections::HashSet<usize> = dm
            .get_tasks_by_group(folder_id)
            .await
            .iter()
            .filter(|t| !matches!(t.status, TaskStatus::Completed | TaskStatus::Failed))
            .filter_map(|t| if t.is_borrowed_slot { t.slot_id } else { None })
            .collect();

        let folders_guard = self.folders.read().await;
        let folder = folders_guard.get(folder_id)?;
        folder
            .borrowed_slot_ids
            .iter()
            .copied()
            .find(|slot_id| {
                !folder.borrowed_subtask_map.values().any(|s| s == slot_id)
                    && !slots_in_use.contains(slot_id)
            })
    }

    /// 🔥 为排队中的文件夹子任务取得一个**借调位**
    ///
    /// 按「顶层任务保底 1 个固定位、剩余容量可借调、新任务进来时借调方要还」这个模型，
    /// 子任务的并行度应该来自借调位，而不是去占不可归还的全局固定位。
    ///
    /// 取位顺序：
    /// 1. 复用本文件夹**已有但空闲**的借调位 —— 等待队列此前只试过文件夹固定位，
    ///    自己借来闲着的位反而没人用
    /// 2. 池子里还有空闲槽 → 补借一个（借调只在文件夹创建/恢复那一刻发生一次，
    ///    之后即使空出槽位也没有任何路径补借，排队子任务只能干等）
    /// 3. 仍然没有 → 回收**别的**文件夹「借了没用」的借调位，再补借
    ///
    /// 成功时已登记 `borrowed_subtask_map`，调用方只需写任务侧的
    /// `slot_id` / `is_borrowed_slot`。
    pub async fn acquire_borrowed_slot_for_subtask(
        &self,
        folder_id: &str,
        task_id: &str,
    ) -> Option<usize> {
        let (owner_uid, backup_config_id) = {
            let folders_guard = self.folders.read().await;
            let folder = folders_guard.get(folder_id)?;
            if matches!(
                folder.status,
                FolderStatus::Paused | FolderStatus::Cancelled | FolderStatus::Failed
            ) {
                return None;
            }
            (folder.owner_uid, folder.backup_config_id.clone())
        };

        let dm = self.download_manager_for(owner_uid).await?;
        let slot_pool = dm.task_slot_pool();
        let priority = folder_slot_priority(backup_config_id.as_deref());
        let is_normal = priority == crate::task_slot_pool::TaskPriority::Normal;

        // 1. 先看本文件夹有没有借来却闲着的位
        let mut slot_id = self.find_idle_borrowed_slot(folder_id, owner_uid).await;

        // 2. 没有就从池子补借一个
        if slot_id.is_none() {
            let newly = if is_normal {
                // Normal 可抢占备份任务的槽位；Backup 只用空闲槽
                slot_pool.allocate_borrowed_slots(folder_id, 1).await.0
            } else {
                slot_pool
                    .allocate_borrowed_slots_no_preempt(folder_id, 1)
                    .await
            };
            if let Some(&sid) = newly.first() {
                let mut folders_guard = self.folders.write().await;
                if let Some(folder) = folders_guard.get_mut(folder_id) {
                    folder.borrowed_slot_ids.push(sid);
                }
                slot_id = Some(sid);
                info!("文件夹 {} 为排队子任务补借 1 个任务位: slot_id={}", folder_id, sid);
            }
        }

        // 3. 池子也空了 → 回收别的文件夹闲置的借调位再补借
        if slot_id.is_none() && is_normal {
            let peers: Vec<(String, crate::auth::Uid)> = {
                let folders_guard = self.folders.read().await;
                folders_guard
                    .iter()
                    .filter(|(id, f)| {
                        id.as_str() != folder_id
                            && f.owner_uid == owner_uid
                            && !f.borrowed_slot_ids.is_empty()
                    })
                    .map(|(id, f)| (id.clone(), f.owner_uid))
                    .collect()
            };

            for (peer_id, peer_uid) in peers {
                if let Some(freed) = self.release_idle_borrowed_slot(&peer_id, peer_uid).await {
                    info!(
                        "文件夹 {} 从文件夹 {} 回收空闲借调位 {}，转为自己的借调位",
                        folder_id, peer_id, freed
                    );
                    let newly = slot_pool.allocate_borrowed_slots(folder_id, 1).await.0;
                    if let Some(&sid) = newly.first() {
                        let mut folders_guard = self.folders.write().await;
                        if let Some(folder) = folders_guard.get_mut(folder_id) {
                            folder.borrowed_slot_ids.push(sid);
                        }
                        slot_id = Some(sid);
                    }
                    break;
                }
            }
        }

        let sid = slot_id?;
        self.register_subtask_borrowed_slot(folder_id, task_id, sid)
            .await;
        info!("排队子任务 {} 取得借调位: slot_id={}", task_id, sid);
        Some(sid)
    }

    /// 🔥 释放一个「借来了但没有任何子任务在用」的借调位
    async fn release_idle_borrowed_slot(
        &self,
        folder_id: &str,
        folder_owner_uid: crate::auth::Uid,
    ) -> Option<usize> {
        let idle_slot = self
            .find_idle_borrowed_slot(folder_id, folder_owner_uid)
            .await?;
        let dm = self.download_manager_for(folder_owner_uid).await?;

        {
            let mut folders_guard = self.folders.write().await;
            if let Some(folder) = folders_guard.get_mut(folder_id) {
                folder.borrowed_slot_ids.retain(|&id| id != idle_slot);
            }
        }
        dm.task_slot_pool()
            .release_borrowed_slot(folder_id, idle_slot)
            .await;

        info!(
            "回收：直接释放空闲借调位 slot_id={}（文件夹 {} 借了没用）",
            idle_slot, folder_id
        );

        // 🔥 释放槽位后不触发 try_start_waiting_tasks
        // 因为这个槽位是要给新任务用的，不是给等待队列的
        Some(idle_slot)
    }

    /// 🔥 暂停一个占着借调位的子任务，把该借调位腾出来
    ///
    /// 失败（没有可暂停的子任务 / 暂停被拒）时返回 `None`，由调用方去试下一个候选文件夹。
    async fn reclaim_by_pausing_subtask(
        &self,
        folder_id: &str,
        folder_owner_uid: crate::auth::Uid,
        owner_uid: crate::auth::Uid,
    ) -> Option<usize> {
        let folder_id = folder_id.to_string();
        debug_assert_eq!(
            folder_owner_uid, owner_uid,
            "reclaim_borrowed_slot_for_owner: 候选 folder owner 与请求 owner 不一致"
        );
        let dm = self.download_manager_for(folder_owner_uid).await?;
        let slot_pool = dm.task_slot_pool();
        info!(
            "触发借调位回收：owner_uid={}, folder={}",
            owner_uid.raw(),
            folder_id
        );

        // 获取该文件夹的借调位子任务映射
        let subtask_to_pause = {
            let folders_guard = self.folders.read().await;
            let folder = folders_guard.get(&folder_id)?;

            // 从 borrowed_subtask_map 中选择第一个
            folder.borrowed_subtask_map.keys().next().cloned()
        };

        let task_id = match subtask_to_pause {
            Some(id) => id,
            None => {
                // borrowed_subtask_map 为空，但可能有正在运行的子任务
                // 从调度器中找到该文件夹正在下载的子任务
                let tasks = dm.get_tasks_by_group(&folder_id).await;
                let running_task = tasks.iter().find(|t| t.status == TaskStatus::Downloading);

                let task = running_task?;
                info!(
                    "borrowed_subtask_map 为空，从调度器找到正在运行的子任务: {}",
                    task.id
                );
                task.id.clone()
            }
        };

        // 🔥 暂停**之前**先锁定这个子任务占的是哪个借调位
        //
        // `pause_task` 内部会走 `release_task_slot_by_kind` → `release_subtask_borrowed_slot`，
        // 把 `borrowed_subtask_map` 里的映射清掉（只清映射，借调位仍归文件夹所有）。
        // 等暂停完成后再去查映射就已经没有了 —— 历史实现在这时会退到
        // `borrowed_slot_ids.first()` 盲选一个，实测（2026-08-21 14:46 日志）
        // 暂停的是占着槽位 3 的子任务，却把槽位 1 释放了出去，而槽位 1 上的子任务
        // 还在下载：它就此占着一个已经转让给别人的槽位，活跃任务数 6 > 槽位上限 5。
        let target_slot: Option<usize> = {
            let from_map = {
                let folders_guard = self.folders.read().await;
                folders_guard
                    .get(&folder_id)
                    .and_then(|f| f.borrowed_subtask_map.get(&task_id).copied())
            };
            match from_map {
                Some(slot) => Some(slot),
                // map 里没有（恢复链路可能没登记）→ 用任务自己记的借调位
                None => dm
                    .get_task(&task_id)
                    .await
                    .filter(|t| t.is_borrowed_slot)
                    .and_then(|t| t.slot_id),
            }
        };

        let Some(target_slot) = target_slot else {
            warn!(
                "子任务 {} 未持有可识别的借调位，跳过该候选（不再盲选 borrowed_slot_ids）",
                task_id
            );
            return None;
        };

        info!(
            "回收流程：暂停借调子任务 {}（持有借调位 {}）",
            task_id, target_slot
        );

        // 暂停子任务（skip_try_start_waiting=true，不触发等待队列启动）
        // 🔥 关键修复：回收借调槽位时，槽位是给新任务预留的，不应让等待队列抢占
        if let Err(e) = dm.pause_task(&task_id, true).await {
            // 典型原因：子任务还卡在 prepare_for_scheduling（Locate + 链接探测）里，
            // 状态还不是 Downloading。这不是错误，只是这个候选暂时腾不出位置，
            // 交给调用方去试下一个文件夹。
            warn!("暂停任务失败（跳过该候选）: {}, task_id: {}", e, task_id);
            return None;
        }

        // 等待任务暂停完成（所有运行中分片完成）
        Self::wait_for_task_paused(&dm, &task_id).await;

        // 🔥 释放的必须是上面锁定的那个槽位
        //
        // 这里**不能**再退回「从 borrowed_slot_ids 取第一个」：那个槽位很可能正被
        // 同文件夹另一个还在下载的子任务使用，放出去就等于超发。
        let slot_id = target_slot;
        {
            let mut folders_guard = self.folders.write().await;
            let folder = folders_guard.get_mut(&folder_id)?;
            folder.borrowed_subtask_map.remove(&task_id);
            folder.borrowed_slot_ids.retain(|&id| id != slot_id);
        }

        // 释放到任务位池
        slot_pool.release_borrowed_slot(&folder_id, slot_id).await;

        info!(
            "回收完成：释放借调位 {} 从文件夹 {}",
            slot_id, folder_id
        );

        // 🔥 关键修复：将被暂停的子任务重新加入等待队列
        // 子任务不应该一直暂停，而是重新排队等待后续有空闲槽位时继续下载
        if let Err(e) = dm.requeue_paused_task(&task_id).await {
            warn!("重新入队暂停任务失败: {}, task_id: {}", e, task_id);
        } else {
            info!("子任务 {} 已重新加入等待队列", task_id);
        }

        // 🔥 修复：释放槽位后不触发 try_start_waiting_tasks
        // 因为这个槽位是要给新任务用的，不是给等待队列的
        // dm.try_start_waiting_tasks().await; // 已移除

        Some(slot_id)
    }

    /// 等待任务暂停完成（所有运行中分片完成）
    async fn wait_for_task_paused(dm: &DownloadManager, task_id: &str) {
        use tokio::time::{interval, Duration};

        let mut check_interval = interval(Duration::from_millis(100));

        for _ in 0..100 {
            // 最多等待10秒
            check_interval.tick().await;

            if let Some(task) = dm.get_task(task_id).await {
                if task.status == TaskStatus::Paused {
                    info!("任务 {} 所有分片已完成，已暂停", task_id);
                    return;
                }
            }
        }

        warn!("任务 {} 暂停超时（10秒），强制继续", task_id);
    }

    /// 🔥 注册子任务使用的借调位
    ///
    /// 当子任务开始使用借调位时调用，记录映射关系
    pub async fn register_subtask_borrowed_slot(
        &self,
        folder_id: &str,
        task_id: &str,
        slot_id: usize,
    ) {
        let mut folders_guard = self.folders.write().await;
        if let Some(folder) = folders_guard.get_mut(folder_id) {
            folder.borrowed_subtask_map.insert(task_id.to_string(), slot_id);
            info!(
                "注册子任务借调位: folder={}, task={}, slot={}",
                folder_id, task_id, slot_id
            );
        }
    }

    /// 🔥 尝试将文件夹固定槽位分配给指定子任务
    ///
    /// 返回 true 表示分配成功，false 表示已被占用。
    /// 同一时刻最多只有一个子任务能占用固定槽位。
    /// 🔥 为「创建时没抢到固定位」的文件夹补发固定位（重试补偿）
    ///
    /// 设计上每个文件夹都应保底持有 1 个固定槽位（见创建路径的注释：
    /// *"这确保了多个文件夹任务之间的公平性：每个文件夹至少能获得一个固定位"*），
    /// 但那只是**创建时试一次**：抢不到就打一条
    /// `无法获得固定任务位，将在有空位时重试` 的 warn 然后再也没人重试
    /// —— [`try_allocate_fixed_slot_for_subtask`] 的注释里把这个缺口写成
    /// "而且没有任何重试补偿"。
    ///
    /// 后果：另一个文件夹只要抢先把槽位借光，后来的文件夹就永远拿不到固定位，
    /// 它的子任务全部堆在等待队列里空转（实测 2026-08-21 10:06 日志）。
    ///
    /// 本方法挂在等待队列监控上（每秒驱动一次），补上那次重试：
    /// 先直接申请，拿不到就回收一个借调位再申请。
    ///
    /// 与创建路径保持一致的两条约束：
    /// - 只有 Normal 优先级（用户手动发起）才触发回收；Backup（自动备份 / 分享同步）
    ///   只用空闲槽，不去削减别人已有的并行度
    /// - 已持有固定位、或文件夹已进入非活跃状态时直接返回，不做任何事
    ///
    /// # 返回
    /// 文件夹当前是否持有固定槽位
    pub async fn ensure_folder_fixed_slot(&self, folder_id: &str) -> bool {
        let (owner_uid, backup_config_id) = {
            let folders_guard = self.folders.read().await;
            let Some(folder) = folders_guard.get(folder_id) else {
                return false;
            };
            // 已经有固定位，无需补发
            if folder.fixed_slot_id.is_some() {
                return true;
            }
            // 非活跃状态的文件夹不占槽位
            if matches!(
                folder.status,
                FolderStatus::Paused | FolderStatus::Cancelled | FolderStatus::Failed
            ) {
                return false;
            }
            (folder.owner_uid, folder.backup_config_id.clone())
        };

        let Some(dm) = self.download_manager_for(owner_uid).await else {
            return false;
        };
        let slot_pool = dm.task_slot_pool();
        let slot_priority = folder_slot_priority(backup_config_id.as_deref());

        // 第一次尝试：直接申请
        let mut allocated = slot_pool
            .allocate_fixed_slot_with_priority(folder_id, true, slot_priority)
            .await;

        // 拿不到就回收一个借调位再试（仅 Normal 优先级）
        if allocated.is_none() && slot_priority == crate::task_slot_pool::TaskPriority::Normal {
            if let Some(reclaimed) = self.reclaim_borrowed_slot_for_owner(owner_uid).await {
                info!(
                    "文件夹 {} 补发固定位：回收到借调位 {}，重新申请",
                    folder_id, reclaimed
                );
                allocated = slot_pool
                    .allocate_fixed_slot_with_priority(folder_id, true, slot_priority)
                    .await;
            }
        }

        let Some((slot_id, preempted)) = allocated else {
            return false;
        };

        {
            let mut folders_guard = self.folders.write().await;
            match folders_guard.get_mut(folder_id) {
                Some(folder) => folder.fixed_slot_id = Some(slot_id),
                None => {
                    // 文件夹在申请过程中被删了，归还槽位避免泄漏
                    drop(folders_guard);
                    slot_pool.release_fixed_slot(folder_id).await;
                    return false;
                }
            }
        }

        if let Some(preempted_id) = preempted {
            info!("文件夹 {} 补发固定位时抢占了槽位持有者 {}", folder_id, preempted_id);
            dm.handle_preempted_slot_owner(&preempted_id).await;
        }

        info!(
            "✅ 文件夹 {} 补发固定任务位成功: slot_id={}（创建时未抢到）",
            folder_id, slot_id
        );
        true
    }

    pub async fn try_allocate_fixed_slot_for_subtask(
        &self,
        folder_id: &str,
        task_id: &str,
    ) -> bool {
        let mut folders_guard = self.folders.write().await;
        match folders_guard.get_mut(folder_id) {
            Some(folder) => {
                // 🔥 发放「文件夹固定槽位」必须同时满足两个条件：
                //   1. 文件夹自己**确实持有**一个 fixed_slot_id
                //   2. 该固定位还没被别的子任务占用
                //
                // 条件 1 原本缺失，只判了条件 2。后果（issue #138 实测确认）：
                // 文件夹创建时若抢不到槽位（例如只有 1 个任务槽、且已被另一个文件夹
                // 以 Normal 优先级持有 —— Normal 抢不动 Normal），fixed_slot_id 会是
                // None，而且没有任何重试补偿。此时仍然返回 true 的话，子任务会带着
                // `uses_folder_fixed_slot=true` 被拉起，却不占用 task_slot_pool 的任何
                // 槽位 —— 直接绕过任务槽上限进入活跃集合。
                //
                // 实测日志（1 个任务槽、3 个目录分享同步）：
                //   文件夹 4803c7cd 无法获得固定任务位
                //   → 分配文件夹固定槽位: folder=4803c7cd, folder_fixed_slot_id=None
                //   → ⚠️ 活跃任务数 2 超过任务槽上限 1
                // 两个任务随后在 ChunkScheduler 里 round-robin 瓜分唯一的下载线程，
                // 表现为多个同步都「在下载」但交替推进、谁也不快。
                //
                // 加上条件 1 后，这类子任务会回落到调用方的全局槽位分配路径，
                // 拿不到就正常进等待队列，等持有者释放后再按序拉起。
                match (folder.fixed_slot_id, folder.fixed_slot_subtask.is_none()) {
                    (Some(fixed_slot_id), true) => {
                        folder.fixed_slot_subtask = Some(task_id.to_string());
                        info!(
                            "分配文件夹固定槽位: folder={}, task={}, slot_id={}",
                            folder_id, task_id, fixed_slot_id
                        );
                        true
                    }
                    (None, _) => {
                        // 文件夹自己都没槽位，发不出去。
                        // 用 debug 而非 warn：等待队列监控每秒都会重试排队中的子任务，
                        // 这里会被高频命中，warn 会刷屏。
                        debug!(
                            "文件夹 {} 未持有固定槽位，子任务 {} 不走文件夹固定位（回落到全局槽位分配 / 等待队列）",
                            folder_id, task_id
                        );
                        false
                    }
                    // 固定位已被别的子任务占用
                    (Some(_), false) => false,
                }
            }
            None => false,
        }
    }

    /// 🔥 该 id 是否是一个已知的文件夹下载。
    ///
    /// 用于区分「被抢占的槽位持有者」到底是单文件下载任务还是文件夹：
    /// 文件夹主任务持有固定位时，`task_slot_pool` 里记录的 owner 是 **folder_id**，
    /// 不是下载任务 id。
    pub async fn is_known_folder(&self, folder_id: &str) -> bool {
        self.folders.read().await.contains_key(folder_id)
    }

    /// 🔥 查询文件夹子任务申请全局槽位时应使用的优先级。
    ///
    /// 子任务的优先级必须**跟随所属文件夹**：后台文件夹（自动备份 / 分享同步，
    /// 带 `backup_config_id`）的子任务同样是后台任务，不该抢占别人的槽位。
    ///
    /// 否则会出现"优先级降一层再现"：文件夹主任务已按 `folder_slot_priority`
    /// 降为 `Backup`，但它的子任务在回落到全局槽位分配时若仍用 `SubTask`(20)，
    /// 照样能抢占 `Backup`(30) —— 后台同步依旧会把别的后台下载踢下槽位。
    ///
    /// 文件夹不存在时返回 `SubTask`（维持旧行为，不影响非文件夹路径）。
    pub async fn subtask_slot_priority(
        &self,
        folder_id: &str,
    ) -> crate::task_slot_pool::TaskPriority {
        let folders = self.folders.read().await;
        match folders.get(folder_id) {
            Some(f) if f.backup_config_id.is_some() => crate::task_slot_pool::TaskPriority::Backup,
            _ => crate::task_slot_pool::TaskPriority::SubTask,
        }
    }

    /// 🔥 查询指定文件夹的固定槽位 ID（若存在）
    ///
    /// 用于 [`DownloadManager::update_task_slot`] 等外部路径判断"本次写入的 slot_id
    /// 是否就是文件夹的 fixed_slot_id"，从而决定是否需要同步调用
    /// [`Self::set_fixed_slot_subtask`] 登记占用关系。
    pub async fn folder_fixed_slot_id(&self, folder_id: &str) -> Option<usize> {
        let folders_guard = self.folders.read().await;
        folders_guard
            .get(folder_id)
            .and_then(|f| f.fixed_slot_id)
    }

    /// 🔥 幂等登记子任务对文件夹固定槽位的占用
    ///
    /// 用途：恢复 / 补任务路径在把某个子任务的 `slot_id` 直接写成 `fixed_slot_id` 时，
    /// 必须同步把 `fixed_slot_subtask` 指向该子任务，否则
    /// [`try_allocate_fixed_slot_for_subtask`] 会因为 `fixed_slot_subtask == None`
    /// 把同一个文件夹固定槽位再分配给另一个等待中的子任务，造成"同一槽位双占"。
    ///
    /// 语义：
    /// - `fixed_slot_subtask == None`              → 直接写入该子任务
    /// - `fixed_slot_subtask == Some(task_id)`     → 无变化（幂等）
    /// - `fixed_slot_subtask == Some(other)`       → 不覆盖；返回 `false` 供调用方诊断
    ///
    /// 返回 `true` 表示登记后 `fixed_slot_subtask` 指向该子任务；`false` 表示已被别人占用。
    pub async fn set_fixed_slot_subtask(&self, folder_id: &str, task_id: &str) -> bool {
        let mut folders_guard = self.folders.write().await;
        match folders_guard.get_mut(folder_id) {
            Some(folder) => {
                match folder.fixed_slot_subtask.as_deref() {
                    None => {
                        folder.fixed_slot_subtask = Some(task_id.to_string());
                        debug!(
                            "set_fixed_slot_subtask: folder={} 登记子任务 {} 占用固定槽位",
                            folder_id, task_id
                        );
                        true
                    }
                    Some(existing) if existing == task_id => {
                        // 幂等：已经是同一个任务
                        true
                    }
                    Some(existing) => {
                        warn!(
                            "set_fixed_slot_subtask: folder={} 固定槽位已被任务 {} 占用，拒绝重复登记 {}",
                            folder_id, existing, task_id
                        );
                        false
                    }
                }
            }
            None => false,
        }
    }

    /// 🔥 释放指定子任务占用的文件夹固定槽位
    ///
    /// 仅当当前占用者与 task_id 匹配时才释放，避免误释放。
    pub async fn release_fixed_slot_from_subtask(&self, folder_id: &str, task_id: &str) {
        let mut folders_guard = self.folders.write().await;
        if let Some(folder) = folders_guard.get_mut(folder_id) {
            if folder.fixed_slot_subtask.as_deref() == Some(task_id) {
                folder.fixed_slot_subtask = None;
                info!(
                    "释放文件夹固定槽位: folder={}, task={}",
                    folder_id, task_id
                );
            }
        }
    }

    /// 🔥 释放子任务对借调槽位的占用（保留借调位归属文件夹，仅清除子任务映射）
    ///
    /// 与子任务完成路径不同：完成路径会把借调位归还到 task_slot_pool；
    /// 此方法用于 `auto_requeue` 等"子任务退回但文件夹仍需保留借调位"场景，
    /// 仅从 `borrowed_subtask_map` 移除映射，使该借调位可被同文件夹的其他子任务复用。
    ///
    /// 返回原本占用的 slot_id 供调用方记录日志或后续处理。
    pub async fn release_subtask_borrowed_slot(
        &self,
        folder_id: &str,
        task_id: &str,
    ) -> Option<usize> {
        let mut folders_guard = self.folders.write().await;
        if let Some(folder) = folders_guard.get_mut(folder_id) {
            let removed = folder.borrowed_subtask_map.remove(task_id);
            if let Some(slot_id) = removed {
                info!(
                    "释放子任务借调位映射: folder={}, task={}, slot={}",
                    folder_id, task_id, slot_id
                );
            }
            removed
        } else {
            None
        }
    }

    /// 🔥 查询文件夹固定槽位当前占用者
    pub async fn get_fixed_slot_subtask(&self, folder_id: &str) -> Option<String> {
        let folders_guard = self.folders.read().await;
        folders_guard
            .get(folder_id)
            .and_then(|f| f.fixed_slot_subtask.clone())
    }

    /// 🔥 释放文件夹的所有槽位
    ///
    /// 当文件夹任务完成或取消时调用。
    ///
    /// # 与 `cancel_tasks_by_group` 的分工
    ///
    /// 文件夹暂停 / 完成的槽位回收分两层互补处理：
    ///
    /// 1. **per-task 层**（由 `DownloadManager::cancel_tasks_by_group` 负责）：
    ///    对本 group 的每个子任务，按其当时持有的槽位 kind 调
    ///    `release_task_slot_by_kind`：
    ///    - 普通全局 fixed slot（owner=task_id，子任务 fallback 路径产物，
    ///      `slot.task_id != folder_id`）→ 释放 task_slot_pool 该 fixed slot
    ///    - 文件夹借调位 → 清 folder 端 `borrowed_subtask_map` 该 task 条目
    ///    - 文件夹固定位 → 清 folder 端 `fixed_slot_subtask`（仅当占用者匹配）
    ///
    /// 2. **per-folder 层**（本函数）：
    ///    - 释放 `task_slot_pool` 中所有 owner=folder_id 的槽位
    ///      （即剩余的借调位 + 文件夹固定位本身）
    ///    - 清 folder 自身的总映射（`fixed_slot_id` / `borrowed_slot_ids` /
    ///      `borrowed_subtask_map` / `fixed_slot_subtask`），保证恢复时
    ///      `try_allocate_fixed_slot_for_subtask` 等路径不会因为残留映射误判
    ///      "槽位已被某子任务占用"。
    ///
    /// 单独依赖第 2 层不够：`release_all_slots(folder_id)` 用
    /// `slot.task_id == folder_id` 比对，无法命中 fallback 到普通全局 fixed slot
    /// 的子任务持有的 owner=task_id 槽位，必须由第 1 层补释放。
    /// 单独依赖第 1 层也不够：第 1 层只清 task 自己持有的那一份，
    /// 借调位的 owner=folder_id 槽位本身、folder 端总映射仍要由第 2 层兜底。
    pub async fn release_folder_slots(&self, folder_id: &str) {
        // 🔥 按 folder.owner_uid 路由 download_manager
        let folder_owner_uid_for_routing = {
            let folders = self.folders.read().await;
            folders.get(folder_id).map(|f| f.owner_uid)
        };
        let dm = match folder_owner_uid_for_routing {
            Some(uid) => self.download_manager_for(uid).await,
            None => self.download_manager.read().await.clone(),
        };

        let dm = match dm {
            Some(dm) => dm,
            None => return,
        };

        let slot_pool = dm.task_slot_pool();

        // 释放所有 owner=folder_id 的槽位（剩余借调位 + 文件夹固定位）。
        // owner=task_id 的 fallback 全局 fixed slot 由 cancel_tasks_by_group
        // 在 per-task 层释放，本函数无需也无法处理。
        slot_pool.release_all_slots(folder_id).await;

        // 清理文件夹自身的总映射
        {
            let mut folders_guard = self.folders.write().await;
            if let Some(folder) = folders_guard.get_mut(folder_id) {
                folder.fixed_slot_id = None;
                folder.borrowed_slot_ids.clear();
                folder.borrowed_subtask_map.clear();
                folder.fixed_slot_subtask = None;
            }
        }

        info!("释放文件夹 {} 的所有槽位（per-folder 层：owner=folder_id 的槽位 + 文件夹端总映射）", folder_id);
    }

    /// 🔥 重命名加密文件夹并更新路径
    ///
    /// 在扫描完成后、创建任务前调用
    /// 按深度从深到浅排序后重命名，避免父文件夹先重命名导致子文件夹路径失效
    async fn rename_encrypted_folders_and_update_paths(&self, folder_id: &str) -> Result<()> {
        // 获取映射和 local_root
        let (mappings, local_root) = {
            let folders = self.folders.read().await;
            let folder = folders.get(folder_id).ok_or_else(|| anyhow!("文件夹不存在"))?;
            (folder.encrypted_folder_mappings.clone(), folder.local_root.clone())
        };

        if mappings.is_empty() {
            return Ok(());
        }

        info!("开始重命名加密文件夹: {} 个映射", mappings.len());

        // 按路径深度排序（从深到浅），确保先重命名子文件夹
        let mut sorted_mappings: Vec<_> = mappings.into_iter().collect();
        sorted_mappings.sort_by(|a, b| {
            let depth_a = a.0.matches('/').count();
            let depth_b = b.0.matches('/').count();
            depth_b.cmp(&depth_a) // 深度大的排前面
        });

        // 记录成功重命名的映射（用于更新 pending_files）
        let mut successful_renames: Vec<(String, String)> = Vec::new();

        for (encrypted_rel, decrypted_rel) in sorted_mappings {
            let encrypted_path = local_root.join(&encrypted_rel);
            let decrypted_path = local_root.join(&decrypted_rel);

            // 如果加密路径不存在，跳过（可能还没创建）
            if !encrypted_path.exists() {
                info!("加密文件夹不存在，跳过: {:?}", encrypted_path);
                continue;
            }

            // 如果解密路径已存在，需要合并
            if decrypted_path.exists() {
                info!("目标文件夹已存在，将合并: {:?}", decrypted_path);
                // 移动加密文件夹内的所有内容到解密文件夹
                if let Err(e) = self.merge_folders(&encrypted_path, &decrypted_path).await {
                    warn!("合并文件夹失败: {:?} -> {:?}, 错误: {}", encrypted_path, decrypted_path, e);
                    continue;
                }
            } else {
                // 确保父目录存在
                if let Some(parent) = decrypted_path.parent() {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        warn!("创建父目录失败: {:?}, 错误: {}", parent, e);
                        continue;
                    }
                }

                // 重命名文件夹
                if let Err(e) = tokio::fs::rename(&encrypted_path, &decrypted_path).await {
                    warn!("重命名文件夹失败: {:?} -> {:?}, 错误: {}", encrypted_path, decrypted_path, e);
                    continue;
                }
            }

            info!("重命名加密文件夹成功: {:?} -> {:?}", encrypted_path, decrypted_path);
            successful_renames.push((encrypted_rel, decrypted_rel));
        }

        // 更新 pending_files 中的路径
        if !successful_renames.is_empty() {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                for pending_file in &mut folder.pending_files {
                    for (encrypted_rel, decrypted_rel) in &successful_renames {
                        // 替换路径中的加密部分
                        if pending_file.relative_path.starts_with(encrypted_rel) {
                            let new_path = pending_file.relative_path
                                .replacen(encrypted_rel, decrypted_rel, 1);
                            info!(
                                "更新 pending_file 路径: {} -> {}",
                                pending_file.relative_path, new_path
                            );
                            pending_file.relative_path = new_path;
                        }
                    }
                }

                // 清空映射（已处理完毕）
                folder.encrypted_folder_mappings.clear();
            }
        }

        Ok(())
    }

    /// 合并文件夹：将 src 中的内容移动到 dst
    async fn merge_folders(&self, src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        let mut entries = tokio::fs::read_dir(src).await?;

        while let Some(entry) = entries.next_entry().await? {
            let src_path = entry.path();
            let file_name = entry.file_name();
            let dst_path = dst.join(&file_name);

            if src_path.is_dir() {
                if dst_path.exists() {
                    // 递归合并子目录
                    Box::pin(self.merge_folders(&src_path, &dst_path)).await?;
                } else {
                    // 直接移动目录
                    tokio::fs::rename(&src_path, &dst_path).await?;
                }
            } else {
                // 移动文件（如果目标存在则覆盖）
                if dst_path.exists() {
                    tokio::fs::remove_file(&dst_path).await?;
                }
                tokio::fs::rename(&src_path, &dst_path).await?;
            }
        }

        // 删除空的源目录
        tokio::fs::remove_dir(src).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 回归 issue #138：文件夹自己没拿到固定槽位时，绝不能把「文件夹固定槽位」
    /// 发给子任务 —— 否则子任务会带着 `uses_folder_fixed_slot=true` 被拉起，
    /// 不占用 task_slot_pool 的任何槽位，直接绕过任务槽上限。
    ///
    /// 复现条件（实测日志）：只有 1 个任务槽，且已被另一个文件夹以 Normal 优先级
    /// 持有。第二个文件夹 `allocate_fixed_slot_with_priority` 失败（Normal 抢不动
    /// Normal），`fixed_slot_id` 保持 None 且无重试补偿。
    #[tokio::test]
    async fn test_no_fixed_slot_grant_when_folder_holds_none() {
        let fm = FolderDownloadManager::new(PathBuf::from("."));

        // 没抢到槽位的文件夹：fixed_slot_id = None
        let mut slotless = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        slotless.fixed_slot_id = None;
        let slotless_id = slotless.id.clone();

        // 正常持有槽位的文件夹
        let mut holder = FolderDownload::new("/b".into(), PathBuf::from("./b"));
        holder.fixed_slot_id = Some(0);
        let holder_id = holder.id.clone();

        {
            let mut folders = fm.folders.write().await;
            folders.insert(slotless_id.clone(), slotless);
            folders.insert(holder_id.clone(), holder);
        }

        assert!(
            !fm.try_allocate_fixed_slot_for_subtask(&slotless_id, "child-1").await,
            "文件夹自己没有 fixed_slot_id，不能把固定槽位发给子任务"
        );
        // 拒绝之后不能留下占用痕迹，否则真拿到槽位时会误判为「已被占用」
        {
            let folders = fm.folders.read().await;
            assert!(
                folders.get(&slotless_id).unwrap().fixed_slot_subtask.is_none(),
                "拒绝发放后不应登记 fixed_slot_subtask"
            );
        }

        // 对照：持有槽位的文件夹正常发放，且只发一次
        assert!(
            fm.try_allocate_fixed_slot_for_subtask(&holder_id, "child-2").await,
            "文件夹持有 fixed_slot_id 时应正常发放"
        );
        assert!(
            !fm.try_allocate_fixed_slot_for_subtask(&holder_id, "child-3").await,
            "固定位已被占用，不能重复发放给第二个子任务"
        );
    }

    /// 构造一个挂好 DownloadManager 的 FolderDownloadManager（走 legacy 单账号路径）
    async fn fm_with_download_manager() -> (FolderDownloadManager, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let auth = crate::auth::UserAuth {
            uid: 123456789,
            username: "test_user".to_string(),
            nickname: None,
            avatar_url: None,
            vip_type: Some(2),
            vip_level: None,
            total_space: None,
            used_space: None,
            bduss: "mock_bduss".to_string(),
            stoken: None,
            ptoken: None,
            baiduid: None,
            passid: None,
            cookies: None,
            panpsc: None,
            csrf_token: None,
            bdstoken: None,
            login_time: 0,
            last_warmup_at: None,
            custom_config: Default::default(),
        };
        let dm = Arc::new(
            DownloadManager::new(auth, tmp.path().to_path_buf())
                .await
                .expect("构造测试 DownloadManager"),
        );
        let fm = FolderDownloadManager::new(tmp.path().to_path_buf());
        fm.set_download_manager(dm).await;
        (fm, tmp)
    }

    /// 回收借调位时，必须先释放**没人用**的那些，且绝不能碰正在用的
    ///
    /// 回归 2026-08-21 10:06 日志：文件夹借了 4 个位只用 1 个，另外 3 个空转；
    /// 历史实现只有在 `borrowed_subtask_map` 完全为空时才走「直接释放空闲借调位」，
    /// 于是这 3 个空位永远回收不掉，同账号第二个文件夹一个槽位都拿不到。
    #[tokio::test]
    async fn test_release_idle_borrowed_slot_skips_slots_in_use() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(0);
        folder.borrowed_slot_ids = vec![1, 2, 3, 4];
        // 只有 1 号位真的有子任务在用
        folder
            .borrowed_subtask_map
            .insert("subtask-on-slot-1".to_string(), 1);
        let folder_id = folder.id.clone();
        let owner_uid = folder.owner_uid;
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        // 连续回收：2/3/4 应被依次释放，1 号位始终保留
        let mut released = Vec::new();
        while let Some(slot) = fm.release_idle_borrowed_slot(&folder_id, owner_uid).await {
            released.push(slot);
            assert!(released.len() <= 4, "释放次数失控，可能把在用的位也放了");
        }
        released.sort();
        assert_eq!(released, vec![2, 3, 4], "只应释放没人用的借调位");

        let folders = fm.folders.read().await;
        assert_eq!(
            folders.get(&folder_id).unwrap().borrowed_slot_ids,
            vec![1],
            "正在被子任务使用的借调位必须保留"
        );
    }

    /// 扫描后发现用不了这么多槽位时，多借的要还回去
    ///
    /// 借调发生在扫描之前（还不知道有几个文件），一律把空位借光；
    /// 补任务跑完就该按「存活子任务 + 待下载文件」把多余的还掉。
    #[tokio::test]
    async fn test_release_surplus_borrowed_slots_trims_to_actual_need() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(0);
        folder.borrowed_slot_ids = vec![1, 2, 3, 4];
        // 只有 1 个文件，扫描后 pending 已清空，也没有子任务占着借调位
        folder.pending_files.clear();
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        fm.release_surplus_borrowed_slots(&folder_id).await;

        let folders = fm.folders.read().await;
        assert!(
            folders.get(&folder_id).unwrap().borrowed_slot_ids.is_empty(),
            "只有 1 个文件的文件夹不该继续占着 4 个借调位（固定位已够用）"
        );
    }

    /// 固定位已被占用时不做搬迁（否则会把两个子任务塞进同一个固定位）
    #[tokio::test]
    async fn test_compact_folder_slots_noop_when_fixed_slot_occupied() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(1);
        folder.fixed_slot_subtask = Some("someone-else".to_string());
        folder.borrowed_slot_ids = vec![2];
        folder.borrowed_subtask_map.insert("sub".to_string(), 2);
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        fm.compact_folder_slots(&folder_id).await;

        let folders = fm.folders.read().await;
        let folder = folders.get(&folder_id).unwrap();
        assert_eq!(folder.fixed_slot_subtask.as_deref(), Some("someone-else"));
        assert_eq!(folder.borrowed_slot_ids, vec![2], "不该动借调位");
        assert_eq!(folder.borrowed_subtask_map.get("sub"), Some(&2));
    }

    /// 没有子任务真的在用借调位时，不能凭空把固定位标成已占用
    ///
    /// 这条守住的是最危险的一种写坏：先占住 `fixed_slot_subtask` 再去找搬迁对象，
    /// 找不到就必须原样退出，否则文件夹的固定位会被一个不存在的任务永久占住。
    #[tokio::test]
    async fn test_compact_folder_slots_noop_without_borrowed_subtask() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(1);
        folder.fixed_slot_subtask = None;
        // 借调位记在文件夹上，但没有任何存活子任务在用
        folder.borrowed_slot_ids = vec![2];
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        fm.compact_folder_slots(&folder_id).await;

        let folders = fm.folders.read().await;
        let folder = folders.get(&folder_id).unwrap();
        assert!(
            folder.fixed_slot_subtask.is_none(),
            "没找到搬迁对象就必须原样退出，不能把固定位标成已占用"
        );
        assert_eq!(folder.borrowed_slot_ids, vec![2]);
    }

    /// 排队子任务应先复用本文件夹「借来却闲着」的借调位
    ///
    /// 回归 2026-08-21 11:17 日志：等待队列里的文件夹子任务只试过文件夹**固定位**，
    /// 自己借来闲着的位反而没人用，于是 3 个借调位空转、子任务每秒重试却起不来。
    #[tokio::test]
    async fn test_queued_subtask_reuses_own_idle_borrowed_slot() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(0);
        // 固定位已被同组第一个子任务占着
        folder.fixed_slot_subtask = Some("subtask-on-fixed".to_string());
        folder.borrowed_slot_ids = vec![1, 3, 4];
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        let got = fm
            .acquire_borrowed_slot_for_subtask(&folder_id, "queued-subtask")
            .await;
        assert!(
            matches!(got, Some(1) | Some(3) | Some(4)),
            "应复用已有的空闲借调位，实际 {got:?}"
        );

        // 已登记映射，后续调用不能把同一个位再发一次
        let folders = fm.folders.read().await;
        let folder = folders.get(&folder_id).unwrap();
        assert_eq!(
            folder.borrowed_subtask_map.get("queued-subtask"),
            got.as_ref()
        );
        // 没有凭空多借
        assert_eq!(folder.borrowed_slot_ids.len(), 3);
    }

    /// 同一个借调位不能同时发给两个排队子任务
    #[tokio::test]
    async fn test_borrowed_slot_not_handed_out_twice() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(0);
        folder.fixed_slot_subtask = Some("subtask-on-fixed".to_string());
        folder.borrowed_slot_ids = vec![1];
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        let first = fm
            .acquire_borrowed_slot_for_subtask(&folder_id, "sub-1")
            .await;
        assert_eq!(first, Some(1));

        // 池子是空的（测试里没有真正借到新槽），第二个子任务应拿不到
        let second = fm
            .acquire_borrowed_slot_for_subtask(&folder_id, "sub-2")
            .await;
        assert_ne!(second, Some(1), "同一个借调位不能重复发放");
    }

    /// 已暂停 / 已取消的文件夹不该去抢借调位
    #[tokio::test]
    async fn test_acquire_borrowed_slot_skips_inactive_folder() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.status = FolderStatus::Cancelled;
        folder.borrowed_slot_ids = vec![1, 2];
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        assert_eq!(
            fm.acquire_borrowed_slot_for_subtask(&folder_id, "sub").await,
            None
        );
    }

    /// 已暂停 / 已取消的文件夹不该去抢固定位
    #[tokio::test]
    async fn test_ensure_folder_fixed_slot_skips_inactive_folder() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = None;
        folder.status = FolderStatus::Paused;
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        assert!(!fm.ensure_folder_fixed_slot(&folder_id).await);
        let folders = fm.folders.read().await;
        assert!(folders.get(&folder_id).unwrap().fixed_slot_id.is_none());
    }

    /// 已持有固定位时直接返回 true，不重复申请
    #[tokio::test]
    async fn test_ensure_folder_fixed_slot_is_noop_when_already_held() {
        let (fm, _tmp) = fm_with_download_manager().await;

        let mut folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        folder.fixed_slot_id = Some(3);
        let folder_id = folder.id.clone();
        {
            let mut folders = fm.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        assert!(fm.ensure_folder_fixed_slot(&folder_id).await);
        let folders = fm.folders.read().await;
        assert_eq!(folders.get(&folder_id).unwrap().fixed_slot_id, Some(3));
    }

    /// 文件夹不存在时不发放（防御性路径，避免误判为「拿到了槽位」）
    #[tokio::test]
    async fn test_no_fixed_slot_grant_for_unknown_folder() {
        let fm = FolderDownloadManager::new(PathBuf::from("."));
        assert!(
            !fm.try_allocate_fixed_slot_for_subtask("nonexistent", "child").await
        );
    }
}

#[cfg(test)]
mod priority_tests {
    use super::*;
    use crate::task_slot_pool::{TaskPriority, TaskSlotPool};

    /// 用户手动发起的文件夹下载是 Normal；归属 backup_config_id 的后台文件夹
    /// （自动备份 / 分享同步）必须是 Backup，与单文件下载段
    /// （`DownloadManager::create_backup_task`）同档。
    #[test]
    fn test_folder_slot_priority_by_backup_config() {
        assert_eq!(folder_slot_priority(None), TaskPriority::Normal);
        assert_eq!(
            folder_slot_priority(Some("share-sync:sub-1")),
            TaskPriority::Backup,
            "分享同步内部文件夹下载必须走 Backup 档"
        );
        assert_eq!(
            folder_slot_priority(Some("2f1c8a10-uuid-backup-config")),
            TaskPriority::Backup,
            "自动备份文件夹下载同样是后台任务"
        );
    }

    /// 子任务优先级必须跟随所属文件夹，否则「主任务已降为 Backup」的修复
    /// 会在子任务这一层原样再现（SubTask(20) 照样能抢占 Backup(30)）。
    #[tokio::test]
    async fn test_subtask_priority_follows_folder() {
        let fm = FolderDownloadManager::new(PathBuf::from("."));

        let mut background = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        background.backup_config_id = Some("share-sync:sub-1".into());
        let bg_id = background.id.clone();

        let manual = FolderDownload::new("/b".into(), PathBuf::from("./b"));
        let manual_id = manual.id.clone();

        {
            let mut folders = fm.folders.write().await;
            folders.insert(bg_id.clone(), background);
            folders.insert(manual_id.clone(), manual);
        }

        assert_eq!(
            fm.subtask_slot_priority(&bg_id).await,
            TaskPriority::Backup,
            "后台文件夹的子任务也是后台任务"
        );
        assert_eq!(
            fm.subtask_slot_priority(&manual_id).await,
            TaskPriority::SubTask,
            "用户手动文件夹的子任务维持 SubTask"
        );
        assert_eq!(
            fm.subtask_slot_priority("unknown").await,
            TaskPriority::SubTask,
            "取不到文件夹信息时回退旧行为"
        );
    }

    /// 回归：文件夹被抢占后，必须能被识别成「文件夹」而不是「下载任务」。
    ///
    /// `task_slot_pool` 里文件夹固定位的 owner 是 **folder_id**。文件夹优先级还是
    /// `Normal` 时它不可被抢占，这条路径是死代码；降为 `Backup` 后变得可达，
    /// 而原来的处理写死按任务走 `pause_task`，结果是：
    /// ```text
    /// 抢占备份任务槽位: preempted_task=Some("<folder_id>")
    /// 暂停被抢占的备份任务 <folder_id> 失败: 任务不存在
    /// ⚠️ 槽位诊断: 活跃任务数 2 超过任务槽上限 1
    /// ```
    /// 槽位被拿走、文件夹却仍在跑。`is_known_folder` 就是分发的依据。
    #[tokio::test]
    async fn test_preempted_owner_is_recognized_as_folder() {
        let fm = FolderDownloadManager::new(PathBuf::from("."));

        let folder = FolderDownload::new("/a".into(), PathBuf::from("./a"));
        let folder_id = folder.id.clone();
        {
            fm.folders.write().await.insert(folder_id.clone(), folder);
        }

        assert!(
            fm.is_known_folder(&folder_id).await,
            "文件夹 id 必须能被识别，否则会被当成下载任务去 pause_task"
        );
        assert!(
            !fm.is_known_folder("some-download-task-id").await,
            "普通下载任务 id 不应被当成文件夹"
        );
    }

    /// 回归 issue #138 的两条核心诉求：
    /// ① 后台同步不得抢占正在跑的其它后台下载；
    /// ② 后台同步必须给用户手动发起的下载让位。
    #[tokio::test]
    async fn test_background_folder_yields_to_user_but_not_preempts_peers() {
        // 非会员：max_concurrent_tasks = 1
        let pool = TaskSlotPool::new(1);

        // 分享同步的单文件下载先拿到唯一的槽位
        assert_eq!(pool.allocate_backup_slot("share-sync-file").await, Some(0));

        // ① 另一个订阅的文件夹下载（后台档）来抢 —— 必须抢不到
        assert!(
            pool.allocate_fixed_slot_with_priority(
                "folder-from-other-sub",
                true,
                folder_slot_priority(Some("share-sync:sub-2")),
            )
                .await
                .is_none(),
            "后台文件夹不得抢占正在跑的后台下载"
        );
        // 借调路径也不能成为绕过优先级的后门
        assert!(
            pool.allocate_borrowed_slots_no_preempt("folder-from-other-sub", 1)
                .await
                .is_empty(),
            "后台文件夹借调时同样不得抢占"
        );
        // 槽位仍归原任务
        assert_eq!(
            pool.get_task_slot("share-sync-file").await.map(|(id, _)| id),
            Some(0)
        );

        // ② 用户手动发起的下载（Normal）必须能把后台任务挤下去
        let (slot_id, preempted) = pool
            .allocate_fixed_slot_with_priority("user-folder", true, folder_slot_priority(None))
            .await
            .expect("用户手动发起的下载应能抢占后台任务");
        assert_eq!(slot_id, 0);
        assert_eq!(preempted.as_deref(), Some("share-sync-file"));
    }
}
