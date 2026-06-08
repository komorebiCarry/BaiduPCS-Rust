//! 文件夹下载管理器

use crate::autobackup::record::BackupRecordManager;
use crate::auth::types::Uid;
use crate::downloader::{DownloadManager, DownloadTask, TaskStatus};
use crate::netdisk::{ClientPool, NetdiskClient};
use crate::server::events::{FolderEvent, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
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
        Arc<RwLock<Option<mpsc::UnboundedSender<(String, String, u64, bool)>>>>,
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

    /// 持久化文件夹状态
    async fn persist_folder(&self, folder_id: &str) {
        let wal_dir = {
            let dir = self.wal_dir.read().await;
            dir.clone()
        };

        let wal_dir = match wal_dir {
            Some(dir) => dir,
            None => return, // WAL 目录未设置，跳过持久化
        };

        let folder = {
            let folders = self.folders.read().await;
            folders.get(folder_id).cloned()
        };

        if let Some(folder) = folder {
            let persisted = FolderPersisted::from_folder(&folder);
            if let Err(e) = save_folder(&wal_dir, &persisted) {
                error!("持久化文件夹 {} 失败: {}", folder_id, e);
            }
        }
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

                let to_create = needed.min(folder.pending_files.len());
                if to_create == 0 {
                    continue;
                }

                let files = folder.pending_files.drain(..to_create).collect::<Vec<_>>();
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

            // 创建暂停状态的任务
            let mut created_count = 0u64;
            for pending_file in files_to_create {
                let local_path = local_root.join(&pending_file.relative_path);

                // 确保目录存在
                if let Some(parent) = local_path.parent() {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        error!("创建目录失败: {:?}, 错误: {}", parent, e);
                        continue;
                    }
                }

                let mut task = DownloadTask::new_with_group(
                    pending_file.fs_id,
                    pending_file.remote_path.clone(),
                    local_path,
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

            // 更新已创建计数
            if created_count > 0 {
                let mut folders = self.folders.write().await;
                if let Some(folder) = folders.get_mut(&folder_id) {
                    folder.created_count += created_count;
                }
                total_created += created_count as usize;
                info!(
                    "恢复模式补任务完成: 文件夹 {} 创建了 {} 个暂停任务",
                    folder_id, created_count
                );
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
        let (tx, rx) = mpsc::unbounded_channel::<(String, String, u64, bool)>();

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

        info!("文件夹下载管理器已设置下载管理器，任务完成监听和进度监听器已启动（首次初始化）");
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
                            f.owner_uid.raw(),
                            f.owner_uid,
                        )
                    })
                };
                let (total_files, total_size, status, completed_files, owner_uid_raw, owner_uid) =
                    match folder_meta {
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

                // 🔥 计算活跃子任务的已下载字节数和速度
                let active_downloaded: u64 = tasks.iter().map(|t| t.downloaded_size).sum();
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
                        total_files,
                        speed,
                        status: format!("{:?}", status).to_lowercase(),

                        owner_uid: Some(owner_uid_raw),
                    }),
                    None,
                );
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
    fn start_task_completed_listener(&self, mut rx: mpsc::UnboundedReceiver<(String, String, u64, bool)>) {
        let folders = self.folders.clone();
        let download_manager_pool = self.download_manager_pool.clone();
        let download_manager_legacy = self.download_manager.clone();
        let wal_dir = self.wal_dir.clone();
        let ws_manager = self.ws_manager.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();

        tokio::spawn(async move {
            while let Some((group_id, task_id, file_size, is_success)) = rx.recv().await {
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
                    let (slot_id_to_release, released_fixed_slot) = {
                        let mut folders_guard = folders.write().await;

                        if let Some(folder) = folders_guard.get_mut(&group_id) {
                            // 🔥 检查任务是否已经被计数过
                            let already_counted = folder.counted_task_ids.contains(&task_id);

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

                            if is_success && !already_counted {
                                // 🔥 成功且未计数：递增 completed_count
                                folder.counted_task_ids.insert(task_id.clone());
                                folder.completed_count += 1;
                                folder.completed_downloaded_size += file_size;
                                // 如果之前失败过（retry→success），从 failed 中移除
                                if folder.failed_task_ids.remove(&task_id) {
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
                                // 🔥 失败且未成功计数：记入 failed_task_ids（去重）
                                if folder.failed_task_ids.insert(task_id.clone()) {
                                    folder.failed_count += 1;
                                    info!(
                                        "文件夹 {} 子任务失败 (failed_count={}, task_id={})",
                                        group_id, folder.failed_count, task_id
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
                        // 逻辑：借调位4个，固定位1个，总共5个槽位可供子任务使用
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
                    if folder.pending_files.is_empty()
                        && folder.scan_completed
                        && active_count == 0
                        && completed_count == folder.total_files
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
                        && (folder.completed_count + folder.failed_count) >= folder.total_files
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

                    // 根据可用槽位数量（借调位+固定位）取出相应数量的文件
                    let count = folder.pending_files.len().min(available);
                    let files: Vec<_> = folder.pending_files.drain(..count).collect();
                    (files, folder.local_root.clone(), folder.remote_root.clone(), folder.owner_uid)
                };

                let (files, local_root, group_root, folder_owner_uid) = files_to_create;
                let total_files = files.len();
                let mut created_count = 0u64;

                // 创建任务
                for file_to_create in files {
                    // ✅ 创建任务前再次检查状态，防止竞态条件
                    // 场景：取出文件后、创建任务前，pause_folder 可能已更新状态
                    {
                        let folders_guard = folders.read().await;
                        if let Some(folder) = folders_guard.get(&group_id) {
                            if folder.status == FolderStatus::Paused
                                || folder.status == FolderStatus::Cancelled
                                || folder.status == FolderStatus::Failed
                            {
                                info!(
                                    "文件夹 {} 状态已变为 {:?}，放弃创建剩余 {} 个任务",
                                    group_id,
                                    folder.status,
                                    total_files - created_count as usize
                                );
                                break;
                            }
                        } else {
                            // 文件夹已被删除
                            break;
                        }
                    }

                    let local_path = local_root.join(&file_to_create.relative_path);

                    // 确保目录存在
                    if let Some(parent) = local_path.parent() {
                        if let Err(e) = tokio::fs::create_dir_all(parent).await {
                            error!("创建目录失败: {:?}, 错误: {}", parent, e);
                            continue;
                        }
                    }

                    let mut task = DownloadTask::new_with_group(
                        file_to_create.fs_id,
                        file_to_create.remote_path.clone(),
                        local_path,
                        file_to_create.size,
                        group_id.clone(),
                        group_root.clone(),
                        file_to_create.relative_path,
                        folder_owner_uid,
                    );

                    // 🔥 尝试为子任务分配借调位
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
                        // 🔥 没有可用的借调位，尝试占用文件夹固定位（直持有语义）
                        //    关键：必须写成 `uses_folder_fixed_slot=true, slot_id=None`，
                        //    并同步登记 `folder.fixed_slot_subtask = Some(task.id)`。
                        //    否则：
                        //    (a) scheduler 完成时会误走 `release_fixed_slot(task_id)`，但 pool 里 owner=group_id，清不掉
                        //    (b) `fixed_slot_subtask` 仍为 None，后续 `try_allocate_fixed_slot_for_subtask` 会把同一 fixed slot 再次分配给别的等待子任务
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
                            // 固定位已被占用或不存在，创建任务但不分配槽位
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

                // 更新已创建计数
                if created_count > 0 {
                    let mut folders_guard = folders.write().await;
                    if let Some(folder) = folders_guard.get_mut(&group_id) {
                        folder.created_count += created_count;
                    }
                    info!(
                        "已补充{}个任务到文件夹 {} (可用槽位: {})",
                        created_count, group_id, available
                    );
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

        self.create_folder_download_internal(remote_path, local_root, conflict_strategy, owner_uid)
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

        self.create_folder_download_internal(remote_path, local_root, conflict_strategy, owner_uid)
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
    ) -> Result<String> {
        let mut folder = FolderDownload::new(remote_path.clone(), local_root);
        let folder_id = folder.id.clone();

        // 🔥 多账号：设置 folder 归属 UID（与 ClientPool/per-uid downloader 路由对齐）
        folder.owner_uid = owner_uid;

        // 🔥 设置冲突策略
        folder.conflict_strategy = conflict_strategy;

        // 🔥 按 folder.owner_uid 解析 download_manager（per-uid）
        let dm_for_owner: Option<Arc<DownloadManager>> = self.download_manager_for(owner_uid).await;

        // 🔥 尝试为文件夹分配固定任务位（使用优先级分配，可抢占备份任务）
        let (mut fixed_slot_id, mut preempted_task_id) = {
            if let Some(ref dm) = dm_for_owner {
                let slot_pool = dm.task_slot_pool();
                // 文件夹主任务使用 Normal 优先级，可以抢占备份任务
                if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                    &folder_id, true, crate::task_slot_pool::TaskPriority::Normal
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
            info!("文件夹 {} 抢占了备份任务 {} 的槽位", folder_id, preempted_id);
            if let Some(ref dm) = dm_for_owner {
                // 暂停被抢占的备份任务并加入等待队列
                if let Err(e) = dm.pause_task(&preempted_id, true).await {
                    warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
                }
                // 将被抢占的任务加入等待队列末尾
                dm.add_preempted_backup_to_queue(&preempted_id).await;
            }
        }

        // 🔥 如果没有空闲槽位，尝试从同一账号的其他文件夹回收借调位
        // 这确保了多个文件夹任务之间的公平性：每个文件夹至少能获得一个固定位
        // reclaim 必须按 owner_uid 过滤，避免跨账号误回收
        if fixed_slot_id.is_none() {
            info!("文件夹 {} 无空闲槽位，尝试回收同账号其他文件夹的借调位", folder_id);
            if let Some(reclaimed_slot_id) = self
                .reclaim_borrowed_slot_for_owner(owner_uid)
                .await
            {
                // 回收成功，重新分配固定位
                if let Some(ref dm) = dm_for_owner {
                    let slot_pool = dm.task_slot_pool();
                    if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                        &folder_id, true, crate::task_slot_pool::TaskPriority::Normal
                    ).await {
                        fixed_slot_id = Some(slot_id);
                        info!(
                            "文件夹 {} 通过回收借调位获得固定任务位: slot_id={} (回收的槽位={})",
                            folder_id, slot_id, reclaimed_slot_id
                        );
                        // 处理可能被抢占的备份任务
                        if let Some(preempted_id) = preempted {
                            info!("文件夹 {} 抢占了备份任务 {} 的槽位", folder_id, preempted_id);
                            if let Err(e) = dm.pause_task(&preempted_id, true).await {
                                warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
                            }
                            dm.add_preempted_backup_to_queue(&preempted_id).await;
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

        // 🔥 尝试借调槽位（最多借调4个，总共5个并行子任务）
        // 支持抢占备份任务：如果空闲槽位不足，会抢占备份任务的槽位
        let (borrowed_slot_ids, preempted_backup_tasks) = {
            if let Some(ref dm) = dm_for_owner {
                let slot_pool = dm.task_slot_pool();
                let available = slot_pool.available_borrow_slots().await;
                let to_borrow = available.min(4); // 最多借调4个
                if to_borrow > 0 {
                    slot_pool.allocate_borrowed_slots(&folder_id, to_borrow).await
                } else {
                    (Vec::new(), Vec::new())
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
                    if let Err(e) = dm.pause_task(preempted_id, true).await {
                        warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
                    }
                    // 将被抢占的任务加入等待队列末尾
                    dm.add_preempted_backup_to_queue(preempted_id).await;
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
        let (remote_root, local_root) = {
            let folders = self.folders.read().await;
            let folder = folders
                .get(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;
            (folder.remote_root.clone(), folder.local_root.clone())
        };

        // 获取网盘客户端
        let client = {
            let nc = self.netdisk_client.read().await;
            nc.clone().ok_or_else(|| anyhow!("网盘客户端未初始化"))?
        };

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
            let file_list = client.get_file_list(current_path, page, page_size).await?;

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
        if removed > 0 {
            info!("从内存中清除了 {} 个已完成的文件夹", removed);
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
        if removed > 0 {
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

        // 🔥 同时取出 folder.owner_uid 用于路由 download_manager
        let (folder_info, old_status, new_status, folder_owner_uid_for_routing) = {
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

            (
                (
                    folder.scan_completed,
                    folder.remote_root.clone(),
                    folder.local_root.clone(),
                ),
                old_status,
                new_status,
                owner_uid,
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

        // 1. 先分配固定位（使用优先级分配，可抢占备份任务）
        let (mut fixed_slot_id, mut preempted_task_id) =
            if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                folder_id, true, crate::task_slot_pool::TaskPriority::Normal
            ).await {
                (Some(slot_id), preempted)
            } else {
                (None, None)
            };

        // 🔥 处理被抢占的备份任务
        if let Some(preempted_id) = preempted_task_id.take() {
            info!("恢复文件夹 {} 抢占了备份任务 {} 的槽位", folder_id, preempted_id);
            // 暂停被抢占的备份任务并加入等待队列
            if let Err(e) = download_manager.pause_task(&preempted_id, true).await {
                warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
            }
            // 将被抢占的任务加入等待队列末尾
            download_manager.add_preempted_backup_to_queue(&preempted_id).await;
        }

        // 🔥 如果没有空闲槽位，尝试从同一账号的其他文件夹回收借调位
        // 这确保了多个文件夹任务之间的公平性：每个文件夹至少能获得一个固定位
        // reclaim 必须按 owner_uid 过滤，避免跨账号误回收
        if fixed_slot_id.is_none() {
            info!("恢复文件夹 {} 无空闲槽位，尝试回收同账号其他文件夹的借调位", folder_id);
            if let Some(reclaimed_slot_id) = self
                .reclaim_borrowed_slot_for_owner(folder_owner_uid_for_routing)
                .await
            {
                // 回收成功，重新分配固定位（使用优先级分配）
                if let Some((slot_id, preempted)) = slot_pool.allocate_fixed_slot_with_priority(
                    folder_id, true, crate::task_slot_pool::TaskPriority::Normal
                ).await {
                    fixed_slot_id = Some(slot_id);
                    info!(
                        "恢复文件夹 {} 通过回收借调位获得固定任务位: slot_id={} (回收的槽位={})",
                        folder_id, slot_id, reclaimed_slot_id
                    );
                    // 处理可能被抢占的备份任务
                    if let Some(preempted_id) = preempted {
                        info!("恢复文件夹 {} 抢占了备份任务 {} 的槽位", folder_id, preempted_id);
                        if let Err(e) = download_manager.pause_task(&preempted_id, true).await {
                            warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
                        }
                        download_manager.add_preempted_backup_to_queue(&preempted_id).await;
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

        // 2. 尝试借调槽位（最多借调4个，总共5个并行子任务）
        // 支持抢占备份任务：如果空闲槽位不足，会抢占备份任务的槽位
        let available = slot_pool.available_borrow_slots().await;
        let to_borrow = available.min(4);
        let (borrowed_slot_ids, preempted_backup_tasks) = if to_borrow > 0 {
            slot_pool.allocate_borrowed_slots(folder_id, to_borrow).await
        } else {
            (Vec::new(), Vec::new())
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
                if let Err(e) = download_manager.pause_task(preempted_id, true).await {
                    warn!("暂停被抢占的备份任务 {} 失败: {}", preempted_id, e);
                }
                // 将被抢占的任务加入等待队列末尾
                download_manager.add_preempted_backup_to_queue(preempted_id).await;
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
            // 为子任务分配借调位
            let assigned_slot = {
                let mut folders_guard = self.folders.write().await;
                if let Some(folder) = folders_guard.get_mut(folder_id) {
                    // 优先使用借调位
                    let mut found_slot = None;
                    for &slot_id in &folder.borrowed_slot_ids {
                        // 🔥 关键修复：同时检查 borrowed_subtask_map 和 used_slot_ids
                        let in_map = folder.borrowed_subtask_map.values().any(|&s| s == slot_id);
                        let in_use = used_slot_ids.contains(&slot_id);
                        if !in_map && !in_use {
                            found_slot = Some((slot_id, true)); // (slot_id, is_borrowed)
                            folder.borrowed_subtask_map.insert(task.id.clone(), slot_id);
                            break;
                        }
                    }
                    // 如果没有空闲借调位，使用固定位
                    if found_slot.is_none() {
                        if let Some(fixed_slot) = folder.fixed_slot_id {
                            // 🔥 关键修复：检查固定位是否已被使用（通过 used_slot_ids）
                            if !used_slot_ids.contains(&fixed_slot) {
                                found_slot = Some((fixed_slot, false)); // 固定位不是借调位
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
            };
            let folder_id = folder_id.to_string();

            tokio::spawn(async move {
                if let Err(e) = self_clone.scan_folder_and_create_tasks(&folder_id).await {
                    error!("恢复扫描失败: {:?}", e);
                }
            });
        } else {
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

        // 🔥 发布删除事件（取消视为删除）
        self.publish_event(FolderEvent::Deleted {
            folder_id: folder_id.to_string(),

            owner_uid: owner_uid_raw_opt,
        })
            .await;

        Ok(())
    }

    /// 删除文件夹下载记录
    pub async fn delete_folder(&self, folder_id: &str) -> Result<()> {
        let mut folders = self.folders.write().await;
        // 🔥 remove 时取出 owner_uid 用于事件
        let owner_uid_raw_opt: Option<u64> = folders.remove(folder_id).map(|f| f.owner_uid.raw());
        drop(folders);

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

    /// 补充任务：保持文件夹有指定数量的活跃任务
    ///
    /// 这是核心方法：检查活跃任务数，如果不足就从 pending_files 补充
    /// 🔥 修复：在分配借调位前，收集所有子任务已占用的槽位，避免重复分配
    async fn refill_tasks(&self, folder_id: &str, target_count: usize) -> Result<()> {
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

        // 如果已经足够，不需要补充
        if active_count >= target_count {
            return Ok(());
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
                return Ok(());
            }

            let to_create = needed.min(folder.pending_files.len());
            if to_create == 0 {
                return Ok(());
            }

            let files = folder.pending_files.drain(..to_create).collect::<Vec<_>>();
            (files, folder.local_root.clone(), folder.remote_root.clone(), folder.owner_uid)
        };

        if files_to_create.is_empty() {
            return Ok(());
        }

        info!(
            "补充任务: 文件夹 {} 需要 {} 个任务 (当前活跃: {}/{})",
            folder_id,
            files_to_create.len(),
            active_count,
            target_count
        );

        // 批量创建任务
        let mut created_count = 0u64;
        for pending_file in files_to_create {
            // ✅ 创建任务前再次检查状态，防止竞态条件
            // 场景：取出文件后、创建任务前，pause_folder 可能已更新状态
            {
                let folders_guard = self.folders.read().await;
                if let Some(folder) = folders_guard.get(folder_id) {
                    if folder.status == FolderStatus::Paused
                        || folder.status == FolderStatus::Cancelled
                        || folder.status == FolderStatus::Failed
                    {
                        info!(
                            "文件夹 {} 状态已变为 {:?}，放弃创建剩余任务",
                            folder_id, folder.status
                        );
                        break;
                    }
                } else {
                    // 文件夹已被删除
                    break;
                }
            }

            let local_path = local_root.join(&pending_file.relative_path);

            // 🔥 应用冲突策略
            let final_local_path = {
                let folders_guard = self.folders.read().await;
                let strategy = folders_guard
                    .get(folder_id)
                    .and_then(|f| f.conflict_strategy)
                    .unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);

                use crate::uploader::conflict_resolver::ConflictResolver;
                match ConflictResolver::resolve_download_conflict(&local_path, strategy) {
                    Ok(crate::uploader::conflict::ConflictResolution::Proceed) => local_path,
                    Ok(crate::uploader::conflict::ConflictResolution::Skip) => {
                        info!("跳过下载（文件已存在）: {:?}", local_path);
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

            // 🔥 尝试为子任务分配借调位
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
                // 🔥 没有可用的借调位，尝试占用文件夹固定位（直持有语义）
                //    必须与另一条补任务路径保持一致：
                //    - 写入 uses_folder_fixed_slot=true, slot_id=None
                //    - 同步登记 fixed_slot_subtask = Some(task.id)
                //    详情见另一处同名注释块。
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
                    // 🔥 所有槽位都已占用，但仍然创建任务（不分配槽位）
                    //    任务会进入等待队列，当有槽位释放时会被调度
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

        // 更新已创建计数
        {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.created_count += created_count;
            }
        }

        info!(
            "补充任务完成: 文件夹 {} 成功创建 {} 个任务",
            folder_id, created_count
        );

        Ok(())
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

        let (should_persist, old_status) = {
            let mut folders = self.folders.write().await;
            let mut should_persist = false;
            let mut old_status = String::new();
            if let Some(folder) = folders.get_mut(folder_id) {
                // 🔥 不再从 tasks 重新计算 completed_count，因为已完成的任务会从内存移除
                // completed_count 由 start_task_completed_listener 递增维护

                // 🔥 使用 compute_downloaded_size：completed_downloaded_size + active_sum
                // max() 保证单调性
                let active_downloaded: u64 = tasks.iter().map(|t| t.downloaded_size).sum();
                folder.compute_downloaded_size(active_downloaded);

                // 检查是否全部完成（成功 + 失败 >= 总数）
                if folder.scan_completed
                    && folder.pending_files.is_empty()
                    && (folder.completed_count + folder.failed_count) >= folder.total_files
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

        // 终态时更新持久化文件
        if should_persist {
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

        // 补充任务：保持10个活跃任务（完成1个，进1个）
        if let Err(e) = self.refill_tasks(folder_id, 10).await {
            warn!("补充任务失败: {}", e);
        }

        Ok(())
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

        // 选第一个有借调位的 folder（保留原始"取一个"语义）
        let (folder_id, folder_owner_uid) = candidate_folders.first()?.clone();
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

                if let Some(task) = running_task {
                    info!(
                        "borrowed_subtask_map 为空，从调度器找到正在运行的子任务: {}",
                        task.id
                    );
                    task.id.clone()
                } else {
                    // 确实没有正在运行的子任务，直接释放一个借调位
                    let borrowed_slots = slot_pool.get_borrowed_slots(&folder_id).await;
                    if let Some(&slot_id) = borrowed_slots.first() {
                        slot_pool.release_borrowed_slot(&folder_id, slot_id).await;

                        // 更新文件夹的借调位记录
                        {
                            let mut folders_guard = self.folders.write().await;
                            if let Some(folder) = folders_guard.get_mut(&folder_id) {
                                folder.borrowed_slot_ids.retain(|&id| id != slot_id);
                            }
                        }

                        info!("直接释放空闲借调位: slot_id={} from folder {}", slot_id, folder_id);

                        // 🔥 修复：释放槽位后不触发 try_start_waiting_tasks
                        // 因为这个槽位是要给新任务用的，不是给等待队列的
                        // dm.try_start_waiting_tasks().await; // 已移除

                        return Some(slot_id);
                    }
                    return None;
                }
            }
        };

        info!("回收流程：暂停借调子任务 {}", task_id);

        // 暂停子任务（skip_try_start_waiting=true，不触发等待队列启动）
        // 🔥 关键修复：回收借调槽位时，槽位是给新任务预留的，不应让等待队列抢占
        if let Err(e) = dm.pause_task(&task_id, true).await {
            warn!("暂停任务失败: {}", e);
            return None;
        }

        // 等待任务暂停完成（所有运行中分片完成）
        Self::wait_for_task_paused(&dm, &task_id).await;

        // 获取并释放借调位
        let slot_id = {
            let mut folders_guard = self.folders.write().await;
            let folder = folders_guard.get_mut(&folder_id)?;

            // 优先从 borrowed_subtask_map 获取槽位
            // 如果 map 中没有记录（恢复任务时可能未维护），则从 borrowed_slot_ids 取第一个
            let slot_id = if let Some(slot_id) = folder.borrowed_subtask_map.remove(&task_id) {
                slot_id
            } else if let Some(&slot_id) = folder.borrowed_slot_ids.first() {
                info!(
                    "borrowed_subtask_map 中无记录，从 borrowed_slot_ids 取槽位: {}",
                    slot_id
                );
                slot_id
            } else {
                warn!("无法获取借调位：borrowed_slot_ids 为空");
                return None;
            };

            folder.borrowed_slot_ids.retain(|&id| id != slot_id);
            slot_id
        };

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
    pub async fn try_allocate_fixed_slot_for_subtask(
        &self,
        folder_id: &str,
        task_id: &str,
    ) -> bool {
        let mut folders_guard = self.folders.write().await;
        match folders_guard.get_mut(folder_id) {
            Some(folder) => {
                if folder.fixed_slot_subtask.is_none() {
                    folder.fixed_slot_subtask = Some(task_id.to_string());
                    info!(
                        "分配文件夹固定槽位: folder={}, task={}",
                        folder_id, task_id
                    );
                    true
                } else {
                    // 已被占用
                    false
                }
            }
            None => false,
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
