// 上传管理器
//
// 负责管理多个上传任务：
// - 任务队列管理
// - 并发控制（唯一走 BudgetScheduler + per-uid UploadManager；唯一构造入口 `new_for_account`）
// - 进度跟踪
// - 暂停/恢复/取消

use crate::auth::UserAuth;
use crate::encryption::{EncryptionConfigStore, SnapshotManager};
use crate::autobackup::events::BackupTransferNotification;
use crate::autobackup::record::BackupRecordManager;
use crate::config::{UploadConfig, VipType};
use crate::netdisk::NetdiskClient;
use crate::persistence::{
    PersistenceManager, TaskMetadata, UploadRecoveryInfo,
};
use crate::server::events::{ProgressThrottler, TaskEvent, UploadEvent};
use crate::server::websocket::WebSocketManager;
use crate::task_slot_pool::{TaskPriority, TaskSlotPool};
use crate::uploader::{
    calculate_upload_task_max_chunks, FolderScanner, PcsServerHealthManager, ScanOptions,
    UploadChunkManager, UploadChunkScheduler, UploadTask, UploadTaskScheduleInfo,
    UploadTaskStatus,
};
use anyhow::{Context, Result};
use dashmap::DashMap;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 上传任务信息（用于调度）
#[derive(Debug, Clone)]
pub struct UploadTaskInfo {
    /// 任务
    pub task: Arc<Mutex<UploadTask>>,
    /// 分片管理器（延迟创建：只有在预注册成功后才创建，避免大量等待任务占用内存）
    pub chunk_manager: Option<Arc<Mutex<UploadChunkManager>>>,
    /// 取消令牌
    pub cancel_token: CancellationToken,
    /// 最大并发分片数（根据文件大小计算）
    pub max_concurrent_chunks: usize,
    /// 当前活跃分片数
    pub active_chunk_count: Arc<AtomicUsize>,
    /// 是否暂停
    pub is_paused: Arc<AtomicBool>,
    /// 已上传字节数（用于调度器模式）
    pub uploaded_bytes: Arc<AtomicU64>,
    /// 上次速度计算时间
    pub last_speed_time: Arc<Mutex<std::time::Instant>>,
    /// 上次速度计算字节数
    pub last_speed_bytes: Arc<AtomicU64>,
    /// 🔥 恢复的 upload_id（如果任务是从持久化恢复的）
    pub restored_upload_id: Option<String>,
    /// 🔥 恢复的已完成分片信息（延迟创建分片管理器时使用）
    pub restored_completed_chunks: Option<RestoredChunkInfo>,
}

/// 恢复任务时保存的分片信息（用于延迟创建分片管理器）
#[derive(Debug, Clone)]
pub struct RestoredChunkInfo {
    /// 分片大小
    pub chunk_size: u64,
    /// 已完成的分片索引列表
    pub completed_chunks: Vec<usize>,
    /// 分片 MD5 列表（索引对应分片索引）
    pub chunk_md5s: Vec<Option<String>>,
}

/// 上传管理器
pub struct UploadManager {
    /// 网盘客户端（共享引用，代理热更新时自动生效）
    client: Arc<StdRwLock<NetdiskClient>>,
    /// 用户 VIP 类型
    vip_type: VipType,
    /// 所有任务（task_id -> TaskInfo）- 使用 Arc 包装以支持跨线程共享
    tasks: Arc<DashMap<String, UploadTaskInfo>>,
    /// 等待队列（task_id 列表，FIFO）
    waiting_queue: Arc<RwLock<VecDeque<String>>>,
    /// 服务器健康管理器
    server_health: Arc<PcsServerHealthManager>,
    /// 全局调度器（始终启用）
    scheduler: Arc<UploadChunkScheduler>,
    /// 最大同时上传任务数（动态可调整）
    max_concurrent_tasks: Arc<AtomicUsize>,
    /// 最大重试次数（动态可调整）
    max_retries: Arc<AtomicUsize>,
    /// 🔥 持久化管理器引用（使用单锁结构避免死锁）
    persistence_manager: Arc<Mutex<Option<Arc<Mutex<PersistenceManager>>>>>,
    /// 🔥 WebSocket 管理器
    ws_manager: Arc<RwLock<Option<Arc<WebSocketManager>>>>,
    /// 🔥 备份任务统一通知发送器（进度、状态、完成、失败等）
    backup_notification_tx:
        Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>>>>,
    /// 🔥 任务槽池管理器（独立实例，与下载分离）
    task_slot_pool: Arc<TaskSlotPool>,
    /// 🔥 加密配置存储（用于从 encryption.json 读取密钥）
    encryption_config_store: Arc<EncryptionConfigStore>,
    /// 🔥 加密快照管理器（用于保存加密映射到 encryption_snapshots 表）
    snapshot_manager: Arc<RwLock<Option<Arc<SnapshotManager>>>>,
    /// 🔥 备份记录管理器（用于文件夹名加密映射）
    backup_record_manager: Arc<RwLock<Option<Arc<BackupRecordManager>>>>,
    /// 去重索引：(local_path, original_remote_path) → task_id
    dedup_index: DashMap<(PathBuf, String), String>,
    /// 反向索引：task_id → (local_path, original_remote_path)
    dedup_reverse: DashMap<String, (PathBuf, String)>,
    /// 🔥 活跃任务计数器（Pending/Uploading/Encrypting/CheckingRapid），O(1) 查询
    active_count: Arc<AtomicUsize>,
    /// 🔥 多账号归属 UID
    ///
    /// 仅与账号生命周期绑定、运态不变，所以是个普通字段。
    /// 所有 UploadTask 创建点都应该链调 .with_owner_uid(self.owner_uid)。
    owner_uid: crate::auth::Uid,
}

impl UploadManager {
    /// 🔥 多账号唯一构造入口
    ///
    /// 多账号上传**唯一**走 `BudgetScheduler` + per-uid `UploadManager`，线程预算来源只有
    /// `state.budget_scheduler.acquire_chunk_permit(uid, BudgetKind::Upload)` 一条
    /// 路径（与下载侧完全对称）。单账号场景作为 `N=1` 退化天然兼容。
    ///
    /// `budget_scheduler` 是构造时必填参数，由 `UploadChunkScheduler::new_with_config`
    /// 一次性接收并保存为非 `Option` 字段，从结构上消除"漏调 wire 时分片调度静默
    /// fallback 绕过 budget"的破口。
    ///
    /// # 参数
    /// * `client` - 网盘客户端
    /// * `user_auth` - 用户认证信息（提供 uid、vip_type）
    /// * `config` - 上传配置
    /// * `budget_scheduler` - 多账号配额调度器（必填，每分片 acquire 唯一来源）
    /// * `config_dir` - 配置目录（用于读取 encryption.json）
    pub fn new_for_account(
        client: NetdiskClient,
        user_auth: &UserAuth,
        config: &UploadConfig,
        budget_scheduler: Arc<crate::downloader::budget_scheduler::BudgetScheduler>,
        config_dir: &Path,
    ) -> Self {
        let max_global_threads = config.max_global_threads;
        let max_concurrent_tasks = config.max_concurrent_tasks;
        let max_retries = config.max_retries as usize;

        // 从 user_auth 获取 VIP 类型
        let vip_type = VipType::from_u32(user_auth.vip_type.unwrap_or(0));

        // 创建服务器健康管理器
        let servers = vec![
            "d.pcs.baidu.com".to_string(),
            "c.pcs.baidu.com".to_string(),
            "pcs.baidu.com".to_string(),
        ];
        let server_health = Arc::new(PcsServerHealthManager::from_servers(servers));

        // 创建调度器：构造时直接注入 budget_scheduler。
        //
        // 🔥 调度器 owner_uid 设计：当前架构是 per-uid 独立 `UploadManager` 实例，
        // 每个 manager 内部都有自己的 `UploadChunkScheduler`。
        // scheduler 内部的 owner_uid 仍用 `Uid::default()` 占位禁用 scheduler-level
        // assert（同 download 侧 — 单 manager 内同时调度的多个 task 可能 owner 不同）；
        // budget acquire 在 spawn_chunk_upload 内按 `task.owner_uid` 路由，
        // 确保跨账号场景 budget 从正确账号桶借调。
        info!(
            "上传管理器（per-uid uid={}）初始化: 全局线程数={}, 最大任务数={}, 最大重试={}",
            user_auth.uid, max_global_threads, max_concurrent_tasks, max_retries
        );
        let scheduler = Arc::new(UploadChunkScheduler::new_with_config(
            max_global_threads,
            max_concurrent_tasks,
            max_retries as u32,
            budget_scheduler,
            crate::auth::Uid::default(),
        ));

        let waiting_queue = Arc::new(RwLock::new(VecDeque::new()));
        let max_concurrent_tasks_atomic = Arc::new(AtomicUsize::new(max_concurrent_tasks));
        let max_retries_atomic = Arc::new(AtomicUsize::new(max_retries));

        let tasks = Arc::new(DashMap::new());

        // 🔥 创建任务槽池（使用 max_concurrent_tasks 作为最大槽位数）
        let task_slot_pool = Arc::new(TaskSlotPool::new(max_concurrent_tasks));

        // 🔥 启动槽位清理后台任务（托管模式，JoinHandle 会被保存以便 shutdown 时取消）
        {
            let pool = task_slot_pool.clone();
            tokio::spawn(async move {
                pool.start_cleanup_task_managed().await;
            });
        }

        // 🔥 创建加密配置存储（用于从 encryption.json 读取密钥）
        let encryption_config_store = Arc::new(EncryptionConfigStore::new(config_dir));

        let manager = Self {
            client: Arc::new(StdRwLock::new(client)),
            vip_type,
            tasks: tasks.clone(),
            waiting_queue: waiting_queue.clone(),
            server_health,
            scheduler: scheduler.clone(),
            max_concurrent_tasks: max_concurrent_tasks_atomic,
            max_retries: max_retries_atomic,
            persistence_manager: Arc::new(Mutex::new(None)),
            ws_manager: Arc::new(RwLock::new(None)),
            backup_notification_tx: Arc::new(RwLock::new(None)),
            task_slot_pool,
            encryption_config_store,
            snapshot_manager: Arc::new(RwLock::new(None)),
            backup_record_manager: Arc::new(RwLock::new(None)),
            dedup_index: DashMap::new(),
            dedup_reverse: DashMap::new(),
            active_count: Arc::new(AtomicUsize::new(0)),
            // 🔥 多账号归属（从 user_auth.uid 提取）——后续主创建/备份/恢复 3 个
            // UploadTask 路径都会链调 .with_owner_uid(self.owner_uid)。
            owner_uid: crate::auth::Uid::new(user_auth.uid),
        };

        // 🔥 设置槽位超时释放处理器
        manager.setup_stale_release_handler();

        // 启动后台任务：定期检查并启动等待队列中的任务
        manager.start_waiting_queue_monitor();

        // 🔥 启动活跃计数漂移校准（每 60 秒）
        {
            let tasks_ref = tasks.clone();
            let counter = manager.active_count.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    let mut real = 0usize;
                    for entry in tasks_ref.iter() {
                        if let Ok(task) = entry.task.try_lock() {
                            if matches!(
                                task.status,
                                UploadTaskStatus::Pending
                                    | UploadTaskStatus::Uploading
                                    | UploadTaskStatus::Encrypting
                                    | UploadTaskStatus::CheckingRapid
                            ) {
                                real += 1;
                            }
                        }
                    }
                    let stored = counter.load(Ordering::SeqCst);
                    if stored != real {
                        tracing::warn!(
                            "active_count 漂移校准: {} -> {}",
                            stored, real
                        );
                        counter.store(real, Ordering::SeqCst);
                    }
                }
            });
        }

        manager
    }

    /// 动态更新最大全局线程数
    pub fn update_max_threads(&self, new_max: usize) {
        self.scheduler.update_max_threads(new_max);
        info!("🔧 上传管理器: 动态调整全局最大线程数为 {}", new_max);
    }

    /// 热更新网盘客户端（代理回退/恢复时调用）
    ///
    /// 替换共享引用内的 NetdiskClient，已调度的上传任务在下次重试时自动使用新客户端。
    pub fn update_netdisk_client(&self, new_client: NetdiskClient) {
        *self.client.write().unwrap() = new_client;
        info!("✓ UploadManager NetdiskClient 已热更新");
    }

    /// 动态更新最大并发任务数
    ///
    /// async fn：`task_slot_pool.resize()` 是异步的。
    pub async fn update_max_concurrent_tasks(&self, new_max: usize) {
        let old_max = self.max_concurrent_tasks.swap(new_max, Ordering::SeqCst);

        // 🔥 同步更新任务槽池容量
        self.task_slot_pool.resize(new_max).await;

        // 同步更新调度器
        self.scheduler.update_max_concurrent_tasks(new_max);

        info!("🔧 动态调整上传最大并发任务数: {} -> {}", old_max, new_max);
    }

    /// 获取任务槽池引用
    pub fn task_slot_pool(&self) -> Arc<TaskSlotPool> {
        self.task_slot_pool.clone()
    }

    /// 动态更新最大重试次数
    pub fn update_max_retries(&self, new_max: u32) {
        self.max_retries.store(new_max as usize, Ordering::SeqCst);
        self.scheduler.update_max_retries(new_max);
        info!("🔧 上传管理器: 动态调整最大重试次数为 {}", new_max);
    }

    /// 🔥 设置 WebSocket 管理器
    pub async fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        let mut ws = self.ws_manager.write().await;
        *ws = Some(ws_manager);
        info!("上传管理器已设置 WebSocket 管理器");
    }

    /// 🔥 设置加密快照管理器（用于保存加密映射到 encryption_snapshots 表）
    pub async fn set_snapshot_manager(&self, snapshot_manager: Arc<SnapshotManager>) {
        let mut sm = self.snapshot_manager.write().await;
        *sm = Some(snapshot_manager);
        info!("上传管理器已设置加密快照管理器");
    }

    /// 🔥 设置备份记录管理器（用于文件夹名加密映射）
    pub async fn set_backup_record_manager(&self, record_manager: Arc<BackupRecordManager>) {
        let mut rm = self.backup_record_manager.write().await;
        *rm = Some(record_manager);
        info!("上传管理器已设置备份记录管理器");
    }

    /// 🔥 加密路径中的文件夹名（用于手动上传）
    /// 使用 "manual_upload" 作为 config_id
    async fn encrypt_folder_path_for_upload(&self, base_path: &str, relative_path: &str) -> Result<String> {
        use crate::encryption::service::EncryptionService;

        let record_manager = self.backup_record_manager.read().await;
        let record_manager = match record_manager.as_ref() {
            Some(rm) => rm,
            None => {
                // 没有设置 record_manager，返回原始路径
                return Ok(format!("{}/{}", base_path.trim_end_matches('/'), relative_path));
            }
        };

        // 🔥 获取当前密钥版本号
        let current_key_version = match self.encryption_config_store.get_current_key() {
            Ok(Some(key_info)) => key_info.key_version,
            Ok(None) => {
                warn!("encrypt_folder_path_for_upload: 未找到加密密钥，使用默认版本 1");
                1u32
            }
            Err(e) => {
                warn!("encrypt_folder_path_for_upload: 获取密钥版本失败: {}，使用默认版本 1", e);
                1u32
            }
        };

        let normalized_path = relative_path.replace('\\', "/");
        let path_parts: Vec<&str> = normalized_path.split('/').filter(|s| !s.is_empty()).collect();

        if path_parts.is_empty() {
            return Ok(base_path.trim_end_matches('/').to_string());
        }

        // 最后一个是文件名，不在这里加密
        let folder_parts = &path_parts[..path_parts.len() - 1];
        let file_name = path_parts.last().unwrap();

        let mut current_parent = base_path.trim_end_matches('/').to_string();
        let mut encrypted_parts = Vec::new();

        for folder_name in folder_parts {
            let encrypted_name = match record_manager.find_encrypted_folder_name(
                &current_parent, folder_name,
            )? {
                Some(name) => name,
                None => {
                    let new_name = EncryptionService::generate_encrypted_folder_name();
                    record_manager.add_folder_mapping(
                        &current_parent,
                        folder_name,
                        &new_name,
                        current_key_version,
                    )?;
                    debug!("创建文件夹映射: {} -> {} (parent={}, key_version={})", folder_name, new_name, current_parent, current_key_version);
                    new_name
                }
            };
            encrypted_parts.push(encrypted_name.clone());
            current_parent = format!("{}/{}", current_parent, encrypted_name);
        }

        let encrypted_folder_path = if encrypted_parts.is_empty() {
            base_path.trim_end_matches('/').to_string()
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), encrypted_parts.join("/"))
        };

        Ok(format!("{}/{}", encrypted_folder_path, file_name))
    }

    /// 🔥 发布上传事件
    async fn publish_event(&self, event: UploadEvent) {
        // 🔥 如果是备份任务，不发送普通的 WebSocket 事件
        // 备份任务的事件由 AutoBackupManager 统一处理
        if event.is_backup() {
            return;
        }

        let ws = self.ws_manager.read().await;
        if let Some(ref ws) = *ws {
            ws.send_if_subscribed(TaskEvent::Upload(event), None);
        }
    }

    /// 🔥 执行文件加密流程
    ///
    /// 在上传前对文件进行加密，返回加密后的临时文件路径
    ///
    /// # 参数
    /// * `task` - 任务引用
    /// * `task_id` - 任务ID
    /// * `local_path` - 原始文件路径
    /// * `original_size` - 原始文件大小
    /// * `is_backup` - 是否为备份任务
    /// * `ws_manager` - WebSocket 管理器（用于发送事件）
    /// * `task_slot_pool` - 任务槽池（用于失败时释放槽位）
    /// * `persistence_manager` - 持久化管理器（用于更新错误信息）
    /// * `encryption_config_store` - 加密配置存储（用于读取密钥）
    ///
    /// # 返回
    /// 加密后的临时文件路径，如果加密失败则返回错误
    async fn execute_encryption(
        task: &Arc<Mutex<UploadTask>>,
        task_id: &str,
        local_path: &Path,
        original_size: u64,
        is_backup: bool,
        ws_manager: Option<&Arc<crate::server::websocket::WebSocketManager>>,
        task_slot_pool: &Arc<TaskSlotPool>,
        persistence_manager: Option<&Arc<Mutex<PersistenceManager>>>,
        encryption_config_store: &Arc<EncryptionConfigStore>,
        backup_notification_tx: Option<&tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>>,
    ) -> Result<PathBuf> {
        use crate::autobackup::config::EncryptionAlgorithm;
        use crate::encryption::service::EncryptionService;
        use crate::server::events::BackupEvent;

        info!(
            "开始加密文件: task_id={}, path={:?}, size={}",
            task_id, local_path, original_size
        );

        // 🔥 获取备份任务相关的 ID（用于发送 BackupEvent）
        // 同时取出 owner_uid 用于事件
        let (backup_task_id, backup_file_task_id, file_name, owner_uid_raw) = {
            let t = task.lock().await;
            let file_name = local_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            (
                t.backup_task_id.clone(),
                t.backup_file_task_id.clone(),
                file_name,
                t.owner_uid.raw(),
            )
        };

        // 🔥 检查任务是否已有加密文件（例如暂停后恢复的情况）
        {
            let mut t = task.lock().await;
            if let Some(existing_encrypted_path) = t.encrypted_temp_path.clone() {
                if existing_encrypted_path.exists() {
                    // 获取加密文件的实际大小
                    match std::fs::metadata(&existing_encrypted_path) {
                        Ok(metadata) => {
                            let encrypted_size = metadata.len();
                            info!(
                                "任务 {} 已存在加密文件: {:?}，跳过重复加密，encrypted_size={}",
                                task_id, existing_encrypted_path, encrypted_size
                            );
                            // 🔥 确保状态和进度正确
                            t.encrypt_progress = 100.0;
                            // 🔥 获取加密文件名用于发送事件
                            let encrypted_name = existing_encrypted_path
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            drop(t); // 释放锁

                            // 🔥 发送加密完成事件（与正常流程一致）
                            if is_backup {
                                // 🔥 备份任务：发送 BackupEvent::FileEncrypted
                                if let (Some(ref b_task_id), Some(ref b_file_task_id)) = (&backup_task_id, &backup_file_task_id) {
                                    if let Some(ws) = ws_manager {
                                        ws.send_if_subscribed(
                                            TaskEvent::Backup(BackupEvent::FileEncrypted {
                                                task_id: b_task_id.clone(),
                                                file_task_id: b_file_task_id.clone(),
                                                file_name: file_name.clone(),
                                                encrypted_name,
                                                encrypted_size,

                                                owner_uid: Some(owner_uid_raw),
                                            }),
                                            None,
                                        );
                                        info!(
                                            "已发送备份加密完成事件(跳过加密): backup_task={}, file_task={}, encrypted_size={}",
                                            b_task_id, b_file_task_id, encrypted_size
                                        );
                                    }
                                }
                            } else {
                                if let Some(ws) = ws_manager {
                                    ws.send_if_subscribed(
                                        TaskEvent::Upload(UploadEvent::EncryptCompleted {
                                            task_id: task_id.to_string(),
                                            encrypted_size,
                                            original_size,
                                            is_backup: false,

                                            owner_uid: Some(owner_uid_raw),
                                        }),
                                        None,
                                    );
                                    info!(
                                        "已发送加密完成事件(跳过加密): task_id={}, original_size={}, encrypted_size={}",
                                        task_id, original_size, encrypted_size
                                    );
                                }
                            }
                            return Ok(existing_encrypted_path);
                        }
                        Err(e) => {
                            warn!(
                                "无法获取加密文件大小: {:?}, 错误: {}，将重新加密",
                                existing_encrypted_path, e
                            );
                            t.encrypted_temp_path = None;
                            t.encrypt_progress = 0.0;
                            // 继续执行下面的正常加密流程
                        }
                    }
                } else {
                    info!(
                        "任务 {} 的加密文件 {:?} 不存在，需要重新加密",
                        task_id, existing_encrypted_path
                    );
                    // 🔥 清除无效的加密文件路径
                    t.encrypted_temp_path = None;
                    t.encrypt_progress = 0.0;
                }
            }
        }

        // 1. 更新任务状态为 Encrypting
        {
            let mut t = task.lock().await;
            t.mark_encrypting();
        }

        // 2. 发送状态变更事件 (Pending -> Encrypting)
        if is_backup {
            // 🔥 备份任务：发送 BackupEvent::FileEncrypting
            if let (Some(ref b_task_id), Some(ref b_file_task_id)) = (&backup_task_id, &backup_file_task_id) {
                if let Some(ws) = ws_manager {
                    ws.send_if_subscribed(
                        TaskEvent::Backup(BackupEvent::FileEncrypting {
                            task_id: b_task_id.clone(),
                            file_task_id: b_file_task_id.clone(),
                            file_name: file_name.clone(),

                            owner_uid: Some(owner_uid_raw),
                        }),
                        None,
                    );
                    info!(
                        "已发送备份加密开始事件: backup_task={}, file_task={}, file={}",
                        b_task_id, b_file_task_id, file_name
                    );
                }
            }
        } else {
            if let Some(ws) = ws_manager {
                ws.send_if_subscribed(
                    TaskEvent::Upload(UploadEvent::StatusChanged {
                        task_id: task_id.to_string(),
                        old_status: "pending".to_string(),
                        new_status: "encrypting".to_string(),
                        is_backup: false,

                        owner_uid: Some(owner_uid_raw),
                    }),
                    None,
                );
                info!(
                    "已发送加密状态变更通知: {} (pending -> encrypting)",
                    task_id
                );
            }
        }

        // 3. 生成临时加密文件路径（使用应用的 config/temp 目录，与自动备份共用）
        let temp_dir = PathBuf::from("config/temp");
        // 确保临时目录存在
        if let Err(e) = std::fs::create_dir_all(&temp_dir) {
            let error_msg = format!("创建临时目录失败: {}", e);
            error!("{}", error_msg);
            {
                let mut t = task.lock().await;
                t.mark_failed(error_msg.clone());
            }
            task_slot_pool.release_fixed_slot(task_id).await;
            if let Some(ws) = ws_manager {
                ws.send_if_subscribed(
                    TaskEvent::Upload(UploadEvent::Failed {
                        task_id: task_id.to_string(),
                        error: error_msg,
                        is_backup: false,

                        owner_uid: Some(owner_uid_raw),
                    }),
                    None,
                );
            }
            return Err(anyhow::anyhow!("创建临时目录失败"));
        }

        // 🔥 从 task.remote_path 提取已有的加密文件名（在 create_task/create_backup_task 时已生成）
        // 这样可以确保与 snapshot 中保存的 encrypted_name 一致
        let encrypted_filename = {
            let t = task.lock().await;
            std::path::Path::new(&t.remote_path)
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    warn!("无法从 remote_path 提取加密文件名，生成新的: remote_path={}", t.remote_path);
                    EncryptionService::generate_encrypted_filename()
                })
        };
        let encrypted_path = temp_dir.join(&encrypted_filename);

        // 4. 从配置中读取加密密钥，如果不存在则生成新密钥并保存
        // 🔥 同时获取 key_version，用于保存到任务中（支持密钥轮换后解密）
        let (encryption_service, current_key_version) = match encryption_config_store.load() {
            Ok(Some(key_config)) => {
                info!("从 encryption.json 加载加密密钥成功, key_version={}", key_config.current.key_version);
                match EncryptionService::from_base64_key(
                    &key_config.current.master_key,
                    key_config.current.algorithm,
                ) {
                    Ok(service) => {
                        // 更新最后使用时间
                        if let Err(e) = encryption_config_store.update_last_used() {
                            warn!("更新加密密钥最后使用时间失败: {}", e);
                        }
                        (service, key_config.current.key_version)
                    }
                    Err(e) => {
                        warn!("加载加密密钥失败，密钥可能已损坏: {}，将生成新密钥", e);
                        let master_key = EncryptionService::generate_master_key();
                        let service =
                            EncryptionService::new(master_key, EncryptionAlgorithm::Aes256Gcm);
                        // 使用安全方法保存新生成的密钥（保留历史密钥）
                        match encryption_config_store.create_new_key_safe(
                            service.get_key_base64(),
                            EncryptionAlgorithm::Aes256Gcm,
                        ) {
                            Ok(config) => (service, config.current.key_version),
                            Err(e) => {
                                warn!("保存新生成的加密密钥失败: {}", e);
                                (service, 1u32)
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                info!("未找到已保存的加密密钥，生成新密钥");
                let master_key = EncryptionService::generate_master_key();
                let service = EncryptionService::new(master_key, EncryptionAlgorithm::Aes256Gcm);
                // 使用安全方法保存新生成的密钥（保留历史密钥）
                match encryption_config_store
                    .create_new_key_safe(service.get_key_base64(), EncryptionAlgorithm::Aes256Gcm)
                {
                    Ok(config) => (service, config.current.key_version),
                    Err(e) => {
                        warn!("保存新生成的加密密钥失败: {}", e);
                        (service, 1u32)
                    }
                }
            }
            Err(e) => {
                warn!("读取加密配置失败: {}，将生成新密钥", e);
                let master_key = EncryptionService::generate_master_key();
                let service = EncryptionService::new(master_key, EncryptionAlgorithm::Aes256Gcm);
                // 使用安全方法保存新生成的密钥（保留历史密钥）
                match encryption_config_store
                    .create_new_key_safe(service.get_key_base64(), EncryptionAlgorithm::Aes256Gcm)
                {
                    Ok(config) => (service, config.current.key_version),
                    Err(e) => {
                        warn!("保存新生成的加密密钥失败: {}", e);
                        (service, 1u32)
                    }
                }
            }
        };

        // 🔥 将当前 key_version 保存到任务中（用于解密时选择正确的密钥）
        {
            let mut t = task.lock().await;
            t.encryption_key_version = current_key_version;
        }

        // 5. 执行加密（带进度回调）
        // 使用 spawn_blocking 执行同步加密操作
        let local_path_clone = local_path.to_path_buf();
        let encrypted_path_clone = encrypted_path.clone();

        // 🔥 创建进度通道，用于从同步回调发送进度到异步上下文
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>();
        let task_id_for_progress = task_id.to_string();
        let ws_for_progress = ws_manager.cloned();
        let is_backup_for_progress = is_backup;
        let task_for_progress = task.clone(); // 🔥 克隆任务引用用于更新进度字段
        // 🔥 克隆备份相关 ID 用于进度事件
        let backup_task_id_for_progress = backup_task_id.clone();
        let backup_file_task_id_for_progress = backup_file_task_id.clone();
        let file_name_for_progress = file_name.clone();

        // 🔥 启动进度监听任务
        let progress_handle = tokio::spawn(async move {
            while let Some((processed, total)) = progress_rx.recv().await {
                let encrypt_progress = if total > 0 {
                    (processed as f64 / total as f64) * 100.0
                } else {
                    0.0
                };

                // 🔥 实时更新任务的 encrypt_progress 字段
                {
                    let mut t = task_for_progress.lock().await;
                    t.update_encrypt_progress(encrypt_progress);
                }

                if is_backup_for_progress {
                    // 🔥 备份任务：发送 BackupEvent::FileEncryptProgress
                    if let (Some(ref b_task_id), Some(ref b_file_task_id)) = (&backup_task_id_for_progress, &backup_file_task_id_for_progress) {
                        if let Some(ref ws) = ws_for_progress {
                            ws.send_if_subscribed(
                                TaskEvent::Backup(BackupEvent::FileEncryptProgress {
                                    task_id: b_task_id.clone(),
                                    file_task_id: b_file_task_id.clone(),
                                    file_name: file_name_for_progress.clone(),
                                    progress: encrypt_progress,
                                    processed_bytes: processed,
                                    total_bytes: total,

                                    owner_uid: Some(owner_uid_raw),
                                }),
                                None,
                            );
                        }
                    }
                } else {
                    if let Some(ref ws) = ws_for_progress {
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::EncryptProgress {
                                task_id: task_id_for_progress.clone(),
                                encrypt_progress,
                                processed_bytes: processed,
                                total_bytes: total,
                                is_backup: false,

                                owner_uid: Some(owner_uid_raw),
                            }),
                            None,
                        );
                    }
                }
            }
        });

        let encrypt_result = tokio::task::spawn_blocking(move || {
            let progress_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let last_progress_time =
                std::sync::Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

            encryption_service.encrypt_file_with_progress(
                &local_path_clone,
                &encrypted_path_clone,
                |processed, total| {
                    // 限制进度更新频率（每 100ms 或每 1% 更新一次）
                    let progress = (processed as f64 / total as f64) * 100.0;
                    let last_reported = progress_counter.load(std::sync::atomic::Ordering::Relaxed);
                    let current_progress = progress as u64;

                    let mut last_time = last_progress_time.lock().unwrap();
                    let elapsed = last_time.elapsed();

                    if current_progress > last_reported
                        || elapsed >= std::time::Duration::from_millis(100)
                    {
                        progress_counter
                            .store(current_progress, std::sync::atomic::Ordering::Relaxed);
                        *last_time = std::time::Instant::now();

                        // 🔥 通过 channel 发送进度到异步上下文
                        let _ = progress_tx.send((processed, total));
                    }
                },
            )
        })
            .await
            .map_err(|e| anyhow::anyhow!("加密任务执行失败: {}", e))?;

        // 🔥 等待进度监听任务结束（加密完成后 channel 会关闭）
        let _ = progress_handle.await;

        match encrypt_result {
            Ok(metadata) => {
                let encrypted_size = metadata.encrypted_size;

                // 6. 更新任务信息（mark_encrypt_completed 会同时将状态设置为 Uploading）
                // 🔥 传递加密元数据，用于上传完成后保存到 encryption_snapshots 表
                {
                    let mut t = task.lock().await;
                    t.mark_encrypt_completed(
                        encrypted_path.clone(),
                        encrypted_size,
                        encrypted_filename.clone(),
                        metadata.nonce.clone(),
                        metadata.algorithm.to_string(),
                        metadata.version,
                    );

                    // 🔥 注意：remote_path 已经在 create_task/create_backup_task 时设置好了
                    // 这里只是验证一下是否一致
                    let current_filename = std::path::Path::new(&t.remote_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("");
                    if current_filename != encrypted_filename {
                        warn!(
                            "remote_path 中的文件名与加密文件名不一致: remote_path={}, encrypted_filename={}",
                            t.remote_path, encrypted_filename
                        );
                    }
                }

                // 🔥 7. 持久化状态变更 (Encrypting -> Uploading) 和加密信息
                if let Some(pm) = persistence_manager {
                    use crate::persistence::types::TaskPersistenceStatus;
                    let pm_guard = pm.lock().await;

                    // 更新任务状态
                    if let Err(e) = pm_guard.update_task_status(task_id, TaskPersistenceStatus::Uploading) {
                        warn!("持久化加密完成状态失败: {}", e);
                    }

                    // 🔥 更新加密信息（encrypt_enabled 和 key_version）
                    if let Err(e) = pm_guard.update_encryption_info(task_id, true, Some(current_key_version)) {
                        warn!("持久化加密信息失败: {}", e);
                    } else {
                        debug!(
                            "已持久化加密信息: task_id={}, key_version={}",
                            task_id, current_key_version
                        );
                    }
                }

                // 8. 发送加密完成事件和状态变更通知
                if is_backup {
                    // 🔥 备份任务：发送 BackupEvent::FileEncrypted
                    if let (Some(ref b_task_id), Some(ref b_file_task_id)) = (&backup_task_id, &backup_file_task_id) {
                        if let Some(ws) = ws_manager {
                            ws.send_if_subscribed(
                                TaskEvent::Backup(BackupEvent::FileEncrypted {
                                    task_id: b_task_id.clone(),
                                    file_task_id: b_file_task_id.clone(),
                                    file_name: file_name.clone(),
                                    encrypted_name: encrypted_filename.clone(),
                                    encrypted_size,

                                    owner_uid: Some(owner_uid_raw),
                                }),
                                None,
                            );
                            info!(
                                "已发送备份加密完成事件: backup_task={}, file_task={}, file={}, encrypted_name={}, encrypted_size={}",
                                b_task_id, b_file_task_id, file_name, encrypted_filename, encrypted_size
                            );
                        }
                    }

                    // 同时发送 BackupTransferNotification 状态变更通知给 AutoBackupManager
                    if let Some(tx) = backup_notification_tx {
                        use crate::autobackup::events::TransferTaskType;
                        let notification = BackupTransferNotification::StatusChanged {
                            task_id: task_id.to_string(),
                            task_type: TransferTaskType::Upload,
                            old_status: crate::autobackup::events::TransferTaskStatus::Pending,
                            new_status: crate::autobackup::events::TransferTaskStatus::Transferring,
                        };
                        if let Err(e) = tx.send(notification) {
                            warn!("发送备份加密任务状态变更通知失败: {}", e);
                        } else {
                            info!(
                                "已发送备份加密任务状态变更通知: {} (Pending -> Transferring)",
                                task_id
                            );
                        }
                    }
                } else {
                    // 普通任务：发送 WebSocket 事件
                    if let Some(ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::EncryptCompleted {
                                task_id: task_id.to_string(),
                                encrypted_size,
                                original_size,
                                is_backup: false,

                                owner_uid: Some(owner_uid_raw),
                            }),
                            None,
                        );
                        info!(
                            "已发送加密完成事件: task_id={}, original_size={}, encrypted_size={}",
                            task_id, original_size, encrypted_size
                        );

                        // 🔥 发送状态变更事件 (Encrypting -> Uploading)
                        // 这确保前端在收到 EncryptCompleted 后查询状态时能得到正确的 Uploading 状态
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::StatusChanged {
                                task_id: task_id.to_string(),
                                old_status: "encrypting".to_string(),
                                new_status: "uploading".to_string(),
                                is_backup: false,

                                owner_uid: Some(owner_uid_raw),
                            }),
                            None,
                        );
                        info!(
                            "已发送状态变更事件: task_id={} (encrypting -> uploading)",
                            task_id
                        );
                    }
                }

                info!(
                    "文件加密完成: task_id={}, encrypted_path={:?}, original_size={}, encrypted_size={}",
                    task_id, encrypted_path, original_size, encrypted_size
                );

                Ok(encrypted_path)
            }
            Err(e) => {
                let error_msg = format!("文件加密失败: {}", e);
                error!("{}", error_msg);

                // 释放槽位
                task_slot_pool.release_fixed_slot(task_id).await;

                // 更新任务状态为失败
                {
                    let mut t = task.lock().await;
                    t.mark_failed(error_msg.clone());
                }

                // 发送失败事件
                if !is_backup {
                    if let Some(ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::Failed {
                                task_id: task_id.to_string(),
                                error: error_msg.clone(),
                                is_backup: false,

                                owner_uid: Some(owner_uid_raw),
                            }),
                            None,
                        );
                    }
                }

                // 更新持久化错误信息
                if let Some(pm) = persistence_manager {
                    if let Err(e) = pm
                        .lock()
                        .await
                        .update_task_error(task_id, error_msg.clone())
                    {
                        warn!("更新上传任务错误信息失败: {}", e);
                    }
                }

                // 清理可能已创建的临时文件
                if encrypted_path.exists() {
                    let _ = std::fs::remove_file(&encrypted_path);
                }

                Err(anyhow::anyhow!(error_msg))
            }
        }
    }

    /// 获取当前最大并发任务数
    pub fn max_concurrent_tasks(&self) -> usize {
        self.max_concurrent_tasks.load(Ordering::SeqCst)
    }

    /// 获取当前最大重试次数
    pub fn max_retries(&self) -> u32 {
        self.max_retries.load(Ordering::SeqCst) as u32
    }

    /// 获取调度器引用
    pub fn scheduler(&self) -> Arc<UploadChunkScheduler> {
        self.scheduler.clone()
    }

    /// 🔥 多账号：获取该管理器所属的账号 UID
    pub fn owner_uid(&self) -> crate::auth::Uid {
        self.owner_uid
    }

    /// 🔥 覆盖单个上传任务的归属 UID
    ///
    /// 用于 handler 接收到 `req.uid` 显式归属（含字段名 alias `owner_uid`）时，
    /// 纠正 `create_task` 默认使用的 `self.owner_uid`（启动时 active 账号）。同时
    /// 同步更新 `.meta` 持久化数据，使重启恢复后任务依然归属到正确账号。
    ///
    /// 语义：
    /// - 任务必须存在于 `self.tasks`，否则 warn 但不报错（已迁出 / 被删除）
    /// - 仅修改运行态 `UploadTask.owner_uid` + 持久化 `.meta`
    /// - 必须在任务实际开始上传分片之前调用，否则 BudgetScheduler 已按旧 owner 借调 permit
    pub async fn override_task_owner_uid(
        &self,
        task_id: &str,
        new_owner_uid: crate::auth::Uid,
    ) -> anyhow::Result<()> {
        // 1) 修改运行态 task
        // 🔥 克隆 task Arc 后释放 DashMap guard，再 await 任务锁，避免锁序倒置/死锁。
        let task_arc = self.tasks.get(task_id).map(|task_info| task_info.task.clone());
        if let Some(task_arc) = task_arc {
            let mut t = task_arc.lock().await;
            if t.owner_uid == new_owner_uid {
                return Ok(());
            }
            t.owner_uid = new_owner_uid;
        } else {
            tracing::warn!(
                "override_task_owner_uid: 上传任务不存在于运行态 tasks: {}（可能已迁出）",
                task_id
            );
        }

        // 2) 同步持久化 .meta
        let pm_opt = self.persistence_manager.lock().await.clone();
        if let Some(pm) = pm_opt {
            pm.lock()
                .await
                .update_upload_owner_uid(task_id, new_owner_uid.raw())
                .map_err(|e| anyhow::anyhow!("更新上传 .meta owner_uid 失败: {e}"))?;
        }
        Ok(())
    }

    /// 🔥 设置持久化管理器
    ///
    /// 由 AppState 在初始化时调用，注入持久化管理器
    pub async fn set_persistence_manager(&self, pm: Arc<Mutex<PersistenceManager>>) {
        let mut lock = self.persistence_manager.lock().await;
        *lock = Some(pm);
        info!("上传管理器已设置持久化管理器");
    }

    /// 获取持久化管理器引用的克隆
    pub async fn persistence_manager(&self) -> Option<Arc<Mutex<PersistenceManager>>> {
        self.persistence_manager.lock().await.clone()
    }

    /// 🔥 设置备份任务统一通知发送器
    ///
    /// AutoBackupManager 调用此方法设置 channel sender，
    /// 所有备份相关事件（进度、状态、完成、失败等）都通过此 channel 发送
    pub async fn set_backup_notification_sender(
        &self,
        tx: tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>,
    ) {
        // 设置到调度器（用于进度和完成/失败事件）
        self.scheduler
            .set_backup_notification_sender(tx.clone())
            .await;
        // 设置到管理器自身（用于状态变更事件，如暂停/恢复）
        let mut guard = self.backup_notification_tx.write().await;
        *guard = Some(tx);
        info!("上传管理器已设置备份任务统一通知发送器");
    }

    /// 创建上传任务
    ///
    /// # 参数
    /// * `local_path` - 本地文件路径
    /// * `remote_path` - 网盘目标路径
    /// * `encrypt` - 是否启用加密
    /// * `is_folder_upload` - 是否是文件夹上传的一部分（用于决定是否加密目录结构）
    ///
    /// # 返回
    /// 任务ID
    pub async fn create_task(
        &self,
        local_path: PathBuf,
        remote_path: String,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
    ) -> Result<String> {
        self.create_task_internal(local_path, remote_path, encrypt, is_folder_upload, conflict_strategy, None).await
    }

    /// 🔥 创建上传任务（显式指定 owner_uid）
    ///
    /// handler 接收到 `req.uid` 显式归属时调用此方法，task / `.meta` / Created event
    /// 在创建瞬间用同一 effective_uid，避免事后 `override_task_owner_uid` 带来的瞬时
    /// owner_uid 错乱。
    pub async fn create_task_with_owner(
        &self,
        local_path: PathBuf,
        remote_path: String,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid_override: crate::auth::Uid,
    ) -> Result<String> {
        self.create_task_internal(local_path, remote_path, encrypt, is_folder_upload, conflict_strategy, Some(owner_uid_override)).await
    }

    /// 内部上传任务创建实现
    #[allow(clippy::too_many_arguments)]
    async fn create_task_internal(
        &self,
        local_path: PathBuf,
        remote_path: String,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid_override: Option<crate::auth::Uid>,
    ) -> Result<String> {
        // 在 task 构造前确定 effective_uid，让 task / 持久化 / Created event 一致
        // 使用同一 owner_uid，避免事后纠错。
        let effective_uid = owner_uid_override.unwrap_or(self.owner_uid);
        // 获取文件大小
        let metadata = tokio::fs::metadata(&local_path)
            .await
            .context(format!("无法获取文件元数据: {:?}", local_path))?;

        if metadata.is_dir() {
            return Err(anyhow::anyhow!(
                "不支持直接上传目录，请使用 create_folder_task"
            ));
        }

        let file_size = metadata.len();

        // 获取冲突策略（如果未指定，使用默认值 SmartDedup）
        let strategy = conflict_strategy.unwrap_or(crate::uploader::UploadConflictStrategy::SmartDedup);

        // 创建任务（多账号：用 effective_uid，可能来自 handler 显式 override）
        let mut task = UploadTask::new(local_path.clone(), remote_path.clone(), file_size)
            .with_owner_uid(effective_uid);

        // 设置冲突策略
        task.conflict_strategy = strategy;

        // 🔥 设置加密标志
        task.encrypt_enabled = encrypt;
        task.original_size = file_size;

        // 保存原始远程路径（用于加密逻辑中的日志）
        let final_remote_path = remote_path.clone();

        // 🔥 如果启用加密，修改远程路径为加密文件名，并加密路径中的文件夹名
        if encrypt {
            use crate::encryption::service::EncryptionService;

            // 获取父目录和文件名
            let parent = std::path::Path::new(&remote_path)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();

            // 加密文件名
            let encrypted_filename = EncryptionService::generate_encrypted_filename();

            // 🔥 加密路径中的文件夹名
            // 注意：只有文件夹上传时才加密目录结构，普通文件上传只加密文件名
            // - 普通文件上传：用户指定的目标目录不加密，只加密文件名
            // - 文件夹上传：上传的文件夹名及其子目录需要加密
            let encrypted_parent = if is_folder_upload && !parent.is_empty() && parent != "/" {
                // 文件夹上传：需要加密目录结构
                // local_path 例如：C:\Users\xxx\你好2\子目录\file.txt
                // 本地文件夹名（你好2）在 remote_path 中的位置就是需要开始加密的位置
                let local_folder_name = local_path
                    .parent()
                    .and_then(|p| {
                        let mut current = p;
                        while let Some(name) = current.file_name() {
                            let name_str = name.to_string_lossy();
                            if parent.contains(&*name_str) {
                                return Some(name_str.to_string());
                            }
                            current = current.parent()?;
                        }
                        None
                    });

                if let Some(folder_name) = local_folder_name {
                    let parent_normalized = parent.replace('\\', "/");
                    if let Some(pos) = parent_normalized.find(&folder_name) {
                        let base_path = &parent_normalized[..pos].trim_end_matches('/');
                        let relative_path = &parent_normalized[pos..];

                        if !relative_path.is_empty() {
                            match self.encrypt_folder_path_for_upload(base_path, &format!("{}/dummy", relative_path)).await {
                                Ok(encrypted_path) => {
                                    encrypted_path.rsplit_once('/').map(|(p, _)| p.to_string()).unwrap_or(encrypted_path)
                                }
                                Err(e) => {
                                    warn!("加密文件夹路径失败，使用原始路径: {}", e);
                                    parent.clone()
                                }
                            }
                        } else {
                            parent.clone()
                        }
                    } else {
                        // 找不到文件夹名，使用原始逻辑
                        let parts: Vec<&str> = parent_normalized.split('/').filter(|s| !s.is_empty()).collect();
                        if parts.len() > 1 {
                            let base = format!("/{}", parts[0]);
                            let relative = parts[1..].join("/");
                            match self.encrypt_folder_path_for_upload(&base, &format!("{}/dummy", relative)).await {
                                Ok(encrypted_path) => {
                                    encrypted_path.rsplit_once('/').map(|(p, _)| p.to_string()).unwrap_or(encrypted_path)
                                }
                                Err(e) => {
                                    warn!("加密文件夹路径失败，使用原始路径: {}", e);
                                    parent.clone()
                                }
                            }
                        } else {
                            parent.clone()
                        }
                    }
                } else {
                    // 无法确定本地文件夹名，使用原始逻辑
                    let parent_normalized = parent.replace('\\', "/");
                    let parts: Vec<&str> = parent_normalized.split('/').filter(|s| !s.is_empty()).collect();
                    if parts.len() > 1 {
                        let base = format!("/{}", parts[0]);
                        let relative = parts[1..].join("/");
                        match self.encrypt_folder_path_for_upload(&base, &format!("{}/dummy", relative)).await {
                            Ok(encrypted_path) => {
                                encrypted_path.rsplit_once('/').map(|(p, _)| p.to_string()).unwrap_or(encrypted_path)
                            }
                            Err(e) => {
                                warn!("加密文件夹路径失败，使用原始路径: {}", e);
                                parent.clone()
                            }
                        }
                    } else {
                        parent.clone()
                    }
                }
            } else {
                // 普通文件上传：不加密目录，保持原始父目录
                parent.clone()
            };

            task.remote_path = if encrypted_parent.is_empty() {
                format!("/{}", encrypted_filename)
            } else {
                format!("{}/{}", encrypted_parent, encrypted_filename)
            };

            // 🔥 存储文件加密映射到 encryption_snapshots（状态为 pending）
            // 上传完成时会更新 nonce、algorithm 等字段并标记为 completed
            let original_filename = std::path::Path::new(&final_remote_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            // 🔥 从 encryption.json 获取正确的 key_version
            let snapshot_key_version = match self.encryption_config_store.get_current_key() {
                Ok(Some(key_info)) => key_info.key_version,
                Ok(None) => {
                    warn!("创建快照时未找到加密密钥配置，使用默认 key_version=1");
                    1u32
                }
                Err(e) => {
                    warn!("创建快照时读取加密密钥配置失败: {}，使用默认 key_version=1", e);
                    1u32
                }
            };

            if let Some(ref rm) = *self.backup_record_manager.read().await {
                // 使用 add_snapshot 存储文件映射（is_directory=false）
                use crate::autobackup::record::EncryptionSnapshot;
                let snapshot = EncryptionSnapshot {
                    config_id: "manual_upload".to_string(),
                    original_path: encrypted_parent.clone(),  // 父路径（已加密）
                    original_name: original_filename.clone(),
                    encrypted_name: encrypted_filename.clone(),
                    file_size,
                    nonce: String::new(),      // 上传时还没有 nonce，上传完成后更新
                    algorithm: String::new(),  // 上传时还没有算法，上传完成后更新
                    version: 1,
                    key_version: snapshot_key_version,
                    remote_path: task.remote_path.clone(),
                    is_directory: false,
                    status: "pending".to_string(),
                };
                if let Err(e) = rm.add_snapshot(&snapshot) {
                    warn!("存储文件加密映射失败: {}", e);
                } else {
                    debug!("存储文件加密映射: {} -> {}", original_filename, encrypted_filename);
                }
            }

            info!(
                "启用加密上传: 原始路径={}, 加密路径={}",
                final_remote_path, task.remote_path
            );
        }

        let task_id = task.id.clone();
        let final_remote_path = task.remote_path.clone();

        // 🔥 延迟创建分片管理器：只计算分片信息用于持久化，不实际创建分片管理器
        // 分片管理器会在预注册成功后（start_task_with_scheduler）才创建
        let chunk_size =
            crate::uploader::calculate_recommended_chunk_size(file_size, self.vip_type);
        let total_chunks = if file_size == 0 {
            0
        } else {
            file_size.div_ceil(chunk_size) as usize
        };

        // 计算最大并发分片数
        let max_concurrent_chunks = calculate_upload_task_max_chunks(file_size);

        info!(
            "创建上传任务: id={}, local={:?}, remote={}, size={}, chunks={}, max_concurrent={}, encrypt={} (分片管理器延迟创建)",
            task_id, local_path, final_remote_path, file_size, total_chunks, max_concurrent_chunks, encrypt
        );

        // 🔥 注册任务到持久化管理器（传递加密信息）
        // 从 encryption.json 获取正确的 key_version
        let current_key_version = if encrypt {
            match self.encryption_config_store.get_current_key() {
                Ok(Some(key_info)) => Some(key_info.key_version),
                Ok(None) => {
                    warn!("加密任务但未找到加密密钥配置，使用默认 key_version=1");
                    Some(1u32)
                }
                Err(e) => {
                    warn!("读取加密密钥配置失败: {}，使用默认 key_version=1", e);
                    Some(1u32)
                }
            }
        } else {
            None
        };

        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            // 显式传 effective_uid
            if let Err(e) = pm_arc.lock().await.register_upload_task(
                task_id.clone(),
                local_path.clone(),
                final_remote_path.clone(),
                file_size,
                chunk_size,
                total_chunks,
                Some(encrypt),  // 🔥 传递 encrypt_enabled
                current_key_version,  // 🔥 使用从 encryption.json 读取的正确 key_version
                Some(effective_uid.raw()),
            ) {
                warn!("注册上传任务到持久化管理器失败: {}", e);
            }
        }

        // 保存任务信息（🔥 分片管理器延迟创建，此处为 None）
        let task_info = UploadTaskInfo {
            task: Arc::new(Mutex::new(task)),
            chunk_manager: None, // 延迟创建：预注册成功后才创建
            cancel_token: CancellationToken::new(),
            max_concurrent_chunks,
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            is_paused: Arc::new(AtomicBool::new(false)),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
            last_speed_time: Arc::new(Mutex::new(std::time::Instant::now())),
            last_speed_bytes: Arc::new(AtomicU64::new(0)),
            restored_upload_id: None, // 新创建的任务没有恢复的 upload_id
            restored_completed_chunks: None, // 新创建的任务没有恢复的分片信息
        };

        self.tasks.insert(task_id.clone(), task_info);
        self.inc_active();

        // 🔥 发送任务创建事件
        // 用 effective_uid，与 task / .meta 一致，避免事后 override 路径下 Created event
        // 短暂带启动账号。
        self.publish_event(UploadEvent::Created {
            task_id: task_id.clone(),
            local_path: local_path.to_string_lossy().to_string(),
            remote_path: final_remote_path,
            total_size: file_size,
            is_backup: false,

            owner_uid: Some(effective_uid.raw()),
        })
            .await;

        Ok(task_id)
    }

    /// 批量创建上传任务
    ///
    /// # 参数
    /// * `files` - 文件列表 [(本地路径, 远程路径)]
    /// * `encrypt` - 是否启用加密
    /// * `conflict_strategy` - 冲突策略（可选）
    pub async fn create_batch_tasks(
        &self,
        files: Vec<(PathBuf, String)>,
        encrypt: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
    ) -> Result<Vec<String>> {
        // 普通批量上传，不是文件夹上传
        self.create_batch_tasks_internal(files, encrypt, false, conflict_strategy, None).await
    }

    /// 🔥 批量创建上传任务（显式指定 owner_uid）
    ///
    /// handler 接收到 `req.uid` 显式归属时，所有衍生 task / `.meta` / Created event
    /// 在创建瞬间用同一 effective_uid。
    pub async fn create_batch_tasks_with_owner(
        &self,
        files: Vec<(PathBuf, String)>,
        encrypt: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid_override: crate::auth::Uid,
    ) -> Result<Vec<String>> {
        self.create_batch_tasks_internal(files, encrypt, false, conflict_strategy, Some(owner_uid_override)).await
    }

    /// 内部批量创建上传任务
    ///
    /// # 参数
    /// * `files` - 文件列表 [(本地路径, 远程路径)]
    /// * `encrypt` - 是否启用加密
    /// * `is_folder_upload` - 是否是文件夹上传的一部分
    /// * `conflict_strategy` - 冲突策略（可选）
    /// * `owner_uid_override` - 显式 owner_uid 覆盖
    async fn create_batch_tasks_internal(
        &self,
        files: Vec<(PathBuf, String)>,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid_override: Option<crate::auth::Uid>,
    ) -> Result<Vec<String>> {
        let mut task_ids = Vec::with_capacity(files.len());

        for (local_path, remote_path) in files {
            match self
                .create_task_internal(local_path.clone(), remote_path, encrypt, is_folder_upload, conflict_strategy, owner_uid_override)
                .await
            {
                Ok(task_id) => {
                    // Skip "skipped" tasks
                    if task_id != "skipped" {
                        task_ids.push(task_id);
                    }
                }
                Err(e) => {
                    warn!("创建任务失败: {:?}, 错误: {}", local_path, e);
                }
            }
        }

        Ok(task_ids)
    }

    /// 创建文件夹上传任务
    ///
    /// # 参数
    /// * `local_folder` - 本地文件夹路径
    /// * `remote_folder` - 网盘目标文件夹路径
    /// * `scan_options` - 扫描选项（可选）
    /// * `encrypt` - 是否启用加密
    ///
    /// # 返回
    /// 所有创建的任务ID列表
    ///
    /// # 说明
    /// - 会递归扫描本地文件夹
    /// - 保持目录结构
    /// - 自动创建批量上传任务
    pub async fn create_folder_task<P: AsRef<Path>>(
        &self,
        local_folder: P,
        remote_folder: String,
        scan_options: Option<ScanOptions>,
        encrypt: bool,
    ) -> Result<Vec<String>> {
        let local_folder = local_folder.as_ref();

        info!(
            "开始创建文件夹上传任务: local={:?}, remote={}, encrypt={}",
            local_folder, remote_folder, encrypt
        );

        // 使用文件夹扫描器扫描文件
        let scanner = if let Some(options) = scan_options {
            FolderScanner::with_options(options)
        } else {
            FolderScanner::new()
        };

        let scanned_files = scanner.scan(local_folder)?;

        if scanned_files.is_empty() {
            return Err(anyhow::anyhow!("文件夹为空或无可上传文件"));
        }

        info!("扫描到 {} 个文件，开始创建上传任务", scanned_files.len());

        // 准备批量任务
        let mut tasks = Vec::with_capacity(scanned_files.len());

        for file in scanned_files {
            // 构建远程路径：remote_folder + relative_path
            let relative_path_str = file.relative_path.to_string_lossy().replace('\\', "/");

            // 🔥 方案A：不在这里加密，统一由 create_task 处理加密逻辑
            let remote_path = if remote_folder.ends_with('/') {
                format!("{}{}", remote_folder, relative_path_str)
            } else {
                format!("{}/{}", remote_folder, relative_path_str)
            };

            tasks.push((file.local_path, remote_path));
        }

        // 批量创建任务（文件夹上传，需要加密目录结构）
        let task_ids = self.create_batch_tasks_internal(tasks, encrypt, true, None, None).await?;

        info!("文件夹上传任务创建完成: 成功 {} 个", task_ids.len());

        Ok(task_ids)
    }

    /// 开始上传任务
    ///
    /// 🔥 职责：负责槽位分配，然后调用 start_task_internal 执行实际启动
    ///
    /// 分配槽位后调用 `start_task_internal`
    pub async fn start_task(&self, task_id: &str) -> Result<()> {
        // 🔥 从 DashMap 提取所需数据后立即释放 shard 锁，避免跨 await 持有
        let (local_path, remote_path, total_size, is_backup, existing_slot_id, task_arc) = {
            // 🔥 先 clone 出 task 的 Arc 再释放 DashMap ref，避免跨 await 持有 shard 锁
            let task_arc = self
                .tasks
                .get(task_id)
                .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?
                .task
                .clone();

            let task = task_arc.lock().await;
            match task.status {
                UploadTaskStatus::Pending | UploadTaskStatus::Paused => {}
                UploadTaskStatus::Uploading
                | UploadTaskStatus::CheckingRapid
                | UploadTaskStatus::Encrypting => {
                    return Err(anyhow::anyhow!("任务已在上传中"));
                }
                UploadTaskStatus::Completed | UploadTaskStatus::RapidUploadSuccess => {
                    return Err(anyhow::anyhow!("任务已完成"));
                }
                UploadTaskStatus::Failed => {
                    // 允许重试失败的任务
                }
            }
            let result = (
                task.local_path.clone(),
                task.remote_path.clone(),
                task.total_size,
                task.is_backup,
                task.slot_id,
                task_arc.clone(),
            );
            drop(task);
            result
        };

        // 🔥 上传前校验本地源文件存在性（快速失败，避免无谓的 locate / 槽位分配）
        //
        // 备份续传场景下，持久化的任务可能指向一个已被删除/移动的本地文件。
        // 文件缺失则立即失败并经由正确通道通知（备份任务走 backup:*）。
        // 注意：等待队列启动路径不经过本函数，因此 start_task_internal 也会再次校验。
        if !self
            .ensure_local_source_valid(task_id, &local_path, &task_arc)
            .await
        {
            return Ok(());
        }

        // 动态获取上传服务器列表（不再持有 DashMap 锁）
        let client_snapshot = self.client.read().unwrap().clone();
        match client_snapshot.locate_upload().await {
            Ok(servers) => {
                if !servers.is_empty() {
                    self.server_health.update_servers(servers);
                }
            }
            Err(e) => {
                warn!("获取上传服务器列表失败，使用默认服务器: {}", e);
            }
        }

        // 启动方式（唯一走 scheduler 模式）
        if existing_slot_id.is_some() {
            warn!(
                    "上传任务 {} 已有槽位 {:?}，直接启动 (is_backup={})",
                    task_id, existing_slot_id, is_backup
                );
        } else {
            let slot_allocation_result = if is_backup {
                self.task_slot_pool
                    .allocate_backup_slot(task_id)
                    .await
                    .map(|sid| (sid, None))
            } else {
                self.task_slot_pool
                    .allocate_fixed_slot_with_priority(task_id, false, TaskPriority::Normal)
                    .await
            };

            match slot_allocation_result {
                Some((slot_id, preempted_task_id)) => {
                    // 🔥 短暂获取 DashMap ref 更新槽位
                    {
                        let mut t = task_arc.lock().await;
                        t.slot_id = Some(slot_id);
                        t.is_borrowed_slot = false;
                    }

                    info!(
                            "上传任务 {} 分配槽位 {} (is_backup={}, 已用槽位: {}/{})",
                            task_id,
                            slot_id,
                            is_backup,
                            self.task_slot_pool.used_slots().await,
                            self.task_slot_pool.max_slots()
                        );

                    if let Some(preempted_id) = preempted_task_id {
                        info!(
                                "普通任务 {} 抢占了备份任务 {} 的槽位",
                                task_id, preempted_id
                            );
                        self.handle_preempted_backup_task(&preempted_id).await;
                    }
                }
                None => {
                    self.add_to_waiting_queue_by_priority(task_id, is_backup)
                        .await;

                    info!(
                            "上传任务 {} 加入等待队列（无可用槽位, is_backup={}）(已用槽位: {}/{}, 等待队列长度: {})",
                            task_id,
                            is_backup,
                            self.task_slot_pool.used_slots().await,
                            self.task_slot_pool.max_slots(),
                            self.waiting_queue.read().await.len()
                        );
                    return Ok(());
                }
            }
        }

        // 🔥 不再传递 DashMap ref
        self.start_task_internal(task_id, local_path, remote_path, total_size)
            .await
    }

    /// 内部方法：真正启动一个上传任务
    ///
    /// 🔥 职责：只检查任务是否有槽位，有槽位才启动
    /// 🔥 不负责槽位分配，槽位分配由 start_task 或 try_start_waiting_tasks 负责
    ///
    /// 该方法会：
    /// 1. 检查任务是否有槽位（没有槽位则加入等待队列）
    /// 2. 执行 precreate 并注册到调度器
    async fn start_task_internal(
        &self,
        task_id: &str,
        local_path: PathBuf,
        remote_path: String,
        total_size: u64,
    ) -> Result<()> {
        let scheduler = &self.scheduler;

        // 🔥 从 DashMap 获取任务信息并立即克隆所需字段，避免长时间持有 shard 锁
        // 🔥 先 clone 出 task 的 Arc 再释放 DashMap ref，避免跨 await 持有 shard 锁
        let task_arc = self.tasks.get(task_id)
            .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?
            .task
            .clone();

        let (is_backup, has_slot) = {
            let t = task_arc.lock().await;
            (t.is_backup, t.slot_id.is_some())
        };

        if !has_slot {
            // 没有槽位，加入等待队列
            warn!(
                "上传任务 {} 没有槽位，无法启动，加入等待队列 (is_backup={})",
                task_id, is_backup
            );
            self.add_to_waiting_queue_by_priority(task_id, is_backup)
                .await;
            return Ok(());
        }

        // 🔥 上传前校验本地源文件存在性（所有启动入口共享的兜底校验）
        //
        // 等待队列启动路径 try_start_waiting_tasks 直接调用本函数、不经过 start_task；
        // 且创建时文件存在、入队等待期间被删除的任务也只能在此处拦截。
        // 缺失则立即失败（备份任务走 backup:*）并释放已分配的槽位，避免：
        // 1) 任务注册到调度器后读不到分片数据、白等 5 分钟槽位超时；
        // 2) 槽位泄漏。
        let task_arc_for_check = task_arc.clone();
        if !self
            .ensure_local_source_valid(task_id, &local_path, &task_arc_for_check)
            .await
        {
            // fail_upload_task 内部已释放槽位池并清任务槽位字段，无需重复释放
            return Ok(());
        }
        // 重新获取 task_info（上面为做文件校验已释放）
        let task_info = self
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;

        info!(
            "启动上传任务: {} (has_slot=true, is_backup={})",
            task_id, is_backup
        );

        // 克隆需要的数据（从 task_info 中提取，然后立即释放 DashMap shard 锁）
        let task = task_info.task.clone();
        let cancel_token = task_info.cancel_token.clone();
        let is_paused = task_info.is_paused.clone();
        let active_chunk_count = task_info.active_chunk_count.clone();
        let max_concurrent_chunks = task_info.max_concurrent_chunks;
        let uploaded_bytes = task_info.uploaded_bytes.clone();
        let last_speed_time = task_info.last_speed_time.clone();
        let last_speed_bytes = task_info.last_speed_bytes.clone();
        let restored_upload_id = task_info.restored_upload_id.clone();
        let restored_completed_chunks = task_info.restored_completed_chunks.clone();
        // 🔥 立即释放 DashMap shard 锁，避免跨 await 持有
        drop(task_info);

        let server_health = self.server_health.clone();
        let client = self.client.clone();
        let scheduler = scheduler.clone();
        let task_id_string = task_id.to_string();
        let vip_type = self.vip_type;
        let task_slot_pool = self.task_slot_pool.clone();
        let persistence_manager = self.persistence_manager.lock().await.clone();
        let ws_manager = self.ws_manager.read().await.clone();
        let tasks = self.tasks.clone();
        let backup_notification_tx = self.backup_notification_tx.read().await.clone();
        // 🔥 克隆活跃计数器，用于后台失败时回滚 active_count
        let active_count = self.active_count.clone();
        // 同时取出 owner_uid
        let (is_backup, encrypt_enabled, original_size, owner_uid_raw) = {
            let t = task.lock().await;
            (t.is_backup, t.encrypt_enabled, t.original_size, t.owner_uid.raw())
        };
        let encryption_config_store = self.encryption_config_store.clone();
        let snapshot_manager = self.snapshot_manager.read().await.clone();

        // 在后台执行 precreate 并注册到调度器
        tokio::spawn(async move {
            info!("开始准备上传任务: {}", task_id_string);

            // 🔥 如果启用加密，先执行加密流程
            let actual_local_path = if encrypt_enabled {
                match Self::execute_encryption(
                    &task,
                    &task_id_string,
                    &local_path,
                    original_size,
                    is_backup,
                    ws_manager.as_ref(),
                    &task_slot_pool,
                    persistence_manager.as_ref(),
                    &encryption_config_store,
                    backup_notification_tx.as_ref(),
                )
                    .await
                {
                    Ok(encrypted_path) => encrypted_path,
                    Err(e) => {
                        error!("加密失败: {}", e);
                        return;
                    }
                }
            } else {
                local_path.clone()
            };

            // 🔥 如果启用加密，需要使用加密后文件的实际大小
            let actual_total_size = if encrypt_enabled {
                match tokio::fs::metadata(&actual_local_path).await {
                    Ok(metadata) => {
                        let encrypted_size = metadata.len();
                        info!(
                            "加密后文件大小: {} -> {} (原始: {}, 加密后: {})",
                            local_path.display(),
                            actual_local_path.display(),
                            total_size,
                            encrypted_size
                        );
                        // 同时更新任务的 total_size
                        {
                            let mut t = task.lock().await;
                            t.total_size = encrypted_size;
                        }
                        encrypted_size
                    }
                    Err(e) => {
                        let error_msg = format!("获取加密文件大小失败: {}", e);
                        error!("{}", error_msg);
                        Self::handle_upload_failure(
                            &task_id_string,
                            &task,
                            error_msg,
                            &task_slot_pool,
                            ws_manager.as_ref(),
                            persistence_manager.as_ref(),
                            backup_notification_tx.as_ref(),
                            &active_count,
                        )
                            .await;
                        return;
                    }
                }
            } else {
                total_size
            };

            // 🔥 如果启用了加密，mark_encrypt_completed 已经将状态设置为 Uploading
            // 只有非加密任务需要调用 mark_uploading()
            if !encrypt_enabled {
                // 标记为上传中
                {
                    let mut t = task.lock().await;
                    t.mark_uploading();
                }

                // 🔥 发送状态变更通知 (Pending -> Uploading)
                if is_backup {
                    // 备份任务：发送 BackupTransferNotification
                    if let Some(ref tx) = backup_notification_tx {
                        use crate::autobackup::events::TransferTaskType;
                        let notification = BackupTransferNotification::StatusChanged {
                            task_id: task_id_string.clone(),
                            task_type: TransferTaskType::Upload,
                            old_status: crate::autobackup::events::TransferTaskStatus::Pending,
                            new_status: crate::autobackup::events::TransferTaskStatus::Transferring,
                        };
                        if let Err(e) = tx.send(notification) {
                            warn!("发送备份上传任务传输状态通知失败: {}", e);
                        } else {
                            info!(
                                "已发送备份上传任务传输状态通知: {} (Pending -> Transferring)",
                                task_id_string
                            );
                        }
                    }
                } else {
                    // 普通任务：发送 UploadEvent::StatusChanged
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Upload(UploadEvent::StatusChanged {
                                task_id: task_id_string.clone(),
                                old_status: "pending".to_string(),
                                new_status: "uploading".to_string(),
                                is_backup: false,

                                owner_uid: Some(owner_uid_raw),
                            }),
                            None,
                        );
                        info!(
                            "已发送普通上传任务状态变更通知: {} (pending -> uploading)",
                            task_id_string
                        );
                    }
                }
            }
            // 🔥 加密任务的 StatusChanged 事件 (Encrypting -> Uploading) 已在 execute_encryption 的
            // mark_encrypt_completed 中处理，此处不再重复发送

            // 1. 计算 block_list（必须重新计算，因为它是按 4MB 固定大小计算的）
            // 🔥 使用 actual_local_path（如果启用加密，则为加密后的文件路径）
            let block_list = match crate::uploader::RapidUploadChecker::calculate_block_list(
                &actual_local_path,
                vip_type,
            )
                .await
            {
                Ok(bl) => bl,
                Err(e) => {
                    let error_msg = format!("计算 block_list 失败: {}", e);
                    error!("{}", error_msg);
                    Self::handle_upload_failure(
                        &task_id_string,
                        &task,
                        error_msg,
                        &task_slot_pool,
                        ws_manager.as_ref(),
                        persistence_manager.as_ref(),
                        backup_notification_tx.as_ref(),
                        &active_count,
                    )
                        .await;
                    return;
                }
            };

            // 2. 检查是否有恢复的 upload_id
            let upload_id = if let Some(restored_id) = restored_upload_id {
                info!(
                    "使用恢复的 upload_id: {} (如果合并失败，说明已过期，需要重新上传)",
                    restored_id
                );
                restored_id
            } else {
                // 没有恢复的 upload_id，需要调用 precreate
                // 🔥 使用 actual_total_size（加密后的文件大小）
                // 🔥 从共享引用读取最新客户端（代理热更新后自动生效）
                let client_snapshot = client.read().unwrap().clone();
                let rtype = {
                    let t = task.lock().await;
                    crate::uploader::conflict::conflict_strategy_to_rtype(t.conflict_strategy)
                };
                let precreate_response = match client_snapshot
                    .precreate(&remote_path, actual_total_size, &block_list, rtype)
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        let error_msg = format!("预创建文件失败: {}", e);
                        error!("{}", error_msg);
                        Self::handle_upload_failure(
                            &task_id_string,
                            &task,
                            error_msg,
                            &task_slot_pool,
                            ws_manager.as_ref(),
                            persistence_manager.as_ref(),
                            backup_notification_tx.as_ref(),
                            &active_count,
                        )
                            .await;
                        return;
                    }
                };

                // 检查秒传
                if precreate_response.is_rapid_upload() {
                    info!("秒传成功: {}", remote_path);
                    // 🔥 秒传成功，释放槽位（任务不会注册到调度器）
                    task_slot_pool.release_fixed_slot(&task_id_string).await;
                    let mut t = task.lock().await;
                    t.mark_rapid_upload_success();
                    return;
                }

                let new_upload_id = precreate_response.uploadid.clone();
                if new_upload_id.is_empty() {
                    let error_msg = "预创建失败：未获取到 uploadid".to_string();
                    error!("{}", error_msg);
                    Self::handle_upload_failure(
                        &task_id_string,
                        &task,
                        error_msg,
                        &task_slot_pool,
                        ws_manager.as_ref(),
                        persistence_manager.as_ref(),
                        backup_notification_tx.as_ref(),
                        &active_count,
                    )
                        .await;
                    return;
                }

                // 🔥 更新持久化元数据中的 upload_id
                if let Some(ref pm_arc) = persistence_manager {
                    if let Err(e) = pm_arc
                        .lock()
                        .await
                        .update_upload_id(&task_id_string, new_upload_id.clone())
                    {
                        warn!("更新上传任务 upload_id 失败: {}", e);
                    }
                }

                // 更新内存中的 restored_upload_id，使后续暂停/恢复可用
                if let Some(mut task_info) = tasks.get_mut(&task_id_string) {
                    task_info.restored_upload_id = Some(new_upload_id.clone());
                    info!(
                        "✓ 已保存 upload_id 到任务信息，支持暂停恢复: {}",
                        task_id_string
                    );
                }

                new_upload_id
            };

            // 3. 🔥 延迟创建分片管理器（只有预注册成功后才创建，节省内存）
            // 🔥 使用 actual_total_size（如果启用加密，则为加密后的文件大小）
            // 分片创建后立即 set_owner_uid，
            //    供 spawn_chunk_upload 路径 assert_chunk_owner 防跨账号污染。
            let chunk_owner_uid = task.lock().await.owner_uid;
            let chunk_manager = {
                let mut cm = UploadChunkManager::with_vip_type(actual_total_size, vip_type);
                cm.set_owner_uid(chunk_owner_uid);

                // 如果是恢复的任务，标记已完成的分片
                if let Some(ref restored_info) = restored_completed_chunks {
                    for &chunk_index in &restored_info.completed_chunks {
                        // chunk_md5s 是 Vec，通过索引获取
                        let md5 = restored_info.chunk_md5s.get(chunk_index).cloned().flatten();
                        cm.mark_completed(chunk_index, md5);
                    }
                    info!(
                        "上传任务 {} 恢复了 {} 个已完成分片",
                        task_id_string,
                        restored_info.completed_chunks.len()
                    );
                }

                Arc::new(Mutex::new(cm))
            };

            // 🔥 将创建的分片管理器保存回 tasks（用于暂停恢复等场景）
            if let Some(mut task_info) = tasks.get_mut(&task_id_string) {
                task_info.chunk_manager = Some(chunk_manager.clone());
            }

            // 4. 创建调度信息并注册到调度器
            // 🔥 使用 actual_local_path（如果启用加密，则为加密后的文件路径）
            // 🔥 使用 actual_total_size（如果启用加密，则为加密后的文件大小）
            let schedule_info = UploadTaskScheduleInfo {
                task_id: task_id_string.clone(),
                task: task.clone(),
                chunk_manager,
                server_health,
                client,
                local_path: actual_local_path,
                remote_path: remote_path.clone(),
                upload_id: upload_id.clone(),
                total_size: actual_total_size,
                block_list,
                cancellation_token: cancel_token,
                is_paused,
                is_merging: Arc::new(AtomicBool::new(false)),
                active_chunk_count,
                max_concurrent_chunks,
                uploaded_bytes,
                last_speed_time,
                last_speed_bytes,
                persistence_manager: persistence_manager.clone(),
                ws_manager: ws_manager.clone(),
                progress_throttler: Arc::new(ProgressThrottler::default()),
                backup_notification_tx: None,
                // 🔥 传入任务槽池引用，用于任务完成/失败时释放槽位
                task_slot_pool: Some(task_slot_pool.clone()),
                // 🔥 槽位刷新节流器（30秒间隔，防止槽位超时释放）
                slot_touch_throttler: Some(Arc::new(crate::task_slot_pool::SlotTouchThrottler::new(
                    task_slot_pool.clone(),
                    task_id_string.clone(),
                ))),
                // 🔥 传入加密快照管理器，用于上传完成后保存加密映射
                snapshot_manager,
                // 🔥 Manager 任务列表引用（用于任务完成时立即清理）
                manager_tasks: Some(tasks.clone()),
            };

            if let Err(e) = scheduler.register_task(schedule_info).await {
                error!("注册任务到调度器失败: {}", e);
                Self::handle_upload_failure(
                    &task_id_string,
                    &task,
                    format!("注册任务失败: {}", e),
                    &task_slot_pool,
                    ws_manager.as_ref(),
                    persistence_manager.as_ref(),
                    backup_notification_tx.as_ref(),
                    &active_count,
                )
                    .await;
                return;
            }

            info!("上传任务已注册到调度器: {}", task_id_string);

            // 注意：调度器会自动处理分片上传和完成
            // 这里不需要等待，调度器会在所有分片完成后调用 create_file
        });

        Ok(())
    }

    // 多账号上传唯一走 BudgetScheduler + per-uid UploadManager，分支唯一保留
    // scheduler 路径，旧的"独立模式"分支无法编译触达。

    /// 暂停上传任务
    ///
    /// # 参数
    /// - `task_id`: 任务ID
    /// - `skip_try_start_waiting`: 是否跳过尝试启动等待队列中的任务
    ///   - `true`: 跳过（用于批量暂停备份任务时，避免暂停一个任务后立即启动另一个等待任务）
    ///   - `false`: 正常行为，暂停后尝试启动等待队列中的任务
    pub async fn pause_task(&self, task_id: &str, skip_try_start_waiting: bool) -> Result<()> {
        // 🔥 先 clone 出 task / is_paused 的 Arc 再释放 DashMap ref，避免跨 await 持有 shard 锁
        let (task_arc, is_paused) = {
            let task_info = self
                .tasks
                .get(task_id)
                .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;
            (task_info.task.clone(), task_info.is_paused.clone())
        };

        // 设置暂停标志（调度器模式使用）
        is_paused.store(true, Ordering::SeqCst);

        let mut task = task_arc.lock().await;

        match task.status {
            UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid => {
                // 🔥 保存旧状态、槽位ID 用于发布 StatusChanged
                let old_status = format!("{:?}", task.status).to_lowercase();
                let is_backup = task.is_backup;
                let slot_id = task.slot_id;
                // 取真实 owner_uid
                let task_owner_uid_raw = task.owner_uid.raw();

                task.mark_paused();
                // 🔥 清除槽位ID
                task.slot_id = None;
                info!("暂停上传任务: {}", task_id);
                drop(task);

                // 🔥 活跃计数 -1（从 active → Paused）
                self.dec_active();

                // 🔥 释放槽位（暂停时释放，让其他任务可以使用）
                if let Some(sid) = slot_id {
                    self.task_slot_pool.release_fixed_slot(task_id).await;
                    info!("上传任务 {} 暂停，释放槽位 {}", task_id, sid);
                }

                // 🔥 发送状态变更事件
                self.publish_event(UploadEvent::StatusChanged {
                    task_id: task_id.to_string(),
                    old_status,
                    new_status: "paused".to_string(),
                    is_backup,

                    owner_uid: Some(task_owner_uid_raw),
                })
                    .await;

                // 🔥 发送暂停事件
                self.publish_event(UploadEvent::Paused {
                    task_id: task_id.to_string(),
                    is_backup,

                    owner_uid: Some(task_owner_uid_raw),
                })
                    .await;

                // 🔥 如果是备份任务，发送暂停通知到 AutoBackupManager
                if is_backup {
                    use crate::autobackup::events::TransferTaskType;
                    let tx_guard = self.backup_notification_tx.read().await;
                    if let Some(tx) = tx_guard.as_ref() {
                        let notification = BackupTransferNotification::Paused {
                            task_id: task_id.to_string(),
                            task_type: TransferTaskType::Upload,
                        };
                        let _ = tx.send(notification);
                    }
                }

                // 🔥 根据参数决定是否尝试启动等待队列中的任务
                if !skip_try_start_waiting {
                    self.try_start_waiting_tasks().await;
                }

                Ok(())
            }
            UploadTaskStatus::Pending => {
                // 🔥 暂停等待中的任务：从等待队列移除 + 标记为 Paused
                let is_backup = task.is_backup;
                // 取真实 owner_uid
                let task_owner_uid_raw = task.owner_uid.raw();

                task.mark_paused();
                info!("暂停等待中的上传任务: {}", task_id);
                drop(task);

                // 从等待队列中移除
                {
                    let mut queue = self.waiting_queue.write().await;
                    queue.retain(|id| id != task_id);
                }

                // 活跃计数 -1（Pending 也算活跃）
                self.dec_active();

                // 发送状态变更事件
                self.publish_event(UploadEvent::StatusChanged {
                    task_id: task_id.to_string(),
                    old_status: "pending".to_string(),
                    new_status: "paused".to_string(),
                    is_backup,

                    owner_uid: Some(task_owner_uid_raw),
                })
                    .await;

                // 发送暂停事件
                self.publish_event(UploadEvent::Paused {
                    task_id: task_id.to_string(),
                    is_backup,

                    owner_uid: Some(task_owner_uid_raw),
                })
                    .await;

                // 如果是备份任务，发送暂停通知到 AutoBackupManager
                if is_backup {
                    use crate::autobackup::events::TransferTaskType;
                    let tx_guard = self.backup_notification_tx.read().await;
                    if let Some(tx) = tx_guard.as_ref() {
                        let notification = BackupTransferNotification::Paused {
                            task_id: task_id.to_string(),
                            task_type: TransferTaskType::Upload,
                        };
                        let _ = tx.send(notification);
                    }
                }

                Ok(())
            }
            _ => Err(anyhow::anyhow!("任务当前状态不支持暂停")),
        }
    }

    /// 恢复上传任务（支持从 Paused 或 Failed 状态恢复）
    pub async fn resume_task(&self, task_id: &str) -> Result<()> {
        // 🔥 从 DashMap 提取数据后立即释放 shard 锁
        let old_status;
        let is_backup;
        let is_failed;
        // 取真实 owner_uid
        let task_owner_uid_raw;
        {
            // 🔥 先 clone 出 task / is_paused 的 Arc 再释放 DashMap ref，避免跨 await 持有 shard 锁
            let (task_arc, is_paused) = {
                let task_info = self
                    .tasks
                    .get(task_id)
                    .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;
                (task_info.task.clone(), task_info.is_paused.clone())
            };

            {
                let mut task = task_arc.lock().await;
                match task.status {
                    UploadTaskStatus::Paused => {
                        is_failed = false;
                        old_status = "paused".to_string();
                    }
                    UploadTaskStatus::Failed => {
                        is_failed = true;
                        old_status = "failed".to_string();
                        task.error = None;
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "任务当前状态不支持恢复: {:?}",
                            task.status
                        ));
                    }
                }
                task.status = UploadTaskStatus::Pending;
                is_backup = task.is_backup;
                task_owner_uid_raw = task.owner_uid.raw();
            }

            is_paused.store(false, Ordering::SeqCst);
        }

        // 🔥 DashMap ref 已释放，安全地执行 async 操作
        self.inc_active();

        // 🔥 发送状态变更事件
        self.publish_event(UploadEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status: old_status.clone(),
            new_status: "pending".to_string(),
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 发送恢复事件
        self.publish_event(UploadEvent::Resumed {
            task_id: task_id.to_string(),
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 如果是备份任务，发送状态变更和恢复通知到 AutoBackupManager
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                // 🔥 发送状态变更通知 (Paused/Failed -> Pending)
                let backup_old_status = if is_failed {
                    crate::autobackup::events::TransferTaskStatus::Failed
                } else {
                    crate::autobackup::events::TransferTaskStatus::Paused
                };
                let status_notification = BackupTransferNotification::StatusChanged {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Upload,
                    old_status: backup_old_status,
                    new_status: crate::autobackup::events::TransferTaskStatus::Pending,
                };
                if let Err(e) = tx.send(status_notification) {
                    warn!("发送备份任务等待状态通知失败: {}", e);
                } else {
                    info!(
                        "已发送备份上传任务等待状态通知: {} ({} -> Pending)",
                        task_id, old_status
                    );
                }

                // 发送恢复通知
                let notification = BackupTransferNotification::Resumed {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Upload,
                };
                let _ = tx.send(notification);
            }
        }

        // 重新开始任务
        self.start_task(task_id).await
    }

    /// 取消上传任务
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        // 从等待队列移除（如果存在）
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| id != task_id);
        }

        // 🔥 先 clone 出 task / cancel_token 的 Arc 再释放 DashMap ref，避免跨 await 持有 shard 锁
        let (task_arc, cancel_token) = {
            let task_info = self
                .tasks
                .get(task_id)
                .ok_or_else(|| anyhow::anyhow!("任务不存在: {}", task_id))?;
            (task_info.task.clone(), task_info.cancel_token.clone())
        };

        cancel_token.cancel();

        let (slot_id, was_active) = {
            let mut task = task_arc.lock().await;
            let active = matches!(
                task.status,
                UploadTaskStatus::Pending | UploadTaskStatus::Uploading
                | UploadTaskStatus::Encrypting | UploadTaskStatus::CheckingRapid
            );
            let sid = task.slot_id;
            task.slot_id = None;
            task.mark_failed("用户取消".to_string());
            (sid, active)
        };

        if was_active {
            self.dec_active();
        }

        // 🔥 DashMap ref 已释放，安全地执行 async 操作
        self.scheduler.cancel_task(task_id).await;

        info!("取消上传任务: {}", task_id);

        // 🔥 释放槽位
        if let Some(sid) = slot_id {
            self.task_slot_pool.release_fixed_slot(task_id).await;
            info!("上传任务 {} 取消，释放槽位 {}", task_id, sid);
        }

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

        Ok(())
    }

    /// 删除上传任务
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        // 从等待队列移除（如果存在）
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| id != task_id);
        }

        // 🔥 在移除任务之前获取 is_backup、slot_id 和活跃状态
        // 取真实 owner_uid
        // 🔥 先取消并克隆 task Arc，立即释放 DashMap guard，再 await 任务锁
        // （避免持 DashMap 分片 guard 跨 task.lock().await 形成锁序倒置/死锁）
        let task_arc_opt = self.tasks.get(task_id).map(|task_info| {
            task_info.cancel_token.cancel();
            task_info.task.clone()
        });
        let (is_backup, slot_id, was_active, task_owner_uid_raw) = if let Some(task_arc) = task_arc_opt {
            let task = task_arc.lock().await;
            let active = matches!(
                task.status,
                UploadTaskStatus::Pending | UploadTaskStatus::Uploading
                | UploadTaskStatus::Encrypting | UploadTaskStatus::CheckingRapid
            );
            (task.is_backup, task.slot_id, active, task.owner_uid.raw())
        } else {
            // 任务不存在内存中，尝试从历史读取
            if let Some(pm_arc) = self
                .persistence_manager
                .lock()
                .await
                .as_ref()
                .map(|pm| pm.clone())
            {
                let pm_guard = pm_arc.lock().await;
                if let Some(metadata) = pm_guard.get_history_task(task_id) {
                    let owner_raw = metadata.owner_uid.unwrap_or_else(|| self.owner_uid.raw());
                    (metadata.is_backup, None, false, owner_raw)
                } else {
                    (false, None, false, self.owner_uid.raw())
                }
            } else {
                (false, None, false, self.owner_uid.raw())
            }
        };

        // 🔥 释放槽位（在移除任务前）
        if let Some(sid) = slot_id {
            self.task_slot_pool.release_fixed_slot(task_id).await;
            info!("上传任务 {} 删除，释放槽位 {}", task_id, sid);
        }

        // 从调度器移除
        self.scheduler.cancel_task(task_id).await;

        // 移除任务
        self.tasks.remove(task_id);

        // 🔥 活跃计数 -1
        if was_active {
            self.dec_active();
        }

        // 清理去重索引
        self.remove_dedup_entry(task_id);

        // 🔥 清理持久化文件
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.on_task_deleted(task_id) {
                warn!("清理上传任务持久化文件失败: {}", e);
            }
        }

        info!("删除上传任务: {}", task_id);

        // 🔥 发送删除事件
        self.publish_event(UploadEvent::Deleted {
            task_id: task_id.to_string(),
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 备份任务：补送 BackupTransferNotification::Deleted。
        //    publish_event 对 is_backup=true 的任务直接跳过，不补发的话
        //    AutoBackupManager 看不到上传子任务被删除，父备份任务的
        //    pending_upload_task_ids / transfer_task_map 会残留，状态机可能一直
        //    卡在 Transferring / WaitingTransfer。与下载侧 delete_task 行为对齐。
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = BackupTransferNotification::Deleted {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Upload,
                };
                if let Err(e) = tx.send(notification) {
                    warn!(
                        "delete_task: 发送备份上传任务 Deleted 通知失败 (task_id={}): {}",
                        task_id, e
                    );
                }
            } else {
                warn!(
                    "delete_task: 备份上传任务 {} 被删除但 backup_notification_tx 未设置",
                    task_id
                );
            }
        }

        // 尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

        Ok(())
    }

    /// 批量删除上传任务（用于自动备份取消等场景）
    ///
    /// 与逐个调用 delete_task 相比，此方法：
    /// - 一次性清理 waiting_queue（O(n) 而非 O(n²)）
    /// - 仅在所有任务删除完成后调用一次 try_start_waiting_tasks
    /// - 避免重复的锁竞争和不必要的调度尝试
    pub async fn batch_delete_tasks(&self, task_ids: &[String]) -> (usize, usize) {
        if task_ids.is_empty() {
            return (0, 0);
        }

        let id_set: HashSet<&str> = task_ids.iter().map(|s| s.as_str()).collect();

        // 1. 一次性从 waiting_queue 移除所有目标任务
        {
            let mut queue = self.waiting_queue.write().await;
            queue.retain(|id| !id_set.contains(id.as_str()));
        }

        // 2. 逐个取消、释放槽位、移除任务（但不调用 try_start_waiting_tasks）
        let mut success = 0usize;
        let mut failed = 0usize;

        for task_id in task_ids {
            if let Err(e) = self.delete_task_internal(task_id).await {
                tracing::debug!("批量删除上传任务失败: task={}, error={}", task_id, e);
                failed += 1;
            } else {
                success += 1;
            }
        }

        // 3. 所有任务删除完成后，仅调用一次 try_start_waiting_tasks
        self.try_start_waiting_tasks().await;

        (success, failed)
    }

    /// 删除单个上传任务的内部实现（不触发 waiting_queue 清理和 try_start_waiting_tasks）
    async fn delete_task_internal(&self, task_id: &str) -> Result<()> {
        // 获取任务信息并取消
        // 取真实 owner_uid
        // 🔥 先取消并克隆 task Arc，释放 DashMap guard 后再 await 任务锁，避免锁序倒置/死锁。
        let task_arc_opt = self.tasks.get(task_id).map(|task_info| {
            task_info.cancel_token.cancel();
            task_info.task.clone()
        });
        let (is_backup, slot_id, was_active, task_owner_uid_raw) = if let Some(task_arc) = task_arc_opt {
            let task = task_arc.lock().await;
            let active = matches!(
                task.status,
                UploadTaskStatus::Pending | UploadTaskStatus::Uploading
                | UploadTaskStatus::Encrypting | UploadTaskStatus::CheckingRapid
            );
            (task.is_backup, task.slot_id, active, task.owner_uid.raw())
        } else {
            warn!("删除上传任务失败: 任务不存在: {}", task_id);
            return Err(anyhow::anyhow!("任务不存在: {}", task_id));
        };

        // 释放槽位
        if slot_id.is_some() {
            self.task_slot_pool.release_fixed_slot(task_id).await;
        }

        // 从调度器移除
        self.scheduler.cancel_task(task_id).await;

        // 移除任务
        self.tasks.remove(task_id);

        if was_active {
            self.dec_active();
        }

        // 清理去重索引
        self.remove_dedup_entry(task_id);

        // 清理持久化文件
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.on_task_deleted(task_id) {
                warn!("清理上传任务持久化文件失败: task_id={}, 错误: {}", task_id, e);
                // 🔥 持久化清理失败不应该导致整个删除操作失败
                // return Err(anyhow::anyhow!("清理持久化文件失败: {}", e));
            }
        }

        // 发送删除事件（备份任务会被 publish_event 跳过）
        self.publish_event(UploadEvent::Deleted {
            task_id: task_id.to_string(),
            is_backup,

            owner_uid: Some(task_owner_uid_raw),
        })
            .await;

        // 🔥 备份任务：补送 BackupTransferNotification::Deleted。
        //    与 delete_task 路径一致；批量删除经由 batch_delete_tasks 调入此函数，
        //    若不发通知，AutoBackupManager 看不到批量取消备份时的上传子任务删除事件，
        //    父备份任务的 pending_upload_task_ids / transfer_task_map 会残留。
        if is_backup {
            use crate::autobackup::events::TransferTaskType;
            let tx_guard = self.backup_notification_tx.read().await;
            if let Some(tx) = tx_guard.as_ref() {
                let notification = BackupTransferNotification::Deleted {
                    task_id: task_id.to_string(),
                    task_type: TransferTaskType::Upload,
                };
                if let Err(e) = tx.send(notification) {
                    warn!(
                        "delete_task_internal: 发送备份上传任务 Deleted 通知失败 (task_id={}): {}",
                        task_id, e
                    );
                }
            } else {
                warn!(
                    "delete_task_internal: 备份上传任务 {} 被删除但 backup_notification_tx 未设置",
                    task_id
                );
            }
        }

        Ok(())
    }

    /// 获取任务状态
    pub async fn get_task(&self, task_id: &str) -> Option<UploadTask> {
        // 🔥 先 clone 出 task 的 Arc 再释放 DashMap ref，避免跨 await 持 shard 锁
        let task_arc = self.tasks.get(task_id).map(|task_info| task_info.task.clone())?;
        let task = task_arc.lock().await;
        Some(task.clone())
    }

    /// 任务是否存在于**内存**中（不查历史库）。
    ///
    /// 用于跨账号路由的"内存优先"判定：内存命中即可确定归属为本 manager 的
    /// `owner_uid`（per-uid 独立 manager）。历史库为全局共享，无法据此判定归属，
    /// 故路由的历史回退路径改用 `metadata.owner_uid`（见
    /// `AppState::find_upload_manager_for_task`）。
    pub fn has_task_in_memory(&self, task_id: &str) -> bool {
        self.tasks.contains_key(task_id)
    }

    /// 检查任务是否存在于内存或持久化存储中
    pub async fn has_task_anywhere(&self, task_id: &str) -> bool {
        if self.tasks.contains_key(task_id) {
            return true;
        }
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm_guard = pm_arc.lock().await;
            if pm_guard.get_history_task(task_id).is_some() {
                return true;
            }
        }
        false
    }

    /// 获取所有任务（包括当前任务和历史任务，排除备份任务）
    pub async fn get_all_tasks(&self) -> Vec<UploadTask> {
        let mut tasks = Vec::new();

        // 获取当前任务（排除备份任务）
        // 🔥 先收集 task 的 Arc 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let task_arcs: Vec<_> = self.tasks.iter().map(|e| e.task.clone()).collect();
        for task_arc in task_arcs {
            let task = task_arc.lock().await;
            if !task.is_backup {
                tasks.push(task.clone());
            }
        }

        // 从历史数据库获取历史任务
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm = pm_arc.lock().await;

            // 从数据库查询已完成的上传任务（排除备份任务）
            if let Some((history_tasks, _total)) = pm.get_history_tasks_by_type_and_status(
                "upload",
                "completed",
                true,  // exclude_backup
                0,
                500,   // 限制最多500条
            ) {
                for metadata in history_tasks {
                    // 排除已在当前任务中的（避免重复）
                    if !self.tasks.contains_key(&metadata.task_id) {
                        if let Some(task) = Self::convert_history_to_task(&metadata) {
                            tasks.push(task);
                        }
                    }
                }
            }
        }

        // 按创建时间倒序排序
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        tasks
    }

    /// 获取所有备份任务
    pub async fn get_backup_tasks(&self) -> Vec<UploadTask> {
        let mut tasks = Vec::new();

        // 🔥 先收集 task 的 Arc 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let task_arcs: Vec<_> = self.tasks.iter().map(|e| e.task.clone()).collect();
        for task_arc in task_arcs {
            let task = task_arc.lock().await;
            if task.is_backup {
                tasks.push(task.clone());
            }
        }

        // 按创建时间倒序排序
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        tasks
    }

    /// 获取指定备份配置的任务
    pub async fn get_tasks_by_backup_config(&self, backup_config_id: &str) -> Vec<UploadTask> {
        let mut tasks = Vec::new();

        // 🔥 先收集 task 的 Arc 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let task_arcs: Vec<_> = self.tasks.iter().map(|e| e.task.clone()).collect();
        for task_arc in task_arcs {
            let task = task_arc.lock().await;
            if task.is_backup && task.backup_config_id.as_deref() == Some(backup_config_id) {
                tasks.push(task.clone());
            }
        }

        tasks
    }

    /// 创建备份上传任务
    ///
    /// 备份任务使用最低优先级，会在普通任务之后执行
    ///
    /// # 参数
    /// * `local_path` - 本地文件路径
    /// * `remote_path` - 网盘目标路径
    /// * `backup_config_id` - 备份配置ID
    /// * `encrypt_enabled` - 是否启用加密
    /// * `backup_task_id` - 备份主任务ID（用于发送 BackupEvent）
    /// * `backup_file_task_id` - 备份文件任务ID（用于发送 BackupEvent）
    /// * `conflict_strategy` - 冲突策略（可选，备份任务默认使用 SmartDedup）
    /// * `owner_uid` - 任务归属账号 UID
    ///
    /// # 返回
    /// 任务ID
    ///
    /// # 多账号说明
    /// 共享 `UploadManager` 设计下 `self.owner_uid` 不可靠（同一个 Arc 服务多账号）。
    /// AutoBackup 创建子上传任务时必须把 `BackupConfig.owner_uid` / `BackupTask.owner_uid`
    /// 显式传进来，否则衍生任务会落到启动账号或随机 manager owner，影响列表过滤、
    /// 预算调度、事件归属、删除账号扫描等。
    pub async fn create_backup_task(
        &self,
        local_path: PathBuf,
        remote_path: String,
        backup_config_id: String,
        encrypt_enabled: bool,
        backup_task_id: Option<String>,
        backup_file_task_id: Option<String>,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<String> {
        // 获取文件大小
        let metadata = tokio::fs::metadata(&local_path)
            .await
            .context(format!("无法获取文件元数据: {:?}", local_path))?;

        if metadata.is_dir() {
            return Err(anyhow::anyhow!(
                "不支持直接上传目录，请使用 create_folder_task"
            ));
        }

        let file_size = metadata.len();

        // 获取冲突策略（如果未指定，使用默认值 SmartDedup）
        let strategy = conflict_strategy.unwrap_or(crate::uploader::UploadConflictStrategy::SmartDedup);

        // 🔥 如果启用加密，修改远程路径为加密文件名（与 create_task 保持一致）
        let (actual_remote_path, encrypted_filename) = if encrypt_enabled {
            use crate::encryption::service::EncryptionService;

            let parent = std::path::Path::new(&remote_path)
                .parent()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();

            let enc_filename = EncryptionService::generate_encrypted_filename();

            let path = if parent.is_empty() || parent == "/" {
                format!("/{}", enc_filename)
            } else {
                format!("{}/{}", parent, enc_filename)
            };
            (path, Some(enc_filename))
        } else {
            (remote_path.clone(), None)
        };

        // 创建备份任务（链调 with_owner_uid 用调用方传入的 owner_uid，
        // 不再使用 self.owner_uid，因为共享 manager 下 self.owner_uid 不可靠）
        let mut task = UploadTask::new_backup(
            local_path.clone(),
            actual_remote_path.clone(),
            file_size,
            backup_config_id.clone(),
            encrypt_enabled,
            backup_task_id,
            backup_file_task_id,
        )
            .with_owner_uid(owner_uid);

        // 设置冲突策略
        task.conflict_strategy = strategy;

        let task_id = task.id.clone();

        // 🔥 如果启用加密，存储文件加密映射到 encryption_snapshots（状态为 pending）
        // 上传完成时会更新 nonce、algorithm 等字段并标记为 completed
        if let Some(ref enc_filename) = encrypted_filename {
            let original_filename = std::path::Path::new(&remote_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let parent = std::path::Path::new(&actual_remote_path)
                .parent()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();

            // 🔥 从 encryption.json 获取正确的 key_version
            let snapshot_key_version = match self.encryption_config_store.get_current_key() {
                Ok(Some(key_info)) => key_info.key_version,
                Ok(None) => {
                    warn!("创建备份快照时未找到加密密钥配置，使用默认 key_version=1");
                    1u32
                }
                Err(e) => {
                    warn!("创建备份快照时读取加密密钥配置失败: {}，使用默认 key_version=1", e);
                    1u32
                }
            };

            if let Some(ref rm) = *self.backup_record_manager.read().await {
                use crate::autobackup::record::EncryptionSnapshot;
                let snapshot = EncryptionSnapshot {
                    config_id: backup_config_id.clone(),
                    original_path: parent.clone(),
                    original_name: original_filename.clone(),
                    encrypted_name: enc_filename.clone(),
                    file_size,
                    nonce: String::new(),      // 上传时还没有 nonce，上传完成后更新
                    algorithm: String::new(),  // 上传时还没有算法，上传完成后更新
                    version: 1,
                    key_version: snapshot_key_version,
                    remote_path: actual_remote_path.clone(),
                    is_directory: false,
                    status: "pending".to_string(),
                };
                if let Err(e) = rm.add_snapshot(&snapshot) {
                    warn!("存储备份文件加密映射失败: {}", e);
                } else {
                    debug!("存储备份文件加密映射: {} -> {}", original_filename, enc_filename);
                }
            }

            info!(
                "启用加密备份上传: 原始路径={}, 加密路径={}",
                remote_path, actual_remote_path
            );
        }

        // 🔥 延迟创建分片管理器：只计算分片信息用于持久化，不实际创建分片管理器
        // 分片管理器会在预注册成功后（start_task_with_scheduler）才创建
        let chunk_size =
            crate::uploader::calculate_recommended_chunk_size(file_size, self.vip_type);
        let total_chunks = if file_size == 0 {
            0
        } else {
            file_size.div_ceil(chunk_size) as usize
        };

        // 计算最大并发分片数
        let max_concurrent_chunks = calculate_upload_task_max_chunks(file_size);

        info!(
            "创建备份上传任务: id={}, local={:?}, remote={}, size={}, chunks={}, backup_config={}, encrypt={} (分片管理器延迟创建)",
            task_id, local_path, actual_remote_path, file_size, total_chunks, backup_config_id, encrypt_enabled
        );

        // 🔥 注册备份任务到持久化管理器
        // 从 encryption.json 获取正确的 key_version
        let current_key_version = if encrypt_enabled {
            match self.encryption_config_store.get_current_key() {
                Ok(Some(key_info)) => Some(key_info.key_version),
                Ok(None) => {
                    warn!("备份加密任务但未找到加密密钥配置，使用默认 key_version=1");
                    Some(1u32)
                }
                Err(e) => {
                    warn!("读取加密密钥配置失败: {}，使用默认 key_version=1", e);
                    Some(1u32)
                }
            }
        } else {
            None
        };

        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.register_upload_backup_task(
                task_id.clone(),
                local_path.clone(),
                actual_remote_path.clone(),
                file_size,
                chunk_size,
                total_chunks,
                backup_config_id.clone(),
                Some(encrypt_enabled),
                current_key_version,  // 🔥 使用从 encryption.json 读取的正确 key_version
            ) {
                warn!("注册备份上传任务到持久化管理器失败: {}", e);
            }
        }

        // 保存任务信息（🔥 分片管理器延迟创建，此处为 None）
        let task_info = UploadTaskInfo {
            task: Arc::new(Mutex::new(task)),
            chunk_manager: None, // 延迟创建：预注册成功后才创建
            cancel_token: CancellationToken::new(),
            max_concurrent_chunks,
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            is_paused: Arc::new(AtomicBool::new(false)),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
            last_speed_time: Arc::new(Mutex::new(std::time::Instant::now())),
            last_speed_bytes: Arc::new(AtomicU64::new(0)),
            restored_upload_id: None,
            restored_completed_chunks: None, // 新创建的备份任务没有恢复的分片信息
        };

        self.tasks.insert(task_id.clone(), task_info);
        self.inc_active();

        // 发送任务创建事件（备份任务也发送事件，但前端可以根据 is_backup 过滤）
        // Created 事件必须携带调用方传入的
        // owner_uid（与 task 本体 .with_owner_uid(owner_uid) 一致），而非 self.owner_uid。
        // 共享 UploadManager 下 self.owner_uid 不可靠，否则 WS 事件 owner 会污染到
        // 错误账号，导致前端账号过滤/任务卡首次显示短暂归错账号。
        self.publish_event(UploadEvent::Created {
            task_id: task_id.clone(),
            local_path: local_path.to_string_lossy().to_string(),
            remote_path:actual_remote_path,
            total_size: file_size,
            is_backup: true,

            owner_uid: Some(owner_uid.raw()),
        })
            .await;

        Ok(task_id)
    }

    /// 将历史元数据转换为上传任务
    fn convert_history_to_task(metadata: &TaskMetadata) -> Option<UploadTask> {
        // 验证必要字段
        let local_path = metadata.source_path.clone()?;
        let remote_path = metadata.target_path.clone()?;
        let file_size = metadata.file_size.unwrap_or(0);

        Some(UploadTask {
            id: metadata.task_id.clone(),
            // 🔥 多账号归属（从 metadata 恢复，缺失时为 Uid(0)）
            owner_uid: metadata.owner_uid.map(crate::auth::types::Uid::new).unwrap_or_default(),
            // 🔥 不可恢复失败原因（历史已完成任务无失败原因）
            failure_reason: None,
            local_path,
            remote_path,
            total_size: file_size,
            uploaded_size: file_size, // 已完成的任务
            status: UploadTaskStatus::Completed,
            speed: 0,
            created_at: metadata.created_at.timestamp(),
            started_at: Some(metadata.created_at.timestamp()),
            completed_at: metadata.completed_at.map(|t| t.timestamp()),
            error: None,
            is_rapid_upload: false,
            content_md5: None,
            slice_md5: None,
            content_crc32: None,
            group_id: None,
            group_root: None,
            relative_path: None,
            total_chunks: metadata.total_chunks.unwrap_or(0),
            completed_chunks: metadata.total_chunks.unwrap_or(0), // 已完成的任务
            // 自动备份字段（从 metadata 恢复）
            is_backup: metadata.is_backup,
            backup_config_id: metadata.backup_config_id.clone(),
            backup_task_id: None, // 历史任务无备份任务ID
            backup_file_task_id: None, // 历史任务无文件任务ID
            // 任务槽位字段（历史任务无槽位信息）
            slot_id: None,
            is_borrowed_slot: false,
            // 加密字段（从 metadata 恢复）
            encrypt_enabled: metadata.encrypt_enabled,
            encrypt_progress: 0.0,
            encrypted_temp_path: None,
            original_size: file_size,
            // 加密映射元数据（历史任务无加密映射）
            encrypted_name: None,
            encryption_nonce: None,
            encryption_algorithm: None,
            encryption_version: 0,
            // 🔥 从 metadata 恢复 key_version，如果没有则使用默认值 1
            encryption_key_version: metadata.encryption_key_version.unwrap_or(1),
            // 冲突策略（历史任务使用默认值）
            conflict_strategy: crate::uploader::UploadConflictStrategy::default(),
        })
    }

    /// 获取正在传输的任务数（Uploading/CheckingRapid）
    pub fn transferring_task_count(&self) -> usize {
        let mut count = 0;
        for entry in self.tasks.iter() {
            if let Ok(task) = entry.task.try_lock() {
                if matches!(
                    task.status,
                    UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid
                ) {
                    count += 1;
                }
            }
        }
        count
    }

    /// 获取活跃任务数（Pending/Uploading/Encrypting/CheckingRapid），O(1)
    pub fn active_task_count(&self) -> usize {
        self.active_count.load(Ordering::SeqCst)
    }

    /// 活跃计数 +1
    fn inc_active(&self) {
        self.active_count.fetch_add(1, Ordering::SeqCst);
    }

    /// 活跃计数 -1
    fn dec_active(&self) {
        let prev = self.active_count.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            // 防止下溢，修正为 0
            self.active_count.store(0, Ordering::SeqCst);
        }
    }

    /// 清除已完成的任务
    pub async fn clear_completed(&self) -> usize {
        let mut to_remove = Vec::new();

        // 1. 收集内存中的已完成任务
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if matches!(
                task.status,
                UploadTaskStatus::Completed | UploadTaskStatus::RapidUploadSuccess
            ) {
                to_remove.push(task_id);
            }
        }

        // 2. 从内存中移除
        let memory_count = to_remove.len();
        for task_id in &to_remove {
            self.remove_dedup_entry(task_id);
            self.tasks.remove(task_id);
        }

        // 3. 从历史数据库中清除已完成任务
        let mut history_count = 0;
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm_guard = pm_arc.lock().await;
            let history_db = pm_guard.history_db().cloned();

            // 释放 pm_guard，避免长时间持锁
            drop(pm_guard);

            // 从历史数据库中删除已完成的上传任务
            if let Some(db) = history_db {
                match db.remove_tasks_by_type_and_status("upload", "completed") {
                    Ok(count) => {
                        history_count = count;
                    }
                    Err(e) => {
                        warn!("从历史数据库删除已完成上传任务失败: {}", e);
                    }
                }
            }
        }

        let total_count = memory_count + history_count;
        info!(
            "清除了 {} 个已完成的上传任务（内存: {}, 历史: {}）",
            total_count, memory_count, history_count
        );
        total_count
    }

    /// 清除失败的任务
    pub async fn clear_failed(&self) -> usize {
        let mut removed = 0;
        let mut to_remove = Vec::new();

        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if matches!(task.status, UploadTaskStatus::Failed) {
                to_remove.push(task_id);
            }
        }

        for task_id in to_remove {
            self.remove_dedup_entry(&task_id);
            self.tasks.remove(&task_id);
            removed += 1;
        }

        info!("清除了 {} 个失败的上传任务", removed);
        removed
    }

    // ==================== 批量操作方法 ====================

    /// 批量暂停上传任务
    pub async fn batch_pause(&self, task_ids: &[String]) -> Vec<(String, bool, Option<String>)> {
        let mut results = Vec::with_capacity(task_ids.len());
        for id in task_ids {
            match self.pause_task(id, true).await {
                Ok(_) => results.push((id.clone(), true, None)),
                Err(e) => results.push((id.clone(), false, Some(e.to_string()))),
            }
        }
        self.try_start_waiting_tasks().await;
        results
    }

    /// 批量恢复上传任务
    pub async fn batch_resume(&self, task_ids: &[String]) -> Vec<(String, bool, Option<String>)> {
        info!("批量恢复上传任务: 共 {} 个任务", task_ids.len());
        let mut results = Vec::with_capacity(task_ids.len());
        let mut success_count = 0;
        let mut fail_count = 0;

        for id in task_ids {
            match self.resume_task(id).await {
                Ok(_) => {
                    results.push((id.clone(), true, None));
                    success_count += 1;
                }
                Err(e) => {
                    results.push((id.clone(), false, Some(e.to_string())));
                    fail_count += 1;
                    warn!("恢复上传任务 {} 失败: {}", id, e);
                }
            }
        }

        info!(
            "批量恢复上传任务完成: 成功 {}, 失败 {}, 总计 {}",
            success_count, fail_count, task_ids.len()
        );

        // 批量恢复后尝试启动等待队列中的任务
        self.try_start_waiting_tasks().await;

        results
    }

    /// 批量删除上传任务
    pub async fn batch_delete(&self, task_ids: &[String]) -> Vec<(String, bool, Option<String>)> {
        let mut results = Vec::with_capacity(task_ids.len());
        for id in task_ids {
            match self.delete_task(id).await {
                Ok(_) => results.push((id.clone(), true, None)),
                Err(e) => results.push((id.clone(), false, Some(e.to_string()))),
            }
        }
        results
    }

    /// 获取可暂停的任务ID列表
    pub async fn get_pausable_task_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            // 🔥 只返回非备份任务（上传管理页面不应操作自动备份任务）
            if !task.is_backup && matches!(task.status, UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid | UploadTaskStatus::Pending) {
                ids.push(task_id);
            }
        }
        ids
    }

    /// 获取可恢复的任务ID列表
    pub async fn get_resumable_task_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            // 🔥 只返回非备份任务（上传管理页面不应操作自动备份任务）
            if !task.is_backup && matches!(task.status, UploadTaskStatus::Paused | UploadTaskStatus::Failed) {
                ids.push(task_id);
            }
        }
        tracing::info!("获取可恢复的上传任务: 找到 {} 个非备份任务（Paused 或 Failed 状态）", ids.len());
        ids
    }

    /// 获取所有任务ID列表（用于批量删除）
    pub fn get_all_task_ids(&self) -> Vec<String> {
        self.tasks.iter().map(|e| e.key().clone()).collect()
    }

    // ============================================================
    // 按 owner_uid 过滤的批量操作助手
    //
    // 共享 `UploadManager` 设计（同一个 Arc 在 DashMap 中映射给所有账号）下，
    // 「全部暂停 / 全部恢复 / 全部删除 / 清除已完成 / 清除失败」必须按
    // `task.owner_uid == uid` 过滤，否则会跨账号误操作。
    // ============================================================

    /// 获取属于指定账号的可暂停任务ID列表
    pub async fn get_pausable_task_ids_for_uid(&self, uid: crate::auth::Uid) -> Vec<String> {
        let mut ids = Vec::new();
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if task.owner_uid == uid
                && !task.is_backup
                && matches!(
                    task.status,
                    UploadTaskStatus::Uploading
                        | UploadTaskStatus::CheckingRapid
                        | UploadTaskStatus::Pending
                )
            {
                ids.push(task_id);
            }
        }
        ids
    }

    /// 获取属于指定账号的可恢复任务ID列表
    pub async fn get_resumable_task_ids_for_uid(&self, uid: crate::auth::Uid) -> Vec<String> {
        let mut ids = Vec::new();
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if task.owner_uid == uid
                && !task.is_backup
                && matches!(
                    task.status,
                    UploadTaskStatus::Paused | UploadTaskStatus::Failed
                )
            {
                ids.push(task_id);
            }
        }
        tracing::info!(
            "获取可恢复的上传任务（owner_uid={}）: 找到 {} 个非备份任务",
            uid.raw(),
            ids.len()
        );
        ids
    }

    /// 获取属于指定账号的所有任务ID列表（备份任务排除）
    pub async fn get_all_task_ids_for_uid(&self, uid: crate::auth::Uid) -> Vec<String> {
        let mut ids = Vec::new();
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if task.owner_uid == uid && !task.is_backup {
                ids.push(task_id);
            }
        }
        ids
    }

    /// 校验显式提交的 task_ids 是否都属于指定账号
    ///
    /// 共享 `UploadManager` 设计下，handler 只用 `effective_uid` 路由是不够的——
    /// 前端直接传 `task_ids` 时也必须每条校验 `task.owner_uid == uid`，否则 A 账号
    /// 上下文可以操作 B 账号任务。
    ///
    /// 返回 `(allowed, denied)`：
    /// - `allowed`：所有 `task.owner_uid == uid` 且任务存在的 id
    /// - `denied`：`(id, reason)`
    pub async fn validate_task_ids_for_uid(
        &self,
        uid: crate::auth::Uid,
        task_ids: &[String],
    ) -> (Vec<String>, Vec<(String, String)>) {
        let mut allowed = Vec::new();
        let mut denied = Vec::new();
        for id in task_ids {
            // 🔥 先 clone 出 task 的 Arc 再释放 DashMap ref，避免跨 await 持 shard 锁
            let task_arc = self.tasks.get(id).map(|task_info| task_info.task.clone());
            match task_arc {
                Some(task_arc) => {
                    let t = task_arc.lock().await;
                    if t.owner_uid != uid {
                        denied.push((
                            id.clone(),
                            format!(
                                "任务不属于当前账号（task.owner_uid={}, 请求 uid={}）",
                                t.owner_uid.raw(),
                                uid.raw()
                            ),
                        ));
                    } else if t.is_backup {
                        denied.push((
                            id.clone(),
                            "备份任务不允许通过上传管理批量接口操作".to_string(),
                        ));
                    } else {
                        allowed.push(id.clone());
                    }
                }
                None => {
                    denied.push((id.clone(), "任务不存在".to_string()));
                }
            }
        }
        (allowed, denied)
    }

    /// 清除指定账号下已完成的上传任务
    pub async fn clear_completed_for_uid(&self, uid: crate::auth::Uid) -> usize {
        let mut to_remove = Vec::new();

        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if task.owner_uid == uid
                && matches!(
                    task.status,
                    UploadTaskStatus::Completed | UploadTaskStatus::RapidUploadSuccess
                )
            {
                to_remove.push(task_id);
            }
        }

        let memory_count = to_remove.len();
        for task_id in &to_remove {
            self.remove_dedup_entry(task_id);
            self.tasks.remove(task_id);
        }

        // 历史数据库：仅删该 owner_uid 的已完成上传记录
        let mut history_count = 0;
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm_guard = pm_arc.lock().await;
            let history_db = pm_guard.history_db().cloned();
            drop(pm_guard);

            if let Some(db) = history_db {
                match db.remove_tasks_by_type_status_owner("upload", "completed", Some(uid.raw())) {
                    Ok(count) => history_count = count,
                    Err(e) => warn!(
                        "从历史数据库删除已完成上传任务（按 owner_uid={}）失败: {}",
                        uid.raw(),
                        e
                    ),
                }
            }
        }

        let total_count = memory_count + history_count;
        info!(
            "清除了 {} 个已完成的上传任务（owner_uid={}, 内存: {}, 历史: {}）",
            total_count,
            uid.raw(),
            memory_count,
            history_count
        );
        total_count
    }

    /// 清除指定账号下失败的上传任务
    pub async fn clear_failed_for_uid(&self, uid: crate::auth::Uid) -> usize {
        let mut to_remove = Vec::new();

        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if task.owner_uid == uid && matches!(task.status, UploadTaskStatus::Failed) {
                to_remove.push(task_id);
            }
        }

        let mut removed = 0;
        for task_id in to_remove {
            self.remove_dedup_entry(&task_id);
            self.tasks.remove(&task_id);
            removed += 1;
        }

        info!(
            "清除了 {} 个失败的上传任务（owner_uid={}）",
            removed,
            uid.raw()
        );
        removed
    }

    /// 删除指定账号下所有上传任务
    ///
    /// 用于 `force_delete_account` 链路：共享 `UploadManager` 设计下，删除账号
    /// 时必须取消并删除该 uid 归属的所有任务（含备份任务），否则任务在共享
    /// manager 内继续跑，账号已从 accounts.json 删除但上传任务还在消耗带宽 +
    /// 占槽位。
    ///
    /// 返回 `(memory_deleted, history_deleted)`。
    pub async fn delete_tasks_for_owner(
        &self,
        uid: crate::auth::Uid,
    ) -> (usize, usize) {
        // 1) 收集内存中归属该 uid 的任务 ID（含备份任务）
        let mut target_ids: Vec<String> = Vec::new();
        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if task.owner_uid == uid {
                target_ids.push(task_id);
            }
        }

        let memory_count = target_ids.len();
        info!(
            "delete_tasks_for_owner: uid={} 内存中找到 {} 个上传任务",
            uid.raw(),
            memory_count
        );

        // 2) batch_delete_tasks 复用：取消、清理去重索引、移除内存、释放槽位
        if !target_ids.is_empty() {
            let (success, failed) = self.batch_delete_tasks(&target_ids).await;
            info!(
                "delete_tasks_for_owner: uid={} batch_delete 完成: 成功={}, 失败={}",
                uid.raw(),
                success,
                failed
            );
        }

        // 3) 历史数据库：按 owner_uid 删除该账号所有 upload 历史
        let mut history_count = 0;
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm_guard = pm_arc.lock().await;
            let history_db = pm_guard.history_db().cloned();
            drop(pm_guard);

            if let Some(db) = history_db {
                match db.remove_tasks_by_type_owner("upload", Some(uid.raw())) {
                    Ok(count) => history_count = count,
                    Err(e) => warn!(
                        "delete_tasks_for_owner: 删除历史上传任务（owner_uid={}）失败: {}",
                        uid.raw(),
                        e
                    ),
                }
            }
        }

        info!(
            "delete_tasks_for_owner: uid={} 完成（内存={}, 历史={}）",
            uid.raw(),
            memory_count,
            history_count
        );
        (memory_count, history_count)
    }

    // ==================== 去重索引方法 ====================

    /// 查询去重索引
    pub fn find_duplicate_task(
        &self,
        local_path: &Path,
        original_remote_path: &str,
    ) -> Option<String> {
        let canonical = dunce::canonicalize(local_path)
            .unwrap_or_else(|_| local_path.to_path_buf());
        let key = (canonical, original_remote_path.to_string());
        self.dedup_index.get(&key).map(|v| v.value().clone())
    }

    /// 重建去重索引条目（恢复任务时调用）
    fn rebuild_dedup_index_entry(
        &self,
        task_id: &str,
        local_path: &Path,
        original_remote_path: &str,
    ) {
        let canonical = dunce::canonicalize(local_path)
            .unwrap_or_else(|e| {
                warn!("canonicalize failed for {}: {}, using raw path", local_path.display(), e);
                local_path.to_path_buf()
            });
        let key = (canonical, original_remote_path.to_string());
        self.dedup_index.insert(key.clone(), task_id.to_string());
        self.dedup_reverse.insert(task_id.to_string(), key);
    }

    /// 移除去重索引条目
    fn remove_dedup_entry(&self, task_id: &str) {
        if let Some((_, key)) = self.dedup_reverse.remove(task_id) {
            self.dedup_index.remove(&key);
        }
    }

    /// 批量创建去重任务
    pub async fn create_batch_tasks_dedup(
        &self,
        files: Vec<(PathBuf, String)>,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        self.create_batch_tasks_dedup_internal(files, encrypt, is_folder_upload, conflict_strategy, None).await
    }

    /// 批量创建去重任务（显式指定 owner_uid）
    ///
    /// 共享 `UploadManager` 设计下 `self.owner_uid`
    /// 是 startup active，与"切账号后发起的文件夹扫描"目标账号不一致。`ScanManager`
    /// 必须把 `effective_uid` 显式传入，才能让批量创建的子任务正确归属到目标账号。
    pub async fn create_batch_tasks_dedup_with_owner(
        &self,
        files: Vec<(PathBuf, String)>,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid: crate::auth::Uid,
    ) -> Result<(Vec<String>, Vec<String>)> {
        self.create_batch_tasks_dedup_internal(files, encrypt, is_folder_upload, conflict_strategy, Some(owner_uid)).await
    }

    async fn create_batch_tasks_dedup_internal(
        &self,
        files: Vec<(PathBuf, String)>,
        encrypt: bool,
        is_folder_upload: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        owner_uid_override: Option<crate::auth::Uid>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut new_ids = Vec::new();
        let mut existing_ids = Vec::new();

        for (local_path, original_remote_path) in files {
            let canonical = dunce::canonicalize(&local_path)
                .unwrap_or_else(|_| local_path.clone());

            if let Some(existing_id) = self.find_duplicate_task(&canonical, &original_remote_path) {
                existing_ids.push(existing_id);
                continue;
            }

            let task_result = match owner_uid_override {
                Some(uid) => self.create_task_with_owner(
                    canonical.clone(),
                    original_remote_path.clone(),
                    encrypt,
                    is_folder_upload,
                    conflict_strategy,
                    uid,
                ).await,
                None => self.create_task(
                    canonical.clone(),
                    original_remote_path.clone(),
                    encrypt,
                    is_folder_upload,
                    conflict_strategy,
                ).await,
            };

            match task_result {
                Ok(task_id) => {
                    let key = (canonical, original_remote_path.clone());
                    self.dedup_index.insert(key.clone(), task_id.clone());
                    self.dedup_reverse.insert(task_id.clone(), key);

                    // 更新 metadata 的 original_remote_path
                    self.update_task_original_remote_path(&task_id, &original_remote_path).await;

                    if let Err(e) = self.start_task(&task_id).await {
                        warn!("启动上传任务 {} 失败: {}", task_id, e);
                    }
                    new_ids.push(task_id);
                }
                Err(e) => {
                    warn!("创建上传任务失败 {}: {}", local_path.display(), e);
                }
            }
        }

        Ok((new_ids, existing_ids))
    }

    /// 更新任务的 original_remote_path
    async fn update_task_original_remote_path(&self, task_id: &str, original_remote_path: &str) {
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            let pm = pm_arc.lock().await;
            if let Err(e) = pm.update_original_remote_path(
                task_id,
                original_remote_path.to_string(),
            ) {
                warn!("更新 original_remote_path 失败: {}", e);
            }
        }
    }

    /// 开始所有待处理的任务
    pub async fn start_all_pending(&self) -> Result<usize> {
        let mut started = 0;
        let mut pending_ids = Vec::new();

        // 🔥 先收集 (key, task Arc) 快照再释放 DashMap 迭代 guard，避免跨 await 持 shard 锁
        let entries: Vec<_> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.task.clone()))
            .collect();
        for (task_id, task_arc) in entries {
            let task = task_arc.lock().await;
            if matches!(task.status, UploadTaskStatus::Pending) {
                pending_ids.push(task_id);
            }
        }

        for task_id in pending_ids {
            if let Err(e) = self.start_task(&task_id).await {
                warn!("启动任务失败: {}, 错误: {}", task_id, e);
            } else {
                started += 1;
            }
        }

        info!("启动了 {} 个待处理的上传任务", started);
        Ok(started)
    }

    /// 🔥 处理被抢占的备份任务
    ///
    /// 当普通任务抢占备份任务的槽位时调用：
    /// 1. 暂停被抢占的任务（直接操作，不调用 pause_task 避免循环）
    /// 2. 从调度器移除
    /// 3. 将任务加入等待队列末尾
    /// 4. 发送状态变更通知
    ///
    /// ⚠️ 注意：槽位已经被抢占方占用，这里不需要释放槽位
    async fn handle_preempted_backup_task(&self, task_id: &str) {
        // 1. 暂停任务（直接操作，不调用 pause_task 避免循环调用 try_start_waiting_tasks）
        // 🔥 先在 guard 内置位 is_paused/cancel 并克隆 task Arc，释放 DashMap guard 后再
        // await 任务锁，避免持分片 guard 跨 task.lock().await 形成锁序倒置/死锁。
        let task_arc = self.tasks.get(task_id).map(|task_info| {
            task_info.is_paused.store(true, Ordering::SeqCst);
            task_info.cancel_token.cancel(); // 取消正在进行的上传
            task_info.task.clone()
        });
        if let Some(task_arc) = task_arc {
            let mut task = task_arc.lock().await;
            let old_status = format!("{:?}", task.status).to_lowercase();
            // 取真实 owner_uid
            let task_owner_uid_raw = task.owner_uid.raw();
            if task.status == UploadTaskStatus::Uploading {
                task.mark_paused();
                // 清除槽位ID（槽位已被抢占方占用）
                task.slot_id = None;
                info!("被抢占的备份上传任务 {} 已暂停", task_id);
            }
            drop(task);

            // 发送状态变更事件
            self.publish_event(UploadEvent::StatusChanged {
                task_id: task_id.to_string(),
                old_status,
                new_status: "paused".to_string(),
                is_backup: true,

                owner_uid: Some(task_owner_uid_raw),
            })
                .await;
        }

        // 2. 从调度器移除
        self.scheduler.cancel_task(task_id).await;

        // 3. 加入等待队列末尾
        self.add_preempted_backup_to_queue(task_id).await;
    }

    /// 🔥 将被抢占的备份任务加入等待队列末尾
    ///
    /// 参考下载管理器的 add_preempted_backup_to_queue 实现
    async fn add_preempted_backup_to_queue(&self, task_id: &str) {
        // 更新任务状态从 Paused 到 Pending
        // 🔥 克隆 task Arc 后释放 DashMap guard，再 await 任务锁，避免锁序倒置/死锁。
        let task_arc = self.tasks.get(task_id).map(|task_info| task_info.task.clone());
        if let Some(task_arc) = task_arc {
            let mut task = task_arc.lock().await;
            task.status = UploadTaskStatus::Pending;
            let is_backup = task.is_backup;
            // 取真实 owner_uid
            let task_owner_uid_raw = task.owner_uid.raw();
            drop(task);

            // 发送状态变更事件
            self.publish_event(UploadEvent::StatusChanged {
                task_id: task_id.to_string(),
                old_status: "paused".to_string(),
                new_status: "pending".to_string(),
                is_backup,

                owner_uid: Some(task_owner_uid_raw),
            })
                .await;

            // 🔥 如果是备份任务，发送通知到 AutoBackupManager
            if is_backup {
                use crate::autobackup::events::{TransferTaskStatus, TransferTaskType};
                let tx_guard = self.backup_notification_tx.read().await;
                if let Some(tx) = tx_guard.as_ref() {
                    let notification = BackupTransferNotification::StatusChanged {
                        task_id: task_id.to_string(),
                        task_type: TransferTaskType::Upload,
                        old_status: TransferTaskStatus::Paused,
                        new_status: TransferTaskStatus::Pending,
                    };
                    let _ = tx.send(notification);
                }
            }
        }

        // 加入等待队列末尾
        let mut queue = self.waiting_queue.write().await;
        queue.push_back(task_id.to_string());
        info!(
            "被抢占的备份上传任务 {} 加入等待队列末尾 (队列长度: {})",
            task_id,
            queue.len()
        );
    }

    /// 🔥 按优先级将任务加入等待队列
    ///
    /// 等待队列按优先级排序：
    /// - 普通上传任务：最高优先级，插入到队列前面（在所有备份任务之前）
    /// - 自动备份任务：最低优先级，插入到队列末尾
    ///
    /// # 参数
    /// - `task_id`: 任务ID
    /// - `is_backup`: 是否为备份任务
    async fn add_to_waiting_queue_by_priority(&self, task_id: &str, is_backup: bool) {
        let mut queue = self.waiting_queue.write().await;

        if is_backup {
            // 备份任务：直接加入队列末尾
            queue.push_back(task_id.to_string());
            info!(
                "备份上传任务 {} 加入等待队列末尾 (队列长度: {})",
                task_id,
                queue.len()
            );
        } else {
            // 普通任务：插入到所有备份任务之前
            let backup_start_pos = {
                let mut pos = None;
                for (i, id) in queue.iter().enumerate() {
                    if let Some(task_info) = self.tasks.get(id) {
                        if let Ok(t) = task_info.task.try_lock() {
                            if t.is_backup {
                                pos = Some(i);
                                break;
                            }
                        }
                    }
                }
                pos
            };

            if let Some(pos) = backup_start_pos {
                queue.insert(pos, task_id.to_string());
                info!(
                    "普通上传任务 {} 插入到等待队列位置 {} (在备份任务之前, 队列长度: {})",
                    task_id,
                    pos,
                    queue.len()
                );
            } else {
                queue.push_back(task_id.to_string());
                info!(
                    "普通上传任务 {} 加入等待队列末尾 (无备份任务, 队列长度: {})",
                    task_id,
                    queue.len()
                );
            }
        }
    }

    /// 🔥 从等待队列移除并暂停指定的任务列表
    ///
    /// 用于备份任务暂停时，将等待队列中属于该备份任务的子任务也暂停
    ///
    /// # 参数
    /// - `task_ids`: 要暂停的任务ID列表
    ///
    /// # 返回
    /// - 成功暂停的任务数量
    pub async fn pause_waiting_tasks(&self, task_ids: &[String]) -> usize {
        if task_ids.is_empty() {
            return 0;
        }

        let task_id_set: std::collections::HashSet<&String> = task_ids.iter().collect();
        let mut paused_count = 0;

        // 1. 从等待队列移除
        {
            let mut queue = self.waiting_queue.write().await;
            let original_len = queue.len();
            queue.retain(|id| !task_id_set.contains(id));
            let removed = original_len - queue.len();
            if removed > 0 {
                tracing::info!(
                    "从上传等待队列移除了 {} 个任务 (队列剩余: {})",
                    removed,
                    queue.len()
                );
            }
        }

        // 2. 将这些任务标记为暂停状态，收集事件数据后再发送（避免持有 DashMap ref 跨 await）
        // tuple 第 4 项为锁内取出的真实 owner_uid.raw()
        let mut events_to_publish: Vec<(String, String, bool, u64)> = Vec::new();

        for task_id in task_ids {
            // 🔥 克隆 task / is_paused Arc 后释放 DashMap guard，再 await 任务锁，
            // 避免持分片 guard 跨 task.lock().await 形成锁序倒置/死锁。
            let arcs = self
                .tasks
                .get(task_id)
                .map(|task_info| (task_info.task.clone(), task_info.is_paused.clone()));
            if let Some((task_arc, is_paused)) = arcs {
                let mut task = task_arc.lock().await;
                if task.status == UploadTaskStatus::Pending {
                    let old_status = format!("{:?}", task.status).to_lowercase();
                    let is_backup = task.is_backup;
                    let task_owner_uid_raw = task.owner_uid.raw();
                    task.mark_paused();
                    paused_count += 1;

                    tracing::debug!(
                        "等待队列中的上传任务 {} 已暂停 (原状态: {})",
                        task_id,
                        old_status
                    );

                    is_paused.store(true, std::sync::atomic::Ordering::SeqCst);

                    drop(task);
                    // 收集事件数据，稍后发送
                    events_to_publish.push((task_id.clone(), old_status, is_backup, task_owner_uid_raw));
                }
            }
        }

        // 🔥 DashMap ref 已释放，安全地发送事件
        for (task_id, old_status, is_backup, task_owner_uid_raw) in events_to_publish {
            self.publish_event(UploadEvent::StatusChanged {
                task_id: task_id.clone(),
                old_status,
                new_status: "paused".to_string(),
                is_backup,

                owner_uid: Some(task_owner_uid_raw),
            })
                .await;

            self.publish_event(UploadEvent::Paused {
                task_id,
                is_backup,

                owner_uid: Some(task_owner_uid_raw),
            })
                .await;
        }

        if paused_count > 0 {
            tracing::info!("已暂停 {} 个等待队列中的上传任务", paused_count);
        }

        paused_count
    }

    /// 🔥 检查等待队列中是否有普通任务（非备份任务）
    async fn has_normal_tasks_waiting(&self) -> bool {
        let queue = self.waiting_queue.read().await;

        for id in queue.iter() {
            if let Some(task_info) = self.tasks.get(id) {
                if let Ok(t) = task_info.task.try_lock() {
                    if !t.is_backup {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// 尝试从等待队列启动任务
    ///
    /// 🔥 区分备份任务和普通任务，实现优先级调度：
    /// - 普通任务优先启动
    /// - 备份任务只有在没有普通任务等待时才启动
    async fn try_start_waiting_tasks(&self) {
        // scheduler 始终启用。
        // 该函数仅在 scheduler 路径有意义；保留 try_start 入口与现有 caller 兼容。
        //
        // 忙等保护：当队首任务的 task 锁被别处持有（try_lock 失败）时，过去的实现
        // 是 push_front + continue —— available_slots 不变、又无 await 让出调度，
        // 下一轮立刻又弹出同一个被锁任务，形成不让出的紧凑忙循环，CPU 打满且本函数
        // 永不返回（而它被完成/暂停/删除处理器内联 .await，会一起挂死，上传泵停摆，
        // 备份任务永远卡在「等待传输」）。现在改为：被锁任务放回队尾并累计 deferred，
        // 当连续推迟次数达到当前队列长度（整轮都拿不到锁）即退出，交给 1s 后台 monitor
        // / 下次事件重试。
        let mut consecutive_deferred = 0usize;
        loop {
            // 🔥 使用槽位池检查可用槽位（替代预注册检查）
            let available_slots = self.task_slot_pool.available_slots().await;
            if available_slots == 0 {
                break;
            }

            // 从等待队列取出任务（同时记录取出时的队列长度，用于忙等保护判定）
            let (task_id, queue_len) = {
                let mut queue = self.waiting_queue.write().await;
                let qlen = queue.len();
                (queue.pop_front(), qlen)
            };

            match task_id {
                Some(id) => {
                    // 忙等保护：整轮剩余任务都拿不到锁时退出，避免死循环
                    if consecutive_deferred >= queue_len {
                        self.waiting_queue.write().await.push_front(id);
                        break;
                    }

                    // 🔥 获取任务信息：is_backup、状态、是否已有槽位
                    //
                    // 先 clone 出任务 Arc 再放开 DashMap guard，避免持分片读锁跨
                    // try_lock；随后只在 Arc 上 try_lock，不再持有 DashMap 引用。
                    let task_arc = match self.tasks.get(&id).map(|t| t.task.clone()) {
                        Some(arc) => arc,
                        None => {
                            warn!("等待队列中的上传任务 {} 不存在，跳过", id);
                            consecutive_deferred = 0;
                            continue;
                        }
                    };
                    let (is_backup, status, existing_slot_id) = match task_arc.try_lock() {
                        Ok(t) => (t.is_backup, t.status.clone(), t.slot_id),
                        Err(_) => {
                            // 任务被锁定：放回队尾 + 计推迟数，避免在队首忙等死循环
                            self.waiting_queue.write().await.push_back(id);
                            consecutive_deferred += 1;
                            continue;
                        }
                    };
                    // 成功取到锁视为有进展，重置推迟计数
                    consecutive_deferred = 0;

                    // 🔥 防御性检查：任务已有槽位或已在上传中，跳过（避免重复分配）
                    if existing_slot_id.is_some() {
                        warn!(
                            "等待队列中的上传任务 {} 已有槽位 {:?}，跳过（可能已被手动启动）",
                            id, existing_slot_id
                        );
                        continue;
                    }

                    if matches!(
                        status,
                        UploadTaskStatus::Uploading | UploadTaskStatus::CheckingRapid
                    ) {
                        warn!(
                            "等待队列中的上传任务 {} 状态为 {:?}，跳过（已在上传中）",
                            id, status
                        );
                        continue;
                    }

                    // 🔥 备份任务特殊处理：检查是否有普通任务在等待
                    if is_backup {
                        let has_normal_waiting = self.has_normal_tasks_waiting().await;
                        if has_normal_waiting {
                            // 有普通任务等待，备份任务放回队列末尾
                            self.waiting_queue.write().await.push_back(id);
                            info!("备份上传任务让位：有普通任务等待，备份任务放回队列末尾");
                            continue;
                        }
                    }

                    // 🔥 先分配槽位
                    let slot_result = if is_backup {
                        self.task_slot_pool
                            .allocate_backup_slot(&id)
                            .await
                            .map(|sid| (sid, None))
                    } else {
                        self.task_slot_pool
                            .allocate_fixed_slot_with_priority(&id, false, TaskPriority::Normal)
                            .await
                    };

                    match slot_result {
                        Some((slot_id, preempted_task_id)) => {
                            // 🔥 获取任务信息并记录槽位ID
                            // 先 clone 出任务 Arc 再放开 DashMap guard，避免持分片锁跨 task.lock().await
                            let task_arc = self.tasks.get(&id).map(|t| t.task.clone());
                            let task_params = if let Some(task_arc) = task_arc {
                                let mut t = task_arc.lock().await;
                                t.slot_id = Some(slot_id);
                                t.is_borrowed_slot = false;
                                Some((t.local_path.clone(), t.remote_path.clone(), t.total_size))
                            } else {
                                warn!("等待队列中的上传任务 {} 不存在，跳过", id);
                                // 释放刚分配的槽位
                                self.task_slot_pool.release_fixed_slot(&id).await;
                                continue;
                            };

                            // 处理被抢占的任务
                            if let Some(preempted_id) = preempted_task_id {
                                self.handle_preempted_backup_task(&preempted_id).await;
                            }

                            // 🔥 刷新上传服务器列表（保持和 start_task 一致的行为）
                            let client_snapshot = self.client.read().unwrap().clone();
                            match client_snapshot.locate_upload().await {
                                Ok(servers) => {
                                    if !servers.is_empty() {
                                        self.server_health.update_servers(servers);
                                    }
                                }
                                Err(e) => {
                                    warn!("获取上传服务器列表失败，使用默认服务器: {}", e);
                                }
                            }

                            info!(
                                "从等待队列启动上传任务: {} (is_backup: {}, slot: {})",
                                id, is_backup, slot_id
                            );

                            // 🔥 调用 start_task_internal（不再传递 DashMap ref）
                            if let Some((local_path, remote_path, total_size)) = task_params {
                                if let Err(e) = self
                                    .start_task_internal(
                                        &id,
                                        local_path,
                                        remote_path,
                                        total_size,
                                    )
                                    .await
                                {
                                    error!("启动等待上传任务失败: {}, 错误: {}", id, e);
                                    // 启动失败，释放槽位
                                    self.task_slot_pool.release_fixed_slot(&id).await;
                                }
                            }
                        }
                        None => {
                            // 槽位分配失败，放回队列
                            self.waiting_queue.write().await.push_front(id.clone());
                            info!("等待队列任务 {} 槽位分配失败，放回队列", id);
                            break;
                        }
                    }
                }
                None => break, // 队列为空
            }
        }
    }

    /// 🔥 统一的上传任务失败处理
    ///
    /// 标记任务失败 + 发送失败通知 + 更新持久化错误信息。
    ///
    /// 备份任务必须通过 `BackupTransferNotification::Failed` 通知 AutoBackupManager，
    /// 否则备份文件任务会一直停留在"传输中"状态（前端订阅 backup:* 而非 upload:*），
    /// 导致 UI 卡死在"传输中 0%"。普通任务走 upload:* 通道。
    async fn fail_upload_task(
        &self,
        task_id: &str,
        task_arc: &Arc<Mutex<UploadTask>>,
        error_msg: String,
    ) {
        let ws_manager = self.ws_manager.read().await.clone();
        let persistence_manager = self.persistence_manager.lock().await.clone();
        let backup_notification_tx = self.backup_notification_tx.read().await.clone();
        Self::handle_upload_failure(
            task_id,
            task_arc,
            error_msg,
            &self.task_slot_pool,
            ws_manager.as_ref(),
            persistence_manager.as_ref(),
            backup_notification_tx.as_ref(),
            &self.active_count,
        )
            .await;
    }

    /// 🔥 统一的上传任务失败收尾（关联函数，不依赖 `&self`，供后台 spawn 复用）
    ///
    /// 后台 spawn（`start_task_internal` / `start_waiting_queue_monitor`）只持有 Arc clone、
    /// 拿不到 `self`，过去各失败分支各自 `release_fixed_slot + mark_failed`，散落且不一致：
    /// 有的不清 `slot_id`、有的只发 `upload:*`、都不回滚 `active_count`。此函数统一收口：
    ///
    /// 1. 释放槽位池（`release_fixed_slot` 按 task_id 匹配，幂等）；
    /// 2. 同一把任务锁内：标记失败 + 清 `slot_id`/`is_borrowed_slot` + 判定是否活跃态；
    /// 3. 活跃态转入 Failed 时回滚 `active_count`（与 cancel/pause 一致，避免等 60s 漂移校准）；
    /// 4. 按正确通道通知（备份任务走 `backup:*`，普通任务走 `upload:*`）；
    /// 5. 写持久化错误信息。
    ///
    /// `release_fixed_slot` 只改槽位池状态、不同步任务字段，因此必须在此一并清 `slot_id`，
    /// 否则任务变 Failed 后仍残留旧 slot_id，用户修复文件 resume 时会因
    /// `existing_slot_id.is_some()` 跳过重新分配、在无真实槽位占用下启动上传。
    #[allow(clippy::too_many_arguments)]
    async fn handle_upload_failure(
        task_id: &str,
        task_arc: &Arc<Mutex<UploadTask>>,
        error_msg: String,
        task_slot_pool: &Arc<TaskSlotPool>,
        ws_manager: Option<&Arc<crate::server::websocket::WebSocketManager>>,
        persistence_manager: Option<&Arc<Mutex<PersistenceManager>>>,
        backup_notification_tx: Option<
            &tokio::sync::mpsc::UnboundedSender<BackupTransferNotification>,
        >,
        active_count: &Arc<AtomicUsize>,
    ) {
        // 1) 释放槽位池
        task_slot_pool.release_fixed_slot(task_id).await;

        // 2) 同一把锁内：判定活跃态 + 标记失败 + 清槽位字段（原子完成，避免与 cancel 竞态重复回滚）
        let (is_backup, owner_uid_raw, was_active) = {
            let mut t = task_arc.lock().await;
            let was_active = matches!(
                t.status,
                UploadTaskStatus::Pending
                    | UploadTaskStatus::Uploading
                    | UploadTaskStatus::Encrypting
                    | UploadTaskStatus::CheckingRapid
            );
            t.mark_failed(error_msg.clone());
            t.slot_id = None;
            t.is_borrowed_slot = false;
            (t.is_backup, t.owner_uid.raw(), was_active)
        };

        // 3) 回滚活跃计数（漂移校准用 store 绝对值，不会与此处的相对扣减重复）
        if was_active {
            let prev = active_count.fetch_sub(1, Ordering::SeqCst);
            if prev == 0 {
                // 防御下溢，钳回 0
                active_count.store(0, Ordering::SeqCst);
            }
        }

        // 4) 按正确通道通知
        if is_backup {
            // 备份任务：通知 AutoBackupManager，使其将文件任务状态从 Transferring 置为 Failed
            if let Some(tx) = backup_notification_tx {
                let notification = BackupTransferNotification::Failed {
                    task_id: task_id.to_string(),
                    task_type: crate::autobackup::events::TransferTaskType::Upload,
                    error_message: error_msg.clone(),
                };
                if let Err(e) = tx.send(notification) {
                    warn!("发送备份上传任务失败通知失败: {}", e);
                }
            }
        } else if let Some(ws) = ws_manager {
            ws.send_if_subscribed(
                TaskEvent::Upload(UploadEvent::Failed {
                    task_id: task_id.to_string(),
                    error: error_msg.clone(),
                    is_backup,
                    owner_uid: Some(owner_uid_raw),
                }),
                None,
            );
        }

        // 5) 持久化错误信息
        if let Some(pm) = persistence_manager {
            if let Err(e) = pm.lock().await.update_task_error(task_id, error_msg) {
                warn!("更新上传任务错误信息失败: {}", e);
            }
        }
    }

    /// 🔥 校验本地源文件存在且为常规文件
    ///
    /// 备份续传场景下，持久化任务可能指向已被删除/移动的本地文件。校验失败则
    /// 立即失败并经由正确通道通知（备份任务走 backup:*，普通任务走 upload:*），
    /// 避免任务注册到调度器后读不到分片数据、白等 5 分钟槽位超时。
    ///
    /// 返回 `true` 表示文件可用；`false` 表示已处理失败，调用方应中止启动。
    async fn ensure_local_source_valid(
        &self,
        task_id: &str,
        local_path: &Path,
        task_arc: &Arc<Mutex<UploadTask>>,
    ) -> bool {
        let error_msg = match tokio::fs::metadata(local_path).await {
            Ok(meta) if meta.is_file() => return true,
            Ok(_) => format!("本地源文件不是常规文件，无法上传: {:?}", local_path),
            Err(e) => format!("本地源文件不存在，无法上传: {:?} ({})", local_path, e),
        };
        warn!("上传任务 {} {}", task_id, error_msg);
        self.fail_upload_task(task_id, task_arc, error_msg).await;
        false
    }

    /// 🔥 设置槽位超时释放处理器
    ///
    /// 当槽位因超时被自动释放时，将对应任务状态设置为失败
    fn setup_stale_release_handler(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // 设置通知通道到槽位池
        let task_slot_pool = self.task_slot_pool.clone();
        tokio::spawn(async move {
            task_slot_pool.set_stale_release_handler(tx).await;
        });

        // 启动监听循环
        let tasks = self.tasks.clone();
        let ws_manager = self.ws_manager.clone();
        // 🔥 备份任务失败必须经由 backup:* 通道通知 AutoBackupManager，否则前端
        // （订阅 backup:*）会一直卡在"传输中 0%"。
        let backup_notification_tx = self.backup_notification_tx.clone();
        // 不再 capture self.owner_uid。
        // 共享 manager 设计下 self.owner_uid 不可靠（同一个 Arc 服务多个账号）。
        // 事件归属一律读 `task.owner_uid`（在锁内一并取出）。
        tokio::spawn(async move {
            const STALE_ERROR_MSG: &str = "槽位超时释放：任务长时间无进度更新，可能已卡住";
            while let Some(task_id) = rx.recv().await {
                info!("收到槽位超时释放通知，将上传任务设置为失败: {}", task_id);

                // 更新任务状态为失败
                // 🔥 先 clone 出 task 的 Arc 再释放 DashMap ref，避免跨 await 持 shard 锁
                let task_arc = tasks.get(&task_id).map(|task_info| task_info.task.clone());
                if let Some(task_arc) = task_arc {
                    let mut t = task_arc.lock().await;
                    t.status = crate::uploader::UploadTaskStatus::Failed;
                    t.error = Some(STALE_ERROR_MSG.to_string());
                    // 🔥 清除已释放的槽位ID，避免重试时误以为还持有槽位
                    t.slot_id = None;
                    // 🔥 取真实 owner_uid（共享 manager 下 self.owner_uid 不可靠）
                    let task_owner_uid_raw = t.owner_uid.raw();
                    let task_is_backup = t.is_backup;
                    drop(t);

                    if task_is_backup {
                        // 🔥 备份任务：通知 AutoBackupManager 将文件任务状态从
                        // Transferring 置为 Failed，避免 UI 永久卡在"传输中"
                        if let Some(tx) = backup_notification_tx.read().await.clone() {
                            let notification = BackupTransferNotification::Failed {
                                task_id: task_id.clone(),
                                task_type: crate::autobackup::events::TransferTaskType::Upload,
                                error_message: STALE_ERROR_MSG.to_string(),
                            };
                            if let Err(e) = tx.send(notification) {
                                warn!("发送备份上传任务超时失败通知失败: {}", e);
                            }
                        }
                    } else {
                        // 普通任务：发送 WebSocket 通知
                        let ws_guard = ws_manager.read().await;
                        if let Some(ref ws) = *ws_guard {
                            ws.send_if_subscribed(
                                TaskEvent::Upload(UploadEvent::Failed {
                                    task_id: task_id.clone(),
                                    error: STALE_ERROR_MSG.to_string(),
                                    is_backup: task_is_backup,

                                    owner_uid: Some(task_owner_uid_raw),
                                }),
                                None,
                            );
                        }
                    }
                }
            }
        });

        info!("上传管理器已设置槽位超时释放处理器");
    }

    /// 启动后台监控任务：定期检查并启动等待队列中的任务
    ///
    /// 这确保了当活跃任务自然完成时，等待队列中的任务能被自动启动
    fn start_waiting_queue_monitor(&self) {
        let waiting_queue = self.waiting_queue.clone();
        // scheduler 始终就绪
        let scheduler = self.scheduler.clone();
        let tasks = self.tasks.clone();
        let client = self.client.clone();
        let server_health = self.server_health.clone();
        let vip_type = self.vip_type;
        let _max_concurrent_tasks = self.max_concurrent_tasks.clone();
        let persistence_manager = self.persistence_manager.clone();
        let ws_manager = self.ws_manager.clone();
        // 🔥 克隆备份通知发送器
        let backup_notification_tx = self.backup_notification_tx.clone();
        let task_slot_pool = self.task_slot_pool.clone();
        // 🔥 克隆活跃计数器，用于后台失败时回滚 active_count
        let active_count = self.active_count.clone();
        // 🔥 克隆加密快照管理器
        let snapshot_manager = self.snapshot_manager.clone();
        // 🔥 克隆加密配置存储（用于后台监控启动加密任务）
        let encryption_config_store = self.encryption_config_store.clone();
        // 不再 capture self.owner_uid。
        // 共享 manager 设计下 self.owner_uid 不可靠（同一个 Arc 服务多个账号）。
        // 事件归属一律读 `task.owner_uid`（已在 task_basic_info 元组里取出 task_owner_uid_raw）。

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

            loop {
                interval.tick().await;

                // 检查是否有等待任务
                let has_waiting = {
                    let queue = waiting_queue.read().await;
                    !queue.is_empty()
                };

                if !has_waiting {
                    continue;
                }

                // 🔥 使用槽位池检查可用槽位
                let available_slots = task_slot_pool.available_slots().await;
                if available_slots == 0 {
                    continue;
                }

                // 尝试启动等待任务
                loop {
                    // 🔥 检查是否有可用槽位
                    let available = task_slot_pool.available_slots().await;
                    if available == 0 {
                        break;
                    }

                    let task_id = {
                        let mut queue = waiting_queue.write().await;
                        queue.pop_front()
                    };

                    match task_id {
                        Some(id) => {
                            // 先取任务基本信息后立即释放 DashMap 引用，避免后续 lock 嵌套
                            // 同时拿到真实 owner_uid（共享 manager 下 self.owner_uid 不可靠）
                            // 🔥 先 clone 出任务 Arc 再放开 DashMap guard，避免持分片读锁跨 task.lock().await
                            //    （锁序倒置死锁：持 shard guard 等 task 锁，而别处持 task 锁等同 shard）
                            let task_arc = tasks.get(&id).map(|t| t.task.clone());
                            let task_basic_info = {
                                if let Some(task_arc) = task_arc {
                                    let task = task_arc.lock().await;
                                    Some((
                                        task.local_path.clone(),
                                        task.remote_path.clone(),
                                        task.total_size,
                                        task.is_backup,
                                        task.encrypt_enabled,
                                        task.owner_uid.raw(),
                                    ))
                                } else {
                                    None
                                }
                            }; // 🔥 DashMap 读锁在这里释放

                            let (local_path, remote_path, total_size, is_backup, encrypt_enabled, task_owner_uid_raw) =
                                match task_basic_info {
                                    Some(info) => info,
                                    None => {
                                        warn!("后台监控：任务 {} 不存在，跳过", id);
                                        continue;
                                    }
                                };

                            // 🔥 分配槽位（此时没有持有 DashMap 锁）
                            let slot_result = if is_backup {
                                task_slot_pool
                                    .allocate_backup_slot(&id)
                                    .await
                                    .map(|sid| (sid, None))
                            } else {
                                task_slot_pool
                                    .allocate_fixed_slot_with_priority(
                                        &id,
                                        false,
                                        TaskPriority::Normal,
                                    )
                                    .await
                            };

                            let (slot_id, preempted_task_id) = match slot_result {
                                Some(result) => result,
                                None => {
                                    // 槽位分配失败，放回队列
                                    waiting_queue.write().await.push_front(id.clone());
                                    info!("后台监控：任务 {} 槽位分配失败，放回队列", id);
                                    break;
                                }
                            };

                            // 🔥 记录槽位ID到任务
                            // 先 clone 出任务 Arc 再放开 DashMap guard，避免持 get_mut 写 guard 跨
                            // task.lock().await（写 guard 会阻塞该分片所有访问，更易死锁）
                            {
                                let task_arc = tasks.get(&id).map(|t| t.task.clone());
                                if let Some(task_arc) = task_arc {
                                    let mut t = task_arc.lock().await;
                                    t.slot_id = Some(slot_id);
                                    t.is_borrowed_slot = false;
                                }
                            } // 🔥 task 锁在这里释放

                            // 🔥 处理被抢占的备份任务（在外部处理，因为无法访问 self）
                            if let Some(preempted_id) = preempted_task_id {
                                info!(
                                    "后台监控：普通任务 {} 抢占了备份任务 {} 的槽位",
                                    id, preempted_id
                                );

                                // 1. 暂停被抢占的任务
                                // 🔥 在 DashMap guard 内只做同步操作（store/cancel）并 clone 出任务 Arc，
                                //    放开 guard 后再 task.lock().await，避免持分片锁跨 await 死锁
                                let preempted_arc = tasks.get(&preempted_id).map(|e| {
                                    e.is_paused.store(true, Ordering::SeqCst);
                                    e.cancel_token.cancel();
                                    e.task.clone()
                                });
                                if let Some(preempted_arc) = preempted_arc {
                                    let mut preempted_task = preempted_arc.lock().await;
                                    if preempted_task.status == UploadTaskStatus::Uploading {
                                        preempted_task.mark_paused();
                                        preempted_task.slot_id = None;
                                        info!(
                                            "后台监控：被抢占的备份上传任务 {} 已暂停",
                                            preempted_id
                                        );
                                    }
                                    // 更新状态为 Pending（等待重新调度）
                                    preempted_task.status = UploadTaskStatus::Pending;
                                    drop(preempted_task);
                                }

                                // 2. 从调度器移除
                                scheduler.cancel_task(&preempted_id).await;

                                // 3. 发送备份任务状态通知
                                {
                                    let tx_guard = backup_notification_tx.read().await;
                                    if let Some(tx) = tx_guard.as_ref() {
                                        use crate::autobackup::events::{
                                            TransferTaskStatus, TransferTaskType,
                                        };
                                        let notification =
                                            BackupTransferNotification::StatusChanged {
                                                task_id: preempted_id.clone(),
                                                task_type: TransferTaskType::Upload,
                                                old_status: TransferTaskStatus::Transferring,
                                                new_status: TransferTaskStatus::Pending,
                                            };
                                        let _ = tx.send(notification);
                                    }
                                }

                                // 4. 加入等待队列末尾
                                waiting_queue.write().await.push_back(preempted_id.clone());
                                info!(
                                    "后台监控：被抢占的备份任务 {} 已加入等待队列末尾",
                                    preempted_id
                                );
                            }

                            info!(
                                "🔄 后台监控：从等待队列启动上传任务 {} (is_backup={}, slot={})",
                                id, is_backup, slot_id
                            );

                            // 重新取 task_info 用于克隆数据（前一个引用已释放）
                            let task_data = {
                                if let Some(task_info) = tasks.get(&id) {
                                    Some((
                                        task_info.task.clone(),
                                        task_info.cancel_token.clone(),
                                        task_info.is_paused.clone(),
                                        task_info.active_chunk_count.clone(),
                                        task_info.max_concurrent_chunks,
                                        task_info.uploaded_bytes.clone(),
                                        task_info.last_speed_time.clone(),
                                        task_info.last_speed_bytes.clone(),
                                        task_info.restored_completed_chunks.clone(),
                                    ))
                                } else {
                                    None
                                }
                            };

                            let (
                                task,
                                cancel_token,
                                is_paused,
                                active_chunk_count,
                                max_concurrent_chunks,
                                uploaded_bytes,
                                last_speed_time,
                                last_speed_bytes,
                                restored_completed_chunks,
                            ) = match task_data {
                                Some(data) => data,
                                None => {
                                    warn!("后台监控：任务 {} 在启动前被删除，释放槽位", id);
                                    task_slot_pool.release_fixed_slot(&id).await;
                                    continue;
                                }
                            };

                            let server_health_clone = server_health.clone();
                            let client_clone = client.clone();
                            let scheduler_clone = scheduler.clone();
                            let task_id_clone = id.clone();
                            let pm_clone = persistence_manager.lock().await.clone();
                            let ws_manager_clone = ws_manager.read().await.clone();
                            // 🔥 克隆 tasks 引用，用于保存创建的分片管理器
                            let tasks_clone = tasks.clone();
                            // 🔥 克隆备份通知发送器
                            let backup_notification_tx_clone =
                                backup_notification_tx.read().await.clone();
                            let task_slot_pool_clone = task_slot_pool.clone();
                            // 🔥 克隆活跃计数器，用于后台失败时回滚 active_count
                            let active_count_clone = active_count.clone();
                            // 🔥 克隆加密快照管理器
                            let snapshot_manager_clone = snapshot_manager.read().await.clone();
                            // 🔥 克隆加密配置存储（用于执行加密流程）
                            let encryption_config_store_clone = encryption_config_store.clone();

                            // 在后台执行 precreate 并注册到调度器
                            tokio::spawn(async move {
                                info!("后台监控：开始准备上传任务: {}", task_id_clone);

                                // 标记为上传中
                                {
                                    let mut t = task.lock().await;
                                    t.mark_uploading();
                                }

                                // 🔥 发送状态变更通知 (Pending -> Uploading)
                                if is_backup {
                                    // 备份任务：发送 BackupTransferNotification
                                    if let Some(ref tx) = backup_notification_tx_clone {
                                        use crate::autobackup::events::TransferTaskType;
                                        let notification = BackupTransferNotification::StatusChanged {
                                            task_id: task_id_clone.clone(),
                                            task_type: TransferTaskType::Upload,
                                            old_status: crate::autobackup::events::TransferTaskStatus::Pending,
                                            new_status: crate::autobackup::events::TransferTaskStatus::Transferring,
                                        };
                                        if let Err(e) = tx.send(notification) {
                                            warn!(
                                                "后台监控：发送备份上传任务传输状态通知失败: {}",
                                                e
                                            );
                                        } else {
                                            info!("后台监控：已发送备份上传任务传输状态通知: {} (Pending -> Transferring)", task_id_clone);
                                        }
                                    }
                                } else {
                                    // 普通任务：发送 UploadEvent::StatusChanged
                                    if let Some(ref ws) = ws_manager_clone {
                                        ws.send_if_subscribed(
                                            TaskEvent::Upload(UploadEvent::StatusChanged {
                                                task_id: task_id_clone.clone(),
                                                old_status: "pending".to_string(),
                                                new_status: "uploading".to_string(),
                                                is_backup: false,

                                                owner_uid: Some(task_owner_uid_raw),
                                            }),
                                            None,
                                        );
                                        info!("后台监控：已发送普通上传任务状态变更通知: {} (pending -> uploading)", task_id_clone);
                                    }
                                }

                                // 🔥 如果启用加密，先执行加密流程
                                let (actual_local_path, actual_total_size) = if encrypt_enabled {
                                    match Self::execute_encryption(
                                        &task,
                                        &task_id_clone,
                                        &local_path,
                                        total_size,
                                        is_backup,
                                        ws_manager_clone.as_ref(),
                                        &task_slot_pool_clone,
                                        pm_clone.as_ref(),
                                        &encryption_config_store_clone,
                                        backup_notification_tx_clone.as_ref(),
                                    )
                                        .await
                                    {
                                        Ok(encrypted_path) => {
                                            // 获取加密后文件大小
                                            let encrypted_size = match tokio::fs::metadata(&encrypted_path).await {
                                                Ok(m) => m.len(),
                                                Err(e) => {
                                                    let error_msg = format!("后台监控：获取加密文件大小失败: {}", e);
                                                    error!("{}", error_msg);
                                                    Self::handle_upload_failure(
                                                        &task_id_clone,
                                                        &task,
                                                        error_msg,
                                                        &task_slot_pool_clone,
                                                        ws_manager_clone.as_ref(),
                                                        pm_clone.as_ref(),
                                                        backup_notification_tx_clone.as_ref(),
                                                        &active_count_clone,
                                                    )
                                                        .await;
                                                    return;
                                                }
                                            };
                                            info!("后台监控：加密完成，使用加密文件: {:?}, size={}", encrypted_path, encrypted_size);
                                            (encrypted_path, encrypted_size)
                                        }
                                        Err(e) => {
                                            // execute_encryption 内部已处理失败通知和槽位释放
                                            error!("后台监控：加密失败: {}", e);
                                            return;
                                        }
                                    }
                                } else {
                                    (local_path.clone(), total_size)
                                };

                                // 1. 计算 block_list（使用实际文件路径，可能是加密后的文件）
                                let block_list =
                                    match crate::uploader::RapidUploadChecker::calculate_block_list(
                                        &actual_local_path,
                                        vip_type,
                                    )
                                        .await
                                    {
                                        Ok(bl) => bl,
                                        Err(e) => {
                                            let error_msg = format!("计算 block_list 失败: {}", e);
                                            error!("后台监控：{}", error_msg);
                                            Self::handle_upload_failure(
                                                &task_id_clone,
                                                &task,
                                                error_msg,
                                                &task_slot_pool_clone,
                                                ws_manager_clone.as_ref(),
                                                pm_clone.as_ref(),
                                                backup_notification_tx_clone.as_ref(),
                                                &active_count_clone,
                                            )
                                                .await;
                                            return;
                                        }
                                    };

                                // 2. 预创建文件（使用实际文件大小，可能是加密后的大小）
                                // 🔥 从共享引用读取最新客户端（代理热更新后自动生效）
                                let client_snapshot = client_clone.read().unwrap().clone();
                                let rtype = {
                                    let t = task.lock().await;
                                    crate::uploader::conflict::conflict_strategy_to_rtype(t.conflict_strategy)
                                };
                                let precreate_response = match client_snapshot
                                    .precreate(&remote_path, actual_total_size, &block_list, rtype)
                                    .await
                                {
                                    Ok(resp) => resp,
                                    Err(e) => {
                                        let error_msg = format!("预创建文件失败: {}", e);
                                        error!("后台监控：{}", error_msg);
                                        Self::handle_upload_failure(
                                            &task_id_clone,
                                            &task,
                                            error_msg,
                                            &task_slot_pool_clone,
                                            ws_manager_clone.as_ref(),
                                            pm_clone.as_ref(),
                                            backup_notification_tx_clone.as_ref(),
                                            &active_count_clone,
                                        )
                                            .await;
                                        return;
                                    }
                                };

                                // 检查秒传
                                if precreate_response.is_rapid_upload() {
                                    info!("后台监控：秒传成功: {}", remote_path);
                                    // 🔥 秒传成功，释放槽位（任务不会注册到调度器）
                                    task_slot_pool_clone
                                        .release_fixed_slot(&task_id_clone)
                                        .await;
                                    let mut t = task.lock().await;
                                    t.mark_rapid_upload_success();
                                    drop(t);

                                    // 🔥 发送秒传成功通知
                                    if is_backup {
                                        if let Some(ref tx) = backup_notification_tx_clone {
                                            use crate::autobackup::events::TransferTaskType;
                                            use crate::autobackup::events::UploadCompletionMeta;
                                            // 秒传后调 filemetas 回填 server_mtime/fs_id，失败则降级为 None
                                            let upload_meta = match client_snapshot
                                                .filemetas(std::slice::from_ref(&remote_path))
                                                .await
                                            {
                                                Ok(resp) if resp.is_success() && !resp.list.is_empty() => {
                                                    let meta = &resp.list[0];
                                                    Some(UploadCompletionMeta {
                                                        fs_id: meta.fs_id,
                                                        mtime: meta.server_mtime,
                                                        size: meta.size,
                                                    })
                                                }
                                                Ok(_) => {
                                                    tracing::warn!(
                                                        "秒传后 filemetas 返回空列表: {}",
                                                        remote_path
                                                    );
                                                    None
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        "秒传后 filemetas 回填失败(降级为 None): {} - {}",
                                                        remote_path, e
                                                    );
                                                    None
                                                }
                                            };
                                            let notification =
                                                BackupTransferNotification::Completed {
                                                    task_id: task_id_clone.clone(),
                                                    task_type: TransferTaskType::Upload,
                                                    upload_meta,
                                                };
                                            let _ = tx.send(notification);
                                        }
                                    } else if let Some(ref ws) = ws_manager_clone {
                                        ws.send_if_subscribed(
                                            TaskEvent::Upload(UploadEvent::Completed {
                                                task_id: task_id_clone.clone(),
                                                completed_at: chrono::Utc::now().timestamp_millis(),
                                                is_rapid_upload: true,
                                                is_backup: false,

                                                owner_uid: Some(task_owner_uid_raw),
                                            }),
                                            None,
                                        );
                                    }
                                    return;
                                }

                                let upload_id = precreate_response.uploadid.clone();
                                if upload_id.is_empty() {
                                    let error_msg = "预创建失败：未获取到 uploadid".to_string();
                                    error!("后台监控：{}", error_msg);
                                    Self::handle_upload_failure(
                                        &task_id_clone,
                                        &task,
                                        error_msg,
                                        &task_slot_pool_clone,
                                        ws_manager_clone.as_ref(),
                                        pm_clone.as_ref(),
                                        backup_notification_tx_clone.as_ref(),
                                        &active_count_clone,
                                    )
                                        .await;
                                    return;
                                }

                                // 🔥 更新持久化元数据中的 upload_id
                                if let Some(ref pm_arc) = pm_clone {
                                    if let Err(e) = pm_arc
                                        .lock()
                                        .await
                                        .update_upload_id(&task_id_clone, upload_id.clone())
                                    {
                                        warn!("后台监控：更新上传任务 upload_id 失败: {}", e);
                                    }
                                }

                                // 3. 🔥 延迟创建分片管理器（只有预注册成功后才创建，节省内存）
                                // 使用实际文件大小（可能是加密后的大小）
                                // 🔥 set_owner_uid 防跨账号污染。
                                let chunk_owner_uid = task.lock().await.owner_uid;
                                let chunk_manager = {
                                    let mut cm =
                                        UploadChunkManager::with_vip_type(actual_total_size, vip_type);
                                    cm.set_owner_uid(chunk_owner_uid);

                                    // 如果是恢复的任务，标记已完成的分片
                                    if let Some(ref restored_info) = restored_completed_chunks {
                                        for &chunk_index in &restored_info.completed_chunks {
                                            // chunk_md5s 是 Vec，通过索引获取
                                            let md5 = restored_info
                                                .chunk_md5s
                                                .get(chunk_index)
                                                .cloned()
                                                .flatten();
                                            cm.mark_completed(chunk_index, md5);
                                        }
                                        info!(
                                            "后台监控：上传任务 {} 恢复了 {} 个已完成分片",
                                            task_id_clone,
                                            restored_info.completed_chunks.len()
                                        );
                                    }

                                    Arc::new(Mutex::new(cm))
                                };

                                // 🔥 将创建的分片管理器保存回 tasks（用于暂停恢复等场景）
                                if let Some(mut task_info) = tasks_clone.get_mut(&task_id_clone) {
                                    task_info.chunk_manager = Some(chunk_manager.clone());
                                }

                                // 🔥 克隆 ws_manager / persistence_manager 用于注册失败时的通知与持久化
                                let ws_manager_for_error = ws_manager_clone.clone();
                                let pm_for_error = pm_clone.clone();

                                // 4. 创建调度信息并注册到调度器
                                // 使用实际文件路径和大小（可能是加密后的）
                                let schedule_info = UploadTaskScheduleInfo {
                                    task_id: task_id_clone.clone(),
                                    task: task.clone(),
                                    chunk_manager,
                                    server_health: server_health_clone,
                                    client: client_clone,
                                    local_path: actual_local_path.to_path_buf(),
                                    remote_path: remote_path.to_string(),
                                    upload_id: upload_id.clone(),
                                    total_size: actual_total_size,
                                    block_list,
                                    cancellation_token: cancel_token,
                                    is_paused,
                                    is_merging: Arc::new(AtomicBool::new(false)),
                                    active_chunk_count,
                                    max_concurrent_chunks,
                                    uploaded_bytes,
                                    last_speed_time,
                                    last_speed_bytes,
                                    persistence_manager: pm_clone,
                                    ws_manager: ws_manager_clone,
                                    progress_throttler: Arc::new(ProgressThrottler::default()),
                                    backup_notification_tx: None,
                                    // 🔥 传入任务槽池引用，用于任务完成/失败时释放槽位
                                    task_slot_pool: Some(task_slot_pool_clone.clone()),
                                    // 🔥 槽位刷新节流器（30秒间隔，防止槽位超时释放）
                                    slot_touch_throttler: Some(Arc::new(crate::task_slot_pool::SlotTouchThrottler::new(
                                        task_slot_pool_clone.clone(),
                                        task_id_clone.clone(),
                                    ))),
                                    // 🔥 传入加密快照管理器，用于上传完成后保存加密映射
                                    snapshot_manager: snapshot_manager_clone,
                                    // 🔥 Manager 任务列表引用（用于任务完成时立即清理）
                                    manager_tasks: Some(tasks_clone.clone()),
                                };

                                if let Err(e) = scheduler_clone.register_task(schedule_info).await {
                                    let error_msg = format!("注册任务失败: {}", e);
                                    error!("后台监控：{}", error_msg);
                                    Self::handle_upload_failure(
                                        &task_id_clone,
                                        &task,
                                        error_msg,
                                        &task_slot_pool_clone,
                                        ws_manager_for_error.as_ref(),
                                        pm_for_error.as_ref(),
                                        backup_notification_tx_clone.as_ref(),
                                        &active_count_clone,
                                    )
                                        .await;
                                    return;
                                }

                                info!("后台监控：上传任务 {} 已注册到调度器", task_id_clone);
                            });
                        }
                        None => {
                            // 队列为空
                            break;
                        }
                    }
                }
            }
        });
    }

    /// 🔥 从恢复信息创建上传任务
    ///
    /// 用于程序启动时恢复未完成的上传任务
    /// 恢复的任务初始状态为 Paused，需要手动调用 start_task 启动
    ///
    /// # Arguments
    /// * `recovery_info` - 从持久化文件恢复的任务信息
    ///
    /// # Returns
    /// 恢复的任务 ID
    ///
    /// # 注意
    /// - upload_id 可能已过期，启动任务时会重新 precreate
    /// - 已完成的分片会在分片管理器中标记为完成
    pub async fn restore_task(&self, recovery_info: UploadRecoveryInfo) -> Result<String> {
        let task_id = recovery_info.task_id.clone();

        // 检查任务是否已存在
        if self.tasks.contains_key(&task_id) {
            anyhow::bail!("任务 {} 已存在，无法恢复", task_id);
        }

        // 验证源文件存在
        if !recovery_info.source_path.exists() {
            anyhow::bail!("源文件不存在: {:?}", recovery_info.source_path);
        }

        // 🔥 多账号 owner_uid 优先级 = recovery_info.owner_uid > self.owner_uid
        //
        // 旧注释假设了 per-uid manager 已就位，但实际启动期 recover_tasks 用的是
        // active_uid 的 manager 处理所有账号的 .meta，会跨账号混入；用 metadata 的
        // owner_uid（已经过 state.rs::filter_by_branch 兜底填充）才能保证显示正确。
        let resolved_owner_uid = recovery_info
            .owner_uid
            .map(crate::auth::Uid::new)
            .unwrap_or(self.owner_uid);

        // 创建恢复任务（使用 Paused 状态）
        // 🔥 根据是否为备份任务选择不同的构造方式
        let mut task = if recovery_info.is_backup {
            UploadTask::new_backup(
                recovery_info.source_path.clone(),
                recovery_info.target_path.clone(),
                recovery_info.file_size,
                recovery_info.backup_config_id.clone().unwrap_or_default(),
                recovery_info.encrypt_enabled,
                None, // backup_task_id - 恢复时不需要
                None, // backup_file_task_id - 恢复时不需要
            )
                .with_owner_uid(resolved_owner_uid)
        } else {
            UploadTask::new(
                recovery_info.source_path.clone(),
                recovery_info.target_path.clone(),
                recovery_info.file_size,
            )
                .with_owner_uid(resolved_owner_uid)
        };

        // 恢复任务 ID（保持原有 ID）
        task.id = task_id.clone();

        // 设置为暂停状态（等待用户手动恢复）
        task.status = UploadTaskStatus::Paused;

        // 设置已上传字节数
        task.uploaded_size = recovery_info.uploaded_bytes();
        task.created_at = recovery_info.created_at;

        // 设置分片信息
        task.total_chunks = recovery_info.total_chunks;
        task.completed_chunks = recovery_info.completed_count();

        // 🔥 恢复加密相关字段
        if recovery_info.encrypt_enabled {
            task.encrypt_enabled = true;
            // 🔥 从恢复信息中获取正确的 key_version，而不是使用默认值 1
            if let Some(key_version) = recovery_info.encryption_key_version {
                task.encryption_key_version = key_version;
            }
        }

        // 🔥 延迟创建分片管理器：保存恢复信息，在预注册成功后才创建
        // 这样可以避免大量恢复任务占用内存
        let restored_chunk_info = RestoredChunkInfo {
            chunk_size: recovery_info.chunk_size,
            // BitSet.iter() 直接返回 usize，不需要 copied()
            completed_chunks: recovery_info.completed_chunks.iter().collect(),
            chunk_md5s: recovery_info.chunk_md5s.clone(),
        };

        // 计算最大并发分片数
        let max_concurrent_chunks = calculate_upload_task_max_chunks(recovery_info.file_size);

        info!(
            "恢复上传任务: id={}, 文件={:?}, 已完成 {}/{} 分片 ({:.1}%){} (分片管理器延迟创建)",
            task_id,
            recovery_info.source_path,
            recovery_info.completed_count(),
            recovery_info.total_chunks,
            if recovery_info.total_chunks > 0 {
                (recovery_info.completed_count() as f64 / recovery_info.total_chunks as f64) * 100.0
            } else {
                0.0
            },
            if recovery_info.is_backup {
                "（备份任务）"
            } else {
                ""
            }
        );

        // 保存任务信息（🔥 分片管理器延迟创建，此处为 None）
        let task_info = UploadTaskInfo {
            task: Arc::new(Mutex::new(task)),
            chunk_manager: None, // 延迟创建：预注册成功后才创建
            cancel_token: CancellationToken::new(),
            max_concurrent_chunks,
            active_chunk_count: Arc::new(AtomicUsize::new(0)),
            is_paused: Arc::new(AtomicBool::new(true)), // 恢复的任务默认暂停
            uploaded_bytes: Arc::new(AtomicU64::new(recovery_info.uploaded_bytes())),
            last_speed_time: Arc::new(Mutex::new(std::time::Instant::now())),
            last_speed_bytes: Arc::new(AtomicU64::new(0)),
            // 🔥 保存恢复的 upload_id（如果存在）
            restored_upload_id: recovery_info.upload_id.clone(),
            // 🔥 保存恢复的分片信息（用于延迟创建分片管理器）
            restored_completed_chunks: Some(restored_chunk_info),
        };

        self.tasks.insert(task_id.clone(), task_info);

        // 🔥 恢复持久化状态（重新加载到内存）
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.restore_task_state(
                &task_id,
                crate::persistence::TaskType::Upload,
                recovery_info.total_chunks,
            ) {
                warn!("恢复任务持久化状态失败: {}", e);
            }
        }

        // 重建去重索引
        let dedup_path = recovery_info.original_remote_path
            .as_deref()
            .unwrap_or(&recovery_info.target_path);
        self.rebuild_dedup_index_entry(&task_id, &recovery_info.source_path, dedup_path);

        Ok(task_id)
    }

    /// 🔥 批量恢复上传任务
    ///
    /// 从恢复信息列表批量创建任务
    ///
    /// # Arguments
    /// * `recovery_infos` - 恢复信息列表
    ///
    /// # Returns
    /// (成功数, 失败数)
    pub async fn restore_tasks(&self, recovery_infos: Vec<UploadRecoveryInfo>) -> (usize, usize) {
        let mut success = 0;
        let mut failed = 0;

        for info in recovery_infos {
            match self.restore_task(info).await {
                Ok(_) => success += 1,
                Err(e) => {
                    warn!("恢复上传任务失败: {}", e);
                    failed += 1;
                }
            }
        }

        info!("上传任务批量恢复完成: {} 成功, {} 失败", success, failed);
        (success, failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UserAuth;
    use crate::AppConfig;
    use std::fs;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn create_test_manager() -> UploadManager {
        let user_auth = UserAuth::new(123456789, "test_user".to_string(), "test_bduss".to_string());
        let client = NetdiskClient::new(user_auth.clone()).unwrap();
        let config = AppConfig::default();
        // 构造时直接注入 budget_scheduler（必填）
        let budget_scheduler = crate::downloader::budget_scheduler::BudgetScheduler::new(
            crate::downloader::budget_scheduler::BudgetSchedulerConfig::default(),
        );
        UploadManager::new_for_account(
            client,
            &user_auth,
            &config.upload,
            budget_scheduler,
            Path::new("config"),
        )
    }

    #[tokio::test]
    async fn test_create_task() {
        let manager = create_test_manager();

        // 创建临时文件
        let mut temp_file = NamedTempFile::new().unwrap();
        let content = b"Test file content for upload";
        temp_file.write_all(content).unwrap();
        temp_file.flush().unwrap();

        let result = manager
            .create_task(
                temp_file.path().to_path_buf(),
                "/test/upload.txt".to_string(),
                false, // encrypt
                false, // is_folder_upload
                None,  // conflict_strategy
            )
            .await;

        assert!(result.is_ok());

        let task_id = result.unwrap();
        let task = manager.get_task(&task_id).await;

        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.status, UploadTaskStatus::Pending);
        assert_eq!(task.total_size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_get_all_tasks() {
        let manager = create_test_manager();

        // 创建多个临时文件和任务
        for i in 0..3 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file
                .write_all(format!("Content {}", i).as_bytes())
                .unwrap();
            temp_file.flush().unwrap();

            manager
                .create_task(
                    temp_file.path().to_path_buf(),
                    format!("/test/file{}.txt", i),
                    false, // encrypt
                    false, // is_folder_upload
                    None,  // conflict_strategy
                )
                .await
                .unwrap();
        }

        let tasks = manager.get_all_tasks().await;
        assert_eq!(tasks.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_task() {
        let manager = create_test_manager();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"Test content").unwrap();
        temp_file.flush().unwrap();

        let task_id = manager
            .create_task(
                temp_file.path().to_path_buf(),
                "/test/delete.txt".to_string(),
                false, // encrypt
                false, // is_folder_upload
                None,  // conflict_strategy
            )
            .await
            .unwrap();

        // 确认任务存在
        assert!(manager.get_task(&task_id).await.is_some());

        // 删除任务
        manager.delete_task(&task_id).await.unwrap();

        // 确认任务已删除
        assert!(manager.get_task(&task_id).await.is_none());
    }

    #[tokio::test]
    async fn test_create_folder_task() {
        let manager = create_test_manager();

        // 创建测试文件夹结构
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // 创建文件
        fs::write(root.join("file1.txt"), "content1").unwrap();
        fs::write(root.join("file2.txt"), "content2").unwrap();

        // 创建子目录和文件
        fs::create_dir(root.join("subdir")).unwrap();
        fs::write(root.join("subdir/file3.txt"), "content3").unwrap();

        // 创建文件夹上传任务
        let result = manager
            .create_folder_task(root, "/test/folder".to_string(), None, false)
            .await;

        assert!(result.is_ok());

        let task_ids = result.unwrap();
        assert_eq!(task_ids.len(), 3, "应该创建3个上传任务");

        // 验证所有任务都已创建
        let all_tasks = manager.get_all_tasks().await;
        assert_eq!(all_tasks.len(), 3);

        // 验证任务状态
        for task in all_tasks {
            assert_eq!(task.status, UploadTaskStatus::Pending);
            assert!(task.remote_path.starts_with("/test/folder/"));
        }
    }

    #[tokio::test]
    async fn test_create_folder_task_empty_folder() {
        let manager = create_test_manager();

        // 创建空文件夹
        let temp_dir = TempDir::new().unwrap();

        // 尝试创建文件夹上传任务
        let result = manager
            .create_folder_task(temp_dir.path(), "/test/empty".to_string(), None, false)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("文件夹为空或无可上传文件"));
    }

    #[tokio::test]
    async fn test_create_batch_tasks() {
        let manager = create_test_manager();

        // 创建多个临时文件
        let mut temp_files = Vec::new();
        for i in 0..3 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file
                .write_all(format!("Content {}", i).as_bytes())
                .unwrap();
            temp_file.flush().unwrap();
            temp_files.push(temp_file);
        }

        // 准备批量任务
        let files: Vec<(PathBuf, String)> = temp_files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.path().to_path_buf(), format!("/test/file{}.txt", i)))
            .collect();

        // 批量创建任务
        let result = manager.create_batch_tasks(files, false, None).await;

        assert!(result.is_ok());

        let task_ids = result.unwrap();
        assert_eq!(task_ids.len(), 3);

        // 验证所有任务
        let all_tasks = manager.get_all_tasks().await;
        assert_eq!(all_tasks.len(), 3);
    }

    // ========== 🔥 步骤9：等待队列优先级测试 ==========

    #[tokio::test]
    async fn test_waiting_queue_priority_normal_before_backup() {
        let manager = create_test_manager();

        // 创建临时文件
        let mut temp_file1 = NamedTempFile::new().unwrap();
        temp_file1.write_all(b"backup content").unwrap();
        temp_file1.flush().unwrap();

        let mut temp_file2 = NamedTempFile::new().unwrap();
        temp_file2.write_all(b"normal content").unwrap();
        temp_file2.flush().unwrap();

        // 创建备份任务
        let backup_task_id = manager
            .create_backup_task(
                temp_file1.path().to_path_buf(),
                "/test/backup.txt".to_string(),
                "config-123".to_string(),
                false,
                Some("backup-task-1".to_string()),
                Some("file-task-1".to_string()),
                None,
                crate::auth::Uid::new(0),
            )
            .await
            .unwrap();

        // 创建普通任务
        let normal_task_id = manager
            .create_task(
                temp_file2.path().to_path_buf(),
                "/test/normal.txt".to_string(),
                false, // encrypt
                false, // is_folder_upload
                None,
            )
            .await
            .unwrap();

        // 手动将备份任务加入等待队列
        manager
            .add_to_waiting_queue_by_priority(&backup_task_id, true)
            .await;

        // 手动将普通任务加入等待队列
        manager
            .add_to_waiting_queue_by_priority(&normal_task_id, false)
            .await;

        // 验证队列顺序：普通任务应该在备份任务之前
        let queue = manager.waiting_queue.read().await;
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[0], normal_task_id, "普通任务应该在队列前面");
        assert_eq!(queue[1], backup_task_id, "备份任务应该在队列后面");
    }

    #[tokio::test]
    async fn test_waiting_queue_backup_at_end() {
        let manager = create_test_manager();

        // 创建多个临时文件
        let mut temp_files = Vec::new();
        for i in 0..3 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file
                .write_all(format!("content {}", i).as_bytes())
                .unwrap();
            temp_file.flush().unwrap();
            temp_files.push(temp_file);
        }

        // 创建普通任务
        let normal_task_id = manager
            .create_task(
                temp_files[0].path().to_path_buf(),
                "/test/normal.txt".to_string(),
                false, // encrypt
                false, // is_folder_upload
                None,  // conflict_strategy
            )
            .await
            .unwrap();

        // 创建两个备份任务
        let backup_task_id1 = manager
            .create_backup_task(
                temp_files[1].path().to_path_buf(),
                "/test/backup1.txt".to_string(),
                "config-1".to_string(),
                false,
                Some("backup-task-1".to_string()),
                Some("file-task-1".to_string()),
                None, // conflict_strategy
                crate::auth::Uid::new(0),
            )
            .await
            .unwrap();

        let backup_task_id2 = manager
            .create_backup_task(
                temp_files[2].path().to_path_buf(),
                "/test/backup2.txt".to_string(),
                "config-2".to_string(),
                false,
                Some("backup-task-2".to_string()),
                Some("file-task-2".to_string()),
                None, // conflict_strategy
                crate::auth::Uid::new(0),
            )
            .await
            .unwrap();

        // 按顺序加入等待队列：备份1 -> 备份2 -> 普通
        manager
            .add_to_waiting_queue_by_priority(&backup_task_id1, true)
            .await;
        manager
            .add_to_waiting_queue_by_priority(&backup_task_id2, true)
            .await;
        manager
            .add_to_waiting_queue_by_priority(&normal_task_id, false)
            .await;

        // 验证队列顺序：普通任务应该在所有备份任务之前
        let queue = manager.waiting_queue.read().await;
        assert_eq!(queue.len(), 3);
        assert_eq!(queue[0], normal_task_id, "普通任务应该在队列最前面");
        assert_eq!(queue[1], backup_task_id1, "备份任务1应该在普通任务之后");
        assert_eq!(queue[2], backup_task_id2, "备份任务2应该在队列最后");
    }

    #[tokio::test]
    async fn test_has_normal_tasks_waiting() {
        let manager = create_test_manager();

        // 创建临时文件
        let mut temp_file1 = NamedTempFile::new().unwrap();
        temp_file1.write_all(b"backup content").unwrap();
        temp_file1.flush().unwrap();

        let mut temp_file2 = NamedTempFile::new().unwrap();
        temp_file2.write_all(b"normal content").unwrap();
        temp_file2.flush().unwrap();

        // 初始状态：队列为空
        assert!(
            !manager.has_normal_tasks_waiting().await,
            "空队列应该返回 false"
        );

        // 创建备份任务
        let backup_task_id = manager
            .create_backup_task(
                temp_file1.path().to_path_buf(),
                "/test/backup.txt".to_string(),
                "config-123".to_string(),
                false,
                Some("backup-task-1".to_string()),
                Some("file-task-1".to_string()),
                None, // conflict_strategy
                crate::auth::Uid::new(0),
            )
            .await
            .unwrap();

        // 创建普通任务
        let normal_task_id = manager
            .create_task(
                temp_file2.path().to_path_buf(),
                "/test/normal.txt".to_string(),
                false, // encrypt
                false, // is_folder_upload
                None,  // conflict_strategy
            )
            .await
            .unwrap();

        // 手动将任务加入等待队列（直接操作队列，避免锁竞争）
        {
            let mut queue = manager.waiting_queue.write().await;
            queue.push_back(backup_task_id.clone());
        }

        // 只有备份任务时应该返回 false
        assert!(
            !manager.has_normal_tasks_waiting().await,
            "只有备份任务时应该返回 false"
        );

        // 添加普通任务到队列
        {
            let mut queue = manager.waiting_queue.write().await;
            queue.push_front(normal_task_id.clone()); // 普通任务在前
        }

        // 有普通任务时应该返回 true
        assert!(
            manager.has_normal_tasks_waiting().await,
            "有普通任务时应该返回 true"
        );
    }

    #[tokio::test]
    async fn test_task_slot_pool_initialization() {
        let manager = create_test_manager();

        // 验证任务槽池已正确初始化
        let slot_pool = manager.task_slot_pool();
        let max_slots = slot_pool.max_slots();
        let available_slots = slot_pool.available_slots().await;

        // 初始状态：所有槽位都可用
        assert!(max_slots > 0, "最大槽位数应该大于0");
        assert_eq!(available_slots, max_slots, "初始状态所有槽位都应该可用");
    }
}
