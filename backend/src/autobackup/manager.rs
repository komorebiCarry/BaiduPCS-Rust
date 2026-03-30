//! 自动备份管理器
//!
//! 主要协调器，管理备份配置、任务调度和执行

use anyhow::{anyhow, Result};
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use crate::common::{ProxyConfig, ProxyFallbackManager};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::autobackup::TransferTaskStatus;
use crate::downloader::DownloadManager;
use crate::server::events::{BackupEvent as WsBackupEvent, TaskEvent};
use crate::server::websocket::WebSocketManager;
use crate::uploader::UploadManager;
use crate::UploadTaskStatus;
use super::config::*;
use crate::encryption::{EncryptionConfigStore, EncryptionService, SnapshotManager};
use super::persistence::BackupPersistenceManager;
use super::priority::{PrepareResourcePool, PriorityManager};
use super::record::{BackupRecordManager, calculate_head_md5};
use super::scheduler::{ChangeAggregator, ChangeEvent, PollScheduler, PollScheduleConfig, ScheduledTime, TaskController, TriggerSource, task_loop};
use super::task::*;
use super::validation::{validate_for_create, validate_for_update, validate_for_execute};
use super::scan_cache::ScanCacheManager;
use super::watcher::{FileChangeEvent, FileWatcher};

/// 自动备份管理器
pub struct AutoBackupManager {
    /// 备份配置存储
    configs: Arc<DashMap<String, BackupConfig>>,
    /// 备份任务存储
    tasks: Arc<DashMap<String, BackupTask>>,
    /// 记录管理器
    record_manager: Arc<BackupRecordManager>,
    /// 快照管理器
    snapshot_manager: Arc<SnapshotManager>,
    /// 加密服务（可选）
    encryption_service: Arc<RwLock<Option<EncryptionService>>>,
    /// 加密配置
    encryption_config: Arc<RwLock<EncryptionConfig>>,
    /// 加密配置存储（用于持久化密钥到 encryption.json）
    encryption_config_store: Arc<EncryptionConfigStore>,
    /// 任务持久化管理器（SQLite）
    persistence_manager: Arc<BackupPersistenceManager>,
    /// 文件监听器
    file_watcher: Arc<RwLock<Option<FileWatcher>>>,
    /// 轮询调度器
    poll_scheduler: Arc<RwLock<Option<PollScheduler>>>,
    /// 准备资源池
    prepare_pool: Arc<PrepareResourcePool>,
    /// 优先级管理器
    priority_manager: Arc<PriorityManager>,
    /// 事件发送通道
    event_tx: mpsc::UnboundedSender<ChangeEvent>,
    /// 配置存储路径
    config_path: PathBuf,
    /// 数据库路径（预留用于后续数据库操作）
    #[allow(dead_code)]
    db_path: PathBuf,
    /// 临时文件目录
    temp_dir: PathBuf,
    /// WebSocket 管理器（用于发送实时事件）
    /// 使用 Weak 引用避免循环引用导致的内存泄漏
    ws_manager: Arc<RwLock<Option<Weak<WebSocketManager>>>>,
    /// 上传管理器引用（用于复用现有上传功能）
    /// 使用 Weak 引用避免循环引用导致的内存泄漏
    upload_manager: Arc<RwLock<Option<Weak<UploadManager>>>>,
    /// 下载管理器引用（用于复用现有下载功能）
    /// 使用 Weak 引用避免循环引用导致的内存泄漏
    download_manager: Arc<RwLock<Option<Weak<DownloadManager>>>>,
    /// 聚合事件接收器（用于启动事件消费循环）
    aggregated_rx: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<ChangeEvent>>>>,
    /// 任务控制器（每个配置一个，用于防止并发执行和触发合并）
    task_controllers: Arc<DashMap<String, Arc<TaskController>>>,
    /// 聚合器任务句柄（用于 shutdown 时取消）
    aggregator_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    /// 扫描缓存管理器（增量扫描用）
    scan_cache_manager: Arc<ScanCacheManager>,
    /// 代理配置（运行时可通过热更新变更）
    proxy_config: Arc<RwLock<Option<ProxyConfig>>>,
    /// 代理故障回退管理器
    fallback_mgr: Arc<RwLock<Option<Arc<ProxyFallbackManager>>>>,
}

impl AutoBackupManager {
    /// 创建新的自动备份管理器
    ///
    /// # 参数
    /// - `config_path`: 备份配置文件路径
    /// - `db_path`: 数据库路径
    /// - `temp_dir`: 临时文件目录
    /// - `record_manager`: 备份记录管理器（外部传入，用于复用）
    /// - `snapshot_manager`: 快照管理器（外部传入，用于复用）
    pub async fn new(
        config_path: PathBuf,
        db_path: PathBuf,
        temp_dir: PathBuf,
        record_manager: Arc<BackupRecordManager>,
        snapshot_manager: Arc<SnapshotManager>,
    ) -> Result<Self> {
        // 创建事件通道
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (aggregated_tx, aggregated_rx) = mpsc::unbounded_channel();

        // 创建变更聚合器
        let mut aggregator = ChangeAggregator::with_default_window(event_rx, aggregated_tx);

        // 启动聚合器并保存句柄
        let aggregator_handle = tokio::spawn(async move {
            aggregator.run().await;
        });
        let aggregator_handle = Arc::new(tokio::sync::Mutex::new(Some(aggregator_handle)));

        // 保存 aggregated_rx 用于后续启动事件消费循环
        let aggregated_rx = Arc::new(tokio::sync::Mutex::new(Some(aggregated_rx)));

        // 创建准备资源池
        let prepare_pool = Arc::new(PrepareResourcePool::new(2, 2));

        // 创建优先级管理器
        let priority_manager = Arc::new(PriorityManager::new(3));

        // 确保临时目录存在
        std::fs::create_dir_all(&temp_dir)?;

        // 创建加密配置存储（使用配置文件所在目录）
        let config_dir = config_path.parent().unwrap_or(Path::new("."));
        let encryption_config_store = Arc::new(EncryptionConfigStore::new(config_dir));

        // 尝试从 encryption.json 加载已保存的密钥
        let (encryption_service, encryption_config) = match encryption_config_store.load() {
            Ok(Some(key_config)) => {
                tracing::info!(
                    "从 encryption.json 加载加密密钥成功, key_version={}, algorithm={:?}, history_count={}",
                    key_config.current.key_version,
                    key_config.current.algorithm,
                    key_config.history.len()
                );
                match EncryptionService::from_base64_key(&key_config.current.master_key, key_config.current.algorithm) {
                    Ok(service) => {
                        let config = EncryptionConfig {
                            enabled: true,
                            master_key: Some(key_config.current.master_key),
                            algorithm: key_config.current.algorithm,
                            key_created_at: Some(chrono::DateTime::from_timestamp_millis(key_config.current.created_at)
                                .unwrap_or_else(chrono::Utc::now)),
                            key_version: key_config.current.key_version,
                            last_used_at: None,
                        };
                        (Some(service), config)
                    }
                    Err(e) => {
                        tracing::warn!("加载加密密钥失败，密钥可能已损坏: {}", e);
                        (None, EncryptionConfig::default())
                    }
                }
            }
            Ok(None) => {
                tracing::info!("未找到已保存的加密密钥");
                (None, EncryptionConfig::default())
            }
            Err(e) => {
                tracing::warn!("读取加密配置失败: {}", e);
                (None, EncryptionConfig::default())
            }
        };

        // 创建任务持久化管理器
        let persistence_manager = Arc::new(BackupPersistenceManager::new(&db_path)?);
        tracing::info!("备份任务持久化管理器已创建");

        // 创建扫描缓存管理器（增量扫描用）
        let scan_cache_db = db_path.parent().unwrap_or(Path::new(".")).join("scan_cache.db");
        let scan_cache_manager = Arc::new(
            ScanCacheManager::new(&scan_cache_db)
                .map_err(|e| anyhow!("创建扫描缓存管理器失败: {}", e))?,
        );

        let manager = Self {
            configs: Arc::new(DashMap::new()),
            tasks: Arc::new(DashMap::new()),
            record_manager,
            snapshot_manager,
            encryption_service: Arc::new(RwLock::new(encryption_service)),
            encryption_config: Arc::new(RwLock::new(encryption_config)),
            encryption_config_store,
            persistence_manager,
            file_watcher: Arc::new(RwLock::new(None)),
            poll_scheduler: Arc::new(RwLock::new(None)),
            prepare_pool,
            priority_manager,
            event_tx,
            config_path,
            db_path,
            temp_dir,
            ws_manager: Arc::new(RwLock::new(None)),
            upload_manager: Arc::new(RwLock::new(None)),
            download_manager: Arc::new(RwLock::new(None)),
            aggregated_rx,
            task_controllers: Arc::new(DashMap::new()),
            aggregator_handle,
            scan_cache_manager,
            proxy_config: Arc::new(RwLock::new(None)),
            fallback_mgr: Arc::new(RwLock::new(None)),
        };

        // 加载已保存的配置
        manager.load_configs().await?;

        // 恢复未完成的任务
        manager.restore_incomplete_tasks()?;

        // 🔥 执行兜底同步：同步历史归档中已完成的备份任务到 backup_file_tasks 表
        manager.sync_completed_backup_tasks_from_history()?;

        Ok(manager)
    }

    /// 🔥 兜底同步：从历史归档表同步已完成的备份任务到 backup_file_tasks 表
    ///
    /// 防止服务重启时数据库和 WAL、元数据不一致的情况：
    /// - 当上传/下载任务已归档到 task_history 表（is_backup=1, status='completed'）
    /// - 但对应的 backup_file_tasks 表中的状态可能还未更新
    /// - 此方法会根据 related_task_id 找到对应的备份文件任务并更新状态
    fn sync_completed_backup_tasks_from_history(&self) -> anyhow::Result<()> {
        use crate::persistence::HistoryDbManager;

        // 创建历史数据库管理器（只读查询）
        let history_db = match HistoryDbManager::new(&self.db_path) {
            Ok(db) => db,
            Err(e) => {
                tracing::warn!("兜底同步: 无法连接历史数据库，跳过同步: {}", e);
                return Ok(());
            }
        };

        // 查询所有已完成的备份任务
        let completed_backup_tasks = match history_db.load_completed_backup_tasks() {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!("兜底同步: 查询已完成备份任务失败，跳过同步: {}", e);
                return Ok(());
            }
        };

        if completed_backup_tasks.is_empty() {
            tracing::debug!("兜底同步: 没有需要同步的已完成备份任务");
            return Ok(());
        }

        tracing::info!(
            "兜底同步: 发现 {} 个已完成的备份任务，开始同步到 backup_file_tasks 表",
            completed_backup_tasks.len()
        );

        // 批量更新备份文件任务状态
        // task_id 在 task_history 中是上传/下载任务的 ID，对应 backup_file_tasks 中的 related_task_id
        let affected_backup_task_ids = match self.persistence_manager.complete_file_tasks_by_related_task_ids(&completed_backup_tasks) {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!("兜底同步: 批量更新备份文件任务失败: {}", e);
                return Ok(());
            }
        };

        // 重新计算受影响的主任务进度
        for backup_task_id in &affected_backup_task_ids {
            if let Err(e) = self.persistence_manager.recalculate_task_progress(backup_task_id) {
                tracing::warn!(
                    "兜底同步: 重新计算主任务进度失败: backup_task_id={}, error={}",
                    backup_task_id, e
                );
            }
        }

        if !affected_backup_task_ids.is_empty() {
            tracing::info!(
                "兜底同步完成: 更新了 {} 个主任务的进度",
                affected_backup_task_ids.len()
            );
        }

        Ok(())
    }

    /// 恢复未完成的任务
    ///
    /// 服务重启后，将未完成的任务恢复到内存中，并重置为待执行状态
    /// 正在执行中的任务（Preparing/Transferring）会被重置为 Queued，等待重新调度
    ///
    /// 关键修复：从 SQLite 加载文件子任务回填到 task.pending_files，
    /// 同时重建 related_task_id 映射，确保能复用 uploader 已恢复的任务
    fn restore_incomplete_tasks(&self) -> Result<()> {
        let tasks = self.persistence_manager.load_incomplete_tasks()?;

        if tasks.is_empty() {
            tracing::info!("没有需要恢复的备份任务");
            return Ok(());
        }

        tracing::info!("恢复 {} 个未完成的备份任务", tasks.len());

        for mut task in tasks {
            let task_id = task.id.clone();
            let old_status = task.status;

            // 服务重启后，正在执行的任务需要重置为待执行状态
            // 因为执行上下文（如文件句柄、网络连接）已经丢失
            match task.status {
                BackupTaskStatus::Preparing | BackupTaskStatus::Transferring | BackupTaskStatus::Paused => {
                    task.status = BackupTaskStatus::Queued;
                    task.sub_phase = None;
                    tracing::info!(
                        "恢复备份任务: {} (状态从 {:?} 重置为 Queued)",
                        task_id, old_status
                    );
                }
                BackupTaskStatus::Queued => {
                    // 保持原状态
                    tracing::info!(
                        "恢复备份任务: {} (状态: {:?})",
                        task_id, task.status
                    );
                }
                _ => {
                    // Completed/Failed/Cancelled/PartiallyCompleted 不应该出现在未完成列表
                    tracing::warn!(
                        "跳过已完成的任务: {} (状态: {:?})",
                        task_id, task.status
                    );
                    continue;
                }
            }

            // 【关键修复】从 SQLite 加载非终态文件任务，回填 pending_files
            match self.persistence_manager.load_file_tasks_for_restore(&task_id) {
                Ok(mut file_tasks) => {
                    let files_loaded = file_tasks.len();
                    let mut related_task_id_count = 0;

                    // 重置文件任务中正在执行的状态，并重建映射
                    for file_task in file_tasks.iter_mut() {
                        // 重置执行中状态为 Pending
                        match file_task.status {
                            BackupFileStatus::Checking
                            | BackupFileStatus::Encrypting
                            | BackupFileStatus::WaitingTransfer
                            | BackupFileStatus::Transferring => {
                                file_task.status = BackupFileStatus::Pending;
                            }
                            _ => {}
                        }

                        // 重建 related_task_id 映射
                        if let Some(ref related_id) = file_task.related_task_id {
                            related_task_id_count += 1;

                            // 根据操作类型填充对应的待完成集合
                            match file_task.backup_operation_type {
                                Some(BackupOperationType::Upload) => {
                                    task.pending_upload_task_ids.insert(related_id.clone());
                                }
                                Some(BackupOperationType::Download) => {
                                    task.pending_download_task_ids.insert(related_id.clone());
                                }
                                None => {
                                    // 默认按上传处理（向后兼容）
                                    task.pending_upload_task_ids.insert(related_id.clone());
                                }
                            }

                            // 重建 transfer_task_map: transfer_task_id -> file_task_id
                            task.transfer_task_map.insert(related_id.clone(), file_task.id.clone());
                        }
                    }

                    // 回填 pending_files
                    task.pending_files = file_tasks;

                    // 详细日志：便于验证断点续传是否生效
                    tracing::info!(
                        "恢复备份任务文件子任务: task_id={}, files_loaded={}, related_task_id_count={}, \
                         pending_upload_ids={}, pending_download_ids={}",
                        task_id,
                        files_loaded,
                        related_task_id_count,
                        task.pending_upload_task_ids.len(),
                        task.pending_download_task_ids.len()
                    );

                    if files_loaded > 0 && related_task_id_count > 0 {
                        tracing::info!(
                            "检测到已恢复的文件任务，将跳过扫描直接续传: task_id={}",
                            task_id
                        );
                    } else if files_loaded == 0 {
                        tracing::info!(
                            "未恢复到任何文件子任务，将重新扫描目录: task_id={}",
                            task_id
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "加载文件子任务失败，将重新扫描目录: task_id={}, error={}",
                        task_id, e
                    );
                    // pending_files 保持为空，后续会重新扫描
                }
            }

            self.tasks.insert(task_id.clone(), task.clone());

            // 🔥 持久化状态变更到数据库
            // 如果状态从 Paused/Preparing/Transferring 改为 Queued，需要更新数据库
            if old_status != task.status {
                if let Err(e) = self.persistence_manager.save_task(&task) {
                    tracing::warn!("持久化恢复任务的状态变更失败: task={}, error={}", task_id, e);
                }
            }
        }

        Ok(())
    }

    /// 服务启动后恢复执行 Queued 状态的任务
    ///
    /// 在 start_event_consumer 中调用，为恢复的任务触发执行
    /// 检查所有 Queued 状态的任务，为其对应的配置触发备份执行
    async fn resume_queued_tasks_on_startup(self: &Arc<Self>) {
        // 收集所有需要恢复执行的配置ID（去重）
        let config_ids_to_resume: std::collections::HashSet<String> = self.tasks.iter()
            .filter(|t| matches!(t.status, BackupTaskStatus::Queued))
            .map(|t| t.config_id.clone())
            .collect();

        if config_ids_to_resume.is_empty() {
            tracing::info!("没有需要恢复执行的 Queued 任务");
            return;
        }

        tracing::info!(
            "发现 {} 个配置有待恢复的 Queued 任务，开始触发执行",
            config_ids_to_resume.len()
        );

        for config_id in config_ids_to_resume {
            // 检查配置是否存在且启用
            let config = match self.get_config(&config_id) {
                Some(c) if c.enabled => c,
                Some(_) => {
                    tracing::warn!("配置已禁用，跳过恢复任务: config={}", config_id);
                    continue;
                }
                None => {
                    tracing::warn!("配置不存在，跳过恢复任务: config={}", config_id);
                    continue;
                }
            };

            // 获取或创建 TaskController 并触发执行
            let controller = self.task_controllers
                .entry(config_id.clone())
                .or_insert_with(|| {
                    let ctrl = Arc::new(TaskController::new(config_id.clone()));

                    // 为新控制器启动任务执行循环
                    let ctrl_clone = ctrl.clone();
                    let manager = Arc::clone(self);
                    let cfg = config.clone();

                    tokio::spawn(async move {
                        task_loop(ctrl_clone, || {
                            let m = manager.clone();
                            let c = cfg.clone();
                            async move {
                                m.execute_backup_for_config(&c).await
                            }
                        }).await;
                    });

                    tracing::info!("为配置 {} 创建了新的 TaskController（恢复任务）", config_id);
                    ctrl
                })
                .clone();

            // 触发执行（使用 Manual 作为触发源，表示系统恢复）
            if controller.trigger(TriggerSource::Manual) {
                tracing::info!(
                    "已触发配置 {} 的恢复任务执行（running: {}, pending: {}）",
                    config_id, controller.is_running(), controller.has_pending()
                );
            } else {
                tracing::debug!(
                    "配置 {} 已有任务在执行，恢复触发被合并",
                    config_id
                );
            }
        }
    }

    // ==================== 配置管理 ====================

    /// 创建备份配置
    pub async fn create_config(&self, request: CreateBackupConfigRequest) -> Result<BackupConfig> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();

        // 🔥 冲突校验：防止同方向重复任务和上传/下载闭环
        // 对比范围：所有现存配置（不区分 enabled/disabled）
        let existing_configs: Vec<BackupConfig> = self.configs.iter().map(|c| c.clone()).collect();
        let conflict_result = validate_for_create(
            Path::new(&request.local_path),
            &request.remote_path,
            request.direction,
            &existing_configs,
        );
        if conflict_result.has_conflict {
            return Err(anyhow!(conflict_result.error_message.unwrap_or_else(|| "配置冲突".to_string())));
        }

        // 验证加密选项
        if request.encrypt_enabled {
            let encryption_config = self.encryption_config.read();
            if !encryption_config.enabled || encryption_config.master_key.is_none() {
                return Err(anyhow!("加密功能未启用或密钥未配置"));
            }
        }

        // 验证下载备份不能启用监听
        if request.direction == BackupDirection::Download && request.watch_config.enabled {
            return Err(anyhow!("下载备份不支持文件监听"));
        }

        let config = BackupConfig {
            id: id.clone(),
            name: request.name,
            local_path: PathBuf::from(&request.local_path),
            remote_path: request.remote_path,
            direction: request.direction,
            watch_config: request.watch_config,
            poll_config: request.poll_config,
            filter_config: request.filter_config,
            encrypt_enabled: request.encrypt_enabled,
            enabled: true,
            created_at: now,
            updated_at: now,
            upload_conflict_strategy: request.upload_conflict_strategy,
            download_conflict_strategy: request.download_conflict_strategy,
        };

        // 保存配置
        self.configs.insert(id.clone(), config.clone());
        self.save_configs().await?;

        // 启动监听和轮询
        if config.enabled {
            self.start_config_services(&config).await?;
        }

        tracing::info!("Created backup config: {} ({})", config.name, config.id);

        // 创建配置后立即触发一次全量备份
        // 通过事件通道触发，让 TaskController 来管理执行
        // 这样可以正确处理并发控制和任务调度
        if config.enabled {
            // 发送手动触发事件到事件通道
            let event = ChangeEvent::PollEvent {
                config_id: config.id.clone(),
            };
            if let Err(e) = self.event_tx.send(event) {
                tracing::warn!(
                    "配置创建后触发首次备份失败（事件发送失败）: config={}, error={}",
                    config.id, e
                );
            } else {
                tracing::info!(
                    "配置创建后已触发首次全量备份事件: config={}",
                    config.id
                );
            }
        }

        Ok(config)
    }

    /// 更新备份配置
    pub async fn update_config(&self, id: &str, request: UpdateBackupConfigRequest) -> Result<BackupConfig> {
        let mut config = self.configs.get_mut(id)
            .ok_or_else(|| anyhow!("配置不存在: {}", id))?;

        // 验证：encrypt_enabled 创建后不可修改（硬性约束 1.5.3）
        // 注意：UpdateBackupConfigRequest 中不包含 encrypt_enabled 字段，这是设计决策
        // 如果尝试通过其他方式修改，这里会拦截

        // 🔥 冲突校验：防止更新后产生同方向重复任务或上传/下载闭环
        // 构建更新后的路径用于校验
        let updated_local_path = request.local_path.as_ref()
            .map(|p| PathBuf::from(p))
            .unwrap_or_else(|| config.local_path.clone());
        let updated_remote_path = request.remote_path.as_ref()
            .cloned()
            .unwrap_or_else(|| config.remote_path.clone());

        // 获取所有现存配置用于校验
        let existing_configs: Vec<BackupConfig> = self.configs.iter().map(|c| c.clone()).collect();
        let conflict_result = validate_for_update(
            id,
            &updated_local_path,
            &updated_remote_path,
            config.direction, // direction 不可更新，使用原值
            &existing_configs,
        );
        if conflict_result.has_conflict {
            return Err(anyhow!(conflict_result.error_message.unwrap_or_else(|| "配置冲突".to_string())));
        }

        // 停止旧的服务
        self.stop_config_services(id).await?;

        // 更新字段
        if let Some(name) = request.name {
            config.name = name;
        }
        if let Some(local_path) = request.local_path {
            config.local_path = PathBuf::from(local_path);
        }
        if let Some(remote_path) = request.remote_path {
            config.remote_path = remote_path;
        }
        if let Some(watch_config) = request.watch_config {
            // 验证下载备份不能启用监听
            if config.direction == BackupDirection::Download && watch_config.enabled {
                return Err(anyhow!("下载备份不支持文件监听"));
            }
            config.watch_config = watch_config;
        }
        if let Some(poll_config) = request.poll_config {
            config.poll_config = poll_config;
        }
        if let Some(filter_config) = request.filter_config {
            config.filter_config = filter_config;
        }
        if let Some(enabled) = request.enabled {
            config.enabled = enabled;
        }

        // 更新冲突策略字段
        if let Some(upload_strategy) = request.upload_conflict_strategy {
            config.upload_conflict_strategy = Some(upload_strategy);
        }
        if let Some(download_strategy) = request.download_conflict_strategy {
            config.download_conflict_strategy = Some(download_strategy);
        }

        config.updated_at = Utc::now();

        let updated_config = config.clone();
        drop(config);

        // 保存配置
        self.save_configs().await?;

        // 重新启动服务
        if updated_config.enabled {
            self.start_config_services(&updated_config).await?;
        }

        tracing::info!("Updated backup config: {} ({})", updated_config.name, updated_config.id);
        Ok(updated_config)
    }

    /// 删除备份配置
    pub async fn delete_config(&self, id: &str) -> Result<()> {
        // 停止服务
        self.stop_config_services(id).await?;

        // 🔥 先取消 TaskController，停止正在运行的备份任务（扫描/传输）
        // 必须在 batch_delete 之前执行，否则 execute_backup_for_config 会继续创建新上传任务
        if let Some(controller) = self.task_controllers.get(id) {
            tracing::info!("取消配置 {} 的任务控制器", id);
            controller.cancel();
        }
        // 等待 task_loop 响应取消信号
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 删除所有关联的底层上传/下载任务
        tracing::info!("开始清理配置 {} 的所有关联任务", id);

        // 批量删除上传任务
        // 使用安全获取方法，处理 Weak 引用升级
        let upload_mgr = self.get_upload_manager();
        if let Some(ref upload_mgr) = upload_mgr {
            let upload_tasks = upload_mgr.get_tasks_by_backup_config(id).await;
            let task_ids: Vec<String> = upload_tasks.iter().map(|t| t.id.clone()).collect();
            tracing::info!("找到 {} 个关联的上传任务，批量删除", task_ids.len());
            if !task_ids.is_empty() {
                let (success, failed) = upload_mgr.batch_delete_tasks(&task_ids).await;
                tracing::info!("批量删除上传任务完成: 成功={}, 失败={}", success, failed);
            }
        }

        // 批量删除下载任务（如果是下载备份配置）
        // 使用安全获取方法，处理 Weak 引用升级
        let download_mgr = self.get_download_manager();
        if let Some(ref download_mgr) = download_mgr {
            let download_tasks = download_mgr.get_tasks_by_backup_config(id).await;
            let task_ids: Vec<String> = download_tasks.iter().map(|t| t.id.clone()).collect();
            tracing::info!("找到 {} 个关联的下载任务，批量删除", task_ids.len());
            if !task_ids.is_empty() {
                let (success, failed) = download_mgr.batch_delete_tasks(&task_ids, false).await;
                tracing::info!("批量删除下载任务完成: 成功={}, 失败={}", success, failed);
            }
        }

        // 删除所有备份任务（从内存和数据库）
        let task_ids: Vec<String> = self.tasks.iter()
            .filter(|t| t.config_id == id)
            .map(|t| t.id.clone())
            .collect();

        tracing::info!("找到 {} 个关联的备份任务", task_ids.len());
        for task_id in task_ids {
            self.tasks.remove(&task_id);
            if let Err(e) = self.persistence_manager.delete_task(&task_id) {
                tracing::warn!("从数据库删除备份任务 {} 失败: {}", task_id, e);
            } else {
                tracing::debug!("已删除备份任务: {}", task_id);
            }
        }

        // 🔥 内存优化：删除配置对应的任务控制器
        if self.task_controllers.remove(id).is_some() {
            tracing::info!("已删除配置 {} 的任务控制器", id);
        }

        // 删除配置
        let config = self.configs.remove(id)
            .ok_or_else(|| anyhow!("配置不存在: {}", id))?;

        // 删除相关记录
        self.record_manager.delete_upload_records_by_config(id)?;

        // 删除扫描缓存
        if let Err(e) = self.scan_cache_manager.delete_by_config(id) {
            tracing::warn!("删除配置 {} 的扫描缓存失败: {}", id, e);
        }

        // 删除加密映射表数据
        if let Err(e) = self.record_manager.delete_snapshots_by_config(id) {
            tracing::warn!("删除配置 {} 的加密快照记录失败: {}", id, e);
        }

        // 保存配置
        self.save_configs().await?;

        tracing::info!("Deleted backup config: {} ({})", config.1.name, id);

        // 如果没有配置了，停止全局监听器和调度器
        self.cleanup_idle_services().await;

        Ok(())
    }

    /// 清理空闲的全局服务
    ///
    /// 当没有任何配置需要监听或轮询时，停止对应的全局服务以释放资源
    async fn cleanup_idle_services(&self) {
        // 检查是否还有需要文件监听的配置
        let has_watch_configs = self.configs.iter().any(|c| {
            c.enabled && c.direction == BackupDirection::Upload && c.watch_config.enabled
        });

        if !has_watch_configs {
            let mut watcher_guard = self.file_watcher.write();
            if watcher_guard.is_some() {
                *watcher_guard = None;
                tracing::info!("没有需要监听的配置，已停止文件监听器");
            }
        }

        // 检查是否还有需要轮询的配置
        let has_poll_configs = self.configs.iter().any(|c| {
            c.enabled && (c.poll_config.enabled && c.poll_config.mode != PollMode::Disabled)
        });

        if !has_poll_configs {
            let mut scheduler_guard = self.poll_scheduler.write();
            if scheduler_guard.is_some() {
                *scheduler_guard = None;
                tracing::info!("没有需要轮询的配置，已停止轮询调度器");
            }
        }
    }

    /// 获取备份配置
    pub fn get_config(&self, id: &str) -> Option<BackupConfig> {
        self.configs.get(id).map(|c| c.clone())
    }

    /// 获取所有备份配置
    pub fn get_all_configs(&self) -> Vec<BackupConfig> {
        self.configs.iter().map(|c| c.clone()).collect()
    }

    /// 获取所有备份配置（别名，用于兼容）
    pub fn list_configs(&self) -> Result<Vec<BackupConfig>> {
        Ok(self.get_all_configs())
    }

    /// 获取记录管理器引用
    pub fn record_manager(&self) -> &Arc<BackupRecordManager> {
        &self.record_manager
    }

    /// 启动配置的服务（仅文件监听，轮询由全局轮询统一管理）
    async fn start_config_services(&self, config: &BackupConfig) -> Result<()> {
        // 启动文件监听（仅上传备份）
        if config.direction == BackupDirection::Upload && config.watch_config.enabled {
            let mut watcher_guard = self.file_watcher.write();
            if watcher_guard.is_none() {
                let (event_tx, mut event_rx) = mpsc::unbounded_channel::<FileChangeEvent>();
                let change_tx = self.event_tx.clone();

                // 转发事件
                tokio::spawn(async move {
                    while let Some(event) = event_rx.recv().await {
                        let change_event = ChangeEvent::WatchEvent {
                            config_id: event.config_id,
                            paths: event.paths,
                        };
                        if change_tx.send(change_event).is_err() {
                            break;
                        }
                    }
                });

                match FileWatcher::new(event_tx) {
                    Ok(watcher) => {
                        *watcher_guard = Some(watcher);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to create file watcher: {}", e);
                        // 注意：不再自动回退到轮询，轮询由全局轮询统一管理
                    }
                }
            }

            if let Some(ref mut watcher) = *watcher_guard {
                if let Err(e) = watcher.watch(&config.local_path, &config.id) {
                    tracing::warn!(
                        "Failed to watch path {:?}: {}",
                        config.local_path, e
                    );
                    // 注意：不再自动回退到轮询，轮询由全局轮询统一管理
                }
            }
        }

        // 注意：不再为单个配置创建轮询
        // 轮询由 update_trigger_config 创建的全局轮询统一管理

        Ok(())
    }

    /// 检查监听器状态并在需要时记录警告
    ///
    /// 当监听器失败次数超过阈值时，记录警告日志
    /// 注意：在全局轮询架构下，不需要为单个配置创建回退轮询
    /// 全局轮询会自动触发所有启用的上传配置
    pub async fn check_watcher_health(&self) -> Result<()> {
        let should_fallback = {
            let watcher_guard = self.file_watcher.read();
            if let Some(ref watcher) = *watcher_guard {
                watcher.should_fallback_to_poll()
            } else {
                false
            }
        };

        if should_fallback {
            tracing::warn!(
                "File watcher has too many failures. \
                 Global poll will handle backup triggers for affected configs."
            );

            // 获取受影响的配置数量用于日志
            let affected_count = self.configs.iter()
                .filter(|c| c.direction == BackupDirection::Upload && c.watch_config.enabled)
                .count();

            tracing::info!(
                "受影响的上传配置数: {}，将由全局轮询统一触发",
                affected_count
            );

            // 重置失败计数器
            if let Some(ref watcher) = *self.file_watcher.read() {
                watcher.reset_failure_count();
            }
        }

        Ok(())
    }

    /// 停止配置的服务
    async fn stop_config_services(&self, config_id: &str) -> Result<()> {
        // 停止文件监听
        if let Some(ref mut watcher) = *self.file_watcher.write() {
            watcher.unwatch_config(config_id)?;
        }

        // 注意：不再为单个配置停止轮询
        // 全局轮询由 update_trigger_config 统一管理
        // 配置禁用后，全局轮询事件处理时会自动过滤掉禁用的配置

        Ok(())
    }

    // ==================== 任务管理 ====================

    /// 手动触发备份
    pub async fn trigger_backup(&self, config_id: &str) -> Result<String> {
        let config = self.get_config(config_id)
            .ok_or_else(|| anyhow!("配置不存在: {}", config_id))?;

        // 🔥 冲突检测：检查是否有正在扫描中的任务（Preparing 状态）
        // 无论是手动触发还是自动触发的扫描，都不允许重复触发
        let is_scanning = self.tasks.iter()
            .any(|t| t.config_id == config_id && t.status == BackupTaskStatus::Preparing);

        if is_scanning {
            tracing::info!(
                "手动备份被拒绝：配置 {} 正在扫描中，请等待扫描完成后再试",
                config_id
            );
            return Err(anyhow!("该配置正在扫描中，请等待扫描完成后再试"));
        }

        // 🔥 冲突检测：检查是否有正在传输中的任务（Transferring 状态）
        let is_transferring = self.tasks.iter()
            .any(|t| t.config_id == config_id && t.status == BackupTaskStatus::Transferring);

        if is_transferring {
            tracing::info!(
                "手动备份被拒绝：配置 {} 正在传输中，请等待传输完成或暂停后再试",
                config_id
            );
            return Err(anyhow!("该配置正在传输中，请等待传输完成或暂停后再试"));
        }

        // 🔥 冲突校验：执行前再次校验，防止配置在创建后被其他配置覆盖
        // 场景：用户先创建配置 A，再创建配置 B（与 A 冲突），然后手动触发 A
        let existing_configs: Vec<BackupConfig> = self.configs.iter().map(|c| c.clone()).collect();
        let conflict_result = validate_for_execute(&config, &existing_configs);
        if conflict_result.has_conflict {
            return Err(anyhow!(conflict_result.error_message.unwrap_or_else(|| "配置冲突".to_string())));
        }

        let task_id = self.create_backup_task(&config, TriggerType::Manual).await?;
        Ok(task_id)
    }

    /// 创建备份任务
    ///
    /// 备份任务使用最低优先级（Priority::Backup），会在普通任务和子任务之后执行
    async fn create_backup_task(&self, config: &BackupConfig, trigger_type: TriggerType) -> Result<String> {
        use super::priority::{Priority, PriorityContext};

        // 步骤8: 同一配置仅允许一个活跃任务
        // 检查是否已有同 config_id 且状态为活跃的任务
        let has_active_task = self.tasks.iter().any(|t| {
            t.config_id == config.id && matches!(
                t.status,
                BackupTaskStatus::Queued | BackupTaskStatus::Preparing |
                BackupTaskStatus::Transferring | BackupTaskStatus::Paused
            )
        });

        if has_active_task {
            tracing::info!(
                "配置 {} 已有活跃任务在运行，跳过创建新任务 (trigger: {:?})",
                config.id, trigger_type
            );
            return Err(anyhow!("配置 {} 已有任务在运行", config.name));
        }

        let task_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        // 检查优先级：备份任务只有在没有高优先级任务等待时才能执行
        let context = PriorityContext {
            active_count: self.tasks.iter().filter(|t| {
                matches!(t.status, BackupTaskStatus::Preparing | BackupTaskStatus::Transferring)
            }).count(),
            waiting_count: self.tasks.iter().filter(|t| {
                matches!(t.status, BackupTaskStatus::Queued)
            }).count(),
            max_concurrent: 3, // 从配置读取
            active_normal_count: 0,
            active_subtask_count: 0,
            active_backup_count: self.tasks.iter().filter(|t| {
                matches!(t.status, BackupTaskStatus::Preparing | BackupTaskStatus::Transferring)
            }).count(),
        };

        // 使用优先级管理器检查是否可以获取槽位
        let can_start = self.priority_manager.can_acquire_slot(Priority::Backup, &context);

        let task = BackupTask {
            id: task_id.clone(),
            config_id: config.id.clone(),
            status: BackupTaskStatus::Queued,
            sub_phase: if can_start { None } else { Some(BackupSubPhase::WaitingSlot) },
            trigger_type,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 0,
            transferred_bytes: 0,
            total_bytes: 0,
            scan_progress: None,
            created_at: now,
            started_at: None,
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };

        // 保存到内存
        self.tasks.insert(task_id.clone(), task.clone());

        // 持久化到数据库
        if let Err(e) = self.persistence_manager.save_task(&task) {
            tracing::warn!("持久化备份任务失败: {}", e);
        }

        // 发送任务创建事件
        self.publish_task_created(&task, config);

        tracing::info!(
            "Created backup task: {} for config: {} (can_start: {}, priority: Backup)",
            task_id, config.id, can_start
        );

        // 如果可以启动，立即开始执行任务
        if can_start {
            let task_id_clone = task_id.clone();
            let config_clone = config.clone();
            let self_tasks = self.tasks.clone();
            let self_upload_manager = self.upload_manager.clone();
            let self_download_manager = self.download_manager.clone();
            let self_persistence_manager = self.persistence_manager.clone();
            let self_ws_manager = self.ws_manager.clone();
            let self_record_manager = self.record_manager.clone();
            let self_configs = self.configs.clone();
            let self_encryption_config_store = self.encryption_config_store.clone();
            let self_proxy_config = self.proxy_config.read().clone();
            let self_fallback_mgr = self.fallback_mgr.read().clone();

            // 在后台任务中执行备份，根据配置方向选择上传或下载
            tokio::spawn(async move {
                let result = match config_clone.direction {
                    BackupDirection::Upload => {
                        Self::execute_backup_task_internal(
                            task_id_clone.clone(),
                            config_clone,
                            self_tasks,
                            self_upload_manager,
                            self_persistence_manager,
                            self_ws_manager,
                            self_record_manager,
                            self_configs,
                            self_encryption_config_store,
                        ).await
                    }
                    BackupDirection::Download => {
                        Self::execute_download_backup_task_internal(
                            task_id_clone.clone(),
                            config_clone,
                            self_tasks,
                            self_download_manager,
                            self_persistence_manager,
                            self_ws_manager,
                            self_record_manager,
                            self_proxy_config,
                            self_fallback_mgr,
                        ).await
                    }
                };

                if let Err(e) = result {
                    tracing::error!("备份任务执行失败: task={}, error={}", task_id_clone, e);
                }
            });
        }

        Ok(task_id)
    }

    /// 执行备份任务（内部静态方法）
    async fn execute_backup_task_internal(
        task_id: String,
        config: BackupConfig,
        tasks: Arc<DashMap<String, BackupTask>>,
        upload_manager: Arc<RwLock<Option<Weak<UploadManager>>>>,
        persistence_manager: Arc<BackupPersistenceManager>,
        ws_manager: Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        record_manager: Arc<BackupRecordManager>,
        _configs: Arc<DashMap<String, BackupConfig>>,
        encryption_config_store: Arc<EncryptionConfigStore>,
    ) -> Result<()> {
        use crate::uploader::{BatchedScanIterator, ScanOptions, SCAN_BATCH_SIZE};

        tracing::info!("开始执行备份任务: task={}, config={}", task_id, config.id);

        // 🔥 获取当前密钥版本号（用于文件夹加密映射）
        let current_key_version = match encryption_config_store.get_current_key() {
            Ok(Some(key_info)) => key_info.key_version,
            Ok(None) => {
                tracing::warn!("execute_backup_task_internal: 未找到加密密钥，使用默认版本 1");
                1u32
            }
            Err(e) => {
                tracing::warn!("execute_backup_task_internal: 获取密钥版本失败: {}，使用默认版本 1", e);
                1u32
            }
        };

        // 更新任务状态为准备中
        if let Some(mut task) = tasks.get_mut(&task_id) {
            task.status = BackupTaskStatus::Preparing;
            task.started_at = Some(Utc::now());
        }

        // 发送状态变更事件
        Self::publish_status_changed_static(&ws_manager, &task_id, "queued", "preparing");

        // 🔥 优先复用已恢复的文件任务（重启续传关键：不要覆盖 related_task_id）
        // 注意：这里的 clone 是必要的，因为我们需要在循环中消费 file_tasks
        // 同时保持 pending_files 用于状态更新
        let mut file_tasks: Vec<BackupFileTask> = Vec::new();

        if let Some(task) = tasks.get(&task_id) {
            if !task.pending_files.is_empty() {
                file_tasks = task.pending_files.clone();

                tracing::info!(
            "检测到已恢复的文件任务，跳过扫描直接续传: task={}, files={}, bytes={}",
            task_id,
            file_tasks.len(),
                    file_tasks.iter().map(|f| f.file_size).sum::<u64>()
        );
            }
        }
        // 没有恢复数据才扫描目录
        if file_tasks.is_empty() {
            // 阶段 1：分批扫描目录（内存优化：每批最多 SCAN_BATCH_SIZE 个文件）
            tracing::info!("备份任务分批扫描目录: task={}, path={:?}, batch_size={}", task_id, config.local_path, SCAN_BATCH_SIZE);

            let scan_options = ScanOptions {
                follow_symlinks: false,
                max_file_size: if config.filter_config.max_file_size > 0 {
                    Some(config.filter_config.max_file_size)
                } else {
                    None
                },
                max_files: None,
                skip_hidden: true,
                allowed_paths: vec![],
            };

            let mut scanner = match BatchedScanIterator::new(&config.local_path, scan_options) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("创建分批扫描器失败: task={}, error={}", task_id, e);
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.status = BackupTaskStatus::Failed;
                        task.error_message = Some(format!("扫描目录失败: {}", e));
                        task.completed_at = Some(Utc::now());
                    }
                    return Err(anyhow!("扫描目录失败: {}", e));
                }
            };

            // 应用过滤规则的配置
            let include_exts = &config.filter_config.include_extensions;
            let exclude_exts = &config.filter_config.exclude_extensions;
            let exclude_dirs = &config.filter_config.exclude_directories;
            let min_file_size = config.filter_config.min_file_size;

            let mut total_file_count = 0usize;
            let mut total_bytes = 0u64;
            let mut batch_number = 0usize;

            // 分批处理：扫描一批 → 过滤 → 持久化 → 继续
            while let Some(scanned_batch) = scanner.next_batch()? {
                batch_number += 1;
                let batch_size = scanned_batch.len();
                tracing::debug!("处理扫描批次 {}: {} 个文件", batch_number, batch_size);

                let mut batch_file_tasks = Vec::with_capacity(batch_size);

                for scanned_file in scanned_batch {
                    let file_ext = scanned_file.local_path.extension()
                        .and_then(|e| e.to_str())
                        .map(|e| e.to_lowercase())
                        .unwrap_or_default();

                    // 检查包含扩展名
                    if !include_exts.is_empty() && !include_exts.iter().any(|e| e.to_lowercase() == file_ext) {
                        continue;
                    }

                    // 检查排除扩展名
                    if exclude_exts.iter().any(|e| e.to_lowercase() == file_ext) {
                        continue;
                    }

                    // 检查排除目录
                    let relative_str = scanned_file.relative_path.to_string_lossy();
                    if exclude_dirs.iter().any(|d| relative_str.contains(d)) {
                        continue;
                    }

                    // 检查最小文件大小
                    if scanned_file.size < min_file_size {
                        continue;
                    }

                    // ========== 去重检查（在扫描阶段进行）==========
                    let file_name = scanned_file.local_path.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown")
                        .to_string();

                    let relative_path = scanned_file.local_path.strip_prefix(&config.local_path)
                        .map(|p| p.parent().unwrap_or(std::path::Path::new("")).to_string_lossy().to_string())
                        .unwrap_or_default();

                    let head_md5 = match calculate_head_md5(&scanned_file.local_path) {
                        Ok(md5) => md5,
                        Err(e) => {
                            tracing::warn!("计算文件头MD5失败，跳过去重检查: {:?}, error={}", scanned_file.local_path, e);
                            "unknown".to_string()
                        }
                    };

                    let (exists, _) = match record_manager.check_upload_record_preliminary(
                        &config.id,
                        &relative_path,
                        &file_name,
                        scanned_file.size,
                        &head_md5,
                    ) {
                        Ok(result) => result,
                        Err(e) => {
                            tracing::warn!("查询去重记录失败，继续添加任务: {:?}, error={}", scanned_file.local_path, e);
                            (false, None)
                        }
                    };

                    if exists {
                        tracing::debug!(
                            "文件已备份，跳过: {} (config={}, size={}, md5={})",
                            file_name, config.id, scanned_file.size, head_md5
                        );
                        continue;
                    }
                    // ========== 去重检查结束 ==========

                    // 计算远程路径
                    let remote_path = if config.encrypt_enabled {
                        match Self::encrypt_folder_path_static(
                            &record_manager,
                            &config.remote_path,
                            &scanned_file.relative_path.to_string_lossy(),
                            current_key_version,
                        ) {
                            Ok(path) => path,
                            Err(e) => {
                                tracing::warn!("加密文件夹路径失败，使用原始路径: {}", e);
                                format!("{}/{}",
                                        config.remote_path.trim_end_matches('/'),
                                        scanned_file.relative_path.to_string_lossy().replace('\\', "/"))
                            }
                        }
                    } else {
                        format!("{}/{}",
                                config.remote_path.trim_end_matches('/'),
                                scanned_file.relative_path.to_string_lossy().replace('\\', "/"))
                    };

                    let file_task = BackupFileTask {
                        id: Uuid::new_v4().to_string(),
                        parent_task_id: task_id.clone(),
                        local_path: scanned_file.local_path.clone(),
                        remote_path,
                        file_size: scanned_file.size,
                        head_md5: Some(head_md5),
                        fs_id: None,
                        status: BackupFileStatus::Pending,
                        sub_phase: None,
                        skip_reason: None,
                        encrypted: config.encrypt_enabled,
                        encrypted_name: None,
                        temp_encrypted_path: None,
                        transferred_bytes: 0,
                        decrypt_progress: None,
                        error_message: None,
                        retry_count: 0,
                        related_task_id: None,
                        backup_operation_type: Some(BackupOperationType::Upload),
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                    };

                    total_bytes += scanned_file.size;
                    batch_file_tasks.push(file_task);
                }

                // 🔥 内存优化：每批处理完立即持久化到数据库
                if !batch_file_tasks.is_empty() {
                    let batch_count = batch_file_tasks.len();
                    if let Err(e) = persistence_manager.save_file_tasks_batch(&batch_file_tasks, &config.id) {
                        tracing::warn!("批量保存文件任务到DB失败: batch={}, error={}", batch_number, e);
                    } else {
                        tracing::debug!("批次 {} 文件任务已持久化: count={}", batch_number, batch_count);
                    }

                    total_file_count += batch_count;
                    // 🔥 内存优化：使用 extend 而不是 clone，直接移动所有权
                    file_tasks.extend(batch_file_tasks);
                }
            }

            tracing::info!(
                "备份任务分批扫描完成: task={}, batches={}, files={}, bytes={}",
                task_id, batch_number, total_file_count, total_bytes
            );

            // 更新任务统计信息
            if let Some(mut task) = tasks.get_mut(&task_id) {
                // 🔥 内存优化：使用 std::mem::take 避免 clone
                task.pending_files = std::mem::take(&mut file_tasks);
                task.total_count = total_file_count;
                task.total_bytes = total_bytes;
                task.status = BackupTaskStatus::Transferring;
            }

            // 持久化任务
            if let Some(task) = tasks.get(&task_id) {
                if let Err(e) = persistence_manager.save_task(&task) {
                    tracing::warn!("持久化备份任务失败: {}", e);
                }
                // 🔥 内存优化：重新获取 file_tasks 用于后续处理
                // 这里仍需要 clone，因为后续循环需要消费 file_tasks
                // 但我们已经通过 std::mem::take 避免了一次额外的 clone
                file_tasks = task.pending_files.clone();
            }
        }

        // 如果没有文件需要备份，直接完成
        if file_tasks.is_empty() {
            tracing::info!("备份任务无文件需要备份: task={}", task_id);
            if let Some(mut task) = tasks.get_mut(&task_id) {
                task.status = BackupTaskStatus::Completed;
                task.completed_at = Some(Utc::now());
            }
            Self::publish_status_changed_static(&ws_manager, &task_id, "preparing", "completed");
            // 发送任务完成事件
            if let Some(task) = tasks.get(&task_id) {
                Self::publish_task_completed_static(&ws_manager, &task);
            }
            return Ok(());
        }

        // 发送状态变更事件
        Self::publish_status_changed_static(&ws_manager, &task_id, "preparing", "transferring");

        // 阶段 2：执行上传
        // 使用 Weak 引用升级获取 Arc
        let upload_mgr = {
            let guard = upload_manager.read();
            guard.as_ref().and_then(|weak| weak.upgrade())
        };

        let upload_mgr = match upload_mgr {
            Some(mgr) => mgr,
            None => {
                tracing::error!("上传管理器未设置: task={}", task_id);
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    task.status = BackupTaskStatus::Failed;
                    task.error_message = Some("上传管理器未设置".to_string());
                    task.completed_at = Some(Utc::now());
                }
                return Err(anyhow!("上传管理器未设置"));
            }
        };

        // 批量创建和启动所有上传任务（立即返回，不等待）
        let mut created_count = 0;

        for file_task in file_tasks {
            // 检查任务是否被取消或暂停
            if let Some(task) = tasks.get(&task_id) {
                if matches!(task.status, BackupTaskStatus::Cancelled | BackupTaskStatus::Paused) {
                    tracing::info!("备份任务已取消或暂停: task={}", task_id);
                    break;
                }
            }

            // 创建上传任务
            let local_path = file_task.local_path.clone();
            let remote_path = file_task.remote_path.clone();
            let file_task_id = file_task.id.clone();

            // 🔥 能续传就续传：有 related_task_id 且 UploadManager 里存在该任务，就直接继续
            if let Some(ref upload_task_id) = file_task.related_task_id {
                if let Some(upload_task) = upload_mgr.get_task(upload_task_id).await {
                    tracing::info!(
                    "自动备份续传：复用上传任务继续上传: backup_task={}, file_task={}, upload_task={}, status={:?}, file={:?}",
                    task_id,
                    file_task_id,
                    upload_task_id,
                    upload_task.status,
                    local_path
                );

                    // 更新文件状态为传输中 + 补齐映射
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.status = BackupFileStatus::Transferring;
                            ft.updated_at = Utc::now();
                        }

                        task.pending_upload_task_ids.insert(upload_task_id.clone());
                        task.transfer_task_map
                            .insert(upload_task_id.clone(), file_task_id.clone());
                    }

                    // 启动/恢复上传任务
                    let resume_or_start = if matches!(upload_task.status, UploadTaskStatus::Paused) {
                        upload_mgr.resume_task(upload_task_id).await
                    } else {
                        upload_mgr.start_task(upload_task_id).await
                    };

                    if resume_or_start.is_ok() {
                        created_count += 1;
                        continue; // ✅ 已续传，跳过新建
                    }

                    tracing::warn!(
                        "续传失败，将回退为新建上传任务: backup_task={}, file_task={}, upload_task={}",
                        task_id,
                        file_task_id,
                        upload_task_id
                    );

                    // 🔥 【关键修复】续传失败时，删除旧的上传任务（清 WAL，避免下次又被恢复）
                    if let Err(e) = upload_mgr.delete_task(upload_task_id).await {
                        tracing::warn!(
                            "删除续传失败的旧上传任务失败: upload_task={}, error={}",
                            upload_task_id, e
                        );
                    } else {
                        tracing::info!(
                            "已删除续传失败的旧上传任务: upload_task={}",
                            upload_task_id
                        );
                    }

                    // 清空 file_task 的 related_task_id 和映射（新建成功后会覆盖）
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.pending_upload_task_ids.remove(upload_task_id);
                        task.transfer_task_map.remove(upload_task_id);

                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.related_task_id = None;
                            ft.updated_at = Utc::now();
                        }
                    }
                    // 继续执行下面的新建逻辑
                }
            }

            // 更新文件状态为等待传输（实际传输开始由 UploadManager 通知）
            if let Some(mut task) = tasks.get_mut(&task_id) {
                if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task.id) {
                    ft.status = BackupFileStatus::WaitingTransfer;
                    ft.updated_at = Utc::now();
                }
            }

            tracing::debug!("开始上传文件: {:?} -> {}", local_path, remote_path);

            // 获取上传冲突策略（如果未指定，使用 SmartDedup 默认值）
            let upload_strategy = config.upload_conflict_strategy
                .unwrap_or(crate::uploader::conflict::UploadConflictStrategy::SmartDedup);

            // 创建并启动上传任务
            match upload_mgr.create_backup_task(
                local_path.clone(),
                remote_path.clone(),
                config.id.clone(),
                config.encrypt_enabled,
                Some(task_id.clone()),
                Some(file_task_id.clone()),
                Some(upload_strategy), // 传递冲突策略
            ).await {
                Ok(upload_task_id) => {
                    tracing::debug!("备份上传任务已创建: upload_task={}, file={:?}", upload_task_id, local_path);

                    // 启动上传任务
                    if let Err(e) = upload_mgr.start_task(&upload_task_id).await {
                        tracing::error!("启动备份上传任务失败: upload_task={}, error={}", upload_task_id, e);
                        // 更新文件任务状态为失败
                        if let Some(mut task) = tasks.get_mut(&task_id) {
                            task.failed_count += 1;
                            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                ft.status = BackupFileStatus::Failed;
                                ft.error_message = Some(format!("启动上传任务失败: {}", e));
                                ft.updated_at = Utc::now();
                            }
                        }
                        continue;
                    }

                    // 🔥 记录上传任务ID到备份任务和文件任务（供监听器和恢复逻辑使用）
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.pending_upload_task_ids.insert(upload_task_id.clone());
                        task.transfer_task_map.insert(upload_task_id.clone(), file_task_id.clone());

                        // 更新文件任务的 related_task_id 和 backup_operation_type
                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.related_task_id = Some(upload_task_id.clone());
                            ft.backup_operation_type = Some(super::task::BackupOperationType::Upload);
                            ft.updated_at = Utc::now();

                            // 持久化到数据库（关键：服务重启后可恢复）
                            if let Err(e) = persistence_manager.save_file_task(ft, &config.id) {
                                tracing::warn!("持久化文件任务失败: {}", e);
                            }
                        }
                    }

                    created_count += 1;
                }
                Err(e) => {
                    tracing::error!("创建上传任务失败: file={:?}, error={}", local_path, e);
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.failed_count += 1;
                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.status = BackupFileStatus::Failed;
                            ft.error_message = Some(format!("创建上传任务失败: {}", e));
                            ft.updated_at = Utc::now();
                        }
                    }
                }
            }
        }

        tracing::info!("已创建并启动 {} 个上传任务，等待监听器处理完成事件", created_count);

        // 🔥 立即返回，不等待上传完成（由全局监听器处理完成事件）
        // 如果没有需要上传的文件，直接标记为完成
        if created_count == 0 {
            if let Some(mut task) = tasks.get_mut(&task_id) {
                if task.status != BackupTaskStatus::Cancelled && task.status != BackupTaskStatus::Paused {
                    if task.failed_count > 0 {
                        task.status = BackupTaskStatus::PartiallyCompleted;
                    } else {
                        task.status = BackupTaskStatus::Completed;
                    }
                    task.completed_at = Some(Utc::now());
                }
            }

            // 持久化最终状态
            if let Some(task) = tasks.get(&task_id) {
                if let Err(e) = persistence_manager.save_task(&task) {
                    tracing::warn!("持久化备份任务失败: {}", e);
                }

                let final_status = format!("{:?}", task.status).to_lowercase();
                Self::publish_status_changed_static(&ws_manager, &task_id, "transferring", &final_status);

                // 发送任务完成/失败事件
                match task.status {
                    BackupTaskStatus::Completed | BackupTaskStatus::PartiallyCompleted => {
                        Self::publish_task_completed_static(&ws_manager, &task);
                    }
                    BackupTaskStatus::Failed => {
                        let error_msg = task.error_message.clone().unwrap_or_else(|| "所有文件传输失败".to_string());
                        Self::publish_task_failed_static(&ws_manager, &task_id, &error_msg);
                    }
                    _ => {}
                }

                tracing::info!(
                    "备份任务完成（无需上传）: task={}, status={:?}, completed={}, failed={}, skipped={}",
                    task_id, task.status, task.completed_count, task.failed_count, task.skipped_count
                );
            }

            // 🔥 内存优化：任务进入终态后从 DashMap 移除
            Self::cleanup_completed_task_static(&tasks, &task_id);
        }

        Ok(())
    }

    /// 执行下载备份任务（内部静态方法）
    ///
    /// 扫描远程目录，下载文件到本地
    async fn execute_download_backup_task_internal(
        task_id: String,
        config: BackupConfig,
        tasks: Arc<DashMap<String, BackupTask>>,
        download_manager: Arc<RwLock<Option<Weak<DownloadManager>>>>,
        persistence_manager: Arc<BackupPersistenceManager>,
        ws_manager: Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        record_manager: Arc<BackupRecordManager>,
        proxy_config: Option<ProxyConfig>,
        fallback_mgr: Option<Arc<ProxyFallbackManager>>,
    ) -> Result<()> {
        use crate::auth::SessionManager;

        tracing::info!("开始执行下载备份任务: task={}, config={}", task_id, config.id);

        // 更新任务状态为准备中
        if let Some(mut task) = tasks.get_mut(&task_id) {
            task.status = BackupTaskStatus::Preparing;
            task.started_at = Some(Utc::now());
        }

        // 发送状态变更事件
        Self::publish_status_changed_static(&ws_manager, &task_id, "queued", "preparing");

        // 获取下载管理器
        // 使用 Weak 引用升级获取 Arc
        let download_mgr = {
            let guard = download_manager.read();
            guard.as_ref().and_then(|weak| weak.upgrade())
        };

        let download_mgr = match download_mgr {
            Some(mgr) => mgr,
            None => {
                tracing::error!("下载管理器未设置: task={}", task_id);
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    task.status = BackupTaskStatus::Failed;
                    task.error_message = Some("下载管理器未设置".to_string());
                    task.completed_at = Some(Utc::now());
                }
                return Err(anyhow!("下载管理器未设置"));
            }
        };

        // 🔥 【关键修复】检查是否有已恢复的文件任务（断点续传）
        // 注意：这里的 clone 是必要的，因为我们需要在循环中消费 restored_file_tasks
        // 同时保持 pending_files 用于状态更新
        let restored_file_tasks: Vec<BackupFileTask> = {
            if let Some(task) = tasks.get(&task_id) {
                // 检查 pending_files 是否非空且有 related_task_id
                let has_restored_tasks = !task.pending_files.is_empty()
                    && task.pending_files.iter().any(|ft| ft.related_task_id.is_some());

                if has_restored_tasks {
                    tracing::info!(
                        "检测到已恢复的下载文件任务，跳过扫描直接续传: task={}, files={}, with_related_id={}",
                        task_id,
                        task.pending_files.len(),
                        task.pending_files.iter().filter(|ft| ft.related_task_id.is_some()).count()
                    );
                    task.pending_files.clone()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };

        // 如果有已恢复的文件任务，跳过扫描阶段，直接进入传输阶段（续传）
        if !restored_file_tasks.is_empty() {
            // 更新任务状态为传输中
            if let Some(mut task) = tasks.get_mut(&task_id) {
                task.status = BackupTaskStatus::Transferring;
            }
            Self::publish_status_changed_static(&ws_manager, &task_id, "preparing", "transferring");

            let mut created_count = 0;

            for file_task in restored_file_tasks {
                // 检查任务是否被取消或暂停
                if let Some(task) = tasks.get(&task_id) {
                    if matches!(task.status, BackupTaskStatus::Cancelled | BackupTaskStatus::Paused) {
                        tracing::info!("下载备份任务已取消或暂停: task={}", task_id);
                        break;
                    }
                }

                let file_task_id = file_task.id.clone();
                let local_path = file_task.local_path.clone();
                let remote_path = file_task.remote_path.clone();

                // 🔥 续传逻辑：有 related_task_id 且下载任务存在，就直接继续
                if let Some(ref download_task_id) = file_task.related_task_id {
                    if let Some(download_task) = download_mgr.get_task(download_task_id).await {
                        tracing::info!(
                            "自动备份续传：复用下载任务继续下载: backup_task={}, file_task={}, download_task={}, status={:?}, file={:?}",
                            task_id,
                            file_task_id,
                            download_task_id,
                            download_task.status,
                            local_path
                        );

                        // 更新文件状态为传输中 + 补齐映射
                        if let Some(mut task) = tasks.get_mut(&task_id) {
                            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                ft.status = BackupFileStatus::Transferring;
                                ft.updated_at = Utc::now();
                            }
                            task.pending_download_task_ids.insert(download_task_id.clone());
                            task.transfer_task_map.insert(download_task_id.clone(), file_task_id.clone());
                        }

                        // 启动/恢复下载任务
                        use crate::downloader::task::TaskStatus as DownloadTaskStatus;
                        let resume_or_start = if matches!(download_task.status, DownloadTaskStatus::Paused) {
                            download_mgr.resume_task(download_task_id).await
                        } else {
                            download_mgr.start_task(download_task_id).await
                        };

                        if resume_or_start.is_ok() {
                            created_count += 1;
                            continue; // ✅ 已续传，跳过
                        }

                        tracing::warn!(
                            "下载续传失败，将回退为新建下载任务: backup_task={}, file_task={}, download_task={}",
                            task_id,
                            file_task_id,
                            download_task_id
                        );

                        // 🔥 【关键修复】续传失败时，删除旧的下载任务
                        if let Err(e) = download_mgr.delete_task(download_task_id, false).await {
                            tracing::warn!(
                                "删除续传失败的旧下载任务失败: download_task={}, error={}",
                                download_task_id, e
                            );
                        } else {
                            tracing::info!(
                                "已删除续传失败的旧下载任务: download_task={}",
                                download_task_id
                            );
                        }

                        // 清空映射
                        if let Some(mut task) = tasks.get_mut(&task_id) {
                            task.pending_download_task_ids.remove(download_task_id);
                            task.transfer_task_map.remove(download_task_id);
                            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                ft.related_task_id = None;
                                ft.updated_at = Utc::now();
                            }
                        }

                        // 尝试使用旧任务的 fs_id 创建新任务
                        let fs_id = download_task.fs_id;

                        // 获取下载冲突策略（如果未指定，使用 Overwrite 默认值）
                        let download_strategy = config.download_conflict_strategy
                            .unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);

                        match download_mgr.create_backup_task(
                            fs_id,
                            remote_path.clone(),
                            local_path.clone(),
                            file_task.file_size,
                            config.id.clone(),
                            Some(download_strategy), // 传递冲突策略
                        ).await {
                            Ok(new_download_task_id) => {
                                // 检查是否为跳过标记
                                if new_download_task_id == "skipped" {
                                    tracing::info!("跳过备份下载（文件已存在）: file={}", remote_path);
                                    if let Some(mut task) = tasks.get_mut(&task_id) {
                                        task.skipped_count += 1;
                                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                            ft.status = BackupFileStatus::Skipped;
                                            ft.error_message = Some("文件已存在".to_string());
                                        }
                                    }
                                    continue;
                                }

                                if let Err(e) = download_mgr.start_task(&new_download_task_id).await {
                                    tracing::error!("启动新下载任务失败: {}", e);
                                    if let Some(mut task) = tasks.get_mut(&task_id) {
                                        task.failed_count += 1;
                                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                            ft.status = BackupFileStatus::Failed;
                                            ft.error_message = Some(format!("启动下载任务失败: {}", e));
                                        }
                                    }
                                    continue;
                                }

                                // 更新映射
                                if let Some(mut task) = tasks.get_mut(&task_id) {
                                    task.pending_download_task_ids.insert(new_download_task_id.clone());
                                    task.transfer_task_map.insert(new_download_task_id.clone(), file_task_id.clone());
                                    if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                        ft.related_task_id = Some(new_download_task_id.clone());
                                        ft.updated_at = Utc::now();
                                        if let Err(e) = persistence_manager.save_file_task(ft, &config.id) {
                                            tracing::warn!("持久化文件任务失败: {}", e);
                                        }
                                    }
                                }
                                created_count += 1;
                            }
                            Err(e) => {
                                tracing::error!("创建新下载任务失败: {}", e);
                                if let Some(mut task) = tasks.get_mut(&task_id) {
                                    task.failed_count += 1;
                                    if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                        ft.status = BackupFileStatus::Failed;
                                        ft.error_message = Some(format!("创建下载任务失败: {}", e));
                                    }
                                }
                            }
                        }
                        continue;
                    }
                }

                // 🔥 【关键修复】没有 related_task_id 或下载任务不存在时，尝试用 fs_id 重建下载任务
                if let Some(fs_id) = file_task.fs_id {
                    tracing::info!(
                        "下载任务不存在，使用 fs_id 重建下载任务: file_task={}, fs_id={}, remote_path={}",
                        file_task_id,
                        fs_id,
                        remote_path
                    );

                    // 使用 fs_id 创建新的下载任务
                    // 获取下载冲突策略（如果未指定，使用 Overwrite 默认值）
                    let download_strategy = config.download_conflict_strategy
                        .unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);

                    match download_mgr.create_backup_task(
                        fs_id,
                        remote_path.clone(),
                        local_path.clone(),
                        file_task.file_size,
                        config.id.clone(),
                        Some(download_strategy), // 传递冲突策略
                    ).await {
                        Ok(new_download_task_id) => {
                            // 检查是否为跳过标记
                            if new_download_task_id == "skipped" {
                                tracing::info!("跳过备份下载（文件已存在）: file={}", remote_path);
                                if let Some(mut task) = tasks.get_mut(&task_id) {
                                    task.skipped_count += 1;
                                    if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                        ft.status = BackupFileStatus::Skipped;
                                        ft.error_message = Some("文件已存在".to_string());
                                        ft.updated_at = Utc::now();
                                    }
                                }
                                continue;
                            }

                            // 启动下载任务
                            if let Err(e) = download_mgr.start_task(&new_download_task_id).await {
                                tracing::error!("启动重建的下载任务失败: download_task={}, error={}", new_download_task_id, e);
                                if let Some(mut task) = tasks.get_mut(&task_id) {
                                    task.failed_count += 1;
                                    if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                        ft.status = BackupFileStatus::Failed;
                                        ft.error_message = Some(format!("启动下载任务失败: {}", e));
                                        ft.updated_at = Utc::now();
                                    }
                                }
                                continue;
                            }

                            // 更新映射和状态
                            if let Some(mut task) = tasks.get_mut(&task_id) {
                                task.pending_download_task_ids.insert(new_download_task_id.clone());
                                task.transfer_task_map.insert(new_download_task_id.clone(), file_task_id.clone());

                                if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                    ft.related_task_id = Some(new_download_task_id.clone());
                                    ft.status = BackupFileStatus::Transferring;
                                    ft.updated_at = Utc::now();

                                    // 持久化更新后的 related_task_id
                                    if let Err(e) = persistence_manager.save_file_task(ft, &config.id) {
                                        tracing::warn!("持久化重建的文件任务失败: {}", e);
                                    }
                                }
                            }

                            tracing::info!(
                                "成功使用 fs_id 重建下载任务: file_task={}, new_download_task={}, fs_id={}",
                                file_task_id,
                                new_download_task_id,
                                fs_id
                            );
                            created_count += 1;
                            continue;
                        }
                        Err(e) => {
                            tracing::error!(
                                "使用 fs_id 重建下载任务失败: file_task={}, fs_id={}, error={}",
                                file_task_id,
                                fs_id,
                                e
                            );
                            if let Some(mut task) = tasks.get_mut(&task_id) {
                                task.failed_count += 1;
                                if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                    ft.status = BackupFileStatus::Failed;
                                    ft.error_message = Some(format!("重建下载任务失败: {}", e));
                                    ft.updated_at = Utc::now();
                                }
                            }
                            continue;
                        }
                    }
                }

                // fs_id 为空，按原逻辑处理（标记失败）
                tracing::warn!(
                    "恢复的文件任务缺少 fs_id，无法重建下载任务，标记为失败: file_task={}, related_task_id={:?}",
                    file_task_id,
                    file_task.related_task_id
                );
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    task.failed_count += 1;
                    if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                        ft.status = BackupFileStatus::Failed;
                        ft.error_message = Some("下载任务已丢失且缺少 fs_id，无法续传".to_string());
                        ft.updated_at = Utc::now();
                    }
                }
            }

            tracing::info!("已恢复并启动 {} 个下载任务（续传）", created_count);

            // 检查是否全部完成
            if created_count == 0 {
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    if task.status != BackupTaskStatus::Cancelled && task.status != BackupTaskStatus::Paused {
                        if task.failed_count > 0 {
                            task.status = BackupTaskStatus::PartiallyCompleted;
                        } else {
                            task.status = BackupTaskStatus::Completed;
                        }
                        task.completed_at = Some(Utc::now());
                    }
                }
                if let Some(task) = tasks.get(&task_id) {
                    if let Err(e) = persistence_manager.save_task(&task) {
                        tracing::warn!("持久化备份任务失败: {}", e);
                    }
                    let final_status = format!("{:?}", task.status).to_lowercase();
                    Self::publish_status_changed_static(&ws_manager, &task_id, "transferring", &final_status);

                    // 发送任务完成/失败事件
                    match task.status {
                        BackupTaskStatus::Completed | BackupTaskStatus::PartiallyCompleted => {
                            Self::publish_task_completed_static(&ws_manager, &task);
                        }
                        BackupTaskStatus::Failed => {
                            let error_msg = task.error_message.clone().unwrap_or_else(|| "所有文件传输失败".to_string());
                            Self::publish_task_failed_static(&ws_manager, &task_id, &error_msg);
                        }
                        _ => {}
                    }
                }

                // 🔥 内存优化：任务进入终态后从 DashMap 移除
                Self::cleanup_completed_task_static(&tasks, &task_id);
            }

            return Ok(());
        }

        // 创建网盘客户端用于获取文件列表
        let mut session_manager = SessionManager::new(None);
        let session = match session_manager.load_session().await {
            Ok(Some(s)) => s,
            Ok(None) => {
                tracing::error!("未登录，无法执行下载备份: task={}", task_id);
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    task.status = BackupTaskStatus::Failed;
                    task.error_message = Some("未登录".to_string());
                    task.completed_at = Some(Utc::now());
                }
                return Err(anyhow!("未登录"));
            }
            Err(e) => {
                tracing::error!("加载会话失败: task={}, error={}", task_id, e);
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    task.status = BackupTaskStatus::Failed;
                    task.error_message = Some(format!("加载会话失败: {}", e));
                    task.completed_at = Some(Utc::now());
                }
                return Err(anyhow!("加载会话失败: {}", e));
            }
        };

        let client = match crate::netdisk::NetdiskClient::new_with_proxy(session, proxy_config.as_ref(), fallback_mgr) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("创建网盘客户端失败: task={}, error={}", task_id, e);
                if let Some(mut task) = tasks.get_mut(&task_id) {
                    task.status = BackupTaskStatus::Failed;
                    task.error_message = Some(format!("创建网盘客户端失败: {}", e));
                    task.completed_at = Some(Utc::now());
                }
                return Err(anyhow!("创建网盘客户端失败: {}", e));
            }
        };

        // 🔥 内存优化：分批扫描远程目录（每批最多 DOWNLOAD_SCAN_BATCH_SIZE 个文件）
        const DOWNLOAD_SCAN_BATCH_SIZE: usize = 1000;

        tracing::info!(
            "下载备份任务分批扫描远程目录: task={}, path={}, batch_size={}",
            task_id, config.remote_path, DOWNLOAD_SCAN_BATCH_SIZE
        );

        let mut file_tasks: Vec<(BackupFileTask, u64)> = Vec::new();
        let mut total_bytes = 0u64;
        let mut total_file_count = 0usize;
        let mut batch_number = 0usize;
        let mut dirs_to_scan = vec![config.remote_path.clone()];

        // 当前批次的文件缓冲区
        let mut current_batch: Vec<(BackupFileTask, u64)> = Vec::with_capacity(DOWNLOAD_SCAN_BATCH_SIZE);

        // 递归扫描远程目录（分批处理）
        while let Some(current_dir) = dirs_to_scan.pop() {
            let mut page = 1;
            loop {
                match client.get_file_list(&current_dir, page, 1000).await {
                    Ok(response) => {
                        if response.errno != 0 {
                            tracing::warn!("获取文件列表失败: dir={}, errno={}", current_dir, response.errno);
                            break;
                        }

                        if response.list.is_empty() {
                            break;
                        }

                        for item in response.list {
                            if item.is_directory() {
                                // 添加子目录到扫描队列
                                dirs_to_scan.push(item.path.clone());
                            } else {
                                // 应用过滤规则
                                let file_ext = std::path::Path::new(&item.server_filename)
                                    .extension()
                                    .and_then(|e| e.to_str())
                                    .map(|e| e.to_lowercase())
                                    .unwrap_or_default();

                                // 检查包含扩展名
                                if !config.filter_config.include_extensions.is_empty()
                                    && !config.filter_config.include_extensions.iter().any(|e| e.to_lowercase() == file_ext)
                                {
                                    continue;
                                }

                                // 检查排除扩展名
                                if config.filter_config.exclude_extensions.iter().any(|e| e.to_lowercase() == file_ext) {
                                    continue;
                                }

                                // 检查文件大小
                                if item.size < config.filter_config.min_file_size {
                                    continue;
                                }
                                if config.filter_config.max_file_size > 0 && item.size > config.filter_config.max_file_size {
                                    continue;
                                }

                                // 计算本地保存路径（保持目录结构）
                                let relative_path = item.path
                                    .strip_prefix(&config.remote_path)
                                    .unwrap_or(&item.path)
                                    .trim_start_matches('/');

                                // 🔥 解密加密文件夹路径
                                let decrypted_relative_path = Self::decrypt_folder_path_static(
                                    &record_manager,
                                    &config.remote_path,
                                    &item.path,
                                ).unwrap_or_else(|_| relative_path.to_string());

                                let local_path = config.local_path.join(&decrypted_relative_path);

                                // 获取文件名用于去重检查
                                let file_name = local_path.file_name()
                                    .and_then(|n| n.to_str())
                                    .unwrap_or("unknown")
                                    .to_string();

                                // ========== 去重检查（在扫描阶段进行）==========
                                // 按设计文档 5.5.2 节要求：通过 remote_path + file_name + file_size + fs_id 判断
                                let exists = match record_manager.check_download_record(
                                    &config.id,
                                    &item.path,
                                    &file_name,
                                    item.size,
                                    &item.fs_id.to_string(),
                                ) {
                                    Ok(result) => result,
                                    Err(e) => {
                                        tracing::warn!("查询下载去重记录失败，继续下载: {}, error={}", item.path, e);
                                        false
                                    }
                                };

                                if exists {
                                    tracing::debug!(
                                        "文件已下载，跳过: {} (config={}, size={}, fs_id={})",
                                        file_name, config.id, item.size, item.fs_id
                                    );
                                    continue;
                                }
                                // ========== 去重检查结束 ==========

                                let file_task = BackupFileTask {
                                    id: Uuid::new_v4().to_string(),
                                    parent_task_id: task_id.clone(),
                                    local_path,
                                    remote_path: item.path.clone(),
                                    file_size: item.size,
                                    head_md5: None,  // 下载任务不需要本地head_md5
                                    fs_id: Some(item.fs_id),  // 🔥 持久化 fs_id，用于重启后重建下载任务
                                    status: BackupFileStatus::Pending,
                                    sub_phase: None,
                                    skip_reason: None,
                                    encrypted: config.encrypt_enabled,
                                    encrypted_name: None,
                                    temp_encrypted_path: None,
                                    transferred_bytes: 0,
                                    decrypt_progress: None,
                                    error_message: None,
                                    retry_count: 0,
                                    related_task_id: None,
                                    backup_operation_type: Some(BackupOperationType::Download),  // 🔥 创建时就设置类型，确保持久化正确
                                    created_at: Utc::now(),
                                    updated_at: Utc::now(),
                                };

                                total_bytes += item.size;
                                current_batch.push((file_task, item.fs_id));

                                // 🔥 内存优化：达到批次大小时立即持久化
                                if current_batch.len() >= DOWNLOAD_SCAN_BATCH_SIZE {
                                    batch_number += 1;
                                    let batch_count = current_batch.len();
                                    tracing::debug!("处理下载扫描批次 {}: {} 个文件", batch_number, batch_count);

                                    // 立即持久化当前批次
                                    let batch_file_tasks: Vec<_> = current_batch.iter().map(|(ft, _)| ft.clone()).collect();
                                    if let Err(e) = persistence_manager.save_file_tasks_batch(&batch_file_tasks, &config.id) {
                                        tracing::warn!("批量保存下载文件任务到DB失败: batch={}, error={}", batch_number, e);
                                    } else {
                                        tracing::debug!("下载批次 {} 文件任务已持久化: count={}", batch_number, batch_count);
                                    }

                                    total_file_count += batch_count;
                                    // 🔥 内存优化：使用 extend 而不是 clone，直接移动所有权
                                    file_tasks.extend(std::mem::take(&mut current_batch));
                                    current_batch = Vec::with_capacity(DOWNLOAD_SCAN_BATCH_SIZE);
                                }
                            }
                        }

                        page += 1;
                    }
                    Err(e) => {
                        tracing::error!("扫描远程目录失败: dir={}, error={}", current_dir, e);
                        break;
                    }
                }
            }
        }

        // 🔥 处理最后一个不完整的批次
        if !current_batch.is_empty() {
            batch_number += 1;
            let batch_count = current_batch.len();
            tracing::debug!("处理下载扫描最后批次 {}: {} 个文件", batch_number, batch_count);

            // 持久化最后一批
            let batch_file_tasks: Vec<_> = current_batch.iter().map(|(ft, _)| ft.clone()).collect();
            if let Err(e) = persistence_manager.save_file_tasks_batch(&batch_file_tasks, &config.id) {
                tracing::warn!("批量保存下载文件任务到DB失败: batch={}, error={}", batch_number, e);
            } else {
                tracing::debug!("下载批次 {} 文件任务已持久化: count={}", batch_number, batch_count);
            }

            total_file_count += batch_count;
            file_tasks.extend(current_batch);
        }

        let file_count = total_file_count;
        tracing::info!(
            "下载备份任务分批扫描完成: task={}, batches={}, files={}, bytes={}",
            task_id, batch_number, file_count, total_bytes
        );

        // 更新任务
        // 🔥 内存优化：使用 drain 和 map 避免额外的 clone
        let pending_files: Vec<BackupFileTask> = file_tasks.iter().map(|(ft, _)| ft.clone()).collect();
        if let Some(mut task) = tasks.get_mut(&task_id) {
            task.pending_files = pending_files;
            task.total_count = file_count;
            task.total_bytes = total_bytes;
            task.status = BackupTaskStatus::Transferring;
        }

        // 持久化任务
        if let Some(task) = tasks.get(&task_id) {
            if let Err(e) = persistence_manager.save_task(&task) {
                tracing::warn!("持久化备份任务失败: {}", e);
            }
        }

        // 如果没有文件需要下载，直接完成
        if file_count == 0 {
            tracing::info!("下载备份任务无文件需要下载: task={}", task_id);
            if let Some(mut task) = tasks.get_mut(&task_id) {
                task.status = BackupTaskStatus::Completed;
                task.completed_at = Some(Utc::now());
            }
            Self::publish_status_changed_static(&ws_manager, &task_id, "preparing", "completed");
            // 发送任务完成事件
            if let Some(task) = tasks.get(&task_id) {
                Self::publish_task_completed_static(&ws_manager, &task);
            }
            return Ok(());
        }

        // 发送状态变更事件
        Self::publish_status_changed_static(&ws_manager, &task_id, "preparing", "transferring");

        // 阶段 2：批量创建和启动所有下载任务（立即返回，不等待）
        let mut created_count = 0;

        for (file_task, fs_id) in file_tasks {
            // 检查任务是否被取消或暂停
            if let Some(task) = tasks.get(&task_id) {
                if matches!(task.status, BackupTaskStatus::Cancelled | BackupTaskStatus::Paused) {
                    tracing::info!("下载备份任务已取消或暂停: task={}", task_id);
                    break;
                }
            }

            let file_task_id = file_task.id.clone();
            let local_path = file_task.local_path.clone();
            let remote_path = file_task.remote_path.clone();
            let file_size = file_task.file_size;

            // 更新文件状态为等待传输（实际传输开始由 DownloadManager 通知）
            if let Some(mut task) = tasks.get_mut(&task_id) {
                if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    ft.status = BackupFileStatus::WaitingTransfer;
                    ft.updated_at = Utc::now();
                }
            }

            // 确保本地目录存在
            if let Some(parent) = local_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::error!("创建本地目录失败: {:?}, error={}", parent, e);
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.failed_count += 1;
                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.status = BackupFileStatus::Failed;
                            ft.error_message = Some(format!("创建目录失败: {}", e));
                            ft.updated_at = Utc::now();
                        }
                    }
                    continue;
                }
            }

            tracing::debug!("开始下载文件: {} -> {:?}", remote_path, local_path);

            // 获取下载冲突策略（如果未指定，使用 Overwrite 默认值）
            let download_strategy = config.download_conflict_strategy
                .unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);

            // 创建并启动备份下载任务
            match download_mgr.create_backup_task(
                fs_id,
                remote_path.clone(),
                local_path.clone(),
                file_size,
                config.id.clone(),
                Some(download_strategy), // 传递冲突策略
            ).await {
                Ok(download_task_id) => {
                    // 检查是否为跳过标记
                    if download_task_id == "skipped" {
                        tracing::info!("跳过备份下载（文件已存在）: file={}", remote_path);
                        if let Some(mut task) = tasks.get_mut(&task_id) {
                            task.skipped_count += 1;
                            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                ft.status = BackupFileStatus::Skipped;
                                ft.error_message = Some("文件已存在".to_string());
                                ft.updated_at = Utc::now();
                            }
                        }
                        continue;
                    }

                    tracing::debug!("备份下载任务已创建: download_task={}, file={}", download_task_id, remote_path);

                    // 启动下载任务
                    if let Err(e) = download_mgr.start_task(&download_task_id).await {
                        tracing::error!("启动备份下载任务失败: download_task={}, error={}", download_task_id, e);
                        if let Some(mut task) = tasks.get_mut(&task_id) {
                            task.failed_count += 1;
                            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                                ft.status = BackupFileStatus::Failed;
                                ft.error_message = Some(format!("启动下载任务失败: {}", e));
                                ft.updated_at = Utc::now();
                            }
                        }
                        continue;
                    }

                    // 🔥 记录下载任务ID到备份任务和文件任务（供监听器和恢复逻辑使用）
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.pending_download_task_ids.insert(download_task_id.clone());
                        task.transfer_task_map.insert(download_task_id.clone(), file_task_id.clone());

                        // 更新文件任务的 related_task_id 和 backup_operation_type
                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.related_task_id = Some(download_task_id.clone());
                            ft.backup_operation_type = Some(super::task::BackupOperationType::Download);
                            ft.updated_at = Utc::now();

                            // 持久化到数据库（关键：服务重启后可恢复）
                            if let Err(e) = persistence_manager.save_file_task(ft, &config.id) {
                                tracing::warn!("持久化文件任务失败: {}", e);
                            }
                        }
                    }

                    created_count += 1;
                }
                Err(e) => {
                    tracing::error!("创建下载任务失败: file={}, error={}", remote_path, e);
                    if let Some(mut task) = tasks.get_mut(&task_id) {
                        task.failed_count += 1;
                        if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                            ft.status = BackupFileStatus::Failed;
                            ft.error_message = Some(format!("创建下载任务失败: {}", e));
                            ft.updated_at = Utc::now();
                        }
                    }
                }
            }
        }

        tracing::info!("已创建并启动 {} 个下载任务，等待监听器处理完成事件", created_count);

        // 🔥 立即返回，不等待下载完成（由全局监听器处理完成事件）
        // 如果没有需要下载的文件，直接标记为完成
        if created_count == 0 {
            if let Some(mut task) = tasks.get_mut(&task_id) {
                if task.status != BackupTaskStatus::Cancelled && task.status != BackupTaskStatus::Paused {
                    if task.failed_count > 0 {
                        task.status = BackupTaskStatus::PartiallyCompleted;
                    } else {
                        task.status = BackupTaskStatus::Completed;
                    }
                    task.completed_at = Some(Utc::now());
                }
            }

            // 持久化最终状态
            if let Some(task) = tasks.get(&task_id) {
                if let Err(e) = persistence_manager.save_task(&task) {
                    tracing::warn!("持久化备份任务失败: {}", e);
                }

                let final_status = format!("{:?}", task.status).to_lowercase();
                Self::publish_status_changed_static(&ws_manager, &task_id, "transferring", &final_status);

                // 发送任务完成/失败事件
                match task.status {
                    BackupTaskStatus::Completed | BackupTaskStatus::PartiallyCompleted => {
                        Self::publish_task_completed_static(&ws_manager, &task);
                    }
                    BackupTaskStatus::Failed => {
                        let error_msg = task.error_message.clone().unwrap_or_else(|| "所有文件传输失败".to_string());
                        Self::publish_task_failed_static(&ws_manager, &task_id, &error_msg);
                    }
                    _ => {}
                }

                tracing::info!(
                    "下载备份任务完成（无需下载）: task={}, status={:?}, completed={}, failed={}, skipped={}",
                    task_id, task.status, task.completed_count, task.failed_count, task.skipped_count
                );
            }

            // 🔥 内存优化：任务进入终态后从 DashMap 移除
            Self::cleanup_completed_task_static(&tasks, &task_id);
        }

        Ok(())
    }

    /// 发送状态变更事件（静态方法）
    fn publish_status_changed_static(
        ws_manager: &Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        task_id: &str,
        old_status: &str,
        new_status: &str,
    ) {
        let ws = ws_manager.read();
        if let Some(ref weak) = *ws {
            if let Some(ws_mgr) = weak.upgrade() {
                ws_mgr.send_if_subscribed(
                    TaskEvent::Backup(WsBackupEvent::StatusChanged {
                        task_id: task_id.to_string(),
                        old_status: old_status.to_string(),
                        new_status: new_status.to_string(),
                    }),
                    None,
                );
            }
        }
    }

    /// 发送进度事件（静态方法）
    fn publish_progress_static(
        ws_manager: &Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        task: &BackupTask,
    ) {
        let ws = ws_manager.read();
        if let Some(ref weak) = *ws {
            if let Some(ws_mgr) = weak.upgrade() {
                ws_mgr.send_if_subscribed(
                    TaskEvent::Backup(WsBackupEvent::Progress {
                        task_id: task.id.clone(),
                        completed_count: task.completed_count,
                        failed_count: task.failed_count,
                        skipped_count: task.skipped_count,
                        total_count: task.total_count,
                        transferred_bytes: task.transferred_bytes,
                        total_bytes: task.total_bytes,
                    }),
                    None,
                );
            }
        }
    }

    /// 发送任务完成事件（静态方法）
    fn publish_task_completed_static(
        ws_manager: &Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        task: &BackupTask,
    ) {
        let ws = ws_manager.read();
        if let Some(ref weak) = *ws {
            if let Some(ws_mgr) = weak.upgrade() {
                ws_mgr.send_if_subscribed(
                    TaskEvent::Backup(WsBackupEvent::Completed {
                        task_id: task.id.clone(),
                        completed_at: task.completed_at.map(|t| t.timestamp()).unwrap_or_else(|| chrono::Utc::now().timestamp()),
                        success_count: task.completed_count,
                        failed_count: task.failed_count,
                        skipped_count: task.skipped_count,
                    }),
                    None,
                );
            }
        }
    }

    /// 发送任务失败事件（静态方法）
    fn publish_task_failed_static(
        ws_manager: &Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        task_id: &str,
        error: &str,
    ) {
        let ws = ws_manager.read();
        if let Some(ref weak) = *ws {
            if let Some(ws_mgr) = weak.upgrade() {
                ws_mgr.send_if_subscribed(
                    TaskEvent::Backup(WsBackupEvent::Failed {
                        task_id: task_id.to_string(),
                        error: error.to_string(),
                    }),
                    None,
                );
            }
        }
    }

    /// 发送文件进度事件（静态方法）
    ///
    /// 仅用于进度更新，不包含状态变更
    fn publish_file_progress_static(
        ws_manager: &Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        task_id: &str,
        file_task: &BackupFileTask,
    ) {
        let ws = ws_manager.read();
        if let Some(ref weak) = *ws {
            if let Some(ws_mgr) = weak.upgrade() {
                let file_name = file_task.local_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                let status = match file_task.status {
                    BackupFileStatus::Pending => "pending",
                    BackupFileStatus::Checking => "checking",
                    BackupFileStatus::Skipped => "skipped",
                    BackupFileStatus::Encrypting => "encrypting",
                    BackupFileStatus::WaitingTransfer => "waiting_transfer",
                    BackupFileStatus::Transferring => "transferring",
                    BackupFileStatus::Completed => "completed",
                    BackupFileStatus::Failed => "failed",
                };

                ws_mgr.send_if_subscribed(
                    TaskEvent::Backup(WsBackupEvent::FileProgress {
                        task_id: task_id.to_string(),
                        file_task_id: file_task.id.clone(),
                        file_name,
                        transferred_bytes: file_task.transferred_bytes,
                        total_bytes: file_task.file_size,
                        status: status.to_string(),
                    }),
                    None,
                );
            }
        }
    }

    /// 发送文件状态变更事件（静态方法）
    ///
    /// 当文件任务状态变更时调用，用于实时更新前端文件列表状态
    fn publish_file_status_changed_static(
        ws_manager: &Arc<RwLock<Option<Weak<WebSocketManager>>>>,
        task_id: &str,
        file_task_id: &str,
        file_name: &str,
        old_status: &str,
        new_status: &str,
    ) {
        let ws = ws_manager.read();
        if let Some(ref weak) = *ws {
            if let Some(ws_mgr) = weak.upgrade() {
                ws_mgr.send_if_subscribed(
                    TaskEvent::Backup(WsBackupEvent::FileStatusChanged {
                        task_id: task_id.to_string(),
                        file_task_id: file_task_id.to_string(),
                        file_name: file_name.to_string(),
                        old_status: old_status.to_string(),
                        new_status: new_status.to_string(),
                    }),
                    None,
                );
            }
        }
    }

    /// 获取任务（步骤6: DB + 内存合并查询）
    /// 先查内存，无则查 DB
    ///
    /// 注意：此方法包含同步数据库操作，在异步上下文中请使用 get_task_async
    pub fn get_task(&self, task_id: &str) -> Option<BackupTask> {
        // 先查内存（活跃任务）
        if let Some(task) = self.tasks.get(task_id) {
            return Some(task.clone());
        }

        // 内存无则查 DB（历史任务）
        match self.persistence_manager.load_task(task_id) {
            Ok(Some(task)) => Some(task),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("从 DB 加载任务失败: {}", e);
                None
            }
        }
    }

    /// 获取任务（异步版本，避免阻塞异步运行时）
    ///
    /// 在 API handler 等异步上下文中使用此方法
    pub async fn get_task_async(&self, task_id: &str) -> Option<BackupTask> {
        // 先查内存（活跃任务）- 无阻塞
        if let Some(task) = self.tasks.get(task_id) {
            return Some(task.clone());
        }

        // 内存无则查 DB（历史任务）- 使用 spawn_blocking 避免阻塞
        let persistence_manager = self.persistence_manager.clone();
        let task_id = task_id.to_string();

        match tokio::task::spawn_blocking(move || {
            persistence_manager.load_task(&task_id)
        }).await {
            Ok(Ok(Some(task))) => Some(task),
            Ok(Ok(None)) => None,
            Ok(Err(e)) => {
                tracing::warn!("从 DB 加载任务失败: {}", e);
                None
            }
            Err(e) => {
                tracing::warn!("spawn_blocking 执行失败: {}", e);
                None
            }
        }
    }

    /// 获取配置的所有任务（步骤6: DB + 内存合并查询）
    /// 先查 DB，再用内存中的活跃任务覆盖
    ///
    /// 注意：此方法包含同步数据库操作，在异步上下文中请使用 get_tasks_by_config_async
    pub fn get_tasks_by_config(&self, config_id: &str) -> Vec<BackupTask> {
        // 先从 DB 查询历史任务
        let mut db_tasks = match self.persistence_manager.get_tasks_by_config(config_id, 100, 0) {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!("从 DB 查询任务失败: {}", e);
                Vec::new()
            }
        };

        // 用内存中的活跃任务覆盖（轻量拷贝，跳过 pending_files 等大字段）
        for task_ref in self.tasks.iter() {
            if task_ref.config_id == config_id {
                let light = Self::lightweight_clone_task(&task_ref);
                if let Some(pos) = db_tasks.iter().position(|t| t.id == task_ref.id) {
                    db_tasks[pos] = light;
                } else {
                    db_tasks.insert(0, light);
                }
            }
        }

        db_tasks
    }

    /// 获取配置的所有任务（异步版本，避免阻塞异步运行时）
    ///
    /// 在 API handler 等异步上下文中使用此方法
    pub async fn get_tasks_by_config_async(&self, config_id: &str) -> Vec<BackupTask> {
        // 先从 DB 查询历史任务 - 使用 spawn_blocking 避免阻塞
        let persistence_manager = self.persistence_manager.clone();
        let config_id_owned = config_id.to_string();

        let mut db_tasks = match tokio::task::spawn_blocking(move || {
            persistence_manager.get_tasks_by_config(&config_id_owned, 100, 0)
        }).await {
            Ok(Ok(tasks)) => tasks,
            Ok(Err(e)) => {
                tracing::warn!("从 DB 查询任务失败: {}", e);
                Vec::new()
            }
            Err(e) => {
                tracing::warn!("spawn_blocking 执行失败: {}", e);
                Vec::new()
            }
        };

        // 用内存中的活跃任务覆盖（轻量拷贝，跳过 pending_files 等大字段）
        for task_ref in self.tasks.iter() {
            if task_ref.config_id == config_id {
                let light = Self::lightweight_clone_task(&task_ref);
                if let Some(pos) = db_tasks.iter().position(|t| t.id == task_ref.id) {
                    db_tasks[pos] = light;
                } else {
                    db_tasks.insert(0, light);
                }
            }
        }

        db_tasks
    }

    /// 获取配置的任务列表（分页）
    pub fn list_tasks_by_config(&self, config_id: &str, page: usize, page_size: usize) -> (Vec<BackupTask>, usize) {
        let offset = (page.saturating_sub(1)) * page_size;

        // 从 DB 查询
        let db_tasks = match self.persistence_manager.get_tasks_by_config(config_id, page_size, offset) {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!("从 DB 查询任务失败: {}", e);
                Vec::new()
            }
        };

        // 获取总数
        let total = match self.persistence_manager.count_tasks_by_config(config_id) {
            Ok(count) => count,
            Err(_) => db_tasks.len(),
        };

        // 用内存中的活跃任务覆盖（轻量拷贝，跳过 pending_files 等大字段）
        let mut result = db_tasks;
        for task_ref in self.tasks.iter() {
            if task_ref.config_id == config_id {
                if let Some(pos) = result.iter().position(|t| t.id == task_ref.id) {
                    result[pos] = Self::lightweight_clone_task(&task_ref);
                }
            }
        }

        (result, total)
    }

    /// 获取配置的任务列表（异步分页版本）
    pub async fn list_tasks_by_config_async(&self, config_id: &str, page: usize, page_size: usize) -> (Vec<BackupTask>, usize) {
        let persistence_manager = self.persistence_manager.clone();
        let config_id_owned = config_id.to_string();
        let offset = (page.saturating_sub(1)) * page_size;

        let (db_tasks, total) = match tokio::task::spawn_blocking(move || {
            let tasks = persistence_manager.get_tasks_by_config(&config_id_owned, page_size, offset)?;
            let total = persistence_manager.count_tasks_by_config(&config_id_owned)?;
            Ok::<_, anyhow::Error>((tasks, total))
        }).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                tracing::warn!("从 DB 查询任务失败: {}", e);
                (Vec::new(), 0)
            }
            Err(e) => {
                tracing::warn!("spawn_blocking 执行失败: {}", e);
                (Vec::new(), 0)
            }
        };

        // 用内存中的活跃任务覆盖（轻量拷贝，跳过 pending_files 等大字段）
        let mut result = db_tasks;
        for task_ref in self.tasks.iter() {
            if task_ref.config_id == config_id {
                if let Some(pos) = result.iter().position(|t| t.id == task_ref.id) {
                    result[pos] = Self::lightweight_clone_task(&task_ref);
                }
            }
        }

        (result, total)
    }
    /// 轻量拷贝 BackupTask，跳过 pending_files 等大字段（用于 API 响应）
    fn lightweight_clone_task(task: &BackupTask) -> BackupTask {
        BackupTask {
            id: task.id.clone(),
            config_id: task.config_id.clone(),
            status: task.status.clone(),
            sub_phase: task.sub_phase,
            trigger_type: task.trigger_type.clone(),
            pending_files: Vec::new(),
            completed_count: task.completed_count,
            failed_count: task.failed_count,
            skipped_count: task.skipped_count,
            total_count: task.total_count,
            transferred_bytes: task.transferred_bytes,
            total_bytes: task.total_bytes,
            scan_progress: None,
            created_at: task.created_at,
            started_at: task.started_at,
            completed_at: task.completed_at,
            error_message: task.error_message.clone(),
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        }
    }

    pub fn get_file_task(&self, task_id: &str, file_task_id: &str) -> Option<BackupFileTask> {
        self.tasks.get(task_id).and_then(|task| {
            task.pending_files.iter().find(|f| f.id == file_task_id).cloned()
        })
    }

    /// 更新文件任务传输进度
    pub fn update_file_task_progress(&self, task_id: &str, file_task_id: &str, transferred_bytes: u64) -> Result<()> {
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                let delta = transferred_bytes.saturating_sub(file_task.transferred_bytes);
                file_task.transferred_bytes = transferred_bytes;
                file_task.updated_at = Utc::now();
                task.transferred_bytes += delta;
                Ok(())
            } else {
                Err(anyhow!("文件任务不存在: {}", file_task_id))
            }
        } else {
            Err(anyhow!("任务不存在: {}", task_id))
        }
    }

    /// 批量重试失败的文件任务（步骤7: 仅限活跃任务）
    pub fn retry_failed_file_tasks(&self, task_id: &str) -> Result<usize> {
        // 步骤7: 操作接口限制为活跃任务
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            let mut retry_count = 0;
            for file_task in task.pending_files.iter_mut() {
                if file_task.status == BackupFileStatus::Failed {
                    file_task.status = BackupFileStatus::Pending;
                    file_task.error_message = None;
                    file_task.retry_count += 1;
                    file_task.updated_at = Utc::now();
                    retry_count += 1;
                }
            }

            if retry_count > 0 {
                // 重置任务状态
                task.status = BackupTaskStatus::Queued;
                task.failed_count = task.failed_count.saturating_sub(retry_count);
                task.completed_at = None;
            }

            tracing::info!("批量重试文件任务: task={}, count={}", task_id, retry_count);
            Ok(retry_count)
        } else {
            // 步骤7: 任务已完成或不存在，无法操作
            Err(anyhow!("任务已完成或不存在，无法操作: {}", task_id))
        }
    }

    /// 按状态筛选文件任务
    /// 🔥 内存优化：使用索引排序，只 clone 分页范围内的数据
    pub fn get_file_tasks_by_status(
        &self,
        task_id: &str,
        status: BackupFileStatus,
        page: usize,
        page_size: usize,
    ) -> Option<(Vec<BackupFileTask>, usize)> {
        self.tasks.get(task_id).map(|task| {
            // 🔥 内存优化：先收集匹配的索引，避免全量 clone
            let filtered_indices: Vec<usize> = task.pending_files.iter()
                .enumerate()
                .filter(|(_, f)| f.status == status)
                .map(|(i, _)| i)
                .collect();

            let total = filtered_indices.len();
            let start = (page - 1) * page_size;
            let end = std::cmp::min(start + page_size, total);

            if start >= total {
                (Vec::new(), total)
            } else {
                // 🔥 只 clone 分页范围内的数据
                let result: Vec<BackupFileTask> = filtered_indices[start..end]
                    .iter()
                    .map(|&i| task.pending_files[i].clone())
                    .collect();
                (result, total)
            }
        })
    }

    /// 获取文件状态的排序优先级
    #[inline]
    fn file_status_priority(status: &BackupFileStatus) -> u8 {
        match status {
            // 传输中最优先
            BackupFileStatus::Transferring => 0,
            // 加密中/检查中次优先
            BackupFileStatus::Encrypting => 1,
            BackupFileStatus::Checking => 2,
            // 等待传输/待处理
            BackupFileStatus::WaitingTransfer => 3,
            BackupFileStatus::Pending => 4,
            // 已完成/已跳过/失败最后
            BackupFileStatus::Completed => 5,
            BackupFileStatus::Skipped => 6,
            BackupFileStatus::Failed => 7,
        }
    }

    /// 获取任务的子任务列表（分页）（步骤6: DB + 内存合并查询）
    /// 内存有则返回 pending_files，内存无则查 DB
    /// 排序规则：传输中 > 加密中/检查中 > 等待传输/待处理 > 已完成/已跳过/失败
    /// 🔥 内存优化：使用索引排序，只 clone 分页范围内的数据
    pub fn get_file_tasks(&self, task_id: &str, page: usize, page_size: usize) -> Option<(Vec<BackupFileTask>, usize)> {
        // 先查内存（活跃任务）
        if let Some(task) = self.tasks.get(task_id) {
            let total = task.pending_files.len();

            // 🔥 内存优化：创建索引数组并按状态排序，避免全量 clone
            let mut indices: Vec<usize> = (0..total).collect();
            indices.sort_by_key(|&i| Self::file_status_priority(&task.pending_files[i].status));

            let start = (page - 1) * page_size;
            let end = std::cmp::min(start + page_size, total);

            if start >= total {
                return Some((Vec::new(), total));
            } else {
                // 🔥 只 clone 分页范围内的数据
                let result: Vec<BackupFileTask> = indices[start..end]
                    .iter()
                    .map(|&i| task.pending_files[i].clone())
                    .collect();
                return Some((result, total));
            }
        }

        // 内存无则查 DB（历史任务）
        match self.persistence_manager.load_file_tasks(task_id, page, page_size) {
            Ok((tasks, total)) => Some((tasks, total)),
            Err(e) => {
                tracing::warn!("从 DB 加载文件任务失败: {}", e);
                None
            }
        }
    }

    /// 获取任务的子任务列表（异步版本，避免阻塞异步运行时）
    ///
    /// 在 API handler 等异步上下文中使用此方法
    pub async fn get_file_tasks_async(&self, task_id: &str, page: usize, page_size: usize) -> Option<(Vec<BackupFileTask>, usize)> {
        // 先查内存（活跃任务）- 无阻塞
        if let Some(task) = self.tasks.get(task_id) {
            let total = task.pending_files.len();

            let mut indices: Vec<usize> = (0..total).collect();
            indices.sort_by_key(|&i| Self::file_status_priority(&task.pending_files[i].status));

            let start = (page - 1) * page_size;
            let end = std::cmp::min(start + page_size, total);

            if start >= total {
                return Some((Vec::new(), total));
            } else {
                let result: Vec<BackupFileTask> = indices[start..end]
                    .iter()
                    .map(|&i| task.pending_files[i].clone())
                    .collect();
                return Some((result, total));
            }
        }

        // 内存无则查 DB - 使用 spawn_blocking 避免阻塞
        let persistence_manager = self.persistence_manager.clone();
        let task_id_owned = task_id.to_string();

        match tokio::task::spawn_blocking(move || {
            persistence_manager.load_file_tasks(&task_id_owned, page, page_size)
        }).await {
            Ok(Ok((tasks, total))) => Some((tasks, total)),
            Ok(Err(e)) => {
                tracing::warn!("从 DB 加载文件任务失败: {}", e);
                None
            }
            Err(e) => {
                tracing::warn!("spawn_blocking 执行失败: {}", e);
                None
            }
        }
    }

    /// 重试单个文件任务（步骤7: 仅限活跃任务）
    pub async fn retry_file_task(&self, task_id: &str, file_task_id: &str) -> Result<()> {
        // 步骤7: 操作接口限制为活跃任务
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                match file_task.status {
                    BackupFileStatus::Failed | BackupFileStatus::Skipped => {
                        file_task.status = BackupFileStatus::Pending;
                        file_task.error_message = None;
                        file_task.retry_count += 1;
                        file_task.updated_at = Utc::now();
                        tracing::info!("Retry file task: {} in task: {}", file_task_id, task_id);
                        Ok(())
                    }
                    _ => Err(anyhow!("文件任务状态不允许重试: {:?}", file_task.status))
                }
            } else {
                Err(anyhow!("文件任务不存在: {}", file_task_id))
            }
        } else {
            // 步骤7: 任务已完成或不存在，无法操作
            Err(anyhow!("任务已完成或不存在，无法操作: {}", task_id))
        }
    }

    /// 取消任务（步骤7: 仅限活跃任务）
    ///
    /// 取消备份任务时，会同时取消所有关联的底层上传/下载任务，并清理未完成的加密映射表
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        // 步骤7: 操作接口限制为活跃任务
        // 先收集需要取消的底层任务ID和config_id，避免持有 DashMap 锁时调用 async 方法
        let (pending_uploads, pending_downloads, config_id) = {
            let task = self.tasks.get(task_id)
                .ok_or_else(|| anyhow!("任务已完成或不存在，无法操作: {}", task_id))?;

            match task.status {
                BackupTaskStatus::Queued | BackupTaskStatus::Preparing | BackupTaskStatus::Transferring | BackupTaskStatus::Paused => {
                    (
                        task.pending_upload_task_ids.iter().cloned().collect::<Vec<_>>(),
                        task.pending_download_task_ids.iter().cloned().collect::<Vec<_>>(),
                        task.config_id.clone(),
                    )
                }
                BackupTaskStatus::Completed | BackupTaskStatus::PartiallyCompleted | BackupTaskStatus::Cancelled | BackupTaskStatus::Failed => {
                    return Err(anyhow!("任务已结束，无法取消: {:?}", task.status));
                }
            }
        };

        // 使用安全获取方法，处理 Weak 引用升级
        let upload_manager = self.get_upload_manager();
        let download_manager = self.get_download_manager();

        // 批量删除所有关联的上传任务（避免逐个删除导致的 O(n²) 锁竞争）
        let mut deleted_uploads = 0;
        if let Some(ref upload_mgr) = upload_manager {
            if !pending_uploads.is_empty() {
                let (success, failed) = upload_mgr.batch_delete_tasks(&pending_uploads).await;
                deleted_uploads = success;
                if failed > 0 {
                    tracing::debug!(
                        "批量删除上传任务: backup_task={}, 成功={}, 失败={}",
                        task_id, success, failed
                    );
                }
            }
        }

        // 批量删除所有关联的下载任务
        let mut deleted_downloads = 0;
        if let Some(ref download_mgr) = download_manager {
            if !pending_downloads.is_empty() {
                let (success, failed) = download_mgr.batch_delete_tasks(&pending_downloads, false).await;
                deleted_downloads = success;
                if failed > 0 {
                    tracing::debug!(
                        "批量删除下载任务: backup_task={}, 成功={}, 失败={}",
                        task_id, success, failed
                    );
                }
            }
        }

        // 清理该配置下未完成的加密映射记录（直接从数据库删除，不依赖内存中的 encrypted_name）
        let deleted_snapshots = match self.record_manager.delete_incomplete_snapshots_by_config(&config_id) {
            Ok(count) => {
                if count > 0 {
                    tracing::debug!(
                        "已清理未完成的加密映射记录: backup_task={}, config_id={}, count={}",
                        task_id, config_id, count
                    );
                }
                count
            }
            Err(e) => {
                tracing::warn!(
                    "清理加密映射记录失败: backup_task={}, config_id={}, error={}",
                    task_id, config_id, e
                );
                0
            }
        };

        // 更新备份任务状态
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            let old_status = format!("{:?}", task.status);
            task.status = BackupTaskStatus::Cancelled;
            task.completed_at = Some(Utc::now());

            // 持久化到数据库
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::error!("持久化取消状态失败: task={}, error={}", task_id, e);
            }

            // 发送状态变更事件
            self.publish_status_changed(task_id, &old_status, "Cancelled");
        }

        // 🔥 内存优化：任务取消后从 DashMap 移除
        self.cleanup_completed_task(task_id);

        tracing::info!(
            "Cancelled backup task: {}, deleted uploads: {}, deleted downloads: {}, deleted snapshots: {}",
            task_id, deleted_uploads, deleted_downloads, deleted_snapshots
        );
        Ok(())
    }

    /// 删除任务
    ///
    /// 从内存和数据库中删除任务。只能删除已完成、已取消或已失败的任务。
    /// 注意：由于内存优化，已完成的任务可能已从内存清理，此时直接从 DB 删除
    /// 删除时会同时删除所有关联的底层上传/下载任务
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        // 先收集需要删除的子任务ID
        let (pending_uploads, pending_downloads) = {
            // 先尝试从内存获取
            if let Some(task) = self.tasks.get(task_id) {
                // 检查是否可删除（活跃任务不可删除）
                if !matches!(
                    task.status,
                    BackupTaskStatus::Completed
                        | BackupTaskStatus::Cancelled
                        | BackupTaskStatus::Failed
                        | BackupTaskStatus::PartiallyCompleted
                ) {
                    return Err(anyhow!("只能删除已完成、已取消或已失败的任务"));
                }
                (
                    task.pending_upload_task_ids.iter().cloned().collect::<Vec<_>>(),
                    task.pending_download_task_ids.iter().cloned().collect::<Vec<_>>(),
                )
            } else {
                // 内存中没有，尝试从 DB 加载
                match self.persistence_manager.load_task(task_id) {
                    Ok(Some(task)) => {
                        if !matches!(
                            task.status,
                            BackupTaskStatus::Completed
                                | BackupTaskStatus::Cancelled
                                | BackupTaskStatus::Failed
                                | BackupTaskStatus::PartiallyCompleted
                        ) {
                            return Err(anyhow!("只能删除已完成、已取消或已失败的任务"));
                        }
                        (
                            task.pending_upload_task_ids.iter().cloned().collect::<Vec<_>>(),
                            task.pending_download_task_ids.iter().cloned().collect::<Vec<_>>(),
                        )
                    }
                    Ok(None) => {
                        return Err(anyhow!("任务不存在: {}", task_id));
                    }
                    Err(e) => {
                        return Err(anyhow!("查询任务失败: {}", e));
                    }
                }
            }
        };

        // 使用安全获取方法，处理 Weak 引用升级
        let upload_manager = self.get_upload_manager();
        let download_manager = self.get_download_manager();

        // 删除所有关联的上传任务
        let mut deleted_uploads = 0;
        if let Some(ref upload_mgr) = upload_manager {
            for upload_task_id in &pending_uploads {
                if let Err(e) = upload_mgr.delete_task(upload_task_id).await {
                    tracing::debug!(
                        "删除上传子任务失败（可能已被清理）: backup_task={}, upload_task={}, error={}",
                        task_id, upload_task_id, e
                    );
                } else {
                    deleted_uploads += 1;
                }
            }
        }

        // 删除所有关联的下载任务
        let mut deleted_downloads = 0;
        if let Some(ref download_mgr) = download_manager {
            for download_task_id in &pending_downloads {
                if let Err(e) = download_mgr.delete_task(download_task_id, false).await {
                    tracing::debug!(
                        "删除下载子任务失败（可能已被清理）: backup_task={}, download_task={}, error={}",
                        task_id, download_task_id, e
                    );
                } else {
                    deleted_downloads += 1;
                }
            }
        }

        // 从内存中删除备份任务
        self.tasks.remove(task_id);

        // 从数据库中删除
        if let Err(e) = self.persistence_manager.delete_task(task_id) {
            tracing::warn!("从数据库删除任务失败: {}", e);
        }

        tracing::info!(
            "Deleted backup task: {}, deleted uploads: {}, deleted downloads: {}",
            task_id, deleted_uploads, deleted_downloads
        );
        Ok(())
    }

    /// 暂停任务（步骤7: 仅限活跃任务）
    ///
    /// 暂停备份任务时，会同时暂停所有关联的底层上传/下载任务
    pub async fn pause_task(&self, task_id: &str) -> Result<()> {
        // 步骤7: 操作接口限制为活跃任务
        // 先收集需要暂停的底层任务ID，避免持有 DashMap 锁时调用 async 方法
        let (pending_uploads, pending_downloads) = {
            let task = self.tasks.get(task_id)
                .ok_or_else(|| anyhow!("任务已完成或不存在，无法操作: {}", task_id))?;

            match task.status {
                BackupTaskStatus::Queued | BackupTaskStatus::Preparing | BackupTaskStatus::Transferring => {
                    (
                        task.pending_upload_task_ids.iter().cloned().collect::<Vec<_>>(),
                        task.pending_download_task_ids.iter().cloned().collect::<Vec<_>>(),
                    )
                }
                BackupTaskStatus::Paused => {
                    return Err(anyhow!("任务已经处于暂停状态"));
                }
                _ => {
                    return Err(anyhow!("任务状态不允许暂停: {:?}", task.status));
                }
            }
        };

        // 使用安全获取方法，处理 Weak 引用升级
        let upload_manager = self.get_upload_manager();
        let download_manager = self.get_download_manager();

        // 暂停所有关联的上传任务
        // skip_try_start_waiting = true，避免暂停一个任务后立即启动另一个等待任务
        let mut paused_uploading = 0;
        let mut paused_upload_waiting = 0;
        if let Some(ref upload_mgr) = upload_manager {
            for upload_task_id in &pending_uploads {
                if let Err(e) = upload_mgr.pause_task(upload_task_id, true).await {
                    // 暂停失败可能是因为任务在等待队列中（不是 Uploading 状态）
                    tracing::debug!(
                        "暂停上传任务失败（可能在等待队列中）: backup_task={}, upload_task={}, error={}",
                        task_id, upload_task_id, e
                    );
                } else {
                    paused_uploading += 1;
                    tracing::debug!(
                        "已暂停上传任务: backup_task={}, upload_task={}",
                        task_id, upload_task_id
                    );
                }
            }

            // 🔥 暂停等待队列中的上传任务
            paused_upload_waiting = upload_mgr.pause_waiting_tasks(&pending_uploads).await;
        }

        // 暂停所有关联的下载任务
        // skip_try_start_waiting = true，避免暂停一个任务后立即启动另一个等待任务
        let mut paused_downloading = 0;
        let mut paused_download_waiting = 0;
        if let Some(ref download_mgr) = download_manager {
            for download_task_id in &pending_downloads {
                if let Err(e) = download_mgr.pause_task(download_task_id, true).await {
                    // 暂停失败可能是因为任务在等待队列中（不是 Downloading 状态）
                    tracing::debug!(
                        "暂停下载任务失败（可能在等待队列中）: backup_task={}, download_task={}, error={}",
                        task_id, download_task_id, e
                    );
                } else {
                    paused_downloading += 1;
                    tracing::debug!(
                        "已暂停下载任务: backup_task={}, download_task={}",
                        task_id, download_task_id
                    );
                }
            }

            // 🔥 暂停等待队列中的下载任务
            paused_download_waiting = download_mgr.pause_waiting_tasks(&pending_downloads).await;
        }

        // 更新备份任务状态并持久化
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            let old_status = format!("{:?}", task.status);
            task.status = BackupTaskStatus::Paused;

            // 持久化到数据库
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::error!("持久化暂停状态失败: task={}, error={}", task_id, e);
            }

            // 发送状态变更事件
            self.publish_status_changed(task_id, &old_status, "Paused");
        }

        // 发送暂停事件
        self.publish_task_paused(task_id);

        tracing::info!(
            "Paused backup task: {}, uploads: {} transferring + {} waiting, downloads: {} transferring + {} waiting",
            task_id, paused_uploading, paused_upload_waiting, paused_downloading, paused_download_waiting
        );
        Ok(())
    }

    /// 恢复任务（步骤7: 仅限活跃任务）
    ///
    /// 恢复备份任务时，会同时恢复所有关联的底层上传/下载任务
    pub async fn resume_task(&self, task_id: &str) -> Result<()> {
        // 步骤7: 操作接口限制为活跃任务
        // 先收集需要恢复的底层任务ID，避免持有 DashMap 锁时调用 async 方法
        let (pending_uploads, pending_downloads) = {
            let task = self.tasks.get(task_id)
                .ok_or_else(|| anyhow!("任务已完成或不存在，无法操作: {}", task_id))?;

            match task.status {
                BackupTaskStatus::Paused => {
                    (
                        task.pending_upload_task_ids.iter().cloned().collect::<Vec<_>>(),
                        task.pending_download_task_ids.iter().cloned().collect::<Vec<_>>(),
                    )
                }
                _ => {
                    return Err(anyhow!("只有暂停状态的任务才能恢复: {:?}", task.status));
                }
            }
        };

        // 使用安全获取方法，处理 Weak 引用升级
        let upload_manager = self.get_upload_manager();
        let download_manager = self.get_download_manager();

        // 恢复所有关联的上传任务
        if let Some(ref upload_mgr) = upload_manager {
            for upload_task_id in &pending_uploads {
                if let Err(e) = upload_mgr.resume_task(upload_task_id).await {
                    tracing::warn!(
                        "恢复上传任务失败: backup_task={}, upload_task={}, error={}",
                        task_id, upload_task_id, e
                    );
                } else {
                    tracing::debug!(
                        "已恢复上传任务: backup_task={}, upload_task={}",
                        task_id, upload_task_id
                    );
                }
            }
        }

        // 恢复所有关联的下载任务
        if let Some(ref download_mgr) = download_manager {
            for download_task_id in &pending_downloads {
                if let Err(e) = download_mgr.resume_task(download_task_id).await {
                    tracing::warn!(
                        "恢复下载任务失败: backup_task={}, download_task={}, error={}",
                        task_id, download_task_id, e
                    );
                } else {
                    tracing::debug!(
                        "已恢复下载任务: backup_task={}, download_task={}",
                        task_id, download_task_id
                    );
                }
            }
        }

        // 更新备份任务状态为 Transferring（如果有正在传输的任务）或 Queued，并持久化
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            let old_status = format!("{:?}", task.status);

            // 如果有待处理的传输任务，设置为 Transferring，否则设置为 Queued
            if !pending_uploads.is_empty() || !pending_downloads.is_empty() {
                task.status = BackupTaskStatus::Transferring;
            } else {
                task.status = BackupTaskStatus::Queued;
            }

            let new_status = format!("{:?}", task.status);

            // 持久化到数据库
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::error!("持久化恢复状态失败: task={}, error={}", task_id, e);
            }

            // 发送状态变更事件
            self.publish_status_changed(task_id, &old_status, &new_status);
        }

        // 发送恢复事件
        self.publish_task_resumed(task_id);

        tracing::info!(
            "Resumed backup task: {}, resumed {} uploads and {} downloads",
            task_id, pending_uploads.len(), pending_downloads.len()
        );
        Ok(())
    }

    // ==================== 加密管理 ====================

    /// 配置加密密钥
    ///
    /// 配置后会自动持久化到 encryption.json 文件
    /// 如果已有密钥配置，会将当前密钥移到历史，保留历史密钥用于解密旧文件
    pub fn configure_encryption(&self, key_base64: &str, algorithm: EncryptionAlgorithm) -> Result<()> {
        let service = EncryptionService::from_base64_key(key_base64, algorithm)?;

        let mut encryption_service = self.encryption_service.write();
        *encryption_service = Some(service);

        // 使用安全方法持久化密钥到 encryption.json（保留历史密钥）
        let key_config = self.encryption_config_store.create_new_key_safe(
            key_base64.to_string(),
            algorithm,
        )?;

        let mut encryption_config = self.encryption_config.write();
        encryption_config.enabled = true;
        encryption_config.master_key = Some(key_base64.to_string());
        encryption_config.algorithm = algorithm;
        encryption_config.key_created_at = Some(Utc::now());
        // 同步 key_version 到内存配置，确保与持久化配置一致
        encryption_config.key_version = key_config.current.key_version;

        tracing::info!(
            "Encryption configured with algorithm: {:?}, key_version: {}, history_count: {}",
            algorithm,
            key_config.current.key_version,
            key_config.history.len()
        );
        Ok(())
    }

    /// 生成新密钥
    ///
    /// 生成后会自动持久化到 encryption.json 文件
    pub fn generate_encryption_key(&self, algorithm: EncryptionAlgorithm) -> Result<String> {
        let key_base64 = EncryptionService::generate_master_key_base64();
        self.configure_encryption(&key_base64, algorithm)?;
        Ok(key_base64)
    }

    /// 删除加密密钥
    ///
    /// 将当前密钥移到历史，保留历史密钥用于解密旧文件。
    /// 如果需要完全删除所有密钥（包括历史），使用 `force_delete_encryption_key`。
    ///
    /// # Requirements
    /// - 17.1: 删除密钥时保留历史密钥
    /// - 17.2: 只移除当前密钥，不删除历史
    pub fn delete_encryption_key(&self) -> Result<()> {
        let mut encryption_service = self.encryption_service.write();
        *encryption_service = None;

        let mut encryption_config = self.encryption_config.write();
        encryption_config.enabled = false;
        encryption_config.master_key = None;
        encryption_config.key_created_at = None;
        encryption_config.key_version = 0;

        // 废弃当前密钥而不是删除整个配置（保留历史密钥）
        self.encryption_config_store.deprecate_current_key()?;

        tracing::info!("Encryption key deprecated, history preserved for decryption");
        Ok(())
    }

    /// 强制删除所有加密密钥（包括历史）
    ///
    /// 警告：这将导致无法解密任何已加密的文件。
    /// 仅在用户明确要求完全清除所有密钥时使用。
    ///
    /// # Requirements
    /// - 17.3: 提供完全删除所有密钥（包括历史）的选项
    pub fn force_delete_encryption_key(&self) -> Result<()> {
        let mut encryption_service = self.encryption_service.write();
        *encryption_service = None;

        let mut encryption_config = self.encryption_config.write();
        encryption_config.enabled = false;
        encryption_config.master_key = None;
        encryption_config.key_created_at = None;
        encryption_config.key_version = 0;

        // 完全删除配置文件（包括历史密钥）
        self.encryption_config_store.force_delete()?;

        tracing::warn!("All encryption keys deleted including history - encrypted files cannot be decrypted");
        Ok(())
    }

    /// 获取加密配置状态
    pub fn get_encryption_status(&self) -> EncryptionStatus {
        let config = self.encryption_config.read();
        EncryptionStatus {
            enabled: config.enabled,
            has_key: config.master_key.is_some(),
            algorithm: config.algorithm,
            key_created_at: config.key_created_at,
        }
    }

    /// 获取加密配置状态（非阻塞版本）
    pub fn get_encryption_status_nonblocking(&self) -> EncryptionStatus {
        match self.encryption_config.try_read() {
            Some(config) => EncryptionStatus {
                enabled: config.enabled,
                has_key: config.master_key.is_some(),
                algorithm: config.algorithm,
                key_created_at: config.key_created_at,
            },
            None => EncryptionStatus {
                enabled: false,
                has_key: false,
                algorithm: super::config::EncryptionAlgorithm::Aes256Gcm,
                key_created_at: None,
            },
        }
    }

    /// 导出加密密钥
    pub fn export_encryption_key(&self) -> Result<String> {
        let config = self.encryption_config.read();
        config.master_key.clone()
            .ok_or_else(|| anyhow!("加密密钥未配置"))
    }

    // ==================== 去重服务 ====================

    /// 检查文件是否需要上传
    pub async fn check_dedup(&self, config_id: &str, file_path: &Path) -> Result<DedupResult> {
        let config = self.get_config(config_id)
            .ok_or_else(|| anyhow!("配置不存在: {}", config_id))?;

        let file_name = file_path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("无效的文件名"))?;

        let relative_path = file_path.strip_prefix(&config.local_path)
            .map(|p| p.parent().unwrap_or(Path::new("")).to_string_lossy().to_string())
            .unwrap_or_default();

        let metadata = std::fs::metadata(file_path)?;
        let file_size = metadata.len();

        // 计算文件头 MD5
        let head_md5 = calculate_head_md5(file_path)?;

        // 检查记录
        let (exists, stored_md5) = self.record_manager.check_upload_record_preliminary(
            config_id,
            &relative_path,
            file_name,
            file_size,
            &head_md5,
        )?;

        if exists {
            Ok(DedupResult {
                should_upload: false,
                reason: Some("文件已备份".to_string()),
                existing_md5: stored_md5,
            })
        } else {
            Ok(DedupResult {
                should_upload: true,
                reason: None,
                existing_md5: None,
            })
        }
    }

    // ==================== 配置持久化 ====================

    /// 保存配置到文件
    async fn save_configs(&self) -> Result<()> {
        let configs: Vec<BackupConfig> = self.configs.iter().map(|c| c.clone()).collect();
        let json = serde_json::to_string_pretty(&configs)?;

        if let Some(parent) = self.config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(&self.config_path, json)?;
        Ok(())
    }

    /// 从文件加载配置
    async fn load_configs(&self) -> Result<()> {
        if !self.config_path.exists() {
            return Ok(());
        }

        let json = std::fs::read_to_string(&self.config_path)?;
        let configs: Vec<BackupConfig> = serde_json::from_str(&json)?;

        for config in configs {
            let id = config.id.clone();
            self.configs.insert(id.clone(), config.clone());

            // 启动服务
            if config.enabled {
                if let Err(e) = self.start_config_services(&config).await {
                    tracing::error!("Failed to start services for config {}: {}", id, e);
                }
            }
        }

        tracing::info!("Loaded {} backup configs", self.configs.len());
        Ok(())
    }

    // ==================== 状态查询 ====================

    /// 获取管理器状态
    pub fn get_status(&self) -> ManagerStatus {
        let watcher_running = self.file_watcher.read().as_ref()
            .map(|w| w.is_running())
            .unwrap_or(false);

        let watched_count = self.file_watcher.read().as_ref()
            .map(|w| w.watched_count())
            .unwrap_or(0);

        let poll_count = self.poll_scheduler.read().as_ref()
            .map(|s| s.schedule_count())
            .unwrap_or(0);

        let (scan_used, scan_total) = self.prepare_pool.scan_slots_info();
        let (encrypt_used, encrypt_total) = self.prepare_pool.encrypt_slots_info();

        ManagerStatus {
            config_count: self.configs.len(),
            // 🔥 修复：直接返回 tasks.len() 避免死锁
            // 因为 handle_transfer_completed 可能持有 tasks 的写锁
            active_task_count: self.tasks.len(),
            watcher_running,
            watched_path_count: watched_count,
            poll_schedule_count: poll_count,
            encryption_enabled: self.encryption_config.read().enabled,
            scan_slots: format!("{}/{}", scan_used, scan_total),
            encrypt_slots: format!("{}/{}", encrypt_used, encrypt_total),
        }
    }

    /// 获取管理器状态（异步版本，避免阻塞异步运行时）
    ///
    /// 在 API handler 等异步上下文中使用此方法
    pub fn get_status_nonblocking(&self) -> ManagerStatus {
        // 使用 try_read 避免阻塞，如果获取不到锁则返回默认值
        let watcher_running = self.file_watcher.try_read()
            .map(|guard| guard.as_ref().map(|w| w.is_running()).unwrap_or(false))
            .unwrap_or(false);

        let watched_count = self.file_watcher.try_read()
            .map(|guard| guard.as_ref().map(|w| w.watched_count()).unwrap_or(0))
            .unwrap_or(0);

        let poll_count = self.poll_scheduler.try_read()
            .map(|guard| guard.as_ref().map(|s| s.schedule_count()).unwrap_or(0))
            .unwrap_or(0);

        let (scan_used, scan_total) = self.prepare_pool.scan_slots_info();
        let (encrypt_used, encrypt_total) = self.prepare_pool.encrypt_slots_info();

        let encryption_enabled = self.encryption_config.try_read()
            .map(|guard| guard.enabled)
            .unwrap_or(false);

        ManagerStatus {
            config_count: self.configs.len(),
            // 🔥 修复：直接返回 tasks.len() 避免死锁
            // 因为 handle_transfer_completed 可能持有 tasks 的写锁
            active_task_count: self.tasks.len(),
            watcher_running,
            watched_path_count: watched_count,
            poll_schedule_count: poll_count,
            encryption_enabled,
            scan_slots: format!("{}/{}", scan_used, scan_total),
            encrypt_slots: format!("{}/{}", encrypt_used, encrypt_total),
        }
    }

    /// 获取记录统计
    pub fn get_record_stats(&self) -> Result<super::record::RecordStats> {
        self.record_manager.get_stats()
    }

    /// 清理过期记录
    pub fn cleanup_old_records(&self, days: u32) -> Result<(usize, usize, usize)> {
        self.record_manager.cleanup_old_records(days)
    }

    /// 更新全局触发配置
    ///
    /// 当用户在系统设置中修改自动备份触发配置时调用
    /// 只创建全局轮询任务（最多4个），不为每个配置创建独立轮询
    pub async fn update_trigger_config(
        &self,
        upload_trigger: crate::config::UploadTriggerConfig,
        download_trigger: crate::config::DownloadTriggerConfig,
    ) {
        use std::time::Duration;
        use super::scheduler::{
            GLOBAL_POLL_UPLOAD_INTERVAL,
            GLOBAL_POLL_UPLOAD_SCHEDULED,
            GLOBAL_POLL_DOWNLOAD_INTERVAL,
            GLOBAL_POLL_DOWNLOAD_SCHEDULED,
        };

        tracing::info!("更新自动备份全局触发配置");

        // 更新文件监听器配置
        if let Some(ref _watcher) = *self.file_watcher.read() {
            // 如果监听被禁用，停止监听器
            if !upload_trigger.watch_enabled {
                tracing::info!("文件监听已禁用，停止监听器");
                // 注意：这里只是记录日志，实际停止需要更复杂的逻辑
                // 因为 watcher 是共享的，需要重新设计停止机制
            }
            // 更新防抖时间等配置需要重新创建 watcher
            // 这里暂时只记录日志，完整实现需要重构 watcher
            tracing::debug!(
                "监听配置: enabled={}, debounce_ms={}, recursive={}",
                upload_trigger.watch_enabled,
                upload_trigger.watch_debounce_ms,
                upload_trigger.watch_recursive
            );
        }

        // 更新轮询调度器配置 - 只创建全局轮询（最多4个）
        let mut scheduler_guard = self.poll_scheduler.write();
        if scheduler_guard.is_none() {
            *scheduler_guard = Some(PollScheduler::new(self.event_tx.clone()));
        }

        if let Some(ref mut scheduler) = *scheduler_guard {
            // 1. 先停止所有旧的全局轮询
            scheduler.remove_schedule(GLOBAL_POLL_UPLOAD_INTERVAL);
            scheduler.remove_schedule(GLOBAL_POLL_UPLOAD_SCHEDULED);
            scheduler.remove_schedule(GLOBAL_POLL_DOWNLOAD_INTERVAL);
            scheduler.remove_schedule(GLOBAL_POLL_DOWNLOAD_SCHEDULED);

            // 2. 上传：间隔轮询
            if upload_trigger.fallback_interval_enabled {
                let schedule = PollScheduleConfig {
                    config_id: GLOBAL_POLL_UPLOAD_INTERVAL.to_string(),
                    enabled: true,
                    interval: Duration::from_secs(upload_trigger.fallback_interval_minutes as u64 * 60),
                    scheduled_time: None,
                };
                scheduler.add_schedule(schedule);
                tracing::info!(
                    "已创建上传间隔轮询: 每 {} 分钟",
                    upload_trigger.fallback_interval_minutes
                );
            }

            // 3. 上传：指定时间轮询
            if upload_trigger.fallback_scheduled_enabled {
                let schedule = PollScheduleConfig {
                    config_id: GLOBAL_POLL_UPLOAD_SCHEDULED.to_string(),
                    enabled: true,
                    interval: Duration::from_secs(0),
                    scheduled_time: Some(ScheduledTime {
                        hour: upload_trigger.fallback_scheduled_hour as u32,
                        minute: upload_trigger.fallback_scheduled_minute as u32,
                    }),
                };
                scheduler.add_schedule(schedule);
                tracing::info!(
                    "已创建上传指定时间轮询: {:02}:{:02}",
                    upload_trigger.fallback_scheduled_hour,
                    upload_trigger.fallback_scheduled_minute
                );
            }

            // 4. 下载：根据 poll_mode 创建对应类型的轮询
            if download_trigger.poll_mode == "interval" {
                let schedule = PollScheduleConfig {
                    config_id: GLOBAL_POLL_DOWNLOAD_INTERVAL.to_string(),
                    enabled: true,
                    interval: Duration::from_secs(download_trigger.poll_interval_minutes as u64 * 60),
                    scheduled_time: None,
                };
                scheduler.add_schedule(schedule);
                tracing::info!(
                    "已创建下载间隔轮询: 每 {} 分钟",
                    download_trigger.poll_interval_minutes
                );
            } else if download_trigger.poll_mode == "scheduled" {
                let schedule = PollScheduleConfig {
                    config_id: GLOBAL_POLL_DOWNLOAD_SCHEDULED.to_string(),
                    enabled: true,
                    interval: Duration::from_secs(0),
                    scheduled_time: Some(ScheduledTime {
                        hour: download_trigger.poll_scheduled_hour as u32,
                        minute: download_trigger.poll_scheduled_minute as u32,
                    }),
                };
                scheduler.add_schedule(schedule);
                tracing::info!(
                    "已创建下载指定时间轮询: {:02}:{:02}",
                    download_trigger.poll_scheduled_hour,
                    download_trigger.poll_scheduled_minute
                );
            }

            tracing::info!(
                "全局轮询配置更新完成，当前活跃轮询数: {}",
                scheduler.schedule_count()
            );
        }
    }

    // ==================== 调试 API ====================

    /// 获取文件状态追踪信息
    ///
    /// 用于调试 API，查询指定文件在备份任务中的状态
    pub fn get_file_state(&self, path: &str) -> Option<FileStateInfo> {
        // 遍历所有任务，查找包含该文件的任务
        for task_ref in self.tasks.iter() {
            let task = task_ref.value();

            // 检查任务的待处理文件列表
            for file_task in &task.pending_files {
                let local_path = file_task.local_path.to_string_lossy();
                let remote_path = &file_task.remote_path;

                if local_path.contains(path) || remote_path.contains(path) {
                    // 找到匹配的文件
                    let config = self.configs.get(&task.config_id);
                    let encryption_enabled = config
                        .map(|c| c.encrypt_enabled)
                        .unwrap_or(false);

                    return Some(FileStateInfo {
                        current_state: format!("{:?}", file_task.status),
                        state_history: vec![
                            (format!("{:?}", file_task.status),
                             chrono::Utc::now().to_rfc3339())
                        ],
                        dedup_result: None, // TODO: 从记录管理器获取
                        encryption_enabled,
                        retry_count: file_task.retry_count,
                        config_id: Some(task.config_id.clone()),
                        task_id: Some(task.id.clone()),
                    });
                }
            }
        }

        None
    }

    /// 执行健康检查
    ///
    /// 检查系统各组件的运行状态
    pub async fn health_check(&self) -> HealthCheckResult {
        // 检查数据库连接（通过记录管理器）
        let database_ok = self.record_manager.get_stats().is_ok();

        // 检查加密密钥状态 - 使用 try_read 避免阻塞
        let encryption_key_ok = self.encryption_config.try_read()
            .map(|config| !config.enabled || config.master_key.is_some())
            .unwrap_or(true);

        // 检查文件监听状态 - 使用 try_read 避免阻塞
        let file_watcher_ok = self.file_watcher.try_read()
            .map(|guard| guard.as_ref().map(|w| w.is_running()).unwrap_or(true))
            .unwrap_or(true);

        // 检查网络连接（简单检查，实际可以 ping 百度服务器）
        let network_ok = true; // TODO: 实现实际的网络检查

        // 检查磁盘空间
        let disk_space_ok = self.check_disk_space();

        HealthCheckResult {
            database_ok,
            encryption_key_ok,
            file_watcher_ok,
            network_ok,
            disk_space_ok,
        }
    }

    /// 检查磁盘空间是否充足
    fn check_disk_space(&self) -> bool {
        // 检查临时目录是否存在且可写
        // 简化实现：只检查目录是否存在
        self.temp_dir.exists() && self.temp_dir.is_dir()
    }

    // ==================== WebSocket 事件 ====================

    /// 设置 WebSocket 管理器
    ///
    /// 使用 Weak 引用存储，避免循环引用导致的内存泄漏
    pub fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        let mut ws = self.ws_manager.write();
        *ws = Some(Arc::downgrade(&ws_manager));
        tracing::info!("自动备份管理器已设置 WebSocket 管理器（Weak 引用）");
    }

    /// 设置上传管理器
    ///
    /// 用于复用现有的上传功能，备份任务会通过 UploadManager 执行
    /// 使用 Weak 引用存储，避免循环引用导致的内存泄漏
    pub fn set_upload_manager(&self, upload_manager: Arc<UploadManager>) {
        let mut um = self.upload_manager.write();
        *um = Some(Arc::downgrade(&upload_manager));
        tracing::info!("自动备份管理器已设置上传管理器（Weak 引用）");
    }

    /// 设置代理配置和回退管理器
    pub fn set_proxy_config(&self, proxy: Option<ProxyConfig>, mgr: Arc<ProxyFallbackManager>) {
        *self.proxy_config.write() = proxy;
        *self.fallback_mgr.write() = Some(mgr);
        tracing::info!("自动备份管理器已设置代理配置");
    }

    /// 热更新代理配置（代理变更时由 ProxyHotUpdater 调用）
    pub fn update_proxy_config(&self, proxy: Option<&ProxyConfig>) {
        *self.proxy_config.write() = proxy.cloned();
        tracing::info!("自动备份管理器代理配置已热更新");
    }

    /// 创建代理感知的 NetdiskClient（内部 helper）
    fn create_netdisk_client(&self, session: crate::auth::UserAuth) -> Result<crate::netdisk::NetdiskClient> {
        let proxy = self.proxy_config.read().clone();
        let fallback = self.fallback_mgr.read().clone();
        crate::netdisk::NetdiskClient::new_with_proxy(session, proxy.as_ref(), fallback)
    }

    /// 启动事件消费循环
    ///
    /// 监听聚合后的变更事件（来自文件监听或定时轮询），通过 TaskController 统一触发
    ///
    /// # 并发控制
    ///
    /// 使用 TaskController 解决三个并发冲突问题：
    /// 1. 同一任务执行很久（如 30 分钟），轮询间隔（如 10 分钟）会再次触发
    /// 2. 文件监听事件随时触发
    /// 3. 轮询和监听同时触发
    ///
    /// 核心保证：
    /// - 同一配置只允许一个执行实例
    /// - 执行中触发会被合并，不丢、不并发
    /// - 轮询和监听共用一套逻辑
    ///
    /// 必须在设置 upload_manager 后调用
    pub async fn start_event_consumer(self: &Arc<Self>) {
        // 取出 aggregated_rx
        let rx = {
            let mut guard = self.aggregated_rx.lock().await;
            guard.take()
        };

        let Some(mut rx) = rx else {
            tracing::warn!("事件消费循环已启动或 aggregated_rx 不可用");
            return;
        };

        let self_clone = Arc::clone(self);
        let task_controllers = self.task_controllers.clone();

        // 🔥 服务重启后恢复执行 Queued 状态的任务
        // 在事件消费循环启动前，为恢复的任务触发执行
        self.resume_queued_tasks_on_startup().await;

        // 事件分发循环：将事件转换为 TaskController 触发
        tokio::spawn(async move {
            tracing::info!("自动备份事件消费循环已启动（使用 TaskController 并发控制）");

            while let Some(event) = rx.recv().await {
                match event {
                    // 🔥 问题3修复：Watch 事件特殊处理，直接处理变化的文件路径
                    ChangeEvent::WatchEvent { config_id, paths } => {
                        tracing::debug!(
                            "收到文件监听事件: config={}, paths={}",
                            config_id, paths.len()
                        );

                        // 检查配置是否启用
                        let config = match self_clone.get_config(&config_id) {
                            Some(c) if c.enabled => c,
                            Some(_) => {
                                tracing::debug!("配置已禁用，跳过Watch事件: config={}", config_id);
                                continue;
                            }
                            None => {
                                tracing::warn!("配置不存在，跳过Watch事件: config={}", config_id);
                                continue;
                            }
                        };

                        // 🔥 Watch 事件：直接处理变化的文件路径，不需要全量扫描
                        // 无论是否有 Transferring 任务，都只处理变化的文件
                        if let Err(e) = self_clone.execute_watch_event(&config, &paths).await {
                            tracing::error!("处理Watch事件失败: config={}, error={}", config_id, e);
                        }
                    }

                    // Poll 事件：走正常的 TaskController 触发流程
                    ChangeEvent::PollEvent { config_id } => {
                        tracing::debug!("收到定时轮询事件: config={}", config_id);

                        let config = match self_clone.get_config(&config_id) {
                            Some(c) if c.enabled => c,
                            Some(_) => {
                                tracing::debug!("配置已禁用，跳过Poll事件: config={}", config_id);
                                continue;
                            }
                            None => {
                                tracing::warn!("配置不存在，跳过Poll事件: config={}", config_id);
                                continue;
                            }
                        };

                        let controller = task_controllers
                            .entry(config_id.clone())
                            .or_insert_with(|| {
                                let ctrl = Arc::new(TaskController::new(config_id.clone()));
                                let ctrl_clone = ctrl.clone();
                                let manager = self_clone.clone();
                                let cfg = config.clone();

                                tokio::spawn(async move {
                                    task_loop(ctrl_clone, || {
                                        let m = manager.clone();
                                        let c = cfg.clone();
                                        async move {
                                            m.execute_backup_for_config(&c).await
                                        }
                                    }).await;
                                });

                                tracing::info!("为配置 {} 创建了新的 TaskController（Poll触发）", config_id);
                                ctrl
                            })
                            .clone();

                        if controller.trigger(TriggerSource::Poll) {
                            tracing::debug!(
                                "配置 {} Poll触发成功（running: {}, pending: {}）",
                                config_id, controller.is_running(), controller.has_pending()
                            );
                        }
                    }

                    // 全局轮询事件：触发所有匹配方向的启用配置
                    ChangeEvent::GlobalPollEvent { direction, poll_type } => {
                        tracing::debug!(
                            "收到全局轮询事件: direction={:?}, poll_type={:?}",
                            direction, poll_type
                        );

                        // 获取所有匹配方向且启用的配置
                        let matching_configs: Vec<BackupConfig> = self_clone.configs.iter()
                            .filter(|c| c.direction == direction && c.enabled)
                            .map(|c| c.clone())
                            .collect();

                        if matching_configs.is_empty() {
                            tracing::debug!(
                                "全局轮询: 没有匹配的启用配置 (direction={:?})",
                                direction
                            );
                            continue;
                        }

                        tracing::info!(
                            "全局轮询触发: direction={:?}, poll_type={:?}, 匹配配置数={}",
                            direction, poll_type, matching_configs.len()
                        );

                        for config in matching_configs {
                            // 🔥 冲突检测：检查是否正在扫描中（Preparing 状态）
                            // 无论是手动触发还是自动触发的扫描，都跳过本次轮询
                            let is_scanning = self_clone.tasks.iter()
                                .any(|t| t.config_id == config.id && t.status == BackupTaskStatus::Preparing);

                            if is_scanning {
                                tracing::info!(
                                    "配置 {} 正在扫描中，跳过 {:?} 轮询触发",
                                    config.id, poll_type
                                );
                                continue;
                            }

                            // 🔥 冲突检测：检查是否正在传输中（Transferring 状态）
                            // 如果有任务正在传输，跳过本次轮询（避免重复触发）
                            // 注意：这里与手动备份的行为一致，都是拒绝而不是合并
                            let is_transferring = self_clone.tasks.iter()
                                .any(|t| t.config_id == config.id && t.status == BackupTaskStatus::Transferring);

                            if is_transferring {
                                tracing::info!(
                                    "配置 {} 正在传输中，跳过 {:?} 轮询触发",
                                    config.id, poll_type
                                );
                                continue;
                            }

                            // 获取或创建 TaskController
                            let controller = task_controllers
                                .entry(config.id.clone())
                                .or_insert_with(|| {
                                    let ctrl = Arc::new(TaskController::new(config.id.clone()));
                                    let ctrl_clone = ctrl.clone();
                                    let manager = self_clone.clone();
                                    let cfg = config.clone();

                                    tokio::spawn(async move {
                                        task_loop(ctrl_clone, || {
                                            let m = manager.clone();
                                            let c = cfg.clone();
                                            async move {
                                                m.execute_backup_for_config(&c).await
                                            }
                                        }).await;
                                    });

                                    tracing::info!(
                                        "为配置 {} 创建了新的 TaskController（全局轮询触发）",
                                        config.id
                                    );
                                    ctrl
                                })
                                .clone();

                            // 触发执行
                            if controller.trigger(TriggerSource::Poll) {
                                tracing::debug!(
                                    "配置 {} 全局轮询触发成功（running: {}, pending: {}）",
                                    config.id, controller.is_running(), controller.has_pending()
                                );
                            } else {
                                tracing::debug!(
                                    "配置 {} 已有任务在执行，全局轮询触发被合并",
                                    config.id
                                );
                            }
                        }
                    }
                }
            }

            tracing::info!("自动备份事件消费循环已停止");
        });
    }

    /// 执行指定配置的备份任务（仅用于 Poll 事件触发的全量扫描）
    ///
    /// 🔥 注意：此方法只被 Poll 事件调用，会执行全量扫描
    /// Watch 事件走 execute_watch_event，只处理变化的文件，不会调用此方法
    ///
    /// 时序：
    /// 1. 扫描阶段检查：如果任务正在扫描（Preparing），丢弃新触发
    /// 2. 执行全量扫描
    /// 3. 如果有增量，判断是否有传输任务（Transferring）
    /// 4. 如果有传输任务，进行增量合并
    /// 5. 如果没有传输任务，创建新任务
    async fn execute_backup_for_config(&self, config: &BackupConfig) -> anyhow::Result<()> {
        // 🔥 扫描阶段优化：如果任务正在扫描（Preparing），丢弃新触发，等待旧扫描完成
        if let Some(scanning_task) = self.tasks.iter()
            .find(|t| t.config_id == config.id && t.status == BackupTaskStatus::Preparing)
        {
            tracing::info!(
                "配置 {} 已有扫描任务正在进行中（task={}），丢弃新触发，等待旧扫描完成",
                config.id, scanning_task.id
            );
            return Ok(()); // 直接返回，不创建新任务
        }

        // 🔥 冲突校验：实际执行前再次校验，防止配置在创建后被其他配置覆盖
        let existing_configs: Vec<BackupConfig> = self.configs.iter().map(|c| c.clone()).collect();
        let conflict_result = validate_for_execute(config, &existing_configs);
        if conflict_result.has_conflict {
            tracing::warn!(
                "配置 {} 执行前冲突校验失败，跳过执行: {}",
                config.id,
                conflict_result.error_message.as_deref().unwrap_or("配置冲突")
            );
            return Err(anyhow!(conflict_result.error_message.unwrap_or_else(|| "配置冲突".to_string())));
        }

        // 确定触发类型（从 TaskController 获取）
        let trigger_type = self.task_controllers
            .get(&config.id)
            .and_then(|ctrl| ctrl.last_trigger_source())
            .map(|s| match s {
                TriggerSource::Poll => TriggerType::Poll,
                TriggerSource::Watch => TriggerType::Watch,
                TriggerSource::Manual => TriggerType::Manual,
            })
            .unwrap_or(TriggerType::Manual);

        tracing::info!(
            "开始执行配置 {} 的备份任务 (方向: {:?}, 触发: {:?})",
            config.id, config.direction, trigger_type
        );

        // 🔥 【关键修复】检查是否有已恢复的 Queued 任务（服务重启后断点续传）
        // 如果有 pending_files 且包含 related_task_id，说明是重启恢复的任务，
        // 跳过扫描直接进入传输阶段，复用已恢复的上传/下载任务

        // 🔥 修复：先找到符合条件的 task_id，避免持有锁导致后续 get_mut 失败
        let restored_task_info = self.tasks.iter()
            .find(|t| t.config_id == config.id && matches!(t.status, BackupTaskStatus::Queued))
            .and_then(|t| {
                let has_restored_files = !t.pending_files.is_empty()
                    && t.pending_files.iter().any(|ft| ft.related_task_id.is_some());

                if has_restored_files {
                    let task_id = t.id.clone();
                    let restored_files = t.pending_files.clone();
                    let restored_count = restored_files.iter().filter(|f| f.related_task_id.is_some()).count();
                    Some((task_id, restored_files, restored_count))
                } else {
                    None
                }
            });

        if let Some((task_id, restored_files, restored_count)) = restored_task_info {
            tracing::info!(
                "检测到重启恢复的备份任务，跳过扫描直接续传: task={}, config={}, files={}, with_related_id={}",
                task_id, config.id, restored_files.len(), restored_count
            );

            // 更新任务状态为 Preparing（execute_*_backup_with_files 内部会改为 Transferring）
            tracing::info!("尝试更新任务状态: task={}", task_id);
            if let Some(mut task) = self.tasks.get_mut(&task_id) {
                tracing::info!("成功获取任务，更新状态为 Preparing: task={}", task_id);
                task.status = BackupTaskStatus::Preparing;
                task.started_at = Some(Utc::now());
            } else {
                tracing::error!("无法获取任务: task={}", task_id);
            }

            tracing::info!(
                "准备调用 execute_*_backup_with_files: task={}, direction={:?}",
                task_id, config.direction
            );

            match config.direction {
                BackupDirection::Upload => {
                    tracing::info!("调用 execute_upload_backup_with_files: task={}", task_id);
                    self.execute_upload_backup_with_files(
                        task_id.clone(), config.clone(), restored_files,
                    ).await?;
                    tracing::info!("execute_upload_backup_with_files 完成: task={}", task_id);
                }
                BackupDirection::Download => {
                    tracing::info!("调用 execute_download_backup_with_files: task={}", task_id);
                    self.execute_download_backup_with_files(
                        task_id.clone(), config.clone(), restored_files,
                    ).await?;
                    tracing::info!("execute_download_backup_with_files 完成: task={}", task_id);
                }
            }

            return Ok(());
        }

        // 🔥 正确时序：先扫描，扫描完成后再判断是否有传输任务
        // 根据配置方向执行扫描
        let new_files = match config.direction {
            BackupDirection::Upload => {
                self.scan_local_directory_for_backup(config).await?
            }
            BackupDirection::Download => {
                self.scan_remote_directory_for_backup(config).await?
            }
        };

        // 如果没有新文件需要备份，直接返回
        if new_files.is_empty() {
            tracing::info!(
                "配置 {} 扫描完成，没有新文件需要备份",
                config.id
            );
            return Ok(());
        }

        tracing::info!(
            "配置 {} 扫描完成，发现 {} 个新文件需要备份",
            config.id, new_files.len()
        );

        // 🔥 传输阶段优化：检查是否有正在传输的任务（Transferring 状态）
        // 如果有，进行增量合并；如果没有，创建新任务
        if let Some(transferring_task) = self.tasks.iter()
            .find(|t| t.config_id == config.id && t.status == BackupTaskStatus::Transferring)
        {
            let task_id = transferring_task.id.clone();
            tracing::info!(
                "配置 {} 已有传输任务正在进行中（task={}），增量合并 {} 个新文件到现有任务",
                config.id, task_id, new_files.len()
            );
            // 增量合并新文件到现有任务
            return self.merge_new_files_to_task(&task_id, config, new_files).await;
        }

        // 没有传输任务，按原有逻辑创建新任务或复用 Queued 任务
        let reusable_task_id = self
            .tasks
            .iter()
            .filter(|t| t.config_id == config.id && matches!(t.status, BackupTaskStatus::Queued))
            .min_by_key(|t| t.created_at)
            .map(|t| t.id.clone());

        let task_id = if let Some(task_id) = reusable_task_id {
            // 重置旧任务的关键字段
            if let Some(mut task) = self.tasks.get_mut(&task_id) {
                task.error_message = None;
                // 🔥 修复：清理旧的待完成任务ID，避免与新创建的任务ID混淆
                // 这些旧ID对应的下载/上传任务可能已经不存在了
                let old_download_count = task.pending_download_task_ids.len();
                let old_upload_count = task.pending_upload_task_ids.len();
                task.pending_download_task_ids.clear();
                task.pending_upload_task_ids.clear();
                task.transfer_task_map.clear();
                if old_download_count > 0 || old_upload_count > 0 {
                    tracing::info!(
                        "复用任务时清理旧的待完成任务ID: task={}, old_download={}, old_upload={}",
                        task_id, old_download_count, old_upload_count
                    );
                }
            }

            if let Some(task) = self.tasks.get(&task_id) {
                if let Err(e) = self.persistence_manager.save_task(&task) {
                    tracing::warn!("持久化复用的备份任务失败: task={}, error={}", task_id, e);
                }
            }

            tracing::info!(
                "复用旧的 Queued 备份任务继续执行: task={}, config={}, trigger={:?}",
                task_id, config.id, trigger_type
            );
            task_id
        } else {
            // 新建任务记录
            self.create_backup_task_record(config, trigger_type).await?
        };

        // 执行传输任务（使用已扫描的文件列表）
        match config.direction {
            BackupDirection::Upload => {
                self.execute_upload_backup_with_files(
                    task_id.clone(),
                    config.clone(),
                    new_files,
                ).await?;
            }
            BackupDirection::Download => {
                self.execute_download_backup_with_files(
                    task_id.clone(),
                    config.clone(),
                    new_files,
                ).await?;
            }
        }

        tracing::info!(
            "配置 {} 的备份任务执行完成: task_id={}",
            config.id, task_id
        );

        Ok(())
    }

    /// 创建备份任务记录（仅创建，不执行）
    ///
    /// 用于 TaskController 调用场景，任务执行由调用方控制
    async fn create_backup_task_record(&self, config: &BackupConfig, trigger_type: TriggerType) -> Result<String> {
        let task_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let task = BackupTask {
            id: task_id.clone(),
            config_id: config.id.clone(),
            status: BackupTaskStatus::Queued,
            sub_phase: None,
            trigger_type,
            pending_files: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            skipped_count: 0,
            total_count: 0,
            transferred_bytes: 0,
            total_bytes: 0,
            scan_progress: None,
            created_at: now,
            started_at: None,
            completed_at: None,
            error_message: None,
            pending_upload_task_ids: std::collections::HashSet::new(),
            pending_download_task_ids: std::collections::HashSet::new(),
            transfer_task_map: std::collections::HashMap::new(),
        };

        // 保存到内存
        self.tasks.insert(task_id.clone(), task.clone());

        // 持久化到数据库
        if let Err(e) = self.persistence_manager.save_task(&task) {
            tracing::warn!("持久化备份任务失败: {}", e);
        }

        // 发送任务创建事件
        self.publish_task_created(&task, config);

        tracing::info!(
            "Created backup task record: {} for config: {} (trigger: {:?})",
            task_id, config.id, trigger_type
        );

        Ok(task_id)
    }

    /// 手动触发备份（通过 TaskController）
    ///
    /// 与自动触发使用相同的并发控制逻辑
    pub fn trigger_backup_manual(&self, config_id: &str) -> Result<bool> {
        let config = self.get_config(config_id)
            .ok_or_else(|| anyhow!("配置不存在: {}", config_id))?;

        if !config.enabled {
            return Err(anyhow!("配置已禁用: {}", config_id));
        }

        // 🔥 冲突检测：检查是否有正在扫描中的任务（Preparing 状态）
        // 无论是手动触发还是自动触发的扫描，都不允许重复触发
        let is_scanning = self.tasks.iter()
            .any(|t| t.config_id == config_id && t.status == BackupTaskStatus::Preparing);

        if is_scanning {
            tracing::info!(
                "手动备份被拒绝：配置 {} 正在扫描中，请等待扫描完成后再试",
                config_id
            );
            return Err(anyhow!("该配置正在扫描中，请等待扫描完成后再试"));
        }

        // 🔥 冲突检测：检查是否有正在传输中的任务（Transferring 状态）
        let is_transferring = self.tasks.iter()
            .any(|t| t.config_id == config_id && t.status == BackupTaskStatus::Transferring);

        if is_transferring {
            tracing::info!(
                "手动备份被拒绝：配置 {} 正在传输中，请等待传输完成或暂停后再试",
                config_id
            );
            return Err(anyhow!("该配置正在传输中，请等待传输完成或暂停后再试"));
        }

        // 获取或创建 TaskController
        let controller = self.task_controllers
            .entry(config_id.to_string())
            .or_insert_with(|| {
                let ctrl = Arc::new(TaskController::new(config_id.to_string()));
                // 注意：这里只创建控制器，不启动任务循环
                // 任务循环会在 start_event_consumer 中启动
                tracing::warn!(
                    "手动触发时创建了新的 TaskController，建议先调用 start_event_consumer: {}",
                    config_id
                );
                ctrl
            })
            .clone();

        let triggered = controller.trigger(TriggerSource::Manual);

        if triggered {
            tracing::info!(
                "手动触发配置 {} 成功（running: {}, pending: {}）",
                config_id, controller.is_running(), controller.has_pending()
            );
        }

        Ok(triggered)
    }

    /// 获取配置的控制器状态
    pub fn get_controller_status(&self, config_id: &str) -> Option<super::scheduler::ControllerStatus> {
        self.task_controllers.get(config_id).map(|ctrl| ctrl.status())
    }

    /// 获取所有控制器状态
    pub fn get_all_controller_statuses(&self) -> Vec<super::scheduler::ControllerStatus> {
        self.task_controllers
            .iter()
            .map(|entry| entry.value().status())
            .collect()
    }

    /// 停止指定配置的控制器
    pub fn stop_controller(&self, config_id: &str) {
        if let Some(ctrl) = self.task_controllers.get(config_id) {
            ctrl.cancel();
            tracing::info!("已停止配置 {} 的 TaskController", config_id);
        }
    }

    /// 停止所有控制器
    pub fn stop_all_controllers(&self) {
        for entry in self.task_controllers.iter() {
            entry.value().cancel();
        }
        tracing::info!("已停止所有 TaskController");
    }

    /// 设置下载管理器
    ///
    /// 用于复用现有的下载功能，备份任务会通过 DownloadManager 执行
    /// 使用 Weak 引用存储，避免循环引用导致的内存泄漏
    pub fn set_download_manager(&self, download_manager: Arc<DownloadManager>) {
        let mut dm = self.download_manager.write();
        *dm = Some(Arc::downgrade(&download_manager));
        tracing::info!("自动备份管理器已设置下载管理器（Weak 引用）");
    }

    /// 🔥 启动统一的传输事件监听器
    ///
    /// 使用单一 channel 监听上传和下载任务的所有事件：
    /// - 进度更新
    /// - 状态变更
    /// - 任务完成
    /// - 任务失败
    /// - 任务暂停/恢复
    /// - 任务删除
    ///
    /// 应在设置 upload_manager 和 download_manager 之后调用
    pub async fn start_transfer_listeners(self: &Arc<Self>) {
        use super::events::{BackupTransferNotification, TransferTaskType};

        // 🔥 创建统一的通知 channel（上传和下载共用）
        let (notification_tx, mut notification_rx) =
            tokio::sync::mpsc::unbounded_channel::<BackupTransferNotification>();

        // 设置上传管理器的通知发送器
        // 使用安全获取方法，处理 Weak 引用升级
        let upload_manager_opt = self.get_upload_manager();
        if let Some(upload_manager) = upload_manager_opt {
            upload_manager
                .set_backup_notification_sender(notification_tx.clone())
                .await;
            tracing::info!("已设置上传备份任务统一通知监听器");
        } else {
            tracing::warn!("上传管理器未设置，无法启动上传通知监听器");
        }

        // 设置下载管理器的通知发送器（共用同一个 sender）
        // 使用安全获取方法，处理 Weak 引用升级
        let download_manager_opt = self.get_download_manager();
        if let Some(download_manager) = download_manager_opt {
            download_manager
                .set_backup_notification_sender(notification_tx)
                .await;
            tracing::info!("已设置下载备份任务统一通知监听器");
        } else {
            tracing::warn!("下载管理器未设置，无法启动下载通知监听器");
        }

        // 🔥 启动统一的事件监听循环（只需要一个 spawn）
        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            tracing::info!("🚀 备份任务统一通知监听循环已启动");

            while let Some(notification) = notification_rx.recv().await {
                let task_id = notification.task_id();
                let is_upload = notification.is_upload();
                let event_name = notification.event_name();

                tracing::debug!(
                    "收到备份任务通知: task_id={}, type={}, event={}",
                    task_id,
                    if is_upload { "upload" } else { "download" },
                    event_name
                );

                match notification {
                    BackupTransferNotification::Progress {
                        task_id,
                        task_type,
                        transferred_bytes,
                        total_bytes,
                    } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        self_clone
                            .handle_transfer_progress(&task_id, transferred_bytes, total_bytes, is_upload)
                            .await;
                    }
                    BackupTransferNotification::Completed { task_id, task_type } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        self_clone
                            .handle_transfer_completed(&task_id, true, is_upload)
                            .await;
                    }
                    BackupTransferNotification::Failed {
                        task_id,
                        task_type,
                        error_message,
                    } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        tracing::warn!(
                            "备份{}任务失败: task_id={}, error={}",
                            if is_upload { "上传" } else { "下载" },
                            task_id,
                            error_message
                        );
                        self_clone
                            .handle_transfer_completed(&task_id, false, is_upload)
                            .await;
                    }
                    BackupTransferNotification::StatusChanged {
                        task_id,
                        task_type,
                        old_status,
                        new_status,
                    } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        tracing::debug!(
                            "备份{}任务状态变更: task_id={}, {:?} -> {:?}",
                            if is_upload { "上传" } else { "下载" },
                            task_id,
                            old_status,
                            new_status
                        );
                        self_clone
                            .handle_transfer_status_changed(&task_id, new_status, is_upload)
                            .await;
                    }
                    BackupTransferNotification::Created { task_id, task_type, total_bytes } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        tracing::debug!(
                            "备份{}任务创建: task_id={}, total_bytes={}",
                            if is_upload { "上传" } else { "下载" },
                            task_id,
                            total_bytes
                        );
                    }
                    BackupTransferNotification::Paused { task_id, task_type } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        tracing::debug!(
                            "备份{}任务暂停: task_id={}",
                            if is_upload { "上传" } else { "下载" },
                            task_id
                        );
                        // 🔥 暂停时更新文件任务状态为 WaitingTransfer
                        self_clone
                            .handle_transfer_status_changed(&task_id, TransferTaskStatus::Paused, is_upload)
                            .await;
                    }
                    BackupTransferNotification::Resumed { task_id, task_type } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        tracing::debug!(
                            "备份{}任务恢复: task_id={}",
                            if is_upload { "上传" } else { "下载" },
                            task_id
                        );
                        // 🔥 恢复时更新文件任务状态为 Transferring
                        self_clone
                            .handle_transfer_status_changed(&task_id, TransferTaskStatus::Transferring, is_upload)
                            .await;
                    }
                    BackupTransferNotification::Deleted { task_id, task_type } => {
                        let is_upload = task_type == TransferTaskType::Upload;
                        tracing::debug!(
                            "备份{}任务删除: task_id={}",
                            if is_upload { "上传" } else { "下载" },
                            task_id
                        );
                    }
                    // 🔥 解密相关通知处理
                    BackupTransferNotification::DecryptStarted { task_id, file_name } => {
                        tracing::info!(
                            "备份下载任务开始解密: task_id={}, file_name={}",
                            task_id, file_name
                        );
                        self_clone
                            .handle_decrypt_started(&task_id, &file_name)
                            .await;
                    }
                    BackupTransferNotification::DecryptProgress {
                        task_id,
                        file_name,
                        progress,
                        processed_bytes,
                        total_bytes,
                    } => {
                        self_clone
                            .handle_decrypt_progress(&task_id, &file_name, progress, processed_bytes, total_bytes)
                            .await;
                    }
                    BackupTransferNotification::DecryptCompleted {
                        task_id,
                        file_name,
                        original_name,
                        decrypted_path,
                    } => {
                        tracing::info!(
                            "备份下载任务解密完成: task_id={}, file_name={}, original_name={}",
                            task_id, file_name, original_name
                        );
                        self_clone
                            .handle_decrypt_completed(&task_id, &file_name, &original_name, &decrypted_path)
                            .await;
                    }
                }
            }

            tracing::info!("备份任务统一通知监听循环已停止");
        });
    }

    /// 处理传输任务完成事件
    ///
    /// 当上传或下载任务完成时，更新对应的备份任务状态
    async fn handle_transfer_completed(&self, transfer_task_id: &str, success: bool, is_upload: bool) {
        use super::record::{UploadRecord, DownloadRecord};

        let task_type = if is_upload { "上传" } else { "下载" };

        // 🔥 第一阶段：快速收集需要处理的任务信息，立即释放锁
        struct TaskUpdateInfo {
            backup_task_id: String,
            config_id: String,
            file_task_id: Option<String>,
            file_size: u64,
            local_path: std::path::PathBuf,
            remote_path: String,
            encrypted: bool,
            encrypted_name: Option<String>,
            head_md5: Option<String>,
            fs_id: Option<u64>,  // 🔥 添加 fs_id 字段，用于下载去重记录
            all_completed: bool,
            old_status: String,
            new_status: String,
            final_status: BackupTaskStatus,
            error_message: Option<String>,
        }

        let mut update_info: Option<TaskUpdateInfo> = None;
        let mut task_id_to_cleanup: Option<String> = None;

        // 快速遍历，只收集信息，不做任何 I/O 操作
        for mut entry in self.tasks.iter_mut() {
            let task = entry.value_mut();

            let is_pending = if is_upload {
                task.pending_upload_task_ids.contains(transfer_task_id)
            } else {
                task.pending_download_task_ids.contains(transfer_task_id)
            };

            if !is_pending {
                continue;
            }

            tracing::info!(
                "备份任务 {} 的{}任务 {} 已完成，success={}",
                task.id, task_type, transfer_task_id, success
            );

            // 从待完成集合中移除
            if is_upload {
                task.pending_upload_task_ids.remove(transfer_task_id);
            } else {
                task.pending_download_task_ids.remove(transfer_task_id);
            }

            // 更新文件任务状态
            let mut file_task_info = None;
            if let Some(file_task_id) = task.transfer_task_map.get(transfer_task_id).cloned() {
                if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    // 🔥 记录旧状态用于发送状态变更事件
                    let old_file_status = file_task.status;
                    file_task.updated_at = Utc::now();
                    if success {
                        file_task.status = BackupFileStatus::Completed;
                        task.completed_count += 1;
                        task.transferred_bytes += file_task.file_size;
                        tracing::debug!("文件任务 {} 已完成", file_task_id);
                    } else {
                        file_task.status = BackupFileStatus::Failed;
                        task.failed_count += 1;
                        tracing::debug!("文件任务 {} 已失败", file_task_id);
                    }

                    // 🔥 发送文件状态变更事件到 WebSocket
                    let file_name = file_task.local_path.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let old_status_str = match old_file_status {
                        BackupFileStatus::Pending => "pending",
                        BackupFileStatus::Checking => "checking",
                        BackupFileStatus::Skipped => "skipped",
                        BackupFileStatus::Encrypting => "encrypting",
                        BackupFileStatus::WaitingTransfer => "waiting_transfer",
                        BackupFileStatus::Transferring => "transferring",
                        BackupFileStatus::Completed => "completed",
                        BackupFileStatus::Failed => "failed",
                    };
                    let new_status_str = if success { "completed" } else { "failed" };
                    Self::publish_file_status_changed_static(
                        &self.ws_manager,
                        &task.id,
                        &file_task_id,
                        &file_name,
                        old_status_str,
                        new_status_str,
                    );

                    file_task_info = Some((
                        file_task_id.clone(),
                        file_task.file_size,
                        file_task.local_path.clone(),
                        file_task.remote_path.clone(),
                        file_task.encrypted,
                        file_task.encrypted_name.clone(),
                        file_task.head_md5.clone(),
                        file_task.fs_id,  // 🔥 添加 fs_id，用于下载去重记录
                    ));
                }
                task.transfer_task_map.remove(transfer_task_id);
            }

            // 发送进度事件（非阻塞）
            Self::publish_progress_static(&self.ws_manager, task);

            let all_completed = task.pending_upload_task_ids.is_empty()
                && task.pending_download_task_ids.is_empty();

            let old_status = format!("{:?}", task.status);
            let mut new_status = old_status.clone();
            let mut final_status = task.status.clone();

            if all_completed {
                // 根据完成情况更新备份任务状态
                if task.failed_count == 0 {
                    task.status = BackupTaskStatus::Completed;
                } else if task.completed_count > 0 {
                    task.status = BackupTaskStatus::PartiallyCompleted;
                } else {
                    task.status = BackupTaskStatus::Failed;
                }
                task.completed_at = Some(Utc::now());
                new_status = format!("{:?}", task.status);
                final_status = task.status.clone();

                tracing::info!(
                    "备份任务 {} 所有传输已完成: completed={}, failed={}, skipped={}, status={}",
                    task.id, task.completed_count, task.failed_count, task.skipped_count, new_status
                );

                // 发送进度事件（最终状态）
                Self::publish_progress_static(&self.ws_manager, task);
            }

            // 收集更新信息
            if let Some((file_task_id, file_size, local_path, remote_path, encrypted, encrypted_name, head_md5, fs_id)) = file_task_info {
                update_info = Some(TaskUpdateInfo {
                    backup_task_id: task.id.clone(),
                    config_id: task.config_id.clone(),
                    file_task_id: Some(file_task_id),
                    file_size,
                    local_path,
                    remote_path,
                    encrypted,
                    encrypted_name,
                    head_md5,
                    fs_id,  // 🔥 添加 fs_id
                    all_completed,
                    old_status,
                    new_status,
                    final_status,
                    error_message: task.error_message.clone(),
                });
            } else if all_completed {
                // 🔥 修复：即使没有 file_task_info，也需要处理任务完成逻辑
                update_info = Some(TaskUpdateInfo {
                    backup_task_id: task.id.clone(),
                    config_id: task.config_id.clone(),
                    file_task_id: None,
                    file_size: 0,
                    local_path: std::path::PathBuf::new(),
                    remote_path: String::new(),
                    encrypted: false,
                    encrypted_name: None,
                    head_md5: None,
                    fs_id: None,  // 🔥 添加 fs_id
                    all_completed,
                    old_status,
                    new_status,
                    final_status,
                    error_message: task.error_message.clone(),
                });
            }

            if all_completed {
                task_id_to_cleanup = Some(task.id.clone());
            }

            break; // 找到对应的备份任务后退出循环
        }
        // 🔥 循环结束，DashMap 锁已释放

        // 🔥 第二阶段：执行异步操作和数据库写入（无锁）
        if let Some(update) = update_info {
            let config = self.configs.get(&update.config_id).map(|c| c.clone());

            if success {
                if let Some(ref cfg) = config {
                    if is_upload {
                        // 写入上传去重记录
                        let file_name = update.local_path.file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let relative_path = update.local_path.strip_prefix(&cfg.local_path)
                            .map(|p| p.parent().unwrap_or(std::path::Path::new("")).to_string_lossy().to_string())
                            .unwrap_or_default();

                        let upload_record = UploadRecord {
                            config_id: cfg.id.clone(),
                            relative_path,
                            file_name,
                            file_size: update.file_size,
                            head_md5: update.head_md5.unwrap_or_default(),
                            full_md5: None,
                            remote_path: update.remote_path.clone(),
                            encrypted: update.encrypted,
                            encrypted_name: update.encrypted_name,
                        };

                        if let Err(e) = self.record_manager.add_upload_record(&upload_record) {
                            tracing::error!("写入上传去重记录失败: file={}, error={}", update.remote_path, e);
                        } else {
                            tracing::debug!("已写入上传去重记录: {}", update.remote_path);
                        }

                        // 更新扫描缓存（上传成功后同步 mtime/size）
                        let mtime = std::fs::metadata(&update.local_path)
                            .and_then(|m| m.modified())
                            .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                            .unwrap_or(0);
                        let rel_path = update.local_path.strip_prefix(&cfg.local_path)
                            .map(|p| p.to_string_lossy().to_string())
                            .unwrap_or_default();
                        let cache_entry = super::scan_cache::CachedFileEntry {
                            config_id: cfg.id.clone(),
                            file_path: update.local_path.to_string_lossy().to_string(),
                            relative_path: rel_path,
                            mtime,
                            size: update.file_size as i64,
                            head_md5: Some(upload_record.head_md5.clone()),
                            last_scan_at: chrono::Utc::now().timestamp(),
                        };
                        if let Err(e) = self.scan_cache_manager.upsert_single(cache_entry) {
                            tracing::warn!("更新扫描缓存失败: {}", e);
                        }
                    } else {
                        // 🔥 现在可以安全调用 .await（已释放 DashMap 锁）
                        let actual_local_path = self.get_download_task_local_path(transfer_task_id).await
                            .unwrap_or_else(|| update.local_path.clone());

                        let file_name = actual_local_path.file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("unknown")
                            .to_string();

                        // 🔥 修复：优先使用 BackupFileTask 中保存的 fs_id，避免从已清理的 DownloadManager 获取
                        let fs_id = match update.fs_id {
                            Some(id) => id.to_string(),
                            None => self.get_download_task_fs_id(transfer_task_id).await
                                .unwrap_or_else(|| "unknown".to_string()),
                        };

                        let download_record = DownloadRecord {
                            config_id: cfg.id.clone(),
                            remote_path: update.remote_path.clone(),
                            file_name,
                            file_size: update.file_size,
                            fs_id,
                            local_path: actual_local_path.to_string_lossy().to_string(),
                            encrypted: update.encrypted,
                        };

                        if let Err(e) = self.record_manager.add_download_record(&download_record) {
                            tracing::error!("写入下载去重记录失败: file={}, error={}", update.remote_path, e);
                        } else {
                            tracing::debug!("已写入下载去重记录: {} -> {}", update.remote_path, actual_local_path.display());
                        }
                    }
                }
            }

            // 持久化文件任务状态
            if let Some(ref file_task_id) = update.file_task_id {
                if let Some(ref cfg) = config {
                    // 需要重新获取 file_task 来持久化
                    if let Some(task_entry) = self.tasks.get(&update.backup_task_id) {
                        if let Some(file_task) = task_entry.pending_files.iter().find(|f| f.id == *file_task_id) {
                            if let Err(e) = self.persistence_manager.save_file_task(file_task, &cfg.id) {
                                tracing::error!("持久化文件任务状态失败: file_task={}, error={}", file_task_id, e);
                            }
                        }
                    }
                }
            }

            // 如果所有任务完成，处理完成逻辑
            if update.all_completed {
                // 发送状态变更事件
                self.publish_status_changed(&update.backup_task_id, &update.old_status, &update.new_status);

                // 持久化任务状态
                if let Some(task_entry) = self.tasks.get(&update.backup_task_id) {
                    let task = task_entry.value();
                    if let Err(e) = self.persistence_manager.save_task(task) {
                        tracing::error!("持久化备份任务状态失败: {}", e);
                    }

                    // 发送任务完成/失败事件（触发前端刷新）
                    match update.final_status {
                        BackupTaskStatus::Completed | BackupTaskStatus::PartiallyCompleted => {
                            self.publish_task_completed(task);
                        }
                        BackupTaskStatus::Failed => {
                            let error_msg = update.error_message.unwrap_or_else(|| "所有文件传输失败".to_string());
                            self.publish_task_failed(&update.backup_task_id, &error_msg);
                        }
                        _ => {}
                    }
                }
            }
        }

        // 🔥 内存优化：任务进入终态后从 DashMap 移除
        if let Some(task_id) = task_id_to_cleanup {
            self.cleanup_completed_task(&task_id);
        }
    }

    /// 处理传输任务进度事件
    ///
    /// 当上传或下载任务进度更新时，更新对应的备份任务进度并发送 WebSocket 事件
    /// transfer_task_id: 上传/下载任务ID
    /// transferred_bytes: 当前已传输字节数
    /// total_bytes: 该任务的总字节数
    /// is_upload: true 表示上传任务，false 表示下载任务
    async fn handle_transfer_progress(
        &self,
        transfer_task_id: &str,
        transferred_bytes: u64,
        _total_bytes: u64,
        is_upload: bool,
    ) {
        // 遍历所有备份任务，查找包含此传输任务ID的备份任务
        for mut entry in self.tasks.iter_mut() {
            let task = entry.value_mut();

            // 检查是否是上传任务或下载任务
            let is_pending = if is_upload {
                task.pending_upload_task_ids.contains(transfer_task_id)
            } else {
                task.pending_download_task_ids.contains(transfer_task_id)
            };

            if !is_pending {
                continue;
            }

            // 查找对应的文件任务并更新进度
            let file_task_for_event = if let Some(file_task_id) = task.transfer_task_map.get(transfer_task_id).cloned() {
                if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    // 更新文件任务的已传输字节数
                    file_task.transferred_bytes = transferred_bytes;
                    file_task.updated_at = Utc::now();
                    Some((task.id.clone(), file_task.clone()))
                } else {
                    None
                }
            } else {
                None
            };

            // 🔥 发送文件进度事件（在锁释放后，避免死锁）
            if let Some((task_id, ref ft)) = file_task_for_event {
                Self::publish_file_progress_static(&self.ws_manager, &task_id, ft);
            }

            // 重新计算备份任务的总已传输字节数
            let total_transferred: u64 = task.pending_files.iter()
                .map(|f| {
                    if matches!(f.status, BackupFileStatus::Completed) {
                        f.file_size  // 已完成的文件用文件大小
                    } else {
                        f.transferred_bytes  // 未完成的文件用已传输字节数
                    }
                })
                .sum();

            task.transferred_bytes = total_transferred;

            // 发送进度事件到 WebSocket（实时更新前端）
            Self::publish_progress_static(&self.ws_manager, task);

            break; // 找到对应的备份任务后退出循环
        }
    }

    /// 处理解密开始通知
    ///
    /// 更新文件任务状态为解密中，并发送 WebSocket 事件
    async fn handle_decrypt_started(&self, transfer_task_id: &str, file_name: &str) {
        use crate::server::events::BackupEvent as WsBackupEvent;

        for mut entry in self.tasks.iter_mut() {
            let task = entry.value_mut();

            // 检查是否是下载任务
            if !task.pending_download_task_ids.contains(transfer_task_id) {
                continue;
            }

            // 查找对应的文件任务并更新状态
            if let Some(file_task_id) = task.transfer_task_map.get(transfer_task_id).cloned() {
                if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    // 更新状态为解密中（复用 Encrypting 状态）
                    file_task.status = BackupFileStatus::Encrypting;
                    file_task.updated_at = Utc::now();

                    // 发送解密开始事件
                    self.publish_event(WsBackupEvent::FileDecrypting {
                        task_id: task.id.clone(),
                        file_task_id: file_task_id.clone(),
                        file_name: file_name.to_string(),
                    });

                    tracing::debug!(
                        "文件任务 {} 状态更新为解密中",
                        file_task_id
                    );
                }
            }

            break;
        }
    }

    /// 处理解密进度通知
    async fn handle_decrypt_progress(
        &self,
        transfer_task_id: &str,
        file_name: &str,
        progress: f64,
        processed_bytes: u64,
        total_bytes: u64,
    ) {
        use crate::server::events::BackupEvent as WsBackupEvent;

        for mut entry in self.tasks.iter_mut() {
            let task = entry.value_mut();

            if !task.pending_download_task_ids.contains(transfer_task_id) {
                continue;
            }

            if let Some(file_task_id) = task.transfer_task_map.get(transfer_task_id).cloned() {
                if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    file_task.decrypt_progress = Some(progress);
                    file_task.updated_at = Utc::now();

                    self.publish_event(WsBackupEvent::FileDecryptProgress {
                        task_id: task.id.clone(),
                        file_task_id: file_task_id.clone(),
                        file_name: file_name.to_string(),
                        progress,
                        processed_bytes,
                        total_bytes,
                    });
                }
            }

            break;
        }
    }

    /// 处理解密完成通知
    async fn handle_decrypt_completed(
        &self,
        transfer_task_id: &str,
        file_name: &str,
        original_name: &str,
        _decrypted_path: &str,
    ) {
        use crate::server::events::BackupEvent as WsBackupEvent;

        for mut entry in self.tasks.iter_mut() {
            let task = entry.value_mut();

            if !task.pending_download_task_ids.contains(transfer_task_id) {
                continue;
            }

            if let Some(file_task_id) = task.transfer_task_map.get(transfer_task_id).cloned() {
                if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    file_task.decrypt_progress = Some(100.0);
                    file_task.updated_at = Utc::now();

                    self.publish_event(WsBackupEvent::FileDecrypted {
                        task_id: task.id.clone(),
                        file_task_id: file_task_id.clone(),
                        file_name: file_name.to_string(),
                        original_name: original_name.to_string(),
                        original_size: file_task.file_size,
                    });

                    tracing::debug!(
                        "文件任务 {} 解密完成: {} -> {}",
                        file_task_id, file_name, original_name
                    );
                }
            }

            break;
        }
    }

    /// 处理传输任务状态变更事件
    ///
    /// 当上传或下载任务状态变更时，更新对应的备份文件任务状态，
    /// 持久化到数据库，并发送 WebSocket 事件
    async fn handle_transfer_status_changed(
        &self,
        transfer_task_id: &str,
        new_status:  TransferTaskStatus,
        is_upload: bool,
    ) {
        use  crate::autobackup::events::TransferTaskStatus;

        // 遍历所有备份任务，查找包含此传输任务ID的备份任务
        for mut entry in self.tasks.iter_mut() {
            let task = entry.value_mut();

            // 检查是否是上传任务或下载任务
            let is_pending = if is_upload {
                task.pending_upload_task_ids.contains(transfer_task_id)
            } else {
                task.pending_download_task_ids.contains(transfer_task_id)
            };

            if !is_pending {
                continue;
            }

            // 获取配置信息（用于持久化）
            let config = self.configs.get(&task.config_id).map(|c| c.clone());

            // 查找对应的文件任务并更新状态
            if let Some(file_task_id) = task.transfer_task_map.get(transfer_task_id).cloned() {
                if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                    // 将上传/下载状态映射到备份文件状态
                    let old_backup_status = file_task.status;
                    let new_backup_status = match (old_backup_status, new_status) {
                        // 🔥 关键：如果文件任务当前是传输中，传输任务变为等待时，文件任务应该变为等待传输
                        (BackupFileStatus::Transferring, TransferTaskStatus::Pending) => BackupFileStatus::WaitingTransfer,
                        // 其他情况下的 Pending 映射（初始状态或从其他状态变为等待）
                        (_, TransferTaskStatus::Pending) => BackupFileStatus::Pending,
                        // 传输中
                        (_, TransferTaskStatus::Transferring) => BackupFileStatus::Transferring,
                        // 暂停 -> 等待传输
                        (_, TransferTaskStatus::Paused) => BackupFileStatus::WaitingTransfer,
                        // 完成
                        (_, TransferTaskStatus::Completed) => BackupFileStatus::Completed,
                        // 失败
                        (_, TransferTaskStatus::Failed) => BackupFileStatus::Failed,
                    };

                    // 只有状态真正变化时才更新
                    if old_backup_status != new_backup_status {
                        file_task.status = new_backup_status;
                        file_task.updated_at = Utc::now();

                        let file_name = file_task.local_path.file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("unknown")
                            .to_string();

                        let old_status_str = match old_backup_status {
                            BackupFileStatus::Pending => "pending",
                            BackupFileStatus::Checking => "checking",
                            BackupFileStatus::Skipped => "skipped",
                            BackupFileStatus::Encrypting => "encrypting",
                            BackupFileStatus::WaitingTransfer => "waiting_transfer",
                            BackupFileStatus::Transferring => "transferring",
                            BackupFileStatus::Completed => "completed",
                            BackupFileStatus::Failed => "failed",
                        };

                        let new_status_str = match new_backup_status {
                            BackupFileStatus::Pending => "pending",
                            BackupFileStatus::Checking => "checking",
                            BackupFileStatus::Skipped => "skipped",
                            BackupFileStatus::Encrypting => "encrypting",
                            BackupFileStatus::WaitingTransfer => "waiting_transfer",
                            BackupFileStatus::Transferring => "transferring",
                            BackupFileStatus::Completed => "completed",
                            BackupFileStatus::Failed => "failed",
                        };

                        tracing::debug!(
                            "文件任务状态变更: file_task={}, {} -> {}",
                            file_task_id, old_status_str, new_status_str
                        );

                        // 🔥 持久化文件任务状态到数据库
                        if let Some(ref cfg) = config {
                            if let Err(e) = self.persistence_manager.save_file_task(file_task, &cfg.id) {
                                tracing::error!("持久化文件任务状态失败: file_task={}, error={}", file_task_id, e);
                            }
                        }

                        // 🔥 发送文件状态变更事件到 WebSocket
                        Self::publish_file_status_changed_static(
                            &self.ws_manager,
                            &task.id,
                            &file_task_id,
                            &file_name,
                            old_status_str,
                            new_status_str,
                        );
                    }
                }
            }

            break; // 找到对应的备份任务后退出循环
        }
    }

    /// 从下载任务获取 fs_id
    async fn get_download_task_fs_id(&self, download_task_id: &str) -> Option<String> {
        // 使用安全获取方法，处理 Weak 引用升级
        let download_mgr = self.get_download_manager();
        if let Some(ref dm) = download_mgr {
            if let Some(task) = dm.get_task(download_task_id).await {
                return Some(task.fs_id.to_string());
            }
        }
        None
    }

    /// 从下载任务获取 local_path（解密后的路径）
    ///
    /// 下载加密文件后会自动解密，解密完成后 DownloadTask.local_path 会更新为解密后的路径。
    /// 此方法用于获取最新的 local_path，确保去重记录中保存的是实际文件路径。
    async fn get_download_task_local_path(&self, download_task_id: &str) -> Option<std::path::PathBuf> {
        // 使用安全获取方法，处理 Weak 引用升级
        let download_mgr = self.get_download_manager();
        if let Some(ref dm) = download_mgr {
            if let Some(task) = dm.get_task(download_task_id).await {
                return Some(task.local_path.clone());
            }
        }
        None
    }

    /// 服务启动时恢复未完成的备份任务（带传输状态检查）
    ///
    /// 从数据库加载未完成的备份任务，检查关联的上传/下载任务状态，
    /// 并根据实际状态更新备份任务
    pub async fn restore_incomplete_tasks_with_transfer_check(&self) -> Result<usize> {
        tracing::info!("开始恢复未完成的备份任务...");

        // 从数据库加载未完成的任务
        let incomplete_tasks = self.persistence_manager.load_incomplete_tasks()?;
        let task_count = incomplete_tasks.len();

        if task_count == 0 {
            tracing::info!("没有需要恢复的备份任务");
            return Ok(0);
        }

        tracing::info!("发现 {} 个未完成的备份任务，开始恢复", task_count);

        // 使用安全获取方法，处理 Weak 引用升级
        let upload_manager = self.get_upload_manager();
        let download_manager = self.get_download_manager();

        for mut task in incomplete_tasks {
            tracing::info!(
                "恢复备份任务: id={}, config_id={}, status={:?}",
                task.id, task.config_id, task.status
            );

            // 加载文件任务
            let (file_tasks, _) = self.persistence_manager.load_file_tasks(&task.id, 10000, 0)?;
            task.pending_files = file_tasks;

            // 检查关联的上传/下载任务状态
            let mut updated = false;

            for file_task in &mut task.pending_files {
                if let Some(ref related_task_id) = file_task.related_task_id {
                    let is_upload = file_task.backup_operation_type == Some(BackupOperationType::Upload);

                    // 检查传输任务是否已完成
                    let transfer_completed = if is_upload {
                        if let Some(ref um) = upload_manager {
                            if let Some(upload_task) = um.get_task(related_task_id).await {
                                match upload_task.status {
                                    crate::uploader::task::UploadTaskStatus::Completed |
                                    crate::uploader::task::UploadTaskStatus::RapidUploadSuccess => Some(true),
                                    crate::uploader::task::UploadTaskStatus::Failed => Some(false),
                                    _ => None
                                }
                            } else {
                                Some(false) // 任务不存在，标记为失败
                            }
                        } else {
                            None
                        }
                    } else {
                        if let Some(ref dm) = download_manager {
                            if let Some(download_task) = dm.get_task(related_task_id).await {
                                match download_task.status {
                                    crate::downloader::task::TaskStatus::Completed => Some(true),
                                    crate::downloader::task::TaskStatus::Failed => Some(false),
                                    _ => None
                                }
                            } else {
                                Some(false)
                            }
                        } else {
                            None
                        }
                    };

                    if let Some(success) = transfer_completed {
                        if success && file_task.status != BackupFileStatus::Completed {
                            file_task.status = BackupFileStatus::Completed;
                            task.completed_count += 1;
                            task.transferred_bytes += file_task.file_size;
                            updated = true;
                        } else if !success && file_task.status != BackupFileStatus::Failed {
                            file_task.status = BackupFileStatus::Failed;
                            task.failed_count += 1;
                            updated = true;
                        }

                        if is_upload {
                            task.pending_upload_task_ids.remove(related_task_id);
                        } else {
                            task.pending_download_task_ids.remove(related_task_id);
                        }
                        task.transfer_task_map.remove(related_task_id);
                    } else {
                        if is_upload {
                            task.pending_upload_task_ids.insert(related_task_id.clone());
                        } else {
                            task.pending_download_task_ids.insert(related_task_id.clone());
                        }
                        task.transfer_task_map.insert(related_task_id.clone(), file_task.id.clone());
                    }
                }
            }

            let all_completed = task.pending_upload_task_ids.is_empty()
                && task.pending_download_task_ids.is_empty();

            if all_completed && updated {
                if task.failed_count == 0 {
                    task.status = BackupTaskStatus::Completed;
                } else if task.completed_count > 0 {
                    task.status = BackupTaskStatus::PartiallyCompleted;
                } else {
                    task.status = BackupTaskStatus::Failed;
                }
                task.completed_at = Some(Utc::now());
            } else if !all_completed {
                task.status = BackupTaskStatus::Transferring;
            }

            // 持久化任务状态
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::error!("持久化恢复的备份任务失败: {}", e);
            }

            // 🔥 内存优化：如果任务已进入终态，不插入 DashMap
            // 终态任务只保留在数据库中，查询时从数据库获取
            if all_completed && updated {
                tracing::info!(
                    "恢复时发现任务已完成，不加载到内存: task={}, status={:?}",
                    task.id, task.status
                );
            } else {
                self.tasks.insert(task.id.clone(), task.clone());
            }
        }

        tracing::info!("备份任务恢复完成，共恢复 {} 个任务", task_count);
        Ok(task_count)
    }

    /// 安全获取 WebSocket 管理器
    ///
    /// 从 Weak 引用升级为 Arc，如果原始对象已被销毁则返回 None
    pub fn get_ws_manager(&self) -> Option<Arc<WebSocketManager>> {
        self.ws_manager.read().as_ref().and_then(|weak| weak.upgrade())
    }

    /// 安全获取上传管理器引用
    ///
    /// 从 Weak 引用升级为 Arc，如果原始对象已被销毁则返回 None
    pub fn get_upload_manager(&self) -> Option<Arc<UploadManager>> {
        self.upload_manager.read().as_ref().and_then(|weak| weak.upgrade())
    }

    /// 安全获取下载管理器引用
    ///
    /// 从 Weak 引用升级为 Arc，如果原始对象已被销毁则返回 None
    pub fn get_download_manager(&self) -> Option<Arc<DownloadManager>> {
        self.download_manager.read().as_ref().and_then(|weak| weak.upgrade())
    }

    /// 发送备份事件到 WebSocket
    fn publish_event(&self, event: WsBackupEvent) {
        // 使用安全获取方法，处理 Weak 引用升级
        if let Some(ws_manager) = self.get_ws_manager() {
            ws_manager.send_if_subscribed(TaskEvent::Backup(event), None);
        }
    }

    /// 发送任务创建事件
    pub fn publish_task_created(&self, task: &BackupTask, config: &BackupConfig) {
        let direction = match config.direction {
            BackupDirection::Upload => "upload",
            BackupDirection::Download => "download",
        };
        let trigger_type = match task.trigger_type {
            TriggerType::Watch => "watch",
            TriggerType::Poll => "poll",
            TriggerType::Manual => "manual",
        };

        self.publish_event(WsBackupEvent::Created {
            task_id: task.id.clone(),
            config_id: config.id.clone(),
            config_name: config.name.clone(),
            direction: direction.to_string(),
            trigger_type: trigger_type.to_string(),
        });
    }

    /// 发送任务状态变更事件
    pub fn publish_status_changed(&self, task_id: &str, old_status: &str, new_status: &str) {
        self.publish_event(WsBackupEvent::StatusChanged {
            task_id: task_id.to_string(),
            old_status: old_status.to_string(),
            new_status: new_status.to_string(),
        });
    }

    /// 发送任务进度事件
    pub fn publish_progress(&self, task: &BackupTask) {
        self.publish_event(WsBackupEvent::Progress {
            task_id: task.id.clone(),
            completed_count: task.completed_count,
            failed_count: task.failed_count,
            skipped_count: task.skipped_count,
            total_count: task.total_count,
            transferred_bytes: task.transferred_bytes,
            total_bytes: task.total_bytes,
        });
    }

    /// 发送扫描进度事件
    pub fn publish_scan_progress(&self, task_id: &str, scanned_files: usize, scanned_dirs: usize) {
        self.publish_event(WsBackupEvent::ScanProgress {
            task_id: task_id.to_string(),
            scanned_files,
            scanned_dirs,
        });
    }

    /// 发送扫描完成事件
    pub fn publish_scan_completed(&self, task_id: &str, total_files: usize, total_bytes: u64) {
        self.publish_event(WsBackupEvent::ScanCompleted {
            task_id: task_id.to_string(),
            total_files,
            total_bytes,
        });
    }

    /// 发送任务完成事件
    pub fn publish_task_completed(&self, task: &BackupTask) {
        self.publish_event(WsBackupEvent::Completed {
            task_id: task.id.clone(),
            completed_at: task.completed_at.map(|t| t.timestamp()).unwrap_or_else(|| Utc::now().timestamp()),
            success_count: task.completed_count,
            failed_count: task.failed_count,
            skipped_count: task.skipped_count,
        });
    }

    /// 发送任务失败事件
    pub fn publish_task_failed(&self, task_id: &str, error: &str) {
        self.publish_event(WsBackupEvent::Failed {
            task_id: task_id.to_string(),
            error: error.to_string(),
        });
    }

    /// 发送任务暂停事件
    pub fn publish_task_paused(&self, task_id: &str) {
        self.publish_event(WsBackupEvent::Paused {
            task_id: task_id.to_string(),
        });
    }

    /// 发送任务恢复事件
    pub fn publish_task_resumed(&self, task_id: &str) {
        self.publish_event(WsBackupEvent::Resumed {
            task_id: task_id.to_string(),
        });
    }

    /// 发送任务取消事件
    pub fn publish_task_cancelled(&self, task_id: &str) {
        self.publish_event(WsBackupEvent::Cancelled {
            task_id: task_id.to_string(),
        });
    }

    // ==================== 内存清理 ====================

    /// 清理已完成的备份任务
    ///
    /// 当任务进入终态（Completed、PartiallyCompleted、Failed、Cancelled）后，
    /// 从内存 DashMap 中移除任务，释放内存。
    /// 任务数据已持久化到数据库，后续查询从数据库获取。
    ///
    /// # 参数
    /// - `task_id`: 要清理的任务ID
    ///
    /// # 清理流程
    /// 1. 持久化任务到数据库（确保数据不丢失）
    /// 2. 从 tasks DashMap 移除任务
    /// 3. 记录清理日志
    ///
    /// # Requirements
    /// - 3.1: 任务进入终态后从 DashMap 移除
    /// - 3.2: 移除前确保任务已持久化
    fn cleanup_completed_task(&self, task_id: &str) {
        // 步骤 1: 持久化任务到数据库
        // 先获取任务数据进行持久化，确保数据不丢失
        if let Some(task) = self.tasks.get(task_id) {
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::error!(
                    "内存清理: 持久化任务失败，跳过清理以防数据丢失: task_id={}, error={}",
                    task_id, e
                );
                return; // 持久化失败时不移除，防止数据丢失
            }
            tracing::debug!(
                "内存清理: 任务已持久化到数据库: task_id={}, status={:?}",
                task_id, task.status
            );
        }

        // 步骤 2 & 3: 从 DashMap 移除并记录日志
        Self::cleanup_completed_task_static(&self.tasks, task_id);
    }

    /// 清理已完成的备份任务（静态方法）
    ///
    /// 供静态方法调用，仅执行移除和日志记录。
    /// 注意：此方法不执行持久化，调用方需确保任务已持久化。
    ///
    /// # 参数
    /// - `tasks`: 任务 DashMap 引用
    /// - `task_id`: 要清理的任务ID
    fn cleanup_completed_task_static(tasks: &Arc<DashMap<String, BackupTask>>, task_id: &str) {
        // 从 DashMap 移除任务
        if let Some((_, removed_task)) = tasks.remove(task_id) {
            tracing::info!(
                "内存清理: 已从 DashMap 移除终态任务 {} (status={:?}, completed={}, failed={}, skipped={})",
                task_id,
                removed_task.status,
                removed_task.completed_count,
                removed_task.failed_count,
                removed_task.skipped_count
            );
        } else {
            tracing::debug!(
                "内存清理: 任务 {} 不在 DashMap 中（可能已被清理）",
                task_id
            );
        }
    }

    // ==================== 加密上传流程集成 ====================

    /// 处理单个文件的加密上传流程
    ///
    /// 流程顺序（参考 6.6 节）：
    /// 1. 去重检查（基于明文文件）
    /// 2. 创建快照记录
    /// 3. 加密文件到临时目录
    /// 4. 上传加密文件
    /// 5. 更新快照状态
    ///
    /// 返回：(是否需要上传, 加密后的临时文件路径, 加密文件名)
    pub async fn prepare_file_for_encrypted_upload(
        &self,
        task_id: &str,
        file_task_id: &str,
    ) -> Result<PrepareEncryptedUploadResult> {
        use crate::encryption::EncryptionService;
        use super::record::calculate_head_md5;

        // 获取文件任务信息
        let (file_task, config) = {
            let task = self.tasks.get(task_id)
                .ok_or_else(|| anyhow!("任务不存在: {}", task_id))?;
            let file_task = task.pending_files.iter()
                .find(|f| f.id == file_task_id)
                .ok_or_else(|| anyhow!("文件任务不存在: {}", file_task_id))?
                .clone();
            let config = self.get_config(&task.config_id)
                .ok_or_else(|| anyhow!("配置不存在: {}", task.config_id))?;
            (file_task, config)
        };

        // 更新文件状态为检查中
        self.update_file_task_internal_status(task_id, file_task_id, BackupFileStatus::Checking)?;

        // 阶段 1：去重检查（无论是否加密都需要检查）
        let file_name = file_task.local_path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("无效的文件名"))?;

        let relative_path = file_task.local_path.strip_prefix(&config.local_path)
            .map(|p| p.parent().unwrap_or(Path::new("")).to_string_lossy().to_string())
            .unwrap_or_default();

        let head_md5 = calculate_head_md5(&file_task.local_path)?;

        let (exists, _stored_md5) = self.record_manager.check_upload_record_preliminary(
            &config.id,
            &relative_path,
            file_name,
            file_task.file_size,
            &head_md5,
        )?;

        if exists {
            // 文件已存在，跳过
            self.update_file_task_skip(task_id, file_task_id, SkipReason::AlreadyExists)?;
            tracing::debug!("文件去重跳过: {} (config={}, size={}, head_md5={})",
                file_name, config.id, file_task.file_size, head_md5);
            return Ok(PrepareEncryptedUploadResult {
                should_upload: false,
                skip_reason: Some("文件已备份（去重）".to_string()),
                encrypted_path: None,
                encrypted_name: None,
                original_remote_path: file_task.remote_path.clone(),
            });
        }

        // 如果不需要加密，直接返回（去重检查已通过）
        if !file_task.encrypted {
            tracing::debug!("文件需要上传（非加密模式）: {} (config={}, size={})",
                file_name, config.id, file_task.file_size);
            return Ok(PrepareEncryptedUploadResult {
                should_upload: true,
                skip_reason: None,
                encrypted_path: None,
                encrypted_name: None,
                original_remote_path: file_task.remote_path.clone(),
            });
        }

        // 阶段 2：获取加密服务
        let encryption_service = {
            let service_guard = self.encryption_service.read();
            service_guard.as_ref()
                .ok_or_else(|| anyhow!("加密服务未配置"))?
                .clone()
        };

        // 查询是否已存在加密映射，存在则复用
        let encrypted_name = match self.record_manager.find_snapshot_by_original(&relative_path, file_name)? {
            Some(snapshot) => {
                tracing::debug!(
                    "复用已存在的加密映射: {} -> {}",
                    file_name, snapshot.encrypted_name
                );
                snapshot.encrypted_name
            }
            None => {
                // 生成新的加密文件名
                EncryptionService::generate_encrypted_filename()
            }
        };

        // 阶段 3：创建快照记录
        let encryption_config = self.encryption_config.read();
        let algorithm_str = match encryption_config.algorithm {
            EncryptionAlgorithm::Aes256Gcm => "aes256gcm",
            EncryptionAlgorithm::ChaCha20Poly1305 => "chacha20poly1305",
        };
        let key_version = encryption_config.key_version;
        drop(encryption_config);

        tracing::info!(
            "自动备份加密上传: file={}, encrypted_name={}, key_version={}, algorithm={}",
            file_name, encrypted_name, key_version, algorithm_str
        );

        // 计算加密后的远程路径
        let remote_dir = file_task.remote_path.rsplit_once('/')
            .map(|(dir, _)| dir)
            .unwrap_or(&config.remote_path);
        let encrypted_remote_path = format!("{}/{}", remote_dir, encrypted_name);

        // 创建快照（先用占位 nonce，加密后更新）
        self.snapshot_manager.create_snapshot(
            &config.id,
            &relative_path,
            file_name,
            &encrypted_name,
            file_task.file_size,
            "pending", // 占位，加密后更新
            algorithm_str,
            1,
            key_version,
            &encrypted_remote_path,
        )?;

        // 更新快照状态为加密中
        self.snapshot_manager.mark_encrypting(&encrypted_name)?;

        // 更新文件状态为加密中
        self.update_file_task_internal_status(task_id, file_task_id, BackupFileStatus::Encrypting)?;

        // 发送加密开始事件
        self.publish_event(WsBackupEvent::FileEncrypting {
            task_id: task_id.to_string(),
            file_task_id: file_task_id.to_string(),
            file_name: file_name.to_string(),
        });

        // 阶段 4：加密文件到临时目录（带进度回调）
        let temp_encrypted_path = self.temp_dir.join(&encrypted_name);

        // 克隆必要的数据用于进度回调闭包
        let ws_manager_clone = self.ws_manager.clone();
        let task_id_clone = task_id.to_string();
        let file_task_id_clone = file_task_id.to_string();
        let file_name_clone = file_name.to_string();

        let metadata = encryption_service.encrypt_file_with_progress(
            &file_task.local_path,
            &temp_encrypted_path,
            move |processed_bytes, total_bytes| {
                // 计算进度百分比
                let progress = if total_bytes > 0 {
                    (processed_bytes as f64 / total_bytes as f64) * 100.0
                } else {
                    0.0
                };

                // 发送加密进度事件
                // 使用 Weak 引用升级获取 Arc
                let ws = ws_manager_clone.read();
                if let Some(ref weak) = *ws {
                    if let Some(ws_manager) = weak.upgrade() {
                        ws_manager.send_if_subscribed(
                            TaskEvent::Backup(WsBackupEvent::FileEncryptProgress {
                                task_id: task_id_clone.clone(),
                                file_task_id: file_task_id_clone.clone(),
                                file_name: file_name_clone.clone(),
                                progress,
                                processed_bytes,
                                total_bytes,
                            }),
                            None,
                        );
                    }
                }
            },
        )?;

        // 更新文件任务的加密信息
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                ft.encrypted_name = Some(encrypted_name.clone());
                ft.temp_encrypted_path = Some(temp_encrypted_path.clone());
                ft.updated_at = Utc::now();
            }
        }

        // 更新快照状态为上传中
        self.snapshot_manager.mark_uploading(&encrypted_name)?;

        // 发送加密完成事件
        self.publish_event(WsBackupEvent::FileEncrypted {
            task_id: task_id.to_string(),
            file_task_id: file_task_id.to_string(),
            file_name: file_name.to_string(),
            encrypted_name: encrypted_name.clone(),
            encrypted_size: metadata.encrypted_size,
        });

        tracing::info!(
            "文件加密完成: {} -> {} (原始: {} bytes, 加密后: {} bytes)",
            file_name, encrypted_name, file_task.file_size, metadata.encrypted_size
        );

        Ok(PrepareEncryptedUploadResult {
            should_upload: true,
            skip_reason: None,
            encrypted_path: Some(temp_encrypted_path),
            encrypted_name: Some(encrypted_name),
            original_remote_path: encrypted_remote_path,
        })
    }

    /// 完成加密上传后的清理工作
    ///
    /// 上传成功后调用，更新快照状态并清理临时文件
    pub fn complete_encrypted_upload(
        &self,
        task_id: &str,
        file_task_id: &str,
        success: bool,
    ) -> Result<()> {
        // 获取文件任务信息
        let encrypted_name = {
            let task = self.tasks.get(task_id)
                .ok_or_else(|| anyhow!("任务不存在: {}", task_id))?;
            let file_task = task.pending_files.iter()
                .find(|f| f.id == file_task_id)
                .ok_or_else(|| anyhow!("文件任务不存在: {}", file_task_id))?;
            file_task.encrypted_name.clone()
        };

        if let Some(ref name) = encrypted_name {
            if success {
                // 更新快照状态为已完成
                self.snapshot_manager.mark_completed(name)?;
            } else {
                // 更新快照状态为失败
                self.snapshot_manager.mark_failed(name)?;
            }
        }

        // 清理临时加密文件
        if let Some(task) = self.tasks.get(task_id) {
            if let Some(file_task) = task.pending_files.iter().find(|f| f.id == file_task_id) {
                if let Some(ref temp_path) = file_task.temp_encrypted_path {
                    if temp_path.exists() {
                        if let Err(e) = std::fs::remove_file(temp_path) {
                            tracing::warn!("清理临时加密文件失败: {:?} - {}", temp_path, e);
                        } else {
                            tracing::debug!("已清理临时加密文件: {:?}", temp_path);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 内部方法：更新文件任务状态（不触发完成计数）
    fn update_file_task_internal_status(
        &self,
        task_id: &str,
        file_task_id: &str,
        status: BackupFileStatus,
    ) -> Result<()> {
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                file_task.status = status;
                file_task.updated_at = Utc::now();
                Ok(())
            } else {
                Err(anyhow!("文件任务不存在: {}", file_task_id))
            }
        } else {
            Err(anyhow!("任务不存在: {}", task_id))
        }
    }

    /// 内部方法：标记文件任务为跳过
    fn update_file_task_skip(
        &self,
        task_id: &str,
        file_task_id: &str,
        reason: SkipReason,
    ) -> Result<()> {
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            if let Some(file_task) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                file_task.status = BackupFileStatus::Skipped;
                file_task.skip_reason = Some(reason);
                file_task.updated_at = Utc::now();
                task.skipped_count += 1;
                Ok(())
            } else {
                Err(anyhow!("文件任务不存在: {}", file_task_id))
            }
        } else {
            Err(anyhow!("任务不存在: {}", task_id))
        }
    }

    /// 获取加密服务（用于外部调用）
    pub fn get_encryption_service(&self) -> Option<EncryptionService> {
        self.encryption_service.read().clone()
    }

    /// 获取快照管理器引用
    pub fn get_snapshot_manager(&self) -> Arc<SnapshotManager> {
        self.snapshot_manager.clone()
    }

    /// 获取加密配置存储引用
    pub fn get_encryption_config_store(&self) -> Arc<EncryptionConfigStore> {
        self.encryption_config_store.clone()
    }

    /// 获取记录管理器引用
    pub fn get_record_manager(&self) -> Arc<BackupRecordManager> {
        self.record_manager.clone()
    }

    /// 获取临时目录路径
    pub fn get_temp_dir(&self) -> &Path {
        &self.temp_dir
    }

    // ==================== 解密下载流程集成 ====================

    /// 准备解密下载
    ///
    /// 检查文件是否为加密文件，如果是则准备解密流程
    /// 返回解密所需的信息
    pub fn prepare_decrypted_download(
        &self,
        remote_file_name: &str,
        local_save_dir: &Path,
    ) -> Result<PrepareDecryptedDownloadResult> {
        use crate::encryption::EncryptionService;

        // 检查是否为加密文件名
        if !EncryptionService::is_encrypted_filename(remote_file_name) {
            return Ok(PrepareDecryptedDownloadResult {
                is_encrypted: false,
                original_name: None,
                original_path: None,
                temp_download_path: None,
                snapshot_info: None,
            });
        }

        // 查找快照信息
        let snapshot_info = self.snapshot_manager.find_by_encrypted_name(remote_file_name)?;

        match snapshot_info {
            Some(info) => {
                // 有快照信息，可以恢复原始文件名和路径
                let original_path = local_save_dir.join(&info.original_name);
                let temp_download_path = self.temp_dir.join(remote_file_name);

                Ok(PrepareDecryptedDownloadResult {
                    is_encrypted: true,
                    original_name: Some(info.original_name.clone()),
                    original_path: Some(original_path),
                    temp_download_path: Some(temp_download_path),
                    snapshot_info: Some(info),
                })
            }
            None => {
                // 没有快照信息（可能是跨设备场景）
                // 仍然可以解密，但无法恢复原始文件名
                let temp_download_path = self.temp_dir.join(remote_file_name);
                // 使用 UUID 作为解密后的文件名（去掉 .dat 扩展名）
                let decrypted_name = remote_file_name
                    .strip_suffix(crate::encryption::ENCRYPTED_FILE_EXTENSION)
                    .unwrap_or(remote_file_name);

                Ok(PrepareDecryptedDownloadResult {
                    is_encrypted: true,
                    original_name: Some(decrypted_name.to_string()),
                    original_path: Some(local_save_dir.join(decrypted_name)),
                    temp_download_path: Some(temp_download_path),
                    snapshot_info: None,
                })
            }
        }
    }

    /// 执行解密下载后的解密操作
    ///
    /// 下载完成后调用，将加密文件解密到目标路径
    /// 当加密服务未配置时，跳过解密并保留加密文件
    pub fn decrypt_downloaded_file(
        &self,
        task_id: &str,
        file_task_id: &str,
        encrypted_file_path: &Path,
        target_path: &Path,
    ) -> Result<DecryptDownloadResult> {
        // 获取加密服务
        let encryption_service = {
            let service_guard = self.encryption_service.read();
            match service_guard.as_ref() {
                Some(service) => service.clone(),
                None => {
                    // 🔥 密钥缺失时跳过解密，保留加密文件
                    tracing::warn!(
                        "任务 {} 文件 {} 是加密文件但未配置加密服务，跳过解密，保留加密文件",
                        task_id, file_task_id
                    );
                    return Ok(DecryptDownloadResult {
                        success: false,
                        decrypted_path: None,
                        original_size: None,
                        error: Some("加密服务未配置，已跳过解密，保留加密文件".to_string()),
                    });
                }
            }
        };

        // 获取文件名用于事件
        let file_name = encrypted_file_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        // 发送解密开始事件
        self.publish_event(WsBackupEvent::FileDecrypting {
            task_id: task_id.to_string(),
            file_task_id: file_task_id.to_string(),
            file_name: file_name.clone(),
        });

        // 更新文件状态
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            if let Some(ft) = task.pending_files.iter_mut().find(|f| f.id == file_task_id) {
                ft.status = BackupFileStatus::Encrypting; // 复用 Encrypting 状态表示解密中
                ft.updated_at = Utc::now();
            }
        }

        // 确保目标目录存在
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 克隆必要的数据用于进度回调闭包
        let ws_manager_clone = self.ws_manager.clone();
        let task_id_clone = task_id.to_string();
        let file_task_id_clone = file_task_id.to_string();
        let file_name_clone = file_name.clone();

        // 执行解密（带进度回调）
        match encryption_service.decrypt_file_with_progress(
            encrypted_file_path,
            target_path,
            move |processed_bytes, total_bytes| {
                // 计算进度百分比
                let progress = if total_bytes > 0 {
                    (processed_bytes as f64 / total_bytes as f64) * 100.0
                } else {
                    0.0
                };

                // 发送解密进度事件
                // 使用 Weak 引用升级获取 Arc
                let ws = ws_manager_clone.read();
                if let Some(ref weak) = *ws {
                    if let Some(ws_manager) = weak.upgrade() {
                        ws_manager.send_if_subscribed(
                            TaskEvent::Backup(WsBackupEvent::FileDecryptProgress {
                                task_id: task_id_clone.clone(),
                                file_task_id: file_task_id_clone.clone(),
                                file_name: file_name_clone.clone(),
                                progress,
                                processed_bytes,
                                total_bytes,
                            }),
                            None,
                        );
                    }
                }
            },
        ) {
            Ok(original_size) => {
                // 获取原始文件名
                let original_name = target_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                // 发送解密完成事件
                self.publish_event(WsBackupEvent::FileDecrypted {
                    task_id: task_id.to_string(),
                    file_task_id: file_task_id.to_string(),
                    file_name,
                    original_name: original_name.clone(),
                    original_size,
                });

                // 清理临时加密文件
                if encrypted_file_path.exists() {
                    if let Err(e) = std::fs::remove_file(encrypted_file_path) {
                        tracing::warn!("清理临时加密文件失败: {:?} - {}", encrypted_file_path, e);
                    }
                }

                tracing::info!(
                    "文件解密完成: {:?} -> {:?} (原始大小: {} bytes)",
                    encrypted_file_path, target_path, original_size
                );

                Ok(DecryptDownloadResult {
                    success: true,
                    decrypted_path: Some(target_path.to_path_buf()),
                    original_size: Some(original_size),
                    error: None,
                })
            }
            Err(e) => {
                let error_msg = format!("解密失败: {}", e);
                tracing::error!("{}", error_msg);

                Ok(DecryptDownloadResult {
                    success: false,
                    decrypted_path: None,
                    original_size: None,
                    error: Some(error_msg),
                })
            }
        }
    }

    /// 检查远程文件是否为加密文件
    pub fn is_encrypted_remote_file(&self, remote_file_name: &str) -> bool {
        crate::encryption::EncryptionService::is_encrypted_filename(remote_file_name)
    }

    // ==================== 优雅关闭 ====================

    /// 优雅关闭管理器
    ///
    /// 执行以下步骤：
    /// 1. 停止接收新任务
    /// 2. 暂停所有正在执行的任务
    /// 3. 持久化所有任务状态
    /// 4. 停止所有文件监听器
    /// 5. 停止所有轮询调度器
    /// 6. 清理临时文件
    /// 7. 取消聚合器任务
    pub async fn shutdown(&self) -> ShutdownResult {
        tracing::info!("开始优雅关闭自动备份管理器...");

        let mut result = ShutdownResult {
            success: true,
            saved_tasks: 0,
            stopped_watchers: 0,
            stopped_schedulers: 0,
            cleaned_temp_files: 0,
            errors: Vec::new(),
        };

        // 0. 取消聚合器任务
        tracing::info!("取消聚合器任务...");
        {
            let mut handle_guard = self.aggregator_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
                match handle.await {
                    Ok(_) => tracing::info!("聚合器任务正常结束"),
                    Err(e) if e.is_cancelled() => tracing::info!("聚合器任务已取消"),
                    Err(e) => {
                        tracing::warn!("聚合器任务异常结束: {}", e);
                        result.errors.push(format!("聚合器任务异常结束: {}", e));
                    }
                }
            }
        }

        // 1. 暂停所有正在执行的任务
        tracing::info!("暂停所有正在执行的任务...");
        for task_ref in self.tasks.iter() {
            let task = task_ref.value();
            match task.status {
                BackupTaskStatus::Preparing | BackupTaskStatus::Transferring => {
                    // 通知任务控制器暂停
                    if let Some(controller) = self.task_controllers.get(&task.config_id) {
                        controller.request_pause();
                    }
                }
                _ => {}
            }
        }

        // 2. 持久化所有任务状态
        tracing::info!("持久化任务状态...");
        for task_ref in self.tasks.iter() {
            let task = task_ref.value();
            match self.persistence_manager.save_task(task) {
                Ok(_) => result.saved_tasks += 1,
                Err(e) => {
                    result.errors.push(format!("保存任务 {} 失败: {}", task.id, e));
                }
            }
        }

        // 3. 停止所有文件监听器
        tracing::info!("停止文件监听器...");
        {
            let mut watcher_guard = self.file_watcher.write();
            if let Some(ref mut watcher) = *watcher_guard {
                let paths = watcher.get_watched_paths();
                result.stopped_watchers = paths.len();
                for (path, _config_id) in paths {
                    if let Err(e) = watcher.unwatch(&path) {
                        result.errors.push(format!("停止监听 {:?} 失败: {}", path, e));
                    }
                }
            }
        }

        // 4. 停止所有轮询调度器
        tracing::info!("停止轮询调度器...");
        {
            let mut scheduler_guard = self.poll_scheduler.write();
            if let Some(ref mut scheduler) = *scheduler_guard {
                // 获取所有配置ID并移除调度
                let config_ids: Vec<String> = self.configs.iter().map(|r| r.key().clone()).collect();
                result.stopped_schedulers = config_ids.len();
                for config_id in config_ids {
                    scheduler.remove_schedule(&config_id);
                }
            }
        }

        // 5. 清理残留的临时文件
        tracing::info!("清理临时文件...");
        match std::fs::read_dir(&self.temp_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            // 只清理备份相关的临时文件
                            if name.ends_with(".bkup.tmp") || name.starts_with("backup_") {
                                match std::fs::remove_file(&path) {
                                    Ok(_) => result.cleaned_temp_files += 1,
                                    Err(e) => {
                                        result.errors.push(format!("删除临时文件 {:?} 失败: {}", path, e));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                result.errors.push(format!("读取临时目录失败: {}", e));
            }
        }

        // 6. 保存配置
        tracing::info!("保存配置...");
        if let Err(e) = self.save_configs().await {
            result.errors.push(format!("保存配置失败: {}", e));
            result.success = false;
        }

        if result.errors.is_empty() {
            tracing::info!(
                "自动备份管理器已优雅关闭: 保存了 {} 个任务, 停止了 {} 个监听器和 {} 个调度器, 清理了 {} 个临时文件",
                result.saved_tasks, result.stopped_watchers, result.stopped_schedulers, result.cleaned_temp_files
            );
        } else {
            tracing::warn!(
                "自动备份管理器关闭时遇到 {} 个错误",
                result.errors.len()
            );
            result.success = false;
        }

        result
    }

    /// 检查是否正在关闭
    pub fn is_shutting_down(&self) -> bool {
        // 检查所有任务控制器是否都已请求暂停
        self.task_controllers.iter().all(|c| c.value().is_pause_requested())
    }

    /// 获取加密文件的原始文件信息
    ///
    /// 用于在文件列表中显示原始文件名而非加密文件名
    pub fn get_encrypted_file_display_info(
        &self,
        encrypted_name: &str,
    ) -> Result<crate::encryption::snapshot::FileDisplayInfo> {
        crate::encryption::snapshot::get_file_display_info(&self.snapshot_manager, encrypted_name)
    }

    // ==================== 扫描与增量合并方法 ====================

    /// 扫描本地目录获取需要备份的文件列表
    ///
    /// 应用过滤规则和去重检查，返回需要上传的文件任务列表
    async fn scan_local_directory_for_backup(&self, config: &BackupConfig) -> Result<Vec<BackupFileTask>> {
        use crate::uploader::{BatchedScanIterator, ScanOptions};

        tracing::info!("扫描本地目录: config={}, path={:?}", config.id, config.local_path);

        let scan_options = ScanOptions {
            follow_symlinks: false,
            max_file_size: if config.filter_config.max_file_size > 0 {
                Some(config.filter_config.max_file_size)
            } else {
                None
            },
            max_files: None,
            skip_hidden: true,
            allowed_paths: vec![],
        };

        // 使用 BatchedScanIterator 分批扫描（通过 channel + spawn_blocking）
        let local_path = config.local_path.clone();
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(4);

        let opts_clone = scan_options.clone();
        tokio::task::spawn_blocking(move || {
            let mut iterator = match BatchedScanIterator::new(&local_path, opts_clone) {
                Ok(it) => it,
                Err(e) => {
                    tracing::error!("创建备份扫描迭代器失败: {}", e);
                    return;
                }
            };
            loop {
                match iterator.next_batch() {
                    Ok(Some(batch)) => {
                        if batch_tx.blocking_send(batch).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("备份扫描批次失败: {}", e);
                        break;
                    }
                }
            }
        });

        let include_exts = &config.filter_config.include_extensions;
        let exclude_exts = &config.filter_config.exclude_extensions;
        let exclude_dirs = &config.filter_config.exclude_directories;
        let min_file_size = config.filter_config.min_file_size;

        let mut file_tasks = Vec::new();
        let scan_cache = Arc::clone(&self.scan_cache_manager);
        let config_id_for_cache = config.id.clone();

        while let Some(batch) = batch_rx.recv().await {
            // 第一步：按扩展名/目录/大小过滤，同时收集 mtime
            let mut filtered: Vec<(crate::uploader::folder::ScannedFile, i64)> = Vec::new();
            for scanned_file in batch {
                let file_ext = scanned_file.local_path.extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_lowercase())
                    .unwrap_or_default();

                if !include_exts.is_empty() && !include_exts.iter().any(|e| e.to_lowercase() == file_ext) {
                    continue;
                }
                if exclude_exts.iter().any(|e| e.to_lowercase() == file_ext) {
                    continue;
                }
                let relative_str = scanned_file.relative_path.to_string_lossy();
                if exclude_dirs.iter().any(|d| relative_str.contains(d)) {
                    continue;
                }
                if scanned_file.size < min_file_size {
                    continue;
                }

                // 读取 mtime 用于增量缓存比对
                let mtime = std::fs::metadata(&scanned_file.local_path)
                    .and_then(|m| m.modified())
                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                    .unwrap_or(0);
                filtered.push((scanned_file, mtime));
            }

            // 第二步：增量缓存比对（仅处理变化/新增文件）
            let scan_metas: Vec<super::scan_cache::ScannedFileMeta> = filtered.iter()
                .map(|(f, mtime)| super::scan_cache::ScannedFileMeta {
                    file_path: f.local_path.to_string_lossy().to_string(),
                    mtime: *mtime,
                    size: f.size as i64,
                })
                .collect();

            let cache_ref = Arc::clone(&scan_cache);
            let cfg_id = config_id_for_cache.clone();
            let changed_set = tokio::task::spawn_blocking(move || {
                cache_ref.find_changed_files(&cfg_id, &scan_metas)
            }).await.unwrap_or_else(|_| Ok(Vec::new())).unwrap_or_default();

            let changed_paths: std::collections::HashSet<String> = changed_set.iter()
                .map(|m| m.file_path.clone())
                .collect();

            // 第三步：仅对变化文件执行 head_md5 + 去重检查
            let mut cache_entries = Vec::new();
            for (scanned_file, mtime) in &filtered {
                let file_path_str = scanned_file.local_path.to_string_lossy().to_string();
                if !changed_paths.contains(&file_path_str) {
                    continue;
                }

                let file_name = scanned_file.local_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                let relative_path = scanned_file.local_path.strip_prefix(&config.local_path)
                    .map(|p| p.parent().unwrap_or(std::path::Path::new("")).to_string_lossy().to_string())
                    .unwrap_or_default();

                let head_md5 = match calculate_head_md5(&scanned_file.local_path) {
                    Ok(md5) => md5,
                    Err(e) => {
                        tracing::warn!("计算文件头MD5失败，跳过去重检查: {:?}, error={}", scanned_file.local_path, e);
                        "unknown".to_string()
                    }
                };

                let (exists, _) = match self.record_manager.check_upload_record_preliminary(
                    &config.id,
                    &relative_path,
                    &file_name,
                    scanned_file.size,
                    &head_md5,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        tracing::warn!("查询去重记录失败: {:?}, error={}", scanned_file.local_path, e);
                        (false, None)
                    }
                };

                // 更新缓存条目（无论是否去重命中）
                cache_entries.push(super::scan_cache::CachedFileEntry {
                    config_id: config_id_for_cache.clone(),
                    file_path: file_path_str.clone(),
                    relative_path: scanned_file.relative_path.to_string_lossy().to_string(),
                    mtime: *mtime,
                    size: scanned_file.size as i64,
                    head_md5: Some(head_md5.clone()),
                    last_scan_at: chrono::Utc::now().timestamp(),
                });

                if exists {
                    tracing::debug!("文件已备份，跳过: {} (size={}, md5={})", file_name, scanned_file.size, head_md5);
                    continue;
                }

                // 计算远程路径
                let remote_path = format!("{}/{}",
                                          config.remote_path.trim_end_matches('/'),
                                          scanned_file.relative_path.to_string_lossy().replace('\\', "/"));

                let file_task = BackupFileTask {
                    id: Uuid::new_v4().to_string(),
                    parent_task_id: String::new(), // 稍后设置
                    local_path: scanned_file.local_path.clone(),
                    remote_path,
                    file_size: scanned_file.size,
                    head_md5: Some(head_md5),
                    fs_id: None,
                    status: BackupFileStatus::Pending,
                    sub_phase: None,
                    skip_reason: None,
                    encrypted: config.encrypt_enabled,
                    encrypted_name: None,
                    temp_encrypted_path: None,
                    transferred_bytes: 0,
                    decrypt_progress: None,
                    error_message: None,
                    retry_count: 0,
                    related_task_id: None,
                    backup_operation_type: Some(BackupOperationType::Upload),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                file_tasks.push(file_task);
            }

            // 第四步：批量更新扫描缓存
            if !cache_entries.is_empty() {
                let cache_ref = Arc::clone(&scan_cache);
                let entries = cache_entries;
                let _ = tokio::task::spawn_blocking(move || {
                    cache_ref.batch_upsert(entries)
                }).await;
            }

            // 让出执行权，避免长时间阻塞
            tokio::task::yield_now().await;
        }

        tracing::info!("本地目录扫描完成: config={}, 发现 {} 个新文件", config.id, file_tasks.len());
        Ok(file_tasks)
    }

    /// 扫描远程目录获取需要下载的文件列表
    ///
    /// 应用过滤规则和去重检查，返回需要下载的文件任务列表
    async fn scan_remote_directory_for_backup(&self, config: &BackupConfig) -> Result<Vec<BackupFileTask>> {
        use crate::auth::SessionManager;

        tracing::info!("扫描远程目录: config={}, path={}", config.id, config.remote_path);

        // 创建网盘客户端
        let mut session_manager = SessionManager::new(None);
        let session = session_manager.load_session().await?
            .ok_or_else(|| anyhow!("未登录"))?;
        let client = self.create_netdisk_client(session)?;

        let mut all_files = Vec::new();
        let mut dirs_to_scan = vec![config.remote_path.clone()];

        // 递归扫描远程目录
        while let Some(current_dir) = dirs_to_scan.pop() {
            let mut page = 1;
            loop {
                match client.get_file_list(&current_dir, page, 1000).await {
                    Ok(response) => {
                        if response.errno != 0 || response.list.is_empty() {
                            break;
                        }

                        for item in response.list {
                            if item.is_directory() {
                                dirs_to_scan.push(item.path.clone());
                            } else {
                                // 应用过滤规则
                                let file_ext = std::path::Path::new(&item.server_filename)
                                    .extension()
                                    .and_then(|e| e.to_str())
                                    .map(|e| e.to_lowercase())
                                    .unwrap_or_default();

                                if !config.filter_config.include_extensions.is_empty()
                                    && !config.filter_config.include_extensions.iter().any(|e| e.to_lowercase() == file_ext)
                                {
                                    continue;
                                }

                                if config.filter_config.exclude_extensions.iter().any(|e| e.to_lowercase() == file_ext) {
                                    continue;
                                }

                                if item.size < config.filter_config.min_file_size {
                                    continue;
                                }
                                if config.filter_config.max_file_size > 0 && item.size > config.filter_config.max_file_size {
                                    continue;
                                }

                                all_files.push(item);
                            }
                        }
                        page += 1;
                    }
                    Err(e) => {
                        tracing::error!("扫描远程目录失败: dir={}, error={}", current_dir, e);
                        break;
                    }
                }
            }
        }

        // 创建文件任务列表
        let mut file_tasks = Vec::new();

        for file_item in all_files {
            let relative_path = file_item.path
                .strip_prefix(&config.remote_path)
                .unwrap_or(&file_item.path)
                .trim_start_matches('/');

            // 🔥 解密加密文件夹路径
            let decrypted_relative_path = self.decrypt_folder_path(
                &config.remote_path,
                &file_item.path,
            ).unwrap_or_else(|_| relative_path.to_string());

            let local_path = config.local_path.join(&decrypted_relative_path);
            let file_name = local_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            // 去重检查
            let exists = match self.record_manager.check_download_record(
                &config.id,
                &file_item.path,
                &file_name,
                file_item.size,
                &file_item.fs_id.to_string(),
            ) {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!("查询下载去重记录失败: {}, error={}", file_item.path, e);
                    false
                }
            };

            if exists {
                tracing::debug!("文件已下载，跳过: {} (size={}, fs_id={})", file_name, file_item.size, file_item.fs_id);
                continue;
            }

            let file_task = BackupFileTask {
                id: Uuid::new_v4().to_string(),
                parent_task_id: String::new(),
                local_path,
                remote_path: file_item.path.clone(),
                file_size: file_item.size,
                head_md5: None,
                fs_id: Some(file_item.fs_id),
                status: BackupFileStatus::Pending,
                sub_phase: None,
                skip_reason: None,
                encrypted: config.encrypt_enabled,
                encrypted_name: None,
                temp_encrypted_path: None,
                transferred_bytes: 0,
                decrypt_progress: None,
                error_message: None,
                retry_count: 0,
                related_task_id: None,
                backup_operation_type: Some(BackupOperationType::Download),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            file_tasks.push(file_task);
        }

        tracing::info!("远程目录扫描完成: config={}, 发现 {} 个新文件", config.id, file_tasks.len());
        Ok(file_tasks)
    }

    /// 增量合并新文件到现有传输任务
    ///
    /// 当任务正在传输时，将新扫描到的文件合并到现有任务中
    ///
    /// 🔥 关键：过滤掉当前任务已有的文件，避免重复加入和统计错误
    async fn merge_new_files_to_task(
        &self,
        task_id: &str,
        config: &BackupConfig,
        mut new_files: Vec<BackupFileTask>,
    ) -> Result<()> {
        if new_files.is_empty() {
            return Ok(());
        }

        // 🔥 问题1和2修复：过滤掉当前任务已有的文件
        // 收集当前任务中已有文件的唯一标识（local_path + remote_path）
        let existing_file_keys: std::collections::HashSet<String> = {
            if let Some(task) = self.tasks.get(task_id) {
                task.pending_files.iter()
                    .map(|f| format!("{}|{}", f.local_path.display(), f.remote_path))
                    .collect()
            } else {
                std::collections::HashSet::new()
            }
        };

        // 过滤掉已存在的文件
        let original_count = new_files.len();
        new_files.retain(|f| {
            let key = format!("{}|{}", f.local_path.display(), f.remote_path);
            !existing_file_keys.contains(&key)
        });

        let filtered_count = original_count - new_files.len();
        if filtered_count > 0 {
            tracing::info!(
                "增量合并去重: task={}, 原始 {} 个文件, 过滤掉 {} 个已存在文件, 剩余 {} 个新文件",
                task_id, original_count, filtered_count, new_files.len()
            );
        }

        // 过滤后如果没有新文件，直接返回
        if new_files.is_empty() {
            tracing::info!("增量合并: task={}, 没有新文件需要合并", task_id);
            return Ok(());
        }

        let new_file_count = new_files.len();
        let new_total_bytes: u64 = new_files.iter().map(|f| f.file_size).sum();

        // 设置 parent_task_id
        for file_task in &mut new_files {
            file_task.parent_task_id = task_id.to_string();
        }

        // 批量保存新文件任务到数据库
        if let Err(e) = self.persistence_manager.save_file_tasks_batch(&new_files, &config.id) {
            tracing::warn!("批量保存增量文件任务到DB失败: {}", e);
        }

        // 更新任务统计
        if let Some(mut task) = self.tasks.get_mut(task_id) {
            task.pending_files.extend(new_files.clone());
            task.total_count += new_file_count;
            task.total_bytes += new_total_bytes;

            tracing::info!(
                "增量合并完成: task={}, 新增 {} 个文件 ({} bytes), 总计 {} 个文件 ({} bytes)",
                task_id, new_file_count, new_total_bytes, task.total_count, task.total_bytes
            );

            // 持久化任务
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::warn!("持久化增量合并后的任务失败: {}", e);
            }

            // 发送进度事件
            Self::publish_progress_static(&self.ws_manager, &task);
        }

        // 为新文件创建传输任务
        match config.direction {
            BackupDirection::Upload => {
                self.create_upload_tasks_for_files(task_id, config, new_files).await?;
            }
            BackupDirection::Download => {
                self.create_download_tasks_for_files(task_id, config, new_files).await?;
            }
        }

        Ok(())
    }

    /// 使用已扫描的文件列表执行上传备份
    async fn execute_upload_backup_with_files(
        &self,
        task_id: String,
        config: BackupConfig,
        mut file_tasks: Vec<BackupFileTask>,
    ) -> Result<()> {
        tracing::info!(
            "🔥 execute_upload_backup_with_files 开始: task={}, files={}",
            task_id, file_tasks.len()
        );

        let file_count = file_tasks.len();
        let total_bytes: u64 = file_tasks.iter().map(|f| f.file_size).sum();

        // 设置 parent_task_id
        for file_task in &mut file_tasks {
            file_task.parent_task_id = task_id.clone();
        }

        // 更新任务状态为准备中
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.status = BackupTaskStatus::Preparing;
            task.started_at = Some(Utc::now());
        }
        Self::publish_status_changed_static(&self.ws_manager, &task_id, "queued", "preparing");

        // 批量保存文件任务到数据库
        if !file_tasks.is_empty() {
            if let Err(e) = self.persistence_manager.save_file_tasks_batch(&file_tasks, &config.id) {
                tracing::warn!("批量保存文件任务到DB失败: {}", e);
            }
        }

        // 🔥 优化：直接移动 file_tasks 到 task.pending_files，避免不必要的 clone
        let is_empty = file_tasks.is_empty();

        // 更新任务
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.pending_files = file_tasks; // 直接移动，不再 clone
            task.total_count = file_count;
            task.total_bytes = total_bytes;
            task.status = BackupTaskStatus::Transferring;
        }

        // 持久化任务
        if let Some(task) = self.tasks.get(&task_id) {
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::warn!("持久化备份任务失败: {}", e);
            }
        }

        // 如果没有文件需要备份，直接完成
        if is_empty {
            if let Some(mut task) = self.tasks.get_mut(&task_id) {
                task.status = BackupTaskStatus::Completed;
                task.completed_at = Some(Utc::now());
            }
            Self::publish_status_changed_static(&self.ws_manager, &task_id, "preparing", "completed");
            // 发送任务完成事件
            if let Some(task) = self.tasks.get(&task_id) {
                Self::publish_task_completed_static(&self.ws_manager, &task);
            }
            return Ok(());
        }

        Self::publish_status_changed_static(&self.ws_manager, &task_id, "preparing", "transferring");

        // 🔥 优化：克隆 pending_files 用于创建上传任务，保留原始数据供 API 查询
        let pending_files_clone = if let Some(task) = self.tasks.get(&task_id) {
            task.pending_files.clone()
        } else {
            Vec::new()
        };

        // 创建上传任务，返回处理后的文件任务列表
        let processed_files = self.create_upload_tasks_for_files(&task_id, &config, pending_files_clone).await?;

        // 🔥 修复：把处理后的文件任务放回 task.pending_files，更新状态
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.pending_files = processed_files;
        }

        Ok(())
    }

    /// 为文件列表创建上传任务
    /// 返回处理后的文件任务列表，调用方需要将其放回 task.pending_files
    async fn create_upload_tasks_for_files(
        &self,
        task_id: &str,
        config: &BackupConfig,
        mut file_tasks: Vec<BackupFileTask>,
    ) -> Result<Vec<BackupFileTask>> {
        // 使用安全获取方法，处理 Weak 引用升级
        let upload_mgr = self.get_upload_manager();

        let upload_mgr = match upload_mgr {
            Some(mgr) => mgr,
            None => {
                tracing::error!("上传管理器未设置: task={}", task_id);
                return Err(anyhow!("上传管理器未设置"));
            }
        };

        let mut created_count = 0;
        let mut reused_count = 0;

        for file_task in file_tasks.iter_mut() {
            if let Some(task) = self.tasks.get(task_id) {
                if matches!(task.status, BackupTaskStatus::Cancelled | BackupTaskStatus::Paused) {
                    break;
                }
            }

            let local_path = file_task.local_path.clone();
            let remote_path = file_task.remote_path.clone();
            let file_task_id = file_task.id.clone();

            // 🔥 修复：如果 related_task_id 已存在，说明是重启恢复的任务，跳过创建直接复用
            if let Some(ref existing_upload_id) = file_task.related_task_id {
                // 更新 task 的映射关系（重建映射）
                if let Some(mut task) = self.tasks.get_mut(task_id) {
                    task.pending_upload_task_ids.insert(existing_upload_id.clone());
                    task.transfer_task_map.insert(existing_upload_id.clone(), file_task_id.clone());
                }

                // 更新文件状态为等待传输
                file_task.status = BackupFileStatus::WaitingTransfer;
                file_task.updated_at = Utc::now();

                reused_count += 1;
                tracing::debug!(
                    "复用已恢复的上传任务: file_task={}, upload_task={}",
                    file_task_id, existing_upload_id
                );
                continue;
            }

            // 更新文件状态（直接在 file_task 上操作）
            file_task.status = BackupFileStatus::WaitingTransfer;
            file_task.updated_at = Utc::now();

            // 获取上传冲突策略（如果未指定，使用 SmartDedup 默认值）
            let upload_strategy = config.upload_conflict_strategy
                .unwrap_or(crate::uploader::conflict::UploadConflictStrategy::SmartDedup);

            match upload_mgr.create_backup_task(
                local_path.clone(),
                remote_path.clone(),
                config.id.clone(),
                config.encrypt_enabled,
                Some(task_id.to_string()),
                Some(file_task_id.clone()),
                Some(upload_strategy), // 传递冲突策略
            ).await {
                Ok(upload_task_id) => {
                    if let Err(e) = upload_mgr.start_task(&upload_task_id).await {
                        tracing::error!("启动上传任务失败: {}", e);
                        if let Some(mut task) = self.tasks.get_mut(task_id) {
                            task.failed_count += 1;
                        }
                        // 直接在 file_task 上更新状态
                        file_task.status = BackupFileStatus::Failed;
                        file_task.error_message = Some(format!("启动上传任务失败: {}", e));
                        continue;
                    }

                    // 更新 task 的映射关系
                    if let Some(mut task) = self.tasks.get_mut(task_id) {
                        task.pending_upload_task_ids.insert(upload_task_id.clone());
                        task.transfer_task_map.insert(upload_task_id.clone(), file_task_id.clone());
                    }

                    // 直接在 file_task 上更新 related_task_id
                    file_task.related_task_id = Some(upload_task_id.clone());
                    file_task.updated_at = Utc::now();

                    if let Err(e) = self.persistence_manager.save_file_task(file_task, &config.id) {
                        tracing::warn!("持久化文件任务失败: {}", e);
                    }

                    created_count += 1;
                }
                Err(e) => {
                    tracing::error!("创建上传任务失败: {:?}, error={}", local_path, e);
                    if let Some(mut task) = self.tasks.get_mut(task_id) {
                        task.failed_count += 1;
                    }
                    // 直接在 file_task 上更新状态
                    file_task.status = BackupFileStatus::Failed;
                    file_task.error_message = Some(format!("创建上传任务失败: {}", e));
                }
            }
        }

        tracing::info!(
            "上传任务创建完成: task={}, created={}, reused={}",
            task_id, created_count, reused_count
        );
        Ok(file_tasks)
    }

    /// 使用已扫描的文件列表执行下载备份
    async fn execute_download_backup_with_files(
        &self,
        task_id: String,
        config: BackupConfig,
        mut file_tasks: Vec<BackupFileTask>,
    ) -> Result<()> {
        let file_count = file_tasks.len();
        let total_bytes: u64 = file_tasks.iter().map(|f| f.file_size).sum();

        // 设置 parent_task_id
        for file_task in &mut file_tasks {
            file_task.parent_task_id = task_id.clone();
        }

        // 更新任务状态
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.status = BackupTaskStatus::Preparing;
            task.started_at = Some(Utc::now());
        }
        Self::publish_status_changed_static(&self.ws_manager, &task_id, "queued", "preparing");

        // 批量保存文件任务
        if !file_tasks.is_empty() {
            if let Err(e) = self.persistence_manager.save_file_tasks_batch(&file_tasks, &config.id) {
                tracing::warn!("批量保存文件任务到DB失败: {}", e);
            }
        }

        // 🔥 优化：直接移动 file_tasks 到 task.pending_files，避免不必要的 clone
        let is_empty = file_tasks.is_empty();

        // 更新任务
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.pending_files = file_tasks; // 直接移动，不再 clone
            task.total_count = file_count;
            task.total_bytes = total_bytes;
            task.status = BackupTaskStatus::Transferring;
        }

        if let Some(task) = self.tasks.get(&task_id) {
            if let Err(e) = self.persistence_manager.save_task(&task) {
                tracing::warn!("持久化备份任务失败: {}", e);
            }
        }

        if is_empty {
            if let Some(mut task) = self.tasks.get_mut(&task_id) {
                task.status = BackupTaskStatus::Completed;
                task.completed_at = Some(Utc::now());
            }
            Self::publish_status_changed_static(&self.ws_manager, &task_id, "preparing", "completed");
            // 发送任务完成事件
            if let Some(task) = self.tasks.get(&task_id) {
                Self::publish_task_completed_static(&self.ws_manager, &task);
            }
            return Ok(());
        }

        Self::publish_status_changed_static(&self.ws_manager, &task_id, "preparing", "transferring");

        // 🔥 优化：克隆 pending_files 用于创建下载任务，保留原始数据供 API 查询
        let pending_files_clone = if let Some(task) = self.tasks.get(&task_id) {
            task.pending_files.clone()
        } else {
            Vec::new()
        };

        // 创建下载任务，返回处理后的文件任务列表
        let processed_files = self.create_download_tasks_for_files(&task_id, &config, pending_files_clone).await?;

        // 🔥 修复：把处理后的文件任务放回 task.pending_files，更新状态
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.pending_files = processed_files;
        }

        Ok(())
    }

    /// 为文件列表创建下载任务
    /// 返回处理后的文件任务列表，调用方需要将其放回 task.pending_files
    async fn create_download_tasks_for_files(
        &self,
        task_id: &str,
        config: &BackupConfig,
        mut file_tasks: Vec<BackupFileTask>,
    ) -> Result<Vec<BackupFileTask>> {
        // 使用安全获取方法，处理 Weak 引用升级
        let download_mgr = self.get_download_manager();

        let download_mgr = match download_mgr {
            Some(mgr) => mgr,
            None => {
                tracing::error!("下载管理器未设置: task={}", task_id);
                return Err(anyhow!("下载管理器未设置"));
            }
        };

        let mut created_count = 0;
        let mut reused_count = 0;

        for file_task in file_tasks.iter_mut() {
            if let Some(task) = self.tasks.get(task_id) {
                if matches!(task.status, BackupTaskStatus::Cancelled | BackupTaskStatus::Paused) {
                    break;
                }
            }

            let file_task_id = file_task.id.clone();
            let local_path = file_task.local_path.clone();
            let remote_path = file_task.remote_path.clone();
            let file_size = file_task.file_size;
            let fs_id = file_task.fs_id.unwrap_or(0);

            // 🔥 修复：如果 related_task_id 已存在，说明是重启恢复的任务，跳过创建直接复用
            if let Some(ref existing_download_id) = file_task.related_task_id {
                // 更新 task 的映射关系（重建映射）
                if let Some(mut task) = self.tasks.get_mut(task_id) {
                    task.pending_download_task_ids.insert(existing_download_id.clone());
                    task.transfer_task_map.insert(existing_download_id.clone(), file_task_id.clone());
                }

                // 更新文件状态为等待传输
                file_task.status = BackupFileStatus::WaitingTransfer;
                file_task.updated_at = Utc::now();

                reused_count += 1;
                tracing::debug!(
                    "复用已恢复的下载任务: file_task={}, download_task={}",
                    file_task_id, existing_download_id
                );
                continue;
            }

            // 确保本地目录存在
            if let Some(parent) = local_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::error!("创建本地目录失败: {:?}, error={}", parent, e);
                    if let Some(mut task) = self.tasks.get_mut(task_id) {
                        task.failed_count += 1;
                    }
                    // 直接在 file_task 上更新状态
                    file_task.status = BackupFileStatus::Failed;
                    file_task.error_message = Some(format!("创建目录失败: {}", e));
                    continue;
                }
            }

            // 直接在 file_task 上更新状态
            file_task.status = BackupFileStatus::WaitingTransfer;
            file_task.updated_at = Utc::now();

            // 获取下载冲突策略（如果未指定，使用 Overwrite 默认值）
            let download_strategy = config.download_conflict_strategy
                .unwrap_or(crate::uploader::conflict::DownloadConflictStrategy::Overwrite);

            match download_mgr.create_backup_task(
                fs_id,
                remote_path.clone(),
                local_path.clone(),
                file_size,
                config.id.clone(),
                Some(download_strategy), // 传递冲突策略
            ).await {
                Ok(download_task_id) => {
                    // 检查是否为跳过标记
                    if download_task_id == "skipped" {
                        tracing::info!("跳过备份下载（文件已存在）: file={}", remote_path);
                        if let Some(mut task) = self.tasks.get_mut(task_id) {
                            task.skipped_count += 1;
                        }
                        // 直接在 file_task 上更新状态
                        file_task.status = BackupFileStatus::Skipped;
                        file_task.error_message = Some("文件已存在".to_string());
                        file_task.updated_at = Utc::now();
                        continue;
                    }

                    if let Err(e) = download_mgr.start_task(&download_task_id).await {
                        tracing::error!("启动下载任务失败: {}", e);
                        if let Some(mut task) = self.tasks.get_mut(task_id) {
                            task.failed_count += 1;
                        }
                        // 直接在 file_task 上更新状态
                        file_task.status = BackupFileStatus::Failed;
                        file_task.error_message = Some(format!("启动下载任务失败: {}", e));
                        continue;
                    }

                    // 更新 task 的映射关系
                    if let Some(mut task) = self.tasks.get_mut(task_id) {
                        task.pending_download_task_ids.insert(download_task_id.clone());
                        task.transfer_task_map.insert(download_task_id.clone(), file_task_id.clone());
                    }

                    // 直接在 file_task 上更新 related_task_id
                    file_task.related_task_id = Some(download_task_id.clone());
                    file_task.updated_at = Utc::now();

                    if let Err(e) = self.persistence_manager.save_file_task(file_task, &config.id) {
                        tracing::warn!("持久化文件任务失败: {}", e);
                    }

                    created_count += 1;
                }
                Err(e) => {
                    tracing::error!("创建下载任务失败: {}, error={}", remote_path, e);
                    if let Some(mut task) = self.tasks.get_mut(task_id) {
                        task.failed_count += 1;
                    }
                    // 直接在 file_task 上更新状态
                    file_task.status = BackupFileStatus::Failed;
                    file_task.error_message = Some(format!("创建下载任务失败: {}", e));
                }
            }
        }

        tracing::info!(
            "下载任务创建完成: task={}, created={}, reused={}",
            task_id, created_count, reused_count
        );
        Ok(file_tasks)
    }

    /// 执行 Watch 事件的完整处理流程
    ///
    /// 🔥 Watch 事件不需要全量扫描，直接处理变化的文件路径
    /// 1. 如果有 Transferring 任务，增量合并到现有任务
    /// 2. 如果没有 Transferring 任务，创建新任务并执行
    async fn execute_watch_event(
        &self,
        config: &BackupConfig,
        paths: &[PathBuf],
    ) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "执行Watch事件: config={}, paths={}",
            config.id, paths.len()
        );

        // 检查是否有正在传输的任务
        let transferring_task_id = self.tasks.iter()
            .find(|t| t.config_id == config.id && t.status == BackupTaskStatus::Transferring)
            .map(|t| t.id.clone());

        if let Some(task_id) = transferring_task_id {
            // 有传输任务，直接处理变化的文件并合并
            tracing::info!(
                "Watch事件: 配置 {} 有传输任务 {}，增量合并 {} 个变化文件",
                config.id, task_id, paths.len()
            );
            return self.process_watch_event_files(&task_id, config, paths).await;
        }

        // 没有传输任务，需要创建新任务
        // 先处理文件，生成文件任务列表
        let new_files = self.build_file_tasks_from_paths(config, paths).await?;

        if new_files.is_empty() {
            tracing::info!("Watch事件: config={}, 没有新文件需要备份", config.id);
            return Ok(());
        }

        tracing::info!(
            "Watch事件: config={}, 发现 {} 个新文件，创建新任务",
            config.id, new_files.len()
        );

        // 创建新任务
        let task_id = self.create_backup_task_record(config, TriggerType::Watch).await?;

        // 执行上传（Watch 事件只用于上传备份）
        self.execute_upload_backup_with_files(task_id, config.clone(), new_files).await
    }

    /// 从文件路径列表构建文件任务列表
    ///
    /// 应用过滤规则和去重检查
    async fn build_file_tasks_from_paths(
        &self,
        config: &BackupConfig,
        paths: &[PathBuf],
    ) -> Result<Vec<BackupFileTask>> {
        let mut file_tasks = Vec::new();

        for path in paths {
            // 检查文件是否存在
            if !path.exists() || !path.is_file() {
                // 文件已删除，清理扫描缓存
                let _ = self.scan_cache_manager.delete_by_path(&config.id, &path.to_string_lossy());
                continue;
            }

            // 检查文件是否在配置的本地路径下
            if !path.starts_with(&config.local_path) {
                continue;
            }

            // 获取文件元数据
            let metadata = match std::fs::metadata(path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let file_size = metadata.len();

            // 应用过滤规则
            let file_ext = path.extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();

            if !config.filter_config.include_extensions.is_empty()
                && !config.filter_config.include_extensions.iter().any(|e| e.to_lowercase() == file_ext)
            {
                continue;
            }

            if config.filter_config.exclude_extensions.iter().any(|e| e.to_lowercase() == file_ext) {
                continue;
            }

            let relative_str = path.strip_prefix(&config.local_path)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            if config.filter_config.exclude_directories.iter().any(|d| relative_str.contains(d)) {
                continue;
            }

            if file_size < config.filter_config.min_file_size {
                continue;
            }
            if config.filter_config.max_file_size > 0 && file_size > config.filter_config.max_file_size {
                continue;
            }

            // 去重检查
            let file_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            let relative_path = path.strip_prefix(&config.local_path)
                .map(|p| p.parent().unwrap_or(std::path::Path::new("")).to_string_lossy().to_string())
                .unwrap_or_default();

            let head_md5 = calculate_head_md5(path).unwrap_or_else(|_| "unknown".to_string());

            let (exists, _) = self.record_manager.check_upload_record_preliminary(
                &config.id,
                &relative_path,
                &file_name,
                file_size,
                &head_md5,
            ).unwrap_or((false, None));

            if exists {
                continue;
            }

            // 计算远程路径
            let remote_path = format!("{}/{}",
                                      config.remote_path.trim_end_matches('/'),
                                      path.strip_prefix(&config.local_path)
                                          .map(|p| p.to_string_lossy().replace('\\', "/"))
                                          .unwrap_or_else(|_| file_name.clone()));

            let file_task = BackupFileTask {
                id: Uuid::new_v4().to_string(),
                parent_task_id: String::new(),
                local_path: path.clone(),
                remote_path,
                file_size,
                head_md5: Some(head_md5),
                fs_id: None,
                status: BackupFileStatus::Pending,
                sub_phase: None,
                skip_reason: None,
                encrypted: config.encrypt_enabled,
                encrypted_name: None,
                temp_encrypted_path: None,
                transferred_bytes: 0,
                decrypt_progress: None,
                error_message: None,
                retry_count: 0,
                related_task_id: None,
                backup_operation_type: Some(BackupOperationType::Upload),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            file_tasks.push(file_task);
        }

        Ok(file_tasks)
    }

    /// 处理 Watch 事件中的文件变化（合并到现有任务）
    ///
    /// 只处理变化的文件，根据备份方向复用对应的去重逻辑
    async fn process_watch_event_files(
        &self,
        task_id: &str,
        config: &BackupConfig,
        paths: &[PathBuf],
    ) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "处理Watch事件文件: task={}, config={}, paths={}",
            task_id, config.id, paths.len()
        );

        let mut new_files = Vec::new();

        for path in paths {
            // 检查文件是否存在（可能已被删除）
            if !path.exists() || !path.is_file() {
                // 文件已删除，清理扫描缓存
                let _ = self.scan_cache_manager.delete_by_path(&config.id, &path.to_string_lossy());
                tracing::debug!("Watch文件不存在或不是文件，跳过: {:?}", path);
                continue;
            }

            // 检查文件是否在配置的本地路径下
            if !path.starts_with(&config.local_path) {
                tracing::debug!("Watch文件不在配置路径下，跳过: {:?}", path);
                continue;
            }

            // 获取文件元数据
            let metadata = match std::fs::metadata(path) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("获取文件元数据失败: {:?}, error={}", path, e);
                    continue;
                }
            };

            let file_size = metadata.len();

            // 应用过滤规则
            let file_ext = path.extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();

            // 检查包含扩展名
            if !config.filter_config.include_extensions.is_empty()
                && !config.filter_config.include_extensions.iter().any(|e| e.to_lowercase() == file_ext)
            {
                continue;
            }

            // 检查排除扩展名
            if config.filter_config.exclude_extensions.iter().any(|e| e.to_lowercase() == file_ext) {
                continue;
            }

            // 检查排除目录
            let relative_str = path.strip_prefix(&config.local_path)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            if config.filter_config.exclude_directories.iter().any(|d| relative_str.contains(d)) {
                continue;
            }

            // 检查文件大小
            if file_size < config.filter_config.min_file_size {
                continue;
            }
            if config.filter_config.max_file_size > 0 && file_size > config.filter_config.max_file_size {
                continue;
            }

            // 去重检查
            let file_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            let relative_path = path.strip_prefix(&config.local_path)
                .map(|p| p.parent().unwrap_or(std::path::Path::new("")).to_string_lossy().to_string())
                .unwrap_or_default();

            let head_md5 = match calculate_head_md5(path) {
                Ok(md5) => md5,
                Err(e) => {
                    tracing::warn!("计算文件头MD5失败: {:?}, error={}", path, e);
                    "unknown".to_string()
                }
            };

            let (exists, _) = match self.record_manager.check_upload_record_preliminary(
                &config.id,
                &relative_path,
                &file_name,
                file_size,
                &head_md5,
            ) {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!("查询去重记录失败: {:?}, error={}", path, e);
                    (false, None)
                }
            };

            if exists {
                tracing::debug!("Watch文件已备份，跳过: {} (size={}, md5={})", file_name, file_size, head_md5);
                continue;
            }

            // 计算远程路径
            let remote_path = format!("{}/{}",
                                      config.remote_path.trim_end_matches('/'),
                                      path.strip_prefix(&config.local_path)
                                          .map(|p| p.to_string_lossy().replace('\\', "/"))
                                          .unwrap_or_else(|_| file_name.clone()));

            let file_task = BackupFileTask {
                id: Uuid::new_v4().to_string(),
                parent_task_id: task_id.to_string(),
                local_path: path.clone(),
                remote_path,
                file_size,
                head_md5: Some(head_md5),
                fs_id: None,
                status: BackupFileStatus::Pending,
                sub_phase: None,
                skip_reason: None,
                encrypted: config.encrypt_enabled,
                encrypted_name: None,
                temp_encrypted_path: None,
                transferred_bytes: 0,
                decrypt_progress: None,
                error_message: None,
                retry_count: 0,
                related_task_id: None,
                backup_operation_type: Some(BackupOperationType::Upload),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            new_files.push(file_task);
        }

        if new_files.is_empty() {
            tracing::info!("Watch事件处理完成: task={}, 没有新文件需要备份", task_id);
            return Ok(());
        }

        tracing::info!(
            "Watch事件处理: task={}, 发现 {} 个新文件需要备份",
            task_id, new_files.len()
        );

        // 使用增量合并方法（会自动过滤当前任务已有的文件）
        self.merge_new_files_to_task(task_id, config, new_files).await
    }

    /// 加密路径中的文件夹名（静态版本，用于静态方法中）
    ///
    /// # 参数
    /// - `record_manager`: 备份记录管理器
    /// - `config_id`: 备份配置ID
    /// - `base_remote_path`: 远程基础路径
    /// - `relative_path`: 相对路径
    /// - `key_version`: 当前加密密钥版本号
    fn encrypt_folder_path_static(
        record_manager: &Arc<BackupRecordManager>,
        base_remote_path: &str,
        relative_path: &str,
        key_version: u32,
    ) -> Result<String> {
        let normalized_path = relative_path.replace('\\', "/");
        let path_parts: Vec<&str> = normalized_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if path_parts.is_empty() {
            return Ok(base_remote_path.trim_end_matches('/').to_string());
        }

        let folder_parts = &path_parts[..path_parts.len() - 1];
        let file_name = path_parts.last().unwrap();

        let mut current_parent = base_remote_path.trim_end_matches('/').to_string();
        let mut encrypted_parts = Vec::new();

        for folder_name in folder_parts {
            let encrypted_name = match record_manager.find_encrypted_folder_name(
                &current_parent,
                folder_name,
            )? {
                Some(name) => name,
                None => {
                    let new_encrypted_name = EncryptionService::generate_encrypted_folder_name();
                    record_manager.add_folder_mapping(
                        &current_parent,
                        folder_name,
                        &new_encrypted_name,
                        key_version,
                    )?;
                    tracing::debug!(
                        "创建文件夹映射: {} -> {} (parent={}, key_version={})",
                        folder_name, new_encrypted_name, current_parent, key_version
                    );
                    new_encrypted_name
                }
            };

            encrypted_parts.push(encrypted_name.clone());
            current_parent = format!("{}/{}", current_parent, encrypted_name);
        }

        let encrypted_folder_path = if encrypted_parts.is_empty() {
            base_remote_path.trim_end_matches('/').to_string()
        } else {
            format!(
                "{}/{}",
                base_remote_path.trim_end_matches('/'),
                encrypted_parts.join("/")
            )
        };

        Ok(format!("{}/{}", encrypted_folder_path, file_name))
    }

    /// 还原加密路径中的文件夹名（静态版本）
    ///
    /// 返回相对路径（不包含 base_remote_path），可直接用于 local_path.join()
    pub fn decrypt_folder_path_static(
        record_manager: &Arc<BackupRecordManager>,
        base_remote_path: &str,
        encrypted_path: &str,
    ) -> Result<String> {
        let relative_encrypted = encrypted_path
            .strip_prefix(base_remote_path.trim_end_matches('/'))
            .unwrap_or(encrypted_path)
            .trim_start_matches('/');

        let path_parts: Vec<&str> = relative_encrypted
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if path_parts.is_empty() {
            // 返回空字符串，表示没有相对路径
            return Ok(String::new());
        }

        let mut current_parent = base_remote_path.trim_end_matches('/').to_string();
        let mut original_parts = Vec::new();

        for part in &path_parts {
            if EncryptionService::is_encrypted_folder_name(part) {
                match record_manager.find_original_folder_name(part)? {
                    Some(original_name) => {
                        original_parts.push(original_name.clone());
                        current_parent = format!("{}/{}", current_parent, part);
                    }
                    None => {
                        original_parts.push(part.to_string());
                        current_parent = format!("{}/{}", current_parent, part);
                    }
                }
            } else {
                original_parts.push(part.to_string());
                current_parent = format!("{}/{}", current_parent, part);
            }
        }

        // 返回相对路径，不包含 base_remote_path
        // 这样调用方可以直接用 config.local_path.join() 拼接
        Ok(original_parts.join("/"))
    }

    /// 加密路径中的文件夹名
    ///
    /// 将相对路径中的每个文件夹名替换为加密名，并保存映射关系
    /// 例如：`documents/photos/image.jpg` -> `BPR_DIR_xxx/BPR_DIR_yyy/image.jpg`
    ///
    /// # 参数
    /// - `config_id`: 备份配置ID
    /// - `base_remote_path`: 远程基础路径（如 `/apps/backup`）
    /// - `relative_path`: 相对路径（如 `documents/photos/image.jpg`）
    ///
    /// # 返回
    /// 加密后的完整远程路径
    pub fn encrypt_folder_path(
        &self,
        base_remote_path: &str,
        relative_path: &str,
    ) -> Result<String> {
        // 🔥 获取当前密钥版本号
        let current_key_version = match self.encryption_config_store.get_current_key() {
            Ok(Some(key_info)) => key_info.key_version,
            Ok(None) => {
                tracing::warn!("encrypt_folder_path: 未找到加密密钥，使用默认版本 1");
                1u32
            }
            Err(e) => {
                tracing::warn!("encrypt_folder_path: 获取密钥版本失败: {}，使用默认版本 1", e);
                1u32
            }
        };

        // 将相对路径分割为各个部分
        let normalized_path = relative_path.replace('\\', "/");
        let path_parts: Vec<&str> = normalized_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if path_parts.is_empty() {
            return Ok(base_remote_path.trim_end_matches('/').to_string());
        }

        // 最后一个是文件名，不需要加密（文件名在后续流程中单独加密）
        let folder_parts = &path_parts[..path_parts.len() - 1];
        let file_name = path_parts.last().unwrap();

        // 逐级加密文件夹名
        let mut current_parent = base_remote_path.trim_end_matches('/').to_string();
        let mut encrypted_parts = Vec::new();

        for folder_name in folder_parts {
            // 查找是否已有映射
            let encrypted_name = match self.record_manager.find_encrypted_folder_name(
                &current_parent,
                folder_name,
            )? {
                Some(name) => name,
                None => {
                    // 生成新的加密文件夹名
                    let new_encrypted_name = EncryptionService::generate_encrypted_folder_name();

                    self.record_manager.add_folder_mapping(
                        &current_parent,
                        folder_name,
                        &new_encrypted_name,
                        current_key_version,
                    )?;

                    tracing::debug!(
                        "创建文件夹映射: {} -> {} (parent={}, key_version={})",
                        folder_name, new_encrypted_name, current_parent, current_key_version
                    );

                    new_encrypted_name
                }
            };

            encrypted_parts.push(encrypted_name.clone());
            current_parent = format!("{}/{}", current_parent, encrypted_name);
        }

        // 构建最终路径：base_path + encrypted_folders + file_name
        let encrypted_folder_path = if encrypted_parts.is_empty() {
            base_remote_path.trim_end_matches('/').to_string()
        } else {
            format!(
                "{}/{}",
                base_remote_path.trim_end_matches('/'),
                encrypted_parts.join("/")
            )
        };

        Ok(format!("{}/{}", encrypted_folder_path, file_name))
    }

    /// 还原加密路径中的文件夹名
    ///
    /// 返回相对路径（不包含 base_remote_path），可直接用于 local_path.join()
    pub fn decrypt_folder_path(
        &self,
        base_remote_path: &str,
        encrypted_path: &str,
    ) -> Result<String> {
        // 移除基础路径前缀
        let relative_encrypted = encrypted_path
            .strip_prefix(base_remote_path.trim_end_matches('/'))
            .unwrap_or(encrypted_path)
            .trim_start_matches('/');

        let path_parts: Vec<&str> = relative_encrypted
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if path_parts.is_empty() {
            // 返回空字符串，表示没有相对路径
            return Ok(String::new());
        }

        // 逐级还原文件夹名
        let mut current_parent = base_remote_path.trim_end_matches('/').to_string();
        let mut original_parts = Vec::new();

        for part in &path_parts {
            if EncryptionService::is_encrypted_folder_name(part) {
                // 查找原始文件夹名
                match self.record_manager.find_original_folder_name(part)? {
                    Some(original_name) => {
                        original_parts.push(original_name.clone());
                        current_parent = format!("{}/{}", current_parent, part);
                    }
                    None => {
                        // 找不到映射，保持原样
                        original_parts.push(part.to_string());
                        current_parent = format!("{}/{}", current_parent, part);
                    }
                }
            } else {
                // 不是加密文件夹名，保持原样
                original_parts.push(part.to_string());
                current_parent = format!("{}/{}", current_parent, part);
            }
        }

        // 返回相对路径，不包含 base_remote_path
        Ok(original_parts.join("/"))
    }
}

// ==================== 数据结构 ====================

/// 加密状态
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptionStatus {
    pub enabled: bool,
    pub has_key: bool,
    pub algorithm: EncryptionAlgorithm,
    pub key_created_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// 去重结果
#[derive(Debug, Clone)]
pub struct DedupResult {
    pub should_upload: bool,
    pub reason: Option<String>,
    pub existing_md5: Option<String>,
}

/// 管理器状态
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ManagerStatus {
    pub config_count: usize,
    pub active_task_count: usize,
    pub watcher_running: bool,
    pub watched_path_count: usize,
    pub poll_schedule_count: usize,
    pub encryption_enabled: bool,
    pub scan_slots: String,
    pub encrypt_slots: String,
}

/// 加密上传准备结果
#[derive(Debug, Clone)]
pub struct PrepareEncryptedUploadResult {
    /// 是否需要上传
    pub should_upload: bool,
    /// 跳过原因（如果不需要上传）
    pub skip_reason: Option<String>,
    /// 加密后的临时文件路径
    pub encrypted_path: Option<PathBuf>,
    /// 加密文件名
    pub encrypted_name: Option<String>,
    /// 上传的远程路径（加密文件使用加密文件名）
    pub original_remote_path: String,
}

/// 解密下载准备结果
#[derive(Debug, Clone)]
pub struct PrepareDecryptedDownloadResult {
    /// 是否为加密文件
    pub is_encrypted: bool,
    /// 原始文件名（解密后的文件名）
    pub original_name: Option<String>,
    /// 原始文件路径（解密后的保存路径）
    pub original_path: Option<PathBuf>,
    /// 临时下载路径（加密文件先下载到这里）
    pub temp_download_path: Option<PathBuf>,
    /// 快照信息
    pub snapshot_info: Option<crate::encryption::snapshot::SnapshotInfo>,
}

/// 解密下载完成结果
#[derive(Debug, Clone)]
pub struct DecryptDownloadResult {
    /// 是否成功
    pub success: bool,
    /// 解密后的文件路径
    pub decrypted_path: Option<PathBuf>,
    /// 原始文件大小
    pub original_size: Option<u64>,
    /// 错误信息
    pub error: Option<String>,
}

/// 文件状态信息（用于调试 API）
#[derive(Debug, Clone)]
pub struct FileStateInfo {
    /// 当前状态
    pub current_state: String,
    /// 状态历史 (状态名, 时间戳)
    pub state_history: Vec<(String, String)>,
    /// 去重结果
    pub dedup_result: Option<String>,
    /// 是否启用加密
    pub encryption_enabled: bool,
    /// 重试次数
    pub retry_count: u32,
    /// 关联的配置ID
    pub config_id: Option<String>,
    /// 关联的任务ID
    pub task_id: Option<String>,
}

/// 健康检查结果
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// 数据库连接状态
    pub database_ok: bool,
    /// 加密密钥状态
    pub encryption_key_ok: bool,
    /// 文件监听状态
    pub file_watcher_ok: bool,
    /// 网络连接状态
    pub network_ok: bool,
    /// 磁盘空间状态
    pub disk_space_ok: bool,
}

/// 优雅关闭结果
#[derive(Debug, Clone)]
pub struct ShutdownResult {
    /// 是否成功关闭
    pub success: bool,
    /// 已保存的任务数
    pub saved_tasks: usize,
    /// 已停止的监听器数
    pub stopped_watchers: usize,
    /// 已停止的调度器数
    pub stopped_schedulers: usize,
    /// 已清理的临时文件数
    pub cleaned_temp_files: usize,
    /// 错误信息（如果有）
    pub errors: Vec<String>,
}
