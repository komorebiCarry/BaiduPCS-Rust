// 应用状态

use crate::auth::{QRCodeAuth, SessionManager, UserAuth};
use crate::encryption::SnapshotManager;
use crate::autobackup::record::BackupRecordManager;
use crate::autobackup::AutoBackupManager;
use crate::common::{MemoryMonitor, MemoryMonitorConfig};
use crate::config::AppConfig;
use crate::downloader::{DownloadManager, FolderDownloadManager};
use crate::netdisk::{CloudDlMonitor, NetdiskClient};
use crate::persistence::{
    cleanup_completed_tasks, cleanup_invalid_tasks, scan_recoverable_tasks, DownloadRecoveryInfo,
    PersistenceManager, TransferRecoveryInfo, UploadRecoveryInfo,
};
use crate::server::websocket::WebSocketManager;
use crate::transfer::TransferManager;
use crate::uploader::UploadManager;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

/// 应用全局状态
#[derive(Clone)]
pub struct AppState {
    /// 二维码认证客户端
    pub qrcode_auth: Arc<QRCodeAuth>,
    /// 会话管理器
    pub session_manager: Arc<Mutex<SessionManager>>,
    /// 当前登录用户
    pub current_user: Arc<RwLock<Option<UserAuth>>>,
    /// 网盘客户端
    pub netdisk_client: Arc<RwLock<Option<NetdiskClient>>>,
    /// 下载管理器（使用 Arc 避免被意外克隆）
    pub download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
    /// 文件夹下载管理器
    pub folder_download_manager: Arc<FolderDownloadManager>,
    /// 上传管理器
    pub upload_manager: Arc<RwLock<Option<Arc<UploadManager>>>>,
    /// 转存管理器
    pub transfer_manager: Arc<RwLock<Option<Arc<TransferManager>>>>,
    /// 应用配置
    pub config: Arc<RwLock<AppConfig>>,
    /// 🔥 持久化管理器
    pub persistence_manager: Arc<Mutex<PersistenceManager>>,
    /// 🔥 WebSocket 管理器
    pub ws_manager: Arc<WebSocketManager>,
    /// 🔥 自动备份管理器
    pub autobackup_manager: Arc<RwLock<Option<Arc<AutoBackupManager>>>>,
    /// 🔥 快照管理器（加密文件映射，独立于自动备份管理器）
    pub snapshot_manager: Arc<SnapshotManager>,
    /// 🔥 备份记录管理器（供 autobackup 复用）
    pub backup_record_manager: Arc<BackupRecordManager>,
    /// 🔥 内存监控器
    pub memory_monitor: Arc<MemoryMonitor>,
    /// 🔥 离线下载监听服务
    pub cloud_dl_monitor: Arc<RwLock<Option<Arc<CloudDlMonitor>>>>,
}

impl AppState {
    /// 创建新的应用状态
    pub async fn new() -> anyhow::Result<Self> {
        // 加载配置
        let config = AppConfig::load_or_default("config/app.toml").await;

        // 创建文件夹下载管理器
        let folder_download_manager = Arc::new(FolderDownloadManager::new(
            config.download.download_dir.clone().into(),
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

        Ok(Self {
            qrcode_auth: Arc::new(QRCodeAuth::new()?),
            session_manager: Arc::new(Mutex::new(SessionManager::default())),
            current_user: Arc::new(RwLock::new(None)),
            netdisk_client: Arc::new(RwLock::new(None)),
            download_manager: Arc::new(RwLock::new(None)),
            folder_download_manager,
            upload_manager: Arc::new(RwLock::new(None)),
            transfer_manager: Arc::new(RwLock::new(None)),
            config: Arc::new(RwLock::new(config)),
            persistence_manager: Arc::new(Mutex::new(persistence_manager)),
            ws_manager,
            autobackup_manager: Arc::new(RwLock::new(None)),
            snapshot_manager,
            backup_record_manager,
            memory_monitor,
            cloud_dl_monitor: Arc::new(RwLock::new(None)),
        })
    }

    /// 初始化时加载会话
    pub async fn load_initial_session(&self) -> anyhow::Result<()> {
        // 🔥 获取持久化管理器的 Arc 引用（直接使用已启动的实例）
        let pm_arc = Arc::clone(&self.persistence_manager);

        let mut session_manager = self.session_manager.lock().await;
        if let Some(mut user_auth) = session_manager.get_session().await? {
            *self.current_user.write().await = Some(user_auth.clone());

            // 初始化网盘客户端
            let client = NetdiskClient::new(user_auth.clone())?;

            // 预热过期时间（2小时 = 7200秒）
            const WARMUP_EXPIRE_SECS: i64 = 86400;

            // 检查是否需要预热：
            // 1. 预热数据不存在
            // 2. 或者预热数据已过期（超过24小时）
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
                        user_auth.panpsc = panpsc;
                        user_auth.csrf_token = csrf_token;
                        user_auth.bdstoken = bdstoken;
                        user_auth.last_warmup_at = Some(chrono::Utc::now().timestamp());
                        // 预热时下发的 STOKEN 优先于之前保存的
                        if stoken.is_some() {
                            user_auth.stoken = stoken;
                        }

                        // 更新内存中的用户信息
                        *self.current_user.write().await = Some(user_auth.clone());

                        // 保存到 session.json
                        if let Err(e) = session_manager.save_session(&user_auth).await {
                            error!("保存预热 Cookie 失败: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("预热失败(可能需要重新登录): {}", e);
                    }
                }
            }

            let client_arc = Arc::new(client.clone());
            *self.netdisk_client.write().await = Some(client.clone());

            // 初始化下载管理器（从配置读取参数）
            let config = self.config.read().await;
            let download_dir = config.download.download_dir.clone();
            let max_global_threads = config.download.max_global_threads;
            let max_concurrent_tasks = config.download.max_concurrent_tasks;
            let max_retries = config.download.max_retries;
            drop(config);

            let mut manager = DownloadManager::with_config(
                user_auth.clone(),
                download_dir,
                max_global_threads,
                max_concurrent_tasks,
                max_retries,
            )?;

            // 🔥 设置持久化管理器
            manager.set_persistence_manager(Arc::clone(&pm_arc));

            // 🔥 设置 WebSocket 管理器
            manager.set_ws_manager(Arc::clone(&self.ws_manager)).await;

            let manager_arc = Arc::new(manager);
            *self.download_manager.write().await = Some(Arc::clone(&manager_arc));

            // 设置文件夹下载管理器的依赖
            self.folder_download_manager
                .set_download_manager(Arc::clone(&manager_arc))
                .await;
            self.folder_download_manager
                .set_netdisk_client(client_arc)
                .await;

            // 🔥 设置文件夹下载管理器的 WAL 目录（用于文件夹持久化）
            let wal_dir = pm_arc.lock().await.wal_dir().clone();
            self.folder_download_manager.set_wal_dir(wal_dir).await;

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

            // 初始化上传管理器（从配置读取参数）
            let config = self.config.read().await;
            let upload_config = config.upload.clone();
            let transfer_config = config.transfer.clone();
            drop(config);

            // 🔥 配置目录（用于读取 encryption.json）
            let config_dir = std::path::Path::new("config");
            let upload_manager =
                UploadManager::new_with_config(client.clone(), &user_auth, &upload_config, config_dir);
            let upload_manager_arc = Arc::new(upload_manager);

            // 🔥 设置持久化管理器
            upload_manager_arc
                .set_persistence_manager(Arc::clone(&pm_arc))
                .await;

            // 🔥 设置上传管理器的 WebSocket 管理器
            upload_manager_arc
                .set_ws_manager(Arc::clone(&self.ws_manager))
                .await;

            // 🔥 设置备份记录管理器（用于文件夹名加密映射）
            upload_manager_arc
                .set_backup_record_manager(Arc::clone(&self.backup_record_manager))
                .await;

            *self.upload_manager.write().await = Some(Arc::clone(&upload_manager_arc));

            // 初始化转存管理器
            let transfer_manager =
                TransferManager::new(Arc::new(client), transfer_config, Arc::clone(&self.config));
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

            *self.transfer_manager.write().await = Some(Arc::clone(&transfer_manager_arc));
            info!("转存管理器初始化完成");

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
            Ok(scan_result) => {
                info!(
                    "扫描完成: {} 个下载任务, {} 个上传任务, {} 个转存任务, {} 个已完成, {} 个无效",
                    scan_result.download_tasks.len(),
                    scan_result.upload_tasks.len(),
                    scan_result.transfer_tasks.len(),
                    scan_result.completed_tasks.len(),
                    scan_result.invalid_tasks.len()
                );

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
                if !scan_result.download_tasks.is_empty() {
                    let recovery_infos: Vec<DownloadRecoveryInfo> = scan_result
                        .download_tasks
                        .iter()
                        .filter_map(|t| DownloadRecoveryInfo::from_recovered(t))
                        .collect();

                    let (success, failed) = download_manager.restore_tasks(recovery_infos).await;
                    info!("下载任务恢复完成: {} 成功, {} 失败", success, failed);

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

                // 恢复上传任务
                if !scan_result.upload_tasks.is_empty() {
                    let recovery_infos: Vec<UploadRecoveryInfo> = scan_result
                        .upload_tasks
                        .iter()
                        .filter_map(|t| UploadRecoveryInfo::from_recovered(t))
                        .collect();

                    let (success, failed) = upload_manager.restore_tasks(recovery_infos).await;
                    info!("上传任务恢复完成: {} 成功, {} 失败", success, failed);
                }

                // 恢复转存任务
                if !scan_result.transfer_tasks.is_empty() {
                    let recovery_infos: Vec<TransferRecoveryInfo> = scan_result
                        .transfer_tasks
                        .iter()
                        .filter_map(|t| TransferRecoveryInfo::from_recovered(t))
                        .collect();

                    let (success, failed) = transfer_manager.restore_tasks(recovery_infos).await;
                    info!("转存任务恢复完成: {} 成功, {} 失败", success, failed);
                }
            }
            Err(e) => {
                error!("扫描可恢复任务失败: {}", e);
            }
        }
    }

    /// 🔥 初始化自动备份管理器
    pub async fn init_autobackup_manager(&self) {
        use std::path::PathBuf;

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
                if let Some(ref upload_mgr) = *self.upload_manager.read().await {
                    manager.set_upload_manager(Arc::clone(upload_mgr));
                }

                // 设置下载管理器（用于执行备份下载）
                if let Some(ref download_mgr) = *self.download_manager.read().await {
                    manager.set_download_manager(Arc::clone(download_mgr));
                }

                // 🔥 注入 snapshot_manager 到 DownloadManager 和 UploadManager
                // 使用 AppState 中已创建的 snapshot_manager（而非从 manager 获取）
                let encryption_config_store = manager.get_encryption_config_store();

                // 注入到下载管理器（用于解密时查询原始文件名和 key_version）
                if let Some(ref download_mgr) = *self.download_manager.read().await {
                    download_mgr.set_snapshot_manager(Arc::clone(&self.snapshot_manager)).await;
                    download_mgr.set_encryption_config_store(Arc::clone(&encryption_config_store)).await;
                    info!("已将 snapshot_manager 和 encryption_config_store 注入到下载管理器");
                }

                // 注入到上传管理器（用于上传完成后保存加密映射）
                if let Some(ref upload_mgr) = *self.upload_manager.read().await {
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

                *self.autobackup_manager.write().await = Some(manager_arc);
                info!("自动备份管理器初始化完成");
            }
            Err(e) => {
                error!("自动备份管理器初始化失败: {}", e);
            }
        }
    }

    /// 🔥 初始化离线下载监听服务
    pub async fn init_cloud_dl_monitor(&self) {
        // 获取网盘客户端
        let client_lock = self.netdisk_client.read().await;
        let client = match client_lock.as_ref() {
            Some(c) => c.clone(),
            None => {
                warn!("网盘客户端未初始化，跳过离线下载监听服务初始化");
                return;
            }
        };
        drop(client_lock);

        // 创建监听服务
        let monitor = CloudDlMonitor::new(Arc::new(client));

        // 设置 WebSocket 管理器
        monitor.set_ws_manager(Arc::clone(&self.ws_manager)).await;

        // 设置数据库路径（用于持久化自动下载配置）
        let config = self.config.read().await;
        let db_path = std::path::PathBuf::from(&config.persistence.db_path);
        drop(config);
        monitor.set_db_path(db_path).await;

        // 🔥 设置下载管理器（用于自动下载功能）
        if let Some(ref dm) = *self.download_manager.read().await {
            monitor.set_download_manager(Arc::clone(dm)).await;
        }

        // 🔥 设置文件夹下载管理器（用于自动下载文件夹）
        monitor.set_folder_download_manager(Arc::clone(&self.folder_download_manager)).await;

        // 从数据库加载未触发的自动下载配置
        let loaded = monitor.load_auto_download_configs_from_db().await;
        if loaded > 0 {
            info!("离线下载监听服务已恢复 {} 个自动下载配置", loaded);
        }

        let monitor_arc = Arc::new(monitor);

        // 启动后台监听任务
        let monitor_clone = Arc::clone(&monitor_arc);
        tokio::spawn(async move {
            monitor_clone.start().await;
        });

        *self.cloud_dl_monitor.write().await = Some(monitor_arc);
        info!("离线下载监听服务初始化完成");
    }

    /// 🔥 手动触发预热
    ///
    /// 当 API 返回特定错误码（如 errno=-6）时，可调用此方法重新预热会话。
    /// 预热成功后会自动更新 session.json。
    ///
    /// # 返回值
    /// - `Ok(true)` - 预热成功
    /// - `Ok(false)` - 无需预热（用户未登录或客户端未初始化）
    /// - `Err(e)` - 预热失败
    pub async fn trigger_warmup(&self) -> anyhow::Result<bool> {
        // 获取网盘客户端
        let client = {
            let client_lock = self.netdisk_client.read().await;
            match client_lock.as_ref() {
                Some(c) => c.clone(),
                None => {
                    warn!("网盘客户端未初始化，无法执行预热");
                    return Ok(false);
                }
            }
        };

        // 获取当前用户
        let mut user_auth = {
            let user_lock = self.current_user.read().await;
            match user_lock.as_ref() {
                Some(u) => u.clone(),
                None => {
                    warn!("用户未登录，无法执行预热");
                    return Ok(false);
                }
            }
        };

        info!("手动触发预热...");

        // 清除旧的预热数据
        user_auth.panpsc = None;
        user_auth.csrf_token = None;
        user_auth.bdstoken = None;

        // 执行预热
        match client.warmup_and_get_cookies().await {
            Ok((panpsc, csrf_token, bdstoken, stoken)) => {
                info!("手动预热成功，更新 session.json");
                user_auth.panpsc = panpsc;
                user_auth.csrf_token = csrf_token;
                user_auth.bdstoken = bdstoken;
                user_auth.last_warmup_at = Some(chrono::Utc::now().timestamp());

                // 预热时下发的 STOKEN 优先于之前保存的
                if stoken.is_some() {
                    user_auth.stoken = stoken;
                }

                // 更新内存中的用户信息
                *self.current_user.write().await = Some(user_auth.clone());

                // 保存到 session.json
                let mut session_manager = self.session_manager.lock().await;
                if let Err(e) = session_manager.save_session(&user_auth).await {
                    error!("保存预热 Cookie 失败: {}", e);
                }

                Ok(true)
            }
            Err(e) => {
                error!("手动预热失败: {}", e);
                Err(anyhow::anyhow!("预热失败: {}", e))
            }
        }
    }

    /// 🔥 优雅关闭
    ///
    /// 关闭持久化管理器，确保所有 WAL 数据刷写到磁盘
    pub async fn shutdown(&self) {
        info!("正在关闭应用状态...");

        // 停止离线下载监听服务
        if let Some(ref monitor) = *self.cloud_dl_monitor.read().await {
            monitor.stop();
            info!("离线下载监听服务已停止");
        }

        // 停止内存监控器
        self.memory_monitor.stop();
        info!("内存监控器已停止");

        // 关闭持久化管理器
        let mut pm = self.persistence_manager.lock().await;
        pm.shutdown().await;

        info!("应用状态已安全关闭");
    }
}

// 注意：Default trait 不能用于 async，移除或使用 lazy_static
