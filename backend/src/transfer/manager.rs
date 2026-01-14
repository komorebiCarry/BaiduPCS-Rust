// 转存任务管理器

use crate::config::{AppConfig, TransferConfig};
use crate::downloader::{DownloadManager, FolderDownloadManager, TaskStatus};
use crate::netdisk::NetdiskClient;
use crate::persistence::{
    PersistenceManager, TaskMetadata, TransferRecoveryInfo,
};
use crate::server::events::{TaskEvent, TransferEvent};
use crate::server::websocket::WebSocketManager;
use crate::transfer::task::{TransferStatus, TransferTask};
use crate::transfer::types::{ShareLink, SharePageInfo, SharedFileInfo, TransferResult};
use anyhow::{Context, Result};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// 转存任务信息（包含任务和取消令牌）
pub struct TransferTaskInfo {
    pub task: Arc<RwLock<TransferTask>>,
    pub cancellation_token: CancellationToken,
}

/// 转存管理器
pub struct TransferManager {
    /// 网盘客户端
    client: Arc<NetdiskClient>,
    /// 所有转存任务
    tasks: Arc<DashMap<String, TransferTaskInfo>>,
    /// 下载管理器（用于自动下载）
    download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
    /// 文件夹下载管理器（用于自动下载文件夹）
    folder_download_manager: Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
    /// 转存配置
    config: Arc<RwLock<TransferConfig>>,
    /// 应用配置（用于获取下载相关配置）
    app_config: Arc<RwLock<AppConfig>>,
    /// 🔥 持久化管理器引用（使用单锁结构避免死锁）
    persistence_manager: Arc<Mutex<Option<Arc<Mutex<PersistenceManager>>>>>,
    /// 🔥 WebSocket 管理器
    ws_manager: Arc<RwLock<Option<Arc<WebSocketManager>>>>,
}

/// 创建转存任务请求
#[derive(Debug, Clone)]
pub struct CreateTransferRequest {
    pub share_url: String,
    pub password: Option<String>,
    pub save_path: String,
    pub save_fs_id: u64,
    pub auto_download: Option<bool>,
    pub local_download_path: Option<String>,
}

/// 创建转存任务响应
#[derive(Debug, Clone)]
pub struct CreateTransferResponse {
    pub task_id: Option<String>,
    pub status: Option<TransferStatus>,
    pub need_password: bool,
    pub error: Option<String>,
}

impl TransferManager {
    /// 创建新的转存管理器
    pub fn new(
        client: Arc<NetdiskClient>,
        config: TransferConfig,
        app_config: Arc<RwLock<AppConfig>>,
    ) -> Self {
        info!("创建转存管理器");
        Self {
            client,
            tasks: Arc::new(DashMap::new()),
            download_manager: Arc::new(RwLock::new(None)),
            folder_download_manager: Arc::new(RwLock::new(None)),
            config: Arc::new(RwLock::new(config)),
            app_config,
            persistence_manager: Arc::new(Mutex::new(None)),
            ws_manager: Arc::new(RwLock::new(None)),
        }
    }

    /// 🔥 设置持久化管理器
    pub async fn set_persistence_manager(&self, pm: Arc<Mutex<PersistenceManager>>) {
        let mut lock = self.persistence_manager.lock().await;
        *lock = Some(pm);
        info!("转存管理器已设置持久化管理器");
    }

    /// 🔥 设置 WebSocket 管理器
    pub async fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        let mut ws = self.ws_manager.write().await;
        *ws = Some(ws_manager);
        info!("转存管理器已设置 WebSocket 管理器");
    }

    /// 🔥 发布转存事件
    #[allow(dead_code)]
    async fn publish_event(&self, event: TransferEvent) {
        let ws = self.ws_manager.read().await;
        if let Some(ref ws) = *ws {
            ws.send_if_subscribed(TaskEvent::Transfer(event), None);
        }
    }

    /// 获取持久化管理器引用的克隆
    pub async fn persistence_manager(&self) -> Option<Arc<Mutex<PersistenceManager>>> {
        self.persistence_manager.lock().await.clone()
    }

    /// 设置下载管理器（用于自动下载功能）
    pub async fn set_download_manager(&self, dm: Arc<DownloadManager>) {
        let mut lock = self.download_manager.write().await;
        *lock = Some(dm);
        info!("转存管理器已设置下载管理器");
    }

    /// 设置文件夹下载管理器（用于自动下载文件夹）
    pub async fn set_folder_download_manager(&self, fdm: Arc<FolderDownloadManager>) {
        let mut lock = self.folder_download_manager.write().await;
        *lock = Some(fdm);
        info!("转存管理器已设置文件夹下载管理器");
    }

    /// 创建转存任务
    ///
    /// 如果需要密码，返回 need_password=true
    /// 如果密码错误，返回错误信息
    pub async fn create_task(
        &self,
        request: CreateTransferRequest,
    ) -> Result<CreateTransferResponse> {
        info!("创建转存任务: url={}", request.share_url);

        // 1. 解析分享链接
        let share_link = self.client.parse_share_link(&request.share_url)?;

        // 合并密码：请求中的密码 > 链接中的密码
        let password = request.password.or(share_link.password.clone());

        // 重新创建 share_link 用于后续使用（避免部分移动问题）
        let share_link = ShareLink {
            short_key: share_link.short_key,
            raw_url: share_link.raw_url,
            password: password.clone(), // 密码已提取
        };

        // 2. 确定是否自动下载
        let auto_download = match request.auto_download {
            Some(v) => v,
            None => {
                let config = self.config.read().await;
                config.default_behavior == "transfer_and_download"
            }
        };

        // 3. 创建任务
        let task = TransferTask::new(
            request.share_url.clone(),
            password.clone(),
            request.save_path.clone(),
            request.save_fs_id,
            auto_download,
            request.local_download_path.clone(),
        );
        let task_id = task.id.clone();

        // 4. 访问分享页面，获取分享信息
        let share_info_result = self
            .client
            .access_share_page(&share_link.short_key, &share_link.password, true)
            .await;

        match share_info_result {
            Ok(info) => {
                // 如果有密码，先验证密码
                if let Some(ref pwd) = password {
                    let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
                    match self
                        .client
                        .verify_share_password(
                            &info.shareid,
                            &info.share_uk,
                            &info.bdstoken,
                            pwd,
                            &referer,
                        )
                        .await
                    {
                        Ok(_randsk) => {
                            info!("提取码验证成功");
                        }
                        Err(e) => {
                            let err_msg = e.to_string();
                            if err_msg.contains("提取码错误") || err_msg.contains("-9") {
                                return Ok(CreateTransferResponse {
                                    task_id: None,
                                    status: None,
                                    need_password: false,
                                    error: Some("提取码错误".to_string()),
                                });
                            }
                            return Ok(CreateTransferResponse {
                                task_id: None,
                                status: None,
                                need_password: false,
                                error: Some(err_msg),
                            });
                        }
                    }
                }

                let task_arc = Arc::new(RwLock::new(task));
                let cancellation_token = CancellationToken::new();

                // 保存分享信息
                {
                    let mut t = task_arc.write().await;
                    t.set_share_info(info.clone());
                }

                // 存储任务
                self.tasks.insert(
                    task_id.clone(),
                    TransferTaskInfo {
                        task: task_arc.clone(),
                        cancellation_token: cancellation_token.clone(),
                    },
                );

                // 🔥 注册任务到持久化管理器
                if let Some(pm_arc) = self
                    .persistence_manager
                    .lock()
                    .await
                    .as_ref()
                    .map(|pm| pm.clone())
                {
                    if let Err(e) = pm_arc.lock().await.register_transfer_task(
                        task_id.clone(),
                        request.share_url.clone(),
                        password.clone(),
                        request.save_path.clone(),
                        auto_download,
                        None, // 文件名在获取文件列表后更新
                    ) {
                        warn!("注册转存任务到持久化管理器失败: {}", e);
                    }
                }

                // 🔥 发送任务创建事件
                self.publish_event(TransferEvent::Created {
                    task_id: task_id.clone(),
                    share_url: request.share_url.clone(),
                    save_path: request.save_path.clone(),
                    auto_download,
                })
                    .await;

                // 启动异步执行
                self.spawn_task_execution(task_id.clone(), share_link, cancellation_token)
                    .await;

                Ok(CreateTransferResponse {
                    task_id: Some(task_id),
                    status: Some(TransferStatus::CheckingShare),
                    need_password: false,
                    error: None,
                })
            }
            Err(e) => {
                let err_msg = e.to_string();

                // 检查是否需要密码
                if err_msg.contains("需要密码") || err_msg.contains("need password") {
                    if password.is_none() {
                        return Ok(CreateTransferResponse {
                            task_id: None,
                            status: None,
                            need_password: true,
                            error: Some("需要提取码".to_string()),
                        });
                    }
                    // 有密码但可能是错误的，继续尝试验证
                }

                // 检查分享是否失效
                if err_msg.contains("已失效") || err_msg.contains("expired") {
                    return Ok(CreateTransferResponse {
                        task_id: None,
                        status: None,
                        need_password: false,
                        error: Some("分享已失效".to_string()),
                    });
                }

                // 检查分享是否不存在
                if err_msg.contains("不存在") || err_msg.contains("not found") {
                    return Ok(CreateTransferResponse {
                        task_id: None,
                        status: None,
                        need_password: false,
                        error: Some("分享不存在".to_string()),
                    });
                }

                // 其他错误
                Err(e)
            }
        }
    }

    /// 异步执行转存任务
    async fn spawn_task_execution(
        &self,
        task_id: String,
        share_link: ShareLink,
        cancellation_token: CancellationToken,
    ) {
        let client = self.client.clone();
        let tasks = self.tasks.clone();
        let download_manager = self.download_manager.clone();
        let folder_download_manager = self.folder_download_manager.clone();
        let config = self.config.clone();
        let app_config = self.app_config.clone();
        let persistence_manager = self.persistence_manager.lock().await.clone();
        let ws_manager = self.ws_manager.read().await.clone();

        tokio::spawn(async move {
            let result = Self::execute_task(
                client,
                tasks.clone(),
                download_manager,
                folder_download_manager,
                config,
                app_config,
                persistence_manager.clone(),
                ws_manager.clone(),
                &task_id,
                share_link,
                cancellation_token,
            )
                .await;

            if let Err(e) = result {
                let error_msg = e.to_string();
                error!("转存任务执行失败: task_id={}, error={}", task_id, error_msg);

                // 更新任务状态为失败
                if let Some(task_info) = tasks.get(&task_id) {
                    let mut task = task_info.task.write().await;
                    task.mark_transfer_failed(error_msg.clone());
                }

                // 🔥 发布失败事件
                if let Some(ref ws) = ws_manager {
                    ws.send_if_subscribed(
                        TaskEvent::Transfer(TransferEvent::Failed {
                            task_id: task_id.clone(),
                            error: error_msg.clone(),
                            error_type: "execution_error".to_string(),
                        }),
                        None,
                    );
                }

                // 🔥 更新持久化状态和错误信息
                if let Some(ref pm) = persistence_manager {
                    let pm_guard = pm.lock().await;

                    // 更新转存状态为失败
                    if let Err(e) = pm_guard.update_transfer_status(&task_id, "transfer_failed") {
                        warn!("更新转存任务状态失败: {}", e);
                    }

                    // 更新错误信息
                    if let Err(e) = pm_guard.update_task_error(&task_id, error_msg) {
                        warn!("更新转存任务错误信息失败: {}", e);
                    }
                }
            }
        });
    }

    /// 执行转存任务的核心逻辑
    async fn execute_task(
        client: Arc<NetdiskClient>,
        tasks: Arc<DashMap<String, TransferTaskInfo>>,
        download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        config: Arc<RwLock<TransferConfig>>,
        app_config: Arc<RwLock<AppConfig>>,
        persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,
        ws_manager: Option<Arc<WebSocketManager>>,
        task_id: &str,
        share_link: ShareLink,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 获取任务
        let task_info = tasks.get(task_id).context("任务不存在")?;
        let task = task_info.task.clone();
        drop(task_info);

        // 更新状态为检查中
        let old_status;
        {
            let mut t = task.write().await;
            old_status = format!("{:?}", t.status).to_lowercase();
            t.mark_checking();
        }

        // 🔥 发送状态变更事件
        if let Some(ref ws) = ws_manager {
            ws.send_if_subscribed(
                TaskEvent::Transfer(TransferEvent::StatusChanged {
                    task_id: task_id.to_string(),
                    old_status,
                    new_status: "checking_share".to_string(),
                }),
                None,
            );
        }

        // 检查取消
        if cancellation_token.is_cancelled() {
            return Ok(());
        }

        // 获取分享信息
        let share_info = {
            let t = task.read().await;
            t.share_info.clone().context("分享信息未设置")?
        };

        // 检查取消
        if cancellation_token.is_cancelled() {
            return Ok(());
        }

        // 列出分享文件
        let file_list = client
            .list_share_files(
                &share_link.short_key,
                &share_info.shareid,
                &share_info.bdstoken,
            )
            .await?;

        info!("获取到 {} 个文件", file_list.len());

        // 🔥 从分享文件列表中提取主要文件名
        let transfer_file_name = if !file_list.is_empty() {
            if file_list.len() == 1 {
                // 只有一个文件/文件夹，使用其名称
                Some(file_list[0].name.clone())
            } else {
                // 多个文件，使用第一个文件名 + 等x个文件
                Some(format!("{} 等{}个文件", file_list[0].name, file_list.len()))
            }
        } else {
            None
        };

        // 更新任务文件列表和文件名
        let old_status;
        {
            let mut t = task.write().await;
            old_status = format!("{:?}", t.status).to_lowercase();
            t.set_file_list(file_list.clone());
            t.mark_transferring();

            // 🔥 设置文件名（用于展示）
            if let Some(ref name) = transfer_file_name {
                t.set_file_name(name.clone());
            }
        }

        // 🔥 发送状态变更事件
        if let Some(ref ws) = ws_manager {
            ws.send_if_subscribed(
                TaskEvent::Transfer(TransferEvent::StatusChanged {
                    task_id: task_id.to_string(),
                    old_status,
                    new_status: "transferring".to_string(),
                }),
                None,
            );
        }

        // 🔥 更新持久化状态和文件名
        if let Some(ref pm_arc) = persistence_manager {
            let pm = pm_arc.lock().await;

            // 更新转存状态
            if let Err(e) = pm.update_transfer_status(task_id, "transferring") {
                warn!("更新转存任务状态失败: {}", e);
            }

            // 更新文件名
            if let Some(ref file_name) = transfer_file_name {
                if let Err(e) = pm.update_transfer_file_name(task_id, file_name.clone()) {
                    warn!("更新转存文件名失败: {}", e);
                }
            }
        }

        // 检查取消
        if cancellation_token.is_cancelled() {
            return Ok(());
        }

        // 执行转存
        let (save_path, save_fs_id) = {
            let t = task.read().await;
            (t.save_path.clone(), t.save_fs_id)
        };

        let fs_ids: Vec<u64> = file_list.iter().map(|f| f.fs_id).collect();
        let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);

        info!("执行转存: {} 个文件 -> {}", fs_ids.len(), save_path);
        let transfer_result = client
            .transfer_share_files(
                &share_info.shareid,
                &share_info.share_uk,
                &share_info.bdstoken,
                &fs_ids,
                &save_path,
                &referer,
            )
            .await;

        match transfer_result {
            Ok(result) => {
                if !result.success {
                    let error_msg = result.error.unwrap_or_else(|| "转存失败".to_string());

                    // 更新任务状态为失败
                    let old_status;
                    {
                        let mut t = task.write().await;
                        old_status = format!("{:?}", t.status).to_lowercase();
                        t.mark_transfer_failed(error_msg.clone());
                    }

                    // 🔥 发送状态变更事件
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Transfer(TransferEvent::StatusChanged {
                                task_id: task_id.to_string(),
                                old_status,
                                new_status: "transfer_failed".to_string(),
                            }),
                            None,
                        );
                    }

                    // 🔥 发布失败事件
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Transfer(TransferEvent::Failed {
                                task_id: task_id.to_string(),
                                error: error_msg.clone(),
                                error_type: "transfer_failed".to_string(),
                            }),
                            None,
                        );
                    }

                    // 🔥 更新持久化状态和错误信息
                    if let Some(ref pm_arc) = persistence_manager {
                        let pm = pm_arc.lock().await;

                        // 更新转存状态为失败
                        if let Err(e) = pm.update_transfer_status(task_id, "transfer_failed") {
                            warn!("更新转存任务状态失败: {}", e);
                        }

                        // 更新错误信息
                        if let Err(e) = pm.update_task_error(task_id, error_msg.clone()) {
                            warn!("更新转存任务错误信息失败: {}", e);
                        }
                    }

                    return Ok(());
                }

                info!("转存成功: {} 个文件", result.transferred_paths.len());

                // 更新最近使用的目录（同时保存 fs_id 和 path）并持久化
                {
                    let mut cfg = config.write().await;
                    cfg.recent_save_fs_id = Some(save_fs_id);
                    cfg.recent_save_path = Some(save_path.clone());

                    // 同步更新 AppConfig 并持久化
                    let mut app_cfg = app_config.write().await;
                    app_cfg.transfer.recent_save_fs_id = Some(save_fs_id);
                    app_cfg.transfer.recent_save_path = Some(save_path.clone());
                    if let Err(e) = app_cfg.save_to_file("config/app.toml").await {
                        warn!("保存转存配置失败: {}", e);
                    }
                }

                // 更新任务状态
                let (auto_download, file_list) = {
                    let mut t = task.write().await;
                    t.transferred_count = result.transferred_paths.len();
                    (t.auto_download, t.file_list.clone())
                };

                if auto_download {
                    // 启动自动下载
                    Self::start_auto_download(
                        client,
                        tasks.clone(),
                        download_manager,
                        folder_download_manager,
                        app_config,
                        persistence_manager.clone(),
                        ws_manager.clone(),
                        task_id,
                        result,
                        file_list,
                        save_path,
                        cancellation_token,
                    )
                        .await?;

                    // 自动下载场景：转存已完成，直接落盘为完成状态
                    if let Some(ref pm_arc) = persistence_manager {
                        let pm = pm_arc.lock().await;

                        if let Err(e) = pm.update_transfer_status(task_id, "completed") {
                            warn!("更新转存任务状态为完成失败: {}", e);
                        }

                        if let Err(e) = pm.on_task_completed(task_id) {
                            warn!("标记转存任务完成失败: {}", e);
                        } else {
                            info!(
                                "转存任务已标记完成（自动下载已启动）: task_id={}",
                                task_id
                            );
                        }
                    }

                    // 🔥 发布完成事件（自动下载场景）
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Transfer(TransferEvent::Completed {
                                task_id: task_id.to_string(),
                                completed_at: chrono::Utc::now().timestamp_millis(),
                            }),
                            None,
                        );
                    }
                } else {
                    // 标记为已转存
                    let old_status;
                    {
                        let mut t = task.write().await;
                        old_status = format!("{:?}", t.status).to_lowercase();
                        t.mark_transferred();
                    }

                    // 🔥 发送状态变更事件
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Transfer(TransferEvent::StatusChanged {
                                task_id: task_id.to_string(),
                                old_status,
                                new_status: "transferred".to_string(),
                            }),
                            None,
                        );
                    }

                    // 🔥 更新持久化状态
                    if let Some(ref pm_arc) = persistence_manager {
                        let pm = pm_arc.lock().await;

                        // 更新转存状态
                        if let Err(e) = pm.update_transfer_status(task_id, "transferred") {
                            warn!("更新转存任务状态失败: {}", e);
                        }

                        // 🔥 标记任务完成（只更新 .meta.status = completed，归档仍由启动/定时任务写 history.jsonl）
                        if let Err(e) = pm.on_task_completed(task_id) {
                            warn!("标记转存任务完成失败: {}", e);
                        } else {
                            info!("转存任务已标记完成，等待归档任务写入 history: task_id={}", task_id);
                        }
                    }

                    // 🔥 发布完成事件（仅转存不下载场景）
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Transfer(TransferEvent::Completed {
                                task_id: task_id.to_string(),
                                completed_at: chrono::Utc::now().timestamp_millis(),
                            }),
                            None,
                        );
                    }
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                let old_status;
                {
                    let mut t = task.write().await;
                    old_status = format!("{:?}", t.status).to_lowercase();
                    t.mark_transfer_failed(err_msg.clone());
                }

                // 🔥 发送状态变更事件
                if let Some(ref ws) = ws_manager {
                    ws.send_if_subscribed(
                        TaskEvent::Transfer(TransferEvent::StatusChanged {
                            task_id: task_id.to_string(),
                            old_status,
                            new_status: "transfer_failed".to_string(),
                        }),
                        None,
                    );
                }

                // 🔥 发布失败事件
                if let Some(ref ws) = ws_manager {
                    ws.send_if_subscribed(
                        TaskEvent::Transfer(TransferEvent::Failed {
                            task_id: task_id.to_string(),
                            error: err_msg.clone(),
                            error_type: "transfer_failed".to_string(),
                        }),
                        None,
                    );
                }

                // 🔥 更新持久化状态和错误信息
                if let Some(ref pm_arc) = persistence_manager {
                    let pm = pm_arc.lock().await;

                    // 更新转存状态为失败
                    if let Err(e) = pm.update_transfer_status(task_id, "transfer_failed") {
                        warn!("更新转存任务状态失败: {}", e);
                    }

                    // 更新错误信息
                    if let Err(e) = pm.update_task_error(task_id, err_msg.clone()) {
                        warn!("更新转存任务错误信息失败: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// 启动自动下载
    ///
    /// 转存成功后自动创建下载任务：
    /// 1. 获取本地下载路径（用户指定 > 下载配置默认目录）
    /// 2. 遍历转存的文件/文件夹，文件调用文件下载，文件夹调用文件夹下载
    /// 3. 启动下载状态监听，更新转存任务状态
    async fn start_auto_download(
        _client: Arc<NetdiskClient>,
        tasks: Arc<DashMap<String, TransferTaskInfo>>,
        download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        app_config: Arc<RwLock<AppConfig>>,
        persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,
        ws_manager: Option<Arc<WebSocketManager>>,
        task_id: &str,
        transfer_result: TransferResult,
        file_list: Vec<SharedFileInfo>,
        _save_path: String,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let dm_lock = download_manager.read().await;
        let dm = dm_lock.as_ref().context("下载管理器未设置")?;

        // 获取任务信息
        let task_info = tasks.get(task_id).context("任务不存在")?;
        let task = task_info.task.clone();
        drop(task_info);

        // 获取本地下载路径配置
        let (local_download_path, ask_each_time, default_download_dir) = {
            let t = task.read().await;
            let local_path = t.local_download_path.clone();
            drop(t);

            let cfg = app_config.read().await;
            let ask = cfg.download.ask_each_time;
            let default_dir = cfg.download.download_dir.clone();
            (local_path, ask, default_dir)
        };

        // 确定下载目录
        let download_dir = if let Some(ref path) = local_download_path {
            PathBuf::from(path)
        } else if ask_each_time {
            // 如果配置为每次询问且没有指定路径，需要返回特殊状态让前端弹窗
            // 这种情况下，前端需要重新调用 API 并提供 local_download_path
            warn!("自动下载需要选择本地保存位置，但未指定路径");
            let mut t = task.write().await;
            t.mark_transferred(); // 暂时标记为已转存，等待前端提供下载路径
            t.error = Some("需要选择本地保存位置".to_string());
            return Ok(());
        } else {
            default_download_dir
        };

        info!(
            "开始自动下载: task_id={}, 文件数={}, 下载目录={:?}",
            task_id,
            transfer_result.transferred_paths.len(),
            download_dir
        );

        // 确保下载目录存在
        if !download_dir.exists() {
            tokio::fs::create_dir_all(&download_dir)
                .await
                .context("创建下载目录失败")?;
        }

        // 分类收集需要下载的文件和文件夹
        let mut download_files: Vec<(u64, String, String, u64)> = Vec::new(); // (fs_id, remote_path, filename, size)
        let mut download_folders: Vec<String> = Vec::new(); // 文件夹路径

        for (idx, transferred_path) in transfer_result.transferred_paths.iter().enumerate() {
            // 尝试从 file_list 中获取对应文件的信息
            if idx < file_list.len() {
                let file_info = &file_list[idx];
                // 调试：打印 file_info 的 JSON
                match serde_json::to_string(file_info) {
                    Ok(json) => {
                        info!("file_info[{}] = {}", idx, json);
                    }
                    Err(e) => {
                        warn!("序列化 file_info 失败: idx={}, error={}", idx, e);
                    }
                }
                if file_info.is_dir {
                    // 文件夹：记录路径，稍后调用文件夹下载
                    download_folders.push(transferred_path.clone());
                    info!("发现文件夹: {}", transferred_path);
                } else {
                    // 文件：记录下载信息
                    download_files.push((
                        transfer_result
                            .transferred_fs_ids
                            .get(idx)
                            .copied()
                            .unwrap_or(file_info.fs_id),
                        transferred_path.clone(),
                        file_info.name.clone(),
                        file_info.size,
                    ));
                }
            }
        }

        info!(
            "分类完成: {} 个文件, {} 个文件夹",
            download_files.len(),
            download_folders.len()
        );

        // 创建文件下载任务
        let mut download_task_ids = Vec::new();
        for (fs_id, remote_path, filename, size) in download_files {
            match dm
                .create_task_with_dir(
                    fs_id,
                    remote_path.clone(),
                    filename.clone(),
                    size,
                    &download_dir,
                )
                .await
            {
                Ok(download_task_id) => {
                    // 🔥 设置下载任务关联的转存任务 ID（内存中）
                    if let Err(e) = dm.set_task_transfer_id(&download_task_id, task_id.to_string()).await {
                        warn!("设置下载任务关联转存任务(内存)失败: {}", e);
                    }

                    // 🔥 设置下载任务关联的转存任务 ID（持久化）
                    if let Some(ref pm_arc) = persistence_manager {
                        if let Err(e) = pm_arc
                            .lock()
                            .await
                            .set_download_transfer_task_id(&download_task_id, task_id.to_string())
                        {
                            warn!("设置下载任务关联转存任务(持久化)失败: {}", e);
                        }
                    }

                    // 启动下载任务
                    if let Err(e) = dm.start_task(&download_task_id).await {
                        warn!("启动下载任务失败: {}, error={}", download_task_id, e);
                    }
                    download_task_ids.push(download_task_id);
                }
                Err(e) => {
                    warn!(
                        "创建下载任务失败: {} -> {}, error={}",
                        remote_path, filename, e
                    );
                }
            }
        }

        // 释放下载管理器锁，避免后面持有两个锁
        drop(dm_lock);

        // 创建文件夹下载任务
        let mut folder_download_ids = Vec::new();
        if !download_folders.is_empty() {
            let fdm_lock = folder_download_manager.read().await;
            if let Some(ref fdm) = *fdm_lock {
                for folder_path in download_folders {
                    match fdm
                        .create_folder_download_with_dir(folder_path.clone(), &download_dir, None)
                        .await
                    {
                        Ok(folder_id) => {
                            info!("创建文件夹下载任务成功: {} -> {}", folder_path, folder_id);
                            folder_download_ids.push(folder_id.clone());

                            // 🔥 设置文件夹关联的转存任务 ID
                            fdm.set_folder_transfer_id(&folder_id, task_id.to_string()).await;
                        }
                        Err(e) => {
                            warn!("创建文件夹下载任务失败: {}, error={}", folder_path, e);
                        }
                    }
                }
            } else {
                warn!("文件夹下载管理器未设置，跳过文件夹下载");
            }
        }

        // 检查是否有任何下载任务创建成功
        if download_task_ids.is_empty() && folder_download_ids.is_empty() {
            warn!("没有下载任务创建成功");
            let mut t = task.write().await;
            t.mark_transferred(); // 标记为已转存，虽然没有文件需要下载

            // 无下载任务也要将转存状态标记为完成（持久化）
            if let Some(ref pm_arc) = persistence_manager {
                let pm = pm_arc.lock().await;

                if let Err(e) = pm.update_transfer_status(task_id, "completed") {
                    warn!("更新转存任务状态为完成失败: {}", e);
                }

                if let Err(e) = pm.on_task_completed(task_id) {
                    warn!("标记转存任务完成失败: {}", e);
                } else {
                    info!("转存任务已标记完成（无自动下载任务）: task_id={}", task_id);
                }
            }

            return Ok(());
        }

        // 更新转存任务状态为下载中
        let (all_task_ids, old_status) = {
            let mut t = task.write().await;
            let old_status = format!("{:?}", t.status).to_lowercase();
            // 合并文件下载和文件夹下载的任务 ID
            let mut all_task_ids = download_task_ids.clone();
            all_task_ids.extend(
                folder_download_ids
                    .iter()
                    .map(|id| format!("folder:{}", id)),
            );
            t.mark_downloading(all_task_ids.clone());
            (all_task_ids, old_status)
        };

        // 🔥 发送状态变更事件
        if let Some(ref ws) = ws_manager {
            ws.send_if_subscribed(
                TaskEvent::Transfer(TransferEvent::StatusChanged {
                    task_id: task_id.to_string(),
                    old_status,
                    new_status: "downloading".to_string(),
                }),
                None,
            );
        }

        // 🔥 更新持久化状态和关联下载任务 ID
        if let Some(ref pm_arc) = persistence_manager {
            if let Err(e) = pm_arc
                .lock()
                .await
                .update_transfer_status(task_id, "downloading")
            {
                warn!("更新转存任务状态失败: {}", e);
            }
            if let Err(e) = pm_arc
                .lock()
                .await
                .update_transfer_download_ids(task_id, all_task_ids)
            {
                warn!("更新转存任务关联下载 ID 失败: {}", e);
            }
        }

        info!(
            "自动下载已启动: task_id={}, 文件下载任务数={}, 文件夹下载任务数={}",
            task_id,
            download_task_ids.len(),
            folder_download_ids.len()
        );

        // 启动下载状态监听
        Self::start_download_status_watcher(
            tasks,
            download_manager,
            ws_manager,
            task_id.to_string(),
            cancellation_token,
        );

        Ok(())
    }

    /// 启动下载状态监听任务
    ///
    /// 通过轮询方式监听关联的下载任务状态，当所有下载完成或失败时更新转存任务状态
    fn start_download_status_watcher(
        tasks: Arc<DashMap<String, TransferTaskInfo>>,
        download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
        ws_manager: Option<Arc<WebSocketManager>>,
        task_id: String,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn(async move {
            const CHECK_INTERVAL: Duration = Duration::from_secs(2);
            const DOWNLOAD_TIMEOUT_HOURS: i64 = 24;

            loop {
                tokio::time::sleep(CHECK_INTERVAL).await;

                // 检查取消
                if cancellation_token.is_cancelled() {
                    info!("下载状态监听被取消: task_id={}", task_id);
                    break;
                }

                // 获取转存任务
                let task_info = match tasks.get(&task_id) {
                    Some(t) => t,
                    None => {
                        info!("转存任务已删除，停止监听: task_id={}", task_id);
                        break;
                    }
                };

                let task = task_info.task.clone();
                drop(task_info);

                let (status, download_task_ids, download_started_at) = {
                    let t = task.read().await;
                    (
                        t.status.clone(),
                        t.download_task_ids.clone(),
                        t.download_started_at,
                    )
                };

                // 非下载中状态，停止监听
                if status != TransferStatus::Downloading {
                    break;
                }

                // 超时检查
                if let Some(started_at) = download_started_at {
                    let now = chrono::Utc::now().timestamp();
                    let elapsed_hours = (now - started_at) / 3600;
                    if elapsed_hours > DOWNLOAD_TIMEOUT_HOURS {
                        warn!(
                            "下载超时: task_id={}, 已超过 {} 小时",
                            task_id, elapsed_hours
                        );
                        let mut t = task.write().await;
                        t.status = TransferStatus::DownloadFailed;
                        t.error = Some(format!("下载超时（超过{}小时）", DOWNLOAD_TIMEOUT_HOURS));
                        t.touch();
                        break;
                    }
                }

                // 检查所有关联下载任务的状态
                let final_status =
                    Self::aggregate_download_status(&download_manager, &download_task_ids).await;

                if let Some(new_status) = final_status {
                    info!(
                        "下载状态聚合完成: task_id={}, status={:?}",
                        task_id, new_status
                    );
                    let old_status;
                    {
                        let mut t = task.write().await;
                        old_status = format!("{:?}", t.status).to_lowercase();
                        t.status = new_status.clone();
                        t.touch();
                    }

                    // 🔥 发送状态变更事件
                    if let Some(ref ws) = ws_manager {
                        ws.send_if_subscribed(
                            TaskEvent::Transfer(TransferEvent::StatusChanged {
                                task_id: task_id.to_string(),
                                old_status,
                                new_status: format!("{:?}", new_status).to_lowercase(),
                            }),
                            None,
                        );
                    }

                    break;
                }
            }
        });
    }

    /// 聚合多个下载任务状态
    ///
    /// 返回 None 表示仍在进行中，不需要状态转换
    async fn aggregate_download_status(
        download_manager: &Arc<RwLock<Option<Arc<DownloadManager>>>>,
        download_task_ids: &[String],
    ) -> Option<TransferStatus> {
        let dm_lock = download_manager.read().await;
        let dm = match dm_lock.as_ref() {
            Some(m) => m,
            None => return Some(TransferStatus::DownloadFailed),
        };

        let mut completed_count = 0;
        let mut failed_count = 0;
        let mut downloading_count = 0;
        let mut paused_count = 0;
        let mut cancelled_count = 0;

        for task_id in download_task_ids {
            if let Some(task) = dm.get_task(task_id).await {
                match task.status {
                    TaskStatus::Completed => completed_count += 1,
                    TaskStatus::Failed => failed_count += 1,
                    TaskStatus::Downloading => downloading_count += 1,
                    TaskStatus::Decrypting => downloading_count += 1, // 解密中视为进行中
                    TaskStatus::Paused => paused_count += 1,
                    TaskStatus::Pending => downloading_count += 1, // 视为进行中
                }
            } else {
                // 任务不存在，视为已取消
                cancelled_count += 1;
            }
        }

        let total = download_task_ids.len();

        // 仍有任务在下载中
        if downloading_count > 0 {
            return None;
        }

        // 全部暂停，保持 Downloading 状态
        if paused_count == total {
            return None;
        }

        // 全部完成
        if completed_count == total {
            return Some(TransferStatus::Completed);
        }

        // 全部取消，回退到已转存
        if cancelled_count == total {
            return Some(TransferStatus::Transferred);
        }

        // 存在失败（无进行中任务）
        if failed_count > 0 {
            return Some(TransferStatus::DownloadFailed);
        }

        // 混合状态（部分完成+部分取消），视为完成
        if completed_count > 0 && failed_count == 0 {
            return Some(TransferStatus::Completed);
        }

        None
    }

    /// 获取所有任务（包括当前任务和历史任务）
    pub async fn get_all_tasks(&self) -> Vec<TransferTask> {
        let mut result = Vec::new();

        // 获取当前任务
        for entry in self.tasks.iter() {
            if let Ok(task) = entry.value().task.try_read() {
                result.push(task.clone());
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

            // 从数据库查询已完成的转存任务
            if let Some((history_tasks, _total)) = pm.get_history_tasks_by_type_and_status(
                "transfer",
                "completed",
                false,  // don't exclude backup (transfer tasks are not backup tasks)
                0,
                500,   // 限制最多500条
            ) {
                for metadata in history_tasks {
                    // 排除已在当前任务中的（避免重复）
                    if !self.tasks.contains_key(&metadata.task_id) {
                        if let Some(task) = Self::convert_history_to_task(&metadata) {
                            result.push(task);
                        }
                    }
                }
            }
        }

        // 按创建时间倒序排序
        result.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        result
    }

    /// 将历史元数据转换为转存任务
    fn convert_history_to_task(metadata: &TaskMetadata) -> Option<TransferTask> {
        // 验证必要字段
        let share_url = metadata.share_link.clone()?;
        let save_path = metadata.transfer_target_path.clone()?;
        // save_fs_id 在 metadata 中不存在，使用默认值 0（对于已完成的历史任务不重要）
        let save_fs_id = 0;

        // 解析分享信息（如果存在）
        let share_info = metadata
            .share_info_json
            .as_ref()
            .and_then(|json_str| serde_json::from_str::<SharePageInfo>(json_str).ok());

        // 解析文件列表（如果存在，需要从其他地方获取，这里暂时为空）
        // TODO: 如果 metadata 中有文件列表信息，可以在这里恢复
        let file_list = Vec::new();

        // 转换转存状态
        let status = match metadata.transfer_status.as_deref() {
            Some("completed") => TransferStatus::Completed,
            Some("transferred") => TransferStatus::Transferred,
            Some("transfer_failed") => TransferStatus::TransferFailed,
            Some("download_failed") => TransferStatus::DownloadFailed,
            _ => TransferStatus::Completed, // 已完成的任务默认使用 Completed
        };

        Some(TransferTask {
            id: metadata.task_id.clone(),
            share_url,
            password: metadata.share_pwd.clone(),
            save_path,
            save_fs_id,
            auto_download: metadata.auto_download.unwrap_or(false), // 从持久化中读取
            local_download_path: None,
            status,
            error: None,
            download_task_ids: metadata.download_task_ids.clone(),
            share_info,
            file_list,
            // 使用 download_task_ids 的长度来推断总文件数（已完成的历史任务）
            // 如果没有下载任务，说明只是转存没有下载，使用 0
            transferred_count: metadata.download_task_ids.len(),
            total_count: metadata.download_task_ids.len(),
            created_at: metadata.created_at.timestamp(),
            updated_at: metadata.updated_at.timestamp(),
            failed_download_ids: Vec::new(),
            completed_download_ids: Vec::new(),
            download_started_at: None,
            file_name: metadata.transfer_file_name.clone(), // 从持久化中读取
        })
    }

    /// 获取单个任务
    pub async fn get_task(&self, id: &str) -> Option<TransferTask> {
        if let Some(task_info) = self.tasks.get(id) {
            Some(task_info.task.read().await.clone())
        } else {
            None
        }
    }

    /// 取消任务
    pub fn cancel_task(&self, id: &str) -> Result<()> {
        if let Some(task_info) = self.tasks.get(id) {
            task_info.cancellation_token.cancel();
            info!("取消转存任务: {}", id);
            Ok(())
        } else {
            anyhow::bail!("任务不存在")
        }
    }

    /// 删除任务
    pub async fn remove_task(&self, id: &str) -> Result<()> {
        // 先尝试从内存中移除
        if let Some((_, task_info)) = self.tasks.remove(id) {
            task_info.cancellation_token.cancel();
            info!("删除转存任务（内存中）: {}", id);
        } else {
            // 不在内存中，仍然执行持久化清理，保证幂等
            info!("删除转存任务（历史/已归档）: {}", id);
        }

        // 🔥 清理持久化文件
        if let Some(pm_arc) = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
        {
            if let Err(e) = pm_arc.lock().await.on_task_deleted(id) {
                warn!("清理转存任务持久化文件失败: {}", e);
            }
        } else {
            warn!("持久化管理器未初始化，无法清理转存任务: {}", id);
        }

        // 🔥 发送删除事件
        self.publish_event(TransferEvent::Deleted {
            task_id: id.to_string(),
        })
            .await;

        Ok(())
    }

    /// 获取配置
    pub async fn get_config(&self) -> TransferConfig {
        self.config.read().await.clone()
    }

    /// 更新配置
    pub async fn update_config(&self, config: TransferConfig) {
        let mut cfg = self.config.write().await;
        *cfg = config;
    }

    // ========================================================================
    // 🔥 任务恢复
    // ========================================================================

    /// 从恢复信息创建任务
    ///
    /// 用于程序启动时恢复未完成的转存任务
    /// 根据保存的状态决定恢复策略：
    /// - checking_share/transferring: 任务需要重新执行（标记为需要重试）
    /// - transferred: 已转存但未下载，可直接恢复
    /// - downloading: 恢复下载状态监听
    ///
    /// # Arguments
    /// * `recovery_info` - 从持久化文件恢复的任务信息
    ///
    /// # Returns
    /// 恢复的任务 ID
    pub async fn restore_task(&self, recovery_info: TransferRecoveryInfo) -> Result<String> {
        let task_id = recovery_info.task_id.clone();

        // 检查任务是否已存在
        if self.tasks.contains_key(&task_id) {
            anyhow::bail!("任务 {} 已存在，无法恢复", task_id);
        }

        // 创建恢复任务
        let mut task = TransferTask::new(
            recovery_info.share_link.clone(),
            recovery_info.share_pwd.clone(),
            recovery_info.target_path.clone(),
            0,     // save_fs_id 未保存，设为 0
            false, // auto_download 稍后设置
            None,
        );

        // 恢复任务 ID（保持原有 ID）
        task.id = task_id.clone();
        task.created_at = recovery_info.created_at;

        // 根据保存的状态恢复任务状态
        let status = recovery_info.status.as_deref().unwrap_or("checking_share");
        match status {
            "transferred" => {
                // 已转存，标记为已转存状态
                task.status = TransferStatus::Transferred;
                info!(
                    "恢复转存任务(已转存): id={}, target={}",
                    task_id, recovery_info.target_path
                );
            }
            "downloading" => {
                // 下载中，恢复下载状态
                task.status = TransferStatus::Downloading;
                task.download_task_ids = recovery_info.download_task_ids.clone();
                info!(
                    "恢复转存任务(下载中): id={}, 关联下载任务数={}",
                    task_id,
                    recovery_info.download_task_ids.len()
                );
            }
            "completed" => {
                // 已完成，不需要恢复
                info!("任务 {} 已完成，无需恢复", task_id);
                return Ok(task_id);
            }
            _ => {
                // checking_share/transferring 状态需要重试
                // 标记为失败，让用户手动重试
                task.status = TransferStatus::TransferFailed;
                task.error = Some("任务中断，请重新创建任务".to_string());
                info!("恢复转存任务(需重试): id={}, 原状态={}", task_id, status);
            }
        }

        let task_arc = Arc::new(RwLock::new(task));
        let cancellation_token = CancellationToken::new();

        // 存储任务
        self.tasks.insert(
            task_id.clone(),
            TransferTaskInfo {
                task: task_arc.clone(),
                cancellation_token: cancellation_token.clone(),
            },
        );

        // 如果是下载中状态，启动下载状态监听
        if status == "downloading" && !recovery_info.download_task_ids.is_empty() {
            let ws_manager = self.ws_manager.read().await.clone();
            Self::start_download_status_watcher(
                self.tasks.clone(),
                self.download_manager.clone(),
                ws_manager,
                task_id.clone(),
                cancellation_token,
            );
        }

        Ok(task_id)
    }

    /// 批量恢复任务
    ///
    /// 从恢复信息列表批量创建任务
    ///
    /// # Arguments
    /// * `recovery_infos` - 恢复信息列表
    ///
    /// # Returns
    /// (成功数, 失败数)
    pub async fn restore_tasks(&self, recovery_infos: Vec<TransferRecoveryInfo>) -> (usize, usize) {
        let mut success = 0;
        let mut failed = 0;

        for info in recovery_infos {
            match self.restore_task(info).await {
                Ok(_) => success += 1,
                Err(e) => {
                    warn!("恢复转存任务失败: {}", e);
                    failed += 1;
                }
            }
        }

        info!("批量恢复转存任务完成: {} 成功, {} 失败", success, failed);
        (success, failed)
    }
}

#[cfg(test)]
mod tests {
    // 测试需要模拟 NetdiskClient，这里先跳过
}
