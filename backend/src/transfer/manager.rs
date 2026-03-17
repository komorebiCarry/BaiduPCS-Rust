// 转存任务管理器

use crate::config::{AppConfig, TransferConfig};
use crate::downloader::{DownloadManager, FolderDownloadManager, FolderStatus, TaskStatus};
use crate::netdisk::NetdiskClient;
use crate::persistence::{
    PersistenceManager, TaskMetadata, TransferRecoveryInfo,
};
use crate::server::events::{TaskEvent, TransferEvent};
use crate::server::websocket::WebSocketManager;
use crate::transfer::task::{TransferStatus, TransferTask};
use crate::transfer::types::{CleanupResult, CleanupStatus, ShareLink, SharePageInfo, SharedFileInfo, TransferResult};
use anyhow::{Context, Result};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// 转存任务信息（包含任务和取消令牌）
pub struct TransferTaskInfo {
    pub task: Arc<RwLock<TransferTask>>,
    pub cancellation_token: CancellationToken,
}

/// 转存管理器
pub struct TransferManager {
    /// 网盘客户端（共享引用，代理热更新时自动生效）
    client: Arc<StdRwLock<NetdiskClient>>,
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
    /// 是否为分享直下任务
    /// 分享直下任务会自动创建临时目录，下载完成后自动清理
    #[allow(dead_code)]
    pub is_share_direct_download: bool,
    /// 用户选择的文件 fs_id 列表（可选）
    /// 为空或未提供时转存所有文件（向后兼容）
    pub selected_fs_ids: Option<Vec<u64>>,
    /// 用户选择的文件完整信息列表（可选）
    /// 前端在文件选择模式下传入，包含选中文件的名称、大小、类型等信息
    pub selected_files: Option<Vec<SharedFileInfo>>,
}

/// 创建转存任务响应
#[derive(Debug, Clone)]
pub struct CreateTransferResponse {
    pub task_id: Option<String>,
    pub status: Option<TransferStatus>,
    pub need_password: bool,
    pub error: Option<String>,
}

/// 预览分享结果（包含文件列表和分享信息）
pub struct PreviewShareResult {
    pub files: Vec<SharedFileInfo>,
    pub short_key: String,
    pub shareid: String,
    pub uk: String,
    pub bdstoken: String,
}

/// handle_transfer_error 的返回值，区分恢复成功、友好失败、无法识别三种场景
enum TransferErrorHandled {
    /// 分享直下 -30 恢复成功，携带恢复的文件信息 (name, Option<fs_id>, Option<temp_dir_path>, source_share_path)
    Recovered(Vec<(String, Option<u64>, Option<String>, String)>),
    /// 已处理为友好错误消息
    Failed(String),
    /// 无法提取/识别错误码，调用方应使用原始错误消息
    Unrecognized,
}

impl TransferManager {
    /// 创建新的转存管理器
    pub fn new(
        client: Arc<StdRwLock<NetdiskClient>>,
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

    /// 🔥 热更新网盘客户端（代理切换时由 ProxyHotUpdater 调用）
    pub fn update_netdisk_client(&self, new_client: NetdiskClient) {
        *self.client.write().unwrap() = new_client;
        info!("✓ TransferManager NetdiskClient 已热更新");
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

    /// 预览分享链接中的文件列表（不执行转存）
    ///
    /// 步骤：
    /// 1. parse_share_link(share_url) → 提取 short_key 和可能的密码
    /// 2. access_share_page(short_key, password) → 获取 SharePageInfo
    /// 3. 如果有密码，调用 verify_share_password() → 验证密码并获取 sekey
    /// 4. list_share_files(short_key, shareid, uk, bdstoken, page, num) → 获取根目录文件列表
    /// 5. 返回 PreviewShareResult（文件列表 + 分享信息）
    pub async fn preview_share(
        &self,
        share_url: &str,
        password: Option<String>,
        page: u32,
        num: u32,
    ) -> Result<PreviewShareResult> {
        info!("预览分享链接: url={}", share_url);

        // 1. 解析分享链接
        let share_link = self.client.read().unwrap().parse_share_link(share_url)?;

        // 合并密码：请求中的密码 > 链接中的密码
        let password = password.or(share_link.password.clone());

        // 🔥 从共享引用快照当前客户端
        let client = self.client.read().unwrap().clone();

        // 2. 访问分享页面，获取分享信息
        let share_info = client
            .access_share_page(&share_link.short_key, &password, true)
            .await?;

        // 3. 如果有密码，验证密码
        if let Some(ref pwd) = password {
            let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
            client
                .verify_share_password(
                    &share_info.shareid,
                    &share_info.share_uk,
                    &share_info.bdstoken,
                    pwd,
                    &referer,
                )
                .await?;
            info!("预览: 提取码验证成功");
        }

        // 4. 获取文件列表（根目录，由前端传入分页参数）
        let list_result = client
            .list_share_files(
                &share_link.short_key,
                &share_info.bdstoken,
                page,
                num,
            )
            .await?;

        // 用根目录响应中的 uk/shareid 补充（access_share_page 可能提取失败）
        let uk = if !list_result.uk.is_empty() {
            list_result.uk
        } else {
            share_info.uk
        };
        let shareid = if !list_result.shareid.is_empty() {
            list_result.shareid
        } else {
            share_info.shareid
        };

        info!("预览: 获取到 {} 个文件, uk={}, shareid={}", list_result.files.len(), uk, shareid);
        Ok(PreviewShareResult {
            files: list_result.files,
            short_key: share_link.short_key,
            shareid,
            uk,
            bdstoken: share_info.bdstoken,
        })
    }

    /// 浏览分享链接中指定目录的文件列表
    ///
    /// 用于文件夹导航：前端点击文件夹后，调用此方法获取子目录内容。
    /// 需要传入首次预览时获取的 share_info，避免重复访问分享页面。
    pub async fn preview_share_dir(
        &self,
        short_key: &str,
        shareid: &str,
        uk: &str,
        bdstoken: &str,
        dir: &str,
        page: u32,
        num: u32,
    ) -> Result<Vec<SharedFileInfo>> {
        info!("浏览分享子目录: short_key={}, dir={}, page={}, num={}", short_key, dir, page, num);

        let client = self.client.read().unwrap().clone();
        let file_list = client
            .list_share_files_in_dir(short_key, shareid, uk, bdstoken, dir, page, num)
            .await?;

        info!("子目录: 获取到 {} 个文件, dir={}", file_list.len(), dir);
        Ok(file_list)
    }

    /// 创建转存任务
    ///
    /// 如果需要密码，返回 need_password=true
    /// 如果密码错误，返回错误信息
    pub async fn create_task(
        &self,
        request: CreateTransferRequest,
    ) -> Result<CreateTransferResponse> {
        info!("创建转存任务: url={}, is_share_direct_download={}", request.share_url, request.is_share_direct_download);

        // 1. 解析分享链接
        let share_link = self.client.read().unwrap().parse_share_link(&request.share_url)?;

        // 合并密码：请求中的密码 > 链接中的密码
        let password = request.password.or(share_link.password.clone());

        // 重新创建 share_link 用于后续使用（避免部分移动问题）
        let share_link = ShareLink {
            short_key: share_link.short_key,
            raw_url: share_link.raw_url,
            password: password.clone(), // 密码已提取
        };

        // 2. 处理分享直下模式
        let (save_path, save_fs_id, auto_download, temp_dir) = if request.is_share_direct_download {
            // 分享直下模式：生成临时目录路径
            let task_uuid = uuid::Uuid::new_v4().to_string();
            let app_cfg = self.app_config.read().await;
            let temp_dir_base = &app_cfg.share_direct_download.temp_dir;
            // 确保临时目录路径格式正确：{config.temp_dir}{uuid}/
            let temp_dir = format!("{}/{}/", temp_dir_base.trim_end_matches('/'), task_uuid);
            info!("分享直下模式: 临时目录={}", temp_dir);

            // 分享直下强制自动下载
            (temp_dir.clone(), 0u64, true, Some(temp_dir))
        } else {
            // 普通转存模式
            let auto_download = match request.auto_download {
                Some(v) => v,
                None => {
                    let config = self.config.read().await;
                    config.default_behavior == "transfer_and_download"
                }
            };
            (request.save_path.clone(), request.save_fs_id, auto_download, None)
        };

        // 3. 创建任务
        let mut task = TransferTask::new(
            request.share_url.clone(),
            password.clone(),
            save_path.clone(),
            save_fs_id,
            auto_download,
            request.local_download_path.clone(),
        );

        // 设置分享直下相关字段
        if request.is_share_direct_download {
            task.is_share_direct_download = true;
            task.temp_dir = temp_dir.clone();
        }

        // 设置选择性转存字段
        task.selected_fs_ids = request.selected_fs_ids.clone();
        task.selected_files = request.selected_files.clone();

        let task_id = task.id.clone();

        // 4. 访问分享页面，获取分享信息
        let client = self.client.read().unwrap().clone();
        let share_info_result = client
            .access_share_page(&share_link.short_key, &share_link.password, true)
            .await;

        match share_info_result {
            Ok(info) => {
                // 如果有密码，先验证密码
                if let Some(ref pwd) = password {
                    let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
                    match client
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
                        save_path.clone(),
                        auto_download,
                        None, // 文件名在获取文件列表后更新
                    ) {
                        warn!("注册转存任务到持久化管理器失败: {}", e);
                    }

                    // 🔥 如果是分享直下任务，更新分享直下相关字段
                    if request.is_share_direct_download {
                        if let Err(e) = pm_arc.lock().await.update_share_direct_download_info(
                            &task_id,
                            true,
                            temp_dir.clone(),
                        ) {
                            warn!("更新分享直下信息失败: {}", e);
                        }
                    }
                }

                // 🔥 发送任务创建事件
                self.publish_event(TransferEvent::Created {
                    task_id: task_id.clone(),
                    share_url: request.share_url.clone(),
                    save_path: save_path.clone(),
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
        client_shared: Arc<StdRwLock<NetdiskClient>>,
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
        // 🔥 从共享引用快照当前客户端（代理热更新后自动生效）
        let client = Arc::new(client_shared.read().unwrap().clone());

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
        // 如果用户已选择了具体文件（selected_fs_ids 非空），只需拉第一页用于展示文件名
        // 如果是全选模式（selected_fs_ids 为空），需要循环分页拉取全部 fs_id
        let has_selected_fs_ids = {
            let t = task.read().await;
            t.selected_fs_ids.as_ref().map_or(false, |ids| !ids.is_empty())
        };

        let file_list = if has_selected_fs_ids {
            // 用户已选择文件，只拉第一页用于展示文件名
            let result = client
                .list_share_files(
                    &share_link.short_key,
                    &share_info.bdstoken,
                    1,
                    100,
                )
                .await?;
            result.files
        } else {
            // 全选模式，循环分页拉取全部
            let mut all_files = Vec::new();
            let page_size: u32 = 100;
            let mut page: u32 = 1;
            loop {
                let result = client
                    .list_share_files(
                        &share_link.short_key,
                        &share_info.bdstoken,
                        page,
                        page_size,
                    )
                    .await?;
                let batch_len = result.files.len();
                all_files.extend(result.files);
                if (batch_len as u32) < page_size {
                    break;
                }
                page += 1;
            }
            all_files
        };

        info!("获取到 {} 个文件", file_list.len());

        // 🔥 根据 selected_fs_ids 和 selected_files 构建过滤后的文件列表
        // 优先使用前端传入的 selected_files（包含完整文件信息，支持子目录选择场景）
        // 如果没有 selected_files，则从根目录 file_list 中按 selected_fs_ids 过滤
        let (selected_fs_ids_snapshot, selected_files_snapshot) = {
            let t = task.read().await;
            (t.selected_fs_ids.clone(), t.selected_files.clone())
        };
        let filtered_file_list = if let Some(ref selected_files) = selected_files_snapshot {
            if !selected_files.is_empty() {
                selected_files.clone()
            } else {
                file_list.clone()
            }
        } else if let Some(ref selected) = selected_fs_ids_snapshot {
            if !selected.is_empty() {
                let selected_set: std::collections::HashSet<u64> = selected.iter().copied().collect();
                file_list.iter().filter(|f| selected_set.contains(&f.fs_id)).cloned().collect::<Vec<_>>()
            } else {
                file_list.clone()
            }
        } else {
            file_list.clone()
        };

        // 🔥 从过滤后的文件列表中提取主要文件名
        let transfer_file_name = if !filtered_file_list.is_empty() {
            if filtered_file_list.len() == 1 {
                // 只有一个文件/文件夹，使用其名称
                Some(filtered_file_list[0].name.clone())
            } else {
                // 多个文件，使用第一个文件名 + 等x个文件
                Some(format!("{} 等{}个文件", filtered_file_list[0].name, filtered_file_list.len()))
            }
        } else {
            None
        };

        // 更新任务文件列表和文件名（使用过滤后的列表）
        let old_status;
        {
            let mut t = task.write().await;
            old_status = format!("{:?}", t.status).to_lowercase();
            t.set_file_list(filtered_file_list.clone());
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

            // 更新文件列表
            match serde_json::to_string(&filtered_file_list) {
                Ok(json) => {
                    if let Err(e) = pm.update_transfer_file_list(task_id, json) {
                        warn!("更新转存文件列表失败: {}", e);
                    }
                }
                Err(e) => warn!("序列化文件列表失败: {}", e),
            }
        }

        // 检查取消
        if cancellation_token.is_cancelled() {
            return Ok(());
        }

        // 执行转存
        let (save_path, save_fs_id, is_share_direct_download) = {
            let t = task.read().await;
            (t.save_path.clone(), t.save_fs_id, t.is_share_direct_download)
        };

        info!("转存参数: save_path={}, is_share_direct_download={}", save_path, is_share_direct_download);

        // 分享直下模式：转存前先在网盘上创建临时目录
        if is_share_direct_download {
            info!("分享直下模式: 创建临时目录 {}", save_path);

            // 先确保父目录（/.bpr_share_temp/）存在
            // 注意：百度 create_folder API 在文件夹已存在时不报错，而是静默重命名（加时间戳后缀）
            // 所以必须先检查父目录是否已存在，已存在就跳过创建，避免产生多余的重命名文件夹
            let parent_path = save_path.trim_end_matches('/');
            if let Some(parent) = parent_path.rsplit_once('/').map(|(p, _)| p) {
                if !parent.is_empty() {
                    let parent_trimmed = parent.trim_end_matches('/');
                    // 列出根目录检查父目录是否已存在
                    let parent_exists = match client.get_file_list("/", 1, 1000).await {
                        Ok(list) => list.list.iter().any(|f| {
                            f.isdir == 1 && f.path.trim_end_matches('/') == parent_trimmed
                        }),
                        Err(e) => {
                            warn!("检查父目录是否存在失败，将尝试创建: {}", e);
                            false
                        }
                    };

                    if parent_exists {
                        info!("分享直下模式: 父目录已存在，跳过创建 {}", parent);
                    } else {
                        info!("分享直下模式: 创建父目录 {}", parent);
                        match client.create_folder(parent).await {
                            Ok(resp) => {
                                // 校验返回路径是否被百度重命名
                                let actual = resp.path.trim_end_matches('/');
                                if !actual.is_empty() && actual != parent_trimmed {
                                    warn!("父目录被百度重命名: 期望={}, 实际={}", parent_trimmed, actual);
                                    let _ = client.delete_files(&[actual.to_string()]).await;
                                    anyhow::bail!("创建父目录失败: 路径被百度重命名为 {}", actual);
                                }
                            }
                            Err(e) => {
                                let err_msg = e.to_string();
                                if !err_msg.contains("errno=-8") {
                                    warn!("创建父目录失败（可能已存在）: {}", err_msg);
                                }
                            }
                        }
                    }
                }
            }

            // 再创建完整的临时目录（UUID子目录）
            let expected_sub = save_path.trim_end_matches('/');
            match client.create_folder(&save_path).await {
                Ok(resp) => {
                    let actual = resp.path.trim_end_matches('/');
                    if !actual.is_empty() && actual != expected_sub {
                        warn!("临时目录被百度重命名: 期望={}, 实际={}", expected_sub, actual);
                        let _ = client.delete_files(&[actual.to_string()]).await;
                        anyhow::bail!("创建临时目录失败: 路径被百度重命名为 {}", actual);
                    }
                    info!("临时目录创建成功: {}", save_path);
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if !err_msg.contains("errno=-8") {
                        error!("创建临时目录失败: {}", err_msg);
                        anyhow::bail!("创建临时目录失败: {}", err_msg);
                    }
                    info!("临时目录已存在，继续转存: {}", save_path);
                }
            }
        }

        // 构建 fs_ids：根据 selected_fs_ids 决定转存哪些文件
        let selected_fs_ids = {
            let t = task.read().await;
            t.selected_fs_ids.clone()
        };
        let fs_ids = build_fs_ids(&file_list, &selected_fs_ids);

        // 根据实际 fs_ids 更新 total_count
        {
            let mut t = task.write().await;
            t.total_count = fs_ids.len();
        }

        let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);

        // ========== 转存请求摘要日志 ==========
        {
            let t = task.read().await;
            let unique_count = {
                let set: std::collections::HashSet<u64> = fs_ids.iter().copied().collect();
                set.len()
            };
            let dup_count = fs_ids.len() - unique_count;

            // 统计同名文件（basename 维度）
            let mut name_counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
            for f in &file_list {
                *name_counts.entry(f.name.as_str()).or_insert(0) += 1;
            }
            let mut dup_basenames: Vec<(&str, usize)> = name_counts
                .into_iter()
                .filter(|(_, c)| *c > 1)
                .collect();
            dup_basenames.sort_by(|a, b| b.1.cmp(&a.1));
            dup_basenames.truncate(10);

            info!(
                "转存请求摘要: internal_task_id={}, share_key={}, save_path={}, \
                 is_share_direct_download={}, selected_fs_ids_count={}, selected_files_count={}, \
                 fs_ids_count={}, unique_fs_ids={}, dup_fs_ids={}, dup_basenames={:?}",
                task_id,
                share_link.short_key,
                save_path,
                is_share_direct_download,
                selected_fs_ids.as_ref().map_or(0, |v| v.len()),
                t.selected_files.as_ref().map_or(0, |v| v.len()),
                fs_ids.len(),
                unique_count,
                dup_count,
                dup_basenames,
            );
        }
        let transfer_result = client
            .transfer_share_files(
                &share_info.shareid,
                &share_info.share_uk,
                &share_info.bdstoken,
                &fs_ids,
                &save_path,
                &referer,
                Some(task_id),
            )
            .await;

        // 如果转存失败且错误是"路径不存在"(errno=2)，尝试创建目录后重试一次
        let transfer_result = match &transfer_result {
            Ok(result) if !result.success => {
                let err_msg = result.error.as_deref().unwrap_or("");
                if err_msg.contains("errno\":2") || err_msg.contains("路径不存在") {
                    warn!("转存路径不存在，尝试创建目录后重试: {}", save_path);

                    // 创建父目录
                    let parent_path = save_path.trim_end_matches('/');
                    if let Some(parent) = parent_path.rsplit_once('/').map(|(p, _)| p) {
                        if !parent.is_empty() {
                            let _ = client.create_folder(parent).await;
                        }
                    }
                    // 创建目标目录
                    let _ = client.create_folder(&save_path).await;

                    info!("重试转存: {} 个文件 -> {}", fs_ids.len(), save_path);
                    client
                        .transfer_share_files(
                            &share_info.shareid,
                            &share_info.share_uk,
                            &share_info.bdstoken,
                            &fs_ids,
                            &save_path,
                            &referer,
                            Some(task_id),
                        )
                        .await
                } else {
                    transfer_result
                }
            }
            _ => transfer_result,
        };

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

                    // 分享直下模式：转存失败时清理临时目录
                    if is_share_direct_download {
                        let temp_dir = {
                            let t = task.read().await;
                            t.temp_dir.clone()
                        };
                        if let Some(ref td) = temp_dir {
                            // ========== 临时目录快照（清理前诊断） ==========
                            match client.get_file_list(td, 1, 100).await {
                                Ok(snapshot) => {
                                    let total = snapshot.list.len();
                                    let items: Vec<String> = snapshot.list.iter().take(20).map(|f| {
                                        format!(
                                            "{}({})",
                                            f.server_filename,
                                            if f.isdir == 1 { "dir" } else { "file" }
                                        )
                                    }).collect();
                                    warn!(
                                        "清理前临时目录快照: task_id={}, temp_dir={}, total_items={}, first_20={:?}",
                                        task_id, td, total, items
                                    );
                                }
                                Err(e) => {
                                    debug!("清理前快照拉取失败: task_id={}, error={}", task_id, e);
                                }
                            }

                            let configured_root = app_config.read().await.share_direct_download.temp_dir.clone();
                            info!("转存失败，清理临时目录: task_id={}, temp_dir={}", task_id, td);
                            let cleanup = Self::cleanup_temp_dir_internal(&client, td, &configured_root).await;
                            info!("转存失败清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                            if let Some(ref pm_arc) = persistence_manager {
                                if let Err(e) = pm_arc.lock().await.update_cleanup_status(task_id, cleanup.status) {
                                    warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                                }
                            }
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
                let (auto_download, file_list, is_share_direct_download) = {
                    let mut t = task.write().await;
                    t.transferred_count = result.transferred_paths.len();
                    (t.auto_download, t.file_list.clone(), t.is_share_direct_download)
                };

                if auto_download {
                    // 启动自动下载
                    Self::start_auto_download(
                        client_shared,
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
                        is_share_direct_download,
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
                let raw_err_msg = e.to_string();

                // 🔥 尝试友好错误处理（区分 task_errno 场景）
                let handled = Self::handle_transfer_error(&task, &client, &raw_err_msg).await;

                match handled {
                    TransferErrorHandled::Recovered(recovered_items) => {
                        // 分享直下模式 -30 恢复成功，视为转存成功
                        // 使用 recover_from_conflict 返回的完整文件信息构造 TransferResult
                        info!(
                            "分享直下 -30 恢复成功，继续下载流程: task_id={}, recovered={}",
                            task_id, recovered_items.len()
                        );

                        // 更新任务状态为已转存（不标记失败）
                        let (auto_download, file_list) = {
                            let mut t = task.write().await;
                            t.transferred_count = t.total_count;
                            (t.auto_download, t.file_list.clone())
                        };

                        if auto_download {
                            // 🔥 直接使用恢复结果构造 TransferResult，
                            // 不再重新扫描临时目录第一页（避免 >1000 项或选择性转存时丢项）
                            let temp_dir = {
                                let t = task.read().await;
                                t.temp_dir.clone().filter(|s| !s.is_empty())
                            };
                            let temp_dir = match temp_dir {
                                Some(td) => td,
                                None => {
                                    error!("恢复后自动下载失败: temp_dir 为空");
                                    anyhow::bail!("临时目录路径为空，无法构造下载任务");
                                }
                            };
                            let mut transferred_paths = Vec::new();
                            let mut transferred_fs_ids = Vec::new();
                            let mut from_paths = Vec::new();

                            for (name, fs_id_opt, path_opt, source_share_path) in &recovered_items {
                                // 使用恢复时扫描到的真实远端路径（不再猜测）
                                let path = match path_opt {
                                    Some(p) => p.clone(),
                                    None => {
                                        // recover_from_conflict 现在总是填充 path，
                                        // 到达此处说明数据不一致
                                        warn!(
                                            "恢复项 {} 缺少远端路径，回退拼接 temp_dir + name",
                                            name
                                        );
                                        let base = temp_dir.trim_end_matches('/');
                                        format!("{}/{}", base, name)
                                    }
                                };
                                transferred_paths.push(path);
                                // fs_id 用于文件下载；文件夹 fs_id 为 None，填 0
                                transferred_fs_ids.push(fs_id_opt.unwrap_or(0));
                                // 原始分享路径由 recover_from_conflict 直接携带，不再按 name 反查
                                from_paths.push(source_share_path.clone());
                            }

                            let virtual_result = TransferResult {
                                success: true,
                                transferred_paths,
                                from_paths,
                                error: None,
                                transferred_fs_ids,
                            };

                            Self::start_auto_download(
                                client_shared,
                                tasks.clone(),
                                download_manager,
                                folder_download_manager,
                                app_config,
                                persistence_manager.clone(),
                                ws_manager.clone(),
                                task_id,
                                virtual_result,
                                file_list,
                                save_path,
                                cancellation_token,
                                is_share_direct_download,
                            )
                                .await?;

                            // 持久化完成状态
                            if let Some(ref pm_arc) = persistence_manager {
                                let pm = pm_arc.lock().await;
                                if let Err(e) = pm.update_transfer_status(task_id, "completed") {
                                    warn!("更新转存任务状态为完成失败: {}", e);
                                }
                                if let Err(e) = pm.on_task_completed(task_id) {
                                    warn!("标记转存任务完成失败: {}", e);
                                }
                            }

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
                            // 无自动下载，标记为已转存
                            let old_status;
                            {
                                let mut t = task.write().await;
                                old_status = format!("{:?}", t.status).to_lowercase();
                                t.mark_transferred();
                            }

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

                            if let Some(ref pm_arc) = persistence_manager {
                                let pm = pm_arc.lock().await;
                                if let Err(e) = pm.update_transfer_status(task_id, "transferred") {
                                    warn!("更新转存任务状态失败: {}", e);
                                }
                                if let Err(e) = pm.on_task_completed(task_id) {
                                    warn!("标记转存任务完成失败: {}", e);
                                }
                            }

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
                    other => {
                        // 恢复失败或非 -30 场景：使用友好消息或原始消息标记失败
                        let err_msg = match other {
                            TransferErrorHandled::Failed(msg) => msg,
                            TransferErrorHandled::Unrecognized => raw_err_msg.clone(),
                            TransferErrorHandled::Recovered(_) => unreachable!(),
                        };
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

                            if let Err(e) = pm.update_transfer_status(task_id, "transfer_failed") {
                                warn!("更新转存任务状态失败: {}", e);
                            }
                            if let Err(e) = pm.update_task_error(task_id, err_msg.clone()) {
                                warn!("更新转存任务错误信息失败: {}", e);
                            }
                        }

                        // 分享直下模式：转存请求异常时清理临时目录
                        if is_share_direct_download {
                            let temp_dir = {
                                let t = task.read().await;
                                t.temp_dir.clone()
                            };
                            if let Some(ref td) = temp_dir {
                                let configured_root = app_config.read().await.share_direct_download.temp_dir.clone();
                                info!("转存请求异常，清理临时目录: task_id={}, temp_dir={}", task_id, td);
                                let cleanup = Self::cleanup_temp_dir_internal(&client, td, &configured_root).await;
                                info!("转存异常清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                                if let Some(ref pm_arc) = persistence_manager {
                                    if let Err(e) = pm_arc.lock().await.update_cleanup_status(task_id, cleanup.status) {
                                        warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                                    }
                                }
                            }
                        }
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
        _client: Arc<StdRwLock<NetdiskClient>>,
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
        is_share_direct_download: bool,
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

        // 🔥 构建两级查找映射：
        //   1. path → SharedFileInfo：用原始分享路径精确匹配（无歧义，优先使用）
        //   2. (name, is_dir) → SharedFileInfo：名称 + 类型匹配（防止文件/文件夹错配）
        // 注意：transferred_fs_ids 是百度返回的转存后新 fs_id（to_fs_id），
        // 与 file_list 中的原始分享 fs_id 不同，无法直接用 fs_id 匹配。
        let file_info_by_path: std::collections::HashMap<&str, &SharedFileInfo> = file_list
            .iter()
            .map(|f| (f.path.as_str(), f))
            .collect();
        let file_info_by_name_dir: std::collections::HashMap<(&str, bool), &SharedFileInfo> = file_list
            .iter()
            .map(|f| ((f.name.as_str(), f.is_dir), f))
            .collect();

        for (idx, transferred_path) in transfer_result.transferred_paths.iter().enumerate() {
            let transferred_fs_id = transfer_result.transferred_fs_ids.get(idx).copied();
            let from_path = transfer_result.from_paths.get(idx);
            let from_filename = from_path
                .map(|p| p.rsplit('/').next().unwrap_or(p).to_string());
            let to_filename = transferred_path.rsplit('/').next().unwrap_or(transferred_path);

            // 匹配优先级：
            // 1. from_path 全路径精确匹配（最可靠，可区分同名文件）
            // 2. from_filename + is_dir 匹配（防止文件/文件夹错配）
            // 3. to_filename + is_dir 匹配（百度可能重命名，最后手段）
            let file_info = from_path
                .and_then(|p| file_info_by_path.get(p.as_str()).copied())
                .or_else(|| {
                    let name = from_filename.as_deref().unwrap_or(to_filename);
                    file_info_by_name_dir.get(&(name, false)).copied()
                        .or_else(|| file_info_by_name_dir.get(&(name, true)).copied())
                })
                .or_else(|| {
                    file_info_by_name_dir.get(&(to_filename, false)).copied()
                        .or_else(|| file_info_by_name_dir.get(&(to_filename, true)).copied())
                });

            if let Some(file_info) = file_info {
                info!("匹配文件信息: idx={}, name={}, is_dir={}, transferred_fs_id={:?}",
                    idx, file_info.name, file_info.is_dir, transferred_fs_id);
                if file_info.is_dir {
                    // 文件夹：记录路径，稍后调用文件夹下载
                    download_folders.push(transferred_path.clone());
                    info!("发现文件夹: {}", transferred_path);
                } else {
                    // 文件：记录下载信息，使用转存后的新 fs_id
                    download_files.push((
                        transferred_fs_id.unwrap_or(0),
                        transferred_path.clone(),
                        file_info.name.clone(),
                        file_info.size,
                    ));
                }
            } else {
                // 无法匹配到文件信息（可能是同名碰撞或分页未拉全）
                warn!("无法匹配文件信息: idx={}, path={}, from={:?}, to_filename={}",
                    idx, transferred_path, from_filename, to_filename);
                // 默认当作文件处理，使用转存后的文件名
                let fs_id = transferred_fs_id.unwrap_or(0);
                download_files.push((fs_id, transferred_path.clone(), to_filename.to_string(), 0));
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
                    None,
                )
                .await
            {
                Ok(download_task_id) => {
                    // 🔥 设置下载任务关联的转存任务 ID（内存中）
                    // 注意：持久化会在 start_task -> register_download_task 时自动从内存任务中获取
                    if let Err(e) = dm.set_task_transfer_id(&download_task_id, task_id.to_string()).await {
                        warn!("设置下载任务关联转存任务(内存)失败: {}", e);
                    }

                    // 🔥 如果是分享直下任务，标记下载任务
                    if is_share_direct_download {
                        if let Err(e) = dm.set_task_share_direct_download(&download_task_id, true).await {
                            warn!("设置下载任务为分享直下任务失败: {}", e);
                        }
                    }

                    // 启动下载任务
                    // 🔥 修复：transfer_task_id 会在 start_task -> register_download_task 时
                    // 从内存任务对象中获取并持久化，解决了之前调用顺序导致的问题
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
                        .create_folder_download_with_dir(folder_path.clone(), &download_dir, None, None)
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
            _client,
            tasks,
            download_manager,
            folder_download_manager,
            app_config,
            persistence_manager,
            ws_manager,
            task_id.to_string(),
            cancellation_token,
        );

        Ok(())
    }

    /// 启动下载状态监听任务
    ///
    /// 通过轮询方式监听关联的下载任务状态，当所有下载完成或失败时更新转存任务状态
    /// 对于分享直下任务，下载完成后会触发临时目录清理
    fn start_download_status_watcher(
        client: Arc<StdRwLock<NetdiskClient>>,
        tasks: Arc<DashMap<String, TransferTaskInfo>>,
        download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        app_config: Arc<RwLock<AppConfig>>,
        persistence_manager: Option<Arc<Mutex<PersistenceManager>>>,
        ws_manager: Option<Arc<WebSocketManager>>,
        task_id: String,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn(async move {
            // 🔥 从共享引用快照当前客户端（代理热更新后自动生效）
            let client = Arc::new(client.read().unwrap().clone());
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

                        // 获取分享直下相关信息
                        let (is_share_direct_download, temp_dir) = {
                            let t = task.read().await;
                            (t.is_share_direct_download, t.temp_dir.clone())
                        };

                        {
                            let mut t = task.write().await;
                            t.status = TransferStatus::DownloadFailed;
                            t.error = Some(format!("下载超时（超过{}小时）", DOWNLOAD_TIMEOUT_HOURS));
                            t.touch();
                        }

                        // 分享直下任务：下载超时也需要清理临时目录
                        if is_share_direct_download {
                            let (cleanup_on_failure, configured_root) = {
                                let cfg = app_config.read().await;
                                (cfg.share_direct_download.cleanup_on_failure, cfg.share_direct_download.temp_dir.clone())
                            };

                            if cleanup_on_failure {
                                if let Some(ref temp_dir) = temp_dir {
                                    info!("下载超时，触发临时目录清理: task_id={}, temp_dir={}", task_id, temp_dir);
                                    let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
                                    info!("下载超时清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                                    if let Some(ref pm_arc) = persistence_manager {
                                        if let Err(e) = pm_arc.lock().await.update_cleanup_status(&task_id, cleanup.status) {
                                            warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                                        }
                                    }
                                }
                            }
                        }

                        break;
                    }
                }

                // 检查所有关联下载任务的状态
                let final_status =
                    Self::aggregate_download_status(&download_manager, &folder_download_manager, &download_task_ids).await;

                if let Some(new_status) = final_status {
                    info!(
                        "下载状态聚合完成: task_id={}, status={:?}",
                        task_id, new_status
                    );

                    // 获取分享直下相关信息
                    let (is_share_direct_download, temp_dir, auto_cleanup, configured_root) = {
                        let t = task.read().await;
                        let cfg = app_config.read().await;
                        (
                            t.is_share_direct_download,
                            t.temp_dir.clone(),
                            cfg.share_direct_download.auto_cleanup,
                            cfg.share_direct_download.temp_dir.clone(),
                        )
                    };

                    // 处理分享直下任务的清理逻辑
                    if is_share_direct_download {
                        match new_status {
                            TransferStatus::Completed => {
                                // 下载完成，进入清理阶段
                                if auto_cleanup {
                                    let old_status;
                                    {
                                        let mut t = task.write().await;
                                        old_status = format!("{:?}", t.status).to_lowercase();
                                        t.mark_cleaning();
                                    }

                                    // 🔥 持久化 Cleaning 状态
                                    if let Some(ref pm_arc) = persistence_manager {
                                        if let Err(e) = pm_arc.lock().await.update_transfer_status(&task_id, "cleaning") {
                                            warn!("持久化 Cleaning 状态失败: {}", e);
                                        }
                                    }

                                    // 发送状态变更事件：Downloading -> Cleaning
                                    if let Some(ref ws) = ws_manager {
                                        ws.send_if_subscribed(
                                            TaskEvent::Transfer(TransferEvent::StatusChanged {
                                                task_id: task_id.to_string(),
                                                old_status,
                                                new_status: "cleaning".to_string(),
                                            }),
                                            None,
                                        );
                                    }

                                    // 执行清理
                                    let cleanup_status = if let Some(ref temp_dir) = temp_dir {
                                        info!("下载完成，开始清理临时目录: task_id={}, temp_dir={}", task_id, temp_dir);
                                        let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
                                        info!("下载完成清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                                        Some(cleanup.status)
                                    } else {
                                        None
                                    };

                                    // 清理完成，标记为 Completed
                                    let old_status;
                                    {
                                        let mut t = task.write().await;
                                        old_status = format!("{:?}", t.status).to_lowercase();
                                        t.mark_completed();
                                    }

                                    // 🔥 持久化清理状态和 Completed 状态
                                    if let Some(ref pm_arc) = persistence_manager {
                                        let pm = pm_arc.lock().await;
                                        // 持久化清理状态
                                        if let Some(cs) = cleanup_status {
                                            if let Err(e) = pm.update_cleanup_status(&task_id, cs) {
                                                warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                                            }
                                        }
                                        if let Err(e) = pm.update_transfer_status(&task_id, "completed") {
                                            warn!("持久化 Completed 状态失败: {}", e);
                                        }
                                        if let Err(e) = pm.on_task_completed(&task_id) {
                                            warn!("标记分享直下任务完成失败: {}", e);
                                        }
                                    }

                                    // 发送状态变更事件：Cleaning -> Completed
                                    if let Some(ref ws) = ws_manager {
                                        ws.send_if_subscribed(
                                            TaskEvent::Transfer(TransferEvent::StatusChanged {
                                                task_id: task_id.to_string(),
                                                old_status,
                                                new_status: "completed".to_string(),
                                            }),
                                            None,
                                        );
                                    }

                                    // 🔥 清理完成后，移除分享直下的下载任务
                                    let dm_lock = download_manager.read().await;
                                    if let Some(ref dm) = *dm_lock {
                                        for download_task_id in &download_task_ids {
                                            // 跳过文件夹下载任务（以 folder: 开头）
                                            if download_task_id.starts_with("folder:") {
                                                continue;
                                            }
                                            if let Err(e) = dm.remove_share_direct_download_task(download_task_id).await {
                                                warn!("移除分享直下下载任务失败: {}, error={}", download_task_id, e);
                                            }
                                        }
                                    }
                                } else {
                                    // 不自动清理，直接标记为完成
                                    let old_status;
                                    {
                                        let mut t = task.write().await;
                                        old_status = format!("{:?}", t.status).to_lowercase();
                                        t.mark_completed();
                                    }

                                    // 🔥 持久化 Completed 状态并标记任务完成
                                    if let Some(ref pm_arc) = persistence_manager {
                                        let pm = pm_arc.lock().await;
                                        if let Err(e) = pm.update_transfer_status(&task_id, "completed") {
                                            warn!("持久化 Completed 状态失败: {}", e);
                                        }
                                        if let Err(e) = pm.on_task_completed(&task_id) {
                                            warn!("标记分享直下任务完成失败: {}", e);
                                        }
                                    }

                                    if let Some(ref ws) = ws_manager {
                                        ws.send_if_subscribed(
                                            TaskEvent::Transfer(TransferEvent::StatusChanged {
                                                task_id: task_id.to_string(),
                                                old_status,
                                                new_status: "completed".to_string(),
                                            }),
                                            None,
                                        );
                                    }
                                }
                            }
                            TransferStatus::DownloadFailed => {
                                // 下载失败，根据配置决定是否清理
                                let cleanup_on_failure = {
                                    let cfg = app_config.read().await;
                                    cfg.share_direct_download.cleanup_on_failure
                                };

                                let old_status;
                                {
                                    let mut t = task.write().await;
                                    old_status = format!("{:?}", t.status).to_lowercase();
                                    t.mark_download_failed();
                                }

                                // 🔥 持久化 DownloadFailed 状态
                                if let Some(ref pm_arc) = persistence_manager {
                                    if let Err(e) = pm_arc.lock().await.update_transfer_status(&task_id, "download_failed") {
                                        warn!("持久化 DownloadFailed 状态失败: {}", e);
                                    }
                                }

                                if let Some(ref ws) = ws_manager {
                                    ws.send_if_subscribed(
                                        TaskEvent::Transfer(TransferEvent::StatusChanged {
                                            task_id: task_id.to_string(),
                                            old_status,
                                            new_status: "download_failed".to_string(),
                                        }),
                                        None,
                                    );
                                }

                                // 失败时清理临时目录
                                if cleanup_on_failure {
                                    if let Some(ref temp_dir) = temp_dir {
                                        info!("下载失败，触发临时目录清理: task_id={}, temp_dir={}", task_id, temp_dir);
                                        let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
                                        info!("下载失败清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                                        if let Some(ref pm_arc) = persistence_manager {
                                            if let Err(e) = pm_arc.lock().await.update_cleanup_status(&task_id, cleanup.status) {
                                                warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                // 其他状态（如 Transferred），直接更新
                                let old_status;
                                {
                                    let mut t = task.write().await;
                                    old_status = format!("{:?}", t.status).to_lowercase();
                                    t.status = new_status.clone();
                                    t.touch();
                                }

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
                            }
                        }
                    } else {
                        // 非分享直下任务，保持原有逻辑
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
                    }

                    break;
                }
            }
        });
    }

    /// 清理临时目录（内部方法，带超时机制）
    ///
    /// 调用 NetdiskClient::delete_files 删除临时目录
    /// 添加 30 秒超时机制，避免 Cleaning 状态卡住
    /// 清理失败或超时时只记录日志，不影响任务状态
    ///
    /// # 返回
    /// `CleanupResult` 结构化清理结果，包含状态和错误信息
    ///
    /// # 参数
    /// * `client` - 网盘客户端
    /// * `temp_dir` - 临时目录路径（网盘路径）
    ///
    /// # 安全性
    /// 确保不删除父目录 `{config.temp_dir}`，只删除任务特定的子目录
    async fn cleanup_temp_dir_internal(client: &NetdiskClient, temp_dir: &str, configured_temp_root: &str) -> CleanupResult {
        const CLEANUP_TIMEOUT_SECS: u64 = 30;

        info!("开始清理临时目录: {}", temp_dir);

        // 安全检查：确保路径在配置的临时目录根下，且不是根目录本身
        // temp_dir 格式应为 /<temp_root>/{uuid}/ ，例如 /.bpr_share_temp/{uuid}/
        let temp_dir_trimmed = temp_dir.trim_end_matches('/');
        let root_trimmed = configured_temp_root.trim_end_matches('/');

        // 检查 0：configured_temp_root 本身必须安全（不能是 /、空、或过短）
        // trim 后至少 2 字符（如 /.x），防止 / 退化导致 starts_with("") 恒真
        if root_trimmed.len() < 2 || !root_trimmed.starts_with('/') {
            error!(
                "配置的临时目录根不安全，跳过清理: configured_root={}",
                configured_temp_root
            );
            return CleanupResult {
                success: false,
                status: CleanupStatus::NotAttempted,
                error: Some(format!(
                    "配置的临时目录根不安全（过短或非绝对路径）: {}",
                    configured_temp_root
                )),
                errno: None,
            };
        }

        let parts: Vec<&str> = temp_dir_trimmed.split('/').filter(|s| !s.is_empty()).collect();

        // 检查 1：至少两级目录（temp_root + uuid）
        if parts.len() < 2 {
            error!("临时目录路径层级不足，跳过清理: {}", temp_dir);
            return CleanupResult {
                success: false,
                status: CleanupStatus::NotAttempted,
                error: Some("路径格式不正确：层级不足".to_string()),
                errno: None,
            };
        }

        // 检查 2：路径必须以配置的临时根目录开头，且根后紧跟 '/'（防止前缀碰撞）
        let is_under_root = temp_dir_trimmed.starts_with(root_trimmed)
            && temp_dir_trimmed.len() > root_trimmed.len()
            && temp_dir_trimmed.as_bytes()[root_trimmed.len()] == b'/';
        if !is_under_root {
            error!(
                "临时目录路径不在配置的临时根目录下，跳过清理: path={}, configured_root={}",
                temp_dir, configured_temp_root
            );
            return CleanupResult {
                success: false,
                status: CleanupStatus::NotAttempted,
                error: Some("路径不在配置的临时目录根下".to_string()),
                errno: None,
            };
        }

        // 执行清理，带超时
        let cleanup_result = tokio::time::timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            client.delete_files(&[temp_dir.to_string()])
        ).await;

        match cleanup_result {
            Ok(Ok(result)) => {
                if result.success {
                    info!("临时目录清理成功: {}", temp_dir);
                    CleanupResult {
                        success: true,
                        status: CleanupStatus::Success,
                        error: None,
                        errno: None,
                    }
                } else {
                    // 检查是否为风控拦截
                    if let Some(errno) = result.errno {
                        if errno == 132 {
                            warn!(
                                "删除操作被百度风控拦截（errno=132），临时目录将保留：{}",
                                temp_dir
                            );
                            if let Some(ref widget) = result.authwidget {
                                warn!(
                                    "风控诊断: saferand={}, safetpl={}, safesign_len={}",
                                    widget.saferand, widget.safetpl, widget.safesign.len()
                                );
                            }
                            return CleanupResult {
                                success: false,
                                status: CleanupStatus::RiskControlBlocked,
                                error: Some("风控拦截".to_string()),
                                errno: Some(132),
                            };
                        } else if errno == 12 {
                            // 文件不存在，视为成功（幂等性）
                            info!("临时目录不存在（errno=12），视为清理成功: {}", temp_dir);
                            return CleanupResult {
                                success: true,
                                status: CleanupStatus::Success,
                                error: None,
                                errno: None,
                            };
                        }
                    }

                    warn!(
                        "临时目录清理失败: {}, error={:?}, errno={:?}",
                        temp_dir, result.error, result.errno
                    );
                    CleanupResult {
                        success: false,
                        status: CleanupStatus::Failed,
                        error: result.error,
                        errno: result.errno,
                    }
                }
            }
            Ok(Err(e)) => {
                // 清理失败只记录日志，不影响任务状态
                error!("临时目录清理请求失败: {}, error={}", temp_dir, e);
                CleanupResult {
                    success: false,
                    status: CleanupStatus::Failed,
                    error: Some(e.to_string()),
                    errno: None,
                }
            }
            Err(_) => {
                // 超时，记录日志但不影响任务状态
                warn!("临时目录清理超时（{}秒）: {}", CLEANUP_TIMEOUT_SECS, temp_dir);
                CleanupResult {
                    success: false,
                    status: CleanupStatus::Failed,
                    error: Some("超时".to_string()),
                    errno: None,
                }
            }
        }
    }

    /// 从错误消息中提取 task_errno 值
    ///
    /// 匹配形如 "task_errno=-30" 的模式，返回错误码数值
    fn extract_task_errno(error_msg: &str) -> Option<i64> {
        // 查找 "task_errno=" 并提取后面的数字（可能为负数）
        if let Some(pos) = error_msg.find("task_errno=") {
            let after = &error_msg[pos + "task_errno=".len()..];
            // 提取数字部分（包括可能的负号）
            let num_str: String = after.chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect();
            num_str.parse::<i64>().ok()
        } else {
            None
        }
    }

    /// 处理转存错误（区分场景，提供友好错误提示）
    ///
    /// 根据错误码和任务模式（普通转存 vs 分享直下）采取不同的处理策略：
    /// - task_errno=-30: 同名文件已存在。分享直下模式下尝试恢复，普通模式直接失败
    /// - task_errno=-31: 保存失败
    /// - task_errno=-32: 网盘空间不足
    /// - task_errno=-33: 文件数量超出限制
    ///
    /// # 返回
    /// - `Recovered(items)`: 分享直下 -30 恢复成功，携带恢复的文件信息
    /// - `Failed(msg)`: 已处理的友好错误消息
    /// - `Unrecognized`: 无法识别的错误码，调用方应使用原始错误消息
    async fn handle_transfer_error(
        task: &Arc<RwLock<TransferTask>>,
        client: &NetdiskClient,
        error_msg: &str,
    ) -> TransferErrorHandled {
        let errno = Self::extract_task_errno(error_msg);

        match errno {
            Some(-30) => {
                let is_share_direct = {
                    let t = task.read().await;
                    t.is_share_direct_download
                };
                if is_share_direct {
                    // 分享直下模式：尝试回查恢复
                    info!("分享直下模式检测到 -30 错误，尝试恢复");
                    match Self::recover_from_conflict(task, client).await {
                        Ok(recovered_items) => {
                            info!("从 -30 冲突恢复成功，已获取 {} 个文件信息", recovered_items.len());
                            TransferErrorHandled::Recovered(recovered_items)
                        }
                        Err(e) => {
                            warn!("从 -30 冲突恢复失败: {}", e);
                            TransferErrorHandled::Failed(format!("转存失败：目标目录已存在同名文件（恢复失败: {}）", e))
                        }
                    }
                } else {
                    TransferErrorHandled::Failed("转存失败：目标目录已存在同名文件".to_string())
                }
            }
            Some(-31) => TransferErrorHandled::Failed("转存失败：保存失败，请稍后重试".to_string()),
            Some(-32) => TransferErrorHandled::Failed("转存失败：网盘空间不足".to_string()),
            Some(-33) => TransferErrorHandled::Failed("转存失败：文件数量超出限制".to_string()),
            Some(code) => TransferErrorHandled::Failed(format!("转存失败：错误码 {}", code)),
            None => TransferErrorHandled::Unrecognized,
        }
    }

    /// 从冲突中恢复（分享直下专用）
    ///
    /// 当异步转存返回 task_errno=-30（文件已存在）时：
    /// 1. 批量拉取临时目录下的所有文件
    /// 2. 匹配原始文件列表中的每个文件
    /// 3. 如果全部文件都能匹配到，返回恢复信息（fs_id/path）
    /// 4. 如果有任何文件无法匹配，返回错误
    ///
    /// # 返回
    /// 成功时返回 Vec<(name, Option<fs_id>, Option<temp_dir_path>, source_share_path)>
    async fn recover_from_conflict(
        task: &Arc<RwLock<TransferTask>>,
        client: &NetdiskClient,
    ) -> Result<Vec<(String, Option<u64>, Option<String>, String)>> {
        let (selected_files, selected_fs_ids, file_list, temp_dir) = {
            let t = task.read().await;
            let td = t.temp_dir.clone().filter(|s| !s.is_empty());
            (
                t.selected_files.clone(),
                t.selected_fs_ids.clone(),
                t.file_list.clone(),
                td,
            )
        };

        let temp_dir = match temp_dir {
            Some(td) => td,
            None => {
                error!("recover_from_conflict: temp_dir 为空，无法执行恢复");
                return Err(anyhow::anyhow!("临时目录路径为空，无法恢复"));
            }
        };

        // 构建需要回查的文件列表
        // 必须使用 selected_files（前端传入的完整信息，包含子目录选择场景）
        // ⚠️ 当 selected_fs_ids 非空但 selected_files 缺失时，file_list 仅包含分享第一页
        //    过滤后的结果（见 manager.rs line 613-620），恢复信息不完整，宁可不恢复
        let has_selected_fs_ids = selected_fs_ids.as_ref().map_or(false, |ids| !ids.is_empty());

        let files_to_check: Vec<SharedFileInfo> = if let Some(ref files) = selected_files {
            if !files.is_empty() {
                files.clone()
            } else if has_selected_fs_ids {
                // selected_files 为空数组但 selected_fs_ids 非空：file_list 不可靠
                error!(
                    "selected_files 为空但 selected_fs_ids 非空，file_list 可能不完整，拒绝恢复"
                );
                return Err(anyhow::anyhow!(
                    "恢复所需的 selected_files 信息缺失（selected_fs_ids 模式下 file_list 不可靠）"
                ));
            } else {
                // 全选模式（无 selected_fs_ids），file_list 是完整的
                file_list
            }
        } else if has_selected_fs_ids {
            // selected_files 为 None 但 selected_fs_ids 非空：file_list 不可靠
            error!(
                "selected_files 缺失但 selected_fs_ids 非空，file_list 可能不完整，拒绝恢复"
            );
            return Err(anyhow::anyhow!(
                "恢复所需的 selected_files 信息缺失（selected_fs_ids 模式下 file_list 不可靠）"
            ));
        } else {
            // 全选模式（无 selected_fs_ids），file_list 是完整的
            file_list
        };

        if files_to_check.is_empty() {
            return Err(anyhow::anyhow!("无可回查的文件列表"));
        }

        // 获取 task_id 用于日志关联
        let recovery_task_id = {
            let t = task.read().await;
            t.id.clone()
        };
        info!(
            "开始从冲突恢复: task_id={}, temp_dir={}, files_to_check={}",
            recovery_task_id,
            temp_dir,
            files_to_check.len()
        );

        // 一次性批量拉取临时目录下的所有文件（支持分页，避免超过 1000 条限制）
        let mut existing_files = Vec::new();
        let mut page = 1u32;
        let page_size = 1000u32;

        loop {
            match client.get_file_list(&temp_dir, page, page_size).await {
                Ok(list) => {
                    let batch_len = list.list.len();
                    debug!(
                        "拉取临时目录文件列表第 {} 页: {} 个项目",
                        page, batch_len
                    );
                    existing_files.extend(list.list);
                    if (batch_len as u32) < page_size {
                        break;
                    }
                    page += 1;
                }
                Err(e) => {
                    warn!("拉取临时目录文件列表失败（第 {} 页）: {}", page, e);
                    break;
                }
            }
        }

        info!("临时目录共有 {} 个文件/文件夹", existing_files.len());

        // ========== Phase 1: 根级 (name, is_dir) 匹配 ==========
        // 构建 (文件名, is_dir) → (fs_id, path) 映射
        // 文件和文件夹都记录完整远端路径，消费端不再猜测路径
        let mut name_dir_to_item: HashMap<(String, bool), (Option<u64>, String)> = HashMap::new();
        for file in &existing_files {
            let is_dir = file.isdir == 1;
            let fs_id = if is_dir { None } else { Some(file.fs_id) };
            name_dir_to_item.insert(
                (file.server_filename.clone(), is_dir),
                (fs_id, file.path.clone()),
            );
        }

        // consumed 集合防止同名项双重匹配同一个根级条目
        let mut consumed: std::collections::HashSet<(String, bool)> = std::collections::HashSet::new();
        let mut recovered_items = Vec::new();
        let mut unmatched_files: Vec<&SharedFileInfo> = Vec::new();

        for file in &files_to_check {
            let key = (file.name.clone(), file.is_dir);
            if !consumed.contains(&key) {
                if let Some((fs_id, path)) = name_dir_to_item.get(&key) {
                    consumed.insert(key);
                    recovered_items.push((file.name.clone(), *fs_id, Some(path.clone()), file.path.clone()));
                    continue;
                }
            }
            unmatched_files.push(file);
        }

        // Phase 1 匹配摘要
        info!(
            "恢复 Phase1 完成: task_id={}, files_to_check={}, existing_root_items={}, \
             phase1_matched={}, phase1_unmatched={}",
            recovery_task_id,
            files_to_check.len(),
            existing_files.len(),
            recovered_items.len(),
            unmatched_files.len()
        );

        // ========== Phase 2: 路径推导回退 ==========
        // 处理：百度按目录结构转存、或同名文件已被 Phase 1 消费的场景
        // 通过 SharedFileInfo.path 推导文件在 temp_dir 中的预期位置
        if !unmatched_files.is_empty() {
            info!(
                "根级匹配后仍有 {} 个未匹配项，尝试路径推导恢复",
                unmatched_files.len()
            );

            // 推导 files_to_check 的公共父目录（即分享浏览目录）
            // 按路径段（/分隔）逐段比较，避免 /share/A 错匹配 /share/AB
            let share_root = {
                let parents: Vec<Vec<&str>> = files_to_check
                    .iter()
                    .filter_map(|f| f.path.rsplit_once('/').map(|(p, _)| p))
                    .map(|p| p.split('/').collect::<Vec<_>>())
                    .collect();
                if parents.is_empty() {
                    String::new()
                } else {
                    let mut common_segments = parents[0].clone();
                    for segs in &parents[1..] {
                        let match_len = common_segments
                            .iter()
                            .zip(segs.iter())
                            .take_while(|(a, b)| a == b)
                            .count();
                        common_segments.truncate(match_len);
                    }
                    common_segments.join("/")
                }
            };
            debug!("推导的分享根目录: {:?}", share_root);

            let temp_base = temp_dir.trim_end_matches('/');
            let mut still_failed = Vec::new();

            // 按父目录缓存扫描结果，避免同一目录重复请求
            let mut dir_cache: HashMap<String, Vec<crate::netdisk::types::FileItem>> = HashMap::new();

            for file in &unmatched_files {
                // 从 SharedFileInfo.path 推导在 temp_dir 中的相对路径
                let relative = if !share_root.is_empty() && file.path.starts_with(&share_root) {
                    file.path[share_root.len()..].trim_start_matches('/')
                } else {
                    &file.name
                };

                let expected_path = format!("{}/{}", temp_base, relative);
                let expected_parent = expected_path
                    .rsplit_once('/')
                    .map_or(temp_base, |(p, _)| p);

                debug!(
                    "路径推导: name={}, share_path={}, expected={}, parent={}",
                    file.name, file.path, expected_path, expected_parent
                );

                // 仅当推导出的父目录与根目录不同时才有意义（根目录已在 Phase 1 扫过）
                let found = if expected_parent != temp_base {
                    // 缓存未命中时拉取完整目录列表（分页）
                    if !dir_cache.contains_key(expected_parent) {
                        let mut all_items = Vec::new();
                        let page_size: u32 = 1000;
                        let mut page: u32 = 1;
                        loop {
                            match client.get_file_list(expected_parent, page, page_size).await {
                                Ok(list) => {
                                    let batch_len = list.list.len();
                                    all_items.extend(list.list);
                                    if (batch_len as u32) < page_size {
                                        break;
                                    }
                                    page += 1;
                                }
                                Err(e) => {
                                    debug!(
                                        "路径推导扫描失败: dir={}, page={}, error={}",
                                        expected_parent, page, e
                                    );
                                    break;
                                }
                            }
                        }
                        dir_cache.insert(expected_parent.to_string(), all_items);
                    }

                    dir_cache.get(expected_parent).and_then(|items| {
                        items.iter().find(|f| {
                            f.server_filename == file.name && (f.isdir == 1) == file.is_dir
                        }).map(|f| {
                            let fs_id = if file.is_dir { None } else { Some(f.fs_id) };
                            (fs_id, f.path.clone())
                        })
                    })
                } else {
                    None
                };

                if let Some((fs_id, path)) = found {
                    info!("路径推导匹配成功: name={}, path={}", file.name, path);
                    recovered_items.push((file.name.clone(), fs_id, Some(path), file.path.clone()));
                } else {
                    still_failed.push(format!(
                        "{}({}) [expected: {}]",
                        file.name,
                        if file.is_dir { "dir" } else { "file" },
                        expected_path
                    ));
                }
            }

            if !still_failed.is_empty() {
                let error_msg = format!(
                    "部分文件无法获取信息（{}/{}）",
                    still_failed.len(),
                    files_to_check.len()
                );
                warn!(
                    "恢复失败: {}, 失败项: {:?}",
                    error_msg, still_failed
                );
                return Err(anyhow::anyhow!(error_msg));
            }
        }

        // ========== 恢复成功摘要 ==========
        {
            let top10: Vec<String> = recovered_items.iter().take(10).map(|(_name, _fs_id, path_opt, src)| {
                format!(
                    "{} -> {}",
                    src,
                    path_opt.as_deref().unwrap_or("N/A")
                )
            }).collect();
            info!(
                "恢复成功: task_id={}, recovered={}/{}, share_root={}, top10_mappings={:?}",
                recovery_task_id,
                recovered_items.len(),
                files_to_check.len(),
                if unmatched_files.is_empty() { "N/A (all phase1)" } else { "see above" },
                top10
            );
        }
        Ok(recovered_items)
    }

    /// 聚合多个下载任务状态
    ///
    /// 返回 None 表示仍在进行中，不需要状态转换
    /// 支持 `folder:` 前缀的任务 ID，会查询 FolderDownloadManager 获取文件夹下载状态
    async fn aggregate_download_status(
        download_manager: &Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: &Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        download_task_ids: &[String],
    ) -> Option<TransferStatus> {
        let dm_lock = download_manager.read().await;
        let dm = match dm_lock.as_ref() {
            Some(m) => m,
            None => return Some(TransferStatus::DownloadFailed),
        };

        let fdm_lock = folder_download_manager.read().await;

        let mut completed_count = 0;
        let mut failed_count = 0;
        let mut downloading_count = 0;
        let mut paused_count = 0;
        let mut cancelled_count = 0;

        for task_id in download_task_ids {
            if let Some(folder_id) = task_id.strip_prefix("folder:") {
                // 文件夹下载任务：查询 FolderDownloadManager
                if let Some(ref fdm) = *fdm_lock {
                    if let Some(folder) = fdm.get_folder(folder_id).await {
                        match folder.status {
                            FolderStatus::Completed => completed_count += 1,
                            FolderStatus::Failed => failed_count += 1,
                            FolderStatus::Downloading | FolderStatus::Scanning => downloading_count += 1,
                            FolderStatus::Paused => paused_count += 1,
                            FolderStatus::Cancelled => cancelled_count += 1,
                        }
                    } else {
                        // 文件夹任务不存在，视为已取消
                        cancelled_count += 1;
                    }
                } else {
                    // FolderDownloadManager 未设置，视为失败
                    failed_count += 1;
                }
            } else {
                // 普通文件下载任务：查询 DownloadManager
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

        // 解析文件列表（从持久化的 JSON 恢复）
        let file_list = metadata
            .file_list_json
            .as_ref()
            .and_then(|json_str| serde_json::from_str::<Vec<SharedFileInfo>>(json_str).ok())
            .unwrap_or_default();

        // 转换转存状态
        let status = match metadata.transfer_status.as_deref() {
            Some("completed") => TransferStatus::Completed,
            Some("transferred") => TransferStatus::Transferred,
            Some("transfer_failed") => TransferStatus::TransferFailed,
            Some("download_failed") => TransferStatus::DownloadFailed,
            _ => TransferStatus::Completed, // 已完成的任务默认使用 Completed
        };

        // 根据文件列表计算 total_count 和 transferred_count
        let total_count = if !file_list.is_empty() {
            file_list.len()
        } else {
            metadata.download_task_ids.len()
        };
        let transferred_count = total_count;

        Some(TransferTask {
            id: metadata.task_id.clone(),
            share_url,
            password: metadata.share_pwd.clone(),
            save_path,
            save_fs_id,
            auto_download: metadata.auto_download.unwrap_or(false),
            local_download_path: None,
            status,
            error: None,
            download_task_ids: metadata.download_task_ids.clone(),
            share_info,
            file_list,
            transferred_count,
            total_count,
            created_at: metadata.created_at.timestamp(),
            updated_at: metadata.updated_at.timestamp(),
            failed_download_ids: Vec::new(),
            completed_download_ids: Vec::new(),
            download_started_at: None,
            file_name: metadata.transfer_file_name.clone(),
            is_share_direct_download: metadata.is_share_direct_download.unwrap_or(false),
            temp_dir: metadata.temp_dir.clone(),
            selected_fs_ids: None,
            selected_files: None,
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
    ///
    /// 扩展的取消逻辑，支持分享直下任务的清理：
    /// - CheckingShare 状态：停止解析，设置状态为 TransferFailed
    /// - Transferring 状态：停止转存，清理临时文件（如果是分享直下），设置状态为 TransferFailed
    /// - Downloading 状态：取消下载任务，清理临时文件（如果是分享直下），设置状态为 DownloadFailed
    /// - Cleaning 状态：等待清理完成（最多 30 秒）
    ///
    /// # Requirements
    /// - 5.1: CheckingShare 状态取消
    /// - 5.2: Transferring 状态取消并清理
    /// - 5.3: Downloading 状态取消并清理
    /// - 5.4: Cleaning 状态等待完成
    pub async fn cancel_task(&self, id: &str) -> Result<()> {
        let task_info = self.tasks.get(id).context("任务不存在")?;
        let task = task_info.task.clone();
        let cancellation_token = task_info.cancellation_token.clone();
        drop(task_info);

        // 获取当前状态和分享直下相关信息
        let (current_status, is_share_direct_download, temp_dir) = {
            let t = task.read().await;
            (t.status.clone(), t.is_share_direct_download, t.temp_dir.clone())
        };

        info!(
            "取消转存任务: id={}, status={:?}, is_share_direct_download={}",
            id, current_status, is_share_direct_download
        );

        match current_status {
            // Requirement 5.4: Cleaning 状态返回提示，不阻塞等待
            TransferStatus::Cleaning => {
                info!("任务正在清理中，无需取消: task_id={}", id);
                // 不阻塞 HTTP 请求，直接返回提示
                // 清理完成后 watcher 会自动将状态更新为 Completed
                Ok(())
            }

            // Requirement 5.1: CheckingShare 状态取消
            TransferStatus::CheckingShare => {
                cancellation_token.cancel();

                {
                    let mut t = task.write().await;
                    t.mark_transfer_failed("用户取消".to_string());
                }

                // 发送状态变更事件
                self.publish_event(TransferEvent::StatusChanged {
                    task_id: id.to_string(),
                    old_status: "checking_share".to_string(),
                    new_status: "transfer_failed".to_string(),
                }).await;

                info!("取消转存任务成功（CheckingShare）: {}", id);
                Ok(())
            }

            // Requirement 5.2: Transferring 状态取消并清理
            TransferStatus::Transferring => {
                cancellation_token.cancel();

                {
                    let mut t = task.write().await;
                    t.mark_transfer_failed("用户取消".to_string());
                }

                // 发送状态变更事件
                self.publish_event(TransferEvent::StatusChanged {
                    task_id: id.to_string(),
                    old_status: "transferring".to_string(),
                    new_status: "transfer_failed".to_string(),
                }).await;

                // 分享直下任务：清理临时目录
                if is_share_direct_download {
                    if let Some(ref temp_dir) = temp_dir {
                        let (cleanup_on_failure, configured_root) = {
                            let cfg = self.app_config.read().await;
                            (cfg.share_direct_download.cleanup_on_failure, cfg.share_direct_download.temp_dir.clone())
                        };

                        if cleanup_on_failure {
                            info!("转存取消，触发临时目录清理: task_id={}, temp_dir={}", id, temp_dir);
                            let client_snap = self.client.read().unwrap().clone();
                            let cleanup = Self::cleanup_temp_dir_internal(&client_snap, temp_dir, &configured_root).await;
                            info!("转存取消清理结果: task_id={}, status={:?}", id, cleanup.status);
                            if let Some(pm) = self.persistence_manager().await {
                                if let Err(e) = pm.lock().await.update_cleanup_status(id, cleanup.status) {
                                    warn!("持久化清理状态失败: task_id={}, error={}", id, e);
                                }
                            }
                        }
                    }
                }

                info!("取消转存任务成功（Transferring）: {}", id);
                Ok(())
            }

            // Requirement 5.3: Downloading 状态取消并清理
            TransferStatus::Downloading => {
                cancellation_token.cancel();

                // 取消关联的下载任务
                let download_task_ids = {
                    let t = task.read().await;
                    t.download_task_ids.clone()
                };

                // 取消下载任务（使用 cancel_task_without_delete 仅停止任务，不删除）
                if let Some(dm) = self.download_manager.read().await.as_ref() {
                    for download_id in &download_task_ids {
                        dm.cancel_task_without_delete(download_id).await;
                    }
                }

                {
                    let mut t = task.write().await;
                    t.mark_download_failed();
                    t.error = Some("用户取消".to_string());
                }

                // 发送状态变更事件
                self.publish_event(TransferEvent::StatusChanged {
                    task_id: id.to_string(),
                    old_status: "downloading".to_string(),
                    new_status: "download_failed".to_string(),
                }).await;

                // 分享直下任务：清理临时目录
                if is_share_direct_download {
                    if let Some(ref temp_dir) = temp_dir {
                        let (cleanup_on_failure, configured_root) = {
                            let cfg = self.app_config.read().await;
                            (cfg.share_direct_download.cleanup_on_failure, cfg.share_direct_download.temp_dir.clone())
                        };

                        if cleanup_on_failure {
                            info!("下载取消，触发临时目录清理: task_id={}, temp_dir={}", id, temp_dir);
                            let client_snap = self.client.read().unwrap().clone();
                            let cleanup = Self::cleanup_temp_dir_internal(&client_snap, temp_dir, &configured_root).await;
                            info!("下载取消清理结果: task_id={}, status={:?}", id, cleanup.status);
                            if let Some(pm) = self.persistence_manager().await {
                                if let Err(e) = pm.lock().await.update_cleanup_status(id, cleanup.status) {
                                    warn!("持久化清理状态失败: task_id={}, error={}", id, e);
                                }
                            }
                        }
                    }
                }

                info!("取消转存任务成功（Downloading）: {}", id);
                Ok(())
            }

            // 其他状态（Queued, Transferred, TransferFailed, DownloadFailed, Completed）
            _ => {
                // 终止状态不需要取消
                if current_status.is_terminal() {
                    info!("任务已处于终止状态，无需取消: task_id={}, status={:?}", id, current_status);
                    return Ok(());
                }

                // Queued 状态：直接取消
                cancellation_token.cancel();

                {
                    let mut t = task.write().await;
                    t.mark_transfer_failed("用户取消".to_string());
                }

                // 发送状态变更事件
                self.publish_event(TransferEvent::StatusChanged {
                    task_id: id.to_string(),
                    old_status: format!("{:?}", current_status).to_lowercase(),
                    new_status: "transfer_failed".to_string(),
                }).await;

                info!("取消转存任务成功: task_id={}, old_status={:?}", id, current_status);
                Ok(())
            }
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

        // 恢复文件列表
        if let Some(ref json) = recovery_info.file_list_json {
            if let Ok(file_list) = serde_json::from_str::<Vec<SharedFileInfo>>(json) {
                task.set_file_list(file_list);
            }
        }

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
                // 恢复分享直下相关字段
                task.is_share_direct_download = recovery_info.is_share_direct_download;
                task.temp_dir = recovery_info.temp_dir.clone();
                info!(
                    "恢复转存任务(下载中): id={}, 关联下载任务数={}, is_share_direct_download={}",
                    task_id,
                    recovery_info.download_task_ids.len(),
                    recovery_info.is_share_direct_download
                );
            }
            "cleaning" => {
                // 清理中状态（分享直下任务），重试清理
                task.status = TransferStatus::Cleaning;
                // 恢复分享直下相关字段
                task.is_share_direct_download = true;
                task.temp_dir = recovery_info.temp_dir.clone();
                info!(
                    "恢复转存任务(清理中): id={}, temp_dir={:?}",
                    task_id, recovery_info.temp_dir
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
            let pm = self.persistence_manager.lock().await.clone();
            Self::start_download_status_watcher(
                self.client.clone(),
                self.tasks.clone(),
                self.download_manager.clone(),
                self.folder_download_manager.clone(),
                self.app_config.clone(),
                pm,
                ws_manager,
                task_id.clone(),
                cancellation_token,
            );
        }

        // 如果是清理中状态，重试清理
        if status == "cleaning" {
            if let Some(ref temp_dir) = recovery_info.temp_dir {
                let client = self.client.clone();
                let tasks = self.tasks.clone();
                let ws_manager = self.ws_manager.read().await.clone();
                let pm_for_cleanup = self.persistence_manager().await;
                let configured_root = self.app_config.read().await.share_direct_download.temp_dir.clone();
                let temp_dir = temp_dir.clone();
                let task_id_clone = task_id.clone();

                tokio::spawn(async move {
                    info!("重试清理临时目录: task_id={}, temp_dir={}", task_id_clone, temp_dir);
                    let client_snap = client.read().unwrap().clone();
                    let cleanup = Self::cleanup_temp_dir_internal(&client_snap, &temp_dir, &configured_root).await;
                    info!("重试清理结果: task_id={}, status={:?}", task_id_clone, cleanup.status);

                    // 持久化清理状态
                    if let Some(ref pm_arc) = pm_for_cleanup {
                        if let Err(e) = pm_arc.lock().await.update_cleanup_status(&task_id_clone, cleanup.status) {
                            warn!("持久化清理状态失败: task_id={}, error={}", task_id_clone, e);
                        }
                    }

                    // 清理完成，更新状态为 Completed
                    if let Some(task_info) = tasks.get(&task_id_clone) {
                        let mut t = task_info.task.write().await;
                        let old_status = format!("{:?}", t.status).to_lowercase();
                        t.mark_completed();

                        // 发送状态变更事件
                        if let Some(ref ws) = ws_manager {
                            ws.send_if_subscribed(
                                TaskEvent::Transfer(TransferEvent::StatusChanged {
                                    task_id: task_id_clone.clone(),
                                    old_status,
                                    new_status: "completed".to_string(),
                                }),
                                None,
                            );
                        }
                    }
                });
            }
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

    // ========================================================================
    // 🔥 孤立目录清理
    // ========================================================================

    /// 清理孤立的临时目录
    ///
    /// 扫描临时目录下的所有子目录，找出不属于任何活跃任务的目录（孤立目录），
    /// 然后删除这些孤立目录。
    ///
    /// # Returns
    /// 清理结果，包含删除的目录数和失败的目录列表
    pub async fn cleanup_orphaned_temp_dirs(&self) -> CleanupOrphanedResult {
        let temp_dir_base = {
            let cfg = self.app_config.read().await;
            cfg.share_direct_download.temp_dir.clone()
        };

        info!("开始清理孤立临时目录: base={}", temp_dir_base);

        // 安全守卫：配置的临时根目录不能是 /、空、或过短
        let root_trimmed = temp_dir_base.trim_end_matches('/');
        if root_trimmed.len() < 2 || !root_trimmed.starts_with('/') {
            error!(
                "配置的临时目录根不安全，拒绝执行孤立目录清理: configured_root={}",
                temp_dir_base
            );
            return CleanupOrphanedResult {
                deleted_count: 0,
                failed_paths: vec![],
                error: Some(format!(
                    "配置的临时目录根不安全（过短或非绝对路径）: {}",
                    temp_dir_base
                )),
            };
        }

        // 1. 获取临时目录下的所有子目录
        let client_snapshot = self.client.read().unwrap().clone();
        let list_result = client_snapshot.get_file_list(&temp_dir_base, 1, 1000).await;
        let subdirs = match list_result {
            Ok(response) => {
                if response.errno != 0 {
                    // API 返回错误
                    let err_msg = if response.errmsg.is_empty() {
                        format!("API 错误码: {}", response.errno)
                    } else {
                        response.errmsg
                    };
                    // 如果目录不存在，说明没有临时文件需要清理
                    if response.errno == -9 {
                        info!("临时目录不存在，无需清理: {}", temp_dir_base);
                        return CleanupOrphanedResult {
                            deleted_count: 0,
                            failed_paths: vec![],
                            error: None,
                        };
                    }
                    warn!("列出临时目录失败: {}", err_msg);
                    return CleanupOrphanedResult {
                        deleted_count: 0,
                        failed_paths: vec![],
                        error: Some(err_msg),
                    };
                }
                response.list
                    .into_iter()
                    .filter(|f| f.isdir == 1)
                    .map(|f| f.path)
                    .collect::<Vec<_>>()
            }
            Err(e) => {
                let err_msg = e.to_string();
                // 如果目录不存在，说明没有临时文件需要清理
                if err_msg.contains("不存在") || err_msg.contains("not found") || err_msg.contains("-9") {
                    info!("临时目录不存在，无需清理: {}", temp_dir_base);
                    return CleanupOrphanedResult {
                        deleted_count: 0,
                        failed_paths: vec![],
                        error: None,
                    };
                }
                warn!("列出临时目录失败: {}", err_msg);
                return CleanupOrphanedResult {
                    deleted_count: 0,
                    failed_paths: vec![],
                    error: Some(err_msg),
                };
            }
        };

        if subdirs.is_empty() {
            info!("临时目录为空，无需清理");
            return CleanupOrphanedResult {
                deleted_count: 0,
                failed_paths: vec![],
                error: None,
            };
        }

        // 2. 获取当前所有活跃任务的 temp_dir 集合
        let active_temp_dirs: std::collections::HashSet<String> = self
            .tasks
            .iter()
            .filter_map(|entry| {
                // 使用 try_read 避免阻塞
                if let Ok(task) = entry.value().task.try_read() {
                    task.temp_dir.clone()
                } else {
                    None
                }
            })
            .collect();

        // 3. 找出孤立目录（不属于任何活跃任务的目录）
        let orphaned_dirs: Vec<String> = subdirs
            .into_iter()
            .filter(|path| {
                // 规范化路径格式进行比较
                let normalized = if path.ends_with('/') {
                    path.clone()
                } else {
                    format!("{}/", path)
                };
                !active_temp_dirs.contains(&normalized) && !active_temp_dirs.contains(path)
            })
            .collect();

        if orphaned_dirs.is_empty() {
            info!("没有孤立目录需要清理");
            return CleanupOrphanedResult {
                deleted_count: 0,
                failed_paths: vec![],
                error: None,
            };
        }

        info!("发现 {} 个孤立目录，开始清理", orphaned_dirs.len());

        // 4. 删除孤立目录
        let delete_result = client_snapshot.delete_files(&orphaned_dirs).await;
        match delete_result {
            Ok(result) => {
                if result.success {
                    info!("成功清理 {} 个孤立目录", result.deleted_count);
                } else {
                    warn!(
                        "部分孤立目录清理失败: 成功={}, 失败={:?}",
                        result.deleted_count, result.failed_paths
                    );
                }
                CleanupOrphanedResult {
                    deleted_count: result.deleted_count,
                    failed_paths: result.failed_paths,
                    error: result.error,
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                error!("清理孤立目录失败: {}", err_msg);
                CleanupOrphanedResult {
                    deleted_count: 0,
                    failed_paths: orphaned_dirs,
                    error: Some(err_msg),
                }
            }
        }
    }
}

/// 清理孤立目录的结果
#[derive(Debug, Clone, serde::Serialize)]
pub struct CleanupOrphanedResult {
    /// 成功删除的目录数
    pub deleted_count: usize,
    /// 删除失败的目录路径列表
    pub failed_paths: Vec<String>,
    /// 错误信息（如果有）
    pub error: Option<String>,
}

impl TransferManager {
    /// 启动时清理孤立目录（如果配置启用）
    ///
    /// 检查 `cleanup_orphaned_on_startup` 配置，如果为 true 则执行清理
    pub async fn cleanup_orphaned_on_startup_if_enabled(&self) {
        let cleanup_enabled = {
            let cfg = self.app_config.read().await;
            cfg.share_direct_download.cleanup_orphaned_on_startup
        };

        if cleanup_enabled {
            info!("启动时清理孤立临时目录已启用，开始清理...");
            let result = self.cleanup_orphaned_temp_dirs().await;
            if let Some(ref err) = result.error {
                warn!("启动时清理孤立目录部分失败: {}", err);
            }
            if result.deleted_count > 0 {
                info!("启动时清理了 {} 个孤立目录", result.deleted_count);
            }
        } else {
            info!("启动时清理孤立临时目录已禁用");
        }
    }
}

/// 根据 selected_fs_ids 构建实际要转存的 fs_id 列表
///
/// - selected_fs_ids 为 None 或空数组 → 返回 file_list 中所有文件的 fs_id（向后兼容）
/// - selected_fs_ids 非空 → 直接返回用户选择的 fs_id 列表（包括文件夹）
pub fn build_fs_ids(
    file_list: &[SharedFileInfo],
    selected_fs_ids: &Option<Vec<u64>>,
) -> Vec<u64> {
    if let Some(ref selected) = selected_fs_ids {
        if selected.is_empty() {
            file_list.iter().map(|f| f.fs_id).collect()
        } else {
            // 直接使用用户选择的 fs_id 列表，不过滤文件夹
            // 用户明确选择了文件夹就应该转存文件夹
            selected.clone()
        }
    } else {
        file_list.iter().map(|f| f.fs_id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_task_errno_negative_30() {
        let msg = "异步转存任务失败: task_errno=-30, response={...}";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(-30));
    }

    #[test]
    fn test_extract_task_errno_negative_31() {
        let msg = "异步转存任务失败: task_errno=-31, response={...}";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(-31));
    }

    #[test]
    fn test_extract_task_errno_negative_32() {
        let msg = "异步转存任务失败: task_errno=-32, response={...}";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(-32));
    }

    #[test]
    fn test_extract_task_errno_negative_33() {
        let msg = "异步转存任务失败: task_errno=-33, response={...}";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(-33));
    }

    #[test]
    fn test_extract_task_errno_positive() {
        let msg = "task_errno=12 something";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(12));
    }

    #[test]
    fn test_extract_task_errno_zero() {
        let msg = "task_errno=0";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(0));
    }

    #[test]
    fn test_extract_task_errno_no_match() {
        let msg = "转存请求失败: connection timeout";
        assert_eq!(TransferManager::extract_task_errno(msg), None);
    }

    #[test]
    fn test_extract_task_errno_empty_string() {
        assert_eq!(TransferManager::extract_task_errno(""), None);
    }

    #[test]
    fn test_extract_task_errno_partial_match() {
        let msg = "some error task_errno=";
        assert_eq!(TransferManager::extract_task_errno(msg), None);
    }

    #[test]
    fn test_extract_task_errno_embedded_in_long_message() {
        let msg = "异步转存任务失败: task_errno=-30, response={\"errno\":0,\"task_id\":123456,\"task_errno\":-30,\"status\":\"failed\"}";
        assert_eq!(TransferManager::extract_task_errno(msg), Some(-30));
    }
}
