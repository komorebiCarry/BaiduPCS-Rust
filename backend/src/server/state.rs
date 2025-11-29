// 应用状态

use crate::auth::{QRCodeAuth, SessionManager, UserAuth};
use crate::config::AppConfig;
use crate::downloader::{DownloadManager, FolderDownloadManager};
use crate::netdisk::NetdiskClient;
use crate::uploader::UploadManager;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

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
    /// 应用配置
    pub config: Arc<RwLock<AppConfig>>,
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

        Ok(Self {
            qrcode_auth: Arc::new(QRCodeAuth::new()?),
            session_manager: Arc::new(Mutex::new(SessionManager::default())),
            current_user: Arc::new(RwLock::new(None)),
            netdisk_client: Arc::new(RwLock::new(None)),
            download_manager: Arc::new(RwLock::new(None)),
            folder_download_manager,
            upload_manager: Arc::new(RwLock::new(None)),
            config: Arc::new(RwLock::new(config)),
        })
    }

    /// 初始化时加载会话
    pub async fn load_initial_session(&self) -> anyhow::Result<()> {
        let mut session_manager = self.session_manager.lock().await;
        if let Some(mut user_auth) = session_manager.get_session().await? {
            *self.current_user.write().await = Some(user_auth.clone());

            // 初始化网盘客户端
            let client = NetdiskClient::new(user_auth.clone())?;

            // 如果没有预热 Cookie,执行预热并保存
            if user_auth.panpsc.is_none() || user_auth.csrf_token.is_none() || user_auth.bdstoken.is_none() {
                tracing::info!("服务启动检测到会话未预热,开始预热...");
                match client.warmup_and_get_cookies().await {
                    Ok((panpsc, csrf_token, bdstoken, stoken)) => {
                        tracing::info!("预热成功,更新 session.json");
                        user_auth.panpsc = panpsc;
                        user_auth.csrf_token = csrf_token;
                        user_auth.bdstoken = bdstoken;
                        // 预热时下发的 STOKEN 优先于之前保存的
                        if stoken.is_some() {
                            user_auth.stoken = stoken;
                        }

                        // 更新内存中的用户信息
                        *self.current_user.write().await = Some(user_auth.clone());

                        // 保存到 session.json
                        if let Err(e) = session_manager.save_session(&user_auth).await {
                            tracing::error!("保存预热 Cookie 失败: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("预热失败(可能需要重新登录): {}", e);
                    }
                }
            } else {
                tracing::info!("检测到已有预热 Cookie,跳过预热");
            }

            let client_arc = Arc::new(client.clone());
            *self.netdisk_client.write().await = Some(client.clone());

            // 初始化下载管理器（从配置读取参数）
            let config = self.config.read().await;
            let download_dir = config.download.download_dir.clone();
            let max_global_threads = config.download.max_global_threads;
            let max_concurrent_tasks = config.download.max_concurrent_tasks;
            drop(config);

            let manager = DownloadManager::with_config(
                user_auth.clone(),
                download_dir,
                max_global_threads,
                max_concurrent_tasks,
            )?;
            let manager_arc = Arc::new(manager);
            *self.download_manager.write().await = Some(Arc::clone(&manager_arc));

            // 设置文件夹下载管理器的依赖
            self.folder_download_manager
                .set_download_manager(Arc::clone(&manager_arc))
                .await;
            self.folder_download_manager
                .set_netdisk_client(client_arc)
                .await;

            // 初始化上传管理器（从配置读取参数）
            let config = self.config.read().await;
            let upload_config = config.upload.clone();
            drop(config);

            let upload_manager = UploadManager::new_with_config(client, &user_auth, &upload_config);
            let upload_manager_arc = Arc::new(upload_manager);
            *self.upload_manager.write().await = Some(upload_manager_arc);
        }
        Ok(())
    }
}

// 注意：Default trait 不能用于 async，移除或使用 lazy_static
