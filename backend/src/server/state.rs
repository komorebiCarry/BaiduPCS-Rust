// 应用状态

use crate::auth::{QRCodeAuth, SessionManager, UserAuth};
use crate::config::AppConfig;
use crate::downloader::{DownloadManager, FolderDownloadManager};
use crate::netdisk::NetdiskClient;
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
            config: Arc::new(RwLock::new(config)),
        })
    }

    /// 初始化时加载会话
    pub async fn load_initial_session(&self) -> anyhow::Result<()> {
        let mut session_manager = self.session_manager.lock().await;
        if let Some(user_auth) = session_manager.get_session().await? {
            *self.current_user.write().await = Some(user_auth.clone());

            // 初始化网盘客户端
            let client = NetdiskClient::new(user_auth.clone())?;
            let client_arc = Arc::new(client.clone());
            *self.netdisk_client.write().await = Some(client);

            // 初始化下载管理器（从配置读取参数）
            let config = self.config.read().await;
            let download_dir = config.download.download_dir.clone();
            let max_global_threads = config.download.max_global_threads;
            let max_concurrent_tasks = config.download.max_concurrent_tasks;
            drop(config);

            let manager = DownloadManager::with_config(
                user_auth,
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
        }
        Ok(())
    }
}

// 注意：Default trait 不能用于 async，移除或使用 lazy_static
