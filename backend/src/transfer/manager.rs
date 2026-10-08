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
use crate::transfer::temp_cleanup;
use crate::transfer::types::{BatchGroupInfo, CleanupResult, CleanupStatus, ShareLink, SharePageInfo, SharedFileInfo, TransferResult};
use anyhow::{Context, Result};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
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
    /// 🔥 多账号归属 UID
    ///
    /// per-uid manager 创建后由 setter 注入或在 `new` 时传入。
    /// 所有 TransferTask 创建点都会链调 .with_owner_uid(self.owner_uid)。
    owner_uid: crate::auth::Uid,
    /// 孤儿临时目录清理互斥锁：手动清理可能跑很久（前端超时后用户再点一次）、
    /// 还会与周期清理撞上，并发跑只会成倍发删除请求、更容易触发 errno=132 风控
    orphan_sweep_lock: Arc<Mutex<()>>,
}

/// 创建转存任务请求
#[derive(Debug, Clone)]
pub struct CreateTransferRequest {
    pub share_url: String,
    pub password: Option<String>,
    /// Caller-provided share randsk. Internal callers use this to keep
    /// concurrent password-protected shares from overwriting each other in the
    /// shared CookieJar.
    pub randsk: Option<String>,
    pub save_path: String,
    pub save_fs_id: u64,
    pub auto_download: Option<bool>,
    pub local_download_path: Option<String>,
    /// 自动下载时使用的本地文件冲突策略；None 时使用全局下载默认策略
    pub download_conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,
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
    /// 🔥 显式 owner_uid 覆盖
    ///
    /// `Some(uid)` → 在创建 task / 持久化 / Created event / 衍生下载之前
    /// 就用此 UID（**而不是事后 override** —— 避免持久化和异步执行竞态）。
    /// `None` → 沿用 `TransferManager.owner_uid`（per-uid manager 架构下即该
    /// manager 自己的 uid；历史共享 Arc / 测试路径下默认是 startup active）。
    pub owner_uid_override: Option<crate::auth::Uid>,

    /// 标记为「分享同步」内部转存任务：从「转存管理」列表隐藏（对齐自动备份隔离）。
    pub is_internal: bool,

    /// 同步配置 id（`"share-sync:{订阅id}"`）。
    /// 设置后，本次转存的自动下载子任务改走 `DownloadManager::create_backup_task`，
    /// 从而从「下载管理」隐藏并走自动备份同款下载槽优先级、归属到分享同步而非自动备份。
    pub backup_config_id: Option<String>,

    /// 已解析的分享上下文（share-sync 拆批复用）。
    ///
    /// 分享同步抓快照时已 `access_share_page` 解析过该分享；大目录二分拆批会对
    /// 同一分享反复 `create_task`，若每批都重新 `access_share_page` 则请求频率翻倍、
    /// 更易触发账号风控（errno=132）。`Some(_)` 时 `create_task` 跳过
    /// `access_share_page`，直接复用此 `SharePageInfo`（密码分享仍会重新校验提取码
    /// 以刷新本次转存所需的 cookie）。其它调用方传 `None`，行为不变。
    pub prefetched_share: Option<SharePageInfo>,
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
    /// 分享根的绝对路径（来自 share/list?root=1 响应的 title 字段）
    pub share_root_path: Option<String>,
    /// 分享体系类型（前端需原样回传，子目录导航要据此分流）
    pub kind: crate::transfer::ShareKind,
    /// 提取码校验换来的凭据
    ///
    /// 个人版是 randsk（已同时写进 Cookie，回传只是为了统一）；
    /// 企业版是 spwd，**后续每个请求都必须显式带上**，不回传就没法翻子目录。
    pub token: Option<String>,
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
    /// 创建新的转存管理器（多账号：owner_uid 初始为 Uid::default()、
    /// 调用方应在创建后立即调 `set_owner_uid` 或使用 `new_with_owner`）
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
            // 多账号：初始为 Uid(0)、由 set_owner_uid 注入
            owner_uid: crate::auth::Uid::default(),
            orphan_sweep_lock: Arc::new(Mutex::new(())),
        }
    }

    /// 🔥 多账号：设置该管理器所属的账号 UID（同 DownloadManager::set_owner_uid）
    pub fn set_owner_uid(&mut self, uid: crate::auth::Uid) {
        self.owner_uid = uid;
    }

    /// 🔥 多账号：获取该管理器所属的账号 UID
    pub fn owner_uid(&self) -> crate::auth::Uid {
        self.owner_uid
    }

    /// 🔥 覆盖单个任务的归属 UID
    ///
    /// 用于 handler 接收到 `req.uid` 显式归属时，纠正 `create_task` 默认使用的
    /// `self.owner_uid`（active 账号）。仅修改运行态 `TransferTask.owner_uid`，
    /// **不会回写 `.meta` 文件**。
    ///
    /// ⚠️ **现已弃用建议**：`CreateTransferRequest` 已支持
    /// `owner_uid_override` 字段（参见 `transfer/manager.rs::create_task`），
    /// task / 持久化 / Created event / 衍生下载在创建瞬间就用 effective_uid。
    /// 新调用方应优先用 `owner_uid_override`，避免本方法的事后纠错路径。
    /// 本方法保留仅供已存在任务的运行态纠错。
    ///
    /// ⚠️ **关于 .meta**：Transfer 任务有 `.meta`（由 `register_transfer_task` 写入）；
    /// 但本方法仍只覆盖运行态字段，**不重写 .meta**，因为 `.meta` 已由
    /// `register_transfer_task(owner_uid_override=Some(effective_uid.raw()))`
    /// 在 task 创建时一次性写正确，无需再改。
    ///
    /// 语义：
    /// - 任务存在 → 直接修改 task.owner_uid + 返回 Ok
    /// - 任务不存在 → 警告日志 + 返回 Ok（幂等，避免 race condition 报错）
    pub async fn override_task_owner_uid(
        &self,
        task_id: &str,
        new_owner_uid: crate::auth::Uid,
    ) -> anyhow::Result<()> {
        if let Some(entry) = self.tasks.get(task_id) {
            let mut t = entry.task.write().await;
            t.owner_uid = new_owner_uid;
        } else {
            tracing::warn!(
                "TransferManager::override_task_owner_uid: 任务不存在: {}（可能已迁出）",
                task_id
            );
        }
        Ok(())
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

    /// 取下载管理器句柄（供分享同步进度广播 / 轮询接口读取子任务下载进度）
    pub async fn download_manager_handle(&self) -> Option<Arc<DownloadManager>> {
        self.download_manager.read().await.clone()
    }

    /// 取文件夹下载管理器句柄（供分享同步收集 tree 模式产生的文件夹子任务进度）
    pub async fn folder_download_manager_handle(&self) -> Option<Arc<FolderDownloadManager>> {
        self.folder_download_manager.read().await.clone()
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

        // 1. 解析分享链接（个人版 / 企业版由 netdisk::share 自动判定）
        let mut share_link = self.client.read().unwrap().parse_share_link(share_url)?;

        // 合并密码：请求中的密码 > 链接中的密码
        let password = password.or(share_link.password.clone());
        share_link.password = password.clone();

        // 🔥 从共享引用快照当前客户端
        let client = self.client.read().unwrap().clone();

        // 2. 访问分享页面，获取分享信息
        let share_info = client.access_share_page_for(&share_link, true).await?;

        // 3. 如果有密码，验证密码
        //
        // 拿到的凭据个人版是 randsk（同时已写进 Cookie），企业版是 spwd（必须
        // 显式带进后续每个请求）。所以这里不能像以前那样丢弃返回值。
        let token = match password {
            Some(ref pwd) => {
                let t = client.verify_share_password_for(&share_info, pwd).await?;
                info!("预览: 提取码验证成功");
                Some(t)
            }
            None => None,
        };

        // 4. 获取文件列表（根目录，由前端传入分页参数）
        let list_result = client
            .list_share_files_for(&share_info, page, num, token.as_deref())
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
            short_key: share_info.short_key.clone(),
            shareid,
            uk,
            bdstoken: share_info.bdstoken,
            share_root_path: list_result.share_root_path,
            kind: share_info.kind,
            token,
        })
    }

    /// 浏览分享链接中指定目录的文件列表
    ///
    /// 用于文件夹导航：前端点击文件夹后，调用此方法获取子目录内容。
    /// 需要传入首次预览时获取的 share_info，避免重复访问分享页面。
    ///
    /// `kind` / `token` 由前端从首次预览的结果原样回传：企业版的 spwd
    /// 不像个人版 randsk 那样存在 Cookie 里，不带上就取不到子目录。
    #[allow(clippy::too_many_arguments)]
    pub async fn preview_share_dir(
        &self,
        short_key: &str,
        shareid: &str,
        uk: &str,
        bdstoken: &str,
        dir: &str,
        page: u32,
        num: u32,
        kind: crate::transfer::ShareKind,
        token: Option<&str>,
    ) -> Result<Vec<SharedFileInfo>> {
        info!(
            "浏览分享子目录: kind={:?}, short_key={}, dir={}, page={}, num={}",
            kind, short_key, dir, page, num
        );

        // 从前端回传的散字段还原分享上下文，避免重新访问分享页
        let share_info = crate::transfer::SharePageInfo {
            shareid: shareid.to_string(),
            uk: uk.to_string(),
            share_uk: uk.to_string(),
            bdstoken: bdstoken.to_string(),
            kind,
            short_key: short_key.to_string(),
        };

        let client = self.client.read().unwrap().clone();
        let file_list = client
            .list_share_files_in_dir_for(&share_info, dir, page, num, token)
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

        // 🔥 effective_uid 在 task 创建前就确定
        //
        // 当前架构是 per-uid 独立 `TransferManager`，但 `self.owner_uid` 是 manager
        // 构造时的 owner（启动 active 或登录账号），与 handler 显式传入的 `req.uid`
        // 不一定一致（前端按 active 切换、显式覆盖、批量场景）。事后
        // `override_task_owner_uid` 会与持久化、Created event、async execute_task /
        // 衍生下载竞态。这里在 task
        // 构造之前就确定 effective_uid，下游所有路径（task.with_owner_uid /
        // metadata 持久化 / Created event / execute_task 衍生下载）统一使用。
        let effective_uid = request.owner_uid_override.unwrap_or(self.owner_uid);

        // 1. 解析分享链接
        let share_link = self.client.read().unwrap().parse_share_link(&request.share_url)?;

        // 合并密码：请求中的密码 > 链接中的密码
        let password = request.password.or(share_link.password.clone());

        // 重新创建 share_link 用于后续使用（避免部分移动问题）
        let share_link = ShareLink {
            short_key: share_link.short_key,
            raw_url: share_link.raw_url,
            password: password.clone(), // 密码已提取
            kind: share_link.kind,
        };

        // 2. 处理分享直下模式
        let (save_path, save_fs_id, auto_download, temp_dir) = if request.is_share_direct_download {
            // 分享直下模式：生成临时目录路径
            let task_uuid = uuid::Uuid::new_v4().to_string();
            let app_cfg = self.app_config.read().await;
            let temp_dir_base = &app_cfg.share_direct_download.temp_dir;
            // 临时目录：{config.temp_dir}/inst-{实例id}/{uuid}/（实例 id 不可用时退回
            // {config.temp_dir}/{uuid}/）。按实例分命名空间，自动孤儿清理只动自己的目录。
            let namespace = temp_cleanup::own_namespace();
            let temp_dir =
                temp_cleanup::build_task_temp_dir(temp_dir_base, namespace.as_deref(), &task_uuid);
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

        // 3. 创建任务（多账号：链调 with_owner_uid 用 effective_uid，避免事后 override）
        let mut task = TransferTask::new(
            request.share_url.clone(),
            password.clone(),
            save_path.clone(),
            save_fs_id,
            auto_download,
            request.local_download_path.clone(),
        )
            .with_owner_uid(effective_uid);

        // 设置分享直下相关字段
        if request.is_share_direct_download {
            task.is_share_direct_download = true;
            task.temp_dir = temp_dir.clone();
        }

        // 设置选择性转存字段
        task.selected_fs_ids = request.selected_fs_ids.clone();
        task.selected_files = request.selected_files.clone();

        // 分享同步内部任务标记 + 同步配置 id（决定衍生下载是否走 create_backup_task）
        task.is_internal = request.is_internal;
        task.backup_config_id = request.backup_config_id.clone();

        let task_id = task.id.clone();

        // 4. 获取分享信息：share-sync 拆批复用已抓快照时解析的上下文，
        //    跳过逐批 access_share_page（降低请求频率、规避账号风控）；
        //    其它调用方仍走 access_share_page 解析。
        let client = self.client.read().unwrap().clone();
        let share_info_result = match request.prefetched_share.clone() {
            Some(mut info) => {
                info!(
                    "复用已捕获分享上下文，跳过 access_share_page: shareid={}",
                    info.shareid
                );
                // 兼容老数据：`kind`/`short_key` 是后加的字段，重启后从库里读回来的
                // 快照没有它们（serde default 会给出 Personal + 空串）。空的 short_key
                // 会让个人版列表接口的 shorturl 变成空，这里用刚解析出的链接补齐。
                if info.short_key.is_empty() {
                    info.short_key = share_link.short_key.clone();
                    info.kind = share_link.kind;
                }
                Ok(info)
            }
            None => client.access_share_page_for(&share_link, true).await,
        };

        match share_info_result {
            Ok(info) => {
                // 如果有密码，先验证密码
                if let Some(ref pwd) = password {
                    match client.verify_share_password_for(&info, pwd).await {
                        Ok(verified_randsk) => {
                            info!("提取码验证成功");
                            task.randsk = Some(verified_randsk);
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

                if task.randsk.is_none() {
                    task.randsk = request.randsk.clone();
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
                    // 🔥 显式传 effective_uid
                    // PersistenceManager 的 `owner_uid` 是 per-uid manager 自身的 uid
                    // （即 startup A），切账号或 handler 显式传 B 时 .meta 必须写 B。
                    if let Err(e) = pm_arc.lock().await.register_transfer_task(
                        task_id.clone(),
                        request.share_url.clone(),
                        password.clone(),
                        save_path.clone(),
                        auto_download,
                        None, // 文件名在获取文件列表后更新
                        Some(effective_uid.raw()),
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

                    // 分享同步内部任务（不分模式）：持久化同步配置归属，
                    // 供 get_all_tasks 历史段过滤，避免污染「转存管理」。
                    if let Some(ref cfg_id) = request.backup_config_id {
                        if let Err(e) = pm_arc
                            .lock()
                            .await
                            .update_transfer_backup_config_id(&task_id, Some(cfg_id.clone()))
                        {
                            warn!("更新转存任务同步归属失败: {}", e);
                        }
                    }
                }

                // 🔥 发送任务创建事件（带 effective_uid）
                self.publish_event(TransferEvent::Created {
                    task_id: task_id.clone(),
                    share_url: request.share_url.clone(),
                    save_path: save_path.clone(),
                    auto_download,

                    owner_uid: Some(effective_uid.raw()),
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
                if (err_msg.contains("需要密码") || err_msg.contains("need password"))
                    && password.is_none() {
                    return Ok(CreateTransferResponse {
                        task_id: None,
                        status: None,
                        need_password: true,
                        error: Some("需要提取码".to_string()),
                    });
                }
                // 有密码但可能是错误的，继续尝试验证

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
                // `{:#}` 展开完整 anyhow 链：这个串会直接写进任务状态给用户看，
                // 只取最外层 context 的话网络类失败全都长一个样。
                let error_msg = format!("{:#}", e);
                error!("转存任务执行失败: task_id={}, error={}", task_id, error_msg);

                // 更新任务状态为失败
                // 🔥 失败事件带 task.owner_uid
                let owner_uid_raw = if let Some(task_info) = tasks.get(&task_id) {
                    let mut task = task_info.task.write().await;
                    let uid = task.owner_uid.raw();
                    task.mark_transfer_failed(error_msg.clone());
                    uid
                } else {
                    0
                };

                // 🔥 发布失败事件
                if let Some(ref ws) = ws_manager {
                    ws.send_if_subscribed(
                        TaskEvent::Transfer(TransferEvent::Failed {
                            task_id: task_id.clone(),
                            error: error_msg.clone(),
                            error_type: "execution_error".to_string(),

                            owner_uid: Some(owner_uid_raw),
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

        // 🔥 execute_task 内所有 TransferEvent 一律带 task 的 owner_uid（按"任务事件都带 owner_uid"契约）。
        let owner_uid_raw = task.read().await.owner_uid.raw();

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

                    owner_uid: Some(owner_uid_raw),
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

        // 获取 randsk（由 create_task 从 verify_share_password 或调用方存入）
        let randsk = {
            let t = task.read().await;
            t.randsk.clone()
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
            t.selected_fs_ids.as_ref().is_some_and(|ids| !ids.is_empty())
        };

        // 根目录响应里的 uk/shareid 才是权威值：access_share_page 常常提取不到 uk
        // （日志里 `提取分享信息成功: shareid=..., uk=` 就是空的），而超限后下钻列子目录
        // 必须带正确的 uk，否则拿不到子项。
        let (file_list, share_root_path_from_api, root_uk, root_shareid): (
            Vec<SharedFileInfo>,
            Option<String>,
            String,
            String,
        ) = if has_selected_fs_ids {
            // 用户已选择文件，只拉第一页用于展示文件名
            let result = client
                .list_share_files_for(&share_info, 1, 100, randsk.as_deref())
                .await?;
            (result.files, result.share_root_path, result.uk, result.shareid)
        } else {
            // 全选模式，循环分页拉取全部
            let mut all_files = Vec::new();
            let mut share_root_path: Option<String> = None;
            let mut uk = String::new();
            let mut shareid = String::new();
            let page_size: u32 = 100;
            let mut page: u32 = 1;
            loop {
                let result = client
                    .list_share_files_for(&share_info, page, page_size, randsk.as_deref())
                    .await?;
                let batch_len = result.files.len();
                if page == 1 {
                    share_root_path = result.share_root_path;
                    uk = result.uk;
                    shareid = result.shareid;
                }
                all_files.extend(result.files);
                if (batch_len as u32) < page_size {
                    break;
                }
                page += 1;
            }
            (all_files, share_root_path, uk, shareid)
        };

        // 空值回退到分享页提取的值
        let list_uk = if root_uk.is_empty() {
            share_info.uk.clone()
        } else {
            root_uk
        };
        let list_shareid = if root_shareid.is_empty() {
            share_info.shareid.clone()
        } else {
            root_shareid
        };

        info!(
            "获取到 {} 个文件, share_root_path={:?}",
            file_list.len(),
            share_root_path_from_api
        );

        // 把分享根缓存到任务，供后续转存与自动下载阶段稳定推导 share_root
        if share_root_path_from_api.is_some() {
            {
                let mut t = task.write().await;
                t.share_root_path = share_root_path_from_api.clone();
            }
            // 同步持久化到 WAL 元数据，确保任务恢复 / 复用历史信息时仍能拿到权威分享根
            if let Some(ref pm_arc) = persistence_manager {
                let pm = pm_arc.lock().await;
                if let Err(e) =
                    pm.update_share_root_path(task_id, share_root_path_from_api.clone())
                {
                    warn!("持久化分享根路径失败: task_id={}, error={}", task_id, e);
                }
            }
        }

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

        // 逐项剥离虚拟根：前端可能对不同目录拿到不同的 uk，导致同一次选择里混进
        // `/sharelink0-<shareid>/` 与 `/sharelink<真实uk>-<shareid>/` 两种前缀。
        // 不在这里归一的话，`derive_share_root` 会因「前缀不统一」判不出虚拟根，
        // 转存目标凭空多出一层 `/sharelink…/`（实测日志里出现过两次）。
        let filtered_file_list: Vec<SharedFileInfo> = filtered_file_list
            .into_iter()
            .map(|mut f| {
                f.path = strip_virtual_share_root(&f.path);
                f
            })
            .collect();
        let virtual_roots_stripped = filtered_file_list
            .iter()
            .any(|f| !f.path.starts_with("/sharelink"));
        debug!(
            "选择集路径归一完成: {} 项, 已剥离虚拟根={}",
            filtered_file_list.len(),
            virtual_roots_stripped
        );

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

                    owner_uid: Some(owner_uid_raw),
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
            let parent_path = save_path.trim_end_matches('/');
            if let Some(parent) = parent_path.rsplit_once('/').map(|(p, _)| p) {
                if !parent.is_empty() {
                    ensure_dirs_exist(&client, parent).await?;
                }
            }

            // 再创建完整的临时目录（UUID子目录，一定是新的）
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

        // Referer 现在由各体系的 provider 按自己的分享页地址拼（个人版 /s/xxx、
        // 企业版 /apaas/share?surl=xxx），这里不再需要手工构造。

        // ========== 转存请求摘要日志 ==========
        {
            let t = task.read().await;
            let unique_count = {
                let set: std::collections::HashSet<u64> = fs_ids.iter().copied().collect();
                set.len()
            };
            let dup_count = fs_ids.len() - unique_count;

            // 🔥 统计同名文件（basename 维度）- 基于 filtered_file_list（真实选择集）
            let mut name_counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
            for f in &filtered_file_list {
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
                 filtered_file_list_count={}, fs_ids_count={}, unique_fs_ids={}, dup_fs_ids={}, dup_basenames={:?}",
                task_id,
                share_link.short_key,
                save_path,
                is_share_direct_download,
                selected_fs_ids.as_ref().map_or(0, |v| v.len()),
                t.selected_files.as_ref().map_or(0, |v| v.len()),
                filtered_file_list.len(),
                fs_ids.len(),
                unique_count,
                dup_count,
                dup_basenames,
            );

            // 🔥 诊断日志：真实选择集里的跨目录同名 basename 列表
            let mut basename_to_paths: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
            for f in &filtered_file_list {
                basename_to_paths.entry(f.name.as_str()).or_default().push(f.path.as_str());
            }
            let cross_dir_duplicates: Vec<(&str, Vec<&str>)> = basename_to_paths
                .into_iter()
                .filter(|(_, paths)| {
                    if paths.len() <= 1 {
                        return false;
                    }
                    // 检查是否跨目录：提取父目录并去重
                    let parent_dirs: std::collections::HashSet<_> = paths
                        .iter()
                        .filter_map(|p| p.rsplit_once('/').map(|(parent, _)| parent))
                        .collect();
                    parent_dirs.len() > 1
                })
                .collect();

            if !cross_dir_duplicates.is_empty() {
                warn!(
                    "🔍 诊断：真实选择集里的跨目录同名文件 (task_id={}, count={})",
                    task_id,
                    cross_dir_duplicates.len()
                );
                for (basename, paths) in cross_dir_duplicates.iter().take(10) {
                    warn!("  - basename='{}', paths={:?}", basename, paths);
                }
            } else {
                info!("🔍 诊断：真实选择集无跨目录同名文件 (task_id={})", task_id);
            }
        }

        // 百度单次转存有目标文件数上限（默认 500），且按「递归展开后的文件总数」
        // (`target_file_nums`) 判定，不是按提交的 fs_id 个数——选中一个含 800 个文件
        // 的目录只有 1 个 fs_id，照样撞 errno=12。
        //
        // 这里**不做**主动预扫：要知道一个目录底下有多少文件就得递归列目录，而绝大
        // 多数转存根本不到 500，为此让每次转存都先爬一遍分享树是不划算的（实测选中
        // 99 项里含目录时预扫要跑十分钟）。改为惰性——先按已知信息尽量拆，真撞上
        // 超限了再逐层下钻拆分**那一批**（见 `split_over_limit_batch`）。
        //
        // share-sync 那边同样有失败后二分（`executor.rs` 的 `share_sync_bisect_split`），
        // 这里用的是同一对拆分函数；它额外还做急切预拆，是因为它为了算 diff 本来就持有
        // 整棵树，`descendants_leaves()` 是纯内存操作、预拆免费，transfer 侧没这个前提。
        let file_limit = transfer_file_limit();

        // 超限下钻的上下文在整个任务内共享：
        // - 目录缓存让多个批次下钻到同一目录时不重复列（你提的「复用查询」）
        // - QuotaLimiter 让「列目录 + 转存提交」合计受同一个全局 RPS 约束，防 errno=132
        let dir_children_cache: Arc<Mutex<HashMap<String, Vec<SharedFileInfo>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let scan_rate_limiter = crate::share_sync::rate_limit::QuotaLimiter::from_env();

        // 进度条的分母。初值是选中项数，超限下钻把目录展开成子项后会随之增长
        // （否则出现「转存 34 / 共 8」这种分子大于分母的显示）。
        let mut effective_total_units = fs_ids.len();

        // ========== 转存策略：统一按原始父目录分组，保留完整目录结构 ==========
        let (transfer_result, batch_groups_info): (Result<TransferResult>, Option<Vec<BatchGroupInfo>>) = {
            let task_share_root_path = {
                let t = task.read().await;
                t.share_root_path.clone()
            };
            let share_root = derive_share_root(task_share_root_path.as_deref(), &filtered_file_list);
            // 先按父目录分组还原目录结构，再按上限把每组切成多批。
            //
            // 这一步是**零请求**的：只按选中项个数切，一个组里塞了 1200 个文件时
            // 必然超限，不用问百度也知道要拆。选中项是目录时无从判断（1 个 fs_id 底下
            // 可能有上万文件），那种情况留给下面的惰性兜底。
            let dir_groups = group_files_by_parent_dir(&filtered_file_list, &share_root);
            let dir_group_count = dir_groups.len();
            let groups = split_groups_by_file_limit(dir_groups, file_limit);
            let total_groups = groups.len();
            if total_groups > dir_group_count {
                info!(
                    "按单次转存上限切批: {} 个目录组 → {} 个批次, limit={}",
                    dir_group_count, total_groups, file_limit
                );
            }

            // 诊断日志：跨目录同名检测（仅用于日志）
            let cross_dir_dups = detect_cross_dir_duplicates(&filtered_file_list);
            if !cross_dir_dups.is_empty() {
                warn!(
                        "检测到 {} 个跨目录同名 basename: {:?}",
                        cross_dir_dups.len(),
                        cross_dir_dups.iter().take(10).collect::<Vec<_>>()
                    );
            }
            info!(
                    "按父目录分组转存: {} 个目录组, share_root={}, 每组文件数: {:?}",
                    total_groups,
                    share_root,
                    groups.iter().map(|(id, f)| format!("{}={}", if id.is_empty() { "<root>" } else { id }, f.len())).collect::<Vec<_>>()
                );

            let mut all_results: Vec<(usize, String, Vec<SharedFileInfo>, Result<TransferResult>)> = Vec::new();

            // 确保 save_path 本身存在（普通转存可能复用历史路径，路径被删后会导致 errno=2）
            // 分享直下模式下，临时目录已在上方正确创建，此处跳过。
            if !save_path.trim_end_matches('/').is_empty() && !is_share_direct_download {
                if let Err(e) = ensure_dirs_exist(&client, &save_path).await {
                    warn!("预建 save_path 目录失败: {}", e);
                }
            }

            for (batch_idx, (relative_parent, group_files)) in groups.into_iter().enumerate() {
                let batch_num = batch_idx + 1;

                // 检查取消
                if cancellation_token.is_cancelled() {
                    warn!("分批转存被取消: batch {}/{}", batch_num, total_groups);
                    break;
                }

                let group_target_dir = if relative_parent.is_empty() {
                    save_path.clone()
                } else {
                    format!("{}/{}", save_path.trim_end_matches('/'), relative_parent)
                };

                info!(
                        "转存批次 {}/{}: {} 个文件 -> {} (原始父目录={})",
                        batch_num, total_groups, group_files.len(), group_target_dir,
                        if relative_parent.is_empty() { "<root>" } else { &relative_parent }
                    );

                // 预建目标目录（百度转存 API 不会自动创建目标路径）
                if !relative_parent.is_empty() {
                    if let Err(e) = ensure_dirs_exist(&client, &group_target_dir).await {
                        warn!("预建批次目录失败（将在转存时重试）: {}, error={}", group_target_dir, e);
                    }
                }

                // 提取该组的 fs_ids
                let group_fs_ids: Vec<u64> = group_files.iter().map(|f| f.fs_id).collect();

                // 转存该组
                let result = client
                    .transfer_share_files_for(
                        &share_info,
                        &group_fs_ids,
                        &group_target_dir,
                        Some(task_id),
                        randsk.as_deref(),
                    )
                    .await;

                // errno=2 重试：逐级创建中间目录后重试一次
                let result = match &result {
                    Ok(r) if !r.success => {
                        let err_msg = r.error.as_deref().unwrap_or("");
                        if err_msg.contains("errno\":2") || err_msg.contains("路径不存在") {
                            warn!("批次 {} 路径不存在，逐级创建目录后重试: {}", batch_num, group_target_dir);
                            if let Err(e) = ensure_dirs_exist(&client, &group_target_dir).await {
                                warn!("重试时创建目录失败: {}", e);
                            }
                            client
                                .transfer_share_files_for(
                                    &share_info,
                                    &group_fs_ids,
                                    &group_target_dir,
                                    Some(task_id),
                                    randsk.as_deref(),
                                )
                                .await
                        } else {
                            result
                        }
                    }
                    _ => result,
                };

                // 临时错误退避重试：百度的 `errno=4「请求超时，请稍后再试」` 之类
                // 重试同一批就能过，不该让一次抖动废掉整批（实测批次 2/3 就是这么挂的）。
                // 注意与另两类区分：文件数超限要拆小、空间不足要停手，都不在这里重试。
                let mut result = result;
                let mut transient_attempt: u32 = 0;
                let max_transient = transfer_transient_retries();
                while transient_attempt < max_transient {
                    let should_retry = matches!(
                        &result,
                        Ok(r) if !r.success
                            && is_transient_transfer_error(r.error.as_deref().unwrap_or(""))
                    );
                    if !should_retry {
                        break;
                    }
                    transient_attempt += 1;
                    let backoff = Duration::from_millis(
                        transfer_transient_backoff_ms() * (1u64 << (transient_attempt - 1)),
                    );
                    warn!(
                        "批次 {}/{} 遇到临时错误，{:?} 后第 {}/{} 次重试: {}",
                        batch_num,
                        total_groups,
                        backoff,
                        transient_attempt,
                        max_transient,
                        result
                            .as_ref()
                            .ok()
                            .and_then(|r| r.error.as_deref())
                            .unwrap_or("")
                    );
                    tokio::time::sleep(backoff).await;
                    result = client
                        .transfer_share_files_for(
                            &share_info,
                            &group_fs_ids,
                            &group_target_dir,
                            Some(task_id),
                            randsk.as_deref(),
                        )
                        .await;
                }
                let result = result;

                // 「转存文件数超过上限」惰性兜底：只有真撞上了才去爬这一批的子树，
                // 用 share-sync 同一套 tree 精确拆分后重提。没超限的转存一路零额外请求。
                let result = match &result {
                    Ok(r)
                    if !r.success
                        && is_file_limit_exceeded(r.error.as_deref().unwrap_or("")) =>
                        {
                            warn!(
                            "批次 {}/{} 撞到转存文件数上限，开始抓取该批子树并拆分重提: {} 个条目 -> {}",
                            batch_num,
                            total_groups,
                            group_files.len(),
                            group_target_dir
                        );
                            // 失败的那次转存百度可能已按 ondup 建出改名的空壳目录，先清掉
                            cleanup_ondup_shells(&client, &group_target_dir, &group_files).await;

                            let dir_ctx = ShareDirCtx {
                                client: &client,
                                short_key: &share_info.short_key,
                                shareid: &list_shareid,
                                uk: &list_uk,
                                bdstoken: &share_info.bdstoken,
                                kind: share_info.kind,
                                randsk: randsk.as_deref(),
                                rate_limiter: Arc::clone(&scan_rate_limiter),
                                cache: Arc::clone(&dir_children_cache),
                            };
                            match split_over_limit_batch(
                                &dir_ctx,
                                &share_info,
                                group_files.clone(),
                                &group_target_dir,
                                task_id,
                                randsk.as_deref(),
                            )
                                .await
                            {
                                Ok((r, unit_total)) => {
                                    // 下钻把目录换成了子项，进度分母要跟着长，
                                    // 否则会显示成「34/8」这种转存数大于总数的样子
                                    effective_total_units = adjust_total_units(
                                        effective_total_units,
                                        group_files.len(),
                                        unit_total,
                                    );
                                    Ok(r)
                                }
                                Err(e) => Err(e),
                            }
                        }
                    _ => result,
                };

                let batch_ok = match &result {
                    Ok(r) => r.success,
                    Err(_) => false,
                };
                info!(
                        "批次 {}/{} 结果: success={}",
                        batch_num, total_groups, batch_ok
                    );

                all_results.push((batch_num, relative_parent, group_files, result));

                // 批次之间添加防抖延时
                if batch_num < total_groups {
                    tokio::time::sleep(Duration::from_millis(800)).await;
                }
            }

            // 取消检查：如果是因为取消而 break，不走 merge 成功分支
            if cancellation_token.is_cancelled() {
                warn!("分批转存已取消，跳过结果合并");
                (Err(anyhow::anyhow!("分批转存被用户取消")), None)
            } else {
                let (merged, groups_info) = merge_batch_results(all_results, &save_path);

                // 如果有部分失败警告，保存到内存状态并持久化
                if merged.success {
                    if let Some(ref warning) = merged.error {
                        {
                            let mut t = task.write().await;
                            t.error = Some(warning.clone());
                        }
                        // 持久化警告信息（不改变任务状态），确保重启后可见
                        if let Some(ref pm_arc) = persistence_manager {
                            let pm = pm_arc.lock().await;
                            if let Err(e) = pm.update_transfer_warning(task_id, warning.clone()) {
                                warn!("持久化分批转存警告失败: {}", e);
                            }
                        }
                    }
                }

                (Ok(merged), Some(groups_info))
            }
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

                                owner_uid: Some(owner_uid_raw),
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

                                owner_uid: Some(owner_uid_raw),
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
                // 分享直下模式下 save_path 是临时目录，不应写入 recent_save_path
                if !is_share_direct_download {
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
                    // 超限下钻展开过目录时，分母已经从「选中项数」长到「实际单元数」；
                    // 再兜一层 max，保证任何情况下分子都不会超过分母
                    t.total_count = effective_total_units.max(t.transferred_count);
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
                        batch_groups_info,
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

                                owner_uid: Some(owner_uid_raw),
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

                                owner_uid: Some(owner_uid_raw),
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

                                owner_uid: Some(owner_uid_raw),
                            }),
                            None,
                        );
                    }
                }
            }
            Err(e) => {
                // `{:#}` 展开完整 anyhow 链。下面 handle_transfer_error 是按
                // `task_errno=` 子串抽错误码的，链变长不影响抽取；而识别不出错误码时
                // 会原样透出这个串，此时多出来的底层原因（超时/连接重置）正是排障要的。
                let raw_err_msg = format!("{:#}", e);

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
                                None,
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

                                        owner_uid: Some(owner_uid_raw),
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

                                        owner_uid: Some(owner_uid_raw),
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

                                        owner_uid: Some(owner_uid_raw),
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

                                    owner_uid: Some(owner_uid_raw),
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

                                    owner_uid: Some(owner_uid_raw),
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
        save_path: String,
        cancellation_token: CancellationToken,
        is_share_direct_download: bool,
        batch_groups_info: Option<Vec<BatchGroupInfo>>,
    ) -> Result<()> {
        let dm_lock = download_manager.read().await;
        let dm = dm_lock.as_ref().context("下载管理器未设置")?;

        // 获取任务信息
        let task_info = tasks.get(task_id).context("任务不存在")?;
        let task = task_info.task.clone();
        drop(task_info);

        // 🔥 提前读 transfer task 的 owner_uid
        // - 让所有衍生下载（文件 + 文件夹）共用同一 effective_uid，与 transfer task 归属一致
        // - start_auto_download 内所有 TransferEvent 也用 owner_uid_raw（不再固定 None）
        let transfer_owner_uid = task.read().await.owner_uid;
        let owner_uid_raw = transfer_owner_uid.raw();

        // 分享同步内部任务：若带同步归属 id，则下载段统一走自动备份同款
        // `DownloadManager::create_backup_task`（is_backup=true → 从「下载管理」隐藏 +
        // 走自动备份下载槽优先级 + 以 "share-sync:{订阅id}" 归属到分享同步）。
        let backup_config_id = task.read().await.backup_config_id.clone();

        // 获取本地下载路径配置 + 缓存的分享根路径（用于 share_root 推导）
        let (local_download_path, ask_each_time, default_download_dir, task_share_root_path) = {
            let t = task.read().await;
            let local_path = t.local_download_path.clone();
            let share_root_path = t.share_root_path.clone();
            drop(t);

            let cfg = app_config.read().await;
            let ask = cfg.download.ask_each_time;
            let default_dir = cfg.download.download_dir.clone();
            (local_path, ask, default_dir, share_root_path)
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
        // 元组：(fs_id, remote_path, filename, size, local_dir)
        let mut download_files: Vec<(u64, String, String, u64, PathBuf)> = Vec::new();
        let mut download_folders: Vec<(String, PathBuf)> = Vec::new(); // (remote_path, local_dir)

        let is_batch = batch_groups_info.is_some();

        // 🔥 构建两级查找映射：
        //   1. path → SharedFileInfo：用原始分享路径精确匹配（无歧义，优先使用）
        //   2. (name, is_dir) → Vec<SharedFileInfo>：名称 + 类型匹配（多值，支持同名文件消歧）
        // 注意：transferred_fs_ids 是百度返回的转存后新 fs_id（to_fs_id），
        // 与 file_list 中的原始分享 fs_id 不同，无法直接用 fs_id 匹配。
        let file_info_by_path: std::collections::HashMap<&str, &SharedFileInfo> = file_list
            .iter()
            .map(|f| (f.path.as_str(), f))
            .collect();
        let mut file_info_by_name_dir: std::collections::HashMap<(&str, bool), Vec<&SharedFileInfo>> =
            std::collections::HashMap::new();
        for f in &file_list {
            file_info_by_name_dir
                .entry((f.name.as_str(), f.is_dir))
                .or_default()
                .push(f);
        }

        let save_prefix = save_path.trim_end_matches('/');
        let share_root = derive_share_root(task_share_root_path.as_deref(), &file_list);

        for (idx, transferred_path) in transfer_result.transferred_paths.iter().enumerate() {
            let transferred_fs_id = transfer_result.transferred_fs_ids.get(idx).copied();
            let from_path = transfer_result.from_paths.get(idx);
            let from_filename = from_path
                .map(|p| p.rsplit('/').next().unwrap_or(p).to_string());
            let to_filename = transferred_path.rsplit('/').next().unwrap_or(transferred_path);

            // transferred_path 相对于 save_path 的父目录（用于同名消歧）
            let transferred_relative_parent = if transferred_path.starts_with(save_prefix) {
                let relative = transferred_path[save_prefix.len()..].trim_start_matches('/');
                Path::new(relative).parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_default()
            } else {
                String::new()
            };

            // 匹配优先级：
            // 1. from_path 全路径精确匹配（最可靠，可区分同名文件）
            // 2. from_filename + is_dir 匹配，多候选时用 transferred_path 的父目录消歧
            // 3. to_filename + is_dir 匹配（百度可能重命名，最后手段）
            let file_info = from_path
                .and_then(|p| file_info_by_path.get(p.as_str()).copied())
                .or_else(|| {
                    let name = from_filename.as_deref().unwrap_or(to_filename);
                    Self::disambiguate_by_parent(
                        &file_info_by_name_dir, name, &transferred_relative_parent, &share_root,
                    )
                })
                .or_else(|| {
                    Self::disambiguate_by_parent(
                        &file_info_by_name_dir, to_filename, &transferred_relative_parent, &share_root,
                    )
                });

            // 按远端 transferred_path 相对于 save_path 的父目录来算 local_dir，
            // 保持本地目录结构与远端一致（普通转存和分享直下统一逻辑）。
            let local_dir = if transferred_path.starts_with(save_prefix) {
                let relative = transferred_path[save_prefix.len()..].trim_start_matches('/');
                match Path::new(relative).parent() {
                    Some(parent) if !parent.as_os_str().is_empty() => download_dir.join(parent),
                    _ => download_dir.clone(),
                }
            } else {
                warn!(
                    "transferred_path 不以 save_path 开头，回退到下载根目录: transferred_path={}, save_path={}",
                    transferred_path, save_path
                );
                download_dir.clone()
            };

            if let Some(file_info) = file_info {
                info!("匹配文件信息: idx={}, name={}, is_dir={}, transferred_fs_id={:?}",
                    idx, file_info.name, file_info.is_dir, transferred_fs_id);
                if file_info.is_dir {
                    // 文件夹：记录路径和本地目录
                    download_folders.push((transferred_path.clone(), local_dir));
                    info!("发现文件夹: {}", transferred_path);
                } else {
                    // 文件：记录下载信息，使用转存后的新 fs_id
                    download_files.push((
                        transferred_fs_id.unwrap_or(0),
                        transferred_path.clone(),
                        file_info.name.clone(),
                        file_info.size,
                        local_dir,
                    ));
                }
            } else {
                // 无法匹配到文件信息（可能是同名碰撞或分页未拉全）
                warn!("无法匹配文件信息: idx={}, path={}, from={:?}, to_filename={}",
                    idx, transferred_path, from_filename, to_filename);
                let fs_id = transferred_fs_id.unwrap_or(0);
                download_files.push((fs_id, transferred_path.clone(), to_filename.to_string(), 0, local_dir));
            }
        }

        info!(
            "分类完成: {} 个文件, {} 个文件夹, is_batch={}",
            download_files.len(),
            download_folders.len(),
            is_batch
        );

        // 创建文件下载任务
        //
        // 大量小文件场景下，逐个 `start_task().await` 会把"任务位分配/入队"
        // 串成一条长链。这里改为创建完成后立即并发投递启动请求；真正下载并发仍由
        // DownloadManager 的任务槽、ChunkScheduler 的全局线程预算控制。
        let mut download_task_ids = Vec::new();
        let mut ensured_local_dirs: HashSet<PathBuf> = HashSet::new();
        let mut start_join_set = tokio::task::JoinSet::new();
        let start_task_concurrency_limit = std::env::var(
            "BAIDUPCS_AUTO_DOWNLOAD_START_CONCURRENCY",
        )
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(128);
        for (fs_id, remote_path, filename, size, local_dir) in download_files {
            // 确保本地下载目录存在（分批模式下可能是按原始结构还原出的父目录）
            if ensured_local_dirs.insert(local_dir.clone()) && !local_dir.exists() {
                if let Err(e) = tokio::fs::create_dir_all(&local_dir).await {
                    warn!("创建本地下载目录失败: {:?}, error={}", local_dir, e);
                }
            }
            let create_result = if let Some(ref cfg_id) = backup_config_id {
                // 分享同步：下载段复用自动备份同款 create_backup_task
                // （is_backup=true → 从「下载管理」隐藏 + 走自动备份下载槽优先级 +
                //  以 "share-sync:{订阅id}" 归属到分享同步而非自动备份）。
                dm.create_backup_task(
                    fs_id,
                    remote_path.clone(),
                    local_dir.join(&filename),
                    size,
                    cfg_id.clone(),
                    None,
                    transfer_owner_uid,
                )
                    .await
            } else {
                dm.create_task_with_dir_and_owner(
                    fs_id,
                    remote_path.clone(),
                    filename.clone(),
                    size,
                    &local_dir,
                    None,
                    transfer_owner_uid,
                )
                    .await
            };
            match create_result {
                Ok(download_task_id) => {
                    // create_backup_task 命中冲突跳过（文件已存在）时返回 "skipped"
                    if download_task_id == "skipped" {
                        info!(
                            "share-sync: 备份下载跳过（文件已存在） remote={}",
                            remote_path
                        );
                        continue;
                    }
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
                    let dm_for_start = Arc::clone(dm);
                    let start_task_id = download_task_id.clone();
                    start_join_set.spawn(async move {
                        let result = dm_for_start
                            .start_task(&start_task_id)
                            .await
                            .map_err(|e| e.to_string());
                        (start_task_id, result)
                    });
                    if start_join_set.len() >= start_task_concurrency_limit {
                        match start_join_set.join_next().await {
                            Some(Ok((download_task_id, Ok(())))) => {
                                debug!("下载任务启动请求已投递: {}", download_task_id);
                            }
                            Some(Ok((download_task_id, Err(e)))) => {
                                warn!("启动下载任务失败: {}, error={}", download_task_id, e);
                            }
                            Some(Err(e)) => {
                                warn!("启动下载任务 join 失败: {}", e);
                            }
                            None => {}
                        }
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
        while let Some(joined) = start_join_set.join_next().await {
            match joined {
                Ok((download_task_id, Ok(()))) => {
                    debug!("下载任务启动请求已投递: {}", download_task_id);
                }
                Ok((download_task_id, Err(e))) => {
                    warn!("启动下载任务失败: {}, error={}", download_task_id, e);
                }
                Err(e) => {
                    warn!("启动下载任务 join 失败: {}", e);
                }
            }
        }

        // 🔥 衍生下载 owner_uid 走 transfer task
        // （此处复用循环前已读的 `transfer_owner_uid`，避免重复加锁）
        let owner_uid = transfer_owner_uid;
        // 释放下载管理器锁，避免后面持有两个锁
        drop(dm_lock);

        // 创建文件夹下载任务
        let mut folder_download_ids = Vec::new();
        if !download_folders.is_empty() {
            let fdm_lock = folder_download_manager.read().await;
            if let Some(ref fdm) = *fdm_lock {
                for (folder_path, local_dir) in download_folders {
                    // 确保本地目录存在
                    if ensured_local_dirs.insert(local_dir.clone()) && !local_dir.exists() {
                        if let Err(e) = tokio::fs::create_dir_all(&local_dir).await {
                            warn!("创建本地文件夹下载目录失败: {:?}, error={}", local_dir, e);
                        }
                    }
                    match fdm
                        // 🔥 分享同步内部任务：把 backup_config_id 透传给文件夹下载，
                        // 使其从「下载管理」隐藏并归属为分享同步子任务（与单文件下载段
                        // 走 create_backup_task 对齐）。
                        .create_folder_download_with_dir_backup(
                            folder_path.clone(),
                            &local_dir,
                            None,
                            None,
                            owner_uid,
                            backup_config_id.clone(),
                        )
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
            let temp_dir = {
                let mut t = task.write().await;
                t.mark_transferred(); // 标记为已转存，虽然没有文件需要下载
                t.temp_dir.clone()
            };

            // 分享直下：文件已转存进临时目录，但不会再有下载任务去读它（全部因本地已存在
            // 被跳过，或下载任务创建失败）。此时不清理，这个目录就再也没人管了（issue #162）。
            let mut cleanup_status = None;
            if is_share_direct_download {
                let (auto_cleanup, configured_root) = {
                    let cfg = app_config.read().await;
                    (
                        cfg.share_direct_download.auto_cleanup,
                        cfg.share_direct_download.temp_dir.clone(),
                    )
                };
                if auto_cleanup {
                    if let Some(ref temp_dir) = temp_dir {
                        info!("无下载任务，清理临时目录: task_id={}, temp_dir={}", task_id, temp_dir);
                        let client = _client.read().unwrap().clone();
                        let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
                        info!("无下载任务清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                        cleanup_status = Some(cleanup.status);
                    }
                }
            }

            // 无下载任务也要将转存状态标记为完成（持久化）
            if let Some(ref pm_arc) = persistence_manager {
                let pm = pm_arc.lock().await;

                if let Some(cs) = cleanup_status {
                    if let Err(e) = pm.update_cleanup_status(task_id, cs) {
                        warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                    }
                }

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

                    owner_uid: Some(owner_uid_raw),
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
            // 下载「停滞超时」：连续这么久零字节进展才判失败(取代旧的绝对 24h 超时)。
            let stall_timeout_secs = Self::download_stall_timeout_secs();
            let share_sync_download_failure_retry_max =
                Self::share_sync_download_failure_retry_max();
            let mut share_sync_download_failure_retry_attempts = 0u32;
            // 停滞检测的滚动状态:上次观察到的已下载字节总量 + 上次「有进展」的时刻。
            // 字节一旦增长就把时刻刷新为当前(等价于把超时计时器归零/顺延)。
            let mut stall_last_bytes: u64 = 0;
            let mut stall_last_progress_at: Option<i64> = None;

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

                // 🔥 本 loop 内所有 TransferEvent 用 task.owner_uid
                let (
                    status,
                    download_task_ids,
                    download_started_at,
                    owner_uid_raw,
                    is_internal,
                    backup_config_id,
                ) = {
                    let t = task.read().await;
                    (
                        t.status.clone(),
                        t.download_task_ids.clone(),
                        t.download_started_at,
                        t.owner_uid.raw(),
                        t.is_internal,
                        t.backup_config_id.clone(),
                    )
                };
                let is_share_sync_internal_download = is_internal
                    && backup_config_id
                    .as_deref()
                    .map(|id| id.starts_with("share-sync:"))
                    .unwrap_or(false);

                // 非下载中状态，停止监听
                if status != TransferStatus::Downloading {
                    break;
                }

                // 停滞超时检查
                //
                // 旧逻辑是「从下载开始算满 24h 就判失败」，会把慢但仍在下载的任务
                // (低带宽/被限速)误杀，还连带清临时目录导致其余在下文件报 31066。
                // 新逻辑:只要已下载字节仍在增长就把计时器归零；只有连续
                // `stall_timeout_secs` 零进展才判「停滞」。
                if download_started_at.is_some() {
                    let now = chrono::Utc::now().timestamp();
                    let (cur_bytes, any_active) = Self::aggregate_download_progress(
                        &download_manager,
                        &folder_download_manager,
                        &download_task_ids,
                    )
                        .await;
                    match stall_last_progress_at {
                        None => {
                            // 首次进入下载：初始化基准，不判超时
                            stall_last_bytes = cur_bytes;
                            stall_last_progress_at = Some(now);
                        }
                        Some(last_at) => {
                            // 判进展的两个信号：
                            // ① 没有活跃下载(全部暂停/无在下任务)—— 停滞时钟应暂停，
                            //    否则用户主动暂停的任务会被误判停滞；
                            // ② 字节总和「与上次不同」—— 用变动而非「超过历史峰值」，
                            //    因为已完成任务归档后被移出内存会让总和**下降**，重试
                            //    重置也会变动，这些都是任务仍在推进的表现。
                            // 只有「有活跃下载 且 字节持续完全不变」(真·0 KB/s 卡死)才判停滞。
                            if !any_active || cur_bytes != stall_last_bytes {
                                stall_last_bytes = cur_bytes;
                                stall_last_progress_at = Some(now);
                            } else if now - last_at > stall_timeout_secs {
                                let stalled_mins = (now - last_at) / 60;
                                warn!(
                                    "下载停滞超时: task_id={}, 连续 {} 分钟无字节进展(已下载 {} 字节)",
                                    task_id, stalled_mins, cur_bytes
                                );

                                // 获取分享直下相关信息
                                let (is_share_direct_download, temp_dir) = {
                                    let t = task.read().await;
                                    (t.is_share_direct_download, t.temp_dir.clone())
                                };

                                // 先取消底层下载 worker，避免它们继续对(即将被清理的)
                                // 临时目录刷 Locate、刷出满屏 31066。
                                Self::cancel_associated_downloads(
                                    &download_manager,
                                    &folder_download_manager,
                                    &download_task_ids,
                                )
                                    .await;

                                let old_status;
                                {
                                    let mut t = task.write().await;
                                    old_status = format!("{:?}", t.status).to_lowercase();
                                    t.status = TransferStatus::DownloadFailed;
                                    // 中性文案：只陈述现象 + 下一步，不猜测原因(会员/带宽等)
                                    t.error = Some(
                                        "下载长时间无进展，已判定为停滞并停止；可稍后重试".to_string(),
                                    );
                                    t.touch();
                                }

                                // 持久化失败状态
                                if let Some(ref pm_arc) = persistence_manager {
                                    if let Err(e) = pm_arc
                                        .lock()
                                        .await
                                        .update_transfer_status(&task_id, "download_failed")
                                    {
                                        warn!("持久化停滞失败状态失败: task_id={}, error={}", task_id, e);
                                    }
                                }

                                // 广播状态变更
                                if let Some(ref ws) = ws_manager {
                                    ws.send_if_subscribed(
                                        TaskEvent::Transfer(TransferEvent::StatusChanged {
                                            task_id: task_id.to_string(),
                                            old_status,
                                            new_status: "download_failed".to_string(),
                                            owner_uid: Some(owner_uid_raw),
                                        }),
                                        None,
                                    );
                                }

                                // 分享直下任务：清理临时目录(worker 已先行取消)
                                if is_share_direct_download {
                                    let (cleanup_on_failure, configured_root) = {
                                        let cfg = app_config.read().await;
                                        (
                                            cfg.share_direct_download.cleanup_on_failure,
                                            cfg.share_direct_download.temp_dir.clone(),
                                        )
                                    };
                                    if cleanup_on_failure {
                                        if let Some(ref temp_dir) = temp_dir {
                                            info!("下载停滞，触发临时目录清理: task_id={}, temp_dir={}", task_id, temp_dir);
                                            let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
                                            info!("下载停滞清理结果: task_id={}, status={:?}", task_id, cleanup.status);
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
                    }
                }

                if is_share_sync_internal_download
                    && share_sync_download_failure_retry_attempts
                    < share_sync_download_failure_retry_max
                {
                    let restarted = Self::restart_failed_downloads_once(
                        &download_manager,
                        &folder_download_manager,
                        &download_task_ids,
                    )
                        .await;
                    if restarted > 0 {
                        share_sync_download_failure_retry_attempts += 1;
                        warn!(
                            "share-sync: failed download subtasks resumed: task_id={}, attempt={}/{}, restarted={}",
                            task_id,
                            share_sync_download_failure_retry_attempts,
                            share_sync_download_failure_retry_max,
                            restarted
                        );
                        continue;
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

                                                owner_uid: Some(owner_uid_raw),
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

                                                owner_uid: Some(owner_uid_raw),
                                            }),
                                            None,
                                        );
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

                                                owner_uid: Some(owner_uid_raw),
                                            }),
                                            None,
                                        );
                                    }
                                }

                                // 🔥 移除分享直下的下载任务（已在完成时归档到历史库）。
                                // 调度器完成时特意把它们留在内存里等这里移除；无论是否自动清理
                                // 临时目录都要移除，否则关掉 auto_cleanup 时它们会永远留在内存里
                                // （「清除已完成」也会跳过分享直下任务）。
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

                                            owner_uid: Some(owner_uid_raw),
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

                                            owner_uid: Some(owner_uid_raw),
                                        }),
                                        None,
                                    );
                                }

                                // 下载子任务全部被取消（回退到 Transferred）：没有任何下载会再
                                // 读这个临时目录，与「取消转存任务」同口径按 cleanup_on_failure
                                // 清理（issue #162）。
                                //
                                // 持久化状态刻意不改成 transferred：那样重启后会按「已转存待下载」
                                // 恢复并对已删除的临时目录重新建下载；保持原状重启后只会再走一遍
                                // 这里（删除幂等，errno=12 视为成功）。
                                if new_status == TransferStatus::Transferred {
                                    let cleanup_on_failure = app_config
                                        .read()
                                        .await
                                        .share_direct_download
                                        .cleanup_on_failure;
                                    if cleanup_on_failure {
                                        if let Some(ref temp_dir) = temp_dir {
                                            info!("下载全部取消，清理临时目录: task_id={}, temp_dir={}", task_id, temp_dir);
                                            let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
                                            info!("下载取消清理结果: task_id={}, status={:?}", task_id, cleanup.status);
                                            if let Some(ref pm_arc) = persistence_manager {
                                                if let Err(e) = pm_arc.lock().await.update_cleanup_status(&task_id, cleanup.status) {
                                                    warn!("持久化清理状态失败: task_id={}, error={}", task_id, e);
                                                }
                                            }
                                        }
                                    }
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

                                    owner_uid: Some(owner_uid_raw),
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

    fn share_sync_download_failure_retry_max() -> u32 {
        std::env::var("BAIDUPCS_SHARE_SYNC_DOWNLOAD_FAILURE_RETRY_MAX")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(20)
    }

    /// 下载「停滞超时」阈值（秒）。
    ///
    /// 取代原先「从下载开始算满 24h 就一刀切判失败」的绝对超时 —— 那种做法会把
    /// 「慢但仍在下载」的任务(低带宽/被限速)误杀。改为:只有在**连续这么久没有
    /// 任何字节进展**时才判定停滞。正常下载即使很慢也会持续有字节增长，永远不会
    /// 触发；只有真正卡死(0 字节/长时间)才会被清掉，防卡死能力不丢。
    ///
    /// 默认 30 分钟。可用 `BAIDUPCS_DOWNLOAD_STALL_TIMEOUT_MINS` 覆盖(取值 >0)。
    fn download_stall_timeout_secs() -> i64 {
        let mins = std::env::var("BAIDUPCS_DOWNLOAD_STALL_TIMEOUT_MINS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(30);
        mins * 60
    }

    /// 汇总关联下载任务的进度信号，用于停滞检测。
    ///
    /// 返回 `(已下载字节总和, 是否有子任务正在下载中)`：
    /// - 字节总和:普通任务查 DownloadManager 内存态 `downloaded_size`，文件夹任务查
    ///   FolderDownloadManager `downloaded_size`；查不到的按 0 计。
    /// - 是否有活跃下载:任一子任务处于「下载中」(Downloading/Pending/Decrypting，
    ///   文件夹为 Downloading/Scanning)时为 `true`。**用户暂停**的子任务不算活跃 ——
    ///   停滞计时只应在真正尝试下载时累积，否则会把用户主动暂停的任务误判为停滞。
    async fn aggregate_download_progress(
        download_manager: &Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: &Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        download_task_ids: &[String],
    ) -> (u64, bool) {
        use crate::downloader::task::TaskStatus;
        let dm_lock = download_manager.read().await;
        let fdm_lock = folder_download_manager.read().await;
        let mut total: u64 = 0;
        let mut any_active = false;
        for task_id in download_task_ids {
            if let Some(folder_id) = task_id.strip_prefix("folder:") {
                if let Some(ref fdm) = *fdm_lock {
                    if let Some(folder) = fdm.get_folder(folder_id).await {
                        total = total.saturating_add(folder.downloaded_size);
                        if matches!(
                            folder.status,
                            FolderStatus::Downloading | FolderStatus::Scanning
                        ) {
                            any_active = true;
                        }
                    }
                }
            } else if let Some(ref dm) = *dm_lock {
                if let Some(t) = dm.get_task(task_id).await {
                    total = total.saturating_add(t.downloaded_size);
                    if matches!(
                        t.status,
                        TaskStatus::Downloading | TaskStatus::Pending | TaskStatus::Decrypting
                    ) {
                        any_active = true;
                    }
                }
            }
        }
        (total, any_active)
    }

    /// 停滞/超时判定成立时，取消所有关联下载 worker。
    ///
    /// 关键:仅标记转存任务失败、却不取消底层下载 worker 的话，worker 会继续对着
    /// (即将被清理的)临时目录刷 Locate、刷出满屏 `31066 file does not exist`。
    /// 这里在清理临时目录**之前**先把 worker 停掉。用 `without_delete` 变体保留
    /// 任务记录，便于前端/历史查看终态。
    async fn cancel_associated_downloads(
        download_manager: &Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: &Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        download_task_ids: &[String],
    ) {
        let dm_lock = download_manager.read().await;
        let fdm_lock = folder_download_manager.read().await;
        for task_id in download_task_ids {
            if let Some(folder_id) = task_id.strip_prefix("folder:") {
                if let Some(ref fdm) = *fdm_lock {
                    if let Err(e) = fdm.cancel_folder(folder_id, false).await {
                        warn!("停滞超时取消文件夹下载失败: folder_id={}, err={}", folder_id, e);
                    }
                }
            } else if let Some(ref dm) = *dm_lock {
                dm.cancel_task_without_delete(task_id).await;
            }
        }
    }

    async fn restart_failed_downloads_once(
        download_manager: &Arc<RwLock<Option<Arc<DownloadManager>>>>,
        folder_download_manager: &Arc<RwLock<Option<Arc<FolderDownloadManager>>>>,
        download_task_ids: &[String],
    ) -> usize {
        let dm = {
            let dm_lock = download_manager.read().await;
            dm_lock.as_ref().cloned()
        };
        let fdm = {
            let fdm_lock = folder_download_manager.read().await;
            fdm_lock.as_ref().cloned()
        };

        let mut restarted = 0usize;

        for task_id in download_task_ids {
            if let Some(folder_id) = task_id.strip_prefix("folder:") {
                let Some(folder_manager) = fdm.as_ref() else {
                    continue;
                };
                let should_resume = folder_manager
                    .get_folder(folder_id)
                    .await
                    .map(|folder| folder.status == FolderStatus::Failed)
                    .unwrap_or(false);
                if should_resume {
                    match folder_manager.resume_folder(folder_id).await {
                        Ok(()) => {
                            restarted += 1;
                            warn!(
                                "share-sync: failed folder download resumed: folder_id={}",
                                folder_id
                            );
                        }
                        Err(e) => warn!(
                            "share-sync: resume failed folder download failed: folder_id={}, error={}",
                            folder_id, e
                        ),
                    }
                }
                continue;
            }

            let Some(download_manager) = dm.as_ref() else {
                continue;
            };
            let should_resume = download_manager
                .get_task(task_id)
                .await
                .map(|task| task.status == TaskStatus::Failed)
                .unwrap_or(false);
            if should_resume {
                match download_manager.resume_task(task_id).await {
                    Ok(()) => {
                        restarted += 1;
                        warn!("share-sync: failed download task resumed: task_id={}", task_id);
                    }
                    Err(e) => warn!(
                        "share-sync: resume failed download task failed: task_id={}, error={}",
                        task_id, e
                    ),
                }
            }
        }

        restarted
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

        // 安全检查：路径必须是 {根}/{uuid} 或 {根}/inst-{id}/{uuid}，根本身必须安全。
        // 末级必须是 UUID，防止畸形路径把整个临时根 / 命名空间删掉。
        if let Err(reason) = temp_cleanup::validate_task_temp_dir(temp_dir, configured_temp_root) {
            error!(
                "临时目录路径不安全，跳过清理: path={}, configured_root={}, reason={}",
                temp_dir, configured_temp_root, reason
            );
            return CleanupResult {
                success: false,
                status: CleanupStatus::NotAttempted,
                error: Some(reason),
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
                                    widget.saferand.as_deref().unwrap_or(""),
                                    widget.safetpl.as_deref().unwrap_or(""),
                                    widget.safesign.as_deref().map(|s| s.len()).unwrap_or(0)
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

    /// 同名文件消歧：从多候选 SharedFileInfo 中，用精确的相对父目录匹配。
    ///
    /// 合并所有同名候选（不区分 is_dir），计算每个候选相对于 share_root 的父目录，
    /// 与 transferred_relative_parent 做精确相等比较（非 ends_with），避免：
    /// - `/root/a/b/7.mp4` 和 `/root/x/a/b/7.mp4` 因后缀相同而误配
    /// - 空 transferred_relative_parent 盲目回退到第一个候选
    /// - 文件/文件夹因 is_dir 固定顺序而错配
    fn disambiguate_by_parent<'a>(
        map: &std::collections::HashMap<(&str, bool), Vec<&'a SharedFileInfo>>,
        name: &str,
        transferred_relative_parent: &str,
        share_root: &str,
    ) -> Option<&'a SharedFileInfo> {
        // 合并所有同名候选（文件 + 文件夹）
        let mut all_candidates: Vec<&'a SharedFileInfo> = Vec::new();
        if let Some(files) = map.get(&(name, false)) {
            all_candidates.extend(files);
        }
        if let Some(dirs) = map.get(&(name, true)) {
            all_candidates.extend(dirs);
        }

        if all_candidates.is_empty() {
            return None;
        }
        if all_candidates.len() == 1 {
            return Some(all_candidates[0]);
        }

        // 多候选：计算每个候选的精确相对父目录，与 transferred_relative_parent 比较
        for c in &all_candidates {
            let original_parent = extract_parent_dir_str(&c.path);
            let candidate_relative = if !share_root.is_empty() && original_parent.starts_with(share_root) {
                original_parent[share_root.len()..].trim_start_matches('/')
            } else {
                original_parent.trim_start_matches('/')
            };
            if candidate_relative == transferred_relative_parent {
                return Some(c);
            }
        }

        // 消歧失败，回退到第一个候选并警告
        warn!(
            "同名消歧失败: name={}, transferred_parent={}, candidates={}",
            name, transferred_relative_parent, all_candidates.len()
        );
        Some(all_candidates[0])
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
        let (selected_files, selected_fs_ids, file_list, temp_dir, task_share_root_path) = {
            let t = task.read().await;
            let td = t.temp_dir.clone().filter(|s| !s.is_empty());
            (
                t.selected_files.clone(),
                t.selected_fs_ids.clone(),
                t.file_list.clone(),
                td,
                t.share_root_path.clone(),
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
        let has_selected_fs_ids = selected_fs_ids.as_ref().is_some_and(|ids| !ids.is_empty());

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

        info!("临时目录根级共有 {} 个文件/文件夹", existing_files.len());

        // ========== 扫描 group_* 子目录（分批转存支持） ==========
        let group_dirs: Vec<String> = existing_files
            .iter()
            .filter(|f| f.isdir == 1 && f.server_filename.starts_with("group_"))
            .map(|f| f.path.clone())
            .collect();

        if !group_dirs.is_empty() {
            info!(
                "检测到 {} 个 group_* 子目录，扫描子目录内容",
                group_dirs.len()
            );
            for group_dir in &group_dirs {
                let mut gpage = 1u32;
                loop {
                    match client.get_file_list(group_dir, gpage, page_size).await {
                        Ok(list) => {
                            let batch_len = list.list.len();
                            debug!(
                                "扫描组子目录 {} 第 {} 页: {} 个项目",
                                group_dir, gpage, batch_len
                            );
                            existing_files.extend(list.list);
                            if (batch_len as u32) < page_size {
                                break;
                            }
                            gpage += 1;
                        }
                        Err(e) => {
                            warn!("扫描组子目录失败: dir={}, error={}", group_dir, e);
                            break;
                        }
                    }
                }
            }
            info!(
                "含组子目录后共有 {} 个文件/文件夹",
                existing_files.len()
            );
        }

        // ========== 预计算 share_root（Phase 1/2 共用） ==========
        // 优先使用任务里持久化的分享根（来自 share/list?root=1 响应的 title 字段），
        // 与转存主链路保持一致；title 缺失时退化到最长公共父目录启发式（见 derive_share_root）。
        // 详见 docs/share-root-fix.md。
        let share_root = derive_share_root(task_share_root_path.as_deref(), &files_to_check);
        debug!(
            "推导的分享根目录: {:?} (title_available={})",
            share_root,
            task_share_root_path.is_some()
        );

        let temp_base = temp_dir.trim_end_matches('/');

        // ========== Phase 1: 路径优先 + 名称回退匹配 ==========
        // 构建 full_path → (fs_id, path) 映射（所有层级，含 group_* 子目录）
        let mut path_to_item: HashMap<&str, (Option<u64>, &str)> = HashMap::new();
        for file in &existing_files {
            let is_dir = file.isdir == 1;
            let fs_id = if is_dir { None } else { Some(file.fs_id) };
            path_to_item.insert(file.path.as_str(), (fs_id, file.path.as_str()));
        }

        // 构建 (文件名, is_dir) → (fs_id, path) 映射（仅根级，用于名称回退）
        let mut name_dir_to_item: HashMap<(String, bool), (Option<u64>, String)> = HashMap::new();
        for file in &existing_files {
            let is_dir = file.isdir == 1;
            let fs_id = if is_dir { None } else { Some(file.fs_id) };
            name_dir_to_item.insert(
                (file.server_filename.clone(), is_dir),
                (fs_id, file.path.clone()),
            );
        }

        // consumed_paths 防止同一个远端文件被多次匹配
        let mut consumed_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut recovered_items = Vec::new();

        // ---------- Pass 1: 严格路径匹配（不做名称回退） ----------
        // 先把所有能精确命中 expected_path 的文件锁定，避免名称回退抢占路径归属
        let mut pass1_remaining: Vec<&SharedFileInfo> = Vec::new();

        for file in &files_to_check {
            let relative = if !share_root.is_empty() && file.path.starts_with(&share_root) {
                file.path[share_root.len()..].trim_start_matches('/')
            } else {
                // share_root 为空（根目录+子目录混选）：
                // 用 file.path 去掉前导 / 保留完整目录结构，而非退化为 basename
                file.path.trim_start_matches('/')
            };

            // 先尝试 temp_base/<relative>（单次转存场景）
            let expected_path = format!("{}/{}", temp_base, relative);
            let mut matched = false;

            if let Some((fs_id, path)) = path_to_item.get(expected_path.as_str()) {
                if !consumed_paths.contains(*path) {
                    consumed_paths.insert(path.to_string());
                    recovered_items.push((file.name.clone(), *fs_id, Some(path.to_string()), file.path.clone()));
                    matched = true;
                }
            }

            // 再尝试 group_dir/<relative>（分批转存场景，文件在 temp_dir/group_N/ 下）
            if !matched {
                for gdir in &group_dirs {
                    let group_expected = format!("{}/{}", gdir.trim_end_matches('/'), relative);
                    if let Some((fs_id, path)) = path_to_item.get(group_expected.as_str()) {
                        if !consumed_paths.contains(*path) {
                            consumed_paths.insert(path.to_string());
                            recovered_items.push((file.name.clone(), *fs_id, Some(path.to_string()), file.path.clone()));
                            matched = true;
                            break;
                        }
                    }
                }
            }

            if !matched {
                pass1_remaining.push(file);
            }
        }

        let pass1_matched = recovered_items.len();
        debug!("Phase1 Pass1 (路径匹配): matched={}, remaining={}", pass1_matched, pass1_remaining.len());

        // ---------- Pass 2: 名称回退（仅对 Pass 1 未命中的文件） ----------
        let mut consumed_names: std::collections::HashSet<(String, bool)> = std::collections::HashSet::new();
        let mut unmatched_files: Vec<&SharedFileInfo> = Vec::new();

        for file in &pass1_remaining {
            let key = (file.name.clone(), file.is_dir);
            if !consumed_names.contains(&key) {
                if let Some((fs_id, path)) = name_dir_to_item.get(&key) {
                    if !consumed_paths.contains(path) {
                        consumed_names.insert(key);
                        consumed_paths.insert(path.clone());
                        recovered_items.push((file.name.clone(), *fs_id, Some(path.clone()), file.path.clone()));
                        continue;
                    }
                }
            }
            unmatched_files.push(file);
        }

        // Phase 1 匹配摘要
        info!(
            "恢复 Phase1 完成: task_id={}, files_to_check={}, existing_items={}, \
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

            let mut still_failed = Vec::new();

            // 按父目录缓存扫描结果，避免同一目录重复请求
            let mut dir_cache: HashMap<String, Vec<crate::netdisk::types::FileItem>> = HashMap::new();

            for file in &unmatched_files {
                // 从 SharedFileInfo.path 推导在 temp_dir 中的相对路径
                let relative = if !share_root.is_empty() && file.path.starts_with(&share_root) {
                    file.path[share_root.len()..].trim_start_matches('/')
                } else {
                    // share_root 为空时保留完整目录结构，与 Phase 1 一致
                    file.path.trim_start_matches('/')
                };

                let expected_path = format!("{}/{}", temp_base, relative);
                let expected_parent = expected_path
                    .rsplit_once('/')
                    .map_or(temp_base, |(p, _)| p);

                debug!(
                    "路径推导: name={}, share_path={}, expected={}, parent={}",
                    file.name, file.path, expected_path, expected_parent
                );

                // 内联辅助：扫描指定目录到缓存（如果尚未缓存）
                macro_rules! ensure_dir_cached {
                    ($dir:expr) => {
                        if !dir_cache.contains_key($dir) {
                            let mut all_items = Vec::new();
                            let ps: u32 = 1000;
                            let mut pg: u32 = 1;
                            let mut fetch_failed = false;
                            loop {
                                match client.get_file_list($dir, pg, ps).await {
                                    Ok(list) => {
                                        let n = list.list.len();
                                        all_items.extend(list.list);
                                        if (n as u32) < ps { break; }
                                        pg += 1;
                                    }
                                    Err(e) => {
                                        warn!("🔍 诊断：路径推导扫描 API 失败 (task_id={}, parent={}, page={}, error={})",
                                            recovery_task_id, $dir, pg, e);
                                        fetch_failed = true;
                                        break;
                                    }
                                }
                            }
                            if !fetch_failed {
                                if all_items.is_empty() {
                                    warn!("🔍 诊断：expected_parent 存在但为空 (task_id={}, parent={}, items=0)",
                                        recovery_task_id, $dir);
                                } else {
                                    let fc = all_items.iter().filter(|f| f.isdir == 0).count();
                                    let dc = all_items.iter().filter(|f| f.isdir == 1).count();
                                    let sample: Vec<&str> = all_items.iter().take(5).map(|f| f.server_filename.as_str()).collect();
                                    info!("🔍 诊断：expected_parent 存在 (task_id={}, parent={}, total={}, files={}, dirs={}, sample={:?})",
                                        recovery_task_id, $dir, all_items.len(), fc, dc, sample);
                                }
                            }
                            dir_cache.insert($dir.to_string(), all_items);
                        }
                    };
                }

                // 在缓存中按 name+is_dir 查找（排除已消费路径）
                let find_in_dir = |dir: &str, cache: &HashMap<String, Vec<crate::netdisk::types::FileItem>>, name: &str, is_dir: bool, consumed: &std::collections::HashSet<String>| -> Option<(Option<u64>, String)> {
                    cache.get(dir).and_then(|items| {
                        items.iter().find(|f| {
                            f.server_filename == name && (f.isdir == 1) == is_dir && !consumed.contains(&f.path)
                        }).map(|f| {
                            let fs_id = if is_dir { None } else { Some(f.fs_id) };
                            (fs_id, f.path.clone())
                        })
                    })
                };

                // 1. 尝试推导出的父目录（仅当不同于根目录时，根目录已在 Phase 1 扫过）
                let mut found: Option<(Option<u64>, String)> = None;
                if expected_parent != temp_base {
                    ensure_dir_cached!(expected_parent);
                    found = find_in_dir(expected_parent, &dir_cache, &file.name, file.is_dir, &consumed_paths);
                }

                // 2. 如果推导目录未命中，尝试每个 group_N 子目录（分批转存场景）
                if found.is_none() && !group_dirs.is_empty() {
                    for gdir in &group_dirs {
                        ensure_dir_cached!(gdir.as_str());
                        if let Some(result) = find_in_dir(gdir, &dir_cache, &file.name, file.is_dir, &consumed_paths) {
                            found = Some(result);
                            break;
                        }
                    }
                }

                if let Some((fs_id, ref path)) = found {
                    consumed_paths.insert(path.clone());
                    info!("路径推导匹配成功: name={}, path={}", file.name, path);
                    recovered_items.push((file.name.clone(), fs_id, Some(path.clone()), file.path.clone()));
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
                // 普通文件下载任务：查询 DownloadManager。
                // 🔥 小文件下载极快，完成瞬间就会「归档到历史库 + 从内存移除」。本监听器
                // 每 2s 才轮询一次，若仅查内存（get_task 返回 None）会把「已完成并归档」
                // 误判为「已取消」→ cancelled==total → 转存状态回退成 Transferred →
                // share-sync 把本已成功的子项标记为失败。改用 lookup_aggregate_outcome
                // 补查历史库，区分「已归档完成/失败」与「真正丢失」。
                use crate::downloader::manager::DownloadAggregateOutcome;
                match dm.lookup_aggregate_outcome(task_id).await {
                    DownloadAggregateOutcome::InMemory(status) => match status {
                        TaskStatus::Completed => completed_count += 1,
                        TaskStatus::Failed => failed_count += 1,
                        TaskStatus::Downloading => downloading_count += 1,
                        TaskStatus::Decrypting => downloading_count += 1, // 解密中视为进行中
                        TaskStatus::Paused => paused_count += 1,
                        TaskStatus::Pending => downloading_count += 1, // 视为进行中
                    },
                    DownloadAggregateOutcome::ArchivedCompleted => completed_count += 1,
                    DownloadAggregateOutcome::ArchivedFailed => failed_count += 1,
                    // 任务在内存与历史库均不存在，视为已取消
                    DownloadAggregateOutcome::NotFound => cancelled_count += 1,
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
    ///
    /// # 锁策略
    ///
    /// 此前用 `try_read()` 收集内存任务：如果任务正在状态流转持有写锁，
    /// 该任务会被静默跳过 → 调用方（如 `force=false` 删除前的运行任务扫描）
    /// 误判为无运行任务，进入静默强删。
    ///
    /// 这里改为先克隆 `(id, Arc<RwLock<TransferTask>>)` 再依次 `read().await`，
    /// 锁等待是瞬时的（写锁持续期短），但保证不漏任何任务。
    /// 只取内存中的活跃任务（不含历史库重建的副本）。
    ///
    /// 跨账号聚合时必须先收集这一份：历史库是全局共享的，非归属账号的 manager 也会
    /// 从里面捞到同一条任务并用 `convert_history_to_task` 重建，而那份重建副本的
    /// `transferred_count` 是按「全部成功」伪造的。谁先进去谁赢的去重会让伪造副本
    /// 盖掉真实进度（实测部分成功的任务被显示成 7/7）。
    pub async fn get_live_tasks(&self) -> Vec<TransferTask> {
        let task_arcs: Vec<Arc<RwLock<TransferTask>>> = self
            .tasks
            .iter()
            .map(|e| e.value().task.clone())
            .collect();

        let mut result = Vec::new();
        for task_arc in task_arcs {
            let task = task_arc.read().await;
            if task.is_internal {
                continue;
            }
            result.push(task.clone());
        }
        result
    }

    pub async fn get_all_tasks(&self) -> Vec<TransferTask> {
        let mut result = Vec::new();

        // 1) 收集内存中的任务 Arc（DashMap iter 不持久持锁，仅克隆 Arc）
        let task_arcs: Vec<Arc<RwLock<TransferTask>>> = self
            .tasks
            .iter()
            .map(|e| e.value().task.clone())
            .collect();

        // 2) 跨 .await 顺序读取，确保每个任务都被收集（不跳过写锁占用的）
        for task_arc in task_arcs {
            let task = task_arc.read().await;
            // 分享同步内部转存任务不在「转存管理」列表展示（对齐自动备份隔离）
            if task.is_internal {
                continue;
            }
            result.push(task.clone());
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
                    // 分享同步内部转存任务（带 share-sync: 归属）不在「转存管理」历史展示
                    if metadata
                        .backup_config_id
                        .as_deref()
                        .is_some_and(|c| c.starts_with("share-sync:"))
                    {
                        continue;
                    }
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
            error: metadata.error_msg.clone(),
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
            // 🔥 多账号：从 metadata 恢复 owner_uid，缺失为 Uid(0)（兜底）
            owner_uid: metadata.owner_uid.map(crate::auth::Uid::new).unwrap_or_default(),
            // 恢复分享根路径（老元数据缺该字段时为 None，调用方退化到启发式）
            share_root_path: metadata.share_root_path.clone(),
            // 内部标记不持久化于独立列：从 backup_config_id 是否带 share-sync: 前缀推断
            is_internal: metadata
                .backup_config_id
                .as_deref()
                .is_some_and(|c| c.starts_with("share-sync:")),
            backup_config_id: metadata.backup_config_id.clone(),
            randsk: None,
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

    /// 内存中归属指定同步配置的分享同步内部转存任务
    ///
    /// `get_all_tasks` 为了「转存管理」列表隐藏内部任务会把它们过滤掉，分享同步收集
    /// 子任务进度 / 残留时必须走这里，否则转存段永远是空的。
    pub async fn get_internal_tasks_by_backup_config(&self, backup_config_id: &str) -> Vec<TransferTask> {
        let task_arcs: Vec<Arc<RwLock<TransferTask>>> =
            self.tasks.iter().map(|e| e.value().task.clone()).collect();
        let mut result = Vec::new();
        for task_arc in task_arcs {
            let task = task_arc.read().await;
            if task.is_internal && task.backup_config_id.as_deref() == Some(backup_config_id) {
                result.push(task.clone());
            }
        }
        result
    }

    /// 内存中已到终态的分享同步内部转存任务（内存回收候选），附 `updated_at` 作版本标记
    ///
    /// 内部任务在「转存管理」里不可见，用户没法删除；除了失败重试时被丢弃、订阅被删
    /// 之外没有任何移除点，成功完成的会一直留在内存里。
    pub async fn finished_internal_task_candidates(&self) -> Vec<(String, i64)> {
        let task_arcs: Vec<(String, Arc<RwLock<TransferTask>>)> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.value().task.clone()))
            .collect();
        let mut ids = Vec::new();
        for (id, task_arc) in task_arcs {
            let task = task_arc.read().await;
            if task.is_internal && task.status.is_terminal() {
                ids.push((id, task.updated_at));
            }
        }
        ids
    }

    /// 把已到终态的分享同步内部转存任务移出内存
    ///
    /// 只移出内存：完成的已归档到历史库，不删持久化、不发删除事件（前端看不到内部任务）。
    /// 逐个复查状态，候选收集之后状态若有变化不会误删。
    pub async fn evict_finished_internal_tasks(&self, task_ids: &[String]) -> usize {
        let mut removed = 0usize;
        for id in task_ids {
            let task_arc = self.tasks.get(id).map(|e| e.value().task.clone());
            let Some(task_arc) = task_arc else {
                continue;
            };
            let evictable = {
                let task = task_arc.read().await;
                task.is_internal && task.status.is_terminal()
            };
            if evictable {
                if let Some((_, info)) = self.tasks.remove(id) {
                    info.cancellation_token.cancel();
                    removed += 1;
                }
            }
        }
        removed
    }

    /// 任务是否存在于**内存**中（不查历史库）。
    ///
    /// 用于跨账号路由的"内存优先"判定：内存命中即可确定归属为本 manager 的
    /// `owner_uid`（per-uid 独立 manager）。
    pub fn has_task_in_memory(&self, id: &str) -> bool {
        self.tasks.contains_key(id)
    }

    /// 任务是否存在于内存**或**持久化历史库中（与 `DownloadManager` /
    /// `UploadManager::has_task_anywhere` 语义对齐）。
    ///
    /// ⚠️ 注意：历史库为**全局共享**（所有账号同一张 `task_history` 表，
    /// 按 `task_id` 查），因此本方法对任意账号的历史任务都会返回 `true`，
    /// **不能**用于判定任务归属哪个账号。跨账号路由请用
    /// `AppState::find_transfer_manager_for_task`（内存优先 + 历史 `owner_uid`
    /// 精确路由）。
    pub async fn has_task_anywhere(&self, id: &str) -> bool {
        if self.tasks.contains_key(id) {
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
            if pm_guard.get_history_task(id).is_some() {
                return true;
            }
        }
        false
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
        // 🔥 cancel_task 各分支 TransferEvent 用 task.owner_uid
        let (current_status, is_share_direct_download, temp_dir, owner_uid_raw) = {
            let t = task.read().await;
            (t.status.clone(), t.is_share_direct_download, t.temp_dir.clone(), t.owner_uid.raw())
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

                    owner_uid: Some(owner_uid_raw),
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

                    owner_uid: Some(owner_uid_raw),
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

                    owner_uid: Some(owner_uid_raw),
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

                    owner_uid: Some(owner_uid_raw),
                }).await;

                info!("取消转存任务成功: task_id={}, old_status={:?}", id, current_status);
                Ok(())
            }
        }
    }

    /// 删除任务
    pub async fn remove_task(&self, id: &str) -> Result<()> {
        // 🔥 在 remove 前取出 task.owner_uid
        // 用于事件 owner_uid，避免事件不带归属导致前端按账号过滤失效。
        // 内存中找不到（已归档）时回退到 self.owner_uid（per-uid manager 的归属）。
        let owner_uid_raw: Option<u64> = if let Some((_, task_info)) = self.tasks.remove(id) {
            let uid_raw = task_info.task.read().await.owner_uid.raw();
            task_info.cancellation_token.cancel();
            info!("删除转存任务（内存中）: {}", id);
            Some(uid_raw)
        } else {
            // 不在内存中，仍然执行持久化清理，保证幂等
            info!("删除转存任务（历史/已归档）: {}", id);
            Some(self.owner_uid.raw())
        };

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

        // 🔥 发送删除事件（带 owner_uid，与 task / .meta 一致）
        self.publish_event(TransferEvent::Deleted {
            task_id: id.to_string(),

            owner_uid: owner_uid_raw,
        })
            .await;

        Ok(())
    }

    /// 删除指定账号下所有转存任务
    ///
    /// 用于 `force_delete_account` 链路：共享 `TransferManager` 设计下，
    /// 删除账号时必须取消并删除该 uid 归属的所有任务（运行中的取消、内存中的
    /// 移除、持久化的删除），否则任务在共享 manager 内继续跑，状态错乱。
    ///
    /// 行为：
    /// - 内存任务：找出 `task.owner_uid == uid` 的全部 → 取消 cancellation_token →
    ///   清理持久化（`.meta`）→ 从 `tasks` 移除 → 发送 `Deleted` 事件
    /// - 历史任务：从 sqlite `task_history` 按 `owner_uid` 删除
    ///
    /// 返回 `(memory_deleted, history_deleted)`。
    ///
    /// # 锁策略
    ///
    /// 此前用 `try_read()` 收集 task ids：如果某个 task 此时正在状态流转持有写锁，
    /// `try_read` 直接跳过 → 该任务被漏删 → `force_delete_account` 后续移除 uid
    /// 映射 + client_pool，但漏掉的 transfer 还在共享 manager 内继续跑/残留 .meta。
    /// 强删路径必须确定性收集，这里改用 `read().await`（短暂等待写锁释放）。
    pub async fn delete_tasks_for_owner(
        &self,
        uid: crate::auth::Uid,
    ) -> (usize, usize) {
        // 1) 收集内存中归属该 uid 的 task ids（确定性 read，避免 try_read 漏删）
        //    DashMap iter 的元素先克隆 task Arc，然后按 .await 顺序取读锁。
        //    这避免 cross-await 持有 DashMap 锁（容易死锁）。
        let task_arcs: Vec<(String, Arc<RwLock<TransferTask>>)> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.value().task.clone()))
            .collect();

        let mut target_ids: Vec<String> = Vec::new();
        for (id, task_arc) in task_arcs {
            let task = task_arc.read().await;
            if task.owner_uid == uid {
                target_ids.push(id);
            }
        }

        let memory_count = target_ids.len();
        info!(
            "delete_tasks_for_owner: uid={} 内存中找到 {} 个转存任务",
            uid.raw(),
            memory_count
        );

        // 2) 逐个删除（复用 remove_task 的取消 + 持久化清理 + 事件流程）
        for id in target_ids {
            if let Err(e) = self.remove_task(&id).await {
                warn!(
                    "delete_tasks_for_owner: 删除任务 {} 失败: {}",
                    id, e
                );
            }
        }

        // 3) 历史数据库：按 owner_uid 删除 transfer 类型的所有历史记录
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
                // 按 owner_uid 删除该账号所有 transfer 历史（无论状态）
                match db.remove_tasks_by_type_owner("transfer", Some(uid.raw())) {
                    Ok(count) => history_count = count,
                    Err(e) => warn!(
                        "delete_tasks_for_owner: 删除历史转存任务（owner_uid={}）失败: {}",
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

    /// 删除归属某 `backup_config_id`（如 `share-sync:{订阅id}`）的全部转存任务
    /// （内存运行中 + 历史），并连带清理其名下的下载子任务。
    ///
    /// 用于删除分享同步订阅时清掉内部转存/下载任务，避免订阅删除后残留孤儿脏数据。
    /// 返回 `(转存内存数, 转存历史数)`。
    pub async fn delete_tasks_for_backup_config(&self, cfg_id: &str) -> (usize, usize) {
        // 1) 收集内存中归属该 cfg_id 的 task ids（先克隆 Arc，再按 .await 顺序取读锁，
        //    避免 cross-await 持有 DashMap 锁）。
        let task_arcs: Vec<(String, Arc<RwLock<TransferTask>>)> = self
            .tasks
            .iter()
            .map(|e| (e.key().clone(), e.value().task.clone()))
            .collect();

        let mut target_ids: Vec<String> = Vec::new();
        // 移除前先记下需要善后的分享直下临时目录（移除后任务就查不到了）
        let mut abandoned_temp_dirs: Vec<String> = Vec::new();
        for (id, task_arc) in task_arcs {
            let task = task_arc.read().await;
            if task.backup_config_id.as_deref() == Some(cfg_id) {
                target_ids.push(id);
                abandoned_temp_dirs.extend(Self::abandoned_temp_dir(&task));
            }
        }

        let memory_count = target_ids.len();
        for id in target_ids {
            if let Err(e) = self.remove_task(&id).await {
                warn!(
                    "delete_tasks_for_backup_config: 删除转存任务 {} 失败: {}",
                    id, e
                );
            }
        }

        // 2) 历史数据库：删除该 backup_config_id 的全部转存历史。
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
                match db.remove_tasks_by_backup_config(cfg_id) {
                    Ok(count) => history_count = count,
                    Err(e) => warn!(
                        "delete_tasks_for_backup_config: 删除历史任务（cfg={}）失败: {}",
                        cfg_id, e
                    ),
                }
            }
        }

        // 3) 连带清理下载子任务（分享同步「转存并下载/分享直下」会建下载任务，
        //    同样带 backup_config_id = share-sync:{id}）。
        if let Some(dm) = self.download_manager_handle().await {
            let (dl_mem, dl_hist) = dm.delete_tasks_for_backup_config(cfg_id).await;
            info!(
                "delete_tasks_for_backup_config: cfg={} 下载子任务清理（内存={}, 历史={}）",
                cfg_id, dl_mem, dl_hist
            );
        }

        // 4) 连带清理 tree 模式整目录下载产生的内部隐藏文件夹下载任务
        //    （同样带 backup_config_id = share-sync:{id}）。
        if let Some(fdm) = self.folder_download_manager_handle().await {
            let folder_count = fdm.delete_folders_for_backup_config(cfg_id).await;
            info!(
                "delete_tasks_for_backup_config: cfg={} 文件夹子任务清理（{} 个）",
                cfg_id, folder_count
            );
        }

        // 5) 下载段收完后再删分享直下临时目录：先删目录的话，仍在跑的下载 worker
        //    会对已消失的文件刷满屏 31066。
        self.cleanup_abandoned_temp_dirs(&abandoned_temp_dirs).await;

        info!(
            "delete_tasks_for_backup_config: cfg={} 完成（转存内存={}, 转存历史={}）",
            cfg_id, memory_count, history_count
        );
        (memory_count, history_count)
    }

    /// 丢弃一个转存任务：取消并移除它本身，**连带清掉它派生的下载子任务**。
    ///
    /// 用于分享同步放弃某次提交后重新提交的场景（瞬时错误重试、配额二分、
    /// 「目标已存在同名」回退分享直下）。此前只是不再理会被放弃的 task_id：
    /// 它若已经走到下载阶段，派生出的下载子任务会一直留着，与重新提交那一支
    /// 叠成同一个文件两条子任务（issue #148）。
    ///
    /// 只清理任务记录，不删已下载的本地文件。返回 `(下载子任务数, 文件夹子任务数)`。
    pub async fn discard_task(&self, task_id: &str) -> (usize, usize) {
        // 🔥 顺序要紧：**先移除转存任务，再收下载段**。
        //
        // `remove_task` 会 cancel 该任务的 cancellation_token —— 它正是 `start_auto_download`
        // 这个「下载子任务生产者」的停止信号。反过来先清下载段的话，仍在跑的
        // start_auto_download 会在清理之后继续 `create_backup_task`，新建出来的那批
        // 就漏网了，等于没清。
        //
        // 生产者观察到取消需要一点时间，因此仍有极小的漏网窗口；兜底是每轮 run 开始
        // 前的 `sweep_residual_subtasks`（见 share_sync/manager.rs），下一轮会收干净。
        //
        // 分享直下临时目录（issue #162）：移除后监听循环随取消令牌退出、不再走任何清理
        // 分支，`.meta` 也被删掉，重启恢复同样找不到它 —— 必须在这里善后，否则每次
        // 重试 / 回退 / 判终态都会在网盘上留下一个装满文件的 {uuid} 目录。
        let task_arc = self.tasks.get(task_id).map(|e| e.value().task.clone());
        let abandoned_temp_dir = match task_arc {
            Some(arc) => Self::abandoned_temp_dir(&*arc.read().await),
            None => None,
        };

        if let Err(e) = self.remove_task(task_id).await {
            warn!("discard_task: 移除转存任务 {} 失败: {}", task_id, e);
        }

        let dl_count = match self.download_manager_handle().await {
            Some(dm) => dm.delete_tasks_for_transfer(task_id).await,
            None => 0,
        };
        let folder_count = match self.folder_download_manager_handle().await {
            Some(fdm) => fdm.delete_folders_for_transfer(task_id).await,
            None => 0,
        };

        // 下载段收完后再删临时目录（顺序原因同 delete_tasks_for_backup_config）
        if let Some(temp_dir) = abandoned_temp_dir {
            self.cleanup_abandoned_temp_dirs(&[temp_dir]).await;
        }

        if dl_count > 0 || folder_count > 0 {
            info!(
                "discard_task: task={} 已丢弃（下载子任务={}, 文件夹子任务={}）",
                task_id, dl_count, folder_count
            );
        }
        (dl_count, folder_count)
    }

    /// 被丢弃的转存任务需要善后删除的分享直下临时目录
    ///
    /// 只看**未到终态**的分享直下任务。到了终态（完成 / 转存失败 / 下载失败 / 已转存）
    /// 的任务在各自分支里已经按配置清理过，清理失败的交给孤儿清理兜底；这里再删一遍
    /// 只会对每个残留终态任务多发一次删除请求 —— 分享同步的内部任务会在内存里累积，
    /// 一轮 sweep 可能是几百次删除，容易触发 errno=132 风控。
    fn abandoned_temp_dir(task: &TransferTask) -> Option<String> {
        if !task.is_share_direct_download || task.status.is_terminal() {
            return None;
        }
        task.temp_dir.clone().filter(|s| !s.trim().is_empty())
    }

    /// 删除被丢弃任务遗留的分享直下临时目录
    ///
    /// 丢弃即放弃这次提交，与「取消转存任务」同口径受 `cleanup_on_failure` 控制。
    /// 逐个走 `cleanup_temp_dir_internal`（含路径安全校验、errno=12 幂等）。命中
    /// errno=132 风控时停止，剩下的交给孤儿清理，避免连续触发风控。
    async fn cleanup_abandoned_temp_dirs(&self, temp_dirs: &[String]) {
        if temp_dirs.is_empty() {
            return;
        }
        let (cleanup_on_failure, configured_root) = {
            let cfg = self.app_config.read().await;
            (
                cfg.share_direct_download.cleanup_on_failure,
                cfg.share_direct_download.temp_dir.clone(),
            )
        };
        if !cleanup_on_failure {
            info!(
                "cleanup_on_failure 已关闭，保留被丢弃任务的 {} 个临时目录",
                temp_dirs.len()
            );
            return;
        }
        let client = self.client.read().unwrap().clone();
        for (idx, temp_dir) in temp_dirs.iter().enumerate() {
            let cleanup = Self::cleanup_temp_dir_internal(&client, temp_dir, &configured_root).await;
            info!(
                "丢弃转存任务，清理临时目录: temp_dir={}, status={:?}",
                temp_dir, cleanup.status
            );
            if cleanup.status == CleanupStatus::RiskControlBlocked {
                warn!(
                    "清理临时目录被风控拦截，剩余 {} 个留给孤儿清理",
                    temp_dirs.len() - idx - 1
                );
                break;
            }
        }
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

        // 🔥 多账号 owner_uid 优先级 = recovery_info.owner_uid > self.owner_uid
        let resolved_owner_uid = recovery_info
            .owner_uid
            .map(crate::auth::Uid::new)
            .unwrap_or(self.owner_uid);

        // 创建恢复任务（多账号：链调 with_owner_uid 使用 resolved_owner_uid）
        let mut task = TransferTask::new(
            recovery_info.share_link.clone(),
            recovery_info.share_pwd.clone(),
            recovery_info.target_path.clone(),
            0,     // save_fs_id 未保存，设为 0
            false, // auto_download 稍后设置
            None,
        )
            .with_owner_uid(resolved_owner_uid);

        // 恢复任务 ID（保持原有 ID）
        task.id = task_id.clone();
        task.created_at = recovery_info.created_at;

        // 恢复分享根路径，避免恢复后退化到启发式推导
        task.share_root_path = recovery_info.share_root_path.clone();

        // 还原任务归属与内部标记：带 share-sync: 前缀的是分享同步内部转存任务，
        // 必须恢复 is_internal，否则重启后会漏进「转存管理」列表（与运行期隔离不一致）。
        task.backup_config_id = recovery_info.backup_config_id.clone();
        task.is_internal = recovery_info
            .backup_config_id
            .as_deref()
            .is_some_and(|c| c.starts_with("share-sync:"));

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
                        // 🔥 恢复后清理事件带 owner_uid
                        let owner_uid_raw = t.owner_uid.raw();
                        t.mark_completed();

                        // 发送状态变更事件
                        if let Some(ref ws) = ws_manager {
                            ws.send_if_subscribed(
                                TaskEvent::Transfer(TransferEvent::StatusChanged {
                                    task_id: task_id_clone.clone(),
                                    old_status,
                                    new_status: "completed".to_string(),

                                    owner_uid: Some(owner_uid_raw),
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

    /// 清理孤立的临时目录（手动 / 启动清理口径）
    ///
    /// 范围：旧版本平铺的 `{根}/{uuid}` 目录 + 本实例命名空间；其他实例的命名空间
    /// 不动（见 `cleanup_orphaned_temp_dirs_with`）。
    pub async fn cleanup_orphaned_temp_dirs(&self) -> CleanupOrphanedResult {
        self.cleanup_orphaned_temp_dirs_with(temp_cleanup::OrphanSweepOptions::manual(false))
            .await
    }

    /// 周期自动清理：只动本实例命名空间。
    ///
    /// 以下情况不执行，返回 None：
    /// - 实例 id 不可用（平铺布局，分不清哪些目录是本实例的）；
    /// - 用户关了 `auto_cleanup` 或 `cleanup_on_failure`：说明想保留临时目录里的文件，
    ///   自动清理不能替用户做主（手动 / 启动清理是用户显式操作，不受此限）。
    pub async fn sweep_own_namespace_orphans(&self) -> Option<CleanupOrphanedResult> {
        temp_cleanup::own_namespace()?;
        let keeps_files = {
            let cfg = self.app_config.read().await;
            !cfg.share_direct_download.auto_cleanup || !cfg.share_direct_download.cleanup_on_failure
        };
        if keeps_files {
            debug!("已关闭 auto_cleanup / cleanup_on_failure，跳过孤儿临时目录周期清理");
            return None;
        }
        Some(
            self.cleanup_orphaned_temp_dirs_with(temp_cleanup::OrphanSweepOptions::periodic())
                .await,
        )
    }

    /// 按指定范围清理孤立的分享直下临时目录（issue #162）
    ///
    /// 一个目录必须同时满足以下条件才会被删（见 `temp_cleanup` 模块说明）：
    /// 1. 本进程任务恢复已完成；
    /// 2. 目录名是 UUID，且位于本次清理范围内；
    /// 3. 创建时间超过 `opts.min_dir_age_secs`；
    /// 4. 不被任何仍在用的转存 / 下载 / 文件夹下载引用（内存 + 磁盘持久化）。
    ///
    /// 「先列目录、后收集白名单」：清理过程中新建的任务目录不会出现在候选里。
    /// 白名单任一来源读取失败都会中止清理 —— 宁可不删，也不在名单不全时误删。
    pub async fn cleanup_orphaned_temp_dirs_with(
        &self,
        opts: temp_cleanup::OrphanSweepOptions,
    ) -> CleanupOrphanedResult {
        let fail = |msg: String| CleanupOrphanedResult {
            error: Some(msg),
            ..Default::default()
        };

        if !temp_cleanup::recovery_done() {
            return fail("任务恢复尚未完成，请稍后再试".to_string());
        }
        let Ok(_sweep_guard) = self.orphan_sweep_lock.try_lock() else {
            info!("已有孤儿临时目录清理在进行中，跳过本次");
            return fail("已有清理任务在进行中，请稍后再试".to_string());
        };

        let temp_dir_base = {
            let cfg = self.app_config.read().await;
            cfg.share_direct_download.temp_dir.clone()
        };
        let root = match temp_cleanup::validate_temp_root(&temp_dir_base) {
            Ok(r) => r.to_string(),
            Err(e) => {
                error!("拒绝执行孤立目录清理: {}", e);
                return fail(e);
            }
        };
        let own_ns = temp_cleanup::own_namespace();
        info!(
            "开始清理孤立临时目录: root={}, own_namespace={:?}, opts={:?}",
            root, own_ns, opts
        );

        let client = self.client.read().unwrap().clone();

        // 1. 列出候选目录
        let root_entries = match list_all_dir_entries(&client, &root).await {
            Ok(Some(entries)) => entries,
            Ok(None) => {
                info!("临时目录不存在，无需清理: {}", root);
                return CleanupOrphanedResult::default();
            }
            Err(e) => {
                warn!("列出临时目录失败: {}", e);
                return fail(e);
            }
        };

        let mut candidates: Vec<(String, i64)> = Vec::new();
        let mut foreign_namespace_skipped = 0usize;
        for entry in root_entries.into_iter().filter(|e| e.isdir == 1) {
            use temp_cleanup::RootEntry;
            let scan_children = match temp_cleanup::classify_root_entry(
                &entry.server_filename,
                own_ns.as_deref(),
            ) {
                RootEntry::LegacyTask => {
                    if opts.include_legacy {
                        candidates.push((entry.path.clone(), entry.server_ctime));
                    }
                    false
                }
                RootEntry::OwnNamespace => opts.include_own_namespace,
                RootEntry::ForeignNamespace => {
                    if !opts.include_foreign_namespaces {
                        foreign_namespace_skipped += 1;
                    }
                    opts.include_foreign_namespaces
                }
                RootEntry::Other => false,
            };
            if !scan_children {
                continue;
            }
            match list_all_dir_entries(&client, &entry.path).await {
                Ok(Some(children)) => {
                    candidates.extend(
                        children
                            .into_iter()
                            .filter(|c| {
                                c.isdir == 1 && temp_cleanup::is_uuid_segment(&c.server_filename)
                            })
                            .map(|c| (c.path, c.server_ctime)),
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("列出命名空间目录失败: {}, error={}", entry.path, e);
                    return fail(e);
                }
            }
        }

        if candidates.is_empty() {
            info!("没有候选临时目录需要清理");
            return CleanupOrphanedResult {
                foreign_namespace_skipped,
                ..Default::default()
            };
        }

        // 2. 收集仍在用的路径（列目录之后再收集，见函数说明）
        let now = chrono::Utc::now().timestamp();
        let protected = match self.collect_protected_temp_paths(&root, now).await {
            Ok(set) => set,
            Err(e) => {
                warn!("收集在用任务失败，为避免误删已中止孤立目录清理: {}", e);
                return fail(format!("收集在用任务失败，已中止清理: {}", e));
            }
        };

        // 3. 过滤出孤立目录
        let orphans: Vec<String> = candidates
            .into_iter()
            .filter_map(|(path, ctime)| {
                let path = temp_cleanup::normalize_dir(&path).to_string();
                let deletable = !protected.contains(&path)
                    && temp_cleanup::old_enough(ctime, now, opts.min_dir_age_secs)
                    && temp_cleanup::validate_task_temp_dir(&path, &root).is_ok();
                deletable.then_some(path)
            })
            .collect();

        if orphans.is_empty() {
            info!("没有孤立目录需要清理");
            return CleanupOrphanedResult {
                foreign_namespace_skipped,
                ..Default::default()
            };
        }

        info!("发现 {} 个孤立目录，开始清理", orphans.len());

        // 4. 分批删除；命中风控即停，剩余留到下次
        const ORPHAN_DELETE_BATCH: usize = 100;
        let mut deleted_count = 0usize;
        let mut failed_paths: Vec<String> = Vec::new();
        let mut last_error: Option<String> = None;
        let batches: Vec<&[String]> = orphans.chunks(ORPHAN_DELETE_BATCH).collect();
        for (idx, batch) in batches.iter().enumerate() {
            match client.delete_files_chunked(batch).await {
                Ok(r) if r.success => deleted_count += batch.len(),
                Ok(r) => {
                    if r.failed_paths.is_empty() {
                        failed_paths.extend(batch.iter().cloned());
                    } else {
                        deleted_count += batch.len().saturating_sub(r.failed_paths.len());
                        failed_paths.extend(r.failed_paths);
                    }
                    if r.errno == Some(132) {
                        warn!("孤立目录清理被风控拦截（errno=132），剩余目录留到下次清理");
                        last_error =
                            Some("删除被百度风控拦截（errno=132），请稍后重试".to_string());
                        for rest in &batches[idx + 1..] {
                            failed_paths.extend(rest.iter().cloned());
                        }
                        break;
                    }
                    last_error = r.error.or(last_error);
                }
                Err(e) => {
                    error!("清理孤立目录失败: {}", e);
                    failed_paths.extend(batch.iter().cloned());
                    last_error = Some(e.to_string());
                }
            }
        }

        info!(
            "孤立目录清理完成: 删除={}, 失败={}",
            deleted_count,
            failed_paths.len()
        );
        CleanupOrphanedResult {
            deleted_count,
            failed_paths,
            error: last_error,
            foreign_namespace_skipped,
        }
    }

    /// 收集临时根目录下仍在用的路径（含每一级上级目录）
    ///
    /// 来源（任一读取失败都返回 Err，由调用方中止清理）：
    /// - 内存中的转存任务：未结束（`Transferred` 也算未结束，重启恢复会据此重建下载），
    ///   或结束未满 `TERMINAL_GRACE_SECS`；
    /// - 内存中未完成的下载任务（含分享同步的备份下载）与文件夹下载；
    /// - 磁盘上的 `.meta` 与文件夹持久化记录 —— 覆盖恢复失败 / 被跳过、或所属账号
    ///   manager 尚未构造的任务，它们不在内存里，但用户之后仍可继续。
    async fn collect_protected_temp_paths(
        &self,
        root: &str,
        now: i64,
    ) -> std::result::Result<HashSet<String>, String> {
        use crate::persistence::{TaskPersistenceStatus, TaskType};

        let mut protected: HashSet<String> = HashSet::new();

        // a) 内存中的转存任务
        let task_arcs: Vec<Arc<RwLock<TransferTask>>> =
            self.tasks.iter().map(|e| e.value().task.clone()).collect();
        for task_arc in task_arcs {
            let t = task_arc.read().await;
            let finished =
                temp_cleanup::is_finished_transfer_status(&format!("{:?}", t.status));
            if !temp_cleanup::transfer_protects_temp_dir(finished, t.updated_at, now) {
                continue;
            }
            if let Some(ref temp_dir) = t.temp_dir {
                temp_cleanup::protect_path(&mut protected, root, temp_dir);
            }
            temp_cleanup::protect_path(&mut protected, root, &t.save_path);
        }

        // b) 内存中的下载任务 / 文件夹下载
        if let Some(dm) = self.download_manager_handle().await {
            for path in dm.unfinished_remote_paths_under(root).await {
                temp_cleanup::protect_path(&mut protected, root, &path);
            }
        }
        if let Some(fdm) = self.folder_download_manager_handle().await {
            fdm.visit_folders(|f| {
                if !matches!(f.status, FolderStatus::Completed | FolderStatus::Cancelled) {
                    temp_cleanup::protect_path(&mut protected, root, &f.remote_root);
                }
            })
                .await;
        }

        // c) 磁盘持久化记录
        let pm_arc = self
            .persistence_manager
            .lock()
            .await
            .as_ref()
            .map(|pm| pm.clone())
            .ok_or_else(|| "持久化管理器未初始化".to_string())?;
        let wal_dir = pm_arc.lock().await.wal_dir().clone();
        let (metas, folders) = tokio::task::spawn_blocking(move || {
            let metas = crate::persistence::metadata::scan_all_metadata(&wal_dir)
                .map_err(|e| format!("扫描任务元数据失败: {}", e))?;
            let folders = crate::persistence::folder::load_all_folders(&wal_dir)
                .map_err(|e| format!("读取文件夹下载记录失败: {}", e))?;
            Ok::<_, String>((metas, folders))
        })
            .await
            .map_err(|e| format!("扫描持久化记录任务异常: {}", e))??;

        for meta in metas {
            match meta.task_type {
                TaskType::Transfer => {
                    let finished = meta
                        .transfer_status
                        .as_deref()
                        .map(temp_cleanup::is_finished_transfer_status)
                        .unwrap_or(false);
                    if !temp_cleanup::transfer_protects_temp_dir(
                        finished,
                        meta.updated_at.timestamp(),
                        now,
                    ) {
                        continue;
                    }
                    for path in [&meta.temp_dir, &meta.transfer_target_path]
                        .into_iter()
                        .flatten()
                    {
                        temp_cleanup::protect_path(&mut protected, root, path);
                    }
                }
                TaskType::Download => {
                    if meta.status == Some(TaskPersistenceStatus::Completed) {
                        continue;
                    }
                    if let Some(ref path) = meta.remote_path {
                        temp_cleanup::protect_path(&mut protected, root, path);
                    }
                }
                TaskType::Upload => {}
            }
        }
        for folder in folders {
            if !matches!(folder.status, FolderStatus::Completed | FolderStatus::Cancelled) {
                temp_cleanup::protect_path(&mut protected, root, &folder.remote_root);
            }
        }

        Ok(protected)
    }
}

/// 清理孤立目录的结果
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CleanupOrphanedResult {
    /// 成功删除的目录数
    pub deleted_count: usize,
    /// 删除失败的目录路径列表
    pub failed_paths: Vec<String>,
    /// 错误信息（如果有）
    pub error: Option<String>,
    /// 因属于其他实例而未清理的命名空间数（手动清理带 include_foreign=true 才会处理）
    pub foreign_namespace_skipped: usize,
}

/// 列出网盘目录下的全部条目（自动翻页）
///
/// 目录不存在时返回 `Ok(None)`。
async fn list_all_dir_entries(
    client: &NetdiskClient,
    dir: &str,
) -> std::result::Result<Option<Vec<crate::netdisk::FileItem>>, String> {
    const PAGE_SIZE: u32 = 1000;
    const MAX_PAGES: u32 = 200;

    let is_not_found_msg =
        |m: &str| m.contains("不存在") || m.contains("not found") || m.contains("-9");

    let mut all = Vec::new();
    for page in 1..=MAX_PAGES {
        match client.get_file_list(dir, page, PAGE_SIZE).await {
            Ok(resp) => {
                if resp.errno != 0 {
                    if resp.errno == -9 && page == 1 {
                        return Ok(None);
                    }
                    return Err(if resp.errmsg.is_empty() {
                        format!("API 错误码: {}", resp.errno)
                    } else {
                        resp.errmsg
                    });
                }
                let n = resp.list.len();
                all.extend(resp.list);
                if n < PAGE_SIZE as usize {
                    return Ok(Some(all));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if page == 1 && is_not_found_msg(&msg) {
                    return Ok(None);
                }
                return Err(msg);
            }
        }
    }
    Err(format!("目录条目过多（超过 {} 页），已中止: {}", MAX_PAGES, dir))
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

/// 逐级确保网盘路径存在，已存在的目录跳过创建，避免百度 API 静默重命名。
/// 一旦发现某层不存在，后续子目录直接创建不再检查（父不存在则子必不存在）。
/// 使用 get_file_list 直接对目标路径探测（page_size=1），避免列举父目录受条目数限制。
async fn ensure_dirs_exist(client: &NetdiskClient, path: &str) -> Result<()> {
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        return Ok(());
    }
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let mut cumulative = String::new();
    let mut parent_missing = false;
    for seg in &segments {
        cumulative.push('/');
        cumulative.push_str(seg);

        if !parent_missing {
            // 直接对目标路径调用 get_file_list：目录存在返回 Ok，不存在返回 Err
            let exists = client.get_file_list(&cumulative, 1, 1).await.is_ok();
            if exists {
                info!("目录已存在，跳过创建: {}", cumulative);
                continue;
            }
            parent_missing = true;
        }

        info!("创建目录: {}", cumulative);
        if let Err(e) = client.create_folder(&cumulative).await {
            warn!("创建目录失败: {} error={}", cumulative, e);
        }
    }
    Ok(())
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

/// 提取路径中的文件名部分
fn extract_basename_str(path: &str) -> &str {
    path.rsplit_once('/').map(|(_, name)| name).unwrap_or(path)
}

/// 提取路径中的父目录部分
fn extract_parent_dir_str(path: &str) -> &str {
    path.rsplit_once('/').map(|(parent, _)| parent).unwrap_or("/")
}

/// 检测跨目录同名文件
///
/// 返回存在跨目录同名的 basename 列表。
/// 如果返回为空，表示没有跨目录同名文件，可以使用单次转存。
fn detect_cross_dir_duplicates(files: &[SharedFileInfo]) -> Vec<String> {
    let mut basename_to_parents: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    for file in files {
        let basename = extract_basename_str(&file.path).to_string();
        let parent = extract_parent_dir_str(&file.path).to_string();
        basename_to_parents.entry(basename).or_default().insert(parent);
    }
    basename_to_parents
        .into_iter()
        .filter(|(_, parents)| parents.len() > 1)
        .map(|(basename, _)| basename)
        .collect()
}

/// 在缺少 API 权威分享根（`title` 字段）时，从文件路径反推 share_root 的兜底逻辑。
///
/// 取所有文件路径的最长公共父目录。例如：
/// - 单文件 `/a/b/c/file.mp4` → share_root = `/a/b/c`
/// - 多文件 `/root/抖音/1.jpg`, `/root/微信/2.jpg` → share_root = `/root`
/// - `/sharelink123/dir/file` → share_root = `/sharelink123/dir`
///
/// 注意：单凭文件路径无法稳定区分"分享者私有上层"和"分享根"，启发式在不同分享深度
/// 下都会失效。优先使用 `share/list?root=1` 响应中的 `title` 字段（参见
/// `derive_share_root` 与 `docs/share-root-fix.md`），仅在 title 缺失时回退到此函数。
fn infer_share_root_fallback(files: &[SharedFileInfo]) -> String {
    if files.is_empty() {
        return String::new();
    }
    let parents: Vec<&str> = files.iter().map(|f| extract_parent_dir_str(&f.path)).collect();
    let first_segs: Vec<&str> = parents[0].split('/').collect();
    let mut common_len = first_segs.len();
    for p in &parents[1..] {
        let segs: Vec<&str> = p.split('/').collect();
        common_len = common_len.min(segs.len());
        for i in 0..common_len {
            if first_segs[i] != segs[i] {
                common_len = i;
                break;
            }
        }
    }
    let common: String = first_segs[..common_len].join("/");
    if common.is_empty() || common == "/" {
        String::new()
    } else {
        common
    }
}

/// 判断分享根名是否为百度生成的虚拟根目录。
///
/// 当一次分享包含多个顶层文件/文件夹时，百度不会有真实的分享根目录，而是生成一个
/// 形如 `sharelink<uk>-<shareid>` 的虚拟根（例如 `sharelink1100862997704-6168417644`，
/// 前段是 uk、后段是 shareid），并把它作为 `share/list?root=1` 响应的 `title`，
/// 同时所有文件 `path` 都带上 `/sharelink<uk>-<shareid>/` 前缀。
///
/// 这个虚拟根不是真实目录，转存/下载时应整体剥离，否则会凭空多出一层
/// `/sharelink...` 前缀目录。
fn is_virtual_share_root(name: &str) -> bool {
    let rest = match name.trim_start_matches('/').strip_prefix("sharelink") {
        Some(rest) => rest,
        None => return false,
    };
    match rest.split_once('-') {
        Some((uk, shareid)) => {
            !uk.is_empty()
                && !shareid.is_empty()
                && uk.bytes().all(|b| b.is_ascii_digit())
                && shareid.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

/// 从文件路径中识别百度多文件分享的虚拟根前缀 `/sharelink<uk>-<shareid>`。
///
/// 百度在"多文件/多文件夹分享"以及子目录导航时，会用虚拟根
/// `sharelink<uk>-<shareid>` 作为文件 `path` 的顶层段——即使
/// `share/list?root=1` 的 `title` 给的是分享者的真实上层路径。此时 `title` 与文件
/// 路径处于不同命名空间，基于 title 推导的 share_root 无法匹配文件路径前缀，
/// 导致虚拟根被当成目录名转存出来（生成多余的 `/sharelink...` 目录）。
///
/// 若所有文件路径都位于同一个 `/sharelink<uk>-<shareid>/` 之下，返回该虚拟根
/// （不含尾部斜杠），否则返回 None。
fn detect_virtual_share_root_prefix(files: &[SharedFileInfo]) -> Option<String> {
    let first = files.first()?;
    let top = first.path.trim_start_matches('/').split('/').next().unwrap_or("");
    if !is_virtual_share_root(top) {
        return None;
    }
    let prefix = format!("/{}", top);
    let prefix_with_slash = format!("{}/", prefix);
    if files
        .iter()
        .all(|f| f.path == prefix || f.path.starts_with(&prefix_with_slash))
    {
        Some(prefix)
    } else {
        None
    }
}

/// 逐项剥离虚拟根前缀 `/sharelink<uk>-<shareid>`，把路径归一到「分享内」命名空间。
///
/// [`detect_virtual_share_root_prefix`] 要求**所有**路径共享同一个前缀才肯剥。但前端
/// 在分享内导航时，不同目录可能拿到不同的 uk（取不到时为 0），于是同一次选择里会混进
/// `/sharelink0-<shareid>/` 和 `/sharelink<真实uk>-<shareid>/` 两种前缀——整体剥离随即
/// 失效，`share_root` 退化为空，虚拟根被当成目录名转存出来（用户看到目标位置多出
/// 一层 `/sharelink…/`）。这里改为按项各剥各的，混合前缀也能归到同一命名空间。
///
/// 只剥虚拟根这一层；分享根目录本身（如 `/13`）仍保留，与 `derive_share_root` 的
/// 「分享根保留在 relative_parent 里」保持一致。
fn strip_virtual_share_root(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    let (top, rest) = match trimmed.split_once('/') {
        Some(pair) => pair,
        None => (trimmed, ""),
    };
    if !is_virtual_share_root(top) {
        return path.to_string();
    }
    if rest.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", rest)
    }
}

/// 推导用于剥离分享者私有上层目录的 share_root。
///
/// 优先使用百度 `share/list?root=1` 响应里的 `title` 字段（分享根的绝对路径），
/// 取其父目录即为分享者私有上层；title 缺失或为空时退化到
/// [`infer_share_root_fallback`] 的最长公共父目录启发式。
///
/// 特例：当文件路径位于虚拟根 `/sharelink<uk>-<shareid>/` 下时（多文件分享 / 子目录
/// 导航），title 可能是分享者真实路径，与文件路径命名空间不一致，无法用来匹配剥离；
/// 此时必须以文件路径为准，直接把虚拟根作为 share_root 剥掉（见
/// [`detect_virtual_share_root_prefix`]），分享根目录本身仍保留在 relative_parent 里。
fn derive_share_root(share_root_path: Option<&str>, files: &[SharedFileInfo]) -> String {
    // 最优先：文件路径带虚拟根前缀时以文件路径为准，因为 group_files_by_parent_dir
    // 是按文件 path 来剥离的，title 的命名空间可能与之不一致。
    if let Some(virtual_root) = detect_virtual_share_root_prefix(files) {
        return virtual_root;
    }
    if let Some(title) = share_root_path {
        if !title.is_empty() {
            // dirname(title)：分享根本身需要保留在 relative_parent 里，
            // 因此 share_root 取分享根的"父目录"，正好是要剥掉的私有部分。
            return extract_parent_dir_str(title).to_string();
        }
    }
    infer_share_root_fallback(files)
}

/// 按原始父目录分组文件，保留分享链接中的目录结构。
///
/// 每个组的 key 是相对于 share_root 的父目录路径（如 "抖音"、"微信"），
/// 同一父目录下的文件天然不会有同名冲突。
fn group_files_by_parent_dir(files: &[SharedFileInfo], share_root: &str) -> Vec<(String, Vec<SharedFileInfo>)> {
    let mut groups: HashMap<String, Vec<SharedFileInfo>> = HashMap::new();

    for file in files {
        let parent = extract_parent_dir_str(&file.path);
        let relative_parent = if !share_root.is_empty() && parent.starts_with(share_root) {
            parent[share_root.len()..].trim_start_matches('/').to_string()
        } else {
            parent.trim_start_matches('/').to_string()
        };
        groups.entry(relative_parent).or_default().push(file.clone());
    }

    let mut result: Vec<(String, Vec<SharedFileInfo>)> = groups.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

/// 百度单次转存的目标文件数上限，默认 500；实际值以超限时响应里的
/// `target_file_nums_limit` 为准，可用 `BAIDUPCS_TRANSFER_FILE_LIMIT` 覆盖。
/// 与 share-sync 的预拆批共用同一个环境变量，两边保持一致。
///
/// 注意：百度按「递归展开后的文件总数」(`target_file_nums`) 判定，不是按提交的
/// fs_id 个数——选中一个含 800 个文件的目录只提交 1 个 fs_id，同样会撞 errno=12。
const TRANSFER_FILE_LIMIT_DEFAULT: usize = 500;

fn transfer_file_limit() -> usize {
    std::env::var("BAIDUPCS_TRANSFER_FILE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(TRANSFER_FILE_LIMIT_DEFAULT)
}

/// 判断转存失败信息是否为「转存文件数超过上限」（对应 errno=12 + target_file_nums 超限）。
fn is_file_limit_exceeded(err: &str) -> bool {
    err.contains("转存文件数") && err.contains("超过上限")
}

/// 用某个批次「展开后的实际单元数」修正进度条分母。
///
/// 超限下钻会把 1 个目录换成 N 个子项，分母不跟着长就会出现「转存 34 / 共 8」
/// 这种分子大于分母的显示。展开只会让单元数变多，但仍用 saturating 运算兜底，
/// 避免日后改动引入下溢（usize 下溢会得到天文数字，比显示错更难查）。
fn adjust_total_units(current_total: usize, batch_before: usize, batch_after: usize) -> usize {
    current_total
        .saturating_sub(batch_before)
        .saturating_add(batch_after)
}

/// 判断转存失败信息是否为百度的**临时性**错误（超时 / 抖动 / 限流）。
///
/// 关键字与 `share_sync::error` 的 `TRANSIENT_KEYWORDS` 保持一致。典型是
/// `errno=4「请求超时，请稍后再试」`——重试同一批就能过，跟「文件数超限」
/// （必须拆小）和「空间不足」（拆多小都没用）是三码事。
fn is_transient_transfer_error(err: &str) -> bool {
    const TRANSIENT_KEYWORDS: &[&str] = &[
        "请求超时",
        "超时",
        "请稍后",
        "稍后再试",
        "timeout",
        "timed out",
        "temporarily",
        "temporary",
        "网络异常",
        "connection reset",
    ];
    TRANSIENT_KEYWORDS.iter().any(|k| err.contains(k))
}

/// 转存批次遇到临时错误时的重试次数与退避基准，可用环境变量覆盖
/// （与 share-sync 的 `BAIDUPCS_SHARE_SYNC_TRANSIENT_*` 对应）。
fn transfer_transient_retries() -> u32 {
    std::env::var("BAIDUPCS_TRANSFER_TRANSIENT_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

fn transfer_transient_backoff_ms() -> u64 {
    std::env::var("BAIDUPCS_TRANSFER_TRANSIENT_BACKOFF_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
}

/// 判断转存失败信息是否为「网盘空间不足」。
///
/// 关键字与 `share_sync::error` 的 `QUOTA_KEYWORDS` 保持一致。这类失败**重试没有
/// 意义**：拆多小都不会有空间。dfe1a0b 就是因为把它误判成文件数超限，一路对半拆到
/// 每批 1 个文件、刷了 347 次 errno=-32 才修掉，这里必须及早停手。
fn is_quota_exceeded(err: &str) -> bool {
    const QUOTA_KEYWORDS: &[&str] = &[
        "网盘空间不足",
        "空间不足",
        "errno=-32",
        "errno=-7",
        "errno=-12",
    ];
    QUOTA_KEYWORDS.iter().any(|k| err.contains(k))
}

/// 按单次转存文件数上限把每个父目录组切成多批。
///
/// **零请求**：只按选中项个数切。一个组里塞了 1200 个条目时必然超限，不用问百度；
/// 但选中项是目录时无从判断（1 个 fs_id 底下可能有上万文件），那种情况交给
/// `split_over_limit_batch` 惰性处理。
fn split_groups_by_file_limit(
    groups: Vec<(String, Vec<SharedFileInfo>)>,
    limit: usize,
) -> Vec<(String, Vec<SharedFileInfo>)> {
    let limit = limit.max(1);
    let mut out: Vec<(String, Vec<SharedFileInfo>)> = Vec::new();

    for (parent, files) in groups {
        if files.len() <= limit {
            out.push((parent, files));
            continue;
        }
        for chunk in files.chunks(limit) {
            out.push((parent.clone(), chunk.to_vec()));
        }
    }

    out
}

/// 列子目录时用的分享上下文（超限下钻专用）。
///
/// `shareid`/`uk` 刻意与 `SharePageInfo` 分开存：这里要用的是**根目录响应**
/// 里的权威值，access_share_page 拿到的那份经常缺 uk。
struct ShareDirCtx<'a> {
    client: &'a NetdiskClient,
    short_key: &'a str,
    shareid: &'a str,
    uk: &'a str,
    bdstoken: &'a str,
    /// 分享体系类型，决定列目录走哪套接口
    kind: crate::transfer::ShareKind,
    randsk: Option<&'a str>,
    rate_limiter: Arc<crate::share_sync::rate_limit::QuotaLimiter>,
    /// 目录 → 直接子项。同一个任务内多个批次下钻到同一目录时直接复用，不重复请求。
    cache: Arc<Mutex<HashMap<String, Vec<SharedFileInfo>>>>,
}

impl ShareDirCtx<'_> {
    /// 列出目录的**直接子项**（自动翻页，带退避重试）。
    ///
    /// 只要一层：超限下钻每次只需要把当前目录拆成子项，不需要整棵子树。
    /// 这是跟「全量抓快照」的关键差别——实测全量抓一次要十分钟，且中途任一请求
    /// 最终失败会让整批告吹。
    async fn list_children(&self, dir: &str) -> Result<Vec<SharedFileInfo>> {
        if let Some(hit) = self.cache.lock().await.get(dir).cloned() {
            debug!("超限下钻命中目录缓存: dir={}, {} 个子项", dir, hit.len());
            return Ok(hit);
        }

        const PAGE_SIZE: u32 = 100;
        const MAX_PAGES: u32 = 500;
        const MAX_RETRIES: u32 = 3;

        let mut all: Vec<SharedFileInfo> = Vec::new();
        let mut page: u32 = 1;
        loop {
            let mut attempt: u32 = 0;
            let batch = loop {
                // 与转存提交共用同一个令牌桶，避免列目录突发撞百度风控 errno=132
                self.rate_limiter.acquire().await;
                let dir_share_info = crate::transfer::SharePageInfo {
                    shareid: self.shareid.to_string(),
                    uk: self.uk.to_string(),
                    share_uk: self.uk.to_string(),
                    bdstoken: self.bdstoken.to_string(),
                    kind: self.kind,
                    short_key: self.short_key.to_string(),
                };
                match self
                    .client
                    .list_share_files_in_dir_for(
                        &dir_share_info,
                        dir,
                        page,
                        PAGE_SIZE,
                        self.randsk,
                    )
                    .await
                {
                    Ok(b) => break b,
                    Err(e) if attempt < MAX_RETRIES => {
                        attempt += 1;
                        let backoff = Duration::from_millis(800 * (1u64 << (attempt - 1)));
                        warn!(
                            "超限下钻列目录失败，{:?} 后第 {} 次重试: dir={}, page={}, error={:#}",
                            backoff, attempt, dir, page, e
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    Err(e) => return Err(e),
                }
            };

            let batch_len = batch.len() as u32;
            all.extend(batch);
            if batch_len < PAGE_SIZE {
                break;
            }
            page += 1;
            if page > MAX_PAGES {
                warn!("超限下钻翻页超过上限，子项可能不全: dir={}", dir);
                break;
            }
        }

        self.cache.lock().await.insert(dir.to_string(), all.clone());
        Ok(all)
    }
}

/// 撞到「转存文件数超限」后的惰性拆分：失败驱动，一次只下钻一层。
///
/// 这就是 share-sync `share_sync_bisect_split`（`executor.rs:1651`）的等价物：
/// - 一批有多项 → 对半拆（`split_indices_two` 的语义），**零请求**
/// - 只剩一个目录还超限 → 列它的**直接子项**当作新的一批（`split_two` 的语义），
///   目标目录追加该目录名以保持结构，只花 1 次请求
/// - 拆到单个文件仍失败 → 认输，如实报错
///
/// 之所以不先抓整棵子树：实测抓一次十分钟，且中途任一请求最终失败整批就废了。
/// share-sync 那边为了算 diff 本来就持有整棵树，所以它能免费预拆；transfer 没这个
/// 前提，只能按需下钻，代价与「真正出问题的节点数」成正比而不是与分享规模成正比。
#[allow(clippy::too_many_arguments)]
async fn split_over_limit_batch(
    ctx: &ShareDirCtx<'_>,
    share_info: &SharePageInfo,
    items: Vec<SharedFileInfo>,
    target_dir: &str,
    internal_task_id: &str,
    randsk: Option<&str>,
) -> Result<(TransferResult, usize)> {
    /// 下钻深度上限，与 share-sync 的 `BISECT_MAX_DEPTH` 对齐
    const MAX_DEPTH: u32 = 32;

    // 「已知单元数」——进度条的分母。下钻会把 1 个目录换成 N 个子项，
    // 分母必须跟着长，否则会出现「34/8」这种转存数大于总数的显示。
    // 对半拆不改变单元数，只有下钻才改变。
    let mut unit_total = items.len();

    let mut merged = TransferResult {
        success: false,
        transferred_paths: Vec::new(),
        from_paths: Vec::new(),
        transferred_fs_ids: Vec::new(),
        error: None,
    };
    let mut last_error: Option<String> = None;
    let mut submits = 0usize;
    let mut lists = 0usize;

    // (待提交项, 目标目录, 深度)
    let mut pending: Vec<(Vec<SharedFileInfo>, String, u32)> =
        vec![(items, target_dir.to_string(), 0)];

    while let Some((mut items, dir, depth)) = pending.pop() {
        if items.is_empty() {
            continue;
        }

        let fs_ids: Vec<u64> = items.iter().map(|f| f.fs_id).collect();
        submits += 1;
        let mut result = client_transfer(
            ctx.client,
            share_info,
            &fs_ids,
            &dir,
            internal_task_id,
            randsk,
        )
            .await?;

        // 临时错误（errno=4 超时等）退避重试同一组；拆小对它没用，重试才有用
        let mut transient_attempt: u32 = 0;
        while transient_attempt < transfer_transient_retries()
            && !result.success
            && is_transient_transfer_error(result.error.as_deref().unwrap_or(""))
        {
            transient_attempt += 1;
            let backoff = Duration::from_millis(
                transfer_transient_backoff_ms() * (1u64 << (transient_attempt - 1)),
            );
            warn!(
                "超限拆分子批次遇到临时错误，{:?} 后第 {} 次重试: {} 项 -> {}",
                backoff,
                transient_attempt,
                items.len(),
                dir
            );
            tokio::time::sleep(backoff).await;
            result = client_transfer(
                ctx.client,
                share_info,
                &fs_ids,
                &dir,
                internal_task_id,
                randsk,
            )
                .await?;
        }

        if result.success {
            merged.success = true;
            merged.transferred_paths.extend(result.transferred_paths);
            merged.from_paths.extend(result.from_paths);
            merged.transferred_fs_ids.extend(result.transferred_fs_ids);
            continue;
        }

        let err = result.error.unwrap_or_default();

        // 网盘空间不足：拆多小都不会有空间，继续提交纯属浪费且徒增风控风险。
        // 立刻停掉所有待处理分支，把剩下的算作未转存（实测一次这样的转存白提交了
        // 14 次，其中 5 次全是空间不足）。
        if is_quota_exceeded(&err) {
            let abandoned: usize = pending.iter().map(|(items, _, _)| items.len()).sum();
            warn!(
                "网盘空间不足，停止后续拆分与提交: {} 项 -> {}, 放弃剩余 {} 项, error={}",
                items.len(),
                dir,
                abandoned,
                err
            );
            last_error = Some(err);
            pending.clear();
            break;
        }

        if !is_file_limit_exceeded(&err) || depth >= MAX_DEPTH {
            warn!(
                "超限拆分子批次失败（不再下钻）: {} 项 -> {}, depth={}, error={}",
                items.len(),
                dir,
                depth,
                err
            );
            last_error = Some(err);
            continue;
        }

        if items.len() >= 2 {
            // 多项：对半拆，零请求
            let mid = items.len() / 2;
            let right = items.split_off(mid_index(&items, mid));
            info!(
                "超限拆分: {} 项 → {} + {}, dir={}, depth={}",
                right.len() + items.len(),
                items.len(),
                right.len(),
                dir,
                depth
            );
            pending.push((right, dir.clone(), depth + 1));
            pending.push((items, dir, depth + 1));
            continue;
        }

        // 单项：只有目录能继续拆，列出它的直接子项
        let only = &items[0];
        if !only.is_dir {
            warn!("单个文件仍报超限，无法继续拆分: path={}", only.path);
            last_error = Some(err);
            continue;
        }

        lists += 1;
        let children = match ctx.list_children(&only.path).await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "超限下钻列目录失败，该目录放弃: path={}, error={:#}",
                    only.path, e
                );
                last_error = Some(format!("{}（下钻列目录失败: {:#}）", err, e));
                continue;
            }
        };
        if children.is_empty() {
            warn!("目录报超限但列不出子项，放弃: path={}", only.path);
            last_error = Some(err);
            continue;
        }

        // 1 个目录展开成 N 个子项，已知单元数随之增长
        unit_total = unit_total + children.len() - 1;

        // 子项落到 <目标目录>/<该目录名>，保持原有目录结构
        let child_dir = format!("{}/{}", dir.trim_end_matches('/'), only.name);
        if let Err(e) = ensure_dirs_exist(ctx.client, &child_dir).await {
            warn!("下钻前预建目录失败（继续尝试）: {}, error={}", child_dir, e);
        }
        info!(
            "超限下钻: 目录 {} → {} 个子项 -> {}, depth={}",
            only.path,
            children.len(),
            child_dir,
            depth
        );
        pending.push((children, child_dir, depth + 1));
    }

    merged.error = last_error;
    info!(
        "超限拆分完成: 提交 {} 次, 列目录 {} 次, success={}, 转存 {}/{} 个单元",
        submits,
        lists,
        merged.success,
        merged.transferred_paths.len(),
        unit_total
    );
    Ok((merged, unit_total))
}

/// `items.split_off` 的下标兜底：保证两边都非空，避免 0 分割导致死循环。
fn mid_index(items: &[SharedFileInfo], mid: usize) -> usize {
    mid.clamp(1, items.len().saturating_sub(1).max(1))
}

/// 提交一次转存（把参数收拢，方便上面的拆分循环调用）。
async fn client_transfer(
    client: &NetdiskClient,
    share_info: &SharePageInfo,
    fs_ids: &[u64],
    target_dir: &str,
    internal_task_id: &str,
    randsk: Option<&str>,
) -> Result<TransferResult> {
    client
        .transfer_share_files_for(
            share_info,
            fs_ids,
            target_dir,
            Some(internal_task_id),
            randsk,
        )
        .await
}

/// 清理那次超限失败留下的空壳目录。
///
/// 转存超限失败时百度仍会按 ondup 把同名目标改名建出一个空目录（用户看到的
/// `name_<时间戳>` 残留）。拆批重提之前先删掉，否则重提又会撞同名再改一次名。
async fn cleanup_ondup_shells(
    client: &NetdiskClient,
    target_dir: &str,
    batch_files: &[SharedFileInfo],
) {
    let existing = match client.get_file_list(target_dir, 1, 1000).await {
        Ok(list) => list,
        Err(e) => {
            warn!("清理空壳目录前列目录失败，跳过清理: dir={}, error={}", target_dir, e);
            return;
        }
    };

    let expected: HashSet<&str> = batch_files.iter().map(|f| f.name.as_str()).collect();
    // 只删「名字是 <选中项名>_<数字后缀> 且为空目录」的，避免误伤用户已有文件
    let shells: Vec<String> = existing
        .list
        .iter()
        .filter(|item| item.isdir == 1)
        .filter(|item| {
            item.server_filename
                .rsplit_once('_')
                .is_some_and(|(stem, suffix)| {
                    expected.contains(stem) && suffix.chars().all(|c| c.is_ascii_digit())
                })
        })
        .map(|item| item.path.clone())
        .collect();

    if shells.is_empty() {
        return;
    }
    info!("清理超限失败留下的空壳目录: {} 个, {:?}", shells.len(), shells);
    if let Err(e) = client.delete_files(&shells).await {
        warn!("清理空壳目录失败（不影响后续拆批）: {}", e);
    }
}

/// 合并分批转存结果
///
/// 关键逻辑：至少一个批次成功就标记 success=true，继续自动下载流程。
fn merge_batch_results(
    results: Vec<(usize, String, Vec<SharedFileInfo>, Result<TransferResult>)>,
    temp_dir: &str,
) -> (TransferResult, Vec<BatchGroupInfo>) {
    let mut merged = TransferResult {
        success: false,
        transferred_paths: Vec::new(),
        from_paths: Vec::new(),
        transferred_fs_ids: Vec::new(),
        error: None,
    };

    let mut batch_groups_info = Vec::new();
    let mut failed_batches = Vec::new();
    let mut success_count = 0usize;
    // 批次内部「部分成功」的告警：整批标 success=true，但里面有子批次失败。
    // 超限拆分就是典型——12 项拆成多批，6 项成功、其余因空间不足失败，
    // 若丢掉这些信息，界面会显示「已转存」而用户看到进度条只走了一半。
    let mut partial_warnings: Vec<String> = Vec::new();

    for (batch_index, group_id, group_files, result) in results {
        match result {
            Ok(r) if r.success => {
                success_count += 1;
                if let Some(ref warn_msg) = r.error {
                    partial_warnings.push(format!("batch_{} ({}): {}", batch_index, group_id, warn_msg));
                }
                merged.transferred_paths.extend(r.transferred_paths.clone());
                merged.from_paths.extend(r.from_paths);
                merged.transferred_fs_ids.extend(r.transferred_fs_ids.clone());

                batch_groups_info.push(BatchGroupInfo {
                    group_id: group_id.clone(),
                    remote_dir: if group_id.is_empty() {
                        temp_dir.to_string()
                    } else {
                        format!("{}/{}", temp_dir.trim_end_matches('/'), group_id)
                    },
                    files: group_files,
                    transferred_paths: r.transferred_paths,
                    transferred_fs_ids: r.transferred_fs_ids,
                });
            }
            Ok(r) => {
                failed_batches.push((batch_index, group_id, r.error.unwrap_or_default()));
            }
            Err(e) => {
                // 用 `{:#}` 展开完整 anyhow 链。`{}`（即 to_string）只取最外层
                // context，网络类失败就只剩一句「转存请求失败」，看不出是超时、
                // 连接重置还是代理不通——排障只能去翻日志。
                // share_sync 那边早就是这么做的（见 snapshot.rs 的同款注释）。
                failed_batches.push((batch_index, group_id, format!("{:#}", e)));
            }
        }
    }

    if success_count > 0 {
        merged.success = true;
        let mut notes: Vec<String> = Vec::new();
        if !failed_batches.is_empty() {
            let failed_info: Vec<String> = failed_batches
                .iter()
                .map(|(idx, gid, err)| format!("batch_{} ({}): {}", idx, gid, err))
                .collect();
            notes.push(format!(
                "部分批次失败 ({}/{}): {}",
                failed_batches.len(),
                success_count + failed_batches.len(),
                failed_info.join("; ")
            ));
        }
        // 批次整体算成功、但内部有子批次失败（超限拆分的常见形态）也要如实报出来，
        // 否则界面显示「已转存」而实际只转了一部分。
        if !partial_warnings.is_empty() {
            notes.push(format!("部分文件未转存: {}", partial_warnings.join("; ")));
        }
        if !notes.is_empty() {
            merged.error = Some(notes.join("；"));
        }
    } else {
        let all_errors: Vec<String> = failed_batches
            .iter()
            .map(|(idx, gid, err)| format!("batch_{} ({}): {}", idx, gid, err))
            .collect();
        merged.error = Some(format!(
            "所有批次转存失败: {}",
            all_errors.join("; ")
        ));
    }

    info!(
        "分批转存结果汇总: total_batches={}, success={}, failed={}, total_files={}",
        success_count + failed_batches.len(),
        success_count,
        failed_batches.len(),
        merged.transferred_paths.len()
    );

    (merged, batch_groups_info)
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

    fn make_file(path: &str, fs_id: u64) -> SharedFileInfo {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        SharedFileInfo {
            fs_id,
            is_dir: false,
            path: path.to_string(),
            size: 100,
            name,
        }
    }

    // ========== extract helpers ==========

    #[test]
    fn test_extract_basename_str_nested() {
        assert_eq!(extract_basename_str("/a/b/c.jpg"), "c.jpg");
    }

    #[test]
    fn test_extract_basename_str_root_file() {
        assert_eq!(extract_basename_str("c.jpg"), "c.jpg");
    }

    #[test]
    fn test_extract_parent_dir_str_nested() {
        assert_eq!(extract_parent_dir_str("/a/b/c.jpg"), "/a/b");
    }

    #[test]
    fn test_extract_parent_dir_str_root_file() {
        assert_eq!(extract_parent_dir_str("c.jpg"), "/");
    }

    // ========== detect_cross_dir_duplicates ==========

    #[test]
    fn test_detect_no_duplicates() {
        let files = vec![
            make_file("/a/1.jpg", 1),
            make_file("/a/2.jpg", 2),
        ];
        let dups = detect_cross_dir_duplicates(&files);
        assert!(dups.is_empty(), "同目录不同名，不应检测到跨目录同名");
    }

    #[test]
    fn test_detect_same_dir_same_name() {
        let files = vec![
            make_file("/a/1.jpg", 1),
            make_file("/a/1.jpg", 2),
        ];
        let dups = detect_cross_dir_duplicates(&files);
        assert!(dups.is_empty(), "同目录同名，不应检测到跨目录同名");
    }

    #[test]
    fn test_detect_cross_dir_duplicates_basic() {
        let files = vec![
            make_file("/抖音/1.jpg", 1),
            make_file("/微信/1.jpg", 2),
        ];
        let dups = detect_cross_dir_duplicates(&files);
        assert!(dups.contains(&"1.jpg".to_string()));
    }

    #[test]
    fn test_detect_cross_dir_multiple() {
        let files = vec![
            make_file("/A/x.jpg", 1),
            make_file("/B/x.jpg", 2),
            make_file("/C/y.jpg", 3),
            make_file("/D/y.jpg", 4),
            make_file("/E/z.jpg", 5),
        ];
        let mut dups = detect_cross_dir_duplicates(&files);
        dups.sort();
        assert_eq!(dups, vec!["x.jpg".to_string(), "y.jpg".to_string()]);
    }

    #[test]
    fn test_detect_empty() {
        assert!(detect_cross_dir_duplicates(&[]).is_empty());
    }

    // ========== group_files_by_parent_dir ==========

    #[test]
    fn test_group_by_parent_same_dir_single_group() {
        let files = vec![
            make_file("/root/a/1.jpg", 1),
            make_file("/root/a/2.jpg", 2),
            make_file("/root/a/3.jpg", 3),
        ];
        let share_root = infer_share_root_fallback(&files);
        assert_eq!(share_root, "/root/a");
        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 1, "同目录文件应只有 1 个组");
        assert_eq!(groups[0].0, "");
        assert_eq!(groups[0].1.len(), 3);
    }

    #[test]
    fn test_group_by_parent_cross_dir_duplicates() {
        let files = vec![
            make_file("/root/抖音/1.jpg", 1),
            make_file("/root/微信/1.jpg", 2),
        ];
        let share_root = infer_share_root_fallback(&files);
        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 2, "不同父目录应分为 2 个组");
        // sorted by UTF-8 byte order: 微信 < 抖音
        let keys: Vec<&str> = groups.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"抖音"));
        assert!(keys.contains(&"微信"));
        assert_eq!(groups[0].1.len(), 1);
        assert_eq!(groups[1].1.len(), 1);
    }

    #[test]
    fn test_group_by_parent_mixed_dirs() {
        let files = vec![
            make_file("/root/A/1.jpg", 1),
            make_file("/root/B/1.jpg", 2),
            make_file("/root/A/2.jpg", 3),
            make_file("/root/C/3.jpg", 4),
        ];
        let share_root = infer_share_root_fallback(&files);
        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 3, "3 个不同父目录应分为 3 个组");
        let total: usize = groups.iter().map(|(_, g)| g.len()).sum();
        assert_eq!(total, 4, "所有文件都应被分配");
        // A 组应有 2 个文件
        let a_group = groups.iter().find(|(id, _)| id == "A").unwrap();
        assert_eq!(a_group.1.len(), 2);
    }

    #[test]
    fn test_group_by_parent_deeply_nested() {
        let files = vec![
            make_file("/root/a/b/file.jpg", 1),
            make_file("/root/c/d/file.jpg", 2),
        ];
        let share_root = infer_share_root_fallback(&files);
        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 2);
        // sorted: "a/b" < "c/d"
        assert_eq!(groups[0].0, "a/b");
        assert_eq!(groups[1].0, "c/d");
    }

    #[test]
    fn test_group_by_parent_empty_input() {
        let groups = group_files_by_parent_dir(&[], "");
        assert!(groups.is_empty());
    }

    #[test]
    fn test_infer_share_root_fallback_for_cross_dir_duplicates() {
        let files = vec![
            make_file("/root/dir_a/7.mp4", 1),
            make_file("/root/dir_b/7.mp4", 2),
        ];
        assert_eq!(infer_share_root_fallback(&files), "/root");
    }

    // ========== derive_share_root（基于 share/list title 字段） ==========

    /// Case A（issue #62 后续反馈）：分享者私有上层只有 1 段，
    /// 用户选中"纯净版"子目录下的文件，期望本地保留 `久别.../纯净版/` 两层。
    #[test]
    fn test_derive_share_root_from_title_case_a_pure_version() {
        let title = "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版";
        let files = vec![
            make_file(
                "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我跟妈妈1.mp4",
                1,
            ),
            make_file(
                "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我跟妈妈2.mp4",
                2,
            ),
        ];
        let share_root = derive_share_root(Some(title), &files);
        assert_eq!(share_root, "/爸爸，别再丢下我和妈妈");

        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, "久别不成悲18集包含纯净版/纯净版");
    }

    /// Case B（PR #78 修复目标）：分享者私有上层有多段，
    /// 用户选中分享根目录本身，期望 relative_parent 为空。
    #[test]
    fn test_derive_share_root_from_title_case_b_deep_private_path() {
        let title = "/纪录片群合集/整理清晰纪录片合集/01.BBC英国广播公司/K/BBC.恐龙行星.第1季";
        let files = vec![make_file(
            "/纪录片群合集/整理清晰纪录片合集/01.BBC英国广播公司/K/BBC.恐龙行星.第1季",
            1,
        )];
        let share_root = derive_share_root(Some(title), &files);
        assert_eq!(
            share_root,
            "/纪录片群合集/整理清晰纪录片合集/01.BBC英国广播公司/K"
        );

        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, "");
    }

    /// title 缺失（None / 空字符串）时退化到最长公共父目录启发式。
    #[test]
    fn test_derive_share_root_falls_back_when_title_missing() {
        let files = vec![
            make_file("/root/抖音/1.jpg", 1),
            make_file("/root/微信/2.jpg", 2),
        ];
        assert_eq!(derive_share_root(None, &files), "/root");
        assert_eq!(derive_share_root(Some(""), &files), "/root");
    }

    /// 回归：-30 冲突恢复路径必须用 derive_share_root，而不是文件最长公共父目录。
    /// 在 Case A（用户选纯净版子目录里的多个文件）下，最长公共父目录会把 "纯净版"
    /// 那一层吃掉，导致 expected_path 计算错误、临时目录回查无法命中。
    /// 用 title 推导后 share_root = dirname(title)，relative 才能保留 "纯净版/file" 两层。
    /// 详见 docs/share-root-fix.md。
    #[test]
    fn test_derive_share_root_matches_main_path_for_conflict_recovery_case_a() {
        let title = "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版";
        let files = vec![
            make_file(
                "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我跟妈妈1.mp4",
                1,
            ),
            make_file(
                "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我跟妈妈2.mp4",
                2,
            ),
        ];

        // 用户原意期望的 share_root（来自 title）
        let with_title = derive_share_root(Some(title), &files);
        assert_eq!(with_title, "/爸爸，别再丢下我和妈妈");

        // 旧 fallback 启发式（最长公共父目录）会错误地把"纯净版"也吃掉
        let fallback = derive_share_root(None, &files);
        assert_eq!(
            fallback,
            "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版/纯净版"
        );

        // 因此两种推导出的 relative 不同：title 路径保留 "纯净版/" 一层；fallback 不保留。
        let temp_base = "/.bpr_share_temp/uuid";
        let with_title_relative = files[0]
            .path
            .strip_prefix(&with_title)
            .unwrap()
            .trim_start_matches('/');
        let with_title_expected = format!("{}/{}", temp_base, with_title_relative);
        assert_eq!(
            with_title_expected,
            "/.bpr_share_temp/uuid/久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我跟妈妈1.mp4"
        );

        let fallback_relative = files[0]
            .path
            .strip_prefix(&fallback)
            .unwrap()
            .trim_start_matches('/');
        let fallback_expected = format!("{}/{}", temp_base, fallback_relative);
        // 旧路径会期望临时目录直接是 basename，与主转存的目录结构不一致
        assert_eq!(
            fallback_expected,
            "/.bpr_share_temp/uuid/爸爸，别再丢下我跟妈妈1.mp4"
        );
    }

    // ========== is_virtual_share_root / 多文件分享虚拟根 ==========

    #[test]
    fn test_is_virtual_share_root() {
        assert!(is_virtual_share_root("sharelink1100862997704-6168417644"));
        assert!(is_virtual_share_root("/sharelink1100862997704-6168417644"));
        // 真实文件夹名不应被误判
        assert!(!is_virtual_share_root("纯净版"));
        assert!(!is_virtual_share_root("sharelink"));
        assert!(!is_virtual_share_root("sharelink123"));
        assert!(!is_virtual_share_root("sharelink-123"));
        assert!(!is_virtual_share_root("sharelink123-"));
        assert!(!is_virtual_share_root("sharelinkabc-123"));
        assert!(!is_virtual_share_root("my-sharelink123-456"));
    }

    /// 多文件分享：title 为虚拟根 `sharelink<uk>-<shareid>`，文件 path 带该前缀。
    /// 期望整段虚拟根被剥离，不再生成 `/sharelink...` 前缀目录。
    #[test]
    fn test_derive_share_root_strips_virtual_sharelink_root() {
        let title = "sharelink1100862997704-6168417644";
        let files = vec![
            make_file("/sharelink1100862997704-6168417644/抖音/1.jpg", 1),
            make_file("/sharelink1100862997704-6168417644/微信/2.jpg", 2),
            make_file("/sharelink1100862997704-6168417644/top.mp4", 3),
        ];
        let share_root = derive_share_root(Some(title), &files);
        assert_eq!(share_root, "/sharelink1100862997704-6168417644");

        let groups = group_files_by_parent_dir(&files, &share_root);
        let keys: Vec<&str> = groups.iter().map(|(k, _)| k.as_str()).collect();
        // 顶层文件落到根（""），子目录保留真实结构，均不含 sharelink 前缀
        assert!(keys.contains(&""));
        assert!(keys.contains(&"抖音"));
        assert!(keys.contains(&"微信"));
        assert!(
            keys.iter().all(|k| !k.contains("sharelink")),
            "relative_parent 不应再包含虚拟根: {:?}",
            keys
        );
    }

    /// title 带前导斜杠的虚拟根也应正确剥离。
    #[test]
    fn test_derive_share_root_strips_virtual_sharelink_root_with_slash() {
        let title = "/sharelink1100862997704-6168417644";
        let files = vec![make_file(
            "/sharelink1100862997704-6168417644/影视/a.mkv",
            1,
        )];
        let share_root = derive_share_root(Some(title), &files);
        assert_eq!(share_root, "/sharelink1100862997704-6168417644");

        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, "影视");
    }

    /// 复现真实日志场景：`title` 是分享者真实路径，但文件 `path` 用虚拟根
    /// `/sharelink<uk>-<shareid>/` 命名空间（子目录导航时百度如此返回）。
    /// 此前 share_root = dirname(title) 与文件路径不在同一命名空间，剥离失败，
    /// 转存出多余的 `/sharelink...` 目录。修复后应以文件路径的虚拟根为准。
    #[test]
    fn test_derive_share_root_virtual_prefix_with_real_title() {
        // 来自用户日志：title=/爸爸.../久别...，文件 path 带 /sharelink1102557053903-51347437953/
        let title = "/爸爸，别再丢下我和妈妈/久别不成悲18集包含纯净版";
        let files = vec![make_file(
            "/sharelink1102557053903-51347437953/久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我和妈妈/抖音/1.mp4",
            1,
        )];
        let share_root = derive_share_root(Some(title), &files);
        assert_eq!(share_root, "/sharelink1102557053903-51347437953");

        let groups = group_files_by_parent_dir(&files, &share_root);
        assert_eq!(groups.len(), 1);
        // 分享根目录本身（久别...）保留，sharelink 虚拟根被剥离
        assert_eq!(
            groups[0].0,
            "久别不成悲18集包含纯净版/纯净版/爸爸，别再丢下我和妈妈/抖音"
        );
        assert!(!groups[0].0.contains("sharelink"));
    }

    // ========== 虚拟根逐项剥离 ==========

    #[test]
    fn test_strip_virtual_share_root_basic() {
        assert_eq!(
            strip_virtual_share_root("/sharelink0-37559844790/13/玉溪资料"),
            "/13/玉溪资料"
        );
        assert_eq!(
            strip_virtual_share_root("/sharelink3745347292-37559844790/13/张工"),
            "/13/张工"
        );
    }

    /// 非虚拟根路径原样返回，不能误伤真实目录名。
    #[test]
    fn test_strip_virtual_share_root_leaves_normal_paths() {
        assert_eq!(strip_virtual_share_root("/13/玉溪资料"), "/13/玉溪资料");
        assert_eq!(strip_virtual_share_root("/a.mp4"), "/a.mp4");
        // 名字里带 sharelink 但格式不符（uk/shareid 非纯数字）不算虚拟根
        assert_eq!(
            strip_virtual_share_root("/sharelink-abc/x"),
            "/sharelink-abc/x"
        );
        assert_eq!(strip_virtual_share_root("/sharelinkfoo/x"), "/sharelinkfoo/x");
    }

    /// 路径就是虚拟根本身时剥成根，不产生空路径。
    #[test]
    fn test_strip_virtual_share_root_bare_root() {
        assert_eq!(strip_virtual_share_root("/sharelink0-123"), "/");
    }

    /// 复现日志里的真实场景：前端对不同目录拿到了不同的 uk（0 与真实 uk），
    /// 整体剥离（detect_virtual_share_root_prefix）会因前缀不统一而失效，
    /// 逐项剥离后两组必须归到同一个相对父目录，转存目标不再多出 sharelink 一层。
    #[test]
    fn test_mixed_virtual_roots_normalize_to_one_namespace() {
        let raw = vec![
            make_file("/sharelink0-37559844790/13/玉溪资料2", 1),
            make_file("/sharelink3745347292-37559844790/13/张工", 2),
        ];
        // 混合前缀时整体识别确实失效——这正是逐项剥离要解决的
        assert!(detect_virtual_share_root_prefix(&raw).is_none());

        let normalized: Vec<SharedFileInfo> = raw
            .into_iter()
            .map(|mut f| {
                f.path = strip_virtual_share_root(&f.path);
                f
            })
            .collect();

        let share_root = derive_share_root(Some("/13"), &normalized);
        let groups = group_files_by_parent_dir(&normalized, &share_root);

        assert_eq!(groups.len(), 1, "两种前缀归一后应落进同一个组");
        assert_eq!(groups[0].0, "13");
        assert_eq!(groups[0].1.len(), 2);
        assert!(
            !groups[0].0.contains("sharelink"),
            "relative_parent 不得残留虚拟根: {}",
            groups[0].0
        );
    }

    /// 归一后必须保留分享根目录本身（如 `/13`）——超限下钻是拿这个路径去列
    /// 子目录的，多剥一层就会列不到东西。
    #[test]
    fn test_normalized_paths_keep_share_root_dir_for_include() {
        let p = strip_virtual_share_root("/sharelink0-37559844790/13/玉溪资料");
        assert!(
            p.starts_with("/13/"),
            "分享根目录 13 必须保留，否则 include 匹配不上 collector 的快照: {}",
            p
        );
    }

    // ========== 单次转存文件数上限拆批（零请求那一层） ==========

    fn make_dir(path: &str, fs_id: u64) -> SharedFileInfo {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        SharedFileInfo {
            fs_id,
            is_dir: true,
            path: path.to_string(),
            size: 0,
            name,
        }
    }

    /// 同一父目录下选中 1200 个条目，按上限 500 应切成 500 + 500 + 200 三批。
    /// 这一层不需要任何网络请求：条目数本身就超限，不用问百度。
    #[test]
    fn test_split_groups_splits_oversized_group() {
        let files: Vec<SharedFileInfo> = (0..1200)
            .map(|i| make_file(&format!("/share/影视/{}.mp4", i), i as u64))
            .collect();

        let batches = split_groups_by_file_limit(vec![("影视".to_string(), files)], 500);

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].1.len(), 500);
        assert_eq!(batches[1].1.len(), 500);
        assert_eq!(batches[2].1.len(), 200);
        // 切批不改变落点，每批仍指向同一个相对父目录
        assert!(batches.iter().all(|(parent, _)| parent == "影视"));
        // 一个都不能丢
        assert_eq!(batches.iter().map(|(_, f)| f.len()).sum::<usize>(), 1200);
    }

    /// 恰好等于上限不切批。
    #[test]
    fn test_split_groups_exactly_at_limit_stays_one_batch() {
        let files: Vec<SharedFileInfo> = (0..500)
            .map(|i| make_file(&format!("/share/{}.jpg", i), i as u64))
            .collect();

        let batches = split_groups_by_file_limit(vec![("".to_string(), files)], 500);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.len(), 500);
    }

    /// 未超限时原样返回，不产生多余批次。
    #[test]
    fn test_split_groups_noop_when_under_limit() {
        let files: Vec<SharedFileInfo> = (0..10)
            .map(|i| make_file(&format!("/share/{}.jpg", i), i as u64))
            .collect();

        let batches = split_groups_by_file_limit(vec![("".to_string(), files)], 500);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.len(), 10);
    }

    /// 不同父目录本来就分属不同批，切批不应把它们混到一起。
    #[test]
    fn test_split_groups_keeps_parent_dirs_separate() {
        let groups = vec![
            ("抖音".to_string(), vec![make_file("/share/抖音/1.mp4", 1)]),
            ("微信".to_string(), vec![make_file("/share/微信/2.mp4", 2)]),
        ];

        let batches = split_groups_by_file_limit(groups, 500);

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].0, "抖音");
        assert_eq!(batches[1].0, "微信");
    }

    /// 选中的是目录时这一层无从判断（1 个 fs_id 底下可能上万文件），
    /// 保持一批原样提交，交给惰性兜底——这正是不主动预扫的代价与前提。
    #[test]
    fn test_split_groups_cannot_judge_directories() {
        let dirs = vec![
            make_dir("/share/巨大目录", 1),
            make_dir("/share/另一个", 2),
        ];

        let batches = split_groups_by_file_limit(vec![("".to_string(), dirs)], 500);

        assert_eq!(batches.len(), 1, "目录数没超限，这一层不该拆");
        assert_eq!(batches[0].1.len(), 2);
    }

    // ========== 超限拆分的分割不变量 ==========

    /// 对半拆必须两边都非空，否则 pending 会无限循环。
    #[test]
    fn test_mid_index_never_produces_empty_side() {
        for len in 2..64usize {
            let items: Vec<SharedFileInfo> = (0..len)
                .map(|i| make_file(&format!("/x/{}.mp4", i), i as u64))
                .collect();
            let idx = mid_index(&items, len / 2);
            assert!(idx >= 1, "左边不能为空: len={}, idx={}", len, idx);
            assert!(idx < len, "右边不能为空: len={}, idx={}", len, idx);
        }
    }

    /// 长度为 2 时必须切成 1 + 1。
    #[test]
    fn test_mid_index_splits_pair_evenly() {
        let items = vec![make_file("/x/a", 1), make_file("/x/b", 2)];
        assert_eq!(mid_index(&items, 1), 1);
    }

    /// 下钻时子项的目标目录 = 原目标目录 + 该目录名，保持分享里的层级。
    #[test]
    fn test_child_dir_preserves_structure() {
        let cases = [
            ("/13", "上传", "/13/上传"),
            ("/13/", "上传", "/13/上传"),
            // 根目录：trim 掉全部尾部斜杠后不能拼出 `//`
            ("/", "影视", "/影视"),
        ];
        for (dir, name, want) in cases {
            let got = format!("{}/{}", dir.trim_end_matches('/'), name);
            assert_eq!(got, want, "dir={} name={}", dir, name);
        }
    }

    /// 复现日志里的「34/8」：8 项里有 1 个目录被下钻成 32 个子项，
    /// 分母应变成 8 - 1 + 32 = 39，而不是继续停在 8。
    #[test]
    fn test_adjust_total_units_matches_real_case() {
        // 该批 8 项，展开后共 39 个单元
        assert_eq!(adjust_total_units(8, 8, 39), 39);
        // 转存了 34 个，34 <= 39，不会再出现分子大于分母
        assert!(34 <= adjust_total_units(8, 8, 39));
    }

    /// 多批次场景：只修正当前批，其他批的计数不受影响。
    #[test]
    fn test_adjust_total_units_only_affects_current_batch() {
        // 共 20 项分 3 批（4 + 7 + 9），第二批 7 项展开成 30
        let total = 20;
        assert_eq!(adjust_total_units(total, 7, 30), 43); // 20 - 7 + 30
    }

    /// 没展开时分母不变。
    #[test]
    fn test_adjust_total_units_noop_when_not_expanded() {
        assert_eq!(adjust_total_units(12, 4, 4), 12);
    }

    /// 任何参数组合都不下溢——usize 下溢会得到天文数字，比显示错更难查。
    #[test]
    fn test_adjust_total_units_never_underflows() {
        assert_eq!(adjust_total_units(0, 100, 0), 0);
        assert_eq!(adjust_total_units(3, 10, 2), 2);
        assert_eq!(adjust_total_units(usize::MAX, 0, usize::MAX), usize::MAX);
    }

    #[test]
    fn test_is_transient_transfer_error() {
        // 实测挂掉批次 2/3 的那条响应
        assert!(is_transient_transfer_error("请求超时，请稍后再试"));
        assert!(is_transient_transfer_error("转存失败: connection reset by peer"));
        assert!(is_transient_transfer_error("operation timed out"));
        assert!(!is_transient_transfer_error("同名文件已存在: a.mp4"));
        assert!(!is_transient_transfer_error(""));
    }

    /// 三类失败必须互斥：临时错误重试同一组、文件数超限拆小、空间不足停手。
    /// 混淆任意两类都会退化成 dfe1a0b 那种空转（347 次 errno=-32）。
    #[test]
    fn test_three_failure_kinds_are_disjoint() {
        let transient = "请求超时，请稍后再试";
        let limit = "转存文件数 1815 超过上限 500";
        let quota = "网盘空间不足，无法转存";

        assert!(is_transient_transfer_error(transient));
        assert!(!is_file_limit_exceeded(transient) && !is_quota_exceeded(transient));

        assert!(is_file_limit_exceeded(limit));
        assert!(!is_transient_transfer_error(limit) && !is_quota_exceeded(limit));

        assert!(is_quota_exceeded(quota));
        assert!(!is_transient_transfer_error(quota) && !is_file_limit_exceeded(quota));
    }

    #[test]
    fn test_is_quota_exceeded() {
        assert!(is_quota_exceeded("网盘空间不足，无法转存（剩余空间不足，无法转存）"));
        assert!(is_quota_exceeded("转存失败: errno=-32"));
        assert!(is_quota_exceeded("空间不足"));
        // 文件数超限不能被误判成空间不足——那是要继续拆的
        assert!(!is_quota_exceeded("转存文件数 800 超过上限 500"));
        assert!(!is_quota_exceeded("同名文件已存在: a.mp4"));
        assert!(!is_quota_exceeded(""));
    }

    /// 空间不足与文件数超限必须互斥判定：前者停手，后者继续拆。
    /// dfe1a0b 就是把前者误判成后者，一路拆到每批 1 个文件刷了 347 次。
    #[test]
    fn test_quota_and_file_limit_are_disjoint() {
        let quota = "网盘空间不足，无法转存";
        let limit = "转存文件数 1815 超过上限 500";
        assert!(is_quota_exceeded(quota) && !is_file_limit_exceeded(quota));
        assert!(is_file_limit_exceeded(limit) && !is_quota_exceeded(limit));
    }

    /// 批次整体成功但内部有子批次失败时，警告必须冒泡到任务上，
    /// 否则界面显示「已转存」而进度条只走了一半（实测 12 项只成功 6 项）。
    #[test]
    fn test_merge_surfaces_partial_failure_inside_successful_batch() {
        let partial = TransferResult {
            success: true,
            transferred_paths: vec!["/13/a".into(), "/13/b".into()],
            from_paths: vec!["/a".into(), "/b".into()],
            transferred_fs_ids: vec![1, 2],
            error: Some("网盘空间不足，无法转存".into()),
        };
        let results = vec![(1usize, "13".to_string(), vec![make_file("/13/a", 1)], Ok(partial))];

        let (merged, _) = merge_batch_results(results, "/tmp");

        assert!(merged.success, "有成功条目，整体仍算成功");
        let err = merged.error.expect("部分失败必须留下告警，不能被吞掉");
        assert!(err.contains("部分文件未转存"), "实际: {}", err);
        assert!(err.contains("网盘空间不足"), "实际: {}", err);
    }

    /// 全部成功且无内部告警时不应凭空产生错误信息。
    #[test]
    fn test_merge_clean_success_has_no_warning() {
        let ok = TransferResult {
            success: true,
            transferred_paths: vec!["/13/a".into()],
            from_paths: vec!["/a".into()],
            transferred_fs_ids: vec![1],
            error: None,
        };
        let results = vec![(1usize, "13".to_string(), vec![make_file("/13/a", 1)], Ok(ok))];

        let (merged, _) = merge_batch_results(results, "/tmp");

        assert!(merged.success);
        assert!(merged.error.is_none(), "干净成功不该带告警: {:?}", merged.error);
    }

    #[test]
    fn test_is_file_limit_exceeded() {
        assert!(is_file_limit_exceeded("转存文件数 800 超过上限 500"));
        assert!(!is_file_limit_exceeded("同名文件已存在: a.mp4"));
        assert!(!is_file_limit_exceeded("转存失败: {\"errno\":2}"));
        assert!(!is_file_limit_exceeded(""));
    }

    /// 企业版转存错误必须落进本模块这三个分类器，否则自适应分批对企业版失效。
    ///
    /// 这三个判定全是**字符串关键字匹配**，`netdisk::share::apaas` 那边一旦
    /// 改了文案就会静默失联：超限不再下钻拆批而是整批失败、抖动不再退避重试、
    /// 空间不足不再早停（dfe1a0b 那次 347 连败就是这么来的）。
    /// 这里把跨模块契约钉死。
    #[test]
    fn apaas_transfer_errors_match_batch_classifiers() {
        use crate::netdisk::share::apaas::describe_transfer_errno;

        // 空间不足 → 早停，且不能被误判成「文件数超限」而去无限拆分
        for errno in [-10, -32, 31112] {
            let msg = describe_transfer_errno(errno, "空间不足，转存失败");
            assert!(is_quota_exceeded(&msg), "errno={} 应判为空间不足: {}", errno, msg);
            assert!(
                !is_file_limit_exceeded(&msg),
                "errno={} 不该判为文件数超限: {}",
                errno,
                msg
            );
        }

        // 文件数超限 → 触发下钻拆批，且不能被误判成空间不足而早停
        for errno in [-33, 120, 130, 31075, 31174, 31175] {
            let msg = describe_transfer_errno(errno, "");
            assert!(
                is_file_limit_exceeded(&msg),
                "errno={} 应判为文件数超限: {}",
                errno,
                msg
            );
            assert!(
                !is_quota_exceeded(&msg),
                "errno={} 不该判为空间不足: {}",
                errno,
                msg
            );
        }

        // 临时错误 → 退避重试
        for errno in [4, -31, 31069, 111, 31171] {
            let msg = describe_transfer_errno(errno, "");
            assert!(
                is_transient_transfer_error(&msg),
                "errno={} 应判为临时错误: {}",
                errno,
                msg
            );
            assert!(!is_file_limit_exceeded(&msg) && !is_quota_exceeded(&msg));
        }

        // 同名冲突：三类都不是，直接失败该批
        let dup = describe_transfer_errno(-30, "");
        assert!(!is_transient_transfer_error(&dup));
        assert!(!is_file_limit_exceeded(&dup));
        assert!(!is_quota_exceeded(&dup));
    }

    /// 默认上限 500，env 可覆盖为正整数；非法值回落默认。
    #[test]
    fn test_transfer_file_limit_default() {
        // 未设置 env 时应为默认值（测试进程内不设置该变量）
        if std::env::var("BAIDUPCS_TRANSFER_FILE_LIMIT").is_err() {
            assert_eq!(transfer_file_limit(), 500);
        }
    }

    #[test]
    fn test_detect_virtual_share_root_prefix() {
        let files = vec![
            make_file("/sharelink123-456/a/1.mp4", 1),
            make_file("/sharelink123-456/b/2.mp4", 2),
        ];
        assert_eq!(
            detect_virtual_share_root_prefix(&files),
            Some("/sharelink123-456".to_string())
        );

        // 真实路径，无虚拟根前缀
        let real = vec![make_file("/影视/a/1.mp4", 1)];
        assert_eq!(detect_virtual_share_root_prefix(&real), None);

        // 混合（部分不在虚拟根下）不应误判
        let mixed = vec![
            make_file("/sharelink123-456/a/1.mp4", 1),
            make_file("/其他/2.mp4", 2),
        ];
        assert_eq!(detect_virtual_share_root_prefix(&mixed), None);

        assert_eq!(detect_virtual_share_root_prefix(&[]), None);
    }

    // ========== local_dir derivation from transferred_path ==========

    #[test]
    fn test_local_dir_from_transferred_path_with_subdir() {
        let download_dir = PathBuf::from("D:/Downloads");
        let save_path = "/.bpr_share_temp/uuid123/";
        let transferred_path = "/.bpr_share_temp/uuid123/抖音/photo.jpg";

        let save_prefix = save_path.trim_end_matches('/');
        let relative = transferred_path[save_prefix.len()..].trim_start_matches('/');
        let local_dir = match Path::new(relative).parent() {
            Some(parent) if !parent.as_os_str().is_empty() => download_dir.join(parent),
            _ => download_dir.clone(),
        };
        assert_eq!(local_dir, download_dir.join("抖音"));
    }

    #[test]
    fn test_local_dir_from_transferred_path_root_file() {
        let download_dir = PathBuf::from("D:/Downloads");
        let save_path = "/.bpr_share_temp/uuid123/";
        let transferred_path = "/.bpr_share_temp/uuid123/photo.jpg";

        let save_prefix = save_path.trim_end_matches('/');
        let relative = transferred_path[save_prefix.len()..].trim_start_matches('/');
        let local_dir = match Path::new(relative).parent() {
            Some(parent) if !parent.as_os_str().is_empty() => download_dir.join(parent),
            _ => download_dir.clone(),
        };
        assert_eq!(local_dir, download_dir);
    }

    #[test]
    fn test_local_dir_from_transferred_path_deeply_nested() {
        let download_dir = PathBuf::from("D:/Downloads");
        let save_path = "/.bpr_share_temp/uuid123";
        let transferred_path = "/.bpr_share_temp/uuid123/a/b/file.jpg";

        let save_prefix = save_path.trim_end_matches('/');
        let relative = transferred_path[save_prefix.len()..].trim_start_matches('/');
        let local_dir = match Path::new(relative).parent() {
            Some(parent) if !parent.as_os_str().is_empty() => download_dir.join(parent),
            _ => download_dir.clone(),
        };
        assert_eq!(local_dir, download_dir.join("a/b"));
    }

    // ========== merge_batch_results ==========

    #[test]
    fn test_merge_all_success() {
        let r1 = TransferResult {
            success: true,
            transferred_paths: vec!["/tmp/抖音/photo.jpg".to_string()],
            from_paths: vec!["/share/抖音/photo.jpg".to_string()],
            transferred_fs_ids: vec![100],
            error: None,
        };
        let r2 = TransferResult {
            success: true,
            transferred_paths: vec!["/tmp/微信/photo.jpg".to_string()],
            from_paths: vec!["/share/微信/photo.jpg".to_string()],
            transferred_fs_ids: vec![200],
            error: None,
        };
        let results = vec![
            (1usize, "抖音".to_string(), vec![make_file("/share/抖音/photo.jpg", 1)], Ok(r1)),
            (2usize, "微信".to_string(), vec![make_file("/share/微信/photo.jpg", 2)], Ok(r2)),
        ];
        let (merged, groups_info) = merge_batch_results(results, "/tmp");
        assert!(merged.success);
        assert!(merged.error.is_none());
        assert_eq!(merged.transferred_paths.len(), 2);
        assert_eq!(groups_info.len(), 2);
        assert_eq!(groups_info[0].remote_dir, "/tmp/抖音");
        assert_eq!(groups_info[1].remote_dir, "/tmp/微信");
    }

    #[test]
    fn test_merge_partial_failure_still_success() {
        let r1 = TransferResult {
            success: true,
            transferred_paths: vec!["/tmp/抖音/a.jpg".to_string()],
            from_paths: vec!["/share/抖音/a.jpg".to_string()],
            transferred_fs_ids: vec![100],
            error: None,
        };
        let results = vec![
            (1usize, "抖音".to_string(), vec![make_file("/share/抖音/a.jpg", 1)], Ok(r1)),
            (2usize, "微信".to_string(), vec![make_file("/share/微信/b.jpg", 2)], Err(anyhow::anyhow!("api error"))),
        ];
        let (merged, groups_info) = merge_batch_results(results, "/tmp");
        assert!(merged.success, "至少一个批次成功应返回 success=true");
        assert!(merged.error.is_some(), "部分失败时应记录 error 警告");
        assert_eq!(merged.transferred_paths.len(), 1);
        assert_eq!(groups_info.len(), 1);
    }

    #[test]
    fn test_merge_all_fail() {
        let results: Vec<(usize, String, Vec<SharedFileInfo>, Result<TransferResult>)> = vec![
            (1usize, "抖音".to_string(), vec![], Err(anyhow::anyhow!("fail 1"))),
            (2usize, "微信".to_string(), vec![], Err(anyhow::anyhow!("fail 2"))),
        ];
        let (merged, groups_info) = merge_batch_results(results, "/tmp");
        assert!(!merged.success, "所有批次失败应返回 success=false");
        assert!(merged.error.is_some());
        assert!(groups_info.is_empty());
    }

    #[test]
    fn test_merge_single_success() {
        let r1 = TransferResult {
            success: true,
            transferred_paths: vec!["/tmp/a.jpg".to_string()],
            from_paths: vec![],
            transferred_fs_ids: vec![1],
            error: None,
        };
        let results = vec![
            (1usize, "".to_string(), vec![], Ok(r1)),
        ];
        let (merged, groups_info) = merge_batch_results(results, "/tmp");
        assert!(merged.success);
        assert!(merged.error.is_none());
        // empty group_id means root level, remote_dir should be temp_dir itself
        assert_eq!(groups_info[0].remote_dir, "/tmp");
    }

    // ========== issue #162：丢弃任务时的临时目录善后 ==========

    fn share_direct_task(status: TransferStatus) -> TransferTask {
        let temp_dir = "/.bpr_share_temp/00aaf329-43bd-49d5-9c3f-8648c6ff4e43/".to_string();
        let mut task = TransferTask::new(
            "https://pan.baidu.com/s/1abc".to_string(),
            None,
            temp_dir.clone(),
            0,
            true,
            None,
        );
        task.is_share_direct_download = true;
        task.temp_dir = Some(temp_dir);
        task.status = status;
        task
    }

    #[test]
    fn test_abandoned_temp_dir_for_unfinished_share_direct_task() {
        for status in [
            TransferStatus::Queued,
            TransferStatus::CheckingShare,
            TransferStatus::Transferring,
            TransferStatus::Downloading,
            TransferStatus::Cleaning,
        ] {
            let task = share_direct_task(status.clone());
            assert_eq!(
                TransferManager::abandoned_temp_dir(&task).as_deref(),
                Some("/.bpr_share_temp/00aaf329-43bd-49d5-9c3f-8648c6ff4e43/"),
                "{:?}",
                status
            );
        }
    }

    #[test]
    fn test_abandoned_temp_dir_skips_terminal_and_normal_tasks() {
        // 终态：各自分支已按配置清理过，不重复发删除请求
        for status in [
            TransferStatus::Completed,
            TransferStatus::Transferred,
            TransferStatus::TransferFailed,
            TransferStatus::DownloadFailed,
        ] {
            let task = share_direct_task(status.clone());
            assert!(TransferManager::abandoned_temp_dir(&task).is_none(), "{:?}", status);
        }

        // 普通转存：save_path 是用户目录，绝不能删
        let mut task = share_direct_task(TransferStatus::Downloading);
        task.is_share_direct_download = false;
        assert!(TransferManager::abandoned_temp_dir(&task).is_none());

        // temp_dir 为空
        let mut task = share_direct_task(TransferStatus::Downloading);
        task.temp_dir = Some("  ".to_string());
        assert!(TransferManager::abandoned_temp_dir(&task).is_none());
    }

    // ========== 已结束内部任务的内存回收 ==========

    fn test_manager() -> TransferManager {
        let user = crate::auth::UserAuth::new(1, "user_1".to_string(), "bduss_1".to_string());
        let client = NetdiskClient::new(user).expect("create test client");
        TransferManager::new(
            Arc::new(StdRwLock::new(client)),
            TransferConfig::default(),
            Arc::new(RwLock::new(AppConfig::default())),
        )
    }

    fn insert_task(
        tm: &TransferManager,
        is_internal: bool,
        cfg: Option<&str>,
        status: TransferStatus,
    ) -> String {
        let mut task = share_direct_task(status);
        task.is_internal = is_internal;
        task.backup_config_id = cfg.map(|c| c.to_string());
        let id = task.id.clone();
        tm.tasks.insert(
            id.clone(),
            TransferTaskInfo {
                task: Arc::new(RwLock::new(task)),
                cancellation_token: CancellationToken::new(),
            },
        );
        id
    }

    #[tokio::test]
    async fn test_get_internal_tasks_by_backup_config() {
        let tm = test_manager();
        let a = insert_task(&tm, true, Some("share-sync:a"), TransferStatus::Transferring);
        insert_task(&tm, true, Some("share-sync:b"), TransferStatus::Transferring);
        // 非内部任务即使带同名归属也不算
        insert_task(&tm, false, Some("share-sync:a"), TransferStatus::Transferring);

        let got: Vec<String> = tm
            .get_internal_tasks_by_backup_config("share-sync:a")
            .await
            .into_iter()
            .map(|t| t.id)
            .collect();
        assert_eq!(got, vec![a]);
    }

    #[tokio::test]
    async fn test_evict_only_finished_internal_tasks() {
        let tm = test_manager();
        let done = insert_task(&tm, true, Some("share-sync:a"), TransferStatus::Completed);
        let running = insert_task(&tm, true, Some("share-sync:a"), TransferStatus::Downloading);
        let visible = insert_task(&tm, false, None, TransferStatus::Completed);

        let candidates: Vec<String> = tm
            .finished_internal_task_candidates()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(candidates, vec![done.clone()]);

        // 传入不该回收的 id 也只会移走真正可回收的
        let removed = tm
            .evict_finished_internal_tasks(&[done.clone(), running.clone(), visible.clone()])
            .await;
        assert_eq!(removed, 1);
        assert!(!tm.has_task_in_memory(&done));
        assert!(tm.has_task_in_memory(&running));
        assert!(tm.has_task_in_memory(&visible));
    }

    #[tokio::test]
    async fn test_evict_rechecks_status() {
        let tm = test_manager();
        let id = insert_task(&tm, true, Some("share-sync:a"), TransferStatus::Transferred);
        let candidates = tm.finished_internal_task_candidates().await;
        assert_eq!(candidates.len(), 1);

        // 收集候选之后状态变回非终态（如恢复后重新开始下载）
        {
            let arc = tm.tasks.get(&id).map(|e| e.value().task.clone()).unwrap();
            arc.write().await.status = TransferStatus::Downloading;
        }
        assert_eq!(tm.evict_finished_internal_tasks(&[id.clone()]).await, 0);
        assert!(tm.has_task_in_memory(&id));
    }
}
