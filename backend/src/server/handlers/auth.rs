// 认证API处理器

use crate::auth::{CookieLoginApiRequest, CookieLoginAuth, QRCode, QRCodeStatus, Uid, UserAuth};
use crate::common::ProxyType;
use crate::server::{broadcast_account_list_changed, set_active_uid, AppState};
use crate::transfer::TransferManager;
use crate::uploader::UploadManager;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

/// 统一API响应格式
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    /// 状态码 (0: 成功, 其他: 错误码)
    pub code: i32,
    /// 消息
    pub message: String,
    /// 数据
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            code: 0,
            message: "Success".to_string(),
            data: Some(data),
        }
    }

    pub fn success_with_message(data: T, message: impl Into<String>) -> Self {
        Self {
            code: 0,
            message: message.into(),
            data: Some(data),
        }
    }

    pub fn error(code: i32, message: String) -> Self {
        Self {
            code,
            message,
            data: None,
        }
    }
}

/// 生成登录二维码
///
/// POST /api/v1/auth/qrcode/generate
pub async fn generate_qrcode(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<QRCode>>, StatusCode> {
    info!("API: 生成登录二维码");

    match state.qrcode_auth.read().await.generate_qrcode().await {
        Ok(qrcode) => {
            info!("二维码生成成功: sign={}", qrcode.sign);
            Ok(Json(ApiResponse::success(qrcode)))
        }
        Err(e) => {
            error!("二维码生成失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("Failed to generate QR code: {}", e),
            )))
        }
    }
}

/// 查询参数：sign
#[derive(Debug, Deserialize)]
pub struct QRCodeStatusQuery {
    pub sign: String,
    /// 是否处于"添加账号"模式（多账号）。
    ///
    /// 添加账号时用户通常已经有一个活跃账号，必须跳过下面"已有活跃账号即视为
    /// 登录成功"的防呆短路，否则会在用户尚未扫码时就用旧账号返回 Success，
    /// 导致前端误报"账号添加成功"。
    #[serde(default)]
    pub add: bool,
}

/// 查询扫码状态
///
/// GET /api/v1/auth/qrcode/status?sign=xxx
pub async fn qrcode_status(
    State(state): State<AppState>,
    Query(params): Query<QRCodeStatusQuery>,
) -> Result<Json<ApiResponse<QRCodeStatus>>, StatusCode> {
    info!("API: 查询扫码状态: sign={}", params.sign);

    // 防呆：检查是否已有有效的活跃账号会话
    // 从 active_uid → AccountManager 取，不再读 legacy session.json
    //
    // 注意：添加账号模式（add=true）下用户本就已有活跃账号，必须跳过此短路，
    // 否则会在尚未扫码时直接用旧账号返回 Success，误报"账号添加成功"。
    if !params.add {
        if let Some(user) = state.active_user_auth().await {
            info!("检测到已有活跃账号: UID={}, 验证 BDUSS 有效性...", user.uid);

            match state
                .qrcode_auth
                .read()
                .await
                .verify_bduss(&user.bduss)
                .await
            {
                Ok(true) => {
                    info!("✅ BDUSS 仍然有效，直接返回登录成功状态");

                    // 确保活跃账号客户端已初始化
                    let client_initialized = state.active_client().await.is_some();
                    if !client_initialized {
                        info!("🔄 客户端未初始化，开始初始化用户资源...");
                        if let Err(e) = state.load_initial_session().await {
                            error!("❌ 初始化用户资源失败: {}", e);
                        } else {
                            info!("✅ 用户资源初始化成功");
                        }
                    }

                    // 直接返回 Success 状态，token 使用 BDUSS
                    return Ok(Json(ApiResponse::success(QRCodeStatus::Success {
                        user: user.clone(),
                        token: user.bduss.clone(),
                    })));
                }
                Ok(false) => {
                    warn!("⚠️ 持久化的 BDUSS 已失效，继续扫码流程");
                    // BDUSS 失效不再触发 session.json 清除：legacy session 已被
                    // accounts.json 取代，由 set_active_uid / force_delete 等专用路径
                    // 处理凭据失效。
                }
                Err(e) => {
                    warn!("⚠️ BDUSS 验证出错: {}，继续扫码流程", e);
                }
            }
        }
    }

    match state
        .qrcode_auth
        .read()
        .await
        .poll_status(&params.sign)
        .await
    {
        Ok(status) => {
            // 如果登录成功，保存会话并初始化用户资源
            if let QRCodeStatus::Success { ref user, .. } = status {
                info!(
                    "检测到登录成功，准备保存会话: UID={}, 用户名={}",
                    user.uid, user.username
                );
                let mut session = state.session_manager.lock().await;

                // 先保存基本会话信息
                if let Err(e) = session.save_session(user).await {
                    error!("❌ 保存会话失败: {}", e);
                    return Ok(Json(ApiResponse::success(status)));
                }

                info!(
                    "✅ 会话保存成功: UID={}, BDUSS长度={}",
                    user.uid,
                    user.bduss.len()
                );

                // 初始化用户资源（网盘客户端和下载管理器）
                *state.current_user.write().await = Some(user.clone());

                // 初始化网盘客户端
                let config_guard = state.config.read().await;
                let proxy_config = if config_guard.network.proxy.proxy_type != ProxyType::None {
                    Some(config_guard.network.proxy.clone())
                } else {
                    None
                };
                drop(config_guard);

                // 设置代理回退管理器的用户代理配置
                state
                    .fallback_mgr
                    .set_user_proxy_config(proxy_config.clone())
                    .await;

                let fallback_for_client = if proxy_config.is_some() {
                    Some(Arc::clone(&state.fallback_mgr))
                } else {
                    None
                };
                let client = match crate::netdisk::NetdiskClient::new_with_proxy(
                    user.clone(),
                    proxy_config.as_ref(),
                    fallback_for_client.clone(),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("初始化网盘客户端失败: {}", e);
                        return Ok(Json(ApiResponse::success(status)));
                    }
                };

                // 执行预热并保存预热 Cookie
                info!("登录成功,开始预热会话...");
                let mut updated_user = user.clone();
                match client.warmup_and_get_cookies().await {
                    Ok((panpsc, csrf_token, bdstoken, stoken)) => {
                        info!("预热成功,保存预热 Cookie 到 session.json");
                        if panpsc.is_some() {
                            updated_user.panpsc = panpsc;
                        }
                        if csrf_token.is_some() {
                            updated_user.csrf_token = csrf_token;
                        }
                        updated_user.bdstoken = bdstoken;
                        // 预热时下发的 STOKEN 优先于登录时获取的
                        if stoken.is_some() {
                            updated_user.stoken = stoken;
                        }

                        // 更新内存和持久化
                        *state.current_user.write().await = Some(updated_user.clone());
                        if let Err(e) = session.save_session(&updated_user).await {
                            error!("保存预热 Cookie 失败: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("预热失败(继续使用未预热的客户端): {}", e);
                    }
                }

                let client_arc = Arc::new(client.clone());
                *state.netdisk_client.write().await = Some(client.clone());

                // 初始化下载管理器
                //
                // 用账号 `custom_config` 推算 effective 配置覆盖全局值。
                let config = state.config.read().await;
                let download_dir = config.download.download_dir.clone();
                let cc = &updated_user.custom_config;
                let vip =
                    crate::downloader::budget_scheduler::VipType::from_raw(updated_user.vip_type);
                let recommended_threads = match vip {
                    crate::downloader::budget_scheduler::VipType::Normal => {
                        config.multi_account_vip_recommended.normal.threads
                    }
                    crate::downloader::budget_scheduler::VipType::Vip => {
                        config.multi_account_vip_recommended.vip.threads
                    }
                    crate::downloader::budget_scheduler::VipType::Svip => {
                        config.multi_account_vip_recommended.svip.threads
                    }
                };
                let max_global_threads = if cc.auto_apply_recommended {
                    recommended_threads
                } else {
                    cc.download.max_global_threads
                };
                let max_concurrent_tasks = cc.download.max_concurrent_tasks;
                let max_retries = cc.download.max_retries;
                let up_threads = if cc.auto_apply_recommended {
                    recommended_threads
                } else {
                    cc.upload.max_global_threads
                };
                let mut upload_config = config.upload.clone();
                upload_config.max_global_threads = up_threads;
                upload_config.max_concurrent_tasks = cc.upload.max_concurrent_tasks;
                upload_config.max_retries = cc.upload.max_retries;
                let transfer_config = config.transfer.clone();
                drop(config);

                // 获取持久化管理器引用
                let pm_arc = Arc::clone(&state.persistence_manager);

                // 🔥 BudgetScheduler + decrypt_semaphore 由 `new_for_account` 构造时直接注入。
                match crate::downloader::DownloadManager::new_for_account(
                    updated_user.clone(),
                    download_dir,
                    max_global_threads,
                    max_concurrent_tasks,
                    max_retries,
                    proxy_config.as_ref(),
                    fallback_for_client,
                    Arc::clone(&state.budget_scheduler),
                    Arc::clone(&state.decrypt_semaphore),
                ) {
                    Ok(mut manager) => {
                        // 设置持久化管理器
                        manager.set_persistence_manager(Arc::clone(&pm_arc));

                        // owner_uid 已在 `new_for_account` 构造时由 `updated_user.uid` 直接注入

                        // 🔥 多账号：将活跃账号 UID 写入持久化管理器，
                        // 使后续所有 register_* 写入的元数据自动带上 owner_uid。
                        pm_arc.lock().await.set_owner_uid(updated_user.uid);
                        info!("PersistenceManager owner_uid 已设置: {}", updated_user.uid);

                        // 设置 WebSocket 管理器
                        manager.set_ws_manager(Arc::clone(&state.ws_manager)).await;

                        // 🔥 SAFETY 断言（chunk-level）+ budget acquire 在 spawn_chunk_download 默认启用。

                        let manager_arc = Arc::new(manager);

                        // 🔥 启动 auto_requeue 消费循环
                        // （否则 scheduler 失败/抢占/重排时 requeue_rx 无人消费，任务卡住）
                        Arc::clone(&manager_arc).start_auto_requeue_consumer().await;

                        // 设置文件夹下载管理器的依赖
                        state
                            .folder_download_manager
                            .set_download_manager(Arc::clone(&manager_arc))
                            .await;
                        state
                            .folder_download_manager
                            .set_netdisk_client(client_arc)
                            .await;

                        // 设置文件夹下载管理器的 WAL 目录
                        let wal_dir = pm_arc.lock().await.wal_dir().clone();
                        state.folder_download_manager.set_wal_dir(wal_dir).await;

                        // 设置文件夹下载管理器的持久化管理器（用于加载历史文件夹）
                        state
                            .folder_download_manager
                            .set_persistence_manager(Arc::clone(&pm_arc))
                            .await;

                        // 设置文件夹下载管理器的 WebSocket 管理器
                        state
                            .folder_download_manager
                            .set_ws_manager(Arc::clone(&state.ws_manager))
                            .await;

                        // 设置下载管理器对文件夹管理器的引用（用于回收借调槽位）
                        manager_arc
                            .set_folder_manager(Arc::clone(&state.folder_download_manager))
                            .await;

                        info!("✅ 下载管理器初始化成功");

                        // 初始化上传管理器（使用配置参数）
                        // 🔥 BudgetScheduler 由 `new_for_account` 构造时直接注入。
                        let config_dir = std::path::Path::new("config");
                        let upload_manager = UploadManager::new_for_account(
                            client.clone(),
                            &updated_user,
                            &upload_config,
                            Arc::clone(&state.budget_scheduler),
                            config_dir,
                        );
                        let upload_manager_arc = Arc::new(upload_manager);

                        // 设置上传管理器的持久化管理器
                        upload_manager_arc
                            .set_persistence_manager(Arc::clone(&pm_arc))
                            .await;

                        // 设置上传管理器的 WebSocket 管理器
                        upload_manager_arc
                            .set_ws_manager(Arc::clone(&state.ws_manager))
                            .await;

                        // 🔥 SAFETY 断言 + budget acquire 在 spawn_chunk_upload 默认启用。

                        // 🔥 设置备份记录管理器（用于文件夹名加密映射）
                        upload_manager_arc
                            .set_backup_record_manager(Arc::clone(&state.backup_record_manager))
                            .await;

                        info!("✅ 上传管理器初始化成功");

                        // 初始化转存管理器
                        let mut transfer_manager = TransferManager::new(
                            Arc::new(std::sync::RwLock::new(client)),
                            transfer_config,
                            Arc::clone(&state.config),
                        );
                        // 🔥 多账号：注入 owner_uid，使后续所有 TransferTask::new(...)
                        // 链调 .with_owner_uid(self.owner_uid) 能写入正确归属（之前默认 0）
                        transfer_manager.set_owner_uid(crate::auth::Uid::new(updated_user.uid));
                        let transfer_manager_arc = Arc::new(transfer_manager);

                        // 设置下载管理器（用于自动下载功能）
                        transfer_manager_arc
                            .set_download_manager(Arc::clone(&manager_arc))
                            .await;

                        // 设置文件夹下载管理器（用于自动下载文件夹）
                        transfer_manager_arc
                            .set_folder_download_manager(Arc::clone(&state.folder_download_manager))
                            .await;

                        // 设置转存管理器的持久化管理器
                        transfer_manager_arc
                            .set_persistence_manager(Arc::clone(&pm_arc))
                            .await;

                        // 设置转存管理器的 WebSocket 管理器
                        transfer_manager_arc
                            .set_ws_manager(Arc::clone(&state.ws_manager))
                            .await;

                        info!("✅ 转存管理器初始化成功");

                        // 🔥 多账号 per-uid Manager 池注入
                        // 登录后活跃账号的 3 个 Manager 同步插入 DashMap 池。
                        state.register_account_managers(
                            crate::auth::Uid::new(updated_user.uid),
                            Arc::clone(&manager_arc),
                            Arc::clone(&upload_manager_arc),
                            Arc::clone(&transfer_manager_arc),
                        );

                        // 🔥 把 per-uid manager 池注入到 FolderDownloadManager
                        // （否则 fresh login / 新账号场景 pool miss → fallback 到单例错误归并）。
                        // 同时初始化 ScanManager 并注入 upload manager 池
                        // （否则 fresh login 后 /uploads/folder 报"扫描管理器未初始化"）。
                        state
                            .folder_download_manager
                            .set_download_manager_pool(Arc::clone(&state.download_managers))
                            .await;
                        info!("✅ FolderDownloadManager per-uid 下载池已注入");

                        // 初始化扫描管理器（如尚未存在）+ 注入 upload manager 池
                        {
                            let scan_already_set = state.scan_manager.read().await.is_some();
                            if !scan_already_set {
                                let max_pending = state.config.read().await.scan.max_pending_tasks;
                                let wal_dir = pm_arc.lock().await.wal_dir().clone();
                                let scan_mgr = crate::uploader::ScanManager::new_with_owner(
                                    Arc::clone(&upload_manager_arc),
                                    Arc::clone(&state.ws_manager),
                                    Arc::clone(&state.memory_monitor),
                                    wal_dir,
                                    max_pending,
                                    Some(crate::auth::Uid::new(updated_user.uid)),
                                );
                                scan_mgr
                                    .set_upload_manager_pool(Arc::clone(&state.upload_managers))
                                    .await;
                                *state.scan_manager.write().await = Some(Arc::new(scan_mgr));
                                info!("✅ 扫描管理器已初始化（含 per-uid upload 池）");
                            } else if let Some(scan_mgr) =
                                state.scan_manager.read().await.as_ref().cloned()
                            {
                                // 已存在 → 重新注入最新池（幂等）
                                scan_mgr
                                    .set_upload_manager_pool(Arc::clone(&state.upload_managers))
                                    .await;
                                info!("✅ ScanManager per-uid upload 池已刷新");
                            }
                        }

                        // 启动 WebSocket 批量发送器
                        Arc::clone(&state.ws_manager).start_batch_sender();
                        info!("✅ WebSocket 批量发送器已启动");

                        // 🔥 启动内存监控器
                        Arc::clone(&state.memory_monitor).start();
                        info!("✅ 内存监控器已启动");

                        // 🔥 初始化自动备份管理器
                        state.init_autobackup_manager().await;
                        info!("✅ 自动备份管理器初始化完成");

                        // 🔥 autobackup 提供 encryption_config_store 后，
                        // 统一为所有 per-uid manager 注入运行时依赖
                        // （folder_manager / snapshot / encryption_config_store / sender 共享）
                        let registered_uids: Vec<crate::auth::Uid> =
                            state.download_managers.iter().map(|e| *e.key()).collect();
                        for uid in registered_uids {
                            if let Err(e) = state.wire_manager_runtime_deps_for_uid(uid).await {
                                warn!(
                                    "登录后 uid={} wire_manager_runtime_deps 失败（继续）: {}",
                                    uid.raw(),
                                    e
                                );
                            }
                        }

                        // 🔥 初始化分享同步管理器
                        state.init_share_sync_manager().await;
                        info!("✅ 分享同步管理器初始化完成");

                        // 🔥 初始化离线下载监听服务
                        state.init_cloud_dl_monitor().await;
                        info!("✅ 离线下载监听服务初始化完成");

                        // 🔥 登录成功后激活账号 + 广播事件
                        if let Err(e) = add_account_and_activate(&state, updated_user.clone()).await
                        {
                            error!("qrcode_status add_account_and_activate 失败: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("❌ 初始化下载管理器失败: {}", e);
                    }
                }
            }

            Ok(Json(ApiResponse::success(status)))
        }
        Err(e) => {
            error!("查询扫码状态失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("Failed to poll status: {}", e),
            )))
        }
    }
}

/// 获取当前用户信息
///
/// GET /api/v1/auth/user
///
/// 从 `active_uid → AccountManager.get_user` 取，符合运行时真源原则。
/// 多账号场景下返回的就是当前 active_uid 对应的账号。
pub async fn get_current_user(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("🔍 API: 获取当前用户信息");

    // 从 active_uid → AccountManager 取活跃账号
    let user = match state.active_user_auth().await {
        Some(u) => u,
        None => {
            warn!("❌ 未找到活跃账号，用户未登录");
            return Ok(Json(ApiResponse::error(401, "Not logged in".to_string())));
        }
    };

    info!(
        "✅ 找到活跃账号: UID={}, 用户名={}",
        user.uid, user.username
    );

    // 验证 BDUSS 是否仍然有效
    match state
        .qrcode_auth
        .read()
        .await
        .verify_bduss(&user.bduss)
        .await
    {
        Ok(true) => {
            // BDUSS 有效，检查活跃账号客户端是否已初始化
            info!("BDUSS 验证通过");

            let client_initialized = state.active_client().await.is_some();
            if !client_initialized {
                info!("🔄 检测到客户端未初始化，开始初始化用户资源...");
                if let Err(e) = state.load_initial_session().await {
                    error!("❌ 初始化用户资源失败: {}", e);
                    // 初始化失败不影响返回用户信息
                } else {
                    info!("✅ 用户资源初始化成功");
                }
            }

            Ok(Json(ApiResponse::success(user)))
        }
        Ok(false) => {
            // BDUSS 已失效。这里只标记 401，账号删除/切换由调用方按需触发。
            warn!("BDUSS 已失效 uid={}", user.uid);
            Ok(Json(ApiResponse::error(
                401,
                "Session expired, please login again".to_string(),
            )))
        }
        Err(e) => {
            // 验证失败（可能是网络问题），暂时允许通过
            warn!("BDUSS 验证失败: {}，暂时允许通过", e);
            Ok(Json(ApiResponse::success(user)))
        }
    }
}

/// 登出
///
/// POST /api/v1/auth/logout
///
/// 通过 `delete_account_helper(force = true)` 编排关闭活跃账号的所有
/// per-uid Manager + 移除 client_pool + 删除 AccountManager 条目 + 广播事件。
pub async fn logout(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 用户登出");

    // 1. 清除持久化 session（文件 + 内存缓存）— legacy 字段保留
    let clear_result = {
        let mut session = state.session_manager.lock().await;
        session.clear_session().await
    };

    // 2. 清除内存中的当前用户，确保下次登录时不携带旧状态
    *state.current_user.write().await = None;

    // 3. 清除网盘客户端（含旧 Cookie Jar），下次登录时重新创建
    *state.netdisk_client.write().await = None;

    // 🔥 多账号删除编排（force 契约）
    // - active_uid = Some(uid)：删除该 uid + force shutdown 所有 per-uid Manager
    // - active_uid = None：跳过（已无活跃账号）
    if let Some(active) = *state.active_uid.read().await {
        if let Err(e) = delete_account_helper(&state, Some(active), true).await {
            error!("delete_account_helper(active) 失败: {}", e);
        }
    } else {
        info!("登出：当前无活跃账号，跳过 force-delete 编排");
    }

    info!("已清除 current_user / netdisk_client / per-uid Managers");

    match clear_result {
        Ok(_) => {
            info!("登出成功");
            Ok(Json(ApiResponse::<()>::success(())))
        }
        Err(e) => {
            error!("登出失败: {}", e);
            Ok(Json(ApiResponse::<()>::error(
                500,
                format!("Failed to logout: {}", e),
            )))
        }
    }
}

/// Cookie 登录
///
/// POST /api/v1/auth/cookie/login
///
/// 接受从浏览器 DevTools 复制的完整 Cookie 字符串，解析出 BDUSS / PTOKEN / STOKEN
/// 等字段后验证有效性并初始化所有管理器（与二维码登录后流程完全一致）。
pub async fn cookie_login(
    State(state): State<AppState>,
    Json(req): Json<CookieLoginApiRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("API: Cookie 登录");

    if req.cookies.trim().is_empty() {
        return Ok(Json(ApiResponse::<crate::auth::UserAuth>::error(
            400,
            "cookies 字段不能为空".to_string(),
        )));
    }

    // 读取当前代理配置
    let proxy_config = {
        let config_guard = state.config.read().await;
        if config_guard.network.proxy.proxy_type != ProxyType::None {
            Some(config_guard.network.proxy.clone())
        } else {
            None
        }
    };

    // 创建 Cookie 登录客户端（复用代理配置）
    let cookie_auth = match CookieLoginAuth::new_with_proxy(proxy_config.as_ref()) {
        Ok(a) => a,
        Err(e) => {
            error!("创建 Cookie 登录客户端失败: {}", e);
            return Ok(Json(ApiResponse::<crate::auth::UserAuth>::error(
                500,
                format!("创建客户端失败: {}", e),
            )));
        }
    };

    // 解析 Cookie 并验证 BDUSS
    let user = match cookie_auth.login_with_cookies(&req.cookies).await {
        Ok(u) => u,
        Err(e) => {
            error!("Cookie 登录失败: {}", e);
            return Ok(Json(ApiResponse::<crate::auth::UserAuth>::error(
                400,
                format!("{}", e),
            )));
        }
    };

    info!(
        "Cookie 验证成功: UID={}, 用户名={}，开始初始化会话...",
        user.uid, user.username
    );

    // 保存会话（先释放锁再调用 load_initial_session，避免死锁）
    {
        let mut session = state.session_manager.lock().await;
        if let Err(e) = session.save_session(&user).await {
            error!("保存会话失败: {}", e);
            return Ok(Json(ApiResponse::<crate::auth::UserAuth>::error(
                500,
                format!("保存会话失败: {}", e),
            )));
        }
        *state.current_user.write().await = Some(user.clone());
        info!("✅ 会话保存成功");
    } // session 锁在此释放

    // 记录 PTOKEN 是否存在（用于后续判断预热是否可能成功）
    let has_ptoken = user.ptoken.is_some();

    if !has_ptoken {
        warn!("Cookie 中缺少 PTOKEN，预热将被跳过 → panpsc/csrf_token/bdstoken 无法获取，转存等功能不可用");
    }

    // 🔥 登录成功后注册到 AccountManager + 切换活跃账号 + 广播事件
    if let Err(e) = add_account_and_activate(&state, user.clone()).await {
        error!("add_account_and_activate 失败: {}", e);
    }

    // 复用 load_initial_session 完成完整初始化：
    //   - 创建 NetdiskClient（含代理）
    //   - 执行预热（获取 PANPSC / csrfToken / bdstoken）
    //   - 初始化下载/上传/转存管理器
    //   - 恢复持久化任务
    //   - 启动 WebSocket / 内存监控 / 自动备份 / 离线下载监听
    if let Err(e) = state.load_initial_session().await {
        error!("初始化用户资源失败: {}", e);
        // 不阻断登录——会话已保存，用户可重试或刷新页面
    } else {
        info!("✅ Cookie 登录后初始化完成");
    }

    // 返回最新内存中的用户信息（包含预热后更新的字段）
    let final_user = state.current_user.read().await.clone().unwrap_or(user);

    // 根据预热结果决定响应 message
    let warmup_ok = final_user.panpsc.is_some() && final_user.csrf_token.is_some();
    let message = if warmup_ok {
        "登录成功，预热完成".to_string()
    } else if !has_ptoken {
        "登录成功（未预热）。文件浏览和下载可正常使用；创建文件夹、上传、转存到新目录等操作需要预热（bdstoken），可能失败。建议从浏览器 Network 请求头复制包含 PTOKEN 的完整 Cookie 以获得完整功能".to_string()
    } else {
        "登录成功（预热失败，可能为网络问题）。文件浏览和下载正常；创建文件夹、上传等操作可能受影响，可尝试重新登录".to_string()
    };

    if warmup_ok {
        info!("✅ Cookie 登录完成，预热成功: UID={}", final_user.uid);
    } else {
        warn!(
            "⚠️ Cookie 登录完成，但预热未成功: ptoken={}, panpsc={}",
            has_ptoken,
            final_user.panpsc.is_some()
        );
    }

    Ok(Json(ApiResponse::success_with_message(final_user, message)))
}

// ============================================================================
// 多账号登录 / 登出 helpers
// ============================================================================

/// 登录成功后注册账号并激活。
///
/// 调用顺序：
///   1. `account_manager.add_user(user)` — 持久化到 `accounts.json`
///   2. `budget_scheduler.add_account(uid, vip, dl_req, up_req)` — 注册到全局预算调度器
///      （未注册时分片调度会走 `acquire_chunk_permit → None`
///      回滚 + return，导致下载/上传整个停摆。这里必须按 `seed_budget_scheduler` 同款逻辑
///      把账号加入 `BudgetScheduler` 双轨；先于 `set_active_uid` 调用，确保切到 active
///      之后第一笔分片就能 acquire 到 permit。）
///   3. `set_active_uid(Some(uid))` — 写运行时真源 + 持久化镜像 + 发 `Switched`
///   4. `broadcast_account_list_changed` — 发 `ListChanged`
///   5. `broadcast_budget_recomputed` — 推 `BudgetEvent::BudgetRecomputed`
///      （`add_account` 内部已 `recompute_budget`，这里只补 WS 推送）
///
/// **幂等性**：`add_user` + `BudgetScheduler::add_account`（内部 `upsert_account`）
/// 都支持「更新已有账号」；多次调用同一 UID 安全。
///
/// 注：本 helper **不**重复执行客户端构造 / per-uid Manager 注入；这些步骤由
/// `state.load_initial_session()` 在主登录流程中按需触发。
pub async fn add_account_and_activate(state: &AppState, user: UserAuth) -> anyhow::Result<()> {
    let uid = Uid::new(user.uid);

    // 提前提取 budget 注册参数（与 `state.rs::seed_budget_scheduler` 同款逻辑）
    let vip = crate::downloader::budget_scheduler::VipType::from_raw(user.vip_type);
    let auto = user.custom_config.auto_apply_recommended;
    let dl_threads = user.custom_config.download.max_global_threads;
    let up_threads = user.custom_config.upload.max_global_threads;
    let dl_req = if auto {
        crate::downloader::budget_scheduler::RequestedSource::Auto
    } else {
        crate::downloader::budget_scheduler::RequestedSource::User(dl_threads)
    };
    let up_req = if auto {
        crate::downloader::budget_scheduler::RequestedSource::Auto
    } else {
        crate::downloader::budget_scheduler::RequestedSource::User(up_threads)
    };

    // Step 1: 写入 AccountManager（持久化到 accounts.json）
    {
        let mut mgr = state.account_manager.lock().await;
        mgr.add_user(user).await?;
    }

    // Step 2: 注册到 BudgetScheduler
    state
        .budget_scheduler
        .add_account(uid, vip, dl_req, up_req)
        .await;
    info!(
        "add_account_and_activate: uid={} 已注册到 BudgetScheduler (vip={:?}, auto={}, dl_req={:?}, up_req={:?})",
        uid.raw(), vip, auto, dl_req, up_req
    );

    // Step 3: 切换活跃账号（唯一入口 → 持久化镜像 + 广播 Switched）
    set_active_uid(state, Some(uid)).await?;

    // Step 4: 广播账号列表变更
    broadcast_account_list_changed(state).await;

    // Step 5: 推送 BudgetRecomputed（让前端 BudgetPanel 立即看到新账号配额）
    state.broadcast_budget_recomputed().await;

    info!("add_account_and_activate: uid={} 已激活", uid.raw());
    Ok(())
}

/// 登出 / 删除账号（force 契约）。
///
/// `force = true`：调用 `force_delete_account` 编排，关闭所有 per-uid Manager。
/// `force = false`：当前实现也走 force 路径（保留兼容旧版二态语义）。
///
/// **NOTE**：当 `uid_to_delete = None` 表示「使用当前 active_uid」；
/// 若同时 `active_uid = None`，返回错误（无账号可删）。
pub async fn delete_account_helper(
    state: &AppState,
    uid_to_delete: Option<Uid>,
    _force: bool,
) -> anyhow::Result<Option<Uid>> {
    let target = match uid_to_delete {
        Some(uid) => uid,
        None => state
            .active_uid
            .read()
            .await
            .ok_or_else(|| anyhow::anyhow!("无活跃账号，无法登出"))?,
    };

    // 调用 AppState 编排
    state.force_delete_account(target).await?;

    // 读取新 active_uid（force_delete_account 内部已通过 set_active_uid 写入并广播 Switched）
    let new_active = state.account_manager.lock().await.active_uid();

    // 🔥 `force_delete_account` 已通过唯一入口 `set_active_uid`
    // 广播 `Switched` + 重绑 CloudDl WS 订阅；这里只补 `ListChanged`，避免重复广播 `Switched`。
    broadcast_account_list_changed(state).await;

    Ok(new_active)
}
