// 认证API处理器

use crate::auth::{CookieLoginAuth, CookieLoginApiRequest, QRCode, QRCodeStatus};
use crate::common::ProxyType;
use crate::server::AppState;
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
}

/// 查询扫码状态
///
/// GET /api/v1/auth/qrcode/status?sign=xxx
pub async fn qrcode_status(
    State(state): State<AppState>,
    Query(params): Query<QRCodeStatusQuery>,
) -> Result<Json<ApiResponse<QRCodeStatus>>, StatusCode> {
    info!("API: 查询扫码状态: sign={}", params.sign);

    // 防呆：检查是否已有有效的持久化会话
    {
        let mut session = state.session_manager.lock().await;
        if let Ok(Some(user)) = session.get_session().await {
            info!(
                "检测到已有持久化会话: UID={}, 验证 BDUSS 有效性...",
                user.uid
            );

            match state.qrcode_auth.read().await.verify_bduss(&user.bduss).await {
                Ok(true) => {
                    info!("✅ BDUSS 仍然有效，直接返回登录成功状态");

                    // 确保客户端已初始化
                    let client_initialized = state.netdisk_client.read().await.is_some();
                    if !client_initialized {
                        info!("🔄 客户端未初始化，开始初始化用户资源...");
                        drop(session); // 释放锁避免死锁
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
                    warn!("⚠️ 持久化的 BDUSS 已失效，清除会话，继续扫码流程");
                    let _ = session.clear_session().await;
                }
                Err(e) => {
                    warn!("⚠️ BDUSS 验证出错: {}，继续扫码流程", e);
                }
            }
        }
    }

    match state.qrcode_auth.read().await.poll_status(&params.sign).await {
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
                state.fallback_mgr
                    .set_user_proxy_config(proxy_config.clone())
                    .await;

                let fallback_for_client = if proxy_config.is_some() {
                    Some(Arc::clone(&state.fallback_mgr))
                } else {
                    None
                };
                let client = match crate::netdisk::NetdiskClient::new_with_proxy(user.clone(), proxy_config.as_ref(), fallback_for_client.clone()) {
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
                let config = state.config.read().await;
                let download_dir = config.download.download_dir.clone();
                let max_global_threads = config.download.max_global_threads;
                let max_concurrent_tasks = config.download.max_concurrent_tasks;
                let max_retries = config.download.max_retries;
                let upload_config = config.upload.clone();
                let transfer_config = config.transfer.clone();
                drop(config);

                // 获取持久化管理器引用
                let pm_arc = Arc::clone(&state.persistence_manager);

                match crate::downloader::DownloadManager::with_config(
                    updated_user.clone(),
                    download_dir,
                    max_global_threads,
                    max_concurrent_tasks,
                    max_retries,
                    proxy_config.as_ref(),
                    fallback_for_client,
                ) {
                    Ok(mut manager) => {
                        // 设置持久化管理器
                        manager.set_persistence_manager(Arc::clone(&pm_arc));

                        // 设置 WebSocket 管理器
                        manager.set_ws_manager(Arc::clone(&state.ws_manager)).await;

                        let manager_arc = Arc::new(manager);
                        *state.download_manager.write().await = Some(Arc::clone(&manager_arc));

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
                        let config_dir = std::path::Path::new("config");
                        let upload_manager =
                            UploadManager::new_with_config(client.clone(), &updated_user, &upload_config, config_dir);
                        let upload_manager_arc = Arc::new(upload_manager);

                        // 设置上传管理器的持久化管理器
                        upload_manager_arc
                            .set_persistence_manager(Arc::clone(&pm_arc))
                            .await;

                        // 设置上传管理器的 WebSocket 管理器
                        upload_manager_arc
                            .set_ws_manager(Arc::clone(&state.ws_manager))
                            .await;

                        // 🔥 设置备份记录管理器（用于文件夹名加密映射）
                        upload_manager_arc
                            .set_backup_record_manager(Arc::clone(&state.backup_record_manager))
                            .await;

                        *state.upload_manager.write().await = Some(Arc::clone(&upload_manager_arc));
                        info!("✅ 上传管理器初始化成功");

                        // 初始化转存管理器
                        let transfer_manager = TransferManager::new(
                            Arc::new(std::sync::RwLock::new(client)),
                            transfer_config,
                            Arc::clone(&state.config),
                        );
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

                        *state.transfer_manager.write().await = Some(Arc::clone(&transfer_manager_arc));
                        info!("✅ 转存管理器初始化成功");

                        // 启动 WebSocket 批量发送器
                        Arc::clone(&state.ws_manager).start_batch_sender();
                        info!("✅ WebSocket 批量发送器已启动");

                        // 🔥 启动内存监控器
                        Arc::clone(&state.memory_monitor).start();
                        info!("✅ 内存监控器已启动");

                        // 🔥 初始化自动备份管理器
                        state.init_autobackup_manager().await;
                        info!("✅ 自动备份管理器初始化完成");

                        // 🔥 初始化离线下载监听服务
                        state.init_cloud_dl_monitor().await;
                        info!("✅ 离线下载监听服务初始化完成");
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
pub async fn get_current_user(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("🔍 API: 获取当前用户信息");

    let mut session = state.session_manager.lock().await;

    match session.get_session().await {
        Ok(Some(user)) => {
            info!("✅ 找到会话: UID={}, 用户名={}", user.uid, user.username);

            // 验证 BDUSS 是否仍然有效
            match state.qrcode_auth.read().await.verify_bduss(&user.bduss).await {
                Ok(true) => {
                    // BDUSS 有效，检查客户端是否已初始化
                    info!("BDUSS 验证通过");

                    // 检查网盘客户端是否已初始化
                    let client_initialized = state.netdisk_client.read().await.is_some();
                    if !client_initialized {
                        info!("🔄 检测到客户端未初始化，开始初始化用户资源...");
                        // 释放 session 锁，避免死锁
                        drop(session);

                        // 调用初始化逻辑
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
                    // BDUSS 已失效，清除会话
                    warn!("BDUSS 已失效，清除会话");
                    let _ = session.clear_session().await;
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
        Ok(None) => {
            warn!("❌ 未找到会话，用户未登录");
            Ok(Json(ApiResponse::error(401, "Not logged in".to_string())))
        }
        Err(e) => {
            error!("获取会话失败: {}", e);
            Ok(Json(ApiResponse::error(
                500,
                format!("Failed to get session: {}", e),
            )))
        }
    }
}

/// 登出
///
/// POST /api/v1/auth/logout
pub async fn logout(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 用户登出");

    // 1. 清除持久化 session（文件 + 内存缓存）
    let clear_result = {
        let mut session = state.session_manager.lock().await;
        session.clear_session().await
    };

    // 2. 清除内存中的当前用户，确保下次登录时不携带旧状态
    *state.current_user.write().await = None;

    // 3. 清除网盘客户端（含旧 Cookie Jar），下次登录时重新创建
    *state.netdisk_client.write().await = None;

    info!("已清除 current_user 和 netdisk_client");

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
    let final_user = state
        .current_user
        .read()
        .await
        .clone()
        .unwrap_or(user);

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
        warn!("⚠️ Cookie 登录完成，但预热未成功: ptoken={}, panpsc={}",
            has_ptoken, final_user.panpsc.is_some());
    }

    Ok(Json(ApiResponse::success_with_message(final_user, message)))
}
