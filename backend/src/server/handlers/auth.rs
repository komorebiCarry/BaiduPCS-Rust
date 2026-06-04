// 认证API处理器

use crate::auth::{AccountSummary, CookieLoginApiRequest, CookieLoginAuth, QRCode, QRCodeStatus};
use crate::common::ProxyType;
use crate::server::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
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
    #[serde(default)]
    pub force_login: bool,
}

/// 查询扫码状态
///
/// GET /api/v1/auth/qrcode/status?sign=xxx
pub async fn qrcode_status(
    State(state): State<AppState>,
    Query(params): Query<QRCodeStatusQuery>,
) -> Result<Json<ApiResponse<QRCodeStatus>>, StatusCode> {
    info!("API: 查询扫码状态: sign={}", params.sign);

    // 防呆：检查是否已有有效的持久化会话。添加新账号时由前端传 force_login=true 跳过。
    if !params.force_login {
        let mut session = state.session_manager.lock().await;
        if let Ok(Some(user)) = session.get_session().await {
            info!(
                "检测到已有持久化会话: UID={}, 验证 BDUSS 有效性...",
                user.uid
            );

            match state
                .qrcode_auth
                .read()
                .await
                .verify_bduss(&user.bduss)
                .await
            {
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
                    warn!("⚠️ 持久化的 BDUSS 已失效，移除当前账号，继续扫码流程");
                    let _ = session.clear_active_session().await;
                    drop(session);
                    state.clear_active_runtime().await;
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
                {
                    let mut session = state.session_manager.lock().await;
                    if let Err(e) = session.save_session(user).await {
                        error!("❌ 保存会话失败: {}", e);
                        return Ok(Json(ApiResponse::success(status)));
                    }
                    info!(
                        "✅ 会话保存成功: UID={}, BDUSS长度={}",
                        user.uid,
                        user.bduss.len()
                    );
                }

                if let Err(e) = state.load_initial_session().await {
                    error!("❌ 登录后初始化用户资源失败: {}", e);
                } else {
                    info!("✅ 登录后用户资源初始化成功");
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
            match state
                .qrcode_auth
                .read()
                .await
                .verify_bduss(&user.bduss)
                .await
            {
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
                    warn!("BDUSS 已失效，移除当前账号");
                    let next_user = session.clear_active_session().await.ok().flatten();
                    drop(session);
                    state.clear_active_runtime().await;
                    if next_user.is_some() {
                        if let Err(e) = state.load_initial_session().await {
                            error!("激活下一个账号失败: {}", e);
                        } else if let Some(active_user) = state.current_user.read().await.clone() {
                            return Ok(Json(ApiResponse::success(active_user)));
                        }
                    }
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

#[derive(Debug, Serialize)]
pub struct AccountListResponse {
    pub active_uid: Option<u64>,
    pub accounts: Vec<AccountSummary>,
}

#[derive(Debug, Serialize)]
pub struct AccountSwitchResponse {
    pub user: crate::auth::UserAuth,
    pub accounts: Vec<AccountSummary>,
}

#[derive(Debug, Serialize)]
pub struct AccountLogoutResponse {
    pub active_user: Option<crate::auth::UserAuth>,
    pub accounts: Vec<AccountSummary>,
}

async fn account_list_response(state: &AppState) -> Result<AccountListResponse, String> {
    let mut session = state.session_manager.lock().await;
    let active_uid = session
        .active_uid()
        .await
        .map_err(|e| format!("获取当前账号失败: {}", e))?;
    let accounts = session
        .list_accounts()
        .await
        .map_err(|e| format!("获取账号列表失败: {}", e))?;

    Ok(AccountListResponse {
        active_uid,
        accounts,
    })
}

/// 列出已保存的百度账号
///
/// GET /api/v1/auth/accounts
pub async fn list_accounts(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 获取百度账号列表");

    match account_list_response(&state).await {
        Ok(resp) => Ok(Json(ApiResponse::success(resp))),
        Err(e) => {
            error!("{}", e);
            Ok(Json(ApiResponse::error(500, e)))
        }
    }
}

/// 切换当前百度账号
///
/// POST /api/v1/auth/accounts/:uid/switch
pub async fn switch_account(
    State(state): State<AppState>,
    Path(uid): Path<u64>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 切换百度账号: UID={}", uid);

    let switched_uid = {
        let mut session = state.session_manager.lock().await;
        match session.switch_session(uid).await {
            Ok(Some(user)) => Some(user.uid),
            Ok(None) => {
                return Ok(Json(ApiResponse::<AccountSwitchResponse>::error(
                    404,
                    format!("账号不存在: {}", uid),
                )));
            }
            Err(e) => {
                error!("切换账号失败: {}", e);
                return Ok(Json(ApiResponse::<AccountSwitchResponse>::error(
                    500,
                    format!("切换账号失败: {}", e),
                )));
            }
        }
    };

    if switched_uid.is_some() {
        if let Err(e) = state.load_initial_session().await {
            error!("切换账号后初始化资源失败: {}", e);
            return Ok(Json(ApiResponse::<AccountSwitchResponse>::error(
                500,
                format!("切换账号后初始化资源失败: {}", e),
            )));
        }

        let accounts = account_list_response(&state)
            .await
            .map(|r| r.accounts)
            .unwrap_or_default();
        let user = state
            .current_user
            .read()
            .await
            .clone()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Json(ApiResponse::success(AccountSwitchResponse {
            user,
            accounts,
        })))
    } else {
        Ok(Json(ApiResponse::<AccountSwitchResponse>::error(
            404,
            format!("账号不存在: {}", uid),
        )))
    }
}

/// 移除指定百度账号
///
/// DELETE /api/v1/auth/accounts/:uid
pub async fn remove_account(
    State(state): State<AppState>,
    Path(uid): Path<u64>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 移除百度账号: UID={}", uid);

    let next_user = {
        let mut session = state.session_manager.lock().await;
        match session.remove_session(uid).await {
            Ok(next) => next,
            Err(e) => {
                error!("移除账号失败: {}", e);
                return Ok(Json(ApiResponse::<AccountLogoutResponse>::error(
                    404,
                    format!("{}", e),
                )));
            }
        }
    };

    state.clear_active_runtime().await;
    if next_user.is_some() {
        if let Err(e) = state.load_initial_session().await {
            error!("移除账号后初始化新账号失败: {}", e);
        }
    }

    let active_user = state.current_user.read().await.clone();
    let accounts = account_list_response(&state)
        .await
        .map(|r| r.accounts)
        .unwrap_or_default();

    Ok(Json(ApiResponse::success(AccountLogoutResponse {
        active_user,
        accounts,
    })))
}

/// 登出
///
/// POST /api/v1/auth/logout
pub async fn logout(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    info!("API: 当前百度账号登出");

    let next_user = {
        let mut session = state.session_manager.lock().await;
        match session.clear_active_session().await {
            Ok(next) => next,
            Err(e) => {
                error!("登出失败: {}", e);
                return Ok(Json(ApiResponse::<AccountLogoutResponse>::error(
                    500,
                    format!("Failed to logout: {}", e),
                )));
            }
        }
    };

    state.clear_active_runtime().await;
    if next_user.is_some() {
        if let Err(e) = state.load_initial_session().await {
            error!("登出后初始化下一个账号失败: {}", e);
        }
    }

    let active_user = state.current_user.read().await.clone();
    let accounts = account_list_response(&state)
        .await
        .map(|r| r.accounts)
        .unwrap_or_default();

    info!(
        "当前账号登出完成，剩余账号数: {}, active_uid={:?}",
        accounts.len(),
        active_user.as_ref().map(|u| u.uid)
    );

    Ok(Json(ApiResponse::success(AccountLogoutResponse {
        active_user,
        accounts,
    })))
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
