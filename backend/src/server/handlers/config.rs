// 配置管理 API

use crate::config::{
    AppConfig, DownloadConfig, PathValidationResult, ShareSyncConfig, VipRecommendedConfig,
    VipType, APP_CONFIG_PATH,
};
use crate::server::error::{ApiError, ApiResult};
use axum::{extract::State, response::Json};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

use super::ApiResponse;

/// 推荐配置响应
#[derive(Debug, Serialize)]
pub struct RecommendedConfigResponse {
    pub vip_type: u32,
    pub vip_name: String,
    pub recommended: VipRecommendedConfig,
    pub warnings: Vec<String>,
}

/// 配置更新响应
#[derive(Debug, Serialize)]
pub struct ConfigUpdateResponse {
    pub message: String,
    pub path_validation: PathValidationResult,
}

/// GET /api/v1/config
/// 获取当前配置
pub async fn get_config(
    State(app_state): State<crate::server::AppState>,
) -> ApiResult<Json<ApiResponse<AppConfig>>> {
    let config = app_state.config.read().await.clone();
    Ok(Json(ApiResponse::success(config)))
}

/// GET /api/v1/config/recommended
/// 获取当前用户的推荐配置
pub async fn get_recommended_config(
    State(app_state): State<crate::server::AppState>,
) -> ApiResult<Json<ApiResponse<RecommendedConfigResponse>>> {
    // 获取当前用户的 VIP 类型
    let current_user = app_state.current_user.read().await;
    let vip_type_value = current_user.as_ref().and_then(|u| u.vip_type).unwrap_or(0);
    let vip_type = VipType::from_u32(vip_type_value);
    drop(current_user);

    let vip_name = match vip_type {
        VipType::Normal => "普通用户",
        VipType::Vip => "普通会员",
        VipType::Svip => "超级会员",
    }
    .to_string();

    // 获取推荐配置
    let recommended = DownloadConfig::recommended_for_vip(vip_type);

    // 获取当前配置并生成警告
    let current_config = app_state.config.read().await;
    let mut warnings = Vec::new();

    if let Err(err) = current_config.download.validate_for_vip(vip_type) {
        warnings.push(err);
    }

    info!(
        "获取推荐配置: VIP类型={}, 推荐线程数={}",
        vip_name, recommended.threads
    );

    Ok(Json(ApiResponse::success(RecommendedConfigResponse {
        vip_type: vip_type_value,
        vip_name,
        recommended,
        warnings,
    })))
}

/// POST /api/v1/config/reset
/// 恢复为推荐的默认配置
pub async fn reset_to_recommended(
    State(app_state): State<crate::server::AppState>,
) -> ApiResult<Json<ApiResponse<String>>> {
    info!("恢复推荐配置");

    // 获取当前用户的 VIP 类型
    let current_user = app_state.current_user.read().await;
    let vip_type_value = current_user.as_ref().and_then(|u| u.vip_type).unwrap_or(0);
    let vip_type = VipType::from_u32(vip_type_value);
    drop(current_user);

    // 应用推荐配置：持写锁完成「改 → 落盘 → 回写内存」，避免与其他保存交错丢更新
    let config = {
        let mut guard = app_state.config.write().await;
        let mut config = guard.clone();
        config.download.apply_recommended(vip_type);

        config
            .save_to_file(APP_CONFIG_PATH)
            .await
            .map_err(ApiError::Internal)?;

        *guard = config.clone();
        config
    };

    // 🔧 动态更新下载管理器配置
    //
    // 按每账号 effective 配置（基于 custom_config + auto_apply_recommended）更新，
    // 避免覆盖账号级配置。
    for (uid, manager) in app_state.list_download_managers() {
        let user_for_uid: Option<crate::auth::UserAuth> = {
            let am = app_state.account_manager.lock().await;
            am.get_user(uid).cloned()
        };
        let (eff_dl_threads, eff_dl_concurrent, _eff_dl_retries, _, _, _) =
            if let Some(ref user) = user_for_uid {
                crate::server::state::resolve_effective_account_config(user, &config)
            } else {
                (
                    config.download.max_global_threads,
                    config.download.max_concurrent_tasks,
                    config.download.max_retries,
                    config.upload.max_global_threads,
                    config.upload.max_concurrent_tasks,
                    config.upload.max_retries,
                )
            };
        manager.update_max_threads(eff_dl_threads);
        manager.update_max_concurrent_tasks(eff_dl_concurrent).await;
        manager
            .update_download_dir(config.download.download_dir.clone())
            .await;
        info!(
            "✓ 下载管理器已更新为推荐配置 (uid={}): 线程数={}, 最大任务数={}, 下载目录={:?}",
            uid.raw(),
            eff_dl_threads,
            eff_dl_concurrent,
            config.download.download_dir
        );
    }

    // 🔧 动态更新文件夹下载管理器的下载目录
    app_state
        .folder_download_manager
        .update_download_dir(config.download.download_dir.clone())
        .await;

    // 🔧 动态更新上传管理器配置（按每账号 effective 配置）
    for (uid, upload_manager) in app_state.list_upload_managers() {
        let user_for_uid: Option<crate::auth::UserAuth> = {
            let am = app_state.account_manager.lock().await;
            am.get_user(uid).cloned()
        };
        let (_, _, _, eff_up_threads, eff_up_concurrent, eff_up_retries) =
            if let Some(ref user) = user_for_uid {
                crate::server::state::resolve_effective_account_config(user, &config)
            } else {
                (
                    config.download.max_global_threads,
                    config.download.max_concurrent_tasks,
                    config.download.max_retries,
                    config.upload.max_global_threads,
                    config.upload.max_concurrent_tasks,
                    config.upload.max_retries,
                )
            };
        upload_manager.update_max_threads(eff_up_threads);
        upload_manager
            .update_max_concurrent_tasks(eff_up_concurrent)
            .await;
        upload_manager.update_max_retries(eff_up_retries);
        info!(
            "✓ 上传管理器已更新为推荐配置 (uid={}): 线程数={}, 最大任务数={}, 最大重试={}",
            uid.raw(),
            eff_up_threads,
            eff_up_concurrent,
            eff_up_retries
        );
    }

    info!("已恢复为推荐配置: VIP类型={:?}", vip_type);
    Ok(Json(ApiResponse::success("已恢复为推荐配置".to_string())))
}

/// PUT /api/v1/config
/// 更新配置
pub async fn update_config(
    State(app_state): State<crate::server::AppState>,
    Json(raw): Json<serde_json::Value>,
) -> ApiResult<Json<ApiResponse<ConfigUpdateResponse>>> {
    info!("更新应用配置");

    let mut new_config: AppConfig = serde_json::from_value(raw.clone())
        .map_err(|e| ApiError::BadRequest(format!("配置格式不合法: {}", e)))?;

    // 基本验证
    if new_config.download.max_global_threads == 0 {
        return Err(ApiError::BadRequest(
            "download.max_global_threads 必须大于 0".to_string(),
        ));
    }

    if new_config.download.chunk_size_mb == 0 {
        return Err(ApiError::BadRequest(
            "download.chunk_size_mb 必须大于 0".to_string(),
        ));
    }

    if new_config.download.max_concurrent_tasks == 0 {
        return Err(ApiError::BadRequest(
            "download.max_concurrent_tasks 必须大于 0".to_string(),
        ));
    }

    // 补齐 upload 侧校验（避免 upload 接受 0 会导致上传分片调度阀门关闭，所有上传永久阻塞）。
    if new_config.upload.max_global_threads == 0 {
        return Err(ApiError::BadRequest(
            "upload.max_global_threads 必须大于 0".to_string(),
        ));
    }
    if new_config.upload.chunk_size_mb == 0 {
        return Err(ApiError::BadRequest(
            "upload.chunk_size_mb 必须大于 0".to_string(),
        ));
    }
    if new_config.upload.max_concurrent_tasks == 0 {
        return Err(ApiError::BadRequest(
            "upload.max_concurrent_tasks 必须大于 0".to_string(),
        ));
    }

    // 获取当前用户的 VIP 类型并验证
    let current_user = app_state.current_user.read().await;
    let vip_type_value = current_user.as_ref().and_then(|u| u.vip_type).unwrap_or(0);
    let vip_type = VipType::from_u32(vip_type_value);
    drop(current_user);

    // 验证配置安全性（生成警告但不阻止）
    if let Err(warning) = new_config.download.validate_for_vip(vip_type) {
        warn!("配置验证警告: {}", warning);
        // 注意：这里只是警告，不阻止用户设置，因为用户可能有特殊需求
    }

    // 代理配置验证
    if let Err(e) = new_config.network.proxy.validate() {
        return Err(ApiError::BadRequest(format!("代理配置验证失败: {}", e)));
    }

    let (validation_result, proxy_changed) = {
        let mut guard = app_state.config.write().await;

        // ⚠️ 时序关键：在覆盖配置之前读取旧代理配置，用于变更检测
        let proxy_changed = guard.network.proxy != new_config.network.proxy;

        // Web 认证开关只能经 /web-auth/config 修改（需校验密码 / TOTP 已设置）。
        // 设置页会把 GET /config 拿到的整份配置原样提交回来，这里一律以服务端
        // 当前值为准，防止旧值或伪造值关闭认证（issue #177）
        new_config.web_auth = guard.web_auth.clone();

        preserve_missing_regex_presets(&raw, &mut new_config, &guard.share_sync);
        if let Err(e) = new_config.share_sync.validate() {
            return Err(ApiError::BadRequest(format!(
                "分享同步排除规则校验失败: {}",
                e
            )));
        }

        // 保存到文件（包含完整的路径验证）
        let validation_result = new_config
            .save_to_file(APP_CONFIG_PATH)
            .await
            .map_err(ApiError::Internal)?;

        // 更新内存中的配置
        *guard = new_config.clone();
        (validation_result, proxy_changed)
    };

    // 🔧 动态更新下载/上传管理器配置
    //
    // 按每账号 `UserAuth.custom_config` 重新计算 effective 配置后，用每账号自己的值更新 manager。
    //
    // 全局变量：仅 `download_dir` 是真正"机器级"配置（用户希望全账号一致），保留
    // 原行为对所有 manager 生效；线程数 / 最大任务数 / 重试次数三个均按账号 effective
    // 走 — 这等价于"全局配置仅影响那些 `auto_apply_recommended=false` 且未设自定义
    // 字段的账号"（因为它们 effective 仍走 custom_config 默认值）。
    {
        let dms = app_state.list_download_managers();
        if dms.is_empty() {
            info!("下载管理器未初始化，配置将在下次登录时生效");
        } else {
            for (uid, manager) in dms {
                let user_for_uid: Option<crate::auth::UserAuth> = {
                    let am = app_state.account_manager.lock().await;
                    am.get_user(uid).cloned()
                };
                let (eff_dl_threads, eff_dl_concurrent, _eff_dl_retries, _, _, _) =
                    if let Some(ref user) = user_for_uid {
                        crate::server::state::resolve_effective_account_config(user, &new_config)
                    } else {
                        // 没拿到 UserAuth → 退回全局值（极端情况）
                        (
                            new_config.download.max_global_threads,
                            new_config.download.max_concurrent_tasks,
                            new_config.download.max_retries,
                            new_config.upload.max_global_threads,
                            new_config.upload.max_concurrent_tasks,
                            new_config.upload.max_retries,
                        )
                    };
                manager.update_max_threads(eff_dl_threads);
                manager.update_max_concurrent_tasks(eff_dl_concurrent).await;
                manager
                    .update_download_dir(new_config.download.download_dir.clone())
                    .await;
                info!(
                    "✓ 下载管理器配置已动态更新 (uid={}): 线程数={}, 最大任务数={}, 下载目录={:?}",
                    uid.raw(),
                    eff_dl_threads,
                    eff_dl_concurrent,
                    new_config.download.download_dir
                );
            }
        }
    }

    // 🔧 动态更新文件夹下载管理器的下载目录
    app_state
        .folder_download_manager
        .update_download_dir(new_config.download.download_dir.clone())
        .await;
    info!(
        "✓ 文件夹下载管理器下载目录已更新: {:?}",
        new_config.download.download_dir
    );

    // 🔧 动态更新上传管理器配置（按账号 effective 配置）
    {
        let ums = app_state.list_upload_managers();
        if ums.is_empty() {
            info!("上传管理器未初始化，配置将在下次登录时生效");
        } else {
            for (uid, upload_manager) in ums {
                let user_for_uid: Option<crate::auth::UserAuth> = {
                    let am = app_state.account_manager.lock().await;
                    am.get_user(uid).cloned()
                };
                let (_, _, _, eff_up_threads, eff_up_concurrent, eff_up_retries) =
                    if let Some(ref user) = user_for_uid {
                        crate::server::state::resolve_effective_account_config(user, &new_config)
                    } else {
                        (
                            new_config.download.max_global_threads,
                            new_config.download.max_concurrent_tasks,
                            new_config.download.max_retries,
                            new_config.upload.max_global_threads,
                            new_config.upload.max_concurrent_tasks,
                            new_config.upload.max_retries,
                        )
                    };
                upload_manager.update_max_threads(eff_up_threads);
                upload_manager
                    .update_max_concurrent_tasks(eff_up_concurrent)
                    .await;
                upload_manager.update_max_retries(eff_up_retries);
                info!(
                    "✓ 上传管理器配置已动态更新 (uid={}): 线程数={}, 最大任务数={}, 最大重试={}",
                    uid.raw(),
                    eff_up_threads,
                    eff_up_concurrent,
                    eff_up_retries
                );
            }
        }
    }

    // 🔧 代理配置热更新（如果代理配置发生变更）
    if proxy_changed {
        let proxy = &new_config.network.proxy;
        info!("代理配置已变更，执行热更新...");

        // 0. 通知 FallbackManager 用户手动修改了代理配置（取消探测、重置状态）
        let new_proxy_config = if proxy.proxy_type != crate::common::ProxyType::None {
            Some(proxy.clone())
        } else {
            None
        };
        app_state
            .fallback_mgr
            .on_user_config_change(new_proxy_config.as_ref())
            .await;

        // 准备代理参数（QRCodeAuth、NetdiskClient、DownloadEngine 共用）
        let proxy_for_client = if proxy.proxy_type != crate::common::ProxyType::None {
            Some(proxy)
        } else {
            None
        };
        let fallback_for_client = if proxy.proxy_type != crate::common::ProxyType::None {
            Some(Arc::clone(&app_state.fallback_mgr))
        } else {
            None
        };

        // 1. 重建 QRCodeAuth
        match crate::auth::QRCodeAuth::new_with_proxy(proxy_for_client) {
            Ok(new_auth) => {
                *app_state.qrcode_auth.write().await = new_auth;
                info!("✓ QRCodeAuth 已热更新");
            }
            Err(e) => warn!("QRCodeAuth 热更新失败: {}", e),
        }

        // 2. 重建 NetdiskClient（如果已登录）
        // 按 active_uid → AccountManager 取，不再读 current_user
        let user_auth = app_state.active_user_auth().await;
        if let Some(user) = user_auth {
            match crate::netdisk::NetdiskClient::new_with_proxy(
                user,
                proxy_for_client,
                fallback_for_client.clone(),
            ) {
                Ok(new_client) => {
                    *app_state.netdisk_client.write().await = Some(new_client);
                    info!("✓ NetdiskClient 已热更新");
                }
                Err(e) => warn!("NetdiskClient 热更新失败: {}", e),
            }
        }

        // 3. 更新 DownloadEngine 代理配置和共享客户端
        //
        // 遍历全部已登录 per-uid DownloadManager（按 `Arc::ptr_eq` 去重，per-uid
        // 独立实例下每个 Arc 只出现一次）。代理是机器级配置，所有账号
        // 的 DownloadEngine 都应该走同一代理。
        for (uid, dm) in app_state.list_download_managers() {
            dm.update_proxy_config(proxy_for_client);
            info!(
                "✓ DownloadEngine uid={} 代理配置和共享客户端已热更新",
                uid.raw()
            );
        }

        // 4. 更新 UploadManager 共享 NetdiskClient（已调度的上传任务在下次重试时自动生效）
        //
        // 遍历所有 per-uid UploadManager，并按各自
        // `UserAuth` 重建 client（每账号 cookies/bduss 不同，不能共用一个 user）。
        for (uid, um) in app_state.list_upload_managers() {
            let user_for_uid: Option<crate::auth::UserAuth> = {
                let am = app_state.account_manager.lock().await;
                am.get_user(uid).cloned()
            };
            let Some(user) = user_for_uid else {
                warn!(
                    "UploadManager uid={} 代理热更新跳过：无法取到 UserAuth",
                    uid.raw()
                );
                continue;
            };
            match crate::netdisk::NetdiskClient::new_with_proxy(
                user,
                proxy_for_client,
                fallback_for_client.clone(),
            ) {
                Ok(new_client) => {
                    um.update_netdisk_client(new_client);
                    info!("✓ UploadManager uid={} NetdiskClient 已热更新", uid.raw());
                }
                Err(e) => warn!(
                    "UploadManager uid={} NetdiskClient 热更新失败: {}",
                    uid.raw(),
                    e
                ),
            }
        }

        // 5. 更新 TransferManager 共享 NetdiskClient
        //
        // 遍历所有 per-uid TransferManager，按各自 `UserAuth` 重建 client。
        for (uid, tm) in app_state.list_transfer_managers() {
            let user_for_uid: Option<crate::auth::UserAuth> = {
                let am = app_state.account_manager.lock().await;
                am.get_user(uid).cloned()
            };
            let Some(user) = user_for_uid else {
                warn!(
                    "TransferManager uid={} 代理热更新跳过：无法取到 UserAuth",
                    uid.raw()
                );
                continue;
            };
            match crate::netdisk::NetdiskClient::new_with_proxy(
                user,
                proxy_for_client,
                fallback_for_client.clone(),
            ) {
                Ok(new_client) => {
                    tm.update_netdisk_client(new_client);
                    info!("✓ TransferManager uid={} NetdiskClient 已热更新", uid.raw());
                }
                Err(e) => warn!(
                    "TransferManager uid={} NetdiskClient 热更新失败: {}",
                    uid.raw(),
                    e
                ),
            }
        }

        // 5.5) 同步刷新 client_pool，让后续按 uid 取 client 的代码路径
        //      （e.g. 各 manager `new_for_account` / 创建任务时按 uid 取 client）
        //      也使用新代理后的客户端。
        {
            let am = app_state.account_manager.lock().await;
            let users = am.list_users().to_vec();
            drop(am);
            for user in users {
                let uid = crate::auth::Uid::new(user.uid);
                match crate::netdisk::NetdiskClient::new_with_proxy(
                    user,
                    proxy_for_client,
                    fallback_for_client.clone(),
                ) {
                    Ok(new_client) => {
                        let mut pool = app_state.client_pool.write().await;
                        pool.add_client(uid, std::sync::Arc::new(new_client));
                        info!("✓ ClientPool uid={} NetdiskClient 已热更新", uid.raw());
                    }
                    Err(e) => warn!(
                        "ClientPool uid={} NetdiskClient 热更新失败: {}",
                        uid.raw(),
                        e
                    ),
                }
            }
        }

        // 6. 更新 CloudDlMonitor 共享 NetdiskClient（遍历所有账号）
        // 严格按 monitor 的 uid 取该账号 UserAuth；不再 fallback
        // 到 current_user（活跃账号），否则会把 uid=A 的 cookie 注入 uid=B 的 monitor
        // 导致后续轮询/事件串账号。
        for (uid, monitor) in app_state.iter_cloud_dl_monitors() {
            let user_auth: Option<crate::auth::UserAuth> = {
                let am = app_state.account_manager.lock().await;
                am.get_user(uid).cloned()
            };

            let Some(user) = user_auth else {
                warn!(
                    "CloudDlMonitor uid={} 代理热更新跳过：AccountManager 中无该账号（已删除？）",
                    uid.raw()
                );
                continue;
            };

            match crate::netdisk::NetdiskClient::new_with_proxy(
                user,
                proxy_for_client,
                fallback_for_client.clone(),
            ) {
                Ok(new_client) => {
                    monitor.update_client(new_client);
                    info!("✓ CloudDlMonitor uid={} NetdiskClient 已热更新", uid.raw());
                }
                Err(e) => warn!(
                    "CloudDlMonitor uid={} NetdiskClient 热更新失败: {}",
                    uid.raw(),
                    e
                ),
            }
        }

        // 7. 更新 FolderDownloadManager 共享 NetdiskClient
        // 按 active_uid → AccountManager 取，不再读 current_user
        {
            let user_auth = app_state.active_user_auth().await;
            if let Some(user) = user_auth {
                match crate::netdisk::NetdiskClient::new_with_proxy(
                    user,
                    proxy_for_client,
                    fallback_for_client.clone(),
                ) {
                    Ok(new_client) => {
                        app_state
                            .folder_download_manager
                            .set_netdisk_client(Arc::new(new_client))
                            .await;
                        info!("✓ FolderDownloadManager NetdiskClient 已热更新");
                    }
                    Err(e) => warn!("FolderDownloadManager NetdiskClient 热更新失败: {}", e),
                }
            }
        }

        // 8. 主动探测代理连通性，避免无效代理显示为 Normal
        if let Some(ref proxy_cfg) = new_proxy_config {
            match crate::common::probe_proxy(proxy_cfg).await {
                Ok(()) => {
                    info!("✓ 代理连通性探测成功");
                    // on_user_config_change 已设为 Normal，无需再改
                }
                Err(e) => {
                    warn!("✗ 代理连通性探测失败: {}，标记为不可用", e);
                    if proxy_cfg.allow_fallback {
                        // 允许回退：直接触发回退流程
                        app_state.fallback_mgr.execute_fallback().await;
                    } else {
                        // 不允许回退：仅标记状态为异常，不切直连
                        app_state
                            .fallback_mgr
                            .set_runtime_status(
                                crate::common::ProxyRuntimeStatus::FallenBackToDirect,
                            )
                            .await;
                    }
                }
            }
        }
    }

    info!("配置更新成功");

    let response = ConfigUpdateResponse {
        message: "配置已更新".to_string(),
        path_validation: validation_result,
    };

    Ok(Json(ApiResponse::success(response)))
}

/// 更新最近目录请求
#[derive(Debug, Deserialize)]
pub struct UpdateRecentDirRequest {
    /// 目录类型: "download" 或 "upload"
    pub dir_type: String,
    /// 最近使用的目录路径
    pub path: String,
}

/// POST /api/v1/config/recent-dir
/// 更新最近使用的目录（下载/上传）
pub async fn update_recent_dir(
    State(app_state): State<crate::server::AppState>,
    Json(req): Json<UpdateRecentDirRequest>,
) -> ApiResult<Json<ApiResponse<String>>> {
    info!("更新最近目录: type={}, path={}", req.dir_type, req.path);

    // 验证路径
    let path = PathBuf::from(&req.path);
    if !path.is_absolute() {
        return Err(ApiError::BadRequest("路径必须是绝对路径".to_string()));
    }

    // 获取当前配置（持写锁直到回写内存，避免与其他保存交错丢更新）
    let mut guard = app_state.config.write().await;
    let mut config = guard.clone();

    // 根据类型更新对应的最近目录
    match req.dir_type.as_str() {
        "download" => {
            config.download.recent_directory = Some(path);
            info!("已更新下载最近目录: {:?}", config.download.recent_directory);
        }
        "upload" => {
            config.upload.recent_directory = Some(path);
            info!("已更新上传最近目录: {:?}", config.upload.recent_directory);
        }
        _ => {
            return Err(ApiError::BadRequest(format!(
                "无效的目录类型: {}，必须是 'download' 或 'upload'",
                req.dir_type
            )));
        }
    }

    // 保存到文件
    config
        .save_to_file(APP_CONFIG_PATH)
        .await
        .map_err(ApiError::Internal)?;

    // 更新内存中的配置
    *guard = config;

    Ok(Json(ApiResponse::success("最近目录已更新".to_string())))
}

/// 设置默认下载目录请求
#[derive(Debug, Deserialize)]
pub struct SetDefaultDirRequest {
    /// 默认下载目录路径
    pub path: String,
}

/// POST /api/v1/config/default-download-dir
/// 设置默认下载目录
pub async fn set_default_download_dir(
    State(app_state): State<crate::server::AppState>,
    Json(req): Json<SetDefaultDirRequest>,
) -> ApiResult<Json<ApiResponse<String>>> {
    info!("设置默认下载目录: {}", req.path);

    // 验证路径
    let path = PathBuf::from(&req.path);
    if !path.is_absolute() {
        return Err(ApiError::BadRequest("路径必须是绝对路径".to_string()));
    }

    // 验证路径存在
    if !path.exists() {
        return Err(ApiError::BadRequest(format!("目录不存在: {}", req.path)));
    }

    // 持写锁完成「改 → 落盘 → 回写内存」，避免与其他保存交错丢更新
    let config = {
        let mut guard = app_state.config.write().await;
        let mut config = guard.clone();
        config.download.default_directory = Some(path.clone());

        // 同时更新 download_dir（主下载目录）
        config.download.download_dir = path;

        // 保存到文件
        config
            .save_to_file(APP_CONFIG_PATH)
            .await
            .map_err(ApiError::Internal)?;

        // 更新内存中的配置
        *guard = config.clone();
        config
    };

    // 同步更新下载管理器的下载目录
    //
    // 遍历所有 per-uid manager。
    for (uid, manager) in app_state.list_download_managers() {
        manager
            .update_download_dir(config.download.download_dir.clone())
            .await;
        info!("✓ 下载管理器下载目录已更新 (uid={})", uid.raw());
    }

    // 同步更新文件夹下载管理器的下载目录
    app_state
        .folder_download_manager
        .update_download_dir(config.download.download_dir.clone())
        .await;

    Ok(Json(ApiResponse::success("默认下载目录已设置".to_string())))
}

/// 设置默认下载账号请求
#[derive(Debug, Deserialize)]
pub struct SetDefaultDownloadAccountRequest {
    /// 账号 UID；`null` 表示取消默认
    pub uid: Option<u64>,
}

/// POST /api/v1/config/default-download-account
/// 设置「用其他账号下载」默认选中的账号
pub async fn set_default_download_account(
    State(app_state): State<crate::server::AppState>,
    Json(req): Json<SetDefaultDownloadAccountRequest>,
) -> ApiResult<Json<ApiResponse<String>>> {
    info!("设置默认下载账号: {:?}", req.uid);

    if let Some(uid) = req.uid {
        let mgr = app_state.account_manager.lock().await;
        if mgr.get_user(crate::auth::Uid::new(uid)).is_none() {
            return Err(ApiError::BadRequest(format!(
                "账号不存在或已移除: UID {}",
                uid
            )));
        }
    }

    let mut guard = app_state.config.write().await;
    let mut config = guard.clone();
    config.download.default_download_account_uid = req.uid;

    config
        .save_to_file(APP_CONFIG_PATH)
        .await
        .map_err(ApiError::Internal)?;

    *guard = config;

    Ok(Json(ApiResponse::success("默认下载账号已设置".to_string())))
}

// ============================================
// 转存配置 API
// ============================================

/// 获取转存配置响应
#[derive(Debug, Serialize)]
pub struct TransferConfigResponse {
    /// 默认行为
    pub default_behavior: String,
    /// 最近使用的网盘目录 fs_id
    pub recent_save_fs_id: Option<u64>,
    /// 最近使用的网盘目录路径
    pub recent_save_path: Option<String>,
}

/// GET /api/v1/config/transfer
/// 获取转存配置
pub async fn get_transfer_config(
    State(app_state): State<crate::server::AppState>,
) -> ApiResult<Json<ApiResponse<TransferConfigResponse>>> {
    // ========== 第一段：只读 ==========
    let (default_behavior, recent_save_fs_id, recent_save_path) = {
        let config = app_state.config.read().await;
        let t = &config.transfer;

        (
            t.default_behavior.clone(),
            t.recent_save_fs_id,
            t.recent_save_path.clone(),
        )
    };

    let path_str = recent_save_path.as_deref().unwrap_or("");

    if path_str.is_empty() || path_str == "/" {
        return Ok(Json(ApiResponse::success(TransferConfigResponse {
            default_behavior: default_behavior.clone(),
            recent_save_fs_id,
            recent_save_path: recent_save_path.clone(),
        })));
    }

    // 多账号路由：按 active_uid 取客户端
    let client = match app_state.active_client().await {
        Some(c) => c,
        None => {
            return Ok(Json(ApiResponse::error(
                401,
                "未登录或客户端未初始化".to_string(),
            )));
        }
    };
    // 获取文件列表
    match client.get_file_list(path_str, 1, 1).await {
        Ok(_) => Ok(Json(ApiResponse::success(TransferConfigResponse {
            default_behavior: default_behavior.clone(),
            recent_save_fs_id,
            recent_save_path: recent_save_path.clone(),
        }))),
        Err(_) => {
            info!("转存获取文件列表失败，路径不存在，清空");
            let mut write_config = app_state.config.write().await;

            write_config.transfer.recent_save_fs_id = None;
            write_config.transfer.recent_save_path = None;

            // 保存到文件
            write_config
                .save_to_file(APP_CONFIG_PATH)
                .await
                .map_err(ApiError::Internal)?;

            Ok(Json(ApiResponse::success(TransferConfigResponse {
                default_behavior: default_behavior.clone(),
                recent_save_fs_id: None,
                recent_save_path: None,
            })))
        }
    }
}

/// 更新转存配置请求
#[derive(Debug, Deserialize)]
pub struct UpdateTransferConfigRequest {
    /// 默认行为: "transfer_only" 或 "transfer_and_download"
    pub default_behavior: Option<String>,
    /// 最近使用的网盘目录 fs_id
    pub recent_save_fs_id: Option<u64>,
    /// 最近使用的网盘目录路径
    pub recent_save_path: Option<String>,
}

/// PUT /api/v1/config/transfer
/// 更新转存配置
pub async fn update_transfer_config(
    State(app_state): State<crate::server::AppState>,
    Json(req): Json<UpdateTransferConfigRequest>,
) -> ApiResult<Json<ApiResponse<String>>> {
    info!("更新转存配置: {:?}", req);

    // 验证 default_behavior
    if let Some(ref behavior) = req.default_behavior {
        if behavior != "transfer_only" && behavior != "transfer_and_download" {
            return Err(ApiError::BadRequest(format!(
                "无效的默认行为: {}，必须是 'transfer_only' 或 'transfer_and_download'",
                behavior
            )));
        }
    }

    // 持写锁完成「改 → 落盘 → 回写内存」，避免与其他保存交错丢更新
    let config = {
        let mut guard = app_state.config.write().await;
        let mut config = guard.clone();

        // 更新转存配置
        if let Some(behavior) = req.default_behavior {
            config.transfer.default_behavior = behavior;
        }
        if let Some(fs_id) = req.recent_save_fs_id {
            config.transfer.recent_save_fs_id = Some(fs_id);
        }
        if let Some(path) = req.recent_save_path {
            config.transfer.recent_save_path = Some(path);
        }

        // 保存到文件
        config
            .save_to_file(APP_CONFIG_PATH)
            .await
            .map_err(ApiError::Internal)?;

        // 更新内存中的配置
        *guard = config.clone();
        config
    };

    // 同步更新转存管理器的配置
    //
    // 遍历所有 per-uid manager。
    for (uid, transfer_manager) in app_state.list_transfer_managers() {
        transfer_manager
            .update_config(config.transfer.clone())
            .await;
        info!("✓ 转存管理器配置已更新 (uid={})", uid.raw());
    }

    Ok(Json(ApiResponse::success("转存配置已更新".to_string())))
}

// ============================================
// 代理运行状态 API
// ============================================

/// 代理运行状态响应
#[derive(Debug, Serialize)]
pub struct ProxyStatusResponse {
    /// 当前运行状态
    pub status: crate::common::ProxyRuntimeStatus,
    /// 抖动计数（代理切回后又快速失败的次数）
    pub flap_count: u32,
    /// 距下次探测的剩余秒数（仅在探测中有意义）
    pub next_probe_in_secs: Option<u64>,
}

/// GET /api/v1/proxy/status
/// 查询代理运行状态
pub async fn get_proxy_status(
    State(app_state): State<crate::server::AppState>,
) -> ApiResult<Json<ApiResponse<ProxyStatusResponse>>> {
    let status = app_state.fallback_mgr.runtime_status().await;
    let flap_count = app_state.fallback_mgr.flap_count();
    let next_probe_in_secs = app_state.fallback_mgr.next_probe_in_secs().await;

    Ok(Json(ApiResponse::success(ProxyStatusResponse {
        status,
        flap_count,
        next_probe_in_secs,
    })))
}

/// 代理测试连接请求（复用 ProxyConfig）
/// POST /api/v1/proxy/test
/// 纯无副作用：不修改 AppState，不影响当前代理状态
pub async fn test_proxy_connection(
    Json(proxy): Json<crate::common::ProxyConfig>,
) -> ApiResult<Json<ApiResponse<ProxyTestResponse>>> {
    // 验证配置
    if let Err(e) = proxy.validate() {
        return Err(ApiError::BadRequest(format!("代理配置验证失败: {}", e)));
    }
    if proxy.proxy_type == crate::common::ProxyType::None {
        return Err(ApiError::BadRequest("代理类型不能为 none".to_string()));
    }

    let start = std::time::Instant::now();
    match crate::common::probe_proxy(&proxy).await {
        Ok(()) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            info!("代理测试连接成功，延迟 {}ms", latency_ms);
            Ok(Json(ApiResponse::success(ProxyTestResponse {
                success: true,
                latency_ms: Some(latency_ms),
                error: None,
            })))
        }
        Err(e) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            warn!("代理测试连接失败: {}", e);
            Ok(Json(ApiResponse::success(ProxyTestResponse {
                success: false,
                latency_ms: Some(latency_ms),
                error: Some(e.to_string()),
            })))
        }
    }
}

/// 代理测试连接响应
#[derive(Debug, Serialize)]
pub struct ProxyTestResponse {
    pub success: bool,
    pub latency_ms: Option<u64>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = AppConfig::default();
        // 默认配置应该有效
        assert!(config.download.max_global_threads > 0);
        assert!(config.download.chunk_size_mb > 0);
    }
}

/// 检查原始 JSON 以区分「字段缺失」与「显式空列表」（字段默认值为空），缺失时保留已存词表。
fn preserve_missing_regex_presets(
    raw: &serde_json::Value,
    incoming: &mut AppConfig,
    stored: &ShareSyncConfig,
) {
    let present = raw
        .get("share_sync")
        .and_then(|v| v.get("exclude_regex_preset_rules"))
        .is_some();
    if !present {
        incoming.share_sync.exclude_regex_preset_rules = stored.exclude_regex_preset_rules.clone();
    }
}

#[cfg(test)]
mod regex_preset_update_tests {
    use super::*;

    fn stored() -> ShareSyncConfig {
        ShareSyncConfig {
            exclude_preset_rules: vec!["*.nfo".into()],
            exclude_regex_preset_rules: vec!["^/股票/按月归档/2025.*$".into()],
        }
    }

    /// 按旧客户端的方式构造请求：完整配置 JSON 但删掉新字段。
    fn raw_without_regex_field() -> serde_json::Value {
        let mut raw = serde_json::to_value(AppConfig::default()).unwrap();
        raw.get_mut("share_sync")
            .and_then(|v| v.as_object_mut())
            .expect("share_sync 必须存在")
            .remove("exclude_regex_preset_rules");
        raw
    }

    fn set_regex_presets(raw: &mut serde_json::Value, value: serde_json::Value) {
        raw["share_sync"]["exclude_regex_preset_rules"] = value;
    }

    fn parse(raw: &serde_json::Value) -> AppConfig {
        serde_json::from_value(raw.clone()).unwrap()
    }

    #[test]
    fn missing_field_preserves_stored_regex_presets() {
        let raw = raw_without_regex_field();
        let mut cfg = parse(&raw);
        preserve_missing_regex_presets(&raw, &mut cfg, &stored());
        assert_eq!(
            cfg.share_sync.exclude_regex_preset_rules,
            vec!["^/股票/按月归档/2025.*$".to_string()]
        );
    }

    #[test]
    fn missing_field_keeps_glob_presets_from_request() {
        let mut raw = raw_without_regex_field();
        raw["share_sync"]["exclude_preset_rules"] = serde_json::json!(["*.tmp"]);
        let mut cfg = parse(&raw);
        preserve_missing_regex_presets(&raw, &mut cfg, &stored());
        assert_eq!(
            cfg.share_sync.exclude_preset_rules,
            vec!["*.tmp".to_string()]
        );
        assert_eq!(cfg.share_sync.exclude_regex_preset_rules.len(), 1);
    }

    #[test]
    fn explicit_empty_list_clears_regex_presets() {
        let mut raw = raw_without_regex_field();
        set_regex_presets(&mut raw, serde_json::json!([]));
        let mut cfg = parse(&raw);
        preserve_missing_regex_presets(&raw, &mut cfg, &stored());
        assert!(cfg.share_sync.exclude_regex_preset_rules.is_empty());
    }

    #[test]
    fn explicit_list_replaces_regex_presets() {
        let mut raw = raw_without_regex_field();
        set_regex_presets(&mut raw, serde_json::json!(["^/基金/.*$"]));
        let mut cfg = parse(&raw);
        preserve_missing_regex_presets(&raw, &mut cfg, &stored());
        assert_eq!(
            cfg.share_sync.exclude_regex_preset_rules,
            vec!["^/基金/.*$".to_string()]
        );
    }

    #[test]
    fn invalid_regex_presets_are_rejected_before_save() {
        let mut raw = raw_without_regex_field();
        set_regex_presets(&mut raw, serde_json::json!(["("]));
        let mut cfg = parse(&raw);
        preserve_missing_regex_presets(&raw, &mut cfg, &stored());
        let err = cfg.share_sync.validate().unwrap_err();
        assert!(err.contains("正则"), "{err}");
    }
}
