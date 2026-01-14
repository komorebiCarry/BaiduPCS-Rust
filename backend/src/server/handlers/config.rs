// 配置管理 API

use crate::config::{
    AppConfig, DownloadConfig, PathValidationResult, VipRecommendedConfig, VipType,
};
use crate::server::error::{ApiError, ApiResult};
use axum::{extract::State, response::Json};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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

    // 应用推荐配置
    let mut config = app_state.config.read().await.clone();
    config.download.apply_recommended(vip_type);

    // 保存到文件
    config
        .save_to_file("config/app.toml")
        .await
        .map_err(ApiError::Internal)?;

    // 更新内存中的配置
    *app_state.config.write().await = config.clone();

    // 🔧 动态更新下载管理器配置
    let manager_guard = app_state.download_manager.read().await;
    if let Some(manager) = manager_guard.as_ref() {
        manager.update_max_threads(config.download.max_global_threads);
        manager
            .update_max_concurrent_tasks(config.download.max_concurrent_tasks)
            .await;
        // 更新下载目录
        manager
            .update_download_dir(config.download.download_dir.clone())
            .await;
        info!(
            "✓ 下载管理器已更新为推荐配置: 线程数={}, 最大任务数={}, 下载目录={:?}",
            config.download.max_global_threads,
            config.download.max_concurrent_tasks,
            config.download.download_dir
        );
    }
    drop(manager_guard);

    // 🔧 动态更新文件夹下载管理器的下载目录
    app_state
        .folder_download_manager
        .update_download_dir(config.download.download_dir.clone())
        .await;

    // 🔧 动态更新上传管理器配置
    let upload_manager_guard = app_state.upload_manager.read().await;
    if let Some(upload_manager) = upload_manager_guard.as_ref() {
        upload_manager.update_max_threads(config.upload.max_global_threads);
        upload_manager.update_max_concurrent_tasks(config.upload.max_concurrent_tasks).await;
        upload_manager.update_max_retries(config.upload.max_retries);
        info!(
            "✓ 上传管理器已更新为推荐配置: 线程数={}, 最大任务数={}, 最大重试={}",
            config.upload.max_global_threads,
            config.upload.max_concurrent_tasks,
            config.upload.max_retries
        );
    }
    drop(upload_manager_guard);

    info!("已恢复为推荐配置: VIP类型={:?}", vip_type);
    Ok(Json(ApiResponse::success("已恢复为推荐配置".to_string())))
}

/// PUT /api/v1/config
/// 更新配置
pub async fn update_config(
    State(app_state): State<crate::server::AppState>,
    Json(new_config): Json<AppConfig>,
) -> ApiResult<Json<ApiResponse<ConfigUpdateResponse>>> {
    info!("更新应用配置");

    // 基本验证
    if new_config.download.max_global_threads == 0 {
        return Err(ApiError::BadRequest("线程数必须大于0".to_string()));
    }

    if new_config.download.chunk_size_mb == 0 {
        return Err(ApiError::BadRequest("分片大小必须大于0".to_string()));
    }

    if new_config.download.max_concurrent_tasks == 0 {
        return Err(ApiError::BadRequest("最大同时下载数必须大于0".to_string()));
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

    // 保存到文件（包含完整的路径验证）
    let validation_result = new_config
        .save_to_file("config/app.toml")
        .await
        .map_err(ApiError::Internal)?;

    // 更新内存中的配置
    *app_state.config.write().await = new_config.clone();

    // 🔧 动态更新下载管理器配置（无需重启，不影响正在进行的任务）
    let manager_guard = app_state.download_manager.read().await;
    if let Some(manager) = manager_guard.as_ref() {
        manager.update_max_threads(new_config.download.max_global_threads);
        manager
            .update_max_concurrent_tasks(new_config.download.max_concurrent_tasks)
            .await;
        // 更新下载目录
        manager
            .update_download_dir(new_config.download.download_dir.clone())
            .await;
        info!(
            "✓ 下载管理器配置已动态更新: 线程数={}, 最大任务数={}, 下载目录={:?}",
            new_config.download.max_global_threads,
            new_config.download.max_concurrent_tasks,
            new_config.download.download_dir
        );
    } else {
        info!("下载管理器未初始化，配置将在下次登录时生效");
    }
    drop(manager_guard);

    // 🔧 动态更新文件夹下载管理器的下载目录
    app_state
        .folder_download_manager
        .update_download_dir(new_config.download.download_dir.clone())
        .await;
    info!(
        "✓ 文件夹下载管理器下载目录已更新: {:?}",
        new_config.download.download_dir
    );

    // 🔧 动态更新上传管理器配置（无需重启，不影响正在进行的任务）
    let upload_manager_guard = app_state.upload_manager.read().await;
    if let Some(upload_manager) = upload_manager_guard.as_ref() {
        upload_manager.update_max_threads(new_config.upload.max_global_threads);
        upload_manager.update_max_concurrent_tasks(new_config.upload.max_concurrent_tasks).await;
        upload_manager.update_max_retries(new_config.upload.max_retries);
        info!(
            "✓ 上传管理器配置已动态更新: 线程数={}, 最大任务数={}, 最大重试={}",
            new_config.upload.max_global_threads,
            new_config.upload.max_concurrent_tasks,
            new_config.upload.max_retries
        );
    } else {
        info!("上传管理器未初始化，配置将在下次登录时生效");
    }
    drop(upload_manager_guard);

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

    // 获取当前配置
    let mut config = app_state.config.read().await.clone();

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
        .save_to_file("config/app.toml")
        .await
        .map_err(ApiError::Internal)?;

    // 更新内存中的配置
    *app_state.config.write().await = config;

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

    // 获取当前配置
    let mut config = app_state.config.read().await.clone();
    config.download.default_directory = Some(path.clone());

    // 同时更新 download_dir（主下载目录）
    config.download.download_dir = path;

    // 保存到文件
    config
        .save_to_file("config/app.toml")
        .await
        .map_err(ApiError::Internal)?;

    // 更新内存中的配置
    *app_state.config.write().await = config.clone();

    // 同步更新下载管理器的下载目录
    let manager_guard = app_state.download_manager.read().await;
    if let Some(manager) = manager_guard.as_ref() {
        manager
            .update_download_dir(config.download.download_dir.clone())
            .await;
        info!("✓ 下载管理器下载目录已更新");
    }
    drop(manager_guard);

    // 同步更新文件夹下载管理器的下载目录
    app_state
        .folder_download_manager
        .update_download_dir(config.download.download_dir.clone())
        .await;

    Ok(Json(ApiResponse::success("默认下载目录已设置".to_string())))
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

    let path_str = recent_save_path.as_ref().map(|s| s.as_str()).unwrap_or("");

    if path_str.is_empty() || path_str == "/" {
        return Ok(Json(ApiResponse::success(TransferConfigResponse {
            default_behavior: default_behavior.clone(),
            recent_save_fs_id,
            recent_save_path: recent_save_path.clone(),
        })));
    }

    // 使用单例网盘客户端
    let client_lock = app_state.netdisk_client.read().await;
    let client = match client_lock.as_ref() {
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
                .save_to_file("config/app.toml")
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

    // 获取当前配置
    let mut config = app_state.config.read().await.clone();

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
        .save_to_file("config/app.toml")
        .await
        .map_err(ApiError::Internal)?;

    // 更新内存中的配置
    *app_state.config.write().await = config.clone();

    // 同步更新转存管理器的配置
    let transfer_manager_guard = app_state.transfer_manager.read().await;
    if let Some(transfer_manager) = transfer_manager_guard.as_ref() {
        transfer_manager
            .update_config(config.transfer.clone())
            .await;
        info!("✓ 转存管理器配置已更新");
    }
    drop(transfer_manager_guard);

    Ok(Json(ApiResponse::success("转存配置已更新".to_string())))
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
