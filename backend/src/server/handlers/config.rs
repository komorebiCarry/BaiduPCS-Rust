// 配置管理 API

use crate::config::{AppConfig, DownloadConfig, PathValidationResult, VipRecommendedConfig, VipType};
use crate::server::error::{ApiError, ApiResult};
use axum::{extract::State, response::Json};
use serde::Serialize;
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
    *app_state.config.write().await = config;

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
    *app_state.config.write().await = new_config;

    info!("配置更新成功");

    let response = ConfigUpdateResponse {
        message: "配置已更新".to_string(),
        path_validation: validation_result,
    };

    Ok(Json(ApiResponse::success(response)))
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
