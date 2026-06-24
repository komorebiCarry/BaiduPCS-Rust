//! 加密数据导出 API 处理器
//!
//! 实现解密数据包导出功能的 API 端点：
//! - POST /api/v1/encryption/export-bundle: 导出完整解密数据包（ZIP）
//! - GET /api/v1/encryption/export-mapping: 导出映射数据（JSON）
//! - GET /api/v1/encryption/export-keys: 导出密钥配置（JSON）

use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::Response,
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::encryption::{
    DecryptBundleExporter, EncryptionConfigStore, MappingExport, MappingGenerator,
};
use crate::server::{ApiError, ApiResult, AppState};

/// API 响应包装
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    #[allow(dead_code)]
    pub fn error(message: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.to_string()),
        }
    }
}

/// 密钥导出响应
#[derive(Debug, Serialize)]
pub struct KeyExportResponse {
    /// 当前密钥信息
    pub current_key: KeyInfo,
    /// 历史密钥列表
    pub key_history: Vec<KeyInfo>,
}

/// 密钥信息
#[derive(Debug, Serialize)]
pub struct KeyInfo {
    /// 主密钥（Base64 编码）
    pub master_key: String,
    /// 加密算法
    pub algorithm: String,
    /// 密钥版本
    pub key_version: u32,
    /// 创建时间（Unix 时间戳，毫秒）
    pub created_at: i64,
    /// 最后使用时间
    pub last_used_at: Option<i64>,
    /// 废弃时间（仅历史密钥）
    pub deprecated_at: Option<i64>,
}

/// POST /api/v1/encryption/export-bundle
///
/// 导出完整的解密数据包（ZIP 格式）
/// 包含 encryption.json 和 mapping.json
pub async fn export_bundle(State(state): State<AppState>) -> Result<Response, ApiError> {
    // 获取加密配置存储
    let config_store = get_encryption_config_store(&state).await?;

    // 创建导出器
    let exporter =
        DecryptBundleExporter::new(config_store, Arc::clone(&state.backup_record_manager));

    // 生成 ZIP 数据
    let zip_data = exporter
        .export_bundle()
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("导出失败: {}", e)))?;

    // 生成文件名（包含时间戳）
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let filename = format!("decrypt_bundle_{}.zip", timestamp);

    // 返回 ZIP 文件响应
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .header(header::CONTENT_LENGTH, zip_data.len())
        .body(Body::from(zip_data))
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("构建响应失败: {}", e)))?;

    Ok(response)
}

/// GET /api/v1/encryption/export-mapping
///
/// 导出映射数据（JSON 格式）
pub async fn export_mapping(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<MappingExport>>> {
    // 创建映射生成器
    let generator = MappingGenerator::new(Arc::clone(&state.backup_record_manager));

    // 生成映射数据
    let mapping = generator
        .generate_mapping()
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("生成映射失败: {}", e)))?;

    Ok(Json(ApiResponse::success(mapping)))
}

/// GET /api/v1/encryption/export-keys
///
/// 导出密钥配置（JSON 格式）
pub async fn export_keys(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<KeyExportResponse>>> {
    // 获取加密配置存储
    let config_store = get_encryption_config_store(&state).await?;

    // 加载密钥配置
    let config = config_store
        .load()
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("加载密钥配置失败: {}", e)))?
        .ok_or_else(|| ApiError::NotFound("没有密钥配置".to_string()))?;

    // 转换为响应格式
    let response = KeyExportResponse {
        current_key: KeyInfo {
            master_key: config.current.master_key,
            algorithm: format!("{:?}", config.current.algorithm).to_lowercase(),
            key_version: config.current.key_version,
            created_at: config.current.created_at,
            last_used_at: config.current.last_used_at,
            deprecated_at: config.current.deprecated_at,
        },
        key_history: config
            .history
            .into_iter()
            .map(|k| KeyInfo {
                master_key: k.master_key,
                algorithm: format!("{:?}", k.algorithm).to_lowercase(),
                key_version: k.key_version,
                created_at: k.created_at,
                last_used_at: k.last_used_at,
                deprecated_at: k.deprecated_at,
            })
            .collect(),
    };

    Ok(Json(ApiResponse::success(response)))
}

/// 获取加密配置存储
///
/// 从 AutoBackupManager 获取 EncryptionConfigStore
async fn get_encryption_config_store(
    state: &AppState,
) -> Result<Arc<EncryptionConfigStore>, ApiError> {
    match state.autobackup_manager_for_active().await {
        Some(manager) => Ok(manager.get_encryption_config_store()),
        None => Err(ApiError::Internal(anyhow::anyhow!(
            "自动备份管理器未初始化"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_success() {
        let response: ApiResponse<String> = ApiResponse::success("test".to_string());
        assert!(response.success);
        assert_eq!(response.data, Some("test".to_string()));
        assert!(response.error.is_none());
    }

    #[test]
    fn test_api_response_error() {
        let response: ApiResponse<String> = ApiResponse::error("error message");
        assert!(!response.success);
        assert!(response.data.is_none());
        assert_eq!(response.error, Some("error message".to_string()));
    }

    #[test]
    fn test_key_info_serialization() {
        let key_info = KeyInfo {
            master_key: "base64key".to_string(),
            algorithm: "aes256gcm".to_string(),
            key_version: 1,
            created_at: 1702454400000,
            last_used_at: Some(1702454500000),
            deprecated_at: None,
        };

        let json = serde_json::to_string(&key_info).unwrap();
        assert!(json.contains("base64key"));
        assert!(json.contains("aes256gcm"));
    }
}
