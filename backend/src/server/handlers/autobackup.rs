//! 自动备份 API 处理器

use axum::{
    extract::{Path, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::autobackup::{
    AutoBackupManager, BackupConfig, BackupTask, CreateBackupConfigRequest,
    EncryptionAlgorithm, UpdateBackupConfigRequest,
};
use crate::server::{ApiError, ApiResult, AppState};

// Helper functions for ApiError
fn internal_error(msg: &str) -> ApiError {
    ApiError::Internal(anyhow::anyhow!("{}", msg))
}

fn not_found_error(msg: &str) -> ApiError {
    ApiError::NotFound(msg.to_string())
}

fn bad_request_error(msg: &str) -> ApiError {
    ApiError::BadRequest(msg.to_string())
}

/// 获取 AutoBackupManager 的 Arc 克隆，立即释放锁
///
/// 这是解决死锁问题的关键：获取 Arc 克隆后立即释放 RwLock，
/// 避免在持有锁的情况下调用 .await
async fn get_manager(state: &AppState) -> Result<Arc<AutoBackupManager>, ApiError> {
    let guard = state.autobackup_manager.read().await;
    match &*guard {
        Some(manager) => Ok(Arc::clone(manager)),
        None => Err(internal_error("自动备份管理器未初始化")),
    }
}

// ==================== 备份配置 API ====================

/// 获取所有备份配置
pub async fn list_backup_configs(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<Vec<BackupConfig>>>> {
    let manager = get_manager(&state).await?;
    let configs = manager.get_all_configs();
    Ok(Json(ApiResponse::success(configs)))
}

/// 获取单个备份配置
pub async fn get_backup_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<BackupConfig>>> {
    let manager = get_manager(&state).await?;
    if let Some(config) = manager.get_config(&id) {
        Ok(Json(ApiResponse::success(config)))
    } else {
        Err(not_found_error("配置不存在"))
    }
}

/// 创建备份配置
pub async fn create_backup_config(
    State(state): State<AppState>,
    Json(request): Json<CreateBackupConfigRequest>,
) -> ApiResult<Json<ApiResponse<BackupConfig>>> {
    let manager = get_manager(&state).await?;
    match manager.create_config(request).await {
        Ok(config) => Ok(Json(ApiResponse::success(config))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 更新备份配置
pub async fn update_backup_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateBackupConfigRequest>,
) -> ApiResult<Json<ApiResponse<BackupConfig>>> {
    let manager = get_manager(&state).await?;
    match manager.update_config(&id, request).await {
        Ok(config) => Ok(Json(ApiResponse::success(config))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}


/// 启用备份配置
pub async fn enable_backup_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<BackupConfig>>> {
    let manager = get_manager(&state).await?;
    let request = crate::autobackup::UpdateBackupConfigRequest {
        name: None,
        local_path: None,
        remote_path: None,
        watch_config: None,
        poll_config: None,
        filter_config: None,
        enabled: Some(true),
    };
    match manager.update_config(&id, request).await {
        Ok(config) => Ok(Json(ApiResponse::success(config))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 禁用备份配置
pub async fn disable_backup_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<BackupConfig>>> {
    let manager = get_manager(&state).await?;
    let request = crate::autobackup::UpdateBackupConfigRequest {
        name: None,
        local_path: None,
        remote_path: None,
        watch_config: None,
        poll_config: None,
        filter_config: None,
        enabled: Some(false),
    };
    match manager.update_config(&id, request).await {
        Ok(config) => Ok(Json(ApiResponse::success(config))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

// ==================== 备份任务 API ====================

/// 手动触发备份
pub async fn trigger_backup(
    State(state): State<AppState>,
    Path(config_id): Path<String>,
) -> ApiResult<Json<ApiResponse<TriggerBackupResponse>>> {
    let manager = get_manager(&state).await?;
    match manager.trigger_backup(&config_id).await {
        Ok(task_id) => Ok(Json(ApiResponse::success(TriggerBackupResponse { task_id }))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 获取备份任务
pub async fn get_backup_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<ApiResponse<BackupTask>>> {
    let manager = get_manager(&state).await?;
    if let Some(task) = manager.get_task_async(&task_id).await {
        Ok(Json(ApiResponse::success(task)))
    } else {
        Err(not_found_error("任务不存在"))
    }
}

/// 获取配置的所有任务
pub async fn list_backup_tasks(
    State(state): State<AppState>,
    Path(config_id): Path<String>,
) -> ApiResult<Json<ApiResponse<Vec<BackupTask>>>> {
    let manager = get_manager(&state).await?;
    let tasks = manager.get_tasks_by_config_async(&config_id).await;
    Ok(Json(ApiResponse::success(tasks)))
}

/// 取消备份任务
pub async fn cancel_backup_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.cancel_task(&task_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 暂停备份任务
pub async fn pause_backup_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.pause_task(&task_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 恢复备份任务
pub async fn resume_backup_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.resume_task(&task_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 删除备份任务
pub async fn delete_backup_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.delete_task(&task_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 删除备份配置
pub async fn delete_backup_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.delete_config(&id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

// ==================== 子任务 API ====================

/// 获取任务的子任务列表（分页）
pub async fn list_file_tasks(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<FileTasksQuery>,
) -> ApiResult<Json<ApiResponse<FileTasksResponse>>> {
    let manager = get_manager(&state).await?;
    let page = params.page.unwrap_or(1);
    let page_size = params.page_size.unwrap_or(20);

    match manager.get_file_tasks_async(&task_id, page, page_size).await {
        Some((file_tasks, total)) => Ok(Json(ApiResponse::success(FileTasksResponse {
            file_tasks,
            total,
            page,
            page_size,
        }))),
        None => Err(not_found_error("任务不存在")),
    }
}

/// 重试单个文件任务
pub async fn retry_file_task(
    State(state): State<AppState>,
    Path((task_id, file_task_id)): Path<(String, String)>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.retry_file_task(&task_id, &file_task_id).await {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

// ==================== 加密 API ====================

/// 获取加密状态
pub async fn get_encryption_status(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<EncryptionStatusResponse>>> {
    let manager = get_manager(&state).await?;
    let status = manager.get_encryption_status_nonblocking();
    Ok(Json(ApiResponse::success(EncryptionStatusResponse {
        enabled: status.enabled,
        has_key: status.has_key,
        algorithm: format!("{:?}", status.algorithm),
        key_created_at: status.key_created_at.map(|t| t.to_rfc3339()),
    })))
}

/// 生成加密密钥
pub async fn generate_encryption_key(
    State(state): State<AppState>,
    Json(request): Json<GenerateKeyRequest>,
) -> ApiResult<Json<ApiResponse<GenerateKeyResponse>>> {
    let manager = get_manager(&state).await?;
    let algorithm = match request.algorithm.as_deref() {
        Some("ChaCha20-Poly1305") => EncryptionAlgorithm::ChaCha20Poly1305,
        _ => EncryptionAlgorithm::Aes256Gcm,
    };

    match manager.generate_encryption_key(algorithm) {
        Ok(key) => Ok(Json(ApiResponse::success(GenerateKeyResponse { key }))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

/// 导入加密密钥
pub async fn import_encryption_key(
    State(state): State<AppState>,
    Json(request): Json<ImportKeyRequest>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    let algorithm = match request.algorithm.as_deref() {
        Some("ChaCha20-Poly1305") => EncryptionAlgorithm::ChaCha20Poly1305,
        _ => EncryptionAlgorithm::Aes256Gcm,
    };

    match manager.configure_encryption(&request.key, algorithm) {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 导出加密密钥
pub async fn export_encryption_key(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<ExportKeyResponse>>> {
    let manager = get_manager(&state).await?;
    match manager.export_encryption_key() {
        Ok(key) => Ok(Json(ApiResponse::success(ExportKeyResponse { key }))),
        Err(e) => Err(bad_request_error(&e.to_string())),
    }
}

/// 删除加密密钥
pub async fn delete_encryption_key(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.delete_encryption_key() {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

/// 强制删除所有加密密钥（包括历史）
///
/// 警告：这将导致无法解密任何已加密的文件
pub async fn force_delete_encryption_key(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<()>>> {
    let manager = get_manager(&state).await?;
    match manager.force_delete_encryption_key() {
        Ok(()) => Ok(Json(ApiResponse::success(()))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

// ==================== 状态和统计 API ====================

/// 获取管理器状态
pub async fn get_manager_status(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<ManagerStatusResponse>>> {
    let manager = get_manager(&state).await?;
    let status = manager.get_status_nonblocking();
    Ok(Json(ApiResponse::success(ManagerStatusResponse {
        config_count: status.config_count,
        active_task_count: status.active_task_count,
        watcher_running: status.watcher_running,
        watched_path_count: status.watched_path_count,
        poll_schedule_count: status.poll_schedule_count,
        encryption_enabled: status.encryption_enabled,
        scan_slots: status.scan_slots,
        encrypt_slots: status.encrypt_slots,
    })))
}

/// 获取记录统计
pub async fn get_record_stats(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<RecordStatsResponse>>> {
    let manager = get_manager(&state).await?;
    match manager.get_record_stats() {
        Ok(stats) => Ok(Json(ApiResponse::success(RecordStatsResponse {
            upload_count: stats.upload_count,
            download_count: stats.download_count,
            snapshot_count: stats.snapshot_count,
        }))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

/// 清理过期记录
pub async fn cleanup_records(
    State(state): State<AppState>,
    Json(request): Json<CleanupRecordsRequest>,
) -> ApiResult<Json<ApiResponse<CleanupRecordsResponse>>> {
    let manager = get_manager(&state).await?;
    let days = request.days.unwrap_or(30);
    match manager.cleanup_old_records(days) {
        Ok((upload, download, snapshot)) => Ok(Json(ApiResponse::success(CleanupRecordsResponse {
            upload_deleted: upload,
            download_deleted: download,
            snapshot_deleted: snapshot,
        }))),
        Err(e) => Err(internal_error(&e.to_string())),
    }
}

// ==================== 数据结构 ====================

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

    pub fn error(message: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.to_string()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TriggerBackupResponse {
    pub task_id: String,
}

#[derive(Debug, Serialize)]
pub struct EncryptionStatusResponse {
    pub enabled: bool,
    pub has_key: bool,
    pub algorithm: String,
    pub key_created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GenerateKeyRequest {
    pub algorithm: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GenerateKeyResponse {
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct ImportKeyRequest {
    pub key: String,
    pub algorithm: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ExportKeyResponse {
    pub key: String,
}

#[derive(Debug, Serialize)]
pub struct ManagerStatusResponse {
    pub config_count: usize,
    pub active_task_count: usize,
    pub watcher_running: bool,
    pub watched_path_count: usize,
    pub poll_schedule_count: usize,
    pub encryption_enabled: bool,
    pub scan_slots: String,
    pub encrypt_slots: String,
}

#[derive(Debug, Serialize)]
pub struct RecordStatsResponse {
    pub upload_count: usize,
    pub download_count: usize,
    pub snapshot_count: usize,
}

#[derive(Debug, Deserialize)]
pub struct CleanupRecordsRequest {
    pub days: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct CleanupRecordsResponse {
    pub upload_deleted: usize,
    pub download_deleted: usize,
    pub snapshot_deleted: usize,
}

// ==================== 子任务相关数据结构 ====================

#[derive(Debug, Deserialize)]
pub struct FileTasksQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct FileTasksResponse {
    pub file_tasks: Vec<crate::autobackup::task::BackupFileTask>,
    pub total: usize,
    pub page: usize,
    pub page_size: usize,
}

// ==================== 调试 API 数据结构 ====================

/// 文件状态查询参数
#[derive(Debug, Deserialize)]
pub struct FileStateQuery {
    /// 文件路径
    pub path: String,
}

/// 文件状态历史记录
#[derive(Debug, Serialize)]
pub struct FileStateHistory {
    /// 状态名称
    pub state: String,
    /// 时间戳
    pub timestamp: String,
}

/// 文件状态追踪响应
#[derive(Debug, Serialize)]
pub struct FileStateResponse {
    /// 文件路径
    pub path: String,
    /// 当前状态
    pub current_state: Option<String>,
    /// 状态历史
    pub state_history: Vec<FileStateHistory>,
    /// 去重结果
    pub dedup_result: Option<String>,
    /// 是否启用加密
    pub encryption_enabled: bool,
    /// 重试次数
    pub retry_count: u32,
    /// 关联的配置ID
    pub config_id: Option<String>,
    /// 关联的任务ID
    pub task_id: Option<String>,
}

/// 健康检查响应
#[derive(Debug, Serialize)]
pub struct HealthCheckResponse {
    /// 数据库连接状态
    pub database_ok: bool,
    /// 加密密钥状态
    pub encryption_key_ok: bool,
    /// 文件监听状态
    pub file_watcher_ok: bool,
    /// 网络连接状态
    pub network_ok: bool,
    /// 磁盘空间状态
    pub disk_space_ok: bool,
    /// 整体健康状态
    pub overall_healthy: bool,
    /// 检查时间
    pub checked_at: String,
}

// ==================== 调试 API 处理函数 ====================

/// 获取文件状态追踪信息
pub async fn get_file_state(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<FileStateQuery>,
) -> ApiResult<Json<ApiResponse<FileStateResponse>>> {
    let manager = get_manager(&state).await?;
    // 查找文件在所有任务中的状态
    let file_state = manager.get_file_state(&params.path);

    match file_state {
        Some(info) => Ok(Json(ApiResponse::success(FileStateResponse {
            path: params.path,
            current_state: Some(info.current_state),
            state_history: info.state_history.into_iter().map(|(state, ts)| {
                FileStateHistory {
                    state,
                    timestamp: ts,
                }
            }).collect(),
            dedup_result: info.dedup_result,
            encryption_enabled: info.encryption_enabled,
            retry_count: info.retry_count,
            config_id: info.config_id,
            task_id: info.task_id,
        }))),
        None => Ok(Json(ApiResponse::success(FileStateResponse {
            path: params.path,
            current_state: None,
            state_history: vec![],
            dedup_result: None,
            encryption_enabled: false,
            retry_count: 0,
            config_id: None,
            task_id: None,
        }))),
    }
}

/// 获取系统健康检查状态
pub async fn get_health_check(
    State(state): State<AppState>,
) -> ApiResult<Json<ApiResponse<HealthCheckResponse>>> {
    let manager = get_manager(&state).await?;
    let health = manager.health_check().await;

    let overall_healthy = health.database_ok
        && health.encryption_key_ok
        && health.file_watcher_ok
        && health.disk_space_ok;

    Ok(Json(ApiResponse::success(HealthCheckResponse {
        database_ok: health.database_ok,
        encryption_key_ok: health.encryption_key_ok,
        file_watcher_ok: health.file_watcher_ok,
        network_ok: health.network_ok,
        disk_space_ok: health.disk_space_ok,
        overall_healthy,
        checked_at: chrono::Utc::now().to_rfc3339(),
    })))
}

// ==================== 文件监听能力检测 API ====================

/// 文件监听能力检测响应
#[derive(Debug, Serialize)]
pub struct WatchCapabilityResponse {
    /// 是否可用
    pub available: bool,
    /// 当前平台
    pub platform: String,
    /// 使用的后端
    pub backend: String,
    /// 不可用原因（如果不可用）
    pub reason: Option<String>,
    /// 建议（如果有问题）
    pub suggestion: Option<String>,
    /// 警告信息
    pub warnings: Vec<String>,
}

/// GET /api/v1/system/watch-capability
/// 检测当前环境的文件监听能力
pub async fn get_watch_capability() -> ApiResult<Json<ApiResponse<WatchCapabilityResponse>>> {
    let mut warnings = Vec::new();
    let mut available = true;
    let mut reason: Option<String> = None;
    let mut suggestion: Option<String> = None;

    // 检测平台
    let platform = if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "unknown"
    };

    // 检测后端
    let backend = if cfg!(target_os = "windows") {
        "ReadDirectoryChangesW"
    } else if cfg!(target_os = "macos") {
        "FSEvents"
    } else if cfg!(target_os = "linux") {
        "inotify"
    } else {
        "unknown"
    };

    // 平台特定的警告和检测
    match platform {
        "windows" => {
            warnings.push("Windows 文件监听可能不稳定，建议同时启用间隔轮询兜底".to_string());
        }
        "linux" => {
            // 检查 inotify watch limit
            if let Ok(content) = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches") {
                if let Ok(limit) = content.trim().parse::<u64>() {
                    if limit < 65536 {
                        warnings.push(format!(
                            "inotify watch limit 较低 (当前: {})，可能无法监听大量文件",
                            limit
                        ));
                        suggestion = Some(format!(
                            "建议执行: sudo sysctl fs.inotify.max_user_watches=524288"
                        ));
                    }
                }
            }
        }
        "macos" => {
            // macOS FSEvents 通常比较稳定
        }
        _ => {
            available = false;
            reason = Some("不支持的操作系统".to_string());
        }
    }

    // 尝试创建一个临时的 watcher 来验证功能
    match notify::recommended_watcher(|_| {}) {
        Ok(_) => {
            // 成功创建 watcher
        }
        Err(e) => {
            available = false;
            reason = Some(format!("无法创建文件监听器: {}", e));
            if platform == "linux" {
                suggestion = Some(
                    "可能是 inotify 资源不足，请检查系统限制".to_string()
                );
            }
        }
    }

    Ok(Json(ApiResponse::success(WatchCapabilityResponse {
        available,
        platform: platform.to_string(),
        backend: backend.to_string(),
        reason,
        suggestion,
        warnings,
    })))
}

// ==================== 全局触发配置 API ====================

/// 全局触发配置响应
#[derive(Debug, Serialize)]
pub struct GlobalTriggerConfigResponse {
    /// 上传备份触发配置
    pub upload_trigger: UploadTriggerConfigResponse,
    /// 下载备份触发配置
    pub download_trigger: DownloadTriggerConfigResponse,
}

/// 上传触发配置响应
#[derive(Debug, Serialize)]
pub struct UploadTriggerConfigResponse {
    /// 是否启用文件系统监听
    pub watch_enabled: bool,
    /// 监听防抖时间（毫秒）
    pub watch_debounce_ms: u64,
    /// 是否递归监听子目录
    pub watch_recursive: bool,
    /// 是否启用间隔时间兜底
    pub fallback_interval_enabled: bool,
    /// 间隔兜底轮询时间（分钟）
    pub fallback_interval_minutes: u32,
    /// 是否启用指定时间全量扫描
    pub fallback_scheduled_enabled: bool,
    /// 指定时间全量扫描 - 小时（0-23）
    pub fallback_scheduled_hour: u8,
    /// 指定时间全量扫描 - 分钟（0-59）
    pub fallback_scheduled_minute: u8,
}

/// 下载触发配置响应
#[derive(Debug, Serialize)]
pub struct DownloadTriggerConfigResponse {
    /// 轮询模式：interval 或 scheduled
    pub poll_mode: String,
    /// 间隔轮询时间（分钟）
    pub poll_interval_minutes: u32,
    /// 指定时间轮询 - 小时（0-23）
    pub poll_scheduled_hour: u8,
    /// 指定时间轮询 - 分钟（0-59）
    pub poll_scheduled_minute: u8,
}

/// GET /api/v1/config/autobackup/trigger
/// 获取全局自动备份触发配置
pub async fn get_trigger_config(
    State(app_state): State<AppState>,
) -> ApiResult<Json<ApiResponse<GlobalTriggerConfigResponse>>> {
    let config = app_state.config.read().await;
    let autobackup = &config.autobackup;

    Ok(Json(ApiResponse::success(GlobalTriggerConfigResponse {
        upload_trigger: UploadTriggerConfigResponse {
            watch_enabled: autobackup.upload_trigger.watch_enabled,
            watch_debounce_ms: autobackup.upload_trigger.watch_debounce_ms,
            watch_recursive: autobackup.upload_trigger.watch_recursive,
            fallback_interval_enabled: autobackup.upload_trigger.fallback_interval_enabled,
            fallback_interval_minutes: autobackup.upload_trigger.fallback_interval_minutes,
            fallback_scheduled_enabled: autobackup.upload_trigger.fallback_scheduled_enabled,
            fallback_scheduled_hour: autobackup.upload_trigger.fallback_scheduled_hour,
            fallback_scheduled_minute: autobackup.upload_trigger.fallback_scheduled_minute,
        },
        download_trigger: DownloadTriggerConfigResponse {
            poll_mode: autobackup.download_trigger.poll_mode.clone(),
            poll_interval_minutes: autobackup.download_trigger.poll_interval_minutes,
            poll_scheduled_hour: autobackup.download_trigger.poll_scheduled_hour,
            poll_scheduled_minute: autobackup.download_trigger.poll_scheduled_minute,
        },
    })))
}

/// 更新全局触发配置请求
#[derive(Debug, Deserialize)]
pub struct UpdateTriggerConfigRequest {
    /// 上传备份触发配置
    pub upload_trigger: Option<UpdateUploadTriggerRequest>,
    /// 下载备份触发配置
    pub download_trigger: Option<UpdateDownloadTriggerRequest>,
}

/// 更新上传触发配置请求
#[derive(Debug, Deserialize)]
pub struct UpdateUploadTriggerRequest {
    pub watch_enabled: Option<bool>,
    pub watch_debounce_ms: Option<u64>,
    pub watch_recursive: Option<bool>,
    pub fallback_interval_enabled: Option<bool>,
    pub fallback_interval_minutes: Option<u32>,
    pub fallback_scheduled_enabled: Option<bool>,
    pub fallback_scheduled_hour: Option<u8>,
    pub fallback_scheduled_minute: Option<u8>,
}

/// 更新下载触发配置请求
#[derive(Debug, Deserialize)]
pub struct UpdateDownloadTriggerRequest {
    pub poll_mode: Option<String>,
    pub poll_interval_minutes: Option<u32>,
    pub poll_scheduled_hour: Option<u8>,
    pub poll_scheduled_minute: Option<u8>,
}

/// PUT /api/v1/config/autobackup/trigger
/// 更新全局自动备份触发配置
pub async fn update_trigger_config(
    State(app_state): State<AppState>,
    Json(req): Json<UpdateTriggerConfigRequest>,
) -> ApiResult<Json<ApiResponse<GlobalTriggerConfigResponse>>> {
    tracing::info!("更新自动备份触发配置");

    // 验证请求参数
    if let Some(ref download) = req.download_trigger {
        if let Some(ref mode) = download.poll_mode {
            if mode != "interval" && mode != "scheduled" {
                return Err(bad_request_error(
                    "无效的轮询模式，必须是 'interval' 或 'scheduled'"
                ));
            }
        }
        if let Some(hour) = download.poll_scheduled_hour {
            if hour > 23 {
                return Err(bad_request_error("小时必须在 0-23 之间"));
            }
        }
        if let Some(minute) = download.poll_scheduled_minute {
            if minute > 59 {
                return Err(bad_request_error("分钟必须在 0-59 之间"));
            }
        }
    }

    if let Some(ref upload) = req.upload_trigger {
        if let Some(hour) = upload.fallback_scheduled_hour {
            if hour > 23 {
                return Err(bad_request_error("小时必须在 0-23 之间"));
            }
        }
        if let Some(minute) = upload.fallback_scheduled_minute {
            if minute > 59 {
                return Err(bad_request_error("分钟必须在 0-59 之间"));
            }
        }
    }

    // 获取当前配置并更新
    let mut config = app_state.config.read().await.clone();

    // 更新上传触发配置
    if let Some(upload) = req.upload_trigger {
        if let Some(v) = upload.watch_enabled {
            config.autobackup.upload_trigger.watch_enabled = v;
        }
        if let Some(v) = upload.watch_debounce_ms {
            config.autobackup.upload_trigger.watch_debounce_ms = v;
        }
        if let Some(v) = upload.watch_recursive {
            config.autobackup.upload_trigger.watch_recursive = v;
        }
        if let Some(v) = upload.fallback_interval_enabled {
            config.autobackup.upload_trigger.fallback_interval_enabled = v;
        }
        if let Some(v) = upload.fallback_interval_minutes {
            config.autobackup.upload_trigger.fallback_interval_minutes = v;
        }
        if let Some(v) = upload.fallback_scheduled_enabled {
            config.autobackup.upload_trigger.fallback_scheduled_enabled = v;
        }
        if let Some(v) = upload.fallback_scheduled_hour {
            config.autobackup.upload_trigger.fallback_scheduled_hour = v;
        }
        if let Some(v) = upload.fallback_scheduled_minute {
            config.autobackup.upload_trigger.fallback_scheduled_minute = v;
        }
    }

    // 更新下载触发配置
    if let Some(download) = req.download_trigger {
        if let Some(v) = download.poll_mode {
            config.autobackup.download_trigger.poll_mode = v;
        }
        if let Some(v) = download.poll_interval_minutes {
            config.autobackup.download_trigger.poll_interval_minutes = v;
        }
        if let Some(v) = download.poll_scheduled_hour {
            config.autobackup.download_trigger.poll_scheduled_hour = v;
        }
        if let Some(v) = download.poll_scheduled_minute {
            config.autobackup.download_trigger.poll_scheduled_minute = v;
        }
    }

    // 保存到文件
    config
        .save_to_file("config/app.toml")
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    // 更新内存中的配置
    *app_state.config.write().await = config.clone();

    // 通知自动备份管理器更新调度器配置
    if let Ok(manager) = get_manager(&app_state).await {
        manager.update_trigger_config(
            config.autobackup.upload_trigger.clone(),
            config.autobackup.download_trigger.clone(),
        ).await;
        tracing::info!("✓ 自动备份管理器触发配置已更新");
    }

    tracing::info!("自动备份触发配置更新成功");

    Ok(Json(ApiResponse::success(GlobalTriggerConfigResponse {
        upload_trigger: UploadTriggerConfigResponse {
            watch_enabled: config.autobackup.upload_trigger.watch_enabled,
            watch_debounce_ms: config.autobackup.upload_trigger.watch_debounce_ms,
            watch_recursive: config.autobackup.upload_trigger.watch_recursive,
            fallback_interval_enabled: config.autobackup.upload_trigger.fallback_interval_enabled,
            fallback_interval_minutes: config.autobackup.upload_trigger.fallback_interval_minutes,
            fallback_scheduled_enabled: config.autobackup.upload_trigger.fallback_scheduled_enabled,
            fallback_scheduled_hour: config.autobackup.upload_trigger.fallback_scheduled_hour,
            fallback_scheduled_minute: config.autobackup.upload_trigger.fallback_scheduled_minute,
        },
        download_trigger: DownloadTriggerConfigResponse {
            poll_mode: config.autobackup.download_trigger.poll_mode.clone(),
            poll_interval_minutes: config.autobackup.download_trigger.poll_interval_minutes,
            poll_scheduled_hour: config.autobackup.download_trigger.poll_scheduled_hour,
            poll_scheduled_minute: config.autobackup.download_trigger.poll_scheduled_minute,
        },
    })))
}
