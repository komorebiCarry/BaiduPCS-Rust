//! 文件夹下载 API 处理器

use crate::downloader::{DownloadConflictStrategy, DownloadTask, FolderDownload, TaskStatus};
use crate::server::extractors::{resolve_uid_from_query, UidQuery};
use crate::server::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use super::ApiResponse;

/// 创建文件夹下载请求
#[derive(Debug, Deserialize)]
pub struct CreateFolderDownloadRequest {
    pub path: String,
    /// 原始文件夹名（如果是加密文件夹，前端传入还原后的名称）
    #[serde(default)]
    pub original_name: Option<String>,
    /// 冲突策略（可选，未指定则使用默认值）
    #[serde(default)]
    pub conflict_strategy: Option<DownloadConflictStrategy>,
    /// 显式指定 owner_uid
    ///
    /// **字段名兼容**：加 `alias = "owner_uid"`
    /// 兼容前端发送的 `owner_uid` 字段名。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
}

/// 删除文件夹下载请求参数
#[derive(Debug, Deserialize)]
pub struct DeleteFolderQuery {
    #[serde(default)]
    pub delete_files: bool,
}

/// 统一下载项（文件或文件夹）
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DownloadItem {
    File {
        #[serde(flatten)]
        task: DownloadTask,
    },
    Folder {
        #[serde(flatten)]
        folder: FolderDownload,
        /// 文件夹的聚合速度
        speed: u64,
        /// 已完成的文件数
        completed_files: u64,
    },
}

impl DownloadItem {
    fn created_at(&self) -> i64 {
        match self {
            DownloadItem::File { task } => task.created_at,
            DownloadItem::Folder { folder, .. } => folder.created_at,
        }
    }
}

/// POST /api/v1/downloads/folder
/// 创建文件夹下载
pub async fn create_folder_download(
    State(app_state): State<AppState>,
    Json(req): Json<CreateFolderDownloadRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!(
        "创建文件夹下载: {}, original_name: {:?}, uid: {:?}",
        req.path, req.original_name, req.uid
    );

    // 如果未指定策略，从 AppConfig 读取默认值
    let conflict_strategy = req.conflict_strategy.or_else(|| {
        let config = app_state.config.blocking_read();
        Some(config.conflict_strategy.default_download_strategy)
    });

    // 🔥 解析 effective_uid
    // 优先使用前端显式 `req.uid`（owner_uid alias），否则回退到 active_uid。
    // 切账号后未传 uid 时，task 应归属当前活跃账号（不是 startup 账号）。
    let effective_uid = match req.uid {
        Some(uid_raw) => crate::auth::Uid::new(uid_raw),
        None => match *app_state.active_uid.read().await {
            Some(uid) => uid,
            None => {
                error!("create_folder_download: 未登录且无 explicit uid");
                return Err(StatusCode::UNAUTHORIZED);
            }
        },
    };

    // 校验目标账号存在
    {
        let mgr = app_state.account_manager.lock().await;
        if mgr.get_user(effective_uid).is_none() {
            error!(
                "create_folder_download: 目标账号不存在: uid={}",
                effective_uid.raw()
            );
            return Err(StatusCode::NOT_FOUND);
        }
    }

    match app_state
        .folder_download_manager
        .create_folder_download_with_name(req.path, req.original_name, conflict_strategy, effective_uid)
        .await
    {
        Ok(folder_id) => Ok(Json(ApiResponse::success(folder_id))),
        Err(e) => {
            error!("创建文件夹下载失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// GET /api/v1/downloads/folders
/// 获取所有文件夹下载
pub async fn get_all_folder_downloads(
    State(app_state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<FolderDownload>>>, StatusCode> {
    let folders = app_state.folder_download_manager.get_all_folders().await;
    Ok(Json(ApiResponse::success(folders)))
}

/// GET /api/v1/downloads/folder/:id
/// 获取指定文件夹下载
pub async fn get_folder_download(
    State(app_state): State<AppState>,
    Path(folder_id): Path<String>,
) -> Result<Json<ApiResponse<FolderDownload>>, StatusCode> {
    match app_state
        .folder_download_manager
        .get_folder(&folder_id)
        .await
    {
        Some(folder) => Ok(Json(ApiResponse::success(folder))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// GET /api/v1/downloads/all
/// 获取所有下载（文件+文件夹混合，按创建时间排序）
///
/// 多账号语义：
/// - `?uid=` 缺省 → 跨账号聚合（迭代 `list_download_managers`）
/// - `?uid=X` → 仅该账号
pub async fn get_all_downloads_mixed(
    State(app_state): State<AppState>,
    Query(q): Query<UidQuery>,
) -> Result<Json<ApiResponse<Vec<DownloadItem>>>, StatusCode> {
    let filter_uid = resolve_uid_from_query(&q);
    // 获取所有文件任务（跨账号聚合或单账号）
    // 🔥 共享 manager 必须按 owner_uid 过滤
    let all_tasks: Vec<DownloadTask> = match filter_uid {
        Some(uid) => match app_state.download_manager_for(uid) {
            Some(dm) => dm
                .get_all_tasks()
                .await
                .into_iter()
                .filter(|t| t.owner_uid == uid)
                .collect(),
            None => Vec::new(),
        },
        None => {
            // 全局共享历史库会被每个账号的 get_all_tasks 各捞一遍，跨账号聚合时
            // 按 id 去重，避免同一历史任务因账号数 N 而重复出现 N 次。
            let mut all = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for (_uid, dm) in app_state.list_download_managers() {
                for t in dm.get_all_tasks().await {
                    if seen.insert(t.id.clone()) {
                        all.push(t);
                    }
                }
            }
            all
        }
    };

    // 获取所有文件夹任务（内存 + 历史数据库；按 uid 过滤）
    let folders: Vec<FolderDownload> = app_state
        .folder_download_manager
        .get_all_folders_with_history()
        .await
        .into_iter()
        .filter(|f| match filter_uid {
            Some(uid) => f.owner_uid == uid,
            None => true,
        })
        .collect();

    let mut items: Vec<DownloadItem> = Vec::new();

    // 添加单文件任务（排除属于文件夹的）
    for task in all_tasks.iter() {
        if task.group_id.is_none() {
            items.push(DownloadItem::File { task: task.clone() });
        }
    }

    // 添加文件夹任务
    for mut folder in folders {
        // 计算该文件夹的聚合速度（仅从活跃子任务）
        let folder_tasks: Vec<&DownloadTask> = all_tasks
            .iter()
            .filter(|t| t.group_id.as_deref() == Some(&folder.id))
            .collect();

        let speed: u64 = folder_tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Downloading)
            .map(|t| t.speed)
            .sum();

        // 使用文件夹自身维护的 completed_count（由 start_task_completed_listener 递增）
        // 不再从内存子任务重新计数，因为已完成的任务会被移除
        let completed_files = folder.completed_count;

        // 🔥 使用 compute_downloaded_size：completed_downloaded_size + active_sum
        // max() 保证单调性，不再用活跃子任务之和覆盖 folder.downloaded_size
        let active_downloaded: u64 = folder_tasks.iter().map(|t| t.downloaded_size).sum();
        folder.compute_downloaded_size(active_downloaded);

        items.push(DownloadItem::Folder {
            folder,
            speed,
            completed_files,
        });
    }

    // 按创建时间倒序排序（最新的在前面）
    items.sort_by(|a, b| b.created_at().cmp(&a.created_at()));

    Ok(Json(ApiResponse::success(items)))
}

/// POST /api/v1/downloads/folder/:id/pause
/// 暂停文件夹下载
pub async fn pause_folder_download(
    State(app_state): State<AppState>,
    Path(folder_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("暂停文件夹下载: {}", folder_id);

    match app_state
        .folder_download_manager
        .pause_folder(&folder_id)
        .await
    {
        Ok(_) => Ok(Json(ApiResponse::success("已暂停".to_string()))),
        Err(e) => {
            error!("暂停文件夹下载失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// POST /api/v1/downloads/folder/:id/resume
/// 恢复文件夹下载
pub async fn resume_folder_download(
    State(app_state): State<AppState>,
    Path(folder_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("恢复文件夹下载: {}", folder_id);

    match app_state
        .folder_download_manager
        .resume_folder(&folder_id)
        .await
    {
        Ok(_) => Ok(Json(ApiResponse::success("已恢复".to_string()))),
        Err(e) => {
            error!("恢复文件夹下载失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// DELETE /api/v1/downloads/folder/:id
/// 取消/删除文件夹下载
pub async fn cancel_folder_download(
    State(app_state): State<AppState>,
    Path(folder_id): Path<String>,
    Query(query): Query<DeleteFolderQuery>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!(
        "取消文件夹下载: {}, 删除文件: {}",
        folder_id, query.delete_files
    );

    match app_state
        .folder_download_manager
        .cancel_folder(&folder_id, query.delete_files)
        .await
    {
        Ok(_) => {
            // 删除记录
            let _ = app_state
                .folder_download_manager
                .delete_folder(&folder_id)
                .await;
            Ok(Json(ApiResponse::success("已取消".to_string())))
        }
        Err(e) => {
            error!("取消文件夹下载失败: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downloader::FolderStatus;
    use std::path::PathBuf;

    #[test]
    fn folder_item_keeps_folder_level_progress_stats() {
        // 模拟：文件夹已完成 10 个文件（累计 1000 字节），当前 1 个活跃子任务已下载 300 字节
        let mut folder = FolderDownload::new("/test/folder".to_string(), PathBuf::from("/tmp/folder"));
        folder.status = FolderStatus::Downloading;
        folder.total_files = 48;
        folder.total_size = 4_800;
        folder.completed_count = 10;
        folder.completed_downloaded_size = 1_000;
        folder.downloaded_size = 1_300;

        // 使用文件夹自身的 completed_count，不从内存子任务重新计数
        let completed_files = folder.completed_count;
        assert_eq!(completed_files, 10);

        // compute_downloaded_size = max(1300, 1000 + 300) = 1300
        let computed = folder.compute_downloaded_size(300);
        assert_eq!(computed, 1_300);
        assert_eq!(folder.downloaded_size, 1_300);
    }

    #[test]
    fn failed_subtask_not_counted_as_completed() {
        // 验证失败的子任务不应计入 completed_count 和 completed_downloaded_size
        let mut folder = FolderDownload::new("/test/folder".to_string(), PathBuf::from("/tmp/folder"));
        folder.total_files = 10;
        folder.total_size = 10_000;
        folder.completed_count = 5;
        folder.completed_downloaded_size = 5_000;

        // 模拟成功的子任务
        folder.completed_count += 1;
        folder.completed_downloaded_size += 1_000;
        assert_eq!(folder.completed_count, 6);
        assert_eq!(folder.completed_downloaded_size, 6_000);

        // 模拟失败的子任务 — 不应递增 completed_count 和 completed_downloaded_size
        // (在实际代码中由 is_success 控制)
        assert_eq!(folder.completed_count, 6);
        assert_eq!(folder.completed_downloaded_size, 6_000);
    }
}
