// 上传/下载任务进度摘要（供侧边栏 / 底部导航的菜单进度使用）
//
// 口径与上传/下载列表页一致：按列表行数任务，文件夹下载算 1 个任务（不展开子任务），
// 与列表页「全部 / 已完成 / 失败」的计数相同；用户点「清除已完成」后相应减少。
// 剩余数 / 速度 / 字节进度只看未结束的任务（进度条只反映还在传的这部分）。
//
// 前端每隔几秒轮询一次，所以这里不复用列表接口：内存任务只读字段、不克隆；
// 历史库只查 id / 状态列，不读整行。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{extract::State, Json};
use serde::Serialize;

use crate::downloader::{DownloadTask, FolderDownload, FolderStatus, TaskStatus};
use crate::persistence::HistoryDbManager;
use crate::server::AppState;
use crate::uploader::{UploadTask, UploadTaskStatus};

use super::ApiResponse;

/// 列表页从历史库取已完成任务的条数上限（与 DownloadManager / UploadManager 的 `get_all_tasks` 一致）
const HISTORY_LIST_LIMIT: usize = 500;

/// 任务的终态分类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitState {
    /// 未到终态（等待 / 进行中 / 暂停 / 扫描中）
    Running,
    Completed,
    Failed,
}

/// 列表里的一行：单文件任务或一个文件夹任务
#[derive(Debug, Clone)]
struct SummaryUnit {
    state: UnitState,
    bytes_total: u64,
    bytes_done: u64,
    speed: u64,
    /// 已暂停（仍算未结束，但不在传输）
    paused: bool,
    /// 文件夹正在扫描，总量还会增长（暂停中的文件夹不算）
    scanning: bool,
}

impl SummaryUnit {
    /// 只在历史库里的任务：只计数，不参与字节进度
    fn history(state: UnitState) -> Self {
        Self {
            state,
            bytes_total: 0,
            bytes_done: 0,
            speed: 0,
            paused: false,
            scanning: false,
        }
    }
}

/// 单个方向（上传或下载）的进度摘要
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TransferSummary {
    /// 列表里的任务总数（文件夹算 1 个）
    pub total: u64,
    /// 已完成的任务数
    pub completed: u64,
    /// 失败的任务数
    pub failed: u64,
    /// 未结束的任务数（含暂停）；为 0 时前端隐藏菜单进度
    pub remaining: u64,
    /// 未结束且未暂停的任务数；remaining > 0 而 active == 0 表示全部暂停
    pub active: u64,
    /// 未结束任务的总字节
    pub total_bytes: u64,
    /// 未结束任务的已完成字节
    pub done_bytes: u64,
    /// 当前总速度（字节/秒）
    pub speed: u64,
    /// 是否有文件夹正在扫描（总量不确定）
    pub scanning: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TasksSummary {
    pub download: TransferSummary,
    pub upload: TransferSummary,
}

/// 把列表行聚合成摘要
fn aggregate(units: &[SummaryUnit]) -> TransferSummary {
    let mut s = TransferSummary::default();
    for u in units {
        s.total += 1;
        match u.state {
            UnitState::Completed => s.completed += 1,
            UnitState::Failed => s.failed += 1,
            UnitState::Running => {
                s.remaining += 1;
                s.active += u64::from(!u.paused);
                s.speed += u.speed;
                s.scanning |= u.scanning;
                s.total_bytes += u.bytes_total;
                s.done_bytes += u.bytes_done.min(u.bytes_total);
            }
        }
    }
    s
}

fn download_task_unit(t: &DownloadTask) -> SummaryUnit {
    let state = match t.status {
        TaskStatus::Completed => UnitState::Completed,
        TaskStatus::Failed => UnitState::Failed,
        _ => UnitState::Running,
    };
    SummaryUnit {
        state,
        bytes_total: t.total_size,
        bytes_done: t.downloaded_size,
        speed: if t.status == TaskStatus::Downloading { t.speed } else { 0 },
        paused: t.status == TaskStatus::Paused,
        scanning: false,
    }
}

/// 文件夹任务；取消的文件夹不计入（返回 None）
fn folder_unit(f: &FolderDownload, speed: u64) -> Option<SummaryUnit> {
    let state = match f.status {
        FolderStatus::Completed => UnitState::Completed,
        FolderStatus::Failed => UnitState::Failed,
        FolderStatus::Cancelled => return None,
        _ => UnitState::Running,
    };
    Some(SummaryUnit {
        state,
        bytes_total: f.total_size,
        // 冲突策略跳过的字节算作已处理（与下载页进度条口径一致）
        bytes_done: f.downloaded_size + f.skipped_size,
        speed,
        paused: f.status == FolderStatus::Paused,
        // 扫描中途暂停的文件夹 scan_completed 仍为 false，不能据此判断，只看状态
        scanning: f.status == FolderStatus::Scanning,
    })
}

/// 只在历史库里的文件夹；取消的不计入（与内存中的文件夹口径一致）
fn history_folder_unit(status: &str) -> Option<SummaryUnit> {
    match status {
        "cancelled" => None,
        "failed" => Some(SummaryUnit::history(UnitState::Failed)),
        // 进了历史库的文件夹都已结束，其它状态按已完成计
        _ => Some(SummaryUnit::history(UnitState::Completed)),
    }
}

fn upload_task_unit(t: &UploadTask) -> SummaryUnit {
    let state = match t.status {
        UploadTaskStatus::Completed | UploadTaskStatus::RapidUploadSuccess => {
            UnitState::Completed
        }
        UploadTaskStatus::Failed => UnitState::Failed,
        _ => UnitState::Running,
    };
    SummaryUnit {
        state,
        bytes_total: t.total_size,
        bytes_done: t.uploaded_size,
        speed: if t.status == UploadTaskStatus::Uploading { t.speed } else { 0 },
        paused: t.status == UploadTaskStatus::Paused,
        scanning: false,
    }
}

async fn history_db(app_state: &AppState) -> Option<Arc<HistoryDbManager>> {
    app_state.persistence_manager.lock().await.history_db().cloned()
}

/// 列表页会从历史库补上的已完成任务 id
///
/// 调用方遍历内存任务时把命中的 id 移除，剩下的就是只在历史库里、要额外计数的
/// （与列表页按 id 去重一致）。
fn history_completed_ids(
    db: Option<&HistoryDbManager>,
    task_type: &str,
    include_grouped: bool,
) -> HashSet<String> {
    let Some(db) = db else {
        return HashSet::new();
    };
    match db.list_recent_completed_task_ids(task_type, HISTORY_LIST_LIMIT) {
        Ok(rows) => rows
            .into_iter()
            .filter(|(_, grouped)| include_grouped || !grouped)
            .map(|(id, _)| id)
            .collect(),
        Err(e) => {
            tracing::warn!("读取{}历史任务失败: {}", task_type, e);
            HashSet::new()
        }
    }
}

async fn download_summary(app_state: &AppState) -> TransferSummary {
    let db = history_db(app_state).await;
    // 下载列表里文件夹子任务不单独成行，历史库只补单文件任务
    let mut history_ids = history_completed_ids(db.as_deref(), "download", false);
    let mut history_folders: HashMap<String, String> = match db.as_deref() {
        Some(db) => db.list_folder_history_statuses().unwrap_or_else(|e| {
            tracing::warn!("读取文件夹历史失败: {}", e);
            Vec::new()
        }),
        None => Vec::new(),
    }
    .into_iter()
    .collect();

    let mut units = Vec::new();
    // 文件夹子任务不单独计数，只把速度汇总到所属文件夹
    let mut folder_speed: HashMap<String, u64> = HashMap::new();

    for (_uid, dm) in app_state.list_download_managers() {
        dm.visit_tasks(|t| {
            history_ids.remove(&t.id);
            if t.is_backup {
                return;
            }
            match &t.group_id {
                Some(group_id) => {
                    if t.status == TaskStatus::Downloading && t.speed > 0 {
                        *folder_speed.entry(group_id.clone()).or_default() += t.speed;
                    }
                }
                None => units.push(download_task_unit(t)),
            }
        })
        .await;
    }

    app_state
        .folder_download_manager
        .visit_folders(|f| {
            history_folders.remove(&f.id);
            // 内部隐藏文件夹下载（分享同步等）不进「下载管理」，这里也不统计
            if f.backup_config_id.is_some() {
                return;
            }
            let speed = folder_speed.get(&f.id).copied().unwrap_or(0);
            units.extend(folder_unit(f, speed));
        })
        .await;

    units.extend(history_ids.iter().map(|_| SummaryUnit::history(UnitState::Completed)));
    units.extend(history_folders.values().filter_map(|status| history_folder_unit(status)));

    aggregate(&units)
}

async fn upload_summary(app_state: &AppState) -> TransferSummary {
    let db = history_db(app_state).await;
    let mut history_ids = history_completed_ids(db.as_deref(), "upload", true);

    let mut units = Vec::new();
    for (_uid, um) in app_state.list_upload_managers() {
        um.visit_tasks(|t| {
            history_ids.remove(&t.id);
            if !t.is_backup {
                units.push(upload_task_unit(t));
            }
        })
        .await;
    }
    units.extend(history_ids.iter().map(|_| SummaryUnit::history(UnitState::Completed)));

    aggregate(&units)
}

/// GET /api/v1/tasks/summary
/// 上传/下载进度摘要（跨账号聚合，口径同列表页）
pub async fn get_tasks_summary(
    State(app_state): State<AppState>,
) -> Json<ApiResponse<TasksSummary>> {
    let (download, upload) =
        tokio::join!(download_summary(&app_state), upload_summary(&app_state));
    Json(ApiResponse::success(TasksSummary { download, upload }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(state: UnitState, bytes: (u64, u64)) -> SummaryUnit {
        SummaryUnit {
            state,
            bytes_total: bytes.0,
            bytes_done: bytes.1,
            speed: 0,
            paused: false,
            scanning: false,
        }
    }

    #[test]
    fn test_counts_tasks_like_list_but_bytes_only_unfinished() {
        // 列表：100 个已完成（部分只在历史库里）+ 1 个下载中 → 100/101，进度条只看那 1 个
        let mut units: Vec<_> = (0..60).map(|_| unit(UnitState::Completed, (10, 10))).collect();
        units.extend((0..40).map(|_| SummaryUnit::history(UnitState::Completed)));
        units.push(unit(UnitState::Running, (100, 40)));

        let s = aggregate(&units);
        assert_eq!((s.total, s.completed, s.remaining), (101, 100, 1));
        assert_eq!((s.total_bytes, s.done_bytes), (100, 40));
    }

    #[test]
    fn test_all_finished_has_no_remaining() {
        let units = vec![unit(UnitState::Completed, (10, 10)), unit(UnitState::Failed, (10, 3))];
        let s = aggregate(&units);
        assert_eq!((s.total, s.completed, s.failed, s.remaining), (2, 1, 1, 0));
        assert_eq!(aggregate(&[]), TransferSummary::default());
    }

    #[test]
    fn test_failed_counted_but_bytes_excluded() {
        let units = vec![unit(UnitState::Running, (100, 50)), unit(UnitState::Failed, (1000, 10))];
        let s = aggregate(&units);
        assert_eq!((s.total, s.completed, s.failed, s.remaining), (2, 0, 1, 1));
        assert_eq!((s.total_bytes, s.done_bytes), (100, 50));
    }

    #[test]
    fn test_folder_counts_as_one_task() {
        // 文件夹里有很多文件也只算 1 个任务；速度汇总
        let mut folder = FolderDownload::new("/remote/dir".to_string(), std::path::PathBuf::from("/local/dir"));
        folder.status = FolderStatus::Downloading;
        folder.total_files = 50;
        folder.completed_count = 20;
        folder.total_size = 5000;
        folder.downloaded_size = 2000;
        let mut file = unit(UnitState::Running, (100, 10));
        file.speed = 200;

        let s = aggregate(&[folder_unit(&folder, 300).unwrap(), file]);
        assert_eq!((s.total, s.completed, s.remaining), (2, 0, 2));
        assert_eq!((s.total_bytes, s.done_bytes, s.speed), (5100, 2010, 500));
    }

    #[test]
    fn test_paused_units_counted_but_not_active() {
        let mut paused_a = unit(UnitState::Running, (5000, 2000));
        paused_a.paused = true;
        let mut paused_b = unit(UnitState::Running, (100, 10));
        paused_b.paused = true;

        let s = aggregate(&[paused_a.clone(), paused_b]);
        assert_eq!((s.remaining, s.active), (2, 0));

        let running = unit(UnitState::Running, (100, 0));
        assert_eq!(aggregate(&[paused_a, running]).active, 1);
    }

    #[test]
    fn test_folder_scanning_only_when_status_scanning() {
        let mut f = FolderDownload::new("/remote/dir".to_string(), std::path::PathBuf::from("/local/dir"));
        f.scan_completed = false;

        f.status = FolderStatus::Paused;
        let u = folder_unit(&f, 0).unwrap();
        assert!(u.paused && !u.scanning, "扫描中途暂停的文件夹不应算扫描中");

        f.status = FolderStatus::Scanning;
        let u = folder_unit(&f, 0).unwrap();
        assert!(!u.paused && u.scanning);

        f.status = FolderStatus::Cancelled;
        assert!(folder_unit(&f, 0).is_none());
    }

    #[test]
    fn test_history_folder_units() {
        assert_eq!(history_folder_unit("completed").unwrap().state, UnitState::Completed);
        assert_eq!(history_folder_unit("failed").unwrap().state, UnitState::Failed);
        assert!(history_folder_unit("cancelled").is_none());
    }

    #[test]
    fn test_done_bytes_clamped_to_total() {
        let units = vec![unit(UnitState::Running, (100, 150))];
        assert_eq!(aggregate(&units).done_bytes, 100);
    }
}
