//! 文件夹扫描管理器
//!
//! 异步扫描大文件夹，分批创建上传任务，支持：
//! - 后台扫描 + WebSocket 进度推送
//! - 检查点持久化与断点恢复
//! - 去重（通过 UploadManager.create_batch_tasks_dedup）
//! - 取消扫描

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::MemoryMonitor;
use crate::server::events::{ScanEvent, TaskEvent};
use crate::server::websocket::WebSocketManager;
use crate::uploader::folder::{BatchedScanIterator, ScanOptions, ScannedFile};
use crate::uploader::UploadManager;

// ============================================================================
// 数据结构
// ============================================================================

/// 扫描任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanTaskStatus {
    Scanning,
    Completed,
    Failed,
    Cancelled,
}

/// 扫描任务信息（内存状态）
#[derive(Debug, Clone)]
pub struct ScanTaskInfo {
    pub scan_task_id: String,
    pub local_folder: PathBuf,
    pub remote_folder: String,
    pub encrypt: bool,
    pub status: ScanTaskStatus,
    pub scanned_files: usize,
    pub created_tasks: usize,
    pub skipped_duplicates: usize,
    pub total_size: u64,
    pub scan_options: ScanOptions,
    pub cancel_token: CancellationToken,
}

/// 扫描检查点（持久化到 JSON 文件）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanCheckpoint {
    pub scan_task_id: String,
    pub local_folder: PathBuf,
    pub remote_folder: String,
    pub encrypt: bool,
    pub scanned_dirs: Vec<PathBuf>,
    pub pending_dirs: Vec<PathBuf>,
    pub current_dir: Option<PathBuf>,
    pub scanned_files_count: usize,
    pub created_tasks_count: usize,
    pub skipped_duplicates_count: usize,
    pub scan_options: ScanOptions,
}

// ============================================================================
// ScanManager 核心结构
// ============================================================================

pub struct ScanManager {
    active_scans: Arc<DashMap<String, ScanTaskInfo>>,
    upload_manager: Arc<UploadManager>,
    ws_manager: Arc<WebSocketManager>,
    memory_monitor: Arc<MemoryMonitor>,
    wal_dir: PathBuf,
    max_pending_tasks: usize,
}

impl ScanManager {
    pub fn new(
        upload_manager: Arc<UploadManager>,
        ws_manager: Arc<WebSocketManager>,
        memory_monitor: Arc<MemoryMonitor>,
        wal_dir: PathBuf,
        max_pending_tasks: usize,
    ) -> Self {
        Self {
            active_scans: Arc::new(DashMap::new()),
            upload_manager,
            ws_manager,
            memory_monitor,
            wal_dir,
            max_pending_tasks,
        }
    }

    /// 启动文件夹扫描（Task 6.3）
    pub async fn start_scan(
        &self,
        local_folder: PathBuf,
        remote_folder: String,
        scan_options: Option<ScanOptions>,
        encrypt: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
    ) -> Result<String> {
        // 验证路径
        if !local_folder.exists() || !local_folder.is_dir() {
            anyhow::bail!("扫描路径不存在或不是文件夹: {}", local_folder.display());
        }

        let scan_task_id = format!("scan_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let options = scan_options.unwrap_or_default();
        let cancel_token = CancellationToken::new();

        // 注册到 active_scans
        let task_info = ScanTaskInfo {
            scan_task_id: scan_task_id.clone(),
            local_folder: local_folder.clone(),
            remote_folder: remote_folder.clone(),
            encrypt,
            status: ScanTaskStatus::Scanning,
            scanned_files: 0,
            created_tasks: 0,
            skipped_duplicates: 0,
            total_size: 0,
            scan_options: options.clone(),
            cancel_token: cancel_token.clone(),
        };
        self.active_scans.insert(scan_task_id.clone(), task_info);

        // 推送 Started 事件
        self.ws_manager.send_if_subscribed(
            TaskEvent::Scan(ScanEvent::Started {
                scan_task_id: scan_task_id.clone(),
                local_folder: local_folder.to_string_lossy().to_string(),
                remote_folder: remote_folder.clone(),
            }),
            None,
        );

        // 创建 channel
        let (batch_tx, batch_rx) = mpsc::channel::<Vec<ScannedFile>>(4);

        // spawn_blocking: 生产端（同步迭代器）
        let scan_id_clone = scan_task_id.clone();
        let local_clone = local_folder.clone();
        let opts_clone = options.clone();
        let token_clone = cancel_token.clone();
        tokio::task::spawn_blocking(move || {
            Self::scan_producer(scan_id_clone, local_clone, opts_clone, batch_tx, token_clone);
        });

        // tokio::spawn: 消费端（异步任务创建）
        let active_scans = Arc::clone(&self.active_scans);
        let upload_manager = Arc::clone(&self.upload_manager);
        let ws_manager = Arc::clone(&self.ws_manager);
        let memory_monitor = Arc::clone(&self.memory_monitor);
        let wal_dir = self.wal_dir.clone();
        let max_pending = self.max_pending_tasks;
        let scan_id_clone2 = scan_task_id.clone();

        tokio::spawn(async move {
            Self::scan_loop(
                scan_id_clone2,
                batch_rx,
                remote_folder,
                encrypt,
                conflict_strategy, // Pass conflict_strategy to scan_loop
                cancel_token,
                active_scans,
                upload_manager,
                ws_manager,
                memory_monitor,
                wal_dir,
                max_pending,
            )
                .await;
        });

        info!("扫描任务已启动: {}", scan_task_id);
        Ok(scan_task_id)
    }

    /// 同步扫描生产端
    fn scan_producer(
        scan_task_id: String,
        local_folder: PathBuf,
        options: ScanOptions,
        batch_tx: mpsc::Sender<Vec<ScannedFile>>,
        cancel_token: CancellationToken,
    ) {
        let mut iterator = match BatchedScanIterator::new(&local_folder, options) {
            Ok(it) => it,
            Err(e) => {
                error!("创建扫描迭代器失败 ({}): {}", scan_task_id, e);
                return;
            }
        };

        loop {
            if cancel_token.is_cancelled() {
                info!("扫描生产端被取消: {}", scan_task_id);
                break;
            }
            match iterator.next_batch() {
                Ok(Some(batch)) => {
                    if batch_tx.blocking_send(batch).is_err() {
                        debug!("扫描 channel 已关闭: {}", scan_task_id);
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error!("扫描批次失败 ({}): {}", scan_task_id, e);
                    break;
                }
            }
        }
    }

    /// 异步消费端：接收批次并创建上传任务（Task 6.4）
    async fn scan_loop(
        scan_task_id: String,
        mut batch_rx: mpsc::Receiver<Vec<ScannedFile>>,
        remote_folder: String,
        encrypt: bool,
        conflict_strategy: Option<crate::uploader::UploadConflictStrategy>,
        cancel_token: CancellationToken,
        active_scans: Arc<DashMap<String, ScanTaskInfo>>,
        upload_manager: Arc<UploadManager>,
        ws_manager: Arc<WebSocketManager>,
        _memory_monitor: Arc<MemoryMonitor>,
        wal_dir: PathBuf,
        max_pending_tasks: usize,
    ) {
        let mut total_scanned: usize = 0;
        let mut total_created: usize = 0;
        let mut total_skipped: usize = 0;
        let mut total_size: u64 = 0;
        let mut last_checkpoint_time = Instant::now();

        while let Some(batch) = batch_rx.recv().await {
            if cancel_token.is_cancelled() {
                info!("扫描消费端被取消: {}", scan_task_id);
                break;
            }

            // 统计文件大小
            let batch_size: u64 = batch.iter().map(|f| f.size).sum();
            total_size += batch_size;
            total_scanned += batch.len();

            // 转换为 (PathBuf, String)
            let files: Vec<(PathBuf, String)> = batch
                .into_iter()
                .map(|f| {
                    let rel = f.relative_path.to_string_lossy().replace('\\', "/");
                    let original_remote = if remote_folder.ends_with('/') {
                        format!("{}{}", remote_folder, rel)
                    } else {
                        format!("{}/{}", remote_folder, rel)
                    };
                    (f.local_path, original_remote)
                })
                .collect();

            // 创建任务（带去重）
            match upload_manager
                .create_batch_tasks_dedup(files, encrypt, true, conflict_strategy)
                .await
            {
                Ok((new_ids, existing_ids)) => {
                    total_created += new_ids.len();
                    total_skipped += existing_ids.len();
                }
                Err(e) => {
                    error!("批量创建任务失败 ({}): {}", scan_task_id, e);
                }
            }

            // 更新内存状态
            if let Some(mut info) = active_scans.get_mut(&scan_task_id) {
                info.scanned_files = total_scanned;
                info.created_tasks = total_created;
                info.skipped_duplicates = total_skipped;
                info.total_size = total_size;
            }

            // 推送进度事件
            ws_manager.send_if_subscribed(
                TaskEvent::Scan(ScanEvent::Progress {
                    scan_task_id: scan_task_id.clone(),
                    scanned_files: total_scanned,
                    scanned_dirs: 0,
                    current_path: String::new(),
                    created_tasks: total_created,
                    skipped_duplicates: total_skipped,
                    total_size,
                }),
                None,
            );

            // 检查点节流写入（每 3 秒）
            if last_checkpoint_time.elapsed().as_secs() >= 3 {
                let checkpoint = ScanCheckpoint {
                    scan_task_id: scan_task_id.clone(),
                    local_folder: active_scans
                        .get(&scan_task_id)
                        .map(|i| i.local_folder.clone())
                        .unwrap_or_default(),
                    remote_folder: remote_folder.clone(),
                    encrypt,
                    scanned_dirs: Vec::new(),
                    pending_dirs: Vec::new(),
                    current_dir: None,
                    scanned_files_count: total_scanned,
                    created_tasks_count: total_created,
                    skipped_duplicates_count: total_skipped,
                    scan_options: active_scans
                        .get(&scan_task_id)
                        .map(|i| i.scan_options.clone())
                        .unwrap_or_default(),
                };
                save_checkpoint(&wal_dir, &checkpoint);
                last_checkpoint_time = Instant::now();
            }

            // 背压：等待活跃任务数降低
            while !cancel_token.is_cancelled() {
                // O(1) 活跃计数（AtomicUsize）
                let task_count = upload_manager.active_task_count();
                if task_count < max_pending_tasks {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }

        // 扫描结束：更新状态
        let final_status = if cancel_token.is_cancelled() {
            ScanTaskStatus::Cancelled
        } else {
            ScanTaskStatus::Completed
        };

        if let Some(mut info) = active_scans.get_mut(&scan_task_id) {
            info.status = final_status;
            info.scanned_files = total_scanned;
            info.created_tasks = total_created;
            info.skipped_duplicates = total_skipped;
            info.total_size = total_size;
        }

        // 推送完成/失败事件
        match final_status {
            ScanTaskStatus::Completed => {
                ws_manager.send_if_subscribed(
                    TaskEvent::Scan(ScanEvent::Completed {
                        scan_task_id: scan_task_id.clone(),
                        total_files: total_scanned,
                        total_size,
                        created_tasks: total_created,
                        skipped_duplicates: total_skipped,
                    }),
                    None,
                );
            }
            ScanTaskStatus::Cancelled => {
                ws_manager.send_if_subscribed(
                    TaskEvent::Scan(ScanEvent::Failed {
                        scan_task_id: scan_task_id.clone(),
                        error: "用户取消扫描".to_string(),
                    }),
                    None,
                );
            }
            _ => {}
        }

        // 删除检查点文件
        delete_checkpoint(&wal_dir, &scan_task_id);

        info!(
            "扫描任务完成: {} (scanned={}, created={}, skipped={}, size={})",
            scan_task_id, total_scanned, total_created, total_skipped, total_size
        );
    }

    /// 取消扫描（Task 6.5）
    pub fn cancel_scan(&self, scan_task_id: &str) -> bool {
        if let Some(info) = self.active_scans.get(scan_task_id) {
            if info.status == ScanTaskStatus::Scanning {
                info.cancel_token.cancel();
                return true;
            }
        }
        false
    }

    /// 查询扫描状态（Task 6.5）
    pub fn get_scan_status(&self, scan_task_id: &str) -> Option<ScanTaskInfo> {
        self.active_scans.get(scan_task_id).map(|v| v.clone())
    }

    /// 恢复中断的扫描任务（Task 8.3）
    pub async fn resume_interrupted_scans(&self) -> Result<usize> {
        let mut resumed = 0;
        let parent = self.wal_dir.clone();

        let entries: Vec<_> = match std::fs::read_dir(&parent) {
            Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
            Err(_) => return Ok(0),
        };

        for entry in entries {
            let path = entry.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) if n.starts_with("scan_") && n.ends_with(".json") => n.to_string(),
                _ => continue,
            };

            match load_checkpoint(&path) {
                Some(cp) => {
                    info!("恢复中断的扫描: {}", cp.scan_task_id);
                    if let Err(e) = self.start_scan(
                        cp.local_folder,
                        cp.remote_folder,
                        Some(cp.scan_options),
                        cp.encrypt,
                        None, // conflict_strategy - use default for resumed scans
                    ).await {
                        warn!("恢复扫描失败 ({}): {}", name, e);
                    } else {
                        resumed += 1;
                    }
                }
                None => {
                    warn!("无效的扫描检查点文件: {}", name);
                }
            }
        }

        if resumed > 0 {
            info!("恢复了 {} 个中断的扫描任务", resumed);
        }
        Ok(resumed)
    }
}

// ============================================================================
// 检查点持久化（Task 8.1）
// ============================================================================

fn save_checkpoint(wal_dir: &Path, checkpoint: &ScanCheckpoint) {
    let path = wal_dir.join(format!("{}.json", checkpoint.scan_task_id));
    match serde_json::to_string(checkpoint) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                warn!("写入扫描检查点失败: {}", e);
            }
        }
        Err(e) => warn!("序列化扫描检查点失败: {}", e),
    }
}

fn load_checkpoint(path: &Path) -> Option<ScanCheckpoint> {
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

fn delete_checkpoint(wal_dir: &Path, scan_task_id: &str) {
    let path = wal_dir.join(format!("{}.json", scan_task_id));
    if path.exists() {
        if let Err(e) = std::fs::remove_file(&path) {
            warn!("删除扫描检查点失败: {}", e);
        }
    }
}
