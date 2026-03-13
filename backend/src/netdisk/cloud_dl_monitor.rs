//! 离线下载任务监听服务
//!
//! 本模块实现后端轮询和 WebSocket 推送功能，用于：
//! - 实时监听离线下载任务状态变化
//! - 智能预测任务完成时间，优化轮询间隔
//! - 支持自动下载功能（任务完成后自动触发本地下载）
//!
//! ## 设计要点
//! - 轮询逻辑在后端统一管理，避免多客户端重复请求
//! - 采用渐进式智能轮询策略，避免风控
//! - 无自动下载任务时完全停止轮询（0 请求）

use crate::netdisk::{AutoDownloadConfig, CloudDlTaskInfo, NetdiskClient};
use crate::persistence::{CloudDlAutoDownloadConfig, HistoryDbManager};
use crate::server::events::{CloudDlEvent as WsCloudDlEvent, TaskEvent};
use crate::server::websocket::WebSocketManager;
use anyhow::Result;
use rand::Rng;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, warn};

// =====================================================
// 离线下载事件枚举
// =====================================================

/// 离线下载事件
///
/// 用于 WebSocket 推送的事件类型
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum CloudDlEvent {
    /// 任务状态变化
    StatusChanged {
        task_id: i64,
        old_status: Option<i32>,
        new_status: i32,
        task: CloudDlTaskInfo,
    },
    /// 任务完成（可触发自动下载）
    TaskCompleted {
        task_id: i64,
        task: CloudDlTaskInfo,
        auto_download_config: Option<AutoDownloadConfig>,
    },
    /// 进度更新
    ProgressUpdate {
        task_id: i64,
        finished_size: i64,
        file_size: i64,
        progress_percent: f32,
    },
    /// 任务列表刷新（初始加载或手动刷新）
    TaskListRefreshed { tasks: Vec<CloudDlTaskInfo> },
}

impl CloudDlEvent {
    /// 获取事件类型名称
    pub fn event_type_name(&self) -> &'static str {
        match self {
            CloudDlEvent::StatusChanged { .. } => "status_changed",
            CloudDlEvent::TaskCompleted { .. } => "task_completed",
            CloudDlEvent::ProgressUpdate { .. } => "progress_update",
            CloudDlEvent::TaskListRefreshed { .. } => "task_list_refreshed",
        }
    }

    /// 获取任务 ID（如果有）
    pub fn task_id(&self) -> Option<i64> {
        match self {
            CloudDlEvent::StatusChanged { task_id, .. } => Some(*task_id),
            CloudDlEvent::TaskCompleted { task_id, .. } => Some(*task_id),
            CloudDlEvent::ProgressUpdate { task_id, .. } => Some(*task_id),
            CloudDlEvent::TaskListRefreshed { .. } => None,
        }
    }
}

// =====================================================
// 轮询配置
// =====================================================

/// 轮询配置
///
/// 定义轮询间隔和退避策略
#[derive(Debug, Clone)]
pub struct PollingConfig {
    // 模式1：用户在页面（实时监听）
    /// 有进行中任务时的轮询间隔（默认 30 秒）
    pub active_interval: Duration,
    /// 无进行中任务时的轮询间隔（默认 60 秒）
    pub idle_interval: Duration,
    /// 退避倍数（默认 1.5）
    pub backoff_multiplier: f32,

    // 模式2：自动下载监听
    /// 最小轮询间隔（默认 3 分钟）
    pub min_interval: Duration,
    /// 最大检查间隔（检查点上限，默认 60 分钟）
    pub max_check_interval: Duration,
    /// 提前检查比例（默认 0.8，即提前 20% 检查）
    pub check_before_completion: f32,
    /// 随机抖动比例（默认 ±15%）
    pub jitter_percent: f32,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            active_interval: Duration::from_secs(15),      // 🔥 用户在页面时 15 秒轮询，平衡体验和风控
            idle_interval: Duration::from_secs(60),
            backoff_multiplier: 1.5,
            min_interval: Duration::from_secs(15),         // 🔥 自动下载最小间隔也调整为 15 秒
            max_check_interval: Duration::from_secs(3600), // 60 分钟
            check_before_completion: 0.8,
            jitter_percent: 0.15,
        }
    }
}

impl PollingConfig {
    /// 创建用于测试的快速配置
    #[cfg(test)]
    pub fn fast_for_testing() -> Self {
        Self {
            active_interval: Duration::from_millis(100),
            idle_interval: Duration::from_millis(200),
            backoff_multiplier: 1.5,
            min_interval: Duration::from_millis(50),
            max_check_interval: Duration::from_secs(1),
            check_before_completion: 0.8,
            jitter_percent: 0.1,
        }
    }
}

// =====================================================
// 任务进度追踪器
// =====================================================

/// 任务进度追踪器
///
/// 用于智能预测任务完成时间
#[derive(Debug)]
pub struct TaskProgressTracker {
    /// 任务 ID
    task_id: i64,
    /// 文件总大小
    file_size: i64,
    /// 进度历史记录 (时间戳, 已完成大小)
    history: VecDeque<(Instant, i64)>,
    /// 预测完成时间
    estimated_completion: Option<Instant>,
}

impl TaskProgressTracker {
    /// 创建新的进度追踪器
    pub fn new(task_id: i64, file_size: i64) -> Self {
        Self {
            task_id,
            file_size,
            history: VecDeque::with_capacity(5),
            estimated_completion: None,
        }
    }

    /// 获取任务 ID
    pub fn task_id(&self) -> i64 {
        self.task_id
    }

    /// 获取预测完成时间
    pub fn estimated_completion(&self) -> Option<Instant> {
        self.estimated_completion
    }

    /// 更新进度并计算预测完成时间
    ///
    /// 返回预测的剩余时间（如果可计算）
    pub fn update(&mut self, finished_size: i64) -> Option<Duration> {
        let now = Instant::now();
        self.history.push_back((now, finished_size));

        // 只保留最近 5 条记录
        while self.history.len() > 5 {
            self.history.pop_front();
        }

        // 至少需要 2 条记录才能计算速度
        if self.history.len() < 2 {
            return None;
        }

        // 计算平均速度
        let (first_time, first_size) = self.history.front().unwrap();
        let (last_time, last_size) = self.history.back().unwrap();

        let time_diff = last_time.duration_since(*first_time);
        let size_diff = last_size - first_size;

        if time_diff.as_secs() == 0 || size_diff <= 0 {
            return None;
        }

        let speed = size_diff as f64 / time_diff.as_secs_f64();
        let remaining = self.file_size - finished_size;

        if remaining <= 0 {
            self.estimated_completion = Some(now);
            return Some(Duration::ZERO);
        }

        let remaining_secs = remaining as f64 / speed;
        let estimated_remaining = Duration::from_secs_f64(remaining_secs);
        self.estimated_completion = Some(now + estimated_remaining);

        Some(estimated_remaining)
    }

    /// 获取历史记录数量
    pub fn history_count(&self) -> usize {
        self.history.len()
    }
}

// =====================================================
// 事件回调类型
// =====================================================

/// 事件回调函数类型
pub type EventCallback = Arc<dyn Fn(CloudDlEvent) + Send + Sync>;

// =====================================================
// 离线任务监听服务
// =====================================================

/// 离线任务监听服务
///
/// 负责后台轮询离线下载任务状态，并通过回调推送事件
pub struct CloudDlMonitor {
    /// 网盘客户端（支持代理热更新）
    client: Arc<StdRwLock<NetdiskClient>>,
    /// 轮询配置
    config: PollingConfig,
    /// 自动下载配置（task_id -> config）
    auto_download_configs: Arc<RwLock<HashMap<i64, AutoDownloadConfig>>>,
    /// 进度追踪器（task_id -> tracker）
    progress_trackers: Arc<RwLock<HashMap<i64, TaskProgressTracker>>>,
    /// 订阅者计数（用户在页面时 > 0）
    subscriber_count: Arc<AtomicUsize>,
    /// 新任务通知
    new_task_notify: Arc<Notify>,
    /// 事件回调列表
    event_callbacks: Arc<RwLock<Vec<EventCallback>>>,
    /// 是否正在运行
    running: Arc<std::sync::atomic::AtomicBool>,
    /// WebSocket 管理器（用于推送事件）
    ws_manager: Arc<RwLock<Option<Arc<WebSocketManager>>>>,
    /// 数据库路径（用于持久化自动下载配置）
    db_path: Arc<RwLock<Option<PathBuf>>>,
    /// 下载管理器（用于自动下载）
    download_manager: Arc<RwLock<Option<Arc<crate::downloader::DownloadManager>>>>,
    /// 文件夹下载管理器（用于自动下载文件夹）
    folder_download_manager: Arc<RwLock<Option<Arc<crate::downloader::FolderDownloadManager>>>>,
}

impl CloudDlMonitor {
    /// 创建新的监听服务
    pub fn new(client: Arc<NetdiskClient>) -> Self {
        let client_inner = Arc::try_unwrap(client)
            .unwrap_or_else(|arc| (*arc).clone());
        Self {
            client: Arc::new(StdRwLock::new(client_inner)),
            config: PollingConfig::default(),
            auto_download_configs: Arc::new(RwLock::new(HashMap::new())),
            progress_trackers: Arc::new(RwLock::new(HashMap::new())),
            subscriber_count: Arc::new(AtomicUsize::new(0)),
            new_task_notify: Arc::new(Notify::new()),
            event_callbacks: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ws_manager: Arc::new(RwLock::new(None)),
            db_path: Arc::new(RwLock::new(None)),
            download_manager: Arc::new(RwLock::new(None)),
            folder_download_manager: Arc::new(RwLock::new(None)),
        }
    }

    /// 使用自定义配置创建监听服务
    pub fn with_config(client: Arc<NetdiskClient>, config: PollingConfig) -> Self {
        let client_inner = Arc::try_unwrap(client)
            .unwrap_or_else(|arc| (*arc).clone());
        Self {
            client: Arc::new(StdRwLock::new(client_inner)),
            config,
            auto_download_configs: Arc::new(RwLock::new(HashMap::new())),
            progress_trackers: Arc::new(RwLock::new(HashMap::new())),
            subscriber_count: Arc::new(AtomicUsize::new(0)),
            new_task_notify: Arc::new(Notify::new()),
            event_callbacks: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ws_manager: Arc::new(RwLock::new(None)),
            db_path: Arc::new(RwLock::new(None)),
            download_manager: Arc::new(RwLock::new(None)),
            folder_download_manager: Arc::new(RwLock::new(None)),
        }
    }

    /// 热更新网盘客户端（代理变更时调用）
    pub fn update_client(&self, new_client: NetdiskClient) {
        *self.client.write().unwrap() = new_client;
        info!("✓ CloudDlMonitor NetdiskClient 已热更新");
    }

    /// 设置下载管理器（用于自动下载功能）
    pub async fn set_download_manager(&self, dm: Arc<crate::downloader::DownloadManager>) {
        *self.download_manager.write().await = Some(dm);
        info!("离线下载监听服务已设置下载管理器");
    }

    /// 设置文件夹下载管理器（用于自动下载文件夹）
    pub async fn set_folder_download_manager(&self, fdm: Arc<crate::downloader::FolderDownloadManager>) {
        *self.folder_download_manager.write().await = Some(fdm);
        info!("离线下载监听服务已设置文件夹下载管理器");
    }

    /// 设置数据库路径（用于持久化自动下载配置）
    pub async fn set_db_path(&self, db_path: PathBuf) {
        *self.db_path.write().await = Some(db_path);
        info!("离线下载监听服务已设置数据库路径");
    }

    /// 从数据库加载未触发的自动下载配置
    ///
    /// 在服务启动时调用，恢复之前注册但未完成的自动下载任务
    /// 会自动清理已完成或不存在的任务配置
    pub async fn load_auto_download_configs_from_db(&self) -> usize {
        let db_path = match self.db_path.read().await.clone() {
            Some(path) => path,
            None => {
                warn!("数据库路径未设置，跳过加载自动下载配置");
                return 0;
            }
        };

        let history_db = match HistoryDbManager::new(&db_path) {
            Ok(db) => db,
            Err(e) => {
                error!("打开历史数据库失败: {}", e);
                return 0;
            }
        };

        match history_db.load_pending_cloud_dl_auto_download() {
            Ok(configs) => {
                if configs.is_empty() {
                    return 0;
                }

                let config_count = configs.len();
                info!("从数据库加载了 {} 个待验证的自动下载配置", config_count);

                // 获取当前离线下载任务列表，验证配置是否有效
                let client_snap = self.client.read().unwrap().clone();
                let current_tasks = match client_snap.cloud_dl_list_task().await {
                    Ok(tasks) => tasks,
                    Err(e) => {
                        warn!("获取离线任务列表失败，跳过配置验证: {}", e);
                        // 如果获取失败，仍然加载配置（保守策略）
                        let mut auto_configs = self.auto_download_configs.write().await;
                        for config in configs {
                            auto_configs.insert(config.task_id, AutoDownloadConfig {
                                task_id: config.task_id,
                                enabled: config.enabled,
                                local_path: config.local_path,
                                ask_each_time: config.ask_each_time,
                            });
                        }
                        self.new_task_notify.notify_one();
                        return config_count;
                    }
                };

                // 构建任务状态映射：task_id -> status
                let task_status_map: std::collections::HashMap<i64, i32> = current_tasks
                    .iter()
                    .map(|t| (t.task_id, t.status))
                    .collect();

                let mut valid_count = 0;
                let mut cleaned_count = 0;
                let mut auto_configs = self.auto_download_configs.write().await;

                for config in configs {
                    match task_status_map.get(&config.task_id) {
                        Some(&status) if status == 1 => {
                            // 任务仍在进行中，保留配置
                            auto_configs.insert(config.task_id, AutoDownloadConfig {
                                task_id: config.task_id,
                                enabled: config.enabled,
                                local_path: config.local_path,
                                ask_each_time: config.ask_each_time,
                            });
                            valid_count += 1;
                        }
                        Some(&status) => {
                            // 任务已完成（status=0）或失败，清理配置
                            info!(
                                "清理已完成的自动下载配置: task_id={}, status={}",
                                config.task_id, status
                            );
                            if let Err(e) = history_db.mark_cloud_dl_auto_download_triggered(config.task_id) {
                                warn!("标记自动下载配置为已触发失败: {}", e);
                            }
                            cleaned_count += 1;
                        }
                        None => {
                            // 任务不存在，清理配置
                            info!(
                                "清理不存在的自动下载配置: task_id={}",
                                config.task_id
                            );
                            if let Err(e) = history_db.mark_cloud_dl_auto_download_triggered(config.task_id) {
                                warn!("标记自动下载配置为已触发失败: {}", e);
                            }
                            cleaned_count += 1;
                        }
                    }
                }

                if cleaned_count > 0 {
                    info!("清理了 {} 个过期的自动下载配置", cleaned_count);
                }

                if valid_count > 0 {
                    info!("恢复了 {} 个有效的自动下载配置", valid_count);
                    self.new_task_notify.notify_one();
                }

                valid_count
            }
            Err(e) => {
                error!("加载自动下载配置失败: {}", e);
                0
            }
        }
    }

    /// 设置 WebSocket 管理器
    pub async fn set_ws_manager(&self, ws_manager: Arc<WebSocketManager>) {
        *self.ws_manager.write().await = Some(ws_manager);
        info!("离线下载监听服务已设置 WebSocket 管理器");
    }

    /// 添加事件回调
    pub async fn add_event_callback(&self, callback: EventCallback) {
        let mut callbacks = self.event_callbacks.write().await;
        callbacks.push(callback);
    }

    /// 发布事件到所有回调和 WebSocket
    async fn publish_event(&self, event: CloudDlEvent) {
        // 记录事件发布
        info!(
            "发布离线下载事件: type={}, task_id={:?}",
            event.event_type_name(),
            event.task_id()
        );

        // 发布到回调
        let callbacks = self.event_callbacks.read().await;
        for callback in callbacks.iter() {
            callback(event.clone());
        }
        drop(callbacks);

        // 发布到 WebSocket
        if let Some(ref ws_manager) = *self.ws_manager.read().await {
            let ws_event = self.convert_to_ws_event(&event);
            ws_manager.send_if_subscribed(ws_event, None);
        } else {
            warn!("WebSocket 管理器未设置，无法推送事件");
        }
    }

    /// 将 CloudDlEvent 转换为 WebSocket TaskEvent
    fn convert_to_ws_event(&self, event: &CloudDlEvent) -> TaskEvent {
        match event {
            CloudDlEvent::StatusChanged { task_id, old_status, new_status, task } => {
                TaskEvent::CloudDl(WsCloudDlEvent::StatusChanged {
                    task_id: *task_id,
                    old_status: *old_status,
                    new_status: *new_status,
                    task: serde_json::to_value(task).unwrap_or_default(),
                })
            }
            CloudDlEvent::TaskCompleted { task_id, task, auto_download_config } => {
                TaskEvent::CloudDl(WsCloudDlEvent::TaskCompleted {
                    task_id: *task_id,
                    task: serde_json::to_value(task).unwrap_or_default(),
                    auto_download_config: auto_download_config.as_ref()
                        .and_then(|c| serde_json::to_value(c).ok()),
                })
            }
            CloudDlEvent::ProgressUpdate { task_id, finished_size, file_size, progress_percent } => {
                TaskEvent::CloudDl(WsCloudDlEvent::ProgressUpdate {
                    task_id: *task_id,
                    finished_size: *finished_size,
                    file_size: *file_size,
                    progress_percent: *progress_percent,
                })
            }
            CloudDlEvent::TaskListRefreshed { tasks } => {
                TaskEvent::CloudDl(WsCloudDlEvent::TaskListRefreshed {
                    tasks: tasks.iter()
                        .filter_map(|t| serde_json::to_value(t).ok())
                        .collect(),
                })
            }
        }
    }

    /// 获取订阅者数量
    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Relaxed)
    }

    /// 获取自动下载配置数量
    pub async fn auto_download_count(&self) -> usize {
        self.auto_download_configs.read().await.len()
    }

    /// 检查是否正在运行
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }


    // ==================== 主循环方法 ====================

    /// 启动监听服务
    ///
    /// 这是一个异步方法，会持续运行直到被停止
    pub async fn start(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("离线下载监听服务已在运行");
            return;
        }

        info!("离线下载监听服务已启动");

        let mut last_states: HashMap<i64, (i32, i64)> = HashMap::new();
        let mut unchanged_count = 0u32;

        loop {
            // 检查是否应该停止
            if !self.running.load(Ordering::Relaxed) {
                info!("离线下载监听服务收到停止信号");
                break;
            }

            // 判断是否需要轮询
            let has_subscribers = self.subscriber_count.load(Ordering::Relaxed) > 0;
            let has_auto_downloads = !self.auto_download_configs.read().await.is_empty();

            // 🔥 添加调试日志，帮助诊断轮询问题
            debug!(
                "轮询状态检查: has_subscribers={}, has_auto_downloads={}, subscriber_count={}, auto_download_count={}",
                has_subscribers,
                has_auto_downloads,
                self.subscriber_count.load(Ordering::Relaxed),
                self.auto_download_configs.read().await.len()
            );

            if !has_subscribers && !has_auto_downloads {
                // 完全空闲，等待新任务或停止信号
                debug!("无监听需求，等待新任务...");

                // 使用 select 同时等待通知和停止信号
                tokio::select! {
                    _ = self.new_task_notify.notified() => {
                        debug!("收到新任务通知");
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // 定期检查停止信号
                        if !self.running.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
                continue;
            }

            // 计算轮询间隔
            // 🔥 修复：当有自动下载配置时，优先使用自动下载间隔（更短），确保及时触发
            let interval = if has_auto_downloads {
                // 有自动下载配置时，使用智能预测间隔（更短）
                // 即使用户在页面，也要确保自动下载能及时触发
                let auto_interval = self.calculate_auto_download_interval().await;
                if has_subscribers {
                    // 用户在页面时，取两者中较短的
                    let realtime_interval = self.calculate_realtime_interval(unchanged_count).await;
                    auto_interval.min(realtime_interval)
                } else {
                    auto_interval
                }
            } else if has_subscribers {
                // 无自动下载配置，用户在页面
                self.calculate_realtime_interval(unchanged_count).await
            } else {
                // 无自动下载配置，无订阅者（理论上不会到这里）
                self.config.idle_interval
            };

            let jittered = self.add_jitter(interval);
            debug!("下次轮询间隔: {:?}", jittered);

            // 等待间隔时间，同时检查停止信号
            tokio::select! {
                _ = tokio::time::sleep(jittered) => {}
                _ = self.new_task_notify.notified() => {
                    debug!("收到新任务通知，立即轮询");
                }
            }

            // 再次检查停止信号
            if !self.running.load(Ordering::Relaxed) {
                break;
            }

            // 执行查询
            let client_snap = self.client.read().unwrap().clone();
            match client_snap.cloud_dl_list_task().await {
                Ok(mut tasks) => {
                    // 对进行中的任务查询详情以获取进度信息
                    let running_task_ids: Vec<i64> = tasks
                        .iter()
                        .filter(|t| t.status == 1)
                        .map(|t| t.task_id)
                        .collect();

                    if !running_task_ids.is_empty() {
                        match client_snap.cloud_dl_query_task(&running_task_ids).await {
                            Ok(details) => {
                                let detail_map: std::collections::HashMap<i64, CloudDlTaskInfo> =
                                    details.into_iter().map(|t| (t.task_id, t)).collect();

                                for task in &mut tasks {
                                    if let Some(detail) = detail_map.get(&task.task_id) {
                                        task.file_size = detail.file_size;
                                        task.finished_size = detail.finished_size;
                                        task.start_time = detail.start_time;
                                        task.finish_time = detail.finish_time;
                                        task.file_list = detail.file_list.clone();
                                    }
                                }
                                debug!("已更新 {} 个进行中任务的进度信息", detail_map.len());
                            }
                            Err(e) => {
                                warn!("查询任务详情失败: {}", e);
                            }
                        }
                    }

                    let mut has_changes = false;

                    for task in &tasks {
                        // 更新进度追踪器
                        self.update_progress_tracker(task).await;

                        let last_state = last_states.get(&task.task_id);

                        // 检测状态变化
                        if let Some((last_status, _)) = last_state {
                            if *last_status != task.status {
                                has_changes = true;

                                // 推送状态变化（仅当有订阅者时）
                                if has_subscribers {
                                    let event = CloudDlEvent::StatusChanged {
                                        task_id: task.task_id,
                                        old_status: Some(*last_status),
                                        new_status: task.status,
                                        task: task.clone(),
                                    };
                                    self.publish_event(event).await;
                                }

                                // 任务完成
                                if task.status == 0 {
                                    self.handle_task_completed(task).await;
                                }
                            }
                        } else {
                            // 新任务
                            has_changes = true;
                            if has_subscribers {
                                let event = CloudDlEvent::StatusChanged {
                                    task_id: task.task_id,
                                    old_status: None,
                                    new_status: task.status,
                                    task: task.clone(),
                                };
                                self.publish_event(event).await;
                            }
                        }

                        // 进度更新（仅当有订阅者且任务进行中时）
                        if has_subscribers && task.status == 1 {
                            if let Some((_, last_finished)) = last_state {
                                if task.finished_size != *last_finished {
                                    let progress = if task.file_size > 0 {
                                        (task.finished_size as f32 / task.file_size as f32) * 100.0
                                    } else {
                                        0.0
                                    };

                                    info!(
                                        "离线下载进度变化: task_id={}, finished={} -> {}, progress={:.1}%",
                                        task.task_id, last_finished, task.finished_size, progress
                                    );

                                    let event = CloudDlEvent::ProgressUpdate {
                                        task_id: task.task_id,
                                        finished_size: task.finished_size,
                                        file_size: task.file_size,
                                        progress_percent: progress,
                                    };
                                    self.publish_event(event).await;
                                }
                            }
                        }

                        last_states.insert(task.task_id, (task.status, task.finished_size));
                    }

                    unchanged_count = if has_changes { 0 } else { unchanged_count + 1 };
                }
                Err(e) => {
                    error!("离线任务轮询失败: {}", e);
                    unchanged_count += 1;
                }
            }
        }

        info!("离线下载监听服务已停止");
    }

    /// 停止监听服务
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.new_task_notify.notify_one();
        info!("离线下载监听服务停止信号已发送");
    }

    // ==================== 智能轮询逻辑 ====================

    /// 计算实时监听间隔（模式1）
    ///
    /// 根据连续无变化次数计算退避间隔
    async fn calculate_realtime_interval(&self, unchanged_count: u32) -> Duration {
        let base = self.config.active_interval;

        if unchanged_count >= 3 {
            let backoff = base.as_secs_f32()
                * self.config.backoff_multiplier.powi(unchanged_count as i32 - 2);
            Duration::from_secs_f32(backoff.min(self.config.idle_interval.as_secs_f32()))
        } else {
            base
        }
    }

    /// 计算自动下载监听间隔（模式2：智能预测）
    ///
    /// 根据任务进度预测下次检查时间
    async fn calculate_auto_download_interval(&self) -> Duration {
        let trackers = self.progress_trackers.read().await;
        let auto_configs = self.auto_download_configs.read().await;

        // 只关注开启了自动下载的任务
        let remaining_times: Vec<Duration> = trackers
            .values()
            .filter(|t| auto_configs.contains_key(&t.task_id))
            .filter_map(|t| t.estimated_completion)
            .map(|completion| completion.saturating_duration_since(Instant::now()))
            .filter(|d| d.as_secs() > 0)
            .collect();

        if remaining_times.is_empty() {
            // 无法预测，使用最小间隔
            return self.config.min_interval;
        }

        // 取最早完成时间
        let earliest = remaining_times.iter().min().unwrap();

        // 提前 20% 检查
        let check_time = Duration::from_secs_f64(
            earliest.as_secs_f64() * self.config.check_before_completion as f64,
        );

        // 应用限制：最小 3 分钟，最大 60 分钟（检查点）
        check_time
            .max(self.config.min_interval)
            .min(self.config.max_check_interval)
    }

    /// 更新进度追踪器
    async fn update_progress_tracker(&self, task: &CloudDlTaskInfo) {
        if task.status != 1 {
            // 非进行中任务，移除追踪器
            self.progress_trackers.write().await.remove(&task.task_id);
            return;
        }

        let mut trackers = self.progress_trackers.write().await;
        let tracker = trackers
            .entry(task.task_id)
            .or_insert_with(|| TaskProgressTracker::new(task.task_id, task.file_size));

        tracker.update(task.finished_size);
    }

    /// 处理任务完成
    async fn handle_task_completed(&self, task: &CloudDlTaskInfo) {
        // 🔥 首先检查数据库中是否已经触发过自动下载（防止重复触发）
        let already_triggered = if let Some(db_path) = self.db_path.read().await.clone() {
            if let Ok(history_db) = HistoryDbManager::new(&db_path) {
                match history_db.get_cloud_dl_auto_download(task.task_id) {
                    Ok(Some(db_config)) => {
                        if db_config.triggered {
                            info!(
                                "离线任务 {} 的自动下载已触发过，跳过",
                                task.task_id
                            );
                            true
                        } else {
                            false
                        }
                    }
                    Ok(None) => false, // 数据库中没有配置，说明没有开启自动下载
                    Err(e) => {
                        warn!("查询自动下载配置失败: {}", e);
                        false
                    }
                }
            } else {
                false
            }
        } else {
            false
        };

        // 获取自动下载配置（从内存中移除）
        let auto_config = self
            .auto_download_configs
            .write()
            .await
            .remove(&task.task_id);

        // 如果有自动下载配置且不需要询问目录，且未触发过，直接执行自动下载
        if !already_triggered {
            if let Some(ref config) = auto_config {
                if config.enabled && !config.ask_each_time {
                    if let Some(ref local_path) = config.local_path {
                        info!(
                            "离线任务完成，执行自动下载: task_id={}, local_path={}",
                            task.task_id, local_path
                        );

                        // 🔥 先查询任务详情获取 file_list（因为轮询时只查询进行中的任务详情）
                        let client_snap2 = self.client.read().unwrap().clone();
                        let task_with_details = match client_snap2.cloud_dl_query_task(&[task.task_id]).await {
                            Ok(details) if !details.is_empty() => {
                                let detail = &details[0];
                                info!(
                                    "获取离线任务详情成功: task_id={}, file_list_count={}",
                                    task.task_id, detail.file_list.len()
                                );
                                detail.clone()
                            }
                            Ok(_) => {
                                warn!("获取离线任务详情返回空: task_id={}", task.task_id);
                                task.clone()
                            }
                            Err(e) => {
                                warn!("获取离线任务详情失败: task_id={}, 错误: {}", task.task_id, e);
                                task.clone()
                            }
                        };

                        self.execute_auto_download(&task_with_details, local_path).await;
                    }
                }
            }
        }

        // 标记为已触发（防止重复触发）
        if auto_config.is_some() && !already_triggered {
            if let Some(db_path) = self.db_path.read().await.clone() {
                if let Ok(history_db) = HistoryDbManager::new(&db_path) {
                    match history_db.mark_cloud_dl_auto_download_triggered(task.task_id) {
                        Ok(updated) => {
                            if updated {
                                info!("已标记离线下载自动下载为已触发: task_id={}", task.task_id);
                            }
                        }
                        Err(e) => {
                            error!("标记自动下载为已触发失败: {}", e);
                        }
                    }
                }
            }
        }

        // 推送完成事件（如果需要询问目录，前端会弹窗）
        let event = CloudDlEvent::TaskCompleted {
            task_id: task.task_id,
            task: task.clone(),
            auto_download_config: auto_config,
        };
        self.publish_event(event).await;

        // 清理追踪器
        self.progress_trackers.write().await.remove(&task.task_id);

        info!("离线下载任务完成: task_id={}", task.task_id);
    }

    /// 执行自动下载
    ///
    /// 根据离线下载任务的保存路径，获取文件信息并创建下载任务
    ///
    /// 🔥 重要：只下载当前离线任务对应的文件，不会下载 save_path 目录下的其他文件
    async fn execute_auto_download(&self, task: &CloudDlTaskInfo, local_path: &str) {
        let target_dir = std::path::Path::new(local_path);

        // 确保目标目录存在
        if !target_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(target_dir) {
                error!("创建自动下载目录失败: {}, 错误: {}", local_path, e);
                return;
            }
        }

        // 🔥 关键检查：必须有 file_list 才能执行自动下载
        // 正常情况下，任务完成后查询详情一定会返回 file_list
        // 如果为空，说明 API 调用失败或数据异常
        if task.file_list.is_empty() {
            warn!(
                "离线任务 {} 的 file_list 为空，这可能是 API 调用失败导致的。\
                为安全起见跳过自动下载，请手动下载。task_name={}",
                task.task_id, task.task_name
            );
            return;
        }

        // 获取离线下载保存路径下的文件列表
        let save_path = task.save_path.trim_end_matches('/');

        // 🔥 构建需要下载的文件名集合（从 file_list 中获取）
        let target_files: std::collections::HashSet<&str> = task
            .file_list
            .iter()
            .map(|f| f.file_name.as_str())
            .collect();

        info!(
            "自动下载: task_id={}, save_path={}, 目标文件: {:?}",
            task.task_id, save_path, target_files
        );

        let client_snap = self.client.read().unwrap().clone();
        match client_snap.get_file_list(save_path, 1, 1000).await {
            Ok(file_list) => {
                let download_manager = self.download_manager.read().await;
                let folder_download_manager = self.folder_download_manager.read().await;

                if download_manager.is_none() {
                    warn!("下载管理器未设置，无法执行自动下载");
                    return;
                }

                let dm = download_manager.as_ref().unwrap();
                let mut success_count = 0;
                let mut fail_count = 0;
                let mut skipped_count = 0;

                for file in &file_list.list {
                    // 🔥 只下载 file_list 中指定的文件
                    if !target_files.contains(file.server_filename.as_str()) {
                        skipped_count += 1;
                        debug!(
                            "跳过非目标文件: {} (不在 file_list 中)",
                            file.server_filename
                        );
                        continue;
                    }

                    if file.isdir == 1 {
                        // 文件夹下载
                        if let Some(ref fdm) = *folder_download_manager {
                            match fdm.create_folder_download_with_dir(
                                file.path.clone(),
                                target_dir,
                                None,
                                None,
                            ).await {
                                Ok(folder_id) => {
                                    info!(
                                        "自动下载文件夹任务创建成功: folder_id={}, name={}",
                                        folder_id, file.server_filename
                                    );
                                    success_count += 1;
                                }
                                Err(e) => {
                                    error!(
                                        "自动下载文件夹任务创建失败: name={}, 错误: {}",
                                        file.server_filename, e
                                    );
                                    fail_count += 1;
                                }
                            }
                        } else {
                            warn!("文件夹下载管理器未设置，跳过文件夹: {}", file.server_filename);
                            fail_count += 1;
                        }
                    } else {
                        // 单文件下载
                        match dm.create_task_with_dir(
                            file.fs_id,
                            file.path.clone(),
                            file.server_filename.clone(),
                            file.size,
                            target_dir,
                            None,
                        ).await {
                            Ok(task_id) => {
                                // 自动开始下载
                                if let Err(e) = dm.start_task(&task_id).await {
                                    warn!("启动下载任务失败: task_id={}, 错误: {}", task_id, e);
                                }
                                info!(
                                    "自动下载任务创建成功: task_id={}, name={}",
                                    task_id, file.server_filename
                                );
                                success_count += 1;
                            }
                            Err(e) => {
                                error!(
                                    "自动下载任务创建失败: name={}, 错误: {}",
                                    file.server_filename, e
                                );
                                fail_count += 1;
                            }
                        }
                    }
                }

                info!(
                    "自动下载执行完成: task_id={}, 成功={}, 失败={}, 跳过={}",
                    task.task_id, success_count, fail_count, skipped_count
                );
            }
            Err(e) => {
                error!(
                    "获取离线下载文件列表失败: save_path={}, 错误: {}",
                    save_path, e
                );
            }
        }
    }

    /// 添加随机抖动（±15%）
    ///
    /// 避免请求模式固定，降低风控风险
    fn add_jitter(&self, interval: Duration) -> Duration {
        let mut rng = rand::thread_rng();
        let jitter = 1.0 + rng.gen_range(-self.config.jitter_percent..self.config.jitter_percent);
        Duration::from_secs_f64(interval.as_secs_f64() * jitter as f64)
    }

    // ==================== 监听服务管理方法 ====================

    /// 注册自动下载配置
    ///
    /// 当用户创建离线任务并启用自动下载时调用
    /// 配置会同时保存到内存和数据库
    pub async fn register_auto_download(&self, task_id: i64, config: AutoDownloadConfig) {
        // 保存到内存
        self.auto_download_configs
            .write()
            .await
            .insert(task_id, config.clone());

        // 持久化到数据库
        if let Some(db_path) = self.db_path.read().await.clone() {
            if let Ok(history_db) = HistoryDbManager::new(&db_path) {
                let db_config = CloudDlAutoDownloadConfig {
                    task_id,
                    enabled: config.enabled,
                    local_path: config.local_path,
                    ask_each_time: config.ask_each_time,
                    created_at: chrono::Utc::now().timestamp(),
                    triggered: false,
                    triggered_at: None,
                };
                if let Err(e) = history_db.save_cloud_dl_auto_download(&db_config) {
                    error!("保存自动下载配置到数据库失败: {}", e);
                }
            }
        }

        // 通知监听服务有新任务
        self.new_task_notify.notify_one();
        info!("已注册自动下载配置: task_id={}", task_id);
    }

    /// 取消自动下载配置
    ///
    /// 同时从内存和数据库中删除
    pub async fn unregister_auto_download(&self, task_id: i64) -> Option<AutoDownloadConfig> {
        let config = self.auto_download_configs.write().await.remove(&task_id);

        // 从数据库删除
        if let Some(db_path) = self.db_path.read().await.clone() {
            if let Ok(history_db) = HistoryDbManager::new(&db_path) {
                if let Err(e) = history_db.remove_cloud_dl_auto_download(task_id) {
                    error!("从数据库删除自动下载配置失败: {}", e);
                }
            }
        }

        if config.is_some() {
            info!("已取消自动下载配置: task_id={}", task_id);
        }
        config
    }

    /// 获取自动下载配置
    pub async fn get_auto_download_config(&self, task_id: i64) -> Option<AutoDownloadConfig> {
        self.auto_download_configs.read().await.get(&task_id).cloned()
    }

    /// 增加订阅者
    ///
    /// 当用户打开离线下载页面时调用
    pub fn add_subscriber(&self) {
        let count = self.subscriber_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.new_task_notify.notify_one();
        info!("离线下载订阅者增加，当前数量: {}", count);
    }

    /// 减少订阅者
    ///
    /// 当用户离开离线下载页面时调用
    pub fn remove_subscriber(&self) {
        let prev = self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
        if prev > 0 {
            info!("离线下载订阅者减少，当前数量: {}", prev - 1);
        } else {
            // 防止下溢
            self.subscriber_count.store(0, Ordering::Relaxed);
            warn!("订阅者计数已为 0，无法继续减少");
        }
    }

    /// 手动触发刷新
    ///
    /// 返回当前任务列表并推送刷新事件
    pub async fn trigger_refresh(&self) -> Result<Vec<CloudDlTaskInfo>> {
        let client_snap = self.client.read().unwrap().clone();
        let tasks = client_snap.cloud_dl_list_task().await?;

        let event = CloudDlEvent::TaskListRefreshed {
            tasks: tasks.clone(),
        };
        self.publish_event(event).await;

        info!("手动刷新离线任务列表，共 {} 个任务", tasks.len());
        Ok(tasks)
    }

    /// 获取进度追踪器数量（用于测试）
    #[cfg(test)]
    pub async fn tracker_count(&self) -> usize {
        self.progress_trackers.read().await.len()
    }
}

// =====================================================
// 单元测试
// =====================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polling_config_default() {
        let config = PollingConfig::default();
        assert_eq!(config.active_interval, Duration::from_secs(15));
        assert_eq!(config.idle_interval, Duration::from_secs(60));
        assert_eq!(config.min_interval, Duration::from_secs(15));
        assert_eq!(config.max_check_interval, Duration::from_secs(3600));
        assert!((config.backoff_multiplier - 1.5).abs() < 0.01);
        assert!((config.check_before_completion - 0.8).abs() < 0.01);
        assert!((config.jitter_percent - 0.15).abs() < 0.01);
    }

    #[test]
    fn test_task_progress_tracker_new() {
        let tracker = TaskProgressTracker::new(123, 1000);
        assert_eq!(tracker.task_id(), 123);
        assert_eq!(tracker.file_size, 1000);
        assert_eq!(tracker.history_count(), 0);
        assert!(tracker.estimated_completion().is_none());
    }

    #[test]
    fn test_task_progress_tracker_update_single() {
        let mut tracker = TaskProgressTracker::new(123, 1000);

        // 单条记录无法计算速度
        let result = tracker.update(100);
        assert!(result.is_none());
        assert_eq!(tracker.history_count(), 1);
    }

    #[test]
    fn test_task_progress_tracker_update_multiple() {
        let mut tracker = TaskProgressTracker::new(123, 1000);

        // 第一条记录
        tracker.update(100);

        // 模拟时间流逝（通过直接操作 history）
        // 注意：这是一个简化测试，实际时间流逝需要 tokio::time::pause
        std::thread::sleep(Duration::from_millis(10));

        // 第二条记录
        let _result = tracker.update(200);

        // 有两条记录后应该能计算
        assert_eq!(tracker.history_count(), 2);
        // 由于时间间隔很短，可能无法计算（time_diff.as_secs() == 0）
        // 这是预期行为
    }

    #[test]
    fn test_task_progress_tracker_history_limit() {
        let mut tracker = TaskProgressTracker::new(123, 1000);

        // 添加超过 5 条记录
        for i in 0..10 {
            tracker.update(i * 100);
        }

        // 应该只保留最近 5 条
        assert_eq!(tracker.history_count(), 5);
    }

    #[test]
    fn test_cloud_dl_event_type_name() {
        let event = CloudDlEvent::StatusChanged {
            task_id: 1,
            old_status: Some(1),
            new_status: 0,
            task: create_test_task_info(1),
        };
        assert_eq!(event.event_type_name(), "status_changed");

        let event = CloudDlEvent::TaskCompleted {
            task_id: 1,
            task: create_test_task_info(1),
            auto_download_config: None,
        };
        assert_eq!(event.event_type_name(), "task_completed");

        let event = CloudDlEvent::ProgressUpdate {
            task_id: 1,
            finished_size: 500,
            file_size: 1000,
            progress_percent: 50.0,
        };
        assert_eq!(event.event_type_name(), "progress_update");

        let event = CloudDlEvent::TaskListRefreshed { tasks: vec![] };
        assert_eq!(event.event_type_name(), "task_list_refreshed");
    }

    #[test]
    fn test_cloud_dl_event_task_id() {
        let event = CloudDlEvent::StatusChanged {
            task_id: 123,
            old_status: None,
            new_status: 1,
            task: create_test_task_info(123),
        };
        assert_eq!(event.task_id(), Some(123));

        let event = CloudDlEvent::TaskListRefreshed { tasks: vec![] };
        assert_eq!(event.task_id(), None);
    }

    /// 创建测试用的任务信息
    fn create_test_task_info(task_id: i64) -> CloudDlTaskInfo {
        CloudDlTaskInfo {
            task_id,
            status: 1,
            status_text: "下载进行中".to_string(),
            file_size: 1000,
            finished_size: 500,
            create_time: 0,
            start_time: 0,
            finish_time: 0,
            save_path: "/".to_string(),
            source_url: "http://example.com/file.zip".to_string(),
            task_name: "file.zip".to_string(),
            od_type: 0,
            file_list: vec![],
            result: 0,
        }
    }
}
