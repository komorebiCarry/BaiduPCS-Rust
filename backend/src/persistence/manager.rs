//! 持久化管理器
//!
//! 核心的持久化管理器，负责：
//! - 管理任务的持久化状态
//! - WAL 缓存的批量刷写
//! - 元数据的保存和更新
//! - 优雅关闭时的最终刷写
//!
//! ## 设计原则
//!
//! 1. **WAL 缓存**: 分片完成时先写入内存缓存，定期批量刷写到磁盘
//! 2. **异步刷写**: 使用独立的 tokio 任务进行后台刷写
//! 3. **优雅关闭**: 支持 shutdown 信号，确保关闭前完成最终刷写

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bit_set::BitSet;
use chrono::Timelike;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::config::PersistenceConfig;

use super::history;
use super::metadata::{delete_task_files, save_metadata, update_metadata};
use super::types::{TaskMetadata, TaskPersistenceInfo, TaskType};
use super::wal::{self, append_records, delete_wal_file, read_records};

/// 持久化管理器
///
/// 管理所有任务的持久化状态，包括 WAL 缓存、元数据和历史归档
pub struct PersistenceManager {
    /// 持久化配置
    config: PersistenceConfig,

    /// WAL/元数据目录
    wal_dir: PathBuf,

    /// 任务持久化信息映射表
    /// Key: task_id, Value: TaskPersistenceInfo
    tasks: Arc<DashMap<String, TaskPersistenceInfo>>,

    /// 历史任务缓存
    /// Key: task_id, Value: TaskMetadata
    history_cache: Arc<DashMap<String, TaskMetadata>>,

    /// 后台刷写任务句柄
    flush_task: Option<tokio::task::JoinHandle<()>>,

    /// 后台清理任务句柄
    cleanup_task: Option<tokio::task::JoinHandle<()>>,

    /// 后台历史归档任务句柄
    archive_task: Option<tokio::task::JoinHandle<()>>,

    /// shutdown 信号发送端
    shutdown_tx: broadcast::Sender<()>,
}

impl std::fmt::Debug for PersistenceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistenceManager")
            .field("wal_dir", &self.wal_dir)
            .field("tasks_count", &self.tasks.len())
            .field("history_cache_count", &self.history_cache.len())
            .field("auto_recover_tasks", &self.config.auto_recover_tasks)
            .finish_non_exhaustive()
    }
}

impl PersistenceManager {
    /// 创建持久化管理器
    ///
    /// # Arguments
    /// * `config` - 持久化配置
    /// * `base_dir` - 基础目录（WAL 目录将相对于此目录）
    pub fn new(config: PersistenceConfig, base_dir: &std::path::Path) -> Self {
        // 解析 WAL 目录路径
        let wal_dir = if std::path::Path::new(&config.wal_dir).is_absolute() {
            PathBuf::from(&config.wal_dir)
        } else {
            base_dir.join(&config.wal_dir)
        };

        // 确保 WAL 目录存在
        if let Err(e) = wal::ensure_wal_dir(&wal_dir) {
            error!("创建 WAL 目录失败: {:?}, 错误: {}", wal_dir, e);
        }

        let (shutdown_tx, _) = broadcast::channel(1);

        info!("持久化管理器已创建，WAL 目录: {:?}", wal_dir);

        Self {
            config,
            wal_dir,
            tasks: Arc::new(DashMap::new()),
            history_cache: Arc::new(DashMap::new()),
            flush_task: None,
            cleanup_task: None,
            archive_task: None,
            shutdown_tx,
        }
    }

    /// 获取 WAL 目录路径
    pub fn wal_dir(&self) -> &PathBuf {
        &self.wal_dir
    }

    /// 获取配置
    pub fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    /// 获取历史缓存引用
    pub fn history_cache(&self) -> &Arc<DashMap<String, TaskMetadata>> {
        &self.history_cache
    }

    // ========================================================================
    // 启动和关闭
    // ========================================================================

    /// 启动后台刷写任务
    ///
    /// 启动一个独立的 tokio 任务，定期将 WAL 缓存刷写到磁盘
    pub fn start(&mut self) {
        if self.flush_task.is_some() {
            warn!("后台刷写任务已在运行");
            return;
        }

        // 加载历史缓存
        self.load_history_cache();

        // 启动时执行一次归档
        self.archive_completed_tasks_once();

        let tasks = Arc::clone(&self.tasks);
        let wal_dir = self.wal_dir.clone();
        let flush_interval_ms = self.config.wal_flush_interval_ms;
        let shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            wal_flush_loop(tasks, wal_dir, flush_interval_ms, shutdown_rx).await;
        });

        self.flush_task = Some(handle);
        info!(
            "后台刷写任务已启动，刷写间隔: {}ms",
            self.config.wal_flush_interval_ms
        );

        // 启动后台清理任务
        self.start_cleanup_task();

        // 启动后台归档任务
        self.start_archive_task();
    }

    /// 加载历史缓存
    fn load_history_cache(&mut self) {
        match history::load_history_cache(&self.wal_dir) {
            Ok(cache) => {
                let count = cache.len();
                // 将加载的数据转移到 self.history_cache
                for entry in cache.into_iter() {
                    self.history_cache.insert(entry.0, entry.1);
                }
                info!("已加载 {} 条历史任务记录", count);
            }
            Err(e) => {
                error!("加载历史缓存失败: {}", e);
            }
        }
    }

    /// 启动时执行一次归档
    fn archive_completed_tasks_once(&self) {
        // 归档已完成的单文件任务
        match history::archive_completed_tasks(&self.wal_dir) {
            Ok(count) => {
                if count > 0 {
                    info!("启动时归档了 {} 个已完成任务", count);
                    // 重新加载历史缓存以包含新归档的任务
                    if let Ok(cache) = history::load_history_cache(&self.wal_dir) {
                        for entry in cache.into_iter() {
                            self.history_cache.insert(entry.0, entry.1);
                        }
                    }
                }
            }
            Err(e) => {
                error!("启动时归档失败: {}", e);
            }
        }

        // 归档已完成的文件夹任务
        match super::folder::archive_completed_folders(&self.wal_dir) {
            Ok(count) => {
                if count > 0 {
                    info!("启动时归档了 {} 个已完成文件夹", count);
                }
            }
            Err(e) => {
                error!("启动时文件夹归档失败: {}", e);
            }
        }
    }

    /// 启动后台归档任务
    fn start_archive_task(&mut self) {
        if self.archive_task.is_some() {
            return;
        }

        let wal_dir = self.wal_dir.clone();
        let history_cache = Arc::clone(&self.history_cache);
        let archive_hour = self.config.history_archive_hour;
        let archive_minute = self.config.history_archive_minute;
        let retention_days = self.config.history_retention_days;
        let shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            history_archive_loop(
                wal_dir,
                history_cache,
                archive_hour,
                archive_minute,
                retention_days,
                shutdown_rx,
            )
                .await;
        });

        self.archive_task = Some(handle);
        info!(
            "后台归档任务已启动，归档时间: {:02}:{:02}",
            self.config.history_archive_hour, self.config.history_archive_minute
        );
    }

    /// 🔥 启动后台清理任务
    ///
    /// 每小时检查一次，清理超过 retention_days 的未完成任务
    fn start_cleanup_task(&mut self) {
        if self.cleanup_task.is_some() {
            return;
        }

        let wal_dir = self.wal_dir.clone();
        let retention_days = self.config.wal_retention_days;
        let shutdown_rx = self.shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            wal_cleanup_loop(wal_dir, retention_days, shutdown_rx).await;
        });

        self.cleanup_task = Some(handle);
        info!(
            "后台清理任务已启动，保留天数: {} 天",
            self.config.wal_retention_days
        );
    }

    /// 关闭持久化管理器
    ///
    /// 发送关闭信号并等待后台任务完成最终刷写
    pub async fn shutdown(&mut self) {
        info!("正在关闭持久化管理器...");

        // 发送关闭信号
        let _ = self.shutdown_tx.send(());

        // 等待刷写任务完成
        if let Some(handle) = self.flush_task.take() {
            match handle.await {
                Ok(_) => info!("后台刷写任务已正常退出"),
                Err(e) => error!("后台刷写任务异常退出: {}", e),
            }
        }

        // 等待清理任务完成
        if let Some(handle) = self.cleanup_task.take() {
            match handle.await {
                Ok(_) => info!("后台清理任务已正常退出"),
                Err(e) => error!("后台清理任务异常退出: {}", e),
            }
        }

        // 等待归档任务完成
        if let Some(handle) = self.archive_task.take() {
            match handle.await {
                Ok(_) => info!("后台归档任务已正常退出"),
                Err(e) => error!("后台归档任务异常退出: {}", e),
            }
        }

        info!("持久化管理器已关闭");
    }

    // ========================================================================
    // 任务注册
    // ========================================================================

    /// 注册下载任务
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `fs_id` - 百度网盘文件 fs_id
    /// * `remote_path` - 远程文件路径
    /// * `local_path` - 本地保存路径
    /// * `file_size` - 文件大小（字节）
    /// * `chunk_size` - 分片大小（字节）
    /// * `total_chunks` - 总分片数
    /// * `group_id` - 文件夹下载组ID（单文件下载时为 None）
    /// * `group_root` - 文件夹根路径（单文件下载时为 None）
    /// * `relative_path` - 相对于根文件夹的路径（单文件下载时为 None）
    pub fn register_download_task(
        &self,
        task_id: String,
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        file_size: u64,
        chunk_size: u64,
        total_chunks: usize,
        group_id: Option<String>,
        group_root: Option<String>,
        relative_path: Option<String>,
    ) -> std::io::Result<()> {
        // 创建元数据
        let mut metadata = TaskMetadata::new_download(
            task_id.clone(),
            fs_id,
            remote_path,
            local_path,
            file_size,
            chunk_size,
            total_chunks,
        );

        // 设置文件夹下载组信息
        metadata.set_group_info(group_id, group_root, relative_path);

        // 保存元数据到文件
        save_metadata(&self.wal_dir, &metadata)?;

        // 创建内存状态
        let info = TaskPersistenceInfo::new_download(task_id.clone(), total_chunks);
        self.tasks.insert(task_id.clone(), info);

        debug!("已注册下载任务: {}", task_id);

        Ok(())
    }

    /// 注册上传任务
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `source_path` - 本地源文件路径
    /// * `target_path` - 远程目标路径
    /// * `file_size` - 文件大小（字节）
    /// * `chunk_size` - 分片大小（字节）
    /// * `total_chunks` - 总分片数
    pub fn register_upload_task(
        &self,
        task_id: String,
        source_path: PathBuf,
        target_path: String,
        file_size: u64,
        chunk_size: u64,
        total_chunks: usize,
    ) -> std::io::Result<()> {
        // 创建元数据
        let metadata = TaskMetadata::new_upload(
            task_id.clone(),
            source_path,
            target_path,
            file_size,
            chunk_size,
            total_chunks,
        );

        // 保存元数据到文件
        save_metadata(&self.wal_dir, &metadata)?;

        // 创建内存状态
        let info = TaskPersistenceInfo::new_upload(task_id.clone(), total_chunks);
        self.tasks.insert(task_id.clone(), info);

        debug!("已注册上传任务: {}", task_id);

        Ok(())
    }

    /// 注册转存任务
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `share_link` - 分享链接
    /// * `share_pwd` - 提取码（可选）
    /// * `target_path` - 转存目标路径
    /// * `auto_download` - 是否开启自动下载
    /// * `file_name` - 文件名称（用于展示）
    pub fn register_transfer_task(
        &self,
        task_id: String,
        share_link: String,
        share_pwd: Option<String>,
        target_path: String,
        auto_download: bool,
        file_name: Option<String>,
    ) -> std::io::Result<()> {
        // 创建元数据
        let metadata = TaskMetadata::new_transfer(
            task_id.clone(),
            share_link,
            share_pwd,
            target_path,
            auto_download,
            file_name,
        );

        // 保存元数据到文件
        save_metadata(&self.wal_dir, &metadata)?;

        // 创建内存状态
        let info = TaskPersistenceInfo::new_transfer(task_id.clone());
        self.tasks.insert(task_id.clone(), info);

        debug!("已注册转存任务: {}", task_id);

        Ok(())
    }

    // ========================================================================
    // 分片完成回调
    // ========================================================================

    /// 标记分片完成（下载任务）
    ///
    /// 将分片完成信息添加到 WAL 缓存，等待批量刷写
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `chunk_index` - 分片索引（0-based）
    pub fn on_chunk_completed(&self, task_id: &str, chunk_index: usize) {
        if let Some(mut info) = self.tasks.get_mut(task_id) {
            info.mark_chunk_completed(chunk_index);
            debug!("分片完成: task_id={}, chunk_index={}", task_id, chunk_index);
        } else {
            warn!("任务不存在，无法标记分片完成: task_id={}", task_id);
        }
    }

    /// 标记分片完成（上传任务，带 MD5）
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `chunk_index` - 分片索引（0-based）
    /// * `md5` - 分片 MD5
    pub fn on_chunk_completed_with_md5(&self, task_id: &str, chunk_index: usize, md5: String) {
        if let Some(mut info) = self.tasks.get_mut(task_id) {
            info.mark_chunk_completed_with_md5(chunk_index, md5);
            debug!(
                "分片完成(带MD5): task_id={}, chunk_index={}",
                task_id, chunk_index
            );
        } else {
            warn!("任务不存在，无法标记分片完成: task_id={}", task_id);
        }
    }

    // ========================================================================
    // 任务完成/删除清理
    // ========================================================================

    /// 任务完成时处理
    ///
    /// 1. 从内存中移除任务
    /// 2. 只删除 WAL 文件（保留元数据）
    /// 3. 更新元数据：标记为已完成
    /// 4. 添加到历史缓存
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    pub fn on_task_completed(&self, task_id: &str) -> std::io::Result<()> {
        // 1. 从内存中移除
        self.tasks.remove(task_id);

        // 2. 只删除 WAL 文件（保留元数据）
        if let Err(e) = delete_wal_file(&self.wal_dir, task_id) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("删除 WAL 文件失败: task_id={}, 错误: {}", task_id, e);
            }
        }

        // 3. 更新元数据：标记为已完成
        update_metadata(&self.wal_dir, task_id, |m| {
            m.mark_completed();
        })?;

        // 4. 加载完成的元数据并添加到历史缓存
        if let Some(metadata) = super::metadata::load_metadata(&self.wal_dir, task_id) {
            self.history_cache.insert(task_id.to_string(), metadata);
        }

        info!(
            "任务完成，已标记为已完成并添加到历史缓存: task_id={}",
            task_id
        );

        Ok(())
    }

    /// 任务删除时清理
    ///
    /// 1. 从内存中移除任务
    /// 2. 删除持久化文件（WAL 和元数据）
    /// 3. 从历史缓存和文件中删除
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    pub fn on_task_deleted(&self, task_id: &str) -> std::io::Result<()> {
        // 1. 从内存中移除
        self.tasks.remove(task_id);

        // 2. 删除持久化文件
        let deleted = delete_task_files(&self.wal_dir, task_id)?;

        // 3. 从历史缓存中删除
        self.history_cache.remove(task_id);

        // 4. 从历史文件中删除
        if let Err(e) = history::remove_from_history_file(&self.wal_dir, task_id) {
            warn!("从历史文件中删除任务失败: task_id={}, 错误: {}", task_id, e);
        }

        info!(
            "任务删除，已清理 {} 个文件并从历史中移除: task_id={}",
            deleted, task_id
        );

        Ok(())
    }

    // ========================================================================
    // 转存任务状态更新
    // ========================================================================

    /// 更新转存任务状态
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `status` - 新状态（checking_share, transferring, transferred, downloading, completed）
    pub fn update_transfer_status(&self, task_id: &str, status: &str) -> std::io::Result<()> {
        let status_owned = status.to_string();
        update_metadata(&self.wal_dir, task_id, move |m| {
            m.set_transfer_status(&status_owned);
        })?;

        debug!("已更新转存状态: task_id={}, status={}", task_id, status);

        Ok(())
    }

    /// 更新转存任务的关联下载任务 ID
    ///
    /// # Arguments
    /// * `task_id` - 转存任务 ID
    /// * `download_ids` - 关联的下载任务 ID 列表
    pub fn update_transfer_download_ids(
        &self,
        task_id: &str,
        download_ids: Vec<String>,
    ) -> std::io::Result<()> {
        update_metadata(&self.wal_dir, task_id, move |m| {
            m.set_download_task_ids(download_ids);
        })?;

        debug!("已更新转存关联下载任务: task_id={}", task_id);

        Ok(())
    }

    /// 设置下载任务的关联转存任务 ID
    ///
    /// # Arguments
    /// * `task_id` - 下载任务 ID
    /// * `transfer_task_id` - 转存任务 ID
    pub fn set_download_transfer_task_id(
        &self,
        task_id: &str,
        transfer_task_id: String,
    ) -> std::io::Result<()> {
        update_metadata(&self.wal_dir, task_id, move |m| {
            m.set_transfer_task_id(transfer_task_id);
        })?;

        debug!(
            "已设置下载任务关联转存任务: download_task_id={}, transfer_task_id={}",
            task_id, task_id
        );

        Ok(())
    }

    /// 更新转存文件名称
    ///
    /// # Arguments
    /// * `task_id` - 转存任务 ID
    /// * `file_name` - 文件名称
    pub fn update_transfer_file_name(
        &self,
        task_id: &str,
        file_name: String,
    ) -> std::io::Result<()> {
        update_metadata(&self.wal_dir, task_id, move |m| {
            m.set_transfer_file_name(file_name);
        })?;

        debug!("已更新转存文件名称: task_id={}", task_id);

        Ok(())
    }

    /// 更新任务错误信息
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `error_msg` - 错误信息
    pub fn update_task_error(&self, task_id: &str, error_msg: String) -> std::io::Result<()> {
        let error_owned = error_msg.clone();
        update_metadata(&self.wal_dir, task_id, move |m| {
            m.set_error_msg(error_owned);
        })?;

        debug!("已更新任务错误信息: task_id={}, error={}", task_id, error_msg);

        Ok(())
    }

    /// 更新上传任务的 upload_id
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `upload_id` - 百度网盘返回的 upload_id
    pub fn update_upload_id(&self, task_id: &str, upload_id: String) -> std::io::Result<()> {
        update_metadata(&self.wal_dir, task_id, move |m| {
            m.set_upload_id(upload_id);
        })?;

        debug!("已更新 upload_id: task_id={}", task_id);

        Ok(())
    }

    // ========================================================================
    // 查询方法
    // ========================================================================

    /// 获取任务的已完成分片集合
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    ///
    /// # Returns
    /// - `Some(BitSet)` - 已完成分片的位集合
    /// - `None` - 任务不存在
    pub fn get_completed_chunks(&self, task_id: &str) -> Option<BitSet> {
        self.tasks
            .get(task_id)
            .map(|info| info.completed_chunks.clone())
    }

    /// 获取任务的已完成分片数
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    ///
    /// # Returns
    /// - `Some(usize)` - 已完成分片数
    /// - `None` - 任务不存在
    pub fn get_completed_count(&self, task_id: &str) -> Option<usize> {
        self.tasks.get(task_id).map(|info| info.completed_count())
    }

    /// 检查分片是否已完成
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `chunk_index` - 分片索引
    ///
    /// # Returns
    /// - `Some(true)` - 分片已完成
    /// - `Some(false)` - 分片未完成
    /// - `None` - 任务不存在
    pub fn is_chunk_completed(&self, task_id: &str, chunk_index: usize) -> Option<bool> {
        self.tasks
            .get(task_id)
            .map(|info| info.is_chunk_completed(chunk_index))
    }

    /// 获取未完成的分片索引列表
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `total_chunks` - 总分片数
    ///
    /// # Returns
    /// - `Some(Vec<usize>)` - 未完成分片索引列表
    /// - `None` - 任务不存在
    pub fn get_pending_chunks(&self, task_id: &str, total_chunks: usize) -> Option<Vec<usize>> {
        self.tasks
            .get(task_id)
            .map(|info| info.get_pending_chunks(total_chunks))
    }

    /// 获取上传任务的分片 MD5 列表
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    ///
    /// # Returns
    /// - `Some(Vec<Option<String>>)` - 分片 MD5 列表
    /// - `None` - 任务不存在或不是上传任务
    pub fn get_chunk_md5s(&self, task_id: &str) -> Option<Vec<Option<String>>> {
        self.tasks
            .get(task_id)
            .and_then(|info| info.chunk_md5s.clone())
    }

    /// 检查任务是否存在
    pub fn task_exists(&self, task_id: &str) -> bool {
        self.tasks.contains_key(task_id)
    }

    /// 获取当前管理的任务数量
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    // ========================================================================
    // 恢复相关方法
    // ========================================================================

    /// 从持久化文件恢复任务状态到内存
    ///
    /// 用于程序启动时恢复未完成的任务
    ///
    /// # Arguments
    /// * `task_id` - 任务 ID
    /// * `task_type` - 任务类型
    /// * `total_chunks` - 总分片数
    ///
    /// # Returns
    /// - `Ok(TaskPersistenceInfo)` - 恢复成功
    /// - `Err` - 恢复失败
    pub fn restore_task_state(
        &self,
        task_id: &str,
        task_type: TaskType,
        total_chunks: usize,
    ) -> std::io::Result<()> {
        // 读取 WAL 记录
        let records = match read_records(&self.wal_dir, task_id) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // WAL 文件不存在，创建空状态
                Vec::new()
            }
            Err(e) => return Err(e),
        };

        // 创建持久化信息
        let mut info = match task_type {
            TaskType::Download => {
                TaskPersistenceInfo::new_download(task_id.to_string(), total_chunks)
            }
            TaskType::Upload => TaskPersistenceInfo::new_upload(task_id.to_string(), total_chunks),
            TaskType::Transfer => TaskPersistenceInfo::new_transfer(task_id.to_string()),
        };

        // 应用 WAL 记录
        for record in records {
            info.completed_chunks.insert(record.chunk_index);
            if let Some(md5) = record.md5 {
                if let Some(ref mut md5s) = info.chunk_md5s {
                    if record.chunk_index < md5s.len() {
                        md5s[record.chunk_index] = Some(md5);
                    }
                }
            }
        }

        // 插入到内存映射
        self.tasks.insert(task_id.to_string(), info);

        debug!(
            "已恢复任务状态: task_id={}, completed_chunks={}",
            task_id,
            self.get_completed_count(task_id).unwrap_or(0)
        );

        Ok(())
    }

    /// 立即刷写所有 WAL 缓存
    ///
    /// 用于测试或强制刷写场景
    pub async fn flush_all(&self) {
        flush_all_tasks(&self.tasks, &self.wal_dir).await;
    }
}

// ============================================================================
// 后台刷写循环
// ============================================================================

/// WAL 刷写循环
///
/// 定期将所有任务的 WAL 缓存刷写到磁盘
async fn wal_flush_loop(
    tasks: Arc<DashMap<String, TaskPersistenceInfo>>,
    wal_dir: PathBuf,
    flush_interval_ms: u64,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(flush_interval_ms));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // 正常刷写
                flush_all_tasks(&tasks, &wal_dir).await;
            }
            _ = shutdown_rx.recv() => {
                // 收到关闭信号，执行最终刷写
                info!("收到关闭信号，执行最终刷写");
                flush_all_tasks(&tasks, &wal_dir).await;
                break;
            }
        }
    }

    info!("WAL 刷写循环已退出");
}

/// 刷写所有任务的 WAL 缓存
async fn flush_all_tasks(tasks: &DashMap<String, TaskPersistenceInfo>, wal_dir: &PathBuf) {
    let mut flushed_count = 0;
    let mut record_count = 0;

    // 遍历所有任务
    for entry in tasks.iter() {
        let task_id = entry.key();
        let info = entry.value();

        // 获取待刷写的记录
        let records = info.take_wal_cache();

        if !records.is_empty() {
            record_count += records.len();

            // 刷写到磁盘
            if let Err(e) = append_records(wal_dir, task_id, &records) {
                error!("WAL 刷写失败: task_id={}, 错误: {}", task_id, e);
                // 失败时将记录放回缓存
                let mut cache = info.wal_cache.lock();
                for record in records {
                    cache.push(record);
                }
            } else {
                flushed_count += 1;
            }
        }
    }

    if record_count > 0 {
        debug!(
            "WAL 刷写完成: {} 个任务, {} 条记录",
            flushed_count, record_count
        );
    }
}

/// 🔥 WAL 清理循环
///
/// 每小时检查一次，清理过期的未完成任务
async fn wal_cleanup_loop(
    wal_dir: PathBuf,
    retention_days: u64,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    use super::recovery::cleanup_expired_tasks;

    // 每小时检查一次
    let cleanup_interval = Duration::from_secs(60 * 60);
    let mut interval = tokio::time::interval(cleanup_interval);

    // 第一次 tick 立即返回，跳过它以避免启动时立即清理
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                info!("执行定期 WAL 清理检查...");
                match cleanup_expired_tasks(&wal_dir, retention_days) {
                    Ok(cleaned) => {
                        if cleaned > 0 {
                            info!("WAL 清理完成: 清理了 {} 个过期任务", cleaned);
                        } else {
                            debug!("WAL 清理完成: 无过期任务");
                        }
                    }
                    Err(e) => {
                        error!("WAL 清理失败: {}", e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("收到关闭信号，WAL 清理循环退出");
                break;
            }
        }
    }

    info!("WAL 清理循环已退出");
}

/// 历史归档循环
///
/// 每天指定时间执行历史归档和过期历史清理
async fn history_archive_loop(
    wal_dir: PathBuf,
    history_cache: Arc<DashMap<String, TaskMetadata>>,
    archive_hour: u8,
    archive_minute: u8,
    retention_days: u64,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    use chrono::Local;

    // 每分钟检查一次是否到达归档时间
    let check_interval = Duration::from_secs(60);
    let mut interval = tokio::time::interval(check_interval);

    // 记录上次执行归档的日期，避免同一天重复执行
    let mut last_archive_date: Option<chrono::NaiveDate> = None;

    // 第一次 tick 立即返回，跳过它
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let now = Local::now();
                let current_date = now.date_naive();
                let current_hour = now.hour() as u8;
                let current_minute = now.minute() as u8;

                // 检查是否到达归档时间
                let should_archive = current_hour == archive_hour
                    && current_minute == archive_minute
                    && last_archive_date != Some(current_date);

                if should_archive {
                    info!("开始执行定时历史归档...");
                    last_archive_date = Some(current_date);

                    // 1. 执行历史归档
                    match history::archive_completed_tasks(&wal_dir) {
                        Ok(count) => {
                            if count > 0 {
                                info!("定时归档完成: 归档了 {} 个已完成任务", count);
                                // 重新加载历史缓存
                                if let Ok(cache) = history::load_history_cache(&wal_dir) {
                                    for entry in cache.into_iter() {
                                        history_cache.insert(entry.0, entry.1);
                                    }
                                }
                            } else {
                                debug!("定时归档完成: 无需归档的任务");
                            }
                        }
                        Err(e) => {
                            error!("定时归档失败: {}", e);
                        }
                    }

                    // 2. 执行文件夹历史归档
                    match super::folder::archive_completed_folders(&wal_dir) {
                        Ok(count) => {
                            if count > 0 {
                                info!("定时归档完成: 归档了 {} 个已完成文件夹", count);
                            }
                        }
                        Err(e) => {
                            error!("文件夹归档失败: {}", e);
                        }
                    }

                    // 3. 清理过期历史
                    match history::cleanup_expired_history(&wal_dir, retention_days) {
                        Ok(count) => {
                            if count > 0 {
                                info!("清理过期历史完成: 清理了 {} 条记录", count);
                                // 从缓存中移除过期任务
                                let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
                                history_cache.retain(|_, v| {
                                    v.completed_at.map(|t| t >= cutoff).unwrap_or(true)
                                });
                            }
                        }
                        Err(e) => {
                            error!("清理过期历史失败: {}", e);
                        }
                    }

                    // 4. 清理过期文件夹历史
                    match super::folder::cleanup_expired_folder_history(&wal_dir, retention_days) {
                        Ok(count) => {
                            if count > 0 {
                                info!("清理过期文件夹历史完成: 清理了 {} 条记录", count);
                            }
                        }
                        Err(e) => {
                            error!("清理过期文件夹历史失败: {}", e);
                        }
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("收到关闭信号，历史归档循环退出");
                break;
            }
        }
    }

    info!("历史归档循环已退出");
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::metadata;
    use crate::persistence::wal;
    use tempfile::TempDir;

    fn setup_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temp dir")
    }

    fn create_test_config() -> PersistenceConfig {
        PersistenceConfig {
            wal_dir: "wal".to_string(),
            wal_flush_interval_ms: 100,
            auto_recover_tasks: true,
            wal_retention_days: 7,
            history_archive_hour: 2,
            history_archive_minute: 0,
            history_retention_days: 30,
        }
    }

    #[test]
    fn test_persistence_manager_new() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();

        let manager = PersistenceManager::new(config, temp_dir.path());

        assert!(manager.wal_dir.exists());
        assert_eq!(manager.task_count(), 0);
    }

    #[test]
    fn test_register_download_task() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_download_task(
                "dl_001".to_string(),
                12345,
                "/remote/file.txt".to_string(),
                PathBuf::from("/local/file.txt"),
                1024 * 1024,
                256 * 1024,
                4,
                None,
                None,
                None,
            )
            .unwrap();

        assert!(manager.task_exists("dl_001"));
        assert_eq!(manager.task_count(), 1);
        assert_eq!(manager.get_completed_count("dl_001"), Some(0));

        // 验证元数据文件存在
        assert!(metadata::metadata_exists(&manager.wal_dir, "dl_001"));
    }

    #[test]
    fn test_register_upload_task() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_upload_task(
                "up_001".to_string(),
                PathBuf::from("/local/upload.txt"),
                "/remote/upload.txt".to_string(),
                2 * 1024 * 1024,
                512 * 1024,
                4,
            )
            .unwrap();

        assert!(manager.task_exists("up_001"));
        assert!(metadata::metadata_exists(&manager.wal_dir, "up_001"));
    }

    #[test]
    fn test_register_transfer_task() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_transfer_task(
                "tr_001".to_string(),
                "https://pan.baidu.com/s/xxx".to_string(),
                Some("1234".to_string()),
                "/save/path".to_string(),
                true,
                Some("test.zip".to_string()),
            )
            .unwrap();

        assert!(manager.task_exists("tr_001"));
        assert!(metadata::metadata_exists(&manager.wal_dir, "tr_001"));
    }

    #[test]
    fn test_on_chunk_completed() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_download_task(
                "dl_002".to_string(),
                111,
                "/path".to_string(),
                PathBuf::from("/local"),
                1024,
                256,
                4,
                None,
                None,
                None,
            )
            .unwrap();

        // 标记分片完成
        manager.on_chunk_completed("dl_002", 0);
        manager.on_chunk_completed("dl_002", 2);

        assert_eq!(manager.get_completed_count("dl_002"), Some(2));
        assert_eq!(manager.is_chunk_completed("dl_002", 0), Some(true));
        assert_eq!(manager.is_chunk_completed("dl_002", 1), Some(false));
        assert_eq!(manager.is_chunk_completed("dl_002", 2), Some(true));

        // 获取未完成分片
        let pending = manager.get_pending_chunks("dl_002", 4).unwrap();
        assert_eq!(pending, vec![1, 3]);
    }

    #[test]
    fn test_on_chunk_completed_with_md5() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_upload_task(
                "up_002".to_string(),
                PathBuf::from("/local"),
                "/remote".to_string(),
                1024,
                256,
                4,
            )
            .unwrap();

        // 标记分片完成（带 MD5）
        manager.on_chunk_completed_with_md5("up_002", 0, "md5_0".to_string());
        manager.on_chunk_completed_with_md5("up_002", 2, "md5_2".to_string());

        // 验证 MD5
        let md5s = manager.get_chunk_md5s("up_002").unwrap();
        assert_eq!(md5s[0], Some("md5_0".to_string()));
        assert_eq!(md5s[1], None);
        assert_eq!(md5s[2], Some("md5_2".to_string()));
    }

    #[test]
    fn test_on_task_completed() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_download_task(
                "dl_003".to_string(),
                111,
                "/path".to_string(),
                PathBuf::from("/local"),
                1024,
                256,
                4,
                None,
                None,
                None,
            )
            .unwrap();

        assert!(manager.task_exists("dl_003"));
        assert!(metadata::metadata_exists(&manager.wal_dir, "dl_003"));

        // 任务完成
        manager.on_task_completed("dl_003").unwrap();

        // 任务从内存中移除
        assert!(!manager.task_exists("dl_003"));

        // 元数据文件仍然存在（用于历史归档）
        assert!(metadata::metadata_exists(&manager.wal_dir, "dl_003"));

        // 元数据状态应该是 completed
        let meta = metadata::load_metadata(&manager.wal_dir, "dl_003").unwrap();
        assert!(meta.is_completed());
        assert!(meta.completed_at.is_some());

        // 任务应该在历史缓存中
        assert!(manager.history_cache().contains_key("dl_003"));
    }

    #[test]
    fn test_update_transfer_status() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_transfer_task(
                "tr_002".to_string(),
                "https://pan.baidu.com/s/yyy".to_string(),
                None,
                "/target".to_string(),
                false,
                None,
            )
            .unwrap();

        // 更新状态
        manager
            .update_transfer_status("tr_002", "downloading")
            .unwrap();

        // 验证
        let metadata = metadata::load_metadata(&manager.wal_dir, "tr_002").unwrap();
        assert_eq!(metadata.transfer_status, Some("downloading".to_string()));
    }

    #[test]
    fn test_update_transfer_download_ids() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_transfer_task(
                "tr_003".to_string(),
                "https://pan.baidu.com/s/zzz".to_string(),
                Some("5678".to_string()),
                "/target".to_string(),
                true,
                Some("file.zip".to_string()),
            )
            .unwrap();

        // 更新关联下载任务
        manager
            .update_transfer_download_ids("tr_003", vec!["dl_a".to_string(), "dl_b".to_string()])
            .unwrap();

        // 验证
        let metadata = metadata::load_metadata(&manager.wal_dir, "tr_003").unwrap();
        assert_eq!(metadata.download_task_ids.len(), 2);
    }

    #[test]
    fn test_update_upload_id() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_upload_task(
                "up_003".to_string(),
                PathBuf::from("/local"),
                "/remote".to_string(),
                1024,
                256,
                4,
            )
            .unwrap();

        // 更新 upload_id
        manager
            .update_upload_id("up_003", "upload_id_xyz".to_string())
            .unwrap();

        // 验证
        let metadata = metadata::load_metadata(&manager.wal_dir, "up_003").unwrap();
        assert_eq!(metadata.upload_id, Some("upload_id_xyz".to_string()));
        assert!(metadata.upload_id_created_at.is_some());
    }

    #[tokio::test]
    async fn test_flush_all() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        manager
            .register_download_task(
                "dl_004".to_string(),
                111,
                "/path".to_string(),
                PathBuf::from("/local"),
                1024,
                256,
                4,
                None,
                None,
                None,
            )
            .unwrap();

        // 标记分片完成
        manager.on_chunk_completed("dl_004", 0);
        manager.on_chunk_completed("dl_004", 1);

        // 刷写
        manager.flush_all().await;

        // 验证 WAL 文件
        assert!(wal::wal_exists(&manager.wal_dir, "dl_004"));

        let records = wal::read_records(&manager.wal_dir, "dl_004").unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].chunk_index, 0);
        assert_eq!(records[1].chunk_index, 1);
    }

    #[tokio::test]
    async fn test_restore_task_state() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let manager = PersistenceManager::new(config, temp_dir.path());

        // 注册任务并标记分片完成
        manager
            .register_download_task(
                "dl_005".to_string(),
                111,
                "/path".to_string(),
                PathBuf::from("/local"),
                1024,
                256,
                4,
                None,
                None,
                None,
            )
            .unwrap();

        manager.on_chunk_completed("dl_005", 0);
        manager.on_chunk_completed("dl_005", 2);

        // 刷写
        manager.flush_all().await;

        // 模拟重启：从内存中移除
        manager.tasks.remove("dl_005");
        assert!(!manager.task_exists("dl_005"));

        // 恢复
        manager
            .restore_task_state("dl_005", TaskType::Download, 4)
            .unwrap();

        // 验证恢复结果
        assert!(manager.task_exists("dl_005"));
        assert_eq!(manager.get_completed_count("dl_005"), Some(2));
        assert_eq!(manager.is_chunk_completed("dl_005", 0), Some(true));
        assert_eq!(manager.is_chunk_completed("dl_005", 1), Some(false));
        assert_eq!(manager.is_chunk_completed("dl_005", 2), Some(true));
    }

    #[tokio::test]
    async fn test_start_and_shutdown() {
        let temp_dir = setup_temp_dir();
        let config = create_test_config();
        let mut manager = PersistenceManager::new(config, temp_dir.path());

        // 启动
        manager.start();
        assert!(manager.flush_task.is_some());

        // 注册任务并标记分片完成
        manager
            .register_download_task(
                "dl_006".to_string(),
                111,
                "/path".to_string(),
                PathBuf::from("/local"),
                1024,
                256,
                4,
                None,
                None,
                None,
            )
            .unwrap();

        manager.on_chunk_completed("dl_006", 0);

        // 等待一个刷写周期
        tokio::time::sleep(Duration::from_millis(150)).await;

        // 关闭
        manager.shutdown().await;
        assert!(manager.flush_task.is_none());

        // 验证 WAL 已刷写
        assert!(wal::wal_exists(&manager.wal_dir, "dl_006"));
    }
}
