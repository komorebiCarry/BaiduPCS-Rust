//! 文件夹下载管理器

use crate::downloader::{DownloadManager, DownloadTask, TaskStatus};
use crate::netdisk::NetdiskClient;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::folder::{FolderDownload, FolderStatus, PendingFile};

/// 文件夹下载管理器
pub struct FolderDownloadManager {
    /// 所有文件夹下载
    folders: Arc<RwLock<HashMap<String, FolderDownload>>>,
    /// 文件夹取消令牌（用于控制扫描任务）
    cancellation_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
    /// 下载管理器（延迟初始化）
    download_manager: Arc<RwLock<Option<Arc<DownloadManager>>>>,
    /// 网盘客户端（延迟初始化）
    netdisk_client: Arc<RwLock<Option<Arc<NetdiskClient>>>>,
    /// 下载目录
    download_dir: PathBuf,
}

impl FolderDownloadManager {
    /// 创建新的文件夹下载管理器
    pub fn new(download_dir: PathBuf) -> Self {
        Self {
            folders: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            download_manager: Arc::new(RwLock::new(None)),
            netdisk_client: Arc::new(RwLock::new(None)),
            download_dir,
        }
    }

    /// 设置下载管理器
    pub async fn set_download_manager(&self, manager: Arc<DownloadManager>) {
        // 创建任务完成通知 channel
        let (tx, rx) = mpsc::unbounded_channel::<String>();

        // 设置 sender 到 download_manager
        manager.set_task_completed_sender(tx).await;

        // 保存 download_manager
        {
            let mut dm = self.download_manager.write().await;
            *dm = Some(manager);
        }

        // 启动监听任务
        self.start_task_completed_listener(rx);

        info!("文件夹下载管理器已设置下载管理器，任务完成监听已启动");
    }

    /// 启动任务完成监听器
    ///
    /// 当收到子任务完成通知时，立即从 pending_files 补充新任务
    /// 根据预注册余量动态补充，充分利用预注册名额
    fn start_task_completed_listener(&self, mut rx: mpsc::UnboundedReceiver<String>) {
        let folders = self.folders.clone();
        let download_manager = self.download_manager.clone();

        tokio::spawn(async move {
            while let Some(group_id) = rx.recv().await {
                // 获取下载管理器
                let dm = {
                    let guard = download_manager.read().await;
                    guard.clone()
                };

                let dm = match dm {
                    Some(dm) => dm,
                    None => continue,
                };

                // 获取预注册余量
                let available = dm.pre_register_available().await;
                if available == 0 {
                    continue;
                }

                // 根据余量补充任务
                let files_to_create = {
                    let mut folders_guard = folders.write().await;
                    let folder = match folders_guard.get_mut(&group_id) {
                        Some(f) => f,
                        None => continue,
                    };

                    // 检查状态
                    if folder.status == FolderStatus::Paused
                        || folder.status == FolderStatus::Cancelled
                        || folder.status == FolderStatus::Failed
                        || folder.status == FolderStatus::Completed
                    {
                        continue;
                    }

                    // 检查是否还有待处理文件
                    if folder.pending_files.is_empty() {
                        // 检查是否全部完成
                        let tasks = dm.get_tasks_by_group(&group_id).await;
                        let completed = tasks
                            .iter()
                            .filter(|t| t.status == TaskStatus::Completed)
                            .count() as u64;
                        let active = tasks
                            .iter()
                            .filter(|t| {
                                t.status == TaskStatus::Downloading
                                    || t.status == TaskStatus::Pending
                            })
                            .count();

                        folder.completed_count = completed;
                        folder.downloaded_size = tasks.iter().map(|t| t.downloaded_size).sum();

                        if folder.scan_completed && active == 0 && completed == folder.total_files {
                            folder.mark_completed();
                            info!("文件夹 {} 全部下载完成！", folder.name);
                        }
                        continue;
                    }

                    // 根据预注册余量取出相应数量的文件
                    let count = folder.pending_files.len().min(available);
                    let files: Vec<_> = folder.pending_files.drain(..count).collect();
                    (files, folder.local_root.clone(), folder.remote_root.clone())
                };

                let (files, local_root, group_root) = files_to_create;
                let mut created_count = 0u64;

                // 创建任务
                for file_to_create in files {
                    let local_path = local_root.join(&file_to_create.relative_path);

                    // 确保目录存在
                    if let Some(parent) = local_path.parent() {
                        if let Err(e) = tokio::fs::create_dir_all(parent).await {
                            error!("创建目录失败: {:?}, 错误: {}", parent, e);
                            continue;
                        }
                    }

                    let task = DownloadTask::new_with_group(
                        file_to_create.fs_id,
                        file_to_create.remote_path.clone(),
                        local_path,
                        file_to_create.size,
                        group_id.clone(),
                        group_root.clone(),
                        file_to_create.relative_path,
                    );

                    // 启动任务
                    if let Err(e) = dm.add_task(task).await {
                        warn!("补充任务失败: {}", e);
                    } else {
                        created_count += 1;
                    }
                }

                // 更新已创建计数
                if created_count > 0 {
                    let mut folders_guard = folders.write().await;
                    if let Some(folder) = folders_guard.get_mut(&group_id) {
                        folder.created_count += created_count;
                    }
                    info!("已补充{}个任务到文件夹 {} (预注册余量: {})", created_count, group_id, available);
                }
            }
        });
    }

    /// 设置网盘客户端
    pub async fn set_netdisk_client(&self, client: Arc<NetdiskClient>) {
        let mut nc = self.netdisk_client.write().await;
        *nc = Some(client);
    }

    /// 创建文件夹下载任务
    pub async fn create_folder_download(&self, remote_path: String) -> Result<String> {
        // 计算本地路径（使用文件夹名称）
        let folder_name = remote_path
            .trim_end_matches('/')
            .split('/')
            .last()
            .unwrap_or("download");
        let local_root = self.download_dir.join(folder_name);

        let folder = FolderDownload::new(remote_path.clone(), local_root);
        let folder_id = folder.id.clone();

        // 保存到列表
        {
            let mut folders = self.folders.write().await;
            folders.insert(folder_id.clone(), folder);
        }

        info!("创建文件夹下载任务: {}, ID: {}", remote_path, folder_id);

        // 异步开始扫描并创建任务
        let self_clone = Self {
            folders: self.folders.clone(),
            cancellation_tokens: self.cancellation_tokens.clone(),
            download_manager: self.download_manager.clone(),
            netdisk_client: self.netdisk_client.clone(),
            download_dir: self.download_dir.clone(),
        };
        let folder_id_clone = folder_id.clone();

        tokio::spawn(async move {
            if let Err(e) = self_clone
                .scan_folder_and_create_tasks(&folder_id_clone)
                .await
            {
                error!("扫描文件夹失败: {:?}", e);
                let mut folders = self_clone.folders.write().await;
                if let Some(folder) = folders.get_mut(&folder_id_clone) {
                    folder.mark_failed(e.to_string());
                }
                // 清理取消令牌
                self_clone
                    .cancellation_tokens
                    .write()
                    .await
                    .remove(&folder_id_clone);
            }
        });

        Ok(folder_id)
    }

    /// 递归扫描文件夹并创建任务（边扫描边创建）
    async fn scan_folder_and_create_tasks(&self, folder_id: &str) -> Result<()> {
        let (remote_root, local_root) = {
            let folders = self.folders.read().await;
            let folder = folders
                .get(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;
            (folder.remote_root.clone(), folder.local_root.clone())
        };

        // 获取网盘客户端
        let client = {
            let nc = self.netdisk_client.read().await;
            nc.clone()
                .ok_or_else(|| anyhow!("网盘客户端未初始化"))?
        };

        // 创建取消令牌
        let cancel_token = CancellationToken::new();
        {
            let mut tokens = self.cancellation_tokens.write().await;
            tokens.insert(folder_id.to_string(), cancel_token.clone());
        }

        // 递归扫描并收集文件信息到 pending_files
        self.scan_recursive(
            folder_id,
            &client,
            &cancel_token,
            &remote_root,
            &remote_root,
            &local_root,
        )
        .await?;

        // 扫描完成，更新状态
        {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.scan_completed = true;
                if folder.status == FolderStatus::Scanning {
                    folder.mark_downloading();
                }
                info!(
                    "文件夹扫描完成: {} 个文件, 总大小: {} bytes, pending队列: {}",
                    folder.total_files, folder.total_size, folder.pending_files.len()
                );
            }
        }

        // 清理取消令牌
        {
            let mut tokens = self.cancellation_tokens.write().await;
            tokens.remove(folder_id);
        }

        // 扫描完成后，立即创建前10个任务
        if let Err(e) = self.refill_tasks(folder_id, 10).await {
            error!("创建初始任务失败: {}", e);
        }

        Ok(())
    }

    /// 递归扫描目录（只收集文件信息到 pending_files，不创建任务）
    #[async_recursion::async_recursion]
    async fn scan_recursive(
        &self,
        folder_id: &str,
        client: &NetdiskClient,
        cancel_token: &CancellationToken,
        root_path: &str,
        current_path: &str,
        local_root: &PathBuf,
    ) -> Result<()> {
        // 检查是否已取消
        if cancel_token.is_cancelled() {
            info!("扫描任务被取消");
            return Ok(());
        }

        let mut page = 1;
        let page_size = 100;

        loop {
            // 每页之前检查取消
            if cancel_token.is_cancelled() {
                info!("扫描任务被取消");
                return Ok(());
            }

            // 更新扫描进度
            {
                let mut folders = self.folders.write().await;
                if let Some(folder) = folders.get_mut(folder_id) {
                    folder.scan_progress = Some(current_path.to_string());
                }
            }

            // 获取文件列表
            let file_list = client.get_file_list(current_path, page, page_size).await?;

            let mut batch_files = Vec::new();
            let mut batch_size = 0u64;

            for item in &file_list.list {
                // 检查取消
                if cancel_token.is_cancelled() {
                    return Ok(());
                }

                if item.isdir == 1 {
                    // 递归处理子目录
                    self.scan_recursive(
                        folder_id,
                        client,
                        cancel_token,
                        root_path,
                        &item.path,
                        local_root,
                    )
                    .await?;
                } else {
                    // 计算相对路径
                    let relative_path = item
                        .path
                        .strip_prefix(root_path)
                        .unwrap_or(&item.path)
                        .trim_start_matches('/')
                        .to_string();

                    // 收集文件信息
                    let pending_file = PendingFile {
                        fs_id: item.fs_id,
                        filename: item.server_filename.clone(),
                        remote_path: item.path.clone(),
                        relative_path,
                        size: item.size,
                    };

                    batch_files.push(pending_file);
                    batch_size += item.size;
                }
            }

            // 批量添加到 pending_files
            if !batch_files.is_empty() {
                let batch_count = batch_files.len();

                {
                    let mut folders = self.folders.write().await;
                    if let Some(folder) = folders.get_mut(folder_id) {
                        folder.pending_files.extend(batch_files);
                        folder.total_files += batch_count as u64;
                        folder.total_size += batch_size;
                    }
                }

                info!(
                    "扫描进度: 发现 {} 个文件，总大小 {} bytes (路径: {})",
                    batch_count, batch_size, current_path
                );
            }

            // 检查是否还有下一页
            if file_list.list.len() < page_size as usize {
                break;
            }
            page += 1;
        }

        Ok(())
    }

    /// 获取所有文件夹下载
    pub async fn get_all_folders(&self) -> Vec<FolderDownload> {
        let folders = self.folders.read().await;
        folders.values().cloned().collect()
    }

    /// 获取指定文件夹下载
    pub async fn get_folder(&self, folder_id: &str) -> Option<FolderDownload> {
        let folders = self.folders.read().await;
        folders.get(folder_id).cloned()
    }

    /// 暂停文件夹下载
    pub async fn pause_folder(&self, folder_id: &str) -> Result<()> {
        info!("暂停文件夹下载: {}", folder_id);

        // 触发取消令牌，停止扫描
        {
            let tokens = self.cancellation_tokens.read().await;
            if let Some(token) = tokens.get(folder_id) {
                token.cancel();
            }
        }

        // 获取下载管理器
        let download_manager = {
            let dm = self.download_manager.read().await;
            dm.clone()
                .ok_or_else(|| anyhow!("下载管理器未初始化"))?
        };

        // 暂停所有相关任务
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        for task in tasks {
            if task.status == TaskStatus::Downloading || task.status == TaskStatus::Pending {
                let _ = download_manager.pause_task(&task.id).await;
            }
        }

        // 更新文件夹状态
        {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.mark_paused();
                info!("文件夹 {} 已暂停", folder.name);
            }
        }

        Ok(())
    }

    /// 恢复文件夹下载
    pub async fn resume_folder(&self, folder_id: &str) -> Result<()> {
        info!("恢复文件夹下载: {}", folder_id);

        let folder_info = {
            let mut folders = self.folders.write().await;
            let folder = folders
                .get_mut(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;

            if folder.status != FolderStatus::Paused {
                return Err(anyhow!(
                    "文件夹状态不正确，当前状态: {:?}",
                    folder.status
                ));
            }

            // 更新状态
            if folder.scan_completed {
                folder.mark_downloading();
            } else {
                folder.status = FolderStatus::Scanning;
            }

            (
                folder.scan_completed,
                folder.remote_root.clone(),
                folder.local_root.clone(),
            )
        };

        // 获取下载管理器
        let download_manager = {
            let dm = self.download_manager.read().await;
            dm.clone()
                .ok_or_else(|| anyhow!("下载管理器未初始化"))?
        };

        // 恢复所有暂停的任务
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        for task in tasks {
            if task.status == TaskStatus::Paused {
                let _ = download_manager.resume_task(&task.id).await;
            }
        }

        // 如果扫描未完成，重新启动扫描
        if !folder_info.0 {
            let self_clone = Self {
                folders: self.folders.clone(),
                cancellation_tokens: self.cancellation_tokens.clone(),
                download_manager: self.download_manager.clone(),
                netdisk_client: self.netdisk_client.clone(),
                download_dir: self.download_dir.clone(),
            };
            let folder_id = folder_id.to_string();

            tokio::spawn(async move {
                if let Err(e) = self_clone.scan_folder_and_create_tasks(&folder_id).await {
                    error!("恢复扫描失败: {:?}", e);
                }
            });
        } else {
            // 如果扫描已完成，补充任务到10个
            if let Err(e) = self.refill_tasks(folder_id, 10).await {
                warn!("恢复时补充任务失败: {}", e);
            }
        }

        Ok(())
    }

    /// 取消文件夹下载
    pub async fn cancel_folder(&self, folder_id: &str, delete_files: bool) -> Result<()> {
        info!("取消文件夹下载: {}, 删除文件: {}", folder_id, delete_files);

        // 触发取消令牌，停止扫描
        {
            let mut tokens = self.cancellation_tokens.write().await;
            if let Some(token) = tokens.remove(folder_id) {
                token.cancel();
            }
        }

        // 🔥 关键：先更新文件夹状态并清空 pending_files，阻止 task_completed_listener 补充新任务
        // 这必须在删除任务之前执行，避免竞态条件
        let local_root = {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.mark_cancelled();
                folder.pending_files.clear(); // 清空待处理队列
                info!(
                    "文件夹 {} 已标记为取消，已清空 pending_files ({} 个待处理文件)",
                    folder.name, folder.pending_files.len()
                );
                Some(folder.local_root.clone())
            } else {
                None
            }
        };

        // 获取下载管理器
        let download_manager = {
            let dm = self.download_manager.read().await;
            dm.clone()
                .ok_or_else(|| anyhow!("下载管理器未初始化"))?
        };

        // 🔥 新策略：直接删除所有任务记录，让分片自然结束
        // 1. 获取所有子任务
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        let task_count = tasks.len();
        info!("正在删除文件夹 {} 的 {} 个子任务...", folder_id, task_count);

        // 2. 立即删除所有任务（触发取消令牌 + 从 HashMap 移除）
        // delete_task 会：
        //   - 触发 cancellation_token（通知分片停止）
        //   - 从调度器移除
        //   - 从 tasks HashMap 移除
        //   - 删除临时文件（如果 delete_files=true）
        for task in tasks {
            let _ = download_manager.delete_task(&task.id, delete_files).await;
        }
        info!("所有子任务已删除，等待分片物理释放...");

        // 3. 等待分片物理释放（文件句柄关闭、flush 完成）
        // 因为分片下载是异步的 tokio::spawn，删除任务后它们仍在运行
        // 需要等待它们检测到 cancellation_token 并退出
        //
        // 关键等待时间：
        // - 分片检测取消：即时（每次写入都检查）
        // - 文件 flush：最多几秒（取决于磁盘速度和缓冲区大小）
        // - 文件句柄释放：flush 完成后立即释放
        //
        // 保守估计：等待 3 秒足够（HDD 最慢情况）
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        info!("分片物理释放完成");

        // 4. 如果需要删除文件，删除整个文件夹目录
        if delete_files {
            if let Some(root_path) = local_root {
                info!("准备删除文件夹目录: {:?}", root_path);
                if root_path.exists() {
                    match tokio::fs::remove_dir_all(&root_path).await {
                        Ok(_) => info!("已删除文件夹目录: {:?}", root_path),
                        Err(e) => error!("删除文件夹目录失败: {:?}, 错误: {}", root_path, e),
                    }
                } else {
                    warn!("文件夹目录不存在: {:?}", root_path);
                }
            } else {
                warn!("local_root 为空，无法删除文件夹目录");
            }
        }

        Ok(())
    }

    /// 删除文件夹下载记录
    pub async fn delete_folder(&self, folder_id: &str) -> Result<()> {
        let mut folders = self.folders.write().await;
        folders.remove(folder_id);
        Ok(())
    }

    /// 补充任务：保持文件夹有指定数量的活跃任务
    ///
    /// 这是核心方法：检查活跃任务数，如果不足就从 pending_files 补充
    async fn refill_tasks(&self, folder_id: &str, target_count: usize) -> Result<()> {
        // 获取下载管理器
        let download_manager = {
            let dm = self.download_manager.read().await;
            dm.clone()
                .ok_or_else(|| anyhow!("下载管理器未初始化"))?
        };

        // 检查当前活跃任务数
        let tasks = download_manager.get_tasks_by_group(folder_id).await;
        let active_count = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Downloading || t.status == TaskStatus::Pending)
            .count();

        // 如果已经足够，不需要补充
        if active_count >= target_count {
            return Ok(());
        }

        // 计算需要补充的数量
        let needed = target_count - active_count;

        // 从 pending_files 取出需要的文件
        let (files_to_create, local_root, group_root) = {
            let mut folders = self.folders.write().await;
            let folder = folders
                .get_mut(folder_id)
                .ok_or_else(|| anyhow!("文件夹不存在"))?;

            // 检查状态，如果暂停或取消，不补充任务
            if folder.status == FolderStatus::Paused
                || folder.status == FolderStatus::Cancelled
                || folder.status == FolderStatus::Failed
            {
                return Ok(());
            }

            let to_create = needed.min(folder.pending_files.len());
            if to_create == 0 {
                return Ok(());
            }

            let files = folder.pending_files.drain(..to_create).collect::<Vec<_>>();
            (files, folder.local_root.clone(), folder.remote_root.clone())
        };

        if files_to_create.is_empty() {
            return Ok(());
        }

        info!(
            "补充任务: 文件夹 {} 需要 {} 个任务 (当前活跃: {}/{})",
            folder_id,
            files_to_create.len(),
            active_count,
            target_count
        );

        // 批量创建任务
        let mut created_count = 0;
        for pending_file in files_to_create {
            let local_path = local_root.join(&pending_file.relative_path);

            // 确保目录存在
            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context(format!("创建目录失败: {:?}", parent))?;
            }

            let task = DownloadTask::new_with_group(
                pending_file.fs_id,
                pending_file.remote_path.clone(),
                local_path,
                pending_file.size,
                folder_id.to_string(),
                group_root.clone(),
                pending_file.relative_path,
            );

            // 创建并启动任务
            if let Err(e) = download_manager.add_task(task).await {
                warn!("创建下载任务失败: {}", e);
            } else {
                created_count += 1;
            }
        }

        // 更新已创建计数
        {
            let mut folders = self.folders.write().await;
            if let Some(folder) = folders.get_mut(folder_id) {
                folder.created_count += created_count;
            }
        }

        info!(
            "补充任务完成: 文件夹 {} 成功创建 {} 个任务",
            folder_id, created_count
        );

        Ok(())
    }

    /// 更新文件夹的下载进度（定期调用）
    ///
    /// 这个方法会：
    /// 1. 更新已完成数和已下载大小
    /// 2. 检查是否全部完成
    /// 3. 补充任务，保持10个活跃任务
    pub async fn update_folder_progress(&self, folder_id: &str) -> Result<()> {
        let download_manager = {
            let dm = self.download_manager.read().await;
            dm.clone()
                .ok_or_else(|| anyhow!("下载管理器未初始化"))?
        };

        let tasks = download_manager.get_tasks_by_group(folder_id).await;

        let mut folders = self.folders.write().await;
        if let Some(folder) = folders.get_mut(folder_id) {
            folder.completed_count = tasks
                .iter()
                .filter(|t| t.status == TaskStatus::Completed)
                .count() as u64;

            folder.downloaded_size = tasks.iter().map(|t| t.downloaded_size).sum();

            // 检查是否全部完成
            if folder.scan_completed
                && folder.pending_files.is_empty()
                && folder.completed_count == folder.total_files
                && folder.status != FolderStatus::Failed
                && folder.status != FolderStatus::Cancelled
            {
                folder.mark_completed();
                info!("文件夹 {} 全部下载完成！", folder.name);
            }
        }
        drop(folders);

        // 补充任务：保持10个活跃任务（完成1个，进1个）
        if let Err(e) = self.refill_tasks(folder_id, 10).await {
            warn!("补充任务失败: {}", e);
        }

        Ok(())
    }
}
