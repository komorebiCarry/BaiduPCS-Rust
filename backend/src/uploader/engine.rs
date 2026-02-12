// 上传引擎
//
// 核心功能：
// 1. 协调文件分片上传（支持并发）
// 2. 管理上传服务器健康状态
// 3. 实现错误分类和指数退避重试
// 4. 支持秒传和断点续传
//
// 并发上传策略：
// - 使用 Semaphore 控制最大并发分片数
// - 使用 JoinSet 管理并发任务
// - 原子计数器追踪进度
// - 根据文件大小自适应调整并发数

use crate::config::VipType;
use crate::netdisk::{NetdiskClient, UploadErrorKind};
use crate::uploader::{
    calculate_upload_task_max_chunks, PcsServerHealthManager, RapidUploadChecker, UploadChunk,
    UploadChunkManager, UploadTask,
};
use anyhow::{Context, Result};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// =====================================================
// 重试配置
// =====================================================

/// 默认最大重试次数
const DEFAULT_MAX_RETRIES: u32 = 3;

/// 初始退避延迟（毫秒）
const INITIAL_BACKOFF_MS: u64 = 100;

/// 最大退避延迟（毫秒）
const MAX_BACKOFF_MS: u64 = 5000;

/// 限流时的额外等待时间（毫秒）
const RATE_LIMIT_BACKOFF_MS: u64 = 10000;

/// 计算指数退避延迟
///
/// # 延迟序列
/// - retry_count=0: 100ms
/// - retry_count=1: 200ms
/// - retry_count=2: 400ms
/// - retry_count=3: 800ms
/// - 最大: 5000ms
fn calculate_backoff_delay(retry_count: u32, error_kind: &UploadErrorKind) -> u64 {
    let base_delay = INITIAL_BACKOFF_MS * 2u64.pow(retry_count);
    let delay = base_delay.min(MAX_BACKOFF_MS);

    // 限流时使用更长的等待时间
    if matches!(error_kind, UploadErrorKind::RateLimited) {
        delay.max(RATE_LIMIT_BACKOFF_MS)
    } else {
        delay
    }
}

// =====================================================
// 上传引擎
// =====================================================

/// 上传引擎
///
/// 负责协调单个文件的上传过程，包括：
/// - 秒传检查
/// - 分片上传
/// - 错误重试
/// - 进度跟踪
pub struct UploadEngine {
    /// 网盘客户端
    client: NetdiskClient,
    /// 上传任务
    task: Arc<Mutex<UploadTask>>,
    /// 分片管理器
    chunk_manager: Arc<Mutex<UploadChunkManager>>,
    /// 服务器健康管理器
    server_health: Arc<PcsServerHealthManager>,
    /// 取消令牌
    cancel_token: CancellationToken,
    /// 已上传字节数（原子计数，用于进度更新）
    uploaded_bytes: Arc<AtomicU64>,
    /// 是否已暂停
    is_paused: Arc<AtomicBool>,
    /// 上次速度计算时间
    last_speed_time: Arc<Mutex<std::time::Instant>>,
    /// 上次速度计算时的已上传字节数
    last_speed_bytes: Arc<AtomicU64>,
    /// VIP 类型
    vip_type: VipType,
    /// 最大重试次数
    max_retries: u32,
}

impl UploadEngine {
    /// 创建新的上传引擎（使用默认重试次数）
    pub fn new(
        client: NetdiskClient,
        task: Arc<Mutex<UploadTask>>,
        chunk_manager: Arc<Mutex<UploadChunkManager>>,
        server_health: Arc<PcsServerHealthManager>,
        cancel_token: CancellationToken,
        vip_type: VipType,
    ) -> Self {
        Self::with_max_retries(
            client,
            task,
            chunk_manager,
            server_health,
            cancel_token,
            vip_type,
            DEFAULT_MAX_RETRIES,
        )
    }

    /// 创建新的上传引擎（指定重试次数）
    pub fn with_max_retries(
        client: NetdiskClient,
        task: Arc<Mutex<UploadTask>>,
        chunk_manager: Arc<Mutex<UploadChunkManager>>,
        server_health: Arc<PcsServerHealthManager>,
        cancel_token: CancellationToken,
        vip_type: VipType,
        max_retries: u32,
    ) -> Self {
        Self {
            client,
            task,
            chunk_manager,
            server_health,
            cancel_token,
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
            is_paused: Arc::new(AtomicBool::new(false)),
            last_speed_time: Arc::new(Mutex::new(std::time::Instant::now())),
            last_speed_bytes: Arc::new(AtomicU64::new(0)),
            vip_type,
            max_retries,
        }
    }

    /// 获取已上传字节数
    pub fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes.load(Ordering::SeqCst)
    }

    /// 暂停上传
    pub fn pause(&self) {
        self.is_paused.store(true, Ordering::SeqCst);
    }

    /// 恢复上传
    pub fn resume(&self) {
        self.is_paused.store(false, Ordering::SeqCst);
    }

    /// 执行上传
    ///
    /// # 上传流程
    /// 1. 计算文件哈希（用于秒传检查）
    /// 2. 尝试秒传
    /// 3. 如果秒传失败，执行普通分片上传
    /// 4. 合并分片，完成上传
    pub async fn upload(&self) -> Result<()> {
        let (local_path, remote_path, total_size) = {
            let task = self.task.lock().await;
            (
                task.local_path.clone(),
                task.remote_path.clone(),
                task.total_size,
            )
        };

        info!(
            "开始上传: local={:?}, remote={}, size={}",
            local_path, remote_path, total_size
        );

        // 检查取消
        if self.cancel_token.is_cancelled() {
            return Err(anyhow::anyhow!("上传已取消"));
        }

        // 1. 计算 block_list（分片 MD5 数组，分片大小根据 VIP 等级动态计算）
        info!(
            "计算 block_list: {:?}, vip_type={:?}",
            local_path, self.vip_type
        );
        let block_list =
            RapidUploadChecker::calculate_block_list(&local_path, self.vip_type).await?;
        info!("block_list 计算完成: {}", block_list);

        // 检查取消
        if self.cancel_token.is_cancelled() {
            return Err(anyhow::anyhow!("上传已取消"));
        }

        // 2. 预创建文件
        let precreate_response = self
            .client
            .precreate(&remote_path, total_size, &block_list)
            .await?;

        // 检查是否秒传成功（precreate 也可能触发秒传）
        if precreate_response.is_rapid_upload() {
            info!("预创建时秒传成功: {}", remote_path);
            return Ok(());
        }

        let upload_id = precreate_response.uploadid.clone();
        if upload_id.is_empty() {
            return Err(anyhow::anyhow!("预创建失败：未获取到 uploadid"));
        }

        // 3. 执行普通分片上传
        self.task.lock().await.mark_uploading();
        self.upload_with_chunks(
            &local_path,
            &remote_path,
            total_size,
            &block_list,
            &upload_id,
        )
            .await?;

        // 4. 标记完成
        self.task.lock().await.mark_completed();
        info!("上传完成: {}", remote_path);

        Ok(())
    }

    /// 分片上传（并发模式）
    ///
    /// 使用 Semaphore 控制并发分片数，JoinSet 管理并发任务
    /// 根据文件大小自适应调整并发数
    async fn upload_with_chunks(
        &self,
        local_path: &Path,
        remote_path: &str,
        total_size: u64,
        block_list: &str,
        upload_id: &str,
    ) -> Result<()> {
        // 计算并发数（根据文件大小自适应）
        let max_concurrent = calculate_upload_task_max_chunks(total_size);

        info!(
            "[并发上传] 文件大小: {} bytes, 最大并发分片数: {}",
            total_size, max_concurrent
        );

        // 执行并发分片上传
        self.upload_chunks_concurrent(
            local_path,
            remote_path,
            total_size,
            block_list,
            upload_id,
            max_concurrent,
        )
            .await?;

        // 创建文件（合并分片）
        // ⚠️ 重要: create_file 也需要 block_list (4MB分片MD5),和上传的分片MD5无关
        info!("合并上传分片,创建文件: {}", remote_path);
        self.client
            .create_file(remote_path, block_list, upload_id, total_size, "0")
            .await?;

        Ok(())
    }

    /// 并发上传分片
    ///
    /// # 参数
    /// * `local_path` - 本地文件路径
    /// * `remote_path` - 远程路径
    /// * `total_size` - 文件总大小
    /// * `block_list` - 分片 MD5 列表（用于 createfile）
    /// * `upload_id` - 上传 ID
    /// * `max_concurrent` - 最大并发分片数
    async fn upload_chunks_concurrent(
        &self,
        local_path: &Path,
        remote_path: &str,
        _total_size: u64,
        _block_list: &str,
        upload_id: &str,
        max_concurrent: usize,
    ) -> Result<()> {
        let chunk_count = {
            let cm = self.chunk_manager.lock().await;
            cm.chunk_count()
        };

        info!(
            "[并发上传] 开始上传 {} 个分片，并发数: {}",
            chunk_count, max_concurrent
        );

        // 信号量控制并发数
        let semaphore = Arc::new(Semaphore::new(max_concurrent));

        // JoinSet 管理并发任务
        let mut join_set: JoinSet<Result<(usize, String)>> = JoinSet::new();

        // 分片 MD5 结果收集（按索引存储）
        let chunk_md5s = Arc::new(Mutex::new(vec![String::new(); chunk_count]));

        // 活跃分片计数器
        let active_chunks = Arc::new(AtomicUsize::new(0));

        // 收集首个分片错误（不立即中断，让其他分片跑完）
        let mut first_error: Option<anyhow::Error> = None;

        // 调度所有分片
        loop {
            // 检查取消
            if self.cancel_token.is_cancelled() {
                // 取消所有正在运行的任务
                join_set.abort_all();
                return Err(anyhow::anyhow!("上传已取消"));
            }

            // 检查暂停（暂停时停止调度新分片，但等待当前分片完成）
            while self.is_paused.load(Ordering::SeqCst) {
                if self.cancel_token.is_cancelled() {
                    join_set.abort_all();
                    return Err(anyhow::anyhow!("上传已取消"));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // 获取下一个待上传分片
            let chunk_opt = {
                let mut cm = self.chunk_manager.lock().await;
                match cm.next_pending() {
                    Some(c) => {
                        let chunk = c.clone();
                        cm.mark_uploading(chunk.index);
                        Some(chunk)
                    }
                    None => None,
                }
            };

            match chunk_opt {
                Some(chunk) => {
                    // 尝试获取信号量许可（非阻塞检查）
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            // 信号量已满，等待一个任务完成后再获取
                            // 先把分片状态恢复
                            {
                                let mut cm = self.chunk_manager.lock().await;
                                cm.unmark_uploading(chunk.index);
                            }

                            // 等待一个任务完成
                            if let Some(result) = join_set.join_next().await {
                                if let Err(e) = self.handle_chunk_result(result, &chunk_md5s, &active_chunks).await {
                                    if self.cancel_token.is_cancelled() {
                                        join_set.abort_all();
                                        return Err(e);
                                    }
                                    error!("分片上传失败（其他分片继续）: {}", e);
                                    if first_error.is_none() {
                                        first_error = Some(e);
                                    }
                                }
                            }

                            // 重新获取分片
                            continue;
                        }
                    };

                    // 增加活跃分片计数
                    active_chunks.fetch_add(1, Ordering::SeqCst);

                    // 克隆所需数据
                    let client = self.client.clone();
                    let server_health = self.server_health.clone();
                    let cancel_token = self.cancel_token.clone();
                    let chunk_manager = self.chunk_manager.clone();
                    let uploaded_bytes = self.uploaded_bytes.clone();
                    let task = self.task.clone();
                    let local_path = local_path.to_path_buf();
                    let remote_path = remote_path.to_string();
                    let upload_id = upload_id.to_string();
                    let last_speed_time = self.last_speed_time.clone();
                    let last_speed_bytes = self.last_speed_bytes.clone();
                    let active_chunks_clone = active_chunks.clone();
                    let max_retries = self.max_retries;

                    // 启动分片上传任务
                    let chunk_index = chunk.index;
                    join_set.spawn(async move {
                        let result = upload_single_chunk(
                            chunk,
                            &local_path,
                            &remote_path,
                            &upload_id,
                            client,
                            server_health,
                            cancel_token,
                            chunk_manager,
                            uploaded_bytes,
                            task,
                            last_speed_time,
                            last_speed_bytes,
                            max_retries,
                        )
                            .await;

                        // 减少活跃分片计数
                        active_chunks_clone.fetch_sub(1, Ordering::SeqCst);

                        // 释放信号量
                        drop(permit);

                        result.map(|md5| (chunk_index, md5))
                    });
                }
                None => {
                    // 没有更多待上传分片，等待所有任务完成
                    break;
                }
            }

            // 非阻塞检查是否有任务完成
            while let Some(result) = join_set.try_join_next() {
                if let Err(e) = self.handle_chunk_result(result, &chunk_md5s, &active_chunks).await {
                    if self.cancel_token.is_cancelled() {
                        join_set.abort_all();
                        return Err(e);
                    }
                    error!("分片上传失败（其他分片继续）: {}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // 等待所有剩余任务完成
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = self.handle_chunk_result(result, &chunk_md5s, &active_chunks).await {
                if self.cancel_token.is_cancelled() {
                    join_set.abort_all();
                    return Err(e);
                }
                error!("分片上传失败（其他分片继续）: {}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        // 检查是否有分片失败
        if let Some(e) = first_error {
            return Err(e);
        }

        info!("[并发上传] 所有 {} 个分片上传完成", chunk_count);

        Ok(())
    }

    /// 处理分片上传结果
    async fn handle_chunk_result(
        &self,
        result: Result<Result<(usize, String)>, tokio::task::JoinError>,
        chunk_md5s: &Arc<Mutex<Vec<String>>>,
        _active_chunks: &Arc<AtomicUsize>,
    ) -> Result<()> {
        match result {
            Ok(Ok((chunk_index, md5))) => {
                // 分片上传成功，保存 MD5
                {
                    let mut md5s = chunk_md5s.lock().await;
                    md5s[chunk_index] = md5;
                }
                Ok(())
            }
            Ok(Err(e)) => {
                // 分片上传失败
                error!("分片上传失败: {}", e);
                Err(e)
            }
            Err(e) => {
                // 任务 panic
                error!("分片任务异常: {}", e);
                Err(anyhow::anyhow!("分片任务异常: {}", e))
            }
        }
    }
}

// =====================================================
// 独立的分片上传函数（用于并发调度）
// =====================================================

/// 读取分片数据（独立函数）
async fn read_chunk_data_standalone(local_path: &Path, chunk: &UploadChunk) -> Result<Vec<u8>> {
    let local_path = local_path.to_path_buf();
    let start = chunk.range.start;
    let size = (chunk.range.end - chunk.range.start) as usize;

    tokio::task::spawn_blocking(move || {
        let mut file =
            std::fs::File::open(&local_path).context(format!("无法打开文件: {:?}", local_path))?;
        file.seek(SeekFrom::Start(start))?;

        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    })
        .await?
}

/// 错误分类（独立函数）
fn classify_upload_error(error: &anyhow::Error) -> UploadErrorKind {
    let error_str = error.to_string().to_lowercase();

    if error_str.contains("timeout") || error_str.contains("timed out") {
        UploadErrorKind::Timeout
    } else if error_str.contains("connection")
        || error_str.contains("network")
        || error_str.contains("dns")
    {
        UploadErrorKind::Network
    } else if error_str.contains("429") || error_str.contains("rate limit") {
        UploadErrorKind::RateLimited
    } else if error_str.contains("404") || error_str.contains("not found") {
        UploadErrorKind::FileNotFound
    } else if error_str.contains("403") || error_str.contains("forbidden") {
        UploadErrorKind::Forbidden
    } else if error_str.contains("400") || error_str.contains("bad request") {
        UploadErrorKind::BadRequest
    } else if error_str.contains("500") || error_str.contains("internal server") {
        UploadErrorKind::ServerError
    } else {
        UploadErrorKind::Unknown
    }
}

/// 单分片上传函数（用于并发调度）
///
/// # 参数
/// * `chunk` - 待上传的分片
/// * `local_path` - 本地文件路径
/// * `remote_path` - 远程路径
/// * `upload_id` - 上传 ID
/// * `client` - 网盘客户端
/// * `server_health` - 服务器健康管理器
/// * `cancel_token` - 取消令牌
/// * `chunk_manager` - 分片管理器
/// * `uploaded_bytes` - 已上传字节数（原子计数器）
/// * `task` - 上传任务
/// * `last_speed_time` - 上次速度计算时间
/// * `last_speed_bytes` - 上次速度计算时的字节数
/// * `max_retries` - 最大重试次数
///
/// # 返回
/// 分片 MD5 或错误
async fn upload_single_chunk(
    chunk: UploadChunk,
    local_path: &Path,
    remote_path: &str,
    upload_id: &str,
    client: NetdiskClient,
    server_health: Arc<PcsServerHealthManager>,
    cancel_token: CancellationToken,
    chunk_manager: Arc<Mutex<UploadChunkManager>>,
    uploaded_bytes: Arc<AtomicU64>,
    task: Arc<Mutex<UploadTask>>,
    last_speed_time: Arc<Mutex<std::time::Instant>>,
    last_speed_bytes: Arc<AtomicU64>,
    max_retries: u32,
) -> Result<String> {
    let chunk_size = chunk.range.end - chunk.range.start;

    debug!(
        "[分片#{}] 开始上传 (范围: {}-{}, 大小: {} bytes)",
        chunk.index,
        chunk.range.start,
        chunk.range.end - 1,
        chunk_size
    );

    // 读取分片数据
    let chunk_data = read_chunk_data_standalone(local_path, &chunk).await?;

    // 上传分片（带重试）
    let mut last_error = None;

    for retry in 0..=max_retries {
        // 检查取消
        if cancel_token.is_cancelled() {
            // 取消上传标记
            let mut cm = chunk_manager.lock().await;
            cm.unmark_uploading(chunk.index);
            return Err(anyhow::anyhow!("上传已取消"));
        }

        // 选择服务器（使用加权选择）
        let server = server_health
            .get_server_hybrid(chunk.index)
            .unwrap_or_else(|| "d.pcs.baidu.com".to_string());

        // 上传分片
        let start_time = std::time::Instant::now();
        match client
            .upload_chunk(
                remote_path,
                upload_id,
                chunk.index,
                chunk_data.clone(),
                Some(&server),
            )
            .await
        {
            Ok(response) => {
                // 记录速度到服务器健康管理器
                let elapsed_ms = start_time.elapsed().as_millis() as u64;
                if elapsed_ms > 0 {
                    server_health.record_chunk_speed(&server, chunk_size, elapsed_ms);
                }

                // 更新已上传字节数（原子操作）
                let new_uploaded =
                    uploaded_bytes.fetch_add(chunk_size, Ordering::SeqCst) + chunk_size;

                // 标记分片完成
                let (completed_chunks, total_chunks) = {
                    let mut cm = chunk_manager.lock().await;
                    cm.mark_completed(chunk.index, Some(response.md5.clone()));
                    (cm.completed_count(), cm.chunk_count())
                };

                // 计算上传速度（每次分片完成都更新）
                let speed = {
                    let mut last_time = last_speed_time.lock().await;
                    let elapsed = last_time.elapsed();
                    let elapsed_secs = elapsed.as_secs_f64();

                    if elapsed_secs >= 0.5 {
                        // 至少 0.5 秒更新一次速度
                        let last_bytes = last_speed_bytes.swap(new_uploaded, Ordering::SeqCst);
                        let bytes_diff = new_uploaded.saturating_sub(last_bytes);
                        *last_time = std::time::Instant::now();

                        if elapsed_secs > 0.0 {
                            (bytes_diff as f64 / elapsed_secs) as u64
                        } else {
                            0
                        }
                    } else {
                        // 时间太短，保持上次速度
                        0
                    }
                };

                // 更新任务状态（关键：前端通过这些字段获取进度）
                {
                    let mut t = task.lock().await;
                    t.uploaded_size = new_uploaded;
                    t.completed_chunks = completed_chunks;
                    t.total_chunks = total_chunks;
                    if speed > 0 {
                        t.speed = speed;
                    }
                }

                info!(
                    "[分片#{}] ✓ 上传成功 ({}/{} 完成, 速度: {} KB/s)",
                    chunk.index,
                    completed_chunks,
                    total_chunks,
                    speed / 1024
                );

                return Ok(response.md5);
            }
            Err(e) => {
                // 判断错误类型
                let error_kind = classify_upload_error(&e);

                // 不可重试的错误立即失败
                if !error_kind.is_retriable() {
                    error!(
                        "[分片#{}] 上传失败（不可重试）: {:?}, 错误: {}",
                        chunk.index, error_kind, e
                    );

                    // 取消上传标记并增加重试次数
                    let mut cm = chunk_manager.lock().await;
                    cm.unmark_uploading(chunk.index);
                    cm.increment_retry(chunk.index);

                    return Err(e);
                }

                // 可重试的错误
                if retry < max_retries {
                    let backoff_ms = calculate_backoff_delay(retry, &error_kind);
                    warn!(
                        "[分片#{}] 上传失败，等待 {}ms 后重试 ({}/{}): {}",
                        chunk.index,
                        backoff_ms,
                        retry + 1,
                        max_retries,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }

                last_error = Some(e);
            }
        }
    }

    // 达到最大重试次数
    {
        let mut cm = chunk_manager.lock().await;
        cm.unmark_uploading(chunk.index);
        cm.increment_retry(chunk.index);
    }

    error!(
        "[分片#{}] 上传失败，已达最大重试次数 ({})",
        chunk.index, max_retries
    );

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("上传失败")))
}

/// 并发上传分片
///
/// 使用信号量控制并发数，多个分片同时上传
pub async fn upload_chunks_concurrent(
    engine: Arc<UploadEngine>,
    _max_concurrent: usize,
) -> Result<()> {
    // 这里的实现需要与 UploadManager 配合
    // 由 UploadManager 负责调度多个任务的分片

    // 目前 upload() 方法内部已经实现了单任务的分片上传
    // 这个函数留作后续扩展，用于多任务并发场景

    engine.upload().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_backoff_delay() {
        // 普通错误的退避延迟
        assert_eq!(calculate_backoff_delay(0, &UploadErrorKind::Network), 100);
        assert_eq!(calculate_backoff_delay(1, &UploadErrorKind::Network), 200);
        assert_eq!(calculate_backoff_delay(2, &UploadErrorKind::Network), 400);
        assert_eq!(calculate_backoff_delay(3, &UploadErrorKind::Network), 800);
        assert_eq!(calculate_backoff_delay(10, &UploadErrorKind::Network), 5000); // 超过最大值

        // 限流错误使用更长的等待时间
        assert_eq!(
            calculate_backoff_delay(0, &UploadErrorKind::RateLimited),
            10000
        );
    }

    #[test]
    fn test_upload_error_kind_retriable() {
        assert!(UploadErrorKind::Network.is_retriable());
        assert!(UploadErrorKind::Timeout.is_retriable());
        assert!(UploadErrorKind::ServerError.is_retriable());
        assert!(UploadErrorKind::RateLimited.is_retriable());

        assert!(!UploadErrorKind::FileNotFound.is_retriable());
        assert!(!UploadErrorKind::Forbidden.is_retriable());
        assert!(!UploadErrorKind::BadRequest.is_retriable());
        assert!(!UploadErrorKind::QuotaExceeded.is_retriable());
    }
}
