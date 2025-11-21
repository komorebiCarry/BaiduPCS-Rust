use crate::auth::UserAuth;
use crate::config::{DownloadConfig, VipType};
use crate::downloader::{ChunkManager, DownloadTask, SpeedCalculator};
use crate::netdisk::NetdiskClient;
use anyhow::{Context, Result};
use reqwest::Client;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 最大重试次数
const MAX_RETRIES: u32 = 3;

/// 链接失败阈值（某个链接失败次数超过此值将被剔除）
const URL_FAILURE_THRESHOLD: u32 = 5;

/// URL 健康状态管理器
///
/// 用于追踪下载链接的可用性，并在链接失败时动态剔除不可用的链接
#[derive(Debug, Clone)]
pub struct UrlHealthManager {
    /// 可用的链接列表（索引 -> URL）
    available_urls: Vec<String>,
    /// 链接失败计数（URL -> 失败次数）
    failure_counts: HashMap<String, u32>,
}

impl UrlHealthManager {
    /// 创建新的 URL 健康管理器
    pub fn new(urls: Vec<String>) -> Self {
        Self {
            available_urls: urls,
            failure_counts: HashMap::new(),
        }
    }

    /// 获取可用的链接数量
    pub fn available_count(&self) -> usize {
        self.available_urls.len()
    }

    /// 根据索引获取链接（使用轮询策略）
    pub fn get_url(&self, index: usize) -> Option<&String> {
        if self.available_urls.is_empty() {
            return None;
        }
        let url_index = index % self.available_urls.len();
        self.available_urls.get(url_index)
    }

    /// 记录链接失败，如果失败次数超过阈值则剔除该链接
    ///
    /// 返回：是否剔除了该链接
    pub fn record_failure(&mut self, url: &str) -> bool {
        let count = self.failure_counts.entry(url.to_string()).or_insert(0);
        *count += 1;

        warn!("链接 {} 失败次数: {}/{}", url, *count, URL_FAILURE_THRESHOLD);

        // 如果失败次数超过阈值，从可用列表中移除
        if *count >= URL_FAILURE_THRESHOLD {
            if let Some(pos) = self.available_urls.iter().position(|u| u == url) {
                self.available_urls.remove(pos);
                error!("链接 {} 失败次数过多，已从可用列表中移除（剩余 {} 个可用链接）",
                       url, self.available_urls.len());
                return true;
            }
        }

        false
    }

    /// 记录链接成功（可选：重置失败计数）
    pub fn record_success(&mut self, url: &str) {
        // 成功后可以重置失败计数，给链接"恢复"的机会
        if let Some(count) = self.failure_counts.get_mut(url) {
            if *count > 0 {
                *count = (*count).saturating_sub(1); // 递减失败计数
                debug!("链接 {} 下载成功，失败计数减少至: {}", url, *count);
            }
        }
    }
}

/// 下载引擎
#[derive(Debug, Clone)]
pub struct DownloadEngine {
    /// HTTP 客户端（基础客户端，未使用但保留以备将来使用）
    #[allow(dead_code)]
    client: Client,
    /// 网盘客户端
    netdisk_client: NetdiskClient,
    /// 用户 VIP 等级
    vip_type: VipType,
}

impl DownloadEngine {
    /// 创建新的下载引擎
    pub fn new(user_auth: UserAuth) -> Self {
        // 基础HTTP客户端，使用较长的超时时间以支持大分片下载
        // 实际超时会在每个请求中根据分片大小动态调整
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .timeout(std::time::Duration::from_secs(600)) // 10分钟基础超时（会被请求级别的超时覆盖）
            .build()
            .expect("Failed to build HTTP client");

        // 从 user_auth 中提取 VIP 等级
        let vip_type = VipType::from_u32(user_auth.vip_type.unwrap_or(0));

        let netdisk_client = NetdiskClient::new(user_auth).expect("Failed to create NetdiskClient");

        Self {
            client,
            netdisk_client,
            vip_type,
        }
    }

    /// 创建用于下载的 HTTP 客户端（使用 Android UA 和 Cookie）
    ///
    /// 关键配置：
    /// - DisableKeepAlives: false (启用 Keep-Alive)
    /// - MaxIdleConns: 100
    /// - IdleConnTimeout: 90s
    /// - Timeout: 2min
    /// - CheckRedirect: 删除 Referer
    fn create_download_client(&self) -> Client {
        // 使用 Android 客户端的 User-Agent（与 NetdiskClient 一致）
        let pan_ua = "netdisk;P2SP;3.0.0.8;netdisk;11.12.3;ANG-AN00;android-android;10.0;JSbridge4.4.0;jointBridge;1.1.0;";

        Client::builder()
            .user_agent(pan_ua)
            .timeout(std::time::Duration::from_secs(120)) // 2分钟超时
            .pool_max_idle_per_host(100) // MaxIdleConns: 100
            .pool_idle_timeout(std::time::Duration::from_secs(90)) // IdleConnTimeout: 90s
            .tcp_keepalive(std::time::Duration::from_secs(60)) // TCP Keep-Alive
            .redirect(reqwest::redirect::Policy::limited(10)) // 最多 10 次重定向
            .http1_only()
            .build()
            .expect("Failed to build download HTTP client")
    }

    /// 根据分片大小计算合理的超时时间（秒）
    ///
    /// 假设最低速度为 100KB/s，同时设置最小和最大超时限制
    /// - 最小超时：60秒
    /// - 最大超时：600秒（10分钟）
    fn calculate_timeout_secs(chunk_size: u64) -> u64 {
        const MIN_SPEED_KBPS: u64 = 100; // 最低速度 100KB/s
        const MIN_TIMEOUT: u64 = 60; // 最小超时 60秒
        const MAX_TIMEOUT: u64 = 600; // 最大超时 600秒（10分钟）

        // 计算预期时间：chunk_size / (MIN_SPEED_KBPS * 1024)
        // 再乘以 3 作为缓冲
        let expected_secs = (chunk_size / (MIN_SPEED_KBPS * 1024)) * 3;

        // 限制在合理范围内
        expected_secs.max(MIN_TIMEOUT).min(MAX_TIMEOUT)
    }

    /// 为调度器准备任务（返回所有下载所需的配置信息）
    ///
    /// 此方法执行以下步骤：
    /// 1. 计算自适应分片大小
    /// 2. 获取并探测下载链接
    /// 3. 准备本地文件
    /// 4. 创建分片管理器和速度计算器
    /// 5. 标记任务为下载中
    ///
    /// 返回所有调度器需要的信息
    pub async fn prepare_for_scheduling(
        &self,
        task: Arc<Mutex<DownloadTask>>,
    ) -> Result<(
        Client,                           // HTTP 客户端
        String,                            // Cookie
        Option<String>,                    // Referer 头
        Arc<Mutex<UrlHealthManager>>,      // URL 健康管理器
        PathBuf,                           // 本地路径
        u64,                               // 分片大小
        u64,                               // 超时时间（秒）
        Arc<Mutex<ChunkManager>>,          // 分片管理器
        Arc<Mutex<SpeedCalculator>>,       // 速度计算器
    )> {
        let (fs_id, remote_path, local_path, total_size) = {
            let t = task.lock().await;
            (
                t.fs_id,
                t.remote_path.clone(),
                t.local_path.clone(),
                t.total_size,
            )
        };

        info!("准备任务调度: fs_id={}, 本地路径={:?}", fs_id, local_path);

        // 1. 计算自适应分片大小
        let chunk_size = DownloadConfig::calculate_adaptive_chunk_size(total_size, self.vip_type);
        info!(
            "自适应分片大小: {} bytes ({}), 文件大小: {} bytes, VIP等级: {:?}",
            chunk_size,
            Self::format_size(chunk_size),
            total_size,
            self.vip_type
        );

        // 2. 获取所有可用下载链接
        let all_urls = match self
            .netdisk_client
            .get_locate_download_url(&remote_path)
            .await
        {
            Ok(urls) => {
                if urls.is_empty() {
                    error!("获取到下载链接列表为空: path={}", remote_path);
                    anyhow::bail!("未找到可用的下载链接");
                }
                urls
            }
            Err(e) => {
                error!("获取下载链接列表失败: path={}, 错误: {}", remote_path, e);
                return Err(e).context("获取下载链接列表失败");
            }
        };

        info!("获取到 {} 个下载链接", all_urls.len());

        // 3. 创建用于下载的专用 HTTP 客户端
        let download_client = self.create_download_client();

        // 4. 探测所有下载链接，过滤出可用的链接
        info!("开始探测 {} 个下载链接...", all_urls.len());
        let mut valid_urls = Vec::new();
        let mut referer: Option<String> = None;

        for (i, url) in all_urls.iter().enumerate() {
            match self
                .probe_download_link_with_client(&download_client, url, total_size)
                .await
            {
                Ok(ref_url) => {
                    info!("✓ 链接 #{} 探测成功", i);
                    valid_urls.push(url.clone());

                    // 保存第一个成功链接的 Referer
                    if referer.is_none() {
                        referer = ref_url;
                    }
                }
                Err(e) => {
                    warn!("✗ 链接 #{} 探测失败: {}", i, e);
                }
            }
        }

        // 检查是否有可用链接
        if valid_urls.is_empty() {
            anyhow::bail!("所有下载链接探测失败，无可用链接");
        }

        info!(
            "探测完成: {}/{} 个链接可用",
            valid_urls.len(),
            all_urls.len()
        );

        // 5. 创建 URL 健康管理器
        let url_health = Arc::new(Mutex::new(UrlHealthManager::new(valid_urls)));

        // 6. 创建本地文件
        self.prepare_file(&local_path, total_size)
            .await
            .context("准备本地文件失败")?;

        // 7. 创建分片管理器
        let chunk_manager = Arc::new(Mutex::new(ChunkManager::new(total_size, chunk_size)));

        // 8. 创建速度计算器
        let speed_calc = Arc::new(Mutex::new(SpeedCalculator::with_default_window()));

        // 9. 标记为下载中
        {
            let mut t = task.lock().await;
            t.mark_downloading();
        }

        // 10. 计算超时时间
        let timeout_secs = Self::calculate_timeout_secs(chunk_size);

        // 11. 生成 Cookie
        let cookie = format!("BDUSS={}", self.netdisk_client.bduss());

        info!("任务准备完成，等待调度器调度");

        Ok((
            download_client,
            cookie,
            referer,
            url_health,
            local_path,
            chunk_size,
            timeout_secs,
            chunk_manager,
            speed_calc,
        ))
    }

    /// 下载文件（自动计算最优分片大小）
    ///
    /// # 参数
    /// * `task` - 下载任务
    /// * `global_semaphore` - 全局线程池（所有任务共享）
    pub async fn download(
        &self,
        task: Arc<Mutex<DownloadTask>>,
        global_semaphore: Arc<Semaphore>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let (fs_id, remote_path, local_path, total_size) = {
            let t = task.lock().await;
            (
                t.fs_id,
                t.remote_path.clone(),
                t.local_path.clone(),
                t.total_size,
            )
        };

        info!("开始下载任务: fs_id={}, 本地路径={:?}", fs_id, local_path);

        // 检查任务是否已被取消
        if cancellation_token.is_cancelled() {
            warn!("任务在启动前已被取消");
            return Ok(());
        }

        // 1. 根据文件大小和 VIP 等级自动计算最优分片大小
        let chunk_size = DownloadConfig::calculate_adaptive_chunk_size(total_size, self.vip_type);
        info!(
            "自适应分片大小: {} bytes ({}), 文件大小: {} bytes, VIP等级: {:?}",
            chunk_size,
            Self::format_size(chunk_size),
            total_size,
            self.vip_type
        );

        // 2. 获取所有可用下载链接（用于失败时切换）
        let all_urls = match self
            .netdisk_client
            .get_locate_download_url(&remote_path)
            .await
        {
            Ok(urls) => {
                if urls.is_empty() {
                    error!("获取到下载链接列表为空: path={}", remote_path);
                    anyhow::bail!("未找到可用的下载链接");
                }
                urls
            }
            Err(e) => {
                error!("获取下载链接列表失败: path={}, 错误: {}", remote_path, e);
                return Err(e).context("获取下载链接列表失败");
            }
        };

        info!("获取到 {} 个下载链接", all_urls.len());

        // 检查任务是否已被取消
        if cancellation_token.is_cancelled() {
            warn!("任务在获取下载链接后被取消");
            return Ok(());
        }

        // 3. 尝试下载（URL 探测和链接管理已在 try_download_with_url 中实现）
        match self
            .try_download_with_url(
                task.clone(),
                global_semaphore.clone(),
                &remote_path,
                &all_urls,
                total_size,
                chunk_size,
                &local_path,
                cancellation_token.clone(),
            )
            .await
        {
            Ok(_) => {
                // 下载成功，标记任务完成
                let mut t = task.lock().await;
                t.mark_completed();
                info!("✓ 任务下载完成: {}", t.id);
                Ok(())
            }
            Err(e) => {
                // 检查是否是因为取消而失败
                if cancellation_token.is_cancelled() {
                    info!("任务已被用户取消");
                    return Ok(());
                }

                // 下载失败，标记任务失败
                let mut t = task.lock().await;
                let error_msg = e.to_string();
                t.mark_failed(error_msg.clone());
                error!("✗ 任务下载失败: {}, 错误: {}", t.id, error_msg);
                Err(e)
            }
        }
    }

    /// 使用指定URL列表尝试下载
    async fn try_download_with_url(
        &self,
        task: Arc<Mutex<DownloadTask>>,
        global_semaphore: Arc<Semaphore>,
        _remote_path: &str, // 保留参数以保持接口一致性，但当前未使用
        download_urls: &[String],
        total_size: u64,
        chunk_size: u64,
        local_path: &Path,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 1. 创建用于下载的专用 HTTP 客户端（所有请求复用同一个 client）
        // ⚠️ 关键：必须复用 client 以保持连接池和 session 一致
        let download_client = self.create_download_client();

        // 2. 探测所有下载链接，过滤出可用的链接
        info!("开始探测 {} 个下载链接...", download_urls.len());
        let mut valid_urls = Vec::new();
        let mut referer: Option<String> = None;

        for (i, url) in download_urls.iter().enumerate() {
            // 检查任务是否已被取消
            if cancellation_token.is_cancelled() {
                warn!("任务在探测链接时被取消");
                anyhow::bail!("任务已被取消");
            }

            match self
                .probe_download_link_with_client(&download_client, url, total_size)
                .await
            {
                Ok(ref_url) => {
                    info!("✓ 链接 #{} 探测成功", i);
                    valid_urls.push(url.clone());

                    // 保存第一个成功链接的 Referer
                    if referer.is_none() {
                        referer = ref_url;
                    }
                }
                Err(e) => {
                    warn!("✗ 链接 #{} 探测失败: {}", i, e);
                }
            }
        }

        // 检查是否有可用链接
        if valid_urls.is_empty() {
            anyhow::bail!("所有下载链接探测失败，无可用链接");
        }

        info!(
            "探测完成: {}/{} 个链接可用",
            valid_urls.len(),
            download_urls.len()
        );

        // 3. 创建 URL 健康管理器
        let url_health = Arc::new(Mutex::new(UrlHealthManager::new(valid_urls)));

        // 4. 创建本地文件
        self.prepare_file(local_path, total_size)
            .await
            .context("准备本地文件失败")?;

        // 5. 创建分片管理器（使用自适应计算的 chunk_size）
        let chunk_manager = Arc::new(Mutex::new(ChunkManager::new(total_size, chunk_size)));

        // 6. 创建速度计算器
        let speed_calc = Arc::new(Mutex::new(SpeedCalculator::with_default_window()));

        // 7. 标记为下载中
        {
            let mut t = task.lock().await;
            t.mark_downloading();
        }

        // 8. 并发下载分片（使用全局 Semaphore 和复用的 download_client，使用 URL 健康管理器）
        self.download_chunks(
            task.clone(),
            chunk_manager.clone(),
            speed_calc.clone(),
            global_semaphore,
            &download_client, // 传递复用的 client
            url_health,       // 传递 URL 健康管理器
            local_path,
            chunk_size,         // 传递分片大小用于计算超时
            total_size,         // 传递文件总大小用于计算延迟
            referer.as_deref(), // 传递 Referer 头（如果存在）
            cancellation_token, // 传递取消令牌
        )
        .await
        .context("下载分片失败")?;

        // 9. 校验文件大小
        self.verify_file_size(local_path, total_size)
            .await
            .context("文件大小校验失败")?;

        Ok(())
    }

    /// 探测下载链接（发送 32KB Range 请求验证）
    ///
    /// 通过小体积的 Range 请求快速验证：
    /// 1. 下载链接是否有效
    /// 2. 服务器是否支持 Range 请求
    /// 3. 文件大小是否匹配
    /// 4. 是否有重定向或其他问题
    ///
    /// # 参数
    /// * `client` - 复用的 HTTP 客户端（确保与后续分片下载使用同一个 client）
    /// * `url` - 下载链接
    /// * `expected_size` - 预期文件大小
    ///
    /// # 返回值
    /// 返回用于后续 Range 请求的 Referer：
    /// - 如果有重定向：返回原始 URL
    /// - 如果无重定向：返回 None（不设置 Referer）
    async fn probe_download_link_with_client(
        &self,
        client: &Client,
        url: &str,
        expected_size: u64,
    ) -> Result<Option<String>> {
        const PROBE_SIZE: u64 = 32 * 1024; // 32KB

        let probe_end = if expected_size > 0 {
            (PROBE_SIZE - 1).min(expected_size - 1)
        } else {
            PROBE_SIZE - 1
        };

        info!(
            "🔍 探测下载链接: Range 0-{} ({} bytes)",
            probe_end,
            probe_end + 1
        );

        // 使用传入的复用 client（与后续分片下载使用同一个 client）
        let bduss = self.netdisk_client.bduss();

        let response = client
            .get(url)
            .header("Cookie", format!("BDUSS={}", bduss))
            .header("Range", format!("bytes=0-{}", probe_end))
            .send()
            .await
            .context("发送探测请求失败")?;

        let status = response.status();
        info!("📡 探测响应状态: {}", status);

        // 检查状态码（应该是 206 Partial Content）
        if status != reqwest::StatusCode::PARTIAL_CONTENT && status != reqwest::StatusCode::OK {
            anyhow::bail!(
                "探测失败: 服务器返回异常状态码 {} (期望 206 或 200)",
                status
            );
        }

        // 检查是否支持 Range
        let accept_ranges = response
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("none");

        if accept_ranges == "none" && status != reqwest::StatusCode::PARTIAL_CONTENT {
            warn!(
                "⚠️  服务器可能不支持 Range 请求 (Accept-Ranges: {})",
                accept_ranges
            );
        } else {
            info!(
                "✅ 服务器支持 Range 请求 (Accept-Ranges: {})",
                accept_ranges
            );
        }

        // 检查 Content-Length 或 Content-Range
        if let Some(content_range) = response.headers().get("content-range") {
            if let Ok(range_str) = content_range.to_str() {
                info!("📦 Content-Range: {}", range_str);

                // 解析 Content-Range: bytes 0-32767/1234567
                if let Some(total_str) = range_str.split('/').nth(1) {
                    if let Ok(total_size) = total_str.parse::<u64>() {
                        if expected_size > 0 && total_size != expected_size {
                            warn!(
                                "⚠️  文件大小不匹配: 服务器报告 {} bytes, 期望 {} bytes",
                                total_size, expected_size
                            );
                        } else {
                            info!("✅ 文件大小验证通过: {} bytes", total_size);
                        }
                    }
                }
            }
        }

        // 获取最终的 URL（如果有重定向，这将是重定向后的 URL）
        let final_url = response.url().to_string();

        // 如果 URL 发生了变化（有重定向），使用原始 URL 作为 Referer
        // 如果没有重定向，不设置 Referer（返回 None）
        let referer = if final_url != url {
            info!("📋 检测到重定向: {} -> {}", url, final_url);
            info!("📋 将使用原始 URL 作为 Referer");
            Some(url.to_string())
        } else {
            info!("📋 无重定向，不设置 Referer 请求头");
            None
        };

        // 读取探测数据（但不保存，只是为了验证连接）
        let probe_data = response.bytes().await.context("读取探测数据失败")?;
        info!(
            "✅ 探测成功: 收到 {} bytes 数据，链接有效",
            probe_data.len()
        );

        Ok(referer)
    }

    /// 格式化文件大小为人类可读格式
    fn format_size(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    /// 准备本地文件（预分配空间）
    async fn prepare_file(&self, path: &Path, size: u64) -> Result<()> {
        // 创建父目录
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("创建父目录失败")?;
        }

        // 创建文件并预分配空间
        let file = File::create(path).await.context("创建文件失败")?;
        file.set_len(size).await.context("预分配文件空间失败")?;

        info!("文件准备完成: {:?}, 大小: {} bytes", path, size);
        Ok(())
    }

    /// 校验文件大小
    ///
    /// 如果文件大小不匹配，返回错误，触发链接切换
    async fn verify_file_size(&self, path: &Path, expected_size: u64) -> Result<()> {
        let metadata = tokio::fs::metadata(path)
            .await
            .context("获取文件元数据失败")?;

        let actual_size = metadata.len();

        if actual_size != expected_size {
            anyhow::bail!(
                "文件大小不匹配: 实际 {} bytes, 期望 {} bytes (差异: {} bytes)",
                actual_size,
                expected_size,
                actual_size as i64 - expected_size as i64
            );
        }

        info!("✅ 文件大小校验通过: {} bytes", actual_size);
        Ok(())
    }

    /// 并发下载所有分片
    ///
    /// 使用全局 Semaphore 控制并发，实现优雅的线程分配：
    /// - 单文件下载：可以使用全部可用线程
    /// - 多文件下载：自动平衡分配，不会强制中断已开始的分片
    ///
    /// # 参数
    /// * `client` - 复用的 HTTP 客户端（确保所有分片使用同一个 client）
    /// * `chunk_size` - 分片大小（用于计算超时）
    /// * `total_size` - 文件总大小（用于判断是否大文件，调整延迟）
    /// * `referer` - Referer 头（如果存在），用于 Range 请求避免 403 Forbidden
    /// * `cancellation_token` - 取消令牌（用于中断下载）
    async fn download_chunks(
        &self,
        task: Arc<Mutex<DownloadTask>>,
        chunk_manager: Arc<Mutex<ChunkManager>>,
        speed_calc: Arc<Mutex<SpeedCalculator>>,
        global_semaphore: Arc<Semaphore>,
        client: &Client,
        url_health: Arc<Mutex<UrlHealthManager>>,
        output_path: &Path,
        chunk_size: u64,
        _total_size: u64,
        referer: Option<&str>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 获取所有待下载的分片
        let chunks_to_download: Vec<usize> = {
            let manager = chunk_manager.lock().await;
            (0..manager.chunk_count()).collect()
        };

        // 根据分片大小计算超时时间
        let timeout_secs = Self::calculate_timeout_secs(chunk_size);

        let available_urls_count = {
            let health = url_health.lock().await;
            health.available_count()
        };

        info!(
            "开始并发下载 {} 个分片 (每个分片超时: {}秒, {} 个可用链接)",
            chunks_to_download.len(),
            timeout_secs,
            available_urls_count
        );

        // 创建下载专用的 Cookie
        let bduss = self.netdisk_client.bduss().to_string();
        let cookie = format!("BDUSS={}", bduss);

        // 将 Referer 转换为 String（如果存在）
        let referer = referer.map(|s| s.to_string());

        let mut handles = Vec::new();

        for chunk_index in chunks_to_download {
            // 检查任务是否已被取消
            if cancellation_token.is_cancelled() {
                warn!("任务在创建分片任务时被取消，停止创建新的分片任务");
                break;
            }

            // 🔥 关键：立即 spawn 所有分片任务（真正的并发）
            // - 所有分片任务立即创建，不会因为 semaphore 而阻塞循环
            // - 每个任务在内部等待 permit，实现公平调度
            // - 多任务场景下，不同任务的分片会交替获得 permit，避免单任务霸占线程池
            let global_semaphore = global_semaphore.clone();

            // ⚠️ 关键：使用引用传递 client，所有分片共享同一个 client
            // 这样可以复用 TCP 连接，避免被百度检测为多个独立连接
            let client = client.clone(); // 克隆 Arc，不是创建新 client
            let cookie = cookie.clone();
            let referer = referer.clone(); // 克隆 Referer
            let url_health = url_health.clone();
            let output_path = output_path.to_path_buf();
            let chunk_manager = chunk_manager.clone();
            let speed_calc = speed_calc.clone();
            let task = task.clone();
            let cancellation_token = cancellation_token.clone();

            let handle = tokio::spawn(async move {
                // ✅ 在任务内部获取 permit（不会阻塞循环，实现真正的并发启动）
                // - 如果有空闲线程，立即获取并开始下载
                // - 如果线程池满了，在这里等待（不影响其他分片任务的创建）
                // - 当其他分片完成后，会自动释放 permit，这个分片就能继续
                debug!("分片 #{} 等待获取线程资源...", chunk_index);

                let permit = match global_semaphore.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        error!("分片 #{} 获取 semaphore permit 失败（semaphore 可能已关闭）", chunk_index);
                        return Err(anyhow::anyhow!("获取线程池资源失败"));
                    }
                };

                let thread_id = std::thread::current().id();
                let thread_name = std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_string();

                info!(
                    "[线程: {}/{}] 分片 #{} 获得线程资源，开始下载",
                    thread_name,
                    format!("{:?}", thread_id),
                    chunk_index
                );

                let result = Self::download_chunk_with_retry(
                    chunk_index,
                    client,
                    &cookie,
                    referer.as_deref(), // 传递 Referer
                    url_health,
                    &output_path,
                    chunk_manager.clone(),
                    speed_calc.clone(),
                    task.clone(),
                    timeout_secs,
                    cancellation_token, "usize".parse()?
                )
                .await;

                drop(permit); // 🔥 释放 permit，其他等待的分片可以使用

                info!(
                    "[线程: {}/{}] 分片 #{} 释放线程资源",
                    thread_name,
                    format!("{:?}", thread_id),
                    chunk_index
                );

                result
            });

            handles.push(handle);
        }

        // 等待所有分片完成
        for handle in handles {
            match handle.await {
                Ok(Ok(_)) => {}, // 分片下载成功
                Ok(Err(e)) => {
                    // 分片下载失败，检查是否是因为取消
                    if cancellation_token.is_cancelled() {
                        warn!("分片下载因任务取消而失败");
                        anyhow::bail!("任务已被取消");
                    }
                    return Err(e);
                }
                Err(e) => {
                    error!("分片任务异常: {}", e);
                    anyhow::bail!("分片任务异常: {}", e);
                }
            }
        }

        // 检查任务是否在下载过程中被取消
        if cancellation_token.is_cancelled() {
            warn!("任务在下载过程中被取消");
            anyhow::bail!("任务已被取消");
        }

        // 验证所有分片是否完成
        let manager = chunk_manager.lock().await;
        if !manager.is_completed() {
            anyhow::bail!("部分分片下载失败");
        }

        Ok(())
    }

    /// 下载单个分片（带重试和智能链接切换）
    ///
    /// # 功能
    /// - 使用轮询策略选择初始下载链接
    /// - 下载失败时自动切换到其他可用链接
    /// - 记录链接失败次数，失败过多时自动剔除
    /// - 成功下载后记录链接成功，给链接"恢复"的机会
    ///
    /// # 参数
    /// * `chunk_index` - 分片索引
    /// * `client` - HTTP 客户端
    /// * `cookie` - Cookie 字符串
    /// * `referer` - Referer 头（如果存在），用于 Range 请求避免 403 Forbidden
    /// * `url_health` - URL 健康管理器，用于动态管理可用链接
    /// * `output_path` - 输出文件路径
    /// * `chunk_manager` - 分片管理器
    /// * `speed_calc` - 速度计算器
    /// * `task` - 下载任务
    /// * `timeout_secs` - 超时时间（秒）
    /// * `cancellation_token` - 取消令牌（用于中断下载）
    /// * `chunk_thread_id` - 分片线程ID（用于日志）
    pub async fn download_chunk_with_retry(
        chunk_index: usize,
        client: Client,
        cookie: &str,
        referer: Option<&str>,
        url_health: Arc<Mutex<UrlHealthManager>>,
        output_path: &Path,
        chunk_manager: Arc<Mutex<ChunkManager>>,
        speed_calc: Arc<Mutex<SpeedCalculator>>,
        task: Arc<Mutex<DownloadTask>>,
        timeout_secs: u64,
        cancellation_token: CancellationToken,
        chunk_thread_id: usize,
    ) -> Result<()> {
        // 记录尝试过的链接（避免在同一次重试循环中重复尝试同一个链接）
        let mut tried_urls = std::collections::HashSet::new();
        let mut retries = 0;
        #[allow(unused_assignments)]
        let mut last_error = None;

        loop {
            // 检查任务是否已被取消
            if cancellation_token.is_cancelled() {
                warn!("[分片线程{}] 分片 #{} 下载被取消", chunk_thread_id, chunk_index);
                anyhow::bail!("分片下载已被取消");
            }

            // 检查是否还有可用链接
            let (available_count, current_url) = {
                let health = url_health.lock().await;
                let count = health.available_count();
                if count == 0 {
                    anyhow::bail!("所有下载链接都不可用");
                }

                // 🔄 URL 轮询策略：
                // 1. 首次尝试：根据分片索引选择链接（chunk_index % count）
                // 2. 重试时：尝试下一个未尝试过的链接
                let url_index = if retries == 0 {
                    chunk_index % count
                } else {
                    // 重试时，找到一个还没尝试过的链接
                    let mut index = chunk_index % count;
                    for i in 0..count {
                        index = (chunk_index + i) % count;
                        if let Some(url) = health.get_url(index) {
                            if !tried_urls.contains(url.as_str()) {
                                break;
                            }
                        }
                    }
                    index
                };

                let url = health
                    .get_url(url_index)
                    .ok_or_else(|| anyhow::anyhow!("无法获取 URL"))?
                    .clone();

                (count, url)
            };

            // 记录该链接已尝试
            tried_urls.insert(current_url.clone());

            debug!(
                "[分片线程{}] 分片 #{} 使用链接: {} (可用链接数: {}, 重试次数: {})",
                chunk_thread_id,
                chunk_index,
                current_url,
                available_count,
                retries
            );

            // 获取分片信息
            let mut chunk = {
                let mut manager = chunk_manager.lock().await;
                manager.chunks_mut()[chunk_index].clone()
            };

            // 创建进度回调闭包（实时更新任务进度和速度）
            let task_clone = task.clone();
            let speed_calc_clone = speed_calc.clone();
            let progress_callback = move |bytes: u64| {
                // 使用 tokio::task::block_in_place 在同步闭包中执行异步操作
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        // 更新任务已下载大小
                        {
                            let mut t = task_clone.lock().await;
                            t.downloaded_size += bytes;
                        }

                        // 更新速度计算器
                        {
                            let mut calc = speed_calc_clone.lock().await;
                            calc.add_sample(bytes);

                            // 更新任务速度
                            let mut t = task_clone.lock().await;
                            t.speed = calc.speed();
                        }
                    })
                });
            };

            // 尝试下载
            match chunk
                .download(
                    &client,
                    cookie,
                    referer,
                    &current_url,
                    output_path,
                    timeout_secs,
                    chunk_thread_id,
                    progress_callback,
                )
                .await
            {
                Ok(_bytes_downloaded) => {
                    // ✅ 下载成功

                    // 记录链接成功（减少失败计数，给链接"恢复"的机会）
                    {
                        let mut health = url_health.lock().await;
                        health.record_success(&current_url);
                    }

                    // 更新分片状态
                    {
                        let mut manager = chunk_manager.lock().await;
                        manager.mark_completed(chunk_index);
                    }

                    // 注意：进度和速度已经在 progress_callback 中实时更新，无需再次更新

                    info!(
                        "[分片线程{}] ✓ 分片 #{} 下载成功",
                        chunk_thread_id, chunk_index
                    );
                    return Ok(());
                }
                Err(e) => {
                    // ❌ 下载失败

                    // 记录链接失败（可能会触发链接剔除）
                    let removed = {
                        let mut health = url_health.lock().await;
                        health.record_failure(&current_url)
                    };

                    if removed {
                        warn!(
                            "[分片线程{}] ✗ 分片 #{} 下载失败，链接已被剔除: {}",
                            chunk_thread_id, chunk_index, current_url
                        );
                    }

                    last_error = Some(e);
                    retries += 1;

                    // 检查是否达到重试次数上限，或所有链接都已尝试过
                    if retries >= MAX_RETRIES || tried_urls.len() >= available_count {
                        error!(
                            "[分片线程{}] ✗ 分片 #{} 下载失败，已尝试 {} 个链接，重试 {} 次",
                            chunk_thread_id, chunk_index, tried_urls.len(), retries
                        );
                        return Err(last_error.unwrap_or_else(|| {
                            anyhow::anyhow!("分片 #{} 下载失败", chunk_index)
                        }));
                    }

                    warn!(
                        "[分片线程{}] ⚠ 分片 #{} 下载失败，切换链接重试 (已尝试 {}/{} 个链接，重试 {}/{}): {:?}",
                        chunk_thread_id,
                        chunk_index,
                        tried_urls.len(),
                        available_count,
                        retries,
                        MAX_RETRIES,
                        last_error
                    );

                    // 等待一段时间后重试（使用不同的链接）
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UserAuth;

    fn create_mock_user_auth() -> UserAuth {
        UserAuth {
            uid: 123456789,
            username: "test_user".to_string(),
            nickname: Some("测试用户".to_string()),
            avatar_url: Some("https://example.com/avatar.jpg".to_string()),
            vip_type: Some(2), // SVIP
            total_space: Some(2 * 1024 * 1024 * 1024 * 1024), // 2TB
            used_space: Some(500 * 1024 * 1024 * 1024), // 500GB
            bduss: "mock_bduss".to_string(),
            stoken: Some("mock_stoken".to_string()),
            ptoken: Some("mock_ptoken".to_string()),
            cookies: Some("BDUSS=mock_bduss".to_string()),
            login_time: 0,
        }
    }

    #[test]
    fn test_engine_creation() {
        let user_auth = create_mock_user_auth();
        let engine = DownloadEngine::new(user_auth);
        assert_eq!(engine.vip_type as u32, 2); // SVIP
    }
}
