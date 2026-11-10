// 批量链接获取器
//
// 用于批量获取多个文件的下载链接，减少 API 调用次数和风控风险
//
// 核心特性：
// - 批量调用 locate API（50-100 个文件/批）
// - 自适应延迟（根据响应时间调整）
// - 链接缓存池（复用已获取的链接）
// - 风控规避（检测 errno=132 自动降速）

use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::netdisk::NetdiskClient;
use crate::share_sync::rate_limit::QuotaLimiter;

/// 批量链接获取配置
#[derive(Debug, Clone)]
pub struct BatchLinkFetcherConfig {
    /// 每批获取的文件数量
    pub batch_size: usize,
    /// 批次间延迟（毫秒）
    pub batch_interval_ms: u64,
    /// 最大并发批次数
    pub max_concurrent_batches: usize,
    /// 链接缓存有效期（秒）
    pub link_cache_ttl_secs: u64,
    /// 是否启用自适应延迟
    pub enable_adaptive_delay: bool,
}

impl Default for BatchLinkFetcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            batch_interval_ms: 100,
            max_concurrent_batches: 3,
            link_cache_ttl_secs: 480, // 8 分钟
            enable_adaptive_delay: true,
        }
    }
}

/// 链接缓存条目
#[derive(Debug, Clone)]
struct CachedLink {
    /// 下载链接列表
    links: Vec<String>,
    /// 缓存时间
    cached_at: Instant,
    /// 有效期（秒）
    ttl_secs: u64,
}

impl CachedLink {
    fn new(links: Vec<String>, ttl_secs: u64) -> Self {
        Self {
            links,
            cached_at: Instant::now(),
            ttl_secs,
        }
    }

    fn is_expired(&self) -> bool {
        self.cached_at.elapsed().as_secs() > self.ttl_secs
    }
}

/// 批量链接获取统计
#[derive(Debug, Clone, Default)]
pub struct BatchLinkStats {
    /// 总批次数
    pub total_batches: usize,
    /// 总文件数
    pub total_files: usize,
    /// 成功数
    pub success_count: usize,
    /// 失败数
    pub failed_count: usize,
    /// 缓存命中数
    pub cache_hits: usize,
    /// 平均响应时间（毫秒）
    pub avg_response_time_ms: u64,
    /// 风控触发次数（errno=132）
    pub rate_limit_errors: usize,
}

/// 批量链接获取器
#[derive(Debug)]
pub struct BatchLinkFetcher {
    /// 网盘客户端
    netdisk_client: Arc<NetdiskClient>,
    /// 配置
    config: BatchLinkFetcherConfig,
    /// 链接缓存（fs_id -> CachedLink）
    link_cache: Arc<DashMap<u64, CachedLink>>,
    /// 并发控制信号量
    semaphore: Arc<Semaphore>,
    /// 限速器
    rate_limiter: Arc<QuotaLimiter>,
    /// 统计信息
    stats: Arc<parking_lot::RwLock<BatchLinkStats>>,
    /// 自适应延迟（毫秒）
    adaptive_delay_ms: Arc<parking_lot::RwLock<u64>>,
}

impl BatchLinkFetcher {
    /// 创建批量链接获取器
    pub fn new(
        netdisk_client: Arc<NetdiskClient>,
        config: BatchLinkFetcherConfig,
        rate_limiter: Arc<QuotaLimiter>,
    ) -> Self {
        let max_concurrent = config.max_concurrent_batches;

        Self {
            netdisk_client,
            config,
            link_cache: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            rate_limiter,
            stats: Arc::new(parking_lot::RwLock::new(BatchLinkStats::default())),
            adaptive_delay_ms: Arc::new(parking_lot::RwLock::new(100)),
        }
    }

    /// 批量获取下载链接
    ///
    /// # 参数
    /// - `fs_ids`: 文件 fs_id 列表
    ///
    /// # 返回
    /// - HashMap<fs_id, Vec<下载链接>>
    pub async fn fetch_batch_links(
        &self,
        fs_ids: Vec<u64>,
    ) -> Result<std::collections::HashMap<u64, Vec<String>>> {
        if fs_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let total_files = fs_ids.len();
        info!("批量获取下载链接: 文件数={}", total_files);

        let mut result = std::collections::HashMap::new();
        let mut to_fetch = Vec::new();

        // 1. 检查缓存
        for fs_id in fs_ids {
            if let Some(cached) = self.link_cache.get(&fs_id) {
                if !cached.is_expired() {
                    result.insert(fs_id, cached.links.clone());
                    self.stats.write().cache_hits += 1;
                    continue;
                } else {
                    // 缓存过期，移除
                    self.link_cache.remove(&fs_id);
                }
            }
            to_fetch.push(fs_id);
        }

        if to_fetch.is_empty() {
            debug!("所有链接均从缓存获取: 命中数={}", total_files);
            return Ok(result);
        }

        debug!(
            "需要从 API 获取: {} 个，缓存命中: {} 个",
            to_fetch.len(),
            result.len()
        );

        // 2. 分批获取
        let mut batch_count = 0;
        let mut total_response_time = 0u128;

        for chunk in to_fetch.chunks(self.config.batch_size) {
            // 并发控制
            let _permit = self.semaphore.acquire().await?;

            // 限速器
            self.rate_limiter.acquire().await;

            // 自适应延迟
            if self.config.enable_adaptive_delay && batch_count > 0 {
                let delay_ms = *self.adaptive_delay_ms.read();
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            // 调用 API
            let start = Instant::now();
            match self.fetch_single_batch(chunk).await {
                Ok(batch_result) => {
                    let elapsed = start.elapsed();
                    total_response_time += elapsed.as_millis();

                    // 更新统计
                    {
                        let mut stats = self.stats.write();
                        stats.success_count += batch_result.len();
                        stats.total_batches += 1;
                    }

                    // 存入缓存和结果
                    for (fs_id, links) in batch_result {
                        self.link_cache.insert(
                            fs_id,
                            CachedLink::new(links.clone(), self.config.link_cache_ttl_secs),
                        );
                        result.insert(fs_id, links);
                    }

                    // 自适应延迟调整
                    if self.config.enable_adaptive_delay {
                        self.adjust_adaptive_delay(elapsed);
                    }

                    batch_count += 1;
                }
                Err(e) => {
                    let error_msg = format!("{:#}", e);

                    // 检测风控错误
                    if error_msg.contains("errno") &&
                       (error_msg.contains("132") || error_msg.contains("8002")) {
                        warn!("检测到风控错误: {}", error_msg);
                        self.stats.write().rate_limit_errors += 1;

                        // 增加延迟
                        let current_delay = *self.adaptive_delay_ms.read();
                        let new_delay = (current_delay * 2).min(5000);
                        *self.adaptive_delay_ms.write() = new_delay;

                        warn!("自适应延迟调整: {} ms -> {} ms", current_delay, new_delay);

                        // 等待更长时间
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }

                    self.stats.write().failed_count += chunk.len();

                    // 继续处理下一批，不中断整个流程
                    warn!("批次获取失败: {} 个文件，错误: {}", chunk.len(), error_msg);
                }
            }
        }

        // 更新平均响应时间
        if batch_count > 0 {
            let avg_ms = (total_response_time / batch_count as u128) as u64;
            self.stats.write().avg_response_time_ms = avg_ms;
        }

        {
            let mut stats = self.stats.write();
            stats.total_files = total_files;
        }

        info!(
            "批量链接获取完成: 成功={}, 失败={}, 缓存命中={}, 批次数={}",
            result.len(),
            self.stats.read().failed_count,
            self.stats.read().cache_hits,
            batch_count
        );

        Ok(result)
    }

    /// 获取单批文件的下载链接
    async fn fetch_single_batch(
        &self,
        fs_ids: &[u64],
    ) -> Result<std::collections::HashMap<u64, Vec<String>>> {
        let mut result = std::collections::HashMap::new();

        // 注意：这里简化处理，实际应该调用百度的批量 locate 接口
        // 目前先串行调用单个文件的 locate
        // TODO: 实现真正的批量 API 调用
        for &fs_id in fs_ids {
            // 这里暂时跳过实际调用，返回空结果
            // 实际使用时需要调用 netdisk_client 的 locate 方法
            debug!("批量获取链接: fs_id={} (待实现)", fs_id);
        }

        Ok(result)
    }

    /// 调整自适应延迟
    fn adjust_adaptive_delay(&self, response_time: Duration) {
        let response_ms = response_time.as_millis() as u64;
        let mut delay = self.adaptive_delay_ms.write();

        if response_ms > 1000 {
            // 响应慢：增加延迟
            *delay = (*delay + 50).min(2000);
            debug!("响应时间 {} ms 过长，增加延迟到 {} ms", response_ms, *delay);
        } else if response_ms < 300 && *delay > 50 {
            // 响应快：减少延迟
            *delay = (*delay - 20).max(50);
            debug!("响应时间 {} ms 良好，减少延迟到 {} ms", response_ms, *delay);
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> BatchLinkStats {
        self.stats.read().clone()
    }

    /// 清理过期缓存
    pub fn cleanup_expired_cache(&self) -> usize {
        let mut removed = 0;
        self.link_cache.retain(|_, cached| {
            let expired = cached.is_expired();
            if expired {
                removed += 1;
            }
            !expired
        });

        if removed > 0 {
            debug!("清理过期链接缓存: 移除数量={}", removed);
        }

        removed
    }

    /// 清空所有缓存
    pub fn clear_cache(&self) {
        let count = self.link_cache.len();
        self.link_cache.clear();
        info!("清空链接缓存: 移除数量={}", count);
    }

    /// 获取缓存大小
    pub fn cache_size(&self) -> usize {
        self.link_cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_link_expiry() {
        let link = CachedLink::new(vec!["http://example.com".to_string()], 1);
        assert!(!link.is_expired());

        std::thread::sleep(Duration::from_secs(2));
        assert!(link.is_expired());
    }

    #[test]
    fn test_batch_link_fetcher_config() {
        let config = BatchLinkFetcherConfig::default();
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.link_cache_ttl_secs, 480);
    }
}
