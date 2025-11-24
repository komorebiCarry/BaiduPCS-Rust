use crate::auth::UserAuth;
use crate::config::{DownloadConfig, VipType};
use crate::downloader::{ChunkManager, DownloadTask, SpeedCalculator};
use crate::netdisk::NetdiskClient;
use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::fs::File;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 最大重试次数
const MAX_RETRIES: u32 = 3;

/// 重试指数退避初始延迟（毫秒）
const INITIAL_BACKOFF_MS: u64 = 100;

/// 重试指数退避最大延迟（毫秒）
const MAX_BACKOFF_MS: u64 = 5000;

/// 最少保留链接数
const MIN_AVAILABLE_LINKS: usize = 2;

/// 短期速度窗口大小（用于 score 判定）
/// 推荐值：5-10，避免早期高速持续影响后期判定
const SPEED_WINDOW_SIZE: usize = 7;

/// 窗口最小样本数（开始评分的阈值）
/// 只有窗口积累了这么多样本，才开始使用窗口 median 进行 score 判定
/// 避免前期数据不足导致误判
const MIN_WINDOW_SAMPLES: usize = 5;

/// 🔥 计算指数退避延迟
///
/// # 延迟序列
/// - retry_count=0: 100ms
/// - retry_count=1: 200ms
/// - retry_count=2: 400ms
/// - retry_count=3: 800ms
/// - ...
/// - 最大: 5000ms
fn calculate_backoff_delay(retry_count: u32) -> u64 {
    let delay = INITIAL_BACKOFF_MS * 2u64.pow(retry_count);
    delay.min(MAX_BACKOFF_MS)
}

/// URL 健康状态管理器
///
/// 用于追踪下载链接的可用性，支持动态权重调整
/// - 权重 > 0：链接可用
/// - 权重 = 0：链接被淘汰（因慢速或失败）
///
/// 使用 score 评分机制 (0-100):
/// - score <= 10: 降权
/// - score >= 30: 恢复
/// - 慢速扣分2，正常加分3
///
/// 速度追踪双轨制：
/// - 短期窗口 median（N=7）：用于 score 判定，避免早期高速影响
/// - EWMA（α=0.85）：用于 timeout 计算和长期统计
///
/// 🔥 并发优化：使用 DashMap + AtomicU64，消除 Mutex 瓶颈
#[derive(Debug, Clone)]
pub struct UrlHealthManager {
    /// 所有链接列表（包括已淘汰的）- 不可变，无需同步
    all_urls: Vec<String>,

    // 🔥 HashMap → DashMap（无锁并发）
    /// 链接权重（URL -> 权重，>0可用，=0不可用）
    weights: Arc<DashMap<String, u32>>,
    /// URL速度映射（URL -> 探测速度KB/s）
    url_speeds: Arc<DashMap<String, f64>>,
    /// URL评分 (0-100), 低于10降权, 高于30恢复
    url_scores: Arc<DashMap<String, i32>>,
    /// 链接下次探测时间 (URL -> Instant)
    next_probe_time: Arc<DashMap<String, std::time::Instant>>,
    /// 链接cooldown时长 (URL -> 秒数), 指数退避
    cooldown_secs: Arc<DashMap<String, u64>>,
    /// 单链接历史平均速度（URL -> 移动平均速度KB/s）
    /// 用于 timeout 计算，使用 EWMA（α=0.85）
    url_avg_speeds: Arc<DashMap<String, f64>>,
    /// 单链接采样计数（URL -> 采样次数）
    url_sample_counts: Arc<DashMap<String, u64>>,
    /// 🔥 新增：短期速度窗口（URL -> 最近 N 个分片速度的队列）
    /// 用于 score 判定，避免早期高速持续影响后期判定
    /// 注意：VecDeque 需要互斥访问，但每个 URL 的窗口独立
    url_recent_speeds: Arc<DashMap<String, StdMutex<VecDeque<f64>>>>,

    // 🔥 简单类型 → 原子操作
    /// 全局平均速度（KB/s），用于判断慢速（存储为 f64.to_bits()）
    global_avg_speed: Arc<AtomicU64>,
    /// 已完成的分片总数（用于计算平均速度）
    total_chunks: Arc<AtomicU64>,
}

impl UrlHealthManager {
    /// 创建新的 URL 健康管理器
    ///
    /// # 参数
    /// * `urls` - URL列表
    /// * `speeds` - 对应的探测速度列表（KB/s）
    pub fn new(urls: Vec<String>, speeds: Vec<f64>) -> Self {
        // 🔥 使用 DashMap 构建
        let weights = Arc::new(DashMap::new());
        let url_speeds = Arc::new(DashMap::new());
        let url_avg_speeds = Arc::new(DashMap::new());
        let url_sample_counts = Arc::new(DashMap::new());
        let url_scores = Arc::new(DashMap::new());
        let cooldown_secs = Arc::new(DashMap::new());
        let url_recent_speeds = Arc::new(DashMap::new());
        let mut total_speed = 0.0;

        for (url, speed) in urls.iter().zip(speeds.iter()) {
            weights.insert(url.clone(), 1); // 初始权重为1（可用）
            url_speeds.insert(url.clone(), *speed);
            // 初始化单链接平均速度为探测速度
            url_avg_speeds.insert(url.clone(), *speed);
            // 🔧 修复：sample_count 初始化为 0，探测不计入采样
            // 第一次 record_chunk_speed 时会设置为真实下载速度
            url_sample_counts.insert(url.clone(), 0);
            // 初始化score=50(中等)
            url_scores.insert(url.clone(), 50);
            // 初始化cooldown=10秒
            cooldown_secs.insert(url.clone(), 10);
            // 🔥 初始化短期速度窗口为空 StdMutex<VecDeque>
            url_recent_speeds.insert(url.clone(), StdMutex::new(VecDeque::new()));
            total_speed += speed;
        }

        // 计算初始平均速度
        let global_avg_speed = if !urls.is_empty() {
            total_speed / urls.len() as f64
        } else {
            0.0
        };

        Self {
            all_urls: urls,
            weights,
            url_speeds,
            url_scores,
            next_probe_time: Arc::new(DashMap::new()), // 初始化时不设置(只有禁用时才设置)
            cooldown_secs,
            global_avg_speed: Arc::new(AtomicU64::new(global_avg_speed.to_bits())),
            total_chunks: Arc::new(AtomicU64::new(0)),
            url_avg_speeds,
            url_sample_counts,
            url_recent_speeds,
        }
    }

    /// 获取可用的链接数量（权重>0的链接）
    pub fn available_count(&self) -> usize {
        self.weights.iter().filter(|entry| *entry.value() > 0).count()
    }

    /// 根据索引获取可用链接（跳过权重=0的链接）
    pub fn get_url(&self, index: usize) -> Option<&String> {
        let available: Vec<&String> = self.all_urls
            .iter()
            .filter(|url| {
                self.weights.get(*url).map(|w| *w > 0).unwrap_or(false)
            })
            .collect();

        if available.is_empty() {
            return None;
        }

        let url_index = index % available.len();
        available.get(url_index).copied()
    }

    /// 🔥 混合加权选择：权重 = 速度 × (score/100)
    ///
    /// 高速链接自动获得更多分片，性能提升 +10-33%（速度差异大时）
    ///
    /// # 参数
    /// * `chunk_index` - 分片索引，用于加权轮询
    ///
    /// # 返回
    /// 选中的 URL（克隆），如果无可用链接则返回 None
    pub fn get_url_hybrid(&self, chunk_index: usize) -> Option<String> {
        // 1. 获取所有可用链接及其综合权重
        let available: Vec<(String, f64)> = self.all_urls
            .iter()
            .filter_map(|url| {
                let weight = self.weights.get(url).map(|w| *w)?;
                if weight == 0 {
                    return None;
                }

                // 速度：优先使用 EWMA，兜底使用探测速度
                let speed = self.url_avg_speeds.get(url).map(|v| *v)
                    .or_else(|| self.url_speeds.get(url).map(|v| *v))
                    .unwrap_or(0.0);
                if speed <= 0.0 {
                    return None;
                }

                // 评分
                let score = self.url_scores.get(url).map(|s| *s).unwrap_or(50);

                // 综合权重 = 速度 × 评分因子
                // score=100 → 1.0, score=50 → 0.5, score=10 → 0.1
                let combined_weight = speed * (score as f64 / 100.0);

                Some((url.clone(), combined_weight))
            })
            .collect();

        if available.is_empty() {
            return None;
        }

        // 2. 加权轮询选择
        let total_weight: f64 = available.iter().map(|(_, w)| w).sum();
        if total_weight <= 0.0 {
            // 权重都是0，退回简单轮询
            return available.get(chunk_index % available.len()).map(|(url, _)| url.clone());
        }

        // 使用 chunk_index 计算在权重空间的位置
        let position = (chunk_index as f64 % total_weight).abs();

        let mut accumulated = 0.0;
        for (url, weight) in &available {
            accumulated += weight;
            if position < accumulated {
                return Some(url.clone());
            }
        }

        // 兜底：返回第一个
        available.first().map(|(url, _)| url.clone())
    }

    /// 🔧 Warm 模式：获取一个被禁用的链接用于低负载探测
    ///
    /// 当可用链接 < 5 时，返回一个被禁用的链接，给它分配少量流量（1个分片）
    /// 让链接在真实下载中自我恢复，无需额外探测
    ///
    /// # 返回
    /// - Some(url): 返回 score 最高的被禁用链接
    /// - None: 链接充足（>=5）或无被禁用链接
    pub fn get_warm_url(&self) -> Option<&String> {
        // 条件1：可用链接数是否不足5个
        if self.available_count() >= 5 {
            return None; // 链接充足，不需要 warm 链路
        }

        // 条件2：找到所有被禁用的链接，按 score 降序排列
        let mut disabled: Vec<(&String, i32)> = self.all_urls
            .iter()
            .filter(|url| {
                self.weights.get(*url).map(|w| *w == 0).unwrap_or(true)
            })
            .map(|url| {
                let score = self.url_scores.get(url).map(|s| *s).unwrap_or(0);
                (url, score)
            })
            .collect();

        if disabled.is_empty() {
            return None;
        }

        // 按 score 降序排序，优先选择恢复潜力大的链接
        disabled.sort_by(|a, b| b.1.cmp(&a.1));

        let (url, score) = disabled.first()?;
        debug!(
            "🌡️ Warm 模式：选择被禁用链接 {} (score={}) 进行低负载探测",
            url, score
        );

        Some(*url)
    }

    /// 记录分片下载速度，使用score评分机制判断是否需要降权
    ///
    /// 🔥 速度追踪双轨制：
    /// - 短期窗口 median（N=7）：用于 score 判定，避免早期高速影响
    /// - EWMA（α=0.85）：用于 timeout 计算和长期统计
    ///
    /// 使用**中位数阈值**替代平均值，避免极端值影响
    /// 使用**score累积评分**替代连续计数，提高稳定性
    ///
    /// # 参数
    /// * `url` - 下载链接
    /// * `chunk_size` - 分片大小（字节）
    /// * `duration_ms` - 下载耗时（毫秒）
    ///
    /// # 返回
    /// 本次下载速度（KB/s）
    pub fn record_chunk_speed(&self, url: &str, chunk_size: u64, duration_ms: u64) -> f64 {
        // 1. 计算本次速度（防止异常 duration_ms）
        let speed_kbps = if duration_ms > 0 && duration_ms < 1_000_000 {
            (chunk_size as f64) / (duration_ms as f64) * 1000.0 / 1024.0
        } else {
            // 🔧 修复数据混用：使用该链接的 EWMA，而非 global_avg_speed
            let url_string = url.to_string();
            self.url_avg_speeds.get(&url_string).map(|v| *v)
                .or_else(|| self.url_speeds.get(&url_string).map(|v| *v))
                .unwrap_or(500.0) // 极端情况兜底
        };

        let url_string = url.to_string();

        // 2. 🔥 先用旧窗口计算阈值（在加入新速度之前）
        // 阈值 = 该链接历史窗口median * 0.6
        // 这样可以判断"新速度是否相对历史表现异常"
        let slow_threshold_opt = self.calculate_window_median(&url_string).map(|window_median| {
            // 允许速度降低到窗口中位数的60%
            // 窗口median 10 MB/s → 阈值 6 MB/s
            // 窗口median 700 KB/s → 阈值 420 KB/s
            window_median * 0.6
        });

        // 3. 🔥 判断新速度是否异常（在加入窗口之前）
        // 只有在样本充足时才进行评分，避免前期误判
        if let Some(slow_threshold) = slow_threshold_opt {
            // 窗口样本充足，可以进行评分
            // 用新分片速度跟历史窗口阈值比较
            let mut current_score_ref = self.url_scores.entry(url_string.clone()).or_insert(50);
            let current_score = *current_score_ref;
            let new_score = if speed_kbps < slow_threshold {
                (current_score - 2).max(0) // 新速度慢于历史表现，扣分
            } else {
                (current_score + 3).min(100) // 新速度正常，加分
            };
            *current_score_ref = new_score;
            drop(current_score_ref); // 释放锁

            // 4. 根据score调整权重
            if new_score <= 10 {
                // score太低，降权
                let available = self.available_count();
                if let Some(mut weight) = self.weights.get_mut(&url_string) {
                    if *weight > 0 && available > MIN_AVAILABLE_LINKS {
                        *weight = 0;
                        drop(weight); // 释放锁

                        // 设置下次探测时间 (当前时间 + cooldown)
                        let cooldown = self.cooldown_secs.get(&url_string).map(|v| *v).unwrap_or(10);
                        let next_time = std::time::Instant::now() + std::time::Duration::from_secs(cooldown);
                        self.next_probe_time.insert(url_string.clone(), next_time);

                        warn!(
                            "🚫 链接降权: {} (score={}, 新速度 {:.2} KB/s < 阈值 {:.2} KB/s, 下次探测: {}秒后)",
                            url, new_score, speed_kbps, slow_threshold, cooldown
                        );
                    }
                }
            } else if new_score >= 30 {
                // score恢复，启用
                if let Some(mut weight) = self.weights.get_mut(&url_string) {
                    if *weight == 0 {
                        *weight = 1;
                        info!("✅ 链接恢复: {} (score={})", url, new_score);
                    }
                }
            }
        } else {
            // 窗口样本不足，跳过评分（前期保护）
            debug!(
                "⏸️ 链接 {} 窗口样本不足，跳过评分（速度 {:.2} KB/s）",
                url, speed_kbps
            );
        }

        // 5. 🔥 更新短期速度窗口（在判断之后加入新速度）
        {
            // 确保窗口存在
            if !self.url_recent_speeds.contains_key(&url_string) {
                self.url_recent_speeds.insert(url_string.clone(), StdMutex::new(VecDeque::new()));
            }

            // 获取窗口引用并更新
            if let Some(window_entry) = self.url_recent_speeds.get(&url_string) {
                if let Ok(mut window) = window_entry.value().try_lock() {
                    window.push_back(speed_kbps);

                    // 保持窗口大小为 SPEED_WINDOW_SIZE
                    if window.len() > SPEED_WINDOW_SIZE {
                        window.pop_front();
                    }
                }
            }
        }

        // 6. 更新单链接 EWMA 速度（用于 timeout 计算，α=0.85）
        {
            let mut sample_count_ref = self.url_sample_counts.entry(url_string.clone()).or_insert(0);
            *sample_count_ref += 1;
            let sample_count = *sample_count_ref;
            drop(sample_count_ref);

            let mut avg_ref = self.url_avg_speeds.entry(url_string.clone()).or_insert(speed_kbps);
            if sample_count == 1 {
                *avg_ref = speed_kbps;
            } else {
                // 🔧 α=0.85，平衡响应速度和抗干扰能力
                *avg_ref = *avg_ref * 0.85 + speed_kbps * 0.15;
            }
        }

        // 7. 更新全局平均速度（仅用于兜底，不参与阈值计算）
        let total = self.total_chunks.fetch_add(1, Ordering::SeqCst) + 1;
        let current_global_avg = f64::from_bits(self.global_avg_speed.load(Ordering::SeqCst));
        let new_global_avg = if total == 1 {
            speed_kbps
        } else {
            current_global_avg * 0.9 + speed_kbps * 0.1
        };
        self.global_avg_speed.store(new_global_avg.to_bits(), Ordering::SeqCst);

        speed_kbps
    }

    /// 🔥 计算单个 URL 的短期窗口 median
    ///
    /// 用于 score 判定，避免早期高速持续影响后期判定
    ///
    /// # 参数
    /// * `url` - URL 字符串
    ///
    /// # 返回
    /// - Some(median): 窗口样本充足（>= MIN_WINDOW_SAMPLES），返回中位数
    /// - None: 窗口样本不足，不应参与评分
    fn calculate_window_median(&self, url: &str) -> Option<f64> {
        let window_entry = self.url_recent_speeds.get(url)?;

        // 获取 Mutex 锁
        let window = window_entry.value().try_lock().ok()?;

        // 🔧 关键修复：窗口样本不足时返回 None，避免前期误判
        if window.len() < MIN_WINDOW_SAMPLES {
            return None;
        }

        let mut speeds: Vec<f64> = window.iter().copied().collect();
        speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mid = speeds.len() / 2;
        let median = if speeds.len() % 2 == 0 {
            (speeds[mid - 1] + speeds[mid]) / 2.0
        } else {
            speeds[mid]
        };

        Some(median)
    }

    /// 🔥 计算慢速阈值（基于所有 URL 的短期窗口 median）
    ///
    /// 使用双层中位数：
    /// 1. 计算每个 URL 的短期窗口 median（只包括样本充足的链接）
    /// 2. 再计算所有 URL 的 median
    /// 3. 阈值 = 全局 median * 0.6
    ///
    /// 无需 clamp，中位数本身就抗干扰，阈值会自适应网络环境
    ///
    /// # 返回
    /// - Some(threshold): 有足够的样本可以计算阈值
    /// - None: 样本不足，不应进行评分（前期保护）
    fn calculate_slow_threshold(&self) -> Option<f64> {
        // 计算所有链接的短期窗口 median（只包括样本充足的）
        let medians: Vec<f64> = self.all_urls
            .iter()
            .filter_map(|url| self.calculate_window_median(url))
            .collect();

        // 🔧 关键：如果样本充足的链接少于 3 个，不进行评分（前期保护）
        if medians.len() < 3 {
            return None;
        }

        // 对所有链接的窗口 median 再求中位数
        let mut sorted_medians = medians;
        sorted_medians.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mid = sorted_medians.len() / 2;
        let global_median = if sorted_medians.len() % 2 == 0 {
            (sorted_medians[mid - 1] + sorted_medians[mid]) / 2.0
        } else {
            sorted_medians[mid]
        };

        // ✅ 直接返回中位数 * 0.6，不做 clamp
        // 自适应各种网络环境：千兆宽带和低速网络都能正确工作
        Some(global_median * 0.6)
    }

    /// 尝试恢复被淘汰的链接 (逐个探测模型)
    ///
    /// 只在以下条件满足时才尝试恢复:
    /// 1. 可用链接数 < 5
    /// 2. 存在已禁用且探测时间已到期的链接
    ///
    /// # 返回
    /// 需要探测的URL (只返回一个最早到期的!)
    pub fn try_restore_links(&self) -> Option<String> {
        // 条件1: 可用链接数是否不足5个
        let available = self.available_count();
        if available >= 5 {
            return None;
        }

        // 条件2: 找到所有已禁用且到期的链接
        let now = std::time::Instant::now();
        let mut candidates: Vec<(String, std::time::Instant)> = Vec::new();

        for url in &self.all_urls {
            let weight = self.weights.get(url).map(|w| *w).unwrap_or(0);
            if weight == 0 {
                if let Some(probe_time_ref) = self.next_probe_time.get(url) {
                    let probe_time = *probe_time_ref;
                    if now >= probe_time {
                        candidates.push((url.clone(), probe_time));
                    }
                }
            }
        }

        if candidates.is_empty() {
            return None;
        }

        // 按 next_probe_time 排序,选择最早到期的那个
        candidates.sort_by(|a, b| a.1.cmp(&b.1));

        let url_to_restore = candidates[0].0.clone();
        info!(
            "🔄 可用链接不足({}<5),准备探测最早到期的链接: {}",
            available, url_to_restore
        );

        Some(url_to_restore)
    }

    /// 重置所有链接的短期速度窗口（任务数变化时调用）
    ///
    /// 当全局并发任务数增加时，带宽会被重新分配，导致单链接速度下降
    /// 此时应清空旧窗口数据，重新进入前期保护期（MIN_WINDOW_SAMPLES），避免误判降权
    ///
    /// 调用时机：ChunkScheduler 检测到活跃任务数增加
    pub fn reset_speed_windows(&self) {
        for entry in self.url_recent_speeds.iter() {
            if let Ok(mut window) = entry.value().try_lock() {
                window.clear();
            }
        }
        info!("🔄 已重置所有链接的速度窗口（任务数变化，带宽重新分配）");
    }

    /// 处理探测失败 (指数退避)
    ///
    /// 当探测失败时,使用指数退避策略增加cooldown时间
    /// cooldown: 10s -> 20s -> 40s (最大)
    pub fn handle_probe_failure(&self, url: &str) {
        let url_string = url.to_string();

        // 获取当前cooldown
        let current_cooldown = self.cooldown_secs.get(&url_string).map(|v| *v).unwrap_or(10);

        // 指数退避: cooldown * 2, 最大40秒
        let new_cooldown = (current_cooldown * 2).min(40);
        self.cooldown_secs.insert(url_string.clone(), new_cooldown);

        // 设置下次探测时间
        let next_time = std::time::Instant::now() + std::time::Duration::from_secs(new_cooldown);
        self.next_probe_time.insert(url_string.clone(), next_time);

        warn!(
            "⚠️ 链接探测失败: {}, cooldown: {}s -> {}s, 下次探测: {}秒后",
            url, current_cooldown, new_cooldown, new_cooldown
        );
    }

    /// 恢复链接权重（探测成功后调用）
    ///
    /// 恢复链接时重置所有相关状态
    pub fn restore_link(&self, url: &str, new_speed: f64) {
        let url_string = url.to_string();

        // 恢复权重
        if let Some(mut weight) = self.weights.get_mut(&url_string) {
            *weight = 1;
        }

        // 重置score为50(中等)
        self.url_scores.insert(url_string.clone(), 50);

        // 重置cooldown为10秒
        self.cooldown_secs.insert(url_string.clone(), 10);

        // 移除next_probe_time
        self.next_probe_time.remove(&url_string);

        // 更新速度
        self.url_speeds.insert(url_string.clone(), new_speed);
        self.url_avg_speeds.insert(url_string.clone(), new_speed);
        self.url_sample_counts.insert(url_string.clone(), 1);

        // 🔥 清空短期速度窗口，让链接重新积累数据
        self.url_recent_speeds.insert(url_string.clone(), StdMutex::new(VecDeque::new()));

        info!(
            "✅ 链接恢复: {} (新速度 {:.2} KB/s, score=50, 当前可用 {} 个链接)",
            url, new_speed, self.available_count()
        );
    }

    /// 根据URL和分片大小计算动态超时时间（秒）
    ///
    /// 🔧 修复：基于**实时EWMA速度**而非探测速度，更准确反映当前网络状况
    /// 公式：timeout = (chunk_size_kb / ewma_speed) × safety_factor
    ///
    /// # 参数
    /// * `url` - 下载链接
    /// * `chunk_size` - 分片大小（字节）
    ///
    /// # 返回
    /// 超时时间（秒），范围在 [30, 180] 之间
    pub fn calculate_timeout(&self, url: &str, chunk_size: u64) -> u64 {
        const SAFETY_FACTOR: f64 = 3.0; // 🔧 提高到3倍，减少超时噪声
        const MIN_TIMEOUT: u64 = 30;    // 🔧 提高最小值到30秒
        const MAX_TIMEOUT: u64 = 180;   // 最大3分钟

        // 🔧 优先使用 EWMA 速度，兜底使用探测速度
        let speed_kbps = self.url_avg_speeds.get(url).map(|v| *v)
            .or_else(|| self.url_speeds.get(url).map(|v| *v))
            .unwrap_or(500.0); // 保守兜底值

        if speed_kbps > 0.0 {
            // 转换分片大小为KB
            let chunk_size_kb = chunk_size as f64 / 1024.0;

            // 计算理论时间（秒）
            let theoretical_time = chunk_size_kb / speed_kbps;

            // 应用安全系数
            let timeout = (theoretical_time * SAFETY_FACTOR) as u64;

            // 限制在合理范围内
            return timeout.clamp(MIN_TIMEOUT, MAX_TIMEOUT);
        }

        // 如果速度<=0，使用默认超时
        60
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
    /// 文件系统操作锁（保护目录创建，防止删除-创建竞态）
    fs_lock: Arc<Mutex<()>>,
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
            fs_lock: Arc::new(Mutex::new(())),
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
            .pool_max_idle_per_host(200) // 增大连接池：100 -> 200
            .pool_idle_timeout(std::time::Duration::from_secs(90)) // IdleConnTimeout: 90s
            .tcp_keepalive(std::time::Duration::from_secs(60)) // TCP Keep-Alive
            .tcp_nodelay(true) // 启用 TCP_NODELAY，减少延迟
            .redirect(reqwest::redirect::Policy::limited(10)) // 最多 10 次重定向
            // HTTP/2 极致优化：大幅增加窗口以消除慢启动影响
            .http2_adaptive_window(true) // 启用HTTP/2自适应窗口
            .http2_initial_stream_window_size(Some(1024 * 1024 * 2)) // 2MB初始流窗口（默认65KB）
            .http2_initial_connection_window_size(Some(1024 * 1024 * 4)) // 4MB初始连接窗口（默认65KB）
            .http2_keep_alive_interval(Some(std::time::Duration::from_secs(10))) // HTTP/2 keep-alive
            .http2_keep_alive_timeout(std::time::Duration::from_secs(20)) // HTTP/2 keep-alive超时
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
        expected_secs.clamp(MIN_TIMEOUT, MAX_TIMEOUT)
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
        cancellation_token: CancellationToken,
    ) -> Result<(
        Client,                           // HTTP 客户端
        String,                            // Cookie
        Option<String>,                    // Referer 头
        Arc<Mutex<UrlHealthManager>>,      // URL 健康管理器
        PathBuf,                           // 本地路径
        u64,                               // 分片大小
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
        let mut url_speeds = Vec::new(); // 记录每个链接的速度
        let mut referer: Option<String> = None;

        for (i, url) in all_urls.iter().enumerate() {
            match self
                .probe_download_link_with_client(&download_client, url, total_size)
                .await
            {
                Ok((ref_url, speed)) => {
                    info!("✓ 链接 #{} 探测成功，速度: {:.2} KB/s", i, speed);
                    valid_urls.push(url.clone());
                    url_speeds.push(speed);

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

        // 🔥 淘汰慢速链接（使用中位数替代平均值）
        if url_speeds.len() > 1 {
            // 计算中位数速度
            let mut sorted_speeds = url_speeds.clone();
            sorted_speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mid = sorted_speeds.len() / 2;
            let median_speed = if sorted_speeds.len() % 2 == 0 {
                (sorted_speeds[mid - 1] + sorted_speeds[mid]) / 2.0
            } else {
                sorted_speeds[mid]
            };
            let threshold = median_speed * 0.6; // 使用中位数 * 0.6

            info!(
                "链接速度分析: 中位数 {:.2} KB/s, 淘汰阈值 {:.2} KB/s (中位数 * 0.6)",
                median_speed, threshold
            );

            let mut filtered_urls = Vec::new();
            let mut filtered_speeds = Vec::new();
            for (idx, (url, speed)) in valid_urls.iter().zip(url_speeds.iter()).enumerate() {
                if *speed >= threshold {
                    filtered_urls.push(url.clone());
                    filtered_speeds.push(*speed);
                    info!("✓ 保留链接 #{}: {:.2} KB/s", idx, speed);
                } else {
                    warn!("✗ 淘汰慢速链接 #{}: {:.2} KB/s (低于阈值 {:.2} KB/s)",
                          idx, speed, threshold);
                }
            }

            if filtered_urls.is_empty() {
                warn!("所有链接都被淘汰，保留速度最快的链接");
                if let Some((idx, _)) = url_speeds.iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()) {
                    filtered_urls.push(valid_urls[idx].clone());
                    filtered_speeds.push(url_speeds[idx]);
                }
            }

            info!(
                "链接过滤完成: 保留 {}/{} 个高速链接",
                filtered_urls.len(),
                valid_urls.len()
            );

            valid_urls = filtered_urls;
            url_speeds = filtered_speeds;
        }

        // 5. 创建 URL 健康管理器（传递speeds）
        let url_health = Arc::new(Mutex::new(UrlHealthManager::new(valid_urls, url_speeds)));

        // 6. 创建本地文件（内部会加锁检查取消状态）
        self.prepare_file(&local_path, total_size, &cancellation_token)
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

        // 10. 生成 Cookie
        let cookie = format!("BDUSS={}", self.netdisk_client.bduss());

        info!("任务准备完成，等待调度器调度");

        Ok((
            download_client,
            cookie,
            referer,
            url_health,
            local_path,
            chunk_size,
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
        let mut url_speeds = Vec::new();
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
                Ok((ref_url, speed)) => {
                    info!("✓ 链接 #{} 探测成功，速度: {:.2} KB/s", i, speed);
                    valid_urls.push(url.clone());
                    url_speeds.push(speed);

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

        // 🔥 淘汰慢速链接（使用中位数替代平均值）
        if url_speeds.len() > 1 {
            // 计算中位数速度
            let mut sorted_speeds = url_speeds.clone();
            sorted_speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mid = sorted_speeds.len() / 2;
            let median_speed = if sorted_speeds.len() % 2 == 0 {
                (sorted_speeds[mid - 1] + sorted_speeds[mid]) / 2.0
            } else {
                sorted_speeds[mid]
            };
            let threshold = median_speed * 0.6; // 使用中位数 * 0.6

            info!(
                "链接速度分析: 中位数 {:.2} KB/s, 淘汰阈值 {:.2} KB/s (中位数 * 0.6)",
                median_speed, threshold
            );

            let mut filtered_urls = Vec::new();
            let mut filtered_speeds = Vec::new();
            for (idx, (url, speed)) in valid_urls.iter().zip(url_speeds.iter()).enumerate() {
                if *speed >= threshold {
                    filtered_urls.push(url.clone());
                    filtered_speeds.push(*speed);
                    info!("✓ 保留链接 #{}: {:.2} KB/s", idx, speed);
                } else {
                    warn!("✗ 淘汰慢速链接 #{}: {:.2} KB/s (低于阈值 {:.2} KB/s)",
                          idx, speed, threshold);
                }
            }

            if filtered_urls.is_empty() {
                warn!("所有链接都被淘汰，保留速度最快的链接");
                if let Some((idx, _)) = url_speeds.iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()) {
                    filtered_urls.push(valid_urls[idx].clone());
                    filtered_speeds.push(url_speeds[idx]);
                }
            }

            info!(
                "链接过滤完成: 保留 {}/{} 个高速链接",
                filtered_urls.len(),
                valid_urls.len()
            );

            valid_urls = filtered_urls;
            url_speeds = filtered_speeds;
        }

        // 3. 创建 URL 健康管理器（传递speeds）
        let url_health = Arc::new(Mutex::new(UrlHealthManager::new(valid_urls, url_speeds)));

        // 4. 创建本地文件（内部会加锁检查取消状态）
        self.prepare_file(local_path, total_size, &cancellation_token)
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

        // 8. 启动链接健康检查循环（用于恢复被降权的链接）
        {
            let url_health_clone = url_health.clone();
            let download_client_clone = download_client.clone();
            let bduss = self.netdisk_client.bduss().to_string();
            let cookie = format!("BDUSS={}", bduss);
            let cancellation_token_clone = cancellation_token.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

                loop {
                    // 检查是否已取消
                    if cancellation_token_clone.is_cancelled() {
                        debug!("健康检查循环已停止（任务已取消）");
                        break;
                    }

                    interval.tick().await;

                    // 检查是否需要探测恢复链接
                    let url_to_restore = {
                        let health = url_health_clone.lock().await;
                        health.try_restore_links()
                    };

                    if let Some(url) = url_to_restore {
                        // 异步探测该链接（不阻塞健康检查循环）
                        let health_clone = url_health_clone.clone();
                        let client_clone = download_client_clone.clone();
                        let cookie_clone = cookie.clone();

                        tokio::spawn(async move {
                            debug!("🔄 开始异步探测恢复链接: {}", url);

                            // 执行探测
                            match DownloadEngine::probe_for_restore(
                                &client_clone,
                                &cookie_clone,
                                &url,
                                total_size,
                            ).await {
                                Ok(speed) => {
                                    let health = health_clone.lock().await;
                                    let threshold_opt = health.calculate_slow_threshold();

                                    // 如果有阈值，检查速度；否则直接恢复（说明还在前期）
                                    if let Some(threshold) = threshold_opt {
                                        if speed >= threshold {
                                            // 速度合格，恢复链接
                                            health.restore_link(&url, speed);
                                        } else {
                                            debug!(
                                                "🚫 探测速度不合格: {} ({:.2} KB/s < 阈值 {:.2} KB/s)",
                                                url, speed, threshold
                                            );
                                            health.handle_probe_failure(&url);
                                        }
                                    } else {
                                        // 前期没有阈值，直接恢复
                                        debug!("⏸️ 前期阶段，直接恢复链接: {}", url);
                                        health.restore_link(&url, speed);
                                    }
                                }
                                Err(e) => {
                                    let health = health_clone.lock().await;
                                    health.handle_probe_failure(&url);
                                    debug!("⚠️ 探测失败: {} - {:?}", url, e);
                                }
                            }
                        });
                    }
                }

                info!("健康检查循环已结束");
            });
        }

        // 9. 并发下载分片（使用全局 Semaphore 和复用的 download_client，使用 URL 健康管理器）
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
    /// 5. 测量链接速度（用于淘汰慢速链接）
    ///
    /// # 参数
    /// * `client` - 复用的 HTTP 客户端（确保与后续分片下载使用同一个 client）
    /// * `url` - 下载链接
    /// * `expected_size` - 预期文件大小
    ///
    /// # 返回值
    /// 返回 (Referer, 下载速度KB/s)：
    /// - Referer: 如果有重定向返回原始URL，否则返回None
    /// - 速度: 探测阶段的下载速度（KB/s），用于评估链接质量
    async fn probe_download_link_with_client(
        &self,
        client: &Client,
        url: &str,
        expected_size: u64,
    ) -> Result<(Option<String>, f64)> {
        const PROBE_SIZE: u64 = 256 * 1024; // 256KB (增大测试块以获得更准确的速度测量)

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

        // 记录开始时间
        let start_time = std::time::Instant::now();

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

        // 计算下载速度
        let elapsed = start_time.elapsed().as_secs_f64();
        let speed_kbps = if elapsed > 0.0 {
            (probe_data.len() as f64) / 1024.0 / elapsed
        } else {
            0.0
        };

        info!(
            "✅ 探测成功: 收到 {} bytes 数据，耗时 {:.2}s，速度 {:.2} KB/s",
            probe_data.len(),
            elapsed,
            speed_kbps
        );

        Ok((referer, speed_kbps))
    }

    /// 用于恢复链接的简化探测函数（静态方法）
    ///
    /// 与 probe_download_link_with_client 类似，但不需要 self，只返回速度
    /// 用于健康检查循环
    async fn probe_for_restore(
        client: &Client,
        cookie: &str,
        url: &str,
        expected_size: u64,
    ) -> Result<f64> {
        const PROBE_SIZE: u64 = 256 * 1024; // 256KB

        let probe_end = if expected_size > 0 {
            (PROBE_SIZE - 1).min(expected_size - 1)
        } else {
            PROBE_SIZE - 1
        };

        debug!("🔍 恢复探测链接: Range 0-{} ({} bytes)", probe_end, probe_end + 1);

        let start_time = std::time::Instant::now();

        let response = client
            .get(url)
            .header("Cookie", cookie)
            .header("Range", format!("bytes=0-{}", probe_end))
            .timeout(std::time::Duration::from_secs(30)) // 探测超时30秒
            .send()
            .await
            .context("恢复探测请求失败")?;

        let status = response.status();
        if status != reqwest::StatusCode::PARTIAL_CONTENT && status != reqwest::StatusCode::OK {
            anyhow::bail!("恢复探测失败: 状态码 {}", status);
        }

        // 读取探测数据
        let probe_data = response.bytes().await.context("读取恢复探测数据失败")?;

        // 计算下载速度
        let elapsed = start_time.elapsed().as_secs_f64();
        let speed_kbps = if elapsed > 0.0 {
            (probe_data.len() as f64) / 1024.0 / elapsed
        } else {
            0.0
        };

        debug!(
            "✅ 恢复探测成功: 收到 {} bytes，耗时 {:.2}s，速度 {:.2} KB/s",
            probe_data.len(), elapsed, speed_kbps
        );

        Ok(speed_kbps)
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
    ///
    /// # 参数
    /// * `path` - 文件路径
    /// * `size` - 文件大小
    /// * `cancellation_token` - 取消令牌
    ///
    /// # 并发安全
    /// 使用 fs_lock 保护"检查取消状态+创建父目录"的原子操作，防止：
    /// 1. 删除文件夹与创建目录的竞态条件
    /// 2. 多个任务重复创建同一目录
    async fn prepare_file(&self, path: &Path, size: u64, cancellation_token: &CancellationToken) -> Result<()> {
        // 🔒 加锁保护：检查取消状态 + 创建父目录
        {
            let _guard = self.fs_lock.lock().await;

            // 检查是否被取消
            if cancellation_token.is_cancelled() {
                debug!("准备文件时发现任务已取消: {:?}", path);
                anyhow::bail!("任务已被取消");
            }

            // 创建父目录
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context("创建父目录失败")?;
            }

            // 锁在此处自动释放
        }

        // 创建文件并预分配空间（不需要锁，因为文件路径唯一）
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
        total_size: u64,
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
                    total_size,
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
    /// * `chunk_size` - 分片大小（用于动态计算超时）
    /// * `total_size` - 文件总大小（用于探测恢复链接）
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
        chunk_size: u64,
        _total_size: u64, // 保留用于未来的健康检查循环
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
            let (available_count, current_url, timeout_secs) = {
                let health = url_health.lock().await;
                let count = health.available_count();
                if count == 0 {
                    anyhow::bail!("所有下载链接都不可用");
                }

                // 🔧 Warm 模式集成：
                // 当可用链接<5时，每10个分片给warm链接分配1个
                // 这样warm链接可以在真实下载中自我恢复
                let use_warm = count < 5 && chunk_index % 10 == 0;

                let url = if use_warm {
                    // 尝试获取 warm 链接
                    if let Some(warm_url) = health.get_warm_url() {
                        info!(
                            "[分片线程{}] 🌡️ Warm模式：分片 #{} 使用被禁用链接进行低负载探测",
                            chunk_thread_id, chunk_index
                        );
                        warm_url.clone()
                    } else {
                        // 没有 warm 链接，使用加权选择
                        health.get_url_hybrid(chunk_index)
                            .or_else(|| {
                                let url_index = chunk_index % count;
                                health.get_url(url_index).map(|s| s.clone())
                            })
                            .ok_or_else(|| anyhow::anyhow!("无法获取 URL"))?
                    }
                } else {
                    // 🔥 动态加权 URL 选择策略：
                    // 1. 首次尝试：使用 get_url_hybrid() 加权选择（高速链接获得更多分片）
                    // 2. 重试时：尝试下一个未尝试过的链接
                    if retries == 0 {
                        // 🔥 使用加权选择，兜底使用简单轮询
                        health.get_url_hybrid(chunk_index)
                            .or_else(|| {
                                let url_index = chunk_index % count;
                                health.get_url(url_index).map(|s| s.clone())
                            })
                            .ok_or_else(|| anyhow::anyhow!("无法获取 URL"))?
                    } else {
                        // 重试时，找到一个还没尝试过的链接
                        let mut found_url: Option<String> = None;
                        for i in 0..count {
                            let index = (chunk_index + i) % count;
                            if let Some(url) = health.get_url(index) {
                                if !tried_urls.contains(url.as_str()) {
                                    found_url = Some(url.clone());
                                    break;
                                }
                            }
                        }
                        found_url.ok_or_else(|| anyhow::anyhow!("无法获取 URL"))?
                    }
                };

                // 🔥 动态计算超时时间（基于 EWMA 速度和分片大小）
                let timeout = health.calculate_timeout(&url, chunk_size);

                (count, url, timeout)
            };

            // 记录该链接已尝试
            tried_urls.insert(current_url.clone());

            debug!(
                "[分片线程{}] 分片 #{} 使用链接: {} (可用链接数: {}, 重试次数: {}, 超时: {}s)",
                chunk_thread_id,
                chunk_index,
                current_url,
                available_count,
                retries,
                timeout_secs
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

            // 记录下载开始时间（用于计算速度）
            let download_start = std::time::Instant::now();

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
                Ok(bytes_downloaded) => {
                    // ✅ 下载成功

                    // 计算下载耗时
                    let duration_ms = download_start.elapsed().as_millis() as u64;

                    // 记录分片速度（动态权重调整,使用score机制）
                    {
                        let health = url_health.lock().await;

                        // 记录分片速度，可能触发链接降权或恢复
                        let speed = health.record_chunk_speed(&current_url, bytes_downloaded, duration_ms);
                        debug!(
                            "[分片线程{}] 分片 #{} 速度: {:.2} KB/s (耗时 {}ms)",
                            chunk_thread_id, chunk_index, speed, duration_ms
                        );
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
                    // 新设计中,失败会通过score机制自动处理
                    // 这里只记录错误并切换链接重试

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

                    // 🔥 使用指数退避延迟重试（100ms → 200ms → 400ms → ...）
                    let backoff_ms = calculate_backoff_delay(retries);
                    debug!(
                        "[分片线程{}] ⏳ 分片 #{} 等待 {}ms 后重试",
                        chunk_thread_id, chunk_index, backoff_ms
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
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
