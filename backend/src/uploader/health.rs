// PCS 服务器健康管理器
//
// 复用下载模块的 UrlHealthManager 设计，使用 DashMap + AtomicU64 消除 Mutex 瓶颈
//
// 功能：
// - 追踪 PCS 上传服务器的可用性
// - 动态权重调整（基于速度和评分）
// - 混合加权选择服务器（高速服务器获得更多分片）
// - 指数退避恢复（失败服务器逐步恢复）

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tracing::{debug, info, warn};

/// 最少保留服务器数
const MIN_AVAILABLE_SERVERS: usize = 2;

/// 短期速度窗口大小（用于 score 判定）
const SPEED_WINDOW_SIZE: usize = 7;

/// 窗口最小样本数（开始评分的阈值）
const MIN_WINDOW_SAMPLES: usize = 5;

/// PCS 服务器健康管理器
///
/// 用于追踪上传服务器的可用性，支持动态权重调整
/// - 权重 > 0：服务器可用
/// - 权重 = 0：服务器被淘汰（因慢速或失败）
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
/// 并发优化：使用 DashMap + AtomicU64，消除 Mutex 瓶颈
#[derive(Debug, Clone)]
pub struct PcsServerHealthManager {
    /// 所有服务器列表（包括已淘汰的）- 不可变，无需同步
    all_servers: Vec<String>,

    // DashMap 实现无锁并发
    /// 服务器权重（URL -> 权重，>0可用，=0不可用）
    weights: Arc<DashMap<String, u32>>,
    /// 服务器速度映射（URL -> 探测速度KB/s）
    server_speeds: Arc<DashMap<String, f64>>,
    /// 服务器评分 (0-100), 低于10降权, 高于30恢复
    server_scores: Arc<DashMap<String, i32>>,
    /// 服务器下次探测时间 (URL -> Instant)
    next_probe_time: Arc<DashMap<String, std::time::Instant>>,
    /// 服务器cooldown时长 (URL -> 秒数), 指数退避
    cooldown_secs: Arc<DashMap<String, u64>>,
    /// 单服务器历史平均速度（URL -> 移动平均速度KB/s）
    server_avg_speeds: Arc<DashMap<String, f64>>,
    /// 单服务器采样计数（URL -> 采样次数）
    server_sample_counts: Arc<DashMap<String, u64>>,
    /// 短期速度窗口（URL -> 最近 N 个分片速度的队列）
    server_recent_speeds: Arc<DashMap<String, StdMutex<VecDeque<f64>>>>,

    // 原子类型
    /// 全局平均速度（KB/s），用于判断慢速（存储为 f64.to_bits()）
    global_avg_speed: Arc<AtomicU64>,
    /// 已完成的分片总数（用于计算平均速度）
    total_chunks: Arc<AtomicU64>,
}

impl PcsServerHealthManager {
    /// 创建新的服务器健康管理器
    ///
    /// # 参数
    /// * `servers` - 服务器列表（PCS 服务器 URL）
    /// * `speeds` - 对应的初始速度列表（KB/s），可以为空
    pub fn new(servers: Vec<String>, speeds: Vec<f64>) -> Self {
        let weights = Arc::new(DashMap::new());
        let server_speeds = Arc::new(DashMap::new());
        let server_avg_speeds = Arc::new(DashMap::new());
        let server_sample_counts = Arc::new(DashMap::new());
        let server_scores = Arc::new(DashMap::new());
        let cooldown_secs = Arc::new(DashMap::new());
        let server_recent_speeds = Arc::new(DashMap::new());
        let mut total_speed = 0.0;

        // 如果 speeds 为空或长度不匹配，使用默认速度
        let default_speed = 1000.0; // 默认 1000 KB/s

        for (i, server) in servers.iter().enumerate() {
            let speed = speeds.get(i).copied().unwrap_or(default_speed);

            weights.insert(server.clone(), 1); // 初始权重为1（可用）
            server_speeds.insert(server.clone(), speed);
            server_avg_speeds.insert(server.clone(), speed);
            server_sample_counts.insert(server.clone(), 0);
            server_scores.insert(server.clone(), 50); // 初始 score=50
            cooldown_secs.insert(server.clone(), 10); // 初始 cooldown=10秒
            server_recent_speeds.insert(server.clone(), StdMutex::new(VecDeque::new()));
            total_speed += speed;
        }

        let global_avg_speed = if !servers.is_empty() {
            total_speed / servers.len() as f64
        } else {
            0.0
        };

        Self {
            all_servers: servers,
            weights,
            server_speeds,
            server_scores,
            next_probe_time: Arc::new(DashMap::new()),
            cooldown_secs,
            global_avg_speed: Arc::new(AtomicU64::new(global_avg_speed.to_bits())),
            total_chunks: Arc::new(AtomicU64::new(0)),
            server_avg_speeds,
            server_sample_counts,
            server_recent_speeds,
        }
    }

    /// 从服务器列表创建（无初始速度）
    pub fn from_servers(servers: Vec<String>) -> Self {
        Self::new(servers, vec![])
    }

    /// 更新服务器列表（动态获取服务器后调用）
    ///
    /// 保留已有服务器的状态，添加新服务器
    pub fn update_servers(&self, new_servers: Vec<String>) {
        for server in &new_servers {
            // 如果是新服务器，初始化其状态
            if !self.weights.contains_key(server) {
                self.weights.insert(server.clone(), 1);
                self.server_speeds.insert(server.clone(), 1000.0); // 默认 1000 KB/s
                self.server_avg_speeds.insert(server.clone(), 1000.0);
                self.server_sample_counts.insert(server.clone(), 0);
                self.server_scores.insert(server.clone(), 50);
                self.cooldown_secs.insert(server.clone(), 10);
                self.server_recent_speeds.insert(
                    server.clone(),
                    StdMutex::new(VecDeque::new()),
                );
                info!("添加新上传服务器: {}", server);
            }
        }
    }

    /// 获取可用的服务器数量（权重>0的服务器）
    pub fn available_count(&self) -> usize {
        self.weights.iter().filter(|entry| *entry.value() > 0).count()
    }

    /// 根据索引获取可用服务器（跳过权重=0的服务器）
    pub fn get_server(&self, index: usize) -> Option<&String> {
        let available: Vec<&String> = self.all_servers
            .iter()
            .filter(|server| {
                self.weights.get(*server).map(|w| *w > 0).unwrap_or(false)
            })
            .collect();

        if available.is_empty() {
            return None;
        }

        let server_index = index % available.len();
        available.get(server_index).copied()
    }

    /// 混合加权选择：权重 = 速度 × (score/100)
    ///
    /// 高速服务器自动获得更多分片，性能提升 +10-33%（速度差异大时）
    ///
    /// # 参数
    /// * `chunk_index` - 分片索引，用于加权轮询
    ///
    /// # 返回
    /// 选中的服务器 URL（克隆），如果无可用服务器则返回 None
    pub fn get_server_hybrid(&self, chunk_index: usize) -> Option<String> {
        // 1. 获取所有可用服务器及其综合权重
        let available: Vec<(String, f64)> = self.all_servers
            .iter()
            .filter_map(|server| {
                let weight = self.weights.get(server).map(|w| *w)?;
                if weight == 0 {
                    return None;
                }

                // 速度：优先使用 EWMA，兜底使用初始速度
                let speed = self.server_avg_speeds.get(server).map(|v| *v)
                    .or_else(|| self.server_speeds.get(server).map(|v| *v))
                    .unwrap_or(0.0);
                if speed <= 0.0 {
                    return None;
                }

                // 评分
                let score = self.server_scores.get(server).map(|s| *s).unwrap_or(50);

                // 综合权重 = 速度 × 评分因子
                let combined_weight = speed * (score as f64 / 100.0);

                Some((server.clone(), combined_weight))
            })
            .collect();

        if available.is_empty() {
            return None;
        }

        // 2. 加权轮询选择
        let total_weight: f64 = available.iter().map(|(_, w)| w).sum();
        if total_weight <= 0.0 {
            return available.get(chunk_index % available.len()).map(|(server, _)| server.clone());
        }

        let position = (chunk_index as f64 % total_weight).abs();

        let mut accumulated = 0.0;
        for (server, weight) in &available {
            accumulated += weight;
            if position < accumulated {
                return Some(server.clone());
            }
        }

        available.first().map(|(server, _)| server.clone())
    }

    /// 记录分片上传速度，使用 score 评分机制判断是否需要降权
    ///
    /// # 参数
    /// * `server` - 服务器 URL
    /// * `chunk_size` - 分片大小（字节）
    /// * `duration_ms` - 上传耗时（毫秒）
    ///
    /// # 返回
    /// 本次上传速度（KB/s）
    pub fn record_chunk_speed(&self, server: &str, chunk_size: u64, duration_ms: u64) -> f64 {
        // 1. 计算本次速度
        let speed_kbps = if duration_ms > 0 && duration_ms < 1_000_000 {
            (chunk_size as f64) / (duration_ms as f64) * 1000.0 / 1024.0
        } else {
            let server_string = server.to_string();
            self.server_avg_speeds.get(&server_string).map(|v| *v)
                .or_else(|| self.server_speeds.get(&server_string).map(|v| *v))
                .unwrap_or(500.0)
        };

        let server_string = server.to_string();

        // 2. 先用旧窗口计算阈值
        let slow_threshold_opt = self.calculate_window_median(&server_string).map(|window_median| {
            window_median * 0.6
        });

        // 3. 判断新速度是否异常
        if let Some(slow_threshold) = slow_threshold_opt {
            let mut current_score_ref = self.server_scores.entry(server_string.clone()).or_insert(50);
            let current_score = *current_score_ref;
            let new_score = if speed_kbps < slow_threshold {
                (current_score - 2).max(0)
            } else {
                (current_score + 3).min(100)
            };
            *current_score_ref = new_score;
            drop(current_score_ref);

            // 4. 根据 score 调整权重
            if new_score <= 10 {
                let available = self.available_count();
                if let Some(mut weight) = self.weights.get_mut(&server_string) {
                    if *weight > 0 && available > MIN_AVAILABLE_SERVERS {
                        *weight = 0;
                        drop(weight);

                        let cooldown = self.cooldown_secs.get(&server_string).map(|v| *v).unwrap_or(10);
                        let next_time = std::time::Instant::now() + std::time::Duration::from_secs(cooldown);
                        self.next_probe_time.insert(server_string.clone(), next_time);

                        warn!(
                            "服务器降权: {} (score={}, 速度 {:.2} KB/s < 阈值 {:.2} KB/s, 下次探测: {}秒后)",
                            server, new_score, speed_kbps, slow_threshold, cooldown
                        );
                    }
                }
            } else if new_score >= 30 {
                if let Some(mut weight) = self.weights.get_mut(&server_string) {
                    if *weight == 0 {
                        *weight = 1;
                        info!("服务器恢复: {} (score={})", server, new_score);
                    }
                }
            }
        } else {
            debug!(
                "服务器 {} 窗口样本不足，跳过评分（速度 {:.2} KB/s）",
                server, speed_kbps
            );
        }

        // 5. 更新短期速度窗口
        {
            if !self.server_recent_speeds.contains_key(&server_string) {
                self.server_recent_speeds.insert(server_string.clone(), StdMutex::new(VecDeque::new()));
            }

            if let Some(window_entry) = self.server_recent_speeds.get(&server_string) {
                if let Ok(mut window) = window_entry.value().try_lock() {
                    window.push_back(speed_kbps);
                    if window.len() > SPEED_WINDOW_SIZE {
                        window.pop_front();
                    }
                }
            }
        }

        // 6. 更新 EWMA 速度
        {
            let mut sample_count_ref = self.server_sample_counts.entry(server_string.clone()).or_insert(0);
            *sample_count_ref += 1;
            let sample_count = *sample_count_ref;
            drop(sample_count_ref);

            let mut avg_ref = self.server_avg_speeds.entry(server_string.clone()).or_insert(speed_kbps);
            if sample_count == 1 {
                *avg_ref = speed_kbps;
            } else {
                *avg_ref = *avg_ref * 0.85 + speed_kbps * 0.15;
            }
        }

        // 7. 更新全局平均速度
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

    /// 计算单个服务器的短期窗口 median
    fn calculate_window_median(&self, server: &str) -> Option<f64> {
        let window_entry = self.server_recent_speeds.get(server)?;
        let window = window_entry.value().try_lock().ok()?;

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

    /// 尝试恢复被淘汰的服务器
    ///
    /// 只在以下条件满足时才尝试恢复:
    /// 1. 可用服务器数 < 5
    /// 2. 存在已禁用且探测时间已到期的服务器
    ///
    /// # 返回
    /// 需要探测的服务器 URL (只返回一个最早到期的!)
    pub fn try_restore_servers(&self) -> Option<String> {
        let available = self.available_count();
        if available >= 5 {
            return None;
        }

        let now = std::time::Instant::now();
        let mut candidates: Vec<(String, std::time::Instant)> = Vec::new();

        for server in &self.all_servers {
            let weight = self.weights.get(server).map(|w| *w).unwrap_or(0);
            if weight == 0 {
                if let Some(probe_time_ref) = self.next_probe_time.get(server) {
                    let probe_time = *probe_time_ref;
                    if now >= probe_time {
                        candidates.push((server.clone(), probe_time));
                    }
                }
            }
        }

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by(|a, b| a.1.cmp(&b.1));

        let server_to_restore = candidates[0].0.clone();
        info!(
            "可用服务器不足({}<5),准备探测最早到期的服务器: {}",
            available, server_to_restore
        );

        Some(server_to_restore)
    }

    /// 重置所有服务器的短期速度窗口（任务数变化时调用）
    pub fn reset_speed_windows(&self) {
        for entry in self.server_recent_speeds.iter() {
            if let Ok(mut window) = entry.value().try_lock() {
                window.clear();
            }
        }
        info!("已重置所有服务器的速度窗口（任务数变化，带宽重新分配）");
    }

    /// 处理探测失败（指数退避）
    pub fn handle_probe_failure(&self, server: &str) {
        let server_string = server.to_string();

        let current_cooldown = self.cooldown_secs.get(&server_string).map(|v| *v).unwrap_or(10);
        let new_cooldown = (current_cooldown * 2).min(40);
        self.cooldown_secs.insert(server_string.clone(), new_cooldown);

        let next_time = std::time::Instant::now() + std::time::Duration::from_secs(new_cooldown);
        self.next_probe_time.insert(server_string.clone(), next_time);

        warn!(
            "服务器探测失败: {}, cooldown: {}s -> {}s, 下次探测: {}秒后",
            server, current_cooldown, new_cooldown, new_cooldown
        );
    }

    /// 恢复服务器权重（探测成功后调用）
    pub fn restore_server(&self, server: &str, new_speed: f64) {
        let server_string = server.to_string();

        if let Some(mut weight) = self.weights.get_mut(&server_string) {
            *weight = 1;
        }

        self.server_scores.insert(server_string.clone(), 50);
        self.cooldown_secs.insert(server_string.clone(), 10);
        self.next_probe_time.remove(&server_string);

        self.server_speeds.insert(server_string.clone(), new_speed);
        self.server_avg_speeds.insert(server_string.clone(), new_speed);
        self.server_sample_counts.insert(server_string.clone(), 1);
        self.server_recent_speeds.insert(server_string.clone(), StdMutex::new(VecDeque::new()));

        info!(
            "服务器恢复: {} (新速度 {:.2} KB/s, score=50, 当前可用 {} 个服务器)",
            server, new_speed, self.available_count()
        );
    }

    /// 根据服务器和分片大小计算动态超时时间（秒）
    ///
    /// # 参数
    /// * `server` - 服务器 URL
    /// * `chunk_size` - 分片大小（字节）
    ///
    /// # 返回
    /// 超时时间（秒），范围在 [30, 180] 之间
    pub fn calculate_timeout(&self, server: &str, chunk_size: u64) -> u64 {
        const SAFETY_FACTOR: f64 = 3.0;
        const MIN_TIMEOUT: u64 = 30;
        const MAX_TIMEOUT: u64 = 180;

        let speed_kbps = self.server_avg_speeds.get(server).map(|v| *v)
            .or_else(|| self.server_speeds.get(server).map(|v| *v))
            .unwrap_or(500.0);

        if speed_kbps > 0.0 {
            let chunk_size_kb = chunk_size as f64 / 1024.0;
            let theoretical_time = chunk_size_kb / speed_kbps;
            let timeout = (theoretical_time * SAFETY_FACTOR) as u64;
            return timeout.clamp(MIN_TIMEOUT, MAX_TIMEOUT);
        }

        60
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_manager_creation() {
        let servers = vec![
            "https://pcs1.baidu.com".to_string(),
            "https://pcs2.baidu.com".to_string(),
            "https://pcs3.baidu.com".to_string(),
        ];
        let speeds = vec![1000.0, 800.0, 600.0];

        let manager = PcsServerHealthManager::new(servers.clone(), speeds);

        assert_eq!(manager.available_count(), 3);
        assert!(manager.get_server(0).is_some());
    }

    #[test]
    fn test_server_selection() {
        let servers = vec![
            "https://pcs1.baidu.com".to_string(),
            "https://pcs2.baidu.com".to_string(),
        ];

        let manager = PcsServerHealthManager::from_servers(servers);

        // 轮询选择
        let s0 = manager.get_server(0);
        let s1 = manager.get_server(1);

        assert!(s0.is_some());
        assert!(s1.is_some());
        assert_ne!(s0, s1);
    }

    #[test]
    fn test_hybrid_selection() {
        let servers = vec![
            "https://pcs1.baidu.com".to_string(),
            "https://pcs2.baidu.com".to_string(),
        ];
        let speeds = vec![1000.0, 500.0]; // pcs1 速度是 pcs2 的两倍

        let manager = PcsServerHealthManager::new(servers, speeds);

        // 加权选择应该更频繁地选择高速服务器
        let mut pcs1_count = 0;
        let mut pcs2_count = 0;

        for i in 0..100 {
            if let Some(server) = manager.get_server_hybrid(i) {
                if server.contains("pcs1") {
                    pcs1_count += 1;
                } else {
                    pcs2_count += 1;
                }
            }
        }

        // pcs1 应该被选择更多次（约 2:1 比例）
        assert!(pcs1_count > pcs2_count);
    }

    #[test]
    fn test_speed_recording() {
        let servers = vec!["https://pcs1.baidu.com".to_string()];
        let manager = PcsServerHealthManager::from_servers(servers);

        // 记录速度
        let speed = manager.record_chunk_speed("https://pcs1.baidu.com", 4 * 1024 * 1024, 4000);

        // 4MB / 4秒 = 1MB/s = 1024 KB/s
        assert!((speed - 1024.0).abs() < 10.0);
    }

    #[test]
    fn test_timeout_calculation() {
        let servers = vec!["https://pcs1.baidu.com".to_string()];
        let speeds = vec![1024.0]; // 1MB/s

        let manager = PcsServerHealthManager::new(servers, speeds);

        // 4MB 分片，1MB/s 速度，理论时间 4 秒，安全系数 3 倍 = 12 秒
        // 但最小超时是 30 秒
        let timeout = manager.calculate_timeout("https://pcs1.baidu.com", 4 * 1024 * 1024);
        assert_eq!(timeout, 30); // 最小超时
    }
}
