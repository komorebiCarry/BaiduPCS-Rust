//! 代理故障自动回退与恢复管理器
//!
//! 当代理连续失败达到阈值时，自动回退到直连模式。
//! 后台探测任务定期检测代理是否恢复，恢复后自动切回代理模式。
//! 用户手动修改代理配置时，立即取消探测任务并重置状态。

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::proxy::ProxyConfig;

/// 判断错误是否为代理/连接相关错误
///
/// 只有代理相关错误（超时、连接拒绝、代理认证失败等）才计入失败计数，
/// 业务错误（API 403、文件不存在等）不触发回退。
pub fn is_proxy_or_connection_error(error: &anyhow::Error) -> bool {
    let err_str = format!("{:#}", error);
    let lower = err_str.to_lowercase();

    // reqwest 错误类型检查
    if let Some(reqwest_err) = error.downcast_ref::<reqwest::Error>() {
        // 连接错误（包括代理连接失败）
        if reqwest_err.is_connect() {
            return true;
        }
        // 超时
        if reqwest_err.is_timeout() {
            return true;
        }
        // 代理认证失败 (407)
        if let Some(status) = reqwest_err.status() {
            if status.as_u16() == 407 {
                return true;
            }
        }
    }

    // 字符串匹配兜底（处理被 anyhow 包装后丢失类型信息的情况）
    if lower.contains("connection refused")
        || lower.contains("connection reset")
        || lower.contains("connection aborted")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("proxy")
        || lower.contains("socks")
        || lower.contains("tunnel")
        || lower.contains("dns")
        || lower.contains("name resolution")
        || lower.contains("no route to host")
        || lower.contains("network unreachable")
        || lower.contains("host unreachable")
    {
        return true;
    }

    false
}

/// 代理运行状态（前端展示用）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyRuntimeStatus {
    /// 代理正常工作
    Normal,
    /// 已回退到直连
    FallenBackToDirect,
    /// 正在探测代理是否恢复
    Probing,
    /// 未配置代理
    NoProxy,
}

/// 代理故障回退管理器
///
/// 存储在 AppState 中，所有热更新操作通过已有的热更新通道执行。
/// `ProxyFallbackManager` 仅负责状态管理，不负责热更新执行。
pub struct ProxyFallbackManager {
    /// 连续失败计数
    consecutive_failures: AtomicU32,
    /// 触发回退的失败阈值（默认 3）
    failure_threshold: u32,
    /// 当前是否处于回退（直连）状态
    fallen_back: AtomicBool,
    /// 抖动计数（代理切回后又快速失败的次数）
    flap_count: AtomicU32,
    /// 上次切回代理的时间（用于判断是否"快速失败"）
    last_proxy_restore_time: TokioMutex<Option<Instant>>,
    /// 探测任务的 CancellationToken
    probe_cancel_token: TokioMutex<Option<CancellationToken>>,
    /// 用户配置的原始代理配置（回退时保留，用于探测恢复）
    user_proxy_config: TokioMutex<Option<ProxyConfig>>,
    /// 当前运行时状态
    runtime_status: TokioMutex<ProxyRuntimeStatus>,
    /// 判定"快速失败"的时间窗口（默认 5 分钟）
    stability_window: Duration,
    /// 上次探测循环开始 sleep 的时间（用于计算 next_probe_in_secs）
    last_probe_sleep_started_at: TokioMutex<Option<Instant>>,
    /// 热更新执行器（由 AppState 注入，用于回退/恢复时执行热更新）
    updater: TokioMutex<Option<Arc<dyn ProxyHotUpdater>>>,
}

impl std::fmt::Debug for ProxyFallbackManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyFallbackManager")
            .field("consecutive_failures", &self.consecutive_failures.load(Ordering::Relaxed))
            .field("failure_threshold", &self.failure_threshold)
            .field("fallen_back", &self.fallen_back.load(Ordering::Relaxed))
            .field("flap_count", &self.flap_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl ProxyFallbackManager {
    pub fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            failure_threshold: 3,
            fallen_back: AtomicBool::new(false),
            flap_count: AtomicU32::new(0),
            last_proxy_restore_time: TokioMutex::new(None),
            probe_cancel_token: TokioMutex::new(None),
            user_proxy_config: TokioMutex::new(None),
            runtime_status: TokioMutex::new(ProxyRuntimeStatus::NoProxy),
            stability_window: Duration::from_secs(300), // 5 分钟
            last_probe_sleep_started_at: TokioMutex::new(None),
            updater: TokioMutex::new(None),
        }
    }

    /// 记录一次代理请求失败
    ///
    /// 返回 true 表示需要触发回退到直连。
    /// 使用 `compare_exchange` 保证只有一个线程能触发回退（解决并发竞态）。
    pub fn record_failure(&self) -> bool {
        let count = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.failure_threshold {
            // 只有第一个到达阈值的线程能将 fallen_back 从 false 翻为 true
            self.fallen_back
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        } else {
            false
        }
    }

    /// 记录一次代理请求成功，重置失败计数
    ///
    /// 注意：仅在使用代理的请求中调用，直连模式下的请求成功不应调用此方法
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
    }

    /// 当前是否处于回退（直连）状态
    ///
    /// 调用方用此判断是否应调用 record_success/record_failure
    pub fn is_fallen_back(&self) -> bool {
        self.fallen_back.load(Ordering::SeqCst)
    }

    /// 获取当前探测间隔（基于 flap_count 的阶梯式间隔）
    pub fn probe_interval(&self) -> Duration {
        match self.flap_count.load(Ordering::SeqCst) {
            0 | 1 => Duration::from_secs(30),
            2 => Duration::from_secs(60),
            3 => Duration::from_secs(120),  // 2min
            4 => Duration::from_secs(300),  // 5min
            _ => Duration::from_secs(600),  // 10min 封顶
        }
    }

    /// 标记已回退到直连（仅更新内部状态，不执行热更新）
    ///
    /// 热更新由调用方通过 `perform_proxy_hot_update()` 独立执行
    pub async fn mark_fallen_back(&self) {
        // 显式设置 fallen_back（启动时 execute_fallback 直接调用，不经过 record_failure）
        self.fallen_back.store(true, Ordering::SeqCst);
        *self.runtime_status.lock().await = ProxyRuntimeStatus::FallenBackToDirect;
        info!(
            "代理连续失败 {} 次，已自动回退到直连模式",
            self.failure_threshold
        );
    }

    /// 标记已恢复代理（仅更新内部状态，不执行热更新）
    ///
    /// 热更新由调用方通过 `perform_proxy_hot_update()` 独立执行
    pub async fn mark_restored(&self) {
        self.fallen_back.store(false, Ordering::SeqCst);
        self.consecutive_failures.store(0, Ordering::SeqCst);
        *self.last_proxy_restore_time.lock().await = Some(Instant::now());
        *self.runtime_status.lock().await = ProxyRuntimeStatus::Normal;
        info!("代理探测成功，已切回代理模式");
    }

    /// 获取用户配置的代理（供探测和恢复使用）
    pub async fn user_proxy_config(&self) -> Option<ProxyConfig> {
        self.user_proxy_config.lock().await.clone()
    }

    /// 代理切回后又失败，检查是否属于"快速失败"（抖动）
    ///
    /// 如果上次恢复时间在稳定窗口内，则 flap_count +1 并返回 true。
    /// 如果已超过稳定窗口，则重置 flap_count 为 0 并返回 false。
    pub async fn check_and_increment_flap(&self) -> bool {
        let last_restore = self.last_proxy_restore_time.lock().await;
        if let Some(restore_time) = *last_restore {
            if restore_time.elapsed() < self.stability_window {
                self.flap_count.fetch_add(1, Ordering::SeqCst);
                return true;
            } else {
                self.flap_count.store(0, Ordering::SeqCst);
            }
        }
        false
    }

    /// 用户手动修改代理配置时调用
    ///
    /// 停止探测任务，重置所有状态
    pub async fn on_user_config_change(&self, new_proxy: Option<&ProxyConfig>) {
        if let Some(token) = self.probe_cancel_token.lock().await.take() {
            token.cancel();
            info!("用户修改代理配置，已取消探测任务");
        }
        self.fallen_back.store(false, Ordering::SeqCst);
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.flap_count.store(0, Ordering::SeqCst);
        *self.last_proxy_restore_time.lock().await = None;
        *self.last_probe_sleep_started_at.lock().await = None;
        *self.user_proxy_config.lock().await = new_proxy.cloned();
        *self.runtime_status.lock().await = if new_proxy.is_some() {
            ProxyRuntimeStatus::Normal
        } else {
            ProxyRuntimeStatus::NoProxy
        };
    }

    /// 获取当前运行时状态（供前端 API 查询）
    pub async fn runtime_status(&self) -> ProxyRuntimeStatus {
        self.runtime_status.lock().await.clone()
    }

    /// 获取当前 flap_count（供前端 API 查询）
    pub fn flap_count(&self) -> u32 {
        self.flap_count.load(Ordering::SeqCst)
    }

    /// 计算距下次探测的剩余秒数（供前端 API 查询）
    ///
    /// 仅在 Probing 状态下有意义，其他状态返回 None
    pub async fn next_probe_in_secs(&self) -> Option<u64> {
        let started_at = self.last_probe_sleep_started_at.lock().await;
        if let Some(start) = *started_at {
            let interval = self.probe_interval();
            let elapsed = start.elapsed();
            if elapsed < interval {
                Some((interval - elapsed).as_secs())
            } else {
                Some(0)
            }
        } else {
            None
        }
    }

    /// 设置探测任务的 CancellationToken（供 start_proxy_probe_task 使用）
    pub async fn set_probe_cancel_token(&self, token: Option<CancellationToken>) {
        let mut guard = self.probe_cancel_token.lock().await;
        // 取消之前的探测任务（如果有）
        if let Some(old_token) = guard.take() {
            old_token.cancel();
        }
        *guard = token;
    }

    /// 设置运行时状态（供探测任务使用）
    pub async fn set_runtime_status(&self, status: ProxyRuntimeStatus) {
        *self.runtime_status.lock().await = status;
    }

    /// 设置上次探测 sleep 开始时间（供探测任务使用）
    pub async fn set_last_probe_sleep_started_at(&self, time: Option<Instant>) {
        *self.last_probe_sleep_started_at.lock().await = time;
    }

    /// 设置用户代理配置（供登录时使用）
    ///
    /// 仅在系统未处于回退/探测状态时更新 runtime_status，
    /// 避免覆盖 execute_fallback() 设置的 Probing/FallenBackToDirect 状态。
    pub async fn set_user_proxy_config(&self, proxy: Option<ProxyConfig>) {
        let has_proxy = proxy.is_some();
        *self.user_proxy_config.lock().await = proxy;

        // 如果已经处于回退状态，不要覆盖 runtime_status
        if self.is_fallen_back() {
            return;
        }

        *self.runtime_status.lock().await = if has_proxy {
            ProxyRuntimeStatus::Normal
        } else {
            ProxyRuntimeStatus::NoProxy
        };
    }

    /// 设置热更新执行器（由 AppState 在初始化后注入）
    ///
    /// 必须在 AppState 创建完成后调用，否则回退触发时无法执行热更新和启动探测任务。
    pub async fn set_updater(&self, updater: Arc<dyn ProxyHotUpdater>) {
        *self.updater.lock().await = Some(updater);
    }

    /// 执行回退到直连的完整流程（热更新 + 启动探测任务）
    ///
    /// 在 `record_failure()` 返回 true 且 `allow_fallback=true` 时调用。
    /// 此方法会：
    /// 1. 标记内部状态为已回退
    /// 2. 检查抖动
    /// 3. 执行热更新（切换所有 HTTP 客户端到直连）
    /// 4. 启动后台探测任务（定期检测代理是否恢复）
    pub async fn execute_fallback(self: &Arc<Self>) {
        self.mark_fallen_back().await;
        self.check_and_increment_flap().await;

        // 执行热更新：切换到直连
        let updater = self.updater.lock().await.clone();
        if let Some(ref updater) = updater {
            perform_proxy_hot_update(updater.as_ref(), None).await;
            // 启动探测任务
            start_proxy_probe_task(Arc::clone(self), Arc::clone(updater)).await;
        } else {
            warn!("代理回退触发但 updater 未设置，无法执行热更新和启动探测任务");
        }
    }
}


/// 将 reqwest 代理错误转换为用户友好的中文提示
fn friendly_proxy_error(e: &reqwest::Error) -> String {
    let raw = e.to_string();

    if e.is_timeout() {
        return "连接超时，请检查代理地址和端口是否正确".to_string();
    }
    if e.is_connect() {
        if raw.contains("Connection refused") || raw.contains("connection refused") {
            return "代理服务器拒绝连接，请确认代理服务是否已启动".to_string();
        }
        if raw.contains("eof while tunneling") || raw.contains("unexpected eof") {
            return "代理隧道建立失败，可能是代理类型不匹配或需要认证".to_string();
        }
        if raw.contains("dns") || raw.contains("resolve") || raw.contains("Name or service not known") {
            return "代理地址无法解析，请检查主机名是否正确".to_string();
        }
        if raw.contains("reset") {
            return "连接被代理服务器重置".to_string();
        }
        return format!("无法连接到代理服务器: {}", raw);
    }

    format!("代理请求失败: {}", raw)
}

/// 探测代理是否可用
///
/// 用原始代理配置构建临时客户端，发 HEAD 请求到百度网盘。
/// 10 秒超时，成功返回 Ok(())，失败返回错误。
pub async fn probe_proxy(proxy: &ProxyConfig) -> anyhow::Result<()> {
    let reqwest_proxy = proxy
        .to_reqwest_proxy()?
        .ok_or_else(|| anyhow::anyhow!("代理配置为 None，无法探测"))?;
    let client = reqwest::Client::builder()
        .proxy(reqwest_proxy)
        .timeout(Duration::from_secs(10))
        .build()?;

    client
        .head("https://pan.baidu.com")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{}", friendly_proxy_error(&e)))?;

    Ok(())
}

/// 独立的 async 热更新函数（供回退和恢复调用）
///
/// 复用已有的热更新逻辑：重建 QRCodeAuth、NetdiskClient、更新 DownloadEngine 共享客户端。
/// `proxy` 为 None 表示切换到直连模式，为 Some 表示切换到代理模式。
///
/// 注意：此函数接受泛型 AppState-like 参数，通过 trait 解耦避免循环依赖。
/// 实际调用时由 server 层提供具体实现。
pub async fn perform_proxy_hot_update(updater: &dyn ProxyHotUpdater, proxy: Option<&ProxyConfig>) {
    if let Err(e) = updater.update_qrcode_auth(proxy).await {
        warn!("QRCodeAuth 热更新失败: {}", e);
    } else {
        info!("✓ QRCodeAuth 已热更新（回退/恢复）");
    }

    if let Err(e) = updater.update_netdisk_client(proxy).await {
        warn!("NetdiskClient 热更新失败: {}", e);
    } else {
        info!("✓ NetdiskClient 已热更新（回退/恢复）");
    }

    updater.update_download_engine(proxy).await;
    info!("✓ DownloadEngine 已热更新（回退/恢复）");

    updater.update_upload_engine(proxy).await;
    info!("✓ UploadEngine 已热更新（回退/恢复）");

    updater.update_transfer_engine(proxy).await;
    info!("✓ TransferEngine 已热更新（回退/恢复）");

    updater.update_cloud_dl_monitor(proxy).await;
    info!("✓ CloudDlMonitor 已热更新（回退/恢复）");

    updater.update_folder_download_manager(proxy).await;
    info!("✓ FolderDownloadManager 已热更新（回退/恢复）");

    updater.update_autobackup_manager(proxy).await;
    info!("✓ AutoBackupManager 已热更新（回退/恢复）");
}

/// 代理热更新 trait，由 AppState 实现
///
/// 通过 trait 解耦 common 模块与 server 模块的循环依赖。
#[async_trait::async_trait]
pub trait ProxyHotUpdater: Send + Sync {
    async fn update_qrcode_auth(&self, proxy: Option<&ProxyConfig>) -> anyhow::Result<()>;
    async fn update_netdisk_client(&self, proxy: Option<&ProxyConfig>) -> anyhow::Result<()>;
    async fn update_download_engine(&self, proxy: Option<&ProxyConfig>);
    async fn update_upload_engine(&self, proxy: Option<&ProxyConfig>);
    async fn update_transfer_engine(&self, proxy: Option<&ProxyConfig>);
    async fn update_cloud_dl_monitor(&self, proxy: Option<&ProxyConfig>);
    async fn update_folder_download_manager(&self, proxy: Option<&ProxyConfig>);
    async fn update_autobackup_manager(&self, proxy: Option<&ProxyConfig>);
}


/// 启动后台代理探测任务
///
/// 在 `mark_fallen_back` 后由调用方启动。
/// 使用 `CancellationToken` 控制生命周期，用户手动修改配置时可取消。
/// 探测成功后调用 `perform_proxy_hot_update()` 执行热更新，然后调用 `mark_restored()` 更新状态。
pub async fn start_proxy_probe_task(
    fallback_mgr: std::sync::Arc<ProxyFallbackManager>,
    updater: std::sync::Arc<dyn ProxyHotUpdater>,
) {
    let cancel_token = CancellationToken::new();

    // 取消之前的探测任务（如果有），设置新的 token
    fallback_mgr
        .set_probe_cancel_token(Some(cancel_token.clone()))
        .await;
    fallback_mgr
        .set_runtime_status(ProxyRuntimeStatus::Probing)
        .await;

    let mgr = fallback_mgr.clone();
    tokio::spawn(async move {
        loop {
            let interval = mgr.probe_interval();
            // 记录 sleep 开始时间，供 next_probe_in_secs() 计算剩余秒数
            mgr.set_last_probe_sleep_started_at(Some(Instant::now()))
                .await;

            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!("代理探测任务已取消");
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    let user_proxy = mgr.user_proxy_config().await;
                    if let Some(ref proxy) = user_proxy {
                        match probe_proxy(proxy).await {
                            Ok(()) => {
                                info!("代理探测成功，准备切回代理");
                                // 1. 执行热更新
                                perform_proxy_hot_update(updater.as_ref(), Some(proxy)).await;
                                // 2. 更新 FallbackManager 内部状态
                                mgr.mark_restored().await;
                                break;
                            }
                            Err(e) => {
                                let flap_count = mgr.flap_count();
                                info!(
                                    "代理探测失败: {}，flap_count={}, 下次探测间隔: {:?}",
                                    e,
                                    flap_count,
                                    mgr.probe_interval()
                                );
                            }
                        }
                    } else {
                        // 用户配置已被清除，停止探测
                        info!("用户代理配置已清除，停止探测");
                        break;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ProxyType;

    fn make_proxy_config(allow_fallback: bool) -> ProxyConfig {
        ProxyConfig {
            proxy_type: ProxyType::Http,
            host: "127.0.0.1".to_string(),
            port: 7890,
            username: None,
            password: None,
            allow_fallback,
        }
    }

    #[tokio::test]
    async fn test_record_failure_triggers_fallback_at_threshold() {
        let mgr = ProxyFallbackManager::new();
        // 默认阈值为 3
        assert!(!mgr.record_failure()); // 1
        assert!(!mgr.record_failure()); // 2
        assert!(mgr.record_failure());  // 3 → 触发回退
        assert!(mgr.is_fallen_back());
    }

    #[tokio::test]
    async fn test_record_failure_only_first_thread_triggers() {
        let mgr = ProxyFallbackManager::new();
        mgr.record_failure(); // 1
        mgr.record_failure(); // 2
        assert!(mgr.record_failure());  // 3 → 第一个触发
        assert!(!mgr.record_failure()); // 4 → 已经回退，compare_exchange 失败
        assert!(!mgr.record_failure()); // 5 → 同上
    }

    #[tokio::test]
    async fn test_record_success_resets_count() {
        let mgr = ProxyFallbackManager::new();
        mgr.record_failure(); // 1
        mgr.record_failure(); // 2
        mgr.record_success(); // 重置
        assert!(!mgr.record_failure()); // 1（重新开始）
        assert!(!mgr.record_failure()); // 2
        assert!(mgr.record_failure());  // 3 → 触发
    }

    #[tokio::test]
    async fn test_probe_interval_escalation() {
        let mgr = ProxyFallbackManager::new();
        // flap_count = 0
        assert_eq!(mgr.probe_interval(), Duration::from_secs(30));

        mgr.flap_count.store(1, Ordering::SeqCst);
        assert_eq!(mgr.probe_interval(), Duration::from_secs(30));

        mgr.flap_count.store(2, Ordering::SeqCst);
        assert_eq!(mgr.probe_interval(), Duration::from_secs(60));

        mgr.flap_count.store(3, Ordering::SeqCst);
        assert_eq!(mgr.probe_interval(), Duration::from_secs(120));

        mgr.flap_count.store(4, Ordering::SeqCst);
        assert_eq!(mgr.probe_interval(), Duration::from_secs(300));

        mgr.flap_count.store(5, Ordering::SeqCst);
        assert_eq!(mgr.probe_interval(), Duration::from_secs(600));

        mgr.flap_count.store(100, Ordering::SeqCst);
        assert_eq!(mgr.probe_interval(), Duration::from_secs(600)); // 封顶
    }

    #[tokio::test]
    async fn test_check_and_increment_flap_within_window() {
        let mgr = ProxyFallbackManager::new();
        // 模拟刚刚恢复
        *mgr.last_proxy_restore_time.lock().await = Some(Instant::now());

        // 在稳定窗口内，应该递增 flap_count
        assert!(mgr.check_and_increment_flap().await);
        assert_eq!(mgr.flap_count(), 1);

        assert!(mgr.check_and_increment_flap().await);
        assert_eq!(mgr.flap_count(), 2);
    }

    #[tokio::test]
    async fn test_check_and_increment_flap_outside_window() {
        let mgr = ProxyFallbackManager::new();
        // 模拟很久以前恢复（超过稳定窗口）
        *mgr.last_proxy_restore_time.lock().await =
            Some(Instant::now() - Duration::from_secs(600));

        mgr.flap_count.store(5, Ordering::SeqCst);

        // 超过稳定窗口，应该重置 flap_count
        assert!(!mgr.check_and_increment_flap().await);
        assert_eq!(mgr.flap_count(), 0);
    }

    #[tokio::test]
    async fn test_on_user_config_change_resets_all() {
        let mgr = ProxyFallbackManager::new();
        let proxy = make_proxy_config(true);

        // 设置一些状态
        mgr.record_failure();
        mgr.record_failure();
        mgr.record_failure(); // 触发回退
        mgr.flap_count.store(3, Ordering::SeqCst);
        *mgr.last_proxy_restore_time.lock().await = Some(Instant::now());

        assert!(mgr.is_fallen_back());
        assert_eq!(mgr.flap_count(), 3);

        // 用户修改配置
        mgr.on_user_config_change(Some(&proxy)).await;

        // 所有状态应该被重置
        assert!(!mgr.is_fallen_back());
        assert_eq!(mgr.flap_count(), 0);
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::Normal);
        assert!(mgr.user_proxy_config().await.is_some());
    }

    #[tokio::test]
    async fn test_on_user_config_change_to_none() {
        let mgr = ProxyFallbackManager::new();

        // 用户清除代理配置
        mgr.on_user_config_change(None).await;

        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::NoProxy);
        assert!(mgr.user_proxy_config().await.is_none());
    }

    #[tokio::test]
    async fn test_mark_fallen_back_and_restored() {
        let mgr = ProxyFallbackManager::new();

        // 触发回退
        mgr.record_failure();
        mgr.record_failure();
        mgr.record_failure();
        mgr.mark_fallen_back().await;

        assert!(mgr.is_fallen_back());
        assert_eq!(
            mgr.runtime_status().await,
            ProxyRuntimeStatus::FallenBackToDirect
        );

        // 恢复
        mgr.mark_restored().await;

        assert!(!mgr.is_fallen_back());
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::Normal);
    }

    #[tokio::test]
    async fn test_allow_fallback_false_still_returns_true() {
        // allow_fallback=false 时，record_failure 仍然返回 true（达到阈值时）
        // 但调用方应检查 allow_fallback 并决定是否执行回退
        let mgr = ProxyFallbackManager::new();
        mgr.set_user_proxy_config(Some(make_proxy_config(false))).await;

        mgr.record_failure();
        mgr.record_failure();
        let should_fallback = mgr.record_failure();

        // record_failure 本身不检查 allow_fallback，它只管计数和 compare_exchange
        assert!(should_fallback);

        // 调用方应该检查 allow_fallback
        let config = mgr.user_proxy_config().await.unwrap();
        assert!(!config.allow_fallback);
        // 因此调用方不应执行回退
    }

    #[test]
    fn test_is_proxy_or_connection_error_connection_refused() {
        let err = anyhow::anyhow!("connection refused by proxy server");
        assert!(is_proxy_or_connection_error(&err));
    }

    #[test]
    fn test_is_proxy_or_connection_error_timeout() {
        let err = anyhow::anyhow!("request timed out after 10s");
        assert!(is_proxy_or_connection_error(&err));
    }

    #[test]
    fn test_is_proxy_or_connection_error_business_error() {
        let err = anyhow::anyhow!("API error 403: forbidden");
        assert!(!is_proxy_or_connection_error(&err));
    }

    #[test]
    fn test_is_proxy_or_connection_error_file_not_found() {
        let err = anyhow::anyhow!("file not found: /path/to/file");
        assert!(!is_proxy_or_connection_error(&err));
    }

    #[tokio::test]
    async fn test_set_user_proxy_config_updates_status() {
        let mgr = ProxyFallbackManager::new();

        // 初始状态
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::NoProxy);

        // 设置代理配置
        mgr.set_user_proxy_config(Some(make_proxy_config(true))).await;
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::Normal);

        // 清除代理配置
        mgr.set_user_proxy_config(None).await;
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::NoProxy);
    }

    #[tokio::test]
    async fn test_set_user_proxy_config_preserves_status_when_fallen_back() {
        let mgr = ProxyFallbackManager::new();

        // 先设置代理配置
        mgr.set_user_proxy_config(Some(make_proxy_config(true))).await;
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::Normal);

        // 模拟回退：标记为已回退，设置状态为 Probing
        mgr.mark_fallen_back().await;
        mgr.set_runtime_status(ProxyRuntimeStatus::Probing).await;
        assert!(mgr.is_fallen_back());
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::Probing);

        // 再次调用 set_user_proxy_config，不应覆盖 Probing 状态
        mgr.set_user_proxy_config(Some(make_proxy_config(true))).await;
        assert_eq!(mgr.runtime_status().await, ProxyRuntimeStatus::Probing);
    }

    #[tokio::test]
    async fn test_next_probe_in_secs_none_when_not_probing() {
        let mgr = ProxyFallbackManager::new();
        assert_eq!(mgr.next_probe_in_secs().await, None);
    }

    #[tokio::test]
    async fn test_next_probe_in_secs_returns_remaining() {
        let mgr = ProxyFallbackManager::new();
        // 模拟刚开始 sleep
        mgr.set_last_probe_sleep_started_at(Some(Instant::now())).await;

        let remaining = mgr.next_probe_in_secs().await;
        assert!(remaining.is_some());
        // 默认 flap_count=0，间隔 30s，刚开始应该接近 30
        assert!(remaining.unwrap() <= 30);
    }
}
