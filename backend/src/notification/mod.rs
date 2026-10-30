//! 消息通知（Gotify / 通用 Webhook）
//!
//! - 配置：[`NotificationConfig`]（`AppConfig.notification`）
//! - 推送：[`notify`] 在 tokio 后台任务中异步发送，失败只记 `warn` 日志，绝不向调用方传播错误，
//!   也不阻塞业务流程。
//! - 事件来源：
//!   - 转存 / 下载 / 文件夹下载 / 自动备份：在 `WebSocketManager::send_if_subscribed`
//!     处统一旁路观察终态事件（[`observe_task_event`]），任务名称等上下文由
//!     [`TaskEventInspector`]（`AppState` 注册）异步补齐；
//!   - 分享同步：在 `share_sync::manager` 中直接调用 [`notify`]（需要订阅名 / 变更统计）。
//! - 安全：Gotify Token、Webhook URL / 请求头都不写日志；reqwest 错误会去掉 URL 再输出。

pub mod config;
mod sender;
mod template;

pub use config::{
    GotifyChannelConfig, NotificationConfig, NotificationEventToggles, WebhookChannelConfig,
    WebhookHeader, DEFAULT_WEBHOOK_BODY_TEMPLATE,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::server::events::{BackupEvent, DownloadEvent, FolderEvent, TaskEvent, TransferEvent};

/// 通知标题统一前缀
pub const TITLE_PREFIX: &str = "BaiduPCS-Rust";

/// 同一任务同一类通知的去重窗口（终态事件在个别路径上可能重复发布）
const DEDUP_WINDOW: Duration = Duration::from_secs(600);
/// 去重表容量上限，超过时清理过期项
const DEDUP_MAX_ENTRIES: usize = 1024;

/// 通知事件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationKind {
    Test,
    ShareSyncUpdated,
    ShareSyncFailed,
    ShareSyncLinkInvalid,
    TransferCompleted,
    TransferFailed,
    DownloadCompleted,
    DownloadFailed,
    BackupCompleted,
    BackupFailed,
}

impl NotificationKind {
    /// 机器可读事件名（模板 `{{event}}`）
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationKind::Test => "test",
            NotificationKind::ShareSyncUpdated => "share_sync_updated",
            NotificationKind::ShareSyncFailed => "share_sync_failed",
            NotificationKind::ShareSyncLinkInvalid => "share_sync_link_invalid",
            NotificationKind::TransferCompleted => "transfer_completed",
            NotificationKind::TransferFailed => "transfer_failed",
            NotificationKind::DownloadCompleted => "download_completed",
            NotificationKind::DownloadFailed => "download_failed",
            NotificationKind::BackupCompleted => "backup_completed",
            NotificationKind::BackupFailed => "backup_failed",
        }
    }

    /// 中文事件名（标题用）
    pub fn label(&self) -> &'static str {
        match self {
            NotificationKind::Test => "测试通知",
            NotificationKind::ShareSyncUpdated => "分享同步发现更新",
            NotificationKind::ShareSyncFailed => "分享同步失败",
            NotificationKind::ShareSyncLinkInvalid => "分享链接失效，订阅已暂停",
            NotificationKind::TransferCompleted => "转存完成",
            NotificationKind::TransferFailed => "转存失败",
            NotificationKind::DownloadCompleted => "下载完成",
            NotificationKind::DownloadFailed => "下载失败",
            NotificationKind::BackupCompleted => "自动备份完成",
            NotificationKind::BackupFailed => "自动备份失败",
        }
    }

    /// 级别（模板 `{{level}}`）：info / warning / error
    pub fn level(&self) -> &'static str {
        match self {
            NotificationKind::ShareSyncFailed
            | NotificationKind::TransferFailed
            | NotificationKind::DownloadFailed
            | NotificationKind::BackupFailed => "error",
            NotificationKind::ShareSyncLinkInvalid => "warning",
            _ => "info",
        }
    }
}

/// 一条待推送的通知
#[derive(Debug, Clone)]
pub struct Notification {
    pub kind: NotificationKind,
    /// 标题（已含前缀）
    pub title: String,
    /// 正文（多行纯文本）
    pub message: String,
    /// 任务 / 订阅名称
    pub name: Option<String>,
    /// 任务 / 订阅 ID
    pub task_id: Option<String>,
    /// 关联账号 UID
    pub account_uid: Option<u64>,
    /// 去重键；`None` 表示不去重
    pub dedup_key: Option<String>,
    /// 生成时间
    pub time: chrono::DateTime<chrono::Local>,
}

impl Notification {
    /// 构造通知；标题自动生成为 `[前缀] 事件名: 名称`
    pub fn new(kind: NotificationKind, name: Option<String>, message: impl Into<String>) -> Self {
        let title = match name.as_deref().filter(|s| !s.is_empty()) {
            Some(n) => format!("[{}] {}：{}", TITLE_PREFIX, kind.label(), n),
            None => format!("[{}] {}", TITLE_PREFIX, kind.label()),
        };
        Self {
            kind,
            title,
            message: message.into(),
            name,
            task_id: None,
            account_uid: None,
            dedup_key: None,
            time: chrono::Local::now(),
        }
    }

    pub fn with_task_id(mut self, id: impl Into<String>) -> Self {
        self.task_id = Some(id.into());
        self
    }

    pub fn with_account(mut self, uid: Option<u64>) -> Self {
        self.account_uid = uid;
        self
    }

    pub fn with_dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }
}

/// 单个渠道的发送结果（测试接口返回）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelSendResult {
    /// `webhook` / `gotify`
    pub channel: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 终态事件的上下文补充
#[derive(Debug, Clone, Default)]
pub struct EventContext {
    /// 为 true 时不推送（例如分享同步 / 备份内部产生的转存、下载任务）
    pub skip: bool,
    /// 任务显示名
    pub name: Option<String>,
    /// 额外正文行
    pub details: Vec<String>,
}

impl EventContext {
    pub fn skip() -> Self {
        Self {
            skip: true,
            ..Default::default()
        }
    }
}

/// 由应用层注册，用于按事件反查任务名称、过滤内部任务
#[async_trait]
pub trait TaskEventInspector: Send + Sync {
    async fn inspect(&self, kind: NotificationKind, event: &TaskEvent) -> EventContext;
}

struct Notifier {
    config: RwLock<Arc<NotificationConfig>>,
    /// enabled && 至少一个渠道启用；作为热路径快速判断
    active: AtomicBool,
    client: reqwest::Client,
    inspector: RwLock<Option<Arc<dyn TaskEventInspector>>>,
    recent: Mutex<HashMap<String, Instant>>,
}

static NOTIFIER: LazyLock<Notifier> = LazyLock::new(|| Notifier {
    config: RwLock::new(Arc::new(NotificationConfig::default())),
    active: AtomicBool::new(false),
    client: reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .user_agent(concat!("BaiduPCS-Rust/", env!("CARGO_PKG_VERSION")))
        .build()
        .unwrap_or_default(),
    inspector: RwLock::new(None),
    recent: Mutex::new(HashMap::new()),
});

/// 应用（热更新）通知配置；启动时与每次保存配置后调用
pub fn apply_config(cfg: &NotificationConfig) {
    let n = &*NOTIFIER;
    *n.config.write() = Arc::new(cfg.clone());
    n.active
        .store(cfg.enabled && cfg.has_active_channel(), Ordering::Release);
}

/// 当前生效的通知配置快照
pub fn current_config() -> Arc<NotificationConfig> {
    NOTIFIER.config.read().clone()
}

/// 注册事件上下文补充器
pub fn set_task_event_inspector(inspector: Arc<dyn TaskEventInspector>) {
    *NOTIFIER.inspector.write() = Some(inspector);
}

/// 是否会推送该类事件（总开关 + 渠道 + 事件开关）
pub fn is_kind_enabled(kind: NotificationKind) -> bool {
    let n = &*NOTIFIER;
    if !n.active.load(Ordering::Acquire) {
        return false;
    }
    n.config.read().events.is_enabled(kind)
}

/// 去重：同 key 在窗口内只放行一次
fn dedup_allows(key: &str) -> bool {
    let mut recent = NOTIFIER.recent.lock();
    let now = Instant::now();
    if recent.len() >= DEDUP_MAX_ENTRIES {
        recent.retain(|_, t| now.duration_since(*t) < DEDUP_WINDOW);
    }
    match recent.get(key) {
        Some(t) if now.duration_since(*t) < DEDUP_WINDOW => false,
        _ => {
            recent.insert(key.to_string(), now);
            true
        }
    }
}

/// 在当前 tokio 运行时中派生后台任务；无运行时（如同步单测）时直接丢弃
fn spawn_background<F>(fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(fut);
        }
        Err(_) => debug!("notification: 无 tokio 运行时，跳过推送"),
    }
}

/// 异步推送通知（fire-and-forget）。
///
/// 总开关关闭 / 事件开关关闭 / 命中去重时直接返回；发送失败只记日志。
pub fn notify(notification: Notification) {
    if !is_kind_enabled(notification.kind) {
        return;
    }
    if let Some(key) = notification.dedup_key.as_deref() {
        if !dedup_allows(key) {
            debug!("notification: 去重跳过 {}", key);
            return;
        }
    }
    let cfg = current_config();
    spawn_background(async move {
        dispatch_logged(&cfg, &notification).await;
    });
}

async fn dispatch_logged(cfg: &NotificationConfig, notification: &Notification) {
    for r in dispatch(cfg, notification).await {
        if r.success {
            debug!(
                "notification: {} 推送成功 ({})",
                r.channel,
                notification.kind.as_str()
            );
        } else {
            warn!(
                "notification: {} 推送失败 ({}): {}",
                r.channel,
                notification.kind.as_str(),
                r.error.as_deref().unwrap_or("unknown")
            );
        }
    }
}

/// 向所有已启用渠道发送（并发），返回每个渠道的结果
pub async fn dispatch(
    cfg: &NotificationConfig,
    notification: &Notification,
) -> Vec<ChannelSendResult> {
    let client = &NOTIFIER.client;
    let webhook = async {
        if cfg.webhook.enabled {
            Some(ChannelSendResult::from_result(
                "webhook",
                sender::send_webhook(client, &cfg.webhook, notification).await,
            ))
        } else {
            None
        }
    };
    let gotify = async {
        if cfg.gotify.enabled {
            Some(ChannelSendResult::from_result(
                "gotify",
                sender::send_gotify(client, &cfg.gotify, notification).await,
            ))
        } else {
            None
        }
    };
    let (w, g) = tokio::join!(webhook, gotify);
    w.into_iter().chain(g).collect()
}

impl ChannelSendResult {
    fn from_result(channel: &str, r: Result<(), String>) -> Self {
        match r {
            Ok(()) => Self {
                channel: channel.to_string(),
                success: true,
                error: None,
            },
            Err(e) => Self {
                channel: channel.to_string(),
                success: false,
                error: Some(e),
            },
        }
    }
}

/// 发送测试通知（同步等待结果，不受总开关 / 事件开关影响，只看渠道开关）
pub async fn send_test(cfg: &NotificationConfig) -> Vec<ChannelSendResult> {
    let n = Notification::new(
        NotificationKind::Test,
        None,
        "这是一条测试通知，收到说明通知渠道配置正确。",
    );
    dispatch(cfg, &n).await
}

/// 终态事件分类：返回 (事件类型, 任务 ID, 账号, 基础正文行)
fn classify_task_event(
    event: &TaskEvent,
) -> Option<(NotificationKind, String, Option<u64>, Vec<String>)> {
    match event {
        // 单文件下载：只通知独立任务；文件夹子任务由文件夹事件汇总，备份任务由备份事件汇总
        TaskEvent::Download(DownloadEvent::Completed {
            task_id,
            group_id: None,
            is_backup: false,
            owner_uid,
            ..
        }) => Some((
            NotificationKind::DownloadCompleted,
            task_id.clone(),
            *owner_uid,
            vec![],
        )),
        TaskEvent::Download(DownloadEvent::Failed {
            task_id,
            error,
            group_id: None,
            is_backup: false,
            owner_uid,
        }) => Some((
            NotificationKind::DownloadFailed,
            task_id.clone(),
            *owner_uid,
            vec![format!("错误：{}", error)],
        )),
        TaskEvent::Folder(FolderEvent::Completed {
            folder_id,
            owner_uid,
            ..
        }) => Some((
            NotificationKind::DownloadCompleted,
            folder_id.clone(),
            *owner_uid,
            vec![],
        )),
        TaskEvent::Folder(FolderEvent::Failed {
            folder_id,
            error,
            owner_uid,
        }) => Some((
            NotificationKind::DownloadFailed,
            folder_id.clone(),
            *owner_uid,
            vec![format!("错误：{}", error)],
        )),
        TaskEvent::Transfer(TransferEvent::Completed {
            task_id, owner_uid, ..
        }) => Some((
            NotificationKind::TransferCompleted,
            task_id.clone(),
            *owner_uid,
            vec![],
        )),
        TaskEvent::Transfer(TransferEvent::Failed {
            task_id,
            error,
            owner_uid,
            ..
        }) => Some((
            NotificationKind::TransferFailed,
            task_id.clone(),
            *owner_uid,
            vec![format!("错误：{}", error)],
        )),
        TaskEvent::Backup(BackupEvent::Completed {
            task_id,
            success_count,
            failed_count,
            skipped_count,
            owner_uid,
            ..
        }) => {
            // 没有任何文件变动的备份轮次（监听 / 定时轮询的空跑）不打扰用户
            if *success_count == 0 && *failed_count == 0 {
                return None;
            }
            Some((
                NotificationKind::BackupCompleted,
                task_id.clone(),
                *owner_uid,
                vec![format!(
                    "成功 {} 个，失败 {} 个，跳过 {} 个",
                    success_count, failed_count, skipped_count
                )],
            ))
        }
        TaskEvent::Backup(BackupEvent::Failed {
            task_id,
            error,
            owner_uid,
        }) => Some((
            NotificationKind::BackupFailed,
            task_id.clone(),
            *owner_uid,
            vec![format!("错误：{}", error)],
        )),
        _ => None,
    }
}

/// 旁路观察 WebSocket 任务事件（由 `WebSocketManager::send_if_subscribed` 调用）。
///
/// 同步部分只做廉价判断；名称反查与网络请求都在后台任务中完成。
pub fn observe_task_event(event: &TaskEvent) {
    if !NOTIFIER.active.load(Ordering::Acquire) {
        return;
    }
    let Some((kind, task_id, owner_uid, base_lines)) = classify_task_event(event) else {
        return;
    };
    if !is_kind_enabled(kind) {
        return;
    }
    let dedup_key = format!("{}:{}", kind.as_str(), task_id);
    if !dedup_allows(&dedup_key) {
        return;
    }
    let cfg = current_config();
    let inspector = NOTIFIER.inspector.read().clone();
    let event = event.clone();
    spawn_background(async move {
        let ctx = match inspector {
            Some(i) => i.inspect(kind, &event).await,
            None => EventContext::default(),
        };
        if ctx.skip {
            debug!("notification: 内部任务跳过 {}:{}", kind.as_str(), task_id);
            return;
        }
        let mut lines = ctx.details;
        lines.extend(base_lines);
        if let Some(uid) = owner_uid {
            lines.push(format!("账号 UID：{}", uid));
        }
        lines.push(format!("任务 ID：{}", task_id));
        let n = Notification::new(kind, ctx.name, lines.join("\n"))
            .with_task_id(task_id)
            .with_account(owner_uid);
        dispatch_logged(&cfg, &n).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_skips_group_and_backup_downloads() {
        let ev = TaskEvent::Download(DownloadEvent::Completed {
            task_id: "t".into(),
            completed_at: 0,
            group_id: Some("g".into()),
            is_backup: false,
            downloaded_size: 0,
            total_size: 0,
            owner_uid: None,
        });
        assert!(classify_task_event(&ev).is_none());
        let ev = TaskEvent::Download(DownloadEvent::Failed {
            task_id: "t".into(),
            error: "x".into(),
            group_id: None,
            is_backup: true,
            owner_uid: None,
        });
        assert!(classify_task_event(&ev).is_none());
        let ev = TaskEvent::Download(DownloadEvent::Failed {
            task_id: "t".into(),
            error: "x".into(),
            group_id: None,
            is_backup: false,
            owner_uid: Some(1),
        });
        let (kind, id, uid, _) = classify_task_event(&ev).unwrap();
        assert_eq!(kind, NotificationKind::DownloadFailed);
        assert_eq!(id, "t");
        assert_eq!(uid, Some(1));
    }

    #[test]
    fn test_classify_skips_empty_backup_round() {
        let ev = TaskEvent::Backup(BackupEvent::Completed {
            task_id: "b".into(),
            completed_at: 0,
            success_count: 0,
            failed_count: 0,
            skipped_count: 12,
            owner_uid: None,
        });
        assert!(classify_task_event(&ev).is_none());
    }

    #[test]
    fn test_notification_title() {
        let n = Notification::new(NotificationKind::TransferFailed, Some("电影".into()), "x");
        assert_eq!(n.title, "[BaiduPCS-Rust] 转存失败：电影");
        let n = Notification::new(NotificationKind::Test, None, "x");
        assert_eq!(n.title, "[BaiduPCS-Rust] 测试通知");
        assert_eq!(NotificationKind::ShareSyncLinkInvalid.level(), "warning");
    }

    #[test]
    fn test_dedup_window() {
        let key = format!("test:{}", uuid::Uuid::new_v4());
        assert!(dedup_allows(&key));
        assert!(!dedup_allows(&key));
    }

    #[test]
    fn test_notify_without_runtime_is_noop() {
        // 默认关闭：直接返回，不 panic
        notify(Notification::new(NotificationKind::Test, None, "x"));
    }
}
