//! 消息通知配置
//!
//! 保存在 `config/app.toml` 的 `[notification]` 段；老配置缺该段时整体按默认值
//! （总开关关闭）反序列化，不影响启动。

use serde::{Deserialize, Serialize};

use super::NotificationKind;

/// 通用 Webhook 默认请求体模板（JSON）。
///
/// 占位符写在 JSON 字符串里时会按 JSON 规则转义，可放心包含引号 / 换行。
pub const DEFAULT_WEBHOOK_BODY_TEMPLATE: &str = r#"{
  "event": "{{event}}",
  "title": "{{title}}",
  "message": "{{message}}",
  "level": "{{level}}",
  "name": "{{name}}",
  "task_id": "{{task_id}}",
  "account_uid": "{{account_uid}}",
  "time": "{{time}}",
  "timestamp": {{timestamp}}
}"#;

/// 单次推送请求超时上限（秒）
pub const MAX_TIMEOUT_SECS: u64 = 60;

/// 消息通知配置
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationConfig {
    /// 总开关；关闭时任何事件都不推送（测试通知不受影响）
    #[serde(default)]
    pub enabled: bool,
    /// 通用 Webhook 渠道
    #[serde(default)]
    pub webhook: WebhookChannelConfig,
    /// Gotify 渠道
    #[serde(default)]
    pub gotify: GotifyChannelConfig,
    /// 触发事件开关
    #[serde(default)]
    pub events: NotificationEventToggles,
}

/// 自定义请求头（`Debug` 输出会隐去值，避免鉴权头进日志）
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookHeader {
    pub name: String,
    #[serde(default)]
    pub value: String,
}

/// 通用 Webhook 渠道配置（`Debug` 输出隐去 URL 与请求头值：URL 里常带推送 Key）
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookChannelConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 目标地址（http / https）；URL 中同样支持占位符（按 URL 编码替换）
    #[serde(default)]
    pub url: String,
    /// 请求方法：`POST`（默认）/ `PUT` / `GET`。GET 不发送请求体。
    #[serde(default = "default_webhook_method")]
    pub method: String,
    /// 自定义请求头（值支持占位符，换行等控制字符折叠为空格）
    #[serde(default)]
    pub headers: Vec<WebhookHeader>,
    /// 请求体 Content-Type；含 `json` 时占位符按 JSON 字符串规则转义，
    /// 含 `x-www-form-urlencoded` 时按 URL 编码
    #[serde(default = "default_webhook_content_type")]
    pub content_type: String,
    /// 请求体模板
    #[serde(default = "default_webhook_body_template")]
    pub body_template: String,
    /// 请求超时（秒）
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

/// 日志里替代敏感值的占位
fn redacted(v: &str) -> &'static str {
    if v.is_empty() {
        ""
    } else {
        "<redacted>"
    }
}

impl std::fmt::Debug for WebhookHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookHeader")
            .field("name", &self.name)
            .field("value", &redacted(&self.value))
            .finish()
    }
}

impl std::fmt::Debug for WebhookChannelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookChannelConfig")
            .field("enabled", &self.enabled)
            .field("url", &redacted(&self.url))
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("content_type", &self.content_type)
            .field("body_template_len", &self.body_template.len())
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

impl std::fmt::Debug for GotifyChannelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GotifyChannelConfig")
            .field("enabled", &self.enabled)
            .field("server_url", &self.server_url)
            .field("app_token", &redacted(&self.app_token))
            .field("priority", &self.priority)
            .finish()
    }
}

impl Default for WebhookChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            method: default_webhook_method(),
            headers: Vec::new(),
            content_type: default_webhook_content_type(),
            body_template: default_webhook_body_template(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

/// Gotify 渠道配置（`Debug` 输出隐去 App Token）
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GotifyChannelConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Gotify 服务地址，如 `https://gotify.example.com`
    #[serde(default)]
    pub server_url: String,
    /// 应用 Token（App Token）
    #[serde(default)]
    pub app_token: String,
    /// 消息优先级（0-10，默认 5）
    #[serde(default = "default_gotify_priority")]
    pub priority: u8,
}

impl Default for GotifyChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_url: String::new(),
            app_token: String::new(),
            priority: default_gotify_priority(),
        }
    }
}

/// 触发事件开关（缺失字段默认开启，由总开关兜底）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationEventToggles {
    /// 分享同步：发现更新并完成转存 / 下载
    #[serde(default = "default_true")]
    pub share_sync_updated: bool,
    /// 分享同步：运行失败
    #[serde(default = "default_true")]
    pub share_sync_failed: bool,
    /// 分享同步：链接失效已自动暂停
    #[serde(default = "default_true")]
    pub share_sync_link_invalid: bool,
    /// 转存完成
    #[serde(default = "default_true")]
    pub transfer_completed: bool,
    /// 转存失败
    #[serde(default = "default_true")]
    pub transfer_failed: bool,
    /// 下载完成（单文件 / 文件夹）
    #[serde(default = "default_true")]
    pub download_completed: bool,
    /// 下载失败
    #[serde(default = "default_true")]
    pub download_failed: bool,
    /// 自动备份完成
    #[serde(default = "default_true")]
    pub backup_completed: bool,
    /// 自动备份失败
    #[serde(default = "default_true")]
    pub backup_failed: bool,
}

impl Default for NotificationEventToggles {
    fn default() -> Self {
        Self {
            share_sync_updated: true,
            share_sync_failed: true,
            share_sync_link_invalid: true,
            transfer_completed: true,
            transfer_failed: true,
            download_completed: true,
            download_failed: true,
            backup_completed: true,
            backup_failed: true,
        }
    }
}

impl NotificationEventToggles {
    /// 某类事件是否开启（测试通知恒为开启）
    pub fn is_enabled(&self, kind: NotificationKind) -> bool {
        match kind {
            NotificationKind::Test => true,
            NotificationKind::ShareSyncUpdated => self.share_sync_updated,
            NotificationKind::ShareSyncFailed => self.share_sync_failed,
            NotificationKind::ShareSyncLinkInvalid => self.share_sync_link_invalid,
            NotificationKind::TransferCompleted => self.transfer_completed,
            NotificationKind::TransferFailed => self.transfer_failed,
            NotificationKind::DownloadCompleted => self.download_completed,
            NotificationKind::DownloadFailed => self.download_failed,
            NotificationKind::BackupCompleted => self.backup_completed,
            NotificationKind::BackupFailed => self.backup_failed,
        }
    }
}

impl NotificationConfig {
    /// 至少有一个渠道启用
    pub fn has_active_channel(&self) -> bool {
        self.webhook.enabled || self.gotify.enabled
    }

    /// 校验配置（保存时调用）。只校验已启用的渠道，未启用的渠道允许留空。
    pub fn validate(&self) -> Result<(), String> {
        if self.webhook.enabled {
            validate_http_url(&self.webhook.url).map_err(|e| format!("Webhook 地址{}", e))?;
            let method = self.webhook.method.trim().to_ascii_uppercase();
            if !matches!(method.as_str(), "POST" | "PUT" | "GET") {
                return Err(format!(
                    "Webhook 请求方法仅支持 POST / PUT / GET（当前: {}）",
                    self.webhook.method
                ));
            }
            for h in &self.webhook.headers {
                let name = h.name.trim();
                if name.is_empty() {
                    return Err("Webhook 请求头名称不能为空".into());
                }
                if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err() {
                    return Err(format!("Webhook 请求头名称不合法: {}", name));
                }
            }
            if self.webhook.timeout_secs == 0 || self.webhook.timeout_secs > MAX_TIMEOUT_SECS {
                return Err(format!(
                    "Webhook 超时时间需在 1-{} 秒之间",
                    MAX_TIMEOUT_SECS
                ));
            }
        }
        if self.gotify.enabled {
            validate_http_url(&self.gotify.server_url)
                .map_err(|e| format!("Gotify 服务地址{}", e))?;
            if self.gotify.app_token.trim().is_empty() {
                return Err("Gotify App Token 不能为空".into());
            }
            if self.gotify.priority > 10 {
                return Err("Gotify 优先级需在 0-10 之间".into());
            }
        }
        Ok(())
    }
}

/// 只允许 http / https，且必须带主机名
pub(crate) fn validate_http_url(url: &str) -> Result<(), String> {
    let url = url.trim();
    if url.is_empty() {
        return Err("不能为空".into());
    }
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| "必须以 http:// 或 https:// 开头".to_string())?;
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return Err("缺少主机名".into());
    }
    Ok(())
}

fn default_true() -> bool {
    true
}

fn default_webhook_method() -> String {
    "POST".to_string()
}

fn default_webhook_content_type() -> String {
    "application/json".to_string()
}

fn default_webhook_body_template() -> String {
    DEFAULT_WEBHOOK_BODY_TEMPLATE.to_string()
}

fn default_timeout_secs() -> u64 {
    10
}

fn default_gotify_priority() -> u8 {
    5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_section_defaults_disabled() {
        let cfg: NotificationConfig = serde_json::from_str("{}").unwrap();
        assert!(!cfg.enabled);
        assert!(!cfg.webhook.enabled);
        assert_eq!(cfg.webhook.method, "POST");
        assert_eq!(cfg.gotify.priority, 5);
        assert!(cfg.events.share_sync_link_invalid);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_toml_roundtrip() {
        let cfg = NotificationConfig {
            enabled: true,
            gotify: GotifyChannelConfig {
                enabled: true,
                server_url: "http://127.0.0.1:8080".into(),
                app_token: "Axxxx".into(),
                priority: 5,
            },
            events: NotificationEventToggles {
                download_completed: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let s = toml::to_string(&cfg).unwrap();
        let back: NotificationConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn test_validate_enabled_channels() {
        let mut cfg = NotificationConfig::default();
        cfg.webhook.enabled = true;
        assert!(cfg.validate().is_err());
        cfg.webhook.url = "ftp://x".into();
        assert!(cfg.validate().is_err());
        cfg.webhook.url = "https://example.com/hook".into();
        assert!(cfg.validate().is_ok());
        cfg.webhook.method = "DELETE".into();
        assert!(cfg.validate().is_err());
        cfg.webhook.method = "put".into();
        cfg.webhook.headers.push(WebhookHeader {
            name: "bad header".into(),
            value: "x".into(),
        });
        assert!(cfg.validate().is_err());
        cfg.webhook.headers.clear();
        assert!(cfg.validate().is_ok());

        cfg.gotify.enabled = true;
        cfg.gotify.server_url = "https://gotify.example.com".into();
        assert!(cfg.validate().is_err(), "token 为空应报错");
        cfg.gotify.app_token = "tok".into();
        assert!(cfg.validate().is_ok());
        cfg.gotify.priority = 11;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_debug_output_redacts_secrets() {
        let mut cfg = NotificationConfig::default();
        cfg.gotify.app_token = "SECRET-TOKEN".into();
        cfg.webhook.url = "https://api.day.app/SECRET-KEY/{title}".into();
        cfg.webhook.headers = vec![WebhookHeader {
            name: "Authorization".into(),
            value: "Bearer SECRET-BEARER".into(),
        }];
        let dbg = format!("{:?}", cfg);
        assert!(!dbg.contains("SECRET"), "Debug 输出泄露了敏感值: {}", dbg);
        assert!(dbg.contains("Authorization"));
        assert!(dbg.contains("<redacted>"));
    }
}
