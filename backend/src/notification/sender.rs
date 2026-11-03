//! 各渠道的 HTTP 发送实现
//!
//! 错误信息只包含状态码 / 截断后的响应体 / 去掉 URL 的 reqwest 错误，
//! 不会带出 Token、Webhook 地址或请求头。

use std::time::Duration;

use reqwest::header::{HeaderName, HeaderValue, CONTENT_TYPE};

use super::config::{GotifyChannelConfig, WebhookChannelConfig, MAX_TIMEOUT_SECS};
use super::template::{render, Escape};
use super::Notification;

/// 错误响应体最多保留的字符数
const MAX_ERROR_BODY_CHARS: usize = 200;
/// Gotify 请求超时
const GOTIFY_TIMEOUT: Duration = Duration::from_secs(10);

fn describe_reqwest_error(e: reqwest::Error) -> String {
    let kind = if e.is_timeout() {
        "请求超时"
    } else if e.is_connect() {
        "连接失败"
    } else {
        "请求失败"
    };
    format!("{}: {}", kind, e.without_url())
}

async fn check_response(resp: reqwest::Response) -> Result<(), String> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    let body: String = body.chars().take(MAX_ERROR_BODY_CHARS).collect();
    if body.trim().is_empty() {
        Err(format!("HTTP {}", status.as_u16()))
    } else {
        Err(format!("HTTP {}: {}", status.as_u16(), body.trim()))
    }
}

/// 发送通用 Webhook
pub async fn send_webhook(
    client: &reqwest::Client,
    cfg: &WebhookChannelConfig,
    n: &Notification,
) -> Result<(), String> {
    super::config::validate_http_url(&cfg.url).map_err(|e| format!("Webhook 地址{}", e))?;
    let url = render(cfg.url.trim(), n, Escape::Url);
    let method = cfg.method.trim().to_ascii_uppercase();
    let timeout = Duration::from_secs(cfg.timeout_secs.clamp(1, MAX_TIMEOUT_SECS));

    let mut req = match method.as_str() {
        "GET" => client.get(&url),
        "PUT" => client.put(&url),
        "POST" | "" => client.post(&url),
        other => return Err(format!("不支持的请求方法: {}", other)),
    }
    .timeout(timeout);

    for h in &cfg.headers {
        let name = h.name.trim();
        if name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| format!("请求头名称不合法: {}", name))?;
        let value = render(&h.value, n, Escape::Header);
        // 值可能含敏感信息，错误里只报头名
        let value = HeaderValue::from_str(&value)
            .map_err(|_| format!("请求头 {} 的值不合法", name.as_str()))?;
        req = req.header(name, value);
    }

    if method != "GET" {
        let content_type = if cfg.content_type.trim().is_empty() {
            "application/json"
        } else {
            cfg.content_type.trim()
        };
        let ct = content_type.to_ascii_lowercase();
        let mode = if ct.contains("json") {
            Escape::Json
        } else if ct.contains("x-www-form-urlencoded") {
            Escape::Url
        } else {
            Escape::None
        };
        let body = render(&cfg.body_template, n, mode);
        let has_custom_ct = cfg
            .headers
            .iter()
            .any(|h| h.name.trim().eq_ignore_ascii_case("content-type"));
        if !has_custom_ct {
            req = req.header(CONTENT_TYPE, content_type);
        }
        req = req.body(body);
    }

    let resp = req.send().await.map_err(describe_reqwest_error)?;
    check_response(resp).await
}

/// 发送 Gotify 消息：`POST {server}/message`，Token 走 `X-Gotify-Key` 请求头
pub async fn send_gotify(
    client: &reqwest::Client,
    cfg: &GotifyChannelConfig,
    n: &Notification,
) -> Result<(), String> {
    super::config::validate_http_url(&cfg.server_url)
        .map_err(|e| format!("Gotify 服务地址{}", e))?;
    let token = cfg.app_token.trim();
    if token.is_empty() {
        return Err("Gotify App Token 为空".into());
    }
    let url = format!("{}/message", cfg.server_url.trim().trim_end_matches('/'));
    let token =
        HeaderValue::from_str(token).map_err(|_| "Gotify App Token 含非法字符".to_string())?;
    let body = serde_json::json!({
        "title": n.title,
        "message": n.message,
        "priority": cfg.priority.min(10),
        "extras": {
            "client::display": { "contentType": "text/plain" },
            "baidupcs::event": {
                "event": n.kind.as_str(),
                "level": n.kind.level(),
                "task_id": n.task_id,
                "account_uid": n.account_uid,
            }
        }
    });
    let resp = client
        .post(&url)
        .header("X-Gotify-Key", token)
        .json(&body)
        .timeout(GOTIFY_TIMEOUT)
        .send()
        .await
        .map_err(describe_reqwest_error)?;
    check_response(resp).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notification::NotificationKind;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 启动一个只接受一次请求的本地 HTTP 服务，返回 (地址, 原始请求文本)
    async fn one_shot_server(
        status_line: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            loop {
                let n = sock.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                let text = String::from_utf8_lossy(&buf);
                if let Some(idx) = text.find("\r\n\r\n") {
                    let len = text[..idx]
                        .lines()
                        .find_map(|l| {
                            let l = l.to_ascii_lowercase();
                            l.strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if buf.len() >= idx + 4 + len {
                        break;
                    }
                }
            }
            let resp = format!(
                "{}\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                status_line
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf).to_string()
        });
        (format!("http://{}", addr), handle)
    }

    #[tokio::test]
    async fn test_gotify_sends_token_header_and_json() {
        let (base, handle) = one_shot_server("HTTP/1.1 200 OK").await;
        let cfg = GotifyChannelConfig {
            enabled: true,
            server_url: format!("{}/", base),
            app_token: "secret-token".into(),
            priority: 7,
        };
        let n = Notification::new(NotificationKind::Test, None, "hello");
        send_gotify(&reqwest::Client::new(), &cfg, &n)
            .await
            .unwrap();
        let req = handle.await.unwrap();
        assert!(req.starts_with("POST /message "));
        assert!(req
            .to_ascii_lowercase()
            .contains("x-gotify-key: secret-token"));
        assert!(req.contains("\"priority\":7"));
        assert!(req.contains("hello"));
    }

    #[tokio::test]
    async fn test_webhook_renders_body_and_reports_status() {
        let (base, handle) = one_shot_server("HTTP/1.1 500 Internal Server Error").await;
        let cfg = WebhookChannelConfig {
            enabled: true,
            url: format!("{}/hook?e={{{{event}}}}", base),
            method: "PUT".into(),
            headers: vec![crate::notification::WebhookHeader {
                name: "X-Custom".into(),
                value: "v-{{level}}".into(),
            }],
            content_type: "application/json".into(),
            body_template: r#"{"m":"{{message}}"}"#.into(),
            timeout_secs: 5,
        };
        let n = Notification::new(NotificationKind::DownloadFailed, None, "a\"b");
        let err = send_webhook(&reqwest::Client::new(), &cfg, &n)
            .await
            .unwrap_err();
        assert!(err.starts_with("HTTP 500"), "{}", err);
        let req = handle.await.unwrap();
        assert!(req.starts_with("PUT /hook?e=download_failed "), "{}", req);
        assert!(req.to_ascii_lowercase().contains("x-custom: v-error"));
        assert!(req.contains(r#"{"m":"a\"b"}"#));
    }

    /// 多行正文用在请求头里：换行折叠为空格，不能让整条 Webhook 因非法头值失败；
    /// form 请求体按 URL 编码
    #[tokio::test]
    async fn test_webhook_multiline_header_and_form_body() {
        let (base, handle) = one_shot_server("HTTP/1.1 200 OK").await;
        let cfg = WebhookChannelConfig {
            enabled: true,
            url: format!("{}/hook", base),
            method: "POST".into(),
            headers: vec![crate::notification::WebhookHeader {
                name: "X-Message".into(),
                value: "{{message}}".into(),
            }],
            content_type: "application/x-www-form-urlencoded".into(),
            body_template: "title={{name}}&message={{message}}".into(),
            timeout_secs: 5,
        };
        let n = Notification::new(
            NotificationKind::DownloadFailed,
            Some("a&b=c".into()),
            "line1\nline2",
        );
        send_webhook(&reqwest::Client::new(), &cfg, &n)
            .await
            .unwrap();
        let req = handle.await.unwrap();
        assert!(
            req.to_ascii_lowercase().contains("x-message: line1 line2"),
            "{}",
            req
        );
        assert!(
            req.ends_with("title=a%26b%3Dc&message=line1%0Aline2"),
            "{}",
            req
        );
    }

    #[tokio::test]
    async fn test_connect_error_does_not_leak_url() {
        let cfg = WebhookChannelConfig {
            enabled: true,
            url: "http://127.0.0.1:1/secret-path-token".into(),
            ..Default::default()
        };
        let n = Notification::new(NotificationKind::Test, None, "x");
        let err = send_webhook(&reqwest::Client::new(), &cfg, &n)
            .await
            .unwrap_err();
        assert!(!err.contains("secret-path-token"), "{}", err);
    }
}
