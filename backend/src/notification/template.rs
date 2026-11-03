//! 通知模板渲染
//!
//! 支持占位符：`{{event}}` `{{event_label}}` `{{title}}` `{{message}}` `{{level}}` `{{name}}`
//! `{{task_id}}` `{{account_uid}}` `{{time}}` `{{timestamp}}`（允许 `{{ name }}` 形式的空白）。
//! 未知占位符原样保留。

use super::Notification;

/// 替换值的转义方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Escape {
    /// 原样替换
    None,
    /// JSON 字符串内容转义（不含两侧引号）
    Json,
    /// URL 百分号编码（URL 与 `application/x-www-form-urlencoded` 请求体）
    Url,
    /// HTTP 请求头值：换行等控制字符折叠为空格（`HeaderValue` 不接受 CR / LF，
    /// 而正文 `{{message}}` 是多行的）
    Header,
}

fn lookup(n: &Notification, key: &str) -> Option<String> {
    Some(match key {
        "event" => n.kind.as_str().to_string(),
        "event_label" => n.kind.label().to_string(),
        "title" => n.title.clone(),
        "message" => n.message.clone(),
        "level" => n.kind.level().to_string(),
        "name" => n.name.clone().unwrap_or_default(),
        "task_id" => n.task_id.clone().unwrap_or_default(),
        "account_uid" => n.account_uid.map(|u| u.to_string()).unwrap_or_default(),
        "time" => n.time.format("%Y-%m-%d %H:%M:%S").to_string(),
        "timestamp" => n.time.timestamp().to_string(),
        _ => return None,
    })
}

fn escape(value: &str, mode: Escape) -> String {
    match mode {
        Escape::None => value.to_string(),
        Escape::Json => {
            let quoted = serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string());
            quoted[1..quoted.len() - 1].to_string()
        }
        Escape::Url => {
            let mut out = String::with_capacity(value.len() * 3);
            for b in value.bytes() {
                if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                    out.push(b as char);
                } else {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
            out
        }
        Escape::Header => value
            .chars()
            .map(|c| if c.is_control() && c != '\t' { ' ' } else { c })
            .collect(),
    }
}

/// 渲染模板
pub fn render(template: &str, n: &Notification, mode: Escape) -> String {
    let mut out = String::with_capacity(template.len() + 64);
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let key = after[..end].trim();
                match lookup(n, key) {
                    Some(v) => out.push_str(&escape(&v, mode)),
                    None => {
                        out.push_str("{{");
                        out.push_str(&after[..end]);
                        out.push_str("}}");
                    }
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notification::{NotificationKind, DEFAULT_WEBHOOK_BODY_TEMPLATE};

    fn sample() -> Notification {
        Notification::new(
            NotificationKind::DownloadFailed,
            Some("a \"quoted\" name".into()),
            "line1\nline2",
        )
        .with_task_id("t-1")
        .with_account(Some(42))
    }

    #[test]
    fn test_default_template_is_valid_json() {
        let body = render(DEFAULT_WEBHOOK_BODY_TEMPLATE, &sample(), Escape::Json);
        let v: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        assert_eq!(v["event"], "download_failed");
        assert_eq!(v["name"], "a \"quoted\" name");
        assert_eq!(v["message"], "line1\nline2");
        assert_eq!(v["account_uid"], "42");
        assert_eq!(v["level"], "error");
        assert!(v["timestamp"].is_i64());
    }

    #[test]
    fn test_unknown_and_whitespace_placeholders() {
        let s = render(
            "{{ task_id }}|{{nope}}|{{unterminated",
            &sample(),
            Escape::None,
        );
        assert_eq!(s, "t-1|{{nope}}|{{unterminated");
    }

    #[test]
    fn test_header_escape_folds_newlines() {
        let s = render("X {{message}}", &sample(), Escape::Header);
        assert_eq!(s, "X line1 line2");
        assert!(reqwest::header::HeaderValue::from_str(&s).is_ok());
    }

    #[test]
    fn test_url_escape() {
        let s = render("https://x/?t={{name}}", &sample(), Escape::Url);
        assert_eq!(s, "https://x/?t=a%20%22quoted%22%20name");
    }
}
