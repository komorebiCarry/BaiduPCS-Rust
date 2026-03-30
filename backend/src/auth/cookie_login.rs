// Cookie 登录功能实现

use crate::auth::constants::{API_USER_INFO, BAIDU_APP_ID, CLIENT_TYPE, USER_AGENT};
use crate::auth::UserAuth;
use crate::common::ProxyConfig;
use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{info, warn};

/// 从原始 Cookie 字符串解析各字段
///
/// 支持格式:
/// - `"BDUSS=xxx; PTOKEN=yyy; STOKEN=zzz"`（浏览器标准格式）
/// - 每行一个 `NAME=VALUE`
/// - 分号分隔但无空格
pub fn parse_cookie_string(raw: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();

    let pairs: Vec<&str> = if raw.contains("; ") {
        raw.split("; ").collect()
    } else if raw.contains(';') {
        raw.split(';').collect()
    } else if raw.contains('\n') {
        raw.split('\n').collect()
    } else {
        vec![raw]
    };

    for pair in pairs {
        let pair = pair.trim();
        if let Some((name, value)) = pair.split_once('=') {
            let name = name.trim().to_string();
            // Cookie value 可能含 '='（base64），只取第一个 '=' 前的 name
            let value = value.trim().to_string();
            if !name.is_empty() && !value.is_empty() {
                map.insert(name, value);
            }
        }
    }

    map
}

/// Cookie 登录客户端
pub struct CookieLoginAuth {
    client: Client,
}

impl CookieLoginAuth {
    /// 创建新的 Cookie 登录客户端
    pub fn new() -> Result<Self> {
        Self::new_with_proxy(None)
    }

    /// 创建新的 Cookie 登录客户端（支持代理配置）
    pub fn new_with_proxy(proxy_config: Option<&ProxyConfig>) -> Result<Self> {
        let mut builder = Client::builder()
            .cookie_store(false)
            .user_agent(USER_AGENT);

        if let Some(proxy) = proxy_config {
            builder = proxy.apply_to_builder(builder)?;
        }

        let client = builder.build().context("Failed to create HTTP client")?;

        Ok(Self { client })
    }

    /// 使用原始 Cookie 字符串登录
    ///
    /// BDUSS 必填；PTOKEN 强烈建议（缺失则登录后跳过预热）。
    /// 返回填充了所有可用字段的 `UserAuth`。
    pub async fn login_with_cookies(&self, raw_cookies: &str) -> Result<UserAuth> {
        let map = parse_cookie_string(raw_cookies);

        let bduss = map
            .get("BDUSS")
            .cloned()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("Cookie 中缺少必填字段 BDUSS，请确认完整粘贴了 Cookie 字符串"))?;

        let ptoken = map.get("PTOKEN").cloned().filter(|s| !s.is_empty());
        if ptoken.is_none() {
            warn!("Cookie 中未找到 PTOKEN，登录后 Web 接口（预热/CSRF）将不可用");
        }

        let stoken = map.get("STOKEN").cloned().filter(|s| !s.is_empty());
        let baiduid = map.get("BAIDUID").cloned().filter(|s| !s.is_empty());
        let passid = map.get("PASSID").cloned().filter(|s| !s.is_empty());
        // 浏览器已登录状态下 Cookie 中通常携带这些预热令牌，直接复用可跳过重新预热
        let panpsc = map.get("PANPSC").cloned().filter(|s| !s.is_empty());
        let csrf_token = map.get("csrfToken")
            .or_else(|| map.get("csrftoken"))
            .or_else(|| map.get("CSRFTOKEN"))
            .cloned()
            .filter(|s| !s.is_empty());

        info!(
            "Cookie 登录解析结果: BDUSS={}字符, PTOKEN={}, STOKEN={}, BAIDUID={}, PANPSC={}, csrfToken={}",
            bduss.len(),
            if ptoken.is_some() { "已设置" } else { "未设置" },
            if stoken.is_some() { "已设置" } else { "未设置" },
            if baiduid.is_some() { "已设置" } else { "未设置" },
            if panpsc.is_some() { "已设置" } else { "未设置" },
            if csrf_token.is_some() { "已设置" } else { "未设置" },
        );

        // 用 BDUSS 获取用户信息（同时验证 BDUSS 有效性）
        let mut user = self
            .get_user_info(&bduss)
            .await
            .context("BDUSS 验证失败，请检查 Cookie 是否完整且未过期")?;

        // 填充其他可用 Cookie 字段
        user.ptoken = ptoken;
        user.stoken = stoken;
        user.baiduid = baiduid;
        user.passid = passid;
        user.panpsc = panpsc;
        user.csrf_token = csrf_token;
        // bdstoken 是 API 响应体字段，不作为 Cookie 传输，此处不提取
        user.cookies = Some(raw_cookies.to_string());

        info!(
            "Cookie 登录成功: UID={}, 用户名={}",
            user.uid, user.username
        );

        Ok(user)
    }

    /// 通过百度网盘 API 获取并验证用户信息
    async fn get_user_info(&self, bduss: &str) -> Result<UserAuth> {
        let url = format!(
            "{}?method=query&clienttype={}&app_id={}&web=1",
            API_USER_INFO, CLIENT_TYPE, BAIDU_APP_ID
        );

        let resp = self
            .client
            .get(&url)
            .header("Cookie", format!("BDUSS={}", bduss))
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .context("Failed to fetch user info")?;

        let json: Value = resp.json().await.context("Failed to parse user info")?;

        info!(
            "网盘API返回: {}",
            serde_json::to_string_pretty(&json).unwrap_or_default()
        );

        // 非零 errno 表示 BDUSS 无效或已过期
        let errno = json["errno"].as_i64().unwrap_or(0);
        if errno != 0 {
            return Err(anyhow!(
                "BDUSS 无效（API errno={}），请重新从浏览器获取最新 Cookie",
                errno
            ));
        }

        let user_info = &json["user_info"];

        let username = user_info["username"]
            .as_str()
            .or_else(|| user_info["baidu_name"].as_str())
            .or_else(|| user_info["netdisk_name"].as_str())
            .unwrap_or("未知用户")
            .to_string();

        let nickname = user_info["username"].as_str().map(|s| s.to_string());

        let uid = user_info["uk"]
            .as_u64()
            .or_else(|| user_info["user_id"].as_u64())
            .unwrap_or(0);

        let avatar_url = user_info["photo"]
            .as_str()
            .or_else(|| user_info["avatar_url"].as_str())
            .map(|s| s.to_string());

        let vip_type = if user_info["is_svip"].as_i64().unwrap_or(0) == 1 {
            Some(2)
        } else if user_info["is_vip"].as_i64().unwrap_or(0) == 1 {
            Some(1)
        } else {
            Some(0)
        };

        let total_space = json["total"].as_u64();
        let used_space = json["used"].as_u64();

        Ok(UserAuth::new_with_details(
            uid,
            username,
            bduss.to_string(),
            nickname,
            avatar_url,
            vip_type,
            total_space,
            used_space,
        ))
    }
}

impl Default for CookieLoginAuth {
    fn default() -> Self {
        Self::new().expect("Failed to create CookieLoginAuth")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cookie_string_semicolon_space() {
        let raw = "BDUSS=abc123; PTOKEN=ppp; STOKEN=sss; BAIDUID=bbb";
        let map = parse_cookie_string(raw);
        assert_eq!(map.get("BDUSS").map(|s| s.as_str()), Some("abc123"));
        assert_eq!(map.get("PTOKEN").map(|s| s.as_str()), Some("ppp"));
        assert_eq!(map.get("STOKEN").map(|s| s.as_str()), Some("sss"));
        assert_eq!(map.get("BAIDUID").map(|s| s.as_str()), Some("bbb"));
    }

    #[test]
    fn test_parse_cookie_string_newline() {
        let raw = "BDUSS=abc123\nPTOKEN=ppp\nSTOKEN=sss";
        let map = parse_cookie_string(raw);
        assert_eq!(map.get("BDUSS").map(|s| s.as_str()), Some("abc123"));
        assert_eq!(map.get("PTOKEN").map(|s| s.as_str()), Some("ppp"));
    }

    #[test]
    fn test_parse_cookie_missing_bduss() {
        let raw = "PTOKEN=ppp; STOKEN=sss";
        let map = parse_cookie_string(raw);
        assert!(map.get("BDUSS").is_none());
    }
}
