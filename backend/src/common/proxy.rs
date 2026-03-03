use anyhow::{Context, Result};
use reqwest::Url;
use reqwest::{ClientBuilder, Proxy};
use serde::{Deserialize, Serialize};

/// 代理类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyType {
    /// 不使用代理
    None,
    /// HTTP 代理
    #[serde(alias = "https")]
    Http,
    /// SOCKS5 代理
    #[serde(alias = "socks5h")]
    Socks5,
}

impl Default for ProxyType {
    fn default() -> Self {
        Self::None
    }
}

/// 代理生效范围
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyScope {
    /// 默认模式：代理作用于所有网络请求（当前行为）
    Default,
    /// 仅代理上传/下载相关逻辑
    TransferOnly,
}

impl Default for ProxyScope {
    fn default() -> Self {
        Self::Default
    }
}

fn default_temporary_fallback_probe_interval_secs() -> u64 {
    20
}

/// 网络代理配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// 代理类型
    #[serde(default)]
    pub proxy_type: ProxyType,
    /// 代理服务器地址（IP/域名）
    #[serde(default)]
    pub host: String,
    /// 代理端口
    #[serde(default)]
    pub port: u16,
    /// 代理认证用户名（可选）
    #[serde(default)]
    pub username: String,
    /// 代理认证密码（可选）
    #[serde(default)]
    pub password: String,
    /// 代理生效范围
    #[serde(default)]
    pub scope: ProxyScope,
    /// 启用后，当代理链路出现网络错误时允许临时回退到直连
    #[serde(default)]
    pub temporary_fallback: bool,
    /// 临时 fallback 下，代理恢复探测间隔（秒，允许范围 3~60）
    #[serde(default = "default_temporary_fallback_probe_interval_secs")]
    pub temporary_fallback_probe_interval_secs: u64,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            proxy_type: ProxyType::None,
            host: String::new(),
            port: 0,
            username: String::new(),
            password: String::new(),
            scope: ProxyScope::Default,
            temporary_fallback: false,
            temporary_fallback_probe_interval_secs: default_temporary_fallback_probe_interval_secs(
            ),
        }
    }
}

impl ProxyConfig {
    /// 是否启用代理
    pub fn is_enabled(&self) -> bool {
        self.proxy_type != ProxyType::None
    }

    /// 面向 API 类请求（登录、列表、元信息等）的有效代理配置
    pub fn for_api(&self) -> Self {
        if self.scope == ProxyScope::TransferOnly {
            let mut cfg = self.clone();
            cfg.proxy_type = ProxyType::None;
            cfg
        } else {
            self.clone()
        }
    }

    /// 面向上传/下载数据传输链路的有效代理配置
    pub fn for_transfer(&self) -> Self {
        self.clone()
    }

    /// 临时 fallback 代理探测间隔（秒，自动夹取到 3~60）
    pub fn temporary_fallback_probe_interval_secs(&self) -> u64 {
        self.temporary_fallback_probe_interval_secs.clamp(3, 60)
    }

    /// 构建 reqwest Proxy
    pub fn to_reqwest_proxy(&self) -> Result<Option<Proxy>> {
        if !self.is_enabled() {
            return Ok(None);
        }

        let host = self.host.trim();
        if host.is_empty() {
            anyhow::bail!("代理已启用，但代理地址不能为空");
        }

        if self.port == 0 {
            anyhow::bail!("代理已启用，但代理端口必须在 1-65535 范围内");
        }

        let username = self.username.trim();
        let password = self.password.trim();

        let host = normalize_proxy_host(host);
        let proxy_url = build_proxy_url(
            self.proxy_type.clone(),
            &host,
            self.port,
            username,
            password,
        )?;

        let mut proxy =
            Proxy::all(&proxy_url).with_context(|| format!("创建代理失败: {proxy_url}"))?;

        // HTTP 代理鉴权使用 Proxy-Authorization 头。
        // SOCKS5 用户名/密码已放入 URL，会在 SOCKS5 握手阶段完成鉴权。
        if self.proxy_type == ProxyType::Http && !username.is_empty() {
            proxy = proxy.basic_auth(username, password);
        }

        Ok(Some(proxy))
    }

    /// 将代理应用到 reqwest builder
    pub fn apply_to_builder(&self, builder: ClientBuilder) -> Result<ClientBuilder> {
        if let Some(proxy) = self.to_reqwest_proxy()? {
            Ok(builder.proxy(proxy))
        } else {
            Ok(builder.no_proxy())
        }
    }
}

fn build_proxy_url(
    proxy_type: ProxyType,
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<String> {
    let scheme = match proxy_type {
        ProxyType::None => return Ok(String::new()),
        ProxyType::Http => "http",
        ProxyType::Socks5 => "socks5h",
    };

    let mut url = Url::parse(&format!("{scheme}://{host}:{port}"))
        .with_context(|| format!("创建代理 URL 失败: {scheme}://{host}:{port}"))?;

    if proxy_type == ProxyType::Socks5 && !username.is_empty() {
        url.set_username(username)
            .map_err(|_| anyhow::anyhow!("SOCKS5 用户名包含非法字符"))?;
        url.set_password(Some(password))
            .map_err(|_| anyhow::anyhow!("SOCKS5 密码包含非法字符"))?;
    }

    Ok(url.to_string())
}

fn normalize_proxy_host(host: &str) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{build_proxy_url, normalize_proxy_host, ProxyConfig, ProxyType};

    #[test]
    fn normalize_ipv6_host() {
        assert_eq!(normalize_proxy_host("::1"), "[::1]");
        assert_eq!(normalize_proxy_host("[::1]"), "[::1]");
    }

    #[test]
    fn socks5_proxy_url_contains_auth_in_url() {
        let url = build_proxy_url(ProxyType::Socks5, "127.0.0.1", 1080, "user", "p@ss")
            .expect("build socks5 proxy url");
        assert_eq!(url, "socks5h://user:p%40ss@127.0.0.1:1080");
    }

    #[test]
    fn proxy_type_supports_legacy_alias_values() {
        let http_cfg: ProxyConfig = toml::from_str(
            r#"proxy_type = "https"
host = "127.0.0.1"
port = 8080
"#,
        )
        .expect("parse legacy http proxy_type");
        assert_eq!(http_cfg.proxy_type, ProxyType::Http);

        let socks_cfg: ProxyConfig = toml::from_str(
            r#"proxy_type = "socks5h"
host = "127.0.0.1"
port = 1080
"#,
        )
        .expect("parse legacy socks5 proxy_type");
        assert_eq!(socks_cfg.proxy_type, ProxyType::Socks5);
    }
}
