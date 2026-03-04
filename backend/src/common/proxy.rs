//! 代理配置与构建模块
//!
//! 负责将用户配置转换为 `reqwest::Proxy` 对象，
//! 提供统一的 `apply_to_builder` 方法供所有 HTTP 客户端使用。

use anyhow::{bail, Result};
use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};

/// 代理类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProxyType {
    None,
    Http,
    Socks5,
}

impl Default for ProxyType {
    fn default() -> Self {
        ProxyType::None
    }
}

/// 代理配置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyConfig {
    #[serde(default)]
    pub proxy_type: ProxyType,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// 是否允许代理故障时自动回退到直连（默认 true）
    /// 设为 false 适用于内网环境中直连本身不通的场景
    #[serde(default = "default_allow_fallback")]
    pub allow_fallback: bool,
}

fn default_allow_fallback() -> bool {
    true
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            proxy_type: ProxyType::None,
            host: String::new(),
            port: 0,
            username: None,
            password: None,
            allow_fallback: true,
        }
    }
}

impl ProxyConfig {
    /// 规范化可选字符串：`Some("")` 视为 `None`，`Some(s)` 去除首尾空白
    fn normalize_optional(s: &Option<String>) -> Option<&str> {
        s.as_deref().map(|v| v.trim()).filter(|v| !v.is_empty())
    }

    /// 规范化主机地址：检测 IPv6 地址（包含冒号）并添加方括号
    pub fn normalize_host(&self) -> String {
        let host = self.host.trim();
        if host.contains(':') && !host.starts_with('[') {
            format!("[{}]", host)
        } else {
            host.to_string()
        }
    }

    /// 验证代理配置
    ///
    /// - 启用代理时：host 不能为空（trim 后）、port 不能为 0
    /// - username/password 必须同时提供或同时为空（`Some("")` 视为 `None`）
    pub fn validate(&self) -> Result<()> {
        if self.proxy_type == ProxyType::None {
            return Ok(());
        }

        if self.host.trim().is_empty() {
            bail!("代理主机地址不能为空");
        }
        if self.port == 0 {
            bail!("代理端口不能为 0");
        }

        let has_user = Self::normalize_optional(&self.username).is_some();
        let has_pass = Self::normalize_optional(&self.password).is_some();
        if has_user != has_pass {
            bail!("用户名和密码必须同时提供或同时为空");
        }

        Ok(())
    }

    /// 构建代理 URL 字符串
    ///
    /// - HTTP: `http://host:port`（认证通过 `basic_auth` 设置，不嵌入 URL）
    /// - SOCKS5: `socks5h://[user:pass@]host:port`（认证嵌入 URL）
    pub fn build_proxy_url(&self) -> Result<String> {
        let scheme = match self.proxy_type {
            ProxyType::None => bail!("proxy_type 为 None，无法构建代理 URL"),
            ProxyType::Http => "http",
            ProxyType::Socks5 => "socks5h",
        };

        let host = self.normalize_host();

        // SOCKS5 认证嵌入 URL
        if self.proxy_type == ProxyType::Socks5 {
            if let (Some(user), Some(pass)) = (
                Self::normalize_optional(&self.username),
                Self::normalize_optional(&self.password),
            ) {
                let encoded_user = urlencoding::encode(user);
                let encoded_pass = urlencoding::encode(pass);
                return Ok(format!(
                    "{}://{}:{}@{}:{}",
                    scheme, encoded_user, encoded_pass, host, self.port
                ));
            }
        }

        Ok(format!("{}://{}:{}", scheme, host, self.port))
    }

    /// 转换为 `reqwest::Proxy` 对象
    ///
    /// - `None` 类型返回 `Ok(None)`
    /// - HTTP 有认证时通过 `basic_auth` 设置
    /// - SOCKS5 认证已嵌入 URL
    pub fn to_reqwest_proxy(&self) -> Result<Option<reqwest::Proxy>> {
        if self.proxy_type == ProxyType::None {
            return Ok(None);
        }

        let url = self.build_proxy_url()?;
        let mut proxy = reqwest::Proxy::all(&url)?;

        // HTTP 认证通过 basic_auth 设置
        if self.proxy_type == ProxyType::Http {
            if let (Some(user), Some(pass)) = (
                Self::normalize_optional(&self.username),
                Self::normalize_optional(&self.password),
            ) {
                proxy = proxy.basic_auth(user, pass);
            }
        }

        Ok(Some(proxy))
    }

    /// 将代理配置应用到 `ClientBuilder`
    ///
    /// - `None` 类型返回未修改的 builder
    /// - 其他类型调用 `builder.proxy()` 设置代理
    pub fn apply_to_builder(&self, builder: ClientBuilder) -> Result<ClientBuilder> {
        match self.to_reqwest_proxy()? {
            Some(proxy) => Ok(builder.proxy(proxy)),
            None => Ok(builder),
        }
    }
}
