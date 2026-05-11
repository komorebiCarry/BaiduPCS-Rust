# BaiduPCS-Rust 安全改进方案

**基于 SECURITY_REVIEW_AND_ARCHITECTURE.md 的分析，制定可落地的改进计划**

---

## 📋 改进优先级总览

| 优先级 | 问题 | 影响面 | 预估工时 |
|--------|------|--------|---------|
| 🔴 P0 | CORS 允许所有来源 | 中 | 0.5h |
| 🔴 P0 | 会话文件明文存储 | 高 | 2h |
| 🔴 P0 | 缺少安全响应头 (CSP等) | 中 | 1h |
| 🔴 P0 | JWT 密钥管理不明确 | 高 | 1h |
| 🟡 P1 | 日志路径泄露 | 低 | 0.5h |
| 🟡 P1 | API 缺少速率限制 | 中 | 2h |
| 🟡 P1 | 会话文件权限未设置 | 中 | 0.5h |
| 🟢 P2 | 依赖漏洞扫描自动化 | 低 | 1h |
| 🟢 P2 | supply chain 安全加固 | 低 | 1h |

---

## 🔴 P0 — 必须修复

### 1. CORS 配置加固

**风险：** 当前 `CorsLayer::new().allow_origin(Any)` 允许任意网站跨域请求 API，可能被恶意网站利用。

**改进方案（分环境配置）：**

在 `backend/src/main.rs` 中，将 CORS 配置改为根据运行环境动态设置：

```rust
use axum::http::{HeaderValue, Method};
use std::env;

// 替换当前的 CorsLayer 创建代码
let cors = match env::var("APP_ENV").unwrap_or_else(|_| "development".into()).as_str() {
    "production" => {
        let allowed_origin = config
            .server
            .cors_origin
            .as_deref()
            .unwrap_or("https://localhost:5173");
        CorsLayer::new()
            .allow_origin(allowed_origin.parse::<HeaderValue>().unwrap())
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ])
            .max_age(std::time::Duration::from_secs(3600))
    }
    _ => {
        // 开发环境保持宽松
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    }
};
```

**同时需要在 `config/app.toml` 中添加配置项：**

```toml
[server]
host = "0.0.0.0"
port = 5000
cors_origin = "https://your-domain.com"  # 新增：生产环境 CORS 允许的来源
```

**对应 `config/mod.rs` 中新增字段：**

```rust
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub cors_origin: Option<String>,  // 新增
}
```

---

### 2. 会话文件加密存储

**风险：** `config/session.json` 以明文存储 access token、refresh token 等敏感凭证，文件系统被入侵即可窃取。

**改进方案：** 使用 AES-256-GCM 对会话文件进行加密，密钥派生自机器级别的持久化密钥。

#### 步骤 1：新增会话加密模块 `backend/src/auth/session_encryption.rs`

```rust
//! 会话文件加密模块
//!
//! 使用 AES-256-GCM 对会话文件进行加密存储。
//! 加密密钥派生自：
//! 1. 机器 ID (/etc/machine-id 或等效)
//! 2. 可选的用户提供的密钥盐 (SESSION_ENCRYPTION_KEY 环境变量)

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs;

/// 从机器标识派生加密密钥
///
/// 密钥来源优先级：
/// 1. 环境变量 `SESSION_ENCRYPTION_KEY`（显式指定）
/// 2. `/etc/machine-id`（Linux）
/// 3. 回退到主机名 + 随机种子（写入文件持久化）
fn derive_encryption_key() -> Result<[u8; 32]> {
    // 尝试环境变量
    if let Ok(key_hex) = std::env::var("SESSION_ENCRYPTION_KEY") {
        let key_bytes = hex::decode(key_hex.trim())
            .context("SESSION_ENCRYPTION_KEY 必须是有效的十六进制字符串")?;
        if key_bytes.len() != 32 {
            anyhow::bail!("SESSION_ENCRYPTION_KEY 必须为 32 字节（64 位十六进制）");
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes);
        return Ok(key);
    }

    // 尝试 /etc/machine-id
    if let Ok(content) = std::fs::read_to_string("/etc/machine-id") {
        let hash = Sha256::digest(content.trim().as_bytes());
        let mut key = [0u8; 32];
        key.copy_from_slice(&hash);
        return Ok(key);
    }

    // 回退：使用主机名 + 持久化密钥文件
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
    let fallback_path = dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("./config"))
        .join("baidupcs")
        .join(".session_key");

    if fallback_path.exists() {
        let stored = std::fs::read_to_string(&fallback_path)?;
        let hash = Sha256::digest(stored.trim().as_bytes());
        let mut key = [0u8; 32];
        key.copy_from_slice(&hash);
        return Ok(key);
    }

    // 首次运行：生成随机密钥并持久化
    use rand::Rng;
    let random_seed: String = (0..32)
        .map(|_| format!("{:02x}", rand::thread_rng().gen::<u8>()))
        .collect();
    let combined = format!("{}:{}", hostname, random_seed);
    let hash = Sha256::digest(combined.as_bytes());

    if let Some(parent) = fallback_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&fallback_path, &random_seed)?;
    // 密钥文件仅所有者可读写
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fallback_path, std::fs::Permissions::from_mode(0o600))?;
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&hash);
    Ok(key)
}

/// 加密会话数据
pub async fn encrypt_session_data(plaintext: &[u8]) -> Result<Vec<u8>> {
    let key = derive_encryption_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| anyhow::anyhow!("无法创建加密器: {}", e))?;

    // 96 位随机 nonce
    use aes_gcm::aead::OsRng;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| anyhow::anyhow!("加密失败: {}", e))?;

    // 格式: [nonce (12 bytes)][ciphertext]
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

/// 解密会话数据
pub async fn decrypt_session_data(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 12 {
        anyhow::bail!("数据格式无效：长度不足");
    }

    let key = derive_encryption_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| anyhow::anyhow!("无法创建解密器: {}", e))?;

    let (nonce_bytes, ciphertext) = data.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("解密失败（密钥可能已变更）: {}", e))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let data = b"hello world, this is sensitive session data!";
        let encrypted = encrypt_session_data(data).await.unwrap();
        let decrypted = decrypt_session_data(&encrypted).await.unwrap();
        assert_eq!(decrypted, data);
    }
}
```

#### 步骤 2：改造 `backend/src/auth/session.rs`

```rust
// 在 save_session 方法中
pub async fn save_session(&mut self, user_auth: &UserAuth) -> Result<()> {
    info!("💾 保存加密会话到文件: {}", self.session_file);

    if let Some(parent) = Path::new(&self.session_file).parent() {
        fs::create_dir_all(parent).await?;
    }

    let json = serde_json::to_vec(user_auth)?;
    let encrypted = session_encryption::encrypt_session_data(&json).await?;
    fs::write(&self.session_file, &encrypted).await?;

    // 设置文件权限 0600
    set_restricted_permissions(&self.session_file)?;

    self.current_session = Some(user_auth.clone());
    info!("✅ 加密会话保存完成");
    Ok(())
}

// 在 load_session 方法中
pub async fn load_session(&mut self) -> Result<Option<UserAuth>> {
    if !Path::new(&self.session_file).exists() {
        return Ok(None);
    }

    let content = fs::read(&self.session_file).await?;
    let decrypted = session_encryption::decrypt_session_data(&content).await?;
    let user_auth: UserAuth = serde_json::from_slice(&decrypted)?;

    self.current_session = Some(user_auth.clone());
    Ok(Some(user_auth))
}
```

#### 步骤 3：文件权限辅助函数

在 `backend/src/auth/session.rs` 或独立工具模块中添加：

```rust
/// 设置文件为仅所有者可读写 (0600)
#[cfg(unix)]
fn set_restricted_permissions(path: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_restricted_permissions(_path: &str) -> Result<()> {
    // Windows 不适用 POSIX 权限模型，跳过
    Ok(())
}
```

---

### 3. 添加安全响应头 (CSP + 安全头中间件)

**风险：** 缺少 CSP、X-Content-Type-Options、X-Frame-Options 等标准安全头，前端易受 XSS/点击劫持攻击。

**改进方案：** 创建一个安全头中间件，统一添加所有标准安全响应头。

#### 新增 `backend/src/server/security_headers.rs`

```rust
//! 安全响应头中间件
//!
//! 为所有 HTTP 响应添加标准安全头。

use axum::{
    http::HeaderValue,
    response::Response,
};

/// 安全响应头配置
pub struct SecurityHeadersConfig {
    pub csp_enabled: bool,
    pub hsts_enabled: bool,
}

impl Default for SecurityHeadersConfig {
    fn default() -> Self {
        Self {
            csp_enabled: true,
            hsts_enabled: true,
        }
    }
}

/// 添加安全响应头
pub async fn security_headers_middleware(
    mut response: Response,
) -> Response {
    let headers = response.headers_mut();

    // Content-Security-Policy
    headers.insert(
        "Content-Security-Policy",
        HeaderValue::from_static(
            "default-src 'self'; \
             script-src 'self' 'unsafe-inline' 'unsafe-eval'; \
             style-src 'self' 'unsafe-inline'; \
             img-src 'self' data: blob:; \
             font-src 'self'; \
             connect-src 'self' ws: wss:; \
             frame-ancestors 'none'; \
             form-action 'self'"
        ),
    );

    // X-Content-Type-Options: 防止 MIME 类型嗅探
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );

    // X-Frame-Options: 防止点击劫持
    headers.insert(
        "X-Frame-Options",
        HeaderValue::from_static("DENY"),
    );

    // Referrer-Policy: 控制 referer 头信息
    headers.insert(
        "Referrer-Policy",
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );

    // Permissions-Policy: 限制浏览器 API 权限
    headers.insert(
        "Permissions-Policy",
        HeaderValue::from_static(
            "camera=(), microphone=(), geolocation=(), \
             payment=(), usb=(), magnetometer=(), accelerometer=()"
        ),
    );

    // X-XSS-Protection: 兼容旧浏览器
    headers.insert(
        "X-XSS-Protection",
        HeaderValue::from_static("1; mode=block"),
    );

    // HTTP Strict-Transport-Security (仅 HTTPS 时有效)
    headers.insert(
        "Strict-Transport-Security",
        HeaderValue::from_static(
            "max-age=31536000; includeSubDomains; preload"
        ),
    );

    response
}
```

#### 在主程序 `backend/src/main.rs` 中添加中间件

```rust
// 引入安全头模块
mod server;
use server::security_headers::security_headers_middleware;

// 在中间件栈中添加
let middleware = ServiceBuilder::new()
    .layer(TraceLayer::new_for_http())
    .layer(from_fn(security_headers_middleware))  // 新增：安全头
    .layer(cors_layer);  // 修改后的 CORS
```

---

### 4. JWT 密钥管理改进

**风险：** 自动生成的密钥在进程重启后丢失，导致已有 JWT 立即失效。生产环境需要持久化密钥。

**改进方案：** 引入层级密钥查找机制。

#### 修改 `backend/src/web_auth/token.rs`

```rust
impl TokenService {
    /// 创建新的 Token 服务
    ///
    /// 密钥查找优先级：
    /// 1. `JWT_SECRET` 环境变量
    /// 2. `config/jwt.key` 文件
    /// 3. `config/app.toml` 中的 jwt_secret 配置
    /// 4. 开发环境自动生成（含持久化）
    pub fn new(jwt_secret: Option<String>) -> Self {
        let secret = jwt_secret
            .or_else(|| std::env::var("JWT_SECRET").ok())
            .or_else(|| Self::load_secret_from_file("config/jwt.key"))
            .unwrap_or_else(|| {
                // 开发环境：自动生成并持久化
                #[cfg(debug_assertions)]
                {
                    let generated = Self::generate_random_secret();
                    let _ = std::fs::write("config/jwt.key", &generated);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = std::fs::set_permissions(
                            "config/jwt.key",
                            std::fs::Permissions::from_mode(0o600),
                        );
                    }
                    generated
                }
                #[cfg(not(debug_assertions))]
                {
                    panic!(
                        "生产环境必须设置 JWT_SECRET 环境变量或提供 config/jwt.key 文件"
                    );
                }
            });

        info!(
            "TokenService 初始化完成 (密钥来源: {})",
            if std::env::var("JWT_SECRET").is_ok() { "环境变量" }
            else if std::path::Path::new("config/jwt.key").exists() { "密钥文件" }
            else { "自动生成" }
        );

        // ... 后续代码不变
    }

    /// 从文件加载密钥
    fn load_secret_from_file(path: &str) -> Option<String> {
        std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
    }
}
```

---

## 🟡 P1 — 建议修复

### 5. 日志路径脱敏

**风险：** 日志中可能记录完整的本地文件路径，泄露用户目录结构信息。

**改进方案：** 在 `backend/src/logging.rs` 中添加路径脱敏函数，在关键位置使用。

```rust
/// 脱敏路径：替换用户主目录和敏感目录
///
/// 例: "/home/alice/downloads/secret.pdf" -> "~/downloads/secret.pdf"
pub fn sanitize_path<P: AsRef<Path>>(path: P) -> String {
    let path_str = path.as_ref().to_string_lossy();

    // 替换用户主目录
    if let Ok(home) = std::env::var("HOME") {
        if let Some(rest) = path_str.strip_prefix(&home) {
            return format!("~{}", rest);
        }
    }

    path_str.to_string()
}

/// 脱敏文件名：只保留文件名和扩展名
pub fn sanitize_filename<P: AsRef<Path>>(path: P) -> String {
    path.as_ref()
        .file_name()
        .map(|n| {
            let name = n.to_string_lossy();
            // 如果文件名看似敏感（token、key、secret 等关键词）
            let lower = name.to_lowercase();
            if lower.contains("secret")
                || lower.contains("token")
                || lower.contains("key")
                || lower.contains("password")
            {
                let parts: Vec<&str> = name.splitn(2, '.').collect();
                if parts.len() == 2 {
                    return format!("***.{}", parts[1]);
                }
                return "***".to_string();
            }
            name.to_string()
        })
        .unwrap_or_else(|| "<unknown>".to_string())
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_sanitize_path_with_home() {
        std::env::set_var("HOME", "/home/testuser");
        assert_eq!(
            sanitize_path("/home/testuser/downloads/file.txt"),
            "~/downloads/file.txt"
        );
    }

    #[test]
    fn test_sanitize_filename_sensitive() {
        assert_eq!(
            sanitize_filename(Path::new("/path/to/access_token.txt")),
            "***.txt"
        );
        assert_eq!(
            sanitize_filename(Path::new("/path/to/normal.txt")),
            "normal.txt"
        );
    }
}
```

**使用方式：** 在日志调用处替换：

```rust
// 之前
info!("文件路径: {}", file_path);

// 之后
info!("文件路径: {}", sanitize_path(file_path));
```

---

### 6. API 全局限速

**风险：** 仅登录接口有速率限制，下载/上传/删除等敏感操作无限制，可能被滥用。

**改进方案：** 使用 `governor` 库实现基于 IP 的分组速率限制。

在 `Cargo.toml` 中添加依赖：

```toml
governor = "0.6"
```

#### 创建速率限制中间件 `backend/src/server/rate_limiter.rs`

```rust
//! API 速率限制中间件
//!
//! 基于 governor 库实现令牌桶算法，按 IP 分组限制。

use axum::{
    extract::ConnectInfo,
    http::Request,
    middleware::Next,
    response::{IntoResponse, Response, Json},
    StatusCode,
};
use governor::{
    clock::QuantaClock,
    middleware::NoOpMiddleware,
    state::keyed::DashMapStateStore,
    quota::PerSecond,
    RateLimiter,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use serde_json::json;

/// 速率限制配置
#[derive(Clone)]
pub struct RateLimitConfig {
    /// 通用 API 限制（次/秒）
    pub default: u32,
    /// 下载操作限制（次/秒）
    pub downloads: u32,
    /// 上传操作限制（次/秒）
    pub uploads: u32,
    /// 删除操作限制（次/秒）
    pub delete: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            default: 30,
            downloads: 10,
            uploads: 5,
            delete: 3,
        }
    }
}

/// 速率限制器
pub struct AppRateLimiter {
    default: RateLimiter<String, DashMapStateStore<String>, QuantaClock, NoOpMiddleware>,
    // 可按需添加更细粒度的限制器
}

impl AppRateLimiter {
    pub fn new(config: &RateLimitConfig) -> Self {
        Self {
            default: RateLimiter::dashmap(
                governor::Quota::with_period(Duration::from_secs(1))
                    .unwrap()
                    .allow_burst(PerSecond::new(config.default as u32) as u32)
                    .burst_size(config.default as u32 * 2),
            ),
        }
    }

    /// 检查是否允许请求
    pub fn check_rate_limit(&self, key: &str) -> Result<(), ()> {
        match self.default.check_key(key) {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        }
    }
}

/// 速率限制中间件
pub async fn rate_limit_middleware<B>(
    req: Request<B>,
    next: Next<B>,
) -> Response {
    // 获取客户端 IP
    let client_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // 获取速率限制器实例
    let limiter = req
        .extensions()
        .get::<Arc<AppRateLimiter>>();

    if let Some(limiter) = limiter {
        if limiter.check_rate_limit(&client_ip).is_err() {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "rate_limit_exceeded",
                    "message": "请求过于频繁，请稍后再试",
                    "retry_after_seconds": 1,
                })),
            )
                .into_response();
        }
    }

    next.run(req).await
}
```

**在主程序中注册：**

```rust
// 创建速率限制器实例
let rate_limiter = Arc::new(AppRateLimiter::new(&config.server.rate_limit));

// 添加到应用状态
let app_state = AppState {
    rate_limiter: rate_limiter.clone(),
    // ... 其他状态
};

// 将 rate_limiter 注入到扩展中
let app = Router::new()
    .layer(Extension(rate_limiter))
    .layer(from_fn(rate_limit_middleware))
    // ... 其他中间件
```

---

### 7. 启动时文件权限自动修复

**风险：** 用户可能忘记手动设置 `session.json` 等敏感文件的权限。

**改进方案：** 在应用启动时自动检查并修复关键文件权限。

在 `backend/src/main.rs` 的初始化阶段添加：

```rust
/// 启动时安全检查：修复关键文件权限
async fn enforce_file_permissions() {
    let sensitive_files = [
        "./config/session.json",
        "./config/jwt.key",
        "./config/app.toml",
    ];

    for path_str in &sensitive_files {
        let path = std::path::Path::new(path_str);
        if !path.exists() {
            continue;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = match std::fs::metadata(path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let mode = metadata.permissions().mode() & 0o777;

            // 如果权限过于宽松，自动修复
            let desired_mode = if path_str.ends_with("app.toml") {
                0o644  // 配置文件可读
            } else {
                0o600  // 敏感文件仅所有者
            };

            if mode != desired_mode {
                warn!(
                    "⚠️ 文件权限不安全: {} (当前: {:o}, 期望: {:o})，正在自动修复",
                    path_str, mode, desired_mode
                );
                if let Err(e) = std::fs::set_permissions(
                    path,
                    std::fs::Permissions::from_mode(desired_mode),
                ) {
                    warn!("无法修复文件权限 {}: {}", path_str, e);
                }
            }
        }
    }
}
```

---

## 🟢 P2 — 建议优化

### 8. 依赖漏洞扫描自动化

**风险：** 依赖库可能存在已知 CVE，需要定期检查。

**改进方案：**

#### 在 `Cargo.toml` 中添加检查脚本

```toml
[package.metadata]
# 通过 cargo audit 自动检查
```

#### 创建 CI 配置文件 `.github/workflows/security.yml`

```yaml
name: Security Audit

on:
  schedule:
    - cron: '0 0 * * 0'  # 每周日执行
  push:
    branches: [main]
    paths:
      - 'backend/Cargo.lock'

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Install cargo-audit
        run: cargo install cargo-audit --locked
      
      - name: Run security audit
        working-directory: ./backend
        run: cargo audit
      
      - name: Run outdated check
        working-directory: ./backend
        run: cargo outdated --exit-code 1 || true
```

#### 添加 `scripts/security_check.sh`

```bash
#!/bin/bash
# 安全扫描脚本

set -euo pipefail

echo "=== 运行 cargo audit ==="
cd "$(dirname "$0")/../backend"
cargo audit

echo ""
echo "=== 检查依赖版本 ==="
cargo outdated

echo ""
echo "=== 检查 Rust 版本 ==="
rustc --version

echo ""
echo "=== 检查未使用的依赖 ==="
cargo +nightly udeps --all-features 2>/dev/null || true

echo ""
echo "✅ 安全检查完成"
```

---

### 9. Supply Chain 安全加固

**风险：** 依赖可能被篡改或引入恶意代码。

**改进方案：**

#### 启用 `Cargo.lock` 版本控制

确保 `Cargo.lock` 已加入 Git：

```bash
# 验证 Cargo.lock 未被 .gitignore 排除
git check-ignore backend/Cargo.lock
# 如果被忽略，移除 .gitignore 中的相关行
```

#### 使用依赖哈希验证

在 `Cargo.toml` 中固定依赖版本并使用 `[patch]` 段验证关键依赖：

```toml
# 固定关键依赖版本，防止意外升级引入变更
[dependencies]
# 加密库固定小版本
aes-gcm = "=0.10.3"
argon2 = "=0.5.3"
jsonwebtoken = "=9.3.0"

# 其他依赖允许兼容性升级
axum = "0.7"
tokio = { version = "1", features = ["full"] }
```

---

## 📦 实施路线图

### 第一阶段：快速修复（1-2 天）

| 任务 | 文件 | 预计时间 |
|------|------|---------|
| CORS 分环境配置 | `main.rs`, `config/mod.rs`, `app.toml` | 0.5h |
| 安全响应头中间件 | 新建 `security_headers.rs` + 接入 `main.rs` | 1h |
| 启动时文件权限修复 | `main.rs` | 0.5h |

### 第二阶段：核心加固（2-3 天）

| 任务 | 文件 | 预计时间 |
|------|------|---------|
| 会话加密 | 新建 `session_encryption.rs` + 改造 `session.rs` | 2h |
| JWT 密钥层级查找 | `token.rs` | 1h |
| 日志脱敏 | `logging.rs` + 关键调用点 | 1h |

### 第三阶段：增强加固（2-3 天）

| 任务 | 文件 | 预计时间 |
|------|------|---------|
| API 全局限速 | 新建 `rate_limiter.rs` + `Cargo.toml` | 2h |
| CI 安全扫描 | `.github/workflows/security.yml` | 1h |
| Supply chain 加固 | `Cargo.toml`, `Cargo.lock` | 0.5h |

---

## ✅ 验收标准

每个安全改进完成后，应验证以下内容：

- [ ] **CORS**：生产环境返回头 `Access-Control-Allow-Origin` 为指定域名而非 `*`
- [ ] **安全头**：`curl -I` 能见到 CSP、X-Frame-Options、HSTS 等头
- [ ] **会话加密**：`config/session.json` 内容为密文（不可读 JSON）
- [ ] **JWT 密钥**：重启进程后已有 token 仍然有效
- [ ] **文件权限**：敏感文件权限为 `0600`
- [ ] **日志脱敏**：日志中无完整 `/home/*` 路径
- [ ] **速率限制**：超出限制时返回 `429 Too Many Requests`
- [ ] **CI 安全扫描**：`cargo audit` 通过
- [ ] **回归测试**：`cargo test` 全量通过

---

## 📚 参考资源

- [OWASP REST Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html)
- [OWASP HTTP Headers Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/HTTP_Headers_Cheat_Sheet.html)
- [Mozilla Web Security Guidelines](https://infosec.mozilla.org/guidelines/web_security)
- [RustSec Advisory Database](https://rustsec.org/)
- [cargo-audit Documentation](https://docs.rs/cargo-audit/latest/)
