// 网盘客户端实现

use crate::auth::constants::{BAIDU_APP_ID, CLIENT_TYPE, API_USER_INFO, USER_AGENT};
use crate::auth::constants::USER_AGENT as WEB_USER_AGENT; // 导入登录时的 UA,确保一致
use crate::auth::UserAuth;
use crate::netdisk::{
    CreateFileResponse, FileListResponse, LocateDownloadResponse, PrecreateResponse,
    RapidUploadResponse, UploadChunkResponse, UploadErrorKind,
};
use crate::sign::LocateSign;
use anyhow::{Context, Result};
use reqwest::cookie::CookieStore;
use reqwest::multipart;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// 百度网盘客户端
#[derive(Debug, Clone)]
pub struct NetdiskClient {
    /// HTTP客户端
    client: Client,
    /// Cookie Jar (用于调试和检查 Cookie 状态)
    cookie_jar: std::sync::Arc<reqwest::cookie::Jar>,
    /// 用户认证信息
    user_auth: UserAuth,
    /// Android 端 User-Agent（用于 Locate/下载等接口）
    mobile_user_agent: String,
    /// Web 端 User-Agent（PCS/浏览器接口需要）
    web_user_agent: String,
    /// Web 会话是否已预热（保留供将来使用）
    #[allow(dead_code)]
    web_session_ready: std::sync::Arc<Mutex<bool>>,
    /// PANPSC Cookie 值（从预热过程中提取）
    panpsc_cookie: std::sync::Arc<Mutex<Option<String>>>,
    /// bdstoken（/api/loginStatus 或 /api/gettemplatevariable 返回）
    bdstoken: std::sync::Arc<Mutex<Option<String>>>,
}

impl NetdiskClient {
    /// 创建新的网盘客户端
    ///
    /// # 参数
    /// * `user_auth` - 用户认证信息（包含BDUSS）
    pub fn new(user_auth: UserAuth) -> Result<Self> {
        use reqwest::cookie::Jar;
        use std::sync::Arc;

        // 1. 先创建启用了自动 Cookie 管理的客户端
        info!("初始化网盘客户端,启用自动 Cookie 管理");

        let jar = Arc::new(Jar::default());
        let url = "https://pan.baidu.com".parse::<reqwest::Url>().unwrap();

        // 2. 如果有保存的 Cookie,手动初始化到 Cookie Jar
        // 这些 Cookie 来自之前的登录会话 (从 session.json 加载)
        // 如果没有保存的 cookies,至少要添加 BDUSS/PTOKEN
        info!("手动添加 BDUSS/PTOKEN");
        let bduss_cookie = format!("BDUSS={}; Domain=.baidu.com; Path=/", user_auth.bduss);
        jar.add_cookie_str(&bduss_cookie, &url);

        if let Some(ref ptoken) = user_auth.ptoken {
            let ptoken_cookie = format!("PTOKEN={}; Domain=.baidu.com; Path=/", ptoken);
            jar.add_cookie_str(&ptoken_cookie, &url);
        }
        if let Some(ref baiduid) = user_auth.baiduid {
            let baiduid_cookie = format!("BAIDUID={}; Domain=.baidu.com; Path=/", baiduid);
            jar.add_cookie_str(&baiduid_cookie, &url);
        }

        // 加载预热后的 Cookie (如果有的话)
        if let Some(ref panpsc) = user_auth.panpsc {
            let panpsc_cookie = format!("PANPSC={}; Domain=.baidu.com; Path=/", panpsc);
            jar.add_cookie_str(&panpsc_cookie, &url);
        }
        if let Some(ref csrf_token) = user_auth.csrf_token {
            let csrf_cookie = format!("csrfToken={}; Domain=.baidu.com; Path=/", csrf_token);
            jar.add_cookie_str(&csrf_cookie, &url);
        }

        // 打印初始化后的 Cookie（调试）
        info!("初始化后的 Cookie:");
        let init_cookies = jar.cookies(&url);
        if let Some(cookie_header) = init_cookies {
            if let Ok(cookie_str) = cookie_header.to_str() {
                for cookie in cookie_str.split("; ") {
                    let name = cookie.split('=').next().unwrap_or("");
                    info!("  已添加: {}", name);
                }
            }
        }

        // 3. 创建客户端,使用 cookie_provider 自动管理 Cookie
        // 后续请求会自动收集服务器返回的 Set-Cookie
        // 注意: 不要禁用重定向 (Policy::none())，否则 Cookie Jar 可能无法正确携带 Cookie
        let client = Client::builder()
            .cookie_provider(Arc::clone(&jar))
            .timeout(std::time::Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::limited(10)) // 允许最多 10 次重定向
            .build()
            .context("Failed to create HTTP client")?;

        info!(
            "初始化网盘客户端成功, UID={}, PTOKEN={}",
            user_auth.uid,
            if user_auth.ptoken.is_some() {
                "已设置"
            } else {
                "未设置"
            }
        );

        // 初始化预热相关字段
        let panpsc_cookie = std::sync::Arc::new(Mutex::new(user_auth.panpsc.clone()));
        let bdstoken = std::sync::Arc::new(Mutex::new(user_auth.bdstoken.clone()));
        let web_session_ready = std::sync::Arc::new(Mutex::new(
            // 如果已有预热 Cookie,标记为已预热
            user_auth.panpsc.is_some() && user_auth.csrf_token.is_some() && user_auth.bdstoken.is_some()
        ));

        Ok(Self {
            client,
            cookie_jar: jar,
            user_auth,
            mobile_user_agent: Self::default_mobile_user_agent(),
            web_user_agent: Self::default_web_user_agent(),
            web_session_ready,
            panpsc_cookie,
            bdstoken,
        })
    }

    /// 打印 Cookie Jar 中的 Cookie（用于调试）
    fn debug_print_cookies(&self, context: &str) {
        let url = "https://pan.baidu.com".parse::<reqwest::Url>().unwrap();
        let cookies = self.cookie_jar.cookies(&url);

        if let Some(cookie_header) = cookies {
            if let Ok(cookie_str) = cookie_header.to_str() {
                info!("Cookie Jar 内容 [{}]:", context);
                // 按分号分割并打印每个 Cookie
                for cookie in cookie_str.split("; ") {
                    if cookie.split_once('=').is_some() {
                        // 对于敏感 Cookie，只显示名称和值的前几个字符
                        if cookie.len() > 50 {
                            info!("  {}...", &cookie[..50]);
                        } else {
                            info!("  {}", cookie);
                        }
                    }
                }
                info!("  总共 {} 个 Cookie", cookie_str.split("; ").count());
            }
        } else {
            warn!("Cookie Jar 为空 [{}]", context);
        }
    }

    /// 默认移动端 User-Agent（模拟网盘 Android 客户端）
    /// Locate 下载 API 需要此 UA
    fn default_mobile_user_agent() -> String {
        "netdisk;P2SP;3.0.0.8;netdisk;11.12.3;ANG-AN00;android-android;10.0;JSbridge4.4.0;jointBridge;1.1.0;".to_string()
    }

    /// 默认 Web 端 User-Agent（模拟 PC 浏览器）
    /// 注意: 必须与登录时的 UA 完全一致 (复用 auth/constants.rs 的 USER_AGENT)
    fn default_web_user_agent() -> String {
        WEB_USER_AGENT.to_string()
    }

    /// 确保 Web 会话已预热（用于获取 BAIDUID / PANPSC 等 Cookie）
    #[allow(dead_code)]
    async fn ensure_web_session(&self) -> Result<()> {
        {
            let ready = self.web_session_ready.lock().await;
            if *ready {
                return Ok(());
            }
        }

        self.perform_web_warmup().await?;

        let mut ready = self.web_session_ready.lock().await;
        *ready = true;
        Ok(())
    }

    /// 访问若干 pan/yun 页面，触发服务端下发 Web 所需 Cookie
    pub async fn perform_web_warmup(&self) -> Result<()> {
        info!("======== 开始 Web 预热，准备获取 PAN/PCS 所需 Cookie ========");

        // 统一的 UA 和 Referer
        let ua = &self.web_user_agent;
        let referer_home = "https://pan.baidu.com/disk/home";

        //--------------------------------------------------------------
        // 提供一个统一的执行器：发送请求 + 检查重定向 + 写 Cookie + 检查登录
        // 注意：使用正常客户端（允许重定向），检查最终 URL 而不是 Location header
        //--------------------------------------------------------------
        async fn exec_request(
            _client: &reqwest::Client,
            req: reqwest::RequestBuilder,
            step: &str,
            panpsc_storage: &std::sync::Arc<Mutex<Option<String>>>,
        ) -> Result<String> {
            if let Some(cloned_builder) = req.try_clone() {
                if let Ok(debug_req) = cloned_builder.build() {
                    let url = debug_req.url().clone();
                    let ua = debug_req
                        .headers()
                        .get("User-Agent")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("<missing>");
                    let referer = debug_req
                        .headers()
                        .get("Referer")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("<missing>");
                    if step.contains("/disk/home") {
                        info!(
                            "{}: 请求头快照 -> UA={}, Referer={}, Cookie={}...",
                            step,
                            ua,
                            referer,
                            debug_req
                                .headers()
                                .get("Cookie")
                                .and_then(|v| v.to_str().ok())
                                .map(|v| v.chars().take(120).collect::<String>())
                                .unwrap_or_else(|| "<missing>".to_string())
                        );
                    } else {
                        debug!(
                            "{}: 请求头快照 -> UA={}, Referer={}",
                            step, ua, referer
                        );
                    }
                    debug!("{}: 最终请求 URL = {}", step, url);
                }
            }

            let resp = req.send().await.context(format!("{} 请求失败", step))?;

            let status = resp.status();
            let final_url = resp.url().to_string();

            // 检查 Location header（重定向目标）
            let location = resp.headers().get("location");
            if let Some(loc) = location {
                if let Ok(loc_str) = loc.to_str() {
                    info!("{}: Location header: {}", step, loc_str);
                    // 如果 Location 指向登录页，说明 BDUSS 失效
                    if loc_str.contains("passport.baidu.com")
                        || loc_str.contains("wappass.baidu.com")
                        || loc_str == "/"
                    {
                        anyhow::bail!(
                            "BDUSS 已失效 ({} Location header 指向登录页: {})",
                            step,
                            loc_str
                        );
                    }
                }
            }

            info!("{}: status={}, final_url={}", step, status, final_url);

            // 检查最终 URL 是否重定向到登录页（代表 BDUSS 失效）
            // 对于 /disk/home，如果最终 URL 是登录页，说明有问题
            if step.contains("/disk/home") {
                if final_url.contains("passport.baidu.com")
                    || final_url.contains("wappass.baidu.com")
                    || final_url.contains("pan.baidu.com/login")
                {
                    anyhow::bail!(
                        "BDUSS 已失效或请求参数错误 ({} 最终重定向到 {})",
                        step,
                        final_url
                    );
                }
            }

            // 打印 Set-Cookie 并提取 PANPSC
            let mut count = 0;
            for ck in resp.headers().get_all("set-cookie") {
                if let Ok(s) = ck.to_str() {
                    let mut parts = s.split(';');
                    let kv = parts.next().unwrap_or("unknown");
                    let (name, value_preview) = if let Some((n, v)) = kv.split_once('=') {
                        (
                            n,
                            if v.len() > 60 {
                                format!("{}...", &v[..60])
                            } else {
                                v.to_string()
                            },
                        )
                    } else {
                        (kv, "<no-value>".to_string())
                    };

                    let mut domain = "<none>";
                    let mut path = "<none>";
                    let mut expires = "<none>";
                    for attr in parts.clone() {
                        let attr_trim = attr.trim();
                        let lower = attr_trim.to_lowercase();
                        if lower.starts_with("domain=") {
                            domain = &attr_trim[7..];
                        } else if lower.starts_with("path=") {
                            path = &attr_trim[5..];
                        } else if lower.starts_with("expires=") {
                            expires = &attr_trim[8..];
                        } else if lower.starts_with("max-age=") {
                            expires = attr_trim;
                        }
                    }

                    count += 1;
                    info!(
                        "{}: Set-Cookie[{}] {}={} (domain={}, path={}, expires={})",
                        step, count, name, value_preview, domain, path, expires
                    );

                    if name.eq_ignore_ascii_case("BDUSS") && value_preview.trim().is_empty() {
                        warn!("{}: 收到清空 BDUSS 的 Set-Cookie！完整内容: {}", step, s);
                    }

                    if name == "PANPSC" {
                        if let Some((_, full_value)) = kv.split_once('=') {
                            if full_value.is_empty() {
                                warn!("{}: PANPSC Cookie 值为空！完整 Set-Cookie: {}", step, s);
                            } else {
                                let mut panpsc = panpsc_storage.lock().await;
                                *panpsc = Some(full_value.to_string());
                                info!(
                                    "{}: 提取到 PANPSC Cookie 值 (长度={}): {}...",
                                    step,
                                    full_value.len(),
                                    &full_value[..full_value.len().min(20)]
                                );
                            }
                        } else {
                            warn!("{}: PANPSC Set-Cookie 格式错误，未找到 '=': {}", step, s);
                        }
                    }
                }
            }
            info!("{}: 本次收到 {} 个 Cookie", step, count);

            let body = resp
                .text()
                .await
                .context(format!("{}: 读取响应失败", step))?;

            // 打印响应体长度（用于调试）
            info!("{}: 响应体长度: {} 字节", step, body.len());

            // 打印 /api/loginStatus 的完整响应
            if step.contains("/api/loginStatus") {
                info!("{}: 完整响应内容: {}", step, body);
            }

            // 若返回登录页则说明 BDUSS 失效
            if body.contains("passport.baidu.com") && body.contains("登录") {
                anyhow::bail!("BDUSS 已失效（{} 响应出现登录页）", step);
            }

            Ok(body)
        }

        // 使用正常的带 CookieJar 的 Client（允许重定向）
        let client = &self.client;

        //--------------------------------------------------------------
        // STEP 1: /disk/home
        //--------------------------------------------------------------
        info!("步骤 1/4：访问 /disk/home");
        let home_url = "https://pan.baidu.com/disk/home";
        self.debug_print_cookies("步骤 1/4 前 Cookie 状态");
        // 步骤 1: 使用简单的 User-Agent（与 BaiduPCS-Go 一致）
        // BaiduPCS-Go 使用 "Mozilla/5.0"，我们使用类似的简单 UA
        let simple_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
        let _body1 = exec_request(
            client,
            client
                .get(home_url)
                .header("User-Agent", simple_ua),
            "步骤 1/4 (/disk/home)",
            &self.panpsc_cookie,
        )
            .await?;

        self.debug_print_cookies("步骤 1/4 后 Cookie 状态");

        //--------------------------------------------------------------
        // STEP 2: /api/loginStatus
        //--------------------------------------------------------------
        info!("步骤 2/4：访问 /api/loginStatus");

        let login_status_url = format!(
            "https://pan.baidu.com/api/loginStatus?clienttype=0&app_id={}&web=1",
            BAIDU_APP_ID
        );
        self.debug_print_cookies("步骤 2/4 前 Cookie 状态");

        let body2 = exec_request(
            client,
            client
                .get(&login_status_url)
                .header("User-Agent", ua)
                .header("Referer", referer_home),
            "步骤 2/4 (/api/loginStatus)",
            &self.panpsc_cookie,
        )
            .await?;

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body2) {
            if let Some(bdstoken) = json["login_info"]["bdstoken"].as_str() {
                info!("步骤 2/4: loginStatus 返回 bdstoken = {}", bdstoken);
                let mut cached = self.bdstoken.lock().await;
                *cached = Some(bdstoken.to_string());
            } else {
                warn!("步骤 2/4: loginStatus 响应缺少 bdstoken 字段");
            }
        } else {
            warn!("步骤 2/4: loginStatus 响应 JSON 解析失败，无法提取 bdstoken");
        }

        self.debug_print_cookies("步骤 2/4 后 Cookie 状态");

        //--------------------------------------------------------------
        // STEP 3: /api/gettemplatevariable
        //--------------------------------------------------------------
        info!("步骤 3/4：访问 /api/gettemplatevariable");

        let bdstoken_url = format!(
            r#"https://pan.baidu.com/api/gettemplatevariable?clienttype=0&app_id={}&web=1&fields=["bdstoken"]"#,
            BAIDU_APP_ID
        );
        self.debug_print_cookies("步骤 3/4 前 Cookie 状态");

        let body3 = exec_request(
            client,
            client
                .get(&bdstoken_url)
                .header("User-Agent", ua)
                .header("Referer", referer_home),
            "步骤 3/4 (/api/gettemplatevariable)",
            &self.panpsc_cookie,
        )
            .await?;

        // 提取 bdstoken
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body3) {
            if let Some(bdstoken) = json["result"]["bdstoken"].as_str() {
                info!("步骤 3/4: 成功获取 bdstoken = {}", bdstoken);
                let mut cached = self.bdstoken.lock().await;
                *cached = Some(bdstoken.to_string());
            } else {
                warn!("步骤 3/4: gettemplatevariable 响应缺少 bdstoken");
            }
        } else {
            warn!("步骤 3/4: gettemplatevariable 响应 JSON 解析失败");
        }

        self.debug_print_cookies("步骤 3/4 后 Cookie 状态");

        //--------------------------------------------------------------
        // STEP 4: /pcloud/user/getinfo
        //--------------------------------------------------------------
        info!("步骤 4/4：访问 /pcloud/user/getinfo");

        let userinfo_url = format!(
            "https://pan.baidu.com/pcloud/user/getinfo?method=userinfo&clienttype=0&app_id={}&web=1&query_uk={}",
            BAIDU_APP_ID,
            self.user_auth.uid
        );
        self.debug_print_cookies("步骤 4/4 前 Cookie 状态");

        let _body4 = exec_request(
            client,
            client
                .get(&userinfo_url)
                .header("User-Agent", ua)
                .header("Referer", referer_home),
            "步骤 4/4 (/pcloud/user/getinfo)",
            &self.panpsc_cookie,
        )
            .await?;

        self.debug_print_cookies("步骤 4/4 后 Cookie 状态");

        //--------------------------------------------------------------
        // FINAL OK
        //--------------------------------------------------------------
        info!("======== Web 预热完成，所有 Cookie 已准备就绪！ ========");
        info!("Cookie Jar 应包含:");
        info!("- BDUSS, STOKEN, PTOKEN");
        info!("- PANPSC, ndut (步骤 1)");
        info!("- pcsett (步骤 2)");
        info!("- bdstoken (步骤 3 Body)");

        self.debug_print_cookies("预热完成 - 最终状态");

        Ok(())
    }

    /// 执行预热并返回预热后的 Cookie 数据
    ///
    /// 返回: (panpsc, csrf_token, bdstoken, stoken)
    ///
    /// 包含重试机制：最多 3 次，间隔指数退避（1秒、3秒、5秒）
    /// 预热前会先验证 BDUSS 是否有效，重试前会恢复被清空的 Cookie
    pub async fn warmup_and_get_cookies(&self) -> Result<(Option<String>, Option<String>, Option<String>, Option<String>)> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAYS: [u64; 3] = [1, 3, 5]; // 指数退避：1秒、3秒、5秒

        // 如果没有 PTOKEN，跳过预热
        if self.user_auth.ptoken.is_none() {
            info!("PTOKEN 为空，跳过预热");
            return Ok((None, None, None, None));
        }

        // 预热前先验证 BDUSS 是否有效
        if !self.verify_bduss().await {
            return Err(anyhow::anyhow!("BDUSS 已失效，请重新登录"));
        }

        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let delay_secs = RETRY_DELAYS.get(attempt as usize - 1).copied().unwrap_or(5);
                warn!(
                    "预热失败，{}秒后进行第 {}/{} 次重试...",
                    delay_secs,
                    attempt + 1,
                    MAX_RETRIES
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;

                // 重试前恢复被清空的 Cookie（防止重定向导致 BDUSS 被删除）
                self.restore_essential_cookies();
            }

            match self.perform_web_warmup().await {
                Ok(()) => {
                    if attempt > 0 {
                        info!("预热重试成功（第 {} 次尝试）", attempt + 1);
                    }

                    // 提取预热后的 Cookie
                    let panpsc = self.panpsc_cookie.lock().await.clone();
                    let bdstoken = self.bdstoken.lock().await.clone();

                    // 从 Cookie Jar 提取 csrfToken 和 STOKEN
                    let url = "https://pan.baidu.com".parse::<reqwest::Url>().unwrap();
                    let cookies = self.cookie_jar.cookies(&url);
                    let mut csrf_token = None;
                    let mut stoken = None;

                    if let Some(cookie_header) = cookies {
                        if let Ok(cookie_str) = cookie_header.to_str() {
                            for cookie in cookie_str.split("; ") {
                                if let Some((name, value)) = cookie.split_once('=') {
                                    match name {
                                        "csrfToken" => csrf_token = Some(value.to_string()),
                                        "STOKEN" => stoken = Some(value.to_string()),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }

                    info!("预热完成,提取到 Cookie: PANPSC={}, csrfToken={}, bdstoken={}, STOKEN={}",
                        panpsc.is_some(), csrf_token.is_some(), bdstoken.is_some(), stoken.is_some());

                    return Ok((panpsc, csrf_token, bdstoken, stoken));
                }
                Err(e) => {
                    warn!(
                        "预热第 {} 次尝试失败: {}",
                        attempt + 1,
                        e
                    );
                    last_error = Some(e);
                }
            }
        }

        // 所有重试都失败
        error!("预热失败，已达到最大重试次数 ({})", MAX_RETRIES);
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("预热失败，未知错误")))
    }

    /// 恢复必要的 Cookie 到 Cookie Jar（用于重试时恢复被清空的 Cookie）
    ///
    /// 当百度服务器返回 `Set-Cookie: BDUSS=;` 时，Cookie Jar 会自动覆盖原有值，
    /// 导致后续请求失败。此方法用于在重试前重新添加必要的 Cookie。
    fn restore_essential_cookies(&self) {
        let url = "https://pan.baidu.com".parse::<reqwest::Url>().unwrap();

        // 重新添加 BDUSS
        let bduss_cookie = format!("BDUSS={}; Domain=.baidu.com; Path=/", self.user_auth.bduss);
        self.cookie_jar.add_cookie_str(&bduss_cookie, &url);

        // 重新添加 STOKEN
        if let Some(ref stoken) = self.user_auth.stoken {
            let stoken_cookie = format!("STOKEN={}; Domain=.baidu.com; Path=/", stoken);
            self.cookie_jar.add_cookie_str(&stoken_cookie, &url);
        }

        // 重新添加 PTOKEN
        if let Some(ref ptoken) = self.user_auth.ptoken {
            let ptoken_cookie = format!("PTOKEN={}; Domain=.baidu.com; Path=/", ptoken);
            self.cookie_jar.add_cookie_str(&ptoken_cookie, &url);
        }

        info!("已恢复 BDUSS/STOKEN/PTOKEN 到 Cookie Jar");
    }

    /// 验证 BDUSS 是否有效
    ///
    /// 通过调用网盘用户信息接口来验证 BDUSS
    /// 复用 QRCodeAuth::verify_bduss 相同的 API 逻辑
    async fn verify_bduss(&self) -> bool {
        info!("验证 BDUSS 是否有效...");

        let url = format!(
            "{}?method=query&clienttype={}&app_id={}&web=1",
            API_USER_INFO, CLIENT_TYPE, BAIDU_APP_ID
        );

        match self
            .client
            .get(&url)
            .header("Cookie", format!("BDUSS={}", self.user_auth.bduss))
            .header("User-Agent", USER_AGENT)
            .send()
            .await
        {
            Ok(resp) => {
                match resp.json::<Value>().await {
                    Ok(json) => {
                        // 检查 user_info 是否存在且有效
                        let user_info = &json["user_info"];
                        let uk = user_info["uk"].as_u64().unwrap_or(0);
                        if uk > 0 {
                            let username = user_info["username"]
                                .as_str()
                                .or_else(|| user_info["baidu_name"].as_str())
                                .unwrap_or("未知");
                            info!("BDUSS 有效，用户: {}, UID: {}", username, uk);
                            true
                        } else {
                            warn!("BDUSS 已失效：用户信息无效");
                            false
                        }
                    }
                    Err(e) => {
                        warn!("BDUSS 验证失败：解析响应失败 {}", e);
                        // 网络/解析错误，假设有效让后续逻辑处理
                        true
                    }
                }
            }
            Err(e) => {
                warn!("BDUSS 验证失败：请求失败 {}", e);
                // 网络错误，假设有效让后续逻辑处理
                true
            }
        }
    }

    /// 获取用户UID
    pub fn uid(&self) -> u64 {
        self.user_auth.uid
    }

    /// 获取用户BDUSS
    pub fn bduss(&self) -> &str {
        &self.user_auth.bduss
    }

    /// 获取文件列表
    ///
    /// # 参数
    /// * `dir` - 目录路径（如 "/" 或 "/test"）
    /// * `page` - 页码（从1开始）
    /// * `page_size` - 每页数量（默认100）
    ///
    /// # 返回
    /// 文件列表响应
    pub async fn get_file_list(
        &self,
        dir: &str,
        page: u32,
        page_size: u32,
    ) -> Result<FileListResponse> {
        info!("获取文件列表: dir={}, page={}", dir, page);

        let url = "https://pan.baidu.com/rest/2.0/xpan/file";

        let response = self
            .client
            .get(url)
            .query(&[
                ("method", "list"),
                ("order", "name"),
                ("desc", "0"),
                ("showempty", "0"),
                ("web", "1"),
                ("page", &page.to_string()),
                ("num", &page_size.to_string()),
                ("dir", dir),
                ("t", &chrono::Utc::now().timestamp_millis().to_string()),
            ])
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.mobile_user_agent)
            .send()
            .await
            .context("Failed to fetch file list")?;

        let file_list: FileListResponse = response
            .json()
            .await
            .context("Failed to parse file list response")?;

        if file_list.errno != 0 {
            anyhow::bail!("API error {}: {}", file_list.errno, file_list.errmsg);
        }

        debug!("获取到 {} 个文件/文件夹", file_list.list.len());
        Ok(file_list)
    }

    /// 获取Locate下载链接（通过文件路径）
    ///
    /// # 参数
    /// * `path` - 文件路径（如 "/apps/test/file.zip"）
    ///
    /// # 返回
    /// 下载URL数组
    pub async fn get_locate_download_url(&self, path: &str) -> Result<Vec<String>> {
        info!("获取Locate下载链接: path={}", path);

        // 1. 检查 UID
        if self.uid() == 0 {
            error!("UID 未设置，无法获取下载链接");
            anyhow::bail!("UID 未设置，请先登录");
        }

        // 2. 生成Locate签名
        let sign = LocateSign::new(self.uid(), self.bduss());

        // 3. 构建完整UR
        let url = format!(
            "https://pcs.baidu.com/rest/2.0/pcs/file?\
             ant=1&\
             check_blue=1&\
             es=1&\
             esl=1&\
             app_id=250528&\
             method=locatedownload&\
             path={}&\
             ver=4.0&\
             clienttype=17&\
             channel=0&\
             apn_id=1_0&\
             freeisp=0&\
             queryfree=0&\
             use=0&\
             {}",
            urlencoding::encode(path),
            sign.url_params()
        );

        debug!("Locate 请求 URL: {}", url);
        debug!("UID: {}", self.uid());
        debug!("BDUSS: {}...", &self.bduss()[..20.min(self.bduss().len())]);

        // 4. 发送 POST 请求
        let response = match self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.mobile_user_agent)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                error!("发送 Locate 下载请求失败: path={}, 错误: {}", path, e);
                return Err(e).context("发送 Locate 下载请求失败");
            }
        };

        let status = response.status();
        info!("Locate 请求响应状态: {} (path={})", status, path);

        // 5. 检查响应状态
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            error!(
                "HTTP 请求失败: status={}, path={}, 响应: {}",
                status, path, error_text
            );
            anyhow::bail!("HTTP 请求失败: {} - {}", status, error_text);
        }

        // 6. 解析响应
        let response_text = match response.text().await {
            Ok(text) => text,
            Err(e) => {
                error!("读取响应内容失败: path={}, 错误: {}", path, e);
                return Err(e).context("读取响应内容失败");
            }
        };

        debug!("响应内容: {}", response_text);

        let json: serde_json::Value = match serde_json::from_str(&response_text) {
            Ok(j) => j,
            Err(e) => {
                error!(
                    "解析 JSON 响应失败: path={}, 错误: {}, 响应: {}",
                    path, e, response_text
                );
                return Err(e).context("解析 JSON 响应失败");
            }
        };

        // 7. 检查错误码
        if let Some(errno) = json["errno"].as_i64() {
            if errno != 0 {
                let errmsg = json["errmsg"].as_str().unwrap_or("未知错误");
                error!(
                    "百度 API 返回错误: errno={}, errmsg={}, path={}",
                    errno, errmsg, path
                );
                anyhow::bail!("百度 API 错误 {}: {}", errno, errmsg);
            }
        }

        // 8. 提取下载链接
        let urls = match json["urls"].as_array() {
            Some(urls_array) => {
                urls_array
                    .iter()
                    .filter(|u| u["encrypt"].as_i64() == Some(0)) // 只要非加密链接
                    .filter_map(|u| u["url"].as_str())
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            }
            None => {
                error!(
                    "响应中没有 urls 字段: path={}, JSON: {}",
                    path, response_text
                );
                anyhow::bail!("响应中没有 urls 字段");
            }
        };

        if urls.is_empty() {
            error!(
                "未找到可用的下载链接: path={}, 响应: {}",
                path, response_text
            );
            anyhow::bail!("未找到可用的下载链接");
        }

        info!("成功获取 {} 个下载链接", urls.len());
        Ok(urls)
    }

    /// 获取Locate下载链接（批量，通过文件ID）
    ///
    /// # 参数
    /// * `fs_ids` - 文件服务器ID列表
    ///
    /// # 返回
    /// Locate下载响应
    pub async fn get_locate_download_urls(&self, fs_ids: &[u64]) -> Result<LocateDownloadResponse> {
        info!("获取Locate下载链接: {} 个文件", fs_ids.len());

        // 注意：这个API可能需要不同的处理方式
        // 目前优先使用 get_locate_download_url (通过路径)

        anyhow::bail!("批量下载暂不支持，请使用 get_locate_download_url (通过文件路径)")
    }

    /// 获取单个文件的下载链接（通过文件路径）
    ///
    /// # 参数
    /// * `path` - 文件路径
    /// * `dlink_prefer` - 链接优先级索引（从0开始，默认使用第几个备选下载链接）
    ///
    /// # 返回
    /// 最优下载URL
    ///
    /// # 链接选择逻辑
    /// 1. 根据 dlink_prefer 选择链接索引
    /// 2. 如果索引超出范围，使用最后一个链接
    /// 3. 如果选中的链接是 nb.cache 开头且有更多链接，自动使用下一个链接
    pub async fn get_download_url(&self, path: &str, dlink_prefer: usize) -> Result<String> {
        let urls = self.get_locate_download_url(path).await?;

        if urls.is_empty() {
            anyhow::bail!("未找到可用的下载链接");
        }

        // 1. 边界检查：如果 dlink_prefer 超出范围，使用最后一个链接
        let mut selected_index = if dlink_prefer >= urls.len() {
            urls.len() - 1
        } else {
            dlink_prefer
        };

        // 2. 选择链接
        let mut selected_url = &urls[selected_index];

        // 3. 跳过 nb.cache 链接（如果选中的是 nb.cache 且有更多链接可用）
        if selected_url.starts_with("http://nb.cache")
            || selected_url.starts_with("https://nb.cache")
        {
            if selected_index + 1 < urls.len() {
                // 使用下一个链接
                selected_index += 1;
                selected_url = &urls[selected_index];
                info!(
                    "检测到 nb.cache 链接，自动切换到下一个链接 (索引: {})",
                    selected_index
                );
            } else {
                warn!("所有链接都是 nb.cache，使用当前链接");
            }
        }

        info!(
            "选择下载链接 (索引: {}, 总数: {}): {}",
            selected_index,
            urls.len(),
            selected_url
        );

        Ok(selected_url.clone())
    }

    // =====================================================
    // 上传相关 API
    // =====================================================

    /// 创建文件
    ///
    /// # 参数
    /// * `remote_path` - 网盘目标路径
    /// * `file_size` - 文件大小
    /// * `upload_id` - 上传ID（从 precreate 获取）
    /// * `block_list` - 所有分片的 MD5 列表（JSON 数组格式，按顺序）
    ///
    /// # 返回
    /// 创建文件响应
    pub async fn create_file(
        &self,
        remote_path: &str,
        block_list: &str,
        upload_id: &str,
        file_size: u64,
        is_dir: &str
    ) -> Result<RapidUploadResponse> {

        let url = "https://pan.baidu.com/api/create";

        let response = self
            .client
            .post(url)
            // .query(&[("method", "create")])
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.mobile_user_agent)
            .form(&[
                ("path", remote_path),
                ("size", &file_size.to_string()),
                ("isdir", &is_dir),
                ("uploadid", &upload_id),
                // rtype 文件命名策略:
                // 1 = path冲突时重命名 (推荐,避免覆盖)
                // 2 = path冲突且block_list不同时重命名 (智能去重)
                // 3 = path冲突时覆盖 (危险)
                ("rtype", "1"),
                ("block_list", &block_list),
            ])
            .send()
            .await
            .context("创建文件请求发送失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取创建文件响应失败")?;

        info!("创建文件响应: status={}, body={}", status, response_text);

        let rapid_response: RapidUploadResponse =
            serde_json::from_str(&response_text).context("解析创建文件响应失败")?;

        if rapid_response.is_success() {
            info!(
                "创建文件成功: path={}, fs_id={}",
                remote_path, rapid_response.fs_id
            );
        } else if rapid_response.file_not_exist() {
            info!("创建文件失败，文件不存在: errno={}", rapid_response.errno);
        } else {
            info!(
                "创建文件失败: errno={}, errmsg={}",
                rapid_response.errno, rapid_response.errmsg
            );
        }

        Ok(rapid_response)
    }

    // =====================================================
    // 上传服务器定位
    // =====================================================

    /// 获取上传服务器列表
    ///
    /// 调用 locateupload 接口动态获取可用的 PCS 上传服务器
    ///
    /// # 返回
    /// 上传服务器主机名列表（如 `["d.pcs.baidu.com", "c.pcs.baidu.com"]`）
    pub async fn locate_upload(&self) -> Result<Vec<String>> {
        info!("获取上传服务器列表");

        let url = format!(
            "https://pcs.baidu.com/rest/2.0/pcs/file?\
             method=locateupload&\
             upload_version=2.0&\
             app_id={}",
            BAIDU_APP_ID
        );

        let response = self
            .client
            .get(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.mobile_user_agent)
            .send()
            .await
            .context("获取上传服务器请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取上传服务器响应失败")?;

        debug!("locate_upload 响应: status={}, body={}", status, response_text);

        let locate_response: crate::netdisk::LocateUploadResponse =
            serde_json::from_str(&response_text).context("解析上传服务器响应失败")?;

        if !locate_response.is_success() {
            anyhow::bail!(
                "获取上传服务器失败: error_code={}, error_msg={}",
                locate_response.error_code,
                locate_response.error_msg
            );
        }

        let servers = locate_response.server_hosts();
        info!("获取到上传服务器: {:?} (有效期: {}秒)", servers, locate_response.expire);

        Ok(servers)
    }

    /// 预创建文件（上传前的准备步骤）
    ///
    /// # 参数
    /// * `remote_path` - 网盘目标路径
    /// * `file_size` - 文件大小
    /// * `block_list` - 分片 MD5 列表（JSON 数组格式，如 `["md5_1", "md5_2"]`）
    ///
    /// # 返回
    /// 预创建响应（包含 uploadid）
    pub async fn precreate(
        &self,
        remote_path: &str,
        file_size: u64,
        block_list: &str,
    ) -> Result<PrecreateResponse> {
        info!("预创建文件: path={}, size={}", remote_path, file_size);

        let url = "https://pan.baidu.com/api/precreate";

        let response = self
            .client
            .post(url)
            // .query(&[("method", "precreate")])
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.mobile_user_agent)
            .form(&[
                ("path", remote_path),
                ("size", &file_size.to_string()),
                ("isdir", "0"),
                ("autoinit", "1"),
                // rtype 文件命名策略:
                // 1 = path冲突时重命名 (推荐,避免覆盖)
                // 2 = path冲突且block_list不同时重命名 (智能去重)
                // 3 = path冲突时覆盖 (危险)
                ("rtype", "1"),
                ("block_list", block_list),
            ])
            .send()
            .await
            .context("预创建请求发送失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取预创建响应失败")?;

        debug!("预创建响应: status={}, body={}", status, response_text);

        let precreate_response: PrecreateResponse =
            serde_json::from_str(&response_text).context("解析预创建响应失败")?;

        if precreate_response.errno != 0 {
            error!(
                "预创建失败: errno={}, errmsg={}",
                precreate_response.errno, precreate_response.errmsg
            );
            anyhow::bail!(
                "预创建失败: {} - {}",
                precreate_response.errno,
                precreate_response.errmsg
            );
        }

        info!(
            "预创建成功: uploadid={}, return_type={}",
            precreate_response.uploadid, precreate_response.return_type
        );

        Ok(precreate_response)
    }

    /// 上传分片
    ///
    /// # 参数
    /// * `remote_path` - 网盘目标路径
    /// * `upload_id` - 上传ID（从 precreate 获取）
    /// * `part_seq` - 分片序号（从 0 开始）
    /// * `data` - 分片数据
    ///
    /// # 返回
    /// 上传分片响应（包含分片 MD5）
    pub async fn upload_chunk(
        &self,
        remote_path: &str,
        upload_id: &str,
        part_seq: usize,
        data: Vec<u8>,
        server: Option<&str>,
    ) -> Result<UploadChunkResponse> {
        // 使用传入的服务器或默认值
        let pcs_server = server.unwrap_or("d.pcs.baidu.com");

        info!(
            "上传分片: path={}, uploadid={}..., part={}, size={}, server={}",
            remote_path,
            &upload_id[..8.min(upload_id.len())],
            part_seq,
            data.len(),
            pcs_server
        );

        // 使用 PCS 上传接口
        let url = format!(
            "https://{}/rest/2.0/pcs/superfile2?\
             method=upload&\
             app_id={}&\
             type=tmpfile&\
             path={}&\
             uploadid={}&\
             partseq={}",
            pcs_server,
            BAIDU_APP_ID,
            urlencoding::encode(remote_path),
            urlencoding::encode(upload_id),
            part_seq
        );

        // 构建 multipart form
        let part = multipart::Part::bytes(data)
            .file_name("file")
            .mime_str("application/octet-stream")?;

        let form = multipart::Form::new().part("file", part);

        let response = self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.mobile_user_agent)
            .multipart(form)
            .send()
            .await
            .context("上传分片请求发送失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取上传分片响应失败")?;

        debug!(
            "上传分片响应: part={}, status={}, body={}",
            part_seq, status, response_text
        );

        let chunk_response: UploadChunkResponse =
            serde_json::from_str(&response_text).with_context(|| {
                format!(
                    "解析上传分片响应失败: status={}, body={}",
                    status, response_text
                )
            })?;

        if !chunk_response.is_success() {
            let error_kind = UploadErrorKind::from_errno(chunk_response.error_code);
            error!(
                "上传分片失败: part={}, error_code={}, error_msg={}, retriable={}",
                part_seq,
                chunk_response.error_code,
                chunk_response.error_msg,
                error_kind.is_retriable()
            );
            anyhow::bail!(
                "上传分片失败: {} - {}",
                chunk_response.error_code,
                chunk_response.error_msg
            );
        }

        debug!(
            "上传分片成功: part={}, md5={}",
            part_seq, chunk_response.md5
        );

        Ok(chunk_response)
    }

    // /// 创建文件（合并分片，完成上传）
    // ///
    // /// # 参数
    // /// * `remote_path` - 网盘目标路径
    // /// * `file_size` - 文件大小
    // /// * `upload_id` - 上传ID（从 precreate 获取）
    // /// * `block_list` - 所有分片的 MD5 列表（JSON 数组格式，按顺序）
    // ///
    // /// # 返回
    // /// 创建文件响应
    // pub async fn create_file(
    //     &self,
    //     remote_path: &str,
    //     file_size: u64,
    //     upload_id: &str,
    //     block_list: &str,
    // ) -> Result<CreateFileResponse> {
    //     info!(
    //         "创建文件: path={}, size={}, uploadid={}...",
    //         remote_path,
    //         file_size,
    //         &upload_id[..8.min(upload_id.len())]
    //     );
    //
    //     let url = "https://pan.baidu.com/rest/2.0/xpan/file";
    //
    //     let response = self
    //         .client
    //         .post(url)
    //         .query(&[("method", "create")])
    //         .header("Cookie", format!("BDUSS={}", self.bduss()))
    //         .header("User-Agent", &self.mobile_user_agent)
    //         .form(&[
    //             ("path", remote_path),
    //             ("size", &file_size.to_string()),
    //             ("isdir", "0"),
    //             // rtype 文件命名策略:
    //             // 1 = path冲突时重命名 (推荐,避免覆盖)
    //             // 2 = path冲突且block_list不同时重命名 (智能去重)
    //             // 3 = path冲突时覆盖 (危险)
    //             ("rtype", "1"),
    //             ("uploadid", upload_id),
    //             ("block_list", block_list),
    //         ])
    //         .send()
    //         .await
    //         .context("创建文件请求发送失败")?;
    //
    //     let status = response.status();
    //     let response_text = response.text().await.context("读取创建文件响应失败")?;
    //
    //     debug!("创建文件响应: status={}, body={}", status, response_text);
    //
    //     let create_response: CreateFileResponse =
    //         serde_json::from_str(&response_text).context("解析创建文件响应失败")?;
    //
    //     if !create_response.is_success() {
    //         error!(
    //             "创建文件失败: errno={}, errmsg={}",
    //             create_response.errno, create_response.errmsg
    //         );
    //         anyhow::bail!(
    //             "创建文件失败: {} - {}",
    //             create_response.errno,
    //             create_response.errmsg
    //         );
    //     }
    //
    //     info!(
    //         "创建文件成功: path={}, fs_id={}, size={}",
    //         create_response.path, create_response.fs_id, create_response.size
    //     );
    //
    //     Ok(create_response)
    // }

    /// 获取上传服务器列表
    ///
    /// # 返回
    /// PCS 上传服务器地址列表
    pub async fn get_upload_servers(&self) -> Result<Vec<String>> {
        // 百度网盘的上传服务器是固定的几个
        // 实际使用时会根据 precreate 响应或者 locateupload API 获取
        // 这里返回默认的服务器列表
        Ok(vec![
            "d.pcs.baidu.com".to_string(),
            "c.pcs.baidu.com".to_string(),
            "pcs.baidu.com".to_string(),
        ])
    }

    /// 从 CookieJar 收集所有 .baidu.com 的 cookie
    pub async fn collect_all_baidu_cookies(&self) -> Result<String> {
        let domains = [
            "https://baidu.com/",
            "https://www.baidu.com/",
            "https://pan.baidu.com/",
            "https://pcs.baidu.com/",
        ];

        let mut result = vec![];

        for d in domains {
            let url = d.parse::<reqwest::Url>()?;
            if let Some(header) = self.cookie_jar.cookies(&url) {
                if let Ok(s) = header.to_str() {
                    for kv in s.split("; ") {
                        if !result.contains(&kv.to_string()) {
                            result.push(kv.to_string());
                        }
                    }
                }
            }
        }

        // 强制保证 BDUSS + PANPSC 必定存在
        if !result.iter().any(|x| x.starts_with("BDUSS=")) {
            let bd = format!("BDUSS={}", self.user_auth.bduss);
            result.push(bd);
        }

        let panpsc_val = self.panpsc_cookie.lock().await.clone();
        if !result.iter().any(|x| x.starts_with("PANPSC=")) {
            if let Some(v) = panpsc_val {
                result.push(format!("PANPSC={}", v));
            }
        }

        let merged = result.join("; ");
        Ok(merged)
    }

    /// 创建文件夹
    ///
    /// # 参数
    /// * `remote_path` - 网盘目标路径（必须以 / 开头）
    ///
    /// # 返回
    /// 创建文件夹响应
    pub async fn create_folder(&self, remote_path: &str) -> Result<CreateFileResponse> {
        // 获取锁
        let token_guard = self.bdstoken.lock().await;

        if token_guard.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            return Err(anyhow::anyhow!("bdstoken 尚未获取，请尝试重新登录"));
        }

        info!("创建文件夹: path={}", remote_path);

        // 打印创建文件夹前的 Cookie Jar 状态
        self.debug_print_cookies("创建文件夹前");
        // 2. 统一从 CookieJar 中收集所有 domain = .baidu.com 的 cookie
        let merged_cookie_str = self.collect_all_baidu_cookies().await?;
        // 3. 创建独立的 HTTP Client，确保我们自定义的 Cookie Header 不会被覆盖
        let pan_client = reqwest::Client::builder()
            .cookie_store(false) // 强制禁止自动 cookie → 我们自己设置 Cookie 头
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        // 使用 Web 端 API (与 Baidu 网页端保持一致)
        let mut url = format!(
            "https://pan.baidu.com/api/create?a=commit&clienttype=0&app_id={}&web=1",
            BAIDU_APP_ID,
        );

        if let Some(token) = self.bdstoken.lock().await.clone() {
            info!("创建文件夹: 使用 bdstoken 参数");
            url.push_str("&bdstoken=");
            url.push_str(&urlencoding::encode(&token));
        } else {
            return Err(anyhow::anyhow!("bdstoken 尚未获取，请尝试重新登录"));
        }

        debug!("创建文件夹 URL: {}", url);

        // 打印创建文件夹前的 Cookie Jar 状态
        self.debug_print_cookies("创建文件夹前");
        // 4. 手动构造 Cookie header（不会被覆盖）
        let mut headers = HeaderMap::new();
        headers.insert("User-Agent", HeaderValue::from_str(&self.web_user_agent)?);
        headers.insert("Cookie", HeaderValue::from_str(&merged_cookie_str)?);

        // Cookie Jar 会自动携带所有 cookies (必须使用 .send() 而不是 .build().execute())
        info!("发送创建文件夹请求...");

        let response = pan_client
            .post(&url)
            .headers(headers)
            .form(&[
                ("path", remote_path),
                ("isdir", "1"),
                ("block_list", "[]"),
            ])
            .send()
            .await
            .context("创建文件夹请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取创建文件夹响应失败")?;

        info!("创建文件夹响应: status={}, body={}", status, response_text);

        // 解析响应
        #[derive(Debug, serde::Deserialize)]
        struct BasicResponse {
            #[serde(default)]
            errno: i32,
            #[serde(default)]
            errmsg: String,
            #[serde(default)]
            error_code: i32,
            #[serde(default)]
            error_msg: String,
        }

        let basic_response: BasicResponse =
            serde_json::from_str(&response_text).context("解析创建文件夹响应失败")?;

        let error_code = if basic_response.errno != 0 {
            basic_response.errno
        } else {
            basic_response.error_code
        };

        let error_msg = if !basic_response.errmsg.is_empty() {
            basic_response.errmsg
        } else {
            basic_response.error_msg
        };

        if error_code != 0 {
            error!(
                "创建文件夹失败: error_code={}, error_msg={}",
                error_code, error_msg
            );
            anyhow::bail!("创建文件夹失败: {} - {}", error_code, error_msg);
        }

        info!("创建文件夹成功: path={}", remote_path);
        self.debug_print_cookies("创建文件夹成功 Cookie 状态");

        let create_response: CreateFileResponse = serde_json::from_str(&response_text)
            .unwrap_or_else(|_| CreateFileResponse {
                errno: 0,
                fs_id: 0,
                md5: String::new(),
                server_filename: String::new(),
                path: remote_path.to_string(),
                size: 0,
                ctime: 0,
                mtime: 0,
                isdir: 1,
                errmsg: String::new(),
            });

        Ok(create_response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_user_auth() -> UserAuth {
        UserAuth::new(123456789, "test_user".to_string(), "test_bduss".to_string())
    }

    #[test]
    fn test_netdisk_client_creation() {
        let user_auth = create_test_user_auth();
        let client = NetdiskClient::new(user_auth.clone());

        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.uid(), user_auth.uid);
        assert_eq!(client.bduss(), user_auth.bduss);
    }

    #[test]
    fn test_default_user_agent() {
        let ua = NetdiskClient::default_mobile_user_agent();
        assert!(ua.contains("netdisk"));
        assert!(ua.contains("android"));
    }
}
