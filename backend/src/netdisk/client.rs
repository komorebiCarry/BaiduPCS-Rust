// 网盘客户端实现

use crate::auth::constants::USER_AGENT as WEB_USER_AGENT; // 导入登录时的 UA,确保一致
use crate::auth::constants::{API_USER_INFO, BAIDU_APP_ID, CLIENT_TYPE, USER_AGENT};
use crate::auth::UserAuth;
use crate::netdisk::{
    CreateFileResponse, FileListResponse, LocateDownloadResponse, PrecreateResponse,
    RapidUploadResponse, UploadChunkResponse, UploadErrorKind,
};
use crate::sign::LocateSign;
use anyhow::{Context, Result};
use reqwest::cookie::CookieStore;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::multipart;
use reqwest::Client;
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
            user_auth.panpsc.is_some()
                && user_auth.csrf_token.is_some()
                && user_auth.bdstoken.is_some(),
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
                        debug!("{}: 请求头快照 -> UA={}, Referer={}", step, ua, referer);
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

        let simple_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
        let _body1 = exec_request(
            client,
            client.get(home_url).header("User-Agent", simple_ua),
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
    pub async fn warmup_and_get_cookies(
        &self,
    ) -> Result<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> {
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

                    info!(
                        "预热完成,提取到 Cookie: PANPSC={}, csrfToken={}, bdstoken={}, STOKEN={}",
                        panpsc.is_some(),
                        csrf_token.is_some(),
                        bdstoken.is_some(),
                        stoken.is_some()
                    );

                    return Ok((panpsc, csrf_token, bdstoken, stoken));
                }
                Err(e) => {
                    warn!("预热第 {} 次尝试失败: {}", attempt + 1, e);
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
        is_dir: &str,
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

        debug!(
            "locate_upload 响应: status={}, body={}",
            status, response_text
        );

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
        info!(
            "获取到上传服务器: {:?} (有效期: {}秒)",
            servers, locate_response.expire
        );

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

        let chunk_response: UploadChunkResponse = serde_json::from_str(&response_text)
            .with_context(|| {
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
        // 获取 bdstoken（只获取一次锁，立即克隆并释放，避免死锁）
        let bdstoken = {
            let token_guard = self.bdstoken.lock().await;
            match token_guard.as_ref() {
                Some(token) if !token.is_empty() => token.clone(),
                _ => return Err(anyhow::anyhow!("bdstoken 尚未获取，请尝试重新登录")),
            }
        };

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
        let url = format!(
            "https://pan.baidu.com/api/create?a=commit&clienttype=0&app_id={}&web=1&bdstoken={}",
            BAIDU_APP_ID,
            urlencoding::encode(&bdstoken),
        );
        info!("创建文件夹: 使用 bdstoken 参数");

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
            .form(&[("path", remote_path), ("isdir", "1"), ("block_list", "[]")])
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
            anyhow::bail!("创建文件夹失败: errno={}, msg={}", error_code, error_msg);
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

    // =====================================================
    // 分享链接转存相关 API
    // =====================================================

    /// 解析分享链接，提取 short_key
    ///
    /// 支持格式：
    /// - https://pan.baidu.com/s/1abcDEFg
    /// - https://pan.baidu.com/s/1abcDEFg?pwd=xxxx
    /// - https://pan.baidu.com/share/init?surl=abcDEFg
    ///
    /// # 返回
    /// ShareLink 结构体，包含 short_key 和可能的密码
    pub fn parse_share_link(&self, url: &str) -> Result<crate::transfer::ShareLink> {
        use regex::Regex;

        let url = url.trim();

        // 检查是否为百度网盘链接
        if !url.contains("pan.baidu.com") && !url.contains("baidu.com/s/") {
            anyhow::bail!("无效的分享链接：不是百度网盘链接");
        }

        let mut short_key: Option<String> = None;
        let mut password: Option<String> = None;

        // 尝试匹配 /s/{key} 格式
        // 例如: https://pan.baidu.com/s/1abcDEFg
        let re_s = Regex::new(r"/s/([a-zA-Z0-9_-]+)")?;
        if let Some(caps) = re_s.captures(url) {
            if let Some(key) = caps.get(1) {
                short_key = Some(key.as_str().to_string());
            }
        }

        // 尝试匹配 /share/init?surl={key} 格式
        // 例如: https://pan.baidu.com/share/init?surl=abcDEFg
        if short_key.is_none() {
            let re_surl = Regex::new(r"[?&]surl=([a-zA-Z0-9_-]+)")?;
            if let Some(caps) = re_surl.captures(url) {
                if let Some(key) = caps.get(1) {
                    // surl 格式需要加 "1" 前缀
                    short_key = Some(format!("1{}", key.as_str()));
                }
            }
        }

        // 提取密码
        // 格式: ?pwd=xxxx 或 &pwd=xxxx
        let re_pwd = Regex::new(r"[?&]pwd=([a-zA-Z0-9]{4})")?;
        if let Some(caps) = re_pwd.captures(url) {
            if let Some(pwd) = caps.get(1) {
                password = Some(pwd.as_str().to_string());
            }
        }

        match short_key {
            Some(key) => {
                info!(
                    "解析分享链接成功: short_key={}, has_password={}",
                    key,
                    password.is_some()
                );
                Ok(crate::transfer::ShareLink {
                    short_key: key,
                    raw_url: url.to_string(),
                    password,
                })
            }
            None => {
                anyhow::bail!("无法从链接中提取分享 ID")
            }
        }
    }

    /// 访问分享页面，获取分享信息
    ///
    /// # 参数
    /// * `short_key` - 分享短链 ID（如 "1abcDEFg"）
    /// * `first` - 是否为首次访问（影响 Referer）
    ///
    /// # 返回
    /// SharePageInfo 或错误（需要密码/分享失效/页面不存在）
    pub async fn access_share_page(
        &self,
        short_key: &str,
        password: &Option<String>,
        first: bool,
    ) -> Result<crate::transfer::SharePageInfo> {
        use regex::Regex;

        let share_link = format!("https://pan.baidu.com/s/{}", short_key);
        let referer = if first {
            "https://pan.baidu.com/disk/home".to_string()
        } else {
            format!("https://pan.baidu.com/share/init?surl={}", &short_key[1..])
        };

        info!("访问分享页面: {}", share_link);

        let response = self
            .client
            .get(&share_link)
            .header("User-Agent", WEB_USER_AGENT)
            .header("Referer", &referer)
            .send()
            .await
            .context("访问分享页面失败")?;

        let status = response.status();
        let body = response.text().await.context("读取分享页面失败")?;

        debug!("分享页面响应: status={}, body_len={}", status, body.len());

        // 检测页面状态
        if body.contains("platform-non-found") {
            anyhow::bail!("分享已失效");
        }
        if body.contains("error-404") {
            anyhow::bail!("分享不存在");
        }

        // 检测是否需要密码
        // 如果页面包含密码输入框或验证逻辑，说明需要密码
        let need_password = body.contains("请输入提取码")
            || body.contains("accesscode")
            || body.contains("verify-form");

        // 从页面 JS 中提取分享信息（即使需要密码，这些信息可能仍然存在）
        // 匹配模式: {... "loginstate":... }
        let re = Regex::new(r"\{[^{}]*loginstate[^{}]*\}")?;

        // 尝试更宽松的匹配
        let re_loose = Regex::new(r#""shareid"\s*:\s*(\d+)"#)?;
        let re_uk = Regex::new(r#""uk"\s*:\s*(\d+)"#)?;
        let re_share_uk = Regex::new(r#""share_uk"\s*:\s*"?(\d+)"?"#)?;
        let re_bdstoken = Regex::new(r#""bdstoken"\s*:\s*"([^"]+)""#)?;

        // 辅助函数：安全地从 JSON Value 提取字符串或数字
        fn extract_json_value(value: &serde_json::Value) -> Option<String> {
            match value {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            }
        }

        // 尝试完整匹配
        if let Some(caps) = re.find(&body) {
            let json_str = caps.as_str();
            if let Ok(json) = serde_json::from_str::<Value>(json_str) {
                // 安全提取字段，统一处理字符串和数字类型
                let shareid = extract_json_value(&json["shareid"]).unwrap_or_default();
                let uk = extract_json_value(&json["uk"]).unwrap_or_default();
                let share_uk = extract_json_value(&json["share_uk"]).unwrap_or_default();
                let bdstoken = extract_json_value(&json["bdstoken"]).unwrap_or_default();

                if !shareid.is_empty() {
                    info!("从 JSON 提取分享信息: shareid={}, uk={}", shareid, uk);
                    return Ok(crate::transfer::SharePageInfo {
                        shareid,
                        uk,
                        share_uk,
                        bdstoken,
                    });
                }
            }
        }

        // 使用宽松匹配提取各个字段
        let shareid = re_loose
            .captures(&body)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        let uk = re_uk
            .captures(&body)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        let share_uk = re_share_uk
            .captures(&body)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| uk.clone());

        let bdstoken = re_bdstoken
            .captures(&body)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        let _password = password
            .as_deref()
            .map(|s| s.trim())
            .unwrap_or("")
            .is_empty();
        if shareid.is_empty() {
            // 尝试检测是否需要登录
            if body.contains("passport.baidu.com") || body.contains("请登录") {
                anyhow::bail!("请先登录百度账号");
            }
            // 如果需要密码且无法提取分享信息，提示需要密码
            if need_password && _password {
                anyhow::bail!("需要密码");
            }
            anyhow::bail!("无法提取分享信息，请确认链接有效");
        }

        // 检测到需要密码时，返回错误让调用方处理
        if need_password && _password {
            anyhow::bail!("需要密码");
        }

        info!("提取分享信息成功: shareid={}, uk={}", shareid, uk);

        Ok(crate::transfer::SharePageInfo {
            shareid,
            uk,
            share_uk,
            bdstoken,
        })
    }

    /// 校验提取码
    ///
    /// # 参数
    /// * `shareid` - 分享 ID
    /// * `share_uk` - 分享者 UK
    /// * `bdstoken` - CSRF 令牌
    /// * `password` - 提取码
    /// * `referer` - 来源页面
    ///
    /// # 返回
    /// 成功返回 randsk，失败返回错误
    pub async fn verify_share_password(
        &self,
        shareid: &str,
        share_uk: &str,
        bdstoken: &str,
        password: &str,
        referer: &str,
    ) -> Result<String> {
        info!("验证提取码: shareid={}", shareid);

        let timestamp = chrono::Utc::now().timestamp_millis();
        let url = format!(
            "https://pan.baidu.com/share/verify?shareid={}&uk={}&t={}&clienttype=1",
            shareid, share_uk, timestamp
        );

        let response = self
            .client
            .post(&url)
            .header("User-Agent", &self.web_user_agent)
            .header("Referer", referer)
            .header(
                "Content-Type",
                "application/x-www-form-urlencoded; charset=UTF-8",
            )
            .form(&[
                ("pwd", password),
                ("vcode", ""),
                ("vcode_str", ""),
                ("bdstoken", bdstoken),
            ])
            .send()
            .await
            .context("验证提取码请求失败")?;

        let response_text = response.text().await.context("读取验证响应失败")?;
        debug!("验证提取码响应: {}", response_text);

        let json: Value = serde_json::from_str(&response_text).context("解析验证响应失败")?;

        let errno = json["errno"].as_i64().unwrap_or(-1);

        if errno == 0 {
            let randsk = json["randsk"].as_str().unwrap_or_default().to_string();
            info!(
                "提取码验证成功，获取到randsk: {}...",
                &randsk[..randsk.len().min(20)]
            );

            // 将 randsk 保存到 Cookie 中
            // 这样后续的转存请求可以从 Cookie 中读取 randsk
            let cookie_url = "https://pan.baidu.com"
                .parse::<reqwest::Url>()
                .context("解析 Cookie URL 失败")?;
            let randsk_cookie = format!("randsk={}; Domain=.baidu.com; Path=/", randsk);
            self.cookie_jar.add_cookie_str(&randsk_cookie, &cookie_url);
            info!("✅ 已将 randsk 保存到 Cookie");

            // 验证Cookie是否成功保存
            if let Some(cookies) = self.cookie_jar.cookies(&cookie_url) {
                let cookie_str = cookies.to_str().unwrap_or("");
                if cookie_str.contains("randsk=") {
                    info!("✅ 验证：Cookie中已包含randsk");
                } else {
                    warn!("❌ 警告：Cookie中未找到randsk，可能保存失败");
                }
            }

            Ok(randsk)
        } else if errno == -9 {
            anyhow::bail!("提取码错误")
        } else {
            anyhow::bail!("验证失败: errno={}", errno)
        }
    }

    /// 列出分享中的文件
    ///
    /// # 参数
    /// * `short_key` - 分享短链 ID
    /// * `shareid` - 分享 ID
    /// * `share_uk` - 分享者 UK
    /// * `bdstoken` - CSRF 令牌
    /// * `sekey` - 验证后的密钥（URL 编码的 randsk，可选）
    ///
    /// # 返回
    /// 分享文件列表
    pub async fn list_share_files(
        &self,
        short_key: &str,
        shareid: &str,
        bdstoken: &str,
    ) -> Result<Vec<crate::transfer::SharedFileInfo>> {
        info!("获取分享文件列表: shareid={}", shareid);

        // short_key 包含 '1'（如 "1abcDEFg"），需要去掉第一个字符
        let shorturl = if short_key.starts_with('1') && short_key.len() > 1 {
            &short_key[1..]
        } else {
            short_key
        };

        let url = format!(
            "https://pan.baidu.com/share/list?\
             shorturl={}&bdstoken={}&\
             root=1&web=5&app_id={}&channel=chunlei",
            shorturl, BAIDU_APP_ID, bdstoken
        );

        let referer = format!("https://pan.baidu.com/s/{}", short_key);

        let response = self
            .client
            .get(&url)
            .header("User-Agent", &self.web_user_agent)
            .header("Referer", &referer)
            .send()
            .await
            .context("获取分享文件列表失败")?;

        let response_text = response.text().await.context("读取文件列表响应失败")?;
        debug!("文件列表响应: {}", response_text);

        let json: Value = serde_json::from_str(&response_text).context("解析文件列表响应失败")?;

        let errno = json["errno"].as_i64().unwrap_or(-1);
        if errno != 0 {
            // 提取错误消息
            let errmsg = json["errmsg"]
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| match errno {
                    132 => "您的帐号可能存在安全风险，为了确保为您本人操作，请先进行安全验证"
                        .to_string(),
                    -7 => "该分享已删除或已取消".to_string(),
                    -9 => "文件不存在".to_string(),
                    -12 => "访问密码错误".to_string(),
                    -19 => "需要输入验证码".to_string(),
                    -62 => "可能需要输入验证码".to_string(),
                    8001 => "已触发验证，请稍后再试".to_string(),
                    _ => format!("未知错误，错误码: {}", errno),
                });

            anyhow::bail!("获取文件列表失败: errno={}, errmsg={}", errno, errmsg);
        }

        let list = json["list"].as_array().context("文件列表格式错误")?;

        let mut files = Vec::new();
        for item in list {
            // fs_id 可能是字符串或数字，需要同时支持两种格式
            let fs_id = if let Some(id_str) = item["fs_id"].as_str() {
                id_str.parse::<u64>().unwrap_or(0)
            } else {
                item["fs_id"].as_u64().unwrap_or(0)
            };

            let is_dir = if let Some(n) = item["isdir"].as_i64() {
                n == 1
            } else if let Some(s) = item["isdir"].as_str() {
                s == "1"
            } else {
                false
            };
            let path = item["path"].as_str().unwrap_or_default().to_string();
            let size = if let Some(n) = item["size"].as_u64() {
                n
            } else if let Some(s) = item["size"].as_str() {
                s.parse::<u64>().unwrap_or(0)
            } else {
                0
            };
            let name = item["server_filename"]
                .as_str()
                .unwrap_or_default()
                .to_string();

            info!(
                "解析文件: fs_id={}, name={}, is_dir={}",
                fs_id, name, is_dir
            );

            files.push(crate::transfer::SharedFileInfo {
                fs_id,
                is_dir,
                path,
                size,
                name,
            });
        }

        Ok(files)
    }

    /// 执行转存
    ///
    /// # 参数
    /// * `shareid` - 分享 ID
    /// * `share_uk` - 分享者 UK
    /// * `bdstoken` - CSRF 令牌
    /// * `fs_ids` - 要转存的文件 fs_id 列表
    /// * `target_path` - 目标路径
    /// * `referer` - 来源页面
    /// * `sekey` - 验证后的密钥（URL 编码的 randsk，可选）
    ///
    /// # 返回
    /// 转存结果
    pub async fn transfer_share_files(
        &self,
        shareid: &str,
        share_uk: &str,
        bdstoken: &str,
        fs_ids: &[u64],
        target_path: &str,
        referer: &str,
    ) -> Result<crate::transfer::TransferResult> {
        // 构建转存URL
        let url = format!(
            "https://pan.baidu.com/share/transfer?\
             shareid={}&from={}&bdstoken={}&app_id={}&channel=chunlei&clienttype=0&web=1",
            shareid, share_uk, bdstoken, BAIDU_APP_ID
        );

        // 构建 fs_id 列表
        let fsidlist = format!(
            "[{}]",
            fs_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        let response = self
            .client
            .post(&url)
            .header("User-Agent", &self.web_user_agent)
            .header("Referer", referer)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&[("fsidlist", fsidlist.as_str()), ("path", target_path)])
            .send()
            .await
            .context("转存请求失败")?;

        let response_text = response.text().await.context("读取转存响应失败")?;
        info!("转存响应: {}", response_text);

        let json: Value = serde_json::from_str(&response_text).context("解析转存响应失败")?;

        let errno = json["errno"].as_i64().unwrap_or(-1);

        if errno == 0 {
            // 提取转存后的路径
            let extra_list = json["extra"]["list"].as_array();
            let mut transferred_paths = Vec::new();
            let mut transferred_fs_ids = Vec::new();

            if let Some(list) = extra_list {
                for item in list {
                    if let Some(path) = item["to"].as_str() {
                        transferred_paths.push(path.to_string());
                    }
                    if let Some(fsid) = item["to_fs_id"].as_u64() {
                        transferred_fs_ids.push(fsid);
                    }
                }
            }

            Ok(crate::transfer::TransferResult {
                success: true,
                transferred_paths,
                error: None,
                transferred_fs_ids,
            })
        } else if errno == 12 {
            // 部分错误
            let info_list = json["info"].as_array();
            if let Some(list) = info_list {
                if let Some(first) = list.first() {
                    let inner_errno = first["errno"].as_i64().unwrap_or(0);
                    if inner_errno == -30 {
                        let path = first["path"].as_str().unwrap_or_default();
                        let filename = std::path::Path::new(path)
                            .file_name()
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_else(|| path.to_string());
                        return Ok(crate::transfer::TransferResult {
                            success: false,
                            transferred_paths: vec![],
                            error: Some(format!("同名文件已存在: {}", filename)),
                            transferred_fs_ids: vec![],
                        });
                    }
                }
            }

            // 检查转存数量限制
            let target_file_nums = json["target_file_nums"].as_u64().unwrap_or(0);
            let target_file_nums_limit = json["target_file_nums_limit"].as_u64().unwrap_or(0);
            if target_file_nums > target_file_nums_limit {
                return Ok(crate::transfer::TransferResult {
                    success: false,
                    transferred_paths: vec![],
                    error: Some(format!(
                        "转存文件数 {} 超过上限 {}",
                        target_file_nums, target_file_nums_limit
                    )),
                    transferred_fs_ids: vec![],
                });
            }

            Ok(crate::transfer::TransferResult {
                success: false,
                transferred_paths: vec![],
                error: Some(format!("转存失败: {}", response_text)),
                transferred_fs_ids: vec![],
            })
        } else if errno == 4 {
            Ok(crate::transfer::TransferResult {
                success: false,
                transferred_paths: vec![],
                error: Some("文件重复".to_string()),
                transferred_fs_ids: vec![],
            })
        } else {
            Ok(crate::transfer::TransferResult {
                success: false,
                transferred_paths: vec![],
                error: Some(format!("转存失败: {}", response_text)),
                transferred_fs_ids: vec![],
            })
        }
    }

    // =====================================================
    // 离线下载（Cloud Download）相关 API
    // =====================================================

    /// 标准化磁力链接
    ///
    /// 百度网盘离线下载 API 只接受大写十六进制格式的 info hash。
    /// 此函数将 Base32 编码的 hash 转换为十六进制，并将小写转换为大写。
    /// 同时简化磁力链接，只保留 xt 参数，去掉 tracker 等其他参数。
    ///
    /// # 参数
    /// * `magnet_url` - 原始磁力链接
    ///
    /// # 返回
    /// 标准化后的磁力链接（格式：magnet:?xt=urn:btih:HASH）
    fn normalize_magnet_link(magnet_url: &str) -> String {
        // 检查是否是磁力链接
        if !magnet_url.to_lowercase().starts_with("magnet:?") {
            return magnet_url.to_string();
        }

        // 解析磁力链接参数，提取 info hash
        let query_start = magnet_url.find('?').unwrap_or(magnet_url.len());
        let query_str = &magnet_url[query_start + 1..];

        // 只提取 xt 参数中的 info hash，忽略其他参数（tr, dn 等）
        // 百度 API 只需要简单的 magnet:?xt=urn:btih:HASH 格式
        for param in query_str.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                if key == "xt" && value.to_lowercase().starts_with("urn:btih:") {
                    // 提取 info hash
                    let hash = &value[9..]; // 跳过 "urn:btih:"

                    let normalized_hash = if hash.len() == 32 {
                        // Base32 编码，需要转换为十六进制
                        match Self::base32_to_hex(hash) {
                            Some(hex) => {
                                info!(
                                    "磁力链接 hash 从 Base32 转换为十六进制: {} -> {}",
                                    hash, hex
                                );
                                hex
                            }
                            None => {
                                warn!("Base32 解码失败，保持原样: {}", hash);
                                hash.to_uppercase()
                            }
                        }
                    } else if hash.len() == 40 {
                        // 已经是十六进制，只需转换为大写
                        let upper = hash.to_uppercase();
                        if upper != hash {
                            info!("磁力链接 hash 转换为大写: {} -> {}", hash, upper);
                        }
                        upper
                    } else {
                        // 未知格式，保持原样
                        warn!("未知的 info hash 格式 (长度={}): {}", hash.len(), hash);
                        hash.to_string()
                    };

                    // 返回简化的磁力链接，只包含 xt 参数
                    let result = format!("magnet:?xt=urn:btih:{}", normalized_hash);
                    info!("磁力链接标准化完成: {} -> {}", magnet_url, result);
                    return result;
                }
            }
        }

        // 没有找到有效的 xt 参数，返回原始链接
        warn!("磁力链接中未找到有效的 xt 参数: {}", magnet_url);
        magnet_url.to_string()
    }

    /// 将 Base32 编码的字符串转换为十六进制
    ///
    /// BitTorrent 使用的是 RFC 4648 Base32 编码（无填充）
    fn base32_to_hex(base32: &str) -> Option<String> {
        // Base32 字母表 (RFC 4648)
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

        let input = base32.to_uppercase();
        let input_bytes = input.as_bytes();

        // 验证输入长度（32 字符 Base32 = 20 字节 = 40 字符十六进制）
        if input_bytes.len() != 32 {
            return None;
        }

        // 解码 Base32
        let mut bits: u64 = 0;
        let mut bit_count = 0;
        let mut output = Vec::with_capacity(20);

        for &c in input_bytes {
            let value = ALPHABET.iter().position(|&x| x == c)? as u64;
            bits = (bits << 5) | value;
            bit_count += 5;

            if bit_count >= 8 {
                bit_count -= 8;
                output.push((bits >> bit_count) as u8);
                bits &= (1 << bit_count) - 1;
            }
        }

        // 转换为十六进制（大写）
        let hex: String = output.iter().map(|b| format!("{:02X}", b)).collect();

        Some(hex)
    }

    /// 查询磁力链接信息，获取文件列表
    ///
    /// # 参数
    /// * `magnet_url` - 磁力链接
    /// * `save_path` - 保存路径
    ///
    /// # 返回
    /// 文件数量
    async fn cloud_dl_query_magnet_info(&self, magnet_url: &str, save_path: &str) -> Result<usize> {
        info!("查询磁力链接信息: {}", magnet_url);

        let url = "https://pan.baidu.com/rest/2.0/services/cloud_dl";

        let response = self
            .client
            .post(url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .form(&[
                ("method", "query_magnetinfo"),
                ("app_id", "250528"),
                ("source_url", magnet_url),
                ("save_path", save_path),
                ("type", "4"),
            ])
            .send()
            .await
            .context("查询磁力链接信息请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取磁力链接信息响应失败")?;

        info!("查询磁力链接信息响应: status={}, body={}", status, response_text);

        // 解析响应，提取文件数量
        let json: serde_json::Value =
            serde_json::from_str(&response_text).context("解析磁力链接信息响应失败")?;

        // 检查错误码
        if let Some(error_code) = json.get("error_code").and_then(|v| v.as_i64()) {
            if error_code != 0 {
                let error_msg = json
                    .get("error_msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("未知错误");
                anyhow::bail!("查询磁力链接信息失败: {} ({})", error_msg, error_code);
            }
        }

        // 提取文件列表
        let file_count = json
            .get("magnet_info")
            .and_then(|v| v.as_array())
            .map(|arr| arr.len())
            .unwrap_or(0);

        info!("磁力链接包含 {} 个文件", file_count);

        Ok(file_count)
    }

    /// 添加离线下载任务
    ///
    /// # 参数
    /// * `source_url` - 下载源链接（支持 HTTP/HTTPS/磁力链接/ed2k）
    /// * `save_path` - 网盘保存路径（默认为根目录 "/"）
    ///
    /// # 返回
    /// 新创建的任务 ID
    pub async fn cloud_dl_add_task(&self, source_url: &str, save_path: &str) -> Result<i64> {
        info!("添加离线下载任务: source_url={}, save_path={}", source_url, save_path);

        // 标准化磁力链接（将 Base32 转换为十六进制，小写转大写）
        let normalized_url = Self::normalize_magnet_link(source_url);

        // 判断是否为磁力链接
        let is_magnet = normalized_url.starts_with("magnet:");

        // 对于磁力链接，需要先查询文件列表，然后选择所有文件
        let selected_idx = if is_magnet {
            match self.cloud_dl_query_magnet_info(&normalized_url, save_path).await {
                Ok(file_count) if file_count > 0 => {
                    // 生成所有文件的索引：1,2,3,...,n（索引从1开始）
                    let indices: Vec<String> = (1..=file_count).map(|i| i.to_string()).collect();
                    indices.join(",")
                }
                Ok(_) => {
                    warn!("磁力链接文件列表为空，使用默认索引");
                    String::new()
                }
                Err(e) => {
                    warn!("查询磁力链接信息失败: {}，使用默认索引", e);
                    String::new()
                }
            }
        } else {
            // 非磁力链接不需要 selected_idx
            String::new()
        };

        info!("selected_idx={}", selected_idx);

        let url = format!(
            "https://pan.baidu.com/rest/2.0/services/cloud_dl?\
             method=add_task&\
             app_id=250528&\
             task_from=0&\
             selected_idx={}&\
             save_path={}&\
             source_url={}",
            urlencoding::encode(&selected_idx),
            urlencoding::encode(save_path),
            urlencoding::encode(&normalized_url)
        );

        let response = self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .send()
            .await
            .context("添加离线下载任务请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取添加任务响应失败")?;

        info!("添加离线任务响应: status={}, body={}", status, response_text);

        let api_response: crate::netdisk::cloud_dl::BaiduAddTaskResponse =
            serde_json::from_str(&response_text).context("解析添加任务响应失败")?;

        if !api_response.is_success() {
            let error_code = api_response.get_error_code();
            let error_msg = api_response.get_error_msg();
            error!(
                "添加离线任务失败: error_code={}, error_msg={}",
                error_code, error_msg
            );
            anyhow::bail!("添加离线任务失败: {}", error_msg);
        }

        info!("添加离线任务成功: task_id={}", api_response.task_id);
        Ok(api_response.task_id)
    }

    /// 查询离线下载任务列表
    ///
    /// # 参数
    /// * `start` - 起始位置（默认 0）
    /// * `limit` - 返回数量限制（默认 1000）
    /// * `status` - 状态过滤（255 表示所有状态）
    ///
    /// # 返回
    /// 任务信息列表
    pub async fn cloud_dl_list_task(&self) -> Result<Vec<crate::netdisk::CloudDlTaskInfo>> {
        self.cloud_dl_list_task_with_params(0, 1000, 255).await
    }

    /// 查询离线下载任务列表（带参数）
    ///
    /// # 参数
    /// * `start` - 起始位置
    /// * `limit` - 返回数量限制
    /// * `status` - 状态过滤（255 表示所有状态）
    ///
    /// # 返回
    /// 任务信息列表
    pub async fn cloud_dl_list_task_with_params(
        &self,
        start: u32,
        limit: u32,
        status: u32,
    ) -> Result<Vec<crate::netdisk::CloudDlTaskInfo>> {
        info!(
            "查询离线下载任务列表: start={}, limit={}, status={}",
            start, limit, status
        );

        let url = format!(
            "https://pan.baidu.com/rest/2.0/services/cloud_dl?\
             method=list_task&\
             need_task_info=1&\
             status={}&\
             start={}&\
             limit={}&\
             app_id=250528",
            status, start, limit
        );

        let response = self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .send()
            .await
            .context("查询离线任务列表请求失败")?;

        let status_code = response.status();
        let response_text = response.text().await.context("读取任务列表响应失败")?;

        debug!(
            "查询离线任务列表响应: status={}, body={}",
            status_code, response_text
        );

        let api_response: crate::netdisk::cloud_dl::BaiduListTaskResponse =
            serde_json::from_str(&response_text).map_err(|e| {
                error!(
                    "解析任务列表响应失败: error={}, response_text={}",
                    e, response_text
                );
                anyhow::anyhow!("解析任务列表响应失败: {}", e)
            })?;

        if api_response.errno != 0 {
            error!(
                "查询离线任务列表失败: errno={}, errmsg={}",
                api_response.errno, api_response.errmsg
            );
            anyhow::bail!(
                "查询离线任务列表失败: errno={}, errmsg={}",
                api_response.errno,
                api_response.errmsg
            );
        }

        let tasks: Vec<crate::netdisk::CloudDlTaskInfo> = api_response
            .task_info
            .into_iter()
            .map(|t| t.into_task_info())
            .collect();

        info!("查询到 {} 个离线下载任务", tasks.len());
        Ok(tasks)
    }

    /// 查询指定任务详情
    ///
    /// # 参数
    /// * `task_ids` - 任务 ID 列表
    ///
    /// # 返回
    /// 任务信息列表
    pub async fn cloud_dl_query_task(
        &self,
        task_ids: &[i64],
    ) -> Result<Vec<crate::netdisk::CloudDlTaskInfo>> {
        if task_ids.is_empty() {
            return Ok(vec![]);
        }

        let ids_str = task_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        info!("查询离线任务详情: task_ids={}", ids_str);

        let url = format!(
            "https://pan.baidu.com/rest/2.0/services/cloud_dl?\
             method=query_task&\
             app_id=250528&\
             op_type=1&\
             task_ids={}",
            ids_str
        );

        let response = self
            .client
            .get(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .send()
            .await
            .context("查询离线任务详情请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取任务详情响应失败")?;

        info!(
            "查询离线任务详情响应: status={}, body={}",
            status, response_text
        );

        let api_response: crate::netdisk::cloud_dl::BaiduQueryTaskResponse =
            serde_json::from_str(&response_text).context("解析任务详情响应失败")?;

        if api_response.errno != 0 {
            error!(
                "查询离线任务详情失败: errno={}, errmsg={}",
                api_response.errno, api_response.errmsg
            );
            anyhow::bail!(
                "查询离线任务详情失败: errno={}, errmsg={}",
                api_response.errno,
                api_response.errmsg
            );
        }

        // task_info 是一个对象，key 是 task_id，value 是任务信息
        let mut tasks = Vec::new();
        if let Some(task_map) = api_response.task_info.as_object() {
            for (task_id_str, task_value) in task_map {
                if let Ok(mut baidu_task) =
                    serde_json::from_value::<crate::netdisk::cloud_dl::BaiduTaskInfo>(
                        task_value.clone(),
                    )
                {
                    // 如果 task_id 为空，使用 JSON key 作为 task_id
                    if baidu_task.task_id.is_empty() {
                        baidu_task.task_id = task_id_str.clone();
                    }
                    tasks.push(baidu_task.into_task_info());
                }
            }
        }

        info!("查询到 {} 个任务详情", tasks.len());
        Ok(tasks)
    }

    /// 取消离线下载任务
    ///
    /// # 参数
    /// * `task_id` - 任务 ID
    ///
    /// # 返回
    /// 操作是否成功
    pub async fn cloud_dl_cancel_task(&self, task_id: i64) -> Result<()> {
        info!("取消离线下载任务: task_id={}", task_id);

        let url = format!(
            "https://pan.baidu.com/rest/2.0/services/cloud_dl?\
             method=cancel_task&\
             app_id=250528&\
             task_id={}",
            task_id
        );

        let response = self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .send()
            .await
            .context("取消离线任务请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取取消任务响应失败")?;

        debug!(
            "取消离线任务响应: status={}, body={}",
            status, response_text
        );

        let api_response: crate::netdisk::cloud_dl::BaiduOperationResponse =
            serde_json::from_str(&response_text).context("解析取消任务响应失败")?;

        if !api_response.is_success() {
            let error_code = api_response.get_error_code();
            let error_msg = api_response.get_error_msg();
            error!(
                "取消离线任务失败: error_code={}, error_msg={}",
                error_code, error_msg
            );
            anyhow::bail!("{}", error_msg);
        }

        info!("取消离线任务成功: task_id={}", task_id);
        Ok(())
    }

    /// 删除离线下载任务
    ///
    /// # 参数
    /// * `task_id` - 任务 ID
    ///
    /// # 返回
    /// 操作是否成功
    pub async fn cloud_dl_delete_task(&self, task_id: i64) -> Result<()> {
        info!("删除离线下载任务: task_id={}", task_id);

        let url = format!(
            "https://pan.baidu.com/rest/2.0/services/cloud_dl?\
             method=delete_task&\
             app_id=250528&\
             task_id={}",
            task_id
        );

        let response = self
            .client
            .post(&url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .send()
            .await
            .context("删除离线任务请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取删除任务响应失败")?;

        info!(
            "删除离线任务响应: status={}, body={}",
            status, response_text
        );

        let api_response: crate::netdisk::cloud_dl::BaiduOperationResponse =
            serde_json::from_str(&response_text).context("解析删除任务响应失败")?;

        if !api_response.is_success() {
            let error_code = api_response.get_error_code();
            let error_msg = api_response.get_error_msg();
            error!(
                "删除离线任务失败: error_code={}, error_msg={}",
                error_code, error_msg
            );
            anyhow::bail!("{}", error_msg);
        }

        info!("删除离线任务成功: task_id={}", task_id);
        Ok(())
    }

    /// 清空离线下载任务记录
    ///
    /// # 返回
    /// 清空的任务数量
    pub async fn cloud_dl_clear_task(&self) -> Result<i32> {
        info!("清空离线下载任务记录");

        let url = "https://pan.baidu.com/rest/2.0/services/cloud_dl?method=clear_task&app_id=250528";

        let response = self
            .client
            .post(url)
            .header("Cookie", format!("BDUSS={}", self.bduss()))
            .header("User-Agent", &self.web_user_agent)
            .send()
            .await
            .context("清空离线任务请求失败")?;

        let status = response.status();
        let response_text = response.text().await.context("读取清空任务响应失败")?;

        debug!(
            "清空离线任务响应: status={}, body={}",
            status, response_text
        );

        let api_response: crate::netdisk::cloud_dl::BaiduClearTaskResponse =
            serde_json::from_str(&response_text).context("解析清空任务响应失败")?;

        if api_response.errno != 0 {
            error!(
                "清空离线任务失败: errno={}, errmsg={}",
                api_response.errno, api_response.errmsg
            );
            anyhow::bail!(
                "清空离线任务失败: errno={}, errmsg={}",
                api_response.errno,
                api_response.errmsg
            );
        }

        info!("清空离线任务成功: total={}", api_response.total);
        Ok(api_response.total)
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

    #[test]
    fn test_parse_share_link_s_format() {
        let user_auth = create_test_user_auth();
        let client = NetdiskClient::new(user_auth).unwrap();

        // 测试 /s/{key} 格式
        let result = client.parse_share_link("https://pan.baidu.com/s/1abcDEFg");
        assert!(result.is_ok());
        let share_link = result.unwrap();
        assert_eq!(share_link.short_key, "1abcDEFg");
        assert!(share_link.password.is_none());
    }

    #[test]
    fn test_parse_share_link_with_password() {
        let user_auth = create_test_user_auth();
        let client = NetdiskClient::new(user_auth).unwrap();

        // 测试带密码的链接
        let result = client.parse_share_link("https://pan.baidu.com/s/1abcDEFg?pwd=a1b2");
        assert!(result.is_ok());
        let share_link = result.unwrap();
        assert_eq!(share_link.short_key, "1abcDEFg");
        assert_eq!(share_link.password, Some("a1b2".to_string()));
    }

    #[test]
    fn test_parse_share_link_surl_format() {
        let user_auth = create_test_user_auth();
        let client = NetdiskClient::new(user_auth).unwrap();

        // 测试 /share/init?surl={key} 格式
        let result = client.parse_share_link("https://pan.baidu.com/share/init?surl=abcDEFg");
        assert!(result.is_ok());
        let share_link = result.unwrap();
        // surl 格式需要加 "1" 前缀
        assert_eq!(share_link.short_key, "1abcDEFg");
    }

    #[test]
    fn test_parse_share_link_invalid() {
        let user_auth = create_test_user_auth();
        let client = NetdiskClient::new(user_auth).unwrap();

        // 测试无效链接
        let result = client.parse_share_link("https://google.com/file");
        assert!(result.is_err());
    }
}
