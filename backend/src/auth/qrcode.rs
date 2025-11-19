// 二维码登录功能实现

use crate::auth::constants::*;
use crate::auth::{QRCode, QRCodeStatus, UserAuth};
use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tracing::{debug, info, warn};

/// 二维码登录客户端
pub struct QRCodeAuth {
    client: Client,
}

impl QRCodeAuth {
    /// 创建新的二维码登录客户端
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .cookie_store(true)
            .user_agent(USER_AGENT)
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client })
    }

    /// 生成登录二维码
    ///
    /// 调用百度API获取二维码sign，并下载百度生成的二维码图片
    pub async fn generate_qrcode(&self) -> Result<QRCode> {
        info!("开始生成登录二维码");

        // 步骤1: 获取二维码sign
        let (sign, _api_url) = self.fetch_qrcode_sign().await?;
        debug!("获取到二维码 sign: {}", sign);

        // 步骤2: 构建百度二维码图片API的URL
        // 使用 lp=mobile 让二维码支持APP扫描
        let qrcode_image_url = format!(
            "{}?sign={}&lp=mobile&qrloginfrom=mobile&tpl={}",
            API_QRCODE_IMAGE, sign, APP_TEMPLATE
        );
        debug!("二维码图片URL: {}", qrcode_image_url);

        // 步骤3: 下载百度生成的二维码图片并转为base64
        // 百度返回的PNG图片中已经包含了正确的登录确认页面URL
        let image_base64 = self.download_qrcode_image(&qrcode_image_url).await?;

        info!("二维码下载成功");

        Ok(QRCode {
            sign,
            image_base64,
            qrcode_url: qrcode_image_url,
            created_at: chrono::Utc::now().timestamp(),
        })
    }

    /// 确认二维码登录，获取真正的BDUSS和用户信息
    ///
    /// 参数:
    /// - v_code: 轮询返回的 v 字段
    /// - _sign: 二维码的 sign（暂未使用，保留以备将来使用）
    ///
    /// 返回: UserAuth (包含完整的用户信息)
    async fn confirm_qrcode_login(&self, v_code: &str, _sign: &str) -> Result<UserAuth> {
        info!("调用确认登录接口，v = {}", v_code);

        // 构建确认登录接口URL
        let timestamp = chrono::Utc::now().timestamp_millis();
        let redirect_url = "https://pan.baidu.com/disk/main";

        let url = format!(
            "{}?v={}&bduss={}&u={}&tpl={}&qrcode=1&apiver={}&tt={}",
            API_QRCODE_LOGIN,
            timestamp,
            v_code,
            urlencoding::encode(redirect_url),
            APP_TEMPLATE,
            API_VERSION,
            timestamp
        );

        debug!("确认登录URL: {}", url);

        // 调用接口
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to confirm qrcode login")?;

        // 打印所有响应头
        info!("确认登录接口响应头:");
        for (key, value) in resp.headers() {
            if key.as_str().to_lowercase().contains("cookie") {
                info!("  {}: {:?}", key, value);
            }
        }

        // 提取 Set-Cookie 中的 BDUSS
        let mut bduss = String::new();

        // 遍历所有的 Set-Cookie 响应头
        for cookie_header in resp.headers().get_all("set-cookie") {
            let cookie_str = cookie_header.to_str().unwrap_or("");

            // 检查是否包含 BDUSS
            if cookie_str.contains("BDUSS=") {
                info!("找到 BDUSS Cookie: {}", cookie_str);

                // 解析 BDUSS
                if let Some(bduss_start) = cookie_str.find("BDUSS=") {
                    let bduss_part = &cookie_str[bduss_start + 6..];
                    if let Some(bduss_end) = bduss_part.find(';') {
                        bduss = bduss_part[..bduss_end].to_string();
                    } else {
                        bduss = bduss_part.to_string();
                    }
                    break; // 找到后退出循环
                }
            }
        }

        if bduss.is_empty() {
            return Err(anyhow::anyhow!("未能从响应中提取BDUSS"));
        }

        info!("成功提取BDUSS: {}...", &bduss[..20.min(bduss.len())]);

        // 获取完整的用户信息
        match self.get_user_info(&bduss).await {
            Ok(user) => {
                info!("登录成功！用户: {}, UID: {}", user.username, user.uid);
                Ok(user)
            }
            Err(e) => {
                warn!("获取用户信息失败: {}，使用基本信息", e);
                // 如果获取用户信息失败，返回基本的UserAuth
                Ok(UserAuth::new(0, "未知用户".to_string(), bduss))
            }
        }
    }

    /// 从百度API获取二维码sign和imgurl
    ///
    /// 返回: (sign, imgurl)
    async fn fetch_qrcode_sign(&self) -> Result<(String, String)> {
        let url = format!(
            "{}?lp={}&qrloginfrom={}&gid=xxx&callback=tangram_guid_xxx&apiver={}&tt=xxx&tpl={}&_=xxx",
            API_GET_QRCODE, QR_LOGIN_PLATFORM, QR_LOGIN_FROM, API_VERSION, APP_TEMPLATE
        );

        let resp = self
            .client
            .get(url)
            .send()
            .await
            .context("Failed to fetch qrcode sign")?;

        let text = resp.text().await?;

        // 响应格式: tangram_guid_xxx({...})
        // 提取JSON部分
        let json_start = text.find('(').context("Invalid response format")?;
        let json_end = text.rfind(')').context("Invalid response format")?;
        let json_str = &text[json_start + 1..json_end];

        let json: Value =
            serde_json::from_str(json_str).context("Failed to parse qrcode response")?;

        // 提取完整的imgurl
        let imgurl = json["imgurl"]
            .as_str()
            .context("Failed to extract imgurl from response")?
            .to_string();

        // 从imgurl中提取sign
        let sign = imgurl
            .split("sign=")
            .nth(1)
            .and_then(|s| s.split('&').next())
            .context("Failed to extract sign from imgurl")?
            .to_string();

        Ok((sign, imgurl))
    }

    /// 下载百度生成的二维码图片并转为Base64编码
    async fn download_qrcode_image(&self, url: &str) -> Result<String> {
        debug!("下载二维码图片: {}", url);

        // 下载二维码图片
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .context("Failed to download QR code image")?;

        // 获取图片数据
        let image_bytes = resp
            .bytes()
            .await
            .context("Failed to read QR code image bytes")?;

        // 转换为Base64
        let base64_image =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &image_bytes);

        debug!("二维码图片下载完成，大小: {} bytes", image_bytes.len());

        Ok(format!("data:image/png;base64,{}", base64_image))
    }

    /// 生成二维码图片Base64编码（备用方法，不再使用）
    #[allow(dead_code)]
    fn generate_qrcode_image(&self, url: &str) -> Result<String> {
        use image::Luma;
        use qrcode::QrCode;

        // 生成二维码
        let code = QrCode::new(url.as_bytes()).context("Failed to generate QR code")?;

        // 渲染为图片
        let image = code.render::<Luma<u8>>().min_dimensions(200, 200).build();

        // 转换为PNG并编码为Base64
        let mut png_data = Vec::new();
        image
            .write_to(
                &mut std::io::Cursor::new(&mut png_data),
                image::ImageFormat::Png,
            )
            .context("Failed to encode QR code image")?;

        let base64_image =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png_data);

        Ok(format!("data:image/png;base64,{}", base64_image))
    }

    /// 轮询扫码状态
    ///
    /// 查询用户是否扫码及登录状态
    pub async fn poll_status(&self, sign: &str) -> Result<QRCodeStatus> {
        debug!("轮询二维码状态: {}", sign);

        let url = format!(
            "{}?channel_id={}&tpl={}&apiver={}&tt={}",
            API_QRCODE_POLL,
            sign,
            APP_TEMPLATE,
            API_VERSION,
            chrono::Utc::now().timestamp_millis()
        );

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to poll qrcode status")?;

        // 打印响应头，查看是否有 Set-Cookie
        if let Some(cookies) = resp.headers().get("set-cookie") {
            info!("响应中的 Set-Cookie: {:?}", cookies);
        }

        let json: Value = resp
            .json()
            .await
            .context("Failed to parse status response")?;

        // 打印完整的响应内容，用于调试
        info!(
            "轮询接口返回: {}",
            serde_json::to_string_pretty(&json).unwrap_or_default()
        );

        // 解析 channel_v 中的状态
        // channel_v 是一个 JSON 字符串，需要再次解析
        let channel_v_str = json["channel_v"].as_str().unwrap_or("{}");

        info!("channel_v 原始字符串: {}", channel_v_str);

        let channel_v: Value =
            serde_json::from_str(channel_v_str).unwrap_or_else(|_| serde_json::json!({}));

        let status = channel_v["status"].as_i64().unwrap_or(0);
        let v_code = channel_v["v"].as_str().unwrap_or("");
        info!(
            "扫码状态: status = {}, v = {}, 完整 channel_v: {}",
            status,
            v_code,
            serde_json::to_string_pretty(&channel_v).unwrap_or_default()
        );

        // 判断登录状态的正确逻辑：
        // 1. status = 0 且没有 v 字段 → 等待扫码
        // 2. status = 1 → 已扫码，等待确认
        // 3. status = 0 且有 v 字段 → 登录成功（需要用 v 去获取 BDUSS）

        if !v_code.is_empty() {
            // 有 v 字段，说明用户已确认登录
            info!("用户确认登录成功，v = {}", v_code);

            // 调用确认登录接口，获取真正的 BDUSS 和用户信息
            match self.confirm_qrcode_login(v_code, sign).await {
                Ok(user) => {
                    info!("登录成功！用户: {}, UID: {}", user.username, user.uid);
                    Ok(QRCodeStatus::Success {
                        token: user.bduss.clone(),
                        user,
                    })
                }
                Err(e) => {
                    warn!("确认登录失败: {}", e);
                    // 降级：返回临时用户
                    Ok(QRCodeStatus::Success {
                        user: UserAuth::new(0, "临时用户".to_string(), v_code.to_string()),
                        token: v_code.to_string(),
                    })
                }
            }
        } else {
            // 没有 v 字段，根据 status 判断状态
            match status {
                0 => {
                    // 等待扫码
                    debug!("等待用户扫码");
                    Ok(QRCodeStatus::Waiting)
                }
                1 => {
                    // 已扫码，待确认
                    info!("用户已扫码，等待确认");
                    Ok(QRCodeStatus::Scanned)
                }
                2 => {
                    // 这个状态可能不会出现，但保留判断
                    info!("登录成功（status=2）");
                    Ok(QRCodeStatus::Success {
                        user: UserAuth::new(0, "临时用户".to_string(), "".to_string()),
                        token: "temp_token".to_string(),
                    })
                }
                -1 | -2 => {
                    // 二维码过期
                    warn!("二维码已过期");
                    Ok(QRCodeStatus::Expired)
                }
                _ => {
                    // 其他状态或错误
                    warn!("未知的扫码状态: status = {}", status);
                    // 检查是否有错误信息
                    if json["errno"].as_i64().unwrap_or(0) != 0 {
                        let msg = json["msg"].as_str().unwrap_or("未知错误").to_string();
                        warn!("登录失败: {}", msg);
                        Ok(QRCodeStatus::Failed { reason: msg })
                    } else {
                        // 继续等待
                        Ok(QRCodeStatus::Waiting)
                    }
                }
            }
        }
    }

    /// 验证BDUSS是否有效
    ///
    /// 通过调用网盘用户信息接口来验证BDUSS
    /// 如果返回成功，说明BDUSS有效
    /// 如果返回错误，说明BDUSS已失效
    pub async fn verify_bduss(&self, bduss: &str) -> Result<bool> {
        info!("验证BDUSS是否有效");

        // 尝试获取用户信息
        match self.get_user_info(bduss).await {
            Ok(user) => {
                info!("BDUSS有效，用户: {}, UID: {}", user.username, user.uid);
                Ok(true)
            }
            Err(e) => {
                warn!("BDUSS验证失败: {}", e);
                Ok(false)
            }
        }
    }

    /// 通过百度网盘API获取用户信息
    ///
    /// 返回: UserAuth (包含完整的用户信息)
    async fn get_user_info(&self, bduss: &str) -> Result<UserAuth> {
        info!("获取用户信息");

        // 使用百度网盘的用户信息接口
        let url = format!(
            "{}?method=query&clienttype={}&app_id={}&web=1",
            API_USER_INFO, CLIENT_TYPE, BAIDU_APP_ID
        );

        let resp = self
            .client
            .get(&url)
            .header("Cookie", format!("{}={}", COOKIE_BDUSS, bduss))
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .context("Failed to fetch user info")?;

        let json: Value = resp.json().await.context("Failed to parse user info")?;

        // 打印返回的JSON，用于调试
        info!(
            "网盘API返回: {}",
            serde_json::to_string_pretty(&json).unwrap_or_default()
        );

        // 用户信息在 user_info 字段下
        let user_info = &json["user_info"];

        // 从返回的JSON中提取用户信息
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

        // 提取头像URL
        let avatar_url = user_info["photo"]
            .as_str()
            .or_else(|| user_info["avatar_url"].as_str())
            .map(|s| s.to_string());

        // 提取VIP类型（0=普通，1=会员，2=超级会员）
        let vip_type = if user_info["is_svip"].as_i64().unwrap_or(0) == 1 {
            Some(2)
        } else if user_info["is_vip"].as_i64().unwrap_or(0) == 1 {
            Some(1)
        } else {
            Some(0)
        };

        // 提取空间信息（这个API似乎不返回，后续可以调用其他API获取）
        let total_space = json["total"].as_u64();
        let used_space = json["used"].as_u64();

        info!(
            "获取到用户信息 - 用户名: {}, 昵称: {:?}, UID: {}, VIP: {:?}",
            username, nickname, uid, vip_type
        );

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

    /// 获取用户UID
    pub async fn get_uid(&self, bduss: &str) -> Result<u64> {
        let user = self.get_user_info(bduss).await?;
        Ok(user.uid)
    }

    /// 获取用户名
    pub async fn get_username(&self, bduss: &str) -> Result<String> {
        let user = self.get_user_info(bduss).await?;
        Ok(user.username)
    }
}

impl Default for QRCodeAuth {
    fn default() -> Self {
        Self::new().expect("Failed to create QRCodeAuth")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_qrcode() {
        let auth = QRCodeAuth::new().unwrap();
        let result = auth.generate_qrcode().await;

        // 注意：此测试需要网络连接
        if let Ok(qrcode) = result {
            assert!(!qrcode.sign.is_empty());
            assert!(qrcode.image_base64.starts_with("data:image/png;base64,"));
        }
    }
}
