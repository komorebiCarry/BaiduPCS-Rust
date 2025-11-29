// 认证模块数据类型定义

use serde::{Deserialize, Serialize};

/// 用户认证信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAuth {
    /// 用户ID
    pub uid: u64,
    /// 用户名
    pub username: String,
    /// 昵称（显示名称）
    pub nickname: Option<String>,
    /// 头像URL
    pub avatar_url: Option<String>,
    /// VIP类型（0=普通用户，1=普通会员，2=超级会员）
    pub vip_type: Option<u32>,
    /// 网盘容量（字节）
    pub total_space: Option<u64>,
    /// 已使用空间（字节）
    pub used_space: Option<u64>,
    /// BDUSS凭证
    pub bduss: String,
    /// STOKEN凭证  
    pub stoken: Option<String>,
    /// PTOKEN凭证
    pub ptoken: Option<String>,
    /// BAIDUID (首次访问百度时生成,必须保存)
    pub baiduid: Option<String>,
    /// PASSID (登录会话相关)
    pub passid: Option<String>,
    /// 完整Cookie字符串
    pub cookies: Option<String>,
    /// PANPSC (预热后获取的会话令牌)
    pub panpsc: Option<String>,
    /// csrfToken (预热后获取的 CSRF 令牌)
    pub csrf_token: Option<String>,
    /// bdstoken (预热后获取的 bdstoken)
    pub bdstoken: Option<String>,
    /// 登录时间戳
    pub login_time: i64,
}

impl UserAuth {
    /// 创建新的用户认证信息
    pub fn new(uid: u64, username: String, bduss: String) -> Self {
        Self {
            uid,
            username,
            nickname: None,
            avatar_url: None,
            vip_type: None,
            total_space: None,
            used_space: None,
            bduss,
            stoken: None,
            ptoken: None,
            baiduid: None,
            passid: None,
            cookies: None,
            panpsc: None,
            csrf_token: None,
            bdstoken: None,
            login_time: chrono::Utc::now().timestamp(),
        }
    }

    /// 创建包含完整信息的用户认证
    pub fn new_with_details(
        uid: u64,
        username: String,
        bduss: String,
        nickname: Option<String>,
        avatar_url: Option<String>,
        vip_type: Option<u32>,
        total_space: Option<u64>,
        used_space: Option<u64>,
    ) -> Self {
        Self {
            uid,
            username,
            nickname,
            avatar_url,
            vip_type,
            total_space,
            used_space,
            bduss,
            stoken: None,
            ptoken: None,
            baiduid: None,
            passid: None,
            cookies: None,
            panpsc: None,
            csrf_token: None,
            bdstoken: None,
            login_time: chrono::Utc::now().timestamp(),
        }
    }

    /// 检查会话是否过期（默认30天）
    pub fn is_expired(&self, timeout_days: i64) -> bool {
        let current_time = chrono::Utc::now().timestamp();
        let elapsed = current_time - self.login_time;
        elapsed > timeout_days * 24 * 3600
    }
}

/// 二维码信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QRCode {
    /// 二维码唯一标识
    pub sign: String,
    /// 二维码图片Base64编码
    pub image_base64: String,
    /// 二维码URL
    pub qrcode_url: String,
    /// 生成时间戳
    pub created_at: i64,
}

/// 扫码状态枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum QRCodeStatus {
    /// 等待扫码
    Waiting,
    /// 已扫码，等待确认
    Scanned,
    /// 登录成功
    Success { user: UserAuth, token: String },
    /// 二维码已过期
    Expired,
    /// 登录失败
    Failed { reason: String },
}

/// 登录请求
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// 通过BDUSS直接登录
    pub bduss: Option<String>,
    /// STOKEN（可选）
    pub stoken: Option<String>,
    /// PTOKEN（可选）
    pub ptoken: Option<String>,
}

/// 登录响应
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    /// 用户信息
    pub user: UserAuth,
    /// JWT Token
    pub token: String,
}
