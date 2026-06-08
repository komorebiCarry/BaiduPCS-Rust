// 认证模块数据类型定义

use serde::{Deserialize, Serialize};
use std::fmt;

// ──────────────────────────────────────────────
// Uid newtype（类型边界）
// ──────────────────────────────────────────────

/// 用户 ID 强类型包装。
///
/// 编译期阻止 `u64` 与 `Uid` 隐式互转（**禁止** `From<u64>` / `Into<u64>`）。
/// 线缆格式透明：JSON `123` ↔ `Uid(123)`。
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Uid(pub u64);

impl Uid {
    /// 从原始 `u64` 构造 `Uid`。
    #[inline]
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// 解包为原始 `u64`（持久化层 / FFI 边界使用）。
    #[inline]
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for Uid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ──────────────────────────────────────────────
// 多账号持久化 DTO（accounts.json 结构）
// ──────────────────────────────────────────────

/// `accounts.json` 顶层结构（持久化层保持 `u64`）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountsData {
    /// 当前活跃账号 UID（`None` = 无活跃账号）
    #[serde(default)]
    pub active_uid: Option<u64>,
    /// 所有已登录用户
    #[serde(default)]
    pub users: Vec<UserAuth>,
}

/// 账号脱敏摘要（前端列表 / 切换器展示用，不含凭证）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSummary {
    pub uid: u64,
    pub username: String,
    pub nickname: Option<String>,
    pub avatar_url: Option<String>,
    pub vip_type: Option<u32>,
    pub is_active: bool,
    /// 持久化的账号自定义配置
    ///
    /// 暴露用途：前端 `BudgetPanel.vue` 在初始化每账号配置表单时优先采用真实持久化
    /// 值，避免用 VIP 推荐默认值重建表单后用户点保存覆盖旧 `accounts.json` 自定义
    /// （`auto_apply_recommended=false` + 自定义 threads/concurrent/retries/skip_hidden_files
    /// 的账号）。
    ///
    /// 字段全部为非凭证、运行时也已通过 `PUT /api/v1/accounts/:uid/custom_config`
    /// 公开可读写，所以脱敏摘要直接透传不引入新泄露面。
    pub custom_config: AccountConfig,
}

impl AccountSummary {
    /// 从 `UserAuth` 生成脱敏摘要。
    pub fn from_user(user: &UserAuth, active_uid: Option<u64>) -> Self {
        Self {
            uid: user.uid,
            username: user.username.clone(),
            nickname: user.nickname.clone(),
            avatar_url: user.avatar_url.clone(),
            vip_type: user.vip_type,
            is_active: active_uid == Some(user.uid),
            custom_config: user.custom_config.clone(),
        }
    }
}

// ──────────────────────────────────────────────
// 每账号自定义配置
// ──────────────────────────────────────────────

/// 单账号下载配置（覆盖全局 `[download]`）
///
/// `accounts.json` wire 层用 `#[serde(default)]` 实现"用户只填部分覆盖"语义，
/// 未填字段走 `Default::default()`。运行态保持完整结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountDownloadConfig {
    #[serde(default = "default_threads_normal")]
    pub max_global_threads: usize,
    #[serde(default = "default_chunk_size_mb")]
    pub chunk_size_mb: u64,
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

/// 单账号上传配置（覆盖全局 `[upload]`）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUploadConfig {
    #[serde(default = "default_threads_normal")]
    pub max_global_threads: usize,
    #[serde(default = "default_chunk_size_mb")]
    pub chunk_size_mb: u64,
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub skip_hidden_files: bool,
}

/// 单账号完整配置（`UserAuth.custom_config`）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    /// 是否自动按 VIP 等级应用 `[multi_account_vip_recommended]` 表的值
    /// 为 `true` 时忽略下面 download/upload 字段的 `max_global_threads`，
    /// 改用 VIP 推荐表值（其它字段仍生效）。
    #[serde(default = "default_auto_apply_recommended")]
    pub auto_apply_recommended: bool,
    #[serde(default)]
    pub download: AccountDownloadConfig,
    #[serde(default)]
    pub upload: AccountUploadConfig,
}

fn default_auto_apply_recommended() -> bool {
    true
}
fn default_threads_normal() -> usize {
    1
}
fn default_chunk_size_mb() -> u64 {
    4
}
fn default_max_concurrent_tasks() -> usize {
    3
}
fn default_max_retries() -> u32 {
    3
}

impl Default for AccountDownloadConfig {
    fn default() -> Self {
        Self {
            max_global_threads: default_threads_normal(),
            chunk_size_mb: default_chunk_size_mb(),
            max_concurrent_tasks: default_max_concurrent_tasks(),
            max_retries: default_max_retries(),
        }
    }
}

impl Default for AccountUploadConfig {
    fn default() -> Self {
        Self {
            max_global_threads: default_threads_normal(),
            chunk_size_mb: default_chunk_size_mb(),
            max_concurrent_tasks: default_max_concurrent_tasks(),
            max_retries: default_max_retries(),
            skip_hidden_files: false,
        }
    }
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            auto_apply_recommended: default_auto_apply_recommended(),
            download: AccountDownloadConfig::default(),
            upload: AccountUploadConfig::default(),
        }
    }
}

// ──────────────────────────────────────────────
// 用户认证信息
// ──────────────────────────────────────────────

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
    /// 上次预热时间戳（用于判断预热数据是否过期）
    #[serde(default)]
    pub last_warmup_at: Option<i64>,
    /// 每账号自定义配置
    ///
    /// 旧 `accounts.json` 无此字段 → 反序列化时填默认值。
    #[serde(default)]
    pub custom_config: AccountConfig,
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
            last_warmup_at: None,
            custom_config: AccountConfig::default(),
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
            last_warmup_at: None,
            custom_config: AccountConfig::default(),
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

/// Cookie 登录 API 请求体
///
/// 前端将从浏览器 DevTools 复制的完整 Cookie 字符串粘贴到此字段。
#[derive(Debug, Deserialize)]
pub struct CookieLoginApiRequest {
    /// 原始 Cookie 字符串，例如: "BDUSS=xxx; PTOKEN=yyy; STOKEN=zzz; ..."
    pub cookies: String,
}

/// 登录响应
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    /// 用户信息
    pub user: UserAuth,
    /// JWT Token
    pub token: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uid_serde_transparent() {
        let uid = Uid::new(123);
        // 序列化为裸 number
        let json = serde_json::to_string(&uid).unwrap();
        assert_eq!(json, "123");
        // 反序列化回 Uid
        let back: Uid = serde_json::from_str(&json).unwrap();
        assert_eq!(back, uid);
        assert_eq!(back.raw(), 123);
    }

    #[test]
    fn uid_hash_eq_ord() {
        use std::collections::HashMap;
        let a = Uid::new(1);
        let b = Uid::new(2);
        let mut map = HashMap::new();
        map.insert(a, "alice");
        map.insert(b, "bob");
        assert_eq!(map[&Uid::new(1)], "alice");
        assert!(a < b);
    }

    #[test]
    fn uid_display() {
        assert_eq!(format!("{}", Uid::new(42)), "42");
    }

    #[test]
    fn accounts_data_serde() {
        // 空 JSON → 默认值
        let empty: AccountsData = serde_json::from_str("{}").unwrap();
        assert!(empty.active_uid.is_none());
        assert!(empty.users.is_empty());

        // 往返
        let data = AccountsData {
            active_uid: Some(100),
            users: vec![UserAuth::new(100, "test".into(), "bduss".into())],
        };
        let json = serde_json::to_string(&data).unwrap();
        let back: AccountsData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.active_uid, Some(100));
        assert_eq!(back.users.len(), 1);
        assert_eq!(back.users[0].uid, 100);
    }

    #[test]
    fn account_summary_from_user() {
        let user = UserAuth::new(42, "alice".into(), "bduss".into());
        let active = AccountSummary::from_user(&user, Some(42));
        assert!(active.is_active);
        let inactive = AccountSummary::from_user(&user, Some(99));
        assert!(!inactive.is_active);
    }
}
