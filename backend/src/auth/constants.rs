// 百度网盘API常量定义

/// 百度网盘应用ID（网页版）
pub const BAIDU_APP_ID: u32 = 250528;

/// 客户端类型（0=网页版）
pub const CLIENT_TYPE: u32 = 0;

/// User-Agent 标识
pub const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";

/// 百度网盘API基础URL
pub const PAN_API_BASE: &str = "https://pan.baidu.com";

/// 百度登录API基础URL
pub const PASSPORT_API_BASE: &str = "https://passport.baidu.com";

// ==================== API 端点 ====================

/// 获取二维码接口
pub const API_GET_QRCODE: &str = "https://passport.baidu.com/v2/api/getqrcode";

/// 二维码图片接口
pub const API_QRCODE_IMAGE: &str = "https://passport.baidu.com/v2/api/qrcode";

/// 轮询扫码状态接口
pub const API_QRCODE_POLL: &str = "https://passport.baidu.com/channel/unicast";

/// 确认登录接口
pub const API_QRCODE_LOGIN: &str = "https://passport.baidu.com/v3/login/main/qrbdusslogin";

/// 网盘用户信息接口
pub const API_USER_INFO: &str = "https://pan.baidu.com/rest/2.0/membership/user/info";

// ==================== 二维码登录参数 ====================

/// 二维码登录平台标识
pub const QR_LOGIN_PLATFORM: &str = "pc";

/// 二维码来源
pub const QR_LOGIN_FROM: &str = "pc";

/// 应用模板
pub const APP_TEMPLATE: &str = "netdisk";

/// API版本
pub const API_VERSION: &str = "v3";

// ==================== 超时和重试配置 ====================

/// 二维码有效期（秒）
pub const QRCODE_EXPIRE_TIME: i64 = 120;

/// 轮询间隔（毫秒）
pub const POLL_INTERVAL_MS: u64 = 3000;

/// 请求超时时间（秒）
pub const REQUEST_TIMEOUT_SEC: u64 = 30;

// ==================== Cookie 配置 ====================

/// BDUSS Cookie 名称
pub const COOKIE_BDUSS: &str = "BDUSS";

/// STOKEN Cookie 名称
pub const COOKIE_STOKEN: &str = "STOKEN";

/// PTOKEN Cookie 名称
pub const COOKIE_PTOKEN: &str = "PTOKEN";

/// Cookie 域名
pub const COOKIE_DOMAIN_BAIDU: &str = "baidu.com";

/// Cookie 域名（Passport）
pub const COOKIE_DOMAIN_PASSPORT: &str = "passport.baidu.com";
