// Locate 下载签名算法
//
// 用于百度网盘 Locate 模式下载的签名生成

use crate::sign::generate_devuid;
use sha1::{Digest, Sha1};

/// Locate 下载签名
///
/// 用于生成百度网盘 Locate 模式下载请求所需的签名参数
#[derive(Debug, Clone)]
pub struct LocateSign {
    /// 时间戳（秒）
    pub time: i64,
    /// 随机签名字符串
    pub rand: String,
    /// 设备UID
    pub devuid: String,
}

impl LocateSign {
    /// 创建新的 Locate 签名
    ///
    /// # 参数
    /// * `uid` - 用户ID
    /// * `bduss` - 用户的 BDUSS 凭证
    ///
    /// # 返回
    /// 包含完整签名信息的 LocateSign 实例
    pub fn new(uid: u64, bduss: &str) -> Self {
        let time = chrono::Utc::now().timestamp();
        let devuid = generate_devuid(bduss);

        let mut sign = Self {
            time,
            rand: String::new(),
            devuid,
        };

        sign.generate_rand(uid, bduss);
        sign
    }

    /// 使用指定时间戳和 DevUID 创建签名
    ///
    /// 主要用于测试或特殊场景
    pub fn with_time_and_devuid(time: i64, devuid: String, uid: u64, bduss: &str) -> Self {
        let mut sign = Self {
            time,
            rand: String::new(),
            devuid,
        };

        sign.generate_rand(uid, bduss);
        sign
    }

    /// 生成随机签名字符串
    ///
    /// # 算法步骤
    /// 1. SHA1(BDUSS) -> bduss_sha1
    /// 2. SHA1(bduss_sha1 + uid + magic_string + time + devuid) -> rand
    ///
    /// magic_string 是百度固定的魔术字符串：
    /// "ebrcUYiuxaZv2XGu7KIYKxUrqfnOfpDF"
    fn generate_rand(&mut self, uid: u64, bduss: &str) {
        // 步骤1: SHA1(BDUSS)
        let mut bduss_hasher = Sha1::new();
        bduss_hasher.update(bduss.as_bytes());
        let bduss_hash = bduss_hasher.finalize();
        let bduss_hex = format!("{:x}", bduss_hash);

        // 步骤2: 组合签名
        let mut rand_hasher = Sha1::new();
        rand_hasher.update(bduss_hex.as_bytes());
        rand_hasher.update(uid.to_string().as_bytes());

        // 魔术字符串（来自 BaiduPCS-Go）
        rand_hasher.update(b"ebrcUYiuxaZv2XGu7KIYKxUrqfnOfpDF");

        rand_hasher.update(self.time.to_string().as_bytes());
        rand_hasher.update(self.devuid.as_bytes());

        let rand_hash = rand_hasher.finalize();
        self.rand = format!("{:x}", rand_hash);
    }

    /// 生成 URL 查询参数字符串
    ///
    /// # 返回
    /// 格式化的查询参数，如：
    /// "time=1699999999&rand=abc123...&devuid=XXX|0&cuid=XXX|0"
    pub fn url_params(&self) -> String {
        format!(
            "time={}&rand={}&devuid={}&cuid={}",
            self.time, self.rand, self.devuid, self.devuid
        )
    }

    /// 生成完整的 URL（附加签名参数）
    ///
    /// # 参数
    /// * `base_url` - 基础 URL
    ///
    /// # 返回
    /// 附加了签名参数的完整 URL
    pub fn sign_url(&self, base_url: &str) -> String {
        let separator = if base_url.contains('?') { "&" } else { "?" };
        format!("{}{}{}", base_url, separator, self.url_params())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_locate_sign_creation() {
        let uid = 123456789u64;
        let bduss = "test_bduss";

        let sign = LocateSign::new(uid, bduss);

        // 验证字段不为空
        assert!(!sign.rand.is_empty());
        assert!(!sign.devuid.is_empty());
        assert!(sign.time > 0);

        // 验证 DevUID 格式
        assert!(sign.devuid.ends_with("|0"));

        // 验证 rand 是40字符的16进制字符串（SHA1）
        assert_eq!(sign.rand.len(), 40);
    }

    #[test]
    fn test_url_params() {
        let sign = LocateSign::with_time_and_devuid(
            1699999999,
            "ABC123|0".to_string(),
            123456789,
            "test_bduss",
        );

        let params = sign.url_params();

        assert!(params.contains("time=1699999999"));
        assert!(params.contains("devuid=ABC123|0"));
        assert!(params.contains("cuid=ABC123|0"));
        assert!(params.contains("rand="));
    }

    #[test]
    fn test_sign_url() {
        let sign = LocateSign::with_time_and_devuid(
            1699999999,
            "ABC123|0".to_string(),
            123456789,
            "test_bduss",
        );

        // 测试不带查询参数的URL
        let url1 = sign.sign_url("https://example.com/file");
        assert!(url1.starts_with("https://example.com/file?"));

        // 测试已有查询参数的URL
        let url2 = sign.sign_url("https://example.com/file?foo=bar");
        assert!(url2.contains("&time="));
    }

    #[test]
    fn test_sign_consistency() {
        // 相同输入应产生相同输出
        let uid = 123456789u64;
        let bduss = "test_bduss";
        let time = 1699999999;
        let devuid = generate_devuid(bduss);

        let sign1 = LocateSign::with_time_and_devuid(time, devuid.clone(), uid, bduss);
        let sign2 = LocateSign::with_time_and_devuid(time, devuid, uid, bduss);

        assert_eq!(sign1.rand, sign2.rand);
    }
}
