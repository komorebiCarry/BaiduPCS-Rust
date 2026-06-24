//! TOTP 管理模块
//!
//! 提供 TOTP 密钥生成、QR 码生成和验证功能，遵循 RFC 6238 标准。

use totp_rs::{Algorithm, Secret, TOTP};

use super::error::WebAuthError;

/// TOTP 时间步长（秒）
pub const TOTP_STEP: u64 = 30;

/// TOTP 码位数
pub const TOTP_DIGITS: usize = 6;

/// TOTP 时间窗口容差（±1 个周期）
pub const TOTP_SKEW: u8 = 1;

/// 默认发行者名称
pub const DEFAULT_ISSUER: &str = "BaiduPCS-Rust";

/// TOTP 管理器
pub struct TOTPManager;

impl TOTPManager {
    /// 生成新的 TOTP 密钥
    ///
    /// # Returns
    /// * Base32 编码的密钥字符串
    pub fn generate_secret() -> String {
        let secret = Secret::generate_secret();
        secret.to_encoded().to_string()
    }

    /// 创建 TOTP 实例
    fn create_totp(secret: &str, issuer: &str, account: &str) -> Result<TOTP, WebAuthError> {
        let secret_bytes = Secret::Encoded(secret.to_string())
            .to_bytes()
            .map_err(|e| WebAuthError::TotpError(format!("无效的密钥格式: {}", e)))?;

        TOTP::new(
            Algorithm::SHA1,
            TOTP_DIGITS,
            TOTP_SKEW,
            TOTP_STEP,
            secret_bytes,
            Some(issuer.to_string()),
            account.to_string(),
        )
        .map_err(|e| WebAuthError::TotpError(format!("创建 TOTP 失败: {}", e)))
    }

    /// 生成 QR 码（Base64 PNG）
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    /// * `issuer` - 发行者名称（如 "BaiduPCS-Rust"）
    /// * `account` - 账户名称（如 "admin"）
    ///
    /// # Returns
    /// * `Ok(String)` - Data URL 格式的 Base64 PNG 图片 (data:image/png;base64,...)
    /// * `Err(WebAuthError)` - 生成失败
    pub fn generate_qr_code(
        secret: &str,
        issuer: &str,
        account: &str,
    ) -> Result<String, WebAuthError> {
        let totp = Self::create_totp(secret, issuer, account)?;

        let base64 = totp
            .get_qr_base64()
            .map_err(|e| WebAuthError::TotpError(format!("生成 QR 码失败: {}", e)))?;

        // 返回完整的 data URL，以便前端直接在 <img> 标签中使用
        Ok(format!("data:image/png;base64,{}", base64))
    }

    /// 获取 TOTP URI（用于手动输入）
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    /// * `issuer` - 发行者名称
    /// * `account` - 账户名称
    ///
    /// # Returns
    /// * `Ok(String)` - otpauth:// URI
    pub fn get_uri(secret: &str, issuer: &str, account: &str) -> Result<String, WebAuthError> {
        let totp = Self::create_totp(secret, issuer, account)?;
        Ok(totp.get_url())
    }

    /// 验证 TOTP 码
    ///
    /// 支持 ±1 个时间窗口的容差（即当前时间 ±30 秒）。
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    /// * `code` - 用户输入的 6 位验证码
    ///
    /// # Returns
    /// * `Ok(true)` - 验证成功
    /// * `Ok(false)` - 验证失败
    /// * `Err(WebAuthError)` - 验证过程出错
    pub fn verify_code(secret: &str, code: &str) -> Result<bool, WebAuthError> {
        let totp = Self::create_totp(secret, DEFAULT_ISSUER, "user")?;

        // totp-rs 的 check_current 已经内置了 skew 容差
        Ok(totp.check_current(code).unwrap_or(false))
    }

    /// 获取当前 TOTP 码（用于测试）
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    ///
    /// # Returns
    /// * `Ok(String)` - 当前的 6 位验证码
    pub fn get_current_code(secret: &str) -> Result<String, WebAuthError> {
        let totp = Self::create_totp(secret, DEFAULT_ISSUER, "user")?;

        totp.generate_current()
            .map_err(|e| WebAuthError::TotpError(format!("生成验证码失败: {}", e)))
    }

    /// 获取指定时间的 TOTP 码（用于测试）
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    /// * `time` - Unix 时间戳
    ///
    /// # Returns
    /// * `Ok(String)` - 指定时间的 6 位验证码
    pub fn get_code_at(secret: &str, time: u64) -> Result<String, WebAuthError> {
        let totp = Self::create_totp(secret, DEFAULT_ISSUER, "user")?;
        Ok(totp.generate(time))
    }

    /// 验证指定时间的 TOTP 码（用于测试）
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    /// * `code` - 用户输入的验证码
    /// * `time` - Unix 时间戳
    ///
    /// # Returns
    /// * `Ok(true)` - 验证成功
    /// * `Ok(false)` - 验证失败
    pub fn verify_code_at(secret: &str, code: &str, time: u64) -> Result<bool, WebAuthError> {
        let totp = Self::create_totp(secret, DEFAULT_ISSUER, "user")?;
        Ok(totp.check(code, time))
    }

    /// 验证密钥格式是否有效
    ///
    /// # Arguments
    /// * `secret` - Base32 编码的密钥
    ///
    /// # Returns
    /// * `true` - 密钥格式有效
    /// * `false` - 密钥格式无效
    pub fn is_valid_secret(secret: &str) -> bool {
        Secret::Encoded(secret.to_string()).to_bytes().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_secret() {
        let secret = TOTPManager::generate_secret();

        // 验证是有效的 Base32 字符串
        assert!(TOTPManager::is_valid_secret(&secret));

        // 验证长度足够（至少 16 字符）
        assert!(secret.len() >= 16);
    }

    #[test]
    fn test_generate_qr_code() {
        let secret = TOTPManager::generate_secret();
        let qr = TOTPManager::generate_qr_code(&secret, "TestApp", "user@example.com");

        assert!(qr.is_ok());
        let qr_data_url = qr.unwrap();

        // 验证是有效的 data URL 格式
        assert!(qr_data_url.starts_with("data:image/png;base64,"));
        // 验证 Base64 部分不为空
        let base64_part = qr_data_url.strip_prefix("data:image/png;base64,").unwrap();
        assert!(!base64_part.is_empty());
    }

    #[test]
    fn test_get_uri() {
        // Use a longer secret (at least 128 bits = 26 Base32 chars)
        let secret = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP";
        let uri = TOTPManager::get_uri(secret, "TestApp", "user@example.com").unwrap();

        assert!(uri.starts_with("otpauth://totp/"));
        assert!(uri.contains("secret="));
        assert!(uri.contains("issuer=TestApp"));
    }

    #[test]
    fn test_verify_code_current() {
        let secret = TOTPManager::generate_secret();

        // 获取当前码并验证
        let code = TOTPManager::get_current_code(&secret).unwrap();
        assert!(TOTPManager::verify_code(&secret, &code).unwrap());
    }

    #[test]
    fn test_verify_code_wrong() {
        let secret = TOTPManager::generate_secret();

        // 错误的验证码
        assert!(!TOTPManager::verify_code(&secret, "000000").unwrap());
        assert!(!TOTPManager::verify_code(&secret, "123456").unwrap());
    }

    #[test]
    fn test_verify_code_at_time() {
        // Use a longer secret (at least 128 bits)
        let secret = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP";
        let time = 1234567890u64;

        // 获取指定时间的码
        let code = TOTPManager::get_code_at(secret, time).unwrap();

        // 验证同一时间的码
        assert!(TOTPManager::verify_code_at(secret, &code, time).unwrap());

        // 验证时间窗口内的码（±30秒）
        assert!(TOTPManager::verify_code_at(secret, &code, time + 15).unwrap());
        assert!(TOTPManager::verify_code_at(secret, &code, time - 15).unwrap());
    }

    #[test]
    fn test_is_valid_secret() {
        // Valid Base32 secrets
        assert!(TOTPManager::is_valid_secret(
            "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"
        ));
        assert!(TOTPManager::is_valid_secret(&TOTPManager::generate_secret()));

        // 无效的 Base32
        assert!(!TOTPManager::is_valid_secret("invalid!@#$"));
    }

    #[test]
    fn test_different_secrets_different_codes() {
        let secret1 = TOTPManager::generate_secret();
        let secret2 = TOTPManager::generate_secret();

        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let code1 = TOTPManager::get_code_at(&secret1, time).unwrap();
        let code2 = TOTPManager::get_code_at(&secret2, time).unwrap();

        // 不同密钥在同一时间应该产生不同的码（极小概率相同）
        // 这里我们只验证交叉验证失败
        assert!(!TOTPManager::verify_code_at(&secret1, &code2, time).unwrap());
        assert!(!TOTPManager::verify_code_at(&secret2, &code1, time).unwrap());
    }
}
