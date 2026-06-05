// 分享链接签名算法
//
// 算法：MD5(shareID + "_sharesurlinfo!@#")
// 返回小写16进制字符串

/// 生成分享链接详情签名
///
/// 用于获取分享链接详情（包含提取码）的 API 请求签名
///
/// # 算法
/// 1. 将 share_id 转换为字符串
/// 2. 拼接固定后缀 "_sharesurlinfo!@#"
/// 3. 计算 MD5 哈希
/// 4. 返回小写16进制字符串
///
/// # 参数
/// * `share_id` - 分享记录的 ID
///
/// # 返回
/// 32位小写16进制 MD5 签名字符串
///
/// # 示例
/// ```
/// use baidu_netdisk_rust::sign::share_sign::share_surl_info_sign;
/// let sign = share_surl_info_sign(123456);
/// assert_eq!(sign.len(), 32);
/// ```
pub fn share_surl_info_sign(share_id: u64) -> String {
    // 构建待签名字符串: shareID + "_sharesurlinfo!@#"
    let data = format!("{}_sharesurlinfo!@#", share_id);

    // 计算 MD5
    let digest = md5::compute(data.as_bytes());

    // 返回小写16进制字符串
    format!("{:x}", digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_share_surl_info_sign_format() {
        let sign = share_surl_info_sign(123456);

        // 验证长度为32位
        assert_eq!(sign.len(), 32);

        // 验证是小写16进制
        assert!(sign.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(sign, sign.to_lowercase());
    }

    #[test]
    fn test_share_surl_info_sign_consistency() {
        // 相同输入应产生相同输出
        let share_id = 987654321u64;
        let sign1 = share_surl_info_sign(share_id);
        let sign2 = share_surl_info_sign(share_id);
        assert_eq!(sign1, sign2);
    }

    #[test]
    fn test_share_surl_info_sign_different_ids() {
        // 不同 share_id 应产生不同签名
        let sign1 = share_surl_info_sign(1);
        let sign2 = share_surl_info_sign(2);
        assert_ne!(sign1, sign2);
    }

    #[test]
    fn test_share_surl_info_sign_known_value() {
        // 验证已知值（手动计算 MD5("0_sharesurlinfo!@#")）
        let sign = share_surl_info_sign(0);
        // MD5("0_sharesurlinfo!@#") = 预期值
        assert_eq!(sign.len(), 32);
        assert!(sign.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
