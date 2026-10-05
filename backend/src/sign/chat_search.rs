// 群聊文件搜索签名算法
//
// 算法：base64( 小写hex( MD5( SALT + "_" + uk + key_word ) ) )
//
// 说明：
// - `SALT` 是官方客户端 `browserengine.dll` 内的明文常量，随客户端版本可能变化。
// - `uk` 是登录用户的数字 ID。
// - 拼接细节：`"_"` 只出现一次、紧跟在 `SALT` 之后；`uk` 与 `key_word` 之间没有分隔符。

use base64::Engine;

/// 「群聊文件搜索」签名使用的固定盐值。
///
/// 提取自官方客户端 `module\BrowserEngine\browserengine.dll` 的明文常量。
/// 百度若更换客户端版本，此值可能变化。
pub const CHAT_SEARCH_SALT: &str = "D3BA5E6D3B16D9202E10DE5D662CFC15";

/// 生成「群聊文件搜索」接口 `/basembox/group/multisearch` 的 `sign` 参数。
///
/// # 算法
/// 1. 拼接 `SALT + "_" + uk + key_word`（注意 `uk` 与关键词之间无分隔符）
/// 2. 计算 MD5，取小写 16 进制字符串
/// 3. 对该 16 进制字符串做标准 Base64 编码
///
/// # 参数
/// * `uk` - 登录用户的数字 ID
/// * `key_word` - 搜索关键词
///
/// # 返回
/// 可直接作为 query 参数 `sign` 使用的字符串。
///
/// # 示例
/// ```
/// use baidu_netdisk_rust::sign::chat_search::chat_search_sign;
/// let sign = chat_search_sign(123456, "test");
/// assert_eq!(sign, "MDhmMThkNjVlNGExNmMzMzQ0MzJiYTZlNGNhMGJiNDE=");
/// ```
pub fn chat_search_sign(uk: u64, key_word: &str) -> String {
    // 1. 拼接待签名串：SALT + "_" + uk + key_word
    let raw = format!("{}_{}{}", CHAT_SEARCH_SALT, uk, key_word);

    // 2. 计算 MD5 并转为小写 16 进制字符串
    let digest = md5::compute(raw.as_bytes());
    let hex = format!("{:x}", digest);

    // 3. 对 16 进制字符串做标准 Base64 编码
    base64::engine::general_purpose::STANDARD.encode(hex.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn test_chat_search_sign_known_value() {
        // 合成输入（uk=123456, key_word="test"）的已知输出，
        // 用于锁定算法不被无意改动。
        assert_eq!(
            chat_search_sign(123456, "test"),
            "MDhmMThkNjVlNGExNmMzMzQ0MzJiYTZlNGNhMGJiNDE="
        );
    }

    #[test]
    fn test_chat_search_sign_empty_keyword() {
        // uk=0、空关键词：拼接结果为 "SALT_0"
        assert_eq!(
            chat_search_sign(0, ""),
            "ZGNiZTg5OWE5ZDU0Mzg3MTdjZDA0Y2IyMjBlMTQ2OGU="
        );
    }

    #[test]
    fn test_chat_search_sign_decodes_to_hex_md5() {
        let sign = chat_search_sign(99887766, "hello");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(sign.as_bytes())
            .expect("sign 应为合法 Base64");
        let hex = String::from_utf8(decoded).expect("解码后应为 ASCII");

        // 解码结果应是 32 位小写 16 进制
        assert_eq!(hex.len(), 32);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(hex, hex.to_lowercase());
    }

    #[test]
    fn test_chat_search_sign_consistency() {
        let uk = 42424242u64;
        assert_eq!(chat_search_sign(uk, "abc"), chat_search_sign(uk, "abc"));
    }

    #[test]
    fn test_chat_search_sign_differs_on_input() {
        // 关键词不同
        assert_ne!(chat_search_sign(1, "a"), chat_search_sign(1, "b"));
        // uk 不同
        assert_ne!(chat_search_sign(1, "a"), chat_search_sign(2, "a"));
    }
}
