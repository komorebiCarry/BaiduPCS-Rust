//! 恢复码管理模块
//!
//! 提供恢复码的生成、哈希和验证功能。

use rand::Rng;

use super::types::RecoveryCode;

/// 恢复码数量
pub const RECOVERY_CODE_COUNT: usize = 8;

/// 恢复码每段长度
const CODE_SEGMENT_LENGTH: usize = 4;

/// 恢复码管理器
pub struct RecoveryCodeManager;

impl RecoveryCodeManager {
    /// 生成指定数量的恢复码
    ///
    /// # Returns
    /// * 8 个 XXXX-XXXX 格式的恢复码
    pub fn generate_codes() -> Vec<String> {
        let mut rng = rand::thread_rng();
        (0..RECOVERY_CODE_COUNT)
            .map(|_| Self::generate_single_code(&mut rng))
            .collect()
    }

    /// 生成单个恢复码
    fn generate_single_code<R: Rng>(rng: &mut R) -> String {
        let chars: Vec<char> = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".chars().collect();

        let part1: String = (0..CODE_SEGMENT_LENGTH)
            .map(|_| chars[rng.gen_range(0..chars.len())])
            .collect();

        let part2: String = (0..CODE_SEGMENT_LENGTH)
            .map(|_| chars[rng.gen_range(0..chars.len())])
            .collect();

        format!("{}-{}", part1, part2)
    }

    /// 哈希恢复码用于存储
    ///
    /// 使用 SHA-1 哈希（对于恢复码足够安全）
    ///
    /// # Arguments
    /// * `code` - 原始恢复码
    ///
    /// # Returns
    /// * SHA-1 哈希的十六进制字符串
    pub fn hash_code(code: &str) -> String {
        use sha1::{Digest, Sha1};

        // 标准化：移除连字符，转大写
        let normalized = code.replace('-', "").to_uppercase();

        let mut hasher = Sha1::new();
        hasher.update(normalized.as_bytes());
        let result = hasher.finalize();

        hex::encode(result)
    }

    /// 验证恢复码
    ///
    /// # Arguments
    /// * `code` - 用户输入的恢复码
    /// * `stored_codes` - 存储的恢复码列表
    ///
    /// # Returns
    /// * `Some(index)` - 匹配的恢复码索引
    /// * `None` - 未找到匹配或已使用
    pub fn verify_code(code: &str, stored_codes: &[RecoveryCode]) -> Option<usize> {
        let input_hash = Self::hash_code(code);

        stored_codes
            .iter()
            .position(|stored| !stored.used && stored.code_hash == input_hash)
    }

    /// 格式化恢复码用于显示
    ///
    /// 确保恢复码是 XXXX-XXXX 格式
    ///
    /// # Arguments
    /// * `codes` - 原始恢复码列表
    ///
    /// # Returns
    /// * 格式化后的恢复码列表
    pub fn format_for_display(codes: &[String]) -> Vec<String> {
        codes
            .iter()
            .map(|code| {
                let clean = code.replace('-', "").to_uppercase();
                if clean.len() == 8 {
                    format!("{}-{}", &clean[0..4], &clean[4..8])
                } else {
                    code.clone()
                }
            })
            .collect()
    }

    /// 将恢复码列表转换为存储格式
    ///
    /// # Arguments
    /// * `codes` - 原始恢复码列表
    ///
    /// # Returns
    /// * 哈希后的 RecoveryCode 列表
    pub fn codes_to_storage(codes: &[String]) -> Vec<RecoveryCode> {
        codes
            .iter()
            .map(|code| RecoveryCode::new(Self::hash_code(code)))
            .collect()
    }

    /// 验证恢复码格式是否有效
    ///
    /// # Arguments
    /// * `code` - 待验证的恢复码
    ///
    /// # Returns
    /// * `true` - 格式有效
    /// * `false` - 格式无效
    pub fn is_valid_format(code: &str) -> bool {
        let clean = code.replace('-', "").to_uppercase();

        // 必须是 8 个字符
        if clean.len() != 8 {
            return false;
        }

        // 所有字符必须是有效字符（排除了 0, 1, I, O 以避免混淆）
        let valid_chars: Vec<char> = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".chars().collect();
        clean.chars().all(|c| valid_chars.contains(&c))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_codes() {
        let codes = RecoveryCodeManager::generate_codes();

        // 应该生成 8 个码
        assert_eq!(codes.len(), RECOVERY_CODE_COUNT);

        // 每个码应该是 XXXX-XXXX 格式
        for code in &codes {
            assert_eq!(code.len(), 9); // 4 + 1 + 4
            assert!(code.contains('-'));
            assert!(RecoveryCodeManager::is_valid_format(code));
        }
    }

    #[test]
    fn test_generate_codes_unique() {
        let codes = RecoveryCodeManager::generate_codes();

        // 所有码应该唯一
        let mut unique_codes = codes.clone();
        unique_codes.sort();
        unique_codes.dedup();
        assert_eq!(unique_codes.len(), codes.len());
    }

    #[test]
    fn test_hash_code() {
        let code = "ABCD-EFGH";
        let hash = RecoveryCodeManager::hash_code(code);

        // SHA-1 哈希应该是 40 字符的十六进制字符串
        assert_eq!(hash.len(), 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_hash_code_normalized() {
        // 不同格式的相同码应该产生相同哈希
        let hash1 = RecoveryCodeManager::hash_code("ABCD-EFGH");
        let hash2 = RecoveryCodeManager::hash_code("abcd-efgh");
        let hash3 = RecoveryCodeManager::hash_code("ABCDEFGH");
        let hash4 = RecoveryCodeManager::hash_code("abcdefgh");

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
        assert_eq!(hash3, hash4);
    }

    #[test]
    fn test_verify_code() {
        let codes = RecoveryCodeManager::generate_codes();
        let stored = RecoveryCodeManager::codes_to_storage(&codes);

        // 验证第一个码
        let result = RecoveryCodeManager::verify_code(&codes[0], &stored);
        assert_eq!(result, Some(0));

        // 验证最后一个码
        let result = RecoveryCodeManager::verify_code(&codes[7], &stored);
        assert_eq!(result, Some(7));
    }

    #[test]
    fn test_verify_code_case_insensitive() {
        let codes = vec!["ABCD-EFGH".to_string()];
        let stored = RecoveryCodeManager::codes_to_storage(&codes);

        // 小写也应该能验证
        let result = RecoveryCodeManager::verify_code("abcd-efgh", &stored);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_verify_code_used() {
        let codes = vec!["ABCD-EFGH".to_string()];
        let mut stored = RecoveryCodeManager::codes_to_storage(&codes);

        // 标记为已使用
        stored[0].mark_used();

        // 已使用的码不应该验证成功
        let result = RecoveryCodeManager::verify_code("ABCD-EFGH", &stored);
        assert_eq!(result, None);
    }

    #[test]
    fn test_verify_code_invalid() {
        let codes = vec!["ABCD-EFGH".to_string()];
        let stored = RecoveryCodeManager::codes_to_storage(&codes);

        // 错误的码不应该验证成功
        let result = RecoveryCodeManager::verify_code("XXXX-YYYY", &stored);
        assert_eq!(result, None);
    }

    #[test]
    fn test_format_for_display() {
        let codes = vec![
            "ABCDEFGH".to_string(),
            "abcd-efgh".to_string(),
            "WXYZ-1234".to_string(),
        ];

        let formatted = RecoveryCodeManager::format_for_display(&codes);

        assert_eq!(formatted[0], "ABCD-EFGH");
        assert_eq!(formatted[1], "ABCD-EFGH");
        assert_eq!(formatted[2], "WXYZ-1234");
    }

    #[test]
    fn test_is_valid_format() {
        assert!(RecoveryCodeManager::is_valid_format("ABCD-EFGH"));
        assert!(RecoveryCodeManager::is_valid_format("abcd-efgh"));
        assert!(RecoveryCodeManager::is_valid_format("ABCDEFGH"));
        assert!(RecoveryCodeManager::is_valid_format("2345-6789"));
        // "ABC-DEFGH" is valid because after removing hyphen it's "ABCDEFGH" (8 chars)
        assert!(RecoveryCodeManager::is_valid_format("ABC-DEFGH"));

        // 无效格式
        assert!(!RecoveryCodeManager::is_valid_format("ABCD-EFG")); // 长度错误 (7 chars)
        assert!(!RecoveryCodeManager::is_valid_format("ABCDEFG")); // 长度错误 (7 chars)
        assert!(!RecoveryCodeManager::is_valid_format("ABCDEFGHI")); // 长度错误 (9 chars)
        assert!(!RecoveryCodeManager::is_valid_format("ABCD-EFG0")); // 包含 0
        assert!(!RecoveryCodeManager::is_valid_format("ABCD-EFG1")); // 包含 1
        assert!(!RecoveryCodeManager::is_valid_format("ABCD-EFGI")); // 包含 I
        assert!(!RecoveryCodeManager::is_valid_format("ABCD-EFGO")); // 包含 O
    }
}
