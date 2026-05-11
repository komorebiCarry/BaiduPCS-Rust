//! 文件解析器
//!
//! 简化为 age 加密文件格式检测。
//! age 文件头以 "age-encryption.org/" 开头。

use std::io::{BufReader, Read};
use std::path::Path;

/// age 文件头标识
const AGE_HEADER_PREFIX: &[u8] = b"age-encryption.org/";

/// 检查文件是否为 age 加密文件
///
/// 通过读取文件前 20 字节并检查 age 头标识。
///
/// # Arguments
/// * `path` - 文件路径
///
/// # Returns
/// * `true` - 该文件是 age 加密文件
/// * `false` - 不是 age 加密文件或读取失败
pub fn is_age_encrypted_file(path: &Path) -> bool {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut reader = BufReader::new(file);
    let mut prefix = [0u8; 20];
    if reader.read_exact(&mut prefix).is_err() {
        return false;
    }
    &prefix[..19] == AGE_HEADER_PREFIX
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_detect_age_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.age");

        // 写入 age 文件头
        std::fs::write(&file_path, b"age-encryption.org/v1\n-> scrypt ...\n--- ...\n").unwrap();

        assert!(is_age_encrypted_file(&file_path));

        // 普通文件
        let normal_path = dir.path().join("normal.txt");
        std::fs::write(&normal_path, b"hello").unwrap();
        assert!(!is_age_encrypted_file(&normal_path));
    }
}
