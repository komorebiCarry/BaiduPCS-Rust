//! 加密服务
//!
//! 使用 age 加密格式 (age-encryption.org/v1) 替代自定义加密格式。
//! 生成的 .age 文件可通过标准 `age -d` CLI 工具解密，无需依赖本项目工具。
//!
//! # 解密方式
//!
//! ```bash
//! # 方法 1: 使用本项目的 decrypt-cli（自动从 encryption.json 读取密钥）
//! decrypt-cli decrypt --key-file encryption.json --in file.age --out file.txt
//!
//! # 方法 2: 使用标准 age CLI（手动输入密钥作为口令）
//! age -d file.age
//! # 输入口令：encryption.json 中的 master_key 字段值（Base64 编码的 32 字节密钥）
//! ```

use age::secrecy::Secret;
use anyhow::{anyhow, Result};
use std::io::{BufReader, Read, Write};
use std::path::Path;

use crate::autobackup::config::EncryptionAlgorithm;

/// 加密文件扩展名（使用 .age 后缀，符合 age-encryption.org/v1 规范）
pub const ENCRYPTED_FILE_EXTENSION: &str = ".age";

/// age 文件头标识（用于快速识别）
const AGE_HEADER_PREFIX: &[u8] = b"age-encryption.org/";

// ============================================================================
// EncryptionService — 同步版
// ============================================================================

/// 加密服务
///
/// 使用 age 加密格式，口令模式 (age-encryption.org/v1)。
/// age 内部使用 scrypt 做内存硬化派生，安全性等同于 Argon2id。
/// 加密数据格式符合 age-encryption.org/v1，可用标准 `age -d` CLI 解密。
///
/// # 解密
/// ```bash
/// age -d file.age
/// # 输入存储在此服务中的口令
/// ```
#[derive(Clone, Debug)]
pub struct EncryptionService {
    /// age 口令（任意长度字符串，age 内部用 scrypt 硬化）
    passphrase: String,
}

impl EncryptionService {
    /// 创建加密服务（直接传入口令）
    pub fn new(passphrase: impl Into<String>, _algorithm: EncryptionAlgorithm) -> Self {
        Self { passphrase: passphrase.into() }
    }

    /// 从字符串创建（兼容旧接口，接受任意口令或 base64 密钥）
    pub fn from_string(passphrase: &str) -> Self {
        Self { passphrase: passphrase.to_string() }
    }

    /// 保持兼容：from_base64_key 现在接受任意口令
    pub fn from_base64_key(passphrase: &str, _algorithm: EncryptionAlgorithm) -> Result<Self> {
        Ok(Self { passphrase: passphrase.to_string() })
    }

    /// 生成随机口令（60 位十六进制，约 240 位熵）
    pub fn generate_random_passphrase() -> String {
        use rand::Rng;
        (0..60).map(|_| format!("{:x}", rand::thread_rng().gen_range(0..16))).collect()
    }

    /// 生成新的主密钥 base64（兼容旧接口，现在就是生成随机口令）
    pub fn generate_master_key_base64() -> String {
        Self::generate_random_passphrase()
    }

    /// 获取口令（兼容旧接口名 get_key_base64）
    pub fn get_key_base64(&self) -> String {
        self.passphrase.clone()
    }

    fn get_passphrase(&self) -> Secret<String> {
        Secret::new(self.passphrase.clone())
    }

    // ========================================================================
    // 内存加密/解密
    // ========================================================================

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData> {
        let passphrase = self.get_passphrase();
        let encryptor = age::Encryptor::with_user_passphrase(passphrase);

        let mut ciphertext = Vec::with_capacity(plaintext.len() + 1024);
        let mut writer = encryptor
            .wrap_output(&mut ciphertext)
            .map_err(|e| anyhow!("无法创建 age 加密输出: {}", e))?;

        writer.write_all(plaintext)
            .map_err(|e| anyhow!("age 加密写入失败: {}", e))?;
        writer.finish()
            .map_err(|e| anyhow!("age 加密完成失败: {}", e))?;

        Ok(EncryptedData { ciphertext })
    }

    pub fn decrypt(&self, encrypted: &EncryptedData) -> Result<Vec<u8>> {
        let passphrase = self.get_passphrase();

        let decryptor = age::Decryptor::new(&encrypted.ciphertext[..])
            .map_err(|e| anyhow!("无法读取 age 加密数据: {}", e))?;

        let mut reader = match decryptor {
            age::Decryptor::Passphrase(d) => d
                .decrypt(&passphrase, None)
                .map_err(|e| anyhow!("age 解密失败（口令错误或数据损坏）: {}", e))?,
            _ => return Err(anyhow!("不支持的 age 解密模式")),
        };

        let mut plaintext = Vec::with_capacity(encrypted.ciphertext.len());
        reader.read_to_end(&mut plaintext)
            .map_err(|e| anyhow!("age 解密读取失败: {}", e))?;

        Ok(plaintext)
    }

    // ========================================================================
    // 文件加密/解密
    // ========================================================================

    pub fn encrypt_file_chunked(&self, input_path: &Path, output_path: &Path) -> Result<EncryptionMetadata> {
        let input_file = std::fs::File::open(input_path)?;
        let file_size = input_file.metadata()?.len();
        let output_file = std::fs::File::create(output_path)?;

        let encryptor = age::Encryptor::with_user_passphrase(self.get_passphrase());
        let mut writer = encryptor
            .wrap_output(output_file)
            .map_err(|e| anyhow!("无法创建 age 加密输出: {}", e))?;

        let mut reader = BufReader::new(input_file);
        let mut buffer = vec![0u8; 65536];
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 { break; }
            writer.write_all(&buffer[..bytes_read])?;
        }
        writer.finish()
            .map_err(|e| anyhow!("age 加密完成失败: {}", e))?;

        let encrypted_size = output_path.metadata()?.len();
        Ok(EncryptionMetadata { original_size: file_size, encrypted_size })
    }

    pub fn encrypt_file_with_progress<F>(
        &self, input_path: &Path, output_path: &Path, progress_callback: F,
    ) -> Result<EncryptionMetadata>
    where F: Fn(u64, u64),
    {
        let input_file = std::fs::File::open(input_path)?;
        let file_size = input_file.metadata()?.len();
        let output_file = std::fs::File::create(output_path)?;

        let encryptor = age::Encryptor::with_user_passphrase(self.get_passphrase());
        let mut writer = encryptor
            .wrap_output(output_file)
            .map_err(|e| anyhow!("无法创建 age 加密输出: {}", e))?;

        let mut reader = BufReader::new(input_file);
        let mut buffer = vec![0u8; 65536];
        let mut processed: u64 = 0;
        progress_callback(0, file_size);

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 { break; }
            writer.write_all(&buffer[..bytes_read])?;
            processed += bytes_read as u64;
            progress_callback(processed, file_size);
        }
        writer.finish()
            .map_err(|e| anyhow!("age 加密完成失败: {}", e))?;

        let encrypted_size = output_path.metadata()?.len();
        Ok(EncryptionMetadata { original_size: file_size, encrypted_size })
    }

    pub fn decrypt_file_with_progress<F>(
        &self, input_path: &Path, output_path: &Path, progress_callback: F,
    ) -> Result<u64>
    where F: Fn(u64, u64),
    {
        let input_file = std::fs::File::open(input_path)?;
        let file_size = input_file.metadata()?.len();
        let passphrase = self.get_passphrase();

        let decryptor = age::Decryptor::new(BufReader::new(input_file))
            .map_err(|e| anyhow!("无法读取 age 加密文件: {}", e))?;

        let mut reader = match decryptor {
            age::Decryptor::Passphrase(d) => d
                .decrypt(&passphrase, None)
                .map_err(|e| anyhow!("age 解密失败: {}", e))?,
            _ => return Err(anyhow!("不支持的 age 解密模式")),
        };

        let mut output_file = std::fs::File::create(output_path)?;
        let mut buffer = vec![0u8; 65536];
        let mut processed: u64 = 0;
        progress_callback(0, file_size);

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 { break; }
            output_file.write_all(&buffer[..bytes_read])?;
            processed += bytes_read as u64;
            progress_callback(processed, file_size);
        }
        Ok(processed)
    }

    pub fn decrypt_file(&self, input_path: &Path, output_path: &Path) -> Result<u64> {
        let passphrase = self.get_passphrase();

        let decryptor = age::Decryptor::new(std::fs::File::open(input_path)?)
            .map_err(|e| anyhow!("无法读取 age 加密文件: {}", e))?;

        let mut reader = match decryptor {
            age::Decryptor::Passphrase(d) => d
                .decrypt(&passphrase, None)
                .map_err(|e| anyhow!("age 解密失败: {}", e))?,
            _ => return Err(anyhow!("不支持的 age 解密模式")),
        };

        let mut output_file = std::fs::File::create(output_path)?;
        let original_size = std::io::copy(&mut reader, &mut output_file)?;
        Ok(original_size)
    }

    // ========================================================================
    // 加密文件识别
    // ========================================================================

    pub fn is_encrypted_file(path: &Path) -> Result<bool> {
        let mut file = std::fs::File::open(path)?;
        let mut prefix = [0u8; 20];
        if file.read_exact(&mut prefix).is_err() {
            return Ok(false);
        }
        Ok(&prefix[..19] == AGE_HEADER_PREFIX)
    }

    pub fn get_encrypted_file_info(path: &Path) -> Result<Option<(u8, u64)>> {
        if !Self::is_encrypted_file(path)? { return Ok(None); }
        Ok(Some((1, 0)))
    }

    // ========================================================================
    // 加密文件名管理（UUID + .age）
    // ========================================================================

    pub fn generate_encrypted_filename() -> String {
        format!("{}{}", uuid::Uuid::new_v4(), ENCRYPTED_FILE_EXTENSION)
    }

    pub fn generate_encrypted_folder_name() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub fn is_encrypted_folder_name(folder_name: &str) -> bool {
        uuid::Uuid::parse_str(folder_name).is_ok()
    }

    pub fn is_encrypted_filename(filename: &str) -> bool {
        filename.strip_suffix(ENCRYPTED_FILE_EXTENSION)
            .and_then(|stem| uuid::Uuid::parse_str(stem).ok())
            .is_some()
    }

    pub fn extract_uuid_from_encrypted_name(filename: &str) -> Option<&str> {
        filename.strip_suffix(ENCRYPTED_FILE_EXTENSION)
            .filter(|stem| uuid::Uuid::parse_str(stem).is_ok())
    }
}

// ============================================================================
// 数据结构
// ============================================================================

#[derive(Debug, Clone)]
pub struct EncryptedData {
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct EncryptionMetadata {
    pub original_size: u64,
    pub encrypted_size: u64,
}

// ============================================================================
// StreamingEncryptionService — 异步版
// ============================================================================

#[derive(Clone)]
pub struct StreamingEncryptionService {
    passphrase: String,
}

impl StreamingEncryptionService {
    pub fn new(passphrase: impl Into<String>, _algorithm: EncryptionAlgorithm) -> Self {
        Self { passphrase: passphrase.into() }
    }

    pub fn from_base64_key(passphrase: &str, _algorithm: EncryptionAlgorithm) -> Result<Self> {
        Ok(Self { passphrase: passphrase.to_string() })
    }

    pub async fn encrypt_file_streaming(
        &self, input_path: &Path, output_path: &Path,
    ) -> Result<EncryptionMetadata> {
        let input_path = input_path.to_path_buf();
        let output_path = output_path.to_path_buf();
        let passphrase_str = self.passphrase.clone();

        // age 是同步库，在阻塞线程中执行
        tokio::task::spawn_blocking(move || {
            let input_file = std::fs::File::open(&input_path)?;
            let file_size = input_file.metadata()?.len();
            let output_file = std::fs::File::create(&output_path)?;

            let passphrase = Secret::new(passphrase_str);
            let encryptor = age::Encryptor::with_user_passphrase(passphrase);

            let mut writer = encryptor
                .wrap_output(output_file)
                .map_err(|e| anyhow!("无法创建 age 加密输出: {}", e))?;

            let mut reader = std::io::BufReader::new(input_file);
            let mut buffer = vec![0u8; 65536];
            loop {
                let bytes_read = reader.read(&mut buffer)?;
                if bytes_read == 0 { break; }
                writer.write_all(&buffer[..bytes_read])?;
            }
            writer.finish()
                .map_err(|e| anyhow!("age 加密完成失败: {}", e))?;

            let encrypted_size = output_path.metadata()?.len();
            Ok(EncryptionMetadata { original_size: file_size, encrypted_size })
        })
        .await
        .map_err(|e| anyhow!("age 加密任务被终止: {}", e))?
    }

    pub async fn decrypt_file_streaming(
        &self, input_path: &Path, output_path: &Path,
    ) -> Result<u64> {
        let input_path = input_path.to_path_buf();
        let output_path = output_path.to_path_buf();
        let passphrase_str = self.passphrase.clone();

        tokio::task::spawn_blocking(move || {
            let passphrase = Secret::new(passphrase_str);

            let decryptor = age::Decryptor::new(std::fs::File::open(&input_path)?)
                .map_err(|e| anyhow!("无法读取 age 加密文件: {}", e))?;

            let mut reader = match decryptor {
                age::Decryptor::Passphrase(d) => d
                    .decrypt(&passphrase, None)
                    .map_err(|e| anyhow!("age 解密失败: {}", e))?,
                _ => return Err(anyhow!("不支持的 age 解密模式")),
            };

            let mut output_file = std::fs::File::create(&output_path)?;
            let total = std::io::copy(&mut reader, &mut output_file)?;
            Ok(total)
        })
        .await
        .map_err(|e| anyhow!("age 解密任务被终止: {}", e))?
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = EncryptionService::generate_random_passphrase();
        let svc = EncryptionService::new(key, EncryptionAlgorithm::Age);
        let plaintext = b"Hello, age encryption!";
        let encrypted = svc.encrypt(plaintext).unwrap();
        let decrypted = svc.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_decrypt_empty() {
        let key = EncryptionService::generate_random_passphrase();
        let svc = EncryptionService::new(key, EncryptionAlgorithm::Age);
        let encrypted = svc.encrypt(b"").unwrap();
        let decrypted = svc.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, b"");
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = EncryptionService::generate_random_passphrase();
        let key2 = EncryptionService::generate_random_passphrase();
        let svc1 = EncryptionService::new(key1, EncryptionAlgorithm::Age);
        let svc2 = EncryptionService::new(key2, EncryptionAlgorithm::Age);
        let encrypted = svc1.encrypt(b"secret data").unwrap();
        assert!(svc2.decrypt(&encrypted).is_err());
    }

    #[test]
    fn test_is_encrypted_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.age");
        let key = EncryptionService::generate_random_passphrase();
        let svc = EncryptionService::new(key, EncryptionAlgorithm::Age);
        svc.encrypt_file_chunked(Path::new("Cargo.toml"), &file_path).unwrap();
        assert!(EncryptionService::is_encrypted_file(&file_path).unwrap());
        assert!(!EncryptionService::is_encrypted_file(Path::new("Cargo.toml")).unwrap());
    }

    #[test]
    fn test_encrypted_filename() {
        let name = EncryptionService::generate_encrypted_filename();
        assert!(name.ends_with(".age"));
        assert!(EncryptionService::is_encrypted_filename(&name));
        assert!(EncryptionService::extract_uuid_from_encrypted_name(&name).is_some());
        assert!(!EncryptionService::is_encrypted_filename("normal.txt"));
    }

    #[test]
    fn test_file_encrypt_decrypt_roundtrip() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("input.txt");
        let enc = dir.path().join("output.age");
        let dec = dir.path().join("decrypted.txt");
        std::fs::write(&input, b"Hello age file encryption!").unwrap();
        let key = EncryptionService::generate_random_passphrase();
        let svc = EncryptionService::new(key, EncryptionAlgorithm::Age);
        svc.encrypt_file_chunked(&input, &enc).unwrap();
        svc.decrypt_file(&enc, &dec).unwrap();
        assert_eq!(std::fs::read_to_string(&dec).unwrap(), "Hello age file encryption!");
    }
}
