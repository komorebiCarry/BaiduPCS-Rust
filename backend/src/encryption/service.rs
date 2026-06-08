//! 加密服务
//!
//! 提供 AES-256-GCM 和 ChaCha20-Poly1305 加密功能

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use rand::RngCore;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::autobackup::config::EncryptionAlgorithm;

/// 加密文件魔数（伪随机字节，避免被识别为加密文件）
const MAGIC_V1: &[u8; 6] = &[0xA3, 0x7F, 0x2C, 0x91, 0xE4, 0x5B];

/// 默认分块大小：16MB
const DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// 加密文件扩展名（使用通用的 .dat 后缀，避免被识别）
pub const ENCRYPTED_FILE_EXTENSION: &str = ".dat";

/// 加密服务
#[derive(Clone, Debug)]
pub struct EncryptionService {
    /// 主密钥（32 字节）
    master_key: [u8; 32],
    /// 加密算法
    algorithm: EncryptionAlgorithm,
}

impl EncryptionService {
    /// 创建新的加密服务
    pub fn new(master_key: [u8; 32], algorithm: EncryptionAlgorithm) -> Self {
        Self {
            master_key,
            algorithm,
        }
    }

    /// 从 Base64 密钥创建
    pub fn from_base64_key(key_base64: &str, algorithm: EncryptionAlgorithm) -> Result<Self> {
        let key_bytes = BASE64.decode(key_base64)?;
        if key_bytes.len() != 32 {
            return Err(anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }

        let mut master_key = [0u8; 32];
        master_key.copy_from_slice(&key_bytes);

        Ok(Self::new(master_key, algorithm))
    }

    /// 生成新的主密钥
    pub fn generate_master_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        key
    }

    /// 生成主密钥并返回 Base64 编码
    pub fn generate_master_key_base64() -> String {
        let key = Self::generate_master_key();
        BASE64.encode(key)
    }

    /// 获取主密钥的 Base64 编码
    pub fn get_key_base64(&self) -> String {
        BASE64.encode(self.master_key)
    }

    /// 生成随机 Nonce
    fn generate_nonce() -> [u8; 12] {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        nonce
    }

    /// 加密数据（内存中）
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData> {
        let nonce = Self::generate_nonce();

        match self.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = Aes256Gcm::new_from_slice(&self.master_key)
                    .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                let nonce_obj = Nonce::from_slice(&nonce);
                let ciphertext = cipher
                    .encrypt(nonce_obj, plaintext)
                    .map_err(|e| anyhow!("Encryption failed: {}", e))?;

                Ok(EncryptedData {
                    ciphertext,
                    nonce,
                    algorithm: self.algorithm,
                    version: 1,
                })
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
                let cipher = ChaCha20Poly1305::new_from_slice(&self.master_key)
                    .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                let nonce_obj = chacha20poly1305::Nonce::from_slice(&nonce);
                let ciphertext = cipher
                    .encrypt(nonce_obj, plaintext)
                    .map_err(|e| anyhow!("Encryption failed: {}", e))?;

                Ok(EncryptedData {
                    ciphertext,
                    nonce,
                    algorithm: self.algorithm,
                    version: 1,
                })
            }
        }
    }

    /// 解密数据（内存中）
    pub fn decrypt(&self, encrypted: &EncryptedData) -> Result<Vec<u8>> {
        match encrypted.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = Aes256Gcm::new_from_slice(&self.master_key)
                    .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                let nonce = Nonce::from_slice(&encrypted.nonce);
                let plaintext = cipher
                    .decrypt(nonce, encrypted.ciphertext.as_ref())
                    .map_err(|e| anyhow!("Decryption failed: {}", e))?;
                Ok(plaintext)
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
                let cipher = ChaCha20Poly1305::new_from_slice(&self.master_key)
                    .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                let nonce = chacha20poly1305::Nonce::from_slice(&encrypted.nonce);
                let plaintext = cipher
                    .decrypt(nonce, encrypted.ciphertext.as_ref())
                    .map_err(|e| anyhow!("Decryption failed: {}", e))?;
                Ok(plaintext)
            }
        }
    }

    /// 加密文件（分块模式，适合大文件）
    pub fn encrypt_file_chunked(&self, input_path: &Path, output_path: &Path) -> Result<EncryptionMetadata> {
        let input_file = std::fs::File::open(input_path)?;
        let file_size = input_file.metadata()?.len();
        let total_chunks = (file_size as usize).div_ceil(DEFAULT_CHUNK_SIZE) as u32;

        let mut output_file = BufWriter::new(std::fs::File::create(output_path)?);

        // 生成主 Nonce
        let master_nonce = Self::generate_nonce();

        // 写入文件头
        output_file.write_all(MAGIC_V1)?;
        output_file.write_all(&[self.algorithm as u8])?;
        output_file.write_all(&master_nonce)?;
        output_file.write_all(&file_size.to_le_bytes())?;
        output_file.write_all(&total_chunks.to_le_bytes())?;

        // 分块加密
        let mut reader = BufReader::with_capacity(DEFAULT_CHUNK_SIZE, input_file);
        let mut buffer = vec![0u8; DEFAULT_CHUNK_SIZE];
        let mut chunk_index: u32 = 0;

        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            // 派生块 Nonce
            let chunk_nonce = Self::derive_chunk_nonce(&master_nonce, chunk_index);
            let nonce = Nonce::from_slice(&chunk_nonce);

            // 加密当前块
            let ciphertext = cipher
                .encrypt(nonce, &buffer[..bytes_read])
                .map_err(|e| anyhow!("Chunk {} encryption failed: {}", chunk_index, e))?;

            // 写入块 Nonce 和密文
            output_file.write_all(&chunk_nonce)?;
            output_file.write_all(&(ciphertext.len() as u32).to_le_bytes())?;
            output_file.write_all(&ciphertext)?;

            chunk_index += 1;
        }

        output_file.flush()?;

        Ok(EncryptionMetadata {
            original_size: file_size,
            encrypted_size: output_path.metadata()?.len(),
            nonce: BASE64.encode(master_nonce),
            algorithm: self.algorithm,
            version: 1,
        })
    }

    /// 加密文件（带进度回调）
    pub fn encrypt_file_with_progress<F>(
        &self,
        input_path: &Path,
        output_path: &Path,
        progress_callback: F,
    ) -> Result<EncryptionMetadata>
    where
        F: Fn(u64, u64),
    {
        self.encrypt_file_chunked_with_progress(input_path, output_path, progress_callback)
    }

    /// 分块加密文件（带进度回调）
    fn encrypt_file_chunked_with_progress<F>(
        &self,
        input_path: &Path,
        output_path: &Path,
        progress_callback: F,
    ) -> Result<EncryptionMetadata>
    where
        F: Fn(u64, u64),
    {
        let input_file = std::fs::File::open(input_path)?;
        let file_size = input_file.metadata()?.len();
        let total_chunks = (file_size as usize).div_ceil(DEFAULT_CHUNK_SIZE) as u32;

        let mut output_file = BufWriter::new(std::fs::File::create(output_path)?);

        // 生成主 Nonce
        let master_nonce = Self::generate_nonce();

        // 写入文件头
        output_file.write_all(MAGIC_V1)?;
        output_file.write_all(&[self.algorithm as u8])?;
        output_file.write_all(&master_nonce)?;
        output_file.write_all(&file_size.to_le_bytes())?;
        output_file.write_all(&total_chunks.to_le_bytes())?;

        // 分块加密
        let mut reader = BufReader::with_capacity(DEFAULT_CHUNK_SIZE, input_file);
        let mut buffer = vec![0u8; DEFAULT_CHUNK_SIZE];
        let mut chunk_index: u32 = 0;
        let mut processed_bytes: u64 = 0;

        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;

        // 初始进度回调
        progress_callback(0, file_size);

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            // 派生块 Nonce
            let chunk_nonce = Self::derive_chunk_nonce(&master_nonce, chunk_index);
            let nonce = Nonce::from_slice(&chunk_nonce);

            // 加密当前块
            let ciphertext = cipher
                .encrypt(nonce, &buffer[..bytes_read])
                .map_err(|e| anyhow!("Chunk {} encryption failed: {}", chunk_index, e))?;

            // 写入块 Nonce 和密文
            output_file.write_all(&chunk_nonce)?;
            output_file.write_all(&(ciphertext.len() as u32).to_le_bytes())?;
            output_file.write_all(&ciphertext)?;

            // 🔥 更新进度
            processed_bytes += bytes_read as u64;
            progress_callback(processed_bytes, file_size);

            chunk_index += 1;
        }

        output_file.flush()?;

        Ok(EncryptionMetadata {
            original_size: file_size,
            encrypted_size: output_path.metadata()?.len(),
            nonce: BASE64.encode(master_nonce),
            algorithm: self.algorithm,
            version: 1,
        })
    }

    /// 解密文件（带进度回调）
    ///
    /// # 参数
    /// * `input_path` - 输入文件路径
    /// * `output_path` - 输出文件路径
    /// * `progress_callback` - 进度回调函数，参数为 (已处理字节数, 总字节数)
    pub fn decrypt_file_with_progress<F>(
        &self,
        input_path: &Path,
        output_path: &Path,
        progress_callback: F,
    ) -> Result<u64>
    where
        F: Fn(u64, u64),
    {
        let mut input_file = BufReader::new(std::fs::File::open(input_path)?);

        // 读取魔数
        let mut magic = [0u8; 6];
        input_file.read_exact(&mut magic)?;

        if &magic != MAGIC_V1 {
            return Err(anyhow!("Invalid encrypted file format"));
        }

        self.decrypt_file_chunked_with_progress(input_file, output_path, progress_callback)
    }

    /// 解密分块加密文件（带进度回调）
    fn decrypt_file_chunked_with_progress<R: Read, F>(
        &self,
        mut reader: R,
        output_path: &Path,
        progress_callback: F,
    ) -> Result<u64>
    where
        F: Fn(u64, u64),
    {
        // 读取算法
        let mut algo = [0u8; 1];
        reader.read_exact(&mut algo)?;

        // 读取主 Nonce
        let mut master_nonce = [0u8; 12];
        reader.read_exact(&mut master_nonce)?;

        // 读取原始文件大小
        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes)?;
        let original_size = u64::from_le_bytes(size_bytes);

        // 读取块数量
        let mut chunk_count_bytes = [0u8; 4];
        reader.read_exact(&mut chunk_count_bytes)?;
        let total_chunks = u32::from_le_bytes(chunk_count_bytes);

        let mut output_file = BufWriter::new(std::fs::File::create(output_path)?);
        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;

        let mut processed_bytes: u64 = 0;

        // 初始进度回调
        progress_callback(0, original_size);

        // 分块解密
        for chunk_index in 0..total_chunks {
            // 读取块 Nonce
            let mut chunk_nonce = [0u8; 12];
            reader.read_exact(&mut chunk_nonce)?;

            // 读取密文长度
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let ciphertext_len = u32::from_le_bytes(len_bytes) as usize;

            // 读取密文
            let mut ciphertext = vec![0u8; ciphertext_len];
            reader.read_exact(&mut ciphertext)?;

            // 解密
            let nonce = Nonce::from_slice(&chunk_nonce);
            let plaintext = cipher
                .decrypt(nonce, ciphertext.as_ref())
                .map_err(|e| anyhow!("Chunk {} decryption failed: {}", chunk_index, e))?;

            output_file.write_all(&plaintext)?;

            // 🔥 更新进度
            processed_bytes += plaintext.len() as u64;
            progress_callback(processed_bytes, original_size);
        }

        output_file.flush()?;

        Ok(original_size)
    }

    /// 解密文件
    pub fn decrypt_file(&self, input_path: &Path, output_path: &Path) -> Result<u64> {
        let mut input_file = BufReader::new(std::fs::File::open(input_path)?);

        // 读取魔数
        let mut magic = [0u8; 6];
        input_file.read_exact(&mut magic)?;

        if &magic != MAGIC_V1 {
            return Err(anyhow!("Invalid encrypted file format"));
        }

        self.decrypt_file_chunked(input_file, output_path)
    }

    /// 解密分块加密文件
    fn decrypt_file_chunked<R: Read>(&self, mut reader: R, output_path: &Path) -> Result<u64> {
        // 读取算法
        let mut algo = [0u8; 1];
        reader.read_exact(&mut algo)?;

        // 读取主 Nonce
        let mut master_nonce = [0u8; 12];
        reader.read_exact(&mut master_nonce)?;

        // 读取原始文件大小
        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes)?;
        let original_size = u64::from_le_bytes(size_bytes);

        // 读取块数量
        let mut chunk_count_bytes = [0u8; 4];
        reader.read_exact(&mut chunk_count_bytes)?;
        let total_chunks = u32::from_le_bytes(chunk_count_bytes);

        let mut output_file = BufWriter::new(std::fs::File::create(output_path)?);
        let cipher = Aes256Gcm::new_from_slice(&self.master_key)
            .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;

        // 分块解密
        for chunk_index in 0..total_chunks {
            // 读取块 Nonce
            let mut chunk_nonce = [0u8; 12];
            reader.read_exact(&mut chunk_nonce)?;

            // 读取密文长度
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let ciphertext_len = u32::from_le_bytes(len_bytes) as usize;

            // 读取密文
            let mut ciphertext = vec![0u8; ciphertext_len];
            reader.read_exact(&mut ciphertext)?;

            // 解密
            let nonce = Nonce::from_slice(&chunk_nonce);
            let plaintext = cipher
                .decrypt(nonce, ciphertext.as_ref())
                .map_err(|e| anyhow!("Chunk {} decryption failed: {}", chunk_index, e))?;

            output_file.write_all(&plaintext)?;
        }

        output_file.flush()?;

        Ok(original_size)
    }

    /// 派生块 Nonce
    fn derive_chunk_nonce(master_nonce: &[u8; 12], chunk_index: u32) -> [u8; 12] {
        let mut chunk_nonce = *master_nonce;
        let index_bytes = chunk_index.to_le_bytes();
        // XOR 最后 4 字节
        for i in 0..4 {
            chunk_nonce[8 + i] ^= index_bytes[i];
        }
        chunk_nonce
    }

    /// 检查文件是否为加密文件
    pub fn is_encrypted_file(path: &Path) -> Result<bool> {
        let mut file = std::fs::File::open(path)?;
        let mut magic = [0u8; 6];

        if file.read_exact(&mut magic).is_err() {
            return Ok(false);
        }

        Ok(&magic == MAGIC_V1)
    }

    /// 获取加密文件的格式版本
    /// 返回 None 如果不是加密文件，Some(1) 表示新格式
    pub fn get_encryption_version(path: &Path) -> Result<Option<u8>> {
        let mut file = std::fs::File::open(path)?;
        let mut magic = [0u8; 6];

        if file.read_exact(&mut magic).is_err() {
            return Ok(None);
        }

        if &magic == MAGIC_V1 {
            Ok(Some(1))
        } else {
            Ok(None)
        }
    }

    /// 获取加密文件信息（版本、原始大小）
    pub fn get_encrypted_file_info(path: &Path) -> Result<Option<(u8, u64)>> {
        let mut file = BufReader::new(std::fs::File::open(path)?);
        let mut magic = [0u8; 6];

        if file.read_exact(&mut magic).is_err() {
            return Ok(None);
        }

        if &magic != MAGIC_V1 {
            return Ok(None);
        }

        // 跳过算法字节
        let mut algo = [0u8; 1];
        file.read_exact(&mut algo)?;

        // 跳过 nonce
        let mut nonce = [0u8; 12];
        file.read_exact(&mut nonce)?;

        // 读取原始大小
        let mut size_bytes = [0u8; 8];
        file.read_exact(&mut size_bytes)?;
        let original_size = u64::from_le_bytes(size_bytes);

        Ok(Some((1, original_size)))
    }

    /// 生成加密文件名（UUID + .dat）
    /// 格式：<UUIDv4>.dat
    /// 示例：a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat
    pub fn generate_encrypted_filename() -> String {
        format!("{}{}", uuid::Uuid::new_v4(), ENCRYPTED_FILE_EXTENSION)
    }

    /// 生成加密文件夹名（纯 UUID）
    /// 格式：<UUIDv4>
    /// 示例：a1b2c3d4-e5f6-7890-abcd-ef1234567890
    pub fn generate_encrypted_folder_name() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// 判断文件夹名是否为加密文件夹（通过文件夹名判断）
    /// 检查文件夹名是否为有效的 UUID 格式
    pub fn is_encrypted_folder_name(folder_name: &str) -> bool {
        uuid::Uuid::parse_str(folder_name).is_ok()
    }

    /// 判断文件名是否为加密文件（通过文件名判断）
    /// 检查文件名是否为 UUID.dat 格式
    pub fn is_encrypted_filename(filename: &str) -> bool {
        filename.strip_suffix(ENCRYPTED_FILE_EXTENSION)
            .and_then(|stem| uuid::Uuid::parse_str(stem).ok())
            .is_some()
    }

    /// 从加密文件名提取 UUID
    /// 返回 None 如果文件名格式不正确
    pub fn extract_uuid_from_encrypted_name(filename: &str) -> Option<&str> {
        filename.strip_suffix(ENCRYPTED_FILE_EXTENSION)
            .filter(|stem| uuid::Uuid::parse_str(stem).is_ok())
    }
}

/// 加密数据
#[derive(Debug, Clone)]
pub struct EncryptedData {
    /// 密文
    pub ciphertext: Vec<u8>,
    /// Nonce
    pub nonce: [u8; 12],
    /// 算法
    pub algorithm: EncryptionAlgorithm,
    /// 版本
    pub version: u8,
}

/// 加密元数据
#[derive(Debug, Clone)]
pub struct EncryptionMetadata {
    /// 原始文件大小
    pub original_size: u64,
    /// 加密后文件大小
    pub encrypted_size: u64,
    /// Nonce（Base64）
    pub nonce: String,
    /// 算法
    pub algorithm: EncryptionAlgorithm,
    /// 版本
    pub version: u8,
}

/// 流式加密服务（异步版本，适合大文件）
///
/// 与 `EncryptionService` 的区别：
/// - 使用异步 I/O（tokio）
/// - 可配置分块大小
/// - 更低的内存占用
#[derive(Clone)]
pub struct StreamingEncryptionService {
    /// 主密钥（32 字节）
    master_key: [u8; 32],
    /// 加密算法
    algorithm: EncryptionAlgorithm,
    /// 分块大小（字节）
    chunk_size: usize,
}

impl StreamingEncryptionService {
    /// 创建新的流式加密服务
    pub fn new(master_key: [u8; 32], algorithm: EncryptionAlgorithm) -> Self {
        Self {
            master_key,
            algorithm,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// 创建带自定义分块大小的流式加密服务
    pub fn with_chunk_size(master_key: [u8; 32], algorithm: EncryptionAlgorithm, chunk_size: usize) -> Self {
        Self {
            master_key,
            algorithm,
            chunk_size,
        }
    }

    /// 从 Base64 密钥创建
    pub fn from_base64_key(key_base64: &str, algorithm: EncryptionAlgorithm) -> Result<Self> {
        let key_bytes = BASE64.decode(key_base64)?;
        if key_bytes.len() != 32 {
            return Err(anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }

        let mut master_key = [0u8; 32];
        master_key.copy_from_slice(&key_bytes);

        Ok(Self::new(master_key, algorithm))
    }

    /// 获取分块大小
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// 生成随机 Nonce
    fn generate_nonce() -> [u8; 12] {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        nonce
    }

    /// 派生块 Nonce（确保每块 Nonce 唯一）
    fn derive_chunk_nonce(master_nonce: &[u8; 12], chunk_index: u32) -> [u8; 12] {
        let mut chunk_nonce = *master_nonce;
        let index_bytes = chunk_index.to_le_bytes();
        // XOR 最后 4 字节
        for i in 0..4 {
            chunk_nonce[8 + i] ^= index_bytes[i];
        }
        chunk_nonce
    }

    /// 流式加密大文件（异步）
    pub async fn encrypt_file_streaming(
        &self,
        input_path: &Path,
        output_path: &Path,
    ) -> Result<EncryptionMetadata> {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

        let input_file = File::open(input_path).await?;
        let file_size = input_file.metadata().await?.len();
        let total_chunks = (file_size as usize).div_ceil(self.chunk_size) as u32;

        let output_file = File::create(output_path).await?;
        let mut writer = BufWriter::new(output_file);

        // 生成主 Nonce
        let master_nonce = Self::generate_nonce();

        // 写入文件头（v2 格式）
        writer.write_all(MAGIC_V1).await?;
        writer.write_all(&[self.algorithm as u8]).await?;
        writer.write_all(&master_nonce).await?;
        writer.write_all(&file_size.to_le_bytes()).await?;
        writer.write_all(&total_chunks.to_le_bytes()).await?;

        // 分块加密
        let mut reader = BufReader::with_capacity(self.chunk_size, input_file);
        let mut buffer = vec![0u8; self.chunk_size];
        let mut chunk_index: u32 = 0;

        loop {
            let bytes_read = reader.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }

            // 派生块 Nonce
            let chunk_nonce = Self::derive_chunk_nonce(&master_nonce, chunk_index);

            // 加密当前块
            let ciphertext = match self.algorithm {
                EncryptionAlgorithm::Aes256Gcm => {
                    let cipher = Aes256Gcm::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = Nonce::from_slice(&chunk_nonce);
                    cipher
                        .encrypt(nonce, &buffer[..bytes_read])
                        .map_err(|e| anyhow!("Chunk {} encryption failed: {}", chunk_index, e))?
                }
                EncryptionAlgorithm::ChaCha20Poly1305 => {
                    use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
                    let cipher = ChaCha20Poly1305::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = chacha20poly1305::Nonce::from_slice(&chunk_nonce);
                    cipher
                        .encrypt(nonce, &buffer[..bytes_read])
                        .map_err(|e| anyhow!("Chunk {} encryption failed: {}", chunk_index, e))?
                }
            };

            // 写入块 Nonce 和密文长度和密文
            writer.write_all(&chunk_nonce).await?;
            writer.write_all(&(ciphertext.len() as u32).to_le_bytes()).await?;
            writer.write_all(&ciphertext).await?;

            chunk_index += 1;
        }

        writer.flush().await?;

        let encrypted_size = tokio::fs::metadata(output_path).await?.len();

        Ok(EncryptionMetadata {
            original_size: file_size,
            encrypted_size,
            nonce: BASE64.encode(master_nonce),
            algorithm: self.algorithm,
            version: 1,
        })
    }

    /// 流式解密大文件（异步）
    pub async fn decrypt_file_streaming(
        &self,
        input_path: &Path,
        output_path: &Path,
    ) -> Result<u64> {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

        let input_file = File::open(input_path).await?;
        let mut reader = BufReader::new(input_file);

        // 读取魔数
        let mut magic = [0u8; 6];
        reader.read_exact(&mut magic).await?;

        if &magic != MAGIC_V1 {
            return Err(anyhow!("Invalid encrypted file format"));
        }

        // 读取算法
        let mut algo = [0u8; 1];
        reader.read_exact(&mut algo).await?;

        // 读取主 Nonce
        let mut master_nonce = [0u8; 12];
        reader.read_exact(&mut master_nonce).await?;

        // 读取原始文件大小
        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes).await?;
        let original_size = u64::from_le_bytes(size_bytes);

        // 读取块数量
        let mut chunk_count_bytes = [0u8; 4];
        reader.read_exact(&mut chunk_count_bytes).await?;
        let total_chunks = u32::from_le_bytes(chunk_count_bytes);

        let output_file = File::create(output_path).await?;
        let mut writer = BufWriter::new(output_file);

        // 分块解密
        for chunk_index in 0..total_chunks {
            // 读取块 Nonce
            let mut chunk_nonce = [0u8; 12];
            reader.read_exact(&mut chunk_nonce).await?;

            // 读取密文长度
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes).await?;
            let ciphertext_len = u32::from_le_bytes(len_bytes) as usize;

            // 读取密文
            let mut ciphertext = vec![0u8; ciphertext_len];
            reader.read_exact(&mut ciphertext).await?;

            // 解密
            let plaintext = match self.algorithm {
                EncryptionAlgorithm::Aes256Gcm => {
                    let cipher = Aes256Gcm::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = Nonce::from_slice(&chunk_nonce);
                    cipher
                        .decrypt(nonce, ciphertext.as_ref())
                        .map_err(|e| anyhow!("Chunk {} decryption failed: {}", chunk_index, e))?
                }
                EncryptionAlgorithm::ChaCha20Poly1305 => {
                    use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
                    let cipher = ChaCha20Poly1305::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = chacha20poly1305::Nonce::from_slice(&chunk_nonce);
                    cipher
                        .decrypt(nonce, ciphertext.as_ref())
                        .map_err(|e| anyhow!("Chunk {} decryption failed: {}", chunk_index, e))?
                }
            };

            writer.write_all(&plaintext).await?;
        }

        writer.flush().await?;

        Ok(original_size)
    }

    /// 流式加密大文件（异步，带进度回调）
    pub async fn encrypt_file_streaming_with_progress<F>(
        &self,
        input_path: &Path,
        output_path: &Path,
        progress_callback: F,
    ) -> Result<EncryptionMetadata>
    where
        F: Fn(u64, u64) + Send + Sync,
    {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

        let input_file = File::open(input_path).await?;
        let file_size = input_file.metadata().await?.len();
        let total_chunks = (file_size as usize).div_ceil(self.chunk_size) as u32;

        let output_file = File::create(output_path).await?;
        let mut writer = BufWriter::new(output_file);

        // 生成主 Nonce
        let master_nonce = Self::generate_nonce();

        // 写入文件头
        writer.write_all(MAGIC_V1).await?;
        writer.write_all(&[self.algorithm as u8]).await?;
        writer.write_all(&master_nonce).await?;
        writer.write_all(&file_size.to_le_bytes()).await?;
        writer.write_all(&total_chunks.to_le_bytes()).await?;

        // 分块加密
        let mut reader = BufReader::with_capacity(self.chunk_size, input_file);
        let mut buffer = vec![0u8; self.chunk_size];
        let mut chunk_index: u32 = 0;
        let mut processed_bytes: u64 = 0;

        // 初始进度回调
        progress_callback(0, file_size);

        loop {
            let bytes_read = reader.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }

            // 派生块 Nonce
            let chunk_nonce = Self::derive_chunk_nonce(&master_nonce, chunk_index);

            // 加密当前块
            let ciphertext = match self.algorithm {
                EncryptionAlgorithm::Aes256Gcm => {
                    let cipher = Aes256Gcm::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = Nonce::from_slice(&chunk_nonce);
                    cipher
                        .encrypt(nonce, &buffer[..bytes_read])
                        .map_err(|e| anyhow!("Chunk {} encryption failed: {}", chunk_index, e))?
                }
                EncryptionAlgorithm::ChaCha20Poly1305 => {
                    use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
                    let cipher = ChaCha20Poly1305::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = chacha20poly1305::Nonce::from_slice(&chunk_nonce);
                    cipher
                        .encrypt(nonce, &buffer[..bytes_read])
                        .map_err(|e| anyhow!("Chunk {} encryption failed: {}", chunk_index, e))?
                }
            };

            // 写入块 Nonce 和密文长度和密文
            writer.write_all(&chunk_nonce).await?;
            writer.write_all(&(ciphertext.len() as u32).to_le_bytes()).await?;
            writer.write_all(&ciphertext).await?;

            // 🔥 更新进度
            processed_bytes += bytes_read as u64;
            progress_callback(processed_bytes, file_size);

            chunk_index += 1;
        }

        writer.flush().await?;

        let encrypted_size = tokio::fs::metadata(output_path).await?.len();

        Ok(EncryptionMetadata {
            original_size: file_size,
            encrypted_size,
            nonce: BASE64.encode(master_nonce),
            algorithm: self.algorithm,
            version: 1,
        })
    }

    /// 流式解密大文件（异步，带进度回调）
    pub async fn decrypt_file_streaming_with_progress<F>(
        &self,
        input_path: &Path,
        output_path: &Path,
        progress_callback: F,
    ) -> Result<u64>
    where
        F: Fn(u64, u64) + Send + Sync,
    {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

        let input_file = File::open(input_path).await?;
        let mut reader = BufReader::new(input_file);

        // 读取魔数
        let mut magic = [0u8; 6];
        reader.read_exact(&mut magic).await?;

        if &magic != MAGIC_V1 {
            return Err(anyhow!("Invalid encrypted file format"));
        }

        // 读取算法
        let mut algo = [0u8; 1];
        reader.read_exact(&mut algo).await?;

        // 读取主 Nonce
        let mut master_nonce = [0u8; 12];
        reader.read_exact(&mut master_nonce).await?;

        // 读取原始文件大小
        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes).await?;
        let original_size = u64::from_le_bytes(size_bytes);

        // 读取块数量
        let mut chunk_count_bytes = [0u8; 4];
        reader.read_exact(&mut chunk_count_bytes).await?;
        let total_chunks = u32::from_le_bytes(chunk_count_bytes);

        let output_file = File::create(output_path).await?;
        let mut writer = BufWriter::new(output_file);

        let mut processed_bytes: u64 = 0;

        // 初始进度回调
        progress_callback(0, original_size);

        // 分块解密
        for chunk_index in 0..total_chunks {
            // 读取块 Nonce
            let mut chunk_nonce = [0u8; 12];
            reader.read_exact(&mut chunk_nonce).await?;

            // 读取密文长度
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes).await?;
            let ciphertext_len = u32::from_le_bytes(len_bytes) as usize;

            // 读取密文
            let mut ciphertext = vec![0u8; ciphertext_len];
            reader.read_exact(&mut ciphertext).await?;

            // 解密
            let plaintext = match self.algorithm {
                EncryptionAlgorithm::Aes256Gcm => {
                    let cipher = Aes256Gcm::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = Nonce::from_slice(&chunk_nonce);
                    cipher
                        .decrypt(nonce, ciphertext.as_ref())
                        .map_err(|e| anyhow!("Chunk {} decryption failed: {}", chunk_index, e))?
                }
                EncryptionAlgorithm::ChaCha20Poly1305 => {
                    use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
                    let cipher = ChaCha20Poly1305::new_from_slice(&self.master_key)
                        .map_err(|e| anyhow!("Failed to create cipher: {}", e))?;
                    let nonce = chacha20poly1305::Nonce::from_slice(&chunk_nonce);
                    cipher
                        .decrypt(nonce, ciphertext.as_ref())
                        .map_err(|e| anyhow!("Chunk {} decryption failed: {}", chunk_index, e))?
                }
            };

            writer.write_all(&plaintext).await?;

            // 🔥 更新进度
            processed_bytes += plaintext.len() as u64;
            progress_callback(processed_bytes, original_size);
        }

        writer.flush().await?;

        Ok(original_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_encrypt_decrypt_aes256gcm() {
        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        let plaintext = b"Hello, World!";
        let encrypted = service.encrypt(plaintext).unwrap();
        let decrypted = service.decrypt(&encrypted).unwrap();

        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
        assert_eq!(encrypted.algorithm, EncryptionAlgorithm::Aes256Gcm);
        assert_eq!(encrypted.version, 1);
    }

    #[test]
    fn test_encrypt_decrypt_chacha20poly1305() {
        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::ChaCha20Poly1305);

        let plaintext = b"Hello, World! This is a test with ChaCha20-Poly1305.";
        let encrypted = service.encrypt(plaintext).unwrap();
        let decrypted = service.decrypt(&encrypted).unwrap();

        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
        assert_eq!(encrypted.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);
    }

    #[test]
    fn test_encrypt_decrypt_file_v1() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("test.txt");
        let encrypted_path = dir.path().join("test.bkup");
        let decrypted_path = dir.path().join("test_decrypted.txt");

        // 创建测试文件（小于 1GB，使用 v1 格式）
        std::fs::write(&input_path, "Hello, World! This is a test file.").unwrap();

        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        // 加密
        let metadata = service.encrypt_file_chunked(&input_path, &encrypted_path).unwrap();
        assert!(metadata.encrypted_size > 0);
        assert_eq!(metadata.version, 1); // v1 格式

        // 验证是加密文件
        assert!(EncryptionService::is_encrypted_file(&encrypted_path).unwrap());

        // 解密
        service.decrypt_file(&encrypted_path, &decrypted_path).unwrap();

        // 验证内容
        let original = std::fs::read_to_string(&input_path).unwrap();
        let decrypted = std::fs::read_to_string(&decrypted_path).unwrap();
        assert_eq!(original, decrypted);
    }

    #[test]
    fn test_encrypt_decrypt_file_chunked() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("test_chunked.bin");
        let encrypted_path = dir.path().join("test_chunked.bkup");
        let decrypted_path = dir.path().join("test_chunked_decrypted.bin");

        // 创建一个大于分块大小的测试文件（使用 20MB 测试分块）
        let test_data: Vec<u8> = (0..20 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        std::fs::write(&input_path, &test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        // 使用分块加密
        let metadata = service.encrypt_file_chunked(&input_path, &encrypted_path).unwrap();
        assert!(metadata.encrypted_size > 0);
        assert_eq!(metadata.version, 1); // 统一使用 v1 格式

        // 验证是加密文件
        assert!(EncryptionService::is_encrypted_file(&encrypted_path).unwrap());

        // 解密
        service.decrypt_file(&encrypted_path, &decrypted_path).unwrap();

        // 验证内容
        let decrypted = std::fs::read(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
    }

    #[test]
    fn test_derive_chunk_nonce_uniqueness() {
        let master_nonce = [1u8; 12];

        // 生成多个块 Nonce
        let nonce_0 = EncryptionService::derive_chunk_nonce(&master_nonce, 0);
        let nonce_1 = EncryptionService::derive_chunk_nonce(&master_nonce, 1);
        let nonce_2 = EncryptionService::derive_chunk_nonce(&master_nonce, 2);
        let nonce_max = EncryptionService::derive_chunk_nonce(&master_nonce, u32::MAX);

        // 验证所有 Nonce 都不同
        assert_ne!(nonce_0, nonce_1);
        assert_ne!(nonce_1, nonce_2);
        assert_ne!(nonce_0, nonce_2);
        assert_ne!(nonce_0, nonce_max);

        // 验证相同索引产生相同 Nonce
        let nonce_1_again = EncryptionService::derive_chunk_nonce(&master_nonce, 1);
        assert_eq!(nonce_1, nonce_1_again);
    }

    #[test]
    fn test_key_generation_randomness() {
        // 生成多个密钥，验证它们都不同
        let key1 = EncryptionService::generate_master_key();
        let key2 = EncryptionService::generate_master_key();
        let key3 = EncryptionService::generate_master_key();

        assert_ne!(key1, key2);
        assert_ne!(key2, key3);
        assert_ne!(key1, key3);

        // 验证密钥长度
        assert_eq!(key1.len(), 32);
    }

    #[test]
    fn test_key_base64_roundtrip() {
        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        let key_base64 = service.get_key_base64();
        let service2 = EncryptionService::from_base64_key(&key_base64, EncryptionAlgorithm::Aes256Gcm).unwrap();

        // 验证两个服务可以互相解密
        let plaintext = b"Test data for key roundtrip";
        let encrypted = service.encrypt(plaintext).unwrap();
        let decrypted = service2.decrypt(&encrypted).unwrap();

        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_encrypted_filename_generation() {
        let filename1 = EncryptionService::generate_encrypted_filename();
        let filename2 = EncryptionService::generate_encrypted_filename();

        // 验证格式：UUID.dat
        assert!(filename1.ends_with(ENCRYPTED_FILE_EXTENSION));
        assert!(filename1.ends_with(".dat"));

        // 验证唯一性
        assert_ne!(filename1, filename2);

        // 验证是加密文件名
        assert!(EncryptionService::is_encrypted_filename(&filename1));
        assert!(EncryptionService::is_encrypted_filename(&filename2));
    }

    #[test]
    fn test_extract_uuid_from_encrypted_name() {
        let filename = "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat";
        let uuid = EncryptionService::extract_uuid_from_encrypted_name(filename);

        assert_eq!(uuid, Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890"));

        // 测试无效文件名
        assert!(EncryptionService::extract_uuid_from_encrypted_name("invalid.txt").is_none());
        assert!(EncryptionService::extract_uuid_from_encrypted_name("not-a-uuid.dat").is_none());
    }

    #[test]
    fn test_is_encrypted_filename() {
        // 有效的加密文件名：UUID.dat
        assert!(EncryptionService::is_encrypted_filename("a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"));
        // 无效的文件名
        assert!(!EncryptionService::is_encrypted_filename("normal_file.txt"));
        assert!(!EncryptionService::is_encrypted_filename("a1b2c3d4-e5f6-7890-abcd-ef1234567890.txt"));
        assert!(!EncryptionService::is_encrypted_filename("not-a-uuid.dat"));
    }


    #[test]
    fn test_is_encrypted_file() {
        let dir = tempdir().unwrap();

        // 创建普通文件
        let normal_file = dir.path().join("normal.txt");
        std::fs::write(&normal_file, "This is a normal file").unwrap();
        assert!(!EncryptionService::is_encrypted_file(&normal_file).unwrap());

        // 创建加密文件
        let encrypted_file = dir.path().join("encrypted.bkup");
        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);
        service.encrypt_file_chunked(&normal_file, &encrypted_file).unwrap();
        assert!(EncryptionService::is_encrypted_file(&encrypted_file).unwrap());
    }

    #[test]
    fn test_invalid_key_length() {
        let invalid_key = "dG9vX3Nob3J0"; // "too_short" in base64
        let result = EncryptionService::from_base64_key(invalid_key, EncryptionAlgorithm::Aes256Gcm);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_wrong_key() {
        let key1 = EncryptionService::generate_master_key();
        let key2 = EncryptionService::generate_master_key();

        let service1 = EncryptionService::new(key1, EncryptionAlgorithm::Aes256Gcm);
        let service2 = EncryptionService::new(key2, EncryptionAlgorithm::Aes256Gcm);

        let plaintext = b"Secret data";
        let encrypted = service1.encrypt(plaintext).unwrap();

        // 使用错误的密钥解密应该失败
        let result = service2.decrypt(&encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_file_encryption() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("empty.txt");
        let encrypted_path = dir.path().join("empty.bkup");
        let decrypted_path = dir.path().join("empty_decrypted.txt");

        // 创建空文件
        std::fs::write(&input_path, "").unwrap();

        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        // 加密空文件
        let metadata = service.encrypt_file_chunked(&input_path, &encrypted_path).unwrap();
        assert_eq!(metadata.original_size, 0);

        // 解密
        service.decrypt_file(&encrypted_path, &decrypted_path).unwrap();

        // 验证内容
        let decrypted = std::fs::read(&decrypted_path).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn test_large_data_encryption() {
        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        // 测试 1MB 数据
        let plaintext: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let encrypted = service.encrypt(&plaintext).unwrap();
        let decrypted = service.decrypt(&encrypted).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    // ==================== StreamingEncryptionService 测试 ====================

    #[tokio::test]
    async fn test_streaming_encrypt_decrypt_aes256gcm() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("streaming_test.txt");
        let encrypted_path = dir.path().join("streaming_test.bkup");
        let decrypted_path = dir.path().join("streaming_test_decrypted.txt");

        // 创建测试文件
        let test_data = "Hello, World! This is a streaming encryption test.";
        std::fs::write(&input_path, test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = StreamingEncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        // 流式加密
        let metadata = service.encrypt_file_streaming(&input_path, &encrypted_path).await.unwrap();
        assert!(metadata.encrypted_size > 0);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.algorithm, EncryptionAlgorithm::Aes256Gcm);

        // 验证是加密文件
        assert!(EncryptionService::is_encrypted_file(&encrypted_path).unwrap());

        // 流式解密
        let original_size = service.decrypt_file_streaming(&encrypted_path, &decrypted_path).await.unwrap();
        assert_eq!(original_size, test_data.len() as u64);

        // 验证内容
        let decrypted = std::fs::read_to_string(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
    }

    #[tokio::test]
    async fn test_streaming_encrypt_decrypt_chacha20() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("streaming_chacha_test.txt");
        let encrypted_path = dir.path().join("streaming_chacha_test.bkup");
        let decrypted_path = dir.path().join("streaming_chacha_test_decrypted.txt");

        // 创建测试文件
        let test_data = "ChaCha20-Poly1305 streaming encryption test data.";
        std::fs::write(&input_path, test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = StreamingEncryptionService::new(key, EncryptionAlgorithm::ChaCha20Poly1305);

        // 流式加密
        let metadata = service.encrypt_file_streaming(&input_path, &encrypted_path).await.unwrap();
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);

        // 流式解密
        service.decrypt_file_streaming(&encrypted_path, &decrypted_path).await.unwrap();

        // 验证内容
        let decrypted = std::fs::read_to_string(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
    }

    #[tokio::test]
    async fn test_streaming_large_file_chunked() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("streaming_large.bin");
        let encrypted_path = dir.path().join("streaming_large.bkup");
        let decrypted_path = dir.path().join("streaming_large_decrypted.bin");

        // 创建一个大于分块大小的测试文件（使用小分块测试多块场景）
        // 使用 1MB 分块，创建 2.5MB 文件
        let chunk_size = 1024 * 1024; // 1MB
        let test_data: Vec<u8> = (0..(chunk_size * 2 + chunk_size / 2)).map(|i| (i % 256) as u8).collect();
        std::fs::write(&input_path, &test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = StreamingEncryptionService::with_chunk_size(key, EncryptionAlgorithm::Aes256Gcm, chunk_size);

        assert_eq!(service.chunk_size(), chunk_size);

        // 流式加密
        let metadata = service.encrypt_file_streaming(&input_path, &encrypted_path).await.unwrap();
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.original_size, test_data.len() as u64);

        // 流式解密
        let original_size = service.decrypt_file_streaming(&encrypted_path, &decrypted_path).await.unwrap();
        assert_eq!(original_size, test_data.len() as u64);

        // 验证内容
        let decrypted = std::fs::read(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
    }

    #[tokio::test]
    async fn test_streaming_cross_compatible_with_sync() {
        // 测试流式加密的文件可以被同步解密器解密
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("cross_compat.txt");
        let encrypted_path = dir.path().join("cross_compat.bkup");
        let decrypted_path = dir.path().join("cross_compat_decrypted.txt");

        let test_data = "Cross compatibility test between streaming and sync encryption.";
        std::fs::write(&input_path, test_data).unwrap();

        let key = EncryptionService::generate_master_key();

        // 使用流式服务加密
        let streaming_service = StreamingEncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);
        streaming_service.encrypt_file_streaming(&input_path, &encrypted_path).await.unwrap();

        // 使用同步服务解密
        let sync_service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);
        sync_service.decrypt_file(&encrypted_path, &decrypted_path).unwrap();

        // 验证内容
        let decrypted = std::fs::read_to_string(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
    }

    #[tokio::test]
    async fn test_streaming_from_base64_key() {
        let key = EncryptionService::generate_master_key();
        let key_base64 = BASE64.encode(key);

        let service = StreamingEncryptionService::from_base64_key(&key_base64, EncryptionAlgorithm::Aes256Gcm).unwrap();

        let dir = tempdir().unwrap();
        let input_path = dir.path().join("base64_key_test.txt");
        let encrypted_path = dir.path().join("base64_key_test.bkup");
        let decrypted_path = dir.path().join("base64_key_test_decrypted.txt");

        std::fs::write(&input_path, "Test with base64 key").unwrap();

        service.encrypt_file_streaming(&input_path, &encrypted_path).await.unwrap();
        service.decrypt_file_streaming(&encrypted_path, &decrypted_path).await.unwrap();

        let decrypted = std::fs::read_to_string(&decrypted_path).unwrap();
        assert_eq!("Test with base64 key", decrypted);
    }

    #[tokio::test]
    async fn test_streaming_invalid_format_error() {
        // 测试无效格式文件解密应该失败
        let dir = tempdir().unwrap();
        let invalid_file = dir.path().join("invalid.bkup");

        // 创建一个无效格式的文件（非加密文件）
        std::fs::write(&invalid_file, "This is not an encrypted file").unwrap();

        let key = EncryptionService::generate_master_key();
        let streaming_service = StreamingEncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);
        let result = streaming_service.decrypt_file_streaming(&invalid_file, &dir.path().join("output.txt")).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid encrypted file format"));
    }

    #[tokio::test]
    async fn test_streaming_derive_chunk_nonce_uniqueness() {
        let master_nonce = [1u8; 12];

        // 生成多个块 Nonce
        let nonce_0 = StreamingEncryptionService::derive_chunk_nonce(&master_nonce, 0);
        let nonce_1 = StreamingEncryptionService::derive_chunk_nonce(&master_nonce, 1);
        let nonce_2 = StreamingEncryptionService::derive_chunk_nonce(&master_nonce, 2);

        // 验证所有 Nonce 都不同
        assert_ne!(nonce_0, nonce_1);
        assert_ne!(nonce_1, nonce_2);
        assert_ne!(nonce_0, nonce_2);

        // 验证与 EncryptionService 的派生结果一致
        let sync_nonce_1 = EncryptionService::derive_chunk_nonce(&master_nonce, 1);
        assert_eq!(nonce_1, sync_nonce_1);
    }

    // ==================== 进度回调测试 ====================

    #[test]
    fn test_encrypt_file_with_progress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("progress_test.bin");
        let encrypted_path = dir.path().join("progress_test.bkup");

        // 创建一个大于分块大小的测试文件（使用 20MB 测试分块）
        let test_data: Vec<u8> = (0..20 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        std::fs::write(&input_path, &test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        let progress_calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let metadata = service
            .encrypt_file_chunked_with_progress(
                &input_path,
                &encrypted_path,
                move |processed, total| {
                    progress_calls_clone.lock().unwrap().push((processed, total));
                },
            )
            .unwrap();

        let calls = progress_calls.lock().unwrap();

        // 验证进度回调被调用
        assert!(!calls.is_empty());

        // 验证最后一次回调是 100%
        let last = calls.last().unwrap();
        assert_eq!(last.0, last.1);

        // 验证进度单调递增
        for i in 1..calls.len() {
            assert!(calls[i].0 >= calls[i - 1].0);
        }

        // 验证元数据
        assert_eq!(metadata.original_size, test_data.len() as u64);
        // 统一使用 v1 格式
        assert_eq!(metadata.version, 1);
    }

    #[test]
    fn test_decrypt_file_with_progress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("decrypt_progress_test.bin");
        let encrypted_path = dir.path().join("decrypt_progress_test.bkup");
        let decrypted_path = dir.path().join("decrypt_progress_test_decrypted.bin");

        // 创建并加密测试文件
        let test_data: Vec<u8> = (0..20 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        std::fs::write(&input_path, &test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        // 加密
        service.encrypt_file_chunked(&input_path, &encrypted_path).unwrap();

        // 解密（带进度回调）
        let progress_calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let original_size = service
            .decrypt_file_with_progress(
                &encrypted_path,
                &decrypted_path,
                move |processed, total| {
                    progress_calls_clone.lock().unwrap().push((processed, total));
                },
            )
            .unwrap();

        let calls = progress_calls.lock().unwrap();

        // 验证进度回调被调用
        assert!(!calls.is_empty());

        // 验证最后一次回调是 100%
        let last = calls.last().unwrap();
        assert_eq!(last.0, last.1);

        // 验证解密后内容正确
        let decrypted = std::fs::read(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
        assert_eq!(original_size, test_data.len() as u64);
    }

    #[test]
    fn test_small_file_encrypt_with_progress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("small_progress_test.txt");
        let encrypted_path = dir.path().join("small_progress_test.bkup");

        // 创建小文件（小于 10MB 阈值）
        std::fs::write(&input_path, "Hello, World!").unwrap();

        let key = EncryptionService::generate_master_key();
        let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

        let progress_calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let metadata = service
            .encrypt_file_chunked_with_progress(
                &input_path,
                &encrypted_path,
                move |processed, total| {
                    progress_calls_clone.lock().unwrap().push((processed, total));
                },
            )
            .unwrap();

        let calls = progress_calls.lock().unwrap();

        // 小文件应该有 2 次回调（0% 和 100%）
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], (0, 13)); // 0%
        assert_eq!(calls[1], (13, 13)); // 100%

        // 验证使用 v1 格式
        assert_eq!(metadata.version, 1);
    }

    #[tokio::test]
    async fn test_streaming_encrypt_with_progress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("streaming_progress_test.bin");
        let encrypted_path = dir.path().join("streaming_progress_test.bkup");

        // 创建测试文件
        let chunk_size = 1024 * 1024; // 1MB
        let test_data: Vec<u8> = (0..(chunk_size * 2 + chunk_size / 2)).map(|i| (i % 256) as u8).collect();
        std::fs::write(&input_path, &test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = StreamingEncryptionService::with_chunk_size(key, EncryptionAlgorithm::Aes256Gcm, chunk_size);

        let progress_calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let metadata = service
            .encrypt_file_streaming_with_progress(
                &input_path,
                &encrypted_path,
                move |processed, total| {
                    progress_calls_clone.lock().unwrap().push((processed, total));
                },
            )
            .await
            .unwrap();

        let calls = progress_calls.lock().unwrap();

        // 验证进度回调被调用（初始 + 每个分块）
        assert!(calls.len() >= 3); // 至少 3 个分块 + 初始回调

        // 验证最后一次回调是 100%
        let last = calls.last().unwrap();
        assert_eq!(last.0, last.1);

        // 验证元数据
        assert_eq!(metadata.original_size, test_data.len() as u64);
        assert_eq!(metadata.version, 1);
    }

    #[tokio::test]
    async fn test_streaming_decrypt_with_progress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("streaming_decrypt_progress_test.bin");
        let encrypted_path = dir.path().join("streaming_decrypt_progress_test.bkup");
        let decrypted_path = dir.path().join("streaming_decrypt_progress_test_decrypted.bin");

        // 创建并加密测试文件
        let chunk_size = 1024 * 1024; // 1MB
        let test_data: Vec<u8> = (0..(chunk_size * 2 + chunk_size / 2)).map(|i| (i % 256) as u8).collect();
        std::fs::write(&input_path, &test_data).unwrap();

        let key = EncryptionService::generate_master_key();
        let service = StreamingEncryptionService::with_chunk_size(key, EncryptionAlgorithm::Aes256Gcm, chunk_size);

        // 加密
        service.encrypt_file_streaming(&input_path, &encrypted_path).await.unwrap();

        // 解密（带进度回调）
        let progress_calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let original_size = service
            .decrypt_file_streaming_with_progress(
                &encrypted_path,
                &decrypted_path,
                move |processed, total| {
                    progress_calls_clone.lock().unwrap().push((processed, total));
                },
            )
            .await
            .unwrap();

        let calls = progress_calls.lock().unwrap();

        // 验证进度回调被调用
        assert!(calls.len() >= 3);

        // 验证最后一次回调是 100%
        let last = calls.last().unwrap();
        assert_eq!(last.0, last.1);

        // 验证解密后内容正确
        let decrypted = std::fs::read(&decrypted_path).unwrap();
        assert_eq!(test_data, decrypted);
        assert_eq!(original_size, test_data.len() as u64);
    }
}

/// Property-based tests for encryption service
///
/// These tests verify correctness properties that should hold across all valid inputs.
#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;

    // 统一使用 v1 格式，所有文件都使用分块加密
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn prop_encryption_always_produces_v1_format(
            // Test with file sizes from 1KB to 5MB
            file_size_kb in 1u32..5_000u32
        ) {
            let file_size = file_size_kb as u64 * 1024;

            let dir = tempdir().unwrap();
            let input_path = dir.path().join("test_input.bin");
            let encrypted_path = dir.path().join("test_encrypted.bkup");

            // Create test file
            let test_data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
            std::fs::write(&input_path, &test_data).unwrap();

            let key = EncryptionService::generate_master_key();
            let service = EncryptionService::new(key, EncryptionAlgorithm::Aes256Gcm);

            // Encrypt the file
            let metadata = service.encrypt_file_chunked(&input_path, &encrypted_path).unwrap();

            // All files should use v1 format
            prop_assert_eq!(
                metadata.version,
                1,
                "File of size {} bytes should be encrypted with version 1, but got version {}",
                file_size, metadata.version
            );

            // Verify round-trip
            let decrypted_path = dir.path().join("test_decrypted.bin");
            let decrypted_size = service.decrypt_file(&encrypted_path, &decrypted_path).unwrap();

            prop_assert_eq!(decrypted_size, file_size);

            let decrypted_data = std::fs::read(&decrypted_path).unwrap();
            prop_assert_eq!(decrypted_data, test_data);
        }
    }
}
