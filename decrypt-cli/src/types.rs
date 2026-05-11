//! 核心数据类型定义
//!
//! 定义 decrypt-cli 使用的所有数据结构，与后端保持一致。

use serde::{Deserialize, Serialize};
use std::fmt;
use std::process::ExitCode as StdExitCode;
use thiserror::Error;

// ============================================================================
// 加密算法枚举
// ============================================================================

/// 支持的加密算法
///
/// 与后端 `EncryptionAlgorithm` 保持一致
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM（默认，推荐）
    /// 算法标识：0
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305（备选）
    /// 算法标识：1
    ChaCha20Poly1305,
}

impl EncryptionAlgorithm {
}

impl fmt::Display for EncryptionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncryptionAlgorithm::Aes256Gcm => write!(f, "aes-256-gcm"),
            EncryptionAlgorithm::ChaCha20Poly1305 => write!(f, "chacha20-poly1305"),
        }
    }
}

// ============================================================================
// 密钥配置结构
// ============================================================================

/// 单个密钥信息
///
/// 与后端 `EncryptionKeyInfo` 保持一致
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKeyInfo {
    /// 主密钥（Base64 编码的 32 字节）
    pub master_key: String,
    /// 加密算法
    #[serde(default)]
    pub algorithm: EncryptionAlgorithm,
    /// 密钥版本（用于密钥轮换）
    #[serde(default = "default_key_version")]
    pub key_version: u32,
    /// 密钥创建时间（Unix 时间戳，毫秒）
    pub created_at: i64,
    /// 密钥最后使用时间（Unix 时间戳，毫秒）
    pub last_used_at: Option<i64>,
    /// 密钥废弃时间（Unix 时间戳，毫秒，仅历史密钥有此字段）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_at: Option<i64>,
}

fn default_key_version() -> u32 {
    1
}

impl EncryptionKeyInfo {
    /// 检查密钥是否有效（非空）
    pub fn is_valid(&self) -> bool {
        !self.master_key.is_empty() && self.key_version > 0
    }
}

/// 加密密钥配置（存储在 encryption.json）
///
/// 与后端 `EncryptionKeyConfig` 保持一致
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// 当前使用的密钥
    #[serde(rename = "current_key")]
    pub current: EncryptionKeyInfo,

    /// 历史密钥（已废弃但保留用于解密旧文件）
    #[serde(rename = "key_history", default)]
    pub history: Vec<EncryptionKeyInfo>,
}

// ============================================================================
// 映射记录结构
// ============================================================================

/// 映射记录
///
/// 包含解密所需的所有必须字段，与后端 `MappingRecord` 保持一致
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRecord {
    /// 备份配置 ID
    pub config_id: String,
    /// 加密后的文件名（UUID.dat 格式）
    pub encrypted_name: String,
    /// 原始文件相对路径
    pub original_path: String,
    /// 原始文件名
    pub original_name: String,
    /// 是否为文件夹
    pub is_directory: bool,
    /// 加密格式版本
    pub version: i32,
    /// 使用的密钥版本
    pub key_version: u32,
    /// 原始文件大小（字节）
    pub file_size: u64,
    /// 加密随机数（Base64 编码）
    pub nonce: String,
    /// 加密算法（aes256gcm 或 chacha20poly1305）
    pub algorithm: String,
    /// 网盘路径（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_path: Option<String>,
    /// 状态（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// 映射导出数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingExport {
    /// 版本号
    pub version: String,
    /// 导出时间（Unix 时间戳，毫秒）
    pub exported_at: i64,
    /// 映射记录列表
    pub records: Vec<MappingRecord>,
}

// ============================================================================
// 退出码枚举
// ============================================================================

/// CLI 退出码
///
/// 定义所有可能的退出状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExitCode {
    /// 全部成功
    Success = 0,
    /// 部分成功（批量解密时部分文件失败）
    PartialSuccess = 1,
    /// 参数错误
    ArgumentError = 2,
    /// 文件格式无效/损坏
    InvalidFormat = 3,
    /// 密钥不匹配
    KeyMismatch = 4,
    /// I/O 错误
    IoError = 5,
    /// 映射错误
    MappingError = 6,
}

impl ExitCode {
    /// 转换为标准库退出码
    pub fn to_std(self) -> StdExitCode {
        StdExitCode::from(self as u8)
    }
}

impl From<ExitCode> for StdExitCode {
    fn from(code: ExitCode) -> Self {
        StdExitCode::from(code as u8)
    }
}

impl fmt::Display for ExitCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExitCode::Success => write!(f, "成功"),
            ExitCode::PartialSuccess => write!(f, "部分成功"),
            ExitCode::ArgumentError => write!(f, "参数错误"),
            ExitCode::InvalidFormat => write!(f, "文件格式无效"),
            ExitCode::KeyMismatch => write!(f, "密钥不匹配"),
            ExitCode::IoError => write!(f, "I/O 错误"),
            ExitCode::MappingError => write!(f, "映射错误"),
        }
    }
}

// ============================================================================
// 错误类型定义
// ============================================================================

/// 解密错误类型
#[derive(Debug, Error)]
pub enum DecryptError {
    /// 参数错误
    #[error("参数错误: {0}")]
    ArgumentError(String),

    /// 文件格式无效
    #[error("文件格式无效: {0}")]
    InvalidFormat(String),

    /// 文件损坏
    #[error("文件损坏: {0}")]
    CorruptedFile(String),

    /// 密钥不匹配
    #[error("密钥不匹配: 版本 {0} 不存在")]
    KeyMismatch(u32),

    /// 密钥解码错误
    #[error("密钥解码错误: {0}")]
    KeyDecodeError(String),

    /// I/O 错误
    #[error("I/O 错误: {0}")]
    IoError(#[from] std::io::Error),

    /// 映射错误
    #[error("映射错误: {0}")]
    MappingError(String),

    /// JSON 解析错误
    #[error("JSON 解析错误: {0}")]
    JsonError(#[from] serde_json::Error),

    /// 解密失败
    #[error("解密失败: {0}")]
    DecryptionFailed(String),

    /// 映射中找不到记录
    #[error("映射中找不到记录: {0}")]
    #[allow(dead_code)]
    MappingNotFound(String),
}

impl DecryptError {
    /// 转换为退出码
    pub fn to_exit_code(&self) -> ExitCode {
        match self {
            DecryptError::ArgumentError(_) => ExitCode::ArgumentError,
            DecryptError::InvalidFormat(_) => ExitCode::InvalidFormat,
            DecryptError::CorruptedFile(_) => ExitCode::InvalidFormat,
            DecryptError::KeyMismatch(_) => ExitCode::KeyMismatch,
            DecryptError::KeyDecodeError(_) => ExitCode::KeyMismatch,
            DecryptError::IoError(_) => ExitCode::IoError,
            DecryptError::MappingError(_) => ExitCode::MappingError,
            DecryptError::JsonError(_) => ExitCode::InvalidFormat,
            DecryptError::DecryptionFailed(_) => ExitCode::InvalidFormat,
            DecryptError::MappingNotFound(_) => ExitCode::MappingError,
        }
    }
}

// ============================================================================
// 批量解密结果
// ============================================================================

/// 单个文件的解密结果
#[derive(Debug, Clone)]
pub struct FileDecryptResult {
    /// 输入文件路径
    pub input_path: String,
    /// 输出文件路径（成功时有值）
    #[allow(dead_code)]
    pub output_path: Option<String>,
    /// 是否成功
    pub success: bool,
    /// 错误信息（失败时有值）
    pub error: Option<String>,
}

/// 批量解密结果汇总
#[derive(Debug, Clone, Default)]
pub struct DecryptSummary {
    /// 成功解密的文件数
    pub success_count: usize,
    /// 跳过的文件数（无映射记录）
    pub skipped_count: usize,
    /// 失败的文件数
    pub failed_count: usize,
    /// 各文件的详细结果
    pub results: Vec<FileDecryptResult>,
}

impl DecryptSummary {
    /// 创建新的汇总
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加成功结果
    pub fn add_success(&mut self, input_path: String, output_path: String) {
        self.success_count += 1;
        self.results.push(FileDecryptResult {
            input_path,
            output_path: Some(output_path),
            success: true,
            error: None,
        });
    }

    /// 添加跳过结果
    pub fn add_skipped(&mut self, input_path: String, reason: String) {
        self.skipped_count += 1;
        self.results.push(FileDecryptResult {
            input_path,
            output_path: None,
            success: false,
            error: Some(format!("跳过: {}", reason)),
        });
    }

    /// 添加失败结果
    pub fn add_failed(&mut self, input_path: String, error: String) {
        self.failed_count += 1;
        self.results.push(FileDecryptResult {
            input_path,
            output_path: None,
            success: false,
            error: Some(error),
        });
    }

    /// 获取总文件数
    pub fn total_count(&self) -> usize {
        self.success_count + self.skipped_count + self.failed_count
    }

    /// 确定最终退出码
    #[allow(dead_code)]
    pub fn exit_code(&self) -> ExitCode {
        if self.failed_count == 0 && self.skipped_count == 0 {
            ExitCode::Success
        } else if self.success_count > 0 {
            ExitCode::PartialSuccess
        } else if self.failed_count > 0 {
            // 根据第一个失败的错误类型确定退出码
            // 这里简化处理，返回 InvalidFormat
            ExitCode::InvalidFormat
        } else {
            ExitCode::Success
        }
    }

    /// 打印汇总信息
    #[allow(dead_code)]
    pub fn print_summary(&self) {
        println!();
        println!("解密完成:");
        println!("  成功: {} 个文件", self.success_count);
        if self.skipped_count > 0 {
            println!("  跳过: {} 个文件 (无映射记录)", self.skipped_count);
        }
        if self.failed_count > 0 {
            println!("  失败: {} 个文件", self.failed_count);
            for result in &self.results {
                if !result.success && result.error.is_some() {
                    if let Some(ref error) = result.error {
                        if !error.starts_with("跳过:") {
                            println!("    - {}: {}", result.input_path, error);
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_algorithm_display() {
        assert_eq!(format!("{}", EncryptionAlgorithm::Aes256Gcm), "aes-256-gcm");
        assert_eq!(
            format!("{}", EncryptionAlgorithm::ChaCha20Poly1305),
            "chacha20-poly1305"
        );
    }

    #[test]
    fn test_encryption_algorithm_serde() {
        let algo = EncryptionAlgorithm::Aes256Gcm;
        let json = serde_json::to_string(&algo).unwrap();
        assert_eq!(json, "\"AES256-GCM\"");

        let algo2 = EncryptionAlgorithm::ChaCha20Poly1305;
        let json2 = serde_json::to_string(&algo2).unwrap();
        assert_eq!(json2, "\"CHA-CHA20-POLY1305\"");

        // 反序列化
        let parsed: EncryptionAlgorithm = serde_json::from_str("\"AES256-GCM\"").unwrap();
        assert_eq!(parsed, EncryptionAlgorithm::Aes256Gcm);
    }

    #[test]
    fn test_encryption_key_info_is_valid() {
        let valid_key = EncryptionKeyInfo {
            master_key: "dGVzdGtleQ==".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_version: 1,
            created_at: 1702454400000,
            last_used_at: None,
            deprecated_at: None,
        };
        assert!(valid_key.is_valid());

        let empty_key = EncryptionKeyInfo {
            master_key: "".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_version: 1,
            created_at: 1702454400000,
            last_used_at: None,
            deprecated_at: None,
        };
        assert!(!empty_key.is_valid());

        let zero_version_key = EncryptionKeyInfo {
            master_key: "dGVzdGtleQ==".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_version: 0,
            created_at: 1702454400000,
            last_used_at: None,
            deprecated_at: None,
        };
        assert!(!zero_version_key.is_valid());
    }

    #[test]
    fn test_encryption_config_serde() {
        let config = EncryptionConfig {
            current: EncryptionKeyInfo {
                master_key: "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleTE=".to_string(),
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                key_version: 2,
                created_at: 1702454400000,
                last_used_at: Some(1702454500000),
                deprecated_at: None,
            },
            history: vec![EncryptionKeyInfo {
                master_key: "b2xka2V5b2xka2V5b2xka2V5b2xka2V5MQ==".to_string(),
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                key_version: 1,
                created_at: 1700000000000,
                last_used_at: Some(1702454399000),
                deprecated_at: Some(1702454400000),
            }],
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: EncryptionConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.current.key_version, 2);
        assert_eq!(parsed.history.len(), 1);
        assert_eq!(parsed.history[0].key_version, 1);
    }

    #[test]
    fn test_mapping_record_serde() {
        let record = MappingRecord {
            config_id: "config-1".to_string(),
            encrypted_name: "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat".to_string(),
            original_path: "/documents/work".to_string(),
            original_name: "report.pdf".to_string(),
            is_directory: false,
            version: 1,
            key_version: 1,
            file_size: 1024000,
            nonce: "dGVzdG5vbmNl".to_string(),
            algorithm: "aes256gcm".to_string(),
            remote_path: Some("/backup/documents/work".to_string()),
            status: Some("completed".to_string()),
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: MappingRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.config_id, "config-1");
        assert_eq!(
            parsed.encrypted_name,
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"
        );
        assert_eq!(parsed.key_version, 1);
        assert!(!parsed.is_directory);
    }

    #[test]
    fn test_mapping_record_optional_fields() {
        // 测试可选字段为 None 时的序列化
        let record = MappingRecord {
            config_id: "config-1".to_string(),
            encrypted_name: "uuid.dat".to_string(),
            original_path: "/path".to_string(),
            original_name: "file.txt".to_string(),
            is_directory: false,
            version: 1,
            key_version: 1,
            file_size: 1024,
            nonce: "nonce".to_string(),
            algorithm: "aes256gcm".to_string(),
            remote_path: None,
            status: None,
        };

        let json = serde_json::to_string(&record).unwrap();
        // 可选字段为 None 时不应该出现在 JSON 中
        assert!(!json.contains("remote_path"));
        assert!(!json.contains("status"));
    }

    #[test]
    fn test_file_header_validate_magic() {
        let valid_header = FileHeader {
            magic: FILE_MAGIC,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            master_nonce: [0u8; 12],
            original_size: 1024,
            total_chunks: 1,
        };
        assert!(valid_header.validate_magic());

        let invalid_header = FileHeader {
            magic: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            master_nonce: [0u8; 12],
            original_size: 1024,
            total_chunks: 1,
        };
        assert!(!invalid_header.validate_magic());
    }

    #[test]
    fn test_exit_code_values() {
        assert_eq!(ExitCode::Success as u8, 0);
        assert_eq!(ExitCode::PartialSuccess as u8, 1);
        assert_eq!(ExitCode::ArgumentError as u8, 2);
        assert_eq!(ExitCode::InvalidFormat as u8, 3);
        assert_eq!(ExitCode::KeyMismatch as u8, 4);
        assert_eq!(ExitCode::IoError as u8, 5);
        assert_eq!(ExitCode::MappingError as u8, 6);
    }

    #[test]
    fn test_exit_code_display() {
        assert_eq!(format!("{}", ExitCode::Success), "成功");
        assert_eq!(format!("{}", ExitCode::PartialSuccess), "部分成功");
        assert_eq!(format!("{}", ExitCode::ArgumentError), "参数错误");
        assert_eq!(format!("{}", ExitCode::InvalidFormat), "文件格式无效");
        assert_eq!(format!("{}", ExitCode::KeyMismatch), "密钥不匹配");
        assert_eq!(format!("{}", ExitCode::IoError), "I/O 错误");
        assert_eq!(format!("{}", ExitCode::MappingError), "映射错误");
    }

    #[test]
    fn test_decrypt_error_to_exit_code() {
        assert_eq!(
            DecryptError::ArgumentError("test".to_string()).to_exit_code(),
            ExitCode::ArgumentError
        );
        assert_eq!(
            DecryptError::InvalidFormat("test".to_string()).to_exit_code(),
            ExitCode::InvalidFormat
        );
        assert_eq!(
            DecryptError::CorruptedFile("test".to_string()).to_exit_code(),
            ExitCode::InvalidFormat
        );
        assert_eq!(
            DecryptError::KeyMismatch(1).to_exit_code(),
            ExitCode::KeyMismatch
        );
        assert_eq!(
            DecryptError::KeyDecodeError("test".to_string()).to_exit_code(),
            ExitCode::KeyMismatch
        );
        assert_eq!(
            DecryptError::MappingError("test".to_string()).to_exit_code(),
            ExitCode::MappingError
        );
        assert_eq!(
            DecryptError::MappingNotFound("test".to_string()).to_exit_code(),
            ExitCode::MappingError
        );
    }

    #[test]
    fn test_decrypt_summary() {
        let mut summary = DecryptSummary::new();

        summary.add_success("file1.dat".to_string(), "file1.txt".to_string());
        summary.add_success("file2.dat".to_string(), "file2.txt".to_string());
        summary.add_skipped("file3.dat".to_string(), "无映射记录".to_string());
        summary.add_failed("file4.dat".to_string(), "密钥不匹配".to_string());

        assert_eq!(summary.success_count, 2);
        assert_eq!(summary.skipped_count, 1);
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.total_count(), 4);
        assert_eq!(summary.exit_code(), ExitCode::PartialSuccess);
    }

    #[test]
    fn test_decrypt_summary_all_success() {
        let mut summary = DecryptSummary::new();
        summary.add_success("file1.dat".to_string(), "file1.txt".to_string());
        summary.add_success("file2.dat".to_string(), "file2.txt".to_string());

        assert_eq!(summary.exit_code(), ExitCode::Success);
    }

    #[test]
    fn test_decrypt_summary_all_failed() {
        let mut summary = DecryptSummary::new();
        summary.add_failed("file1.dat".to_string(), "错误1".to_string());
        summary.add_failed("file2.dat".to_string(), "错误2".to_string());

        assert_eq!(summary.exit_code(), ExitCode::InvalidFormat);
    }

    #[test]
    fn test_file_magic_constant() {
        assert_eq!(FILE_MAGIC, [0xA3, 0x7F, 0x2C, 0x91, 0xE4, 0x5B]);
    }

    #[test]
    fn test_file_header_size_constant() {
        // magic[6] + algo[1] + master_nonce[12] + original_size[8] + total_chunks[4] = 31
        assert_eq!(FILE_HEADER_SIZE, 31);
    }
}
