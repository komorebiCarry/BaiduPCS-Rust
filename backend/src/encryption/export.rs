//! 加密数据导出模块
//!
//! 实现解密数据包导出功能，用于 decrypt-cli 工具解密加密文件。
//!
//! 导出内容包括：
//! - encryption.json: 密钥配置（current_key + key_history）
//! - mapping.json: 加密文件映射表

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Write};
use std::sync::Arc;
use zip::write::FileOptions;
use zip::ZipWriter;

use crate::autobackup::record::BackupRecordManager;
use crate::encryption::config_store::{EncryptionConfigStore, EncryptionKeyConfig};

/// 映射记录（用于导出）
///
/// 包含解密所需的所有必须字段
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

/// 映射生成器
///
/// 从数据库生成映射 JSON，用于导出解密数据包
pub struct MappingGenerator {
    record_manager: Arc<BackupRecordManager>,
}

impl MappingGenerator {
    /// 创建新的映射生成器
    pub fn new(record_manager: Arc<BackupRecordManager>) -> Self {
        Self { record_manager }
    }

    /// 生成所有映射记录
    ///
    /// 从 encryption_snapshots 表查询所有已完成的加密映射记录
    pub fn generate_mapping(&self) -> Result<MappingExport> {
        let conn = self.record_manager.get_conn_for_export()?;

        let mut stmt = conn.prepare(
            "SELECT config_id, encrypted_name, original_path, original_name, 
                    is_directory, version, key_version, file_size, nonce, 
                    algorithm, remote_path, status
             FROM encryption_snapshots
             WHERE status = 'completed'
             ORDER BY config_id, original_path, original_name",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(MappingRecord {
                config_id: row.get(0)?,
                encrypted_name: row.get(1)?,
                original_path: row.get(2)?,
                original_name: row.get(3)?,
                is_directory: row.get::<_, i32>(4)? == 1,
                version: row.get(5)?,
                key_version: row.get::<_, i64>(6)? as u32,
                file_size: row.get::<_, i64>(7)? as u64,
                nonce: row.get(8)?,
                algorithm: row.get(9)?,
                remote_path: row.get::<_, Option<String>>(10)?,
                status: row.get::<_, Option<String>>(11)?,
            })
        })?;

        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }

        Ok(MappingExport {
            version: "1.0".to_string(),
            exported_at: chrono::Utc::now().timestamp_millis(),
            records,
        })
    }

    /// 生成指定配置的映射记录
    pub fn generate_mapping_by_config(&self, config_id: &str) -> Result<MappingExport> {
        let conn = self.record_manager.get_conn_for_export()?;

        let mut stmt = conn.prepare(
            "SELECT config_id, encrypted_name, original_path, original_name, 
                    is_directory, version, key_version, file_size, nonce, 
                    algorithm, remote_path, status
             FROM encryption_snapshots
             WHERE config_id = ?1 AND status = 'completed'
             ORDER BY original_path, original_name",
        )?;

        let rows = stmt.query_map([config_id], |row| {
            Ok(MappingRecord {
                config_id: row.get(0)?,
                encrypted_name: row.get(1)?,
                original_path: row.get(2)?,
                original_name: row.get(3)?,
                is_directory: row.get::<_, i32>(4)? == 1,
                version: row.get(5)?,
                key_version: row.get::<_, i64>(6)? as u32,
                file_size: row.get::<_, i64>(7)? as u64,
                nonce: row.get(8)?,
                algorithm: row.get(9)?,
                remote_path: row.get::<_, Option<String>>(10)?,
                status: row.get::<_, Option<String>>(11)?,
            })
        })?;

        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }

        Ok(MappingExport {
            version: "1.0".to_string(),
            exported_at: chrono::Utc::now().timestamp_millis(),
            records,
        })
    }
}

/// 解密数据包导出器
///
/// 将密钥配置和映射数据打包为 ZIP 文件
pub struct DecryptBundleExporter {
    config_store: Arc<EncryptionConfigStore>,
    mapping_generator: MappingGenerator,
}

impl DecryptBundleExporter {
    /// 创建新的导出器
    pub fn new(
        config_store: Arc<EncryptionConfigStore>,
        record_manager: Arc<BackupRecordManager>,
    ) -> Self {
        Self {
            config_store,
            mapping_generator: MappingGenerator::new(record_manager),
        }
    }

    /// 导出密钥配置
    pub fn export_keys(&self) -> Result<EncryptionKeyConfig> {
        self.config_store
            .load()?
            .ok_or_else(|| anyhow!("没有密钥配置"))
    }

    /// 导出映射数据
    pub fn export_mapping(&self) -> Result<MappingExport> {
        self.mapping_generator.generate_mapping()
    }

    /// 导出完整的解密数据包（ZIP 格式）
    ///
    /// 返回 ZIP 文件的字节数据
    pub fn export_bundle(&self) -> Result<Vec<u8>> {
        // 获取密钥配置
        let key_config = self.export_keys()?;
        let key_json = serde_json::to_string_pretty(&key_config)?;

        // 获取映射数据
        let mapping = self.export_mapping()?;
        let mapping_json = serde_json::to_string_pretty(&mapping)?;

        // 创建 ZIP 文件
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut zip = ZipWriter::new(&mut buffer);
            let options = FileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated)
                .unix_permissions(0o644);

            // 添加 encryption.json
            zip.start_file("encryption.json", options)?;
            zip.write_all(key_json.as_bytes())?;

            // 添加 mapping.json
            zip.start_file("mapping.json", options)?;
            zip.write_all(mapping_json.as_bytes())?;

            zip.finish()?;
        }

        Ok(buffer.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autobackup::record::EncryptionSnapshot;
    use tempfile::tempdir;

    fn create_test_record_manager() -> Arc<BackupRecordManager> {
        let dir = tempdir().unwrap();
        let db_path = dir.keep().join("test_backup.db");
        Arc::new(BackupRecordManager::new(&db_path).unwrap())
    }

    #[test]
    fn test_mapping_generator_empty() {
        let record_manager = create_test_record_manager();
        let generator = MappingGenerator::new(record_manager);

        let mapping = generator.generate_mapping().unwrap();
        assert_eq!(mapping.version, "1.0");
        assert!(mapping.records.is_empty());
    }

    #[test]
    fn test_mapping_generator_with_records() {
        let record_manager = create_test_record_manager();

        // 添加测试快照
        let snapshot = EncryptionSnapshot {
            config_id: "test-config".to_string(),
            original_path: "/documents".to_string(),
            original_name: "test.txt".to_string(),
            encrypted_name: "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat".to_string(),
            file_size: 1024,
            nonce: "dGVzdG5vbmNl".to_string(),
            algorithm: "aes256gcm".to_string(),
            version: 1,
            key_version: 1,
            remote_path: "/backup/documents".to_string(),
            is_directory: false,
            status: "completed".to_string(),
        };
        record_manager.add_snapshot(&snapshot).unwrap();

        let generator = MappingGenerator::new(record_manager);
        let mapping = generator.generate_mapping().unwrap();

        assert_eq!(mapping.records.len(), 1);
        let record = &mapping.records[0];
        assert_eq!(record.config_id, "test-config");
        assert_eq!(record.original_name, "test.txt");
        assert_eq!(
            record.encrypted_name,
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"
        );
        assert_eq!(record.key_version, 1);
        assert!(!record.is_directory);
    }

    #[test]
    fn test_mapping_record_serialization() {
        let record = MappingRecord {
            config_id: "config-1".to_string(),
            encrypted_name: "uuid.dat".to_string(),
            original_path: "/path".to_string(),
            original_name: "file.txt".to_string(),
            is_directory: false,
            version: 1,
            key_version: 1,
            file_size: 1024,
            nonce: "base64nonce".to_string(),
            algorithm: "aes256gcm".to_string(),
            remote_path: Some("/remote/path".to_string()),
            status: Some("completed".to_string()),
        };

        let json = serde_json::to_string(&record).unwrap();
        let deserialized: MappingRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.config_id, record.config_id);
        assert_eq!(deserialized.encrypted_name, record.encrypted_name);
        assert_eq!(deserialized.key_version, record.key_version);
    }
}
