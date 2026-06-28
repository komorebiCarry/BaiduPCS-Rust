//! 映射加载器模块
//!
//! 负责从 JSON 文件加载映射数据，并提供按加密文件名查找映射记录的功能。

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::types::{DecryptError, MappingExport, MappingRecord};

/// 映射加载器
///
/// 从 mapping.json 文件加载映射数据，提供按加密文件名查找映射记录的功能。
/// 内部使用 HashMap 索引加速查找。
#[derive(Debug, Clone)]
pub struct MappingLoader {
    /// 映射导出数据
    export: MappingExport,
    /// 按 encrypted_name 索引的映射记录
    index: HashMap<String, usize>,
}

impl MappingLoader {
    /// 从文件加载映射数据
    ///
    /// # Arguments
    /// * `path` - mapping.json 文件路径
    ///
    /// # Returns
    /// * `Ok(MappingLoader)` - 成功加载的映射加载器
    /// * `Err(DecryptError)` - 加载失败的错误
    ///
    /// # Example
    /// ```ignore
    /// let loader = MappingLoader::load(Path::new("mapping.json"))?;
    /// ```
    pub fn load(path: &Path) -> Result<Self, DecryptError> {
        let file = File::open(path).map_err(|e| {
            DecryptError::IoError(std::io::Error::new(
                e.kind(),
                format!("无法打开映射文件 '{}': {}", path.display(), e),
            ))
        })?;

        let reader = BufReader::new(file);
        let export: MappingExport = serde_json::from_reader(reader).map_err(|e| {
            DecryptError::MappingError(format!(
                "映射文件 '{}' 格式无效: {}",
                path.display(),
                e
            ))
        })?;

        Ok(Self::from_export(export))
    }

    /// 从 MappingExport 直接创建（用于测试）
    pub fn from_export(export: MappingExport) -> Self {
        // 构建索引
        let index = export
            .records
            .iter()
            .enumerate()
            .map(|(i, record)| (record.encrypted_name.clone(), i))
            .collect();

        Self { export, index }
    }

    /// 根据加密文件名查找映射记录
    ///
    /// # Arguments
    /// * `encrypted_name` - 加密后的文件名（如 "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"）
    ///
    /// # Returns
    /// * `Some(&MappingRecord)` - 找到的映射记录
    /// * `None` - 未找到对应的映射记录
    pub fn find_by_encrypted_name(&self, encrypted_name: &str) -> Option<&MappingRecord> {
        self.index
            .get(encrypted_name)
            .map(|&i| &self.export.records[i])
    }

    /// 获取所有映射记录
    #[allow(dead_code)]
    pub fn records(&self) -> &[MappingRecord] {
        &self.export.records
    }

    /// 获取映射记录数量
    pub fn len(&self) -> usize {
        self.export.records.len()
    }

    /// 检查是否为空
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.export.records.is_empty()
    }

    /// 获取导出版本
    #[allow(dead_code)]
    pub fn version(&self) -> &str {
        &self.export.version
    }

    /// 获取导出时间
    #[allow(dead_code)]
    pub fn exported_at(&self) -> i64 {
        self.export.exported_at
    }

    /// 获取原始导出数据的引用
    #[allow(dead_code)]
    pub fn export(&self) -> &MappingExport {
        &self.export
    }

    /// 获取所有加密文件名
    #[allow(dead_code)]
    pub fn encrypted_names(&self) -> impl Iterator<Item = &str> {
        self.export.records.iter().map(|r| r.encrypted_name.as_str())
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// 创建测试用的映射记录
    fn create_test_record(encrypted_name: &str, original_name: &str) -> MappingRecord {
        MappingRecord {
            config_id: "config-1".to_string(),
            encrypted_name: encrypted_name.to_string(),
            original_path: "/documents".to_string(),
            original_name: original_name.to_string(),
            is_directory: false,
            version: 1,
            key_version: 1,
            file_size: 1024,
            remote_path: None,
            status: None,
        }
    }

    /// 创建测试用的映射导出数据
    fn create_test_export() -> MappingExport {
        MappingExport {
            version: "1.0".to_string(),
            exported_at: 1702454400000,
            records: vec![
                create_test_record("a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat", "file1.txt"),
                create_test_record("b2c3d4e5-f6a7-8901-bcde-f12345678901.dat", "file2.pdf"),
                create_test_record("c3d4e5f6-a7b8-9012-cdef-123456789012.dat", "file3.doc"),
            ],
        }
    }

    #[test]
    fn test_load_from_file() {
        let export = create_test_export();
        let json = serde_json::to_string_pretty(&export).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();

        let loader = MappingLoader::load(temp_file.path()).unwrap();
        assert_eq!(loader.len(), 3);
        assert_eq!(loader.version(), "1.0");
    }

    #[test]
    fn test_load_file_not_found() {
        let result = MappingLoader::load(Path::new("nonexistent.json"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::IoError(_)));
    }

    #[test]
    fn test_load_invalid_json() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"invalid json").unwrap();

        let result = MappingLoader::load(temp_file.path());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::MappingError(_)));
    }

    #[test]
    fn test_find_by_encrypted_name_found() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        let record = loader.find_by_encrypted_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat");
        assert!(record.is_some());
        assert_eq!(record.unwrap().original_name, "file1.txt");
    }

    #[test]
    fn test_find_by_encrypted_name_not_found() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        let record = loader.find_by_encrypted_name("nonexistent.dat");
        assert!(record.is_none());
    }

    #[test]
    fn test_find_by_encrypted_name_all_records() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        // 测试所有记录都能被找到
        let record1 = loader.find_by_encrypted_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat");
        assert!(record1.is_some());
        assert_eq!(record1.unwrap().original_name, "file1.txt");

        let record2 = loader.find_by_encrypted_name("b2c3d4e5-f6a7-8901-bcde-f12345678901.dat");
        assert!(record2.is_some());
        assert_eq!(record2.unwrap().original_name, "file2.pdf");

        let record3 = loader.find_by_encrypted_name("c3d4e5f6-a7b8-9012-cdef-123456789012.dat");
        assert!(record3.is_some());
        assert_eq!(record3.unwrap().original_name, "file3.doc");
    }

    #[test]
    fn test_records() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        let records = loader.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].original_name, "file1.txt");
        assert_eq!(records[1].original_name, "file2.pdf");
        assert_eq!(records[2].original_name, "file3.doc");
    }

    #[test]
    fn test_len() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        assert_eq!(loader.len(), 3);
    }

    #[test]
    fn test_is_empty() {
        let empty_export = MappingExport {
            version: "1.0".to_string(),
            exported_at: 1702454400000,
            records: vec![],
        };
        let empty_loader = MappingLoader::from_export(empty_export);
        assert!(empty_loader.is_empty());

        let export = create_test_export();
        let loader = MappingLoader::from_export(export);
        assert!(!loader.is_empty());
    }

    #[test]
    fn test_version() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        assert_eq!(loader.version(), "1.0");
    }

    #[test]
    fn test_exported_at() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        assert_eq!(loader.exported_at(), 1702454400000);
    }

    #[test]
    fn test_export_accessor() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export.clone());

        assert_eq!(loader.export().version, export.version);
        assert_eq!(loader.export().records.len(), export.records.len());
    }

    #[test]
    fn test_encrypted_names() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        let names: Vec<&str> = loader.encrypted_names().collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"));
        assert!(names.contains(&"b2c3d4e5-f6a7-8901-bcde-f12345678901.dat"));
        assert!(names.contains(&"c3d4e5f6-a7b8-9012-cdef-123456789012.dat"));
    }

    #[test]
    fn test_duplicate_encrypted_names() {
        // 如果有重复的 encrypted_name，后面的会覆盖前面的
        let export = MappingExport {
            version: "1.0".to_string(),
            exported_at: 1702454400000,
            records: vec![
                create_test_record("same-name.dat", "first.txt"),
                create_test_record("same-name.dat", "second.txt"),
            ],
        };
        let loader = MappingLoader::from_export(export);

        // 索引会指向最后一个
        let record = loader.find_by_encrypted_name("same-name.dat");
        assert!(record.is_some());
        assert_eq!(record.unwrap().original_name, "second.txt");
    }

    #[test]
    fn test_case_sensitive_lookup() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        // 大小写敏感
        let record = loader.find_by_encrypted_name("A1B2C3D4-E5F6-7890-ABCD-EF1234567890.DAT");
        assert!(record.is_none());
    }

    #[test]
    fn test_mapping_record_fields() {
        let export = create_test_export();
        let loader = MappingLoader::from_export(export);

        let record = loader
            .find_by_encrypted_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat")
            .unwrap();

        assert_eq!(record.config_id, "config-1");
        assert_eq!(
            record.encrypted_name,
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"
        );
        assert_eq!(record.original_path, "/documents");
        assert_eq!(record.original_name, "file1.txt");
        assert!(!record.is_directory);
        assert_eq!(record.version, 1);
        assert_eq!(record.key_version, 1);
        assert_eq!(record.file_size, 1024);
        assert_eq!(record.file_size, 1024);
        assert!(record.remote_path.is_none());
        assert!(record.status.is_none());
    }
}


// ============================================================================
// 属性测试 (Property-Based Tests)
// ============================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// 生成有效的 UUID 格式字符串
    fn arb_uuid() -> impl Strategy<Value = String> {
        (
            prop::collection::vec(any::<u8>(), 4..=4),
            prop::collection::vec(any::<u8>(), 2..=2),
            prop::collection::vec(any::<u8>(), 2..=2),
            prop::collection::vec(any::<u8>(), 2..=2),
            prop::collection::vec(any::<u8>(), 6..=6),
        )
            .prop_map(|(a, b, c, d, e)| {
                format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    a[0], a[1], a[2], a[3],
                    b[0], b[1],
                    c[0], c[1],
                    d[0], d[1],
                    e[0], e[1], e[2], e[3], e[4], e[5]
                )
            })
    }

    /// 生成加密文件名（UUID.dat 格式）
    fn arb_encrypted_name() -> impl Strategy<Value = String> {
        arb_uuid().prop_map(|uuid| format!("{}.dat", uuid))
    }

    /// 生成原始文件名
    fn arb_original_name() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_]{1,20}\\.[a-z]{2,4}".prop_map(String::from)
    }

    /// 生成原始路径
    fn arb_original_path() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("/".to_string()),
            Just("/documents".to_string()),
            Just("/documents/work".to_string()),
            Just("/photos/2024".to_string()),
            "[a-zA-Z0-9_/]{1,30}".prop_map(|s| format!("/{}", s)),
        ]
    }

    /// 生成映射记录
    fn arb_mapping_record() -> impl Strategy<Value = MappingRecord> {
        (
            arb_encrypted_name(),
            arb_original_name(),
            arb_original_path(),
            1u32..=10,
            1u64..=1_000_000_000,
        )
            .prop_map(
                |(encrypted_name, original_name, original_path, key_version, file_size)| {
                    MappingRecord {
                        config_id: "config-1".to_string(),
                        encrypted_name,
                        original_path,
                        original_name,
                        is_directory: false,
                        version: 1,
                        key_version,
                        file_size,
                        remote_path: None,
                        status: None,
                    }
                },
            )
    }

    /// 生成映射记录列表（确保 encrypted_name 唯一）
    fn arb_unique_mapping_records(count: usize) -> impl Strategy<Value = Vec<MappingRecord>> {
        prop::collection::vec(arb_mapping_record(), count..=count).prop_map(move |mut records| {
            // 确保 encrypted_name 唯一
            let mut seen = std::collections::HashSet::new();
            records.retain(|r| seen.insert(r.encrypted_name.clone()));
            // 如果有重复，添加后缀使其唯一
            let mut i = 0;
            while records.len() < count && i < 1000 {
                let mut new_record = records.last().cloned().unwrap_or_else(|| MappingRecord {
                    config_id: "config-1".to_string(),
                    encrypted_name: format!("fallback-{}.age", i),
                    original_path: "/".to_string(),
                    original_name: format!("file{}.txt", i),
                    is_directory: false,
                    version: 1,
                    key_version: 1,
                    file_size: 1024,
                    remote_path: None,
                    status: None,
                });
                new_record.encrypted_name = format!("unique-{}.age", i);
                new_record.original_name = format!("file{}.txt", i);
                if seen.insert(new_record.encrypted_name.clone()) {
                    records.push(new_record);
                }
                i += 1;
            }
            records
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// **Property 8: 映射查找正确性**
        ///
        /// 对于任何输入目录中的加密文件，如果其文件名在 mapping.json 中存在
        /// 对应的 encrypted_name 记录，则必须使用该记录的 original_path 和
        /// original_name 作为输出路径。
        ///
        /// **Validates: Requirements 2.2**
        #[test]
        fn prop_mapping_lookup_correctness(
            records in arb_unique_mapping_records(5),
        ) {
            prop_assume!(!records.is_empty());

            let export = MappingExport {
                version: "1.0".to_string(),
                exported_at: 1702454400000,
                records: records.clone(),
            };
            let loader = MappingLoader::from_export(export);

            // 测试每个记录都能被正确找到
            for record in &records {
                let found = loader.find_by_encrypted_name(&record.encrypted_name);
                prop_assert!(
                    found.is_some(),
                    "应该能找到 encrypted_name: {}",
                    record.encrypted_name
                );

                let found = found.unwrap();
                prop_assert_eq!(
                    &found.encrypted_name,
                    &record.encrypted_name,
                    "encrypted_name 应该匹配"
                );
                prop_assert_eq!(
                    &found.original_path,
                    &record.original_path,
                    "original_path 应该匹配"
                );
                prop_assert_eq!(
                    &found.original_name,
                    &record.original_name,
                    "original_name 应该匹配"
                );
                prop_assert_eq!(
                    found.key_version,
                    record.key_version,
                    "key_version 应该匹配"
                );
            }
        }

        /// **Property 8 补充: 不存在的文件名返回 None**
        ///
        /// 对于不在映射中的文件名，find_by_encrypted_name 应返回 None。
        ///
        /// **Validates: Requirements 2.2**
        #[test]
        fn prop_nonexistent_name_returns_none(
            records in arb_unique_mapping_records(3),
            nonexistent_name in arb_encrypted_name(),
        ) {
            // 确保 nonexistent_name 不在 records 中
            prop_assume!(!records.iter().any(|r| r.encrypted_name == nonexistent_name));

            let export = MappingExport {
                version: "1.0".to_string(),
                exported_at: 1702454400000,
                records,
            };
            let loader = MappingLoader::from_export(export);

            let found = loader.find_by_encrypted_name(&nonexistent_name);
            prop_assert!(
                found.is_none(),
                "不存在的文件名 {} 应返回 None",
                nonexistent_name
            );
        }

        /// **Property 8 补充: 索引与记录数量一致**
        ///
        /// MappingLoader 的索引应该与记录数量一致。
        ///
        /// **Validates: Requirements 2.2**
        #[test]
        fn prop_index_count_matches_records(
            records in arb_unique_mapping_records(5),
        ) {
            let export = MappingExport {
                version: "1.0".to_string(),
                exported_at: 1702454400000,
                records: records.clone(),
            };
            let loader = MappingLoader::from_export(export);

            prop_assert_eq!(
                loader.len(),
                records.len(),
                "len() 应该与记录数量一致"
            );

            // 所有 encrypted_names 都应该能被找到
            let names: Vec<&str> = loader.encrypted_names().collect();
            prop_assert_eq!(
                names.len(),
                records.len(),
                "encrypted_names 数量应该与记录数量一致"
            );
        }

        /// **Property 8 补充: 空映射处理**
        ///
        /// 空映射应该正确处理，is_empty() 返回 true，查找返回 None。
        ///
        /// **Validates: Requirements 2.2**
        #[test]
        fn prop_empty_mapping_handling(
            query_name in arb_encrypted_name(),
        ) {
            let export = MappingExport {
                version: "1.0".to_string(),
                exported_at: 1702454400000,
                records: vec![],
            };
            let loader = MappingLoader::from_export(export);

            prop_assert!(loader.is_empty(), "空映射应该 is_empty() 返回 true");
            prop_assert_eq!(loader.len(), 0, "空映射 len() 应该为 0");
            prop_assert!(
                loader.find_by_encrypted_name(&query_name).is_none(),
                "空映射查找应返回 None"
            );
        }

        /// **Property 8 补充: 记录字段完整性**
        ///
        /// 查找到的记录应该包含所有必须字段。
        ///
        /// **Validates: Requirements 2.2**
        #[test]
        fn prop_record_fields_integrity(
            record in arb_mapping_record(),
        ) {
            let export = MappingExport {
                version: "1.0".to_string(),
                exported_at: 1702454400000,
                records: vec![record.clone()],
            };
            let loader = MappingLoader::from_export(export);

            let found = loader.find_by_encrypted_name(&record.encrypted_name);
            prop_assert!(found.is_some());

            let found = found.unwrap();
            // 验证所有必须字段
            prop_assert!(!found.config_id.is_empty(), "config_id 不应为空");
            prop_assert!(!found.encrypted_name.is_empty(), "encrypted_name 不应为空");
            prop_assert!(!found.original_name.is_empty(), "original_name 不应为空");
            prop_assert!(found.key_version > 0, "key_version 应大于 0");
        }
    }
}
