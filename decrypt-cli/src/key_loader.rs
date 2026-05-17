//! 密钥加载器模块
//!
//! 负责从 JSON 文件加载密钥配置，并提供密钥查找功能。

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::types::{DecryptError, EncryptionConfig, EncryptionKeyInfo};

/// 密钥加载器
///
/// 从 encryption.json 文件加载密钥配置，提供按版本号查找密钥的功能。
#[derive(Debug, Clone)]
pub struct KeyLoader {
    /// 加密配置
    config: EncryptionConfig,
}

impl KeyLoader {
    /// 从文件加载密钥配置
    ///
    /// # Arguments
    /// * `path` - encryption.json 文件路径
    ///
    /// # Returns
    /// * `Ok(KeyLoader)` - 成功加载的密钥加载器
    /// * `Err(DecryptError)` - 加载失败的错误
    ///
    /// # Example
    /// ```ignore
    /// let loader = KeyLoader::load(Path::new("encryption.json"))?;
    /// ```
    pub fn load(path: &Path) -> Result<Self, DecryptError> {
        let file = File::open(path).map_err(|e| {
            DecryptError::IoError(std::io::Error::new(
                e.kind(),
                format!("无法打开密钥文件 '{}': {}", path.display(), e),
            ))
        })?;

        let reader = BufReader::new(file);
        let config: EncryptionConfig = serde_json::from_reader(reader).map_err(|e| {
            DecryptError::InvalidFormat(format!(
                "密钥文件 '{}' 格式无效: {}",
                path.display(),
                e
            ))
        })?;

        Ok(Self { config })
    }

    /// 从 EncryptionConfig 直接创建（用于测试）
    #[allow(dead_code)]
    pub fn from_config(config: EncryptionConfig) -> Self {
        Self { config }
    }

    /// 根据版本号获取密钥
    ///
    /// 查找顺序：
    /// 1. 先检查 current_key 是否匹配
    /// 2. 再在 key_history 中查找
    ///
    /// # Arguments
    /// * `version` - 密钥版本号
    ///
    /// # Returns
    /// * `Some(&EncryptionKeyInfo)` - 找到的密钥
    /// * `None` - 未找到对应版本的密钥
    pub fn get_key(&self, version: u32) -> Option<&EncryptionKeyInfo> {
        // 先检查 current_key
        if self.config.current.is_valid() && self.config.current.key_version == version {
            return Some(&self.config.current);
        }

        // 再在 key_history 中查找
        self.config
            .history
            .iter()
            .find(|key| key.key_version == version && key.is_valid())
    }

    /// 获取当前密钥
    ///
    /// 如果 current_key.master_key 为空或 key_version 为 0，返回 None。
    ///
    /// # Returns
    /// * `Some(&EncryptionKeyInfo)` - 当前有效的密钥
    /// * `None` - 当前密钥无效（已废弃）
    #[allow(dead_code)]
    pub fn current_key(&self) -> Option<&EncryptionKeyInfo> {
        if self.config.current.is_valid() {
            Some(&self.config.current)
        } else {
            None
        }
    }

    /// 获取所有可用密钥
    ///
    /// 返回 current_key（如果有效）和 key_history 中的所有有效密钥。
    /// 用于单文件模式遍历所有密钥尝试解密。
    ///
    /// # Returns
    /// 所有可用密钥的引用列表
    pub fn all_keys(&self) -> Vec<&EncryptionKeyInfo> {
        let mut keys = Vec::new();

        // 添加 current_key（如果有效）
        if self.config.current.is_valid() {
            keys.push(&self.config.current);
        }

        // 添加 key_history 中的有效密钥
        for key in &self.config.history {
            if key.is_valid() {
                keys.push(key);
            }
        }

        keys
    }

    /// 获取密钥数量
    pub fn key_count(&self) -> usize {
        self.all_keys().len()
    }

    /// 检查是否有可用密钥
    pub fn has_keys(&self) -> bool {
        self.config.current.is_valid() || self.config.history.iter().any(|k| k.is_valid())
    }

    /// 获取原始配置的引用
    #[allow(dead_code)]
    pub fn config(&self) -> &EncryptionConfig {
        &self.config
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EncryptionAlgorithm;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// 创建测试用的密钥信息
    fn create_test_key(version: u32, master_key: &str) -> EncryptionKeyInfo {
        EncryptionKeyInfo {
            master_key: master_key.to_string(),
            algorithm: EncryptionAlgorithm::Age,
            key_version: version,
            created_at: 1702454400000,
            last_used_at: None,
            deprecated_at: None,
        }
    }

    /// 创建测试用的加密配置
    fn create_test_config() -> EncryptionConfig {
        EncryptionConfig {
            current: create_test_key(2, "Y3VycmVudGtleWN1cnJlbnRrZXljdXJyZW50a2V5MQ=="),
            history: vec![
                create_test_key(1, "aGlzdG9yeWtleTFoaXN0b3J5a2V5MWhpc3Rvcnkx"),
            ],
        }
    }

    #[test]
    fn test_load_from_file() {
        let config = create_test_config();
        let json = serde_json::to_string_pretty(&config).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();

        let loader = KeyLoader::load(temp_file.path()).unwrap();
        assert_eq!(loader.config.current.key_version, 2);
        assert_eq!(loader.config.history.len(), 1);
    }

    #[test]
    fn test_load_file_not_found() {
        let result = KeyLoader::load(Path::new("nonexistent.json"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::IoError(_)));
    }

    #[test]
    fn test_load_invalid_json() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"invalid json").unwrap();

        let result = KeyLoader::load(temp_file.path());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DecryptError::InvalidFormat(_)));
    }

    #[test]
    fn test_get_key_current() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        let key = loader.get_key(2);
        assert!(key.is_some());
        assert_eq!(key.unwrap().key_version, 2);
    }

    #[test]
    fn test_get_key_history() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        let key = loader.get_key(1);
        assert!(key.is_some());
        assert_eq!(key.unwrap().key_version, 1);
    }

    #[test]
    fn test_get_key_not_found() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        let key = loader.get_key(99);
        assert!(key.is_none());
    }

    #[test]
    fn test_current_key_valid() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        let key = loader.current_key();
        assert!(key.is_some());
        assert_eq!(key.unwrap().key_version, 2);
    }

    #[test]
    fn test_current_key_empty_master_key() {
        let config = EncryptionConfig {
            current: EncryptionKeyInfo {
                master_key: "".to_string(),
                algorithm: EncryptionAlgorithm::Age,
                key_version: 0,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: vec![create_test_key(1, "aGlzdG9yeWtleTFoaXN0b3J5a2V5MWhpc3Rvcnkx")],
        };
        let loader = KeyLoader::from_config(config);

        let key = loader.current_key();
        assert!(key.is_none());
    }

    #[test]
    fn test_current_key_zero_version() {
        let config = EncryptionConfig {
            current: EncryptionKeyInfo {
                master_key: "c29tZWtleQ==".to_string(),
                algorithm: EncryptionAlgorithm::Age,
                key_version: 0,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: vec![],
        };
        let loader = KeyLoader::from_config(config);

        let key = loader.current_key();
        assert!(key.is_none());
    }

    #[test]
    fn test_all_keys() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        let keys = loader.all_keys();
        assert_eq!(keys.len(), 2);
        // current_key 应该在前面
        assert_eq!(keys[0].key_version, 2);
        assert_eq!(keys[1].key_version, 1);
    }

    #[test]
    fn test_all_keys_no_current() {
        let config = EncryptionConfig {
            current: EncryptionKeyInfo {
                master_key: "".to_string(),
                algorithm: EncryptionAlgorithm::Age,
                key_version: 0,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: vec![
                create_test_key(1, "a2V5MQ=="),
                create_test_key(2, "a2V5Mg=="),
            ],
        };
        let loader = KeyLoader::from_config(config);

        let keys = loader.all_keys();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].key_version, 1);
        assert_eq!(keys[1].key_version, 2);
    }

    #[test]
    fn test_all_keys_empty() {
        let config = EncryptionConfig {
            current: EncryptionKeyInfo {
                master_key: "".to_string(),
                algorithm: EncryptionAlgorithm::Age,
                key_version: 0,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: vec![],
        };
        let loader = KeyLoader::from_config(config);

        let keys = loader.all_keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_key_count() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        assert_eq!(loader.key_count(), 2);
    }

    #[test]
    fn test_has_keys() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config);

        assert!(loader.has_keys());
    }

    #[test]
    fn test_has_keys_false() {
        let config = EncryptionConfig {
            current: EncryptionKeyInfo {
                master_key: "".to_string(),
                algorithm: EncryptionAlgorithm::Age,
                key_version: 0,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
            history: vec![],
        };
        let loader = KeyLoader::from_config(config);

        assert!(!loader.has_keys());
    }

    #[test]
    fn test_config_accessor() {
        let config = create_test_config();
        let loader = KeyLoader::from_config(config.clone());

        assert_eq!(loader.config().current.key_version, config.current.key_version);
    }

    #[test]
    fn test_get_key_prefers_current_over_history() {
        // 如果 current_key 和 history 中有相同版本号的密钥，应该返回 current_key
        let config = EncryptionConfig {
            current: create_test_key(1, "Y3VycmVudA=="),
            history: vec![create_test_key(1, "aGlzdG9yeQ==")],
        };
        let loader = KeyLoader::from_config(config);

        let key = loader.get_key(1);
        assert!(key.is_some());
        assert_eq!(key.unwrap().master_key, "Y3VycmVudA==");
    }

    #[test]
    fn test_all_keys_filters_invalid() {
        let config = EncryptionConfig {
            current: create_test_key(2, "dmFsaWQ="),
            history: vec![
                create_test_key(1, "dmFsaWQx"),
                EncryptionKeyInfo {
                    master_key: "".to_string(), // 无效
                    algorithm: EncryptionAlgorithm::Age,
                    key_version: 3,
                    created_at: 1702454400000,
                    last_used_at: None,
                    deprecated_at: None,
                },
                EncryptionKeyInfo {
                    master_key: "c29tZWtleQ==".to_string(),
                    algorithm: EncryptionAlgorithm::Age,
                    key_version: 0, // 无效版本
                    created_at: 1702454400000,
                    last_used_at: None,
                    deprecated_at: None,
                },
            ],
        };
        let loader = KeyLoader::from_config(config);

        let keys = loader.all_keys();
        assert_eq!(keys.len(), 2); // 只有 2 个有效密钥
    }
}


// ============================================================================
// 属性测试 (Property-Based Tests)
// ============================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::types::EncryptionAlgorithm;
    use proptest::prelude::*;

    /// 生成有效的 Base64 编码密钥
    fn arb_master_key() -> impl Strategy<Value = String> {
        // 生成 32 字节的随机数据并 Base64 编码
        prop::collection::vec(any::<u8>(), 32..=32)
            .prop_map(|bytes| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes))
    }

    /// 生成有效的密钥版本号（1-100）
    fn arb_key_version() -> impl Strategy<Value = u32> {
        1u32..=100
    }

    fn arb_algorithm() -> impl Strategy<Value = EncryptionAlgorithm> {
        Just(EncryptionAlgorithm::Age)
    }

    /// 生成有效的密钥信息
    fn arb_valid_key_info() -> impl Strategy<Value = EncryptionKeyInfo> {
        (arb_master_key(), arb_algorithm(), arb_key_version()).prop_map(
            |(master_key, algorithm, key_version)| EncryptionKeyInfo {
                master_key,
                algorithm,
                key_version,
                created_at: 1702454400000,
                last_used_at: None,
                deprecated_at: None,
            },
        )
    }

    /// 生成无效的密钥信息（空 master_key 或 key_version 为 0）
    fn arb_invalid_key_info() -> impl Strategy<Value = EncryptionKeyInfo> {
        prop_oneof![
            // 空 master_key
            (arb_algorithm(), arb_key_version()).prop_map(|(algorithm, key_version)| {
                EncryptionKeyInfo {
                    master_key: "".to_string(),
                    algorithm,
                    key_version,
                    created_at: 1702454400000,
                    last_used_at: None,
                    deprecated_at: None,
                }
            }),
            // key_version 为 0
            (arb_master_key(), arb_algorithm()).prop_map(|(master_key, algorithm)| {
                EncryptionKeyInfo {
                    master_key,
                    algorithm,
                    key_version: 0,
                    created_at: 1702454400000,
                    last_used_at: None,
                    deprecated_at: None,
                }
            }),
        ]
    }

    /// 生成密钥历史列表（0-5 个有效密钥）
    fn arb_key_history() -> impl Strategy<Value = Vec<EncryptionKeyInfo>> {
        prop::collection::vec(arb_valid_key_info(), 0..=5)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// **Property 4: 密钥版本选择正确性**
        ///
        /// 对于任何映射记录中指定的 key_version，解密引擎必须从 encryption.json
        /// 的 current_key 或 key_history 中选择版本号匹配的密钥进行解密。
        ///
        /// **Validates: Requirements 2.3, 5.1, 5.4, 5.5**
        #[test]
        fn prop_key_version_selection_correctness(
            current_key in arb_valid_key_info(),
            history in arb_key_history(),
        ) {
            let config = EncryptionConfig {
                current: current_key.clone(),
                history: history.clone(),
            };
            let loader = KeyLoader::from_config(config);

            // 测试 current_key 的版本号能被正确找到
            let found_current = loader.get_key(current_key.key_version);
            prop_assert!(
                found_current.is_some(),
                "应该能找到 current_key 的版本 {}",
                current_key.key_version
            );
            prop_assert_eq!(
                found_current.unwrap().key_version,
                current_key.key_version,
                "找到的密钥版本应该匹配"
            );

            // 测试 history 中的每个密钥版本号能被正确找到
            for hist_key in &history {
                // 如果版本号与 current_key 相同，get_key 应该返回 current_key
                let found = loader.get_key(hist_key.key_version);
                prop_assert!(
                    found.is_some(),
                    "应该能找到 history 中的版本 {}",
                    hist_key.key_version
                );
                prop_assert_eq!(
                    found.unwrap().key_version,
                    hist_key.key_version,
                    "找到的密钥版本应该匹配"
                );
            }

            // 测试不存在的版本号返回 None
            let max_version = std::iter::once(current_key.key_version)
                .chain(history.iter().map(|k| k.key_version))
                .max()
                .unwrap_or(0);
            let nonexistent_version = max_version + 1;
            prop_assert!(
                loader.get_key(nonexistent_version).is_none(),
                "不存在的版本 {} 应该返回 None",
                nonexistent_version
            );
        }

        /// **Property 4 补充: current_key 为空时的处理**
        ///
        /// 当 current_key.master_key 为空时，current_key() 应返回 None，
        /// 但仍应能从 key_history 中查找密钥。
        ///
        /// **Validates: Requirements 5.4, 5.5**
        #[test]
        fn prop_empty_current_key_handling(
            history in prop::collection::vec(arb_valid_key_info(), 1..=5),
        ) {
            let config = EncryptionConfig {
                current: EncryptionKeyInfo {
                    master_key: "".to_string(),
                    algorithm: EncryptionAlgorithm::Age,
                    key_version: 0,
                    created_at: 1702454400000,
                    last_used_at: None,
                    deprecated_at: None,
                },
                history: history.clone(),
            };
            let loader = KeyLoader::from_config(config);

            // current_key() 应返回 None
            prop_assert!(
                loader.current_key().is_none(),
                "current_key 为空时应返回 None"
            );

            // 但仍应能从 history 中查找密钥
            for hist_key in &history {
                let found = loader.get_key(hist_key.key_version);
                prop_assert!(
                    found.is_some(),
                    "应该能从 history 中找到版本 {}",
                    hist_key.key_version
                );
            }

            // all_keys() 应只返回 history 中的密钥
            let all = loader.all_keys();
            prop_assert_eq!(
                all.len(),
                history.len(),
                "all_keys 应只包含 history 中的密钥"
            );
        }

        /// **Property 4 补充: all_keys 返回所有有效密钥**
        ///
        /// all_keys() 应返回 current_key（如果有效）和 key_history 中的所有有效密钥。
        ///
        /// **Validates: Requirements 5.3**
        #[test]
        fn prop_all_keys_returns_all_valid(
            current_key in arb_valid_key_info(),
            history in arb_key_history(),
        ) {
            let config = EncryptionConfig {
                current: current_key.clone(),
                history: history.clone(),
            };
            let loader = KeyLoader::from_config(config);

            let all = loader.all_keys();

            // 应该包含 current_key
            prop_assert!(
                all.iter().any(|k| k.key_version == current_key.key_version),
                "all_keys 应包含 current_key"
            );

            // 应该包含所有 history 中的有效密钥
            for hist_key in &history {
                if hist_key.is_valid() {
                    prop_assert!(
                        all.iter().any(|k| k.key_version == hist_key.key_version),
                        "all_keys 应包含 history 中的版本 {}",
                        hist_key.key_version
                    );
                }
            }

            // 数量应该正确
            let expected_count = 1 + history.iter().filter(|k| k.is_valid()).count();
            prop_assert_eq!(
                all.len(),
                expected_count,
                "all_keys 数量应该正确"
            );
        }

        /// **Property 4 补充: 无效密钥被过滤**
        ///
        /// all_keys() 和 get_key() 应该过滤掉无效的密钥（空 master_key 或 key_version 为 0）。
        ///
        /// **Validates: Requirements 5.4, 5.5**
        #[test]
        fn prop_invalid_keys_filtered(
            valid_key in arb_valid_key_info(),
            invalid_key in arb_invalid_key_info(),
        ) {
            let config = EncryptionConfig {
                current: valid_key.clone(),
                history: vec![invalid_key.clone()],
            };
            let loader = KeyLoader::from_config(config);

            // all_keys 应该只包含有效密钥
            let all = loader.all_keys();
            prop_assert_eq!(
                all.len(),
                1,
                "all_keys 应该只包含 1 个有效密钥"
            );
            prop_assert_eq!(
                all[0].key_version,
                valid_key.key_version,
                "all_keys 应该只包含有效的 current_key"
            );

            // get_key 对无效密钥的版本号应返回 None
            if invalid_key.key_version != valid_key.key_version {
                prop_assert!(
                    loader.get_key(invalid_key.key_version).is_none(),
                    "get_key 对无效密钥版本应返回 None"
                );
            }
        }

        /// **Property 4 补充: current_key 优先于 history**
        ///
        /// 如果 current_key 和 history 中有相同版本号的密钥，
        /// get_key 应该返回 current_key。
        ///
        /// **Validates: Requirements 5.1**
        #[test]
        fn prop_current_key_priority(
            version in arb_key_version(),
            current_master_key in arb_master_key(),
            history_master_key in arb_master_key(),
        ) {
            // 确保两个 master_key 不同
            prop_assume!(current_master_key != history_master_key);

            let config = EncryptionConfig {
                current: EncryptionKeyInfo {
                    master_key: current_master_key.clone(),
                    algorithm: EncryptionAlgorithm::Age,
                    key_version: version,
                    created_at: 1702454400000,
                    last_used_at: None,
                    deprecated_at: None,
                },
                history: vec![EncryptionKeyInfo {
                    master_key: history_master_key.clone(),
                    algorithm: EncryptionAlgorithm::Age,
                    key_version: version, // 相同版本号
                    created_at: 1700000000000,
                    last_used_at: None,
                    deprecated_at: Some(1702454400000),
                }],
            };
            let loader = KeyLoader::from_config(config);

            let found = loader.get_key(version);
            prop_assert!(found.is_some());
            prop_assert_eq!(
                &found.unwrap().master_key,
                &current_master_key,
                "get_key 应该返回 current_key 而不是 history 中的密钥"
            );
        }
    }
}
