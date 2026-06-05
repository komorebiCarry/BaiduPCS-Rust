use serde::{Deserialize, Serialize};

/// 上传冲突策略（直接映射百度网盘 API 的 rtype 参数）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadConflictStrategy {
    /// 智能去重：比较 block_list，相同则秒传，不同则重命名（rtype=2）
    SmartDedup,
    /// 自动重命名：路径冲突时自动生成唯一名称（rtype=1）
    AutoRename,
    /// 覆盖：直接覆盖已存在的文件（rtype=3，危险操作）
    Overwrite,
}

impl Default for UploadConflictStrategy {
    fn default() -> Self {
        Self::SmartDedup
    }
}

/// 下载冲突策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadConflictStrategy {
    /// 覆盖：目标文件存在时覆盖
    Overwrite,
    /// 跳过：目标文件存在时跳过
    Skip,
    /// 自动重命名：目标文件存在时生成唯一名称
    AutoRename,
}

impl Default for DownloadConflictStrategy {
    fn default() -> Self {
        Self::Overwrite
    }
}

/// 冲突解决方案
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResolution {
    /// 继续操作（无冲突或覆盖策略）
    Proceed,
    /// 跳过此文件
    Skip,
    /// 使用新路径（重命名后的路径）
    UseNewPath(String),
}

/// 将冲突策略转换为百度网盘 API 的 rtype 参数
///
/// # 参数
/// - strategy: 上传冲突策略
///
/// # 返回
/// - "1": 路径冲突时自动重命名（AutoRename）
/// - "2": block_list 不同时自动重命名（SmartDedup）
/// - "3": 直接覆盖已存在文件（Overwrite，危险）
///
/// # 说明
/// 百度网盘 API 的 rtype 参数说明：
/// - rtype=1: 当上传路径已存在文件时，自动重命名为 "文件名(1).ext"
/// - rtype=2: 当上传路径已存在文件且 block_list 不同时，自动重命名；如果 block_list 相同则秒传
/// - rtype=3: 直接覆盖已存在的文件（危险操作，可能导致数据丢失）
pub fn conflict_strategy_to_rtype(strategy: UploadConflictStrategy) -> &'static str {
    match strategy {
        UploadConflictStrategy::SmartDedup => "2", // block_list 不同时重命名，相同则秒传
        UploadConflictStrategy::AutoRename => "1", // 路径冲突时重命名
        UploadConflictStrategy::Overwrite => "3",  // 覆盖已存在文件
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // 生成器：上传冲突策略
    fn prop_upload_strategy() -> impl Strategy<Value = UploadConflictStrategy> {
        prop_oneof![
            Just(UploadConflictStrategy::SmartDedup),
            Just(UploadConflictStrategy::AutoRename),
            Just(UploadConflictStrategy::Overwrite),
        ]
    }

    // 生成器：下载冲突策略
    fn prop_download_strategy() -> impl Strategy<Value = DownloadConflictStrategy> {
        prop_oneof![
            Just(DownloadConflictStrategy::Overwrite),
            Just(DownloadConflictStrategy::Skip),
            Just(DownloadConflictStrategy::AutoRename),
        ]
    }

    // Feature: file-conflict-strategy, Property 1: 配置持久化往返
    // **Validates: Requirements 4.3**
    proptest! {
        #[test]
        fn test_upload_strategy_serialization_roundtrip(
            strategy in prop_upload_strategy()
        ) {
            // 序列化
            let serialized = serde_json::to_string(&strategy).unwrap();

            // 反序列化
            let deserialized: UploadConflictStrategy = serde_json::from_str(&serialized).unwrap();

            // 验证等价性
            prop_assert_eq!(strategy, deserialized);
        }

        #[test]
        fn test_download_strategy_serialization_roundtrip(
            strategy in prop_download_strategy()
        ) {
            // 序列化
            let serialized = serde_json::to_string(&strategy).unwrap();

            // 反序列化
            let deserialized: DownloadConflictStrategy = serde_json::from_str(&serialized).unwrap();

            // 验证等价性
            prop_assert_eq!(strategy, deserialized);
        }
    }

    #[test]
    fn test_upload_strategy_default() {
        assert_eq!(
            UploadConflictStrategy::default(),
            UploadConflictStrategy::SmartDedup
        );
    }

    #[test]
    fn test_download_strategy_default() {
        assert_eq!(
            DownloadConflictStrategy::default(),
            DownloadConflictStrategy::Overwrite
        );
    }

    #[test]
    fn test_upload_strategy_serde_format() {
        // 测试序列化格式为 snake_case
        assert_eq!(
            serde_json::to_string(&UploadConflictStrategy::SmartDedup).unwrap(),
            r#""smart_dedup""#
        );
        assert_eq!(
            serde_json::to_string(&UploadConflictStrategy::AutoRename).unwrap(),
            r#""auto_rename""#
        );
        assert_eq!(
            serde_json::to_string(&UploadConflictStrategy::Overwrite).unwrap(),
            r#""overwrite""#
        );
    }

    #[test]
    fn test_download_strategy_serde_format() {
        // 测试序列化格式为 snake_case
        assert_eq!(
            serde_json::to_string(&DownloadConflictStrategy::Overwrite).unwrap(),
            r#""overwrite""#
        );
        assert_eq!(
            serde_json::to_string(&DownloadConflictStrategy::Skip).unwrap(),
            r#""skip""#
        );
        assert_eq!(
            serde_json::to_string(&DownloadConflictStrategy::AutoRename).unwrap(),
            r#""auto_rename""#
        );
    }

    #[test]
    fn test_conflict_strategy_to_rtype_smart_dedup() {
        assert_eq!(
            conflict_strategy_to_rtype(UploadConflictStrategy::SmartDedup),
            "2"
        );
    }

    #[test]
    fn test_conflict_strategy_to_rtype_auto_rename() {
        assert_eq!(
            conflict_strategy_to_rtype(UploadConflictStrategy::AutoRename),
            "1"
        );
    }

    #[test]
    fn test_conflict_strategy_to_rtype_overwrite() {
        assert_eq!(
            conflict_strategy_to_rtype(UploadConflictStrategy::Overwrite),
            "3"
        );
    }

    #[test]
    fn test_conflict_strategy_to_rtype_all_variants() {
        // 测试所有策略映射到正确的 rtype 值
        let test_cases = vec![
            (UploadConflictStrategy::SmartDedup, "2"),
            (UploadConflictStrategy::AutoRename, "1"),
            (UploadConflictStrategy::Overwrite, "3"),
        ];

        for (strategy, expected_rtype) in test_cases {
            assert_eq!(
                conflict_strategy_to_rtype(strategy),
                expected_rtype,
                "Strategy {:?} should map to rtype {}",
                strategy,
                expected_rtype
            );
        }
    }
}
