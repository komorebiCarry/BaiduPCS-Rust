use anyhow::Result;
use std::path::Path;

/// 生成唯一文件名
///
/// # 参数
/// - original_path: 原始路径
/// - check_exists: 检查路径是否存在的闭包
///
/// # 返回
/// - Ok(String): 唯一的文件路径
/// - Err: 无法生成唯一路径（超过 9999 次尝试）
///
/// # 示例
/// - "file.txt" -> "file (1).txt"
/// - "file (1).txt" -> "file (2).txt"
/// - "/path/to/file.txt" -> "/path/to/file (1).txt" (Unix)
/// - "C:\\path\\to\\file.txt" -> "C:\\path\\to\\file (1).txt" (Windows)
pub fn generate_unique_path<F>(original_path: &str, check_exists: F) -> Result<String>
where
    F: Fn(&str) -> bool,
{
    if !check_exists(original_path) {
        return Ok(original_path.to_string());
    }

    let path = Path::new(original_path);
    let parent = path
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    // 获取原始文件名
    let filename = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    // 解析文件名，提取基础名称和扩展名
    // 同时检查是否已经是 "name (N)" 格式
    let (base_name, extension) = parse_filename(&filename);

    for counter in 1..=9999 {
        let new_name = if let Some(ext) = &extension {
            format!("{} ({}).{}", base_name, counter, ext)
        } else {
            format!("{} ({})", base_name, counter)
        };

        // 使用 PathBuf 正确处理路径分隔符
        let new_path = if parent.is_empty() {
            new_name
        } else {
            let mut path_buf = std::path::PathBuf::from(&parent);
            path_buf.push(&new_name);
            path_buf.to_string_lossy().to_string()
        };

        if !check_exists(&new_path) {
            return Ok(new_path);
        }
    }

    anyhow::bail!(
        "无法生成唯一路径：已尝试 9999 次，原始路径: {}",
        original_path
    )
}

/// 解析文件名，提取基础名称和扩展名
/// 如果文件名已经是 "name (N)" 格式，则提取原始名称
///
/// # 示例
/// - "file.txt" -> ("file", Some("txt"))
/// - "file (1).txt" -> ("file", Some("txt"))
/// - "file (2).txt" -> ("file", Some("txt"))
/// - "file" -> ("file", None)
/// - "file (1)" -> ("file", None)
fn parse_filename(filename: &str) -> (String, Option<String>) {
    // 先分离扩展名
    let (name_part, extension) = if let Some(dot_pos) = filename.rfind('.') {
        let name = &filename[..dot_pos];
        let ext = &filename[dot_pos + 1..];
        (name, Some(ext.to_string()))
    } else {
        (filename, None)
    };

    // 检查是否已经是 "name (N)" 格式
    // 使用正则表达式匹配 " (数字)" 结尾
    if let Some(paren_pos) = name_part.rfind(" (") {
        let after_paren = &name_part[paren_pos + 2..];
        if let Some(number_part) = after_paren.strip_suffix(')') {
            // 检查括号内是否全是数字
            if number_part.chars().all(|c| c.is_ascii_digit()) && !number_part.is_empty() {
                // 提取原始基础名称（去掉 " (N)" 部分）
                return (name_part[..paren_pos].to_string(), extension);
            }
        }
    }

    // 不是 "name (N)" 格式，直接返回
    (name_part.to_string(), extension)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use proptest::prelude::*;

    // Feature: file-conflict-strategy, Property 12: 自动重命名唯一性（上传）
    // Feature: file-conflict-strategy, Property 13: 自动重命名唯一性（下载）
    // Feature: file-conflict-strategy, Property 14: 重命名格式一致性
    // **Validates: Requirements 5.7, 5.8, 5.9, 5.10**

    proptest! {
        #[test]
        fn test_generated_path_is_unique(
            filename in "[a-zA-Z0-9_-]{1,20}",
            ext in "[a-z]{2,4}",
            existing_count in 0usize..10
        ) {
            let original = format!("{}.{}", filename, ext);
            let mut existing = HashSet::new();
            existing.insert(original.clone());
            
            // 添加一些已存在的重命名版本
            for i in 1..=existing_count {
                existing.insert(format!("{} ({}).{}", filename, i, ext));
            }
            
            let result = generate_unique_path(&original, |p| existing.contains(p)).unwrap();
            
            // 验证生成的路径不在已存在集合中
            assert!(!existing.contains(&result));
            
            // 验证格式正确
            let expected_counter = existing_count + 1;
            let expected_pattern = format!("({}).{}", expected_counter, ext);
            assert!(result.contains(&expected_pattern), "Result '{}' should contain '{}'", result, expected_pattern);
        }

        #[test]
        fn test_rename_format_consistency(
            filename in "[a-zA-Z0-9_-]{1,20}",
            ext in "[a-z]{2,4}"
        ) {
            let original = format!("{}.{}", filename, ext);
            let existing = HashSet::from([original.clone()]);
            
            let result = generate_unique_path(&original, |p| existing.contains(p)).unwrap();
            
            // 验证格式为 "filename (N).ext"
            let expected = format!("{} (1).{}", filename, ext);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_no_extension_format(
            filename in "[a-zA-Z0-9_-]{1,20}"
        ) {
            let existing = HashSet::from([filename.clone()]);
            
            let result = generate_unique_path(&filename, |p| existing.contains(p)).unwrap();
            
            // 验证格式为 "filename (N)"
            let expected = format!("{} (1)", filename);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_basic_rename() {
        let existing = HashSet::from(["file.txt"]);
        let result = generate_unique_path("file.txt", |p| existing.contains(p)).unwrap();
        assert_eq!(result, "file (1).txt");
    }

    #[test]
    fn test_incremental_rename() {
        let existing = HashSet::from(["file.txt", "file (1).txt"]);
        let result = generate_unique_path("file.txt", |p| existing.contains(p)).unwrap();
        assert_eq!(result, "file (2).txt");
    }

    #[test]
    fn test_no_extension() {
        let existing = HashSet::from(["file"]);
        let result = generate_unique_path("file", |p| existing.contains(p)).unwrap();
        assert_eq!(result, "file (1)");
    }

    #[test]
    fn test_with_path() {
        let existing = HashSet::from(["/path/to/file.txt"]);
        let result = generate_unique_path("/path/to/file.txt", |p| existing.contains(p)).unwrap();
        // On Windows, PathBuf will use backslashes, so we need to check for the pattern
        assert!(result.contains("file (1).txt"), "Expected 'file (1).txt' in result: {}", result);
    }

    #[test]
    fn test_no_conflict() {
        let existing = HashSet::<&str>::new();
        let result = generate_unique_path("file.txt", |p| existing.contains(p)).unwrap();
        assert_eq!(result, "file.txt");
    }

    #[test]
    fn test_special_characters() {
        let existing = HashSet::from(["file (test).txt"]);
        let result =
            generate_unique_path("file (test).txt", |p| existing.contains(p)).unwrap();
        assert_eq!(result, "file (test) (1).txt");
    }

    #[test]
    fn test_windows_path() {
        let existing = HashSet::from(["C:\\path\\to\\file.txt"]);
        let result =
            generate_unique_path("C:\\path\\to\\file.txt", |p| existing.contains(p)).unwrap();
        // On Windows, this should produce a Windows-style path
        // On Unix, it will still work but with forward slashes
        assert!(result.contains("file (1).txt"));
    }

    #[test]
    fn test_counter_limit() {
        // Create a closure that always returns true (file always exists)
        let result = generate_unique_path("file.txt", |_| true);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("已尝试 9999 次"));
    }
}

#[test]
fn test_incremental_rename_from_numbered() {
    use std::collections::HashSet;
    // 测试从已编号的文件继续递增
    let existing = HashSet::from(["file.txt", "file (1).txt", "file (2).txt"]);
    let result = generate_unique_path("file (1).txt", |p| existing.contains(p)).unwrap();
    assert_eq!(result, "file (3).txt");
}

#[test]
fn test_parse_filename_basic() {
    assert_eq!(parse_filename("file.txt"), ("file".to_string(), Some("txt".to_string())));
    assert_eq!(parse_filename("file"), ("file".to_string(), None));
}

#[test]
fn test_parse_filename_numbered() {
    assert_eq!(parse_filename("file (1).txt"), ("file".to_string(), Some("txt".to_string())));
    assert_eq!(parse_filename("file (2).txt"), ("file".to_string(), Some("txt".to_string())));
    assert_eq!(parse_filename("file (10).txt"), ("file".to_string(), Some("txt".to_string())));
    assert_eq!(parse_filename("file (1)"), ("file".to_string(), None));
}

#[test]
fn test_parse_filename_special_cases() {
    // 括号内不是数字，不应该被解析为编号格式
    assert_eq!(parse_filename("file (test).txt"), ("file (test)".to_string(), Some("txt".to_string())));
    assert_eq!(parse_filename("file (a1).txt"), ("file (a1)".to_string(), Some("txt".to_string())));
}
