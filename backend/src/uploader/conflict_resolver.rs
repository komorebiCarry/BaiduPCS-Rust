use crate::netdisk::NetdiskClient;
use crate::uploader::{ConflictResolution, DownloadConflictStrategy};
use anyhow::Result;
use std::path::Path;
use std::sync::{Arc, RwLock as StdRwLock};
use tracing::info;

/// 冲突解决器（仅用于下载）
pub struct ConflictResolver {
    #[allow(dead_code)]
    netdisk_client: Arc<StdRwLock<NetdiskClient>>,
}

impl ConflictResolver {
    pub fn new(netdisk_client: Arc<StdRwLock<NetdiskClient>>) -> Self {
        Self { netdisk_client }
    }

    /// 解决下载冲突
    ///
    /// # 参数
    /// - local_path: 本地目标路径
    /// - strategy: 冲突策略
    ///
    /// # 返回
    /// - Ok(ConflictResolution): 解决方案
    pub fn resolve_download_conflict(
        local_path: &Path,
        strategy: DownloadConflictStrategy,
    ) -> Result<ConflictResolution> {
        match strategy {
            DownloadConflictStrategy::Overwrite => Ok(ConflictResolution::Proceed),
            DownloadConflictStrategy::Skip => {
                if local_path.exists() {
                    info!("跳过策略：文件已存在，跳过下载 - local: {:?}", local_path);
                    Ok(ConflictResolution::Skip)
                } else {
                    Ok(ConflictResolution::Proceed)
                }
            }
            DownloadConflictStrategy::AutoRename => {
                if local_path.exists() {
                    let new_path = crate::common::path_utils::generate_unique_path(
                        &local_path.to_string_lossy(),
                        |path| Path::new(path).exists(),
                    )?;
                    info!("自动重命名：{:?} -> {}", local_path, new_path);
                    Ok(ConflictResolution::UseNewPath(new_path))
                } else {
                    Ok(ConflictResolution::Proceed)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // 创建一个测试用的 NetdiskClient mock
    // 注意：由于 NetdiskClient 没有 trait，我们需要使用真实的客户端
    // 但在单元测试中，我们可以测试逻辑而不依赖网络

    #[test]
    fn test_resolve_download_conflict_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, "content").unwrap();

        let result = ConflictResolver::resolve_download_conflict(
            &file_path,
            DownloadConflictStrategy::Overwrite,
        )
        .unwrap();

        assert_eq!(result, ConflictResolution::Proceed);
    }

    #[test]
    fn test_resolve_download_conflict_skip_existing() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, "content").unwrap();

        let result =
            ConflictResolver::resolve_download_conflict(&file_path, DownloadConflictStrategy::Skip)
                .unwrap();

        assert_eq!(result, ConflictResolution::Skip);
    }

    #[test]
    fn test_resolve_download_conflict_skip_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        let result =
            ConflictResolver::resolve_download_conflict(&file_path, DownloadConflictStrategy::Skip)
                .unwrap();

        assert_eq!(result, ConflictResolution::Proceed);
    }

    #[test]
    fn test_resolve_download_conflict_auto_rename_existing() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, "content").unwrap();

        let result = ConflictResolver::resolve_download_conflict(
            &file_path,
            DownloadConflictStrategy::AutoRename,
        )
        .unwrap();

        match result {
            ConflictResolution::UseNewPath(new_path) => {
                assert!(new_path.contains("test (1).txt"));
            }
            _ => panic!("Expected UseNewPath"),
        }
    }

    #[test]
    fn test_resolve_download_conflict_auto_rename_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        let result = ConflictResolver::resolve_download_conflict(
            &file_path,
            DownloadConflictStrategy::AutoRename,
        )
        .unwrap();

        assert_eq!(result, ConflictResolution::Proceed);
    }

    #[test]
    fn test_resolve_download_conflict_auto_rename_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, "content").unwrap();
        std::fs::write(temp_dir.path().join("test (1).txt"), "content").unwrap();
        std::fs::write(temp_dir.path().join("test (2).txt"), "content").unwrap();

        let result = ConflictResolver::resolve_download_conflict(
            &file_path,
            DownloadConflictStrategy::AutoRename,
        )
        .unwrap();

        match result {
            ConflictResolution::UseNewPath(new_path) => {
                assert!(new_path.contains("test (3).txt"));
            }
            _ => panic!("Expected UseNewPath"),
        }
    }

    // 注意：上传冲突解决的测试需要 NetdiskClient，这些测试应该是集成测试
    // 或者需要一个 mock 框架。由于时间限制，我们先实现下载相关的单元测试
    // 上传相关的测试可以在集成测试中完成
}

#[test]
fn test_root_path_parsing() {
    // 测试根目录路径解析逻辑
    let path = "/file.txt";
    let is_absolute = path.starts_with('/');
    assert!(is_absolute);

    let parts: Vec<&str> = path.rsplitn(2, '/').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "file.txt"); // filename
    assert_eq!(parts[1], ""); // parent (empty for root)
}

#[test]
fn test_nested_path_parsing() {
    // 测试嵌套路径解析逻辑
    let path = "/path/to/file.txt";
    let parts: Vec<&str> = path.rsplitn(2, '/').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "file.txt"); // filename
    assert_eq!(parts[1], "/path/to"); // parent
}

#[test]
fn test_relative_path_parsing() {
    // 测试相对路径解析逻辑
    let path = "file.txt";
    let is_absolute = path.starts_with('/');
    assert!(!is_absolute);

    let parts: Vec<&str> = path.rsplitn(2, '/').collect();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0], "file.txt");
}
