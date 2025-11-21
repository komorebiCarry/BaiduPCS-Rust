// 路径验证模块

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// 路径验证结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathValidationResult {
    /// 路径是否完全可用
    pub valid: bool,
    /// 路径是否存在
    pub exists: bool,
    /// 路径是否可写
    pub is_writable: bool,
    /// 是否是目录
    pub is_directory: bool,
    /// Docker 环境下是否是挂载点
    pub is_mount: bool,
    /// 验证消息
    pub message: String,
    /// 详细错误信息（如果有）
    pub details: Option<String>,
}

impl PathValidationResult {
    /// 创建一个成功的验证结果
    pub fn success(is_mount: bool) -> Self {
        Self {
            valid: true,
            exists: true,
            is_writable: true,
            is_directory: true,
            is_mount,
            message: "路径验证通过".to_string(),
            details: None,
        }
    }

    /// 创建一个失败的验证结果
    pub fn failure(message: String, details: Option<String>) -> Self {
        Self {
            valid: false,
            exists: false,
            is_writable: false,
            is_directory: false,
            is_mount: false,
            message,
            details,
        }
    }
}

/// 路径验证器
pub struct PathValidator;

impl PathValidator {
    /// 验证路径是否可用于下载
    ///
    /// 执行以下检查：
    /// 1. 路径是否存在
    /// 2. 路径是否为目录
    /// 3. 路径是否可写
    /// 4. Docker 环境下是否是挂载点（可选）
    ///
    /// # 参数
    /// - path: 要验证的路径
    ///
    /// # 返回值
    /// - PathValidationResult: 详细的验证结果
    pub fn validate(path: &Path) -> PathValidationResult {
        Self::validate_with_docker_check(path, false)
    }

    /// 验证路径（带 Docker 环境检查）
    ///
    /// # 参数
    /// - path: 要验证的路径
    /// - is_docker: 是否在 Docker 环境中
    ///
    /// # 返回值
    /// - PathValidationResult: 详细的验证结果
    pub fn validate_with_docker_check(path: &Path, is_docker: bool) -> PathValidationResult {
        // 1. 检查路径是否存在
        if !path.exists() {
            return PathValidationResult::failure(
                "路径不存在".to_string(),
                Some(format!("路径 {:?} 不存在，请确保路径正确或先创建该目录", path)),
            );
        }

        // 2. 检查是否是目录
        if !path.is_dir() {
            return PathValidationResult {
                valid: false,
                exists: true,
                is_writable: false,
                is_directory: false,
                is_mount: false,
                message: "路径不是目录".to_string(),
                details: Some(format!("路径 {:?} 不是一个目录，请指定目录路径", path)),
            };
        }

        // 3. 检查是否可写
        let is_writable = Self::check_writable(path);
        if !is_writable {
            return PathValidationResult {
                valid: false,
                exists: true,
                is_writable: false,
                is_directory: true,
                is_mount: false,
                message: "路径不可写".to_string(),
                details: Some(format!(
                    "路径 {:?} 没有写入权限，请检查目录权限或使用其他目录",
                    path
                )),
            };
        }

        // 4. Docker 环境：检查是否是挂载点（可选，仅警告）
        let is_mount = if is_docker {
            use crate::config::MountDetector;
            MountDetector::is_mount_point(path)
        } else {
            false
        };

        if is_docker && !is_mount {
            // Docker 环境下路径不是挂载点，给出警告但不阻止使用
            tracing::warn!(
                "⚠️ Docker 环境检测到路径 {:?} 不是挂载点，数据可能无法持久化",
                path
            );
            tracing::warn!(
                "建议使用 docker run -v /host/path:{:?} 挂载外部目录",
                path
            );
        }

        // 验证通过
        PathValidationResult::success(is_mount)
    }

    /// 检查路径是否可写
    ///
    /// 通过创建临时文件的方式检测写入权限
    ///
    /// # 参数
    /// - path: 要检查的路径
    ///
    /// # 返回值
    /// - true: 路径可写
    /// - false: 路径不可写
    fn check_writable(path: &Path) -> bool {
        let test_file = path.join(".write_test");

        match fs::File::create(&test_file) {
            Ok(_) => {
                // 创建成功，删除测试文件
                let _ = fs::remove_file(&test_file);
                true
            }
            Err(_) => false,
        }
    }

    /// 验证路径并提供友好的错误信息
    ///
    /// # 参数
    /// - path: 要验证的路径
    ///
    /// # 返回值
    /// - Ok(()): 路径验证通过
    /// - Err: 验证失败，包含详细错误信息
    pub fn validate_or_error(path: &Path) -> Result<()> {
        let result = Self::validate(path);

        if !result.valid {
            let error_msg = if let Some(details) = result.details {
                format!("{}\n详情: {}", result.message, details)
            } else {
                result.message
            };

            anyhow::bail!(error_msg);
        }

        Ok(())
    }

    /// 自动创建目录（如果不存在）
    ///
    /// # 参数
    /// - path: 要创建的路径
    ///
    /// # 返回值
    /// - Ok(()): 目录创建成功或已存在
    /// - Err: 创建失败
    pub fn ensure_directory_exists(path: &Path) -> Result<()> {
        if !path.exists() {
            fs::create_dir_all(path)
                .with_context(|| format!("无法创建目录: {:?}", path))?;
            tracing::info!("自动创建下载目录: {:?}", path);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_validate_existing_directory() {
        // 创建临时目录
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();

        let result = PathValidator::validate(path);

        assert!(result.valid, "验证应该通过");
        assert!(result.exists, "路径应该存在");
        assert!(result.is_directory, "路径应该是目录");
        assert!(result.is_writable, "路径应该可写");
    }

    #[test]
    fn test_validate_non_existing_path() {
        let path = Path::new("/non/existing/path/12345");

        let result = PathValidator::validate(path);

        assert!(!result.valid, "验证应该失败");
        assert!(!result.exists, "路径不应该存在");
        assert_eq!(result.message, "路径不存在");
    }

    #[test]
    fn test_validate_file_instead_of_directory() {
        // 创建临时文件
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        fs::write(&file_path, "test").unwrap();

        let result = PathValidator::validate(&file_path);

        assert!(!result.valid, "验证应该失败");
        assert!(result.exists, "路径应该存在");
        assert!(!result.is_directory, "路径不应该是目录");
        assert_eq!(result.message, "路径不是目录");
    }

    #[test]
    fn test_check_writable() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();

        assert!(
            PathValidator::check_writable(path),
            "临时目录应该可写"
        );
    }

    #[test]
    fn test_ensure_directory_exists() {
        let temp_dir = TempDir::new().unwrap();
        let new_dir = temp_dir.path().join("new_directory");

        // 目录不存在
        assert!(!new_dir.exists());

        // 创建目录
        let result = PathValidator::ensure_directory_exists(&new_dir);
        assert!(result.is_ok(), "创建目录应该成功");
        assert!(new_dir.exists(), "目录应该已创建");

        // 再次调用应该成功（目录已存在）
        let result = PathValidator::ensure_directory_exists(&new_dir);
        assert!(result.is_ok(), "已存在的目录应该返回成功");
    }

    #[test]
    fn test_validate_or_error() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();

        // 有效路径应该成功
        let result = PathValidator::validate_or_error(path);
        assert!(result.is_ok());

        // 无效路径应该失败
        let invalid_path = Path::new("/non/existing/path");
        let result = PathValidator::validate_or_error(invalid_path);
        assert!(result.is_err());
    }
}
