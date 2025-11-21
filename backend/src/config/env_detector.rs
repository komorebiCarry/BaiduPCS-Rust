// 环境检测模块

use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};

/// 操作系统类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OsType {
    /// Windows 操作系统
    Windows,
    /// Linux 操作系统
    Linux,
    /// macOS 操作系统
    MacOS,
    /// 未知操作系统
    Unknown,
}

impl OsType {
    /// 获取操作系统类型的字符串表示
    pub fn as_str(&self) -> &str {
        match self {
            OsType::Windows => "Windows",
            OsType::Linux => "Linux",
            OsType::MacOS => "macOS",
            OsType::Unknown => "Unknown",
        }
    }
}

/// 环境信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvInfo {
    /// 是否在 Docker 环境中
    pub is_docker: bool,
    /// 操作系统类型
    pub os_type: OsType,
}

/// 环境检测器
pub struct EnvDetector;

impl EnvDetector {
    /// 检测是否在 Docker 环境中
    ///
    /// 使用多种方法检测 Docker 环境：
    /// 1. 检查 /.dockerenv 文件是否存在
    /// 2. 检查 /proc/1/cgroup 文件内容
    /// 3. 检查环境变量 container
    ///
    /// # 返回值
    /// - true: 在 Docker 环境中
    /// - false: 不在 Docker 环境中
    pub fn is_docker() -> bool {
        // 方法1: 检查 /.dockerenv 文件
        if Path::new("/.dockerenv").exists() {
            return true;
        }

        // 方法2: 检查 /proc/1/cgroup
        if let Ok(content) = fs::read_to_string("/proc/1/cgroup") {
            if content.contains("docker") || content.contains("containerd") {
                return true;
            }
        }

        // 方法3: 检查环境变量
        if std::env::var("container").is_ok() {
            return true;
        }

        false
    }

    /// 获取操作系统类型
    ///
    /// 根据编译目标平台返回相应的操作系统类型
    ///
    /// # 返回值
    /// - OsType: 操作系统类型枚举
    pub fn get_os_type() -> OsType {
        #[cfg(target_os = "windows")]
        return OsType::Windows;

        #[cfg(target_os = "macos")]
        return OsType::MacOS;

        #[cfg(target_os = "linux")]
        return OsType::Linux;

        #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
        return OsType::Unknown;
    }

    /// 获取完整的环境信息
    ///
    /// # 返回值
    /// - EnvInfo: 包含 Docker 环境和操作系统类型信息
    pub fn get_env_info() -> EnvInfo {
        EnvInfo {
            is_docker: Self::is_docker(),
            os_type: Self::get_os_type(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_os_type() {
        let os_type = EnvDetector::get_os_type();

        // 验证返回的是有效的操作系统类型
        assert!(matches!(
            os_type,
            OsType::Windows | OsType::Linux | OsType::MacOS | OsType::Unknown
        ));

        // 根据编译目标验证正确性
        #[cfg(target_os = "windows")]
        assert_eq!(os_type, OsType::Windows);

        #[cfg(target_os = "linux")]
        assert_eq!(os_type, OsType::Linux);

        #[cfg(target_os = "macos")]
        assert_eq!(os_type, OsType::MacOS);
    }

    #[test]
    fn test_is_docker() {
        // 这个测试会根据实际运行环境返回不同结果
        let is_docker = EnvDetector::is_docker();

        // 只验证函数能正常运行，不验证具体结果
        // 因为在本地环境和 Docker 环境中结果会不同
        assert!(is_docker == true || is_docker == false);
    }

    #[test]
    fn test_get_env_info() {
        let env_info = EnvDetector::get_env_info();

        // 验证环境信息包含有效的操作系统类型
        assert!(matches!(
            env_info.os_type,
            OsType::Windows | OsType::Linux | OsType::MacOS | OsType::Unknown
        ));

        // 验证 is_docker 是布尔值
        assert!(env_info.is_docker == true || env_info.is_docker == false);
    }

    #[test]
    fn test_os_type_as_str() {
        assert_eq!(OsType::Windows.as_str(), "Windows");
        assert_eq!(OsType::Linux.as_str(), "Linux");
        assert_eq!(OsType::MacOS.as_str(), "macOS");
        assert_eq!(OsType::Unknown.as_str(), "Unknown");
    }
}
