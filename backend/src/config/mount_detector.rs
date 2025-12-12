// 挂载点检测模块（适用于 Linux/Docker 环境）

use std::path::Path;

/// 挂载点信息
#[derive(Debug, Clone)]
pub struct MountPoint {
    /// 挂载点路径
    pub path: String,
    /// 文件系统类型
    pub fs_type: String,
    /// 设备名称
    pub device: String,
}

/// 挂载点检测器
pub struct MountDetector;

impl MountDetector {
    /// 获取所有非系统挂载点
    ///
    /// 读取 /proc/mounts 文件并过滤系统挂载点
    ///
    /// # 返回值
    /// - Vec<MountPoint>: 用户挂载的路径列表
    pub fn get_mount_points() -> Vec<MountPoint> {
        #[cfg(target_os = "linux")]
        {
            use std::collections::HashSet;
            use std::fs;

            let mut mounts_linux: Vec<MountPoint> = Vec::new();

            if let Ok(content) = fs::read_to_string("/proc/mounts") {
                // 系统路径列表（这些路径通常是系统自动挂载的）
                let system_paths: HashSet<&str> = [
                    "/proc", "/sys", "/dev", "/run", "/tmp", "/var", "/usr", "/bin", "/sbin",
                    "/lib", "/lib64", "/boot", "/root", "/home", "/etc", "/opt",
                ]
                    .iter()
                    .cloned()
                    .collect();

                for line in content.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 3 {
                        let device = parts[0];
                        let mount_point = parts[1];
                        let fs_type = parts[2];

                        // 过滤系统路径和特殊文件系统
                        let is_system_path = system_paths.iter().any(|&sys_path| {
                            mount_point == sys_path
                                || mount_point.starts_with(&format!("{}/", sys_path))
                        });

                        // 过滤特殊文件系统类型
                        let is_special_fs = matches!(
                            fs_type,
                            "proc"
                                | "sysfs"
                                | "devpts"
                                | "tmpfs"
                                | "cgroup"
                                | "cgroup2"
                                | "mqueue"
                                | "hugetlbfs"
                                | "devtmpfs"
                                | "securityfs"
                                | "pstore"
                                | "bpf"
                                | "tracefs"
                                | "debugfs"
                                | "fusectl"
                                | "configfs"
                        );

                        if !is_system_path && !is_special_fs {
                            mounts_linux.push(MountPoint {
                                path: mount_point.to_string(),
                                fs_type: fs_type.to_string(),
                                device: device.to_string(),
                            });
                        }
                    }
                }
            }

            return mounts_linux;
        }

        #[cfg(not(target_os = "linux"))]
        {
            Vec::new()
        }
    }

    /// 检测路径是否是挂载点或其子路径
    ///
    /// # 参数
    /// - path: 要检查的路径
    ///
    /// # 返回值
    /// - true: 路径是挂载点或挂载点的子路径
    /// - false: 路径不是挂载点
    pub fn is_mount_point(path: &Path) -> bool {
        let mount_points = Self::get_mount_points();
        let path_str = path.to_string_lossy();

        mount_points.iter().any(|mount| {
            // 完全匹配或者是挂载点的子路径
            mount.path == path_str.as_ref() || path_str.starts_with(&format!("{}/", mount.path))
        })
    }

    /// 检测路径是否是挂载点（精确匹配）
    ///
    /// # 参数
    /// - path: 要检查的路径
    ///
    /// # 返回值
    /// - true: 路径是挂载点
    /// - false: 路径不是挂载点
    pub fn is_exact_mount_point(path: &Path) -> bool {
        let mount_points = Self::get_mount_points();
        let path_str = path.to_string_lossy();

        mount_points
            .iter()
            .any(|mount| mount.path == path_str.as_ref())
    }

    /// 查找路径所在的挂载点
    ///
    /// # 参数
    /// - path: 要查询的路径
    ///
    /// # 返回值
    /// - Some(MountPoint): 找到的挂载点信息
    /// - None: 路径不在任何挂载点下
    pub fn find_mount_point_for_path(path: &Path) -> Option<MountPoint> {
        let mount_points = Self::get_mount_points();
        let path_str = path.to_string_lossy();

        // 找到最长匹配的挂载点
        mount_points
            .into_iter()
            .filter(|mount| {
                mount.path == path_str.as_ref() || path_str.starts_with(&format!("{}/", mount.path))
            })
            .max_by_key(|mount| mount.path.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_mount_points() {
        let mounts = MountDetector::get_mount_points();

        // 在非 Linux 系统上应该返回空列表
        #[cfg(not(target_os = "linux"))]
        assert_eq!(mounts.len(), 0);

        // 在 Linux 系统上应该至少有一个挂载点（根目录 /）
        // 注意：在某些容器环境中可能会被过滤掉
        #[cfg(target_os = "linux")]
        {
            // 只验证函数能正常运行
            // 不验证具体数量，因为不同环境下挂载点数量不同
            println!("Found {} mount points:", mounts.len());
            for mount in &mounts {
                println!("  {} ({}) on {}", mount.path, mount.fs_type, mount.device);
            }
        }
    }

    #[test]
    fn test_is_mount_point() {
        // 测试根路径
        let root_path = Path::new("/");
        // 根路径可能是挂载点，也可能不是（取决于环境）
        let _ = MountDetector::is_mount_point(root_path);

        // 测试明确不存在的路径
        let fake_path = Path::new("/definitely/not/a/mount/point/12345");
        // 这个测试只验证函数能正常运行
        let _ = MountDetector::is_mount_point(fake_path);
    }

    #[test]
    fn test_is_exact_mount_point() {
        // 测试根路径
        let root_path = Path::new("/");
        let _ = MountDetector::is_exact_mount_point(root_path);

        // 测试子路径（不应该是精确挂载点）
        let sub_path = Path::new("/app/downloads/test");
        let result = MountDetector::is_exact_mount_point(sub_path);

        // 大多数情况下，这个深度的路径不会是精确挂载点
        println!("Is exact mount point: {}", result);
    }

    #[test]
    fn test_find_mount_point_for_path() {
        // 测试 /app/downloads 路径
        let test_path = Path::new("/app/downloads");
        let mount_point = MountDetector::find_mount_point_for_path(test_path);

        if let Some(mp) = mount_point {
            println!(
                "Found mount point for /app/downloads: {} ({})",
                mp.path, mp.fs_type
            );
        } else {
            println!("No mount point found for /app/downloads");
        }
    }

    #[test]
    fn test_mount_point_filtering() {
        let mounts = MountDetector::get_mount_points();

        // 验证系统路径已被过滤
        for mount in &mounts {
            // 确保不包含明显的系统路径
            assert!(!mount.path.starts_with("/proc"));
            assert!(!mount.path.starts_with("/sys"));
            assert!(!mount.path.starts_with("/dev"));

            // 确保不是特殊文件系统
            assert!(!matches!(
                mount.fs_type.as_str(),
                "proc" | "sysfs" | "devpts" | "tmpfs" | "cgroup" | "cgroup2"
            ));
        }

        println!("All system paths successfully filtered");
    }
}
