// 文件系统服务
//
// 提供目录列表、路径校验等核心功能

use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{DateTime, Utc};

#[cfg(not(target_os = "windows"))]
use crate::config::{EnvDetector, MountDetector};

use super::guard::PathGuard;
use super::types::*;

/// 文件系统服务
pub struct FilesystemService {
    guard: PathGuard,
}

impl FilesystemService {
    /// 创建新的文件系统服务
    pub fn new(config: FilesystemConfig) -> Self {
        Self {
            guard: PathGuard::new(config),
        }
    }

    /// 列出目录内容（支持分页）
    pub fn list_directory(&self, req: &ListRequest) -> Result<ListResponse, FsError> {
        let path = self.guard.normalize(&req.path)?;

        // 检查是否为目录
        if !path.is_dir() {
            return Err(FsError::new(FsErrorCode::NotADirectory).with_path(req.path.clone()));
        }

        // 读取目录
        let read_dir = fs::read_dir(&path).map_err(|e| {
            tracing::error!("读取目录失败: {:?}, 错误: {}", path, e);
            FsError::new(FsErrorCode::DirectoryReadFailed)
                .with_path(path.to_string_lossy().to_string())
                .with_message(format!("读取目录失败: {}", e))
        })?;

        // 收集并过滤条目
        let mut entries: Vec<FileEntry> = read_dir
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let entry_path = entry.path();
                // 过滤隐藏文件
                if self.guard.is_hidden(&entry_path) {
                    return false;
                }
                // 过滤符号链接
                if self.guard.should_skip_symlink(&entry_path) {
                    return false;
                }
                true
            })
            .filter_map(|entry| self.to_file_entry(&entry).ok())
            .collect();

        let total = entries.len();

        // 排序
        self.sort_entries(&mut entries, &req.sort_field, &req.sort_order);

        // 分页
        let offset = req.page * req.page_size;
        let paginated: Vec<FileEntry> = entries
            .into_iter()
            .skip(offset)
            .take(req.page_size)
            .collect();

        // 计算父目录
        let parent_path = path
            .parent()
            .map(|p| {
                #[cfg(target_os = "windows")]
                {
                    // Windows 上，如果父目录是驱动器根目录的父目录，返回 None
                    if p.as_os_str().is_empty() {
                        return None;
                    }
                    Some(p.to_string_lossy().to_string())
                }
                #[cfg(not(target_os = "windows"))]
                {
                    Some(p.to_string_lossy().to_string())
                }
            })
            .flatten();

        Ok(ListResponse {
            entries: paginated,
            current_path: path.to_string_lossy().to_string(),
            parent_path,
            total,
            page: req.page,
            page_size: req.page_size,
            has_more: offset + req.page_size < total,
        })
    }

    /// 路径跳转（直达路径）
    pub fn goto_path(&self, req: &GotoRequest) -> GotoResponse {
        // 1. 规范化路径
        let resolved = match self.guard.normalize(&req.path) {
            Ok(p) => p,
            Err(e) => {
                return GotoResponse {
                    valid: false,
                    resolved_path: req.path.clone(),
                    entry_type: None,
                    message: Some(e.to_string()),
                };
            }
        };

        // 2. 检查路径是否存在
        if !resolved.exists() {
            return GotoResponse {
                valid: false,
                resolved_path: resolved.to_string_lossy().to_string(),
                entry_type: None,
                message: Some("路径不存在".to_string()),
            };
        }

        // 3. 返回成功结果
        let entry_type = if resolved.is_dir() {
            Some(EntryType::Directory)
        } else {
            Some(EntryType::File)
        };

        GotoResponse {
            valid: true,
            resolved_path: resolved.to_string_lossy().to_string(),
            entry_type,
            message: None,
        }
    }

    /// 校验路径有效性
    pub fn validate_path(&self, req: &ValidateRequest) -> ValidateResponse {
        // 尝试规范化路径
        let normalized = match self.guard.normalize(&req.path) {
            Ok(p) => p,
            Err(e) => {
                return ValidateResponse {
                    valid: false,
                    exists: false,
                    entry_type: None,
                    message: Some(e.to_string()),
                };
            }
        };

        let exists = normalized.exists();
        let actual_type = if normalized.is_dir() {
            Some(EntryType::Directory)
        } else if normalized.is_file() {
            Some(EntryType::File)
        } else {
            None
        };

        // 检查类型匹配
        let type_matches = match (&req.entry_type, &actual_type) {
            (Some(expected), Some(actual)) => expected == actual,
            (None, _) => true,
            (Some(_), None) => false,
        };

        if !type_matches {
            let expected_str = match &req.entry_type {
                Some(EntryType::File) => "文件",
                Some(EntryType::Directory) => "目录",
                None => "未知",
            };
            return ValidateResponse {
                valid: false,
                exists,
                entry_type: actual_type,
                message: Some(format!("期望类型为{}，但实际不是", expected_str)),
            };
        }

        ValidateResponse {
            valid: exists,
            exists,
            entry_type: actual_type,
            message: None,
        }
    }

    /// 获取单个文件/文件夹信息
    pub fn get_entry_info(&self, path: &str) -> Result<FileEntry, FsError> {
        let normalized = self.guard.normalize(path)?;

        let metadata = fs::metadata(&normalized)
            .map_err(|_| FsError::new(FsErrorCode::FileNotFound).with_path(path))?;

        let name = normalized
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| normalized.to_string_lossy().to_string());

        let entry_type = if metadata.is_dir() {
            EntryType::Directory
        } else {
            EntryType::File
        };

        let size = if metadata.is_file() {
            Some(metadata.len())
        } else {
            None
        };

        let created_at = metadata
            .created()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        let modified_at = metadata
            .modified()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        let icon = get_icon_for_extension(&normalized);

        Ok(FileEntry {
            id: self.path_to_id(&normalized),
            name,
            entry_type,
            size,
            created_at,
            updated_at: modified_at,
            icon,
            path: normalized.to_string_lossy().to_string(),
        })
    }

    /// 获取根目录列表（Windows 驱动器列表 / Unix 根目录）
    pub fn get_roots(&self) -> Result<Vec<FileEntry>, FsError> {
        #[cfg(target_os = "windows")]
        {
            self.get_windows_drives()
        }

        #[cfg(not(target_os = "windows"))]
        {
            self.get_unix_roots()
        }
    }

    /// Windows: 获取驱动器列表
    #[cfg(target_os = "windows")]
    fn get_windows_drives(&self) -> Result<Vec<FileEntry>, FsError> {
        let mut drives = Vec::new();

        // 遍历 A-Z 驱动器
        for letter in 'A'..='Z' {
            let drive_path = format!("{}:\\", letter);
            let path = PathBuf::from(&drive_path);

            if path.exists() {
                drives.push(FileEntry {
                    id: self.path_to_id(&path),
                    name: format!("本地磁盘 ({}:)", letter),
                    entry_type: EntryType::Directory,
                    size: None,
                    created_at: String::new(),
                    updated_at: String::new(),
                    icon: Some("drive".to_string()),
                    path: drive_path,
                });
            }
        }

        Ok(drives)
    }

    /// Unix: 获取根目录
    /// 在 Docker 环境下返回挂载目录列表
    #[cfg(not(target_os = "windows"))]
    fn get_unix_roots(&self) -> Result<Vec<FileEntry>, FsError> {
        // 检测是否在 Docker 环境中
        let env_info = EnvDetector::get_env_info();

        if env_info.is_docker {
            // Docker 环境：返回挂载目录列表
            return self.get_docker_mount_points();
        }

        // 非 Docker 环境：返回根目录
        let root = PathBuf::from("/");

        if !root.exists() {
            return Err(FsError::new(FsErrorCode::DirectoryNotFound).with_path("/"));
        }

        let metadata = fs::metadata(&root)
            .map_err(|_| FsError::new(FsErrorCode::DirectoryReadFailed).with_path("/"))?;

        let created_at = metadata
            .created()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        let modified_at = metadata
            .modified()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        Ok(vec![FileEntry {
            id: self.path_to_id(&root),
            name: "/".to_string(),
            entry_type: EntryType::Directory,
            size: None,
            created_at,
            updated_at: modified_at,
            icon: Some("drive".to_string()),
            path: "/".to_string(),
        }])
    }

    /// Docker 环境：获取挂载目录列表
    #[cfg(not(target_os = "windows"))]
    fn get_docker_mount_points(&self) -> Result<Vec<FileEntry>, FsError> {
        let mount_points = MountDetector::get_mount_points();

        // 如果没有检测到挂载点，至少返回 /app（常见的 Docker 工作目录）
        if mount_points.is_empty() {
            let app_path = PathBuf::from("/app");
            if app_path.exists() {
                return Ok(vec![self.create_mount_entry(&app_path, "应用目录")?]);
            }
            // 如果 /app 也不存在，返回根目录
            return Ok(vec![self.create_mount_entry(&PathBuf::from("/"), "根目录")?]);
        }

        // 转换挂载点为 FileEntry
        let mut entries: Vec<FileEntry> = Vec::new();
        for mount in mount_points {
            let path = PathBuf::from(&mount.path);
            if path.exists() {
                if let Ok(entry) = self.create_mount_entry(&path, &mount.path) {
                    entries.push(entry);
                }
            }
        }

        // 按路径排序
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(entries)
    }

    /// 创建挂载点条目
    #[cfg(not(target_os = "windows"))]
    fn create_mount_entry(&self, path: &PathBuf, display_name: &str) -> Result<FileEntry, FsError> {
        let metadata = fs::metadata(path).map_err(|_| {
            FsError::new(FsErrorCode::DirectoryReadFailed)
                .with_path(path.to_string_lossy().to_string())
        })?;

        let created_at = metadata
            .created()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        let modified_at = metadata
            .modified()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        Ok(FileEntry {
            id: self.path_to_id(path),
            name: display_name.to_string(),
            entry_type: EntryType::Directory,
            size: None,
            created_at,
            updated_at: modified_at,
            icon: Some("drive".to_string()),
            path: path.to_string_lossy().to_string(),
        })
    }

    /// 将 DirEntry 转换为 FileEntry
    fn to_file_entry(&self, entry: &DirEntry) -> Result<FileEntry, FsError> {
        let path = entry.path();
        let metadata = entry.metadata().map_err(|_| {
            FsError::new(FsErrorCode::PermissionDenied)
                .with_path(path.to_string_lossy().to_string())
        })?;

        let name = entry.file_name().to_string_lossy().to_string();

        let entry_type = if metadata.is_dir() {
            EntryType::Directory
        } else {
            EntryType::File
        };

        let size = if metadata.is_file() {
            Some(metadata.len())
        } else {
            None
        };

        let created_at = metadata
            .created()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        let modified_at = metadata
            .modified()
            .ok()
            .and_then(|t| self.system_time_to_iso8601(t))
            .unwrap_or_default();

        let icon = get_icon_for_extension(&path);

        Ok(FileEntry {
            id: self.path_to_id(&path),
            name,
            entry_type,
            size,
            created_at,
            updated_at: modified_at,
            icon,
            path: path.to_string_lossy().to_string(),
        })
    }

    /// 对条目进行排序
    fn sort_entries(&self, entries: &mut [FileEntry], field: &SortField, order: &SortOrder) {
        entries.sort_by(|a, b| {
            // 文件夹始终排在前面
            let dir_cmp = match (&a.entry_type, &b.entry_type) {
                (EntryType::Directory, EntryType::File) => return std::cmp::Ordering::Less,
                (EntryType::File, EntryType::Directory) => return std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            };

            if dir_cmp != std::cmp::Ordering::Equal {
                return dir_cmp;
            }

            // 按字段排序
            let cmp = match field {
                SortField::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
                SortField::Size => {
                    let a_size = a.size.unwrap_or(0);
                    let b_size = b.size.unwrap_or(0);
                    a_size.cmp(&b_size)
                }
                SortField::CreatedAt => a.created_at.cmp(&b.created_at),
                SortField::UpdatedAt => a.updated_at.cmp(&b.updated_at),
            };

            // 应用排序顺序
            match order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            }
        });
    }

    /// 将路径转换为唯一 ID（使用哈希）
    fn path_to_id(&self, path: &Path) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        path.to_string_lossy().hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// 将 SystemTime 转换为 ISO8601 字符串
    fn system_time_to_iso8601(&self, time: SystemTime) -> Option<String> {
        let datetime: DateTime<Utc> = time.into();
        Some(datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_new() {
        let _service = FilesystemService::new(FilesystemConfig::default());
        // 确保服务可以创建
        assert!(true);
    }

    #[test]
    fn test_get_roots() {
        let service = FilesystemService::new(FilesystemConfig::default());
        let roots = service.get_roots().unwrap();

        #[cfg(target_os = "windows")]
        {
            // Windows 上至少有一个驱动器（C:）
            assert!(!roots.is_empty());
            assert!(roots.iter().any(|r| r.path.contains("C:\\")));
        }

        #[cfg(not(target_os = "windows"))]
        {
            // Unix 上有根目录
            assert_eq!(roots.len(), 1);
            assert_eq!(roots[0].path, "/");
        }
    }

    #[test]
    fn test_list_current_directory() {
        let service = FilesystemService::new(FilesystemConfig::default());
        let current_dir = std::env::current_dir().unwrap();

        let req = ListRequest {
            path: current_dir.to_string_lossy().to_string(),
            page: 0,
            page_size: 100,
            sort_field: SortField::Name,
            sort_order: SortOrder::Asc,
        };

        let response = service.list_directory(&req);
        assert!(response.is_ok());

        let list = response.unwrap();
        assert_eq!(list.page, 0);
        // total 是 usize 类型，验证列表返回成功即可
        let _ = list.total;
    }
}
