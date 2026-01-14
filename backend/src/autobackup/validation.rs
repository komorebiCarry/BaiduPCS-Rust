//! 备份配置冲突校验模块
//!
//! 防止以下冲突场景：
//! 1. 同方向重复任务（父/子目录导致重复扫描与重复上传/下载）
//! 2. 上传/下载闭环（A→B 与 B→A 组合导致周期性同步）
//!
//! ## 冲突判定规则
//!
//! - **对比范围**：与所有现存配置对比（不区分 enabled/disabled）
//! - **同方向冲突**：`LocalOverlap && RemoteOverlap` → 拒绝
//! - **反方向闭环**：`LocalOverlap && RemoteOverlap` → 拒绝
//!
//! ## 路径规范化规则
//!
//! - **本地路径**：Windows 下统一分隔符、去尾 `\`、大小写不敏感、按路径段边界比较
//! - **云端路径**：统一 `/` 分隔、去尾 `/`、压缩重复 `//`、按路径段边界比较

use std::path::Path;

use super::config::{BackupConfig, BackupDirection};

/// 冲突校验结果
#[derive(Debug, Clone)]
pub struct ConflictCheckResult {
    /// 是否存在冲突
    pub has_conflict: bool,
    /// 冲突类型
    pub conflict_type: Option<ConflictType>,
    /// 冲突的配置列表
    pub conflicting_configs: Vec<ConflictingConfig>,
    /// 用户友好的错误消息
    pub error_message: Option<String>,
}

impl ConflictCheckResult {
    /// 创建无冲突的结果
    pub fn ok() -> Self {
        Self {
            has_conflict: false,
            conflict_type: None,
            conflicting_configs: Vec::new(),
            error_message: None,
        }
    }

    /// 创建有冲突的结果
    pub fn conflict(
        conflict_type: ConflictType,
        conflicting_configs: Vec<ConflictingConfig>,
        error_message: String,
    ) -> Self {
        Self {
            has_conflict: true,
            conflict_type: Some(conflict_type),
            conflicting_configs,
            error_message: Some(error_message),
        }
    }
}

/// 冲突类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    /// 同方向冲突：父配置已存在，正在创建子配置
    SameDirectionParentExists,
    /// 同方向冲突：子配置已存在，正在创建父配置
    SameDirectionChildExists,
    /// 反方向闭环冲突：上传与下载形成闭环
    LoopConflict,
}

/// 冲突的配置信息
#[derive(Debug, Clone)]
pub struct ConflictingConfig {
    /// 配置 ID
    pub id: String,
    /// 配置名称
    pub name: String,
    /// 本地路径
    pub local_path: String,
    /// 云端路径
    pub remote_path: String,
    /// 备份方向
    pub direction: BackupDirection,
    /// 是否为父配置（相对于候选配置）
    pub is_parent: bool,
}

/// 路径重叠关系
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapRelation {
    /// 无重叠
    None,
    /// 完全相等
    Equal,
    /// A 是 B 的祖先（A 包含 B）
    AIsAncestor,
    /// A 是 B 的后代（B 包含 A）
    AIsDescendant,
}

impl OverlapRelation {
    /// 是否存在重叠（祖先/后代/相等）
    pub fn is_overlap(&self) -> bool {
        !matches!(self, OverlapRelation::None)
    }
}

// ==================== 路径规范化 ====================

/// 规范化本地路径
///
/// 规则：
/// 1. 尝试 canonicalize 获取绝对路径
/// 2. 失败则退化处理：统一分隔符为 `/`、去尾 `/`
/// 3. Windows 下转小写（大小写不敏感）
///
/// 注意：返回的路径用于比较，不用于实际文件操作
pub fn normalize_local_path(path: &Path) -> String {
    // 尝试 canonicalize
    let normalized = match path.canonicalize() {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => {
            // 退化处理
            path.to_string_lossy().to_string()
        }
    };

    // 统一分隔符为 /
    let normalized = normalized.replace('\\', "/");

    // 去尾 /
    let normalized = normalized.trim_end_matches('/').to_string();

    // Windows 下大小写不敏感
    #[cfg(windows)]
    let normalized = normalized.to_lowercase();

    normalized
}

/// 规范化云端路径
///
/// 规则：
/// 1. 统一为 `/` 分隔
/// 2. 去尾 `/`
/// 3. 压缩重复 `//`
/// 4. 确保以 `/` 开头
pub fn normalize_remote_path(path: &str) -> String {
    // 统一分隔符
    let normalized = path.replace('\\', "/");

    // 压缩重复 //
    let mut result = String::with_capacity(normalized.len());
    let mut prev_slash = false;
    for c in normalized.chars() {
        if c == '/' {
            if !prev_slash {
                result.push(c);
            }
            prev_slash = true;
        } else {
            result.push(c);
            prev_slash = false;
        }
    }

    // 去尾 /
    let result = result.trim_end_matches('/').to_string();

    // 确保以 / 开头
    if !result.starts_with('/') {
        format!("/{}", result)
    } else {
        result
    }
}

// ==================== 路径重叠判定 ====================

/// 判断两个本地路径的重叠关系
///
/// 按路径段边界比较，避免 `C:\A` 误判为 `C:\AA` 的祖先
pub fn check_local_overlap(path_a: &str, path_b: &str) -> OverlapRelation {
    let norm_a = normalize_local_path(Path::new(path_a));
    let norm_b = normalize_local_path(Path::new(path_b));

    check_path_overlap(&norm_a, &norm_b)
}

/// 判断两个云端路径的重叠关系
pub fn check_remote_overlap(path_a: &str, path_b: &str) -> OverlapRelation {
    let norm_a = normalize_remote_path(path_a);
    let norm_b = normalize_remote_path(path_b);

    check_path_overlap(&norm_a, &norm_b)
}

/// 通用路径重叠判定（已规范化的路径）
///
/// 按路径段边界比较：
/// - `/a/b` 是 `/a/b/c` 的祖先
/// - `/a/b` 不是 `/a/bc` 的祖先
fn check_path_overlap(norm_a: &str, norm_b: &str) -> OverlapRelation {
    if norm_a == norm_b {
        return OverlapRelation::Equal;
    }

    // 分割为路径段
    let segments_a: Vec<&str> = norm_a.split('/').filter(|s| !s.is_empty()).collect();
    let segments_b: Vec<&str> = norm_b.split('/').filter(|s| !s.is_empty()).collect();

    // 检查 A 是否是 B 的祖先
    if segments_a.len() < segments_b.len() {
        let is_ancestor = segments_a
            .iter()
            .zip(segments_b.iter())
            .all(|(a, b)| a == b);
        if is_ancestor {
            return OverlapRelation::AIsAncestor;
        }
    }

    // 检查 A 是否是 B 的后代
    if segments_a.len() > segments_b.len() {
        let is_descendant = segments_b
            .iter()
            .zip(segments_a.iter())
            .all(|(b, a)| a == b);
        if is_descendant {
            return OverlapRelation::AIsDescendant;
        }
    }

    OverlapRelation::None
}

// ==================== 冲突校验 ====================

/// 校验配置冲突
///
/// # 参数
/// - `candidate_local`: 候选配置的本地路径
/// - `candidate_remote`: 候选配置的云端路径
/// - `candidate_direction`: 候选配置的备份方向
/// - `existing_configs`: 所有现存配置
/// - `exclude_id`: 排除的配置 ID（更新时排除自身）
///
/// # 返回
/// - `ConflictCheckResult`: 校验结果，包含冲突类型和详细信息
pub fn validate_config_conflicts(
    candidate_local: &Path,
    candidate_remote: &str,
    candidate_direction: BackupDirection,
    existing_configs: &[BackupConfig],
    exclude_id: Option<&str>,
) -> ConflictCheckResult {
    let norm_candidate_local = normalize_local_path(candidate_local);
    let norm_candidate_remote = normalize_remote_path(candidate_remote);

    let mut conflicting_configs = Vec::new();
    let mut conflict_type: Option<ConflictType> = None;

    for config in existing_configs {
        // 排除自身（更新场景）
        if let Some(exclude) = exclude_id {
            if config.id == exclude {
                continue;
            }
        }

        let norm_existing_local = normalize_local_path(&config.local_path);
        let norm_existing_remote = normalize_remote_path(&config.remote_path);

        // 检查本地路径重叠
        let local_overlap = check_path_overlap(&norm_candidate_local, &norm_existing_local);

        // 检查云端路径重叠
        let remote_overlap = check_path_overlap(&norm_candidate_remote, &norm_existing_remote);

        // 只有同时重叠才构成冲突
        if !local_overlap.is_overlap() || !remote_overlap.is_overlap() {
            continue;
        }

        // 判断冲突类型
        let is_same_direction = candidate_direction == config.direction;

        if is_same_direction {
            // 同方向冲突
            // 判断是"父已存在建子"还是"子已存在建父"
            // 以本地路径为主判断（因为本地路径是扫描的起点）
            let is_parent = matches!(
                local_overlap,
                OverlapRelation::AIsDescendant | OverlapRelation::Equal
            );

            let current_conflict_type = if is_parent {
                ConflictType::SameDirectionParentExists
            } else {
                ConflictType::SameDirectionChildExists
            };

            // 记录冲突配置
            conflicting_configs.push(ConflictingConfig {
                id: config.id.clone(),
                name: config.name.clone(),
                local_path: config.local_path.to_string_lossy().to_string(),
                remote_path: config.remote_path.clone(),
                direction: config.direction,
                is_parent,
            });

            // 设置冲突类型（优先级：父存在 > 子存在）
            if conflict_type.is_none()
                || matches!(current_conflict_type, ConflictType::SameDirectionParentExists)
            {
                conflict_type = Some(current_conflict_type);
            }
        } else {
            // 反方向闭环冲突
            conflicting_configs.push(ConflictingConfig {
                id: config.id.clone(),
                name: config.name.clone(),
                local_path: config.local_path.to_string_lossy().to_string(),
                remote_path: config.remote_path.clone(),
                direction: config.direction,
                is_parent: false, // 闭环冲突不区分父子
            });

            conflict_type = Some(ConflictType::LoopConflict);
        }
    }

    if conflicting_configs.is_empty() {
        return ConflictCheckResult::ok();
    }

    // 生成错误消息
    let error_message = generate_conflict_message(
        conflict_type.unwrap(),
        &conflicting_configs,
        candidate_direction,
    );

    ConflictCheckResult::conflict(conflict_type.unwrap(), conflicting_configs, error_message)
}

/// 生成冲突错误消息
///
/// 根据冲突类型生成差异化的用户友好错误消息
fn generate_conflict_message(
    conflict_type: ConflictType,
    conflicting_configs: &[ConflictingConfig],
    candidate_direction: BackupDirection,
) -> String {
    let direction_str = match candidate_direction {
        BackupDirection::Upload => "上传备份",
        BackupDirection::Download => "下载备份",
    };

    match conflict_type {
        ConflictType::SameDirectionParentExists => {
            // 父配置已存在，正在创建子配置
            let parent = conflicting_configs
                .iter()
                .find(|c| c.is_parent)
                .unwrap_or(&conflicting_configs[0]);

            format!(
                "配置冲突：已存在父级{}配置「{}」(ID: {})\n\
                 - 本地路径: {}\n\
                 - 云端路径: {}\n\n\
                 父配置已覆盖该范围，创建子配置会导致重复{}。\n\
                 建议：使用现有父配置，或先删除/调整父配置的路径范围。",
                direction_str,
                parent.name,
                parent.id,
                parent.local_path,
                parent.remote_path,
                if candidate_direction == BackupDirection::Upload {
                    "扫描和上传"
                } else {
                    "扫描和下载"
                }
            )
        }
        ConflictType::SameDirectionChildExists => {
            // 子配置已存在，正在创建父配置
            let children: Vec<_> = conflicting_configs.iter().filter(|c| !c.is_parent).collect();
            let children_list: String = children
                .iter()
                .map(|c| format!("  - 「{}」(ID: {}, 本地: {}, 云端: {})", c.name, c.id, c.local_path, c.remote_path))
                .collect::<Vec<_>>()
                .join("\n");

            format!(
                "配置冲突：存在 {} 个子目录{}配置与当前配置路径重叠：\n{}\n\n\
                 创建父配置会与这些子配置产生重复{}。\n\
                 建议：先禁用/删除/调整上述子配置的路径，再创建父配置。",
                children.len(),
                direction_str,
                children_list,
                if candidate_direction == BackupDirection::Upload {
                    "扫描和上传"
                } else {
                    "扫描和下载"
                }
            )
        }
        ConflictType::LoopConflict => {
            // 上传/下载闭环冲突
            let opposite_direction = match candidate_direction {
                BackupDirection::Upload => "下载备份",
                BackupDirection::Download => "上传备份",
            };

            let configs_list: String = conflicting_configs
                .iter()
                .map(|c| {
                    let dir = match c.direction {
                        BackupDirection::Upload => "上传",
                        BackupDirection::Download => "下载",
                    };
                    format!(
                        "  - 「{}」(ID: {}, 方向: {}, 本地: {}, 云端: {})",
                        c.name, c.id, dir, c.local_path, c.remote_path
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            format!(
                "配置冲突：当前{}配置与以下{}配置形成闭环：\n{}\n\n\
                 上传和下载配置的本地路径与云端路径同时重叠，会导致文件在本地和云端之间循环同步。\n\
                 建议：\n\
                 1. 保留单向备份（删除其中一个方向的配置）\n\
                 2. 调整本地或云端路径，使其不再重叠",
                direction_str,
                opposite_direction,
                configs_list
            )
        }
    }
}

/// 快捷校验函数：用于 create_config
pub fn validate_for_create(
    local_path: &Path,
    remote_path: &str,
    direction: BackupDirection,
    existing_configs: &[BackupConfig],
) -> ConflictCheckResult {
    validate_config_conflicts(local_path, remote_path, direction, existing_configs, None)
}

/// 快捷校验函数：用于 update_config
pub fn validate_for_update(
    config_id: &str,
    local_path: &Path,
    remote_path: &str,
    direction: BackupDirection,
    existing_configs: &[BackupConfig],
) -> ConflictCheckResult {
    validate_config_conflicts(
        local_path,
        remote_path,
        direction,
        existing_configs,
        Some(config_id),
    )
}

/// 快捷校验函数：用于 trigger_backup 和 execute_backup_for_config
///
/// 执行前再次校验，防止配置在创建后被其他配置覆盖
pub fn validate_for_execute(
    config: &BackupConfig,
    existing_configs: &[BackupConfig],
) -> ConflictCheckResult {
    validate_config_conflicts(
        &config.local_path,
        &config.remote_path,
        config.direction,
        existing_configs,
        Some(&config.id),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use super::*;

    #[test]
    fn test_normalize_local_path() {
        // Windows 路径
        let path = Path::new("C:\\Users\\test\\Documents");
        let normalized = normalize_local_path(path);
        #[cfg(windows)]
        assert!(normalized.contains("users/test/documents") || normalized.ends_with("users/test/documents"));

        // 去尾斜杠
        let path = Path::new("/home/user/");
        let normalized = normalize_local_path(path);
        assert!(!normalized.ends_with('/'));
    }

    #[test]
    fn test_normalize_remote_path() {
        assert_eq!(normalize_remote_path("/backup/docs"), "/backup/docs");
        assert_eq!(normalize_remote_path("/backup/docs/"), "/backup/docs");
        assert_eq!(normalize_remote_path("backup/docs"), "/backup/docs");
        assert_eq!(normalize_remote_path("/backup//docs"), "/backup/docs");
        assert_eq!(normalize_remote_path("\\backup\\docs"), "/backup/docs");
    }

    #[test]
    fn test_path_overlap() {
        // 相等
        assert_eq!(
            check_path_overlap("/a/b", "/a/b"),
            OverlapRelation::Equal
        );

        // A 是 B 的祖先
        assert_eq!(
            check_path_overlap("/a/b", "/a/b/c"),
            OverlapRelation::AIsAncestor
        );

        // A 是 B 的后代
        assert_eq!(
            check_path_overlap("/a/b/c", "/a/b"),
            OverlapRelation::AIsDescendant
        );

        // 无重叠（路径段边界）
        assert_eq!(
            check_path_overlap("/a/b", "/a/bc"),
            OverlapRelation::None
        );

        // 无重叠（完全不同）
        assert_eq!(
            check_path_overlap("/a/b", "/c/d"),
            OverlapRelation::None
        );
    }

    #[test]
    fn test_same_direction_conflict() {
        let existing = vec![BackupConfig {
            id: "config-1".to_string(),
            name: "父配置".to_string(),
            local_path: PathBuf::from("/home/user/documents"),
            remote_path: "/backup/documents".to_string(),
            direction: BackupDirection::Upload,
            watch_config: Default::default(),
            poll_config: Default::default(),
            filter_config: Default::default(),
            encrypt_enabled: false,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        // 创建子配置应该冲突
        let result = validate_for_create(
            Path::new("/home/user/documents/work"),
            "/backup/documents/work",
            BackupDirection::Upload,
            &existing,
        );

        assert!(result.has_conflict);
        assert_eq!(
            result.conflict_type,
            Some(ConflictType::SameDirectionParentExists)
        );
    }

    #[test]
    fn test_loop_conflict() {
        let existing = vec![BackupConfig {
            id: "config-1".to_string(),
            name: "上传配置".to_string(),
            local_path: PathBuf::from("/home/user/sync"),
            remote_path: "/cloud/sync".to_string(),
            direction: BackupDirection::Upload,
            watch_config: Default::default(),
            poll_config: Default::default(),
            filter_config: Default::default(),
            encrypt_enabled: false,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        // 创建反向下载配置应该冲突（闭环）
        let result = validate_for_create(
            Path::new("/home/user/sync"),
            "/cloud/sync",
            BackupDirection::Download,
            &existing,
        );

        assert!(result.has_conflict);
        assert_eq!(result.conflict_type, Some(ConflictType::LoopConflict));
    }

    #[test]
    fn test_no_conflict_different_paths() {
        let existing = vec![BackupConfig {
            id: "config-1".to_string(),
            name: "配置1".to_string(),
            local_path: PathBuf::from("/home/user/documents"),
            remote_path: "/backup/documents".to_string(),
            direction: BackupDirection::Upload,
            watch_config: Default::default(),
            poll_config: Default::default(),
            filter_config: Default::default(),
            encrypt_enabled: false,
            enabled: true,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        // 本地路径相同但云端路径不同，不冲突
        let result = validate_for_create(
            Path::new("/home/user/documents"),
            "/backup/documents2",
            BackupDirection::Upload,
            &existing,
        );

        assert!(!result.has_conflict);

        // 云端路径相同但本地路径不同，不冲突
        let result = validate_for_create(
            Path::new("/home/user/documents2"),
            "/backup/documents",
            BackupDirection::Upload,
            &existing,
        );

        assert!(!result.has_conflict);
    }
}
