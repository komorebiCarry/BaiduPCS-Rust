// 分享直下临时目录：路径布局、实例命名空间与孤儿判定规则（issue #162）
//
// 分享直下 / 分享同步（仅同步到本地）会把文件先转存到网盘
// `{temp_root}/{uuid}/` 中转，下载完再删。这里集中放「哪些目录可以删」的纯规则，
// 网络与任务表的访问留在 `TransferManager`，便于单测覆盖。
//
// 目录布局：
// - 新任务：`{temp_root}/inst-{实例id}/{uuid}/`，实例 id 持久化在 `config/instance_id`；
//   同一百度账号挂在多个实例上时，自动清理只动本实例自己的命名空间。
// - 旧版本（或实例 id 不可用时）：`{temp_root}/{uuid}/`，称为「平铺目录」，只在
//   启动清理 / 手动清理时处理。
//
// 孤儿判定（必须同时满足）：
// 1. 本进程任务恢复已完成；
// 2. 目录名是 UUID；
// 3. 目录创建时间超过最小年龄；
// 4. 不被任何「仍在用」的转存任务、下载任务、文件夹下载引用（内存 + 磁盘持久化）。

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use tracing::{info, warn};

/// 实例命名空间目录名前缀
pub const NAMESPACE_PREFIX: &str = "inst-";

/// 自动周期清理：目录创建后至少这么久才可能被删
pub const PERIODIC_MIN_DIR_AGE_SECS: i64 = 6 * 3600;

/// 启动清理 / 手动清理：目录创建后至少这么久才可能被删
///
/// 白名单来自内存 + 持久化记录，且「先列目录、后收集白名单」，新建目录不会成为候选；
/// 这个门槛只是额外挡一下极端竞态，所以可以比自动清理短得多。
pub const MANUAL_MIN_DIR_AGE_SECS: i64 = 10 * 60;

/// 已结束（完成 / 失败）的转存任务在最后一次更新后仍保护其临时目录的时长
///
/// 用于挡住「状态刚落终态、后续动作（如重试下载）尚未登记」的过渡窗口。
pub const TERMINAL_GRACE_SECS: i64 = 3600;

static INSTANCE_ID: OnceLock<String> = OnceLock::new();
static RECOVERY_DONE: AtomicBool = AtomicBool::new(false);

const INSTANCE_ID_FILE: &str = "instance_id";

/// 实例 id 是否合法：8~32 位小写十六进制
fn is_valid_instance_id(s: &str) -> bool {
    (8..=32).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// 读取或生成实例 id（持久化在 `{config_dir}/instance_id`）
///
/// 复制整个 config 目录起第二个实例时两边 id 相同：若两个实例登录同一百度账号，
/// 周期清理可能删掉对方运行超过 6 小时的临时目录。这种情况先删掉副本里的
/// `instance_id` 再启动即可。
///
/// 写入失败时不启用命名空间（返回 None），新任务退回平铺布局：若每次重启都换一个
/// 新 id，上一个命名空间里的残留会变成「其他实例」目录，自动清理永远碰不到它。
pub fn init_instance_id(config_dir: &Path) -> Option<&'static str> {
    if let Some(id) = INSTANCE_ID.get() {
        return Some(id.as_str());
    }

    let file = config_dir.join(INSTANCE_ID_FILE);
    let existing = std::fs::read_to_string(&file)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| is_valid_instance_id(s));

    let id = match existing {
        Some(id) => id,
        None => {
            let id = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
            if let Err(e) = std::fs::create_dir_all(config_dir)
                .and_then(|_| std::fs::write(&file, &id))
            {
                warn!(
                    "写入实例 id 失败，分享直下临时目录退回平铺布局（不启用自动孤儿清理）: {:?}, error={}",
                    file, e
                );
                return None;
            }
            info!("已生成实例 id: {}（{:?}）", id, file);
            id
        }
    };

    let _ = INSTANCE_ID.set(id);
    INSTANCE_ID.get().map(|s| s.as_str())
}

/// 本实例的命名空间目录名（`inst-{id}`）；实例 id 不可用时为 None
pub fn own_namespace() -> Option<String> {
    INSTANCE_ID.get().map(|id| format!("{}{}", NAMESPACE_PREFIX, id))
}

/// 标记本进程任务恢复已完成（孤儿清理的前置条件）
pub fn mark_recovery_done() {
    RECOVERY_DONE.store(true, Ordering::SeqCst);
}

/// 本进程任务恢复是否已完成
pub fn recovery_done() -> bool {
    RECOVERY_DONE.load(Ordering::SeqCst)
}

/// 去掉末尾 `/`（根 `/` 保持不变）
pub fn normalize_dir(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() && path.starts_with('/') {
        "/"
    } else {
        trimmed
    }
}

/// 是否为标准连字符格式的 UUID（任务临时目录名）
pub fn is_uuid_segment(s: &str) -> bool {
    s.len() == 36 && uuid::Uuid::parse_str(s).is_ok()
}

/// 是否为实例命名空间目录名
pub fn is_namespace_segment(s: &str) -> bool {
    s.strip_prefix(NAMESPACE_PREFIX)
        .map(is_valid_instance_id)
        .unwrap_or(false)
}

/// 构造任务临时目录：`{root}/{namespace}/{uuid}/` 或 `{root}/{uuid}/`
pub fn build_task_temp_dir(root: &str, namespace: Option<&str>, task_uuid: &str) -> String {
    let root = root.trim_end_matches('/');
    match namespace {
        Some(ns) => format!("{}/{}/{}/", root, ns, task_uuid),
        None => format!("{}/{}/", root, task_uuid),
    }
}

/// 配置的临时根目录是否安全（绝对路径且不是 `/`）
pub fn validate_temp_root(root: &str) -> Result<&str, String> {
    let root_trimmed = root.trim_end_matches('/');
    if root_trimmed.len() < 2 || !root_trimmed.starts_with('/') {
        return Err(format!(
            "配置的临时目录根不安全（过短或非绝对路径）: {}",
            root
        ));
    }
    Ok(root_trimmed)
}

/// 校验任务临时目录：必须是 `{root}/{uuid}` 或 `{root}/inst-{id}/{uuid}`
///
/// 末级必须是 UUID —— 防止畸形路径（如 `{root}/inst-x/`）把整个命名空间删掉。
pub fn validate_task_temp_dir(temp_dir: &str, root: &str) -> Result<(), String> {
    let root_trimmed = validate_temp_root(root)?;
    let dir = temp_dir.trim_end_matches('/');

    let rel = dir
        .strip_prefix(root_trimmed)
        .and_then(|rest| rest.strip_prefix('/'))
        .ok_or_else(|| "路径不在配置的临时目录根下".to_string())?;

    let segments: Vec<&str> = rel.split('/').collect();
    let ok = match segments.as_slice() {
        [task] => is_uuid_segment(task),
        [ns, task] => is_namespace_segment(ns) && is_uuid_segment(task),
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err("路径格式不正确：不是 {根}/{uuid} 或 {根}/inst-{id}/{uuid}".to_string())
    }
}

/// 把 `path` 及其位于 `root` 之下的每一级上级目录加入保护集合
///
/// 例如 root=`/t`、path=`/t/inst-a/u1/x/y.mp4` 会保护 `/t/inst-a`、`/t/inst-a/u1`、
/// `/t/inst-a/u1/x`、`/t/inst-a/u1/x/y.mp4`。这样无论引用的是任务目录本身，还是
/// 其中的某个文件（下载任务的 remote_path），都能命中候选目录。
pub fn protect_path(set: &mut HashSet<String>, root: &str, path: &str) {
    let root_trimmed = root.trim_end_matches('/');
    let path = path.trim_end_matches('/');
    let Some(rel) = path
        .strip_prefix(root_trimmed)
        .and_then(|rest| rest.strip_prefix('/'))
    else {
        return;
    };

    let mut current = root_trimmed.to_string();
    for seg in rel.split('/').filter(|s| !s.is_empty()) {
        current.push('/');
        current.push_str(seg);
        set.insert(current.clone());
    }
}

/// 转存任务是否「已结束」：完成或失败
///
/// `Transferred` 刻意不算已结束：重启恢复时状态为 transferred 的分享直下任务会
/// 重新创建下载子任务，期间临时目录必须保留。
pub fn is_finished_transfer_status(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().as_str(),
        "completed" | "transferfailed" | "transfer_failed" | "downloadfailed" | "download_failed"
    )
}

/// 转存任务是否仍保护它的临时目录
pub fn transfer_protects_temp_dir(finished: bool, updated_at_secs: i64, now_secs: i64) -> bool {
    !finished || now_secs.saturating_sub(updated_at_secs) < TERMINAL_GRACE_SECS
}

/// 根目录下一项的归类
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootEntry {
    /// 旧版本平铺的任务目录 `{root}/{uuid}`
    LegacyTask,
    /// 本实例命名空间
    OwnNamespace,
    /// 其他实例的命名空间
    ForeignNamespace,
    /// 其他（不是本程序创建的，一律不碰）
    Other,
}

/// 归类临时根目录下的一个子目录
pub fn classify_root_entry(name: &str, own_namespace: Option<&str>) -> RootEntry {
    if is_uuid_segment(name) {
        RootEntry::LegacyTask
    } else if own_namespace == Some(name) {
        RootEntry::OwnNamespace
    } else if is_namespace_segment(name) {
        RootEntry::ForeignNamespace
    } else {
        RootEntry::Other
    }
}

/// 孤儿临时目录清理的范围与门槛
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanSweepOptions {
    /// 旧版本平铺的 `{root}/{uuid}` 目录
    pub include_legacy: bool,
    /// 本实例命名空间下的目录
    pub include_own_namespace: bool,
    /// 其他实例命名空间下的目录 —— 可能是另一实例正在用的，只在用户明确要求时开启
    pub include_foreign_namespaces: bool,
    /// 目录创建后至少多久才可删
    pub min_dir_age_secs: i64,
}

impl OrphanSweepOptions {
    /// 手动 / 启动清理：平铺目录 + 本实例命名空间（可选连同其他实例）
    pub fn manual(include_foreign_namespaces: bool) -> Self {
        Self {
            include_legacy: true,
            include_own_namespace: true,
            include_foreign_namespaces,
            min_dir_age_secs: MANUAL_MIN_DIR_AGE_SECS,
        }
    }

    /// 周期自动清理：只动本实例命名空间
    pub fn periodic() -> Self {
        Self {
            include_legacy: false,
            include_own_namespace: true,
            include_foreign_namespaces: false,
            min_dir_age_secs: PERIODIC_MIN_DIR_AGE_SECS,
        }
    }
}

/// 目录是否达到删除所需的最小年龄
///
/// `ctime <= 0`（接口没给时间）视为年龄未知，不删。
pub fn old_enough(ctime_secs: i64, now_secs: i64, min_age_secs: i64) -> bool {
    ctime_secs > 0 && now_secs.saturating_sub(ctime_secs) >= min_age_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    const U1: &str = "00aaf329-43bd-49d5-9c3f-8648c6ff4e43";
    const U2: &str = "00ddc5e5-f4bd-4877-af9d-23d7b55ceb63";

    #[test]
    fn test_build_task_temp_dir() {
        assert_eq!(
            build_task_temp_dir("/.bpr_share_temp/", None, U1),
            format!("/.bpr_share_temp/{}/", U1)
        );
        assert_eq!(
            build_task_temp_dir("/.bpr_share_temp", Some("inst-abcdef012345"), U1),
            format!("/.bpr_share_temp/inst-abcdef012345/{}/", U1)
        );
    }

    #[test]
    fn test_validate_task_temp_dir_accepts_both_layouts() {
        let root = "/.bpr_share_temp/";
        assert!(validate_task_temp_dir(&format!("/.bpr_share_temp/{}/", U1), root).is_ok());
        assert!(validate_task_temp_dir(
            &format!("/.bpr_share_temp/inst-abcdef012345/{}", U1),
            root
        )
        .is_ok());
    }

    #[test]
    fn test_validate_task_temp_dir_rejects_unsafe() {
        let root = "/.bpr_share_temp";
        // 根本身 / 命名空间本身 / 非 UUID 末级
        assert!(validate_task_temp_dir("/.bpr_share_temp/", root).is_err());
        assert!(validate_task_temp_dir("/.bpr_share_temp/inst-abcdef012345/", root).is_err());
        assert!(validate_task_temp_dir("/.bpr_share_temp/my-folder", root).is_err());
        // 前缀碰撞
        assert!(validate_task_temp_dir(&format!("/.bpr_share_temp2/{}", U1), root).is_err());
        // 不在根下
        assert!(validate_task_temp_dir(&format!("/other/{}", U1), root).is_err());
        // 层级过深 / 命名空间名非法
        assert!(validate_task_temp_dir(&format!("/.bpr_share_temp/{}/{}", U1, U2), root).is_err());
        assert!(validate_task_temp_dir(&format!("/.bpr_share_temp/inst-XYZ/{}", U1), root).is_err());
        // 根不安全
        assert!(validate_task_temp_dir(&format!("/{}", U1), "/").is_err());
        assert!(validate_task_temp_dir(&format!("/{}", U1), "").is_err());
    }

    #[test]
    fn test_protect_path_covers_all_ancestors_under_root() {
        let mut set = HashSet::new();
        protect_path(
            &mut set,
            "/.bpr_share_temp/",
            &format!("/.bpr_share_temp/inst-abcdef012345/{}/sub/a.mp4", U1),
        );
        assert!(set.contains("/.bpr_share_temp/inst-abcdef012345"));
        assert!(set.contains(&format!("/.bpr_share_temp/inst-abcdef012345/{}", U1)));
        assert!(set.contains(&format!("/.bpr_share_temp/inst-abcdef012345/{}/sub", U1)));
        assert!(!set.contains("/.bpr_share_temp"));

        // 带尾斜杠的任务目录同样命中
        let mut set = HashSet::new();
        protect_path(&mut set, "/.bpr_share_temp", &format!("/.bpr_share_temp/{}/", U1));
        assert!(set.contains(&format!("/.bpr_share_temp/{}", U1)));
    }

    #[test]
    fn test_protect_path_ignores_paths_outside_root() {
        let mut set = HashSet::new();
        protect_path(&mut set, "/.bpr_share_temp", "/电影/a.mp4");
        protect_path(&mut set, "/.bpr_share_temp", &format!("/.bpr_share_temp2/{}", U1));
        assert!(set.is_empty());
    }

    #[test]
    fn test_classify_root_entry() {
        let own = Some("inst-abcdef012345");
        assert_eq!(classify_root_entry(U1, own), RootEntry::LegacyTask);
        assert_eq!(classify_root_entry("inst-abcdef012345", own), RootEntry::OwnNamespace);
        assert_eq!(classify_root_entry("inst-0123456789ab", own), RootEntry::ForeignNamespace);
        assert_eq!(classify_root_entry("inst-0123456789ab", None), RootEntry::ForeignNamespace);
        assert_eq!(classify_root_entry("我的资源", own), RootEntry::Other);
        assert_eq!(classify_root_entry("inst-", own), RootEntry::Other);
    }

    #[test]
    fn test_transfer_protection_rules() {
        let now = 1_000_000;
        // 未结束：无论多久都保护（长时间暂停的任务不会被删）
        assert!(transfer_protects_temp_dir(false, 0, now));
        // 已结束但仍在宽限期
        assert!(transfer_protects_temp_dir(true, now - 60, now));
        // 已结束且超过宽限期
        assert!(!transfer_protects_temp_dir(true, now - TERMINAL_GRACE_SECS, now));
    }

    #[test]
    fn test_is_finished_transfer_status() {
        for s in ["completed", "transfer_failed", "download_failed", "TransferFailed", "DownloadFailed"] {
            assert!(is_finished_transfer_status(s), "{}", s);
        }
        for s in ["transferred", "downloading", "cleaning", "transferring", "checking_share", "queued"] {
            assert!(!is_finished_transfer_status(s), "{}", s);
        }
    }

    #[test]
    fn test_old_enough() {
        assert!(old_enough(100, 100 + 600, 600));
        assert!(!old_enough(100, 100 + 599, 600));
        assert!(!old_enough(0, 1_000_000, 600));
    }

    #[test]
    fn test_instance_id_validation() {
        assert!(is_valid_instance_id("abcdef012345"));
        assert!(!is_valid_instance_id("abc"));
        assert!(!is_valid_instance_id("ABCDEF012345"));
        assert!(!is_valid_instance_id("abcdef01234g"));
    }

    #[test]
    fn test_normalize_dir() {
        assert_eq!(normalize_dir("/a/b/"), "/a/b");
        assert_eq!(normalize_dir("/"), "/");
        assert_eq!(normalize_dir("/a"), "/a");
    }
}
