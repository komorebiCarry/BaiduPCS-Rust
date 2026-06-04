//! 分享快照数据模型
//!
//! 一次"抓取"产生一个 `ShareSnapshot`，包含完整的文件/目录条目列表；
//! 后续的 `diff_snapshots` 在两次快照之间计算 added/removed/modified。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use uuid::Uuid;

/// 快照中的一条记录（文件或目录）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShareSnapshotItem {
    /// 相对分享根的路径（如 `/剧集/01.mp4`）；根级条目 path 为 `/<name>`
    pub path: String,
    /// 百度返回的原始分享路径，用于后续转存/下载定位；老快照缺失时回退到 `path`
    #[serde(default)]
    pub raw_path: String,
    /// 百度 fs_id（目录也分配）
    pub fs_id: u64,
    /// 文件大小（目录固定 0）
    pub size: u64,
    /// 条目名称（path 的 basename）
    pub name: String,
    /// 是否为目录
    pub is_dir: bool,
}

impl ShareSnapshotItem {
    pub fn new(
        path: impl Into<String>,
        name: impl Into<String>,
        fs_id: u64,
        size: u64,
        is_dir: bool,
    ) -> Self {
        let path = path.into();
        Self {
            raw_path: path.clone(),
            path,
            name: name.into(),
            fs_id,
            size,
            is_dir,
        }
    }

    pub fn with_raw_path(
        path: impl Into<String>,
        name: impl Into<String>,
        fs_id: u64,
        size: u64,
        is_dir: bool,
        raw_path: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            raw_path: raw_path.into(),
            name: name.into(),
            fs_id,
            size,
            is_dir,
        }
    }
}

/// 一次抓取的完整快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareSnapshot {
    /// 快照 ID（UUID）
    pub id: String,
    /// 关联的订阅 ID
    pub subscription_id: String,
    /// 抓取时间
    pub captured_at: DateTime<Utc>,
    /// 抓取到的条目（含目录）
    pub items: Vec<ShareSnapshotItem>,
}

impl ShareSnapshot {
    /// 创建一个空快照（用于初始化场景）
    pub fn empty(subscription_id: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            subscription_id: subscription_id.into(),
            captured_at: Utc::now(),
            items: Vec::new(),
        }
    }

    /// 创建一个带条目的快照
    pub fn with_items(
        subscription_id: impl Into<String>,
        items: Vec<ShareSnapshotItem>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            subscription_id: subscription_id.into(),
            captured_at: Utc::now(),
            items,
        }
    }

    /// 按 path 构建 map，便于 O(1) 查找
    pub fn index_by_path(&self) -> BTreeMap<String, &ShareSnapshotItem> {
        self.items.iter().map(|i| (i.path.clone(), i)).collect()
    }

    /// 排序（按 path 字典序），保证序列化稳定
    pub fn sorted_items(&self) -> Vec<ShareSnapshotItem> {
        let mut v = self.items.clone();
        v.sort_by(|a, b| a.path.cmp(&b.path));
        v
    }

    /// 文件数量（不含目录）
    pub fn file_count(&self) -> usize {
        self.items.iter().filter(|i| !i.is_dir).count()
    }
}

// =====================================================
// 抓取（递归列出分享内容）
// =====================================================

use crate::netdisk::client::NetdiskClient;
use crate::transfer::types::{ShareFileListResult, SharedFileInfo};
use crate::share_sync::error::ShareSyncError;

/// 抓取结果（含访问元数据，便于后续转存/下载使用）
#[derive(Debug, Clone)]
pub struct CapturedShare {
    pub short_key: String,
    pub shareid: String,
    pub uk: String,
    pub bdstoken: String,
    pub password: Option<String>,
}

/// 抓取器：递归 list 整个分享内容
///
/// 内部约束：
/// - 每页拉 100 条；翻页直到 errno/空列表
/// - 递归使用 BFS 队列，避免深栈
/// - 应用 include_paths / exclude_patterns 过滤
pub struct SnapshotCollector<'a> {
    client: &'a NetdiskClient,
    short_key: String,
    shareid: String,
    uk: String,
    bdstoken: String,
    password: Option<String>,
    include_paths: BTreeSet<String>,
    exclude_patterns: Vec<String>,
}

impl<'a> SnapshotCollector<'a> {
    /// 从 URL + 密码构造一个 collector（先访问分享页取 bdstoken）
    pub async fn from_url(
        client: &'a NetdiskClient,
        share_url: &str,
        password: Option<String>,
        include_paths: Vec<String>,
        exclude_patterns: Vec<String>,
    ) -> Result<Self, ShareSyncError> {
        let share_link = client
            .parse_share_link(share_url)
            .map_err(|e| ShareSyncError::ShareLinkError(e.to_string()))?;
        let effective_pwd = password.or(share_link.password.clone());

        let page = client
            .access_share_page(&share_link.short_key, &effective_pwd, true)
            .await
            .map_err(|e| ShareSyncError::ShareLinkError(e.to_string()))?;

        if page.shareid.is_empty() {
            return Err(ShareSyncError::ShareLinkError(
                "分享页面返回的 shareid 为空".into(),
            ));
        }

        // 🔥 关键：如有密码，必须先 verify_share_password 拿到 randsk 写入 Cookie，
        // 否则 list_share_files 会返回 errno=-9 "提取码验证失败"。
        if let Some(ref pwd) = effective_pwd {
            if !pwd.is_empty() {
                let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
                if let Err(e) = client
                    .verify_share_password(
                        &page.shareid,
                        &page.share_uk,
                        &page.bdstoken,
                        pwd,
                        &referer,
                    )
                    .await
                {
                    return Err(ShareSyncError::ShareLinkError(format!(
                        "验证提取码失败: {}",
                        e
                    )));
                }
            }
        }

        Ok(Self {
            client,
            short_key: share_link.short_key,
            shareid: page.shareid,
            uk: page.uk,
            bdstoken: page.bdstoken,
            password: effective_pwd,
            include_paths: include_paths
                .into_iter()
                .filter_map(normalize_snapshot_path)
                .collect(),
            exclude_patterns,
        })
    }

    /// 抓取完整快照
    ///
    /// 流程：root list → BFS 遍历所有子目录 → 合并去重 → 过滤
    pub async fn collect(self) -> Result<(CapturedShare, ShareSnapshot), ShareSyncError> {
        let page_size: u32 = 100;

        // Step 1: root
        let root = self
            .client
            .list_share_files(&self.short_key, &self.bdstoken, 1, page_size)
            .await
            .map_err(|e| ShareSyncError::ShareLinkError(e.to_string()))?;

        // root shareid/uk 可能比 access_share_page 拿到的更"权威"（部分场景下）
        let root_shareid = if !root.shareid.is_empty() {
            root.shareid
        } else {
            self.shareid.clone()
        };
        let root_uk = if !root.uk.is_empty() {
            root.uk
        } else {
            self.uk.clone()
        };

        let share_root = infer_share_root(&root.files);
        let mut all_items: Vec<ShareSnapshotItem> = Vec::new();
        let mut seen: HashSet<(String, u64)> = HashSet::new();
        let mut queued_dirs: HashSet<String> = HashSet::new();
        let mut found_included_files: BTreeSet<String> = BTreeSet::new();

        // 推入 root
        for f in root.files {
            let is_dir = f.is_dir;
            let raw_path = f.path.clone();
            let normalized_path = normalize_share_path(&f.path, &f.name, &share_root);
            if !is_dir && self.include_paths.contains(&normalized_path) {
                found_included_files.insert(normalized_path.clone());
            }
            push_unique(&mut all_items, &mut seen, &share_root, f);
            if is_dir && self.dir_allowed(&normalized_path) {
                queued_dirs.insert(raw_path);
            }
        }

        // Step 2: BFS 子目录
        let mut queue: VecDeque<String> = queued_dirs.iter().cloned().collect();

        while let Some(dir) = queue.pop_front() {
            let normalized_dir = normalize_share_path(&dir, dir.rsplit('/').next().unwrap_or(&dir), &share_root);
            if !self.dir_allowed(&normalized_dir) {
                continue;
            }
            let mut page: u32 = 1;
            loop {
                let batch = self
                    .client
                    .list_share_files_in_dir(
                        &self.short_key,
                        &root_shareid,
                        &root_uk,
                        &self.bdstoken,
                        &dir,
                        page,
                        page_size,
                    )
                    .await
                    .map_err(|e| ShareSyncError::ShareLinkError(e.to_string()))?;

                if batch.is_empty() {
                    break;
                }

                let batch_len = batch.len();
                for f in batch {
                    let is_dir = f.is_dir;
                    let raw_path = f.path.clone();
                    let normalized_path = normalize_share_path(&f.path, &f.name, &share_root);
                    if !is_dir && self.include_paths.contains(&normalized_path) {
                        found_included_files.insert(normalized_path.clone());
                    }
                    push_unique(&mut all_items, &mut seen, &share_root, f);
                    if is_dir && self.dir_allowed(&normalized_path) && queued_dirs.insert(raw_path.clone()) {
                        queue.push_back(raw_path);
                    }
                }

                if !self.dir_needs_more_pages(&normalized_dir, &found_included_files) {
                    break;
                }
                if batch_len < page_size as usize {
                    break;
                }
                page += 1;
                if page > 10_000 {
                    return Err(ShareSyncError::Internal(
                        "递归层数/翻页数超过安全上限，可能存在循环引用".into(),
                    ));
                }
            }
        }

        // Step 3: 过滤
        let filtered: Vec<ShareSnapshotItem> = all_items
            .into_iter()
            .filter(|it| self.item_allowed(it))
            .collect();

        // 排序（path 字典序）
        let mut filtered = filtered;
        filtered.sort_by(|a, b| a.path.cmp(&b.path));

        let captured = CapturedShare {
            short_key: self.short_key.clone(),
            shareid: root_shareid,
            uk: root_uk,
            bdstoken: self.bdstoken.clone(),
            password: self.password.clone(),
        };
        let snap = ShareSnapshot::with_items(
            /*subscription_id*/ "", // 由调用方在 manager 处填
            filtered,
        );
        Ok((captured, snap))
    }

    fn dir_allowed(&self, dir: &str) -> bool {
        if self.include_paths.is_empty() {
            return true;
        }
        // dir 允许遍历当：
        //   - dir 自身就是 include_path（include 一个目录），或
        //   - dir 是某个 include_path 的祖先（include 一个具体文件，需要 descend 进去才能找到它）。
        // 之前只看 `is_path_ancestor_or_self(dir, inc)`，会把 `/monthly` 在 inc=`/monthly/300426.SZ.csv` 时过滤掉，
        // 导致文件无法被发现。
        self.include_paths
            .iter()
            .any(|inc| is_path_ancestor_or_self(dir, inc) || is_path_ancestor_or_self(inc, dir))
    }

    fn item_allowed(&self, item: &ShareSnapshotItem) -> bool {
        if !self.include_paths.is_empty()
            && !self
                .include_paths
                .iter()
                .any(|inc| is_path_ancestor_or_self(&item.path, inc))
        {
            return false;
        }
        for pat in &self.exclude_patterns {
            if glob_match(pat, &item.path) {
                return false;
            }
        }
        true
    }

    fn dir_needs_more_pages(&self, dir: &str, found_included_files: &BTreeSet<String>) -> bool {
        if self.include_paths.is_empty() {
            return true;
        }

        // If the selected include path is this directory or an ancestor of it,
        // the user selected a whole subtree, so we must scan all pages.
        if self
            .include_paths
            .iter()
            .any(|inc| is_path_ancestor_or_self(dir, inc))
        {
            return true;
        }

        // Otherwise this directory is only being scanned to find exact file
        // include paths below it. Once every requested descendant file has been
        // found, continuing to page through thousands of siblings is wasted work.
        self.include_paths
            .iter()
            .any(|inc| is_path_ancestor_or_self(inc, dir) && !found_included_files.contains(inc))
    }
}

/// 简单的 glob 匹配（仅支持 `*` 与 `?`）
fn glob_match(pattern: &str, s: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = s.chars().collect();
    glob_match_inner(&p, &t)
}

fn glob_match_inner(p: &[char], t: &[char]) -> bool {
    if p.is_empty() {
        return t.is_empty();
    }
    match p[0] {
        '*' => {
            // 跳过连续 *
            let mut i = 0;
            while i < p.len() && p[i] == '*' {
                i += 1;
            }
            for j in 0..=t.len() {
                if glob_match_inner(&p[i..], &t[j..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if t.is_empty() {
                false
            } else {
                glob_match_inner(&p[1..], &t[1..])
            }
        }
        c => {
            if t.is_empty() || t[0] != c {
                false
            } else {
                glob_match_inner(&p[1..], &t[1..])
            }
        }
    }
}

pub fn infer_share_root(files: &[SharedFileInfo]) -> String {
    let parents: Vec<String> = files
        .iter()
        .filter_map(|f| normalize_snapshot_path(f.path.clone()))
        .map(|p| parent_dir(&p))
        .collect();
    if parents.is_empty() {
        return String::new();
    }

    let mut common: Vec<String> = path_components(&parents[0]);
    for parent in parents.iter().skip(1) {
        let parts = path_components(parent);
        let keep = common
            .iter()
            .zip(parts.iter())
            .take_while(|(a, b)| a == b)
            .count();
        common.truncate(keep);
        if common.is_empty() {
            break;
        }
    }

    if common.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", common.join("/"))
    }
}

pub fn normalize_share_path(raw_path: &str, name: &str, share_root: &str) -> String {
    let raw = normalize_snapshot_path(raw_path.to_string()).unwrap_or_default();
    let root = normalize_snapshot_path(share_root.to_string()).unwrap_or_default();
    let relative = if raw.is_empty() {
        String::new()
    } else if root.is_empty() || root == "/" {
        raw.trim_start_matches('/').to_string()
    } else if raw == root {
        String::new()
    } else if raw.starts_with(&root) && raw.as_bytes().get(root.len()) == Some(&b'/') {
        raw[root.len() + 1..].to_string()
    } else {
        raw.trim_start_matches('/').to_string()
    };

    let fallback = if name.trim().is_empty() {
        raw.rsplit('/').next().unwrap_or("").to_string()
    } else {
        name.trim().to_string()
    };
    let candidate = if relative.trim().is_empty() {
        fallback
    } else {
        relative
    };

    normalize_snapshot_path(candidate).unwrap_or_else(|| "/".to_string())
}

fn normalize_snapshot_path(path: String) -> Option<String> {
    let trimmed = path.trim().replace('\\', "/");
    if trimmed.is_empty() {
        return None;
    }
    let prefixed = if trimmed.starts_with('/') {
        trimmed
    } else {
        format!("/{}", trimmed)
    };
    let mut collapsed = String::with_capacity(prefixed.len());
    let mut prev_slash = false;
    for ch in prefixed.chars() {
        if ch == '/' {
            if !prev_slash {
                collapsed.push(ch);
            }
            prev_slash = true;
        } else {
            collapsed.push(ch);
            prev_slash = false;
        }
    }
    if collapsed == "/" {
        return Some("/".to_string());
    }
    let normalized = collapsed.trim_end_matches('/').to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn parent_dir(path: &str) -> String {
    let path = normalize_snapshot_path(path.to_string()).unwrap_or_else(|| "/".to_string());
    if path == "/" {
        return "/".to_string();
    }
    match path.rsplit_once('/') {
        Some(("", _)) => "/".to_string(),
        Some((parent, _)) if parent.is_empty() => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
        None => "/".to_string(),
    }
}

fn path_components(path: &str) -> Vec<String> {
    path.trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn is_path_ancestor_or_self(path: &str, ancestor: &str) -> bool {
    if ancestor == "/" {
        return true;
    }
    if path == ancestor {
        return true;
    }
    path.starts_with(ancestor) && path.as_bytes().get(ancestor.len()) == Some(&b'/')
}

fn push_unique(
    out: &mut Vec<ShareSnapshotItem>,
    seen: &mut HashSet<(String, u64)>,
    share_root: &str,
    info: SharedFileInfo,
) {
    let normalized_path = normalize_share_path(&info.path, &info.name, share_root);
    let key = (normalized_path.clone(), info.fs_id);
    if seen.insert(key) {
        out.push(ShareSnapshotItem::with_raw_path(
            normalized_path,
            info.name,
            info.fs_id,
            info.size,
            info.is_dir,
            info.path,
        ));
    }
}

// 方便其它模块用：把 ShareFileListResult 当成 Vec<SharedFileInfo> 的薄包装
impl ShareFileListResult {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
    pub fn len(&self) -> usize {
        self.files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }

    fn shared_file(path: &str, name: &str, fs_id: u64, is_dir: bool) -> SharedFileInfo {
        SharedFileInfo {
            fs_id,
            is_dir,
            path: path.to_string(),
            size: if is_dir { 0 } else { 123 },
            name: name.to_string(),
        }
    }

    #[test]
    fn test_empty_snapshot() {
        let s = ShareSnapshot::empty("sub-1");
        assert_eq!(s.subscription_id, "sub-1");
        assert!(s.items.is_empty());
        assert_eq!(s.file_count(), 0);
    }

    #[test]
    fn test_snapshot_with_items() {
        let s = ShareSnapshot::with_items(
            "sub-1",
            vec![
                item("/a.txt", 1, 100),
                item("/b/c.txt", 2, 200),
                ShareSnapshotItem::new("/dir", "dir", 3, 0, true),
            ],
        );
        assert_eq!(s.file_count(), 2);
    }

    #[test]
    fn test_index_by_path() {
        let s = ShareSnapshot::with_items(
            "sub-1",
            vec![item("/a.txt", 1, 100), item("/b.txt", 2, 200)],
        );
        let map = s.index_by_path();
        assert_eq!(map.get("/a.txt").unwrap().fs_id, 1);
        assert_eq!(map.get("/b.txt").unwrap().fs_id, 2);
        assert!(map.get("/c.txt").is_none());
    }

    #[test]
    fn test_sorted_items() {
        let s = ShareSnapshot::with_items(
            "sub-1",
            vec![item("/c.txt", 3, 1), item("/a.txt", 1, 1), item("/b.txt", 2, 1)],
        );
        let sorted = s.sorted_items();
        assert_eq!(sorted[0].path, "/a.txt");
        assert_eq!(sorted[1].path, "/b.txt");
        assert_eq!(sorted[2].path, "/c.txt");
    }

    #[test]
    fn test_normalize_single_file_share_to_root_file() {
        let files = vec![shared_file(
            "/_pcs_.workspace/curated/report.csv",
            "report.csv",
            1,
            false,
        )];
        let root = infer_share_root(&files);

        assert_eq!(root, "/_pcs_.workspace/curated");
        assert_eq!(
            normalize_share_path(&files[0].path, &files[0].name, &root),
            "/report.csv"
        );
    }

    #[test]
    fn test_normalize_multi_folder_share_keeps_folder_prefixes() {
        let files = vec![
            shared_file("/_pcs_.workspace/curated/fina_indicator", "fina_indicator", 1, true),
            shared_file("/_pcs_.workspace/curated/stock_basic", "stock_basic", 2, true),
        ];
        let root = infer_share_root(&files);

        assert_eq!(root, "/_pcs_.workspace/curated");
        assert_eq!(
            normalize_share_path(&files[0].path, &files[0].name, &root),
            "/fina_indicator"
        );
        assert_eq!(
            normalize_share_path(
                "/_pcs_.workspace/curated/fina_indicator/000004.SZ.csv",
                "000004.SZ.csv",
                &root,
            ),
            "/fina_indicator/000004.SZ.csv"
        );
    }

    #[test]
    fn test_serialize_roundtrip() {
        let s = ShareSnapshot::with_items("sub-1", vec![item("/a", 1, 100)]);
        let json = serde_json::to_string(&s).unwrap();
        let back: ShareSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.subscription_id, "sub-1");
        assert_eq!(back.items.len(), 1);
        assert_eq!(back.items[0].path, "/a");
    }

    // ========== glob 匹配测试 ==========

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match("*.txt", "a.txt"));
        assert!(glob_match("*.txt", "abc.txt"));
        assert!(!glob_match("*.txt", "a.png"));
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("a?c", "abc"));
        assert!(glob_match("a?c", "axc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(!glob_match("a?c", "abbc"));
    }

    #[test]
    fn test_glob_match_literal() {
        assert!(glob_match("foo", "foo"));
        assert!(!glob_match("foo", "bar"));
        assert!(!glob_match("foo", "fooo"));
    }

    #[test]
    fn test_glob_match_combined() {
        // * 匹配任意字符（含 /），与 shell glob 不同；本实现以"扩展名过滤"为主
        assert!(glob_match("a/*/b", "a/x/b"));
        assert!(glob_match("a/*/b", "a/x/y/b")); // * 匹配 x/y
        assert!(glob_match("file-*.txt", "file-2024.txt"));
        assert!(glob_match("?est.tmp", "test.tmp"));
        // 典型用法：扩展名排除
        assert!(glob_match("*.tmp", "anything.tmp"));
        assert!(!glob_match("*.tmp", "anything.txt"));
    }

    #[test]
    fn test_is_path_ancestor_or_self() {
        assert!(is_path_ancestor_or_self("/foo/bar", "/foo"));
        assert!(is_path_ancestor_or_self("/foo", "/foo"));
        assert!(!is_path_ancestor_or_self("/foobar", "/foo"));
        assert!(is_path_ancestor_or_self("/foobar", "/"));
    }

    #[test]
    fn test_normalize_snapshot_path() {
        assert_eq!(normalize_snapshot_path("/foo/".to_string()), Some("/foo".to_string()));
        assert_eq!(normalize_snapshot_path("foo".to_string()), Some("/foo".to_string()));
        assert_eq!(normalize_snapshot_path("   /bar//".to_string()), Some("/bar".to_string()));
        assert_eq!(normalize_snapshot_path("".to_string()), None);
    }
}
