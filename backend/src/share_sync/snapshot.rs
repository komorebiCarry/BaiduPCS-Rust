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
    pub fn with_items(subscription_id: impl Into<String>, items: Vec<ShareSnapshotItem>) -> Self {
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

use std::sync::Arc;

use crate::netdisk::client::NetdiskClient;
use crate::share_sync::error::ShareSyncError;
use crate::share_sync::rate_limit::QuotaLimiter;
use crate::transfer::types::{ShareFileListResult, SharedFileInfo};
use regex::RegexSet;

/// 抓取结果（含访问元数据，便于后续转存/下载使用）
#[derive(Debug, Clone)]
pub struct CapturedShare {
    pub short_key: String,
    pub shareid: String,
    pub uk: String,
    /// 分享 UK（access_share_page 返回的 share_uk，转存接口需要，可能与 uk 不同）。
    /// 留存它,拆批转存时各批可复用而不必每批重新 access_share_page。
    pub share_uk: String,
    pub bdstoken: String,
    pub password: Option<String>,
    pub randsk: Option<String>,
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
    share_uk: String,
    bdstoken: String,
    password: Option<String>,
    randsk: Option<String>,
    include_paths: BTreeSet<String>,
    /// 预计算的 include 路径前缀索引（dir 维度）：
    /// - `ancestors`：所有 include_path 的祖先 + include_path 自身的并集
    ///   命中此集合的目录是值得递归进入的（O(log N) 查询）
    include_index: BTreeSet<String>,
    /// 预编译的 exclude glob → RegexSet
    exclude_set: Option<RegexSet>,
    /// v2 阶段 6 补全:列目录抓快照同样走全局风控限速器。
    /// 与 ProductionHooks 的 submit_transfer/submit_download 共用同一个令牌桶,
    /// 因此「列目录 + 转存提交」合计受同一个全局 RPS 上限约束 — 大分享单轮
    /// BFS 翻页的 list 突发是最容易撞百度风控 errno=132 的地方,必须限速。
    rate_limiter: Arc<QuotaLimiter>,
}

impl<'a> SnapshotCollector<'a> {
    /// 从 URL + 密码构造一个 collector（先访问分享页取 bdstoken）
    pub async fn from_url(
        client: &'a NetdiskClient,
        share_url: &str,
        password: Option<String>,
        include_paths: Vec<String>,
        exclude_patterns: Vec<String>,
        rate_limiter: Arc<QuotaLimiter>,
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

        // If a password exists, keep the returned randsk on the collector.
        // The global CookieJar only has one randsk slot, so concurrent shares
        // must pass their own randsk explicitly when listing pages.
        let mut randsk = None;
        if let Some(ref pwd) = effective_pwd {
            if !pwd.is_empty() {
                let referer = format!("https://pan.baidu.com/s/{}", share_link.short_key);
                match client
                    .verify_share_password(
                        &page.shareid,
                        &page.share_uk,
                        &page.bdstoken,
                        pwd,
                        &referer,
                    )
                    .await
                {
                    Ok(sekey) => randsk = Some(sekey),
                    Err(e) => {
                        return Err(ShareSyncError::ShareLinkError(format!(
                            "验证提取码失败: {}",
                            e
                        )));
                    }
                }
            }
        }

        let include_paths: BTreeSet<String> = include_paths
            .into_iter()
            .filter_map(normalize_snapshot_path)
            .collect();
        let include_index = build_include_index(&include_paths);
        let exclude_set = compile_exclude_patterns(&exclude_patterns);
        Ok(Self {
            client,
            short_key: share_link.short_key,
            shareid: page.shareid,
            uk: page.uk,
            share_uk: page.share_uk,
            bdstoken: page.bdstoken,
            password: effective_pwd,
            randsk,
            include_paths,
            include_index,
            exclude_set,
            rate_limiter,
        })
    }

    /// 抓取完整快照
    ///
    /// 流程：root list → BFS 遍历所有子目录 → 合并去重 → 过滤
    pub async fn collect(mut self) -> Result<(CapturedShare, ShareSnapshot), ShareSyncError> {
        let page_size: u32 = 100;

        // Step 1: root
        self.rate_limiter.acquire().await;
        let root = self
            .client
            .list_share_files_with_randsk(
                &self.short_key,
                &self.bdstoken,
                1,
                page_size,
                self.randsk.as_deref(),
            )
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

        // include_paths 在 from_url 阶段只做了 slash 归一，仍处于「分享内绝对路径 /
        // sharelink 合成路径」命名空间；而快照条目 path 是「相对分享根」。此处用
        // share_root 把 include 重新归一到同一命名空间，否则「分享根是某个目录」的
        // 非根分享会因 dir_allowed 全部判否而采集到 0 个文件 —— 表现为首同步空跑、
        // added=0、不转存/不下载。
        if !self.include_paths.is_empty() {
            let remapped: BTreeSet<String> = self
                .include_paths
                .iter()
                .map(|p| remap_include_to_share_root(p, &share_root))
                .collect();
            self.include_index = build_include_index(&remapped);
            self.include_paths = remapped;
        }

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
            let normalized_dir =
                normalize_share_path(&dir, dir.rsplit('/').next().unwrap_or(&dir), &share_root);
            if !self.dir_allowed(&normalized_dir) {
                continue;
            }
            let mut page: u32 = 1;
            loop {
                self.rate_limiter.acquire().await;
                let batch = self
                    .client
                    .list_share_files_in_dir_with_randsk(
                        &self.short_key,
                        &root_shareid,
                        &root_uk,
                        &self.bdstoken,
                        &dir,
                        page,
                        page_size,
                        self.randsk.as_deref(),
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
                    if is_dir
                        && self.dir_allowed(&normalized_path)
                        && queued_dirs.insert(raw_path.clone())
                    {
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
            share_uk: self.share_uk.clone(),
            bdstoken: self.bdstoken.clone(),
            password: self.password.clone(),
            randsk: self.randsk.clone(),
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
        // 命中预计算索引 → dir 是某个 include_path 自身或它的祖先
        self.include_index.contains(dir)
    }

    fn item_allowed(&self, item: &ShareSnapshotItem) -> bool {
        // 1) include 过滤：item.path 是某个 include_path 自身或后代
        if !self.include_paths.is_empty() && !self.include_index.contains(&item.path) {
            // 二级检查：item.path 是否是某个 include_path 的后代（include 选了目录，
            // 它下面的所有文件/子目录都应被收录）
            let mut allowed = false;
            for inc in &self.include_paths {
                if is_path_ancestor_or_self(&item.path, inc) {
                    allowed = true;
                    break;
                }
            }
            if !allowed {
                return false;
            }
        }
        // 2) exclude 过滤：任意 exclude glob 命中则排除
        if let Some(ref set) = self.exclude_set {
            if set.is_match(&item.path) {
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

/// 把 include_paths 展开为"祖先 + 自身"的并集索引。
///
/// 这样 `dir_allowed(dir)` 只需要一次 `BTreeSet::contains`（O(log N)），
/// 不再每次线性扫 `include_paths`。
///
/// 例：include_paths = `["/a/b/c.csv", "/a/x/y.csv"]` →
///   `{"/", "/a", "/a/b", "/a/b/c.csv", "/a/x", "/a/x/y.csv"}`
fn build_include_index(include_paths: &BTreeSet<String>) -> BTreeSet<String> {
    let mut idx: BTreeSet<String> = BTreeSet::new();
    idx.insert("/".to_string());
    for inc in include_paths {
        idx.insert(inc.clone());
        // 逐级加祖先
        let mut cur = inc.as_str();
        while let Some(slash) = cur.rfind('/') {
            if slash == 0 {
                break;
            }
            cur = &cur[..slash];
            idx.insert(cur.to_string());
        }
    }
    idx
}

/// 把 glob 模式（仅 `*` / `?`）编译为 `RegexSet`，对每条路径做 O(L) 匹配。
///
/// 旧实现是手写递归 + 回溯，复杂度 O(2^L)，对万级条目 × 多个 pattern 会很慢。
/// 编译失败时回退到"无 exclude"（不阻塞主流程）。
fn compile_exclude_patterns(patterns: &[String]) -> Option<RegexSet> {
    if patterns.is_empty() {
        return None;
    }
    let regexes: Vec<String> = patterns
        .iter()
        .map(|p| glob_to_regex(p).map(|r| format!("^{}$", r)))
        .collect::<Result<_, _>>()
        .ok()?;
    RegexSet::new(&regexes).ok()
}

/// 简单 glob → regex 转换（仅 `*` 和 `?`，其余元字符转义）。
fn glob_to_regex(pattern: &str) -> Result<String, String> {
    let mut out = String::with_capacity(pattern.len() * 2);
    for ch in pattern.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            // regex 元字符需要转义
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '[' | ']' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    Ok(out)
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

/// 去掉 baidu「sharelink 合成路径」头部 `/sharelink<uk>-<shareid>`，
/// 还原为「相对分享根」路径；非 sharelink 路径原样返回。
///
/// 例：`/sharelink3745347292-20270075815/剧集/01.mp4` → `/剧集/01.mp4`
fn strip_sharelink_prefix(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if let Some(rest) = trimmed.strip_prefix("sharelink") {
        // rest 形如 "<uk>-<shareid>/子路径..." 或 "<uk>-<shareid>"
        return match rest.find('/') {
            Some(idx) => format!("/{}", &rest[idx + 1..]),
            None => "/".to_string(),
        };
    }
    path.to_string()
}

/// 把订阅里存的 include_path 归一到「相对分享根」命名空间，与快照条目 path
/// （`normalize_share_path` 产物）保持一致。
///
/// include_path 可能来自前端三种来源：
/// 1. 根级勾选 → 分享内真实绝对路径（如 `/13/a/scan_test`）
/// 2. 子目录浏览勾选 → sharelink 合成路径（如 `/sharelink<uk>-<id>/scan_test`）
/// 3. 历史/已相对化数据（如 `/scan_test`）
///
/// 三者统一映射到相对分享根，否则非根分享（分享根是某个目录）会匹配不到任何文件。
fn remap_include_to_share_root(inc: &str, share_root: &str) -> String {
    let stripped = strip_sharelink_prefix(inc);
    let name = stripped.rsplit('/').next().unwrap_or("");
    normalize_share_path(&stripped, name, share_root)
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
            vec![
                item("/c.txt", 3, 1),
                item("/a.txt", 1, 1),
                item("/b.txt", 2, 1),
            ],
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
            shared_file(
                "/_pcs_.workspace/curated/fina_indicator",
                "fina_indicator",
                1,
                true,
            ),
            shared_file(
                "/_pcs_.workspace/curated/stock_basic",
                "stock_basic",
                2,
                true,
            ),
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
    fn test_remap_include_absolute_path_to_share_root() {
        // 用户实际场景：分享根是单个目录 scan_test，分享内真实路径
        // /13/a测试上传1/scan_test；前端按根级勾选把真实绝对路径存进 include。
        let files = vec![shared_file(
            "/13/a测试上传1/scan_test",
            "scan_test",
            1,
            true,
        )];
        let share_root = infer_share_root(&files);
        assert_eq!(share_root, "/13/a测试上传1");

        // 修复前：include 仍是绝对路径，与快照相对路径 /scan_test 对不上。
        // 修复后：remap 到相对分享根 → /scan_test。
        let remapped = remap_include_to_share_root("/13/a测试上传1/scan_test", &share_root);
        assert_eq!(remapped, "/scan_test");

        let mut set = BTreeSet::new();
        set.insert(remapped);
        let index = build_include_index(&set);
        // 目录自身命中 → dir_allowed 会放行、BFS 进入该目录
        assert!(index.contains("/scan_test"));
        // 目录下的文件（快照相对路径）是 include 的后代 → item_allowed 放行
        assert!(is_path_ancestor_or_self("/scan_test/foo.mp4", "/scan_test"));
    }

    #[test]
    fn test_remap_include_sharelink_path_to_share_root() {
        // 子目录浏览勾选时前端存的是 sharelink 合成路径。
        let share_root = "/13/a测试上传1";
        let remapped = remap_include_to_share_root(
            "/sharelink3745347292-20270075815/scan_test/sub",
            share_root,
        );
        assert_eq!(remapped, "/scan_test/sub");
    }

    #[test]
    fn test_remap_include_already_relative_is_idempotent() {
        // 已是相对分享根的历史/正确数据，remap 后保持不变。
        let share_root = "/13/a测试上传1";
        assert_eq!(
            remap_include_to_share_root("/scan_test", share_root),
            "/scan_test"
        );
    }

    #[test]
    fn test_remap_include_root_share_keeps_absolute() {
        // 分享根为 "/"（多个不同顶层目录）时，绝对路径与快照命名空间一致，保持不变。
        let remapped = remap_include_to_share_root("/13/foo", "/");
        assert_eq!(remapped, "/13/foo");
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

    // ========== glob 匹配测试（RegexSet 路径） ==========

    fn compile(patterns: &[&str]) -> RegexSet {
        // 模拟 compile_exclude_patterns 的 "^...$" 锚定
        let regexes: Vec<String> = patterns
            .iter()
            .map(|p| format!("^{}$", glob_to_regex(p).unwrap()))
            .collect();
        RegexSet::new(&regexes).unwrap()
    }

    fn is_hit(set: &RegexSet, s: &str) -> bool {
        set.is_match(s)
    }

    #[test]
    fn test_glob_match_star() {
        let set = compile(&["*.txt"]);
        assert!(is_hit(&set, "a.txt"));
        assert!(is_hit(&set, "abc.txt"));
        assert!(!is_hit(&set, "a.png"));
        // 单独的 "*" 经 ^.*$ 锚定后能匹配空字符串和非空
        let set_any = compile(&["*"]);
        assert!(is_hit(&set_any, "anything"));
    }

    #[test]
    fn test_glob_match_question() {
        let set = compile(&["a?c"]);
        assert!(is_hit(&set, "abc"));
        assert!(is_hit(&set, "axc"));
        assert!(!is_hit(&set, "ac"));
        assert!(!is_hit(&set, "abbc"));
    }

    #[test]
    fn test_glob_match_literal() {
        let set = compile(&["foo"]);
        assert!(is_hit(&set, "foo"));
        assert!(!is_hit(&set, "bar"));
        assert!(!is_hit(&set, "fooo"));
    }

    #[test]
    fn test_glob_match_combined() {
        // * 匹配任意字符（含 /），与 shell glob 不同；本实现以"扩展名过滤"为主
        let set = compile(&["a/*/b", "file-*.txt", "?est.tmp", "*.tmp"]);
        assert!(is_hit(&set, "a/x/b"));
        assert!(is_hit(&set, "a/x/y/b")); // * 匹配 x/y
        assert!(is_hit(&set, "file-2024.txt"));
        assert!(is_hit(&set, "test.tmp"));
        // 典型用法：扩展名排除
        assert!(is_hit(&set, "anything.tmp"));
        assert!(!is_hit(&set, "anything.txt"));
    }

    #[test]
    fn test_glob_to_regex_escapes_metachars() {
        // 扩展名 dot、字符类、管道等需要转义
        assert_eq!(glob_to_regex("a.b").unwrap(), "a\\.b");
        assert_eq!(glob_to_regex("a+b").unwrap(), "a\\+b");
        assert_eq!(glob_to_regex("a(b)c").unwrap(), "a\\(b\\)c");
    }

    #[test]
    fn test_build_include_index_basic() {
        let mut inc = BTreeSet::new();
        inc.insert("/a/b/c.csv".to_string());
        inc.insert("/a/x/y.csv".to_string());
        let idx = build_include_index(&inc);
        assert!(idx.contains("/"));
        assert!(idx.contains("/a"));
        assert!(idx.contains("/a/b"));
        assert!(idx.contains("/a/b/c.csv"));
        assert!(idx.contains("/a/x"));
        assert!(idx.contains("/a/x/y.csv"));
        // 不相关路径
        assert!(!idx.contains("/c"));
        assert!(!idx.contains("/a/b/other.csv"));
    }

    #[test]
    fn test_compile_exclude_empty_returns_none() {
        let set = compile_exclude_patterns(&[]);
        assert!(set.is_none());
    }

    #[test]
    fn test_compile_exclude_invalid_falls_back() {
        // `compile_exclude_patterns` 在 `RegexSet::new` 返回 Err 时回退到 None。
        // 现代 regex crate 非常宽松，常规 glob 都能编译——这里用 Pattern::new 直接
        // 验证 glob_to_regex 不 panic + compile_exclude_patterns 接受空 patterns。
        assert!(glob_to_regex("[abc]").is_ok());
        assert!(compile_exclude_patterns(&[]).is_none());
        // 正常一组 pattern 编译成功
        let set = compile_exclude_patterns(&["*.tmp".to_string(), "*.bak".to_string()]);
        assert!(set.is_some());
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
        assert_eq!(
            normalize_snapshot_path("/foo/".to_string()),
            Some("/foo".to_string())
        );
        assert_eq!(
            normalize_snapshot_path("foo".to_string()),
            Some("/foo".to_string())
        );
        assert_eq!(
            normalize_snapshot_path("   /bar//".to_string()),
            Some("/bar".to_string())
        );
        assert_eq!(normalize_snapshot_path("".to_string()), None);
    }
}
