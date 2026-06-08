//! 快照差异计算
//!
//! 两次 `ShareSnapshot` 之间计算 `ShareDiff`：
//! - **added**   ：新出现的条目
//! - **removed** ：消失的条目
//! - **modified**：路径相同但 `fs_id` 或 `size` 不同（视为内容变更）
//! - **unchanged_count**：未变化条目数（不含目录，统计更直观）
//!
//! 设计原则：
//! - 纯函数、无副作用，便于单元测试
//! - 以 `path` 为唯一键；`fs_id` 变化即视为不同文件（分享者重传）
//! - 不依赖 md5 / mtime（百度分享列表 API 不返回，filemetas 又受 BDUSS 鉴权限制）

use crate::share_sync::snapshot::{ShareSnapshot, ShareSnapshotItem};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 单条修改记录（old → new）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShareModifiedItem {
    /// 旧条目（已不存在于新快照中）
    pub old: ShareSnapshotItem,
    /// 新条目
    pub new: ShareSnapshotItem,
}

/// 两次快照之间的差异
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ShareDiff {
    /// 新增条目
    pub added: Vec<ShareSnapshotItem>,
    /// 删除条目
    pub removed: Vec<ShareSnapshotItem>,
    /// 修改条目
    pub modified: Vec<ShareModifiedItem>,
    /// 未变化的文件数（不含目录）
    pub unchanged_count: usize,
}

impl ShareDiff {
    /// 是否无差异
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.modified.is_empty()
    }

    /// 总计需要执行的动作数
    pub fn total_actions(&self) -> usize {
        self.added.len() + self.modified.len() + self.removed.len()
    }

    /// 摘要（用于持久化与 WS 事件）
    pub fn summary(&self) -> DiffSummaryView {
        DiffSummaryView {
            added: self.added.len(),
            modified: self.modified.len(),
            removed: self.removed.len(),
            unchanged: self.unchanged_count,
        }
    }
}

/// 给上层用的精简摘要（不携带具体条目）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiffSummaryView {
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
    pub unchanged: usize,
}

/// 计算两次快照之间的差异
///
/// `prev == None` 表示首次抓取：所有条目视为 `added`
pub fn diff_snapshots(prev: Option<&ShareSnapshot>, curr: &ShareSnapshot) -> ShareDiff {
    let prev_map: BTreeMap<String, &ShareSnapshotItem> =
        prev.map(|s| s.index_by_path()).unwrap_or_default();
    let curr_map: BTreeMap<String, &ShareSnapshotItem> = curr.index_by_path();

    let mut added = Vec::new();
    let mut modified = Vec::new();
    let mut unchanged_count = 0usize;

    for (path, curr_item) in &curr_map {
        match prev_map.get(path) {
            None => added.push((*curr_item).clone()),
            Some(prev_item) => {
                if is_changed(prev_item, curr_item) {
                    modified.push(ShareModifiedItem {
                        old: (*prev_item).clone(),
                        new: (*curr_item).clone(),
                    });
                } else if !curr_item.is_dir {
                    // 文件未变化才计入 unchanged_count（目录不算"文件未变"）
                    unchanged_count += 1;
                }
            }
        }
    }

    let mut removed = Vec::new();
    for (path, prev_item) in &prev_map {
        if !curr_map.contains_key(path) {
            removed.push((*prev_item).clone());
        }
    }

    // 稳定排序（按 path）
    added.sort_by(|a, b| a.path.cmp(&b.path));
    modified.sort_by(|a, b| a.old.path.cmp(&b.old.path));
    removed.sort_by(|a, b| a.path.cmp(&b.path));

    ShareDiff {
        added,
        removed,
        modified,
        unchanged_count,
    }
}

/// 判断条目是否变更
///
/// 规则：
/// - fs_id 不同 → 变更（百度重传了同名但内容不同的文件）
/// - size 不同 → 变更
/// - is_dir 不同 → 变更（很少见，但语义重要）
fn is_changed(prev: &ShareSnapshotItem, curr: &ShareSnapshotItem) -> bool {
    prev.fs_id != curr.fs_id || prev.size != curr.size || prev.is_dir != curr.is_dir
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share_sync::snapshot::ShareSnapshot;

    fn file(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }

    fn dir(path: &str, fs_id: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, 0, true)
    }

    fn snap(items: Vec<ShareSnapshotItem>) -> ShareSnapshot {
        ShareSnapshot::with_items("sub-1", items)
    }

    #[test]
    fn test_first_capture_marks_all_as_added() {
        let curr = snap(vec![file("/a", 1, 100), file("/b", 2, 200)]);
        let diff = diff_snapshots(None, &curr);
        assert_eq!(diff.added.len(), 2);
        assert_eq!(diff.removed.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.unchanged_count, 0);
    }

    #[test]
    fn test_no_changes() {
        let prev = snap(vec![file("/a", 1, 100), file("/b", 2, 200)]);
        let curr = snap(vec![file("/a", 1, 100), file("/b", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert!(diff.is_empty());
        assert_eq!(diff.unchanged_count, 2);
    }

    #[test]
    fn test_added() {
        let prev = snap(vec![file("/a", 1, 100)]);
        let curr = snap(vec![file("/a", 1, 100), file("/b", 2, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].path, "/b");
        assert_eq!(diff.unchanged_count, 1);
    }

    #[test]
    fn test_removed() {
        let prev = snap(vec![file("/a", 1, 100), file("/b", 2, 200)]);
        let curr = snap(vec![file("/a", 1, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].path, "/b");
        assert_eq!(diff.unchanged_count, 1);
    }

    #[test]
    fn test_modified_by_size() {
        let prev = snap(vec![file("/a", 1, 100)]);
        let curr = snap(vec![file("/a", 1, 200)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].old.size, 100);
        assert_eq!(diff.modified[0].new.size, 200);
        assert_eq!(diff.unchanged_count, 0);
    }

    #[test]
    fn test_modified_by_fs_id() {
        let prev = snap(vec![file("/a", 1, 100)]);
        let curr = snap(vec![file("/a", 2, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.modified.len(), 1);
    }

    #[test]
    fn test_modified_when_dir_to_file() {
        let prev = snap(vec![dir("/a", 1)]);
        let curr = snap(vec![file("/a", 1, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.modified.len(), 1);
    }

    #[test]
    fn test_dirs_not_counted_in_unchanged() {
        let prev = snap(vec![dir("/d1", 1), file("/a", 2, 100)]);
        let curr = snap(vec![dir("/d1", 1), file("/a", 2, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        // /d1 是目录，不计入 unchanged；/a 是文件
        assert_eq!(diff.unchanged_count, 1);
    }

    #[test]
    fn test_complex_mix() {
        let prev = snap(vec![
            file("/keep", 1, 100), // unchanged
            file("/grow", 2, 100), // modified (size)
            file("/vanish", 3, 100), // removed
                                   // /new is added
        ]);
        let curr = snap(vec![
            file("/keep", 1, 100),
            file("/grow", 2, 999),
            file("/new", 4, 50),
        ]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].path, "/new");
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].path, "/vanish");
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].old.path, "/grow");
        assert_eq!(diff.unchanged_count, 1);
    }

    #[test]
    fn test_total_actions() {
        let prev = snap(vec![file("/a", 1, 100)]);
        let curr = snap(vec![file("/a", 1, 200), file("/b", 2, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        assert_eq!(diff.total_actions(), 2); // 1 modified + 1 added
    }

    #[test]
    fn test_diff_serialize_roundtrip() {
        let prev = snap(vec![file("/a", 1, 100)]);
        let curr = snap(vec![file("/a", 1, 200), file("/b", 2, 100)]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let json = serde_json::to_string(&diff).unwrap();
        let back: ShareDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(diff, back);
    }

    #[test]
    fn test_summary_view() {
        let prev = snap(vec![file("/a", 1, 100)]);
        let curr = snap(vec![
            file("/a", 1, 200),
            file("/b", 2, 100),
            file("/c", 3, 50),
        ]);
        let diff = diff_snapshots(Some(&prev), &curr);
        let s = diff.summary();
        assert_eq!(s.added, 2);
        assert_eq!(s.modified, 1);
        assert_eq!(s.removed, 0);
        assert_eq!(s.unchanged, 0);
    }
}
