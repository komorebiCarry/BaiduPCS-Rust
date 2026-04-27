//! SyncIntent：意图位合并
//!
//! 使用单个 AtomicU8 位掩码管理同步触发意图，
//! 解决 Watch 和 Poll 触发交错时的意图丢失问题。

use std::sync::atomic::{AtomicU8, Ordering};

const INTENT_UPLOAD: u8 = 0b01;
const INTENT_DOWNLOAD: u8 = 0b10;
const INTENT_FULL: u8 = 0b11;

/// 同步意图位图
///
/// Watch 触发时仅置位 upload（快路径，隐式 LocalWins）。
/// Poll/Manual 触发时同时置位 upload + download（完整同步）。
///
/// 合并规则：只升级不降级（`fetch_or` 只会置位，不会清位）。
/// 例：Watch(upload) + Poll(upload+download) = upload+download。
pub struct SyncIntent {
    /// bit 0 = needs_upload, bit 1 = needs_download
    flags: AtomicU8,
}

impl SyncIntent {
    pub fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
        }
    }

    /// Watch 触发：仅置位 upload
    pub fn merge_watch(&self) {
        self.flags.fetch_or(INTENT_UPLOAD, Ordering::AcqRel);
    }

    /// Poll/Manual 触发：同时置位 upload + download
    pub fn merge_full_sync(&self) {
        self.flags.fetch_or(INTENT_FULL, Ordering::AcqRel);
    }

    /// 执行前原子取走全部意图并重置
    /// 单次 swap 保证不会丢位
    pub fn take(&self) -> (bool, bool) {
        let bits = self.flags.swap(0, Ordering::AcqRel);
        (bits & INTENT_UPLOAD != 0, bits & INTENT_DOWNLOAD != 0)
    }

    /// 检查是否有任何待处理意图
    pub fn has_pending(&self) -> bool {
        self.flags.load(Ordering::Acquire) != 0
    }
}

impl Default for SyncIntent {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_sets_upload_only() {
        let intent = SyncIntent::new();
        intent.merge_watch();
        let (upload, download) = intent.take();
        assert!(upload);
        assert!(!download);
    }

    #[test]
    fn test_poll_sets_both() {
        let intent = SyncIntent::new();
        intent.merge_full_sync();
        let (upload, download) = intent.take();
        assert!(upload);
        assert!(download);
    }

    #[test]
    fn test_watch_then_poll_merges_to_full() {
        let intent = SyncIntent::new();
        intent.merge_watch();
        intent.merge_full_sync();
        let (upload, download) = intent.take();
        assert!(upload);
        assert!(download);
    }

    #[test]
    fn test_poll_then_watch_preserves_download() {
        let intent = SyncIntent::new();
        intent.merge_full_sync();
        intent.merge_watch();
        let (upload, download) = intent.take();
        assert!(upload);
        assert!(download);
    }

    #[test]
    fn test_take_clears_flags() {
        let intent = SyncIntent::new();
        intent.merge_full_sync();
        let _ = intent.take();
        let (upload, download) = intent.take();
        assert!(!upload);
        assert!(!download);
    }

    #[test]
    fn test_empty_take() {
        let intent = SyncIntent::new();
        let (upload, download) = intent.take();
        assert!(!upload);
        assert!(!download);
    }

    #[test]
    fn test_has_pending() {
        let intent = SyncIntent::new();
        assert!(!intent.has_pending());
        intent.merge_watch();
        assert!(intent.has_pending());
        let _ = intent.take();
        assert!(!intent.has_pending());
    }

    #[test]
    fn test_multiple_watches_idempotent() {
        let intent = SyncIntent::new();
        intent.merge_watch();
        intent.merge_watch();
        intent.merge_watch();
        let (upload, download) = intent.take();
        assert!(upload);
        assert!(!download);
    }
}
