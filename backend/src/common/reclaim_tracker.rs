//! 内存回收的「持续观察」计时器
//!
//! 已到终态的任务不能一到终态就移出内存：有的调用方还在轮询它（如分享同步每秒
//! `get_task` 等转存任务结束），而任务自带的时间戳也不可靠（重启恢复出来的任务
//! `updated_at` 是旧值）。这里改为「从回收任务**第一次观察到**它可回收起计时，
//! 持续满 ttl 才放行」，与任务自身的时间戳无关。
//!
//! 每个候选带一个版本标记（如 `updated_at`）：两次采样之间任务若经历过
//! 「终态 → 又开始跑 → 再次终态」，标记会变，计时从头开始 —— 否则采样间隔内的
//! 状态往返会被当成一直处于终态，提前回收。

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// 记录每个候选第一次被观察到可回收的时刻（及当时的版本标记）
#[derive(Debug, Default)]
pub struct ReclaimTracker {
    first_seen: HashMap<String, (i64, Instant)>,
}

impl ReclaimTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// 传入本轮的全部候选 `(id, 版本标记)`，返回其中已持续可回收满 `ttl` 的 id
    ///
    /// - 不在本轮候选里的旧记录直接遗忘：任务已被移除，或状态又变回非终态；
    /// - 版本标记与上次不同：从这一刻重新计时；
    /// - 返回的候选也会被遗忘，调用方负责回收它们。
    pub fn due<I>(&mut self, candidates: I, now: Instant, ttl: Duration) -> Vec<String>
    where
        I: IntoIterator<Item = (String, i64)>,
    {
        let mut next: HashMap<String, (i64, Instant)> = HashMap::new();
        let mut due = Vec::new();
        for (id, version) in candidates {
            let seen = match self.first_seen.get(&id) {
                Some(&(v, seen)) if v == version => seen,
                _ => now,
            };
            if now.saturating_duration_since(seen) >= ttl {
                due.push(id);
            } else {
                next.insert(id, (version, seen));
            }
        }
        self.first_seen = next;
        due
    }

    /// 当前在计时的候选数
    pub fn tracked(&self) -> usize {
        self.first_seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(3600);

    fn c(v: &[(&str, i64)]) -> Vec<(String, i64)> {
        v.iter().map(|(s, ver)| (s.to_string(), *ver)).collect()
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_due_only_after_ttl_since_first_seen() {
        let mut tracker = ReclaimTracker::new();
        let t0 = Instant::now();

        // 第一次看到：开始计时，不放行
        assert!(tracker.due(c(&[("a", 1)]), t0, TTL).is_empty());
        // 未满 ttl
        assert!(tracker
            .due(c(&[("a", 1)]), t0 + TTL - Duration::from_secs(1), TTL)
            .is_empty());
        // 满 ttl
        assert_eq!(tracker.due(c(&[("a", 1)]), t0 + TTL, TTL), ids(&["a"]));
        // 放行后遗忘
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn test_candidate_leaving_resets_clock() {
        let mut tracker = ReclaimTracker::new();
        let t0 = Instant::now();

        tracker.due(c(&[("a", 1)]), t0, TTL);
        // 中途不再是候选（例如恢复后重新开始下载）→ 遗忘
        tracker.due(c(&[]), t0 + Duration::from_secs(600), TTL);
        // 再次成为候选：从这一刻重新计时
        let t1 = t0 + Duration::from_secs(1200);
        assert!(tracker.due(c(&[("a", 1)]), t1, TTL).is_empty());
        assert!(tracker.due(c(&[("a", 1)]), t0 + TTL, TTL).is_empty());
        assert_eq!(tracker.due(c(&[("a", 1)]), t1 + TTL, TTL), ids(&["a"]));
    }

    #[test]
    fn test_version_change_between_samples_resets_clock() {
        let mut tracker = ReclaimTracker::new();
        let t0 = Instant::now();

        tracker.due(c(&[("a", 100)]), t0, TTL);
        // 两次采样之间经历了「终态 → 又跑 → 再次终态」：版本标记变了
        let t1 = t0 + Duration::from_secs(600);
        assert!(tracker.due(c(&[("a", 200)]), t1, TTL).is_empty());
        // 按第一次的时刻算已满 ttl，但要从版本变化那次重新算
        assert!(tracker.due(c(&[("a", 200)]), t0 + TTL, TTL).is_empty());
        assert_eq!(tracker.due(c(&[("a", 200)]), t1 + TTL, TTL), ids(&["a"]));
    }

    #[test]
    fn test_independent_candidates() {
        let mut tracker = ReclaimTracker::new();
        let t0 = Instant::now();

        tracker.due(c(&[("a", 1)]), t0, TTL);
        let t1 = t0 + Duration::from_secs(1800);
        tracker.due(c(&[("a", 1), ("b", 1)]), t1, TTL);
        assert_eq!(tracker.due(c(&[("a", 1), ("b", 1)]), t0 + TTL, TTL), ids(&["a"]));
        assert_eq!(tracker.tracked(), 1);
        assert_eq!(tracker.due(c(&[("b", 1)]), t1 + TTL, TTL), ids(&["b"]));
    }
}
