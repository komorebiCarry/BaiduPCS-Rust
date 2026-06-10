//! 分享订阅配置
//!
//! 一个 `ShareSubscription` = 一条分享链接 + 至少一个目标（网盘/本地）
//! + 冲突策略 + 轮询节奏。

use crate::share_sync::types::{ConflictStrategy, PollMode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use uuid::Uuid;

/// 网盘目标
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetdiskTarget {
    /// 网盘内的目标目录绝对路径（如 `/我的资源/同步`）
    pub remote_path: String,
    /// 网盘目标目录的 fs_id（0 表示需要 resolve；不传则按 remote_path 解析）
    #[serde(default)]
    pub save_fs_id: u64,
    /// 仅本目标生效的冲突策略；None 时继承订阅级
    #[serde(default)]
    pub conflict_strategy: Option<ConflictStrategy>,
}

/// 本地目标
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalTarget {
    /// 本地保存绝对路径
    pub local_path: PathBuf,
    /// 仅本目标生效的冲突策略
    #[serde(default)]
    pub conflict_strategy: Option<ConflictStrategy>,
}

/// 同步目标（一份订阅可同时配多个）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncTarget {
    Netdisk(NetdiskTarget),
    Local(LocalTarget),
}

impl SyncTarget {
    /// 获取此目标的冲突策略（优先自身，回退到订阅级）
    pub fn effective_conflict_strategy(&self, fallback: ConflictStrategy) -> ConflictStrategy {
        match self {
            SyncTarget::Netdisk(t) => t.conflict_strategy.unwrap_or(fallback),
            SyncTarget::Local(t) => t.conflict_strategy.unwrap_or(fallback),
        }
    }
}

/// 轮询配置（最小化版本，参考 autobackup 但去掉不必要的 watch 维度）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PollConfig {
    /// 是否启用
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 轮询模式
    #[serde(default)]
    pub mode: PollMode,
    /// 间隔秒数（Interval 模式）
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u32,
    /// 指定小时（Scheduled 模式）
    #[serde(default)]
    pub schedule_hour: Option<u32>,
    /// 指定分钟（Scheduled 模式）
    #[serde(default)]
    pub schedule_minute: Option<u32>,
}

fn default_true() -> bool {
    true
}

fn default_interval_secs() -> u32 {
    1800 // 30 分钟
}

/// 分享同步的最小轮询间隔（与 autobackup `MIN_INTERVAL_SECS=600` 对齐）
pub const MIN_POLL_INTERVAL_SECS: u32 = 600;

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: PollMode::Interval,
            interval_secs: default_interval_secs(),
            schedule_hour: None,
            schedule_minute: None,
        }
    }
}

impl PollConfig {
    /// 获取实际生效的间隔秒数（强制下限 600s）
    pub fn effective_interval_secs(&self) -> u32 {
        if !self.enabled {
            return 0; // 禁用
        }
        match self.mode {
            PollMode::Disabled => 0,
            PollMode::Interval => self.interval_secs.max(MIN_POLL_INTERVAL_SECS),
            // Scheduled 模式：每天一次，按"秒"算 = 86400，但仍遵守 600s 下限
            PollMode::Scheduled => 86_400,
        }
    }
}

/// 一条订阅
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShareSubscription {
    /// 订阅 ID（UUID）
    pub id: String,
    /// 用户可见名称
    pub name: String,
    /// 分享链接（可含 pwd=xxxx）
    pub share_url: String,
    /// 提取码（URL 中已有则无需重复）
    #[serde(default)]
    pub password: Option<String>,
    /// 限定订阅的子路径列表（空 = 整个分享）
    #[serde(default)]
    pub include_paths: Vec<String>,
    /// 排除 glob 模式列表
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    /// 同步目标（至少 1 个）
    pub targets: Vec<SyncTarget>,
    /// 订阅级冲突策略（各目标可单独覆盖）
    #[serde(default)]
    pub conflict_strategy: ConflictStrategy,
    /// 分享者删除文件时，是否同步删除目标中的副本
    #[serde(default)]
    pub delete_missing: bool,
    /// 轮询配置
    #[serde(default)]
    pub poll_config: PollConfig,
    /// 是否启用
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// v2 阶段 5:tree 入口下并行提交 transfer batch 的最大并发数。
    /// `None` → 走全局默认(env BAIDUPCS_BISECT_CONCURRENCY 或硬编码 4)。
    /// 单个分享文件数多、风控敏感时,可在订阅级别调低(如 2);
    /// 反之多账号低风控时可调高(如 8)。仅在 BAIDUPCS_BISECT_PARALLEL=1 时生效。
    #[serde(default)]
    pub max_concurrent_transfers: Option<u32>,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 更新时间
    pub updated_at: DateTime<Utc>,
}

impl ShareSubscription {
    /// 创建一条新订阅
    pub fn new(name: String, share_url: String, targets: Vec<SyncTarget>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            share_url,
            password: None,
            include_paths: Vec::new(),
            exclude_patterns: Vec::new(),
            targets,
            conflict_strategy: ConflictStrategy::default(),
            delete_missing: false,
            poll_config: PollConfig::default(),
            enabled: true,
            max_concurrent_transfers: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// 触摸 updated_at
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }

    /// 校验基础合法性（HTTP handler 调用）
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("订阅名称不能为空".into());
        }
        if self.share_url.trim().is_empty() {
            return Err("分享链接不能为空".into());
        }
        validate_share_url(&self.share_url)?;
        if self.targets.is_empty() {
            return Err("至少需要配置一个同步目标".into());
        }
        for (idx, target) in self.targets.iter().enumerate() {
            match target {
                SyncTarget::Netdisk(t) => {
                    if t.remote_path.trim().is_empty() {
                        return Err(format!("目标 #{} 网盘路径不能为空", idx + 1));
                    }
                }
                SyncTarget::Local(t) => {
                    if t.local_path.as_os_str().is_empty() {
                        return Err(format!("目标 #{} 本地路径不能为空", idx + 1));
                    }
                    validate_local_path(&t.local_path)
                        .map_err(|e| format!("目标 #{} {}", idx + 1, e))?;
                }
            }
        }
        if matches!(self.poll_config.mode, PollMode::Interval) {
            if self.poll_config.interval_secs < MIN_POLL_INTERVAL_SECS {
                return Err(format!(
                    "轮询间隔不能小于 {} 秒（{} 分钟）",
                    MIN_POLL_INTERVAL_SECS,
                    MIN_POLL_INTERVAL_SECS / 60
                ));
            }
        }
        if let Some(h) = self.poll_config.schedule_hour {
            if h > 23 {
                return Err("schedule_hour 必须在 0-23".into());
            }
        }
        if let Some(m) = self.poll_config.schedule_minute {
            if m > 59 {
                return Err("schedule_minute 必须在 0-59".into());
            }
        }
        Ok(())
    }
}

/// 校验分享链接：
/// 1. 必须是 `http(s)://pan.baidu.com/s/<short_key>[?pwd=xxxx]` 格式
/// 2. 域名后必须跟 `/s/` 路径段（防 `https://evilpan.baidu.com.attack.com/` 类同形钓鱼）
fn validate_share_url(url: &str) -> Result<(), String> {
    let url = url.trim();
    // 协议（仅允许 http/https）
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| "分享链接必须以 http:// 或 https:// 开头".to_string())?;
    // 域名段必须以 pan.baidu.com 结尾
    let domain_end = rest
        .find('/')
        .ok_or_else(|| "分享链接缺少路径段 /s/<short_key>".to_string())?;
    let host = &rest[..domain_end];
    if !host.eq_ignore_ascii_case("pan.baidu.com") {
        return Err(format!(
            "分享链接域名必须是 pan.baidu.com（当前: {}）",
            host
        ));
    }
    // 路径必须以 /s/ 开头（即 /s/<short_key>）
    let path = &rest[domain_end..];
    let after_slash_s = path
        .strip_prefix("/s/")
        .or_else(|| path.strip_prefix("/s"))
        .ok_or_else(|| "分享链接必须包含 /s/<short_key> 路径段".to_string())?;
    if after_slash_s.is_empty() {
        return Err("分享链接缺少 short_key".to_string());
    }
    // short_key 至少 6 位字母数字
    let short_key = after_slash_s
        .split(|c: char| c == '?' || c == '#' || c == '/')
        .next()
        .unwrap_or("");
    if short_key.len() < 6 {
        return Err(format!("分享链接 short_key 过短: {}", short_key));
    }
    // 百度 short_key 实际由 [A-Za-z0-9_-] 组成（base64 变体）
    if !short_key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!("分享链接 short_key 含非法字符: {}", short_key));
    }
    Ok(())
}

fn validate_local_path(path: &Path) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!(
            "本地路径必须是绝对路径: {}",
            path.to_string_lossy()
        ));
    }
    let meta = path
        .metadata()
        .map_err(|e| format!("本地路径不可访问: {} ({})", path.to_string_lossy(), e))?;
    if !meta.is_dir() {
        return Err(format!("本地路径必须是目录: {}", path.to_string_lossy()));
    }
    if meta.permissions().readonly() {
        return Err(format!("本地路径不可写: {}", path.to_string_lossy()));
    }

    if let Ok(system_time) = SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        let probe = path.join(format!(
            ".baidu-pcs-rust-share-sync-probe-{}",
            system_time.as_nanos()
        ));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
            .map_err(|e| format!("本地路径不可写: {} ({})", path.to_string_lossy(), e))?;
        drop(file);
        let _ = std::fs::remove_file(&probe);
    }

    Ok(())
}

// =====================================================
// HTTP DTO
// =====================================================

/// 创建订阅的请求体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateShareSubscriptionRequest {
    pub name: String,
    pub share_url: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub include_paths: Vec<String>,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    pub targets: Vec<SyncTarget>,
    #[serde(default)]
    pub conflict_strategy: Option<ConflictStrategy>,
    #[serde(default)]
    pub delete_missing: Option<bool>,
    #[serde(default)]
    pub poll_config: Option<PollConfig>,
}

impl CreateShareSubscriptionRequest {
    /// 转换为订阅对象
    pub fn into_subscription(self) -> ShareSubscription {
        let mut sub = ShareSubscription::new(self.name, self.share_url, self.targets);
        sub.password = self.password;
        sub.include_paths = self.include_paths;
        sub.exclude_patterns = self.exclude_patterns;
        if let Some(s) = self.conflict_strategy {
            sub.conflict_strategy = s;
        }
        if let Some(d) = self.delete_missing {
            sub.delete_missing = d;
        }
        if let Some(p) = self.poll_config {
            sub.poll_config = p;
        }
        sub
    }
}

/// 更新订阅的请求体
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateShareSubscriptionRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub share_url: Option<String>,
    #[serde(default)]
    pub password: Option<Option<String>>,
    #[serde(default)]
    pub include_paths: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub targets: Option<Vec<SyncTarget>>,
    #[serde(default)]
    pub conflict_strategy: Option<ConflictStrategy>,
    #[serde(default)]
    pub delete_missing: Option<bool>,
    #[serde(default)]
    pub poll_config: Option<PollConfig>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_subscription_defaults() {
        let sub = ShareSubscription::new(
            "test".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        assert_eq!(sub.name, "test");
        assert!(sub.enabled);
        assert!(!sub.delete_missing);
        assert_eq!(sub.conflict_strategy, ConflictStrategy::Overwrite);
    }

    #[test]
    fn test_validate_empty_name() {
        let sub = ShareSubscription::new(
            " ".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        assert!(sub.validate().is_err());
    }

    #[test]
    fn test_validate_bad_url() {
        let sub = ShareSubscription::new(
            "t".into(),
            "https://example.com/x".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        assert!(sub.validate().is_err());
    }

    #[test]
    fn test_validate_lookalike_url_rejected() {
        // 同形钓鱼：pan.baidu.com.attack.com 域名结尾不是 pan.baidu.com
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com.attack.com/s/1abcdef".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        let err = sub.validate().unwrap_err();
        assert!(err.contains("pan.baidu.com"), "实际错误: {}", err);
    }

    #[test]
    fn test_validate_short_short_key_rejected() {
        // short_key 太短
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/abc".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        let err = sub.validate().unwrap_err();
        assert!(err.contains("short_key"), "实际错误: {}", err);
    }

    #[test]
    fn test_validate_url_missing_s_path_rejected() {
        // 域名是 pan.baidu.com 但路径不是 /s/
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/disk/home".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        let err = sub.validate().unwrap_err();
        assert!(err.contains("/s/"), "实际错误: {}", err);
    }

    #[test]
    fn test_validate_valid_url_with_pwd_accepted() {
        // 合法链接 + pwd 参数
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7Clu03oLWC4_ZjCHY8Wdw?pwd=i96y".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        assert!(sub.validate().is_ok());
    }

    #[test]
    fn test_validate_no_targets() {
        let mut sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![],
        );
        sub.targets.clear();
        assert!(sub.validate().is_err());
    }

    #[test]
    fn test_validate_short_interval() {
        let tmp = std::env::temp_dir();
        let mut sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: tmp,
                conflict_strategy: None,
            })],
        );
        sub.poll_config.interval_secs = 60;
        let err = sub.validate().unwrap_err();
        assert!(err.contains("不能小于"));
    }

    #[test]
    fn test_validate_local_path_must_be_absolute() {
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: PathBuf::from("relative/path"),
                conflict_strategy: None,
            })],
        );
        let err = sub.validate().unwrap_err();
        assert!(err.contains("本地路径必须是绝对路径"));
    }

    #[test]
    fn test_validate_local_path_not_directory() {
        let tmp = std::env::temp_dir().join("baidu-pcs-rust-share-sync-test-file");
        let _ = std::fs::remove_file(&tmp);
        std::fs::write(&tmp, b"probe").unwrap();
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: tmp,
                conflict_strategy: None,
            })],
        );
        let err = sub.validate().unwrap_err();
        assert!(err.contains("本地路径必须是目录"));
        let _ =
            std::fs::remove_file(std::env::temp_dir().join("baidu-pcs-rust-share-sync-test-file"));
    }

    #[test]
    fn test_poll_effective_interval_min() {
        let mut cfg = PollConfig::default();
        cfg.interval_secs = 60;
        assert_eq!(cfg.effective_interval_secs(), MIN_POLL_INTERVAL_SECS);
    }

    #[test]
    fn test_poll_disabled_returns_zero() {
        let mut cfg = PollConfig::default();
        cfg.enabled = false;
        assert_eq!(cfg.effective_interval_secs(), 0);
    }

    #[test]
    fn test_poll_scheduled_returns_24h() {
        let mut cfg = PollConfig::default();
        cfg.mode = PollMode::Scheduled;
        assert_eq!(cfg.effective_interval_secs(), 86_400);
    }

    #[test]
    fn test_target_effective_strategy_inherits() {
        let t = SyncTarget::Netdisk(NetdiskTarget {
            remote_path: "/x".into(),
            save_fs_id: 0,
            conflict_strategy: None,
        });
        assert_eq!(
            t.effective_conflict_strategy(ConflictStrategy::Versioned),
            ConflictStrategy::Versioned
        );
    }

    #[test]
    fn test_target_effective_strategy_overrides() {
        let t = SyncTarget::Local(LocalTarget {
            local_path: PathBuf::from("/x"),
            conflict_strategy: Some(ConflictStrategy::Skip),
        });
        assert_eq!(
            t.effective_conflict_strategy(ConflictStrategy::Overwrite),
            ConflictStrategy::Skip
        );
    }

    #[test]
    fn test_subscription_serialize_roundtrip() {
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        let json = serde_json::to_string(&sub).unwrap();
        let back: ShareSubscription = serde_json::from_str(&json).unwrap();
        assert_eq!(sub.id, back.id);
        assert_eq!(sub.name, back.name);
        assert_eq!(sub.targets, back.targets);
    }

    #[test]
    fn test_create_request_into_subscription() {
        let req = CreateShareSubscriptionRequest {
            name: "t".into(),
            share_url: "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            password: Some("1234".into()),
            include_paths: vec!["/a".into()],
            exclude_patterns: vec![],
            targets: vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/y".into(),
                save_fs_id: 0,
                conflict_strategy: Some(ConflictStrategy::Versioned),
            })],
            conflict_strategy: Some(ConflictStrategy::Skip),
            delete_missing: Some(true),
            poll_config: Some(PollConfig {
                enabled: true,
                mode: PollMode::Scheduled,
                interval_secs: 1800,
                schedule_hour: Some(3),
                schedule_minute: Some(30),
            }),
        };
        let sub = req.into_subscription();
        assert_eq!(sub.password.as_deref(), Some("1234"));
        assert!(sub.delete_missing);
        assert_eq!(sub.conflict_strategy, ConflictStrategy::Skip);
        assert_eq!(sub.poll_config.mode, PollMode::Scheduled);
        assert_eq!(sub.poll_config.schedule_hour, Some(3));
    }
}
