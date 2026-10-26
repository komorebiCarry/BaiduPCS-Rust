//! 分享订阅配置
//!
//! 一个 `ShareSubscription` = 一条分享链接 + 至少一个目标（网盘/本地）
//! + 冲突策略 + 轮询节奏。

use crate::share_sync::cron::CronSchedule;
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

/// 本地同步模式：决定「同步到本地」时网盘侧如何留存。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LocalSyncMode {
    /// 分享直下：转存到临时目录 → 下载到本地 → 清理临时目录（网盘不留存）。
    /// 老订阅未带 `mode` 字段时默认此模式，保持既有行为不破。
    #[default]
    ShareDirect,
    /// 转存并下载：转存到同订阅「网盘目标」的 remote_path（保留）→ 再下载到本地。
    /// 要求该订阅已配置网盘目标，否则创建/保存时报错提示先添加网盘目标。
    TransferAndDownload,
}

/// 本地目标
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalTarget {
    /// 本地保存绝对路径
    pub local_path: PathBuf,
    /// 仅本目标生效的冲突策略
    #[serde(default)]
    pub conflict_strategy: Option<ConflictStrategy>,
    /// 本地同步模式（默认 `ShareDirect`，老订阅反序列化不报错）。
    #[serde(default)]
    pub mode: LocalSyncMode,
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
    /// cron 表达式（Cron 模式），5 字段：分 时 日 月 星期，按服务器本地时区
    #[serde(default)]
    pub cron_expr: Option<String>,
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
            cron_expr: None,
        }
    }
}

/// 调度器实际执行的轮询节奏
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollSchedule {
    /// 固定间隔秒数（已钳到 `MIN_POLL_INTERVAL_SECS` 以上）
    Interval(u32),
    /// 按 cron 时刻触发（「每日定时」也折算成 `m h * * *`）
    Cron(CronSchedule),
}

/// 「每日定时」未填时刻时的默认值，与前端默认 03:00 一致
const DEFAULT_SCHEDULE_HOUR: u32 = 3;
const DEFAULT_SCHEDULE_MINUTE: u32 = 0;

impl PollConfig {
    /// 解析出实际生效的调度节奏；禁用或配置无效时返回 `None`（不启动调度器）
    pub fn schedule(&self) -> Option<PollSchedule> {
        if !self.enabled {
            return None;
        }
        match self.mode {
            PollMode::Disabled => None,
            PollMode::Interval => Some(PollSchedule::Interval(
                self.interval_secs.max(MIN_POLL_INTERVAL_SECS),
            )),
            PollMode::Scheduled => Some(PollSchedule::Cron(CronSchedule::daily(
                self.schedule_hour.unwrap_or(DEFAULT_SCHEDULE_HOUR),
                self.schedule_minute.unwrap_or(DEFAULT_SCHEDULE_MINUTE),
            ))),
            PollMode::Cron => {
                let expr = self.cron_expr.as_deref().unwrap_or("");
                match parse_poll_cron(expr) {
                    Ok(c) => Some(PollSchedule::Cron(c)),
                    Err(e) => {
                        tracing::warn!(
                            "share-sync: cron 表达式 {:?} 无效，不启动轮询: {}",
                            expr,
                            e
                        );
                        None
                    }
                }
            }
        }
    }
}

/// 解析并校验轮询用 cron 表达式：语法合法、能触发、相邻两次触发不少于最小轮询间隔
pub fn parse_poll_cron(expr: &str) -> Result<CronSchedule, String> {
    let cron = CronSchedule::parse(expr)?;
    cron.check_min_gap(MIN_POLL_INTERVAL_SECS)?;
    Ok(cron)
}

/// 一条订阅
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShareSubscription {
    /// 订阅 ID（UUID）
    pub id: String,
    /// 所属账号 uid（多账号隔离）。0 表示历史/未归属数据，由上层在创建/导入时赋值。
    #[serde(default)]
    pub owner_uid: u64,
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
    /// 排除 glob 模式列表（历史字段，始终按通配符解释）
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    /// 排除正则列表（Rust 语法，整条路径匹配）；旧配置缺省为空。
    #[serde(default)]
    pub exclude_regex_patterns: Vec<String>,
    /// 精确排除的目录路径（相对分享根）：连同全部内容排除，且不再递归列举。
    #[serde(default)]
    pub exclude_paths: Vec<String>,
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
    /// 连续「链接确定性失效」的失败次数。每次 run 因分享链接失效/提取码失效而失败时
    /// +1，成功抓取一次即归零。达到阈值后置 `link_invalid=true` 自动暂停轮询。
    #[serde(default)]
    pub consecutive_link_failures: u32,
    /// 链接已确定性失效并被自动暂停：`true` 时调度触发会直接跳过（不再访问百度），
    /// 等用户更新链接后手动「恢复」清除。
    #[serde(default)]
    pub link_invalid: bool,
    /// 链接失效的可读原因（前端展示，如「分享资源不存在或已失效」）。
    #[serde(default)]
    pub link_invalid_reason: Option<String>,
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
            owner_uid: 0,
            name,
            share_url,
            password: None,
            include_paths: Vec::new(),
            exclude_patterns: Vec::new(),
            exclude_regex_patterns: Vec::new(),
            exclude_paths: Vec::new(),
            targets,
            conflict_strategy: ConflictStrategy::default(),
            delete_missing: false,
            poll_config: PollConfig::default(),
            enabled: true,
            max_concurrent_transfers: None,
            consecutive_link_failures: 0,
            link_invalid: false,
            link_invalid_reason: None,
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
        crate::share_sync::exclusion::validate_rules(
            &self.exclude_patterns,
            &self.exclude_regex_patterns,
        )
        .map_err(|e| e.to_string())?;
        if self.targets.is_empty() {
            return Err("至少需要配置一个同步目标".into());
        }
        // 目标模型收窄：最多 1 个网盘目标 + 1 个本地目标（可共存=转存直下）。
        // 多个本地目标会让同一文件重复下载到多个目录、浪费带宽；多个网盘目标也无清晰需求。
        let netdisk_count = self
            .targets
            .iter()
            .filter(|t| matches!(t, SyncTarget::Netdisk(_)))
            .count();
        let local_count = self
            .targets
            .iter()
            .filter(|t| matches!(t, SyncTarget::Local(_)))
            .count();
        if netdisk_count > 1 {
            return Err("最多只能配置 1 个网盘目标".into());
        }
        if local_count > 1 {
            return Err("最多只能配置 1 个本地目标".into());
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
        // 「转存并下载」本地模式需复用同订阅「网盘目标」的 remote_path，
        // 因此要求该订阅已配置网盘目标，否则提示先添加。
        let has_transfer_and_download = self.targets.iter().any(|t| {
            matches!(
                t,
                SyncTarget::Local(LocalTarget {
                    mode: LocalSyncMode::TransferAndDownload,
                    ..
                })
            )
        });
        let has_netdisk_target = self
            .targets
            .iter()
            .any(|t| matches!(t, SyncTarget::Netdisk(_)));
        if has_transfer_and_download && !has_netdisk_target {
            return Err("「转存并下载」模式需复用网盘目标，请先为该订阅添加网盘目标".into());
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
        if matches!(self.poll_config.mode, PollMode::Cron) {
            let expr = self.poll_config.cron_expr.as_deref().unwrap_or("").trim();
            if expr.is_empty() {
                return Err("Cron 模式下必须填写 cron 表达式".into());
            }
            parse_poll_cron(expr).map_err(|e| format!("cron 表达式无效：{}", e))?;
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
    /// 排除正则列表（Rust 语法，整条路径匹配）；缺省为空。
    #[serde(default)]
    pub exclude_regex_patterns: Vec<String>,
    /// 精确排除的目录路径（相对分享根）；缺省为空 = 不排除。
    #[serde(default)]
    pub exclude_paths: Vec<String>,
    pub targets: Vec<SyncTarget>,
    #[serde(default)]
    pub conflict_strategy: Option<ConflictStrategy>,
    #[serde(default)]
    pub delete_missing: Option<bool>,
    #[serde(default)]
    pub poll_config: Option<PollConfig>,
    /// 显式指定订阅归属账号（多账号场景）。
    /// 缺省回退到当前活跃账号；兼容前端发的 `owner_uid` 字段名。
    #[serde(default, alias = "owner_uid")]
    pub uid: Option<u64>,
}

impl CreateShareSubscriptionRequest {
    /// 转换为订阅对象
    pub fn into_subscription(self) -> ShareSubscription {
        let mut sub = ShareSubscription::new(self.name, self.share_url, self.targets);
        sub.password = self.password;
        sub.include_paths = self.include_paths;
        sub.exclude_patterns = self.exclude_patterns;
        sub.exclude_regex_patterns = self.exclude_regex_patterns;
        sub.exclude_paths = self.exclude_paths;
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
    /// 缺省（字段缺失）= 保留已有正则规则；显式 `[]` = 清空。
    #[serde(default)]
    pub exclude_regex_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_paths: Option<Vec<String>>,
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
    fn test_validate_rejects_multiple_netdisk_targets() {
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![
                SyncTarget::Netdisk(NetdiskTarget {
                    remote_path: "/a".into(),
                    save_fs_id: 0,
                    conflict_strategy: None,
                }),
                SyncTarget::Netdisk(NetdiskTarget {
                    remote_path: "/b".into(),
                    save_fs_id: 0,
                    conflict_strategy: None,
                }),
            ],
        );
        let err = sub.validate().unwrap_err();
        assert!(
            err.contains("最多只能配置 1 个网盘目标"),
            "实际错误: {}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_multiple_local_targets() {
        let tmp = std::env::temp_dir();
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![
                SyncTarget::Local(LocalTarget {
                    local_path: tmp.clone(),
                    conflict_strategy: None,
                    mode: Default::default(),
                }),
                SyncTarget::Local(LocalTarget {
                    local_path: tmp,
                    conflict_strategy: None,
                    mode: Default::default(),
                }),
            ],
        );
        let err = sub.validate().unwrap_err();
        assert!(
            err.contains("最多只能配置 1 个本地目标"),
            "实际错误: {}",
            err
        );
    }

    #[test]
    fn test_validate_allows_one_netdisk_plus_one_local() {
        // 网盘 + 本地共存（转存直下）应当合法
        let tmp = std::env::temp_dir();
        let sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![
                SyncTarget::Netdisk(NetdiskTarget {
                    remote_path: "/a".into(),
                    save_fs_id: 0,
                    conflict_strategy: None,
                }),
                SyncTarget::Local(LocalTarget {
                    local_path: tmp,
                    conflict_strategy: None,
                    mode: Default::default(),
                }),
            ],
        );
        assert!(sub.validate().is_ok());
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
                mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
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
                mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
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
                mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
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
        assert_eq!(
            cfg.schedule(),
            Some(PollSchedule::Interval(MIN_POLL_INTERVAL_SECS))
        );
    }

    #[test]
    fn test_poll_disabled_returns_none() {
        let mut cfg = PollConfig::default();
        cfg.enabled = false;
        assert_eq!(cfg.schedule(), None);
        let mut cfg = PollConfig::default();
        cfg.mode = PollMode::Disabled;
        assert_eq!(cfg.schedule(), None);
    }

    #[test]
    fn test_poll_scheduled_uses_configured_time() {
        let mut cfg = PollConfig::default();
        cfg.mode = PollMode::Scheduled;
        cfg.schedule_hour = Some(21);
        cfg.schedule_minute = Some(15);
        assert_eq!(
            cfg.schedule(),
            Some(PollSchedule::Cron(CronSchedule::daily(21, 15)))
        );
        // 未填时刻时回退到 03:00（与前端默认一致）
        cfg.schedule_hour = None;
        cfg.schedule_minute = None;
        assert_eq!(
            cfg.schedule(),
            Some(PollSchedule::Cron(CronSchedule::daily(3, 0)))
        );
    }

    #[test]
    fn test_poll_cron_schedule() {
        let mut cfg = PollConfig::default();
        cfg.mode = PollMode::Cron;
        cfg.cron_expr = Some("*/10 11-13 * * 6".into());
        assert_eq!(
            cfg.schedule(),
            Some(PollSchedule::Cron(
                CronSchedule::parse("*/10 11-13 * * 6").unwrap()
            ))
        );
        // 无效 / 过密表达式不启动调度
        cfg.cron_expr = Some("*/5 * * * *".into());
        assert_eq!(cfg.schedule(), None);
        cfg.cron_expr = None;
        assert_eq!(cfg.schedule(), None);
    }

    #[test]
    fn test_validate_cron_mode() {
        let tmp = std::env::temp_dir();
        let mut sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Local(LocalTarget {
                local_path: tmp,
                conflict_strategy: None,
                mode: LocalSyncMode::ShareDirect,
            })],
        );
        sub.poll_config.mode = PollMode::Cron;
        sub.poll_config.cron_expr = None;
        assert!(sub.validate().unwrap_err().contains("必须填写"));
        sub.poll_config.cron_expr = Some("* * * *".into());
        assert!(sub.validate().unwrap_err().contains("cron 表达式无效"));
        sub.poll_config.cron_expr = Some("*/5 * * * *".into());
        assert!(sub.validate().unwrap_err().contains("过于频繁"));
        sub.poll_config.cron_expr = Some("*/10 11-13 * * 6".into());
        assert!(sub.validate().is_ok());
    }

    #[test]
    fn test_poll_config_without_cron_field_deserializes() {
        // 老版本持久化的订阅没有 cron_expr 字段
        let cfg: PollConfig = serde_json::from_str(
            r#"{"enabled":true,"mode":"interval","interval_secs":1800,"schedule_hour":null,"schedule_minute":null}"#,
        )
        .unwrap();
        assert_eq!(cfg.cron_expr, None);
        assert_eq!(cfg.mode, PollMode::Interval);
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
            mode: crate::share_sync::config::LocalSyncMode::ShareDirect,
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
            exclude_regex_patterns: vec!["^/股票/2025.*$".into()],
            exclude_paths: vec!["/a/tmp".into()],
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
                cron_expr: None,
            }),
            uid: Some(42),
        };
        let sub = req.into_subscription();
        // into_subscription 不消费 uid（归属由 handler 显式设置），默认仍为 0
        assert_eq!(sub.owner_uid, 0);
        assert_eq!(sub.password.as_deref(), Some("1234"));
        assert!(sub.delete_missing);
        assert_eq!(sub.conflict_strategy, ConflictStrategy::Skip);
        assert_eq!(sub.poll_config.mode, PollMode::Scheduled);
        assert_eq!(sub.poll_config.schedule_hour, Some(3));
        assert_eq!(sub.exclude_paths, vec!["/a/tmp".to_string()]);
    }

    /// 老订阅没有 exclude_paths 字段，必须仍能反序列化。
    #[test]
    fn test_subscription_without_exclude_paths_deserializes() {
        let json = r#"{
            "id": "s1",
            "name": "t",
            "share_url": "https://pan.baidu.com/s/1y7CluAbCdEfGh",
            "include_paths": ["/a"],
            "exclude_patterns": ["*.tmp"],
            "targets": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let sub: ShareSubscription = serde_json::from_str(json).unwrap();
        assert!(sub.exclude_paths.is_empty());
        assert_eq!(sub.include_paths, vec!["/a".to_string()]);
        assert_eq!(sub.exclude_patterns, vec!["*.tmp".to_string()]);
    }

    /// 老订阅没有 exclude_regex_patterns 字段：默认空，且旧 glob（含字面量括号 /
    /// 括号 / `re:` 前缀）语义不变。
    #[test]
    fn test_subscription_without_regex_field_keeps_glob_semantics() {
        let json = r#"{
            "id": "s1",
            "name": "t",
            "share_url": "https://pan.baidu.com/s/1y7CluAbCdEfGh",
            "include_paths": [],
            "exclude_patterns": ["*.tmp", "/a(1)", "/b[2]", "re:x"],
            "targets": [],
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let sub: ShareSubscription = serde_json::from_str(json).unwrap();
        assert!(sub.exclude_regex_patterns.is_empty());
        assert_eq!(
            sub.exclude_patterns,
            vec![
                "*.tmp".to_string(),
                "/a(1)".to_string(),
                "/b[2]".to_string(),
                "re:x".to_string()
            ]
        );
        // 全部仍按通配符解释：字面量括号 / 括号 / `re:` 前缀都不做正则解析
        let rules = crate::share_sync::exclusion::CompiledRules::compile(
            &sub.exclude_patterns,
            &sub.exclude_regex_patterns,
        )
        .unwrap();
        assert!(rules.matches_self("/a(1)"));
        assert!(!rules.matches_self("/a1"));
        assert!(rules.matches_self("/b[2]"));
        assert!(rules.matches_self("re:x"));
        // 规则校验（订阅保存入口使用同一套）对这份历史配置保持放行
        assert!(crate::share_sync::exclusion::validate_rules(
            &sub.exclude_patterns,
            &sub.exclude_regex_patterns
        )
        .is_ok());
    }

    #[test]
    fn test_regex_patterns_roundtrip() {
        let mut sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        sub.exclude_regex_patterns = vec!["^/股票/按月归档/20(25|26)-.*$".into()];
        let json = serde_json::to_string(&sub).unwrap();
        let back: ShareSubscription = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.exclude_regex_patterns,
            vec!["^/股票/按月归档/20(25|26)-.*$".to_string()]
        );
    }

    #[test]
    fn test_validate_rejects_invalid_regex() {
        let mut sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        sub.exclude_regex_patterns = vec!["^/ok$".into(), "(".into()];
        let err = sub.validate().unwrap_err();
        assert!(err.contains("第 2 条"), "{err}");

        sub.exclude_regex_patterns = vec!["  ".into()];
        assert!(sub.validate().unwrap_err().contains("不能为空"));
    }

    #[test]
    fn test_update_request_missing_vs_empty_regex_list() {
        // 字段缺失 = 保留（Option None）
        let missing: UpdateShareSubscriptionRequest =
            serde_json::from_str(r#"{"name":"x"}"#).unwrap();
        assert!(missing.exclude_regex_patterns.is_none());
        // 显式 [] = 清空（Option Some(vec![])）
        let cleared: UpdateShareSubscriptionRequest =
            serde_json::from_str(r#"{"exclude_regex_patterns":[]}"#).unwrap();
        assert_eq!(cleared.exclude_regex_patterns, Some(Vec::new()));
    }

    /// 精确目录排除随订阅序列化往返，编辑后不丢失。
    #[test]
    fn test_exclude_paths_roundtrip() {
        let mut sub = ShareSubscription::new(
            "t".into(),
            "https://pan.baidu.com/s/1y7CluAbCdEfGh".into(),
            vec![SyncTarget::Netdisk(NetdiskTarget {
                remote_path: "/x".into(),
                save_fs_id: 0,
                conflict_strategy: None,
            })],
        );
        sub.exclude_paths = vec!["/剧集/S01".into()];
        let json = serde_json::to_string(&sub).unwrap();
        let back: ShareSubscription = serde_json::from_str(&json).unwrap();
        assert_eq!(back.exclude_paths, vec!["/剧集/S01".to_string()]);
    }
}
