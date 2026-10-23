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
    /// 该目录在本次抓取中「有后代被 include/exclude 过滤剔除」。
    /// 仅对**存活于快照中的目录**有意义：为 `true` 时，转存阶段禁止把该目录当作
    /// 单个目录 fs_id 整体直传（百度服务端会按 fs_id 递归复制整目录，连带把被过滤
    /// 的子项也搬过去），必须展开到子节点逐层提交。
    /// 运行期派生字段，不持久化（老快照按 serde 默认 `false`），每次抓取重新计算。
    #[serde(default)]
    pub subtree_pruned: bool,
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
            subtree_pruned: false,
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
            subtree_pruned: false,
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
use crate::share_sync::exclusion::CompiledRules;
use crate::share_sync::rate_limit::QuotaLimiter;
use crate::transfer::types::{ShareFileListResult, SharedFileInfo};
use tokio_util::sync::CancellationToken;

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
    /// 分享体系类型（个人版 / 企业版）
    ///
    /// 拆批转存时要据此还原出正确的分享 URL 和接口分流；
    /// 默认个人版，与历史行为一致。
    pub kind: crate::transfer::ShareKind,
    pub password: Option<String>,
    /// 提取码凭据：个人版 randsk，企业版 spwd
    pub randsk: Option<String>,
}

/// 扫描阶段的实时进度快照
///
/// 大分享递归列目录动辄上百秒,期间没有任何事件的话前端只能干显示「运行中」,
/// 用户无法区分「在爬目录」和「卡死了」。抓取器每处理完一个目录就回调一次
/// （由调用方节流后广播）。
///
/// 注意 `dirs_pending` 会边爬边涨（BFS 发现新子目录），**不要**据此算百分比,
/// 否则进度条会倒退。前端应按「已扫描 N 个目录」这类计数式文案展示。
#[derive(Debug, Clone, Default)]
pub struct ScanProgress {
    /// 已完成列目录的目录数
    pub dirs_done: usize,
    /// BFS 队列中待扫描的目录数（会随扫描进行动态增长）
    pub dirs_pending: usize,
    /// 累计发现的文件数（不含目录）
    pub files_seen: usize,
    /// 当前正在扫描的目录（分享内路径）
    pub current_dir: String,
    /// 命中缓存、本轮无需重新请求的目录数（整轮重试时用于说明"续爬"进度）
    pub cached_hits: usize,
}

/// 扫描进度回调。由 manager 注入,内部负责节流 + 广播 WS 事件。
pub type ScanProgressSink = Arc<dyn Fn(ScanProgress) + Send + Sync>;

/// 跨「整轮重试」复用的目录列表缓存。
///
/// 抓取的重试粒度是整轮（`from_url` + `collect` 全部重来），大分享一轮要列几百个
/// 目录、耗时上百秒 —— 只因为最后一个目录超时就把前面几百个结果全丢掉重爬,既慢
/// 又会因请求量翻倍更容易再撞超时/风控,形成正反馈。
///
/// 这个缓存由 manager 在**单次 run 内**创建并跨重试传入,让重试变成「续爬」:
/// 已完整列过的目录直接命中缓存,不再发请求。
///
/// 只缓存**完整翻完页**的目录（提前 break 的不缓存），保证缓存值语义等价于
/// 一次成功的完整列举。生命周期仅限单次 run,不存在跨 run 的陈旧问题。
#[derive(Debug, Default)]
pub struct ScanCache {
    dirs: std::sync::Mutex<std::collections::HashMap<String, Vec<SharedFileInfo>>>,
}

impl ScanCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&self, dir: &str) -> Option<Vec<SharedFileInfo>> {
        self.dirs.lock().ok()?.get(dir).cloned()
    }

    fn put(&self, dir: String, files: Vec<SharedFileInfo>) {
        if let Ok(mut m) = self.dirs.lock() {
            m.insert(dir, files);
        }
    }

    /// 已缓存的完整目录数（日志/诊断用）
    pub fn len(&self) -> usize {
        self.dirs.lock().map(|m| m.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 单个 list 请求的重试次数（不含首次）。默认 3。
/// 可用 `BAIDUPCS_SHARE_SYNC_LIST_RETRIES` 覆盖（设 0 关闭）。
fn list_retry_limit() -> u32 {
    std::env::var("BAIDUPCS_SHARE_SYNC_LIST_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

/// 单个 list 请求重试的基础退避（毫秒），第 n 次等待 = base × 2^n。默认 800。
fn list_retry_backoff_ms() -> u64 {
    std::env::var("BAIDUPCS_SHARE_SYNC_LIST_BACKOFF_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(800)
}

/// 抓取器：递归 list 整个分享内容
///
/// 内部约束：
/// - 每页拉 100 条；翻页直到 errno/空列表
/// - 递归使用 BFS 队列，避免深栈
/// - 应用 include_paths / exclude_patterns 过滤
/// - 单个 list 请求自带退避重试；已完整列过的目录走 `ScanCache`,整轮重试时续爬
pub struct SnapshotCollector<'a> {
    client: &'a NetdiskClient,
    short_key: String,
    shareid: String,
    uk: String,
    share_uk: String,
    bdstoken: String,
    /// 分享体系类型（个人版 / 企业版），决定列目录走哪套接口
    kind: crate::transfer::ShareKind,
    password: Option<String>,
    randsk: Option<String>,
    include_paths: BTreeSet<String>,
    /// 预计算的 include 路径前缀索引（dir 维度）：
    /// - `ancestors`：所有 include_path 的祖先 + include_path 自身的并集
    ///   命中此集合的目录是值得递归进入的（O(log N) 查询）
    include_index: BTreeSet<String>,
    /// 排除规则（精确目录路径 + glob 模式）
    exclusions: ExclusionRules,
    /// 取消令牌：触发后立即中断扫描
    cancel: Option<CancellationToken>,
    /// v2 阶段 6 补全:列目录抓快照同样走全局风控限速器。
    /// 与 ProductionHooks 的 submit_transfer/submit_download 共用同一个令牌桶,
    /// 因此「列目录 + 转存提交」合计受同一个全局 RPS 上限约束 — 大分享单轮
    /// BFS 翻页的 list 突发是最容易撞百度风控 errno=132 的地方,必须限速。
    rate_limiter: Arc<QuotaLimiter>,
    /// 扫描进度回调（可选）。每列完一个目录调一次,由调用方节流。
    progress: Option<ScanProgressSink>,
    /// 跨整轮重试的目录缓存（可选）。命中则跳过网络请求。
    cache: Option<Arc<ScanCache>>,
}

impl<'a> SnapshotCollector<'a> {
    /// 把散落的分享字段收成 `SharePageInfo`，交给按体系分流的客户端方法
    fn share_info(&self) -> crate::transfer::SharePageInfo {
        crate::transfer::SharePageInfo {
            shareid: self.shareid.clone(),
            uk: self.uk.clone(),
            share_uk: self.share_uk.clone(),
            bdstoken: self.bdstoken.clone(),
            kind: self.kind,
            short_key: self.short_key.clone(),
        }
    }

    /// 从 URL + 密码构造一个 collector（先访问分享页取 bdstoken）
    ///
    /// 排除规则在**任何网络访问之前**编译：非法规则直接以 `ConfigError` 失败，
    /// 不会静默退化成「不做任何排除」。
    pub async fn from_url(
        client: &'a NetdiskClient,
        share_url: &str,
        password: Option<String>,
        include_paths: Vec<String>,
        exclude_patterns: Vec<String>,
        exclude_regex_patterns: Vec<String>,
        exclude_paths: Vec<String>,
        rate_limiter: Arc<QuotaLimiter>,
    ) -> Result<Self, ShareSyncError> {
        let compiled_exclusions =
            ExclusionRules::compile(exclude_paths, &exclude_patterns, &exclude_regex_patterns)
                .map_err(|e| ShareSyncError::ConfigError(e.to_string()))?;

        let mut share_link = client
            .parse_share_link(share_url)
            // 用 `{:#}` 保留完整 anyhow 错误链（含底层超时/百度 errno+errmsg），
            // 否则 `to_string()` 只取最外层 context，排障时看不到百度到底返回了什么。
            .map_err(|e| ShareSyncError::ShareLinkError(format!("{:#}", e)))?;
        let effective_pwd = password.or(share_link.password.clone());
        share_link.password = effective_pwd.clone();

        let page = client
            .access_share_page_for(&share_link, true)
            .await
            // 用 `{:#}` 保留完整 anyhow 错误链（含底层超时/百度 errno+errmsg），
            // 否则 `to_string()` 只取最外层 context，排障时看不到百度到底返回了什么。
            .map_err(|e| ShareSyncError::ShareLinkError(format!("{:#}", e)))?;

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
                match client.verify_share_password_for(&page, pwd).await {
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
        let exclusions = compiled_exclusions;
        Ok(Self {
            client,
            short_key: page.short_key,
            shareid: page.shareid,
            uk: page.uk,
            share_uk: page.share_uk,
            bdstoken: page.bdstoken,
            kind: page.kind,
            password: effective_pwd,
            randsk,
            include_paths,
            include_index,
            exclusions,
            cancel: None,
            rate_limiter,
            progress: None,
            cache: None,
        })
    }

    /// 注入扫描进度回调（每列完一个目录调用一次；调用方负责节流）
    pub fn with_progress(mut self, sink: ScanProgressSink) -> Self {
        self.progress = Some(sink);
        self
    }

    /// 注入取消令牌，扫描阶段被取消时立即中断
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// 注入跨重试的目录缓存,让整轮重试变成「续爬」而非「重爬」
    pub fn with_cache(mut self, cache: Arc<ScanCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// 抓取完整快照
    ///
    /// 流程：root list → BFS 遍历所有子目录 → 合并去重 → 过滤
    pub async fn collect(mut self) -> Result<(CapturedShare, ShareSnapshot), ShareSyncError> {
        let page_size: u32 = 100;

        // Step 1: root（同样带单请求级退避重试，避免整棵树因根列表一次抖动就重来）
        let root = {
            let max_retries = list_retry_limit();
            let base_delay = list_retry_backoff_ms();
            let mut attempt: u32 = 0;
            loop {
                self.rate_limiter.acquire().await;
                let r = self
                    .client
                    .list_share_files_for(
                        &self.share_info(),
                        1,
                        page_size,
                        self.randsk.as_deref(),
                    )
                    .await
                    // 用 `{:#}` 保留完整 anyhow 错误链（含底层超时/百度 errno+errmsg），
                    // 否则 `to_string()` 只取最外层 context，排障时看不到百度到底返回了什么。
                    .map_err(|e| ShareSyncError::ShareLinkError(format!("{:#}", e)));
                match r {
                    Ok(v) => break v,
                    Err(e) if e.should_retry() && attempt < max_retries => {
                        let backoff = base_delay.saturating_mul(1u64 << attempt);
                        tracing::warn!(
                            "share-sync: 列分享根目录临时失败，{}ms 后重试 attempt={}/{} err={}",
                            backoff,
                            attempt + 1,
                            max_retries,
                            e
                        );
                        if backoff > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                        }
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        };

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

        self.exclusions.remap_exact_paths(&share_root);

        self.check_cancelled()?;

        let mut all_items: Vec<ShareSnapshotItem> = Vec::new();
        let mut seen: HashSet<(String, u64)> = HashSet::new();
        let mut queued_dirs: HashSet<String> = HashSet::new();
        let mut found_included_files: BTreeSet<String> = BTreeSet::new();
        // 扫描进度计数（仅用于回调展示，不参与抓取逻辑）
        let mut files_seen: usize = 0;
        let mut dirs_done: usize = 0;
        let mut cached_hits: usize = 0;

        // 推入 root（与子目录吸收共用同一段逻辑，排除规则因此同样生效）
        {
            let mut root_queue: VecDeque<String> = VecDeque::new();
            absorb_entries(
                root.files,
                &share_root,
                &self.include_paths,
                &self.include_index,
                &self.exclusions,
                &mut all_items,
                &mut seen,
                &mut queued_dirs,
                &mut root_queue,
                &mut found_included_files,
                &mut files_seen,
            );
        }

        // Step 2: BFS 子目录
        let mut queue: VecDeque<String> = queued_dirs.iter().cloned().collect();

        // 首帧进度：让前端在第一个目录还没列完时就能从「运行中」切到「扫描中」。
        self.emit_progress(ScanProgress {
            dirs_done,
            dirs_pending: queue.len(),
            files_seen,
            current_dir: share_root.clone(),
            cached_hits,
        });

        while let Some(dir) = queue.pop_front() {
            self.check_cancelled()?;
            let normalized_dir =
                normalize_share_path(&dir, dir.rsplit('/').next().unwrap_or(&dir), &share_root);
            if !self.dir_allowed(&normalized_dir) {
                continue;
            }

            // 缓存命中 → 本轮不发任何请求，直接复用上一轮已列完的结果（续爬）。
            if let Some(hit) = self.cache.as_ref().and_then(|c| c.get(&dir)) {
                cached_hits += 1;
                absorb_entries(
                    hit,
                    &share_root,
                    &self.include_paths,
                    &self.include_index,
                    &self.exclusions,
                    &mut all_items,
                    &mut seen,
                    &mut queued_dirs,
                    &mut queue,
                    &mut found_included_files,
                    &mut files_seen,
                );
                dirs_done += 1;
                self.emit_progress(ScanProgress {
                    dirs_done,
                    dirs_pending: queue.len(),
                    files_seen,
                    current_dir: normalized_dir.clone(),
                    cached_hits,
                });
                continue;
            }

            // 本目录累计到的全部条目。**只有完整翻完页**才写入缓存 ——
            // 被 include 短路提前 break 的结果是不完整的，缓存下来会让后续
            // 重试漏掉条目。
            let mut collected: Vec<SharedFileInfo> = Vec::new();
            let mut fully_paged = false;
            let mut page: u32 = 1;
            loop {
                self.check_cancelled()?;
                let batch = self
                    .list_dir_page_with_retry(
                        &root_shareid,
                        &root_uk,
                        &dir,
                        page,
                        page_size,
                    )
                    .await?;

                if batch.is_empty() {
                    fully_paged = true;
                    break;
                }

                let batch_len = batch.len();
                // 只有开了缓存才需要留副本；否则大目录每页白克隆 100 条。
                if self.cache.is_some() {
                    collected.extend(batch.iter().cloned());
                }
                absorb_entries(
                    batch,
                    &share_root,
                    &self.include_paths,
                    &self.include_index,
                    &self.exclusions,
                    &mut all_items,
                    &mut seen,
                    &mut queued_dirs,
                    &mut queue,
                    &mut found_included_files,
                    &mut files_seen,
                );

                if !self.dir_needs_more_pages(&normalized_dir, &found_included_files) {
                    break;
                }
                if batch_len < page_size as usize {
                    fully_paged = true;
                    break;
                }
                page += 1;
                if page > 10_000 {
                    return Err(ShareSyncError::Internal(
                        "递归层数/翻页数超过安全上限，可能存在循环引用".into(),
                    ));
                }
            }

            if fully_paged {
                if let Some(cache) = self.cache.as_ref() {
                    cache.put(dir.clone(), collected);
                }
            }

            dirs_done += 1;
            self.emit_progress(ScanProgress {
                dirs_done,
                dirs_pending: queue.len(),
                files_seen,
                current_dir: normalized_dir.clone(),
                cached_hits,
            });
        }

        // Step 3: 过滤 + 标记（抽成自由函数，见 `filter_and_mark_pruned` 注释）
        let filtered = filter_and_mark_pruned(
            all_items,
            &self.include_paths,
            &self.include_index,
            &self.exclusions,
        );

        let captured = CapturedShare {
            short_key: self.short_key.clone(),
            shareid: root_shareid,
            uk: root_uk,
            share_uk: self.share_uk.clone(),
            bdstoken: self.bdstoken.clone(),
            kind: self.kind,
            password: self.password.clone(),
            randsk: self.randsk.clone(),
        };
        let snap = ShareSnapshot::with_items(
            /*subscription_id*/ "", // 由调用方在 manager 处填
            filtered,
        );
        Ok((captured, snap))
    }

    /// 触发一次扫描进度回调（无回调时零开销）。节流由回调实现方负责。
    fn emit_progress(&self, p: ScanProgress) {
        if let Some(sink) = self.progress.as_ref() {
            sink(p);
        }
    }

    /// 扫描被取消即返回错误；检查点覆盖每个目录与分页。
    fn check_cancelled(&self) -> Result<(), ShareSyncError> {
        cancelled_error(self.cancel.as_ref())
    }

    /// 列单个子目录的一页，**单请求级**退避重试。
    ///
    /// 抓取的外层重试粒度是整轮（重新 `from_url` + 重爬整棵树）。大分享一轮几百个
    /// 目录、上百秒，仅仅因为某一个请求超时就整轮作废，代价极高且会因请求量翻倍
    /// 更容易再次撞上超时/风控。绝大多数抖动重发一次就能成功，所以在这里先自救，
    /// 把外层整轮重试留给「连分享页都访问不了」这类真·硬故障。
    ///
    /// 只对 `should_retry()`（临时类）错误重试；链接失效 / 风控等确定性错误立即上抛。
    async fn list_dir_page_with_retry(
        &self,
        root_shareid: &str,
        root_uk: &str,
        dir: &str,
        page: u32,
        page_size: u32,
    ) -> Result<Vec<SharedFileInfo>, ShareSyncError> {
        let max_retries = list_retry_limit();
        let base_delay = list_retry_backoff_ms();
        let mut attempt: u32 = 0;
        loop {
            self.rate_limiter.acquire().await;
            // 子目录用**根目录响应**里的 shareid/uk（比分享页解析出的更权威）
            let dir_share_info = crate::transfer::SharePageInfo {
                shareid: root_shareid.to_string(),
                uk: root_uk.to_string(),
                ..self.share_info()
            };
            let result = self
                .client
                .list_share_files_in_dir_for(
                    &dir_share_info,
                    dir,
                    page,
                    page_size,
                    self.randsk.as_deref(),
                )
                .await
                // 用 `{:#}` 保留完整 anyhow 错误链（含底层超时/百度 errno+errmsg），
                // 否则 `to_string()` 只取最外层 context，排障时看不到百度到底返回了什么。
                .map_err(|e| ShareSyncError::ShareLinkError(format!("{:#}", e)));

            match result {
                Ok(batch) => return Ok(batch),
                Err(e) if e.should_retry() && attempt < max_retries => {
                    let backoff = base_delay.saturating_mul(1u64 << attempt);
                    tracing::warn!(
                        "share-sync: 列目录临时失败，{}ms 后重试 dir={} page={} attempt={}/{} err={}",
                        backoff,
                        dir,
                        page,
                        attempt + 1,
                        max_retries,
                        e
                    );
                    if backoff > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    }
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn dir_allowed(&self, dir: &str) -> bool {
        dir_allowed_with(
            &self.include_paths,
            &self.include_index,
            &self.exclusions,
            dir,
        )
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
    build_include_index_impl(include_paths)
}

/// 返回 `path` 的所有真祖先目录路径（不含虚拟根 `/`，也不含自身）。
///
/// 例：`/a/b/c.mp4` → `["/a/b", "/a"]`；`/a` → `[]`
fn ancestor_dirs(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = path.trim_end_matches('/');
    while let Some(slash) = cur.rfind('/') {
        if slash == 0 {
            break; // 再往上就是虚拟根 "/"，不纳入
        }
        cur = &cur[..slash];
        out.push(cur.to_string());
    }
    out
}

fn build_include_index_impl(include_paths: &BTreeSet<String>) -> BTreeSet<String> {
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

/// 取消检查：令牌已触发时返回「扫描已取消」。
///
/// 自由函数形式便于脱离 collector 直接测试。
pub fn cancelled_error(cancel: Option<&CancellationToken>) -> Result<(), ShareSyncError> {
    if cancel.is_some_and(|t| t.is_cancelled()) {
        return Err(ShareSyncError::ScanCancelled("用户取消扫描".into()));
    }
    Ok(())
}

/// 排除规则集合：精确目录路径（`exclude_paths`）+ glob / 正则模式。
///
/// 三类规则的语义：
/// - **精确路径**：命中的目录连同其中全部内容一起排除，且扫描时不再列举该目录。
/// - **glob / 正则**：路径命中即排除；**目录**命中时其整棵子树一起排除。
///
/// 精确路径的匹配是**边界感知**的：`/a/b` 不会误伤同层的 `/a/bc`。
#[derive(Debug, Default, Clone)]
pub struct ExclusionRules {
    /// 精确排除的目录路径（相对分享根）
    exact_paths: BTreeSet<String>,
    /// 预编译的 glob + 正则规则
    rules: CompiledRules,
}

impl ExclusionRules {
    /// 编译排除规则（精确目录 + glob + 正则）。
    ///
    /// `Err` 表示规则非法：调用方必须显式失败，**不得**退化成「不做任何排除」。
    pub fn compile(
        exclude_paths: Vec<String>,
        exclude_patterns: &[String],
        exclude_regex_patterns: &[String],
    ) -> Result<Self, crate::share_sync::exclusion::RuleError> {
        let exact_paths: BTreeSet<String> = exclude_paths
            .into_iter()
            .filter_map(normalize_snapshot_path)
            .collect();
        let rules = CompiledRules::compile(exclude_patterns, exclude_regex_patterns)?;
        Ok(Self::new(exact_paths, rules))
    }

    fn new(exact_paths: BTreeSet<String>, rules: CompiledRules) -> Self {
        Self { exact_paths, rules }
    }

    /// 没有任何排除规则
    pub fn is_empty(&self) -> bool {
        self.exact_paths.is_empty() && self.rules.is_empty()
    }

    /// 精确路径是否覆盖 `path`（`path` 自身或其后代）
    fn exact_hit(&self, path: &str) -> bool {
        self.exact_paths
            .iter()
            .any(|p| is_path_ancestor_or_self(path, p))
    }

    /// glob / 正则是否命中 `path` 自身或其祖先目录
    fn pattern_hit(&self, path: &str) -> bool {
        self.rules.matches_path(path)
    }

    /// 该目录是否应被整棵剪掉（不再列举，其内容不出现在快照里）
    pub fn excludes_dir(&self, dir: &str) -> bool {
        self.exact_hit(dir) || self.pattern_hit(dir)
    }

    /// 该条目是否应被排除
    pub fn excludes_item(&self, path: &str, is_dir: bool) -> bool {
        if is_dir {
            return self.excludes_dir(path);
        }
        self.exact_hit(path) || self.pattern_hit(path)
    }

    /// 把精确排除路径归一到「相对分享根」命名空间（与快照条目 path 对齐）。
    ///
    /// 用户挑的可能是分享内绝对路径或 sharelink 合成路径；不归一时非根分享
    /// （分享根是某个目录）的排除规则匹配不到任何条目。
    fn remap_exact_paths(&mut self, share_root: &str) {
        if self.exact_paths.is_empty() {
            return;
        }
        self.exact_paths = self
            .exact_paths
            .iter()
            .map(|p| remap_include_to_share_root(p, share_root))
            .collect();
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

/// 返回 `true` 表示这是首次见到该条目（已推入 `out`）；`false` 表示重复被丢弃。
/// 调用方据此累计「已发现文件数」，避免重复计数。
fn push_unique(
    out: &mut Vec<ShareSnapshotItem>,
    seen: &mut HashSet<(String, u64)>,
    share_root: &str,
    info: SharedFileInfo,
) -> bool {
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
        true
    } else {
        false
    }
}

/// `SnapshotCollector::dir_allowed` 的自由函数版本。
///
/// BFS 主循环里要一边持有 `&mut` 的本地状态（队列 / 去重集）一边判断目录准入，
/// 借用 `&self` 会和这些可变借用打架，所以把判定逻辑抽成不依赖 `self` 的形式，
/// 供 `absorb_entries` 复用。两者语义必须保持一致。
fn dir_allowed_with(
    include_paths: &BTreeSet<String>,
    include_index: &BTreeSet<String>,
    exclusions: &ExclusionRules,
    dir: &str,
) -> bool {
    // 排除优先：命中的目录整棵剪掉 —— 既不列举它，也不入队。
    if exclusions.excludes_dir(dir) {
        return false;
    }
    if include_paths.is_empty() {
        return true;
    }
    if include_index.contains(dir) {
        return true;
    }
    // dir 是某个 include **目录**的后代 → 用户圈选的是整棵子树，必须继续深入。
    // 漏掉这一支时（issue #128 追加反馈的「多层级过滤失效」根因），BFS 在
    // include 目录下一层就截断：深层内容从未被扫描，exclude 过滤与
    // subtree_pruned 标记都无从发生，后续整目录 fs_id 直传会让百度服务端
    // 把被排除的深层内容原样递归复制回来。
    // `item_allowed_with` / `dir_needs_more_pages` 均有同款后代判断，三者语义须一致。
    include_paths
        .iter()
        .any(|inc| is_path_ancestor_or_self(dir, inc))
}

/// 单条目 include/exclude 准入判断（原 `SnapshotCollector::item_allowed`）。
/// 做成自由函数是为了让 `filter_and_mark_pruned` 可以脱离 collector（不依赖
/// 网络客户端）被单元测试直接驱动。
fn item_allowed_with(
    include_paths: &BTreeSet<String>,
    include_index: &BTreeSet<String>,
    exclusions: &ExclusionRules,
    item: &ShareSnapshotItem,
) -> bool {
    // 1) include 过滤：item.path 是某个 include_path 自身/祖先（索引命中）或后代
    if !include_paths.is_empty() && !include_index.contains(&item.path) {
        // 二级检查：item.path 是否是某个 include_path 的后代（include 选了目录，
        // 它下面的所有文件/子目录都应被收录）
        let mut allowed = false;
        for inc in include_paths {
            if is_path_ancestor_or_self(&item.path, inc) {
                allowed = true;
                break;
            }
        }
        if !allowed {
            return false;
        }
    }
    // 2) exclude 过滤：精确目录排除（连同整棵子树）或 glob 命中
    if exclusions.excludes_item(&item.path, item.is_dir) {
        return false;
    }
    true
}

/// 抓取 Step 3：应用 include/exclude 过滤，并给「有后代被剔除」的存活目录打
/// `subtree_pruned` 标记，返回按 path 字典序排好的最终快照条目。
///
/// 为什么要标记：转存阶段若把某个目录当作单个 fs_id 整体直传，百度服务端会按
/// fs_id **递归复制整目录**，连带把这里被过滤掉的子项也搬过去 —— 过滤形同虚设。
/// 标记后转存阶段会拒绝对这类目录整目录直传、展开到子节点逐层提交（executor 的
/// `transfer_node_set` 入口），从而真正跳过被过滤的分支。
///
/// 抽成自由函数：`collect()` 依赖真实网络客户端无法单测，而这段过滤/标记逻辑
/// 恰是 issue #128（多层级过滤失效）的回归高发区，必须可以被直接测试。
fn filter_and_mark_pruned(
    all_items: Vec<ShareSnapshotItem>,
    include_paths: &BTreeSet<String>,
    include_index: &BTreeSet<String>,
    exclusions: &ExclusionRules,
) -> Vec<ShareSnapshotItem> {
    let mut pruned_ancestors: BTreeSet<String> = BTreeSet::new();
    let mut filtered: Vec<ShareSnapshotItem> = all_items
        .into_iter()
        .filter(|it| {
            if item_allowed_with(include_paths, include_index, exclusions, it) {
                true
            } else {
                for anc in ancestor_dirs(&it.path) {
                    pruned_ancestors.insert(anc);
                }
                false
            }
        })
        .collect();

    // 给存活下来的、且有后代被剔除的目录条目打标。
    if !pruned_ancestors.is_empty() {
        for it in filtered.iter_mut() {
            if it.is_dir && pruned_ancestors.contains(&it.path) {
                it.subtree_pruned = true;
            }
        }
    }

    // 排序（path 字典序）
    filtered.sort_by(|a, b| a.path.cmp(&b.path));
    filtered
}

/// 把一批 list 结果并入抓取状态：去重推入快照、记录命中的 include 文件、
/// 把新目录压进 BFS 队列。
///
/// 抽成自由函数是为了让「网络拉取」和「缓存命中」两条路径共用同一段吸收逻辑 ——
/// 两边行为一旦分叉，缓存命中的那轮就会漏掉子目录或漏计文件数。
#[allow(clippy::too_many_arguments)]
fn absorb_entries(
    entries: Vec<SharedFileInfo>,
    share_root: &str,
    include_paths: &BTreeSet<String>,
    include_index: &BTreeSet<String>,
    exclusions: &ExclusionRules,
    all_items: &mut Vec<ShareSnapshotItem>,
    seen: &mut HashSet<(String, u64)>,
    queued_dirs: &mut HashSet<String>,
    queue: &mut VecDeque<String>,
    found_included_files: &mut BTreeSet<String>,
    files_seen: &mut usize,
) {
    for f in entries {
        let is_dir = f.is_dir;
        let raw_path = f.path.clone();
        let normalized_path = normalize_share_path(&f.path, &f.name, share_root);
        if !is_dir && include_paths.contains(&normalized_path) {
            found_included_files.insert(normalized_path.clone());
        }
        let inserted = push_unique(all_items, seen, share_root, f);
        if inserted && !is_dir {
            *files_seen += 1;
        }
        // 被排除的目录不入队，其内容永远不会被列举（根入队与网络/缓存吸收共用此路径）。
        if is_dir
            && dir_allowed_with(include_paths, include_index, exclusions, &normalized_path)
            && queued_dirs.insert(raw_path.clone())
        {
            queue.push_back(raw_path);
        }
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
    use crate::share_sync::exclusion::glob_to_regex;
    use regex::RegexSet;

    #[test]
    fn test_ancestor_dirs() {
        assert_eq!(
            ancestor_dirs("/a/b/c.mp4"),
            vec!["/a/b".to_string(), "/a".to_string()]
        );
        assert_eq!(ancestor_dirs("/a"), Vec::<String>::new());
        assert_eq!(ancestor_dirs("/a/b"), vec!["/a".to_string()]);
        // 尾部斜杠不影响
        assert_eq!(ancestor_dirs("/a/b/"), vec!["/a".to_string()]);
    }

    fn item(path: &str, fs_id: u64, size: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, size, false)
    }

    fn dir_item(path: &str, fs_id: u64) -> ShareSnapshotItem {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        ShareSnapshotItem::new(path, name, fs_id, 0, true)
    }

    /// 无任何排除规则的快捷构造
    fn no_exclusions() -> ExclusionRules {
        ExclusionRules::default()
    }

    /// 精确目录排除 + glob / 正则排除的组合
    fn exclusions(exact: &[&str], patterns: &[&str]) -> ExclusionRules {
        let exact_paths: BTreeSet<String> = exact.iter().map(|p| p.to_string()).collect();
        let globs: Vec<String> = patterns.iter().map(|p| p.to_string()).collect();
        let rules = CompiledRules::compile(&globs, &[]).expect("测试 glob 应可编译");
        ExclusionRules::new(exact_paths, rules)
    }

    /// ScanCache 的读写与计数。整轮重试靠它续爬，命中即意味着少发一批请求。
    #[test]
    fn test_scan_cache_roundtrip() {
        let cache = ScanCache::new();
        assert!(cache.is_empty());
        assert!(cache.get("/a").is_none());

        let files = vec![
            shared_file("/a/1.mp4", "1.mp4", 11, false),
            shared_file("/a/sub", "sub", 12, true),
        ];
        cache.put("/a".into(), files.clone());

        assert_eq!(cache.len(), 1);
        let hit = cache.get("/a").expect("刚写入的目录应命中");
        assert_eq!(hit.len(), 2);
        assert_eq!(hit[0].fs_id, 11);
        assert!(hit[1].is_dir);
        // 未写入的目录不应命中，避免把「没爬过」误当成「爬完了是空的」
        assert!(cache.get("/b").is_none());
    }

    /// `absorb_entries` 是「网络拉取」和「缓存命中」两条路径共用的吸收逻辑，
    /// 必须做到：文件计数去重、目录入队去重、include 命中被记录。
    #[test]
    fn test_absorb_entries_dedups_and_enqueues_dirs() {
        let include_paths: BTreeSet<String> = BTreeSet::new();
        let include_index: BTreeSet<String> = BTreeSet::new();
        let mut all_items = Vec::new();
        let mut seen = HashSet::new();
        let mut queued_dirs = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut found = BTreeSet::new();
        let mut files_seen = 0usize;

        let batch = vec![
            shared_file("/a/1.mp4", "1.mp4", 11, false),
            shared_file("/a/sub", "sub", 12, true),
        ];
        absorb_entries(
            batch.clone(),
            "",
            &include_paths,
            &include_index,
            &no_exclusions(),
            &mut all_items,
            &mut seen,
            &mut queued_dirs,
            &mut queue,
            &mut found,
            &mut files_seen,
        );
        assert_eq!(files_seen, 1, "只有文件计数，目录不算");
        assert_eq!(queue.len(), 1, "子目录应入队");

        // 同一批再吸收一次（模拟重试时缓存与网络结果重叠）：不得重复计数/重复入队
        absorb_entries(
            batch,
            "",
            &include_paths,
            &include_index,
            &no_exclusions(),
            &mut all_items,
            &mut seen,
            &mut queued_dirs,
            &mut queue,
            &mut found,
            &mut files_seen,
        );
        assert_eq!(files_seen, 1, "重复条目不应重复计数");
        assert_eq!(queue.len(), 1, "重复目录不应重复入队");
        assert_eq!(all_items.len(), 2, "快照条目应保持去重");
    }

    /// `dir_allowed_with` 必须与 `SnapshotCollector::dir_allowed` 语义一致 ——
    /// 两者分叉会让缓存命中的那轮漏爬子目录。
    #[test]
    fn test_dir_allowed_with_matches_collector_semantics() {
        // 无 include 限制 → 全部放行
        let empty = BTreeSet::new();
        assert!(dir_allowed_with(
            &empty,
            &empty,
            &no_exclusions(),
            "/anything"
        ));

        // include 是具体文件 → 放行其祖先（用于走到该文件），不放行无关目录
        let mut includes = BTreeSet::new();
        includes.insert("/a/b/c.mp4".to_string());
        let index = build_include_index(&includes);
        assert!(dir_allowed_with(&includes, &index, &no_exclusions(), "/a"));
        assert!(dir_allowed_with(
            &includes,
            &index,
            &no_exclusions(),
            "/a/b"
        ));
        assert!(!dir_allowed_with(
            &includes,
            &index,
            &no_exclusions(),
            "/other"
        ));
        // 文件 include 没有"后代目录"概念：同层其它目录不放行（避免全树扫描）
        assert!(!dir_allowed_with(
            &includes,
            &index,
            &no_exclusions(),
            "/a/b/other_dir"
        ));

        // include 是目录 → **后代目录必须放行**（issue #128 多层级过滤失效的根因：
        // 只查索引会在 include 目录下一层截断 BFS，深层内容从未被扫描，
        // exclude / subtree_pruned 全部失效）
        let mut dir_includes = BTreeSet::new();
        dir_includes.insert("/大主宰3D".to_string());
        let dir_index = build_include_index(&dir_includes);
        assert!(dir_allowed_with(
            &dir_includes,
            &dir_index,
            &no_exclusions(),
            "/大主宰3D"
        ));
        assert!(
            dir_allowed_with(
                &dir_includes,
                &dir_index,
                &no_exclusions(),
                "/大主宰3D/S02 连载中"
            ),
            "include 目录的一级子目录必须放行"
        );
        assert!(
            dir_allowed_with(
                &dir_includes,
                &dir_index,
                &no_exclusions(),
                "/大主宰3D/S02 连载中/S02 4K NoSub/字幕文件"
            ),
            "include 目录的任意深度后代都必须放行"
        );
        assert!(!dir_allowed_with(
            &dir_includes,
            &dir_index,
            &no_exclusions(),
            "/其他目录"
        ));
        // 前缀相似但非路径后代：/大主宰3Dxx 不是 /大主宰3D 的后代
        assert!(!dir_allowed_with(
            &dir_includes,
            &dir_index,
            &no_exclusions(),
            "/大主宰3Dxx"
        ));
    }

    /// 端到端复现 issue #128 追加反馈：include 圈定目录 + exclude 排除深层内容。
    /// 模拟修复后 BFS 应扫出的完整条目集，走真实的 `filter_and_mark_pruned`：
    /// 深层被排除项必须剔除，且其祖先链每一层都被标 `subtree_pruned`（否则转存
    /// 阶段整目录直传会把它们带回来）。
    #[test]
    fn test_issue128_deep_exclude_marks_whole_ancestor_chain() {
        let mut includes = BTreeSet::new();
        includes.insert("/大主宰3D".to_string());
        let index = build_include_index(&includes);

        let mk_dir = |path: &str, fs_id: u64| {
            let name = path.rsplit('/').next().unwrap().to_string();
            ShareSnapshotItem::new(path, name, fs_id, 0, true)
        };
        // BFS(修复后)扫出的全量条目：含深层 zip / Soft / S01
        let all_items = vec![
            mk_dir("/大主宰3D", 1),
            mk_dir("/大主宰3D/S01", 6), // 一级排除
            mk_dir("/大主宰3D/S02 连载中", 2),
            mk_dir("/大主宰3D/S02 连载中/S02 4K NoSub", 3),
            mk_dir("/大主宰3D/S02 连载中/S02 4K NoSub/字幕文件", 4),
            item("/大主宰3D/S02 连载中/S02 4K NoSub/字幕文件/56.zip", 41, 1),
            item(
                "/大主宰3D/S02 连载中/S02 4K NoSub/E01.4K.Soft-GM.mp4",
                31,
                100,
            ),
            item(
                "/大主宰3D/S02 连载中/S02 4K NoSub/E01.4K.HEVC-GM.mp4",
                32,
                100,
            ),
        ];

        let out = filter_and_mark_pruned(
            all_items,
            &includes,
            &index,
            &exclusions(&[], &["*S01*", "*Soft*", "*.zip*"]),
        );
        let by_path: BTreeMap<&str, &ShareSnapshotItem> =
            out.iter().map(|i| (i.path.as_str(), i)).collect();

        // 被排除项（无论层级）不得存活
        for gone in [
            "/大主宰3D/S01",
            "/大主宰3D/S02 连载中/S02 4K NoSub/字幕文件/56.zip",
            "/大主宰3D/S02 连载中/S02 4K NoSub/E01.4K.Soft-GM.mp4",
        ] {
            assert!(!by_path.contains_key(gone), "{gone} 应被排除");
        }
        // 存活文件仍在
        assert!(by_path.contains_key("/大主宰3D/S02 连载中/S02 4K NoSub/E01.4K.HEVC-GM.mp4"));
        // 关键：祖先链每一层都必须被标 pruned，禁止整目录直传
        for (dir, why) in [
            ("/大主宰3D", "含被排除的 S01 与深层内容"),
            ("/大主宰3D/S02 连载中", "含深层被排除内容"),
            ("/大主宰3D/S02 连载中/S02 4K NoSub", "直接含被排除的 Soft/zip"),
            ("/大主宰3D/S02 连载中/S02 4K NoSub/字幕文件", "全部子项被排除"),
        ] {
            let d = by_path
                .get(dir)
                .unwrap_or_else(|| panic!("{dir} 应存活于快照"));
            assert!(d.subtree_pruned, "{dir} 应被标 subtree_pruned（{why}）");
        }
    }

    /// absorb_entries 的目录入队必须放行 include 目录的后代（与 dir_allowed 同款
    /// 修复）：否则 BFS 在 include 目录下一层截断，深层内容永远扫不到。
    #[test]
    fn test_absorb_entries_enqueues_descendants_of_included_dir() {
        let mut includes = BTreeSet::new();
        includes.insert("/大主宰3D".to_string());
        let index = build_include_index(&includes);
        let mut all_items = Vec::new();
        let mut seen = HashSet::new();
        let mut queued_dirs = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut found = BTreeSet::new();
        let mut files_seen = 0usize;

        // 列 /大主宰3D 得到的一级条目：子目录 + 无关目录（不在 include 下）
        let batch = vec![
            shared_file("/大主宰3D/S02 连载中", "S02 连载中", 2, true),
            shared_file("/其他目录", "其他目录", 9, true),
        ];
        absorb_entries(
            batch,
            "",
            &includes,
            &index,
            &no_exclusions(),
            &mut all_items,
            &mut seen,
            &mut queued_dirs,
            &mut queue,
            &mut found,
            &mut files_seen,
        );
        assert!(
            queued_dirs.contains("/大主宰3D/S02 连载中"),
            "include 目录的子目录必须入队继续深入: {:?}",
            queued_dirs
        );
        assert!(
            !queued_dirs.contains("/其他目录"),
            "include 范围外的目录不应入队: {:?}",
            queued_dirs
        );
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
        let files = vec![shared_file("/13/a测试上传1/scan_test", "scan_test", 1, true)];
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
            .map(|p| format!("^{}$", glob_to_regex(p)))
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
        assert_eq!(glob_to_regex("a.b"), "a\\.b");
        assert_eq!(glob_to_regex("a+b"), "a\\+b");
        assert_eq!(glob_to_regex("a(b)c"), "a\\(b\\)c");
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

    /// 精确目录 + glob + 正则的组合（正则单独成列，便于覆盖扫描侧语义）
    fn exclusions_with_regex(globs: &[&str], regexes: &[&str]) -> ExclusionRules {
        let to_vec = |items: &[&str]| items.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        ExclusionRules::compile(Vec::new(), &to_vec(globs), &to_vec(regexes))
            .expect("测试规则应可编译")
    }

    #[test]
    fn test_regex_dir_admission_prunes_whole_subtree() {
        let includes: BTreeSet<String> = BTreeSet::new();
        let index: BTreeSet<String> = BTreeSet::new();
        let rules = exclusions_with_regex(&[], &["^/股票/按月归档/临时文件$"]);

        // 命中的目录整棵剪掉（既不列举，也不入队）
        assert!(!dir_allowed_with(
            &includes,
            &index,
            &rules,
            "/股票/按月归档/临时文件"
        ));
        // 兄弟月份与其它父目录不受影响
        assert!(dir_allowed_with(
            &includes,
            &index,
            &rules,
            "/股票/按月归档/2026-10"
        ));
        assert!(dir_allowed_with(
            &includes,
            &index,
            &rules,
            "/指数/按月归档/2026-10"
        ));
        // 后代路径因祖先命中同样被剪
        assert!(!dir_allowed_with(
            &includes,
            &index,
            &rules,
            "/股票/按月归档/临时文件/深层"
        ));
    }

    #[test]
    fn test_absorb_entries_skips_regex_excluded_dirs_and_keeps_new_months() {
        let includes: BTreeSet<String> = BTreeSet::new();
        let index: BTreeSet<String> = BTreeSet::new();
        let rules = exclusions_with_regex(&[], &["^/股票/按月归档/2025.*$"]);
        let mut all_items = Vec::new();
        let mut seen = HashSet::new();
        let mut queued_dirs = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut found = BTreeSet::new();
        let mut files_seen = 0usize;

        // 列 /股票/按月归档 得到的一批：一个被正则命中的旧月份 + 一个新的月份
        let batch = vec![
            shared_file("/股票/按月归档/2025-01", "2025-01", 1, true),
            shared_file("/股票/按月归档/2026-10", "2026-10", 2, true),
            shared_file("/股票/按月归档/2026-10/说明.txt", "说明.txt", 3, false),
        ];
        absorb_entries(
            batch,
            "",
            &includes,
            &index,
            &rules,
            &mut all_items,
            &mut seen,
            &mut queued_dirs,
            &mut queue,
            &mut found,
            &mut files_seen,
        );

        assert!(
            !queued_dirs.contains("/股票/按月归档/2025-01"),
            "命中正则的目录不得入队"
        );
        assert!(
            queued_dirs.contains("/股票/按月归档/2026-10"),
            "新月份必须继续跟进"
        );
        // 该吸收函数同时服务网络与缓存两条路径，因此两种来源都受同一套剪枝约束
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_filter_and_mark_pruned_regex_subtree() {
        let includes: BTreeSet<String> = BTreeSet::new();
        let index: BTreeSet<String> = BTreeSet::new();
        let rules = exclusions_with_regex(&[], &["^/股票/按月归档/临时文件$"]);
        let all_items = vec![
            dir_item("/股票/按月归档", 1),
            dir_item("/股票/按月归档/临时文件", 2),
            item("/股票/按月归档/临时文件/缓存.bin", 3, 100),
            item("/股票/按月归档/2026-10/月报.csv", 4, 100),
        ];

        let out = filter_and_mark_pruned(all_items, &includes, &index, &rules);
        let by_path: BTreeMap<&str, &ShareSnapshotItem> =
            out.iter().map(|i| (i.path.as_str(), i)).collect();

        assert!(!by_path.contains_key("/股票/按月归档/临时文件"));
        assert!(!by_path.contains_key("/股票/按月归档/临时文件/缓存.bin"));
        assert!(by_path.contains_key("/股票/按月归档/2026-10/月报.csv"));
        // 被排除的子树会让存活祖先进入「禁止整目录直传」状态
        assert!(by_path["/股票/按月归档"].subtree_pruned);
    }

    #[test]
    fn test_regex_does_not_touch_sibling_or_other_parent() {
        let rules = exclusions_with_regex(&[], &["^/基金/按月归档/重复数据$"]);
        assert!(rules.excludes_dir("/基金/按月归档/重复数据"));
        assert!(
            !rules.excludes_dir("/基金/按月归档/2026-10"),
            "兄弟月份不受影响"
        );
        assert!(
            !rules.excludes_dir("/债券/按月归档/重复数据"),
            "其它父目录不受影响"
        );
        assert!(rules.excludes_item("/基金/按月归档/重复数据/a.csv", false));
    }

    #[test]
    fn test_invalid_regex_fails_compile_instead_of_disabling_excludes() {
        let err = ExclusionRules::compile(Vec::new(), &[], &["(".to_string()])
            .expect_err("非法正则必须显式失败");
        assert_eq!(err.index, Some(0));
        let mapped = ShareSyncError::ConfigError(err.to_string());
        assert!(mapped.to_string().contains("正则"));
    }

    #[test]
    fn test_compiled_rules_empty_is_empty() {
        let rules = CompiledRules::compile(&[], &[]).unwrap();
        assert!(rules.is_empty());
        assert!(!rules.matches_path("/a"));
    }

    #[test]
    fn test_compiled_rules_invalid_regex_is_error() {
        // 非法规则必须显式失败：不能静默退化成「不做任何排除」。
        let err = CompiledRules::compile(&[], &["(".to_string()]).unwrap_err();
        assert_eq!(err.kind, crate::share_sync::exclusion::RuleKind::Regex);
        assert_eq!(err.index, Some(0));
    }

    #[test]
    fn test_compiled_rules_glob_literals_compile() {
        assert!(CompiledRules::compile(&["[abc]".to_string()], &[]).is_ok());
        let rules =
            CompiledRules::compile(&["*.tmp".to_string(), "*.bak".to_string()], &[]).unwrap();
        assert!(!rules.is_empty());
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

    // ========== 精确目录排除 / glob 子树排除 ==========

    /// 精确排除的目录**不入队** —— 它的内容永远不会被列举。
    #[test]
    fn test_exact_excluded_dir_not_enqueued() {
        let mut includes = BTreeSet::new();
        let mut index = BTreeSet::new();
        let mut all_items = Vec::new();
        let mut seen = HashSet::new();
        let mut queued_dirs = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut found = BTreeSet::new();
        let mut files_seen = 0usize;

        // 根目录条目：被排除的目录 + 一个允许的兄弟目录
        let batch = vec![
            shared_file("/剧集/S01", "S01", 11, true),
            shared_file("/剧集/S02", "S02", 12, true),
        ];
        absorb_entries(
            batch,
            "",
            &includes,
            &index,
            &exclusions(&["/剧集/S01"], &[]),
            &mut all_items,
            &mut seen,
            &mut queued_dirs,
            &mut queue,
            &mut found,
            &mut files_seen,
        );

        assert!(
            !queued_dirs.contains("/剧集/S01"),
            "被排除的目录不得入队: {:?}",
            queued_dirs
        );
        assert!(
            queued_dirs.contains("/剧集/S02"),
            "允许的兄弟目录必须继续入队: {:?}",
            queued_dirs
        );
        // 允许的兄弟目录仍可继续深入（不被排除规则波及）
        includes.insert("/剧集".to_string());
        index = build_include_index(&includes);
        assert!(dir_allowed_with(
            &includes,
            &index,
            &exclusions(&["/剧集/S01"], &[]),
            "/剧集/S02"
        ));
    }

    /// 精确排除是边界感知的：`/剧集/S01` 不能误伤同层的 `/剧集/S01x`。
    #[test]
    fn test_exact_exclusion_is_boundary_aware() {
        let rules = exclusions(&["/剧集/S01"], &[]);
        assert!(rules.excludes_dir("/剧集/S01"));
        assert!(rules.excludes_dir("/剧集/S01/第1集"));
        assert!(!rules.excludes_dir("/剧集/S01x"));
        assert!(!rules.excludes_dir("/剧集/S012"));
        assert!(!rules.excludes_dir("/剧集/S02"));
        // 文件同样按「是否位于被排除目录之下」判定
        assert!(rules.excludes_item("/剧集/S01/第1集.mp4", false));
        assert!(!rules.excludes_item("/剧集/S01x.mp4", false));
    }

    /// 被排除目录及其后代不得出现在快照里，且**存活的祖先目录必须标 subtree_pruned** ——
    /// 否则转存阶段会把祖先目录当作单个 fs_id 整体直传，把被排除的内容原样带过去。
    #[test]
    fn test_excluded_dir_absent_and_ancestor_marked_pruned() {
        let includes = BTreeSet::new();
        let index = build_include_index(&includes);
        let all_items = vec![
            dir_item("/剧集", 1),
            dir_item("/剧集/S01", 2),
            dir_item("/剧集/S02", 3),
            item("/剧集/S01/第1集.mp4", 21, 100),
            item("/剧集/S02/第2集.mp4", 31, 100),
        ];

        let out = filter_and_mark_pruned(
            all_items,
            &includes,
            &index,
            &exclusions(&["/剧集/S01"], &[]),
        );
        let by_path: BTreeMap<&str, &ShareSnapshotItem> =
            out.iter().map(|i| (i.path.as_str(), i)).collect();

        assert!(!by_path.contains_key("/剧集/S01"), "被排除目录不得存活");
        assert!(
            !by_path.contains_key("/剧集/S01/第1集.mp4"),
            "被排除目录的后代不得存活"
        );
        assert!(by_path.contains_key("/剧集/S02"), "允许的兄弟目录应存活");
        let pruned = by_path.get("/剧集").expect("/剧集 应存活").subtree_pruned;
        assert!(pruned, "祖先目录必须标 subtree_pruned，禁止整目录直传");
        assert!(
            !by_path.get("/剧集/S02").unwrap().subtree_pruned,
            "未被剪枝的目录不应带标记"
        );
    }

    /// glob 命中的**目录**排除整棵子树。
    #[test]
    fn test_glob_matched_dir_excludes_whole_subtree() {
        let rules = exclusions(&[], &["*S01*"]);
        assert!(rules.excludes_dir("/剧集/S01"));
        assert!(rules.excludes_dir("/剧集/S01/第1集"));
        assert!(!rules.excludes_dir("/剧集/S02"));

        let includes = BTreeSet::new();
        let index = build_include_index(&includes);
        let all_items = vec![
            dir_item("/剧集", 1),
            dir_item("/剧集/S01", 2),
            dir_item("/剧集/S01/第1集", 3),
            item("/剧集/S01/第1集/a.mp4", 31, 100),
            dir_item("/剧集/S02", 4),
        ];
        let out = filter_and_mark_pruned(all_items, &includes, &index, &rules);
        let by_path: BTreeMap<&str, &ShareSnapshotItem> =
            out.iter().map(|i| (i.path.as_str(), i)).collect();
        assert!(!by_path.contains_key("/剧集/S01"));
        assert!(!by_path.contains_key("/剧集/S01/第1集"));
        assert!(by_path.contains_key("/剧集/S02"));
        assert!(by_path.get("/剧集").unwrap().subtree_pruned);
    }

    /// 排除目录路径要归一到「相对分享根」命名空间，否则非根分享里排除规则不生效。
    #[test]
    fn test_exclude_paths_remapped_to_share_root() {
        let mut rules = exclusions(&["/13/a测试上传1/scan_test/广告"], &[]);
        rules.remap_exact_paths("/13/a测试上传1");
        assert!(rules.excludes_dir("/scan_test/广告"));
        assert!(rules.excludes_dir("/scan_test/广告/x.mp4"));
        assert!(!rules.excludes_dir("/scan_test"));
    }

    /// 取消令牌被触发后，扫描立刻返回「已取消」而不是继续跑。
    #[test]
    fn test_check_cancelled_reports_cancelled() {
        let token = CancellationToken::new();
        assert!(cancelled_error(None).is_ok());
        assert!(cancelled_error(Some(&token)).is_ok());
        token.cancel();
        let err = cancelled_error(Some(&token)).unwrap_err();
        assert!(
            matches!(err, ShareSyncError::ScanCancelled(_)),
            "取消必须返回 ScanCancelled，实际: {:?}",
            err
        );
        // 取消不是可重试错误，也不算链接失效
        assert!(!err.should_retry());
        assert!(!err.is_link_invalid());
    }
}
