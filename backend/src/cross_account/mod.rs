//! 跨账号复制 / 移动 / 下载
//!
//! 百度网盘没有账号间直传接口，这里用「临时私密分享 + 自动转存 + 立即撤销分享」拼出来：
//!
//! 1. 源账号对选中项建一个带随机提取码、1 天有效的私密分享
//! 2. 目标账号走现成的转存链路（分批、超限拆分、瞬时错误重试都在 `TransferManager` 里）
//!    把分享转存到目标目录；该转存任务是内部任务，不出现在「转存管理」
//! 3. 不论转存成败，立即撤销分享
//! 4. 移动模式：转存**完全成功**、且逐项核对目标与源一致（文件数 + 总字节）后才删除源文件
//!    （进回收站）；任何一步无法确认都保留源文件
//!
//! 下载模式（用另一个账号下载，比如会员账号）就是自动生成链接的「分享直下」：第 2 步改为
//! 下载账号建一个普通的分享直下任务（转存到临时目录 → 自动下载 → 清理临时目录，与手动
//! 粘贴链接完全一样，在「转存管理」「下载管理」可见），转存阶段一结束就撤销分享——文件此时
//! 已在下载账号的临时目录里，后续下载不再依赖分享。
//!
//! 分享转存接口不支持 `ondup`：目标目录已有同名项时百度直接报错（errno=12 / -30），
//! 不会自动改名，所以建分享前先检查冲突，直接给出明确提示。
//!
//! 选中项必须同在一个目录下（handler 校验）：既与网页版分享一致，也排除了父子目录
//! 重叠、跨目录同名等情况。
//!
//! 撤销失败或进程中途退出时，shareid 留在 [`LEDGER_FILE`]，下次启动重试撤销；
//! 最后的兜底是分享本身 1 天后过期。

use crate::auth::Uid;
use crate::netdisk::{FileItem, NetdiskClient, ShareSetResponse};
use crate::server::AppState;
use crate::transfer::manager::{list_all_dir_entries, CreateTransferRequest};
use crate::transfer::TransferStatus;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// 内部转存任务 `backup_config_id` 前缀：`cross-account:{job_id}`
pub const INTERNAL_CONFIG_PREFIX: &str = "cross-account:";

/// 临时分享有效期（天）。百度可选 0(永久)/1/7/30，取最短的
const TEMP_SHARE_PERIOD_DAYS: i32 = 1;

/// 待撤销分享台账
const LEDGER_FILE: &str = "config/cross_account_shares.json";

/// 转存等待的硬上限：`TransferManager` 自身不会让任务无限挂起，这里只防意外
const TRANSFER_HARD_TIMEOUT: Duration = Duration::from_secs(6 * 3600);

/// 已结束任务在内存里保留多久（供前端轮询拿最终结果）
const FINISHED_JOB_TTL_SECS: i64 = 3600;

/// 移动前校验时，源 / 目标各自最多遍历多少个目录；超出则放弃校验、保留源文件
const VERIFY_MAX_DIRS: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossAccountMode {
    Copy,
    Move,
    /// 用目标账号下载到本地（分享直下）
    Download,
}

impl CrossAccountMode {
    fn label(self) -> &'static str {
        match self {
            Self::Copy => "复制",
            Self::Move => "移动",
            Self::Download => "下载",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossAccountStage {
    /// 检查源文件与目标目录同名冲突
    Checking,
    /// 源账号创建临时分享
    Sharing,
    /// 目标账号转存中（下载模式：转存到下载账号的临时目录）
    Transferring,
    /// 撤销临时分享
    Revoking,
    /// 核对目标与源一致（仅移动）
    Verifying,
    /// 删除源文件（仅移动）
    DeletingSource,
    Completed,
    Failed,
}

impl CrossAccountStage {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CrossAccountJob {
    pub id: String,
    pub mode: CrossAccountMode,
    pub source_uid: u64,
    pub target_uid: u64,
    pub paths: Vec<String>,
    /// 目标账号中的保存目录（下载模式为空）
    pub target_dir: String,
    /// 本地下载目录（仅下载模式）
    pub local_download_path: Option<String>,
    /// 目标账号上的转存任务 id（下载模式即分享直下任务，可在「转存管理」查看）
    pub transfer_task_id: Option<String>,
    pub stage: CrossAccountStage,
    pub transferred_count: usize,
    pub total_count: usize,
    /// 致命错误（stage=failed 时）
    pub error: Option<String>,
    /// 失败发生在哪个阶段（checking / sharing / transferring）；转存失败时会先撤销分享再置失败，
    /// 所以不能用失败前最后一个 stage 推断
    pub failed_stage: Option<CrossAccountStage>,
    /// 非致命问题：部分文件未转存、撤销分享失败、删除源文件失败等
    pub warnings: Vec<String>,
    /// 移动模式下源文件是否已删除
    pub source_deleted: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

impl CrossAccountJob {
    pub fn new(
        mode: CrossAccountMode,
        source_uid: Uid,
        target_uid: Uid,
        paths: Vec<String>,
        target_dir: String,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            mode,
            source_uid: source_uid.raw(),
            target_uid: target_uid.raw(),
            paths,
            target_dir,
            local_download_path: None,
            transfer_task_id: None,
            stage: CrossAccountStage::Checking,
            transferred_count: 0,
            total_count: 0,
            error: None,
            failed_stage: None,
            warnings: Vec::new(),
            source_deleted: false,
            created_at: now,
            updated_at: now,
        }
    }

    /// 下载模式：`target_uid` 为下载账号
    pub fn new_download(
        source_uid: Uid,
        target_uid: Uid,
        paths: Vec<String>,
        local_download_path: String,
    ) -> Self {
        let mut job = Self::new(
            CrossAccountMode::Download,
            source_uid,
            target_uid,
            paths,
            String::new(),
        );
        job.local_download_path = Some(local_download_path);
        job
    }
}

/// 待撤销的临时分享
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingShare {
    job_id: String,
    source_uid: u64,
    shareid: u64,
    created_at: i64,
}

/// 任务表 + 待撤销分享台账
pub struct CrossAccountRegistry {
    jobs: DashMap<String, CrossAccountJob>,
    ledger_path: PathBuf,
    /// 串行化台账文件的读改写
    ledger_lock: Mutex<()>,
}

impl Default for CrossAccountRegistry {
    fn default() -> Self {
        Self::with_ledger_path(PathBuf::from(LEDGER_FILE))
    }
}

impl CrossAccountRegistry {
    fn with_ledger_path(ledger_path: PathBuf) -> Self {
        Self {
            jobs: DashMap::new(),
            ledger_path,
            ledger_lock: Mutex::new(()),
        }
    }

    pub fn get(&self, id: &str) -> Option<CrossAccountJob> {
        self.jobs.get(id).map(|j| j.clone())
    }

    /// 全部任务，新的在前
    pub fn list(&self) -> Vec<CrossAccountJob> {
        let mut jobs: Vec<CrossAccountJob> = self.jobs.iter().map(|j| j.clone()).collect();
        jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        jobs
    }

    fn insert(&self, job: CrossAccountJob) {
        let now = chrono::Utc::now().timestamp();
        self.jobs
            .retain(|_, j| !(j.stage.is_terminal() && now - j.updated_at > FINISHED_JOB_TTL_SECS));
        self.jobs.insert(job.id.clone(), job);
    }

    fn update(&self, id: &str, f: impl FnOnce(&mut CrossAccountJob)) {
        if let Some(mut job) = self.jobs.get_mut(id) {
            f(&mut job);
            job.updated_at = chrono::Utc::now().timestamp();
        }
    }

    fn set_stage(&self, id: &str, stage: CrossAccountStage) {
        self.update(id, |j| j.stage = stage);
    }

    fn warn(&self, id: &str, msg: String) {
        warn!("跨账号任务 {}: {}", id, msg);
        self.update(id, |j| j.warnings.push(msg));
    }

    async fn ledger_load(&self) -> Vec<PendingShare> {
        match tokio::fs::read(&self.ledger_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                warn!("跨账号分享台账解析失败，按空处理: {}", e);
                Vec::new()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                warn!("读取跨账号分享台账失败: {}", e);
                Vec::new()
            }
        }
    }

    async fn ledger_store(&self, entries: &[PendingShare]) -> std::io::Result<()> {
        if let Some(parent) = self.ledger_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = self.ledger_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(entries).map_err(std::io::Error::other)?;
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, &self.ledger_path).await
    }

    async fn ledger_modify(&self, f: impl FnOnce(&mut Vec<PendingShare>)) {
        let _guard = self.ledger_lock.lock().await;
        let mut entries = self.ledger_load().await;
        f(&mut entries);
        if let Err(e) = self.ledger_store(&entries).await {
            warn!("写入跨账号分享台账失败: {}", e);
        }
    }

    async fn ledger_add(&self, entry: PendingShare) {
        self.ledger_modify(|v| v.push(entry)).await;
    }

    async fn ledger_remove(&self, shareid: u64) {
        self.ledger_modify(|v| v.retain(|e| e.shareid != shareid))
            .await;
    }
}

/// 登记任务并在后台执行，立即返回任务快照
///
/// 入参须已由 handler 校验：两个账号都存在且不同、路径已归一化。
pub fn spawn_job(state: &AppState, job: CrossAccountJob) -> CrossAccountJob {
    info!(
        "跨账号{}: job={} {} -> {} paths={:?} target_dir={} local_download_path={:?}",
        job.mode.label(),
        job.id,
        job.source_uid,
        job.target_uid,
        job.paths,
        job.target_dir,
        job.local_download_path
    );
    state.cross_account_jobs.insert(job.clone());
    let state = state.clone();
    let snapshot = job.clone();
    tokio::spawn(async move { run_job(state, job).await });
    snapshot
}

async fn run_job(state: AppState, job: CrossAccountJob) {
    let registry = state.cross_account_jobs.clone();
    let id = job.id.clone();
    let source = Uid::new(job.source_uid);
    let target = Uid::new(job.target_uid);

    // 0. 检查源文件都在、目标目录无同名项（分享转存遇同名直接失败，不会自动改名）。
    //    下载模式转存到每个任务独有的临时目录，不会冲突
    let items = match precheck(&state, &job).await {
        Ok(items) => items,
        Err(msg) => {
            fail(&registry, &id, CrossAccountStage::Checking, msg);
            return;
        }
    };

    // 1. 源账号建临时分享
    registry.set_stage(&id, CrossAccountStage::Sharing);
    let pwd = crate::server::handlers::share::generate_random_pwd();
    let share = match create_temp_share(&state, source, &job.paths, &pwd).await {
        Ok(s) => s,
        Err(msg) => {
            fail(&registry, &id, CrossAccountStage::Sharing, msg);
            return;
        }
    };
    registry
        .ledger_add(PendingShare {
            job_id: id.clone(),
            source_uid: source.raw(),
            shareid: share.shareid,
            created_at: chrono::Utc::now().timestamp(),
        })
        .await;

    // 2. 目标账号转存（下载模式：转存进临时目录后由分享直下自动下载）
    registry.set_stage(&id, CrossAccountStage::Transferring);
    let outcome = transfer_into_target(&state, &job, &share.link, &pwd).await;

    // 3. 不论成败都撤销分享
    registry.set_stage(&id, CrossAccountStage::Revoking);
    match revoke_share(&state, source, share.shareid).await {
        Ok(()) => registry.ledger_remove(share.shareid).await,
        Err(msg) => registry.warn(
            &id,
            format!(
                "撤销临时分享失败（{}），请到源账号「我的分享」手动取消；该分享 1 天后也会自动失效",
                msg
            ),
        ),
    }

    let (status, transfer_warning) = match outcome {
        Err(msg) => {
            fail(&registry, &id, CrossAccountStage::Transferring, msg);
            return;
        }
        Ok(done) => done,
    };

    // 分享直下的下载在本轮询看到「下载中」之前就整体失败了（如本地目录不可写）：
    // 转存已成功、分享已撤销，按下载失败报告，步骤停在「开始下载」而不是「转存」。
    // 复制 / 移动不自动下载，不会出现该状态
    if status == TransferStatus::DownloadFailed {
        fail(
            &registry,
            &id,
            CrossAccountStage::Revoking,
            format!(
                "文件已转存到下载账号，但下载失败：{}。详情见「转存管理」「下载管理」",
                transfer_warning.as_deref().unwrap_or("未知原因")
            ),
        );
        return;
    }

    if let Some(ref msg) = transfer_warning {
        registry.warn(&id, format!("部分文件未转存成功：{}", msg));
    }

    // 分享直下转存后停在「已转存」= 一个下载任务都没建出来（本地已有同名文件被跳过，
    // 或创建失败），临时目录由分享直下按配置清理
    if job.mode == CrossAccountMode::Download && status == TransferStatus::Transferred {
        registry.warn(
            &id,
            "没有创建任何下载任务：所选文件可能已存在于本地下载目录，或下载任务创建失败"
                .to_string(),
        );
    }

    // 4. 移动：转存完全成功 + 目标逐项核对一致，才删源文件。
    //    核对不依赖转存任务的计数 / error 字段：异步转存的响应可能不带 list（成功时
    //    transferred_count 也为 0），计数不可靠；直接比对目标网盘里的实际结果。
    if job.mode == CrossAccountMode::Move {
        if transfer_warning.is_some() {
            registry.warn(
                &id,
                "存在未转存成功的文件，已保留全部源文件，请核对后手动删除".to_string(),
            );
        } else {
            registry.set_stage(&id, CrossAccountStage::Verifying);
            match verify_target(&state, source, target, &items, &job.target_dir).await {
                Err(msg) => registry.warn(
                    &id,
                    format!(
                        "目标文件校验未通过：{}。已保留全部源文件，请核对后手动删除",
                        msg
                    ),
                ),
                Ok(()) => {
                    registry.set_stage(&id, CrossAccountStage::DeletingSource);
                    match delete_source(&state, source, &job.paths).await {
                        Ok(()) => registry.update(&id, |j| j.source_deleted = true),
                        Err(msg) => registry.warn(
                            &id,
                            format!(
                                "文件已复制到目标账号，但删除源文件失败：{}。请手动删除源文件",
                                msg
                            ),
                        ),
                    }
                }
            }
        }
    }

    registry.set_stage(&id, CrossAccountStage::Completed);
    info!("跨账号任务完成: job={}", id);
}

/// 路径的父目录（`/a` → `/`，`/a/b` → `/a`）
pub(crate) fn parent_dir(path: &str) -> &str {
    match path.rfind('/') {
        Some(0) | None => "/",
        Some(idx) => &path[..idx],
    }
}

fn base_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// 选中的源文件 / 文件夹
#[derive(Debug, Clone)]
struct SourceItem {
    path: String,
    name: String,
    is_dir: bool,
    size: u64,
}

async fn client_of(state: &AppState, uid: Uid) -> Result<Arc<NetdiskClient>, String> {
    state
        .client_pool
        .read()
        .await
        .get_client(uid)
        .ok_or_else(|| format!("账号 uid={} 客户端未初始化，请重新登录该账号", uid.raw()))
}

/// 列出目录全部条目；遇 errno=-6 预热该账号后重试一次。`Ok(None)` 表示目录不存在
async fn list_dir(state: &AppState, uid: Uid, dir: &str) -> Result<Option<Vec<FileItem>>, String> {
    let mut warmed_up = false;
    loop {
        let client = client_of(state, uid).await?;
        match list_all_dir_entries(&client, dir).await {
            Err(msg) if msg.contains("API error -6") && !warmed_up => {
                warmed_up = true;
                if let Err(e) = state.warmup_uid(uid).await {
                    warn!("预热账号失败: uid={}, {}", uid.raw(), e);
                }
            }
            other => return other,
        }
    }
}

/// 选中项里与目标目录现有条目同名的名字（百度文件名区分大小写）
fn conflicting_names<'a>(items: &'a [SourceItem], existing: &HashSet<&str>) -> Vec<&'a str> {
    items
        .iter()
        .map(|i| i.name.as_str())
        .filter(|n| existing.contains(n))
        .collect()
}

/// 建分享前的检查：源文件都在；复制 / 移动还要求目标目录存在且无同名项。
/// 返回源文件元信息供移动前核对
///
/// `paths` 由 handler 保证同在一个父目录下。
async fn precheck(state: &AppState, job: &CrossAccountJob) -> Result<Vec<SourceItem>, String> {
    let items = load_source_items(state, Uid::new(job.source_uid), &job.paths).await?;
    if job.mode != CrossAccountMode::Download {
        check_target_conflicts(state, Uid::new(job.target_uid), &items, &job.target_dir).await?;
    }
    Ok(items)
}

async fn load_source_items(
    state: &AppState,
    source: Uid,
    paths: &[String],
) -> Result<Vec<SourceItem>, String> {
    let parent = parent_dir(&paths[0]);
    let entries = list_dir(state, source, parent)
        .await
        .map_err(|e| format!("读取源目录失败：{}", e))?
        .ok_or_else(|| format!("源目录不存在：{}", parent))?;
    let by_path: HashMap<&str, &FileItem> = entries.iter().map(|f| (f.path.as_str(), f)).collect();
    let mut items = Vec::with_capacity(paths.len());
    for p in paths {
        let f = by_path
            .get(p.as_str())
            .ok_or_else(|| format!("源文件不存在：{}", p))?;
        items.push(SourceItem {
            path: p.clone(),
            name: base_name(p).to_string(),
            is_dir: f.isdir == 1,
            size: f.size,
        });
    }
    Ok(items)
}

async fn check_target_conflicts(
    state: &AppState,
    target: Uid,
    items: &[SourceItem],
    target_dir: &str,
) -> Result<(), String> {
    let existing = list_dir(state, target, target_dir)
        .await
        .map_err(|e| format!("读取目标目录失败：{}", e))?
        .ok_or_else(|| format!("目标目录不存在：{}", target_dir))?;
    let names: HashSet<&str> = existing
        .iter()
        .map(|f| f.server_filename.as_str())
        .collect();
    let conflicts = conflicting_names(items, &names);
    if !conflicts.is_empty() {
        return Err(format!(
            "目标目录已存在同名项：{}。百度转存不会自动改名，请先重命名或换一个目标目录",
            conflicts.join("、")
        ));
    }
    Ok(())
}

/// 目录树汇总
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TreeSummary {
    files: u64,
    bytes: u64,
}

/// 递归统计目录下的文件数与总字节；`budget` 为剩余可遍历目录数
async fn summarize_dir(
    client: &NetdiskClient,
    root: &str,
    budget: &mut usize,
) -> Result<TreeSummary, String> {
    let mut sum = TreeSummary::default();
    let mut queue = VecDeque::from([root.to_string()]);
    while let Some(dir) = queue.pop_front() {
        if *budget == 0 {
            return Err(format!(
                "目录过多（超过 {} 个），无法完成核对",
                VERIFY_MAX_DIRS
            ));
        }
        *budget -= 1;
        let entries = list_all_dir_entries(client, &dir)
            .await?
            .ok_or_else(|| format!("目录不存在：{}", dir))?;
        for e in entries {
            if e.isdir == 1 {
                queue.push_back(e.path);
            } else {
                sum.files += 1;
                sum.bytes += e.size;
            }
        }
    }
    Ok(sum)
}

/// 移动前逐项核对：目标目录下每个选中项都存在、类型一致，且文件数与总字节和源一致
async fn verify_target(
    state: &AppState,
    source: Uid,
    target: Uid,
    items: &[SourceItem],
    target_dir: &str,
) -> Result<(), String> {
    let src = client_of(state, source).await?;
    let dst = client_of(state, target).await?;
    let existing = list_dir(state, target, target_dir)
        .await?
        .ok_or_else(|| format!("目标目录不存在：{}", target_dir))?;
    let by_name: HashMap<&str, &FileItem> = existing
        .iter()
        .map(|f| (f.server_filename.as_str(), f))
        .collect();

    let (mut src_budget, mut dst_budget) = (VERIFY_MAX_DIRS, VERIFY_MAX_DIRS);
    for item in items {
        let t = by_name
            .get(item.name.as_str())
            .ok_or_else(|| format!("目标目录中未找到「{}」", item.name))?;
        if (t.isdir == 1) != item.is_dir {
            return Err(format!("目标中的「{}」类型与源不一致", item.name));
        }
        let (s, d) = if item.is_dir {
            (
                summarize_dir(&src, &item.path, &mut src_budget).await?,
                summarize_dir(&dst, &t.path, &mut dst_budget).await?,
            )
        } else {
            (
                TreeSummary {
                    files: 1,
                    bytes: item.size,
                },
                TreeSummary {
                    files: 1,
                    bytes: t.size,
                },
            )
        };
        if s != d {
            return Err(format!(
                "「{}」不一致（源 {} 个文件 / {} 字节，目标 {} 个文件 / {} 字节）",
                item.name, s.files, s.bytes, d.files, d.bytes
            ));
        }
    }
    Ok(())
}

fn fail(registry: &CrossAccountRegistry, id: &str, at: CrossAccountStage, msg: String) {
    error!("跨账号任务失败: job={}, stage={:?}, error={}", id, at, msg);
    registry.update(id, |j| {
        j.stage = CrossAccountStage::Failed;
        j.failed_stage = Some(at);
        j.error = Some(msg);
    });
}

async fn create_temp_share(
    state: &AppState,
    uid: Uid,
    paths: &[String],
    pwd: &str,
) -> Result<ShareSetResponse, String> {
    let mut warmed_up = false;
    loop {
        let client = state
            .client_pool
            .read()
            .await
            .get_client(uid)
            .ok_or_else(|| "源账号客户端未初始化，请重新登录该账号".to_string())?;
        let resp = client
            .share_set(paths, TEMP_SHARE_PERIOD_DAYS, pwd)
            .await
            .map_err(|e| format!("创建临时分享失败: {:#}", e))?;
        if resp.is_success() {
            return Ok(resp);
        }
        if resp.errno == -6 && !warmed_up {
            warn!("创建临时分享遇到 errno=-6，预热源账号后重试");
            warmed_up = true;
            if let Err(e) = state.warmup_uid(uid).await {
                warn!("预热源账号失败: {}", e);
            }
            continue;
        }
        return Err(share_set_error_message(&resp));
    }
}

fn share_set_error_message(resp: &ShareSetResponse) -> String {
    let reason = match resp.errno {
        // 通用文案说的是「请重新登录」，跨账号场景要指明是源账号
        -6 => "源账号登录已过期或凭证不完整，请重新登录源账号".to_string(),
        _ => resp.error_reason(),
    };
    format!("源账号创建临时分享失败：{}", reason)
}

/// 转存阶段结果：`Ok((结束时的状态, 部分失败说明))`，`Err` 为整体失败
type TransferOutcome = Result<(TransferStatus, Option<String>), String>;

fn build_transfer_request(
    job: &CrossAccountJob,
    share_url: &str,
    pwd: &str,
) -> CreateTransferRequest {
    let base = CreateTransferRequest {
        share_url: share_url.to_string(),
        password: Some(pwd.to_string()),
        randsk: None,
        prefetched_share: None,
        save_path: job.target_dir.clone(),
        save_fs_id: 0,
        auto_download: Some(false),
        local_download_path: None,
        download_conflict_strategy: None,
        is_share_direct_download: false,
        selected_fs_ids: None,
        selected_files: None,
        owner_uid_override: Some(Uid::new(job.target_uid)),
        // 内部任务：分享撤销后无法重试，不进「转存管理」
        is_internal: true,
        backup_config_id: Some(format!("{}{}", INTERNAL_CONFIG_PREFIX, job.id)),
        // 目标目录属于目标账号，不能写成活跃账号的「最近保存目录」
        remember_recent_dir: false,
    };
    match job.mode {
        CrossAccountMode::Copy | CrossAccountMode::Move => base,
        // 普通的分享直下任务：在「转存管理」「下载管理」可见，下载进度、重试、临时目录
        // 清理全部沿用分享直下。不能带 backup_config_id，否则衍生下载会走
        // create_backup_task，被当成分享同步的隐藏下载
        CrossAccountMode::Download => CreateTransferRequest {
            save_path: String::new(),
            auto_download: Some(true),
            local_download_path: job.local_download_path.clone(),
            is_share_direct_download: true,
            is_internal: false,
            backup_config_id: None,
            ..base
        },
    }
}

/// 转存任务走完转存阶段后返回结果，否则 `None`。转存阶段之后的步骤（下载、清理
/// 临时目录）只读写目标账号自己的网盘，不再访问分享，此时即可撤销分享
///
/// `DownloadFailed` 只会在下载开始之后出现，说明转存本身已成功，归入 `Ok`，
/// 由调用方按「下载失败」处理（此时 error 是下载错误，不是转存的部分失败说明）
fn transfer_phase_outcome(
    status: &TransferStatus,
    error: Option<String>,
) -> Option<TransferOutcome> {
    match status {
        TransferStatus::Queued | TransferStatus::CheckingShare | TransferStatus::Transferring => {
            None
        }
        TransferStatus::TransferFailed => {
            Some(Err(error.unwrap_or_else(|| "转存失败".to_string())))
        }
        TransferStatus::Transferred
        | TransferStatus::Downloading
        | TransferStatus::DownloadFailed
        | TransferStatus::Cleaning
        | TransferStatus::Completed => Some(Ok((status.clone(), error))),
    }
}

async fn transfer_into_target(
    state: &AppState,
    job: &CrossAccountJob,
    share_url: &str,
    pwd: &str,
) -> TransferOutcome {
    let Some(tm) = state.transfer_manager_for(Uid::new(job.target_uid)) else {
        return Err("目标账号转存管理器未初始化，请重新登录目标账号".into());
    };
    let task_id = match tm
        .create_task(build_transfer_request(job, share_url, pwd))
        .await
    {
        Ok(resp) => match (resp.task_id, resp.error) {
            (_, Some(err)) => return Err(format!("转存失败：{}", err)),
            (Some(id), None) => id,
            (None, None) => return Err("转存失败：未返回任务 ID".into()),
        },
        Err(e) => return Err(format!("转存失败：{:#}", e)),
    };

    let registry = &state.cross_account_jobs;
    registry.update(&job.id, |j| j.transfer_task_id = Some(task_id.clone()));
    let started = tokio::time::Instant::now();
    loop {
        let Some(task) = tm.get_task(&task_id).await else {
            return Err("转存任务丢失（可能已被删除）".into());
        };
        registry.update(&job.id, |j| {
            j.transferred_count = task.transferred_count;
            j.total_count = task.total_count;
        });
        if let Some(outcome) = transfer_phase_outcome(&task.status, task.error) {
            return outcome;
        }
        if started.elapsed() > TRANSFER_HARD_TIMEOUT {
            if let Err(e) = tm.cancel_task(&task_id).await {
                warn!("取消超时的跨账号转存任务失败: {}", e);
            }
            return Err("转存超时".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn revoke_share(state: &AppState, uid: Uid, shareid: u64) -> Result<(), String> {
    let mut last_err = String::new();
    for attempt in 0..3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let Some(client) = state.client_pool.read().await.get_client(uid) else {
            return Err("源账号客户端未初始化".into());
        };
        match client.share_cancel(&[shareid]).await {
            Ok(resp) if resp.is_success() => return Ok(()),
            Ok(resp) => {
                last_err = format!("errno={} {}", resp.errno, resp.errmsg);
                if resp.errno == -6 {
                    if let Err(e) = state.warmup_uid(uid).await {
                        warn!("预热源账号失败: {}", e);
                    }
                }
            }
            Err(e) => last_err = format!("{:#}", e),
        }
    }
    Err(last_err)
}

async fn delete_source(state: &AppState, uid: Uid, paths: &[String]) -> Result<(), String> {
    let mut warmed_up = false;
    loop {
        let Some(client) = state.client_pool.read().await.get_client(uid) else {
            return Err("源账号客户端未初始化".into());
        };
        let resp = client
            .delete_files_chunked(paths)
            .await
            .map_err(|e| format!("{:#}", e))?;
        if resp.success {
            return Ok(());
        }
        if resp.errno == Some(-6) && !warmed_up {
            warmed_up = true;
            if let Err(e) = state.warmup_uid(uid).await {
                warn!("预热源账号失败: {}", e);
            }
            continue;
        }
        return Err(match resp.errno {
            Some(132) => "触发百度安全验证（errno=132），请到网页版完成验证".to_string(),
            _ => resp.error.unwrap_or_else(|| "删除失败".to_string()),
        });
    }
}

/// 启动时撤销上次运行遗留的临时分享（进程中途退出 / 撤销失败）
pub async fn revoke_leftover_shares(state: &AppState) {
    let registry = state.cross_account_jobs.clone();
    let entries = {
        let _guard = registry.ledger_lock.lock().await;
        registry.ledger_load().await
    };
    if entries.is_empty() {
        return;
    }
    info!("撤销上次遗留的跨账号临时分享: {} 个", entries.len());
    let expire_before = chrono::Utc::now().timestamp() - i64::from(TEMP_SHARE_PERIOD_DAYS) * 86400;
    for entry in entries {
        // 本进程刚发起、仍在进行的任务：分享由它自己撤销
        if registry.jobs.contains_key(&entry.job_id) {
            continue;
        }
        let uid = Uid::new(entry.source_uid);
        let done = if entry.created_at < expire_before {
            // 已自然过期
            true
        } else if state.client_pool.read().await.get_client(uid).is_none() {
            // 账号已删除，无从撤销
            warn!(
                "遗留临时分享的源账号已不存在，跳过: uid={}, shareid={}",
                entry.source_uid, entry.shareid
            );
            true
        } else {
            match revoke_share(state, uid, entry.shareid).await {
                Ok(()) => true,
                Err(e) => {
                    warn!(
                        "撤销遗留临时分享失败，下次启动重试: shareid={}, error={}",
                        entry.shareid, e
                    );
                    false
                }
            }
        };
        if done {
            registry.ledger_remove(entry.shareid).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_registry() -> (tempfile::TempDir, CrossAccountRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let reg =
            CrossAccountRegistry::with_ledger_path(dir.path().join("cfg").join("ledger.json"));
        (dir, reg)
    }

    fn pending(shareid: u64) -> PendingShare {
        PendingShare {
            job_id: format!("job-{}", shareid),
            source_uid: 1,
            shareid,
            created_at: 0,
        }
    }

    #[tokio::test]
    async fn ledger_add_remove_roundtrip() {
        let (_dir, reg) = temp_registry();
        assert!(reg.ledger_load().await.is_empty());
        reg.ledger_add(pending(11)).await;
        reg.ledger_add(pending(22)).await;
        let ids: Vec<u64> = reg.ledger_load().await.iter().map(|e| e.shareid).collect();
        assert_eq!(ids, vec![11, 22]);
        reg.ledger_remove(11).await;
        let ids: Vec<u64> = reg.ledger_load().await.iter().map(|e| e.shareid).collect();
        assert_eq!(ids, vec![22]);
    }

    #[test]
    fn insert_prunes_only_stale_finished_jobs() {
        let (_dir, reg) = temp_registry();
        let mk = |stage, age: i64| {
            let mut j = CrossAccountJob::new(
                CrossAccountMode::Copy,
                Uid::new(1),
                Uid::new(2),
                vec!["/a".into()],
                "/".into(),
            );
            j.stage = stage;
            j.updated_at -= age;
            j
        };
        let stale_done = mk(CrossAccountStage::Completed, FINISHED_JOB_TTL_SECS + 10);
        let fresh_done = mk(CrossAccountStage::Failed, 10);
        let old_running = mk(CrossAccountStage::Transferring, FINISHED_JOB_TTL_SECS + 10);
        let (a, b, c) = (
            stale_done.id.clone(),
            fresh_done.id.clone(),
            old_running.id.clone(),
        );
        reg.insert(stale_done);
        reg.insert(fresh_done);
        reg.insert(old_running);
        reg.insert(mk(CrossAccountStage::Sharing, 0));
        assert!(reg.get(&a).is_none());
        assert!(reg.get(&b).is_some());
        assert!(reg.get(&c).is_some());
    }

    #[test]
    fn path_helpers() {
        assert_eq!(parent_dir("/a"), "/");
        assert_eq!(parent_dir("/a/b/c.txt"), "/a/b");
        assert_eq!(base_name("/a/b/c.txt"), "c.txt");
        assert_eq!(base_name("/a"), "a");
    }

    #[test]
    fn conflicting_names_matches_exact_names_only() {
        let item = |name: &str| SourceItem {
            path: format!("/src/{}", name),
            name: name.to_string(),
            is_dir: false,
            size: 1,
        };
        let items = vec![item("a.txt"), item("dir"), item("b.txt")];
        let existing: HashSet<&str> = ["a.txt", "dir(1)", "B.txt"].into_iter().collect();
        assert_eq!(conflicting_names(&items, &existing), vec!["a.txt"]);
    }

    #[test]
    fn share_set_error_messages() {
        let resp = |errno: i32, errmsg: &str, show_msg: &str| ShareSetResponse {
            errno,
            link: String::new(),
            pwd: String::new(),
            shareid: 0,
            errmsg: errmsg.to_string(),
            show_msg: show_msg.to_string(),
        };
        assert!(share_set_error_message(&resp(-6, "", "")).contains("重新登录源账号"));
        assert!(share_set_error_message(&resp(115, "", "")).contains("禁止分享"));
        assert!(share_set_error_message(&resp(110, "", "")).contains("上限"));
        assert!(share_set_error_message(&resp(42, "boom", "")).contains("boom（errno=42）"));
        assert!(share_set_error_message(&resp(42, "", "")).contains("errno=42"));
        assert!(share_set_error_message(&resp(42, "", "请先绑定手机号")).contains("请先绑定手机号"));
    }

    #[test]
    fn job_serializes_snake_case() {
        let j = CrossAccountJob::new(
            CrossAccountMode::Move,
            Uid::new(1),
            Uid::new(2),
            vec!["/a".into()],
            "/b".into(),
        );
        let v = serde_json::to_value(&j).unwrap();
        assert_eq!(v["mode"], "move");
        assert_eq!(v["stage"], "checking");
        assert_eq!(v["source_deleted"], false);
        assert!(v["local_download_path"].is_null());

        let d = CrossAccountJob::new_download(
            Uid::new(1),
            Uid::new(2),
            vec!["/a".into()],
            "D:/dl".into(),
        );
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["mode"], "download");
        assert_eq!(v["target_dir"], "");
        assert_eq!(v["local_download_path"], "D:/dl");
    }

    #[test]
    fn transfer_phase_outcome_waits_until_transfer_done() {
        use TransferStatus::*;
        for s in [Queued, CheckingShare, Transferring] {
            assert!(transfer_phase_outcome(&s, None).is_none(), "{:?}", s);
        }
        for s in [
            Transferred,
            Downloading,
            DownloadFailed,
            Cleaning,
            Completed,
        ] {
            assert_eq!(
                transfer_phase_outcome(&s, None),
                Some(Ok((s.clone(), None)))
            );
        }
        assert_eq!(
            transfer_phase_outcome(&Downloading, Some("2 个失败".into())),
            Some(Ok((Downloading, Some("2 个失败".into()))))
        );
        assert_eq!(
            transfer_phase_outcome(&TransferFailed, Some("空间不足".into())),
            Some(Err("空间不足".into()))
        );
        assert_eq!(
            transfer_phase_outcome(&TransferFailed, None),
            Some(Err("转存失败".into()))
        );
    }

    #[test]
    fn transfer_request_per_mode() {
        let copy = CrossAccountJob::new(
            CrossAccountMode::Copy,
            Uid::new(1),
            Uid::new(2),
            vec!["/a".into()],
            "/dst".into(),
        );
        let req = build_transfer_request(&copy, "https://pan.baidu.com/s/1x", "abcd");
        assert!(req.is_internal);
        assert!(!req.is_share_direct_download);
        assert_eq!(req.auto_download, Some(false));
        assert_eq!(req.save_path, "/dst");
        assert_eq!(
            req.backup_config_id,
            Some(format!("{}{}", INTERNAL_CONFIG_PREFIX, copy.id))
        );
        assert_eq!(req.owner_uid_override, Some(Uid::new(2)));
        assert_eq!(req.password.as_deref(), Some("abcd"));

        let dl = CrossAccountJob::new_download(
            Uid::new(1),
            Uid::new(2),
            vec!["/a".into()],
            "D:/dl".into(),
        );
        let req = build_transfer_request(&dl, "https://pan.baidu.com/s/1x", "abcd");
        assert!(!req.is_internal);
        assert!(req.is_share_direct_download);
        assert_eq!(req.auto_download, Some(true));
        assert_eq!(req.local_download_path.as_deref(), Some("D:/dl"));
        // 带 backup_config_id 会让衍生下载变成分享同步的隐藏下载
        assert!(req.backup_config_id.is_none());
        assert_eq!(req.owner_uid_override, Some(Uid::new(2)));
    }
}
