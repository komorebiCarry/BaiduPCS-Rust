//! 文件夹下载数据结构

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use uuid::Uuid;

/// 文件夹下载状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FolderStatus {
    /// 正在扫描文件夹
    Scanning,
    /// 扫描完成，正在下载
    Downloading,
    /// 已暂停
    Paused,
    /// 全部完成
    Completed,
    /// 失败
    Failed,
    /// 已取消
    Cancelled,
}

/// 待下载的文件信息（扫描结果）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingFile {
    pub fs_id: u64,
    pub filename: String,
    pub remote_path: String,
    pub relative_path: String,
    pub size: u64,
}

/// 因冲突策略被跳过的文件（不会创建子任务）
///
/// 跳过的文件既不产生子任务、也不产生下载字节，若不单独记下来，下载详情里就完全看不到
/// 它们——用户看到的是「已完成 + 空列表」，无法确认到底跳过了哪些文件（issue #141 用户
/// 反馈"希望详情里能有跳过状态"）。这里持久化文件级明细，供详情列表以「跳过」状态展示。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedFile {
    /// 相对文件夹根的路径
    pub relative_path: String,
    /// 文件大小
    pub size: u64,
    /// 跳过时间（秒级时间戳）
    pub skipped_at: i64,
}

/// 文件夹下载任务组
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderDownload {
    #[serde(skip)]
    pub file_store: Option<std::sync::Arc<crate::persistence::folder_files::FolderFileStore>>,
    #[serde(skip)]
    pub sqlite_files: bool,
    /// 文件夹ID
    pub id: String,
    /// 文件夹名称
    pub name: String,
    /// 网盘根路径
    pub remote_root: String,
    /// 本地根路径
    pub local_root: PathBuf,
    /// 状态
    pub status: FolderStatus,
    /// 总文件数
    pub total_files: u64,
    /// 总大小
    pub total_size: u64,
    /// 已创建任务数
    pub created_count: u64,
    /// 已完成任务数
    pub completed_count: u64,
    /// 已下载大小
    pub downloaded_size: u64,
    /// 待下载的文件队列（扫描发现但还未创建下载任务）
    /// 跳过序列化，避免 API 返回大量数据
    #[serde(default, skip_serializing)]
    pub pending_files: Vec<PendingFile>,
    /// 扫描是否完成
    #[serde(default)]
    pub scan_completed: bool,
    /// 扫描进度（当前扫描到的目录）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_progress: Option<String>,
    /// 创建时间
    pub created_at: i64,
    /// 开始时间
    pub started_at: Option<i64>,
    /// 完成时间
    pub completed_at: Option<i64>,
    /// 错误信息
    pub error: Option<String>,
    /// 🔥 关联的转存任务 ID（如果此文件夹下载任务由转存任务自动创建）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_task_id: Option<String>,

    /// 🔥 归属的备份配置 ID（`Some` 时为内部隐藏文件夹下载）
    ///
    /// 与单文件下载的 `DownloadTask::backup_config_id` 同义：分享同步在 tree
    /// 模式下整目录转存 + 自动下载会产生文件夹下载任务，需要据此从「下载管理」
    /// 隐藏并归属到 `share-sync:{订阅id}`，否则会泄漏进下载管理且无法作为分享
    /// 同步子任务被收集。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_config_id: Option<String>,

    /// 🔥 多账号归属 UID
    ///
    /// 旧持久化数据反序列化时为默认值 `Uid(0)`，恢复链路按
    /// 分支 C/D 填充为 `active_uid` 或进入失败终态。
    #[serde(default)]
    pub owner_uid: crate::auth::types::Uid,

    /// 🔥 不可恢复失败原因（恢复链路）
    ///
    /// 当文件夹任务进入 `FolderStatus::Failed` 时承载结构性原因：
    /// - `"account_deleted"`：账号已删除
    /// - `"unrecoverable_no_active_account"`：无 active_uid 兜底
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,

    // === 🔥 新增：任务位借调机制相关字段 ===
    /// 固定任务位ID（文件夹主任务位）
    #[serde(skip)]
    pub fixed_slot_id: Option<usize>,

    /// 借调任务位ID列表（用于子任务并行）
    #[serde(skip)]
    pub borrowed_slot_ids: Vec<usize>,

    /// 正在使用借调位的子任务ID映射（task_id -> slot_id）
    #[serde(skip)]
    pub borrowed_subtask_map: HashMap<String, usize>,

    /// 🔥 正在使用文件夹固定槽位的子任务 ID（运行时字段，重启不保留）
    ///
    /// 同一时刻最多只有 1 个子任务占用固定槽位；其他子任务使用借调槽位。
    /// None = 固定槽位空闲；Some(id) = 被 id 任务占用中
    #[serde(skip)]
    pub fixed_slot_subtask: Option<String>,

    /// 🔥 加密文件夹映射（加密相对路径 -> 解密后相对路径）
    /// 用于在扫描完成后重命名文件夹并更新路径
    #[serde(default, skip)]
    pub encrypted_folder_mappings: HashMap<String, String>,

    /// 🔥 已计数的任务ID集合（用于避免重复计数 completed_count）
    /// 解决问题：使用固定位的子任务完成时也需要递增 completed_count
    ///
    /// 注意：这个集合只用于「把已完成任务从 active_sum 里剔掉」（见
    /// `active_downloaded_excluding_counted`），**不再充当完成计数的去重闸门** ——
    /// 去重键已换成 `counted_fs_ids`，原因见该字段。
    #[serde(default, skip)]
    pub counted_task_ids: HashSet<String>,

    /// 🔥 已计入完成数的文件 fs_id 集合（issue #156）
    ///
    /// 完成计数原本按 `task_id` 去重，而 task_id 是每次建任务新生成的 UUID：同一个
    /// 文件只要被重新建成子任务（重启后对账失效、跳过分支没摘队列、失败重入队等），
    /// 就会**再 +1 并把 file_size 再累加进 `completed_downloaded_size`**。实测
    /// 1041 个文件数成 5337、18.4GB 数成 94.5GB，进度冲过 100% 后文件夹永远到不了
    /// 终态，于是无限补任务、把同一批文件反复重下（用户实测烧掉约 100GB 流量）。
    ///
    /// `fs_id` 是百度网盘侧稳定的文件身份，跨任务、跨重启都不变，用它做去重键才能
    /// 真正把重复计数堵死在源头 —— 这也意味着**不需要枚举出所有"重复建任务"的路径**
    /// 就能止血。因此本字段必须持久化（见 `FolderPersisted.counted_fs_ids`）。
    #[serde(default, skip)]
    pub counted_fs_ids: HashSet<u64>,

    /// 🔥 下载冲突策略（用于子任务）
    ///
    /// 注意：这里的 `skip` 只是不参与 `FolderDownload` 自身的序列化（该结构体用于
    /// 接口返回），**不代表本字段无需持久化**。跨重启的持久化走
    /// `FolderPersisted.conflict_strategy`，原因见该字段注释（issue #141）。
    #[serde(default, skip)]
    pub conflict_strategy: Option<crate::uploader::conflict::DownloadConflictStrategy>,

    /// 🔥 已成功完成的子任务累计字节数（运行时字段，单调递增）
    /// 每当一个子任务成功完成时 += file_size，失败的任务不计入
    /// downloaded_size = max(downloaded_size, completed_downloaded_size + active_sum)
    #[serde(default, skip)]
    pub completed_downloaded_size: u64,

    /// 🔥 因冲突策略命中"跳过"而未创建任务的文件数
    ///
    /// 与 `completed_count` 分开统计：两者都算"已处理完"（共同参与终态判定），
    /// 但语义不同——前者是真下载完，后者是本地已存在被跳过。前端据此展示
    /// "已跳过 N 个"，让用户能确认队列在工作而不是卡住（issue #141 用户反馈）。
    #[serde(default)]
    pub skipped_count: u64,

    /// 🔥 因冲突策略跳过的文件字节数
    ///
    /// 跳过的文件不产生子任务、也不产生下载字节，光看 `downloaded_size` 的话，
    /// 一个"全部跳过"的文件夹会显示 0% 却又是「已完成」状态，用户无从判断发生了什么。
    /// 前端用 `(downloaded_size + skipped_size) / total_size` 作为整体处理进度。
    #[serde(default)]
    pub skipped_size: u64,

    /// 🔥 被跳过文件的明细清单
    ///
    /// 与 `pending_files` 同样 `skip_serializing`：列表接口不需要它，避免大文件夹的响应
    /// 膨胀；详情通过 `GET /downloads/folder/:id/skipped` 单独取。
    #[serde(default, skip_serializing)]
    pub skipped_entries: Vec<SkippedFile>,

    /// 🔥 失败的子任务数（运行时字段）
    /// 用于判断文件夹是否应进入终态：completed_count + failed_count >= total_files
    #[serde(default, skip)]
    pub failed_count: u64,

    /// 🔥 子任务自动重试次数（task_id → 已重试次数，运行时字段）
    ///
    /// 子任务耗尽分片级/链接级重试后仍失败时不直接判死：把它重新排到等待队列尾部再试，
    /// 最多 `MAX_SUBTASK_AUTO_RETRIES` 次，耗尽才计入 `failed_count`。
    /// 与 `failed_task_ids` 一样不持久化——重启后重新给一份额度。
    #[serde(default, skip)]
    pub subtask_retry_counts: HashMap<String, u32>,

    /// 🔥 已计入 failed_count 的文件 fs_id 集合（运行时字段）
    ///
    /// 与 `counted_fs_ids` 同因同治（issue #156）：`failed_task_ids` 按 task_id 记账，
    /// 而同一个文件被重新建成子任务时 task_id 会变。于是「任务 A 耗尽重试被判死 →
    /// 同一文件的任务 B 下载成功」时，成功侧按 task_id 抵消不掉 A 的那笔失败，
    /// 文件明明都在盘上，文件夹却以"N 个文件下载失败"收场。用 fs_id 记一份才能抵消。
    ///
    /// 与 `failed_task_ids` 一样不持久化 —— 重启后重新给一份重试额度。
    #[serde(default, skip)]
    pub failed_fs_ids: HashSet<u64>,

    /// 🔥 已失败的任务ID集合（运行时字段）
    /// 避免同一任务多次失败时重复计数；重试成功时从此集合移除并减少 failed_count
    #[serde(default, skip)]
    pub failed_task_ids: HashSet<String>,
}

impl FolderDownload {
    pub fn pending_len(&self) -> usize {
        self.file_store.as_ref().map_or(self.pending_files.len(), |s| s.pending_count())
    }
    pub fn has_pending(&self) -> bool { (self.sqlite_files && self.file_store.is_none()) || self.pending_len() != 0 }
    pub fn load_pending_page(&mut self, limit: usize) -> anyhow::Result<()> {
        anyhow::ensure!(!self.sqlite_files || self.file_store.is_some(), "文件级数据库尚未恢复，拒绝派发");
        if let Some(store) = &self.file_store { self.pending_files = store.page(limit)?; }
        Ok(())
    }
    pub fn is_completed_file(&self, fs_id:u64, task_id:&str) -> anyhow::Result<bool> {
        match &self.file_store {
            Some(s)=>s.is_completed(fs_id,task_id),
            None=>Ok(if fs_id == 0 { self.counted_task_ids.contains(task_id) } else { self.counted_fs_ids.contains(&fs_id) }),
        }
    }
    pub fn mark_skipped(&mut self, file:&PendingFile) -> anyhow::Result<bool> {
        let inserted = if let Some(s)=&self.file_store { s.skip(file)? } else { true };
        self.pending_files.retain(|p| p.relative_path != file.relative_path);
        Ok(inserted)
    }
    pub fn append_skipped(&mut self, entries:&mut Vec<SkippedFile>) {
        if self.file_store.is_some() { entries.clear(); } else { self.skipped_entries.append(entries); }
    }
    pub fn sync_file_totals(&mut self) -> anyhow::Result<()> {
        if let Some(store)=&self.file_store {
            let (done,bytes,skips,skip_bytes)=store.totals()?;
            self.completed_count=self.completed_count.max(done);
            self.completed_downloaded_size=self.completed_downloaded_size.max(bytes);
            self.skipped_count=self.skipped_count.max(skips);
            self.skipped_size=self.skipped_size.max(skip_bytes);
            self.failed_count=store.failure_count()?;
        }
        Ok(())
    }
    /// API 投影：只复制可序列化字段，不复制扫描待办和运行时账本。
    pub fn api_snapshot(&self) -> Self {
        let mut snapshot = Self::new(self.remote_root.clone(), self.local_root.clone());
        snapshot.id = self.id.clone();
        snapshot.name = self.name.clone();
        snapshot.status = self.status.clone();
        snapshot.total_files = self.total_files;
        snapshot.total_size = self.total_size;
        snapshot.created_count = self.created_count;
        snapshot.completed_count = self.completed_count;
        snapshot.downloaded_size = self.downloaded_size;
        snapshot.scan_completed = self.scan_completed;
        snapshot.scan_progress = self.scan_progress.clone();
        snapshot.created_at = self.created_at;
        snapshot.started_at = self.started_at;
        snapshot.completed_at = self.completed_at;
        snapshot.error = self.error.clone();
        snapshot.transfer_task_id = self.transfer_task_id.clone();
        snapshot.backup_config_id = self.backup_config_id.clone();
        snapshot.owner_uid = self.owner_uid;
        snapshot.failure_reason = self.failure_reason.clone();
        snapshot.skipped_count = self.skipped_count;
        snapshot.skipped_size = self.skipped_size;
        snapshot.completed_downloaded_size = self.completed_downloaded_size;
        snapshot.file_store = self.file_store.clone();
        snapshot.sqlite_files = self.sqlite_files;
        snapshot
    }

    /// 创建新的文件夹下载
    ///
    /// 🔥 修复：从 local_root 提取文件夹名称（解密后的原始名称）
    /// 而不是从 remote_root（可能是加密的 BPR_DIR_xxx 格式）
    pub fn new(remote_root: String, local_root: PathBuf) -> Self {
        // 优先从 local_root 提取名称（已解密的原始名称）
        let name = local_root
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        Self {
            file_store: None,
            sqlite_files: false,
            id: Uuid::new_v4().to_string(),
            name,
            remote_root,
            local_root,
            status: FolderStatus::Scanning,
            total_files: 0,
            total_size: 0,
            created_count: 0,
            completed_count: 0,
            downloaded_size: 0,
            pending_files: Vec::new(),
            scan_completed: false,
            scan_progress: None,
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            error: None,
            transfer_task_id: None,
            backup_config_id: None,
            owner_uid: crate::auth::types::Uid::default(),
            failure_reason: None,
            // 任务位借调机制字段初始化
            fixed_slot_id: None,
            borrowed_slot_ids: Vec::new(),
            borrowed_subtask_map: HashMap::new(),
            fixed_slot_subtask: None,
            encrypted_folder_mappings: HashMap::new(),
            counted_task_ids: HashSet::new(),
            counted_fs_ids: HashSet::new(),
            conflict_strategy: None,
            skipped_count: 0,
            skipped_size: 0,
            skipped_entries: Vec::new(),
            completed_downloaded_size: 0,
            failed_count: 0,
            failed_task_ids: HashSet::new(),
            failed_fs_ids: HashSet::new(),
            subtask_retry_counts: HashMap::new(),
        }
    }

    /// 计算进度百分比
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        // 钳到 [0,100]：聚合 downloaded_size 已在 PR #52 修掉双算，这里再做一层防御，
        // 与上传 UploadTask::progress() 口径一致。
        ((self.downloaded_size as f64 / self.total_size as f64) * 100.0).clamp(0.0, 100.0)
    }

    /// 标记为下载中
    /// 登记「这个文件已经下完了」，并维持不变量：**已下完的文件不留在 pending_files 里**
    ///
    /// 这两件事必须绑在一起做，否则就是 issue #156 那条尾巴的形状：
    /// `counted_fs_ids` 把重复**计数**挡住了，但文件还赖在 `pending_files` 里，
    /// 补任务路径下一轮照样挑中它 → 全新 task_id、全新 WAL、整文件重下一遍。
    /// 实测 182 文件 / 15.4GiB 的文件夹因此建了 891 个子任务、白烧 33.5GiB。
    ///
    /// 调用点收敛在「子任务成功完成」这一处（`FolderDownloadManager` 的完成监听器），
    /// 因为不论子任务是从哪条启动路径起来的，成功后都必经那里——不必枚举启动路径。
    ///
    /// `fs_id == 0` 是异常数据，计数侧按 task_id 回退去重，这里同样不做任何处理：
    /// 真按 0 去剪会一次剪掉所有异常文件，让文件夹永远收不了工。
    pub fn mark_fs_id_completed(&mut self, fs_id: u64) {
        if fs_id == 0 {
            return;
        }
        if self.file_store.is_none() { self.counted_fs_ids.insert(fs_id); }
        self.pending_files.retain(|f| f.fs_id != fs_id);
    }

    /// 把「已下完但还赖在 pending 里」的文件一次性剪掉，用于恢复时自愈脏快照
    ///
    /// issue #156 修复之前写下的快照，pending_files 里带着已经下完的文件；重启恢复后
    /// 补任务路径会照样挑中它们重下一遍。已完成的子任务重启后不在内存任务表里
    /// （scheduler 完成时就把它移出了），按"活着的任务"去重兜不住这种情况，
    /// 只有持久化的 `counted_fs_ids` 才是权威答案。
    ///
    /// 返回剪掉的条数，便于恢复日志说明自愈了多少。
    pub fn prune_completed_from_pending(&mut self) -> usize {
        let before = self.pending_files.len();
        let counted = &self.counted_fs_ids;
        self.pending_files
            .retain(|f| f.fs_id == 0 || !counted.contains(&f.fs_id));
        before - self.pending_files.len()
    }

    /// 用子任务历史补回快照里漏记的完成，返回快照是否因此变化
    ///
    /// 子任务完成时先删 WAL、归档进历史库，之后文件夹才在内存里 +1，快照落盘还要再晚
    /// 一点（节流）。进程在这之间被强杀，重启后这几个文件两头都找不到：下载管理器里
    /// 已经没有任务，快照的 `completed_count` / `counted_fs_ids` 也没记上。文件夹
    /// 永远凑不满 `total_files`，停在 downloading —— 依赖它收尾的分享同步续跑接管
    /// 也就一直挂着（实测 6 个文件下完 5 个后被杀，重启后只认 2 个，最终卡在 3/6）。
    ///
    /// 完成数取三者最大值，不做加法：
    /// - 快照自己的 `completed_count`；
    /// - `counted_fs_ids` 与历史 fs_id 的并集大小 —— 历史库按天清理，老的完成记录可能
    ///   已经没了，但快照里还记着，取并集两边都不丢；
    /// - 历史里的文件数 —— 兜住 fs_id 缺失的记录，以及 `counted_fs_ids` 还没持久化
    ///   时写下的老快照（那时只有 `completed_count`，fs_id 集合是空的）。
    pub fn merge_completed_history(
        &mut self,
        files: &[crate::persistence::history_db::CompletedGroupFile],
    ) -> bool {
        if files.is_empty() {
            return false;
        }
        let counted_before = self.counted_fs_ids.len();
        let completed_before = self.completed_count;

        self.counted_fs_ids
            .extend(files.iter().filter_map(|f| f.fs_id));
        let floor = (self.counted_fs_ids.len() as u64).max(files.len() as u64);
        self.completed_count = self.completed_count.max(floor);

        // 已下载字节同理补齐，否则文件夹完成了进度条还停在漏记前的位置
        let history_bytes: u64 = files.iter().map(|f| f.file_size).sum();
        self.completed_downloaded_size = self.completed_downloaded_size.max(history_bytes);
        let floor_bytes = if self.total_size > 0 {
            self.completed_downloaded_size.min(self.total_size)
        } else {
            self.completed_downloaded_size
        };
        self.downloaded_size = self.downloaded_size.max(floor_bytes);

        self.completed_count != completed_before || self.counted_fs_ids.len() != counted_before
    }

    pub fn mark_downloading(&mut self) {
        self.status = FolderStatus::Downloading;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 重扫前清空上一轮的扫描产物（累计计数 + 待办队列）
    ///
    /// `total_files` / `total_size` 是 `scan_one_dir` 里的 `+=` 累加器，全项目没有第二个
    /// 写入点。扫描中途进程被杀时 `scan_completed` 仍是 `false`，而 `restore_folders` 会把
    /// 快照原样恢复成 Paused —— 用户点继续就会从根目录重扫一遍。若不清零，同一批文件被
    /// 二次累加：总数虚高后 `completed + skipped` 永远够不到，文件夹卡在 downloading 进不了
    /// 终态；`pending_files` 里还会留下同一 `fs_id` 的两条记录，补任务时重复建任务、整文件重下。
    ///
    /// 只清「扫描产物」，不动 `completed_count` / `counted_fs_ids` 等下载账本 ——
    /// 重启恢复时那些是既成事实，清了会导致重复下载。
    pub fn reset_scan_result(&mut self) {
        self.total_files = 0;
        self.total_size = 0;
        self.pending_files.clear();
        self.scan_progress = None;
        self.scan_completed = false;
        // 加密目录映射是扫描过程收集的，重扫会重新收集；留着会和上一轮混在一起
        self.encrypted_folder_mappings.clear();
    }

    /// 标记为已完成
    pub fn mark_completed(&mut self) {
        self.status = FolderStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
    }

    /// 标记为失败
    pub fn mark_failed(&mut self, error: String) {
        self.status = FolderStatus::Failed;
        self.error = Some(error);
    }

    /// 标记为暂停
    pub fn mark_paused(&mut self) {
        self.status = FolderStatus::Paused;
    }

    /// 标记为取消
    pub fn mark_cancelled(&mut self) {
        self.status = FolderStatus::Cancelled;
    }

    /// 🔥 计算并更新 downloaded_size：已完成累计 + 当前活跃子任务已下载
    ///
    /// 使用 max() 保证单调性，即使完成通知和进度通知乱序也不会丢字节。
    ///
    /// **封顶到 total_size**：`completed_downloaded_size` 与 `active_sum` 一旦有一次重复累加，
    /// `max()` 会把虚高值永久锁死（实测出现过 30.78 GB / 31.35 GB = 98.2%，而真实下载量只有
    /// 17 GB）。已下载超过总大小在任何情况下都不是合法状态，这里钳住并告警，把静默的数据
    /// 损坏变成可定位的信号，而不是让用户看到一个永远回不去的错误进度。
    ///
    /// 注意：`total_size == 0` 表示扫描尚未完成，此时不封顶。
    pub fn compute_downloaded_size(&mut self, active_sum: u64) -> u64 {
        let computed = self.completed_downloaded_size + active_sum;
        let mut next = self.downloaded_size.max(computed);

        if self.total_size > 0 && next > self.total_size {
            tracing::warn!(
                "文件夹 {} 已下载字节超过总大小并被钳制: {} > {} (completed_downloaded_size={}, active_sum={}, completed_count={}/{}), \
                 通常意味着已完成字节被重复计入活跃任务",
                self.id,
                next,
                self.total_size,
                self.completed_downloaded_size,
                active_sum,
                self.completed_count,
                self.total_files,
            );
            next = self.total_size;
        }

        self.downloaded_size = next;
        self.downloaded_size
    }

    /// 🔥 统计「活跃子任务已下载字节」总和，排除已计入 completed_downloaded_size 的任务。
    ///
    /// 子任务成功时 completed_downloaded_size 先 += 该文件 total_size，但完成的任务可能仍短暂
    /// 残留在内存任务表（downloaded_size 已钳到 total_size）。若此刻把它也计进 active_sum，
    /// compute_downloaded_size 的 max() 会把「completed + active 双重累加」的翻倍值永久锁死，
    /// 表现为已下载/进度翻倍（200%）。这里按 counted_task_ids 过滤掉这些任务。
    pub fn active_downloaded_excluding_counted<'a, I>(&self, tasks: I) -> u64
    where
        I: IntoIterator<Item = (&'a str, u64)>,
    {
        tasks
            .into_iter()
            .filter(|(id, _)| !self.counted_task_ids.contains(*id) && self.file_store.as_ref().is_none_or(|s| !s.task_completed(id).unwrap_or(true)))
            .map(|(_, downloaded)| downloaded)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_snapshot_preserves_json_without_runtime_collections() {
        let mut folder = FolderDownload::new("/remote".into(), PathBuf::from("./local"));
        folder.pending_files.push(pending(42, "a.bin"));
        folder.counted_fs_ids.insert(7);
        folder.skipped_count = 2;
        folder.skipped_size = 64;
        folder.scan_progress = Some("/remote/sub".into());
        let snapshot = folder.api_snapshot();
        assert_eq!(serde_json::to_value(&folder).unwrap(), serde_json::to_value(&snapshot).unwrap());
        assert!(snapshot.pending_files.is_empty());
        assert!(snapshot.counted_fs_ids.is_empty());
        assert_eq!(folder.pending_files.len(), 1);
    }

    fn pending(fs_id: u64, name: &str) -> PendingFile {
        PendingFile {
            fs_id,
            filename: name.to_string(),
            remote_path: format!("/remote/{}", name),
            relative_path: name.to_string(),
            size: 1024,
        }
    }

    /// 🔥 issue #156 续（2026-09-18 日志复现）：文件下完后必须同时退出 pending 队列。
    ///
    /// 只 insert counted_fs_ids 而不剪 pending_files 时，补任务路径下一轮还会挑中它，
    /// 重新建一个子任务把整个文件再下一遍 —— 实测 182 文件的文件夹建了 891 个子任务。
    #[test]
    fn test_completed_file_leaves_pending_queue() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));
        folder.pending_files = vec![pending(101, "a.mp4"), pending(102, "b.mp4")];

        folder.mark_fs_id_completed(101);

        assert!(
            folder.counted_fs_ids.contains(&101),
            "完成的文件必须登记进 counted_fs_ids"
        );
        assert!(
            !folder.pending_files.iter().any(|f| f.fs_id == 101),
            "完成的文件必须从 pending_files 剪掉，否则补任务路径会重复建任务、整文件重下"
        );
        assert!(
            folder.pending_files.iter().any(|f| f.fs_id == 102),
            "未完成的文件不能被误删"
        );
    }

    /// 重扫必须清零累加器，否则"中断重启 → 继续"会让总数翻倍、文件夹永远收不了工。
    ///
    /// 场景：扫描到一半进程被杀 → 快照里 `total_files` 已有值、`scan_completed=false`
    /// → 恢复成 Paused → 点继续 → `scan_folder_and_create_tasks` 从根目录重扫。
    /// `total_files` 只在 `scan_one_dir` 里 `+=`，没有清零就会把同一批文件再加一遍。
    #[test]
    fn test_reset_scan_result_clears_accumulator_and_pending() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));
        folder.total_files = 5;
        folder.total_size = 5 * 1024;
        folder.pending_files = vec![pending(1, "a.mp4"), pending(2, "b.mp4")];
        folder.scan_progress = Some("/remote/sub".to_string());
        folder
            .encrypted_folder_mappings
            .insert("enc".to_string(), "dec".to_string());
        // 下载账本：重扫不该动
        folder.completed_count = 3;
        folder.counted_fs_ids.insert(9);

        folder.reset_scan_result();

        assert_eq!(folder.total_files, 0, "重扫前必须清零，否则同一批文件被二次累加");
        assert_eq!(folder.total_size, 0, "字节数同理");
        assert!(folder.pending_files.is_empty(), "上一轮的待办会被重扫重新灌入，留着会重复");
        assert!(folder.scan_progress.is_none());
        assert!(!folder.scan_completed);
        assert!(
            folder.encrypted_folder_mappings.is_empty(),
            "加密目录映射由扫描过程收集，重扫会重建，混着上一轮会串"
        );

        assert_eq!(folder.completed_count, 3, "重扫不能动已完成的下载账");
        assert!(folder.counted_fs_ids.contains(&9), "去重账本必须保留，否则会重复下载");
    }

    /// 重复完成（同一文件被两条路径各下了一遍）时也要自愈：
    /// 把残留在 pending 里的那份剪掉，否则循环会一直转下去。
    #[test]
    fn test_duplicate_completion_still_prunes_pending() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));
        folder.counted_fs_ids.insert(101);
        folder.pending_files = vec![pending(101, "a.mp4")];

        folder.mark_fs_id_completed(101);

        assert!(
            folder.pending_files.is_empty(),
            "已完成过的文件再次完成时，仍必须把 pending 里的残留剪掉（脏快照自愈）"
        );
    }

    /// fs_id == 0 是异常数据，计数侧按 task_id 回退去重。
    /// 若这里按 0 去剪，会一次剪掉所有异常文件，文件夹永远凑不满 total_files。
    #[test]
    fn test_zero_fs_id_is_not_pruned() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));
        folder.pending_files = vec![pending(0, "x.bin"), pending(0, "y.bin")];

        folder.mark_fs_id_completed(0);

        assert_eq!(
            folder.pending_files.len(),
            2,
            "fs_id==0 不能参与按 fs_id 的剪枝"
        );
        assert!(
            !folder.counted_fs_ids.contains(&0),
            "fs_id==0 不应进入 counted_fs_ids"
        );
    }

    /// 恢复脏快照时自愈：pending 里残留的已完成文件必须被剪掉，
    /// 否则重启后会把它们原样重下一遍（issue #156 续）。
    #[test]
    fn test_prune_completed_from_pending_heals_dirty_snapshot() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));
        folder.counted_fs_ids.insert(101);
        folder.counted_fs_ids.insert(103);
        folder.pending_files = vec![
            pending(101, "done.mp4"),
            pending(102, "todo.mp4"),
            pending(0, "weird.bin"),
            pending(103, "done2.mp4"),
        ];

        let pruned = folder.prune_completed_from_pending();

        assert_eq!(pruned, 2, "应剪掉两个已完成的文件");
        let left: Vec<u64> = folder.pending_files.iter().map(|f| f.fs_id).collect();
        assert_eq!(
            left,
            vec![102, 0],
            "只应留下未完成的文件；fs_id==0 必须放行（计数侧按 task_id 回退）"
        );
    }

    #[test]
    fn test_folder_download_creation() {
        let folder = FolderDownload::new(
            "/test/folder".to_string(),
            PathBuf::from("./downloads/folder"),
        );

        assert_eq!(folder.name, "folder");
        assert_eq!(folder.status, FolderStatus::Scanning);
        assert_eq!(folder.total_files, 0);
        assert_eq!(folder.progress(), 0.0);
    }

    #[test]
    fn test_progress_calculation() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));

        folder.total_size = 1000;
        folder.downloaded_size = 250;
        assert_eq!(folder.progress(), 25.0);

        folder.downloaded_size = 500;
        assert_eq!(folder.progress(), 50.0);
    }

    #[test]
    fn test_status_transitions() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));

        folder.mark_downloading();
        assert_eq!(folder.status, FolderStatus::Downloading);
        assert!(folder.started_at.is_some());

        folder.mark_paused();
        assert_eq!(folder.status, FolderStatus::Paused);

        folder.mark_failed("Network error".to_string());
        assert_eq!(folder.status, FolderStatus::Failed);
        assert_eq!(folder.error, Some("Network error".to_string()));

        folder.mark_completed();
        assert_eq!(folder.status, FolderStatus::Completed);
        assert!(folder.completed_at.is_some());
    }

    #[test]
    fn test_compute_downloaded_size_monotonic() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));

        // 子任务 A(100) 和 B(200) 活跃，分别已下载 50 和 80
        assert_eq!(folder.compute_downloaded_size(130), 130);

        // A 完成，completed_downloaded_size += 100
        folder.completed_downloaded_size += 100;
        // B 仍在下载 80
        assert_eq!(folder.compute_downloaded_size(80), 180);

        // 乱序：完成通知先到达，active_sum 短暂变小，max() 保证不回退
        folder.completed_downloaded_size += 200;
        // B 已从内存移除，active_sum = 0，但 downloaded_size 不应回退
        assert_eq!(folder.compute_downloaded_size(0), 300);
    }

    #[test]
    fn test_compute_downloaded_size_never_regresses() {
        let mut folder = FolderDownload::new("/test".to_string(), PathBuf::from("./test"));
        folder.completed_downloaded_size = 1000;
        folder.downloaded_size = 1500; // 之前快照过 active_sum=500

        // 完成通知先到达，active_sum 短暂变为 0
        // computed = 1000 + 0 = 1000，但 max(1500, 1000) = 1500，不回退
        assert_eq!(folder.compute_downloaded_size(0), 1500);
    }

    #[test]
    fn test_active_downloaded_excludes_counted_no_double_count() {
        // 回归：单文件夹只有一个文件 A(total=3.25G)，下载完成。
        // 完成时 completed_downloaded_size += A，但 A 仍短暂残留在内存任务表里（downloaded_size=A）。
        // 旧逻辑把 A 同时计进 active_sum → compute = A + A = 2A，max() 锁死 → 200%。
        // 新逻辑按 counted_task_ids 过滤掉 A，active_sum=0 → downloaded_size=A，不翻倍。
        const A: u64 = 3_250_000_000;
        let mut folder = FolderDownload::new("/测试2-1".to_string(), PathBuf::from("./t"));
        folder.total_size = A;
        folder.total_files = 1;

        // A 完成：计入完成累计 + 标记 counted
        folder.completed_downloaded_size += A;
        folder.counted_task_ids.insert("task-A".to_string());

        // A 仍残留在内存任务表，downloaded_size 已钳到 total_size=A
        let lingering = [("task-A", A)];
        let active = folder.active_downloaded_excluding_counted(lingering.iter().copied());
        assert_eq!(active, 0, "已计数的完成任务必须从 active_sum 中排除");

        let total = folder.compute_downloaded_size(active);
        assert_eq!(total, A, "已下载不应翻倍");
        assert!(folder.progress() <= 100.0, "进度不应超过 100%");
        assert_eq!(folder.progress(), 100.0);
    }

    #[test]
    fn test_active_downloaded_counts_uncounted_tasks() {
        // 未完成（未计数）的活跃任务仍应计入 active_sum。
        let folder = FolderDownload::new("/d".to_string(), PathBuf::from("./d"));
        let tasks = [("task-A", 50u64), ("task-B", 80u64)];
        let active = folder.active_downloaded_excluding_counted(tasks.iter().copied());
        assert_eq!(active, 130);
    }

    fn done(
        rel: &str,
        fs_id: Option<u64>,
        size: u64,
    ) -> crate::persistence::history_db::CompletedGroupFile {
        crate::persistence::history_db::CompletedGroupFile {
            relative_path: rel.to_string(),
            fs_id,
            file_size: size,
        }
    }

    /// 回归 2026-10-18 本地复现：6 个文件下完 5 个后被强杀，快照只记了 2 个。
    /// 历史库里有 5 条完成记录，恢复时必须据此补回，否则文件夹卡在 3/6 永远收不了尾。
    #[test]
    fn test_merge_completed_history_recovers_lost_completions() {
        let mut folder = FolderDownload::new("/d".to_string(), PathBuf::from("./d"));
        folder.total_files = 6;
        folder.total_size = 600;
        folder.completed_count = 2;
        folder.counted_fs_ids.extend([1, 2]);
        folder.completed_downloaded_size = 200;
        folder.downloaded_size = 250;

        let history: Vec<_> = (1..=5)
            .map(|i| done(&format!("{}.mp4", i), Some(i), 100))
            .collect();
        assert!(folder.merge_completed_history(&history));

        assert_eq!(folder.completed_count, 5, "漏记的 3 个完成必须补回");
        assert_eq!(
            folder.counted_fs_ids.len(),
            5,
            "补回的文件要登记 fs_id，防止重复计数/重下"
        );
        assert_eq!(folder.completed_downloaded_size, 500);
        assert_eq!(folder.downloaded_size, 500);

        // 再合并一次是幂等的
        assert!(!folder.merge_completed_history(&history));
        assert_eq!(folder.completed_count, 5);
    }

    /// 历史库按天清理：老的完成记录没了，但快照里记着。取并集，两边都不能丢。
    #[test]
    fn test_merge_completed_history_unions_with_snapshot() {
        let mut folder = FolderDownload::new("/d".to_string(), PathBuf::from("./d"));
        folder.completed_count = 3;
        folder.counted_fs_ids.extend([1, 2, 3]);

        // 1、2 的历史已被清理；3 还在；4 是快照漏记的
        let history = vec![done("3.mp4", Some(3), 10), done("4.mp4", Some(4), 10)];
        assert!(folder.merge_completed_history(&history));
        assert_eq!(
            folder.completed_count, 4,
            "应为 {{1,2,3}} ∪ {{3,4}} 共 4 个"
        );
    }

    /// 老快照没有持久化 counted_fs_ids：完成数只能和历史条数取最大，不能相加，
    /// 否则同一批文件被算两遍。
    #[test]
    fn test_merge_completed_history_does_not_double_count_legacy_snapshot() {
        let mut folder = FolderDownload::new("/d".to_string(), PathBuf::from("./d"));
        folder.completed_count = 3;

        let history: Vec<_> = (1..=3)
            .map(|i| done(&format!("{}.mp4", i), Some(i), 10))
            .collect();
        assert!(
            folder.merge_completed_history(&history),
            "fs_id 补登记算变化"
        );
        assert_eq!(folder.completed_count, 3);
    }

    /// 历史记录缺 fs_id 时按条数兜底；空历史什么都不动。
    #[test]
    fn test_merge_completed_history_without_fs_id_and_empty() {
        let mut folder = FolderDownload::new("/d".to_string(), PathBuf::from("./d"));
        folder.completed_count = 1;
        assert!(!folder.merge_completed_history(&[]));

        let history = vec![done("a.bin", None, 10), done("b.bin", None, 10)];
        assert!(folder.merge_completed_history(&history));
        assert_eq!(folder.completed_count, 2);
        assert!(folder.counted_fs_ids.is_empty());
    }
}
