use crate::auth::Uid;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// 下载任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    /// 等待中
    Pending,
    /// 下载中
    Downloading,
    /// 解密中（新增）
    Decrypting,
    /// 已暂停
    Paused,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

impl TaskStatus {
    /// 任务是否处于"活跃下载"状态：尚未到达终态（Completed/Failed/Paused），
    /// 且当前正在消耗/即将消耗下载槽位的状态。
    ///
    /// 用法：上层聚合瞬时下载速度 / 文件夹子任务活跃计数时使用，
    /// 不能只看 `Downloading` —— folder group 的子任务在下载/解密/等待之间切换，
    /// 仅过滤 `Downloading` 会漏掉 `Decrypting`/`Pending` 子任务的速度贡献，
    /// 导致前端展示速度恒为 0。
    pub fn is_active_download_status(&self) -> bool {
        matches!(
            self,
            TaskStatus::Pending | TaskStatus::Downloading | TaskStatus::Decrypting
        )
    }
}

/// 下载任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    /// 任务ID
    pub id: String,
    /// 🔥 多账号归属 UID
    ///
    /// 所有运态下载任务必须带 owner_uid。旧持久化数据反序列化时使用默认值 `Uid(0)`，
    /// 随后由恢复逻辑在加载时填充为 `active_uid` 或进入账号丢失分支。
    #[serde(default)]
    pub owner_uid: Uid,
    /// 文件服务器ID
    pub fs_id: u64,
    /// 网盘路径
    pub remote_path: String,
    /// 本地保存路径
    pub local_path: PathBuf,
    /// 文件大小
    pub total_size: u64,
    /// 已下载大小
    pub downloaded_size: u64,
    /// 任务状态
    pub status: TaskStatus,
    /// 下载速度 (bytes/s)
    pub speed: u64,
    /// 创建时间 (Unix timestamp)
    pub created_at: i64,
    /// 开始时间 (Unix timestamp)
    pub started_at: Option<i64>,
    /// 完成时间 (Unix timestamp)
    pub completed_at: Option<i64>,
    /// 错误信息
    pub error: Option<String>,

    // === 文件夹下载相关字段 ===
    /// 文件夹下载组ID，单文件下载时为 None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// 文件夹根路径，如 "/电影"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_root: Option<String>,
    /// 相对于根文件夹的路径，如 "科幻片/星际穿越.mp4"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,

    // === 🔥 新增：跨任务跳转相关字段 ===
    /// 关联的转存任务 ID（如果此下载任务由转存任务自动创建）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_task_id: Option<String>,

    // === 🔥 新增：任务位借调机制相关字段 ===
    /// 占用的槽位ID
    #[serde(skip)]
    pub slot_id: Option<usize>,

    /// 是否使用借调位（而非固定位）
    #[serde(skip)]
    pub is_borrowed_slot: bool,

    /// 🔥 是否占用文件夹固定槽位（既不占 task_slot_pool 也不占借调位）
    ///
    /// 当文件夹子任务被分配到所属文件夹的 fixed_slot_subtask 时为 true。
    /// 此时 `slot_id = None`、`is_borrowed_slot = false`，但任务并不需要再申请新槽位。
    ///
    /// 等待队列消费点判断 `needs_slot` 时必须额外检查此字段，
    /// 避免出现"slot_id=None → 申请槽位 → 立即又被打回等待队列"的自循环。
    #[serde(skip)]
    pub uses_folder_fixed_slot: bool,

    // === 🔥 新增：自动备份相关字段 ===
    /// 是否为自动备份任务
    #[serde(default)]
    pub is_backup: bool,

    /// 关联的备份配置ID（is_backup=true 时使用）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_config_id: Option<String>,

    /// 🔥 启动重试次数（用于限制准备/注册失败的重试）
    #[serde(skip)]
    pub start_retry_count: u32,

    // === 🔥 解密相关字段 ===
    /// 是否为加密文件（通过文件名或内容检测）
    #[serde(default)]
    pub is_encrypted: bool,

    /// 解密进度 (0.0 - 100.0)
    #[serde(default)]
    pub decrypt_progress: f64,

    /// 解密后的最终文件路径
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decrypted_path: Option<PathBuf>,

    /// 原始文件名（解密后恢复的文件名）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_filename: Option<String>,

    // === 🔥 分享直下相关字段 ===
    /// 是否为分享直下任务（完成后不自动清除，由转存管理器清理）
    #[serde(default)]
    pub is_share_direct_download: bool,

    /// 🔥 退回等待队列冷却时间（Unix 毫秒时间戳）
    ///
    /// auto_requeue_task 设置为 now + REQUEUE_COOLDOWN_SECS。
    /// 等待队列消费点（try_start_waiting_tasks / monitor / 0 延迟 trigger）
    /// 在拉起任务前必须检查此字段，未到期任务放回队尾。
    ///
    /// 任务被实际拉起（槽位分配成功）后清空。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<i64>,

    /// 🔥 解密协程版本号（epoch / generation token）
    ///
    /// 用于在"暂停 Decrypting → 快速恢复 → 新一轮解密协程启动 → 旧协程跑完"
    /// 这种 race 中，识别并失效旧协程的执行结果，防止旧协程的破坏性副作用
    /// （删除加密文件、改写 task.local_path、持久化 update_local_path、
    ///  以及 handle_task_completion 的 mark_completed/failed 终态写入）
    /// 污染新一轮恢复后的状态机。
    ///
    /// 工作机制：
    /// - 调度器在 `tokio::spawn` 解密协程的最开始一次 lock 任务，快照当前
    ///   `decrypt_epoch` 到本地变量 `my_epoch`，并把它一路传到
    ///   `try_decrypt_if_encrypted` 与 `handle_task_completion`。
    /// - `cancel_tasks_by_group` 把 `Decrypting` 状态翻成 `Paused` 的同锁段内
    ///   调 `invalidate_decrypt_epoch()` 递增此字段——这相当于通知
    ///   "在我之前 spawn 的所有解密协程都过期了"。
    /// - 解密协程在所有破坏性副作用之前（spawn_blocking 前预检 / spawn_blocking 后
    ///   硬检查 / 进度回调内 / handle_task_completion 入口原子判定）再次 lock 任务，
    ///   比对 `task.decrypt_epoch == my_epoch`：不一致即视为过期协程，跳过所有
    ///   副作用 return。
    ///
    /// 仅 `#[serde(skip)]`：纯运行时状态，重启后从 0 开始也安全（重启不会有遗留协程）。
    #[serde(skip)]
    pub decrypt_epoch: u64,

    /// 🔥 解密原子提交标志（不可中断的收尾标记）
    ///
    /// 用于闭合 R21 注释里诚实标注的剩余时序窗口：rename(attempt → final)
    /// 成功之后到删除加密文件 / 持久化 update_local_path 之间，如果用户暂停文件夹，
    /// `cancel_tasks_by_group` 会把 status 翻成 Paused 并 invalidate_decrypt_epoch；
    /// 后续的破坏性操作（rm 加密文件、持久化）虽然会被本协程内的 stale check
    /// 在某些位置拦截，但在 rename 之后已经把 final 文件就位的状态下，让任务
    /// 走"暂停期间任务意外完成了一半（final 已就位但 task 字段未更新）"的不一致
    /// transient state 不是好的语义。
    ///
    /// **协作机制**：
    /// - `try_decrypt_if_encrypted` 在 rename 成功后的同一锁段内做最后一次 stale check：
    ///   - stale → 不动 final，return Ok(())
    ///   - 通过 → 锁内置位 `decrypt_committed = true`，并同步完成 `mark_decrypt_completed`
    ///     + 改 `task.local_path = decrypted_path`（这些动作原本在锁外的 step 9，
    ///     现在合并入锁内提交点保证原子性）
    /// - `cancel_tasks_by_group` 在判定 active 时多看一项：`decrypt_committed=true`
    ///   时绝不翻 Paused / 不释放槽位 / 不递增 epoch——让本协程无中断地完成
    ///   后续锁外的删加密文件 + 持久化。这两个锁外动作即使发生 race 也只是
    ///   warn 不返回 Err，task 字段已经在锁内提交了，最终 handle_task_completion
    ///   会正常 mark_completed。
    ///
    /// 仅 `#[serde(skip)]`：与 `decrypt_epoch` 同理，纯运行时状态。重启后从 false
    /// 开始也安全（重启不会有遗留协程）。
    ///
    /// 一旦置位永不回退（任务即将进入 Completed 终态，回退无意义）。
    #[serde(skip)]
    pub decrypt_committed: bool,
}

impl DownloadTask {
    pub fn new(
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        owner_uid: Uid,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            owner_uid,
            fs_id,
            remote_path,
            local_path,
            total_size,
            downloaded_size: 0,
            status: TaskStatus::Pending,
            speed: 0,
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            error: None,
            // 文件夹下载字段默认为 None
            group_id: None,
            group_root: None,
            relative_path: None,
            // 转存任务关联字段默认为 None
            transfer_task_id: None,
            // 任务位借调机制字段初始化
            slot_id: None,
            is_borrowed_slot: false,
            uses_folder_fixed_slot: false,
            // 自动备份字段初始化
            is_backup: false,
            backup_config_id: None,
            start_retry_count: 0,
            // 解密字段初始化
            is_encrypted: false,
            decrypt_progress: 0.0,
            decrypted_path: None,
            original_filename: None,
            // 分享直下字段初始化
            is_share_direct_download: false,
            // 🔥 退回等待队列冷却字段初始化
            next_retry_at: None,
            // 🔥 解密协程版本号字段初始化
            decrypt_epoch: 0,
            // 🔥 解密原子提交标志初始化
            decrypt_committed: false,
        }
    }

    /// 设置关联的转存任务 ID
    pub fn set_transfer_task_id(&mut self, transfer_task_id: String) {
        self.transfer_task_id = Some(transfer_task_id);
    }

    /// 创建带文件夹组信息的任务
    pub fn new_with_group(
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        group_id: String,
        group_root: String,
        relative_path: String,
        owner_uid: Uid,
    ) -> Self {
        let mut task = Self::new(fs_id, remote_path, local_path, total_size, owner_uid);
        task.group_id = Some(group_id);
        task.group_root = Some(group_root);
        task.relative_path = Some(relative_path);
        task
    }

    /// 创建自动备份下载任务
    pub fn new_backup(
        fs_id: u64,
        remote_path: String,
        local_path: PathBuf,
        total_size: u64,
        backup_config_id: String,
        owner_uid: Uid,
    ) -> Self {
        let mut task = Self::new(fs_id, remote_path, local_path, total_size, owner_uid);
        task.is_backup = true;
        task.backup_config_id = Some(backup_config_id);
        task
    }

    /// 计算进度百分比
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        // 钳到 [0,100]：downloaded_size 在分片回调处已封顶到 total_size，这里再做一层
        // 防御，与上传 UploadTask::progress() 口径一致。
        ((self.downloaded_size as f64 / self.total_size as f64) * 100.0).clamp(0.0, 100.0)
    }

    /// 估算剩余时间 (秒)
    pub fn eta(&self) -> Option<u64> {
        if self.speed == 0 || self.downloaded_size >= self.total_size {
            return None;
        }
        let remaining = self.total_size - self.downloaded_size;
        Some(remaining / self.speed)
    }

    /// 标记为下载中
    pub fn mark_downloading(&mut self) {
        self.status = TaskStatus::Downloading;
        if self.started_at.is_none() {
            self.started_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// 标记为解密中
    pub fn mark_decrypting(&mut self) {
        self.status = TaskStatus::Decrypting;
    }

    /// 🔥 失效正在进行的解密协程
    ///
    /// 递增 `decrypt_epoch`，让任何已经 spawn 但还未走到检查点的解密协程
    /// 在比对 `my_epoch` 时识别自己已过期，跳过所有破坏性副作用 return。
    ///
    /// 必须在与 `cancel_tasks_by_group` 把 `Decrypting → Paused` 的状态翻转
    /// 同一把锁内调用，保证旧协程后续任何检查点都不会再看到与自身 epoch 一致
    /// 的状态。
    ///
    /// 用 wrapping_add 避免理论上的 u64 溢出 panic（实际上 `2^64` 次暂停不可能达到，
    /// 但防御性编码不增加成本）。
    pub fn invalidate_decrypt_epoch(&mut self) {
        self.decrypt_epoch = self.decrypt_epoch.wrapping_add(1);
    }

    /// 更新解密进度
    pub fn update_decrypt_progress(&mut self, progress: f64) {
        self.decrypt_progress = progress.clamp(0.0, 100.0);
    }

    /// 标记解密完成
    pub fn mark_decrypt_completed(&mut self, decrypted_path: PathBuf, original_size: u64) {
        self.decrypted_path = Some(decrypted_path);
        self.total_size = original_size; // 恢复为原始大小
        self.downloaded_size = original_size;
        self.decrypt_progress = 100.0;
    }

    /// 检测文件名是否为加密文件
    pub fn detect_encrypted_filename(filename: &str) -> bool {
        crate::encryption::EncryptionService::is_encrypted_filename(filename)
    }

    /// 标记为已完成
    pub fn mark_completed(&mut self) {
        self.status = TaskStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
        self.downloaded_size = self.total_size;
    }

    /// 标记为失败
    pub fn mark_failed(&mut self, error: String) {
        self.status = TaskStatus::Failed;
        self.error = Some(error);
    }

    /// 标记为暂停
    pub fn mark_paused(&mut self) {
        self.status = TaskStatus::Paused;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let task = DownloadTask::new(
            12345,
            "/test/file.txt".to_string(),
            PathBuf::from("./downloads/file.txt"),
            1024 * 1024, // 1MB
            Uid::default(),
        );

        assert_eq!(task.fs_id, 12345);
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.downloaded_size, 0);
        assert_eq!(task.progress(), 0.0);
    }

    #[test]
    fn test_progress_calculation() {
        let mut task = DownloadTask::new(
            1,
            "/test".to_string(),
            PathBuf::from("./test"),
            1000,
            Uid::default(),
        );

        task.downloaded_size = 250;
        assert_eq!(task.progress(), 25.0);

        task.downloaded_size = 500;
        assert_eq!(task.progress(), 50.0);

        task.downloaded_size = 1000;
        assert_eq!(task.progress(), 100.0);
    }

    /// 防御：downloaded_size 异常超出 total_size 时，进度仍钳到 100%。
    #[test]
    fn test_progress_clamped_when_downloaded_exceeds_total() {
        let mut task = DownloadTask::new(
            1,
            "/test".to_string(),
            PathBuf::from("./test"),
            1000,
            Uid::default(),
        );
        task.downloaded_size = 1420; // 模拟异常超出
        assert_eq!(task.progress(), 100.0);
    }

    #[test]
    fn test_eta_calculation() {
        let mut task = DownloadTask::new(
            1,
            "/test".to_string(),
            PathBuf::from("./test"),
            1000,
            Uid::default(),
        );

        task.downloaded_size = 200;
        task.speed = 100; // 100 bytes/s
        assert_eq!(task.eta(), Some(8)); // (1000 - 200) / 100 = 8s

        task.speed = 0;
        assert_eq!(task.eta(), None); // 速度为0，无法估算
    }

    #[test]
    fn test_status_transitions() {
        let mut task = DownloadTask::new(
            1,
            "/test".to_string(),
            PathBuf::from("./test"),
            1000,
            Uid::default(),
        );

        task.mark_downloading();
        assert_eq!(task.status, TaskStatus::Downloading);
        assert!(task.started_at.is_some());

        task.mark_paused();
        assert_eq!(task.status, TaskStatus::Paused);

        task.mark_failed("Network error".to_string());
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.error, Some("Network error".to_string()));

        task.mark_completed();
        assert_eq!(task.status, TaskStatus::Completed);
        assert_eq!(task.downloaded_size, task.total_size);
        assert!(task.completed_at.is_some());
    }

    #[test]
    fn test_decrypting_status() {
        let mut task = DownloadTask::new(
            12345,
            "/test/BPR_BKUP_uuid.bkup".to_string(),
            PathBuf::from("./downloads/BPR_BKUP_uuid.bkup"),
            1100,
            Uid::default(),
        );

        // 测试解密状态转换
        task.is_encrypted = true;
        task.mark_downloading();
        assert_eq!(task.status, TaskStatus::Downloading);

        task.mark_decrypting();
        assert_eq!(task.status, TaskStatus::Decrypting);

        // 测试解密进度更新
        task.update_decrypt_progress(75.0);
        assert_eq!(task.decrypt_progress, 75.0);

        // 测试进度边界
        task.update_decrypt_progress(150.0);
        assert_eq!(task.decrypt_progress, 100.0);

        task.update_decrypt_progress(-10.0);
        assert_eq!(task.decrypt_progress, 0.0);
    }

    #[test]
    fn test_decrypt_completed() {
        let mut task = DownloadTask::new(
            12345,
            "/test/BPR_BKUP_uuid.bkup".to_string(),
            PathBuf::from("./downloads/BPR_BKUP_uuid.bkup"),
            1100,
            Uid::default(),
        );

        task.is_encrypted = true;
        task.mark_decrypting();
        task.mark_decrypt_completed(PathBuf::from("./downloads/original.txt"), 1024);

        assert_eq!(task.decrypt_progress, 100.0);
        assert_eq!(task.total_size, 1024);
        assert!(task.decrypted_path.is_some());
    }

    #[test]
    fn test_detect_encrypted_filename() {
        // 有效的加密文件名：UUID.dat
        assert!(DownloadTask::detect_encrypted_filename(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890.dat"
        ));
        // 无效的文件名
        assert!(!DownloadTask::detect_encrypted_filename("normal_file.txt"));
        assert!(!DownloadTask::detect_encrypted_filename("not-a-uuid.dat"));
    }

    /// 测试旧版本 JSON 数据反序列化兼容性
    /// 确保缺少新增解密字段的旧数据能正确反序列化
    #[test]
    fn test_backward_compatibility_deserialization() {
        // 模拟旧版本的 JSON 数据（不包含解密相关字段）
        let old_json = r#"{
            "id": "old-task-456",
            "fs_id": 12345,
            "remote_path": "/test/file.txt",
            "local_path": "./downloads/file.txt",
            "total_size": 1024,
            "downloaded_size": 512,
            "status": "downloading",
            "speed": 100,
            "created_at": 1703203200,
            "started_at": 1703203201,
            "completed_at": null,
            "error": null,
            "group_id": null,
            "group_root": null,
            "relative_path": null,
            "transfer_task_id": null,
            "is_backup": false,
            "backup_config_id": null
        }"#;

        // 反序列化应该成功，新字段使用默认值
        let task: DownloadTask = serde_json::from_str(old_json).expect("反序列化旧版本数据失败");

        // 验证基本字段
        assert_eq!(task.id, "old-task-456");
        assert_eq!(task.fs_id, 12345);
        assert_eq!(task.total_size, 1024);
        assert_eq!(task.status, TaskStatus::Downloading);

        // 验证新增解密字段使用默认值
        assert!(!task.is_encrypted); // 默认 false
        assert_eq!(task.decrypt_progress, 0.0); // 默认 0.0
        assert!(task.decrypted_path.is_none()); // 默认 None
        assert!(task.original_filename.is_none()); // 默认 None

        // 🔥 验证 next_retry_at 字段兼容（旧 JSON 无此字段 → None）
        assert!(task.next_retry_at.is_none());
        // 🔥 验证 is_share_direct_download 字段兼容
        assert!(!task.is_share_direct_download);
    }

    /// 测试 next_retry_at 的序列化/反序列化
    #[test]
    fn test_next_retry_at_roundtrip() {
        let mut task = DownloadTask::new(
            1,
            "/remote".to_string(),
            PathBuf::from("./local"),
            1024,
            Uid::default(),
        );

        // Case 1: None 时不应该序列化出来（skip_serializing_if）
        let json_none = serde_json::to_string(&task).expect("序列化失败");
        assert!(
            !json_none.contains("next_retry_at"),
            "None 时 next_retry_at 不应被序列化，实际 JSON: {}",
            json_none
        );

        // Case 2: 有值时正确往返
        task.next_retry_at = Some(1234567890123);
        let json_some = serde_json::to_string(&task).expect("序列化失败");
        assert!(json_some.contains("next_retry_at"));
        let restored: DownloadTask = serde_json::from_str(&json_some).expect("反序列化失败");
        assert_eq!(restored.next_retry_at, Some(1234567890123));
    }

    /// 测试新版本 JSON 数据序列化/反序列化
    #[test]
    fn test_new_version_serialization() {
        let mut task = DownloadTask::new(
            12345,
            "/test/BPR_BKUP_uuid.bkup".to_string(),
            PathBuf::from("./downloads/BPR_BKUP_uuid.bkup"),
            1100,
            Uid::default(),
        );
        task.is_encrypted = true;
        task.decrypt_progress = 75.0;
        task.decrypted_path = Some(PathBuf::from("./downloads/original.txt"));
        task.original_filename = Some("original.txt".to_string());

        // 序列化
        let json = serde_json::to_string(&task).expect("序列化失败");

        // 反序列化
        let restored: DownloadTask = serde_json::from_str(&json).expect("反序列化失败");

        // 验证解密字段正确恢复
        assert!(restored.is_encrypted);
        assert_eq!(restored.decrypt_progress, 75.0);
        assert_eq!(
            restored.decrypted_path,
            Some(PathBuf::from("./downloads/original.txt"))
        );
        assert_eq!(restored.original_filename, Some("original.txt".to_string()));
    }
}
