// 多账号管理器
//
// 职责：accounts.json 的 CRUD + 持久化；
//       从旧版 session.json 单用户数据迁移到 accounts.json。
//
// 完整的损坏回退链路 + 滚动 .bak + 幂等迁移
// （详见 docs/multi-account-design.md）。

use crate::auth::types::{AccountSummary, AccountsData, Uid, UserAuth};
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{info, warn};

/// 多账号管理器
pub struct AccountManager {
    /// accounts.json 文件路径
    accounts_file: PathBuf,
    /// 内存中的账号数据
    data: AccountsData,
}

impl AccountManager {
    // ───────── 加载 / 保存 ─────────

    /// 从 `accounts.json` 加载，按设计文档的回退链路：
    ///   1. `accounts.json` 解析成功 → 使用此数据
    ///   2. `accounts.json.bak` 存在 + 解析成功 → 使用备份并重写 `accounts.json`
    ///      （重写走 `save_no_rolling`：不把损坏的 accounts.json 滚成新 .bak，
    ///      避免覆盖刚验证通过的好备份）
    ///   3. 都失败 → 返回空 `AccountsData`（实际触发迁移由调用方
    ///      `migrate_from_session` 完成 session.json 链路）
    ///
    /// 文件不存在不算失败；读取/反序列化失败才走 `.bak` 回退。
    ///
    /// **注意**：本方法不读 `session.json` / `session.json.migrated`。完整启动
    /// 路径请使用 `load_or_migrate`，由它统一编排 4 级回退链路。
    pub async fn load(accounts_file: impl Into<PathBuf>) -> Result<Self> {
        let accounts_file = accounts_file.into();
        let bak_file = accounts_file.with_extension("json.bak");

        // 第 1 步：优先读 accounts.json
        match Self::read_json(&accounts_file).await {
            Ok(Some(data)) => {
                info!(
                    "AccountManager 加载完成：{} 个账号，active_uid={:?}",
                    data.users.len(),
                    data.active_uid
                );
                return Ok(Self {
                    accounts_file,
                    data,
                });
            }
            Ok(None) => {
                // 文件不存在 — 不是损坏；返回空数据（迁移路径会从 session.json 兜底）
                return Ok(Self {
                    accounts_file,
                    data: AccountsData::default(),
                });
            }
            Err(e) => {
                warn!(
                    "AccountManager: accounts.json 解析失败：{}（将尝试 .bak 回退）",
                    e
                );
            }
        }

        // 第 2 步：accounts.json 损坏 → 试 accounts.json.bak
        match Self::read_json(&bak_file).await {
            Ok(Some(data)) => {
                warn!(
                    "AccountManager: 已从 accounts.json.bak 恢复 {} 个账号（accounts.json 损坏）",
                    data.users.len()
                );
                let mgr = Self {
                    accounts_file: accounts_file.clone(),
                    data,
                };
                // 用 save_no_rolling 重写 accounts.json，
                // 避免把坏的 accounts.json 滚动成新 .bak 覆盖掉刚刚被验证为好的 .bak。
                if let Err(e) = mgr.save_no_rolling().await {
                    warn!(
                        "AccountManager: .bak 恢复后重写 accounts.json 失败: {}",
                        e
                    );
                }
                Ok(mgr)
            }
            Ok(None) => {
                warn!(
                    "AccountManager: accounts.json 损坏且 .bak 不存在 → 返回空数据（等待 session.json 迁移兜底）"
                );
                Ok(Self {
                    accounts_file,
                    data: AccountsData::default(),
                })
            }
            Err(e) => {
                bail!(
                    "AccountManager: accounts.json 与 accounts.json.bak 均损坏: bak err={e}"
                );
            }
        }
    }

    /// 内部 helper：读取并解析 JSON 文件。
    /// `Ok(None)` = 文件不存在；`Ok(Some(d))` = 解析成功；`Err(_)` = 文件存在但损坏。
    async fn read_json(path: &Path) -> Result<Option<AccountsData>> {
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(path)
            .await
            .with_context(|| format!("读取 {} 失败", path.display()))?;
        let data: AccountsData = serde_json::from_str(&content)
            .with_context(|| format!("反序列化 {} 失败", path.display()))?;
        Ok(Some(data))
    }

    /// 持久化到 `accounts.json`（原子写：先写 `.tmp` → 旧文件 rename 为 `.bak`
    /// → tmp rename 为 `accounts.json`）。
    ///
    /// 滚动备份契约 — 每次成功保存前把旧 `accounts.json` 复制为 `accounts.json.bak`，
    /// 作为下一次损坏的回退源。
    /// 临时文件名带 PID 避免多进程竞争（虽然当前只有一个进程）。
    pub async fn save(&self) -> Result<()> {
        self.save_inner(true).await
    }

    /// 保存但不滚动备份。**仅供从 `.bak` 恢复时使用** — 直接把当前内存数据
    /// 写到 `accounts.json`，跳过"复制旧 accounts.json → .bak"步骤。
    ///
    /// 背景：当 `load()` 探测到 `accounts.json` 损坏并
    /// 从 `accounts.json.bak` 恢复后，必须把内存数据回写到 `accounts.json`。
    /// 如果走普通 `save()`，第一步就是"删除旧 .bak、把当前 accounts.json 复制
    /// 成新 .bak"，但磁盘上的 `accounts.json` 此刻是已知损坏的 — 等于把好备份
    /// 替换成坏数据，下一次再损坏时就没有可用 .bak 了。
    pub async fn save_no_rolling(&self) -> Result<()> {
        self.save_inner(false).await
    }

    /// `save` / `save_no_rolling` 的共享实现。`roll_bak=true` 时执行
    /// 滚动备份步骤；`false` 时只写主文件（恢复路径专用）。
    async fn save_inner(&self, roll_bak: bool) -> Result<()> {
        // 确保父目录存在
        if let Some(parent) = self.accounts_file.parent() {
            fs::create_dir_all(parent).await.ok();
        }
        let tmp = self
            .accounts_file
            .with_extension(format!("json.tmp.{}", std::process::id()));
        let bak = self.accounts_file.with_extension("json.bak");

        let json =
            serde_json::to_string_pretty(&self.data).context("序列化 accounts.json 失败")?;

        // 1) 写临时文件
        fs::write(&tmp, &json)
            .await
            .with_context(|| format!("写入 {} 失败", tmp.display()))?;

        // 2) 滚动备份（仅 roll_bak=true 时执行）：把旧 accounts.json 复制
        //    （不是 rename，避免 rename 后再写 accounts.json 时若失败，磁盘上
        //     没有 accounts.json 也没有 .bak 的窗口期）
        if roll_bak && self.accounts_file.exists() {
            // copy 是原子可重试的；先删除旧 .bak（如果存在），再复制
            // 这里用 rename(old → bak) 比 copy 更原子，但要求 .bak 不存在；
            // Windows 上 rename 目标存在会失败，先尝试 remove。
            if bak.exists() {
                if let Err(e) = fs::remove_file(&bak).await {
                    warn!("AccountManager.save: 移除旧 .bak 失败（继续）: {}", e);
                }
            }
            // copy 兼容性最好（保留旧 accounts.json 直到 rename 成功）
            if let Err(e) = fs::copy(&self.accounts_file, &bak).await {
                warn!(
                    "AccountManager.save: 复制 accounts.json → .bak 失败（继续）: {}",
                    e
                );
            }
        }

        // 3) 原子 rename: tmp → accounts.json（覆盖式）
        // tokio::fs::rename 在大多数平台都是 atomic replace（包括 Windows MoveFileEx）
        fs::rename(&tmp, &self.accounts_file)
            .await
            .with_context(|| {
                format!(
                    "rename {} → {} 失败",
                    tmp.display(),
                    self.accounts_file.display()
                )
            })?;
        Ok(())
    }

    // ───────── CRUD ─────────

    /// 添加用户（已存在则更新凭证）。
    pub async fn add_user(&mut self, user: UserAuth) -> Result<()> {
        let uid = user.uid;
        if let Some(existing) = self.data.users.iter_mut().find(|u| u.uid == uid) {
            *existing = user;
            info!("AccountManager: 更新已有账号 uid={uid}");
        } else {
            info!("AccountManager: 新增账号 uid={uid}");
            self.data.users.push(user);
        }
        // 如果是第一个账号，自动设为活跃
        if self.data.active_uid.is_none() {
            self.data.active_uid = Some(uid);
            info!("AccountManager: 首个账号自动设为活跃 uid={uid}");
        }
        self.save().await
    }

    /// 设置活跃账号（持久化层写入）。
    ///
    /// `uid = None` 表示清除活跃账号。
    pub async fn set_active_persisted(&mut self, uid: Option<Uid>) -> Result<()> {
        let raw = uid.map(|u| u.raw());
        if let Some(r) = raw {
            if !self.data.users.iter().any(|u| u.uid == r) {
                bail!("set_active_persisted: uid={r} 不存在");
            }
        }
        self.data.active_uid = raw;
        self.save().await
    }

    /// 更新指定用户的 `custom_config`。
    ///
    /// 返回 `Ok(false)` 表示 `uid` 不存在；返回 `Ok(true)` 表示已更新并持久化。
    /// 不修改 `active_uid`，不更新凭证。BudgetScheduler 联动由调用方完成。
    pub async fn update_user_custom_config(
        &mut self,
        uid: Uid,
        custom_config: crate::auth::types::AccountConfig,
    ) -> Result<bool> {
        let raw = uid.raw();
        match self.data.users.iter_mut().find(|u| u.uid == raw) {
            Some(user) => {
                user.custom_config = custom_config;
                info!("AccountManager: 更新 custom_config uid={raw}");
                self.save().await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// 删除用户。如果被删除的是活跃账号，自动切到第一个剩余账号或 `None`。
    pub async fn delete_user(&mut self, uid: Uid) -> Result<()> {
        let raw = uid.raw();
        let before = self.data.users.len();
        self.data.users.retain(|u| u.uid != raw);
        if self.data.users.len() == before {
            bail!("delete_user: uid={raw} 不存在");
        }
        // 如果删除的是活跃账号 → 切到 first 或 None
        if self.data.active_uid == Some(raw) {
            self.data.active_uid = self.data.users.first().map(|u| u.uid);
            info!(
                "AccountManager: 活跃账号被删除，自动切换到 {:?}",
                self.data.active_uid
            );
        }
        self.save().await
    }

    /// 获取指定用户。
    pub fn get_user(&self, uid: Uid) -> Option<&UserAuth> {
        self.data.users.iter().find(|u| u.uid == uid.raw())
    }

    /// 所有已登录用户列表。
    pub fn list_users(&self) -> &[UserAuth] {
        &self.data.users
    }

    /// 返回脱敏摘要列表（前端展示用）。
    pub fn list_accounts(&self) -> Vec<AccountSummary> {
        self.data
            .users
            .iter()
            .map(|u| AccountSummary::from_user(u, self.data.active_uid))
            .collect()
    }

    /// 当前活跃账号 UID。
    pub fn active_uid(&self) -> Option<Uid> {
        self.data.active_uid.map(Uid::new)
    }

    /// 内部数据引用（仅供迁移 / 测试）。
    pub fn data(&self) -> &AccountsData {
        &self.data
    }

    // ───────── 旧数据迁移 ─────────

    /// 启动加载主入口：完整 4 级回退链路决议账号数据：
    ///
    /// ```text
    ///   accounts.json
    ///     ↓ 不存在 / 损坏
    ///   accounts.json.bak
    ///     ↓ 不存在 / 损坏
    ///   session.json（旧单账号数据）
    ///     ↓ 不存在 / 损坏
    ///   session.json.migrated（历史快照，回退最后兜底）
    ///     ↓ 不存在 / 损坏
    ///   空账号列表（用户需重新登录）
    /// ```
    ///
    /// 本方法保留 `migrate_from_session` 命名只为向后兼容；实际语义是"加载或迁移"。
    pub async fn migrate_from_session(session_path: &Path, accounts_file: &Path) -> Result<Self> {
        info!(
            "AccountManager: 加载/迁移账号数据 accounts={:?} session={:?}",
            accounts_file, session_path
        );

        let bak_file = accounts_file.with_extension("json.bak");
        let migrated_session = session_path.with_extension("json.migrated");

        // 第 1 级：accounts.json
        match Self::read_json(accounts_file).await {
            Ok(Some(data)) => {
                info!(
                    "AccountManager 加载完成：{} 个账号，active_uid={:?}",
                    data.users.len(),
                    data.active_uid
                );
                return Ok(Self {
                    accounts_file: accounts_file.to_path_buf(),
                    data,
                });
            }
            Ok(None) => {
                info!("AccountManager: accounts.json 不存在，尝试 .bak / session.json 回退链路");
            }
            Err(e) => {
                warn!(
                    "AccountManager: accounts.json 损坏（{}），尝试 .bak / session.json 回退链路",
                    e
                );
            }
        }

        // 第 2 级：accounts.json.bak
        match Self::read_json(&bak_file).await {
            Ok(Some(data)) => {
                warn!(
                    "AccountManager: 已从 accounts.json.bak 恢复 {} 个账号",
                    data.users.len()
                );
                let mgr = Self {
                    accounts_file: accounts_file.to_path_buf(),
                    data,
                };
                // 用 save_no_rolling 重写主文件，
                // 避免把损坏的 accounts.json 滚成新 .bak 覆盖好备份。
                if let Err(e) = mgr.save_no_rolling().await {
                    warn!(
                        "AccountManager: .bak 恢复后重写 accounts.json 失败: {}",
                        e
                    );
                }
                return Ok(mgr);
            }
            Ok(None) => {
                info!(
                    "AccountManager: accounts.json.bak 不存在，尝试 session.json 回退链路"
                );
            }
            Err(e) => {
                warn!(
                    "AccountManager: accounts.json.bak 损坏（{}），尝试 session.json 回退链路",
                    e
                );
            }
        }

        // 第 3 级：session.json
        if let Some(mgr) = Self::try_migrate_from_legacy(session_path, accounts_file, true).await? {
            return Ok(mgr);
        }

        // 第 4 级：session.json.migrated（历史快照兜底）
        if let Some(mgr) =
            Self::try_migrate_from_legacy(&migrated_session, accounts_file, false).await?
        {
            warn!(
                "AccountManager: 已从 session.json.migrated 历史快照恢复（最后兜底）"
            );
            return Ok(mgr);
        }

        // 第 5 级：所有源都不可用 → 空数据，等待用户重新登录
        warn!(
            "AccountManager: accounts.json / .bak / session.json / .migrated 均不可用 → 返回空账号列表"
        );
        Ok(Self {
            accounts_file: accounts_file.to_path_buf(),
            data: AccountsData::default(),
        })
    }

    /// 尝试从一个 legacy 单账号文件（`session.json` 或 `session.json.migrated`）
    /// 迁移到 `accounts.json`。
    ///
    /// `rename_after_migrate=true` 时（标准 `session.json` 路径），迁移成功后把
    /// 源文件改名为 `session.json.migrated`（已存在则保留旧 `.migrated` 不动）。
    /// `false` 时（已经是 `.migrated`），保留原文件不动。
    ///
    /// 返回 `Ok(None)` 表示源文件不存在或解析失败（调用方继续走下一级回退）。
    async fn try_migrate_from_legacy(
        legacy_path: &Path,
        accounts_file: &Path,
        rename_after_migrate: bool,
    ) -> Result<Option<Self>> {
        if !legacy_path.exists() {
            return Ok(None);
        }
        let content = match fs::read_to_string(legacy_path).await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "AccountManager: 读取 {} 失败（{}），跳过此级回退",
                    legacy_path.display(),
                    e
                );
                return Ok(None);
            }
        };
        let user: UserAuth = match serde_json::from_str(&content) {
            Ok(u) => u,
            Err(e) => {
                warn!(
                    "AccountManager: 反序列化 {} 失败（{}），跳过此级回退",
                    legacy_path.display(),
                    e
                );
                return Ok(None);
            }
        };

        let uid = user.uid;
        let data = AccountsData {
            active_uid: Some(uid),
            users: vec![user],
        };

        // 确保目标目录
        if let Some(parent) = accounts_file.parent() {
            fs::create_dir_all(parent).await.ok();
        }

        // 走标准持久化路径写入（不滚动 .bak — 此时 accounts.json 可能损坏或不存在）
        let mgr = Self {
            accounts_file: accounts_file.to_path_buf(),
            data,
        };
        mgr.save_no_rolling()
            .await
            .with_context(|| format!("迁移写入 {} 失败", accounts_file.display()))?;

        // session.json 路径：迁移后改名（标准 session.json → .migrated）
        if rename_after_migrate {
            let migrated = legacy_path.with_extension("json.migrated");
            if migrated.exists() {
                info!(
                    "AccountManager: {} 已存在 → 不覆盖；保留 {} 供本次启动后处理",
                    migrated.display(),
                    legacy_path.display()
                );
            } else if let Err(e) = fs::rename(legacy_path, &migrated).await {
                warn!(
                    "AccountManager: rename {} → {} 失败（{}）；保留原文件",
                    legacy_path.display(),
                    migrated.display(),
                    e
                );
            } else {
                info!(
                    "AccountManager: 迁移完成 uid={uid}，旧文件已改名为 {:?}",
                    migrated
                );
            }
        } else {
            info!(
                "AccountManager: 已从 {} 恢复 uid={}（保留原文件不变）",
                legacy_path.display(),
                uid
            );
        }

        Ok(Some(mgr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_crud_roundtrip() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");

        let mut mgr = AccountManager::load(&file).await.unwrap();
        assert!(mgr.list_users().is_empty());
        assert!(mgr.active_uid().is_none());

        // 添加第一个用户 → 自动成为活跃
        let u1 = UserAuth::new(100, "alice".into(), "bduss_a".into());
        mgr.add_user(u1).await.unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(100)));
        assert_eq!(mgr.list_users().len(), 1);

        // 添加第二个用户 → 活跃不变
        let u2 = UserAuth::new(200, "bob".into(), "bduss_b".into());
        mgr.add_user(u2).await.unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(100)));
        assert_eq!(mgr.list_users().len(), 2);

        // 切换活跃
        mgr.set_active_persisted(Some(Uid::new(200))).await.unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(200)));

        // 持久化往返
        let mgr2 = AccountManager::load(&file).await.unwrap();
        assert_eq!(mgr2.active_uid(), Some(Uid::new(200)));
        assert_eq!(mgr2.list_users().len(), 2);

        // 删除活跃账号 → 自动切到第一个
        let mut mgr2 = mgr2;
        mgr2.delete_user(Uid::new(200)).await.unwrap();
        assert_eq!(mgr2.active_uid(), Some(Uid::new(100)));
        assert_eq!(mgr2.list_users().len(), 1);

        // 删除最后一个
        mgr2.delete_user(Uid::new(100)).await.unwrap();
        assert!(mgr2.active_uid().is_none());
        assert!(mgr2.list_users().is_empty());
    }

    #[tokio::test]
    async fn test_update_user_custom_config() {
        use crate::auth::types::{AccountConfig, AccountDownloadConfig, AccountUploadConfig};

        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");
        let mut mgr = AccountManager::load(&file).await.unwrap();
        mgr.add_user(UserAuth::new(7, "x".into(), "b".into()))
            .await
            .unwrap();

        // 不存在的 uid 返回 false
        let cc = AccountConfig::default();
        let r = mgr.update_user_custom_config(Uid::new(8), cc.clone()).await.unwrap();
        assert!(!r);

        // 存在的 uid 更新成功并落盘
        let mut new_cc = AccountConfig {
            auto_apply_recommended: false,
            download: AccountDownloadConfig {
                max_global_threads: 4,
                ..Default::default()
            },
            upload: AccountUploadConfig::default(),
        };
        new_cc.upload.max_global_threads = 6;
        let r = mgr.update_user_custom_config(Uid::new(7), new_cc.clone()).await.unwrap();
        assert!(r);

        // 重新 load 验证持久化
        let mgr2 = AccountManager::load(&file).await.unwrap();
        let user = mgr2.get_user(Uid::new(7)).unwrap();
        assert!(!user.custom_config.auto_apply_recommended);
        assert_eq!(user.custom_config.download.max_global_threads, 4);
        assert_eq!(user.custom_config.upload.max_global_threads, 6);
    }

    #[tokio::test]
    async fn test_set_active_nonexistent() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");
        let mut mgr = AccountManager::load(&file).await.unwrap();
        let result = mgr.set_active_persisted(Some(Uid::new(999))).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_migrate_from_session() {
        let dir = tempdir().unwrap();
        let session_file = dir.path().join("session.json");
        let accounts_file = dir.path().join("accounts.json");

        // 写一个旧 session.json
        let user = UserAuth::new(42, "legacy".into(), "bduss_legacy".into());
        let json = serde_json::to_string_pretty(&user).unwrap();
        fs::write(&session_file, &json).await.unwrap();

        // 迁移
        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();

        // 验证
        assert_eq!(mgr.active_uid(), Some(Uid::new(42)));
        assert_eq!(mgr.list_users().len(), 1);
        assert_eq!(mgr.list_users()[0].username, "legacy");

        // session.json 应已不存在，改名为 .migrated
        assert!(!session_file.exists());
        assert!(session_file.with_extension("json.migrated").exists());

        // accounts.json 应存在
        assert!(accounts_file.exists());
    }

    #[tokio::test]
    async fn test_migrate_skip_if_accounts_exists() {
        let dir = tempdir().unwrap();
        let session_file = dir.path().join("session.json");
        let accounts_file = dir.path().join("accounts.json");

        // 先写两个文件
        let user = UserAuth::new(42, "legacy".into(), "bduss".into());
        fs::write(&session_file, serde_json::to_string(&user).unwrap())
            .await
            .unwrap();
        let existing = AccountsData {
            active_uid: Some(99),
            users: vec![UserAuth::new(99, "existing".into(), "bduss99".into())],
        };
        fs::write(
            &accounts_file,
            serde_json::to_string_pretty(&existing).unwrap(),
        )
        .await
        .unwrap();

        // 迁移应跳过
        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(99)));
        // session.json 应保持不动
        assert!(session_file.exists());
    }

    #[test]
    fn test_list_accounts_summary() {
        let data = AccountsData {
            active_uid: Some(1),
            users: vec![
                UserAuth::new(1, "alice".into(), "b1".into()),
                UserAuth::new(2, "bob".into(), "b2".into()),
            ],
        };
        let mgr = AccountManager {
            accounts_file: PathBuf::from("dummy"),
            data,
        };
        let summaries = mgr.list_accounts();
        assert_eq!(summaries.len(), 2);
        assert!(summaries[0].is_active);
        assert!(!summaries[1].is_active);
    }

    // accounts.json 损坏 + .bak 回退
    #[tokio::test]
    async fn test_load_corrupt_accounts_falls_back_to_bak() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");
        let bak = dir.path().join("accounts.json.bak");

        // .bak 是合法数据
        let good = AccountsData {
            active_uid: Some(11),
            users: vec![UserAuth::new(11, "from-bak".into(), "b11".into())],
        };
        fs::write(&bak, serde_json::to_string_pretty(&good).unwrap())
            .await
            .unwrap();

        // accounts.json 是损坏的（非合法 JSON）
        fs::write(&file, "{{ this is not json")
            .await
            .unwrap();

        let mgr = AccountManager::load(&file).await.unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(11)));
        assert_eq!(mgr.list_users().len(), 1);
        assert_eq!(mgr.list_users()[0].username, "from-bak");

        // 重写 accounts.json：从 .bak 恢复后必须把磁盘上的 accounts.json 修好
        let on_disk = fs::read_to_string(&file).await.unwrap();
        let parsed: AccountsData = serde_json::from_str(&on_disk).unwrap();
        assert_eq!(parsed.active_uid, Some(11));
    }

    // load() 单独调用：accounts.json 损坏且 .bak 不存在 → 空数据
    // 注意：load() 不读 session.json — 完整 4 级回退链路在 migrate_from_session 内编排。
    // 这只验证 load() 自身的兜底行为；用户级回退看 test_full_fallback_chain_*。
    #[tokio::test]
    async fn test_load_corrupt_accounts_no_bak_returns_empty() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");
        fs::write(&file, "garbage").await.unwrap();

        let mgr = AccountManager::load(&file).await.unwrap();
        assert!(mgr.list_users().is_empty());
        assert!(mgr.active_uid().is_none());
    }

    // 完整 4 级回退链路 — accounts.json 不存在 + 有效 session.json
    // → 必须迁移 session.json，不能返回空账号
    #[tokio::test]
    async fn test_full_fallback_chain_no_accounts_with_session() {
        let dir = tempdir().unwrap();
        let accounts_file = dir.path().join("accounts.json");
        let session_file = dir.path().join("session.json");

        let user = UserAuth::new(31, "from-session".into(), "bduss_31".into());
        fs::write(&session_file, serde_json::to_string_pretty(&user).unwrap())
            .await
            .unwrap();

        // 用 migrate_from_session（统一入口）
        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(31)));
        assert_eq!(mgr.list_users().len(), 1);
        assert_eq!(mgr.list_users()[0].username, "from-session");
        assert!(accounts_file.exists());
        // session.json 已改名为 .migrated
        assert!(!session_file.exists());
        assert!(session_file.with_extension("json.migrated").exists());
    }

    // accounts.json 损坏 + .bak 不存在 + 有效 session.json
    // → 应继续回退到 session.json
    #[tokio::test]
    async fn test_full_fallback_chain_corrupt_accounts_no_bak_with_session() {
        let dir = tempdir().unwrap();
        let accounts_file = dir.path().join("accounts.json");
        let session_file = dir.path().join("session.json");

        fs::write(&accounts_file, "garbage data").await.unwrap();
        let user = UserAuth::new(57, "rescued".into(), "bduss_57".into());
        fs::write(&session_file, serde_json::to_string_pretty(&user).unwrap())
            .await
            .unwrap();

        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(57)));
        assert_eq!(mgr.list_users()[0].username, "rescued");
    }

    // accounts.json 损坏 + .bak 损坏 + 有效 session.json
    // → 应继续回退到 session.json
    #[tokio::test]
    async fn test_full_fallback_chain_corrupt_accounts_corrupt_bak_with_session() {
        let dir = tempdir().unwrap();
        let accounts_file = dir.path().join("accounts.json");
        let bak_file = dir.path().join("accounts.json.bak");
        let session_file = dir.path().join("session.json");

        fs::write(&accounts_file, "garbage").await.unwrap();
        fs::write(&bak_file, "also garbage").await.unwrap();
        let user = UserAuth::new(91, "rescued-2".into(), "bduss_91".into());
        fs::write(&session_file, serde_json::to_string_pretty(&user).unwrap())
            .await
            .unwrap();

        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(91)));
        assert_eq!(mgr.list_users()[0].username, "rescued-2");
    }

    // accounts.json 不存在 + session.json 不存在 + 有效 .migrated
    // → 应回退到 .migrated 历史快照（最后兜底）
    #[tokio::test]
    async fn test_full_fallback_chain_only_migrated_remaining() {
        let dir = tempdir().unwrap();
        let accounts_file = dir.path().join("accounts.json");
        let session_file = dir.path().join("session.json");
        let migrated_file = dir.path().join("session.json.migrated");

        let user = UserAuth::new(123, "from-migrated".into(), "bduss_123".into());
        fs::write(&migrated_file, serde_json::to_string_pretty(&user).unwrap())
            .await
            .unwrap();

        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(123)));
        assert_eq!(mgr.list_users()[0].username, "from-migrated");

        // .migrated 应该保持不变（不重命名）
        assert!(migrated_file.exists());

        // accounts.json 应当生成
        assert!(accounts_file.exists());
    }

    // 所有源都不可用 → 返回空账号列表（用户需重新登录）
    #[tokio::test]
    async fn test_full_fallback_chain_all_sources_missing() {
        let dir = tempdir().unwrap();
        let accounts_file = dir.path().join("accounts.json");
        let session_file = dir.path().join("session.json");

        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert!(mgr.list_users().is_empty());
        assert!(mgr.active_uid().is_none());
    }

    // 从 .bak 恢复时必须用 save_no_rolling，
    // 不能把损坏的 accounts.json 滚成新 .bak 覆盖好备份
    #[tokio::test]
    async fn test_recovery_from_bak_does_not_overwrite_good_bak() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");
        let bak = dir.path().join("accounts.json.bak");

        // .bak 是有效数据
        let good = AccountsData {
            active_uid: Some(77),
            users: vec![UserAuth::new(77, "good-bak".into(), "b77".into())],
        };
        let good_json = serde_json::to_string_pretty(&good).unwrap();
        fs::write(&bak, &good_json).await.unwrap();

        // accounts.json 损坏
        fs::write(&file, "corrupted!!!").await.unwrap();

        // load 触发恢复路径
        let _mgr = AccountManager::load(&file).await.unwrap();

        // .bak 应当保持不变（不能被损坏的 accounts.json 滚成新 .bak）
        let bak_after = fs::read_to_string(&bak).await.unwrap();
        assert_eq!(
            bak_after, good_json,
            ".bak 在恢复路径里被错误覆盖了"
        );

        // accounts.json 应当被修好（写入 .bak 内容）
        let main_after = fs::read_to_string(&file).await.unwrap();
        let parsed: AccountsData = serde_json::from_str(&main_after).unwrap();
        assert_eq!(parsed.active_uid, Some(77));
    }

    // save 必须先复制旧 accounts.json 为 .bak
    #[tokio::test]
    async fn test_save_creates_rolling_bak() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("accounts.json");
        let bak = dir.path().join("accounts.json.bak");

        let mut mgr = AccountManager::load(&file).await.unwrap();
        mgr.add_user(UserAuth::new(1, "v1".into(), "b1".into()))
            .await
            .unwrap();
        // 第一次保存：accounts.json 不存在 → 不应该有 .bak
        assert!(file.exists());
        assert!(!bak.exists());

        // 再保存一次：旧 accounts.json 应该被复制成 .bak
        mgr.add_user(UserAuth::new(2, "v2".into(), "b2".into()))
            .await
            .unwrap();
        assert!(file.exists());
        assert!(bak.exists());

        // .bak 应当能反序列化（合法 JSON）
        let bak_content = fs::read_to_string(&bak).await.unwrap();
        let _: AccountsData = serde_json::from_str(&bak_content).unwrap();
    }

    // session.json + session.json.migrated 同时存在 → 不报错且不覆盖 .migrated
    #[tokio::test]
    async fn test_migrate_idempotent_when_migrated_exists() {
        let dir = tempdir().unwrap();
        let session_file = dir.path().join("session.json");
        let migrated_file = dir.path().join("session.json.migrated");
        let accounts_file = dir.path().join("accounts.json");

        // 旧 session.json（要迁移的当前数据）
        let user_curr = UserAuth::new(42, "current".into(), "bduss_curr".into());
        fs::write(&session_file, serde_json::to_string_pretty(&user_curr).unwrap())
            .await
            .unwrap();

        // 已存在的 .migrated（历史快照，必须保留）
        let user_old = UserAuth::new(7, "ancient".into(), "bduss_old".into());
        let migrated_content = serde_json::to_string_pretty(&user_old).unwrap();
        fs::write(&migrated_file, &migrated_content).await.unwrap();

        // 迁移：必须不报错
        let mgr = AccountManager::migrate_from_session(&session_file, &accounts_file)
            .await
            .unwrap();
        assert_eq!(mgr.active_uid(), Some(Uid::new(42)));

        // .migrated 内容应当未被覆盖（保留 ancient 用户）
        let still_there = fs::read_to_string(&migrated_file).await.unwrap();
        assert_eq!(still_there, migrated_content);

        // session.json 应当保留（任务清单要求）
        assert!(session_file.exists());

        // accounts.json 应当生成
        assert!(accounts_file.exists());
    }
}
