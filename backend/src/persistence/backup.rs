//! 迁移前完整备份
//!
//! 在**任何破坏性迁移操作之前**（`session.json → session.json.migrated` 改名、
//! `accounts.json` 写入、SQLite `ALTER` / 历史回填、`.meta` 回写、
//! `autobackup_configs.json` 回写）把关键数据完整复制到
//! `config/backups/pre_migration_<时间戳>/`，使得万一迁移失败用户能整目录还原。
//!
//! 设计约束：
//!   - 备份**内容必须完整**：覆盖下方 [`PRE_MIGRATION_FILES`] 清单 + 真实 DB 路径
//!     （含 `-wal` / `-shm` 边车文件）+ 整个 `wal_dir` 目录。
//!   - DB 路径**按 `AppConfig.persistence.db_path` 取真实值**，不写死。
//!   - 备份**失败仅告警、不阻塞启动**（不抛错），避免把可恢复的小问题升级成启动失败；
//!     真正的破坏性写入若随后失败，会另行进入只读保护模式。

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// 迁移前需要备份的 `config/` 下单文件清单（相对 `config_dir`，不存在则跳过）。
pub const PRE_MIGRATION_FILES: &[&str] = &[
    "app.toml",
    "session.json",
    "session.json.migrated",
    "accounts.json",
    "accounts.json.bak",
    "autobackup_configs.json",
];

/// 备份完成标记文件名。**仅当备份成功（实际复制了内容）后**才写入；
/// 据此判断一个 `pre_migration_*` 目录是否为**有效**（完整）备份。
const BACKUP_MANIFEST: &str = "backup_manifest.json";

/// 是否需要创建迁移前备份。
///
/// 采用「**本数据目录是否已被本类备份覆盖过一次**」的稳妥判据，而不是去枚举
/// 具体哪一步迁移（schema / `.meta` / 历史回填）待执行 —— 后者容易漏判（例如
/// `accounts.json` 与 `backup_configs.owner_uid` 都已就绪，但 `.meta` 或历史表
/// 仍需回填时，窄判据会跳过备份后又写盘）。
///
/// 返回 `true` 当且仅当：
///   1. `config/backups/` 下尚无任何**有效**的 `pre_migration_*` 备份（即带
///      [`BACKUP_MANIFEST`] 完成标记的目录）；**且**
///   2. 存在升级前的既有数据（`db_path` / `session.json` / `accounts.json`
///      任一存在）—— 全新安装无需备份。
///
/// 这样可保证：升级用户在**任何破坏性迁移发生前**恰好备份一次；之后每次启动
/// 因已存在**有效**备份而跳过，不会堆积。
///
/// 注意「**有效**」：只创建了目录但复制失败 / 不完整（未写入 manifest）的残缺
/// 目录**不算**已备份，下次启动会重新备份，避免「以为有备份其实是空壳」。
pub fn needs_pre_migration_backup(
    config_dir: &Path,
    db_path: &Path,
    session_path: &Path,
    accounts_path: &Path,
) -> bool {
    if valid_pre_migration_backup_exists(config_dir) {
        return false;
    }
    db_path.exists() || session_path.exists() || accounts_path.exists()
}

/// 探测 `config/backups/` 下是否已存在**有效**的 `pre_migration_*` 备份目录。
///
/// 「有效」= 目录名以 `pre_migration_` 开头 **且** 目录内存在
/// [`BACKUP_MANIFEST`] 完成标记，**且** manifest 声明的 `required` 关键文件
/// 都确实存在于该备份目录（见 [`manifest_required_satisfied`]）。
fn valid_pre_migration_backup_exists(config_dir: &Path) -> bool {
    let backups = config_dir.join("backups");
    match std::fs::read_dir(&backups) {
        Ok(rd) => rd.filter_map(|e| e.ok()).any(|e| {
            let dir = e.path();
            e.file_name()
                .to_string_lossy()
                .starts_with("pre_migration_")
                && dir.join(BACKUP_MANIFEST).exists()
                && manifest_required_satisfied(&dir)
        }),
        Err(_) => false,
    }
}

/// 校验备份目录的 manifest 是否表明这是一份有效（完整）备份。
///
/// 语义（manifest 作为完成标记应当可靠，故对损坏/半写入从严）：
/// - manifest **读取失败 / JSON 解析失败** → `false`：损坏或半写入的标记不算有效备份。
/// - manifest 存在但**不含 `required` 字段**（如**旧版**备份）→ `true`：按「存在即
///   有效」的旧口径处理，**向后兼容**、不让既有备份失效。
/// - 含 `required` 数组 → 逐项确认对应文件/目录在备份目录内存在；任一缺失 → `false`。
/// - 另对 wal 目录做内容齐全校验：优先按 `wal_files`（相对路径清单）逐项核对，旧
///   manifest 仅有 `wal_file_count` 时回退到数量校验，两者皆无则跳过。
fn manifest_required_satisfied(backup_dir: &Path) -> bool {
    let raw = match std::fs::read_to_string(backup_dir.join(BACKUP_MANIFEST)) {
        Ok(s) => s,
        Err(_) => return false, // 读不出 manifest（损坏/权限）：不算有效
    };
    let json: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return false, // JSON 解析失败（半写入/损坏）：不算有效
    };
    // 1) required 列出的关键文件 / 目录必须都存在
    let required_ok = match json.get("required").and_then(|v| v.as_array()) {
        None => true, // 旧 manifest 无 required：向后兼容
        Some(items) => items
            .iter()
            .filter_map(|v| v.as_str())
            .all(|name| backup_dir.join(name).exists()),
    };
    if !required_ok {
        return false;
    }

    // 2) wal 目录内容齐全校验。优先用 `wal_files` 逐项核对（既证明数量、也证明每个
    //    具体文件都在，可识别「缺 a.meta、多 b.meta、数量却不变」）；旧 manifest 无
    //    `wal_files` 时回退到 `wal_file_count` 数量校验（#10）；两者都无则跳过。
    let wal_name = json.get("wal_backup_dir").and_then(|v| v.as_str());
    if let Some(name) = wal_name {
        let wal_root = backup_dir.join(name);
        match json.get("wal_files").and_then(|v| v.as_array()) {
            Some(files) => {
                let all_present = files
                    .iter()
                    .filter_map(|v| v.as_str())
                    .all(|rel| wal_root.join(rel).is_file());
                if !all_present {
                    return false;
                }
            }
            None => {
                if let Some(expect) = json.get("wal_file_count").and_then(|v| v.as_u64()) {
                    if (count_files_recursive(&wal_root) as u64) < expect {
                        return false;
                    }
                }
            }
        }
    }
    true
}

/// 递归统计目录下的普通文件数（出错按 0 计）。用于兼容旧 manifest 的 `wal_file_count`
/// 数量校验。
fn count_files_recursive(dir: &Path) -> usize {
    let mut count = 0usize;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        let rd = match std::fs::read_dir(&cur) {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        for entry in rd.filter_map(|e| e.ok()) {
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => stack.push(entry.path()),
                Ok(_) => count += 1,
                Err(_) => {}
            }
        }
    }
    count
}

/// 一次迁移前备份的结果。
#[derive(Debug, Clone)]
pub struct PreMigrationBackup {
    /// 备份目录（`config/backups/pre_migration_<时间戳>/`）。
    pub dir: PathBuf,
    /// 已成功复制的条目（文件 / 目录）描述列表。
    pub copied: Vec<String>,
}

/// 在任何破坏性迁移前创建一份完整备份。
///
/// * `config_dir` —— 配置目录（通常为 `config`）。
/// * `db_path` —— **真实** SQLite 路径（来自 `AppConfig.persistence.db_path`）。
/// * `wal_dir` —— WAL / `.meta` 目录。
///
/// 单文件 / 子目录复制失败仅 `warn!` 跳过（best-effort）；但若**一项都没复制成功**
/// 或**完成标记（manifest）写入失败**，则返回 `Err`（不写 manifest，目录不算有效
/// 备份，下次启动重做）。调用方对 `Err` 仅告警、不阻塞启动。
pub async fn create_pre_migration_backup(
    config_dir: &Path,
    db_path: &Path,
    wal_dir: &Path,
) -> Result<PreMigrationBackup> {
    let ts = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    let backup_dir = config_dir
        .join("backups")
        .join(format!("pre_migration_{ts}"));
    tokio::fs::create_dir_all(&backup_dir)
        .await
        .with_context(|| format!("创建迁移前备份目录失败: {}", backup_dir.display()))?;

    let mut copied: Vec<String> = Vec::new();
    // `required`：本次按既有数据**应当**出现在备份目录里的关键文件（以在备份目录中
    // 的相对名记录）。启动时据此严格校验有效性——这些文件若因复制失败缺失，则该备份
    // 视为无效并重做。只把**源端确实存在**的关键文件计入，避免要求本就不存在的文件。
    let mut required: Vec<String> = Vec::new();

    // 1) config/ 下单文件清单
    for name in PRE_MIGRATION_FILES {
        let src = config_dir.join(name);
        copy_file_best_effort(&src, &backup_dir.join(name), &mut copied).await;
    }
    // session.json / accounts.json 属关键数据，源端存在则计入 required
    for key in ["session.json", "accounts.json"] {
        if config_dir.join(key).exists() {
            required.push(key.to_string());
        }
    }

    // 2) 真实 DB 路径 + WAL/SHM 边车文件
    if let Some(db_name) = db_path.file_name() {
        copy_file_best_effort(db_path, &backup_dir.join(db_name), &mut copied).await;
        // 真实 DB 文件若源端存在则属关键文件，计入 required
        if db_path.exists() {
            required.push(db_name.to_string_lossy().into_owned());
        }
        // `-wal`（可能含尚未 checkpoint 的数据）/ `-shm` 边车：源端存在则也计入 required
        for ext in ["-wal", "-shm"] {
            let side = sidecar_path(db_path, ext);
            if let Some(side_name) = side.file_name() {
                copy_file_best_effort(&side, &backup_dir.join(side_name), &mut copied).await;
                if side.exists() {
                    required.push(side_name.to_string_lossy().into_owned());
                }
            }
        }
    } else {
        warn!("迁移前备份：db_path 无文件名，跳过 DB 复制: {}", db_path.display());
    }

    // 3) 整个 wal_dir 目录（任务 `.meta` 状态）：源端存在则计入 required。
    //    内容**必须完整复制**——若有任一文件复制 / 读取 / 遍历失败，则该目录备份不
    //    可信，视为整体备份失败（不写 manifest、返回 Err），下次启动重做；仅校验目录
    //    存在不足以保证内容齐全。完整复制时把每个文件的相对路径记入 `wal_files`，启
    //    动时据此**逐项**核对备份目录内对应文件是否存在，防止备份后内容被删 / 替换。
    let mut wal_files: Option<Vec<String>> = None;
    let mut wal_backup_name: Option<String> = None;
    if wal_dir.exists() {
        let dest_name = wal_dir
            .file_name()
            .map(|n| n.to_owned())
            .unwrap_or_else(|| std::ffi::OsString::from("wal"));
        let dest = backup_dir.join(&dest_name);
        match copy_dir_recursive(wal_dir, &dest).await {
            Ok(stats) if stats.failed == 0 => {
                copied.push(format!("{} (目录, {} 个文件)", dest.display(), stats.copied()));
                let name = dest_name.to_string_lossy().into_owned();
                required.push(name.clone());
                wal_backup_name = Some(name);
                wal_files = Some(stats.files);
            }
            Ok(stats) => {
                anyhow::bail!(
                    "迁移前备份：wal 目录 {} 复制不完整（成功 {} / 失败 {}），不写入完成标记",
                    wal_dir.display(),
                    stats.copied(),
                    stats.failed
                );
            }
            Err(e) => anyhow::bail!("迁移前备份：复制 wal 目录 {} 失败（{}）", wal_dir.display(), e),
        }
    }

    // 复制结果为空（既有数据应被复制却一项都没成功）→ 视为**失败**：不写
    // manifest，使该目录不被认作有效备份，下次启动会重做。调用方据 Err 告警。
    if copied.is_empty() {
        anyhow::bail!(
            "迁移前备份未复制任何内容（目标 {}），不写入完成标记",
            backup_dir.display()
        );
    }

    // 写入完成标记（manifest）。**必须是最后一步**——只有它存在才算有效备份。
    let manifest = serde_json::json!({
        "completed_at": chrono::Local::now().to_rfc3339(),
        "db_path": db_path.display().to_string(),
        "wal_dir": wal_dir.display().to_string(),
        "required": required,
        // 备份目录内 wal 子目录名与其下文件相对路径清单；启动校验时据此逐项核对内容齐全。
        "wal_backup_dir": wal_backup_name,
        "wal_files": wal_files,
        "copied": copied,
    });
    let manifest_str = serde_json::to_string_pretty(&manifest)
        .unwrap_or_else(|_| "{\"completed\":true}".to_string());
    tokio::fs::write(backup_dir.join(BACKUP_MANIFEST), manifest_str)
        .await
        .with_context(|| {
            format!(
                "写入迁移前备份完成标记失败: {}",
                backup_dir.join(BACKUP_MANIFEST).display()
            )
        })?;

    info!(
        "迁移前备份完成 → {} （共 {} 项，已写入 {}）",
        backup_dir.display(),
        copied.len(),
        BACKUP_MANIFEST
    );
    Ok(PreMigrationBackup {
        dir: backup_dir,
        copied,
    })
}

/// 在 `db_path` 同名基础上拼接边车后缀（`-wal` / `-shm`）。
fn sidecar_path(db_path: &Path, ext: &str) -> PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push(ext);
    PathBuf::from(s)
}

/// 复制单个文件；源不存在则静默跳过，失败仅告警。成功则把目标路径加入 `copied`。
async fn copy_file_best_effort(src: &Path, dest: &Path, copied: &mut Vec<String>) {
    if !src.exists() {
        return;
    }
    if let Some(parent) = dest.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            warn!("迁移前备份：创建目录 {} 失败（{}）", parent.display(), e);
            return;
        }
    }
    match tokio::fs::copy(src, dest).await {
        Ok(_) => copied.push(dest.display().to_string()),
        Err(e) => warn!("迁移前备份：复制 {} 失败（{}）", src.display(), e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backup_copies_config_files_db_and_wal_dir() {
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        let wal_dir = root.path().join("wal");
        tokio::fs::create_dir_all(&config_dir).await.unwrap();
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();

        // 真实 DB 路径在 config 下，且带 -wal 边车文件
        let db_path = config_dir.join("baidu-pcs.db");
        tokio::fs::write(&db_path, b"db").await.unwrap();
        tokio::fs::write(config_dir.join("baidu-pcs.db-wal"), b"wal")
            .await
            .unwrap();
        // 清单中的部分文件存在、部分缺失（缺失应被静默跳过）
        tokio::fs::write(config_dir.join("app.toml"), b"x")
            .await
            .unwrap();
        tokio::fs::write(config_dir.join("accounts.json"), b"x")
            .await
            .unwrap();
        // session.json 不存在 → 跳过
        // wal 目录内放一个 .meta
        tokio::fs::write(wal_dir.join("task1.meta"), b"m")
            .await
            .unwrap();

        let out = create_pre_migration_backup(&config_dir, &db_path, &wal_dir)
            .await
            .unwrap();

        assert!(out.dir.exists());
        assert!(out.dir.join("app.toml").exists());
        assert!(out.dir.join("accounts.json").exists());
        assert!(out.dir.join("baidu-pcs.db").exists());
        assert!(out.dir.join("baidu-pcs.db-wal").exists());
        assert!(!out.dir.join("session.json").exists(), "缺失文件不应被创建");
        // 整个 wal 目录被复制
        assert!(out.dir.join("wal").join("task1.meta").exists());
    }

    #[test]
    fn needs_backup_true_for_existing_data_then_false_after_backup_exists() {
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db_path = config_dir.join("baidu-pcs.db");
        let session_path = config_dir.join("session.json");
        let accounts_path = config_dir.join("accounts.json");

        // 全新安装：无既有数据 → 不备份
        assert!(!needs_pre_migration_backup(
            &config_dir,
            &db_path,
            &session_path,
            &accounts_path
        ));

        // 升级用户：存在既有数据（任一即可）→ 需要备份
        std::fs::write(&db_path, b"db").unwrap();
        assert!(needs_pre_migration_backup(
            &config_dir,
            &db_path,
            &session_path,
            &accounts_path
        ));

        // 残缺备份（只建了目录但无 manifest 完成标记）→ 不算有效备份，仍需备份
        let incomplete = config_dir.join("backups").join("pre_migration_20240101_000000");
        std::fs::create_dir_all(&incomplete).unwrap();
        assert!(
            needs_pre_migration_backup(&config_dir, &db_path, &session_path, &accounts_path),
            "残缺（无 manifest）的备份目录不应被认作已备份"
        );

        // 写入 manifest 完成标记后 → 视为有效备份，不再重复
        std::fs::write(incomplete.join(BACKUP_MANIFEST), b"{}").unwrap();
        assert!(!needs_pre_migration_backup(
            &config_dir,
            &db_path,
            &session_path,
            &accounts_path
        ));
    }

    #[tokio::test]
    async fn backup_writes_manifest_and_gates_validity() {
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        // 既有数据：DB + -wal/-shm 边车 + session.json + wal/ 任务目录
        let db_path = config_dir.join("baidu-pcs.db");
        std::fs::write(&db_path, b"db").unwrap();
        std::fs::write(config_dir.join("baidu-pcs.db-wal"), b"wal").unwrap();
        std::fs::write(config_dir.join("baidu-pcs.db-shm"), b"shm").unwrap();
        std::fs::write(config_dir.join("session.json"), b"{}").unwrap();
        let wal_dir = root.path().join("wal");
        std::fs::create_dir_all(wal_dir.join("sub")).unwrap();
        std::fs::write(wal_dir.join("task1.meta"), b"m").unwrap();
        std::fs::write(wal_dir.join("sub").join("task2.meta"), b"m").unwrap();

        let out = create_pre_migration_backup(&config_dir, &db_path, &wal_dir)
            .await
            .expect("应成功并写入 manifest");
        // manifest 必须作为完成标记存在
        assert!(out.dir.join(BACKUP_MANIFEST).exists());
        assert!(!out.copied.is_empty());
        // manifest 应记录 required：DB + -wal/-shm 边车 + session.json + wal 目录（源端均存在）
        let raw = std::fs::read_to_string(out.dir.join(BACKUP_MANIFEST)).unwrap();
        let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let required: Vec<String> = json["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        for expect in [
            "baidu-pcs.db",
            "baidu-pcs.db-wal",
            "baidu-pcs.db-shm",
            "session.json",
            "wal",
        ] {
            assert!(required.contains(&expect.to_string()), "required 应含 {expect}");
        }
        // manifest 应记录 wal 目录名与其下文件相对路径清单（含子目录，用 / 分隔）
        assert_eq!(json["wal_backup_dir"].as_str(), Some("wal"));
        let wal_files: Vec<String> = json["wal_files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(wal_files.contains(&"task1.meta".to_string()));
        assert!(wal_files.contains(&"sub/task2.meta".to_string()));
        // 备份已有效 → 后续不再需要备份
        assert!(valid_pre_migration_backup_exists(&config_dir));

        // 删除备份中的 -wal 边车 → 该备份立即失效（即使 manifest 仍在）
        std::fs::remove_file(out.dir.join("baidu-pcs.db-wal")).unwrap();
        assert!(
            !valid_pre_migration_backup_exists(&config_dir),
            "required 的 -wal 边车缺失时备份应判为无效"
        );
        std::fs::write(out.dir.join("baidu-pcs.db-wal"), b"wal").unwrap();
        assert!(valid_pre_migration_backup_exists(&config_dir));

        // 删除 wal 子目录中的具体文件（目录仍在）→ 逐项校验缺失 → 失效
        std::fs::remove_file(out.dir.join("wal").join("sub").join("task2.meta")).unwrap();
        assert!(
            !valid_pre_migration_backup_exists(&config_dir),
            "wal 子目录内具体文件缺失时备份应判为无效"
        );
        std::fs::write(out.dir.join("wal").join("sub").join("task2.meta"), b"m").unwrap();
        assert!(valid_pre_migration_backup_exists(&config_dir));

        // 改名顶替：删 task1.meta、补一个无关同数量文件 → 数量不变但具体文件缺失 → 失效
        std::fs::remove_file(out.dir.join("wal").join("task1.meta")).unwrap();
        std::fs::write(out.dir.join("wal").join("unrelated.meta"), b"m").unwrap();
        assert!(
            !valid_pre_migration_backup_exists(&config_dir),
            "具体 wal 文件被顶替（数量相同）时备份应判为无效"
        );
        // 恢复内容 → 再次有效；删整个 wal/ 目录 → 同样失效（目录存在性校验）
        std::fs::write(out.dir.join("wal").join("task1.meta"), b"m").unwrap();
        assert!(valid_pre_migration_backup_exists(&config_dir));
        std::fs::remove_dir_all(out.dir.join("wal")).unwrap();
        assert!(
            !valid_pre_migration_backup_exists(&config_dir),
            "required 的 wal/ 目录缺失时备份应判为无效"
        );
    }

    #[test]
    fn legacy_wal_file_count_fallback_is_honored() {
        // 向后兼容：旧 manifest（#10）只有 wal_file_count、无 wal_files → 回退到数量校验
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        let dir = config_dir.join("backups").join("pre_migration_20230101_000000");
        std::fs::create_dir_all(dir.join("wal")).unwrap();
        std::fs::write(dir.join("wal").join("a.meta"), b"m").unwrap();
        std::fs::write(dir.join("wal").join("b.meta"), b"m").unwrap();
        let manifest = br#"{"required":["wal"],"wal_backup_dir":"wal","wal_file_count":2}"#;
        std::fs::write(dir.join(BACKUP_MANIFEST), manifest).unwrap();
        assert!(valid_pre_migration_backup_exists(&config_dir));

        // 删到数量不足 → 失效
        std::fs::remove_file(dir.join("wal").join("b.meta")).unwrap();
        assert!(!valid_pre_migration_backup_exists(&config_dir));
    }

    #[tokio::test]
    async fn incomplete_wal_copy_fails_backup() {
        // wal 目录内有文件复制失败（此处用一个无读权限的文件模拟）→ 整体备份失败、
        // 不写 manifest、返回 Err，使其不被认作有效备份。
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db_path = config_dir.join("baidu-pcs.db");
        std::fs::write(&db_path, b"db").unwrap();
        let wal_dir = root.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let bad = wal_dir.join("locked.meta");
        std::fs::write(&bad, b"x").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o000)).unwrap();
        }

        // 仅当权限确实生效（非 root、文件确不可读）才断言；root 下可读则跳过
        let unreadable = std::fs::read(&bad).is_err();
        let res = create_pre_migration_backup(&config_dir, &db_path, &wal_dir).await;
        if unreadable {
            assert!(res.is_err(), "wal 内容复制不完整时应返回 Err");
            assert!(
                !valid_pre_migration_backup_exists(&config_dir),
                "复制不完整的备份不应被认作有效"
            );
        }
        // 还原权限以便 tempdir 清理
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o644));
        }
        let _ = res;
    }

    #[test]
    fn legacy_manifest_without_required_is_still_valid() {
        // 向后兼容：旧 manifest 不含 required 字段 → 仍按「存在即有效」处理
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        let dir = config_dir.join("backups").join("pre_migration_20230101_000000");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(BACKUP_MANIFEST), br#"{"completed_at":"x"}"#).unwrap();
        assert!(valid_pre_migration_backup_exists(&config_dir));

        // required 为空数组 → 也算有效（无任何关键文件可校验）
        std::fs::write(dir.join(BACKUP_MANIFEST), br#"{"required":[]}"#).unwrap();
        assert!(valid_pre_migration_backup_exists(&config_dir));

        // required 含缺失文件 → 无效
        std::fs::write(dir.join(BACKUP_MANIFEST), br#"{"required":["missing.db"]}"#).unwrap();
        assert!(!valid_pre_migration_backup_exists(&config_dir));
    }

    #[test]
    fn corrupt_or_unreadable_manifest_is_invalid() {
        // 损坏 / 半写入的 manifest（JSON 解析失败）→ 不算有效备份
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        let dir = config_dir.join("backups").join("pre_migration_20230101_000000");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(BACKUP_MANIFEST), b"{not valid json").unwrap();
        assert!(
            !valid_pre_migration_backup_exists(&config_dir),
            "损坏的 manifest 不应被认作有效备份"
        );
    }
}

/// 目录递归复制统计：`files` 为成功复制的文件**相对 `dest` 根**的路径（用 `/`
/// 分隔）；`failed` 为复制 / 读取失败的项数（含目录读取、目录遍历、条目类型读取、
/// 单文件复制失败）。`failed > 0` 表示备份**不完整**。
#[derive(Debug, Default, Clone)]
struct CopyDirStats {
    files: Vec<String>,
    failed: usize,
}

impl CopyDirStats {
    fn copied(&self) -> usize {
        self.files.len()
    }
}

/// 递归复制目录，返回 [`CopyDirStats`]。单文件 / 子项失败仅告警、不中断整体，但会
/// 计入 `failed`，由调用方据此判断目录备份是否完整。
async fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<CopyDirStats> {
    tokio::fs::create_dir_all(dest)
        .await
        .with_context(|| format!("创建目录失败: {}", dest.display()))?;
    let mut stats = CopyDirStats::default();
    let dest_root = dest.to_path_buf();
    // 用显式栈避免 async 递归。
    let mut stack: Vec<(PathBuf, PathBuf)> = vec![(src.to_path_buf(), dest.to_path_buf())];
    while let Some((cur_src, cur_dest)) = stack.pop() {
        let mut entries = match tokio::fs::read_dir(&cur_src).await {
            Ok(e) => e,
            Err(e) => {
                warn!("迁移前备份：读取目录 {} 失败（{}）", cur_src.display(), e);
                stats.failed += 1;
                continue;
            }
        };
        // 显式处理 next_entry() 的 Err：目录遍历中途失败必须计入 failed，
        // 否则会把残缺的 wal 备份误判为「完整复制」。
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    warn!("迁移前备份：遍历目录 {} 失败（{}）", cur_src.display(), e);
                    stats.failed += 1;
                    break;
                }
            };
            let path = entry.path();
            let target = cur_dest.join(entry.file_name());
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => {
                    if let Err(e) = tokio::fs::create_dir_all(&target).await {
                        warn!("迁移前备份：创建子目录 {} 失败（{}）", target.display(), e);
                        stats.failed += 1;
                        continue;
                    }
                    stack.push((path, target));
                }
                Ok(_) => match tokio::fs::copy(&path, &target).await {
                    Ok(_) => {
                        let rel = target
                            .strip_prefix(&dest_root)
                            .unwrap_or(&target)
                            .to_string_lossy()
                            .replace('\\', "/");
                        stats.files.push(rel);
                    }
                    Err(e) => {
                        warn!("迁移前备份：复制 {} 失败（{}）", path.display(), e);
                        stats.failed += 1;
                    }
                },
                Err(e) => {
                    warn!("迁移前备份：读取条目类型失败 {}（{}）", path.display(), e);
                    stats.failed += 1;
                }
            }
        }
    }
    Ok(stats)
}
