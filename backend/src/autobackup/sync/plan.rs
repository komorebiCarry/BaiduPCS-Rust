//! 同步计划生成（Stage 2: Plan）
//!
//! 基于双端快照 + SyncState 决策树生成同步动作列表。
//! 纯计算，无副作用。

use std::collections::HashMap;

use anyhow::Result;

use super::state_manager::SyncStateManager;
use super::types::*;
use crate::autobackup::config::{SyncConflictStrategy, SyncInitMode};

/// 生成同步计划
///
/// 对每个 relative_path，按 §4.5.1 决策树生成动作：
/// - Case A: sync_state 不存在（首次发现的文件）
/// - Case B: sync_state 存在，两端文件都存在
/// - Case C: sync_state 存在，某端文件消失（tombstone）
pub fn generate_sync_plan(
    state_manager: &SyncStateManager,
    config_id: &str,
    local_snapshot: &[LocalScannedFile],
    remote_snapshot: Option<&[RemoteScannedFile]>,
    strategy: SyncConflictStrategy,
    init_mode: SyncInitMode,
) -> Result<SyncPlan> {
    let mut plan = SyncPlan::empty();

    // 建立索引
    let local_map: HashMap<&str, &LocalScannedFile> = local_snapshot
        .iter()
        .map(|f| (f.relative_path.as_str(), f))
        .collect();

    let remote_map: HashMap<&str, &RemoteScannedFile> = remote_snapshot
        .map(|rs| rs.iter().map(|f| (f.relative_path.as_str(), f)).collect())
        .unwrap_or_default();

    // 获取所有 SyncState
    let existing_states = state_manager.get_all_states(config_id)?;
    let state_map: HashMap<&str, &SyncStateRow> = existing_states
        .iter()
        .map(|s| (s.relative_path.as_str(), s))
        .collect();

    // 收集所有已知的 relative_path
    let mut all_paths: Vec<&str> = Vec::new();
    for path in local_map.keys() {
        all_paths.push(path);
    }
    for path in remote_map.keys() {
        if !local_map.contains_key(path) {
            all_paths.push(path);
        }
    }
    for path in state_map.keys() {
        if !local_map.contains_key(path) && !remote_map.contains_key(path) {
            all_paths.push(path);
        }
    }
    all_paths.sort();
    all_paths.dedup();

    let has_remote = remote_snapshot.is_some();

    for path in &all_paths {
        let local_file = local_map.get(path).copied();
        let remote_file = remote_map.get(path).copied();
        let sync_state = state_map.get(path).copied();

        process_file(
            path,
            local_file,
            remote_file,
            sync_state,
            has_remote,
            strategy,
            init_mode,
            &mut plan,
        );
    }

    tracing::info!(
        "同步计划生成完成: config={}, uploads={}, downloads={}, state_updates={}, conflicts={}, skipped={}",
        config_id, plan.uploads.len(), plan.downloads.len(),
        plan.state_updates.len(), plan.conflicts.len(), plan.skipped
    );

    Ok(plan)
}

/// 处理单个文件的同步决策
fn process_file(
    path: &str,
    local_file: Option<&LocalScannedFile>,
    remote_file: Option<&RemoteScannedFile>,
    sync_state: Option<&SyncStateRow>,
    has_remote: bool,
    strategy: SyncConflictStrategy,
    init_mode: SyncInitMode,
    plan: &mut SyncPlan,
) {
    match (sync_state, local_file, remote_file) {
        // ═══ Case A: sync_state 不存在（首次发现的文件） ═══
        (None, Some(local), None) => {
            // A1: 仅本地有 → Upload
            plan.uploads.push(SyncUploadAction {
                relative_path: path.to_string(),
                local_path: local.local_path.clone(),
                local_mtime: local.mtime,
                local_size: local.size,
            });
        }
        (None, None, Some(remote)) => {
            // A2: 仅远端有 → Download
            plan.downloads.push(SyncDownloadAction {
                relative_path: path.to_string(),
                remote_path: remote.remote_path.clone(),
                remote_mtime: remote.mtime,
                remote_size: remote.size,
                fs_id: remote.fs_id,
            });
        }
        (None, Some(local), Some(remote)) => {
            // A3: 两端都有，无 SyncState → 首次对齐检查
            handle_case_a3(path, local, remote, strategy, init_mode, plan);
        }

        // ═══ Case B/C: sync_state 存在 ═══
        (Some(state), local_opt, remote_opt) => {
            let local_present = local_opt.is_some();
            let remote_present = if has_remote { remote_opt.is_some() } else {
                // Watch 路径：无远端快照，假设远端状态未知（不处理远端变更）
                // 使用 SyncState 中的 remote_exists 作为参考
                state.remote_exists
            };

            match (local_present, remote_present) {
                (true, true) => {
                    // Case B: 两端都存在 → 增量比对
                    let local = local_opt.unwrap();
                    if has_remote {
                        let remote = remote_opt.unwrap();
                        handle_case_b(path, local, remote, state, strategy, plan);
                    } else {
                        // Watch 路径：仅比对本地变更
                        handle_case_b_watch(path, local, state, plan);
                    }
                }
                (false, true) if has_remote => {
                    // C1: 本地消失，远端仍存在
                    handle_case_c1(path, state, remote_opt, plan);
                }
                (false, true) if !has_remote => {
                    // Watch 路径下本地消失：C1 tombstone
                    handle_case_c1(path, state, None, plan);
                }
                (true, false) if has_remote => {
                    // C2: 远端消失，本地仍存在
                    handle_case_c2(path, state, local_opt, plan);
                }
                (false, false) if has_remote => {
                    // C3: 两端都消失
                    plan.state_updates.push(SyncStateUpdate {
                        relative_path: path.to_string(),
                        observed: ObservedFileState {
                            local_mtime: None,
                            local_size: None,
                            local_exists: false,
                            remote_mtime: None,
                            remote_size: None,
                            remote_fs_id: None,
                            remote_exists: false,
                            direction: SyncDirection::StateOnly,
                        },
                        reason: StateUpdateReason::BothDeleted,
                        meta: None,
                    });
                }
                _ => {
                    // 其他情况（Watch 路径 + 本地存在 + 远端不可见等）
                    plan.skipped += 1;
                }
            }
        }

        // sync_state 不存在，两端都不存在 → 忽略
        (None, None, None) => {}
    }
}

/// Case A3: 两端都有，无 SyncState → 首次对齐
fn handle_case_a3(
    path: &str,
    local: &LocalScannedFile,
    remote: &RemoteScannedFile,
    strategy: SyncConflictStrategy,
    init_mode: SyncInitMode,
    plan: &mut SyncPlan,
) {
    // AdoptBothSides: 直接建立基线，不传输
    if init_mode == SyncInitMode::AdoptBothSides {
        plan.state_updates.push(SyncStateUpdate {
            relative_path: path.to_string(),
            observed: ObservedFileState {
                local_mtime: Some(local.mtime),
                local_size: Some(local.size),
                local_exists: true,
                remote_mtime: Some(remote.mtime),
                remote_size: Some(remote.size),
                remote_fs_id: Some(remote.fs_id),
                remote_exists: true,
                direction: SyncDirection::StateOnly,
            },
            reason: StateUpdateReason::AdoptBaseline,
            meta: None,
        });
        return;
    }

    // AutoDetect: 同大小时尝试 md5 比对，确认是否为同一文件
    if local.size == remote.size {
        if let Some(ref remote_md5) = remote.md5 {
            if !remote_md5.is_empty() {
                // 计算本地文件完整 MD5 与远端比对
                match compute_file_md5(&local.local_path) {
                    Ok(local_md5) => {
                        if local_md5.eq_ignore_ascii_case(remote_md5) {
                            // MD5 一致：同一文件，直接建立基线，不传输
                            plan.state_updates.push(SyncStateUpdate {
                                relative_path: path.to_string(),
                                observed: ObservedFileState {
                                    local_mtime: Some(local.mtime),
                                    local_size: Some(local.size),
                                    local_exists: true,
                                    remote_mtime: Some(remote.mtime),
                                    remote_size: Some(remote.size),
                                    remote_fs_id: Some(remote.fs_id),
                                    remote_exists: true,
                                    direction: SyncDirection::StateOnly,
                                },
                                reason: StateUpdateReason::Md5Match,
                                meta: None,
                            });
                            return;
                        }
                        // MD5 不同：真正的冲突，继续走冲突策略
                    }
                    Err(e) => {
                        tracing::warn!("计算本地 MD5 失败，回退到冲突策略: path={}, err={}", path, e);
                        // 计算失败时保守处理，走冲突策略
                    }
                }
            }
        }
    }

    // 走冲突策略
    resolve_conflict(path, local, remote, strategy, plan);
}

/// Case B: sync_state 存在，两端都存在 → 增量比对
fn handle_case_b(
    path: &str,
    local: &LocalScannedFile,
    remote: &RemoteScannedFile,
    state: &SyncStateRow,
    strategy: SyncConflictStrategy,
    plan: &mut SyncPlan,
) {
    let mut local_changed = is_local_changed(local, state);
    let mut remote_changed = is_remote_changed(remote, state);

    // NULL mtime 回填特判
    if local_changed && is_local_mtime_backfill(local, state) {
        plan.state_updates.push(SyncStateUpdate {
            relative_path: path.to_string(),
            observed: ObservedFileState {
                local_mtime: Some(local.mtime),
                local_size: Some(local.size),
                local_exists: true,
                remote_mtime: state.remote_mtime,
                remote_size: state.remote_size.map(|s| s as u64),
                remote_fs_id: state.remote_fs_id.map(|id| id as u64),
                remote_exists: state.remote_exists,
                direction: SyncDirection::StateOnly,
            },
            reason: StateUpdateReason::MtimeBackfill,
            meta: None,
        });
        local_changed = false;
    }

    if remote_changed && is_remote_mtime_backfill(remote, state) {
        plan.state_updates.push(SyncStateUpdate {
            relative_path: path.to_string(),
            observed: ObservedFileState {
                local_mtime: state.local_mtime,
                local_size: state.local_size.map(|s| s as u64),
                local_exists: state.local_exists,
                remote_mtime: Some(remote.mtime),
                remote_size: Some(remote.size),
                remote_fs_id: Some(remote.fs_id),
                remote_exists: true,
                direction: SyncDirection::StateOnly,
            },
            reason: StateUpdateReason::MtimeBackfill,
            meta: None,
        });
        remote_changed = false;
    }

    match (local_changed, remote_changed) {
        (true, false) => {
            // B1: 仅本地变更 → Upload
            plan.uploads.push(SyncUploadAction {
                relative_path: path.to_string(),
                local_path: local.local_path.clone(),
                local_mtime: local.mtime,
                local_size: local.size,
            });
        }
        (false, true) => {
            // B2: 仅远端变更 → Download
            plan.downloads.push(SyncDownloadAction {
                relative_path: path.to_string(),
                remote_path: remote.remote_path.clone(),
                remote_mtime: remote.mtime,
                remote_size: remote.size,
                fs_id: remote.fs_id,
            });
        }
        (true, true) => {
            // B3: 双端变更 → Conflict
            resolve_conflict(path, local, remote, strategy, plan);
        }
        (false, false) => {
            // B4: 都未变化 → Skip
            plan.skipped += 1;
        }
    }
}

/// Watch 路径下的 Case B：仅比对本地变更
fn handle_case_b_watch(
    path: &str,
    local: &LocalScannedFile,
    state: &SyncStateRow,
    plan: &mut SyncPlan,
) {
    let local_changed = is_local_changed(local, state);

    if local_changed {
        // Watch 路径：本地变更直接上传（隐式 LocalWins）
        plan.uploads.push(SyncUploadAction {
            relative_path: path.to_string(),
            local_path: local.local_path.clone(),
            local_mtime: local.mtime,
            local_size: local.size,
        });
    } else {
        plan.skipped += 1;
    }
}

/// C1: 本地消失，远端仍存在
fn handle_case_c1(
    path: &str,
    state: &SyncStateRow,
    remote_file: Option<&RemoteScannedFile>,
    plan: &mut SyncPlan,
) {
    // 幂等检查：如果已经是 tombstone 稳态，纯 Skip
    if !state.local_exists {
        // 检查存活侧是否有变化
        if let Some(remote) = remote_file {
            if state.remote_mtime == Some(remote.mtime)
                && state.remote_size == Some(remote.size as i64)
                && state.remote_fs_id == Some(remote.fs_id as i64)
            {
                // 完全稳态，纯 Skip
                plan.skipped += 1;
                return;
            }
            // 存活侧有变化 → C5
            plan.state_updates.push(SyncStateUpdate {
                relative_path: path.to_string(),
                observed: ObservedFileState {
                    local_mtime: None,
                    local_size: None,
                    local_exists: false,
                    remote_mtime: Some(remote.mtime),
                    remote_size: Some(remote.size),
                    remote_fs_id: Some(remote.fs_id),
                    remote_exists: true,
                    direction: SyncDirection::StateOnly,
                },
                reason: StateUpdateReason::Tombstone,
                meta: Some(SyncStateMeta {
                    c5_detected: Some(true),
                    tombstoned_at: None, // 保持原有的 tombstoned_at
                }),
            });
            return;
        }
        // Watch 路径下无远端信息，纯 Skip
        plan.skipped += 1;
        return;
    }

    // 首次 tombstone
    let now = chrono::Utc::now().timestamp();
    let remote_state = if let Some(remote) = remote_file {
        (Some(remote.mtime), Some(remote.size), Some(remote.fs_id))
    } else {
        (state.remote_mtime, state.remote_size.map(|s| s as u64), state.remote_fs_id.map(|id| id as u64))
    };

    plan.state_updates.push(SyncStateUpdate {
        relative_path: path.to_string(),
        observed: ObservedFileState {
            local_mtime: None,
            local_size: None,
            local_exists: false,
            remote_mtime: remote_state.0,
            remote_size: remote_state.1,
            remote_fs_id: remote_state.2,
            remote_exists: true,
            direction: SyncDirection::StateOnly,
        },
        reason: StateUpdateReason::Tombstone,
        meta: Some(SyncStateMeta {
            c5_detected: Some(false),
            tombstoned_at: Some(Some(now)),
        }),
    });
}

/// C2: 远端消失，本地仍存在
fn handle_case_c2(
    path: &str,
    state: &SyncStateRow,
    local_file: Option<&LocalScannedFile>,
    plan: &mut SyncPlan,
) {
    // 幂等检查
    if !state.remote_exists {
        if let Some(local) = local_file {
            if state.local_mtime == Some(local.mtime)
                && state.local_size == Some(local.size as i64)
            {
                plan.skipped += 1;
                return;
            }
            // C5: 存活侧有变化
            plan.state_updates.push(SyncStateUpdate {
                relative_path: path.to_string(),
                observed: ObservedFileState {
                    local_mtime: Some(local.mtime),
                    local_size: Some(local.size),
                    local_exists: true,
                    remote_mtime: None,
                    remote_size: None,
                    remote_fs_id: None,
                    remote_exists: false,
                    direction: SyncDirection::StateOnly,
                },
                reason: StateUpdateReason::Tombstone,
                meta: Some(SyncStateMeta {
                    c5_detected: Some(true),
                    tombstoned_at: None,
                }),
            });
            return;
        }
        plan.skipped += 1;
        return;
    }

    // 首次 tombstone
    let now = chrono::Utc::now().timestamp();
    let local_state = if let Some(local) = local_file {
        (Some(local.mtime), Some(local.size))
    } else {
        (state.local_mtime, state.local_size.map(|s| s as u64))
    };

    plan.state_updates.push(SyncStateUpdate {
        relative_path: path.to_string(),
        observed: ObservedFileState {
            local_mtime: local_state.0,
            local_size: local_state.1,
            local_exists: true,
            remote_mtime: None,
            remote_size: None,
            remote_fs_id: None,
            remote_exists: false,
            direction: SyncDirection::StateOnly,
        },
        reason: StateUpdateReason::Tombstone,
        meta: Some(SyncStateMeta {
            c5_detected: Some(false),
            tombstoned_at: Some(Some(now)),
        }),
    });
}

/// 冲突解决
fn resolve_conflict(
    path: &str,
    local: &LocalScannedFile,
    remote: &RemoteScannedFile,
    strategy: SyncConflictStrategy,
    plan: &mut SyncPlan,
) {
    match strategy {
        SyncConflictStrategy::LocalWins => {
            plan.uploads.push(SyncUploadAction {
                relative_path: path.to_string(),
                local_path: local.local_path.clone(),
                local_mtime: local.mtime,
                local_size: local.size,
            });
        }
        SyncConflictStrategy::RemoteWins => {
            plan.downloads.push(SyncDownloadAction {
                relative_path: path.to_string(),
                remote_path: remote.remote_path.clone(),
                remote_mtime: remote.mtime,
                remote_size: remote.size,
                fs_id: remote.fs_id,
            });
        }
        SyncConflictStrategy::NewerWins => {
            resolve_newer_wins(path, local, remote, plan);
        }
        SyncConflictStrategy::Skip => {
            plan.conflicts.push(SyncConflictRecord {
                relative_path: path.to_string(),
                local_mtime: local.mtime,
                local_size: local.size,
                remote_mtime: remote.mtime,
                remote_size: remote.size,
            });
            // Skip 仍写 SyncState 基线
            plan.state_updates.push(SyncStateUpdate {
                relative_path: path.to_string(),
                observed: ObservedFileState {
                    local_mtime: Some(local.mtime),
                    local_size: Some(local.size),
                    local_exists: true,
                    remote_mtime: Some(remote.mtime),
                    remote_size: Some(remote.size),
                    remote_fs_id: Some(remote.fs_id),
                    remote_exists: true,
                    direction: SyncDirection::Skip,
                },
                reason: StateUpdateReason::ConflictSkipped,
                meta: None,
            });
        }
    }
}

/// NewerWins 精确语义（§4.2.1）
fn resolve_newer_wins(
    path: &str,
    local: &LocalScannedFile,
    remote: &RemoteScannedFile,
    plan: &mut SyncPlan,
) {
    if local.mtime > remote.mtime {
        // 本地更新 → Upload
        plan.uploads.push(SyncUploadAction {
            relative_path: path.to_string(),
            local_path: local.local_path.clone(),
            local_mtime: local.mtime,
            local_size: local.size,
        });
    } else if local.mtime < remote.mtime {
        // 远端更新 → Download
        plan.downloads.push(SyncDownloadAction {
            relative_path: path.to_string(),
            remote_path: remote.remote_path.clone(),
            remote_mtime: remote.mtime,
            remote_size: remote.size,
            fs_id: remote.fs_id,
        });
    } else {
        // 同秒：tie-break = LocalWins
        if local.size != remote.size {
            // 同秒不同大小 → LocalWins
            plan.uploads.push(SyncUploadAction {
                relative_path: path.to_string(),
                local_path: local.local_path.clone(),
                local_mtime: local.mtime,
                local_size: local.size,
            });
        } else {
            // 同秒同大小：md5 比对，一致则 StateOnly，不一致则 LocalWins
            let mut need_upload = true;
            if let Some(ref remote_md5) = remote.md5 {
                if !remote_md5.is_empty() {
                    if let Ok(local_md5) = compute_file_md5(&local.local_path) {
                        if local_md5.eq_ignore_ascii_case(remote_md5) {
                            // MD5 一致，无需传输
                            plan.state_updates.push(SyncStateUpdate {
                                relative_path: path.to_string(),
                                observed: ObservedFileState {
                                    local_mtime: Some(local.mtime),
                                    local_size: Some(local.size),
                                    local_exists: true,
                                    remote_mtime: Some(remote.mtime),
                                    remote_size: Some(remote.size),
                                    remote_fs_id: Some(remote.fs_id),
                                    remote_exists: true,
                                    direction: SyncDirection::StateOnly,
                                },
                                reason: StateUpdateReason::Md5Match,
                                meta: None,
                            });
                            need_upload = false;
                        }
                    }
                }
            }
            if need_upload {
                plan.uploads.push(SyncUploadAction {
                    relative_path: path.to_string(),
                    local_path: local.local_path.clone(),
                    local_mtime: local.mtime,
                    local_size: local.size,
                });
            }
        }
    }
}

// ==================== 变更检测辅助函数 ====================

/// 检测本地文件是否相对于 SyncState 发生变更
fn is_local_changed(local: &LocalScannedFile, state: &SyncStateRow) -> bool {
    if state.local_mtime.is_none() {
        return true; // 之前未记录
    }
    local.mtime != state.local_mtime.unwrap()
        || local.size as i64 != state.local_size.unwrap_or(-1)
}

/// 检测远端文件是否相对于 SyncState 发生变更
fn is_remote_changed(remote: &RemoteScannedFile, state: &SyncStateRow) -> bool {
    if state.remote_mtime.is_none() {
        // 远端 mtime 未记录 — 需要区分"真正变更"和"回填"：
        // 如果 remote_fs_id 和 remote_size 已有值且匹配，is_remote_mtime_backfill 会处理
        // 如果 remote_fs_id/remote_size 也为 None（首次上传后），也标为 changed
        //   让 handle_case_b 触发 mtime 回填写入完整的远端元数据
        return true;
    }
    remote.mtime != state.remote_mtime.unwrap()
        || remote.size as i64 != state.remote_size.unwrap_or(-1)
        || remote.fs_id as i64 != state.remote_fs_id.unwrap_or(-1)
}

/// 检测是否为本地 mtime 回填（非真正变更）
fn is_local_mtime_backfill(local: &LocalScannedFile, state: &SyncStateRow) -> bool {
    state.local_mtime.is_none()
        && state.local_size.is_some()
        && state.local_size.unwrap() == local.size as i64
}

/// 检测是否为远端 mtime 回填（非真正变更）
///
/// 两种回填场景：
/// 1. remote_fs_id/remote_size 已有且匹配（上传后远端元数据部分写入）
/// 2. remote_fs_id/remote_size 全为 None（首次上传后，远端信息完全缺失）
///    此时通过 local_size 与远端 size 宽松匹配判断是否为同一文件
fn is_remote_mtime_backfill(remote: &RemoteScannedFile, state: &SyncStateRow) -> bool {
    if !state.remote_mtime.is_none() {
        return false;
    }
    // 场景 1：remote_fs_id/remote_size 已有且匹配
    if state.remote_fs_id.is_some() && state.remote_size.is_some() {
        return state.remote_fs_id.unwrap() == remote.fs_id as i64
            && state.remote_size.unwrap() == remote.size as i64;
    }
    // 场景 2：远端元数据全为 None，通过 local_size 宽松匹配
    // 首次上传完成后 SyncState 有 local_size 但无 remote_*
    if state.remote_exists && state.remote_fs_id.is_none() && state.local_size.is_some() {
        return remote.size as i64 == state.local_size.unwrap_or(-1);
    }
    false
}

/// 计算文件完整 MD5（用于 AutoDetect 同大小文件比对）
fn compute_file_md5(path: &std::path::Path) -> Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut context = md5::Context::new();
    let mut buffer = [0u8; 8192];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        context.consume(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", context.compute()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    fn create_test_env() -> (SyncStateManager, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let mgr = SyncStateManager::new(tmp.path()).unwrap();
        (mgr, tmp)
    }

    fn local_file(path: &str, mtime: i64, size: u64) -> LocalScannedFile {
        LocalScannedFile {
            relative_path: path.to_string(),
            local_path: PathBuf::from(format!("/local/{}", path)),
            size,
            mtime,
        }
    }

    fn remote_file(path: &str, mtime: i64, size: u64, fs_id: u64) -> RemoteScannedFile {
        RemoteScannedFile {
            relative_path: path.to_string(),
            remote_path: format!("/remote/{}", path),
            size,
            mtime,
            fs_id,
            md5: None,
        }
    }

    #[test]
    fn test_case_a1_local_only() {
        let (mgr, _tmp) = create_test_env();
        let local = vec![local_file("new.txt", 1000, 100)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&[]),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert_eq!(plan.uploads.len(), 1);
        assert_eq!(plan.uploads[0].relative_path, "new.txt");
        assert!(plan.downloads.is_empty());
    }

    #[test]
    fn test_case_a2_remote_only() {
        let (mgr, _tmp) = create_test_env();
        let remote = vec![remote_file("cloud.txt", 2000, 200, 42)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &[], Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert!(plan.uploads.is_empty());
        assert_eq!(plan.downloads.len(), 1);
        assert_eq!(plan.downloads[0].relative_path, "cloud.txt");
    }

    #[test]
    fn test_case_a3_adopt_both_sides() {
        let (mgr, _tmp) = create_test_env();
        let local = vec![local_file("shared.txt", 1000, 100)];
        let remote = vec![remote_file("shared.txt", 1001, 100, 55)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AdoptBothSides,
        ).unwrap();

        assert!(plan.uploads.is_empty());
        assert!(plan.downloads.is_empty());
        assert_eq!(plan.state_updates.len(), 1);
        assert_eq!(plan.state_updates[0].reason, StateUpdateReason::AdoptBaseline);
    }

    #[test]
    fn test_case_b1_local_changed() {
        let (mgr, _tmp) = create_test_env();

        // 先建立基线
        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        // 本地修改了
        let local = vec![local_file("file.txt", 2000, 200)];
        let remote = vec![remote_file("file.txt", 1000, 100, 1)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert_eq!(plan.uploads.len(), 1);
        assert!(plan.downloads.is_empty());
    }

    #[test]
    fn test_case_b2_remote_changed() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        let local = vec![local_file("file.txt", 1000, 100)];
        let remote = vec![remote_file("file.txt", 2000, 200, 2)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert!(plan.uploads.is_empty());
        assert_eq!(plan.downloads.len(), 1);
    }

    #[test]
    fn test_case_b3_conflict_newer_wins() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        // 双端都变化，远端更新
        let local = vec![local_file("file.txt", 1500, 150)];
        let remote = vec![remote_file("file.txt", 2000, 200, 2)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        // 远端 mtime 更大，应该 Download
        assert!(plan.uploads.is_empty());
        assert_eq!(plan.downloads.len(), 1);
    }

    #[test]
    fn test_case_b3_conflict_local_wins() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        let local = vec![local_file("file.txt", 2000, 200)];
        let remote = vec![remote_file("file.txt", 1500, 150, 2)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::LocalWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert_eq!(plan.uploads.len(), 1);
        assert!(plan.downloads.is_empty());
    }

    #[test]
    fn test_case_b3_conflict_skip() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        let local = vec![local_file("file.txt", 2000, 200)];
        let remote = vec![remote_file("file.txt", 1500, 150, 2)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::Skip, SyncInitMode::AutoDetect,
        ).unwrap();

        assert!(plan.uploads.is_empty());
        assert!(plan.downloads.is_empty());
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.state_updates.len(), 1);
        assert_eq!(plan.state_updates[0].reason, StateUpdateReason::ConflictSkipped);
    }

    #[test]
    fn test_case_b4_no_change() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        let local = vec![local_file("file.txt", 1000, 100)];
        let remote = vec![remote_file("file.txt", 1000, 100, 1)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert!(plan.uploads.is_empty());
        assert!(plan.downloads.is_empty());
        assert_eq!(plan.skipped, 1);
    }

    #[test]
    fn test_case_c1_local_deleted() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "deleted.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        // 本地已删除
        let local: Vec<LocalScannedFile> = vec![];
        let remote = vec![remote_file("deleted.txt", 1000, 100, 1)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, Some(&remote),
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        assert!(plan.uploads.is_empty());
        assert!(plan.downloads.is_empty());
        assert_eq!(plan.state_updates.len(), 1);
        assert_eq!(plan.state_updates[0].reason, StateUpdateReason::Tombstone);
        assert!(!plan.state_updates[0].observed.local_exists);
    }

    #[test]
    fn test_watch_path_upload_only() {
        let (mgr, _tmp) = create_test_env();

        mgr.update_after_sync("cfg1", "file.txt", &ObservedFileState {
            local_mtime: Some(1000), local_size: Some(100), local_exists: true,
            remote_mtime: Some(1000), remote_size: Some(100), remote_fs_id: Some(1), remote_exists: true,
            direction: SyncDirection::Upload,
        }).unwrap();

        // Watch 路径：无远端快照
        let local = vec![local_file("file.txt", 2000, 200)];

        let plan = generate_sync_plan(
            &mgr, "cfg1", &local, None, // remote_snapshot = None
            SyncConflictStrategy::NewerWins, SyncInitMode::AutoDetect,
        ).unwrap();

        // 应该只有上传，没有下载
        assert_eq!(plan.uploads.len(), 1);
        assert!(plan.downloads.is_empty());
    }
}
