# 加密映射表保护记录

> 日期：2026-05-17
> 关联文档：`docs/AGE_MIGRATION_SUMMARY.md`、`docs/AUTO_BACKUP_ARCHITECTURE.md`

---

## 背景

`encryption_snapshots` 表存储 **uuid.age 与原始文件名的对应关系**，是加密文件还原原名和目录结构的**唯一线索**。一旦丢失，即使有 age 口令解密出文件内容，也无法恢复原始文件名和目录结构。

UUID v4（128 位随机，碰撞概率极低）保证了映射键的全局唯一性，因此没有理由删除任何记录。

---

## 修改清单

### 1. `delete_config` — 不再删除加密映射

**文件**：`backend/src/autobackup/manager.rs`

移除对 `delete_snapshots_by_config()` 的调用。删除备份配置不再连带删除加密映射。

```diff
- self.record_manager.delete_snapshots_by_config(id)?;
```

### 2. `cleanup_old_records` — 不再清理映射表

**文件**：`backend/src/autobackup/record/record_manager.rs`

移除对 `encryption_snapshots` 的 DELETE SQL。API 仅清理上传/下载去重记录，不再碰映射表。

```diff
- conn.execute("DELETE FROM encryption_snapshots WHERE updated_at < ?1 AND status = 'completed'")?;
```

### 3. `cancel_task` — 不再清理未完成映射

**文件**：`backend/src/autobackup/manager.rs`

移除对 `delete_incomplete_snapshots_by_config()` 的调用。取消备份任务不再清理任何映射记录。

```diff
- let deleted_snapshots = self.record_manager.delete_incomplete_snapshots_by_config(&config_id);
```

### 4. 三个删除函数降级为 no-op

**文件**：`backend/src/autobackup/record/record_manager.rs`

三个曾执行 DELETE SQL 的函数改为仅打警告日志，返回 0，不执行任何数据库操作：

| 函数 | 原 SQL | 现行为 |
|------|--------|--------|
| `delete_snapshots_by_config` | `DELETE WHERE config_id = ?1` | no-op + warning |
| `delete_incomplete_snapshots_by_config` | `DELETE WHERE config_id = ?1 AND status != 'completed'` | no-op + warning |
| `delete_snapshots_by_encrypted_names` | `DELETE WHERE encrypted_name IN (...)` | no-op + warning |

保留签名是为了编译兼容，防止外部未发现的调用方导致编译错误。

---

## 保护范围总览

| 触发路径 | 原先会删映射吗 | 现在 |
|---------|--------------|------|
| 前端删除云端文件 | ❌ 不删 | ❌ 不删（未改动） |
| 删除备份配置 | ✅ 会 | **❌ 已移除** |
| 取消备份任务 | ⚠️ 只删未完成的 | **❌ 已移除** |
| `cleanup_old_records` API | ✅ 会删 completed | **❌ 已移除** |
| `delete_snapshots_by_*` 函数 | ✅ 会删 | **❌ no-op** |

**当前允许的操作**：

- `INSERT` — 加密上传时新增映射 ✅
- `UPDATE` — 更新映射状态（encrypting → uploading → completed）✅
- `DELETE` — **全部禁用** ❌

---

## 相关文件

- `backend/src/autobackup/manager.rs` — 删除了两处调用
- `backend/src/autobackup/record/record_manager.rs` — `cleanup_old_records` 移除了 snapshots 删除 + 三个函数降级为 no-op
- `backend/src/server/handlers/autobackup.rs` — `CleanupRecordsResponse` 保留 `snapshot_deleted` 字段（向后兼容，恒为 0）
