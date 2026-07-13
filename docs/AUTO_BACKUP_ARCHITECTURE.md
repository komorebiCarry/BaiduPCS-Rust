# 自动备份模块架构文档

> 文档版本：v1.0  
> 最后更新：2026-05-17  
> 对应代码：`backend/src/autobackup/`

---

## 一、模块概述

自动备份模块实现**本地文件夹自动备份到百度网盘**的功能，支持三种备份方向：

| 方向 | 含义 | 数据流 |
|------|------|--------|
| **Upload** | 上传备份 | 本地 → 云端 |
| **Download** | 下载备份 | 云端 → 本地 |
| **Sync** | 双向同步 | 本地 ↔ 云端 |

### 核心能力

- **文件系统监听**：实时检测文件变更（基于 `notify` crate）
- **定时轮询**：兜底机制，带随机抖动防反爬
- **客户端侧加密**：age 口令加密 (age-encryption.org/v1)
- **去重服务**：head_md5 快速比对，避免重复上传
- **增量扫描缓存**：仅处理变化/新增文件
- **优先级控制**：备份任务优先级最低，可被手动任务抢占
- **SQLite 持久化**：支持断点续传和重启恢复
- **配置冲突校验**：防止重复备份和循环备份

---

## 二、目录结构

```
backend/src/autobackup/
├── mod.rs                  # 模块入口
├── config.rs               # 备份配置数据结构
├── manager.rs              # 【核心协调器】~9000+ 行
├── task.rs                 # 备份任务数据结构
├── error.rs                # 错误处理与重试策略
├── events.rs               # WebSocket 事件 + 传输任务通知
├── persistence.rs          # SQLite 持久化（断点恢复）
├── validation.rs           # 配置冲突校验
├── scan_cache.rs           # 增量扫描缓存
├── common/
│   ├── mod.rs
│   └── temp_file.rs        # 临时文件安全管理
├── scheduler/
│   ├── mod.rs
│   ├── task_controller.rs  # 单配置任务控制器（并发控制核心）
│   ├── change_aggregator.rs# 变更事件聚合器（防抖去重）
│   ├── poll_scheduler.rs   # 定时轮询调度器
│   └── backup_scheduler.rs # 三阶段执行调度器
├── watcher/
│   ├── mod.rs
│   └── file_watcher.rs     # 文件系统监听服务
├── sync/
│   ├── mod.rs
│   ├── intent.rs           # Sync 意图位图
│   ├── state_manager.rs    # 同步状态管理（SQLite）
│   ├── plan.rs             # 同步计划生成
│   └── types.rs            # 同步类型定义
├── priority/
│   ├── mod.rs
│   └── policy.rs           # 优先级策略
└── record/
    ├── mod.rs
    └── record_manager.rs   # 去重服务 + 备份记录
```

---

## 三、事件驱动架构

### 3.1 总体数据流

```
┌─────────────────────────────────────────────────────────────┐
│                       触发源                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐               │
│  │  Watch   │    │   Poll   │    │  Manual  │               │
│  │ (notify) │    │ (定时器)  │    │ (用户/API)│               │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘               │
│       │               │               │                      │
│       ▼               ▼               ▼                      │
│  ┌─────────────────────────────────────────┐                 │
│  │         ChangeEvent (enum)              │                 │
│  │  WatchEvent{config_id, paths}           │                 │
│  │  PollEvent{config_id}                   │                 │
│  │  GlobalPollEvent{direction, poll_type}  │                 │
│  └─────────────────┬───────────────────────┘                 │
│                    │ unbounded channel                       │
│                    ▼                                         │
│  ┌─────────────────────────────────────────┐                 │
│  │         ChangeAggregator                │                 │
│  │  - 防抖窗口 (默认 3 秒)                  │                 │
│  │  - 按 config_id 去重合并                 │                 │
│  └─────────────────┬───────────────────────┘                 │
│                    │ 聚合后事件                              │
│                    ▼                                         │
│  ┌─────────────────────────────────────────┐                 │
│  │      start_event_consumer (tokio task)   │                │
│  │  事件分发循环                              │                 │
│  └──────┬──────────────────────┬────────────┘                 │
│         │                      │                              │
│   WatchEvent            PollEvent / GlobalPollEvent            │
│         │                      │                              │
│         ▼                      ▼                              │
│  ┌─────────────────────────────────────────┐                 │
│  │         TaskController (per config)     │                 │
│  │  - running / pending 原子标志            │                 │
│  │  - Notify 唤醒机制                       │                 │
│  │  - CancellationToken 取消                │                 │
│  │  - 信号合并 (coalescing)                 │                 │
│  └─────────────────┬───────────────────────┘                 │
│                    │ task_loop()                              │
│                    ▼                                         │
│  ┌─────────────────────────────────────────┐                 │
│  │      execute_backup_for_config()        │                 │
│  │  扫描 → 传输 → 完成                      │                 │
│  └─────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 触发机制对比

| 特性 | Watch（文件监听） | Poll（定时轮询） |
|------|------------------|-----------------|
| 实时性 | 亚秒级 | 分钟级 |
| 扫描范围 | 仅变化的文件（快路径） | 全量目录扫描 |
| 技术栈 | `notify` crate (`RecommendedWatcher`) | `tokio::time::interval` |
| 防抖 | 3 秒聚合窗口 + 文件稳定性检测（1s） | 随机抖动 0-25% |
| 适用方向 | Upload、Sync（仅 upload 意图） | Upload、Download、Sync |
| 兜底策略 | 失败次数 > 3 次时仅打日志，不自动切换 | N/A（本身即是兜底） |

### 3.3 TaskController — 并发控制核心

每个备份配置对应一个 `TaskController`，解决三个并发冲突问题：

1. **任务执行很久（如 30 分钟）** + **轮询间隔（如 10 分钟）** → 防止重复触发
2. **文件监听随时触发** → 正确合并
3. **轮询和监听同时触发** → 不丢不并发

**核心机制：**

```rust
pub fn trigger(&self, source: TriggerSource) -> bool {
    if self.running.load(Ordering::Acquire) {
        // 正在执行 → 仅标记 pending，不启动新任务
        self.pending.swap(true, Ordering::Release);
        return true;
    }
    // 空闲 → 唤醒 task_loop
    self.notify.notify_one();
    true
}
```

`task_loop()` 主循环：
1. 等待 `Notify` 信号
2. CAS 抢执行权（`running: false → true`）
3. 执行实际任务（可取消）
4. 标记 `running: false`
5. 检查 `pending` → 有则立即重新执行，无则回到等待

---

## 四、扫描流程（以 Upload 为例）

```
execute_backup_for_config(config)
│
├─ 1. 前置检查
│   ├─ 冲突校验 (validate_for_execute)
│   ├─ 检查 Preparing 状态 → 丢弃新触发
│   └─ 检查 Queued 续传任务 → 跳过扫描直接续传
│
├─ 2. 扫描阶段 (scan_local_directory_for_backup)
│   ├─ 创建 BatchedScanIterator
│   │   └─ spawn_blocking 中分批读取目录
│   ├─ 扩展名/目录/大小过滤
│   ├─ 读取 mtime + 增量缓存比对
│   │   └─ scan_cache: 仅处理变化/新增文件
│   ├─ 文件稳定性检测（检查大小是否仍变化）
│   ├─ head_md5 计算 + 去重检查
│   │   └─ record_manager.check_upload_record_preliminary()
│   ├─ 构建 BackupFileTask 列表
│   └─ 批量更新扫描缓存
│
├─ 3. 传输阶段 (execute_upload_backup_with_files)
│   ├─ 更新任务状态: Queued → Preparing
│   ├─ 遍历文件任务:
│   │   ├─ 加密 (可选 age 口令加密)
│   │   ├─ 创建 UploadManager 任务
│   │   └─ 记录 related_task_id 映射
│   ├─ 更新状态: Preparing → Transferring
│   ├─ 等待传输完成 (监听 BackupTransferNotification)
│   │   ├─ handle_transfer_progress (进度更新)
│   │   ├─ handle_transfer_completed (成功/失败)
│   │   └─ 写备份记录
│   └─ 清理临时文件
│
└─ 4. 完成处理
    ├─ 更新任务状态 → Completed / PartiallyCompleted / Failed
    ├─ 持久化到 SQLite
    └─ 发送 WebSocket 事件
```

---

## 五、三阶段调度模型 (BackupScheduler)

每个文件任务经过三个阶段：

| 阶段 | 名称 | 具体操作 | 副作用 | 槽位 | 可中断 |
|:----:|------|---------|:------:|:----:|:------:|
| 1 | **逻辑准备** | 扫描、过滤、去重 | 无 | 不占 | — |
| 2 | **资源准备** | 写快照、加密 | 有 | 不占 | ❌ |
| 3 | **上传提交** | 上传、写记录 | 有 | **占用** | ✅ 可抢占 |

> **优先级设计**：备份任务优先级最低。当系统资源紧张时，备份任务的上传槽位会被手动上传等高优先级任务抢占。被抢占后任务标记为 `Paused` 或 `Preempted`，等待资源释放后自动恢复。

---

## 六、双向同步 (Sync)

### 6.1 三阶段同步模型

```
Stage 1: Snapshot（快照）
├─ 扫描本地文件系统
├─ 扫描远端文件系统（根据 SyncIntent）
│   └─ Watch 触发 → scan_remote=false（仅上传）
│   └─ Poll/Manual 触发 → scan_remote=true（完整同步）
│   └─ needs_full_sync=true → 强制完整同步
└─ 生成双端文件列表

Stage 2: Plan（计划）
├─ 比对双端快照
├─ 检测冲突 → 应用 SyncConflictStrategy
│   ├─ NewerWins（默认，按 mtime）
│   ├─ LocalWins
│   ├─ RemoteWins
│   └─ Skip
├─ 生成同步计划（上传/下载/跳过/冲突列表）
└─ 初始化模式: AutoDetect / AdoptBothSides

Stage 3: Execute（执行）
├─ 执行上传传输（UploadManager）
├─ 执行下载传输（DownloadManager）
├─ 更新 SyncState（SQLite 持久化）
└─ 写 Tombstone 处理删除同步
```

### 6.2 SyncIntent 意图合并机制

Watch 和 Poll 事件可能同时触发，通过意图位图合并：

```rust
pub struct SyncIntent {
    needs_upload: bool,   // Watch 事件设此位
    needs_download: bool, // Poll 事件设此位
    full_sync: bool,      // 手动触发或 needs_full_sync
}
```

- Watch 事件 → `merge_watch()` → 设 `needs_upload=true`
- Poll 事件或手动触发 → `merge_full_sync()` → 设 `full_sync=true`
- 执行时取意图 → 有 full_sync 则双向扫描，否则仅扫描本地
- 任务执行完成后检查意图 → 有残留意图则自动重新触发

### 6.3 SyncState

- 每个文件的同步状态存储在独立的 `sync_state.db`（SQLite）
- 记录文件的 mtime、size、fs_id 等元数据
- Tombstone 机制处理文件删除同步
- `reset_sync_state()` API 可重置状态，触发全量重同步

---

## 七、断点续传与容错

### 7.1 SQLite 持久化

**表结构：**

| 表名 | 用途 | 关键字段 |
|------|------|----------|
| `backup_tasks` | 备份主任务 | id, config_id, status, trigger_type, 计数, 字节数 |
| `backup_file_tasks` | 文件子任务 | id, parent_task_id, status, file_path, remote_path, related_task_id |

### 7.2 服务重启恢复流程

```
AutoBackupManager::new()
│
├─ 1. load_configs() — 从 JSON 配置文件加载
├─ 2. restore_incomplete_tasks() — 恢复未完成任务
│   ├─ 从 backup_tasks 表加载非终态任务
│   ├─ 从 backup_file_tasks 表加载文件子任务
│   ├─ 重置 Preparing/Transferring → Queued
│   ├─ 重建 related_task_id 映射
│   └─ 回填 pending_files
├─ 3. sync_completed_backup_tasks_from_history() — 兜底同步
│   └─ 从 task_history 同步已归档但未更新的任务
└─ 4. start_event_consumer() → resume_queued_tasks_on_startup()
    └─ 自动触发所有 Queued 任务的执行
```

### 7.3 兜底同步机制

为防止数据库与 WAL、元数据不一致，服务重启时执行兜底同步：
- 查询 `task_history` 表中 `is_backup=1, status='completed'` 的任务
- 根据 `related_task_id` 更新 `backup_file_tasks` 中对应任务的状态
- 重新计算主任务进度

---

## 八、配置冲突校验

### 8.1 冲突类型

```rust
pub enum ConflictType {
    SameDirectionParentExists,  // 同方向父目录已存在
    SameDirectionChildExists,   // 同方向子目录已存在
    LoopConflict,               // 上传/下载形成闭环
}
```

### 8.2 判定规则

| 场景 | 本地路径 | 云端路径 | 判定 |
|------|---------|---------|------|
| 同方向冲突 | `LocalOverlap` | `RemoteOverlap` | ❌ 拒绝 |
| 反方向闭环 | `LocalOverlap` | `RemoteOverlap` | ❌ 拒绝 |

路径比较规则：
- **本地路径**：统一分隔符、去尾 `\`、大小写不敏感（Windows）、按路径段边界比较
- **云端路径**：统一 `/` 分隔、去尾 `/`、压缩重复 `//`、按路径段边界比较

### 8.3 校验时机

1. **创建配置时** (`validate_for_create`)
2. **更新配置时** (`validate_for_update`)
3. **执行备份前** (`validate_for_execute`)

---

## 九、文件监听模块 (Watcher)

### 9.1 技术实现

基于 `notify` crate 的 `RecommendedWatcher`（Linux 下使用 inotify）。

### 9.2 事件处理

```
notify::Event → FileWatcher::process_event()
├─ 过滤事件类型: Create / Modify / Remove
├─ 过滤有效路径: 隐藏文件、临时文件等
├─ 匹配配置 ID: 查找所属的备份配置
└─ 发送 FileChangeEvent → ChangeAggregator
```

### 9.3 失败检测

- `failure_count` 原子计数器跟踪连续失败次数
- 超阈值时记录告警日志（不自切换轮询，轮询是独立机制）

---

## 十、定时轮询 (PollScheduler)

### 10.1 调度类型

| 类型 | 说明 | 特点 |
|------|------|------|
| **间隔模式** | 固定间隔 + 随机抖动（0-25%） | 防风控识别 |
| **定时模式** | 每日指定时间执行 | 适合低峰期执行 |

### 10.2 全局轮询

每个备份方向（Upload/Download/Sync）有独立的全局轮询触发器：

```
GLOBAL_POLL_UPLOAD_INTERVAL   → 触发所有 Upload 配置
GLOBAL_POLL_UPLOAD_SCHEDULED
GLOBAL_POLL_DOWNLOAD_INTERVAL → 触发所有 Download 配置
GLOBAL_POLL_DOWNLOAD_SCHEDULED
GLOBAL_POLL_SYNC_INTERVAL     → 触发所有 Sync 配置
GLOBAL_POLL_SYNC_SCHEDULED
```

通过 `GlobalPollEvent` 统一触发，事件消费循环中筛选匹配方向的所有启用配置。

---

## 十一、加密模块

### 11.1 Age 口令加密

本项目的客户端加密已全面迁移至 **age 加密格式 (age-encryption.org/v1)**，详细迁移记录见 `docs/AGE_MIGRATION_SUMMARY.md`。

- **算法**：age 口令模式 — 内部使用 scrypt (`N=2^18, r=8, p=1`) 做内存硬化口令派生，安全性等同于 Argon2id
- **文件格式**：`age-encryption.org/v1`，标准规范，**不依赖本项目即可解密**
- **扩展名**：`.age`
- **口令存储**：用户提供的口令原样持久化在 `encryption.json` 的 `passphrase` 字段
- **密钥轮换**：不支持密钥历史；更换口令会直接替换当前口令，不兼容旧密钥配置

### 11.2 解密方式

```bash
# 方式 1：使用本项目的 decrypt-cli（自动读取 encryption.json）
decrypt-cli decrypt --key-file encryption.json --in file.age --out file.txt

# 方式 2：使用标准 age CLI（不依赖本项目）
age -d file.age
# 输入口令：encryption.json 中的 passphrase 字段值
```

### 11.3 限制

- Sync 模式暂不支持加密
- 配置创建后 `encrypt_enabled` 不可修改

---

## 十二、去重服务 (Record Manager)

### 12.1 去重策略

1. **head_md5 快速比对**：计算文件头部 4KB 的 MD5
2. **多维匹配**：按 `(config_id, relative_path, file_name, size, head_md5)` 联合匹配
3. **去重记录**：成功后写入 `UploadRecord`，包含路径、大小、md5、云端的 fs_id 和 mtime

### 12.2 增量缓存

`ScanCacheManager` 使用独立的 `scan_cache.db`（SQLite）：
- 记录每个文件的 mtime 和 size
- 下次扫描时比对 → 仅处理变化文件
- 批量 upsert 减少 IO

---

## 十三、关键设计决策

### 13.1 为什么使用 TaskController + task_loop 而非直接 tokio::spawn？

避免同一配置同时运行多个扫描任务导致：
- 文件状态竞态（A 任务扫描到文件 X，B 任务也扫描到）
- 网络连接浪费（两次登录、两次 API 调用）
- 去重失效（双任务同时上传同一文件）

### 13.2 为什么 Watch 和 Poll 使用不同路径？

- **Watch** → 仅处理变化的文件（快路径），适合实时同步少量变更
- **Poll** → 全量扫描（保险路径），确保不会遗漏任何文件
- 两者互补：Watch 负责实时性，Poll 负责完整性

### 13.3 为什么备份优先级最低？

防止用户手动上传/下载任务被备份任务阻塞。当并发槽位不足时，备份任务自动让位给用户主动操作。

---

## 十四、相关配置

### 14.1 备份配置 (`BackupConfig`)

```rust
pub struct BackupConfig {
    pub id: String,
    pub name: String,
    pub local_path: PathBuf,
    pub remote_path: String,
    pub direction: BackupDirection,        // Upload / Download / Sync
    pub watch_config: WatchConfig,          // 监听配置
    pub poll_config: PollConfig,            // 轮询配置
    pub filter_config: FilterConfig,        // 过滤配置
    pub encrypt_enabled: bool,
    pub upload_conflict_strategy: Option<UploadConflictStrategy>,
    pub download_conflict_strategy: Option<DownloadConflictStrategy>,
    pub sync_conflict_strategy: Option<SyncConflictStrategy>,
    pub sync_init_mode: Option<SyncInitMode>,
    pub needs_full_sync: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

### 14.2 冲突策略默认值

| 方向 | 默认策略 | 说明 |
|------|---------|------|
| Upload | `SmartDedup` | 智能去重，相同文件不重复上传 |
| Download | `Overwrite` | 覆盖本地已有文件 |
| Sync | `NewerWins` | 较新版本优先 |

---

## 十五、相关文件索引

| 文件 | 说明 |
|------|------|
| `backend/src/autobackup/manager.rs` | 核心协调器，~9000+ 行，包含配置/任务管理、扫描、传输执行 |
| `backend/src/autobackup/scheduler/task_controller.rs` | 并发控制核心，约 300 行 |
| `backend/src/autobackup/scheduler/change_aggregator.rs` | 事件聚合器，防抖去重 |
| `backend/src/autobackup/scheduler/poll_scheduler.rs` | 定时轮询调度器 |
| `backend/src/autobackup/scheduler/backup_scheduler.rs` | 三阶段执行调度器 |
| `backend/src/autobackup/watcher/file_watcher.rs` | 文件系统监听 |
| `backend/src/autobackup/sync/plan.rs` | 同步计划生成 |
| `backend/src/autobackup/sync/state_manager.rs` | 同步状态管理 |
| `backend/src/autobackup/sync/intent.rs` | Sync 意图位图 |
| `backend/src/autobackup/record/record_manager.rs` | 去重服务 |
| `backend/src/autobackup/scan_cache.rs` | 增量扫描缓存 |
| `backend/src/autobackup/persistence.rs` | SQLite 持久化 |
| `backend/src/autobackup/validation.rs` | 配置冲突校验 |
| `backend/src/autobackup/error.rs` | 错误处理 |
| `backend/src/autobackup/events.rs` | WebSocket 事件 |
| `backend/src/autobackup/config.rs` | 配置数据结构 |
| `backend/src/autobackup/task.rs` | 任务数据结构 |
