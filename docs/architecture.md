# 百度网盘 Rust 客户端 — 架构说明

> 适用版本：v1.14.x  ·  目标读者：项目贡献者 / 二次开发者

## 1. 总览

项目分三层：

```
┌───────────────────────────────────────────────────────┐
│  frontend  (Vue 3 + Element Plus + Vite)              │
│  - Pinia stores / Axios API / WebSocket 订阅          │
└─────────────── HTTP + WS ───────────────────────────┘
                          │
┌───────────────────────────────────────────────────────┐
│  backend  (axum + tokio)                              │
│  - server/    : HTTP routes + AppState + WS handler   │
│  - auth/      : QR-code / cookie / 账号切换           │
│  - netdisk/   : 百度网盘 API 客户端                    │
│  - transfer/  : 分享转存                              │
│  - downloader/: 多线程分块下载                        │
│  - uploader/  : 分块上传 + 高速分片                   │
│  - encryption/: 客户端加密 (AES-GCM/ChaCha20)         │
│  - autobackup/: 本地 → 网盘 定时/实时备份             │
│  - share_sync/: 第三方分享订阅与持续同步              │
│  - persistence/: SQLite + WAL 任务历史                │
└───────────────────────────────────────────────────────┘
```

## 2. 后端模块依赖

```
server/state (AppState)
   │
   ├── netdisk/client  (NetdiskClient, 多账号 Arc<RwLock<...>>)
   │
   ├── transfer/manager ──→ netdisk/client
   │
   ├── downloader/manager ──→ netdisk/client + persistence
   │
   ├── uploader/manager ──→ netdisk/client + encryption + persistence
   │
   ├── encryption/service ──→ encryption/snapshot
   │
   ├── autobackup/manager
   │       │
   │       ├── scheduler/  (backup_scheduler + poll_scheduler)
   │       ├── sync/       (intent + plan + state_manager)
   │       ├── watcher/    (file_watcher)
   │       ├── record/     (record_manager: dedup)
   │       ├── priority/   (policy: 上传任务降级)
   │       ├── scan_cache/
   │       └── health.rs   (TCP 探测 + 磁盘检查)
   │
   ├── share_sync/manager
   │       │
   │       ├── snapshot/   (递归 list, glob→regex 过滤)
   │       ├── diff/       (BTreeMap 比较)
   │       ├── executor/   (应用 diff 到目标)
   │       ├── persistence/(subscriptions/snapshots/runs SQLite)
   │       ├── scheduler/  (每订阅独立轮询)
   │       └── events/     (WS 事件)
   │
   ├── web_auth/      (Web 端鉴权中间件 + TOTP + 恢复码)
   │
   └── common/        (ProxyConfig / 内存监控 / 路径工具)
```

## 3. 关键流程

### 3.1 分享同步（v2）

```
用户配置订阅
   │
   ↓
ShareSyncManager.create_subscription(sub)
   │ 1. 校验 share_url（必须是 pan.baidu.com/s/<short_key>）
   │ 2. 持久化到 JSON + SQLite
   │ 3. 启 Scheduler（≥10 分钟轮询）
   ↓
SubscriptionScheduler.on_tick
   ↓
ShareSyncManager.execute_one(id)
   │ 1. SnapshotCollector 递归 list
   │ 2. 用 include_paths BTreeSet 索引 + exclude RegexSet 过滤
   │ 3. latest_snapshot(prev) → diff
   │ 4. ShareSyncExecutor.apply_with_run_id(diff)
   │ 5. 失败项不阻塞整体；只对"全成功"才 save_snapshot 推进基线
   ↓
WebSocket 推送 DiffDetected / RunCompleted
```

### 3.2 自动备份

```
BackupConfig 启动
   ↓
FileWatcher 监听 + PollScheduler 兜底
   ↓
ChangeAggregator 合并
   ↓
sync::plan 生成 SyncPlan
   ↓
PriorityManager 排队（下载 > 上传 > 备份）
   ↓
Uploader/Downloader 执行
   ↓
record_manager 记 dedup
```

### 3.3 多账号切换

```
auth/session 持有 Arc<RwLock<HashMap<id, Bduss>>>
AppState.netdisk_client = Arc<RwLock<Option<NetdiskClient>>>
切换账号时重建 NetdiskClient + TransferManager / DownloadManager
   ↓
WS 推送 AccountSwitchedEvent，前端切换 dialog
```

## 4. 数据持久化

| 模块         | 存储            | 路径                                |
|--------------|----------------|-------------------------------------|
| autobackup   | SQLite (rusqlite) | `config/persistence.db`           |
| share_sync   | SQLite         | `config/share_sync/share_sync.db`    |
| share_sync   | JSON           | `config/share_sync/subscriptions.json` |
| 全局         | SQLite + WAL   | `config/history.db`                  |

## 5. WebSocket 事件总线

`server/events/types.rs::TaskEvent` 是顶层枚举：

```rust
pub enum TaskEvent {
    Download(DownloadEvent),
    Upload(UploadEvent),
    Transfer(TransferEvent),
    Backup(WsBackupEvent),
    CloudDl(CloudDlEvent),
    ShareSync(ShareSyncWsEvent),  // v2 新增
    Auth(AuthWsEvent),            // 多账号切换
    // ...
}
```

每条新增变体必须在 `server/websocket/manager.rs` 的 match arm 中实现 broadcast，
否则订阅者收不到事件。

## 6. 性能关键点

| 点                                | 位置                              | 说明 |
|----------------------------------|----------------------------------|------|
| 抓取 → 过滤 include              | `share_sync/snapshot.rs`         | include_paths 预构建 BTreeSet 索引（O(log N)） |
| 抓取 → 排除 exclude              | `share_sync/snapshot.rs`         | glob 编译为 RegexSet（O(L) DFA 匹配） |
| 下载分块                          | `downloader/chunk.rs`            | tokio + reqwest stream + 并发分块 |
| 上传分片                          | `uploader/chunk.rs`              | 并发分片 + 失败重传 |
| 轮询任务调度                      | `task_slot_pool.rs`              | 优先级队列 + 槽位抢占 |
| 内存监控                          | `common/memory_monitor.rs`       | sysinfo + 自适应限流 |

## 7. 已识别并完成的可优化点（v1.14.2）

| 类型     | 改动                                                        | 收益 |
|----------|-------------------------------------------------------------|------|
| 性能     | 抓取阶段 glob 匹配换成 `regex::RegexSet`                    | O(2^L) → O(L) |
| 性能     | `include_paths` 预计算祖先索引集                            | O(N·M) → O(log N) |
| 健壮性   | `share_url` 校验加固（防同形钓鱼 + 短 short_key 拒绝）      | 安全 |
| 健壮性   | `SnapshotCollector` 用预计算索引代替线性扫                  | 性能 + 正确性 |
| 健壮性   | `autobackup::health.rs` 子模块，TCP 真实网络探测            | 解决 TODO |
| 健壮性   | 前端 `as any` 收敛为类型化（SyncTarget / AxiosError / TreeNode） | 类型安全 |
| 项目     | `[profile.release]` 加 lto+strip+panic=abort                | 二进制 -30~50% |
| 项目     | CI 加 `cargo test` + `vue-tsc` + 前端 build                 | 回归门 |
| 项目     | `autobackup::manager.rs` 顶部加 Module Map 注释             | 可读性 |
| 项目     | 抽出 `autobackup::health.rs` 子模块                         | 大文件拆分第 1 步 |

## 8. 扩展点

- **新增持久化表**：在 `persistence::` 下加表 + 索引，导出函数到 `mod.rs`。
- **新增 WS 事件**：在 `server/events/types.rs::TaskEvent` 加变体，并在
  `server/websocket/manager.rs` match arm 中加 broadcast 调用。
- **新增定时任务**：参考 `share_sync::scheduler::SubscriptionScheduler` 模板，
  每个 manager 持有 `DashMap<id, Scheduler>`，shutdown 时遍历停。
- **新增同步策略**：实现 `executor::ExecutorHooks` trait，在 `share_sync::manager::ProductionHooks`
  中委托给具体的 TransferManager / DownloadManager 调用。
