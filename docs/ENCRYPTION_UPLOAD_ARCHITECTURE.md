# 加密上传模块架构分析（历史设计文档）

> 本文保留的是早期设计记录，其中的 AES/ChaCha 双算法、`master_key`、密钥轮换和旧文件兼容路径均已废弃。当前 `age` 分支只使用用户提供的 passphrase，通过 `age::Encryptor::with_user_passphrase` 生成 `.age` 文件；没有自动生成口令、算法选择或历史密钥回退。当前实现以 `backend/src/encryption` 和 `decrypt-cli` 为准。

---

## 一、整体架构

加密上传功能涉及三个核心模块的协同工作：

```
用户配置 BackupConfig.encrypt_enabled = true
        │
        ▼
┌──────────────────────────────────────────────────────────┐
│                  AutoBackupManager                       │
│  (自动备份管理器：调度、加密、通知)                        │
│                                                          │
│  1. 扫描文件                                              │
│  2. 去重检查                                              │
│  3. 加密文件 → 临时目录                                    │
│  4. 调用 UploadManager 上传加密文件                        │
│  5. 更新加密快照记录                                        │
└─────────────────────────┬────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────┐          ┌──────────────────────┐
│ EncryptionService │          │    UploadManager      │
│ (加密/解密引擎)    │          │  (多线程上传管理器)    │
│                   │          │                      │
│ AES-256-GCM       │          │ 秒传 → 分片上传 → 合并 │
│ ChaCha20-Poly1305 │          │                      │
└──────────────────┘          └──────────────────────┘
          │
          ▼
┌──────────────────┐
│ SnapshotManager   │
│ (加密快照记录)     │
│                   │
│ SQLite 持久化      │
│ encryption_snapshots  │
└──────────────────┘

解密端（独立 CLI 工具）：
┌──────────────────────────────────────────────┐
│               decrypt-cli                     │
│                                               │
│ 加密文件.dat → 解析文件头 → 逐块解密 → 原始文件   │
│                                               │
│ 依赖：encryption.json（密钥）+ mapping.json（映射）│
└──────────────────────────────────────────────┘
```

---

## 二、加密文件格式

### 2.1 文件头（31 字节固定头）

```
偏移   大小    字段          说明
──────────────────────────────────────
0      6      magic         魔数 0xA3 0x7F 0x2C 0x91 0xE4 0x5B
6      1      algorithm     算法标识（0=AES256GCM, 1=ChaCha20Poly1305）
7      12     master_nonce  主随机数
19     8      original_size 原始文件大小（小端序 u64）
27     4      total_chunks  总分块数（小端序 u32）
```

### 2.2 分块格式（每块变长）

```
每个分块：
┌─────────────┬──────────────┬──────────────────┐
│ chunk_nonce │ ciphertext_len│   ciphertext     │
│  12 bytes   │  4 bytes (LE)│  ciphertext_len B │
└─────────────┴──────────────┴──────────────────┘
```

### 2.3 设计要点

- **魔数**使用伪随机字节 `0xA3 0x7F 0x2C 0x91 0xE4 0x5B`，避免被识别为加密文件
- 加密文件名改为 UUID 格式，**".dat" 扩展名**，原始文件名及路径仅保存在映射表中
- 分块 Nonce 由主 Nonce 派生：`derive_chunk_nonce(master_nonce, chunk_index)`
- 每个分块使用独立的 Nonce，满足 AES-GCM 安全要求

---

## 三、核心流程详解

### 3.1 完整的加密上传流水线

```
┌─────────────────────────────────────────────────────────────┐
│  步骤 1：扫描本地目录（BackupConfig.encrypt_enabled = true） │
│   - 扫描本地文件，计算 head_md5（用于去重）                   │
│   - 标记每个文件任务 encrypted = true                        │
│   - 远程路径加密处理：文件夹名也加密                           │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  步骤 2：执行备份任务                                        │
│   - 创建 BackupTask，状态 Queued → Preparing → Transferring  │
│   - 遍历每个文件任务，调用 prepare_file_for_encrypted_upload() │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  步骤 3：prepare_file_for_encrypted_upload()                │
│                                                             │
│  3a. 去重检查（check_upload_record_preliminary）              │
│      - 查询 backup_records 表（config_id + relative_path +  │
│        file_name + file_size + head_md5）                    │
│      - 命中 → 跳过 (Skipped: AlreadyExists)                 │
│      - 未命中 → 继续                                         │
│                                                             │
│  3b. 如果不需要加密（!file_task.encrypted）                    │
│      - 直接返回，走普通上传路径                                │
│                                                             │
│  3c. 加密准备                                                │
│      - 生成/复用加密文件名（UUID.dat 格式）                    │
│      - 计算加密后的远程路径（remote_dir / encrypted_name）      │
│      - 创建 EncryptionSnapshot 记录到 SQLite（状态 pending）   │
│      - 更新快照状态为 encrypting                              │
│      - 更新文件任务状态为 Encrypting                           │
│      - 发送 WebSocket 事件：FileEncrypting                    │
│                                                             │
│  3d. 执行加密（EncryptionService.encrypt_file_with_progress）  │
│      - 输入：原始文件 local_path                              │
│      - 输出：temp_dir / encrypted_name（临时加密文件）          │
│      - 分块加密（16MB/块），带进度回调                          │
│      - 发送 WebSocket 事件：FileEncryptProgress               │
│                                                             │
│  3e. 加密完成                                                │
│      - 更新文件任务：encrypted_name、temp_encrypted_path       │
│      - 更新快照状态为 uploading                               │
│      - 发送 WebSocket 事件：FileEncrypted                     │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  步骤 4：创建上传任务（create_upload_tasks_for_files）        │
│                                                             │
│  调用 UploadManager.create_backup_task():                    │
│   - 参数：local_path（原始文件路径！）                         │
│   - 参数：remote_path（加密后的远程路径）                      │
│   - 参数：encrypt_enabled = true                             │
│                                                             │
│  注意：create_backup_task 内部会：                            │
│   - 再次生成 encrypted_filename（UUID.dat）                   │
│   - 覆盖 remote_path 为加密路径                               │
│   - 创建加密快照记录（状态 pending）                           │
│   - 上传完成后更新 nonce/algorithm 字段并标记 completed        │
│                                                             │
│  **关键问题**：加密后的临时文件路径不作为 local_path 传入，      │
│   而是仍传原始文件路径。上传引擎每次分片时重新读取原始文件加密。    │
│   还是说上传引擎直接读临时加密文件？这里需要进一步确认。          │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  步骤 5：执行上传                                            │
│   - UploadEngine.upload()                                   │
│   - 秒传检查（RapidUploadChecker）                           │
│   - 预创建文件（precreate）                                  │
│   - 并发分片上传（Semaphore + JoinSet）                       │
│   - 合并分片创建文件（create_file）                           │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  步骤 6：清理和完成                                          │
│   complete_encrypted_upload(task_id, file_task_id, success)  │
│   - 成功：更新快照状态为 completed                            │
│   - 失败：更新快照状态为 failed                              │
│   - 清理 temp_dir 中的临时加密文件                            │
└─────────────────────────────────────────────────────────────┘
```

---

### 3.2 加密服务（EncryptionService）

**位置：** `backend/src/encryption/service.rs`

**支持的算法：**

| 算法 | 密钥长度 | Nonce 长度 | 认证标签 | 性能 |
|------|---------|-----------|---------|------|
| AES-256-GCM | 32 字节 | 12 字节 | 16 字节 | ⭐ 硬件加速快 |
| ChaCha20-Poly1305 | 32 字节 | 12 字节 | 16 字节 | ⭐ 无硬件加速时快 |

**加密方式：**

```
内存加密（小数据）:
  encrypt(&[u8]) → EncryptedData { ciphertext, nonce, algorithm, version }
  decrypt(&EncryptedData) → Vec<u8>

文件分块加密（大文件）:
  encrypt_file_chunked(input_path, output_path) → EncryptionMetadata
  encrypt_file_with_progress(input, output, callback) → EncryptionMetadata
    ↓
  内部：encrypt_file_chunked_with_progress
  分块大小：16MB（DEFAULT_CHUNK_SIZE）
  每块派生独立 Nonce，防止 overflow
```

---

### 3.3 密钥管理

**位置：** `backend/src/encryption/config_store.rs`

**存储文件：** `config/encryption.json`

```json
{
  "current_key": {
    "master_key": "<Base64 编码的 32 字节密钥>",
    "algorithm": "aes256gcm",
    "key_version": 1,
    "created_at": 1700000000000,
    "last_used_at": 1700000000000
  },
  "key_history": [
    // 旧密钥（用于解密旧文件）
    {
      "master_key": "...",
      "algorithm": "aes256gcm",
      "key_version": 1,
      "created_at": 1690000000000,
      "last_used_at": 1699000000000,
      "deprecated_at": 1700000000000
    }
  ]
}
```

**密钥轮换机制：**
- `current_key`：当前使用的密钥，新加密的文件使用此密钥
- `key_history`：历史密钥列表，保留用于解密旧文件（不可删除）
- `created_at` / `deprecated_at` 记录密钥生命周期
- 上传文件时，快照记录中保存 `key_version`，解密时据此选择对应密钥

---

### 3.4 文件夹名加密

加密上传时，不仅文件名被替换为 UUID，**文件夹路径也会被加密**：

```
原始路径: /我的文档/工作报告/2024/财务.xlsx
          ↓
加密路径: /a1b2c3d4-e5f6/70123456-789a/2024/uuid.dat
```

加密方式：对每个路径组件（最后一个分隔符前的所有部分）进行哈希截断 + UUID 映射，通过 SQLite 持久化映射关系。

实现位置：
- `AutoBackupManager::encrypt_folder_path_static()`（扫描阶段）
- `UploadManager::encrypt_folder_path_for_upload()`（上传阶段直接调用）

---

### 3.5 去重机制

**位置：** `AutoBackupManager::prepare_file_for_encrypted_upload()` 阶段 1

去重使用 `backup_records` 表，匹配条件：
1. `config_id` — 同一备份配置
2. `relative_path` — 相对路径一致
3. `file_name` — 文件名一致
4. `file_size` — 文件大小一致
5. `head_md5` — 文件头 MD5（前 4KB 的 MD5 哈希）

注意：这是**初步去重**（快速检查），非完整文件哈希。场景是防止同一文件被多次重复加密上传。

---

## 四、解密端（decrypt-cli）

**位置：** `decrypt-cli/src/`

### 4.1 工作模式

```
批量解密（推荐）:
  decrypt-cli decrypt \
    --key-file encryption.json \   # 密钥配置
    --map mapping.json \            # 加密映射表
    --in-dir ./encrypted \           # 加密文件目录
    --out-dir ./decrypted            # 解密输出目录
    --mirror                         # 使用镜像/原始目录结构

单文件解密:
  decrypt-cli decrypt \
    --key-file encryption.json \
    --in file.dat \
    --out file.txt \
    --key-version 2                  # 可选，指定密钥版本
```

### 4.2 解密流程

```
解密引擎 decrypt_file():
  1. 打开加密文件
  2. 解析 31 字节文件头 → 获取 magic / algorithm / master_nonce / original_size / total_chunks
  3. 验证 magic 是否匹配
  4. 根据 algorithm 选择 AES-256-GCM 或 ChaCha20-Poly1305
  5. 逐块读取：
     a. 读取 12 字节 chunk_nonce
     b. 读取 4 字节 ciphertext_len
     c. 读取 ciphertext_len 字节密文
     d. 使用 chunk_nonce 解密该块
     e. 写入解密数据到输出文件
  6. 刷新输出文件

多密钥尝试:
  decrypt_file_with_any_key():
  - 遍历所有可用密钥尝试解密
  - 第一个解密成功的密钥即为正确密钥
  - 全部失败 → KeyMismatch 错误
```

### 4.3 依赖的外部文件

| 文件 | 来源 | 用途 |
|------|------|------|
| `encryption.json` | 服务器配置目录 | 包含当前密钥 + 历史密钥 |
| `mapping.json` | 从 API 导出 | 加密文件名 → 原始路径映射 |

**如何准备这些文件：**

通过 Web API 导出解密包：
```
POST /api/v1/encryption/export-bundle  → 返回包含两者的 zip 包
GET  /api/v1/encryption/export-keys    → 仅导出 encryption.json
GET  /api/v1/encryption/export-mapping  → 仅导出 mapping.json
```

---

## 五、数据流全景

```
备份前                                 备份后
──────                                 ──────
本地文件系统                           百度网盘
                                       
/我的文档/                              /备份文件夹/
  ├── 照片/                              ├── a1b2c3d4-...(加密文件夹名)/
  │   ├── 旅行.jpg  ← 加密 →             │   ├── uuid-1.dat
  │   └── 家人.jpg                       │   └── uuid-2.dat
  └── 文档/                              └── uuid-3.dat(加密文件夹名)/
      └── 重要.txt                            └── uuid-4.dat

SQLite 数据库 (encryption_snapshots 表):
┌──────────────┬────────────┬──────────────┬──────────┐
│ encrypted_name │ original  │ original_name│ key_ver  │
├──────────────┼────────────┼──────────────┼──────────┤
│ uuid-1.dat   │ /照片/     │ 旅行.jpg     │ 1        │
│ uuid-2.dat   │ /照片/     │ 家人.jpg     │ 1        │
│ uuid-3.dat   │ /文档/     │ 重要.txt     │ 2        │
└──────────────┴────────────┴──────────────┴──────────┘

本地临时目录 (backup temp/):
  └── uuid-1.dat  ← 加密后的临时文件，上传后自动清理
  └── uuid-2.dat
  └── ...

解密时:
  decrypt-cli + encryption.json(密钥) + mapping.json(映射)
  → uuid-1.dat → /照片/旅行.jpg
```

---

## 六、关键设计决策

### 6.1 客户端加密而非服务端加密

```
文件加密发生在本地 → 加密文件上传到百度网盘
                  → 百度无法解密（没有密钥）
                  → 即使百度账号泄露，攻击者也读不了文件内容
```

**优点：**
- 零信任：数据在离开本机前已加密
- 百度无法查看用户文件内容
- 密钥由用户独立管理

**代价：**
- 无法在线预览
- 需要额外的解密工具（decrypt-cli）
- 密钥丢失 = 数据永久丢失

### 6.2 文件名混淆

```
原始:  财务报告_2024_季度数据.xlsx
加密:  7c9d8e3f-1a2b-4c5d-8e7f-6a5b4c3d2e1f.dat
```

- 百度只看到 UUID.dat，不知道文件内容和类型
- 文件夹名也被加密，目录结构不可见
- 映射关系仅存在于用户本地的 SQLite 中

### 6.3 分块加密设计

```
文件头 (31 bytes) + [chunk_nonce(12) + chunk_len(4) + chunk_ciphertext(N)]×N
```

- 每个分块独立加密：即使部分损坏，其他分块仍可解密
- 分块 Nonce 由主 Nonce + 分块索引派生：`SHA256(master_nonce ‖ chunk_index)[:12]`
- 支持超大文件（分块大小 16MB，总分块数 u32 上限约 4B，即最大 64EB）

---

## 七、安全注意事项

| 注意点 | 说明 |
|--------|------|
| **密钥保管** | 密钥仅存储在本地 `encryption.json`，丢失不可恢复。建议导出 `encryption.json` 和 `mapping.json` 到安全位置 |
| **临时文件** | 加密后的临时文件在 `temp_dir` 中，上传成功后自动清理。如果上传中断，临时文件可能残留 |
| **密钥轮换** | 密钥轮换后，旧文件仍使用旧密钥加密，解密时需要 `key_history` 中的旧密钥。新旧并行使用时注意导出完整密钥链 |
| **性能影响** | 加密 + 上传是串行的（先加密再上传），大文件（1GB+）加密会占用 CPU 和磁盘 IO |
| **去重限制** | 仅使用 head_md5（前 4KB）做初步去重，碰撞概率极低但理论上存在 |

---

## 八、涉及源代码文件清单

| 文件路径 | 职责 |
|---------|------|
| `backend/src/encryption/service.rs` | 核心加密/解密服务 |
| `backend/src/encryption/config_store.rs` | 密钥持久化存储 |
| `backend/src/encryption/snapshot.rs` | 加密快照管理 |
| `backend/src/encryption/export.rs` | 解密包导出（encryption.json + mapping.json） |
| `backend/src/encryption/buffer_pool.rs` | 加密缓冲区复用 |
| `backend/src/autobackup/manager.rs` | 自动备份主控（含加密调度） |
| `backend/src/autobackup/config.rs` | 备份配置（含 encrypt_enabled） |
| `backend/src/autobackup/record.rs` | 记录管理 + 加密快照 SQLite 表 |
| `backend/src/uploader/manager.rs` | 上传管理器（含加密路径处理） |
| `backend/src/uploader/engine.rs` | 上传引擎（并发分片上传） |
| `decrypt-cli/src/decrypt_engine.rs` | 解密引擎 |
| `decrypt-cli/src/file_parser.rs` | 加密文件头解析 |
| `decrypt-cli/src/key_loader.rs` | 密钥加载 |
| `decrypt-cli/src/mapping_loader.rs` | 映射文件加载 |
