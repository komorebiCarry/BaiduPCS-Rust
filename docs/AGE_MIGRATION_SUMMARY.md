# Age 加密格式迁移总结

**迁移日期：** 2026 年 5 月 11 日  
**提交记录：** `ad2c998` — `refactor(encryption): 全面替换为 age 加密格式`  
**涉及文件：** 23 个文件，+4300 / -4592 行

> 当前约束（`age` 分支）：加密只接受用户明确提供的 passphrase。随机口令生成、算法参数、旧 `master_key/current_key/key_history` 配置、密钥历史和兼容回退均已移除；旧配置会被拒绝，必须重新输入用户口令。

---

## 为什么迁移

原来自定义加密格式有 3 个问题：

1. **供应商锁定** — 只能用本项目 `decrypt-cli` 解密，一旦项目停更等于数据死亡
2. **非标容器格式** — 魔数+自研分块布局，`openssl`、`age` 等标准工具无法直接解密
3. **不必要的复杂性** — 同时支持 AES-256-GCM 和 ChaCha20-Poly1305 两种算法，实际只用后者

**age 格式 (age-encryption.org/v1) 解决了这些：**
- 标准规范，Go/Rust/Python/C 等多语言实现
- 可直接用 `age -d` 命令行解密，不依赖本项目
- 内置 scrypt 内存硬化，无需额外配置

---

## 核心改动

### 1. 加密服务重写

**之前：**
```
用户一次登录 → base64密钥(32B随机/口令SHA256派生) → age as passphrase
                                                          ↓
                                                  EncryptionService
                                                  master_key: [u8; 32]
                                                  algorithm: Aes256Gcm | ChaCha20Poly1305
```

**之后：**
```
用户一次登录 → 任意口令 → EncryptionService
                           passphrase: String
                           age 内部 scrypt(N=2^18, r=8, p=1) 自动硬化
```

`backend/src/encryption/service.rs` — 核心改动：

```diff
-pub struct EncryptionService {
-    master_key: [u8; 32],
-    algorithm: EncryptionAlgorithm,
-}
+pub struct EncryptionService {
+    passphrase: String,
+}

 impl EncryptionService {
-    pub fn new(master_key: [u8; 32], algorithm: EncryptionAlgorithm) -> Self
+    pub fn new(passphrase: impl Into<String>, _algorithm: EncryptionAlgorithm) -> Self
```

所有加密/解密操作改为代理到 `age::Encryptor::with_user_passphrase` / `age::Decryptor::Passphrase`。

### 2. 算法枚举精简

`backend/src/autobackup/config.rs`：

```diff
-pub enum EncryptionAlgorithm {
-    Aes256Gcm,
-    ChaCha20Poly1305,
-}
+pub enum EncryptionAlgorithm {
+    Age,
+}
```

全部 20+ 处引用同步替换，零旧变体残留。

### 3. 后端 API 简化

`POST /api/v1/encryption/key/import`

```diff
-{ key: string, algorithm?: string }   // 用户传 base64 + 算法
+{ key: string }                        // 用户传任意口令，无算法参数
```

`POST /api/v1/encryption/key/generate`

```diff
-{ algorithm?: string }
 // 不再需要参数，固定生成随机口令
```

删除了新增后又废弃的 `/encryption/key/derive` 端点（Argon2id 派生，被 age 内置 scrypt 替代）。

### 4. 加密文件扩展名

```diff
-pub const ENCRYPTED_FILE_EXTENSION: &str = ".dat";
+pub const ENCRYPTED_FILE_EXTENSION: &str = ".age";
```

所有引用处同步更新（`UUID.dat` → `UUID.age`）。

### 5. 删除的数据结构

| 结构/函数 | 文件 | 原因 |
|-----------|------|------|
| `MAGIC_V1` | `service.rs` | 自定义魔数不再需要 |
| `DEFAULT_CHUNK_SIZE` | `service.rs` | 自定义分块不再需要 |
| `derive_chunk_nonce()` | `service.rs` | 自定义 nonce 派生不再需要 |
| `EncryptedData.nonce` | `service.rs` | age 内部管理 nonce |
| `EncryptedData.algorithm` | `service.rs` | 唯一算法 |
| `EncryptedData.version` | `service.rs` | age 格式自带版本 |
| `EncryptionMetadata.nonce` | `service.rs` | 同上 |
| `EncryptionMetadata.algorithm` | `service.rs` | 同上 |
| `EncryptionMetadata.version` | `service.rs` | 同上 |
| `generate_master_key()` | `service.rs` | 改为 `generate_random_passphrase()` |
| `FILE_MAGIC` | `types.rs` (decrypt-cli) | 自定义魔数 |
| `FILE_HEADER_SIZE` | `types.rs` (decrypt-cli) | 自定义文件头 |
| `FileHeader` | `types.rs` (decrypt-cli) | 自定义文件头结构 |
| `ChunkReader` | `file_parser.rs` (decrypt-cli) | 自定义分块读取器 |
| `FileHeaderParser` | `file_parser.rs` (decrypt-cli) | 自定义文件头解析器 |

### 6. 新增文件

| 文件 | 内容 |
|------|------|
| `docs/ENCRYPTION_UPLOAD_ARCHITECTURE.md` | 加密上传模块架构分析 |
| `docs/NETWORK_CONFIG_GUIDE.md` | 网络配置指南（host/port/CORS） |
| `docs/SECURITY_REVIEW_AND_ARCHITECTURE.md` | 安全审查与架构分析（初始已有） |
| `docs/SECURITY_IMPROVEMENT_PLAN.md` | 安全改进方案（后经讨论大部分对单用户内网场景不适用） |

---

## 数据流对比

### 之前（自定义格式）

```
原始文件 → [自定义文件头(31B) + 分块×N(16MB/块)] → UUID.dat
                                                      ↓
解密只能使用: decrypt-cli --key-file encryption.json --map mapping.json
```

### 之后（age 格式）

```
原始文件 → [age 标准格式(.age)] → UUID.age
                                    ↓
解密方式 1: decrypt-cli --key-file encryption.json --map mapping.json
解密方式 2: age -d UUID.age                      ← 输入口令即可
解密方式 3: 任何语言标准 age 库 (Go/Rust/Python/JS)
```

---

## 文件权限影响

| 文件 | 之前 | 之后 |
|------|------|------|
| `config/encryption.json` | 存 base64 密钥 + algorithm | 存口令字符串 + algorithm="age" |
| `backend/Cargo.toml` | aes-gcm, chacha20poly1305 | age = "0.10" |
| `decrypt-cli/Cargo.toml` | aes-gcm, chacha20poly1305 | age = "0.10" |

---

## 后续建议

- [ ] 旧的自定义格式备份文件（`.dat`）仍存在，需用旧版 decrypt-cli 先解密再重新用 age 加密
- [ ] 现有 `config/encryption.json` 算法字段为旧值（`AES256-GCM`），新版通过 serde `alias` 兼容读取
- [ ] 口令安全取决于长度和复杂度，建议 16 位以上，age 自带的 scrypt 会补偿弱口令但不建议依赖
