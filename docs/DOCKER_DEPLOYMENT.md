# Docker 容器部署概览

> 最后更新: 2026-05-17
>
> 本文档梳理了 `BaiduPCS-Rust` 项目当前 Docker 容器的运行模式、镜像构建方式以及各环境的部署配置。

---

## 目录

1. [当前运行状态](#1-当前运行状态)
2. [容器详情](#2-容器详情)
3. [前端容器的作用](#3-前端容器的作用)
4. [Docker Compose 配置族](#4-docker-compose-配置族)
5. [Dockerfile 构建策略](#5-dockerfile-构建策略)
6. [辅助脚本](#6-辅助脚本)
7. [数据持久化与挂载](#7-数据持久化与挂载)
8. [网络模式对比](#8-网络模式对比)
9. [开发工作流](#9-开发工作流)

---

## 1. 当前运行状态

| 容器名 | 容器 ID | 镜像 | 状态 | 启动时间 |
|--------|---------|------|------|----------|
| `baidu-netdisk-rust-dev` | `2a6fe6f1dcb7` | `baidupcs-rust-baidu-netdisk:latest` | **Up 2 days (healthy)** | 2026-05-12 |
| `baidu-netdisk-frontend-dev` | `128f46c35cc0` | `baidupcs-rust-frontend-dev:latest` | **Exited (143)** (2 天前) | 2026-05-12 |

> **说明：**
>
> - 后端容器正常运行中，健康检查通过。前端容器已退出（Exit Code 143 = SIGTERM），通常由 `docker compose down` 或 `docker stop` 触发。
> - 前端容器退出**并不影响网页访问**，因为 Rust 后端内置了静态文件托管——它会自动检测并服务 `frontend/dist/` 目录下的编译产物。详见下方 [#前端容器的作用](#前端容器的作用)。
> - 目前实际只使用了**开发环境**的 Docker Compose 配置（`docker-compose.dev.yml`）。

---

## 2. 容器详情

### 2.1 后端 `baidu-netdisk-rust-dev`

| 属性 | 值 |
|------|-----|
| **镜像** | `baidupcs-rust-baidu-netdisk:latest` (90 MB) |
| **入口命令** | `./target/release/baidu-netdisk-rust` |
| **网络模式** | `host`（无网络隔离） |
| **端口映射** | 无（host 网络下直接监听宿主机端口 18888） |
| **重启策略** | `unless-stopped` |
| **工作目录** | `/app/backend` |

**环境变量：**

```ini
RUST_LOG=debug
RUST_BACKTRACE=1
```

**挂载卷（均为 bind mount）：**

| 宿主机路径 | 容器路径 | 用途 |
|-----------|---------|------|
| `BaiduPCS-Rust/`（项目根目录） | `/app` | 源码与编译产物 |
| `BaiduPCS-Rust/data/` | `/app/data` | 会话与应用数据 |
| `BaiduPCS-Rust/logs/` | `/app/logs` | 运行日志 |
| `BaiduPCS-Rust/wal/` | `/app/wal` | WAL 持久化 |
| `/home/dreamydust` | `/home/dreamydust` | 宿主机家目录直通 |
| `/mnt/BackupHDD` | `/mnt/BackupHDD` | 外置备份硬盘直通 |

### 2.2 前端 `baidu-netdisk-frontend-dev`

| 属性 | 值 |
|------|-----|
| **镜像** | `baidupcs-rust-frontend-dev:latest` (354 MB) |
| **入口命令** | `npm run dev -- --host 0.0.0.0` |
| **网络模式** | `host` |
| **状态** | Exited (143) — 收到 SIGTERM 退出 |
| **重启策略** | `unless-stopped` |

**环境变量：**

```ini
NODE_ENV=development
VITE_API_PROXY_TARGET=http://localhost:18888
NODE_VERSION=18.20.8
YARN_VERSION=1.22.22
```

**挂载卷：**

| 宿主机路径 | 容器路径 | 用途 |
|-----------|---------|------|
| `BaiduPCS-Rust/frontend/` | `/app` | 前端源码（热更新） |
| 匿名卷（`/var/lib/docker/volumes/...`） | `/app/node_modules` | 避免覆盖宿主机 node_modules |

---

## 3. 前端容器的作用

> 核心问题：前端容器已退出，为什么 18888 端口还能正常访问网页？

### 3.1 Rust 后端自带静态文件托管

`BaiduPCS-Rust` 的 Rust 后端（`baidu-netdisk-rust`）**内置了前端静态资源托管功能**。在 `backend/src/main.rs` 中有一个 `detect_frontend_dir()` 函数，启动时按优先级自动检测 `frontend/dist/` 目录：

```rust
// backend/src/main.rs 中的候选路径（按优先级）
1. ./frontend/dist          // 开发环境标准路径
2. ./frontend               // GitHub Actions 打包路径
3. ../frontend/dist         // 开发环境，源码目录结构
4. ../frontend              // GitHub Actions 打包路径（上级目录）
5. /app/frontend/dist       // Docker 容器标准路径
6. /app/frontend            // Docker 容器 GitHub 打包路径
// ...更多备选路径
```

找到 `index.html` 后，通过 `tower_http::services::ServeDir` 将整个 `frontend/dist/` 作为静态资源提供服务。**因此只要 `frontend/dist/` 存在且包含编译产物，无需前端容器就能直接访问网页。**

### 3.2 前端容器的角色：Vite 开发服务器

| 特性 | Rust 托管静态文件 | 前端 Vite 开发容器 |
|------|------------------|-------------------|
| **端口** | 18888（后端同一端口） | 5173 |
| **文件来源** | `frontend/dist/` 编译产物 | Vite 开发服务器内存编译 |
| **前端热更新** | ❌ 需 `npm run build` 重编译 | ✅ 修改源码即时生效（HMR） |
| **依赖** | 仅需编译产物，无运行时依赖 | 需要 Node.js 18 + npm 包 |
| **适用场景** | **日常使用 / 生产部署** | **前端开发调试** |

### 3.3 典型场景对比

```bash
# 场景 A：只是用网页版百度网盘（当前状态）
# → 后端容器就够了，前端容器不需要
curl http://localhost:18888     # ✅ 正常访问

# 场景 B：前端开发，需要热更新调试
docker compose -f docker-compose.dev.yml up -d frontend-dev
# 访问 http://localhost:5173，Vite 代理 API 请求到 http://localhost:18888
```

### 3.4 各部署方式的前端处理

| 部署方式 | 前端如何被服务 |
|---------|--------------|
| **生产 Docker 镜像** (`Dockerfile`) | 多阶段构建中 `frontend-builder` 编译前端 → 产物复制到 runtime 镜像 → Rust 后端托管 |
| **开发 Docker 容器** (`docker-compose.dev.yml`) | 宿主机 `npm run build` 输出到 `frontend/dist/` → bind mount 注入容器 → Rust 后端托管 |
| **前端 Vite 开发容器** (`docker-compose.dev.yml`) | Vite 开发服务器（端口 5173）提供 HMR，API 代理到后端 18888 |

---

## 4. Docker Compose 配置族

项目提供了 **三套** Docker Compose 文件，覆盖不同场景：

### 3.1 `docker-compose.yml` — 生产部署（桥接网络）

```yaml
# 核心特征：
# - bridge 网络 + 端口映射 18888:18888
# - 多阶段构建（Dockerfile）
# - 资源限制（2 CPU / 2GB 内存）
# - 健康检查
# - 独立网络 baidu-network
```

| 服务名 | 说明 |
|--------|------|
| `baidu-netdisk` | 生产后端（桥接模式） |

**用法：**

```bash
docker compose build
docker compose up -d
```

### 3.2 `docker-compose.dev.yml` — 开发环境（无隔离模式）

```yaml
# 核心特征：
# - network_mode: host —— 容器网络直通宿主机
# - 整个项目根目录挂载到 /app
# - 额外挂载 /home/dreamydust 和 /mnt/BackupHDD
# - 不包含构建步骤，编译产物从宿主机直接使用
# - 附带前端开发服务（Vite hot-reload）
```

| 服务名 | 容器名 | 说明 |
|--------|--------|------|
| `baidu-netdisk` | `baidu-netdisk-rust-dev` | 后端（无隔离） |
| `frontend-dev` | `baidu-netdisk-frontend-dev` | 前端（Vite 热更新） |

**用法：**

```bash
cd backend && cargo build --release                    # 宿主机编译
docker compose -f docker-compose.dev.yml up -d         # 启动容器
docker compose -f docker-compose.dev.yml restart baidu-netdisk  # 重启后端
```

### 3.3 `docker-compose.image.yml` — 预构建镜像部署

```yaml
# 核心特征：
# - 使用预构建镜像 komorebicarry/baidupcs-rust:latest
# - 与 docker-compose.yml 相同的网络、卷、资源限制配置
# - 无需本地构建
```

**用法：**

```bash
docker compose -f docker-compose.image.yml up -d
```

---

## 4. Dockerfile 构建策略

### 4.1 `Dockerfile` — 多阶段生产构建

```
┌──────────────────────┐
│  Stage 1             │
│  frontend-builder    │  node:18-alpine → npm ci → npm run build
│  (Node.js 18)        │  产物: frontend/dist/
└─────────┬────────────┘
          │
┌─────────▼────────────┐
│  Stage 2             │
│  backend-builder     │  rust:1.87-slim → cargo build --release
│  (Rust 1.87)         │  产物: target/release/baidu-netdisk-rust
└─────────┬────────────┘
          │
┌─────────▼────────────┐
│  Stage 3 (runtime)   │
│  debian:bookworm-    │  仅安装运行时依赖（ca-certificates,
│  slim                │  libssl3, curl）
│                      │  复制前后端产物
│  最终镜像 ~90MB      │
└──────────────────────┘
```

### 4.2 `Dockerfile.dev` — 开发运行时镜像

- 基础镜像：`debian:bookworm-slim`
- **不含任何构建步骤**
- 仅安装运行时依赖 + 健康检查
- 源码和编译产物通过挂载从宿主机注入
- 使用中科大镜像源加速首次构建

---

## 5. 辅助脚本

| 脚本 | 用途 | 说明 |
|------|------|------|
| `scripts/dev.sh` | 启动开发环境 | 检查 Docker、创建持久化目录、校验配置与编译产物、启动 `docker-compose.dev.yml` |
| `scripts/build.sh` | 生产构建 | 清理旧产物、`docker compose build --no-cache` |
| `scripts/deploy.sh` | 生产部署 | git pull → build → up → 健康检查轮询 |
| `scripts/test.sh` | 测试 | 集成/性能测试入口 |

---

## 6. 数据持久化与挂载

### 6.1 容器间共享目录

所有三套 Compose 配置都挂载了以下目录：

```
./config/    → /app/config      # 应用配置（app.toml、auth.json 等）
./downloads/ → /app/downloads   # 下载文件存储
./data/      → /app/data        # 会话与应用数据
./logs/      → /app/logs        # 运行日志
./wal/       → /app/wal         # WAL 预写日志
```

### 6.2 开发环境独有挂载

开发环境额外直通宿主机路径（用于配置文件中引用绝对路径的备份/上传任务）：

| 宿主机路径 | 容器路径 |
|-----------|---------|
| `/home/dreamydust` | `/home/dreamydust` |
| `/mnt/BackupHDD` | `/mnt/BackupHDD` |

---

## 7. 网络模式对比

| 模式 | 生产（compose.yml） | 开发（dev.yml） |
|------|-------------------|----------------|
| **类型** | `bridge`（桥接） | `host`（无隔离） |
| **端口** | `18888:18888` 映射 | 直接监听宿主机端口 |
| **隔离性** | 容器有独立网络栈 | 完全共享宿主机网络 |
| **适用场景** | 生产部署 | 本地开发调试 |
| **自定义网络** | `baidu-network` | 无（使用宿主机网络） |

> **当前实际运行中的容器均使用 host 网络模式**（开发环境配置）。

---

## 8. 开发工作流

### 8.1 典型迭代流程

```bash
# 1. 修改后端代码
vim backend/src/...

# 2. 编译
cd backend && cargo build --release

# 3. 重启容器（秒级生效）
docker compose -f docker-compose.dev.yml restart baidu-netdisk

# 或重建镜像重启（慢，不推荐开发时用）
docker compose -f docker-compose.dev.yml build baidu-netdisk
docker compose -f docker-compose.dev.yml up -d baidu-netdisk
```

### 8.2 前端开发

```bash
# 前端通过 Vite hot-reload 自动更新，无需重启容器
# 如需新增 npm 包：
docker compose -f docker-compose.dev.yml build frontend-dev
docker compose -f docker-compose.dev.yml up -d frontend-dev
```

### 8.3 常用运维命令

```bash
# 查看日志
docker compose -f docker-compose.dev.yml logs -f baidu-netdisk

# 健康检查
curl http://localhost:18888/health

# 停止所有服务
docker compose -f docker-compose.dev.yml down

# 仅重启后端
docker compose -f docker-compose.dev.yml restart baidu-netdisk
```

---

## 附录：当前实际运行拓扑

```
┌─────────────────────────────────────────────────────────┐
│                      宿主机                             │
│  Ubuntu 24.04                                          │
│  /home/dreamydust/baidunetdisk/BaiduPCS-Rust/           │
│                                                         │
│  ┌──────────────────────────────────────────┐           │
│  │  Docker 容器: baidu-netdisk-rust-dev     │           │
│  │  ──────────────────────────────────────  │           │
│  │  镜像: baidupcs-rust-baidu-netdisk:latest│           │
│  │  入口: ./target/release/baidu-netdisk-   │           │
│  │        rust                               │           │
│  │  网络: host (直通)                       │           │
│  │                                          │           │
│  │  /app  ← BaiduPCS-Rust/ (源码+编译产物)  │           │
│  │  /app/config    ← config/                │           │
│  │  /app/data      ← data/                  │           │
│  │  /app/logs      ← logs/                  │           │
│  │  /app/wal       ← wal/                   │           │
│  │  /home/dreamydust ← /home/dreamydust     │           │
│  │  /mnt/BackupHDD   ← /mnt/BackupHDD       │           │
│  │                                          │           │
│  │  健康检查 → http://localhost:18888/health│           │
│  └──────────────────────────────────────────┘           │
│                                                         │
│  ┌──────────────────────────────────────────┐           │
│  │  Docker 容器: baidu-netdisk-frontend-dev  │           │
│  │  ──────────────────────────────────────  │           │
│  │  镜像: baidupcs-rust-frontend-dev:latest │           │
│  │  状态: Exited (143) — 当前未运行         │           │
│  │  网络: host (直通)                       │           │
│  └──────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘

网络:
  docker compose 默认创建的桥接网络
  └─ baidupcs-rust_baidu-network-dev (当前未被容器使用)
```

---

## 快速参考：改了代码怎么编译重启？

| 改了什么 | 编译命令 | 重启命令 |
|---------|---------|---------|
| **后端 Rust 代码** | `cd backend && cargo build --release` | `docker compose -f docker-compose.dev.yml restart baidu-netdisk` |
| **前端源码（不上生产）** | 无需编译，Rust 后端自动托管已有 `dist/` | 直接刷新浏览器即可看到旧版 |
| **前端源码（更新静态文件）** | `cd frontend && npm run build` | `docker compose -f docker-compose.dev.yml restart baidu-netdisk` |
| **前端源码（Vite HMR 调试）** | 无需编译 | `docker compose -f docker-compose.dev.yml up -d frontend-dev` 启动后访问 `localhost:5173` |
| **配置文件** （`config/app.toml` 等）| 无需编译 | `docker compose -f docker-compose.dev.yml restart baidu-netdisk` |
| **Dockerfile / 依赖变更** | `docker compose -f docker-compose.dev.yml build baidu-netdisk` | `docker compose -f docker-compose.dev.yml up -d baidu-netdisk` |

> **一句话口诀：** 改 Rust 代码 → `cargo build --release` + `restart`；改前端 → `npm run build` + `restart`；改配置 → 直接 `restart`。（都不用 `down`/`up`，`restart` 就够了。）
