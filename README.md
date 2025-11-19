# BaiduPCS-Rust

<div align="center">

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Rust](https://img.shields.io/badge/Rust-1.75+-orange.svg)
![Vue](https://img.shields.io/badge/Vue-3.3+-green.svg)
![Docker](https://img.shields.io/badge/Docker-ready-blue.svg)

🚀 基于 Rust + Vue 3 的高性能百度网盘客户端 | High-performance Baidu Netdisk client built with Rust and Vue 3

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [技术栈](#️-技术栈) • [项目结构](#-项目结构) • [贡献](#-贡献)

</div>

---

## 📖 简介

BaiduPCS-Rust 是一个使用 Rust 和 Vue 3 构建的现代化百度网盘第三方客户端，提供简洁的 Web 界面和高效的下载功能。

本项目参考并受到了以下优秀项目的启发：

- [qjfoidnh/BaiduPCS-Go](https://github.com/qjfoidnh/BaiduPCS-Go) - iikira/BaiduPCS-Go 原版基础上集成了分享链接/秒传链接转存功能
- [GangZhuo/BaiduPCS](https://github.com/GangZhuo/BaiduPCS) - 百度网盘命令行客户端

感谢这些项目的开源贡献，为本项目的开发提供了重要参考。

---

## ✨ 功能特性

### 🔐 认证系统
- ✅ 二维码扫码登录（百度网盘 APP 扫码）
- ✅ 自动会话管理
- ✅ 会话持久化
- ✅ 自动登录状态验证
- ✅ 失效自动跳转登录页

### 📁 文件管理
- ✅ 浏览网盘文件和目录
- ✅ 目录导航（面包屑）
- ✅ 文件列表展示（表格视图）
- ✅ 文件信息展示（名称、大小、时间、类型）

### ⬇️ 下载引擎
- ✅ 多线程并发下载（8 个并发分片，可配置）
- ✅ 断点续传支持
- ✅ 速度限制（可配置）
- ✅ 实时进度显示（下载速度、进度百分比、ETA）
- ✅ 任务队列管理
- ✅ 暂停/继续/删除功能

### 🎨 Web 界面
- ✅ 现代化 Vue 3 + Element Plus UI
- ✅ 响应式设计
- ✅ 实时状态更新
- ✅ 友好的用户体验

### 🐳 部署支持
- ✅ Docker 一键部署
- ✅ Docker Compose 支持
- ✅ 多阶段构建优化

---

## 🚀 快速开始

### 界面预览

#### 文件管理
![文件管理](docs/images/file-management.png)

浏览和管理百度网盘中的文件和目录，支持目录导航、文件信息查看和下载操作。

#### 下载管理
![下载管理](docs/images/download-management.png)

实时查看下载进度，支持多任务并发下载，显示下载速度、进度百分比和剩余时间。

#### 系统设置
![系统设置](docs/images/system-settings.png)

配置服务器参数和下载选项，包括线程数、分片大小、下载目录等。

### 使用 Docker（推荐）

#### 方式一：使用预构建镜像（最简单）

**使用 docker run（推荐）**：

```bash
# 1. 创建必要的目录
mkdir -p baidupcs-rust/{config,downloads,data}
cd baidupcs-rust

# 2. 运行容器（使用预构建镜像）
docker run -d \
  --name baidu-netdisk-rust \
  -p 18888:18888 \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/downloads:/app/downloads \
  -v $(pwd)/data:/app/data \
  --restart unless-stopped \
  komorebicarry/baidupcs-rust:latest

# 3. 访问应用
open http://localhost:18888
```

**使用 docker-compose**：

```bash
# 1. 创建必要的目录
mkdir -p baidupcs-rust/{config,downloads,data}
cd baidupcs-rust

# 2. 下载 docker-compose 配置文件
# 从项目仓库下载 docker-compose.image.yml 文件

# 3. 启动服务（使用预构建镜像）
docker-compose -f docker-compose.image.yml up -d

# 4. 访问应用
open http://localhost:18888
```

#### 方式二：从源码构建

```bash
# 1. 克隆项目
git clone https://github.com/komorebiCarry/BaiduPCS-Rust.git
cd BaiduPCS-Rust

# 2. 构建并启动服务
docker-compose up -d

# 3. 访问应用
open http://localhost:18888
```

**说明**：
- Docker 容器内，后端 API 服务和前端静态文件服务运行在同一个进程中
- 只需要暴露一个端口（18888）即可访问完整应用
- 前端页面和 API 调用都通过 `http://localhost:18888` 访问
- 前端在容器内部通过 `http://localhost:18888/api/v1` 调用后端 API
- 预构建镜像自动发布在以下位置：
  - [GitHub Container Registry](https://github.com/komorebiCarry/BaiduPCS-Rust/pkgs/container/baidupcs-rust) - `ghcr.io/komorebicarry/baidupcs-rust:latest`（推荐，无需额外注册）
  - [Docker Hub](https://hub.docker.com/r/komorebicarry/baidupcs-rust) - `komorebicarry/baidupcs-rust:latest`（可选，需要配置 secrets）
- 当创建 Git 标签（如 `v1.0.0`）时，GitHub Actions 会自动构建并推送 Docker 镜像
- **Linux 用户推荐使用 Docker Hub 镜像**（更通用，无需登录）：`komorebicarry/baidupcs-rust:latest`

### 手动安装

#### 前提条件

- Rust 1.75+
- Node.js 18+
- npm 或 pnpm

#### 后端

```bash
cd backend
cargo build --release
cargo run --release
```

#### 前端

```bash
cd frontend
npm install
npm run build
# 或者开发模式
npm run dev
```

访问 http://localhost:5173（开发模式）或 http://localhost:18888（生产模式）

---

## 📝 平台支持说明

### 测试平台

本项目主要在以下平台进行测试：

- ✅ **Windows** - 主要开发和测试平台
- ✅ **Docker** - 容器化部署，支持跨平台运行

### 其他平台

虽然项目理论上支持 Linux、macOS 等其他平台，但**目前不会进行主动测试**。如果您在其他平台使用遇到问题，欢迎提交 Issue。

### 多平台打包

项目使用 **GitHub Actions** 进行自动化多平台构建和打包，支持：

- Windows (x86_64)
- Linux (x86_64, ARM64)
- macOS (x86_64, ARM64)

预编译的二进制文件可在 [Releases](https://github.com/komorebiCarry/BaiduPCS-Rust/releases) 页面下载。

---

## 🛠️ 技术栈

### 后端

- **语言**: Rust 1.75+
- **框架**: Axum 0.7（Web 框架）
- **异步运行时**: Tokio
- **HTTP 客户端**: Reqwest
- **序列化**: Serde + Serde JSON
- **日志**: Tracing

### 前端

- **框架**: Vue 3（Composition API）
- **语言**: TypeScript 5
- **构建工具**: Vite 6
- **UI 库**: Element Plus
- **状态管理**: Pinia
- **路由**: Vue Router 4
- **HTTP 客户端**: Axios

### 部署

- **容器化**: Docker + Docker Compose
- **多阶段构建**: 优化镜像大小
- **健康检查**: 自动故障检测

---

## 📁 项目结构

```
BaiduPCS-Rust/
├── backend/                # Rust 后端
│   ├── src/
│   │   ├── auth/          # 认证模块
│   │   ├── netdisk/       # 网盘 API
│   │   ├── downloader/    # 下载引擎
│   │   ├── config/        # 配置管理
│   │   ├── server/        # Web 服务器
│   │   └── sign/          # 签名算法
│   └── Cargo.toml
├── frontend/               # Vue 3 前端
│   ├── src/
│   │   ├── views/         # 页面组件
│   │   ├── components/    # 公共组件
│   │   ├── api/           # API 封装
│   │   ├── stores/        # Pinia 状态
│   │   └── router/        # 路由配置
│   └── package.json
├── config/                 # 配置文件示例
│   ├── app.toml.example
│   └── config.example.json
├── scripts/                # 构建脚本
│   ├── build.sh
│   ├── deploy.sh
│   ├── dev.sh
│   └── test.sh
├── tests/                  # 测试脚本
│   ├── integration_test.sh
│   └── performance_test.sh
├── Dockerfile              # Docker 镜像
├── docker-compose.yml      # 生产环境
├── docker-compose.dev.yml  # 开发环境
├── rustfmt.toml            # Rust 代码格式配置
├── .gitignore              # Git 忽略文件
├── LICENSE                 # Apache License 2.0
└── README.md               # 项目说明
```

---

## ⚙️ 配置

配置文件位于 `backend/config/app.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080
cors_origins = ["*"]

[download]
download_dir = "downloads"
concurrent_chunks = 8
chunk_size_mb = 10
max_retries = 3
```

---

## 🧪 测试

```bash
# 运行所有测试
./scripts/test.sh

# 后端测试
cd backend
cargo test

# 集成测试
./tests/integration_test.sh

# 性能测试
./tests/performance_test.sh
```

---

## 📈 性能指标

| 指标 | 数值 |
|------|------|
| Docker 镜像大小 | ~150MB |
| 启动时间 | 5-10 秒 |
| 内存占用（空闲） | 100-200MB |
| 内存占用（下载） | 200-500MB |
| 并发下载分片 | 最多 16 个 |

---

## 🗺️ 路线图

- [x] ✅ 基础架构
- [x] ✅ 认证模块（二维码登录）
- [x] ✅ 文件浏览
- [x] ✅ 下载引擎（多线程 + 断点续传）
- [x] ✅ Web 服务器（RESTful API）
- [x] ✅ 前端界面（Vue 3）
- [x] ✅ Docker 部署
- [ ] 📝 批量下载
- [ ] 📝 目录下载
- [ ] 📝 上传功能
- [ ] 📝 文件分享
- [ ] 📝 离线下载

---

## 🤝 贡献

欢迎贡献代码！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发规范

- **Rust**: 遵循 Rust 官方代码风格（使用 `cargo fmt` 和 `cargo clippy`）
- **TypeScript**: 遵循 Vue 3 最佳实践
- **提交信息**: 使用清晰的提交信息

---

## 📄 许可证

本项目采用 Apache License 2.0 许可证 - 详见 [LICENSE](LICENSE) 文件

---

## ⚠️ 免责声明

本项目仅供学习和研究使用，请勿用于商业用途。使用本工具产生的任何问题与本项目无关。

---

## 🙏 致谢

- [qjfoidnh/BaiduPCS-Go](https://github.com/qjfoidnh/BaiduPCS-Go) - 本项目的重要参考
- [GangZhuo/BaiduPCS](https://github.com/GangZhuo/BaiduPCS) - 原始项目灵感来源
- [iikira/BaiduPCS-Go](https://github.com/iikira/BaiduPCS-Go) - 原版 BaiduPCS-Go
- [Rust 社区](https://www.rust-lang.org/)
- [Vue.js 社区](https://vuejs.org/)
- [Element Plus](https://element-plus.org/)

---

<div align="center">

**⭐ 如果这个项目对你有帮助，请给一个 Star！**

Made with ❤️ by Rust + Vue 3

</div>
