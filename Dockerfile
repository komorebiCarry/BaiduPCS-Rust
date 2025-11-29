# 多阶段构建 Dockerfile
# Stage 1: 前端构建
FROM node:18-alpine AS frontend-builder

WORKDIR /app/frontend

# 复制前端依赖文件
COPY frontend/package*.json ./

# 安装前端依赖
RUN npm ci

# 复制前端源代码
COPY frontend/ ./

# 构建前端
RUN npm run build

# Stage 2: 后端构建
FROM rust:1.84-slim AS backend-builder

WORKDIR /app

# 安装构建依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制后端依赖文件
COPY backend/Cargo.toml backend/Cargo.lock ./

# 创建一个虚拟的 main.rs 来缓存依赖
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# 复制后端源代码
COPY backend/ ./

# 构建后端（使用缓存的依赖）
RUN cargo build --release

# Stage 3: 运行时镜像
FROM debian:bookworm-slim

WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制后端可执行文件
COPY --from=backend-builder /app/target/release/baidu-netdisk-rust /app/baidu-netdisk-rust

# 复制前端构建产物
COPY --from=frontend-builder /app/frontend/dist /app/frontend/dist

# 创建必要的目录
RUN mkdir -p /app/downloads /app/config /app/data

# 复制配置文件示例
COPY config/app.toml.example /app/config/app.toml.example

# 设置环境变量
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# 暴露端口（后端 API 和前端静态文件服务都在此端口）
EXPOSE 18888

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:18888/health || exit 1

# 启动应用
CMD ["/app/baidu-netdisk-rust"]

