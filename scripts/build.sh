#!/bin/bash

# 构建脚本 - 用于生产环境构建

set -e

echo "🚀 开始构建百度网盘 Rust 客户端..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker 未安装，请先安装 Docker${NC}"
    exit 1
fi

# 检查 docker-compose 是否安装
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ docker-compose 未安装，请先安装 docker-compose${NC}"
    exit 1
fi

# 清理旧的构建
echo -e "${YELLOW}📦 清理旧的构建产物...${NC}"
rm -rf backend/target/release/baidu-netdisk-rust
rm -rf frontend/dist

# 构建 Docker 镜像
echo -e "${YELLOW}🔨 构建 Docker 镜像...${NC}"
docker-compose build --no-cache

# 检查构建是否成功
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 构建成功！${NC}"
    echo ""
    echo "使用以下命令启动应用:"
    echo "  docker-compose up -d"
    echo ""
    echo "查看日志:"
    echo "  docker-compose logs -f"
    echo ""
    echo "停止应用:"
    echo "  docker-compose down"
else
    echo -e "${RED}❌ 构建失败${NC}"
    exit 1
fi

