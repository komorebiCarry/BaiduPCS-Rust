#!/bin/bash

# 部署脚本 - 用于生产环境部署

set -e

echo "🚀 开始部署百度网盘 Rust 客户端..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# 检查是否以 root 权限运行
if [ "$EUID" -ne 0 ]; then
    echo -e "${YELLOW}⚠️  建议使用 sudo 运行此脚本${NC}"
fi

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker 未运行，请启动 Docker${NC}"
    exit 1
fi

# 停止旧的容器
echo -e "${YELLOW}🛑 停止旧容器...${NC}"
docker-compose down || true

# 拉取最新代码（如果在 Git 仓库中）
if [ -d ".git" ]; then
    echo -e "${YELLOW}📥 拉取最新代码...${NC}"
    git pull
fi

# 构建新镜像
echo -e "${YELLOW}🔨 构建新镜像...${NC}"
docker-compose build --no-cache

# 创建必要的目录
echo -e "${YELLOW}📁 创建必要的目录...${NC}"
mkdir -p config downloads data

# 检查配置文件
if [ ! -f "config/app.toml" ]; then
    echo -e "${YELLOW}📝 创建默认配置文件...${NC}"
    cp config/app.toml.example config/app.toml
    echo -e "${YELLOW}⚠️  请检查并修改 config/app.toml 配置文件${NC}"
    read -p "按任意键继续..." -n1 -s
    echo ""
fi

# 启动新容器
echo -e "${YELLOW}🚀 启动新容器...${NC}"
docker-compose up -d

# 等待服务启动
echo -e "${YELLOW}⏳ 等待服务启动...${NC}"
sleep 10

# 健康检查
echo -e "${YELLOW}🏥 执行健康检查...${NC}"
max_attempts=10
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}✅ 健康检查通过${NC}"
        break
    fi
    attempt=$((attempt + 1))
    echo "  尝试 $attempt/$max_attempts..."
    sleep 3
done

if [ $attempt -eq $max_attempts ]; then
    echo -e "${RED}❌ 健康检查失败，请查看日志${NC}"
    docker-compose logs --tail=50
    exit 1
fi

# 显示服务状态
echo ""
echo -e "${BLUE}📊 服务状态:${NC}"
docker-compose ps

echo ""
echo -e "${GREEN}✅ 部署成功！${NC}"
echo ""
echo -e "${BLUE}访问地址:${NC}"
echo "  应用: http://localhost:8080"
echo "  健康检查: http://localhost:8080/health"
echo ""
echo -e "${BLUE}常用命令:${NC}"
echo "  查看日志: docker-compose logs -f"
echo "  停止服务: docker-compose down"
echo "  重启服务: docker-compose restart"
echo "  查看状态: docker-compose ps"
echo ""

