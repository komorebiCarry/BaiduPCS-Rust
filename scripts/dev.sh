#!/bin/bash

# 开发环境启动脚本

set -e

echo "🔧 启动开发环境..."

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

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker 未安装，请先安装 Docker${NC}"
    exit 1
fi

# 创建必要的目录
echo -e "${YELLOW}📁 创建必要的目录...${NC}"
mkdir -p config downloads data

# 复制配置文件（如果不存在）
if [ ! -f "config/app.toml" ]; then
    echo -e "${YELLOW}📝 创建默认配置文件...${NC}"
    cp config/app.toml.example config/app.toml
fi

# 启动开发环境
echo -e "${YELLOW}🚀 启动开发环境容器...${NC}"
docker-compose -f docker-compose.dev.yml up -d

# 等待服务启动
echo -e "${YELLOW}⏳ 等待服务启动...${NC}"
sleep 5

# 检查服务状态
echo -e "${BLUE}📊 服务状态:${NC}"
docker-compose -f docker-compose.dev.yml ps

echo ""
echo -e "${GREEN}✅ 开发环境已启动！${NC}"
echo ""
echo -e "${BLUE}访问地址:${NC}"
echo "  前端: http://localhost:5173"
echo "  后端: http://localhost:8080"
echo "  健康检查: http://localhost:8080/health"
echo ""
echo -e "${BLUE}常用命令:${NC}"
echo "  查看日志: docker-compose -f docker-compose.dev.yml logs -f"
echo "  停止服务: docker-compose -f docker-compose.dev.yml down"
echo "  重启服务: docker-compose -f docker-compose.dev.yml restart"
echo ""

