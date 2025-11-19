#!/bin/bash

# 集成测试脚本 - 测试完整的应用流程

set -e

echo "🧪 开始集成测试..."

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

# 配置
API_URL="http://localhost:8080"
TEST_TIMEOUT=30

# 测试计数器
total_tests=0
passed_tests=0
failed_tests=0

# 测试函数
test_api() {
    local name="$1"
    local method="$2"
    local endpoint="$3"
    local expected_status="$4"
    
    echo -e "${YELLOW}测试: $name${NC}"
    total_tests=$((total_tests + 1))
    
    response=$(curl -s -w "\n%{http_code}" -X "$method" "$API_URL$endpoint" || echo "000")
    status_code=$(echo "$response" | tail -n 1)
    body=$(echo "$response" | head -n -1)
    
    if [ "$status_code" = "$expected_status" ]; then
        echo -e "${GREEN}✅ 通过 (状态码: $status_code)${NC}"
        passed_tests=$((passed_tests + 1))
        return 0
    else
        echo -e "${RED}❌ 失败 (期望: $expected_status, 实际: $status_code)${NC}"
        echo "响应: $body"
        failed_tests=$((failed_tests + 1))
        return 1
    fi
}

# 等待服务启动
wait_for_service() {
    echo -e "${YELLOW}⏳ 等待服务启动...${NC}"
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -f "$API_URL/health" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ 服务已启动${NC}"
            return 0
        fi
        attempt=$((attempt + 1))
        echo "  尝试 $attempt/$max_attempts..."
        sleep 1
    done
    
    echo -e "${RED}❌ 服务启动超时${NC}"
    return 1
}

# 主测试流程
main() {
    echo ""
    echo -e "${BLUE}===================${NC}"
    echo -e "${BLUE}=== 集成测试开始 ===${NC}"
    echo -e "${BLUE}===================${NC}"
    echo ""
    
    # 检查服务是否运行
    if ! wait_for_service; then
        echo -e "${RED}服务未运行，请先启动服务${NC}"
        echo "运行: docker-compose up -d"
        exit 1
    fi
    
    echo ""
    echo -e "${BLUE}=== 基础健康检查 ===${NC}"
    test_api "健康检查" "GET" "/health" "200"
    
    echo ""
    echo -e "${BLUE}=== 认证 API 测试 ===${NC}"
    test_api "生成二维码" "POST" "/api/v1/auth/qrcode/generate" "200"
    test_api "获取当前用户（未登录）" "GET" "/api/v1/auth/user" "401"
    
    echo ""
    echo -e "${BLUE}=== 配置 API 测试 ===${NC}"
    test_api "获取配置" "GET" "/api/v1/config" "200"
    
    echo ""
    echo -e "${BLUE}=== 文件 API 测试（需要登录）===${NC}"
    echo -e "${YELLOW}注意: 这些测试需要登录后才能通过${NC}"
    test_api "获取文件列表" "GET" "/api/v1/files?dir=/" "401" || echo -e "${YELLOW}  (未登录时返回401是正常的)${NC}"
    
    echo ""
    echo -e "${BLUE}=== 下载 API 测试 ===${NC}"
    test_api "获取所有下载任务" "GET" "/api/v1/downloads" "200"
    
    # 测试总结
    echo ""
    echo -e "${BLUE}===================${NC}"
    echo -e "${BLUE}=== 测试总结 ===${NC}"
    echo -e "${BLUE}===================${NC}"
    echo "总测试数: $total_tests"
    echo -e "${GREEN}通过: $passed_tests${NC}"
    if [ $failed_tests -gt 0 ]; then
        echo -e "${RED}失败: $failed_tests${NC}"
    else
        echo -e "${GREEN}失败: $failed_tests${NC}"
    fi
    
    echo ""
    if [ $failed_tests -eq 0 ]; then
        echo -e "${GREEN}✅ 所有集成测试通过！${NC}"
        exit 0
    else
        echo -e "${RED}❌ 有测试失败${NC}"
        exit 1
    fi
}

# 运行测试
main

