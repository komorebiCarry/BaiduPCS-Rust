#!/bin/bash

# 性能测试脚本 - 测试应用性能

set -e

echo "⚡ 开始性能测试..."

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
CONCURRENT_REQUESTS=10
TOTAL_REQUESTS=100

# 检查 ab 工具是否安装
check_dependencies() {
    if ! command -v ab &> /dev/null; then
        echo -e "${YELLOW}⚠️  Apache Bench (ab) 未安装${NC}"
        echo "请安装: "
        echo "  Ubuntu/Debian: sudo apt-get install apache2-utils"
        echo "  macOS: brew install httpie"
        echo "  或使用 wrk: brew install wrk"
        return 1
    fi
    return 0
}

# 测试响应时间
test_response_time() {
    local endpoint="$1"
    local name="$2"
    
    echo ""
    echo -e "${BLUE}=== $name ===${NC}"
    echo "端点: $endpoint"
    echo "并发数: $CONCURRENT_REQUESTS"
    echo "总请求数: $TOTAL_REQUESTS"
    echo ""
    
    if check_dependencies; then
        ab -n $TOTAL_REQUESTS -c $CONCURRENT_REQUESTS "$API_URL$endpoint" 2>&1 | grep -E "(Requests per second|Time per request|Transfer rate)"
    else
        echo -e "${YELLOW}使用 curl 进行简单测试...${NC}"
        local total_time=0
        local count=10
        
        for i in $(seq 1 $count); do
            start=$(date +%s%N)
            curl -s "$API_URL$endpoint" > /dev/null
            end=$(date +%s%N)
            time_ms=$(( (end - start) / 1000000 ))
            total_time=$((total_time + time_ms))
            echo "  请求 $i: ${time_ms}ms"
        done
        
        avg_time=$((total_time / count))
        echo ""
        echo "平均响应时间: ${avg_time}ms"
    fi
}

# 测试内存占用
test_memory_usage() {
    echo ""
    echo -e "${BLUE}=== 内存占用测试 ===${NC}"
    
    if command -v docker &> /dev/null; then
        container_name="baidu-netdisk-rust"
        if docker ps | grep -q "$container_name"; then
            echo "容器内存使用:"
            docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" $container_name
        else
            echo -e "${YELLOW}容器未运行${NC}"
        fi
    else
        echo -e "${YELLOW}Docker 未安装，跳过内存测试${NC}"
    fi
}

# 测试镜像大小
test_image_size() {
    echo ""
    echo -e "${BLUE}=== Docker 镜像大小 ===${NC}"
    
    if command -v docker &> /dev/null; then
        docker images baidu-netdisk-rust:latest --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
    else
        echo -e "${YELLOW}Docker 未安装，跳过镜像大小测试${NC}"
    fi
}

# 主测试流程
main() {
    echo ""
    echo -e "${BLUE}========================${NC}"
    echo -e "${BLUE}=== 性能测试开始 ===${NC}"
    echo -e "${BLUE}========================${NC}"
    
    # 检查服务是否运行
    if ! curl -f "$API_URL/health" > /dev/null 2>&1; then
        echo -e "${RED}服务未运行，请先启动服务${NC}"
        echo "运行: docker-compose up -d"
        exit 1
    fi
    
    # 响应时间测试
    test_response_time "/health" "健康检查响应时间"
    test_response_time "/api/v1/config" "配置API响应时间"
    test_response_time "/api/v1/downloads" "下载列表响应时间"
    
    # 资源使用测试
    test_memory_usage
    test_image_size
    
    # 总结
    echo ""
    echo -e "${BLUE}========================${NC}"
    echo -e "${BLUE}=== 性能测试完成 ===${NC}"
    echo -e "${BLUE}========================${NC}"
    echo ""
    echo -e "${GREEN}✅ 性能测试完成${NC}"
    echo ""
    echo "建议性能指标:"
    echo "  - 响应时间 < 100ms"
    echo "  - 内存占用 < 500MB"
    echo "  - 镜像大小 < 200MB"
    echo "  - CPU 使用 < 50%"
}

# 运行测试
main

