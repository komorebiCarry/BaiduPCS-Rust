#!/bin/bash

# 本地部署脚本（不使用 Docker）
# 直接在本机构建并启动后端（rust 二进制）+ 前端（vite）
# 前端端口：5173
# 后端端口：取自 config/app.toml（默认 18888）
#
# 关键设计：
#   - 后端：cargo build 后直接 exec 出 target/release 二进制，PID 即为服务进程，避免 cargo wrapper 残留
#   - 前端：直接调用 frontend/node_modules/.bin/vite，绕过 npx wrapper，避免 node 子进程残留
#   - 启动用 setsid 建立独立进程组，停止时 kill -- -PGID 整组
#   - 兜底再用 fuser/lsof 清理仍占端口的进程

set -e

# ----------------- 颜色 -----------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ----------------- 路径 -----------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BACKEND_DIR="$PROJECT_ROOT/backend"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/.pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"
BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
BACKEND_PGID_FILE="$PID_DIR/backend.pgid"
FRONTEND_PGID_FILE="$PID_DIR/frontend.pgid"
FRONTEND_PORT_FILE="$PID_DIR/frontend.port"

BACKEND_BIN_NAME="baidu-netdisk-rust"
BACKEND_BIN="$BACKEND_DIR/target/release/$BACKEND_BIN_NAME"

# ----------------- 配置 -----------------
FRONTEND_PORT=5173
BACKEND_HOST="127.0.0.1"
BACKEND_PORT=18888
MODE="prod"   # prod | dev

# systemd
SYSTEMD_BACKEND_UNIT="baidupcs-backend.service"
SYSTEMD_FRONTEND_UNIT="baidupcs-frontend.service"
SYSTEMD_DIR="/etc/systemd/system"

# 解析参数
ACTION=""
while [ $# -gt 0 ]; do
    case "$1" in
        --dev) MODE="dev"; shift ;;
        --prod) MODE="prod"; shift ;;
        --port) FRONTEND_PORT="$2"; shift 2 ;;
        start)            ACTION="start"; shift ;;
        stop)             ACTION="stop"; shift ;;
        restart)          ACTION="restart"; shift ;;
        status)           ACTION="status"; shift ;;
        logs)             ACTION="logs"; shift ;;
        run-backend)      ACTION="run-backend"; shift ;;     # 前台跑（systemd 用）
        run-frontend)     ACTION="run-frontend"; shift ;;    # 前台跑（systemd 用）
        install-systemd)  ACTION="install-systemd"; shift ;;
        uninstall-systemd) ACTION="uninstall-systemd"; shift ;;
        systemd-status)   ACTION="systemd-status"; shift ;;
        systemd-logs)     ACTION="systemd-logs"; shift ;;
        -h|--help)
            cat <<EOF
用法: $0 [选项] [子命令]

选项:
  --dev          以开发模式启动前端（vite dev，HMR）
  --prod         以生产模式启动前端（vite build + vite preview，默认）
  --port <port>  指定前端端口（默认 5173）

子命令（手动模式）:
  start              启动前后端服务（默认）
  stop               停止前后端服务（按 PGID + 端口兜底）
  restart            重启
  status             查看服务状态
  logs               跟踪查看日志（Ctrl+C 退出）

子命令（systemd 模式，开机自启）:
  install-systemd    安装 systemd 服务并设置开机自启（需要 sudo）
  uninstall-systemd  卸载 systemd 服务（需要 sudo）
  systemd-status     查看 systemd 服务状态
  systemd-logs       跟踪 systemd 服务日志（journalctl -f）

子命令（内部，给 systemd 调用）:
  run-backend        前台运行后端（不要手动调用）
  run-frontend       前台运行前端（不要手动调用）

示例:
  $0                       # 生产模式启动，前端端口 5173
  $0 --dev                 # 开发模式启动
  $0 stop                  # 停止服务
  sudo $0 install-systemd  # 安装为系统服务，开机自启
EOF
            exit 0 ;;
        *) echo -e "${RED}未知参数: $1${NC}"; exit 1 ;;
    esac
done

# ----------------- 工具函数 -----------------
log()   { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()    { echo -e "${GREEN}✅ $*${NC}"; }
warn()  { echo -e "${YELLOW}⚠️  $*${NC}"; }
err()   { echo -e "${RED}❌ $*${NC}"; }

is_running() {
    local pid_file="$1"
    [ -f "$pid_file" ] || return 1
    local pid
    pid=$(cat "$pid_file" 2>/dev/null || echo "")
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

detect_frontend_port() {
    local port="$FRONTEND_PORT"

    if [ -f "$FRONTEND_PORT_FILE" ]; then
        local saved
        saved=$(cat "$FRONTEND_PORT_FILE" 2>/dev/null || echo "")
        case "$saved" in
            ''|*[!0-9]*) ;;
            *) echo "$saved"; return ;;
        esac
    fi

    if [ -f "$FRONTEND_PID_FILE" ]; then
        local pid detected
        pid=$(cat "$FRONTEND_PID_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && [ -r "/proc/$pid/cmdline" ]; then
            detected=$(tr '\0' '\n' < "/proc/$pid/cmdline" | awk 'prev=="--port"{print; exit} {prev=$0}')
            case "$detected" in
                ''|*[!0-9]*) ;;
                *) port="$detected" ;;
            esac
        fi
    fi

    echo "$port"
}

# 释放占用某端口的所有进程（兜底）
free_port() {
    local port="$1"
    if command -v fuser >/dev/null 2>&1; then
        fuser -k -TERM "${port}/tcp" 2>/dev/null || true
        sleep 1
        if fuser -s "${port}/tcp" 2>/dev/null; then
            fuser -k -KILL "${port}/tcp" 2>/dev/null || true
        fi
    elif command -v lsof >/dev/null 2>&1; then
        local pids
        pids=$(lsof -ti tcp:"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
            sleep 1
            echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
        fi
    fi
}

# 停止单个服务：进程组优先，超时强制，端口兜底
stop_service() {
    local name="$1" pid_file="$2" pgid_file="$3" port="$4"
    local pid="" pgid=""

    [ -f "$pid_file" ] && pid=$(cat "$pid_file" 2>/dev/null || echo "")
    [ -f "$pgid_file" ] && pgid=$(cat "$pgid_file" 2>/dev/null || echo "")

    # 若 PID 已不存在（被人手 kill 过），但 PGID 文件还在，依然尝试
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        # 没有 PGID 文件就当场取一个
        if [ -z "$pgid" ]; then
            pgid=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || echo "")
        fi
        log "停止 $name (PID $pid${pgid:+, PGID $pgid})..."
        if [ -n "$pgid" ]; then
            kill -TERM -- "-$pgid" 2>/dev/null || true
        else
            kill -TERM "$pid" 2>/dev/null || true
        fi

        # 等待最多 10s
        local i
        for i in 1 2 3 4 5 6 7 8 9 10; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done

        if kill -0 "$pid" 2>/dev/null; then
            warn "$name 未在 10s 内退出，强制 kill"
            if [ -n "$pgid" ]; then
                kill -KILL -- "-$pgid" 2>/dev/null || true
            else
                kill -KILL "$pid" 2>/dev/null || true
            fi
        fi
        ok "$name 主进程已停止"
    else
        log "$name 未运行（按 PID 文件判断）"
    fi

    # 端口兜底：清理一切仍在监听该端口的进程（应对 wrapper 残留 / PID 文件丢失）
    if [ -n "$port" ]; then
        if command -v ss >/dev/null 2>&1 && ss -lnt 2>/dev/null | awk '{print $4}' | grep -q ":${port}\$"; then
            warn "$name 端口 $port 仍被占用，端口兜底清理"
            free_port "$port"
        elif command -v fuser >/dev/null 2>&1 && fuser -s "${port}/tcp" 2>/dev/null; then
            warn "$name 端口 $port 仍被占用，端口兜底清理"
            free_port "$port"
        fi
    fi

    rm -f "$pid_file" "$pgid_file"
}

read_backend_port() {
    local cfg="$PROJECT_ROOT/config/app.toml"
    if [ -f "$cfg" ]; then
        local p
        p=$(awk '/^\[server\]/{flag=1;next} /^\[/{flag=0} flag && /^[[:space:]]*port[[:space:]]*=/ {gsub(/[^0-9]/,"",$0); print; exit}' "$cfg")
        [ -n "$p" ] && BACKEND_PORT="$p"
    fi
}

ensure_config() {
    local cfg="$PROJECT_ROOT/config/app.toml"
    if [ ! -f "$cfg" ]; then
        warn "未发现 config/app.toml，从示例复制"
        cp "$PROJECT_ROOT/config/app.toml.example" "$cfg"
    fi
    mkdir -p "$PROJECT_ROOT/downloads" "$PROJECT_ROOT/data"
}

check_deps() {
    command -v cargo >/dev/null 2>&1 || { err "未找到 cargo，请先安装 Rust 工具链"; exit 1; }
    command -v node  >/dev/null 2>&1 || { err "未找到 node，请先安装 Node.js"; exit 1; }
    command -v npm   >/dev/null 2>&1 || { err "未找到 npm";  exit 1; }
    command -v setsid >/dev/null 2>&1 || { err "未找到 setsid（util-linux）"; exit 1; }
}

write_runtime_vite_config() {
    cat > "$FRONTEND_DIR/vite.local.config.ts" <<EOF
import { defineConfig, mergeConfig } from 'vite'
import base from './vite.config'

export default mergeConfig(base, defineConfig({
  server: {
    host: '0.0.0.0',
    port: ${FRONTEND_PORT},
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api': { target: 'http://${BACKEND_HOST}:${BACKEND_PORT}', changeOrigin: true },
      '/ws':  { target: 'ws://${BACKEND_HOST}:${BACKEND_PORT}', ws: true, changeOrigin: true, rewrite: (p) => p.replace(/^\\/ws/, '') },
      '/api/v1/ws': { target: 'ws://${BACKEND_HOST}:${BACKEND_PORT}', ws: true, changeOrigin: true }
    }
  },
  preview: {
    host: '0.0.0.0',
    port: ${FRONTEND_PORT},
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api': { target: 'http://${BACKEND_HOST}:${BACKEND_PORT}', changeOrigin: true },
      '/ws':  { target: 'ws://${BACKEND_HOST}:${BACKEND_PORT}', ws: true, changeOrigin: true, rewrite: (p) => p.replace(/^\\/ws/, '') },
      '/api/v1/ws': { target: 'ws://${BACKEND_HOST}:${BACKEND_PORT}', ws: true, changeOrigin: true }
    }
  }
}))
EOF
}

build_backend() {
    log "构建后端 (cargo build --release)..."
    ( cd "$BACKEND_DIR" && cargo build --release 2>&1 | tee -a "$BACKEND_LOG" )
    [ -x "$BACKEND_BIN" ] || { err "未找到后端二进制 $BACKEND_BIN"; exit 1; }
}

build_frontend_if_needed() {
    if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
        log "安装前端依赖 (npm install)..."
        ( cd "$FRONTEND_DIR" && npm install 2>&1 | tee -a "$FRONTEND_LOG" )
    fi
    write_runtime_vite_config
    if [ "$MODE" = "prod" ] && [ ! -d "$FRONTEND_DIR/dist" ]; then
        log "构建前端 (npm run build)..."
        ( cd "$FRONTEND_DIR" && npm run build 2>&1 | tee -a "$FRONTEND_LOG" )
    fi
}

start_backend() {
    if is_running "$BACKEND_PID_FILE"; then
        warn "后端已在运行 (PID $(cat "$BACKEND_PID_FILE"))，跳过启动"
        return
    fi
    build_backend

    log "启动后端 -> $BACKEND_LOG"
    cd "$BACKEND_DIR"
    # setsid 让进程拥有独立 PGID；nohup + < /dev/null 彻底脱离终端
    setsid nohup "$BACKEND_BIN" >>"$BACKEND_LOG" 2>&1 < /dev/null &
    local pid=$!
    echo "$pid" > "$BACKEND_PID_FILE"
    # 取真实 PGID 落盘
    local pgid
    pgid=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || echo "$pid")
    echo "$pgid" > "$BACKEND_PGID_FILE"

    sleep 2
    if is_running "$BACKEND_PID_FILE"; then
        ok "后端已启动 (PID $pid, PGID $pgid)，监听 ${BACKEND_HOST}:${BACKEND_PORT}"
    else
        err "后端启动失败，查看 $BACKEND_LOG"
        exit 1
    fi
}

start_frontend() {
    if is_running "$FRONTEND_PID_FILE"; then
        local running_port
        running_port=$(detect_frontend_port)
        echo "$running_port" > "$FRONTEND_PORT_FILE"
        warn "前端已在运行 (PID $(cat "$FRONTEND_PID_FILE"), 端口 $running_port)，跳过启动"
        return
    fi

    build_frontend_if_needed

    local vite_bin="$FRONTEND_DIR/node_modules/.bin/vite"
    [ -x "$vite_bin" ] || { err "未找到 vite 可执行文件 $vite_bin"; exit 1; }

    cd "$FRONTEND_DIR"
    if [ "$MODE" = "dev" ]; then
        log "启动前端 (vite dev) 端口 $FRONTEND_PORT -> $FRONTEND_LOG"
        setsid nohup "$vite_bin" --config vite.local.config.ts >>"$FRONTEND_LOG" 2>&1 < /dev/null &
    else
        log "启动前端 (vite preview) 端口 $FRONTEND_PORT -> $FRONTEND_LOG"
        setsid nohup "$vite_bin" preview --config vite.local.config.ts --port "$FRONTEND_PORT" --host 0.0.0.0 >>"$FRONTEND_LOG" 2>&1 < /dev/null &
    fi
    local pid=$!
    echo "$pid" > "$FRONTEND_PID_FILE"
    local pgid
    pgid=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || echo "$pid")
    echo "$pgid" > "$FRONTEND_PGID_FILE"
    echo "$FRONTEND_PORT" > "$FRONTEND_PORT_FILE"

    sleep 2
    if is_running "$FRONTEND_PID_FILE"; then
        ok "前端已启动 (PID $pid, PGID $pgid)，监听 0.0.0.0:${FRONTEND_PORT}"
    else
        err "前端启动失败，查看 $FRONTEND_LOG"
        exit 1
    fi
}

show_status() {
    echo -e "${BLUE}=== 服务状态 ===${NC}"
    if is_running "$BACKEND_PID_FILE"; then
        ok "后端运行中 (PID $(cat "$BACKEND_PID_FILE"))  http://${BACKEND_HOST}:${BACKEND_PORT}"
    else
        warn "后端未运行"
    fi
    if is_running "$FRONTEND_PID_FILE"; then
        local frontend_port
        frontend_port=$(detect_frontend_port)
        ok "前端运行中 (PID $(cat "$FRONTEND_PID_FILE"))  http://localhost:${frontend_port}"
    else
        warn "前端未运行"
    fi
    # 顺带提示 systemd
    if systemctl list-unit-files 2>/dev/null | grep -q "^${SYSTEMD_BACKEND_UNIT}"; then
        echo ""
        echo -e "${BLUE}=== systemd ===${NC}"
        systemctl is-enabled "$SYSTEMD_BACKEND_UNIT" 2>/dev/null | xargs -I{} echo "$SYSTEMD_BACKEND_UNIT: enabled={}"
        systemctl is-active  "$SYSTEMD_BACKEND_UNIT" 2>/dev/null | xargs -I{} echo "$SYSTEMD_BACKEND_UNIT: active={}"
        systemctl is-enabled "$SYSTEMD_FRONTEND_UNIT" 2>/dev/null | xargs -I{} echo "$SYSTEMD_FRONTEND_UNIT: enabled={}"
        systemctl is-active  "$SYSTEMD_FRONTEND_UNIT" 2>/dev/null | xargs -I{} echo "$SYSTEMD_FRONTEND_UNIT: active={}"
    fi
}

tail_logs() {
    log "跟踪日志（Ctrl+C 退出）..."
    touch "$BACKEND_LOG" "$FRONTEND_LOG"
    tail -F "$BACKEND_LOG" "$FRONTEND_LOG"
}

# ----------------- 前台运行（systemd 入口）-----------------
run_backend_foreground() {
    check_deps
    ensure_config
    read_backend_port
    if [ ! -x "$BACKEND_BIN" ]; then
        build_backend
    fi
    cd "$BACKEND_DIR"
    exec "$BACKEND_BIN"
}

run_frontend_foreground() {
    check_deps
    ensure_config
    read_backend_port
    build_frontend_if_needed
    local vite_bin="$FRONTEND_DIR/node_modules/.bin/vite"
    [ -x "$vite_bin" ] || { err "未找到 vite 可执行文件 $vite_bin"; exit 1; }
    cd "$FRONTEND_DIR"
    if [ "$MODE" = "dev" ]; then
        exec "$vite_bin" --config vite.local.config.ts
    else
        exec "$vite_bin" preview --config vite.local.config.ts --port "$FRONTEND_PORT" --host 0.0.0.0
    fi
}

# ----------------- systemd 安装 -----------------
require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        err "该操作需要 root 权限，请用 sudo 重新执行：sudo $0 $ACTION"
        exit 1
    fi
}

# 探测 invoking 用户的 PATH（含 nvm/cargo），用于写入 unit Environment
detect_user_paths() {
    local target_user="$1"
    local candidates extra
    candidates="$(sudo -u "$target_user" -i bash -lc 'echo "$PATH"' 2>/dev/null || true)"
    [ -z "$candidates" ] && candidates="$PATH"
    # 追加常见路径，去重
    extra="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    echo "${candidates}:${extra}" | awk -v RS=':' '!a[$0]++ {if (NR>1) printf ":"; printf "%s", $0}'
}

install_systemd() {
    require_root
    command -v systemctl >/dev/null 2>&1 || { err "系统未安装 systemd"; exit 1; }

    local target_user="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
    [ -z "$target_user" ] && target_user="root"

    local user_path
    user_path=$(detect_user_paths "$target_user")
    log "目标用户: $target_user"
    log "Service PATH: $user_path"

    # 预检：用目标用户能否找到 cargo / node
    sudo -u "$target_user" env PATH="$user_path" bash -lc 'command -v cargo && command -v node && command -v npm' >/dev/null \
        || { err "目标用户 $target_user 在该 PATH 下找不到 cargo/node/npm，请确认 nvm/rust 是系统级安装或调整 PATH"; exit 1; }

    log "写入 $SYSTEMD_DIR/$SYSTEMD_BACKEND_UNIT"
    cat > "$SYSTEMD_DIR/$SYSTEMD_BACKEND_UNIT" <<UNIT
[Unit]
Description=BaiduPCS-Rust Backend
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$target_user
WorkingDirectory=$PROJECT_ROOT
Environment=PATH=$user_path
Environment=HOME=$(getent passwd "$target_user" | cut -d: -f6)
ExecStart=$SCRIPT_DIR/local-deploy.sh run-backend
Restart=on-failure
RestartSec=5
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=20
StandardOutput=append:$BACKEND_LOG
StandardError=append:$BACKEND_LOG

[Install]
WantedBy=multi-user.target
UNIT

    log "写入 $SYSTEMD_DIR/$SYSTEMD_FRONTEND_UNIT"
    cat > "$SYSTEMD_DIR/$SYSTEMD_FRONTEND_UNIT" <<UNIT
[Unit]
Description=BaiduPCS-Rust Frontend (vite)
After=network-online.target $SYSTEMD_BACKEND_UNIT
Wants=network-online.target
PartOf=$SYSTEMD_BACKEND_UNIT

[Service]
Type=simple
User=$target_user
WorkingDirectory=$PROJECT_ROOT
Environment=PATH=$user_path
Environment=HOME=$(getent passwd "$target_user" | cut -d: -f6)
ExecStart=$SCRIPT_DIR/local-deploy.sh run-frontend --$MODE --port $FRONTEND_PORT
Restart=on-failure
RestartSec=5
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=20
StandardOutput=append:$FRONTEND_LOG
StandardError=append:$FRONTEND_LOG

[Install]
WantedBy=multi-user.target
UNIT

    chown "$target_user":"$target_user" "$BACKEND_LOG" "$FRONTEND_LOG" 2>/dev/null || true
    chown -R "$target_user":"$target_user" "$LOG_DIR" "$PID_DIR" 2>/dev/null || true

    systemctl daemon-reload
    systemctl enable "$SYSTEMD_BACKEND_UNIT" "$SYSTEMD_FRONTEND_UNIT"
    # 安装时若已有手动启动的服务，先停掉避免端口冲突
    if [ -f "$BACKEND_PID_FILE" ] || [ -f "$FRONTEND_PID_FILE" ]; then
        warn "检测到手动启动的服务，先停止"
        sudo -u "$target_user" "$SCRIPT_DIR/local-deploy.sh" stop || true
    fi
    systemctl restart "$SYSTEMD_BACKEND_UNIT"
    sleep 2
    systemctl restart "$SYSTEMD_FRONTEND_UNIT"

    ok "已安装 systemd 服务并启用开机自启："
    echo "  - $SYSTEMD_BACKEND_UNIT"
    echo "  - $SYSTEMD_FRONTEND_UNIT"
    echo ""
    echo -e "${BLUE}常用命令:${NC}"
    echo "  systemctl status  $SYSTEMD_BACKEND_UNIT"
    echo "  systemctl status  $SYSTEMD_FRONTEND_UNIT"
    echo "  journalctl -u $SYSTEMD_BACKEND_UNIT -f"
    echo "  journalctl -u $SYSTEMD_FRONTEND_UNIT -f"
    echo "  sudo $0 uninstall-systemd   # 卸载"
}

uninstall_systemd() {
    require_root
    command -v systemctl >/dev/null 2>&1 || { err "系统未安装 systemd"; exit 1; }

    systemctl stop "$SYSTEMD_FRONTEND_UNIT" 2>/dev/null || true
    systemctl stop "$SYSTEMD_BACKEND_UNIT"  2>/dev/null || true
    systemctl disable "$SYSTEMD_FRONTEND_UNIT" 2>/dev/null || true
    systemctl disable "$SYSTEMD_BACKEND_UNIT"  2>/dev/null || true
    rm -f "$SYSTEMD_DIR/$SYSTEMD_BACKEND_UNIT" "$SYSTEMD_DIR/$SYSTEMD_FRONTEND_UNIT"
    systemctl daemon-reload
    systemctl reset-failed 2>/dev/null || true
    ok "已卸载 systemd 服务"
}

systemd_status() {
    systemctl --no-pager status "$SYSTEMD_BACKEND_UNIT" || true
    echo ""
    systemctl --no-pager status "$SYSTEMD_FRONTEND_UNIT" || true
}

systemd_logs() {
    journalctl -u "$SYSTEMD_BACKEND_UNIT" -u "$SYSTEMD_FRONTEND_UNIT" -f
}

# ----------------- 主流程 -----------------
do_start() {
    check_deps
    ensure_config
    read_backend_port

    echo -e "${BLUE}=== 本地部署（无 Docker） ===${NC}"
    echo "模式:       $MODE"
    echo "前端端口:   $FRONTEND_PORT"
    echo "后端端口:   $BACKEND_PORT"
    echo "项目目录:   $PROJECT_ROOT"
    echo ""

    start_backend
    start_frontend

    echo ""
    ok "部署完成！"
    echo ""
    echo -e "${BLUE}访问地址:${NC}"
    echo "  前端:       http://localhost:${FRONTEND_PORT}"
    echo "  后端:       http://${BACKEND_HOST}:${BACKEND_PORT}"
    echo "  健康检查:   http://${BACKEND_HOST}:${BACKEND_PORT}/health"
    echo ""
    echo -e "${BLUE}常用命令:${NC}"
    echo "  查看状态:   $0 status"
    echo "  查看日志:   $0 logs"
    echo "  停止服务:   $0 stop"
    echo "  开机自启:   sudo $0 install-systemd"
    echo ""
}

do_stop() {
    read_backend_port
    local frontend_port
    frontend_port=$(detect_frontend_port)
    stop_service "前端" "$FRONTEND_PID_FILE" "$FRONTEND_PGID_FILE" "$frontend_port"
    rm -f "$FRONTEND_PORT_FILE"
    stop_service "后端" "$BACKEND_PID_FILE"  "$BACKEND_PGID_FILE"  "$BACKEND_PORT"
}

case "${ACTION:-start}" in
    start)              do_start ;;
    stop)               do_stop ;;
    restart)            do_stop; sleep 1; do_start ;;
    status)             read_backend_port; show_status ;;
    logs)               tail_logs ;;
    run-backend)        run_backend_foreground ;;
    run-frontend)       run_frontend_foreground ;;
    install-systemd)    install_systemd ;;
    uninstall-systemd)  uninstall_systemd ;;
    systemd-status)     systemd_status ;;
    systemd-logs)       systemd_logs ;;
    *)                  err "未知子命令: $ACTION"; exit 1 ;;
esac
