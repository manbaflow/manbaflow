#!/usr/bin/env bash

# ============================================================
# Relay 一键远程部署脚本（本机运行）
#
# 从本地机器出发，自动完成：
#   1. 检查本机与服务器连通性
#   2. 在服务器上安装 Docker / Compose / rsync（如缺失）
#   3. rsync 同步本仓库到服务器
#   4. 调用服务器上的 ./deploy/install.sh 完成安装与初始化
#   5. 输出 SSH 隧道命令（7777 端口只绑定服务器 loopback）
#
# 用法：
#   ./deploy/remote-install.sh            # 交互式一键安装（幂等，可重复执行）
#   ./deploy/remote-install.sh upgrade    # 同步代码并滚动升级
#   ./deploy/remote-install.sh status     # 查看服务状态
#   ./deploy/remote-install.sh logs [svc] # 跟踪日志
#   ./deploy/remote-install.sh tunnel     # 打开浏览器访问隧道
#   ./deploy/remote-install.sh ssh        # 登录服务器
#   ./deploy/remote-install.sh uninstall  # 停止并卸载（需确认）
#
# 服务器与安装参数可用环境变量覆盖，首次交互输入后会保存到
# deploy/.remote-install.env（权限 0600，已被 Git 忽略）。
# ============================================================

set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONFIG_FILE="$SCRIPT_DIR/.remote-install.env"

color_red='\033[0;31m'
color_green='\033[0;32m'
color_yellow='\033[0;33m'
color_blue='\033[0;34m'
color_reset='\033[0m'

info()  { echo -e "${color_blue}[INFO]${color_reset} $*"; }
ok()    { echo -e "${color_green}[OK]${color_reset} $*"; }
warn()  { echo -e "${color_yellow}[WARN]${color_reset} $*"; }
err()   { echo -e "${color_red}[ERR]${color_reset} $*" >&2; }

run() {
    "$@"
    local status=$?
    if [ "$status" -ne 0 ]; then
        err "命令失败($status): $*"
        exit "$status"
    fi
}

# ---------- 配置（环境变量 > 保存的配置 > 默认值） ----------

if [ -f "$CONFIG_FILE" ]; then
    # shellcheck disable=SC1090
    . "$CONFIG_FILE"
fi

REMOTE_HOST="${REMOTE_HOST:-root@64.186.226.43}"   # 备用: root@154.17.0.159
SSH_KEY="${SSH_KEY:-$HOME/.ssh/dmit-key-0624}"
REMOTE_DIR="${REMOTE_DIR:-/opt/relay}"
ORGANIZATION="${ORGANIZATION:-My Team}"
ADMINISTRATOR="${ADMINISTRATOR:-$(id -un 2>/dev/null || echo Admin)}"
TEAM="${TEAM:-Core Team}"
CAPABILITIES="${CAPABILITIES:-product,delivery,operations,backend,quality}"
UTC_OFFSET="${UTC_OFFSET:-+08:00}"
PORT="${PORT:-7777}"

SSH_OPTS=(-i "$SSH_KEY" -o ConnectTimeout=10)

remote() {
    ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" "$@"
}

remote_tty() {
    ssh -t "${SSH_OPTS[@]}" "$REMOTE_HOST" "$@"
}

save_config() {
    umask 077
    cat > "$CONFIG_FILE" <<EOF
REMOTE_HOST=$(printf '%q' "$REMOTE_HOST")
SSH_KEY=$(printf '%q' "$SSH_KEY")
REMOTE_DIR=$(printf '%q' "$REMOTE_DIR")
ORGANIZATION=$(printf '%q' "$ORGANIZATION")
ADMINISTRATOR=$(printf '%q' "$ADMINISTRATOR")
TEAM=$(printf '%q' "$TEAM")
CAPABILITIES=$(printf '%q' "$CAPABILITIES")
UTC_OFFSET=$(printf '%q' "$UTC_OFFSET")
PORT=$(printf '%q' "$PORT")
EOF
    ok "配置已保存到 $CONFIG_FILE"
}

prompt_value() {
    local label=$1 current=$2 answer
    read -rp "$label [$current]: " answer
    if [ -n "$answer" ]; then
        printf '%s' "$answer"
    else
        printf '%s' "$current"
    fi
}

prompt_config() {
    echo ""
    echo "———— 部署目标 ————"
    REMOTE_HOST=$(prompt_value "服务器 (user@host)" "$REMOTE_HOST")
    SSH_KEY=$(prompt_value "SSH 私钥" "$SSH_KEY")
    REMOTE_DIR=$(prompt_value "服务器安装目录" "$REMOTE_DIR")
    echo ""
    echo "———— 组织初始化（幂等，重复安装不会重建） ————"
    ORGANIZATION=$(prompt_value "组织名" "$ORGANIZATION")
    ADMINISTRATOR=$(prompt_value "管理员" "$ADMINISTRATOR")
    TEAM=$(prompt_value "首个团队" "$TEAM")
    UTC_OFFSET=$(prompt_value "管理员 UTC 偏移" "$UTC_OFFSET")
    SSH_OPTS=(-i "$SSH_KEY" -o ConnectTimeout=10)
    save_config
}

# ---------- 步骤 ----------

check_local_deps() {
    info "检查本机依赖..."
    local missing=()
    for cmd in ssh rsync; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done
    if [ ${#missing[@]} -ne 0 ]; then
        err "本机缺少依赖: ${missing[*]}（macOS: brew install rsync）"
        exit 1
    fi
    if [ ! -f "$SSH_KEY" ]; then
        err "找不到 SSH 私钥: $SSH_KEY"
        exit 1
    fi
    ok "本机依赖就绪"
}

check_remote() {
    info "检查服务器连通性: $REMOTE_HOST"
    if ! remote 'echo ok' >/dev/null 2>&1; then
        err "无法连接 $REMOTE_HOST，请检查网络、IP 与私钥"
        exit 1
    fi
    ok "服务器可达"
}

ensure_remote_deps() {
    info "检查服务器上的 Docker / Compose / rsync..."
    remote 'bash -s' <<'REMOTE_SCRIPT'
set -eu
if ! command -v rsync >/dev/null 2>&1; then
    echo "[remote] 安装 rsync..."
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq && apt-get install -y -qq rsync
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y -q rsync
    elif command -v yum >/dev/null 2>&1; then
        yum install -y -q rsync
    else
        echo "[remote] 无法自动安装 rsync" >&2; exit 1
    fi
fi
if ! command -v docker >/dev/null 2>&1; then
    echo "[remote] 安装 Docker（get.docker.com）..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable --now docker
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "[remote] 安装 Docker Compose v2 插件..."
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq && apt-get install -y -qq docker-compose-plugin
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y -q docker-compose-plugin
    elif command -v yum >/dev/null 2>&1; then
        yum install -y -q docker-compose-plugin
    else
        echo "[remote] 无法自动安装 docker compose v2" >&2; exit 1
    fi
fi
echo "[remote] Docker: $(docker --version)"
echo "[remote] Compose: $(docker compose version --short)"
REMOTE_SCRIPT
    local status=$?
    if [ "$status" -ne 0 ]; then
        err "服务器依赖准备失败"
        exit "$status"
    fi
    ok "服务器依赖就绪"
}

sync_repo() {
    info "同步仓库到 $REMOTE_HOST:$REMOTE_DIR ..."
    remote "mkdir -p $(printf '%q' "$REMOTE_DIR")"
    run rsync -az --delete \
        -e "ssh ${SSH_OPTS[*]}" \
        --exclude '.git' \
        --exclude 'target' \
        --exclude 'backups' \
        --exclude '.env' \
        --exclude 'deploy/secrets' \
        --exclude 'deploy/.remote-install.env' \
        --exclude '.relay' \
        --exclude '.relay-showcase' \
        --exclude '*.log' \
        "$REPO_ROOT/" "$REMOTE_HOST:$REMOTE_DIR/"
    ok "代码同步完成"
}

run_installer() {
    info "在服务器上执行安装（首次会构建镜像，耗时较长）..."
    local args
    args=$(printf '%q ' \
        --local --non-interactive \
        --organization "$ORGANIZATION" \
        --administrator "$ADMINISTRATOR" \
        --team "$TEAM" \
        --capabilities "$CAPABILITIES" \
        --utc-offset "$UTC_OFFSET" \
        --port "$PORT")
    remote_tty "cd $(printf '%q' "$REMOTE_DIR") && ./deploy/install.sh $args"
    local status=$?
    if [ "$status" -ne 0 ]; then
        err "服务器安装失败，可运行 '$0 logs' 查看日志"
        exit "$status"
    fi
    ok "服务器安装完成"
}

print_access() {
    echo ""
    echo "============================================"
    echo "  Relay 部署完成"
    echo "============================================"
    echo ""
    echo "服务端口 ${PORT} 只绑定服务器 loopback（安全默认），"
    echo "浏览器访问请先打开 SSH 隧道："
    echo ""
    echo "  $0 tunnel"
    echo ""
    echo "然后在本机浏览器打开:  http://127.0.0.1:${PORT}/console"
    echo "登录 Token 只在上方安装输出中显示一次，请立即存入密码管理器。"
    echo ""
    echo "常用命令:"
    echo "  $0 status     查看状态"
    echo "  $0 logs       跟踪日志"
    echo "  $0 upgrade    同步代码并升级"
    echo "  $0 ssh        登录服务器"
    echo ""
    ok "完成"
}

manage_proxy() {
    remote_tty "cd $(printf '%q' "$REMOTE_DIR") && ./deploy/manage.sh $(printf '%q ' "$@")"
}

open_tunnel() {
    info "打开隧道: 本机 127.0.0.1:${PORT} → 服务器 127.0.0.1:${PORT}"
    info "浏览器访问 http://127.0.0.1:${PORT}/console ，Ctrl+C 断开"
    ssh "${SSH_OPTS[@]}" -N -L "${PORT}:127.0.0.1:${PORT}" "$REMOTE_HOST"
}

install_all() {
    if [ -t 0 ]; then
        prompt_config
    else
        info "非交互模式，使用当前配置: $REMOTE_HOST → $REMOTE_DIR"
    fi
    check_local_deps
    check_remote
    ensure_remote_deps
    sync_repo
    run_installer
    print_access
}

upgrade_all() {
    check_local_deps
    check_remote
    sync_repo
    info "滚动升级（自动先备份内置数据库）..."
    manage_proxy upgrade
    ok "升级完成"
}

uninstall_all() {
    warn "即将停止 $REMOTE_HOST 上的 Relay"
    read -rp "确认卸载? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "已取消"; exit 0; }
    manage_proxy stop || true
    read -rp "是否同时删除数据卷（内置数据库将永久丢失）? [y/N]: " wipe
    if [[ "$wipe" =~ ^[Yy]$ ]]; then
        remote_tty "cd $(printf '%q' "$REMOTE_DIR") && docker compose --profile local-db --profile hosted down --volumes"
    fi
    read -rp "是否删除服务器上的代码目录 $REMOTE_DIR? [y/N]: " del
    if [[ "$del" =~ ^[Yy]$ ]]; then
        remote "rm -rf $(printf '%q' "$REMOTE_DIR")"
    fi
    ok "卸载完成"
}

main() {
    echo ""
    echo "╔══════════════════════════════════════════╗"
    echo "║     Relay 一键远程部署脚本           ║"
    echo "╚══════════════════════════════════════════╝"
    echo ""
    case "${1:-install}" in
        install)
            install_all
            ;;
        upgrade)
            upgrade_all
            ;;
        status|start|stop|backup)
            manage_proxy "$1"
            ;;
        logs)
            shift || true
            manage_proxy logs "$@"
            ;;
        tunnel)
            open_tunnel
            ;;
        ssh)
            remote_tty "cd $(printf '%q' "$REMOTE_DIR") 2>/dev/null; exec \$SHELL -l"
            ;;
        uninstall|remove)
            uninstall_all
            ;;
        *)
            err "未知命令: $1"
            echo "用法: $0 [install|upgrade|status|logs|start|stop|backup|tunnel|ssh|uninstall]"
            exit 2
            ;;
    esac
}

main "$@"
