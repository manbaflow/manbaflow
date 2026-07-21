#!/bin/sh
set -eu

umask 077

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
COMPOSE_FILE="$ROOT/compose.yaml"
ENV_FILE="$ROOT/.env"

[ -f "$ENV_FILE" ] || { printf '%s\n' 'missing .env; run ./deploy/install.sh first' >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { printf '%s\n' 'Docker is required' >&2; exit 1; }

env_value() {
    sed -n "s/^$1=//p" "$ENV_FILE" | tail -n 1
}

compose() {
    docker compose --project-directory "$ROOT" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

start_stack() {
    domain=$(env_value MAMBA_DOMAIN)
    if [ -n "$domain" ]; then
        compose --profile hosted up -d
    else
        compose up -d postgres mamba
    fi
}

backup_database() {
    user=$(env_value POSTGRES_USER)
    database=$(env_value POSTGRES_DB)
    timestamp=$(date -u +%Y%m%dT%H%M%SZ)
    directory="$ROOT/backups"
    output=${1:-"$directory/manbaflow-$timestamp.dump"}
    mkdir -p "$(dirname -- "$output")"
    [ ! -e "$output" ] || { printf 'backup already exists: %s\n' "$output" >&2; exit 1; }
    compose exec -T postgres pg_dump -U "$user" -d "$database" --format=custom >"$output"
    chmod 600 "$output"
    printf 'Backup created: %s\n' "$output"
}

command=${1:-status}
case "$command" in
    status)
        compose ps
        ;;
    logs)
        shift || true
        compose logs --tail 200 -f "$@"
        ;;
    start)
        start_stack
        ;;
    stop)
        compose --profile hosted down
        ;;
    backup)
        backup_database "${2:-}"
        ;;
    upgrade)
        backup_database
        image=$(env_value MAMBA_IMAGE)
        if [ "$image" = "manbaflow:local" ]; then
            compose build --pull mamba
        else
            compose pull mamba
        fi
        start_stack
        ;;
    *)
        printf 'Usage: %s {status|logs [service]|start|stop|backup [path]|upgrade}\n' "$0" >&2
        exit 2
        ;;
esac
