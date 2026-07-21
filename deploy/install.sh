#!/bin/sh
set -eu

umask 077

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
COMPOSE_FILE="$ROOT/compose.yaml"
ENV_FILE="$ROOT/.env"
MODE=local
DOMAIN=
ORGANIZATION=
ADMINISTRATOR=
TEAM="Core Team"
CAPABILITIES="product,delivery,operations,backend,quality"
PORT=7777
TOKEN_TTL_DAYS=30
NON_INTERACTIVE=0
IMAGE=

usage() {
    cat <<'EOF'
Usage:
  ./deploy/install.sh
  ./deploy/install.sh --local [options]
  ./deploy/install.sh --hosted flow.example.com [options]

Options:
  --organization NAME   Organization name
  --administrator NAME  First Tenant administrator
  --team NAME           Initial team (default: Core Team)
  --capabilities LIST   Comma-separated initial capabilities
  --port PORT           Loopback port (default: 7777)
  --utc-offset +08:00   Administrator work calendar offset
  --token-ttl-days N    Bootstrap browser Token lifetime (default: 30)
  --image IMAGE         Pull a published image instead of building local source
  --non-interactive     Require all mandatory values as flags
  -h, --help            Show this help
EOF
}

raw_offset=$(date +%z 2>/dev/null || printf '+0000')
UTC_OFFSET=$(printf '%s' "$raw_offset" | sed 's/^\([+-][0-9][0-9]\)\([0-9][0-9]\)$/\1:\2/')

while [ "$#" -gt 0 ]; do
    case "$1" in
        --local)
            MODE=local
            ;;
        --hosted)
            [ "$#" -ge 2 ] || { printf '%s\n' '--hosted requires a domain' >&2; exit 2; }
            MODE=hosted
            DOMAIN=$2
            shift
            ;;
        --organization)
            [ "$#" -ge 2 ] || { printf '%s\n' '--organization requires a value' >&2; exit 2; }
            ORGANIZATION=$2
            shift
            ;;
        --administrator)
            [ "$#" -ge 2 ] || { printf '%s\n' '--administrator requires a value' >&2; exit 2; }
            ADMINISTRATOR=$2
            shift
            ;;
        --team)
            [ "$#" -ge 2 ] || { printf '%s\n' '--team requires a value' >&2; exit 2; }
            TEAM=$2
            shift
            ;;
        --capabilities)
            [ "$#" -ge 2 ] || { printf '%s\n' '--capabilities requires a value' >&2; exit 2; }
            CAPABILITIES=$2
            shift
            ;;
        --port)
            [ "$#" -ge 2 ] || { printf '%s\n' '--port requires a value' >&2; exit 2; }
            PORT=$2
            shift
            ;;
        --utc-offset)
            [ "$#" -ge 2 ] || { printf '%s\n' '--utc-offset requires a value' >&2; exit 2; }
            UTC_OFFSET=$2
            shift
            ;;
        --token-ttl-days)
            [ "$#" -ge 2 ] || { printf '%s\n' '--token-ttl-days requires a value' >&2; exit 2; }
            TOKEN_TTL_DAYS=$2
            shift
            ;;
        --image)
            [ "$#" -ge 2 ] || { printf '%s\n' '--image requires a value' >&2; exit 2; }
            IMAGE=$2
            shift
            ;;
        --non-interactive)
            NON_INTERACTIVE=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'unknown option: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

prompt_value() {
    label=$1
    current=$2
    printf '%s [%s]: ' "$label" "$current" >&2
    IFS= read -r answer
    if [ -n "$answer" ]; then
        printf '%s' "$answer"
    else
        printf '%s' "$current"
    fi
}

if [ "$NON_INTERACTIVE" -eq 0 ] && [ -t 0 ]; then
    [ -n "$ORGANIZATION" ] || ORGANIZATION=$(prompt_value 'Organization name' 'My Team')
    default_admin=$(id -un 2>/dev/null || printf 'Admin')
    [ -n "$ADMINISTRATOR" ] || ADMINISTRATOR=$(prompt_value 'Administrator name' "$default_admin")
    TEAM=$(prompt_value 'Initial team' "$TEAM")
    UTC_OFFSET=$(prompt_value 'Administrator UTC offset' "$UTC_OFFSET")
    if [ "$MODE" = hosted ] && [ -z "$DOMAIN" ]; then
        DOMAIN=$(prompt_value 'Public domain' 'flow.example.com')
    fi
fi

[ -n "$ORGANIZATION" ] || { printf '%s\n' 'organization name is required' >&2; exit 2; }
[ -n "$ADMINISTRATOR" ] || { printf '%s\n' 'administrator name is required' >&2; exit 2; }

case "$PORT" in
    *[!0-9]*|'') printf '%s\n' 'port must be an integer' >&2; exit 2 ;;
esac
[ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] || { printf '%s\n' 'port must be between 1 and 65535' >&2; exit 2; }

case "$TOKEN_TTL_DAYS" in
    *[!0-9]*|'') printf '%s\n' 'token TTL must be an integer' >&2; exit 2 ;;
esac
[ "$TOKEN_TTL_DAYS" -ge 1 ] && [ "$TOKEN_TTL_DAYS" -le 365 ] || { printf '%s\n' 'token TTL must be between 1 and 365 days' >&2; exit 2; }

if [ "$MODE" = hosted ]; then
    printf '%s' "$DOMAIN" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$' || {
        printf '%s\n' 'hosted domain must not contain a scheme, path, port, or wildcard' >&2
        exit 2
    }
    case "$DOMAIN" in
        *.*) ;;
        *) printf '%s\n' 'hosted domain must be a public DNS name containing a dot' >&2; exit 2 ;;
    esac
fi

command -v docker >/dev/null 2>&1 || { printf '%s\n' 'Docker is required: https://docs.docker.com/engine/install/' >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { printf '%s\n' 'Docker Compose v2 is required' >&2; exit 1; }

random_secret() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32
    else
        od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
    fi
}

if [ ! -f "$ENV_FILE" ]; then
    password=$(random_secret)
    cat >"$ENV_FILE" <<EOF
POSTGRES_DB=manbaflow
POSTGRES_USER=manbaflow
POSTGRES_PASSWORD=$password
MAMBA_DATABASE_URL=postgresql://manbaflow:$password@postgres:5432/manbaflow
MAMBA_PORT=$PORT
MAMBA_IMAGE=manbaflow:local
MAMBA_DOMAIN=$DOMAIN
EOF
    chmod 600 "$ENV_FILE"
    printf 'Created %s with mode 0600.\n' "$ENV_FILE"
else
    printf 'Reusing existing %s and its database credentials.\n' "$ENV_FILE"
fi

set_env_value() {
    key=$1
    value=$2
    temporary="$ENV_FILE.tmp.$$"
    awk -v key="$key" -v value="$value" '
        BEGIN { found = 0 }
        index($0, key "=") == 1 { print key "=" value; found = 1; next }
        { print }
        END { if (!found) print key "=" value }
    ' "$ENV_FILE" >"$temporary"
    chmod 600 "$temporary"
    mv "$temporary" "$ENV_FILE"
}

set_env_value MAMBA_PORT "$PORT"
if [ -n "$IMAGE" ]; then
    set_env_value MAMBA_IMAGE "$IMAGE"
fi
if [ "$MODE" = hosted ]; then
    set_env_value MAMBA_DOMAIN "$DOMAIN"
else
    set_env_value MAMBA_DOMAIN ""
fi

env_value() {
    sed -n "s/^$1=//p" "$ENV_FILE" | tail -n 1
}

POSTGRES_USER=$(env_value POSTGRES_USER)
POSTGRES_DB=$(env_value POSTGRES_DB)
IMAGE=$(env_value MAMBA_IMAGE)
[ -n "$POSTGRES_USER" ] && [ -n "$POSTGRES_DB" ] || {
    printf '%s\n' '.env is missing POSTGRES_USER or POSTGRES_DB' >&2
    exit 1
}
[ -n "$IMAGE" ] || { printf '%s\n' '.env is missing MAMBA_IMAGE' >&2; exit 1; }

compose() {
    docker compose --project-directory "$ROOT" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

printf '%s\n' 'Starting PostgreSQL...'
compose up -d postgres

attempt=0
until compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        printf '%s\n' 'PostgreSQL did not become ready; inspect ./deploy/manage.sh logs' >&2
        exit 1
    fi
    sleep 1
done

if [ "$IMAGE" = "manbaflow:local" ]; then
    printf '%s\n' 'Building MambaFlow from the current source checkout...'
    compose build mamba
else
    printf 'Pulling MambaFlow image %s...\n' "$IMAGE"
    compose pull mamba
fi

printf '%s\n' 'Creating the production organization (no Showcase data)...'
setup_output=$(compose run --rm -T mamba setup \
    --organization "$ORGANIZATION" \
    --administrator "$ADMINISTRATOR" \
    --team "$TEAM" \
    --capabilities "$CAPABILITIES" \
    --utc-offset "$UTC_OFFSET" \
    --token-ttl-days "$TOKEN_TTL_DAYS")

compose up -d mamba
attempt=0
until compose exec -T mamba curl --fail --silent http://127.0.0.1:7777/health/ready >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        printf '%s\n' 'MambaFlow did not become ready; inspect ./deploy/manage.sh logs' >&2
        exit 1
    fi
    sleep 1
done
if [ "$MODE" = hosted ]; then
    compose --profile hosted up -d caddy
    URL="https://$DOMAIN/console"
else
    compose --profile hosted stop caddy >/dev/null 2>&1 || true
    URL="http://127.0.0.1:$PORT/console"
fi

printf '\n%s\n' "$setup_output"
printf '\nMambaFlow is ready: %s\n' "$URL"
printf '%s\n' 'If a new bootstrap Token was printed above, store it in a password manager.'
printf '%s\n' 'Operations: ./deploy/manage.sh status | logs | backup | upgrade | stop'
