# shellcheck shell=bash
# A throwaway NEBULA stack for end-to-end and load runs. Sourced, not executed.
#
#   PostgreSQL + Redis (docker compose, or already running)
#   → a throwaway database, migrated
#   → nebula-controlplane, seeded (the dev org, an owner and a full-scope key)
#   → nebula-worker with the mock runtime
#   → nebula-gateway with a static route to that worker
#
# After stack_up, these are set: GATEWAY_URL, WORKER_URL, CONTROLPLANE_URL,
# API_KEY, LOGS. Everything started is stopped on exit, including on failure.
#
# Environment:
#   PY                python with the worker installed (default python3)
#   KEEP=1            leave the stack running on success, for poking at by hand
#   NEBULA_E2E_DEPS   "compose" (default) starts PostgreSQL and Redis with docker
#                     compose; "external" uses whatever already listens on
#                     127.0.0.1:5432 and :6379 (a native install, WSL, a tunnel)
#   GATEWAY_ENV       extra "NAME=value" words for the gateway's environment
#   WORKER_RUNTIME_CONFIG  the mock runtime's configuration (JSON)

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
PY=${PY:-python3}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/nebula-stack.XXXXXX")
LOGS=$WORK/logs
mkdir -p "$LOGS"

DB_NAME=nebula_e2e
DB_URL="postgres://nebula:nebula@127.0.0.1:5432/${DB_NAME}?sslmode=disable"
# A logical database of its own, so a run never touches development data in 0.
REDIS_URL=redis://127.0.0.1:6379/15
PEPPER=nebula-development-pepper-do-not-use-in-production
SECRET=nebula-development-internal-secret-do-not-use

EXE=""
case "$(uname -s)" in MINGW* | MSYS* | CYGWIN*) EXE=".exe" ;; esac

# Free ports, chosen at run time: fixed ports collide with whatever else the
# machine runs, and on Windows hosts with Hyper-V some are refused outright.
free_port() {
  "$PY" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

STACK_PIDS=()
stack_down() {
  local rc=$?
  if [[ "${KEEP:-0}" == "1" && $rc -eq 0 ]]; then
    echo "KEEP=1: stack left running (pids ${STACK_PIDS[*]}); logs in $LOGS"
    return
  fi
  for p in "${STACK_PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ $rc -ne 0 ]]; then
    echo "--- failed; last lines of each log ($LOGS) ---"
    for f in "$LOGS"/*.log; do
      echo "== $f"
      tail -25 "$f"
    done
  fi
}
trap stack_down EXIT

wait_http() { # url, name
  for _ in $(seq 1 120); do
    if curl -fsS -o /dev/null "$1" 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  echo "$2 did not become ready at $1"
  return 1
}

stack_up() {
  local cp_port gw_port wk_port bin
  cp_port=$(free_port)
  gw_port=$(free_port)
  wk_port=$(free_port)

  echo "== dependencies (${NEBULA_E2E_DEPS:-compose})"
  if [[ "${NEBULA_E2E_DEPS:-compose}" == "compose" ]]; then
    docker compose up -d postgres redis >/dev/null
  fi
  go run ./tests/e2e/tools/resetdeps \
    -postgres "postgres://nebula:nebula@127.0.0.1:5432/postgres?sslmode=disable" -db "$DB_NAME" \
    -redis "$REDIS_URL"

  echo "== build"
  bin=$WORK/bin
  mkdir -p "$bin"
  for b in controlplane:./services/controlplane gateway:./services/gateway migrate:./cmd/nebula-migrate; do
    CGO_ENABLED=0 go build -o "$bin/nebula-${b%%:*}$EXE" "${b#*:}"
  done

  echo "== migrate"
  NEBULA_DATABASE_URL=$DB_URL "$bin/nebula-migrate$EXE" up >"$LOGS/migrate.log" 2>&1

  echo "== control plane"
  NEBULA_ENV=dev NEBULA_LOG_FORMAT=json NEBULA_DATABASE_URL=$DB_URL NEBULA_HTTP_ADDR=127.0.0.1:$cp_port \
    NEBULA_AUTH_KEY_PEPPER=$PEPPER NEBULA_INTERNAL_AUTH_SECRET=$SECRET NEBULA_DEV_SEED=true \
    "$bin/nebula-controlplane$EXE" >"$LOGS/controlplane.log" 2>&1 &
  STACK_PIDS+=($!)
  CONTROLPLANE_URL="http://127.0.0.1:$cp_port"
  wait_http "$CONTROLPLANE_URL/readyz" "control plane"
  # The seeder prints the plaintext key exactly once, in development only.
  API_KEY=$(grep -o '"api_key":"nbk_[A-Za-z0-9]*"' "$LOGS/controlplane.log" | head -1 | cut -d'"' -f4)
  [[ -n "$API_KEY" ]] || {
    echo "no seeded key in the control plane log"
    return 1
  }

  echo "== mock worker"
  (cd workers/inference && NEBULA_ENV=dev NEBULA_WORKER_HOST=127.0.0.1 NEBULA_WORKER_PORT=$wk_port \
    NEBULA_WORKER_RUNTIME=mock NEBULA_WORKER_MODEL_VERSION=e2e:mock \
    NEBULA_WORKER_RUNTIME_CONFIG="${WORKER_RUNTIME_CONFIG:-{\"tokens_per_second\": 400, \"parallel_slots\": 8\}}" \
    NEBULA_WORKER_MAX_QUEUE_DEPTH=${WORKER_QUEUE_DEPTH:-64} NEBULA_WORKER_DRAIN_DELAY_S=0 \
    exec "$PY" -m nebula_worker.main) >"$LOGS/worker.log" 2>&1 &
  STACK_PIDS+=($!)
  WORKER_URL="http://127.0.0.1:$wk_port"
  wait_http "$WORKER_URL/readyz" "worker"

  cat >"$WORK/routes.yaml" <<EOF
routes:
  - model: nebula-mock
    org: dev
    context_window: 4096
    chat_template: plain
    default_max_tokens: 32
    capabilities: {streaming: true}
    targets:
      - {deployment: mock-e2e, model_version: "e2e:mock", weight: 100, label: baseline,
         endpoints: ["$WORKER_URL"]}
EOF

  echo "== gateway"
  # shellcheck disable=SC2086 # GATEWAY_ENV is a list of NAME=value words
  env NEBULA_ENV=dev NEBULA_LOG_FORMAT=json NEBULA_HTTP_ADDR=127.0.0.1:$gw_port \
    NEBULA_AUTH_KEY_PEPPER=$PEPPER NEBULA_INTERNAL_AUTH_SECRET=$SECRET \
    NEBULA_REDIS_URL=$REDIS_URL \
    NEBULA_GATEWAY_CONTROLPLANE_URL="$CONTROLPLANE_URL" \
    NEBULA_GATEWAY_ROUTES_FILE="$WORK/routes.yaml" \
    NEBULA_GATEWAY_KEEPALIVE_INTERVAL=1s \
    ${GATEWAY_ENV:-} \
    "$bin/nebula-gateway$EXE" >"$LOGS/gateway.log" 2>&1 &
  STACK_PIDS+=($!)
  GATEWAY_URL="http://127.0.0.1:$gw_port"
  wait_http "$GATEWAY_URL/readyz" "gateway"
}
