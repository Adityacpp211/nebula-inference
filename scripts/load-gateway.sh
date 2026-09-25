#!/usr/bin/env bash
# Phase 4 load baseline: tests/load/gateway-baseline.js against a live stack.
#
# Rate limits are raised far above the offered load so the run measures the
# serving path, limiter included, rather than the limiter refusing work.
#
# Environment: see scripts/lib/stack.sh, plus
#   RATE       non-streamed requests per second (streamed runs at half)   default 40
#   DURATION   k6 duration                                                default 60s
#   K6         the k6 binary                                              default k6
#   OUT        where to write the k6 JSON summary                         optional
set -euo pipefail
export GATEWAY_ENV="NEBULA_LIMITS_KEY_RPM=1000000 NEBULA_LIMITS_KEY_TPM=100000000 NEBULA_LIMITS_KEY_CONCURRENCY=10000 NEBULA_LIMITS_ORG_RPM=1000000 NEBULA_LIMITS_ORG_TPM=100000000 NEBULA_LIMITS_ORG_CONCURRENCY=10000 NEBULA_LOG_LEVEL=warn"
export WORKER_RUNTIME_CONFIG='{"tokens_per_second": 400, "parallel_slots": 32}'
export WORKER_QUEUE_DEPTH=256
# shellcheck source=lib/stack.sh
source "$(dirname "$0")/lib/stack.sh"

stack_up

echo "== k6"
args=()
[[ -n "${OUT:-}" ]] && args+=(--summary-export "$OUT")
GATEWAY_URL="$GATEWAY_URL" API_KEY="$API_KEY" RATE="${RATE:-40}" DURATION="${DURATION:-60s}" \
  "${K6:-k6}" run --quiet "${args[@]}" tests/load/gateway-baseline.js
