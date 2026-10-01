#!/usr/bin/env bash
# Phase 7 exit test: three times capacity, sustained (tests/load/overload.js).
#
# Capacity is made known and small so the overload is real on a laptop: one mock
# worker with 4 slots at 100 tokens/s (16-token completions, about 25 requests/s),
# and a gateway that admits 4 at a time per endpoint with a queue of 16. Rate
# limits are lifted so every refusal comes from the admission queue.
#
# Environment: see scripts/lib/stack.sh, plus
#   RATE       requests per second offered     default 75 (3x capacity)
#   DURATION   k6 duration                     default 60s
#   K6         the k6 binary                   default k6
#   OUT        where to write the k6 JSON summary (optional)
set -euo pipefail
export GATEWAY_ENV="NEBULA_LIMITS_KEY_RPM=1000000 NEBULA_LIMITS_KEY_TPM=100000000 NEBULA_LIMITS_KEY_CONCURRENCY=10000 NEBULA_LIMITS_ORG_RPM=1000000 NEBULA_LIMITS_ORG_TPM=100000000 NEBULA_LIMITS_ORG_CONCURRENCY=10000 NEBULA_LOG_LEVEL=warn NEBULA_GATEWAY_SLOT_OVERCOMMIT=1 NEBULA_GATEWAY_QUEUE_MAX_DEPTH=16 NEBULA_DEV_DEBUG_ENDPOINTS=true"
export WORKER_RUNTIME_CONFIG='{"tokens_per_second": 100, "parallel_slots": 4}'
export WORKER_QUEUE_DEPTH=8
export ROUTE_SLOTS=4
# shellcheck source=lib/stack.sh
source "$(dirname "$0")/lib/stack.sh"

stack_up

echo "== k6 (${RATE:-75} req/s for ${DURATION:-60s})"
args=()
[[ -n "${OUT:-}" ]] && args+=(--summary-export "$OUT")
GATEWAY_URL="$GATEWAY_URL" API_KEY="$API_KEY" RATE="${RATE:-75}" DURATION="${DURATION:-60s}" MAX_DEPTH=16 \
  "${K6:-k6}" run --quiet "${args[@]}" tests/load/overload.js
