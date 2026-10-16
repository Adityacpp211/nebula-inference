#!/usr/bin/env bash
# The Phase 4 demonstration, as a script: a live stack (scripts/lib/stack.sh),
# driven by the unmodified OpenAI Python SDK (tests/e2e/gateway).
#
# Environment: see scripts/lib/stack.sh, plus
#   E2E_ARGS   extra arguments for pytest
set -euo pipefail
# shellcheck source=lib/stack.sh
source "$(dirname "$0")/lib/stack.sh"

stack_up

echo "== tests (OpenAI SDK $("$PY" -c 'import openai; print(openai.__version__)'))"
# shellcheck disable=SC2086
NEBULA_GATEWAY_URL="$GATEWAY_URL" NEBULA_WORKER_URL="$WORKER_URL" \
  NEBULA_API_KEY="$API_KEY" NEBULA_E2E_GATEWAY_LOG="$LOGS/gateway.log" \
  "$PY" -m pytest -q tests/e2e/gateway ${E2E_ARGS:-}
