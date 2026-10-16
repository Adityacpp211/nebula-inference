#!/usr/bin/env bash
# Verifies the toolchain a NEBULA developer needs, and says how to install what
# is missing rather than failing with a bare "command not found".
#
# Phase 1 needs Go, Docker and git. kubectl, kind, Helm and k6 are checked as
# optional because they are not needed until Phase 5 and Phase 16.
set -uo pipefail

ok=0
missing=0

green() { printf '  \033[32m✓\033[0m %s\n' "$1"; }
red()   { printf '  \033[31m✗\033[0m %s\n' "$1"; }
warn()  { printf '  \033[33m•\033[0m %s\n' "$1"; }

require() {
  local cmd="$1" min="$2" howto="$3"
  if command -v "$cmd" >/dev/null 2>&1; then
    green "$cmd $(version_of "$cmd") (want >= $min)"
    ok=$((ok + 1))
  else
    red "$cmd is missing — $howto"
    missing=$((missing + 1))
  fi
}

optional() {
  local cmd="$1" phase="$2" howto="$3"
  if command -v "$cmd" >/dev/null 2>&1; then
    green "$cmd $(version_of "$cmd")"
  else
    warn "$cmd is not installed; needed from $phase — $howto"
  fi
}

version_of() {
  case "$1" in
    go)      go version | awk '{print $3}' ;;
    docker)  docker --version 2>/dev/null | awk '{print $3}' | tr -d ',' ;;
    git)     git --version | awk '{print $3}' ;;
    kubectl) kubectl version --client -o json 2>/dev/null | grep -o '"gitVersion":"[^"]*"' | head -1 | cut -d'"' -f4 ;;
    *)       "$1" --version 2>/dev/null | head -1 ;;
  esac
}

echo "NEBULA preflight"
echo
echo "required now (Phase 1):"
require go     "1.24" "https://go.dev/dl/"
require git    "2.30" "your package manager"
require docker "24"   "https://docs.docker.com/get-docker/ (Docker Desktop on Windows/macOS)"

echo
echo "optional until later phases:"
optional golangci-lint "now, for the full linter" "https://golangci-lint.run/welcome/install/"
optional kubectl "Phase 5" "https://kubernetes.io/docs/tasks/tools/"
optional kind    "Phase 5" "go install sigs.k8s.io/kind@latest"
optional helm    "Phase 5" "https://helm.sh/docs/intro/install/"
optional k6      "Phase 16" "https://grafana.com/docs/k6/latest/set-up/install-k6/"

echo
if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    green "the Docker daemon is running"
  else
    red "Docker is installed but the daemon is not reachable — start Docker Desktop"
    missing=$((missing + 1))
  fi
fi

echo
if [ "$missing" -gt 0 ]; then
  echo "$missing required tool(s) missing. Install them, then run 'make preflight' again."
  exit 1
fi
echo "All required tools are present. Next: make db-up && make migrate-up && make test"
