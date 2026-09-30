#!/usr/bin/env bash
# Bring up NEBULA on a local kind cluster (docs/deployment-architecture.md §1.2).
#
#   kind cluster "nebula-dev" (1 control plane, 2 CPU workers)
#   → images built and loaded into kind (never pulled)
#   → helm install: PostgreSQL, Redis, an S3 store, control plane, controller, gateway
#   → prints the gateway endpoint and the development API key
#
# Runs on Linux, macOS and WSL. Needs docker, kind, kubectl, helm.
#
# Environment:
#   NEBULA_TAG=dev                image tag
#   NEBULA_BUILD_LLAMACPP=1       also build the llama.cpp worker image (slow: it
#                                 compiles the engine); the mock image is always built
#   NEBULA_SKIP_BUILD=1           reuse images already loaded
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=nebula-dev
TAG=${NEBULA_TAG:-dev}
CACHE=/var/lib/nebula-kind/models

for tool in docker kind kubectl helm; do
  command -v "$tool" >/dev/null || { echo "missing: $tool (see docs/development.md)"; exit 1; }
done

echo "== model cache ($CACHE)"
# Owned by the uid the non-root artifact puller and workers run as, so the cache
# is writable without a root init container.
if [ ! -d "$CACHE" ]; then
  sudo mkdir -p "$CACHE" 2>/dev/null || mkdir -p "$CACHE"
fi
sudo chown 65532:65532 "$CACHE" 2>/dev/null || chown 65532:65532 "$CACHE" || true

echo "== cluster"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind/cluster.yaml --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

if [ "${NEBULA_SKIP_BUILD:-0}" != "1" ]; then
  echo "== images (tag $TAG)"
  build() { # dockerfile target-or-empty image
    local args=(-f "deploy/docker/$1" -t "nebula/$3:$TAG" --build-arg VERSION="$TAG"
      --build-arg COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)")
    [ -n "$2" ] && args+=(--target "$2")
    echo "   nebula/$3:$TAG"
    docker build -q "${args[@]}" . >/dev/null
    kind load docker-image "nebula/$3:$TAG" --name "$CLUSTER" >/dev/null
  }
  build Dockerfile.controlplane "" controlplane
  build Dockerfile.gateway "" gateway
  build Dockerfile.controller "" controller
  build Dockerfile.migrate "" migrate
  build Dockerfile.puller "" artifact-puller
  build Dockerfile.worker mock worker-mock
  if [ "${NEBULA_BUILD_LLAMACPP:-0}" = "1" ]; then
    build Dockerfile.worker llamacpp worker-llamacpp
  fi
fi

echo "== install"
helm upgrade --install nebula deploy/helm/nebula -n nebula-system --create-namespace \
  -f deploy/helm/nebula/values-dev.yaml --set images.tag="$TAG" --wait --timeout 10m

# The development seed key is fixed in values-dev.yaml, so it survives restarts.
key=$(grep -o 'nbk_[A-Za-z0-9]*' deploy/helm/nebula/values-dev.yaml | head -1)

cat <<EOF

NEBULA is up.
  gateway:  http://127.0.0.1:8080
  api key:  $key   (development only)

  curl -H "Authorization: Bearer $key" http://127.0.0.1:8080/v1/me
  kubectl -n nebula-workloads get deploy,pods
EOF
