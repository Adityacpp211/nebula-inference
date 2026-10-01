#!/usr/bin/env bash
# Delete the kind cluster. The model cache on the host is kept, so the next
# dev-up does not download artifacts again; remove /var/lib/nebula-kind to reset it.
set -euo pipefail
kind delete cluster --name nebula-dev
