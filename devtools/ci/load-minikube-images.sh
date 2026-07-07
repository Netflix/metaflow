#!/usr/bin/env bash
set -euo pipefail
#
# Load cached tar images into minikube.
#
# Env vars:
#   CACHE_DIR  — directory containing saved tars (default: /tmp/minikube-image-cache)

CACHE_DIR="${CACHE_DIR:-/tmp/minikube-image-cache}"

for f in "${CACHE_DIR}"/*.tar; do
  [ -f "$f" ] || continue
  echo "Loading $(basename "$f")..."
  minikube image load "$f" || echo "Warning: failed to load $f (skipping)"
done
