#!/usr/bin/env bash
set -euo pipefail
#
# Save minikube images to a tar cache directory for later restoration.
#
# Env vars:
#   CACHE_DIR  — directory to save tars into (default: /tmp/minikube-image-cache)

CACHE_DIR="${CACHE_DIR:-/tmp/minikube-image-cache}"
mkdir -p "$CACHE_DIR"

minikube image ls 2>/dev/null \
  | grep -vE 'registry\.k8s\.io|k8s\.gcr\.io|<none>|pause' \
  | while read -r img; do
      safe=$(echo "$img" | tr '/:@' '---')
      echo "Saving $img..."
      minikube image save "$img" "${CACHE_DIR}/${safe}.tar" \
        || echo "Warning: skipping $img"
    done

echo "Saved images:"
find "$CACHE_DIR/" -maxdepth 1 -name '*.tar' -exec ls -lh {} + 2>/dev/null | head -30
