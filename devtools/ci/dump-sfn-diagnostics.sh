#!/usr/bin/env bash
set -euo pipefail
#
# Dump sfn-batch diagnostics on failure: sfn-local logs, failed jobs,
# and recent Docker container logs.

echo "=== sfn-local pod logs ==="
kubectl logs deployment/sfn-local --tail=50 2>&1 | grep -v "^\s*{" || true

echo "=== Failed localbatch jobs ==="
curl -s -X POST "http://localhost:8000/v1/listjobs" \
  -H "Content-Type: application/json" \
  -d '{"jobQueue":"localbatch-default","jobStatus":"FAILED"}' | python3 -m json.tool || true

echo "=== Last Docker container logs (exited=0 or exited=1) ==="
for cid in $(docker ps -a --filter "exited=1" --filter "exited=0" --format "{{.ID}}" | head -5); do
  name=$(docker inspect --format '{{.Name}}' "$cid" 2>/dev/null || echo "unknown")
  echo "--- Container $cid ($name) ---"
  docker logs "$cid" 2>&1 | tail -20 || true
done
