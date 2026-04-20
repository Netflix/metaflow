#!/usr/bin/env bash
set -euo pipefail
#
# Start the devstack via Tilt and wait for all services to be ready.
#
# Env vars:
#   SERVICES  — comma-separated list of Tilt resources to enable
#               (e.g. "minio,postgresql,metadata-service")

# The Tiltfile writes config files into .devtools/ — create it first.
mkdir -p .devtools

# --stream replaces the TUI with plain log output, suitable for CI.
SERVICES="${SERVICES:?}" \
  tilt up --stream 2>&1 | tee /tmp/tilt.log &

# Wait for Tilt API server (takes a few seconds to bind after launch).
echo "Waiting for Tilt API server..."
for _i in $(seq 1 60); do
  tilt get session >/dev/null 2>&1 && break
  sleep 2
done

# Wait for the Tiltfile itself to finish loading (helm pulls happen here).
echo "Waiting for Tiltfile to load..."
tilt wait --for=condition=Ready "uiresource/(Tiltfile)" --timeout=120s

echo "Waiting for all devstack services (generate-configs)..."
tilt wait --for=condition=Ready uiresource/generate-configs --timeout=1800s
