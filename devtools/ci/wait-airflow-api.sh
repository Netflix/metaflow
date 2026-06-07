#!/usr/bin/env bash
set -euo pipefail
#
# Poll the Airflow REST API until it returns HTTP 200 or we time out.
# The webserver startup probe passes on /health before gunicorn workers
# are fully ready to serve API requests.

AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8090}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-60}"
SLEEP_SECS="${SLEEP_SECS:-10}"

echo "Waiting for Airflow REST API at ${AIRFLOW_URL}/api/v1/health ..."
status="000"
for i in $(seq 1 "$MAX_ATTEMPTS"); do
  status=$(curl -s -o /dev/null -w "%{http_code}" \
    -u admin:admin "${AIRFLOW_URL}/api/v1/health" 2>/dev/null || echo "000")
  echo "  attempt $i: HTTP $status"
  if [ "$status" = "200" ]; then
    echo "Airflow REST API is ready."
    exit 0
  fi
  sleep "$SLEEP_SECS"
done

echo "Airflow REST API did not become ready in time (last status: $status)"
exit 1
