#!/usr/bin/env bash
set -euo pipefail
#
# Dump Airflow diagnostics on failure: DAGs directory, scheduler config,
# import errors, and processor manager log.

echo "=== Scheduler pod DAGs directory ==="
kubectl exec -n default airflow-scheduler-0 -- ls -la /opt/airflow/dags/ 2>&1 || true

echo "=== Airflow scheduler config: dag_dir_list_interval ==="
kubectl exec -n default airflow-scheduler-0 -- \
  airflow config get-value scheduler dag_dir_list_interval 2>&1 || true

echo "=== Airflow import errors (via API) ==="
curl -s -u admin:admin http://localhost:8090/api/v1/importErrors 2>&1 || true

echo "=== DagFileProcessorManager log (last 50 lines) ==="
kubectl exec -n default airflow-scheduler-0 -- \
  tail -50 /opt/airflow/logs/dag_processor_manager/dag_processor_manager.log 2>&1 || true
