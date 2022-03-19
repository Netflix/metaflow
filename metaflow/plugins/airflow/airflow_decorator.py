import os
import json
import time

from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from .plumbing.airflow_xcom_push import push_xcom_values
from .airflow_utils import TASK_ID_XCOM_KEY


class AirflowInternalDecorator(StepDecorator):
    name = "airflow_internal"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        # todo : find out where the execution is taking place.
        # Once figured where the execution is happening then we can do
        # handle xcom push / pull differently
        meta = {}
        meta["airflow-execution"] = os.environ["METAFLOW_RUN_ID"]
        meta["airflow-dag-run-id"] = os.environ["METAFLOW_AIRFLOW_DAG_RUN_ID"]
        meta["airflow-job-id"] = os.environ["METAFLOW_AIRFLOW_JOB_ID"]
        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)
        if retry_count == 0:
            push_xcom_values(
                {
                    TASK_ID_XCOM_KEY: os.environ["METAFLOW_AIRFLOW_TASK_ID"],
                }
            )

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        pass
        # todo : Figure ways to find out foreach cardinality over here,
