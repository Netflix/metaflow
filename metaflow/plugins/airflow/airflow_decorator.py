import os
import json
import time

from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from .plumbing.airflow_xcom_push import push_xcom_values


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
        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            return
        # todo : Figure ways to find out foreach cardinality over here,
        push_xcom_values(
            {
                "metaflow_task_id": os.environ["METAFLOW_AIRFLOW_TASK_ID"],
            }
        )
