import os
import json
import time

from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from .airflow_utils import TASK_ID_XCOM_KEY


K8S_XCOM_DIR_PATH = "/airflow/xcom"


def safe_mkdir(dir):
    try:
        os.makedirs(dir)
    except FileExistsError:
        pass


def push_xcom_values(xcom_dict):
    safe_mkdir(K8S_XCOM_DIR_PATH)
    with open(os.path.join(K8S_XCOM_DIR_PATH, "return.json"), "w") as f:
        json.dump(xcom_dict, f)


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
        # find out where the execution is taking place.
        # Once figured where the execution is happening then we can do
        # handle xcom push / pull differently
        meta = {}
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
        push_xcom_values(
            {
                TASK_ID_XCOM_KEY: os.environ["METAFLOW_AIRFLOW_TASK_ID"],
            }
        )
