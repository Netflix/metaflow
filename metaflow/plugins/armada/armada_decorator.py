import os

from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum


class ArmadaDecorator(StepDecorator):
    name = "armada"

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
        max_retries,
        ubf_context,
        inputs,
    ):
        meta = {}
        meta["armada-job-id"] = os.environ["METAFLOW_ARMADA_JOB_ID"]
        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]

        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        pass
