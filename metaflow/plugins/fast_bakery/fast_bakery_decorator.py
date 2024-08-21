import os
from metaflow.decorators import StepDecorator
from metaflow.metadata.metadata import MetaDatum


class InternalFastBakeryDecorator(StepDecorator):
    """
    Internal decorator to support Fast bakery
    """

    name = "fast_bakery_internal"

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
        # task_pre_step may run locally if fallback is activated for @catch
        # decorator. In that scenario, we skip collecting Kubernetes execution
        # metadata. A rudimentary way to detect non-local execution is to
        # check for the existence of METAFLOW_KUBERNETES_WORKLOAD environment
        # variable.
        meta = {}
        if "METAFLOW_KUBERNETES_WORKLOAD" in os.environ:
            image = os.environ.get("FASTBAKERY_IMAGE")
            if image:
                meta["fast-bakery-image-name"] = image

        if len(meta) > 0:
            entries = [
                MetaDatum(
                    field=k,
                    value=v,
                    type=k,
                    tags=["attempt_id:{0}".format(retry_count)],
                )
                for k, v in meta.items()
                if v is not None
            ]
            # Register book-keeping metadata for debugging.
            metadata.register_metadata(run_id, step_name, task_id, entries)
