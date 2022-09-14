import os
from typing import Dict

from metaflow import current
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metadata import MetaDatum
from metaflow.plugins.kfp.kfp_foreach_splits import KfpForEachSplits
from metaflow.sidecar import SidecarSubProcess


class KfpException(MetaflowException):
    headline = "KFP plugin error"


class KfpInternalDecorator(StepDecorator):
    """
    image: str
      Defaults to None, where default is determined in the following order:
      1. specified with --base-image by the user running a flow with
        python <flow_name>.py kfp run --base-image <image-url>
      2. KFP_CONTAINER_IMAGE defined in metaflow_config.py

    @step
    @kfp(
        image="tensorflow/tensorflow:latest-devel",
    )
    def myStep(self):
        pass
    """

    name = "kfp"
    defaults = {
        "image": None,
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(KfpInternalDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        self.flow_datastore = flow_datastore
        self.logger = logger

        # Add env vars from the optional @environment decorator.
        # FIXME: may be cleaner implementation to decouple @environment from kfp
        # ref: step function is also handling environment decorator ad-hoc
        # See plugins/aws/step_functions/step_functions.StepFunctions._batch
        env_deco = [
            deco for deco in graph[step].decorators if deco.name == "environment"
        ]
        if env_deco:
            os.environ.update(env_deco[0].attributes["vars"].items())

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
        """
        Analogous to step_functions_decorator.py
        Invoked from Task.run_step within the KFP container
        """
        self.metadata = metadata
        self.task_datastore = task_datastore

        # TODO: any other KFP environment variables to get and register to Metadata service?
        meta = {
            "kfp-execution": run_id,
            "pod-name": os.environ.get("MF_POD_NAME"),
            "argo-workflow": os.environ.get("MF_ARGO_WORKFLOW_NAME"),
        }

        entries = [
            MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()
        ]
        metadata.register_metadata(run_id, step_name, task_id, entries)

        self._save_logs_sidecar = SidecarSubProcess("save_logs_periodically")

    def task_finished(
        self,
        step_name,
        flow,
        graph,
        is_task_ok,
        retry_count,
        max_retries,
    ):
        """
        Analogous to step_functions_decorator.py
        Invoked from Task.run_step within the KFP container
        """
        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            pass
        else:
            if graph[step_name].type == "foreach":
                # Save context to S3 for downstream DAG steps to access this
                # step's foreach_splits
                with KfpForEachSplits(
                    graph,
                    step_name,
                    current.run_id,
                    self.flow_datastore,
                    self.logger,
                ) as split_contexts:
                    foreach_splits: Dict = split_contexts.build_foreach_splits(flow)

                    # Pass along foreach_splits to step_op_func
                    KfpForEachSplits.save_foreach_splits_to_local_fs(foreach_splits)

                    # If this step is retried, then S3 doesn't guarantee
                    # consistency that the last uploaded data is returned. It'd
                    # be very rare that the data would change as this is the
                    # last step, and we assume the same inputs should produce
                    # the same outputs.
                    split_contexts.upload_foreach_splits_to_flow_root(foreach_splits)

        try:
            self._save_logs_sidecar.kill()
        except:
            pass
