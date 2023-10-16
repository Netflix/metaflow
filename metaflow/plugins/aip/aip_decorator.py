import json
import os
from typing import Dict, List, NamedTuple

from metaflow import current
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metadata import MetaDatum
from metaflow.plugins.aip.aip_constants import PRECEDING_COMPONENT_INPUTS_PATH
from metaflow.plugins.aip.aip_foreach_splits import AIPForEachSplits
from metaflow.sidecar import SidecarSubProcess


class AIPException(MetaflowException):
    headline = "WFSDK plugin error"


StepOpBinding = NamedTuple(
    "StepOpBinding",
    [
        ("preceding_component_inputs", List[str]),
        ("preceding_component_outputs", List[str]),
    ],
)


class AIPInternalDecorator(StepDecorator):
    """
    **`preceding_component` feature is no longer supported as of Q3 2022.**

    [Deprecated] preceding_component_inputs:
      preceding_component_outputs are returned by the KFP component to incorporate
      back into Metaflow Flow state.  We do this by having the previous step
      return these as a namedtuple to KFP.  They are saved to
      PRECEDING_COMPONENT_INPUTS_PATH in task_finished

    [Deprecated] preceding_component_outputs:
      preceding_component_inputs are Flow state fields to expose to a KFP step by
      returning them as KFP step return values.  It is a list of KFP component
      returned namedtuple to bind to Metaflow self state.  These are
      environment set as environment variables and loaded to Metaflow self
      state within task_pre_step.

    image: str
      Defaults to None, where default is determined in the following order:
      1. specified with --base-image by the user running a flow with
        python <flow_name>.py aip run --base-image <image-url>
      2. AIP_CONTAINER_IMAGE defined in metaflow_config.py

    @step
    @aip(
        preceding_component=my_step_op_func,
        preceding_component_inputs=["var1", "var2"],
        preceding_component_outputs=["var3"],
        image="tensorflow/tensorflow:latest-devel",
    )
    def myStep(self):
        pass
    """

    name = "aip"
    defaults = {
        "preceding_component": None,
        "preceding_component_inputs": [],
        "preceding_component_outputs": [],
        "image": None,
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(AIPInternalDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        if self.attributes["preceding_component"] is not None:
            raise NotImplementedError(
                "KFP preceding component is no longer supported. "
                "Please contact AIP team if you have a use case that requires this feature."
            )

        if self.attributes["preceding_component"] is not None:
            node = graph[step]
            if step == "start":
                raise AIPException(
                    "A @aip preceding_component cannot be on the start step."
                )

            # Only support linear/start types to avoid complex merge_artifacts & inputs in "join" type
            #   If we have A->C and B->C where C is a join type with preceding component,
            #   ideally merge_artifacts should happen before C.preceding_component,
            #   but this behavior is not yet implemented.
            linear_types = (
                "linear",
                "start",
            )
            if (
                len(node.in_funcs) > 1
                or graph[node.in_funcs[0]].type not in linear_types
            ):
                raise AIPException(
                    "The incoming step of a @aip with a preceding_component must be linear."
                )

        self.flow_datastore = flow_datastore
        self.logger = logger

        # Add env vars from the optional @environment decorator.
        # FIXME: may be cleaner implementation to decouple @environment from aip
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

        # TODO: any other AIP/KFP environment variables to get and register to Metadata service?
        meta = {
            "pod-name": os.environ.get("MF_POD_NAME"),
            "argo-workflow": os.environ.get("MF_ARGO_WORKFLOW_NAME"),
        }

        entries = [
            MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()
        ]
        metadata.register_metadata(run_id, step_name, task_id, entries)

        preceding_component_outputs: List[str] = json.loads(
            os.environ["PRECEDING_COMPONENT_OUTPUTS"]
        )
        if len(preceding_component_outputs) > 0:
            for field in preceding_component_outputs:
                # Update running Flow with environment preceding_component_outputs variables
                # preceding_component_outputs is a list of environment variables that exist
                field_value = os.environ[field]
                flow.__setattr__(field, field_value)

        self._save_logs_sidecar = SidecarSubProcess("save_logs_periodically")

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        preceding_component_inputs: List[str] = json.loads(
            os.environ["PRECEDING_COMPONENT_INPUTS"]
        )
        if len(preceding_component_inputs) > 0:
            with open(PRECEDING_COMPONENT_INPUTS_PATH, "w") as file:
                # Get fields from running Flow and persist as json to local FS
                fields_dictionary = {
                    key: flow.__getattribute__(key)
                    for key in preceding_component_inputs
                }
                json.dump(fields_dictionary, file)

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
                with AIPForEachSplits(
                    graph,
                    step_name,
                    current.run_id,
                    self.flow_datastore,
                    self.logger,
                ) as split_contexts:
                    foreach_splits: Dict = split_contexts.build_foreach_splits(flow)

                    # Pass along foreach_splits to step_op_func
                    AIPForEachSplits.save_foreach_splits_to_local_fs(foreach_splits)

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
