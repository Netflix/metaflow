import json
import os
import tarfile
from typing import Dict, List, NamedTuple
from urllib.parse import urlparse

from metaflow import current, util
from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.kfp.kfp_constants import (
    PRECEDING_COMPONENT_INPUTS_PATH,
)
from metaflow.plugins.kfp.kfp_foreach_splits import KfpForEachSplits
from metaflow.sidecar import SidecarSubProcess


class KfpException(MetaflowException):
    headline = "KFP plugin error"


StepOpBinding = NamedTuple(
    "StepOpBinding",
    [
        ("preceding_component_inputs", List[str]),
        ("preceding_component_outputs", List[str]),
    ],
)


class KfpInternalDecorator(StepDecorator):
    """
    preceding_component_inputs:
      preceding_component_outputs are returned by the KFP component to incorporate
      back into Metaflow Flow state.  We do this by having the previous step
      return these as a namedtuple to KFP.  They are saved to
      PRECEDING_COMPONENT_INPUTS_PATH in task_finished

    preceding_component_outputs:
      preceding_component_inputs are Flow state fields to expose to a KFP step by
      returning them as KFP step return values.  It is a list of KFP component
      returned namedtuple to bind to Metaflow self state.  These are
      environment set as environment variables and loaded to Metaflow self
      state within task_pre_step.

    image: str
      Defaults to None, which means default to either the container image
      specified with --base-image by the user running a flow with
      python sample_flow.py kfp run --base-image tensorflow/tensorflow:latest-devel
      OR the container image specified as BASE_IMAGE in
      metaflow/plugins/kfp/kfp_constants.py.


    @step
    @kfp(
        preceding_component=my_step_op_func,
        preceding_component_inputs=["var1", "var2"],
        preceding_component_outputs=["var3"],
        image="tensorflow/tensorflow:latest-devel",
    )
    def myStep(self):
        pass
    """

    name = "kfp"
    defaults = {
        "preceding_component": None,
        "preceding_component_inputs": [],
        "preceding_component_outputs": [],
        "image": None,
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(KfpInternalDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if self.attributes["preceding_component"] is not None:
            node = graph[step]
            if step == "start":
                raise KfpException(
                    "A @kfp preceding_component cannot be on the start step."
                )

            if len(node.in_funcs) > 1 or graph[node.in_funcs[0]].type != "linear":
                raise KfpException(
                    "The incoming step of a @kfp with a preceding_component must be linear."
                )

        self.datastore = datastore
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
        datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
    ):
        """
        Analogous to step_functions_decorator.py
        Invoked from Task.run_step within the KFP container
        """
        # TODO: any other KFP environment variables to get and register to Metadata service?
        meta = {"kfp-execution": run_id}
        entries = [
            MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()
        ]

        metadata.register_metadata(run_id, step_name, task_id, entries)
        self._save_logs_sidecar = SidecarSubProcess("save_logs_periodically")

        if metadata.TYPE == "local":
            self.ds_root = datastore.root
        else:
            self.ds_root = None

        preceding_component_outputs: List[str] = json.loads(
            os.environ["PRECEDING_COMPONENT_OUTPUTS"]
        )
        if len(preceding_component_outputs) > 0:
            for field in preceding_component_outputs:
                # Update running Flow with environment preceding_component_outputs variables
                # preceding_component_outputs is a list of environment variables that exist
                field_value = os.environ[field]
                flow.__setattr__(field, field_value)

    def task_finished(
        self,
        step_name,
        flow,
        graph,
        is_task_ok,
        retry_count,
        max_user_code_retries,
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

            # TODO: Could we copy [context file, metadata.tgz, stdout files] in
            #   parallel using the S3 client shaving off a few seconds for every
            #   task??  These seconds add up when running lightweight Metaflow
            #   tests on KFP.
            if self.ds_root:
                # We have a local metadata service so we need to persist it to the datastore.
                # Note that the datastore is *always* s3 (see runtime_task_created function)
                with util.TempDir() as td:
                    tar_file_path = os.path.join(td, "metadata.tgz")
                    with tarfile.open(tar_file_path, "w:gz") as tar:
                        # The local metadata is stored in the local datastore
                        # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
                        tar.add(DATASTORE_LOCAL_DIR)
                    # At this point we upload what need to s3
                    s3, _ = get_s3_client()
                    with open(tar_file_path, "rb") as f:
                        path = os.path.join(
                            self.ds_root,
                            MetaflowDataStore.filename_with_attempt_prefix(
                                "metadata.tgz", retry_count
                            ),
                        )
                        url = urlparse(path)
                        s3.upload_fileobj(f, url.netloc, url.path.lstrip("/"))
            else:
                # we are publishing to a Metadata service
                pass

            if graph[step_name].type == "foreach":
                # Save context to S3 for downstream DAG steps to access this
                # step's foreach_splits
                with KfpForEachSplits(
                    graph,
                    step_name,
                    current.run_id,
                    self.datastore,
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
