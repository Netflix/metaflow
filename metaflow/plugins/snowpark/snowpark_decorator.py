import os
import sys
import platform

from metaflow import R
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.sidecar import Sidecar
from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import DEFAULT_CONTAINER_IMAGE, DEFAULT_CONTAINER_REGISTRY

from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
)

from .snowpark_exceptions import SnowflakeException
from ..aws.aws_utils import get_docker_registry


class SnowparkDecorator(StepDecorator):
    name = "snowpark"

    defaults = {
        "image": None,
        "stage": None,
        "compute_pool": None,
        "volume_mounts": None,
        "external_integration": None,
        "cpu": None,
        "gpu": None,
        "memory": None,
    }

    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(SnowparkDecorator, self).__init__(attributes, statically_defined)

        # If no docker image is explicitly specified, impute a default image.
        if not self.attributes["image"]:
            # If metaflow-config specifies a docker image, just use that.
            if DEFAULT_CONTAINER_IMAGE:
                self.attributes["image"] = DEFAULT_CONTAINER_IMAGE
            # If metaflow-config doesn't specify a docker image, assign a
            # default docker image.
            else:
                # Metaflow-R has its own default docker image (rocker family)
                if R.use_r():
                    self.attributes["image"] = R.container_image()
                # Default to vanilla Python image corresponding to major.minor
                # version of the Python interpreter launching the flow.
                self.attributes["image"] = "python:%s.%s" % (
                    platform.python_version_tuple()[0],
                    platform.python_version_tuple()[1],
                )

        # Assign docker registry URL for the image.
        if not get_docker_registry(self.attributes["image"]):
            if DEFAULT_CONTAINER_REGISTRY:
                self.attributes["image"] = "%s/%s" % (
                    DEFAULT_CONTAINER_REGISTRY.rstrip("/"),
                    self.attributes["image"],
                )

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    # to understand where these functions are invoked in the lifecycle of a
    # Metaflow flow.
    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

    def package_init(self, flow, step_name, environment):
        try:
            # Snowflake is a soft dependency.
            from snowflake.snowpark import Session
        except (NameError, ImportError):
            raise SnowflakeException(
                "Could not import module 'snowflake'.\n\nInstall Snowflake "
                "Python package (https://pypi.org/project/snowflake/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install snowflake\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

    def runtime_init(self, flow, graph, package, run_id):
        # Set some more internal state.
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["snowpark", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options["run-time-limit"] = self.run_time_limit
            if not R.use_r():
                cli_args.entrypoint[0] = sys.executable

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
        self.metadata = metadata
        self.task_datastore = task_datastore

        if "METAFLOW_SNOWPARK_WORKLOAD" in os.environ:
            meta = {}
            # TODO: inject task metadata

            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        if "METAFLOW_SNOWPARK_WORKLOAD" in os.environ:
            if hasattr(self, "metadata") and self.metadata.TYPE == "local":
                # Note that the datastore is *always* Amazon S3 (see
                # runtime_task_created function).
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

        try:
            self._save_logs_sidecar.terminate()
        except:
            # Best effort kill
            pass

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
