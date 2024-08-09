import os
import sys

from metaflow import R
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.sidecar import Sidecar
from metaflow.decorators import StepDecorator

from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
)

from .slurm_exceptions import SlurmException


class SlurmDecorator(StepDecorator):
    name = "slurm"

    defaults = {
        "partition": None,
        "nodes": None,
        "ntasks": None,
        "cpus_per_task": None,
        "memory": None,
    }

    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(SlurmDecorator, self).__init__(attributes, statically_defined)

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
            # asyncssh is a soft dependency.
            import asyncssh
        except (NameError, ImportError, ModuleNotFoundError):
            raise SlurmException(
                "Could not import module 'asyncssh'.\n\nInstall asyncssh "
                "Python package (https://pypi.org/project/asyncssh/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install asyncssh\n"
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
            cli_args.commands = ["slurm", "step"]
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

        if "METAFLOW_SLURM_WORKLOAD" in os.environ:
            meta = {}
            # TODO: inject task metadata

            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        if "METAFLOW_SLURM_WORKLOAD" in os.environ:
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
