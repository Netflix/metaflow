import os
import sys

from metaflow import R
from metaflow.metadata import MetaDatum
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.sidecar import Sidecar
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    SLURM_USERNAME,
    SLURM_ADDRESS,
    SLURM_SSH_KEY_FILE,
    SLURM_CERT_FILE,
    SLURM_REMOTE_WORKDIR,
)

from .slurm_exceptions import SlurmException


class SlurmDecorator(StepDecorator):
    name = "slurm"

    defaults = {
        "username": None,
        "address": None,
        "ssh_key_file": None,
        "cert_file": None,
        "remote_workdir": None,
        "cleanup": False,
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

        if not self.attributes["username"]:
            self.attributes["username"] = SLURM_USERNAME
        if not self.attributes["address"]:
            self.attributes["address"] = SLURM_ADDRESS
        if not self.attributes["ssh_key_file"]:
            self.attributes["ssh_key_file"] = SLURM_SSH_KEY_FILE
        if not self.attributes["cert_file"]:
            self.attributes["cert_file"] = SLURM_CERT_FILE
        if not self.attributes["remote_workdir"]:
            self.attributes["remote_workdir"] = SLURM_REMOTE_WORKDIR

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    # to understand where these functions are invoked in the lifecycle of a
    # Metaflow flow.
    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        if any([deco.name == "parallel" for deco in decos]):
            raise MetaflowException(
                "Step *{step}* contains a @parallel decorator "
                "with the @slurm decorator. @parallel is not supported with @slurm.".format(
                    step=step
                )
            )

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

        meta = {}
        if "METAFLOW_SLURM_WORKLOAD" in os.environ:
            meta["slurm-job-user"] = os.environ.get("SLURM_JOB_USER")
            meta["slurm-submit-dir"] = os.environ.get("SLURM_SUBMIT_DIR")
            meta["slurm-nodename"] = os.environ.get("SLURMD_NODENAME")
            meta["slurm-cluster-name"] = os.environ.get("SLURM_CLUSTER_NAME")
            meta["slurm-job-partition"] = os.environ.get("SLURM_JOB_PARTITION")
            meta["slurm-job-id"] = os.environ.get("SLURM_JOB_ID")
            meta["slurm-job-name"] = os.environ.get("SLURM_JOB_NAME")

            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

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
