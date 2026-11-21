import os
import sys

from metaflow import current
from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum
from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    FEAT_ALWAYS_UPLOAD_CODE_PACKAGE,
    TENKI_CONTAINER_IMAGE,
    TENKI_CPU,
    TENKI_MEMORY,
)
from metaflow.plugins.aws.aws_utils import compute_resource_attributes
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.sidecar import Sidecar

from .tenki_client import TenkiException


class TenkiDecorator(StepDecorator):
    """
    Specifies that this step should execute inside a Tenki Sandbox microVM.

    Parameters
    ----------
    cpu : int, default 2
        Number of CPU cores required for this step. If `@resources` is
        also present, the maximum value from all decorators is used.
    memory : int, default 4096
        Memory size (in MB) required for this step. If `@resources` is
        also present, the maximum value from all decorators is used.
    image : str, optional, default None
        Base image to use for the microVM. If not specified, and
        METAFLOW_TENKI_CONTAINER_IMAGE is specified, that image is used. If
        neither is set, the Tenki default microVM image is used.

        Any image must be published to the Tenki registry (Tenki does not pull
        from external registries such as Docker Hub) and must provide `python3`.
        Metaflow itself is shipped in the code package, so it does not need to
        be pre-installed. The backend makes the image a viable runtime at
        startup: it exposes a `python` command (symlinked to `python3` when
        absent) and bootstraps pip if missing, so the Tenki default image works
        out of the box. For faster startup, provide an image with `python`, pip,
        and the datastore SDK (e.g. boto3) already installed.
    """

    name = "tenki"
    defaults = {
        "cpu": None,
        "memory": None,
        "image": None,
        "executable": None,
    }
    resource_defaults = {
        "cpu": "2",
        "memory": "4096",
    }
    package_metadata = None
    package_url = None
    package_sha = None
    run_time_limit = None

    # Conda environment support (the microVM runs Linux).
    supports_conda_environment = True
    target_platform = "linux-64"

    def init(self):
        # Apply config-level defaults for cpu/memory only when the attribute is
        # still unset (mirrors the KUBERNETES_CPU/MEMORY precedence rules).
        if self.attributes["cpu"] is None and TENKI_CPU:
            self.attributes["cpu"] = TENKI_CPU
        if self.attributes["memory"] is None and TENKI_MEMORY:
            self.attributes["memory"] = TENKI_MEMORY
        if not self.attributes["image"] and TENKI_CONTAINER_IMAGE:
            self.attributes["image"] = TENKI_CONTAINER_IMAGE

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # A Tenki microVM cannot reach a local-filesystem datastore, so a
        # remote, network-reachable datastore is required (mirrors
        # @kubernetes). The backend forwards the relevant cloud credentials into
        # the microVM for each supported datastore.
        if flow_datastore.TYPE not in ("s3", "azure", "gs"):
            raise TenkiException(
                "The *@tenki* decorator requires --datastore=s3, "
                "--datastore=azure or --datastore=gs at the moment."
            )

        # @parallel / gang scheduling is not supported yet.
        for deco in decos:
            if deco.name == "parallel":
                raise TenkiException(
                    "The *@tenki* decorator does not support @parallel steps yet."
                )

        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        # Merge @resources with the @tenki resource knobs (batch-style; this
        # path is covered by test_compute_resource_attributes).
        self.attributes.update(
            compute_resource_attributes(decos, self, self.resource_defaults)
        )

        self.run_time_limit = get_run_time_limit_for_task(decos)
        if self.run_time_limit < 60:
            raise TenkiException(
                "The timeout for step *{step}* should be at least 60 seconds "
                "for execution inside a Tenki sandbox.".format(step=step)
            )

    def package_init(self, flow, step_name, environment):
        # Fail early with an actionable message if the SDK is missing.
        from .tenki_client import get_tenki_module

        get_tenki_module()

    def runtime_init(self, flow, graph, package, run_id):
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        # The microVM downloads the code package from the datastore as part of
        # its entrypoint, so it must be uploaded before launch.
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            # After all user-code retries are exhausted, fallback (@catch) code
            # runs locally, so we stop routing to Tenki.
            cli_args.commands = ["tenki", "step"]
            cli_args.command_args.append(self.package_metadata)
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options["run-time-limit"] = self.run_time_limit
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

        # task_pre_step may run locally when @catch fallback is active; only do
        # remote-workload bookkeeping when actually inside the microVM.
        if "METAFLOW_TENKI_WORKLOAD" in os.environ:
            entries = [
                MetaDatum(
                    field="tenki-sandbox-name",
                    value=os.environ.get("METAFLOW_TENKI_SANDBOX_NAME", ""),
                    type="tenki-sandbox-name",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            ]
            metadata.register_metadata(run_id, step_name, task_id, entries)

            # Periodically flush structured logs to the datastore so the local
            # orchestrator's log tailer sees output while the task runs.
            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        # task_finished may run locally (the @catch fallback path), in which case
        # task_pre_step never started the log sidecar. Only tear it down when we
        # actually ran as the remote workload.
        if "METAFLOW_TENKI_WORKLOAD" in os.environ:
            # With `local` metadata, sync it back to the datastore so it reaches
            # the user's machine. There is no guarantee task_pre_step ran, so we
            # guard against a missing metadata object.
            if hasattr(self, "metadata") and self.metadata.TYPE == "local":
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )
            try:
                self._save_logs_sidecar.terminate()
            except Exception:
                # Best effort.
                pass

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            if not FEAT_ALWAYS_UPLOAD_CODE_PACKAGE:
                cls.package_url, cls.package_sha = flow_datastore.save_data(
                    [package.blob], len_hint=1
                )[0]
                cls.package_metadata = package.package_metadata
            else:
                # Blocks until the package is uploaded.
                cls.package_url = package.package_url()
                cls.package_sha = package.package_sha()
                cls.package_metadata = package.package_metadata
