import os
import sys

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metadata import MetaDatum
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
)


from .armada import ArmadaException


class ArmadaDecorator(StepDecorator):
    """
    Specifies that this step should execute on Armada.

    Parameters
    ----------
    host: str, default None
        The armada instance hostname.
    port: str, default None
        The armada instance port.
    logging_host: str, default None
        The armada logging hostname.
    logging_port: str, default None
        The armada logging port.
    queue: str, default None
        The armada queue to place the job into.
    job_set_id: str, default None
        The armada job set ID to place the job into.
    cpu : int, default 1
        Number of CPUs required for this step. If `@resources` is
        also present, the maximum value from all decorators is used.
    disk : int, default 10240
        Disk size (in MB) required for this step.
    memory : int, default 4096
        Memory size (in MB) required for this step.
    gpu : int, optional, default None
        Number of NVIDIA GPUs required for this step. A value of zero implies that
        the scheduled node should not have GPUs.
    gpu_vendor : str, default None
        The vendor of the GPU.
    secrets : List[str], optional, default None
        Kubernetes secrets to use when launching pod in Kubernetes. These
        secrets are in addition to the ones defined in `METAFLOW_KUBERNETES_SECRETS`
    insecure_no_ssl: Boolean, optional, default False
        Turn off SSL for Armada related connections. Useful for debugging locally.
    """

    name = "armada"
    defaults = {
        "cpu": "120m",
        "disk": "10240M",
        "memory": "1Gi",
        "gpu": None,
        "gpu_vendor": None,
        "host": None,
        "port": None,
        "logging_host": None,
        "logging_port": None,
        "queue": None,
        "job_set_id": None,
        "secrets": None,
        "insecure_no_ssl": False,
    }
    package_url = None
    package_sha = None

    def __init__(self, attributes=None, statically_defined=False):
        super(ArmadaDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # TODO: Support other datastores?
        if flow_datastore.TYPE not in ("s3"):
            raise ArmadaException("The *@armada* decorator requires --datastore=s3.")

        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        if any([deco.name == "batch" or deco.name == "kubernetes" for deco in decos]):
            raise MetaflowException(
                "Step *{step}* is marked for execution on Armada and on another "
                "remote compuer provider. Please only use one.".format(step=step)
            )

        for deco in decos:
            if getattr(deco, "IS_PARALLEL", False):
                raise ArmadaException(
                    "@kubernetes does not support parallel execution currently."
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
            # after all attempts to run the user code have failed, we don't need
            # to execute on Armada anymore. We can execute possible fallback
            # code locally.
            cli_args.commands = ["armada", "step"]
            for key in (
                "host",
                "port",
                "logging_host",
                "logging_port",
                "cpu",
                "disk",
                "memory",
                "gpu",
                "gpu_vendor",
                "secrets",
                "insecure_no_ssl",
            ):
                if self.attributes[key] is not None:
                    cli_args.command_options[key] = self.attributes[key]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_args.append(self.attributes["queue"])
            cli_args.command_args.append(self.attributes["job_set_id"])
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
        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]

        # Register book-keeping metadata for debugging.
        self.metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        # task_finished may run locally if fallback is activated for @catch
        # decorator.
        if "METAFLOW_ARMADA_WORKLOAD" in os.environ:
            # If `local` metadata is configured, we would need to copy task
            # execution metadata from the Armada container to user's
            # local file system after the user code has finished execution.
            # This happens via datastore as a communication bridge.

            # TODO:  There is no guarantee that task_prestep executes before
            #        task_finished is invoked. That will result in AttributeError:
            #        'KubernetesDecorator' object has no attribute 'metadata' error.
            if self.metadata.TYPE == "local":
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
