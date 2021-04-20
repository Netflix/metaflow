import sys
import platform

from metaflow import R
from metaflow.datastore.datastore import TransformableObject
from metaflow.decorators import Decorator, StepDecorator
from metaflow.exception import MetaflowException

from metaflow.plugins.aws.batch.batch_decorator import (
    DEFAULT_TIMEOUT_SECONDS,
    BatchDecorator,
    ResourcesDecorator,
)
from metaflow.plugins.aws.utils import sync_local_metadata_to_datastore
from metaflow.sidecar import SidecarSubProcess
from metaflow.metadata import MetaDatum
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task

if sys.version_info > (3, 0):
    from typing import TYPE_CHECKING, Dict, List, Optional

    if TYPE_CHECKING:
        from metaflow.runtime import CLIArgs
        from metaflow.graph import FlowGraph
        from metaflow.package import MetaflowPackage
        from metaflow.flowspec import FlowSpec
        from metaflow.datastore import MetaflowDataStore
        from metaflow.metaflow_environment import MetaflowEnvironment
        from metaflow.event_logger import EventLogger


from metaflow.metaflow_config import (
    KUBERNETES_IMAGE_URI,
)

DEFAULT_TIMEOUT_SECONDS = 600


class KubernetesException(MetaflowException):
    headline = "Kubernetes error"


class KubernetesDecorator(StepDecorator):
    name = "kubernetes"
    defaults = {
        "image": None,
    }

    package_url = None  # type: Optional[str]
    package_sha = None  # type: Optional[str]
    resource_attributes = {}  # type: Dict[str, str]
    run_time_limit = None
    ds_root = None  # type: Optional[str]

    def __init__(self, attributes=None, statically_defined=False):
        super(StepDecorator, self).__init__(attributes, statically_defined)

        if not self.attributes["image"]:
            if KUBERNETES_IMAGE_URI:
                self.attributes["image"] = KUBERNETES_IMAGE_URI
            else:
                if R.use_r():
                    self.attributes["image"] = R.container_image()
                else:
                    self.attributes["image"] = "python:%s.%s" % (
                        platform.python_version_tuple()[0],
                        platform.python_version_tuple()[1],
                    )

    def step_init(
        self,
        flow,  # type: "FlowSpec"
        graph,  # type : "FlowGraph"
        step,  # type: str
        decos,  # type: List[Decorator]
        environment,  # type: "MetaflowEnvironment"
        datastore,  # type: "MetaflowDataStore"
        logger,  # type: "EventLogger"
    ):
        if datastore.TYPE != "s3":
            raise KubernetesException(
                "The *@kubernetes* decorator requires --datastore=s3."
            )

        self.logger = logger
        self.environment = environment
        self.step = step
        self.resource_attributes = {}
        for deco in decos:
            if isinstance(deco, BatchDecorator):
                raise KubernetesException(
                    "You can't use @batch and *@kubernetes* at the same time"
                )

            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    if v != deco.defaults[k]:
                        self.resource_attributes[k] = str(v)
        self.run_time_limit = get_run_time_limit_for_task(
            decos, default_limit=DEFAULT_TIMEOUT_SECONDS
        )
        if self.run_time_limit < 10:
            raise KubernetesException(
                "The timeout for step *{step}* should be at "
                "least 10 seconds for execution on Kubernetes".format(step=step)
            )
        if self.run_time_limit > 24 * 3600:
            raise KubernetesException(
                "The timeout for step *{step}* should be no more than "
                "24 hours for execution on Kubernetes".format(step=step)
            )

    def runtime_step_cli(
        self,
        cli_args,  # type: "CLIArgs"
        retry_count,  # type: int
        max_user_code_retries,  # type: int
    ):
        # After all attempts to run the user code have failed, we don't need
        # Lambda anymore. We can execute possible fallback code locally, so we'll
        # leave args as is
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["kubernetes", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options.update(self.resource_attributes)
            cli_args.command_options["run-time-limit"] = self.run_time_limit
            if not R.use_r():
                cli_args.entrypoint[0] = sys.executable

    @classmethod
    def _save_package_once(
        cls,
        datastore,  # type: "MetaflowDataStore"
        package,  # type: "MetaflowPackage"
    ):
        if cls.package_url is None:
            cls.package_url = datastore.save_data(
                package.sha, TransformableObject(package.blob)
            )
            cls.package_sha = package.sha

    def runtime_init(
        self,
        flow,  # type: "FlowSpec"
        graph,  # type: "FlowGraph"
        package,  # type: "MetaflowPackage"
        run_id,  # type: str
    ):
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self,
        datastore,  # type: "MetaflowDataStore"
        task_id,  # type: str
        split_index,  # type: Optional[int]
        input_paths,  # type: List[str]
        is_cloned,  # type: bool
    ):
        if not is_cloned:
            self._save_package_once(datastore, self.package)

    def task_pre_step(
        self,
        step_name,
        ds,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
    ):
        if metadata.TYPE == "local":
            self.ds_root = ds.root
        else:
            self.ds_root = None

        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, [])
        self._save_logs_sidecar = SidecarSubProcess("save_logs_periodically")

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        if self.ds_root:
            # We have a local metadata service so we need to persist it to the datastore.
            # Note that the datastore is *always* s3 (see runtime_task_created function)
            sync_local_metadata_to_datastore(self.ds_root, retry_count)
        try:
            self._save_logs_sidecar.kill()
        except:
            pass