import os
import sys

from metaflow import R

from metaflow.decorators import Decorator, StepDecorator
from metaflow.exception import MetaflowException
from metaflow.plugins.aws.awslambda.lambda_runner import LAMBDA_MAX_MEMORY_MB
from metaflow.plugins.aws.batch.batch_decorator import (
    BatchDecorator,
    ResourcesDecorator,
)
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.plugins.conda.conda_step_decorator import CondaStepDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from .lambda_runner import ensure_lambda, lambda_name
from metaflow.sidecar import SidecarSubProcess
from metaflow.metadata import MetaDatum
from ..aws_utils import get_docker_registry, compute_resource_attributes


if sys.version_info > (3, 0):
    from typing import TYPE_CHECKING, Dict, List, Optional, Callable

    if TYPE_CHECKING:
        from metaflow.runtime import CLIArgs
        from metaflow.graph import FlowGraph
        from metaflow.package import MetaflowPackage
        from metaflow.flowspec import FlowSpec
        from metaflow.datastore import FlowDataStore
        from metaflow.datastore.task_datastore import TaskDataStore
        from metaflow.metaflow_environment import MetaflowEnvironment
        from metaflow.event_logger import EventLogger

from metaflow.metaflow_config import (
    LAMBDA_CONTAINER_REGISTRY,
    LAMBDA_CONTAINER_IMAGE,
    LAMBDA_ROLE_ARN,
    DATASTORE_LOCAL_DIR,
)

DEFAULT_TIMEOUT_SECONDS = 900
LAMBDA_DEFAULT_MEMORY_MB = 4096


class LambdaException(MetaflowException):
    headline = "AWS Lambda error"


class LambdaDecorator(StepDecorator):
    """
    The decorator is called @awslambda to avoid confusion, since "lambda" is a reserved
    word in Python.
    """

    name = "awslambda"
    defaults = {
        "image": None,
        "memory": None,
        "refresh_image": False,
    }
    resource_defaults = {
        "memory": "4096",
    }

    package_url = None  # type: Optional[str]
    package_sha = None  # type: Optional[str]
    memory_mb = LAMBDA_DEFAULT_MEMORY_MB  # type: int
    run_time_limit = None  # type: Optional[int]
    ds_root = None  # type: Optional[str]
    lambda_arn = None  # type: Optional[str]
    lambda_name = None  # type: Optional[str]

    def __init__(self, attributes=None, statically_defined=False):
        super(LambdaDecorator, self).__init__(attributes, statically_defined)

        if not self.attributes["image"]:
            # If metaflow-config specifies a docker image, just use that.
            if LAMBDA_CONTAINER_IMAGE:
                self.attributes["image"] = LAMBDA_CONTAINER_IMAGE
            # If metaflow-config doesn't specify a docker image, throw an error.
            # Unlike with other platforms, AWS Lambda needs a special image
            # and it has to be uploaded in users ECR repo, so we cannot provide
            # a good default here.
            else:
                raise LambdaException(
                    "The *@awslambda* decorator requires either image parameter or "
                    "METAFLOW_LAMBDA_CONTAINER_IMAGE to be set to a lambda container image uri"
                )
        if not get_docker_registry(self.attributes["image"]):
            if LAMBDA_CONTAINER_REGISTRY:
                self.attributes["image"] = "%s/%s" % (
                    LAMBDA_CONTAINER_REGISTRY.rstrip("/"),
                    self.attributes["image"],
                )

    def step_init(
        self,
        flow,  # type: "FlowSpec"
        graph,  # type : "FlowGraph"
        step,  # type: str
        decos,  # type: List[Decorator]
        environment,  # type: "MetaflowEnvironment"
        flow_datastore,  # type: "FlowDataStore"
        logger,  # type: "Callable[[str], None]"
    ):
        if flow_datastore.TYPE != "s3":
            raise LambdaException("The *@awslambda* decorator requires --datastore=s3.")

        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        for deco in decos:
            if isinstance(deco, CondaStepDecorator):
                raise LambdaException(
                    "*@awslambda* doesn't support using @conda. "
                    "We recommend using custom container images to manage"
                    " dependencies instead"
                )

            if isinstance(deco, BatchDecorator):
                raise LambdaException(
                    "You can't use @batch and *@awslambda* at the same time"
                )

        resource_attrs = compute_resource_attributes(
            decos, self, self.resource_defaults
        )
        if int(resource_attrs["memory"]) > LAMBDA_MAX_MEMORY_MB:
            raise LambdaException(
                "{r} cannot exceed {max_v} for *@awslambda* (currently set to {v})".format(
                    r="memory", max_v=LAMBDA_MAX_MEMORY_MB, v=resource_attrs["memory"]
                )
            )

        # ignore all other resource attrs, we only support setting memory for lambda
        self.memory_mb = int(resource_attrs["memory"])

        # Run time limit more than 15m is effectively ignored
        self.run_time_limit = min(
            get_run_time_limit_for_task(decos),
            900,
        )

        if self.run_time_limit < 10:
            raise LambdaException(
                "The timeout for step *{step}* should be at "
                "least 10 seconds for execution on AWS Lambda".format(step=step)
            )

    def runtime_step_cli(
        self,
        cli_args,  # type: "CLIArgs"
        retry_count,  # type: int
        max_user_code_retries,  # type: int
        ubf_context,
    ):
        # After all attempts to run the user code have failed, we don't need
        # Lambda anymore. We can execute possible fallback code locally, so we'll
        # leave args as is
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["awslambda", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options["lambda_arn"] = self.lambda_arn
            cli_args.command_options["lambda_name"] = self.lambda_name
            if not R.use_r():
                cli_args.entrypoint[0] = sys.executable

    @classmethod
    def _save_package_once(
        cls,
        flow_datastore,  # type: "FlowDataStore"
        package,  # type: "MetaflowPackage"
    ):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]

    def package_init(
        self,
        flow,  # type: "FlowSpec"
        step_name,  # type: str
        environment,  # type: "MetaflowEnvironment"
        echo,  # type: Callable[..., None]
    ):
        # self.step_init(..) should've run by now and populated this and other
        # attributes. This assert makes mypy happy as it cannot infer that statically.
        assert self.run_time_limit is not None

        self.lambda_name = lambda_name(
            self.memory_mb, self.run_time_limit, self.attributes["image"]
        )

        def echo_with_prefix(msg, err):  # type: (str, bool) -> None
            echo(
                "[awslambda decorator] %s" % (msg,),
                err=err,
            )

        if LAMBDA_ROLE_ARN is None:
            raise LambdaException(
                "The *@lambda* decorator requires METAFLOW_LAMBDA_ROLE_ARN to be set to a "
                "lambda role arn"
            )

        self.lambda_arn = ensure_lambda(
            name=self.lambda_name,
            memory_mb=self.memory_mb,
            timeout_seconds=self.run_time_limit,
            role_arn=LAMBDA_ROLE_ARN,
            image_uri=self.attributes["image"],
            refresh_image=self.attributes["refresh_image"],
            echo=echo_with_prefix,
        )

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
        task_datastore,  # type: "TaskDataStore"
        task_id,  # type: str
        split_index,  # type: Optional[int]
        input_paths,  # type: List[str]
        is_cloned,  # type: bool
        ubf_context,
    ):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

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
        if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
            meta["aws-lambda-function-name"] = os.environ["AWS_LAMBDA_FUNCTION_NAME"]
            meta["aws-lambda-log-group-name"] = os.environ["AWS_LAMBDA_LOG_GROUP_NAME"]
            meta["aws-lambda-log-stream-name"] = os.environ[
                "AWS_LAMBDA_LOG_STREAM_NAME"
            ]
            meta["aws-lambda-execution-env"] = os.environ["AWS_EXECUTION_ENV"]

            # Unlike the above vars, this is set by our own code in entrypoint.py
            meta["aws-lambda-request-id"] = os.environ["LAMBDA_REQUEST_ID"]

            entries = [
                MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()
            ]

            # Register book-keeping metadata for debugging.
            metadata.register_metadata(run_id, step_name, task_id, entries)
            self._save_logs_sidecar = SidecarSubProcess("save_logs_periodically")

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        # task_post_step may run locally if fallback is activated for @catch
        # decorator.
        if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
            # If `local` metadata is configured, we would need to copy task
            # execution metadata from the AWS Batch container to user's
            # local file system after the user code has finished execution.
            # This happens via datastore as a communication bridge.
            if self.metadata.TYPE == "local":
                # Note that the datastore is *always* Amazon S3 (see
                # runtime_task_created function).
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

    def task_exception(
        self, exception, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        # task_exception may run locally if fallback is activated for @catch
        # decorator.
        if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
            # If `local` metadata is configured, we would need to copy task
            # execution metadata from the AWS Batch container to user's
            # local file system after the user code has finished execution.
            # This happens via datastore as a communication bridge.
            if self.metadata.TYPE == "local":
                # Note that the datastore is *always* Amazon S3 (see
                # runtime_task_created function).
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        try:
            self._save_logs_sidecar.kill()
        except:
            # Best effort kill
            pass
