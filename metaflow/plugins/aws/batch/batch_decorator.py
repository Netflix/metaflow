import os
import sys
import platform
import requests
import time

from metaflow import util
from metaflow import R, current

from metaflow.decorators import StepDecorator
from metaflow.plugins import ResourcesDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.metadata import MetaDatum
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.metaflow_config import (
    ECS_S3_ACCESS_IAM_ROLE,
    BATCH_JOB_QUEUE,
    BATCH_CONTAINER_IMAGE,
    BATCH_CONTAINER_REGISTRY,
    ECS_FARGATE_EXECUTION_ROLE,
    DATASTORE_LOCAL_DIR,
)
from metaflow.sidecar import Sidecar
from metaflow.unbounded_foreach import UBF_CONTROL

from .batch import BatchException
from ..aws_utils import compute_resource_attributes, get_docker_registry


class BatchDecorator(StepDecorator):
    """
    Step decorator to specify that this step should execute on AWS Batch.

    This decorator indicates that your step should execute on AWS Batch. Note
    that you can apply this decorator automatically to all steps using the
    ```--with batch``` argument when calling run/resume. Step level decorators
    within the code are overrides and will force a step to execute on AWS Batch
    regardless of the ```--with``` specification.

    To use, annotate your step as follows:
    ```
    @batch
    @step
    def my_step(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1. If @resources is
        also present, the maximum value from all decorators is used
    gpu : int
        Number of GPUs required for this step. Defaults to 0. If @resources is
        also present, the maximum value from all decorators is used
    memory : int
        Memory size (in MB) required for this step. Defaults to 4096. If
        @resources is also present, the maximum value from all decorators is
        used
    image : string
        Docker image to use when launching on AWS Batch. If not specified, a
        default docker image mapping to the current version of Python is used
    queue : string
        AWS Batch Job Queue to submit the job to. Defaults to the one
        specified by the environment variable METAFLOW_BATCH_JOB_QUEUE
    iam_role : string
        AWS IAM role that AWS Batch container uses to access AWS cloud resources
        (Amazon S3, Amazon DynamoDb, etc). Defaults to the one specified by the
        environment variable METAFLOW_ECS_S3_ACCESS_IAM_ROLE
    execution_role : string
        AWS IAM role that AWS Batch can use to trigger AWS Fargate tasks.
        Defaults to the one determined by the environment variable
        METAFLOW_ECS_FARGATE_EXECUTION_ROLE https://docs.aws.amazon.com/batch/latest/userguide/execution-IAM-role.html
    shared_memory : int
        The value for the size (in MiB) of the /dev/shm volume for this step.
        This parameter maps to the --shm-size option to docker run.
    max_swap : int
        The total amount of swap memory (in MiB) a container can use for this
        step. This parameter is translated to the --memory-swap option to
        docker run where the value is the sum of the container memory plus the
        max_swap value.
    swappiness : int
        This allows you to tune memory swappiness behavior for this step.
        A swappiness value of 0 causes swapping not to happen unless absolutely
        necessary. A swappiness value of 100 causes pages to be swapped very
        aggressively. Accepted values are whole numbers between 0 and 100.
    """

    name = "batch"
    defaults = {
        "cpu": None,
        "gpu": None,
        "memory": None,
        "image": None,
        "queue": BATCH_JOB_QUEUE,
        "iam_role": ECS_S3_ACCESS_IAM_ROLE,
        "execution_role": ECS_FARGATE_EXECUTION_ROLE,
        "shared_memory": None,
        "max_swap": None,
        "swappiness": None,
        "host_volumes": None,
    }
    resource_defaults = {
        "cpu": "1",
        "gpu": "0",
        "memory": "4096",
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(BatchDecorator, self).__init__(attributes, statically_defined)

        # If no docker image is explicitly specified, impute a default image.
        if not self.attributes["image"]:
            # If metaflow-config specifies a docker image, just use that.
            if BATCH_CONTAINER_IMAGE:
                self.attributes["image"] = BATCH_CONTAINER_IMAGE
            # If metaflow-config doesn't specify a docker image, assign a
            # default docker image.
            else:
                # Metaflow-R has it's own default docker image (rocker family)
                if R.use_r():
                    self.attributes["image"] = R.container_image()
                # Default to vanilla Python image corresponding to major.minor
                # version of the Python interpreter launching the flow.
                else:
                    self.attributes["image"] = "python:%s.%s" % (
                        platform.python_version_tuple()[0],
                        platform.python_version_tuple()[1],
                    )
        # Assign docker registry URL for the image.
        if not get_docker_registry(self.attributes["image"]):
            if BATCH_CONTAINER_REGISTRY:
                self.attributes["image"] = "%s/%s" % (
                    BATCH_CONTAINER_REGISTRY.rstrip("/"),
                    self.attributes["image"],
                )

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    # to understand where these functions are invoked in the lifecycle of a
    # Metaflow flow.
    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        if flow_datastore.TYPE != "s3":
            raise BatchException("The *@batch* decorator requires --datastore=s3.")

        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        self.attributes.update(
            compute_resource_attributes(decos, self, self.resource_defaults)
        )

        # Set run time limit for the AWS Batch job.
        self.run_time_limit = get_run_time_limit_for_task(decos)
        if self.run_time_limit < 60:
            raise BatchException(
                "The timeout for step *{step}* should be at "
                "least 60 seconds for execution on AWS Batch.".format(step=step)
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
            # to execute on AWS Batch anymore. We can execute possible fallback
            # code locally.
            cli_args.commands = ["batch", "step"]
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

        # task_pre_step may run locally if fallback is activated for @catch
        # decorator. In that scenario, we skip collecting AWS Batch execution
        # metadata. A rudimentary way to detect non-local execution is to
        # check for the existence of AWS_BATCH_JOB_ID environment variable.

        if "AWS_BATCH_JOB_ID" in os.environ:
            meta = {}
            meta["aws-batch-job-id"] = os.environ["AWS_BATCH_JOB_ID"]
            meta["aws-batch-job-attempt"] = os.environ["AWS_BATCH_JOB_ATTEMPT"]
            meta["aws-batch-ce-name"] = os.environ["AWS_BATCH_CE_NAME"]
            meta["aws-batch-jq-name"] = os.environ["AWS_BATCH_JQ_NAME"]
            meta["aws-batch-execution-env"] = os.environ["AWS_EXECUTION_ENV"]

            # Capture AWS Logs metadata. This is best effort only since
            # only V4 of the metadata uri for the ECS container hosts this
            # information and it is quite likely that not all consumers of
            # Metaflow would be running the container agent compatible with
            # version V4.
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html
            try:
                logs_meta = (
                    requests.get(url=os.environ["ECS_CONTAINER_METADATA_URI_V4"])
                    .json()
                    .get("LogOptions", {})
                )
                meta["aws-batch-awslogs-group"] = logs_meta.get("awslogs-group")
                meta["aws-batch-awslogs-region"] = logs_meta.get("awslogs-region")
                meta["aws-batch-awslogs-stream"] = logs_meta.get("awslogs-stream")
            except:
                pass

            entries = [
                MetaDatum(
                    field=k,
                    value=v,
                    type=k,
                    tags=["attempt_id:{0}".format(retry_count)],
                )
                for k, v in meta.items()
            ]
            # Register book-keeping metadata for debugging.
            metadata.register_metadata(run_id, step_name, task_id, entries)

            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

        num_parallel = int(os.environ.get("AWS_BATCH_JOB_NUM_NODES", 0))
        if num_parallel >= 1 and ubf_context == UBF_CONTROL:
            # UBF handling for multinode case
            control_task_id = current.task_id
            top_task_id = control_task_id.replace("control-", "")  # chop "-0"
            mapper_task_ids = [control_task_id] + [
                "%s-node-%d" % (top_task_id, node_idx)
                for node_idx in range(1, num_parallel)
            ]
            flow._control_mapper_tasks = [
                "%s/%s/%s" % (run_id, step_name, mapper_task_id)
                for mapper_task_id in mapper_task_ids
            ]
            flow._control_task_is_mapper_zero = True

        if num_parallel >= 1:
            _setup_multinode_environment()

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):

        # task_finished may run locally if fallback is activated for @catch
        # decorator.
        if "AWS_BATCH_JOB_ID" in os.environ:
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

        try:
            self._save_logs_sidecar.terminate()
        except:
            # Best effort kill
            pass

        if is_task_ok and len(getattr(flow, "_control_mapper_tasks", [])) > 1:
            self._wait_for_mapper_tasks(flow, step_name)

    def _wait_for_mapper_tasks(self, flow, step_name):
        """
        When lauching multinode task with UBF, need to wait for the secondary
        tasks to finish cleanly and produce their output before exiting the
        main task. Otherwise main task finishing will cause secondary nodes
        to terminate immediately, and possibly prematurely.
        """
        from metaflow import Step  # avoid circular dependency

        TIMEOUT = 600
        last_completion_timeout = time.time() + TIMEOUT
        print("Waiting for batch secondary tasks to finish")
        while last_completion_timeout > time.time():
            time.sleep(2)
            try:
                step_path = "%s/%s/%s" % (flow.name, current.run_id, step_name)
                tasks = [task for task in Step(step_path)]
                if len(tasks) == len(flow._control_mapper_tasks):
                    if all(
                        task.finished_at is not None for task in tasks
                    ):  # for some reason task.finished fails
                        return True
                else:
                    print(
                        "Waiting for all parallel tasks to finish. Finished: {}/{}".format(
                            len(tasks),
                            len(flow._control_mapper_tasks),
                        )
                    )
            except Exception as e:
                pass
        raise Exception(
            "Batch secondary workers did not finish in %s seconds" % TIMEOUT
        )

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]


def _setup_multinode_environment():
    # setup the multinode environment variables.
    import socket

    if "AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS" not in os.environ:
        # we are the main node
        local_ips = socket.gethostbyname_ex(socket.gethostname())[-1]
        assert local_ips, "Could not find local ip address"
        os.environ["MF_PARALLEL_MAIN_IP"] = local_ips[0]
    else:
        os.environ["MF_PARALLEL_MAIN_IP"] = os.environ[
            "AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS"
        ]
    os.environ["MF_PARALLEL_NUM_NODES"] = os.environ["AWS_BATCH_JOB_NUM_NODES"]
    os.environ["MF_PARALLEL_NODE_INDEX"] = os.environ["AWS_BATCH_JOB_NODE_INDEX"]
