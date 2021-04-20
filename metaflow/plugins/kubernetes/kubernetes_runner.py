import json
import random
import string
import sys
import atexit

from datetime import datetime, timedelta

from requests.api import delete

from metaflow.datastore.util.s3tail import S3Tail
from metaflow.mflog import delayed_update_while
from metaflow.mflog.mflog import refine
from metaflow.plugins.aws.utils import build_task_command

if sys.version_info > (3, 0):
    from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Any
    from datetime import timezone

    if TYPE_CHECKING:
        from metaflow.metaflow_environment import MetaflowEnvironment
        from kubernetes.client.api.batch_v1_api import BatchV1Api

from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_METADATA,
    METADATA_SERVICE_HEADERS,
    METADATA_SERVICE_URL,
)


def generate_job_name(step_name):
    return (
        random.choice(string.ascii_lowercase)
        + "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(5)]
        )
        + "-"
        + step_name
    )


def create_job_object(
    job_name,  # type: str
    image,  # type: str
    command,  # type: List[str]
    env,  # type: Dict[str, str]
    labels,  # type: Dict[str, str]
    run_time_limit,  # type: int
    cpu,  # type: Optional[float]
    memory,  # type: Optional[int]
):
    from kubernetes import client

    # Configureate Pod template container
    container = client.V1Container(
        name=job_name,
        image=image,
        command=command,
        env=[client.V1EnvVar(name=k, value=v) for k, v in env.items()],
    )

    if cpu or memory:
        container.resources = client.V1ResourceRequirements()
        requests = {}
        if cpu:
            requests["cpu"] = str(cpu)
        if memory:
            requests["memory"] = str(memory) + "M"
        container.resources.requests = requests

    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "pi"}),
        spec=client.V1PodSpec(restart_policy="Never", containers=[container]),
    )
    # Create the specification of deployment
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=1,  # retries handled by Metaflow
        ttl_seconds_after_finished=600,
        active_deadline_seconds=run_time_limit,
    )

    # Instantiate the job object
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name, labels=labels),
        spec=spec,
    )

    return job


def submit_job(batch_api, job, namespace):
    # NB. namespace here is k8s namespace, not metaflow namespace
    api_response = batch_api.create_namespaced_job(body=job, namespace=namespace)
    # print("Job created. status='%s'" % str(api_response.status))


def delete_job(batch_api, namespace, name):
    from kubernetes.client.rest import ApiException

    try:
        batch_api.delete_namespaced_job(
            name,
            namespace=namespace,
            propagation_policy="Background",
        )
    except ApiException as e:
        if e.status == 404:
            # Race conditions are possible here if other workers will
            # attempt the cleanup as well.
            pass
        else:
            raise


def cleanup_jobs(
    batch_api,
    namespace,  # type: str
    ttl,  # type: timedelta
):
    """
    Clean up completed job resources.

    Until the bright future arrives in [1], we need to do this ourselves.

    [1] https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/
    """
    from kubernetes.client.rest import ApiException

    jobs = batch_api.list_namespaced_job(namespace=namespace, label_selector="metaflow")
    for job in jobs.items:
        if (job.status.succeeded or job.status.failed) and (
            job.status.completion_time
            and job.status.completion_time < datetime.now(timezone.utc) - ttl
        ):
            delete_job(batch_api, job.metadata.name, namespace)


class KubernetesJobRunner:
    _running_job = None  # type: Optional[Tuple[str, str]]
    _batch_api = None  # type: Optional[BatchV1Api]

    def __init__(
        self,
        batch_api,  # type: BatchV1Api
        environment,  # type: "MetaflowEnvironment"
    ):
        self._environment = environment
        self._batch_api = batch_api

        atexit.register(self._kill_running)

    def _kill_running(self):
        if self._running_job is not None:
            namespace, job_name = self._running_job
            delete_job(self._batch_api, namespace, job_name)

    def run(
        self,
        step_name,  # type: str
        step_cli,  # type: str
        image,  # type: str
        flow_name,  # type: str
        run_id,  # type: str
        task_id,  # type: str
        attempt,  # type: str
        code_package_sha,  # type: str
        code_package_url,  # type: str
        code_package_ds,  # type: str
        env,  # type: Dict[str, str]
        attrs,  # type: Dict[str, str]
        echo,  # type: Callable[[str, bool], None]
        stdout_location,  # type: str
        stderr_location,  # type: str
        run_time_limit,  # type: int
        cpu,  # type: Optional[float]
        memory,  # type: Optional[int]
    ):
        # type: (...) -> int

        env_dict = {
            "METAFLOW_DEFAULT_DATASTORE": "s3",
            "METAFLOW_CODE_DS": code_package_ds,
            "METAFLOW_CODE_SHA": code_package_sha,
            "METAFLOW_CODE_URL": code_package_url,
            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
            "METAFLOW_USER": attrs["metaflow.user"],
        }

        if METADATA_SERVICE_URL:
            env["METAFLOW_SERVICE_URL"] = METADATA_SERVICE_URL
        if METADATA_SERVICE_HEADERS:
            env["METAFLOW_SERVICE_HEADERS"] = json.dumps(METADATA_SERVICE_HEADERS)
        if DEFAULT_METADATA:
            env["METAFLOW_DEFAULT_METADATA"] = DEFAULT_METADATA

        env_dict.update(env)

        job_name = generate_job_name(step_name)
        k8s_namespace = "default"

        job_object = create_job_object(
            job_name=job_name,
            image=image,
            run_time_limit=run_time_limit,
            command=build_task_command(
                code_package_url=code_package_url,
                environment=self._environment,
                step_name=step_name,
                step_cmds=[step_cli],
                flow_name=flow_name,
                run_id=run_id,
                task_id=task_id,
                attempt=attempt,
                stdout_path="/tmp/logs/stdout.log",
                stderr_path="/tmp/logs/stderr.log",
                extra_post_init_commands=[
                    "pip install kubernetes",
                ],
            ),
            env=env_dict,
            labels={"metaflow": "metaflow"},
            cpu=cpu,
            memory=memory,
        )

        submit_job(self._batch_api, job_object, namespace=k8s_namespace)

        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        def _print_available_from_s3():
            try:
                for line in stdout_tail:
                    line = refine(line, prefix="[%s] " % job_name)
                    echo(line.strip().decode("utf-8", errors="replace"), False)
                for line in stderr_tail:
                    line = refine(line, prefix="[%s] " % job_name)
                    echo(line.strip().decode("utf-8", errors="replace"), True)
            except Exception as ex:
                echo("[ temporary error in fetching logs: %s ]" % ex, True)

        def _check_running():
            job = self._batch_api.read_namespaced_job_status(
                job_name, namespace=k8s_namespace
            )  # type: Any
            if job.status.succeeded:
                return 0
            elif job.status.failed:
                return 1
            else:
                return None

        # Set this so atexit() can clean it up if this process gets terminated
        self._running_job = k8s_namespace, job_name

        res = delayed_update_while(
            condition=_check_running,
            update_fn=_print_available_from_s3,
        )
        _print_available_from_s3()

        self._running_job = None

        cleanup_jobs(
            self._batch_api, namespace=k8s_namespace, ttl=timedelta(seconds=120)
        )
        return res
