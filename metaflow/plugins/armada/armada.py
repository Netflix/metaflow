import json
import os
import shlex

from armada_client.client import ArmadaClient
from armada_client.event import EventType
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
import grpc

import metaflow.metaflow_config as config
from metaflow.exception import MetaflowException
from metaflow.tracing import inject_tracing_vars
from metaflow.mflog import (
    BASH_SAVE_LOGS,
    bash_capture_logs,
    export_mflog_env_vars,
)

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class ArmadaException(MetaflowException):
    headline = "Armada error"


def _get_client(host, port, use_ssl=True):
    grpc_target = f"{host}:{port}"
    if use_ssl:
        channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(grpc_target, channel_credentials)
    else:
        channel = grpc.insecure_channel(grpc_target)

    return ArmadaClient(channel)


def create_queue(host, port, queue, priority_factor=1, use_ssl=True):
    client = _get_client(host, port, use_ssl)

    queue_request = client.create_queue_request(
        name=queue, priority_factor=priority_factor
    )

    try:
        # create_queue returns an empty response on success.
        client.create_queue(queue_request)
    except grpc.RpcError as e:
        code = e.code()
        # Handle queue already existing.
        if code == grpc.StatusCode.ALREADY_EXISTS:
            client.update_queue(queue_request)
        else:
            raise ArmadaException(e) from e
    except Exception as e:
        raise ArmadaException(e) from e


def submit_jobs(host, port, queue, job_set_id, job_request_items, use_ssl=True):
    client = _get_client(host, port, use_ssl)

    response = client.submit_jobs(
        queue=queue, job_set_id=job_set_id, job_request_items=job_request_items
    )
    # Returns a list of job_ids created for this request.
    return [job_item.job_id for job_item in response.job_response_items]


def create_job_request_item(pod_spec, priority=1):
    # We don't actually have to use the grpc channel for this, but we do need
    # a client object.
    client = _get_client("localhost", "1234", use_ssl=False)
    return client.create_job_request_item(
        priority=priority, namespace="default", pod_spec=pod_spec
    )


def create_armada_pod_spec(
    step_name, run_id, config_options, container_args, env_vars, secrets
):
    """
    Create a job with a single container.
    """
    requests = {
        "cpu": api_resource.Quantity(string=config_options["cpu"]),
        "memory": api_resource.Quantity(string=config_options["memory"]),
        "ephemeral-storage": api_resource.Quantity(string=config_options["disk"]),
    }
    limits = {
        "cpu": api_resource.Quantity(string=config_options["cpu"]),
        "memory": api_resource.Quantity(string=config_options["memory"]),
        "ephemeral-storage": api_resource.Quantity(string=config_options["disk"]),
    }

    gpu = config_options["gpu"]
    gpu_vendor = config_options["gpu_vendor"]
    if gpu_vendor is not None and gpu is not None:
        request_limit_key = f"{gpu_vendor.lower()}.com/gpu"
        requests[request_limit_key] = api_resource.Quantity(string=gpu)
        limits[request_limit_key] = api_resource.Quantity(string=gpu)

    container_name = f"metaflow-{step_name}-{run_id}".replace("_", "-")

    # For infomation on where this comes from,
    # see https://github.com/kubernetes/api/blob/master/core/v1/generated.proto
    pod = core_v1.PodSpec(
        containers=[
            core_v1.Container(
                name=container_name,
                # TODO: Allow custom image.
                image="python:3.12",
                args=container_args,
                resources=core_v1.ResourceRequirements(
                    requests=requests, limits=limits
                ),
                env=[core_v1.EnvVar(name=k, value=str(v)) for k, v in env_vars.items()]
                # And some downward API magic. Add (key, value)
                # pairs below to make pod metadata available
                # within Kubernetes container.
                + [
                    core_v1.EnvVar(
                        name=k,
                        valueFrom=core_v1.EnvVarSource(
                            fieldRef=core_v1.ObjectFieldSelector(fieldPath=str(v))
                        ),
                    )
                    for k, v in {
                        "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                        "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                        "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                        "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
                        "METAFLOW_KUBERNETES_NODE_IP": "status.hostIP",
                    }.items()
                ]
                + [
                    core_v1.EnvVar(name=k, value=str(v))
                    for k, v in inject_tracing_vars({}).items()
                ],
                envFrom=[
                    core_v1.EnvFromSource(
                        secretRef=core_v1.SecretEnvSource(
                            name=str(k),
                            # optional=True
                        )
                    )
                    for k in list(secrets) + config.KUBERNETES_SECRETS.split(",")
                    if k
                ],
            )
        ],
    )

    return [create_job_request_item(priority=1, pod_spec=pod)]


def generate_container_command(
    environment,
    datastore,
    flow_name,
    run_id,
    step_name,
    task_id,
    attempt,
    code_package_url,
    step_cmds,
):
    mflog_expr = export_mflog_env_vars(
        flow_name=flow_name,
        run_id=run_id,
        step_name=step_name,
        task_id=task_id,
        retry_count=attempt,
        datastore_type=datastore.TYPE,
        stdout_path=STDOUT_PATH,
        stderr_path=STDERR_PATH,
    )
    init_cmds = environment.get_package_commands(code_package_url, datastore.TYPE)
    init_cmds.append("python3 -m pip install armada_client==0.3.0")
    init_expr = " && ".join(init_cmds)
    step_expr = bash_capture_logs(
        " && ".join(
            environment.bootstrap_commands(step_name, datastore.TYPE) + step_cmds
        )
    )

    # Construct an entry point that
    # 1) initializes the mflog environment (mflog_expr)
    # 2) bootstraps a metaflow environment (init_expr)
    # 3) executes a task (step_expr)

    # The `true` command is to make sure that the generated command
    # plays well with docker containers which have entrypoint set as
    # eval $@
    cmd_str = "true && mkdir -p %s && %s && %s && %s; " % (
        LOGS_DIR,
        mflog_expr,
        init_expr,
        step_expr,
    )
    # After the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario.
    #
    # TODO: Capture hard exit logs in Kubernetes.
    cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
    # For supporting sandboxes, ensure that a custom script is executed before
    # anything else is executed. The script is passed in as an env var.
    cmd_str = (
        '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"} && %s' % cmd_str
    )
    # FIXME: Sleep to make it easy to grab lobs
    return shlex.split('bash -c "sleep 15; %s"' % cmd_str)


def gather_metaflow_config_to_env_vars():
    return {
        "METAFLOW_SERVICE_URL": config.SERVICE_INTERNAL_URL,
        "METAFLOW_SERVICE_HEADERS": json.dumps(config.SERVICE_HEADERS),
        "METAFLOW_DATASTORE_SYSROOT_S3": config.DATASTORE_SYSROOT_S3,
        "METAFLOW_DATATOOLS_S3ROOT": config.DATATOOLS_S3ROOT,
        "METAFLOW_DEFAULT_METADATA": config.DEFAULT_METADATA,
        "METAFLOW_RUNTIME_ENVIRONMENT": "armada",
        "METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE": config.DEFAULT_SECRETS_BACKEND_TYPE,
        "METAFLOW_CARD_S3ROOT": config.CARD_S3ROOT,
        "METAFLOW_DEFAULT_AWS_CLIENT_PROVIDER": config.DEFAULT_AWS_CLIENT_PROVIDER,
        "METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION": config.AWS_SECRETS_MANAGER_DEFAULT_REGION,
        "METAFLOW_S3_ENDPOINT_URL": config.S3_ENDPOINT_URL,
        "METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT": config.AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
        "METAFLOW_DATASTORE_SYSROOT_AZURE": config.DATASTORE_SYSROOT_AZURE,
        "METAFLOW_CARD_AZUREROOT": config.CARD_AZUREROOT,
        "METAFLOW_DATASTORE_SYSROOT_GS": config.DATASTORE_SYSROOT_GS,
        "METAFLOW_CARD_GSROOT": config.CARD_GSROOT,
        "METAFLOW_INIT_SCRIPT": 'echo "init script"',  # config.KUBERNETES_SANDBOX_INIT_SCRIPT,
        "METAFLOW_OTEL_ENDPOINT": config.OTEL_ENDPOINT,
        "METAFLOW_ARMADA_WORKLOAD": 1,
    }


TERMINAL_JOB_STATES = (
    EventType.unable_to_schedule,
    EventType.failed,
    EventType.succeeded,
    EventType.cancelled,
)


def wait_for_job_finish(host, port, queue, job_set_id, job_id, use_ssl=True):
    client = _get_client(host, port, use_ssl)

    events = client.get_job_events_stream(queue, job_set_id)

    for event in events:
        event = client.unmarshal_event_response(event)
        # Look for status events related to our job_id.
        # TODO time-out mechanism.
        if event.message.job_id == job_id:
            if event.type in TERMINAL_JOB_STATES:
                return event

    raise ArmadaException(
        "Reached end of event stream without reaching terminal job state"
    )


def wait_for_job_finish_generator(host, port, queue, job_set_id, job_id, use_ssl=True):
    client = _get_client(host, port, use_ssl)

    events = client.get_job_events_stream(queue, job_set_id)

    for event in events:
        event = client.unmarshal_event_response(event)
        # Look for status events related to our job_id.
        # TODO time-out mechanism.
        if event.message.job_id == job_id:
            yield event

    raise ArmadaException(
        "Reached end of event stream without reaching terminal job state"
    )
