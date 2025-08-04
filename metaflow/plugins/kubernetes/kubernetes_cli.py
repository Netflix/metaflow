import os
import sys
import time
import traceback

from metaflow.plugins.kubernetes.kube_utils import (
    parse_cli_options,
    parse_kube_keyvalue_list,
)
from metaflow.plugins.kubernetes.kubernetes_client import KubernetesClient
import metaflow.tracing as tracing
from metaflow import JSONTypeClass, util
from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY, MetaflowException
from metaflow.metadata_provider.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.mflog import TASK_LOG_SOURCE
from metaflow.unbounded_foreach import UBF_CONTROL, UBF_TASK

from .kubernetes import (
    Kubernetes,
    KubernetesException,
    KubernetesKilledException,
)


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kubernetes.")
def kubernetes():
    pass


@kubernetes.command(
    help="Execute a single task on Kubernetes. This command calls the top-level step "
    "command inside a Kubernetes pod with the given options. Typically you do not call "
    "this command directly; it is used internally by Metaflow."
)
@tracing.cli("kubernetes/step")
@click.argument("step-name")
@click.argument("code-package-metadata")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option(
    "--executable",
    help="Executable requirement for Kubernetes pod.",
)
@click.option("--image", help="Docker image requirement for Kubernetes pod.")
@click.option(
    "--image-pull-policy",
    default=None,
    help="Optional Docker Image Pull Policy for Kubernetes pod.",
)
@click.option(
    "--image-pull-secrets",
    default=None,
    type=JSONTypeClass(),
    multiple=False,
)
@click.option(
    "--service-account",
    help="IRSA requirement for Kubernetes pod.",
)
@click.option(
    "--secrets",
    multiple=True,
    default=None,
    help="Secrets for Kubernetes pod.",
)
@click.option(
    "--node-selector",
    multiple=True,
    default=None,
    help="NodeSelector for Kubernetes pod.",
)
@click.option(
    # Note that ideally we would have liked to use `namespace` rather than
    # `k8s-namespace` but unfortunately, `namespace` is already reserved for
    # Metaflow namespaces.
    "--k8s-namespace",
    default=None,
    help="Namespace for Kubernetes job.",
)
@click.option("--cpu", help="CPU requirement for Kubernetes pod.")
@click.option("--disk", help="Disk requirement for Kubernetes pod.")
@click.option("--memory", help="Memory requirement for Kubernetes pod.")
@click.option("--gpu", help="GPU requirement for Kubernetes pod.")
@click.option("--gpu-vendor", help="GPU vendor requirement for Kubernetes pod.")
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--input-paths", help="Passed to the top-level 'step'.")
@click.option("--split-index", help="Passed to the top-level 'step'.")
@click.option("--clone-path", help="Passed to the top-level 'step'.")
@click.option("--clone-run-id", help="Passed to the top-level 'step'.")
@click.option(
    "--tag", multiple=True, default=None, help="Passed to the top-level 'step'."
)
@click.option("--namespace", default=None, help="Passed to the top-level 'step'.")
@click.option("--retry-count", default=0, help="Passed to the top-level 'step'.")
@click.option(
    "--max-user-code-retries", default=0, help="Passed to the top-level 'step'."
)
@click.option("--use-tmpfs", is_flag=True, help="tmpfs requirement for Kubernetes pod.")
@click.option(
    "--tmpfs-tempdir", is_flag=True, help="tmpfs requirement for Kubernetes pod."
)
@click.option("--tmpfs-size", help="tmpfs requirement for Kubernetes pod.")
@click.option("--tmpfs-path", help="tmpfs requirement for Kubernetes pod.")
@click.option(
    "--run-time-limit",
    default=5 * 24 * 60 * 60,  # Default is set to 5 days
    help="Run time limit in seconds for Kubernetes pod.",
)
@click.option(
    "--persistent-volume-claims", type=JSONTypeClass(), default=None, multiple=False
)
@click.option(
    "--tolerations",
    default=None,
    type=JSONTypeClass(),
    multiple=False,
)
@click.option("--shared-memory", default=None, help="Size of shared memory in MiB")
@click.option("--port", default=None, help="Port number to expose from the container")
@click.option(
    "--ubf-context", default=None, type=click.Choice([None, UBF_CONTROL, UBF_TASK])
)
@click.option(
    "--num-parallel",
    default=None,
    type=int,
    help="Number of parallel nodes to run as a multi-node job.",
)
@click.option(
    "--qos",
    default=None,
    type=str,
    help="Quality of Service class for the Kubernetes pod",
)
@click.option(
    "--labels",
    default=None,
    type=JSONTypeClass(),
    multiple=False,
)
@click.option(
    "--annotations",
    default=None,
    type=JSONTypeClass(),
    multiple=False,
)
@click.option(
    "--security-context",
    default=None,
    type=JSONTypeClass(),
    multiple=False,
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_metadata,
    code_package_sha,
    code_package_url,
    executable=None,
    image=None,
    image_pull_policy=None,
    image_pull_secrets=None,
    service_account=None,
    secrets=None,
    node_selector=None,
    k8s_namespace=None,
    cpu=None,
    disk=None,
    memory=None,
    gpu=None,
    gpu_vendor=None,
    use_tmpfs=None,
    tmpfs_tempdir=None,
    tmpfs_size=None,
    tmpfs_path=None,
    run_time_limit=None,
    persistent_volume_claims=None,
    tolerations=None,
    shared_memory=None,
    port=None,
    num_parallel=None,
    qos=None,
    labels=None,
    annotations=None,
    security_context=None,
    **kwargs
):
    def echo(msg, stream="stderr", job_id=None, **kwargs):
        msg = util.to_unicode(msg)
        if job_id:
            msg = "[%s] %s" % (job_id, msg)
        ctx.obj.echo_always(msg, err=(stream == sys.stderr), **kwargs)

    node = ctx.obj.graph[step_name]

    # Construct entrypoint CLI
    executable = ctx.obj.environment.executable(step_name, executable)

    # Set environment
    env = {"METAFLOW_FLOW_FILENAME": os.path.basename(sys.argv[0])}
    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env = env_deco[0].attributes["vars"]

    # Set input paths.
    input_paths = kwargs.get("input_paths")
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            "METAFLOW_INPUT_PATHS_%d" % (i // max_size): input_paths[i : i + max_size]
            for i in range(0, len(input_paths), max_size)
        }
        kwargs["input_paths"] = "".join("${%s}" % s for s in split_vars.keys())
        env.update(split_vars)

    if num_parallel is not None and num_parallel <= 1:
        raise KubernetesException(
            "Using @parallel with `num_parallel` <= 1 is not supported with "
            "@kubernetes. Please set the value of `num_parallel` to be greater than 1."
        )

    # Set retry policy.
    retry_count = int(kwargs.get("retry_count", 0))
    retry_deco = [deco for deco in node.decorators if deco.name == "retry"]
    minutes_between_retries = None
    if retry_deco:
        minutes_between_retries = int(
            retry_deco[0].attributes.get("minutes_between_retries", 2)
        )
    if retry_count:
        ctx.obj.echo_always(
            "Sleeping %d minutes before the next retry" % minutes_between_retries
        )
        time.sleep(minutes_between_retries * 60)

    # Explicitly Remove `ubf_context` from `kwargs` so that it's not passed as a commandline option
    # If an underlying step command is executing a vanilla Kubernetes job, then it should never need
    # to know about the UBF context.
    # If it is a jobset which is executing a multi-node job, then the UBF context is set based on the
    # `ubf_context` parameter passed to the jobset.
    kwargs.pop("ubf_context", None)
    # `task_id` is also need to be removed from `kwargs` as it needs to be dynamically
    # set in the downstream code IF num_parallel is > 1
    task_id = kwargs["task_id"]
    if num_parallel:
        kwargs.pop("task_id")

    step_cli = "{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint="%s -u %s" % (executable, os.path.basename(sys.argv[0])),
        top_args=" ".join(util.dict_to_cli_options(ctx.parent.parent.params)),
        step=step_name,
        step_args=" ".join(util.dict_to_cli_options(kwargs)),
    )
    # Since it is a parallel step there are some parts of the step_cli that need to be modified
    # based on the type of worker in the JobSet. This is why we will create a placeholder string
    # in the template which will be replaced based on the type of worker.

    if num_parallel:
        step_cli = "%s {METAFLOW_PARALLEL_STEP_CLI_OPTIONS_TEMPLATE}" % step_cli

    # Set log tailing.
    ds = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=task_id,
        attempt=int(retry_count),
    )
    stdout_location = ds.get_log_location(TASK_LOG_SOURCE, "stdout")
    stderr_location = ds.get_log_location(TASK_LOG_SOURCE, "stderr")

    # `node_selector` is a tuple of strings, convert it to a dictionary
    node_selector = parse_kube_keyvalue_list(node_selector)

    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, task_id
                ),
            )

    try:
        kubernetes = Kubernetes(
            datastore=ctx.obj.flow_datastore,
            metadata=ctx.obj.metadata,
            environment=ctx.obj.environment,
        )
        # Configure and launch Kubernetes job.
        with ctx.obj.monitor.measure("metaflow.kubernetes.launch_job"):
            kubernetes.launch_job(
                flow_name=ctx.obj.flow.name,
                run_id=kwargs["run_id"],
                step_name=step_name,
                task_id=task_id,
                attempt=str(retry_count),
                user=util.get_username(),
                code_package_metadata=code_package_metadata,
                code_package_sha=code_package_sha,
                code_package_url=code_package_url,
                code_package_ds=ctx.obj.flow_datastore.TYPE,
                step_cli=step_cli,
                docker_image=image,
                docker_image_pull_policy=image_pull_policy,
                image_pull_secrets=image_pull_secrets,
                service_account=service_account,
                secrets=secrets,
                node_selector=node_selector,
                namespace=k8s_namespace,
                cpu=cpu,
                disk=disk,
                memory=memory,
                gpu=gpu,
                gpu_vendor=gpu_vendor,
                use_tmpfs=use_tmpfs,
                tmpfs_tempdir=tmpfs_tempdir,
                tmpfs_size=tmpfs_size,
                tmpfs_path=tmpfs_path,
                run_time_limit=run_time_limit,
                env=env,
                persistent_volume_claims=persistent_volume_claims,
                tolerations=tolerations,
                shared_memory=shared_memory,
                port=port,
                num_parallel=num_parallel,
                qos=qos,
                labels=labels,
                annotations=annotations,
                security_context=security_context,
            )
    except Exception:
        traceback.print_exc(chain=False)
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        kubernetes.wait(stdout_location, stderr_location, echo=echo)
    except KubernetesKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        _sync_metadata()


@kubernetes.command(help="List unfinished Kubernetes tasks of this flow.")
@click.option(
    "--my-runs",
    default=False,
    is_flag=True,
    help="List all my unfinished tasks.",
)
@click.option("--user", default=None, help="List unfinished tasks for the given user.")
@click.option(
    "--run-id",
    default=None,
    help="List unfinished tasks corresponding to the run id.",
)
@click.pass_obj
def list(obj, run_id, user, my_runs):
    flow_name, run_id, user = parse_cli_options(
        obj.flow.name, run_id, user, my_runs, obj.echo
    )
    kube_client = KubernetesClient()
    pods = kube_client.list(obj.flow.name, run_id, user)

    def format_timestamp(timestamp=None):
        if timestamp is None:
            return "-"
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    for pod in pods:
        obj.echo(
            "Run: *{run_id}* "
            "Pod: *{pod_id}* "
            "Started At: {startedAt} "
            "Status: *{status}*".format(
                run_id=pod.metadata.annotations.get(
                    "metaflow/run_id",
                    pod.metadata.labels.get("workflows.argoproj.io/workflow"),
                ),
                pod_id=pod.metadata.name,
                startedAt=format_timestamp(pod.status.start_time),
                status=pod.status.phase,
            )
        )

    if not pods:
        obj.echo("No active Kubernetes pods found.")


@kubernetes.command(
    help="Terminate unfinished Kubernetes tasks of this flow. Killed pods may result in newer attempts when using @retry."
)
@click.option(
    "--my-runs",
    default=False,
    is_flag=True,
    help="Kill all my unfinished tasks.",
)
@click.option(
    "--user",
    default=None,
    help="Terminate unfinished tasks for the given user.",
)
@click.option(
    "--run-id",
    default=None,
    help="Terminate unfinished tasks corresponding to the run id.",
)
@click.pass_obj
def kill(obj, run_id, user, my_runs):
    flow_name, run_id, user = parse_cli_options(
        obj.flow.name, run_id, user, my_runs, obj.echo
    )

    if run_id is not None and run_id.startswith("argo-") or user == "argo-workflows":
        raise MetaflowException(
            "Killing pods launched by Argo Workflows is not supported. "
            "Use *argo-workflows terminate* instead."
        )

    kube_client = KubernetesClient()
    kube_client.kill_pods(flow_name, run_id, user, obj.echo)
