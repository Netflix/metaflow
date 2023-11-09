import os
import sys
import time
import traceback

from metaflow import JSONTypeClass, util
from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY, CommandException
from metaflow.metadata.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, KUBERNETES_LABELS
from metaflow.mflog import TASK_LOG_SOURCE
import metaflow.tracing as tracing

from .kubernetes import Kubernetes, KubernetesKilledException, parse_kube_keyvalue_list
from .kubernetes_decorator import KubernetesDecorator


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kubernetes.")
def kubernetes():
    pass


@tracing.cli_entrypoint("kubernetes/step")
@kubernetes.command(
    help="Execute a single task on Kubernetes. This command calls the top-level step "
    "command inside a Kubernetes pod with the given options. Typically you do not call "
    "this command directly; it is used internally by Metaflow."
)
@click.argument("step-name")
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
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    executable=None,
    image=None,
    image_pull_policy=None,
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
    env = {}
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

    step_cli = "{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint="%s -u %s" % (executable, os.path.basename(sys.argv[0])),
        top_args=" ".join(util.dict_to_cli_options(ctx.parent.parent.params)),
        step=step_name,
        step_args=" ".join(util.dict_to_cli_options(kwargs)),
    )

    # Set log tailing.
    ds = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=kwargs["task_id"],
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
                    kwargs["run_id"], step_name, kwargs["task_id"]
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
                task_id=kwargs["task_id"],
                attempt=str(retry_count),
                user=util.get_username(),
                code_package_sha=code_package_sha,
                code_package_url=code_package_url,
                code_package_ds=ctx.obj.flow_datastore.TYPE,
                step_cli=step_cli,
                docker_image=image,
                docker_image_pull_policy=image_pull_policy,
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
            )
    except Exception as e:
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
