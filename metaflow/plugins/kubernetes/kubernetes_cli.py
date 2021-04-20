import os
import sys

import click
from metaflow import R, util
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.mflog import TASK_LOG_SOURCE
from metaflow.plugins.aws.utils import CommonTaskAttrs, sync_local_metadata_from_datastore, get_datastore_root, get_writeable_datastore

from .kubernetes_runner import KubernetesJobRunner

if sys.version_info > (3, 0):
    from typing import Dict, Optional


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kubernetes.")
def kubernetes():
    pass


@kubernetes.command(
    help="Execute a single task on a Kubernetes cluster. This command "
    "calls the top-level step command inside a Kubernetes cluster "
    "with the given options. Typically you do not "
    "call this command directly; it is used internally "
    "by Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--cpu", type=int, help="CPU request for the pod")
@click.option("--memory", type=int, help="Memory request for the pod (MB)")
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--image", help="Docker image")
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
@click.option(
    "--run-time-limit",
    default=10 * 60,
    type=int,
    help="Run time limit in seconds for the Lambda job. " "Default is 10 minutes.",
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    image,  # type: str
    run_time_limit, # type: int
    memory=None, # type: Optional[int]
    cpu=None, # type: Optional[float]
    **kwargs
):
    # type: (...) -> None

    def echo(msg, err):  # type (str, bool) -> None
        ctx.obj.echo_always("%s" % (msg,), err=err)

    if ctx.obj.datastore.datastore_root is None:
        ctx.obj.datastore.datastore_root = (
            ctx.obj.datastore.get_datastore_root_from_config(echo)
        )

    if R.use_r():
        entrypoint = R.entrypoint()
    else:
        executable = ctx.obj.environment.executable(step_name)
        entrypoint = "%s -u %s" % (executable, os.path.basename(sys.argv[0]))

    top_args = " ".join(util.dict_to_cli_options(ctx.parent.parent.params))

    step_args = " ".join(util.dict_to_cli_options(kwargs))
    step_cli = u"{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint=entrypoint, top_args=top_args, step=step_name, step_args=step_args
    )
    node = ctx.obj.graph[step_name]

    # Get retry information. TODO: actually use it somewhere.
    retry_count = kwargs.get("retry_count", 0)

    # Set attributes

    common_attrs = CommonTaskAttrs(
        flow_name=ctx.obj.flow.name,
        step_name=step_name,
        run_id=kwargs["run_id"],
        task_id=kwargs["task_id"],
        attempt=retry_count,
        user=util.get_username(),
            version=ctx.obj.environment.get_environment_info()[
            "metaflow_version"
        ]
    )

    # Set attributes
    attrs = common_attrs.to_dict(key_prefix="metaflow.")

    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env = env_deco[0].attributes["vars"]
    else:
        env = {}

    datastore_root = get_datastore_root(common_attrs, ctx.obj.datastore)

    ds = get_writeable_datastore(common_attrs, ctx.obj.datastore)
    stdout_location = ds.get_log_location(TASK_LOG_SOURCE, "stdout")
    stderr_location = ds.get_log_location(TASK_LOG_SOURCE, "stderr")

    from kubernetes import client, config

    config.load_kube_config()
    batch_v1 = client.BatchV1Api()

    runner = KubernetesJobRunner(
        batch_v1,
        environment=ctx.obj.environment,
    )
    try:
        with ctx.obj.monitor.measure("metaflow.kubernetes.launch"):
            return_code = runner.run(
                step_name=step_name,
                step_cli=step_cli,
                image=image,
                code_package_sha=code_package_sha,
                code_package_url=code_package_url,
                task_id=common_attrs.task_id,
                run_id=common_attrs.run_id,
                attempt=str(common_attrs.attempt),
                flow_name=common_attrs.flow_name,
                code_package_ds=ctx.obj.datastore.TYPE,
                env=env,
                attrs=attrs,
                echo=echo,
                stdout_location=stdout_location,
                stderr_location=stderr_location,
                run_time_limit=run_time_limit,
                cpu=cpu,
                memory=memory,
            )
            sync_local_metadata_from_datastore(ctx.obj.metadata, datastore_root, retry_count)
            if return_code != 0:
                sys.exit(1)
    except Exception as e:
        print(e)
        sync_local_metadata_from_datastore(ctx.obj.metadata, datastore_root, retry_count)
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
