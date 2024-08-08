import os
import sys
import time
import traceback

from metaflow._vendor import click
from metaflow import util
from metaflow import R
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.metadata.util import sync_local_metadata_from_datastore
from metaflow.mflog import TASK_LOG_SOURCE

from .snowpark_exceptions import SnowparkKilledException
from .snowpark import Snowpark


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Snowpark.")
def snowpark():
    pass


@snowpark.command(
    help="Execute a single task using Snowpark. This command calls the "
    "top-level step command inside a Snowpark job with the given options. "
    "Typically you do not call this command directly; it is used internally by "
    "Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option(
    "--executable", help="Executable requirement for Snowpark Container Services."
)
# below are snowpark specific options
@click.option(
    "--account",
    help="Account ID for Snowpark Container Services.",
)
@click.option(
    "--user",
    help="Username for Snowpark Container Services.",
)
@click.option(
    "--password",
    help="Password for Snowpark Container Services.",
)
@click.option(
    "--role",
    help="Role for Snowpark Container Services.",
)
@click.option(
    "--database",
    help="Database for Snowpark Container Services.",
)
@click.option(
    "--warehouse",
    help="Warehouse for Snowpark Container Services.",
)
@click.option(
    "--schema",
    help="Schema for Snowpark Container Services.",
)
@click.option(
    "--image",
    help="Docker image requirement for Snowpark Container Services. In name:version format.",
)
@click.option("--stage", help="Stage requirement for Snowpark Container Services.")
@click.option(
    "--compute-pool", help="Compute Pool requirement for Snowpark Container Services."
)
@click.option("--volume-mounts", multiple=True)
@click.option(
    "--external-integration",
    multiple=True,
    help="External Integration requirement for Snowpark Container Services.",
)
@click.option("--cpu", help="CPU requirement for Snowpark Container Services.")
@click.option("--gpu", help="GPU requirement for Snowpark Container Services.")
@click.option("--memory", help="Memory requirement for Snowpark Container Services.")
# TODO: secrets, volumes for snowpark
# TODO: others to consider: ubf-context, num-parallel
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
# TODO: this is not used anywhere as of now...
@click.option(
    "--run-time-limit",
    default=5 * 24 * 60 * 60,  # Default is set to 5 days
    help="Run time limit in seconds for Snowpark container.",
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    executable=None,
    account=None,
    user=None,
    password=None,
    role=None,
    database=None,
    warehouse=None,
    schema=None,
    image=None,
    stage=None,
    compute_pool=None,
    volume_mounts=None,
    external_integration=None,
    cpu=None,
    gpu=None,
    memory=None,
    run_time_limit=None,
    **kwargs
):
    def echo(msg, stream="stderr", job_id=None, **kwargs):
        msg = util.to_unicode(msg)
        if job_id:
            msg = "[%s] %s" % (job_id, msg)
        ctx.obj.echo_always(msg, err=(stream == sys.stderr), **kwargs)

    if R.use_r():
        entrypoint = R.entrypoint()
    else:
        executable = ctx.obj.environment.executable(step_name, executable)
        entrypoint = "%s -u %s" % (executable, os.path.basename(sys.argv[0]))

    top_args = " ".join(util.dict_to_cli_options(ctx.parent.parent.params))

    input_paths = kwargs.get("input_paths")
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            "METAFLOW_INPUT_PATHS_%d" % (i // max_size): input_paths[i : i + max_size]
            for i in range(0, len(input_paths), max_size)
        }
        kwargs["input_paths"] = "".join("${%s}" % s for s in split_vars.keys())

    step_args = " ".join(util.dict_to_cli_options(kwargs))

    step_cli = "{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint=entrypoint,
        top_args=top_args,
        step=step_name,
        step_args=step_args,
    )
    node = ctx.obj.graph[step_name]

    # Get retry information
    retry_count = kwargs.get("retry_count", 0)
    retry_deco = [deco for deco in node.decorators if deco.name == "retry"]
    minutes_between_retries = None
    if retry_deco:
        minutes_between_retries = int(
            retry_deco[0].attributes.get("minutes_between_retries", 1)
        )
    if retry_count:
        ctx.obj.echo_always(
            "Sleeping %d minutes before the next retry" % minutes_between_retries
        )
        time.sleep(minutes_between_retries * 60)

    # Set batch attributes
    task_spec = {
        "flow_name": ctx.obj.flow.name,
        "step_name": step_name,
        "run_id": kwargs["run_id"],
        "task_id": kwargs["task_id"],
        "retry_count": str(retry_count),
    }
    attrs = {"metaflow.%s" % k: v for k, v in task_spec.items()}
    attrs["metaflow.user"] = util.get_username()
    attrs["metaflow.version"] = ctx.obj.environment.get_environment_info()[
        "metaflow_version"
    ]

    # Set environment
    env = {}
    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env = env_deco[0].attributes["vars"]

    # Add the environment variables related to the input-paths argument
    if split_vars:
        env.update(split_vars)

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

    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, kwargs["task_id"]
                ),
            )

    try:
        snowpark = Snowpark(
            datastore=ctx.obj.flow_datastore,
            metadata=ctx.obj.metadata,
            environment=ctx.obj.environment,
            client_credentials={
                "account": account,
                "user": user,
                "password": password,
                "role": role,
                "database": database,
                "warehouse": warehouse,
                "schema": schema,
            },
        )
        with ctx.obj.monitor.measure("metaflow.snowpark.launch_job"):
            snowpark.launch_job(
                step_name=step_name,
                step_cli=step_cli,
                task_spec=task_spec,
                code_package_sha=code_package_sha,
                code_package_url=code_package_url,
                code_package_ds=ctx.obj.flow_datastore.TYPE,
                image=image,
                stage=stage,
                compute_pool=compute_pool,
                volume_mounts=volume_mounts,
                external_integration=external_integration,
                cpu=cpu,
                gpu=gpu,
                memory=memory,
                run_time_limit=run_time_limit,
                env=env,
                attrs=attrs,
            )
    except Exception:
        traceback.print_exc(chain=False)
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        snowpark.wait(stdout_location, stderr_location, echo=echo)
    except SnowparkKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        _sync_metadata()
