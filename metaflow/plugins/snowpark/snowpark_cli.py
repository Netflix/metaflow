import json
import os
import sys
import time
import traceback
import atexit
import copy
import json
import os
import select
import shlex
import time
from metaflow import util
from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.metadata.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import (
    CARD_S3ROOT,
    DATASTORE_LOCAL_DIR,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_METADATA,
    SERVICE_HEADERS,
    SERVICE_URL,
)
from metaflow.mflog import TASK_LOG_SOURCE
from metaflow import util
from metaflow.plugins.datatools.s3.s3tail import S3Tail
from metaflow.plugins.aws.aws_utils import sanitize_batch_tag
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
    SERVICE_INTERNAL_URL,
    DATATOOLS_S3ROOT,
    DATASTORE_SYSROOT_S3,
    DEFAULT_METADATA,
    SERVICE_HEADERS,
    BATCH_EMIT_TAGS,
    CARD_S3ROOT,
    S3_ENDPOINT_URL,
    DEFAULT_SECRETS_BACKEND_TYPE,
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    S3_SERVER_SIDE_ENCRYPTION,
)

from metaflow.metaflow_config_funcs import config_values

from metaflow.mflog import (
    export_mflog_env_vars,
    bash_capture_logs,
    tail_logs,
    BASH_SAVE_LOGS,
)


from .snowpark import SnowparkJob, SnowparkException, SnowparkKilledException


from metaflow.mflog import (
    export_mflog_env_vars,
    bash_capture_logs,
    tail_logs,
    BASH_SAVE_LOGS,
)

# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


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
@click.pass_context
def step(ctx, step_name, code_package_sha, code_package_url, **kwargs):
    def echo(msg, stream="stderr", _id=None, **kwargs):
        msg = util.to_unicode(msg)
        if _id:
            msg = "[%s] %s" % (_id, msg)
        ctx.obj.echo_always(msg, err=(stream == sys.stderr), **kwargs)

    node = ctx.obj.graph[step_name]

    # Get retry information
    retry_count = kwargs.get("retry_count", 0)
    retry_deco = [deco for deco in node.decorators if deco.name == "retry"]
    minutes_between_retries = None
    if retry_deco:
        minutes_between_retries = int(
            retry_deco[0].attributes.get("minutes_between_retries", 1)
        )

    # Construct container entrypoint
    executable = ctx.obj.environment.executable(step_name)
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
    # construct an entry point that
    # 1) initializes the mflog environment (mflog_expr)
    # 2) bootstraps a metaflow environment (init_expr)
    # 3) executes a task (step_expr)
    mflog_expr = export_mflog_env_vars(
        datastore_type=ctx.obj.flow_datastore.TYPE,
        stdout_path=STDOUT_PATH,
        stderr_path=STDERR_PATH,
        flow_name=ctx.obj.flow.name,
        step_name=step_name,
        run_id=kwargs["run_id"],
        task_id=kwargs["task_id"],
        retry_count=str(retry_count),
    )
    init_expr = " && ".join(
        ctx.obj.environment.get_package_commands(
            code_package_url, ctx.obj.flow_datastore.TYPE
        )
    )
    step_expr = bash_capture_logs(
        " && ".join(
            ctx.obj.environment.bootstrap_commands(
                step_name, ctx.obj.flow_datastore.TYPE
            )
            + [step_cli]
        )
    )
    cmd_str = "mkdir -p %s && %s && %s && %s; " % (
        LOGS_DIR,
        mflog_expr,
        init_expr,
        step_expr,
    )
    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
    cmd = shlex.split('bash -c "%s"' % cmd_str)

    # TODO: Introduce Azure/GCP bits
    env = {
        "METAFLOW_CODE_SHA": code_package_sha,
        "METAFLOW_CODE_URL": code_package_url,
        "METAFLOW_CODE_DS": ctx.obj.flow_datastore.TYPE,
        "METAFLOW_SERVICE_URL": SERVICE_URL,
        # "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
        "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
        "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
        "METAFLOW_DEFAULT_DATASTORE": ctx.obj.flow_datastore.TYPE,
        "METAFLOW_USER": util.get_username(),
        "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
        "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
        "METAFLOW_RUNTIME_ENVIRONMENT": "snowpark",
    }

    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env.update(env_deco[0].attributes["vars"])

    # Add the environment variables related to the input-paths argument
    if split_vars:
        env.update(split_vars)

    if retry_count:
        ctx.obj.echo_always(
            "Sleeping %d minutes before the next retry" % minutes_between_retries
        )
        time.sleep(minutes_between_retries * 60)

    # this information is needed for log tailing
    ds = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=kwargs["task_id"],
        attempt=int(retry_count),
    )
    stdout_location = ds.get_log_location(TASK_LOG_SOURCE, "stdout")
    stderr_location = ds.get_log_location(TASK_LOG_SOURCE, "stderr")


    snowpark_job = SnowparkJob(cmd=cmd, env=env)
    try:
        with ctx.obj.monitor.measure("metaflow.snowpark.launch_job"):
            query_id = snowpark_job.submit(compute_pool="tutorial_compute_pool")
    except Exception as e:
        traceback.print_exc()
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    
    try:
        prefix = b"[%s] " % util.to_bytes(query_id)
        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        status = snowpark_job.status["status"]
        echo(
                "Task is starting (status %s)..." % status,
                "stderr",
                _id=query_id,
            )
        t = time.time()
        # 1) Loop until the job has started
        while True:
            if status != snowpark_job.status["status"] or (time.time() - t) > 30:
                status = snowpark_job.status["status"]
                echo(
                    "Task is starting (status %s)..." % (status),
                    "stderr",
                    _id=query_id,
                )
                t = time.time()
            if snowpark_job.is_running or snowpark_job.has_finished:
                break
            select.poll().poll(200)

        # 2) Tail logs until the job has finished
        tail_logs(
            prefix=prefix,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            echo=echo,
            has_log_updates=lambda: snowpark_job.is_running,
        )

        if snowpark_job.has_failed:
            msg = snowpark_job.status.get("message", "Task crashed.")
            if snowpark_job.status.get("transient", True):
                msg = "%s\n This could be a transient error. Use @retry to retry." % msg
            raise SnowparkException(msg)
        else:
            if snowpark_job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise SnowparkException("Task failed!")
            echo(
                "Task finished with exit code %s." % -1, # snowpark_job.status_code,
                "stderr",
                _id=query_id,
            )
    except SnowparkKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, kwargs["task_id"]
                ),
            )
