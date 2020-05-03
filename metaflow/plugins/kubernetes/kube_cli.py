import os
import sys
import tarfile
import time
import traceback

import click

from distutils.dir_util import copy_tree

from .kube import Kube, KubeKilledException, KubeException
from metaflow import decorators
from metaflow.graph import FlowGraph

from metaflow.datastore import MetaflowDataStore
from functools import wraps
from metaflow import namespace
from metaflow.datastore.local import LocalDataStore
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow import util
from metaflow.exception import (
    CommandException,
    METAFLOW_EXIT_DISALLOW_RETRY,
)

try:
    # python2
    from urlparse import urlparse
except:  # noqa E722
    # python3
    from urllib.parse import urlparse


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kube.")
def kube():
    pass


def _execute_cmd(func, flow_name, run_id, user, my_runs, echo):
    if user and my_runs:
        raise CommandException("--user and --my-runs are mutually exclusive")

    if run_id and my_runs:
        raise CommandException("--run_id and --my-runs are mutually exclusive")

    if my_runs:
        user = util.get_username()

    latest_run = False

    if user and not run_id:
        latest_run = True

    if not run_id and latest_run:
        run_id = util.get_latest_run_id(echo, flow_name)
        if run_id is None:
            raise CommandException(
                "A previous run id was not found. Specify --run-id.")

    func(flow_name, run_id, user, echo)


def _sync_metadata(echo, metadata, datastore_root, attempt):
    if metadata.TYPE == 'local':
        def echo_none(*args, **kwargs):
            pass
        path = os.path.join(
            datastore_root,
            MetaflowDataStore.filename_with_attempt_prefix('metadata.tgz', attempt))
        url = urlparse(path)
        bucket = url.netloc
        key = url.path.lstrip('/')
        s3, err = get_s3_client()
        try:
            s3.head_object(Bucket=bucket, Key=key)
            # If we are here, we can download the object
            with util.TempDir() as td:
                tar_file_path = os.path.join(td, 'metadata.tgz')
                with open(tar_file_path, 'wb') as f:
                    s3.download_fileobj(bucket, key, f)
                with tarfile.open(tar_file_path, 'r:gz') as tar:
                    tar.extractall(td)
                copy_tree(
                    os.path.join(td, DATASTORE_LOCAL_DIR),
                    LocalDataStore.get_datastore_root_from_config(echo_none),
                    update=True)
        except err as e:  # noqa F841
            pass


@kube.command(help="List running Kube tasks of this flow")
@click.option(
    "--my-runs", default=False, is_flag=True, help="Run the command over all tasks."
)
@click.option("--user", default=None, help="List tasks for the given user.")
@click.option("--run-id", default=None, help="List tasks corresponding to the run id.")
@click.pass_context
def list(ctx, run_id, user, my_runs):
    batch = Kube(ctx.obj.metadata, ctx.obj.environment,ctx.obj.datastore)
    _execute_cmd(
        batch.list_jobs, ctx.obj.flow.name, run_id, user, my_runs, ctx.obj.echo
    )


@kube.command(help="Terminate running Kube tasks of this flow.")
@click.option("--my-runs", default=False, is_flag=True, help="Kill all running tasks.")
@click.option("--user", default=None, help="List tasks for the given user.")
@click.option(
    "--run-id", default=None, help="Terminate tasks corresponding to the run id."
)
@click.pass_context
def kill(ctx, run_id, user, my_runs):
    batch = Kube(ctx.obj.metadata, ctx.obj.environment,ctx.obj.datastore)
    _execute_cmd(
        batch.kill_jobs, ctx.obj.flow.name, run_id, user, my_runs, ctx.obj.echo
    )


@kube.command(
    help="Execute a single task using Kube. This command "
    "calls the top-level step command inside a Kube Job Container"
    "with the given options. Typically you do not "
    "call this command directly; it is used internally "
    "by Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--executable", help="Executable requirement for Kube.")
@click.option(
    "--image", help="Docker image requirement for Kube. In name:version format."
)
@click.option("--cpu", help="CPU requirement for Kube.")
@click.option("--gpu", help="GPU requirement for Kube.")
@click.option("--memory", help="Memory requirement for Kube.")
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
@click.option("--kube_namespace", default=None, help="Name Space to use on Kubernetes. This is not a metaflow namespace")
@click.option("--retry-count", default=0, help="Passed to the top-level 'step'.")
@click.option(
    "--max-user-code-retries", default=0, help="Passed to the top-level 'step'."
)
@click.option(
    "--run-time-limit",
    default=5 * 24 * 60 * 60,
    help="Run time limit in seconds for the Batch job. " "Default is 5 days.",
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    executable=None,
    image=None,
    cpu=None,
    gpu=None,
    memory=None,
    run_time_limit=None,
    kube_namespace=None,
    **kwargs
):
    def echo(batch_id, msg, stream=sys.stdout):
        ctx.obj.echo_always("[%s] %s" % (batch_id, msg))

    if ctx.obj.datastore.datastore_root is None:
        ctx.obj.datastore.datastore_root = ctx.obj.datastore.get_datastore_root_from_config(
            echo)

    if executable is None:
        executable = ctx.obj.environment.executable(step_name)
    entrypoint = "%s -u %s" % (executable, os.path.basename(sys.argv[0]))

    top_args = " ".join(util.dict_to_cli_options(ctx.parent.parent.params))

    input_paths = kwargs.get("input_paths")
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            "METAFLOW_INPUT_PATHS_%d" % (i // max_size): input_paths[i: i + max_size]
            for i in range(0, len(input_paths), max_size)
        }
        kwargs["input_paths"] = "".join("${%s}" % s for s in split_vars.keys())

    step_args = " ".join(util.dict_to_cli_options(kwargs))
    step_cli = u"{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint=entrypoint, top_args=top_args, step=step_name, step_args=step_args
    )
    # Example value of what the result of the step_cli parameter when running remotely on Batch.
    # STEP CLI :  metaflow_HelloAWSFlow_linux-64_42c548a8c1def9ca0d877bf671025b5e69b97f83/bin/python -s -u hello.py --quiet --metadata local --environment conda --datastore s3 --event-logger nullSidecarLogger --monitor nullSidecarMonitor --datastore-root s3://kubernetes-first-test-store/metaflow_store --with batch:cpu=1,gpu=0,memory=1000,image=python:3.7,queue=arn:aws:batch:us-east-1:314726501535:job-queue/Metaflow-Job-Q,iam_role=arn:aws:iam::314726501535:role/Metaflow_Batch_ECS_Role_TEST --package-suffixes .py --pylint step start --run-id 1581986312466293 --task-id 1 --input-paths ${METAFLOW_INPUT_PATHS_0}
    node = ctx.obj.graph[step_name]

    # Get retry information
    retry_count = kwargs.get("retry_count", 0)
    retry_deco = [deco for deco in node.decorators if deco.name == "retry"]
    minutes_between_retries = None
    if retry_deco:
        minutes_between_retries = int(
            retry_deco[0].attributes.get("minutes_between_retries", 1)
        )

    # Set kube attributes
    attrs = {
        "metaflow.user": util.get_username(),
        "metaflow.flow_name": ctx.obj.flow.name,
        "metaflow.step_name": step_name,
        "metaflow.run_id": kwargs["run_id"],
        "metaflow.task_id": kwargs["task_id"],
        "metaflow.retry_count": str(retry_count),
        "metaflow.version": ctx.obj.environment.get_environment_info()[
            "metaflow_version"
        ],
    }

    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env = env_deco[0].attributes["vars"]
    else:
        env = {}

    datastore_root = os.path.join(ctx.obj.datastore.make_path(
        ctx.obj.flow.name, kwargs['run_id'], step_name, kwargs['task_id']))
    # Add the environment variables related to the input-paths argument
    if split_vars:
        env.update(split_vars)

    if retry_count:
        ctx.obj.echo_always(
            "Sleeping %d minutes before the next Batch retry" % minutes_between_retries
        )
        time.sleep(minutes_between_retries * 60)
    kube = Kube(ctx.obj.metadata, ctx.obj.environment,ctx.obj.datastore)
    try:
        with ctx.obj.monitor.measure("metaflow.kube.launch"):
            kube.launch_job(
                step_name,
                step_cli,
                code_package_sha,
                code_package_url,
                ctx.obj.datastore.TYPE,
                image=image,
                cpu=cpu,
                gpu=gpu,
                memory=memory,
                kube_namespace=kube_namespace,
                run_time_limit=run_time_limit,
                env=env,
                attrs=attrs,
            )
    except Exception as e:
        print(e)
        _sync_metadata(echo, ctx.obj.metadata, datastore_root, retry_count)
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        kube.wait(echo=echo)
    except KubeKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        _sync_metadata(echo, ctx.obj.metadata, datastore_root, retry_count)
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    _sync_metadata(echo, ctx.obj.metadata, datastore_root, retry_count)
