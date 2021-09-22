import os
import sys

import click
import traceback

from metaflow import R, util
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.datastore import FlowDataStore
from metaflow.metadata.util import (
    sync_local_metadata_from_datastore,
)
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

from .lambda_runner import LambdaRunner, LambdaRuntimeException
from metaflow.mflog import TASK_LOG_SOURCE


class CommonTaskAttrs:
    def __init__(
        self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        user,
        version,
    ):
        self.flow_name = flow_name
        self.step_name = step_name
        self.run_id = run_id
        self.task_id = task_id
        self.attempt = attempt
        self.user = user
        self.version = version

    def to_dict(self, key_prefix):
        attrs = {
            key_prefix + "flow_name": self.flow_name,
            key_prefix + "step_name": self.step_name,
            key_prefix + "run_id": self.run_id,
            key_prefix + "task_id": self.task_id,
            key_prefix + "retry_count": str(self.attempt),
            key_prefix + "version": self.version,
        }
        if self.user is not None:
            attrs[key_prefix + "user"] = self.user
        return attrs


@click.group()
def cli():
    pass


@cli.group(help="Commands related to AWS Lambda.")
def awslambda():
    pass


@awslambda.command(
    help="Execute a single task using AWS Lambda. This command "
    "calls the top-level step command inside a AWS Lambda "
    "job with the given options. Typically you do not "
    "call this command directly; it is used internally "
    "by Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--lambda-arn", help="Lambda ARN")
@click.option("--lambda-name", help="Lambda name")
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
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    lambda_arn,
    lambda_name,
    **kwargs
):
    # type: (...) -> None

    def echo_without_prefix(msg, err):
        ctx.obj.echo_always(msg, err=err)

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

    retry_count = kwargs.get("retry_count", 0)

    common_attrs = CommonTaskAttrs(
        flow_name=ctx.obj.flow.name,
        step_name=step_name,
        run_id=kwargs["run_id"],
        task_id=kwargs["task_id"],
        attempt=retry_count,
        user=util.get_username(),
        version=ctx.obj.environment.get_environment_info()["metaflow_version"],
    )

    env_deco = [deco for deco in node.decorators if deco.name == "environment"]
    if env_deco:
        env = env_deco[0].attributes["vars"]
    else:
        env = {}

    def echo_with_prefix(msg, err):  # type: (str, bool) -> None
        ctx.obj.echo_always(
            "[%s] %s"
            % (
                lambda_name,
                msg,
            ),
            err=err,
        )

    runner = LambdaRunner(
        datastore=ctx.obj.flow_datastore,
        environment=ctx.obj.environment,
        lambda_arn=lambda_arn,
        name=lambda_name,
    )

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
        with ctx.obj.monitor.measure("metaflow.awslambda.launch"):
            return_code = runner.run(
                step_name,
                step_cli,
                code_package_sha,
                code_package_url,
                ctx.obj.flow_datastore.TYPE,
                env=env,
                attrs=common_attrs.to_dict(key_prefix="metaflow."),
                echo=echo_without_prefix,
                stdout_location=stdout_location,
                stderr_location=stderr_location,
                task_id=kwargs["task_id"],
                run_id=kwargs["run_id"],
                attempt=str(retry_count),
                flow_name=ctx.obj.flow.name,
            )
            task_datastore = FlowDataStore(
                ctx.obj.flow.name,
                ctx.obj.environment,
                ctx.obj.metadata,
                ctx.obj.event_logger,
                ctx.obj.monitor,
            ).get_task_datastore(kwargs["run_id"], step_name, kwargs["task_id"])
            _sync_metadata()
            if return_code != 0:
                sys.exit(1)
    except LambdaRuntimeException as e:
        echo_with_prefix(
            "Lambda runtime exception (possibly an issue with the container image)",
            True,
        )
        echo_with_prefix(str(e), True)
        task_datastore = FlowDataStore(
            ctx.obj.flow.name,
            ctx.obj.environment,
            ctx.obj.metadata,
            ctx.obj.event_logger,
            ctx.obj.monitor,
        ).get_task_datastore(kwargs["run_id"], step_name, kwargs["task_id"])
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    except Exception as e:
        traceback.print_exc()
        task_datastore = FlowDataStore(
            ctx.obj.flow.name,
            ctx.obj.environment,
            ctx.obj.metadata,
            ctx.obj.event_logger,
            ctx.obj.monitor,
        ).get_task_datastore(kwargs["run_id"], step_name, kwargs["task_id"])
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
