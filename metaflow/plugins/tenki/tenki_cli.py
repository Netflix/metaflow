import os
import sys
import time
import traceback

import metaflow.tracing as tracing
from metaflow import util
from metaflow._vendor import click
from metaflow.exception import CommandException, METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.metadata_provider.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    TENKI_API_KEY,
    TENKI_BASE_URL,
)
from metaflow.mflog import TASK_LOG_SOURCE

from .tenki import (
    Tenki,
    TENKI_TAG,
    TENKI_TAG_FLOW,
    TENKI_TAG_RUN,
    TENKI_TAG_USER,
    _tag,
    is_permanent_launch_error,
)
from .tenki_client import TenkiClient, TenkiKilledException


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Tenki sandboxes.")
def tenki():
    pass


def _resolve_scope(flow_name, run_id, user, my_runs, echo):
    # Mirror @kubernetes / @batch (parse_cli_options) but harden the default:
    # with no scope flags, resolve to the *current user's* latest run of this
    # flow, never just "the flow's latest run". This keeps a destructive default
    # `tenki kill` from ever touching another user's sandboxes on shared storage.
    # Replicated locally (as @batch does) to keep the plugin self-contained.
    if user and my_runs:
        raise CommandException("--user and --my-runs are mutually exclusive.")
    if run_id and my_runs:
        raise CommandException("--run-id and --my-runs are mutually exclusive.")
    if my_runs:
        user = util.get_username()
    # A user filter alone means "all of that user's runs of this flow"; a run id
    # alone targets exactly that run. With nothing specified, default to the
    # current user's latest run (both the user and run-id scopes apply).
    if not run_id and not user:
        user = util.get_username()
        run_id = util.get_latest_run_id(echo, flow_name)
        if run_id is None:
            raise CommandException("A previous run id was not found. Specify --run-id.")
    return run_id, user


def _matching_sandboxes(flow_name, run_id, user):
    # Always scope by flow so cleanup can never touch another flow's sandboxes.
    tags = [TENKI_TAG, _tag(TENKI_TAG_FLOW, flow_name)]
    if run_id:
        tags.append(_tag(TENKI_TAG_RUN, run_id))
    if user:
        tags.append(_tag(TENKI_TAG_USER, user))
    # Reuse the same client configuration as launch so `list`/`kill` work when
    # credentials are supplied only via Metaflow config, not the environment.
    client = TenkiClient(api_key=TENKI_API_KEY, base_url=TENKI_BASE_URL)
    return client.list_sandboxes(tags=tags)


def _terminate_sandboxes(sandboxes, echo):
    # Return (killed, failed). For each sandbox try every teardown method in
    # turn and don't give up after the first failure (mirrors
    # tenki.py._cleanup); only report a failure when *no* method worked, so a
    # recovered error never prints a false-alarm "Failed" line.
    killed = 0
    failed = 0
    for sb in sandboxes:
        name = getattr(sb, "name", "?")
        terminated = False
        last_error = None
        for method in ("terminate", "close", "delete"):
            fn = getattr(sb, method, None)
            if fn is None:
                continue
            try:
                fn()
                terminated = True
                break
            except Exception as e:
                last_error = e
        if terminated:
            killed += 1
            echo("Terminated %s." % name)
        else:
            failed += 1
            reason = last_error if last_error is not None else "no teardown method"
            echo("Failed to terminate %s: %s" % (name, reason))
    return killed, failed


@tenki.command(name="list", help="List running Metaflow-launched Tenki sandboxes.")
@click.option(
    "--my-runs",
    default=False,
    is_flag=True,
    help="List all my sandboxes of this flow.",
)
@click.option("--run-id", default=None, help="Only sandboxes for this run id.")
@click.option("--user", default=None, help="Only sandboxes launched by this user.")
@click.pass_context
def list_sandboxes(ctx, run_id, user, my_runs):
    run_id, user = _resolve_scope(
        ctx.obj.flow.name, run_id, user, my_runs, ctx.obj.echo_always
    )
    sandboxes = _matching_sandboxes(ctx.obj.flow.name, run_id, user)
    for sb in sandboxes:
        ctx.obj.echo_always(
            "%s [%s]" % (getattr(sb, "name", "?"), getattr(sb, "state", "?"))
        )
    ctx.obj.echo_always("%d sandbox(es) found." % len(sandboxes))


@tenki.command(help="Terminate running Metaflow-launched Tenki sandboxes.")
@click.option(
    "--my-runs",
    default=False,
    is_flag=True,
    help="Terminate all my sandboxes of this flow.",
)
@click.option(
    "--run-id", default=None, help="Only terminate sandboxes for this run id."
)
@click.option(
    "--user", default=None, help="Only terminate sandboxes launched by this user."
)
@click.pass_context
def kill(ctx, run_id, user, my_runs):
    run_id, user = _resolve_scope(
        ctx.obj.flow.name, run_id, user, my_runs, ctx.obj.echo_always
    )
    sandboxes = _matching_sandboxes(ctx.obj.flow.name, run_id, user)
    if not sandboxes:
        ctx.obj.echo_always("No matching sandboxes to terminate.")
        return
    killed, failed = _terminate_sandboxes(sandboxes, ctx.obj.echo_always)
    ctx.obj.echo_always("Terminated %d sandbox(es)." % killed)
    if failed:
        ctx.obj.echo_always("Failed to terminate %d sandbox(es)." % failed)


@tenki.command(
    help="Execute a single task inside a Tenki sandbox. This command calls the "
    "top-level step command inside a Tenki microVM with the given options. "
    "Typically you do not call this command directly; it is used internally by "
    "Metaflow."
)
@tracing.cli("tenki/step")
@click.argument("step-name")
@click.argument("code-package-metadata")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--executable", help="Executable requirement for the Tenki sandbox.")
@click.option("--image", help="Base image requirement for the Tenki sandbox.")
@click.option("--cpu", help="CPU requirement for the Tenki sandbox.")
@click.option("--memory", help="Memory requirement for the Tenki sandbox.")
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
@click.option(
    "--run-time-limit",
    default=5 * 24 * 60 * 60,  # Default is set to 5 days
    help="Run time limit in seconds for the Tenki sandbox.",
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
    cpu=None,
    memory=None,
    run_time_limit=None,
    **kwargs
):
    def echo(msg, stream="stderr", job_id=None, **kwargs):
        msg = util.to_unicode(msg)
        if job_id:
            msg = "[%s] %s" % (job_id, msg)
        ctx.obj.echo_always(msg, err=(stream == sys.stderr), **kwargs)

    node = ctx.obj.graph[step_name]

    # Construct entrypoint CLI.
    executable = ctx.obj.environment.executable(step_name, executable)

    # Set environment.
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

    task_id = kwargs["task_id"]

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
        task_id=task_id,
        attempt=int(retry_count),
    )
    stdout_location = ds.get_log_location(TASK_LOG_SOURCE, "stdout")
    stderr_location = ds.get_log_location(TASK_LOG_SOURCE, "stderr")

    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    kwargs["run_id"], step_name, task_id
                ),
            )

    # Bound before the try so the except-block can safely read tenki._client
    # even if the Tenki(...) constructor itself were to raise.
    tenki = None
    try:
        tenki = Tenki(
            datastore=ctx.obj.flow_datastore,
            metadata=ctx.obj.metadata,
            environment=ctx.obj.environment,
        )
        with ctx.obj.monitor.measure("metaflow.tenki.launch_job"):
            tenki.launch_job(
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
                image=image,
                cpu=cpu,
                memory=memory,
                run_time_limit=run_time_limit,
                env=env,
            )
    except Exception as e:
        traceback.print_exc(chain=False)
        _sync_metadata()
        # launch_job does synchronous network I/O (auth, project discovery,
        # sandbox create), so a transient Tenki API/network failure must stay
        # retryable when the step has @retry. Only permanent auth/permission/
        # misconfig errors (and our own non-retryable signals) disallow retry;
        # everything else exits non-zero so @retry can relaunch.
        if is_permanent_launch_error(e, client=getattr(tenki, "_client", None)):
            sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
        sys.exit(1)

    try:
        tenki.wait(stdout_location, stderr_location, echo=echo)
    except TenkiKilledException:
        # Do not retry killed / non-retryable tasks.
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        _sync_metadata()
