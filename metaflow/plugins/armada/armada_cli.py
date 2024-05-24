import os
import sys

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow import util
from metaflow.metadata.util import (
    sync_local_metadata_from_datastore,
    sync_local_metadata_to_datastore,
)
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
import metaflow.tracing as tracing

from .armada import (
    submit_jobs,
    create_armada_pod_spec,
    generate_container_command,
    gather_metaflow_config_to_env_vars,
    wait_for_job_finish,
)


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Armada.")
def armada():
    pass


@tracing.cli_entrypoint("armada/step")
@armada.command(
    help="Submit this step to Armada. If not using --host and --port, then "
    "set ARMADA_HOST and ARMADA_PORT environment variables appropriately."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.argument("queue", required=True)
@click.argument("job-set-id", required=True)
@click.argument("job-file", required=True)
@click.option(
    "--executable",
    help="Executable requirement for Armada Kubernetes pod.",
)
@click.option("--host", default=None, help="The hostname for Armada.")
@click.option("--port", default=None, help="The port for Armada.")
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
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    queue,
    job_set_id,
    job_file,
    host,
    port,
    executable,
    **kwargs,
):
    hostname = os.getenv("ARMADA_HOST", host)
    if hostname is None:
        raise MetaflowException(
            "Host cannot be None. Either use --host or set ARMADA_HOST"
        )

    portnum = os.getenv("ARMADA_PORT", port)
    if portnum is None:
        raise MetaflowException(
            "Port cannot be None. Either use --port or set ARMADA_PORT"
        )

    if queue == "":
        raise MetaflowException("queue name must be set")

    # with open(job_file, "r", encoding="utf-8") as f:
    # TODO: Read and verify.
    # job_contents = f.read()

    retry_count = int(kwargs.get("retry_count", 0))
    _ = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=kwargs["task_id"],
        attempt=int(retry_count),
    )

    executable = ctx.obj.environment.executable(step_name, executable)

    step_cli = "{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint="%s -u %s" % (executable, os.path.basename(sys.argv[0])),
        top_args=" ".join(util.dict_to_cli_options(ctx.parent.parent.params)),
        step=step_name,
        step_args=" ".join(util.dict_to_cli_options(kwargs)),
    )

    container_cmd = generate_container_command(
        ctx.obj.environment,
        ctx.obj.flow_datastore,
        ctx.obj.flow.name,
        kwargs["run_id"],
        step_name,
        kwargs["task_id"],
        retry_count,
        code_package_url,
        [step_cli],
    )

    env_vars = gather_metaflow_config_to_env_vars()
    env_vars["METAFLOW_CODE_SHA"] = code_package_sha
    env_vars["METAFLOW_CODE_URL"] = code_package_url
    env_vars["METAFLOW_CODE_DS"] = ctx.obj.flow_datastore.TYPE
    env_vars["METAFLOW_USER"] = util.get_username()
    # FIXME: Is this right?
    env_vars["METAFLOW_DEFAULT_DATASTORE"] = ctx.obj.flow_datastore.TYPE

    job_request_items = create_armada_pod_spec(container_cmd, env_vars, [])

    # FIXME: We should probably limit submission to one job per step (ie the
    # step itself executing under armada)
    job_ids = submit_jobs(
        hostname, portnum, queue, job_set_id, job_request_items, use_ssl=False
    )
    print(f"job_ids: {job_ids}")
    print(f"{kwargs['run_id']}, {step_name}, {kwargs['task_id']}")
    ctx.obj.echo_always(f"Submitted jobs with IDs: {job_ids}")

    # FIXME: Need to await job completion?
    event = wait_for_job_finish(
        hostname, portnum, queue, job_set_id, job_ids[0], use_ssl=False
    )
    print(f"Got job finish: {event.message}")

    task_datastore = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=kwargs["task_id"],
        attempt=int(retry_count),
    )

    # FIXME: Supposed to be done with task...
    ctx.obj.echo_always("Sync metadata to !")
    sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, task_datastore)

    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            ctx.obj.echo_always("Sync metadata from!")
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    run_id=kwargs["run_id"],
                    step_name=step_name,
                    task_id=kwargs["task_id"],
                ),
            )
            ctx.obj.echo_always("Sync done!")

    _sync_metadata()
