import os
import sys
from threading import Event, Thread

from armada_client.event import EventType

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
    wait_for_job_finish_generator,
    TERMINAL_JOB_STATES,
)

from .logging import log_thread


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
@click.option(
    "--secrets",
    multiple=True,
    default=None,
    help="Secrets for pod launched under Armada.",
)
@click.argument("queue", required=True)
@click.argument("job-set-id", required=True)
@click.option(
    "--executable",
    help="Executable requirement for Armada Kubernetes pod.",
)
@click.option("--host", default=None, help="The hostname for Armada.")
@click.option("--port", default=None, help="The port for Armada.")
@click.option(
    "--logging-host",
    default=None,
    help="The hostname of the Armada logging host.",
)
@click.option(
    "--logging-port",
    default=None,
    help="The port for Armada logging port.",
)
@click.option("--cpu", help="CPU requirement for Armada job.")
@click.option("--disk", help="Disk requirement for Armada job.")
@click.option("--memory", help="Memory requirement for Armada job.")
@click.option("--gpu", help="GPU requirement for Armada job.")
@click.option("--gpu-vendor", help="GPU vendor requirement for Armada job.")
@click.option(
    "--insecure-no-ssl",
    is_flag=True,
    help="Turn off SSL for Armada connections. Useful for local debugging.",
)
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--input-paths", help="Passed to the top-level 'step'.")
@click.option("--split-index", help="Passed to the top-level 'step'.")
@click.option("--clone-path", help="Passed to the top-level 'step'.")
@click.option("--clone-run-id", help="Passed to the top-level 'step'.")
@click.option(
    "--tag", multiple=True, default=None, help="Passed to the top-level 'step'."
)
@click.option("--namespace", default="default", help="Passed to the top-level 'step'.")
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
    host,
    port,
    logging_host,
    logging_port,
    cpu,
    disk,
    memory,
    gpu,
    gpu_vendor,
    insecure_no_ssl,
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

    job_logs = True
    logging_hostname = os.getenv("LOGGING_HOST", logging_host)
    if logging_hostname is None:
        job_logs = False

    logging_portnum = os.getenv("LOGGING_PORT", logging_port)
    if logging_portnum is None:
        job_logs = False

    if not job_logs:
        ctx.obj.echo_always(
            "Logging host and port are not set, logging for armada jobs "
            "will not be available."
        )

    if queue == "":
        raise MetaflowException("queue name must be set")

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
    env_vars["METAFLOW_DEFAULT_DATASTORE"] = ctx.obj.flow_datastore.TYPE

    # TODO: Better secrets injection/support other storage backends.
    env_vars["AWS_ACCESS_KEY_ID"] = os.environ["AWS_ACCESS_KEY_ID"]
    env_vars["AWS_SECRET_ACCESS_KEY"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    env_vars["AWS_DEFAULT_REGION"] = os.environ["AWS_DEFAULT_REGION"]

    job_request_items = create_armada_pod_spec(
        step_name,
        kwargs["run_id"],
        {
            "cpu": cpu,
            "disk": disk,
            "memory": memory,
            "gpu": gpu,
            "gpu_vendor": gpu_vendor,
        },
        container_cmd,
        env_vars,
        [],
    )

    use_ssl = True
    if insecure_no_ssl:
        use_ssl = False

    # Note that only one armada job is created per metaflow step.
    job_ids = submit_jobs(
        hostname, portnum, queue, job_set_id, job_request_items, use_ssl=use_ssl
    )
    ctx.obj.echo_always(f"Submitted job with ID: {job_ids[0]}")

    gen = wait_for_job_finish_generator(
        hostname, portnum, queue, job_set_id, job_ids[0], use_ssl=use_ssl
    )

    done_signal = Event()
    job_logging_thread = None

    for event in gen:
        ctx.obj.echo_always(f"Armada job entered {event.type} state.")
        if event.type == EventType.running and job_logs:
            job_logging_thread = Thread(
                target=log_thread,
                args=(
                    logging_hostname,
                    logging_portnum,
                    job_ids[0],
                    done_signal,
                    ctx.obj.echo_always,
                ),
                kwargs={"use_ssl": use_ssl},
            )
            job_logging_thread.start()

        if event.type in TERMINAL_JOB_STATES:
            ctx.obj.echo_always(f"Armada job reached terminal state: {event.message}")
            break

    # Clean up log thread.
    if job_logs:
        done_signal.set()
        job_logging_thread.join()

    task_datastore = ctx.obj.flow_datastore.get_task_datastore(
        mode="w",
        run_id=kwargs["run_id"],
        step_name=step_name,
        task_id=kwargs["task_id"],
        attempt=int(retry_count),
    )

    sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, task_datastore)

    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                ctx.obj.flow_datastore.get_task_datastore(
                    run_id=kwargs["run_id"],
                    step_name=step_name,
                    task_id=kwargs["task_id"],
                ),
            )

    _sync_metadata()
