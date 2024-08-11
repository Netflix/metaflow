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

from .slurm_exceptions import SlurmException
from .slurm import Slurm


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Slurm.")
def slurm():
    pass


@slurm.command(
    help="Execute a single task using Slurm. This command calls the "
    "top-level step command inside a Slurm job with the given options. "
    "Typically you do not call this command directly; it is used internally by "
    "Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--executable", help="Executable requirement for Slurm.")
# below are slurm specific options
@click.option("--username", help="Username for login node for Slurm.")
@click.option("--address", help="IP Address of login node for Slurm.")
@click.option("--ssh-key-file", help="SSH key file for login node for Slurm.")
@click.option("--cert-file", help="Certificate file for login node for Slurm.")
@click.option("--remote-workdir", help="Remote working directory for Slurm.")
@click.option(
    "--cleanup", help="Cleanup created artifacts on Slurm.", is_flag=True, default=False
)
@click.option("--partition", help="partition requirement for Slurm.")
@click.option("--nodes", help="nodes requirement for Slurm.")
@click.option("--ntasks", help="ntasks requirement for Slurm.")
@click.option("--ntasks-per-node", help="ntasks per node requirement for Slurm.")
@click.option("--cpus-per-task", help="cpus per task requirement for Slurm.")
@click.option("--memory", help="memory requirement for Slurm.")
@click.option("--memory-per-cpu", help="memory per cpu requirement for Slurm.")
@click.option("--constraint", help="constraint requirement for Slurm.")
@click.option("--nodelist", help="nodelist requirement for Slurm.")
@click.option("--exclude", help="exclude requirement for Slurm.")
@click.option("--gres", help="generic resources per node for Slurm.")
# TODO: any other params for slurm??
# TODO: others to consider: ubf-context, num-parallel??
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
    help="Run time limit in seconds for Slurm job.",
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    executable=None,
    username=None,
    address=None,
    ssh_key_file=None,
    cert_file=None,
    remote_workdir=None,
    cleanup=False,
    partition=None,
    nodes=None,
    ntasks=None,
    ntasks_per_node=None,
    cpus_per_task=None,
    memory=None,
    memory_per_cpu=None,
    constraint=None,
    nodelist=None,
    exclude=None,
    gres=None,
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
        slurm = Slurm(
            datastore=ctx.obj.flow_datastore,
            metadata=ctx.obj.metadata,
            environment=ctx.obj.environment,
            slurm_access_params={
                "username": username,
                "address": address,
                "ssh_key_file": ssh_key_file,
                "cert_file": cert_file,
                "remote_workdir": remote_workdir,
                "cleanup": cleanup,
            },
        )
        with ctx.obj.monitor.measure("metaflow.slurm.launch_job"):
            slurm.launch_job(
                step_name=step_name,
                step_cli=step_cli,
                task_spec=task_spec,
                code_package_sha=code_package_sha,
                code_package_url=code_package_url,
                code_package_ds=ctx.obj.flow_datastore.TYPE,
                partition=partition,
                nodes=nodes,
                ntasks=ntasks,
                ntasks_per_node=ntasks_per_node,
                cpus_per_task=cpus_per_task,
                memory=memory,
                memory_per_cpu=memory_per_cpu,
                constraint=constraint,
                nodelist=nodelist,
                exclude=exclude,
                gres=gres,
                run_time_limit=run_time_limit,
                env=env,
                attrs=attrs,
            )
    except Exception:
        traceback.print_exc(chain=False)
        _sync_metadata()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        slurm.wait(stdout_location, stderr_location, echo=echo)
    except SlurmException:
        # don't retry killed tasks
        traceback.print_exc()
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    finally:
        _sync_metadata()
