from metaflow._vendor import click

from .. import decorators, namespace
from ..cli import echo_always, echo_dev_null
from ..cli_args import cli_args
from ..exception import CommandException
from ..task import MetaflowTask
from ..unbounded_foreach import UBF_CONTROL, UBF_TASK
from ..util import decompress_list
import metaflow.tracing as tracing


@click.command(help="Internal command to execute a single task.", hidden=True)
@tracing.cli("cli/step")
@click.argument("step-name")
@click.option(
    "--run-id",
    default=None,
    required=True,
    help="ID for one execution of all steps in the flow.",
)
@click.option(
    "--task-id",
    default=None,
    required=True,
    show_default=True,
    help="ID for this instance of the step.",
)
@click.option(
    "--input-paths",
    help="A comma-separated list of pathspecs specifying inputs for this step.",
)
@click.option(
    "--input-paths-filename",
    type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
    help="A filename containing the argument typically passed to `input-paths`",
    hidden=True,
)
@click.option(
    "--split-index",
    type=int,
    default=None,
    show_default=True,
    help="Index of this foreach split.",
)
@click.option(
    "--tag",
    "opt_tag",
    multiple=True,
    default=None,
    help="Annotate this run with the given tag. You can specify "
    "this option multiple times to attach multiple tags in "
    "the task.",
)
@click.option(
    "--namespace",
    "opt_namespace",
    default=None,
    help="Change namespace from the default (your username) to the specified tag.",
)
@click.option(
    "--retry-count",
    default=0,
    help="How many times we have attempted to run this task.",
)
@click.option(
    "--max-user-code-retries",
    default=0,
    help="How many times we should attempt running the user code.",
)
@click.option(
    "--clone-only",
    default=None,
    help="Pathspec of the origin task for this task to clone. Do "
    "not execute anything.",
)
@click.option(
    "--clone-run-id",
    default=None,
    help="Run id of the origin flow, if this task is part of a flow being resumed.",
)
@click.option(
    "--ubf-context",
    default="none",
    type=click.Choice(["none", UBF_CONTROL, UBF_TASK]),
    help="Provides additional context if this task is of type unbounded foreach.",
)
@click.option(
    "--num-parallel",
    default=0,
    type=int,
    help="Number of parallel instances of a step. Ignored in local mode (see parallel decorator code).",
)
@click.pass_context
def step(
    ctx,
    step_name,
    opt_tag=None,
    run_id=None,
    task_id=None,
    input_paths=None,
    input_paths_filename=None,
    split_index=None,
    opt_namespace=None,
    retry_count=None,
    max_user_code_retries=None,
    clone_only=None,
    clone_run_id=None,
    ubf_context="none",
    num_parallel=None,
):

    if ctx.obj.is_quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    if ubf_context == "none":
        ubf_context = None
    if opt_namespace is not None:
        namespace(opt_namespace or None)

    func = None
    try:
        func = getattr(ctx.obj.flow, step_name)
    except:
        raise CommandException("Step *%s* doesn't exist." % step_name)
    if not func.is_step:
        raise CommandException("Function *%s* is not a step." % step_name)
    echo("Executing a step, *%s*" % step_name, fg="magenta", bold=False)

    step_kwargs = ctx.params
    # Remove argument `step_name` from `step_kwargs`.
    step_kwargs.pop("step_name", None)
    # Remove `opt_*` prefix from (some) option keys.
    step_kwargs = dict(
        [(k[4:], v) if k.startswith("opt_") else (k, v) for k, v in step_kwargs.items()]
    )
    cli_args._set_step_kwargs(step_kwargs)

    ctx.obj.metadata.add_sticky_tags(tags=opt_tag)
    if not input_paths and input_paths_filename:
        with open(input_paths_filename, mode="r", encoding="utf-8") as f:
            input_paths = f.read().strip(" \n\"'")

    paths = decompress_list(input_paths) if input_paths else []

    task = MetaflowTask(
        ctx.obj.flow,
        ctx.obj.flow_datastore,
        ctx.obj.metadata,
        ctx.obj.environment,
        ctx.obj.echo,
        ctx.obj.event_logger,
        ctx.obj.monitor,
        ubf_context,
    )
    if clone_only:
        task.clone_only(
            step_name,
            run_id,
            task_id,
            clone_only,
            retry_count,
        )
    else:
        task.run_step(
            step_name,
            run_id,
            task_id,
            clone_run_id,
            paths,
            split_index,
            retry_count,
            max_user_code_retries,
        )

    echo("Success", fg="green", bold=True, indent=True)
