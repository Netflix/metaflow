from metaflow._vendor import click

from .. import namespace
from ..cli import echo_always, echo_dev_null
from ..cli_args import cli_args
from ..datastore.flow_datastore import FlowDataStore
from ..exception import CommandException
from ..client.filecache import FileCache, FileBlobCache, TaskMetadataCache
from ..metaflow_config import SPIN_ALLOWED_DECORATORS
from ..metaflow_profile import from_start
from ..plugins import DATASTORES
from ..task import MetaflowTask
from ..unbounded_foreach import UBF_CONTROL, UBF_TASK
from ..util import decompress_list, read_artifacts_module
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
        namespace(opt_namespace)

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


@click.command(help="Internal command to spin a single task.", hidden=True)
@click.argument("step-name")
@click.option(
    "--run-id",
    default=None,
    required=True,
    help="Original run ID for the step that will be spun",
)
@click.option(
    "--task-id",
    default=None,
    required=True,
    help="Original Task ID for the step that will be spun",
)
@click.option(
    "--orig-flow-datastore",
    show_default=True,
    help="Original datastore for the flow from which a task is being spun",
)
@click.option(
    "--input-paths",
    help="A comma-separated list of pathspecs specifying inputs for this step.",
)
@click.option(
    "--split-index",
    type=int,
    default=None,
    show_default=True,
    help="Index of this foreach split.",
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
    "--namespace",
    "opt_namespace",
    default=None,
    help="Change namespace from the default (your username) to the specified tag.",
)
@click.option(
    "--skip-decorators/--no-skip-decorators",
    is_flag=True,
    default=False,
    show_default=True,
    help="Skip decorators attached to the step or flow.",
)
@click.option(
    "--persist/--no-persist",
    "persist",
    default=True,
    show_default=True,
    help="Whether to persist the artifacts in the spun step. If set to false, the artifacts will not"
    " be persisted and will not be available in the spun step's datastore.",
)
@click.option(
    "--artifacts-module",
    default=None,
    show_default=True,
    help="Path to a module that contains artifacts to be used in the spun step. The artifacts should "
    "be defined as a dictionary called ARTIFACTS with keys as the artifact names and values as the "
    "artifact values. The artifact values will overwrite the default values of the artifacts used in "
    "the spun step.",
)
@click.pass_context
def spin_step(
    ctx,
    step_name,
    orig_flow_datastore,
    run_id=None,
    task_id=None,
    input_paths=None,
    split_index=None,
    retry_count=None,
    max_user_code_retries=None,
    opt_namespace=None,
    skip_decorators=False,
    artifacts_module=None,
    persist=True,
):
    import time

    if ctx.obj.is_quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    if opt_namespace is not None:
        namespace(opt_namespace)

    input_paths = decompress_list(input_paths) if input_paths else []

    skip_decorators = skip_decorators
    whitelist_decorators = [] if skip_decorators else SPIN_ALLOWED_DECORATORS
    from_start("SpinStep: initialized decorators")
    spin_artifacts = read_artifacts_module(artifacts_module) if artifacts_module else {}
    from_start("SpinStep: read artifacts module")

    ds_type, ds_root = orig_flow_datastore.split("@")
    orig_datastore_impl = [d for d in DATASTORES if d.TYPE == ds_type][0]
    orig_datastore_impl.datastore_root = ds_root
    orig_flow_datastore = FlowDataStore(
        ctx.obj.flow.name,
        environment=None,
        storage_impl=orig_datastore_impl,
        ds_root=ds_root,
    )

    filecache = FileCache()
    orig_flow_datastore.set_metadata_cache(
        TaskMetadataCache(filecache, ds_type, ds_root, ctx.obj.flow.name)
    )
    orig_flow_datastore.ca_store.set_blob_cache(
        FileBlobCache(
            filecache, FileCache.flow_ds_id(ds_type, ds_root, ctx.obj.flow.name)
        )
    )

    task = MetaflowTask(
        ctx.obj.flow,
        ctx.obj.flow_datastore,
        ctx.obj.metadata,
        ctx.obj.environment,
        echo,
        ctx.obj.event_logger,
        ctx.obj.monitor,
        None,  # no unbounded foreach context
        orig_flow_datastore=orig_flow_datastore,
        spin_artifacts=spin_artifacts,
    )
    from_start("SpinStep: initialized task")
    task.run_step(
        step_name,
        run_id,
        task_id,
        None,
        input_paths,
        split_index,
        retry_count,
        max_user_code_retries,
        whitelist_decorators,
        persist,
    )
    from_start("SpinStep: ran step")
