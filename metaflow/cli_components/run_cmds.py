import json

from functools import wraps

from metaflow._vendor import click

from .. import decorators, namespace, parameters, tracing
from ..exception import CommandException
from ..graph import FlowGraph
from ..metaflow_current import current
from ..metaflow_config import (
    DEFAULT_DECOSPECS,
    FEAT_ALWAYS_UPLOAD_CODE_PACKAGE,
    SPIN_PERSIST,
)
from ..metaflow_profile import from_start
from ..package import MetaflowPackage
from ..runtime import NativeRuntime, SpinRuntime
from ..system import _system_logger

# from ..client.core import Run

from ..tagging_util import validate_tags
from ..util import get_latest_run_id, write_latest_run_id, parse_spin_pathspec


def before_run(obj, tags, decospecs, skip_decorators=False):
    validate_tags(tags)

    # There's a --with option both at the top-level and for the run/resume/spin
    # subcommand. Why?
    #
    # "run --with shoes" looks so much better than "--with shoes run".
    # This is a very common use case of --with.
    #
    # A downside is that we need to have the following decorators handling
    # in two places in this module and make sure _init_step_decorators
    # doesn't get called twice.

    # We want the order to be the following:
    # - run level decospecs
    # - top level decospecs
    # - environment decospecs
    from_start(
        f"Inside before_run, skip_decorators={skip_decorators}, is_spin={obj.is_spin}"
    )
    if not skip_decorators:
        all_decospecs = (
            list(decospecs or [])
            + obj.tl_decospecs
            + list(obj.environment.decospecs() or [])
        )
        if all_decospecs:
            # These decospecs are the ones from run/resume/spin PLUS the ones from the
            # environment (for example the @conda)
            decorators._attach_decorators(obj.flow, all_decospecs)
            decorators._init(obj.flow)
            # Regenerate graph if we attached more decorators
            obj.flow.__class__._init_graph()
            obj.graph = obj.flow._graph

        obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
        # obj.environment.init_environment(obj.logger)

        decorators._init_step_decorators(
            obj.flow,
            obj.graph,
            obj.environment,
            obj.flow_datastore,
            obj.logger,
            obj.is_spin,
            skip_decorators,
        )
    # Re-read graph since it may have been modified by mutators
    obj.graph = obj.flow._graph

    obj.metadata.add_sticky_tags(tags=tags)

    # Package working directory only once per run.
    # We explicitly avoid doing this in `start` since it is invoked for every
    # step in the run.
    obj.package = MetaflowPackage(
        obj.flow,
        obj.environment,
        obj.echo,
        suffixes=obj.package_suffixes,
        flow_datastore=obj.flow_datastore if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE else None,
    )


def common_runner_options(func):
    @click.option(
        "--run-id-file",
        default=None,
        show_default=True,
        type=str,
        help="Write the ID of this run to the file specified.",
    )
    @click.option(
        "--runner-attribute-file",
        default=None,
        show_default=True,
        type=str,
        help="Write the metadata and pathspec of this run to the file specified. Used internally "
        "for Metaflow's Runner API.",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def write_file(file_path, content):
    if file_path is not None:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(content))


def config_callback(ctx, param, value):
    # Callback to:
    #  - read  the Click auto_envvar variable from both the
    #    environment AND the configuration
    #  - merge that value with the value passed in the command line (value)
    #  - return the value as a tuple
    # Note that this function gets called even if there is no option passed on the
    # command line.
    # NOTE: Assumes that ctx.auto_envvar_prefix is set to METAFLOW (same as in
    # from_conf)

    # Read decospecs options from the environment (METAFLOW_DEFAULT_DECOSPECS=...)
    # and merge them with the one provided as --with.
    splits = DEFAULT_DECOSPECS.split()
    return tuple(list(value) + splits)


def common_run_options(func):
    @click.option(
        "--tag",
        "tags",
        multiple=True,
        default=None,
        help="Annotate this run with the given tag. You can specify "
        "this option multiple times to attach multiple tags in "
        "the run.",
    )
    @click.option(
        "--max-workers",
        default=16,
        show_default=True,
        help="Maximum number of parallel processes.",
    )
    @click.option(
        "--max-num-splits",
        default=100,
        show_default=True,
        help="Maximum number of splits allowed in a foreach. This "
        "is a safety check preventing bugs from triggering "
        "thousands of steps inadvertently.",
    )
    @click.option(
        "--max-log-size",
        default=10,
        show_default=True,
        help="Maximum size of stdout and stderr captured in "
        "megabytes. If a step outputs more than this to "
        "stdout/stderr, its output will be truncated.",
    )
    @click.option(
        "--with",
        "decospecs",
        multiple=True,
        help="Add a decorator to all steps. You can specify this "
        "option multiple times to attach multiple decorators "
        "in steps.",
        callback=config_callback,
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.option(
    "--origin-run-id",
    default=None,
    help="ID of the run that should be resumed. By default, the "
    "last run executed locally.",
)
@click.option(
    "--run-id",
    default=None,
    help="Run ID for the new run. By default, a new run-id will be generated",
    hidden=True,
)
@click.option(
    "--clone-only/--no-clone-only",
    default=False,
    show_default=True,
    help="Only clone tasks without continuing execution",
    hidden=True,
)
@click.option(
    "--reentrant/--no-reentrant",
    default=False,
    show_default=True,
    hidden=True,
    help="If specified, allows this call to be called in parallel",
)
@click.option(
    "--resume-identifier",
    default=None,
    show_default=True,
    hidden=True,
    help="If specified, it identifies the task that started this resume call. It is in the form of {step_name}-{task_id}",
)
@click.argument("step-to-rerun", required=False)
@click.command(help="Resume execution of a previous run of this flow.")
@tracing.cli("cli/resume")
@common_run_options
@common_runner_options
@click.pass_obj
def resume(
    obj,
    tags=None,
    step_to_rerun=None,
    origin_run_id=None,
    run_id=None,
    clone_only=False,
    reentrant=False,
    max_workers=None,
    max_num_splits=None,
    max_log_size=None,
    decospecs=None,
    run_id_file=None,
    resume_identifier=None,
    runner_attribute_file=None,
):
    before_run(obj, tags, decospecs)

    if origin_run_id is None:
        origin_run_id = get_latest_run_id(obj.echo, obj.flow.name)
        if origin_run_id is None:
            raise CommandException(
                "A previous run id was not found. Specify --origin-run-id."
            )

    if step_to_rerun is None:
        steps_to_rerun = set()
    else:
        # validate step name
        if step_to_rerun not in obj.graph.nodes:
            raise CommandException(
                "invalid step name {0} specified, must be step present in "
                "current form of execution graph. Valid step names include: {1}".format(
                    step_to_rerun, ",".join(list(obj.graph.nodes.keys()))
                )
            )

        ## TODO: instead of checking execution path here, can add a warning later
        ## instead of throwing an error. This is for resuming a step which was not
        ## taken inside a branch i.e. not present in the execution path.

        # origin_run = Run(f"{obj.flow.name}/{origin_run_id}", _namespace_check=False)
        # executed_steps = {step.path_components[-1] for step in origin_run}
        # if step_to_rerun not in executed_steps:
        #     raise CommandException(
        #         f"Cannot resume from step '{step_to_rerun}'. This step was not "
        #         f"part of the original execution path for run '{origin_run_id}'."
        #     )

        steps_to_rerun = {step_to_rerun}

    if run_id:
        # Run-ids that are provided by the metadata service are always integers.
        # External providers or run-ids (like external schedulers) always need to
        # be non-integers to avoid any clashes. This condition ensures this.
        try:
            int(run_id)
        except:
            pass
        else:
            raise CommandException("run-id %s cannot be an integer" % run_id)

    runtime = NativeRuntime(
        obj.flow,
        obj.graph,
        obj.flow_datastore,
        obj.metadata,
        obj.environment,
        obj.package,
        obj.logger,
        obj.entrypoint,
        obj.event_logger,
        obj.monitor,
        run_id=run_id,
        clone_run_id=origin_run_id,
        clone_only=clone_only,
        reentrant=reentrant,
        steps_to_rerun=steps_to_rerun,
        max_workers=max_workers,
        max_num_splits=max_num_splits,
        max_log_size=max_log_size * 1024 * 1024,
        resume_identifier=resume_identifier,
    )
    write_file(run_id_file, runtime.run_id)
    runtime.print_workflow_info()

    runtime.persist_constants()

    if runner_attribute_file:
        with open(runner_attribute_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "run_id": runtime.run_id,
                    "flow_name": obj.flow.name,
                    "metadata": obj.metadata.metadata_str(),
                },
                f,
            )

    # We may skip clone-only resume if this is not a resume leader,
    # and clone is already complete.
    if runtime.should_skip_clone_only_execution():
        return

    current._update_env(
        {
            "run_id": runtime.run_id,
        }
    )
    _system_logger.log_event(
        level="info",
        module="metaflow.resume",
        name="start",
        payload={
            "msg": "Resuming run",
        },
    )

    with runtime.run_heartbeat():
        if clone_only:
            runtime.clone_original_run()
        else:
            runtime.clone_original_run(generate_task_obj=True, verbose=False)
            runtime.execute()


@parameters.add_custom_parameters(deploy_mode=True)
@click.command(help="Run the workflow locally.")
@tracing.cli("cli/run")
@common_run_options
@common_runner_options
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Change namespace from the default (your username) to "
    "the specified tag. Note that this option does not alter "
    "tags assigned to the objects produced by this run, just "
    "what existing objects are visible in the client API. You "
    "can enable the global namespace with an empty string."
    "--namespace=",
)
@click.pass_obj
def run(
    obj,
    tags=None,
    max_workers=None,
    max_num_splits=None,
    max_log_size=None,
    decospecs=None,
    run_id_file=None,
    runner_attribute_file=None,
    user_namespace=None,
    **kwargs,
):
    if user_namespace is not None:
        namespace(user_namespace or None)
    before_run(obj, tags, decospecs)

    runtime = NativeRuntime(
        obj.flow,
        obj.graph,
        obj.flow_datastore,
        obj.metadata,
        obj.environment,
        obj.package,
        obj.logger,
        obj.entrypoint,
        obj.event_logger,
        obj.monitor,
        max_workers=max_workers,
        max_num_splits=max_num_splits,
        max_log_size=max_log_size * 1024 * 1024,
    )
    write_latest_run_id(obj, runtime.run_id)
    write_file(run_id_file, runtime.run_id)

    obj.flow._set_constants(obj.graph, kwargs, obj.config_options)
    current._update_env(
        {
            "run_id": runtime.run_id,
        }
    )
    _system_logger.log_event(
        level="info",
        module="metaflow.run",
        name="start",
        payload={
            "msg": "Starting run",
        },
    )

    runtime.print_workflow_info()
    runtime.persist_constants()
    if runner_attribute_file:
        with open(runner_attribute_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "run_id": runtime.run_id,
                    "flow_name": obj.flow.name,
                    "metadata": obj.metadata.metadata_str(),
                },
                f,
            )
    with runtime.run_heartbeat():
        runtime.execute()


# @parameters.add_custom_parameters(deploy_mode=True)
@click.command(help="Spins up a task for a given step from a previous run locally.")
@tracing.cli("cli/spin")
@click.argument("pathspec")
@click.option(
    "--skip-decorators/--no-skip-decorators",
    is_flag=True,
    # Default False matches the saved_args check in cli.py for spin steps - skip_decorators
    # only becomes True when explicitly passed, otherwise decorators are applied by default
    default=False,
    show_default=True,
    help="Skip decorators attached to the step or flow.",
)
@click.option(
    "--artifacts-module",
    default=None,
    show_default=True,
    help="Path to a module that contains artifacts to be used in the spun step. "
    "The artifacts should be defined as a dictionary called ARTIFACTS with keys as "
    "the artifact names and values as the artifact values. The artifact values will "
    "overwrite the default values of the artifacts used in the spun step.",
)
@click.option(
    "--persist/--no-persist",
    "persist",
    default=SPIN_PERSIST,
    show_default=True,
    help="Whether to persist the artifacts in the spun step. If set to False, "
    "the artifacts will not be persisted and will not be available in the spun step's "
    "datastore.",
)
@click.option(
    "--max-log-size",
    default=10,
    show_default=True,
    help="Maximum size of stdout and stderr captured in "
    "megabytes. If a step outputs more than this to "
    "stdout/stderr, its output will be truncated.",
)
@common_runner_options
@click.pass_obj
def spin(
    obj,
    pathspec,
    persist=True,
    artifacts_module=None,
    skip_decorators=False,
    max_log_size=None,
    run_id_file=None,
    runner_attribute_file=None,
    **kwargs,
):
    # Parse the pathspec argument to extract step name and full pathspec
    step_name, parsed_pathspec = parse_spin_pathspec(pathspec, obj.flow.name)

    before_run(obj, [], [], skip_decorators)
    obj.echo(f"Spinning up step *{step_name}* locally for flow *{obj.flow.name}*")
    # For spin, flow parameters come from the original run, but _set_constants
    # requires them in kwargs. Use parameter defaults as placeholders - they'll be
    # overwritten when the spin step loads artifacts from the original run.
    flow_param_defaults = {}
    for var, param in obj.flow._get_parameters():
        if not param.IS_CONFIG_PARAMETER:
            default_value = param.kwargs.get("default")
            # Use None for required parameters without defaults
            flow_param_defaults[param.name.replace("-", "_").lower()] = default_value
    obj.flow._set_constants(obj.graph, flow_param_defaults, obj.config_options)
    step_func = getattr(obj.flow, step_name, None)
    if step_func is None:
        raise CommandException(
            f"Step '{step_name}' not found in flow '{obj.flow.name}'. "
            "Please provide a valid step name."
        )
    from_start("Spin: before spin runtime init")
    spin_runtime = SpinRuntime(
        obj.flow,
        obj.graph,
        obj.flow_datastore,
        obj.metadata,
        obj.environment,
        obj.package,
        obj.logger,
        obj.entrypoint,
        obj.event_logger,
        obj.monitor,
        step_func,
        step_name,
        parsed_pathspec,
        skip_decorators,
        artifacts_module,
        persist,
        max_log_size * 1024 * 1024,
    )
    write_latest_run_id(obj, spin_runtime.run_id)
    write_file(run_id_file, spin_runtime.run_id)
    # We only need the root for the metadata, i.e. the portion before DATASTORE_LOCAL_DIR
    datastore_root = spin_runtime._flow_datastore._storage_impl.datastore_root
    orig_task_metadata_root = datastore_root.rsplit("/", 1)[0]
    from_start("Spin: going to execute")
    spin_runtime.execute()
    from_start("Spin: after spin runtime execute")

    if runner_attribute_file:
        with open(runner_attribute_file, "w") as f:
            json.dump(
                {
                    "task_id": spin_runtime.task.task_id,
                    "step_name": step_name,
                    "run_id": spin_runtime.run_id,
                    "flow_name": obj.flow.name,
                    # Store metadata in a format that can be used by the Runner API
                    "metadata": f"{obj.metadata.__class__.TYPE}@{orig_task_metadata_root}",
                },
                f,
            )
