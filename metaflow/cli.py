import inspect
import os
import sys
import traceback
from datetime import datetime
from functools import wraps

import metaflow.tracing as tracing
from metaflow._vendor import click

from . import decorators, lint, metaflow_version, namespace, parameters, plugins
from .cli_args import cli_args
from .datastore import FlowDataStore, TaskDataStore, TaskDataStoreSet
from .exception import CommandException, MetaflowException
from .graph import FlowGraph
from .metaflow_config import (
    DECOSPECS,
    DEFAULT_DATASTORE,
    DEFAULT_ENVIRONMENT,
    DEFAULT_EVENT_LOGGER,
    DEFAULT_METADATA,
    DEFAULT_MONITOR,
    DEFAULT_PACKAGE_SUFFIXES,
)
from .metaflow_current import current
from metaflow.system import _system_monitor, _system_logger
from .metaflow_environment import MetaflowEnvironment
from .mflog import LOG_SOURCES, mflog
from .package import MetaflowPackage
from .plugins import (
    DATASTORES,
    ENVIRONMENTS,
    LOGGING_SIDECARS,
    METADATA_PROVIDERS,
    MONITOR_SIDECARS,
)
from .pylint_wrapper import PyLint
from .R import metaflow_r_version, use_r
from .runtime import NativeRuntime
from .tagging_util import validate_tags
from .task import MetaflowTask
from .unbounded_foreach import UBF_CONTROL, UBF_TASK
from .util import (
    decompress_list,
    get_latest_run_id,
    resolve_identity,
    write_latest_run_id,
)

ERASE_TO_EOL = "\033[K"
HIGHLIGHT = "red"
INDENT = " " * 4

LOGGER_TIMESTAMP = "magenta"
LOGGER_COLOR = "green"
LOGGER_BAD_COLOR = "red"

try:
    # Python 2
    import cPickle as pickle
except ImportError:
    # Python 3
    import pickle


def echo_dev_null(*args, **kwargs):
    pass


def echo_always(line, **kwargs):
    kwargs["err"] = kwargs.get("err", True)
    if kwargs.pop("indent", None):
        line = "\n".join(INDENT + x for x in line.splitlines())
    if "nl" not in kwargs or kwargs["nl"]:
        line += ERASE_TO_EOL
    top = kwargs.pop("padding_top", None)
    bottom = kwargs.pop("padding_bottom", None)
    highlight = kwargs.pop("highlight", HIGHLIGHT)
    if top:
        click.secho(ERASE_TO_EOL, **kwargs)

    hl_bold = kwargs.pop("highlight_bold", True)
    nl = kwargs.pop("nl", True)
    fg = kwargs.pop("fg", None)
    bold = kwargs.pop("bold", False)
    kwargs["nl"] = False
    hl = True
    nobold = kwargs.pop("no_bold", False)
    if nobold:
        click.secho(line, **kwargs)
    else:
        for span in line.split("*"):
            if hl:
                hl = False
                kwargs["fg"] = fg
                kwargs["bold"] = bold
                click.secho(span, **kwargs)
            else:
                hl = True
                kwargs["fg"] = highlight
                kwargs["bold"] = hl_bold
                click.secho(span, **kwargs)
    if nl:
        kwargs["nl"] = True
        click.secho("", **kwargs)
    if bottom:
        click.secho(ERASE_TO_EOL, **kwargs)


def logger(body="", system_msg=False, head="", bad=False, timestamp=True, nl=True):
    if timestamp:
        if timestamp is True:
            dt = datetime.now()
        else:
            dt = timestamp
        tstamp = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        click.secho(tstamp + " ", fg=LOGGER_TIMESTAMP, nl=False)
    if head:
        click.secho(head, fg=LOGGER_COLOR, nl=False)
    click.secho(body, bold=system_msg, fg=LOGGER_BAD_COLOR if bad else None, nl=nl)


def config_merge_cb(ctx, param, value):
    # Callback to:
    #  - read  the Click auto_envvar variable from both the
    #    environment AND the configuration
    #  - merge that value with the value passed in the command line (value)
    #  - return the value as a tuple
    # Note that this function gets called even if there is no option passed on the
    # command line.
    # NOTE: Assumes that ctx.auto_envvar_prefix is set to METAFLOW (same as in
    # from_conf)

    # Special case where DECOSPECS and value are the same. This happens
    # when there is no --with option at the TL and DECOSPECS is read from
    # the env var. In this case, click also passes it as value
    splits = DECOSPECS.split()
    if len(splits) == len(value) and all([a == b for (a, b) in zip(splits, value)]):
        return value
    return tuple(list(value) + DECOSPECS.split())


@click.group()
def cli(ctx):
    pass


@cli.command(help="Check that the flow is valid (default).")
@click.option(
    "--warnings/--no-warnings",
    default=False,
    show_default=True,
    help="Show all Pylint warnings, not just errors.",
)
@click.pass_obj
def check(obj, warnings=False):
    _check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint, warnings=warnings)
    fname = inspect.getfile(obj.flow.__class__)
    echo(
        "\n*'{cmd} show'* shows a description of this flow.\n"
        "*'{cmd} run'* runs the flow locally.\n"
        "*'{cmd} help'* shows all available commands and options.\n".format(cmd=fname),
        highlight="magenta",
        highlight_bold=False,
    )


@cli.command(help="Show structure of the flow.")
@click.pass_obj
def show(obj):
    echo_always("\n%s" % obj.graph.doc)
    for _, node in sorted((n.func_lineno, n) for n in obj.graph):
        echo_always("\nStep *%s*" % node.name, err=False)
        echo_always(node.doc if node.doc else "?", indent=True, err=False)
        if node.type != "end":
            echo_always(
                "*=>* %s" % ", ".join("*%s*" % n for n in node.out_funcs),
                indent=True,
                highlight="magenta",
                highlight_bold=False,
                err=False,
            )
    echo_always("")


@cli.command(help="Show all available commands.")
@click.pass_context
def help(ctx):
    print(ctx.parent.get_help())


@cli.command(help="Output internal state of the flow graph.")
@click.option("--json", is_flag=True, help="Output the flow graph in JSON format.")
@click.pass_obj
def output_raw(obj, json):
    if json:
        import json as _json

        _msg = "Internal representation of the flow in JSON format:"
        _graph_dict, _graph_struct = obj.graph.output_steps()
        _graph = _json.dumps(
            dict(graph=_graph_dict, graph_structure=_graph_struct), indent=4
        )
    else:
        _graph = str(obj.graph)
        _msg = "Internal representation of the flow:"
    echo(_msg, fg="magenta", bold=False)
    echo_always(_graph, err=False)


@cli.command(help="Visualize the flow with Graphviz.")
@click.pass_obj
def output_dot(obj):
    echo("Visualizing the flow as a GraphViz graph", fg="magenta", bold=False)
    echo(
        "Try piping the output to 'dot -Tpng -o graph.png' to produce "
        "an actual image.",
        indent=True,
    )
    echo_always(obj.graph.output_dot(), err=False)


@cli.command(
    help="Get data artifacts of a task or all tasks in a step. "
    "The format for input-path is either <run_id>/<step_name> or "
    "<run_id>/<step_name>/<task_id>."
)
@click.argument("input-path")
@click.option(
    "--private/--no-private",
    default=False,
    show_default=True,
    help="Show also private attributes.",
)
@click.option(
    "--max-value-size",
    default=1000,
    show_default=True,
    type=int,
    help="Show only values that are smaller than this number. "
    "Set to 0 to see only keys.",
)
@click.option(
    "--include",
    type=str,
    default="",
    help="Include only artifacts in the given comma-separated list.",
)
@click.option(
    "--file", type=str, default=None, help="Serialize artifacts in the given file."
)
@click.pass_obj
def dump(obj, input_path, private=None, max_value_size=None, include=None, file=None):
    output = {}
    kwargs = {
        "show_private": private,
        "max_value_size": max_value_size,
        "include": {t for t in include.split(",") if t},
    }

    # Pathspec can either be run_id/step_name or run_id/step_name/task_id.
    parts = input_path.split("/")
    if len(parts) == 2:
        run_id, step_name = parts
        task_id = None
    elif len(parts) == 3:
        run_id, step_name, task_id = parts
    else:
        raise CommandException(
            "input_path should either be run_id/step_name or run_id/step_name/task_id"
        )

    datastore_set = TaskDataStoreSet(
        obj.flow_datastore,
        run_id,
        steps=[step_name],
        prefetch_data_artifacts=kwargs.get("include"),
    )
    if task_id:
        ds_list = [datastore_set.get_with_pathspec(input_path)]
    else:
        ds_list = list(datastore_set)  # get all tasks

    for ds in ds_list:
        echo(
            "Dumping output of run_id=*{run_id}* "
            "step=*{step}* task_id=*{task_id}*".format(
                run_id=ds.run_id, step=ds.step_name, task_id=ds.task_id
            ),
            fg="magenta",
        )

        if file is None:
            echo_always(
                ds.format(**kwargs), highlight="green", highlight_bold=False, err=False
            )
        else:
            output[ds.pathspec] = ds.to_dict(**kwargs)

    if file is not None:
        with open(file, "wb") as f:
            pickle.dump(output, f, protocol=pickle.HIGHEST_PROTOCOL)
        echo("Artifacts written to *%s*" % file)


# TODO - move step and init under a separate 'internal' subcommand


@cli.command(help="Internal command to execute a single task.", hidden=True)
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
    "--with",
    "decospecs",
    multiple=True,
    help="Add a decorator to this task. You can specify this "
    "option multiple times to attach multiple decorators "
    "to this task.",
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
    decospecs=None,
    ubf_context="none",
    num_parallel=None,
):
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

    if decospecs:
        decorators._attach_decorators_to_step(func, decospecs)

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


@parameters.add_custom_parameters(deploy_mode=False)
@cli.command(help="Internal command to initialize a run.", hidden=True)
@click.option(
    "--run-id",
    default=None,
    required=True,
    help="ID for one execution of all steps in the flow.",
)
@click.option(
    "--task-id", default=None, required=True, help="ID for this instance of the step."
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Tags for this instance of the step.",
)
@click.pass_obj
def init(obj, run_id=None, task_id=None, tags=None, **kwargs):
    # init is a separate command instead of an option in 'step'
    # since we need to capture user-specified parameters with
    # @add_custom_parameters. Adding custom parameters to 'step'
    # is not desirable due to the possibility of name clashes between
    # user-specified parameters and our internal options. Note that
    # user-specified parameters are often defined as environment
    # variables.

    obj.metadata.add_sticky_tags(tags=tags)

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
    )
    obj.flow._set_constants(obj.graph, kwargs)
    runtime.persist_constants(task_id=task_id)


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
    )
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
        help="Write the metadata and pathspec of this run to the file specified. Used internally for Metaflow's Runner API.",
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
@cli.command(help="Resume execution of a previous run of this flow.")
@common_run_options
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
        clone_steps = set()
    else:
        # validate step name
        if step_to_rerun not in obj.graph.nodes:
            raise CommandException(
                "invalid step name {0} specified, must be step present in "
                "current form of execution graph. Valid step names include: {1}".format(
                    step_to_rerun, ",".join(list(obj.graph.nodes.keys()))
                )
            )
        clone_steps = {step_to_rerun}

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
        clone_steps=clone_steps,
        max_workers=max_workers,
        max_num_splits=max_num_splits,
        max_log_size=max_log_size * 1024 * 1024,
        resume_identifier=resume_identifier,
    )
    write_file(run_id_file, runtime.run_id)
    runtime.print_workflow_info()

    runtime.persist_constants()
    write_file(
        runner_attribute_file,
        "%s@%s:%s"
        % (
            obj.metadata.__class__.TYPE,
            obj.metadata.__class__.INFO,
            "/".join((obj.flow.name, runtime.run_id)),
        ),
    )

    # We may skip clone-only resume if this is not a resume leader,
    # and clone is already complete.
    if runtime.should_skip_clone_only_execution():
        return

    with runtime.run_heartbeat():
        if clone_only:
            runtime.clone_original_run()
        else:
            runtime.clone_original_run(generate_task_obj=True, verbose=False)
            runtime.execute()


@tracing.cli_entrypoint("cli/run")
@parameters.add_custom_parameters(deploy_mode=True)
@cli.command(help="Run the workflow locally.")
@common_run_options
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
    **kwargs
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

    obj.flow._set_constants(obj.graph, kwargs)
    runtime.print_workflow_info()
    runtime.persist_constants()
    write_file(
        runner_attribute_file,
        "%s@%s:%s"
        % (
            obj.metadata.__class__.TYPE,
            obj.metadata.__class__.INFO,
            "/".join((obj.flow.name, runtime.run_id)),
        ),
    )
    runtime.execute()


def write_file(file_path, content):
    if file_path is not None:
        with open(file_path, "w") as f:
            f.write(str(content))


def before_run(obj, tags, decospecs):
    validate_tags(tags)

    # There's a --with option both at the top-level and for the run
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
    all_decospecs = (
        list(decospecs or [])
        + obj.tl_decospecs
        + list(obj.environment.decospecs() or [])
    )
    if all_decospecs:
        decorators._attach_decorators(obj.flow, all_decospecs)
        obj.graph = FlowGraph(obj.flow.__class__)

    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    # obj.environment.init_environment(obj.logger)

    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )

    obj.metadata.add_sticky_tags(tags=tags)

    # Package working directory only once per run.
    # We explicitly avoid doing this in `start` since it is invoked for every
    # step in the run.
    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.echo, obj.package_suffixes
    )


@cli.command(help="Print the Metaflow version")
@click.pass_obj
def version(obj):
    echo_always(obj.version)


@tracing.cli_entrypoint("cli/start")
@decorators.add_decorator_options
@click.command(
    cls=click.CommandCollection,
    sources=[cli] + plugins.get_plugin_cli(),
    invoke_without_command=True,
)
@click.option(
    "--quiet/--not-quiet",
    show_default=True,
    default=False,
    help="Suppress unnecessary messages",
)
@click.option(
    "--metadata",
    default=DEFAULT_METADATA,
    show_default=True,
    type=click.Choice([m.TYPE for m in METADATA_PROVIDERS]),
    help="Metadata service type",
)
@click.option(
    "--environment",
    default=DEFAULT_ENVIRONMENT,
    show_default=True,
    type=click.Choice(["local"] + [m.TYPE for m in ENVIRONMENTS]),
    help="Execution environment type",
)
@click.option(
    "--datastore",
    default=DEFAULT_DATASTORE,
    show_default=True,
    type=click.Choice([d.TYPE for d in DATASTORES]),
    help="Data backend type",
)
@click.option("--datastore-root", help="Root path for datastore")
@click.option(
    "--package-suffixes",
    help="A comma-separated list of file suffixes to include in the code package.",
    default=DEFAULT_PACKAGE_SUFFIXES,
    show_default=True,
)
@click.option(
    "--with",
    "decospecs",
    multiple=True,
    help="Add a decorator to all steps. You can specify this option "
    "multiple times to attach multiple decorators in steps.",
    callback=config_merge_cb,
)
@click.option(
    "--pylint/--no-pylint",
    default=True,
    show_default=True,
    help="Run Pylint on the flow if pylint is installed.",
)
@click.option(
    "--event-logger",
    default=DEFAULT_EVENT_LOGGER,
    show_default=True,
    type=click.Choice(LOGGING_SIDECARS),
    help="type of event logger used",
)
@click.option(
    "--monitor",
    default=DEFAULT_MONITOR,
    show_default=True,
    type=click.Choice(MONITOR_SIDECARS),
    help="Monitoring backend type",
)
@click.pass_context
def start(
    ctx,
    quiet=False,
    metadata=None,
    environment=None,
    datastore=None,
    datastore_root=None,
    decospecs=None,
    package_suffixes=None,
    pylint=None,
    event_logger=None,
    monitor=None,
    **deco_options
):
    global echo
    if quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    ctx.obj.version = metaflow_version.get_version()
    version = ctx.obj.version
    if use_r():
        version = metaflow_r_version()

    echo("Metaflow %s" % version, fg="magenta", bold=True, nl=False)
    echo(" executing *%s*" % ctx.obj.flow.name, fg="magenta", nl=False)
    echo(" for *%s*" % resolve_identity(), fg="magenta")

    cli_args._set_top_kwargs(ctx.params)
    ctx.obj.echo = echo
    ctx.obj.echo_always = echo_always
    ctx.obj.is_quiet = quiet
    ctx.obj.graph = FlowGraph(ctx.obj.flow.__class__)
    ctx.obj.logger = logger
    ctx.obj.check = _check
    ctx.obj.pylint = pylint
    ctx.obj.top_cli = cli
    ctx.obj.package_suffixes = package_suffixes.split(",")
    ctx.obj.reconstruct_cli = _reconstruct_cli

    ctx.obj.environment = [
        e for e in ENVIRONMENTS + [MetaflowEnvironment] if e.TYPE == environment
    ][0](ctx.obj.flow)
    ctx.obj.environment.validate_environment(ctx.obj.logger, datastore)

    ctx.obj.event_logger = LOGGING_SIDECARS[event_logger](
        flow=ctx.obj.flow, env=ctx.obj.environment
    )
    ctx.obj.event_logger.start()
    _system_logger.init_system_logger(ctx.obj.flow.name, ctx.obj.event_logger)

    ctx.obj.monitor = MONITOR_SIDECARS[monitor](
        flow=ctx.obj.flow, env=ctx.obj.environment
    )
    ctx.obj.monitor.start()
    _system_monitor.init_system_monitor(ctx.obj.flow.name, ctx.obj.monitor)

    ctx.obj.metadata = [m for m in METADATA_PROVIDERS if m.TYPE == metadata][0](
        ctx.obj.environment, ctx.obj.flow, ctx.obj.event_logger, ctx.obj.monitor
    )

    ctx.obj.datastore_impl = [d for d in DATASTORES if d.TYPE == datastore][0]

    if datastore_root is None:
        datastore_root = ctx.obj.datastore_impl.get_datastore_root_from_config(
            ctx.obj.echo
        )
    if datastore_root is None:
        raise CommandException(
            "Could not find the location of the datastore -- did you correctly set the "
            "METAFLOW_DATASTORE_SYSROOT_%s environment variable?" % datastore.upper()
        )

    ctx.obj.datastore_impl.datastore_root = datastore_root

    FlowDataStore.default_storage_impl = ctx.obj.datastore_impl
    ctx.obj.flow_datastore = FlowDataStore(
        ctx.obj.flow.name,
        ctx.obj.environment,
        ctx.obj.metadata,
        ctx.obj.event_logger,
        ctx.obj.monitor,
    )

    # It is important to initialize flow decorators early as some of the
    # things they provide may be used by some of the objects initialized after.
    decorators._init_flow_decorators(
        ctx.obj.flow,
        ctx.obj.graph,
        ctx.obj.environment,
        ctx.obj.flow_datastore,
        ctx.obj.metadata,
        ctx.obj.logger,
        echo,
        deco_options,
    )

    # In the case of run/resume, we will want to apply the TL decospecs
    # *after* the run decospecs so that they don't take precedence. In other
    # words, for the same decorator, we want `myflow.py run --with foo` to
    # take precedence over any other `foo` decospec
    ctx.obj.tl_decospecs = list(decospecs or [])

    # initialize current and parameter context for deploy-time parameters
    current._set_env(flow=ctx.obj.flow, is_running=False)
    parameters.set_parameter_context(
        ctx.obj.flow.name, ctx.obj.echo, ctx.obj.flow_datastore
    )

    if ctx.invoked_subcommand not in ("run", "resume"):
        # run/resume are special cases because they can add more decorators with --with,
        # so they have to take care of themselves.
        all_decospecs = ctx.obj.tl_decospecs + list(
            ctx.obj.environment.decospecs() or []
        )
        if all_decospecs:
            decorators._attach_decorators(ctx.obj.flow, all_decospecs)
            # Regenerate graph if we attached more decorators
            ctx.obj.graph = FlowGraph(ctx.obj.flow.__class__)

        decorators._init_step_decorators(
            ctx.obj.flow,
            ctx.obj.graph,
            ctx.obj.environment,
            ctx.obj.flow_datastore,
            ctx.obj.logger,
        )

        # TODO (savin): Enable lazy instantiation of package
        ctx.obj.package = None
    if ctx.invoked_subcommand is None:
        ctx.invoke(check)


def _reconstruct_cli(params):
    for k, v in params.items():
        if v:
            if k == "decospecs":
                k = "with"
            k = k.replace("_", "-")
            if not isinstance(v, tuple):
                v = [v]
            for value in v:
                yield "--%s" % k
                if not isinstance(value, bool):
                    yield str(value)


def _check(graph, flow, environment, pylint=True, warnings=False, **kwargs):
    echo("Validating your flow...", fg="magenta", bold=False)
    linter = lint.linter
    # TODO set linter settings
    linter.run_checks(graph, **kwargs)
    echo("The graph looks good!", fg="green", bold=True, indent=True)
    if pylint:
        echo("Running pylint...", fg="magenta", bold=False)
        fname = inspect.getfile(flow.__class__)
        pylint = PyLint(fname)
        if pylint.has_pylint():
            pylint_is_happy, pylint_exception_msg = pylint.run(
                warnings=warnings,
                pylint_config=environment.pylint_config(),
                logger=echo_always,
            )

            if pylint_is_happy:
                echo("Pylint is happy!", fg="green", bold=True, indent=True)
            else:
                echo(
                    "Pylint couldn't analyze your code.\n\tPylint exception: %s"
                    % pylint_exception_msg,
                    fg="red",
                    bold=True,
                    indent=True,
                )
                echo("Skipping Pylint checks.", fg="red", bold=True, indent=True)
        else:
            echo(
                "Pylint not found, so extra checks are disabled.",
                fg="green",
                indent=True,
                bold=False,
            )


def print_metaflow_exception(ex):
    echo_always(ex.headline, indent=True, nl=False, bold=True)
    if ex.line_no is None:
        echo_always(":")
    else:
        echo_always(" on line %d:" % ex.line_no, bold=True)
    echo_always(ex.message, indent=True, bold=False, padding_bottom=True)


def print_unknown_exception(ex):
    echo_always("Internal error", indent=True, bold=True)
    echo_always(traceback.format_exc(), highlight=None, highlight_bold=False)


class CliState(object):
    def __init__(self, flow):
        self.flow = flow


def main(flow, args=None, handle_exceptions=True, entrypoint=None):
    # Ignore warning(s) and prevent spamming the end-user.
    # TODO: This serves as a short term workaround for RuntimeWarning(s) thrown
    # in py3.8 related to log buffering (bufsize=1).
    import warnings

    warnings.filterwarnings("ignore")
    if entrypoint is None:
        entrypoint = [sys.executable, sys.argv[0]]

    state = CliState(flow)
    state.entrypoint = entrypoint

    try:
        if args is None:
            start(auto_envvar_prefix="METAFLOW", obj=state)
        else:
            try:
                start(args=args, obj=state, auto_envvar_prefix="METAFLOW")
            except SystemExit as e:
                return e.code
    except MetaflowException as x:
        if handle_exceptions:
            print_metaflow_exception(x)
            sys.exit(1)
        else:
            raise
    except Exception as x:
        if handle_exceptions:
            print_unknown_exception(x)
            sys.exit(1)
        else:
            raise
    finally:
        if hasattr(state, "monitor") and state.monitor is not None:
            state.monitor.terminate()
        if hasattr(state, "event_logger") and state.event_logger is not None:
            state.event_logger.terminate()
