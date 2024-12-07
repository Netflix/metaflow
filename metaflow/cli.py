import functools
import inspect
import sys
import traceback
from datetime import datetime

import metaflow.tracing as tracing
from metaflow._vendor import click

from . import decorators, lint, metaflow_version, parameters, plugins
from .cli_args import cli_args
from .cli_components.utils import LazyGroup, LazyPluginCommandCollection
from .datastore import FlowDataStore
from .exception import CommandException, MetaflowException
from .flowspec import _FlowState
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
from .plugins import (
    DATASTORES,
    ENVIRONMENTS,
    LOGGING_SIDECARS,
    METADATA_PROVIDERS,
    MONITOR_SIDECARS,
)
from .pylint_wrapper import PyLint
from .R import metaflow_r_version, use_r
from .util import resolve_identity
from .user_configs.config_options import LocalFileInput, config_options
from .user_configs.config_parameters import ConfigValue

ERASE_TO_EOL = "\033[K"
HIGHLIGHT = "red"
INDENT = " " * 4

LOGGER_TIMESTAMP = "magenta"
LOGGER_COLOR = "green"
LOGGER_BAD_COLOR = "red"


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


@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        "init": "metaflow.cli_components.init_cmd.init",
        "dump": "metaflow.cli_components.dump_cmd.dump",
        "step": "metaflow.cli_components.step_cmd.step",
        "run": "metaflow.cli_components.run_cmds.run",
        "resume": "metaflow.cli_components.run_cmds.resume",
    },
)
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
    if obj.is_quiet:
        echo = echo_dev_null
    else:
        echo = echo_always
    _check(
        echo, obj.graph, obj.flow, obj.environment, pylint=obj.pylint, warnings=warnings
    )
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


@cli.command(help="Print the Metaflow version")
@click.pass_obj
def version(obj):
    echo_always(obj.version)


# NOTE: add_decorator_options should be TL because it checks to make sure
# that no option conflict with the ones below
@decorators.add_decorator_options
@config_options
@click.command(
    cls=LazyPluginCommandCollection,
    sources=[cli],
    lazy_sources=plugins.get_plugin_cli_path(),
    invoke_without_command=True,
)
@tracing.cli_entrypoint("cli/start")
# Quiet is eager to make sure it is available when processing --config options since
# we need it to construct a context to pass to any DeployTimeField for the default
# value.
@click.option(
    "--quiet/--not-quiet",
    show_default=True,
    default=False,
    help="Suppress unnecessary messages",
    is_eager=True,
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
# See comment for --quiet
@click.option(
    "--datastore",
    default=DEFAULT_DATASTORE,
    show_default=True,
    type=click.Choice([d.TYPE for d in DATASTORES]),
    help="Data backend type",
    is_eager=True,
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
@click.option(
    "--local-config-file",
    type=LocalFileInput(exists=True, readable=True, dir_okay=False, resolve_path=True),
    required=False,
    default=None,
    help="A filename containing the dumped configuration values. Internal use only.",
    hidden=True,
    is_eager=True,
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
    local_config_file=None,
    config_file_options=None,
    config_value_options=None,
    **deco_options
):
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

    # At this point, we are able to resolve the user-configuration options so we can
    # process all those decorators that the user added that will modify the flow based
    # on those configurations. It is important to do this as early as possible since it
    # actually modifies the flow itself

    # When we process the options, the first one processed will return None and the
    # second one processed will return the actual options. The order of processing
    # depends on what (and in what order) the user specifies on the command line.
    config_options = config_file_options or config_value_options
    ctx.obj.flow = ctx.obj.flow._process_config_decorators(config_options)

    cli_args._set_top_kwargs(ctx.params)
    ctx.obj.echo = echo
    ctx.obj.echo_always = echo_always
    ctx.obj.is_quiet = quiet
    ctx.obj.graph = ctx.obj.flow._graph
    ctx.obj.logger = logger
    ctx.obj.pylint = pylint
    ctx.obj.check = functools.partial(_check, echo)
    ctx.obj.top_cli = cli
    ctx.obj.package_suffixes = package_suffixes.split(",")

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

    ctx.obj.config_options = config_options

    decorators._init(ctx.obj.flow)

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
        ctx.obj.flow.name,
        ctx.obj.echo,
        ctx.obj.flow_datastore,
        {
            k: ConfigValue(v)
            for k, v in ctx.obj.flow.__class__._flow_state.get(
                _FlowState.CONFIGS, {}
            ).items()
        },
    )

    if (
        hasattr(ctx, "saved_args")
        and ctx.saved_args
        and ctx.saved_args[0] not in ("run", "resume")
    ):
        # run/resume are special cases because they can add more decorators with --with,
        # so they have to take care of themselves.
        all_decospecs = ctx.obj.tl_decospecs + list(
            ctx.obj.environment.decospecs() or []
        )
        if all_decospecs:
            decorators._attach_decorators(ctx.obj.flow, all_decospecs)
            decorators._init(ctx.obj.flow)
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


def _check(echo, graph, flow, environment, pylint=True, warnings=False, **kwargs):
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
