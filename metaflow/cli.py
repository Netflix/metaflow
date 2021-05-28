import inspect
import sys
import traceback
from datetime import datetime
from functools import wraps

import click

from . import lint
from . import plugins
from . import parameters
from . import decorators
from . import metaflow_version
from . import namespace
from . import current
from .util import resolve_identity, decompress_list, write_latest_run_id, \
    get_latest_run_id, to_unicode
from .task import MetaflowTask
from .exception import CommandException, MetaflowException
from .graph import FlowGraph
from .datastore import DATASTORES
from .runtime import NativeRuntime
from .package import MetaflowPackage
from .plugins import ENVIRONMENTS, LOGGING_SIDECARS, METADATA_PROVIDERS, MONITOR_SIDECARS
from .metaflow_config import DEFAULT_DATASTORE, DEFAULT_ENVIRONMENT, DEFAULT_EVENT_LOGGER, \
    DEFAULT_METADATA, DEFAULT_MONITOR, DEFAULT_PACKAGE_SUFFIXES
from .metaflow_environment import MetaflowEnvironment
from .pylint_wrapper import PyLint
from .event_logger import EventLogger
from .monitor import Monitor
from .R import use_r, metaflow_r_version
from .mflog import mflog, LOG_SOURCES


ERASE_TO_EOL = '\033[K'
HIGHLIGHT = 'red'
INDENT = ' ' * 4

LOGGER_TIMESTAMP = 'magenta'
LOGGER_COLOR = 'green'
LOGGER_BAD_COLOR = 'red'

try:
    # Python 2
    import cPickle as pickle
except ImportError:
    # Python 3
    import pickle


def echo_dev_null(*args, **kwargs):
    pass


def echo_always(line, **kwargs):
    kwargs['err'] = kwargs.get('err', True)
    if kwargs.pop('indent', None):
        line = '\n'.join(INDENT + x for x in line.splitlines())
    if 'nl' not in kwargs or kwargs['nl']:
        line += ERASE_TO_EOL
    top = kwargs.pop('padding_top', None)
    bottom = kwargs.pop('padding_bottom', None)
    highlight = kwargs.pop('highlight', HIGHLIGHT)
    if top:
        click.secho(ERASE_TO_EOL, **kwargs)

    hl_bold = kwargs.pop('highlight_bold', True)
    nl = kwargs.pop('nl', True)
    fg = kwargs.pop('fg', None)
    bold = kwargs.pop('bold', False)
    kwargs['nl'] = False
    hl = True
    nobold = kwargs.pop('no_bold', False)
    if nobold:
        click.secho(line, **kwargs)
    else:
        for span in line.split('*'):
            if hl:
                hl = False
                kwargs['fg'] = fg
                kwargs['bold'] = bold
                click.secho(span, **kwargs)
            else:
                hl = True
                kwargs['fg'] = highlight
                kwargs['bold'] = hl_bold
                click.secho(span, **kwargs)
    if nl:
        kwargs['nl'] = True
        click.secho('', **kwargs)
    if bottom:
        click.secho(ERASE_TO_EOL, **kwargs)


def logger(body='',
           system_msg=False,
           head='',
           bad=False,
           timestamp=True):
    if timestamp:
        if timestamp is True:
            dt = datetime.now()
        else:
            dt = timestamp
        tstamp = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        click.secho(tstamp + ' ', fg=LOGGER_TIMESTAMP, nl=False)
    if head:
        click.secho(head, fg=LOGGER_COLOR, nl=False)
    click.secho(body,
                bold=system_msg,
                fg=LOGGER_BAD_COLOR if bad else None)


@click.group()
def cli(ctx):
    pass


@cli.command(help='Check that the flow is valid (default).')
@click.option('--warnings/--no-warnings',
              default=False,
              show_default=True,
              help='Show all Pylint warnings, not just errors.')
@click.pass_obj
def check(obj, warnings=False):
    _check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint, warnings=warnings)
    fname = inspect.getfile(obj.flow.__class__)
    echo("\n*'{cmd} show'* shows a description of this flow.\n"
         "*'{cmd} run'* runs the flow locally.\n"
         "*'{cmd} help'* shows all available commands and options.\n"
         .format(cmd=fname), highlight='magenta', highlight_bold=False)


@cli.command(help='Show structure of the flow.')
@click.pass_obj
def show(obj):
    echo_always('\n%s' % obj.graph.doc)
    for _, node in sorted((n.func_lineno, n) for n in obj.graph):
        echo_always('\nStep *%s*' % node.name, err=False)
        echo_always(node.doc if node.doc else '?', indent=True, err=False)
        if node.type != 'end':
            echo_always('*=>* %s' % ', '.join('*%s*' % n for n in node.out_funcs),
                        indent=True,
                        highlight='magenta',
                        highlight_bold=False,
                        err=False)
    echo_always('')


@cli.command(help='Show all available commands.')
@click.pass_context
def help(ctx):
    print(ctx.parent.get_help())


@cli.command(help='Output internal state of the flow graph.')
@click.pass_obj
def output_raw(obj):
    echo('Internal representation of the flow:',
         fg='magenta',
         bold=False)
    echo_always(str(obj.graph), err=False)


@cli.command(help='Visualize the flow with Graphviz.')
@click.pass_obj
def output_dot(obj):
    echo('Visualizing the flow as a GraphViz graph',
         fg='magenta',
         bold=False)
    echo("Try piping the output to 'dot -Tpng -o graph.png' to produce "
         "an actual image.", indent=True)
    echo_always(obj.graph.output_dot(), err=False)


@cli.command(help='Get data artifacts of a task or all tasks in a step. '
                  'The format for input-path is either <run_id>/<step_name> or '
                  '<run_id>/<step_name>/<task_id>.')
@click.argument('input-path')
@click.option('--private/--no-private',
              default=False,
              show_default=True,
              help='Show also private attributes.')
@click.option('--max-value-size',
              default=1000,
              show_default=True,
              type=int,
              help='Show only values that are smaller than this number. '
                   'Set to 0 to see only keys.')
@click.option('--include',
              type=str,
              default='',
              help='Include only artifacts in the given comma-separated list.')
@click.option('--file',
              type=str,
              default=None,
              help='Serialize artifacts in the given file.')
@click.pass_obj
def dump(obj,
         input_path,
         private=None,
         max_value_size=None,
         include=None,
         file=None):

    output = {}
    kwargs = {'show_private': private,
              'max_value_size': max_value_size,
              'include': {t for t in include.split(',') if t}}

    if obj.datastore.datastore_root is None:
        obj.datastore.datastore_root = obj.datastore.get_datastore_root_from_config(
            obj.echo, create_on_absent=False)
    if obj.datastore.datastore_root is None:
        raise CommandException(
            "Could not find the location of the datastore -- did you correctly set the "
            "METAFLOW_DATASTORE_SYSROOT_%s environment variable" % (obj.datastore.TYPE).upper())

    # Pathspec can either be run_id/step_name or run_id/step_name/task_id.
    parts = input_path.split('/')
    if len(parts) == 2:
        run_id, step_name = parts
        task_id = None
    elif len(parts) == 3:
        run_id, step_name, task_id = parts
    else:
        raise CommandException("input_path should either be run_id/step_name"
                               "or run_id/step_name/task_id")

    from metaflow.datastore.datastore_set import MetaflowDatastoreSet

    datastore_set = MetaflowDatastoreSet(
                        obj.datastore,
                        obj.flow.name,
                        run_id,
                        steps=[step_name],
                        metadata=obj.metadata,
                        monitor=obj.monitor,
                        event_logger=obj.event_logger,
                        prefetch_data_artifacts=kwargs.get('include'))
    if task_id:
        ds_list = [datastore_set.get_with_pathspec(input_path)]
    else:
        ds_list = list(datastore_set) # get all tasks

    for ds in ds_list:
        echo('Dumping output of run_id=*{run_id}* '
             'step=*{step}* task_id=*{task_id}*'.format(run_id=ds.run_id,
                                                        step=ds.step_name,
                                                        task_id=ds.task_id),
             fg='magenta')

        if file is None:
            echo_always(ds.format(**kwargs),
                        highlight='green',
                        highlight_bold=False,
                        err=False)
        else:
            output[ds.pathspec] = ds.to_dict(**kwargs)

    if file is not None:
        with open(file, 'wb') as f:
            pickle.dump(output, f, protocol=pickle.HIGHEST_PROTOCOL)
        echo('Artifacts written to *%s*' % file)


@cli.command(help='Show stdout/stderr produced by a task or all tasks in a step. '
                  'The format for input-path is either <run_id>/<step_name> or '
                  '<run_id>/<step_name>/<task_id>.')
@click.argument('input-path')
@click.option('--stdout/--no-stdout',
              default=False,
              show_default=True,
              help='Show stdout of the task.')
@click.option('--stderr/--no-stderr',
              default=False,
              show_default=True,
              help='Show stderr of the task.')
@click.option('--both/--no-both',
              default=True,
              show_default=True,
              help='Show both stdout and stderr of the task.')
@click.option('--timestamps/--no-timestamps',
              default=False,
              show_default=True,
              help='Show timestamps.')
@click.pass_obj
def logs(obj,
         input_path,
         stdout=None,
         stderr=None,
         both=None,
         timestamps=False):
    types = set()
    if stdout:
        types.add('stdout')
        both = False
    if stderr:
        types.add('stderr')
        both = False
    if both:
        types.update(('stdout', 'stderr'))

    streams = list(sorted(types, reverse=True))

    # Pathspec can either be run_id/step_name or run_id/step_name/task_id.
    parts = input_path.split('/')
    if len(parts) == 2:
        run_id, step_name = parts
        task_id = None
    elif len(parts) == 3:
        run_id, step_name, task_id = parts
    else:
        raise CommandException("input_path should either be run_id/step_name "
                               "or run_id/step_name/task_id")

    if obj.datastore.datastore_root is None:
        obj.datastore.datastore_root = obj.datastore.get_datastore_root_from_config(
            obj.echo, create_on_absent=False)
    if obj.datastore.datastore_root is None:
        raise CommandException(
            "Could not find the location of the datastore -- did you correctly set the "
            "METAFLOW_DATASTORE_SYSROOT_%s environment variable" % (obj.datastore.TYPE).upper())

    if task_id:
        ds_list = [obj.datastore(obj.flow.name,
                                 run_id=run_id,
                                 step_name=step_name,
                                 task_id=task_id,
                                 mode='r',
                                 allow_unsuccessful=True)]
    else:
        from metaflow.datastore.datastore_set import MetaflowDatastoreSet
        datastore_set = MetaflowDatastoreSet(
                            obj.datastore,
                            obj.flow.name,
                            run_id,
                            steps=[step_name],
                            metadata=obj.metadata,
                            monitor=obj.monitor,
                            event_logger=obj.event_logger)
        # get all successful tasks
        ds_list = list(datastore_set)

    if ds_list:
        def echo_unicode(line, **kwargs):
            click.secho(line.decode('UTF-8', errors='replace'), **kwargs)

        # old style logs are non mflog-style logs
        maybe_old_style = True
        for ds in ds_list:
            echo('Dumping logs of run_id=*{run_id}* '
                 'step=*{step}* task_id=*{task_id}*'.format(run_id=ds.run_id,
                                                            step=ds.step_name,
                                                            task_id=ds.task_id),
                 fg='magenta')

            for stream in streams:
                echo(stream, bold=True)
                logs = ds.load_logs(LOG_SOURCES, stream)
                if any(data for _, data in logs):
                    # attempt to read new, mflog-style logs
                    for line in mflog.merge_logs([blob for _, blob in logs]):
                        if timestamps:
                            ts = mflog.utc_to_local(line.utc_tstamp)
                            tstamp = ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            click.secho(tstamp + ' ',
                                        fg=LOGGER_TIMESTAMP,
                                        nl=False)
                        echo_unicode(line.msg)
                    maybe_old_style = False
                elif maybe_old_style:
                    # if they are not available, we may be looking at
                    # a legacy run (unless we have seen new-style data already
                    # for another stream). This return an empty string if
                    # nothing is found
                    log = ds.load_log_legacy(stream)
                    if log and timestamps:
                        raise CommandException("We can't show --timestamps for "
                                               "old runs. Sorry!")
                    echo_unicode(log, nl=False)

    elif len(parts) == 2:
        # TODO if datastore provided a way to find unsuccessful task IDs, we
        # could make handle this case automatically
        raise CommandException("Successful tasks were not found at the given "
                               "path. You can see logs for unsuccessful tasks "
                               "by giving an exact task ID using the "
                               "run_id/step_name/task_id format.")


# TODO - move step and init under a separate 'internal' subcommand

@cli.command(help="Internal command to execute a single task.")
@click.argument('step-name')
@click.option('--run-id',
              default=None,
              required=True,
              help='ID for one execution of all steps in the flow.')
@click.option('--task-id',
              default=None,
              required=True,
              show_default=True,
              help='ID for this instance of the step.')
@click.option('--input-paths',
              help='A comma-separated list of pathspecs '
                   'specifying inputs for this step.')
@click.option('--split-index',
              type=int,
              default=None,
              show_default=True,
              help='Index of this foreach split.')
@click.option('--tag',
              'tags',
              multiple=True,
              default=None,
              help="Annotate this run with the given tag. You can specify "
                   "this option multiple times to attach multiple tags in "
                   "the task.")
@click.option('--namespace',
              'user_namespace',
              default=None,
              help="Change namespace from the default (your username) to "
                   "the specified tag.")
@click.option('--retry-count',
              default=0,
              help="How many times we have attempted to run this task.")
@click.option('--max-user-code-retries',
              default=0,
              help="How many times we should attempt running the user code.")
@click.option('--clone-only',
              default=None,
              help="Pathspec of the origin task for this task to clone. Do "
              "not execute anything.")
@click.option('--clone-run-id',
              default=None,
              help="Run id of the origin flow, if this task is part of a flow "
              "being resumed.")
@click.option('--with',
              'decospecs',
              multiple=True,
              help="Add a decorator to this task. You can specify this "
              "option multiple times to attach multiple decorators "
              "to this task.")
@click.pass_obj
def step(obj,
         step_name,
         tags=None,
         run_id=None,
         task_id=None,
         input_paths=None,
         split_index=None,
         user_namespace=None,
         retry_count=None,
         max_user_code_retries=None,
         clone_only=None,
         clone_run_id=None,
         decospecs=None):
    if user_namespace is not None:
        namespace(user_namespace or None)

    func = None
    try:
        func = getattr(obj.flow, step_name)
    except:
        raise CommandException("Step *%s* doesn't exist." % step_name)
    if not func.is_step:
        raise CommandException("Function *%s* is not a step." % step_name)
    echo('Executing a step, *%s*' % step_name,
         fg='magenta',
         bold=False)

    if decospecs:
        decorators._attach_decorators_to_step(func, decospecs)

    obj.datastore.datastore_root = obj.datastore_root
    if obj.datastore.datastore_root is None:
        obj.datastore.datastore_root = \
            obj.datastore.get_datastore_root_from_config(obj.echo)

    obj.metadata.add_sticky_tags(tags=tags)
    paths = decompress_list(input_paths) if input_paths else []

    task = MetaflowTask(obj.flow,
                        obj.datastore,
                        obj.metadata,
                        obj.environment,
                        obj.echo,
                        obj.event_logger,
                        obj.monitor)
    if clone_only:
        task.clone_only(step_name,
                        run_id,
                        task_id,
                        clone_only)
    else:
        task.run_step(step_name,
                      run_id,
                      task_id,
                      clone_run_id,
                      paths,
                      split_index,
                      retry_count,
                      max_user_code_retries)

    echo('Success', fg='green', bold=True, indent=True)

@parameters.add_custom_parameters(deploy_mode=False)
@cli.command(help="Internal command to initialize a run.")
@click.option('--run-id',
              default=None,
              required=True,
              help='ID for one execution of all steps in the flow.')
@click.option('--task-id',
              default=None,
              required=True,
              help='ID for this instance of the step.')
@click.option('--tag',
              'tags',
              multiple=True,
              default=None,
              help="Tags for this instance of the step.")
@click.pass_obj
def init(obj, run_id=None, task_id=None, tags=None, **kwargs):
    # init is a separate command instead of an option in 'step'
    # since we need to capture user-specified parameters with
    # @add_custom_parameters. Adding custom parameters to 'step'
    # is not desirable due to the possibility of name clashes between
    # user-specified parameters and our internal options. Note that
    # user-specified parameters are often defined as environment
    # variables.

    if obj.datastore.datastore_root is None:
        obj.datastore.datastore_root = \
            obj.datastore.get_datastore_root_from_config(obj.echo)

    obj.metadata.add_sticky_tags(tags=tags)

    runtime = NativeRuntime(obj.flow,
                            obj.graph,
                            obj.datastore,
                            obj.metadata,
                            obj.environment,
                            obj.package,
                            obj.logger,
                            obj.entrypoint,
                            obj.event_logger,
                            obj.monitor,
                            run_id=run_id)
    parameters.set_parameters(obj.flow, kwargs)
    runtime.persist_parameters(task_id=task_id)

def common_run_options(func):
    @click.option('--tag',
                  'tags',
                  multiple=True,
                  default=None,
                  help="Annotate this run with the given tag. You can specify "
                       "this option multiple times to attach multiple tags in "
                       "the run.")
    @click.option('--max-workers',
                  default=16,
                  show_default=True,
                  help='Maximum number of parallel processes.')
    @click.option('--max-num-splits',
                  default=100,
                  show_default=True,
                  help='Maximum number of splits allowed in a foreach. This '
                       'is a safety check preventing bugs from triggering '
                       'thousands of steps inadvertently.')
    @click.option('--max-log-size',
                  default=10,
                  show_default=True,
                  help='Maximum size of stdout and stderr captured in '
                       'megabytes. If a step outputs more than this to '
                       'stdout/stderr, its output will be truncated.')
    @click.option('--with',
                  'decospecs',
                  multiple=True,
                  help="Add a decorator to all steps. You can specify this "
                       "option multiple times to attach multiple decorators "
                       "in steps.")
    @click.option('--run-id-file',
                  default=None,
                  show_default=True,
                  type=str,
                  help="Write the ID of this run to the file specified.")
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.option('--origin-run-id',
              default=None,
              help="ID of the run that should be resumed. By default, the "
                   "last run executed locally.")
@click.argument('step-to-rerun',
                required=False)
@cli.command(help='Resume execution of a previous run of this flow.')
@common_run_options
@click.pass_obj
def resume(obj,
           tags=None,
           step_to_rerun=None,
           origin_run_id=None,
           max_workers=None,
           max_num_splits=None,
           max_log_size=None,
           decospecs=None,
           run_id_file=None):

    before_run(obj, tags, decospecs + obj.environment.decospecs())

    if origin_run_id is None:
        origin_run_id = get_latest_run_id(obj.echo, obj.flow.name)
        if origin_run_id is None:
            raise CommandException("A previous run id was not found. Specify --origin-run-id.")

    if step_to_rerun is None:
        clone_steps = set()
    else:
        # validate step name
        if step_to_rerun not in obj.graph.nodes:
            raise CommandException(
                "invalid step name {0} specified, must be step present in "
                "current form of execution graph. Valid step names include: {1}"
                .format(
                    step_to_rerun,
                    ",".join(list(obj.graph.nodes.keys()))))
        clone_steps = {step_to_rerun}

    runtime = NativeRuntime(obj.flow,
                            obj.graph,
                            obj.datastore,
                            obj.metadata,
                            obj.environment,
                            obj.package,
                            obj.logger,
                            obj.entrypoint,
                            obj.event_logger,
                            obj.monitor,
                            clone_run_id=origin_run_id,
                            clone_steps=clone_steps,
                            max_workers=max_workers,
                            max_num_splits=max_num_splits,
                            max_log_size=max_log_size * 1024 * 1024)
    runtime.persist_parameters()
    runtime.execute()

    write_run_id(run_id_file, runtime.run_id)


@parameters.add_custom_parameters(deploy_mode=True)
@cli.command(help='Run the workflow locally.')
@common_run_options
@click.option('--namespace',
              'user_namespace',
              default=None,
              help="Change namespace from the default (your username) to "
                   "the specified tag. Note that this option does not alter "
                   "tags assigned to the objects produced by this run, just "
                   "what existing objects are visible in the client API. You "
                   "can enable the global namespace with an empty string."
                   "--namespace=")
@click.pass_obj
def run(obj,
        tags=None,
        max_workers=None,
        max_num_splits=None,
        max_log_size=None,
        decospecs=None,
        run_id_file=None,
        user_namespace=None,
        **kwargs):

    if user_namespace is not None:
        namespace(user_namespace or None)
    before_run(obj, tags, decospecs + obj.environment.decospecs())

    runtime = NativeRuntime(obj.flow,
                            obj.graph,
                            obj.datastore,
                            obj.metadata,
                            obj.environment,
                            obj.package,
                            obj.logger,
                            obj.entrypoint,
                            obj.event_logger,
                            obj.monitor,
                            max_workers=max_workers,
                            max_num_splits=max_num_splits,
                            max_log_size=max_log_size * 1024 * 1024)
    write_latest_run_id(obj, runtime.run_id)
    write_run_id(run_id_file, runtime.run_id)

    parameters.set_parameters(obj.flow, kwargs)
    runtime.persist_parameters()
    runtime.execute()


def write_run_id(run_id_file, run_id):
    if run_id_file is not None:
        with open(run_id_file, 'w') as f:
            f.write(str(run_id))


def before_run(obj, tags, decospecs):
    # There's a --with option both at the top-level and for the run
    # subcommand. Why?
    #
    # "run --with shoes" looks so much better than "--with shoes run".
    # This is a very common use case of --with.
    #
    # A downside is that we need to have the following decorators handling
    # in two places in this module and we need to make sure that
    # _init_step_decorators doesn't get called twice.
    if decospecs:
        decorators._attach_decorators(obj.flow, decospecs)
        obj.graph = FlowGraph(obj.flow.__class__)
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    #obj.environment.init_environment(obj.logger)

    if obj.datastore.datastore_root is None:
        obj.datastore.datastore_root = \
            obj.datastore.get_datastore_root_from_config(obj.echo)

    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger)
    obj.metadata.add_sticky_tags(tags=tags)

    # Package working directory only once per run.
    # We explicitly avoid doing this in `start` since it is invoked for every
    # step in the run.
    obj.package = MetaflowPackage(obj.flow,
                                  obj.environment,
                                  obj.echo,
                                  obj.package_suffixes)


@cli.command(help='Print the Metaflow version')
@click.pass_obj
def version(obj):
    echo_always(obj.version)

@decorators.add_decorator_options
@click.command(cls=click.CommandCollection,
               sources=[cli] + plugins.get_plugin_cli(),
               invoke_without_command=True)
@click.option('--quiet/--not-quiet',
              show_default=True,
              default=False,
              help='Suppress unnecessary messages')
@click.option('--metadata',
              default=DEFAULT_METADATA,
              show_default=True,
              type=click.Choice([m.TYPE for m in METADATA_PROVIDERS]),
              help='Metadata service type')
@click.option('--environment',
              default=DEFAULT_ENVIRONMENT,
              show_default=True,
              type=click.Choice(['local'] + [m.TYPE for m in ENVIRONMENTS]),
              help='Execution environment type')
@click.option('--datastore',
              default=DEFAULT_DATASTORE,
              show_default=True,
              type=click.Choice(DATASTORES),
              help='Data backend type')
@click.option('--datastore-root',
              help='Root path for datastore')
@click.option('--package-suffixes',
              help='A comma-separated list of file suffixes to include '
                   'in the code package.',
              default=DEFAULT_PACKAGE_SUFFIXES,
              show_default=True)
@click.option('--with',
              'decospecs',
              multiple=True,
              help="Add a decorator to all steps. You can specify this option "
                   "multiple times to attach multiple decorators in steps.")
@click.option('--pylint/--no-pylint',
              default=True,
              show_default=True,
              help='Run Pylint on the flow if pylint is installed.')
@click.option('--coverage',
              is_flag=True,
              default=False,
              show_default=True,
              help='Measure code coverage using coverage.py.')
@click.option('--event-logger',
              default=DEFAULT_EVENT_LOGGER,
              show_default=True,
              type=click.Choice(LOGGING_SIDECARS),
              help='type of event logger used')
@click.option('--monitor',
              default=DEFAULT_MONITOR,
              show_default=True,
              type=click.Choice(MONITOR_SIDECARS),
              help='Monitoring backend type')
@click.pass_context
def start(ctx,
          quiet=False,
          metadata=None,
          environment=None,
          datastore=None,
          datastore_root=None,
          decospecs=None,
          package_suffixes=None,
          pylint=None,
          coverage=None,
          event_logger=None,
          monitor=None,
          **deco_options):
    global echo
    if quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    ctx.obj.version = metaflow_version.get_version()
    version = ctx.obj.version
    if use_r():
        version = metaflow_r_version()

    echo('Metaflow %s' % version, fg='magenta', bold=True, nl=False)
    echo(" executing *%s*" % ctx.obj.flow.name, fg='magenta', nl=False)
    echo(" for *%s*" % resolve_identity(), fg='magenta')

    if coverage:
        from coverage import Coverage
        cov = Coverage(data_suffix=True,
                       auto_data=True,
                       source=['metaflow'],
                       branch=True)
        cov.start()

    ctx.obj.echo = echo
    ctx.obj.echo_always = echo_always
    ctx.obj.graph = FlowGraph(ctx.obj.flow.__class__)
    ctx.obj.logger = logger
    ctx.obj.check = _check
    ctx.obj.pylint = pylint
    ctx.obj.top_cli = cli
    ctx.obj.package_suffixes = package_suffixes.split(',')
    ctx.obj.reconstruct_cli = _reconstruct_cli

    ctx.obj.event_logger = EventLogger(event_logger)

    ctx.obj.environment = [e for e in ENVIRONMENTS + [MetaflowEnvironment]
                           if e.TYPE == environment][0](ctx.obj.flow)
    ctx.obj.environment.validate_environment(echo)

    ctx.obj.datastore = DATASTORES[datastore]
    ctx.obj.datastore_root = datastore_root

    # It is important to initialize flow decorators early as some of the
    # things they provide may be used by some of the objects initialize after.
    decorators._init_flow_decorators(ctx.obj.flow,
                                     ctx.obj.graph,
                                     ctx.obj.environment,
                                     ctx.obj.datastore,
                                     ctx.obj.logger,
                                     echo,
                                     deco_options)

    ctx.obj.monitor = Monitor(monitor, ctx.obj.environment, ctx.obj.flow.name)
    ctx.obj.monitor.start()

    ctx.obj.metadata = [m for m in METADATA_PROVIDERS
                        if m.TYPE == metadata][0](ctx.obj.environment,
                                                  ctx.obj.flow,
                                                  ctx.obj.event_logger,
                                                  ctx.obj.monitor)
    ctx.obj.datastore = DATASTORES[datastore]

    if datastore_root is None:
        datastore_root = \
          ctx.obj.datastore.get_datastore_root_from_config(ctx.obj.echo)
    ctx.obj.datastore_root = ctx.obj.datastore.datastore_root = datastore_root

    if decospecs:
        decorators._attach_decorators(ctx.obj.flow, decospecs)

    # initialize current and parameter context for deploy-time parameters
    current._set_env(flow_name=ctx.obj.flow.name, is_running=False)
    parameters.set_parameter_context(ctx.obj.flow.name,
                                        ctx.obj.echo,
                                        ctx.obj.datastore)

    if ctx.invoked_subcommand not in ('run', 'resume'):
        # run/resume are special cases because they can add more decorators with --with,
        # so they have to take care of themselves.
        decorators._attach_decorators(
            ctx.obj.flow, ctx.obj.environment.decospecs())
        decorators._init_step_decorators(
            ctx.obj.flow, ctx.obj.graph, ctx.obj.environment, ctx.obj.datastore, ctx.obj.logger)
        #TODO (savin): Enable lazy instantiation of package
        ctx.obj.package = None
    if ctx.invoked_subcommand is None:
        ctx.invoke(check)


def _reconstruct_cli(params):
    for k, v in params.items():
        if v:
            if k == 'decospecs':
                k = 'with'
            k = k.replace('_', '-')
            if not isinstance(v, tuple):
                v = [v]
            for value in v:
                yield '--%s' % k
                if not isinstance(value, bool):
                    yield str(value)


def _check(graph, flow, environment, pylint=True, warnings=False, **kwargs):
    echo("Validating your flow...", fg='magenta', bold=False)
    linter = lint.linter
    # TODO set linter settings
    linter.run_checks(graph, **kwargs)
    echo('The graph looks good!', fg='green', bold=True, indent=True)
    if pylint:
        echo("Running pylint...", fg='magenta', bold=False)
        fname = inspect.getfile(flow.__class__)
        pylint = PyLint(fname)
        if pylint.has_pylint():
            pylint_is_happy, pylint_exception_msg = pylint.run(
                        warnings=warnings,
                        pylint_config=environment.pylint_config(),
                        logger=echo_always)

            if pylint_is_happy:
                echo('Pylint is happy!',
                     fg='green',
                     bold=True,
                     indent=True)
            else:
                echo("Pylint couldn't analyze your code.\n\tPylint exception: %s"
                     % pylint_exception_msg,
                     fg='red',
                     bold=True,
                     indent=True)
                echo("Skipping Pylint checks.",
                     fg='red',
                     bold=True,
                     indent=True)
        else:
            echo("Pylint not found, so extra checks are disabled.",
                 fg='green',
                 indent=True,
                 bold=False)


def print_metaflow_exception(ex):
    echo_always(ex.headline, indent=True, nl=False, bold=True)
    if ex.line_no is None:
        echo_always(':')
    else:
        echo_always(' on line %d:' % ex.line_no, bold=True)
    echo_always(ex.message, indent=True, bold=False, padding_bottom=True)


def print_unknown_exception(ex):
    echo_always('Internal error', indent=True, bold=True)
    echo_always(traceback.format_exc(), highlight=None, highlight_bold=False)


class CliState(object):
    def __init__(self, flow):
        self.flow = flow

def main(flow, args=None, handle_exceptions=True, entrypoint=None):
    # Ignore warning(s) and prevent spamming the end-user.
    # TODO: This serves as a short term workaround for RuntimeWarning(s) thrown
    # in py3.8 related to log buffering (bufsize=1).
    import warnings
    warnings.filterwarnings('ignore')
    if entrypoint is None:
        entrypoint = [sys.executable, sys.argv[0]]

    state = CliState(flow)
    state.entrypoint = entrypoint

    try:
        if args is None:
            start(auto_envvar_prefix='METAFLOW', obj=state)
        else:
            try:
                start.main(args=args,
                           obj=state,
                           auto_envvar_prefix='METAFLOW')
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
