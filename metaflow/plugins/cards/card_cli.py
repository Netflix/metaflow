from metaflow.client import Task
from metaflow import JSONType, namespace
from metaflow.exception import CommandException
import webbrowser
import re
import click
import os
import signal
import random
from contextlib import contextmanager
from functools import wraps
from metaflow.exception import MetaflowNamespaceMismatch
from .card_datastore import CardDatastore, NUM_SHORT_HASH_CHARS
from .exception import (
    CardClassFoundException,
    IncorrectCardArgsException,
    UnrenderableCardException,
    CardNotPresentException,
)

from .card_resolver import resolve_paths_from_task, resumed_info

# FIXME :  Import the changes from Netflix/metaflow#833 for Graph
def serialize_flowgraph(flowgraph):
    graph_dict = {}
    for node in flowgraph:
        graph_dict[node.name] = {
            "type": node.type,
            "box_next": node.type not in ("linear", "join"),
            "box_ends": node.matching_join,
            "next": node.out_funcs,
            "doc": node.doc,
        }
    return graph_dict


def open_in_browser(card_path):
    url = "file://" + os.path.abspath(card_path)
    webbrowser.open(url)


def resolve_card(
    ctx,
    pathspec,
    follow_resumed=True,
    hash=None,
    type=None,
):
    """Resolves the card path based on the arguments provided. We allow identifier to be a pathspec or a id of card.

    Args:
        ctx: click context object
        pathspec: pathspec
        hash (optional): This is to specifically resolve the card via the hash. This is useful when there may be many card with same id or type for a pathspec.
        type : type of card
    Raises:
        CardNotPresentException: No card could be found for the pathspec

    Returns:
        (card_paths, card_datastore, taskpathspec) : Tuple[List[str], CardDatastore, str]
    """
    if len(pathspec.split("/")) != 3:
        raise CommandException(
            msg="Expecting pathspec of form <runid>/<stepname>/<taskid>"
        )

    flow_name = ctx.obj.flow.name
    run_id, step_name, task_id = None, None, None
    # what should be the args we expose
    run_id, step_name, task_id = pathspec.split("/")
    pathspec = "/".join([flow_name, pathspec])
    # we set namespace to be none to avoid namespace mismatch error.
    namespace(None)
    task = Task(pathspec)
    print_str = "Resolving card: %s" % pathspec
    if follow_resumed:
        origin_taskpathspec = resumed_info(task)
        if origin_taskpathspec:
            pathspec = origin_taskpathspec
            ctx.obj.echo(
                "Resolving card resumed from: %s" % origin_taskpathspec,
                fg="green",
            )
        else:
            ctx.obj.echo(print_str, fg="green")
    else:
        ctx.obj.echo(print_str, fg="green")
    # to resolve card_id we first check if the identifier is a pathspec and if it is then we check if the `id` is set or not to resolve card_id
    # todo : Fix this with `coalesce function`
    card_paths_found, card_datastore = resolve_paths_from_task(
        ctx.obj.flow_datastore,
        pathspec=pathspec,
        type=type,
        hash=hash,
    )

    if len(card_paths_found) == 0:
        # If there are no files found on the Path then raise an error of
        raise CardNotPresentException(
            flow_name,
            run_id,
            step_name,
            card_hash=hash,
            card_type=type,
        )

    return card_paths_found, card_datastore, pathspec


@contextmanager
def timeout(time):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after ``time``.
    signal.alarm(time)

    try:
        yield
    except TimeoutError:
        pass
    finally:
        # Unregister the signal so it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def raise_timeout(signum, frame):
    raise TimeoutError


def list_available_cards(ctx, path_spec, card_paths, card_datastore, command="view"):
    # todo : create nice response messages on the CLI for cards which were found.
    scriptname = ctx.obj.flow.script_name
    path_tuples = card_datastore.get_card_names(card_paths)
    ctx.obj.echo(
        "\nFound %d card matching for your query..." % len(path_tuples), fg="green"
    )
    task_pathspec = "/".join(path_spec.split("/")[1:])
    card_list = []
    for path_tuple in path_tuples:
        card_name = "%s-%s" % (path_tuple.type, path_tuple.hash)
        card_list.append(card_name)

    random_idx = 0 if len(path_tuples) == 1 else random.randint(0, len(path_tuples) - 1)
    _, randhash = path_tuples[random_idx]
    ctx.obj.echo("\n\t".join([""] + card_list), fg="blue")
    ctx.obj.echo(
        "\n\tExample access from CLI via: \n\t %s\n"
        % make_command(
            scriptname,
            task_pathspec,
            command=command,
            hash=randhash[:NUM_SHORT_HASH_CHARS],
        ),
        fg="yellow",
    )


def make_command(
    script_name,
    taskspec,
    command="get",
    hash=None,
):
    calling_args = ["--hash", hash]
    return " ".join(
        [
            ">>>",
            "python",
            script_name,
            "card",
            command,
            taskspec,
        ]
        + calling_args
    )


@click.group()
def cli():
    pass


@cli.group(help="Commands related to @card decorator.")
@click.pass_context
def card(ctx):
    # We set the metadata values here so that top level arguments to --datastore and --metadata
    # Can work with the Metaflow client.
    # If we don't set the metadata here than the metaflow client picks the defaults when calling the `Task`/`Run` objects. These defaults can come from the `config.json` file or based on the `METAFLOW_PROFILE`
    from metaflow import metadata

    setting_metadata = "@".join(
        [ctx.obj.metadata.TYPE, ctx.obj.metadata.default_info()]
    )
    metadata(setting_metadata)
    # set the card root to the datastore according to the configuration.
    root_pth = CardDatastore.get_storage_root(ctx.obj.flow_datastore._storage_impl.TYPE)
    if root_pth is not None:
        ctx.obj.flow_datastore._storage_impl.datastore_root = root_pth


def card_read_options_and_arguments(func):
    @click.option(
        "--hash",
        default=None,
        show_default=True,
        type=str,
        help="Hash of the stored HTML",
    )
    @click.option(
        "--type",
        default=None,
        show_default=True,
        type=str,
        help="Type of card being created",
    )
    @click.option(
        "--follow-resumed/--no-follow-resumed",
        default=True,
        show_default=True,
        help="Follow the origin-task-id of resumed tasks to seek cards stored for resumed tasks.",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def render_card(mf_card, task, timeout_value=None):
    rendered_info = None
    if timeout_value is None or timeout_value < 0:
        rendered_info = mf_card.render(task)
    else:
        with timeout(timeout_value):
            rendered_info = mf_card.render(task)
    return rendered_info


@card.command(help="create a HTML card")
@click.argument("pathspec", type=str)
@click.option(
    "--type",
    default="default",
    show_default=True,
    type=str,
    help="Type of card being created",
)
@click.option(
    "--options",
    default={},
    show_default=True,
    type=JSONType,
    help="arguments of the card being created.",
)
@click.option(
    "--timeout",
    default=None,
    show_default=True,
    type=int,
    help="Maximum amount of time allowed to create card.",
)
@click.option(
    "--render-error-card",
    default=False,
    is_flag=True,
    help="Upon failing to render a card, render a card holding the stack trace",
)
@click.pass_context
def create(
    ctx,
    pathspec,
    type=None,
    options=None,
    timeout=None,
    render_error_card=False,
):

    rendered_info = None  # Variable holding all the information which will be rendered
    error_stack_trace = None  # Variable which will keep a track of error

    if len(pathspec.split("/")) != 3:
        raise CommandException(
            msg="Expecting pathspec of form <runid>/<stepname>/<taskid>"
        )
    flowname = ctx.obj.flow.name
    full_pathspec = "/".join([flowname, pathspec])

    # todo : Import the changes from Netflix/metaflow#833 for Graph
    graph_dict = serialize_flowgraph(ctx.obj.graph)

    task = Task(full_pathspec)
    from metaflow.plugins import CARDS
    from metaflow.plugins.cards.exception import CARD_ID_PATTERN
    from metaflow.cards import ErrorCard

    error_card = ErrorCard
    filtered_cards = [CardClass for CardClass in CARDS if CardClass.type == type]
    card_datastore = CardDatastore(ctx.obj.flow_datastore, pathspec=full_pathspec)

    if len(filtered_cards) == 0 or type is None:
        if render_error_card:
            error_stack_trace = str(CardClassFoundException(type))
        else:
            raise CardClassFoundException(type)

    if len(filtered_cards) > 0:
        filtered_card = filtered_cards[0]
        ctx.obj.echo(
            "Creating new card of type %s with timeout %s"
            % (filtered_card.type, timeout),
            fg="green",
        )
        # If the card is Instatiatable then
        # first instantiate; If instantiation has a TypeError
        # then check for render_error_card and accordingly
        # store the exception as a string or raise the exception
        try:
            mf_card = filtered_card(options=options, components=[], graph=graph_dict)
        except TypeError as e:
            if render_error_card:
                mf_card = None
                error_stack_trace = str(IncorrectCardArgsException(type, options))
            else:
                raise IncorrectCardArgsException(type, options)

        if mf_card:
            try:
                rendered_info = render_card(mf_card, task, timeout_value=timeout)
            except:
                if render_error_card:
                    error_stack_trace = str(UnrenderableCardException(type, options))
                else:
                    raise UnrenderableCardException(type, options)
        #

    if error_stack_trace is not None:
        rendered_info = error_card().render(task, stack_trace=error_stack_trace)

    if rendered_info is None and render_error_card:
        rendered_info = error_card().render(
            task, stack_trace="No information rendered From card of type %s" % type
        )

    # todo : should we save native type for error card or error type ?
    if type is not None and re.match(CARD_ID_PATTERN, type) is not None:
        save_type = type
    else:
        save_type = "error"

    if rendered_info is not None:
        card_info = card_datastore.save_card(save_type, rendered_info)
        ctx.obj.echo(
            "Card created with type: %s and hash: %s"
            % (card_info.type, card_info.hash[:NUM_SHORT_HASH_CHARS]),
            fg="green",
        )


@card.command()
@click.argument("pathspec")
@card_read_options_and_arguments
@click.pass_context
def view(
    ctx,
    pathspec,
    hash=None,
    type=None,
    follow_resumed=False,
):
    """
    View the HTML card in browser based on the pathspec.\n
    The Task pathspec is of the form:\n
        <runid>/<stepname>/<taskid>\n
    """
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        pathspec,
        type=type,
        hash=hash,
        follow_resumed=follow_resumed,
    )
    if len(available_card_paths) == 1:
        open_in_browser(card_datastore.cache_locally(available_card_paths[0]))
    else:
        list_available_cards(
            ctx, pathspec, available_card_paths, card_datastore, command="view"
        )


@card.command()
@click.argument("pathspec")
@card_read_options_and_arguments
@click.pass_context
def get(
    ctx,
    pathspec,
    hash=None,
    type=None,
    follow_resumed=False,
):
    """
    Get the HTML string of the card based on pathspec.\n
    The Task pathspec is of the form:\n
        <runid>/<stepname>/<taskid>\n
    """
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        pathspec,
        type=type,
        hash=hash,
        follow_resumed=follow_resumed,
    )
    if len(available_card_paths) == 1:
        print(card_datastore.get_card_html(available_card_paths[0]))
    else:
        list_available_cards(
            ctx, pathspec, available_card_paths, card_datastore, command="get"
        )
