from metaflow.client import Task
from metaflow import Flow, JSONType, Step
from metaflow.exception import CommandException
import webbrowser
import json
import click
import os
import signal
import sys
import random
from contextlib import contextmanager
from functools import wraps
from metaflow.exception import MetaflowNotFound
from .card_datastore import CardDatastore, stepname_from_card_id, NUM_SHORT_HASH_CHARS
from .exception import (
    CardClassFoundException,
    IncorrectCardArgsException,
    UnrenderableCardException,
    CardNotPresentException,
    TypeRequiredException,
)

from .card_resolver import resolve_paths_from_task, resumed_info


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


def resolve_card(ctx, pathspec, hash=None, type=None, follow_resumed=True):
    """Resolves the card path based on the arguments provided. We allow identifier to be a pathspec or a id of card.

    Args:
        ctx : click context object
        pathspec : pathspec
        hash (optional): This is to specifically resolve the card via the hash. This is useful when there may be many card with same id or type for a pathspec.
        type : type of card
    Raises:
        CardNotPresentException: No card could be found for the pathspec

    Returns:
        (card_paths, card_datastore, taskpathspec) : Tuple[List[str],CardDatastore,str]
    """
    if len(pathspec.split("/")) != 3:
        raise CommandException(
            msg="Expecting pathspec of form <runid>/<stepname>/<taskid>"
        )

    flow_name = ctx.obj.flow.name
    run_id, step_name, task_id = None, None, None
    more_than_one_task = False
    # what should be the args we expose
    run_id, step_name, task_id = pathspec.split("/")
    pathspec = "/".join([flow_name, run_id, step_name, task_id])
    task = Task(pathspec)
    print_str = "Resolving card: %s" % pathspec
    if follow_resumed:
        resume_status = resumed_info(task)
        if resume_status.task_resumed:
            pathspec = resume_status.origin_task_pathspec
            ctx.obj.echo(
                "Resolving card resumed from: %s" % resume_status.origin_task_pathspec,
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
        run_id,
        step_name,
        task_id,
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
def card():
    pass


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
        "--dont-follow-resumed",
        default=False,
        is_flag=True,
        help="Doesn't follow the origin-task-id of resumed tasks to seek cards stored for resumed tasks.",
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


# Finished According to the Memo
@card.command(help="create the HTML card")
@click.argument("pathspec", type=str)
@click.option(
    "--type",
    default=None,
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
    "--component-file",
    default=None,
    show_default=True,
    type=str,
    help="JSON File with Pre-rendered components.(internal)",
)
@click.option(
    "--with-error-card",
    default=False,
    is_flag=True,
    help="Upon failing to render a card render a card holding the stack trace",
)
@click.pass_context
def create(
    ctx,
    pathspec,
    type=None,
    options=None,
    timeout=None,
    component_file=None,
    with_error_card=False,
):

    rendered_info = None  # Variable holding all the information which will be rendered
    error_stack_trace = None  # Variable which will keep a track if there was an error

    if len(pathspec.split("/")) != 3:
        raise CommandException(
            msg="Expecting pathspec of form <runid>/<stepname>/<taskid>"
        )
    runid, step_name, task_id = pathspec.split("/")
    flowname = ctx.obj.flow.name
    full_pathspec = "/".join([flowname, runid, step_name, task_id])

    # todo : Import the changes from Netflix/metaflow#833 for Graph
    graph_dict = serialize_flowgraph(ctx.obj.graph)

    # Components are rendered in a Step and added via `current.card.append` are added here.
    component_arr = []
    if component_file is not None:
        with open(component_file, "r") as f:
            component_arr = json.load(f)

    task = Task(full_pathspec)
    from metaflow.plugins import CARDS
    from metaflow.cards import ErrorCard

    error_card = ErrorCard
    filtered_cards = [CardClass for CardClass in CARDS if CardClass.type == type]
    card_datastore = CardDatastore(
        ctx.obj.flow_datastore, runid, step_name, task_id, path_spec=full_pathspec
    )

    if len(filtered_cards) == 0:
        if with_error_card:
            error_stack_trace = str(CardClassFoundException(type))
        else:
            raise CardClassFoundException(type)

    if len(filtered_cards) > 0:
        filtered_card = filtered_cards[0]
        ctx.obj.echo(
            "Creating new card of type %s With timeout %s"
            % (filtered_card.type, timeout),
            fg="green",
        )
        # If the card is Instatiatable then
        # first instantiate; If instantiation has a TypeError
        # then check for with_error_card and accordingly
        # store the exception as a string or raise the exception
        try:
            mf_card = filtered_card(
                options=options, components=component_arr, graph=graph_dict
            )
        except TypeError as e:
            if with_error_card:
                mf_card = None
                error_stack_trace = str(IncorrectCardArgsException(type, options))
            else:
                raise IncorrectCardArgsException(type, options)

        if mf_card:
            try:
                rendered_info = render_card(mf_card, task, timeout_value=timeout)
            except:
                if with_error_card:
                    error_stack_trace = str(UnrenderableCardException(type, options))
                else:
                    raise UnrenderableCardException(type, options)
        #

    if error_stack_trace is not None:
        rendered_info = error_card().render(task, stack_trace=error_stack_trace)

    if rendered_info is None:
        rendered_info = error_card().render(
            task, stack_trace="No information rendered From card of type %s" % type
        )

    # todo : should we save native type for error card or error type ?
    save_type = type  # if error_stack_trace is not None else 'error'
    card_datastore.save_card(save_type, rendered_info)


@card.command()
@click.argument("identifier")
@card_read_options_and_arguments
@click.pass_context
def view(
    ctx,
    identifier,
    hash=None,
    type=None,
    dont_follow_resumed=False,
):
    """
    View the HTML card in browser based on the IDENTIFIER.\n
    The IDENTIFIER can be:\n
        - run path spec : <runid>/<stepname>/<taskid>\n
                    OR\n
        - id given in the @card\n
    """
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        identifier,
        type=type,
        hash=hash,
        follow_resumed=not dont_follow_resumed,
    )
    if len(available_card_paths) == 1:
        open_in_browser(card_datastore.cache_locally(available_card_paths[0]))
    else:
        list_available_cards(
            ctx, pathspec, available_card_paths, card_datastore, command="view"
        )


@card.command()
@click.argument("identifier")
@card_read_options_and_arguments
@click.pass_context
def get(
    ctx,
    identifier,
    hash=None,
    type=None,
    dont_follow_resumed=False,
):
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        identifier,
        type=type,
        hash=hash,
        follow_resumed=not dont_follow_resumed,
    )
    if len(available_card_paths) == 1:
        print(card_datastore.get_card_html(available_card_paths[0]))
    else:
        list_available_cards(
            ctx, pathspec, available_card_paths, card_datastore, command="get"
        )
