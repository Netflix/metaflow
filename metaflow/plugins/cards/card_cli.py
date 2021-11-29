from metaflow.client import Task
from metaflow import Flow, JSONType, Step
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
    IdNotFoundException,
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


def resolve_card(
    ctx, identifier, card_id=None, hash=None, type=None, index=0, follow_resumed=True
):
    """Resolves the card path based on the arguments provided. We allow identifier to be a pathspec or a id of card.

    Args:
        ctx : click context object
        identifier : id of card or pathspec
        card_id (optional): id of card that can be specified along with the pathspec; If a task has multiple cards then card_id can help narrow down the exact card to be resolved.
        hash (optional): This is to specifically resolve the card via the hash. This is useful when there may be many card with same id or type for a pathspec.
        type : type of card
        index : index of card decorator (Will be useful once we support many @card decorators.)

    Raises:
        IdNotFoundException: the identifier is an ID and metaflow cannot resolve this ID in the datastore.
        CardNotPresentException: No card could be found for the identifier (may it be pathspec or id)

    Returns:
        (card_paths, card_datastore, taskpathspec) : Tuple[List[str],CardDatastore,str]
    """
    is_path_spec = False
    if "/" in identifier:
        is_path_spec = True
        assert (
            len(identifier.split("/")) == 3
        ), "Expecting pathspec of form <runid>/<stepname>/<taskid>"

    flow_name = ctx.obj.flow.name
    pathspec, run_id, step_name, task_id = None, None, None, None
    more_than_one_task = False
    # this means that identifier is a pathspec
    if is_path_spec:
        # what should be the args we expose
        run_id, step_name, task_id = identifier.split("/")
        pathspec = "/".join([flow_name, run_id, step_name, task_id])
        task = Task(pathspec)
    else:  # If identifier is not a pathspec then it may be referencing the latest run
        # this means the identifier is card-id
        # So we first resolve the id to stepname.
        step_name = stepname_from_card_id(identifier, ctx.obj.flow)
        # this means that the `id` doesn't match
        # the one specified in the decorator
        if step_name is None:
            raise IdNotFoundException(identifier)
        run_id = Flow(flow_name).latest_run.id
        step = Step("/".join([flow_name, run_id, step_name]))
        tasks = list(step)
        if len(tasks) == 0:
            raise Exception("No Tasks found for %s/%s" % (step_name, run_id))
        task = tasks[0]
        if len(tasks) > 1:
            more_than_one_task = True
        pathspec = task.pathspec
        task_id = task.id
    # Check if we need to chase origin or not.
    if follow_resumed:
        resume_status = resumed_info(task)
        if resume_status.task_resumed:
            pathspec = resume_status.origin_task_pathspec
            ctx.obj.echo(
                "Resolving card resumed from: %s" % resume_status.origin_task_pathspec,
                fg="green",
            )
        else:
            print_str = "Resolving card: %s" % pathspec
            if more_than_one_task:
                print_str = (
                    "Resolving card of the first task from %d tasks with pathspec %s"
                    % (len(tasks), pathspec)
                )
            ctx.obj.echo(print_str, fg="green")
    else:
        print_str = "Resolving card: %s" % pathspec
        if more_than_one_task:
            print_str = (
                "Resolving card of the first task from %d tasks with pathspec %s"
                % (len(tasks), pathspec)
            )
        ctx.obj.echo(print_str, fg="green")
    # to resolve card_id we first check if the identifier is a pathspec and if it is then we check if the `id` is set or not to resolve card_id
    # todo : Fix this with `coalesce function`
    cid = None
    if is_path_spec and card_id is not None:
        cid = card_id
    elif not is_path_spec:
        cid = identifier
    card_paths_found, card_datastore = resolve_paths_from_task(
        ctx.obj.flow_datastore,
        run_id,
        step_name,
        task_id,
        pathspec=pathspec,
        type=type,
        card_id=cid,
        index=index,
        hash=hash,
    )

    if len(card_paths_found) == 0:
        # If there are no files found on the Path then raise an error of
        raise CardNotPresentException(
            flow_name,
            run_id,
            step_name,
            card_hash=hash,
            card_index=index,
            card_id=card_id,
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


def list_availble_cards(ctx, path_spec, card_paths, card_datastore, command="view"):
    # todo : create nice response messages on the CLI for cards which were found.
    scriptname = ctx.obj.flow.script_name
    path_tuples = card_datastore.get_card_names(card_paths)
    ctx.obj.echo(
        "\nFound %d card matching for your query..." % len(path_tuples), fg="green"
    )
    task_pathspec = "/".join(path_spec.split("/")[1:])
    card_list = []
    for path_tuple in path_tuples:
        card_id, card_type, card_index, card_hash = path_tuple
        card_name = "%s-%s-%s" % (card_type, card_index, card_hash)
        if card_id is not None:
            card_name = "%s-%s-%s-%s" % (card_id, card_type, card_index, card_hash)
        card_list.append(card_name)

    random_idx = 0 if len(path_tuples) == 1 else random.randint(0, len(path_tuples) - 1)
    randid, _, _, randhash = path_tuples[random_idx]
    ctx.obj.echo("\n\t".join([""] + card_list), fg="blue")
    ctx.obj.echo(
        "\n\tExample access from CLI via: \n\t %s\n"
        % make_command(
            scriptname,
            task_pathspec,
            command=command,
            hash=randhash[:NUM_SHORT_HASH_CHARS],
            identifier=randid,
        ),
        fg="yellow",
    )


def make_command(script_name, taskspec, command="get", hash=None, identifier=None):
    calling_args = ["--hash", hash]
    if identifier is not None:
        calling_args = ["--id", identifier]
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
        "--id", default=None, show_default=True, type=str, help="Id of the card"
    )
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
        "--index",
        default=0,
        show_default=True,
        type=int,
        help="Index of the card decorator",
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
    "--id", default=None, show_default=True, type=str, help="Unique ID of the card"
)
@click.option(
    "--index",
    default=0,
    show_default=True,
    type=int,
    help="Index of the card decorator",
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
@click.pass_context
def create(
    ctx,
    pathspec,
    type=None,
    id=None,
    index=None,
    options=None,
    timeout=None,
    component_file=None,
):

    assert (
        len(pathspec.split("/")) == 3
    ), "Expecting pathspec of form <runid>/<stepname>/<taskid>"
    runid, step_name, task_id = pathspec.split("/")
    flowname = ctx.obj.flow.name
    full_pathspec = "/".join([flowname, runid, step_name, task_id])
    graph_dict = serialize_flowgraph(ctx.obj.graph)
    # Components are rendered in a MetaflowStep are added here.
    component_arr = []
    if component_file is not None:
        with open(component_file, "r") as f:
            component_arr = json.load(f)

    task = Task(full_pathspec)
    from metaflow.plugins import CARDS

    filtered_cards = [CardClass for CardClass in CARDS if CardClass.type == type]
    if len(filtered_cards) == 0:
        raise CardClassFoundException(type)

    card_datastore = CardDatastore(
        ctx.obj.flow_datastore, runid, step_name, task_id, path_spec=full_pathspec
    )

    filtered_card = filtered_cards[0]
    ctx.obj.echo(
        "Creating new card of type %s With timeout %s" % (filtered_card.type, timeout),
        fg="green",
    )
    # save card to datastore
    try:
        mf_card = filtered_card(
            options=options, components=component_arr, graph=graph_dict
        )
    except TypeError as e:
        raise IncorrectCardArgsException(type, options)

    try:
        rendered_info = render_card(mf_card, task, timeout_value=timeout)
    except:  # TODO : Catch exec trace over here.
        raise UnrenderableCardException(type, options)
    else:
        if rendered_info is None:
            return
        card_datastore.save_card(type, id, index, rendered_info)


@card.command()
@click.argument("identifier")
@card_read_options_and_arguments
@click.pass_context
def view(
    ctx,
    identifier,
    id=None,
    hash=None,
    type=None,
    index=None,
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
        index=index,
        card_id=id,
        hash=hash,
        follow_resumed=not dont_follow_resumed,
    )
    if len(available_card_paths) == 1:
        open_in_browser(card_datastore.cache_locally(available_card_paths[0]))
    else:
        list_availble_cards(
            ctx, pathspec, available_card_paths, card_datastore, command="view"
        )


@card.command()
@click.argument("identifier")
@card_read_options_and_arguments
@click.pass_context
def get(
    ctx,
    identifier,
    id=None,
    hash=None,
    type=None,
    index=None,
    dont_follow_resumed=False,
):
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        identifier,
        type=type,
        index=index,
        card_id=id,
        hash=hash,
        follow_resumed=not dont_follow_resumed,
    )
    if len(available_card_paths) == 1:
        print(card_datastore.get_card_html(available_card_paths[0]))
    else:
        list_availble_cards(
            ctx, pathspec, available_card_paths, card_datastore, command="get"
        )
