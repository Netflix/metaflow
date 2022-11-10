from metaflow.client import Task
from metaflow import JSONType, namespace
from metaflow.exception import CommandException
import webbrowser
import re
from metaflow._vendor import click
import os
import json
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
    TaskNotFoundException,
)

from .card_resolver import resolve_paths_from_task, resumed_info

id_func = id


def open_in_browser(card_path):
    url = "file://" + os.path.abspath(card_path)
    webbrowser.open(url)


def resolve_task_from_pathspec(flow_name, pathspec):
    """
    resolves a task object for the pathspec query on the CLI.
    Args:
        flow_name : (str) : name of flow
        pathspec (str) : can be `stepname` / `runid/stepname` / `runid/stepname/taskid`

    Returns:
        metaflow.Task | None
    """
    from metaflow import Flow, Step, Task
    from metaflow.exception import MetaflowNotFound

    # since pathspec can have many variations.
    pthsplits = pathspec.split("/")
    task = None
    run_id = None
    resolving_from = "task_pathspec"
    if len(pthsplits) == 1:
        # This means stepname
        resolving_from = "stepname"
        latest_run = Flow(flow_name).latest_run
        if latest_run is not None:
            run_id = latest_run.pathspec
            try:
                task = latest_run[pathspec].task
            except KeyError:
                pass
    elif len(pthsplits) == 2:
        # This means runid/stepname
        namespace(None)
        resolving_from = "step_pathspec"
        try:
            task = Step("/".join([flow_name, pathspec])).task
        except MetaflowNotFound:
            pass
    elif len(pthsplits) == 3:
        # this means runid/stepname/taskid
        namespace(None)
        resolving_from = "task_pathspec"
        try:
            task = Task("/".join([flow_name, pathspec]))
        except MetaflowNotFound:
            pass
    else:
        # raise exception for invalid pathspec format
        raise CommandException(
            msg="The PATHSPEC argument should be of the form 'stepname' Or '<runid>/<stepname>' Or '<runid>/<stepname>/<taskid>'"
        )

    if task is None:
        # raise Exception that task could not be resolved for the query.
        raise TaskNotFoundException(pathspec, resolving_from, run_id=run_id)

    return task


def resolve_card(
    ctx,
    pathspec,
    follow_resumed=True,
    hash=None,
    type=None,
    card_id=None,
    no_echo=False,
):
    """Resolves the card path for a query.

    Args:
        ctx: click context object
        pathspec: pathspec can be `stepname` or `runid/stepname` or `runid/stepname/taskid`
        hash (optional): This is to specifically resolve the card via the hash. This is useful when there may be many card with same id or type for a pathspec.
        type : type of card
        card_id : `id` given to card
        no_echo : if set to `True` then supress logs about pathspec resolution.
    Raises:
        CardNotPresentException: No card could be found for the pathspec

    Returns:
        (card_paths, card_datastore, taskpathspec) : Tuple[List[str], CardDatastore, str]
    """
    flow_name = ctx.obj.flow.name
    task = resolve_task_from_pathspec(flow_name, pathspec)
    card_pathspec = task.pathspec
    print_str = "Resolving card: %s" % card_pathspec
    if follow_resumed:
        origin_taskpathspec = resumed_info(task)
        if origin_taskpathspec:
            card_pathspec = origin_taskpathspec
            print_str = "Resolving card resumed from: %s" % origin_taskpathspec

    if not no_echo:
        ctx.obj.echo(print_str, fg="green")
    # to resolve card_id we first check if the identifier is a pathspec and if it is then we check if the `id` is set or not to resolve card_id
    # todo : Fix this with `coalesce function`
    card_paths_found, card_datastore = resolve_paths_from_task(
        ctx.obj.flow_datastore,
        pathspec=card_pathspec,
        type=type,
        hash=hash,
        card_id=card_id,
    )

    if len(card_paths_found) == 0:
        # If there are no files found on the Path then raise an error of
        raise CardNotPresentException(
            card_pathspec, card_hash=hash, card_type=type, card_id=card_id
        )

    return card_paths_found, card_datastore, card_pathspec


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
        # Unregister the signal so that it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def raise_timeout(signum, frame):
    raise TimeoutError


def list_available_cards(
    ctx,
    pathspec,
    card_paths,
    card_datastore,
    command="view",
    show_list_as_json=False,
    list_many=False,
    file=None,
):
    # pathspec is full pathspec.
    # todo : create nice response messages on the CLI for cards which were found.
    scriptname = ctx.obj.flow.script_name
    path_tuples = card_datastore.get_card_names(card_paths)
    if show_list_as_json:
        json_arr = [
            dict(id=tup.id, hash=tup.hash, type=tup.type, filename=tup.filename)
            for tup in path_tuples
        ]
        if not list_many:
            # This means that `list_available_cards` is being called once.
            # So we can directly dump the file
            dump_dict = dict(pathspec=pathspec, cards=json_arr)
            if file:
                with open(file, "w") as f:
                    json.dump(dump_dict, f)
            else:
                ctx.obj.echo_always(json.dumps(dump_dict, indent=4), err=False)
        # if you have to list many in json format then return
        return dict(pathspec=pathspec, cards=json_arr)

    if list_many:
        ctx.obj.echo("\tTask: %s" % pathspec.split("/")[-1], fg="green")
    else:
        ctx.obj.echo(
            "Found %d card matching for your query..." % len(path_tuples), fg="green"
        )
    task_pathspec = "/".join(pathspec.split("/")[1:])
    card_list = []
    for path_tuple, file_path in zip(path_tuples, card_paths):
        full_pth = card_datastore.create_full_path(file_path)
        cpr = """
        Card Id: %s
        Card Type: %s
        Card Hash: %s 
        Card Path: %s
        """ % (
            path_tuple.id,
            path_tuple.type,
            path_tuple.hash,
            full_pth,
        )
        card_list.append(cpr)

    random_idx = 0 if len(path_tuples) == 1 else random.randint(0, len(path_tuples) - 1)
    _, randhash, _, file_name = path_tuples[random_idx]
    join_char = "\n\t"
    ctx.obj.echo(join_char.join([""] + card_list) + "\n", fg="blue")

    if command is not None:
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


def list_many_cards(
    ctx,
    type=None,
    hash=None,
    card_id=None,
    follow_resumed=None,
    as_json=None,
    file=None,
):
    from metaflow import Flow

    flow = Flow(ctx.obj.flow.name)
    run = flow.latest_run
    cards_found = 0
    if not as_json:
        pass
        ctx.obj.echo("Listing cards for run %s" % run.pathspec, fg="green")
    js_list = []
    for step in run:
        step_str_printed = False  # variable to control printing stepname once.
        for task in step:
            try:
                available_card_paths, card_datastore, pathspec = resolve_card(
                    ctx,
                    "/".join(task.pathspec.split("/")[1:]),
                    type=type,
                    hash=hash,
                    card_id=card_id,
                    follow_resumed=follow_resumed,
                    no_echo=True,
                )
                if not step_str_printed and not as_json:
                    ctx.obj.echo("Step : %s" % step.id, fg="green")
                    step_str_printed = True

                js_resp = list_available_cards(
                    ctx,
                    pathspec,
                    available_card_paths,
                    card_datastore,
                    command=None,
                    show_list_as_json=as_json,
                    list_many=True,
                    file=file,
                )
                if as_json:
                    js_list.append(js_resp)
                cards_found += 1
            except CardNotPresentException:
                pass
    if cards_found == 0:
        raise CardNotPresentException(
            run.pathspec, card_hash=hash, card_type=type, card_id=card_id
        )
    if as_json:
        if file:
            with open(file, "w") as f:
                json.dump(js_list, f)
        else:
            ctx.obj.echo_always(json.dumps(js_list, indent=4), err=False)


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
        help="Type of card",
    )
    @click.option(
        "--id",
        default=None,
        show_default=True,
        type=str,
        help="Id of the card",
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
    default=None,
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
@click.option(
    "--component-file",
    default=None,
    show_default=True,
    type=str,
    help="JSON File with Pre-rendered components.(internal)",
)
@click.option(
    "--id",
    default=None,
    show_default=True,
    type=str,
    help="ID of the card",
)
@click.pass_context
def create(
    ctx,
    pathspec,
    type=None,
    options=None,
    timeout=None,
    component_file=None,
    render_error_card=False,
    id=None,
):
    card_id = id
    rendered_info = None  # Variable holding all the information which will be rendered
    error_stack_trace = None  # Variable which will keep a track of error

    if len(pathspec.split("/")) != 3:
        raise CommandException(
            msg="Expecting pathspec of form <runid>/<stepname>/<taskid>"
        )
    flowname = ctx.obj.flow.name
    full_pathspec = "/".join([flowname, pathspec])

    graph_dict, _ = ctx.obj.graph.output_steps()

    # Components are rendered in a Step and added via `current.card.append` are added here.
    component_arr = []
    if component_file is not None:
        with open(component_file, "r") as f:
            component_arr = json.load(f)

    task = Task(full_pathspec)
    from metaflow.plugins import CARDS
    from metaflow.plugins.cards.exception import CARD_ID_PATTERN, TYPE_CHECK_REGEX
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
        # If the card is Instantiatable then
        # first instantiate; If instantiation has a TypeError
        # then check for render_error_card and accordingly
        # store the exception as a string or raise the exception
        try:
            if options is not None:
                mf_card = filtered_card(
                    options=options, components=component_arr, graph=graph_dict
                )
            else:
                mf_card = filtered_card(components=component_arr, graph=graph_dict)
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

    # If card_id is doesn't match regex pattern then we will set it as None
    if card_id is not None and re.match(CARD_ID_PATTERN, card_id) is None:
        ctx.obj.echo(
            "`--id=%s` doesn't match REGEX pattern. `--id` will be set to `None`. Please create `--id` of pattern %s."
            % (card_id, TYPE_CHECK_REGEX),
            fg="red",
        )
        card_id = None

    if rendered_info is not None:
        card_info = card_datastore.save_card(save_type, rendered_info, card_id=card_id)
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
    id=None,
    follow_resumed=False,
):
    """
    View the HTML card in browser based on the pathspec.\n
    The pathspec can be of the form:\n
        - <stepname>\n
        - <runid>/<stepname>\n
        - <runid>/<stepname>/<taskid>\n
    """
    card_id = id
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        pathspec,
        type=type,
        hash=hash,
        card_id=card_id,
        follow_resumed=follow_resumed,
    )
    if len(available_card_paths) == 1:
        open_in_browser(card_datastore.cache_locally(available_card_paths[0]))
    else:
        list_available_cards(
            ctx,
            pathspec,
            available_card_paths,
            card_datastore,
            command="view",
        )


@card.command()
@click.argument("pathspec")
@click.argument("path", required=False)
@card_read_options_and_arguments
@click.pass_context
def get(
    ctx,
    pathspec,
    path,
    hash=None,
    type=None,
    id=None,
    follow_resumed=False,
):
    """
    Get the HTML string of the card based on pathspec.\n
    The pathspec can be of the form:\n
        - <stepname>\n
        - <runid>/<stepname>\n
        - <runid>/<stepname>/<taskid>\n

    Save the card by adding the `path` argument.
    ```
    python myflow.py card get start a.html --type default
    ```
    """
    card_id = id
    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        pathspec,
        type=type,
        hash=hash,
        card_id=card_id,
        follow_resumed=follow_resumed,
    )
    if len(available_card_paths) == 1:
        if path is not None:
            card_datastore.cache_locally(available_card_paths[0], path)
            return
        print(card_datastore.get_card_html(available_card_paths[0]))
    else:
        list_available_cards(
            ctx,
            pathspec,
            available_card_paths,
            card_datastore,
            command="get",
        )


@card.command()
@click.argument("pathspec", required=False)
@card_read_options_and_arguments
@click.option(
    "--as-json",
    default=False,
    is_flag=True,
    help="Print all available cards as a JSON object",
)
@click.option(
    "--file",
    default=None,
    help="Save the available card list to file.",
)
@click.pass_context
def list(
    ctx,
    pathspec=None,
    hash=None,
    type=None,
    id=None,
    follow_resumed=False,
    as_json=False,
    file=None,
):

    card_id = id
    if pathspec is None:
        list_many_cards(
            ctx,
            type=type,
            hash=hash,
            card_id=card_id,
            follow_resumed=follow_resumed,
            as_json=as_json,
            file=file,
        )
        return

    available_card_paths, card_datastore, pathspec = resolve_card(
        ctx,
        pathspec,
        type=type,
        hash=hash,
        card_id=card_id,
        follow_resumed=follow_resumed,
        no_echo=as_json,
    )
    list_available_cards(
        ctx,
        pathspec,
        available_card_paths,
        card_datastore,
        command=None,
        show_list_as_json=as_json,
        file=file,
    )
