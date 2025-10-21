from metaflow.client import Task
from metaflow.parameters import JSONTypeClass
from metaflow import namespace
from metaflow.util import resolve_identity
from metaflow.exception import (
    CommandException,
    MetaflowNotFound,
    MetaflowNamespaceMismatch,
)
import webbrowser
import re
from metaflow._vendor import click
import os
import time
import json
import uuid
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
import traceback
from collections import namedtuple
from .metadata import _save_metadata
from .card_resolver import resolve_paths_from_task, resumed_info

id_func = id

CardRenderInfo = namedtuple(
    "CardRenderInfo",
    ["mode", "is_implemented", "data", "timed_out", "timeout_stack_trace"],
)


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


def _extract_reload_token(data, task, mf_card):
    if "render_seq" not in data:
        return "never"

    if data["render_seq"] == "final":
        # final data update should always trigger a card reload to show
        # the final card, hence a different token for the final update
        return "final"
    elif mf_card.RELOAD_POLICY == mf_card.RELOAD_POLICY_ALWAYS:
        return "render-seq-%s" % data["render_seq"]
    elif mf_card.RELOAD_POLICY == mf_card.RELOAD_POLICY_NEVER:
        return "never"
    elif mf_card.RELOAD_POLICY == mf_card.RELOAD_POLICY_ONCHANGE:
        return mf_card.reload_content_token(task, data)


def update_card(mf_card, mode, task, data, timeout_value=None):
    """
    This method will be responsible for creating a card/data-update based on the `mode`.
    There are three possible modes taken by this function.
        - render :
            - This will render the "final" card.
            - This mode is passed at task completion.
            - Setting this mode will call the `render` method of a MetaflowCard.
            - It will result in the creation of an HTML page.
        - render_runtime:
            - Setting this mode will render a card during task "runtime".
            - Setting this mode will call the `render_runtime` method of a MetaflowCard.
            - It will result in the creation of an HTML page.
        - refresh:
            - Setting this mode will refresh the data update for a card.
            - We support this mode because rendering a full card can be an expensive operation, but shipping tiny data updates can be cheap.
            - Setting this mode will call the `refresh` method of a MetaflowCard.
            - It will result in the creation of a JSON object.

    Parameters
    ----------
    mf_card : MetaflowCard
        MetaflowCard object which will be used to render the card.
    mode : str
        Mode of rendering the card.
    task : Task
        Task object which will be passed to render the card.
    data : dict
        object created and passed down from `current.card._get_latest_data` method.
        For more information on this object's schema have a look at `current.card._get_latest_data` method.
    timeout_value : int
        Timeout value for rendering the card.

    Returns
    -------
    CardRenderInfo
        - NamedTuple which will contain:
            - `mode`: The mode of rendering the card.
            - `is_implemented`: whether the function was implemented or not.
            - `data` : output from rendering the card (Can be string/dict)
            - `timed_out` : whether the function timed out or not.
            - `timeout_stack_trace` : stack trace of the function if it timed out.
    """

    def _add_token_html(html):
        if html is None:
            return None
        return html.replace(
            mf_card.RELOAD_POLICY_TOKEN,
            _extract_reload_token(data=data, task=task, mf_card=mf_card),
        )

    def _add_token_json(json_msg):
        if json_msg is None:
            return None
        return {
            "reload_token": _extract_reload_token(
                data=data, task=task, mf_card=mf_card
            ),
            "data": json_msg,
            "created_on": time.time(),
        }

    def _safe_call_function(func, *args, **kwargs):
        """
        returns (data, is_implemented)
        """
        try:
            return func(*args, **kwargs), True
        except NotImplementedError as e:
            return None, False

    def _call():
        if mode == "render":
            setattr(
                mf_card.__class__,
                "runtime_data",
                property(fget=lambda _, data=data: data),
            )
            output = _add_token_html(mf_card.render(task))
            return CardRenderInfo(
                mode=mode,
                is_implemented=True,
                data=output,
                timed_out=False,
                timeout_stack_trace=None,
            )

        elif mode == "render_runtime":
            # Since many cards created by metaflow users may not have implemented a
            # `render_time` / `refresh` methods, it can result in an exception and thereby
            # creation of error cards (especially for the `render_runtime` method). So instead
            # we will catch the NotImplementedError and return None if users have not implemented it.
            # If there any any other exception from the user code, it should be bubbled to the top level.
            output, is_implemented = _safe_call_function(
                mf_card.render_runtime, task, data
            )
            return CardRenderInfo(
                mode=mode,
                is_implemented=is_implemented,
                data=_add_token_html(output),
                timed_out=False,
                timeout_stack_trace=None,
            )

        elif mode == "refresh":
            output, is_implemented = _safe_call_function(mf_card.refresh, task, data)
            return CardRenderInfo(
                mode=mode,
                is_implemented=is_implemented,
                data=_add_token_json(output),
                timed_out=False,
                timeout_stack_trace=None,
            )

    render_info = None
    if timeout_value is None or timeout_value < 0:
        return _call()
    else:
        try:
            with timeout(timeout_value):
                render_info = _call()
        except TimeoutError:
            stack_trace = traceback.format_exc()
            return CardRenderInfo(
                mode=mode,
                is_implemented=True,
                data=None,
                timed_out=True,
                timeout_stack_trace=stack_trace,
            )
        return render_info


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
    type=JSONTypeClass(),
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
    "--id",
    default=None,
    show_default=True,
    type=str,
    help="ID of the card",
)
@click.option(
    "--component-file",
    default=None,
    show_default=True,
    type=str,
    help="JSON File with pre-rendered components. (internal)",
)
@click.option(
    "--mode",
    default="render",
    show_default=True,
    type=click.Choice(["render", "render_runtime", "refresh"]),
    help="Rendering mode. (internal)",
)
@click.option(
    "--data-file",
    default=None,
    show_default=True,
    type=str,
    hidden=True,
    help="JSON file containing data to be updated. (internal)",
)
@click.option(
    "--card-uuid",
    default=None,
    show_default=True,
    type=str,
    hidden=True,
    help="Card UUID. (internal)",
)
@click.option(
    "--delete-input-files",
    default=False,
    is_flag=True,
    show_default=True,
    hidden=True,
    help="Delete data-file and component-file after reading. (internal)",
)
@click.option(
    "--save-metadata",
    default=None,
    show_default=True,
    type=JSONTypeClass(),
    hidden=True,
    help="JSON string containing metadata to be saved. (internal)",
)
@click.pass_context
def create(
    ctx,
    pathspec,
    mode=None,
    type=None,
    options=None,
    timeout=None,
    component_file=None,
    data_file=None,
    render_error_card=False,
    card_uuid=None,
    delete_input_files=None,
    id=None,
    save_metadata=None,
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

    if card_uuid is None:
        card_uuid = str(uuid.uuid4()).replace("-", "")

    # Components are rendered in a Step and added via `current.card.append` are added here.
    component_arr = []
    if component_file is not None:
        with open(component_file, "r") as f:
            component_arr = json.load(f)
        # Component data used in card runtime is passed in as temporary files which can be deleted after use
        if delete_input_files:
            os.remove(component_file)

    # Load data to be refreshed for runtime cards
    data = {}
    if data_file is not None:
        with open(data_file, "r") as f:
            data = json.load(f)
        # data is passed in as temporary files which can be deleted after use
        if delete_input_files:
            os.remove(data_file)

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
                    options=options,
                    components=component_arr,
                    graph=graph_dict,
                    flow=ctx.obj.flow,
                )
            else:
                mf_card = filtered_card(
                    components=component_arr, graph=graph_dict, flow=ctx.obj.flow
                )
        except TypeError as e:
            if render_error_card:
                mf_card = None
                error_stack_trace = str(IncorrectCardArgsException(type, options))
            else:
                raise IncorrectCardArgsException(type, options)

        rendered_content = None
        if mf_card:
            try:
                rendered_info = update_card(
                    mf_card, mode, task, data, timeout_value=timeout
                )
                rendered_content = rendered_info.data
            except:
                rendered_info = CardRenderInfo(
                    mode=mode,
                    is_implemented=True,
                    data=None,
                    timed_out=False,
                    timeout_stack_trace=None,
                )
                if render_error_card:
                    error_stack_trace = str(UnrenderableCardException(type, options))
                else:
                    raise UnrenderableCardException(type, options)

    # In the entire card rendering process, there are a few cases we want to handle:
    # - [mode == "render"]
    #   1. Card is rendered successfully (We store it in the datastore as a HTML file)
    #   2. Card is not rendered successfully and we have --save-error-card flag set to True
    #      (We store it in the datastore as a HTML file with stack trace)
    #   3. Card render timed-out and we have --save-error-card flag set to True
    #      (We store it in the datastore as a HTML file with stack trace)
    #   4. `render` returns nothing and we have --save-error-card flag set to True.
    #       (We store it in the datastore as a HTML file with some message saying you returned nothing)
    # - [mode == "render_runtime"]
    #   1. Card is rendered successfully (We store it in the datastore as a HTML file)
    #   2. `render_runtime` is not implemented but gets called and we have --save-error-card flag set to True.
    #       (We store it in the datastore as a HTML file with some message saying the card should not be a runtime card if this method is not Implemented)
    #   3. `render_runtime` is implemented and raises an exception and we have --save-error-card flag set to True.
    #       (We store it in the datastore as a HTML file with stack trace)
    #   4. `render_runtime` is implemented but returns nothing and we have --save-error-card flag set to True.
    #       (We store it in the datastore as a HTML file with some message saying you returned nothing)
    #   5. `render_runtime` is implemented but times out and we have --save-error-card flag set to True.
    #       (We store it in the datastore as a HTML file with stack trace)
    # - [mode == "refresh"]
    #   1. Data update is created successfully (We store it in the datastore as a JSON file)
    #   2. `refresh` is not implemented. (We do nothing. Don't store anything.)
    #   3. `refresh` is implemented but it raises an exception. (We do nothing. Don't store anything.)
    #   4. `refresh` is implemented but it times out. (We do nothing. Don't store anything.)

    def _render_error_card(stack_trace):
        _card = error_card()
        token = _extract_reload_token(data, task, _card)
        return _card.render(
            task,
            stack_trace=stack_trace,
        ).replace(_card.RELOAD_POLICY_TOKEN, token)

    if error_stack_trace is not None and mode != "refresh":
        rendered_content = _render_error_card(error_stack_trace)
    elif (
        rendered_info.is_implemented
        and rendered_info.timed_out
        and mode != "refresh"
        and render_error_card
    ):
        timeout_stack_trace = (
            "\nCard rendering timed out after %s seconds. "
            "To increase the timeout duration for card rendering, please set the `timeout` parameter in the @card decorator. "
            "\nStack Trace : \n%s"
        ) % (timeout, rendered_info.timeout_stack_trace)
        rendered_content = _render_error_card(timeout_stack_trace)
    elif (
        rendered_info.is_implemented
        and rendered_info.data is None
        and render_error_card
        and mode != "refresh"
    ):
        rendered_content = _render_error_card(
            "No information rendered from card of type %s" % type
        )
    elif (
        not rendered_info.is_implemented
        and render_error_card
        and mode == "render_runtime"
    ):
        message = (
            "Card of type %s is a runtime time card with no `render_runtime` implemented. "
            "Please implement `render_runtime` method to allow rendering this card at runtime."
        ) % type
        rendered_content = _render_error_card(message)

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

    if rendered_content is not None:
        if mode == "refresh":
            card_datastore.save_data(
                card_uuid, save_type, rendered_content, card_id=card_id
            )
            ctx.obj.echo("Data updated", fg="green")
        else:
            card_info = card_datastore.save_card(
                card_uuid, save_type, rendered_content, card_id=card_id
            )
            ctx.obj.echo(
                "Card created with type: %s and hash: %s"
                % (card_info.type, card_info.hash[:NUM_SHORT_HASH_CHARS]),
                fg="green",
            )
            if save_metadata:
                _save_metadata(
                    ctx.obj.metadata,
                    task.parent.parent.id,
                    task.parent.id,
                    task.id,
                    task.current_attempt,
                    card_uuid,
                    save_metadata,
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


@card.command(help="Run local card viewer server")
@click.option(
    "--run-id",
    default=None,
    show_default=True,
    type=str,
    help="Run ID of the flow",
)
@click.option(
    "--port",
    default=8324,
    show_default=True,
    type=int,
    help="Port on which Metaflow card viewer server will run",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    show_default=True,
    type=str,
    help="Namespace of the flow",
)
@click.option(
    "--poll-interval",
    default=5,
    show_default=True,
    type=int,
    help="Polling interval of the card viewer server.",
)
@click.option(
    "--max-cards",
    default=30,
    show_default=True,
    type=int,
    help="Maximum number of cards to be shown at any time by the card viewer server",
)
@click.pass_context
def server(ctx, run_id, port, user_namespace, poll_interval, max_cards):
    from .card_server import create_card_server, CardServerOptions

    user_namespace = resolve_identity() if user_namespace is None else user_namespace
    run, follow_new_runs, _status_message = _get_run_object(
        ctx.obj, run_id, user_namespace
    )
    if _status_message is not None:
        ctx.obj.echo(_status_message, fg="red")
    options = CardServerOptions(
        flow_name=ctx.obj.flow.name,
        run_object=run,
        only_running=False,
        follow_resumed=False,
        flow_datastore=ctx.obj.flow_datastore,
        max_cards=max_cards,
        follow_new_runs=follow_new_runs,
        poll_interval=poll_interval,
    )
    create_card_server(options, port, ctx.obj)


def _get_run_from_cli_set_runid(obj, run_id):
    # This run-id will be set from the command line args.
    # So if we hit a MetaflowNotFound exception / Namespace mismatch then
    # we should raise an exception
    from metaflow import Run

    flow_name = obj.flow.name
    if len(run_id.split("/")) > 1:
        raise CommandException(
            "run_id should NOT be of the form: `<flowname>/<runid>`. Please provide only run-id"
        )
    try:
        pathspec = "%s/%s" % (flow_name, run_id)
        # Since we are looking at all namespaces,
        # we will not
        namespace(None)
        return Run(pathspec)
    except MetaflowNotFound:
        raise CommandException("No run (%s) found for *%s*." % (run_id, flow_name))


def _get_run_object(obj, run_id, user_namespace):
    from metaflow import Flow

    follow_new_runs = True
    flow_name = obj.flow.name

    if run_id is not None:
        follow_new_runs = False
        run = _get_run_from_cli_set_runid(obj, run_id)
        obj.echo("Using run-id %s" % run.pathspec, fg="blue", bold=False)
        return run, follow_new_runs, None

    _msg = "Searching for runs in namespace: %s" % user_namespace
    obj.echo(_msg, fg="blue", bold=False)

    try:
        namespace(user_namespace)
        flow = Flow(pathspec=flow_name)
        run = flow.latest_run
    except MetaflowNotFound:
        # When we have no runs found for the Flow, we need to ensure that
        # if the `follow_new_runs` is set to True; If `follow_new_runs` is set to True then
        # we don't raise the Exception and instead we return None and let the
        # background Thread wait on the Retrieving the run object.
        _status_msg = "No run found for *%s*." % flow_name
        return None, follow_new_runs, _status_msg

    except MetaflowNamespaceMismatch:
        _status_msg = (
            "No run found for *%s* in namespace *%s*. You can switch the namespace using --namespace"
            % (
                flow_name,
                user_namespace,
            )
        )
        return None, follow_new_runs, _status_msg

    obj.echo("Using run-id %s" % run.pathspec, fg="blue", bold=False)
    return run, follow_new_runs, None
