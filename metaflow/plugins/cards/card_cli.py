from metaflow.client import Task
from metaflow import Flow, JSONType,Step
import webbrowser
import click
import os
from metaflow.exception import MetaflowNotFound
from .card_datastore import CardDatastore,stepname_from_card_id
from .exception import CardClassFoundException,\
                        IncorrectCardArgsException,\
                        UnrenderableCardException,\
                        CardNotPresentException,\
                        IdNotFoundException,\
                        TypeRequiredException

def open_in_browser(card_path):
    url = 'file://' + os.path.abspath(card_path)
    webbrowser.open(url)

def resolve_card(ctx,identifier,type=None,index=None):
    is_path_spec = False
    if "/" in identifier:
        is_path_spec= True
        assert len(identifier.split('/'))  == 3, "Expecting pathspec of form <runid>/<stepname>/<taskid>"
    
    flow_name = ctx.obj.flow.name
    pathspec,run_id,step_name,task_id = None,None,None,None
    # this means that identifier is a pathspec 
    if is_path_spec:
        try:
            assert type is not None, "if IDENTIFIER is a pathspec than --type is required"
        except AssertionError:
            raise TypeRequiredException()
        # what should be the args we expose 
        run_id,step_name,task_id = identifier.split('/')
        pathspec = '/'.join([flow_name,run_id,step_name,task_id])
    else:
        # this means the identifier is card-id
        # So we first resolve the id to stepname.
        step_name = stepname_from_card_id(
            identifier,ctx.obj.flow
        )
        # this means that the `id` doesn't match 
        # the one specified in the decorator
        if step_name is None:
            raise IdNotFoundException(
                identifier
            )
        # todo : Should this only resolve cards of the latest runs. 
        run_id = Flow(flow_name).latest_run.id
        step = Step('/'.join([flow_name,run_id,step_name]))
        tasks = list(step)
        # todo : how to resolve cards in a foreach with card-id
        if len(tasks) == 0:
            raise Exception(
                "No Tasks found for Step '%s' and Run '%s'" % (step_name,run_id)
            )
        # todo : What to do when we have multiple tasks to resolve? 
        task = tasks[0]
        pathspec = task.pathspec
        task_id = task.id
    
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                run_id,\
                                step_name,\
                                task_id,\
                                path_spec=pathspec)
    
    return card_datastore.extract_cards(card_type=type,card_id=None if is_path_spec else identifier,card_index=index),card_datastore



def list_availble_cards(card_paths):
    # todo : create nice response messages on the CLI for cards which were found.
    
    pass



@click.group()
def cli():
    pass


@cli.group(help="Commands related to @card decorator.")
def card():
    pass



# Finished According to the Memo
@card.command(help='create the HTML card')
@click.argument('pathspec',type=str)
@click.option('--type', 
                default=None,
                show_default=True,
                type=str,
                help="Type of card being created")
@click.option('--options', 
                default={},
                show_default=True,
                type=JSONType,
                help="arguments of the card being created.")
@click.option('--id', 
                default=None,
                show_default=True,
                type=str,
                help="Unique ID of the card")
@click.option('--index', 
                default=0,
                show_default=True,
                type=int,
                help="Index of the card decorator")
@click.pass_context
def create(ctx,pathspec,type=None,id=None,index=None,options=None):
    ctx.obj.echo("Creating new card of type %s" % type, fg='green')
    assert len(pathspec.split('/'))  == 3, "Expecting pathspec of form <runid>/<stepname>/<taskid>"
    runid,step_name,task_id = pathspec.split('/')
    flowname = ctx.obj.flow.name
    full_pathspec = '/'.join([flowname,runid,step_name,task_id])
    task = Task(full_pathspec)
    from metaflow.plugins import CARDS

    filtered_cards = [CardClass for CardClass in CARDS if CardClass.type == type]
    if len(filtered_cards) == 0:
        raise CardClassFoundException(type)
    
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                runid,\
                                step_name,\
                                task_id,\
                                path_spec=full_pathspec)
    
    filtered_card = filtered_cards[0]
    # save card to datastore
    try:
        mf_card = filtered_card(**options)
    except TypeError as e:
        raise IncorrectCardArgsException(type,options)
    
    try:
        rendered_info = mf_card.render(task)
    except: # TODO : Catch exec trace over here. 
        raise UnrenderableCardException(type,options)
    else:
        card_datastore.save_card(type,id,index,rendered_info)

@card.command()
@click.argument('identifier')
@click.option('--type', 
                default=None,
                show_default=True,
                type=str,
                help="Type of card being created")
@click.option('--index', 
                default=0,
                show_default=True,
                type=int,
                help="Index of the card decorator")
@click.pass_context
def view(ctx,identifier,type=None,index=None):
    """
    View the HTML card in browser based on the IDENTIFIER.\n
    The IDENTIFIER can be:\n 
        - run path spec : <runid>/<stepname>/<taskid>\n
                    OR\n
        - id given in the @card\n
    """
    available_card_paths,card_datastore = resolve_card(ctx,identifier,type=type,index=index)
    if len(available_card_paths) == 1:
        open_in_browser(
            card_datastore.cache_locally(available_card_paths[0])
        )
    else:
        list_availble_cards(available_card_paths)

@card.command()
@click.argument('identifier')
@click.option('--type', 
                default=None,
                show_default=True,
                type=str,
                help="Type of card being created")
@click.option('--index', 
                default=0,
                show_default=True,
                type=int,
                help="Index of the card decorator")
@click.pass_context
def get(ctx,identifier,type=None,index=None):
    available_card_paths,card_datastore = resolve_card(ctx,identifier,type=type,index=index)
    
    if len(available_card_paths) == 1:
        with open(card_datastore.cache_locally(available_card_paths[0]),'r') as f:
            print(f.read())
    else:
        list_availble_cards(available_card_paths)