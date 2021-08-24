import metaflow
from metaflow.client import Task
from .card_datastore import CardDatastore
from .exception import CardNotFoundException
from metaflow import Flow,Step
import click
from metaflow.exception import MetaflowNotFound

@click.group()
def cli():
    pass


@cli.group(help="Commands related to @card decorator.")
def card():
    pass

@card.command(help='create the HTML card')
@click.option('--card-type',
                default=None,
                show_default=True,
                type=str,
                help="Type of card being created")
@click.option('--run-path-spec',
                default=None,
                show_default=True,
                type=str,
                help="Path spec of the run.")
@click.option('--metadata-path',
                default=None,
                show_default=True,
                type=str,
                help="Metadata of the run instance.")
@click.pass_context
def create(ctx, run_path_spec=None,card_type=None,metadata_path=None):
    from metaflow import get_metadata,metadata
    metadata(metadata_path)
    assert run_path_spec is not None
    flow_name,runid,step_name,task_id = run_path_spec.split('/')
    task = Task(run_path_spec)
    from metaflow.plugins import CARDS

    filtered_cards = [CardClass for CardClass in CARDS if CardClass.name == card_type]
    if len(filtered_cards) == 0:
        raise CardNotFoundException(card_type)
    
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                runid,\
                                step_name,\
                                task_id,\
                                mode='w',\
                                path_spec=run_path_spec)
    
    filtered_card = filtered_cards[0]
    # save card to datastore
    rendered_info = filtered_card().render(task)
    card_datastore.save_card(card_type,rendered_info)

@card.command(help='View the HTML card')
@click.argument('step-name',
                type=str,)
@click.option('--card-type',
                default=None,
                show_default=True,
                type=str,
                help="Type of card being created")
@click.option('--run-id',
                default='latest',
                show_default=True,
                type=str,
                help="Id of the run")
@click.pass_context
def view(ctx, step_name, run_id=None,card_type=None):
    assert step_name is not None
    flow_name = ctx.obj.flow.name
    
    if run_id == 'latest':
        # what should be the args we expose 
        run_id = Flow(flow_name).latest_run.id

    try : 
        mf_step = Step('/'.join([flow_name,run_id,step_name]))
        # Todo : What is the best way to get a particular task;
        tasks = list(mf_step)
    except MetaflowNotFound as e:
        ctx.obj.echo("Cannot find Step/Task '%s'"%step_name,fg='red',bold=True)
        return
    
    if len(tasks) == 0:
        ctx.obj.echo("No tasks found for step '%s'"%step_name,fg='red',bold=True)
        return
    
    # ! Currently showing task for One taskid;
    task = tasks[0]
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                run_id,\
                                step_name,\
                                task.id,\
                                path_spec=None)
    
    card_datastore.view_card(card_type)