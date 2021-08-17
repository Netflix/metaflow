from metaflow.client import Task
from .card_datastore import CardDatastore
from .exception import CardNotFoundException
import click

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
    