from metaflow.client import Task
from metaflow.plugins.cards.card_datastore import CardDatastore
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
    from .renderer.basic import BasicRenderer
    rendered_info = BasicRenderer().render(task)
    card_datastore = CardDatastore(ctx.obj.flow_datastore,\
                                runid,\
                                step_name,\
                                task_id,\
                                mode='w',\
                                path_spec=run_path_spec)
    # Save the copy of the Card in the `Datastore`
    # :MetaflowDataStore 
    card_datastore.save_card(card_type,rendered_info)
    # write_datastore = Datastore(
    #     flow_name,
    #     run_id = runid,
    #     step_name = step_name,
    #     task_id = task_id,
    #     mode='w'
    # )
    # # todo : Create a new function in the datastore to store cards
    # # todo : move mustache From remote dependency to in-MF dep;
    # write_datastore.save_card(
    #     card_type,rendered_info
    # )