from metaflow.datastore.datastore import MetaflowDataStore
from metaflow.client import Task
import click

@click.group()
def cli():
    pass


@cli.group(help="Commands related to @card decorator.")
def card():
    pass

@card.command(help='generate the HTML card')
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
def generate(ctx, run_path_spec=None,card_type=None,metadata_path=None):
    from metaflow import get_metadata,metadata
    metadata(metadata_path)
    assert run_path_spec is not None
    Datastore = ctx.obj.datastore
    flow_name,runid,step_name,task_id = run_path_spec.split('/')
    task = Task(run_path_spec)
    from .renderer.basic import BasicRenderer
    rendered_info = BasicRenderer().render(task)
    # Save the copy of the Card in the `Datastore`
    # :MetaflowDataStore 
    write_datastore = Datastore(
        flow_name,
        run_id = runid,
        step_name = step_name,
        task_id = task_id,
        mode='w'
    )
    # todo : Create a new function in the datastore to store cards
    # todo : move mustache From remote dependency to in-MF dep;
    write_datastore.save_card(
        card_type,rendered_info
    )