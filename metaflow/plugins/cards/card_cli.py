from metaflow.datastore.datastore import MetaflowDataStore
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
@click.option('--run-id-file',
                default=None,
                show_default=True,
                type=str,
                help="Write the ID of this run to the file specified.")
@click.pass_context
def generate(ctx, run_id_file=None,card_type=None):
    assert run_id_file is not None
    Datastore : MetaflowDataStore = ctx.obj.datastore
    flow_name,runid,step_name,task_id = run_id_file.split('/')
    datastore_instance = Datastore(
        flow_name,
        run_id = runid,
        step_name = step_name,
        task_id = task_id,
    )
    from .renderer.basic import BasicRenderer
    rendered_info = BasicRenderer().render(datastore_instance)
    print(rendered_info)
    # todo : Save the copy of the Card in the `Datastore`