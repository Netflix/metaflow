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
    datastore : MetaflowDataStore = ctx.obj.datastore
    print("generating New cards with datastore : ",ctx.obj.datastore)