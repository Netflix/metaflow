import click

@click.group()
def cli():
    pass


@cli.group(help="Commands related to @card decorator.")
@click.pass_obj
def card(obj):
    pass

@card.command(help='generate the HTML card')
@click.option('--run-id-file',
                default=None,
                show_default=True,
                type=str,
                help="Write the ID of this run to the file specified.")
@click.pass_obj
def generate(obj, run_id_file=None,):
    pass
