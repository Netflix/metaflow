from metaflow import decorators
from metaflow._vendor import click
import metaflow.tracing as tracing


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Armada.")
def armada():
    pass


@tracing.cli_entrypoint("armada/step")
@armada.command(help="Submit a job to Armada.")
def step(ctx):
    ctx.obj.echo_always("Hello world Armada.")
