from metaflow._vendor import click
from metaflow.cli import LOGGER_TIMESTAMP


@click.group()
def cli():
    pass


@cli.group(help="Commands related to logs")
@click.pass_context
def environment(ctx):
    # the logger is configured in cli.py
    global echo
    echo = ctx.obj.echo


@environment.command(help="Rebuild the environment")
@click.option(
    "--step",
    "steps",
    multiple=True,
    default=[],
    help="Steps to rebuild the environment",
)
@click.pass_obj
def rebuild(obj, steps):

    steps = list(steps)
    print(steps, type(steps))

    print(obj.flow)
    print(obj.environment)
    obj.environment.init_environment(echo, steps)
