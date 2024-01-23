from typing import Any

from metaflow.cli import echo_dev_null, echo_always
from metaflow._vendor import click


class CommandObj:
    def __init__(self):
        pass


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.group(help="Metaflow develop commands")
@click.option(
    "--quiet/--no-quiet",
    show_default=True,
    default=False,
    help="Suppress unnecessary messages",
)
@click.pass_context
def develop(
    ctx: Any,
    quiet: bool,
):
    if quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    obj = CommandObj()
    obj.quiet = quiet
    obj.echo = echo
    obj.echo_always = echo_always
    ctx.obj = obj


from . import stubs
