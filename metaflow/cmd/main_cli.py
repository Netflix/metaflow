import os

from metaflow._vendor import click

from metaflow.extension_support.cmd import process_cmds, resolve_cmds
from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, CONTACT_INFO
from metaflow.metaflow_version import get_version

from .util import echo_always
import metaflow.tracing as tracing


@click.group()
@tracing.cli_entrypoint("cli/main")
def main():
    pass


@main.command(help="Show all available commands.")
@click.pass_context
def help(ctx):
    print(ctx.parent.get_help())


@main.command(help="Show flows accessible from the current working tree.")
def status():
    from metaflow.client import get_metadata

    res = get_metadata()
    if res:
        res = res.split("@")
    else:
        raise click.ClickException("Unknown status: cannot find a Metadata provider")
    if res[0] == "service":
        echo("Using Metadata provider at: ", nl=False)
        echo('"%s"\n' % res[1], fg="cyan")
        echo("To list available flows, type:\n")
        echo("1. python")
        echo("2. from metaflow import Metaflow")
        echo("3. list(Metaflow())")
        return

    from metaflow.client import namespace, metadata, Metaflow

    # Get the local data store path
    path = LocalStorage.get_datastore_root_from_config(echo, create_on_absent=False)
    # Throw an exception
    if path is None:
        raise click.ClickException(
            "Could not find "
            + click.style('"%s"' % DATASTORE_LOCAL_DIR, fg="red")
            + " in the current working tree."
        )

    stripped_path = os.path.dirname(path)
    namespace(None)
    metadata("local@%s" % stripped_path)
    echo("Working tree found at: ", nl=False)
    echo('"%s"\n' % stripped_path, fg="cyan")
    echo("Available flows:", fg="cyan", bold=True)
    for flow in Metaflow():
        echo("* %s" % flow, fg="cyan")


CMDS_DESC = [
    ("configure", ".configure_cmd.cli"),
    ("tutorials", ".tutorials_cmd.cli"),
    ("develop", ".develop.cli"),
]

process_cmds(globals())


@click.command(
    cls=click.CommandCollection,
    sources=[main] + resolve_cmds(),
    invoke_without_command=True,
)
@click.pass_context
def start(ctx):
    global echo
    echo = echo_always

    import metaflow

    version = get_version()
    echo("Metaflow ", fg="magenta", bold=True, nl=False)

    if ctx.invoked_subcommand is None:
        echo("(%s): " % version, fg="magenta", bold=False, nl=False)
    else:
        echo("(%s)\n" % version, fg="magenta", bold=False)

    if ctx.invoked_subcommand is None:
        echo("More data science, less engineering\n", fg="magenta")

        lnk_sz = max(len(lnk) for lnk in CONTACT_INFO.values()) + 1
        for what, lnk in CONTACT_INFO.items():
            echo("%s%s" % (lnk, " " * (lnk_sz - len(lnk))), fg="cyan", nl=False)
            echo("- %s" % what)
        echo("")

        print(ctx.get_help())


if __name__ == "__main__":
    start()
