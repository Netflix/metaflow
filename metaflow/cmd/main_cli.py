import importlib
import os
import traceback

from metaflow._vendor import click

from metaflow.extension_support import get_modules, _ext_debug
from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

from .util import echo_always


def add_cmd_support(g, base_init=False):
    g["__cmds"] = {}

    if base_init:
        g["__ext_add_cmds"] = []

    def _add(name, path, cli, pkg=g["__package__"], add_to=g["__cmds"]):
        if path[0] == ".":
            pkg_components = pkg.split(".")
            i = 1
            while i < len(path) and path[i] == ".":
                i += 1
            # We deal with multiple periods at the start
            if i > len(pkg_components):
                raise ValueError("Path '%s' exits out of metaflow module" % path)
            path = (
                ".".join(pkg_components[: -i + 1] if i > 1 else pkg_components)
                + path[i - 1 :]
            )
        _ext_debug("    Adding cmd: %s from %s.%s" % (name, path, cli))
        add_to[name] = (path, cli)

    g["cmd_add"] = _add


add_cmd_support(globals(), base_init=True)


@click.group()
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


cmd_add("configure", ".configure_cmd", "cli")
cmd_add("tutorials", ".tutorials_cmd", "cli")


def _get_ext_cmds(module):
    return getattr(module, "__cmds", {})


def _lazy_cmd_resolve():
    from metaflow.metaflow_config import ENABLED_CMDS

    list_of_cmds = list(globals()["__ext_add_cmds"])
    list_of_cmds.extend(ENABLED_CMDS)
    _ext_debug("Got raw list of commands as: %s" % str(list_of_cmds))

    set_of_commands = set()
    for p in list_of_cmds:
        if p.startswith("-"):
            set_of_commands.discard(p[1:])
        elif p.startswith("+"):
            set_of_commands.add(p[1:])
        else:
            set_of_commands.add(p)
    _ext_debug("Resolved list of commands is: %s" % str(list_of_cmds))

    to_return = [main]
    for name in set_of_commands:
        path, cli = __cmds.get(name, (None, None))
        if path is None:
            raise ValueError(
                "Configuration requested command '%s' but no such command is available"
                % name
            )
        plugin_module = importlib.import_module(path)
        cls = getattr(plugin_module, cli, None)
        if cli is None:
            raise ValueError("'%s' not found in module '%s'" % (cli, path))
        all_cmds = list(cls.commands)
        if len(all_cmds) > 1:
            raise ValueError(
                "%s.%s defines more than one command -- use a group" % (path, cli)
            )
        if all_cmds[0] != name:
            raise ValueError(
                "%s.%s: expected name to be '%s' but got '%s' instead"
                % (path, cli, name, all_cmds[0])
            )
        to_return.append(cls)
    return to_return


try:
    _modules_to_import = get_modules("cmd")
    for m in _modules_to_import:
        globals()["__cmds"].update(_get_ext_cmds(m.module))
        globals()["__ext_add_cmds"].extend(list(_get_ext_cmds(m.module).keys()))

except Exception as e:
    _ext_debug("\tWARNING: ignoring all plugins due to error during import: %s" % e)
    print(
        "WARNING: Command extensions did not load -- ignoring all of them which may not "
        "be what you want: %s" % e
    )
    _clis = []
    traceback.print_exc()


@click.command(
    cls=click.CommandCollection,
    sources=_lazy_cmd_resolve(),
    invoke_without_command=True,
)
@click.pass_context
def start(ctx):
    global echo
    echo = echo_always

    import metaflow

    echo("Metaflow ", fg="magenta", bold=True, nl=False)

    if ctx.invoked_subcommand is None:
        echo("(%s): " % metaflow.__version__, fg="magenta", bold=False, nl=False)
    else:
        echo("(%s)\n" % metaflow.__version__, fg="magenta", bold=False)

    if ctx.invoked_subcommand is None:
        echo("More data science, less engineering\n", fg="magenta")

        # metaflow URL
        echo("http://docs.metaflow.org", fg="cyan", nl=False)
        echo(" - Read the documentation")

        # metaflow chat
        echo("http://chat.metaflow.org", fg="cyan", nl=False)
        echo(" - Chat with us")

        # metaflow help email
        echo("help@metaflow.org", fg="cyan", nl=False)
        echo("        - Get help by email\n")

        print(ctx.get_help())


if __name__ == "__main__":
    start()

for _n in [
    "__cmds",
    "__ext_add_cmds",
    "add_cmd_support",
    "cmd_add",
    "get_modules",
    "load_module",
    "_modules_to_import",
    "m",
    "_get_ext_cmds",
    "_clis",
    "_ext_debug",
    "e",
]:
    try:
        del globals()[_n]
    except KeyError:
        pass
del globals()["_n"]
