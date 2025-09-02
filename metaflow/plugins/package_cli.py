from metaflow._vendor import click
from hashlib import sha1
from metaflow.package import MetaflowPackage


@click.group()
def cli():
    pass


@cli.group(help="Commands related to code packages.")
@click.option(
    "--timeout", default=60, help="Timeout for package operations in seconds."
)
@click.pass_obj
def package(obj, timeout):
    # Prepare the package before any of the sub-commands are invoked.
    # We explicitly will *not* upload it to the datastore.
    obj.package = MetaflowPackage(
        obj.flow,
        obj.environment,
        obj.echo,
        suffixes=obj.package_suffixes,
        flow_datastore=None,
    )
    obj.package_op_timeout = timeout


@package.command(help="Output information about the code package.")
@click.pass_obj
def info(obj):
    obj.echo_always(obj.package.show())


@package.command(help="List all files included in the code package.")
@click.option(
    "--archive/--no-archive",
    default=False,
    help="If True, lists the file paths as present in the code package archive; "
    "otherwise, lists the files on your filesystem included in the code package",
    show_default=True,
)
@click.pass_obj
def list(obj, archive=False):
    _ = obj.package.blob_with_timeout(timeout=obj.package_op_timeout)
    # We now have all the information about the blob
    obj.echo(
        "Files included in the code package (change with --package-suffixes):",
        fg="magenta",
        bold=False,
    )
    if archive:
        obj.echo_always("\n".join(path for _, path in obj.package.path_tuples()))
    else:
        obj.echo_always("\n".join(path for path, _ in obj.package.path_tuples()))


@package.command(help="Save the current code package to a file.")
@click.argument("path")
@click.pass_obj
def save(obj, path):
    with open(path, "wb") as f:
        f.write(obj.package.blob)
    obj.echo(
        "Code package saved in *%s* with metadata: %s"
        % (path, obj.package.package_metadata),
        fg="magenta",
        bold=False,
    )
