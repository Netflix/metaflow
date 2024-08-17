import json
import os
from metaflow._vendor import click
from metaflow.cli import echo_always as echo
from metaflow.plugins.datastores.local_storage import LocalStorage

from .docker_environment import get_fastbakery_metafile_path
from .fast_bakery import FastBakeryApiResponse


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Fast Bakery support.")
@click.pass_context
def fast_bakery(ctx):
    path = LocalStorage.get_datastore_root_from_config(echo, create_on_absent=False)
    ctx.obj.metafile_path = get_fastbakery_metafile_path(path, ctx.obj.flow.name)


@fast_bakery.command(help="Purge local Fast Bakery cache.")
@click.pass_obj
def purge(obj):
    try:
        os.remove(obj.metafile_path)
        echo("Local Fast Bakery cache purged.")
    except FileNotFoundError:
        echo("No local Fast Bakery cache found.")


@fast_bakery.command(help="List the cached images")
@click.pass_obj
def images(obj):
    current_cache = None
    try:
        with open(obj.metafile_path, "r") as f:
            current_cache = json.load(f)
    except FileNotFoundError:
        pass

    if current_cache:
        echo("List of locally cached image tags:\n")

        for val in current_cache.values():
            response = FastBakeryApiResponse(val)
            echo(response.container_image)

        echo(
            "In order to clear the cached images, you can use the command\n *fast-bakery purge*"
        )
    else:
        echo("No locally cached images.")
