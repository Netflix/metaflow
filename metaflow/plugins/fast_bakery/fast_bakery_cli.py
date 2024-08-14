import json
import os
from metaflow._vendor import click
from metaflow.cli import echo_always as echo

from .docker_environment import BAKERY_METAFILE
from .fast_bakery import FastBakeryApiResponse


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Fast Bakery support.")
def fast_bakery():
    pass


@fast_bakery.command(help="Purge local Fast Bakery cache.")
def purge():
    try:
        os.remove(BAKERY_METAFILE)
        echo("Local Fast Bakery cache purged.")
    except FileNotFoundError:
        echo("No local Fast Bakery cache found.")


@fast_bakery.command(help="List the cached images")
def images():
    current_cache = None
    try:
        with open(BAKERY_METAFILE, "r") as f:
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
