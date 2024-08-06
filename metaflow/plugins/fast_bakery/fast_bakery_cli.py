import os
from metaflow._vendor import click
from metaflow.cli import echo_always as echo

# from .docker_environment import read_metafile, BAKERY_METAFILE


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Fast Bakery support.")
def fast_bakery():
    pass


@fast_bakery.command(help="Purge local Fast Bakery cache.")
def purge():
    try:
        # os.remove(BAKERY_METAFILE)
        echo("Local Fast Bakery cache purged.")
    except FileNotFoundError:
        echo("No local Fast Bakery cache found.")
