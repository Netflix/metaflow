import os

from metaflow._vendor import click


def makedirs(path):
    # This is for python2 compatibility.
    # Python3 has os.makedirs(exist_ok=True).
    try:
        os.makedirs(path)
    except OSError as x:
        if x.errno == 17:
            return
        else:
            raise


def echo_dev_null(*args, **kwargs):
    pass


def echo_always(line, **kwargs):
    click.secho(line, **kwargs)
