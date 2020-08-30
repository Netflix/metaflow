import json
import importlib

import click

from metaflow.client.cache.cache_action import import_action_class_spec

@click.command()
@click.option("--request-file",
              default='request.json',
              help="Read request from this file.")
@click.argument('action_spec')
def cli(action_spec, request_file=None):
    """
    Execute an action specified by action_spec.
    """
    action_cls = import_action_class_spec(action_spec)
    with open(request_file) as f:
        req = json.load(f)

    action_cls.execute(req['message'], req['keys'], req['stream_key'])

if __name__ == '__main__':
    cli(auto_envvar_prefix='MFCACHE_WORKER')
