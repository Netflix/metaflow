# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys

import click
import typer

from kfp._client import Client
from kfp.cli.run import run
from kfp.cli.recurring_run import recurring_run
from kfp.cli.pipeline import pipeline
from kfp.cli.diagnose_me_cli import diagnose_me
from kfp.cli.experiment import experiment
from kfp.cli.output import OutputFormat
from kfp.cli import components


@click.group()
@click.option('--endpoint', help='Endpoint of the KFP API service to connect.')
@click.option('--iap-client-id', help='Client ID for IAP protected endpoint.')
@click.option(
    '-n',
    '--namespace',
    default='kubeflow',
    show_default=True,
    help='Kubernetes namespace to connect to the KFP API.')
@click.option(
    '--other-client-id',
    help='Client ID for IAP protected endpoint to obtain the refresh token.')
@click.option(
    '--other-client-secret',
    help='Client ID for IAP protected endpoint to obtain the refresh token.')
@click.option(
    '--output',
    type=click.Choice(list(map(lambda x: x.name, OutputFormat))),
    default=OutputFormat.table.name,
    show_default=True,
    help='The formatting style for command output.')
@click.option(
    '--userid',
    help='Client ID for IAP protected endpoint to obtain the refresh token.')
@click.pass_context
def cli(ctx: click.Context, endpoint: str, iap_client_id: str, namespace: str,
        other_client_id: str, other_client_secret: str, output: OutputFormat, userid: str):
    """kfp is the command line interface to KFP service.

    Feature stage:
    [Alpha](https://github.com/kubeflow/pipelines/blob/07328e5094ac2981d3059314cc848fbb71437a76/docs/release/feature-stages.md#alpha)
    """
    if ctx.invoked_subcommand == 'diagnose_me':
        # Do not create a client for diagnose_me
        return
    ctx.obj['client'] = Client(endpoint, iap_client_id, namespace,
                               other_client_id, other_client_secret, userid=userid)
    ctx.obj['namespace'] = namespace
    ctx.obj['output'] = output

def main():
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    cli.add_command(run)
    cli.add_command(recurring_run)
    cli.add_command(pipeline)
    cli.add_command(diagnose_me, 'diagnose_me')
    cli.add_command(experiment)
    cli.add_command(typer.main.get_command(components.app))
    try:
        cli(obj={}, auto_envvar_prefix='KFP')
    except Exception as e:
        click.echo(str(e), err=True)
        sys.exit(1)
