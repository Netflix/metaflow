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
import sys
import subprocess
import time
import json
import click
import shutil
import datetime
from typing import List

import kfp_server_api
from kfp._client import Client
from kfp.cli.output import print_output, OutputFormat


@click.group()
def run():
    """manage run resources."""
    pass


@run.command()
@click.option(
    '-e', '--experiment-id', help='Parent experiment ID of listed runs.')
@click.option(
    '--page-token', default='', help="Token for starting of the page.")
@click.option(
    '-m', '--max-size', default=100, help="Max size of the listed runs.")
@click.option(
    '--sort-by',
    default="created_at desc",
    help="Can be '[field_name]', '[field_name] desc'. For example, 'name desc'."
)
@click.option(
    '--filter',
    help=(
        "filter: A url-encoded, JSON-serialized Filter protocol buffer "
        "(see [filter.proto](https://github.com/kubeflow/pipelines/blob/master/backend/api/filter.proto))."
    ))
@click.pass_context
def list(ctx: click.Context, experiment_id: str, page_token: str, max_size: int,
         sort_by: str, filter: str):
    """list recent KFP runs."""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    response = client.list_runs(
        experiment_id=experiment_id,
        page_token=page_token,
        page_size=max_size,
        sort_by=sort_by,
        filter=filter)
    if response and response.runs:
        _print_runs(response.runs, output_format)
    else:
        if output_format == OutputFormat.json.name:
            msg = json.dumps([])
        else:
            msg = 'No runs found.'
        click.echo(msg)


@run.command()
@click.option(
    '-e',
    '--experiment-name',
    required=True,
    help='Experiment name of the run.')
@click.option('-r', '--run-name', help='Name of the run.')
@click.option(
    '-f',
    '--package-file',
    type=click.Path(exists=True, dir_okay=False),
    help='Path of the pipeline package file.')
@click.option('-p', '--pipeline-id', help='ID of the pipeline template.')
@click.option('-n', '--pipeline-name', help='Name of the pipeline template.')
@click.option(
    '-w',
    '--watch',
    is_flag=True,
    default=False,
    help='Watch the run status until it finishes.')
@click.option('-v', '--version', help='ID of the pipeline version.')
@click.option(
    '-t',
    '--timeout',
    default=0,
    help='Wait for a run to complete until timeout in seconds.',
    type=int)
@click.argument('args', nargs=-1)
@click.pass_context
def submit(ctx: click.Context, experiment_name: str, run_name: str,
           package_file: str, pipeline_id: str, pipeline_name: str, watch: bool,
           timeout: int, version: str, args: List[str]):
    """submit a KFP run."""
    client = ctx.obj['client']
    namespace = ctx.obj['namespace']
    output_format = ctx.obj['output']
    if not run_name:
        run_name = experiment_name

    if not pipeline_id and pipeline_name:
        pipeline_id = client.get_pipeline_id(name=pipeline_name)

    if not package_file and not pipeline_id and not version:
        click.echo(
            'You must provide one of [package_file, pipeline_id, version].',
            err=True)
        sys.exit(1)

    arg_dict = dict(arg.split('=', maxsplit=1) for arg in args)

    experiment = client.create_experiment(experiment_name)
    run = client.run_pipeline(
        experiment.id,
        run_name,
        package_file,
        arg_dict,
        pipeline_id,
        version_id=version)
    if timeout > 0:
        _wait_for_run_completion(client, run.id, timeout, output_format)
    else:
        _display_run(client, namespace, run.id, watch, output_format)


@run.command()
@click.option(
    '-w',
    '--watch',
    is_flag=True,
    default=False,
    help='Watch the run status until it finishes.')
@click.option(
    '-d',
    '--detail',
    is_flag=True,
    default=False,
    help='Get detailed information of the run in json format.')
@click.argument('run-id')
@click.pass_context
def get(ctx: click.Context, watch: bool, detail: bool, run_id: str):
    """display the details of a KFP run."""
    client = ctx.obj['client']
    namespace = ctx.obj['namespace']
    output_format = ctx.obj['output']

    _display_run(client, namespace, run_id, watch, output_format, detail)


def _display_run(client: click.Context,
                 namespace: str,
                 run_id: str,
                 watch: bool,
                 output_format: OutputFormat,
                 detail: bool = False):
    run = client.get_run(run_id).run

    if detail:
        data = {
            key:
            value.isoformat() if isinstance(value, datetime.datetime) else value
            for key, value in run.to_dict().items()
            if key not in ['pipeline_spec'
                          ]  # useless but too much detailed field
        }
        if output_format == OutputFormat.json.name:
            click.echo(json.dumps(data, indent=4))
        else:
            click.echo(data)
        return

    _print_runs([run], output_format)
    if not watch:
        return
    argo_path = shutil.which('argo')
    if not argo_path:
        raise RuntimeError(
            "argo isn't found in $PATH. It's necessary for watch. "
            "Please make sure it's installed and available. "
            "Installation instructions be found here - "
            "https://github.com/argoproj/argo-workflows/releases")

    argo_workflow_name = None
    while True:
        time.sleep(1)
        run_detail = client.get_run(run_id)
        run = run_detail.run
        if run_detail.pipeline_runtime and run_detail.pipeline_runtime.workflow_manifest:
            manifest = json.loads(run_detail.pipeline_runtime.workflow_manifest)
            if manifest['metadata'] and manifest['metadata']['name']:
                argo_workflow_name = manifest['metadata']['name']
                break
        if run_detail.run.status in ['Succeeded', 'Skipped', 'Failed', 'Error']:
            click.echo('Run is finished with status {}.'.format(
                run_detail.run.status))
            return
    if argo_workflow_name:
        subprocess.run(
            [argo_path, 'watch', argo_workflow_name, '-n', namespace])
        _print_runs([run], output_format)


def _wait_for_run_completion(client: Client, run_id: str, timeout: int,
                             output_format: OutputFormat):
    run_detail = client.wait_for_run_completion(run_id, timeout)
    _print_runs([run_detail.run], output_format)


def _print_runs(runs: List[kfp_server_api.ApiRun], output_format: OutputFormat):
    headers = ['run id', 'name', 'status', 'created at', 'experiment id']
    data = [[
        run.id, run.name, run.status,
        run.created_at.isoformat(),
        next(rr
             for rr in run.resource_references
             if rr.key.type == kfp_server_api.ApiResourceType.EXPERIMENT).key.id
    ]
            for run in runs]
    print_output(data, headers, output_format, table_format='grid')
