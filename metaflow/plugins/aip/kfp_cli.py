import inspect
from pathlib import Path

from metaflow._vendor import click
from metaflow.plugins.aip.aip_decorator import AIPException


@click.group()
def cli():
    pass


@cli.group(name="kfp", help="Deprecated, please use aip instead.")
@click.pass_obj
def kubeflow_pipelines(obj):
    pass


@kubeflow_pipelines.command(help="Deprecated, please use aip instead.")
@click.pass_obj
def run(
    obj,
):
    file_name = Path(inspect.getfile(obj.flow.__class__)).name
    raise AIPException(f'Deprecated, please use "python {file_name} aip run" instead.')


@kubeflow_pipelines.command(help="Deprecated, please use aip instead.")
@click.pass_obj
def create(
    obj,
):
    file_name = Path(inspect.getfile(obj.flow.__class__)).name
    raise AIPException(
        f'Deprecated, please use "python {file_name} aip create" instead.'
    )


@kubeflow_pipelines.command(help="Deprecated, please use aip instead.")
@click.pass_obj
def trigger(
    obj,
):
    file_name = Path(inspect.getfile(obj.flow.__class__)).name
    raise AIPException(
        f'Deprecated, please use "python {file_name} aip trigger" instead.'
    )
