import click
import json
from typing import List

from kfp.cli.output import print_output, OutputFormat
import kfp_server_api
from kfp_server_api.models.api_experiment import ApiExperiment


@click.group()
def experiment():
    """Manage experiment resources."""
    pass


@experiment.command()
@click.option('-d', '--description', help="Description of the experiment.")
@click.argument("name")
@click.pass_context
def create(ctx: click.Context, description: str, name: str):
    """Create an experiment."""
    client = ctx.obj["client"]
    output_format = ctx.obj["output"]

    response = client.create_experiment(name, description=description)
    _display_experiment(response, output_format)


@experiment.command()
@click.option(
    '--page-token', default='', help="Token for starting of the page.")
@click.option(
    '-m', '--max-size', default=100, help="Max size of the listed experiments.")
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
def list(ctx: click.Context, page_token: str, max_size: int, sort_by: str,
         filter: str):
    """List experiments."""
    client = ctx.obj['client']
    output_format = ctx.obj['output']

    response = client.list_experiments(
        page_token=page_token,
        page_size=max_size,
        sort_by=sort_by,
        filter=filter)
    if response.experiments:
        _display_experiments(response.experiments, output_format)
    else:
        if output_format == OutputFormat.json.name:
            msg = json.dumps([])
        else:
            msg = "No experiments found"
        click.echo(msg)


@experiment.command()
@click.argument("experiment-id")
@click.pass_context
def get(ctx: click.Context, experiment_id: str):
    """Get detailed information about an experiment."""
    client = ctx.obj["client"]
    output_format = ctx.obj["output"]

    response = client.get_experiment(experiment_id)
    _display_experiment(response, output_format)


@experiment.command()
@click.argument("experiment-id")
@click.pass_context
def delete(ctx: click.Context, experiment_id: str):
    """Delete an experiment."""

    confirmation = "Caution. The RunDetails page could have an issue" \
                   " when it renders a run that has no experiment." \
                   " Do you want to continue?"
    if not click.confirm(confirmation):
        return

    client = ctx.obj["client"]

    client.delete_experiment(experiment_id)
    click.echo("{} is deleted.".format(experiment_id))


def _display_experiments(experiments: List[ApiExperiment],
                         output_format: OutputFormat):
    headers = ["Experiment ID", "Name", "Created at"]
    data = [
        [exp.id, exp.name, exp.created_at.isoformat()] for exp in experiments
    ]
    print_output(data, headers, output_format, table_format="grid")


def _display_experiment(exp: kfp_server_api.ApiExperiment,
                        output_format: OutputFormat):
    table = [
        ["ID", exp.id],
        ["Name", exp.name],
        ["Description", exp.description],
        ["Created at", exp.created_at.isoformat()],
    ]
    if output_format == OutputFormat.table.name:
        print_output([], ["Experiment Details"], output_format)
        print_output(table, [], output_format, table_format="plain")
    elif output_format == OutputFormat.json.name:
        print_output(dict(table), [], output_format)


@experiment.command()
@click.option(
    "--experiment-id",
    default=None,
    help="The ID of the experiment to archive, can only supply either an experiment ID or name.")
@click.option(
    "--experiment-name",
    default=None,
    help="The name of the experiment to archive, can only supply either an experiment ID or name.")
@click.pass_context
def archive(ctx: click.Context, experiment_id: str, experiment_name: str):
    """Archive an experiment"""
    client = ctx.obj["client"]

    if (experiment_id is None) == (experiment_name is None):
        raise ValueError('Either experiment_id or experiment_name is required')

    if not experiment_id:
        experiment = client.get_experiment(experiment_name=experiment_name)
        experiment_id = experiment.id

    client.archive_experiment(experiment_id=experiment_id)
