from metaflow._vendor import click
from metaflow import Flow
from metaflow.exception import MetaflowNotFound
import json


@click.group()
def cli():
    pass


@cli.group(help="List objects pertaining to your flow.")
def list():
    pass


def _fetch_runs(flow_name, num_runs):
    counter = 1
    try:
        flow = Flow(flow_name)
    except MetaflowNotFound:
        flow = None
    run_list = []
    if flow:
        for run in Flow(flow_name).runs():
            if counter > num_runs:
                break
            counter += 1
            run_list.append(
                dict(
                    created=str(run.created_at),
                    name=flow_name,
                    id=run.id,
                    status=run.successful,
                    finished=run.finished,
                )
            )
    return run_list


@click.option(
    "--num-runs",
    default=10,
    help="Number of runs to show.",
)
@click.option(
    "--as-json",
    default=False,
    is_flag=True,
    help="Print run list as a JSON object",
)
@click.option(
    "--file",
    default=None,
    help="Save the run list to file.",
)
@list.command(help="List recent runs for your flow.")
@click.pass_context
def runs(ctx, num_runs, as_json, file):
    run_list = _fetch_runs(ctx.obj.flow.name, num_runs)
    if not run_list:
        ctx.obj.echo("No runs found for flow: {name}".format(name=ctx.obj.flow.name))
        return
    if as_json:
        if file:
            with open(file, "w") as f:
                json.dump(run_list, f)
        else:
            ctx.obj.echo(json.dumps(run_list, indent=4))
