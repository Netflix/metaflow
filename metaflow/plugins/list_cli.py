from metaflow._vendor import click
from metaflow import Flow
from metaflow.exception import MetaflowNotFound


@click.group()
def cli():
    pass


@cli.group(help="List objects pertaining to your flow.")
def list():
    pass


def _execute_cmd(echo, flow_name, num_runs):
    found = False
    counter = 1
    try:
        flow = Flow(flow_name)
    except MetaflowNotFound:
        flow = None

    if flow:
        for run in Flow(flow_name).runs():
            found = True
            if counter > num_runs:
                break
            counter += 1
            echo(
                "{created} {name} [{id}] (Successful:{status} Finished:{finished})".format(
                    created=run.created_at,
                    name=flow_name,
                    id=run.id,
                    status=run.successful,
                    finished=run.finished,
                )
            )

    if not found:
        echo("No runs found for flow: {name}".format(name=flow_name))


@click.option(
    "--num-runs",
    default=10,
    help="Number of runs to show.",
)
@list.command(help="List recent runs for your flow.")
@click.pass_context
def runs(ctx, num_runs):
    _execute_cmd(ctx.obj.echo, ctx.obj.flow.name, num_runs)
