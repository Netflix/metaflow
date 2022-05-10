from metaflow._vendor import click
from metaflow import Flow, Run
from metaflow import util
from metaflow.exception import MetaflowNotFound, CommandException
import json


@click.group()
def cli():
    pass


@cli.group(help="List objects pertaining to your flow.")
def list():
    pass


def _fetch_runs(flow_name, num_runs, namespace):
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
            tags = [t for t in run.tags]
            if namespace is None or namespace in tags:
                counter += 1
                run_list.append(
                    dict(
                        created=str(run.created_at),
                        name=flow_name,
                        id=run.id,
                        status=run.successful,
                        finished=run.finished,
                        tags=tags,
                    )
                )
    return run_list


@click.option(
    "--num-runs",
    default=10,
    type=click.IntRange(1, None),
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
    help="Save the run list to file as json.",
)
@click.option("--user", default=None, help="List runs for the given user.")
@click.option("--namespace", default=None, help="List runs for the given namespace.")
@click.option(
    "--my-runs",
    default=False,
    is_flag=True,
    help="List all my runs.",
)
@list.command(help="List recent runs for your flow.")
@click.pass_context
def runs(ctx, num_runs, as_json, file, user, my_runs, namespace):
    if user and my_runs:
        raise CommandException("--user and --my-runs are mutually exclusive.")
    if user and namespace:
        raise CommandException("--user and --namespace are mutually exclusive.")
    if my_runs and namespace:
        raise CommandException("--my-runs and --namespace are mutually exclusive.")

    if my_runs:
        namespace = "user:{}".format(util.get_username())
    if user:
        namespace = "user:{}".format(user)

    run_list = _fetch_runs(ctx.obj.flow.name, num_runs, namespace)
    if not run_list:
        ctx.obj.echo("No runs found for flow: {name}".format(name=ctx.obj.flow.name))
        return

    if file:
        with open(file, "w") as f:
            json.dump(run_list, f)
    if as_json:
        ctx.obj.echo(json.dumps(run_list, indent=4), err=False)
    else:
        for run in run_list:
            ctx.obj.echo(
                "{created} {name} [{id}] (Successful:{status} Finished:{finished})".format(
                    **run
                )
            )
