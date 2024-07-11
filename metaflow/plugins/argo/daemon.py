from collections import namedtuple
from time import sleep
from metaflow.metaflow_config import DEFAULT_METADATA
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.plugins import METADATA_PROVIDERS
from metaflow._vendor import click


class CliState:
    pass


@click.group()
@click.option("--flow_name", required=True)
@click.option("--run_id", required=True)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all objects produced by Argo Workflows runs "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.pass_context
def cli(ctx, flow_name, run_id, tags=None):
    ctx.obj = CliState()
    ctx.obj.flow_name = flow_name
    ctx.obj.run_id = run_id
    ctx.obj.tags = tags
    # Use a dummy flow to initialize the environment and metadata service,
    # as we only need a name for the flow object.
    flow = namedtuple("DummyFlow", "name")
    dummyflow = flow(flow_name)

    # Initialize a proper metadata service instance
    environment = MetaflowEnvironment(dummyflow)

    ctx.obj.metadata = [m for m in METADATA_PROVIDERS if m.TYPE == DEFAULT_METADATA][0](
        environment, dummyflow, None, None
    )


@cli.command(help="start heartbeat process for a run")
@click.pass_obj
def heartbeat(obj):
    # Try to register a run in case the start task has not taken care of it yet.
    obj.metadata.register_run_id(obj.run_id, obj.tags)
    # Start run heartbeat
    obj.metadata.start_run_heartbeat(obj.flow_name, obj.run_id)
    # Keepalive loop
    while True:
        # Do not pollute daemon logs with anything unnecessary,
        # as they might be extremely long running.
        sleep(10)


if __name__ == "__main__":
    cli()
