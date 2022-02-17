from metaflow._vendor import click
from metaflow import decorators
from metaflow.util import get_username
from metaflow.package import MetaflowPackage
from .airflow import Airflow


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Airflow.")
@click.pass_context
def airflow(ctx):
    pass


def make_flow(obj, name, tags, namespace, worker_pools, is_project):
    # Attach AWS Batch decorator to the flow
    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )

    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.echo, obj.package_suffixes
    )
    package_url, package_sha = obj.flow_datastore.save_data(
        [obj.package.blob], len_hint=1
    )[0]
    return Airflow(
        name,
        obj.graph,
        obj.flow,
        package_sha,
        package_url,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        tags=tags,
        namespace=namespace,
        max_workers=worker_pools,
        username=get_username(),
        is_project=is_project,
    )


@airflow.command(help="Create an airflow workflow from this metaflow workflow")
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all objects produced by AWS Step Functions runs "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
)
@click.option(
    "--only-json",
    is_flag=True,
    default=False,
    help="Only print out JSON",
)
@click.option(
    "--worker-pools",
    default=100,
    show_default=True,
)
@click.pass_obj
def create(
    obj,
    tags=None,
    user_namespace=None,
    only_json=False,
    worker_pools=None,
):
    flow = make_flow(
        obj,
        obj.state_machine_name,
        tags,
        user_namespace,
        worker_pools,
        obj.is_project,
    )
