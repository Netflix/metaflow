from metaflow._vendor import click
from metaflow import decorators
from metaflow.util import get_username
from metaflow.package import MetaflowPackage
from metaflow.plugins import KubernetesDecorator
from .airflow_compiler import Airflow
from metaflow import S3


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Airflow.")
@click.pass_context
def airflow(ctx):
    pass


def make_flow(
    obj, tags, namespace, max_workers, is_project, file_path=None, worker_pool=None
):
    # Attach K8s decorator over here.
    # todo This will be affected in the future based on how many compute providers are supported on Airflow.
    decorators._attach_decorators(obj.flow, [KubernetesDecorator.name])
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
        obj.flow.name,
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
        max_workers=max_workers,
        worker_pool=worker_pool,
        username=get_username(),
        is_project=is_project,
        description=obj.flow.__doc__,
        file_path=file_path,
    )


@airflow.command(help="Create an airflow workflow from this metaflow workflow")
@click.argument("file_path", required=False)
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
    "--max-workers",
    default=100,
    show_default=True,
    help="Maximum number of concurrent airflow tasks to run for the DAG. ",
)
@click.option(
    "--worker-pool",
    default=None,
    show_default=True,
    help="Worker pool the for the airflow tasks."
)
@click.pass_obj
def create(
    obj,
    file_path,
    tags=None,
    user_namespace=None,
    max_workers=None,
    worker_pool=None,
):
    flow = make_flow(
        obj,
        tags,
        user_namespace,
        max_workers,
        False,
        file_path=file_path,
        worker_pool=worker_pool,
    )
    compiled_dag_file = flow.compile()
    if file_path is None:
        obj.echo_always(compiled_dag_file)
    else:
        if file_path.startswith("s3://"):
            with S3() as s3:
                s3.put(file_path, compiled_dag_file)
        else:
            with open(file_path, "w") as f:
                f.write(compiled_dag_file)
