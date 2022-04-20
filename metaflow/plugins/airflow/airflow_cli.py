from metaflow._vendor import click
from metaflow import decorators
from metaflow.util import get_username
from metaflow.package import MetaflowPackage
from metaflow.plugins import KubernetesDecorator
from .airflow_compiler import Airflow, AirflowException, NotSupportedException
from metaflow import S3
from metaflow import current
from metaflow.exception import MetaflowException

import re

VALID_NAME = re.compile("[^a-zA-Z0-9_\-\.]")


def _validate_workflow(graph, flow_datastore, metadata):
    # check for other compute related decorators.
    # supported compute : k8s (v1), local(v2), batch(v3),
    for node in graph:
        if node.type == "foreach":
            raise NotSupportedException(
                "Step *%s* is a foreach and for foreach steps are not supported with airflow."
            )

        if any([d.name == "batch" for d in node.decorators]):
            raise NotSupportedException(
                "@batch is not supported with Airflow. Use @kubernetes instead."
            )

    if metadata.TYPE != "service":
        raise AirflowException(
            'Metadata of type "service" required with `airflow create`.'
        )
    if flow_datastore.TYPE != "s3":
        raise AirflowException('Datastore of type "s3" required with `airflow create`')


def resolve_dag_name(name):

    project = current.get("project_name")
    if project:
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. " "Use --branch instead."
            )
        dag_name = current.project_flow_name
    else:
        if name and VALID_NAME.search(name):
            raise MetaflowException("Name '%s' contains invalid characters." % name)

        dag_name = name if name else current.flow_name

    return dag_name


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Airflow.")
@click.pass_context
def airflow(ctx):
    pass


def make_flow(
    obj,
    dag_name,
    tags,
    namespace,
    max_workers,
    file_path=None,
    worker_pool=None,
    set_active=False,
):
    # Validate if the workflow is correctly parsed.
    _validate_workflow(obj.graph, obj.flow_datastore, obj.metadata)
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
    flow_name = resolve_dag_name(dag_name)
    return Airflow(
        flow_name,
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
        description=obj.flow.__doc__,
        file_path=file_path,
        set_active=set_active,
    )


@airflow.command(help="Create an airflow workflow from this metaflow workflow")
@click.argument("file_path", required=True)
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
    "--name",
    default=None,
    type=str,
    help="`dag_id` of airflow DAG. The flow name is used instead "
    "if this option is not specified",
)
@click.option(
    "--is-paused-upon-creation",
    "is_paused",
    default=False,
    is_flag=True,
    help="Sets `is_paused_upon_creation=True` for the Airflow DAG. ",
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
    help="Maximum number of concurrent Airflow tasks.",
)
@click.option(
    "--worker-pool",
    default=None,
    show_default=True,
    help="Worker pool the for the airflow tasks.",
)
@click.pass_obj
def create(
    obj,
    file_path,
    tags=None,
    name=None,
    is_paused=False,
    user_namespace=None,
    max_workers=None,
    worker_pool=None,
):
    flow = make_flow(
        obj,
        name,
        tags,
        user_namespace,
        max_workers,
        file_path=file_path,
        worker_pool=worker_pool,
        set_active=not is_paused,
    )
    compiled_dag_file = flow.compile()
    with open(file_path, "w") as f:
        f.write(compiled_dag_file)
