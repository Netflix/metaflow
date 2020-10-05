import click

from metaflow import current
from metaflow.datastore.datastore import TransformableObject
from metaflow.package import MetaflowPackage
from metaflow.exception import MetaflowException
from .argo_workflow import ArgoWorkflow


@click.group()
def cli():
    pass


@cli.group(help="Commands related to MLF Argo Workflows.")
@click.pass_obj
def argo(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@argo.command(help="Generate yaml for an MLF argo workflow.")
@click.option('--only-yaml',
              is_flag=True,
              default=False,
              help="Only print out YAML sent to MLF Argo Workflows.. Do not "
                   "deploy anything.")
@click.option(
    "--image", help="Docker image requirement in name:version format."
)
@click.pass_obj
def create(obj, only_yaml=False, image="python:alpine"):
    name = current.flow_name
    obj.echo("Deploying *%s* to MLF Argo Workflows..." % name, bold=True)

    datastore = obj.datastore(obj.flow.name,
                              mode='w',
                              metadata=obj.metadata,
                              event_logger=obj.event_logger,
                              monitor=obj.monitor)
    if datastore.TYPE != 's3':
        raise MetaflowException("AWS Step Functions requires --datastore=s3.")

    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.logger, obj.package_suffixes)
    package_url = datastore.save_data(
        obj.package.sha, TransformableObject(obj.package.blob))

    flow = ArgoWorkflow(name.lower(),
                        obj.graph,
                        obj.package,
                        package_url,
                        obj.metadata,
                        obj.datastore,
                        obj.environment,
                        obj.event_logger,
                        obj.monitor,
                        image)

    if only_yaml:
        obj.echo_always(flow.to_yaml(), err=False, no_bold=True, nl=False)
