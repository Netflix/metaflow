import click
import json

from metaflow import current, decorators, parameters, JSONType
from metaflow.datastore.datastore import TransformableObject
from metaflow.package import MetaflowPackage
from metaflow.plugins import BatchDecorator
from .argo_workflow import ArgoWorkflow
from .argo_exception import ArgoException


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Argo Workflows.")
@click.pass_obj
def argo(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@argo.command(help="Deploy a new version of this workflow to "
                    "Argo Workflow Templates.")
@click.option("--image",
              default=None,
              help="Docker image requirement in name:version format.")
@click.option("--token",
              default=None,
              help="Authentication token to call Argo Server.")
@click.option('--namespace',
              default=None,
              help="Deploy into the specified kubernetes namespace.")
@click.option('--only-json',
              is_flag=True,
              default=False,
              help="Only print out JSON sent to Argo. Do not "
                   "deploy anything.")
@click.pass_obj
def create(obj, image, token, namespace, only_json=False):
    obj.echo("Deploying *%s* to Argo Workflow Templates..." % current.flow_name, bold=True)

    datastore = obj.datastore(obj.flow.name,
                              mode='w',
                              metadata=obj.metadata,
                              event_logger=obj.event_logger,
                              monitor=obj.monitor)
    if datastore.TYPE != 's3':
        raise ArgoException("Argo Workflows require --datastore=s3.")

    # When using conda attach AWS Batch decorator to the flow. This results in 'linux-64' libraries to be packaged.
    decorators._attach_decorators(obj.flow, [BatchDecorator.name])
    decorators._init_step_decorators(
            obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger)

    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.logger, obj.package_suffixes)
    package_url = datastore.save_data(
        obj.package.sha, TransformableObject(obj.package.blob))

    name = current.flow_name.lower()
    workflow = ArgoWorkflow(name,
                            obj.flow,
                            obj.graph,
                            obj.package,
                            package_url,
                            obj.metadata,
                            obj.datastore,
                            obj.environment,
                            obj.event_logger,
                            obj.monitor,
                            image)

    if only_json:
        obj.echo_always(workflow.to_json(), err=False, no_bold=True, nl=False)
    else:
        workflow.deploy(token, namespace)
        obj.echo("WorkflowTemplate *{name}* pushed to "
                 "Argo Workflows successfully.\n".format(name=name),
                 bold=True)


@parameters.add_custom_parameters(deploy_mode=False)
@argo.command(help="Trigger the workflow from the Argo Workflow Template.")
@click.option("--token",
              default=None,
              help="Authentication token to call Argo Server.")
@click.option('--namespace',
              default=None,
              help="Submit the Workflow in the specified kubernetes namespace.")
@click.pass_obj
def trigger(obj, token, namespace, **kwargs):
    def _convert_value(param):
        v = kwargs.get(param.name)
        return json.dumps(v) if param.kwargs.get('type') == JSONType else v

    params = {p.name: _convert_value(p)
              for _, p in obj.flow._get_parameters()
                if kwargs.get(p.name) is not None}
    name = current.flow_name.lower()
    response = ArgoWorkflow.trigger(token, namespace, name, params)
    id = response['metadata']['name']
    obj.echo("Workflow *{name}* triggered on Argo Workflows"
        "(run-id *{id}*).".format(name=name, id=id), bold=True)

@argo.command(help="List workflows on Argo Workflows.")
@click.pass_obj
@click.option("--token",
              default=None,
              help="Authentication token to call Argo Server.")
@click.option('--namespace',
              default=None,
              help="List workflows in the specified kubernetes namespace.")
@click.option("--pending", default=False, is_flag=True,
              help="List workflows in the 'Pending' state on Argo Workflows.")
@click.option("--running", default=False, is_flag=True,
              help="List workflows in the 'Running' state on Argo Workflows.")
@click.option("--succeeded", default=False, is_flag=True,
              help="List workflows in the 'Succeeded' state on Argo Workflows.")
@click.option("--failed", default=False, is_flag=True,
              help="List workflows in the 'Failed' state on Argo Workflows.")
@click.option("--error", default=False, is_flag=True,
              help="List workflows in the 'Error' state on Argo Workflows.")
def list_runs(obj, token, namespace, pending, running, succeeded, failed, error):
    states = []
    if pending:
        states.append('Pending')
    if running:
        states.append('Running')
    if succeeded:
        states.append('Succeeded')
    if failed:
        states.append('Failed')
    if error:
        states.append('Error')

    tmpl = current.flow_name.lower()
    workflows = ArgoWorkflow.list(token, namespace, tmpl, states)
    if not workflows:
        if states:
            status = ','.join(['*%s*' % s for s in states])
            obj.echo('No %s workflows for *%s* found on Argo Workflows.' % (status, tmpl))
        else:
            obj.echo('No workflows for *%s* found on Argo Workflows.' % (tmpl))
    for wf in workflows:
        if wf['status']['finishedAt']:
            obj.echo(
                "*{id}* "
                "startedAt:'{startedAt}' "
                "stoppedAt:'{finishedAt}' "
                "*{status}*".format(
                    id=wf['metadata']['name'],
                    status=wf['status']['phase'],
                    startedAt=wf['status']['startedAt'],
                    finishedAt=wf['status']['finishedAt'])
            )
        else:
            obj.echo(
                "*{id}* "
                "startedAt:'{startedAt}' "
                "*{status}*".format(
                    id=wf['metadata']['name'],
                    status=wf['status']['phase'],
                    startedAt=wf['status']['startedAt'])
            )
