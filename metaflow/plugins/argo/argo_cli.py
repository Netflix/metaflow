import click
import json
import re

from metaflow import current, decorators, parameters, JSONType
from metaflow.datastore.datastore import TransformableObject
from metaflow.metaflow_config import from_conf
from metaflow.package import MetaflowPackage
from metaflow.plugins import BatchDecorator
from .argo_workflow import ArgoWorkflow, dns_name
from .argo_exception import ArgoException


class JsonParam(click.ParamType):
    name = 'json'

    def convert(self, value, param, ctx):
        try:
            return json.loads(value) if value else param.default
        except json.JSONDecodeError as ex:
            self.fail('expecting a valid JSON. {}'.format(str(ex)))

JSONParam = JsonParam()


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Argo Workflows.")
@click.option('--name',
              default=None,
              type=str,
              help="Workflow Template name. The flow name is used instead "
                   "if this option is not specified.")
@click.pass_obj
def argo(obj,
         name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.workflow_template_name = resolve_workflow_template_name(name)


@argo.command(help="Deploy a new version of this workflow to "
                   "Argo Workflow Templates.")
@click.option("--image",
              default=None,
              help="Docker image requirement in name:version format.")
@click.option("--image-pull-secrets",
              type=JSONParam,
              default=[],
              help="Docker image pull secrets.")
@click.option("--env",
              type=JSONParam,
              default=[],
              help="Environment variables to be set for the workflow.")
@click.option("--env-from",
              type=JSONParam,
              default=[],
              help="Environment variables to be set for the workflow.")
@click.option("--labels",
              type=JSONParam,
              default={},
              help="Labels to attach to the workflow.")
@click.option("--annotations",
              type=JSONParam,
              default={},
              help="Annotations to attach to the workflow.")
@click.option("--token",
              default=None,
              help="Authentication token to call Argo Server.")
@click.option('--k8s-namespace',
              'k8s_namespace',
              default=None,
              help="Deploy into the specified kubernetes namespace.")
@click.option('--embedded',
              is_flag=True,
              default=False,
              help="Don't download code package into step containers. "
                   "Docker images should have a flow and all dependencies embedded.")
@click.option('--only-json',
              is_flag=True,
              default=False,
              help="Only print out JSON sent to Argo. Do not deploy anything.")
@click.pass_obj
def create(obj,
           image,
           image_pull_secrets,
           env,
           env_from,
           labels,
           annotations,
           token,
           k8s_namespace,
           embedded,
           only_json=False):
    obj.echo("Deploying *%s* to Argo Workflow Templates..." % obj.workflow_template_name,
             bold=True)

    datastore = obj.datastore(obj.flow.name,
                              mode='w',
                              metadata=obj.metadata,
                              event_logger=obj.event_logger,
                              monitor=obj.monitor)
    if datastore.TYPE != 's3':
        raise ArgoException("Argo Workflows require --datastore=s3.")

    # When using conda attach AWS Batch decorator to the flow.
    # This results in 'linux-64' libraries to be packaged.
    decorators._attach_decorators(obj.flow, [BatchDecorator.name])
    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger)

    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.logger, obj.package_suffixes)
    package_url = datastore.save_data(
        obj.package.sha, TransformableObject(obj.package.blob))

    workflow = ArgoWorkflow(obj.workflow_template_name,
                            obj.flow,
                            obj.graph,
                            obj.package,
                            package_url if not embedded else None,
                            obj.metadata,
                            obj.datastore,
                            obj.environment,
                            obj.event_logger,
                            obj.monitor,
                            image,
                            image_pull_secrets,
                            env,
                            env_from,
                            labels,
                            annotations)

    if only_json:
        obj.echo_always(workflow.to_json(), err=False, no_bold=True, nl=False)
    else:
        workflow.deploy(token, k8s_namespace)
        obj.echo("WorkflowTemplate *{name}* is pushed to Argo Workflows successfully.\n"
                 .format(name=obj.workflow_template_name),
                 bold=True)


@parameters.add_custom_parameters(deploy_mode=False)
@argo.command(help="Trigger the workflow from the Argo Workflow Template.")
@click.option("--token",
              default=None,
              help="Authentication token to call Argo Server.")
@click.option('--k8s-namespace',
              'k8s_namespace',
              default=None,
              help="Submit the Workflow in the specified kubernetes namespace.")
@click.pass_obj
def trigger(obj, token, k8s_namespace, **kwargs):
    def _convert_value(param):
        v = kwargs.get(param.name)
        return json.dumps(v) if param.kwargs.get('type') == JSONType else \
            v() if callable(v) else v

    params = {p.name: _convert_value(p)
              for _, p in obj.flow._get_parameters()
              if kwargs.get(p.name) is not None}
    response = ArgoWorkflow.trigger(token, k8s_namespace, obj.workflow_template_name, params)
    id = response['metadata']['name']
    obj.echo("Workflow *{name}* is triggered on Argo Workflows (run-id *{id}*)."
             .format(name=obj.workflow_template_name, id=id),
             bold=True)


@argo.command(help="List workflows on Argo Workflows.")
@click.pass_obj
@click.option("--token",
              default=None,
              help="Authentication token to call Argo Server.")
@click.option('--k8s-namespace',
              'k8s_namespace',
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
def list_runs(obj, token, k8s_namespace, pending, running, succeeded, failed, error):
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

    workflows = ArgoWorkflow.list(token, k8s_namespace, obj.workflow_template_name, states)
    if not workflows:
        if states:
            status = ','.join(['*%s*' % s for s in states])
            obj.echo('No %s workflows for *%s* found on Argo Workflows.' % \
                     (status, obj.workflow_template_name))
        else:
            obj.echo('No workflows for *%s* found on Argo Workflows.' % \
                     (obj.workflow_template_name))
        return
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


def resolve_workflow_template_name(name):
    """
    Returns a valid workflow template name.
    """
    if name is None:
        name = dns_name(current.flow_name)

    prefix = from_conf("ARGO_WORKFLOW_PREFIX")
    if prefix:
        name = prefix + '-' + name

    if not re.match('^[a-z0-9]([-.a-z0-9]*[a-z0-9])?$', name):
        raise ArgoException("Invalid workflow template name: *%s*.\n"
                            "See https://kubernetes.io/docs/concepts/overview/working-with-objects/names/" % \
                            name)
    return name
