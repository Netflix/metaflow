import click

from metaflow import current, decorators, parameters, JSONType
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.datastore.datastore import TransformableObject
from metaflow.package import MetaflowPackage
from metaflow.plugins import BatchDecorator
from metaflow.util import get_username

from .step_functions import StepFunctions
from .production_token import load_token, store_token, new_token

class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"

@click.group()
def cli():
    pass

@cli.group(help="Commands related to AWS Step Functions.")
@click.pass_obj
def step_functions(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)

@step_functions.command(help="Deploy a new version of this workflow to "
                    "AWS Step Functions.")
@click.option('--authorize',
              default=None,
              help="Authorize using this production token. You need this "
                   "when you are re-deploying an existing flow for the first "
                   "time. The token is cached in METAFLOW_HOME, so you only "
                   "need to specify this once.")
@click.option('--generate-new-token',
              is_flag=True,
              help="Generate a new production token for this flow. "
                   "This will move the production flow to a new "
                   "namespace.")
@click.option('--new-token',
              'given_token',
              default=None,
              help="Use the given production token for this flow. "
                   "This will move the production flow to the given "
                   "namespace.")
@click.option('--tag',
              'tags',
              multiple=True,
              default=None,
              help="Annotate all objects produced by AWS Step Functions runs "
                   "with the given tag. You can specify this option multiple "
                   "times to attach multiple tags.")
@click.option('--namespace',
              'user_namespace',
              default=None,
              help="Change the namespace from the default (production token) "
                   "to the given tag. See run --help for more information.")
@click.option('--only-json',
              is_flag=True,
              default=False,
              help="Only print out JSON sent to AWS Step Functions. Do not "
                   "deploy anything.")
@click.option('--max-workers',
              default=100,
              show_default=True,
              help="Maximum number of parallel processes.")
@click.pass_obj
def create(obj,
           tags=None,
           user_namespace=None,
           only_json=False,
           authorize=None,
           generate_new_token=False,
           given_token=None,
           max_workers=None,
           enable_debug_logs=None):

    name = current.flow_name
    token_prefix = name.lower()

    obj.echo("Deploying *%s* to AWS Step Functions..." % name, bold=True)

    token = resolve_token(name,
                          token_prefix,
                          obj,
                          authorize,
                          given_token,
                          generate_new_token)

    flow = make_flow(obj,
                     token,
                     name,
                     tags,
                     user_namespace,
                     max_workers)

    if only_json:
        obj.echo_always(flow.to_json(), err=False, no_bold=True)
    else:
        #obj.echo_always(flow.to_json(), err=False, no_bold=True)
        flow.deploy()
        obj.echo("Workflow *{name}* pushed to "
                 "AWS Step Functions successfully.\n".format(name=name), 
                 bold=True)

        flow.schedule()
        obj.echo("What will trigger execution of the workflow:", bold=True)
        obj.echo(flow.trigger_explanation(), indent=True)
        

def make_flow(obj,
              token,
              name,
              tags,
              namespace,
              max_workers):
    datastore = obj.datastore(obj.flow.name,
                              mode='w',
                              metadata=obj.metadata,
                              event_logger=obj.event_logger,
                              monitor=obj.monitor)
    if datastore.TYPE != 's3':
        raise MetaflowException("AWS Step Functions requires --datastore=s3.")

    # Attach AWS Batch decorator to the flow
    decorators._attach_decorators(obj.flow, [BatchDecorator.name])
    decorators._init_decorators(
            obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger)

    obj.package = MetaflowPackage(obj.flow, obj.environment, obj.logger, obj.package_suffixes)
    package_url = datastore.save_data(obj.package.sha, TransformableObject(obj.package.blob))
    return StepFunctions(name,
                         obj.graph,
                         obj.flow,
                         obj.package,
                         package_url,
                         token,
                         obj.metadata,
                         obj.datastore,
                         obj.environment,
                         obj.event_logger,
                         obj.monitor,
                         tags=tags,
                         namespace=namespace,
                         max_workers=max_workers,
                         username=get_username())

def resolve_token(name,
                  token_prefix,
                  obj,
                  authorize,
                  given_token,
                  generate_new_token):

    # 1) retrieve the previous deployment, if one exists
    workflow = StepFunctions.get_existing_deployment(name)
    if workflow is None:
        obj.echo("It seems this is the first time you are deploying *%s* to "
                 "AWS Step Functions." % name)
        prev_token = None
    else:
        prev_user, prev_token = workflow

    # 2) authorize this deployment
    if prev_token is not None:
        if authorize is None:
            authorize = load_token(token_prefix)
        elif authorize.startswith('production:'):
            authorize = authorize[11:]

        # we allow the user who deployed the previous version to re-deploy,
        # even if they don't have the token
        if prev_user != get_username() and authorize != prev_token:
            obj.echo("There is an existing version of *%s* on AWS Step Functions which was "
                     "deployed by the user *%s*." % (name, prev_user))
            obj.echo("To deploy a new version of this flow, you need to use "
                     "the same production token that they used. ")
            obj.echo("Please reach out to them to get the token. Once you "
                     "have it, call this command:")
            obj.echo("    step-functions create --authorize MY_TOKEN", fg='green')
            obj.echo('See "Organizing Results" at docs.metaflow.org for more '
                     "information about production tokens.")
            raise IncorrectProductionToken("Try again with the correct "
                                           "production token.")

    # 3) do we need a new token or should we use the existing token?
    if given_token:
        if given_token.startswith('production:'):
            given_token = given_token[11:]
        token = given_token
        obj.echo('')
        obj.echo("Using the given token, *%s*." % token)
    elif prev_token is None or generate_new_token:
        token = new_token(token_prefix, prev_token)
        if token is None:
            if prev_token is None:
                raise MetaflowInternalError("We could not generate a new "
                                            "token. This is unexpected. ")
            else:
                raise MetaflowException("--generate-new-token option is not "
                                        "supported after using --new-token. "
                                        "Use --new-token to make a new "
                                        "namespace.")
        obj.echo('')
        obj.echo("A new production token generated.")
    else:
        token = prev_token

    obj.echo('')
    obj.echo("The namespace of this production flow is")
    obj.echo('    production:%s' % token, fg='green')
    obj.echo("To analyze results of this production flow "
             "add this line in your notebooks:")
    obj.echo('    namespace(\"production:%s\")' % token, fg='green')
    obj.echo("If you want to authorize other people to deploy new versions "
             "of this flow to AWS Step Functions, they need to call")
    obj.echo("    step-functions create --authorize %s" % token, fg='green')
    obj.echo("when deploying this flow to AWS Step Functions for the first time.")
    obj.echo('See "Organizing Results" at https://docs.metaflow.org/ for more '
             "information about production tokens.")
    obj.echo('')
    store_token(token_prefix, token)
    return token


@parameters.add_custom_parameters
@step_functions.command(help="Trigger the workflow on AWS Step Functions.")
@click.pass_obj
def trigger(obj, **kwargs):
    def _convert_value(param):
        v = kwargs.get(param.name)
        return json.dumps(v) if param.kwargs.get('type') == JSONType else v

    params = {param.name: _convert_value(param)
              for _, param in obj.flow._get_parameters()}
    response = StepFunctions.trigger(current.flow_name, params)

    name = current.flow_name
    id = response['executionArn'].split(':')[-1]
    obj.echo("Workflow *{name}* triggered on AWS Step Functions "
        "(run-id *{id}*).".format(name=name, id=id), bold=True)

