import base64
import json
import platform
import re
import sys
from distutils.version import LooseVersion
from hashlib import sha1

from metaflow import JSONType, current, decorators, parameters
from metaflow._vendor import click
from metaflow.metaflow_config import SERVICE_VERSION_CHECK
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.package import MetaflowPackage
from metaflow.plugins import EnvironmentDecorator, KubernetesDecorator

# TODO: Move production_token to utils
from metaflow.plugins.aws.step_functions.production_token import (
    load_token,
    new_token,
    store_token,
)
from metaflow.util import get_username, to_bytes, to_unicode
from metaflow.tagging_util import validate_tags

from .argo_workflows import ArgoWorkflows

VALID_NAME = re.compile("^[a-z0-9]([a-z0-9\.\-]*[a-z0-9])?$")


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


class IncorrectMetadataServiceVersion(MetaflowException):
    headline = "Incorrect version for metaflow service"


class ArgoWorkflowsNameTooLong(MetaflowException):
    headline = "Argo Workflows name too long"


class UnsupportedPythonVersion(MetaflowException):
    headline = "Unsupported version of Python"


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Argo Workflows.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Argo Workflow name. The flow name is used instead if "
    "this option is not specified",
)
@click.pass_obj
def argo_workflows(obj, name=None):
    check_python_version(obj)
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    (
        obj.workflow_name,
        obj.token_prefix,
        obj.is_project,
    ) = resolve_workflow_name(obj, name)


@argo_workflows.command(help="Deploy a new version of this workflow to Argo Workflows.")
@click.option(
    "--authorize",
    default=None,
    help="Authorize using this production token. You need this "
    "when you are re-deploying an existing flow for the first "
    "time. The token is cached in METAFLOW_HOME, so you only "
    "need to specify this once.",
)
@click.option(
    "--generate-new-token",
    is_flag=True,
    help="Generate a new production token for this flow. "
    "This will move the production flow to a new namespace.",
)
@click.option(
    "--new-token",
    "given_token",
    default=None,
    help="Use the given production token for this flow. "
    "This will move the production flow to the given namespace.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all objects produced by Argo Workflows runs "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Change the namespace from the default (production token) "
    "to the given tag. See run --help for more information.",
)
@click.option(
    "--only-json",
    is_flag=True,
    default=False,
    help="Only print out JSON sent to Argo Workflows. Do not deploy anything.",
)
@click.option(
    "--max-workers",
    default=100,
    show_default=True,
    help="Maximum number of parallel processes.",
)
@click.option(
    "--workflow-timeout", default=None, type=int, help="Workflow timeout in seconds."
)
@click.option(
    "--workflow-priority",
    default=None,
    type=int,
    help="Workflow priority as an integer. Workflows with higher priority "
    "are processed first if Argo Workflows controller is configured to process limited "
    "number of workflows in parallel",
)
@click.pass_obj
def create(
    obj,
    tags=None,
    user_namespace=None,
    only_json=False,
    authorize=None,
    generate_new_token=False,
    given_token=None,
    max_workers=None,
    workflow_timeout=None,
    workflow_priority=None,
):
    validate_tags(tags)

    obj.echo("Deploying *%s* to Argo Workflows..." % obj.workflow_name, bold=True)

    if SERVICE_VERSION_CHECK:
        # TODO: Consider dispelling with this check since it's been 2 years since the
        #       needed metadata service changes have been available in open-source. It's
        #       likely that Metaflow users may not have access to metadata service from
        #       within their workstations.
        check_metadata_service_version(obj)

    token = resolve_token(
        obj.workflow_name,
        obj.token_prefix,
        obj,
        authorize,
        given_token,
        generate_new_token,
        obj.is_project,
    )

    flow = make_flow(
        obj,
        token,
        obj.workflow_name,
        tags,
        user_namespace,
        max_workers,
        workflow_timeout,
        workflow_priority,
    )

    if only_json:
        obj.echo_always(str(flow), err=False, no_bold=True)
    else:
        flow.deploy()
        obj.echo(
            "Workflow *{workflow_name}* "
            "for flow *{name}* pushed to "
            "Argo Workflows successfully.\n".format(
                workflow_name=obj.workflow_name, name=current.flow_name
            ),
            bold=True,
        )
        if obj._is_workflow_name_modified:
            obj.echo(
                "Note that the flow was deployed with a modified name "
                "due to Kubernetes naming conventions\non Argo Workflows. The "
                "original flow name is stored in the workflow annotation.\n"
            )
        flow.schedule()
        obj.echo("What will trigger execution of the workflow:", bold=True)
        obj.echo(flow.trigger_explanation(), indent=True)

        # response = ArgoWorkflows.trigger(obj.workflow_name)
        # run_id = "argo-" + response["metadata"]["name"]

        # obj.echo(
        #     "Workflow *{name}* triggered on Argo Workflows "
        #     "(run-id *{run_id}*).".format(name=obj.workflow_name, run_id=run_id),
        #     bold=True,
        # )


def check_python_version(obj):
    # argo-workflows integration for Metaflow isn't supported for Py versions below 3.5.
    # This constraint can very well be lifted if desired.
    if sys.version_info < (3, 5):
        obj.echo("")
        obj.echo(
            "Metaflow doesn't support Argo Workflows for Python %s right now."
            % platform.python_version()
        )
        obj.echo(
            "Please upgrade your Python interpreter to version 3.5 (or higher) or "
            "reach out to us at slack.outerbounds.co for more help."
        )
        raise UnsupportedPythonVersion(
            "Try again with a more recent version of Python (>=3.5)."
        )


def check_metadata_service_version(obj):
    metadata = obj.metadata
    version = metadata.version()
    if version == "local":
        return
    elif version is not None and LooseVersion(version) >= LooseVersion("2.0.2"):
        # Metaflow metadata service needs to be at least at version 2.0.2
        # since prior versions did not support strings as object ids.
        return
    else:
        obj.echo("")
        obj.echo(
            "You are running a version of the metaflow service that currently doesn't "
            "support Argo Workflows. "
        )
        obj.echo(
            "For more information on how to upgrade your service to a compatible "
            "version (>= 2.0.2), visit:"
        )
        obj.echo(
            "    https://admin-docs.metaflow.org/metaflow-on-aws/operation"
            "s-guide/metaflow-service-migration-guide",
            fg="green",
        )
        obj.echo(
            "Once you have upgraded your metadata service, please re-execute your "
            "command."
        )
        raise IncorrectMetadataServiceVersion(
            "Try again with a more recent version of metaflow service (>=2.0.2)."
        )


def resolve_workflow_name(obj, name):
    project = current.get("project_name")
    obj._is_workflow_name_modified = False
    if project:
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. Use --branch instead."
            )
        workflow_name = current.project_flow_name
        project_branch = to_bytes(".".join((project, current.branch_name)))
        token_prefix = (
            "mfprj-%s"
            % to_unicode(base64.b32encode(sha1(project_branch).digest()))[:16]
        )
        is_project = True
        # Argo Workflow names can't be longer than 253 characters, so we truncate
        # by default. Also, while project and branch allow for underscores, Argo
        # Workflows doesn't (DNS Subdomain names as defined in RFC 1123) - so we will
        # remove any underscores as well as convert the name to lower case.
        if len(workflow_name) > 253:
            name_hash = to_unicode(
                base64.b32encode(sha1(to_bytes(workflow_name)).digest())
            )[:8].lower()
            workflow_name = "%s-%s" % (workflow_name[:242], name_hash)
            obj._is_workflow_name_modified = True
        if not VALID_NAME.search(workflow_name):
            workflow_name = (
                re.compile(r"^[^A-Za-z0-9]+")
                .sub("", workflow_name)
                .replace("_", "")
                .lower()
            )
            obj._is_workflow_name_modified = True
    else:
        if name and not VALID_NAME.search(name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. The "
                "name must consist of lower case alphanumeric characters, '-' or '.'"
                ", and must start and end with an alphanumeric character." % name
            )

        workflow_name = name if name else current.flow_name
        token_prefix = workflow_name
        is_project = False

        if len(workflow_name) > 253:
            msg = (
                "The full name of the workflow:\n*%s*\nis longer than 253 "
                "characters.\n\n"
                "To deploy this workflow to Argo Workflows, please "
                "assign a shorter name\nusing the option\n"
                "*argo-workflows --name <name> create*." % workflow_name
            )
            raise ArgoWorkflowsNameTooLong(msg)

        if not VALID_NAME.search(workflow_name):
            workflow_name = (
                re.compile(r"^[^A-Za-z0-9]+")
                .sub("", workflow_name)
                .replace("_", "")
                .lower()
            )
            obj._is_workflow_name_modified = True

    return workflow_name, token_prefix.lower(), is_project


def make_flow(
    obj, token, name, tags, namespace, max_workers, workflow_timeout, workflow_priority
):
    # TODO: Make this check less specific to Amazon S3 as we introduce
    #       support for more cloud object stores.
    if obj.flow_datastore.TYPE not in ("azure", "gs", "s3"):
        raise MetaflowException(
            "Argo Workflows requires --datastore=s3 or --datastore=azure or --datastore=gs"
        )

    # Attach @kubernetes and @environment decorator to the flow to
    # ensure that the related decorator hooks are invoked.
    decorators._attach_decorators(
        obj.flow, [KubernetesDecorator.name, EnvironmentDecorator.name]
    )

    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )

    # Save the code package in the flow datastore so that both user code and
    # metaflow package can be retrieved during workflow execution.
    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.echo, obj.package_suffixes
    )
    package_url, package_sha = obj.flow_datastore.save_data(
        [obj.package.blob], len_hint=1
    )[0]

    return ArgoWorkflows(
        name,
        obj.graph,
        obj.flow,
        package_sha,
        package_url,
        token,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        tags=tags,
        namespace=namespace,
        max_workers=max_workers,
        username=get_username(),
        workflow_timeout=workflow_timeout,
        workflow_priority=workflow_priority,
    )


# TODO: Unify this method with the one in step_functions_cli.py
def resolve_token(
    name, token_prefix, obj, authorize, given_token, generate_new_token, is_project
):
    # 1) retrieve the previous deployment, if one exists
    workflow = ArgoWorkflows.get_existing_deployment(name)
    if workflow is None:
        obj.echo(
            "It seems this is the first time you are deploying *%s* to "
            "Argo Workflows." % name
        )
        prev_token = None
    else:
        prev_user, prev_token = workflow

    # 2) authorize this deployment
    if prev_token is not None:
        if authorize is None:
            authorize = load_token(token_prefix)
        elif authorize.startswith("production:"):
            authorize = authorize[11:]

        # we allow the user who deployed the previous version to re-deploy,
        # even if they don't have the token
        if prev_user != get_username() and authorize != prev_token:
            obj.echo(
                "There is an existing version of *%s* on Argo Workflows which was "
                "deployed by the user *%s*." % (name, prev_user)
            )
            obj.echo(
                "To deploy a new version of this flow, you need to use the same "
                "production token that they used. "
            )
            obj.echo(
                "Please reach out to them to get the token. Once you have it, call "
                "this command:"
            )
            obj.echo("    argo-workflows create --authorize MY_TOKEN", fg="green")
            obj.echo(
                'See "Organizing Results" at docs.metaflow.org for more information '
                "about production tokens."
            )
            raise IncorrectProductionToken(
                "Try again with the correct production token."
            )

    # 3) do we need a new token or should we use the existing token?
    if given_token:
        if is_project:
            # we rely on a known prefix for @project tokens, so we can't
            # allow the user to specify a custom token with an arbitrary prefix
            raise MetaflowException(
                "--new-token is not supported for @projects. Use --generate-new-token "
                "to create a new token."
            )
        if given_token.startswith("production:"):
            given_token = given_token[11:]
        token = given_token
        obj.echo("")
        obj.echo("Using the given token, *%s*." % token)
    elif prev_token is None or generate_new_token:
        token = new_token(token_prefix, prev_token)
        if token is None:
            if prev_token is None:
                raise MetaflowInternalError(
                    "We could not generate a new token. This is unexpected. "
                )
            else:
                raise MetaflowException(
                    "--generate-new-token option is not supported after using "
                    "--new-token. Use --new-token to make a new namespace."
                )
        obj.echo("")
        obj.echo("A new production token generated.")
    else:
        token = prev_token

    obj.echo("")
    obj.echo("The namespace of this production flow is")
    obj.echo("    production:%s" % token, fg="green")
    obj.echo(
        "To analyze results of this production flow add this line in your notebooks:"
    )
    obj.echo('    namespace("production:%s")' % token, fg="green")
    obj.echo(
        "If you want to authorize other people to deploy new versions of this flow to "
        "Argo Workflows, they need to call"
    )
    obj.echo("    argo-workflows create --authorize %s" % token, fg="green")
    obj.echo("when deploying this flow to Argo Workflows for the first time.")
    obj.echo(
        'See "Organizing Results" at https://docs.metaflow.org/ for more '
        "information about production tokens."
    )
    obj.echo("")
    store_token(token_prefix, token)
    return token


@parameters.add_custom_parameters(deploy_mode=False)
@argo_workflows.command(help="Trigger the workflow on Argo Workflows.")
@click.option(
    "--run-id-file",
    default=None,
    show_default=True,
    type=str,
    help="Write the ID of this run to the file specified.",
)
@click.pass_obj
def trigger(obj, run_id_file=None, **kwargs):
    def _convert_value(param):
        # Swap `-` with `_` in parameter name to match click's behavior
        val = kwargs.get(param.name.replace("-", "_").lower())
        if param.kwargs.get("type") == JSONType:
            val = json.dumps(val)
        elif isinstance(val, parameters.DelayedEvaluationParameter):
            val = val(return_str=True)
        return val

    params = {
        param.name: _convert_value(param)
        for _, param in obj.flow._get_parameters()
        if kwargs.get(param.name.replace("-", "_").lower()) is not None
    }

    response = ArgoWorkflows.trigger(obj.workflow_name, params)
    run_id = "argo-" + response["metadata"]["name"]

    if run_id_file:
        with open(run_id_file, "w") as f:
            f.write(str(run_id))

    obj.echo(
        "Workflow *{name}* triggered on Argo Workflows "
        "(run-id *{run_id}*).".format(name=obj.workflow_name, run_id=run_id),
        bold=True,
    )
