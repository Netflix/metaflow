import base64
import json
import platform
import re
import sys
from hashlib import sha1

from metaflow import Run, JSONType, current, decorators, parameters
from metaflow.client.core import get_metadata
from metaflow.exception import MetaflowNotFound
from metaflow._vendor import click
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    ARGO_WORKFLOWS_UI_URL,
    KUBERNETES_NAMESPACE,
    SERVICE_VERSION_CHECK,
    UI_URL,
)
from metaflow.package import MetaflowPackage

# TODO: Move production_token to utils
from metaflow.plugins.aws.step_functions.production_token import (
    load_token,
    new_token,
    store_token,
)
from metaflow.plugins.environment_decorator import EnvironmentDecorator
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.tagging_util import validate_tags
from metaflow.util import get_username, to_bytes, to_unicode, version_parse

from .argo_workflows import ArgoWorkflows

VALID_NAME = re.compile(r"^[a-z0-9]([a-z0-9\.\-]*[a-z0-9])?$")


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


class RunIdMismatch(MetaflowException):
    headline = "Run ID mismatch"


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
    "this option is not specified.",
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
    "are processed first if Argo Workflows controller is configured to process "
    "limited number of workflows in parallel",
)
@click.option(
    "--auto-emit-argo-events/--no-auto-emit-argo-events",
    default=True,  # TODO: Default to a value from config
    show_default=True,
    help="Auto emits Argo Events when the run completes successfully.",
)
@click.option(
    "--notify-on-error/--no-notify-on-error",
    default=False,
    show_default=True,
    help="Notify if the workflow fails.",
)
@click.option(
    "--notify-on-success/--no-notify-on-success",
    default=False,
    show_default=True,
    help="Notify if the workflow succeeds.",
)
@click.option(
    "--notify-slack-webhook-url",
    default="",
    help="Slack incoming webhook url for workflow success/failure notifications.",
)
@click.option(
    "--notify-pager-duty-integration-key",
    default="",
    help="PagerDuty Events API V2 Integration key for workflow success/failure notifications.",
)
@click.option(
    "--enable-heartbeat-daemon/--no-enable-heartbeat-daemon",
    default=False,
    show_default=True,
    help="Use a daemon container to broadcast heartbeats.",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    show_default=True,
    type=str,
    help="Write the workflow name to the file specified. Used internally for Metaflow's Deployer API.",
    hidden=True,
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
    auto_emit_argo_events=False,
    notify_on_error=False,
    notify_on_success=False,
    notify_slack_webhook_url=None,
    notify_pager_duty_integration_key=None,
    enable_heartbeat_daemon=True,
    deployer_attribute_file=None,
):
    validate_tags(tags)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": obj.workflow_name,
                    "flow_name": obj.flow.name,
                    "metadata": get_metadata(),
                },
                f,
            )

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
        auto_emit_argo_events,
        notify_on_error,
        notify_on_success,
        notify_slack_webhook_url,
        notify_pager_duty_integration_key,
        enable_heartbeat_daemon,
    )

    if only_json:
        obj.echo_always(str(flow), err=False, no_bold=True)
        # TODO: Support echo-ing Argo Events Sensor template
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

        if ARGO_WORKFLOWS_UI_URL:
            obj.echo("See the deployed workflow here:", bold=True)
            argo_workflowtemplate_link = "%s/workflow-templates/%s" % (
                ARGO_WORKFLOWS_UI_URL.rstrip("/"),
                KUBERNETES_NAMESPACE,
            )
            obj.echo(
                "%s/%s\n\n" % (argo_workflowtemplate_link, obj.workflow_name),
                indent=True,
            )
        flow.schedule()
        obj.echo("What will trigger execution of the workflow:", bold=True)
        obj.echo(flow.trigger_explanation(), indent=True)

        # TODO: Print events emitted by execution of this flow

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
    elif version is not None and version_parse(version) >= version_parse("2.0.2"):
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
        # Also remove + and @ as not allowed characters, which can be part of the
        # project branch due to using email addresses as user names.
        if len(workflow_name) > 253:
            name_hash = to_unicode(
                base64.b32encode(sha1(to_bytes(workflow_name)).digest())
            )[:8].lower()
            workflow_name = "%s-%s" % (workflow_name[:242], name_hash)
            obj._is_workflow_name_modified = True
        if not VALID_NAME.search(workflow_name):
            workflow_name = sanitize_for_argo(workflow_name)
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
            workflow_name = sanitize_for_argo(workflow_name)
            obj._is_workflow_name_modified = True

    return workflow_name, token_prefix.lower(), is_project


def make_flow(
    obj,
    token,
    name,
    tags,
    namespace,
    max_workers,
    workflow_timeout,
    workflow_priority,
    auto_emit_argo_events,
    notify_on_error,
    notify_on_success,
    notify_slack_webhook_url,
    notify_pager_duty_integration_key,
    enable_heartbeat_daemon,
):
    # TODO: Make this check less specific to Amazon S3 as we introduce
    #       support for more cloud object stores.
    if obj.flow_datastore.TYPE not in ("azure", "gs", "s3"):
        raise MetaflowException(
            "Argo Workflows requires --datastore=s3 or --datastore=azure or --datastore=gs"
        )

    if (notify_on_error or notify_on_success) and not (
        notify_slack_webhook_url or notify_pager_duty_integration_key
    ):
        raise MetaflowException(
            "Notifications require specifying an incoming Slack webhook url via --notify-slack-webhook-url or "
            "PagerDuty events v2 integration key via --notify-pager-duty-integration-key.\n If you would like to set up "
            "notifications for your Slack workspace, follow the instructions at "
            "https://api.slack.com/messaging/webhooks to generate a webhook url.\n For notifications through PagerDuty, "
            "generate an integration key by following the instructions at "
            "https://support.pagerduty.com/docs/services-and-integrations#create-a-generic-events-api-integration"
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
        auto_emit_argo_events=auto_emit_argo_events,
        notify_on_error=notify_on_error,
        notify_on_success=notify_on_success,
        notify_slack_webhook_url=notify_slack_webhook_url,
        notify_pager_duty_integration_key=notify_pager_duty_integration_key,
        enable_heartbeat_daemon=enable_heartbeat_daemon,
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
@click.option(
    "--deployer-attribute-file",
    default=None,
    show_default=True,
    type=str,
    help="Write the metadata and pathspec of this run to the file specified.\nUsed internally for Metaflow's Deployer API.",
    hidden=True,
)
@click.pass_obj
def trigger(obj, run_id_file=None, deployer_attribute_file=None, **kwargs):
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

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": obj.workflow_name,
                    "metadata": get_metadata(),
                    "pathspec": "/".join((obj.flow.name, run_id)),
                },
                f,
            )

    obj.echo(
        "Workflow *{name}* triggered on Argo Workflows "
        "(run-id *{run_id}*).".format(name=obj.workflow_name, run_id=run_id),
        bold=True,
    )

    run_url = (
        "%s/%s/%s" % (UI_URL.rstrip("/"), obj.flow.name, run_id) if UI_URL else None
    )

    if run_url:
        obj.echo(
            "See the run in the UI at %s" % run_url,
            bold=True,
        )


@argo_workflows.command(help="Delete the flow on Argo Workflows.")
@click.option(
    "--authorize",
    default=None,
    type=str,
    help="Authorize the deletion with a production token",
)
@click.pass_obj
def delete(obj, authorize=None):
    def _token_instructions(flow_name, prev_user):
        obj.echo(
            "There is an existing version of *%s* on Argo Workflows which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To delete this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo("    argo-workflows delete --authorize MY_TOKEN", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    validate_token(obj.workflow_name, obj.token_prefix, authorize, _token_instructions)
    obj.echo("Deleting workflow *{name}*...".format(name=obj.workflow_name), bold=True)

    schedule_deleted, sensor_deleted, workflow_deleted = ArgoWorkflows.delete(
        obj.workflow_name
    )

    if schedule_deleted:
        obj.echo(
            "Deleting cronworkflow *{name}*...".format(name=obj.workflow_name),
            bold=True,
        )

    if sensor_deleted:
        obj.echo(
            "Deleting sensor *{name}*...".format(name=obj.workflow_name),
            bold=True,
        )

    if workflow_deleted:
        obj.echo(
            "Deleting Kubernetes resources may take a while. "
            "Deploying the flow again to Argo Workflows while the delete is in-flight will fail."
        )
        obj.echo(
            "In-flight executions will not be affected. "
            "If necessary, terminate them manually."
        )


@argo_workflows.command(help="Suspend flow execution on Argo Workflows.")
@click.option(
    "--authorize",
    default=None,
    type=str,
    help="Authorize the suspension with a production token",
)
@click.argument("run-id", required=True, type=str)
@click.pass_obj
def suspend(obj, run_id, authorize=None):
    def _token_instructions(flow_name, prev_user):
        obj.echo(
            "There is an existing version of *%s* on Argo Workflows which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To suspend this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo("    argo-workflows suspend RUN_ID --authorize MY_TOKEN", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    validate_run_id(
        obj.workflow_name, obj.token_prefix, authorize, run_id, _token_instructions
    )

    # Trim prefix from run_id
    name = run_id[5:]

    workflow_suspended = ArgoWorkflows.suspend(name)

    if workflow_suspended:
        obj.echo("Suspended execution of *%s*" % run_id)


@argo_workflows.command(help="Unsuspend flow execution on Argo Workflows.")
@click.option(
    "--authorize",
    default=None,
    type=str,
    help="Authorize the unsuspend with a production token",
)
@click.argument("run-id", required=True, type=str)
@click.pass_obj
def unsuspend(obj, run_id, authorize=None):
    def _token_instructions(flow_name, prev_user):
        obj.echo(
            "There is an existing version of *%s* on Argo Workflows which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To unsuspend this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo(
            "    argo-workflows unsuspend RUN_ID --authorize MY_TOKEN",
            fg="green",
        )
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    validate_run_id(
        obj.workflow_name, obj.token_prefix, authorize, run_id, _token_instructions
    )

    # Trim prefix from run_id
    name = run_id[5:]

    workflow_suspended = ArgoWorkflows.unsuspend(name)

    if workflow_suspended:
        obj.echo("Unsuspended execution of *%s*" % run_id)


def validate_token(name, token_prefix, authorize, instructions_fn=None):
    """
    Validate that the production token matches that of the deployed flow.
    In case both the user and token do not match, raises an error.
    Optionally outputs instructions on token usage via the provided instruction_fn(flow_name, prev_user)
    """
    # TODO: Unify this with the existing resolve_token implementation.

    # 1) retrieve the previous deployment, if one exists
    workflow = ArgoWorkflows.get_existing_deployment(name)
    if workflow is None:
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
        # NOTE: The username is visible in multiple sources, and can be set by the user.
        # Should we consider being stricter here?
        if prev_user != get_username() and authorize != prev_token:
            if instructions_fn:
                instructions_fn(flow_name=name, prev_user=prev_user)
            raise IncorrectProductionToken(
                "Try again with the correct production token."
            )

    # 3) all validations passed, store the previous token for future use
    token = prev_token

    store_token(token_prefix, token)
    return True


def get_run_object(pathspec: str):
    try:
        return Run(pathspec, _namespace_check=False)
    except MetaflowNotFound:
        return None


def get_status_considering_run_object(status, run_obj):
    remapped_status = remap_status(status)
    if remapped_status == "Running" and run_obj is None:
        return "Pending"
    return remapped_status


@argo_workflows.command(help="Fetch flow execution status on Argo Workflows.")
@click.argument("run-id", required=True, type=str)
@click.pass_obj
def status(obj, run_id):
    if not run_id.startswith("argo-"):
        raise RunIdMismatch(
            "Run IDs for flows executed through Argo Workflows begin with 'argo-'"
        )
    obj.echo(
        "Fetching status for run *{run_id}* for {flow_name} ...".format(
            run_id=run_id, flow_name=obj.flow.name
        ),
        bold=True,
    )
    # Trim prefix from run_id
    name = run_id[5:]
    status = ArgoWorkflows.get_workflow_status(obj.flow.name, name)
    run_obj = get_run_object("/".join((obj.flow.name, run_id)))
    if status is not None:
        status = get_status_considering_run_object(status, run_obj)
        obj.echo_always(status)


@argo_workflows.command(help="Terminate flow execution on Argo Workflows.")
@click.option(
    "--authorize",
    default=None,
    type=str,
    help="Authorize the termination with a production token",
)
@click.argument("run-id", required=True, type=str)
@click.pass_obj
def terminate(obj, run_id, authorize=None):
    def _token_instructions(flow_name, prev_user):
        obj.echo(
            "There is an existing version of *%s* on Argo Workflows which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To terminate this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo("    argo-workflows terminate --authorize MY_TOKEN RUN_ID", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    validate_run_id(
        obj.workflow_name, obj.token_prefix, authorize, run_id, _token_instructions
    )

    # Trim prefix from run_id
    name = run_id[5:]
    obj.echo(
        "Terminating run *{run_id}* for {flow_name} ...".format(
            run_id=run_id, flow_name=obj.flow.name
        ),
        bold=True,
    )

    terminated = ArgoWorkflows.terminate(obj.flow.name, name)
    if terminated:
        obj.echo("\nRun terminated.")


@argo_workflows.command(help="List Argo Workflow templates for the flow.")
@click.option(
    "--all",
    default=False,
    is_flag=True,
    type=bool,
    help="list all Argo Workflow Templates (not just limited to this flow)",
)
@click.pass_obj
def list_workflow_templates(obj, all=None):
    templates = ArgoWorkflows.list_templates(obj.flow.name, all)
    for template_name in templates:
        obj.echo_always(template_name)


def validate_run_id(
    workflow_name, token_prefix, authorize, run_id, instructions_fn=None
):
    """
    Validates that a run_id adheres to the Argo Workflows naming rules, and
    that it belongs to the current flow (accounting for project branch as well).
    """
    # Verify that user is trying to change an Argo workflow
    if not run_id.startswith("argo-"):
        raise RunIdMismatch(
            "Run IDs for flows executed through Argo Workflows begin with 'argo-'"
        )

    # Verify that run_id belongs to the Flow, and that branches match
    name = run_id[5:]
    workflow = ArgoWorkflows.get_execution(name)
    if workflow is None:
        raise MetaflowException("Could not find workflow *%s* on Argo Workflows" % name)

    owner, token, flow_name, branch_name, project_name = workflow

    # Verify we are operating on the correct Flow file compared to the running one.
    # Without this check, using --name could be used to run commands for arbitrary run_id's, disregarding the Flow in the file.
    if current.flow_name != flow_name:
        raise RunIdMismatch(
            "The workflow with the run_id *%s* belongs to the flow *%s*, not for the flow *%s*."
            % (run_id, flow_name, current.flow_name)
        )

    if project_name is not None:
        # Verify we are operating on the correct project.
        # Perform match with separators to avoid substrings matching
        # e.g. 'test_proj' and 'test_project' should count as a mismatch.
        project_part = "%s." % sanitize_for_argo(project_name)
        if (
            current.get("project_name") != project_name
            and project_part not in workflow_name
        ):
            raise RunIdMismatch(
                "The workflow belongs to the project *%s*. "
                "Please use the project decorator or --name to target the correct project"
                % project_name
            )

        # Verify we are operating on the correct branch.
        # Perform match with separators to avoid substrings matching.
        # e.g. 'user.tes' and 'user.test' should count as a mismatch.
        branch_part = ".%s." % sanitize_for_argo(branch_name)
        if (
            current.get("branch_name") != branch_name
            and branch_part not in workflow_name
        ):
            raise RunIdMismatch(
                "The workflow belongs to the branch *%s*. "
                "Please use --branch, --production or --name to target the correct branch"
                % branch_name
            )

    # Verify that the production tokens match. We do not want to cache the token that was used though,
    # as the operations that require run_id validation can target runs not authored from the local environment
    if authorize is None:
        authorize = load_token(token_prefix)
    elif authorize.startswith("production:"):
        authorize = authorize[11:]

    if owner != get_username() and authorize != token:
        if instructions_fn:
            instructions_fn(flow_name=name, prev_user=owner)
        raise IncorrectProductionToken("Try again with the correct production token.")

    return True


def sanitize_for_argo(text):
    """
    Sanitizes a string so it does not contain characters that are not permitted in Argo Workflow resource names.
    """
    return (
        re.compile(r"^[^A-Za-z0-9]+")
        .sub("", text)
        .replace("_", "")
        .replace("@", "")
        .replace("+", "")
        .lower()
    )


def remap_status(status):
    """
    Group similar Argo Workflow statuses together in order to have similar output to step functions statuses.
    """
    STATUS_MAP = {"Error": "Failed"}
    return STATUS_MAP.get(status, status)
