import base64
import json
import re
from hashlib import sha1

from metaflow import JSONType, current, decorators, parameters
from metaflow._vendor import click
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    FEAT_ALWAYS_UPLOAD_CODE_PACKAGE,
    SERVICE_VERSION_CHECK,
    SFN_STATE_MACHINE_PREFIX,
    SFN_COMPRESS_STATE_MACHINE,
    UI_URL,
)
from metaflow.package import MetaflowPackage
from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator
from metaflow.tagging_util import validate_tags
from metaflow.util import get_username, to_bytes, to_unicode, version_parse

from .production_token import load_token, new_token, store_token
from .step_functions import StepFunctions
from metaflow.tagging_util import validate_tags
from ..aws_utils import validate_aws_tag

VALID_NAME = re.compile(r"[^a-zA-Z0-9_\-\.]")


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


class RunIdMismatch(MetaflowException):
    headline = "Run ID mismatch"


class IncorrectMetadataServiceVersion(MetaflowException):
    headline = "Incorrect version for metaflow service"


class StepFunctionsStateMachineNameTooLong(MetaflowException):
    headline = "AWS Step Functions state machine name too long"


@click.group()
def cli():
    pass


@cli.group(help="Commands related to AWS Step Functions.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="State Machine name. The flow name is used instead "
    "if this option is not specified",
)
@click.pass_obj
def step_functions(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    (
        obj.state_machine_name,
        obj.token_prefix,
        obj.is_project,
    ) = resolve_state_machine_name(obj, name)


@step_functions.command(
    help="Deploy a new version of this workflow to " "AWS Step Functions."
)
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
    "This will move the production flow to a new "
    "namespace.",
)
@click.option(
    "--new-token",
    "given_token",
    default=None,
    help="Use the given production token for this flow. "
    "This will move the production flow to the given "
    "namespace.",
)
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
    "--aws-batch-tag",
    "aws_batch_tags",
    multiple=True,
    default=None,
    help="AWS Batch tags.",
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
    help="Only print out JSON sent to AWS Step Functions. Do not " "deploy anything.",
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
    "--log-execution-history",
    is_flag=True,
    help="Log AWS Step Functions execution history to AWS CloudWatch "
    "Logs log group.",
)
@click.option(
    "--use-distributed-map/--no-use-distributed-map",
    is_flag=True,
    help="Use AWS Step Functions Distributed Map instead of Inline Map for "
    "defining foreach tasks in Amazon State Language.",
)
@click.option(
    "--compress-state-machine/--no-compress-state-machine",
    is_flag=True,
    default=SFN_COMPRESS_STATE_MACHINE,
    help="Compress AWS Step Functions state machine to fit within the 8K limit.",
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
    aws_batch_tags=None,
    user_namespace=None,
    only_json=False,
    authorize=None,
    generate_new_token=False,
    given_token=None,
    max_workers=None,
    workflow_timeout=None,
    log_execution_history=False,
    use_distributed_map=False,
    compress_state_machine=False,
    deployer_attribute_file=None,
):
    for node in obj.graph:
        if any([d.name == "slurm" for d in node.decorators]):
            raise MetaflowException(
                "Step *%s* is marked for execution on Slurm with AWS Step Functions which isn't currently supported."
                % node.name
            )

    validate_tags(tags)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": obj.state_machine_name,
                    "flow_name": obj.flow.name,
                    "metadata": obj.metadata.metadata_str(),
                },
                f,
            )

    obj.echo(
        "Deploying *%s* to AWS Step Functions..." % obj.state_machine_name, bold=True
    )

    if SERVICE_VERSION_CHECK:
        check_metadata_service_version(obj)

    token = resolve_token(
        obj.state_machine_name,
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
        obj.state_machine_name,
        tags,
        aws_batch_tags,
        user_namespace,
        max_workers,
        workflow_timeout,
        obj.is_project,
        use_distributed_map,
        compress_state_machine,
    )

    if only_json:
        obj.echo_always(flow.to_json(), err=False, no_bold=True)
    else:
        flow.deploy(log_execution_history)
        obj.echo(
            "State Machine *{state_machine}* "
            "for flow *{name}* pushed to "
            "AWS Step Functions successfully.\n".format(
                state_machine=obj.state_machine_name, name=current.flow_name
            ),
            bold=True,
        )
        if obj._is_state_machine_name_hashed:
            obj.echo(
                "Note that the flow was deployed with a truncated name "
                "due to a length limit on AWS Step Functions. The "
                "original long name is stored in task metadata.\n"
            )
        flow.schedule()
        obj.echo("What will trigger execution of the workflow:", bold=True)
        obj.echo(flow.trigger_explanation(), indent=True)


def check_metadata_service_version(obj):
    metadata = obj.metadata
    version = metadata.version()
    if version == "local":
        return
    elif version is not None and version_parse(version) >= version_parse("2.0.2"):
        # Metaflow metadata service needs to be at least at version 2.0.2
        return
    else:
        obj.echo("")
        obj.echo(
            "You are running a version of the metaflow service "
            "that currently doesn't support AWS Step Functions. "
        )
        obj.echo(
            "For more information on how to upgrade your "
            "service to a compatible version (>= 2.0.2), visit:"
        )
        obj.echo(
            "    https://admin-docs.metaflow.org/metaflow-on-aws/operation"
            "s-guide/metaflow-service-migration-guide",
            fg="green",
        )
        obj.echo(
            "Once you have upgraded your metadata service, please "
            "re-execute your command."
        )
        raise IncorrectMetadataServiceVersion(
            "Try again with a more recent " "version of metaflow service " "(>=2.0.2)."
        )


def resolve_state_machine_name(obj, name):
    def attach_prefix(name: str):
        if SFN_STATE_MACHINE_PREFIX is not None and (
            not name.startswith(SFN_STATE_MACHINE_PREFIX)
        ):
            return SFN_STATE_MACHINE_PREFIX + "_" + name
        return name

    project = current.get("project_name")
    obj._is_state_machine_name_hashed = False
    if project:
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. " "Use --branch instead."
            )
        state_machine_name = attach_prefix(current.project_flow_name)
        project_branch = to_bytes(".".join((project, current.branch_name)))
        token_prefix = (
            "mfprj-%s"
            % to_unicode(base64.b32encode(sha1(project_branch).digest()))[:16]
        )
        is_project = True
        # AWS Step Functions has a limit of 80 chars for state machine names.
        # We truncate the state machine name if the computed name is greater
        # than 60 chars and append a hashed suffix to ensure uniqueness.
        if len(state_machine_name) > 60:
            name_hash = to_unicode(
                base64.b32encode(sha1(to_bytes(state_machine_name)).digest())
            )[:16].lower()
            state_machine_name = "%s-%s" % (state_machine_name[:60], name_hash)
            obj._is_state_machine_name_hashed = True
    else:
        if name and VALID_NAME.search(name):
            raise MetaflowException("Name '%s' contains invalid characters." % name)

        state_machine_name = attach_prefix(name if name else current.flow_name)
        token_prefix = state_machine_name
        is_project = False

        if len(state_machine_name) > 80:
            msg = (
                "The full name of the workflow:\n*%s*\nis longer than 80 "
                "characters.\n\n"
                "To deploy this workflow to AWS Step Functions, please "
                "assign a shorter name\nusing the option\n"
                "*step-functions --name <name> create*." % state_machine_name
            )
            raise StepFunctionsStateMachineNameTooLong(msg)

    return state_machine_name, token_prefix.lower(), is_project


def make_flow(
    obj,
    token,
    name,
    tags,
    aws_batch_tags,
    namespace,
    max_workers,
    workflow_timeout,
    is_project,
    use_distributed_map,
    compress_state_machine=False,
):
    if obj.flow_datastore.TYPE != "s3":
        raise MetaflowException("AWS Step Functions requires --datastore=s3.")

    # Attach AWS Batch decorator to the flow
    decorators._attach_decorators(obj.flow, [BatchDecorator.name])
    decorators._init(obj.flow)
    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )
    obj.graph = obj.flow._graph

    obj.package = MetaflowPackage(
        obj.flow,
        obj.environment,
        obj.echo,
        suffixes=obj.package_suffixes,
        flow_datastore=obj.flow_datastore if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE else None,
    )
    # This blocks until the package is created
    if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE:
        package_url = obj.package.package_url()
        package_sha = obj.package.package_sha()
    else:
        package_url, package_sha = obj.flow_datastore.save_data(
            [obj.package.blob], len_hint=1
        )[0]

    if aws_batch_tags is not None:
        batch_tags = {}
        for item in list(aws_batch_tags):
            key, value = item.split("=")
            # These are fresh AWS tags provided by the user through the CLI,
            # so we must validate them.
            validate_aws_tag(key, value)
            batch_tags[key] = value

    return StepFunctions(
        name,
        obj.graph,
        obj.flow,
        obj.package.package_metadata,
        package_sha,
        package_url,
        token,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        tags=tags,
        aws_batch_tags=batch_tags,
        namespace=namespace,
        max_workers=max_workers,
        username=get_username(),
        workflow_timeout=workflow_timeout,
        is_project=is_project,
        use_distributed_map=use_distributed_map,
        compress_state_machine=compress_state_machine,
    )


def resolve_token(
    name, token_prefix, obj, authorize, given_token, generate_new_token, is_project
):
    # 1) retrieve the previous deployment, if one exists
    workflow = StepFunctions.get_existing_deployment(name)
    if workflow is None:
        obj.echo(
            "It seems this is the first time you are deploying *%s* to "
            "AWS Step Functions." % name
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
                "There is an existing version of *%s* on AWS Step "
                "Functions which was deployed by the user "
                "*%s*." % (name, prev_user)
            )
            obj.echo(
                "To deploy a new version of this flow, you need to use "
                "the same production token that they used. "
            )
            obj.echo(
                "Please reach out to them to get the token. Once you "
                "have it, call this command:"
            )
            obj.echo("    step-functions create --authorize MY_TOKEN", fg="green")
            obj.echo(
                'See "Organizing Results" at docs.metaflow.org for more '
                "information about production tokens."
            )
            raise IncorrectProductionToken(
                "Try again with the correct " "production token."
            )

    # 3) do we need a new token or should we use the existing token?
    if given_token:
        if is_project:
            # we rely on a known prefix for @project tokens, so we can't
            # allow the user to specify a custom token with an arbitrary prefix
            raise MetaflowException(
                "--new-token is not supported for "
                "@projects. Use --generate-new-token to "
                "create a new token."
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
                    "We could not generate a new " "token. This is unexpected. "
                )
            else:
                raise MetaflowException(
                    "--generate-new-token option is not "
                    "supported after using --new-token. "
                    "Use --new-token to make a new "
                    "namespace."
                )
        obj.echo("")
        obj.echo("A new production token generated.")
    else:
        token = prev_token

    obj.echo("")
    obj.echo("The namespace of this production flow is")
    obj.echo("    production:%s" % token, fg="green")
    obj.echo(
        "To analyze results of this production flow " "add this line in your notebooks:"
    )
    obj.echo('    namespace("production:%s")' % token, fg="green")
    obj.echo(
        "If you want to authorize other people to deploy new versions "
        "of this flow to AWS Step Functions, they need to call"
    )
    obj.echo("    step-functions create --authorize %s" % token, fg="green")
    obj.echo("when deploying this flow to AWS Step Functions for the first " "time.")
    obj.echo(
        'See "Organizing Results" at https://docs.metaflow.org/ for more '
        "information about production tokens."
    )
    obj.echo("")
    store_token(token_prefix, token)
    return token


@parameters.add_custom_parameters(deploy_mode=False)
@step_functions.command(help="Trigger the workflow on AWS Step Functions.")
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

    response = StepFunctions.trigger(obj.state_machine_name, params)

    id = response["executionArn"].split(":")[-1]
    run_id = "sfn-" + id

    if run_id_file:
        with open(run_id_file, "w") as f:
            f.write(str(run_id))

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": obj.state_machine_name,
                    "metadata": obj.metadata.metadata_str(),
                    "pathspec": "/".join((obj.flow.name, run_id)),
                },
                f,
            )

    obj.echo(
        "Workflow *{name}* triggered on AWS Step Functions "
        "(run-id *{run_id}*).".format(name=obj.state_machine_name, run_id=run_id),
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


@step_functions.command(help="List all runs of the workflow on AWS Step Functions.")
@click.option(
    "--running",
    default=False,
    is_flag=True,
    help="List all runs of the workflow in RUNNING state on " "AWS Step Functions.",
)
@click.option(
    "--succeeded",
    default=False,
    is_flag=True,
    help="List all runs of the workflow in SUCCEEDED state on " "AWS Step Functions.",
)
@click.option(
    "--failed",
    default=False,
    is_flag=True,
    help="List all runs of the workflow in FAILED state on " "AWS Step Functions.",
)
@click.option(
    "--timed-out",
    default=False,
    is_flag=True,
    help="List all runs of the workflow in TIMED_OUT state on " "AWS Step Functions.",
)
@click.option(
    "--aborted",
    default=False,
    is_flag=True,
    help="List all runs of the workflow in ABORTED state on " "AWS Step Functions.",
)
@click.pass_obj
def list_runs(
    obj, running=False, succeeded=False, failed=False, timed_out=False, aborted=False
):
    states = []
    if running:
        states.append("RUNNING")
    if succeeded:
        states.append("SUCCEEDED")
    if failed:
        states.append("FAILED")
    if timed_out:
        states.append("TIMED_OUT")
    if aborted:
        states.append("ABORTED")
    executions = StepFunctions.list(obj.state_machine_name, states)
    found = False
    for execution in executions:
        found = True
        if execution.get("stopDate"):
            obj.echo(
                "*sfn-{id}* "
                "startedAt:'{startDate}' "
                "stoppedAt:'{stopDate}' "
                "*{status}*".format(
                    id=execution["name"],
                    status=execution["status"],
                    startDate=execution["startDate"].replace(microsecond=0),
                    stopDate=execution["stopDate"].replace(microsecond=0),
                )
            )
        else:
            obj.echo(
                "*sfn-{id}* "
                "startedAt:'{startDate}' "
                "*{status}*".format(
                    id=execution["name"],
                    status=execution["status"],
                    startDate=execution["startDate"].replace(microsecond=0),
                )
            )
    if not found:
        if len(states) > 0:
            status = ""
            for idx, state in enumerate(states):
                if idx == 0:
                    pass
                elif idx == len(states) - 1:
                    status += " and "
                else:
                    status += ", "
                status += "*%s*" % state
            obj.echo(
                "No %s executions for *%s* found on AWS Step Functions."
                % (status, obj.state_machine_name)
            )
        else:
            obj.echo(
                "No executions for *%s* found on AWS Step Functions."
                % (obj.state_machine_name)
            )


@step_functions.command(help="Delete a workflow")
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
            "There is an existing version of *%s* on AWS Step "
            "Functions which was deployed by the user "
            "*%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To delete this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you "
            "have it, call this command:"
        )
        obj.echo("    step-functions delete --authorize MY_TOKEN", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more '
            "information about production tokens."
        )

    validate_token(
        obj.state_machine_name, obj.token_prefix, authorize, _token_instructions
    )

    obj.echo(
        "Deleting AWS Step Functions state machine *{name}*...".format(
            name=obj.state_machine_name
        ),
        bold=True,
    )
    schedule_deleted, sfn_deleted = StepFunctions.delete(obj.state_machine_name)

    if schedule_deleted:
        obj.echo(
            "Deleting Amazon EventBridge rule *{name}* as well...".format(
                name=obj.state_machine_name
            ),
            bold=True,
        )
    if sfn_deleted:
        obj.echo(
            "Deleting the AWS Step Functions state machine may take a while. "
            "Deploying the flow again to AWS Step Functions while the delete is in-flight will fail."
        )
        obj.echo(
            "In-flight executions will not be affected. "
            "If necessary, terminate them manually."
        )


@step_functions.command(help="Terminate flow execution on Step Functions.")
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
            "There is an existing version of *%s* on AWS Step Functions which was "
            "deployed by the user *%s*." % (flow_name, prev_user)
        )
        obj.echo(
            "To terminate this flow, you need to use the same production token that they used."
        )
        obj.echo(
            "Please reach out to them to get the token. Once you have it, call "
            "this command:"
        )
        obj.echo("    step-functions terminate --authorize MY_TOKEN RUN_ID", fg="green")
        obj.echo(
            'See "Organizing Results" at docs.metaflow.org for more information '
            "about production tokens."
        )

    validate_run_id(
        obj.state_machine_name, obj.token_prefix, authorize, run_id, _token_instructions
    )

    # Trim prefix from run_id
    name = run_id[4:]
    obj.echo(
        "Terminating run *{run_id}* for {flow_name} ...".format(
            run_id=run_id, flow_name=obj.flow.name
        ),
        bold=True,
    )

    terminated = StepFunctions.terminate(obj.state_machine_name, name)
    if terminated:
        obj.echo("\nRun terminated at %s." % terminated.get("stopDate"))


def validate_run_id(
    state_machine_name, token_prefix, authorize, run_id, instructions_fn=None
):
    if not run_id.startswith("sfn-"):
        raise RunIdMismatch(
            "Run IDs for flows executed through AWS Step Functions begin with 'sfn-'"
        )

    name = run_id[4:]
    execution = StepFunctions.get_execution(state_machine_name, name)
    if execution is None:
        raise MetaflowException(
            "Could not find the execution *%s* (in RUNNING state) for the state machine *%s* on AWS Step Functions"
            % (name, state_machine_name)
        )

    _, owner, token, _ = execution

    if authorize is None:
        authorize = load_token(token_prefix)
    elif authorize.startswith("production:"):
        authorize = authorize[11:]

    if owner != get_username() and authorize != token:
        if instructions_fn:
            instructions_fn(flow_name=name, prev_user=owner)
        raise IncorrectProductionToken("Try again with the correct production token.")

    return True


def validate_token(name, token_prefix, authorize, instruction_fn=None):
    """
    Validate that the production token matches that of the deployed flow.

    In case both the user and token do not match, raises an error.
    Optionally outputs instructions on token usage via the provided instruction_fn(flow_name, prev_user)
    """
    # TODO: Unify this with the existing resolve_token implementation.

    # 1) retrieve the previous deployment, if one exists
    workflow = StepFunctions.get_existing_deployment(name)
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
            if instruction_fn:
                instruction_fn(flow_name=name, prev_user=prev_user)
            raise IncorrectProductionToken(
                "Try again with the correct production token."
            )

    # 3) all validations passed, store the previous token for future use
    token = prev_token

    store_token(token_prefix, token)
    return True
