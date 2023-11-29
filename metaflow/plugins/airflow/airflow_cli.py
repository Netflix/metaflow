import base64
import os
import re
import sys
from hashlib import sha1

from metaflow import current, decorators
from metaflow._vendor import click
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.package import MetaflowPackage
from metaflow.plugins.aws.step_functions.production_token import (
    load_token,
    new_token,
    store_token,
)
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.util import get_username, to_bytes, to_unicode

from .airflow import Airflow
from .exception import AirflowException, NotSupportedException


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


VALID_NAME = re.compile(r"[^a-zA-Z0-9_\-\.]")


def resolve_token(
    name, token_prefix, obj, authorize, given_token, generate_new_token, is_project
):
    # 1) retrieve the previous deployment, if one exists

    workflow = Airflow.get_existing_deployment(name, obj.flow_datastore)
    if workflow is None:
        obj.echo(
            "It seems this is the first time you are deploying *%s* to "
            "Airflow." % name
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
                "There is an existing version of *%s* on Airflow which was "
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
            obj.echo("    airflow create --authorize MY_TOKEN", fg="green")
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
        Airflow.save_deployment_token(get_username(), name, token, obj.flow_datastore)
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
        "Airflow, they need to call"
    )
    obj.echo("    airflow create --authorize %s" % token, fg="green")
    obj.echo("when deploying this flow to Airflow for the first time.")
    obj.echo(
        'See "Organizing Results" at https://docs.metaflow.org/ for more '
        "information about production tokens."
    )
    obj.echo("")
    store_token(token_prefix, token)

    return token


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Airflow.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Airflow DAG name. The flow name is used instead if this option is not "
    "specified",
)
@click.pass_obj
def airflow(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.dag_name, obj.token_prefix, obj.is_project = resolve_dag_name(name)


@airflow.command(help="Compile a new version of this flow to Airflow DAG.")
@click.argument("file", required=True)
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
    help="Annotate all objects produced by Airflow DAG executions "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--is-paused-upon-creation",
    default=False,
    is_flag=True,
    help="Generated Airflow DAG is paused/unpaused upon creation.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    # TODO (savin): Identify the default namespace?
    help="Change the namespace from the default to the given tag. "
    "See run --help for more information.",
)
@click.option(
    "--max-workers",
    default=100,
    show_default=True,
    help="Maximum number of parallel processes.",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Workflow timeout in seconds. Enforced only for scheduled DAGs.",
)
@click.option(
    "--worker-pool",
    default=None,
    show_default=True,
    help="Worker pool for Airflow DAG execution.",
)
@click.pass_obj
def create(
    obj,
    file,
    authorize=None,
    generate_new_token=False,
    given_token=None,
    tags=None,
    is_paused_upon_creation=False,
    user_namespace=None,
    max_workers=None,
    workflow_timeout=None,
    worker_pool=None,
):
    if os.path.abspath(sys.argv[0]) == os.path.abspath(file):
        raise MetaflowException(
            "Airflow DAG file name cannot be the same as flow file name"
        )

    # Validate if the workflow is correctly parsed.
    _validate_workflow(
        obj.flow, obj.graph, obj.flow_datastore, obj.metadata, workflow_timeout
    )

    obj.echo("Compiling *%s* to Airflow DAG..." % obj.dag_name, bold=True)
    token = resolve_token(
        obj.dag_name,
        obj.token_prefix,
        obj,
        authorize,
        given_token,
        generate_new_token,
        obj.is_project,
    )

    flow = make_flow(
        obj,
        obj.dag_name,
        token,
        tags,
        is_paused_upon_creation,
        user_namespace,
        max_workers,
        workflow_timeout,
        worker_pool,
        file,
    )
    with open(file, "w") as f:
        f.write(flow.compile())

    obj.echo(
        "DAG *{dag_name}* "
        "for flow *{name}* compiled to "
        "Airflow successfully.\n".format(dag_name=obj.dag_name, name=current.flow_name),
        bold=True,
    )


def make_flow(
    obj,
    dag_name,
    production_token,
    tags,
    is_paused_upon_creation,
    namespace,
    max_workers,
    workflow_timeout,
    worker_pool,
    file,
):
    # Attach @kubernetes.
    decorators._attach_decorators(obj.flow, [KubernetesDecorator.name])

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

    return Airflow(
        dag_name,
        obj.graph,
        obj.flow,
        package_sha,
        package_url,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        production_token,
        tags=tags,
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        worker_pool=worker_pool,
        workflow_timeout=workflow_timeout,
        description=obj.flow.__doc__,
        file_path=file,
        is_paused_upon_creation=is_paused_upon_creation,
    )


def _validate_foreach_constraints(graph):
    def traverse_graph(node, state):
        if node.type == "foreach" and node.is_inside_foreach:
            raise NotSupportedException(
                "Step *%s* is a foreach step called within a foreach step. "
                "This type of graph is currently not supported with Airflow."
                % node.name
            )

        if node.type == "foreach":
            state["foreach_stack"] = [node.name]

        if node.type in ("start", "linear", "join", "foreach"):
            if node.type == "linear" and node.is_inside_foreach:
                state["foreach_stack"].append(node.name)

            if "foreach_stack" in state and len(state["foreach_stack"]) > 2:
                raise NotSupportedException(
                    "The foreach step *%s* created by step *%s* needs to have an immediate join step. "
                    "Step *%s* is invalid since it is a linear step with a foreach. "
                    "This type of graph is currently not supported with Airflow."
                    % (
                        state["foreach_stack"][1],
                        state["foreach_stack"][0],
                        state["foreach_stack"][-1],
                    )
                )

            traverse_graph(graph[node.out_funcs[0]], state)

        elif node.type == "split":
            for func in node.out_funcs:
                traverse_graph(graph[func], state)

    traverse_graph(graph["start"], {})


def _validate_workflow(flow, graph, flow_datastore, metadata, workflow_timeout):
    seen = set()
    for var, param in flow._get_parameters():
        # Throw an exception if the parameter is specified twice.
        norm = param.name.lower()
        if norm in seen:
            raise MetaflowException(
                "Parameter *%s* is specified twice. "
                "Note that parameter names are "
                "case-insensitive." % param.name
            )
        seen.add(norm)
        if "default" not in param.kwargs:
            raise MetaflowException(
                "Parameter *%s* does not have a default value. "
                "A default value is required for parameters when deploying flows on Airflow."
                % param.name
            )
    # check for other compute related decorators.
    _validate_foreach_constraints(graph)
    for node in graph:
        if node.parallel_foreach:
            raise AirflowException(
                "Deploying flows with @parallel decorator(s) "
                "to Airflow is not supported currently."
            )
        if any([d.name == "batch" for d in node.decorators]):
            raise NotSupportedException(
                "Step *%s* is marked for execution on AWS Batch with Airflow which isn't currently supported."
                % node.name
            )
    SUPPORTED_DATASTORES = ("azure", "s3", "gs")
    if flow_datastore.TYPE not in SUPPORTED_DATASTORES:
        raise AirflowException(
            "Datastore type `%s` is not supported with `airflow create`. "
            "Please choose from datastore of type %s when calling `airflow create`"
            % (
                str(flow_datastore.TYPE),
                "or ".join(["`%s`" % x for x in SUPPORTED_DATASTORES]),
            )
        )

    schedule = flow._flow_decorators.get("schedule")
    if not schedule:
        return

    schedule = schedule[0]
    if schedule.timezone is not None:
        raise AirflowException(
            "`airflow create` does not support scheduling with `timezone`."
        )


def resolve_dag_name(name):
    project = current.get("project_name")
    is_project = False

    if project:
        is_project = True
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. " "Use --branch instead."
            )
        dag_name = current.project_flow_name
        if dag_name and VALID_NAME.search(dag_name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. Please construct a name using regex %s"
                % (dag_name, VALID_NAME.pattern)
            )
        project_branch = to_bytes(".".join((project, current.branch_name)))
        token_prefix = (
            "mfprj-%s"
            % to_unicode(base64.b32encode(sha1(project_branch).digest()))[:16]
        )
    else:
        if name and VALID_NAME.search(name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. Please construct a name using regex %s"
                % (name, VALID_NAME.pattern)
            )
        dag_name = name if name else current.flow_name
        token_prefix = dag_name
    return dag_name, token_prefix.lower(), is_project
