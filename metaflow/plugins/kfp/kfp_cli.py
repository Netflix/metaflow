import functools
import json
import re
import shutil
import subprocess
from typing import Dict, Any

from metaflow import JSONType, current, decorators, parameters
from metaflow._vendor import click
from metaflow.exception import CommandException, MetaflowException
from metaflow.metaflow_config import (
    KFP_DEFAULT_CONTAINER_IMAGE,
    KFP_MAX_PARALLELISM,
    from_conf,
    KUBERNETES_NAMESPACE,
    ARGO_RUN_URL_PREFIX,
    METAFLOW_RUN_URL_PREFIX,
)
from metaflow.package import MetaflowPackage
from metaflow.plugins.aws.step_functions.step_functions_cli import (
    check_metadata_service_version,
)
from metaflow.plugins.kfp.argo_utils import (
    run_id_to_url,
    run_id_to_metaflow_url,
    to_metaflow_run_id,
)
from metaflow.plugins.kfp.kfp_decorator import KfpException
from metaflow.plugins.kfp.kfp_step_init import save_step_environment_variables
from metaflow.util import get_username


class IncorrectMetadataServiceVersion(MetaflowException):
    headline = "Incorrect version for metaflow service"


@click.group()
def cli():
    pass


@cli.group(name="kfp", help="Commands related to Workflow SDK.")
@click.pass_obj
def kubeflow_pipelines(obj):
    pass


@kubeflow_pipelines.command(help="Internal step command to initialize parent taskIds")
@click.option("--run-id")
@click.option("--step_name")
@click.option("--passed_in_split_indexes")
@click.option("--task_id")
@click.pass_obj
def step_init(obj, run_id, step_name, passed_in_split_indexes, task_id):
    save_step_environment_variables(
        obj.flow_datastore,
        obj.graph,
        run_id,
        step_name,
        passed_in_split_indexes,
        task_id,
        obj.logger,
    )


def common_create_run_options(func):
    @click.option(
        "--name",
        "name",
        default=None,
        help="The workflow name. The default is the flow name.",
        show_default=True,
    )
    @click.option(
        "--experiment",
        "-e",
        "experiment",
        default=None,
        help="The associated experiment name for the run. ",
        show_default=True,
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
        "--sys-tag",
        "sys_tags",
        multiple=True,
        default=None,
        help="Annotate all Metaflow objects produced by Argo Metaflow runs "
        "with the given system tag. You can specify this option multiple "
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
        "--k8s-namespace",
        "--kubernetes-namespace",
        "kubernetes_namespace",
        default=KUBERNETES_NAMESPACE,
        help="Kubernetes Namespace for your run in Argo.",
        show_default=True,
    )
    @click.option(
        "--yaml-only",
        "yaml_only",
        is_flag=True,
        default=False,
        help="Generate the Workflow YAML which is used to run the workflow on Argo.",
        show_default=True,
    )
    @click.option(
        "--yaml-format",
        "yaml_format",
        default="argo-workflow",
        type=click.Choice(["argo-workflow", "argo-workflow-template"]),
        show_default=True,
    )
    @click.option(
        "--pipeline-path",
        "pipeline_path",
        default=None,
        help="The output path of the generated Argo pipeline yaml file",
        show_default=False,
    )
    @click.option(
        "--s3-code-package/--no-s3-code-package",
        "s3_code_package",
        default=True,
        help="Whether to package the code to S3 datastore",
        show_default=True,
    )
    @click.option(
        "--base-image",
        "base_image",
        default=KFP_DEFAULT_CONTAINER_IMAGE,
        help="Base docker image used in Argo.",
        show_default=True,
    )
    @click.option(
        "--max-parallelism",
        "-m",
        default=KFP_MAX_PARALLELISM,
        show_default=True,
        help="Maximum number of parallel pods.",
    )
    @click.option(
        "--workflow-timeout",
        default=None,
        type=int,
        help="Workflow timeout in seconds.",
    )
    @click.option(
        "--notify",
        "-n",
        "notify",
        is_flag=True,
        default=bool(from_conf("METAFLOW_NOTIFY")),
        help="Whether to notify upon completion.  Default is METAFLOW_NOTIFY env variable. "
        "METAFLOW_NOTIFY_ON_SUCCESS and METAFLOW_NOTIFY_ON_ERROR env variables determine "
        "whether a notification is sent.",
        show_default=True,
    )
    @click.option(
        "--notify-on-error",
        "-noe",
        "notify_on_error",
        default=from_conf("METAFLOW_NOTIFY_ON_ERROR", default=None),
        help="Email address to notify upon error. "
        "If not set, METAFLOW_NOTIFY_ON_ERROR is used from Metaflow config or environment variable",
        show_default=True,
    )
    @click.option(
        "--notify-on-success",
        "-nos",
        "notify_on_success",
        default=from_conf("METAFLOW_NOTIFY_ON_SUCCESS", default=None),
        help="Email address to notify upon success"
        "If not set, METAFLOW_NOTIFY_ON_SUCCESS is used from Metaflow config or environment variable",
        show_default=True,
    )
    @click.option(
        "--sqs-url-on-error",
        "-su",
        "sqs_url_on_error",
        default=from_conf("METAFLOW_SQS_URL_ON_ERROR", default=None),
        help="SQS url to send messages upon error"
        "If not set, messages will NOT be sent to SQS",
        show_default=True,
    )
    @click.option(
        "--sqs-role-arn-on-error",
        "-sra",
        "sqs_role_arn_on_error",
        default=from_conf("METAFLOW_SQS_ROLE_ARN_ON_ERROR", default=None),
        help="aws iam role used for sending messages to SQS upon error"
        "If not set, the default iam role associated with the pod will be used",
        show_default=True,
    )
    @functools.wraps(func)
    def wrapper_common_options(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper_common_options


def common_wait_options(func):
    @click.option(
        "--wait-for-completion-timeout",
        "--wait-timeout",
        "-wt",
        "wait_for_completion_timeout",
        default=None,
        type=int,
        help="Completion timeout (seconds) to wait before flow exits, else TimeoutExpired is raised.",
    )
    @click.option(
        "--argo-wait",
        "-aw",
        "argo_wait",
        is_flag=True,
        default=False,
        help="Use Argo CLI watch to wait for Argo run to complete.",
        show_default=True,
    )
    @functools.wraps(func)
    def wrapper_common_options(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper_common_options


@parameters.add_custom_parameters(deploy_mode=True)
@kubeflow_pipelines.command(help="Submit this flow to run in the cluster.")
@common_create_run_options
@common_wait_options
@click.pass_obj
def run(
    obj,
    name=None,
    experiment=None,
    tags=None,
    sys_tags=None,
    user_namespace=None,
    kubernetes_namespace=KUBERNETES_NAMESPACE,
    yaml_only=False,
    yaml_format=None,
    pipeline_path=None,
    s3_code_package=True,
    base_image=None,
    max_parallelism=None,
    workflow_timeout=None,
    notify=False,
    notify_on_error=None,
    notify_on_success=None,
    sqs_url_on_error=None,
    sqs_role_arn_on_error=None,
    argo_wait=False,
    wait_for_completion_timeout=None,
    **kwargs,
):
    """
    Analogous to step_functions_cli.py
    """

    flow_parameters: Dict[str, Any] = _get_flow_parameters(kwargs, obj)

    # Add experiment as tag
    if experiment:
        tags = tags + (experiment,)

    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    check_metadata_service_version(obj)
    flow = make_flow(
        obj=obj,
        name=name if name else obj.flow.name,
        tags=tags,
        sys_tags=sys_tags,
        experiment=experiment,
        user_namespace=user_namespace,
        base_image=base_image,
        s3_code_package=s3_code_package,
        max_parallelism=max_parallelism,
        workflow_timeout=workflow_timeout,
        notify=notify,
        notify_on_error=notify_on_error,
        notify_on_success=notify_on_success,
        sqs_url_on_error=sqs_url_on_error,
        sqs_role_arn_on_error=sqs_role_arn_on_error,
    )

    if yaml_only:
        if pipeline_path is None:
            raise CommandException("Please specify --pipeline-path")

        workflow, pipeline_path = flow.create_workflow_yaml_file(
            pipeline_path, flow_parameters, yaml_format
        )
        obj.echo(f"\nDone compiling *{current.flow_name}* to {pipeline_path}")
    else:
        if s3_code_package and obj.flow_datastore.TYPE != "s3":
            raise CommandException(
                "Kubeflow Pipelines s3-code-package requires --datastore=s3."
            )

        obj.echo(
            f"Deploying *{current.flow_name}* to Argo...",
            bold=True,
        )
        workflow_manifest: Dict[str, Any] = flow.create_run_on_argo(
            kubernetes_namespace, flow_parameters
        )
        obj.echo("\nRun created successfully!\n")
        (
            argo_ui_url,
            argo_workflow_name,
            metaflow_run_id,
            metaflow_ui_url,
        ) = _echo_workflow_run(flow.name, kubernetes_namespace, obj, workflow_manifest)

        if argo_wait:
            _argo_wait(
                argo_ui_url,
                argo_workflow_name,
                kubernetes_namespace,
                metaflow_run_id,
                metaflow_ui_url,
                obj,
                wait_for_completion_timeout,
            )


def _get_flow_parameters(kwargs, obj) -> Dict[str, Any]:
    def _convert_value(param: parameters.Parameter):
        v = kwargs.get(param.name)
        return json.dumps(v) if param.kwargs.get("type") == JSONType else v

    flow_parameters = {
        param.name: _convert_value(param)
        for _, param in obj.flow._get_parameters()
        if kwargs.get(param.name) is not None
    }
    return flow_parameters


def _echo_workflow_run(
    flow_name: str, kubernetes_namespace: str, obj, workflow_manifest: Dict[str, Any]
):
    argo_workflow_name = workflow_manifest["metadata"]["name"]
    metaflow_run_id = to_metaflow_run_id(argo_workflow_name)
    metaflow_ui_url = run_id_to_metaflow_url(flow_name, argo_workflow_name)
    argo_ui_url = run_id_to_url(argo_workflow_name, kubernetes_namespace)
    # ddog_ui_url = (
    #     f"https://ai-platform.datadoghq.com/orchestration/overview/pod?query=annotation%23metaflow.org%2F"
    #     f"run_id%3A{metaflow_run_id}"
    #     f"&groups=tag%23kube_namespace%2Cannotation%23metaflow.org%2Fflow_name"
    #     "%2Cannotation%23metaflow.org%2Fstep&inspect=&inspect_group=&panel_tab=yaml&pg=0"
    # )
    # ddog_wf_url = (
    #     "https://ai-platform.datadoghq.com/dashboard/mtq-j2e-p6g"
    #     f"?tpl_var_env%5B0%5D=%2A&tpl_var_run_id%5B0%5D={metaflow_run_id}"
    # )
    obj.echo(f"Metaflow run_id=*{metaflow_run_id}*\n", fg="magenta")
    obj.echo(f"*Argo UI:* {argo_ui_url}", fg="cyan")
    obj.echo(f"*Metaflow UI:* {metaflow_ui_url}", fg="cyan")
    # obj.echo(f"*ddog dashboard:* {ddog_wf_url}", fg="cyan")
    # obj.echo(f"*ddog pod groups:* {ddog_ui_url}\n", fg="cyan")
    argo_workflow_name = workflow_manifest["metadata"]["name"]
    obj.echo(
        f"*Argo workflow:* argo -n {kubernetes_namespace} watch {argo_workflow_name}\n",
        fg="cyan",
    )
    return argo_ui_url, argo_workflow_name, metaflow_run_id, metaflow_ui_url


def _argo_wait(
    argo_ui_url,
    argo_workflow_name,
    kubernetes_namespace,
    metaflow_run_id,
    metaflow_ui_url,
    obj,
    wait_for_completion_timeout,
):
    argo_path: str = shutil.which("argo")
    argo_cmd = f"{argo_path} -n {kubernetes_namespace} "
    cmd = f"{argo_cmd} watch {argo_workflow_name}"
    subprocess.run(
        cmd,
        shell=True,
        universal_newlines=True,
        timeout=wait_for_completion_timeout,
    )
    cmd = f"{argo_cmd} get {argo_workflow_name} | grep Status | awk '{{print $2}}'"
    ret = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, encoding="utf8")
    succeeded = "Succeeded" in ret.stdout
    show_status(metaflow_run_id, argo_ui_url, metaflow_ui_url, obj.echo, succeeded)


@kubeflow_pipelines.command(help="Deploy a new version of this flow to the cluster.")
@common_create_run_options
@click.pass_obj
def create(
    obj,
    name=None,
    experiment=None,
    tags=None,
    sys_tags=None,
    user_namespace=None,
    kubernetes_namespace=KUBERNETES_NAMESPACE,
    yaml_only=False,
    yaml_format=None,
    pipeline_path=None,
    s3_code_package=True,
    base_image=None,
    max_parallelism=None,
    workflow_timeout=None,
    notify=False,
    notify_on_error=None,
    notify_on_success=None,
):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)

    check_metadata_service_version(obj)
    flow = make_flow(
        obj=obj,
        name=obj.flow.name,
        tags=tags,
        sys_tags=sys_tags,
        experiment=experiment,
        user_namespace=user_namespace,
        base_image=base_image,
        s3_code_package=s3_code_package,
        max_parallelism=max_parallelism,
        workflow_timeout=workflow_timeout,
        notify=notify,
        notify_on_error=notify_on_error,
        notify_on_success=notify_on_success,
    )

    workflow_name: str = resolve_workflow_name(name)
    if yaml_only:
        if pipeline_path is None:
            raise CommandException("Please specify --pipeline-path")

        if yaml_format != "argo-workflow-template":
            raise CommandException(
                f"create requires --yaml-format argo-workflow-template"
            )

        workflow, path = flow.create_workflow_yaml_file(
            pipeline_path, output_format=yaml_format, name=workflow_name
        )
        obj.echo(
            f"\nDone compiling *{flow.name}* to {path} with {workflow['metadata']['name']}"
        )
    else:
        obj.echo(f"Deploying *{flow.name}* to Argo Workflows...", bold=True)
        workflow_template: Dict[str, Any] = flow.deploy(
            kubernetes_namespace, workflow_name
        )
        template_name = workflow_template["metadata"]["name"]
        obj.echo(
            f"Workflow Template *{template_name}* "
            f"for flow *{current.flow_name}* pushed to "
            "Argo Workflows successfully.\n",
            bold=True,
        )
        argo_ui_url = f"{ARGO_RUN_URL_PREFIX}/argo-ui/workflow-templates/{kubernetes_namespace}/{template_name}"
        obj.echo(f"*Argo:* {argo_ui_url}", fg="cyan")

        # TODO(talebz): using @schedule decorator?
        #  flow.schedule()
        #  obj.echo("What will trigger execution of the workflow:", bold=True)
        #  obj.echo(flow.trigger_explanation(), indent=True)


@parameters.add_custom_parameters(deploy_mode=False)
@kubeflow_pipelines.command(help="Trigger the workflow on Argo Workflows.")
@click.option(
    "--name",
    "name",
    default=None,
    help="The workflow template name. The default is the flow name.",
    show_default=True,
)
@click.option(
    "--k8s-namespace",
    "--kubernetes-namespace",
    "kubernetes_namespace",
    default=KUBERNETES_NAMESPACE,
    help="Kubernetes Namespace for your run in Argo.",
    show_default=True,
)
@click.option(
    "--run-id-file",
    default=None,
    show_default=True,
    type=str,
    help="Write the ID of this run to the file specified.",
)
@common_wait_options
@click.pass_obj
def trigger(
    obj,
    name=None,
    kubernetes_namespace=KUBERNETES_NAMESPACE,
    run_id_file=None,
    argo_wait=False,
    wait_for_completion_timeout=None,
    **kwargs,
):
    from metaflow.plugins.kfp.kfp import KubeflowPipelines

    flow_parameters: Dict[str, Any] = _get_flow_parameters(kwargs, obj)
    workflow_name: str = resolve_workflow_name(name)
    workflow_manifest: Dict[str, Any] = KubeflowPipelines.trigger(
        kubernetes_namespace, workflow_name, flow_parameters
    )
    argo_workflow_name = workflow_manifest["metadata"]["name"]

    if run_id_file:
        with open(run_id_file, "w") as f:
            f.write(str(argo_workflow_name))

    (
        argo_ui_url,
        argo_workflow_name,
        metaflow_run_id,
        metaflow_ui_url,
    ) = _echo_workflow_run(
        current.flow_name, kubernetes_namespace, obj, workflow_manifest
    )

    if argo_wait:
        _argo_wait(
            argo_ui_url,
            argo_workflow_name,
            kubernetes_namespace,
            metaflow_run_id,
            metaflow_ui_url,
            obj,
            wait_for_completion_timeout,
        )


def resolve_workflow_name(name: str) -> str:
    valid_name = re.compile("^[a-z0-9]([a-z0-9\.\-]*[a-z0-9])?$")
    if name and not valid_name.search(name):
        raise MetaflowException(
            f"Name '{name}' contains invalid characters. The "
            "name must consist of lower case alphanumeric characters, '-' or '.'"
            ", and must start and end with an alphanumeric character."
        )

    workflow_name = name if name else current.flow_name

    if len(workflow_name) > 253:
        msg = (
            f"The full name of the workflow:\n*{workflow_name}*\n"
            "is longer than 253 characters.\n\n"
            "To deploy this workflow to Argo Workflows, please assign a shorter name\n"
            "using the option\n"
            "*kfp --name <name> create*." % workflow_name
        )
        raise KfpException(msg)

    if not valid_name.search(workflow_name):
        workflow_name = (
            re.compile(r"^[^A-Za-z0-9]+")
            .sub("", workflow_name)
            .replace("_", "")
            .lower()
        )

    return workflow_name


def show_status(
    run_id: str, argo_ui_url: str, metaflow_ui_url: str, echo: callable, succeeded: bool
):
    if succeeded:
        echo("\nSUCCEEDED!", fg="green")
    else:
        raise Exception(
            f"Flow: {current.flow_name}, run_id: {run_id} FAILED!\n"
            f"Argo UI: {argo_ui_url}, Metaflow UI: {metaflow_ui_url}"
        )


def make_flow(
    obj,
    name,
    tags,
    sys_tags,
    experiment,
    user_namespace,
    base_image,
    s3_code_package,
    max_parallelism,
    workflow_timeout,
    notify,
    notify_on_error,
    notify_on_success,
    sqs_url_on_error,
    sqs_role_arn_on_error,
):
    """
    Analogous to step_functions_cli.py
    """

    # Import declared inside here because this file has Python3 syntax while
    # Metaflow supports Python2 for backward compat, so only load Python3 if the KFP plugin
    # is being run.
    from metaflow.plugins.kfp.kfp import KubeflowPipelines
    from metaflow.plugins.kfp.kfp_decorator import KfpInternalDecorator

    # Attach KFP decorator to the flow
    decorators._attach_decorators(obj.flow, [KfpInternalDecorator.name])
    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )

    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.logger, obj.package_suffixes
    )

    package_url = None
    if s3_code_package:
        package_url, package_sha = obj.flow_datastore.save_data(
            [obj.package.blob], len_hint=1
        )[0]
        obj.echo(
            f"*Uploaded package to:* {package_url}",
            fg="cyan",
        )

    return KubeflowPipelines(
        name=name,
        graph=obj.graph,
        flow=obj.flow,
        code_package=obj.package,
        code_package_url=package_url,
        metadata=obj.metadata,
        flow_datastore=obj.flow_datastore,
        environment=obj.environment,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
        base_image=base_image,
        s3_code_package=s3_code_package,
        tags=tags,
        sys_tags=sys_tags,
        experiment=experiment,
        user_namespace=user_namespace,
        username=get_username(),
        max_parallelism=max_parallelism,
        workflow_timeout=workflow_timeout,
        notify=notify,
        notify_on_error=notify_on_error,
        notify_on_success=notify_on_success,
        sqs_url_on_error=sqs_url_on_error,
        sqs_role_arn_on_error=sqs_role_arn_on_error,
    )
