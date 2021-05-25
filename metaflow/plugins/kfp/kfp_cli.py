import json
import posixpath
import shutil
import subprocess

import click

from metaflow import current, decorators, parameters, JSONType
from metaflow.datastore.datastore import TransformableObject
from metaflow.exception import CommandException, MetaflowException
from metaflow.metaflow_config import (
    KFP_RUN_URL_PREFIX,
    KFP_SDK_API_NAMESPACE,
    KFP_SDK_NAMESPACE,
    KFP_USER_DOMAIN,
    KFP_MAX_PARALLELISM,
    from_conf,
)
from metaflow.package import MetaflowPackage
from metaflow.plugins.aws.step_functions.step_functions_cli import (
    check_metadata_service_version,
)
from metaflow.plugins.kfp.kfp_constants import BASE_IMAGE
from metaflow.plugins.kfp.kfp_step_init import save_step_environment_variables
from metaflow.util import get_username


class IncorrectMetadataServiceVersion(MetaflowException):
    headline = "Incorrect version for metaflow service"


@click.group()
def cli():
    pass


@cli.group(name="kfp", help="Commands related to Kubeflow Pipelines.")
@click.pass_obj
def kubeflow_pipelines(obj):
    pass


@kubeflow_pipelines.command(
    help="Internal KFP step command to initialize parent taskIds"
)
@click.option("--run-id")
@click.option("--step_name")
@click.option("--passed_in_split_indexes")
@click.option("--task_id")
@click.pass_obj
def step_init(obj, run_id, step_name, passed_in_split_indexes, task_id):
    save_step_environment_variables(
        obj.datastore,
        obj.graph,
        run_id,
        step_name,
        passed_in_split_indexes,
        task_id,
        obj.logger,
    )


@parameters.add_custom_parameters(deploy_mode=True)
@kubeflow_pipelines.command(
    help="Deploy a new version of this workflow to Kubeflow Pipelines."
)
@click.option(
    "--experiment-name",
    "experiment",
    default=None,
    help="Deprecated. Please use --experiment option."
    "Default of None uses KFP 'default' experiment",
    show_default=True,
)
@click.option(
    "--experiment",
    "-e",
    "experiment",
    default=None,
    help="The associated experiment name for the run. "
    "Default of None uses KFP 'default' experiment",
    show_default=True,
)
@click.option(
    "--run-name",
    "run_name",
    default=None,
    help="Name assigned to the new KFP run. If not assigned None is sent to KFP",
    show_default=True,
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all Metaflow objects produced by KFP Metaflow runs "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--namespace",
    "namespace",
    default=None,
    help="Metaflow read namespace.",
    show_default=True,
)
@click.option(
    "--kfp-namespace",
    "kfp_namespace",
    default=KFP_SDK_NAMESPACE,
    help="Namespace of your run in KFP.",
    show_default=True,
)
@click.option(
    "--api-namespace",
    "api_namespace",
    default=KFP_SDK_API_NAMESPACE,
    help="Namespace where the API service is run.",
    show_default=True,
)
@click.option(
    "--yaml-only",
    "yaml_only",
    is_flag=True,
    default=False,
    help="Generate the KFP YAML which is used to run the workflow on Kubeflow Pipelines.",
    show_default=True,
)
@click.option(
    "--pipeline-path",
    "pipeline_path",
    default=None,
    help="The output path of the generated KFP pipeline yaml file",
    show_default=True,
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
    default=BASE_IMAGE,
    help="Base docker image used in Kubeflow Pipelines.",
    show_default=True,
)
@click.option(
    "--pipeline-name",
    "pipeline_name",
    default=None,
    help="If not set uses flow_name.",
)
@click.option(
    "--max-parallelism",
    "-m",
    default=KFP_MAX_PARALLELISM,
    show_default=True,
    help="Maximum number of parallel pods.",
)
@click.option(
    "--workflow-timeout", default=None, type=int, help="Workflow timeout in seconds."
)
@click.option(
    "--wait-for-completion",
    "--wait",
    "-w",
    "wait_for_completion",
    is_flag=True,
    default=False,
    help="Wait for KFP run to complete before process exits.",
    show_default=True,
)
@click.option(
    "--wait-for-completion-timeout",
    "--wait-timeout",
    "-wt",
    "wait_for_completion_timeout",
    default=1800,
    type=int,
    help="Timeout to wait for completion of run before process exits.",
)
@click.option(
    "--argo-wait",
    "-aw",
    "argo_wait",
    is_flag=True,
    default=False,
    help="Use Argo CLI watch to wait for KFP run to complete.",
    show_default=True,
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
@click.pass_obj
def run(
    obj,
    experiment=None,
    run_name=None,
    tags=None,
    namespace=None,
    kfp_namespace=KFP_SDK_NAMESPACE,
    api_namespace=KFP_SDK_API_NAMESPACE,
    yaml_only=False,
    pipeline_path=None,
    s3_code_package=True,
    base_image=BASE_IMAGE,
    pipeline_name=None,
    max_parallelism=None,
    workflow_timeout=None,
    wait_for_completion=False,
    wait_for_completion_timeout=None,
    notify=False,
    notify_on_error=None,
    notify_on_success=None,
    argo_wait=False,
    **kwargs,
):
    """
    Analogous to step_functions_cli.py
    """

    def _convert_value(param: parameters.Parameter):
        v = kwargs.get(param.name)
        return json.dumps(v) if param.kwargs.get("type") == JSONType else v

    flow_parameters = {
        param.name: _convert_value(param)
        for _, param in obj.flow._get_parameters()
        if kwargs.get(param.name) is not None
    }

    # Add additional tags
    if kfp_namespace:
        tags = tags + (kfp_namespace,)

    if experiment:
        tags = tags + (experiment,)

    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    check_metadata_service_version(obj)
    flow = make_flow(
        obj,
        pipeline_name if pipeline_name else obj.flow.name,
        tags,
        experiment,
        namespace,
        kfp_namespace,
        api_namespace,
        base_image,
        s3_code_package,
        yaml_only,
        max_parallelism,
        workflow_timeout,
        notify,
        notify_on_error,
        notify_on_success,
    )

    if yaml_only:
        if pipeline_path is None:
            raise CommandException("Please specify --pipeline-path")

        pipeline_path = flow.create_kfp_pipeline_yaml(pipeline_path)
        obj.echo(
            "\nDone converting *{name}* to {path}".format(
                name=current.flow_name, path=pipeline_path
            )
        )
    else:
        if s3_code_package and flow.datastore.TYPE != "s3":
            raise CommandException(
                "Kubeflow Pipelines s3-code-package requires --datastore=s3."
            )

        obj.echo(
            "Deploying *%s* to Kubeflow Pipelines..." % current.flow_name,
            bold=True,
        )
        run_pipeline_result = flow.create_run_on_kfp(run_name, flow_parameters)

        obj.echo("\nRun created successfully!\n")

        run_id = f"kfp-{run_pipeline_result.run_id}"
        obj.echo(f"Metaflow run_id=*{run_id}* \n", fg="magenta")

        kfp_run_url = posixpath.join(
            KFP_RUN_URL_PREFIX,
            "_/pipeline/#/runs/details",
            run_pipeline_result.run_id,
        )

        obj.echo(
            "*Run link:* {kfp_run_url}\n".format(kfp_run_url=kfp_run_url),
            fg="cyan",
        )

        run_info = flow._client.get_run(run_pipeline_result.run_id)
        workflow_manifest = json.loads(run_info.pipeline_runtime.workflow_manifest)
        argo_workflow_name = workflow_manifest["metadata"]["name"]

        obj.echo(
            f"*Argo workflow:* argo -n {kfp_namespace} watch {argo_workflow_name}\n",
            fg="cyan",
        )

        if argo_wait:
            argo_path: str = shutil.which("argo")

            argo_cmd = f"{argo_path} -n {kfp_namespace} "
            cmd = f"{argo_cmd} watch {argo_workflow_name}"
            subprocess.run(cmd, shell=True, universal_newlines=True)

            cmd = f"{argo_cmd} get {argo_workflow_name} | grep Status | awk '{{print $2}}'"
            ret = subprocess.run(
                cmd, shell=True, stdout=subprocess.PIPE, encoding="utf8"
            )
            succeeded = "Succeeded" in ret.stdout
            show_status(run_id, kfp_run_url, obj.echo, succeeded)
        elif wait_for_completion:
            response = flow._client.wait_for_run_completion(
                run_pipeline_result.run_id, timeout=wait_for_completion_timeout
            )
            succeeded = (response.run.status == "Succeeded",)
            show_status(run_id, kfp_run_url, obj.echo, succeeded)


def show_status(run_id: str, kfp_run_url: str, echo: callable, succeeded: bool):
    if succeeded:
        echo("\nSUCCEEDED!", fg="green")
    else:
        raise Exception(
            f"Flow: {current.flow_name}, run_id: {run_id}, run_link: {kfp_run_url} FAILED!"
        )


def make_flow(
    obj,
    name,
    tags,
    experiment,
    namespace,
    kfp_namespace,
    api_namespace,
    base_image,
    s3_code_package,
    yaml_only,
    max_parallelism,
    workflow_timeout,
    notify,
    notify_on_error,
    notify_on_success,
):
    """
    Analogous to step_functions_cli.py
    """

    # Import declared inside here because this file has Python3 syntax while
    # Metaflow supports Python2 for backward compat, so only load Python3 if the KFP plugin
    # is being run.
    from metaflow.plugins.kfp.kfp import KubeflowPipelines
    from metaflow.plugins.kfp.kfp_decorator import KfpInternalDecorator

    datastore = (
        None
        if (not s3_code_package)
        else obj.datastore(
            obj.flow.name,
            mode="w",
            metadata=obj.metadata,
            event_logger=obj.event_logger,
            monitor=obj.monitor,
        )
    )

    # Attach KFP decorator to the flow
    decorators._attach_decorators(obj.flow, [KfpInternalDecorator.name])
    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger
    )

    obj.package = MetaflowPackage(
        obj.flow, obj.environment, obj.logger, obj.package_suffixes
    )

    package_url = (
        datastore.save_data(obj.package.sha, TransformableObject(obj.package.blob))
        if s3_code_package
        else None
    )

    if package_url:
        obj.echo(
            "*Uploaded package to:* {package_url}".format(package_url=package_url),
            fg="cyan",
        )

    return KubeflowPipelines(
        name,
        obj.graph,
        obj.flow,
        obj.package,
        package_url,
        obj.metadata,
        obj.datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        base_image=base_image,
        s3_code_package=s3_code_package,
        tags=tags,
        experiment=experiment,
        namespace=namespace,
        kfp_namespace=kfp_namespace,
        api_namespace=api_namespace,
        username=get_username(),
        max_parallelism=max_parallelism,
        workflow_timeout=workflow_timeout,
        notify=notify,
        notify_on_error=notify_on_error,
        notify_on_success=notify_on_success,
    )
