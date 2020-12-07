import posixpath

import click
import json

from metaflow import current, decorators, parameters, JSONType
from metaflow.datastore.datastore import TransformableObject
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    KFP_RUN_URL_PREFIX,
    KFP_SDK_API_NAMESPACE,
    KFP_SDK_NAMESPACE,
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
    "experiment_name",
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
    default=10,
    show_default=True,
    help="Maximum number of parallel pods.",
)
@click.option(
    "--workflow-timeout", default=None, type=int, help="Workflow timeout in seconds."
)
@click.option(
    "--wait-for-completion",
    "wait_for_completion",
    is_flag=True,
    default=False,
    help="Wait for KFP run to complete before process exits.",
    show_default=True,
)
@click.pass_obj
def run(
    obj,
    experiment_name=None,
    run_name=None,
    tags=None,
    namespace=KFP_SDK_NAMESPACE,
    api_namespace=KFP_SDK_API_NAMESPACE,
    yaml_only=False,
    pipeline_path=None,
    s3_code_package=True,
    base_image=BASE_IMAGE,
    pipeline_name=None,
    max_parallelism=None,
    workflow_timeout=None,
    wait_for_completion=False,
    **kwargs
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

    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    check_metadata_service_version(obj)
    flow = make_flow(
        obj,
        pipeline_name if pipeline_name else obj.flow.name,
        tags,
        namespace,
        api_namespace,
        base_image,
        s3_code_package,
        max_parallelism,
        workflow_timeout,
    )

    if yaml_only:
        pipeline_path = flow.create_kfp_pipeline_yaml(pipeline_path)
        obj.echo(
            "\nDone converting *{name}* to {path}".format(
                name=current.flow_name, path=pipeline_path
            )
        )
    else:
        if s3_code_package and flow.datastore.TYPE != "s3":
            raise MetaflowException(
                "Kubeflow Pipelines s3-code-package requires --datastore=s3."
            )

        obj.echo(
            "Deploying *%s* to Kubeflow Pipelines..." % current.flow_name, bold=True
        )
        run_pipeline_result = flow.create_run_on_kfp(
            experiment_name, run_name, flow_parameters
        )

        obj.echo("\nRun created successfully!\n")

        obj.echo(
            "Metaflow run_id=*kfp-{run_id}* \n".format(
                run_id=run_pipeline_result.run_id
            ),
            fg="magenta",
        )

        kfp_run_url = posixpath.join(
            KFP_RUN_URL_PREFIX, "_/pipeline/#/runs/details", run_pipeline_result.run_id
        )

        obj.echo(
            "*Run link:* {kfp_run_url}\n".format(kfp_run_url=kfp_run_url), fg="cyan"
        )

        if wait_for_completion:
            response = flow._client.wait_for_run_completion(
                run_pipeline_result.run_id, 1200
            )

            if response.run.status == "Succeeded":
                obj.echo("SUCCEEDED!", fg="green")
            else:
                raise Exception(
                    "Flow: {flow_name}, run link: {kfp_run_url} FAILED!".format(
                        flow_name=current.flow_name, kfp_run_url=kfp_run_url
                    )
                )


def make_flow(
    obj,
    name,
    tags,
    namespace,
    api_namespace,
    base_image,
    s3_code_package,
    max_parallelism,
    workflow_timeout,
):
    """
    Analogous to step_functions_cli.py
    """

    # Import declared inside here because this file has Python3 syntax while
    # Metaflow supports Python2 for backward compat, so only load Python3 if the KFP plugin
    # is being run.
    from metaflow.plugins.kfp.kfp import KubeflowPipelines
    from metaflow.plugins.kfp.kfp_decorator import KfpInternalDecorator

    datastore = obj.datastore(
        obj.flow.name,
        mode="w",
        metadata=obj.metadata,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
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
        namespace=namespace,
        api_namespace=api_namespace,
        username=get_username(),
        max_parallelism=max_parallelism,
        workflow_timeout=workflow_timeout,
    )
