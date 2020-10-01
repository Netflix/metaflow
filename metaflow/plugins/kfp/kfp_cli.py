import posixpath

import click

from metaflow import current, decorators
from metaflow.datastore.datastore import TransformableObject
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    KFP_SDK_API_NAMESPACE,
    KFP_SDK_NAMESPACE,
    KFP_RUN_URL_PREFIX,
)
from metaflow.package import MetaflowPackage
from metaflow.plugins.aws.step_functions.step_functions_cli import (
    check_metadata_service_version,
)
from metaflow.plugins.kfp.constants import (
    DEFAULT_EXPERIMENT_NAME,
    DEFAULT_RUN_NAME,
    DEFAULT_KFP_YAML_OUTPUT_PATH,
    BASE_IMAGE,
)
from metaflow.plugins.kfp.kfp_decorator import KfpInternalDecorator
from metaflow.util import get_username


class IncorrectMetadataServiceVersion(MetaflowException):
    headline = "Incorrect version for metaflow service"


@click.group()
def cli():
    pass


@cli.group(name="kfp", help="Commands related to Kubeflow Pipelines.")
@click.pass_obj
def kubeflow_pipelines(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@kubeflow_pipelines.command(
    help="Deploy a new version of this workflow to Kubeflow Pipelines."
)
@click.option(
    "--experiment-name",
    "experiment_name",
    default=DEFAULT_EXPERIMENT_NAME,
    help="The associated experiment name for the run",
)
@click.option(
    "--run-name",
    "run_name",
    default=DEFAULT_RUN_NAME,
    help="Name assigned to the new KFP run",
)
@click.option(
    "--namespace",
    "namespace",
    default=KFP_SDK_NAMESPACE,
    help="Namespace of your run in KFP.",
)
@click.option(
    "--api-namespace",
    "api_namespace",
    default=KFP_SDK_API_NAMESPACE,
    help="Namespace where the API service is run.",
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
    default=DEFAULT_KFP_YAML_OUTPUT_PATH,
    help="The output path of the generated KFP pipeline yaml file",
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
@click.pass_obj
def run(
    obj,
    experiment_name=DEFAULT_EXPERIMENT_NAME,
    run_name=DEFAULT_RUN_NAME,
    namespace=KFP_SDK_NAMESPACE,
    api_namespace=KFP_SDK_API_NAMESPACE,
    yaml_only=False,
    pipeline_path=DEFAULT_KFP_YAML_OUTPUT_PATH,
    s3_code_package=True,
    base_image=BASE_IMAGE,
    pipeline_name=None,
):
    """
    Analogous to step_functions_cli.py
    """
    check_metadata_service_version(obj)
    flow = make_flow(
        obj,
        pipeline_name if pipeline_path else current.flow_name,
        namespace,
        api_namespace,
        base_image,
        s3_code_package,
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
        run_pipeline_result = flow.create_run_on_kfp(experiment_name, run_name)

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

        obj.echo("Run link: {kfp_run_url}\n".format(kfp_run_url=kfp_run_url), fg="cyan")


def make_flow(obj, name, namespace, api_namespace, base_image, s3_code_package):
    """
    Analogous to step_functions_cli.py
    """
    datastore = obj.datastore(
        obj.flow.name,
        mode="w",
        metadata=obj.metadata,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
    )

    # Attach KFP decorator to the flow
    decorators._attach_decorators(obj.flow, [KfpInternalDecorator.name])
    decorators._init_decorators(
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
            "Uploaded package to: {package_url}".format(package_url=package_url),
            fg="magenta",
        )

    from metaflow.plugins.kfp.kfp import KubeflowPipelines

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
        namespace=namespace,
        api_namespace=api_namespace,
        username=get_username(),
    )
