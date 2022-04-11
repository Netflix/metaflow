import os
import pathlib

from kubernetes import config
from kubernetes.client import api_client
from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.resource import Resource, ResourceInstance

from metaflow._vendor import click


@click.command()
@click.option("--workflow_name")
@click.option("--s3_sensor_path")
def get_workflow_uid(
    workflow_name: str,
    # See https://zbrt.atl.zillow.net/browse/AIP-5404
    # (Title: Investigate bug in KFP SDK .after() construct)
    # for explanation of s3_sensor_path parameter.
    s3_sensor_path: str,
) -> str:
    """
    The environment variables that this depends on:
        POD_NAMESPACE
    """
    namespace: str = os.environ.get("POD_NAMESPACE", default=None)

    configuration = config.load_incluster_config()
    dynamic_client: Resource = DynamicClient(
        api_client.ApiClient(configuration=configuration)
    )
    workflow_api: ResourceInstance = dynamic_client.resources.get(
        api_version="argoproj.io/v1alpha1", kind="Workflow"
    )
    workflow: ResourceInstance = workflow_api.get(
        name=workflow_name,
        namespace=namespace,
    )

    uid = workflow["metadata"]["uid"]
    print("uid=", uid)

    output_path = "/tmp/outputs/Output"
    output_file = "/tmp/outputs/Output/data"
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        f.write(str(uid))


if __name__ == "__main__":
    get_workflow_uid()
