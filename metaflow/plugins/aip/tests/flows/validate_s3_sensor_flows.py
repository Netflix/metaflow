import time
from os import environ
from os.path import join
from subprocess import PIPE, run
from urllib.parse import ParseResult, urlparse

import botocore
from kubernetes import config
from kubernetes.client import api_client
from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.resource import Resource, ResourceInstance

from metaflow import FlowSpec, Parameter, resources, s3_sensor, step
from metaflow.datatools.s3util import get_s3_client

"""
This test flow validates the execution of s3_sensor_flow.py and 
s3_sensor_with_formatter_flow.py. 

1. It uploads a file at a particular path in S3 known to the above 2
flows so during execution, the s3_sensor in the above 2 flows are tested to
find the the file.
2. It deletes the s3_sensor pod in the above 2 flows to ensure s3_sensor recovers
from workflow failures.
3. It waits for the completion of the above 2 flows. This is necesary because we don't
specify --wait-for-completion in the above 2 flows, so the aip run command will return
immediately with success. We didn't specify --wait-for-completion because we parse the
output of the aip run command to obtain the workflow_name, which is used within this
flow to delete the s3_sensor pods.
"""

SUBMIT_RUN_POLL_TIMEOUT_SECONDS = 5
WAIT_FOR_S3_SENSOR_FLOW_COMPLETION_TIMEOUT = 1200


def get_dynamic_client() -> Resource:
    dynamic_client: Resource = DynamicClient(
        api_client.ApiClient(configuration=config.load_incluster_config())
    )
    return dynamic_client


def get_argo_workflow(argo_workflow_name: str) -> ResourceInstance:
    namespace: str = environ.get("POD_NAMESPACE", default=None)
    dynamic_client: Resource = get_dynamic_client()
    workflow_api: ResourceInstance = dynamic_client.resources.get(
        api_version="argoproj.io/v1alpha1", kind="Workflow"
    )
    argo_workflow: ResourceInstance = workflow_api.get(
        name=argo_workflow_name,
        namespace=namespace,
    )
    return argo_workflow


def delete_pod(pod_name: str) -> None:
    namespace: str = environ.get("POD_NAMESPACE", default=None)
    dynamic_client: Resource = get_dynamic_client()
    pod_api: ResourceInstance = dynamic_client.resources.get(
        api_version="v1", kind="Pod"
    )
    pod_api.delete(
        name=pod_name,
        namespace=namespace,
    )


def upload_file_to_s3(file_name: str) -> None:
    run(f"touch {file_name}", universal_newlines=True, stdout=PIPE, shell=True)
    # using environ with METAFLOW_DATASTORE_SYSROOT_S3 env var
    # since it is available at run time in the pods on Kubeflow
    root: ParseResult = urlparse(
        join(environ["METAFLOW_DATASTORE_SYSROOT_S3"], "s3_sensor_test_key_files")
    )
    bucket: str = root.netloc
    key: str = root.path.lstrip("/")

    s3: botocore.client.BaseClient
    s3, _ = get_s3_client()
    s3.upload_file(f"./{file_name}", bucket, join(key, file_name))


def delete_s3_sensor_pod_to_test_retry(argo_workflow_name: str) -> None:
    argo_workflow: ResourceInstance = get_argo_workflow(argo_workflow_name)
    for node in argo_workflow["status"]["nodes"]:
        node_name: str = node[0]
        node_info: dict = node[1]
        if node_info["type"] == "Pod" and "s3sensor" in node_name:
            s3_sensor_pod_name = node_name
            break
    else:
        raise Exception("s3_sensor pod not found.")
    delete_pod(s3_sensor_pod_name)


def wait_for_s3_sensor_flow_completion(argo_workflow_name: str) -> None:
    argo_workflow: ResourceInstance = get_argo_workflow(argo_workflow_name)
    argo_workflow_status: str = argo_workflow["status"]["phase"]
    start_time: float = time.time()

    while argo_workflow_status not in {"Succeeded", "Skipped", "Failed", "Error"}:
        print(f"Waiting for workflow f{argo_workflow_name} to complete...")
        current_time: float = time.time()
        elapsed_time: float = current_time - start_time
        if elapsed_time > WAIT_FOR_S3_SENSOR_FLOW_COMPLETION_TIMEOUT:
            raise TimeoutError("Timed out waiting for s3_sensor_flow completion.")

        argo_workflow: ResourceInstance = get_argo_workflow(argo_workflow_name)
        argo_workflow_status: str = argo_workflow["status"]["phase"]
        time.sleep(SUBMIT_RUN_POLL_TIMEOUT_SECONDS)

    if argo_workflow_status == "Succeeded":
        print(f"workflow {argo_workflow_name} passed!")
    else:
        raise Exception(f"workflow {argo_workflow_name} failed!")


class ValidateS3SensorFlows(FlowSpec):
    file_name = Parameter(
        "file_name",
    )
    file_name_for_formatter_test = Parameter("file_name_for_formatter_test")
    s3_sensor_argo_workflow_name = Parameter(
        "s3_sensor_argo_workflow_name",
    )
    s3_sensor_with_formatter_argo_workflow_name = Parameter(
        "s3_sensor_with_formatter_argo_workflow_name"
    )

    @step
    def start(self):
        delete_s3_sensor_pod_to_test_retry(self.s3_sensor_argo_workflow_name)
        delete_s3_sensor_pod_to_test_retry(
            self.s3_sensor_with_formatter_argo_workflow_name
        )

        print("Waiting to upload file...")
        time.sleep(20)

        print(f"Uploading {self.file_name} to S3...")
        upload_file_to_s3(self.file_name)

        print(f"Uploading {self.file_name_for_formatter_test} to S3...")
        upload_file_to_s3(self.file_name_for_formatter_test)

        self.next(self.end)

    @step
    def end(self):
        wait_for_s3_sensor_flow_completion(self.s3_sensor_argo_workflow_name)
        wait_for_s3_sensor_flow_completion(
            self.s3_sensor_with_formatter_argo_workflow_name
        )
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    ValidateS3SensorFlows()
