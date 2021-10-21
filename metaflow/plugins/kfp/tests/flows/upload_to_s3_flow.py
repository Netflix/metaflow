from metaflow import FlowSpec, step, resources, s3_sensor, Parameter

import boto3
import time
from subprocess import run, PIPE

from os import environ
from os.path import join

from urllib.parse import urlparse

"""
This test flow uploads a file at a particular S3 location. The test flows s3_sensor_flow.py
and s3_sensor_flow_with_formatter.py then wait for this file to appear in S3 through the use
of the @s3_sensor, after which the flows proceed.
"""


def upload_file_to_s3(file_name: str) -> None:
    run(f"touch {file_name}", universal_newlines=True, stdout=PIPE, shell=True)
    # using environ with METAFLOW_DATASTORE_SYSROOT_S3 env var
    # since it is available at run time in the pods on Kubeflow
    root = urlparse(environ["METAFLOW_DATASTORE_SYSROOT_S3"])
    bucket, key = root.netloc, root.path.lstrip("/")

    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(f"./{file_name}", bucket, join(key, file_name))


class UploadToS3Flow(FlowSpec):
    file_name = Parameter(
        "file_name",
    )
    file_name_for_formatter_test = Parameter("file_name_for_formatter_test")

    @step
    def start(self):
        print("Waiting to upload file...")
        time.sleep(100)
        print(f"Uploading {self.file_name} to S3...")
        upload_file_to_s3(self.file_name)

        print("Waiting to upload file for formatter test...")
        time.sleep(100)
        print(f"Uploading {self.file_name_for_formatter_test} to S3...")
        upload_file_to_s3(self.file_name_for_formatter_test)

        self.next(self.end)

    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    UploadToS3Flow()
