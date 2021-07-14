from unittest import mock
from metaflow.plugins.kfp.kfp_s3_sensor import wait_for_s3_path

from unittest.mock import call, Mock, patch, PropertyMock
import pytest

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3

import tempfile

import base64
import marshal

import os

"""
To run these tests from your terminal, go to the root directory and run:

`python -m pytest metaflow/plugins/kfp/tests/test_kfp_s3_sensor.py -c /dev/null`

The `-c` flag above tells PyTest to ignore the setup.cfg config file which is used
for the integration tests.

"""


def identity_formatter(path: str, flow_parameters: dict) -> str:
    return path


@mock_s3
@pytest.mark.parametrize(
    "upload_bucket, upload_key, upload_path, processed_path, flow_parameters_json, os_expandvars",
    [
        (
            "sample_bucket",
            "sample_prefix/sample_file.txt",
            "s3://sample_bucket/sample_prefix/sample_file.txt",
            "s3://sample_bucket/sample_prefix/sample_file.txt",
            '{"key": "value"}',
            False,
        ),
        (
            "sample_bucket",
            "sample_prefix/date=07-02-2021/sample.txt",
            "s3://sample_bucket/sample_prefix/date={date}/sample.txt",
            "s3://sample_bucket/sample_prefix/date=07-02-2021/sample.txt",
            '{"date": "07-02-2021"}',
            False,
        ),
        (
            "sample_bucket",
            "sample_prefix/date=08-03-2022/sample.txt",
            "s3://sample_bucket/sample_prefix/date=$DATE/sample.txt",
            "s3://sample_bucket/sample_prefix/date=08-03-2022/sample.txt",
            '{"key": "value"}',
            True,
        ),
    ],
)
def test_wait_for_s3_path(
    upload_bucket: str,
    upload_key: str,
    upload_path: str,
    processed_path: str,
    flow_parameters_json: str,
    os_expandvars: bool,
):
    os.environ["DATE"] = "08-03-2022"

    identity_formatter_code_encoded = base64.b64encode(
        marshal.dumps(identity_formatter.__code__)
    ).decode("ascii")

    upload_file = tempfile.NamedTemporaryFile()

    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=upload_bucket)
    s3.meta.client.upload_file(upload_file.name, upload_bucket, upload_key)

    path = wait_for_s3_path(
        path=upload_path,
        timeout_seconds=1,
        polling_interval_seconds=1,
        path_formatter_code_encoded=identity_formatter_code_encoded,
        flow_parameters_json=flow_parameters_json,
        os_expandvars=os_expandvars,
    )

    assert path == processed_path


# This test ensures a timeout exception is raised when wait_for_s3_path
# looks for a nonexistent S3 path. This ensures we don't have an idle
# pod using up resources continuously.
def test_wait_for_s3_path_timeout_exception():
    identity_formatter_code_encoded = base64.b64encode(
        marshal.dumps(identity_formatter.__code__)
    ).decode("ascii")

    with pytest.raises(TimeoutError):
        wait_for_s3_path(
            path="s3://sample_bucket/sample_prefix/sample_key",
            timeout_seconds=1,
            polling_interval_seconds=1,
            path_formatter_code_encoded=identity_formatter_code_encoded,
            flow_parameters_json='{"key": "value"}',
            os_expandvars=False,
        )
