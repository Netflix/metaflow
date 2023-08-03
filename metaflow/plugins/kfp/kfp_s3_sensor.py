"""
This function is called within the s3_sensor_op running container.
(1) It decodes the path_formatter function's code and runs it to obtain a final S3 path
(2) It splits the formatted path into an S3 bucket and key 
(3) It polls for an object with the specified bucket and key until timeout
"""
import base64
import json
import marshal
import os
import pathlib
import time
from typing import Dict, Tuple
from urllib.parse import ParseResult, urlparse

import botocore

from metaflow._vendor import click
from metaflow.datatools.s3util import get_s3_client


def construct_elapsed_time_s3_bucket_and_key(
    flow_name: str, run_id: str
) -> Tuple[str, str]:
    s3_path = os.path.join(
        os.getenv("METAFLOW_DATASTORE_SYSROOT_S3"),
        flow_name,
        run_id,
        "_s3_sensor",
    )
    s3_path_parsed: ParseResult = urlparse(s3_path)
    bucket: str = s3_path_parsed.netloc
    key: str = s3_path_parsed.path.lstrip("/")
    return bucket, key


def read_elapsed_time_s3_path(
    s3: botocore.client.BaseClient, flow_name: str, run_id: str
) -> float:
    bucket: str
    key: str
    bucket, key = construct_elapsed_time_s3_bucket_and_key(flow_name, run_id)

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        elapsed_time: float = 0.0
    else:
        s3_object: dict = s3.get_object(Bucket=bucket, Key=key)
        elapsed_time: float = float(s3_object["Body"].read().decode("utf-8"))
    return elapsed_time


def write_elapsed_time_s3_path(
    s3: botocore.client.BaseClient, flow_name: str, run_id: str, elapsed_time: float
) -> None:
    bucket, key = construct_elapsed_time_s3_bucket_and_key(flow_name, run_id)
    elapsed_time_binary_data = str(elapsed_time).encode("ascii")
    s3.put_object(Body=elapsed_time_binary_data, Bucket=bucket, Key=key)


# We separate out this function to ensure it can be unit-tested.
def wait_for_s3_path(
    path: str,
    flow_name: str,
    run_id: str,
    timeout_seconds: int,
    polling_interval_seconds: int,
    path_formatter_code_encoded: str,
    flow_parameters_json: str,
    os_expandvars: bool,
) -> str:
    flow_parameters: Dict[str, str] = {
        param["name"]: param["value"] for param in json.loads(flow_parameters_json)
    }

    if path_formatter_code_encoded:
        # path_formatter_code is of type `code object`,
        # see: https://docs.python.org/3/c-api/code.html
        path_formatter_code = marshal.loads(
            base64.b64decode(path_formatter_code_encoded)
        )

        def path_formatter_template(key: str, flow_parameters: dict) -> str:
            pass

        path_formatter_template.__code__ = path_formatter_code
        path: str = path_formatter_template(path, flow_parameters)
    else:
        if os_expandvars:
            # expand OS env variables
            path: str = os.path.expandvars(path)
        # default variable substitution
        path: str = path.format(**flow_parameters)

    # debugging print statement for customers so they know the final path
    # we're looking for
    print(f"Waiting for path: {path}...")

    parsed_path: ParseResult = urlparse(path)
    bucket: str
    key: str
    bucket, key = parsed_path.netloc, parsed_path.path.lstrip("/")
    s3: botocore.client.BaseClient
    s3, _ = get_s3_client()

    previous_elapsed_time: float = read_elapsed_time_s3_path(s3, flow_name, run_id)

    start_time: float = time.time()
    while True:
        current_time: float = time.time()
        elapsed_time: float = current_time - start_time + previous_elapsed_time
        if elapsed_time > timeout_seconds:
            raise TimeoutError("Timed out while waiting for S3 key..")

        try:
            s3.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            print(".")
        else:
            print(f"Object found at path {path}! Elapsed time: {elapsed_time}.")
            break

        write_elapsed_time_s3_path(s3, flow_name, run_id, elapsed_time)
        time.sleep(polling_interval_seconds)

    output_path = "/tmp/outputs/Output"
    output_file = "/tmp/outputs/Output/data"
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        f.write(str(path))
    return path


@click.command()
@click.option("--path")
@click.option("--flow_name")
@click.option("--run_id")
@click.option("--timeout_seconds", type=int)
@click.option("--polling_interval_seconds", type=int)
@click.option("--path_formatter_code_encoded")
@click.option("--flow_parameters_json")
@click.option("--os_expandvars/--no_os_expandvars", default=False)
def wait_for_s3_path_cli(
    path: str,
    flow_name: str,
    run_id: str,
    timeout_seconds: int,
    polling_interval_seconds: int,
    path_formatter_code_encoded: str,
    flow_parameters_json: str,
    os_expandvars: bool,
) -> str:
    return wait_for_s3_path(
        path,
        flow_name,
        run_id,
        timeout_seconds,
        polling_interval_seconds,
        path_formatter_code_encoded,
        flow_parameters_json,
        os_expandvars,
    )


if __name__ == "__main__":
    wait_for_s3_path_cli()
