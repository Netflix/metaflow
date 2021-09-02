import re
from os import listdir
from os.path import isfile, join
from subprocess_tee import run
import json
import re
import requests
from typing import List, Dict

from .... import R

from metaflow.exception import MetaflowException

import pytest

import yaml
import tempfile

import time

import uuid

"""
To run these tests from your terminal, go to the tests directory and run: 
`python -m pytest -s -n 3 run_integration_tests.py`

This script runs all the flows in the `flows` directory. It creates
each kfp run, waits for the run to fully complete, and prints whether
or not the run was successful. It also checks to make sure the logging
functionality works.

More specifically, the tests spawn KFP runs and ensure the spawning processes
have a returncode of 0. If any test fails within KFP, an exception
is raised, the test fails, and the user can access the run link to the failed
KFP run.

Parameters:
-n: specifies the number of parallel processes used by PyTest.

Sometimes, the tests may fail on KFP due to resource quota issues. If they do,
try reducing -n (number of parallel processes) so less simultaneous
KFP runs will be scheduled.

"""


def _python():
    if R.use_r():
        return "python3"
    else:
        return "python"


non_standard_test_flows = [
    "accelerator_flow.py",
    "check_error_handling_flow.py",
    "raise_error_flow.py",
    "s3_sensor_flow.py",
    "upload_to_s3_flow.py",
]


def obtain_flow_file_paths(flow_dir_path: str) -> List[str]:
    file_paths = [
        file_name
        for file_name in listdir(flow_dir_path)
        if isfile(join(flow_dir_path, file_name))
        and not file_name.startswith(".")
        and not file_name in non_standard_test_flows
    ]
    return file_paths


def test_s3_sensor_flow(pytestconfig) -> None:
    # ensure the s3_sensor waits for some time before the key exists
    file_name = f"s3-sensor-file-{uuid.uuid1()}.txt"

    upload_to_s3_flow_cmd = (
        f"{_python()} flows/upload_to_s3_flow.py --datastore=s3 kfp run "
    )
    s3_sensor_flow_cmd = f"{_python()} flows/s3_sensor_flow.py --datastore=s3 kfp run --wait-for-completion "

    main_config_cmds = (
        f"--workflow-timeout 1800 "
        f"--experiment metaflow_test --tag test_t1 "
        f"--file_name {file_name} "
    )
    upload_to_s3_flow_cmd += main_config_cmds
    s3_sensor_flow_cmd += main_config_cmds

    if pytestconfig.getoption("image"):
        image_cmds = (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')} "
        )
        upload_to_s3_flow_cmd += image_cmds
        s3_sensor_flow_cmd += image_cmds

    exponential_backoff_from_platform_errors(upload_to_s3_flow_cmd, 0)
    exponential_backoff_from_platform_errors(s3_sensor_flow_cmd, 0)

    return


# This test ensures that a flow fails correctly,
# and when it fails, an OpsGenie email is sent.
def test_error_and_opsgenie_alert(pytestconfig) -> None:
    test_cmd = (
        f"{_python()} flows/raise_error_flow.py --datastore=s3 kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--experiment metaflow_test --tag test_t1 --notify "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    error_flow_id = exponential_backoff_from_platform_errors(test_cmd, 1)
    opsgenie_auth_headers = {
        "Content-Type": "application/json",
        "Authorization": f"GenieKey {pytestconfig.getoption('opsgenie_api_token')}",
    }

    # Look for the alert with the correct kfp_run_id in the description.
    list_alerts_endpoint = f"https://api.opsgenie.com/v2/alerts?query=description:{error_flow_id}&limit=1&sort=createdAt&order=des"
    list_alerts_response = requests.get(
        list_alerts_endpoint, headers=opsgenie_auth_headers
    )
    assert list_alerts_response.status_code == 200

    list_alerts_response_json = json.loads(list_alerts_response.text)
    # assert we have found the alert (there should only be one alert with that kfp_run_id)
    assert len(list_alerts_response_json["data"]) == 1
    alert_alias = list_alerts_response_json["data"][0]["alias"]

    close_alert_data = {
        "user": "AIP Integration Testing Service",
        "source": "AIP Integration Testing Service",
        "note": "Closing ticket because the test is complete.",
    }
    close_alert_endpoint = (
        f"https://api.opsgenie.com/v2/alerts/{alert_alias}/close?identifierType=alias"
    )
    close_alert_response = requests.post(
        close_alert_endpoint,
        data=json.dumps(close_alert_data),
        headers=opsgenie_auth_headers,
    )
    # Sometimes the response status code is 202, signaling
    # the request has been accepted and is being queued for processing.
    assert (
        close_alert_response.status_code == 200
        or close_alert_response.status_code == 202
    )

    # Test logging of raise_error_flow
    test_cmd = (
        f"{_python()} flows/check_error_handling_flow.py "
        f"--datastore=s3 --with retry kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--experiment metaflow_test --tag test_t1 "
        f"--error_flow_id={error_flow_id} "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )
    exponential_backoff_from_platform_errors(test_cmd, 0)

    return


def exists_nvidia_accelerator(node_selector_term: Dict) -> bool:
    for affinity_match_expression in node_selector_term["matchExpressions"]:
        if (
            affinity_match_expression["key"] == "k8s.amazonaws.com/accelerator"
            and affinity_match_expression["operator"] == "In"
            and "nvidia-tesla-v100" in affinity_match_expression["values"]
        ):
            return True
    return False


def is_nvidia_accelerator_noschedule(toleration: Dict) -> bool:
    if (
        toleration["effect"] == "NoSchedule"
        and toleration["key"] == "k8s.amazonaws.com/accelerator"
        and toleration["operator"] == "Equal"
        and toleration["value"] == "nvidia-tesla-v100"
    ):
        return True
    return False


def test_compile_only_accelerator_test() -> None:
    with tempfile.TemporaryDirectory() as yaml_tmp_dir:
        yaml_file_path = join(yaml_tmp_dir, "accelerator_flow.yaml")

        compile_to_yaml_cmd = (
            f"{_python()} flows/accelerator_flow.py --datastore=s3 --with retry kfp run "
            f" --no-s3-code-package --yaml-only --pipeline-path {yaml_file_path}"
        )

        compile_to_yaml_process = run(
            compile_to_yaml_cmd,
            universal_newlines=True,
            shell=True,
        )
        assert compile_to_yaml_process.returncode == 0

        with open(f"{yaml_file_path}", "r") as stream:
            try:
                flow_yaml = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        for step in flow_yaml["spec"]["templates"]:
            if step["name"] == "start":
                start_step = step
                break

    affinity_found = False
    for node_selector_term in start_step["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"]:
        if exists_nvidia_accelerator(node_selector_term):
            affinity_found = True
            break
    assert affinity_found

    toleration_found = False
    for toleration in start_step["tolerations"]:
        if is_nvidia_accelerator_noschedule(toleration):
            toleration_found = True
            break
    assert toleration_found


@pytest.mark.parametrize("flow_file_path", obtain_flow_file_paths("flows"))
def test_flows(pytestconfig, flow_file_path: str) -> None:
    full_path = join("flows", flow_file_path)

    test_cmd = (
        f"{_python()} {full_path} --datastore=s3 --with retry kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    exponential_backoff_from_platform_errors(test_cmd, 0)

    return


def exponential_backoff_from_platform_errors(
    kfp_run_cmd: str, correct_return_code: int
) -> str:
    # Within this function, we use the special feature of subprocess_tee which allows us
    # to capture both stdout and stderr (akin to stdout=PIPE, stderr=PIPE in the regular subprocess.run)
    # as well as output to stdout and stderr (which users can see on the Gitlab logs). We check
    # if the error message is due to a KFAM issue, and if so, we do an exponential backoff.

    backoff_intervals_in_seconds = [0, 2, 4, 8, 16, 32]

    platform_error_messages = [
        "Reason: Unauthorized",
        "Failed to connect to the KFAM service",
        "Failed to create a new experiment",
    ]

    for interval in backoff_intervals_in_seconds:
        time.sleep(interval)

        run_and_wait_process = run(
            kfp_run_cmd,
            universal_newlines=True,
            shell=True,
        )

        for platform_error_message in platform_error_messages:
            if platform_error_message in run_and_wait_process.stderr:
                print(
                    f"Error: {run_and_wait_process.stderr}. Backing off for {interval} seconds..."
                )
                break
        else:
            assert run_and_wait_process.returncode == correct_return_code
            break
    else:
        raise MetaflowException(
            "KFAM issues not resolved after successive backoff attempts."
        )

    kfp_run_id = re.search(
        "Metaflow run_id=(.*)\n",
        run_and_wait_process.stderr
    ).group(
        1
    )

    return kfp_run_id
