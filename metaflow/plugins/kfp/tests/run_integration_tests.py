import json
import re
import tempfile
import time
import uuid
from os import listdir
from os.path import isfile, join
from subprocess import CompletedProcess
from typing import Dict, List, Tuple

import pytest
import requests
import yaml
from requests.models import Response
from subprocess_tee import run

from metaflow import R
from metaflow.exception import MetaflowException

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

non_standard_test_flows = [
    "check_error_handling_flow.py",
    "raise_error_flow.py",
    "s3_sensor_flow.py",
    "s3_sensor_with_formatter_flow.py",
    "validate_s3_sensor_flows.py",
    "toleration_and_affinity_flow.py",
]


def _python():
    if R.use_r():
        return "python3"
    else:
        return "python"


def obtain_flow_file_paths(flow_dir_path: str) -> List[str]:
    file_paths: List[str] = [
        file_name
        for file_name in listdir(flow_dir_path)
        if isfile(join(flow_dir_path, file_name))
        and not file_name.startswith(".")
        and not file_name in non_standard_test_flows
    ]
    return file_paths


def test_s3_sensor_flow(pytestconfig) -> None:
    # ensure the s3_sensor waits for some time before the key exists
    file_name: str = f"s3-sensor-file-{uuid.uuid1()}.txt"
    file_name_for_formatter_test: str = f"s3-sensor-file-{uuid.uuid1()}.txt"

    s3_sensor_flow_cmd: str = (
        f"{_python()} flows/s3_sensor_flow.py --datastore=s3 --with retry  kfp run "
        f"--file_name {file_name} --notify "
    )
    s3_sensor_with_formatter_flow_cmd: str = (
        f"{_python()} flows/s3_sensor_with_formatter_flow.py --datastore=s3 --with retry kfp run "
        f"--file_name_for_formatter_test {file_name_for_formatter_test} --notify "
    )

    main_config_cmds: str = (
        f"--workflow-timeout 1800 " f"--experiment metaflow_test --tag test_t1 "
    )

    s3_sensor_flow_cmd += main_config_cmds
    s3_sensor_with_formatter_flow_cmd += main_config_cmds

    if pytestconfig.getoption("image"):
        image_cmds: str = (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')} "
        )
    else:
        image_cmds: str = ""

    s3_sensor_flow_cmd += image_cmds
    s3_sensor_with_formatter_flow_cmd += image_cmds

    kfp_run_id: str
    s3_sensor_argo_workflow_name: str
    (
        kfp_run_id,
        s3_sensor_argo_workflow_name,
    ) = run_cmd_with_backoff_from_platform_errors(
        s3_sensor_flow_cmd, correct_return_code=0
    )
    kfp_run_id_formatter_flow: str
    s3_sensor_with_formatter_argo_workflow_name: str
    (
        kfp_run_id_formatter_flow,
        s3_sensor_with_formatter_argo_workflow_name,
    ) = run_cmd_with_backoff_from_platform_errors(
        s3_sensor_with_formatter_flow_cmd, correct_return_code=0
    )

    validate_s3_sensor_flow_cmd: str = (
        f"{_python()} flows/validate_s3_sensor_flows.py --datastore=s3 --with retry kfp run "
        f"--file_name {file_name} --file_name_for_formatter_test {file_name_for_formatter_test} "
        f"--s3_sensor_argo_workflow_name {s3_sensor_argo_workflow_name} --s3_sensor_with_formatter_argo_workflow_name {s3_sensor_with_formatter_argo_workflow_name} "
        f"--wait-for-completion "
    )
    validate_s3_sensor_flow_cmd += main_config_cmds
    validate_s3_sensor_flow_cmd += image_cmds
    run_cmd_with_backoff_from_platform_errors(
        validate_s3_sensor_flow_cmd, correct_return_code=0
    )


# This test ensures that a flow fails correctly,
# and when it fails, an OpsGenie email is sent.
def test_error_and_opsgenie_alert(pytestconfig) -> None:
    raise_error_flow_cmd: str = (
        f"{_python()} flows/raise_error_flow.py --datastore=s3 kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--experiment metaflow_test --tag test_t1 --notify "
    )
    if pytestconfig.getoption("image"):
        image_cmds: str = (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )
    else:
        image_cmds: str = ""
    raise_error_flow_cmd += image_cmds

    error_flow_id: str
    error_flow_workflow_name: str
    error_flow_id, error_flow_workflow_name = run_cmd_with_backoff_from_platform_errors(
        raise_error_flow_cmd, correct_return_code=1
    )
    opsgenie_auth_headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "Authorization": f"GenieKey {pytestconfig.getoption('opsgenie_api_token')}",
    }

    # Look for the alert with the correct kfp_run_id in the description.
    list_alerts_endpoint: str = f"https://api.opsgenie.com/v2/alerts?query=description:{error_flow_id}&limit=1&sort=createdAt&order=des"
    list_alerts_response: Response = requests.get(
        list_alerts_endpoint, headers=opsgenie_auth_headers
    )
    assert list_alerts_response.status_code == 200

    list_alerts_response_json: dict = json.loads(list_alerts_response.text)
    # assert we have found the alert (there should only be one alert with that kfp_run_id)
    assert len(list_alerts_response_json["data"]) == 1
    alert_alias = list_alerts_response_json["data"][0]["alias"]

    close_alert_data: str = {
        "user": "AIP Integration Testing Service",
        "source": "AIP Integration Testing Service",
        "note": "Closing ticket because the test is complete.",
    }
    close_alert_endpoint: str = (
        f"https://api.opsgenie.com/v2/alerts/{alert_alias}/close?identifierType=alias"
    )
    close_alert_response: Response = requests.post(
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
    check_error_handling_flow_cmd: str = (
        f"{_python()} flows/check_error_handling_flow.py "
        f"--datastore=s3 --with retry kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--experiment metaflow_test --tag test_t1 "
        f"--error_flow_id={error_flow_id} "
        f"--notify "
    )
    check_error_handling_flow_cmd += image_cmds
    run_cmd_with_backoff_from_platform_errors(
        check_error_handling_flow_cmd, correct_return_code=0
    )

    return


@pytest.mark.parametrize("flow_file_path", obtain_flow_file_paths("flows"))
def test_flows(pytestconfig, flow_file_path: str) -> None:
    full_path: str = join("flows", flow_file_path)

    test_cmd: str = (
        f"{_python()} {full_path} --datastore=s3 --with retry kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
        f"--sys-tag test_sys_t1:sys_tag_value "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    run_cmd_with_backoff_from_platform_errors(test_cmd, correct_return_code=0)

    return


def run_cmd_with_backoff_from_platform_errors(
    kfp_run_cmd: str, correct_return_code: int
) -> Tuple[str, str]:
    # Within this function, we use the special feature of subprocess_tee which allows us
    # to capture both stdout and stderr (akin to stdout=PIPE, stderr=PIPE in the regular subprocess.run)
    # as well as output to stdout and stderr (which users can see on the Gitlab logs). We check
    # if the error message is due to a KFAM issue, and if so, we do an exponential backoff.

    backoff_intervals_in_seconds: List[int] = [0, 2, 4, 8, 16, 32]

    platform_error_messages: List[str] = [
        "Reason: Unauthorized",
        "Reason: Forbidden",
        "Failed to connect to the KFAM service",
        "Failed to create a new experiment",
    ]

    for interval in backoff_intervals_in_seconds:
        time.sleep(interval)

        run_and_wait_process: CompletedProcess = run(
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

    kfp_run_id: str = re.search(
        "Metaflow run_id=(.*)\n", run_and_wait_process.stderr
    ).group(1)
    argo_workflow_output_string: str = re.search(
        "Argo workflow: (.*)\n", run_and_wait_process.stderr
    ).group(1)
    argo_workflow_name: str = argo_workflow_output_string.split(" ")[-1]

    return kfp_run_id, argo_workflow_name


def exists_nvidia_accelerator(node_selector_term: Dict) -> bool:
    for affinity_match_expression in node_selector_term["matchExpressions"]:
        if (
            affinity_match_expression["key"] == "k8s.amazonaws.com/accelerator"
            and affinity_match_expression["operator"] == "In"
            and "nvidia-tesla-v100" in affinity_match_expression["values"]
        ):
            return True
    return False


def has_node_toleration(
    step_template, key, value, operator="Equal", effect="NoSchedule"
):
    return any(
        toleration.get("key") == key
        and toleration.get("value") == value
        and toleration.get("operator") == operator
        and toleration.get("effect") == effect
        for toleration in step_template.get("tolerations", [])
    )


def test_toleration_and_affinity_compile_only() -> None:
    step_templates: Dict[str, str] = {}
    with tempfile.TemporaryDirectory() as yaml_tmp_dir:
        yaml_file_path: str = join(yaml_tmp_dir, "toleration_and_affinity_flow.yaml")

        compile_to_yaml_cmd: str = (
            f"{_python()} flows/toleration_and_affinity_flow.py --datastore=s3 --with retry kfp run"
            f" --no-s3-code-package --yaml-only --pipeline-path {yaml_file_path}"
        )

        compile_to_yaml_process: CompletedProcess = run(
            compile_to_yaml_cmd,
            universal_newlines=True,
            shell=True,
        )
        assert compile_to_yaml_process.returncode == 0

        with open(f"{yaml_file_path}", "r") as stream:
            try:
                flow_yaml: dict = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        for step in flow_yaml["spec"]["templates"]:
            # step name in yaml use "-" in place of "_"
            step_templates[step["name"].replace("-", "_")] = step

    # Test accelerator deco: Both affinity and toleration need to be added
    assert any(
        exists_nvidia_accelerator(node_selector_term)
        for node_selector_term in step_templates["start"]["affinity"]["nodeAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ]["nodeSelectorTerms"]
    )
    assert has_node_toleration(
        step_template=step_templates["start"],
        key="k8s.amazonaws.com/accelerator",
        value="nvidia-tesla-v100",
    )

    # Test toleration generated from resource spec for CPU pods
    assert not has_node_toleration(
        step_template=step_templates["small_default_pod"],
        key="node.k8s.zgtools.net/purpose",
        value="high-memory",
    )
    assert not has_node_toleration(
        step_template=step_templates["small_cpu_pod"],
        key="node.k8s.zgtools.net/purpose",
        value="high-memory",
    )
    assert not has_node_toleration(
        step_template=step_templates["small_memory_pod"],
        key="node.k8s.zgtools.net/purpose",
        value="high-memory",
    )
    assert has_node_toleration(
        step_template=step_templates["large_cpu_pod"],
        key="node.k8s.zgtools.net/purpose",
        value="high-memory",
    )
    assert has_node_toleration(
        step_template=step_templates["large_memory_pod"],
        key="node.k8s.zgtools.net/purpose",
        value="high-memory",
    )
    assert has_node_toleration(
        step_template=step_templates["large_memory_cpu_pod"],
        key="node.k8s.zgtools.net/purpose",
        value="high-memory",
    )
