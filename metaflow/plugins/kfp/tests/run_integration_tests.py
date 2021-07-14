from os import listdir
from os.path import isfile, join
from subprocess_tee import run
from typing import List, Dict

from .... import R

from metaflow.exception import MetaflowException

import kfp

import pytest

import yaml
import tempfile

import time

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


def obtain_flow_file_paths(flow_dir_path: str) -> List[str]:
    file_paths = [
        file_name
        for file_name in listdir(flow_dir_path)
        if isfile(join(flow_dir_path, file_name))
        and not file_name.startswith(".")
        and not "raise_error_flow" in file_name
        and not "accelerator_flow" in file_name
    ]
    return file_paths


# this test ensures the integration tests fail correctly
def test_raise_failure_flow(pytestconfig) -> None:
    test_cmd = (
        f"{_python()} flows/raise_error_flow.py --datastore=s3 kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--max-parallelism 5 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    exponential_backoff_from_kfam_errors(test_cmd, 1)

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
        f"--max-parallelism 5 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    exponential_backoff_from_kfam_errors(test_cmd, 0)

    return


def exponential_backoff_from_kfam_errors(kfp_run_cmd: str, correct_return_code: int) -> None:
    # Within this function, we use the special feature of subprocess_tee which allows us
    # to capture both stdout and stderr (akin to stdout=PIPE, stderr=PIPE in the regular subprocess.run)
    # as well as output to stdout and stderr (which users can see on the Gitlab logs). We check
    # if the error message is due to a KFAM issue, and if so, we do an exponential backoff.

    backoff_intervals = [0, 2, 4, 8, 16, 32]

    for interval in backoff_intervals:
        time.sleep(interval)

        run_and_wait_process = run(
            kfp_run_cmd,
            universal_newlines=True,
            shell=True,
        )

        if "Reason: Unauthorized" in run_and_wait_process.stderr or "Failed to connect to the KFAM service" in run_and_wait_process.stderr:
            print(f"KFAM issue encountered. Backing off for {interval} seconds...")
            continue
        else:
            assert run_and_wait_process.returncode == correct_return_code
            break
    else:
        raise MetaflowException("KFAM issues not resolved after successive backoff attempts.")
