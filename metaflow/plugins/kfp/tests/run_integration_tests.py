from os import listdir
from os.path import isfile, join
from subprocess import run, PIPE
from typing import List, Dict

from .... import R

import kfp

import pytest

import yaml
import tempfile

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
        f"--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    run_and_wait_process = run(
        test_cmd,
        universal_newlines=True,
        stdout=PIPE,
        shell=True,
    )
    # this ensures the integration testing framework correctly catches a failing flow
    # and reports the error
    assert run_and_wait_process.returncode == 1

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
            f"{_python()} flows/accelerator_flow.py --datastore=s3 kfp run "
            f" --no-s3-code-package --yaml-only --pipeline-path {yaml_file_path}"
        )

        compile_to_yaml_process = run(
            compile_to_yaml_cmd,
            universal_newlines=True,
            stdout=PIPE,
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
    # In the process below, stdout=PIPE because we only want to capture stdout.
    # The reason is that the click echo function prints to stderr, and contains
    # the main logs (run link, graph validation, package uploading, etc). We
    # want to ensure these logs are visible to users and not captured.
    # We use the print function in kfp_cli.py to print a magic token containing the
    # run id and capture this to correctly test logging. See the
    # `check_valid_logs_process` process.

    test_cmd = (
        f"{_python()} {full_path} --datastore=s3 kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    run_and_wait_process = run(
        test_cmd,
        universal_newlines=True,
        stdout=PIPE,
        shell=True,
    )
    assert run_and_wait_process.returncode == 0

    return
