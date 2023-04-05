import os
from tempfile import TemporaryDirectory

import pytest
import subprocess_tee

from . import _python, obtain_flow_file_paths


@pytest.mark.parametrize("flow_file_path", obtain_flow_file_paths("flows"))
def test_flows(pytestconfig, flow_file_path: str) -> None:
    """Validate that all test flows can be successfully converted to Argo Workflow / WorkflowTemplate schema."""

    full_path: str = os.path.join("flows", flow_file_path)
    flow_base_name = os.path.basename(flow_file_path)

    output_formats = ["argo-workflow", "argo-workflow-template"]

    for output_format in output_formats:
        output_yaml_name = f"{flow_base_name}-{output_format}.yaml"
        output_path = os.path.join(test_dir, output_yaml_name)

        with TemporaryDirectory() as test_dir:
            test_cmd: str = (
                f"{_python()} {full_path} --datastore=s3 --with retry kfp run "
                "--argo-wait --workflow-timeout 1800 "
                "--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
                "--sys-tag test_sys_t1:sys_tag_value "
                "--yaml-only --yaml-format argo-workflow-template "
                f"--pipeline-path {output_path} "
            )
            if pytestconfig.getoption("image"):
                test_cmd += f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"

            subprocess_tee.run(
                test_cmd,
                universal_newlines=True,
                shell=True,
            )
