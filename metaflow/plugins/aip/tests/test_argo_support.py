import os
from tempfile import TemporaryDirectory

import pytest
import subprocess_tee

from . import _python, obtain_flow_file_paths


disabled_test_flows = [
    "aip_flow.py",  # kfp_preceding_component feature has been deprecated.
]


@pytest.mark.parametrize(
    "flow_file_path", obtain_flow_file_paths("flows", disabled_test_flows)
)
def test_argo_flows(pytestconfig, flow_file_path: str) -> None:
    """Validate that all test flows can be successfully converted to Argo Workflow / WorkflowTemplate schema."""

    full_path: str = os.path.join("flows", flow_file_path)
    flow_base_name = os.path.splitext(os.path.basename(flow_file_path))[0]

    output_formats = ["argo-workflow", "argo-workflow-template"]

    for output_format in output_formats:
        with TemporaryDirectory() as test_dir:
            output_yaml_name = f"{flow_base_name}-{output_format}.yaml"
            output_path = os.path.join(test_dir, output_yaml_name)

            test_cmd: str = (
                f"{_python()} {full_path} --datastore=s3 --with retry kfp run "
                "--argo-wait --workflow-timeout 1800 "
                "--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
                "--sys-tag test_sys_t1:sys_tag_value "
                f"--yaml-only --yaml-format {output_format} "
                f"--pipeline-path {output_path} "
            )
            if pytestconfig.getoption("image"):
                test_cmd += f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"

            assert (
                subprocess_tee.run(
                    test_cmd,
                    universal_newlines=True,
                    shell=True,
                ).returncode
                == 0
            )

            if output_format == "argo-workflow":
                validation_cmd = f"argo lint {output_path}"
            else:
                validation_cmd = f"argo template lint {output_path}"

            assert (
                subprocess_tee.run(
                    validation_cmd,
                    universal_newlines=True,
                    shell=True,
                ).returncode
                == 0
            )
