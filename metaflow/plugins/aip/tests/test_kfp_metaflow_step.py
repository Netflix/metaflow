from typing import List
from unittest.mock import Mock, patch

import pytest

from metaflow.plugins.aip.aip_constants import STDERR_PATH, STDOUT_PATH
from metaflow.plugins.aip.aip_metaflow_step import _command, _step_cli

"""
To run these tests from your terminal, go to the root directory and run:

`python -m pytest metaflow/plugins/aip/tests/test_aip_metaflow_step.py -c /dev/null`

The `-c` flag above tells PyTest to ignore the setup.cfg config file which is used
for the integration tests.

"""


@pytest.fixture
def bash_capture_logs():
    with patch(
        "metaflow.plugins.aip.aip_metaflow_step.bash_capture_logs",
        return_value="bash_capture_logs_cmd",
    ) as bash_capture_logs:
        yield bash_capture_logs


@pytest.fixture
def export_mflog_env_vars():
    with patch(
        "metaflow.plugins.aip.aip_metaflow_step.export_mflog_env_vars",
        return_value="export_mflog_env_vars_cmd",
    ) as export_mflog_env_vars:
        yield export_mflog_env_vars


@pytest.mark.parametrize(
    "node_name, task_id, metaflow_run_id, namespace, tags, "
    "need_split_index, environment_type, logger_type, monitor_type, "
    "user_code_retries, workflow_name, script_name, included_vars, not_included_vars",
    [
        (
            "start",
            "kfp1",
            "aip-1234",
            "aip_namespace",
            ["tag1", "tag2"],
            True,
            "local",
            "nullSidecarLogger",
            "nullSidecarMonitor",
            3,
            "sample-workflow",
            "sample_script",
            [
                "argo-workflow:sample-workflow",
                "metaflow.plugins.aws.step_functions.set_batch_environment",
                "sample_script",
                "--environment=local",
                "--event-logger=nullSidecarLogger",
                "--max-user-code-retries 3",
                "--monitor=nullSidecarMonitor",
                "--namespace aip_namespace",
                "--run-id aip-1234",
                "--split-index",
                "--tag tag1 --tag tag2",
                "--task_id kfp1",
            ],
            [],
        ),
        (
            "end",
            "kfp1",
            "aip-1234",
            "aip_namespace",
            [],
            False,
            "local",
            "nullSidecarLogger",
            "nullSidecarMonitor",
            3,
            "sample-workflow",
            "sample_script",
            [],
            [
                "metaflow.plugins.aws.step_functions.set_batch_environment",
                "--split-index",
            ],
        ),
    ],
)
def test_step_cli(
    node_name: str,
    task_id: str,
    metaflow_run_id: str,
    namespace: str,
    tags: List[str],
    need_split_index: bool,
    environment_type: str,
    logger_type: str,
    monitor_type: str,
    user_code_retries: int,
    workflow_name: str,
    script_name: str,
    included_vars: List[str],
    not_included_vars: List[str],
):
    step_cli_cmd = _step_cli(
        node_name,
        task_id,
        metaflow_run_id,
        namespace,
        tags,
        need_split_index,
        environment_type,
        logger_type,
        monitor_type,
        user_code_retries,
        workflow_name,
        script_name,
    )

    for included_var in included_vars:
        assert included_var in step_cli_cmd
    for not_included_var in not_included_vars:
        assert not_included_var not in step_cli_cmd


def test_command(bash_capture_logs: Mock, export_mflog_env_vars: Mock):
    retry_count_python = (
        "import os;"
        'name = os.environ.get("MF_ARGO_NODE_NAME");'
        'index = name.rfind("(");'
        'retry_count = (0 if index == -1 else name[index + 1: -1]) if name.endswith(")") else 0;'
        "print(str(retry_count))"
    )

    cmd_str = _command(
        volume_dir="/opt/metaflow/volume_dir",
        step_cli="cli command",
        task_id="kfp1",
        passed_in_split_indexes="01",
        step_name="start",
        flow_name="sample_flow",
    )

    assert cmd_str == (
        "rm -rf /opt/metaflow/volume_dir/* && mkdir -p /opt/metaflow_volume/metaflow_logs && "
        "export_mflog_env_vars_cmd && bash_capture_logs_cmd;c=$?; "
        "python -m metaflow.mflog.save_logs; exit $c"
    )

    export_mflog_env_vars.assert_called_once_with(
        flow_name="sample_flow",
        run_id="{run_id}",
        step_name="start",
        task_id="kfp1.01",
        retry_count=f"`python -c '{retry_count_python}'`",
        datastore_type="s3",
        datastore_root="$METAFLOW_DATASTORE_SYSROOT_S3",
        stdout_path=STDOUT_PATH,
        stderr_path=STDERR_PATH,
    )

    bash_capture_logs.assert_called_once_with("echo 'Task is starting.' && cli command")
