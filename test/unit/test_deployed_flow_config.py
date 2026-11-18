"""
Regression test for GitHub issue #2717.

When a flow with a deploy-time Config triggers another flow via DeployedFlow,
the parent flow's METAFLOW_FLOW_CONFIG_VALUE environment variable was leaking
into the child flow's click API initialization, causing:

    "Options were not properly set -- this is an internal error."

The fix ensures that _compute_flow_parameters skips config env var processing
when the flow has no config parameters (self._config_input is False).
"""

import importlib
import json
import os
import sys
import tempfile

import pytest


@pytest.fixture
def fake_flow_file(tmp_path):
    """Create a temporary flow file without any Config parameters."""
    flow_code = """\
from metaflow import FlowSpec, Parameter, step

class FakeTargetFlow(FlowSpec):
    alpha = Parameter("alpha", type=str, help="A param", required=False)

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    FakeTargetFlow()
"""
    flow_file = tmp_path / "fake_target_flow.py"
    flow_file.write_text(flow_code)
    return str(flow_file)


def test_no_config_flow_ignores_config_env_var(fake_flow_file, monkeypatch):
    """
    A flow without Config parameters should not fail when
    METAFLOW_FLOW_CONFIG_VALUE is set in the environment (e.g., from a parent
    flow that does have Config).

    This simulates the DeployedFlow trigger path: the parent flow sets
    METAFLOW_FLOW_CONFIG_VALUE, then DeployedFlow creates a MetaflowAPI
    from a fake flow file (no configs). Accessing a sub-command (like 'run')
    triggers _compute_flow_parameters which must not choke on the stale
    env var.
    """
    # Simulate the parent flow having set config env vars
    monkeypatch.setenv(
        "METAFLOW_FLOW_CONFIG_VALUE", json.dumps({"config": {"key": "value"}})
    )
    monkeypatch.setenv("METAFLOW_FLOW_CONFIG", json.dumps({"config": "kv.config"}))

    from metaflow.parameters import flow_context

    # Reload CLI modules to get a clean state (same as DeployerImpl does)
    with flow_context(None):
        for module_name in [
            "metaflow.cli",
            "metaflow.cli_components.run_cmds",
            "metaflow.cli_components.init_cmd",
        ]:
            if module_name in sys.modules:
                importlib.reload(sys.modules[module_name])

    from metaflow.cli import start
    from metaflow.runner.click_api import MetaflowAPI

    api_func = MetaflowAPI.from_cli(fake_flow_file, start)
    api = api_func()

    # Accessing a sub-command triggers _compute_flow_parameters which
    # previously raised "Options were not properly set" when config env
    # vars from a parent flow were present but the target flow had none.
    run_cmd = api.run
    assert run_cmd is not None
