import os
import subprocess
import sys
from pathlib import Path

import pytest


FLOW_FILE = Path(__file__).parent / "flows" / "config_root_type_flow.py"
REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def run_flow(tmp_path):
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(REPO_ROOT), env.get("PYTHONPATH")))
    )

    def invoke(value, config_name="cfg", command="show"):
        return subprocess.run(
            [
                sys.executable,
                str(FLOW_FILE),
                "--config-value",
                config_name,
                value,
                command,
            ],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
        )

    return invoke


@pytest.mark.parametrize(
    ("value", "value_type"),
    [("[]", list), ("123", int), ('"text"', str), ("true", bool)],
    ids=["array", "number", "string", "boolean"],
)
def test_non_mapping_config_value_reports_usage_error(run_flow, value, value_type):
    result = run_flow(value)
    output = result.stdout + result.stderr

    assert result.returncode != 0
    assert "must be a mapping (got type %s)" % value_type.__name__ in output
    assert "Internal error" not in output


@pytest.mark.parametrize("value", ["null", "{}"], ids=["null", "object"])
def test_null_or_object_config_value_is_accepted(run_flow, value):
    result = run_flow(value)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Step start" in result.stdout + result.stderr


def test_plain_config_value_accepts_bare_string(run_flow):
    result = run_flow("a bare testvalue", config_name="barecfg", command="run")
    output = result.stdout + result.stderr

    assert result.returncode == 0, output
    assert "BareCFG:  a bare testvalue" in output
