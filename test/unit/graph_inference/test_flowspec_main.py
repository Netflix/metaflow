"""
Tests for FlowSpec.main() — multi-flow registry CLI router.
"""

import os
import sys
import subprocess

import pytest
from metaflow import Runner


FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")
PYTHON = sys.executable


def run_flow_script(filename, args=None):
    """Run a flow script as a subprocess, return (returncode, stdout, stderr)."""
    script = os.path.join(FLOWS_DIR, filename)
    cmd = [PYTHON, script] + (args or [])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return result.returncode, result.stdout, result.stderr


# ---------------------------------------------------------------------------
# Single flow with FlowSpec.main()
# ---------------------------------------------------------------------------


class TestSingleFlowMain:
    """FlowSpec.main() with one flow in the file."""

    def test_without_name(self):
        """Single flow: name is optional."""
        rc, _, stderr = run_flow_script("single_flow_main.py", ["show"])
        assert rc == 0
        assert "OnlyFlow" in stderr

    def test_with_name(self):
        """Single flow: explicit name also works."""
        rc, _, stderr = run_flow_script("single_flow_main.py", ["OnlyFlow", "show"])
        assert rc == 0
        assert "OnlyFlow" in stderr

    def test_run_produces_artifacts(self):
        """Single flow: actual run via Runner works."""
        flow_path = os.path.join(FLOWS_DIR, "single_flow_main.py")
        with Runner(flow_path, cwd=FLOWS_DIR).run() as running:
            run = running.run
            assert run.successful
            assert run["end"].task["val"].data == "only"


# ---------------------------------------------------------------------------
# Multiple flows with FlowSpec.main()
# ---------------------------------------------------------------------------


class TestMultiFlowMain:
    """FlowSpec.main() with multiple flows in the file."""

    def test_no_name_shows_error(self):
        """Multiple flows without a name should error and list available flows."""
        rc, _, stderr = run_flow_script("multi_flow_file.py")
        assert rc == 1
        assert "Multiple flows defined" in stderr
        assert "FlowAlpha" in stderr
        assert "FlowBeta" in stderr

    def test_select_flow_alpha_show(self):
        rc, _, stderr = run_flow_script("multi_flow_file.py", ["FlowAlpha", "show"])
        assert rc == 0
        assert "FlowAlpha" in stderr

    def test_select_flow_beta_show(self):
        rc, _, stderr = run_flow_script("multi_flow_file.py", ["FlowBeta", "show"])
        assert rc == 0
        assert "FlowBeta" in stderr

    def test_unknown_name_shows_error(self):
        rc, _, stderr = run_flow_script("multi_flow_file.py", ["NoSuchFlow", "show"])
        assert rc == 1
        assert "Multiple flows defined" in stderr

    def test_run_flow_alpha(self):
        """Run FlowAlpha from a multi-flow file."""
        rc, _, stderr = run_flow_script("multi_flow_file.py", ["FlowAlpha", "run"])
        assert rc == 0, f"FlowAlpha run failed:\n{stderr}"
        assert "FlowAlpha" in stderr

    def test_run_flow_beta(self):
        """Run FlowBeta from a multi-flow file."""
        rc, _, stderr = run_flow_script("multi_flow_file.py", ["FlowBeta", "run"])
        assert rc == 0, f"FlowBeta run failed:\n{stderr}"
        assert "FlowBeta" in stderr
