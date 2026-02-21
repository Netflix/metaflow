import os
import subprocess
import sys

import pytest
from metaflow import Flow


def test_modifying_tags_does_not_change_created_at(tmp_path, monkeypatch):
    # Ensure both subprocess and parent use same metadata directory
    monkeypatch.setenv("METAFLOW_HOME", str(tmp_path))

    flow_file = tmp_path / "temp_flow.py"

    flow_file.write_text(
        """
from metaflow import FlowSpec, step

class TempFlow(FlowSpec):

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    TempFlow()
"""
    )

    # Run flow in subprocess
    subprocess.check_call(
        [sys.executable, str(flow_file), "run"],
        cwd=str(tmp_path),
        env=os.environ.copy(),
    )

    # Now parent process reads from same METAFLOW_HOME
    runs = list(Flow("TempFlow"))
    assert runs, "No runs found"

    r = runs[0]
    original_created_at = r.created_at

    r.add_tag("status:test")

    assert r.created_at == original_created_at
    