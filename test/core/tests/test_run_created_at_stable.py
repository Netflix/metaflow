import os
import sys
import json
import subprocess

import pytest


def test_modifying_tags_does_not_change_created_at(tmp_path, monkeypatch):
    # Ensure parent & subprocess use same working directory
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", str(tmp_path))

    # Reset cached datastore root BEFORE importing Flow
    from metaflow.plugins.datastores.local_storage import LocalStorage
    LocalStorage.datastore_root = None

    from metaflow import Flow
    from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
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

    # Run the flow in subprocess
    subprocess.check_call(
        [sys.executable, str(flow_file), "run"],
        cwd=str(tmp_path),
        env=os.environ.copy(),
    )

    runs = list(Flow("TempFlow"))
    assert runs

    r = runs[0]
    
    # ✅ Correct metadata path (includes DATASTORE_LOCAL_DIR, usually ".metaflow")
    run_dir = tmp_path / DATASTORE_LOCAL_DIR / "TempFlow" / r.id / "_meta"
    self_file = run_dir / "_self.json"

    with open(self_file) as f:
        before = json.load(f)["ts_epoch"]

    # Mutate tags
    r.add_tag("status:test")

    with open(self_file) as f:
        after = json.load(f)["ts_epoch"]
        
    # Ensure creation timestamp remains unchanged
    assert before == after