import os
import sys
import json
import subprocess

import pytest


def test_modifying_tags_does_not_change_created_at(tmp_path, monkeypatch):
    # Isolate working directory
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", str(tmp_path))

    # Reset datastore root cache BEFORE importing Flow
    from metaflow.plugins.datastores.local_storage import LocalStorage
    LocalStorage.datastore_root = None

    from metaflow import Flow

    # Create minimal flow file
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

    # Execute flow via CLI in subprocess
    subprocess.check_call(
        [sys.executable, str(flow_file), "run"],
        cwd=str(tmp_path),
        env=os.environ.copy(),
    )

    # Load run via API
    runs = list(Flow("TempFlow"))
    assert runs, "No runs found for TempFlow"

    r = runs[0]

    # Dynamically resolve datastore root (avoid hardcoding .metaflow)
    storage = LocalStorage()
    datastore_root = storage.datastore_root

    assert datastore_root is not None, "Datastore root could not be resolved"

    # Construct metadata path robustly
    run_meta_dir = os.path.join(
        datastore_root,
        "TempFlow",
        r.id,
        "_meta",
    )

    self_file = os.path.join(run_meta_dir, "_self.json")

    assert os.path.exists(self_file), f"_self.json not found at {self_file}"

    # Capture original creation timestamp
    with open(self_file, "r") as f:
        before = json.load(f)["ts_epoch"]

    # Mutate tags
    r.add_tag("status:test")

    # Read again
    with open(self_file, "r") as f:
        after = json.load(f)["ts_epoch"]

    # Ensure timestamp did not change
    assert before == after, "ts_epoch changed after tag mutation"