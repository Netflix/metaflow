import json
import os

import pytest


def test_modifying_tags_does_not_change_created_at(tmp_path):
    """
    Ensure that mutating user tags does not modify the original
    ts_epoch (run creation timestamp) stored in _self.json.
    """

    from metaflow.plugins.metadata.local import LocalMetadataProvider
    from metaflow.plugins.datastores.local_storage import LocalStorage

    # Force datastore root explicitly (no CWD fallback)
    LocalStorage.datastore_root = str(tmp_path)

    # Create provider instance
    provider = LocalMetadataProvider(
        environment=None,
        flow="TestFlow",
        event_logger=None,
        monitor=None,
    )

    # Create new run
    run_id = provider.new_run_id()

    # Resolve metadata directory safely using provider helper
    meta_dir = provider._get_metadir("TestFlow", run_id)
    assert meta_dir is not None

    self_file = os.path.join(meta_dir, "_self.json")
    assert os.path.exists(self_file)

    # Capture original timestamp
    with open(self_file, "r") as f:
        before = json.load(f)["ts_epoch"]

    # Mutate tags
    LocalMetadataProvider._mutate_user_tags_for_run(
        flow_id="TestFlow",
        run_id=run_id,
        tags_to_add=["status:test"],
        tags_to_remove=[],
    )

    # Read timestamp again
    with open(self_file, "r") as f:
        after = json.load(f)["ts_epoch"]

    # Assert creation time unchanged
    assert before == after
    