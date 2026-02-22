import pytest

from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
from metaflow.plugins.datastores.local_storage import LocalStorage


class DummyEnvironment:
    def get_environment_info(self):
        # Minimal required structure for MetadataProvider
        return {
            "platform": "test",
            "production_token": None,
            "runtime": "dev",
        }


class DummyFlow:
    name = "TestFlow"


def test_modifying_tags_does_not_change_created_at(tmp_path):
    # Save original datastore root to avoid state leakage
    original_root = LocalStorage.datastore_root

    try:
        # Explicitly control datastore root
        LocalStorage.datastore_root = str(tmp_path)

        provider = LocalMetadataProvider(
            environment=DummyEnvironment(),
            flow=DummyFlow(),
            event_logger=None,
            monitor=None,
        )

        # Create a run
        run_id = provider.new_run_id()

        # Read ts_epoch using public API
        before = LocalMetadataProvider.get_object(
            "run", "self", {}, None, "TestFlow", run_id
        )["ts_epoch"]

        # Mutate tags
        LocalMetadataProvider._mutate_user_tags_for_run(
            flow_id="TestFlow",
            run_id=run_id,
            tags_to_add=["status:test"],
            tags_to_remove=[],
        )

        # Read again
        after = LocalMetadataProvider.get_object(
            "run", "self", {}, None, "TestFlow", run_id
        )["ts_epoch"]

        assert before == after

    finally:
        # Restore global state
        LocalStorage.datastore_root = original_root