import json
import os


def test_modifying_tags_does_not_change_created_at(tmp_path):
    from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
    from metaflow.plugins.datastores.local_storage import LocalStorage
    from metaflow.metaflow_environment import MetaflowEnvironment

    class DummyFlow:
        name = "TestFlow"

    # Preserve original class-level datastore root
    original_root = LocalStorage.datastore_root

    try:
        # Use isolated temporary directory
        LocalStorage.datastore_root = str(tmp_path)

        # Use real Metaflow environment (no fragile stubs)
        environment = MetaflowEnvironment(None)

        provider = LocalMetadataProvider(
            environment=environment,
            flow=DummyFlow(),
            event_logger=None,
            monitor=None,
        )

        run_id = provider.new_run_id()

        meta_dir = provider._get_metadir("TestFlow", run_id)
        self_file = os.path.join(meta_dir, "_self.json")

        assert os.path.exists(self_file)

        with open(self_file) as f:
            before = json.load(f)["ts_epoch"]

        # Mutate tags
        LocalMetadataProvider._mutate_user_tags_for_run(
            flow_id="TestFlow",
            run_id=run_id,
            tags_to_add=["status:test"],
            tags_to_remove=[],
        )

        with open(self_file) as f:
            after = json.load(f)["ts_epoch"]

        assert before == after

    finally:
        # Restore original state to avoid test leakage
        LocalStorage.datastore_root = original_root
        