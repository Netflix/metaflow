import json
import os


def test_modifying_tags_does_not_change_created_at(tmp_path):
    from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
    from metaflow.plugins.datastores.local_storage import LocalStorage

    # Minimal environment stub
    class DummyEnvironment:
        def get_environment_info(self):
            return {}

    # Minimal flow stub
    class DummyFlow:
        name = "TestFlow"

    original_root = LocalStorage.datastore_root

    try:
        LocalStorage.datastore_root = str(tmp_path)

        provider = LocalMetadataProvider(
            environment=DummyEnvironment(),
            flow=DummyFlow(),
            event_logger=None,
            monitor=None,
        )

        run_id = provider.new_run_id()

        meta_dir = provider._get_metadir("TestFlow", run_id)
        self_file = os.path.join(meta_dir, "_self.json")

        with open(self_file) as f:
            before = json.load(f)["ts_epoch"]

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
        LocalStorage.datastore_root = original_root