from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
from metaflow.metaflow_config import DATASTORE_SPIN_LOCAL_DIR


class SpinMetadataProvider(LocalMetadataProvider):
    TYPE = "spin"
    DATASTORE_DIR = DATASTORE_SPIN_LOCAL_DIR  # ".metaflow_spin"

    @classmethod
    def _get_storage_class(cls):
        from metaflow.plugins.datastores.spin_storage import SpinStorage

        return SpinStorage

    def version(self):
        return "spin"
