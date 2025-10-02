from metaflow.metaflow_config import (
    DATASTORE_SPIN_LOCAL_DIR,
    DATASTORE_SYSROOT_SPIN,
)
from metaflow.plugins.datastores.local_storage import LocalStorage


class SpinStorage(LocalStorage):
    TYPE = "spin"
    METADATA_DIR = "_meta"
    DATASTORE_DIR = DATASTORE_SPIN_LOCAL_DIR  # ".metaflow_spin"
    SYSROOT_VAR = DATASTORE_SYSROOT_SPIN
