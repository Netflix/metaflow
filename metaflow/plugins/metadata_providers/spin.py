from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
from metaflow.metaflow_config import DATASTORE_SPIN_LOCAL_DIR


class SpinMetadataProvider(LocalMetadataProvider):
    TYPE = "spin"
    LOCAL_DIR = DATASTORE_SPIN_LOCAL_DIR

    def __init__(self, environment, flow, event_logger, monitor):
        super(SpinMetadataProvider, self).__init__(
            environment, flow, event_logger, monitor
        )
