from .inputs import Inputs
from .flow_datastore import FlowDataStore
from .datastore_set import TaskDataStoreSet
from .task_datastore import TaskDataStore

from .local_storage import LocalStorage
from .s3_storage import S3Storage
from .azure_storage import AzureStorage
from .gs_storage import GSStorage

DATASTORES = {
    "local": LocalStorage,
    "s3": S3Storage,
    "azure": AzureStorage,
    "gs": GSStorage,
}
