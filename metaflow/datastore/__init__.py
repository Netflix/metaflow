from .inputs import Inputs
from .flow_datastore import FlowDataStore
from .datastore_set import TaskDataStoreSet
from .task_datastore import TaskDataStore

from .local_storage import LocalStorage
from .s3_storage import S3Storage

DATASTORES = {"local": LocalStorage, "s3": S3Storage}
