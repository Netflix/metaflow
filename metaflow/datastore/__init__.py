from .inputs import Inputs
from .flow_datastore import FlowDataStore
from .datastore_set import TaskDataStoreSet

from .local_backend import LocalBackend
from .s3_backend import S3Backend

DATASTORES = {'local': LocalBackend,
              's3': S3Backend}
