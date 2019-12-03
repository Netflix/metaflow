
from . import local, s3
from .datastore import Inputs, DataException, MetaflowDataStore
from .datastore_set import MetaflowDatastoreSet

DATASTORES = {'local': local.LocalDataStore,
              's3': s3.S3DataStore}
