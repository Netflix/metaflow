from collections import namedtuple

from .inputs import Inputs
from .flow_datastore import FlowDataStore
from .datastore_set import TaskDataStoreSet
from .task_datastore import TaskDataStore

# Keep these imports at top level, other modules depend on this
from .local_storage import LocalStorage
from .s3_storage import S3Storage

from metaflow.exception import MetaflowInternalError

import sys

DatastoreAvailabilityInfo = namedtuple(
    "DatastoreAvailabilityInfo",
    ["is_available", "unavailable_reason"],
    defaults=[None, None],
)


class DatastoreSpec(object):
    @staticmethod
    def get_availability_info():
        raise NotImplementedError()

    @staticmethod
    def get_impl():
        raise NotImplementedError()


class LocalDatastoreSpec(DatastoreSpec):
    @staticmethod
    def get_availability_info():
        return DatastoreAvailabilityInfo(is_available=True)

    @staticmethod
    def get_impl():
        return LocalStorage


class S3DatastoreSpec(DatastoreSpec):
    @staticmethod
    def get_availability_info():
        return DatastoreAvailabilityInfo(is_available=True)

    @staticmethod
    def get_impl():
        return S3Storage


class AzureDatastoreSpec(DatastoreSpec):
    @staticmethod
    def get_availability_info():
        if sys.version_info[:2] < (3, 6):
            return DatastoreAvailabilityInfo(
                is_available=False,
                unavailable_reason="Metaflow may only use Azure Blob Storage with Python 3.6 or newer",
            )
        return DatastoreAvailabilityInfo(is_available=True)

    @staticmethod
    def get_impl():
        # Import here, not at top level. Only-Azure users need to install AzureStorage dependencies.
        from metaflow.datastore.azure_storage import AzureStorage

        return AzureStorage


_DATASTORE_SPECS = {
    # might be able to flatten specs to just class methods, no objects needed
    "local": LocalDatastoreSpec,
    "s3": S3DatastoreSpec,
    "azure": AzureDatastoreSpec,
}


def get_datastore_impl(ds_type):
    """Get a DatastoreStorage implementation of a certain type (e.g. "s3").

    KeyError is raised if the type is completely unrecognized.
    MetaflowInternalError is raised if the type is recognized, but is not
    available in the current environment (e.g. incompatible Python version).


    Use list_datastore_types() to get a list of supported types.
    """
    ds_spec = _DATASTORE_SPECS[ds_type]
    avail_info = ds_spec.get_availability_info()
    if avail_info.is_available:
        return ds_spec.get_impl()
    raise MetaflowInternalError(
        "Datastore %s unavailable. %s" % (ds_type, avail_info.unavailable_reason)
    )


def list_datastore_types():
    """Lists available datastore types."""
    ds_types = []
    for k, v in _DATASTORE_SPECS.items():
        if v.get_availability_info().is_available:
            ds_types.append(k)
    return ds_types
