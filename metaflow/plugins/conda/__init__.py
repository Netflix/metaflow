import errno
import os
import json
import fcntl

from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    CONDA_PACKAGE_S3ROOT,
    DATASTORE_SYSROOT_S3,
    CONDA_PACKAGE_AZUREROOT,
    CONDA_PACKAGE_GSROOT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
)

CONDA_MAGIC_FILE = "conda.dependencies"


def get_conda_manifest_path(ds_root, flow_name):
    return os.path.join(ds_root, flow_name, CONDA_MAGIC_FILE)


def read_conda_manifest(ds_root, flow_name):
    path = get_conda_manifest_path(ds_root, flow_name)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path) as f:
            return json.load(f)
    else:
        return {}


def write_to_conda_manifest(ds_root, flow_name, key, value):
    path = get_conda_manifest_path(ds_root, flow_name)
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as x:
        if x.errno != errno.EEXIST:
            raise
    with os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT), "r+") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
            data = {}
            if os.path.getsize(path) > 0:
                f.seek(0)
                data = json.load(f)
            data[key] = value
            f.seek(0)
            json.dump(data, f)
            f.truncate()
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def get_conda_package_root(datastore_type):
    # Yes, code duplication. But easier to read this way, at N=2
    if datastore_type == "s3":
        if CONDA_PACKAGE_S3ROOT is None:
            if DATASTORE_SYSROOT_S3 is None:
                raise MetaflowException(
                    msg="METAFLOW_DATASTORE_SYSROOT_S3 must be set!"
                )
            return "%s/conda" % DATASTORE_SYSROOT_S3
        else:
            return CONDA_PACKAGE_S3ROOT
    elif datastore_type == "azure":
        if CONDA_PACKAGE_AZUREROOT is None:
            if DATASTORE_SYSROOT_AZURE is None:
                raise MetaflowException(
                    msg="METAFLOW_DATASTORE_SYSROOT_AZURE must be set!"
                )
            return "%s/conda" % DATASTORE_SYSROOT_AZURE
        else:
            return CONDA_PACKAGE_AZUREROOT
    elif datastore_type == "gs":
        if CONDA_PACKAGE_GSROOT is None:
            if DATASTORE_SYSROOT_GS is None:
                raise MetaflowException(
                    msg="METAFLOW_DATASTORE_SYSROOT_GS must be set!"
                )
            return "%s/conda" % DATASTORE_SYSROOT_GS
        else:
            return CONDA_PACKAGE_GSROOT
    else:
        raise MetaflowInternalError(
            msg="Unsupported storage backend '%s' for working with Conda"
            % (datastore_type,)
        )
