import errno
import os
import json
import fcntl
import platform

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    CONDA_MAGIC_FILE,
    CONDA_S3ROOT,
    CONDA_PACKAGE_S3ROOT,
    CONDA_AZUREROOT,
    CONDA_PACKAGE_AZUREROOT,
)

from metaflow.metaflow_environment import InvalidEnvironmentException


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


def get_conda_root(datastore_type):
    if datastore_type == "s3":
        if CONDA_S3ROOT is None:
            # We error on METAFLOW_DATASTORE_SYSROOT_S3 because that is the default used
            raise MetaflowException(msg="METAFLOW_DATASTORE_SYSROOT_S3 must be set!")
        return CONDA_S3ROOT
    elif datastore_type == "azure":
        if CONDA_AZUREROOT is None:
            # We error on METAFLOW_DATASTORE_SYSROOT_AZURE because that is the default used
            raise MetaflowException(msg="METAFLOW_DATASTORE_SYSROOT_AZURE must be set!")
        return CONDA_AZUREROOT


def get_conda_package_root(datastore_type):
    if datastore_type == "s3":
        if CONDA_PACKAGE_S3ROOT is None:
            # We error on METAFLOW_DATASTORE_SYSROOT_S3 because that is the default used
            raise MetaflowException(msg="METAFLOW_DATASTORE_SYSROOT_S3 must be set!")
        return CONDA_PACKAGE_S3ROOT
    elif datastore_type == "azure":
        if CONDA_PACKAGE_AZUREROOT is None:
            # We error on METAFLOW_DATASTORE_SYSROOT_AZURE because that is the default used
            raise MetaflowException(msg="METAFLOW_DATASTORE_SYSROOT_AZURE must be set!")
        return CONDA_PACKAGE_AZUREROOT


def arch_id():
    bit = "32"
    if platform.machine().endswith("64"):
        bit = "64"
    if platform.system() == "Linux":
        return "linux-%s" % bit
    elif platform.system() == "Darwin":
        # Support M1 Mac
        if platform.machine() == "arm64":
            return "osx-arm64"
        else:
            return "osx-%s" % bit
    else:
        raise InvalidEnvironmentException(
            "The *@conda* decorator is not supported "
            "outside of Linux and Darwin platforms"
        )
