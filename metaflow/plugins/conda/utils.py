# pyright: strict, reportTypeCommentUsage=false

import os
import platform
import re
import subprocess

from hashlib import md5
from shutil import which
from typing import NamedTuple, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    CONDA_MAGIC_FILE,  # type: ignore
    CONDA_PREFERRED_FORMAT,  # type: ignore
    CONDA_S3ROOT,  # type: ignore
    CONDA_AZUREROOT,  # type: ignore
)

from metaflow.metaflow_environment import InvalidEnvironmentException

# NOTA: Most of the code does not assume that there are only two formats BUT the
# transmute code does (since you can only specify the infile -- the outformat and file
# are inferred)
_ALL_CONDA_FORMATS = (".tar.bz2", ".conda")

# List of formats that guarantees the preferred format is first. This is important as
# functions that rely on selecting the "preferred" source of a package rely on the
# preferred format being first.
CONDA_FORMATS = (
    CONDA_PREFERRED_FORMAT,
    *[x for x in _ALL_CONDA_FORMATS if x != CONDA_PREFERRED_FORMAT],
)  # type: Tuple[str, ...]
TRANSMUT_PATHCOMPONENT = "_transmut"


# On Linux systems, called md5sum and md5 on mac
_md5sum_path = which("md5sum") or which("md5")


class CondaException(MetaflowException):
    headline = "Conda ran into an error while setting up environment."

    def __init__(self, error: Union[Sequence[Exception], str]):
        if isinstance(error, list):
            error = "\n".join([str(x) for x in error])
        super(CondaException, self).__init__(error)  # type: ignore


class CondaStepException(CondaException):
    def __init__(self, exception: CondaException, steps: Sequence[str]):
        msg = "Step(s): {steps}, Error: {error}".format(
            steps=steps, error=exception.message
        )
        super(CondaStepException, self).__init__(msg)


def get_md5_hash(path: str) -> str:
    if False and _md5sum_path:
        try:
            md5sum_str = subprocess.check_output([_md5sum_path, path]).decode(
                encoding="utf-8"
            )
            # Linux and Mac return slightly different formats but the MD5 string is
            # fairly easy to identify.
            md5sum_match = re.search(r"[0-9a-f]{32}", md5sum_str)
            if md5sum_match:
                return md5sum_match[0]
        except subprocess.CalledProcessError:
            pass
    # If we are here, we couldn't compute with the subprocess way so fall back
    # debug.conda_exec("WARN: Fallback to internal md5 computation (slow) for %s" % path)
    md5_hash = md5()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(8192), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()


def convert_filepath(path: str, file_format: Optional[str] = None) -> Tuple[str, str]:
    if file_format and not path.endswith(file_format):
        for f in CONDA_FORMATS:
            if path.endswith(f):
                path = path[: -len(f)] + file_format
                break
        else:
            raise ValueError(
                "URL '%s' does not end with a supported file format %s"
                % (path, str(CONDA_FORMATS))
            )
    return os.path.split(path)


def get_conda_manifest_path(ds_root: str) -> str:
    return os.path.join(ds_root, CONDA_MAGIC_FILE)  # type: ignore


def get_conda_root(datastore_type: str) -> str:
    if datastore_type == "s3":
        if CONDA_S3ROOT is None:
            # We error on METAFLOW_DATASTORE_SYSROOT_S3 because that is the default used
            raise MetaflowException(msg="METAFLOW_DATASTORE_SYSROOT_S3 must be set!")
        return CONDA_S3ROOT  # type: ignore
    elif datastore_type == "azure":
        if CONDA_AZUREROOT is None:
            # We error on METAFLOW_DATASTORE_SYSROOT_AZURE because that is the default used
            raise MetaflowException(msg="METAFLOW_DATASTORE_SYSROOT_AZURE must be set!")
        return CONDA_AZUREROOT  # type: ignore
    raise CondaException("Unknown datastore type: %s" % datastore_type)


def arch_id() -> str:
    bit = "32"
    if platform.machine().endswith("64"):
        bit = "64"
    if platform.system() == "Linux":
        return "linux-%s" % bit
    elif platform.system() == "Darwin":
        # Support M1 Macmetaf
        if platform.machine() == "arm64":
            return "osx-arm64"
        else:
            return "osx-%s" % bit
    else:
        raise InvalidEnvironmentException(
            "The *@conda* decorator is not supported "
            "outside of Linux and Darwin platforms"
        )


ParseExplicitResult = NamedTuple(
    "ParseExplicitResult",
    [("filename", str), ("url", str), ("url_format", str), ("hash", str)],
)


def parse_explicit_url(url: str) -> ParseExplicitResult:
    # Takes a URL in the form url#hash and returns:
    #  - the filename
    #  - the URL (without the hash)
    #  - the format for the URL
    #  - the hash
    filename = None
    url_format = None

    url_clean, url_hash = url.rsplit("#", 1)

    filename = os.path.split(urlparse(url_clean).path)[1]
    for f in _ALL_CONDA_FORMATS:
        if filename.endswith(f):
            url_format = f
            filename = filename[: -len(f)]
            break
    else:
        raise CondaException(
            "URL '%s' is not a supported format (%s)" % (url, CONDA_FORMATS)
        )
    return ParseExplicitResult(
        filename=filename, url=url_clean, url_format=url_format, hash=url_hash
    )
