import platform
import sys

from metaflow._vendor.packaging.tags import (
    _cpython_abis,
    compatible_tags,
    cpython_tags,
    mac_platforms,
)
from metaflow.exception import MetaflowException


def conda_platform():
    # Returns the conda platform for the Python interpreter
    _32_bit_interpreter = sys.maxsize <= 2**32
    if platform.system() == "Linux":
        if _32_bit_interpreter:
            return "linux-32"
        else:
            return "linux-64"
    elif platform.system() == "Darwin":
        if platform.machine() == "arm64":
            return "osx-arm64"
        elif _32_bit_interpreter:
            return "osx-32"
        else:
            return "osx-64"


def pip_tags(python_version, mamba_platform):
    # Returns a list of pip tags containing (implementation, platforms, abis) tuples
    # assuming a CPython implementation for Python interpreter.

    # Inspired by https://github.com/pypa/pip/blob/0442875a68f19b0118b0b88c747bdaf6b24853ba/src/pip/_internal/utils/compatibility_tags.py
    py_version = tuple(map(int, python_version.split(".")[:2]))
    if mamba_platform == "linux-64":
        platforms = [
            "manylinux%s_x86_64" % s
            for s in (
                "1",
                "2010",
                "2014",
                "_2_17",
                "_2_18",
                "_2_19",
                "_2_20",
                "_2_21",
                "_2_23",
                "_2_24",
                "_2_25",
                "_2_26",
                "_2_27",
            )
        ]
        platforms.append("linux_x86_64")
    elif mamba_platform == "osx-64":
        platforms = mac_platforms(arch="x86_64")
    elif mamba_platform == "osx-arm64":
        platforms = mac_platforms(arch="arm64")
    else:
        raise MetaflowException("Unsupported platform: %s" % arch)

    interpreter = "cp%s" % ("".join(map(str, py_version)))

    abis = _cpython_abis(py_version)

    supported = []
    supported.extend(cpython_tags(py_version, abis, platforms))
    supported.extend(compatible_tags(py_version, interpreter, platforms))
    return supported
