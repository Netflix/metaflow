import json
import os

from enum import Enum
from typing import Any, Dict, Optional, Union

from .util import get_metaflow_root

_info_file_content = None
_info_file_present = None
_included_dist_info = None
_included_dist_present = None

# Ideally these would be in package/mfenv.py but that would cause imports to fail so
# moving here. The reason is that this is needed to read extension information which needs
# to happen before mfenv gets packaged.

MFENV_DIR = (
    ".mfenv"  # Directory containing "system" code (metaflow and user dependencies)
)
MFCONF_DIR = ".mfconf"  # Directory containing Metaflow's configuration files
MFENV_MARKER = (
    ".mfenv_install"  # Special file containing metadata about how Metaflow is packaged
)


class MetaFile(Enum):
    INFO_FILE = "INFO"
    CONFIG_FILE = "CONFIG_PARAMETERS"
    INCLUDED_DIST_INFO = "INCLUDED_DIST_INFO"


def meta_file_name(name: Union[MetaFile, str]) -> str:
    if isinstance(name, MetaFile):
        return name.value
    return name


def generic_get_filename(
    name: Union[MetaFile, str], is_meta: Optional[bool] = None
) -> Optional[str]:
    # We are not in a MFEnv package (so this is an old style package). Everything
    # is at metaflow root. There is no distinction between code and config.
    real_name = meta_file_name(name)

    path_to_file = os.path.join(get_metaflow_root(), real_name)
    if os.path.isfile(path_to_file):
        return path_to_file
    return None


def v1_get_filename(
    name: Union[MetaFile, str],
    meta_info: Dict[str, Any],
    is_meta: Optional[bool] = None,
) -> Optional[str]:
    if is_meta is None:
        is_meta = isinstance(name, MetaFile)
    if is_meta:
        conf_dir = meta_info.get("conf_dir")
        if conf_dir is None:
            raise ValueError(
                "Invalid package -- package info does not contain conf_dir key"
            )
        return os.path.join(conf_dir, meta_file_name(name))
    # Not meta -- so code
    code_dir = meta_info.get("code_dir")
    if code_dir is None:
        raise ValueError(
            "Invalid package -- package info does not contain code_dir key"
        )
    return os.path.join(code_dir, meta_file_name(name))


get_filname_map = {1: v1_get_filename}


def get_filename(
    name: Union[MetaFile, str], is_meta: Optional[bool] = None
) -> Optional[str]:
    if os.path.exists(os.path.join(get_metaflow_root(), MFENV_MARKER)):
        with open(
            os.path.join(get_metaflow_root(), MFENV_MARKER), "r", encoding="utf-8"
        ) as f:
            meta_info = json.load(f)
        version = meta_info.get("version")
        if version not in get_filname_map:
            raise ValueError(
                "Unsupported packaging version '%s'. Please update Metaflow" % version
            )
        return get_filname_map[version](name, meta_info, is_meta)
    return generic_get_filename(name, is_meta)


def read_info_file():
    # The info file is a bit special because it needs to be read to determine what
    # extensions to load. We need to therefore not load anything yet. This explains
    # the slightly wheird code structure where there is a bit of the file loading logic
    # here that is then used in MFEnv (it logically belongs in MFEnv but that file can't
    # be loaded just yet).
    global _info_file_content
    global _info_file_present

    if _info_file_present is None:
        info_filename = get_filename(MetaFile.INFO_FILE)
        if info_filename is not None:
            with open(info_filename, "r", encoding="utf-8") as f:
                _info_file_content = json.load(f)
                _info_file_present = True
        else:
            _info_file_present = False
    if _info_file_present:
        return _info_file_content
    return None


def read_included_dist_info():
    global _included_dist_info
    global _included_dist_present

    from metaflow.package.mfenv import MFEnv

    if _included_dist_present is None:
        c = MFEnv.get_content(MetaFile.INCLUDED_DIST_INFO)
        if c is not None:
            _included_dist_info = json.loads(c.decode("utf-8"))
            _included_dist_present = True
        else:
            _included_dist_present = False
    if _included_dist_present:
        return _included_dist_info
    return None
