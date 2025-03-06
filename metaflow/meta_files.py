import json
import os

from enum import Enum

from .util import get_metaflow_root

_info_file_content = None
_info_file_present = None
_included_dist_info = None
_included_dist_present = None

# Ideally these would be in package/mfenv.py but that would cause imports to fail so
# moving here. The reason is that this is needed to read extension information which needs
# to happen before mfenv gets packaged.

MFENV_DIR = ".mfenv"


class MetaFile(Enum):
    INFO_FILE = "INFO"
    CONFIG_FILE = "CONFIG_PARAMETERS"
    INCLUDED_DIST_INFO = "INCLUDED_DIST_INFO"


def read_info_file():

    global _info_file_content
    global _info_file_present
    if _info_file_present is None:
        file_path = os.path.join(get_metaflow_root(), MetaFile.INFO_FILE.value)
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
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
    if _included_dist_present is None:
        file_path = os.path.join(get_metaflow_root(), MetaFile.INCLUDED_DIST_INFO.value)
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                _included_dist_info = json.load(f)
            _included_dist_present = True
        else:
            _included_dist_present = False

    if _included_dist_present:
        return _included_dist_info
    return None
