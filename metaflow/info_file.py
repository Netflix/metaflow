import json

from os import path

CURRENT_DIRECTORY = path.dirname(path.abspath(__file__))
INFO_FILE = path.join(path.dirname(CURRENT_DIRECTORY), "INFO")

_info_file_content = None
_info_file_present = None


def read_info_file():
    global _info_file_content
    global _info_file_present
    if _info_file_present is None:
        _info_file_present = path.exists(INFO_FILE)
        if _info_file_present:
            try:
                with open(INFO_FILE, "r", encoding="utf-8") as contents:
                    _info_file_content = json.load(contents)
            except IOError:
                pass
    if _info_file_present:
        return _info_file_content
    return None
