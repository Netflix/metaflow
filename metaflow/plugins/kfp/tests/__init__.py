import os

from typing import List
from metaflow import R


def _python():
    if R.use_r():
        return "python3"
    else:
        return "python"


def obtain_flow_file_paths(flow_dir_path: str) -> List[str]:
    file_paths: List[str] = [
        file_name
        for file_name in os.listdir(flow_dir_path)
        if os.path.isfile(os.path.join(flow_dir_path, file_name))
        and not file_name.startswith(".")
    ]
    return file_paths
