import os
import time
from typing import Dict


def clear_and_set_os_environ(env: Dict):
    os.environ.clear()
    os.environ.update(env)


def read_from_file_when_ready(file_path: str, timeout: float = 5):
    start_time = time.time()
    with open(file_path, "r", encoding="utf-8") as file_pointer:
        content = file_pointer.read()
        while not content:
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    "Timeout while waiting for file content from '%s'" % file_path
                )
            time.sleep(0.1)
            content = file_pointer.read()
        return content
