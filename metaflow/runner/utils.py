import os
import ast
import time

from subprocess import CalledProcessError
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from .subprocess_manager import CommandManager


def get_current_cell(ipython):
    if ipython:
        return ipython.history_manager.input_hist_raw[-1]
    return None


def format_flowfile(cell):
    """
    Formats the given cell content to create a valid Python script that can be executed as a Metaflow flow.
    """
    flowspec = [
        x
        for x in ast.parse(cell).body
        if isinstance(x, ast.ClassDef) and any(b.id == "FlowSpec" for b in x.bases)
    ]

    if not flowspec:
        raise ModuleNotFoundError(
            "The cell doesn't contain any class that inherits from 'FlowSpec'"
        )

    lines = cell.splitlines()[: flowspec[0].end_lineno]
    lines += ["if __name__ == '__main__':", f"    {flowspec[0].name}()"]
    return "\n".join(lines)


def clear_and_set_os_environ(env: Dict):
    os.environ.clear()
    os.environ.update(env)


def read_from_file_when_ready(
    file_path: str, command_obj: "CommandManager", timeout: float = 5
):
    start_time = time.time()
    with open(file_path, "r", encoding="utf-8") as file_pointer:
        content = file_pointer.read()
        while not content:
            if command_obj.process.poll() is not None:
                # Check to make sure the file hasn't been read yet to avoid a race
                # where the file is written between the end of this while loop and the
                # poll call above.
                content = file_pointer.read()
                if content:
                    break
                raise CalledProcessError(
                    command_obj.process.returncode, command_obj.command
                )
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    "Timeout while waiting for file content from '%s'" % file_path
                )
            time.sleep(0.1)
            content = file_pointer.read()
        return content
