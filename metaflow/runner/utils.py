import os
import ast
import time
import asyncio

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


def check_process_status(command_obj: "CommandManager"):
    if isinstance(command_obj.process, asyncio.subprocess.Process):
        return command_obj.process.returncode is not None
    else:
        return command_obj.process.poll() is not None


def read_from_file_when_ready(
    file_path: str, command_obj: "CommandManager", timeout: float = 5
):
    start_time = time.time()
    with open(file_path, "r", encoding="utf-8") as file_pointer:
        content = file_pointer.read()
        while not content:
            if check_process_status(command_obj):
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


def handle_timeout(
    tfp_runner_attribute, command_obj: "CommandManager", file_read_timeout: int
):
    """
    Handle the timeout for a running subprocess command that reads a file
    and raises an error with appropriate logs if a TimeoutError occurs.

    Parameters
    ----------
    tfp_runner_attribute : NamedTemporaryFile
        Temporary file that stores runner attribute data.
    command_obj : CommandManager
        Command manager object that encapsulates the running command details.
    file_read_timeout : int
        Timeout for reading the file.

    Returns
    -------
    str
        Content read from the temporary file.

    Raises
    ------
    RuntimeError
        If a TimeoutError occurs, it raises a RuntimeError with the command's
        stdout and stderr logs.
    """
    try:
        content = read_from_file_when_ready(
            tfp_runner_attribute.name, command_obj, timeout=file_read_timeout
        )
        return content
    except (CalledProcessError, TimeoutError) as e:
        stdout_log = open(command_obj.log_files["stdout"]).read()
        stderr_log = open(command_obj.log_files["stderr"]).read()
        command = " ".join(command_obj.command)
        error_message = "Error executing: '%s':\n" % command
        if stdout_log.strip():
            error_message += "\nStdout:\n%s\n" % stdout_log
        if stderr_log.strip():
            error_message += "\nStderr:\n%s\n" % stderr_log
        raise RuntimeError(error_message) from e
