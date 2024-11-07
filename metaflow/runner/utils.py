import os
import ast
import time
import asyncio

from subprocess import CalledProcessError
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    import tempfile
    import metaflow.runner.subprocess_manager
    import metaflow.runner.click_api


def get_current_cell(ipython):
    if ipython:
        return ipython.history_manager.input_hist_raw[-1]
    return None


def format_flowfile(cell):
    """
    Formats the given cell content to create a valid Python script that can be
    executed as a Metaflow flow.
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


def check_process_status(
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
):
    if isinstance(command_obj.process, asyncio.subprocess.Process):
        return command_obj.process.returncode is not None
    else:
        return command_obj.process.poll() is not None


def read_from_file_when_ready(
    file_path: str,
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
    timeout: float = 5,
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
    tfp_runner_attribute: "tempfile._TemporaryFileWrapper[str]",
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
    file_read_timeout: int,
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
        stdout_log = open(command_obj.log_files["stdout"], encoding="utf-8").read()
        stderr_log = open(command_obj.log_files["stderr"], encoding="utf-8").read()
        command = " ".join(command_obj.command)
        error_message = "Error executing: '%s':\n" % command
        if stdout_log.strip():
            error_message += "\nStdout:\n%s\n" % stdout_log
        if stderr_log.strip():
            error_message += "\nStderr:\n%s\n" % stderr_log
        raise RuntimeError(error_message) from e


def get_lower_level_group(
    api: "metaflow.runner.click_api.MetaflowAPI",
    top_level_kwargs: Dict[str, Any],
    sub_command: str,
    sub_command_kwargs: Dict[str, Any],
) -> "metaflow.runner.click_api.MetaflowAPI":
    """
    Retrieve a lower-level group from the API based on the type and provided arguments.

    Parameters
    ----------
    api : MetaflowAPI
        Metaflow API instance.
    top_level_kwargs : Dict[str, Any]
        Top-level keyword arguments to pass to the API.
    sub_command : str
        Sub-command of API to get the API for
    sub_command_kwargs : Dict[str, Any]
        Sub-command arguments

    Returns
    -------
    MetaflowAPI
        The lower-level group object retrieved from the API.

    Raises
    ------
    ValueError
        If the `_type` is None.
    """
    sub_command_obj = getattr(api(**top_level_kwargs), sub_command)

    if sub_command_obj is None:
        raise ValueError(f"Sub-command '{sub_command}' not found in API '{api.name}'")

    return sub_command_obj(**sub_command_kwargs)
