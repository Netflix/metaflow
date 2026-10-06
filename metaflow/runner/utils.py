import os
import ast
import time
import asyncio
import tempfile
import select
try:
    import fcntl
except ImportError:  # pragma: no cover - Windows / unsupported platforms
    fcntl = None
from contextlib import contextmanager
from subprocess import CalledProcessError
from typing import Any, Dict, TYPE_CHECKING, ContextManager, Tuple

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


def check_process_exited(
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
) -> bool:
    if isinstance(command_obj.process, asyncio.subprocess.Process):
        return command_obj.process.returncode is not None
    else:
        return command_obj.process.poll() is not None


@contextmanager
def temporary_fifo() -> ContextManager[Tuple[str, int]]:
    """
    Create and open a temporary file descriptor that can act like a FIFO on
    platforms without POSIX FIFO support.

    Returns
    -------
    str
        Path to the temporary FIFO-like file.
    int
        File descriptor of the temporary file.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        path = os.path.join(temp_dir, "fifo")
        if hasattr(os, "mkfifo"):
            os.mkfifo(path)
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        else:
            with open(path, "wb+") as handle:
                fd = os.open(path, os.O_RDWR | os.O_BINARY)
                try:
                    yield path, fd
                finally:
                    os.close(fd)
                return
        try:
            yield path, fd
        finally:
            os.close(fd)


def read_from_fifo_when_ready(
    fifo_fd: int,
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
    encoding: str = "utf-8",
    timeout: int = 3600,
) -> str:
    """
    Read the content from a temporary FIFO-like file descriptor when it is ready.

    On Windows, regular temporary files are used instead of POSIX FIFOs. In that
    case we poll the file length rather than relying on ``select.poll()``.
    """
    content = bytearray()
    if not hasattr(select, "poll") or os.name == "nt":
        deadline = time.time() + timeout
        read_offset = 0
        while time.time() < deadline:
            if check_process_exited(command_obj) and command_obj.process.returncode != 0:
                raise CalledProcessError(
                    command_obj.process.returncode, command_obj.command
                )
            try:
                os.lseek(fifo_fd, read_offset, os.SEEK_SET)
                size = os.fstat(fifo_fd).st_size
            except OSError:
                size = read_offset

            if size <= read_offset:
                if check_process_exited(command_obj):
                    break
                time.sleep(0.05)
                continue

            try:
                os.lseek(fifo_fd, read_offset, os.SEEK_SET)
                chunk = os.read(fifo_fd, 65536)
                if chunk:
                    content.extend(chunk)
                    read_offset += len(chunk)
                    if read_offset >= size:
                        return content.decode(encoding)
                    continue
            except OSError:
                pass

            if content and check_process_exited(command_obj):
                return content.decode(encoding)
            if check_process_exited(command_obj):
                break
            time.sleep(0.05)
        else:
            raise TimeoutError("Timeout while waiting for the file content")
        if not content and check_process_exited(command_obj):
            return ""
        if not content:
            raise TimeoutError("Timeout while waiting for the file content")
        return content.decode(encoding)

    poll = select.poll()
    poll.register(fifo_fd, select.POLLIN)
    while True:
        if check_process_exited(command_obj) and command_obj.process.returncode != 0:
            raise CalledProcessError(
                command_obj.process.returncode, command_obj.command
            )

        if timeout < 0:
            raise TimeoutError("Timeout while waiting for the file content")

        poll_begin = time.time()
        events = poll.poll(min(10, timeout * 1000))
        timeout -= time.time() - poll_begin

        try:
            data = os.read(fifo_fd, 8192)
            if data:
                content += data
                if fcntl is not None:
                    flags = fcntl.fcntl(fifo_fd, fcntl.F_GETFL)
                    fcntl.fcntl(fifo_fd, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)
                while True:
                    chunk = os.read(fifo_fd, 8192)
                    if not chunk:
                        break
                    content += chunk
                break
            else:
                if len(events):
                    break
        except BlockingIOError:
            pass

    if not content and check_process_exited(command_obj):
        raise CalledProcessError(command_obj.process.returncode, command_obj.command)

    return content.decode(encoding)


async def async_read_from_fifo_when_ready(
    fifo_fd: int,
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
    encoding: str = "utf-8",
    timeout: int = 3600,
) -> str:
    """
    Read the content from the FIFO file descriptor when it is ready.

    Parameters
    ----------
    fifo_fd : int
        File descriptor of the FIFO.
    command_obj : CommandManager
        Command manager object that handles the write side of the FIFO.
    encoding : str, optional
        Encoding to use while reading the file, by default "utf-8".
    timeout : int, optional
        Timeout for reading the file in seconds, by default 3600.

    Returns
    -------
    str
        Content read from the FIFO.

    Raises
    ------
    TimeoutError
        If no event occurs on the FIFO within the timeout.
    CalledProcessError
        If the process managed by `command_obj` has exited without writing any
        content to the FIFO.
    """
    return await asyncio.to_thread(
        read_from_fifo_when_ready, fifo_fd, command_obj, encoding, timeout
    )


def make_process_error_message(
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
):
    stdout_log = open(command_obj.log_files["stdout"], encoding="utf-8").read()
    stderr_log = open(command_obj.log_files["stderr"], encoding="utf-8").read()
    command = " ".join(command_obj.command)
    error_message = "Error executing: '%s':\n" % command
    if stdout_log.strip():
        error_message += "\nStdout:\n%s\n" % stdout_log
    if stderr_log.strip():
        error_message += "\nStderr:\n%s\n" % stderr_log
    return error_message


def handle_timeout(
    attribute_file_fd: int,
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
    file_read_timeout: int,
):
    """
    Handle the timeout for a running subprocess command that reads a file
    and raises an error with appropriate logs if a TimeoutError occurs.

    Parameters
    ----------
    attribute_file_fd : int
        File descriptor belonging to the FIFO containing the attribute data.
    command_obj : CommandManager
        Command manager object that encapsulates the running command details.
    file_read_timeout : int
        Timeout for reading the file, in seconds

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
        return read_from_fifo_when_ready(
            attribute_file_fd, command_obj=command_obj, timeout=file_read_timeout
        )
    except (CalledProcessError, TimeoutError) as e:
        raise RuntimeError(make_process_error_message(command_obj)) from e


async def async_handle_timeout(
    attribute_file_fd: "int",
    command_obj: "metaflow.runner.subprocess_manager.CommandManager",
    file_read_timeout: int,
):
    """
    Handle the timeout for a running subprocess command that reads a file
    and raises an error with appropriate logs if a TimeoutError occurs.

    Parameters
    ----------
    attribute_file_fd : int
        File descriptor belonging to the FIFO containing the attribute data.
    command_obj : CommandManager
        Command manager object that encapsulates the running command details.
    file_read_timeout : int
        Timeout for reading the file, in seconds

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
        return await async_read_from_fifo_when_ready(
            attribute_file_fd, command_obj=command_obj, timeout=file_read_timeout
        )
    except (CalledProcessError, TimeoutError) as e:
        raise RuntimeError(make_process_error_message(command_obj)) from e


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


@contextmanager
def with_dir(new_dir):
    current_dir = os.getcwd()
    os.chdir(new_dir)
    yield new_dir
    os.chdir(current_dir)
