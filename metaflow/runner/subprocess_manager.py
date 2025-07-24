import asyncio
import os
import time
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
from typing import Callable, Dict, Iterator, List, Optional, Tuple

from metaflow.packaging_sys import MetaflowCodeContent
from metaflow.util import get_metaflow_root
from .utils import check_process_exited


def kill_processes_and_descendants(pids: List[str], termination_timeout: float):
    # TODO: there's a race condition that new descendants might
    # spawn b/w the invocations of 'pkill' and 'kill'.
    # Needs to be fixed in future.
    try:
        subprocess.check_call(["pkill", "-TERM", "-P", *pids])
        subprocess.check_call(["kill", "-TERM", *pids])
    except subprocess.CalledProcessError:
        pass

    time.sleep(termination_timeout)

    try:
        subprocess.check_call(["pkill", "-KILL", "-P", *pids])
        subprocess.check_call(["kill", "-KILL", *pids])
    except subprocess.CalledProcessError:
        pass


async def async_kill_processes_and_descendants(
    pids: List[str], termination_timeout: float
):
    # TODO: there's a race condition that new descendants might
    # spawn b/w the invocations of 'pkill' and 'kill'.
    # Needs to be fixed in future.
    try:
        sub_term = await asyncio.create_subprocess_exec("pkill", "-TERM", "-P", *pids)
        await sub_term.wait()
    except Exception:
        pass

    try:
        main_term = await asyncio.create_subprocess_exec("kill", "-TERM", *pids)
        await main_term.wait()
    except Exception:
        pass

    await asyncio.sleep(termination_timeout)

    try:
        sub_kill = await asyncio.create_subprocess_exec("pkill", "-KILL", "-P", *pids)
        await sub_kill.wait()
    except Exception:
        pass

    try:
        main_kill = await asyncio.create_subprocess_exec("kill", "-KILL", *pids)
        await main_kill.wait()
    except Exception:
        pass


class LogReadTimeoutError(Exception):
    """Exception raised when reading logs times out."""


class SubprocessManager(object):
    """
    A manager for subprocesses. The subprocess manager manages one or more
    CommandManager objects, each of which manages an individual subprocess.
    """

    def __init__(self):
        self.commands: Dict[int, CommandManager] = {}

        try:
            try:
                loop = asyncio.get_running_loop()
                loop.add_signal_handler(
                    signal.SIGINT,
                    lambda: asyncio.create_task(self._async_handle_sigint()),
                )
            except RuntimeError:
                signal.signal(signal.SIGINT, self._handle_sigint)
        except ValueError:
            sys.stderr.write(
                "Warning: Unable to set signal handlers in non-main thread. "
                "Interrupt handling will be limited.\n"
            )

    async def _async_handle_sigint(self):
        pids = [
            str(command.process.pid)
            for command in self.commands.values()
            if command.process and not check_process_exited(command)
        ]
        if pids:
            await async_kill_processes_and_descendants(pids, termination_timeout=2)

    def _handle_sigint(self, signum, frame):
        pids = [
            str(command.process.pid)
            for command in self.commands.values()
            if command.process and not check_process_exited(command)
        ]
        if pids:
            kill_processes_and_descendants(pids, termination_timeout=2)

    async def __aenter__(self) -> "SubprocessManager":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def run_command(
        self,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        show_output: bool = False,
    ) -> int:
        """
        Run a command synchronously and return its process ID.

        Note: in no case does this wait for the process to *finish*. Use sync_wait()
        to wait for the command to finish.

        Parameters
        ----------
        command : List[str]
            The command to run in List form.
        env : Optional[Dict[str, str]], default None
            Environment variables to set for the subprocess; if not specified,
            the current enviornment variables are used.
        cwd : Optional[str], default None
            The directory to run the subprocess in; if not specified, the current
            directory is used.
        show_output : bool, default False
            Suppress the 'stdout' and 'stderr' to the console by default.
            They can be accessed later by reading the files present in the
            CommandManager object:
                - command_obj.log_files["stdout"]
                - command_obj.log_files["stderr"]
        Returns
        -------
        int
            The process ID of the subprocess.
        """
        env = env or {}
        installed_root = os.environ.get("METAFLOW_EXTRACTED_ROOT", get_metaflow_root())

        for k, v in MetaflowCodeContent.get_env_vars_for_packaged_metaflow(
            installed_root
        ).items():
            if k.endswith(":"):
                # Override
                env[k[:-1]] = v
            elif k in env:
                env[k] = "%s:%s" % (v, env[k])
            else:
                env[k] = v

        command_obj = CommandManager(command, env, cwd)
        pid = command_obj.run(show_output=show_output)
        self.commands[pid] = command_obj
        return pid

    async def async_run_command(
        self,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> int:
        """
        Run a command asynchronously and return its process ID.

        Parameters
        ----------
        command : List[str]
            The command to run in List form.
        env : Optional[Dict[str, str]], default None
            Environment variables to set for the subprocess; if not specified,
            the current enviornment variables are used.
        cwd : Optional[str], default None
            The directory to run the subprocess in; if not specified, the current
            directory is used.

        Returns
        -------
        int
            The process ID of the subprocess.
        """
        env = env or {}
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = "%s:%s" % (get_metaflow_root(), env["PYTHONPATH"])
        else:
            env["PYTHONPATH"] = get_metaflow_root()

        command_obj = CommandManager(command, env, cwd)
        pid = await command_obj.async_run()
        self.commands[pid] = command_obj
        return pid

    def get(self, pid: int) -> Optional["CommandManager"]:
        """
        Get one of the CommandManager managed by this SubprocessManager.

        Parameters
        ----------
        pid : int
            The process ID of the subprocess (returned by run_command or async_run_command).

        Returns
        -------
        Optional[CommandManager]
            The CommandManager object for the given process ID, or None if not found.
        """
        return self.commands.get(pid, None)

    def cleanup(self) -> None:
        """Clean up log files for all running subprocesses."""

        for v in self.commands.values():
            v.cleanup()


class CommandManager(object):
    """A manager for an individual subprocess."""

    def __init__(
        self,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ):
        """
        Create a new CommandManager object.
        This does not run the process itself but sets it up.

        Parameters
        ----------
        command : List[str]
            The command to run in List form.
        env : Optional[Dict[str, str]], default None
            Environment variables to set for the subprocess; if not specified,
            the current enviornment variables are used.
        cwd : Optional[str], default None
            The directory to run the subprocess in; if not specified, the current
            directory is used.
        """
        self.command = command

        self.env = env if env is not None else os.environ.copy()
        self.cwd = cwd or os.getcwd()

        self.process = None
        self.stdout_thread = None
        self.stderr_thread = None
        self.run_called: bool = False
        self.timeout: bool = False
        self.log_files: Dict[str, str] = {}

    async def __aenter__(self) -> "CommandManager":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    async def wait(
        self, timeout: Optional[float] = None, stream: Optional[str] = None
    ) -> None:
        """
        Wait for the subprocess to finish, optionally with a timeout
        and optionally streaming its output.

        You can only call `wait` if `async_run` has already been called.

        Parameters
        ----------
        timeout : Optional[float], default None
            The maximum time to wait for the subprocess to finish.
            If the timeout is reached, the subprocess is killed.
        stream : Optional[str], default None
            If specified, the specified stream is printed to stdout. `stream` can
            be one of `stdout` or `stderr`.
        """

        if not self.run_called:
            raise RuntimeError("No command run yet to wait for...")

        if timeout is None:
            if stream is None:
                await self.process.wait()
            else:
                await self.emit_logs(stream)
        else:
            try:
                if stream is None:
                    await asyncio.wait_for(self.process.wait(), timeout)
                else:
                    await asyncio.wait_for(self.emit_logs(stream), timeout)
            except asyncio.TimeoutError:
                self.timeout = True
                command_string = " ".join(self.command)
                self.kill(termination_timeout=2)
                print(
                    "Timeout: The process (PID %d; command: '%s') did not complete "
                    "within %s seconds." % (self.process.pid, command_string, timeout)
                )

    def sync_wait(self):
        if not self.run_called:
            raise RuntimeError("No command run yet to wait for...")

        self.process.wait()
        self.stdout_thread.join()
        self.stderr_thread.join()

    def run(self, show_output: bool = False):
        """
        Run the subprocess synchronously. This can only be called once.

        This also waits on the process implicitly.

        Parameters
        ----------
        show_output : bool, default False
            Suppress the 'stdout' and 'stderr' to the console by default.
            They can be accessed later by reading the files present in:
                - self.log_files["stdout"]
                - self.log_files["stderr"]
        """

        if not self.run_called:
            self.temp_dir = tempfile.mkdtemp()
            stdout_logfile = os.path.join(self.temp_dir, "stdout.log")
            stderr_logfile = os.path.join(self.temp_dir, "stderr.log")

            def stream_to_stdout_and_file(pipe, log_file):
                with open(log_file, "w") as file:
                    for line in iter(pipe.readline, ""):
                        if show_output:
                            sys.stdout.write(line)
                        file.write(line)
                pipe.close()

            try:
                self.process = subprocess.Popen(
                    self.command,
                    cwd=self.cwd,
                    env=self.env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    bufsize=1,
                    universal_newlines=True,
                )

                self.log_files["stdout"] = stdout_logfile
                self.log_files["stderr"] = stderr_logfile

                self.run_called = True

                self.stdout_thread = threading.Thread(
                    target=stream_to_stdout_and_file,
                    args=(self.process.stdout, stdout_logfile),
                )
                self.stderr_thread = threading.Thread(
                    target=stream_to_stdout_and_file,
                    args=(self.process.stderr, stderr_logfile),
                )

                self.stdout_thread.start()
                self.stderr_thread.start()

                return self.process.pid
            except Exception as e:
                print("Error starting subprocess: %s" % e)
                self.cleanup()
        else:
            command_string = " ".join(self.command)
            print(
                "Command '%s' has already been called. Please create another "
                "CommandManager object." % command_string
            )

    async def async_run(self):
        """
        Run the subprocess asynchronously. This can only be called once.

        Once this is called, you can then wait on the process (using `wait`), stream
        logs (using `stream_logs`) or kill it (using `kill`).
        """

        if not self.run_called:
            self.temp_dir = tempfile.mkdtemp()
            stdout_logfile = os.path.join(self.temp_dir, "stdout.log")
            stderr_logfile = os.path.join(self.temp_dir, "stderr.log")

            try:
                # returns when process has been started,
                # not when it is finished...
                self.process = await asyncio.create_subprocess_exec(
                    *self.command,
                    cwd=self.cwd,
                    env=self.env,
                    stdout=open(stdout_logfile, "w", encoding="utf-8"),
                    stderr=open(stderr_logfile, "w", encoding="utf-8"),
                )

                self.log_files["stdout"] = stdout_logfile
                self.log_files["stderr"] = stderr_logfile

                self.run_called = True
                return self.process.pid
            except Exception as e:
                print("Error starting subprocess: %s" % e)
                self.cleanup()
        else:
            command_string = " ".join(self.command)
            print(
                "Command '%s' has already been called. Please create another "
                "CommandManager object." % command_string
            )

    async def stream_log(
        self,
        stream: str,
        position: Optional[int] = None,
        timeout_per_line: Optional[float] = None,
        log_write_delay: float = 0.01,
    ) -> Iterator[Tuple[int, str]]:
        """
        Stream logs from the subprocess line by line.

        Parameters
        ----------
        stream : str
            The stream to stream logs from. Can be one of "stdout" or "stderr".
        position : Optional[int], default None
            The position in the log file to start streaming from. If None, it starts
            from the beginning of the log file. This allows resuming streaming from
            a previously known position
        timeout_per_line : Optional[float], default None
            The time to wait for a line to be read from the log file. If None, it
            waits indefinitely. If the timeout is reached, a LogReadTimeoutError
            is raised. Note that this timeout is *per line* and not cumulative so this
            function may take significantly more time than `timeout_per_line`
        log_write_delay : float, default 0.01
            Improves the probability of getting whole lines. This setting is for
            advanced use cases.

        Yields
        ------
        Tuple[int, str]
            A tuple containing the position in the log file and the line read. The
            position returned can be used to feed into another `stream_logs` call
            for example.
        """

        if not self.run_called:
            raise RuntimeError("No command run yet to get the logs for...")

        if stream not in self.log_files:
            raise ValueError(
                "No log file found for '%s', valid values are: %s"
                % (stream, ", ".join(self.log_files.keys()))
            )

        log_file = self.log_files[stream]

        with open(log_file, mode="r", encoding="utf-8") as f:
            if position is not None:
                f.seek(position)

            while True:
                # wait for a small time for complete lines to be written to the file
                # else, there's a possibility that a line may not be completely
                # written when attempting to read it.
                # This is not a problem, but improves readability.
                await asyncio.sleep(log_write_delay)

                try:
                    if timeout_per_line is None:
                        line = f.readline()
                    else:
                        line = await asyncio.wait_for(f.readline(), timeout_per_line)
                except asyncio.TimeoutError as e:
                    raise LogReadTimeoutError(
                        "Timeout while reading a line from the log file for the "
                        "stream: %s" % stream
                    ) from e

                # when we encounter an empty line
                if not line:
                    # either the process has terminated, in which case we want to break
                    # and stop the reading process of the log file since no more logs
                    # will be written to it
                    if self.process.returncode is not None:
                        break
                    # or the process is still running and more logs could be written to
                    # the file, in which case we continue reading the log file
                    else:
                        continue

                position = f.tell()
                yield position, line.rstrip()

    async def emit_logs(
        self, stream: str = "stdout", custom_logger: Callable[..., None] = print
    ):
        """
        Helper function that can easily emit all the logs for a given stream.

        This function will only terminate when all the log has been printed.

        Parameters
        ----------
        stream : str, default "stdout"
            The stream to emit logs for. Can be one of "stdout" or "stderr".
        custom_logger : Callable[..., None], default print
            A custom logger function that takes in a string and "emits" it. By default,
            the log is printed to stdout.
        """

        async for _, line in self.stream_log(stream):
            custom_logger(line)

    def cleanup(self):
        """Clean up log files for a running subprocesses."""

        if self.run_called:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def kill(self, termination_timeout: float = 2):
        """
        Kill the subprocess and its descendants.

        Parameters
        ----------
        termination_timeout : float, default 2
            The time to wait after sending a SIGTERM to the process and its descendants
            before sending a SIGKILL.
        """

        if self.process is not None:
            kill_processes_and_descendants([str(self.process.pid)], termination_timeout)
        else:
            print("No process to kill.")


async def main():
    flow_file = "../try.py"
    from metaflow.cli import start
    from metaflow.runner.click_api import MetaflowAPI

    api = MetaflowAPI.from_cli(flow_file, start)
    command = api().run(alpha=5)
    cmd = [sys.executable, *command]

    async with SubprocessManager() as spm:
        # returns immediately
        pid = await spm.async_run_command(cmd)
        command_obj = spm.get(pid)

        print(pid)

        # this is None since the process has not completed yet
        print(command_obj.process.returncode)

        # wait / do some other processing while the process runs in background.
        # if the process finishes before this sleep period, the calls to `wait`
        # below are instantaneous since it has already ended..
        # time.sleep(10)

        # wait for process to finish
        await command_obj.wait()

        # wait for process to finish with a timeout, kill if timeout expires before completion
        await command_obj.wait(timeout=2)

        # wait for process to finish while streaming logs
        await command_obj.wait(stream="stdout")

        # wait for process to finish with a timeout while streaming logs
        await command_obj.wait(stream="stdout", timeout=3)

        # stream logs line by line and check for existence of a string, noting down the position
        interesting_position = 0
        async for position, line in command_obj.stream_log(stream="stdout"):
            print(line)
            if "alpha is" in line:
                interesting_position = position
                break

        print("ended streaming at: %s" % interesting_position)

        # wait / do some other processing while the process runs in background
        # if the process finishes before this sleep period, the streaming of logs
        # below are instantaneous since it has already ended..
        # time.sleep(10)

        # this blocks till the process completes unless we uncomment the `time.sleep` above..
        print(
            "resuming streaming from: %s while process is still running..."
            % interesting_position
        )
        async for position, line in command_obj.stream_log(
            stream="stdout", position=interesting_position
        ):
            print(line)

        # this will be instantaneous since the process has finished and we just read from the log file
        print("process has ended by now... streaming again from scratch..")
        async for position, line in command_obj.stream_log(stream="stdout"):
            print(line)

        # this will be instantaneous since the process has finished and we just read from the log file
        print(
            "process has ended by now... streaming again but from position of choice.."
        )
        async for position, line in command_obj.stream_log(
            stream="stdout", position=interesting_position
        ):
            print(line)

        # two parallel streams for stdout
        tasks = [
            command_obj.emit_logs(
                stream="stdout", custom_logger=lambda x: print("[STREAM A]: %s" % x)
            ),
            # this can be another 'command_obj' too, in which case
            # we stream logs from 2 different subprocesses in parallel :)
            command_obj.emit_logs(
                stream="stdout", custom_logger=lambda x: print("[STREAM B]: %s" % x)
            ),
        ]
        await asyncio.gather(*tasks)

        # get the location of log files..
        print(command_obj.log_files)


if __name__ == "__main__":
    asyncio.run(main())
