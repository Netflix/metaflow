import os
import sys
import time
import signal
import shutil
import asyncio
import tempfile
import subprocess
from typing import List, Dict, Optional, Callable


def kill_process_and_descendants(pid, termination_timeout):
    try:
        subprocess.check_call(["pkill", "-TERM", "-P", str(pid)])
    except subprocess.CalledProcessError as e:
        pass

    time.sleep(termination_timeout)

    try:
        subprocess.check_call(["pkill", "-KILL", "-P", str(pid)])
    except subprocess.CalledProcessError as e:
        pass


class LogReadTimeoutError(Exception):
    """Exception raised when reading logs times out."""

    pass


class SubprocessManager(object):
    """A manager for subprocesses."""

    def __init__(self):
        self.commands: Dict[int, CommandManager] = {}

    async def __aenter__(self) -> "SubprocessManager":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    async def run_command(
        self,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> int:
        """Run a command asynchronously and return its process ID."""

        command_obj = CommandManager(command, env, cwd)
        pid = await command_obj.run()
        self.commands[pid] = command_obj
        return pid

    def get(self, pid: int) -> "CommandManager":
        """Get the CommandManager object for a given process ID."""

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
        self.command = command

        self.env = env if env is not None else os.environ.copy()
        self.cwd = cwd if cwd is not None else os.getcwd()

        self.process = None
        self.run_called: bool = False
        self.log_files: Dict[str, str] = {}

        signal.signal(signal.SIGINT, self.handle_sigint)

    async def __aenter__(self) -> "CommandManager":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def handle_sigint(self, signum, frame):
        """Handle the SIGINT signal."""

        asyncio.create_task(self.kill())

    async def wait(
        self, timeout: Optional[float] = None, stream: Optional[str] = None
    ) -> None:
        """Wait for the subprocess to finish, optionally with a timeout and optionally streaming its output."""

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
                command_string = " ".join(self.command)
                await self.kill()
                print(
                    "Timeout: The process (PID %d; command: '%s') did not complete within %s seconds."
                    % (self.process.pid, command_string, timeout)
                )

    async def run(self):
        """Run the subprocess, streaming the logs to temporary files"""

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
                    stdout=open(stdout_logfile, "w"),
                    stderr=open(stderr_logfile, "w"),
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
                "Command '%s' has already been called. Please create another CommandManager object."
                % command_string
            )

    async def stream_log(
        self,
        stream: str,
        position: Optional[int] = None,
        timeout_per_line: Optional[float] = None,
        log_write_delay: float = 0.01,
    ):
        """Stream logs from the subprocess using the log files"""

        if not self.run_called:
            raise RuntimeError("No command run yet to get the logs for...")

        if stream not in self.log_files:
            raise ValueError(
                "No log file found for '%s', valid values are: %s"
                % (stream, ", ".join(self.log_files.keys()))
            )

        log_file = self.log_files[stream]

        with open(log_file, mode="r") as f:
            if position is not None:
                f.seek(position)

            while True:
                # wait for a small time for complete lines to be written to the file
                # else, there's a possibility that a line may not be completely written when
                # attempting to read it. This is not a problem, but improves readability.
                await asyncio.sleep(log_write_delay)

                try:
                    if timeout_per_line is None:
                        line = f.readline()
                    else:
                        line = await asyncio.wait_for(f.readline(), timeout_per_line)
                except asyncio.TimeoutError as e:
                    raise LogReadTimeoutError(
                        "Timeout while reading a line from the log file for the stream: %s"
                        % stream
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

    async def emit_logs(self, stream: str = "stdout", custom_logger: Callable = print):
        """Helper function to iterate over stream_log"""

        async for _, line in self.stream_log(stream):
            custom_logger(line)

    def cleanup(self):
        """Clean up log files for a running subprocesses."""

        if self.run_called:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def kill(self, termination_timeout: float = 1):
        """Kill the subprocess and its descendants."""

        if self.process is not None:
            kill_process_and_descendants(self.process.pid, termination_timeout)
        else:
            print("No process to kill.")


async def main():
    flow_file = "../try.py"
    from metaflow.cli import start
    from metaflow.click_api import MetaflowAPI

    api = MetaflowAPI.from_cli(flow_file, start)
    command = api().run(alpha=5)
    cmd = [sys.executable, *command]

    async with SubprocessManager() as spm:
        # returns immediately
        pid = await spm.run_command(cmd)
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
