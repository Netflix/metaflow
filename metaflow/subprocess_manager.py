import os
import sys
import time
import signal
import shutil
import asyncio
import aiofiles
import tempfile
from typing import List


class LogReadTimeoutError(Exception):
    pass


class SubprocessManager(object):
    def __init__(self, env=None, cwd=None):
        if env is None:
            env = os.environ.copy()
        self.env = env

        if cwd is None:
            cwd = os.getcwd()
        self.cwd = cwd

        self.process = None
        self.run_command_called = False
        self.log_files = {}

        signal.signal(signal.SIGINT, self.handle_sigint)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.cleanup()

    def handle_sigint(self, signum, frame):
        print("SIGINT received.")
        asyncio.create_task(self.kill_process())

    async def wait(self, timeout=None, stream=None):
        if timeout is None:
            if stream is None:
                await self.process.wait()
            else:
                await self.emit_logs(stream)
        else:
            tasks = [asyncio.create_task(asyncio.sleep(timeout))]
            if stream is None:
                tasks.append(asyncio.create_task(self.process.wait()))
            else:
                tasks.append(asyncio.create_task(self.emit_logs(stream)))

            await asyncio.wait(tasks, return_when="FIRST_COMPLETED")

    async def run_command(self, command: List[str]):
        self.temp_dir = tempfile.mkdtemp()
        stdout_logfile = os.path.join(self.temp_dir, "stdout.log")
        stderr_logfile = os.path.join(self.temp_dir, "stderr.log")

        try:
            # returns when subprocess has been started, not
            # when it is finished...
            self.process = await asyncio.create_subprocess_exec(
                *command,
                cwd=self.cwd,
                env=self.env,
                stdout=await aiofiles.open(stdout_logfile, "w"),
                stderr=await aiofiles.open(stderr_logfile, "w"),
            )

            self.log_files["stdout"] = stdout_logfile
            self.log_files["stderr"] = stderr_logfile

            self.run_command_called = True
            return self.process
        except Exception as e:
            print(f"Error starting subprocess: {e}")
            await self.cleanup()

    async def stream_logs(
        self, stream, position=None, timeout_per_line=None, log_write_delay=0.01
    ):
        if self.run_command_called is False:
            raise ValueError("No command run yet to get the logs for...")

        if stream not in self.log_files:
            raise ValueError(
                f"No log file found for {stream}, valid values are: {list(self.log_files.keys())}"
            )

        log_file = self.log_files[stream]

        async with aiofiles.open(log_file, mode="r") as f:
            if position is not None:
                await f.seek(position)

            while True:
                # wait for a small time for complete lines to be written to the file
                # else, there's a possibility that a line may not be completely written when
                # attempting to read it. This is not a problem, but improves readability.
                await asyncio.sleep(log_write_delay)

                try:
                    if timeout_per_line is None:
                        line = await f.readline()
                    else:
                        line = await asyncio.wait_for(f.readline(), timeout_per_line)
                except asyncio.TimeoutError as e:
                    raise LogReadTimeoutError(
                        f"Timeout while reading a line from the log file for the stream: {stream}"
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

                position = await f.tell()
                yield line.strip(), position

    async def emit_logs(self, stream="stdout", custom_logger=print):
        async for line, _ in self.stream_logs(stream):
            custom_logger(line)

    async def cleanup(self):
        if hasattr(self, "temp_dir"):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def kill_process(self, termination_timeout=5):
        if self.process is not None:
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), termination_timeout)
                except asyncio.TimeoutError:
                    self.process.kill()
            else:
                print(
                    f"Process has already terminated with return code {self.process.returncode}."
                )
        else:
            print("No process to kill.")


async def main():
    flow_file = "../try.py"
    from metaflow.cli import start
    from metaflow.click_api import MetaflowAPI

    api = MetaflowAPI.from_cli(flow_file, start)
    command = api().run(alpha=5)
    cmd = [sys.executable, *command.split()]

    spm = SubprocessManager()

    # returns immediately
    process = await spm.run_command(cmd)
    print(process.returncode)  # this is None since not completed yet..

    # wait for process to finish..
    await spm.wait()

    # wait for process with a timeout, kill when timeout expires..
    await spm.wait(timeout=2)

    # wait for process to finish but also stream logs..
    await spm.wait(stream="stdout")

    # wait for process to finish while streaming logs but with a timeout..
    # kill when timeout expires..
    await spm.wait(timeout=2, stream="stdout")


if __name__ == "__main__":
    asyncio.run(main())
