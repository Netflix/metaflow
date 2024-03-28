import os
import sys
import time
import signal
import shutil
import asyncio
import tempfile
from typing import List


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
        self.process_dict = {}

        signal.signal(signal.SIGINT, self.handle_sigint)

    def handle_sigint(self, signum, frame):
        print("SIGINT received.")
        asyncio.create_task(self.kill_process())

    async def wait(self, timeout=None, stream=None):
        if timeout is None:
            if stream is None:
                await self.process.wait()
            else:
                await self.get_logs(stream)
        else:
            tasks = [asyncio.create_task(asyncio.sleep(timeout))]
            if stream is None:
                tasks.append(asyncio.create_task(self.process.wait()))
            else:
                tasks.append(asyncio.create_task(self.get_logs(stream)))

            await asyncio.wait(tasks, return_when="FIRST_COMPLETED")

        await self.cleanup()

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
                stdout=open(stdout_logfile, "w"),
                stderr=open(stderr_logfile, "w"),
            )

            self.log_files["stdout"] = stdout_logfile
            self.log_files["stderr"] = stderr_logfile

            self.run_command_called = True
            return self.process
        except Exception as e:
            print(f"Error starting subprocess: {e}")
            await self.cleanup()

    async def stream_logs(self, stream):
        if self.run_command_called is False:
            raise ValueError("No command run yet to get the logs for...")

        if stream not in self.log_files:
            raise ValueError(f"No log file found for {stream}")

        log_file = self.log_files[stream]

        with open(log_file, mode="r") as f:
            last_position = self.process_dict.get(stream, 0)
            f.seek(last_position)

            while True:
                line = f.readline()
                if not line:
                    break
                print(line.strip())

            self.process_dict[stream] = f.tell()

    async def get_logs(self, stream="stdout", delay=0.1):
        while self.process.returncode is None:
            await self.stream_logs(stream)
            await asyncio.sleep(delay)

    async def cleanup(self):
        if hasattr(self, "temp_dir"):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def kill_process(self, timeout=5):
        if self.process is not None:
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), timeout)
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
