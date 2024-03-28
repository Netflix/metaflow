import os
import sys
import time
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
                print("Process has already terminated.")
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
    process = await spm.run_command(cmd)
    # await process.wait()
    print(process.returncode)
    print(process)

    # print("kill after 2 seconds...get logs upto the point of killing...")
    # await asyncio.wait([
    #     asyncio.create_task(spm.get_logs(stream="stdout")),
    #     asyncio.create_task(asyncio.sleep(2)),
    # ], return_when="FIRST_COMPLETED")
    # await spm.kill_process()
    # print("done...")

    # print("will print logs after 15 secs, flow has ended by then...")
    # time.sleep(15)
    # print("done waiting...")
    # await spm.get_logs(stream="stdout")
    # await spm.cleanup()

    # await spm.get_logs(stream="stdout")
    # await spm.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
