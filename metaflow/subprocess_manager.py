import os
import sys
import shutil
import asyncio
import tempfile
import aiofiles
from typing import List
from asyncio.queues import Queue


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

    async def get_logs(self, stream="stdout"):
        if self.run_command_called is False:
            raise ValueError("No command run yet to get the logs for...")
        if stream == "stdout":
            stdout_task = asyncio.create_task(self.consume_queue(self.stdout_queue))
            await stdout_task
        elif stream == "stderr":
            stderr_task = asyncio.create_task(self.consume_queue(self.stderr_queue))
            await stderr_task
        else:
            raise ValueError(
                f"Invalid value for `stream`: {stream}, valid values are: {['stdout', 'stderr']}"
            )

    async def stream_logs_to_queue(self, logfile, queue, process):
        async with aiofiles.open(logfile, "r") as f:
            while True:
                if process.returncode is None:
                    # process is still running
                    line = await f.readline()
                    if not line:
                        continue
                    await queue.put(line.strip())
                elif process.returncode == 0:
                    # insert an indicator that no more items
                    # will be inserted into the queue
                    await queue.put(None)
                    break
                elif process.returncode != 0:
                    # insert an indicator that no more items
                    # will be inserted into the queue
                    await queue.put(None)
                    raise Exception("Ran into an issue...")

    async def consume_queue(self, queue: Queue):
        while True:
            item = await queue.get()
            # break out of loop when we get the `indicator`
            if item is None:
                break
            print(item)
            queue.task_done()

    async def run_command(self, command: List[str]):
        self.temp_dir = tempfile.mkdtemp()
        stdout_logfile = os.path.join(self.temp_dir, "stdout.log")
        stderr_logfile = os.path.join(self.temp_dir, "stderr.log")

        self.stdout_queue = Queue()
        self.stderr_queue = Queue()

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

            self.stdout_task = asyncio.create_task(
                self.stream_logs_to_queue(
                    stdout_logfile, self.stdout_queue, self.process
                )
            )
            self.stderr_task = asyncio.create_task(
                self.stream_logs_to_queue(
                    stderr_logfile, self.stderr_queue, self.process
                )
            )

            self.run_command_called = True
            return self.process
        except Exception as e:
            print(f"Error starting subprocess: {e}")
            # Clean up temp files if process fails to start
            shutil.rmtree(self.temp_dir, ignore_errors=True)


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
    # print(process.returncode)
    print("will print logs after 15 secs, flow has ended by then...")
    await asyncio.sleep(15)
    print("done waiting...")
    await spm.get_logs(stream="stdout")


if __name__ == "__main__":
    asyncio.run(main())
