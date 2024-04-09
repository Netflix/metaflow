import os
import sys
import time
import signal
import shutil
import hashlib
import asyncio
import tempfile
from typing import List


def hash_command_invocation(command: List[str]):
    concatenated_string = "".join(command)
    current_time = str(time.time())
    concatenated_string += current_time
    hash_object = hashlib.sha256(concatenated_string.encode())
    return hash_object.hexdigest()


class LogReadTimeoutError(Exception):
    pass


class SubprocessManager(object):
    def __init__(self):
        self.commands = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        for _, v in self.commands.items():
            await v.cleanup()

    async def run_command(self, command: List[str], env=None, cwd=None):
        command_id = hash_command_invocation(command)
        self.commands[command_id] = CommandManager(command, env, cwd)
        await self.commands[command_id].run()
        return command_id

    def get(self, command_id: str) -> "CommandManager":
        return self.commands.get(command_id, None)


class CommandManager(object):
    def __init__(self, command: List[str], env=None, cwd=None):
        self.command = command

        if env is None:
            env = os.environ.copy()
        self.env = env

        if cwd is None:
            cwd = os.getcwd()
        self.cwd = cwd

        self.process = None
        self.run_called = False
        self.log_files = {}

        signal.signal(signal.SIGINT, self.handle_sigint)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.cleanup()

    def handle_sigint(self, signum, frame):
        print("SIGINT received.")
        asyncio.create_task(self.kill())

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

    async def run(self):
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
            return self.process
        except Exception as e:
            print(f"Error starting subprocess: {e}")
            await self.cleanup()

    async def stream_logs(
        self, stream, position=None, timeout_per_line=None, log_write_delay=0.01
    ):
        if self.run_called is False:
            raise ValueError("No command run yet to get the logs for...")

        if stream not in self.log_files:
            raise ValueError(
                f"No log file found for {stream}, valid values are: {list(self.log_files.keys())}"
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

                position = f.tell()
                yield position, line.strip()

    async def emit_logs(self, stream="stdout", custom_logger=print):
        async for _, line in self.stream_logs(stream):
            custom_logger(line)

    async def cleanup(self):
        if hasattr(self, "temp_dir"):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def kill(self, termination_timeout=5):
        if self.process is not None:
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), termination_timeout)
                except asyncio.TimeoutError:
                    self.process.kill()
            else:
                print(
                    f"Process has already terminated with return code: {self.process.returncode}"
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

    async with SubprocessManager() as spm:
        # returns immediately
        command_id = await spm.run_command(cmd)
        command_obj = spm.get(command_id)

        print(command_id)

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
        async for position, line in command_obj.stream_logs(stream="stdout"):
            print(line)
            if "alpha is" in line:
                interesting_position = position
                break

        print(f"ended streaming at: {interesting_position}")

        # wait / do some other processing while the process runs in background
        # if the process finishes before this sleep period, the streaming of logs
        # below are instantaneous since it has already ended..
        # time.sleep(10)

        # this blocks till the process completes unless we uncomment the `time.sleep` above..
        print(
            f"resuming streaming from: {interesting_position} while process is still running..."
        )
        async for position, line in command_obj.stream_logs(
            stream="stdout", position=interesting_position
        ):
            print(line)

        # this will be instantaneous since the process has finished and we just read from the log file
        print("process has ended by now... streaming again from scratch..")
        async for position, line in command_obj.stream_logs(stream="stdout"):
            print(line)

        # this will be instantaneous since the process has finished and we just read from the log file
        print(
            "process has ended by now... streaming again but from position of choice.."
        )
        async for position, line in command_obj.stream_logs(
            stream="stdout", position=interesting_position
        ):
            print(line)

        # two parallel streams for stdout
        tasks = [
            command_obj.emit_logs(
                stream="stdout", custom_logger=lambda x: print(f"[STREAM A]: {x}")
            ),
            command_obj.emit_logs(
                stream="stdout", custom_logger=lambda x: print(f"[STREAM B]: {x}")
            ),
        ]
        await asyncio.gather(*tasks)

        # get the location of log files..
        print(command_obj.log_files)


if __name__ == "__main__":
    asyncio.run(main())
