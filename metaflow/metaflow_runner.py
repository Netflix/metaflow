import os
import sys
import time
import asyncio
import tempfile
from typing import Dict, Optional
from metaflow import Run
from metaflow.subprocess_manager import SubprocessManager, CommandManager


def read_from_file_when_ready(file_path: str, timeout: float = 5):
    start_time = time.time()
    with open(file_path, "r") as file_pointer:
        content = file_pointer.read()
        while not content:
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    "Timeout while waiting for file content from '%s'" % file_path
                )
            time.sleep(0.1)
            content = file_pointer.read()
        return content


class ExecutingRun(object):
    """
    An object that encapsulates both the:
        - CommandManager Object (that has ability to stream logs, kill the underlying process, etc.)
        - Metaflow's Run object

    This is a user facing object exposing methods and properties for:
        - waiting (with an optional timeout and optional streaming of logs)
        - current snapshot of stdout
        - current snapshot of stderr
        - ability to stream logs
    """

    def __init__(
        self, runner: "Runner", command_obj: CommandManager, run_obj: Run
    ) -> None:
        self.runner = runner
        self.command_obj = command_obj
        self.run = run_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.runner.__exit__(exc_type, exc_value, traceback)

    async def wait(self, timeout: Optional[float] = None, stream: Optional[str] = None):
        """Wait for the run to finish, optionally with a timeout and optionally streaming its output."""
        await self.command_obj.wait(timeout, stream)
        return self

    @property
    def stdout(self):
        """Get the current snapshot of stdout from the log file."""
        with open(self.command_obj.log_files.get("stdout"), "r") as fp:
            return fp.read()

    @property
    def stderr(self):
        """Get the current snapshot of stderr from the log file."""
        with open(self.command_obj.log_files.get("stderr"), "r") as fp:
            return fp.read()

    async def stream_log(self, stream: str, position: Optional[int] = None):
        """Stream logs from the run using the log files. Used with an async for loop"""
        async for position, line in self.command_obj.stream_log(stream, position):
            yield position, line


class Runner(object):
    def __init__(
        self, flow_file: str, profile: Optional[str] = None, env: Dict = {}, **kwargs
    ):
        """
        Metaflow's Runner API that presents a programmatic interface
        to run flows either synchronously or asynchronously. The class expects
        a path to the flow file along with optional top level parameters such as:
        metadata, environment, datastore, etc.

        The run() method expects run-level parameters. The object returned by the
        'run()' method has access to Metaflow's Run object using `.run` along with
        other abilities such as streaming logs, etc.

        Example:
            with metaflow_runner.Runner('../try.py', metadata="local") as runner:
                result = runner.run(alpha=5, tags=["abc", "def"], max_workers=5)
                print(result.run.finished)
        """

        # these imports are required here and not at the top
        # since they interfere with the user defined Parameters
        # in the flow file, this is related to the ability of
        # importing 'Runner' directly i.e.
        #    from metaflow import Runner
        # This ability is made possible by the statement:
        # 'from .metaflow_runner import Runner' in '__init__.py'
        from metaflow.cli import start
        from metaflow.click_api import MetaflowAPI

        self.flow_file = flow_file
        self.env_vars = os.environ.copy().update(env)
        if profile:
            self.env_vars["METAFLOW_PROFILE"] = profile
        self.spm = SubprocessManager()
        self.api = MetaflowAPI.from_cli(self.flow_file, start)
        self.runner = self.api(**kwargs).run

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self

    def run(self, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = loop.run_until_complete(self.async_run(**kwargs))
            result = loop.run_until_complete(result.wait())
            return result
        finally:
            loop.close()

    async def async_run(self, **kwargs):
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_pathspec = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)

            command = self.runner(pathspec_file=tfp_pathspec.name, **kwargs)

            pid = await self.spm.run_command(
                [sys.executable, *command], env=self.env_vars
            )
            command_obj = self.spm.get(pid)

            try:
                pathspec = read_from_file_when_ready(tfp_pathspec.name, timeout=5)
                run_object = Run(pathspec, _namespace_check=False)
                return ExecutingRun(self, command_obj, run_object)
            except TimeoutError as e:
                stdout_log = open(command_obj.log_files["stdout"]).read()
                stderr_log = open(command_obj.log_files["stderr"]).read()
                command = " ".join(command_obj.command)
                error_message = "Error executing: '%s':\n" % command
                if stdout_log.strip():
                    error_message += "\nStdout:\n%s\n" % stdout_log
                if stderr_log.strip():
                    error_message += "\nStderr:\n%s\n" % stderr_log
                raise RuntimeError(error_message) from e

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()
