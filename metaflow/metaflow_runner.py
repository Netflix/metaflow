import os
import sys
import time
import asyncio
import tempfile
from typing import Dict, Optional
from metaflow import Run
from metaflow.subprocess_manager import SubprocessManager, CommandManager


def read_from_file_when_ready(file_path):
    with open(file_path, "r") as file_pointer:
        content = file_pointer.read()
        while not content:
            time.sleep(0.1)
            content = file_pointer.read()
        return content


class ExecutingRun(object):
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
        await self.command_obj.wait(timeout, stream)
        return self

    @property
    def stdout(self):
        with open(self.command_obj.log_files.get("stdout"), "r") as fp:
            return fp.read()

    @property
    def stderr(self):
        with open(self.command_obj.log_files.get("stderr"), "r") as fp:
            return fp.read()

    async def stream_logs(self, stream: str, position: Optional[int] = None):
        async for position, line in self.command_obj.stream_logs(stream, position):
            yield position, line


class Runner(object):
    def __init__(
        self,
        flow_file: str,
        profile: Optional[str] = None,
        env: Dict = {},
        **kwargs,
    ):
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
            tfp_flow = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)
            tfp_run_id = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)

            command = self.runner(
                run_id_file=tfp_run_id.name, flow_name_file=tfp_flow.name, **kwargs
            )

            pid = await self.spm.run_command(
                [sys.executable, *command], env=self.env_vars
            )
            command_obj = self.spm.get(pid)

            # detect failures even before writing to the run_id and flow_name files
            # the error (if any) must happen within the first 0.5 seconds
            try:
                await asyncio.wait_for(command_obj.process.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

            # if the returncode is None, the process has encountered no error within the
            # initial 0.5 seconds and we proceed to run it in the background
            # during which it would have written to the run_id and flow_name files
            if command_obj.process.returncode is not None:
                stderr_log = open(command_obj.log_files["stderr"]).read()
                command = " ".join(command_obj.command)
                raise RuntimeError(f"Error executing: '{command}':\n\n{stderr_log}")
            else:
                flow_name = read_from_file_when_ready(tfp_flow.name)
                run_id = read_from_file_when_ready(tfp_run_id.name)

                pathspec_components = (flow_name, run_id)
                run_object = Run("/".join(pathspec_components), _namespace_check=False)

                return ExecutingRun(self, command_obj, run_object)

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()
