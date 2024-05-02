import os
import sys
import time
import asyncio
import tempfile
from typing import Dict, Optional
from metaflow import Run
from metaflow.cli import start
from metaflow.click_api import MetaflowAPI
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
        profile: str,
        env: Dict = {},
        **kwargs,
    ):
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

            flow_name = read_from_file_when_ready(tfp_flow.name)
            run_id = read_from_file_when_ready(tfp_run_id.name)

            pathspec_components = (flow_name, run_id)
            run_object = Run("/".join(pathspec_components), _namespace_check=False)

            return ExecutingRun(self, command_obj, run_object)

    def __exit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.spm.cleanup()
