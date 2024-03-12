import os
import sys
import shutil
import asyncio
import tempfile
import aiofiles
from typing import Dict
from metaflow import Run
from metaflow.cli import start
from metaflow.click_api import MetaflowAPI
from metaflow.subprocess_manager import SubprocessManager


async def read_from_file_when_ready(file_path):
    async with aiofiles.open(file_path, "r") as file_pointer:
        content = await file_pointer.read()
        while not content:
            await asyncio.sleep(0.1)
            content = await file_pointer.read()
        return content


class Runner(object):
    def __init__(
        self,
        flow_file: str,
        env: Dict = {},
        **kwargs,
    ):
        self.flow_file = flow_file
        self.env_vars = os.environ.copy().update(env)
        self.spm = SubprocessManager(env=self.env_vars)
        self.api = MetaflowAPI.from_cli(self.flow_file, start)
        self.runner = self.api(**kwargs).run

    def __enter__(self):
        return self

    async def tail_logs(self, stream="stdout"):
        await self.spm.get_logs(stream)

    async def run(self, blocking: bool = False, **kwargs):
        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_flow = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)
            tfp_run_id = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)

            command = self.runner(
                run_id_file=tfp_run_id.name, flow_name_file=tfp_flow.name, **kwargs
            )

            process = await self.spm.run_command([sys.executable, *command.split()])

            if blocking:
                await process.wait()

            flow_name = await read_from_file_when_ready(tfp_flow.name)
            run_id = await read_from_file_when_ready(tfp_run_id.name)

            pathspec_components = (flow_name, run_id)
            run_object = Run("/".join(pathspec_components), _namespace_check=False)

            self.run = run_object

            return run_object

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.spm.temp_dir, ignore_errors=True)
