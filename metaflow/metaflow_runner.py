import os
import sys
import time
import tempfile
from typing import Dict
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
    def __init__(self, command_obj: CommandManager, run_obj: Run) -> None:
        self.command_obj = command_obj
        self.run_obj = run_obj

    def __getattr__(self, name: str):
        if hasattr(self.run_obj, name):
            run_attr = getattr(self.run_obj, name)
            if callable(run_attr):
                return lambda *args, **kwargs: run_attr(*args, **kwargs)
            else:
                return run_attr
        elif hasattr(self.command_obj, name):
            command_attr = getattr(self.command_obj, name)
            if callable(command_attr):
                return lambda *args, **kwargs: command_attr(*args, **kwargs)
            else:
                return command_attr
        else:
            raise AttributeError("Invalid attribute %s" % name)


class Runner(object):
    def __init__(
        self,
        flow_file: str,
        env: Dict = {},
        **kwargs,
    ):
        self.flow_file = flow_file
        self.env_vars = os.environ.copy().update(env)
        self.spm = SubprocessManager()
        self.api = MetaflowAPI.from_cli(self.flow_file, start)
        self.runner = self.api(**kwargs).run

    async def __aenter__(self):
        return self

    async def run(self, **kwargs):
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

            return ExecutingRun(command_obj, run_object)

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.spm.cleanup()
