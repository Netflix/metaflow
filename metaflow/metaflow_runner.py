import os
import sys
import time
import tempfile
import subprocess
from typing import Dict
from metaflow import Run
from metaflow.cli import start
from metaflow.click_api import MetaflowAPI


def cli_runner(command: str, env_vars: Dict):
    process = subprocess.Popen(
        [sys.executable, *command.split()],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env_vars,
    )
    return process


def read_from_file_when_ready(file_pointer):
    content = file_pointer.read().decode()
    while not content:
        time.sleep(0.1)
        content = file_pointer.read().decode()
    return content


class Runner(object):
    def __init__(
        self,
        flow_file: str,
        **kwargs,
    ):
        self.flow_file = flow_file
        self.api = MetaflowAPI.from_cli(self.flow_file, start)
        self.runner = self.api(**kwargs).run

    def __enter__(self):
        return self

    def run(self, blocking: bool = False, **kwargs):
        env_vars = os.environ.copy()

        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_flow_name = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)
            tfp_run_id = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)

            command = self.runner(
                run_id_file=tfp_run_id.name, flow_name_file=tfp_flow_name.name, **kwargs
            )

            process = cli_runner(command, env_vars)
            if blocking:
                process.wait()

            flow_name = read_from_file_when_ready(tfp_flow_name)
            run_id = read_from_file_when_ready(tfp_run_id)

            pathspec_components = (flow_name, run_id)
            run_object = Run("/".join(pathspec_components), _namespace_check=False)

            return run_object

    def __exit__(self, exc_type, exc_value, traceback):
        pass
