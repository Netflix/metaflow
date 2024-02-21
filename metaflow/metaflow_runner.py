import os
import sys
import time
import asyncio
import tempfile
import subprocess
from metaflow import Run
from typing import List, Dict
from concurrent.futures import ProcessPoolExecutor


def cli_runner(flow_file: str, command: str, args: List[str], env_vars: Dict):
    process = subprocess.Popen(
        [sys.executable, flow_file, command, *args],
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


class Pool(object):
    def __init__(
        self,
        flow_file: str,
        num_processes: int,
    ):
        self.flow_file = flow_file
        self.num_processes = num_processes
        self.runner = Runner(self.flow_file)

    def __enter__(self):
        return self

    def map(self, paramater_space: List[Dict], blocking: bool = False):
        return asyncio.run(self._map(paramater_space, blocking))

    async def _map(self, paramater_space: List[Dict], blocking: bool = False):
        loop = asyncio.get_running_loop()
        tasks, runs = [], []
        with ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            for set_of_params in paramater_space:
                tasks.append(
                    loop.run_in_executor(
                        executor, self.runner.run, set_of_params, blocking
                    )
                )
            for done in asyncio.as_completed(tasks):
                runs.append(await done)

        return runs

    def __exit__(self, exc_type, exc_value, traceback):
        pass


# consider more args in constructor,
# list of them is available using:
# `python ../try.py --help`
class Runner(object):
    def __init__(
        self,
        flow_file: str,
    ):
        self.flow_file = flow_file

    def __enter__(self):
        return self

    # consider more args for run method,
    # list of them is available using:
    # `python ../try.py run --help`
    def run(
        self,
        params: Dict,  # eventually, parse the file for parameters? contains stuff like {'alpha': 30}
        blocking: bool = False,
    ):
        env_vars = os.environ.copy()

        params_as_cli_args = []
        for (k, v) in params.items():
            params_as_cli_args.extend(["--" + str(k), str(v)])

        with tempfile.TemporaryDirectory() as temp_dir:
            tfp_flow_name = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)
            tfp_run_id = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)

            params_as_cli_args.extend(["--run-id-file", tfp_run_id.name])
            params_as_cli_args.extend(["--flow-name-file", tfp_flow_name.name])

            process = cli_runner(
                self.flow_file,
                command="run",
                args=params_as_cli_args,
                env_vars=env_vars,
            )
            if blocking:
                process.wait()

            flow_name = read_from_file_when_ready(tfp_flow_name)
            run_id = read_from_file_when_ready(tfp_run_id)

            pathspec_components = (flow_name, run_id)
            run_object = Run("/".join(pathspec_components), _namespace_check=False)

            return run_object

    def __exit__(self, exc_type, exc_value, traceback):
        pass
