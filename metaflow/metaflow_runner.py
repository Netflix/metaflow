import asyncio
from concurrent.futures import ProcessPoolExecutor
import subprocess, sys, os
from typing import List, Dict, Optional


def convert_params_to_cli_args(params: List[Dict]):
    converted_params = [item for pair in params.items() for item in pair]
    converted_params = [
        str(val) if idx % 2 else f"--{val}" for idx, val in enumerate(converted_params)
    ]
    return converted_params


def cli_runner(flow_file: str, command: str, args: List[str], env_vars: Dict):
    result = subprocess.run([sys.executable, flow_file, command, *args], env=env_vars)
    return result.returncode


class Pool(object):
    def __init__(
        self,
        flow_file: str,
        num_processes: int,
        profile: Optional[str] = None,
        with_context: Optional[str] = None,
    ):
        self.flow_file = flow_file
        self.num_processes = num_processes
        self.profile = profile
        self.with_context = with_context

    def map(self, params: List[Dict], command: str = "run"):
        return asyncio.run(self._map(params, command))

    async def _map(self, params: List[Dict], command: str = "run"):
        self.command = command
        loop = asyncio.get_running_loop()
        tasks, runs = [], []
        with ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            for each_param in params:
                tasks.append(loop.run_in_executor(executor, self._execute, each_param))
            for done in asyncio.as_completed(tasks):
                runs.append(await done)

        return runs

    def _execute(self, params: List[Dict]):
        env_vars = os.environ.copy()
        if self.profile:
            env_vars.update({"METAFLOW_PROFILE": self.profile})
        params = convert_params_to_cli_args(params)
        print(f"Starting for {params}")
        result = cli_runner(self.flow_file, self.command, params, env_vars)
        print(f"Result ready for {params}")
        return {" ".join(params): result}

    def __enter__(self):
        print("Start....")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("End...")
