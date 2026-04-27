import json
import os
import subprocess
import sys
import tempfile
from typing import List, Tuple


class _WithDir:
    def __init__(self, new_dir: str) -> None:
        self._old = os.getcwd()
        self._new = new_dir

    def __enter__(self) -> str:
        os.chdir(self._new)
        return self._new

    def __exit__(self, *_) -> None:
        os.chdir(self._old)


def run_core_test_combination(
    context: str, graph: str, tests: List[str], masked_cpu_count: int
) -> None:
    num_parallel = min(masked_cpu_count, len(tests))

    core_dir = os.path.dirname(__file__)

    env = os.environ.copy()
    env["METAFLOW_CLICK_API_PROCESS_CONFIG"] = "0"
    env["METAFLOW_TEST_PRINT_FLOW"] = "1"
    env["PYTHONPATH"] = (
        "%s:%s" % (core_dir, env["PYTHONPATH"]) if "PYTHONPATH" in env else core_dir
    )
    # Ensure METAFLOW_USER is set before run_tests.py imports metaflow so that
    # metaflow_config.USER is cached as a non-root value at module load time.
    # This is required for the API executor when the host user is root.
    env.setdefault("METAFLOW_USER", "tester")

    with _WithDir(core_dir):
        fd, failure_file = tempfile.mkstemp(dir=".")
        os.close(fd)
        try:
            cmd = [
                sys.executable,
                "run_tests.py",
                "--num-parallel",
                str(num_parallel),
                "--failed-dump",
                failure_file,
                "--contexts",
                context,
                "--tests",
                ",".join(tests),
                "--graphs",
                graph,
            ]
            result = subprocess.run(cmd, env=env)
            if result.returncode != 0:
                try:
                    with open(failure_file, "rt") as f:
                        failures = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    failures = {
                        "unknown": "run_tests.py exited with code %d"
                        % result.returncode
                    }
                import pytest

                pytest.fail(
                    "Core tests failed in CoreTest(%s, %s, %s): %s"
                    % (context, graph, tests, failures)
                )
        finally:
            if os.path.exists(failure_file):
                os.remove(failure_file)


def test_core_combination(
    core_test_params: Tuple[str, str, List[str]], masked_cpu_count: int
) -> None:
    context, graph, tests = core_test_params
    run_core_test_combination(context, graph, tests, masked_cpu_count)
