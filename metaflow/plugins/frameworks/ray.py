import inspect
import subprocess
import pickle
import tempfile
import os
import sys
from metaflow import current
from metaflow.plugins.parallel_decorator import ParallelDecorator


class RayParallelDecorator(ParallelDecorator):
    name = "ray_parallel"
    defaults = {"master_port": None}
    IS_PARALLEL = True

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return super().task_decorate(
            step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
        )

    def setup_distributed_env(self, flow):
        setup_ray_distributed(self.attributes["master_port"])


def setup_ray_distributed(master_port=None):
    """
    Manually set up Ray cluster
    """
    # Choose port depending on run id to reduce probability of collisions, unless
    # provided by the user.
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "ray[default]"]
    )

    try:
        master_port = master_port or (9001 + abs(int(current.run_id)) % 1000)
    except:
        # if `int()` fails, i.e. `run_id` is not an `int`, use just a constant port. Can't use `hash()`,
        # as that is not constant.
        master_port = 9001

    if current.parallel.node_index == 0:
        subprocess.run(
            [sys.executable, "-m", "ray", "start", "--head", f"--port={master_port}"]
        )
    else:
        address = f"{current.parallel.main_ip}:{master_port}"
        subprocess.run([sys.executable, "-m", "ray", "start", "--address", address])
