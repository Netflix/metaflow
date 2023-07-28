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
    subprocess.Popen(
        [sys.executable, "-m", "pip", "install", "-U", "ray[air]"]
    ).wait()

    try:
        master_port = master_port or (6379 + abs(int(current.run_id)) % 1000)
    except:
        # if `int()` fails, i.e. `run_id` is not an `int`, use just a constant port. Can't use `hash()`,
        # as that is not constant.
        master_port = 6379

    if current.parallel.node_index == 0:
        subprocess.Popen(f"ray start --head --node-ip-address {current.parallel.main_ip} --port {master_port} --block", shell=True).wait()
    else:
        import ray
        node_ip_address = ray._private.services.get_node_ip_address()
        subprocess.Popen(f"ray start --node-ip-address {node_ip_address} --address {current.parallel.main_ip}:{master_port} --block", shell=True).wait()

