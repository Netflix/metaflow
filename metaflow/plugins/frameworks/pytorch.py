import inspect
import subprocess
import pickle
import tempfile
import os
import sys
from metaflow import current
from metaflow.plugins.parallel_decorator import ParallelDecorator


class PytorchParallelDecorator(ParallelDecorator):
    name = "pytorch_parallel"
    defaults = {"master_port": None}
    IS_PARALLEL = True

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return super().task_decorate(
            step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
        )

    def setup_distributed_env(self, flow):
        setup_torch_distributed(self.attributes["master_port"])


def setup_torch_distributed(master_port=None):
    """
    Set up environment variables for PyTorch's distributed (DDP).
    """
    # Choose port depending on run id to reduce probability of collisions, unless
    # provided by the user.
    try:
        master_port = master_port or (51000 + abs(int(current.run_id)) % 10000)
    except:
        # if `int()` fails, i.e. `run_id` is not an `int`, use just a constant port. Can't use `hash()`,
        # as that is not constant.
        master_port = 51001
    os.environ["MASTER_PORT"] = str(master_port)
    os.environ["MASTER_ADDR"] = current.parallel.main_ip
    os.environ["NODE_RANK"] = str(current.parallel.node_index)
    os.environ["WORLD_SIZE"] = str(current.parallel.num_nodes)
    os.environ["NUM_NODES"] = str(current.parallel.num_nodes)
    # Specific for PyTorch Lightning
    os.environ["PL_TORCH_DISTRIBUTED_BACKEND"] = "gloo"  # NCCL crashes on aws batch!
