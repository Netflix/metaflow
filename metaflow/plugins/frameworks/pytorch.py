import inspect
import subprocess
import pickle
import tempfile
import os
import sys
from metaflow import current
from . import paths
import importlib
import os
from metaflow.plugins.parallel_decorator import ParallelDecorator
from metaflow.plugins.frameworks import spawn_subprocess


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


class PyTorchHelper:
    @staticmethod
    def run_trainer(run, target, num_local_workers=None, **kwargs):
        import torch

        num_local_workers = num_local_workers or max(torch.cuda.device_count(), 1)
        # Inject checkpoint args
        sig = inspect.signature(target)
        if "checkpoint_url" in sig.parameters:
            kwargs["checkpoint_url"] = paths.get_s3_checkpoint_url(run)
        else:
            print(
                "NOTE: checkpoint_url not an argument to the pytorch target '{}'".format(
                    target.__name__
                )
            )
        if "latest_checkpoint_url" in sig.parameters:
            assert (
                "checkpoint_url" in sig.parameters
            ), "With latest_checkpoint_url argument, need also add checkpoint_url argument."
            kwargs["latest_checkpoint_url"] = paths.get_s3_latest_checkpoint_url(run)
        else:
            print(
                "NOTE: latest_checkpoint_url not an argument to the pytorch target '{}'".format(
                    target.__name__
                )
            )
        if "logger_url" in sig.parameters:
            kwargs["logger_url"] = paths.get_s3_logger_url(run=run)
        else:
            print(
                "NOTE: logger_url not an argument to the pytorch target '{}".format(
                    target.__name__
                )
            )

        # If run with a pytorch parallel decorator, the parallel environment is not
        # set, so we'll do it here.
        if "WORLD_SIZE" not in os.environ:
            setup_torch_distributed()
        # Update the WORLD_SIZE environment to include the local workers
        os.environ["WORLD_SIZE"] = str(current.parallel.num_nodes * num_local_workers)
        with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as args_file:
            pickle.dump(kwargs, file=args_file)

        # Run the target via a script that spawn function in a separate subprocess.
        module_path = importlib.import_module(target.__module__).__file__

        subprocess.run(
            check=True,
            args=[
                sys.executable,
                spawn_subprocess.__file__,
                os.path.dirname(module_path),
                target.__module__,
                target.__name__,
                args_file.name,
            ],
        )

        output_file = args_file.name + ".out"
        if not os.path.exists(output_file):
            print("No output file")
            return None
        with open(output_file, "rb") as output_values_f:
            output = pickle.load(output_values_f)
        return output


def setup_torch_distributed(master_port=None):
    """
    Set up environment variables for PyTorch's distributed (DDP).
    """
    # Choose port depending on run id to reduce probability of collisions, unless
    # provided by the user.
    try:
        master_port = master_port or (51000 + abs(int(current.run_id)) % 10000)
    except:
        # if int() fails, i.e run_id is not an int use just a constant port. Can't use hash()
        # as that is not constant.
        master_port = 51001
    os.environ["MASTER_PORT"] = str(master_port)
    os.environ["MASTER_ADDR"] = current.parallel.main_ip
    os.environ["NODE_RANK"] = str(current.parallel.node_index)
    os.environ["WORLD_SIZE"] = str(current.parallel.num_nodes)
    os.environ["NUM_NODES"] = str(current.parallel.num_nodes)
    # Specific for PyTorch Lightning
    os.environ["PL_TORCH_DISTRIBUTED_BACKEND"] = "gloo"  # NCCL crashes on aws batch!
