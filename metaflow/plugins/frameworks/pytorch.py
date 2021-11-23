import inspect
import subprocess
import pickle
import tempfile
import os
import sys
from metaflow import current
from . import paths
from metaflow.plugins.parallel_decorator import ParallelDecorator


class PytorchParallelDecorator(ParallelDecorator):
    name = "pytorch_parallel"
    defaults = {}
    IS_PARALLEL = True

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        setup_torch_distributed(1)
        return super().task_decorate(
            step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
        )


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
            print("NOTE: logger_url not an argument to the pytorch target '{}").format(
                target.__name__
            )

        # SETUP DISTRIBUTED ENV FOR METAFLOW & TORCH
        setup_torch_distributed(num_local_workers)

        with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as args_file:
            pickle.dump(kwargs, file=args_file)

        # Run the target via "spawn" command of the flow.
        subprocess.run(
            check=True,
            args=[
                sys.executable,
                sys.argv[0],
                "spawn",
                target.__module__,
                target.__name__,
                args_file.name,
            ],
        )

        output_file = args_file.name + ".out"
        if not os.path.exists(output_file):
            return None
        with open(output_file, "rb") as output_values_f:
            output = pickle.load(output_values_f)
        return output


def setup_torch_distributed(num_local_devices):
    """
    Set up environment variables for PyTorch's distributed (DDP).
    """
    os.environ["MASTER_PORT"] = "64398"  # arbitrary
    os.environ["MASTER_ADDR"] = current.parallel.main_ip
    os.environ["NODE_RANK"] = str(current.parallel.node_index)
    os.environ["WORLD_SIZE"] = str(current.parallel.num_nodes * num_local_devices)
    os.environ["NUM_NODES"] = str(current.parallel.num_nodes)
    os.environ["PL_TORCH_DISTRIBUTED_BACKEND"] = "gloo"  # NCCL crashes on aws batch!
