from metaflow import current
from metaflow.plugins.parallel_decorator import ParallelDecorator
from metaflow.unbounded_foreach import UBF_CONTROL

import time

_DASK_SCHEDULER_PORT = 8786


class DaskDistributed(ParallelDecorator):
    name = "dask_distributed"
    defaults = {}
    IS_PARALLEL = True

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        if ubf_context == UBF_CONTROL:
            from dask.distributed import Scheduler, Client

            def start_scheduler_run_step_and_stop_scheduler():
                import subprocess

                scheduler_process = subprocess.Popen("dask-scheduler")
                worker_process = subprocess.Popen(
                    [
                        "dask-worker",
                        "{}:{}".format(current.parallel.main_ip, _DASK_SCHEDULER_PORT),
                    ]
                )

                with Client(
                    "{}:{}".format(current.parallel.main_ip, _DASK_SCHEDULER_PORT)
                ) as client:
                    num_workers = len(client.scheduler_info()["workers"])
                    while num_workers < current.parallel.num_nodes:
                        if scheduler_process.poll() is not None:
                            print("Scheduler process failed! Bail out.")
                            raise AssertionError(
                                "Dask scheduler process failed. Return code={}".format(
                                    scheduler_process.returncode
                                )
                            )
                        if worker_process.poll() is not None:
                            print("Worker process  failed! Bail out.")
                            raise AssertionError(
                                "Dask worker process failed, return code={}".format(
                                    worker_process.returncode
                                )
                            )
                        print(
                            "Waiting until all workers ready. Currently {}/{} workers registered".format(
                                num_workers, current.parallel.num_nodes
                            )
                        )
                        time.sleep(1.0)
                        num_workers = len(client.scheduler_info()["workers"])
                    print("Starting step function")
                    step_func()
                    print("Finished step function, stop scheduler.")
                    client.shutdown()

                scheduler_process.wait(timeout=30)
                worker_process.wait(timeout=30)

            return super().task_decorate(
                start_scheduler_run_step_and_stop_scheduler,
                flow,
                graph,
                retry_count,
                max_user_code_retries,
                ubf_context,
            )
        return dask_worker_node

    def setup_distributed_env(self, flow):
        pass


def dask_worker_node():
    import subprocess

    print("Start dask worker")
    worker_process = subprocess.Popen(
        ["dask-worker", "{}:{}".format(current.parallel.main_ip, _DASK_SCHEDULER_PORT)]
    )
    if worker_process.wait():
        raise AssertionError(
            "Worker process failed! Return code={}".format(worker_process.returncode)
        )
