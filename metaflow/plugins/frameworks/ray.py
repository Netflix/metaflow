import inspect
import subprocess
import pickle
import tempfile
import os
import sys
import time
from functools import partial
from metaflow import current
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.plugins.parallel_decorator import ParallelDecorator


class RayParallelDecorator(ParallelDecorator):

    name = "ray_parallel"
    defaults = {"main_port": None}
    IS_PARALLEL = True

    def _mapper_heartbeat(self, graph_info):
        print('HEARTBEAT')
        from metaflow import Task # avoid circular import
        from metaflow.plugins.parallel_decorator import identify_control_task_pathspec
        control_task_pathspec = identify_control_task_pathspec(graph_info, current)
        while not Task(control_task_pathspec).finished:
            time.sleep(10)

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        if ubf_context == UBF_CONTROL:
            # print('CONTROL')
            return super().task_decorate(
                step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
            )
        else:
            print('MAPPER')
            # doesn't do @pip
            return partial(self._mapper_heartbeat, graph_info=flow._graph_info)

    def setup_distributed_env(self, flow):
        ray_cli_path = sys.executable.replace('python', 'ray')
        print("RAY PATH: ", ray_cli_path)
        setup_ray_distributed(self.attributes["main_port"], ray_cli_path)


def setup_ray_distributed(main_port=None, ray_cli_path=None):

    try:
        main_port = main_port or (6379 + abs(int(current.run_id)) % 1000)
    except:
        # if `int()` fails, i.e. `run_id` is not an `int`, use just a constant port. Can't use `hash()`,
        # as that is not constant.
        main_port = 6379

    main_ip = current.parallel.main_ip
    print('MAIN IP', main_ip)

    if current.parallel.node_index == 0:
        print(f"main: The main Node IP address is: {main_ip}")
        subprocess.run([ray_cli_path, "start", "--head", "--node-ip-address", main_ip, "--port", main_port], check=True)
    else:
        import ray # put here, so ray import is only imported after environment exists in the task container
        node_ip_address = ray._private.services.get_node_ip_address()
        print(f"MAPPER: The main Node IP address is: {main_ip}")
        print(f"MAPPER: The Node IP address is: {node_ip_address}")
        subprocess.run([ray_cli_path, "start", "--node-ip-address", node_ip_address, "--address", f"{main_ip}:{main_port}"], check=True)