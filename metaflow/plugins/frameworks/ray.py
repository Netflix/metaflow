import subprocess
import os
import sys
import time
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.plugins.parallel_decorator import ParallelDecorator, _local_multinode_control_task_step_func


class RayParallelDecorator(ParallelDecorator):
    name = "ray_parallel"
    defaults = {"main_port": None}
    IS_PARALLEL = True

    def task_decorate(
            self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):

        from functools import partial

        def _worker_heartbeat(graph_info=flow._graph_info):
            from metaflow import Task, current
            control = get_previous_task_pathspec(graph_info, current)
            while not Task(control).finished:
                time.sleep(3)

        def _empty_worker_task():
            pass

        if os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local":
            if ubf_context == UBF_CONTROL:
                env_to_use = getattr(self.environment, "base_env", self.environment)
                return partial(
                    _local_multinode_control_task_step_func,
                    # assigns the flow._control_mapper_tasks variables & runs worker subprocesses.
                    flow,
                    env_to_use,
                    step_func,
                    # run user code and let ray.init() auto-detect available resources. could swap this for an entrypoint.py file to match ray job submission API.
                    retry_count,
                )
            return partial(
                _empty_worker_task)  # don't need to run code on worker task. ray.init() in control attaches driver to the cluster.
        else:
            self.setup_distributed_env(flow, ubf_context)
            if ubf_context == UBF_CONTROL:
                return step_func
            return partial(
                _worker_heartbeat)  # don't need to run user code on worker task. ray.init() in control attaches driver to the cluster.

    def setup_distributed_env(self, flow, ubf_context):
        self.ensure_ray_air_installed()
        ray_cli_path = sys.executable.replace("python", "ray")
        setup_ray_distributed(self.attributes["main_port"], ray_cli_path, flow, ubf_context)

    def ensure_ray_air_installed(self):
        try:
            import ray
        except ImportError:
            print("Ray is not installed. Installing latest version of ray-air package.")
            subprocess.run([sys.executable, "-m", "pip", "install", "-U", "ray[air]"])

def setup_ray_distributed(
        main_port=None,
        ray_cli_path=None,
        run=None,
        ubf_context=None
):
    import ray
    import json
    import socket
    from metaflow import S3, current

    # Why are deco.task_pre_step and deco.task_decorate calls in the same loop?
    # https://github.com/Netflix/metaflow/blob/76eee802cba1983dffe7e7731dd8e31e2992e59b/metaflow/task.py#L553
    # this causes these current.parallel variables to be defaults on all nodes,
    # since AWS Batch decorator's task_pre_step hasn't run yet.
    # num_nodes = current.parallel.num_nodes
    # node_index = current.parallel.node_index

    # AWS Batch-specific workaround.
    num_nodes = int(os.environ["AWS_BATCH_JOB_NUM_NODES"])
    node_index = os.environ["AWS_BATCH_JOB_NODE_INDEX"]
    node_key = os.path.join("ray_nodes", "node_%s.json" % node_index)

    # Similar to above comment,
    # better to use current.parallel.main_ip instead of this conditional block,
    # but this seems to require a change to the main loop in metaflow.task.
    if ubf_context == UBF_CONTROL:
        local_ips = socket.gethostbyname_ex(socket.gethostname())[-1]
        main_ip = local_ips[0]
    else:
        main_ip = os.environ['AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS']

    try:
        main_port = main_port or (6379 + abs(int(current.run_id)) % 1000)
    except:
        # if `int()` fails, i.e. `run_id` is not an `int`, use just a constant port. Can't use `hash()`,
        # as that is not constant.
        main_port = 6379

    s3 = S3(run=run)

    if ubf_context == UBF_CONTROL:
        runtime_start_result = subprocess.run(
            [
                ray_cli_path,
                "start",
                "--head",
                "--node-ip-address",
                main_ip,
                "--port",
                str(main_port),
            ]
        )
    else:
        node_ip_address = ray._private.services.get_node_ip_address()
        runtime_start_result = subprocess.run(
            [
                ray_cli_path,
                "start",
                "--node-ip-address",
                node_ip_address,
                "--address",
                "%s:%s" % (main_ip, main_port),
            ]
        )

    if runtime_start_result.returncode != 0:
        raise Exception("Ray runtime failed to start on node %s" % node_index)
    else:
        s3.put(node_key, json.dumps({'node_started': True}))

    def _num_nodes_started(path="ray_nodes"):
        objs = s3.get_recursive([path])
        num_started = 0
        for obj in objs:
            obj = json.loads(obj.text)
            if obj['node_started']:
                num_started += 1
            else:
                raise Exception("Node {} failed to start Ray runtime".format(node_index))
        return num_started

    # poll until all workers have joined the cluster
    if ubf_context == UBF_CONTROL:
        while _num_nodes_started() < num_nodes:
            time.sleep(10)

    s3.close()


def get_previous_task_pathspec(graph_info, current):
    """
    Find the pathspec of the control task that a worker task is coupled to.
    """

    from metaflow import Step

    steps_info = graph_info['steps']
    for step_name, step_info in steps_info.items():
        if current.step_name == step_name:
            previous_step_name = step_name
            step_pathspec = "{flow_name}/{run_id}/{step_name}".format(
                flow_name=current.flow_name,
                run_id=current.run_id,
                step_name=previous_step_name
            )
            step = Step(step_pathspec)
            for task in step:
                if task.id.startswith("control"):
                    control_task_pathspec = "{step_pathspec}/{task_id}".format(
                        step_pathspec=step.pathspec,
                        task_id=task.id
                    )
                    return control_task_pathspec
