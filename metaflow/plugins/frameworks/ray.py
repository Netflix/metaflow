import os
import sys
import time
import json
import signal
import subprocess
from pathlib import Path
from threading import Thread
from metaflow.exception import MetaflowException
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.plugins.parallel_decorator import ParallelDecorator, _local_multinode_control_task_step_func

RAY_CHECKPOINT_VAR_NAME = 'checkpoint_path'
RAY_JOB_COMPLETE_VAR = 'ray_job_completed'
RAY_NODE_STARTED_VAR = 'node_started'
CONTROL_TASK_S3_ID = 'control'

class RayParallelDecorator(ParallelDecorator):

    name = "ray_parallel"
    defaults = {"main_port": None, "worker_polling_freq": 10, "all_nodes_started_timeout": 90}
    IS_PARALLEL = True

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):

        from functools import partial
        from metaflow import S3, current
        from metaflow.metaflow_config import DATATOOLS_S3ROOT

        def _empty_worker_task():
            pass # local case

        def _worker_heartbeat(polling_freq=self.attributes["worker_polling_freq"], var=RAY_JOB_COMPLETE_VAR):
            while not json.loads(s3.get(CONTROL_TASK_S3_ID).blob)[var]:
                time.sleep(polling_freq)

        def _control_wrapper(step_func, flow, var=RAY_JOB_COMPLETE_VAR):
            watcher = NodeParticipationWatcher(expected_num_nodes=current.num_nodes, polling_freq=10)
            try:
                step_func()
            except Exception as e:
                raise ControlTaskException(e)
            finally:
                watcher.end()
            s3.put(CONTROL_TASK_S3_ID, json.dumps({var: True}))

        s3 = S3(run=flow)
        ensure_ray_installed()

        if os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local":
            checkpoint_path = os.path.join(os.getcwd(), "ray_checkpoints")
        else:
            checkpoint_path = os.path.join(
                DATATOOLS_S3ROOT, current.flow_name, current.run_id, "ray_checkpoints"
            )
        setattr(flow, RAY_CHECKPOINT_VAR_NAME, checkpoint_path)

        if os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local":
            if ubf_context == UBF_CONTROL:
                env_to_use = getattr(self.environment, "base_env", self.environment)
                return partial(
                    _local_multinode_control_task_step_func,
                    flow,
                    env_to_use, 
                    step_func,
                    retry_count,
                )    
            return partial(_empty_worker_task)
        else:
            self.setup_distributed_env(flow, ubf_context)
            if ubf_context == UBF_CONTROL:
                return partial(_control_wrapper, step_func=step_func, flow=flow)
            return partial(_worker_heartbeat)

    def setup_distributed_env(self, flow, ubf_context):
        py_cli_path = Path(sys.executable).resolve()
        py_exec_dir = py_cli_path.parent
        ray_cli_path = py_exec_dir / "ray"

        if ray_cli_path.is_file():
            ray_cli_path = ray_cli_path.resolve()
            setup_ray_distributed(self.attributes["main_port"], self.attributes["all_nodes_started_timeout"],
                                  str(ray_cli_path), flow, ubf_context)
        else:
            print("'ray' executable not found in:", ray_cli_path)


def setup_ray_distributed(
    main_port,
    all_nodes_started_timeout,
    ray_cli_path, 
    run, 
    ubf_context
):

    import ray
    import json
    import socket
    from metaflow import S3, current

    # Why are deco.task_pre_step and deco.task_decorate calls in the same loop?
    # https://github.com/Netflix/metaflow/blob/76eee802cba1983dffe7e7731dd8e31e2992e59b/metaflow/task.py#L553
        # The way this runs now causes these current.parallel variables to be defaults on all nodes,
        # since AWS Batch decorator task_pre_step hasn't run prior to the above task_decorate call.
    # num_nodes = current.parallel.num_nodes
    # node_index = current.parallel.node_index

    # AWS Batch-specific workaround.
    num_nodes = int(os.environ["AWS_BATCH_JOB_NUM_NODES"])
    node_index = os.environ["AWS_BATCH_JOB_NODE_INDEX"]
    node_key = os.path.join(RAY_NODE_STARTED_VAR, "node_%s.json" % node_index)
    current._update_env({'num_nodes': num_nodes})

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
        s3.put('control', json.dumps({RAY_JOB_COMPLETE_VAR: False}))
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
        raise RayWorkerFailedStartException(node_index)
    else:
        s3.put(node_key, json.dumps({'node_started': True}))

    def _num_nodes_started(path=RAY_NODE_STARTED_VAR):
        objs = s3.get_recursive([path])
        num_started = 0
        for obj in objs:
            obj = json.loads(obj.text)
            if obj['node_started']:
                num_started += 1
            else:
                raise RayWorkerFailedStartException(node_index)
        return num_started
    
    # poll until all workers have joined the cluster
    if ubf_context == UBF_CONTROL:
        t0 = time.time()
        while _num_nodes_started() < num_nodes:
            if all_nodes_started_timeout <= time.time() - t0:
                raise AllNodesStartupTimeoutException()
            time.sleep(10)

    s3.close()


def ensure_ray_installed():
    while True:
        try:
            import ray
            break
        except ImportError:
            print("Ray is not installed. Installing latest version of ray-air package.")
            subprocess.run([sys.executable, "-m", "pip", "install", "-U", "ray[air]"], check=True)
    

class NodeParticipationWatcher(object):

    def __init__(self, expected_num_nodes, polling_freq=10, t_user_code_start_buffer=30):
        self.t_user_code_start_buffer = t_user_code_start_buffer
        self.expected_num_nodes = expected_num_nodes
        self.polling_freq = polling_freq
        self._thread = Thread(target = self._enforce_participation)
        self.is_alive = True
        self._thread.start()

    def end(self):
        self.is_alive = False

    def _enforce_participation(self):

        import ray

        # Why this sleep?
        time.sleep(self.t_user_code_start_buffer)
        # The user code is expected to run ray.init(), in line with ergonomic Ray workflows.
        # To run self._num_nodes_started() in following loop, ray.init() needs to already run.
        # If we don't wait for user code to run ray.init(), 
            # then we need to do it before this loop,
            # which causes the user code ray.init() to throw error like:
                # `Maybe you called ray.init twice by accident?`
                # and will ask user to put 'ignore_reinit_error=True' in 'ray.init()', which is annoying UX.
        # So we wait for user code to run ray.init() before we run self._num_nodes_started() in following loop.

        while self.is_alive:
            n = self._num_nodes(ray)
            if n < self.expected_num_nodes:
                self.is_alive = False
                self._kill_run(n)
            time.sleep(self.polling_freq)

    def _num_nodes(self, ray):
        return len(ray._private.state.state._live_node_ids()) # Should this use ray._private.state.node_ids()?

    def _kill_run(self, n):
        msg = "Node {} stopped participating. Expected {} nodes to participate.".format(n, self.expected_num_nodes)
        print(msg)
        os.kill(os.getpid(), signal.SIGINT)


class ControlTaskException(MetaflowException):
    headline = "Contral task error"

    def __init__(self, e):
        msg = """
Spinning down all workers because of the following exception running the @step code on the control task:
    {exception_str}
        """.format(exception_str=str(e))
        super(ControlTaskException, self).__init__(msg)


class RayWorkerFailedStartException(MetaflowException):
    headline = "Worker task startup error"

    def __init__(self, node_index):
        msg = "Worker task failed to start on node {}".format(node_index)
        super(RayWorkerFailedStartException, self).__init__(msg)


class AllNodesStartupTimeoutException(MetaflowException):
    headline = "All workers did not join cluster error"

    def __init__(self):
        msg = "Exiting job due to time out waiting for all workers to join cluster. You can set the timeout in @ray_parallel(all_nodes_started_timeout=X)"
        super(AllNodesStartupTimeoutException, self).__init__(msg)