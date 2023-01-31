from collections import namedtuple
from typing import Optional

from .flowspec import FlowSpec
import os

Parallel = namedtuple("Parallel", ["main_ip", "num_nodes", "node_index"])


class Current(object):
    def __init__(self):
        self._flow = None
        self._task = None
        self._flow_name = None
        self._run_id = None
        self._step_name = None
        self._task_id = None
        self._retry_count = None
        self._origin_run_id = None
        self._namespace = None
        self._username = None
        self._is_running = False

        def _raise(ex):
            raise ex

        self.__class__.graph = property(
            fget=lambda _: _raise(RuntimeError("Graph is not available"))
        )

    def _set_env(
        self,
        flow=None,
        task=None,
        run_id=None,
        step_name=None,
        task_id=None,
        retry_count=None,
        origin_run_id=None,
        namespace=None,
        username=None,
        is_running=True,
    ):
        if flow is not None:
            self._flow = flow
            self._flow_name = flow.name
            self.__class__.graph = property(fget=lambda _, flow=flow: flow._graph_info)

        self._task = task
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._retry_count = retry_count
        self._origin_run_id = origin_run_id
        self._namespace = namespace
        self._username = username
        self._is_running = is_running

    def _update_env(self, env):
        for k, v in env.items():
            setattr(self.__class__, k, property(fget=lambda _, v=v: v))

    def __contains__(self, key):
        return getattr(self, key, None) is not None

    def get(self, key, default=None):
        return getattr(self, key, default)

    @property
    def is_running_flow(self):
        return self._is_running

    @property
    def flow(self) -> FlowSpec:
        return self._flow

    def task_log_location(self, log_prefix: str, stream: Optional[str] = None) -> str:
        """
        This function returns the current Task and attempt datastore appropriate location
        to store logs.

        Args:
            log_prefix (str): The log prefix.
            stream (Optional[str], optional):
                Returns the following format: f"{attempt}.{log_prefix}_{stream}.log"
                If None, then the following is returned: f"{attempt}.{log_prefix}.log"

        Returns:
            str: Task and attempt specific datastore path to log.

        Examples:
            Notice that the attempt number is prefixed to the log location path,
            for example "0." on the first attempt, and on a retry the prefix
            would be "1."

            >>> current.log_location('lightning_logs')
                ".metaflow/LightningFlow/4718/start/29753/0.lightning_logs"

            >>> current.log_location('lightning', 'out')
                ".metaflow/LightningFlow/4718/start/29753/0.lightning_out.log"
        """
        ds = self._task.flow_datastore.get_task_datastore(
            self.run_id,
            self.step_name,
            self.task_id,
            mode="r",
            attempt=self.retry_count,
            allow_not_done=True,
        )
        return ds.get_log_location(log_prefix, stream)

    @property
    def flow_name(self):
        return self._flow_name

    @property
    def run_id(self):
        return self._run_id

    @property
    def step_name(self):
        return self._step_name

    @property
    def task_id(self):
        return self._task_id

    @property
    def retry_count(self):
        return self._retry_count

    @property
    def origin_run_id(self):
        return self._origin_run_id

    @property
    def pathspec(self):
        return "/".join((self._flow_name, self._run_id, self._step_name, self._task_id))

    @property
    def namespace(self):
        return self._namespace

    @property
    def username(self):
        return self._username

    @property
    def parallel(self):
        return Parallel(
            main_ip=os.environ.get("MF_PARALLEL_MAIN_IP", "127.0.0.1"),
            num_nodes=int(os.environ.get("MF_PARALLEL_NUM_NODES", "1")),
            node_index=int(os.environ.get("MF_PARALLEL_NODE_INDEX", "0")),
        )


# instantiate the Current singleton. This will be populated
# by task.MetaflowTask before a task is executed.
current = Current()
