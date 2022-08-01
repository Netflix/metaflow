from collections import namedtuple
import os

Parallel = namedtuple("Parallel", ["main_ip", "num_nodes", "node_index"])


class Current(object):
    def __init__(self):
        self._flow_name = None
        self._run_id = None
        self._step_name = None
        self._task_id = None
        self._retry_count = None
        self._origin_run_id = None
        self._namespace = None
        self._username = None
        self._metadata_str = None
        self._is_running = False

        def _raise(ex):
            raise ex

        self.__class__.graph = property(
            fget=lambda _: _raise(RuntimeError("Graph is not available"))
        )

    def _set_env(
        self,
        flow=None,
        run_id=None,
        step_name=None,
        task_id=None,
        retry_count=None,
        origin_run_id=None,
        namespace=None,
        username=None,
        metadata_str=None,
        is_running=True,
        tags=None,
    ):
        if flow is not None:
            self._flow_name = flow.name
            self.__class__.graph = property(fget=lambda _, flow=flow: flow._graph_info)

        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._retry_count = retry_count
        self._origin_run_id = origin_run_id
        self._namespace = namespace
        self._username = username
        self._metadata_str = metadata_str
        self._is_running = is_running
        self._tags = tags

    def _update_env(self, env):
        for k, v in env.items():
            setattr(self.__class__, k, property(fget=lambda _, v=v: v))

    def __contains__(self, key):
        return getattr(self, key, None) is not None

    def get(self, key, default=None):
        return getattr(self, key, default)

    @property
    def is_running_flow(self):
        """
        Returns True if called inside a running Flow, False otherwise.

        You can use this property e.g. inside a library to choose the desired
        behavior depending on the execution context.

        Returns
        -------
        bool
            True if called inside a run, False otherwise.
        """
        return self._is_running

    @property
    def flow_name(self):
        """
        The name of the currently executing flow.

        Returns
        -------
        str
            Flow name.
        """
        return self._flow_name

    @property
    def run_id(self):
        """
        The run ID of the currently executing run.

        Returns
        -------
        str
            Run ID.
        """
        return self._run_id

    @property
    def step_name(self):
        """
        The name of the currently executing step.

        Returns
        -------
        str
            Step name.
        """
        return self._step_name

    @property
    def task_id(self):
        """
        The task ID of the currently executing task.

        Returns
        -------
        str
            Task ID.
        """
        return self._task_id

    @property
    def retry_count(self):
        """
        The index of the task execution attempt.

        This property returns 0 for the first attempt to execute the task.
        If the @retry decorator is used and the first attempt fails, this
        property returns the number of times the task was attempted prior
        to the current attempt.

        Returns
        -------
        int
            The retry count.
        """
        return self._retry_count

    @property
    def origin_run_id(self):
        """
        The run ID of the original run this run was resumed from.

        This property returns None for ordinary runs. If the run
        was started by the resume command, the property returns
        the ID of the original run.

        You can use this property to detect if the run is resumed
        or not.

        Returns
        -------
        str
            Run ID of the original run.
        """
        return self._origin_run_id

    @property
    def pathspec(self):
        """
        Pathspec of the current run, i.e. a unique
        identifier of the current task. The returned
        string follows this format:
        ```
        {flow_name}/{run_id}/{step_name}/{task_id}
        ```

        Returns
        -------
        str
            Pathspec.
        """

        pathspec_components = (
            self._flow_name,
            self._run_id,
            self._step_name,
            self._task_id,
        )
        if any(v is None for v in pathspec_components):
            return None
        return "/".join(pathspec_components)

    @property
    def namespace(self):
        """
        The current namespace.

        Returns
        -------
        str
            Namespace.
        """
        return self._namespace

    @property
    def username(self):
        """
        The name of the user who started the run, if available.

        Returns
        -------
        str
            User name.
        """
        return self._username

    @property
    def parallel(self):
        return Parallel(
            main_ip=os.environ.get("MF_PARALLEL_MAIN_IP", "127.0.0.1"),
            num_nodes=int(os.environ.get("MF_PARALLEL_NUM_NODES", "1")),
            node_index=int(os.environ.get("MF_PARALLEL_NODE_INDEX", "0")),
        )

    @property
    def tags(self):
        """
        [Legacy function - do not use]

        Access tags through the Run object instead.
        """
        return self._tags


# instantiate the Current singleton. This will be populated
# by task.MetaflowTask before a task is executed.
current = Current()
