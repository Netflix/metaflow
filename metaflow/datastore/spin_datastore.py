# A *read-only* datastore that fetches artifacts through Metaflow’s
# Client API.  All mutating helpers are implemented as cheap no-ops so
# that existing runtime paths which expect them won’t break.
from types import MethodType, FunctionType
from ..parameters import Parameter
from .task_datastore import (
    require_mode,
)


class SpinDataStore(object):
    """
    Minimal, read-only replacement for TaskDataStore.

    Artefacts are lazily materialised through the Metaflow Client
    (`metaflow.Task(...).data`).  All write/side-effecting methods are
    stubbed out.
    """

    def __init__(self, flow_name, run_id, step_name, task_id, mode="r"):
        assert mode in ("r",)  # write modes unsupported
        self._mode = mode
        self._flow_name = flow_name
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._is_done_set = True  # always read-only
        self._task = None

    # Public API
    @property
    def pathspec(self):
        return f"{self._run_id}/{self._step_name}/{self._task_id}"

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
    def task(self):
        if self._task is None:
            # Metaflow client task handle
            # from metaflow.client.core import get_metadata
            from metaflow import Task

            # tp = get_metadata()
            # print(f"tp: {tp}")
            # print("LALALALA")
            self._task = Task(
                f"{self._flow_name}/{self._run_id}/{self._step_name}/{self._task_id}",
                _namespace_check=False,
                _current_metadata="mli@https://mliservice.dynprod.netflix.net:7002/api/v0",
            )
            # print(f"_metaflow: {self._task._metaflow}")
        return self._task

    # artifact access and iteration helpers
    @require_mode(None)
    def __getitem__(self, name):
        print(f"I am in SpinDataStore __getitem__ for {name}")
        try:
            # Attempt to access the artifact directly from the task
            # Used for `_foreach_stack`, `_graph_info`, etc.
            print(f"Task: {self.task}")
            print(f"Task ID: {self.task.id}")
            print(f"_graph_info: {self.task['_graph_info']}")
            res = self.task.__getitem__(name)
        except Exception as e:
            print(f"Exception accessing {name} directly from task: {e}")
            print(
                f"Failed to access {name} directly from task, falling back to artifacts."
            )
            # If the direct access fails, fall back to the artifacts
            try:
                res = getattr(self.task.artifacts, name).data
            except AttributeError:
                raise AttributeError(
                    f"Attribute '{name}' not found in the previous execution of the task for "
                    f"`{self.step_name}`."
                )
        return res

    @require_mode("r")
    def __contains__(self, name):
        return hasattr(self.task.artifacts, name)

    @require_mode("r")
    def __iter__(self):
        for name in self.task.artifacts:
            yield name, getattr(self.task.artifacts, name).data

    @require_mode("r")
    def keys_for_artifacts(self, names):
        return [None for _ in names]

    @require_mode(None)
    def load_artifacts(self, names):
        for n in names:
            yield n, getattr(self.task.artifacts, n).data

    # metadata & logging helpers
    def load_metadata(self, names, add_attempt=True):
        return {n: None for n in names}

    def has_metadata(self, name, add_attempt=True):
        return False

    def get_log_location(self, *a, **k):
        return None

    def load_logs(self, *a, **k):
        return []

    def load_log_legacy(self, *a, **k):
        return b""

    def get_log_size(self, *a, **k):
        return 0

    def get_legacy_log_size(self, *a, **k):
        return 0

    # write-side no-ops
    def init_task(self, *a, **k):
        pass

    def save_artifacts(self, *a, **k):
        pass

    def save_metadata(self, *a, **k):
        pass

    def _dangerous_save_metadata_post_done(self, *a, **k):
        pass

    def save_logs(self, *a, **k):
        pass

    def scrub_logs(self, *a, **k):
        pass

    def clone(self, *a, **k):
        pass

    def passdown_partial(self, *a, **k):
        pass

    def persist(self, flow, *a, **k):
        # Should we just do __setitem__ or __setattr__ here?

        print(f"flow: {flow}")
        valid_artifacts = []
        for var in dir(flow):
            if var.startswith("__") or var in flow._EPHEMERAL:
                continue
            # Skip over properties of the class (Parameters or class variables)
            if hasattr(flow.__class__, var) and isinstance(
                getattr(flow.__class__, var), property
            ):
                continue

            val = getattr(flow, var)
            if not (
                isinstance(val, MethodType)
                or isinstance(val, FunctionType)
                or isinstance(val, Parameter)
            ):
                valid_artifacts.append((var, val))

        print(f"valid_artifacts: {valid_artifacts}")
        # Use __setattr__ to set the attributes on the SpinDataStore instance
        for name, value in valid_artifacts:
            # print(f"Setting {name} to {value}")
            setattr(self, name, value)
        # print("Calling persist on SpinDataStore, which is a no-op.")
        pass

    def done(self, *a, **k):
        pass
