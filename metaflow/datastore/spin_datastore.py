from typing import Dict, Any
from .task_datastore import TaskDataStore, require_mode
from ..metaflow_profile import from_start


class SpinTaskDatastore(object):
    def __init__(
        self,
        flow_name: str,
        run_id: str,
        step_name: str,
        task_id: str,
        orig_datastore: TaskDataStore,
        spin_artifacts: Dict[str, Any],
    ):
        """
        SpinTaskDatastore is a datastore for a task that is used to retrieve
        artifacts and attributes for a spin step. It uses the task pathspec
        from a previous execution of the step to access the artifacts and attributes.

        Parameters:
        -----------
        flow_name : str
            Name of the flow
        run_id : str
            Run ID of the flow
        step_name : str
            Name of the step
        task_id : str
            Task ID of the step
        orig_datastore : TaskDataStore
            The datastore for the underlying task that is being spun.
        spin_artifacts : Dict[str, Any]
            User provided artifacts that are to be used in the spin task. This is a dictionary
            where keys are artifact names and values are the actual data or metadata.
        """
        self.flow_name = flow_name
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.orig_datastore = orig_datastore
        self.spin_artifacts = spin_artifacts
        self._task = None

        # Update _objects and _info in order to persist artifacts
        # See `persist` method in `TaskDatastore` for more details
        self._objects = self.orig_datastore._objects.copy()
        self._info = self.orig_datastore._info.copy()

        # We strip out some of the control ones
        for key in ("_transition",):
            if key in self._objects:
                del self._objects[key]
                del self._info[key]

        from_start("SpinTaskDatastore: Initialized artifacts")

    @require_mode(None)
    def __getitem__(self, name):
        try:
            # Check if it's an artifact in the spin_artifacts
            return self.spin_artifacts[name]
        except KeyError:
            try:
                # Check if it's an attribute of the task
                # _foreach_stack, _foreach_index, ...
                return self.orig_datastore[name]
            except (KeyError, AttributeError) as e:
                raise KeyError(
                    f"Attribute '{name}' not found in the previous execution "
                    f"of the tasks for `{self.step_name}`."
                ) from e

    @require_mode(None)
    def is_none(self, name):
        val = self.__getitem__(name)
        return val is None

    @require_mode(None)
    def __contains__(self, name):
        try:
            _ = self.__getitem__(name)
            return True
        except KeyError:
            return False

    @require_mode(None)
    def items(self):
        if self._objects:
            return self._objects.items()
        return {}
