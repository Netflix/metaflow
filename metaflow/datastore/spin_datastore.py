from typing import Dict, Any
from .task_datastore import require_mode


class SpinTaskDatastore(object):
    def __init__(
        self,
        flow_name: str,
        run_id: str,
        step_name: str,
        task_id: str,
        spin_metadata: str,
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
        spin_metadata : str
            Metadata for the spin task, typically a URI to the metadata service.
        spin_artifacts : Dict[str, Any]
            User provided artifacts that are to be used in the spin task. This is a dictionary
            where keys are artifact names and values are the actual data or metadata.
        """
        self.flow_name = flow_name
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.spin_metadata = spin_metadata
        self.spin_artifacts = spin_artifacts
        self._task = None

        # Update _objects and _info in order to persist artifacts
        # See `persist` method in `TaskDatastore` for more details
        self._objects = {}
        self._info = {}

        for artifact in self.task.artifacts:
            self._objects[artifact.id] = artifact.sha
            # Fulfills the contract for _info: name -> metadata
            self._info[artifact.id] = {
                # Do not save the type of the data
                # "type": str(type(artifact.data)),
                "size": artifact.size,
                "encoding": artifact._object["content_type"],
            }

    @property
    def task(self):
        if self._task is None:
            # Initialize the metaflow
            from metaflow import Task

            # print(f"Setting task with metadata: {self.spin_metadata} and pathspec: {self.run_id}/{self.step_name}/{self.task_id}")
            self._task = Task(
                f"{self.flow_name}/{self.run_id}/{self.step_name}/{self.task_id}",
                _namespace_check=False,
                # We need to get this form the task pathspec somehow
                _current_metadata=self.spin_metadata,
            )
        return self._task

    @require_mode(None)
    def __getitem__(self, name):
        try:
            # Check if it's an artifact in the spin_artifacts
            return self.spin_artifacts[name]
        except Exception:
            try:
                # Check if it's an attribute of the task
                # _foreach_stack, _foreach_index, ...
                return self.task.__getitem__(name).data
            except Exception:
                # If not an attribute, check if it's an artifact
                try:
                    return getattr(self.task.artifacts, name).data
                except AttributeError:
                    raise AttributeError(
                        f"Attribute '{name}' not found in the previous execution of the task for "
                        f"`{self.step_name}`."
                    )

    @require_mode(None)
    def is_none(self, name):
        val = self.__getitem__(name)
        return val is None

    @require_mode(None)
    def __contains__(self, name):
        try:
            _ = self.__getitem__(name)
            return True
        except AttributeError:
            return False

    @require_mode(None)
    def items(self):
        if self._objects:
            return self._objects.items()
        return {}
