class LinearStepDatastore(object):
    def __init__(self, task_pathspec):
        from metaflow import Task

        self._task_pathspec = task_pathspec
        self._task = Task(task_pathspec, _namespace_check=False)
        self._previous_task = None
        self._data = {}

        # Set them to empty dictionaries in order to persist artifacts
        # See `persist` method in `TaskDatastore` for more details
        self._objects = {}
        self._info = {}

    def __contains__(self, name):
        try:
            _ = self.__getattr__(name)
        except AttributeError:
            return False
        return True

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __setitem__(self, name, value):
        self._data[name] = value

    def __getattr__(self, name):
        # Check internal data first
        if name in self._data:
            return self._data[name]

        # We always look for any artifacts provided by the user first
        if name in self.artifacts:
            return self.artifacts[name]

        if self.run_id is None:
            raise AttributeError(
                f"Attribute '{name}' not provided by the user and no `run_id` was provided. "
            )

        # If the linear step is part of a foreach step, we need to set the input attribute
        # and the index attribute
        if name == "input":
            if not self._task.index:
                raise AttributeError(
                    f"Attribute '{name}' does not exist for step `{self.step_name}` as it is not part of a foreach step."
                )

            foreach_stack = self._task["_foreach_stack"].data
            foreach_index = foreach_stack[-1].index
            foreach_var = foreach_stack[-1].var

            # Fetch the artifact corresponding to the foreach var and index from the previous task
            input_val = self.previous_task[foreach_var].data[foreach_index]
            setattr(self, name, input_val)
            return input_val

        # If the linear step is part of a foreach step, we need to set the index attribute
        if name == "index":
            if not self._task.index:
                raise AttributeError(
                    f"Attribute '{name}' does not exist for step `{self.step_name}` as it is not part of a foreach step."
                )
            foreach_stack = self._task["_foreach_stack"].data
            foreach_index = foreach_stack[-1].index
            setattr(self, name, foreach_index)
            return foreach_index

        # If the user has not provided the artifact, we look for it in the
        # task using the client API
        try:
            return getattr(self.previous_task.artifacts, name).data
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.step_name}`."
            )

    @property
    def previous_task(self):
        # This is a linear step, so we only have one immediate ancestor
        if self._previous_task:
            return self._previous_task

        prev_task_pathspecs = self._task.immediate_ancestors
        prev_task_pathspec = list(chain.from_iterable(prev_task_pathspecs.values()))[0]
        self._previous_task = Task(prev_task_pathspec, _namespace_check=False)
        return self._previous_task

    def get(self, key, default=None):
        try:
            return self.__getattr__(key)
        except AttributeError:
            return default
