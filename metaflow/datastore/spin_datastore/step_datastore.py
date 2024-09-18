from . import SpinDatastore


class SpinStepDatastore(SpinDatastore):
    def __init__(self, spin_parser_validator):
        super(SpinStepDatastore, self).__init__(spin_parser_validator)
        self._previous_task = None
        self._data = {}

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
        if name == "input":
            if self.foreach_index:
                _foreach_var = self.foreach_var
                _foreach_index = self.foreach_index
                _foreach_step_name = self.step_name
            elif len(self.foreach_stack) > 0:
                _foreach_stack = self.previous_task["_foreach_stack"].data
                cur_foreach_step_var = _foreach_stack[-1].var
                cur_foreach_step_index = _foreach_stack[-1].index
                cur_foreach_step_name = _foreach_stack[-1].step
                foreach_task = self.get_task_for_step(cur_foreach_step_name)
                foreach_value = foreach_task[cur_foreach_step_var].data[
                    cur_foreach_step_index
                ]
                setattr(self, name, foreach_value)
                return foreach_value

            foreach_task = self.get_task_for_step(_foreach_step_name)
            foreach_value = foreach_task[_foreach_var].data[_foreach_index]
            setattr(self, name, foreach_value)
            return foreach_value

        # If the linear step is part of a foreach step, we need to set the index attribute
        if name == "index":
            if self.foreach_index:
                setattr(self, name, self.foreach_index)
                return self.foreach_index
            if len(self.foreach_stack) > 0:
                cur_foreach_step_index = (
                    self.previous_task["_foreach_stack"].data[-1].index
                )
                setattr(self, name, cur_foreach_step_index)
                return cur_foreach_step_index
            raise AttributeError(
                f"Attribute index does not exist for step `{self.step_name}` as it is not part of a foreach step."
            )

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
        # Since this is not a join step, we can safely assume that there is only one
        # previous step and one corresponding previous task
        if self.spin_parser_validator.previous_steps_task:
            return self.spin_parser_validator.previous_steps_task

        if self._previous_task:
            return self._previous_task

        prev_step_name = self.previous_steps[0]
        self._previous_task = self.get_all_previous_tasks(prev_step_name)[0]
        return self._previous_task

    def get(self, key, default=None):
        try:
            return self.__getattr__(key)
        except AttributeError:
            return default
