from .spin_datastore import SpinDatastore


class SpinStepDatastore(SpinDatastore):
    def __init__(self, spin_parser_validator):
        super(SpinStepDatastore, self).__init__(spin_parser_validator)
        self._previous_task = None

    def __getattr__(self, name):
        # We always look for any artifacts provided by the user first
        if name in self.artifacts:
            return self.artifacts[name]

        if self.run_id is None:
            raise AttributeError(
                f"Attribute '{name}' not provided by the user and no `run_id` was provided. "
            )

        # If the linear step is part of a foreach step, we need to set the input attribute
        if len(self.foreach_stack) > 0:
            cur_foreach_step_var = self.foreach_stack[-1].var
            setattr(self, name, cur_foreach_step_var)
            return cur_foreach_step_var

        # If the linear step is part of a foreach step, we need to set the index attribute
        if len(self.foreach_stack) > 0:
            cur_foreach_step_index = self.foreach_stack[-1].index
            setattr(self, name, cur_foreach_step_index)
            return cur_foreach_step_index

        # If the user has not provided the artifact, we look for it in the
        # task using the client API
        try:
            return __getattr__(self.previous_task.artifacts, name)
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.step_name}`."
            )

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
        self._previous_task = self.previous_tasks(prev_step_name)
        return previous_task
