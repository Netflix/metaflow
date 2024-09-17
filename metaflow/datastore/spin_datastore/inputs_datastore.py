class SpinInput(object):
    def __init__(self, artifacts, task=None):
        self.artifacts = artifacts
        self.task = task

    def __getattr__(self, name):
        # We always look for any artifacts provided by the user first
        if name in self.artifacts:
            return self.artifacts[name]

        if self.task is None:
            raise AttributeError(
                f"Attribute '{name}' not provided by the user and no `task` was provided."
            )

        try:
            return __getattr__(self.task.artifacts, name)
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.step_name}`."
            )


class StaticSpinInputsDatastore:
    def __init__(self, spin_parser_validator):
        super(StaticSpinInputsDatastore, self).__init__(spin_parser_validator)
        self._previous_tasks = {}

    @property
    def previous_tasks(self):
        if self._previous_tasks:
            return self._previous_tasks

        for prev_step_name in self.previous_steps:
            previous_task = self.previous_tasks(prev_step_name)
            self._previous_tasks[prev_step_name] = previous_task
        return self._previous_tasks


class SpinInputsDatastore:
    def __init__(self, spin_parser_validator):
        super(SpinInputsDatastore, self).__init__(spin_parser_validator)
        self._previous_tasks = None

    @property
    def previous_tasks(self):
        if self._previous_tasks:
            return self._previous_tasks
        pass
