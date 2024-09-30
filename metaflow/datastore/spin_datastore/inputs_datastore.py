from . import SpinDatastore


class SpinInput(object):
    def __init__(self, artifacts, task=None):
        self.artifacts = artifacts
        self.task = task

    def __getattr__(self, name):
        # We always look for any artifacts provided by the user first
        if self.artifacts is not None and name in self.artifacts:
            return self.artifacts[name]

        if self.task is None:
            raise AttributeError(
                f"Attribute '{name}' not provided by the user and no `task` was provided."
            )

        try:
            return getattr(self.task.artifacts, name).data
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.step_name}`."
            )

        raise AttributeError(
            f"Attribute '{name}' not found in the artifacts provided by the user or in the"
            f"the previous execution of the task for `{self.step_name}`"
        )


class StaticSpinInputsDatastore(SpinDatastore):
    def __init__(self, spin_parser_validator):
        super(StaticSpinInputsDatastore, self).__init__(spin_parser_validator)
        self._previous_tasks = {}

    def __getattr__(self, name):
        if name not in self.previous_steps:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.step_name}`."
            )

        input_step = SpinInput(
            self.spin_parser_validator.artifacts["join"][name],
            self.get_previous_tasks[name],
        )
        setattr(self, name, input_step)
        return input_step

    def __iter__(self):
        for prev_step_name in self.previous_steps:
            yield self[prev_step_name]

    def __len__(self):
        return len(self.get_previous_tasks)

    @property
    def get_previous_tasks(self):
        if self._previous_tasks:
            return self._previous_tasks

        for prev_step_name in self.previous_steps:
            previous_task = self.get_all_previous_tasks(prev_step_name)
            self._previous_tasks[prev_step_name] = previous_task
        return self._previous_tasks


class SpinInputsDatastore(SpinDatastore):
    def __init__(self, spin_parser_validator):
        super(SpinInputsDatastore, self).__init__(spin_parser_validator)
        self._previous_tasks = None

    def __len__(self):
        return len(self.get_previous_tasks)

    def __getitem__(self, idx):
        _item_task = self.get_previous_tasks[idx]
        _item_artifacts = self.spin_parser_validator.artifacts
        # _item_artifacts = self.spin_parser_validator.artifacts[idx]
        return SpinInput(_item_artifacts, _item_task)

    def __iter__(self):
        for idx in range(len(self.get_previous_tasks)):
            yield self[idx]

    @property
    def get_previous_tasks(self):
        if self._previous_tasks:
            return self._previous_tasks

        # This a join step for a foreach split, so only has one previous step
        prev_step_name = self.previous_steps[0]
        self._previous_tasks = self.get_all_previous_tasks(prev_step_name)
        # Sort the tasks by index
        self._previous_tasks = sorted(self._previous_tasks, key=lambda x: x.index)
        return self._previous_tasks
