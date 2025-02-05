from itertools import chain


class SpinInput(object):
    def __init__(self, artifacts, task):
        self.artifacts = artifacts
        self.task = task

    def __getattr__(self, name):
        # We always look for any artifacts provided by the user first
        if self.artifacts is not None and name in self.artifacts:
            return self.artifacts[name]

        try:
            return getattr(self.task.artifacts, name).data
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.task.parent.id}`."
            )


class StaticSpinInputsDatastore(object):
    def __init__(self, task, immediate_ancestors, artifacts={}):
        self.task = task
        self.immediate_ancestors = immediate_ancestors
        self.artifacts = artifacts

        self._previous_tasks = None
        self._previous_steps = None

    @property
    def previous_tasks(self):
        if self._previous_tasks:
            return self._previous_tasks

        from metaflow import Task

        prev_task_pathspecs = self.immediate_ancestors
        # Static Join step, so each previous step only has one task
        self._previous_tasks = {
            step_name: Task(prev_task_pathspec[0], _namespace_check=False)
            for step_name, prev_task_pathspec in prev_task_pathspecs.items()
        }
        return self._previous_tasks

    @property
    def previous_steps(self):
        if self._previous_steps:
            return self._previous_steps
        self._previous_steps = self.task.metadata_dict.get("previous-steps")
        return self._previous_steps

    def __getattr__(self, name):
        if name not in self.previous_steps:
            raise AttributeError(
                f"Step '{self.task.parent.id}' does not have a previous step with name '{name}'."
            )

        input_step = SpinInput(
            self.artifacts.get(
                name, {}
            ),  # Get the artifacts corresponding to the previous step
            self.previous_tasks.get(
                name
            ),  # Get the task corresponding to the previous step
        )
        setattr(self, name, input_step)
        return input_step

    def __iter__(self):
        for prev_step_name in self.previous_steps:
            yield getattr(self, prev_step_name)

    def __len__(self):
        return len(self.previous_steps)


class SpinInputsDatastore(object):
    def __init__(self, task, immediate_ancestors, artifacts={}):
        self.task = task
        self.immediate_ancestors = immediate_ancestors
        self.artifacts = artifacts

        self._previous_tasks = None

    def __len__(self):
        return len(self.previous_tasks)

    def __getitem__(self, idx):
        _item_task = self.previous_tasks[idx]
        _item_artifacts = self.artifacts.get(self.previous_step, {}).get(idx, {})
        return SpinInput(_item_artifacts, _item_task)

    def __iter__(self):
        for idx in range(len(self.previous_tasks)):
            yield self[idx]

    @property
    def previous_step(self):
        return self.task.metadata_dict.get("previous-steps")[0]

    @property
    def previous_tasks(self):
        if self._previous_tasks:
            return self._previous_tasks

        # Foreach Join step so we have one previous step with multiple tasks
        from metaflow import Task

        prev_task_pathspecs = list(
            chain.from_iterable(self.immediate_ancestors.values())
        )
        self._previous_tasks = [
            Task(prev_task_pathspec, _namespace_check=False)
            for prev_task_pathspec in prev_task_pathspecs
        ]
        self._previous_tasks = sorted(self._previous_tasks, key=lambda x: x.index)
        return self._previous_tasks
