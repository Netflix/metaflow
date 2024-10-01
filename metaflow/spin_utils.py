from typing import List
from metaflow.exception import CommandException
from .util import REQUIRED_ANCESTORS


class SpinParserValidator(object):
    def __init__(
        self,
        ctx,
        step_name,
        prev_run_id,
        ancestor_tasks=None,
        artifacts=None,
        artifacts_module=None,
        foreach_index=None,
        foreach_value=None,
        skip_decorators=False,
        step_func=None,
    ):
        self.ctx = ctx
        self.step_name = step_name
        self.run_id = prev_run_id
        self.ancestor_tasks = ancestor_tasks
        self.artifacts = artifacts if artifacts else []
        self.artifacts_module = artifacts_module
        self.skip_decorators = skip_decorators
        self.step_func = step_func
        self._foreach_index = foreach_index
        self._foreach_value = foreach_value
        self._task = None
        self._previous_steps = None
        self._graph_info = None
        self._step_type = None
        self._previous_step_type = None
        self._parsed_ancestor_tasks = None
        self._required_ancestor_tasks = None
        self._previous_steps_task = None
        self._step_decorators = None

    @property
    def foreach_var(self):
        return self.foreach_stack[-1].var

    @property
    def foreach_index(self):
        return self._foreach_index

    @property
    def foreach_value(self):
        # TODO: Generate the foreach_index corresponding to this value
        return self._foreach_value

    @property
    def task(self):
        """
        Returns an instance of the task corresponding to the step name and run id.
        """
        if self.run_id is None:
            return None
        if self._task:
            return self._task

        from metaflow import Step

        step = Step(f"{self.ctx.obj.flow.name}/{self.run_id}/{self.step_name}")
        self._task = next(iter(step.tasks()))
        return self._task

    @property
    def step_decorators(self):
        if self.skip_decorators:
            return []
        if self._step_decorators:
            return self._step_decorators
        decorators = []
        for decorator in self.step_func.decorators:
            if decorator.name in ["environment", "pypi", "conda"]:
                decorators.append(decorator)
        self._step_decorators = decorators
        return self._step_decorators

    @property
    def flow_name(self):
        """
        Returns the name of the flow.
        """
        return self.ctx.obj.flow.name

    @property
    def graph_info(self):
        """
        Returns the graph info for the current step.
        """
        if self._graph_info:
            return self._graph_info
        self._graph_info = self.task["_graph_info"].data
        return self._graph_info

    @property
    def previous_steps(self) -> List[str]:
        """
        Returns the previous nodes for a given node.

        Returns
        -------
        List[str]
            The list of previous nodes.
        """
        if self._previous_steps:
            return self._previous_steps
        if self.step_name == "start":
            return []
        self._previous_steps = [
            node_name
            for node_name, attributes in self.graph_info["steps"].items()
            if self.step_name in attributes["next"]
        ]
        return self._previous_steps

    @property
    def step_type(self):
        """
        Returns the type of the current step.
        """
        if self._step_type:
            return self._step_type
        self._step_type = self.graph_info["steps"][self.step_name]["type"]
        return self._step_type

    @property
    def is_linear_foreach(self):
        """
        Returns whether the current step is the start of a new for-each loop.
        """
        if self.step_type == "split-foreach" or self.step_type == "linear":
            # It is not a join step, so it will have at most one previous step
            prev_step_type = self.graph_info["steps"][self.previous_steps[0]]["type"]
            return prev_step_type == "split-foreach"
        return False

    @property
    def foreach_stack(self):
        """
        Returns the foreach stack for the current step.
        """
        return self.task["_foreach_stack"].data

    @property
    def foreach_step_var(self):
        """
        Returns the foreach variable for the current step.
        """
        return self.foreach_stack[-1].var

    @property
    def foreach_stack_steps(self):
        return set([foreach_frame.step for foreach_frame in self.foreach_stack])

    @property
    def parsed_ancestor_tasks(self):
        """
        Validates the ancestor tasks and returns a parsed dictionary
        """
        if self._parsed_ancestor_tasks:
            return self._parsed_ancestor_tasks

        # `ancestor_tasks` is a JSON string with key-value pairs in the following format:
        # {
        #     "step_name1.task_specifier1": "task_val1",
        #     "step_name2.task_specifier2": "task_val2",
        #      ...
        # }
        # where task_specifier can be one of task_id, task_index, or task_val
        import json as _json

        try:
            ancestor_tasks = _json.loads(self.ancestor_tasks)
        except (ValueError, TypeError):
            raise CommandException(
                f"Step *`{self.step_name}`* needs to be spun with ancestor tasks and Invalid JSON format for "
                "*`ancestor_tasks`*. Please provide a valid JSON string."
            )
        ancestor_tasks_parsed = {}
        for ancestor_task, ancestor_task_val in ancestor_tasks.items():
            step_name, task_specifier = ancestor_task.split(".")
            if task_specifier not in ["task_id", "task_index", "task_val"]:
                raise CommandException(
                    f"Invalid task specifier {task_specifier}. Must be one of task_id, task_index, or task_val."
                )
            ancestor_tasks_parsed[step_name] = [task_specifier, ancestor_task_val]
        self._parsed_ancestor_tasks = ancestor_tasks_parsed
        return self._parsed_ancestor_tasks

    @property
    def required_ancestor_tasks(self):
        """
        Returns the required ancestor tasks for spinning the task.
        """
        return self._required_ancestor_tasks

    @property
    def previous_steps_task(self):
        """
        Returns the task corresponding to the previous step.
        """
        return self._previous_steps_task

    def _verify_required_ancestor_tasks(self):
        """
        Verifies that the required ancestor tasks are provided for spinning the task
        and returns the ancestor tasks that are required to recreate the input artifacts for the task.
        """
        # To recreate the input artifacts for the `spin` task, we need to know the ancestor tasks
        # Ancestor tasks are the tasks that lie in the path from the current step to the start step
        # and are part of the current step's foreach stack
        required_ancestor_tasks = []
        for foreach_frame in reversed(self.foreach_stack):
            cur_foreach_step_name, cur_foreach_step_var = (
                foreach_frame.step,
                foreach_frame.var,
            )
            if cur_foreach_step_name == self.step_name:
                if cur_foreach_step_name in self.parsed_ancestor_tasks:
                    if self.foreach_value:
                        raise CommandException(
                            f"*`{self.step_name}`* is the start of a new for-each loop and you cannot provide "
                            f"*`{self.step_name}`* as an ancestor task for itself. You have provided "
                            f"the foreach variable *`{self.foreach_value}`* for the foreach loop variable "
                            f"*`{cur_foreach_step_var}`* already."
                        )
                    raise CommandException(
                        f"*`{self.step_name}`* is the start of a new for-each loop and you cannot provide "
                        f"*`{self.step_name}`* as an ancestor task for itself. You have provided "
                        f"the foreach index *`{self.foreach_index}`* for the foreach loop variable "
                        f"*`{cur_foreach_step_var}`* already."
                    )
                # If the current step is the start of a new for-each loop, we don't need any task_specifier
                # for the current step
                continue

            if cur_foreach_step_name not in self.parsed_ancestor_tasks:
                raise CommandException(
                    f"*`{self.step_name}`* is part of a for-each and requires either a task_id for "
                    f"*`{cur_foreach_step_name}`*, a task index corresponding to foreach variable "
                    f"*`{cur_foreach_step_var}`*, or a matching entry in foreach variable *`{cur_foreach_step_var}`*."
                )

            required_ancestor_tasks.append(
                REQUIRED_ANCESTORS(
                    cur_foreach_step_name,
                    self.parsed_ancestor_tasks[cur_foreach_step_name][0],
                    self.parsed_ancestor_tasks[cur_foreach_step_name][1],
                )
            )
            if "task_id" in self.parsed_ancestor_tasks[cur_foreach_step_name]:
                # If we know the task_id for one of the ancestor tasks in the path from
                # current step to the start step, we don't need to check any of the previous
                # ancestor tasks
                break

        # Reverse the required ancestor tasks since we will always iterate from the oldest ancestor
        required_ancestor_tasks = required_ancestor_tasks[::-1]
        self._display_required_params(required_ancestor_tasks)
        return required_ancestor_tasks

    def _display_required_params(self, required_ancestor_tasks):
        self.ctx.obj.echo(
            f"Using following ancestors for spinning {self.step_name}:",
            fg="magenta",
            bold=True,
        )
        for required_ancestor in required_ancestor_tasks:
            self.ctx.obj.echo(
                f"Using *{required_ancestor.task_specifier}* with value *{required_ancestor.value}* for "
                f"*{required_ancestor.step_name}* as ancestor task.",
                indent=True,
            )
        if self.is_linear_foreach:
            if self.foreach_value:
                self.ctx.obj.echo(
                    f"Using value *`{self.foreach_value}`* corresponding to the foreach variable "
                    f"*`{self.foreach_step_var}`* for *{self.step_name}*.",
                    indent=True,
                )
            else:
                self.ctx.obj.echo(
                    f"Using index *`{self.foreach_index}`* corresponding to the foreach variable "
                    f"*`{self.foreach_step_var}`* for *{self.step_name}*.",
                    indent=True,
                )

    def validate(self):
        if not any([self.run_id, self.artifacts, self.artifacts_module]):
            raise CommandException(
                "Either *`run_id`* or *`artifacts`* / *`artifacts_module`* should be provided to spin a task."
            )

        # If artifacts are provided, we don't need a run_id, and we don't need to validate the ancestor tasks
        if self.run_id is None:
            return True

        try:
            from metaflow import Run

            run = Run(f"{self.flow_name}/{self.run_id}")
        except Exception as e:
            raise CommandException(
                f"Run *`{self.run_id}`* not found for flow *`{self.flow_name}`*."
            )

        # If the spin step is the start of a new for-each loop, we need to provide either the foreach_index or the
        # foreach_var corresponding to the foreach variable. Raise an error if neither is provided.
        if (
            self.foreach_value is None
            and self.foreach_index is None
            and self.is_linear_foreach
        ):
            raise CommandException(
                f"*`{self.step_name}`* is the start of a new for-each loop. Please provide either the *`foreach_index`*"
                f" or the *`foreach_var`* corresponding to the foreach variable *`{self.foreach_step_var}`*."
            )

        if not self.is_linear_foreach and (self.foreach_value or self.foreach_index):
            raise CommandException(
                f"Cannot provide *`foreach_index`* or *`foreach_var`* for *{self.step_name}*. "
                f"Only steps that are the start of a new for-each loop can have a *`foreach_index`* or *`foreach_var`*."
            )

        # If a task_id corresponding to the previous step is provided, we don't need to validate the ancestor tasks
        # since we can directly fetch the input artifacts from the task_id
        # Only applicable for steps that are not join steps
        if self.step_type != "join":
            # It is not a join step, so it will have at most one previous step
            prev_step_name = self.previous_steps[0]
            if prev_step_name in self.parsed_ancestor_tasks:
                task_specifier = self.parsed_ancestor_tasks[prev_step_name][0]
                task_val = self.parsed_ancestor_tasks[prev_step_name][1]
                if task_specifier == "task_id":
                    if len(self.parsed_ancestor_tasks) > 1:
                        raise CommandException(
                            f"Already provided *`task_id`* for *`{prev_step_name}`*. Remove any additional "
                            f"ancestor tasks provided as they are no-ops."
                        )
                    self._required_ancestor_tasks = [
                        REQUIRED_ANCESTORS(
                            prev_step_name,
                            self.parsed_ancestor_tasks[prev_step_name][0],
                            self.parsed_ancestor_tasks[prev_step_name][1],
                        )
                    ]
                    from metaflow import Task

                    try:
                        self._previous_steps_task = Task(
                            f"{self.flow_name}/{self.run_id}/{prev_step_name}/{task_val}"
                        )
                    except Exception as e:
                        raise CommandException(
                            f"Task with task_id *`{task_val}`* for *`{prev_step_name}`* not found for flow "
                            f"*`{self.flow_name}`* and run *`{self.run_id}`*."
                        )
                    return

        # Verify that all the required ancestor tasks are provided
        self._required_ancestor_tasks = self._verify_required_ancestor_tasks()

        # Validate that no additional tasks were provided as ancestor tasks
        for ancestor_task in self.parsed_ancestor_tasks:
            if ancestor_task not in self.foreach_stack_steps:
                raise CommandException(
                    f"*`{ancestor_task}`* is not part of the ancestor tasks for spinning *`{self.step_name}`*."
                )
