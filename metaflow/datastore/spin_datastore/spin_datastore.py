class SpinDatastore(object):
    def __init__(self, spin_parser_validator):
        self.spin_parser_validator = spin_parser_validator

    @property
    def flow_name(self):
        return self.spin_parser_validator.flow_name

    @property
    def parsed_ancestor_tasks(self):
        return self.spin_parser_validator.parsed_ancestor_tasks

    @property
    def foreach_index(self):
        return self.spin_parser_validator.foreach_index

    @property
    def foreach_var(self):
        return self.spin_parser_validator.foreach_var

    @property
    def foreach_stack(self):
        return self.spin_parser_validator.foreach_stack

    @property
    def artifacts(self):
        return self.spin_parser_validator.artifacts

    @property
    def run_id(self):
        return self.spin_parser_validator.run_id

    @property
    def step_name(self):
        return self.spin_parser_validator.step_name

    @property
    def task(self):
        return self.spin_parser_validator.task

    @property
    def step_type(self):
        return self.spin_parser_validator.step_type

    @property
    def previous_steps(self):
        return self.spin_parser_validator.previous_steps

    @property
    def required_ancestor_tasks(self):
        return self.spin_parser_validator.required_ancestor_tasks

    def previous_tasks(self, prev_step):
        # We go through all the required ancestors for the current step
        # and filter the tasks from the previous step whose foreach stack
        # entries match the required ancestors
        def _parse_foreach_stack(foreach_stack):
            return {
                entry.step: {
                    "task_val": entry.value,
                    "task_index": entry.index,
                }
                for entry in foreach_stack
            }

        def _parse_required_ancestor_tasks(required_ancestor_tasks):
            result = {}
            for required_ancestor in required_ancestor_tasks:
                step_name = required_ancestor.step_name
                task_specifier, task_val = required_ancestor
                if task_specifier == "task_id":
                    from metaflow import Task

                    task = Task(
                        f"{self.flow_name}/{self.run_id}/{step_name}/{task_val}"
                    )
                    return {
                        **_parse_foreach_stack(task["_foreach_stack"]),
                    }
                elif task_specifier == "task_index":
                    result[step_name] = {
                        "task_index": task_val,
                    }
                elif task_specifier == "task_val":
                    result[step_name] = {
                        "task_val": task_val,
                    }
                else:
                    raise ValueError("Invalid task specifier")
            return result

        def _is_ancestor(foreach_stack, required_ancestors):
            for step_name, required_ancestor in required_ancestors.items():
                if step_name not in foreach_stack:
                    return False
                if (
                    "task_val" in required_ancestor
                    and required_ancestor["task_val"]
                    != foreach_stack[step_name]["task_val"]
                ):
                    return False
                if (
                    "task_index" in required_ancestor
                    and required_ancestor["task_index"]
                    != foreach_stack[step_name]["task_index"]
                ):
                    return False
            return True

        previous_tasks = []
        for task in prev_step.tasks():
            foreach_stack = task["_foreach_stack"]
            required_ancestor_tasks_parsed = _parse_required_ancestor_tasks(
                self.required_ancestor_tasks
            )
            foreach_stack_parsed = _parse_foreach_stack(foreach_stack)
            if _is_ancestor(foreach_stack_parsed, required_ancestor_tasks_parsed):
                previous_tasks.append(task)

        if len(previous_tasks) == 0:
            raise ValueError(
                "No previous tasks found for the current step. Please check if the values provided for "
                "ancestor tasks are correct."
            )
        return previous_tasks
