from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ChildrenTest(MetaflowTest):
    """
    Test that child_tasks API returns correct child tasks
    by comparing with parent task ids stored during execution.
    """

    PRIORITY = 1

    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.step_name = current.step_name
        self.task_pathspec = current.pathspec
        self.parent_pathspecs = set()

    @steps(1, ["join"])
    def step_join(self):
        from metaflow import current

        self.step_name = current.step_name

        # Store the parent task ids
        # Store the task pathspec for all the parent tasks
        self.parent_pathspecs = set(inp.task_pathspec for inp in inputs)

        # Set the current task id
        self.task_pathspec = current.pathspec

        print(
            f"Task Pathspec: {self.task_pathspec} and parent_pathspecs: {self.parent_pathspecs}"
        )

    @steps(2, ["all"])
    def step_all(self):
        from metaflow import current

        self.step_name = current.step_name
        # Store the parent task ids
        # Task only has one parent, so we store the parent task id
        self.parent_pathspecs = set([self.task_pathspec])

        # Set the current task id
        self.task_pathspec = current.pathspec

        print(
            f"Task Pathspec: {self.task_pathspec} and parent_pathspecs: {self.parent_pathspecs}"
        )

    def check_results(self, flow, checker):
        from metaflow import Task
        from itertools import chain

        run = checker.get_run()

        if run is None:
            print("Run is None")
            # very basic sanity check for CLI checker
            for step in flow:
                checker.assert_artifact(step.name, "step_name", step.name)
            return

        # For each step in the flow
        for step in run:
            # For each task in the step
            for task in step:
                cur_task_pathspec = task.pathspec
                child_tasks = task.child_tasks
                actual_children_pathspecs_set = set(
                    [task.pathspec for task in child_tasks]
                )
                expected_children_pathspecs_set = set()

                # Get successor steps for the current task
                child_steps = task.parent.child_steps

                for child_task in child_tasks:
                    assert task.pathspec in child_task.data.parent_pathspecs, (
                        f"Task {task.pathspec} is not in the `parent_pathspecs` of the successor task "
                        f"{child_task.pathspec}"
                    )

                for step in child_steps:
                    child_step = run[step]
                    for child_task in child_step:
                        if cur_task_pathspec in child_task.data.parent_pathspecs:
                            expected_children_pathspecs_set.add(child_task.pathspec)

                # Assert that None of the tasks in the successor steps have the current task in their
                # parent_pathspecs
                assert (
                    actual_children_pathspecs_set == expected_children_pathspecs_set
                ), (
                    f"Expected children pathspecs: {expected_children_pathspecs_set}, got "
                    f"{actual_children_pathspecs_set}"
                )
