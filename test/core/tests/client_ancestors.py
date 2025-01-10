from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ImmediateAncestorTest(MetaflowTest):
    """
    Test that immediate_ancestors API returns correct parent tasks
    by comparing with parent task ids stored during execution.
    """

    PRIORITY = 1

    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.step_name = current.step_name
        self.task_pathspec = f"{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}"
        self.parent_pathspecs = set()

    @steps(1, ["join"])
    def step_join(self):
        from metaflow import current

        self.step_name = current.step_name

        # Store the parent task ids
        # Store the task pathspec for all the parent tasks
        self.parent_pathspecs = set(inp.task_pathspec for inp in inputs)

        # Set the current task id
        self.task_pathspec = f"{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}"

        print(f"Task Pathspec: {self.task_pathspec} and parent_pathspecs: {self.parent_pathspecs}")

    @steps(2, ["all"])
    def step_all(self):
        from metaflow import current

        self.step_name = current.step_name
        # Store the parent task ids
        # Task only has one parent, so we store the parent task id
        self.parent_pathspecs = set([self.task_pathspec])

        # Set the current task id
        self.task_pathspec = f"{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}"

        print(f"Task Pathspec: {self.task_pathspec} and parent_pathspecs: {self.parent_pathspecs}")


    def check_results(self, flow, checker):
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
                ancestors = task.immediate_ancestors
                print(f"Task is {task.data.task_pathspec} and ancestors are {ancestors}")
                ancestor_pathspecs = set(chain.from_iterable(ancestors.values()))

                # Compare with stored parent_task_pathspecs
                task_pathspec = task.data.task_pathspec
                assert (
                    ancestor_pathspecs == task.data.parent_pathspecs
                ), (f"Mismatch in ancestor task ids for task {task_pathspec}: Expected {task.data.parent_pathspecs}, "
                    f"got {ancestor_pathspecs}")
