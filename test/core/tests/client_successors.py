from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ImmediateSuccessorTest(MetaflowTest):
    """
    Test that immediate_successors API returns correct successor tasks
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
        self.task_pathspec = f"{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}"

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
                cur_task_pathspec = task.data.task_pathspec
                successors = task.immediate_successors
                actual_successors_pathspecs_set = set(
                    chain.from_iterable(successors.values())
                )
                expected_successor_pathspecs_set = set()
                for successor_step_name, successor_pathspecs in successors.items():
                    # Assert that the current task is in the parent_pathspecs of the successor tasks
                    for successor_pathspec in successor_pathspecs:
                        successor_task = Task(
                            successor_pathspec, _namespace_check=False
                        )
                        print(f"Successor task: {successor_task}")
                        assert (
                            task.data.task_pathspec
                            in successor_task.data.parent_pathspecs
                        ), (
                            f"Task {task.data.task_pathspec} is not in the parent_pathspecs of the successor task "
                            f"{successor_task.data.task_pathspec}"
                        )

                    successor_step = run[successor_step_name]
                    for successor_task in successor_step:
                        if cur_task_pathspec in successor_task.data.parent_pathspecs:
                            expected_successor_pathspecs_set.add(
                                successor_task.data.task_pathspec
                            )

                # Assert that None of the tasks in the successor steps have the current task in their
                # parent_pathspecs
                assert (
                    actual_successors_pathspecs_set == expected_successor_pathspecs_set
                ), (
                    f"Expected successor pathspecs: {expected_successor_pathspecs_set}, got "
                    f"{actual_successors_pathspecs_set}"
                )
