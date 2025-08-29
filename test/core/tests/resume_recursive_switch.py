from metaflow_test import MetaflowTest, steps, assert_equals


class ResumeRecursiveSwitchFlowTest(MetaflowTest):
    RESUME = True
    PRIORITY = 2
    ONLY_GRAPHS = ["recursive_switch"]

    @steps(0, ["start"], required=True)
    def step_start(self):
        if not is_resumed():
            self.count = 0
            self.max_iterations = 10

    @steps(0, ["loop"], required=True)
    def step_loop(self):
        self.count += 1
        if not is_resumed() and self.count == 6:
            raise ResumeFromHere()
        self.loop_status = "continue" if self.count < self.max_iterations else "exit"

    @steps(0, ["exit"], required=True)
    def step_exit(self):
        assert_equals(10, self.count)

    @steps(1, ["end"], required=True)
    def step_end(self):
        assert_equals(10, self.count)

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is not None:
            checker.assert_artifact("end", "count", 10)

            loop_steps = run["loop_step"]
            assert_equals(10, len(list(loop_steps)))

            start_task_metadata = run["start"].task.metadata_dict
            assert (
                "origin-run-id" in start_task_metadata
            ), "The 'origin-run-id' should be present in a resumed run's metadata."

            loop_steps_by_count = {s.data.count: s for s in loop_steps}

            task_5_metadata = loop_steps_by_count[5].metadata_dict
            assert (
                "origin-task-id" in task_5_metadata
            ), "Task for iteration 5 should be a clone with an 'origin-task-id'."

            task_6_metadata = loop_steps_by_count[6].metadata_dict
            assert (
                "origin-task-id" not in task_6_metadata
            ), "Task for iteration 6 should be a new execution without an 'origin-task-id'."
