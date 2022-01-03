from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class BasicUnboundedForeachTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ["foreach-split-small"], required=True)
    def split(self):
        self.my_index = None
        from metaflow.plugins import InternalTestUnboundedForeachInput

        self.arr = InternalTestUnboundedForeachInput(range(2))

    @tag("unbounded_test_foreach_internal")
    @steps(0, ["foreach-inner-small"], required=True)
    def inner(self):
        # index must stay constant over multiple steps inside foreach
        if self.my_index is None:
            self.my_index = self.index
        assert_equals(self.my_index, self.index)
        assert_equals(self.input, self.arr[self.index])
        self.my_input = self.input

    @steps(0, ["foreach-join-small"], required=True)
    def join(self, inputs):
        got = sorted([inp.my_input for inp in inputs])
        assert_equals(list(range(2)), got)

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if type(checker).__name__ == "CliCheck":
            # CliCheck doesn't support enlisting of tasks.
            assert run is None
        else:
            assert run is not None
            tasks = run["foreach_inner"].tasks()
            task_list = list(tasks)
            assert_equals(3, len(task_list))
            assert_equals(1, len(list(run["foreach_inner"].control_tasks())))
