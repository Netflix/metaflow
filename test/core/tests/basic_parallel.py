from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class BasicParallelTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ["parallel-split"], required=True)
    def split(self):
        self.my_node_index = None

    @steps(0, ["parallel-step"], required=True)
    def inner(self):
        from metaflow import current

        assert_equals(4, current.parallel.num_nodes)
        self.my_node_index = current.parallel.node_index
        assert_equals(self.my_node_index, self.input)

    @steps(0, ["join"], required=True)
    def join(self, inputs):
        got = sorted([inp.my_node_index for inp in inputs])
        assert_equals(list(range(4)), got)

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
            tasks = run["parallel_inner"].tasks()
            task_list = list(tasks)
            assert_equals(4, len(task_list))
            assert_equals(1, len(list(run["parallel_inner"].control_tasks())))
