from metaflow_test import FlowDefinition, steps, tag


class BasicParallel(FlowDefinition):
    PRIORITY = 1
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

    @steps(0, ["parallel-split"], required=True)
    def split(self):
        self.my_node_index = None

    @steps(0, ["parallel-step"], required=True)
    def inner(self):
        from metaflow import current

        assert 4 == current.parallel.num_nodes
        self.my_node_index = current.parallel.node_index
        assert self.my_node_index == self.input

    @steps(0, ["join"], required=True)
    def join(self, inputs):
        got = sorted([inp.my_node_index for inp in inputs])
        assert list(range(4)) == got

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
            assert 4 == len(task_list)
            assert 1 == len(list(run["parallel_inner"].control_tasks()))
