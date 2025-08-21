from metaflow_test import MetaflowTest, ExpectationFailed, steps


class WideForeachTest(MetaflowTest):
    PRIORITY = 3
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
    ]

    @steps(0, ["foreach-split-small"], required=True)
    def split(self):
        self.my_index = None
        self.arr = range(1200)

    @steps(0, ["foreach-inner-small"], required=True)
    def inner(self):
        self.my_input = self.input

    @steps(0, ["foreach-join-small"], required=True)
    def join(self, inputs):
        got = sorted([inp.my_input for inp in inputs])
        assert_equals(list(range(1200)), got)

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run:
            # The client API shouldn't choke on many tasks
            res = sorted(task.data.my_input for task in run["foreach_inner"])
            assert_equals(list(range(1200)), res)
