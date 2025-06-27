from metaflow_test import MetaflowTest, steps, assert_equals


class SwitchInBranchTest(MetaflowTest):
    PRIORITY = 2

    @steps(0, ["start-split"], required=True)
    def step_start(self):
        self.condition = "case1"

    @steps(0, ["switch-a"], required=True)
    def step_a(self):
        pass

    @steps(0, ["branch-b"], required=True)
    def step_b(self):
        self.data = "from_b"

    @steps(0, ["branch-c"], required=True)
    def step_c(self):
        self.data = "from_a_c"

    @steps(0, ["branch-d"], required=True)
    def step_d(self):
        self.data = "from_a_d"

    @steps(0, ["join"], required=True)
    def step_join(self, inputs):
        self.final_data = sorted([inp.data for inp in inputs])

    @steps(1, ["end"], required=True)
    def step_end(self):
        assert_equals(self.final_data, ["from_a_c", "from_b"])

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            return
        checker.assert_artifact("join", "final_data", ["from_a_c", "from_b"])
