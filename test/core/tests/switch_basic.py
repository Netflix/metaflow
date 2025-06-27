from metaflow_test import MetaflowTest, ExpectationFailed, steps, assert_equals


class BasicSwitchTest(MetaflowTest):
    """
    Tests a basic switch with multiple branches.
    """

    PRIORITY = 2

    @steps(0, ["start"], required=True)
    def step_start(self):
        self.condition = "case2"

    @steps(0, ["switch-simple"], required=True)
    def step_switch_simple(self):
        pass

    @steps(0, ["path-a"], required=True)
    def step_a(self):
        self.result = "Path A taken"

    @steps(0, ["path-b"], required=True)
    def step_b(self):
        self.result = "Path B taken"

    @steps(0, ["path-c"], required=True)
    def step_c(self):
        self.result = "Path C taken"

    @steps(1, ["end"], required=True)
    def step_end(self):
        assert_equals("Path B taken", self.result)

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            return

        checker.assert_artifact("b", "result", "Path B taken")
        if "a" in run:
            raise ExpectationFailed("Step 'a' should not have run")
        if "c" in run:
            raise ExpectationFailed("Step 'c' should not have run")
