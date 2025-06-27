from metaflow_test import MetaflowTest, ExpectationFailed, steps, assert_equals


class NestedSwitchTest(MetaflowTest):
    """
    Tests a switch that leads to another switch.
    """

    PRIORITY = 2

    @steps(0, ["start-nested"], required=True)
    def step_start(self):
        self.condition1 = "case1"
        self.condition2 = "case2_2"

    @steps(0, ["switch-nested"], required=True)
    def step_switch2(self):
        pass

    @steps(0, ["path-b"], required=True)
    def step_b(self):
        self.result = "Direct path B"

    @steps(0, ["path-c-nested"], required=True)
    def step_c(self):
        self.result = "Nested path C"

    @steps(0, ["path-d-nested"], required=True)
    def step_d(self):
        self.result = "Nested path D"

    @steps(1, ["end-nested"], required=True)
    def step_end(self):
        assert_equals("Nested path D", self.result)

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            return

        checker.assert_artifact("d", "result", "Nested path D")
        if "b" in run:
            raise ExpectationFailed("Step 'b' should not have run")
        if "c" in run:
            raise ExpectationFailed("Step 'c' should not have run")
