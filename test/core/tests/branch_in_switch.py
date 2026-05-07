from metaflow_test import FlowDefinition, steps


class BranchInSwitch(FlowDefinition):
    PRIORITY = 2
    ONLY_GRAPHS = ["branch_in_switch"]

    @steps(0, ["start-branch-in-switch"], required=True)
    def step_start(self):
        self.mode = "process"

    @steps(0, ["process-path"], required=True)
    def step_process(self):
        pass

    @steps(0, ["p1"], required=True)
    def step_p1(self):
        self.result = "p1_done"

    @steps(0, ["p2"], required=True)
    def step_p2(self):
        self.result = "p2_done"

    @steps(0, ["process-join"], required=True)
    def step_join(self, inputs):
        self.final_data = sorted([inp.result for inp in inputs])
        self.final_result = "Processed"

    @steps(0, ["skip-path"], required=True)
    def step_skip(self):
        self.final_result = "Skipped"

    @steps(1, ["end-branch-in-switch"], required=True)
    def step_end(self):
        assert self.final_data == ["p1_done", "p2_done"]
        assert self.final_result == "Processed"

    def check_results(self, flow, checker):
        checker.assert_artifact("end", "final_result", "Processed")
