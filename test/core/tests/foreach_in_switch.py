from metaflow_test import MetaflowTest, ExpectationFailed, steps, assert_equals


class ForeachInSwitchTest(MetaflowTest):
    PRIORITY = 2

    @steps(0, ["start-foreach-in-switch"], required=True)
    def step_start(self):
        self.mode = "process"

    @steps(0, ["process-items"], required=True)
    def step_process(self):
        self.items_to_process = ["item_1", "item_2"]

    @steps(0, ["do-work"], required=True)
    def step_do_work(self):
        self.work_result = f"Processed {self.input}"

    @steps(0, ["join-work"], required=True)
    def step_join_work(self, inputs):
        self.final_result = sorted([inp.work_result for inp in inputs])

    @steps(0, ["skip-processing"], required=True)
    def step_skip(self):
        self.final_result = "Skipped"

    @steps(1, ["end-foreach-in-switch"], required=True)
    def step_end(self):
        assert_equals(self.final_result, ["Processed item_1", "Processed item_2"])

    def check_results(self, flow, checker):
        checker.assert_artifact(
            "end", "final_result", ["Processed item_1", "Processed item_2"]
        )
