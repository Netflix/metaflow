from metaflow_test import MetaflowTest, steps, assert_equals


class SwitchInForeachTest(MetaflowTest):
    PRIORITY = 2

    @steps(0, ["start-foreach"], required=True)
    def step_start(self):
        self.items = [
            {"id": 1, "type": "type_a", "data": 100},
            {"id": 2, "type": "type_b", "data": 200},
            {"id": 3, "type": "type_a", "data": 300},
        ]

    @steps(0, ["process-item"], required=True)
    def step_process_item(self):
        self.item_type = self.input["type"]

    @steps(0, ["handle-a"], required=True)
    def step_handle_a(self):
        self.result = f"A({self.input['data'] * 2})"

    @steps(0, ["handle-b"], required=True)
    def step_handle_b(self):
        self.result = f"B({self.input['data'] / 2.0})"

    @steps(0, ["join-foreach"], required=True)
    def step_join_foreach(self, inputs):
        self.results = sorted([inp.result for inp in inputs])

    @steps(1, ["end"], required=True)
    def step_end(self):
        assert_equals(self.results, ["A(200)", "A(600)", "B(100.0)"])

    def check_results(self, flow, checker):
        checker.assert_artifact("join", "results", ["A(200)", "A(600)", "B(100.0)"])
