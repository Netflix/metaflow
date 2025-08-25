from metaflow_test import MetaflowTest, steps, assert_equals


class RecursiveSwitchInsideForeachFlowTest(MetaflowTest):
    PRIORITY = 2
    ONLY_GRAPHS = ["recursive_switch_inside_foreach"]

    @steps(0, ["start"], required=True)
    def step_start(self):
        self.items = [
            {"id": "A", "iterations": 3},
            {"id": "B", "iterations": 5},
            {"id": "C", "iterations": 2},
        ]

    @steps(0, ["loop_start"], required=True)
    def step_start_loop_for_item(self):
        self.item_id = self.input["id"]
        self.max_loops = self.input["iterations"]
        self.item_loop_count = 0

    @steps(0, ["loop_body"], required=True)
    def step_loop_body(self):
        self.item_loop_count += 1
        self.should_continue = str(self.item_loop_count < self.max_loops)

    @steps(0, ["loop_exit"], required=True)
    def step_exit_item_loop(self):
        assert_equals(self.max_loops, self.item_loop_count)
        self.result = (
            f"Item {self.item_id} finished after {self.item_loop_count} iterations."
        )

    @steps(0, ["join-foreach"], required=True)
    def step_join(self, inputs):
        self.results = sorted([inp.result for inp in inputs])

    @steps(1, ["end"], required=True)
    def step_end(self):
        pass

    def check_results(self, flow, checker):
        expected = [
            "Item A finished after 3 iterations.",
            "Item B finished after 5 iterations.",
            "Item C finished after 2 iterations.",
        ]
        checker.assert_artifact("join", "results", expected)
