from metaflow_test import MetaflowTest, steps, assert_equals


class ResumeRecursiveSwitchInsideForeachFlowTest(MetaflowTest):
    RESUME = True
    PRIORITY = 2
    ONLY_GRAPHS = ["recursive_switch_inside_foreach"]

    @steps(0, ["start"], required=True)
    def step_start(self):
        if not is_resumed():
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

        if not is_resumed() and self.item_id == "B" and self.item_loop_count == 3:
            raise ResumeFromHere()

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
        run = checker.get_run()
        if run is not None:
            expected = [
                "Item A finished after 3 iterations.",
                "Item B finished after 5 iterations.",
                "Item C finished after 2 iterations.",
            ]
            checker.assert_artifact("join", "results", expected)

            exit_steps = run["exit_item_loop"]
            exit_steps_by_id = {s.data.item_id: s for s in exit_steps}
            assert_equals(3, len(list(exit_steps)))

            # Branches 'A' and 'C' succeeded in the first run, so their exit steps
            # should be clones in the resumed run, identified by 'origin-task-id'.
            assert "origin-task-id" in exit_steps_by_id["A"].metadata_dict
            assert "origin-task-id" in exit_steps_by_id["C"].metadata_dict

            # Branch 'B' failed and was re-executed from the start of the branch.
            # Its exit step is a new task and should NOT have an 'origin-task-id'.
            assert "origin-task-id" not in exit_steps_by_id["B"].metadata_dict
