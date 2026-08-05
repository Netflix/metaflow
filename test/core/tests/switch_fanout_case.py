from metaflow_test import MetaflowTest, steps, assert_equals


class SwitchFanoutHitTest(MetaflowTest):
    """
    Tests a switch where the selected case is a single-target ('hit' -> finalize).
    """

    PRIORITY = 2
    ONLY_GRAPHS = ["switch_fanout"]

    @steps(0, ["start"], required=True)
    def step_start(self):
        self.condition = "hit"

    @steps(0, ["switch-simple"], required=True)
    def step_switch(self):
        pass

    @steps(0, ["path-hit"], required=True)
    def step_finalize(self):
        self.result = "hit"

    @steps(0, ["path-clip"], required=True)
    def step_clip(self):
        self.result = "clip"

    @steps(0, ["path-face"], required=True)
    def step_face(self):
        self.result = "face"

    @steps(0, ["fanout-join"], required=True)
    def step_join_miss(self, inputs):
        self.result = ",".join(sorted(inp.result for inp in inputs))

    @steps(1, ["end"], required=True)
    def step_end(self):
        assert_equals("hit", self.result)

    def check_results(self, flow, checker):
        checker.assert_artifact("finalize", "result", "hit")


class SwitchFanoutMissTest(MetaflowTest):
    """
    Tests a switch where the selected case is a list-valued fanout ('miss' -> [clip, face]).
    """

    PRIORITY = 2
    ONLY_GRAPHS = ["switch_fanout"]

    @steps(0, ["start"], required=True)
    def step_start(self):
        self.condition = "miss"

    @steps(0, ["switch-simple"], required=True)
    def step_switch(self):
        pass

    @steps(0, ["path-hit"], required=True)
    def step_finalize(self):
        self.result = "hit"

    @steps(0, ["path-clip"], required=True)
    def step_clip(self):
        self.result = "clip"

    @steps(0, ["path-face"], required=True)
    def step_face(self):
        self.result = "face"

    @steps(0, ["fanout-join"], required=True)
    def step_join_miss(self, inputs):
        self.result = ",".join(sorted(inp.result for inp in inputs))

    @steps(1, ["end"], required=True)
    def step_end(self):
        assert_equals("clip,face", self.result)

    def check_results(self, flow, checker):
        checker.assert_artifact("join_miss", "result", "clip,face")
