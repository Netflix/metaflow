from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class ResumeUBFJoinTest(MetaflowTest):
    """
    Resuming from a foreach join should work.
    Check that data changes in all downstream steps after resume.
    """

    RESUME = True
    PRIORITY = 3

    @steps(0, ["start"])
    def step_start(self):
        self.data = "start"
        self.after = False

    @steps(0, ["parallel-split"], required=True)
    def split(self):
        self.my_node_index = None

    @steps(0, ["parallel-step"], required=True)
    def inner(self):
        from metaflow import current

        assert_equals(4, current.parallel.num_nodes)
        self.my_node_index = current.parallel.node_index
        assert_equals(self.my_node_index, self.input)

    @steps(0, ["join"], required=True)
    def join(self, inputs):
        if is_resumed():
            self.data = "resume"
            got = sorted([inp.my_node_index for inp in inputs])
            assert_equals(list(range(4)), got)
            self.after = True
        else:
            self.data = "run"
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        if self.after:
            assert_equals("resume", self.data)
        else:
            assert_equals("start", self.data)

    def check_results(self, flow, checker):
        from itertools import product

        checker.assert_artifact("start", "data", "start")
        checker.assert_artifact("end", "data", "resume")
