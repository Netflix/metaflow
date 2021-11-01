from metaflow_test import MetaflowTest, ExpectationFailed, steps


class BasicForeachTest(MetaflowTest):
    PRIORITY = 0

    @steps(0, ["foreach-split"], required=True)
    def split(self):
        self.my_index = None
        self.arr = range(32)

    @steps(0, ["foreach-inner"], required=True)
    def inner(self):
        # index must stay constant over multiple steps inside foreach
        if self.my_index is None:
            self.my_index = self.index
        assert_equals(self.my_index, self.index)
        assert_equals(self.input, self.arr[self.index])
        self.my_input = self.input

    @steps(0, ["foreach-join"], required=True)
    def join(self, inputs):
        got = sorted([inp.my_input for inp in inputs])
        assert_equals(list(range(32)), got)

    @steps(1, ["all"])
    def step_all(self):
        pass
