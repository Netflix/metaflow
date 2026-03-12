from metaflow_test import MetaflowTest, ExpectationFailed, steps


class NonMonotonicForeachTest(MetaflowTest):
    """
    Regression test for foreach join ordering with non-monotonic values.

    Uses >4 branches to trigger the TaskDataStoreSet path, and non-monotonic
    values so that any ordering bug (e.g., partial sort) is immediately visible
    via inputs[i].index == i.
    """

    PRIORITY = 0
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

    @steps(0, ["foreach-split"], required=True)
    def split(self):
        self.arr = [5, 3, 1, 4, 2, 6]

    @steps(0, ["foreach-inner"], required=True)
    def inner(self):
        assert_equals(self.input, self.arr[self.index])
        self.my_input = self.input

    @steps(0, ["foreach-join"], required=True)
    def join(self, inputs):
        # Verify inputs arrive in original foreach order (by index), not sorted
        for i, inp in enumerate(inputs):
            assert_equals(i, inp.index)
        got = [inp.my_input for inp in inputs]
        assert_equals([5, 3, 1, 4, 2, 6], got)

    @steps(1, ["all"])
    def step_all(self):
        pass
