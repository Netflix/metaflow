from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ForeachJoinOrderTest(MetaflowTest):
    """
    Tests that inputs at a foreach join step arrive in the same order as the
    original foreach list, not in task-completion or storage-retrieval order.

    Uses >4 elements to exercise the TaskDataStoreSet code path, which was
    subject to non-deterministic ordering due to set iteration in
    get_task_datastores(). The list is intentionally non-monotonic so that
    ordering bugs are immediately visible.
    """

    PRIORITY = 1
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
        # Non-monotonic list with >4 elements to trigger the TaskDataStoreSet
        # code path (threshold is len > 4).
        self.arr = [5, 3, 1, 4, 2, 6]

    @steps(0, ["foreach-inner"], required=True)
    def inner(self):
        self.my_input = self.input

    @steps(0, ["foreach-join"], required=True)
    def join(self, inputs):
        # inputs[i].index must equal i: inputs must be in foreach-index order.
        indices = [inp.index for inp in inputs]
        assert_equals(list(range(len(indices))), indices)
        # my_input values must match the original foreach array positionally.
        got = [inp.my_input for inp in inputs]
        assert_equals(list(inputs[0].arr), got)

    @steps(1, ["all"])
    def step_all(self):
        pass
