"""Basic foreach: split, inner, join — index/input/result invariants."""

from metaflow import FlowSpec, step


_ARR = [
    26, 5, 10, 15, 25, 11, 22, 6, 19, 12, 16, 9, 28, 14, 24, 20,
    30, 1, 13, 18, 2, 17, 21, 3, 29, 4, 27, 31, 8, 23, 0, 7,
]


class BasicForeachFlow(FlowSpec):
    @step
    def start(self):
        self.arr = _ARR
        self.next(self.process, foreach="arr")

    @step
    def process(self):
        # Index must stay constant; input must equal arr[index].
        assert self.input == self.arr[self.index]
        self.my_input = self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        got = [inp.my_input for inp in inputs]
        assert got == _ARR
        self.next(self.end)

    @step
    def end(self):
        pass


def test_basic_foreach(metaflow_runner, executor):
    result = metaflow_runner(BasicForeachFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    # 32 task instances in the foreach branch.
    assert len(list(run["process"])) == len(_ARR)
