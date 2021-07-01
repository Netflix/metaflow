# Stand-alone example test file:
# - define equivalent old- and new-style flows
# - parameterize a test to run each flow and verify its data artifacts

from metaflow import FlowSpec, Parameter
import metaflow.api as ma
from metaflow.api import foreach, join, step
from metaflow.tests.utils import parametrize, run


class OldSumSquares(FlowSpec):
    num = Parameter("num", required=True, type=int, default=4)

    @step
    def start(self):
        self.nums = list(range(1, self.num + 1))
        self.next(self.square, foreach="nums")

    @step
    def square(self):
        self.num2 = self.input**2
        self.next(self.sum)

    @step
    def sum(self, inputs):
        self.sum2 = sum(input.num2 for input in inputs)
        self.next(self.end)

    @step
    def end(self):
        print("Sum of squares up to %d: %d" % (int(self.num), int(self.sum2)))


class NewSumSquares(ma.FlowSpec):
    num = Parameter("num", required=True, type=int, default=4)

    @step
    def start(self):
        self.nums = list(range(1, self.num + 1))

    @foreach("nums")
    def square(self, num):
        self.num2 = num**2

    @join
    def sum(self, inputs):
        self.sum2 = sum(input.num2 for input in inputs)

    @step
    def end(self):
        print("Sum of squares up to %d: %d" % (int(self.num), int(self.sum2)))


@parametrize(
    "flow",
    [
        OldSumSquares,
        NewSumSquares,
    ],
)
def test_simple_foreach(flow):
    data = run(flow)
    assert data == {
        "num": 4,
        "sum2": 30,
    }
