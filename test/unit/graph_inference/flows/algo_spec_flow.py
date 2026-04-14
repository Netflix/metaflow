from metaflow import Parameter
from metaflow.algospec import AlgoSpec


class SquareModel(AlgoSpec):
    """AlgoSpec test — step name should be 'squaremodel'."""

    multiplier = Parameter("multiplier", type=float, default=1.0)

    def init(self):
        pass

    def call(self):
        self.result = 5**2 * self.multiplier


if __name__ == "__main__":
    SquareModel()
