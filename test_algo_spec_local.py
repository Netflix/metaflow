"""
Test AlgoSpec as a direct callable -- no CLI, no Maestro.

Usage:
    python test_algo_spec_local.py
"""

from metaflow.algospec import AlgoSpec
from metaflow import Parameter, nflx_resources


@nflx_resources(cpu=2, memory=4096)
class SquareModel(AlgoSpec):
    """Squares a number and scales by multiplier."""

    multiplier = Parameter("multiplier", type=float, default=1.0)

    def init(self):
        print("SquareModel.init() -- multiplier=%s" % self.multiplier)

    def call(self, row: dict) -> dict:
        row["result"] = row["value"] ** 2 * self.multiplier
        return row


if __name__ == "__main__":
    model = SquareModel(use_cli=False)
    model.multiplier = 2.0
    model.init()

    rows = [{"value": 5}, {"value": 3}, {"value": 10}]
    for row in rows:
        result = model(row)
        print(result)
