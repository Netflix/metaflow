"""
Test AlgoSpec -- minimal example for deployment validation.

Computes the square of an input number. Validates:
- AlgoSpec graph construction (single "call" node)
- Parameter binding
- @nflx_resources as a flow decorator
- Maestro deployment (init + call lifecycle)

Usage:
    python test_algo_spec.py show
    python test_algo_spec.py check
    python test_algo_spec.py maestro create --name square-model-test --cluster test --only-json
"""

from metaflow import Parameter
from metaflow.algospec import AlgoSpec
from metaflow import nflx_resources


@nflx_resources(cpu=2, memory=4096)
class SquareModel(AlgoSpec):
    """Squares a number. Minimal AlgoSpec for deployment testing."""

    multiplier = Parameter(
        "multiplier",
        type=float,
        default=1.0,
        help="Scale factor applied after squaring.",
    )

    def init(self):
        """Called once -- nothing to load for this test model."""
        print("SquareModel.init() -- multiplier=%s" % self.multiplier)

    def call(self, row: dict) -> dict:
        """Square the input value and scale by multiplier."""
        row["result"] = row["value"] ** 2 * self.multiplier
        return row


if __name__ == "__main__":
    SquareModel()
