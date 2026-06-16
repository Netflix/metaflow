"""Flow with a parameter named 'retry_count' to test parameter passing correctness.

When deployed to Mage, parameters were silently dropped when trigger variables
had None values or when JSON serialization lost the value. This flow verifies
that parameter values arrive correctly at task runtime.
"""

from metaflow import FlowSpec, Parameter, step, project


@project(name="reserved_param_flow")
class ReservedParamFlow(FlowSpec):
    retry_count = Parameter(
        "retry_count",
        default=0,
        type=int,
        help="Number of retries (named retry_count to test parameter passing)",
    )

    @step
    def start(self):
        self.stored_retry_count = self.retry_count
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ReservedParamFlow()
