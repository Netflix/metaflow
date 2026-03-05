import os

from metaflow import FlowSpec, step, environment, var


class DynamicVarsForeachFlow(FlowSpec):
    """Test that var(pertask=True) resolves per-split values in a foreach."""

    @step
    def start(self):
        self.cpu_list = [2, 4, 8]
        self.next(self.compute, foreach="cpu_list")

    @environment(vars={"CPU_COUNT": var("cpu_list", pertask=True)})
    @step
    def compute(self):
        expected = str(self.cpu_list[self.index])
        actual = os.environ.get("CPU_COUNT")
        assert (
            actual == expected
        ), "CPU_COUNT mismatch for split %d: expected %r, got %r" % (
            self.index,
            expected,
            actual,
        )
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DynamicVarsForeachFlow()
