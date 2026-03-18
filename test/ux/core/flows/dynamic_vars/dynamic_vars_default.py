import os

from metaflow import FlowSpec, step, environment, var


class DynamicVarsDefaultFlow(FlowSpec):
    """Test that var(pertask=True, default=...) falls back for missing dict keys."""

    @step
    def start(self):
        self.items = ["x", "y", "z"]
        # Only keys 0 and 2 are present; split index 1 should use the default.
        self.gpu_map = {0: "4", 2: "8"}
        self.next(self.compute, foreach="items")

    @environment(vars={"GPU_COUNT": var("gpu_map", pertask=True, default="1")})
    @step
    def compute(self):
        expected = self.gpu_map.get(self.index, "1")
        actual = os.environ.get("GPU_COUNT")
        assert (
            actual == expected
        ), "GPU_COUNT mismatch for split %d: expected %r, got %r" % (
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
    DynamicVarsDefaultFlow()
