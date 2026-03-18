import os

from metaflow import FlowSpec, step, environment, var


class DynamicVarsDictFlow(FlowSpec):
    """Test that var(pertask=True) with a dict artifact uses split index as key."""

    @step
    def start(self):
        # Dict keyed by split index; foreach splits over self.items
        self.items = ["a", "b", "c"]
        self.label_map = {0: "label-a", 1: "label-b", 2: "label-c"}
        self.next(self.compute, foreach="items")

    @environment(vars={"LABEL": var("label_map", pertask=True)})
    @step
    def compute(self):
        expected = self.label_map[self.index]
        actual = os.environ.get("LABEL")
        assert (
            actual == expected
        ), "LABEL mismatch for split %d: expected %r, got %r" % (
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
    DynamicVarsDictFlow()
