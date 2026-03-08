"""
ForeachFlow — tests foreach (dynamic parallelism) with --with kubernetes
Validates: foreach fan-out over a list, per-item processing, fan-in join
"""
from metaflow import FlowSpec, step


class ForeachFlow(FlowSpec):

    @step
    def start(self):
        """Define the items to process in parallel."""
        self.items = [1, 2, 3, 4, 5]
        print(f"[start] Processing {len(self.items)} items in parallel")
        self.next(self.process_item, foreach="items")

    @step
    def process_item(self):
        """Process each item independently (runs as separate K8s pod)."""
        self.squared = self.input ** 2
        print(f"[process_item] input={self.input}  squared={self.squared}")
        self.next(self.join)

    @step
    def join(self, inputs):
        """Collect all per-item results."""
        self.results = [inp.squared for inp in inputs]
        self.total = sum(self.results)
        print(f"[join] results={self.results}  total={self.total}")
        self.next(self.end)

    @step
    def end(self):
        # 1²+2²+3²+4²+5² = 1+4+9+16+25 = 55
        assert self.total == 55, f"Expected 55, got {self.total}"
        assert len(self.results) == 5, f"Expected 5 results, got {len(self.results)}"
        print(f"[end] ForeachFlow completed ✅  total={self.total}")


if __name__ == "__main__":
    ForeachFlow()