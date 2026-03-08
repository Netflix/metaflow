"""
LinearFlow — simplest end-to-end test for --with kubernetes
Validates: basic step execution, artifact passing between steps
"""
from metaflow import FlowSpec, step


class LinearFlow(FlowSpec):

    @step
    def start(self):
        """Initialize the flow."""
        self.message = "Hello from Kubernetes CI!"
        self.numbers = list(range(10))
        print(f"[start] message={self.message}")
        self.next(self.process)

    @step
    def process(self):
        """Do some computation — verify artifacts passed correctly."""
        assert self.message == "Hello from Kubernetes CI!", \
            f"Artifact mismatch: got {self.message!r}"
        self.total = sum(self.numbers)
        print(f"[process] sum={self.total}")
        self.next(self.end)

    @step
    def end(self):
        """Final assertions."""
        assert self.total == 45, f"Expected 45, got {self.total}"
        print(f"[end] LinearFlow completed ✅  total={self.total}")


if __name__ == "__main__":
    LinearFlow()