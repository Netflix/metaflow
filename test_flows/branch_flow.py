"""
BranchFlow — tests parallel branch execution with --with kubernetes
Validates: fan-out, fan-in, artifact merging across branches
"""
from metaflow import FlowSpec, step


class BranchFlow(FlowSpec):

    @step
    def start(self):
        """Kick off two parallel branches."""
        self.input_value = 100
        print(f"[start] Splitting into branch_a and branch_b")
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        """Branch A: multiply."""
        self.result = self.input_value * 2
        print(f"[branch_a] result={self.result}")
        self.next(self.join)

    @step
    def branch_b(self):
        """Branch B: add."""
        self.result = self.input_value + 50
        print(f"[branch_b] result={self.result}")
        self.next(self.join)

    @step
    def join(self, inputs):
        """Merge results from both branches."""
        results = [inp.result for inp in inputs]
        self.merged = sum(results)
        print(f"[join] branch results={results}  merged={self.merged}")
        assert len(results) == 2, "Expected exactly 2 branch results"
        self.next(self.end)

    @step
    def end(self):
        # branch_a = 200, branch_b = 150 → merged = 350
        assert self.merged == 350, f"Expected 350, got {self.merged}"
        print(f"[end] BranchFlow completed ✅  merged={self.merged}")


if __name__ == "__main__":
    BranchFlow()