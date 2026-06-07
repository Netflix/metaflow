import os

from metaflow import FlowSpec, Parameter, project, step


@project(name="foreach_resume_flow")
class ForeachResumeFlow(FlowSpec):
    """Foreach flow for testing resume behavior.

    When fail_on_item >= 0, the body step fails for that specific item value,
    allowing tests to resume a partially-failed foreach execution.
    """

    fail_on_item = Parameter(
        "fail_on_item",
        help="If >= 0, fail the body step when processing this item value",
        default=-1,
        type=int,
    )

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.items = [1, 2, 3]
        self.next(self.process, foreach="items")

    @step
    def process(self):
        if self.fail_on_item >= 0 and self.input == self.fail_on_item:
            raise RuntimeError("Intentional failure for item %d" % self.input)
        self.result = self.input * 2
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted([i.result for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachResumeFlow()
