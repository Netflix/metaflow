"""Flow for testing @environment decorator on a foreach body step."""

import os

from metaflow import FlowSpec, environment, project, step


@project(name="env_foreach_flow")
class EnvForeachFlow(FlowSpec):
    """@environment vars injected on a foreach body step.

    Verifies that env vars from @environment are correctly propagated to
    foreach body steps — which require their command templates to be
    constructed differently from linear steps.
    """

    @step
    def start(self):
        self.items = [1, 2]
        self.next(self.process, foreach="items")

    @environment(vars={"TEST_FOREACH_VAR": "injected"})
    @step
    def process(self):
        self.env_val = os.environ.get("TEST_FOREACH_VAR", "")
        self.result = self.input * 2
        self.next(self.join)

    @step
    def join(self, inputs):
        self.env_vals = [i.env_val for i in inputs]
        self.results = sorted([i.result for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    EnvForeachFlow()
