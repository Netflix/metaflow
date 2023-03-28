from metaflow import resources
from metaflow.api import FlowSpec, step


class ResourcesFlow(FlowSpec):
    @resources(memory=1_000)
    @step
    def one(self):
        self.a = 111

    @resources(memory=2_000)
    @step
    def two(self):
        self.b = self.a * 2


class ResourcesFlow2(ResourcesFlow):
    pass
