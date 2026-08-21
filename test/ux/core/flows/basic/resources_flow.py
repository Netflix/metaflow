from metaflow import FlowSpec, step, resources, project


@project(name="resources_flow")
class ResourcesFlow(FlowSpec):
    """Verify @resources propagates without breaking execution.

    Runs two parallel branches with different CPU/memory specs and joins them.
    Locally, @resources is a no-op; on Batch/Kubernetes it sets container limits.
    """

    @step
    def start(self):
        self.next(self.small, self.medium)

    @resources(cpu=1, memory=256)
    @step
    def small(self):
        self.label = "small"
        self.next(self.join)

    @resources(cpu=2, memory=512)
    @step
    def medium(self):
        self.label = "medium"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.labels = sorted([i.label for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ResourcesFlow()
