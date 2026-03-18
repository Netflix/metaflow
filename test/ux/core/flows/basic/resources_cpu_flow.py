from metaflow import FlowSpec, step, resources, project


@project(name="hello_resources_cpu")
class ResourcesCpuFlow(FlowSpec):

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("ResourcesCpuFlow is starting.")
        self.next(
            self.default,
            self.cpu2,
            self.cpu4,
        )

    @resources()
    @step
    def default(self):
        self.next(self.join)

    @resources(cpu=1)
    @step
    def cpu2(self):
        self.next(self.join)

    @resources(cpu=1, memory=512)
    @step
    def cpu4(self):
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        self.message = "Metaflow says: Hi Resources CPU!"
        print("ResourcesCpuFlow is all done.")


if __name__ == "__main__":
    ResourcesCpuFlow()
