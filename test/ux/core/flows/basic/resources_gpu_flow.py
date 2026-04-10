from metaflow import FlowSpec, step, resources, project


@project(name="hello_resources_gpu")
class ResourcesGpuFlow(FlowSpec):

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("ResourcesGpuFlow is starting.")
        self.next(self.gpu_step)

    @resources(gpu=1)
    @step
    def gpu_step(self):
        # No actual GPU validation — devstack has no GPU nodes.
        # This test verifies the @resources(gpu=N) decorator compiles
        # and deploys successfully on each scheduler backend.
        self.gpu_requested = True
        self.next(self.end)

    @step
    def end(self):
        self.message = "Metaflow says: Hi Resources GPU!"
        print("ResourcesGpuFlow is all done.")


if __name__ == "__main__":
    ResourcesGpuFlow()
