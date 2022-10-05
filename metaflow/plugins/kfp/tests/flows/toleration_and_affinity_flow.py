from metaflow import FlowSpec, accelerator, resources, step


class TolerationAndAffinityFlow(FlowSpec):
    @accelerator(type="nvidia-tesla-v100")
    @resources(
        cpu="0.6",
        memory="2G",
    )
    @step
    def start(self):
        print("This step simulates usage of a nvidia-tesla-v100 GPU.")
        self.next(self.small_default_pod)

    # For node type toleration test
    # We expect pods using >= 50% of host resource (16vCPU or 32G memory) to use larger nodes
    # Pods having strictly less than 8 vCPU or 16G memory are considered small pods

    @step
    def small_default_pod(self):
        """Expect no taint for small node type"""
        self.next(self.small_cpu_pod)

    @resources(cpu=4)
    @step
    def small_cpu_pod(self):
        """Expect no taint for small node type"""
        self.next(self.small_memory_pod)

    @resources(memory="8000Mi")
    @step
    def small_memory_pod(self):
        """Expect taint for small node type"""
        self.next(self.large_cpu_pod)

    @resources(cpu=16.0, memory=5)
    @step
    def large_cpu_pod(self):
        """Expect taint for large node type"""
        self.next(self.large_memory_pod)

    # 32000MB ~= 32GB memory - testing different resource request formats
    @resources(memory=32000, cpu=1)
    @step
    def large_memory_pod(self):
        """Expect taint for large node type"""
        self.next(self.large_memory_cpu_pod)

    # 300 Gi, 50.5 CPU - testing different resource request formats
    @resources(memory="300Gi", cpu="50500m")
    @step
    def large_memory_cpu_pod(self):
        """Expect taint for large node type"""
        self.next(self.end)

    @step
    def end(self):
        print("All done.")


if __name__ == "__main__":
    TolerationAndAffinityFlow()
