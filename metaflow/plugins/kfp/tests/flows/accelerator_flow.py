import os

from metaflow import FlowSpec, step, resources, accelerator


class AcceleratorFlow(FlowSpec):
    @accelerator(type="nvidia-tesla-v100")
    @resources(
        local_storage="100",
        local_storage_limit="242",
        cpu="0.1",
        cpu_limit="0.6",
        memory="1G",
        memory_limit="2G",
    )
    @step
    def start(self):
        print("This step simulates usage of a nvidia-tesla-v100 GPU.")
        self.next(self.end)

    @step
    def end(self):
        print("All done.")


if __name__ == "__main__":
    AcceleratorFlow()
