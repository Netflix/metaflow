import time
from metaflow import FlowSpec, step, Config, timeout


class TimeoutConfigFlow(FlowSpec):
    config = Config("config", default="myconfig.json")

    @timeout(seconds=config.timeout)
    @step
    def start(self):
        print(f"timing out after {self.config.timeout} seconds")
        time.sleep(5)
        print("success")
        self.next(self.end)

    @step
    def end(self):
        print("full config", self.config)


if __name__ == "__main__":
    TimeoutConfigFlow()
