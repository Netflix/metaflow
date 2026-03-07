import os

from metaflow import FlowSpec, step, project


@project(name="hello_world")
class HelloFlow(FlowSpec):
    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("HelloFlow is starting.")
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.next(self.hello)

    @step
    def hello(self):
        self.message = "Metaflow says: Hi!"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloFlow()
