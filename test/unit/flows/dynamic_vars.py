from metaflow import FlowSpec, step, environment, var, kubernetes


class DynamicEnvTestFlow(FlowSpec):
    @step
    def start(self):
        self.my_bucket = "s3://my-data/run-42"
        self.num_cpu = 4
        self.next(self.compute)

    @environment(vars={"DATA_PATH": var("my_bucket"), "MODE": "production"})
    # @kubernetes(cpu=var("num_cpu"))
    @step
    def compute(self):
        import os

        print("DATA_PATH =", os.environ.get("DATA_PATH"))
        print("MODE =", os.environ.get("MODE"))
        assert os.environ.get("DATA_PATH") == self.my_bucket
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DynamicEnvTestFlow()
