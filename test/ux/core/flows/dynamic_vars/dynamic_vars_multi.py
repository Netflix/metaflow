import os

from metaflow import FlowSpec, step, environment, var


class DynamicVarsMultiFlow(FlowSpec):
    """Test multiple var() references on the same step."""

    @step
    def start(self):
        self.bucket = "s3://data/run-99"
        self.region = "us-west-2"
        self.next(self.compute)

    @environment(
        vars={
            "DATA_PATH": var("bucket"),
            "REGION": var("region"),
            "STATIC_KEY": "static-value",
        }
    )
    @step
    def compute(self):
        assert (
            os.environ.get("DATA_PATH") == self.bucket
        ), "DATA_PATH mismatch: %r != %r" % (os.environ.get("DATA_PATH"), self.bucket)
        assert os.environ.get("REGION") == self.region, "REGION mismatch: %r != %r" % (
            os.environ.get("REGION"),
            self.region,
        )
        assert os.environ.get("STATIC_KEY") == "static-value"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DynamicVarsMultiFlow()
