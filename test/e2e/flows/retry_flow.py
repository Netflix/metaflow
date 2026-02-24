from metaflow import FlowSpec, step, retry, Parameter
import os


class RetryFlow(FlowSpec):
    @step
    def start(self):
        self.attempt = 0
        self.next(self.fail_step)

    @retry(times=2)
    @step
    def fail_step(self):
        dummy_file = "/tmp/retry_test_attempt"
        if not os.path.exists(dummy_file):
            open(dummy_file, "w").close()
            raise Exception("Intentional failure on first attempt")
        self.next(self.end)

    @step
    def end(self):
        self.message = "Retry flow successful"
        print("Retry flow successful")


if __name__ == "__main__":
    RetryFlow()
