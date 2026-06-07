from metaflow import FlowSpec, step, retry, current, project


@project(name="retry_flow")
class RetryFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.flaky)

    @retry(times=2)
    @step
    def flaky(self):
        if current.retry_count < 1:
            raise RuntimeError(
                "Intentional failure on attempt %d" % current.retry_count
            )
        self.attempts = current.retry_count
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    RetryFlow()
