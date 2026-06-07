import os

from metaflow import FlowSpec, current, project, retry, step


@project(name="retry_foreach_flow")
class RetryForeachFlow(FlowSpec):
    """Foreach where the body step has @retry and fails on attempt 0.

    Exercises the interaction between Prefect retries and Metaflow's
    datastore: after a retry, the datastore must be read from the correct
    attempt index (not hardcoded attempt=0).
    """

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.items = [1, 2]
        self.next(self.process, foreach="items")

    @retry(times=1)
    @step
    def process(self):
        if current.retry_count < 1:
            raise RuntimeError(
                "Intentional failure on attempt %d" % current.retry_count
            )
        self.result = self.input * 10
        self.attempts = current.retry_count
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted([i.result for i in inputs])
        self.all_succeeded_on_retry = all(i.attempts == 1 for i in inputs)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    RetryForeachFlow()
