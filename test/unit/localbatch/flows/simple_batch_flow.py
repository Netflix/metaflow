from metaflow import FlowSpec, step, batch


class SimpleBatchFlow(FlowSpec):
    """
    Minimal flow used by the localbatch integration tests.
    Runs a single step on the @batch compute layer, writes a known artifact,
    and verifies it in the end step (which runs locally).
    """

    @batch(cpu=1, memory=256)
    @step
    def start(self):
        self.message = "hello from localbatch"
        self.value = 42
        self.next(self.end)

    @step
    def end(self):
        assert self.message == "hello from localbatch"
        assert self.value == 42
        print(f"SimpleBatchFlow finished: message={self.message!r}")


if __name__ == "__main__":
    SimpleBatchFlow()
