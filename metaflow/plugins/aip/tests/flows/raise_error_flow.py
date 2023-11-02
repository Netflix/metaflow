from metaflow import FlowSpec, step


class RaiseErrorFlow(FlowSpec):
    """
    This flow is intended to "test" the Metaflow integration testing framework.
    In the past, the Metaflow integration test has incorrectly reported failing
    fails are passing within the Gitlab (and this Github UI). This flow is supposed
    to fail, and we ensure the integration test detects that.
    """

    @step
    def start(self):
        print("This step should complete successfuly!")
        self.next(self.error_step)

    @step
    def error_step(self):
        raise Exception("This exception is intended to test the integration test!")
        self.next(self.end)

    @step
    def end(self):
        print("This step should not be reachable!")


if __name__ == "__main__":
    RaiseErrorFlow()
