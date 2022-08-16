
from metaflow import FlowSpec, step


class FailureFlow(FlowSpec):
    """
    This flow fails.
    
    Use this flow to test flow-triggering-flow function's ability to respond to failing flow.

    """
    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("FailureFlow is starting.")
        self.next(self.hello)

    @step
    def hello(self):
        print("hey")
        self.next(self.failure_step)

    @step
    def failure_step(self):
        print("this should fail")
        x = 2
        if x > 0:
            raise Exception("Failure flow failed!")
        self.next(self.end)
    
    
    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    FailureFlow()
