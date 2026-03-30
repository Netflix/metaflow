from metaflow import FlowSpec, step


class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """

    # Each @step defines a stage in the workflow.
    # Metaflow executes steps based on transitions defined using self.next().

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloFlow is starting.")

        # self.next() defines which step runs next.
        # Here, after 'start', Metaflow will execute the 'hello' step.
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        print("Metaflow says: Hi!")

        # After 'hello', execution moves to the 'end' step.
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloFlow()