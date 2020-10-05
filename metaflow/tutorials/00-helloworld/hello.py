from metaflow import FlowSpec, step


class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """
    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("\n\n_______________________________________")
        print("START step:\n We are now inside the START step - Hello World!\n")
        print("___________________________________________")
        self.x = 100
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print("HELLO step:\n We are now inside the HELLO step!\n")
        print("METAFLOW says Hi!")
        print("Metaflow on KFP: Reading state...\nself.x = ", self.x)
        print("___________________________________________")
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("\n\n_______________________________________")
        print("END step:\n We have reached the END step!\n")
        print("HelloFlow is all done.")
        print("___________________________________________")


if __name__ == '__main__':
    HelloFlow()
