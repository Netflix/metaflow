from metaflow import FlowSpec, step


class RearrangedLinearFlow(FlowSpec):
    """
    A longer linear flow where Metaflow prints more than just "Hi".

    We define this to test and demo the construction of the KFP pipeline for simple linear flows with no state sharing.
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
        self.next(self.greeting)

    @step
    def greeting(self):
        """
        An additional step where we add some more greetings to have a longer DAG.

        """
        print("\n\n_______________________________________")
        print("GREETING step:\n")
        print("Greetings :D Metaflow is excited to see itself running on KFP! This is truly a dream come true :) ")
        print("___________________________________________")
        self.next(self.hello)


    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print("HELLO step:\n We are now inside the HELLO step!\n")
        print("METAFLOW says Hi!")
        print("___________________________________________")
        self.next(self.friendly_intro)

    @step
    def friendly_intro(self):
        """
        Another step to add a friendly intro!

        """
        print("\n\n_______________________________________")
        print("Friendly intro step:\n")
        print("Hello folks! I'm Metaflow. I was developed and open sourced by Netflix. It's truly an honour to be here. Looking forward to " \
              "bringing my strengths to the AIP platform!")
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
        print("That's all folks! See you soon")
        print("___________________________________________")

if __name__ == '__main__':
    RearrangedLinearFlow()
