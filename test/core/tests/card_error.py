# Todo : Write Test case on graceful error handling.
from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardErrorTest(MetaflowTest):
    """
    Test that checks if the card decorator handles Errors gracefully.
    """

    PRIORITY = 2

    @tag('card(type="error_card")')
    @steps(0, ["start"])
    def step_start(self):
        self.data = "abc"

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        pass
