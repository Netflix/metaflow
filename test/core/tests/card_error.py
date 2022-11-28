# Todo : Write Test case on graceful error handling.
from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardErrorTest(MetaflowTest):
    """
    Test that checks if the card decorator handles Errors gracefully.
    In the checker assert that the end step finished and has artifacts after failing
    to create the card on the start step.
    """

    PRIORITY = 2

    @tag('card(type="test_error_card")')
    @steps(0, ["start"])
    def step_start(self):
        self.data = "abc"

    @steps(1, ["all"])
    def step_all(self):
        self.data = "end"

    def check_results(self, flow, checker):
        checker.assert_artifact("end", "data", "end")
