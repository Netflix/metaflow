from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardImportTest(MetaflowTest):
    """
    Test that checks if the card decorator imports custom modules as intended
    """

    PRIORITY = 2

    @tag('card(type="mock_card")')
    @steps(
        0,
        ["start"],
    )
    def step_start(self):
        self.data = "abc"

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        pass
