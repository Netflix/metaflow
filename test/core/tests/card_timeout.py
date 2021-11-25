from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardTimeoutTest(MetaflowTest):
    """
    Test that checks if the card decorator works as intended with the timeout decorator.
    # todo: Set timeout in the card arguement
    # todo: timeout decorator doesn't timeout for cards. We use the arguement in the card_decorator.
    """

    PRIORITY = 2

    @tag('card(type="timeout_card",timeout=30,options=dict(timeout=50))')
    @steps(0, ["start"])
    def step_start(self):
        self.data = "abc"

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        print(flow)
