from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardDecoratorBasicTest(MetaflowTest):
    """
    Test that checks if the card decorator stores the information as intended for a built in card
    # todo: Add code to cli_checker to create a get_card methods
    # todo: Add uuid key in an artifact and store it in card;
    # todo: Check in the checker this UUID
    """

    PRIORITY = 3

    @tag('card(type="basic")')
    @steps(
        0,
        ["start"],
    )
    def step_start(self):
        self.data = "abc"

    @tag('card(type="basic")')
    @steps(
        0,
        ["foreach-nested-inner"],
    )
    def step_foreach_inner(self):
        self.data = "bcd"

    @tag('card(type="basic")')
    @steps(
        1,
        ["join"],
    )
    def step_join(self):
        self.data = "jkl"

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        pass
