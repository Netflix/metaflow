from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class MultipleCardDecoratorTest(MetaflowTest):
    """
    Test that checks if the multiple card decorators work with @step code.
    - This test adds multiple `test_pathspec_card` cards to a @step
    - Each card will contain taskpathspec
    - CLI Check:
        - List cards and cli will assert multiple cards are present per taskspec using `CliCheck.list_cards`
        - Assert the information about the card using the hash and check if taskspec is present in the data
    - Metadata Check
        - List cards and cli will assert multiple cards are present per taskspec using `MetadataCheck.list_cards`
        - Assert the information about the card using the hash and check if taskspec is present in the data
    """

    PRIORITY = 3

    @tag('card(type="test_pathspec_card")')
    @tag('card(type="test_pathspec_card")')
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.task = current.pathspec

    @tag('card(type="test_pathspec_card")')
    @tag('card(type="test_pathspec_card")')
    @steps(0, ["foreach-nested-inner"])
    def step_foreach_inner(self):
        from metaflow import current

        self.task = current.pathspec

    @tag('card(type="test_pathspec_card")')
    @tag('card(type="test_pathspec_card")')
    @steps(1, ["join"])
    def step_join(self):
        from metaflow import current

        self.task = current.pathspec

    @tag('card(type="test_pathspec_card")')
    @tag('card(type="test_pathspec_card")')
    @steps(1, ["all"])
    def step_all(self):
        from metaflow import current

        self.task = current.pathspec

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # This means CliCheck is in context.
            for step in flow:
                cli_check_dict = checker.artifact_dict(step.name, "task")
                for task_pathspec in cli_check_dict:
                    full_pathspec = "/".join([flow.name, task_pathspec])
                    task_id = task_pathspec.split("/")[-1]
                    cards_info = checker.list_cards(step.name, task_id)
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 2,
                        True,
                    )
                    for card in cards_info["cards"]:
                        checker.assert_card(
                            step.name,
                            task_id,
                            "test_pathspec_card",
                            "%s" % full_pathspec,
                            card_hash=card["hash"],
                            exact_match=False,
                        )
        else:
            # This means MetadataCheck is in context.
            for step in flow:
                meta_check_dict = checker.artifact_dict(step.name, "task")
                for task_id in meta_check_dict:
                    full_pathspec = meta_check_dict[task_id]["task"]
                    cards_info = checker.list_cards(step.name, task_id)
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 2,
                        True,
                    )
                    for card in cards_info["cards"]:
                        checker.assert_card(
                            step.name,
                            task_id,
                            "test_pathspec_card",
                            "%s" % full_pathspec,
                            card_hash=card["hash"],
                            exact_match=False,
                        )
