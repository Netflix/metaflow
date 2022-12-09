from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class DefaultEditableCardWithIdTest(MetaflowTest):
    """
    `current.card.append` should add to default editable card and not the one with `id`
    when a card with `id` and non id are present
        - Access of `current.card` with nonexistent id should not fail.
    """

    PRIORITY = 3

    @tag('environment(vars={"METAFLOW_CARD_NO_WARNING": "True"})')
    @tag('card(type="test_editable_card",id="abc")')
    @tag('card(type="test_editable_card")')
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current
        from metaflow.plugins.cards.card_modules.test_cards import TestStringComponent
        import random

        self.random_number = random.randint(0, 100)
        current.card.append(current.pathspec)
        # This should not fail user code.
        current.card["xyz"].append(TestStringComponent(str(self.random_number)))
        current.card.append(TestStringComponent(str(self.random_number)))

    @steps(0, ["end"], required=True)
    def step_end(self):
        self.here = True

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # This means CliCheck is in context.
            for step in flow:
                if step.name == "end":
                    # Ensure we reach the `end` even when a wrong `id` is used with `current.card`
                    checker.assert_artifact(step.name, "here", True)
                    continue
                elif step.name != "start":
                    continue

                cli_check_dict = checker.artifact_dict(step.name, "random_number")
                for task_pathspec in cli_check_dict:

                    task_id = task_pathspec.split("/")[-1]
                    cards_info = checker.list_cards(step.name, task_id)
                    number = cli_check_dict[task_pathspec]["random_number"]
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 2,
                        True,
                    )
                    # Find the card without the id
                    default_editable_cards = [
                        c for c in cards_info["cards"] if c["id"] is None
                    ]
                    assert_equals(len(default_editable_cards) == 1, True)
                    card = default_editable_cards[0]
                    checker.assert_card(
                        step.name,
                        task_id,
                        "test_editable_card",
                        "%d" % number,
                        card_hash=card["hash"],
                        exact_match=True,
                    )
        else:
            # This means MetadataCheck is in context.
            for step in flow:
                if step.name == "end":
                    # Ensure we reach the `end` even when a wrong `id` is used with `current.card`
                    checker.assert_artifact(step.name, "here", True)
                    continue
                elif step.name != "start":
                    continue
                meta_check_dict = checker.artifact_dict(step.name, "random_number")
                for task_id in meta_check_dict:
                    random_number = meta_check_dict[task_id]["random_number"]
                    cards_info = checker.list_cards(step.name, task_id)
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 2,
                        True,
                    )
                    default_editable_cards = [
                        c for c in cards_info["cards"] if c["id"] is None
                    ]
                    assert_equals(len(default_editable_cards) == 1, True)
                    card = default_editable_cards[0]
                    checker.assert_card(
                        step.name,
                        task_id,
                        "test_editable_card",
                        "%d" % random_number,
                        card_hash=card["hash"],
                        exact_match=False,
                    )
