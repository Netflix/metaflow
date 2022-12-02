from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardImportTest(MetaflowTest):
    """
    This test tries to check if the import scheme for cards works as intended.
        - Importing a card and calling it via the `type` should work
        - Importable cards could be editable.
        - If the submodule has errors while importing then the rest of metaflow should not fail.
    """

    PRIORITY = 4

    @tag('card(type="editable_import_test_card",save_errors=False)')
    @tag('card(type="test_broken_card",save_errors=False)')
    @tag('card(type="non_editable_import_test_card",save_errors=False)')
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current
        from metaflow.plugins.cards.card_modules.test_cards import TestStringComponent
        import random

        self.random_number = random.randint(0, 100)
        # Adds a card to editable_import_test_card
        current.card.append(TestStringComponent(str(self.random_number)))

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # This means CliCheck is in context.
            for step in flow:
                if step.name != "start":
                    continue

                cli_check_dict = checker.artifact_dict(step.name, "random_number")
                for task_pathspec in cli_check_dict:
                    task_id = task_pathspec.split("/")[-1]
                    random_number = cli_check_dict[task_pathspec]["random_number"]
                    cards_info = checker.list_cards(step.name, task_id)
                    # Safely importable cards should be present.
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 2,
                        True,
                    )
                    impc_e = [
                        c
                        for c in cards_info["cards"]
                        if c["type"] == "editable_import_test_card"
                    ]
                    impc_e = impc_e[0]
                    impc_ne = [
                        c
                        for c in cards_info["cards"]
                        if c["type"] == "non_editable_import_test_card"
                    ]
                    impc_ne = impc_ne[0]
                    checker.assert_card(
                        step.name,
                        task_id,
                        impc_ne["type"],
                        "%s" % cards_info["pathspec"],
                        card_hash=impc_ne["hash"],
                        exact_match=True,
                    )
                    checker.assert_card(
                        step.name,
                        task_id,
                        impc_e["type"],
                        "%d" % random_number,
                        card_hash=impc_e["hash"],
                        exact_match=True,
                    )

        else:
            # This means MetadataCheck is in context.
            for step in flow:
                if step.name != "start":
                    continue
                meta_check_dict = checker.artifact_dict(step.name, "random_number")
                for task_id in meta_check_dict:
                    random_number = meta_check_dict[task_id]["random_number"]
                    cards_info = checker.list_cards(
                        step.name,
                        task_id,
                    )
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 2,
                        True,
                    )
                    impc_e = [
                        c
                        for c in cards_info["cards"]
                        if c["type"] == "editable_import_test_card"
                    ]
                    impc_e = impc_e[0]
                    impc_ne = [
                        c
                        for c in cards_info["cards"]
                        if c["type"] == "non_editable_import_test_card"
                    ]
                    impc_ne = impc_ne[0]
                    # print()
                    task_pathspec = cards_info["pathspec"]
                    checker.assert_card(
                        step.name,
                        task_id,
                        impc_ne["type"],
                        "%s" % task_pathspec,
                        card_hash=impc_ne["hash"],
                        exact_match=True,
                    )
                    checker.assert_card(
                        step.name,
                        task_id,
                        impc_e["type"],
                        "%d" % random_number,
                        card_hash=impc_e["hash"],
                        exact_match=True,
                    )
