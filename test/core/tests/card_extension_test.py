from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardExtensionsImportTest(MetaflowTest):
    """
    - Requires on tests/extensions/packages to be installed.
    """

    PRIORITY = 5

    @tag('card(type="card_ext_init_b",save_errors=False)')
    @tag('card(type="card_ext_init_a",save_errors=False)')
    @tag('card(type="card_ns_subpackage",save_errors=False)')
    @tag('card(type="card_init",save_errors=False)')
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.task = current.pathspec

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
                cli_check_dict = checker.artifact_dict(step.name, "task")
                for task_pathspec in cli_check_dict:
                    full_pathspec = "/".join([flow.name, task_pathspec])
                    task_id = task_pathspec.split("/")[-1]
                    cards_info = checker.list_cards(step.name, task_id)
                    # Just check if the cards are created.
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 4,
                        True,
                    )
        else:
            # This means MetadataCheck is in context.
            for step in flow:
                if step.name != "start":
                    continue
                meta_check_dict = checker.artifact_dict(step.name, "task")
                for task_id in meta_check_dict:
                    full_pathspec = meta_check_dict[task_id]["task"]
                    cards_info = checker.list_cards(step.name, task_id)
                    assert_equals(
                        cards_info is not None
                        and "cards" in cards_info
                        and len(cards_info["cards"]) == 4,
                        True,
                    )
