from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardDuplicateIdRegressionTest(MetaflowTest):
    """
    Regression test for issue #3347: Stacked @card with duplicate id from
    a non-editable card should not raise IndexError or silently discard content.

    Test scenarios:
    1. Non-editable card with id="mycard" + editable card with id="mycard"
       -> Should resolve to the editable card
    2. The flow should complete successfully without IndexError
    """

    PRIORITY = 3
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

    @tag('card(type="default_json",id="mycard")')  # non-editable
    @tag('card(type="blank",id="mycard")')  # editable, duplicate id
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current
        from metaflow.plugins.cards.card_modules.basic import MarkdownComponent

        # This should not fail - content should go to the editable blank card
        current.card["mycard"].append(MarkdownComponent("# Test Content"))
        self.content_added = True

    @steps(0, ["end"], required=True)
    def step_end(self):
        # Verify we successfully completed
        assert self.content_added
        self.success = True

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # CLI check
            for step in flow:
                if step.name == "end":
                    # Ensure we reach the end without IndexError
                    checker.assert_artifact(step.name, "success", True)
                elif step.name == "start":
                    checker.assert_artifact(step.name, "content_added", True)
        else:
            # Metadata check
            for step in flow:
                if step.name == "end":
                    checker.assert_artifact(step.name, "success", True)
                elif step.name == "start":
                    checker.assert_artifact(step.name, "content_added", True)
