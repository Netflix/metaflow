from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardHTMLBackwardCompatTest(MetaflowTest):
    """
    Regression test for backward compatibility of @card(type="html").

    Ensures strict card lookup does not break existing flows that use type="html".
    Historically, type="html" was mapped to the blank/default card.
    This test ensures that behavior is preserved.
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

    @tag('card(type="html")')
    @steps(0, ["start"])
    def step_start(self):
        # Set HTML content as users historically did
        self.html = "<h1>Test HTML Card</h1><p>This is a test.</p>"

    @tag('card(type="html")')
    @steps(1, ["all"])
    def step_all(self):
        # Set HTML content in other steps
        self.html = "<div>HTML content in step</div>"

    def check_results(self, flow, checker):
        """
        Verify that cards were created successfully without raising
        CardClassFoundException for type="html"
        """
        run = checker.get_run()
        if run is None:
            # CliCheck context - verify cards exist
            for step in flow:
                cli_check_dict = checker.artifact_dict(step.name, "html")
                for task_pathspec in cli_check_dict:
                    task_id = task_pathspec.split("/")[-1]
                    # Just verify the card exists - the fact that we got here
                    # means type="html" didn't raise CardClassFoundException
                    checker.assert_card(
                        step.name,
                        task_id,
                        "blank",  # html maps to blank internally
                        None,  # We don't check content, just that it exists
                    )
        else:
            # MetadataCheck context
            for step in flow:
                meta_check_dict = checker.artifact_dict(step.name, "html")
                for task_id in meta_check_dict:
                    # Verify card exists with blank type (html maps to blank)
                    checker.assert_card(
                        step.name,
                        task_id,
                        "blank",
                        None,
                    )
