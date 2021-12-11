from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardImportTest(MetaflowTest):
    """
    Test that checks if the card decorator stores the information as intended for an imported card in card
    - Adds uuid key in an artifact and stores it in card;
    - Checker Asserts that UUID of the task and the one in the card match.
    """

    PRIORITY = 3

    @tag('card(type="mock_card",options={"key":"uuid"})')
    @steps(0, ["start"])
    def step_start(self):
        import uuid

        self.uuid = str(uuid.uuid4())

    @tag('card(type="mock_card",options={"key":"uuid"})')
    @steps(0, ["foreach-nested-inner"])
    def step_foreach_inner(self):
        import uuid

        self.uuid = str(uuid.uuid4())

    @tag('card(type="mock_card",options={"key":"uuid"})')
    @steps(1, ["join"])
    def step_join(self):
        import uuid

        self.uuid = str(uuid.uuid4())

    @tag('card(type="mock_card",options={"key":"uuid"})')
    @steps(1, ["all"])
    def step_all(self):
        import uuid

        self.uuid = str(uuid.uuid4())

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # This means CliCheck is in context.
            for step in flow:
                cli_check_dict = checker.artifact_dict(step.name, "uuid")
                for task_pathspec in cli_check_dict:
                    uuid = cli_check_dict[task_pathspec]["uuid"]
                    task_id = task_pathspec.split("/")[-1]
                    checker.assert_card(step.name, task_id, "mock_card", "%s\n" % uuid)
        else:
            # This means MetadataCheck is in context.
            for step in flow:
                meta_check_dict = checker.artifact_dict(step.name, "uuid")
                for task_id in meta_check_dict:
                    uuid = meta_check_dict[task_id]["uuid"]
                    checker.assert_card(step.name, task_id, "mock_card", "%s" % uuid)
