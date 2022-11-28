from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardDecoratorBasicTest(MetaflowTest):
    """
    Test that checks if the card decorator stores the information as intended for a built-in card
    - sets the pathspec in the task
    - Checker Asserts that taskpathspec in the card and the one set in the task match.
    """

    PRIORITY = 3

    @tag('card(type="taskspec_card")')
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.task = current.pathspec

    @tag('card(type="taskspec_card")')
    @steps(0, ["foreach-nested-inner"])
    def step_foreach_inner(self):
        from metaflow import current

        self.task = current.pathspec

    @tag('card(type="taskspec_card")')
    @steps(1, ["join"])
    def step_join(self):
        from metaflow import current

        self.task = current.pathspec

    @tag('card(type="taskspec_card")')
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
                    taskpathspec_artifact = cli_check_dict[task_pathspec]["task"]
                    task_id = task_pathspec.split("/")[-1]
                    checker.assert_card(
                        step.name,
                        task_id,
                        "taskspec_card",
                        "%s" % taskpathspec_artifact,
                    )
        else:
            # This means MetadataCheck is in context.
            for step in flow:
                meta_check_dict = checker.artifact_dict(step.name, "task")
                for task_id in meta_check_dict:
                    taskpathspec = meta_check_dict[task_id]["task"]
                    checker.assert_card(
                        step.name, task_id, "taskspec_card", "%s" % taskpathspec
                    )
