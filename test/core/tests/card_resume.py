from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardResumeTest(MetaflowTest):
    """
    Resuming a flow with card decorators should reference a origin task's card when calling `get_cards` or `card get` cli commands.
    """

    RESUME = True
    PRIORITY = 4

    @tag('card(type="taskspec_card")')
    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.origin_pathspec = current.pathspec

    @steps(0, ["singleton-end"], required=True)
    def step_end(self):
        if not is_resumed():
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is not None:
            for step in run.steps():
                if step.id == "start":
                    task = step.task
                    checker.assert_card(
                        step.id, task.id, "taskspec_card", "%s" % task.origin_pathspec
                    )
        else:
            for step in flow:
                if step.name != "start":
                    continue
                cli_check_dict = checker.artifact_dict(step.name, "origin_pathspec")
                for task_pathspec in cli_check_dict:
                    task_id = task_pathspec.split("/")[-1]
                    checker.assert_card(
                        step.name,
                        task_id,
                        "taskspec_card",
                        "%s" % cli_check_dict[task_pathspec]["origin_pathspec"],
                    )
