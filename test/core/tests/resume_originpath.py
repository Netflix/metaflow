from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ResumeOriginPathSpec(MetaflowTest):
    """
    `Step.origin_pathspec` and `Run.origin_pathspec` and `Task.origin_pathspec` should be present

    How does this test work:
        - Store origin_pathspec via the current object in the start step (step that is resumed)
        - In the checker validate the origin_pathspec stored in the start step is the same as the `Step.origin_pathspec` and `Run.origin_pathspec` and `Task.origin_pathspec`

    """

    RESUME = True
    PRIORITY = 4
    PARAMETERS = {"int_param": {"default": 123}}

    @steps(0, ["start"])
    def step_start(self):
        from metaflow import current

        self.origin_pathspec = current.pathspec
        self.data = "start"

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
                    orig_pathspec = task.data.origin_pathspec
                    steporiginpth = "/".join(orig_pathspec.split("/")[:-1])
                    runoriginpth = "/".join(orig_pathspec.split("/")[:-2])
                    assert_equals(orig_pathspec, task.origin_pathspec)
                    assert_equals(steporiginpth, step.origin_pathspec)
                    assert_equals(runoriginpth, run.origin_pathspec)
