from metaflow_test import MetaflowTest, ExpectationFailed, steps


class RunIdFileTest(MetaflowTest):
    """
    Resuming and initial running of a flow should write run id file early (prior to execution)
    """

    RESUME = True
    PRIORITY = 3

    @steps(0, ["singleton-start"], required=True)
    def step_start(self):
        import os
        from metaflow import current

        # Whether we are in "run" or "resume" mode, --run-id-file must be written prior to execution
        assert os.path.isfile(
            "run-id"
        ), "run id file should exist before resume execution"
        with open("run-id", "r") as f:
            run_id_from_file = f.read()
        assert run_id_from_file == current.run_id

        # Test both regular run and resume paths
        if not is_resumed():
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        pass
