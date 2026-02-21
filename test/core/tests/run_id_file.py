from metaflow_test import MetaflowTest, ExpectationFailed, steps


class RunIdFileTest(MetaflowTest):
    """
    Resuming and initial running of a flow should write run id file early (prior to execution)
    """

    RESUME = True
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

    @steps(0, ["singleton-start"], required=True)
    def step_start(self):
        import os
        from metaflow import current
        from metaflow.util import get_latest_run_id

        # Whether we are in "run" or "resume" mode, --run-id-file must be written prior to execution
        assert os.path.isfile(
            "run-id"
        ), "run id file should exist before resume execution"
        with open("run-id", "r") as f:
            run_id_from_file = f.read()
        assert run_id_from_file == current.run_id
        latest_run_id = get_latest_run_id(lambda *_: None, current.flow_name)
        assert latest_run_id == current.run_id

        # Test both regular run and resume paths
        if not is_resumed():
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        pass
