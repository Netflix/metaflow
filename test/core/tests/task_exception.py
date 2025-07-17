from metaflow_test import MetaflowTest, ExpectationFailed, steps


class TaskExceptionTest(MetaflowTest):
    """
    A test to validate if exceptions are stored and retrieved correctly
    """

    PRIORITY = 1
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
    ]
    SHOULD_FAIL = True

    @steps(0, ["singleton-end"], required=True)
    def step_start(self):
        raise KeyError("Something has gone wrong")

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is not None:
            for task in run["end"]:
                assert_equals("KeyError" in str(task.exception), True)
                assert_equals(task.exception.exception, "'Something has gone wrong'")
